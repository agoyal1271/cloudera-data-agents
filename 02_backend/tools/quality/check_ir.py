"""
Quality Guardian v2 — bounded check IR + validator + compiler.

This is the guardrail layer. The LLM (or a user) proposes checks as structured JSON;
NOTHING runs until it passes validate_request(). The boundary is enforced here, not in
a prompt:

  • Schema-grounded — every check must target a column that exists in the live schema.
  • Allowlisted — only the check types in ALLOWED_CHECKS, with params inside fixed ranges.
  • Read-only — checks compile to a single SELECT of COUNT/SUM aggregates. No DDL, no
    writes, no joins, no subqueries from the model. Regex patterns come from a fixed set.
  • Bounded — caps on column count, check count, in-set size; scope defaults to "sample";
    scope="full" is flagged needs_confirm so a full-table scan requires explicit approval.

The model is an author of *intent*; the executor stays deterministic and safe.

A request looks like:
  {"columns": ["email","order_total"],
   "checks": [{"col":"email","type":"regex_match","pattern":"email"},
              {"col":"order_total","type":"range","min":0}],
   "scope": "sample"}
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ── Caps ──────────────────────────────────────────────────────────────────────
MAX_CHECKS = 100
MAX_IN_SET = 50
ABS_NUM_BOUND = 1e15   # reject absurd numeric range params

# Named regex allowlist — the model may only reference these by name, never supply raw regex.
PATTERNS = {
    "email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$",
    "ipv4":  r"^([0-9]{1,3}\\.){3}[0-9]{1,3}$",
    "uuid":  r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$",
    "url":   r"^(https?|s3a?|hdfs)://",
    "phone": r"^[+]?[0-9 ()-]{7,20}$",
    "date_iso": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}",
}

# ── Check type allowlist ──────────────────────────────────────────────────────
# Each entry: applies_to(type)->bool, build(col, params)->violation_expr (SQL counting
# a row as a violation when TRUE), default warn/fail % thresholds, and which params are
# allowed. unique/completeness are aggregate kinds handled specially in the compiler.

def _q(col: str) -> str:
    return f"`{col}`"


ALLOWED_CHECKS: dict[str, dict] = {
    "not_null": {
        "kind": "violation", "params": [],
        "build": lambda c, p: f"{_q(c)} IS NULL",
        "warn": 5.0, "fail": 20.0, "desc": "must not be null",
    },
    "non_empty": {
        "kind": "violation", "params": [],
        "build": lambda c, p: f"TRIM({_q(c)}) = ''",
        "warn": 1.0, "fail": 10.0, "desc": "must not be blank",
    },
    "regex_match": {
        "kind": "violation", "params": ["pattern"],
        "build": lambda c, p: f"{_q(c)} NOT REGEXP '{PATTERNS[p['pattern']]}'",
        "warn": 1.0, "fail": 5.0, "desc": "must match format",
    },
    "range": {
        "kind": "violation", "params": ["min", "max"],
        "build": lambda c, p: " OR ".join(
            ([f"{_q(c)} < {p['min']}"] if p.get("min") is not None else [])
            + ([f"{_q(c)} > {p['max']}"] if p.get("max") is not None else [])
        ) or "FALSE",
        "warn": 1.0, "fail": 5.0, "desc": "must be in range",
    },
    "non_negative": {
        "kind": "violation", "params": [],
        "build": lambda c, p: f"{_q(c)} < 0",
        "warn": 1.0, "fail": 5.0, "desc": "must be >= 0",
    },
    "not_future": {
        "kind": "violation", "params": [],
        "build": lambda c, p: f"{_q(c)} > now()",
        "warn": 1.0, "fail": 5.0, "desc": "must not be in the future",
    },
    "not_before": {
        "kind": "violation", "params": ["date"],
        "build": lambda c, p: f"{_q(c)} < '{p['date']}'",
        "warn": 1.0, "fail": 5.0, "desc": "must not pre-date a floor",
    },
    "in_set": {
        "kind": "violation", "params": ["values"],
        "build": lambda c, p: f"{_q(c)} NOT IN ({', '.join(_lit(v) for v in p['values'])})",
        "warn": 1.0, "fail": 5.0, "desc": "must be an allowed value",
    },
    "length_between": {
        "kind": "violation", "params": ["min", "max"],
        "build": lambda c, p: f"LENGTH({_q(c)}) < {p['min']} OR LENGTH({_q(c)}) > {p['max']}",
        "warn": 1.0, "fail": 5.0, "desc": "length must be in range",
    },
    "unique": {
        "kind": "aggregate", "params": [],
        "warn": None, "fail": None, "desc": "values should be unique",
    },
}

# Catalog handed to the LLM so it only proposes things that exist.
CHECK_CATALOG = {name: {"params": spec["params"], "desc": spec["desc"]}
                 for name, spec in ALLOWED_CHECKS.items()}


def _lit(v: Any) -> str:
    """Safe literal for in_set. Strings single-quoted with quotes escaped; numbers bare."""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _valid_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and abs(x) <= ABS_NUM_BOUND


# ── Validation ────────────────────────────────────────────────────────────────

def validate_request(req: dict, schema: dict[str, str]) -> dict:
    """Validate an LLM/user-proposed request against the live schema + caps.

    schema: {column_name: column_type}.
    Returns {ok, checks:[normalized], errors:[...], scope, needs_confirm}.
    Anything not provably safe is dropped with an error — fail closed.
    """
    errors: list[str] = []
    normalized: list[dict] = []

    scope = (req.get("scope") or "sample").lower()
    if scope not in ("sample", "full"):
        errors.append(f"invalid scope '{scope}', defaulting to sample")
        scope = "sample"

    raw_checks = req.get("checks") or []
    if not isinstance(raw_checks, list):
        return {"ok": False, "checks": [], "errors": ["checks must be a list"],
                "scope": scope, "needs_confirm": False}
    if len(raw_checks) > MAX_CHECKS:
        errors.append(f"truncated to first {MAX_CHECKS} checks (got {len(raw_checks)})")
        raw_checks = raw_checks[:MAX_CHECKS]

    for ch in raw_checks:
        ok, norm, err = _validate_one(ch, schema)
        if ok:
            normalized.append(norm)
        else:
            errors.append(err)

    return {
        "ok": bool(normalized),
        "checks": normalized,
        "errors": errors,
        "scope": scope,
        # full-table scan is the only escalation in this read-only agent → confirm gate
        "needs_confirm": scope == "full",
    }


def _validate_one(ch: Any, schema: dict[str, str]) -> tuple[bool, dict, str]:
    if not isinstance(ch, dict):
        return False, {}, f"check must be an object, got {type(ch).__name__}"
    col = ch.get("col")
    ctype = ch.get("type")

    if ctype not in ALLOWED_CHECKS:
        return False, {}, f"check type '{ctype}' not in allowlist"
    if col not in schema:                                   # schema-grounded
        return False, {}, f"column '{col}' not in table schema"

    spec = ALLOWED_CHECKS[ctype]
    params: dict[str, Any] = {}

    if ctype == "regex_match":
        pat = ch.get("pattern")
        if pat not in PATTERNS:
            return False, {}, f"pattern '{pat}' not in allowlist for {col}"
        params["pattern"] = pat
    elif ctype in ("range", "length_between"):
        lo, hi = ch.get("min"), ch.get("max")
        if lo is None and hi is None:
            return False, {}, f"{ctype} on {col} needs min and/or max"
        for k, v in (("min", lo), ("max", hi)):
            if v is not None:
                if not _valid_number(v):
                    return False, {}, f"{ctype} on {col}: {k}={v!r} is not a valid number"
                params[k] = v
        if ctype == "length_between":   # lengths are non-negative ints
            params.setdefault("min", 0)
            params.setdefault("max", ABS_NUM_BOUND)
    elif ctype == "not_before":
        d = ch.get("date")
        if not isinstance(d, str) or not d[:4].isdigit():
            return False, {}, f"not_before on {col} needs an ISO date string"
        params["date"] = d.replace("'", "")
    elif ctype == "in_set":
        vals = ch.get("values")
        if not isinstance(vals, list) or not vals:
            return False, {}, f"in_set on {col} needs a non-empty values list"
        if len(vals) > MAX_IN_SET:
            return False, {}, f"in_set on {col} exceeds {MAX_IN_SET} values"
        params["values"] = vals

    return True, {"col": col, "type": ctype, "params": params,
                  "warn": spec["warn"], "fail": spec["fail"], "kind": spec["kind"]}, ""


# ── Compilation: validated IR → ONE cohesive SELECT ───────────────────────────

def compile_sql(asset: str, checks: list[dict], sample_clause: str | None) -> tuple[str, list[dict]]:
    """Compile validated checks into a single aggregate query.

    sample_clause: a TABLESAMPLE subquery string for scope='sample', or None for full.
    Returns (sql, specs) where specs map each SELECT alias to its check for grading.
    """
    exprs = ["COUNT(*) AS total"]
    specs: list[dict] = []
    nn_alias: dict[str, str] = {}

    for i, ch in enumerate(checks):
        col, ctype = ch["col"], ch["type"]
        if col not in nn_alias:                              # non-null denominator, once per col
            a = f"nn_{len(nn_alias)}"
            exprs.append(f"COUNT(`{col}`) AS {a}")
            nn_alias[col] = a

        if ch["kind"] == "aggregate" and ctype == "unique":
            a = f"ndv_{i}"
            exprs.append(f"NDV(`{col}`) AS {a}")
            specs.append({"alias": a, "nn_alias": nn_alias[col], "check": ch})
        else:
            cond = ALLOWED_CHECKS[ctype]["build"](col, ch["params"])
            a = f"v_{i}"
            exprs.append(f"SUM(CASE WHEN `{col}` IS NOT NULL AND ({cond}) THEN 1 ELSE 0 END) AS {a}")
            specs.append({"alias": a, "nn_alias": nn_alias[col], "check": ch})

    src = sample_clause.format(asset=asset) if sample_clause else asset
    sql = "SELECT " + ",\n       ".join(exprs) + f"\nFROM {src}"
    return sql, specs


def grade(row: dict, specs: list[dict], scope: str) -> list[dict]:
    """Turn the single result row into per-check results with pass/warn/fail."""
    total = int(row.get("total", 0) or 0)
    out = []
    for s in specs:
        ch = s["check"]
        col, ctype = ch["col"], ch["type"]
        nn = int(row.get(s["nn_alias"], 0) or 0)
        if ch["kind"] == "aggregate":                        # unique
            ndv = int(row.get(s["alias"], 0) or 0)
            ratio = ndv / nn if nn else 1.0
            status = "pass" if ratio >= 0.99 else ("warn" if ratio >= 0.90 else "fail")
            out.append({"check": ctype, "column": col, "scope": scope,
                        "metric_value": round(1 - ratio, 4),
                        "label": f"{ratio * 100:.1f}% distinct", "status": status})
        else:
            viol = int(row.get(s["alias"], 0) or 0)
            pct = (viol / nn * 100) if nn else 0.0
            status = "pass" if pct < ch["warn"] else ("warn" if pct < ch["fail"] else "fail")
            out.append({"check": ctype, "column": col, "scope": scope,
                        "metric_value": round(pct / 100, 4),
                        "label": f"{pct:.1f}% invalid", "status": status})
    return out
