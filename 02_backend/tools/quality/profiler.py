"""
Quality Guardian v2 — Profiling engine.

Two passes, both pushdown to Impala-over-Knox (zero compute in the container):

  basic_checks(asset, fields)    — EXACT, full table. Volume + completeness (all
                                   columns) + uniqueness (id-like columns). One
                                   cohesive aggregate query. This is the cheap floor
                                   that always produces a scorecard with no knowledge
                                   of what the columns mean.

  sample_profile(asset, fields)  — ESTIMATED, on a bounded sample of
                                   min(1% of rows, 100k rows). Per-column stats
                                   (null rate, distinct ratio, numeric min/max/avg,
                                   string length, regex "fingerprints" for email/ip/
                                   uuid/url/date). Feeds the LLM classifier — the model
                                   reasons over evidence (the profile), not the name.

Sampling uses Impala TABLESAMPLE SYSTEM(<pct>) with a hard LIMIT ceiling, so a
billion-row table never scans more than the cap. Row count for sizing the sample
comes from the basic pass (which counts anyway), so we never COUNT(*) twice.
"""

import logging
import math
import os
import re
from typing import Optional

logger = logging.getLogger(__name__)

# Sample sizing — min(SAMPLE_PCT of N, SAMPLE_ROW_CAP)
SAMPLE_PCT = 0.01          # 1%
SAMPLE_ROW_CAP = 100_000   # hard ceiling regardless of table size
QUERY_TIMEOUT_SECS = 60

# Regex fingerprints — "does this string column look like X?". Impala REGEXP syntax;
# backslashes are doubled for the SQL string literal. Used only on string columns.
FINGERPRINTS = {
    "email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$",
    "ipv4":  r"^([0-9]{1,3}\\.){3}[0-9]{1,3}$",
    "uuid":  r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$",
    "url":   r"^(https?|s3a?|hdfs)://",
    "date":  r"^[0-9]{4}-[0-9]{2}-[0-9]{2}",
}


# ── Type helpers (mirror dq_rule_engine, kept local to avoid coupling) ─────────

def _is_numeric(t: str) -> bool:
    return any(k in t.lower() for k in ("int", "long", "float", "double", "decimal", "numeric"))


def _is_string(t: str) -> bool:
    return any(k in t.lower() for k in ("string", "varchar", "char", "text"))


def _is_temporal(t: str) -> bool:
    return any(k in t.lower() for k in ("timestamp", "date", "time"))


def _is_id_col(name: str) -> bool:
    n = name.lower()
    return n.endswith("_id") or n == "id" or n.endswith("_key") or n.endswith("_uuid")


# ── Impala connection (same pattern as tools/quality/quality_tools.py) ─────────

def get_impala_conn():
    """Impala over Knox. Caller must run inside asyncio.to_thread (blocking driver)."""
    from urllib.parse import urlparse
    from impala.dbapi import connect

    knox_login_url = os.getenv("KNOX_LOGIN_URL", "")
    host = (
        os.getenv("KNOX_HOST")
        or (urlparse(knox_login_url).hostname if knox_login_url else None)
        or "cdp-utility.cdp.local"
    )
    return connect(
        host=host,
        port=int(os.getenv("IMPALA_PORT", "8443")),
        use_http_transport=True,
        http_path=os.getenv("IMPALA_HTTP_PATH", "gateway/cdp-proxy-api/impala/"),
        auth_mechanism="LDAP",
        user=os.getenv("KNOX_USERNAME") or os.getenv("KNOX_USER") or "admin",
        password=os.getenv("KNOX_PASSWORD", ""),
        timeout=QUERY_TIMEOUT_SECS,
    )


def _cols(fields: list[dict]) -> list[tuple[str, str]]:
    return [(f.get("name", ""), f.get("type", "string")) for f in fields if f.get("name")]


# ── Pass 1: basic exact checks (full table) ───────────────────────────────────

def build_basic_sql(asset: str, fields: list[dict]) -> tuple[str, list[dict]]:
    """ONE aggregate over the full table: total rows, non-null per column,
    NDV per id-like column. Returns (sql, specs) mapping aliases back to checks."""
    cols = _cols(fields)
    exprs = ["COUNT(*) AS total"]
    specs: list[dict] = []
    for i, (c, _t) in enumerate(cols):
        exprs.append(f"COUNT(`{c}`) AS nn_{i}")
        specs.append({"alias": f"nn_{i}", "kind": "completeness", "column": c})
    for i, (c, _t) in enumerate(cols):
        if _is_id_col(c):
            exprs.append(f"NDV(`{c}`) AS ndv_{i}")
            specs.append({"alias": f"ndv_{i}", "kind": "uniqueness", "column": c})
    sql = "SELECT " + ", ".join(exprs) + f"\nFROM {asset}"
    return sql, specs


def basic_checks(asset: str, fields: list[dict]) -> dict:
    """Run the basic exact pass. Returns {total_rows, checks:[...], counts, overall_score}.
    completeness: pass <5% null / warn <20% / fail. uniqueness: pass >=99% distinct."""
    sql, specs = build_basic_sql(asset, fields)
    conn = get_impala_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        rd = dict(zip([d[0] for d in cur.description], row))
    finally:
        conn.close()

    total = int(rd.get("total", 0) or 0)
    checks = []
    for s in specs:
        val = rd.get(s["alias"])
        if s["kind"] == "completeness":
            nn = int(val or 0)
            null_rate = (total - nn) / total if total else 0.0
            status = "pass" if null_rate < 0.05 else ("warn" if null_rate < 0.20 else "fail")
            checks.append({"check": "completeness", "column": s["column"], "scope": "full",
                           "metric_value": round(null_rate, 4),
                           "label": f"{null_rate * 100:.1f}% null", "status": status})
        elif s["kind"] == "uniqueness":
            ndv = int(val or 0)
            ratio = ndv / total if total else 1.0
            status = "pass" if ratio >= 0.99 else ("warn" if ratio >= 0.90 else "fail")
            checks.append({"check": "uniqueness", "column": s["column"], "scope": "full",
                           "metric_value": round(1 - ratio, 4),
                           "label": f"{ratio * 100:.1f}% distinct", "status": status})

    return {"asset": asset, "total_rows": total, "checks": checks, **_score(checks)}


# ── Pass 2: sample profile (bounded) ──────────────────────────────────────────

def _sample_clause(total_rows: int) -> str:
    """Subquery that yields min(1%, 100k) rows. TABLESAMPLE for the %, LIMIT for the cap."""
    if total_rows and total_rows > 0:
        pct = max(1, min(100, math.ceil(SAMPLE_PCT * 100)))            # 1% → SYSTEM(1)
        cap = min(SAMPLE_ROW_CAP, max(1, math.ceil(total_rows * SAMPLE_PCT)))
    else:
        pct, cap = 1, SAMPLE_ROW_CAP
    return f"(SELECT * FROM {{asset}} TABLESAMPLE SYSTEM({pct}) LIMIT {cap}) AS _s"


def build_profile_sql(asset: str, fields: list[dict], total_rows: int) -> tuple[str, list[dict]]:
    """ONE aggregate over the sample producing per-column profile metrics."""
    cols = _cols(fields)
    exprs = ["COUNT(*) AS sampled"]
    specs: list[dict] = []

    for i, (c, t) in enumerate(cols):
        exprs.append(f"COUNT(`{c}`) AS nn_{i}")
        exprs.append(f"NDV(`{c}`) AS ndv_{i}")
        spec = {"index": i, "column": c, "type": t, "fingerprints": []}

        if _is_numeric(t):
            exprs.append(f"MIN(`{c}`) AS min_{i}")
            exprs.append(f"MAX(`{c}`) AS max_{i}")
            exprs.append(f"AVG(`{c}`) AS avg_{i}")
            exprs.append(f"SUM(CASE WHEN `{c}` < 0 THEN 1 ELSE 0 END) AS neg_{i}")
            spec["numeric"] = True
        elif _is_temporal(t):
            exprs.append(f"CAST(MIN(`{c}`) AS STRING) AS min_{i}")
            exprs.append(f"CAST(MAX(`{c}`) AS STRING) AS max_{i}")
            exprs.append(f"SUM(CASE WHEN `{c}` > now() THEN 1 ELSE 0 END) AS fut_{i}")
            spec["temporal"] = True
        elif _is_string(t):
            exprs.append(f"MIN(LENGTH(`{c}`)) AS minlen_{i}")
            exprs.append(f"MAX(LENGTH(`{c}`)) AS maxlen_{i}")
            for fp, pat in FINGERPRINTS.items():
                exprs.append(
                    f"SUM(CASE WHEN `{c}` IS NOT NULL AND `{c}` REGEXP '{pat}' THEN 1 ELSE 0 END) AS fp_{fp}_{i}"
                )
                spec["fingerprints"].append(fp)
            spec["string"] = True
        specs.append(spec)

    sample = _sample_clause(total_rows).format(asset=asset)
    sql = "SELECT " + ",\n       ".join(exprs) + f"\nFROM {sample}"
    return sql, specs


def sample_profile(asset: str, fields: list[dict], total_rows: int) -> dict:
    """Run the sample pass. Returns {sampled_rows, estimated, columns:[per-col profile]}."""
    sql, specs = build_profile_sql(asset, fields, total_rows)
    conn = get_impala_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        rd = dict(zip([d[0] for d in cur.description], row))
    finally:
        conn.close()

    sampled = int(rd.get("sampled", 0) or 0)
    columns = []
    for s in specs:
        i = s["index"]
        nn = int(rd.get(f"nn_{i}", 0) or 0)
        ndv = int(rd.get(f"ndv_{i}", 0) or 0)
        prof = {
            "column": s["column"],
            "type": s["type"],
            "null_rate": round((sampled - nn) / sampled, 4) if sampled else None,
            "distinct": ndv,
            "distinct_ratio": round(ndv / nn, 4) if nn else None,
        }
        if s.get("numeric"):
            prof.update({
                "min": _num(rd.get(f"min_{i}")), "max": _num(rd.get(f"max_{i}")),
                "avg": _num(rd.get(f"avg_{i}")),
                "negatives": int(rd.get(f"neg_{i}", 0) or 0),
            })
        if s.get("temporal"):
            prof.update({
                "min": rd.get(f"min_{i}"), "max": rd.get(f"max_{i}"),
                "future_dates": int(rd.get(f"fut_{i}", 0) or 0),
            })
        if s.get("string"):
            prof.update({"min_len": _num(rd.get(f"minlen_{i}")), "max_len": _num(rd.get(f"maxlen_{i}"))})
            fps = {}
            for fp in s["fingerprints"]:
                hits = int(rd.get(f"fp_{fp}_{i}", 0) or 0)
                if nn and hits:
                    fps[fp] = round(hits / nn, 3)   # fraction of non-null values matching
            if fps:
                prof["looks_like"] = fps            # e.g. {"email": 0.98}
        columns.append(prof)

    return {
        "asset": asset, "sampled_rows": sampled, "estimated": True,
        "sample_rule": f"min({int(SAMPLE_PCT*100)}% of {total_rows:,}, {SAMPLE_ROW_CAP:,})",
        "columns": columns,
    }


# ── helpers ───────────────────────────────────────────────────────────────────

def _num(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return None if math.isnan(f) else round(f, 4)
    except (TypeError, ValueError):
        return None


def _score(checks: list[dict]) -> dict:
    graded = [c for c in checks if c.get("status") in ("pass", "warn", "fail")]
    if not graded:
        return {"overall_score": 100.0, "counts": {"pass": 0, "warn": 0, "fail": 0}, "driver": ""}
    weight = {"pass": 1.0, "warn": 0.5, "fail": 0.0}
    score = sum(weight[c["status"]] for c in graded) / len(graded) * 100
    counts = {s: sum(1 for c in graded if c["status"] == s) for s in ("pass", "warn", "fail")}
    bad = sorted([c for c in graded if c["status"] != "pass"], key=lambda c: -c.get("metric_value", 0))
    driver = f"{bad[0]['column']} {bad[0]['label']}" if bad else ""
    return {"overall_score": round(score, 1), "counts": counts, "driver": driver}
