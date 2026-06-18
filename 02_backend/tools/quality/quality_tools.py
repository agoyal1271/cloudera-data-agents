"""
Quality Guardian — tools.

Designed as first-class, composable tools (deterministic floor) so a stronger
model can later compose them in an autonomous loop (agentic ceiling) without a
rewrite. Today the handlers call them in a fixed workflow.

Tools:
  run_quality_check(asset, fields)        — fast combined Impala check → scorecard
  get_quality_history(asset, days)        — read rollup scores over time
  quality_trend(asset, days)              — direction / level / driver from history
  write_quality_to_om(asset, result)      — push column profile to OpenMetadata
  suggest_dq_rules_with_lineage(asset)    — infer rules + walk 1 hop upstream for root cause

Results roll up into the Iceberg table default.dq_quality_scores (one row per run).
"""

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

SCORES_TABLE = "default.dq_quality_scores"
SCORES_LOCATION = "s3a://iceberg-warehouse/warehouse/default.db/dq_quality_scores"

# Impala-over-Knox is slow (~15-40s/query) and these change rarely — cache hard.
_HISTORY_CACHE: dict[str, tuple[float, list]] = {}
_SCORECARD_CACHE: dict[str, tuple[float, dict]] = {}
_HISTORY_TTL = 1800.0    # 30 min — history is seeded/static for the demo
_SCORECARD_TTL = 1800.0  # 30 min — a table's quality moves slowly


def _invalidate(asset: str):
    _HISTORY_CACHE.pop(asset, None)
    _SCORECARD_CACHE.pop(asset, None)


def cached_scorecard(asset: str) -> Optional[dict]:
    """Return a cached scorecard if fresh, else None — lets callers skip describe."""
    import time
    c = _SCORECARD_CACHE.get(asset)
    if c and time.monotonic() - c[0] < _SCORECARD_TTL:
        return c[1]
    return None

# Completeness thresholds (null rate) — per README quality rules
WARN_NULL = 0.05   # 5%
FAIL_NULL = 0.20   # 20%
UNIQUE_OK = 0.99   # distinct ratio for id columns


def _impala():
    from impala.dbapi import connect
    return connect(
        host=os.getenv("KNOX_HOST", "cdp-utility.cdp.local"),
        port=8443, use_http_transport=True,
        http_path="gateway/cdp-proxy-api/impala/",
        auth_mechanism="LDAP",
        user=os.getenv("KNOX_USERNAME", "admin"),
        password=os.getenv("KNOX_PASSWORD", ""),
        timeout=60,
    )


def _is_id_col(name: str) -> bool:
    n = name.lower()
    return n.endswith("_id") or n == "id" or n.endswith("_key")


def _score_checks(checks: list[dict]) -> tuple[float, dict, str]:
    """Overall score 0-100 = weighted pass rate (pass=1, warn=0.5, fail=0).
    Returns (score, {pass,warn,fail}, top_driver_label)."""
    graded = [c for c in checks if c["status"] in ("pass", "warn", "fail")]
    if not graded:
        return 100.0, {"pass": 0, "warn": 0, "fail": 0}, ""
    weight = {"pass": 1.0, "warn": 0.5, "fail": 0.0}
    score = sum(weight[c["status"]] for c in graded) / len(graded) * 100
    counts = {s: sum(1 for c in graded if c["status"] == s) for s in ("pass", "warn", "fail")}
    # driver = worst check (highest null rate among warn/fail)
    bad = sorted([c for c in graded if c["status"] != "pass"],
                 key=lambda c: -c.get("metric_value", 0))
    driver = ""
    if bad:
        d = bad[0]
        driver = f"{d['column']} {d['label']}"
    return round(score, 1), counts, driver


# ── One cohesive DQ SQL — all rules as aggregate expressions in a single pass ──

def build_cohesive_dq_sql(asset: str, fields: list[dict]) -> tuple[str, list[dict]]:
    """Build ONE Impala SELECT that computes EVERY rule for the table in a single
    pass: completeness (all cols), uniqueness (id cols), and semantic validity/
    format/range rules (from the rule engine). No per-column queries.

    Returns (sql, specs) where specs maps each SELECT alias back to its check.
    """
    from tools.iceberg.dq_rule_engine import _infer_rules_from_column, _RULE_TEMPLATES

    cols = [(f.get("name", ""), f.get("type", "string")) for f in fields if f.get("name")]
    exprs = ["COUNT(*) AS total"]
    specs: list[dict] = []
    nn_alias: dict[str, str] = {}

    # 1. Completeness — non-null count per column
    for i, (c, _t) in enumerate(cols):
        a = f"nn_{i}"
        exprs.append(f"COUNT(`{c}`) AS {a}")
        nn_alias[c] = a
        specs.append({"alias": a, "kind": "completeness", "column": c})

    # 2. Uniqueness — approx distinct for id-like columns
    for i, (c, _t) in enumerate(cols):
        if _is_id_col(c):
            a = f"ndv_{i}"
            exprs.append(f"NDV(`{c}`) AS {a}")
            specs.append({"alias": a, "kind": "uniqueness", "column": c})

    # 3. Semantic validity / format / range — one CASE-aggregate per rule
    for i, (c, t) in enumerate(cols):
        for rk in _infer_rules_from_column(c, t):
            tmpl = _RULE_TEMPLATES.get(rk)
            if not tmpl:
                continue
            cond = tmpl["violation_expr"](c)
            a = f"v_{i}_{rk}"
            # count violations only among non-null values
            exprs.append(f"SUM(CASE WHEN `{c}` IS NOT NULL AND ({cond}) THEN 1 ELSE 0 END) AS {a}")
            specs.append({
                "alias": a, "kind": "validity", "rule": rk, "column": c,
                "desc": tmpl["description"],
                "warn": tmpl["threshold_warn_pct"], "fail": tmpl["threshold_fail_pct"],
                "nn_alias": nn_alias.get(c),
            })

    sql = "SELECT " + ",\n       ".join(exprs) + f"\nFROM {asset}"
    return sql, specs


# ── Tool: run the cohesive quality check (single Impala query) ────────────────

def run_quality_check(asset: str, fields: list[dict], write_rollup: bool = False) -> dict:
    """Execute the one cohesive DQ SQL on Impala (via Knox) and grade the result.
    Python only builds the SQL and reads the single result row — no per-column work.
    Cached per asset (30 min) since Knox round-trips are slow. write_rollup is off
    by default — live checks don't append to the seeded history every time."""
    import time
    cached = _SCORECARD_CACHE.get(asset)
    if cached and time.monotonic() - cached[0] < _SCORECARD_TTL:
        return cached[1]

    sql, specs = build_cohesive_dq_sql(asset, fields)

    conn = _impala()
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    colnames = [d[0] for d in cur.description]
    rd = dict(zip(colnames, row))
    conn.close()

    total = int(rd.get("total", 0) or 0)
    checks: list[dict] = []

    for s in specs:
        val = rd.get(s["alias"])
        if s["kind"] == "completeness":
            nn = int(val or 0)
            null_rate = (total - nn) / total if total else 0.0
            status = "pass" if null_rate < WARN_NULL else ("warn" if null_rate < FAIL_NULL else "fail")
            checks.append({"check": "completeness", "column": s["column"],
                           "metric_value": round(null_rate, 4),
                           "label": f"{null_rate*100:.1f}% null", "status": status})
        elif s["kind"] == "uniqueness":
            ndv = int(val or 0)
            ratio = ndv / total if total else 1.0
            status = "pass" if ratio >= UNIQUE_OK else ("warn" if ratio >= 0.90 else "fail")
            checks.append({"check": "uniqueness", "column": s["column"],
                           "metric_value": round(1 - ratio, 4),
                           "label": f"{ratio*100:.1f}% distinct", "status": status})
        elif s["kind"] == "validity":
            viol = int(val or 0)
            nn = int(rd.get(s.get("nn_alias"), 0) or 0) if s.get("nn_alias") else total
            pct = (viol / nn * 100) if nn else 0.0
            status = "pass" if pct < s["warn"] else ("warn" if pct < s["fail"] else "fail")
            checks.append({"check": s["rule"], "column": s["column"],
                           "metric_value": round(pct / 100, 4),
                           "label": f"{pct:.1f}% invalid ({s['desc']})", "status": status})

    score, counts, driver = _score_checks(checks)
    ts = datetime.now(timezone.utc)
    result = {
        "asset": asset, "overall_score": score, "counts": counts,
        "checks": checks, "total_rows": total, "driver": driver,
        "run_timestamp": ts.isoformat(),
    }

    if write_rollup:
        try:
            _write_rollup(asset, score, counts, driver, total, ts)
        except Exception as e:
            logger.warning(f"[quality] rollup write failed: {e}")

    import time
    _SCORECARD_CACHE[asset] = (time.monotonic(), result)
    return result


# ── Score history store ───────────────────────────────────────────────────────
# The DQ *measurement* runs on Impala (the cohesive query). The score *history*
# is just cached past results — kept in a fast local JSON store because Iceberg
# writes over Knox are 30-60s each and would make the live experience unusable.
# (In production this rollup would persist to Iceberg/OpenMetadata via a batch job.)

import json as _json
import pathlib as _pathlib

_STORE = _pathlib.Path(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))) / "memory" / "dq_quality_scores.json"


def _load_store() -> dict:
    try:
        if _STORE.exists():
            return _json.loads(_STORE.read_text())
    except Exception as e:
        logger.warning(f"[quality] store read failed: {e}")
    return {}


def _save_store(data: dict):
    try:
        _STORE.parent.mkdir(parents=True, exist_ok=True)
        _STORE.write_text(_json.dumps(data, indent=0))
    except Exception as e:
        logger.warning(f"[quality] store write failed: {e}")


def _write_rollup(asset, score, counts, driver, total, ts: datetime):
    store = _load_store()
    store.setdefault(asset, []).append({
        "timestamp": ts.isoformat(), "score": score, "driver": driver,
        "pass": counts["pass"], "warn": counts["warn"], "fail": counts["fail"],
    })
    _save_store(store)


# ── Tool: history + trend ─────────────────────────────────────────────────────

def get_quality_history(asset: str, days: int = 14) -> list[dict]:
    """Return [{timestamp, score, driver}] for the asset over the last N days, oldest first."""
    from datetime import timedelta
    store = _load_store()
    rows = store.get(asset, [])
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    out = []
    for r in rows:
        try:
            ts = datetime.fromisoformat(r["timestamp"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts >= cutoff:
                out.append(r)
        except Exception:
            continue
    out.sort(key=lambda r: r["timestamp"])
    return out


def quality_trend(asset: str, days: int = 14) -> Optional[dict]:
    """Compute trend from history. Returns None if no history.
    {current, baseline, delta, direction, level, driver, points}."""
    hist = get_quality_history(asset, days)
    if not hist:
        return None
    scores = [h["score"] for h in hist]
    current = scores[-1]
    baseline = scores[0]
    delta = round(current - baseline, 1)

    if abs(delta) < 2:
        direction = "stable"
    elif delta > 0:
        direction = "up"
    else:
        direction = "down"

    level = "good" if current >= 90 else ("fair" if current >= 75 else "poor")
    driver = next((h["driver"] for h in reversed(hist) if h["driver"]), "")
    return {
        "asset": asset, "current": current, "baseline": baseline, "delta": delta,
        "direction": direction, "level": level, "driver": driver,
        "points": [round(s, 1) for s in scores], "window_days": days,
        "runs": len(scores),
    }


# ── Demo seeding — historical rollup rows so trends have something to show ─────

def seed_history(asset: str, start_score: float, end_score: float,
                 days: int = 14, driver: str = "") -> int:
    """Insert one rollup row per day for the last `days` days, interpolating the
    score from start (oldest) to end (newest). Demo helper only."""
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    store = _load_store()
    rows = []
    for d in range(days):
        frac = d / max(days - 1, 1)
        score = round(start_score + (end_score - start_score) * frac, 1)
        ts = now - timedelta(days=(days - 1 - d))
        fail = 1 if score < 85 else 0
        warn = 2 if score < 92 else 1
        passes = max(0, 10 - warn - fail)
        rows.append({
            "timestamp": ts.isoformat(), "score": score,
            "driver": driver if score < 92 else "",
            "pass": passes, "warn": warn, "fail": fail,
        })
    store[asset] = rows  # replace history for this asset
    _save_store(store)
    return len(rows)


# ── Tool: write quality back to OpenMetadata (column profile time-series) ──────

def write_quality_to_om(asset: str, result: dict) -> bool:
    """Push the check result to OpenMetadata as a table profile (native time-series).
    OM then shows row count + per-column null/distinct on the asset page with history."""
    try:
        import time, requests
        from tools.openmetadata.client import find_table_by_name, _headers, OM_URL

        entity = find_table_by_name(asset)
        if not entity or not entity.get("id"):
            logger.info(f"[quality] OM write skipped — {asset} not in OpenMetadata")
            return False

        ts_ms = int(time.time() * 1000)
        total = result.get("total_rows", 0)
        col_profiles = []
        # map completeness checks → column profile null proportion
        for c in result.get("checks", []):
            if c["check"] != "completeness":
                continue
            col_profiles.append({
                "name": c["column"],
                "timestamp": ts_ms,
                "nullProportion": c["metric_value"],
                "nullCount": int(c["metric_value"] * total),
                "valuesCount": total,
            })

        body = {
            "tableProfile": {
                "timestamp": ts_ms,
                "rowCount": total,
                "columnCount": len(result.get("checks", [])),
                "profileSample": 100,
            },
            "columnProfile": col_profiles,
        }
        resp = requests.put(
            f"{OM_URL}/v1/tables/{entity['id']}/tableProfile",
            headers=_headers(), json=body, timeout=15,
        )
        ok = resp.status_code in (200, 201)
        if not ok:
            logger.warning(f"[quality] OM profile push: {resp.status_code} {resp.text[:160]}")
        return ok
    except Exception as e:
        logger.warning(f"[quality] OM write failed: {e}")
        return False


def write_quality_testcases_to_om(asset: str, result: dict) -> int:
    """Promote the quality result to OpenMetadata's native Data Quality tab:
    one Test Case per check (completeness/uniqueness) with a pass/fail result, under
    an executable Test Suite on the table. Returns the number of results written.
    The composite score lands in the suite description ("92/100 · N of M passing")."""
    try:
        import time, requests
        from tools.openmetadata.client import find_table_by_name, _headers, OM_URL

        entity = find_table_by_name(asset)
        if not entity or not entity.get("id"):
            logger.info(f"[quality] OM test-case write skipped — {asset} not in OpenMetadata")
            return 0
        fqn = entity.get("fqn") or entity.get("fullyQualifiedName")
        suite_fqn = f"{fqn}.testSuite"
        checks = result.get("checks", [])
        n_pass = sum(1 for c in checks if c.get("status") == "pass")

        # 1) ensure an executable test suite on the table (idempotent — ignore "exists")
        requests.post(
            f"{OM_URL}/v1/dataQuality/testSuites/executable", headers=_headers(),
            json={"name": f"{fqn}.dqSuite", "executableEntityReference": fqn,
                  "description": (f"Data quality by Cloudera AI · score "
                                  f"{result.get('overall_score')}/100 · {n_pass}/{len(checks)} checks passing")},
            timeout=15,
        )

        TDEF = {"completeness": "columnValuesToBeNotNull", "uniqueness": "columnValuesToBeUnique"}
        STATUS = {"pass": "Success", "warn": "Failed", "fail": "Failed"}
        now = int(time.time() * 1000)
        written = 0
        for c in checks:
            tdef = TDEF.get(c.get("check"))
            col = c.get("column")
            if not tdef or not col:
                continue
            name = f"{col}_{c['check']}"
            # create the test case (idempotent — ignore if it already exists)
            requests.post(
                f"{OM_URL}/v1/dataQuality/testCases", headers=_headers(),
                json={"name": name, "entityLink": f"<#E::table::{fqn}::columns::{col}>",
                      "testSuite": suite_fqn, "testDefinition": tdef},
                timeout=15,
            )
            tcfqn = f"{fqn}.{col}.{name}"
            r = requests.put(
                f"{OM_URL}/v1/dataQuality/testCases/{tcfqn}/testCaseResult", headers=_headers(),
                json={"timestamp": now, "testCaseStatus": STATUS.get(c.get("status"), "Failed"),
                      "result": f"{c.get('check')} · {c.get('label', '')}",
                      "testResultValue": [{"name": c.get("check"), "value": str(c.get("metric_value"))}]},
                timeout=15,
            )
            if r.status_code in (200, 201):
                written += 1
        return written
    except Exception as e:
        logger.warning(f"[quality] OM test-case write failed: {e}")
        return 0


# ── Tool: lineage-aware rule suggestion + 1-hop upstream root cause ────────────

async def suggest_dq_rules_with_lineage(asset: str, fields: list[dict]) -> dict:
    """Deterministic spine + 1-hop upstream root-cause probe.

    1. Infer DQ rules for THIS asset from its schema.
    2. Fetch lineage, take direct upstream sources.
    3. For each upstream Iceberg table, look at its quality trend; if it dropped
       in the same window, flag it as a likely root cause.

    This is the 'agentic seam': today it walks a fixed 1-hop path; a stronger
    model can later turn this into an open-ended multi-hop investigation.
    """
    import asyncio
    from tools.iceberg.dq_rule_engine import generate_semantic_dq_rules
    from tools.openmetadata.client import get_lineage_by_name

    suggested = generate_semantic_dq_rules(asset, fields)
    rule_summary = [{"rule": r["rule_name"], "column": r["column"], "domain": r.get("domain", "")} for r in suggested]

    upstream_findings = []
    try:
        lin = await asyncio.to_thread(get_lineage_by_name, asset, "table")
        upstream = (lin or {}).get("upstream", [])
        seen = set()
        for node in upstream:
            fqn = node.get("fqn", "")
            # only Iceberg tables can be quality-checked (skip topics/dashboards)
            if fqn.startswith("cdp_kafka"):
                continue
            # derive the catalog name (db.table) from the OM fqn cdp_hive.<db>.default.<table>
            parts = fqn.split(".")
            name = f"{parts[1]}.{parts[-1]}" if len(parts) >= 4 else node.get("name", "")
            if name in seen:
                continue
            seen.add(name)
            trend = await asyncio.to_thread(quality_trend, name)
            if trend:
                upstream_findings.append({
                    "asset": name, "current": trend["current"],
                    "direction": trend["direction"], "delta": trend["delta"],
                    "driver": trend["driver"],
                })
    except Exception as e:
        logger.debug(f"[quality] upstream probe skipped: {e}")

    # Root cause = an upstream source whose quality is also dropping
    root_cause = next((u for u in upstream_findings if u["direction"] == "down"), None)
    return {
        "asset": asset,
        "suggested_rules": rule_summary,
        "upstream_checked": upstream_findings,
        "root_cause": root_cause,
    }
