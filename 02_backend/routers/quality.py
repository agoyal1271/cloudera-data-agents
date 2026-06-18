"""
Quality Guardian — HTTP surface (admin + direct use).

  POST /api/quality/check        — run a quality check on an asset, return scorecard
  GET  /api/quality/trend        — 14-day trend for an asset
  POST /api/quality/seed-history — seed demo history (one-time)
"""

import asyncio
import logging
from fastapi import APIRouter
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/quality", tags=["quality"])


class CheckRequest(BaseModel):
    asset: str
    write_om: bool = True


async def _fields_for(asset: str) -> list[dict]:
    """Resolve an asset's fields from the Iceberg catalog."""
    from tools.iceberg.iceberg_tools import describe_iceberg_table
    schema = await asyncio.to_thread(describe_iceberg_table, asset)
    return schema.get("fields", [])


@router.post("/check")
async def check(req: CheckRequest):
    from tools.quality.quality_tools import (
        run_quality_check, write_quality_to_om, quality_trend, cached_scorecard,
    )
    # Fast path: cached scorecard → skip the describe round-trip entirely.
    result = cached_scorecard(req.asset)
    if result is None:
        fields = await _fields_for(req.asset)
        if not fields:
            return {"error": f"Could not resolve schema for {req.asset}"}
        result = await asyncio.to_thread(run_quality_check, req.asset, fields)
        if req.write_om:
            result["written_to_om"] = await asyncio.to_thread(write_quality_to_om, req.asset, result)
    result = {**result, "trend": await asyncio.to_thread(quality_trend, req.asset)}
    return result


@router.get("/trend")
async def trend(asset: str, days: int = 14):
    from tools.quality.quality_tools import quality_trend
    t = await asyncio.to_thread(quality_trend, asset, days)
    return t or {"found": False, "asset": asset}


class SeedRequest(BaseModel):
    reset: bool = False
    degrade: bool = True   # actually degrade customer_360 data so the story is real


def _impala_conn():
    import os
    from impala.dbapi import connect
    return connect(host=os.getenv("KNOX_HOST", "cdp-utility.cdp.local"), port=8443,
                   use_http_transport=True, http_path="gateway/cdp-proxy-api/impala/",
                   auth_mechanism="LDAP", user=os.getenv("KNOX_USERNAME", "admin"),
                   password=os.getenv("KNOX_PASSWORD", ""), timeout=120)


@router.post("/seed-history")
async def seed_history(req: SeedRequest):
    """Make the demo coherent and honest:
      - degrade customer_360 (null risk_score for ~1/3 of rows) so a live check
        really fails on completeness — the story is real, not just a chart
      - seed 14-day history that declines to roughly that real score
      - upstream customer_events also declines (root-cause story); others healthy."""
    from tools.quality.quality_tools import seed_history as _seed, _invalidate

    def _degrade():
        # null risk_score for 5 of 15 customers → ~33% null → completeness FAIL.
        # One Iceberg UPDATE — the only Impala write here (history is local now).
        conn = _impala_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE demo.customer_360 SET risk_score = CAST(NULL AS DOUBLE) "
                "WHERE customer_id IN ('C-1000','C-1001','C-1002','C-1003','C-1004')"
            )
        except Exception as e:
            logger.warning(f"degrade customer_360: {e}")
        conn.close()

    try:
        if req.degrade:
            await asyncio.to_thread(_degrade)

        out = {}
        # hero: customer_360 slips 96 → 85 (driver matches the real failing check)
        out["demo.customer_360"] = await asyncio.to_thread(
            _seed, "demo.customer_360", 96.0, 85.0, 14, "risk_score rising nulls")
        # upstream root cause: customer_events also slips 95 → 88
        out["demo.customer_events"] = await asyncio.to_thread(
            _seed, "demo.customer_events", 95.0, 88.0, 14, "email completeness dropping")
        # healthy upstream + downstream
        out["demo.payment_transactions"] = await asyncio.to_thread(
            _seed, "demo.payment_transactions", 96.0, 96.0, 14, "")
        out["demo.fraud_alerts"] = await asyncio.to_thread(
            _seed, "demo.fraud_alerts", 91.0, 90.0, 14, "")

        for a in out:
            _invalidate(a)
        return {"seeded": out, "degraded": req.degrade}
    except Exception as e:
        logger.exception("[quality] seed failed")
        return {"error": str(e)}
