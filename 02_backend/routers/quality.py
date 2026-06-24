"""
Quality Guardian — HTTP surface (admin + direct use).

  POST /api/quality/check        — run a quality check on an asset, return scorecard
  GET  /api/quality/trend        — 14-day trend for an asset (built from real runs only)
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
        result = await asyncio.to_thread(run_quality_check, req.asset, fields, True)  # write_rollup → real trend
        if req.write_om:
            result["written_to_om"] = await asyncio.to_thread(write_quality_to_om, req.asset, result)
    result = {**result, "trend": await asyncio.to_thread(quality_trend, req.asset)}
    return result


@router.get("/trend")
async def trend(asset: str, days: int = 14):
    from tools.quality.quality_tools import quality_trend
    t = await asyncio.to_thread(quality_trend, asset, days)
    return t or {"found": False, "asset": asset}
