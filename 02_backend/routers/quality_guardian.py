"""
Quality Guardian v2 — HTTP/SSE surface.

  POST /api/quality-guardian/profile   — stages A+B+C: basic checks + sample profile,
                                          then renders schema and asks what to scan.
  POST /api/quality-guardian/act        — stage D: takes the user's instruction, turns it
                                          into bounded checks, validates, runs, returns results.
  GET  /api/quality-guardian/checks     — the allowlisted check catalog (for UI hints).

Both POSTs stream Server-Sent Events, matching the other agents in routers/agents.py.
The original Quality Guardian (/api/agents/validate-quality) is unchanged.
"""

import json
import logging
from typing import Optional

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/quality-guardian", tags=["quality-guardian"])

_SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}


class ProfileRequest(BaseModel):
    asset: str
    goal: str = "Profile and quality-check this asset"
    force: bool = False   # bypass the freshness gate and re-scan even if unchanged


class ActRequest(BaseModel):
    asset: str
    user_action: str
    profile: Optional[dict] = None   # the profile object returned by /profile (LLM context only)
    confirm: bool = False            # set true to approve a full-table scan


def _stream(agen_factory):
    async def event_stream():
        try:
            async for event in agen_factory():
                yield f"data: {json.dumps(event, default=str)}\n\n"
        except Exception as e:
            logger.exception("Quality Guardian v2 error")
            yield f"data: {json.dumps({'type': 'error', 'agent': 'quality_guardian', 'message': str(e)})}\n\n"
        yield "data: {\"type\": \"stream_end\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=_SSE_HEADERS)


@router.post("/profile")
async def profile(req: ProfileRequest):
    """Stage A+B+C — basic checks, sample profile, then ask the user what to scan."""
    from agents.quality_guardian.guardian_graph import run_quality_guardian

    return _stream(lambda: run_quality_guardian(
        req.asset, req.goal, mode="profile", force=req.force,
    ))


@router.post("/act")
async def act(req: ActRequest):
    """Stage D — bounded action on the user's natural-language instruction."""
    from agents.quality_guardian.guardian_graph import run_quality_guardian

    return _stream(lambda: run_quality_guardian(
        req.asset, mode="act", user_action=req.user_action,
        profile=req.profile, confirm=req.confirm,
    ))


@router.get("/badge")
async def badge(asset: str):
    """Last-known quality at a glance for an asset — a pure store read, no scan.
    Cheap enough to call for every row in a discovery list (Source Scout hand-off)."""
    from tools.quality import scan_state
    last = scan_state.get_last(asset)
    if not last:
        return {"asset": asset, "has_result": False}
    b = last.get("basic") or {}
    return {
        "asset": asset,
        "has_result": True,
        "overall_score": b.get("overall_score"),
        "counts": b.get("counts"),
        "scanned_at": last.get("scanned_at"),
        "version": last.get("version"),
    }


@router.get("/checks")
async def list_checks():
    """The allowlisted check catalog + named regex patterns — for UI hints and docs."""
    from tools.quality.check_ir import CHECK_CATALOG, PATTERNS
    return {"checks": CHECK_CATALOG, "patterns": list(PATTERNS.keys())}
