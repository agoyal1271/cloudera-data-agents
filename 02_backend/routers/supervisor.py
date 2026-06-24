"""
Supervisor — HTTP/SSE surface (Phase 1).

  POST /api/supervisor/chat  — route one request through the specialist agents it
                               needs (Scout, Guardian), streaming their SSE events.

The specialists keep their own endpoints; this is an additional, composing entry
point — not a replacement.
"""

import json
import logging
from typing import Optional

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/supervisor", tags=["supervisor"])

_SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}


class SupervisorRequest(BaseModel):
    message: str
    context_asset: Optional[str] = None


def _stream(agen_factory):
    async def event_stream():
        try:
            async for event in agen_factory():
                yield f"data: {json.dumps(event, default=str)}\n\n"
        except Exception as e:
            logger.exception("Supervisor error")
            yield f"data: {json.dumps({'type': 'error', 'agent': 'supervisor', 'message': str(e)})}\n\n"
        yield "data: {\"type\": \"stream_end\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=_SSE_HEADERS)


@router.post("/chat")
async def chat(req: SupervisorRequest):
    """Route through the specialists this request needs; stream their events."""
    from agents.supervisor.supervisor_graph import run_supervisor

    return _stream(lambda: run_supervisor(req.message, req.context_asset))
