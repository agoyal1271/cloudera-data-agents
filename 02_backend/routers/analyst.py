"""
Data Analyst — HTTP/SSE surface.

  POST /api/analyst/ask  — answer an open-ended natural-language question about ONE
                           dataset (schema-grounded, read-only SQL, quality-aware).

Streams the same SSE vocabulary the other agents use.
"""

import json
import logging

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/analyst", tags=["analyst"])

_SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}


class AskRequest(BaseModel):
    asset: str
    question: str


def _stream(agen_factory):
    async def event_stream():
        try:
            async for event in agen_factory():
                yield f"data: {json.dumps(event, default=str)}\n\n"
        except Exception as e:
            logger.exception("Analyst error")
            yield f"data: {json.dumps({'type': 'error', 'agent': 'data_analyst', 'message': str(e)})}\n\n"
        yield "data: {\"type\": \"stream_end\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=_SSE_HEADERS)


@router.post("/ask")
async def ask(req: AskRequest):
    """Open-ended Q&A over one dataset."""
    from agents.analyst.analyst_graph import run_analyst

    return _stream(lambda: run_analyst(req.asset, req.question))
