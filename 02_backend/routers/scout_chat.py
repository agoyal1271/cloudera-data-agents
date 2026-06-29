"""
Scout Chat — true ReAct agent for Source Scout.

The LLM receives a set of tools and decides which ones to call, in what order,
and how many times — no hardcoded routing, no keyword matching, no intent dispatch.

Flow: user message → LangGraph ReAct loop → tool calls → tool results → answer
      (repeat until LLM decides it has enough to answer)

SSE block types emitted:
  step       — "Searching catalog…" live status
  assets     — discovered asset cards
  lineage    — N-hop lineage graph from OpenMetadata
  sql_result — generated SQL + Impala result rows
  schema     — field list for an asset
  quality    — data quality score + checks
  text       — final prose answer
  provenance — trace of every LLM/tool call (for the debug panel)
  done       — end of turn
"""

import asyncio
import json
import logging
import time
from typing import AsyncGenerator, Optional

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.tools import tool
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/scout", tags=["scout-chat"])


class ChatRequest(BaseModel):
    message: str
    context_asset: Optional[str] = None
    context_asset_type: Optional[str] = None


def _sse(block: dict) -> str:
    return f"data: {json.dumps(block)}\n\n"


def _pack(summary: str, blocks: list) -> str:
    """Tools return this JSON — summary is what the LLM reads, blocks are emitted as SSE."""
    return json.dumps({"result": summary, "_blocks": blocks})


# ── Tools ─────────────────────────────────────────────────────────────────────
# Each tool returns _pack(summary, blocks).
# The stream handler reads the blocks and emits them as SSE; the LLM reads the summary.

@tool
async def catalog_search(query: str, asset_type: str = "") -> str:
    """Search for data assets (Iceberg tables, Kafka topics) by topic, domain, or keyword.
    Use asset_type='table' to restrict to Iceberg, 'topic' for Kafka, or leave empty for both.
    Returns matching assets with their field names."""
    from routers.scout_chat_helpers import do_catalog_search
    return await do_catalog_search(query, asset_type)


@tool
async def asset_lineage(asset_name: str, depth: int = 3) -> str:
    """Get the full upstream and downstream lineage chain for a named table or Kafka topic.
    Shows every hop: source systems → pipelines → this asset → downstream consumers.
    Leave depth at 3 (the maximum); it already returns the full end-to-end chain."""
    from routers.scout_chat_helpers import do_asset_lineage
    return await do_asset_lineage(asset_name, depth)


@tool
async def asset_schema(asset_name: str) -> str:
    """Get the column names and types for a named table or Kafka topic schema."""
    from routers.scout_chat_helpers import do_asset_schema
    return await do_asset_schema(asset_name)


@tool
async def query_asset(asset_name: str, question: str) -> str:
    """Translate a natural-language question into SQL, run it on Impala, and return the result rows.
    Use this for any question that needs actual values: counts, averages, top-N, trends, etc."""
    from routers.scout_chat_helpers import do_query_asset
    return await do_query_asset(asset_name, question)


@tool
async def data_quality(asset_name: str) -> str:
    """Run data quality checks on an Iceberg table: completeness, uniqueness, business rules.
    Returns an overall score (0-100), pass/warn/fail counts, and per-column metrics."""
    from routers.scout_chat_helpers import do_data_quality
    return await do_data_quality(asset_name)


@tool
async def build_pipeline(source_asset: str, sink_type: str = "adls_iceberg", sink_table: str = "") -> str:
    """Generate a runnable NiFi data pipeline that moves data from a source into a lakehouse/warehouse.
    source_asset: a Kafka topic (e.g. 'order-events') or Iceberg table (e.g. 'demo.raw_orders').
    sink_type: 'adls_iceberg' (default), 'adls_delta', or 'snowflake'.
    sink_table: optional target table name. Use this when the user wants to create, build, or set up
    a pipeline / data flow / ingestion to move data into a target store."""
    from routers.scout_chat_helpers import do_build_pipeline
    return await do_build_pipeline(source_asset, sink_type, sink_table)


# ── Agent ──────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are Source Scout, an AI assistant for the Cloudera data platform.
You have tools to discover assets, trace lineage, describe schemas, query data, and check quality.

Rules:
- ALWAYS use a tool to answer — never guess or hallucinate data, table names, or field values.
- For discovery ("find", "show me", "what data"): use catalog_search.
- For lineage, pipeline, build chain, origin, impact: use asset_lineage.
- For schema / column questions: use asset_schema.
- For counts, averages, top-N, values from data: use query_asset.
- For data quality, trust, cleanliness: use data_quality.
- For creating/building a pipeline or data flow to move data into a lakehouse/warehouse: use build_pipeline.
- You may call multiple tools in sequence if the question needs it (e.g. schema then query).
- After getting results, give a concise answer (2-5 sentences). The UI already shows the tool results visually, so do not repeat the raw data — synthesize and explain."""

_AGENT = None

def _get_agent():
    global _AGENT
    if _AGENT is None:
        from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY
        from langchain_openai import ChatOpenAI
        from langgraph.prebuilt import create_react_agent
        llm = ChatOpenAI(
            base_url=LLM_BASE_URL, model=LLM_MODEL, api_key=LLM_API_KEY,
            temperature=0, streaming=True,
        )
        _AGENT = create_react_agent(
            llm,
            tools=[catalog_search, asset_lineage, asset_schema, query_asset, data_quality, build_pipeline],
        )
    return _AGENT


# ── Stream ─────────────────────────────────────────────────────────────────────

_TOOL_LABELS = {
    "catalog_search": "Searching the catalog",
    "asset_lineage":  "Tracing lineage in OpenMetadata",
    "asset_schema":   "Reading asset schema",
    "query_asset":    "Generating and running SQL",
    "data_quality":   "Profiling data quality",
    "build_pipeline": "Building NiFi pipeline",
}

_TOOL_KINDS = {
    "catalog_search": "deterministic",
    "asset_lineage":  "openmetadata",
    "asset_schema":   "deterministic",
    "query_asset":    "knox",
    "data_quality":   "knox",
    "build_pipeline": "deterministic",
}


async def _stream(req: ChatRequest) -> AsyncGenerator[str, None]:
    t0 = time.monotonic()
    spans: list[dict] = []
    text_chunks: list[str] = []

    yield _sse({"type": "thinking", "text": "Working on it…"})

    human_msg = req.message
    if req.context_asset:
        human_msg = f"[Current asset: {req.context_asset}]\n\n{req.message}"

    inputs = {"messages": [SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=human_msg)]}

    try:
        agent = _get_agent()
        tool_t0: dict[str, float] = {}

        async for event in agent.astream_events(inputs, version="v2"):
            kind  = event["event"]
            name  = event.get("name", "")
            data  = event.get("data", {})

            # ── Tool started ────────────────────────────────────────────────
            if kind == "on_tool_start":
                tool_t0[name] = time.monotonic()
                label  = _TOOL_LABELS.get(name, name)
                inp    = data.get("input", {})
                detail = " · ".join(f"{k}={v}" for k, v in inp.items() if v)[:80] if isinstance(inp, dict) else str(inp)[:80]
                yield _sse({"type": "step", "label": label, "detail": detail})

            # ── Tool finished ───────────────────────────────────────────────
            elif kind == "on_tool_end":
                elapsed = (time.monotonic() - tool_t0.pop(name, t0)) * 1000
                raw = data.get("output", "")
                # langgraph wraps tool output in a ToolMessage — unwrap to the raw string
                if hasattr(raw, "content"):
                    raw = raw.content
                spans.append({
                    "name": _TOOL_LABELS.get(name, name),
                    "kind": _TOOL_KINDS.get(name, "deterministic"),
                    "ms": round(elapsed),
                })

                # Parse the structured blocks the tool packaged for SSE
                try:
                    payload = json.loads(raw if isinstance(raw, str) else str(raw))
                    for block in payload.get("_blocks", []):
                        yield _sse(block)
                except (json.JSONDecodeError, TypeError):
                    pass   # non-JSON tool output is fine — the LLM synthesizes it

            # ── LLM streaming text ──────────────────────────────────────────
            elif kind == "on_chat_model_stream":
                chunk = data.get("chunk")
                if chunk and hasattr(chunk, "content") and chunk.content:
                    # Only accumulate the FINAL answer turn (not tool-call turns which have no text content)
                    text_chunks.append(chunk.content)

            # ── LLM call complete ───────────────────────────────────────────
            elif kind == "on_chat_model_end":
                tokens = None
                try:
                    resp = data.get("output")
                    um = getattr(resp, "usage_metadata", None)
                    if um:
                        tokens = um.get("total_tokens") or (um.get("input_tokens", 0) + um.get("output_tokens", 0))
                except Exception:
                    pass
                spans.append({
                    "name": "LLM reasoning",
                    "kind": "llm",
                    "ms": round((time.monotonic() - t0) * 1000),
                    "model": req.context_asset or "",
                    "tokens": tokens,
                })

                # Emit the text answer when the last LLM call ends (the final synthesis)
                text = "".join(text_chunks).strip()
                if text:
                    yield _sse({"type": "text", "text": text})
                text_chunks.clear()

    except Exception as exc:
        logger.exception("[scout] agent run failed")
        yield _sse({"type": "text", "text": f"Something went wrong: {exc}"})

    # Provenance trace for the debug panel
    total_ms = round((time.monotonic() - t0) * 1000)
    llm_spans  = [s for s in spans if s["kind"] == "llm"]
    yield _sse({"type": "provenance", "spans": spans, "summary": {
        "llm_calls": len(llm_spans),
        "deterministic_steps": len(spans) - len(llm_spans),
        "total_tokens": sum(s.get("tokens") or 0 for s in llm_spans),
        "total_ms": total_ms,
    }})
    yield _sse({"type": "done"})


@router.post("/chat")
async def chat(req: ChatRequest):
    return StreamingResponse(
        _stream(req),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Backward-compat re-exports ─────────────────────────────────────────────────
# analyst_graph.py, supervisor_graph.py, and app.py import these by name.
from routers.scout_chat_helpers import (          # noqa: E402
    _search_assets, _resolve_asset, _semantic_filter,
    _classify, _wants_quality,
)
