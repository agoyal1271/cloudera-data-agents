"""
Supervisor — LangGraph orchestrator over the specialist agents (Phase 2).

A thin hub that routes a request to the specialists it actually needs and lets
them hand context to each other through a shared blackboard (SupervisorState).
It does NOT make any single specialist the boss — each stays an independent
agent/graph; this graph only sequences them and carries state between them.

Topology — a star, NOT a fixed pipeline (agents are skipped when not needed):

   route → supervisor ─┬─ scout ─────┐
              ▲         ├─ guardian ──┤
              │         ├─ pipeline ──┤
              │         ├─ heal ──────┤
              │         └─ respond → END
              └─────────────┘   (pop next step from `plan`; empty → respond)

The blackboard flows context forward, so later agents reuse earlier work:
   scout    → asset, asset_type, schema     (resolved ONCE)
   guardian → quality      (reuses scout's schema, no re-resolve)
   pipeline → flow         (source = scout's asset + type)
   heal     → health       (verifies pipeline's flow)

Examples:
   "is X clean?"            → [guardian]                       (scout/pipeline/heal skipped)
   "find payment data"      → [scout]
   "onboard demo.payments"  → [scout, guardian, pipeline, heal]  (the full chain)

Public entry: run_supervisor(message, context_asset) → async stream of SSE events.
"""

import asyncio
import logging
import re
from typing import Annotated, AsyncGenerator, Optional, TypedDict

from langgraph.graph import END, StateGraph
from langgraph.types import StreamWriter


def _append_history(existing: list, new: list) -> list:
    """Reducer: accumulate conversation turns across graph passes (one session)."""
    return (existing or []) + (new or [])


# Keep session memory bounded — only the last N turns are kept in state.
_MAX_HISTORY_TURNS = 24

from agents.quality_guardian.guardian_graph import run_quality_guardian
# Optimized retrieval + lightweight classify already live in the chat router; reuse
# them rather than duplicating catalog logic. scout_chat is imported by app at
# startup, so this top-level import is safe (no cycle: scout_chat never imports us).
from routers.scout_chat import (
    _search_assets, _semantic_filter, _resolve_asset, _classify, _wants_quality,
)

logger = logging.getLogger(__name__)
AGENT_ID = "supervisor"

_DOTTED = re.compile(r"\b[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\b", re.I)
_DISCOVER_CUES = ("find", "search", "discover", "show me", "give me", "list",
                  "assets", "data about", "tables with", "which table", "what data")
_LINEAGE_CUES = ("lineage", "how is", "how's", "how was", "formed", "built", "build",
                 "derived", "computed", "constructed", "produced", "comes from", "come from",
                 "where does", "upstream", "downstream", "depends on", "feeds", "fed by",
                 "origin", "source of", "made from", "made of", "pipeline that")
_QUALITY_CUES = ("quality", "clean", "trust", "reliab", "valid", "accurate")
_PIPELINE_CUES = ("pipeline", "nifi", "flow", "ingest", "load into", "build a flow")
_HEAL_CUES = ("heal", "self-heal", "monitor", "health", "broken", "failing", "fix the")
_ONBOARD_CUES = ("onboard", "end to end", "end-to-end", "set up ingestion", "full pipeline")
_ANALYST_CUES = ("how many", "how much", "average", "avg", "count of", "total ", "sum of",
                 "top ", "most ", "least ", "distribution", "trend", "anomal", "interesting",
                 "tell me about", "explain", "what is the", "what's the", "compare", "per ",
                 "group by", "analyze", "analyse", "insight", "breakdown",
                 # follow-up phrasings that lean on conversation context
                 "break it down", "break down", "broken down", "drill", "slice by",
                 "split by", "by region", "by category", "by channel", "by day", "by month")

_DEFAULT_SINK = "adls_iceberg"


def _emit(event_type: str, **kwargs) -> dict:
    return {"type": event_type, "agent": AGENT_ID, **kwargs}


def _norm_fields(m: dict) -> list:
    """Normalize an index candidate's fields to [{name,type}] for the UI."""
    out = []
    for f in (m.get("fields") or []):
        if isinstance(f, dict) and f.get("name"):
            out.append({"name": f["name"], "type": f.get("type", "")})
        elif isinstance(f, str) and f:
            out.append({"name": f, "type": ""})
    return out


# ── Graph state = the shared blackboard ───────────────────────────────────────

class SupervisorState(TypedDict, total=False):
    # request
    message: str
    context_asset: Optional[str]
    # session memory — accumulates across turns (persisted by the checkpointer)
    history: Annotated[list, _append_history]
    # plan / routing
    intent: str
    plan: list          # ordered specialists still to run, e.g. ["scout", "guardian"]
    next: str           # node the supervisor chose this hop
    # blackboard — each specialist writes; later ones read
    asset: Optional[str]
    asset_type: Optional[str]
    fields: Optional[list]      # resolved schema (list of {name,type}) — the reuse payload
    version: Optional[str]
    discovered: list
    lineage: Optional[dict]
    quality: Optional[dict]
    pipeline: Optional[dict]
    health: Optional[dict]
    analysis: Optional[str]


# ── Routing ───────────────────────────────────────────────────────────────────

async def route_node(state: SupervisorState, writer: StreamWriter) -> dict:
    """Decide which specialists this request needs — cheaply. The LLM router is a
    FALLBACK, only used when keyword cues don't already determine the plan: on a
    large local model that round-trip is ~5s, so we skip it whenever we can."""
    message = state["message"]
    context_asset = state.get("context_asset")
    ml = message.lower()

    plan: list[str] = []
    intent = ""
    if any(c in ml for c in _ONBOARD_CUES):
        plan, intent = ["scout", "guardian", "pipeline", "heal"], "onboard"
    else:
        # Lineage is checked before discover: "how is X built" should trace the
        # pipeline, not run a generic catalog search.
        if any(c in ml for c in _LINEAGE_CUES):
            plan.append("lineage")
        if any(c in ml for c in _DISCOVER_CUES):
            plan.append("scout")
        if _wants_quality(message) or any(c in ml for c in _QUALITY_CUES):
            plan.append("guardian")
        if any(c in ml for c in _PIPELINE_CUES):
            plan.append("pipeline")
        if any(c in ml for c in _HEAL_CUES):
            plan.append("heal")
        if any(c in ml for c in _ANALYST_CUES):
            plan.append("analyst")

    # Carry the focus asset across turns: explicit context wins, else the asset the
    # session last settled on (loaded from the checkpointer) — so "check its quality"
    # after "how is X built" still knows what "it" is.
    asset = context_asset or state.get("asset")
    if not plan:                          # cues didn't decide → one LLM router call
        cls = await _classify(message, context_asset)
        intent = cls.get("intent", "smalltalk")
        plan = {"discover": ["scout"], "quality": ["guardian"],
                "lineage": ["lineage"]}.get(intent, [])
        asset = asset or cls.get("asset")
    if not intent:
        intent = "onboard" if len(plan) >= 3 else (plan[0] if plan else "smalltalk")

    writer(_emit("plan", intent=intent, plan=plan,
                 skipped=[a for a in ("scout", "lineage", "guardian", "pipeline", "heal", "analyst") if a not in plan],
                 note="only these agents run — the rest are skipped"))
    # Record the user turn in session memory (reducer appends).
    return {"intent": intent, "plan": plan, "asset": asset,
            "history": [{"role": "user", "content": message}]}


async def supervisor_node(state: SupervisorState, writer: StreamWriter) -> dict:
    """The hub: pop the next specialist off the plan, or finish."""
    plan = list(state.get("plan") or [])
    if not plan:
        return {"next": "respond"}
    nxt = plan.pop(0)
    writer(_emit("handoff", to=nxt, remaining=plan))
    return {"next": nxt, "plan": plan}


def supervisor_route(state: SupervisorState) -> str:
    return state.get("next", "respond")


# ── Specialist nodes (each forwards its agent's events + writes the blackboard) ─

async def scout_node(state: SupervisorState, writer: StreamWriter) -> dict:
    """Discovery (optimized retrieval) → write the focus asset + schema to the blackboard."""
    goal = state["message"]
    writer(_emit("agent_start", agent_name="Source Scout", role="discovery"))
    writer(_emit("step", name="discover", status="running"))

    # The vector index already ranks by meaning ("geo-coordinates" → lat/lon tables),
    # so we use its order directly and skip the LLM rerank — that rerank costs ~30s on
    # a large local model and barely changes the top results.
    candidates = await _search_assets(goal, top_k=8)
    matched = candidates[:8]
    # Each card carries its schema (from the index) + type, so the UI can categorize
    # them and make any one's schema explorable on click.
    cards = [{"name": m["name"], "asset_type": m["asset_type"], "columns": _norm_fields(m),
              "field_count": len(m.get("fields", [])), "reason": ""}
             for m in matched]
    writer(_emit("assets", assets=cards))
    writer(_emit("step", name="discover", status="complete"))

    # Focus asset's schema straight from the catalog INDEX — never a live Iceberg
    # describe (that round-trips Knox + the REST catalog + S3 and costs ~15-20s).
    top = matched[0] if matched else None
    asset = top["name"] if top else state.get("asset")
    atype = (top.get("asset_type") if top else None) or state.get("asset_type")
    fields = state.get("fields")
    if top:
        norm = _norm_fields(top)
        if not fields:
            fields = norm
        writer(_emit("blackboard", wrote=["asset", "asset_type", "schema"], asset=asset,
                     asset_type=atype, field_count=len(norm), columns=norm,
                     note="schema from the catalog index — instant, no live describe"))
    return {"discovered": cards, "asset": asset, "asset_type": atype, "fields": fields, "version": None}


async def lineage_node(state: SupervisorState, writer: StreamWriter) -> dict:
    """Trace how an asset is formed — pull its OpenMetadata lineage graph and stream it."""
    import json as _json
    from routers.scout_chat_helpers import do_asset_lineage

    asset = await _resolve_target(state, state["message"])
    if not asset:
        writer(_emit("error", message="Name a table to trace — e.g. “how is demo.order_analytics_mart built”."))
        return {}

    writer(_emit("agent_start", agent_name="Source Scout", role="lineage", asset=asset,
                 note="tracing lineage in OpenMetadata"))
    writer(_emit("step", name="lineage", status="running"))

    packed = await do_asset_lineage(asset, depth=3)
    blocks = []
    try:
        blocks = _json.loads(packed).get("_blocks", [])
    except Exception:
        pass

    lineage_block = next((b for b in blocks if b.get("type") == "lineage"), None)
    resolved = asset
    if lineage_block:
        resolved = lineage_block.get("asset", asset)
        writer(_emit("lineage", asset=resolved,
                     upstream=lineage_block.get("upstream", []),
                     downstream=lineage_block.get("downstream", []),
                     graph=lineage_block.get("graph", {"nodes": [], "edges": []}),
                     edge_count=lineage_block.get("edge_count", 0)))
        writer(_emit("blackboard", wrote=["asset"], asset=resolved,
                     note=f"lineage: {lineage_block.get('edge_count', 0)} edges"))
    else:
        writer(_emit("text", text=f"No lineage is recorded for {resolved} in OpenMetadata yet."))

    writer(_emit("step", name="lineage", status="complete"))
    return {"asset": resolved, "lineage": lineage_block}


async def _resolve_target(state: SupervisorState, message: str) -> Optional[str]:
    """Find the asset a single-domain request is about (no scout step ran)."""
    asset = state.get("asset") or state.get("context_asset")
    if not asset:
        m = _DOTTED.search(message)
        asset = m.group(0) if m else None
    if not asset:
        hits = await _search_assets(message, top_k=1)
        asset = hits[0]["name"] if hits else None
    return asset


async def guardian_node(state: SupervisorState, writer: StreamWriter) -> dict:
    """Quality — reuse the blackboard schema if present, else resolve the asset itself."""
    asset = await _resolve_target(state, state["message"])
    if not asset:
        writer(_emit("error", message="No asset to quality-check. Name a table or run discovery first."))
        return {}

    fields, version = state.get("fields"), state.get("version")
    reused = bool(fields)
    writer(_emit("agent_start", agent_name="Quality Guardian", role="evaluator",
                 asset=asset, schema_reused=reused,
                 note=("reusing schema from the blackboard (no re-resolve)" if reused
                       else "no upstream schema — resolving it directly")))

    quality = None
    async for ev in run_quality_guardian(asset, mode="profile", fields=fields, version=version):
        writer(ev)
        if ev.get("type") == "basic_scorecard":
            quality = {"overall_score": ev.get("overall_score"),
                       "counts": ev.get("counts"), "cached": ev.get("cached", False)}
    return {"asset": asset, "quality": quality}


async def pipeline_node(state: SupervisorState, writer: StreamWriter) -> dict:
    """Pipeline Builder — derive the source from the blackboard asset, build a NiFi flow."""
    from agents.pipeline_builder.agent import PipelineBuilderAgent
    from agents.pipeline_builder.nifi_flow_builder import SOURCES

    asset = await _resolve_target(state, state["message"])
    if not asset:
        writer(_emit("error", message="No source asset for a pipeline. Discover or name one first."))
        return {}

    atype = state.get("asset_type")
    if not atype:
        rec = await _resolve_asset(asset)
        atype = (rec or {}).get("asset_type")
    src_type = atype if atype in SOURCES else "iceberg_table"
    source = {"type": src_type, "name": asset}
    sink = {"type": _DEFAULT_SINK}
    flow_name = "ingest_" + asset.replace(".", "_")

    writer(_emit("agent_start", agent_name="Pipeline Builder", role="tool-use",
                 source=source, sink=sink,
                 note=f"{src_type} → {_DEFAULT_SINK} (source from the blackboard)"))

    pipeline = None
    async for ev in PipelineBuilderAgent().run(
        goal=f"Onboard {asset}", source=source, sink=sink, flow_name=flow_name,
    ):
        writer(ev)
        if ev.get("type") == "flow_generated":
            pipeline = {"flow_name": ev.get("flow_name"), "summary": ev.get("summary")}
    if pipeline:
        writer(_emit("blackboard", wrote=["pipeline"], flow_name=pipeline["flow_name"],
                     note="flow handed to the Healer for verification"))
        # Record source → sink lineage in OpenMetadata (fire-and-forget; non-fatal).
        # Sink table name mirrors the flow builder's convention: short name of the source asset.
        _sink_table = asset.split(".")[-1].replace("-", "_") if asset else ""
        _sink_fqn   = f"adls_iceberg.default.{_sink_table}" if _sink_table else ""
        _src_type   = "topic" if (state.get("asset_type") == "kafka_topic") else "table"

        async def _write_om_lineage():
            try:
                from tools.openmetadata.client import create_lineage_edge
                await asyncio.to_thread(
                    create_lineage_edge,
                    asset, _src_type,
                    _sink_fqn, "table",
                    pipeline["flow_name"],
                )
                logger.info(f"[pipeline] OM lineage edge: {asset} → {_sink_fqn}")
            except Exception as _e:
                logger.debug(f"[pipeline] OM lineage write skipped: {_e}")

        if asset and _sink_fqn:
            asyncio.create_task(_write_om_lineage())
    return {"pipeline": pipeline}


async def heal_node(state: SupervisorState, writer: StreamWriter) -> dict:
    """Pipeline Healer — verify/monitor the freshly-built flow (guard-skips if none)."""
    pipeline = state.get("pipeline")
    if not pipeline:
        writer(_emit("agent_skipped", agent_name="Pipeline Healer",
                     reason="no pipeline on the blackboard to verify"))
        return {}

    pid = pipeline.get("flow_name") or state.get("asset") or "pipeline"
    writer(_emit("agent_start", agent_name="Pipeline Healer", role="reactive", pipeline=pid,
                 note="health check on the new flow"))

    from agents.pipeline_healer.agent import PipelineHealerAgent
    health = None
    async for ev in PipelineHealerAgent().run(goal=f"Verify {pid}", pipeline_id=pid):
        writer(ev)
        if ev.get("type") == "health_check":
            health = {"state": ev.get("state"), "metrics": ev.get("metrics")}
    return {"health": health}


async def analyst_node(state: SupervisorState, writer: StreamWriter) -> dict:
    """Open-ended Q&A over the dataset — reuses the blackboard schema if present."""
    from agents.analyst.analyst_graph import run_analyst

    asset = await _resolve_target(state, state["message"])
    if not asset:
        writer(_emit("error", message="No dataset to analyze. Discover or name one first."))
        return {}

    fields, atype = state.get("fields"), state.get("asset_type")
    reused = bool(fields)
    writer(_emit("agent_start", agent_name="Data Analyst", role="analytics", asset=asset,
                 schema_reused=reused,
                 note=("reusing schema from the blackboard" if reused else "resolving the dataset itself")))

    # Hand the prior conversation to the analyst so follow-ups resolve ("break THAT
    # down by region" knows what "that" was).
    history = (state.get("history") or [])[-_MAX_HISTORY_TURNS:]
    answer = None
    async for ev in run_analyst(asset, state["message"], fields=fields, asset_type=atype,
                                history=history):
        writer(ev)
        if ev.get("type") == "answer":
            answer = ev.get("text")
    return {"asset": asset, "analysis": answer}


async def respond_node(state: SupervisorState, writer: StreamWriter) -> dict:
    """Summarize what actually ran (and, implicitly, what was skipped)."""
    ran, parts = [], []
    if state.get("discovered"):
        ran.append("scout"); parts.append(f"discovered {len(state['discovered'])}")
    if state.get("lineage") is not None:
        ran.append("lineage"); parts.append(f"lineage {(state.get('lineage') or {}).get('edge_count', 0)} edges")
    if state.get("quality") is not None:
        ran.append("guardian"); parts.append(f"quality {(state.get('quality') or {}).get('overall_score')}")
    if state.get("pipeline") is not None:
        ran.append("pipeline"); parts.append(f"flow {(state.get('pipeline') or {}).get('flow_name')}")
    if state.get("health") is not None:
        ran.append("heal"); parts.append(f"health {(state.get('health') or {}).get('state')}")
    if state.get("analysis"):
        ran.append("analyst"); parts.append("answered")

    if not ran and state.get("intent") == "smalltalk":
        writer(_emit("text", text="I can discover data, check its quality, build an ingestion pipeline, "
                                  "and verify its health. Try “onboard demo.payments”."))
    writer(_emit("complete", summary=(" → ".join(parts) if parts else "Nothing to run."),
                 asset=state.get("asset"), agents_run=ran,
                 quality=state.get("quality"), pipeline=state.get("pipeline"),
                 health=state.get("health")))

    # Record the assistant turn in session memory: the analyst's prose answer when
    # there is one, else a compact recap of what ran (keeps follow-ups grounded).
    assistant = state.get("analysis") or (
        f"[{state.get('asset') or 'session'}] " + (" · ".join(parts) if parts else "nothing to run"))
    return {"history": [{"role": "assistant", "content": str(assistant)[:1500]}]}


# ── Graph assembly ────────────────────────────────────────────────────────────

def build_supervisor_graph(checkpointer=None):
    g = StateGraph(SupervisorState)
    g.add_node("route", route_node)
    g.add_node("supervisor", supervisor_node)
    g.add_node("scout", scout_node)
    g.add_node("lineage", lineage_node)
    g.add_node("guardian", guardian_node)
    g.add_node("pipeline", pipeline_node)
    g.add_node("heal", heal_node)
    g.add_node("analyst", analyst_node)
    g.add_node("respond", respond_node)

    g.set_entry_point("route")
    g.add_edge("route", "supervisor")
    g.add_conditional_edges("supervisor", supervisor_route, {
        "scout": "scout", "lineage": "lineage", "guardian": "guardian", "pipeline": "pipeline",
        "heal": "heal", "analyst": "analyst", "respond": "respond",
    })
    for spec in ("scout", "lineage", "guardian", "pipeline", "heal", "analyst"):
        g.add_edge(spec, "supervisor")       # every specialist returns control to the hub
    g.add_edge("respond", END)
    return g.compile(checkpointer=checkpointer)


# Uncheckpointed graph — kept for visualization/compat (run_supervisor uses the
# session-aware one below).
_GRAPH = build_supervisor_graph()

# Session-aware graph, compiled once with the shared Postgres checkpointer.
_SESSION_GRAPH = None


async def _get_session_graph():
    global _SESSION_GRAPH
    if _SESSION_GRAPH is None:
        from agents.common.session_store import get_checkpointer
        saver = await get_checkpointer()          # None if Postgres is down → stateless
        _SESSION_GRAPH = build_supervisor_graph(checkpointer=saver)
    return _SESSION_GRAPH


# ── Public entry point ────────────────────────────────────────────────────────

async def run_supervisor(message: str, context_asset: Optional[str] = None,
                         session_id: Optional[str] = None) -> AsyncGenerator[dict, None]:
    """Route a request through the specialists it needs, streaming their SSE events.

    When a session_id is given (and Postgres is up), the run is checkpointed under
    that thread — prior turns are reloaded so the conversation has memory.
    """
    if not message:
        yield _emit("error", message="No message provided")
        return

    graph = await _get_session_graph()
    config: dict = {"recursion_limit": 40}        # full onboard chain is ~11 supersteps
    if session_id:
        config["configurable"] = {"thread_id": session_id}

    # With a checkpointer, only the NEW turn is submitted — persisted state (history,
    # focus asset) is merged in by LangGraph. Transient routing fields are reset.
    initial: SupervisorState = {
        "message": message, "context_asset": context_asset,
        "plan": [], "intent": "", "next": "",
    }
    try:
        async for event in graph.astream(initial, config=config, stream_mode="custom"):
            yield event
    except Exception as e:
        logger.error(f"[supervisor] graph run failed: {e}")
        yield _emit("error", message=f"Supervisor failed: {e}")
