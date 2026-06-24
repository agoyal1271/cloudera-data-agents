"""
Quality Guardian — LangGraph implementation.

The same profile-first, bounded, human-in-the-loop flow as guardian_agent.py, but
the orchestration is a LangGraph StateGraph instead of two hand-rolled async methods.
Two staged flows share one graph, selected by `mode` at the entry edge:

  PROFILE (mode="profile")
      p_resolve ──[fresh?]──┬─ unchanged ─▶ p_cached ─▶ END   (serve last result, ask)
                            └─ changed ───▶ p_basic ─[ok?]─▶ p_profile ─▶ END

  ACT (mode="act")
      a_resolve ─▶ a_translate ─[valid? needs-confirm?]──┬─ stop ─▶ END
                                                         └─ run ──▶ a_execute ─▶ END

Each node streams the *same* SSE events the original agent emitted, via the custom
stream writer (stream_mode="custom"), so routers/quality_guardian.py and the frontend
are unchanged. The boundary is still tools/quality/check_ir.py — the LLM only authors
intent; nothing runs until validate_request() passes.

The graph delegates heavy lifting (schema resolve, NL→IR, profiling helpers, SQL run)
to the existing QualityGuardianV2Agent so there is a single, vetted implementation of
that logic — this module owns flow, not DQ mechanics.

Public entry point: run_quality_guardian(asset, goal, mode=..., ...) → async event stream.
"""

import asyncio
import logging
from typing import AsyncGenerator, Optional, TypedDict

from langgraph.graph import END, StateGraph
from langgraph.types import StreamWriter

from agents.decision_store.logger import log_decision
from agents.quality_guardian.guardian_agent import QualityGuardianV2Agent
from tools.quality import profiler, check_ir, scan_state

logger = logging.getLogger(__name__)

AGENT_ID = "quality_guardian"   # shares the audit trail with the original agent

# Reuse the existing agent purely as a helper/service object (schema resolve, NL→IR,
# row estimate, run_sql, semantic hints). It holds no per-request state, so one shared
# instance is safe and keeps the DQ logic single-sourced.
_core = QualityGuardianV2Agent()


def _emit(event_type: str, **kwargs) -> dict:
    """One SSE event dict — identical shape to BaseAgent.emit()."""
    return {"type": event_type, "agent": AGENT_ID, **kwargs}


# ── Graph state ───────────────────────────────────────────────────────────────

class GuardianState(TypedDict, total=False):
    # inputs
    asset: str
    goal: str
    mode: str                 # "profile" | "act"
    force: bool
    user_action: Optional[str]
    client_profile: Optional[dict]
    confirm: bool
    # working set (filled in by nodes)
    fields: list
    schema: dict
    version: Optional[str]
    basic: Optional[dict]
    validation: Optional[dict]
    error: Optional[str]
    stop: bool


# ── PROFILE nodes (stages A + B + C) ──────────────────────────────────────────

async def p_resolve(state: GuardianState, writer: StreamWriter) -> dict:
    """Resolve schema + data version server-side (the boundary never trusts the client)."""
    asset, goal = state["asset"], state.get("goal", "")
    writer(_emit("started", asset=asset, goal=goal, mode="profile"))
    writer(_emit("step", name="resolve_schema", status="running"))

    # Reuse a schema handed down by an upstream agent (the supervisor blackboard) —
    # skip the catalog describe entirely. This is the cross-agent context win.
    prefetched = state.get("fields")
    if prefetched:
        schema = {f["name"]: f.get("type", "string") for f in prefetched}
        writer(_emit("schema", asset=asset, reused=True,
                     columns=[{"name": n, "type": t} for n, t in schema.items()]))
        writer(_emit("step", name="resolve_schema", status="complete", detail="reused upstream schema"))
        return {"fields": prefetched, "schema": schema, "version": state.get("version")}

    meta = await _core._describe_meta(asset)
    fields = (meta or {}).get("fields", [])
    if not fields:
        writer(_emit("error", message=f"Could not resolve schema for {asset}"))
        return {"error": "no_schema"}

    version = _core._data_version(meta)
    schema = {f["name"]: f.get("type", "string") for f in fields}
    writer(_emit("schema", asset=asset, columns=[{"name": n, "type": t} for n, t in schema.items()]))
    writer(_emit("step", name="resolve_schema", status="complete"))
    return {"fields": fields, "schema": schema, "version": version}


async def p_cached(state: GuardianState, writer: StreamWriter) -> dict:
    """Freshness gate hit — serve the last scan instead of re-running, then ask."""
    asset, version = state["asset"], state.get("version")
    last = scan_state.get_last(asset) or {}
    b, p = last.get("basic") or {}, last.get("profile") or {}

    writer(_emit("skipped_unchanged", asset=asset, version=version,
                 last_scanned=last.get("scanned_at"),
                 message="No new Iceberg snapshot since the last scan — serving the cached result "
                         "instead of re-running. Use 'Re-scan anyway' to force."))
    writer(_emit("basic_scorecard", asset=asset, exact=True, scope="full", cached=True,
                 overall_score=b.get("overall_score"), counts=b.get("counts"),
                 total_rows=b.get("total_rows"), driver=b.get("driver"), checks=b.get("checks", [])))
    writer(_emit("sample_profile", asset=asset, cached=True, estimated=True,
                 sampled_rows=p.get("sampled_rows", 0), sample_rule=p.get("sample_rule", ""),
                 columns=p.get("columns", [])))

    hints = _core._semantic_hints(p.get("columns", []))
    if hints:
        writer(_emit("semantic_hints", asset=asset, hints=hints,
                     note="From the last scan — confirm before binding"))
    writer(_emit("question", asset=asset,
                 message="Showing the last result (data unchanged). Name columns/rules to scan, "
                         "or send your answer to /api/quality-guardian/act.",
                 suggested=hints, profile=p))
    writer(_emit("complete", summary=f"{asset} unchanged since {last.get('scanned_at', 'last scan')} "
                                     f"— served cached score {b.get('overall_score')}."))
    return {}


async def p_basic(state: GuardianState, writer: StreamWriter) -> dict:
    """Stage A — exact, full-table volume / completeness / uniqueness scorecard."""
    asset, fields = state["asset"], state["fields"]
    writer(_emit("step", name="basic_checks", status="running"))
    try:
        basic = await asyncio.wait_for(
            asyncio.to_thread(profiler.basic_checks, asset, fields),
            timeout=profiler.QUERY_TIMEOUT_SECS,
        )
    except Exception as e:
        logger.warning(f"[qg-graph] basic checks failed for {asset}: {e}")
        writer(_emit("error", message=f"Basic checks failed: {e}"))
        return {"error": "basic_failed"}

    writer(_emit("basic_scorecard", asset=asset,
                 overall_score=basic["overall_score"], counts=basic["counts"],
                 total_rows=basic["total_rows"], driver=basic["driver"],
                 checks=basic["checks"], scope="full", exact=True))
    writer(_emit("step", name="basic_checks", status="complete"))
    return {"basic": basic}


async def p_profile(state: GuardianState, writer: StreamWriter) -> dict:
    """Stage B + C — sample profile, persist version, then render hints and ask."""
    asset, goal, fields = state["asset"], state.get("goal", ""), state["fields"]
    basic, version = state["basic"], state.get("version")

    writer(_emit("step", name="sample_profile", status="running"))
    try:
        prof = await asyncio.wait_for(
            asyncio.to_thread(profiler.sample_profile, asset, fields, basic["total_rows"]),
            timeout=profiler.QUERY_TIMEOUT_SECS,
        )
    except Exception as e:
        logger.warning(f"[qg-graph] sample profile failed for {asset}: {e}")
        prof = {"asset": asset, "sampled_rows": 0, "estimated": True, "columns": [], "error": str(e)}

    writer(_emit("sample_profile", asset=asset, sampled_rows=prof.get("sampled_rows", 0),
                 sample_rule=prof.get("sample_rule", ""), estimated=True,
                 columns=prof.get("columns", [])))
    writer(_emit("step", name="sample_profile", status="complete"))

    # Persist version + results so the next profile can skip if the data is unchanged.
    scan_state.save(asset, version, basic, prof)

    hints = _core._semantic_hints(prof.get("columns", []))
    if hints:
        writer(_emit("semantic_hints", asset=asset, hints=hints,
                     note="Inferred from sample evidence — confirm before binding"))
    log_decision(
        agent_id=AGENT_ID, decision_type="profile",
        inputs={"asset": asset, "goal": goal, "version": version},
        output={"overall_score": basic["overall_score"], "total_rows": basic["total_rows"],
                "sampled_rows": prof.get("sampled_rows", 0)},
        metadata={"hints": hints},
    )
    writer(_emit("question", asset=asset,
                 message="Any specific columns or rules you want me to scan? "
                         "(Name columns, or say 'go' to deep-scan the suggested ones.) "
                         "Send your answer to /api/quality-guardian/act.",
                 suggested=hints, profile=prof))
    writer(_emit("complete", summary=f"Profiled {asset}: score {basic['overall_score']} "
                                     f"on {basic['total_rows']:,} rows. Awaiting focus."))
    return {}


# ── ACT nodes (stage D — bounded action) ──────────────────────────────────────

async def a_resolve(state: GuardianState, writer: StreamWriter) -> dict:
    """Re-resolve schema server-side — grounding must not depend on client input."""
    asset, user_action = state["asset"], state.get("user_action", "")
    writer(_emit("started", asset=asset, mode="act", user_action=user_action))

    fields = state.get("fields")               # reuse upstream schema if the blackboard has it
    reused = bool(fields)
    if not fields:
        fields = await _core._schema_fields(asset)
    if not fields:
        writer(_emit("error", message=f"Could not resolve schema for {asset}"))
        return {"error": "no_schema"}
    schema = {f["name"]: f.get("type", "string") for f in fields}
    if reused:
        writer(_emit("schema", asset=asset, reused=True,
                     columns=[{"name": n, "type": t} for n, t in schema.items()]))
    return {"fields": fields, "schema": schema}


async def a_translate(state: GuardianState, writer: StreamWriter) -> dict:
    """LLM NL→IR, then the boundary: validate_request() + confirm gate (fail closed)."""
    asset, user_action = state["asset"], state.get("user_action", "")
    schema, client_profile, confirm = state["schema"], state.get("client_profile"), state.get("confirm", False)

    writer(_emit("thought", message="Translating your request into bounded checks…"))
    req = await _core._llm_to_ir(asset, user_action, schema, client_profile)
    writer(_emit("proposed_checks", asset=asset, raw=req.get("checks", []), scope=req.get("scope", "sample")))

    result = check_ir.validate_request(req, schema)
    writer(_emit("validation", ok=result["ok"], accepted=len(result["checks"]),
                 errors=result["errors"], scope=result["scope"]))

    if not result["ok"]:
        writer(_emit("complete", summary="No valid checks to run. "
                     f"Rejected: {'; '.join(result['errors']) or 'nothing proposed'}"))
        return {"validation": result, "stop": True}

    if result["needs_confirm"] and not confirm:
        writer(_emit("confirm_required", asset=asset, scope="full", checks=result["checks"],
                     message="This requests a FULL-TABLE scan (beyond the sample). "
                             "Re-send with confirm=true to proceed; otherwise it runs on the sample."))
        return {"validation": result, "stop": True}

    return {"validation": result}


async def a_execute(state: GuardianState, writer: StreamWriter) -> dict:
    """Compile validated checks → SQL, run (read-only), grade, score, report."""
    asset, fields, user_action = state["asset"], state["fields"], state.get("user_action", "")
    result = state["validation"]
    scope = result["scope"]

    # sample scope needs the row count to size the TABLESAMPLE; full scope = no clause.
    sample_clause = None
    if scope == "sample":
        total = await _core._row_estimate(asset, fields)
        sample_clause = profiler._sample_clause(total)

    sql, specs = check_ir.compile_sql(asset, result["checks"], sample_clause)
    logger.info(f"[qg-graph] compiled DQ SQL for {asset} ({scope}): {sql[:300]}")
    writer(_emit("executing", asset=asset, scope=scope, check_count=len(result["checks"])))

    try:
        row = await asyncio.wait_for(
            asyncio.to_thread(_core._run_sql, sql), timeout=profiler.QUERY_TIMEOUT_SECS)
    except Exception as e:
        logger.warning(f"[qg-graph] check execution failed for {asset}: {e}")
        log_decision(agent_id=AGENT_ID, decision_type="targeted_check_failed",
                     inputs={"asset": asset, "action": user_action}, output={"error": str(e)}, status="fail")
        writer(_emit("error", message=f"Check execution failed: {e}"))
        return {"error": "exec_failed"}

    checks = check_ir.grade(row, specs, scope)
    score = profiler._score(checks)
    writer(_emit("results", asset=asset, scope=scope, exact=(scope == "full"),
                 overall_score=score["overall_score"], counts=score["counts"],
                 driver=score["driver"], checks=checks))

    status = "fail" if score["counts"]["fail"] else ("warn" if score["counts"]["warn"] else "success")
    log_decision(
        agent_id=AGENT_ID, decision_type="targeted_check",
        inputs={"asset": asset, "action": user_action, "scope": scope},
        output={"overall_score": score["overall_score"], "counts": score["counts"]},
        metadata={"checks": checks}, status=status,
    )
    writer(_emit("complete", summary=f"Ran {len(checks)} checks on {asset} ({scope}). "
                                     f"Score {score['overall_score']}. Send another instruction to continue."))
    return {}


# ── Routing (conditional edges) ───────────────────────────────────────────────

def entry_route(state: GuardianState) -> str:
    return "act" if state.get("mode") == "act" else "profile"


def after_p_resolve(state: GuardianState) -> str:
    if state.get("error"):
        return "end"
    if not state.get("force") and scan_state.is_unchanged(state["asset"], state.get("version")):
        return "cached"
    return "basic"


def after_p_basic(state: GuardianState) -> str:
    return "end" if state.get("error") else "profile"


def after_a_resolve(state: GuardianState) -> str:
    return "end" if state.get("error") else "translate"


def after_a_translate(state: GuardianState) -> str:
    return "end" if state.get("stop") or state.get("error") else "execute"


# ── Graph assembly ────────────────────────────────────────────────────────────

def build_guardian_graph():
    g = StateGraph(GuardianState)
    g.add_node("p_resolve", p_resolve)
    g.add_node("p_cached", p_cached)
    g.add_node("p_basic", p_basic)
    g.add_node("p_profile", p_profile)
    g.add_node("a_resolve", a_resolve)
    g.add_node("a_translate", a_translate)
    g.add_node("a_execute", a_execute)

    g.set_conditional_entry_point(entry_route, {"profile": "p_resolve", "act": "a_resolve"})

    g.add_conditional_edges("p_resolve", after_p_resolve,
                            {"cached": "p_cached", "basic": "p_basic", "end": END})
    g.add_edge("p_cached", END)
    g.add_conditional_edges("p_basic", after_p_basic, {"profile": "p_profile", "end": END})
    g.add_edge("p_profile", END)

    g.add_conditional_edges("a_resolve", after_a_resolve, {"translate": "a_translate", "end": END})
    g.add_conditional_edges("a_translate", after_a_translate, {"execute": "a_execute", "end": END})
    g.add_edge("a_execute", END)

    return g.compile()


_GRAPH = build_guardian_graph()


# ── Public entry point (same SSE vocabulary as the original agent) ────────────

async def run_quality_guardian(
    asset: str,
    goal: str = "Profile and quality-check this asset",
    *,
    mode: str = "profile",
    user_action: Optional[str] = None,
    profile: Optional[dict] = None,
    confirm: bool = False,
    force: bool = False,
    fields: Optional[list] = None,
    version: Optional[str] = None,
) -> AsyncGenerator[dict, None]:
    """Run the Quality Guardian graph and stream its SSE events.

    mode="profile" → stages A+B+C (basic checks, sample profile, ask).
    mode="act"     → stage D (NL instruction → bounded checks → run → score).

    fields/version: pre-resolved schema from an upstream agent (supervisor blackboard).
    When provided, the resolve nodes skip the catalog describe and reuse them.
    """
    if not asset:
        yield _emit("error", message="No asset/table_name provided")
        return

    initial: GuardianState = {
        "asset": asset, "goal": goal, "mode": mode, "force": force,
        "user_action": user_action, "client_profile": profile, "confirm": confirm,
        "fields": fields, "version": version,
        "stop": False, "error": None,
    }
    try:
        async for event in _GRAPH.astream(initial, stream_mode="custom"):
            yield event
    except Exception as e:
        logger.error(f"[qg-graph] graph run failed: {e}")
        yield _emit("error", message=f"Quality Guardian failed: {e}")
