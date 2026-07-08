"""
Data Analyst — open-ended Q&A over ONE dataset (LangGraph).

Where Source Scout reasons about *which* dataset, the Analyst reasons about
*what's in* a dataset. It answers an arbitrary natural-language question about a
single asset by selecting only the tools the question needs — not a fixed
lineage+quality+SQL pipeline.

Design holes this closes (see the chat critique):
  • tool-SELECTING, not mandatory traversal  → plan_node decides per question
  • NL→SQL is fragile/dangerous              → reuse ask_asset (schema-grounded)
                                                + run_asset (read-only) + LIMIT cap
  • ungrounded answers                        → answer synthesized FROM the rows,
                                                and the SQL is shown for verifiability
  • quality as decoration                     → score woven into the answer as a caveat
  • lineage usually irrelevant                → fetched only for provenance questions
  • cost / result blowup                      → rows capped to the model; blackboard reused

Flow:  resolve → plan → gather(quality, lineage?) → [sql?] → answer

Public entry: run_analyst(asset, question, fields=…, asset_type=…) → SSE event stream.
"""

import asyncio
import json
import logging
import re as _re
from typing import AsyncGenerator, Optional, TypedDict

from langgraph.graph import END, StateGraph
from langgraph.types import StreamWriter

from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY
from routers.scout_chat import _resolve_asset

logger = logging.getLogger(__name__)
AGENT_ID = "data_analyst"

ROW_CAP_MODEL = 50      # rows handed to the LLM for synthesis
ROW_CAP_SQL = 200       # LIMIT injected when the generated SQL has none

# Provenance/trust questions are the only ones that need lineage.
_LINEAGE_CUES = ("where", "come from", "comes from", "source", "upstream", "downstream",
                 "origin", "lineage", "trust", "trustworth", "reliable", "depend")
# Pure structure questions don't need to touch the data at all.
_SCHEMA_ONLY_CUES = ("what columns", "which columns", "schema", "what fields", "structure of",
                     "describe ", "what is in", "data types")


def _emit(event_type: str, **kwargs) -> dict:
    return {"type": event_type, "agent": AGENT_ID, **kwargs}


def _cols(fields: list) -> list:
    """Compact [{name,type}] list for the UI's clickable schema card."""
    return [{"name": f.get("name", ""), "type": f.get("type", "")} for f in (fields or []) if f.get("name")]


def _ensure_limit(sql: str, cap: int) -> str:
    """Inject a LIMIT if the model didn't — a hard cap on what a single answer can scan."""
    s = (sql or "").strip().rstrip(";")
    if not s or _re.search(r"\blimit\b", s, _re.I):
        return s
    return f"{s} LIMIT {cap}"


def _pin_table(sql: str, asset: str) -> str:
    """The Analyst is scoped to ONE known asset, so force the FROM table to it. A model
    that mangles the name (observed: payments→payment) can't break the query this way.
    Subqueries (FROM '(') are left alone."""
    if not sql or not asset:
        return sql
    return _re.sub(r"(\bfrom\s+)([a-zA-Z_][\w.]*)", lambda m: f"{m.group(1)}{asset}",
                   sql, count=1, flags=_re.I)


# ── State ─────────────────────────────────────────────────────────────────────

class AnalystState(TypedDict, total=False):
    asset: str
    question: str
    history: list          # prior conversation turns [{role, content}] for follow-up grounding
    fields: list
    asset_type: Optional[str]
    needs_sql: bool
    needs_lineage: bool
    lineage_depth: int
    sql: str
    rows: list
    columns: list
    row_count: int
    sql_error: Optional[str]
    quality: Optional[dict]
    lineage: Optional[dict]
    error: Optional[str]


# ── Nodes ─────────────────────────────────────────────────────────────────────

async def resolve_node(state: AnalystState, writer: StreamWriter) -> dict:
    """Bind the asset + schema. Reuses the blackboard schema if handed down."""
    asset, question = state["asset"], state.get("question", "")
    writer(_emit("started", asset=asset, question=question))

    fields = state.get("fields")
    atype = state.get("asset_type")
    if fields:
        writer(_emit("context", asset=asset, asset_type=atype, field_count=len(fields),
                     reused=True, columns=_cols(fields)))
        return {"fields": fields, "asset_type": atype}

    # No prefetched schema → take it from the catalog INDEX (fast), not a live Iceberg
    # describe (which round-trips Knox + the REST catalog and costs ~15-20s).
    from routers.scout_chat import _search_assets
    hits = await _search_assets(asset, top_k=1)
    if hits and hits[0].get("fields"):
        h = hits[0]
        norm = [({"name": f.get("name", ""), "type": f.get("type", "")} if isinstance(f, dict) else {"name": str(f), "type": ""}) for f in h["fields"]]
        norm = [f for f in norm if f["name"]]
        if norm:
            atype = h.get("asset_type") or atype
            writer(_emit("context", asset=asset, asset_type=atype, field_count=len(norm), columns=_cols(norm)))
            return {"fields": norm, "asset_type": atype}

    rec = await _resolve_asset(asset)
    if not rec:
        writer(_emit("error", message=f"Could not resolve **{asset}**. Discover it first."))
        return {"error": "no_asset"}
    fields = rec.get("field_objs") or [{"name": f} for f in rec.get("fields", [])]
    atype = rec.get("asset_type") or atype
    writer(_emit("context", asset=asset, asset_type=atype, field_count=len(fields), columns=_cols(fields)))
    return {"fields": fields, "asset_type": atype}


_FULL_LINEAGE_CUES = ("full lineage", "complete lineage", "all upstream", "all downstream",
                      "entire lineage", "end to end", "end-to-end", "impact", "what breaks",
                      "what will break", "who depends", "all dependencies")

async def plan_node(state: AnalystState, writer: StreamWriter) -> dict:
    """Pick ONLY the tools this question needs — no fixed pipeline."""
    q = (state.get("question") or "").lower()
    schema_only = any(c in q for c in _SCHEMA_ONLY_CUES)
    needs_sql = state.get("asset_type") == "iceberg_table" and not schema_only
    needs_lineage = any(c in q for c in _LINEAGE_CUES)

    # Derive traversal depth from the question.
    # Explicit number wins ("5 levels deep"); "full/impact/all" → 6; default → 3.
    depth = 3
    m = _re.search(r'(\d+)\s*(?:level|hop|depth|step)', q)
    if m:
        depth = min(int(m.group(1)), 10)
    elif any(c in q for c in _FULL_LINEAGE_CUES):
        depth = 6

    writer(_emit("plan", needs_sql=needs_sql, needs_lineage=needs_lineage,
                 lineage_depth=depth,
                 skipped=[t for t, on in (("sql", needs_sql), ("lineage", needs_lineage)) if not on],
                 note="only the tools this question needs"))
    return {"needs_sql": needs_sql, "needs_lineage": needs_lineage, "lineage_depth": depth}


async def gather_node(state: AnalystState, writer: StreamWriter) -> dict:
    """Cheap context: cached quality score (always) + lineage (only if asked)."""
    asset = state["asset"]

    quality = None
    try:
        from tools.quality.quality_tools import cached_scorecard
        sc = await asyncio.to_thread(cached_scorecard, asset)
        if not sc:
            from tools.quality import scan_state
            last = await asyncio.to_thread(scan_state.get_last, asset)
            sc = (last or {}).get("basic")
        if sc:
            quality = {"overall_score": sc.get("overall_score"), "counts": sc.get("counts")}
            writer(_emit("quality", asset=asset, cached=True, **quality))
    except Exception as e:
        logger.debug(f"[analyst] quality lookup failed: {e}")

    lineage = None
    if state.get("needs_lineage"):
        writer(_emit("step", name="lineage", status="running"))
        try:
            from tools.openmetadata.client import (
                get_lineage_by_name, async_enrich_lineage_graph, format_lineage_for_llm,
            )
            depth = state.get("lineage_depth") or 3
            atype = "topic" if state.get("asset_type") == "kafka_topic" else "table"
            lin = await asyncio.to_thread(get_lineage_by_name, asset, atype, depth, depth)
            if lin and (lin.get("upstream") or lin.get("downstream") or
                        (lin.get("graph") or {}).get("nodes")):
                enriched = await async_enrich_lineage_graph(lin)
                summary  = format_lineage_for_llm(enriched, asset)
                lineage  = {
                    "upstream":   enriched.get("upstream", []),
                    "downstream": enriched.get("downstream", []),
                    "graph":      enriched.get("graph"),
                    "edge_count": enriched.get("edge_count"),
                    "summary":    summary,
                }
                writer(_emit("lineage", asset=asset,
                             upstream=lineage["upstream"],
                             downstream=lineage["downstream"],
                             edge_count=lineage["edge_count"],
                             depth=depth))
        except Exception as e:
            logger.debug(f"[analyst] lineage lookup failed: {e}")
        writer(_emit("step", name="lineage", status="complete"))

    return {"quality": quality, "lineage": lineage}


async def sql_node(state: AnalystState, writer: StreamWriter) -> dict:
    """NL→SQL (schema-grounded), read-only, LIMIT-capped, executed on Impala."""
    from routers.nl_to_code import AskAssetRequest, ask_asset, RunSQLRequest, run_asset

    asset, question, fields = state["asset"], state["question"], state["fields"]
    writer(_emit("step", name="generate_sql", status="running"))
    try:
        gen = await ask_asset(AskAssetRequest(
            question=question, asset_name=asset, fields=fields,
            asset_type="iceberg_table", engine="impala",
        ))
    except Exception as e:
        writer(_emit("step", name="generate_sql", status="error"))
        return {"sql_error": f"SQL generation failed: {e}"}

    sql = gen.get("sql", "")
    sql = _pin_table(sql, asset)                 # force the correct table (single-asset scope)
    sql = _ensure_limit(sql, ROW_CAP_SQL)
    if not sql:
        writer(_emit("step", name="generate_sql", status="error"))
        return {"sql_error": "Could not form a SQL query for that question."}

    writer(_emit("step", name="run_sql", status="running",
                 detail=" ".join(sql.split())[:90]))
    run = await run_asset(RunSQLRequest(sql=sql, engine="impala"))   # read-only enforced here
    if run.get("error"):
        writer(_emit("sql_result", asset=asset, sql=sql, columns=[], rows=[], error=run["error"]))
        return {"sql": sql, "sql_error": run["error"]}

    rows = run.get("rows", [])
    writer(_emit("sql_result", asset=asset, sql=sql, columns=run.get("columns", []),
                 rows=rows, row_count=run.get("row_count", len(rows))))
    return {"sql": sql, "rows": rows[:ROW_CAP_MODEL],
            "columns": run.get("columns", []), "row_count": run.get("row_count", len(rows))}


async def answer_node(state: AnalystState, writer: StreamWriter) -> dict:
    """Synthesize the answer FROM the evidence (rows/schema/quality) — never free-hand."""
    answer = await _synthesize(state)
    writer(_emit("answer", asset=state["asset"], text=answer,
                 sql=state.get("sql"), grounded=state.get("rows") is not None,
                 quality=state.get("quality")))
    writer(_emit("complete", summary=f"Answered: {(state.get('question') or '')[:60]}"))
    return {}


# ── Synthesis ─────────────────────────────────────────────────────────────────

async def _synthesize(state: AnalystState) -> str:
    import httpx

    question = state.get("question", "")
    rows = state.get("rows")
    quality = state.get("quality") or {}
    lineage = state.get("lineage")
    sql_error = state.get("sql_error")
    history = state.get("history") or []

    evidence: list[str] = []
    if history:
        # Prior turns let the model resolve references ("that", "those regions",
        # "the same but for last month") against what was already asked/answered.
        convo = "\n".join(f"{h.get('role', 'user')}: {h.get('content', '')}" for h in history[-8:])
        evidence.append(f"Conversation so far (for context, do not re-answer):\n{convo}\n")
    evidence += [f"Question: {question}", f"Asset: {state.get('asset')}"]
    if sql_error:
        evidence.append(f"SQL error (no data): {sql_error}")
    elif rows is not None:
        evidence.append(f"SQL: {state.get('sql')}")
        evidence.append(f"Result rows (capped at {ROW_CAP_MODEL}): {json.dumps(rows[:ROW_CAP_MODEL], default=str)[:3500]}")
        evidence.append(f"Total rows: {state.get('row_count')}")
    else:
        cols = [f.get("name") for f in (state.get("fields") or [])]
        evidence.append(f"Columns: {cols}")
    if quality.get("overall_score") is not None:
        evidence.append(f"Data-quality score (cached): {quality.get('overall_score')} · counts={quality.get('counts')}")
    if lineage:
        if lineage.get("summary"):
            evidence.append(f"Lineage graph (enriched, hop-by-hop):\n{lineage['summary']}")
        else:
            evidence.append(
                f"Lineage: upstream={[u.get('name') for u in lineage.get('upstream', [])][:5]} "
                f"downstream={[d.get('name') for d in lineage.get('downstream', [])][:5]}"
            )

    system = (
        "You are a data analyst in an ongoing conversation. Use the 'Conversation so far' only to "
        "interpret the current question (resolve references like 'that' or 'those'); answer the CURRENT "
        "question using ONLY the evidence provided "
        "(query result rows, schema, quality, lineage). Never invent numbers not present in the rows. "
        "If the rows are empty or a SQL error is shown, say you could not answer and why. "
        "Only add a data-quality caveat when a score is provided AND it is below 80 (or a relevant "
        "column is clearly null-heavy); otherwise do not mention quality. "
        "When a lineage graph is provided, reason over it hop by hop using the depth labels "
        "(Hop -N = N hops upstream, Hop +N = N hops downstream). "
        "For impact analysis ('what breaks', 'who depends on this'), list ALL downstream nodes "
        "by hop, including their owners and PII/tier tags. "
        "For provenance questions ('where does this come from'), trace the upstream chain. "
        "Never guess relationships not present in the lineage graph. "
        "Be concise (3-6 sentences for lineage questions, 2-4 for others) and respond in plain English. "
        "Do not restate the SQL — the UI already shows it."
    )
    user = "\n".join(evidence) + "\n\nAnswer:"

    try:
        async with httpx.AsyncClient(timeout=40.0) as client:
            resp = await client.post(
                f"{LLM_BASE_URL}/chat/completions",
                json={"model": LLM_MODEL, "temperature": 0.2, "max_tokens": 400,
                      "messages": [{"role": "system", "content": system},
                                   {"role": "user", "content": user}]},
                headers={"Authorization": f"Bearer {LLM_API_KEY}"},
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.warning(f"[analyst] synthesis failed: {e}")
        if sql_error:
            return f"I couldn't answer that — the query failed: {sql_error}"
        if rows is not None:
            return f"Query returned {state.get('row_count')} rows (see the result below); I couldn't summarize them just now."
        return "I couldn't synthesize an answer just now."


# ── Routing + assembly ────────────────────────────────────────────────────────

def after_resolve(state: AnalystState) -> str:
    return "end" if state.get("error") else "plan"


def after_gather(state: AnalystState) -> str:
    return "sql" if state.get("needs_sql") else "answer"


def build_analyst_graph():
    g = StateGraph(AnalystState)
    g.add_node("resolve", resolve_node)
    g.add_node("plan", plan_node)
    g.add_node("gather", gather_node)
    g.add_node("sql", sql_node)
    g.add_node("answer", answer_node)

    g.set_entry_point("resolve")
    g.add_conditional_edges("resolve", after_resolve, {"plan": "plan", "end": END})
    g.add_edge("plan", "gather")
    g.add_conditional_edges("gather", after_gather, {"sql": "sql", "answer": "answer"})
    g.add_edge("sql", "answer")
    g.add_edge("answer", END)
    return g.compile()


_GRAPH = build_analyst_graph()


# ── Public entry point ────────────────────────────────────────────────────────

async def run_analyst(
    asset: str,
    question: str,
    *,
    fields: Optional[list] = None,
    asset_type: Optional[str] = None,
    history: Optional[list] = None,
) -> AsyncGenerator[dict, None]:
    """Answer an open-ended question about one dataset, streaming SSE events.

    `history` (prior [{role, content}] turns) is used only to interpret the current
    question — it is not re-answered.
    """
    if not asset or not question:
        yield _emit("error", message="asset and question are required")
        return
    initial: AnalystState = {"asset": asset, "question": question, "fields": fields,
                             "asset_type": asset_type, "history": history or []}
    try:
        async for event in _GRAPH.astream(initial, stream_mode="custom"):
            yield event
    except Exception as e:
        logger.error(f"[analyst] graph run failed: {e}")
        yield _emit("error", message=f"Analyst failed: {e}")
