"""
Scout Chat — conversational orchestrator for Source Scout.

A single SSE endpoint that takes a natural-language message (+ optional current
asset context) and streams back typed answer blocks:

  thinking   — status line shown while working
  text       — assistant prose
  assets     — discovered asset cards
  lineage    — upstream/current/downstream from OpenMetadata
  sql_result — generated SQL + executed rows (Impala)
  context    — sets the conversation's current asset
  done       — end of turn

Routing is LLM-classified into: discover | lineage | query | describe | smalltalk.
All heavy lifting reuses existing tools (catalog lists, OM lineage, ask/run SQL).
"""

import asyncio
import json
import logging
import time
from typing import AsyncGenerator, Optional

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/scout", tags=["scout-chat"])


class ChatRequest(BaseModel):
    message: str
    context_asset: Optional[str] = None        # current asset in the conversation
    context_asset_type: Optional[str] = None   # "iceberg_table" | "kafka_topic"


def _sse(block: dict) -> str:
    return f"data: {json.dumps(block)}\n\n"


def _step(label: str, detail: str = "") -> str:
    """A persistent pipeline step shown in the conversation's step trail —
    gives the user visibility into what the agent is doing (and did)."""
    block = {"type": "step", "label": label}
    if detail:
        block["detail"] = detail
    return _sse(block)


def _sql_one_line(sql: str) -> str:
    s = " ".join((sql or "").split())
    return s if len(s) <= 88 else s[:85] + "…"


# ── Provenance trace ──────────────────────────────────────────────────────────
# Per-turn record of every step, tagged by KIND so the UI (and we) can see what
# was decided by the model vs. by deterministic code / governed systems.
#   llm           — a model call (prompt + completion + tokens captured)
#   deterministic — plain code (routing, parsing, ranking, templating)
#   knox          — SQL executed on Cloudera via Knox (Impala) — real data, no model
#   openmetadata  — read/write against the OpenMetadata catalog
import contextvars

_trace_var: contextvars.ContextVar = contextvars.ContextVar("scout_trace", default=None)
_PROMPT_CAP = 6000   # keep SSE payloads sane; UI shows "(truncated)"


def _cap(text: str, n: int = _PROMPT_CAP) -> str:
    text = text or ""
    return text if len(text) <= n else text[:n] + "\n…(truncated)"


class Trace:
    def __init__(self):
        self.spans: list[dict] = []

    def add(self, name: str, kind: str, ms: float, **meta) -> None:
        span = {"name": name, "kind": kind, "ms": round(ms)}
        for k, v in meta.items():
            if v is not None and v != "":
                span[k] = v
        self.spans.append(span)

    def summary(self) -> dict:
        llm = [s for s in self.spans if s["kind"] == "llm"]
        return {
            "llm_calls": len(llm),
            "deterministic_steps": len(self.spans) - len(llm),
            "total_tokens": sum(s.get("tokens", 0) for s in self.spans),
            "total_ms": sum(s.get("ms", 0) for s in self.spans),
        }


def _trace() -> Optional["Trace"]:
    return _trace_var.get()


def _tok(resp) -> Optional[int]:
    """Best-effort token count from a LangChain AIMessage."""
    try:
        um = getattr(resp, "usage_metadata", None)
        if um:
            return um.get("total_tokens") or (um.get("input_tokens", 0) + um.get("output_tokens", 0))
        meta = getattr(resp, "response_metadata", {}) or {}
        tu = meta.get("token_usage") or meta.get("usage") or {}
        return tu.get("total_tokens")
    except Exception:
        return None


ROUTER_PROMPT = """You are the intent router for a Cloudera data-discovery assistant.

Current asset in context: {context}

User message: "{message}"

Respond with ONLY compact JSON, no prose:
{{"intent":"discover|lineage|query|describe|smalltalk","asset":"<asset name or empty>","question":"<analytical question or empty>","keywords":["..."]}}

Rules:
- quality   → user asks about data QUALITY/TRUST ("is X clean", "data quality", "quality score", "how good is X", "is X reliable", "check quality", "is X trustworthy")
- discover  → user wants to FIND data ("find", "show me", "which tables", "what data about X", "discover", "<domain> data")
- lineage   → ONLY when the user explicitly asks about data ORIGIN or FLOW. Must contain one of: "lineage", "come(s) from", "upstream", "downstream", "feeds", "depends on", "what breaks", "impact of changing", "where does <X> get its data". A bare "where" is NOT lineage.
- query     → user wants VALUES/NUMBERS from data ("how many", "average", "top N", "count", "total", "sum", "by <dimension>", "where <subject> <verb>", "which <X> has the most", "show rows")
- describe  → user wants to UNDERSTAND one asset ("what is X", "describe", "schema", "columns", "explain")
- smalltalk → greetings or anything off-topic
- Decision order: if it asks for values/aggregates → query. Else if it explicitly names origin/flow → lineage. Else if it names a domain to find → discover.
- If the user says "it", "this", "that", "the table" → asset is the context asset.
- For query/lineage/describe, set "asset" to the referenced or context asset.
- "question" is the analytical ask in plain English (only for query)."""


async def _classify(message: str, context_asset: Optional[str]) -> dict:
    from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY
    from langchain_openai import ChatOpenAI
    from langchain_core.messages import SystemMessage, HumanMessage

    prompt = ROUTER_PROMPT.format(context=context_asset or "none", message=message)
    t0 = time.monotonic()
    try:
        llm = ChatOpenAI(base_url=LLM_BASE_URL, model=LLM_MODEL, api_key=LLM_API_KEY, temperature=0)
        resp = await llm.ainvoke([SystemMessage(content="You output only compact JSON."),
                                  HumanMessage(content=prompt)])
        txt = resp.content.strip()
        tr = _trace()
        if tr:
            tr.add("Route the question (intent)", "llm", (time.monotonic() - t0) * 1000,
                   model=LLM_MODEL, temperature=0, tokens=_tok(resp),
                   prompt=_cap(prompt), completion=_cap(txt))
        s, e = txt.find("{"), txt.rfind("}") + 1
        data = json.loads(txt[s:e])
        if not data.get("asset") and context_asset:
            data["asset"] = context_asset
        return data
    except Exception as exc:
        logger.warning(f"[chat] classify failed: {exc}")
        tr = _trace()
        if tr:
            tr.add("Route the question (heuristic fallback)", "deterministic",
                   (time.monotonic() - t0) * 1000, note=f"model unavailable: {exc}")
        # Heuristic fallback
        m = message.lower()
        if any(w in m for w in ("lineage", "come from", "upstream", "downstream", "feed", "impact")):
            intent = "lineage"
        elif any(w in m for w in ("how many", "average", "avg", "top", "count", "total", "sum", "by ")):
            intent = "query"
        elif any(w in m for w in ("describe", "schema", "columns", "what is")):
            intent = "describe"
        elif any(w in m for w in ("find", "show", "which", "discover", "data about", "tables")):
            intent = "discover"
        else:
            intent = "smalltalk"
        return {"intent": intent, "asset": context_asset or "", "question": message, "keywords": message.split()}


# ── Catalog helpers (fast, no Qdrant dependency) ──────────────────────────────

async def _load_catalog() -> list[dict]:
    """Return a flat list of all assets: iceberg tables + kafka topics, with fields."""
    import asyncio
    assets: list[dict] = []

    try:
        from tools.iceberg.iceberg_tools import list_iceberg_tables
        tables = await asyncio.to_thread(list_iceberg_tables)
        for t in tables:
            assets.append({
                "name": t.get("name", ""),
                "asset_type": "iceberg_table",
                "fields": [f.get("name", "") for f in t.get("fields", [])],
                "field_objs": t.get("fields", []),
            })
    except Exception as exc:
        logger.debug(f"[chat] iceberg list failed: {exc}")

    try:
        from tools.kafka.kafka_tools import get_all_topics_from_schema_registry
        topics = await asyncio.to_thread(get_all_topics_from_schema_registry)
        for name, info in topics.items():
            clean = name[:-6] if name.endswith("-value") else (name[:-4] if name.endswith("-key") else name)
            assets.append({
                "name": clean,
                "asset_type": "kafka_topic",
                "fields": [f.get("name", "") for f in info.get("fields", [])],
                "field_objs": info.get("fields", []),
            })
    except Exception as exc:
        logger.debug(f"[chat] topic list failed: {exc}")

    return assets


import re as _re

_STOPWORDS = {
    "find", "show", "me", "search", "for", "the", "a", "an", "all", "get", "list",
    "discover", "about", "in", "of", "data", "tables", "table", "topic", "topics",
    "what", "whats", "is", "are", "give", "any",
}


def _tokenize(text: str) -> list[str]:
    """Lowercase, split on non-alphanumerics AND letter/digit boundaries.
    'customer360' and 'customer_360' both → ['customer', '360']."""
    s = text.lower()
    s = _re.sub(r"(?<=[a-z])(?=[0-9])", " ", s)
    s = _re.sub(r"(?<=[0-9])(?=[a-z])", " ", s)
    toks = _re.split(r"[^a-z0-9]+", s)
    return [t for t in toks if t and len(t) >= 2 and t not in _STOPWORDS]


async def _semantic_filter(goal: str, catalog: list[dict]) -> dict[str, str]:
    """LLM semantic discovery — one batched call that understands intent from
    column names ('geolocation' → lat/lon, 'spend' → amount/total). Returns
    {asset_name: reason}. Mirrors the original Source Scout LLM filter."""
    import httpx
    from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY

    compact = [{"name": a["name"], "type": a["asset_type"], "fields": a["fields"]} for a in catalog]
    system_prompt = (
        "You decide which data assets match a user's discovery goal based on their "
        "name and column names. Use your knowledge of what column names commonly represent. "
        'Return strict JSON: {"matches":[{"name":"...","reason":"... cite the columns/name that match"}]}. '
        "An asset matches only if it actually contains the kind of data the user asked for. "
        "Exclude assets that are only tangentially related. Order matches best-first."
    )
    user_prompt = f"User goal: {goal}\n\nAssets:\n{json.dumps(compact, indent=2)}\n\nReturn JSON now."
    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{LLM_BASE_URL}/chat/completions",
                json={
                    "model": LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0.1,
                    "response_format": {"type": "json_object"},
                },
                headers={"Authorization": f"Bearer {LLM_API_KEY}"},
            )
            resp.raise_for_status()
            payload = resp.json()
            content = payload["choices"][0]["message"]["content"]
            content = content.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            tr = _trace()
            if tr:
                tr.add("Match assets to your intent (semantic)", "llm", (time.monotonic() - t0) * 1000,
                       model=LLM_MODEL, temperature=0.1,
                       tokens=(payload.get("usage") or {}).get("total_tokens"),
                       prompt=_cap(f"{system_prompt}\n\n{user_prompt}"), completion=_cap(content))
            parsed = json.loads(content)
            return {m["name"]: m.get("reason", "matched") for m in parsed.get("matches", []) if m.get("name")}
    except Exception as e:
        logger.warning(f"[chat] semantic filter failed ({e}); will fall back to keyword")
        return {}


def _keyword_rank(catalog: list[dict], query_text: str, limit: int = 8) -> list[dict]:
    """Rank assets by token overlap. Name-token matches dominate field matches."""
    qtoks = set(_tokenize(query_text))
    if not qtoks:
        return catalog[:limit]

    scored = []
    for a in catalog:
        ntoks = set(_tokenize(a["name"]))
        ftoks: set[str] = set()
        for f in a["fields"]:
            ftoks |= set(_tokenize(f))

        score = 0
        for q in qtoks:
            if q in ntoks:                                        # exact name token
                score += 3
            elif any(q in nt or nt in q for nt in ntoks):         # partial name token
                score += 2
            elif q in ftoks:                                      # field-level match
                score += 1
        if score > 0:
            scored.append((score, a))

    scored.sort(key=lambda x: -x[0])
    return [a for _, a in scored[:limit]]


def _find_asset(catalog: list[dict], name: str) -> Optional[dict]:
    if not name:
        return None
    nl = name.lower()
    short = nl.split(".")[-1]
    for a in catalog:
        if a["name"].lower() == nl:
            return a
    for a in catalog:
        if a["name"].lower().split(".")[-1] == short:
            return a
    for a in catalog:
        if short in a["name"].lower():
            return a
    return None


def _extract_asset_from_message(catalog: list[dict], message: str) -> str:
    """Scan the raw message for any catalog asset name (full or short). Most specific wins."""
    m = message.lower()
    best = ""
    for a in catalog:
        full = a["name"].lower()
        short = full.split(".")[-1]
        if full in m and len(full) > len(best):
            best = a["name"]
        elif short in m and len(short) > len(best):
            best = a["name"]
    return best


_ROW_VERBS = (
    "sample", "preview", "rows", "show data", "show me data", "show me the data",
    "data in", "data from", "records", "select ", "first ", "head", "few rows",
    "some data", "show rows", "browse",
)
_AGG_WORDS = ("average", "avg", "count", "sum", "total", "group", " by ", "top ",
              "max", "min", "distinct", "most", "least", "per ")


def _wants_rows(message: str) -> bool:
    """True if the user is asking to see raw rows/sample data (not an aggregate)."""
    m = message.lower()
    return any(w in m for w in _ROW_VERBS)


def _is_plain_sample(question: str) -> bool:
    """A 'just show me the data' request with no aggregation."""
    q = question.lower()
    return _wants_rows(question) and not any(w in q for w in _AGG_WORDS)


_QUALITY_VERBS = ("clean", "data quality", "quality score", "quality of", "is it clean",
                  "trustworth", "reliable", "how good", "dq ", "data trust", "quality check")


def _wants_quality(message: str) -> bool:
    m = message.lower()
    return any(w in m for w in _QUALITY_VERBS)


# ── Route handlers (each yields SSE blocks) ───────────────────────────────────

async def _handle_discover(message: str, cls: dict, catalog: Optional[list] = None) -> AsyncGenerator[str, None]:
    if catalog is None:
        catalog = await _load_catalog()
    by_name = {a["name"]: a for a in catalog}

    yield _step("Scanning your catalog", f"{len(catalog)} assets")
    yield _step("Matching your intent with the model")
    # Primary: LLM semantic match (understands intent, not just keywords)
    reasons = await _semantic_filter(message, catalog)
    matched = [by_name[n] for n in reasons if n in by_name]

    # Fallback: keyword ranking if the LLM returned nothing
    used_semantic = bool(matched)
    if not matched:
        matched = _keyword_rank(catalog, message, limit=8)

    if not matched:
        yield _sse({"type": "text", "text": f"I couldn't find assets matching “{message}”. Try a broader term, or name a domain like *payments*, *customers*, or *fraud*."})
        return

    matched = matched[:10]
    cards = [{
        "name": m["name"],
        "asset_type": m["asset_type"],
        "field_count": len(m["fields"]),
        "fields": m["fields"][:6],
        "reason": reasons.get(m["name"], "") if used_semantic else "",
    } for m in matched]

    lead = (f"Found {len(cards)} asset{'s' if len(cards) != 1 else ''} that match your intent. "
            "Click one to trace its lineage, or ask a question about it.") if used_semantic else \
           (f"Found {len(cards)} asset{'s' if len(cards) != 1 else ''}. Click one to explore.")
    yield _sse({"type": "text", "text": lead})
    yield _sse({"type": "assets", "assets": cards})


async def _handle_lineage(cls: dict) -> AsyncGenerator[str, None]:
    asset = cls.get("asset", "")
    if not asset:
        yield _sse({"type": "text", "text": "Which asset's lineage would you like to see?"})
        return

    yield _step(f"Looking up {asset} in OpenMetadata")

    import asyncio
    from tools.openmetadata.client import get_lineage_by_name
    yield _step("Tracing upstream sources & downstream consumers")
    # try table then topic
    _t = time.monotonic()
    result = await asyncio.to_thread(get_lineage_by_name, asset, "table")
    if not result or not (result.get("upstream") or result.get("downstream")):
        topic_res = await asyncio.to_thread(get_lineage_by_name, asset, "topic")
        if topic_res and (topic_res.get("upstream") or topic_res.get("downstream")):
            result = topic_res
    if (tr := _trace()):
        tr.add("Fetch lineage from OpenMetadata", "openmetadata", (time.monotonic() - _t) * 1000,
               note=f"{(result or {}).get('edge_count', 0)} edges in graph")

    if not result:
        yield _sse({"type": "text", "text": f"I couldn't find **{asset}** in OpenMetadata. It may not be registered yet."})
        return

    up = result.get("upstream", [])
    down = result.get("downstream", [])
    yield _sse({"type": "context", "asset": asset, "asset_type": "iceberg_table"})

    summary = f"**{asset}** has {len(up)} upstream source{'s' if len(up)!=1 else ''} and {len(down)} downstream consumer{'s' if len(down)!=1 else ''}."
    if down:
        summary += " Changes here would affect: " + ", ".join(f"`{n['name']}`" for n in down[:4]) + "."
    yield _sse({"type": "text", "text": summary})
    yield _sse({"type": "lineage", "asset": asset,
                "upstream": up, "downstream": down,
                "graph": result.get("graph", {"nodes": [], "edges": []}),
                "edge_count": result.get("edge_count", 0)})


async def _handle_query(cls: dict, catalog: Optional[list] = None) -> AsyncGenerator[str, None]:
    asset = cls.get("asset", "")
    question = cls.get("question") or ""
    if not asset:
        yield _sse({"type": "text", "text": "Which table should I query? Name one and I'll run it."})
        return

    if catalog is None:
        catalog = await _load_catalog()
    a = _find_asset(catalog, asset)
    if not a:
        yield _sse({"type": "text", "text": f"I don't see **{asset}** in the catalog. Try discovering it first."})
        return
    if a["asset_type"] != "iceberg_table":
        yield _sse({"type": "text", "text": f"**{asset}** is a Kafka topic — live streams are queried with Flink, not batch SQL. I can run SQL on Iceberg tables."})
        return

    yield _sse({"type": "context", "asset": a["name"], "asset_type": "iceberg_table"})

    from routers.nl_to_code import AskAssetRequest, ask_asset, RunSQLRequest, run_asset

    # Plain "show me sample data / rows" → deterministic SELECT *, no LLM needed.
    if _is_plain_sample(question):
        yield _step("Building a preview query", f"SELECT * FROM {a['name']} LIMIT 10")
        sql = f"SELECT * FROM {a['name']} LIMIT 10"
        if (tr := _trace()):
            tr.add("Build preview query (template, no model)", "deterministic", 0, completion=sql)
    else:
        yield _step(f"Reading the schema for {a['name']}", f"{len(a['field_objs'])} columns")
        yield _step("Generating SQL from your question with the model")
        from config import LLM_MODEL
        _t = time.monotonic()
        gen = await ask_asset(AskAssetRequest(
            question=question, asset_name=a["name"], fields=a["field_objs"],
            asset_type="iceberg_table", engine="impala",
        ))
        sql = gen.get("sql", "")
        if (tr := _trace()):
            tr.add("Generate SQL (NL→SQL)", "llm", (time.monotonic() - _t) * 1000,
                   model=LLM_MODEL, tokens=gen.get("tokens"),
                   prompt=_cap(f"Question: {question}\nTable: {a['name']} "
                               f"({len(a['field_objs'])} columns) — full prompt assembled in nl_to_code"),
                   completion=_cap(sql))
        if not sql:
            yield _sse({"type": "text", "text": "I couldn't form a SQL query for that. Try rephrasing."})
            return
        yield _step("Validating the query is read-only", _sql_one_line(sql))

    yield _step("Running on Cloudera · Impala via Knox", "no data leaves the platform")
    _t = time.monotonic()
    run = await run_asset(RunSQLRequest(sql=sql, engine="impala"))
    run_ms = (time.monotonic() - _t) * 1000
    if (tr := _trace()):
        tr.add("Run SQL on Cloudera (Impala via Knox)", "knox", run_ms,
               note=f"{run.get('row_count', 0)} rows · no data left the platform")

    if run.get("error"):
        yield _sse({"type": "text", "text": f"Generated the query but it failed to run: {run['error']}"})
        yield _sse({"type": "sql_result", "asset": a["name"], "sql": sql, "columns": [], "rows": [], "error": run["error"]})
        return

    rc = run.get("row_count", 0)
    yield _sse({"type": "text", "text": f"Here's the result — {rc} row{'s' if rc != 1 else ''} from `{a['name']}` on Impala."})
    yield _sse({"type": "sql_result", "asset": a["name"], "sql": sql,
                "columns": run.get("columns", []), "rows": run.get("rows", []),
                "row_count": rc, "executed_on": run.get("executed_on", "impala")})

    # Enrich OpenMetadata with usage — query history + popularity (governance compounds).
    yield _step("Recording the query & usage in OpenMetadata")
    try:
        from tools.openmetadata.client import record_query_and_usage
        _t = time.monotonic()
        await asyncio.to_thread(record_query_and_usage, a["name"], sql, run_ms)
        if (tr := _trace()):
            tr.add("Record query + usage in OpenMetadata", "openmetadata",
                   (time.monotonic() - _t) * 1000, note="query history + popularity")
    except Exception as _ue:
        logger.debug(f"[chat] usage write skipped: {_ue}")

    # Trust caveat — quality travels with the answer, but only when it matters.
    try:
        from tools.quality.quality_tools import quality_trend
        t = await asyncio.to_thread(quality_trend, a["name"])
        if t and (t["direction"] == "down" or t["level"] in ("fair", "poor")):
            arrow = "↓" if t["direction"] == "down" else "•"
            yield _sse({"type": "caveat", "asset": a["name"], "level": t["level"],
                        "direction": t["direction"],
                        "text": (f"Data trust — {a['name']} quality is {t['current']}/100 {arrow} "
                                 f"({'down' if t['direction']=='down' else 'flat'} from {t['baseline']} over "
                                 f"{t['window_days']} days). Driver: {t['driver']}. Treat aggregates with care.")})
    except Exception as _ce:
        logger.debug(f"[chat] caveat skipped: {_ce}")


async def _handle_quality(cls: dict, catalog: Optional[list] = None) -> AsyncGenerator[str, None]:
    asset = cls.get("asset", "")
    if not asset:
        yield _sse({"type": "text", "text": "Which asset should I check the quality of?"})
        return
    if catalog is None:
        catalog = await _load_catalog()
    a = _find_asset(catalog, asset)
    if not a or a["asset_type"] != "iceberg_table":
        yield _sse({"type": "text", "text": f"I can run quality checks on Iceberg tables. **{asset}** isn't one I can check."})
        return

    import asyncio
    from tools.quality.quality_tools import (
        run_quality_check, quality_trend, write_quality_to_om, write_quality_testcases_to_om,
        suggest_dq_rules_with_lineage,
    )

    yield _sse({"type": "context", "asset": a["name"], "asset_type": "iceberg_table"})
    yield _step(f"Profiling {a['name']}", "one cohesive quality query")
    yield _step("Running on Cloudera · Impala via Knox", "no data leaves the platform")
    _t = time.monotonic()
    result = await asyncio.to_thread(run_quality_check, a["name"], a["field_objs"])
    if (tr := _trace()):
        tr.add("Profile data on Cloudera (Impala via Knox)", "knox", (time.monotonic() - _t) * 1000,
               note="one cohesive DQ query · no data left the platform")

    yield _step("Scoring completeness, uniqueness & business rules")
    _t = time.monotonic()
    trend = await asyncio.to_thread(quality_trend, a["name"])
    if (tr := _trace()):
        tr.add("Score & compute 14-day trend", "deterministic", (time.monotonic() - _t) * 1000)

    # write the score back to OpenMetadata (governance loop)
    yield _step("Writing the profile back to OpenMetadata")
    _t = time.monotonic()
    written = await asyncio.to_thread(write_quality_to_om, a["name"], result)
    if (tr := _trace()):
        tr.add("Write table profile to OpenMetadata", "openmetadata", (time.monotonic() - _t) * 1000)

    # promote the checks to OM's native Data Quality tab (Test Cases + results)
    yield _step("Publishing checks to the OpenMetadata Data Quality tab")
    _t = time.monotonic()
    n_tc = await asyncio.to_thread(write_quality_testcases_to_om, a["name"], result)
    if (tr := _trace()):
        tr.add("Write Test Cases to OpenMetadata", "openmetadata", (time.monotonic() - _t) * 1000,
               note=f"{n_tc} test results")

    # 1-hop upstream root-cause probe (the agentic seam)
    yield _step("Tracing one hop upstream for root cause")
    _t = time.monotonic()
    rc = await suggest_dq_rules_with_lineage(a["name"], a["field_objs"])
    root_cause = rc.get("root_cause")
    if (tr := _trace()):
        tr.add("Trace 1 hop upstream for root cause", "openmetadata", (time.monotonic() - _t) * 1000)

    score = result["overall_score"]
    c = result["counts"]
    summary = (f"**{a['name']}** scores **{score}/100** — "
               f"{c['pass']} pass, {c['warn']} warn, {c['fail']} fail.")
    if trend and trend["direction"] == "down":
        summary += f" Quality is **trending down** ({trend['baseline']}→{trend['current']} over {trend['window_days']} days), driven by {trend['driver']}."
    elif trend and trend["direction"] == "up":
        summary += f" Quality is improving ({trend['baseline']}→{trend['current']})."
    if root_cause:
        summary += f" Likely root cause is upstream: **{root_cause['asset']}** is also degrading ({root_cause['delta']})."
    if written:
        summary += " Score written to OpenMetadata."
    yield _sse({"type": "text", "text": summary})

    yield _sse({
        "type": "quality", "asset": a["name"],
        "overall_score": score, "counts": c, "checks": result["checks"],
        "total_rows": result["total_rows"],
        "trend": trend, "root_cause": root_cause, "written_to_om": written,
    })


async def _handle_describe(cls: dict, catalog: Optional[list] = None) -> AsyncGenerator[str, None]:
    asset = cls.get("asset", "")
    if not asset:
        yield _sse({"type": "text", "text": "Which asset would you like me to describe?"})
        return
    if catalog is None:
        catalog = await _load_catalog()
    a = _find_asset(catalog, asset)
    if not a:
        yield _sse({"type": "text", "text": f"I don't see **{asset}** in the catalog."})
        return

    yield _sse({"type": "context", "asset": a["name"], "asset_type": a["asset_type"]})
    field_objs = a["field_objs"]
    lines = ", ".join(f["name"] for f in field_objs[:12])
    kind = "Iceberg table" if a["asset_type"] == "iceberg_table" else "Kafka topic"
    yield _sse({"type": "text",
                "text": f"**{a['name']}** is a {kind} with {len(field_objs)} field{'s' if len(field_objs)!=1 else ''}: {lines}."})
    yield _sse({"type": "schema", "asset": a["name"], "asset_type": a["asset_type"], "fields": field_objs})


async def _handle_smalltalk(message: str) -> AsyncGenerator[str, None]:
    yield _sse({"type": "text", "text": "I'm Source Scout — I help you discover data across your Cloudera platform, trace its lineage, and answer questions by running SQL. Try: *“find payment data”*, *“where does customer_360 come from”*, or *“top 5 merchants by amount”*."})


# ── Endpoint ──────────────────────────────────────────────────────────────────

async def _stream(req: ChatRequest) -> AsyncGenerator[str, None]:
    t0 = time.monotonic()
    trace = Trace()
    _trace_var.set(trace)
    try:
        # First byte out immediately — the UI shows life before any model/IO work.
        yield _sse({"type": "thinking", "text": "Working on it…"})

        msg = req.message

        # 1) Cheap, text-only verb detection — no model, no catalog needed.
        det_intent = "quality" if _wants_quality(msg) else ("query" if _wants_rows(msg) else None)

        # 2) Load the catalog and (only if a deterministic verb didn't already
        #    decide the route) run the LLM router IN PARALLEL. They're independent,
        #    so wall-clock is max(classify, catalog) — not the sum.
        t_cat = time.monotonic()
        catalog_task = asyncio.create_task(_load_catalog())
        classify_task = (
            asyncio.create_task(_classify(msg, req.context_asset)) if det_intent is None else None
        )

        catalog = await catalog_task
        trace.add("Load catalog (Iceberg + Kafka)", "deterministic",
                  (time.monotonic() - t_cat) * 1000, note=f"{len(catalog)} assets")
        named = _extract_asset_from_message(catalog, msg)

        # 3) Resolve intent. Deterministic verb + a real asset → skip the LLM entirely.
        if det_intent and (named or req.context_asset):
            intent = det_intent
            cls = {"intent": intent, "asset": named or req.context_asset or "",
                   "question": msg, "keywords": []}
            route_via = "deterministic"
            trace.add("Route the question (deterministic verb match)", "deterministic", 0,
                      note=f"intent={intent} · model not used")
        else:
            if classify_task is None:        # verb matched but no asset → we do need the router
                classify_task = asyncio.create_task(_classify(msg, req.context_asset))
            cls = await classify_task
            intent = cls.get("intent", "smalltalk")
            route_via = "llm"
            # Safety net: keep the original take-control override.
            if _wants_quality(msg) and (named or req.context_asset):
                intent = cls["intent"] = "quality"
                cls["asset"] = named or req.context_asset or ""
            elif _wants_rows(msg) and (named or req.context_asset):
                intent = cls["intent"] = "query"
                cls["asset"] = named or req.context_asset or ""
                cls["question"] = cls.get("question") or msg

        # 4) Robust asset resolution: trust LLM, else scan message, else context.
        if intent in ("lineage", "query", "describe", "quality"):
            resolved = cls.get("asset") or ""
            if not _find_asset(catalog, resolved):
                resolved = named or req.context_asset or resolved
            cls["asset"] = resolved

        logger.info(f"[chat] routed intent={intent} via={route_via} "
                    f"asset={cls.get('asset')!r} in {(time.monotonic()-t0)*1000:.0f}ms msg={msg!r}")

        if intent == "discover":
            async for b in _handle_discover(msg, cls, catalog):
                yield b
        elif intent == "lineage":
            async for b in _handle_lineage(cls):
                yield b
        elif intent == "query":
            async for b in _handle_query(cls, catalog):
                yield b
        elif intent == "quality":
            async for b in _handle_quality(cls, catalog):
                yield b
        elif intent == "describe":
            async for b in _handle_describe(cls, catalog):
                yield b
        else:
            async for b in _handle_smalltalk(msg):
                yield b

        logger.info(f"[chat] turn complete intent={intent} in {(time.monotonic()-t0)*1000:.0f}ms")
        yield _sse({"type": "provenance", "spans": trace.spans, "summary": trace.summary()})
        yield _sse({"type": "done"})
    except Exception as exc:
        logger.exception("[chat] stream failed")
        yield _sse({"type": "text", "text": f"Something went wrong: {exc}"})
        yield _sse({"type": "done"})


@router.post("/chat")
async def chat(req: ChatRequest):
    return StreamingResponse(
        _stream(req),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
