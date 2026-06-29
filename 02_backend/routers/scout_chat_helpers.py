"""
Tool implementations for the Scout ReAct agent.

Each function is called by a @tool wrapper in scout_chat.py.
They return _pack(summary, blocks) — summary for the LLM, blocks for SSE emission.
"""

import asyncio
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _pack(summary: str, blocks: list) -> str:
    return json.dumps({"result": summary, "_blocks": blocks})


# ── catalog_search ─────────────────────────────────────────────────────────────

async def do_catalog_search(query: str, asset_type: str = "") -> str:
    from tools.catalog import catalog_store

    atype = asset_type.lower() if asset_type else None
    if atype in ("table", "iceberg", "iceberg_table"):
        atype = "iceberg_table"
    elif atype in ("topic", "kafka", "kafka_topic", "stream"):
        atype = "kafka_topic"
    else:
        atype = None

    # Try semantic index first
    try:
        stats = await asyncio.to_thread(catalog_store.get_stats)
        if stats.get("available"):
            hits = await asyncio.to_thread(catalog_store.search, query, [atype] if atype else None, 12)
            if hits:
                cards = _hits_to_cards(hits)
                summary = f"Found {len(cards)} assets matching '{query}': {', '.join(c['name'] for c in cards[:5])}"
                return _pack(summary, [{"type": "assets", "assets": cards}])
    except Exception as e:
        logger.debug(f"[helper] catalog index unavailable: {e}")

    # Fallback: full catalog keyword match
    catalog = await _load_catalog(atype)
    matched = _keyword_rank(catalog, query)[:10]
    if not matched:
        return _pack(f"No assets found matching '{query}'.", [])

    cards = [{
        "name": a["name"], "asset_type": a["asset_type"],
        "field_count": len(a["fields"]), "fields": a["fields"][:6], "reason": "",
    } for a in matched]
    summary = f"Found {len(cards)} assets: {', '.join(c['name'] for c in cards[:5])}"
    return _pack(summary, [{"type": "assets", "assets": cards}])


def _hits_to_cards(hits: list) -> list:
    return [{
        "name": h.get("name", ""),
        "asset_type": h.get("asset_type", "iceberg_table"),
        "field_count": h.get("field_count", 0),
        "fields": (h.get("field_names") or [])[:6],
        "reason": "",
    } for h in hits if h.get("name")]


async def _load_catalog(atype: Optional[str] = None) -> list:
    assets = []
    try:
        from tools.iceberg.iceberg_tools import list_iceberg_tables
        for t in await asyncio.to_thread(list_iceberg_tables):
            assets.append({"name": t["name"], "asset_type": "iceberg_table",
                           "fields": [f.get("name", "") for f in t.get("fields", [])]})
    except Exception:
        pass
    try:
        from tools.kafka.kafka_tools import get_all_topics_from_schema_registry
        for name, info in (await asyncio.to_thread(get_all_topics_from_schema_registry)).items():
            clean = name[:-6] if name.endswith("-value") else (name[:-4] if name.endswith("-key") else name)
            assets.append({"name": clean, "asset_type": "kafka_topic",
                           "fields": [f.get("name", "") for f in info.get("fields", [])]})
    except Exception:
        pass
    if atype:
        assets = [a for a in assets if a["asset_type"] == atype]
    return assets


import re as _re
_STOPWORDS = {"find","show","me","search","for","the","a","an","all","get","list","discover",
              "about","in","of","data","tables","table","topic","topics","what","whats",
              "is","are","give","any","assets"}

def _tokenize(text: str) -> list:
    s = text.lower()
    s = _re.sub(r"(?<=[a-z])(?=[0-9])", " ", s)
    s = _re.sub(r"(?<=[0-9])(?=[a-z])", " ", s)
    return [t for t in _re.split(r"[^a-z0-9]+", s) if t and len(t) >= 2 and t not in _STOPWORDS]

def _keyword_rank(catalog: list, query: str, limit: int = 8) -> list:
    qtoks = set(_tokenize(query))
    if not qtoks:
        return catalog[:limit]
    scored = []
    for a in catalog:
        ntoks = set(_tokenize(a["name"]))
        ftoks = {t for f in a["fields"] for t in _tokenize(f)}
        score = sum(3 if q in ntoks else (2 if any(q in n or n in q for n in ntoks) else (1 if q in ftoks else 0)) for q in qtoks)
        if score > 0:
            scored.append((score, a))
    scored.sort(key=lambda x: -x[0])
    return [a for _, a in scored[:limit]]


# ── asset_lineage ─────────────────────────────────────────────────────────────

async def _canonical_asset_name(name: str) -> Optional[str]:
    """OpenMetadata only matches exact names. The catalog (Qdrant) tolerates typos,
    so resolve a user-typed name to the closest real asset name before giving up."""
    try:
        hits = await _search_assets(name, top_k=1)
        if hits and hits[0]["name"].lower() != name.lower():
            return hits[0]["name"]
    except Exception as e:
        logger.debug(f"[helper] canonical resolve failed: {e}")
    return None


async def _lineage_lookup(asset_name: str, depth: int):
    """Return an OM lineage result with nodes, trying table then topic, or None."""
    from tools.openmetadata.client import get_lineage_by_name
    result = await asyncio.to_thread(get_lineage_by_name, asset_name, "table", depth, depth)
    if result and (result.get("graph") or {}).get("nodes"):
        return result
    topic_result = await asyncio.to_thread(get_lineage_by_name, asset_name, "topic", depth, depth)
    if topic_result and (topic_result.get("graph") or {}).get("nodes"):
        return topic_result
    return result  # may be a no-edges result or None


async def do_asset_lineage(asset_name: str, depth: int = 3) -> str:
    from tools.openmetadata.client import async_enrich_lineage_graph, format_lineage_for_llm

    depth = min(max(depth, 1), 6)
    resolved = asset_name

    result = await _lineage_lookup(asset_name, depth)

    # No graph for the typed name → resolve to the closest real catalog name and retry.
    if not result or not ((result.get("graph") or {}).get("nodes")):
        canonical = await _canonical_asset_name(asset_name)
        if canonical:
            retry = await _lineage_lookup(canonical, depth)
            if retry and (retry.get("graph") or {}).get("nodes"):
                result, resolved = retry, canonical

    if not result:
        return _pack(f"'{asset_name}' not found in OpenMetadata. It may not be registered yet.", [])

    nodes = (result.get("graph") or {}).get("nodes", [])
    if not nodes:
        return _pack(f"'{resolved}' is registered in OpenMetadata but has no lineage edges yet.", [])

    enriched = await async_enrich_lineage_graph(result)
    summary_text = format_lineage_for_llm(enriched, resolved)

    block = {
        "type":       "lineage",
        "asset":      resolved,
        "upstream":   enriched.get("upstream", []),
        "downstream": enriched.get("downstream", []),
        "graph":      enriched.get("graph", {"nodes": [], "edges": []}),
        "edge_count": enriched.get("edge_count", 0),
    }

    up   = enriched.get("upstream", [])
    down = enriched.get("downstream", [])
    graph_nodes = (enriched.get("graph") or {}).get("nodes", [])
    resolved_note = f" (resolved from '{asset_name}')" if resolved != asset_name else ""
    llm_summary = (
        f"Lineage for '{resolved}'{resolved_note}: {len(graph_nodes)} nodes, {enriched.get('edge_count', 0)} edges. "
        f"Direct upstream ({len(up)}): {', '.join(n['name'] for n in up[:4])}. "
        f"Direct downstream ({len(down)}): {', '.join(n['name'] for n in down[:4]) or 'none'}.\n"
        f"Full hop-by-hop:\n{summary_text}"
    )

    # Also set context so the UI knows the current asset
    blocks = [
        {"type": "context", "asset": resolved, "asset_type": "iceberg_table"},
        block,
    ]
    return _pack(llm_summary, blocks)


# ── asset_schema ──────────────────────────────────────────────────────────────

async def do_asset_schema(asset_name: str) -> str:
    # Try Iceberg first
    try:
        from tools.iceberg.iceberg_tools import describe_iceberg_table
        meta = await asyncio.to_thread(describe_iceberg_table, asset_name)
        if meta and meta.get("fields"):
            fields = meta["fields"]
            summary = f"Schema for '{asset_name}': {len(fields)} fields — {', '.join(f['name'] for f in fields[:8])}"
            blocks = [
                {"type": "context", "asset": asset_name, "asset_type": "iceberg_table"},
                {"type": "schema", "asset": asset_name, "asset_type": "iceberg_table", "fields": fields},
            ]
            return _pack(summary, blocks)
    except Exception:
        pass

    # Try Kafka topic
    try:
        from tools.kafka.kafka_tools import get_topic_schema_from_registry
        topic = await asyncio.to_thread(get_topic_schema_from_registry, asset_name)
        if topic and topic.get("fields"):
            fields = topic["fields"]
            summary = f"Schema for topic '{asset_name}': {len(fields)} fields — {', '.join(f['name'] for f in fields[:8])}"
            blocks = [
                {"type": "context", "asset": asset_name, "asset_type": "kafka_topic"},
                {"type": "schema", "asset": asset_name, "asset_type": "kafka_topic", "fields": fields},
            ]
            return _pack(summary, blocks)
    except Exception:
        pass

    # Fallback: search catalog
    from tools.catalog import catalog_store
    try:
        hits = await asyncio.to_thread(catalog_store.search, asset_name, None, 3)
        for h in hits:
            if h.get("name", "").lower() == asset_name.lower() or asset_name.lower() in h.get("name", "").lower():
                fields = [{"name": n} for n in (h.get("field_names") or [])]
                summary = f"Schema for '{h['name']}': {len(fields)} fields — {', '.join(f['name'] for f in fields[:8])}"
                blocks = [
                    {"type": "context", "asset": h["name"], "asset_type": h.get("asset_type", "iceberg_table")},
                    {"type": "schema", "asset": h["name"], "asset_type": h.get("asset_type", "iceberg_table"), "fields": fields},
                ]
                return _pack(summary, blocks)
    except Exception:
        pass

    return _pack(f"Could not find schema for '{asset_name}'. Try discovering it first.", [])


# ── query_asset ───────────────────────────────────────────────────────────────

async def do_query_asset(asset_name: str, question: str) -> str:
    from routers.nl_to_code import AskAssetRequest, ask_asset, RunSQLRequest, run_asset
    import re

    # Resolve schema
    try:
        from tools.iceberg.iceberg_tools import describe_iceberg_table
        meta = await asyncio.to_thread(describe_iceberg_table, asset_name)
        fields = meta.get("fields", []) if meta else []
    except Exception:
        fields = []

    if not fields:
        return _pack(f"Could not resolve schema for '{asset_name}' — cannot generate SQL.", [])

    # Generate SQL
    try:
        gen = await ask_asset(AskAssetRequest(
            question=question, asset_name=asset_name, fields=fields,
            asset_type="iceberg_table", engine="impala",
        ))
    except Exception as e:
        return _pack(f"SQL generation failed: {e}", [])

    sql = gen.get("sql", "")
    if not sql:
        return _pack("Could not generate a SQL query for that question.", [])

    # Cap rows
    if not re.search(r"\blimit\b", sql, re.I):
        sql = sql.rstrip(";") + " LIMIT 200"

    # Execute
    try:
        run = await run_asset(RunSQLRequest(sql=sql, engine="impala"))
    except Exception as e:
        return _pack(f"SQL execution failed: {e}", [{"type": "sql_result", "asset": asset_name, "sql": sql, "columns": [], "rows": [], "error": str(e)}])

    if run.get("error"):
        blocks = [{"type": "sql_result", "asset": asset_name, "sql": sql, "columns": [], "rows": [], "error": run["error"]}]
        return _pack(f"Query ran but returned an error: {run['error']}", blocks)

    rows = run.get("rows", [])
    cols = run.get("columns", [])
    summary = f"Query on '{asset_name}' returned {run.get('row_count', len(rows))} rows. SQL: {sql}"
    blocks = [
        {"type": "context", "asset": asset_name, "asset_type": "iceberg_table"},
        {"type": "sql_result", "asset": asset_name, "sql": sql,
         "columns": cols, "rows": rows[:50], "row_count": run.get("row_count", len(rows)),
         "executed_on": run.get("executed_on", "impala")},
    ]
    return _pack(summary, blocks)


# ── data_quality ──────────────────────────────────────────────────────────────

async def do_data_quality(asset_name: str) -> str:
    from tools.quality.quality_tools import run_quality_check, quality_trend, suggest_dq_rules_with_lineage

    # Resolve schema
    try:
        from tools.iceberg.iceberg_tools import describe_iceberg_table
        meta = await asyncio.to_thread(describe_iceberg_table, asset_name)
        fields = meta.get("fields", []) if meta else []
    except Exception:
        fields = []

    if not fields:
        return _pack(f"Could not resolve '{asset_name}' as an Iceberg table for quality checks.", [])

    try:
        result = await asyncio.to_thread(run_quality_check, asset_name, fields, False)
        trend  = await asyncio.to_thread(quality_trend, asset_name)
    except Exception as e:
        return _pack(f"Quality check failed: {e}", [])

    score = result.get("overall_score", 0)
    c = result.get("counts", {})

    # When there's an actual problem (failures or a downward trend), walk lineage
    # upstream to find which source asset is the likely root cause — the whole point
    # of having lineage: fix the issue where it originates, not where it surfaces.
    root_cause = None
    has_issue = c.get("fail", 0) > 0 or (trend and trend.get("direction") == "down")
    if has_issue:
        try:
            probe = await suggest_dq_rules_with_lineage(asset_name, fields)
            root_cause = probe.get("root_cause")
        except Exception as e:
            logger.debug(f"[helper] lineage root-cause probe skipped: {e}")

    summary = (
        f"Data quality for '{asset_name}': {score}/100. "
        f"{c.get('pass', 0)} pass, {c.get('warn', 0)} warn, {c.get('fail', 0)} fail."
    )
    if trend and trend.get("direction") == "down":
        summary += f" Trending DOWN ({trend['baseline']}→{trend['current']})."
    if root_cause:
        summary += (
            f" Likely root cause is upstream table '{root_cause['asset']}' "
            f"(also trending {root_cause['direction']}, {root_cause['driver']}). "
            f"Recommend fixing the issue at that source."
        )

    blocks = [
        {"type": "context", "asset": asset_name, "asset_type": "iceberg_table"},
        {"type": "quality", "asset": asset_name,
         "overall_score": score, "counts": c,
         "checks": result.get("checks", []),
         "total_rows": result.get("total_rows", 0),
         "trend": trend, "root_cause": root_cause, "written_to_om": False},
    ]
    return _pack(summary, blocks)


# ── build_pipeline ────────────────────────────────────────────────────────────

_SINK_ALIASES = {
    "iceberg": "adls_iceberg", "adls_iceberg": "adls_iceberg", "lakehouse": "adls_iceberg",
    "delta": "adls_delta", "adls_delta": "adls_delta",
    "snowflake": "snowflake", "warehouse": "snowflake",
}


async def do_build_pipeline(source_asset: str, sink_type: str = "adls_iceberg", sink_table: str = "") -> str:
    from agents.pipeline_builder.nifi_flow_builder import build_flow, build_flow_summary, SINKS

    sink = _SINK_ALIASES.get((sink_type or "").lower().strip(), "adls_iceberg")
    if sink not in SINKS:
        return _pack(f"Unsupported sink '{sink_type}'. Choose one of: {', '.join(SINKS)}.", [])

    # Resolve the source so we can attach its real schema to the NiFi flow
    rec = await _resolve_asset(source_asset)
    if not rec:
        return _pack(
            f"Could not resolve '{source_asset}'. Make sure it is a known Kafka topic or Iceberg table.", [])

    src_type = "kafka_topic" if rec["asset_type"] == "kafka_topic" else "iceberg_table"
    schema_fields = [
        {"name": f.get("name", ""), "type": f.get("type", "string")}
        for f in rec.get("field_objs", [])
        if f.get("name")
    ]
    source = {"type": src_type, "name": rec["name"], "schema": schema_fields or None}

    sink_spec: dict = {"type": sink}
    if sink == "adls_iceberg":
        tgt = sink_table or rec["name"].split(".")[-1].replace("-", "_")
        ns, _, tb = tgt.partition(".")
        sink_spec.update(namespace=(tb and ns) or "default", table=tb or ns)

    try:
        flow = await asyncio.to_thread(lambda: build_flow(source=source, sink=sink_spec, flow_name=None))
    except Exception as e:
        return _pack(f"Pipeline build failed: {e}", [])

    summ = build_flow_summary(flow)
    params = summ.get("parameters_to_fill", [])
    summary = (
        f"Built NiFi flow '{summ['flow_name']}': {src_type} '{rec['name']}' → {sink}. "
        f"{summ['processor_count']} processors, {summ['connection_count']} connections. "
        f"{len(params)} parameters to fill in before starting."
    )
    blocks = [
        {"type": "context", "asset": rec["name"], "asset_type": rec["asset_type"]},
        {"type": "pipeline",
         "flow_name": summ["flow_name"],
         "source": {"type": src_type, "name": rec["name"]},
         "sink": {"type": sink, **({"table": sink_spec.get("table")} if sink == "adls_iceberg" else {})},
         "processors": summ.get("processors", []),
         "controller_services": summ.get("controller_services", []),
         "connection_count": summ.get("connection_count", 0),
         "parameters_to_fill": params,
         "flow": flow},
    ]
    return _pack(summary, blocks)


# ── Shared helpers re-exported for backward compat ────────────────────────────
# analyst_graph.py, supervisor_graph.py, and app.py import these by name.

async def _search_assets(query: str, top_k: int = 10, asset_type=None) -> list:
    from tools.catalog import catalog_store
    try:
        stats = await asyncio.to_thread(catalog_store.get_stats)
        if not stats.get("available"):
            return []
        hits = await asyncio.to_thread(catalog_store.search, query, [asset_type] if asset_type else None, top_k)
        return [{
            "name": h.get("name", ""),
            "asset_type": h.get("asset_type", "") or "iceberg_table",
            "fields": h.get("field_names", []) or [],
            "field_count": h.get("field_count", 0),
            "similarity": h.get("similarity"),
            "description": h.get("description", ""),
        } for h in hits if h.get("name")]
    except Exception as e:
        logger.debug(f"[helper] index search failed: {e}")
        return []


async def _resolve_asset(name: str):
    if not name:
        return None
    def _iceberg(n):
        from tools.iceberg.iceberg_tools import describe_iceberg_table
        meta = describe_iceberg_table(n)
        if meta and not meta.get("mock") and meta.get("fields"):
            return {"name": n, "asset_type": "iceberg_table",
                    "fields": [f.get("name", "") for f in meta["fields"]],
                    "field_objs": meta["fields"],
                    "snapshots": len(meta.get("snapshots", []) or [])}
        return None
    if "." in name:
        rec = await asyncio.to_thread(_iceberg, name)
        if rec:
            return rec
    try:
        from tools.kafka.kafka_tools import get_topic_schema_from_registry
        topic = await asyncio.to_thread(get_topic_schema_from_registry, name)
        if topic and topic.get("fields"):
            return {"name": name, "asset_type": "kafka_topic",
                    "fields": [f.get("name", "") for f in topic["fields"]],
                    "field_objs": topic["fields"]}
    except Exception:
        pass
    cand = await _search_assets(name, top_k=1)
    if cand:
        c = cand[0]
        if c["asset_type"] == "iceberg_table":
            rec = await asyncio.to_thread(_iceberg, c["name"])
            if rec:
                return rec
        c["field_objs"] = [{"name": f} for f in c.get("fields", [])]
        return c
    return None


async def _semantic_filter(goal: str, catalog: list) -> dict:
    import httpx
    from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY
    compact = [{"name": a["name"], "type": a.get("asset_type", ""), "fields": a.get("fields", [])} for a in catalog]
    system_prompt = (
        "You decide which data assets match a user's discovery goal based on their name and column names. "
        'Return strict JSON: {"matches":[{"name":"...","reason":"..."}]}. '
        "An asset matches only if it actually contains the kind of data the user asked for."
    )
    user_prompt = f"User goal: {goal}\n\nAssets:\n{json.dumps(compact, indent=2)}\n\nReturn JSON now."
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{LLM_BASE_URL}/chat/completions",
                json={"model": LLM_MODEL, "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ], "temperature": 0.1, "response_format": {"type": "json_object"}},
                headers={"Authorization": f"Bearer {LLM_API_KEY}"},
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            content = content.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            parsed = json.loads(content)
            return {m["name"]: m.get("reason", "matched") for m in parsed.get("matches", []) if m.get("name")}
    except Exception as e:
        logger.warning(f"[helper] semantic filter failed: {e}")
        return {}


async def _classify(message: str, context_asset=None) -> dict:
    """Legacy classifier — kept for supervisor_graph and LLM keepwarm."""
    from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY
    from langchain_openai import ChatOpenAI
    from langchain_core.messages import SystemMessage, HumanMessage
    prompt = f"""Intent router. Context: {context_asset or 'none'}. Message: "{message}"
Output ONLY JSON: {{"intent":"discover|lineage|query|describe|quality|smalltalk","asset":"","question":""}}"""
    try:
        llm = ChatOpenAI(base_url=LLM_BASE_URL, model=LLM_MODEL, api_key=LLM_API_KEY, temperature=0)
        resp = await llm.ainvoke([SystemMessage(content="Output only compact JSON."), HumanMessage(content=prompt)])
        txt = resp.content.strip()
        s, e = txt.find("{"), txt.rfind("}") + 1
        data = json.loads(txt[s:e])
        if not data.get("asset") and context_asset:
            data["asset"] = context_asset
        return data
    except Exception:
        m = message.lower()
        if any(w in m for w in ("lineage","come from","upstream","downstream","built","computed","pipeline")):
            return {"intent": "lineage", "asset": context_asset or "", "question": message}
        if any(w in m for w in ("how many","average","count","total","top","sum")):
            return {"intent": "query", "asset": context_asset or "", "question": message}
        return {"intent": "discover", "asset": "", "question": message}


def _wants_quality(message: str) -> bool:
    m = message.lower()
    return any(w in m for w in ("clean","data quality","quality score","quality of","trustworth","reliable","how good","dq ","data trust"))
