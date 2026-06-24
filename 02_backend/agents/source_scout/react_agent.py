"""
Source Scout ReAct Agent — LangGraph implementation.

The reasoning loop is a LangGraph StateGraph instead of a hand-rolled `while`:

        ┌──────────────────────────────┐
        ▼                              │
   ┌─────────┐   route   ┌─────────┐   │ loop
   │ reason  │──────────▶│  tools  │───┘
   └────┬────┘           └─────────┘
        │ finish / max-iters
        ▼
   ┌─────────┐
   │ finish  │──▶ END
   └─────────┘

- reason: LLM produces Thought / Action / Action Input
- route:  conditional edge — "finish" (or iteration cap) → finish, else → tools
- tools:  execute the chosen tool, append the observation, loop back to reason
- finish: emit the final result + OpenMetadata lineage, then END

State is shared and persisted across supersteps (LangGraph reducers). Progress is
streamed out as the *same* SSE events the hand-rolled version emitted, via the custom
stream writer (stream_mode="custom"), so the endpoint and frontend are unchanged.

Public entry point (unchanged): run_source_scout_react(goal) → async generator of events.
"""

import asyncio
import json
import logging
import re
from typing import Annotated, AsyncGenerator, Dict, List, Optional, Set, Tuple, TypedDict

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.types import StreamWriter

logger = logging.getLogger(__name__)

MAX_ITERATIONS = 15
AGENT_ID = "source_scout_react"


REACT_SYSTEM_PROMPT = """You are a data discovery agent for a Cloudera data platform.
Your goal is to help find relevant data assets based on user queries.

Respond in this exact format with no deviation:
Thought: <your reasoning about what to do next>
Action: <tool_name>
Action Input: <JSON object or null>

Available tools:
- semantic_search: Find assets in the catalog via vector search. Input: null
- search_kafka: List all Kafka topics from schema registry. Input: null
- search_iceberg: List all Iceberg tables from the catalog. Input: null
- search_ozone: List all Ozone storage volumes. Input: null
- get_topic_schema: Get detailed schema for a specific Kafka topic. Input: {"topic": "<name>"}
- get_table_schema: Get detailed schema for a specific Iceberg table. Input: {"table": "<name>"}
- generate_dq_rules: Preview semantic data quality rules for an Iceberg table (no execution). Input: {"table": "<db.table>"}
- execute_dq_rules: Run semantic DQ rules against Impala, returns violation counts. Input: {"table": "<db.table>"}
- finish: Return final results with discovered assets. Input: {"assets": [...], "summary": "..."}

Strategy:
1. Start with semantic_search to find relevant assets quickly
2. If semantic_search returns results, examine them with get_*_schema tools
3. If semantic_search returns 0 results, search ALL sources exhaustively:
   - search_kafka (Kafka topics with field schemas shown)
   - search_iceberg (Iceberg tables with column schemas shown)
   - search_ozone (Ozone volumes)
4. Filter results by examining which assets match the user's goal (look for matching field/column names)
5. For discovered Iceberg tables: use generate_dq_rules (preview) or execute_dq_rules (live Impala check)
6. Use finish when you have found all relevant assets or checked their quality

Important: After semantic_search returns 0, you MUST search kafka, iceberg, and ozone before finishing."""


def _emit(event_type: str, **kwargs) -> dict:
    """Build one SSE event dict — identical shape to the original hand-rolled agent."""
    return {"type": event_type, "agent": AGENT_ID, **kwargs}


# ── Reasoning + tool primitives (unchanged) ───────────────────────────────────

async def reason(messages: list) -> Tuple[str, str, Optional[Dict]]:
    """LLM reasoning step: given message history, return Thought/Action/Action Input."""
    from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY

    try:
        llm = ChatOpenAI(
            base_url=LLM_BASE_URL,
            model=LLM_MODEL,
            api_key=LLM_API_KEY,
            temperature=0.2,
        )
        response = await llm.ainvoke(messages)
        return parse_react_response(response.content)
    except Exception as e:
        logger.error(f"[react] LLM reasoning failed: {e}")
        raise


def parse_react_response(text: str) -> Tuple[str, str, Optional[Dict]]:
    """Parse LLM response into Thought / Action / Action Input."""
    thought = ""
    action = ""
    action_input = None

    thought_match = re.search(r"Thought:\s*(.+?)(?=Action:|$)", text, re.DOTALL)
    if thought_match:
        thought = thought_match.group(1).strip()

    action_match = re.search(r"Action:\s*(\w+)", text)
    if action_match:
        action = action_match.group(1).strip()

    input_match = re.search(r"Action Input:\s*(.+?)(?=\n\n|$)", text, re.DOTALL)
    if input_match:
        input_str = input_match.group(1).strip()
        if input_str.lower() == "null":
            action_input = None
        else:
            try:
                action_input = json.loads(input_str)
            except json.JSONDecodeError:
                logger.debug(f"[react] Could not parse action input as JSON: {input_str}")
                action_input = {"raw": input_str}

    return thought, action, action_input


async def semantic_search(goal: str) -> Dict[str, Set[str]]:
    """Vector search for candidate assets in the data catalog."""
    try:
        from tools.catalog import catalog_store

        stats = await asyncio.to_thread(catalog_store.get_stats)
        if not stats.get("available") or stats.get("total", 0) == 0:
            logger.debug("[react] Catalog unavailable")
            return {}

        asset_types = ["kafka_topic", "iceberg_table", "ozone_volume"]
        results = await asyncio.to_thread(catalog_store.search, goal, asset_types, 50)
        if not results:
            logger.debug(f"[react] No semantic search results for: {goal}")
            return {}

        prefilter: Dict[str, Set[str]] = {}
        for r in results:
            atype = r.get("asset_type", "")
            name = r.get("name", "")
            if not name:
                continue
            if atype == "kafka_topic":
                prefilter.setdefault("kafka", set()).add(name)
            elif atype == "iceberg_table":
                prefilter.setdefault("iceberg", set()).add(name)
            elif atype == "ozone_volume":
                prefilter.setdefault("ozone", set()).add(name)

        logger.debug(f"[react] Semantic search found {sum(len(v) for v in prefilter.values())} assets")
        return prefilter
    except Exception as e:
        logger.debug(f"[react] Semantic search failed: {e}")
        return {}


# ── Graph state ───────────────────────────────────────────────────────────────

class ScoutState(TypedDict):
    goal: str
    messages: Annotated[List[BaseMessage], add_messages]
    iteration: int
    thought: str
    action: str
    action_input: Optional[dict]
    assets: list
    summary: str


# ── Nodes ─────────────────────────────────────────────────────────────────────

async def reason_node(state: ScoutState, writer: StreamWriter) -> dict:
    """Think: ask the LLM for the next Thought/Action/Action Input."""
    iteration = state["iteration"] + 1
    thought, action, action_input = await reason(state["messages"])
    writer(_emit("thought", content=thought, iteration=iteration))

    update: dict = {"iteration": iteration, "thought": thought,
                    "action": action, "action_input": action_input}
    if action == "finish":
        update["assets"] = (action_input or {}).get("assets", [])
        update["summary"] = (action_input or {}).get("summary", "Discovery complete.")
    return update


def route(state: ScoutState) -> str:
    """Conditional edge: stop on finish or the iteration cap, else run the tool."""
    if state["action"] == "finish" or state["iteration"] >= MAX_ITERATIONS:
        return "finish"
    return "tools"


async def tools_node(state: ScoutState, writer: StreamWriter) -> dict:
    """Act + Observe: execute the chosen tool and append the observation to history."""
    action = state["action"]
    action_input = state["action_input"]
    iteration = state["iteration"]
    goal = state["goal"]
    thought = state["thought"]

    writer(_emit("action", tool=action, input=action_input, iteration=iteration))

    observation = ""
    try:
        if action == "semantic_search":
            result = await semantic_search(goal)
            total = sum(len(v) for v in result.values())
            observation = f"Found {total} assets: {result}"

        elif action == "search_kafka":
            from tools.kafka.kafka_tools import get_all_topics_from_schema_registry
            topics_dict = await asyncio.to_thread(get_all_topics_from_schema_registry)
            topic_details = []
            for topic_name, info in topics_dict.items():
                field_names = [f.get("name", "") for f in info.get("fields", [])]
                topic_details.append(f"{topic_name}: {field_names}")
            observation = f"Found {len(topics_dict)} Kafka topics:\n" + "\n".join(topic_details[:15])
            if len(topic_details) > 15:
                observation += f"\n... and {len(topic_details)-15} more topics"

        elif action == "search_iceberg":
            from tools.iceberg.iceberg_tools import list_iceberg_tables
            tables = await asyncio.to_thread(list_iceberg_tables)
            table_details = []
            for table in tables:
                schema = table.get("schema", {})
                col_names = [c.get("name", "") for c in schema.get("columns", [])]
                table_details.append(f"{table.get('name', '')}: {col_names}")
            observation = f"Found {len(tables)} Iceberg tables:\n" + "\n".join(table_details[:15])
            if len(table_details) > 15:
                observation += f"\n... and {len(table_details)-15} more tables"

        elif action == "search_ozone":
            from tools.ozone.ozone_tools import list_ozone_volumes
            volumes = await asyncio.to_thread(list_ozone_volumes)
            volume_names = [v.get("name", "") for v in volumes]
            observation = f"Found {len(volume_names)} Ozone volumes: {volume_names}"

        elif action == "get_topic_schema":
            topic = (action_input or {}).get("topic", "")
            if not topic:
                observation = "Error: 'topic' parameter required in Action Input"
            else:
                from tools.kafka.kafka_tools import get_all_topics_from_schema_registry
                topics_dict = await asyncio.to_thread(get_all_topics_from_schema_registry)
                observation = f"Schema for {topic}: {topics_dict.get(topic, {})}"

        elif action == "get_table_schema":
            table = (action_input or {}).get("table", "")
            if not table:
                observation = "Error: 'table' parameter required in Action Input"
            else:
                from tools.iceberg.iceberg_tools import describe_iceberg_table
                schema = await asyncio.to_thread(describe_iceberg_table, table)
                observation = f"Schema for {table}: {schema}"

        elif action == "generate_dq_rules":
            table = (action_input or {}).get("table", "")
            if not table:
                observation = "Error: 'table' parameter required in Action Input"
            else:
                from tools.iceberg.iceberg_tools import describe_iceberg_table
                from tools.iceberg.dq_rule_engine import generate_semantic_dq_rules
                schema = await asyncio.to_thread(describe_iceberg_table, table)
                rules = generate_semantic_dq_rules(table, schema.get("fields", []))
                if not rules:
                    observation = f"No semantic DQ rules matched any column in {table}."
                else:
                    lines = [f"  [{r['domain']}] {r['rule_name']}: {r['description']}" for r in rules[:15]]
                    observation = f"Generated {len(rules)} semantic DQ rules for {table}:\n" + "\n".join(lines)
                    if len(rules) > 15:
                        observation += f"\n  ... and {len(rules)-15} more rules"

        elif action == "execute_dq_rules":
            table = (action_input or {}).get("table", "")
            if not table:
                observation = "Error: 'table' parameter required in Action Input"
            else:
                from tools.iceberg.iceberg_tools import describe_iceberg_table
                from tools.iceberg.dq_rule_engine import execute_semantic_dq_rules
                try:
                    schema = await asyncio.wait_for(
                        asyncio.to_thread(describe_iceberg_table, table), timeout=15)
                    fields = schema.get("fields", [])
                    results = await asyncio.wait_for(
                        asyncio.to_thread(execute_semantic_dq_rules, table, fields), timeout=60)
                    if not results:
                        observation = f"No semantic DQ rules matched any column in {table}."
                    else:
                        pass_ct = sum(1 for r in results if r["status"] == "pass")
                        warn_ct = sum(1 for r in results if r["status"] == "warn")
                        fail_ct = sum(1 for r in results if r["status"] == "fail")
                        err_ct = sum(1 for r in results if r["status"] == "error")
                        lines = [f"DQ results for {table}: {pass_ct} PASS, {warn_ct} WARN, "
                                 f"{fail_ct} FAIL, {err_ct} ERROR ({len(results)} rules)"]
                        for r in results[:10]:
                            viol, total, pct = r.get("violation_count"), r.get("total_rows"), r.get("violation_pct")
                            if viol is not None:
                                lines.append(f"[{r['status'].upper()}] {r['rule_name']}: {viol}/{total} violations ({pct:.1f}%)")
                                sql = r['impala_sql'].replace(chr(10), ' ')
                                lines.append(f"  SQL: {sql[:300]}{'...' if len(sql) > 300 else ''}")
                            else:
                                lines.append(f"[ERROR] {r['rule_name']}: {r.get('error', 'unknown')}")
                        if len(results) > 10:
                            lines.append(f"... and {len(results)-10} more rules")
                        observation = "\n".join(lines)
                        writer(_emit("dq_results", table=table, results=results, iteration=iteration))
                except asyncio.TimeoutError:
                    observation = f"DQ rule execution timed out after 60s for {table}. Impala connection may be unavailable."
                except Exception as e:
                    observation = f"DQ rule execution failed: {str(e)}"

        else:
            observation = (f"Unknown action: '{action}'. Available: semantic_search, search_kafka, "
                           "search_iceberg, search_ozone, get_topic_schema, get_table_schema, "
                           "generate_dq_rules, execute_dq_rules, finish.")

        writer(_emit("observation", tool=action, summary=observation, iteration=iteration))
    except Exception as e:
        observation = f"Error executing {action}: {str(e)}"
        writer(_emit("observation", tool=action, summary=observation, error=True, iteration=iteration))

    # Append this turn's reasoning + observation so the next reason step sees it.
    return {"messages": [
        AIMessage(content=f"Thought: {thought}\nAction: {action}\nAction Input: {action_input}"),
        HumanMessage(content=f"Observation: {observation}"),
    ]}


async def finish_node(state: ScoutState, writer: StreamWriter) -> dict:
    """Emit the final result, then enrich with OpenMetadata lineage (non-blocking)."""
    if state["action"] != "finish":
        writer(_emit("complete", summary=f"Discovery complete after {MAX_ITERATIONS} iterations.",
                     assets_count=0))
        return {}

    assets = state.get("assets", [])
    writer(_emit("complete", summary=state.get("summary", "Discovery complete."),
                 assets_count=len(assets)))

    try:
        from tools.openmetadata.client import health_check, get_lineage_by_name
        if health_check():
            for asset in assets:
                name = asset.get("name", "") or asset.get("id", "")
                atype = "topic" if asset.get("type") == "kafka_topic" else "table"
                lineage = await asyncio.to_thread(get_lineage_by_name, name, atype)
                if lineage and (lineage.get("upstream") or lineage.get("downstream")):
                    writer(_emit("lineage", asset=name, **{
                        k: lineage[k] for k in ("entity", "upstream", "downstream", "edge_count")
                    }))
    except Exception as _le:
        logger.debug(f"[react] OM lineage fetch skipped: {_le}")
    return {}


# ── Graph assembly ────────────────────────────────────────────────────────────

def build_scout_graph():
    graph = StateGraph(ScoutState)
    graph.add_node("reason", reason_node)
    graph.add_node("tools", tools_node)
    graph.add_node("finish", finish_node)

    graph.set_entry_point("reason")
    graph.add_conditional_edges("reason", route, {"tools": "tools", "finish": "finish"})
    graph.add_edge("tools", "reason")   # the loop
    graph.add_edge("finish", END)

    return graph.compile()


_GRAPH = build_scout_graph()


# ── Public entry point (unchanged signature + event stream) ───────────────────

async def run_source_scout_react(goal: str) -> AsyncGenerator[dict, None]:
    """Run the ReAct graph and stream its SSE events (same vocabulary as before)."""
    initial: ScoutState = {
        "goal": goal,
        "messages": [SystemMessage(content=REACT_SYSTEM_PROMPT), HumanMessage(content=f"Goal: {goal}")],
        "iteration": 0,
        "thought": "",
        "action": "",
        "action_input": None,
        "assets": [],
        "summary": "",
    }
    try:
        # recursion_limit covers reason+tools per loop (2 supersteps) up to MAX_ITERATIONS + finish.
        async for event in _GRAPH.astream(
            initial,
            config={"recursion_limit": 2 * MAX_ITERATIONS + 5},
            stream_mode="custom",
        ):
            yield event
    except Exception as e:
        logger.error(f"[react] graph run failed: {e}")
        yield _emit("error", content=f"Reasoning failed: {e}")
