"""
OpenMetadata REST client.

Handles:
- Asset search (keyword, returns OM entities)
- Lineage fetch by entity name + type
- Entity lookup by FQN or storage location
- Asset registration (table + topic)
- Lineage edge creation

Async layer (async_*):  uses a shared httpx.AsyncClient (persistent TCP connection pool,
no thread-pool usage). Drop-in replacements for the sync functions in async call sites.
The sync functions remain for backward-compat (called via asyncio.to_thread elsewhere).
"""

import logging
import os
from typing import Optional
import requests

logger = logging.getLogger(__name__)

OM_URL   = os.getenv("OPENMETADATA_URL", "http://localhost:8585/api")
OM_TOKEN = os.getenv("OPENMETADATA_TOKEN", "")   # JWT from OM; falls back to basic auth
OM_USER  = os.getenv("OPENMETADATA_USER", "admin")
OM_PASS  = os.getenv("OPENMETADATA_PASSWORD", "admin")

# Cloudera CDP connection details for service registration — sourced from env
CDP_HOST          = os.getenv("KNOX_HOST", "cdp-utility.cdp.local")
CDP_USER          = os.getenv("KNOX_USERNAME", "")
CDP_PASS          = os.getenv("KNOX_PASSWORD", "")
CDP_SR_URL        = os.getenv("SCHEMA_REGISTRY_URL", f"http://{CDP_HOST}:8443/gateway/cdp-proxy-api/schema-registry")
CDP_KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")

_jwt_cache: dict = {"token": "", "expires": 0}

# ── Shared async HTTP client ───────────────────────────────────────────────────
# One client per process, persistent connection pool — avoids the thread overhead
# of asyncio.to_thread(requests.get, ...) at high concurrency.
_async_client = None   # httpx.AsyncClient, lazily created on first async call


def _get_async_client():
    global _async_client
    if _async_client is None:
        try:
            import httpx
            _async_client = httpx.AsyncClient(
                timeout=httpx.Timeout(15.0),
                limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
            )
        except ImportError:
            pass   # httpx not installed; callers fall back to asyncio.to_thread
    return _async_client


def _headers() -> dict:
    token = _get_token()
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _get_token() -> str:
    """Return cached JWT, refreshing if needed."""
    import time
    if _jwt_cache["token"] and time.time() < _jwt_cache["expires"]:
        return _jwt_cache["token"]
    try:
        import base64
        pwd_b64 = base64.b64encode(OM_PASS.encode()).decode()
        resp = requests.post(
            f"{OM_URL}/v1/users/login",
            json={"email": f"{OM_USER}@open-metadata.org", "password": pwd_b64},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        _jwt_cache["token"] = data["accessToken"]
        _jwt_cache["expires"] = time.time() + data.get("tokenExpirationMs", 3600000) / 1000 - 60
        return _jwt_cache["token"]
    except Exception as e:
        logger.warning(f"[OM] login failed: {e}")
        return ""


async def _async_get_token() -> str:
    """Async JWT fetch — uses the shared httpx client, no thread."""
    import time
    if _jwt_cache["token"] and time.time() < _jwt_cache["expires"]:
        return _jwt_cache["token"]
    client = _get_async_client()
    if client is None:
        import asyncio
        return await asyncio.to_thread(_get_token)
    try:
        import base64
        pwd_b64 = base64.b64encode(OM_PASS.encode()).decode()
        resp = await client.post(
            f"{OM_URL}/v1/users/login",
            json={"email": f"{OM_USER}@open-metadata.org", "password": pwd_b64},
        )
        resp.raise_for_status()
        data = resp.json()
        _jwt_cache["token"] = data["accessToken"]
        _jwt_cache["expires"] = time.time() + data.get("tokenExpirationMs", 3600000) / 1000 - 60
        return _jwt_cache["token"]
    except Exception as e:
        logger.warning(f"[OM] async login failed: {e}")
        return ""


async def _async_get(path: str, params: dict = None) -> Optional[dict]:
    """Non-blocking GET — reuses the shared connection pool."""
    client = _get_async_client()
    if client is None:
        import asyncio
        return await asyncio.to_thread(_get, path, params)
    try:
        token = await _async_get_token()
        resp = await client.get(
            f"{OM_URL}{path}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            params=params,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"[OM] async GET {path} failed: {e}")
        return None


def _get(path: str, params: dict = None) -> Optional[dict]:
    try:
        resp = requests.get(f"{OM_URL}{path}", headers=_headers(), params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"[OM] GET {path} failed: {e}")
        return None


def _post(path: str, body: dict) -> Optional[dict]:
    try:
        resp = requests.post(f"{OM_URL}{path}", headers=_headers(), json=body, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"[OM] POST {path} failed: {e}")
        return None


def _put(path: str, body: dict) -> Optional[dict]:
    try:
        resp = requests.put(f"{OM_URL}{path}", headers=_headers(), json=body, timeout=15)
        resp.raise_for_status()
        return resp.json() if resp.text.strip() else {}   # lineage PUT returns 200 with empty body
    except Exception as e:
        logger.warning(f"[OM] PUT {path} failed: {e}")
        return None


# ─── Search ───────────────────────────────────────────────────────────────────

def search(query: str, entity_type: str = "all", limit: int = 10) -> list[dict]:
    """
    Full-text search across OM catalog.
    entity_type: 'table' | 'topic' | 'all'
    Returns list of hit dicts with id, name, fullyQualifiedName, description, entityType.
    """
    index_map = {
        "table": "table_search_index",
        "topic": "topic_search_index",
        "all":   "all",
    }
    index = index_map.get(entity_type, "all")
    result = _get("/v1/search/query", params={
        "q": query, "index": index, "from": 0, "size": limit
    })
    if not result:
        return []
    hits = result.get("hits", {}).get("hits", [])
    return [
        {
            "id":   h["_source"].get("id"),
            "name": h["_source"].get("name"),
            "fqn":  h["_source"].get("fullyQualifiedName"),
            "description": h["_source"].get("description", ""),
            "entity_type": h["_source"].get("entityType", h.get("_index", "")),
            "service": h["_source"].get("service", {}).get("name", ""),
            "tags": [t.get("tagFQN") for t in h["_source"].get("tags", [])],
        }
        for h in hits
    ]


# ─── Entity lookup ────────────────────────────────────────────────────────────

def get_entity_by_fqn(fqn: str, entity_type: str = "table") -> Optional[dict]:
    """Fetch a single entity by fully qualified name."""
    type_path = {"table": "tables", "topic": "topics", "pipeline": "pipelines"}.get(entity_type, "tables")
    return _get(f"/v1/{type_path}/name/{fqn}", params={"fields": "tags,owner,columns"})


async def async_get_entity_by_fqn(fqn: str, entity_type: str = "table") -> Optional[dict]:
    """Async: fetch a single entity by FQN — no thread, uses shared httpx pool."""
    type_path = {"table": "tables", "topic": "topics", "pipeline": "pipelines"}.get(entity_type, "tables")
    return await _async_get(f"/v1/{type_path}/name/{fqn}", params={"fields": "tags,owner,columns"})


def find_table_by_name(name: str) -> Optional[dict]:
    """
    Find a table by short or dotted name.
    1. Try common FQN patterns via direct GET (works even before ES is indexed)
    2. Fall back to ES search
    """
    short = name.split(".")[-1]
    db    = name.split(".")[0] if "." in name else "demo"

    # Try direct FQN patterns: service.db.schema.table
    for fqn in [
        f"cdp_hive.{db}.default.{short}",
        f"cdp_hive.demo.default.{short}",
        f"cdp_hive.default.default.{short}",
    ]:
        result = _get(f"/v1/tables/name/{fqn}", params={"fields": "id,name,fullyQualifiedName,description"})
        if result and result.get("id"):
            return {"id": result["id"], "name": result["name"],
                    "fqn": result["fullyQualifiedName"], "entity_type": "table",
                    "description": result.get("description", ""), "service": "cdp_hive"}

    # Fall back to ES search
    hits = search(name, entity_type="table", limit=5)
    for h in hits:
        if h["name"].lower() == short.lower():
            return h
    return hits[0] if hits else None


async def async_find_table_by_name(name: str) -> Optional[dict]:
    """Async: find a table by short/dotted name — no thread, uses shared httpx pool."""
    short = name.split(".")[-1]
    db    = name.split(".")[0] if "." in name else "demo"
    for fqn in [
        f"cdp_hive.{db}.default.{short}",
        f"cdp_hive.demo.default.{short}",
        f"cdp_hive.default.default.{short}",
    ]:
        result = await _async_get(f"/v1/tables/name/{fqn}", params={"fields": "id,name,fullyQualifiedName,description"})
        if result and result.get("id"):
            return {"id": result["id"], "name": result["name"],
                    "fqn": result["fullyQualifiedName"], "entity_type": "table",
                    "description": result.get("description", ""), "service": "cdp_hive"}
    # Fall back to sync search (ES search path — acceptable for the fallback)
    import asyncio
    hits = await asyncio.to_thread(search, name, "table", 5)
    for h in hits:
        if h["name"].lower() == short.lower():
            return h
    return hits[0] if hits else None


def find_topic_by_name(name: str) -> Optional[dict]:
    """
    Find a Kafka topic by name.
    1. Try direct FQN GET
    2. Fall back to ES search
    """
    short = name.split(".")[-1]

    # OM wraps dotted topic names in quotes: cdp_kafka."demo.payment_transactions"
    for fqn in [f'cdp_kafka."{name}"', f"cdp_kafka.{name}", f"cdp_kafka.{short}"]:
        result = _get(f"/v1/topics/name/{fqn}", params={"fields": "id,name,fullyQualifiedName,description"})
        if result and result.get("id"):
            return {"id": result["id"], "name": result["name"],
                    "fqn": result["fullyQualifiedName"], "entity_type": "topic",
                    "description": result.get("description", ""), "service": "cdp_kafka"}

    hits = search(name, entity_type="topic", limit=5)
    for h in hits:
        if h["name"].lower() == short.lower():
            return h
    return hits[0] if hits else None


# ─── Lineage ──────────────────────────────────────────────────────────────────

# OpenMetadata's lineage API rejects depths above this with HTTP 400.
OM_MAX_LINEAGE_DEPTH = 3


def get_lineage(entity_id: str, entity_type: str = "table",
                upstream_depth: int = 3, downstream_depth: int = 3) -> Optional[dict]:
    """Fetch upstream + downstream lineage for an entity.

    Depths are clamped to OM_MAX_LINEAGE_DEPTH — OpenMetadata returns 400 for
    anything larger, which would silently drop the whole graph.
    """
    type_path = {"table": "table", "topic": "topic", "pipeline": "pipeline"}.get(entity_type, "table")
    return _get(f"/v1/lineage/{type_path}/{entity_id}", params={
        "upstreamDepth": min(upstream_depth, OM_MAX_LINEAGE_DEPTH),
        "downstreamDepth": min(downstream_depth, OM_MAX_LINEAGE_DEPTH),
    })


def get_lineage_by_name(asset_name: str, asset_type: str = "table",
                        upstream_depth: int = 3, downstream_depth: int = 3) -> Optional[dict]:
    """
    High-level: find entity by name then fetch its full lineage.
    Returns {'entity': {...}, 'upstream': [...], 'downstream': [...], 'edges': [...]}
    or None if not found in OM.
    """
    if asset_type == "topic":
        entity = find_topic_by_name(asset_name)
    else:
        entity = find_table_by_name(asset_name)

    if not entity or not entity.get("id"):
        logger.info(f"[OM] '{asset_name}' not found in OpenMetadata")
        return None

    raw = get_lineage(entity["id"], asset_type, upstream_depth, downstream_depth)
    if not raw:
        return {"entity": entity, "upstream": [], "downstream": [], "edges": [], "edge_count": 0}

    # OM 1.5+ returns the FULL multi-hop graph as upstreamEdges / downstreamEdges.
    # We BFS out from this entity so every node gets a hop-distance ("depth") and a
    # side (up/cur/down). The UI lays each depth out in its own column, so a chain
    # like  kafka topic → table → customer_360 → dashboard  renders as a proper
    # left-to-right flow instead of collapsing every ancestor onto one column.
    nodes_by_id = {n["id"]: n for n in raw.get("nodes", [])}
    eid = entity.get("id")
    if eid and eid not in nodes_by_id:
        nodes_by_id[eid] = entity

    def _eid(v):
        return v.get("id") if isinstance(v, dict) else v

    upstream_edges   = raw.get("upstreamEdges",   [])
    downstream_edges = raw.get("downstreamEdges", [])

    depth = {eid: 0}
    side  = {eid: "cur"}

    # walk upstream: edge X → node  ⇒  X is one hop further upstream (negative depth)
    frontier, d = [eid], 0
    while frontier:
        d += 1
        nxt = []
        for node_id in frontier:
            for e in upstream_edges:
                if _eid(e.get("toEntity")) == node_id:
                    f = _eid(e.get("fromEntity"))
                    if f in nodes_by_id and f not in depth:
                        depth[f], side[f] = -d, "up"
                        nxt.append(f)
        frontier = nxt

    # walk downstream: edge node → Y  ⇒  Y is one hop further downstream (positive depth)
    frontier, d = [eid], 0
    while frontier:
        d += 1
        nxt = []
        for node_id in frontier:
            for e in downstream_edges:
                if _eid(e.get("fromEntity")) == node_id:
                    t = _eid(e.get("toEntity"))
                    if t in nodes_by_id and t not in depth:
                        depth[t], side[t] = d, "down"
                        nxt.append(t)
        frontier = nxt

    graph_nodes = []
    for nid, dep in depth.items():
        n = _slim_node(nodes_by_id[nid])
        n["depth"], n["side"] = dep, side[nid]
        graph_nodes.append(n)
    graph_nodes.sort(key=lambda n: (n["depth"], n["name"]))

    seen_e = set()
    graph_edges = []
    for e in upstream_edges + downstream_edges:
        f, t = _eid(e.get("fromEntity")), _eid(e.get("toEntity"))
        if f in depth and t in depth and (f, t) not in seen_e:
            seen_e.add((f, t))
            graph_edges.append({"from": f, "to": t})

    # direct (1-hop) neighbours, for summaries / chip counts
    upstream   = [n for n in graph_nodes if n["depth"] == -1]
    downstream = [n for n in graph_nodes if n["depth"] == 1]

    return {
        "entity":     entity,
        "upstream":   upstream,
        "downstream": downstream,
        "graph":      {"nodes": graph_nodes, "edges": graph_edges},
        "edges":      upstream_edges + downstream_edges,
        "edge_count": len(graph_edges),
        "raw":        raw,
    }


_FQN_TYPE_MAP = {
    "cdp_kafka":    "topic",
    "cdp_hive":     "table",
    "cdp_nifi":     "pipeline",
    "looker":       "dashboard",
    "metabase":     "dashboard",
    "superset":     "dashboard",
    "dbt":          "pipeline",
}


def _infer_entity_type(node: dict) -> str:
    """Derive entity type from the node's FQN prefix when OM omits entityType in lineage responses."""
    explicit = node.get("entityType") or node.get("type")
    if explicit:
        return explicit
    fqn = node.get("fullyQualifiedName") or ""
    prefix = fqn.split(".")[0]
    return _FQN_TYPE_MAP.get(prefix, "table")


def _slim_node(node: dict) -> dict:
    # Extract whatever OM already returned in the lineage node (varies by OM version).
    raw_tags = node.get("tags") or []
    tags     = [t.get("tagFQN") for t in raw_tags if isinstance(t, dict) and t.get("tagFQN")]
    owner_obj = node.get("owner") or {}
    owner = owner_obj.get("name") or owner_obj.get("displayName") if isinstance(owner_obj, dict) else None
    tier  = next((t for t in tags if t and "tier" in t.lower()), None)
    return {
        "id":          node.get("id"),
        "name":        node.get("name"),
        "fqn":         node.get("fullyQualifiedName") or node.get("fqn"),
        "entity_type": _infer_entity_type(node),
        "description": node.get("description", ""),
        "service":     node.get("service", {}).get("name", "") if isinstance(node.get("service"), dict) else "",
        "owner":       owner,
        "tier":        tier,
        "tags":        [t for t in tags if t and "tier" not in t.lower()],
    }


async def async_enrich_node(fqn: str, entity_type: str = "table") -> dict:
    """Fetch full OM properties for one lineage node: tags, owner, tier, column count.
    Best-effort — returns {} on any error so callers keep the slim baseline."""
    if not fqn:
        return {}
    type_path = {
        "table": "tables", "topic": "topics",
        "pipeline": "pipelines", "dashboard": "dashboards",
    }.get(entity_type, "tables")
    result = await _async_get(f"/v1/{type_path}/name/{fqn}",
                              params={"fields": "tags,owner,columns,customMetrics"})
    if not result:
        return {}
    raw_tags = result.get("tags") or []
    tags     = [t.get("tagFQN") for t in raw_tags if isinstance(t, dict) and t.get("tagFQN")]
    owner_obj = result.get("owner") or {}
    owner = owner_obj.get("name") or owner_obj.get("displayName") if isinstance(owner_obj, dict) else None
    tier  = next((t for t in tags if t and "tier" in t.lower()), None)
    return {
        "owner":        owner,
        "tier":         tier,
        "tags":         [t for t in tags if t and "tier" not in t.lower()],
        "column_count": len(result.get("columns") or []),
        "description":  result.get("description") or "",
    }


async def async_enrich_lineage_graph(graph_result: dict) -> dict:
    """Concurrently enrich every lineage node with full OM properties.
    Caps at 20 nodes to avoid flooding OM; deeper nodes keep the slim baseline."""
    import asyncio
    nodes = (graph_result.get("graph") or {}).get("nodes", [])
    if not nodes:
        return graph_result

    cap = 20
    to_enrich, rest = nodes[:cap], nodes[cap:]

    async def _enrich_one(node: dict) -> dict:
        props = await async_enrich_node(node.get("fqn", ""), node.get("entity_type", "table"))
        # Merge: enriched values win over the slim-node baseline, but only when non-empty.
        merged = {**node}
        for k, v in props.items():
            if v or v == 0:        # keep 0 column_count, drop None/""
                merged[k] = v
        return merged

    results = await asyncio.gather(*[_enrich_one(n) for n in to_enrich], return_exceptions=True)
    enriched_nodes = [
        r if not isinstance(r, Exception) else to_enrich[i]
        for i, r in enumerate(results)
    ] + rest

    return {
        **graph_result,
        "graph": {**(graph_result.get("graph") or {}), "nodes": enriched_nodes},
    }


def format_lineage_for_llm(graph_result: dict, asset: str) -> str:
    """Convert an enriched lineage graph into structured prose the LLM can traverse hop-by-hop.

    Output groups nodes by depth so the model sees the data flow left-to-right:
      upstream hops (most distant first) → current asset → downstream hops.
    Each node line includes every non-empty property so the model can answer
    ownership, compliance, quality, and impact-analysis questions without guessing.
    """
    nodes      = (graph_result.get("graph") or {}).get("nodes", [])
    edge_count = graph_result.get("edge_count", 0)

    if not nodes:
        return f"No lineage data found for '{asset}' in OpenMetadata."

    by_depth: dict[int, list] = {}
    for n in nodes:
        by_depth.setdefault(n.get("depth", 0), []).append(n)

    def _node_line(n: dict) -> str:
        parts = [f"  • [{n.get('entity_type', 'table').upper()}] {n.get('name') or n.get('fqn', '?')}"]
        if n.get("service"):
            parts.append(f"service={n['service']}")
        if n.get("owner"):
            parts.append(f"owner={n['owner']}")
        if n.get("tier"):
            parts.append(f"tier={n['tier']}")
        tags = n.get("tags") or []
        if tags:
            parts.append(f"tags=[{', '.join(tags[:5])}]")
        if n.get("column_count"):
            parts.append(f"columns={n['column_count']}")
        desc = (n.get("description") or "").strip()
        if desc:
            parts.append(f'"{desc[:100]}"')
        return "    " + " | ".join(parts)

    lines = [
        f"=== Lineage graph for: {asset} ===",
        f"Nodes: {len(nodes)}  Edges: {edge_count}",
        "",
    ]

    upstream_depths   = sorted([d for d in by_depth if d < 0])   # [-3, -2, -1]
    downstream_depths = sorted([d for d in by_depth if d > 0])   # [1, 2, 3]

    if upstream_depths:
        lines.append("UPSTREAM  (data flows FROM these INTO the current asset)")
        for d in upstream_depths:
            hop = abs(d)
            lines.append(f"  Hop -{hop}  ({'direct source' if hop == 1 else f'{hop} hops away'}):")
            for n in by_depth[d]:
                lines.append(_node_line(n))
        lines.append("")

    if 0 in by_depth:
        lines.append("CURRENT ASSET")
        for n in by_depth[0]:
            lines.append(_node_line(n))
        lines.append("")

    if downstream_depths:
        lines.append("DOWNSTREAM  (data flows FROM the current asset INTO these)")
        for d in downstream_depths:
            lines.append(f"  Hop +{d}  ({'direct consumer' if d == 1 else f'{d} hops away'}):")
            for n in by_depth[d]:
                lines.append(_node_line(n))
        lines.append("")

    return "\n".join(lines)


# ─── Registration (push assets + lineage into OM) ────────────────────────────

def ensure_database_service(service_name: str = "cdp_hive") -> Optional[str]:
    """Get or create a Hive database service. Returns service id."""
    existing = _get(f"/v1/services/databaseServices/name/{service_name}")
    if existing:
        return existing["id"]
    body = {
        "name": service_name,
        "displayName": "Cloudera CDP Hive/Iceberg",
        "description": "Iceberg tables via Cloudera Knox (Impala/Hive)",
        "serviceType": "Hive",
        "connection": {
            "config": {
                "type": "Hive",
                "hostPort": f"{CDP_HOST}:8443",
                "auth": "LDAP",
                "username": CDP_USER,
                "password": CDP_PASS,
            }
        },
    }
    result = _post("/v1/services/databaseServices", body)
    return result["id"] if result else None


def ensure_messaging_service(service_name: str = "cdp_kafka") -> Optional[str]:
    """Get or create a Kafka messaging service. Returns service id."""
    existing = _get(f"/v1/services/messagingServices/name/{service_name}")
    if existing:
        return existing["id"]
    body = {
        "name": service_name,
        "displayName": "Cloudera CDP Kafka",
        "description": "Kafka via Schema Registry (Knox gateway)",
        "serviceType": "Kafka",
        "connection": {
            "config": {
                "type": "Kafka",
                "bootstrapServers": CDP_KAFKA_BROKERS,
                "schemaRegistryURL": CDP_SR_URL,
            }
        },
    }
    result = _post("/v1/services/messagingServices", body)
    return result["id"] if result else None


def register_table(table_name: str, fields: list[dict], description: str = "",
                   service_name: str = "cdp_hive", db_name: str = "demo") -> Optional[dict]:
    """Register an Iceberg table in OpenMetadata. table_name = 'demo.payment_transactions'"""
    parts = table_name.split(".")
    db    = parts[0] if len(parts) > 1 else db_name
    tbl   = parts[-1]

    columns = [
        {"name": f["name"], "dataType": _map_type(f.get("type", "string")),
         "dataTypeDisplay": f.get("type", "string")}
        for f in fields
    ]
    body = {
        "name": tbl,
        "displayName": tbl,
        "description": description,
        "tableType": "Iceberg",
        "columns": columns,
        "databaseSchema": f"{service_name}.{db}.default",   # must be FQN string, not object
    }
    return _post("/v1/tables", body)


def record_query_and_usage(asset_name: str, sql: str, duration_ms: Optional[float] = None,
                           default_service: str = "cdp_hive") -> bool:
    """Enrich OpenMetadata with how an asset is being USED:
      1. append the executed SQL to the table's Queries tab  (POST /v1/queries)
      2. bump the table's usage count for today               (POST /v1/usage/table/{id})
    OM then shows query history + a popularity rank on the asset page. Best-effort.
    """
    try:
        import time as _t, datetime as _dt, requests
        entity = find_table_by_name(asset_name)
        if not entity or not entity.get("id"):
            logger.info(f"[OM] usage write skipped — {asset_name} not in OpenMetadata")
            return False
        tid = entity["id"]
        svc = entity.get("service")
        if isinstance(svc, dict):
            service = svc.get("name") or svc.get("fullyQualifiedName") or default_service
        elif isinstance(svc, str) and svc:
            service = svc
        else:
            service = default_service

        q_body = {
            "query": sql,
            "service": service,
            "queryUsedIn": [{"id": tid, "type": "table"}],
            "queryDate": int(_t.time() * 1000),
        }
        if duration_ms is not None:
            q_body["duration"] = float(duration_ms)
        rq = requests.post(f"{OM_URL}/v1/queries", headers=_headers(), json=q_body, timeout=15)
        if rq.status_code not in (200, 201, 409):   # 409 = identical query already recorded
            logger.warning(f"[OM] query record: {rq.status_code} {rq.text[:160]}")

        today = _dt.date.today().isoformat()
        ru = requests.post(f"{OM_URL}/v1/usage/table/{tid}", headers=_headers(),
                           json={"date": today, "count": 1}, timeout=15)
        if ru.status_code not in (200, 201):
            logger.warning(f"[OM] usage bump: {ru.status_code} {ru.text[:160]}")
        return rq.status_code in (200, 201, 409) or ru.status_code in (200, 201)
    except Exception as e:
        logger.warning(f"[OM] query/usage write failed: {e}")
        return False


def register_topic(topic_name: str, schema_fields: list[dict],
                   description: str = "", service_name: str = "cdp_kafka") -> Optional[dict]:
    """Register a Kafka topic in OpenMetadata."""
    short_name = topic_name.split(".")[-1]
    body = {
        "name": topic_name,
        "displayName": short_name,
        "description": description,
        "service": service_name,   # must be FQN string, not object
        "messageSchema": {
            "schemaType": "Avro",
            "schemaFields": [
                {"name": f["name"], "dataType": _map_topic_type(_avro_type(f.get("type", "string")))}
                for f in schema_fields
            ],
        },
        "partitions": 12,
    }
    return _post("/v1/topics", body)


def create_lineage_edge(from_fqn: str, from_type: str,
                        to_fqn: str,   to_type: str,
                        pipeline_name: str = "") -> Optional[dict]:
    """Create a directed lineage edge from_entity → to_entity in OM."""
    from_entity = _resolve_entity(from_fqn, from_type)
    to_entity   = _resolve_entity(to_fqn, to_type)
    if not from_entity or not to_entity:
        logger.warning(f"[OM] lineage edge skipped — could not resolve {from_fqn} or {to_fqn}")
        return None

    body = {
        "edge": {
            "fromEntity": {"id": from_entity["id"], "type": from_type},
            "toEntity":   {"id": to_entity["id"],   "type": to_type},
        }
    }
    if pipeline_name:
        body["edge"]["lineageDetails"] = {"description": pipeline_name}

    return _put("/v1/lineage", body)


def _resolve_entity(fqn: str, entity_type: str) -> Optional[dict]:
    type_path = {"table": "tables", "topic": "topics"}.get(entity_type, "tables")
    result = _get(f"/v1/{type_path}/name/{fqn}")
    if result:
        return result
    # Fallback: search by name
    hits = search(fqn.split(".")[-1], entity_type=entity_type, limit=3)
    return hits[0] if hits else None


def _map_type(t: str) -> str:
    """Map generic/Avro type string to OM table column dataType enum."""
    t = t.lower()
    if "int" in t or "long" in t or "bigint" in t: return "BIGINT"
    if "double" in t or "float" in t:              return "DOUBLE"
    if "bool" in t:                                return "BOOLEAN"
    if "timestamp" in t:                           return "TIMESTAMP"
    if "date" in t:                                return "DATE"
    if "decimal" in t:                             return "DECIMAL"
    return "STRING"   # VARCHAR requires dataLength in OM; STRING does not


def _map_topic_type(t: str) -> str:
    """Map type string to OM topic schema field dataType enum.
    Topic fields use a stricter enum: LONG not BIGINT, no TIMESTAMP/DATE."""
    t = t.lower()
    if "long" in t or "bigint" in t:    return "LONG"
    if "int" in t:                       return "INT"
    if "double" in t:                    return "DOUBLE"
    if "float" in t:                     return "FLOAT"
    if "bool" in t:                      return "BOOLEAN"
    if "byte" in t or "binary" in t:    return "BYTES"
    return "STRING"


def _avro_type(t) -> str:
    if isinstance(t, list):
        non_null = [x for x in t if x != "null"]
        return non_null[0] if non_null else "string"
    return str(t)


def health_check() -> bool:
    """Returns True if OpenMetadata server is reachable."""
    try:
        resp = requests.get(f"{OM_URL}/v1/system/version", timeout=5)
        return resp.status_code == 200
    except Exception:
        return False
