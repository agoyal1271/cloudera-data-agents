"""
Schema Registry indexer — fetches all schemas from Cloudera SR and caches them.

Uses the Cloudera native aggregated endpoint:
  GET /api/v1/schemaregistry/schemas/aggregated
One call returns all schemas including latest schemaText. Fast even at 10k+ topics.
"""
import logging
import threading
import time
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

_state: Dict[str, Any] = {
    "status": "idle",       # idle | indexing | done | error
    "total": 0,
    "started_at": None,
    "finished_at": None,
    "error": None,
    "sr_type": None,
}
_state_lock = threading.Lock()


def _set(**kw) -> None:
    with _state_lock:
        _state.update(kw)


def get_indexing_status() -> dict:
    """Returns current indexer state merged with live cache stats."""
    from tools.kafka.schema_registry_cache import get_stats
    with _state_lock:
        out = dict(_state)
    try:
        out.update(get_stats())
    except Exception:
        pass
    return out


def run_index() -> dict:
    """
    Index the Schema Registry into SQLite.
    Also adds T-Life demo topics from local cache.
    Returns {"indexed": N, "sr_type": "...", "error": "..."}.
    Thread-safe — safe to call from a background task.
    """
    from config import SCHEMA_REGISTRY_URL
    from tools.kafka.schema_registry_cache import init_db, store_schemas_bulk

    _set(status="indexing", started_at=time.time(), error=None, sr_type="cloudera")
    init_db()

    schemas = []
    sr_count = 0

    if SCHEMA_REGISTRY_URL:
        try:
            schemas = _index_cloudera()
            sr_count = len(schemas)
            logger.info(f"[sr_indexer] Fetched {sr_count} schemas from Cloudera SR")
        except Exception as e:
            logger.warning(f"[sr_indexer] Live SR fetch failed ({e}), using offline cache")
            # Fallback: use offline cache

    # Also index T-Life demo topics from local cache
    try:
        tlife_schemas = _index_tlife()
        schemas.extend(tlife_schemas)
        logger.info(f"[sr_indexer] Added {len(tlife_schemas)} T-Life demo topics")
    except Exception as e:
        logger.debug(f"[sr_indexer] T-Life indexing skipped: {e}")

    try:
        count = store_schemas_bulk(schemas)

        # Push to unified ChromaDB catalog for semantic search
        try:
            from tools.catalog import catalog_store
            catalog_store.index_schemas_bulk(schemas)
        except Exception as _ce:
            logger.debug(f"[sr_indexer] catalog push skipped: {_ce}")

        _set(status="done", total=count, finished_at=time.time())
        logger.info(f"[sr_indexer] Indexed {count} total schemas ({sr_count} from SR + {len(schemas)-sr_count} T-Life)")
        return {"indexed": count, "sr_type": "cloudera", "tlife_demo_topics": len(schemas) - sr_count}

    except Exception as e:
        _set(status="error", error=str(e), finished_at=time.time())
        logger.warning(f"[sr_indexer] Indexing failed: {e}")
        return {"indexed": 0, "error": str(e), "sr_type": "cloudera"}


# ── Cloudera SR ───────────────────────────────────────────────────────────────

def _index_cloudera() -> List[dict]:
    """
    Single aggregated call to Cloudera Schema Registry.
    Returns all schemas including their latest version's schemaText.

    The aggregated endpoint shape:
    {
      "entities": [{
        "schemaMetadata": {"name": "orders", "type": "avro", "schemaGroup": "Kafka", ...},
        "id": 42,
        "schemaVersionInfos": [{"version": 1, "schemaText": "{...}", ...}],
        ...
      }]
    }
    """
    from tools.kafka.schema_registry import _get_json

    data = _get_json("/api/v1/schemaregistry/schemas/aggregated")
    entities = data.get("entities", [])
    logger.info(f"[sr_indexer] Cloudera SR returned {len(entities)} entities")

    schemas = []
    for entity in entities:
        meta = entity.get("schemaMetadata", {})
        name = meta.get("name", "")
        if not name:
            continue

        # Cloudera SR nests schemaVersionInfos inside schemaBranches (not at entity level).
        # Collect all versions across all branches, then pick the highest version number.
        all_versions = []
        for branch in entity.get("schemaBranches", []):
            all_versions.extend(branch.get("schemaVersionInfos", []))
            # rootSchemaVersion is a fallback when schemaVersionInfos is empty
            rsv = branch.get("rootSchemaVersion")
            if rsv and rsv not in all_versions:
                all_versions.append(rsv)
        # Also check top-level schemaVersionInfos for older SR versions
        all_versions.extend(entity.get("schemaVersionInfos", []))
        latest = max(all_versions, key=lambda v: v.get("version", 0)) if all_versions else {}

        schemas.append({
            "name": name,
            "schemaGroup": meta.get("schemaGroup", "Kafka"),
            "type": meta.get("type", "avro"),
            "compatibility": meta.get("compatibility", "BACKWARD"),
            "description": meta.get("description", ""),
            "id": entity.get("id"),
            "version": latest.get("version"),
            "schema": latest.get("schemaText") or latest.get("schema", ""),
        })

    return schemas


# ── T-Life Demo Topics (Offline Cache) ────────────────────────────────────────

def _index_tlife() -> List[dict]:
    """
    Index T-Life demo topics from local SQLite cache.
    Used when live Schema Registry is unavailable.
    """
    from tools.kafka.tlife_schema_cache import init_tlife_cache, get_tlife_schema, list_tlife_schemas
    import json

    try:
        init_tlife_cache()
    except Exception as e:
        logger.debug(f"[sr_indexer] T-Life cache init failed: {e}")

    schemas = []
    for topic_name in list_tlife_schemas():
        schema_dict = get_tlife_schema(topic_name)
        if not schema_dict:
            continue

        schemas.append({
            "name": topic_name,
            "topic_name": topic_name,
            "schemaGroup": "T-Life Demo",
            "type": "avro",
            "schema": json.dumps(schema_dict),
            "version": 1,
            "is_tlife_demo": True,
        })

    logger.info(f"[sr_indexer] Indexed {len(schemas)} T-Life demo topics")
    return schemas
