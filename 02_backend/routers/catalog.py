"""
Catalog search — lightweight asset typeahead for the shared Asset Picker.

  GET /api/catalog/search?q=&type=&limit=

Returns top-N matching assets suitable for an autocomplete picker — never the full
list (that doesn't scale to hundreds of schemas). Semantic match via the Qdrant
catalog index (the same one Source Scout uses) when it's populated; falls back to a
cached name-substring filter over the live Iceberg catalog when it isn't.
"""

import asyncio
import logging
import time

from fastapi import APIRouter, Query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/catalog", tags=["catalog"])

# Cache the live table list so a fallback search doesn't hit Knox on every keystroke.
_TABLES_CACHE: dict = {"ts": 0.0, "rows": []}
_TABLES_TTL = 600.0


def _all_iceberg_tables() -> list[dict]:
    if _TABLES_CACHE["rows"] and time.monotonic() - _TABLES_CACHE["ts"] < _TABLES_TTL:
        return _TABLES_CACHE["rows"]
    from tools.iceberg.iceberg_tools import list_iceberg_tables
    rows = list_iceberg_tables() or []
    _TABLES_CACHE.update(ts=time.monotonic(), rows=rows)
    return rows


def _row(h: dict) -> dict:
    return {
        "name": h.get("name", ""),
        "type": h.get("asset_type", "") or "iceberg_table",
        "namespace": h.get("namespace", ""),
        "field_count": h.get("field_count", 0),
        "similarity": round(h.get("similarity", 0.0), 3) if h.get("similarity") is not None else None,
    }


@router.post("/refresh")
async def refresh_catalog():
    """Force-refresh the Iceberg table list and re-index into Qdrant.
    Call this after creating or dropping tables so they appear in search immediately,
    without waiting for the 600s TTL to expire."""
    from tools.iceberg.iceberg_tools import list_iceberg_tables, invalidate_iceberg_list_cache
    t0 = time.monotonic()
    invalidate_iceberg_list_cache()
    _TABLES_CACHE.update(ts=0.0, rows=[])   # also bust this router's own cache
    tables = await asyncio.to_thread(list_iceberg_tables, True)
    return {
        "refreshed": True,
        "table_count": len(tables),
        "ms": round((time.monotonic() - t0) * 1000),
    }


@router.get("/search")
async def search(
    q: str = Query(default="", description="Search term (table/field/namespace)"),
    type: str = Query(default="iceberg_table", description="Asset type filter"),
    limit: int = Query(default=20, le=100),
):
    # 1. Semantic search if the catalog is indexed (finds by name AND field meaning)
    try:
        from tools.catalog import catalog_store
        stats = await asyncio.to_thread(catalog_store.get_stats)
        if stats.get("available") and q.strip():
            hits = await asyncio.to_thread(
                catalog_store.search, q.strip(), [type] if type else None, limit
            )
            if hits:
                return {"results": [_row(h) for h in hits], "source": "semantic", "count": len(hits)}
    except Exception as e:
        logger.debug(f"[catalog] semantic search skipped: {e}")

    # 2. Fallback: name-substring over the cached Iceberg catalog
    try:
        rows = await asyncio.to_thread(_all_iceberg_tables)
        ql = q.strip().lower()
        matched = [r for r in rows if not ql or ql in (r.get("name", "").lower())][:limit]
        results = [{
            "name": r.get("name", ""),
            "type": "iceberg_table",
            "namespace": r.get("name", "").split(".")[0] if "." in r.get("name", "") else "",
            "field_count": len(r.get("fields", []) or []),
            "similarity": None,
        } for r in matched]
        return {"results": results, "source": "name", "count": len(results)}
    except Exception as e:
        logger.warning(f"[catalog] search fallback failed: {e}")
        return {"results": [], "source": "none", "count": 0, "error": str(e)}
