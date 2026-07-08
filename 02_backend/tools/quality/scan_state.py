"""
Quality Guardian — scan-state store (backs the freshness gate).

Records, per asset: the data 'version' (Iceberg snapshot id) at the time of the last
scan, when it ran, and the cached basic scorecard + sample profile. This lets the agent
SKIP re-scanning when the table hasn't received a new snapshot since last time — the
cheapest possible "don't run every time" check (one metadata read, no table scan).

JSON-backed for single-node today. For multi-replica / 100-user scale this should move
to Postgres (memory/postgres_cache.get_connection) — the interface here is deliberately
tiny (get_last / save) so that swap is a drop-in.
"""

import json
import logging
import os
import pathlib
import threading
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_STORE = pathlib.Path(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))) / "memory" / "qg_scan_state.json"
_LOCK = threading.Lock()


def _load() -> dict:
    try:
        if _STORE.exists():
            return json.loads(_STORE.read_text())
    except Exception as e:
        logger.warning(f"[qg_scan_state] read failed: {e}")
    return {}


def _save(data: dict) -> None:
    try:
        _STORE.parent.mkdir(parents=True, exist_ok=True)
        _STORE.write_text(json.dumps(data, indent=0))
    except Exception as e:
        logger.warning(f"[qg_scan_state] write failed: {e}")


def get_last(asset: str) -> Optional[dict]:
    """Last scan record for an asset: {version, scanned_at, basic, profile} or None."""
    return _load().get(asset)


def save(asset: str, version: Optional[str], basic: dict, profile: dict) -> None:
    """Persist the version + cached results from a completed scan."""
    with _LOCK:
        data = _load()
        data[asset] = {
            "version": version,
            "scanned_at": datetime.now(timezone.utc).isoformat(),
            "basic": basic,
            "profile": profile,
        }
        _save(data)


def is_unchanged(asset: str, version: Optional[str]) -> bool:
    """True iff we have a prior scan AND the data version matches it (→ safe to skip).
    A None version (non-Iceberg / unknown) is never considered unchanged — fail open."""
    if not version:
        return False
    last = get_last(asset)
    return bool(last and last.get("version") == version)
