"""
Cloudera Schema Registry client — BASIC auth over Knox gateway.

Config env vars:
  SCHEMA_REGISTRY_URL   — e.g. https://cdp-utility.cdp.local:8443/gateway/cdp-proxy-api/schema-registry
  KNOX_USERNAME         — CDP username
  KNOX_PASSWORD         — CDP password

Two API surfaces exposed by Cloudera SR:
  Native API  /api/v1/schemaregistry/...  — registration, aggregated fetch
  Compat API  /subjects/...               — read-only Confluent wire-format compat (fetch/decode)
"""
import json
import logging
import struct
from functools import lru_cache
from typing import Any

import config
import requests

logger = logging.getLogger(__name__)

SR_URL      = config.SCHEMA_REGISTRY_URL
SR_USER     = config.KNOX_USERNAME
SR_PASSWORD = config.KNOX_PASSWORD

_AUTH = (SR_USER, SR_PASSWORD) if SR_USER else None

_MOCK_AVRO_SCHEMA = {
    "type": "record",
    "name": "MockEvent",
    "fields": [
        {"name": "id",      "type": "string"},
        {"name": "ts",      "type": "long"},
        {"name": "payload", "type": "string"},
    ],
}


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _get_json(path: str) -> Any:
    if not SR_URL:
        raise RuntimeError("SCHEMA_REGISTRY_URL is not configured")
    resp = requests.get(
        f"{SR_URL.rstrip('/')}{path}",
        auth=_AUTH, timeout=30, verify=False,
    )
    resp.raise_for_status()
    return resp.json()


def _post_json(path: str, body: dict) -> Any:
    if not SR_URL:
        raise RuntimeError("SCHEMA_REGISTRY_URL is not configured")
    resp = requests.post(
        f"{SR_URL.rstrip('/')}{path}",
        json=body, auth=_AUTH, timeout=15, verify=False,
    )
    resp.raise_for_status()
    return resp.json() if resp.text.strip() else {}


# ── Public API ────────────────────────────────────────────────────────────────

@lru_cache(maxsize=512)
def fetch_schema_by_id(schema_id: int) -> dict:
    """Fetch an Avro schema by its numeric ID (Confluent compat endpoint). LRU-cached."""
    if not SR_URL:
        return _MOCK_AVRO_SCHEMA
    try:
        data = _get_json(f"/schemas/ids/{schema_id}")
        raw  = data.get("schema", "{}")
        return json.loads(raw) if isinstance(raw, str) else raw
    except Exception as e:
        logger.warning(f"[schema_registry] fetch_schema_by_id({schema_id}) failed: {e}")
        return _MOCK_AVRO_SCHEMA


def fetch_topic_schema(topic: str, is_key: bool = False) -> dict:
    """Fetch the latest schema for a topic via the Confluent compat endpoint (read-only)."""
    subject = f"{topic}{'-key' if is_key else '-value'}"
    if not SR_URL:
        return _MOCK_AVRO_SCHEMA
    try:
        data   = _get_json(f"/subjects/{subject}/versions/latest")
        raw    = data.get("schema", "{}")
        schema = json.loads(raw) if isinstance(raw, str) else raw
        logger.debug(f"[schema_registry] fetched schema for {subject!r} id={data.get('id')}")
        return schema
    except Exception as e:
        logger.warning(f"[schema_registry] fetch_topic_schema({subject!r}) failed: {e}")
        return _MOCK_AVRO_SCHEMA


def list_subjects() -> list[str]:
    """List all subjects via the Confluent compat endpoint."""
    if not SR_URL:
        return []
    try:
        return _get_json("/subjects")
    except Exception as e:
        logger.warning(f"[schema_registry] list_subjects failed: {e}")
        return []


def register_schema(
    name: str,
    avro_schema: dict,
    description: str = "",
    schema_group: str = "Kafka",
    compatibility: str = "BACKWARD",
) -> int | None:
    """Register a new Avro schema via the Cloudera native API.

    Returns the schema ID on success, None on failure.
    POST /api/v1/schemaregistry/schemas  — idempotent (409 = already exists).
    """
    if not SR_URL:
        logger.debug("[schema_registry] register_schema skipped — no SR URL")
        return None
    body = {
        "type":            "avro",
        "schemaGroup":     schema_group,
        "name":            name,
        "description":     description,
        "compatibility":   compatibility,
        "validationLevel": "ALL",
        "schemaText":      json.dumps(avro_schema),
    }
    try:
        resp = requests.post(
            f"{SR_URL.rstrip('/')}/api/v1/schemaregistry/schemas",
            json=body, auth=_AUTH, timeout=15, verify=False,
        )
        if resp.status_code == 409:
            logger.info(f"[schema_registry] schema already exists: {name!r}")
            return None
        resp.raise_for_status()
        schema_id = int(resp.text.strip())
        logger.info(f"[schema_registry] registered {name!r} id={schema_id}")
        return schema_id
    except Exception as e:
        logger.warning(f"[schema_registry] register_schema({name!r}) failed: {e}")
        return None


def decode_avro_message(raw_bytes: bytes) -> dict[str, Any]:
    """Decode a Confluent-framed Avro message.

    Frame: [0x00][4-byte schema_id big-endian][avro payload]
    Requires fastavro; degrades gracefully if not installed.
    """
    if not raw_bytes or len(raw_bytes) < 5 or raw_bytes[0] != 0x00:
        return {"_raw": raw_bytes[:64].hex() if raw_bytes else ""}

    schema_id  = struct.unpack(">I", raw_bytes[1:5])[0]
    avro_bytes = raw_bytes[5:]

    try:
        import io
        import fastavro
        schema = fetch_schema_by_id(schema_id)
        parsed = fastavro.parse_schema(schema)
        return dict(fastavro.schemaless_reader(io.BytesIO(avro_bytes), parsed))
    except ImportError:
        logger.debug("[schema_registry] fastavro not installed — returning schema_id only")
        return {"schema_id": schema_id, "_raw": avro_bytes[:32].hex()}
    except Exception as e:
        logger.warning(f"[schema_registry] avro decode failed (id={schema_id}): {e}")
        return {"schema_id": schema_id, "_error": str(e)}
