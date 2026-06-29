import logging
import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

sys.path.insert(0, os.path.dirname(__file__))

from routers import agents, health, nl_to_code, registry, knox, pipeline, openmetadata, scout_chat, quality, quality_guardian, catalog, supervisor, analyst
from middleware.jwt_identity import JWTIdentityMiddleware

DEBUG_LOG = "/tmp/cloudera_agents_debug.log"


def _attach_file_logger():
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s")
    fh = logging.FileHandler(DEBUG_LOG, mode="a")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logging.root.setLevel(logging.DEBUG)
    logging.root.addHandler(fh)
    for noisy in ("uvicorn", "uvicorn.access", "httpx", "httpcore", "asyncio", "multipart"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    logging.getLogger(__name__).info(f"[startup] debug log → {DEBUG_LOG}")


async def _knox_refresh_loop():
    """
    Background task: checks Knox JWT expiry every 60 s.
    get_valid_knox_token() is a no-op when the token is still fresh, so this is cheap.
    When the token is within KNOX_TOKEN_REFRESH_BUFFER_SECS of expiry it fetches a new one.
    """
    import asyncio
    from agents.source_scout.sidecar import get_valid_knox_token

    _log = logging.getLogger(__name__)
    while True:
        await asyncio.sleep(60)
        try:
            await asyncio.to_thread(get_valid_knox_token)
        except Exception as exc:
            _log.warning(f"[knox_loop] refresh check failed: {exc}")


async def _iceberg_catalog_refresh_loop():
    """Background task: re-indexes the Iceberg catalog into Qdrant every 5 minutes.
    Any table created in Impala (outside the agent) is discoverable within one cycle."""
    import asyncio
    from tools.iceberg.iceberg_tools import list_iceberg_tables, invalidate_iceberg_list_cache

    _log = logging.getLogger(__name__)
    while True:
        await asyncio.sleep(300)   # 5-minute cycle
        try:
            invalidate_iceberg_list_cache()
            tables = await asyncio.to_thread(list_iceberg_tables, True)
            _log.debug(f"[catalog_loop] refreshed {len(tables)} tables into Qdrant")
        except Exception as exc:
            _log.debug(f"[catalog_loop] refresh skipped: {exc}")


async def _llm_keepwarm_loop():
    """
    Keeps the local LLM loaded so the first Source Scout query isn't cold.
    Pings once at boot, then every 240 s — under Ollama's default 300 s idle-unload.
    """
    import asyncio
    import httpx
    from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY

    _log = logging.getLogger(__name__)
    while True:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                await client.post(
                    f"{LLM_BASE_URL}/chat/completions",
                    json={"model": LLM_MODEL, "messages": [{"role": "user", "content": "ping"}], "max_tokens": 1},
                    headers={"Authorization": f"Bearer {LLM_API_KEY}"},
                )
        except Exception as exc:
            _log.debug(f"[llm_warm] ping failed: {exc}")
        await asyncio.sleep(240)


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio
    _attach_file_logger()
    _log = logging.getLogger(__name__)

    # ── Knox JWT background refresh ──────────────────────────────────────────
    from config import KNOX_LOGIN_URL
    knox_task = None
    if KNOX_LOGIN_URL:
        knox_task = asyncio.create_task(_knox_refresh_loop())
        _log.info("[startup] Knox JWT auto-refresh loop started (checks every 60 s)")

    # ── LLM keep-warm (avoid cold-model latency on the first query) ──────────
    llm_task = asyncio.create_task(_llm_keepwarm_loop())
    _log.info("[startup] LLM keep-warm loop started (pings every 240 s)")

    catalog_task = asyncio.create_task(_iceberg_catalog_refresh_loop())
    _log.info("[startup] Iceberg catalog refresh loop started (re-indexes every 300 s)")

    # ── Session memory (Postgres checkpointer) ───────────────────────────────
    try:
        from agents.common.session_store import get_checkpointer
        saver = await get_checkpointer()
        _log.info(f"[startup] Session memory {'ON (Postgres)' if saver else 'OFF (Postgres unavailable)'}")
    except Exception as exc:
        _log.warning(f"[startup] Session memory init skipped: {exc}")

    # ── Schema Registry auto-index ───────────────────────────────────────────
    from config import SCHEMA_REGISTRY_URL
    if SCHEMA_REGISTRY_URL:
        try:
            from tools.kafka.schema_registry_cache import init_db, is_stale, get_stats
            init_db()

            # Warm cache synchronously if empty (with timeout)
            stats = get_stats()
            if stats.get("count", 0) == 0:
                _log.info("[startup] Schema Registry cache empty, warm-indexing (timeout: 30s)...")
                from tools.kafka.schema_registry_indexer import run_index
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(run_index),
                        timeout=30.0
                    )
                    _log.info("[startup] Schema Registry warm-indexed successfully")
                except asyncio.TimeoutError:
                    # Timeout: continue with empty cache, background refresh will populate it
                    asyncio.get_event_loop().run_in_executor(None, run_index)
                    _log.warning("[startup] SR warm-index timed out, retrying in background")
            elif is_stale():
                # Cache exists but is stale: refresh in background
                asyncio.get_event_loop().run_in_executor(None, run_index)
                _log.info("[startup] Schema Registry cache stale, background refresh triggered")
        except Exception as exc:
            _log.warning(f"[startup] SR auto-index skipped: {exc}")

    # ── Iceberg catalog warm-up (populate Qdrant for discover search) ─────────
    # list_iceberg_tables() indexes into Qdrant when it runs — calling it here
    # ensures MOCK_TABLES (including order_analytics_mart etc.) are searchable
    # before the first user query, even when the live catalog is unreachable.
    try:
        from tools.iceberg.iceberg_tools import list_iceberg_tables
        tables = await asyncio.to_thread(list_iceberg_tables)
        _log.info(f"[startup] Iceberg catalog warm-indexed ({len(tables)} tables)")
    except Exception as exc:
        _log.warning(f"[startup] Iceberg catalog warm-up skipped: {exc}")

    yield

    if knox_task:
        knox_task.cancel()
    llm_task.cancel()
    catalog_task.cancel()
    try:
        from agents.common.session_store import close_checkpointer
        await close_checkpointer()
    except Exception:
        pass


app = FastAPI(title="Cloudera AI Agents", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(JWTIdentityMiddleware)

app.include_router(agents.router)
app.include_router(health.router)
app.include_router(nl_to_code.router)
app.include_router(registry.router)
app.include_router(knox.router)
app.include_router(pipeline.router)
app.include_router(openmetadata.router)
app.include_router(scout_chat.router)
app.include_router(quality.router)
app.include_router(quality_guardian.router)
app.include_router(catalog.router)
app.include_router(supervisor.router)
app.include_router(analyst.router)

# Serve frontend build if it exists
FRONTEND_DIST = os.path.join(os.path.dirname(__file__), "..", "03_frontend", "dist")
if os.path.isdir(FRONTEND_DIST):
    app.mount("/", StaticFiles(directory=FRONTEND_DIST, html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    from config import APP_PORT
    uvicorn.run("app:app", host="0.0.0.0", port=APP_PORT, reload=True)
