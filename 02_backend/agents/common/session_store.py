"""
Session memory — a shared Postgres checkpointer for the LangGraph agents.

LangGraph persistence works by giving a compiled graph a *checkpointer* and calling
it with a `thread_id`. The checkpointer saves the graph state after every step and
reloads it on the next call with the same thread_id — so a conversation survives
across many graph passes (and across backend restarts, since this is Postgres).

One saver is shared process-wide (one connection pool). It is created lazily on the
first request and its tables are provisioned once via `.setup()`.

Usage:
    saver = await get_checkpointer()
    graph = builder.compile(checkpointer=saver)
    graph.astream(input, config={"configurable": {"thread_id": session_id}})
"""

import asyncio
import logging

from config import POSTGRES_URL

logger = logging.getLogger(__name__)

_saver = None
_pool = None
_lock = asyncio.Lock()


async def get_checkpointer():
    """Return the process-wide AsyncPostgresSaver, creating it on first use.

    Returns None if Postgres is unreachable — callers fall back to a stateless run
    so a missing DB degrades gracefully (no memory) instead of breaking chat.
    """
    global _saver, _pool
    if _saver is not None:
        return _saver

    async with _lock:
        if _saver is not None:        # double-checked: another task won the race
            return _saver
        try:
            from psycopg_pool import AsyncConnectionPool
            from psycopg.rows import dict_row
            from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

            # autocommit + dict_row are required by AsyncPostgresSaver.
            pool = AsyncConnectionPool(
                conninfo=POSTGRES_URL,
                max_size=10,
                open=False,
                kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row},
            )
            await pool.open(wait=True, timeout=10.0)
            saver = AsyncPostgresSaver(pool)
            await saver.setup()       # idempotent — creates checkpoint tables if absent
            _pool, _saver = pool, saver
            logger.info("[session] Postgres checkpointer ready (durable session memory ON)")
        except Exception as e:
            logger.warning(f"[session] Postgres checkpointer unavailable — running stateless: {e}")
            return None
    return _saver


async def close_checkpointer():
    """Close the pool on app shutdown."""
    global _pool, _saver
    if _pool is not None:
        try:
            await _pool.close()
        except Exception:
            pass
    _pool, _saver = None, None
