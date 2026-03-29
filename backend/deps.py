"""
backend/deps.py — Shared singletons and FastAPI dependency functions.

`_store` and `_pool` are module-level singletons initialised once during
`backend/main.py` lifespan and then injected into route handlers via the
`get_store()` / `get_pool()` FastAPI dependency functions.
"""
from __future__ import annotations

from typing import AsyncGenerator

import asyncpg
from fastapi import HTTPException

from src.event_store import EventStore

# ---------------------------------------------------------------------------
# Module-level singletons — set by backend/main.py lifespan
# ---------------------------------------------------------------------------

_store: EventStore | None = None
_pool: asyncpg.Pool | None = None


def set_singletons(store: EventStore, pool: asyncpg.Pool) -> None:
    """Called once from the lifespan to wire up the singletons."""
    global _store, _pool
    _store = store
    _pool = pool


# ---------------------------------------------------------------------------
# FastAPI dependency functions
# ---------------------------------------------------------------------------

async def get_store() -> EventStore:
    if _store is None:
        raise HTTPException(status_code=503, detail="EventStore not initialised")
    return _store


async def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise HTTPException(status_code=503, detail="Database pool not initialised")
    return _pool
