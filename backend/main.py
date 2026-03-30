"""
backend/main.py — Apex Financial Services Loan Platform API

Production FastAPI backend for the loan origination platform.
Handles application submission, document upload, pipeline triggering,
and human review workflow.

Run with:
    PYTHONPATH=. uvicorn backend.main:app --port 8000
"""
from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from src.event_store import EventStore
from src.upcasting.registry import registry as upcaster_registry

load_dotenv(Path(__file__).parent.parent / ".env")

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

_store: EventStore | None = None
_pool: asyncpg.Pool | None = None


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _store, _pool
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")

    # Pooled connection — for HTTP request handlers (pgBouncer-compatible)
    _store = EventStore(db_url=db_url, upcaster_registry=upcaster_registry)
    await _store.connect()
    _pool = _store._pool

    # Direct (non-pooled) connection — required for ProjectionDaemon LISTEN/NOTIFY,
    # OutboxProcessor long-running transactions, and PipelineWorker agent pipelines.
    # Falls back to DATABASE_URL if DATABASE_URL_DIRECT is not set (e.g. local dev).
    direct_url = os.environ.get("DATABASE_URL_DIRECT") or db_url
    _direct_pool = await asyncpg.create_pool(direct_url, min_size=2, max_size=5, command_timeout=30.0)

    # Ensure projection tables exist
    from src.projections.application_summary import ApplicationSummaryProjection
    from src.projections.agent_performance import AgentPerformanceLedgerProjection
    from src.projections.compliance_audit import ComplianceAuditViewProjection
    from src.projections.daemon import ProjectionDaemon
    from src.outbox.processor import OutboxProcessor
    from src.worker.pipeline_worker import run_forever as pipeline_run_forever

    app_proj = ApplicationSummaryProjection()
    agent_proj = AgentPerformanceLedgerProjection()
    compliance_proj = ComplianceAuditViewProjection()

    async with _direct_pool.acquire() as conn:
        await app_proj.ensure_table_exists(conn)
        await agent_proj.ensure_table_exists(conn)
        await compliance_proj.ensure_table_exists(conn)

    # Worker tasks — run alongside the API in the same process
    _worker_store = EventStore(db_url=direct_url, upcaster_registry=upcaster_registry)
    _worker_store._pool = _direct_pool

    daemon = ProjectionDaemon(store=_worker_store, pool=_direct_pool)
    daemon.register(app_proj)
    daemon.register(agent_proj)
    daemon.register(compliance_proj)
    _daemon_task = asyncio.create_task(daemon.run_forever(poll_interval_ms=200))
    _outbox_task = asyncio.create_task(OutboxProcessor(pool=_direct_pool).run_forever(poll_interval_ms=500))
    _pipeline_task = asyncio.create_task(pipeline_run_forever(store=_worker_store, pool=_direct_pool))

    # Wire up deps.py singletons for any code that uses get_store()/get_pool()
    from backend.deps import set_singletons
    set_singletons(_store, _pool)

    # Also inject directly into route modules (module-level _store/_pool pattern)
    from backend.routes import applications, documents, pipeline, review
    for module in (applications, documents, pipeline, review):
        module._store = _store
        module._pool = _pool

    yield

    _daemon_task.cancel()
    _outbox_task.cancel()
    _pipeline_task.cancel()
    await _store.close()
    await _direct_pool.close()
    _store = None
    _pool = None


app = FastAPI(
    title="Apex Financial Services — Loan Platform API",
    version="1.0.0",
    lifespan=_lifespan,
)

# CORS
_frontend_origin = os.environ.get("FRONTEND_ORIGIN", "")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[_frontend_origin] if _frontend_origin else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
from backend.routes import applications, documents, pipeline, review

app.include_router(applications.router, prefix="/api")
app.include_router(documents.router,    prefix="/api")
app.include_router(pipeline.router,     prefix="/api")
app.include_router(review.router,       prefix="/api")


@app.get("/api/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}
