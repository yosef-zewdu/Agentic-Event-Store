"""
src/worker/main.py — Worker_Service entrypoint (Tasks 28.1, 30.4)

Runs three concurrent asyncio tasks:
  1. ProjectionDaemon     — keeps projection tables current
  2. OutboxProcessor      — delivers transactional outbox rows
  3. PipelineWorker       — executes queued async pipeline jobs (Phase 8A)

Uses the DIRECT (non-pooled) Neon connection string — required because:
  - ProjectionDaemon holds a persistent LISTEN/NOTIFY connection
  - pgBouncer transaction mode (used by pooled endpoint) breaks LISTEN/NOTIFY
    and long-running transactions
  - PipelineWorker runs long-lived agent pipelines that must not be interrupted
    by pgBouncer statement/transaction timeouts

Start with:
    python -m src.worker.main

Environment variables:
    DATABASE_URL_DIRECT  — direct Neon connection string (non-pooled)
    DATABASE_URL         — fallback if DATABASE_URL_DIRECT is not set
    OPENROUTER_API_KEY   — required for pipeline agent LLM calls
    OPENROUTER_MODEL     — model override (default: google/gemini-2.0-flash-001)
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    # Use direct connection string for the worker — see module docstring
    db_url = os.environ.get("DATABASE_URL_DIRECT") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL_DIRECT (or DATABASE_URL) environment variable is not set."
        )

    logger.info("Worker_Service starting — connecting to database…")
    pool = await asyncpg.create_pool(
        db_url,
        min_size=2,
        max_size=5,
        command_timeout=30.0,
    )

    from src.event_store import EventStore
    from src.upcasting.registry import registry as upcaster_registry
    from src.projections.application_summary import ApplicationSummaryProjection
    from src.projections.agent_performance import AgentPerformanceLedgerProjection
    from src.projections.compliance_audit import ComplianceAuditViewProjection
    from src.projections.daemon import ProjectionDaemon
    from src.outbox.processor import OutboxProcessor

    store = EventStore(db_url=db_url, upcaster_registry=upcaster_registry)
    store._pool = pool  # reuse the pool we just created

    # Ensure projection tables exist before starting the daemon
    app_proj = ApplicationSummaryProjection()
    agent_proj = AgentPerformanceLedgerProjection()
    compliance_proj = ComplianceAuditViewProjection()

    async with pool.acquire() as conn:
        await app_proj.ensure_table_exists(conn)
        await agent_proj.ensure_table_exists(conn)
        await compliance_proj.ensure_table_exists(conn)

    logger.info("Projection tables ready.")

    daemon = ProjectionDaemon(store=store, pool=pool)
    daemon.register(app_proj)
    daemon.register(agent_proj)
    daemon.register(compliance_proj)

    outbox = OutboxProcessor(pool=pool)

    from src.worker.pipeline_worker import run_forever as pipeline_run_forever

    # Graceful shutdown on SIGTERM / SIGINT
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("Shutdown signal received.")
        stop_event.set()
        daemon.stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    logger.info("Starting ProjectionDaemon, OutboxProcessor, and PipelineWorker…")
    try:
        await asyncio.gather(
            daemon.run_forever(poll_interval_ms=100),
            outbox.run_forever(poll_interval_ms=500),
            pipeline_run_forever(store=store, pool=pool),
        )
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Worker_Service shutting down — closing pool…")
        await pool.close()
        logger.info("Worker_Service stopped (ProjectionDaemon + OutboxProcessor + PipelineWorker).")


if __name__ == "__main__":
    asyncio.run(main())
