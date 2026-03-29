"""
src/worker/pipeline_worker.py — PipelineWorker background loop (Phase 8A, Task 30.3)

Polls pipeline_jobs every 5 seconds and executes queued jobs via run_pending_jobs().
Runs as a third asyncio task alongside ProjectionDaemon and OutboxProcessor in
src/worker/main.py, sharing the same direct-connection pool.
"""
from __future__ import annotations

import asyncio
import logging

import asyncpg

from src.event_store import EventStore
from src.pipeline_runner import run_pending_jobs

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 5


async def run_forever(store: EventStore, pool: asyncpg.Pool) -> None:
    """Poll for queued pipeline jobs and execute them indefinitely."""
    logger.info("PipelineWorker started — polling every %ss", POLL_INTERVAL_SECONDS)
    while True:
        try:
            processed = await run_pending_jobs(store, pool)
            if processed == 0:
                # Nothing to do — wait before next poll
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
            # If a job was processed, immediately check for more without sleeping
        except asyncio.CancelledError:
            logger.info("PipelineWorker cancelled — shutting down.")
            return
        except Exception as exc:
            # Log and keep running — individual job failures are handled inside run_pending_jobs
            logger.error("PipelineWorker unexpected error: %s", exc, exc_info=True)
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
