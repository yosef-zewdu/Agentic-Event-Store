"""
backend/routes/pipeline.py — Pipeline control and health endpoints (Task 35)

Routes:
  POST  /api/applications/{id}/pipeline/retry  — re-enqueue a failed job
  GET   /api/applications/{id}/pipeline/status — current pipeline_jobs row
  GET   /api/ledger/health                     — projection checkpoint lags
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

import asyncpg
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.event_store import EventStore
from src.pipeline_runner import enqueue_pipeline

logger = logging.getLogger(__name__)
router = APIRouter()

# Injected by backend/main.py lifespan
_store: EventStore | None = None
_pool: asyncpg.Pool | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_pool() -> asyncpg.Pool:
    if _pool is None:
        raise HTTPException(status_code=503, detail="Database pool not ready")
    return _pool


def _job_row_to_dict(row) -> dict[str, Any]:
    if row is None:
        return {}
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
        elif isinstance(v, uuid.UUID):
            d[k] = str(v)
        elif isinstance(v, Decimal):
            d[k] = float(v)
    return d


# ---------------------------------------------------------------------------
# POST /api/applications/{id}/pipeline/retry
# ---------------------------------------------------------------------------

@router.post("/applications/{application_id}/pipeline/retry", status_code=201)
async def retry_pipeline(application_id: str):
    """
    Re-enqueue the pipeline for an application whose latest job has failed.

    Only allowed when the current job status is 'failed'.
    Returns the new job_id.
    """
    pool = _require_pool()

    async with pool.acquire() as conn:
        job = await conn.fetchrow(
            """
            SELECT job_id, status, from_agent
            FROM pipeline_jobs
            WHERE application_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            application_id,
        )

    if job is None:
        raise HTTPException(
            status_code=404,
            detail=f"No pipeline job found for application {application_id!r}",
        )

    if job["status"] != "failed":
        raise HTTPException(
            status_code=400,
            detail=f"Cannot retry — current job status is {job['status']!r}, expected 'failed'",
        )

    new_job_id = await enqueue_pipeline(pool, application_id, from_agent=job["from_agent"])

    return {
        "application_id": application_id,
        "job_id": str(new_job_id),
        "status": "queued",
    }


# ---------------------------------------------------------------------------
# GET /api/applications/{id}/pipeline/status
# ---------------------------------------------------------------------------

@router.get("/applications/{application_id}/pipeline/status")
async def get_pipeline_status(application_id: str):
    """Return the most recent pipeline_jobs row for an application."""
    pool = _require_pool()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT job_id, application_id, status, from_agent,
                   error_message, created_at, started_at, completed_at
            FROM pipeline_jobs
            WHERE application_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            application_id,
        )

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"No pipeline job found for application {application_id!r}",
        )

    return _job_row_to_dict(row)


# ---------------------------------------------------------------------------
# GET /api/ledger/health
# ---------------------------------------------------------------------------

@router.get("/ledger/health")
async def ledger_health():
    """
    Return projection checkpoint lag from projection_checkpoints table.

    Reports lag_events (events behind the latest global_position) and
    last_updated_at for each registered projection.
    """
    pool = _require_pool()

    async with pool.acquire() as conn:
        # Latest global position across all events
        max_pos_row = await conn.fetchrow("SELECT MAX(global_position) AS max_pos FROM events")
        max_pos: Optional[int] = max_pos_row["max_pos"] if max_pos_row else None

        # Projection checkpoints
        checkpoints = await conn.fetch(
            "SELECT projection_name, last_processed_position, updated_at FROM projection_checkpoints"
        )

    projections = []
    for cp in checkpoints:
        last_pos = cp["last_processed_position"] or 0
        lag = (max_pos - last_pos) if max_pos is not None else 0
        projections.append({
            "projection_name": cp["projection_name"],
            "last_processed_position": last_pos,
            "lag_events": max(lag, 0),
            "updated_at": cp["updated_at"].isoformat() if cp["updated_at"] else None,
        })

    return {
        "status": "ok",
        "max_global_position": max_pos,
        "projections": projections,
    }
