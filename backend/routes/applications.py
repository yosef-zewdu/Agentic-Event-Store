"""
backend/routes/applications.py — Application lifecycle endpoints (Task 32)

Routes:
  POST   /api/applications                  — submit + enqueue pipeline
  GET    /api/applications                  — list all (optional ?status= filter)
  GET    /api/applications/{id}             — single summary
  GET    /api/applications/{id}/status      — lightweight status for frontend polling
  GET    /api/applications/{id}/events      — full event stream
  GET    /api/applications/{id}/compliance  — compliance audit view
  GET    /api/applications/{id}/agents      — agent sessions
  GET    /api/applications/{id}/analysis    — structured analysis summary
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

import asyncpg
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.commands.handlers import handle_submit_application
from src.event_store import EventStore
from src.pipeline_runner import enqueue_pipeline

logger = logging.getLogger(__name__)
router = APIRouter()

# Injected by backend/main.py lifespan
_store: EventStore | None = None
_pool: asyncpg.Pool | None = None


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class SubmitApplicationRequest(BaseModel):
    application_id: Optional[str] = None
    applicant_id: str
    requested_amount_usd: float
    loan_purpose: str = "working_capital"
    loan_term_months: int = 12
    contact_email: str = ""
    contact_name: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_store() -> EventStore:
    if _store is None:
        raise HTTPException(status_code=503, detail="EventStore not ready")
    return _store


def _require_pool() -> asyncpg.Pool:
    if _pool is None:
        raise HTTPException(status_code=503, detail="Database pool not ready")
    return _pool


def _row_to_dict(row) -> dict[str, Any]:
    """Convert an asyncpg Record to a plain dict with serialisable values."""
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
        elif isinstance(v, Decimal):
            d[k] = float(v)
        elif isinstance(v, uuid.UUID):
            d[k] = str(v)
    return d


# ---------------------------------------------------------------------------
# POST /api/applications
# ---------------------------------------------------------------------------

@router.post("/applications", status_code=201)
async def submit_application(body: SubmitApplicationRequest):
    """Submit a new loan application and enqueue the agent pipeline."""
    store = _require_store()
    pool = _require_pool()

    application_id = body.application_id or f"APEX-{uuid.uuid4().hex[:8].upper()}"

    try:
        await handle_submit_application(
            store,
            application_id=application_id,
            applicant_id=body.applicant_id,
            requested_amount_usd=Decimal(str(body.requested_amount_usd)),
            loan_purpose=body.loan_purpose,
            loan_term_months=body.loan_term_months,
            contact_email=body.contact_email,
            contact_name=body.contact_name,
        )
    except Exception as exc:
        logger.error("handle_submit_application failed: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc))

    job_id = await enqueue_pipeline(pool, application_id)

    return {"application_id": application_id, "job_id": str(job_id), "status": "submitted"}


# ---------------------------------------------------------------------------
# GET /api/applications
# ---------------------------------------------------------------------------

@router.get("/applications")
async def list_applications(status: Optional[str] = Query(default=None)):
    """List all applications from the application_summary projection."""
    pool = _require_pool()

    async with pool.acquire() as conn:
        if status:
            rows = await conn.fetch(
                "SELECT * FROM application_summary WHERE state = $1 ORDER BY last_event_at DESC NULLS LAST",
                status,
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM application_summary ORDER BY last_event_at DESC NULLS LAST"
            )

    return [_row_to_dict(r) for r in rows]


# ---------------------------------------------------------------------------
# GET /api/applications/{id}
# ---------------------------------------------------------------------------

@router.get("/applications/{application_id}")
async def get_application(application_id: str):
    """Return a single application summary from the projection."""
    pool = _require_pool()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            application_id,
        )

    if row is None:
        raise HTTPException(status_code=404, detail=f"Application {application_id!r} not found")

    return _row_to_dict(row)


# ---------------------------------------------------------------------------
# GET /api/applications/{id}/status  — lightweight polling endpoint
# ---------------------------------------------------------------------------

@router.get("/applications/{application_id}/status")
async def get_application_status(application_id: str):
    """
    Lightweight status endpoint for frontend polling.

    Returns application state + latest pipeline_jobs row.
    """
    pool = _require_pool()

    async with pool.acquire() as conn:
        summary = await conn.fetchrow(
            "SELECT state, last_event_type, last_event_at FROM application_summary WHERE application_id = $1",
            application_id,
        )
        if summary is None:
            raise HTTPException(status_code=404, detail=f"Application {application_id!r} not found")

        job = await conn.fetchrow(
            """
            SELECT job_id, status, error_message, created_at, started_at, completed_at
            FROM pipeline_jobs
            WHERE application_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            application_id,
        )

    result: dict[str, Any] = {
        "application_id": application_id,
        "state": summary["state"],
        "last_event_type": summary["last_event_type"],
        "last_event_at": summary["last_event_at"].isoformat() if summary["last_event_at"] else None,
    }

    if job:
        result["pipeline_job"] = {
            "job_id": str(job["job_id"]),
            "status": job["status"],
            "error_message": job["error_message"],
            "created_at": job["created_at"].isoformat() if job["created_at"] else None,
            "started_at": job["started_at"].isoformat() if job["started_at"] else None,
            "completed_at": job["completed_at"].isoformat() if job["completed_at"] else None,
        }
    else:
        result["pipeline_job"] = None

    return result


# ---------------------------------------------------------------------------
# GET /api/applications/{id}/events
# ---------------------------------------------------------------------------

@router.get("/applications/{application_id}/events")
async def get_application_events(application_id: str):
    """Return the full loan event stream for an application."""
    store = _require_store()

    try:
        events = await store.load_stream(f"loan-{application_id}")
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    return [
        {
            "event_id": str(e.event_id),
            "event_type": e.event_type,
            "stream_position": e.stream_position,
            "global_position": e.global_position,
            "recorded_at": e.recorded_at.isoformat() if e.recorded_at else None,
            "payload": e.payload,
        }
        for e in events
    ]


# ---------------------------------------------------------------------------
# GET /api/applications/{id}/compliance
# ---------------------------------------------------------------------------

@router.get("/applications/{application_id}/compliance")
async def get_application_compliance(
    application_id: str,
    as_of: Optional[str] = Query(default=None),
):
    """Return the compliance audit view, with optional point-in-time filtering."""
    pool = _require_pool()

    async with pool.acquire() as conn:
        if as_of:
            try:
                as_of_dt = datetime.fromisoformat(as_of)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid as_of timestamp format")
            rows = await conn.fetch(
                """
                SELECT * FROM compliance_audit_view
                WHERE application_id = $1 AND recorded_at <= $2
                ORDER BY recorded_at DESC
                """,
                application_id,
                as_of_dt,
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM compliance_audit_view WHERE application_id = $1 ORDER BY recorded_at DESC",
                application_id,
            )

    return [_row_to_dict(r) for r in rows]


# ---------------------------------------------------------------------------
# GET /api/applications/{id}/agents
# ---------------------------------------------------------------------------

@router.get("/applications/{application_id}/agents")
async def get_application_agents(application_id: str):
    """Return agent sessions that processed this application."""
    pool = _require_pool()

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM agent_performance_ledger
            WHERE application_id = $1
            ORDER BY last_event_at DESC NULLS LAST
            """,
            application_id,
        )

    return [_row_to_dict(r) for r in rows]


# ---------------------------------------------------------------------------
# GET /api/applications/{id}/analysis
# ---------------------------------------------------------------------------

@router.get("/applications/{application_id}/analysis")
async def get_application_analysis(application_id: str):
    """
    Structured analysis summary assembled from the application_summary projection.

    Returns: risk_tier, fraud_score, compliance_verdict, confidence_score,
             recommendation, contributing_sessions.
    """
    pool = _require_pool()
    store = _require_store()

    async with pool.acquire() as conn:
        summary = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            application_id,
        )

    if summary is None:
        raise HTTPException(status_code=404, detail=f"Application {application_id!r} not found")

    # Pull confidence from the most recent DecisionGenerated event if available
    confidence_score: Optional[float] = None
    try:
        events = await store.load_stream(f"loan-{application_id}")
        for e in reversed(events):
            if e.event_type == "DecisionGenerated":
                confidence_score = e.payload.get("confidence")
                break
    except Exception:
        pass

    return {
        "application_id": application_id,
        "risk_tier": summary["risk_tier"],
        "fraud_score": float(summary["fraud_score"]) if summary["fraud_score"] is not None else None,
        "compliance_verdict": summary["compliance_status"],
        "recommendation": summary["decision"],
        "confidence_score": confidence_score,
        "contributing_sessions": summary["agent_sessions_completed"] or [],
        "state": summary["state"],
    }
