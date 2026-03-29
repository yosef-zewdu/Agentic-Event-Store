"""
backend/routes/review.py — Human review workflow endpoints (Task 34)

Routes:
  GET   /api/review/queue                          — applications awaiting review
  POST  /api/applications/{id}/review              — submit reviewer decision
  GET   /api/applications/{id}/review-context      — full context for reviewer
"""
from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional
import uuid

import asyncpg
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.commands.handlers import handle_human_review_completed
from src.event_store import EventStore
from src.models.exceptions import DomainError

logger = logging.getLogger(__name__)
router = APIRouter()

# Injected by backend/main.py lifespan
_store: EventStore | None = None
_pool: asyncpg.Pool | None = None

# State value that the application_summary projection uses for human review
_PENDING_REVIEW_STATES = {
    "ApprovedPendingHuman",
    "DeclinedPendingHuman",
    "PENDING_HUMAN_REVIEW",   # legacy / direct state name
}


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
# Request models
# ---------------------------------------------------------------------------

class ReviewDecisionRequest(BaseModel):
    reviewer_id: str
    decision: str          # "APPROVE" or "DECLINE"
    override: bool = False
    override_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# GET /api/review/queue
# ---------------------------------------------------------------------------

@router.get("/review/queue")
async def get_review_queue():
    """
    List all applications currently awaiting human review.

    Includes risk_tier, fraud_score, compliance_status, and decision
    (confidence proxy) for each so the reviewer has enough context without
    opening the full detail page.
    """
    pool = _require_pool()

    state_params = ", ".join(f"${i+1}" for i in range(len(_PENDING_REVIEW_STATES)))
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT application_id, state, applicant_id, requested_amount_usd,
                   risk_tier, fraud_score, compliance_status, decision,
                   last_event_at
            FROM application_summary
            WHERE state = ANY($1::text[])
            ORDER BY last_event_at DESC NULLS LAST
            """,
            list(_PENDING_REVIEW_STATES),
        )

    return [_row_to_dict(r) for r in rows]


# ---------------------------------------------------------------------------
# POST /api/applications/{id}/review
# ---------------------------------------------------------------------------

@router.post("/applications/{application_id}/review")
async def submit_review(application_id: str, body: ReviewDecisionRequest):
    """
    Submit a human review decision.

    Requires the application to be in a PENDING_HUMAN_REVIEW state.
    Calls handle_human_review_completed which appends HumanReviewCompleted
    + ApplicationApproved/ApplicationDeclined to the event stream.
    """
    pool = _require_pool()
    store = _require_store()

    # Guard: verify the application is awaiting review before writing
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state FROM application_summary WHERE application_id = $1",
            application_id,
        )

    if row is None:
        raise HTTPException(status_code=404, detail=f"Application {application_id!r} not found")

    if row["state"] not in _PENDING_REVIEW_STATES:
        raise HTTPException(
            status_code=400,
            detail=f"Application is in state {row['state']!r}, not PENDING_HUMAN_REVIEW",
        )

    decision = body.decision.upper()
    if decision not in ("APPROVE", "DECLINE"):
        raise HTTPException(status_code=400, detail="decision must be APPROVE or DECLINE")

    try:
        await handle_human_review_completed(
            store,
            application_id=application_id,
            reviewer_id=body.reviewer_id,
            decision=decision,
            override=body.override,
        )
    except DomainError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        logger.error("handle_human_review_completed failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))

    # Return the updated summary
    async with pool.acquire() as conn:
        updated = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            application_id,
        )

    return _row_to_dict(updated) if updated else {"application_id": application_id, "status": "review_submitted"}


# ---------------------------------------------------------------------------
# GET /api/applications/{id}/review-context
# ---------------------------------------------------------------------------

@router.get("/applications/{application_id}/review-context")
async def get_review_context(application_id: str):
    """
    Full analysis context assembled for a human reviewer.

    Returns:
    - application summary (projection)
    - compliance records (compliance_audit_view)
    - agent sessions summary (agent_performance_ledger)
    - key decision events from the event stream (DecisionGenerated, FraudScreeningCompleted, CreditAnalysisCompleted)
    """
    pool = _require_pool()
    store = _require_store()

    async with pool.acquire() as conn:
        summary_row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            application_id,
        )
        if summary_row is None:
            raise HTTPException(status_code=404, detail=f"Application {application_id!r} not found")

        compliance_rows = await conn.fetch(
            "SELECT * FROM compliance_audit_view WHERE application_id = $1 ORDER BY recorded_at DESC",
            application_id,
        )
        agent_rows = await conn.fetch(
            "SELECT * FROM agent_performance_ledger WHERE application_id = $1 ORDER BY last_event_at DESC NULLS LAST",
            application_id,
        )

    # Pull key decision events from the raw event stream
    decision_events: list[dict[str, Any]] = []
    _key_types = {"DecisionGenerated", "FraudScreeningCompleted", "CreditAnalysisCompleted", "ComplianceCheckCompleted"}
    try:
        events = await store.load_stream(f"loan-{application_id}")
        for e in events:
            if e.event_type in _key_types:
                decision_events.append({
                    "event_type": e.event_type,
                    "recorded_at": e.recorded_at.isoformat() if e.recorded_at else None,
                    "payload": e.payload,
                })
    except Exception as exc:
        logger.warning("Could not load event stream for review-context %s: %s", application_id, exc)

    return {
        "application": _row_to_dict(summary_row),
        "compliance_records": [_row_to_dict(r) for r in compliance_rows],
        "agent_sessions": [_row_to_dict(r) for r in agent_rows],
        "decision_events": decision_events,
    }
