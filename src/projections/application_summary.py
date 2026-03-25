"""
src/projections/application_summary.py — ApplicationSummary projection

Maintains a current-state summary row for every loan application.

Requirements covered:
  - Req 9.1: ApplicationSummary table with all required columns
  - Req 9.3: lag < 500ms SLO under normal operating conditions
  - Req 9.5: rebuild_from_scratch() with shadow table + atomic rename swap
  - Req 11.3 (tasks): all handle() calls use INSERT ... ON CONFLICT DO UPDATE (upsert)
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import asyncpg

from src.event_store import EventStore
from src.models.events import StoredEvent
from src.projections.base import BaseProjection

logger = logging.getLogger(__name__)


def _parse_dt(value: Any) -> datetime | None:
    """
    Coerce a payload timestamp field to a datetime object.

    Payload values are stored as JSON and come back as strings after the
    DB round-trip.  asyncpg requires actual datetime instances for TIMESTAMPTZ
    parameters, so we parse strings here.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        # ISO 8601 with or without timezone
        try:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            return None
    return None


# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    application_id           TEXT        NOT NULL,
    state                    TEXT,
    applicant_id             TEXT,
    requested_amount_usd     NUMERIC,
    approved_amount_usd      NUMERIC,
    risk_tier                TEXT,
    fraud_score              NUMERIC,
    compliance_status        TEXT,
    decision                 TEXT,
    agent_sessions_completed JSONB       NOT NULL DEFAULT '[]'::jsonb,
    last_event_type          TEXT,
    last_event_at            TIMESTAMPTZ,
    human_reviewer_id        TEXT,
    final_decision_at        TIMESTAMPTZ,
    CONSTRAINT {table}_pkey PRIMARY KEY (application_id)
);
"""

# All event types this projection cares about (Req 9.1 / 9.3)
_SUBSCRIBED_EVENT_TYPES: list[str] = [
    "ApplicationSubmitted",
    "CreditAnalysisRequested",
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "ComplianceCheckCompleted",
    "DecisionGenerated",
    "HumanReviewRequested",
    "HumanReviewCompleted",
    "ApplicationApproved",
    "ApplicationDeclined",
    "ApplicationWithdrawn",
    "AgentSessionCompleted",
    "AgentSessionClosed",   # legacy alias
]


class ApplicationSummaryProjection(BaseProjection):
    """
    Projection that maintains a current-state summary for every loan application.

    All writes use INSERT ... ON CONFLICT DO UPDATE (upsert) for idempotency (task 11.3).
    The `conn` passed to `handle()` is already inside a transaction managed by the
    ProjectionDaemon — all writes use that connection for atomic checkpoint + write (Req 12.3).
    """

    name: str = "application_summary"
    subscribed_event_types: list[str] = _SUBSCRIBED_EVENT_TYPES

    # Live table name — overridden in _ShadowProjection during rebuild
    _table: str = "application_summary"

    async def ensure_table_exists(self, conn: asyncpg.Connection) -> None:
        """Create the projection table if it doesn't exist."""
        await conn.execute(_CREATE_TABLE_SQL.format(table=self._table))

    async def handle(self, event: StoredEvent, conn: asyncpg.Connection) -> None:
        """Route event to the appropriate handler method."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler is None:
            return
        await handler(event, conn)

    # ------------------------------------------------------------------
    # Event handlers — all use upsert for idempotency (task 11.3)
    # ------------------------------------------------------------------

    async def _on_ApplicationSubmitted(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (application_id, state, applicant_id, requested_amount_usd,
                 last_event_type, last_event_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (application_id) DO UPDATE SET
                state                = EXCLUDED.state,
                applicant_id         = EXCLUDED.applicant_id,
                requested_amount_usd = EXCLUDED.requested_amount_usd,
                last_event_type      = EXCLUDED.last_event_type,
                last_event_at        = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            "Submitted",
            p.get("applicant_id"),
            p.get("requested_amount_usd"),
            event.event_type,
            event.recorded_at,
        )

    async def _on_CreditAnalysisRequested(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (application_id, state, last_event_type, last_event_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (application_id) DO UPDATE SET
                state           = EXCLUDED.state,
                last_event_type = EXCLUDED.last_event_type,
                last_event_at   = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            "AwaitingAnalysis",
            event.event_type,
            event.recorded_at,
        )

    async def _on_CreditAnalysisCompleted(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        # decision is a dict with risk_tier, recommended_limit_usd, etc.
        decision = p.get("decision") or {}
        if isinstance(decision, str):
            try:
                decision = json.loads(decision)
            except Exception:
                decision = {}
        risk_tier = decision.get("risk_tier") if isinstance(decision, dict) else None
        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (application_id, state, risk_tier, last_event_type, last_event_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (application_id) DO UPDATE SET
                state           = EXCLUDED.state,
                risk_tier       = EXCLUDED.risk_tier,
                last_event_type = EXCLUDED.last_event_type,
                last_event_at   = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            "AnalysisComplete",
            risk_tier,
            event.event_type,
            event.recorded_at,
        )

    async def _on_FraudScreeningCompleted(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (application_id, fraud_score, last_event_type, last_event_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (application_id) DO UPDATE SET
                fraud_score     = EXCLUDED.fraud_score,
                last_event_type = EXCLUDED.last_event_type,
                last_event_at   = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            p.get("fraud_score"),
            event.event_type,
            event.recorded_at,
        )

    async def _on_ComplianceCheckCompleted(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        # overall_verdict is a ComplianceVerdict enum value (CLEAR/BLOCKED/CONDITIONAL)
        verdict = p.get("overall_verdict")
        if hasattr(verdict, "value"):
            verdict = verdict.value
        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (application_id, compliance_status, last_event_type, last_event_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (application_id) DO UPDATE SET
                compliance_status = EXCLUDED.compliance_status,
                last_event_type   = EXCLUDED.last_event_type,
                last_event_at     = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            verdict,
            event.event_type,
            event.recorded_at,
        )

    async def _on_DecisionGenerated(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (application_id, decision, state, last_event_type, last_event_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (application_id) DO UPDATE SET
                decision        = EXCLUDED.decision,
                state           = EXCLUDED.state,
                last_event_type = EXCLUDED.last_event_type,
                last_event_at   = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            p.get("recommendation"),
            "PendingDecision",
            event.event_type,
            event.recorded_at,
        )

    async def _on_HumanReviewRequested(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        # Infer pending state from the reason field; default to ApprovedPendingHuman
        reason = (p.get("reason") or "").upper()
        new_state = "DeclinedPendingHuman" if "DECLIN" in reason else "ApprovedPendingHuman"
        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (application_id, state, last_event_type, last_event_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (application_id) DO UPDATE SET
                state           = EXCLUDED.state,
                last_event_type = EXCLUDED.last_event_type,
                last_event_at   = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            new_state,
            event.event_type,
            event.recorded_at,
        )

    async def _on_HumanReviewCompleted(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (application_id, human_reviewer_id, last_event_type, last_event_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (application_id) DO UPDATE SET
                human_reviewer_id = EXCLUDED.human_reviewer_id,
                last_event_type   = EXCLUDED.last_event_type,
                last_event_at     = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            p.get("reviewer_id"),
            event.event_type,
            event.recorded_at,
        )

    async def _on_ApplicationApproved(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        final_at = _parse_dt(p.get("approved_at")) or event.recorded_at
        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (application_id, approved_amount_usd, state,
                 final_decision_at, last_event_type, last_event_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (application_id) DO UPDATE SET
                approved_amount_usd = EXCLUDED.approved_amount_usd,
                state               = EXCLUDED.state,
                final_decision_at   = EXCLUDED.final_decision_at,
                last_event_type     = EXCLUDED.last_event_type,
                last_event_at       = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            p.get("approved_amount_usd"),
            "FinalApproved",
            final_at,
            event.event_type,
            event.recorded_at,
        )

    async def _on_ApplicationDeclined(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        final_at = _parse_dt(p.get("declined_at")) or event.recorded_at
        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (application_id, state, final_decision_at,
                 last_event_type, last_event_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (application_id) DO UPDATE SET
                state             = EXCLUDED.state,
                final_decision_at = EXCLUDED.final_decision_at,
                last_event_type   = EXCLUDED.last_event_type,
                last_event_at     = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            "FinalDeclined",
            final_at,
            event.event_type,
            event.recorded_at,
        )

    async def _on_ApplicationWithdrawn(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (application_id, state, last_event_type, last_event_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (application_id) DO UPDATE SET
                state           = EXCLUDED.state,
                last_event_type = EXCLUDED.last_event_type,
                last_event_at   = EXCLUDED.last_event_at
            """,
            p.get("application_id"),
            "Withdrawn",
            event.event_type,
            event.recorded_at,
        )

    async def _on_AgentSessionCompleted(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        """Append session_id to agent_sessions_completed[] without duplicates."""
        p = event.payload
        application_id = p.get("application_id")
        session_id = p.get("session_id")
        if not application_id or not session_id:
            return
        session_json = json.dumps([session_id])
        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (application_id, agent_sessions_completed, last_event_type, last_event_at)
            VALUES ($1, $2::jsonb, $3, $4)
            ON CONFLICT (application_id) DO UPDATE SET
                agent_sessions_completed = (
                    CASE
                        WHEN {self._table}.agent_sessions_completed @> $2::jsonb
                        THEN {self._table}.agent_sessions_completed
                        ELSE {self._table}.agent_sessions_completed || $2::jsonb
                    END
                ),
                last_event_type = EXCLUDED.last_event_type,
                last_event_at   = EXCLUDED.last_event_at
            """,
            application_id,
            session_json,
            event.event_type,
            event.recorded_at,
        )

    # AgentSessionClosed is a legacy alias for AgentSessionCompleted
    _on_AgentSessionClosed = _on_AgentSessionCompleted

    # ------------------------------------------------------------------
    # Rebuild from scratch (Req 9.5)
    # ------------------------------------------------------------------

    async def rebuild_from_scratch(
        self, store: EventStore, pool: asyncpg.Pool | None = None
    ) -> None:
        """
        Rebuild the projection from scratch using a shadow table (Req 9.5).

        Steps:
          1. Create shadow table with a timestamped name
          2. Replay all events from global_position=0 into the shadow table
          3. Atomically swap: rename live → old, shadow → live, drop old
          4. Live reads continue from the original table until the atomic swap

        Args:
            store: EventStore used to replay events.
            pool:  asyncpg pool for DDL operations. Falls back to store._pool.
        """
        _pool = pool or store._pool
        if _pool is None:
            raise RuntimeError("rebuild_from_scratch requires a connected pool")

        ts = uuid.uuid4().hex[:12]  # unique suffix — avoids collisions in rapid test runs
        shadow_name = f"application_summary_shadow_{ts}"
        live_name = "application_summary"
        old_name = f"application_summary_old_{ts}"

        async with _pool.acquire() as conn:
            # 1. Create shadow table
            await conn.execute(_CREATE_TABLE_SQL.format(table=shadow_name))

            # 2. Replay all events into shadow table
            shadow_proj = _ShadowProjection(shadow_name)
            async for event in store.load_all(from_position=0):
                handler = getattr(shadow_proj, f"_on_{event.event_type}", None)
                if handler is not None:
                    try:
                        await handler(event, conn)
                    except Exception:
                        logger.exception(
                            "rebuild_from_scratch: error handling event_id=%s type=%s",
                            event.event_id,
                            event.event_type,
                        )

            # 3 & 4. Atomic swap — live reads continue until this point
            async with conn.transaction():
                await conn.execute(
                    f'ALTER TABLE "{live_name}" RENAME TO "{old_name}"'
                )
                await conn.execute(
                    f'ALTER TABLE "{shadow_name}" RENAME TO "{live_name}"'
                )
                await conn.execute(f'DROP TABLE "{old_name}"')

        logger.info("ApplicationSummaryProjection.rebuild_from_scratch complete")


# ---------------------------------------------------------------------------
# Internal helper: shadow projection targeting a different table during rebuild
# ---------------------------------------------------------------------------

class _ShadowProjection(ApplicationSummaryProjection):
    """Thin subclass that writes to a shadow table during rebuild_from_scratch."""

    def __init__(self, table_name: str) -> None:
        self._table = table_name
