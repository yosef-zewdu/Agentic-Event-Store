"""
src/projections/agent_performance.py — AgentPerformanceLedger projection

Maintains aggregated performance metrics per AI agent model version.

Requirements covered:
  - Req 10.1: AgentPerformanceLedger table with all required columns
  - Req 10.2: CreditAnalysisCompleted increments analyses_completed,
              avg_confidence_score, avg_duration_ms
  - Req 10.3: HumanReviewCompleted with override=true increments human_override_rate
              for contributing agent sessions
  - Idempotency: all writes use INSERT ... ON CONFLICT DO UPDATE (upsert)
"""
from __future__ import annotations

import logging

import asyncpg

from src.models.events import StoredEvent
from src.projections.base import BaseProjection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    agent_id              TEXT        NOT NULL,
    model_version         TEXT        NOT NULL,
    analyses_completed    BIGINT      NOT NULL DEFAULT 0,
    decisions_generated   BIGINT      NOT NULL DEFAULT 0,
    avg_confidence_score  NUMERIC,
    avg_duration_ms       NUMERIC,
    approve_rate          NUMERIC,
    decline_rate          NUMERIC,
    refer_rate            NUMERIC,
    human_override_rate   NUMERIC,
    first_seen_at         TIMESTAMPTZ,
    last_seen_at          TIMESTAMPTZ,
    CONSTRAINT {table}_pkey PRIMARY KEY (agent_id, model_version)
);
CREATE TABLE IF NOT EXISTS agent_performance_processed_events (
    event_id UUID PRIMARY KEY
);
"""

_SUBSCRIBED_EVENT_TYPES: list[str] = [
    "CreditAnalysisCompleted",
    "HumanReviewCompleted",
    "DecisionGenerated",
]


class AgentPerformanceLedgerProjection(BaseProjection):
    """
    Projection that maintains per-(agent_id, model_version) performance metrics.

    All writes use INSERT ... ON CONFLICT DO UPDATE (upsert) for idempotency.
    The `conn` passed to `handle()` is already inside a transaction managed by
    the ProjectionDaemon — all writes use that connection for atomic checkpoint
    + write (Req 12.3).
    """

    name: str = "agent_performance_ledger"
    subscribed_event_types: list[str] = _SUBSCRIBED_EVENT_TYPES

    _table: str = "agent_performance_ledger"

    async def ensure_table_exists(self, conn: asyncpg.Connection) -> None:
        """Create the projection table and deduplication table if they don't exist."""
        await conn.execute(_CREATE_TABLE_SQL.format(table=self._table))

    async def handle(self, event: StoredEvent, conn: asyncpg.Connection) -> None:
        """Route event to the appropriate handler method, skipping already-processed events."""
        # Idempotency guard: skip if this event_id was already processed
        already = await conn.fetchval(
            "SELECT 1 FROM agent_performance_processed_events WHERE event_id = $1",
            event.event_id,
        )
        if already:
            return

        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler is None:
            # Still mark as processed so we don't re-check on every replay
            await conn.execute(
                "INSERT INTO agent_performance_processed_events (event_id) VALUES ($1) "
                "ON CONFLICT DO NOTHING",
                event.event_id,
            )
            return

        await handler(event, conn)

        # Mark processed after successful handler execution
        await conn.execute(
            "INSERT INTO agent_performance_processed_events (event_id) VALUES ($1) "
            "ON CONFLICT DO NOTHING",
            event.event_id,
        )

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    async def _on_CreditAnalysisCompleted(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        """
        Increment analyses_completed and update rolling averages for
        avg_confidence_score and avg_duration_ms (Req 10.2).

        CreditAnalysisCompleted payload fields used:
          - session_id: used to derive agent_id (stored in AgentSession stream;
            here we use session_id as agent_id proxy since agent_id is not
            directly in the CreditAnalysisCompleted payload in this schema)
          - model_version
          - decision.confidence  (float)
          - analysis_duration_ms (int)
        """
        p = event.payload

        # agent_id: prefer explicit field, fall back to session_id
        agent_id = p.get("agent_id") or p.get("session_id", "unknown")
        model_version = p.get("model_version", "unknown")

        # Extract confidence from nested decision dict
        decision = p.get("decision") or {}
        if isinstance(decision, str):
            import json
            try:
                decision = json.loads(decision)
            except Exception:
                decision = {}
        confidence = decision.get("confidence") if isinstance(decision, dict) else None
        if confidence is None:
            confidence = p.get("confidence_score")

        duration_ms = p.get("analysis_duration_ms")

        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (agent_id, model_version, analyses_completed,
                 avg_confidence_score, avg_duration_ms,
                 first_seen_at, last_seen_at)
            VALUES ($1, $2, 1, $3, $4, $5, $5)
            ON CONFLICT (agent_id, model_version) DO UPDATE SET
                analyses_completed   = {self._table}.analyses_completed + 1,
                avg_confidence_score = CASE
                    WHEN $3 IS NOT NULL THEN
                        ROUND(
                            (
                                COALESCE({self._table}.avg_confidence_score, 0)
                                * {self._table}.analyses_completed
                                + $3
                            ) / ({self._table}.analyses_completed + 1),
                            6
                        )
                    ELSE {self._table}.avg_confidence_score
                END,
                avg_duration_ms      = CASE
                    WHEN $4 IS NOT NULL THEN
                        ROUND(
                            (
                                COALESCE({self._table}.avg_duration_ms, 0)
                                * {self._table}.analyses_completed
                                + $4
                            ) / ({self._table}.analyses_completed + 1),
                            2
                        )
                    ELSE {self._table}.avg_duration_ms
                END,
                first_seen_at        = LEAST({self._table}.first_seen_at, EXCLUDED.first_seen_at),
                last_seen_at         = GREATEST({self._table}.last_seen_at, EXCLUDED.last_seen_at)
            """,
            agent_id,
            model_version,
            float(confidence) if confidence is not None else None,
            float(duration_ms) if duration_ms is not None else None,
            event.recorded_at,
        )

    async def _on_HumanReviewCompleted(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        """
        When override=true, increment human_override_rate for each agent session
        that contributed to the overridden decision (Req 10.3).

        HumanReviewCompleted payload fields used:
          - override: bool
          - contributing_sessions: list[str] (session IDs of contributing agents)
          - model_versions: dict[session_id -> model_version] (optional)
        """
        p = event.payload
        if not p.get("override", False):
            return

        # contributing_sessions may be a list of session IDs
        contributing = p.get("contributing_sessions") or p.get("contributing_agent_sessions") or []
        model_versions: dict = p.get("model_versions") or {}

        for session_id in contributing:
            agent_id = session_id  # best proxy without joining AgentSession stream
            model_version = model_versions.get(session_id, "unknown")

            # Ensure a row exists, then increment override numerator.
            # human_override_rate is stored as a running count here and can be
            # normalised to a rate by dividing by analyses_completed at query time.
            # We store it as a raw increment count (NUMERIC) so callers can compute
            # the rate: human_override_rate / analyses_completed.
            await conn.execute(
                f"""
                INSERT INTO {self._table}
                    (agent_id, model_version, human_override_rate,
                     first_seen_at, last_seen_at)
                VALUES ($1, $2, 1, $3, $3)
                ON CONFLICT (agent_id, model_version) DO UPDATE SET
                    human_override_rate = COALESCE({self._table}.human_override_rate, 0) + 1,
                    last_seen_at        = GREATEST({self._table}.last_seen_at, EXCLUDED.last_seen_at)
                """,
                agent_id,
                model_version,
                event.recorded_at,
            )

    async def _on_DecisionGenerated(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        """
        Increment decisions_generated and update approve/decline/refer rates
        for the orchestrator agent session.
        """
        p = event.payload
        agent_id = p.get("agent_id") or p.get("orchestrator_session_id", "unknown")
        model_versions: dict = p.get("model_versions") or {}
        # Use the orchestrator's model version if available
        model_version = (
            model_versions.get(agent_id)
            or p.get("model_version", "unknown")
        )
        recommendation = (p.get("recommendation") or "").upper()

        approve_inc = 1 if recommendation == "APPROVE" else 0
        decline_inc = 1 if recommendation == "DECLINE" else 0
        refer_inc = 1 if recommendation == "REFER" else 0

        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (agent_id, model_version, decisions_generated,
                 approve_rate, decline_rate, refer_rate,
                 first_seen_at, last_seen_at)
            VALUES ($1, $2, 1, $3, $4, $5, $6, $6)
            ON CONFLICT (agent_id, model_version) DO UPDATE SET
                decisions_generated = {self._table}.decisions_generated + 1,
                approve_rate        = COALESCE({self._table}.approve_rate, 0) + $3,
                decline_rate        = COALESCE({self._table}.decline_rate, 0) + $4,
                refer_rate          = COALESCE({self._table}.refer_rate, 0) + $5,
                first_seen_at       = LEAST({self._table}.first_seen_at, EXCLUDED.first_seen_at),
                last_seen_at        = GREATEST({self._table}.last_seen_at, EXCLUDED.last_seen_at)
            """,
            agent_id,
            model_version,
            approve_inc,
            decline_inc,
            refer_inc,
            event.recorded_at,
        )
