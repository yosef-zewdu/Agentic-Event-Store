"""
src/projections/compliance_audit.py — ComplianceAuditView projection

Maintains a regulatory read model for compliance officers with temporal query support.

Requirements covered:
  - Req 11.1: ComplianceAuditView with all required columns including recorded_at
              and evaluation_timestamp
  - Req 11.2: lag < 2000ms SLO under normal operating conditions
  - Req 11.3: get_compliance_at() filters on recorded_at as authoritative anchor
  - Req 11.4: get_projection_lag() returns lag in milliseconds
  - Req 11.5: rebuild_from_scratch() with shadow table + atomic swap
  - Req 11.6: p99 latency < 200ms for ledger://applications/{id}/compliance
  - Req 11.7: snapshot strategy — event-count trigger at 50 events per application
  - Idempotency: all writes use INSERT ... ON CONFLICT DO UPDATE (upsert)
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import asyncpg

from src.event_store import EventStore
from src.models.events import StoredEvent
from src.projections.base import BaseProjection

logger = logging.getLogger(__name__)

# Schema version — bump this when the snapshot state shape changes (Req 11.7)
SNAPSHOT_SCHEMA_VERSION = 1

# Trigger a snapshot every N compliance events per application (Req 11.7)
SNAPSHOT_EVENT_THRESHOLD = 50

# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------

_CREATE_COMPLIANCE_AUDIT_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    id                    UUID        NOT NULL DEFAULT gen_random_uuid(),
    application_id        TEXT        NOT NULL,
    event_id              UUID        NOT NULL,
    event_type            TEXT        NOT NULL,
    rule_id               TEXT,
    rule_version          TEXT,
    verdict               TEXT,
    evaluation_timestamp  TIMESTAMPTZ,
    evidence_hash         TEXT,
    regulation_set_version TEXT,
    session_id            TEXT,
    payload               JSONB       NOT NULL DEFAULT '{{}}'::jsonb,
    recorded_at           TIMESTAMPTZ NOT NULL,
    CONSTRAINT {table}_pkey PRIMARY KEY (application_id, event_id)
);
CREATE INDEX IF NOT EXISTS {table}_app_recorded_at
    ON {table} (application_id, recorded_at);
"""

_CREATE_SNAPSHOTS_SQL = """
CREATE TABLE IF NOT EXISTS compliance_snapshots (
    application_id   TEXT        NOT NULL,
    snapshot_position BIGINT     NOT NULL,
    snapshot_version  INT        NOT NULL,
    state            JSONB       NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (application_id, snapshot_position)
);
"""

_SUBSCRIBED_EVENT_TYPES: list[str] = [
    "ComplianceCheckRequested",
    "ComplianceCheckInitiated",
    "ComplianceRulePassed",
    "ComplianceRuleFailed",
    "ComplianceRuleNoted",
    "ComplianceCheckCompleted",
]


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


class ComplianceAuditViewProjection(BaseProjection):
    """
    Projection that maintains a per-event compliance audit trail for each application.

    Each compliance event gets its own row, preserving both recorded_at (DB-assigned,
    authoritative for temporal queries) and evaluation_timestamp (agent-assigned, for
    auditability). All writes are idempotent upserts.

    Snapshot strategy (Req 11.7):
      After every SNAPSHOT_EVENT_THRESHOLD compliance events for an application, a
      snapshot of the current compliance state is written to compliance_snapshots.
      On load, if the snapshot_version matches SNAPSHOT_SCHEMA_VERSION it is used as
      the starting point; otherwise it is discarded and state is rebuilt from position 0.
    """

    name: str = "compliance_audit_view"
    subscribed_event_types: list[str] = _SUBSCRIBED_EVENT_TYPES

    _table: str = "compliance_audit_view"

    # In-memory event counter per application for snapshot triggering.
    # Resets on daemon restart (snapshots are persisted in DB).
    _event_counts: dict[str, int]

    def __init__(self) -> None:
        self._event_counts = {}

    async def ensure_table_exists(self, conn: asyncpg.Connection) -> None:
        """Create projection and snapshot tables if they don't exist."""
        await conn.execute(_CREATE_COMPLIANCE_AUDIT_SQL.format(table=self._table))
        await conn.execute(_CREATE_SNAPSHOTS_SQL)

    async def handle(self, event: StoredEvent, conn: asyncpg.Connection) -> None:
        """Route event to the appropriate handler method."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler is None:
            return
        await handler(event, conn)

    # ------------------------------------------------------------------
    # Event handlers — all use upsert for idempotency
    # ------------------------------------------------------------------

    async def _on_ComplianceCheckRequested(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        await self._upsert_row(
            conn=conn,
            application_id=p.get("application_id"),
            event=event,
            rule_id=None,
            rule_version=None,
            verdict=None,
            evaluation_timestamp=_parse_dt(p.get("requested_at")),
            evidence_hash=None,
            regulation_set_version=p.get("regulation_set_version"),
            session_id=p.get("session_id"),
        )

    async def _on_ComplianceCheckInitiated(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        await self._upsert_row(
            conn=conn,
            application_id=p.get("application_id"),
            event=event,
            rule_id=None,
            rule_version=None,
            verdict=None,
            evaluation_timestamp=_parse_dt(p.get("initiated_at")),
            evidence_hash=None,
            regulation_set_version=p.get("regulation_set_version"),
            session_id=p.get("session_id"),
        )

    async def _on_ComplianceRulePassed(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        await self._upsert_row(
            conn=conn,
            application_id=p.get("application_id"),
            event=event,
            rule_id=p.get("rule_id"),
            rule_version=p.get("rule_version"),
            verdict="PASSED",
            evaluation_timestamp=_parse_dt(p.get("evaluated_at")),
            evidence_hash=p.get("evidence_hash"),
            regulation_set_version=None,
            session_id=p.get("session_id"),
        )

    async def _on_ComplianceRuleFailed(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        await self._upsert_row(
            conn=conn,
            application_id=p.get("application_id"),
            event=event,
            rule_id=p.get("rule_id"),
            rule_version=p.get("rule_version"),
            verdict="FAILED",
            evaluation_timestamp=_parse_dt(p.get("evaluated_at")),
            evidence_hash=p.get("evidence_hash"),
            regulation_set_version=None,
            session_id=p.get("session_id"),
        )

    async def _on_ComplianceRuleNoted(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        await self._upsert_row(
            conn=conn,
            application_id=p.get("application_id"),
            event=event,
            rule_id=p.get("rule_id"),
            rule_version=None,
            verdict="NOTED",
            evaluation_timestamp=_parse_dt(p.get("evaluated_at")),
            evidence_hash=None,
            regulation_set_version=None,
            session_id=p.get("session_id"),
        )

    async def _on_ComplianceCheckCompleted(
        self, event: StoredEvent, conn: asyncpg.Connection
    ) -> None:
        p = event.payload
        verdict = p.get("overall_verdict")
        if hasattr(verdict, "value"):
            verdict = verdict.value
        await self._upsert_row(
            conn=conn,
            application_id=p.get("application_id"),
            event=event,
            rule_id=None,
            rule_version=None,
            verdict=str(verdict) if verdict else None,
            evaluation_timestamp=_parse_dt(p.get("completed_at")),
            evidence_hash=None,
            regulation_set_version=None,
            session_id=p.get("session_id"),
        )

    # ------------------------------------------------------------------
    # Core upsert helper
    # ------------------------------------------------------------------

    async def _upsert_row(
        self,
        conn: asyncpg.Connection,
        application_id: str | None,
        event: StoredEvent,
        rule_id: str | None,
        rule_version: str | None,
        verdict: str | None,
        evaluation_timestamp: datetime | None,
        evidence_hash: str | None,
        regulation_set_version: str | None,
        session_id: str | None,
    ) -> None:
        if not application_id:
            return

        payload_json = json.dumps(event.payload)

        await conn.execute(
            f"""
            INSERT INTO {self._table}
                (application_id, event_id, event_type, rule_id, rule_version,
                 verdict, evaluation_timestamp, evidence_hash,
                 regulation_set_version, session_id, payload, recorded_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12)
            ON CONFLICT (application_id, event_id) DO UPDATE SET
                event_type             = EXCLUDED.event_type,
                rule_id                = EXCLUDED.rule_id,
                rule_version           = EXCLUDED.rule_version,
                verdict                = EXCLUDED.verdict,
                evaluation_timestamp   = EXCLUDED.evaluation_timestamp,
                evidence_hash          = EXCLUDED.evidence_hash,
                regulation_set_version = EXCLUDED.regulation_set_version,
                session_id             = EXCLUDED.session_id,
                payload                = EXCLUDED.payload,
                recorded_at            = EXCLUDED.recorded_at
            """,
            application_id,
            event.event_id,
            event.event_type,
            rule_id,
            rule_version,
            verdict,
            evaluation_timestamp,
            evidence_hash,
            regulation_set_version,
            session_id,
            payload_json,
            event.recorded_at,
        )

        # Track event count for snapshot triggering (Req 11.7)
        count = self._event_counts.get(application_id, 0) + 1
        self._event_counts[application_id] = count
        if count % SNAPSHOT_EVENT_THRESHOLD == 0:
            await self._write_snapshot(conn, application_id, event.global_position)

    # ------------------------------------------------------------------
    # Snapshot strategy (Req 11.7)
    # ------------------------------------------------------------------

    async def _write_snapshot(
        self,
        conn: asyncpg.Connection,
        application_id: str,
        snapshot_position: int,
    ) -> None:
        """
        Persist a snapshot of the current compliance state for an application.

        The snapshot captures all rows for the application at this point in time.
        snapshot_version is set to SNAPSHOT_SCHEMA_VERSION so stale snapshots
        can be detected and discarded on load.
        """
        rows = await conn.fetch(
            f"""
            SELECT event_id, event_type, rule_id, rule_version, verdict,
                   evaluation_timestamp, evidence_hash, regulation_set_version,
                   session_id, payload, recorded_at
            FROM {self._table}
            WHERE application_id = $1
            ORDER BY recorded_at
            """,
            application_id,
        )

        state = [
            {
                "event_id": str(r["event_id"]),
                "event_type": r["event_type"],
                "rule_id": r["rule_id"],
                "rule_version": r["rule_version"],
                "verdict": r["verdict"],
                "evaluation_timestamp": r["evaluation_timestamp"].isoformat()
                    if r["evaluation_timestamp"] else None,
                "evidence_hash": r["evidence_hash"],
                "regulation_set_version": r["regulation_set_version"],
                "session_id": r["session_id"],
                "payload": r["payload"] if isinstance(r["payload"], dict)
                    else json.loads(r["payload"]),
                "recorded_at": r["recorded_at"].isoformat()
                    if r["recorded_at"] else None,
            }
            for r in rows
        ]

        await conn.execute(
            """
            INSERT INTO compliance_snapshots
                (application_id, snapshot_position, snapshot_version, state, created_at)
            VALUES ($1, $2, $3, $4::jsonb, NOW())
            ON CONFLICT (application_id, snapshot_position) DO UPDATE SET
                snapshot_version = EXCLUDED.snapshot_version,
                state            = EXCLUDED.state,
                created_at       = EXCLUDED.created_at
            """,
            application_id,
            snapshot_position,
            SNAPSHOT_SCHEMA_VERSION,
            json.dumps(state),
        )
        logger.debug(
            "ComplianceAuditView: wrote snapshot for application_id=%s at position=%d",
            application_id,
            snapshot_position,
        )

    async def load_snapshot(
        self, conn: asyncpg.Connection, application_id: str
    ) -> tuple[list[dict], int] | None:
        """
        Load the latest valid snapshot for an application.

        Returns (state_rows, snapshot_position) if a valid snapshot exists,
        or None if no snapshot exists or the snapshot_version is stale (Req 11.7).
        """
        row = await conn.fetchrow(
            """
            SELECT snapshot_position, snapshot_version, state
            FROM compliance_snapshots
            WHERE application_id = $1
            ORDER BY snapshot_position DESC
            LIMIT 1
            """,
            application_id,
        )
        if row is None:
            return None

        if row["snapshot_version"] != SNAPSHOT_SCHEMA_VERSION:
            logger.info(
                "ComplianceAuditView: discarding stale snapshot for application_id=%s "
                "(snapshot_version=%d, current=%d)",
                application_id,
                row["snapshot_version"],
                SNAPSHOT_SCHEMA_VERSION,
            )
            return None

        state = row["state"]
        if isinstance(state, str):
            state = json.loads(state)

        return state, row["snapshot_position"]

    # ------------------------------------------------------------------
    # Query: get_compliance_at (Req 11.3)
    # ------------------------------------------------------------------

    async def get_compliance_at(
        self,
        application_id: str,
        timestamp: datetime,
        pool: asyncpg.Pool,
    ) -> list[dict]:
        """
        Return all compliance events for an application as they existed at `timestamp`.

        Uses recorded_at (DB-assigned) as the authoritative temporal anchor, NOT
        evaluation_timestamp (agent-assigned, could be backdated). Both timestamps
        are included in the returned records for auditability (Req 11.3).

        Args:
            application_id: The application to query.
            timestamp:       Point-in-time cutoff. Events with recorded_at > timestamp
                             are excluded.
            pool:            asyncpg pool for the query.

        Returns:
            List of compliance record dicts ordered by recorded_at ascending.
        """
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT application_id, event_id, event_type, rule_id, rule_version,
                       verdict, evaluation_timestamp, evidence_hash,
                       regulation_set_version, session_id, payload, recorded_at
                FROM {self._table}
                WHERE application_id = $1
                  AND recorded_at <= $2
                ORDER BY recorded_at ASC
                """,
                application_id,
                timestamp,
            )

        return [
            {
                "application_id": r["application_id"],
                "event_id": str(r["event_id"]),
                "event_type": r["event_type"],
                "rule_id": r["rule_id"],
                "rule_version": r["rule_version"],
                "verdict": r["verdict"],
                # Both timestamps included for auditability (Req 11.3)
                "recorded_at": r["recorded_at"].isoformat() if r["recorded_at"] else None,
                "evaluation_timestamp": r["evaluation_timestamp"].isoformat()
                    if r["evaluation_timestamp"] else None,
                "evidence_hash": r["evidence_hash"],
                "regulation_set_version": r["regulation_set_version"],
                "session_id": r["session_id"],
                "payload": r["payload"] if isinstance(r["payload"], dict)
                    else json.loads(r["payload"]),
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Lag monitoring (Req 11.4)
    # ------------------------------------------------------------------

    async def get_projection_lag(self, pool: asyncpg.Pool) -> int:
        """
        Return lag in milliseconds between the store's latest event and the latest
        event this projection has processed (Req 11.4).

        Returns 0 if fully caught up or no events exist.
        """
        async with pool.acquire() as conn:
            latest_row = await conn.fetchrow(
                "SELECT recorded_at FROM events ORDER BY global_position DESC LIMIT 1"
            )
            if latest_row is None:
                return 0

            checkpoint_row = await conn.fetchrow(
                "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
                self.name,
            )
            if checkpoint_row is None or checkpoint_row["last_position"] == 0:
                # No checkpoint yet — lag is the full age of the store
                oldest_row = await conn.fetchrow(
                    "SELECT recorded_at FROM events ORDER BY global_position ASC LIMIT 1"
                )
                if oldest_row is None:
                    return 0
                checkpoint_ts = oldest_row["recorded_at"]
            else:
                pos_row = await conn.fetchrow(
                    "SELECT recorded_at FROM events WHERE global_position = $1",
                    checkpoint_row["last_position"],
                )
                if pos_row is None:
                    return 0
                checkpoint_ts = pos_row["recorded_at"]

            latest_ts = latest_row["recorded_at"]
            if latest_ts.tzinfo is None:
                latest_ts = latest_ts.replace(tzinfo=timezone.utc)
            if checkpoint_ts.tzinfo is None:
                checkpoint_ts = checkpoint_ts.replace(tzinfo=timezone.utc)

            return max(int((latest_ts - checkpoint_ts).total_seconds() * 1000), 0)

    # ------------------------------------------------------------------
    # Rebuild from scratch (Req 11.5)
    # ------------------------------------------------------------------

    async def rebuild_from_scratch(
        self, store: EventStore, pool: asyncpg.Pool | None = None
    ) -> None:
        """
        Rebuild the projection from scratch using a shadow table (Req 11.5).

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

        ts = uuid.uuid4().hex[:12]
        shadow_name = f"compliance_audit_view_shadow_{ts}"
        live_name = "compliance_audit_view"
        old_name = f"compliance_audit_view_old_{ts}"

        async with _pool.acquire() as conn:
            # 1. Create shadow table (snapshots table is shared, not shadowed)
            await conn.execute(_CREATE_COMPLIANCE_AUDIT_SQL.format(table=shadow_name))

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

        logger.info("ComplianceAuditViewProjection.rebuild_from_scratch complete")


# ---------------------------------------------------------------------------
# Internal helper: shadow projection targeting a different table during rebuild
# ---------------------------------------------------------------------------

class _ShadowProjection(ComplianceAuditViewProjection):
    """Thin subclass that writes to a shadow table during rebuild_from_scratch."""

    def __init__(self, table_name: str) -> None:
        super().__init__()
        self._table = table_name
