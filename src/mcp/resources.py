"""
MCP query-side resources for The Ledger (Req 16.1).

All 6 resources are registered here. Resources read exclusively from projections
EXCEPT the two named exceptions that load streams directly (Req 16.4):
  - ledger://applications/{id}/audit-trail  → direct AuditLedger stream load
  - ledger://agents/{id}/sessions/{session_id} → direct AgentSession stream load

SLO targets (Req 16.6):
  - /applications/{id}          p99 < 50ms
  - /applications/{id}/compliance p99 < 200ms
  - /applications/{id}/audit-trail p99 < 500ms
  - /agents/{id}/performance    p99 < 50ms
  - /agents/{id}/sessions/{id}  p99 < 300ms
  - /ledger/health              p99 < 10ms
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from src.mcp.utils import get_store

logger = logging.getLogger(__name__)


def _get_pool():
    """Return the asyncpg pool from the shared EventStore."""
    return get_store()._pool


def _json(data) -> str:
    """Serialize data to JSON string for MCP resource responses."""
    return json.dumps(data, default=str)




def register_resources(mcp):

    # ---------------------------------------------------------------------------
    # Resource 1 — ledger://applications/{id}
    # Reads ApplicationSummary projection only, never replays stream (Req 16.2)
    # SLO: p99 < 50ms
    # ---------------------------------------------------------------------------

    @mcp.resource("ledger://applications/{id}")
    async def get_application(id: str) -> str:
        """
        Return the current ApplicationSummary for a loan application.

        Reads exclusively from the ApplicationSummary projection table.
        Never replays the event stream (Req 16.2).
        SLO: p99 < 50ms (Req 16.6).

        Returns 404-style dict if the application is not found.
        """
        pool = _get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT application_id, state, applicant_id,
                    requested_amount_usd, approved_amount_usd,
                    risk_tier, fraud_score, compliance_status,
                    decision, agent_sessions_completed,
                    last_event_type, last_event_at,
                    human_reviewer_id, final_decision_at
                FROM application_summary
                WHERE application_id = $1
                """,
                id,
            )

        if row is None:
            return json.dumps({"error": "not_found", "application_id": id})

        return json.dumps({
            "application_id": row["application_id"],
            "state": row["state"],
            "applicant_id": row["applicant_id"],
            "requested_amount_usd": float(row["requested_amount_usd"])
                if row["requested_amount_usd"] is not None else None,
            "approved_amount_usd": float(row["approved_amount_usd"])
                if row["approved_amount_usd"] is not None else None,
            "risk_tier": row["risk_tier"],
            "fraud_score": float(row["fraud_score"])
                if row["fraud_score"] is not None else None,
            "compliance_status": row["compliance_status"],
            "decision": row["decision"],
            "agent_sessions_completed": row["agent_sessions_completed"]
                if isinstance(row["agent_sessions_completed"], list)
                else json.loads(row["agent_sessions_completed"] or "[]"),
            "last_event_type": row["last_event_type"],
            "last_event_at": row["last_event_at"].isoformat()
                if row["last_event_at"] else None,
            "human_reviewer_id": row["human_reviewer_id"],
            "final_decision_at": row["final_decision_at"].isoformat()
                if row["final_decision_at"] else None,
        })


    # ---------------------------------------------------------------------------
    # Resource 2 — ledger://applications/{id}/compliance
    # Reads ComplianceAuditView, supports ?as_of=timestamp (Req 16.3)
    # SLO: p99 < 200ms
    # ---------------------------------------------------------------------------

    @mcp.resource("ledger://applications/{id}/compliance")
    async def get_application_compliance(id: str, as_of: str | None = None) -> str:
        """
        Return the ComplianceAuditView for a loan application.

        Supports optional point-in-time query via as_of (ISO 8601 timestamp).
        When as_of is provided, only compliance events with recorded_at <= as_of
        are returned. recorded_at (DB-assigned) is the authoritative temporal anchor,
        not evaluation_timestamp (Req 11.3).

        Reads exclusively from the ComplianceAuditView projection table (Req 16.3).
        SLO: p99 < 200ms (Req 16.6).
        """
        pool = _get_pool()

        # Parse as_of timestamp if provided
        cutoff: datetime | None = None
        if as_of:
            try:
                cutoff = datetime.fromisoformat(as_of)
                if cutoff.tzinfo is None:
                    cutoff = cutoff.replace(tzinfo=timezone.utc)
            except ValueError:
                return json.dumps({
                    "error": "invalid_timestamp",
                    "message": f"as_of must be an ISO 8601 timestamp, got: {as_of!r}",
                    "application_id": id,
                })

        async with pool.acquire() as conn:
            if cutoff is not None:
                rows = await conn.fetch(
                    """
                    SELECT application_id, event_id, event_type,
                        rule_id, rule_version, verdict,
                        evaluation_timestamp, evidence_hash,
                        regulation_set_version, session_id,
                        payload, recorded_at
                    FROM compliance_audit_view
                    WHERE application_id = $1
                    AND recorded_at <= $2
                    ORDER BY recorded_at ASC
                    """,
                    id,
                    cutoff,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT application_id, event_id, event_type,
                        rule_id, rule_version, verdict,
                        evaluation_timestamp, evidence_hash,
                        regulation_set_version, session_id,
                        payload, recorded_at
                    FROM compliance_audit_view
                    WHERE application_id = $1
                    ORDER BY recorded_at ASC
                    """,
                    id,
                )

        records = [
            {
                "application_id": r["application_id"],
                "event_id": str(r["event_id"]),
                "event_type": r["event_type"],
                "rule_id": r["rule_id"],
                "rule_version": r["rule_version"],
                "verdict": r["verdict"],
                # Both timestamps for auditability (Req 11.3)
                "recorded_at": r["recorded_at"].isoformat() if r["recorded_at"] else None,
                "evaluation_timestamp": r["evaluation_timestamp"].isoformat()
                    if r["evaluation_timestamp"] else None,
                "evidence_hash": r["evidence_hash"],
                "regulation_set_version": r["regulation_set_version"],
                "session_id": r["session_id"],
                "payload": r["payload"] if isinstance(r["payload"], dict)
                    else json.loads(r["payload"] or "{}"),
            }
            for r in rows
        ]

        return _json({
            "application_id": id,
            "as_of": cutoff.isoformat() if cutoff else None,
            "record_count": len(records),
            "records": records,
        })


    # ---------------------------------------------------------------------------
    # Resource 3 — ledger://applications/{id}/audit-trail
    # NAMED EXCEPTION: direct AuditLedger stream load (Req 16.4)
    # Supports from/to position range. SLO: p99 < 500ms
    # ---------------------------------------------------------------------------

    @mcp.resource("ledger://applications/{id}/audit-trail")
    async def get_audit_trail(
        id: str,
        from_pos: int = 0,
        to_pos: int | None = None,
    ) -> str:
        """
        Return the AuditLedger stream for a loan application.

        NAMED EXCEPTION to the projection-only rule (Req 16.4):
        Loads the audit-loan-{id} stream directly. Justified because the AuditLedger
        stream IS the authoritative audit record; a separate projection would duplicate
        it without benefit.

        Supports optional from/to stream position range for pagination.
        SLO: p99 < 500ms (Req 16.6).
        """
        store = get_store()
        stream_id = f"audit-loan-{id}"

        events = await store.load_stream(
            stream_id=stream_id,
            from_position=from_pos,
            to_position=to_pos,
        )

        return _json({
            "application_id": id,
            "stream_id": stream_id,
            "from_position": from_pos,
            "to_position": to_pos,
            "event_count": len(events),
            "events": [
                {
                    "event_id": str(e.event_id),
                    "stream_position": e.stream_position,
                    "global_position": e.global_position,
                    "event_type": e.event_type,
                    "event_version": e.event_version,
                    "payload": e.payload,
                    "metadata": e.metadata,
                    "recorded_at": e.recorded_at.isoformat() if e.recorded_at else None,
                }
                for e in events
            ],
        })


    # ---------------------------------------------------------------------------
    # Resource 4 — ledger://agents/{id}/performance
    # Reads AgentPerformanceLedger projection (Req 16.1)
    # SLO: p99 < 50ms
    # ---------------------------------------------------------------------------

    @mcp.resource("ledger://agents/{id}/performance")
    async def get_agent_performance(id: str) -> str:
        """
        Return AgentPerformanceLedger metrics for an agent.

        Reads exclusively from the AgentPerformanceLedger projection table.
        Returns all (agent_id, model_version) rows for the given agent_id.
        SLO: p99 < 50ms (Req 16.6).
        """
        pool = _get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT agent_id, model_version,
                    analyses_completed, decisions_generated,
                    avg_confidence_score, avg_duration_ms,
                    approve_rate, decline_rate, refer_rate,
                    human_override_rate,
                    first_seen_at, last_seen_at
                FROM agent_performance_ledger
                WHERE agent_id = $1
                ORDER BY model_version
                """,
                id,
            )

        if not rows:
            return _json({"error": "not_found", "agent_id": id})

        return _json({
            "agent_id": id,
            "model_versions": [
                {
                    "model_version": r["model_version"],
                    "analyses_completed": r["analyses_completed"],
                    "decisions_generated": r["decisions_generated"],
                    "avg_confidence_score": float(r["avg_confidence_score"])
                        if r["avg_confidence_score"] is not None else None,
                    "avg_duration_ms": float(r["avg_duration_ms"])
                        if r["avg_duration_ms"] is not None else None,
                    "approve_rate": float(r["approve_rate"])
                        if r["approve_rate"] is not None else None,
                    "decline_rate": float(r["decline_rate"])
                        if r["decline_rate"] is not None else None,
                    "refer_rate": float(r["refer_rate"])
                        if r["refer_rate"] is not None else None,
                    "human_override_rate": float(r["human_override_rate"])
                        if r["human_override_rate"] is not None else None,
                    "first_seen_at": r["first_seen_at"].isoformat()
                        if r["first_seen_at"] else None,
                    "last_seen_at": r["last_seen_at"].isoformat()
                        if r["last_seen_at"] else None,
                }
                for r in rows
            ],
        })


    # ---------------------------------------------------------------------------
    # Resource 5 — ledger://agents/{id}/sessions/{session_id}
    # NAMED EXCEPTION: direct AgentSession stream load (Req 16.4)
    # SLO: p99 < 300ms
    # ---------------------------------------------------------------------------

    @mcp.resource("ledger://agents/{id}/sessions/{session_id}")
    async def get_agent_session(id: str, session_id: str) -> str:
        """
        Return the full AgentSession stream for a specific session.

        NAMED EXCEPTION to the projection-only rule (Req 16.4):
        Loads the agent-{id}-{session_id} stream directly. Justified because agent
        session streams are low-volume, session-scoped, and require full replay
        capability that a projection cannot provide.

        SLO: p99 < 300ms (Req 16.6).
        """
        store = get_store()
        stream_id = f"agent-{id}-{session_id}"

        events = await store.load_stream(stream_id=stream_id)

        if not events:
            return _json({"error": "not_found", "agent_id": id, "session_id": session_id})

        return _json({
            "agent_id": id,
            "session_id": session_id,
            "stream_id": stream_id,
            "event_count": len(events),
            "events": [
                {
                    "event_id": str(e.event_id),
                    "stream_position": e.stream_position,
                    "global_position": e.global_position,
                    "event_type": e.event_type,
                    "event_version": e.event_version,
                    "payload": e.payload,
                    "metadata": e.metadata,
                    "recorded_at": e.recorded_at.isoformat() if e.recorded_at else None,
                }
                for e in events
            ],
        })


    # ---------------------------------------------------------------------------
    # Resource 6 — ledger://ledger/health
    # Returns ProjectionDaemon.get_all_lags() (Req 16.5)
    # SLO: p99 < 10ms
    # ---------------------------------------------------------------------------

    @mcp.resource("ledger://ledger/health")
    async def get_ledger_health() -> str:
        """
        Return health metrics for The Ledger, including projection lag for all
        registered projections.

        Reads lag data from projection_checkpoints and the events table.
        SLO: p99 < 10ms (Req 16.6).

        Returns:
            status: "healthy" | "degraded" (degraded if any projection lag > SLO)
            projections: dict of projection_name -> lag_ms
            slo_violations: list of projections exceeding their lag SLO
        """
        pool = _get_pool()

        # SLO thresholds in milliseconds per projection
        _LAG_SLOS: dict[str, int] = {
            "application_summary": 500,
            "compliance_audit_view": 2000,
            "agent_performance_ledger": 2000,
        }

        async with pool.acquire() as conn:
            # Latest event timestamp in the store
            latest_row = await conn.fetchrow(
                "SELECT recorded_at, global_position FROM events "
                "ORDER BY global_position DESC LIMIT 1"
            )

            # All projection checkpoints
            checkpoint_rows = await conn.fetch(
                "SELECT projection_name, last_position, updated_at "
                "FROM projection_checkpoints"
            )

        if latest_row is None:
            return _json({
                "status": "healthy",
                "store_latest_position": 0,
                "projections": {},
                "slo_violations": [],
            })

        latest_ts: datetime = latest_row["recorded_at"]
        if latest_ts.tzinfo is None:
            latest_ts = latest_ts.replace(tzinfo=timezone.utc)

        lags: dict[str, int] = {}
        for row in checkpoint_rows:
            name = row["projection_name"]
            updated_at: datetime = row["updated_at"]
            if updated_at is None:
                lags[name] = -1
                continue
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)
            lag_ms = max(int((latest_ts - updated_at).total_seconds() * 1000), 0)
            lags[name] = lag_ms

        slo_violations = [
            {"projection": name, "lag_ms": lag_ms, "slo_ms": _LAG_SLOS.get(name, 2000)}
            for name, lag_ms in lags.items()
            if lag_ms > _LAG_SLOS.get(name, 2000)
        ]

        return _json({
            "status": "degraded" if slo_violations else "healthy",
            "store_latest_position": latest_row["global_position"],
            "projections": lags,
            "slo_violations": slo_violations,
        })
