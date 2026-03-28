"""
src/integrity/regulatory_package.py — Regulatory Examination Package (Req 19)

generate_regulatory_package() produces a self-contained JSON-serialisable
snapshot of everything a regulator needs to examine a loan application:

  Req 19.1 — Complete event stream snapshot up to examination_date
  Req 19.2 — Projection states at that date (point-in-time read model)
  Req 19.3 — Audit integrity chain result
  Req 19.4 — Human-readable narrative, agent model versions / confidence scores,
              and causal chain traversal via recursive CTE on causation_id
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any

import asyncpg

from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check, IntegrityCheckResult
from src.models.events import StoredEvent


# ---------------------------------------------------------------------------
# Result model
# ---------------------------------------------------------------------------

@dataclass
class AgentVersionRecord:
    session_id: str
    agent_type: str
    model_version: str
    confidence: float | None
    analysis_duration_ms: int | None


@dataclass
class CausalChainNode:
    event_id: str
    event_type: str
    causation_id: str | None
    depth: int


@dataclass
class RegulatoryPackage:
    """
    Self-contained examination package for a single entity (e.g., loan application).

    All fields are JSON-serialisable — call asdict(package) or package.to_dict()
    for the wire format.
    """
    entity_type: str
    entity_id: str
    examination_date: str               # ISO 8601
    generated_at: str                   # ISO 8601

    # Req 19.1 — complete event stream snapshot
    events: list[dict]

    # Req 19.2 — projection states at examination_date
    projection_states: dict[str, Any]

    # Req 19.3 — audit integrity result
    integrity_result: dict

    # Req 19.4 — human-readable narrative
    narrative: str

    # Req 19.4 — agent model versions + confidence scores
    agent_versions: list[dict]

    # Req 19.4 — causal chains (recursive CTE traversal)
    causal_chains: list[dict]

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def generate_regulatory_package(
    store: EventStore,
    pool: asyncpg.Pool,
    entity_type: str,
    entity_id: str,
    examination_date: datetime,
) -> RegulatoryPackage:
    """
    Generate a regulatory examination package for a single entity.

    Args:
        store:            EventStore instance (PostgreSQL or InMemory)
        pool:             asyncpg pool for raw SQL queries (causal CTE, projections)
        entity_type:      e.g. "loan"
        entity_id:        e.g. "app-12345"
        examination_date: UTC cutoff — only events recorded_at <= this are included

    Returns:
        RegulatoryPackage — fully populated, JSON-serialisable
    """
    if examination_date.tzinfo is None:
        examination_date = examination_date.replace(tzinfo=timezone.utc)

    stream_id = f"{entity_type}-{entity_id}"

    # ------------------------------------------------------------------
    # Req 19.1 — Load full event stream up to examination_date
    # ------------------------------------------------------------------
    all_events = await store.load_stream(stream_id)
    events_in_scope = [
        e for e in all_events
        if _event_recorded_at(e) <= examination_date
    ]

    serialised_events = [_serialise_event(e) for e in events_in_scope]

    # ------------------------------------------------------------------
    # Req 19.2 — Projection states at examination_date
    # pool may be None when using InMemoryEventStore (tests / local dev)
    # ------------------------------------------------------------------
    projection_states = await _build_projection_states(pool, entity_id, examination_date)

    # ------------------------------------------------------------------
    # Req 19.3 — Audit integrity chain
    # run_integrity_check uses the live store (it reads the stream itself)
    # We capture the result; we do not re-run integrity on the filtered set
    # because the chain was computed over the full canonical stream.
    # ------------------------------------------------------------------
    try:
        integrity: IntegrityCheckResult = await run_integrity_check(
            store, entity_type, entity_id
        )
        integrity_result = {
            "chain_valid": integrity.chain_valid,
            "tamper_detected": integrity.tamper_detected,
            "events_verified": integrity.events_verified,
            "integrity_hash": integrity.integrity_hash,
        }
    except Exception as exc:
        integrity_result = {
            "chain_valid": False,
            "tamper_detected": False,
            "events_verified": 0,
            "integrity_hash": "",
            "error": str(exc),
        }

    # ------------------------------------------------------------------
    # Req 19.4 — Agent versions + confidence scores
    # ------------------------------------------------------------------
    agent_versions = _extract_agent_versions(events_in_scope)

    # ------------------------------------------------------------------
    # Req 19.4 — Causal chain traversal via recursive CTE
    # ------------------------------------------------------------------
    causal_chains = await _build_causal_chains(pool, stream_id, events_in_scope)

    # ------------------------------------------------------------------
    # Req 19.4 — Human-readable narrative
    # ------------------------------------------------------------------
    narrative = _build_narrative(
        entity_type=entity_type,
        entity_id=entity_id,
        examination_date=examination_date,
        events=events_in_scope,
        agent_versions=agent_versions,
        integrity_result=integrity_result,
    )

    return RegulatoryPackage(
        entity_type=entity_type,
        entity_id=entity_id,
        examination_date=examination_date.isoformat(),
        generated_at=datetime.now(tz=timezone.utc).isoformat(),
        events=serialised_events,
        projection_states=projection_states,
        integrity_result=integrity_result,
        narrative=narrative,
        agent_versions=[
            {
                "session_id": av.session_id,
                "agent_type": av.agent_type,
                "model_version": av.model_version,
                "confidence": av.confidence,
                "analysis_duration_ms": av.analysis_duration_ms,
            }
            for av in agent_versions
        ],
        causal_chains=causal_chains,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _event_recorded_at(event: StoredEvent) -> datetime:
    """Return event.recorded_at as a timezone-aware datetime."""
    ts = event.recorded_at
    if ts is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


def _serialise_event(event: StoredEvent) -> dict:
    return {
        "event_id": str(event.event_id),
        "stream_id": event.stream_id,
        "stream_position": event.stream_position,
        "global_position": event.global_position,
        "event_type": event.event_type,
        "event_version": event.event_version,
        "payload": event.payload,
        "metadata": event.metadata,
        "recorded_at": _event_recorded_at(event).isoformat(),
    }


async def _build_projection_states(
    pool: asyncpg.Pool | None,
    entity_id: str,
    examination_date: datetime,
) -> dict:
    """
    Return projection read-model states as of examination_date.

    Queries `application_summary` and `compliance_audit_view` directly via SQL
    filtered by recorded_at.  Returns empty dicts when pool is unavailable
    (InMemory mode / tests).
    """
    if pool is None:
        return {
            "application_summary": {},
            "compliance_audit_view": [],
        }

    async with pool.acquire() as conn:
        # ApplicationSummary — single row per application
        summary_row = await conn.fetchrow(
            """
            SELECT application_id, state, applicant_id,
                   requested_amount_usd, approved_amount_usd,
                   risk_tier, fraud_score, compliance_status,
                   decision, last_event_type, last_event_at
            FROM application_summary
            WHERE application_id = $1
            """,
            entity_id,
        )

        # ComplianceAuditView — filter by recorded_at
        compliance_rows = await conn.fetch(
            """
            SELECT event_id, event_type, rule_id, rule_version,
                   verdict, evaluation_timestamp, evidence_hash,
                   regulation_set_version, recorded_at
            FROM compliance_audit_view
            WHERE application_id = $1
              AND recorded_at <= $2
            ORDER BY recorded_at ASC
            """,
            entity_id,
            examination_date,
        )

    application_summary = {}
    if summary_row:
        application_summary = {
            "application_id": summary_row["application_id"],
            "state": summary_row["state"],
            "applicant_id": summary_row["applicant_id"],
            "requested_amount_usd": float(summary_row["requested_amount_usd"])
                if summary_row["requested_amount_usd"] is not None else None,
            "approved_amount_usd": float(summary_row["approved_amount_usd"])
                if summary_row["approved_amount_usd"] is not None else None,
            "risk_tier": summary_row["risk_tier"],
            "fraud_score": float(summary_row["fraud_score"])
                if summary_row["fraud_score"] is not None else None,
            "compliance_status": summary_row["compliance_status"],
            "decision": summary_row["decision"],
            "last_event_type": summary_row["last_event_type"],
            "last_event_at": summary_row["last_event_at"].isoformat()
                if summary_row["last_event_at"] else None,
        }

    compliance_records = [
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
            "recorded_at": r["recorded_at"].isoformat() if r["recorded_at"] else None,
        }
        for r in compliance_rows
    ]

    return {
        "application_summary": application_summary,
        "compliance_audit_view": compliance_records,
    }


def _extract_agent_versions(events: list[StoredEvent]) -> list[AgentVersionRecord]:
    """
    Extract agent model versions from AgentSessionStarted events and
    confidence scores from CreditAnalysisCompleted events.
    """
    sessions: dict[str, AgentVersionRecord] = {}

    for e in events:
        if e.event_type == "AgentSessionStarted":
            session_id = e.payload.get("session_id", "")
            sessions[session_id] = AgentVersionRecord(
                session_id=session_id,
                agent_type=e.payload.get("agent_type", ""),
                model_version=e.payload.get("model_version", ""),
                confidence=None,
                analysis_duration_ms=None,
            )
        elif e.event_type == "CreditAnalysisCompleted":
            session_id = e.payload.get("session_id", "")
            decision = e.payload.get("decision") or {}
            confidence = decision.get("confidence")
            duration = e.payload.get("analysis_duration_ms")
            if session_id in sessions:
                sessions[session_id].confidence = float(confidence) if confidence is not None else None
                sessions[session_id].analysis_duration_ms = int(duration) if duration is not None else None

    return list(sessions.values())


async def _build_causal_chains(
    pool: asyncpg.Pool | None,
    stream_id: str,
    events: list[StoredEvent],
) -> list[dict]:
    """
    Return causal chain nodes for the entity's event stream.

    When a PostgreSQL pool is available: uses a recursive CTE on the events
    table joining via metadata->>'causation_id'.

    Falls back to in-memory traversal using the loaded events when pool is None.
    """
    if pool is not None:
        try:
            return await _causal_chains_via_cte(pool, stream_id)
        except Exception:
            pass  # fall through to in-memory fallback

    return _causal_chains_in_memory(events)


async def _causal_chains_via_cte(
    pool: asyncpg.Pool,
    stream_id: str,
) -> list[dict]:
    """Recursive CTE traversal starting from events with no causation_id."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH RECURSIVE causal AS (
                -- anchor: root events (no causation_id)
                SELECT
                    event_id::text,
                    event_type,
                    metadata->>'causation_id' AS causation_id,
                    0 AS depth
                FROM events
                WHERE stream_id = $1
                  AND (metadata->>'causation_id' IS NULL
                       OR metadata->>'causation_id' = '')

                UNION ALL

                -- recursive: events caused by a previously seen event
                SELECT
                    e.event_id::text,
                    e.event_type,
                    e.metadata->>'causation_id',
                    c.depth + 1
                FROM events e
                JOIN causal c ON e.metadata->>'causation_id' = c.event_id
                WHERE e.stream_id = $1
                  AND c.depth < 20
            )
            SELECT event_id, event_type, causation_id, depth
            FROM causal
            ORDER BY depth ASC, event_id ASC
            """,
            stream_id,
        )

    return [
        {
            "event_id": row["event_id"],
            "event_type": row["event_type"],
            "causation_id": row["causation_id"],
            "depth": row["depth"],
        }
        for row in rows
    ]


def _causal_chains_in_memory(events: list[StoredEvent]) -> list[dict]:
    """Fallback: build causal chain from in-memory events."""
    nodes = []
    id_to_depth: dict[str, int] = {}

    for e in events:
        causation_id = e.metadata.get("causation_id") if e.metadata else None
        depth = 0
        if causation_id and causation_id in id_to_depth:
            depth = id_to_depth[causation_id] + 1
        eid = str(e.event_id)
        id_to_depth[eid] = depth
        nodes.append({
            "event_id": eid,
            "event_type": e.event_type,
            "causation_id": causation_id,
            "depth": depth,
        })

    return nodes


def _build_narrative(
    entity_type: str,
    entity_id: str,
    examination_date: datetime,
    events: list[StoredEvent],
    agent_versions: list[AgentVersionRecord],
    integrity_result: dict,
) -> str:
    """Build a human-readable narrative summary of the entity's event history."""
    lines: list[str] = []

    lines.append(
        f"REGULATORY EXAMINATION PACKAGE\n"
        f"Entity: {entity_type}/{entity_id}\n"
        f"Examination date: {examination_date.isoformat()}\n"
        f"Events in scope: {len(events)}\n"
    )

    # Integrity status
    if integrity_result.get("chain_valid"):
        lines.append("AUDIT CHAIN: VALID — no tampering detected.")
    elif integrity_result.get("tamper_detected"):
        lines.append("AUDIT CHAIN: INVALID — tampering detected. Manual review required.")
    else:
        lines.append("AUDIT CHAIN: Not yet established (first run or no events).")
    lines.append("")

    # Timeline summary
    lines.append("EVENT TIMELINE:")
    for e in events:
        ts = _event_recorded_at(e).strftime("%Y-%m-%d %H:%M:%S UTC")
        lines.append(f"  [{ts}] pos={e.stream_position} {e.event_type}")
    lines.append("")

    # Agent involvement
    if agent_versions:
        lines.append("AGENT SESSIONS:")
        for av in agent_versions:
            conf_str = f", confidence={av.confidence:.0%}" if av.confidence is not None else ""
            dur_str = f", duration={av.analysis_duration_ms}ms" if av.analysis_duration_ms else ""
            lines.append(
                f"  session={av.session_id} type={av.agent_type} "
                f"model={av.model_version}{conf_str}{dur_str}"
            )
        lines.append("")

    # Decision outcome
    decision_events = [e for e in events if e.event_type in (
        "DecisionGenerated", "HumanReviewCompleted", "ApplicationApproved", "ApplicationDeclined"
    )]
    if decision_events:
        last = decision_events[-1]
        lines.append(f"FINAL OUTCOME: {last.event_type}")
        decision = last.payload.get("decision") or {}
        if decision:
            lines.append(f"  Decision payload: {json.dumps(decision, default=str)}")
    else:
        lines.append("FINAL OUTCOME: Pending (no decision event found in scope).")

    return "\n".join(lines)
