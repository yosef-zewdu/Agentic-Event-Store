"""
tests/test_upcasting.py — Upcasting immutability tests (Reqs 13.6, 20.2)

Validates that load_stream() applies upcasting transparently at read time
without modifying the raw payload stored in the events table.

For each upcaster under test:
  1. Insert a v1 event directly via raw SQL (bypassing the EventStore write path)
  2. Read the raw payload bytes from the DB and store them
  3. Call store.load_stream() — verify the returned event is v2 (upcasted)
  4. Read the raw payload bytes from the DB again
  5. Assert the raw payload is byte-for-byte identical before and after the load

Validates: Requirements 13.6, 20.2
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import pytest

# Ensure upcasters are registered against the singleton registry
import src.upcasting.upcasters  # noqa: F401 — side-effect: registers upcasters
from src.upcasting.registry import registry
from src.event_store import EventStore


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _insert_raw_event(
    pool,
    *,
    stream_id: str,
    event_id: str,
    event_type: str,
    event_version: int,
    payload: dict,
    stream_position: int = 1,
) -> None:
    """Insert an event row directly via SQL, bypassing the EventStore write path."""
    async with pool.acquire() as conn:
        # Ensure the stream row exists
        await conn.execute(
            """
            INSERT INTO event_streams (stream_id, aggregate_type, current_version)
            VALUES ($1, $2, $3)
            ON CONFLICT (stream_id) DO UPDATE SET current_version = EXCLUDED.current_version
            """,
            stream_id,
            stream_id.split("-")[0],
            stream_position,
        )
        await conn.execute(
            """
            INSERT INTO events
              (event_id, stream_id, stream_position, event_type, event_version, payload, metadata)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, '{}'::jsonb)
            """,
            uuid.UUID(event_id),
            stream_id,
            stream_position,
            event_type,
            event_version,
            json.dumps(payload),
        )


async def _read_raw_payload_bytes(pool, event_id: str) -> bytes:
    """Return the raw JSONB payload bytes for an event row."""
    async with pool.acquire() as conn:
        # Cast to text to get the canonical JSON representation from PostgreSQL
        raw = await conn.fetchval(
            "SELECT payload::text FROM events WHERE event_id = $1",
            uuid.UUID(event_id),
        )
    assert raw is not None, f"Event {event_id} not found in DB"
    return raw.encode("utf-8")


# ---------------------------------------------------------------------------
# CreditAnalysisCompleted v1 → v2 immutability
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_credit_analysis_completed_upcasting_immutability(store, db_pool):
    """
    Validates: Requirements 13.6, 20.2

    Insert a CreditAnalysisCompleted v1 event directly into the DB.
    Load via load_stream() — verify the returned event is v2 (upcasted).
    Assert the raw DB payload is byte-for-byte unchanged after the load.
    """
    event_id = str(uuid.uuid4())
    stream_id = f"credit-upcast-test-{uuid.uuid4().hex[:8]}"

    # v1 payload — does NOT contain model_version, confidence_score, or regulatory_basis
    v1_payload = {
        "application_id": "app-upcast-001",
        "session_id": "session-upcast-001",
        "decision": {
            "risk_tier": "MEDIUM",
            "recommended_limit_usd": "300000",
            "confidence": 0.78,
            "rationale": "Solid financials",
            "key_concerns": [],
            "data_quality_caveats": [],
            "policy_overrides_applied": [],
        },
        "model_deployment_id": "deploy-v1-001",
        "input_data_hash": "sha256-abc123",
        "analysis_duration_ms": 1200,
        "completed_at": _now_iso(),
        "recorded_at": _now_iso(),
    }

    # Step 1: Insert v1 event directly into DB
    await _insert_raw_event(
        db_pool,
        stream_id=stream_id,
        event_id=event_id,
        event_type="CreditAnalysisCompleted",
        event_version=1,
        payload=v1_payload,
    )

    # Step 2: Read raw payload bytes BEFORE load_stream()
    raw_before = await _read_raw_payload_bytes(db_pool, event_id)

    # Step 3: Load via store.load_stream() — upcasting should be applied
    # Wire the singleton registry into the store for this test
    store.upcasters = registry
    events = await store.load_stream(stream_id)

    assert len(events) == 1, f"Expected 1 event, got {len(events)}"
    loaded = events[0]

    # Verify the event was upcasted to v2
    assert loaded.event_version == 2, (
        f"Expected event_version=2 after upcasting, got {loaded.event_version}"
    )
    assert loaded.event_type == "CreditAnalysisCompleted"

    # v2 must have the new fields added by the upcaster
    assert "model_version" in loaded.payload, "v2 payload must contain 'model_version'"
    assert "confidence_score" in loaded.payload, "v2 payload must contain 'confidence_score'"
    assert "regulatory_basis" in loaded.payload, "v2 payload must contain 'regulatory_basis'"

    # confidence_score must be null (never fabricated per Req 13.4)
    assert loaded.payload["confidence_score"] is None, (
        "confidence_score must be null for v1 events (not fabricated)"
    )

    # Step 4: Read raw payload bytes AFTER load_stream()
    raw_after = await _read_raw_payload_bytes(db_pool, event_id)

    # Step 5: Assert byte-for-byte immutability (Reqs 13.6, 20.2)
    assert raw_before == raw_after, (
        "Raw DB payload was mutated by load_stream()!\n"
        f"  Before: {raw_before!r}\n"
        f"  After:  {raw_after!r}"
    )

    # Also verify the stored version is still v1 in the DB
    async with db_pool.acquire() as conn:
        stored_version = await conn.fetchval(
            "SELECT event_version FROM events WHERE event_id = $1",
            uuid.UUID(event_id),
        )
    assert stored_version == 1, (
        f"DB event_version was modified! Expected 1, got {stored_version}"
    )


# ---------------------------------------------------------------------------
# DecisionGenerated v1 → v2 immutability
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_decision_generated_upcasting_immutability(store, db_pool):
    """
    Validates: Requirements 13.6, 20.2

    Insert a DecisionGenerated v1 event directly into the DB.
    Load via load_stream() — verify the returned event is v2 (upcasted).
    Assert the raw DB payload is byte-for-byte unchanged after the load.
    """
    event_id = str(uuid.uuid4())
    stream_id = f"loan-upcast-test-{uuid.uuid4().hex[:8]}"

    # v1 payload — does NOT contain model_versions
    v1_payload = {
        "application_id": "app-upcast-002",
        "orchestrator_session_id": "orch-session-001",
        "recommendation": "APPROVE",
        "confidence": 0.85,
        "approved_amount_usd": "450000",
        "conditions": ["Annual review required"],
        "executive_summary": "Strong application with solid financials.",
        "key_risks": ["Market concentration risk"],
        "contributing_sessions": ["session-a", "session-b"],
        "generated_at": _now_iso(),
        "recorded_at": _now_iso(),
    }

    # Step 1: Insert v1 event directly into DB
    await _insert_raw_event(
        db_pool,
        stream_id=stream_id,
        event_id=event_id,
        event_type="DecisionGenerated",
        event_version=1,
        payload=v1_payload,
    )

    # Step 2: Read raw payload bytes BEFORE load_stream()
    raw_before = await _read_raw_payload_bytes(db_pool, event_id)

    # Step 3: Load via store.load_stream() — upcasting should be applied
    store.upcasters = registry
    events = await store.load_stream(stream_id)

    assert len(events) == 1, f"Expected 1 event, got {len(events)}"
    loaded = events[0]

    # Verify the event was upcasted to v2
    assert loaded.event_version == 2, (
        f"Expected event_version=2 after upcasting, got {loaded.event_version}"
    )
    assert loaded.event_type == "DecisionGenerated"

    # v2 must have model_versions dict added by the upcaster
    assert "model_versions" in loaded.payload, "v2 payload must contain 'model_versions'"
    assert isinstance(loaded.payload["model_versions"], dict), (
        "model_versions must be a dict"
    )

    # Each contributing session must appear as a key in model_versions
    for session_id in v1_payload["contributing_sessions"]:
        assert session_id in loaded.payload["model_versions"], (
            f"Contributing session '{session_id}' missing from model_versions"
        )

    # Step 4: Read raw payload bytes AFTER load_stream()
    raw_after = await _read_raw_payload_bytes(db_pool, event_id)

    # Step 5: Assert byte-for-byte immutability (Reqs 13.6, 20.2)
    assert raw_before == raw_after, (
        "Raw DB payload was mutated by load_stream()!\n"
        f"  Before: {raw_before!r}\n"
        f"  After:  {raw_after!r}"
    )

    # Also verify the stored version is still v1 in the DB
    async with db_pool.acquire() as conn:
        stored_version = await conn.fetchval(
            "SELECT event_version FROM events WHERE event_id = $1",
            uuid.UUID(event_id),
        )
    assert stored_version == 1, (
        f"DB event_version was modified! Expected 1, got {stored_version}"
    )


# ---------------------------------------------------------------------------
# Verify upcasting does NOT affect events already at current version
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_current_version_event_not_modified(store, db_pool):
    """
    Validates: Requirement 13.6

    A v2 CreditAnalysisCompleted event stored directly should pass through
    load_stream() unchanged — no upcaster should be applied, and the raw
    payload must remain byte-for-byte identical.
    """
    event_id = str(uuid.uuid4())
    stream_id = f"credit-v2-test-{uuid.uuid4().hex[:8]}"

    # v2 payload — already has all v2 fields
    v2_payload = {
        "application_id": "app-v2-001",
        "session_id": "session-v2-001",
        "decision": {
            "risk_tier": "LOW",
            "recommended_limit_usd": "500000",
            "confidence": 0.92,
            "rationale": "Excellent financials",
            "key_concerns": [],
            "data_quality_caveats": [],
            "policy_overrides_applied": [],
        },
        "model_version": "credit-model-v2.0",
        "model_deployment_id": "deploy-v2-001",
        "input_data_hash": "sha256-def456",
        "analysis_duration_ms": 900,
        "confidence_score": 0.92,
        "regulatory_basis": ["REG-2024-v1"],
        "completed_at": _now_iso(),
        "recorded_at": _now_iso(),
    }

    await _insert_raw_event(
        db_pool,
        stream_id=stream_id,
        event_id=event_id,
        event_type="CreditAnalysisCompleted",
        event_version=2,
        payload=v2_payload,
    )

    raw_before = await _read_raw_payload_bytes(db_pool, event_id)

    store.upcasters = registry
    events = await store.load_stream(stream_id)

    assert len(events) == 1
    loaded = events[0]
    assert loaded.event_version == 2

    raw_after = await _read_raw_payload_bytes(db_pool, event_id)

    assert raw_before == raw_after, (
        "Raw DB payload was mutated even for a current-version event!\n"
        f"  Before: {raw_before!r}\n"
        f"  After:  {raw_after!r}"
    )
