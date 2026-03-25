"""
tests/test_audit_chain.py — Cryptographic audit chain tests (Req 14)

Tests cover:
  - First-run: previous_hash="" and chain starts correctly (Req 14.4)
  - Subsequent runs: chain links correctly (Req 14.1, 14.5)
  - Tamper detection: modified payload breaks chain (Req 14.3)
  - AuditIntegrityCheckRun event is appended to audit stream (Req 14.2)
  - chain_valid=False / tamper_detected=True on broken chain (Req 14.3)
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import pytest

from src.event_store import InMemoryEventStore
from src.integrity.audit_chain import IntegrityCheckResult, _json_dumps, run_integrity_check
from src.models.events import ApplicationSubmitted, LoanPurpose


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_store() -> InMemoryEventStore:
    return InMemoryEventStore()


async def _seed_primary_stream(store: InMemoryEventStore, entity_type: str, entity_id: str, n: int = 3) -> None:
    """Append n ApplicationSubmitted events to the primary stream."""
    stream_id = f"{entity_type}-{entity_id}"
    version = -1  # track current stream version
    for i in range(n):
        event = ApplicationSubmitted(
            application_id=entity_id,
            applicant_id=f"applicant-{i}",
            requested_amount_usd=100_000,
            loan_purpose=LoanPurpose.WORKING_CAPITAL,
            loan_term_months=12,
            submission_channel="web",
            contact_email="test@example.com",
            contact_name="Test User",
            submitted_at=datetime.now(tz=timezone.utc),
            application_reference=f"REF-{i:04d}",
        )
        version = await store.append(stream_id, [event], expected_version=version)


# ---------------------------------------------------------------------------
# Test: first-run integrity check
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_first_run_uses_empty_previous_hash():
    """
    Req 14.4 — first run treats previous_hash as empty string.
    Req 14.2 — AuditIntegrityCheckRun is appended to audit stream.
    """
    store = _make_store()
    await _seed_primary_stream(store, "loan", "app-001", n=2)

    result = await run_integrity_check(store, "loan", "app-001")

    assert isinstance(result, IntegrityCheckResult)
    assert result.chain_valid is True
    assert result.tamper_detected is False
    assert result.events_verified == 2
    assert len(result.integrity_hash) == 64  # sha256 hex digest

    # Verify AuditIntegrityCheckRun was appended to the audit stream
    audit_events = await store.load_stream("audit-loan-app-001")
    assert len(audit_events) == 1
    ae = audit_events[0]
    assert ae.event_type == "AuditIntegrityCheckRun"
    assert ae.payload["previous_hash"] == ""
    assert ae.payload["events_verified_count"] == 2
    assert ae.payload["integrity_hash"] == result.integrity_hash


@pytest.mark.asyncio
async def test_first_run_hash_computation():
    """
    Req 14.1 — hash = sha256(previous_hash + concatenated_event_payload_hashes).
    Verify the computed hash matches manual computation.
    """
    store = _make_store()
    await _seed_primary_stream(store, "loan", "app-002", n=2)

    result = await run_integrity_check(store, "loan", "app-002")

    # Manually recompute using the same encoder as audit_chain
    events = await store.load_stream("loan-app-002")
    event_hashes = "".join(
        hashlib.sha256(_json_dumps(e.payload).encode()).hexdigest()
        for e in events
    )
    expected_hash = hashlib.sha256(("" + event_hashes).encode()).hexdigest()

    assert result.integrity_hash == expected_hash


# ---------------------------------------------------------------------------
# Test: subsequent run chains correctly
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_second_run_chains_from_first():
    """
    Req 14.5 — AuditIntegrityCheckRun events are included in subsequent computations.
    Second run's previous_hash must equal first run's integrity_hash.
    """
    store = _make_store()
    await _seed_primary_stream(store, "loan", "app-003", n=2)

    first = await run_integrity_check(store, "loan", "app-003")

    # Append one more event to the primary stream
    extra = ApplicationSubmitted(
        application_id="app-003",
        applicant_id="applicant-extra",
        requested_amount_usd=200_000,
        loan_purpose=LoanPurpose.EXPANSION,
        loan_term_months=24,
        submission_channel="api",
        contact_email="extra@example.com",
        contact_name="Extra User",
        submitted_at=datetime.now(tz=timezone.utc),
        application_reference="REF-EXTRA",
    )
    await store.append("loan-app-003", [extra], expected_version=2)

    second = await run_integrity_check(store, "loan", "app-003")

    assert second.chain_valid is True
    assert second.tamper_detected is False

    # The second audit event's previous_hash must equal the first's integrity_hash
    audit_events = await store.load_stream("audit-loan-app-003")
    assert len(audit_events) == 2
    second_audit = audit_events[1]
    assert second_audit.payload["previous_hash"] == first.integrity_hash


# ---------------------------------------------------------------------------
# Test: tamper detection
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tamper_detection_broken_chain():
    """
    Req 14.3 — when hash chain is broken, return chain_valid=False, tamper_detected=True.

    Simulate tampering by running a first check, then manually injecting a
    corrupted AuditIntegrityCheckRun with a wrong integrity_hash into the
    audit stream, then running a second check.
    """
    store = _make_store()
    await _seed_primary_stream(store, "loan", "app-tamper", n=2)

    # First legitimate check
    await run_integrity_check(store, "loan", "app-tamper")

    # Tamper: inject a fake audit event with a wrong hash directly into the store
    # by appending a new AuditIntegrityCheckRun with a corrupted integrity_hash
    from src.models.events import AuditIntegrityCheckRun
    tampered_event = AuditIntegrityCheckRun(
        entity_type="loan",
        entity_id="app-tamper",
        check_timestamp=datetime.now(tz=timezone.utc),
        events_verified_count=2,
        integrity_hash="deadbeef" * 8,  # wrong hash — 64 chars of garbage
        previous_hash="",
        chain_valid=True,
        tamper_detected=False,
    )
    # Overwrite the audit stream with the tampered event by appending to a fresh store
    # that has the tampered event as the only audit record
    tampered_store = _make_store()
    # Copy primary events
    primary_events = await store.load_stream("loan-app-tamper")
    version = -1
    for e in primary_events:
        version = await tampered_store.append("loan-app-tamper", [e], expected_version=version)
    # Append tampered audit event
    await tampered_store.append(
        "audit-loan-app-tamper",
        [tampered_event],
        expected_version=-1,
        aggregate_type="audit",
    )

    # Now run integrity check on the tampered store
    result = await run_integrity_check(tampered_store, "loan", "app-tamper")

    assert result.chain_valid is False
    assert result.tamper_detected is True


# ---------------------------------------------------------------------------
# Test: empty primary stream
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_primary_stream():
    """
    Edge case: integrity check on a stream with no events.
    Should succeed with events_verified=0 and a valid (empty) hash.
    """
    store = _make_store()
    # Don't seed any events — primary stream is empty

    result = await run_integrity_check(store, "loan", "app-empty")

    assert result.chain_valid is True
    assert result.tamper_detected is False
    assert result.events_verified == 0
    # sha256("" + "") = sha256("") — a valid deterministic hash
    expected = hashlib.sha256(b"").hexdigest()
    assert result.integrity_hash == expected


# ---------------------------------------------------------------------------
# Test: IntegrityCheckResult fields
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_result_fields_populated():
    """
    Req 14.2 — AuditIntegrityCheckRun payload contains required fields.
    """
    store = _make_store()
    await _seed_primary_stream(store, "loan", "app-fields", n=1)

    result = await run_integrity_check(store, "loan", "app-fields")

    audit_events = await store.load_stream("audit-loan-app-fields")
    assert len(audit_events) == 1
    payload = audit_events[0].payload

    assert "events_verified_count" in payload
    assert "integrity_hash" in payload
    assert "previous_hash" in payload
    assert "check_timestamp" in payload
    assert payload["events_verified_count"] == 1
    assert payload["integrity_hash"] == result.integrity_hash
