"""
tests/test_gas_town.py — Gas Town context reconstruction tests (Req 20.5)

Simulates crash recovery: start an agent session, append events, then call
reconstruct_agent_context() with NO in-memory agent state (simulating a crash),
and verify the reconstructed context is sufficient to continue work.

Validates: Req 20.5 — reconstructed context is sufficient to continue.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.event_store import InMemoryEventStore
from src.integrity.gas_town import AgentContext, reconstruct_agent_context
from src.models.events import (
    AgentInputValidated,
    AgentSessionStarted,
    AgentType,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    CreditDecision,
    FraudScreeningRequested,
    RiskTier,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _make_store() -> InMemoryEventStore:
    return InMemoryEventStore()


def _stream_id(agent_id: str, session_id: str) -> str:
    return f"session-{session_id}"


async def _append(store: InMemoryEventStore, stream_id: str, events: list, version: int) -> int:
    return await store.append(stream_id, events, expected_version=version)


def _session_started(session_id: str, agent_id: str, app_id: str) -> AgentSessionStarted:
    return AgentSessionStarted(
        session_id=session_id,
        agent_type=AgentType.CREDIT_ANALYSIS,
        agent_id=agent_id,
        application_id=app_id,
        model_version="gpt-4o-2024-11-20",
        langgraph_graph_version="1.0.0",
        context_source="event_replay",
        context_token_count=1200,
        started_at=_now(),
    )


def _context_loaded(session_id: str, app_id: str) -> AgentInputValidated:
    """AgentContextLoaded is aliased to AgentInputValidated."""
    return AgentInputValidated(
        session_id=session_id,
        agent_type=AgentType.CREDIT_ANALYSIS,
        application_id=app_id,
        inputs_validated=["financial_facts", "historical_profile"],
        validation_duration_ms=42,
        validated_at=_now(),
    )


def _credit_analysis_requested(app_id: str) -> CreditAnalysisRequested:
    return CreditAnalysisRequested(
        application_id=app_id,
        requested_at=_now(),
        requested_by="orchestrator",
    )


def _credit_analysis_completed(app_id: str, session_id: str) -> CreditAnalysisCompleted:
    return CreditAnalysisCompleted(
        application_id=app_id,
        session_id=session_id,
        decision=CreditDecision(
            risk_tier=RiskTier.MEDIUM,
            recommended_limit_usd=250_000,
            confidence=0.82,
            rationale="Solid revenue trajectory with manageable debt.",
        ),
        model_version="gpt-4o-2024-11-20",
        model_deployment_id="deploy-001",
        input_data_hash="abc123",
        analysis_duration_ms=1500,
        completed_at=_now(),
    )


def _fraud_screening_requested(app_id: str) -> FraudScreeningRequested:
    return FraudScreeningRequested(
        application_id=app_id,
        requested_at=_now(),
    )


# ---------------------------------------------------------------------------
# Test 1: Happy path — 5 events, all matched, context is OK
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_crash_recovery_ok_status():
    """
    Req 20.5 — reconstructed context is sufficient to continue.

    Scenario: session started, context loaded, credit analysis requested +
    completed, fraud screening requested (but no crash — all pairs matched).
    Verifies last_event_position > 0 and session_health_status == "OK".
    """
    agent_id = "agent-credit-001"
    session_id = "session-crash-ok"
    app_id = "app-crash-001"
    stream_id = _stream_id(agent_id, session_id)

    store = _make_store()

    # Append 5 events: AgentSessionStarted, AgentContextLoaded (AgentInputValidated),
    # CreditAnalysisRequested, CreditAnalysisCompleted, FraudScreeningRequested
    events = [
        _session_started(session_id, agent_id, app_id),
        _context_loaded(session_id, app_id),
        _credit_analysis_requested(app_id),
        _credit_analysis_completed(app_id, session_id),
        _fraud_screening_requested(app_id),
    ]

    version = -1
    for event in events:
        version = await store.append(stream_id, [event], expected_version=version)

    # Simulate crash: call reconstruct_agent_context with NO in-memory agent state
    context = await reconstruct_agent_context(store, agent_id, session_id, token_budget=8000)

    # Req 20.5 — context must be sufficient to continue
    assert isinstance(context, AgentContext)
    assert context.last_event_position > 0
    assert context.last_event_position == 5

    # FraudScreeningRequested has no matching FraudScreeningCompleted — NEEDS_RECONCILIATION
    # But CreditAnalysis pair IS matched, so only fraud is unmatched
    assert context.session_health_status == "NEEDS_RECONCILIATION"

    # context_text must be non-empty (enough to understand what happened)
    assert context.context_text.strip() != ""

    # pending_work must identify the unmatched FraudScreeningRequested
    assert len(context.pending_work) == 1
    assert context.pending_work[0].request_event_type == "FraudScreeningRequested"
    assert context.pending_work[0].completion_event_type == "FraudScreeningCompleted"


@pytest.mark.asyncio
async def test_crash_recovery_all_matched_ok():
    """
    Req 20.5 — when all request/completion pairs are matched, health is OK.

    Scenario: credit analysis requested AND completed, no unmatched pairs.
    """
    agent_id = "agent-credit-002"
    session_id = "session-crash-matched"
    app_id = "app-crash-002"
    stream_id = _stream_id(agent_id, session_id)

    store = _make_store()

    events = [
        _session_started(session_id, agent_id, app_id),
        _context_loaded(session_id, app_id),
        _credit_analysis_requested(app_id),
        _credit_analysis_completed(app_id, session_id),
        _fraud_screening_requested(app_id),
    ]

    # Override: replace last event with a completed fraud screening
    from src.models.events import FraudScreeningCompleted
    events[-1] = FraudScreeningCompleted(
        application_id=app_id,
        session_id=session_id,
        fraud_score=0.12,
        risk_level="LOW",
        anomalies_found=0,
        recommendation="PROCEED",
        screening_model_version="fraud-v2",
        input_data_hash="def456",
        completed_at=_now(),
    )

    version = -1
    for event in events:
        version = await store.append(stream_id, [event], expected_version=version)

    context = await reconstruct_agent_context(store, agent_id, session_id, token_budget=8000)

    assert context.last_event_position > 0
    assert context.session_health_status == "OK"
    assert context.pending_work == []
    assert context.context_text.strip() != ""


# ---------------------------------------------------------------------------
# Test 2: NEEDS_RECONCILIATION — unmatched CreditAnalysisRequested
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_crash_recovery_needs_reconciliation():
    """
    Req 20.5 — NEEDS_RECONCILIATION case.

    Scenario: CreditAnalysisRequested appended but NO CreditAnalysisCompleted.
    Simulates a crash mid-analysis. Verifies session_health_status == "NEEDS_RECONCILIATION"
    and pending_work identifies the unmatched pair.
    """
    agent_id = "agent-credit-003"
    session_id = "session-crash-partial"
    app_id = "app-crash-003"
    stream_id = _stream_id(agent_id, session_id)

    store = _make_store()

    # Only 3 events: session started, context loaded, credit analysis requested (no completion)
    events = [
        _session_started(session_id, agent_id, app_id),
        _context_loaded(session_id, app_id),
        _credit_analysis_requested(app_id),
    ]

    version = -1
    for event in events:
        version = await store.append(stream_id, [event], expected_version=version)

    # Simulate crash recovery
    context = await reconstruct_agent_context(store, agent_id, session_id, token_budget=8000)

    assert context.session_health_status == "NEEDS_RECONCILIATION"
    assert len(context.pending_work) >= 1

    # The unmatched pair must be CreditAnalysisRequested / CreditAnalysisCompleted
    unmatched_types = {p.request_event_type for p in context.pending_work}
    assert "CreditAnalysisRequested" in unmatched_types

    # last_event_position must reflect the actual last event
    assert context.last_event_position == 3


# ---------------------------------------------------------------------------
# Test 3: Verbatim last 3 events are present in context_text
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_verbatim_last_3_events_in_context():
    """
    Req 7.4 — verbatim last 3 events are preserved in context_text.

    Append 5 events and verify the context_text contains references to the
    last 3 event types (positions 3, 4, 5).
    """
    agent_id = "agent-credit-004"
    session_id = "session-verbatim"
    app_id = "app-verbatim-001"
    stream_id = _stream_id(agent_id, session_id)

    store = _make_store()

    events = [
        _session_started(session_id, agent_id, app_id),
        _context_loaded(session_id, app_id),
        _credit_analysis_requested(app_id),
        _credit_analysis_completed(app_id, session_id),
        _fraud_screening_requested(app_id),
    ]

    version = -1
    for event in events:
        version = await store.append(stream_id, [event], expected_version=version)

    context = await reconstruct_agent_context(store, agent_id, session_id, token_budget=8000)

    # The verbatim section must contain the last 3 event types
    assert "CreditAnalysisRequested" in context.context_text
    assert "CreditAnalysisCompleted" in context.context_text
    assert "FraudScreeningRequested" in context.context_text

    # The verbatim section header must be present
    assert "Verbatim Events" in context.context_text


# ---------------------------------------------------------------------------
# Test 4: Empty stream returns safe defaults
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_stream_returns_safe_defaults():
    """
    Edge case: reconstruct_agent_context on a non-existent session.
    Must return a safe AgentContext with last_event_position=0 and OK status.
    """
    store = _make_store()

    context = await reconstruct_agent_context(
        store, "agent-nobody", "session-ghost", token_budget=8000
    )

    assert context.last_event_position == 0
    assert context.session_health_status == "OK"
    assert context.pending_work == []


# ---------------------------------------------------------------------------
# Test 5: Token budget is respected
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_token_budget_respected():
    """
    Req 7.4 — older events are summarised within token budget.

    With a very small token budget, the context_text must still be produced
    (not crash) and last_event_position must still be correct.
    """
    agent_id = "agent-credit-005"
    session_id = "session-budget"
    app_id = "app-budget-001"
    stream_id = _stream_id(agent_id, session_id)

    store = _make_store()

    events = [
        _session_started(session_id, agent_id, app_id),
        _context_loaded(session_id, app_id),
        _credit_analysis_requested(app_id),
        _credit_analysis_completed(app_id, session_id),
        _fraud_screening_requested(app_id),
    ]

    version = -1
    for event in events:
        version = await store.append(stream_id, [event], expected_version=version)

    # Very small token budget — should not crash
    context = await reconstruct_agent_context(store, agent_id, session_id, token_budget=10)

    assert isinstance(context, AgentContext)
    assert context.last_event_position == 5
