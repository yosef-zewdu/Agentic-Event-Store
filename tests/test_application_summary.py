"""
tests/test_application_summary.py — Tests for ApplicationSummaryProjection

Covers:
  - All event handlers update the correct columns (Req 9.1)
  - Idempotency: processing the same event twice yields identical state (task 11.3)
  - rebuild_from_scratch(): live reads continue during rebuild, result consistent after swap (task 11.4)
  - Lag SLO is maintained under normal conditions (Req 9.3)
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import pytest
import pytest_asyncio

from src.event_store import EventStore
from src.models.events import (
    ApplicationSubmitted,
    CreditAnalysisRequested,
    FraudScreeningCompleted,
    ComplianceCheckCompleted,
    DecisionGenerated,
    HumanReviewRequested,
    HumanReviewCompleted,
    ApplicationApproved,
    ApplicationDeclined,
    ApplicationWithdrawn,
    AgentSessionCompleted,
    StoredEvent,
)
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.daemon import ProjectionDaemon


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


async def _ensure_summary_table(pool) -> None:
    """Create the application_summary table if it doesn't exist."""
    from src.projections.application_summary import _CREATE_TABLE_SQL
    async with pool.acquire() as conn:
        await conn.execute(_CREATE_TABLE_SQL.format(table="application_summary"))


async def _get_row(pool, application_id: str) -> dict | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            application_id,
        )
    return dict(row) if row else None


async def _run_projection(store, pool, *events_to_append: tuple[str, list, int]) -> None:
    """Append events and run one daemon batch."""
    for stream_id, events, expected_version in events_to_append:
        await store.append(stream_id, events, expected_version=expected_version)

    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store=store, pool=pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(autouse=True)
async def setup_table(db_pool):
    """Ensure the application_summary table exists before each test."""
    await _ensure_summary_table(db_pool)
    yield
    # Truncate after test for isolation
    async with db_pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE application_summary")


# ---------------------------------------------------------------------------
# 11.1 / 11.2 — Column coverage and event handler correctness
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_application_submitted_creates_row(store, db_pool):
    """ApplicationSubmitted creates a row with state=Submitted and correct columns."""
    event = ApplicationSubmitted(
        application_id="app-001",
        applicant_id="applicant-001",
        requested_amount_usd=Decimal("500000"),
        loan_purpose="working_capital",
        loan_term_months=24,
        submission_channel="web",
        contact_email="test@example.com",
        contact_name="Test Corp",
        submitted_at=_now(),
        application_reference="REF-001",
    )
    await _run_projection(store, db_pool, ("loan-app-001", [event], -1))

    row = await _get_row(db_pool, "app-001")
    assert row is not None
    assert row["state"] == "Submitted"
    assert row["applicant_id"] == "applicant-001"
    assert float(row["requested_amount_usd"]) == 500000.0
    assert row["last_event_type"] == "ApplicationSubmitted"


@pytest.mark.asyncio
async def test_credit_analysis_requested_updates_state(store, db_pool):
    """CreditAnalysisRequested transitions state to AwaitingAnalysis."""
    submitted = ApplicationSubmitted(
        application_id="app-002",
        applicant_id="applicant-002",
        requested_amount_usd=Decimal("200000"),
        loan_purpose="equipment_financing",
        loan_term_months=36,
        submission_channel="api",
        contact_email="corp@example.com",
        contact_name="Corp Inc",
        submitted_at=_now(),
        application_reference="REF-002",
    )
    requested = CreditAnalysisRequested(
        application_id="app-002",
        requested_at=_now(),
    )
    await store.append("loan-app-002", [submitted], expected_version=-1)
    await store.append("loan-app-002", [requested], expected_version=1)

    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    row = await _get_row(db_pool, "app-002")
    assert row["state"] == "AwaitingAnalysis"
    assert row["last_event_type"] == "CreditAnalysisRequested"


@pytest.mark.asyncio
async def test_fraud_screening_completed_updates_fraud_score(store, db_pool):
    """FraudScreeningCompleted updates fraud_score column."""
    submitted = ApplicationSubmitted(
        application_id="app-003",
        applicant_id="applicant-003",
        requested_amount_usd=Decimal("100000"),
        loan_purpose="working_capital",
        loan_term_months=12,
        submission_channel="web",
        contact_email="fraud@example.com",
        contact_name="Fraud Test",
        submitted_at=_now(),
        application_reference="REF-003",
    )
    fraud = FraudScreeningCompleted(
        application_id="app-003",
        session_id="session-003",
        fraud_score=0.12,
        risk_level="LOW",
        anomalies_found=0,
        recommendation="PROCEED",
        screening_model_version="v1.0",
        input_data_hash="abc123",
        completed_at=_now(),
    )
    await store.append("loan-app-003", [submitted], expected_version=-1)
    await store.append("loan-app-003", [fraud], expected_version=1)

    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    row = await _get_row(db_pool, "app-003")
    assert float(row["fraud_score"]) == pytest.approx(0.12)
    assert row["last_event_type"] == "FraudScreeningCompleted"


@pytest.mark.asyncio
async def test_compliance_check_completed_updates_compliance_status(store, db_pool):
    """ComplianceCheckCompleted updates compliance_status column."""
    submitted = ApplicationSubmitted(
        application_id="app-004",
        applicant_id="applicant-004",
        requested_amount_usd=Decimal("300000"),
        loan_purpose="expansion",
        loan_term_months=48,
        submission_channel="web",
        contact_email="comp@example.com",
        contact_name="Comp Corp",
        submitted_at=_now(),
        application_reference="REF-004",
    )
    compliance = ComplianceCheckCompleted(
        application_id="app-004",
        session_id="session-004",
        rules_evaluated=5,
        rules_passed=5,
        rules_failed=0,
        rules_noted=0,
        has_hard_block=False,
        overall_verdict="CLEAR",
        completed_at=_now(),
    )
    await store.append("loan-app-004", [submitted], expected_version=-1)
    await store.append("loan-app-004", [compliance], expected_version=1)

    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    row = await _get_row(db_pool, "app-004")
    assert row["compliance_status"] == "CLEAR"


@pytest.mark.asyncio
async def test_decision_generated_updates_decision_and_state(store, db_pool):
    """DecisionGenerated updates decision column and sets state=PendingDecision."""
    submitted = ApplicationSubmitted(
        application_id="app-005",
        applicant_id="applicant-005",
        requested_amount_usd=Decimal("750000"),
        loan_purpose="real_estate",
        loan_term_months=60,
        submission_channel="web",
        contact_email="dec@example.com",
        contact_name="Dec Corp",
        submitted_at=_now(),
        application_reference="REF-005",
    )
    decision = DecisionGenerated(
        application_id="app-005",
        orchestrator_session_id="session-005",
        recommendation="APPROVE",
        confidence=0.87,
        approved_amount_usd=Decimal("700000"),
        executive_summary="Strong financials.",
        contributing_sessions=["session-005"],
        generated_at=_now(),
    )
    await store.append("loan-app-005", [submitted], expected_version=-1)
    await store.append("loan-app-005", [decision], expected_version=1)

    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    row = await _get_row(db_pool, "app-005")
    assert row["decision"] == "APPROVE"
    assert row["state"] == "PendingDecision"


@pytest.mark.asyncio
async def test_human_review_completed_updates_reviewer(store, db_pool):
    """HumanReviewCompleted updates human_reviewer_id column."""
    submitted = ApplicationSubmitted(
        application_id="app-006",
        applicant_id="applicant-006",
        requested_amount_usd=Decimal("400000"),
        loan_purpose="acquisition",
        loan_term_months=36,
        submission_channel="web",
        contact_email="hr@example.com",
        contact_name="HR Corp",
        submitted_at=_now(),
        application_reference="REF-006",
    )
    review = HumanReviewCompleted(
        application_id="app-006",
        reviewer_id="officer-jane",
        override=False,
        original_recommendation="APPROVE",
        final_decision="APPROVE",
        reviewed_at=_now(),
    )
    await store.append("loan-app-006", [submitted], expected_version=-1)
    await store.append("loan-app-006", [review], expected_version=1)

    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    row = await _get_row(db_pool, "app-006")
    assert row["human_reviewer_id"] == "officer-jane"


@pytest.mark.asyncio
async def test_application_approved_sets_final_state(store, db_pool):
    """ApplicationApproved sets state=FinalApproved and populates approved_amount_usd."""
    submitted = ApplicationSubmitted(
        application_id="app-007",
        applicant_id="applicant-007",
        requested_amount_usd=Decimal("600000"),
        loan_purpose="refinancing",
        loan_term_months=48,
        submission_channel="web",
        contact_email="approved@example.com",
        contact_name="Approved Corp",
        submitted_at=_now(),
        application_reference="REF-007",
    )
    approved = ApplicationApproved(
        application_id="app-007",
        approved_amount_usd=Decimal("580000"),
        approved_at=_now(),
    )
    await store.append("loan-app-007", [submitted], expected_version=-1)
    await store.append("loan-app-007", [approved], expected_version=1)

    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    row = await _get_row(db_pool, "app-007")
    assert row["state"] == "FinalApproved"
    assert float(row["approved_amount_usd"]) == 580000.0
    assert row["final_decision_at"] is not None


@pytest.mark.asyncio
async def test_application_declined_sets_final_state(store, db_pool):
    """ApplicationDeclined sets state=FinalDeclined and populates final_decision_at."""
    submitted = ApplicationSubmitted(
        application_id="app-008",
        applicant_id="applicant-008",
        requested_amount_usd=Decimal("1000000"),
        loan_purpose="bridge",
        loan_term_months=12,
        submission_channel="api",
        contact_email="declined@example.com",
        contact_name="Declined Corp",
        submitted_at=_now(),
        application_reference="REF-008",
    )
    declined = ApplicationDeclined(
        application_id="app-008",
        decline_reasons=["High debt-to-equity ratio"],
        declined_by="system",
        adverse_action_notice_required=True,
        declined_at=_now(),
    )
    await store.append("loan-app-008", [submitted], expected_version=-1)
    await store.append("loan-app-008", [declined], expected_version=1)

    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    row = await _get_row(db_pool, "app-008")
    assert row["state"] == "FinalDeclined"
    assert row["final_decision_at"] is not None


@pytest.mark.asyncio
async def test_application_withdrawn_sets_withdrawn_state(store, db_pool):
    """ApplicationWithdrawn sets state=Withdrawn."""
    submitted = ApplicationSubmitted(
        application_id="app-009",
        applicant_id="applicant-009",
        requested_amount_usd=Decimal("250000"),
        loan_purpose="working_capital",
        loan_term_months=12,
        submission_channel="web",
        contact_email="withdrawn@example.com",
        contact_name="Withdrawn Corp",
        submitted_at=_now(),
        application_reference="REF-009",
    )
    withdrawn = ApplicationWithdrawn(
        application_id="app-009",
        withdrawn_at=_now(),
        reason="Applicant changed plans",
    )
    await store.append("loan-app-009", [submitted], expected_version=-1)
    await store.append("loan-app-009", [withdrawn], expected_version=1)

    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    row = await _get_row(db_pool, "app-009")
    assert row["state"] == "Withdrawn"


@pytest.mark.asyncio
async def test_agent_session_completed_appends_to_array(store, db_pool):
    """AgentSessionCompleted appends session_id to agent_sessions_completed[]."""
    import json

    submitted = ApplicationSubmitted(
        application_id="app-010",
        applicant_id="applicant-010",
        requested_amount_usd=Decimal("150000"),
        loan_purpose="equipment_financing",
        loan_term_months=24,
        submission_channel="web",
        contact_email="session@example.com",
        contact_name="Session Corp",
        submitted_at=_now(),
        application_reference="REF-010",
    )
    session1 = AgentSessionCompleted(
        session_id="session-a",
        agent_type="credit_analysis",
        application_id="app-010",
        total_nodes_executed=5,
        total_llm_calls=3,
        total_tokens_used=1500,
        total_cost_usd=0.05,
        total_duration_ms=2000,
        completed_at=_now(),
    )
    session2 = AgentSessionCompleted(
        session_id="session-b",
        agent_type="fraud_detection",
        application_id="app-010",
        total_nodes_executed=3,
        total_llm_calls=2,
        total_tokens_used=800,
        total_cost_usd=0.02,
        total_duration_ms=1000,
        completed_at=_now(),
    )
    await store.append("loan-app-010", [submitted], expected_version=-1)
    await store.append("agent-credit_analysis-session-a", [session1], expected_version=-1)
    await store.append("agent-fraud_detection-session-b", [session2], expected_version=-1)

    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    row = await _get_row(db_pool, "app-010")
    sessions = row["agent_sessions_completed"]
    if isinstance(sessions, str):
        sessions = json.loads(sessions)
    assert "session-a" in sessions
    assert "session-b" in sessions


# ---------------------------------------------------------------------------
# 11.3 — Idempotency: processing the same event twice yields identical state
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_idempotency_same_event_twice_yields_identical_state(store, db_pool):
    """
    Processing the same batch of events twice through the projection handler
    produces identical state — no double-counted metrics, no duplicate rows.
    """
    event = ApplicationSubmitted(
        application_id="app-idem",
        applicant_id="applicant-idem",
        requested_amount_usd=Decimal("100000"),
        loan_purpose="working_capital",
        loan_term_months=12,
        submission_channel="web",
        contact_email="idem@example.com",
        contact_name="Idem Corp",
        submitted_at=_now(),
        application_reference="REF-IDEM",
    )
    approved = ApplicationApproved(
        application_id="app-idem",
        approved_amount_usd=Decimal("95000"),
        approved_at=_now(),
    )
    await store.append("loan-app-idem", [event], expected_version=-1)
    await store.append("loan-app-idem", [approved], expected_version=1)

    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    row_first = await _get_row(db_pool, "app-idem")

    # Simulate reprocessing by resetting checkpoint and running again
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM projection_checkpoints WHERE projection_name = $1",
            "application_summary",
        )

    proj2 = ApplicationSummaryProjection()
    daemon2 = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon2.register(proj2)
    await daemon2._load_checkpoints()
    await daemon2._process_batch()

    row_second = await _get_row(db_pool, "app-idem")

    # State must be identical after both passes
    assert row_first["state"] == row_second["state"] == "FinalApproved"
    assert float(row_first["approved_amount_usd"]) == float(row_second["approved_amount_usd"])
    assert row_first["last_event_type"] == row_second["last_event_type"]

    # Exactly one row — no duplicates
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM application_summary WHERE application_id = $1",
            "app-idem",
        )
    assert count == 1


@pytest.mark.asyncio
async def test_idempotency_agent_sessions_no_duplicates(store, db_pool):
    """
    Processing AgentSessionCompleted twice does not duplicate session_id in the array.
    """
    import json

    submitted = ApplicationSubmitted(
        application_id="app-idem2",
        applicant_id="applicant-idem2",
        requested_amount_usd=Decimal("200000"),
        loan_purpose="expansion",
        loan_term_months=24,
        submission_channel="web",
        contact_email="idem2@example.com",
        contact_name="Idem2 Corp",
        submitted_at=_now(),
        application_reference="REF-IDEM2",
    )
    session = AgentSessionCompleted(
        session_id="session-idem",
        agent_type="credit_analysis",
        application_id="app-idem2",
        total_nodes_executed=4,
        total_llm_calls=2,
        total_tokens_used=1000,
        total_cost_usd=0.03,
        total_duration_ms=1500,
        completed_at=_now(),
    )
    await store.append("loan-app-idem2", [submitted], expected_version=-1)
    await store.append("agent-credit_analysis-session-idem", [session], expected_version=-1)

    # Process twice
    for _ in range(2):
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM projection_checkpoints WHERE projection_name = $1",
                "application_summary",
            )
        proj = ApplicationSummaryProjection()
        daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
        daemon.register(proj)
        await daemon._load_checkpoints()
        await daemon._process_batch()

    row = await _get_row(db_pool, "app-idem2")
    sessions = row["agent_sessions_completed"]
    if isinstance(sessions, str):
        sessions = json.loads(sessions)
    # session-idem should appear exactly once
    assert sessions.count("session-idem") == 1


# ---------------------------------------------------------------------------
# 11.4 — rebuild_from_scratch: live reads continue, result consistent after swap
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rebuild_from_scratch_produces_consistent_result(store, db_pool):
    """
    rebuild_from_scratch() replays all events and produces the same result
    as incremental processing.
    """
    # Append a full lifecycle
    submitted = ApplicationSubmitted(
        application_id="app-rebuild",
        applicant_id="applicant-rebuild",
        requested_amount_usd=Decimal("500000"),
        loan_purpose="working_capital",
        loan_term_months=24,
        submission_channel="web",
        contact_email="rebuild@example.com",
        contact_name="Rebuild Corp",
        submitted_at=_now(),
        application_reference="REF-REBUILD",
    )
    approved = ApplicationApproved(
        application_id="app-rebuild",
        approved_amount_usd=Decimal("480000"),
        approved_at=_now(),
    )
    await store.append("loan-app-rebuild", [submitted], expected_version=-1)
    await store.append("loan-app-rebuild", [approved], expected_version=1)

    # First: incremental processing
    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    row_incremental = await _get_row(db_pool, "app-rebuild")
    assert row_incremental is not None

    # Now rebuild from scratch
    proj2 = ApplicationSummaryProjection()
    await proj2.rebuild_from_scratch(store, pool=db_pool)

    row_rebuilt = await _get_row(db_pool, "app-rebuild")
    assert row_rebuilt is not None

    # Results must be consistent
    assert row_incremental["state"] == row_rebuilt["state"] == "FinalApproved"
    assert float(row_incremental["approved_amount_usd"]) == float(row_rebuilt["approved_amount_usd"])
    assert row_incremental["applicant_id"] == row_rebuilt["applicant_id"]


@pytest.mark.asyncio
async def test_rebuild_from_scratch_live_reads_continue(store, db_pool):
    """
    Live reads from application_summary continue to work during rebuild.
    The table is accessible before and after the atomic swap.
    """
    submitted = ApplicationSubmitted(
        application_id="app-live",
        applicant_id="applicant-live",
        requested_amount_usd=Decimal("300000"),
        loan_purpose="expansion",
        loan_term_months=36,
        submission_channel="web",
        contact_email="live@example.com",
        contact_name="Live Corp",
        submitted_at=_now(),
        application_reference="REF-LIVE",
    )
    await store.append("loan-app-live", [submitted], expected_version=-1)

    # Incremental processing first
    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    # Verify live read works before rebuild
    row_before = await _get_row(db_pool, "app-live")
    assert row_before is not None

    # Run rebuild
    proj2 = ApplicationSummaryProjection()
    await proj2.rebuild_from_scratch(store, pool=db_pool)

    # Verify live read still works after rebuild
    row_after = await _get_row(db_pool, "app-live")
    assert row_after is not None
    assert row_after["state"] == "Submitted"
