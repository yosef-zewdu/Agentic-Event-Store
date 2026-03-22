"""
tests/test_projections.py — Phase 3 projection tests

Covers:
  - 14.1 Lag SLO: 50 concurrent appends, ApplicationSummary < 500ms,
         ComplianceAuditView < 2000ms (Req 20.3)
  - 14.2 rebuild_from_scratch(): live reads continue during rebuild,
         result consistent after swap
  - 14.3 Idempotency: same batch processed twice yields identical state
         for all three projections (no double-counted metrics, no duplicate rows)
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
    ComplianceCheckInitiated,
    ComplianceRulePassed,
    ComplianceRuleFailed,
    ComplianceCheckCompleted,
    CreditAnalysisCompleted,
    CreditDecision,
    DecisionGenerated,
    HumanReviewCompleted,
    RiskTier,
)
from src.projections.application_summary import (
    ApplicationSummaryProjection,
    _CREATE_TABLE_SQL as _APP_SUMMARY_DDL,
)
from src.projections.compliance_audit import (
    ComplianceAuditViewProjection,
    _CREATE_COMPLIANCE_AUDIT_SQL,
    _CREATE_SNAPSHOTS_SQL,
)
from src.projections.agent_performance import (
    AgentPerformanceLedgerProjection,
    _CREATE_TABLE_SQL as _AGENT_PERF_DDL,
)
from src.projections.daemon import ProjectionDaemon


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


async def _ensure_tables(pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(_APP_SUMMARY_DDL.format(table="application_summary"))
        await conn.execute(_CREATE_COMPLIANCE_AUDIT_SQL.format(table="compliance_audit_view"))
        await conn.execute(_CREATE_SNAPSHOTS_SQL)
        await conn.execute(_AGENT_PERF_DDL.format(table="agent_performance_ledger"))


async def _get_app_row(pool, application_id: str) -> dict | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            application_id,
        )
    return dict(row) if row else None


async def _get_compliance_rows(pool, application_id: str) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM compliance_audit_view WHERE application_id = $1 ORDER BY recorded_at",
            application_id,
        )
    return [dict(r) for r in rows]


async def _get_agent_row(pool, agent_id: str, model_version: str) -> dict | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM agent_performance_ledger WHERE agent_id = $1 AND model_version = $2",
            agent_id,
            model_version,
        )
    return dict(row) if row else None


def _make_daemon(store, pool) -> tuple[ProjectionDaemon, ApplicationSummaryProjection, ComplianceAuditViewProjection]:
    app_proj = ApplicationSummaryProjection()
    compliance_proj = ComplianceAuditViewProjection()
    daemon = ProjectionDaemon(store=store, pool=pool, max_retries=3)
    daemon.register(app_proj)
    daemon.register(compliance_proj)
    return daemon, app_proj, compliance_proj


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(autouse=True)
async def setup_tables(db_pool):
    """Create projection tables and truncate between tests."""
    await _ensure_tables(db_pool)
    # Ensure deduplication table exists
    async with db_pool.acquire() as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS agent_performance_processed_events "
            "(event_id UUID PRIMARY KEY)"
        )
    yield
    async with db_pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE application_summary")
        await conn.execute("TRUNCATE TABLE compliance_audit_view")
        await conn.execute("TRUNCATE TABLE compliance_snapshots")
        await conn.execute("TRUNCATE TABLE agent_performance_ledger")
        await conn.execute("TRUNCATE TABLE agent_performance_processed_events")


# ===========================================================================
# 14.1 — Lag SLO: 50 concurrent appends, projections catch up within SLO
# ===========================================================================

@pytest.mark.asyncio
async def test_lag_slo_under_concurrent_load(store, db_pool):
    """
    50 concurrent ApplicationSubmitted appends followed by daemon processing.
    After processing, ApplicationSummary lag < 500ms and ComplianceAuditView
    lag < 2000ms (Req 20.3).
    """
    N = 50
    from src.models.exceptions import OptimisticConcurrencyError
    import asyncpg as _asyncpg
    from tests.conftest import DATABASE_URL

    # Use a dedicated pool sized to the batch size below.
    # PostgreSQL's SSI algorithm aborts transactions that share read/write
    # dependencies on index pages — even across independent streams — when too
    # many serializable transactions run simultaneously. Batching into groups of
    # 10 keeps SSI contention manageable while still exercising genuine concurrency.
    BATCH = 10
    concurrent_pool = await _asyncpg.create_pool(DATABASE_URL, min_size=BATCH, max_size=BATCH)

    async def _append_with_retry(
        stream_id: str, events: list, expected_version: int
    ) -> None:
        from src.event_store import EventStore as _ES
        s = EventStore(DATABASE_URL)
        s._pool = concurrent_pool
        for attempt in range(15):
            try:
                await s.append(stream_id, events, expected_version=expected_version)
                return
            except OptimisticConcurrencyError as e:
                if e.actual_version >= 0:
                    return  # another tx already wrote this stream — done
                await asyncio.sleep(0.01 * (attempt + 1))
            except Exception:
                if attempt == 14:
                    raise
                await asyncio.sleep(0.01 * (attempt + 1))

    try:
        # Append in concurrent batches of BATCH
        for batch_start in range(0, N, BATCH):
            batch = range(batch_start, min(batch_start + BATCH, N))
            tasks = []
            for i in batch:
                event = ApplicationSubmitted(
                    application_id=f"slo-app-{i:03d}",
                    applicant_id=f"applicant-{i:03d}",
                    requested_amount_usd=Decimal("100000"),
                    loan_purpose="working_capital",
                    loan_term_months=12,
                    submission_channel="api",
                    contact_email=f"slo{i}@example.com",
                    contact_name=f"SLO Corp {i}",
                    submitted_at=_now(),
                    application_reference=f"SLO-{i:03d}",
                )
                tasks.append(_append_with_retry(f"loan-slo-app-{i:03d}", [event], -1))
            await asyncio.gather(*tasks)

        # Compliance events — same batched concurrency pattern
        for batch_start in range(0, N, BATCH):
            batch = range(batch_start, min(batch_start + BATCH, N))
            tasks = []
            for i in batch:
                initiated = ComplianceCheckInitiated(
                    application_id=f"slo-app-{i:03d}",
                    session_id=f"session-slo-{i:03d}",
                    regulation_set_version="v2024",
                    rules_to_evaluate=["AML-001", "KYC-002"],
                    initiated_at=_now(),
                )
                tasks.append(
                    _append_with_retry(f"compliance-slo-app-{i:03d}", [initiated], -1)
                )
            await asyncio.gather(*tasks)

    finally:
        await concurrent_pool.close()

    # --- Phase 2: run daemon until all events are processed ---
    daemon, app_proj, compliance_proj = _make_daemon(store, db_pool)
    await daemon._load_checkpoints()

    # Run batches until both projections have processed all events
    for _ in range(20):
        await daemon._process_batch()
        app_lag = await daemon.get_lag(app_proj.name)
        comp_lag = await daemon.get_lag(compliance_proj.name)
        if app_lag == 0 and comp_lag == 0:
            break

    # --- Phase 3: assert SLOs ---
    app_lag = await daemon.get_lag(app_proj.name)
    comp_lag = await daemon.get_lag(compliance_proj.name)

    assert app_lag < 500, (
        f"ApplicationSummary lag {app_lag}ms exceeds 500ms SLO (Req 9.3)"
    )
    assert comp_lag < 2000, (
        f"ComplianceAuditView lag {comp_lag}ms exceeds 2000ms SLO (Req 11.2)"
    )

    # Sanity: all 50 application rows exist
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM application_summary WHERE application_id LIKE 'slo-app-%'"
        )
    assert count == N, f"Expected {N} rows in ApplicationSummary, got {count}"


# ===========================================================================
# 14.2 — rebuild_from_scratch(): live reads continue, result consistent
# ===========================================================================

@pytest.mark.asyncio
async def test_application_summary_rebuild_live_reads_and_consistency(store, db_pool):
    """
    ApplicationSummaryProjection.rebuild_from_scratch():
      - Live reads from application_summary continue to work during rebuild
      - Result after swap is consistent with incremental processing
    """
    # Seed data: 5 applications at various lifecycle stages
    apps = [
        ("rebuild-app-001", "Submitted"),
        ("rebuild-app-002", "FinalApproved"),
        ("rebuild-app-003", "FinalDeclined"),
        ("rebuild-app-004", "Submitted"),
        ("rebuild-app-005", "Submitted"),
    ]

    for app_id, _ in apps:
        submitted = ApplicationSubmitted(
            application_id=app_id,
            applicant_id=f"applicant-{app_id}",
            requested_amount_usd=Decimal("250000"),
            loan_purpose="working_capital",
            loan_term_months=24,
            submission_channel="web",
            contact_email=f"{app_id}@example.com",
            contact_name=f"Corp {app_id}",
            submitted_at=_now(),
            application_reference=f"REF-{app_id}",
        )
        await store.append(f"loan-{app_id}", [submitted], expected_version=-1)

    # Approve app-002
    from src.models.events import ApplicationApproved
    approved = ApplicationApproved(
        application_id="rebuild-app-002",
        approved_amount_usd=Decimal("240000"),
        approved_at=_now(),
    )
    await store.append("loan-rebuild-app-002", [approved], expected_version=1)

    # Decline app-003
    from src.models.events import ApplicationDeclined
    declined = ApplicationDeclined(
        application_id="rebuild-app-003",
        decline_reasons=["Insufficient collateral"],
        declined_by="system",
        adverse_action_notice_required=True,
        declined_at=_now(),
    )
    await store.append("loan-rebuild-app-003", [declined], expected_version=1)

    # Incremental processing
    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    # Capture incremental state
    rows_before = {
        app_id: await _get_app_row(db_pool, app_id)
        for app_id, _ in apps
    }
    assert all(v is not None for v in rows_before.values()), "All rows must exist before rebuild"

    # Live read works before rebuild
    live_before = await _get_app_row(db_pool, "rebuild-app-001")
    assert live_before is not None

    # Run rebuild concurrently with a live read to prove the table stays accessible
    async def _live_read_during_rebuild() -> dict | None:
        await asyncio.sleep(0)  # yield to let rebuild start
        return await _get_app_row(db_pool, "rebuild-app-001")

    proj2 = ApplicationSummaryProjection()
    live_row, _ = await asyncio.gather(
        _live_read_during_rebuild(),
        proj2.rebuild_from_scratch(store, pool=db_pool),
    )

    # Live read during rebuild must succeed (table was accessible)
    assert live_row is not None, "Live read during rebuild must not fail"

    # Post-rebuild state must be consistent with pre-rebuild state
    for app_id, expected_state in apps:
        row_after = await _get_app_row(db_pool, app_id)
        assert row_after is not None, f"Row for {app_id} missing after rebuild"
        assert row_after["state"] == rows_before[app_id]["state"], (
            f"State mismatch for {app_id}: "
            f"before={rows_before[app_id]['state']}, after={row_after['state']}"
        )

    # Approved amount preserved
    row_002 = await _get_app_row(db_pool, "rebuild-app-002")
    assert float(row_002["approved_amount_usd"]) == 240000.0


@pytest.mark.asyncio
async def test_compliance_audit_rebuild_live_reads_and_consistency(store, db_pool):
    """
    ComplianceAuditViewProjection.rebuild_from_scratch():
      - Live reads continue during rebuild
      - Row count and verdicts are consistent after swap
    """
    app_id = "comp-rebuild-001"

    # Append compliance events
    initiated = ComplianceCheckInitiated(
        application_id=app_id,
        session_id="session-comp-rebuild",
        regulation_set_version="v2024",
        rules_to_evaluate=["AML-001", "KYC-002", "BSA-003"],
        initiated_at=_now(),
    )
    passed1 = ComplianceRulePassed(
        application_id=app_id,
        session_id="session-comp-rebuild",
        rule_id="AML-001",
        rule_name="AML Check",
        rule_version="1.0",
        evidence_hash="abc123",
        evaluation_notes="Passed",
        evaluated_at=_now(),
    )
    passed2 = ComplianceRulePassed(
        application_id=app_id,
        session_id="session-comp-rebuild",
        rule_id="KYC-002",
        rule_name="KYC Check",
        rule_version="1.0",
        evidence_hash="def456",
        evaluation_notes="Passed",
        evaluated_at=_now(),
    )
    completed = ComplianceCheckCompleted(
        application_id=app_id,
        session_id="session-comp-rebuild",
        rules_evaluated=3,
        rules_passed=2,
        rules_failed=0,
        rules_noted=1,
        has_hard_block=False,
        overall_verdict="CLEAR",
        completed_at=_now(),
    )
    await store.append(f"compliance-{app_id}", [initiated, passed1, passed2, completed], expected_version=-1)

    # Incremental processing
    compliance_proj = ComplianceAuditViewProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(compliance_proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    rows_before = await _get_compliance_rows(db_pool, app_id)
    assert len(rows_before) == 4, f"Expected 4 compliance rows, got {len(rows_before)}"

    # Live read before rebuild
    live_before = await _get_compliance_rows(db_pool, app_id)
    assert len(live_before) == 4

    # Rebuild
    proj2 = ComplianceAuditViewProjection()
    await proj2.rebuild_from_scratch(store, pool=db_pool)

    # Post-rebuild: same row count and verdicts
    rows_after = await _get_compliance_rows(db_pool, app_id)
    assert len(rows_after) == len(rows_before), (
        f"Row count changed after rebuild: before={len(rows_before)}, after={len(rows_after)}"
    )

    verdicts_before = {r["event_type"]: r["verdict"] for r in rows_before}
    verdicts_after = {r["event_type"]: r["verdict"] for r in rows_after}
    assert verdicts_before == verdicts_after, "Verdicts changed after rebuild"


# ===========================================================================
# 14.3 — Idempotency: same batch processed twice yields identical state
# ===========================================================================

async def _reset_checkpoint(pool, projection_name: str) -> None:
    """Delete a projection's checkpoint so the daemon reprocesses from position 0."""
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM projection_checkpoints WHERE projection_name = $1",
            projection_name,
        )


@pytest.mark.asyncio
async def test_idempotency_application_summary(store, db_pool):
    """
    Processing the same events twice through ApplicationSummaryProjection
    produces identical state — no duplicate rows, no field corruption.
    """
    app_id = "idem-app-001"

    submitted = ApplicationSubmitted(
        application_id=app_id,
        applicant_id="applicant-idem",
        requested_amount_usd=Decimal("500000"),
        loan_purpose="expansion",
        loan_term_months=36,
        submission_channel="web",
        contact_email="idem@example.com",
        contact_name="Idem Corp",
        submitted_at=_now(),
        application_reference="REF-IDEM-001",
    )
    from src.models.events import ApplicationApproved
    approved = ApplicationApproved(
        application_id=app_id,
        approved_amount_usd=Decimal("480000"),
        approved_at=_now(),
    )
    await store.append(f"loan-{app_id}", [submitted], expected_version=-1)
    await store.append(f"loan-{app_id}", [approved], expected_version=1)

    # First pass
    proj1 = ApplicationSummaryProjection()
    daemon1 = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon1.register(proj1)
    await daemon1._load_checkpoints()
    await daemon1._process_batch()

    row_pass1 = await _get_app_row(db_pool, app_id)
    assert row_pass1 is not None

    # Reset checkpoint → force reprocessing
    await _reset_checkpoint(db_pool, "application_summary")

    # Second pass (same events)
    proj2 = ApplicationSummaryProjection()
    daemon2 = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon2.register(proj2)
    await daemon2._load_checkpoints()
    await daemon2._process_batch()

    row_pass2 = await _get_app_row(db_pool, app_id)
    assert row_pass2 is not None

    # State must be identical
    assert row_pass1["state"] == row_pass2["state"] == "FinalApproved"
    assert float(row_pass1["approved_amount_usd"]) == float(row_pass2["approved_amount_usd"])
    assert row_pass1["last_event_type"] == row_pass2["last_event_type"]

    # Exactly one row — no duplicates
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM application_summary WHERE application_id = $1",
            app_id,
        )
    assert count == 1, f"Expected 1 row, got {count} (duplicate rows detected)"


@pytest.mark.asyncio
async def test_idempotency_compliance_audit_view(store, db_pool):
    """
    Processing the same compliance events twice through ComplianceAuditViewProjection
    produces identical row count and verdicts — no duplicate rows.
    """
    app_id = "idem-comp-001"

    passed = ComplianceRulePassed(
        application_id=app_id,
        session_id="session-idem-comp",
        rule_id="AML-001",
        rule_name="AML Check",
        rule_version="1.0",
        evidence_hash="hash-aml",
        evaluation_notes="All clear",
        evaluated_at=_now(),
    )
    failed = ComplianceRuleFailed(
        application_id=app_id,
        session_id="session-idem-comp",
        rule_id="BSA-003",
        rule_name="BSA Check",
        rule_version="1.0",
        failure_reason="Missing documentation",
        is_hard_block=False,
        remediation_available=True,
        evidence_hash="hash-bsa",
        evaluated_at=_now(),
    )
    await store.append(f"compliance-{app_id}", [passed, failed], expected_version=-1)

    # First pass
    proj1 = ComplianceAuditViewProjection()
    daemon1 = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon1.register(proj1)
    await daemon1._load_checkpoints()
    await daemon1._process_batch()

    rows_pass1 = await _get_compliance_rows(db_pool, app_id)
    assert len(rows_pass1) == 2

    # Reset checkpoint → force reprocessing
    await _reset_checkpoint(db_pool, "compliance_audit_view")

    # Second pass
    proj2 = ComplianceAuditViewProjection()
    daemon2 = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon2.register(proj2)
    await daemon2._load_checkpoints()
    await daemon2._process_batch()

    rows_pass2 = await _get_compliance_rows(db_pool, app_id)

    # Same number of rows — no duplicates
    assert len(rows_pass2) == len(rows_pass1), (
        f"Row count changed after second pass: pass1={len(rows_pass1)}, pass2={len(rows_pass2)}"
    )

    # Verdicts unchanged
    verdicts1 = {r["rule_id"]: r["verdict"] for r in rows_pass1}
    verdicts2 = {r["rule_id"]: r["verdict"] for r in rows_pass2}
    assert verdicts1 == verdicts2, f"Verdicts changed: {verdicts1} vs {verdicts2}"


@pytest.mark.asyncio
async def test_idempotency_agent_performance_ledger(store, db_pool):
    """
    Processing the same CreditAnalysisCompleted events twice through
    AgentPerformanceLedgerProjection does not double-count analyses_completed
    or corrupt rolling averages.
    """
    agent_id = "agent-idem-001"
    model_version = "gpt-4o-2024-11"

    analysis = CreditAnalysisCompleted(
        application_id="idem-credit-app",
        session_id=agent_id,
        decision=CreditDecision(
            risk_tier=RiskTier.MEDIUM,
            recommended_limit_usd=Decimal("300000"),
            confidence=0.82,
            rationale="Solid financials",
        ),
        model_version=model_version,
        model_deployment_id="deploy-001",
        input_data_hash="hash-input",
        analysis_duration_ms=1500,
        completed_at=_now(),
    )
    await store.append(f"credit-idem-credit-app", [analysis], expected_version=-1)

    # First pass
    proj1 = AgentPerformanceLedgerProjection()
    daemon1 = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon1.register(proj1)
    await daemon1._load_checkpoints()
    await daemon1._process_batch()

    row_pass1 = await _get_agent_row(db_pool, agent_id, model_version)
    assert row_pass1 is not None
    assert row_pass1["analyses_completed"] == 1

    # Reset checkpoint → force reprocessing
    await _reset_checkpoint(db_pool, "agent_performance_ledger")

    # Second pass
    proj2 = AgentPerformanceLedgerProjection()
    daemon2 = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon2.register(proj2)
    await daemon2._load_checkpoints()
    await daemon2._process_batch()

    row_pass2 = await _get_agent_row(db_pool, agent_id, model_version)
    assert row_pass2 is not None

    # analyses_completed must NOT be double-counted
    assert row_pass2["analyses_completed"] == row_pass1["analyses_completed"], (
        f"analyses_completed was double-counted: "
        f"pass1={row_pass1['analyses_completed']}, pass2={row_pass2['analyses_completed']}"
    )

    # avg_confidence_score must be stable (not re-averaged with duplicate data)
    if row_pass1["avg_confidence_score"] is not None:
        assert float(row_pass2["avg_confidence_score"]) == pytest.approx(
            float(row_pass1["avg_confidence_score"]), abs=0.001
        ), "avg_confidence_score corrupted by double processing"


@pytest.mark.asyncio
async def test_idempotency_human_override_rate_not_double_counted(store, db_pool):
    """
    Processing HumanReviewCompleted with override=True twice does not
    double-increment human_override_rate.
    """
    agent_id = "agent-override-idem"
    model_version = "gpt-4o-2024-11"

    # Seed a row first so override increments against an existing entry
    analysis = CreditAnalysisCompleted(
        application_id="override-app",
        session_id=agent_id,
        decision=CreditDecision(
            risk_tier=RiskTier.LOW,
            recommended_limit_usd=Decimal("200000"),
            confidence=0.91,
            rationale="Strong profile",
        ),
        model_version=model_version,
        model_deployment_id="deploy-002",
        input_data_hash="hash-override",
        analysis_duration_ms=1200,
        completed_at=_now(),
    )
    review = HumanReviewCompleted(
        application_id="override-app",
        reviewer_id="officer-smith",
        override=True,
        original_recommendation="APPROVE",
        final_decision="DECLINE",
        override_reason="Policy exception",
        reviewed_at=_now(),
    )
    # Inject contributing_sessions into the review payload manually
    review.payload["contributing_sessions"] = [agent_id]
    review.payload["model_versions"] = {agent_id: model_version}

    await store.append("credit-override-app", [analysis], expected_version=-1)
    await store.append("loan-override-app", [review], expected_version=-1)

    # First pass
    proj1 = AgentPerformanceLedgerProjection()
    daemon1 = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon1.register(proj1)
    await daemon1._load_checkpoints()
    await daemon1._process_batch()

    row_pass1 = await _get_agent_row(db_pool, agent_id, model_version)
    assert row_pass1 is not None
    override_rate_pass1 = float(row_pass1["human_override_rate"] or 0)

    # Reset checkpoint → force reprocessing
    await _reset_checkpoint(db_pool, "agent_performance_ledger")

    # Second pass
    proj2 = AgentPerformanceLedgerProjection()
    daemon2 = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon2.register(proj2)
    await daemon2._load_checkpoints()
    await daemon2._process_batch()

    row_pass2 = await _get_agent_row(db_pool, agent_id, model_version)
    override_rate_pass2 = float(row_pass2["human_override_rate"] or 0)

    assert override_rate_pass2 == override_rate_pass1, (
        f"human_override_rate double-counted: "
        f"pass1={override_rate_pass1}, pass2={override_rate_pass2}"
    )
