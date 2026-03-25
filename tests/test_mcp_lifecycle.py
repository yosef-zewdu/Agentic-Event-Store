"""
tests/test_mcp_lifecycle.py — Phase 5: Full MCP lifecycle test (Req 20.4)

Exercises the complete loan application lifecycle exclusively through MCP tools
and resources, asserting that the full trace is present in the compliance view.

Lifecycle under test:
  start_agent_session
  → submit_application
  → record_credit_analysis
  → record_fraud_screening
  → record_compliance_check
  → generate_decision
  → record_human_review
  → query ledger://applications/{id}/compliance
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
import pytest_asyncio

# ---------------------------------------------------------------------------
# Import MCP tool/resource functions directly (bypasses FastMCP transport).
# We patch src.mcp.server._store so get_store() returns our test store.
# ---------------------------------------------------------------------------
import src.mcp.server as _mcp_server
import src.mcp.tools as _tools  # noqa: F401 — registers tools as side-effect
import src.mcp.resources as _resources  # noqa: F401 — registers resources as side-effect
import src.mcp.utils as _mcp_utils

from src.mcp.tools import (
    generate_decision,
    record_compliance_check,
    record_credit_analysis,
    record_fraud_screening,
    record_human_review,
    start_agent_session,
    submit_application,
)
from src.mcp.resources import get_application_compliance
from src.models.events import (
    CreditAnalysisRequested,
    FraudScreeningRequested,
    ComplianceCheckRequested,
    HumanReviewRequested,
)


# ---------------------------------------------------------------------------
# Fixture: inject the test store into the MCP server module
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def mcp_store(store):
    """
    Patch src.mcp.server._store with the test EventStore so all MCP tools
    that call get_store() use the isolated test database.
    """
    
    with patch.object(_mcp_utils, "_store", store):
        yield store


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


async def _advance_loan_state(store, app_id: str, event_cls, **kwargs) -> None:
    """Append a single state-advancing event directly (simulates system events)."""
    v = await store.stream_version(f"loan-{app_id}")
    await store.append(
        f"loan-{app_id}",
        [event_cls(application_id=app_id, requested_at=_now(), **kwargs)],
        expected_version=v,
    )


# ---------------------------------------------------------------------------
# Full lifecycle test (Req 20.4)
# ---------------------------------------------------------------------------

class TestMCPFullLifecycle:

    @pytest.mark.asyncio
    async def test_full_lifecycle_via_mcp_tools(self, mcp_store):
        """
        Drive a complete loan application through every MCP tool in sequence,
        then assert the compliance trace is fully present via the resource.
        """
        app_id = "mcp-lifecycle-001"
        agent_id = "agent-credit"

        # ── 1. Start agent session ──────────────────────────────────────────
        result = await start_agent_session(
            agent_id=agent_id,
            agent_type="credit_analysis",
            context_source="event_replay",
            event_replay_from_position=0,
            context_token_count=2048,
            model_version="gpt-4o",
            application_id=app_id,
        )
        assert result.get("success") is True, f"start_agent_session failed: {result}"
        session_id = result["session_id"]
        assert session_id, "session_id must be non-empty"
        assert result["context_position"] == 2

        # ── 2. Submit application ───────────────────────────────────────────
        result = await submit_application(
            application_id=app_id,
            applicant_id="applicant-001",
            requested_amount_usd=250_000.0,
            loan_purpose="equipment_financing",
        )
        assert result.get("success") is True, f"submit_application failed: {result}"
        assert result["application_id"] == app_id

        # Advance to AwaitingAnalysis (system event — not an MCP tool)
        await _advance_loan_state(mcp_store, app_id, CreditAnalysisRequested)

        # ── 3. Record credit analysis ───────────────────────────────────────
        result = await record_credit_analysis(
            application_id=app_id,
            agent_id=agent_id,
            session_id=session_id,
            risk_tier="LOW",
            recommended_limit_usd=240_000.0,
            model_version="gpt-4o",
            confidence_score=0.88,
            regulatory_basis="Basel III",
            duration_ms=320,
            input_data={"source": "financial_statements"},
        )
        assert result.get("success") is True, f"record_credit_analysis failed: {result}"

        # Advance to FraudScreening state
        await _advance_loan_state(mcp_store, app_id, FraudScreeningRequested)

        # ── 4. Record fraud screening ───────────────────────────────────────
        result = await record_fraud_screening(
            application_id=app_id,
            agent_id=agent_id,
            session_id=session_id,
            fraud_score=0.04,
            screening_model="fraud-detector-v3",
            duration_ms=150,
            flags=[],
        )
        assert result.get("success") is True, f"record_fraud_screening failed: {result}"

        # Advance to ComplianceReview state
        await _advance_loan_state(mcp_store, app_id, ComplianceCheckRequested)

        # ── 5. Record compliance check ──────────────────────────────────────
        result = await record_compliance_check(
            application_id=app_id,
            rule_verdicts=[
                {
                    "rule_id": "AML-001",
                    "rule_version": "2.0",
                    "passed": True,
                    "evidence_hash": "sha256-aml-evidence",
                },
                {
                    "rule_id": "KYC-001",
                    "rule_version": "1.5",
                    "passed": True,
                    "evidence_hash": "sha256-kyc-evidence",
                },
            ],
            regulation_set_version="2024-Q1",
        )
        assert result.get("success") is True, f"record_compliance_check failed: {result}"

        # ── 6. Generate decision ────────────────────────────────────────────
        result = await generate_decision(
            application_id=app_id,
            agent_id=agent_id,
            session_id=session_id,
            recommendation="APPROVE",
            confidence_score=0.88,
            model_versions={"credit": "gpt-4o"},
            contributing_agent_sessions=[session_id],
        )
        assert result.get("success") is True, f"generate_decision failed: {result}"
        assert result["recommendation"] == "APPROVE"
        assert result["confidence_floor_applied"] is False

        # Advance to HumanReview state
        v = await mcp_store.stream_version(f"loan-{app_id}")
        await mcp_store.append(
            f"loan-{app_id}",
            [HumanReviewRequested(
                application_id=app_id,
                reason="Standard review",
                decision_event_id="evt-decision",
                requested_at=_now(),
            )],
            expected_version=v,
        )

        # ── 7. Record human review ──────────────────────────────────────────
        result = await record_human_review(
            application_id=app_id,
            reviewer_id="loan-officer-007",
            decision="APPROVE",
            override=False,
        )
        assert result.get("success") is True, f"record_human_review failed: {result}"
        assert result["final_decision"] == "APPROVE"

        # ── 8. Verify compliance trace via resource ─────────────────────────
        # Manually populate the compliance_audit_view projection from the
        # compliance stream (daemon is not running in tests).
        await self._populate_compliance_view(mcp_store, app_id)

        compliance = json.loads(await get_application_compliance(id=app_id))

        assert compliance["application_id"] == app_id
        assert compliance["record_count"] > 0, "Compliance trace must be non-empty"

        event_types = {r["event_type"] for r in compliance["records"]}
        # At minimum the rule verdicts must be present
        assert "ComplianceRulePassed" in event_types, (
            f"Expected ComplianceRulePassed in compliance trace, got: {event_types}"
        )

        # Both AML and KYC rules must appear
        rule_ids = {r["rule_id"] for r in compliance["records"] if r.get("rule_id")}
        assert "AML-001" in rule_ids, f"AML-001 missing from compliance trace: {rule_ids}"
        assert "KYC-001" in rule_ids, f"KYC-001 missing from compliance trace: {rule_ids}"

        # All returned records must reference this application
        for record in compliance["records"]:
            assert record["application_id"] == app_id

        # Both temporal anchors must be present (Req 11.3)
        for record in compliance["records"]:
            assert "recorded_at" in record
            assert "evaluation_timestamp" in record

    @pytest.mark.asyncio
    async def test_confidence_floor_applied_in_lifecycle(self, mcp_store):
        """
        Req 15.5 — when confidence_score < 0.6, generate_decision must override
        recommendation to REFER regardless of submitted value.
        """
        app_id = "mcp-lifecycle-002"
        agent_id = "agent-credit"

        session_result = await start_agent_session(
            agent_id=agent_id,
            agent_type="credit_analysis",
            context_source="event_replay",
            event_replay_from_position=0,
            context_token_count=1024,
            model_version="gpt-4o",
        )
        session_id = session_result["session_id"]

        await submit_application(
            application_id=app_id,
            applicant_id="applicant-002",
            requested_amount_usd=100_000.0,
        )
        await _advance_loan_state(mcp_store, app_id, CreditAnalysisRequested)
        await record_credit_analysis(
            application_id=app_id,
            agent_id=agent_id,
            session_id=session_id,
            risk_tier="HIGH",
            recommended_limit_usd=80_000.0,
            model_version="gpt-4o",
            confidence_score=0.55,
            regulatory_basis=None,
            duration_ms=200,
            input_data={},
        )
        await _advance_loan_state(mcp_store, app_id, FraudScreeningRequested)
        await record_fraud_screening(
            application_id=app_id,
            agent_id=agent_id,
            session_id=session_id,
            fraud_score=0.1,
            screening_model="fraud-v2",
            duration_ms=100,
        )
        await _advance_loan_state(mcp_store, app_id, ComplianceCheckRequested)
        await record_compliance_check(
            application_id=app_id,
            rule_verdicts=[
                {"rule_id": "AML-001", "rule_version": "1.0", "passed": True, "evidence_hash": "h1"},
            ],
            regulation_set_version="2024-Q1",
        )

        result = await generate_decision(
            application_id=app_id,
            agent_id=agent_id,
            session_id=session_id,
            recommendation="APPROVE",
            confidence_score=0.55,  # below 0.6 floor
            model_versions={"credit": "gpt-4o"},
            contributing_agent_sessions=[session_id],
        )
        assert result.get("success") is True, f"generate_decision failed: {result}"
        assert result["recommendation"] == "REFER", (
            "Confidence floor must override APPROVE → REFER when score < 0.6"
        )
        assert result["confidence_floor_applied"] is True

    @pytest.mark.asyncio
    async def test_fraud_score_out_of_range_returns_validation_error(self, mcp_store):
        """Req 15.9 — fraud_score outside [0.0, 1.0] must return ValidationError."""
        result = await record_fraud_screening(
            application_id="mcp-lifecycle-003",
            agent_id="agent-fraud",
            session_id="session-x",
            fraud_score=1.5,
            screening_model="fraud-v2",
            duration_ms=50,
        )
        assert result["error_type"] == "ValidationError"
        assert "fraud_score" in result["message"]
        assert result["suggested_action"] == "provide_fraud_score_between_0_and_1"

    @pytest.mark.asyncio
    async def test_start_agent_session_returns_context_position_2(self, mcp_store):
        """Req 15.4 — start_agent_session must return context_position = 2."""
        result = await start_agent_session(
            agent_id="agent-test",
            agent_type="credit_analysis",
            context_source="event_replay",
            event_replay_from_position=0,
            context_token_count=512,
            model_version="gpt-4o",
        )
        assert result.get("success") is True
        assert result["context_position"] == 2
        # Verify the stream has exactly 2 events in Gas Town order
        session_id = result["session_id"]
        events = await mcp_store.load_stream(f"session-{session_id}")
        assert len(events) == 2
        assert events[0].event_type == "AgentSessionStarted"
        assert events[1].event_type in ("AgentContextLoaded", "AgentInputValidated")

    @pytest.mark.asyncio
    async def test_duplicate_application_returns_error(self, mcp_store):
        """Submitting the same application_id twice must return an error, not raise."""
        app_id = "mcp-lifecycle-004"
        r1 = await submit_application(
            application_id=app_id,
            applicant_id="applicant-004",
            requested_amount_usd=50_000.0,
        )
        assert r1.get("success") is True

        r2 = await submit_application(
            application_id=app_id,
            applicant_id="applicant-004",
            requested_amount_usd=50_000.0,
        )
        assert "error_type" in r2
        assert r2["error_type"] == "OptimisticConcurrencyError"

    # ------------------------------------------------------------------
    # Helper: populate compliance_audit_view from the compliance stream
    # ------------------------------------------------------------------

    async def _populate_compliance_view(self, store, app_id: str) -> None:
        """
        Replay the compliance stream into the compliance_audit_view table.
        In production this is done by the ProjectionDaemon; in tests we drive
        it directly so the resource query has data to return.
        """
        from src.projections.compliance_audit import ComplianceAuditViewProjection

        projection = ComplianceAuditViewProjection()
        pool = store._pool

        async with pool.acquire() as conn:
            await projection.ensure_table_exists(conn)

        compliance_events = await store.load_stream(f"compliance-{app_id}")
        async with pool.acquire() as conn:
            for event in compliance_events:
                await projection.handle(event, conn)
