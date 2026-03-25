"""
tests/test_narratives.py -- Narrative scenario tests for The Ledger

NARR-01: Concurrent OCC collision (CreditAnalysisAgent)
NARR-02: Document extraction with missing EBITDA
NARR-03: Agent crash recovery (skipped -- TODO)
NARR-04: Compliance hard block (Montana / REG-003)
NARR-05: Human override (DECLINE -> APPROVE)

Run: pytest tests/test_narratives.py -v
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.agents.compliance_agent import ComplianceAgent
from src.agents.credit_analysis_agent import CreditAnalysisAgent
from src.commands.handlers import (
    handle_compliance_check,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_generate_decision,
    handle_human_review_completed,
    handle_request_human_review,
    handle_start_agent_session,
    handle_submit_application,
)
from src.registry.client import CompanyProfile, ComplianceFlag


def _now():
    return datetime.now(tz=timezone.utc)


def make_mock_registry(jurisdiction="CA", legal_type="LLC", founded_year=2018, flags=None):
    registry = MagicMock()
    profile = CompanyProfile(
        company_id="COMP-TEST", name="Test Co", industry="Tech",
        naics="541511", jurisdiction=jurisdiction, legal_type=legal_type,
        founded_year=founded_year, employee_count=50, risk_segment="MEDIUM",
        trajectory="STABLE", submission_channel="web", ip_region="US-CA",
    )
    registry.get_company = AsyncMock(return_value=profile)
    registry.get_compliance_flags = AsyncMock(return_value=flags or [])
    registry.get_financial_history = AsyncMock(return_value=[])
    registry.get_loan_relationships = AsyncMock(return_value=[])
    return registry


def make_mock_openai_client():
    return MagicMock()


async def _submit(store, app_id, amount=100_000.0):
    return await handle_submit_application(
        store, application_id=app_id,
        applicant_id=f"applicant-{app_id}",
        requested_amount_usd=amount,
    )


async def _setup_to_credit_analysis_requested(store, app_id):
    await _submit(store, app_id)
    from src.models.events import CreditAnalysisRequested
    v = await store.stream_version(f"loan-{app_id}")
    await store.append(
        f"loan-{app_id}",
        [CreditAnalysisRequested(application_id=app_id, requested_at=_now())],
        expected_version=v,
    )


async def _full_setup_to_compliance_complete(store, app_id):
    await _submit(store, app_id)
    from src.models.events import (
        CreditAnalysisRequested, FraudScreeningRequested,
    )
    v = await store.stream_version(f"loan-{app_id}")
    await store.append(
        f"loan-{app_id}",
        [CreditAnalysisRequested(application_id=app_id, requested_at=_now())],
        expected_version=v,
    )
    sid = await handle_start_agent_session(
        store, agent_id="agent-credit", agent_type="credit_analysis",
        context_source="event_replay", event_replay_from_position=0,
        context_token_count=1000, model_version="gpt-4o",
    )
    await handle_credit_analysis_completed(
        store, application_id=app_id, agent_id="agent-credit", session_id=sid,
        risk_tier="LOW", recommended_limit_usd=90_000.0, model_version="gpt-4o",
        confidence_score=0.85, regulatory_basis="Basel III", duration_ms=200,
        input_data={"source": "test"},
    )
    v = await store.stream_version(f"loan-{app_id}")
    await store.append(
        f"loan-{app_id}",
        [FraudScreeningRequested(application_id=app_id, requested_at=_now())],
        expected_version=v,
    )
    await handle_fraud_screening_completed(
        store, application_id=app_id, agent_id="agent-fraud", session_id=sid,
        fraud_score=0.05, screening_model="fraud-v2", duration_ms=100, flags=[],
    )
    # handle_compliance_check internally appends ComplianceCheckRequested — don't pre-append it
    from src.models.events import ComplianceCheckRequested
    v = await store.stream_version(f"loan-{app_id}")
    await store.append(
        f"loan-{app_id}",
        [ComplianceCheckRequested(application_id=app_id, requested_at=_now())],
        expected_version=v,
    )
    # Use direct store appends to write compliance events without going through the handler
    # (which would try to append ComplianceCheckRequested again)
    from src.models.events import ComplianceRulePassed, ComplianceCheckCompleted
    now = _now()
    compliance_ver = await store.stream_version(f"compliance-{app_id}")
    await store.append(
        f"compliance-{app_id}",
        [
            ComplianceRulePassed(
                application_id=app_id, session_id="",
                rule_id="AML-001", rule_name="AML-001", rule_version="1.0",
                evidence_hash="h1", evaluation_notes="", evaluated_at=now,
            ),
            ComplianceRulePassed(
                application_id=app_id, session_id="",
                rule_id="KYC-001", rule_name="KYC-001", rule_version="1.0",
                evidence_hash="h2", evaluation_notes="", evaluated_at=now,
            ),
            ComplianceCheckCompleted(
                application_id=app_id, session_id="",
                rules_evaluated=2, rules_passed=2, rules_failed=0, rules_noted=0,
                has_hard_block=False, overall_verdict="CLEAR", completed_at=now,
            ),
        ],
        expected_version=compliance_ver,
        aggregate_type="compliance_record",
    )
    # Write ComplianceCheckCompleted to loan stream to advance state
    loan_ver = await store.stream_version(f"loan-{app_id}")
    await store.append(
        f"loan-{app_id}",
        [ComplianceCheckCompleted(
            application_id=app_id, session_id="",
            rules_evaluated=2, rules_passed=2, rules_failed=0, rules_noted=0,
            has_hard_block=False, overall_verdict="CLEAR", completed_at=now,
        )],
        expected_version=loan_ver,
        aggregate_type="loan_application",
    )


def _etype(e):
    return e.get("event_type", "") if isinstance(e, dict) else getattr(e, "event_type", "")


def _payload(e):
    return e.get("payload", {}) if isinstance(e, dict) else getattr(e, "payload", {})


class MockStore:
    def __init__(self):
        self.streams = {}

    async def load_stream(self, stream_id):
        return self.streams.get(stream_id, [])

    async def stream_version(self, stream_id):
        return len(self.streams.get(stream_id, [])) - 1

    async def append(self, stream_id, events, **kwargs):
        if stream_id not in self.streams:
            self.streams[stream_id] = []
        self.streams[stream_id].extend(events)
        return len(self.streams[stream_id]) - 1

    def get_events(self, stream_id):
        return self.streams.get(stream_id, [])


# ===========================================================================
# NARR-01: Concurrent OCC collision
# ===========================================================================

class TestNarr01ConcurrentOCC:
    """NARR-01: Two CreditAnalysisAgent instances run simultaneously.
    Expected: exactly ONE CreditAnalysisCompleted in credit stream."""

    async def test_only_one_credit_analysis_completed(self, store):
        app_id = "narr01-occ-001"
        await _setup_to_credit_analysis_requested(store, app_id)

        docpkg_event = {
            "event_type": "ExtractionCompleted",
            "payload": {
                "document_id": f"income_statement-{app_id}",
                "document_type": "income_statement",
                "facts": {"fiscal_year": 2024, "total_revenue": 5_000_000,
                           "ebitda": 800_000, "net_income": 400_000},
            },
        }
        v = await store.stream_version(f"docpkg-{app_id}")
        await store.append(f"docpkg-{app_id}", [docpkg_event], expected_version=v)

        registry = make_mock_registry()
        client = make_mock_openai_client()

        async def mock_llm(system, user, max_tokens=1024):
            return (
                '{"risk_tier":"LOW","recommended_limit_usd":80000,'
                '"confidence":0.75,"rationale":"test",'
                '"key_concerns":[],"data_quality_caveats":[],'
                '"policy_overrides_applied":[]}',
                100, 50, 0.0,
            )

        agent1 = CreditAnalysisAgent(
            agent_id="agent-credit-A", agent_type="credit_analysis",
            store=store, registry=registry, client=client,
        )
        agent1._call_llm = mock_llm

        agent2 = CreditAnalysisAgent(
            agent_id="agent-credit-B", agent_type="credit_analysis",
            store=store, registry=registry, client=client,
        )
        agent2._call_llm = mock_llm

        results = await asyncio.gather(
            agent1.process_application(app_id),
            agent2.process_application(app_id),
            return_exceptions=True,
        )

        errors = [r for r in results if isinstance(r, Exception)]
        # With idempotent nodes, concurrent agents should both succeed without conflicts
        assert len(errors) == 0, (
            f"Expected no agents to fail due to idempotency. Results: {results}"
        )

        credit_events = await store.load_stream(f"credit-{app_id}")
        completed = [e for e in credit_events if _etype(e) == "CreditAnalysisCompleted"]
        assert len(completed) == 1, (
            f"Expected exactly 1 CreditAnalysisCompleted, found {len(completed)}"
        )


# ===========================================================================
# NARR-02: Document extraction with missing EBITDA
# ===========================================================================

class TestNarr02MissingEbitda:
    """NARR-02: Income statement with missing EBITDA -> quality assessment flags it.
    Uses MockStore + mocked _call_llm -- no database required."""

    async def test_missing_ebitda_flagged_in_quality_assessment(self):
        from src.agents.document_processor import DocumentProcessingAgent

        app_id = "narr02-ebitda-001"
        applicant_id = "COMP-001"

        store = MockStore()

        class FakeSubmittedEvent:
            event_type = "ApplicationSubmitted"
            payload = {
                "applicant_id": applicant_id,
                "application_id": app_id,
                "requested_amount_usd": 500_000.0,
            }

        store.streams[f"loan-{app_id}"] = [FakeSubmittedEvent()]

        registry = make_mock_registry()
        client = make_mock_openai_client()

        income_response = (
            '{"fiscal_year":2024,"total_revenue":5000000,'
            '"cost_of_goods_sold":3000000,"gross_profit":2000000,'
            '"operating_expenses":1200000,"depreciation_amortization":null,'
            '"operating_income":800000,"interest_expense":100000,'
            '"income_before_tax":700000,"tax_expense":175000,'
            '"net_income":525000,"ebitda":null}',
            100, 50, 0.0,
        )
        balance_response = (
            '{"fiscal_year":2024,"total_assets":10000000,'
            '"current_assets":3000000,"cash_and_equivalents":500000,'
            '"accounts_receivable":1000000,"inventory":1500000,'
            '"property_plant_equipment_net":7000000,'
            '"total_liabilities":6000000,"current_liabilities":2000000,'
            '"accounts_payable":800000,"accrued_liabilities":600000,'
            '"current_portion_long_term_debt":600000,'
            '"long_term_debt":2000000,"other_long_term_liabilities":2000000,'
            '"total_equity":4000000}',
            100, 50, 0.0,
        )
        quality_response = (
            '{"overall_confidence":0.55,"is_coherent":false,'
            '"anomalies":["EBITDA is null - cannot compute debt service coverage"],'
            '"critical_missing_fields":["ebitda"],'
            '"reextraction_recommended":true,'
            '"auditor_notes":"EBITDA missing from income statement"}',
            80, 40, 0.0,
        )

        call_count = 0

        async def mock_llm(system, user, max_tokens=1024):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return income_response
            elif call_count == 2:
                return balance_response
            else:
                return quality_response

        agent = DocumentProcessingAgent(
            agent_id="doc-agent-test", agent_type="DocumentProcessing",
            store=store, registry=registry, client=client,
        )
        agent._call_llm = mock_llm

        await agent.process_application(app_id)

        docpkg_events = store.get_events(f"docpkg-{app_id}")
        qa_events = [e for e in docpkg_events if _etype(e) == "QualityAssessmentCompleted"]
        assert len(qa_events) >= 1, "Expected at least one QualityAssessmentCompleted event"

        flagged = any(
            "ebitda" in _payload(e).get("critical_missing_fields", [])
            or len(_payload(e).get("anomalies", [])) > 0
            for e in qa_events
        )
        assert flagged, (
            "Expected QualityAssessmentCompleted to flag missing ebitda. Got: "
            + str([_payload(e) for e in qa_events])
        )


# ===========================================================================
# NARR-03: Agent crash recovery (SKIPPED)
# ===========================================================================

# ===========================================================================
# NARR-03: Agent crash recovery
# ===========================================================================

class TestNarr03AgentCrashRecovery:
    """NARR-03: Agent crashes mid-run (after open_credit_record, before write_output).
    Recovery resumes from the same session, skips completed nodes, produces exactly one CreditAnalysisCompleted."""

    async def test_agent_crash_recovery(self, store):
        app_id = "narr03-crash-001"
        await _setup_to_credit_analysis_requested(store, app_id)

        docpkg_event = {
            "event_type": "ExtractionCompleted",
            "payload": {
                "document_id": f"income_statement-{app_id}",
                "document_type": "income_statement",
                "facts": {"fiscal_year": 2024, "total_revenue": 5_000_000,
                           "ebitda": 800_000, "net_income": 400_000},
            },
        }
        v = await store.stream_version(f"docpkg-{app_id}")
        await store.append(f"docpkg-{app_id}", [docpkg_event], expected_version=v)

        registry = make_mock_registry()
        client = make_mock_openai_client()

        async def mock_llm(system, user, max_tokens=1024):
            return (
                '{"risk_tier":"LOW","recommended_limit_usd":80000,'
                '"confidence":0.75,"rationale":"test",'
                '"key_concerns":[],"data_quality_caveats":[],'
                '"policy_overrides_applied":[]}',
                100, 50, 0.0,
            )

        # First run: crash at analyze_credit_risk
        agent1 = CreditAnalysisAgent(
            agent_id="agent-credit-crash", agent_type="credit_analysis",
            store=store, registry=registry, client=client,
        )
        agent1._call_llm = mock_llm
        agent1._fail_at_node = "analyze_credit_risk"  # Simulate crash after LLM call

        session_id = None
        try:
            await agent1.process_application(app_id)
        except ValueError as e:
            if "Simulated crash" in str(e):
                session_id = agent1.session_id
            else:
                raise

        assert session_id, "Agent should have crashed and set session_id"

        # Check partial state: some nodes completed, but not write_output
        session_events = await store.load_stream(f"session-{session_id}")
        executed_nodes = [e["payload"]["node_name"] for e in session_events if e["event_type"] == "AgentNodeExecuted"]
        assert "open_credit_record" in executed_nodes
        assert "analyze_credit_risk" in executed_nodes  # Recorded before crash
        assert "write_output" not in executed_nodes

        # Credit stream has some events but not CreditAnalysisCompleted
        credit_events = await store.load_stream(f"credit-{app_id}")
        credit_types = [e["event_type"] for e in credit_events]
        assert "CreditRecordOpened" in credit_types
        assert "CreditAnalysisCompleted" not in credit_types

        # Resume with the same session_id
        agent2 = CreditAnalysisAgent(
            agent_id="agent-credit-resume", agent_type="credit_analysis",
            store=store, registry=registry, client=client,
        )
        agent2._call_llm = mock_llm
        # No fail_at_node for resume

        await agent2.process_application(app_id, resume_session_id=session_id)

        # Now check final state: exactly one CreditAnalysisCompleted
        credit_events_after = await store.load_stream(f"credit-{app_id}")
        completed = [e for e in credit_events_after if e["event_type"] == "CreditAnalysisCompleted"]
        assert len(completed) == 1, f"Expected exactly 1 CreditAnalysisCompleted, found {len(completed)}"

        # Session completed
        session_events_after = await store.load_stream(f"session-{session_id}")
        assert any(e["event_type"] == "AgentSessionCompleted" for e in session_events_after)


# ===========================================================================
# NARR-04: Compliance hard block (Montana)
# ===========================================================================

class TestNarr04MontanaHardBlock:
    """NARR-04: Montana applicant triggers REG-003 hard block.
    Expected: ComplianceRuleFailed(REG-003, is_hard_block=True),
              ApplicationDeclined in loan stream, NO DecisionGenerated."""

    async def test_montana_triggers_reg003_hard_block(self, store):
        app_id = "narr04-montana-001"
        await _submit(store, app_id)

        registry = make_mock_registry(jurisdiction="MT")
        client = make_mock_openai_client()

        agent = ComplianceAgent(
            agent_id="compliance-agent-test", agent_type="compliance",
            store=store, registry=registry, client=client,
        )
        await agent.process_application(app_id)

        compliance_events = await store.load_stream(f"compliance-{app_id}")
        reg003_failures = [
            e for e in compliance_events
            if _etype(e) == "ComplianceRuleFailed"
            and _payload(e).get("rule_id") == "REG-003"
        ]
        assert len(reg003_failures) == 1, (
            f"Expected exactly 1 ComplianceRuleFailed for REG-003, found {len(reg003_failures)}. "
            f"Compliance events: {[_etype(e) for e in compliance_events]}"
        )
        assert _payload(reg003_failures[0]).get("is_hard_block") is True, (
            f"Expected is_hard_block=True for REG-003, got: {_payload(reg003_failures[0])}"
        )

        loan_events = await store.load_stream(f"loan-{app_id}")
        loan_types = [_etype(e) for e in loan_events]
        assert "ApplicationDeclined" in loan_types, (
            f"Expected ApplicationDeclined in loan stream. Got: {loan_types}"
        )
        assert "DecisionGenerated" not in loan_types, (
            f"Expected NO DecisionGenerated after hard block. Loan events: {loan_types}"
        )


# ===========================================================================
# NARR-05: Human override (DECLINE -> APPROVE)
# ===========================================================================

class TestNarr05HumanOverride:
    """NARR-05: Orchestrator recommends DECLINE (low confidence),
    human loan officer overrides to APPROVE.
    Expected: DecisionGenerated(DECLINE) -> HumanReviewRequested ->
              HumanReviewCompleted(override=True, reviewer_id='LO-Sarah-Chen') ->
              ApplicationApproved."""

    async def test_human_override_approve(self, store):
        app_id = "narr05-override-001"
        await _full_setup_to_compliance_complete(store, app_id)

        await handle_generate_decision(
            store, application_id=app_id, agent_id="orchestrator-agent",
            session_id="sess-orch-001", recommendation="DECLINE",
            confidence_score=0.75,   # above 0.6 floor so DECLINE is allowed
            model_versions={"credit": "gpt-4o"},
            contributing_agent_sessions=[],
        )

        await handle_request_human_review(
            store, application_id=app_id,
            reason="Low confidence score -- escalated to loan officer",
            decision_event_id="evt-decision-001",
        )

        await handle_human_review_completed(
            store, application_id=app_id,
            reviewer_id="LO-Sarah-Chen",
            decision="APPROVE",
            override=True,
        )

        loan_events = await store.load_stream(f"loan-{app_id}")
        event_types = [_etype(e) for e in loan_events]

        for expected in ("DecisionGenerated", "HumanReviewRequested",
                         "HumanReviewCompleted", "ApplicationApproved"):
            assert expected in event_types, (
                f"Expected {expected} in loan stream. Got: {event_types}"
            )

        idx_decision = event_types.index("DecisionGenerated")
        idx_review = event_types.index("HumanReviewCompleted")
        idx_approved = event_types.index("ApplicationApproved")
        assert idx_decision < idx_review < idx_approved, (
            f"Expected DecisionGenerated < HumanReviewCompleted < ApplicationApproved. "
            f"Indices: {idx_decision}, {idx_review}, {idx_approved}"
        )

        decision_event = next(e for e in loan_events if _etype(e) == "DecisionGenerated")
        assert _payload(decision_event).get("recommendation") == "DECLINE", (
            f"Expected recommendation=DECLINE, got: {_payload(decision_event).get('recommendation')}"
        )

        review_event = next(e for e in loan_events if _etype(e) == "HumanReviewCompleted")
        review_payload = _payload(review_event)
        assert review_payload.get("override") is True, (
            f"Expected override=True, got: {review_payload.get('override')}"
        )
        assert review_payload.get("reviewer_id") == "LO-Sarah-Chen", (
            f"Expected reviewer_id='LO-Sarah-Chen', got: {review_payload.get('reviewer_id')}"
        )
