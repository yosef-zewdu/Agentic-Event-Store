"""
Phase 2 — Aggregate tests (Reqs 6.1–6.5, 7.1–7.3, 7.6–7.7, 8.1–8.5)

Covers:
  - LoanApplicationAggregate: state machine, business rules
  - AgentSessionAggregate: Gas Town ordering, model version consistency
  - ComplianceRecordAggregate: rule tracking, compliance cleared check
  - AuditLedgerAggregate: append-only enforcement
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from src.aggregates.audit_ledger import AuditLedgerAggregate
from src.aggregates.agent_session import AgentSessionAggregate, SessionState
from src.aggregates.compliance_record import ComplianceRecordAggregate, ComplianceState
from src.aggregates.loan_application import (
    ApplicationState,
    LoanApplicationAggregate,
    TERMINAL_STATES,
)
from src.models.exceptions import DomainError


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# Minimal fake StoredEvent for unit tests (no DB needed)
# ---------------------------------------------------------------------------

@dataclass
class _FakeEvent:
    event_type: str
    payload: dict


def _e(event_type: str, **payload) -> _FakeEvent:
    return _FakeEvent(event_type=event_type, payload=payload)


# ===========================================================================
# LoanApplicationAggregate
# ===========================================================================

class TestLoanApplicationStateMachine:
    """State machine transitions (Reqs 6.1–6.2)."""

    def _base_agg(self) -> LoanApplicationAggregate:
        agg = LoanApplicationAggregate(application_id="app-001")
        agg._apply(_e("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=100_000))
        return agg

    def test_initial_state_is_new(self):
        agg = LoanApplicationAggregate(application_id="app-000")
        assert agg.state == ApplicationState.NEW

    def test_submitted_transitions_to_submitted(self):
        agg = LoanApplicationAggregate(application_id="app-001")
        agg._apply(_e("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=50_000))
        assert agg.state == ApplicationState.SUBMITTED

    def test_valid_transition_does_not_raise(self):
        agg = self._base_agg()
        # SUBMITTED → CREDIT_ANALYSIS_REQUESTED is valid
        agg.assert_valid_transition(ApplicationState.CREDIT_ANALYSIS_REQUESTED)

    def test_invalid_transition_raises_domain_error(self):
        agg = self._base_agg()
        # SUBMITTED → APPROVED is not valid
        with pytest.raises(DomainError):
            agg.assert_valid_transition(ApplicationState.APPROVED)

    def test_terminal_state_blocks_all_transitions(self):
        agg = self._base_agg()
        agg._apply(_e("ApplicationWithdrawn"))
        assert agg.state == ApplicationState.WITHDRAWN
        with pytest.raises(DomainError):
            agg.assert_valid_transition(ApplicationState.CREDIT_ANALYSIS_REQUESTED)

    def test_all_terminal_states_block_transitions(self):
        for terminal in TERMINAL_STATES:
            agg = LoanApplicationAggregate(application_id="app-t")
            agg.state = terminal
            with pytest.raises(DomainError):
                agg.assert_valid_transition(ApplicationState.SUBMITTED)

    def test_full_happy_path_state_sequence(self):
        agg = LoanApplicationAggregate(application_id="app-hp")
        events = [
            _e("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=200_000),
            _e("CreditAnalysisRequested"),
            _e("CreditAnalysisCompleted", session_id="s1"),
            _e("FraudScreeningRequested"),
            _e("FraudScreeningCompleted", fraud_score=0.1),
            _e("ComplianceCheckRequested"),
            _e("ComplianceCheckCompleted"),
            _e("DecisionGenerated", recommendation="APPROVE", confidence_score=0.9,
               contributing_agent_sessions=[]),
            _e("ApplicationApproved"),
        ]
        for ev in events:
            agg._apply(ev)
        assert agg.state == ApplicationState.APPROVED

    def test_version_increments_per_event(self):
        agg = LoanApplicationAggregate(application_id="app-v")
        assert agg.version == 0
        agg._apply(_e("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=10_000))
        assert agg.version == 1
        agg._apply(_e("CreditAnalysisRequested"))
        assert agg.version == 2


class TestLoanApplicationBusinessRules:
    """Business rules (Reqs 8.1–8.5)."""

    def _agg_with_credit_done(self) -> LoanApplicationAggregate:
        agg = LoanApplicationAggregate(application_id="app-br")
        agg._apply(_e("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=100_000))
        agg._apply(_e("CreditAnalysisRequested"))
        agg._apply(_e("CreditAnalysisCompleted", session_id="s1"))
        return agg

    def test_duplicate_credit_analysis_raises(self):
        """Req 8.1 — second CreditAnalysisCompleted must raise."""
        agg = self._agg_with_credit_done()
        with pytest.raises(DomainError, match="already recorded"):
            agg.assert_no_duplicate_credit_analysis()

    def test_no_duplicate_credit_analysis_passes_when_not_done(self):
        agg = LoanApplicationAggregate(application_id="app-nd")
        agg._apply(_e("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=50_000))
        agg.assert_no_duplicate_credit_analysis()  # should not raise

    def test_confidence_floor_below_threshold_requires_refer(self):
        """Req 8.2 — confidence < 0.6 must produce REFER."""
        agg = LoanApplicationAggregate(application_id="app-cf")
        with pytest.raises(DomainError, match="REFER"):
            agg.assert_confidence_floor(0.55, "APPROVE")

    def test_confidence_floor_below_threshold_with_refer_passes(self):
        agg = LoanApplicationAggregate(application_id="app-cf2")
        agg.assert_confidence_floor(0.55, "REFER")  # should not raise

    def test_confidence_floor_above_threshold_any_recommendation_passes(self):
        agg = LoanApplicationAggregate(application_id="app-cf3")
        agg.assert_confidence_floor(0.75, "APPROVE")  # should not raise
        agg.assert_confidence_floor(0.75, "DECLINE")  # should not raise

    def test_confidence_floor_none_passes(self):
        agg = LoanApplicationAggregate(application_id="app-cf4")
        agg.assert_confidence_floor(None, "APPROVE")  # should not raise

    def test_approved_amount_cap_exceeds_requested_raises(self):
        """Req 8.5 — approved_amount > requested_amount must raise."""
        agg = LoanApplicationAggregate(application_id="app-cap")
        agg._apply(_e("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=100_000))
        with pytest.raises(DomainError, match="exceeds"):
            agg.assert_approved_amount_cap(150_000)

    def test_approved_amount_cap_equal_to_requested_passes(self):
        agg = LoanApplicationAggregate(application_id="app-cap2")
        agg._apply(_e("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=100_000))
        agg.assert_approved_amount_cap(100_000)  # should not raise

    def test_approved_amount_cap_below_requested_passes(self):
        agg = LoanApplicationAggregate(application_id="app-cap3")
        agg._apply(_e("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=100_000))
        agg.assert_approved_amount_cap(80_000)  # should not raise

    def test_causal_chain_unknown_session_raises(self):
        """Req 8.4 — contributing sessions must be known."""
        agg = LoanApplicationAggregate(application_id="app-cc")
        with pytest.raises(DomainError, match="Unknown"):
            agg.assert_causal_chain(["unknown-session-id"])

    def test_causal_chain_known_session_passes(self):
        agg = LoanApplicationAggregate(application_id="app-cc2")
        agg._apply(_e("AgentSessionStarted", session_id="known-session"))
        agg.assert_causal_chain(["known-session"])  # should not raise

    def test_causal_chain_empty_list_passes(self):
        agg = LoanApplicationAggregate(application_id="app-cc3")
        agg.assert_causal_chain([])  # should not raise

    def test_credit_analysis_superseded_resets_flag(self):
        agg = LoanApplicationAggregate(application_id="app-sup")
        agg._apply(_e("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=50_000))
        agg._apply(_e("CreditAnalysisRequested"))
        agg._apply(_e("CreditAnalysisCompleted", session_id="s1"))
        assert agg.credit_analysis_completed is True
        agg._apply(_e("CreditAnalysisSuperseded"))
        assert agg.credit_analysis_completed is False


class TestLoanApplicationLoad:
    """load() classmethod replays stream from event store (Req 6.3)."""

    @pytest.mark.asyncio
    async def test_load_empty_stream_returns_new_state(self, store):
        agg = await LoanApplicationAggregate.load(store, "app-load-001")
        assert agg.state == ApplicationState.NEW
        assert agg.version == 0

    @pytest.mark.asyncio
    async def test_load_replays_events_correctly(self, store):
        from src.models.events import ApplicationSubmitted, CreditAnalysisRequested

        app_id = "app-load-002"
        stream_id = f"loan-{app_id}"

        ev1 = ApplicationSubmitted(
            application_id=app_id,
            applicant_id="a1",
            requested_amount_usd=Decimal("75000"),
            loan_purpose="working_capital",
            loan_term_months=12,
            submission_channel="web",
            contact_email="test@example.com",
            contact_name="Test",
            submitted_at=_now(),
            application_reference="REF-001",
        )
        ev2 = CreditAnalysisRequested(application_id=app_id, requested_at=_now())

        await store.append(stream_id, [ev1, ev2], expected_version=-1)

        agg = await LoanApplicationAggregate.load(store, app_id)
        assert agg.state == ApplicationState.CREDIT_ANALYSIS_REQUESTED
        assert agg.version == 2
        assert agg.applicant_id == "a1"


# ===========================================================================
# AgentSessionAggregate
# ===========================================================================

class TestAgentSessionGasTownOrdering:
    """Gas Town ordering (Reqs 7.1, 7.6, 7.7)."""

    def _started_agg(self) -> AgentSessionAggregate:
        agg = AgentSessionAggregate(session_id="sess-001")
        agg._apply(_e("AgentSessionStarted", agent_id="agent-1", agent_type="credit_analysis"))
        return agg

    def _context_loaded_agg(self) -> AgentSessionAggregate:
        agg = self._started_agg()
        agg._apply(_e(
            "AgentContextLoaded",
            context_source="event_replay",
            event_replay_from_position=0,
            context_token_count=1500,
            model_version="gpt-4o-2024-11",
        ))
        return agg

    def test_initial_state_is_new(self):
        agg = AgentSessionAggregate(session_id="sess-000")
        assert agg.state == SessionState.NEW

    def test_session_started_transitions_to_started(self):
        agg = self._started_agg()
        assert agg.state == SessionState.STARTED

    def test_context_loaded_transitions_to_context_loaded(self):
        agg = self._context_loaded_agg()
        assert agg.state == SessionState.CONTEXT_LOADED

    def test_session_started_must_be_first_event(self):
        """AgentSessionStarted on non-NEW state raises DomainError (Req 7.1)."""
        agg = self._started_agg()
        with pytest.raises(DomainError):
            agg.assert_gas_town_ordering("AgentSessionStarted")

    def test_context_loaded_must_follow_started(self):
        """AgentContextLoaded on NEW state raises DomainError (Req 7.6)."""
        agg = AgentSessionAggregate(session_id="sess-002")
        with pytest.raises(DomainError):
            agg.assert_gas_town_ordering("AgentContextLoaded")

    def test_decision_event_before_context_loaded_raises(self):
        """Decision events before AgentContextLoaded raise DomainError (Req 7.7)."""
        agg = self._started_agg()
        for decision_event in [
            "CreditAnalysisCompleted",
            "FraudScreeningCompleted",
            "ComplianceCheckCompleted",
            "DecisionGenerated",
        ]:
            with pytest.raises(DomainError, match="Gas Town"):
                agg.assert_gas_town_ordering(decision_event)

    def test_decision_event_after_context_loaded_passes(self):
        agg = self._context_loaded_agg()
        agg.assert_gas_town_ordering("CreditAnalysisCompleted")  # should not raise

    def test_assert_not_closed_raises_on_closed_session(self):
        agg = self._context_loaded_agg()
        agg._apply(_e("AgentSessionClosed"))
        with pytest.raises(DomainError, match="closed"):
            agg.assert_not_closed()

    def test_assert_not_closed_passes_on_active_session(self):
        agg = self._context_loaded_agg()
        agg.assert_not_closed()  # should not raise


class TestAgentSessionContextFields:
    """Context fields recorded from AgentContextLoaded (Req 7.2)."""

    def test_context_fields_populated(self):
        agg = AgentSessionAggregate(session_id="sess-ctx")
        agg._apply(_e("AgentSessionStarted", agent_id="a1", agent_type="credit_analysis"))
        agg._apply(_e(
            "AgentContextLoaded",
            context_source="event_replay",
            event_replay_from_position=5,
            context_token_count=2048,
            model_version="gpt-4o-2024-11",
        ))
        assert agg.context_source == "event_replay"
        assert agg.event_replay_from_position == 5
        assert agg.context_token_count == 2048
        assert agg.model_version == "gpt-4o-2024-11"


class TestAgentSessionModelVersionConsistency:
    """Model version consistency on output events (Req 7.3)."""

    def _loaded_agg(self, model_version: str = "gpt-4o-2024-11") -> AgentSessionAggregate:
        agg = AgentSessionAggregate(session_id="sess-mv")
        agg._apply(_e("AgentSessionStarted", agent_id="a1", agent_type="credit_analysis"))
        agg._apply(_e(
            "AgentContextLoaded",
            context_source="event_replay",
            event_replay_from_position=0,
            context_token_count=1000,
            model_version=model_version,
        ))
        return agg

    def test_mismatched_model_version_raises(self):
        agg = self._loaded_agg("gpt-4o-2024-11")
        with pytest.raises(DomainError, match="mismatch"):
            agg._apply(_e("CreditAnalysisCompleted", model_version="gpt-3.5-turbo"))

    def test_matching_model_version_passes(self):
        agg = self._loaded_agg("gpt-4o-2024-11")
        agg._apply(_e("CreditAnalysisCompleted", model_version="gpt-4o-2024-11"))  # no raise

    def test_none_model_version_on_output_passes(self):
        agg = self._loaded_agg("gpt-4o-2024-11")
        agg._apply(_e("CreditAnalysisCompleted", model_version=None))  # no raise


class TestAgentSessionLoad:
    """load() classmethod replays session stream."""

    @pytest.mark.asyncio
    async def test_load_empty_stream_returns_new_state(self, store):
        agg = await AgentSessionAggregate.load(store, "sess-load-001")
        assert agg.state == SessionState.NEW
        assert agg.version == 0

    @pytest.mark.asyncio
    async def test_load_replays_gas_town_sequence(self, store):
        from src.models.events import AgentSessionStarted, AgentInputValidated

        sid = "sess-load-002"
        stream_id = f"session-{sid}"

        started = AgentSessionStarted(
            session_id=sid,
            agent_type="credit_analysis",
            agent_id="agent-1",
            application_id="app-001",
            model_version="gpt-4o",
            langgraph_graph_version="1.0",
            context_source="event_replay",
            context_token_count=1000,
            started_at=_now(),
        )
        context = AgentInputValidated(
            session_id=sid,
            agent_type="credit_analysis",
            application_id="app-001",
            inputs_validated=["financial_facts"],
            validation_duration_ms=50,
            validated_at=_now(),
        )

        await store.append(stream_id, [started, context], expected_version=-1)

        agg = await AgentSessionAggregate.load(store, sid)
        assert agg.version == 2


# ===========================================================================
# ComplianceRecordAggregate
# ===========================================================================

class TestComplianceRecordAggregate:
    """Compliance rule tracking and cleared check (Req 8.3)."""

    def _agg(self) -> ComplianceRecordAggregate:
        return ComplianceRecordAggregate(application_id="app-comp-001")

    def test_initial_state_is_new(self):
        agg = self._agg()
        assert agg.state == ComplianceState.NEW
        assert agg.all_rules_passed is False

    def test_check_requested_transitions_state(self):
        agg = self._agg()
        agg._apply(_e("ComplianceCheckRequested"))
        assert agg.state == ComplianceState.CHECK_REQUESTED

    def test_rule_passed_recorded(self):
        agg = self._agg()
        agg._apply(_e(
            "ComplianceRulePassed",
            rule_id="AML-001",
            rule_version="1.0",
            regulation_set_version="2024-Q1",
            evidence_hash="abc123",
        ))
        assert "AML-001" in agg.rule_verdicts
        assert agg.rule_verdicts["AML-001"].passed is True

    def test_rule_failed_sets_blocking_failure(self):
        agg = self._agg()
        agg._apply(_e(
            "ComplianceRuleFailed",
            rule_id="KYC-001",
            rule_version="1.0",
            regulation_set_version="2024-Q1",
            evidence_hash="def456",
            failure_reason="Identity not verified",
        ))
        assert agg.has_blocking_failure is True
        assert agg.rule_verdicts["KYC-001"].passed is False

    def test_all_rules_passed_when_all_pass(self):
        agg = self._agg()
        for rule_id in ["AML-001", "KYC-001", "OFAC-001"]:
            agg._apply(_e(
                "ComplianceRulePassed",
                rule_id=rule_id,
                rule_version="1.0",
                regulation_set_version="2024-Q1",
                evidence_hash="hash",
            ))
        agg._apply(_e("ComplianceCheckCompleted"))
        assert agg.all_rules_passed is True
        assert agg.state == ComplianceState.COMPLETED

    def test_blocked_state_when_rule_fails(self):
        agg = self._agg()
        agg._apply(_e(
            "ComplianceRuleFailed",
            rule_id="KYC-001",
            rule_version="1.0",
            regulation_set_version="2024-Q1",
            evidence_hash="hash",
            failure_reason="fail",
        ))
        agg._apply(_e("ComplianceCheckCompleted"))
        assert agg.state == ComplianceState.BLOCKED
        assert agg.all_rules_passed is False

    def test_assert_compliance_cleared_raises_when_blocked(self):
        """Req 8.3 — compliance not cleared must raise DomainError."""
        agg = self._agg()
        agg._apply(_e(
            "ComplianceRuleFailed",
            rule_id="KYC-001",
            rule_version="1.0",
            regulation_set_version="2024-Q1",
            evidence_hash="hash",
            failure_reason="fail",
        ))
        agg._apply(_e("ComplianceCheckCompleted"))
        with pytest.raises(DomainError, match="not cleared"):
            agg.assert_compliance_cleared()

    def test_assert_compliance_cleared_passes_when_all_pass(self):
        agg = self._agg()
        agg._apply(_e(
            "ComplianceRulePassed",
            rule_id="AML-001",
            rule_version="1.0",
            regulation_set_version="2024-Q1",
            evidence_hash="hash",
        ))
        agg._apply(_e("ComplianceCheckCompleted"))
        agg.assert_compliance_cleared()  # should not raise

    def test_assert_compliance_cleared_raises_when_not_completed(self):
        agg = self._agg()
        with pytest.raises(DomainError):
            agg.assert_compliance_cleared()

    def test_regulation_set_version_tracked(self):
        agg = self._agg()
        agg._apply(_e(
            "ComplianceRulePassed",
            rule_id="AML-001",
            rule_version="1.0",
            regulation_set_version="2024-Q2",
            evidence_hash="hash",
        ))
        assert agg.regulation_set_version == "2024-Q2"

    @pytest.mark.asyncio
    async def test_load_replays_compliance_stream(self, store):
        from src.models.events import ComplianceRulePassed, ComplianceCheckCompleted

        app_id = "app-comp-load"
        stream_id = f"compliance-{app_id}"

        rule_passed = ComplianceRulePassed(
            application_id=app_id,
            session_id="sess-1",
            rule_id="AML-001",
            rule_name="AML Check",
            rule_version="1.0",
            evidence_hash="abc",
            evaluation_notes="passed",
            evaluated_at=_now(),
        )
        completed = ComplianceCheckCompleted(
            application_id=app_id,
            session_id="sess-1",
            rules_evaluated=1,
            rules_passed=1,
            rules_failed=0,
            rules_noted=0,
            has_hard_block=False,
            overall_verdict="CLEAR",
            completed_at=_now(),
        )

        await store.append(stream_id, [rule_passed, completed], expected_version=-1)

        agg = await ComplianceRecordAggregate.load(store, app_id)
        assert agg.version == 2
        assert "AML-001" in agg.rule_verdicts


# ===========================================================================
# AuditLedgerAggregate
# ===========================================================================

class TestAuditLedgerAggregate:
    """Append-only enforcement and integrity check tracking (Req 14.6)."""

    def test_assert_append_only_always_raises(self):
        agg = AuditLedgerAggregate(entity_id="app-001", entity_type="application")
        with pytest.raises(DomainError, match="append-only"):
            agg.assert_append_only()

    def test_integrity_check_recorded(self):
        agg = AuditLedgerAggregate(entity_id="app-001", entity_type="application")
        agg._apply(_e(
            "AuditIntegrityCheckRun",
            check_timestamp=_now().isoformat(),
            integrity_hash="sha256-abc",
            previous_hash="",
            events_verified_count=5,
        ))
        assert agg.last_integrity_hash == "sha256-abc"
        assert agg.events_verified_count == 5
        assert len(agg.integrity_checks) == 1

    def test_multiple_integrity_checks_accumulated(self):
        agg = AuditLedgerAggregate(entity_id="app-001", entity_type="application")
        for i in range(3):
            agg._apply(_e(
                "AuditIntegrityCheckRun",
                check_timestamp=_now().isoformat(),
                integrity_hash=f"hash-{i}",
                previous_hash=f"hash-{i-1}" if i > 0 else "",
                events_verified_count=i + 1,
            ))
        assert len(agg.integrity_checks) == 3
        assert agg.last_integrity_hash == "hash-2"

    def test_version_increments_per_event(self):
        agg = AuditLedgerAggregate(entity_id="app-001", entity_type="application")
        assert agg.version == 0
        agg._apply(_e(
            "AuditIntegrityCheckRun",
            check_timestamp=_now().isoformat(),
            integrity_hash="h1",
            previous_hash="",
            events_verified_count=1,
        ))
        assert agg.version == 1

    def test_stream_id_format(self):
        agg = AuditLedgerAggregate(entity_id="app-001", entity_type="application")
        assert agg.stream_id == "audit-application-app-001"

    @pytest.mark.asyncio
    async def test_load_replays_audit_stream(self, store):
        from src.models.events import AuditIntegrityCheckRun

        entity_id = "app-audit-load"
        stream_id = f"audit-application-{entity_id}"

        check = AuditIntegrityCheckRun(
            entity_id=entity_id,
            entity_type="application",
            check_timestamp=_now(),
            events_verified_count=3,
            integrity_hash="sha256-xyz",
            previous_hash="",
            chain_valid=True,
            tamper_detected=False,
        )

        await store.append(stream_id, [check], expected_version=-1)

        agg = await AuditLedgerAggregate.load(store, entity_id, "application")
        assert agg.version == 1
        assert agg.last_integrity_hash == "sha256-xyz"
