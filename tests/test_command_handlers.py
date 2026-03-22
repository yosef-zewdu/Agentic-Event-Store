"""
Phase 2 — Command handler tests (Reqs 9.1–9.4)

Covers:
  - handle_submit_application
  - handle_credit_analysis_completed
  - handle_fraud_screening_completed
  - handle_compliance_check
  - handle_generate_decision
  - handle_human_review_completed
  - handle_start_agent_session
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.commands.handlers import (
    handle_compliance_check,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_generate_decision,
    handle_human_review_completed,
    handle_start_agent_session,
    handle_submit_application,
)
from src.models.exceptions import DomainError, OptimisticConcurrencyError


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _submit(store, app_id: str, amount: float = 100_000.0) -> int:
    return await handle_submit_application(
        store,
        application_id=app_id,
        applicant_id=f"applicant-{app_id}",
        requested_amount_usd=amount,
    )


async def _full_setup_to_compliance_complete(store, app_id: str) -> None:
    """Drive an application through to ComplianceCheckCompleted."""
    await _submit(store, app_id)

    session_id = await handle_start_agent_session(
        store,
        agent_id="agent-credit",
        agent_type="credit_analysis",
        context_source="event_replay",
        event_replay_from_position=0,
        context_token_count=1000,
        model_version="gpt-4o",
    )

    # Manually append CreditAnalysisRequested to advance state
    from src.models.events import CreditAnalysisRequested
    from src.event_store import EventStore
    stream_version = await store.stream_version(f"loan-{app_id}")
    await store.append(
        f"loan-{app_id}",
        [CreditAnalysisRequested(application_id=app_id, requested_at=_now())],
        expected_version=stream_version,
    )

    await handle_credit_analysis_completed(
        store,
        application_id=app_id,
        agent_id="agent-credit",
        session_id=session_id,
        risk_tier="LOW",
        recommended_limit_usd=90_000.0,
        model_version="gpt-4o",
        confidence_score=0.85,
        regulatory_basis="Basel III",
        duration_ms=200,
        input_data={"source": "test"},
    )

    # FraudScreeningRequested
    from src.models.events import FraudScreeningRequested
    stream_version = await store.stream_version(f"loan-{app_id}")
    await store.append(
        f"loan-{app_id}",
        [FraudScreeningRequested(application_id=app_id, requested_at=_now())],
        expected_version=stream_version,
    )

    await handle_fraud_screening_completed(
        store,
        application_id=app_id,
        agent_id="agent-fraud",
        session_id=session_id,
        fraud_score=0.05,
        screening_model="fraud-v2",
        duration_ms=100,
        flags=[],
    )

    # ComplianceCheckRequested
    from src.models.events import ComplianceCheckRequested
    stream_version = await store.stream_version(f"loan-{app_id}")
    await store.append(
        f"loan-{app_id}",
        [ComplianceCheckRequested(application_id=app_id, requested_at=_now())],
        expected_version=stream_version,
    )

    await handle_compliance_check(
        store,
        application_id=app_id,
        rule_verdicts=[
            {"rule_id": "AML-001", "rule_version": "1.0", "passed": True, "evidence_hash": "h1"},
            {"rule_id": "KYC-001", "rule_version": "1.0", "passed": True, "evidence_hash": "h2"},
        ],
        regulation_set_version="2024-Q1",
    )


# ===========================================================================
# handle_submit_application
# ===========================================================================

class TestHandleSubmitApplication:

    @pytest.mark.asyncio
    async def test_creates_stream_at_version_1(self, store):
        v = await _submit(store, "cmd-sub-001")
        assert v == 1

    @pytest.mark.asyncio
    async def test_event_persisted_with_correct_type(self, store):
        await _submit(store, "cmd-sub-002")
        events = await store.load_stream("loan-cmd-sub-002")
        assert len(events) == 1
        assert events[0].event_type == "ApplicationSubmitted"

    @pytest.mark.asyncio
    async def test_payload_is_self_contained(self, store):
        """Req 9.4 — payload must include all fields without joining other streams."""
        await _submit(store, "cmd-sub-003", amount=75_000.0)
        events = await store.load_stream("loan-cmd-sub-003")
        payload = events[0].payload
        assert "application_id" in payload
        assert "applicant_id" in payload
        assert "requested_amount_usd" in payload

    @pytest.mark.asyncio
    async def test_duplicate_submit_raises_concurrency_error(self, store):
        await _submit(store, "cmd-sub-004")
        with pytest.raises(OptimisticConcurrencyError):
            await _submit(store, "cmd-sub-004")


# ===========================================================================
# handle_credit_analysis_completed
# ===========================================================================

class TestHandleCreditAnalysisCompleted:

    @pytest.mark.asyncio
    async def test_appends_credit_analysis_event(self, store):
        app_id = "cmd-credit-001"
        await _submit(store, app_id)

        from src.models.events import CreditAnalysisRequested
        v = await store.stream_version(f"loan-{app_id}")
        await store.append(
            f"loan-{app_id}",
            [CreditAnalysisRequested(application_id=app_id, requested_at=_now())],
            expected_version=v,
        )

        sid = await handle_start_agent_session(
            store, agent_id="a1", agent_type="credit_analysis",
            context_source="replay", event_replay_from_position=0,
            context_token_count=500, model_version="gpt-4o",
        )

        new_v = await handle_credit_analysis_completed(
            store,
            application_id=app_id,
            agent_id="a1",
            session_id=sid,
            risk_tier="MEDIUM",
            recommended_limit_usd=80_000.0,
            model_version="gpt-4o",
            confidence_score=0.78,
            regulatory_basis="Basel III",
            duration_ms=150,
            input_data={},
        )
        assert new_v > 1

        events = await store.load_stream(f"loan-{app_id}")
        types = [e.event_type for e in events]
        assert "CreditAnalysisCompleted" in types

    @pytest.mark.asyncio
    async def test_duplicate_credit_analysis_raises(self, store):
        app_id = "cmd-credit-002"
        await _submit(store, app_id)

        from src.models.events import CreditAnalysisRequested
        v = await store.stream_version(f"loan-{app_id}")
        await store.append(
            f"loan-{app_id}",
            [CreditAnalysisRequested(application_id=app_id, requested_at=_now())],
            expected_version=v,
        )

        sid = await handle_start_agent_session(
            store, agent_id="a1", agent_type="credit_analysis",
            context_source="replay", event_replay_from_position=0,
            context_token_count=500, model_version="gpt-4o",
        )

        await handle_credit_analysis_completed(
            store, application_id=app_id, agent_id="a1", session_id=sid,
            risk_tier="LOW", recommended_limit_usd=90_000.0, model_version="gpt-4o",
            confidence_score=0.9, regulatory_basis=None, duration_ms=100, input_data={},
        )

        # Second call must raise (Req 8.1) — either duplicate check or invalid transition
        with pytest.raises(DomainError):
            await handle_credit_analysis_completed(
                store, application_id=app_id, agent_id="a1", session_id=sid,
                risk_tier="LOW", recommended_limit_usd=90_000.0, model_version="gpt-4o",
                confidence_score=0.9, regulatory_basis=None, duration_ms=100, input_data={},
            )

    @pytest.mark.asyncio
    async def test_credit_analysis_payload_self_contained(self, store):
        """Req 9.4 — CreditAnalysisCompleted must include application_id, agent_id, session_id."""
        app_id = "cmd-credit-003"
        await _submit(store, app_id)

        from src.models.events import CreditAnalysisRequested
        v = await store.stream_version(f"loan-{app_id}")
        await store.append(
            f"loan-{app_id}",
            [CreditAnalysisRequested(application_id=app_id, requested_at=_now())],
            expected_version=v,
        )

        sid = await handle_start_agent_session(
            store, agent_id="a1", agent_type="credit_analysis",
            context_source="replay", event_replay_from_position=0,
            context_token_count=500, model_version="gpt-4o",
        )

        await handle_credit_analysis_completed(
            store, application_id=app_id, agent_id="a1", session_id=sid,
            risk_tier="LOW", recommended_limit_usd=90_000.0, model_version="gpt-4o",
            confidence_score=0.9, regulatory_basis=None, duration_ms=100, input_data={},
        )

        events = await store.load_stream(f"loan-{app_id}")
        credit_event = next(e for e in events if e.event_type == "CreditAnalysisCompleted")
        assert "application_id" in credit_event.payload
        assert "session_id" in credit_event.payload


# ===========================================================================
# handle_fraud_screening_completed
# ===========================================================================

class TestHandleFraudScreeningCompleted:

    @pytest.mark.asyncio
    async def test_invalid_fraud_score_raises(self, store):
        """Req 15.9 — fraud_score must be in [0.0, 1.0]."""
        app_id = "cmd-fraud-001"
        await _submit(store, app_id)

        with pytest.raises(DomainError, match="fraud_score"):
            await handle_fraud_screening_completed(
                store, application_id=app_id, agent_id="a1", session_id="s1",
                fraud_score=1.5, screening_model="v1", duration_ms=50, flags=[],
            )

    @pytest.mark.asyncio
    async def test_negative_fraud_score_raises(self, store):
        app_id = "cmd-fraud-002"
        await _submit(store, app_id)

        with pytest.raises(DomainError, match="fraud_score"):
            await handle_fraud_screening_completed(
                store, application_id=app_id, agent_id="a1", session_id="s1",
                fraud_score=-0.1, screening_model="v1", duration_ms=50, flags=[],
            )

    @pytest.mark.asyncio
    async def test_valid_fraud_score_boundary_values(self, store):
        """0.0 and 1.0 are valid boundary values."""
        for app_id, score in [("cmd-fraud-003", 0.0), ("cmd-fraud-004", 1.0)]:
            await _submit(store, app_id)

            from src.models.events import CreditAnalysisRequested, CreditAnalysisCompleted, FraudScreeningRequested
            from src.models.events import CreditDecision, RiskTier
            from decimal import Decimal

            v = await store.stream_version(f"loan-{app_id}")
            await store.append(f"loan-{app_id}", [CreditAnalysisRequested(application_id=app_id, requested_at=_now())], expected_version=v)

            sid = await handle_start_agent_session(
                store, agent_id="a1", agent_type="credit_analysis",
                context_source="replay", event_replay_from_position=0,
                context_token_count=500, model_version="gpt-4o",
            )

            await handle_credit_analysis_completed(
                store, application_id=app_id, agent_id="a1", session_id=sid,
                risk_tier="LOW", recommended_limit_usd=90_000.0, model_version="gpt-4o",
                confidence_score=0.9, regulatory_basis=None, duration_ms=100, input_data={},
            )

            v = await store.stream_version(f"loan-{app_id}")
            await store.append(f"loan-{app_id}", [FraudScreeningRequested(application_id=app_id, requested_at=_now())], expected_version=v)

            # Should not raise
            await handle_fraud_screening_completed(
                store, application_id=app_id, agent_id="a1", session_id=sid,
                fraud_score=score, screening_model="v1", duration_ms=50, flags=[],
            )


# ===========================================================================
# handle_compliance_check
# ===========================================================================

class TestHandleComplianceCheck:

    @pytest.mark.asyncio
    async def test_compliance_events_written_to_compliance_stream(self, store):
        app_id = "cmd-comp-001"
        await _submit(store, app_id)

        from src.models.events import CreditAnalysisRequested, FraudScreeningRequested, ComplianceCheckRequested
        v = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [CreditAnalysisRequested(application_id=app_id, requested_at=_now())], expected_version=v)

        sid = await handle_start_agent_session(
            store, agent_id="a1", agent_type="credit_analysis",
            context_source="replay", event_replay_from_position=0,
            context_token_count=500, model_version="gpt-4o",
        )

        await handle_credit_analysis_completed(
            store, application_id=app_id, agent_id="a1", session_id=sid,
            risk_tier="LOW", recommended_limit_usd=90_000.0, model_version="gpt-4o",
            confidence_score=0.9, regulatory_basis=None, duration_ms=100, input_data={},
        )

        v = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [FraudScreeningRequested(application_id=app_id, requested_at=_now())], expected_version=v)

        await handle_fraud_screening_completed(
            store, application_id=app_id, agent_id="a1", session_id=sid,
            fraud_score=0.1, screening_model="v1", duration_ms=50, flags=[],
        )

        v = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [ComplianceCheckRequested(application_id=app_id, requested_at=_now())], expected_version=v)

        await handle_compliance_check(
            store,
            application_id=app_id,
            rule_verdicts=[
                {"rule_id": "AML-001", "rule_version": "1.0", "passed": True, "evidence_hash": "h1"},
            ],
            regulation_set_version="2024-Q1",
        )

        compliance_events = await store.load_stream(f"compliance-{app_id}")
        types = [e.event_type for e in compliance_events]
        assert "ComplianceRulePassed" in types
        assert "ComplianceCheckCompleted" in types

    @pytest.mark.asyncio
    async def test_failed_rule_written_to_compliance_stream(self, store):
        app_id = "cmd-comp-002"
        await _submit(store, app_id)

        from src.models.events import CreditAnalysisRequested, FraudScreeningRequested, ComplianceCheckRequested

        v = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [CreditAnalysisRequested(application_id=app_id, requested_at=_now())], expected_version=v)

        sid = await handle_start_agent_session(
            store, agent_id="a1", agent_type="credit_analysis",
            context_source="replay", event_replay_from_position=0,
            context_token_count=500, model_version="gpt-4o",
        )

        await handle_credit_analysis_completed(
            store, application_id=app_id, agent_id="a1", session_id=sid,
            risk_tier="LOW", recommended_limit_usd=90_000.0, model_version="gpt-4o",
            confidence_score=0.9, regulatory_basis=None, duration_ms=100, input_data={},
        )

        v = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [FraudScreeningRequested(application_id=app_id, requested_at=_now())], expected_version=v)

        await handle_fraud_screening_completed(
            store, application_id=app_id, agent_id="a1", session_id=sid,
            fraud_score=0.1, screening_model="v1", duration_ms=50, flags=[],
        )

        v = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [ComplianceCheckRequested(application_id=app_id, requested_at=_now())], expected_version=v)

        await handle_compliance_check(
            store,
            application_id=app_id,
            rule_verdicts=[
                {"rule_id": "KYC-001", "rule_version": "1.0", "passed": False,
                 "evidence_hash": "h1", "failure_reason": "Identity mismatch"},
            ],
            regulation_set_version="2024-Q1",
        )

        compliance_events = await store.load_stream(f"compliance-{app_id}")
        types = [e.event_type for e in compliance_events]
        assert "ComplianceRuleFailed" in types


# ===========================================================================
# handle_generate_decision
# ===========================================================================

class TestHandleGenerateDecision:

    @pytest.mark.asyncio
    async def test_confidence_below_floor_requires_refer(self, store):
        """Req 8.2 — confidence < 0.6 must produce REFER."""
        app_id = "cmd-dec-001"
        await _full_setup_to_compliance_complete(store, app_id)

        with pytest.raises(DomainError, match="REFER"):
            await handle_generate_decision(
                store,
                application_id=app_id,
                agent_id="a1",
                session_id="s1",
                recommendation="APPROVE",
                confidence_score=0.45,
                model_versions={"credit": "gpt-4o"},
                contributing_agent_sessions=[],
            )

    @pytest.mark.asyncio
    async def test_confidence_below_floor_with_refer_passes(self, store):
        app_id = "cmd-dec-002"
        await _full_setup_to_compliance_complete(store, app_id)

        v = await handle_generate_decision(
            store,
            application_id=app_id,
            agent_id="a1",
            session_id="s1",
            recommendation="REFER",
            confidence_score=0.45,
            model_versions={"credit": "gpt-4o"},
            contributing_agent_sessions=[],
        )
        assert v > 0

    @pytest.mark.asyncio
    async def test_compliance_not_cleared_blocks_approve(self, store):
        """Req 8.3 / 9.3 — compliance must be cleared before APPROVE."""
        app_id = "cmd-dec-003"
        await _submit(store, app_id)

        from src.models.events import (
            CreditAnalysisRequested, FraudScreeningRequested,
            ComplianceCheckRequested,
        )

        v = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [CreditAnalysisRequested(application_id=app_id, requested_at=_now())], expected_version=v)

        sid = await handle_start_agent_session(
            store, agent_id="a1", agent_type="credit_analysis",
            context_source="replay", event_replay_from_position=0,
            context_token_count=500, model_version="gpt-4o",
        )

        await handle_credit_analysis_completed(
            store, application_id=app_id, agent_id="a1", session_id=sid,
            risk_tier="LOW", recommended_limit_usd=90_000.0, model_version="gpt-4o",
            confidence_score=0.9, regulatory_basis=None, duration_ms=100, input_data={},
        )

        v = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [FraudScreeningRequested(application_id=app_id, requested_at=_now())], expected_version=v)

        await handle_fraud_screening_completed(
            store, application_id=app_id, agent_id="a1", session_id=sid,
            fraud_score=0.1, screening_model="v1", duration_ms=50, flags=[],
        )

        v = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [ComplianceCheckRequested(application_id=app_id, requested_at=_now())], expected_version=v)

        # Compliance with a failing rule
        await handle_compliance_check(
            store,
            application_id=app_id,
            rule_verdicts=[
                {"rule_id": "KYC-001", "rule_version": "1.0", "passed": False,
                 "evidence_hash": "h1", "failure_reason": "fail"},
            ],
            regulation_set_version="2024-Q1",
        )

        with pytest.raises(DomainError):
            await handle_generate_decision(
                store,
                application_id=app_id,
                agent_id="a1",
                session_id=sid,
                recommendation="APPROVE",
                confidence_score=0.9,
                model_versions={"credit": "gpt-4o"},
                contributing_agent_sessions=[],
            )

    @pytest.mark.asyncio
    async def test_decision_event_persisted(self, store):
        app_id = "cmd-dec-004"
        await _full_setup_to_compliance_complete(store, app_id)

        await handle_generate_decision(
            store,
            application_id=app_id,
            agent_id="a1",
            session_id="s1",
            recommendation="APPROVE",
            confidence_score=0.9,
            model_versions={"credit": "gpt-4o"},
            contributing_agent_sessions=[],
        )

        events = await store.load_stream(f"loan-{app_id}")
        types = [e.event_type for e in events]
        assert "DecisionGenerated" in types


# ===========================================================================
# handle_human_review_completed
# ===========================================================================

class TestHandleHumanReviewCompleted:

    async def _setup_pending_review(self, store, app_id: str) -> None:
        await _full_setup_to_compliance_complete(store, app_id)
        await handle_generate_decision(
            store,
            application_id=app_id,
            agent_id="a1",
            session_id="s1",
            recommendation="REFER",
            confidence_score=0.5,
            model_versions={},
            contributing_agent_sessions=[],
        )
        # Manually append HumanReviewRequested to advance state
        from src.models.events import HumanReviewRequested
        v = await store.stream_version(f"loan-{app_id}")
        await store.append(
            f"loan-{app_id}",
            [HumanReviewRequested(
                application_id=app_id,
                reason="Low confidence",
                decision_event_id="evt-1",
                requested_at=_now(),
            )],
            expected_version=v,
        )

    @pytest.mark.asyncio
    async def test_approve_decision_produces_approved_state(self, store):
        app_id = "cmd-review-001"
        await self._setup_pending_review(store, app_id)

        await handle_human_review_completed(
            store,
            application_id=app_id,
            reviewer_id="reviewer-1",
            decision="APPROVE",
            override=True,
        )

        events = await store.load_stream(f"loan-{app_id}")
        types = [e.event_type for e in events]
        assert "HumanReviewCompleted" in types
        assert "ApplicationApproved" in types

    @pytest.mark.asyncio
    async def test_decline_decision_produces_declined_state(self, store):
        app_id = "cmd-review-002"
        await self._setup_pending_review(store, app_id)

        await handle_human_review_completed(
            store,
            application_id=app_id,
            reviewer_id="reviewer-1",
            decision="DECLINE",
            override=False,
        )

        events = await store.load_stream(f"loan-{app_id}")
        types = [e.event_type for e in events]
        assert "HumanReviewCompleted" in types
        assert "ApplicationDeclined" in types


# ===========================================================================
# handle_start_agent_session
# ===========================================================================

class TestHandleStartAgentSession:

    @pytest.mark.asyncio
    async def test_returns_session_id(self, store):
        sid = await handle_start_agent_session(
            store,
            agent_id="agent-1",
            agent_type="credit_analysis",
            context_source="event_replay",
            event_replay_from_position=0,
            context_token_count=1000,
            model_version="gpt-4o",
        )
        assert isinstance(sid, str)
        assert len(sid) > 0

    @pytest.mark.asyncio
    async def test_gas_town_ordering_started_then_context(self, store):
        """Req 15.4 — AgentSessionStarted then AgentContextLoaded as first two events."""
        sid = await handle_start_agent_session(
            store,
            agent_id="agent-1",
            agent_type="credit_analysis",
            context_source="event_replay",
            event_replay_from_position=0,
            context_token_count=1000,
            model_version="gpt-4o",
        )

        events = await store.load_stream(f"session-{sid}")
        assert len(events) == 2
        assert events[0].event_type == "AgentSessionStarted"
        # Second event is AgentContextLoaded (AgentInputValidated alias)
        assert events[1].event_type in ("AgentContextLoaded", "AgentInputValidated")

    @pytest.mark.asyncio
    async def test_explicit_session_id_used(self, store):
        sid = await handle_start_agent_session(
            store,
            agent_id="agent-1",
            agent_type="credit_analysis",
            context_source="event_replay",
            event_replay_from_position=0,
            context_token_count=1000,
            model_version="gpt-4o",
            session_id="explicit-session-id",
        )
        assert sid == "explicit-session-id"
        events = await store.load_stream("session-explicit-session-id")
        assert len(events) == 2

    @pytest.mark.asyncio
    async def test_session_stream_version_is_2(self, store):
        sid = await handle_start_agent_session(
            store,
            agent_id="agent-1",
            agent_type="credit_analysis",
            context_source="event_replay",
            event_replay_from_position=0,
            context_token_count=1000,
            model_version="gpt-4o",
        )
        v = await store.stream_version(f"session-{sid}")
        assert v == 2

    @pytest.mark.asyncio
    async def test_session_payload_contains_agent_fields(self, store):
        """Req 9.4 — session events must be self-contained."""
        sid = await handle_start_agent_session(
            store,
            agent_id="agent-42",
            agent_type="fraud_detection",
            context_source="snapshot",
            event_replay_from_position=10,
            context_token_count=2048,
            model_version="gpt-4o-mini",
        )

        events = await store.load_stream(f"session-{sid}")
        started_payload = events[0].payload
        assert started_payload.get("agent_id") == "agent-42"
        assert started_payload.get("session_id") == sid
