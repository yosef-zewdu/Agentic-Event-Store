"""
Command handlers — load → validate → produce events → append.

Each handler follows the pattern:
  1. Load aggregate(s) from the event store
  2. Validate business rules against current state
  3. Produce domain event(s)
  4. Append to the event store

Compliance dependency check (Req 8.3) is done at the application service layer
here — aggregates never load other aggregates' streams directly.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.aggregates.loan_application import ApplicationState, LoanApplicationAggregate
from src.models.events import (
    AgentContextLoaded,
    AgentSessionStarted,
    ApplicationApproved,
    ApplicationDeclined,
    ApplicationSubmitted,
    ApplicationWithdrawn,
    ComplianceCheckCompleted,
    ComplianceCheckRequested,
    ComplianceRuleFailed,
    ComplianceRulePassed,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    DecisionGenerated,
    FraudScreeningCompleted,
    FraudScreeningRequested,
    HumanReviewCompleted,
    HumanReviewRequested,
)
from src.models.exceptions import DomainError


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Application lifecycle handlers
# ---------------------------------------------------------------------------

async def handle_submit_application(
    store,
    application_id: str,
    applicant_id: str,
    requested_amount_usd: float,
    loan_purpose: str = "working_capital",
    loan_term_months: int = 12,
    submission_channel: str = "web",
    contact_email: str = "",
    contact_name: str = "",
    application_reference: str = "",
    correlation_id: str | None = None,
) -> int:
    """Submit a new loan application (Req 9.2)."""
    event = ApplicationSubmitted(
        application_id=application_id,
        applicant_id=applicant_id,
        requested_amount_usd=requested_amount_usd,
        loan_purpose=loan_purpose,
        loan_term_months=loan_term_months,
        submission_channel=submission_channel,
        contact_email=contact_email,
        contact_name=contact_name,
        application_reference=application_reference or application_id,
        submitted_at=_now(),
    )
    return await store.append(
        stream_id=f"loan-{application_id}",
        events=[event],
        expected_version=-1,
        aggregate_type="loan_application",
        correlation_id=correlation_id,
    )


async def handle_credit_analysis_completed(
    store,
    application_id: str,
    agent_id: str,
    session_id: str,
    risk_tier: str,
    recommended_limit_usd: float,
    model_version: str,
    confidence_score: float | None,
    regulatory_basis: str | None,
    duration_ms: int,
    input_data: dict,
    model_deployment_id: str = "default",
    correlation_id: str | None = None,
) -> int:
    """Record a completed credit analysis (Req 9.2)."""
    import hashlib, json
    from src.models.events import CreditDecision, RiskTier
    from decimal import Decimal

    agg = await LoanApplicationAggregate.load(store, application_id)

    # Business rules
    agg.assert_valid_transition(ApplicationState.CREDIT_ANALYSIS_COMPLETE)
    agg.assert_no_duplicate_credit_analysis()

    # Agent session guards — Gas Town ordering (Req 7.1, 7.3)
    session_agg = await AgentSessionAggregate.load(store, session_id)
    session_agg.assert_gas_town_ordering("CreditAnalysisCompleted")
    session_agg.assert_not_closed()

    decision = CreditDecision(
        risk_tier=RiskTier(risk_tier),
        recommended_limit_usd=Decimal(str(recommended_limit_usd)),
        confidence=confidence_score or 0.0,
        rationale="",
    )
    input_data_hash = hashlib.sha256(json.dumps(input_data, sort_keys=True).encode()).hexdigest()

    event = CreditAnalysisCompleted(
        application_id=application_id,
        session_id=session_id,
        decision=decision,
        model_version=model_version,
        model_deployment_id=model_deployment_id,
        input_data_hash=input_data_hash,
        analysis_duration_ms=duration_ms,
        regulatory_basis=[regulatory_basis] if regulatory_basis else [],
        completed_at=_now(),
    )
    return await store.append(
        stream_id=f"loan-{application_id}",
        events=[event],
        expected_version=agg.version,
        aggregate_type="loan_application",
        correlation_id=correlation_id,
    )


async def handle_fraud_screening_completed(
    store,
    application_id: str,
    agent_id: str,
    session_id: str,
    fraud_score: float,
    screening_model: str,
    duration_ms: int,
    flags: list[str],
    correlation_id: str | None = None,
) -> int:
    """Record a completed fraud screening (Req 9.2)."""
    if not (0.0 <= fraud_score <= 1.0):
        raise DomainError(
            f"fraud_score must be in [0.0, 1.0], got {fraud_score}",
            context={"fraud_score": fraud_score},
        )

    agg = await LoanApplicationAggregate.load(store, application_id)
    agg.assert_valid_transition(ApplicationState.FRAUD_SCREENING_COMPLETE)

    event = FraudScreeningCompleted(
        application_id=application_id,
        session_id=session_id,
        fraud_score=fraud_score,
        risk_level="HIGH" if fraud_score > 0.7 else "MEDIUM" if fraud_score > 0.3 else "LOW",
        anomalies_found=len(flags),
        recommendation="REFER" if fraud_score > 0.7 else "PASS",
        screening_model_version=screening_model,
        input_data_hash="",
        completed_at=_now(),
    )
    return await store.append(
        stream_id=f"loan-{application_id}",
        events=[event],
        expected_version=agg.version,
        aggregate_type="loan_application",
        correlation_id=correlation_id,
    )


async def handle_compliance_check(
    store,
    application_id: str,
    rule_verdicts: list[dict[str, Any]],
    regulation_set_version: str,
    correlation_id: str | None = None,
) -> int:
    """Record compliance check results (Req 9.2).

    rule_verdicts: list of dicts with keys:
        rule_id, rule_version, passed, evidence_hash, failure_reason (optional)
    """
    agg = await LoanApplicationAggregate.load(store, application_id)
    agg.assert_valid_transition(ApplicationState.COMPLIANCE_CHECK_COMPLETE)

    events: list[Any] = [
        ComplianceCheckRequested(
            application_id=application_id,
            requested_at=_now(),
            regulation_set_version=regulation_set_version,
        )
    ]

    for verdict in rule_verdicts:
        now = _now()
        if verdict.get("passed", False):
            events.append(ComplianceRulePassed(
                application_id=application_id,
                session_id="",
                rule_id=verdict["rule_id"],
                rule_name=verdict.get("rule_name", verdict["rule_id"]),
                rule_version=verdict.get("rule_version", "1.0"),
                evidence_hash=verdict.get("evidence_hash", ""),
                evaluation_notes="",
                evaluated_at=now,
            ))
        else:
            events.append(ComplianceRuleFailed(
                application_id=application_id,
                session_id="",
                rule_id=verdict["rule_id"],
                rule_name=verdict.get("rule_name", verdict["rule_id"]),
                rule_version=verdict.get("rule_version", "1.0"),
                failure_reason=verdict.get("failure_reason", ""),
                is_hard_block=True,
                remediation_available=False,
                evidence_hash=verdict.get("evidence_hash", ""),
                evaluated_at=now,
            ))

    passed_count = sum(1 for v in rule_verdicts if v.get("passed", False))
    failed_count = len(rule_verdicts) - passed_count
    events.append(ComplianceCheckCompleted(
        application_id=application_id,
        session_id="",
        rules_evaluated=len(rule_verdicts),
        rules_passed=passed_count,
        rules_failed=failed_count,
        rules_noted=0,
        has_hard_block=failed_count > 0,
        overall_verdict="CLEAR" if failed_count == 0 else "BLOCKED",
        completed_at=_now(),
    ))

    # Append to compliance stream
    compliance_version = await store.stream_version(f"compliance-{application_id}")
    await store.append(
        stream_id=f"compliance-{application_id}",
        events=events,
        expected_version=compliance_version,
        aggregate_type="compliance_record",
        correlation_id=correlation_id,
    )

    # Also update loan application stream
    passed_count = sum(1 for v in rule_verdicts if v.get("passed", False))
    failed_count = len(rule_verdicts) - passed_count
    loan_event = ComplianceCheckCompleted(
        application_id=application_id,
        session_id="",
        rules_evaluated=len(rule_verdicts),
        rules_passed=passed_count,
        rules_failed=failed_count,
        rules_noted=0,
        has_hard_block=failed_count > 0,
        overall_verdict="CLEAR" if failed_count == 0 else "BLOCKED",
        completed_at=_now(),
    )
    return await store.append(
        stream_id=f"loan-{application_id}",
        events=[loan_event],
        expected_version=agg.version,
        aggregate_type="loan_application",
        correlation_id=correlation_id,
    )


async def handle_generate_decision(
    store,
    application_id: str,
    agent_id: str,
    session_id: str,
    recommendation: str,
    confidence_score: float | None,
    model_versions: dict[str, str],
    contributing_agent_sessions: list[str],
    correlation_id: str | None = None,
) -> int:
    """Generate a loan decision (Req 9.2, 9.3).

    Compliance dependency check: loads ComplianceRecord to verify all rules passed.
    """
    agg = await LoanApplicationAggregate.load(store, application_id)
    agg.assert_valid_transition(ApplicationState.PENDING_DECISION)

    # Confidence floor enforcement (Req 8.2)
    agg.assert_confidence_floor(confidence_score, recommendation)

    # Compliance dependency check at service layer (Req 8.3, 9.3)
    compliance = await ComplianceRecordAggregate.load(store, application_id)
    compliance_cleared = compliance.all_rules_passed and not compliance.has_blocking_failure

    if not compliance_cleared and recommendation.upper() != "DECLINE":
        raise DomainError(
            "Compliance not cleared — only DECLINE is allowed",
            context={"recommendation": recommendation, "compliance_state": compliance.state},
        )

    event = DecisionGenerated(
        application_id=application_id,
        orchestrator_session_id=session_id,
        recommendation=recommendation,
        confidence=confidence_score or 0.0,
        executive_summary="",
        contributing_sessions=contributing_agent_sessions,
        model_versions=model_versions,
        generated_at=_now(),
    )
    return await store.append(
        stream_id=f"loan-{application_id}",
        events=[event],
        expected_version=agg.version,
        aggregate_type="loan_application",
        correlation_id=correlation_id,
    )


async def handle_human_review_completed(
    store,
    application_id: str,
    reviewer_id: str,
    decision: str,
    override: bool,
    correlation_id: str | None = None,
) -> int:
    """Record a human review decision (Req 9.2)."""
    agg = await LoanApplicationAggregate.load(store, application_id)
    agg.assert_valid_transition(
        ApplicationState.APPROVED if decision.upper() == "APPROVE" else ApplicationState.DECLINED
    )

    review_event = HumanReviewCompleted(
        application_id=application_id,
        reviewer_id=reviewer_id,
        override=override,
        original_recommendation=agg.recommendation or "",
        final_decision=decision.upper(),
        reviewed_at=_now(),
    )

    if decision.upper() == "APPROVE":
        final_event: Any = ApplicationApproved(
            application_id=application_id,
            approved_amount_usd=agg.requested_amount_usd or 0.0,
            approved_by=reviewer_id,
            approved_at=_now(),
        )
    else:
        final_event = ApplicationDeclined(
            application_id=application_id,
            decline_reasons=["Human review declined"],
            declined_by=reviewer_id,
            adverse_action_notice_required=False,
            declined_at=_now(),
        )

    return await store.append(
        stream_id=f"loan-{application_id}",
        events=[review_event, final_event],
        expected_version=agg.version,
        aggregate_type="loan_application",
        correlation_id=correlation_id,
    )


# ---------------------------------------------------------------------------
# Agent session handlers
# ---------------------------------------------------------------------------

async def handle_start_agent_session(
    store,
    agent_id: str,
    agent_type: str,
    context_source: str,
    event_replay_from_position: int,
    context_token_count: int,
    model_version: str,
    application_id: str = "",
    langgraph_graph_version: str = "1.0",
    session_id: str | None = None,
    correlation_id: str | None = None,
) -> str:
    """Start an agent session with Gas Town ordering (Req 9.2, 15.4).

    Appends AgentSessionStarted then AgentContextLoaded as the first two events.
    Returns the session_id.
    """
    sid = session_id or str(uuid.uuid4())
    now = _now()

    started = AgentSessionStarted(
        agent_id=agent_id,
        session_id=sid,
        started_at=now,
        agent_type=agent_type,
        application_id=application_id,
        model_version=model_version,
        langgraph_graph_version=langgraph_graph_version,
        context_source=context_source,
        context_token_count=context_token_count,
    )
    context_loaded = AgentContextLoaded(
        agent_id=agent_id,
        session_id=sid,
        agent_type=agent_type,
        application_id=application_id,
        inputs_validated=[],
        validation_duration_ms=0,
        validated_at=now,
    )

    await store.append(
        stream_id=f"session-{sid}",
        events=[started, context_loaded],
        expected_version=-1,
        aggregate_type="agent_session",
        correlation_id=correlation_id,
    )
    return sid
