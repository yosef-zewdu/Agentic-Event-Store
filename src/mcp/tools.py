"""
MCP command-side tools for The Ledger (Req 15.1).

All 8 tools map directly to command handlers. Errors are returned as structured
dicts with {error_type, message, suggested_action, context} (Req 15.2).
Each tool docstring documents preconditions and all error types (Req 15.3).

Rate-limit and role state is stored in module-level dicts (in-process).
For multi-process deployments, move these to a shared Redis key.
"""

from __future__ import annotations
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src.commands.handlers import (
    handle_compliance_check,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_generate_decision,
    handle_human_review_completed,
    handle_request_credit_analysis,
    handle_request_fraud_screening,
    handle_request_human_review,
    handle_start_agent_session,
    handle_submit_application,
    with_occ_retry,
)
from src.integrity.audit_chain import run_integrity_check as _run_integrity_check
from src.mcp.utils import get_store
from src.models.exceptions import DomainError, OptimisticConcurrencyError, RetryBudgetExhausted, StreamNotFoundError

# ---------------------------------------------------------------------------
# Rate-limit for run_integrity_check (Req 15.7): 1 call/minute per caller
# State is persisted in PostgreSQL so it survives restarts and is shared
# across multiple service instances.
# ---------------------------------------------------------------------------
_INTEGRITY_RATE_LIMIT_SECONDS = 60.0

_CREATE_RATE_LIMIT_TABLE = """
    CREATE TABLE IF NOT EXISTS integrity_rate_limits (
        rate_key       TEXT        PRIMARY KEY,
        last_called_at TIMESTAMPTZ NOT NULL
    )
"""

async def _check_and_set_rate_limit(rate_key: str) -> float | None:
    """
    Atomically check and set the rate limit for a given key using PostgreSQL.

    Returns None if the call is allowed (and updates last_called_at).
    Returns the number of seconds to wait if the rate limit is exceeded.

    Using a single conditional UPDATE ensures atomicity across multiple processes.
    """
    store = get_store()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=_INTEGRITY_RATE_LIMIT_SECONDS)

    async with store._pool.acquire() as conn:
        await conn.execute(_CREATE_RATE_LIMIT_TABLE)

        # Try to insert (first call) or update only if last_called_at is old enough.
        # RETURNING last_called_at is NULL when the WHERE clause blocked the update.
        updated = await conn.fetchval(
            """
            INSERT INTO integrity_rate_limits (rate_key, last_called_at)
            VALUES ($1, $2)
            ON CONFLICT (rate_key) DO UPDATE
                SET last_called_at = EXCLUDED.last_called_at
                WHERE integrity_rate_limits.last_called_at < $3
            RETURNING last_called_at
            """,
            rate_key, now, cutoff,
        )

        if updated is not None:
            return None  # allowed

        # Rate-limited — fetch the actual last_called_at to compute wait time
        last = await conn.fetchval(
            "SELECT last_called_at FROM integrity_rate_limits WHERE rate_key = $1",
            rate_key,
        )
        if last is None:
            return None  # race: another process just cleared it
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        elapsed = (now - last).total_seconds()
        return max(1.0, _INTEGRITY_RATE_LIMIT_SECONDS - elapsed)

# ---------------------------------------------------------------------------
# Authorised compliance roles (Req 15.6)
# ---------------------------------------------------------------------------
_COMPLIANCE_ROLES = {"compliance_officer", "compliance_admin", "auditor"}

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _err(error_type: str, message: str, suggested_action: str, context: dict | None = None) -> dict:
    """Build the standard error response shape (Req 15.2)."""
    return {
        "error_type": error_type,
        "message": message,
        "suggested_action": suggested_action,
        "context": context or {},
    }


# ---------------------------------------------------------------------------
# Tool 1 — submit_application
# ---------------------------------------------------------------------------
async def submit_application(
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
) -> dict:
    """
    Submit a new commercial loan application to The Ledger.

    PRECONDITIONS:
    - application_id must be globally unique; re-submitting the same ID returns
    OptimisticConcurrencyError (stream already exists at version 0).

    ERRORS:
    - OptimisticConcurrencyError: application_id already exists.
    suggested_action: use_unique_application_id
    - DomainError: business rule violation.
    suggested_action: check_error_context_for_details
    """
    try:
        # No OCC retry here — a duplicate application_id is a hard error, not a race.
        version = await handle_submit_application(
            store=get_store(),
            application_id=application_id,
            applicant_id=applicant_id,
            requested_amount_usd=Decimal(str(requested_amount_usd)),
            loan_purpose=loan_purpose,
            loan_term_months=loan_term_months,
            submission_channel=submission_channel,
            contact_email=contact_email,
            contact_name=contact_name,
            application_reference=application_reference,
            correlation_id=correlation_id,
        )
        return {"success": True, "application_id": application_id, "stream_version": version}
    except OptimisticConcurrencyError as exc:
        return _err(
            "OptimisticConcurrencyError",
            str(exc),
            "use_unique_application_id",
            {"stream_id": exc.stream_id, "actual_version": exc.actual_version},
        )
    except DomainError as exc:
        return _err("DomainError", str(exc), "check_error_context_for_details", exc.context)
    except Exception as exc:
        logger.exception("Unexpected error in MCP tool: %s", exc)
        return _err("InternalError", str(exc), "contact_support", {})


# ---------------------------------------------------------------------------
# Tool 2 — request_credit_analysis
# ---------------------------------------------------------------------------
async def request_credit_analysis(
    application_id: str,
    requested_by: str = "system",
    priority: str = "NORMAL",
    correlation_id: str | None = None,
) -> dict:
    """
    Transition a submitted application to CREDIT_ANALYSIS_REQUESTED state.

    Must be called after submit_application and before record_credit_analysis.

    ERRORS:
    - DomainError(InvalidStateTransition): application not in SUBMITTED state.
    """
    try:
        version = await with_occ_retry(
            handle_request_credit_analysis,
            store=get_store(),
            application_id=application_id,
            requested_by=requested_by,
            priority=priority,
            correlation_id=correlation_id,
        )
        return {"success": True, "application_id": application_id, "stream_version": version}
    except RetryBudgetExhausted as exc:
        return _err("RetryBudgetExhausted", str(exc), "investigate_contention", {"stream_id": exc.stream_id})
    except DomainError as exc:
        return _err("DomainError", str(exc), "check_error_context_for_details", exc.context)
    except Exception as exc:
        logger.exception("Unexpected error in MCP tool: %s", exc)
        return _err("InternalError", str(exc), "contact_support", {})


# ---------------------------------------------------------------------------
# Tool 3 — record_credit_analysis
# ---------------------------------------------------------------------------
async def record_credit_analysis(
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
) -> dict:
    """
    Record a completed credit analysis for a loan application.

    PRECONDITIONS:
    - start_agent_session must have been called for this session_id.
    - AgentContextLoaded (AgentInputValidated) must be present in the session stream
    before any decision event (Gas Town pattern, Req 7.1).
    - Application must be in AwaitingAnalysis / CREDIT_ANALYSIS_REQUESTED state.
    - No prior CreditAnalysisCompleted without a CreditAnalysisSuperseded (Req 8.1).

    ERRORS:
    - OptimisticConcurrencyError: concurrent write conflict.
    suggested_action: reload_stream_and_retry
    - DomainError(GasTownPreconditionNotMet): call start_agent_session first.
    suggested_action: call_start_agent_session_first
    - DomainError(InvalidStateTransition): check application state.
    suggested_action: query_ledger_applications_id_for_current_state
    - DomainError(DuplicateCreditAnalysis): supersede the existing analysis first.
    suggested_action: submit_credit_analysis_superseded_event_first
    """
    try:
        version = await with_occ_retry(
            handle_credit_analysis_completed,
            store=get_store(),
            application_id=application_id,
            agent_id=agent_id,
            session_id=session_id,
            risk_tier=risk_tier,
            recommended_limit_usd=recommended_limit_usd,
            model_version=model_version,
            confidence_score=confidence_score,
            regulatory_basis=regulatory_basis,
            duration_ms=duration_ms,
            input_data=input_data,
            model_deployment_id=model_deployment_id,
            correlation_id=correlation_id,
        )
        return {"success": True, "application_id": application_id, "stream_version": version}
    except RetryBudgetExhausted as exc:
        return _err("RetryBudgetExhausted", str(exc), "investigate_contention", {"stream_id": exc.stream_id})
    except DomainError as exc:
        return _err("DomainError", str(exc), "check_error_context_for_details", exc.context)
    except StreamNotFoundError as exc:
        return _err(
            "StreamNotFoundError",
            str(exc),
            "call_start_agent_session_first",
            {"stream_id": exc.stream_id},
        )
    except Exception as exc:
        logger.exception("Unexpected error in MCP tool: %s", exc)
        return _err("InternalError", str(exc), "contact_support", {})


# ---------------------------------------------------------------------------
# Tool 4 — request_fraud_screening
# ---------------------------------------------------------------------------
async def request_fraud_screening(
    application_id: str,
    correlation_id: str | None = None,
) -> dict:
    """
    Transition application from CREDIT_ANALYSIS_COMPLETE to FRAUD_SCREENING_REQUESTED.

    Must be called after record_credit_analysis and before record_fraud_screening.

    ERRORS:
    - DomainError(InvalidStateTransition): application not in CREDIT_ANALYSIS_COMPLETE state.
    """
    try:
        version = await with_occ_retry(
            handle_request_fraud_screening,
            store=get_store(),
            application_id=application_id,
            correlation_id=correlation_id,
        )
        return {"success": True, "application_id": application_id, "stream_version": version}
    except RetryBudgetExhausted as exc:
        return _err("RetryBudgetExhausted", str(exc), "investigate_contention", {"stream_id": exc.stream_id})
    except DomainError as exc:
        return _err("DomainError", str(exc), "check_error_context_for_details", exc.context)
    except Exception as exc:
        logger.exception("Unexpected error in MCP tool: %s", exc)
        return _err("InternalError", str(exc), "contact_support", {})


# ---------------------------------------------------------------------------
# Tool 5 — record_fraud_screening
# ---------------------------------------------------------------------------
async def record_fraud_screening(
    application_id: str,
    agent_id: str,
    session_id: str,
    fraud_score: float,
    screening_model: str,
    duration_ms: int,
    flags: list[str] | None = None,
    correlation_id: str | None = None,
) -> dict:
    """
    Record a completed fraud screening for a loan application.

    PRECONDITIONS:
    - start_agent_session must have been called for this session_id.
    - Application must be in a state that accepts fraud screening results.

    VALIDATION:
    - fraud_score must be in [0.0, 1.0] inclusive (Req 15.9).
    Values outside this range return ValidationError immediately without
    appending any event.

    ERRORS:
    - ValidationError(fraud_score_out_of_range): provide a value in [0.0, 1.0].
    suggested_action: provide_fraud_score_between_0_and_1
    - OptimisticConcurrencyError: concurrent write conflict.
    suggested_action: reload_stream_and_retry
    - DomainError(GasTownPreconditionNotMet): call start_agent_session first.
    suggested_action: call_start_agent_session_first
    - DomainError(InvalidStateTransition): check application state.
    suggested_action: query_ledger_applications_id_for_current_state
    """
    # Req 15.9 — validate fraud_score range before touching the store
    if not (0.0 <= fraud_score <= 1.0):
        return _err(
            "ValidationError",
            f"fraud_score must be in [0.0, 1.0], got {fraud_score}",
            "provide_fraud_score_between_0_and_1",
            {"field": "fraud_score", "value": fraud_score, "valid_range": [0.0, 1.0]},
        )

    try:
        version = await with_occ_retry(
            handle_fraud_screening_completed,
            store=get_store(),
            application_id=application_id,
            agent_id=agent_id,
            session_id=session_id,
            fraud_score=fraud_score,
            screening_model=screening_model,
            duration_ms=duration_ms,
            flags=flags or [],
            correlation_id=correlation_id,
        )
        return {"success": True, "application_id": application_id, "stream_version": version}
    except RetryBudgetExhausted as exc:
        return _err("RetryBudgetExhausted", str(exc), "investigate_contention", {"stream_id": exc.stream_id})
    except DomainError as exc:
        return _err("DomainError", str(exc), "check_error_context_for_details", exc.context)
    except Exception as exc:
        logger.exception("Unexpected error in MCP tool: %s", exc)
        return _err("InternalError", str(exc), "contact_support", {})


# ---------------------------------------------------------------------------
# Tool 6 — record_compliance_check
# ---------------------------------------------------------------------------
async def record_compliance_check(
    application_id: str,
    rule_verdicts: list[dict],
    regulation_set_version: str,
    correlation_id: str | None = None,
) -> dict:
    """
    Record compliance check results for a loan application.

    Each entry in rule_verdicts must contain:
    rule_id (str), passed (bool).
    Optional: rule_version (str), evidence_hash (str), failure_reason (str).

    PRECONDITIONS:
    - Application must be in ComplianceReview / COMPLIANCE_CHECK_REQUESTED state.

    ERRORS:
    - ValidationError: a verdict is missing required keys (rule_id, passed).
    suggested_action: ensure_each_verdict_has_rule_id_and_passed
    - OptimisticConcurrencyError: concurrent write conflict.
    suggested_action: reload_stream_and_retry
    - DomainError(InvalidStateTransition): check application state.
    suggested_action: query_ledger_applications_id_for_current_state
    - DomainError: business rule violation.
    suggested_action: check_error_context_for_details
    """
    # Validate structure before touching the store
    required_keys = {"rule_id", "passed"}
    for i, verdict in enumerate(rule_verdicts):
        missing = required_keys - verdict.keys()
        if missing:
            return _err(
                "ValidationError",
                f"rule_verdicts[{i}] is missing required keys: {sorted(missing)}",
                "ensure_each_verdict_has_rule_id_and_passed",
                {"index": i, "missing_keys": sorted(missing)},
            )
    try:
        version = await with_occ_retry(
            handle_compliance_check,
            store=get_store(),
            application_id=application_id,
            rule_verdicts=rule_verdicts,
            regulation_set_version=regulation_set_version,
            correlation_id=correlation_id,
        )
        return {"success": True, "application_id": application_id, "stream_version": version}
    except RetryBudgetExhausted as exc:
        return _err("RetryBudgetExhausted", str(exc), "investigate_contention", {"stream_id": exc.stream_id})
    except DomainError as exc:
        return _err("DomainError", str(exc), "check_error_context_for_details", exc.context)
    except Exception as exc:
        logger.exception("Unexpected error in MCP tool: %s", exc)
        return _err("InternalError", str(exc), "contact_support", {})


# ---------------------------------------------------------------------------
# Tool 7 — generate_decision
# ---------------------------------------------------------------------------
async def generate_decision(
    application_id: str,
    agent_id: str,
    session_id: str,
    recommendation: str,
    confidence_score: float | None,
    model_versions: dict[str, str],
    contributing_agent_sessions: list[str],
    approved_amount_usd: float | None = None,
    correlation_id: str | None = None,
) -> dict:
    """
    Generate a loan decision for an application.

    CONFIDENCE FLOOR (Req 15.5):
    - When confidence_score < 0.6, recommendation is overridden to "REFER"
    regardless of the submitted value.

    APPROVED AMOUNT CAP (Req 8.5):
    - approved_amount_usd must not exceed the originally requested amount.

    PRECONDITIONS:
    - All compliance checks must be cleared (ComplianceRulePassed for all rules).
    - Application must be in PendingDecision state.
    - All session IDs in contributing_agent_sessions must have processed this
    application (Req 8.4).

    ERRORS:
    - OptimisticConcurrencyError: concurrent write conflict.
    suggested_action: reload_stream_and_retry
    - DomainError(ComplianceNotCleared): compliance checks must pass before approval.
    suggested_action: ensure_all_compliance_rules_passed
    - DomainError(InvalidStateTransition): check application state.
    suggested_action: query_ledger_applications_id_for_current_state
    - DomainError(InvalidContributingSession): a session did not process this application.
    suggested_action: verify_contributing_agent_sessions
    - DomainError(ApprovedAmountExceedsRequested): reduce approved_amount_usd.
    suggested_action: set_approved_amount_usd_at_or_below_requested
    """
    # Req 15.5 — enforce confidence floor before delegating to handler
    effective_recommendation = recommendation
    if confidence_score is not None and confidence_score < 0.6:
        effective_recommendation = "REFER"

    try:
        version = await with_occ_retry(
            handle_generate_decision,
            store=get_store(),
            application_id=application_id,
            agent_id=agent_id,
            session_id=session_id,
            recommendation=effective_recommendation,
            confidence_score=confidence_score,
            model_versions=model_versions,
            contributing_agent_sessions=contributing_agent_sessions,
            approved_amount_usd=approved_amount_usd,
            correlation_id=correlation_id,
        )
        return {
            "success": True,
            "application_id": application_id,
            "stream_version": version,
            "recommendation": effective_recommendation,
            "confidence_floor_applied": effective_recommendation != recommendation,
        }
    except RetryBudgetExhausted as exc:
        return _err("RetryBudgetExhausted", str(exc), "investigate_contention", {"stream_id": exc.stream_id})
    except DomainError as exc:
        return _err("DomainError", str(exc), "check_error_context_for_details", exc.context)
    except Exception as exc:
        logger.exception("Unexpected error in MCP tool: %s", exc)
        return _err("InternalError", str(exc), "contact_support", {})


# ---------------------------------------------------------------------------
# Tool 8 — request_human_review
# ---------------------------------------------------------------------------
async def request_human_review(
    application_id: str,
    reason: str,
    decision_event_id: str = "",
    assigned_to: str | None = None,
    correlation_id: str | None = None,
) -> dict:
    """
    Transition application from PENDING_DECISION to PENDING_HUMAN_REVIEW.

    Must be called after generate_decision and before record_human_review.

    Args:
        reason: Why human review is required (e.g. "REFER from low confidence").
        decision_event_id: Optional event_id of the DecisionGenerated event.
        assigned_to: Optional reviewer_id to pre-assign the review.

    ERRORS:
    - DomainError(InvalidStateTransition): application not in PENDING_DECISION state.
    """
    try:
        version = await with_occ_retry(
            handle_request_human_review,
            store=get_store(),
            application_id=application_id,
            reason=reason,
            decision_event_id=decision_event_id,
            assigned_to=assigned_to,
            correlation_id=correlation_id,
        )
        return {"success": True, "application_id": application_id, "stream_version": version}
    except RetryBudgetExhausted as exc:
        return _err("RetryBudgetExhausted", str(exc), "investigate_contention", {"stream_id": exc.stream_id})
    except DomainError as exc:
        return _err("DomainError", str(exc), "check_error_context_for_details", exc.context)
    except Exception as exc:
        logger.exception("Unexpected error in MCP tool: %s", exc)
        return _err("InternalError", str(exc), "contact_support", {})


# ---------------------------------------------------------------------------
# Tool 9 — record_human_review
# ---------------------------------------------------------------------------
async def record_human_review(
    application_id: str,
    reviewer_id: str,
    decision: str,
    override: bool = False,
    correlation_id: str | None = None,
) -> dict:
    """
    Record a human loan officer's final review decision.

    PRECONDITIONS:
    - Application must be in ApprovedPendingHuman or DeclinedPendingHuman state.
    - decision must be "APPROVE" or "DECLINE" (case-insensitive).

    ERRORS:
    - OptimisticConcurrencyError: concurrent write conflict.
    suggested_action: reload_stream_and_retry
    - DomainError(InvalidStateTransition): application is not awaiting human review.
    suggested_action: query_ledger_applications_id_for_current_state
    - DomainError: business rule violation.
    suggested_action: check_error_context_for_details
    """
    if decision.upper() not in {"APPROVE", "DECLINE"}:
        return _err(
            "ValidationError",
            f"decision must be 'APPROVE' or 'DECLINE', got {decision!r}",
            "provide_approve_or_decline",
            {"field": "decision", "value": decision},
        )

    try:
        version = await with_occ_retry(
            handle_human_review_completed,
            store=get_store(),
            application_id=application_id,
            reviewer_id=reviewer_id,
            decision=decision,
            override=override,
            correlation_id=correlation_id,
        )
        return {
            "success": True,
            "application_id": application_id,
            "stream_version": version,
            "final_decision": decision.upper(),
        }
    except RetryBudgetExhausted as exc:
        return _err("RetryBudgetExhausted", str(exc), "investigate_contention", {"stream_id": exc.stream_id})
    except DomainError as exc:
        return _err("DomainError", str(exc), "check_error_context_for_details", exc.context)
    except Exception as exc:
        logger.exception("Unexpected error in MCP tool: %s", exc)
        return _err("InternalError", str(exc), "contact_support", {})


# ---------------------------------------------------------------------------
# Tool 7 — start_agent_session
# ---------------------------------------------------------------------------
async def start_agent_session(
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
) -> dict:
    """
    Start a new agent session, appending AgentSessionStarted then AgentContextLoaded
    as the first two events (Gas Town ordering, Req 15.4).

    PRECONDITIONS:
    - agent_id must identify a known agent type.
    - model_version must be a non-empty string.

    RETURNS:
    - session_id: the UUID of the created session.
    - context_position: stream_position of the AgentContextLoaded event (always 2).

    ERRORS:
    - OptimisticConcurrencyError: session_id already exists.
    suggested_action: use_unique_session_id_or_omit_to_auto_generate
    - DomainError: business rule violation.
    suggested_action: check_error_context_for_details
    """
    try:
        sid = await with_occ_retry(
            handle_start_agent_session,
            store=get_store(),
            agent_id=agent_id,
            agent_type=agent_type,
            context_source=context_source,
            event_replay_from_position=event_replay_from_position,
            context_token_count=context_token_count,
            model_version=model_version,
            application_id=application_id,
            langgraph_graph_version=langgraph_graph_version,
            session_id=session_id,
            correlation_id=correlation_id,
        )
        return {
            "success": True,
            "session_id": sid,
            # AgentSessionStarted = position 1, AgentContextLoaded = position 2
            "context_position": 2,
        }
    except RetryBudgetExhausted as exc:
        return _err("RetryBudgetExhausted", str(exc), "investigate_contention", {"stream_id": exc.stream_id})
    except DomainError as exc:
        return _err("DomainError", str(exc), "check_error_context_for_details", exc.context)
    except Exception as exc:
        logger.exception("Unexpected error in MCP tool: %s", exc)
        return _err("InternalError", str(exc), "contact_support", {})


# ---------------------------------------------------------------------------
# Tool 8 — run_integrity_check
# ---------------------------------------------------------------------------
async def run_integrity_check(
    entity_type: str,
    entity_id: str,
    caller_role: str = "",
    caller_id: str = "",
) -> dict:
    """
    Run a cryptographic hash-chain integrity check over an entity's audit ledger.

    PRECONDITIONS (Req 15.6):
    - caller_role must be one of: compliance_officer, compliance_admin, auditor.
    - caller_id must be a non-empty string (used as the rate-limit key).

    RATE LIMIT (Req 15.7):
    - Maximum 1 call per minute per (caller_id, entity_type, entity_id) triple.
    Exceeding this returns RateLimitError.

    ERRORS:
    - AuthorizationError: caller_role is not authorised.
    suggested_action: use_authorised_compliance_role
    - ValidationError: caller_id is empty.
    suggested_action: provide_non_empty_caller_id
    - RateLimitError: rate limit exceeded.
    suggested_action: wait_60_seconds_before_retrying
    - OptimisticConcurrencyError: concurrent integrity check in progress.
    suggested_action: reload_stream_and_retry
    - DomainError: business rule violation.
    suggested_action: check_error_context_for_details
    """
    # Req 15.6 — role check
    if caller_role not in _COMPLIANCE_ROLES:
        return _err(
            "AuthorizationError",
            f"Role {caller_role!r} is not authorised to run integrity checks. "
            f"Authorised roles: {sorted(_COMPLIANCE_ROLES)}",
            "use_authorised_compliance_role",
            {"caller_role": caller_role, "authorised_roles": sorted(_COMPLIANCE_ROLES)},
        )

    # caller_id must be non-empty — otherwise the rate limit key is meaningless
    if not caller_id or not caller_id.strip():
        return _err(
            "ValidationError",
            "caller_id must be a non-empty string to enforce per-caller rate limiting",
            "provide_non_empty_caller_id",
            {"field": "caller_id"},
        )

    # Req 15.7 — 1/minute rate limit keyed by caller + entity (persisted in PostgreSQL)
    rate_key = f"{caller_id.strip()}:{entity_type}:{entity_id}"
    wait = await _check_and_set_rate_limit(rate_key)
    if wait is not None:
        return _err(
            "RateLimitError",
            f"Integrity check rate limit exceeded. Wait {int(wait) + 1}s before retrying.",
            "wait_60_seconds_before_retrying",
            {"retry_after_seconds": int(wait) + 1, "rate_limit_seconds": _INTEGRITY_RATE_LIMIT_SECONDS},
        )

    try:
        result = await _run_integrity_check(
            store=get_store(),
            entity_type=entity_type,
            entity_id=entity_id,
        )
        return {
            "success": True,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "chain_valid": result.chain_valid,
            "tamper_detected": result.tamper_detected,
            "events_verified": result.events_verified,
            "integrity_hash": result.integrity_hash,
        }
    except OptimisticConcurrencyError as exc:
        return _err(
            "OptimisticConcurrencyError",
            str(exc),
            "reload_stream_and_retry",
            {
                "stream_id": exc.stream_id,
                "expected_version": exc.expected_version,
                "actual_version": exc.actual_version,
            },
        )
    except DomainError as exc:
        return _err("DomainError", str(exc), "check_error_context_for_details", exc.context)
    except Exception as exc:
        logger.exception("Unexpected error in MCP tool: %s", exc)
        return _err("InternalError", str(exc), "contact_support", {})


async def withdraw_application(
    application_id: str,
    reason: str,
    correlation_id: str | None = None,
) -> dict:
    """
    Withdraw a loan application, moving it to the WITHDRAWN terminal state.

    Valid from: SUBMITTED, CREDIT_ANALYSIS_REQUESTED, CREDIT_ANALYSIS_COMPLETE.

    ERRORS:
    - DomainError(InvalidStateTransition): application is past the point of withdrawal.
    suggested_action: query_ledger_applications_id_for_current_state
    """
    from src.commands.handlers import handle_withdraw_application
    try:
        version = await with_occ_retry(
            handle_withdraw_application,
            store=get_store(),
            application_id=application_id,
            reason=reason,
            correlation_id=correlation_id,
        )
        return {"success": True, "application_id": application_id, "stream_version": version}
    except RetryBudgetExhausted as exc:
        return _err("RetryBudgetExhausted", str(exc), "investigate_contention", {"stream_id": exc.stream_id})
    except DomainError as exc:
        return _err("DomainError", str(exc), "check_error_context_for_details", exc.context)
    except Exception as exc:
        logger.exception("Unexpected error in MCP tool: %s", exc)
        return _err("InternalError", str(exc), "contact_support", {})


async def generate_regulatory_package(
    entity_type: str,
    entity_id: str,
    examination_date: str,
) -> dict:
    """
    Generate a regulatory examination package for a loan application (Req 19).

    Produces a self-contained JSON document containing:
    - Complete event stream snapshot up to examination_date (Req 19.1)
    - Projection states at that date: ApplicationSummary + ComplianceAuditView (Req 19.2)
    - Cryptographic audit chain integrity result (Req 19.3)
    - Human-readable narrative summary (Req 19.4)
    - Agent model versions and confidence scores (Req 19.4)
    - Causal chain traversal via recursive CTE on causation_id (Req 19.4)

    Args:
        entity_type:      Stream prefix, e.g. "loan"
        entity_id:        Entity identifier, e.g. "app-12345"
        examination_date: ISO 8601 UTC timestamp — only events up to this date
                          are included (e.g. "2025-01-31T23:59:59Z")

    ERRORS:
    - InvalidTimestamp: examination_date is not a valid ISO 8601 timestamp
    - InternalError: unexpected failure during package generation
    """
    from src.integrity.regulatory_package import generate_regulatory_package as _gen_pkg

    # Parse examination_date
    try:
        exam_dt = datetime.fromisoformat(examination_date.replace("Z", "+00:00"))
    except (ValueError, AttributeError) as exc:
        return _err(
            "InvalidTimestamp",
            f"examination_date must be an ISO 8601 timestamp, got: {examination_date!r}",
            "provide_iso8601_timestamp",
            {"example": "2025-01-31T23:59:59Z"},
        )

    try:
        store = get_store()
        pool = store._pool  # may be None in InMemory mode
        pkg = await _gen_pkg(
            store=store,
            pool=pool,
            entity_type=entity_type,
            entity_id=entity_id,
            examination_date=exam_dt,
        )
        return {"success": True, **pkg.to_dict()}
    except Exception as exc:
        logger.exception("Unexpected error in generate_regulatory_package: %s", exc)
        return _err("InternalError", str(exc), "contact_support", {})


def register_tools(mcp):
    mcp.tool()(submit_application)
    mcp.tool()(request_credit_analysis)
    mcp.tool()(record_credit_analysis)
    mcp.tool()(request_fraud_screening)
    mcp.tool()(record_fraud_screening)
    mcp.tool()(record_compliance_check)
    mcp.tool()(generate_decision)
    mcp.tool()(request_human_review)
    mcp.tool()(record_human_review)
    mcp.tool()(start_agent_session)
    mcp.tool()(run_integrity_check)
    mcp.tool()(generate_regulatory_package)
    mcp.tool()(withdraw_application)
