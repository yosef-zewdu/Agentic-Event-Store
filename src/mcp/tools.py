"""
MCP command-side tools for The Ledger (Req 15.1).

All 8 tools map directly to command handlers. Errors are returned as structured
dicts with {error_type, message, suggested_action, context} (Req 15.2).
Each tool docstring documents preconditions and all error types (Req 15.3).

Rate-limit and role state is stored in module-level dicts (in-process).
For multi-process deployments, move these to a shared Redis key.
"""



from __future__ import annotations
# import os 
# import sys
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import time
from typing import Any

from src.commands.handlers import (
    handle_compliance_check,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_generate_decision,
    handle_human_review_completed,
    handle_start_agent_session,
    handle_submit_application,
)
from src.integrity.audit_chain import run_integrity_check as _run_integrity_check
from src.mcp.utils import get_store
from src.models.exceptions import DomainError, OptimisticConcurrencyError, StreamNotFoundError

# ---------------------------------------------------------------------------
# Rate-limit state for run_integrity_check (Req 15.7): 1 call/minute per caller
# ---------------------------------------------------------------------------
_integrity_last_called: dict[str, float] = {}
_INTEGRITY_RATE_LIMIT_SECONDS = 60.0

# ---------------------------------------------------------------------------
# Authorised compliance roles (Req 15.6)
# ---------------------------------------------------------------------------
_COMPLIANCE_ROLES = {"compliance_officer", "compliance_admin", "auditor"}


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


def _wrap(result: Any) -> dict:
    """Wrap a successful scalar result."""
    if isinstance(result, dict):
        return result
    return {"success": True, "result": result}



def register_tools(mcp):


    # ---------------------------------------------------------------------------
    # Tool 1 — submit_application
    # ---------------------------------------------------------------------------
    @mcp.tool()
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
            version = await handle_submit_application(
                store=get_store(),
                application_id=application_id,
                applicant_id=applicant_id,
                requested_amount_usd=requested_amount_usd,
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
            return _err("InternalError", str(exc), "contact_support", {})


    # ---------------------------------------------------------------------------
    # Tool 2 — record_credit_analysis
    # ---------------------------------------------------------------------------

    @mcp.tool()
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
            version = await handle_credit_analysis_completed(
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
        except StreamNotFoundError as exc:
            return _err(
                "StreamNotFoundError",
                str(exc),
                "call_start_agent_session_first",
                {"stream_id": exc.stream_id},
            )
        except Exception as exc:
            return _err("InternalError", str(exc), "contact_support", {})


    # ---------------------------------------------------------------------------
    # Tool 3 — record_fraud_screening
    # ---------------------------------------------------------------------------

    @mcp.tool()
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
            version = await handle_fraud_screening_completed(
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
            return _err("InternalError", str(exc), "contact_support", {})


    # ---------------------------------------------------------------------------
    # Tool 4 — record_compliance_check
    # ---------------------------------------------------------------------------

    @mcp.tool()
    async def record_compliance_check(
        application_id: str,
        rule_verdicts: list[dict],
        regulation_set_version: str,
        correlation_id: str | None = None,
    ) -> dict:
        """
        Record compliance check results for a loan application.

        Each entry in rule_verdicts must contain:
        rule_id (str), rule_version (str), passed (bool),
        evidence_hash (str), failure_reason (str, optional).

        PRECONDITIONS:
        - Application must be in ComplianceReview / COMPLIANCE_CHECK_REQUESTED state.

        ERRORS:
        - OptimisticConcurrencyError: concurrent write conflict.
        suggested_action: reload_stream_and_retry
        - DomainError(InvalidStateTransition): check application state.
        suggested_action: query_ledger_applications_id_for_current_state
        - DomainError: business rule violation.
        suggested_action: check_error_context_for_details
        """
        try:
            version = await handle_compliance_check(
                store=get_store(),
                application_id=application_id,
                rule_verdicts=rule_verdicts,
                regulation_set_version=regulation_set_version,
                correlation_id=correlation_id,
            )
            return {"success": True, "application_id": application_id, "stream_version": version}
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
            return _err("InternalError", str(exc), "contact_support", {})


    # ---------------------------------------------------------------------------
    # Tool 5 — generate_decision
    # ---------------------------------------------------------------------------

    @mcp.tool()
    async def generate_decision(
        application_id: str,
        agent_id: str,
        session_id: str,
        recommendation: str,
        confidence_score: float | None,
        model_versions: dict[str, str],
        contributing_agent_sessions: list[str],
        correlation_id: str | None = None,
    ) -> dict:
        """
        Generate a loan decision for an application.

        CONFIDENCE FLOOR (Req 15.5):
        - When confidence_score < 0.6, recommendation is overridden to "REFER"
        regardless of the submitted value.

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
        """
        # Req 15.5 — enforce confidence floor before delegating to handler
        effective_recommendation = recommendation
        if confidence_score is not None and confidence_score < 0.6:
            effective_recommendation = "REFER"

        try:
            version = await handle_generate_decision(
                store=get_store(),
                application_id=application_id,
                agent_id=agent_id,
                session_id=session_id,
                recommendation=effective_recommendation,
                confidence_score=confidence_score,
                model_versions=model_versions,
                contributing_agent_sessions=contributing_agent_sessions,
                correlation_id=correlation_id,
            )
            return {
                "success": True,
                "application_id": application_id,
                "stream_version": version,
                "recommendation": effective_recommendation,
                "confidence_floor_applied": effective_recommendation != recommendation,
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
            return _err("InternalError", str(exc), "contact_support", {})


    # ---------------------------------------------------------------------------
    # Tool 6 — record_human_review
    # ---------------------------------------------------------------------------

    @mcp.tool()
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
            version = await handle_human_review_completed(
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
            return _err("InternalError", str(exc), "contact_support", {})


    # ---------------------------------------------------------------------------
    # Tool 7 — start_agent_session
    # ---------------------------------------------------------------------------

    @mcp.tool()
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
            sid = await handle_start_agent_session(
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
        except OptimisticConcurrencyError as exc:
            return _err(
                "OptimisticConcurrencyError",
                str(exc),
                "use_unique_session_id_or_omit_to_auto_generate",
                {"stream_id": exc.stream_id, "actual_version": exc.actual_version},
            )
        except DomainError as exc:
            return _err("DomainError", str(exc), "check_error_context_for_details", exc.context)
        except Exception as exc:
            return _err("InternalError", str(exc), "contact_support", {})


    # ---------------------------------------------------------------------------
    # Tool 8 — run_integrity_check
    # ---------------------------------------------------------------------------

    @mcp.tool()
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
        Any other role returns AuthorizationError immediately.

        RATE LIMIT (Req 15.7):
        - Maximum 1 call per minute per (caller_id, entity_type, entity_id) triple.
        Exceeding this returns RateLimitError.

        ERRORS:
        - AuthorizationError: caller_role is not authorised.
        suggested_action: use_authorised_compliance_role
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

        # Req 15.7 — 1/minute rate limit keyed by caller + entity
        rate_key = f"{caller_id}:{entity_type}:{entity_id}"
        now = time.monotonic()
        last = _integrity_last_called.get(rate_key, 0.0)
        elapsed = now - last
        if elapsed < _INTEGRITY_RATE_LIMIT_SECONDS:
            wait = int(_INTEGRITY_RATE_LIMIT_SECONDS - elapsed) + 1
            return _err(
                "RateLimitError",
                f"Integrity check rate limit exceeded. Wait {wait}s before retrying.",
                "wait_60_seconds_before_retrying",
                {"retry_after_seconds": wait, "rate_limit_seconds": _INTEGRITY_RATE_LIMIT_SECONDS},
            )

        _integrity_last_called[rate_key] = now

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
            return _err("InternalError", str(exc), "contact_support", {})
