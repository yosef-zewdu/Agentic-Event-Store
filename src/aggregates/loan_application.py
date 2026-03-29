"""
LoanApplicationAggregate — rebuilds loan application state by replaying events.

Business rules enforced here (Reqs 6.1–6.5, 8.1–8.5):
  1. State machine: only valid transitions allowed
  2. Duplicate CreditAnalysisCompleted prevention (Req 8.1)
  3. confidence_score < 0.6 → recommendation must be REFER (Req 8.2)
  4. approved_amount_usd cap at requested_amount_usd (Req 8.5)
  5. Causal chain: contributing_agent_sessions must reference known sessions (Req 8.4)
  6. Compliance must be cleared before APPROVE (Req 8.3 — checked at service layer)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from src.models.exceptions import DomainError


class ApplicationState(str, Enum):
    NEW = "NEW"
    SUBMITTED = "SUBMITTED"
    CREDIT_ANALYSIS_REQUESTED = "CREDIT_ANALYSIS_REQUESTED"
    CREDIT_ANALYSIS_COMPLETE = "CREDIT_ANALYSIS_COMPLETE"
    FRAUD_SCREENING_REQUESTED = "FRAUD_SCREENING_REQUESTED"
    FRAUD_SCREENING_COMPLETE = "FRAUD_SCREENING_COMPLETE"
    COMPLIANCE_CHECK_REQUESTED = "COMPLIANCE_CHECK_REQUESTED"
    COMPLIANCE_CHECK_COMPLETE = "COMPLIANCE_CHECK_COMPLETE"
    PENDING_DECISION = "PENDING_DECISION"
    PENDING_HUMAN_REVIEW = "PENDING_HUMAN_REVIEW"
    APPROVED = "APPROVED"
    DECLINED = "DECLINED"
    DECLINED_COMPLIANCE = "DECLINED_COMPLIANCE"
    WITHDRAWN = "WITHDRAWN"


# Valid state transitions (Req 6.1)
VALID_TRANSITIONS: dict[ApplicationState, list[ApplicationState]] = {
    ApplicationState.NEW: [ApplicationState.SUBMITTED],
    ApplicationState.SUBMITTED: [
        ApplicationState.CREDIT_ANALYSIS_REQUESTED,
        ApplicationState.WITHDRAWN,
    ],
    ApplicationState.CREDIT_ANALYSIS_REQUESTED: [
        ApplicationState.CREDIT_ANALYSIS_COMPLETE,
        ApplicationState.WITHDRAWN,
    ],
    ApplicationState.CREDIT_ANALYSIS_COMPLETE: [
        ApplicationState.FRAUD_SCREENING_REQUESTED,
        ApplicationState.WITHDRAWN,
    ],
    ApplicationState.FRAUD_SCREENING_REQUESTED: [ApplicationState.FRAUD_SCREENING_COMPLETE],
    ApplicationState.FRAUD_SCREENING_COMPLETE: [ApplicationState.COMPLIANCE_CHECK_REQUESTED],
    ApplicationState.COMPLIANCE_CHECK_REQUESTED: [ApplicationState.COMPLIANCE_CHECK_COMPLETE],
    ApplicationState.COMPLIANCE_CHECK_COMPLETE: [
        ApplicationState.PENDING_DECISION,
        ApplicationState.DECLINED_COMPLIANCE,
    ],
    ApplicationState.PENDING_DECISION: [
        ApplicationState.APPROVED,
        ApplicationState.DECLINED,
        ApplicationState.PENDING_HUMAN_REVIEW,
    ],
    ApplicationState.PENDING_HUMAN_REVIEW: [
        ApplicationState.APPROVED,
        ApplicationState.DECLINED,
    ],
}

# Terminal states — no further transitions allowed
TERMINAL_STATES = {
    ApplicationState.APPROVED,
    ApplicationState.DECLINED,
    ApplicationState.DECLINED_COMPLIANCE,
    ApplicationState.WITHDRAWN,
}


@dataclass
class LoanApplicationAggregate:
    """
    Aggregate root for a loan application.

    Rebuilt by replaying the stream via load(). Never instantiate directly
    except in tests — use load() classmethod.
    """

    application_id: str
    state: ApplicationState = ApplicationState.NEW
    version: int = 0

    # Domain state
    applicant_id: str | None = None
    requested_amount_usd: float | None = None

    # Analysis tracking
    credit_analysis_completed: bool = False
    credit_analysis_session_ids: list[str] = field(default_factory=list)
    fraud_score: float | None = None

    # Decision tracking
    recommendation: str | None = None
    confidence_score: float | None = None
    approved_amount_usd: float | None = None  # from DecisionGenerated.approved_amount_usd
    contributing_agent_sessions: list[str] = field(default_factory=list)

    # Known agent sessions (for causal chain validation)
    known_session_ids: set[str] = field(default_factory=set)

    @classmethod
    async def load(cls, store, application_id: str) -> "LoanApplicationAggregate":
        """Replay the event stream to rebuild aggregate state (Req 6.3)."""
        agg = cls(application_id=application_id)
        events = await store.load_stream(f"loan-{application_id}")
        for event in events:
            agg._apply(event)
        return agg

    @property
    def stream_id(self) -> str:
        return f"loan-{self.application_id}"

    # ------------------------------------------------------------------
    # Event application dispatcher (Req 6.4)
    # ------------------------------------------------------------------

    def _apply(self, event) -> None:
        """Dispatch to the appropriate _on_* handler."""
        # Support both StoredEvent objects and plain dicts
        if hasattr(event, "event_type"):
            et = event.event_type
            payload = event.payload
        else:
            et = event.get("event_type", "")
            payload = event.get("payload", {})

        self.version += 1
        handler = getattr(self, f"_on_{_snake(et)}", None)
        if handler:
            handler(payload)

    # ------------------------------------------------------------------
    # _on_* handlers (Req 6.5)
    # ------------------------------------------------------------------

    def _on_application_submitted(self, p: dict) -> None:
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = p.get("applicant_id")
        self.requested_amount_usd = p.get("requested_amount_usd")

    def _on_credit_analysis_requested(self, p: dict) -> None:
        self.state = ApplicationState.CREDIT_ANALYSIS_REQUESTED

    def _on_credit_analysis_completed(self, p: dict) -> None:
        self.state = ApplicationState.CREDIT_ANALYSIS_COMPLETE
        self.credit_analysis_completed = True
        session_id = p.get("session_id")
        if session_id:
            self.credit_analysis_session_ids.append(session_id)

    def _on_fraud_screening_requested(self, p: dict) -> None:
        self.state = ApplicationState.FRAUD_SCREENING_REQUESTED

    def _on_fraud_screening_completed(self, p: dict) -> None:
        self.state = ApplicationState.FRAUD_SCREENING_COMPLETE
        self.fraud_score = p.get("fraud_score")

    def _on_compliance_check_requested(self, p: dict) -> None:
        self.state = ApplicationState.COMPLIANCE_CHECK_REQUESTED

    def _on_compliance_check_completed(self, p: dict) -> None:
        self.state = ApplicationState.COMPLIANCE_CHECK_COMPLETE

    def _on_decision_generated(self, p: dict) -> None:
        self.state = ApplicationState.PENDING_DECISION
        self.recommendation = p.get("recommendation")
        self.confidence_score = p.get("confidence_score")
        self.approved_amount_usd = p.get("approved_amount_usd")
        self.contributing_agent_sessions = p.get("contributing_agent_sessions", [])

    def _on_human_review_requested(self, p: dict) -> None:
        self.state = ApplicationState.PENDING_HUMAN_REVIEW

    def _on_human_review_completed(self, p: dict) -> None:
        decision = p.get("decision", "").upper()
        if decision == "APPROVE":
            self.state = ApplicationState.APPROVED
        else:
            self.state = ApplicationState.DECLINED

    def _on_application_approved(self, p: dict) -> None:
        self.state = ApplicationState.APPROVED

    def _on_application_declined(self, p: dict) -> None:
        self.state = ApplicationState.DECLINED

    def _on_application_withdrawn(self, p: dict) -> None:
        self.state = ApplicationState.WITHDRAWN

    def _on_agent_session_started(self, p: dict) -> None:
        session_id = p.get("session_id")
        if session_id:
            self.known_session_ids.add(session_id)

    def _on_credit_analysis_superseded(self, p: dict) -> None:
        # Reset credit analysis so a new one can be requested
        self.credit_analysis_completed = False

    # ------------------------------------------------------------------
    # Business rule enforcement (Reqs 8.1–8.5)
    # ------------------------------------------------------------------

    def assert_valid_transition(self, target: ApplicationState) -> None:
        """Raise DomainError if the transition is not allowed (Req 6.2)."""
        if self.state in TERMINAL_STATES:
            raise DomainError(
                f"Application {self.application_id} is in terminal state {self.state}",
                context={"state": self.state, "target": target},
            )
        allowed = VALID_TRANSITIONS.get(self.state, [])
        if target not in allowed:
            raise DomainError(
                f"Invalid transition {self.state} → {target}. Allowed: {allowed}",
                context={"state": self.state, "target": target, "allowed": allowed},
            )

    def assert_no_duplicate_credit_analysis(self) -> None:
        """Prevent duplicate CreditAnalysisCompleted (Req 8.1)."""
        if self.credit_analysis_completed:
            raise DomainError(
                "CreditAnalysisCompleted already recorded for this application",
                context={"application_id": self.application_id},
            )

    def assert_confidence_floor(self, confidence_score: float | None, recommendation: str) -> None:
        """confidence < 0.6 must produce REFER recommendation (Req 8.2)."""
        if confidence_score is not None and confidence_score < 0.6:
            if recommendation.upper() != "REFER":
                raise DomainError(
                    f"confidence_score {confidence_score} < 0.6 requires recommendation=REFER, got {recommendation}",
                    context={"confidence_score": confidence_score, "recommendation": recommendation},
                )

    def assert_approved_amount_cap(self, approved_amount_usd: float) -> None:
        """approved_amount_usd must not exceed requested_amount_usd (Req 8.5)."""
        if self.requested_amount_usd is not None and approved_amount_usd > self.requested_amount_usd:
            raise DomainError(
                f"approved_amount_usd {approved_amount_usd} exceeds requested {self.requested_amount_usd}",
                context={
                    "approved": approved_amount_usd,
                    "requested": self.requested_amount_usd,
                },
            )

    def assert_causal_chain(self, contributing_sessions: list[str]) -> None:
        """All contributing sessions must be known to this aggregate (Req 8.4)."""
        unknown = [s for s in contributing_sessions if s not in self.known_session_ids]
        if unknown:
            raise DomainError(
                f"Unknown contributing_agent_sessions: {unknown}",
                context={"unknown": unknown, "known": list(self.known_session_ids)},
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _snake(event_type: str) -> str:
    """Convert PascalCase event type to snake_case handler name."""
    import re
    s = re.sub(r"(?<!^)(?=[A-Z])", "_", event_type).lower()
    return s
