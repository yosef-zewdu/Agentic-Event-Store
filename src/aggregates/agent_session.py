"""
AgentSessionAggregate — enforces Gas Town ordering for agent sessions.

Gas Town ordering (Reqs 7.1, 7.6, 7.7):
  1. AgentSessionStarted must be the first event
  2. AgentContextLoaded must be the second event
  3. Any decision/analysis event before AgentContextLoaded raises DomainError
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from src.models.exceptions import DomainError


class SessionState(str, Enum):
    NEW = "NEW"
    STARTED = "STARTED"
    CONTEXT_LOADED = "CONTEXT_LOADED"
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"


# Events that require context to be loaded first
_DECISION_EVENTS = {
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "ComplianceCheckCompleted",
    "DecisionGenerated",
    "ComplianceRulePassed",
    "ComplianceRuleFailed",
}


@dataclass
class AgentSessionAggregate:
    """
    Aggregate root for an agent session.

    Enforces Gas Town ordering: Started → ContextLoaded → decisions.
    """

    session_id: str
    state: SessionState = SessionState.NEW
    version: int = 0

    # From AgentSessionStarted
    agent_id: str | None = None
    agent_type: str | None = None

    # From AgentContextLoaded (Req 7.2)
    context_source: str | None = None
    event_replay_from_position: int | None = None
    context_token_count: int | None = None
    model_version: str | None = None

    # Output events recorded during session
    output_event_types: list[str] = field(default_factory=list)

    @classmethod
    async def load(cls, store, session_id: str) -> "AgentSessionAggregate":
        """Replay the event stream to rebuild aggregate state."""
        agg = cls(session_id=session_id)
        events = await store.load_stream(f"session-{session_id}")
        for event in events:
            agg._apply(event)
        return agg

    @property
    def stream_id(self) -> str:
        return f"session-{self.session_id}"

    def _apply(self, event) -> None:
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
    # _on_* handlers
    # ------------------------------------------------------------------

    def _on_agent_session_started(self, p: dict) -> None:
        self.state = SessionState.STARTED
        self.agent_id = p.get("agent_id")
        self.agent_type = p.get("agent_type")

    def _on_agent_context_loaded(self, p: dict) -> None:
        self.state = SessionState.CONTEXT_LOADED
        self.context_source = p.get("context_source")
        self.event_replay_from_position = p.get("event_replay_from_position")
        self.context_token_count = p.get("context_token_count")
        self.model_version = p.get("model_version")

    def _on_agent_session_closed(self, p: dict) -> None:
        self.state = SessionState.CLOSED

    def _on_credit_analysis_completed(self, p: dict) -> None:
        self.output_event_types.append("CreditAnalysisCompleted")
        # Enforce model version consistency (Req 7.3)
        self._assert_model_version_consistent(p.get("model_version"))

    def _on_fraud_screening_completed(self, p: dict) -> None:
        self.output_event_types.append("FraudScreeningCompleted")

    def _on_decision_generated(self, p: dict) -> None:
        self.output_event_types.append("DecisionGenerated")

    # ------------------------------------------------------------------
    # Business rule enforcement (Reqs 7.1, 7.3, 7.6, 7.7)
    # ------------------------------------------------------------------

    def assert_gas_town_ordering(self, next_event_type: str) -> None:
        """Enforce Gas Town ordering — context must be loaded before decisions."""
        if next_event_type == "AgentSessionStarted":
            if self.state != SessionState.NEW:
                raise DomainError(
                    "AgentSessionStarted can only be the first event",
                    context={"state": self.state},
                )
        elif next_event_type == "AgentContextLoaded":
            if self.state != SessionState.STARTED:
                raise DomainError(
                    "AgentContextLoaded must follow AgentSessionStarted",
                    context={"state": self.state},
                )
        elif next_event_type in _DECISION_EVENTS:
            if self.state not in (SessionState.CONTEXT_LOADED, SessionState.ACTIVE):
                raise DomainError(
                    f"{next_event_type} requires AgentContextLoaded first (Gas Town ordering)",
                    context={"state": self.state, "event_type": next_event_type},
                )

    def assert_not_closed(self) -> None:
        if self.state == SessionState.CLOSED:
            raise DomainError(
                f"Session {self.session_id} is already closed",
                context={"session_id": self.session_id},
            )

    def _assert_model_version_consistent(self, model_version: str | None) -> None:
        """Output events must use the same model version as context load (Req 7.3)."""
        if model_version and self.model_version and model_version != self.model_version:
            raise DomainError(
                f"Model version mismatch: context loaded with {self.model_version}, "
                f"output event uses {model_version}",
                context={
                    "context_model_version": self.model_version,
                    "output_model_version": model_version,
                },
            )


def _snake(event_type: str) -> str:
    import re
    return re.sub(r"(?<!^)(?=[A-Z])", "_", event_type).lower()
