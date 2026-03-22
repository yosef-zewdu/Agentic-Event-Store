"""
ComplianceRecordAggregate — tracks mandatory compliance checks for a loan application.

Separate aggregate from LoanApplication to allow independent concurrent writes
(Req 29.1 — merging would create concurrent-write coupling on the same stream).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from src.models.exceptions import DomainError


class ComplianceState(str, Enum):
    NEW = "NEW"
    CHECK_REQUESTED = "CHECK_REQUESTED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    BLOCKED = "BLOCKED"


@dataclass
class RuleVerdict:
    rule_id: str
    rule_version: str
    passed: bool
    regulation_set_version: str
    evidence_hash: str
    failure_reason: str | None = None


@dataclass
class ComplianceRecordAggregate:
    """
    Tracks compliance rule verdicts and regulation set versions for an application.
    """

    application_id: str
    state: ComplianceState = ComplianceState.NEW
    version: int = 0

    # Rule tracking
    rule_verdicts: dict[str, RuleVerdict] = field(default_factory=dict)
    regulation_set_version: str | None = None

    # Derived
    all_rules_passed: bool = False
    has_blocking_failure: bool = False

    @classmethod
    async def load(cls, store, application_id: str) -> "ComplianceRecordAggregate":
        """Replay the compliance stream to rebuild state."""
        agg = cls(application_id=application_id)
        events = await store.load_stream(f"compliance-{application_id}")
        for event in events:
            agg._apply(event)
        return agg

    @property
    def stream_id(self) -> str:
        return f"compliance-{self.application_id}"

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

    def _on_compliance_check_requested(self, p: dict) -> None:
        self.state = ComplianceState.CHECK_REQUESTED

    def _on_compliance_rule_passed(self, p: dict) -> None:
        self.state = ComplianceState.IN_PROGRESS
        rule_id = p.get("rule_id", "")
        self.regulation_set_version = p.get("regulation_set_version")
        self.rule_verdicts[rule_id] = RuleVerdict(
            rule_id=rule_id,
            rule_version=p.get("rule_version", ""),
            passed=True,
            regulation_set_version=p.get("regulation_set_version", ""),
            evidence_hash=p.get("evidence_hash", ""),
        )
        self._recompute_status()

    def _on_compliance_rule_failed(self, p: dict) -> None:
        self.state = ComplianceState.IN_PROGRESS
        rule_id = p.get("rule_id", "")
        self.regulation_set_version = p.get("regulation_set_version")
        self.rule_verdicts[rule_id] = RuleVerdict(
            rule_id=rule_id,
            rule_version=p.get("rule_version", ""),
            passed=False,
            regulation_set_version=p.get("regulation_set_version", ""),
            evidence_hash=p.get("evidence_hash", ""),
            failure_reason=p.get("failure_reason"),
        )
        self.has_blocking_failure = True
        self._recompute_status()

    def _on_compliance_check_completed(self, p: dict) -> None:
        if self.has_blocking_failure:
            self.state = ComplianceState.BLOCKED
        else:
            self.state = ComplianceState.COMPLETED
        self.all_rules_passed = not self.has_blocking_failure

    # ------------------------------------------------------------------
    # Business rule helpers
    # ------------------------------------------------------------------

    def assert_compliance_cleared(self) -> None:
        """Raise DomainError if compliance is not fully cleared (Req 8.3)."""
        if self.state != ComplianceState.COMPLETED or not self.all_rules_passed:
            raise DomainError(
                f"Compliance not cleared for application {self.application_id}",
                context={
                    "state": self.state,
                    "has_blocking_failure": self.has_blocking_failure,
                    "rules_evaluated": list(self.rule_verdicts.keys()),
                },
            )

    def _recompute_status(self) -> None:
        """Recompute all_rules_passed from current verdicts."""
        if self.rule_verdicts:
            self.all_rules_passed = all(v.passed for v in self.rule_verdicts.values())


def _snake(event_type: str) -> str:
    import re
    return re.sub(r"(?<!^)(?=[A-Z])", "_", event_type).lower()
