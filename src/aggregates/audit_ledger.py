"""
AuditLedgerAggregate — governance stream, append-only, no business state machine.

This aggregate is intentionally minimal: it only tracks the audit event log
for an entity and enforces append-only semantics (Req 14.6).
No state transitions — every event is valid at any time.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from src.models.exceptions import DomainError


@dataclass
class AuditLedgerAggregate:
    """
    Append-only audit ledger for an entity (application, agent, etc.).

    No state machine — any audit event can be appended at any time.
    Enforces that events are never deleted or modified (Req 14.6).
    """

    entity_id: str
    entity_type: str
    version: int = 0

    # Audit trail
    integrity_checks: list[dict] = field(default_factory=list)
    last_integrity_hash: str = ""
    events_verified_count: int = 0

    @classmethod
    async def load(cls, store, entity_id: str, entity_type: str = "application") -> "AuditLedgerAggregate":
        """Replay the audit stream to rebuild state."""
        agg = cls(entity_id=entity_id, entity_type=entity_type)
        events = await store.load_stream(f"audit-{entity_type}-{entity_id}")
        for event in events:
            agg._apply(event)
        return agg

    @property
    def stream_id(self) -> str:
        return f"audit-{self.entity_type}-{self.entity_id}"

    def _apply(self, event) -> None:
        if hasattr(event, "event_type"):
            et = event.event_type
            payload = event.payload
        else:
            et = event.get("event_type", "")
            payload = event.get("payload", {})

        self.version += 1
        if et == "AuditIntegrityCheckRun":
            self._on_audit_integrity_check_run(payload)

    def _on_audit_integrity_check_run(self, p: dict) -> None:
        self.last_integrity_hash = p.get("integrity_hash", "")
        self.events_verified_count = p.get("events_verified_count", 0)
        self.integrity_checks.append({
            "check_timestamp": p.get("check_timestamp"),
            "integrity_hash": p.get("integrity_hash"),
            "previous_hash": p.get("previous_hash"),
            "events_verified_count": p.get("events_verified_count"),
        })

    def assert_append_only(self) -> None:
        """Audit streams are append-only — raise if any modification is attempted."""
        raise DomainError(
            "Audit ledger streams are append-only and cannot be modified",
            context={"entity_id": self.entity_id, "entity_type": self.entity_type},
        )
