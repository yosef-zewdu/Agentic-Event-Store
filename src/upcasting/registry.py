"""
UpcasterRegistry — stub implementation.

Registers upcaster functions keyed by (event_type, from_version) and applies
them in version order at read time. The stored events table is never modified.

Concrete upcasters (CreditAnalysisCompleted v1→v2, DecisionGenerated v1→v2)
are registered in Phase 4 (src/upcasting/upcasters.py).
"""

from __future__ import annotations

from collections.abc import Callable

from src.models.events import StoredEvent


class UpcasterRegistry:
    """
    Central registry for event schema migration functions.

    Usage:
        registry = UpcasterRegistry()

        @registry.register("CreditAnalysisCompleted", from_version=1)
        def upcast_credit_v1_to_v2(payload: dict) -> dict:
            return {**payload, "model_version": None, "confidence_score": None}
    """

    def __init__(self) -> None:
        self._upcasters: dict[tuple[str, int], Callable[[dict], dict]] = {}

    def register(self, event_type: str, from_version: int) -> Callable:
        """Decorator — registers fn as upcaster from event_type@from_version."""
        def decorator(fn: Callable[[dict], dict]) -> Callable[[dict], dict]:
            self._upcasters[(event_type, from_version)] = fn
            return fn
        return decorator

    def upcast(self, event: StoredEvent) -> StoredEvent:
        """
        Apply the upcaster chain to event, returning a new StoredEvent at the
        latest schema version. The original DB row is never touched.
        """
        current = event
        v = event.event_version
        while (current.event_type, v) in self._upcasters:
            new_payload = self._upcasters[(current.event_type, v)](current.payload)
            current = current.with_payload(new_payload, version=v + 1)
            v += 1
        return current


# Module-level singleton — imported by EventStore and registered by upcasters.py
registry = UpcasterRegistry()
