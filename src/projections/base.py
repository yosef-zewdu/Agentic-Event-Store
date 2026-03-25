"""
src/projections/base.py — BaseProjection abstract base class

All concrete projections (ApplicationSummary, AgentPerformanceLedger,
ComplianceAuditView) must inherit from this class.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

import asyncpg

from src.models.events import StoredEvent


class BaseProjection(ABC):
    """
    Abstract base class for all projections.

    Concrete projections must:
      - Set `name` as a class-level string attribute (used as projection_checkpoints PK)
      - Set `subscribed_event_types` as a list of event type strings
      - Implement `handle(event, conn)` to process a single event

    The `conn` parameter in `handle` is an open asyncpg connection already inside
    a transaction managed by the ProjectionDaemon.  All writes in `handle` MUST use
    this connection so that the projection write and checkpoint update are committed
    atomically (Req 12.3).
    """

    name: str
    subscribed_event_types: list[str]

    def subscribes_to(self, event_type: str) -> bool:
        """Return True if this projection handles the given event type."""
        return event_type in self.subscribed_event_types

    @abstractmethod
    async def handle(self, event: StoredEvent, conn: asyncpg.Connection) -> None:
        """
        Process a single event.

        Must be idempotent — the same event may be delivered more than once
        (at-least-once delivery).  Use INSERT ... ON CONFLICT DO UPDATE (upsert)
        for all writes.
        """
        ...
