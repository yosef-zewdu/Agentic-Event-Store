"""
src/outbox/processor.py — OutboxProcessor

Background service that delivers unpublished outbox rows to downstream systems.

The transactional outbox pattern guarantees that every event appended to the
EventStore also produces a row in the `outbox` table (same transaction).  This
processor reads those rows and "delivers" them — currently by logging to
structured output.  Real delivery is pluggable via the DeliveryHandler protocol.

Design mirrors ProjectionDaemon:
  - poll loop with configurable interval
  - SKIP LOCKED so multiple instances can run safely in parallel
  - per-row exception handling — the loop never crashes
  - published_at set atomically with marking the row as delivered
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Protocol, runtime_checkable

import asyncpg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DeliveryHandler protocol — plug in real messaging here
# ---------------------------------------------------------------------------

@runtime_checkable
class DeliveryHandler(Protocol):
    """Interface for downstream delivery backends."""

    async def deliver(self, row_id: str, destination: str, payload: dict) -> None:
        """Deliver a single outbox message. Raise on failure."""
        ...


class LoggingDeliveryHandler:
    """Default handler — logs each outbox row. Acts as an audit trail."""

    async def deliver(self, row_id: str, destination: str, payload: dict) -> None:
        logger.info(
            "OutboxProcessor: delivered outbox_id=%s destination=%s payload_keys=%s",
            row_id,
            destination,
            list(payload.keys()),
        )


# ---------------------------------------------------------------------------
# OutboxProcessor
# ---------------------------------------------------------------------------

class OutboxProcessor:
    """
    Background service that polls the outbox table and delivers unpublished rows.

    Usage::

        processor = OutboxProcessor(pool=asyncpg_pool)
        await processor.run_forever(poll_interval_ms=500)
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        handler: DeliveryHandler | None = None,
        poll_interval_ms: int = 500,
        batch_size: int = 100,
        max_attempts: int = 5,
    ) -> None:
        self._pool = pool
        self._handler: DeliveryHandler = handler or LoggingDeliveryHandler()
        self._poll_interval_ms = poll_interval_ms
        self._batch_size = batch_size
        self._max_attempts = max_attempts
        self._running = False

    def stop(self) -> None:
        """Signal the poll loop to stop after the current batch."""
        self._running = False

    async def run_forever(self, poll_interval_ms: int | None = None) -> None:
        """Main poll loop. Runs until stop() is called."""
        if poll_interval_ms is not None:
            self._poll_interval_ms = poll_interval_ms

        self._running = True
        logger.info("OutboxProcessor: starting (interval=%dms)", self._poll_interval_ms)

        while self._running:
            try:
                delivered = await self._process_batch()
                if delivered:
                    logger.debug("OutboxProcessor: delivered %d rows", delivered)
            except Exception:
                logger.exception("OutboxProcessor: unexpected error in _process_batch")
            await asyncio.sleep(self._poll_interval_ms / 1000)

        logger.info("OutboxProcessor: stopped")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _process_batch(self) -> int:
        """
        Fetch up to batch_size unpublished rows, deliver each, and mark as published.

        Uses SELECT … FOR UPDATE SKIP LOCKED so multiple processor instances
        can run in parallel without double-delivering.

        Returns the number of rows successfully delivered.
        """
        delivered = 0

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, event_id, destination, payload, attempts
                FROM outbox
                WHERE published_at IS NULL
                  AND attempts < $1
                ORDER BY created_at ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
                """,
                self._max_attempts,
                self._batch_size,
            )

            for row in rows:
                row_id = str(row["id"])
                destination = row["destination"]
                payload = row["payload"]
                if isinstance(payload, str):
                    payload = json.loads(payload)

                try:
                    await self._handler.deliver(row_id, destination, dict(payload))
                    # Mark as published in the same connection (not a new transaction)
                    await conn.execute(
                        """
                        UPDATE outbox
                        SET published_at = NOW(),
                            attempts     = attempts + 1
                        WHERE id = $1
                        """,
                        row["id"],
                    )
                    delivered += 1

                except Exception as exc:
                    # Increment attempt counter; row will be retried on next poll
                    await conn.execute(
                        "UPDATE outbox SET attempts = attempts + 1 WHERE id = $1",
                        row["id"],
                    )
                    logger.error(
                        "OutboxProcessor: delivery failed outbox_id=%s destination=%s "
                        "attempt=%d error=%s",
                        row_id,
                        destination,
                        row["attempts"] + 1,
                        exc,
                    )

        return delivered
