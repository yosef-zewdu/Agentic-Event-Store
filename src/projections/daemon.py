"""
src/projections/daemon.py — ProjectionDaemon

Continuously polls the events table and routes new events to registered projections.

Requirements covered:
  - Req 12.1: poll from lowest checkpoint, route to subscribed projections
  - Req 12.2: fault tolerance — catch exceptions, retry, skip after exhaustion, never crash
  - Req 12.3: checkpoint update and projection write in the same DB transaction
  - Req 12.4: get_lag(projection_name) -> int (milliseconds)
  - Req 12.5: get_all_lags() -> dict[str, int]
  - Req 12.6: resume from last persisted checkpoint on restart (not from 0)
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable

import asyncpg

from src.event_store import EventStore
from src.models.events import StoredEvent

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Projection protocol — every projection must implement this interface
# ---------------------------------------------------------------------------

@runtime_checkable
class Projection(Protocol):
    """Interface that every projection must satisfy."""

    @property
    def name(self) -> str:
        """Unique name used as the projection_checkpoints PK."""
        ...

    def subscribes_to(self, event_type: str) -> bool:
        """Return True if this projection wants to handle this event type."""
        ...

    async def handle(self, event: StoredEvent, conn: asyncpg.Connection) -> None:
        """
        Process a single event.

        IMPORTANT: `conn` is an open asyncpg connection that is already inside a
        transaction managed by the daemon.  The projection MUST perform its writes
        using this connection so that the checkpoint update and the projection write
        are committed atomically (Req 12.3).
        """
        ...


# ---------------------------------------------------------------------------
# ProjectionDaemon
# ---------------------------------------------------------------------------

class ProjectionDaemon:
    """
    Background daemon that polls the events table and routes events to projections.

    Usage::

        daemon = ProjectionDaemon(store=event_store, pool=asyncpg_pool)
        daemon.register(my_projection)
        await daemon.run_forever(poll_interval_ms=100)
    """

    def __init__(
        self,
        store: EventStore,
        pool: asyncpg.Pool,
        max_retries: int = 3,
    ) -> None:
        self._store = store
        self._pool = pool
        self._max_retries = max_retries
        self._running = False

        # name -> Projection
        self._projections: dict[str, Projection] = {}

        # name -> current in-memory checkpoint (global_position of last processed event)
        self._checkpoints: dict[str, int] = {}

        # (projection_name, event_id_str) -> retry count
        self._retry_counts: dict[tuple[str, str], int] = defaultdict(int)

        # Lock that ensures only one _process_batch runs at a time, whether triggered
        # by LISTEN/NOTIFY or by the fallback poll loop.
        self._batch_lock: asyncio.Lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register(self, projection: Projection) -> None:
        """Register a projection with the daemon."""
        self._projections[projection.name] = projection

    def stop(self) -> None:
        """Signal the polling loop to stop after the current batch."""
        self._running = False

    async def run_forever(self, poll_interval_ms: int = 100) -> None:
        """
        Main loop (Req 12.1).

        Uses PostgreSQL LISTEN/NOTIFY for near-zero-latency event delivery.
        The poll loop continues as a fallback heartbeat — it catches any events
        missed during a NOTIFY connection blip and keeps the daemon self-healing.
        """
        self._running = True

        # Load persisted checkpoints so we resume from the right position (Req 12.6)
        await self._load_checkpoints()

        # Start LISTEN task in the background
        asyncio.create_task(self._listen_for_notifications())

        # Fallback poll loop — runs at the configured interval regardless of NOTIFY.
        # When NOTIFY is healthy this mostly finds zero new events (cheap no-op).
        while self._running:
            try:
                await self._run_batch_under_lock()
            except Exception:
                # Daemon-level errors (e.g. DB connectivity) are logged but never fatal
                logger.exception("ProjectionDaemon: unexpected error in _process_batch")
            await asyncio.sleep(poll_interval_ms / 1000)

    # ------------------------------------------------------------------
    # Lag monitoring (Reqs 12.4–12.5)
    # ------------------------------------------------------------------

    async def get_lag(self, projection_name: str) -> int:
        """
        Return lag in milliseconds between the store's latest event recorded_at
        and the recorded_at of the event at the projection's checkpoint position.

        Returns 0 if the projection is fully caught up or has no events to process.
        """
        async with self._pool.acquire() as conn:
            # Latest event timestamp in the store
            latest_row = await conn.fetchrow(
                "SELECT recorded_at FROM events ORDER BY global_position DESC LIMIT 1"
            )
            if latest_row is None:
                return 0

            latest_ts: datetime = latest_row["recorded_at"]

            # Timestamp of the event at the projection's checkpoint
            checkpoint = self._checkpoints.get(projection_name, 0)
            if checkpoint == 0:
                # Projection hasn't processed anything yet — lag = full store age
                checkpoint_ts_row = await conn.fetchrow(
                    "SELECT recorded_at FROM events ORDER BY global_position ASC LIMIT 1"
                )
                if checkpoint_ts_row is None:
                    return 0
                checkpoint_ts: datetime = checkpoint_ts_row["recorded_at"]
            else:
                checkpoint_ts_row = await conn.fetchrow(
                    "SELECT recorded_at FROM events WHERE global_position = $1",
                    checkpoint,
                )
                if checkpoint_ts_row is None:
                    return 0
                checkpoint_ts = checkpoint_ts_row["recorded_at"]

            # Ensure both are timezone-aware for subtraction
            if latest_ts.tzinfo is None:
                latest_ts = latest_ts.replace(tzinfo=timezone.utc)
            if checkpoint_ts.tzinfo is None:
                checkpoint_ts = checkpoint_ts.replace(tzinfo=timezone.utc)

            delta_ms = int((latest_ts - checkpoint_ts).total_seconds() * 1000)
            return max(delta_ms, 0)

    async def get_all_lags(self) -> dict[str, int]:
        """Return lag in milliseconds for every registered projection (Req 12.5)."""
        return {
            name: await self.get_lag(name)
            for name in self._projections
        }

    # ------------------------------------------------------------------
    # LISTEN/NOTIFY (primary delivery path)
    # ------------------------------------------------------------------

    async def _listen_for_notifications(self) -> None:
        """
        Hold a dedicated connection with LISTEN active for the daemon's lifetime.

        Reconnects with exponential backoff if the connection is lost.
        The fallback poll loop in run_forever() acts as a safety net during reconnection.
        """
        backoff = 1.0
        while self._running:
            try:
                async with self._pool.acquire() as conn:
                    await conn.add_listener("new_events", self._on_notify)
                    logger.info("ProjectionDaemon: LISTEN/NOTIFY active on 'new_events'")
                    backoff = 1.0  # reset on successful connection
                    while self._running:
                        await asyncio.sleep(1.0)
                    await conn.remove_listener("new_events", self._on_notify)
                    return  # clean shutdown
            except Exception:
                logger.error(
                    "ProjectionDaemon: LISTEN/NOTIFY connection lost — "
                    "retrying in %.1fs (poll loop continues as fallback)",
                    backoff,
                    exc_info=True,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)  # cap at 30s

    async def _on_notify(self, conn, pid: int, channel: str, payload: str) -> None:
        """Called by asyncpg when a NOTIFY new_events arrives."""
        if not self._batch_lock.locked():
            asyncio.create_task(self._run_batch_under_lock())

    async def _run_batch_under_lock(self) -> None:
        """Run one processing cycle under the batch lock (prevents concurrent batches)."""
        async with self._batch_lock:
            await self._process_batch()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _ensure_dlq_table(self) -> None:
        """Create the dead-letter table for persistently-failing projection events."""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS projection_failed_events (
                    projection_name TEXT        NOT NULL,
                    event_id        UUID        NOT NULL,
                    event_type      TEXT,
                    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    error_message   TEXT,
                    attempts        INT,
                    PRIMARY KEY (projection_name, event_id)
                )
            """)

    async def _load_checkpoints(self) -> None:
        """
        Load persisted checkpoints from projection_checkpoints table (Req 12.6).
        Also restores DLQ retry counts so previously-exhausted events are skipped
        immediately on restart rather than being retried from zero.
        """
        # Ensure DLQ table exists — _load_checkpoints may be called directly in tests
        await self._ensure_dlq_table()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT projection_name, last_position FROM projection_checkpoints"
            )
            for row in rows:
                name = row["projection_name"]
                if name in self._projections:
                    self._checkpoints[name] = row["last_position"]

            # Restore DLQ entries — mark them at max_retries so _dispatch skips them
            dlq_rows = await conn.fetch(
                "SELECT projection_name, event_id FROM projection_failed_events"
            )
            for row in dlq_rows:
                key = (row["projection_name"], str(row["event_id"]))
                self._retry_counts[key] = self._max_retries

    async def _process_batch(self) -> None:
        """
        Core processing loop for one poll cycle.

        1. Determine the minimum checkpoint across all projections.
        2. Fetch events from that position onwards.
        3. For each event, route to subscribed projections inside a per-event transaction.
        4. Checkpoint is updated inside the same transaction as the projection write.
        """
        if not self._projections:
            return

        # 1. Lowest checkpoint across all projections (Req 12.1)
        min_pos = self._min_checkpoint()

        # 2. Fetch and route events
        async for event in self._store.load_all(from_position=min_pos):
            for proj in self._projections.values():
                # Skip events the projection has already processed
                proj_checkpoint = self._checkpoints.get(proj.name, 0)
                if event.global_position <= proj_checkpoint:
                    continue

                if not proj.subscribes_to(event.event_type):
                    # Advance checkpoint even for unsubscribed events so the projection
                    # doesn't fall behind on events it doesn't care about.
                    await self._advance_checkpoint(proj.name, event.global_position)
                    continue

                await self._dispatch(proj, event)

    async def _dispatch(self, proj: Projection, event: StoredEvent) -> None:
        """
        Dispatch a single event to a projection with retry logic (Req 12.2).

        The projection write and checkpoint update happen in the same DB transaction
        (Req 12.3).  On failure the transaction is rolled back and the retry counter
        is incremented.  After max_retries the event is skipped — the daemon never
        crashes.
        """
        retry_key = (proj.name, str(event.event_id))

        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    # Distributed coordination (Req 0.6): acquire a projection-specific
                    # advisory lock so only one daemon node processes this projection
                    # at a time.  pg_try_advisory_xact_lock is transaction-scoped —
                    # it releases automatically at transaction end, no cleanup needed.
                    lock_id = hash(proj.name) & 0x7FFFFFFF
                    acquired = await conn.fetchval(
                        "SELECT pg_try_advisory_xact_lock($1)", lock_id
                    )
                    if not acquired:
                        # Another node holds the lock — skip; it will advance the checkpoint
                        return

                    # Projection writes using the transactional connection
                    await proj.handle(event, conn)

                    # Checkpoint update in the same transaction (Req 12.3)
                    await conn.execute(
                        """
                        INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
                        VALUES ($1, $2, NOW())
                        ON CONFLICT (projection_name)
                        DO UPDATE SET last_position = EXCLUDED.last_position,
                                      updated_at    = EXCLUDED.updated_at
                        """,
                        proj.name,
                        event.global_position,
                    )

            # Success — update in-memory checkpoint and clear retry counter
            self._checkpoints[proj.name] = event.global_position
            self._retry_counts.pop(retry_key, None)

        except Exception as exc:
            self._retry_counts[retry_key] += 1
            attempts = self._retry_counts[retry_key]

            logger.error(
                "ProjectionDaemon: projection=%s event_id=%s event_type=%s "
                "attempt=%d/%d error=%s",
                proj.name,
                event.event_id,
                event.event_type,
                attempts,
                self._max_retries,
                exc,
                exc_info=True,
            )

            if attempts >= self._max_retries:
                # Retry exhausted — write to DLQ, skip, and advance checkpoint (Req 12.2)
                logger.warning(
                    "ProjectionDaemon: skipping event_id=%s for projection=%s "
                    "after %d failed attempts — writing to DLQ",
                    event.event_id,
                    proj.name,
                    attempts,
                )
                async with self._pool.acquire() as dlq_conn:
                    await dlq_conn.execute(
                        """
                        INSERT INTO projection_failed_events
                            (projection_name, event_id, event_type, error_message, attempts)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (projection_name, event_id) DO UPDATE SET
                            error_message = EXCLUDED.error_message,
                            attempts      = EXCLUDED.attempts,
                            failed_at     = NOW()
                        """,
                        proj.name,
                        event.event_id,
                        event.event_type,
                        str(exc),
                        attempts,
                    )
                await self._advance_checkpoint(proj.name, event.global_position)
                self._retry_counts.pop(retry_key, None)
            # else: will be retried on the next poll cycle

    async def _advance_checkpoint(self, projection_name: str, position: int) -> None:
        """Persist a checkpoint update without a projection write (used for skips)."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (projection_name)
                DO UPDATE SET last_position = EXCLUDED.last_position,
                              updated_at    = EXCLUDED.updated_at
                """,
                projection_name,
                position,
            )
        self._checkpoints[projection_name] = position

    def _min_checkpoint(self) -> int:
        """Return the lowest checkpoint across all registered projections."""
        if not self._projections:
            return 0
        return min(
            self._checkpoints.get(name, 0)
            for name in self._projections
        )
