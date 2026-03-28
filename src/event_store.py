# -*- coding: utf-8 -*-
"""
src/event_store.py -- PostgreSQL-backed EventStore

Events are passed as BaseEvent Pydantic model instances.
append() returns the new stream version (int) after the append.
load_stream() / load_all() return StoredEvent instances.
"""
from __future__ import annotations

import json
from collections import defaultdict as _defaultdict
from datetime import datetime as _datetime, timezone as _timezone
from typing import AsyncGenerator
from uuid import UUID, uuid4 as _uuid4

import asyncpg
import asyncio as _asyncio

from src.models.events import BaseEvent, StoredEvent
from src.models.exceptions import OptimisticConcurrencyError


def _normalise_event(event) -> dict:
    """
    Coerce a BaseEvent instance or a raw dict into a plain dict with the
    keys expected by the append() insert loop:
      event_id, event_type, event_version, payload, metadata
    """
    if isinstance(event, dict):
        payload = event.get("payload") or {}
        return {
            "event_id":      event.get("event_id") or str(_uuid4()),
            "event_type":    event["event_type"],
            "event_version": event.get("event_version", 1),
            "payload":       payload,
            "metadata":      event.get("metadata") or {},
        }
    # BaseEvent (Pydantic model)
    payload = event.payload if hasattr(event, "payload") and event.payload else event.to_payload()
    return {
        "event_id":      str(event.event_id),
        "event_type":    event.event_type,
        "event_version": event.event_version,
        "payload":       payload,
        "metadata":      event.metadata if isinstance(event.metadata, dict) else {},
    }


class EventStore:
    """Append-only PostgreSQL event store. All agents and projections use this class."""

    def __init__(self, db_url: str, upcaster_registry=None):
        self.db_url = db_url
        self.upcasters = upcaster_registry
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self._pool is not None:
            await self._pool.close()
        self._pool = await asyncpg.create_pool(
            self.db_url,
            min_size=2,
            max_size=10,
            command_timeout=30.0,   # per-query timeout — prevents stuck connections
            timeout=5.0,            # connection acquisition timeout
        )
        if self._pool is None:
            raise RuntimeError("Failed to create asyncpg connection pool")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def stream_version(self, stream_id: str) -> int:
        """Returns current version, or -1 if stream doesn't exist."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
            return row["current_version"] if row else -1

    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        aggregate_type: str | None = None,
    ) -> int:
        """
        Atomically append Pydantic BaseEvent instances to a stream with OCC.

        Returns:
            New current_version of the stream after the append (int).

        Raises:
            OptimisticConcurrencyError: if actual version != expected_version,
                or if a serializable conflict is detected by PostgreSQL.
        """
        if not events:
            raise ValueError("events list must not be empty")

        agg_type = aggregate_type or stream_id.split("-")[0]

        async with self._pool.acquire() as conn:
            try:
                async with conn.transaction(isolation="serializable"):
                    # 1. Lock stream row — serialises concurrent appends
                    row = await conn.fetchrow(
                        "SELECT current_version FROM event_streams "
                        "WHERE stream_id = $1 FOR UPDATE",
                        stream_id,
                    )
                    current = row["current_version"] if row else -1

                    # 2. OCC check
                    if current != expected_version:
                        raise OptimisticConcurrencyError(stream_id, expected_version, current)

                    # 3. Create stream row if new
                    if row is None:
                        await conn.execute(
                            "INSERT INTO event_streams(stream_id, aggregate_type, current_version)"
                            " VALUES($1, $2, 0)",
                            stream_id,
                            agg_type,
                        )

                    # 4. Build shared metadata
                    meta: dict = {}
                    if correlation_id:
                        meta["correlation_id"] = correlation_id
                    if causation_id:
                        meta["causation_id"] = causation_id

                    # Normalise: accept both BaseEvent instances and raw dicts
                    # (agents in base_agent.py pass dicts with event_type/payload keys)
                    normalised = [_normalise_event(e) for e in events]

                    # 5. Insert each event
                    start = 1 if expected_version == -1 else expected_version + 1
                    for i, event in enumerate(normalised):
                        pos = start + i
                        merged_meta = {**event["metadata"], **meta}
                        await conn.execute(
                            "INSERT INTO events"
                            "(event_id, stream_id, stream_position, event_type,"
                            " event_version, payload, metadata)"
                            " VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb)",
                            event["event_id"],
                            stream_id,
                            pos,
                            event["event_type"],
                            event["event_version"],
                            json.dumps(event["payload"], default=str),
                            json.dumps(merged_meta, default=str),
                        )

                    # 6. Update stream version
                    new_version = start + len(normalised) - 1
                    await conn.execute(
                        "UPDATE event_streams SET current_version=$1 WHERE stream_id=$2",
                        new_version,
                        stream_id,
                    )

                    # 7. Insert outbox rows (same transaction)
                    for event in normalised:
                        await conn.execute(
                            "INSERT INTO outbox(id, event_id, destination, payload)"
                            " VALUES($1,$2,$3,$4::jsonb)",
                            _uuid4(),
                            event["event_id"],
                            "default",
                            json.dumps(event["payload"], default=str),
                        )

                    # 8. Signal listeners — NOTIFY is held until commit, so it fires
                    #    if and only if the transaction succeeds (no spurious wakeups).
                    await conn.execute("NOTIFY new_events")

                    return new_version

            except asyncpg.SerializationError:
                # PostgreSQL serializable isolation detected a concurrent conflict.
                # Re-read the actual version and surface it as our domain error.
                actual = await conn.fetchval(
                    "SELECT current_version FROM event_streams WHERE stream_id = $1",
                    stream_id,
                )
                raise OptimisticConcurrencyError(
                    stream_id,
                    expected_version,
                    actual if actual is not None else -1,
                )

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        """
        Load events from a stream in stream_position order.
        Returns StoredEvent instances with upcasting applied if a registry is set.
        Returns an empty list if the stream does not exist.
        """
        async with self._pool.acquire() as conn:
            q = (
                "SELECT event_id, stream_id, stream_position, global_position,"
                " event_type, event_version, payload, metadata, recorded_at"
                " FROM events WHERE stream_id=$1 AND stream_position>=$2"
            )
            params: list = [stream_id, from_position]
            if to_position is not None:
                q += " AND stream_position<=$3"
                params.append(to_position)
            q += " ORDER BY stream_position ASC"
            rows = await conn.fetch(q, *params)

        events = [_row_to_stored_event(row) for row in rows]
        if self.upcasters:
            events = [self.upcasters.upcast(e) for e in events]
        return events

    async def load_all(
        self,
        from_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
    ) -> AsyncGenerator[StoredEvent, None]:
        """Async generator yielding StoredEvents in global_position order."""
        async with self._pool.acquire() as conn:
            pos = from_position
            while True:
                if event_types:
                    rows = await conn.fetch(
                        "SELECT event_id, stream_id, stream_position, global_position,"
                        " event_type, event_version, payload, metadata, recorded_at"
                        " FROM events WHERE global_position > $1"
                        " AND event_type = ANY($2::text[])"
                        " ORDER BY global_position ASC LIMIT $3",
                        pos, event_types, batch_size,
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT event_id, stream_id, stream_position, global_position,"
                        " event_type, event_version, payload, metadata, recorded_at"
                        " FROM events WHERE global_position > $1"
                        " ORDER BY global_position ASC LIMIT $2",
                        pos, batch_size,
                    )
                if not rows:
                    break
                for row in rows:
                    event = _row_to_stored_event(row)
                    if self.upcasters:
                        event = self.upcasters.upcast(event)
                    yield event
                pos = rows[-1]["global_position"]
                if len(rows) < batch_size:
                    break

    async def get_event(self, event_id: UUID) -> StoredEvent | None:
        """Load one event by UUID."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT event_id, stream_id, stream_position, global_position,"
                " event_type, event_version, payload, metadata, recorded_at"
                " FROM events WHERE event_id=$1",
                event_id,
            )
        return _row_to_stored_event(row) if row else None

    async def get_stream_metadata(self, stream_id: str) -> dict | None:
        """Return stream metadata dict or None if the stream does not exist."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT stream_id, aggregate_type, current_version, created_at, archived_at"
                " FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
        return dict(row) if row else None

    async def archive_stream(self, stream_id: str) -> None:
        """Soft-delete a stream by setting archived_at. Events are never deleted."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE event_streams SET archived_at = NOW() WHERE stream_id = $1",
                stream_id,
            )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _row_to_stored_event(row: asyncpg.Record) -> StoredEvent:
    payload = row["payload"]
    metadata = row["metadata"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    return StoredEvent(
        event_id=row["event_id"],
        stream_id=row["stream_id"],
        stream_position=row["stream_position"],
        global_position=row["global_position"],
        event_type=row["event_type"],
        event_version=row["event_version"],
        payload=dict(payload) if payload else {},
        metadata=dict(metadata) if metadata else {},
        recorded_at=row["recorded_at"],
    )


# ---------------------------------------------------------------------------
# UpcasterRegistry
# ---------------------------------------------------------------------------

class UpcasterRegistry:
    """
    Transforms old event versions to current versions on load.
    Upcasters are PURE functions — they never write to the database.
    Works with StoredEvent instances.
    """

    def __init__(self):
        self._upcasters: dict[tuple[str, int], callable] = {}

    def register(self, event_type: str, from_version: int):
        """Decorator — registers fn as upcaster from event_type@from_version."""
        def decorator(fn):
            self._upcasters[(event_type, from_version)] = fn
            return fn
        return decorator

    def upcast(self, event: StoredEvent) -> StoredEvent:
        """Apply chain of upcasters until latest version reached."""
        current = event
        v = event.event_version
        while (current.event_type, v) in self._upcasters:
            new_payload = self._upcasters[(current.event_type, v)](dict(current.payload))
            current = current.with_payload(new_payload, version=v + 1)
            v += 1
        return current


# ---------------------------------------------------------------------------
# InMemoryEventStore — for tests / local dev without a database
# ---------------------------------------------------------------------------

class InMemoryEventStore:
    """
    Asyncio-safe in-memory event store with the same interface as EventStore.
    Accepts BaseEvent instances; returns StoredEvent instances.
    """

    def __init__(self, upcaster_registry=None):
        self.upcasters = upcaster_registry
        self._streams: dict[str, list[StoredEvent]] = _defaultdict(list)
        self._versions: dict[str, int] = {}
        self._global: list[StoredEvent] = []
        self._checkpoints: dict[str, int] = {}
        self._locks: dict[str, _asyncio.Lock] = _defaultdict(_asyncio.Lock)

    async def stream_version(self, stream_id: str) -> int:
        return self._versions.get(stream_id, -1)

    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent | dict],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        aggregate_type: str | None = None,
    ) -> int:
        """
        Accepts either BaseEvent instances or plain dicts (legacy/test usage).
        Returns the new stream version (1-based, matching the real EventStore).
        """
        async with self._locks[stream_id]:
            current = self._versions.get(stream_id, -1)
            if current != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current)

            meta: dict = {}
            if correlation_id:
                meta["correlation_id"] = correlation_id
            if causation_id:
                meta["causation_id"] = causation_id

            # 1-based: first event in a new stream gets position 1
            start = expected_version + 1 + 1  # next position after current version
            # simpler: position = current_version + 1 + offset
            # current_version=-1 → first position=1, current_version=1 → next=2, etc.
            # 1-based: version=-1 (empty) → first pos=1; version=N → next pos=N+1
            next_pos = max(expected_version, 0) + 1
            for i, event in enumerate(events):
                pos = next_pos + i
                if isinstance(event, dict):
                    event_type = event["event_type"]
                    event_version = event.get("event_version", 1)
                    payload = dict(event.get("payload", {}))
                    event_id = _uuid4()
                else:
                    event_type = event.event_type
                    event_version = event.event_version
                    payload = event.payload if event.payload else event.to_payload()
                    event_id = event.event_id

                stored = StoredEvent(
                    event_id=event_id,
                    stream_id=stream_id,
                    stream_position=pos,
                    global_position=len(self._global) + 1,
                    event_type=event_type,
                    event_version=event_version,
                    payload=payload,
                    metadata={**meta},
                    recorded_at=_datetime.now(tz=_timezone.utc),
                )
                self._streams[stream_id].append(stored)
                self._global.append(stored)

            new_version = next_pos + len(events) - 1
            self._versions[stream_id] = new_version
            return new_version

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 1,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        """Returns StoredEvent instances, matching the real EventStore interface."""
        events = [
            e for e in self._streams.get(stream_id, [])
            if e.stream_position >= from_position
            and (to_position is None or e.stream_position <= to_position)
        ]
        result = sorted(events, key=lambda e: e.stream_position)
        if self.upcasters:
            result = [self.upcasters.upcast(e) for e in result]
        return result

    async def load_all(
        self,
        from_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
    ) -> AsyncGenerator[StoredEvent, None]:
        for e in self._global:
            if e.global_position >= from_position:
                if event_types is None or e.event_type in event_types:
                    yield e

    async def get_event(self, event_id: UUID) -> StoredEvent | None:
        for e in self._global:
            if e.event_id == event_id:
                return e
        return None

    async def get_stream_metadata(self, stream_id: str) -> dict | None:
        """Return basic stream metadata or None if the stream does not exist."""
        if stream_id not in self._versions:
            return None
        return {
            "stream_id": stream_id,
            "aggregate_type": stream_id.split("-")[0],
            "current_version": self._versions[stream_id],
            "created_at": None,
            "archived_at": None,
        }

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = position

    async def load_checkpoint(self, projection_name: str) -> int:
        return self._checkpoints.get(projection_name, 0)
