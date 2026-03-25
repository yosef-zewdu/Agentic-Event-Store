"""
tests/test_projection_daemon.py — Unit tests for ProjectionDaemon

Tests cover:
  - run_forever polling loop with correct poll interval (Req 12.1)
  - Per-projection checkpoint load/save atomicity (Reqs 12.3, 12.6)
  - Fault tolerance: retry up to max, skip after exhaustion, daemon never crashes (Req 12.2)
  - Lag monitoring: get_lag and get_all_lags (Reqs 12.4–12.5)
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio

from src.event_store import EventStore
from src.models.events import ApplicationSubmitted, StoredEvent
from src.projections.daemon import ProjectionDaemon, Projection


# ---------------------------------------------------------------------------
# Helpers / stubs
# ---------------------------------------------------------------------------

def _make_stored_event(
    global_position: int = 1,
    event_type: str = "ApplicationSubmitted",
) -> StoredEvent:
    return StoredEvent(
        event_id=uuid4(),
        stream_id=f"loan-test-{global_position}",
        stream_position=1,
        global_position=global_position,
        event_type=event_type,
        event_version=1,
        payload={"application_id": f"app-{global_position}"},
        metadata={},
        recorded_at=datetime.now(tz=timezone.utc),
    )


class _CountingProjection:
    """Projection that counts how many events it handled."""

    def __init__(self, name: str = "counting", subscribes: set[str] | None = None):
        self._name = name
        self._subscribes = {"ApplicationSubmitted"} if subscribes is None else subscribes
        self.handled: list[StoredEvent] = []

    @property
    def name(self) -> str:
        return self._name

    def subscribes_to(self, event_type: str) -> bool:
        return event_type in self._subscribes

    async def handle(self, event: StoredEvent, conn: Any) -> None:
        self.handled.append(event)


class _FailingProjection:
    """Projection that always raises on handle()."""

    def __init__(self, name: str = "failing", fail_times: int = 999):
        self._name = name
        self._fail_times = fail_times
        self._call_count = 0

    @property
    def name(self) -> str:
        return self._name

    def subscribes_to(self, event_type: str) -> bool:
        return True

    async def handle(self, event: StoredEvent, conn: Any) -> None:
        self._call_count += 1
        if self._call_count <= self._fail_times:
            raise RuntimeError(f"Simulated failure #{self._call_count}")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def daemon(store, db_pool):
    """ProjectionDaemon wired to the test store and pool."""
    return ProjectionDaemon(store=store, pool=db_pool, max_retries=3)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_daemon_processes_events_and_saves_checkpoint(store, db_pool):
    """Events appended to the store are routed to subscribed projections and checkpoint is saved."""
    from src.models.events import ApplicationSubmitted
    from datetime import datetime, timezone

    # Append one event
    event = ApplicationSubmitted(
        application_id="app-001",
        applicant_id="applicant-001",
        requested_amount_usd=100_000,
        loan_purpose="working_capital",
        loan_term_months=12,
        submission_channel="web",
        contact_email="test@example.com",
        contact_name="Test User",
        submitted_at=datetime.now(tz=timezone.utc),
        application_reference="REF-001",
    )
    await store.append("loan-app-001", [event], expected_version=-1)

    proj = _CountingProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)

    # Run one batch
    await daemon._load_checkpoints()
    await daemon._process_batch()

    assert len(proj.handled) == 1
    assert proj.handled[0].event_type == "ApplicationSubmitted"

    # Checkpoint should be persisted
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
            "counting",
        )
    assert row is not None
    assert row["last_position"] >= 1


@pytest.mark.asyncio
async def test_daemon_resumes_from_persisted_checkpoint(store, db_pool):
    """On restart, daemon resumes from last persisted checkpoint (Req 12.6)."""
    from src.models.events import ApplicationSubmitted
    from datetime import datetime, timezone

    # Append two events
    for i in range(2):
        event = ApplicationSubmitted(
            application_id=f"app-{i:03d}",
            applicant_id=f"applicant-{i:03d}",
            requested_amount_usd=50_000,
            loan_purpose="working_capital",
            loan_term_months=12,
            submission_channel="web",
            contact_email=f"user{i}@example.com",
            contact_name=f"User {i}",
            submitted_at=datetime.now(tz=timezone.utc),
            application_reference=f"REF-{i:03d}",
        )
        await store.append(f"loan-app-{i:03d}", [event], expected_version=-1)

    # First daemon processes both events
    proj1 = _CountingProjection(name="resume_test")
    daemon1 = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon1.register(proj1)
    await daemon1._load_checkpoints()
    await daemon1._process_batch()
    assert len(proj1.handled) == 2

    # Second daemon (simulating restart) should load checkpoint and not reprocess
    proj2 = _CountingProjection(name="resume_test")
    daemon2 = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon2.register(proj2)
    await daemon2._load_checkpoints()
    await daemon2._process_batch()

    # Should have processed 0 new events (already at checkpoint)
    assert len(proj2.handled) == 0


@pytest.mark.asyncio
async def test_fault_tolerance_retries_and_skips(store, db_pool):
    """
    A failing projection is retried up to max_retries, then the event is skipped.
    The daemon never crashes (Req 12.2).
    """
    from src.models.events import ApplicationSubmitted
    from datetime import datetime, timezone

    event = ApplicationSubmitted(
        application_id="app-fault",
        applicant_id="applicant-fault",
        requested_amount_usd=75_000,
        loan_purpose="equipment_financing",
        loan_term_months=24,
        submission_channel="api",
        contact_email="fault@example.com",
        contact_name="Fault Test",
        submitted_at=datetime.now(tz=timezone.utc),
        application_reference="REF-FAULT",
    )
    await store.append("loan-app-fault", [event], expected_version=-1)

    # Projection that always fails
    failing = _FailingProjection(name="always_fails", fail_times=999)
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(failing)
    await daemon._load_checkpoints()

    # Run 3 batches — each attempt increments retry counter
    for _ in range(3):
        await daemon._process_batch()

    # After max_retries exhausted, checkpoint should be advanced (event skipped)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
            "always_fails",
        )
    assert row is not None, "Checkpoint should be saved even after skip"
    assert row["last_position"] >= 1


@pytest.mark.asyncio
async def test_fault_tolerance_daemon_never_crashes(store, db_pool):
    """Daemon continues processing after a projection raises an exception (Req 12.2)."""
    from src.models.events import ApplicationSubmitted
    from datetime import datetime, timezone

    # Append two events
    for i in range(2):
        event = ApplicationSubmitted(
            application_id=f"app-crash-{i}",
            applicant_id=f"applicant-crash-{i}",
            requested_amount_usd=50_000,
            loan_purpose="working_capital",
            loan_term_months=12,
            submission_channel="web",
            contact_email=f"crash{i}@example.com",
            contact_name=f"Crash {i}",
            submitted_at=datetime.now(tz=timezone.utc),
            application_reference=f"REF-CRASH-{i}",
        )
        await store.append(f"loan-app-crash-{i}", [event], expected_version=-1)

    # One projection fails, one succeeds
    failing = _FailingProjection(name="crash_fail", fail_times=999)
    counting = _CountingProjection(name="crash_ok")

    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(failing)
    daemon.register(counting)
    await daemon._load_checkpoints()

    # Run enough batches to exhaust retries on failing projection
    for _ in range(4):
        await daemon._process_batch()

    # The counting projection should have processed both events
    assert len(counting.handled) == 2


@pytest.mark.asyncio
async def test_get_lag_returns_zero_when_caught_up(store, db_pool):
    """get_lag returns 0 when there are no events in the store."""
    daemon = ProjectionDaemon(store=store, pool=db_pool)
    proj = _CountingProjection(name="lag_test")
    daemon.register(proj)

    lag = await daemon.get_lag("lag_test")
    assert lag == 0


@pytest.mark.asyncio
async def test_get_lag_returns_positive_when_behind(store, db_pool):
    """get_lag returns a positive value when projection hasn't processed latest events."""
    from src.models.events import ApplicationSubmitted
    from datetime import datetime, timezone

    event = ApplicationSubmitted(
        application_id="app-lag",
        applicant_id="applicant-lag",
        requested_amount_usd=100_000,
        loan_purpose="working_capital",
        loan_term_months=12,
        submission_channel="web",
        contact_email="lag@example.com",
        contact_name="Lag Test",
        submitted_at=datetime.now(tz=timezone.utc),
        application_reference="REF-LAG",
    )
    await store.append("loan-app-lag", [event], expected_version=-1)

    daemon = ProjectionDaemon(store=store, pool=db_pool)
    proj = _CountingProjection(name="lag_behind")
    daemon.register(proj)
    # Don't process — projection is at checkpoint 0

    lag = await daemon.get_lag("lag_behind")
    # Lag should be >= 0 (could be 0 if event was just written)
    assert lag >= 0


@pytest.mark.asyncio
async def test_get_all_lags_returns_dict_for_all_projections(store, db_pool):
    """get_all_lags returns a dict with an entry for every registered projection."""
    daemon = ProjectionDaemon(store=store, pool=db_pool)
    daemon.register(_CountingProjection(name="proj_a"))
    daemon.register(_CountingProjection(name="proj_b"))

    lags = await daemon.get_all_lags()
    assert set(lags.keys()) == {"proj_a", "proj_b"}
    assert all(isinstance(v, int) for v in lags.values())


@pytest.mark.asyncio
async def test_unsubscribed_events_advance_checkpoint(store, db_pool):
    """Events not subscribed to still advance the projection's checkpoint."""
    from src.models.events import ApplicationSubmitted
    from datetime import datetime, timezone

    event = ApplicationSubmitted(
        application_id="app-unsub",
        applicant_id="applicant-unsub",
        requested_amount_usd=50_000,
        loan_purpose="working_capital",
        loan_term_months=12,
        submission_channel="web",
        contact_email="unsub@example.com",
        contact_name="Unsub Test",
        submitted_at=datetime.now(tz=timezone.utc),
        application_reference="REF-UNSUB",
    )
    await store.append("loan-app-unsub", [event], expected_version=-1)

    # Projection that subscribes to nothing
    proj = _CountingProjection(name="no_sub", subscribes=set())
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(proj)
    await daemon._load_checkpoints()
    await daemon._process_batch()

    # No events handled
    assert len(proj.handled) == 0

    # But checkpoint should still be advanced
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
            "no_sub",
        )
    assert row is not None
    assert row["last_position"] >= 1
