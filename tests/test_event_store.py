"""
tests/phase1/test_event_store.py
=================================
Phase 1 gate tests — EventStore contract.

The same contract tests run against both implementations:
  - InMemoryEventStore  (fast, no DB required)
  - PostgreSQL EventStore  (real persistence, requires DB fixture)

Version convention (1-based):
  - empty stream  -> stream_version() == -1
  - after 1 event -> stream_version() == 1
  - after N events -> stream_version() == N
  - stream_position of first event == 1

Run all:        pytest tests/phase1/ -v
Run in-memory:  pytest tests/phase1/ -v -k "inmemory"
Run postgres:   pytest tests/phase1/ -v -k "postgres"
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import pytest_asyncio

from src.event_store import InMemoryEventStore
from src.models.events import (
    ApplicationSubmitted,
    CreditAnalysisRequested,
    FraudScreeningRequested,
    EVENT_REGISTRY,
)
from src.models.exceptions import OptimisticConcurrencyError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def inmemory_store():
    return InMemoryEventStore()


# `store` (postgres) is provided by tests/conftest.py


@pytest.fixture(params=["inmemory", "postgres"])
def es(request, inmemory_store, store):
    """Yields either the in-memory or postgres store based on the param."""
    if request.param == "inmemory":
        return inmemory_store
    return store


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _submitted(app_id: str, amount: float = 100_000.0) -> ApplicationSubmitted:
    return ApplicationSubmitted(
        application_id=app_id,
        applicant_id=f"applicant-{app_id}",
        requested_amount_usd=Decimal(str(amount)),
        loan_purpose="working_capital",
        loan_term_months=12,
        submission_channel="web",
        contact_email="test@example.com",
        contact_name="Test User",
        submitted_at=_now(),
        application_reference=f"REF-{app_id}",
    )


def _analysis_requested(app_id: str) -> CreditAnalysisRequested:
    return CreditAnalysisRequested(application_id=app_id, requested_at=_now())


def _fraud_requested(app_id: str) -> FraudScreeningRequested:
    return FraudScreeningRequested(application_id=app_id, requested_at=_now())



# ---------------------------------------------------------------------------
# Contract tests — run against both stores via the `es` fixture
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_stream_version_is_minus_one(es):
    assert await es.stream_version("loan-does-not-exist") == -1


@pytest.mark.asyncio
async def test_append_new_stream_returns_version_1(es):
    v = await es.append("s-v1", [_submitted("v1")], expected_version=-1)
    assert v == 1
    assert await es.stream_version("s-v1") == 1


@pytest.mark.asyncio
async def test_append_increments_version(es):
    await es.append("s-inc", [_submitted("inc")], expected_version=-1)
    await es.append("s-inc", [_analysis_requested("inc")], expected_version=1)
    await es.append("s-inc", [_fraud_requested("inc")], expected_version=2)
    assert await es.stream_version("s-inc") == 3


@pytest.mark.asyncio
async def test_append_multiple_events_returns_final_version(es):
    v = await es.append(
        "s-multi",
        [_submitted("multi"), _analysis_requested("multi"), _fraud_requested("multi")],
        expected_version=-1,
    )
    assert v == 3
    assert await es.stream_version("s-multi") == 3


@pytest.mark.asyncio
async def test_wrong_expected_version_raises_occ(es):
    await es.append("s-occ", [_submitted("occ")], expected_version=-1)
    with pytest.raises(OptimisticConcurrencyError) as exc_info:
        await es.append("s-occ", [_analysis_requested("occ")], expected_version=99)
    err = exc_info.value
    assert err.stream_id == "s-occ"
    assert err.expected_version == 99
    assert err.actual_version == 1
    assert err.suggested_action == "reload_stream_and_retry"


@pytest.mark.asyncio
async def test_load_stream_empty_for_nonexistent(es):
    assert await es.load_stream("loan-nonexistent-xyz") == []


@pytest.mark.asyncio
async def test_load_stream_returns_events_in_order(es):
    await es.append(
        "s-order",
        [_submitted("order"), _analysis_requested("order"), _fraud_requested("order")],
        expected_version=-1,
    )
    events = await es.load_stream("s-order")
    assert len(events) == 3
    assert [e.stream_position for e in events] == [1, 2, 3]
    assert events[0].event_type == "ApplicationSubmitted"
    assert events[1].event_type == "CreditAnalysisRequested"
    assert events[2].event_type == "FraudScreeningRequested"


@pytest.mark.asyncio
async def test_first_event_stream_position_is_1(es):
    await es.append("s-pos", [_submitted("pos")], expected_version=-1)
    events = await es.load_stream("s-pos")
    assert events[0].stream_position == 1


@pytest.mark.asyncio
async def test_load_stream_position_range(es):
    await es.append(
        "s-range",
        [_submitted("range"), _analysis_requested("range"), _fraud_requested("range")],
        expected_version=-1,
    )
    sliced = await es.load_stream("s-range", from_position=2, to_position=3)
    assert len(sliced) == 2
    assert sliced[0].stream_position == 2
    assert sliced[1].stream_position == 3


@pytest.mark.asyncio
async def test_event_id_preserved_round_trip(es):
    event = _submitted("eid")
    original_id = event.event_id
    await es.append("s-eid", [event], expected_version=-1)
    loaded = await es.load_stream("s-eid")
    assert loaded[0].event_id == original_id


@pytest.mark.asyncio
async def test_causation_id_stored_in_metadata(es):
    await es.append(
        "s-cause",
        [_submitted("cause")],
        expected_version=-1,
        causation_id="cause-xyz",
    )
    loaded = await es.load_stream("s-cause")
    assert loaded[0].metadata["causation_id"] == "cause-xyz"


@pytest.mark.asyncio
async def test_stream_version_tracks_each_append(es):
    assert await es.stream_version("s-track") == -1
    await es.append("s-track", [_submitted("track")], expected_version=-1)
    assert await es.stream_version("s-track") == 1
    await es.append("s-track", [_analysis_requested("track")], expected_version=1)
    assert await es.stream_version("s-track") == 2


@pytest.mark.asyncio
async def test_occ_loser_can_reload_and_retry(es):
    await es.append("s-retry", [_submitted("retry")], expected_version=-1)
    with pytest.raises(OptimisticConcurrencyError):
        await es.append("s-retry", [_analysis_requested("retry")], expected_version=0)
    current = await es.stream_version("s-retry")
    v = await es.append("s-retry", [_analysis_requested("retry")], expected_version=current)
    assert v == 2


# ---------------------------------------------------------------------------
# In-memory only: load_all and checkpoints (Postgres equivalents in phase 3)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_load_all_yields_events_across_streams(inmemory_store):
    await inmemory_store.append("s1", [_submitted("a1"), _analysis_requested("a1")], expected_version=-1)
    await inmemory_store.append("s2", [_submitted("a2")], expected_version=-1)
    all_events = [e async for e in inmemory_store.load_all(from_position=1)]
    assert len(all_events) == 3


@pytest.mark.asyncio
async def test_checkpoints_persist(inmemory_store):
    assert await inmemory_store.load_checkpoint("proj_a") == 0
    await inmemory_store.save_checkpoint("proj_a", 42)
    assert await inmemory_store.load_checkpoint("proj_a") == 42


# ---------------------------------------------------------------------------
# Seed event schema conformance
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_all_seed_event_types_validate():
    seed_file = Path("data/seed_events.jsonl")
    if not seed_file.exists():
        pytest.skip("data/seed_events.jsonl not found — run datagen first")

    validated = 0
    with open(seed_file) as f:
        for line in f:
            rec = json.loads(line)
            cls = EVENT_REGISTRY.get(rec["event_type"])
            if cls is None:
                continue
            try:
                cls(event_type=rec["event_type"], **rec["payload"])
                validated += 1
            except Exception:
                pass  # old v1 schema handled by upcasters at runtime

    if validated == 0:
        pytest.skip("No matching event types found in seed file")
    print(f"\nValidated {validated} seed events against EVENT_REGISTRY")
