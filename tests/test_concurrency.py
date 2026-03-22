"""
Phase 1 — Concurrency tests (Req 20.1, Req 4.1–4.6)

Double-decision test: two concurrent asyncio tasks race to append to the same
stream at the same expected_version. Exactly one must succeed; the other must
raise OptimisticConcurrencyError.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from src.models.events import ApplicationSubmitted, CreditAnalysisRequested
from src.models.exceptions import OptimisticConcurrencyError


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _submitted(app_id: str) -> ApplicationSubmitted:
    return ApplicationSubmitted(
        application_id=app_id,
        applicant_id="applicant-001",
        requested_amount_usd=500_000,
        loan_purpose="working_capital",
        loan_term_months=24,
        submission_channel="web",
        contact_email="test@example.com",
        contact_name="Test User",
        submitted_at=_now(),
        application_reference=f"REF-{app_id}",
    )


def _analysis_requested(app_id: str) -> CreditAnalysisRequested:
    return CreditAnalysisRequested(
        application_id=app_id,
        requested_at=_now(),
    )


# ---------------------------------------------------------------------------
# Double-decision test (Req 20.1 / Req 4.1–4.6)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_double_decision_exactly_one_succeeds(store):
    """
    Two concurrent tasks both hold expected_version=3 and race to append.
    Exactly one must succeed; the other must raise OptimisticConcurrencyError.
    After the race the stream must contain exactly 4 events (no phantom inserts).
    """
    app_id = "concurrency-001"
    stream_id = f"loan-{app_id}"

    # Setup: bring stream to version 3
    await store.append(stream_id, [_submitted(app_id)], expected_version=-1)
    await store.append(stream_id, [_analysis_requested(app_id)], expected_version=1)
    await store.append(stream_id, [_analysis_requested(app_id)], expected_version=2)

    assert await store.stream_version(stream_id) == 3

    # Race: two tasks both try to append at expected_version=3
    results: list[int | OptimisticConcurrencyError] = []

    async def try_append(task_num: int) -> None:
        print(f"\n  [task-{task_num}] attempting append at expected_version=3")
        try:
            v = await store.append(
                stream_id, [_analysis_requested(app_id)], expected_version=3
            )
            print(f"  [task-{task_num}] SUCCESS — won the race, new version={v}")
            results.append(v)
        except OptimisticConcurrencyError as exc:
            print(f"  [task-{task_num}] LOST   — OptimisticConcurrencyError: "
                  f"expected={exc.expected_version}, actual={exc.actual_version}, "
                  f"suggested_action={exc.suggested_action!r}")
            results.append(exc)

    print(f"\n--- stream '{stream_id}' at version 3, launching two concurrent tasks ---")
    await asyncio.gather(try_append(1), try_append(2))

    successes = [r for r in results if isinstance(r, int)]
    failures  = [r for r in results if isinstance(r, OptimisticConcurrencyError)]

    print(f"\n--- race complete ---")
    print(f"  winners : {len(successes)} (version {successes[0] if successes else 'n/a'})")
    print(f"  losers  : {len(failures)}")

    assert len(successes) == 1, f"Expected 1 success, got {len(successes)}: {results}"
    assert len(failures)  == 1, f"Expected 1 failure, got {len(failures)}: {results}"

    # Winner returns version 4
    assert successes[0] == 4

    # Stream length is exactly 4 — no phantom events
    events = await store.load_stream(stream_id)
    print(f"  stream length after race: {len(events)} events (positions: {[e.stream_position for e in events]})")
    assert len(events) == 4

    # Loser carries the correct error payload (Req 4.3)
    loser = failures[0]
    assert loser.stream_id == stream_id
    assert loser.expected_version == 3
    assert loser.suggested_action == "reload_stream_and_retry"


@pytest.mark.asyncio
async def test_loser_can_reload_and_retry(store):
    """
    After receiving OptimisticConcurrencyError the losing agent reloads the
    stream, obtains the new version, and retries successfully (Req 4.4).
    """
    app_id = "concurrency-002"
    stream_id = f"loan-{app_id}"

    await store.append(stream_id, [_submitted(app_id)], expected_version=-1)

    with pytest.raises(OptimisticConcurrencyError):
        await store.append(stream_id, [_analysis_requested(app_id)], expected_version=0)

    # Reload and retry with the correct version
    current_version = await store.stream_version(stream_id)
    new_v = await store.append(
        stream_id, [_analysis_requested(app_id)], expected_version=current_version
    )
    assert new_v == 2


@pytest.mark.asyncio
async def test_losing_transaction_fully_rolled_back(store):
    """
    The losing transaction must produce zero partial inserts — no phantom
    events, no orphaned outbox rows (Req 4.6).
    """
    app_id = "concurrency-003"
    stream_id = f"loan-{app_id}"

    await store.append(stream_id, [_submitted(app_id)], expected_version=-1)

    with pytest.raises(OptimisticConcurrencyError):
        await store.append(stream_id, [_analysis_requested(app_id)], expected_version=0)

    events = await store.load_stream(stream_id)
    assert len(events) == 1
    assert await store.stream_version(stream_id) == 1
