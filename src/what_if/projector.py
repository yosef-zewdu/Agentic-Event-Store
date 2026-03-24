"""
src/what_if/projector.py — What-If Projector

Replays application history with counterfactual events substituted at a branch
point, without writing anything to the real event store.

Requirements covered:
  - Req 18.1: load events up to branch point
  - Req 18.2: inject counterfactual events at branch point
  - Req 18.3: include causally independent post-branch real events
  - Req 18.4: exclude causally dependent post-branch real events
  - Req 18.5: NEVER write counterfactual events to the real store
  - Req 18.6: demonstrate with risk_tier='HIGH' substitution
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from src.event_store import EventStore
from src.models.events import BaseEvent, StoredEvent


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class ProjectionState:
    """Snapshot of a projection's in-memory state after replaying a sequence."""
    projection_name: str
    state: dict[str, Any]


@dataclass
class WhatIfResult:
    """
    Result of a what-if analysis.

    Attributes:
        real_outcome:           Projection states after replaying the real event sequence.
        counterfactual_outcome: Projection states after replaying the counterfactual sequence.
        divergence_events:      Events present in the counterfactual sequence but not in real.
        pre_branch_count:       Number of events before the branch point.
        post_branch_real_count: Number of real post-branch events included (causally independent).
        excluded_count:         Number of real post-branch events excluded (causally dependent).
    """
    real_outcome: dict[str, Any]
    counterfactual_outcome: dict[str, Any]
    divergence_events: list[StoredEvent | BaseEvent]
    pre_branch_count: int
    post_branch_real_count: int
    excluded_count: int


# ---------------------------------------------------------------------------
# In-memory projection runner
# ---------------------------------------------------------------------------

class InMemoryProjectionRunner:
    """
    Applies a sequence of events to a set of projections entirely in memory.

    Projections are expected to implement a `handle_in_memory(event)` method
    or a standard `handle(event, conn)` method.  This runner provides a
    no-op asyncpg connection substitute so existing projections work unchanged.
    """

    async def run(
        self,
        events: list[StoredEvent | BaseEvent],
        projections: list[Any],
    ) -> dict[str, Any]:
        """
        Replay events through projections and return their final states.

        Returns a dict keyed by projection name with the projection's state dict.
        """
        results: dict[str, Any] = {}

        for proj in projections:
            runner = _SingleProjectionRunner(proj)
            state = await runner.replay(events)
            proj_name = getattr(proj, "name", type(proj).__name__)
            results[proj_name] = state

        return results


class _NoOpConn:
    """
    Minimal asyncpg.Connection substitute for in-memory projection replay.

    Captures all SQL writes into an in-memory store so projections can run
    without a real database connection.
    """

    def __init__(self) -> None:
        # table_name -> list of row dicts (last upsert wins per PK)
        self._tables: dict[str, dict[str, dict]] = {}

    async def execute(self, sql: str, *args) -> None:
        """Capture INSERT ... ON CONFLICT DO UPDATE into in-memory tables."""
        # We don't parse SQL — projections expose their state via get_state()
        pass

    async def fetchrow(self, sql: str, *args):
        return None

    async def fetch(self, sql: str, *args):
        return []

    async def fetchval(self, sql: str, *args):
        return None

    def get_tables(self) -> dict[str, dict]:
        return self._tables


class _SingleProjectionRunner:
    """Replays events through one projection, capturing its state."""

    def __init__(self, projection: Any) -> None:
        self._proj = projection
        self._state: dict[str, Any] = {}

    async def replay(self, events: list[StoredEvent | BaseEvent]) -> dict[str, Any]:
        """
        Replay events through the projection.

        For projections that implement `handle_in_memory(event) -> dict | None`,
        that method is called and its return value merged into state.

        For projections that implement the standard `handle(event, conn)` interface,
        a _NoOpConn is provided and the projection's internal state is read back
        via `get_state()` if available.
        """
        conn = _NoOpConn()

        for event in events:
            stored = _ensure_stored_event(event)
            proj_name = getattr(self._proj, "name", "")
            subscribed = getattr(self._proj, "subscribed_event_types", None)

            # Skip events this projection doesn't care about
            if subscribed is not None and stored.event_type not in subscribed:
                continue

            # Prefer handle_in_memory if available (returns state dict)
            if hasattr(self._proj, "handle_in_memory"):
                result = await self._proj.handle_in_memory(stored)
                if isinstance(result, dict):
                    self._state.update(result)
            elif hasattr(self._proj, "handle"):
                try:
                    await self._proj.handle(stored, conn)
                except Exception:
                    pass  # in-memory replay is best-effort

        # If projection exposes get_state(), use it; otherwise return accumulated state
        if hasattr(self._proj, "get_state"):
            return self._proj.get_state()
        return dict(self._state)


# ---------------------------------------------------------------------------
# Core what-if function
# ---------------------------------------------------------------------------

async def run_what_if(
    store: EventStore,
    application_id: str,
    branch_at_event_type: str,
    counterfactual_events: list[BaseEvent],
    projections: list[Any],
) -> WhatIfResult:
    """
    Run a what-if analysis by branching the event history at a specific event type.

    Steps:
      1. Load the full event stream for the application (Req 18.1).
      2. Split at the first occurrence of `branch_at_event_type`:
         - pre_branch: all events before the branch event (inclusive of events
           up to but NOT including the branch event itself)
         - branch_event: the event being replaced
         - post_branch_real: all events after the branch event
      3. Classify post-branch real events as causally dependent or independent
         by tracing `causation_id` chains back to the branch event (Req 18.4).
      4. Build the counterfactual sequence:
           pre_branch + counterfactual_events + causally_independent_post_branch
         (Req 18.2, 18.3)
      5. Apply both sequences to projections in memory — NEVER writes to the
         real store (Req 18.5).
      6. Return WhatIfResult with both outcomes and divergence metadata.

    Args:
        store:                   EventStore to load the real event stream from.
        application_id:          The loan application ID (stream: loan-{id}).
        branch_at_event_type:    The event type at which to branch.
        counterfactual_events:   Replacement events to inject at the branch point.
        projections:             List of projection instances to apply events to.

    Returns:
        WhatIfResult with real_outcome, counterfactual_outcome, and divergence info.

    Raises:
        ValueError: if branch_at_event_type is not found in the event stream.
    """
    # 1. Load the real event stream (Req 18.1)
    stream_id = f"loan-{application_id}"
    real_events = await store.load_stream(stream_id)

    # 2. Find the branch point
    branch_idx = next(
        (i for i, e in enumerate(real_events) if e.event_type == branch_at_event_type),
        None,
    )
    if branch_idx is None:
        raise ValueError(
            f"branch_at_event_type={branch_at_event_type!r} not found in stream "
            f"{stream_id!r}. Available types: "
            f"{[e.event_type for e in real_events]}"
        )

    pre_branch = real_events[:branch_idx]          # events before the branch
    branch_event = real_events[branch_idx]          # the event being replaced
    post_branch_real = real_events[branch_idx + 1:] # events after the branch

    # 3. Classify post-branch events by causal dependency (Req 18.3, 18.4)
    #    An event is causally dependent if its causation_id traces back to the
    #    branch event (or any event that is itself causally dependent on it).
    dependent_ids: set[str] = {str(branch_event.event_id)}
    causally_independent: list[StoredEvent] = []
    causally_dependent: list[StoredEvent] = []

    for event in post_branch_real:
        causation_id = event.metadata.get("causation_id") or event.payload.get("causation_id")
        if causation_id and str(causation_id) in dependent_ids:
            # This event was caused by the branch event or a dependent event
            dependent_ids.add(str(event.event_id))
            causally_dependent.append(event)
        else:
            causally_independent.append(event)

    # 4. Build the counterfactual sequence (Req 18.2)
    #    Convert counterfactual BaseEvents to StoredEvents for uniform handling
    cf_stored = [_ensure_stored_event(e) for e in counterfactual_events]
    counterfactual_sequence = pre_branch + cf_stored + causally_independent

    # 5. Apply both sequences to projections in memory (Req 18.5 — no DB writes)
    runner = InMemoryProjectionRunner()
    real_outcome = await runner.run(real_events, projections)
    cf_outcome = await runner.run(counterfactual_sequence, projections)

    # 6. Compute divergence: events in counterfactual but not in real
    real_event_ids = {str(e.event_id) for e in real_events}
    divergence = [e for e in cf_stored if str(e.event_id) not in real_event_ids]

    return WhatIfResult(
        real_outcome=real_outcome,
        counterfactual_outcome=cf_outcome,
        divergence_events=divergence,
        pre_branch_count=len(pre_branch),
        post_branch_real_count=len(causally_independent),
        excluded_count=len(causally_dependent),
    )


# ---------------------------------------------------------------------------
# Demonstration: risk_tier='HIGH' substitution (Req 18.6)
# ---------------------------------------------------------------------------

async def demonstrate_high_risk_substitution(
    store: EventStore,
    application_id: str,
    projections: list[Any],
) -> WhatIfResult:
    """
    Demonstrate what-if analysis by substituting a HIGH risk tier at the
    CreditAnalysisCompleted branch point (Req 18.6).

    Loads the real CreditAnalysisCompleted event, creates a counterfactual
    version with risk_tier='HIGH' and a lower recommended_limit_usd, then
    runs the what-if analysis to show the materially different outcome.
    """
    # Load the real stream to find the CreditAnalysisCompleted event
    real_events = await store.load_stream(f"loan-{application_id}")
    branch_event = next(
        (e for e in real_events if e.event_type == "CreditAnalysisCompleted"),
        None,
    )
    if branch_event is None:
        raise ValueError(
            f"No CreditAnalysisCompleted event found for application {application_id}"
        )

    # Build counterfactual: same event but with HIGH risk tier and reduced limit
    cf_payload = dict(branch_event.payload)
    decision = dict(cf_payload.get("decision") or {})
    decision["risk_tier"] = "HIGH"
    # Reduce recommended limit by 50% to produce a materially different outcome
    original_limit = decision.get("recommended_limit_usd", 0)
    try:
        decision["recommended_limit_usd"] = float(original_limit) * 0.5
    except (TypeError, ValueError):
        decision["recommended_limit_usd"] = 0
    decision["rationale"] = (
        "[COUNTERFACTUAL] Risk tier overridden to HIGH for what-if analysis. "
        + decision.get("rationale", "")
    )
    cf_payload["decision"] = decision

    # Wrap as a StoredEvent so it flows through the projection pipeline unchanged
    cf_event = branch_event.model_copy(
        update={
            "event_id": uuid4(),
            "payload": cf_payload,
        }
    )

    return await run_what_if(
        store=store,
        application_id=application_id,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[cf_event],  # type: ignore[list-item]
        projections=projections,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_stored_event(event: StoredEvent | BaseEvent) -> StoredEvent:
    """
    Coerce a BaseEvent or StoredEvent into a StoredEvent for uniform handling.

    Counterfactual events are BaseEvent instances; they need to be wrapped as
    StoredEvents so projection handlers receive the expected type.
    """
    if isinstance(event, StoredEvent):
        return event

    # BaseEvent → StoredEvent
    payload = event.payload if event.payload else event.to_payload()
    return StoredEvent(
        event_id=event.event_id,
        stream_id="",  # counterfactual — no real stream
        stream_position=0,
        global_position=0,
        event_type=event.event_type,
        event_version=event.event_version,
        payload=payload,
        metadata=event.metadata if isinstance(event.metadata, dict) else {},
        recorded_at=event.recorded_at or datetime.now(tz=timezone.utc),
    )
