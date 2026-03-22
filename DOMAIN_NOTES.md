# DOMAIN_NOTES.md — Agentic Event Store (The Ledger)

---

## 0.1 EDA vs ES Distinction

### Why LangChain Callbacks Are EDA, Not Event Sourcing

LangChain's callback system fires events as **side-effects** of agent execution. When a chain
runs, callbacks like `on_llm_start`, `on_tool_end`, or `on_chain_error` are invoked to notify
observers — tracing backends, loggers, monitoring dashboards. These callbacks are purely
observational: they do not determine what the agent does next, and they are not the source of
truth for any state.

This is the defining characteristic of **Event-Driven Architecture (EDA)**: events are
*notifications* that something happened. The actual state lives elsewhere — in the in-memory
agent object, in a separate database, in a Redis cache. If the callback handler fails or is
never registered, the agent continues executing identically. The event is a broadcast, not a
record of fact.

In EDA:
- State is primary; events are derived notifications.
- Losing an event loses observability, not correctness.
- Reconstructing "what happened" requires querying the state store, not replaying events.
- The event stream has no authority over the aggregate's current state.

### What Changes Architecturally with Event Sourcing (The Ledger)

In **Event Sourcing (ES)**, events *are* the state. The `events` table is the single source of
truth. There is no separate "current state" table for aggregates — the current state of a
`LoanApplication` is computed by replaying its event stream from position 1 to the latest
`stream_position`. If you delete the events, you lose the state entirely.

The architectural changes when redesigning from LangChain callbacks to The Ledger:

1. **State reconstruction moves from in-memory to stream replay.** Instead of an agent holding
   its context in a Python object that vanishes on crash, the agent's full decision history is
   reconstructible by calling `store.load_stream(f"agent-{agent_id}-{session_id}")`. The
   aggregate is the replay, not the object.

2. **Agent context becomes reconstructible from the store (Gas Town pattern).** The
   `AgentContextLoaded` event must be the second event in every `AgentSession` stream. This
   means any agent that crashes mid-operation can be resumed by replaying its session stream —
   the context is not lost with the process.

3. **Audit trail is a first-class citizen, not a side-effect.** In LangChain, the trace is
   optional and lossy. In The Ledger, every `CreditAnalysisCompleted`, `ComplianceRulePassed`,
   and `DecisionGenerated` event is durably persisted in an append-only, cryptographically
   chained store. The audit trail cannot be disabled or lost.

4. **Concurrency is managed via optimistic locking on streams.** In EDA, two agents can
   overwrite each other's state silently. In ES, the `SELECT ... FOR UPDATE` on
   `event_streams.current_version` ensures that only one agent can successfully append to a
   stream at a given version — the other receives `OptimisticConcurrencyError` and must reload
   and retry.

5. **The CQRS split becomes explicit.** Writes go through `EventStore.append()` (command side);
   reads go through projections (`ApplicationSummary`, `ComplianceAuditView`) that are built
   asynchronously from the event stream (query side). In LangChain EDA, reads and writes share
   the same mutable state object.

---

## 0.2 Aggregate Boundary Justification

### The Alternative Considered: Merging `ComplianceRecord` into `LoanApplication`

One natural simplification would be to merge the `ComplianceRecord` aggregate into the
`LoanApplication` aggregate, placing all events on a single `loan-{id}` stream. This eliminates
the need for a separate `compliance-{application_id}` stream and removes the cross-stream
dependency check in the approval command handler.

### Why This Was Rejected: The Concurrent-Write Failure Mode

Consider stream `loan-abc123` at `current_version = 7`. Two agents are operating concurrently:

- **ComplianceAgent** has loaded the stream at version 7 and is about to append
  `ComplianceRulePassed` (rule KYC-001).
- **DecisionOrchestrator** has also loaded the stream at version 7 and is about to append
  `DecisionGenerated`.

Both call `append(stream_id="loan-abc123", expected_version=7)` concurrently.

PostgreSQL grants the `SELECT ... FOR UPDATE` lock to one — say the ComplianceAgent. The
DecisionOrchestrator blocks. The ComplianceAgent succeeds, advancing the stream to version 8.
The DecisionOrchestrator acquires the lock, reads `current_version = 8`, and raises
`OptimisticConcurrencyError(expected=7, actual=8)`.

This is the expected behaviour for a single-owner stream. The problem is what the
DecisionOrchestrator must do to retry:

**It must reload the entire merged stream** — including all `ComplianceRulePassed`,
`ComplianceRuleFailed`, and `ComplianceCheckRequested` events that it does not own and does not
care about — just to get the updated version number. This creates three forms of coupling:

1. **Write throughput coupling.** If 3 compliance rules are evaluated in parallel (KYC-001,
   AML-002, SANCTIONS-003), all three `ComplianceRulePassed` appends must serialise through the
   same version counter on `loan-abc123`. What could be 3 concurrent writes becomes 3 sequential
   writes, each requiring a retry by the others. At peak load (100 applications × 4 agents),
   this serialisation bottleneck is severe.

2. **Replay coupling.** The DecisionOrchestrator's retry requires replaying compliance events it
   has no semantic interest in. Its domain logic does not change based on which KYC rule passed —
   it only needs to know that all required checks are cleared. Yet it must process every
   compliance event to advance its version pointer.

3. **Ownership coupling.** The `loan-{id}` stream now has two distinct owners writing to it:
   the ComplianceAgent (compliance events) and the DecisionOrchestrator (decision events). There
   is no single aggregate that "owns" the stream's invariants. This violates the aggregate
   consistency boundary principle — an aggregate should be the sole writer to its stream.

By keeping `ComplianceRecord` on its own `compliance-{application_id}` stream, compliance agents
write to their stream without contending with the DecisionOrchestrator. The approval command
handler loads both streams at the application service layer, verifies compliance clearance, and
passes `compliance_cleared: bool` into the `LoanApplication` aggregate — which never loads
another aggregate's stream directly.

---

## 0.3 Concurrency Walkthrough

### Exact DB Operation Sequence: Two Agents, `expected_version=3`

Stream `loan-abc123` is at `current_version = 3`. Agent A (CreditAnalysis) and Agent B
(DecisionOrchestrator) have both loaded the stream and are about to append.

**Step 1 — Both agents read stream at version 3 (their local state).**
Each has called `store.load_stream("loan-abc123")` and received 3 events. Both hold
`expected_version = 3` in memory.

**Step 2 — Agent A calls `append(stream_id="loan-abc123", events=[...], expected_version=3)`.**

**Step 3 — Agent B calls `append(stream_id="loan-abc123", events=[...], expected_version=3)`
concurrently.**

**Step 4 — Both enter serializable transactions.**
Each opens `async with conn.transaction(isolation="serializable")`.

**Step 5 — Both execute:**
```sql
SELECT current_version FROM event_streams
WHERE stream_id = 'loan-abc123'
FOR UPDATE
```

**Step 6 — PostgreSQL grants the row lock to Agent A. Agent B blocks**, waiting for the lock
to be released. Only one transaction can hold the `FOR UPDATE` lock on this row at a time.

**Step 7 — Agent A reads `current_version = 3`.**
`actual_version = 3 == expected_version = 3` → version check passes. Agent A proceeds.

**Step 8 — Agent A inserts its events** at `stream_position = 4` (and 5, 6, ... if multiple
events). The `UNIQUE (stream_id, stream_position)` constraint is satisfied.

**Step 9 — Agent A updates `event_streams.current_version = 4`** (or N, the new highest
position).

**Step 10 — Agent A inserts outbox rows** for downstream publication, in the same transaction.

**Step 11 — Agent A COMMITs.** The transaction is durable. The row lock is released.

**Step 12 — Agent B acquires the lock.** It now executes the `SELECT ... FOR UPDATE` and reads
`current_version = 4` (updated by Agent A's committed transaction).

**Step 13 — Agent B: `actual_version = 4 != expected_version = 3`.**
Agent B raises:
```python
OptimisticConcurrencyError(
    stream_id="loan-abc123",
    expected_version=3,
    actual_version=4,
    suggested_action="reload_stream_and_retry",
)
```

**Step 14 — Agent B's transaction ROLLS BACK in full.** No events are inserted, no outbox rows
are written, no partial state exists in the database.

**Step 15 — What Agent B must do:**
1. Call `store.load_stream("loan-abc123")` to retrieve the current stream at version 4.
2. Re-apply its aggregate logic against the updated state (which now includes Agent A's events).
3. Re-validate its business rules with the updated state — Agent A's events may have changed
   the outcome (e.g., if Agent A appended a `CreditAnalysisCompleted` that changes the
   application state).
4. Retry `append(stream_id="loan-abc123", events=[...], expected_version=4)`.

The key guarantee: **no phantom events, no partial writes, no split-brain state.** The database
transaction is the unit of atomicity. Either all of Agent A's events commit together, or none do.

---

## 0.4 Projection Lag Consequence

### Scenario: Loan Officer Queries Credit Limit Within 200ms of Disbursement

**t=0ms** — `ApplicationApproved` event (containing `approved_amount_usd = 500,000`) commits to
the `events` table. `global_position` is assigned by the database. The event is durable.

**t=0ms to t=100ms** — The `ProjectionDaemon` is sleeping between poll cycles (default interval:
100ms). It has not yet seen this event. The `ApplicationSummary` projection's
`approved_amount_usd` column still holds its previous value (`null` for a newly approved
application, or the prior approved amount if this is a revision).

**t=150ms** — The loan officer's UI calls `ledger://applications/{id}`. The MCP resource handler
reads from the `ApplicationSummary` projection table only — it **never replays the event stream**
(Requirement 16.2). The query returns the stale row: `approved_amount_usd = null`,
`state = "PendingDecision"` (not yet updated to `"FinalApproved"`).

**t=150ms** — The response is returned to the UI with stale data. The loan officer sees the
credit limit as not yet set.

**t=200ms** — The `ProjectionDaemon` wakes, polls events from the last checkpoint, finds the
`ApplicationApproved` event, and routes it to the `ApplicationSummary` handler. The handler
executes an `INSERT ... ON CONFLICT DO UPDATE` upsert, setting `approved_amount_usd = 500000`
and `state = "FinalApproved"`.

**t=200ms** — The projection is now current. Any query after this point returns the correct value.

### How It Is Communicated to the UI

This is by design — CQRS eventual consistency. The write path (`append`) does not block waiting
for the projection to update. The SLO is 500ms lag under normal operating conditions
(Requirement 9.3).

To communicate staleness to the loan officer, the system includes a `projection_lag_ms` field
in every resource response, populated by calling `get_lag("application_summary")` on the
`ProjectionDaemon`. The UI should display a "data as of X seconds ago" indicator when
`projection_lag_ms` exceeds a threshold (e.g., 200ms). The loan officer sees the stale value
but knows it is stale and can refresh after the SLO window.

A response at t=150ms might look like:
```json
{
  "application_id": "abc123",
  "state": "PendingDecision",
  "approved_amount_usd": null,
  "projection_lag_ms": 150,
  "_note": "Data may be up to 500ms behind. Refresh to see latest."
}
```

The loan officer is not misled — they see both the current projection value and the lag
indicator. This is preferable to blocking the read path on projection completion, which would
couple read latency to write throughput and violate the CQRS separation.

---

## 0.5 Upcasting Scenario

### `CreditAnalysisCompleted` v1 → v2 Upcaster

The v1 schema for `CreditAnalysisCompleted` did not include `model_version`,
`confidence_score`, or `regulatory_basis`. These fields were added in v2. Historical v1 events
stored in the `events` table must be transparently migrated at read time without modifying the
stored row.

```python
from datetime import datetime
from typing import Any

MODEL_VERSION_TIMELINE = [
    (datetime(2024, 1, 1), datetime(2025, 6, 1), "credit-model-v1.0"),
    (datetime(2025, 6, 1), datetime(2026, 1, 1), "credit-model-v2.0"),
    (datetime(2026, 1, 1), None,                 "credit-model-v3.0"),
]

REGULATION_VERSION_TIMELINE = [
    (datetime(2024, 1, 1), datetime(2025, 3, 1), "REG-SET-2024-Q1"),
    (datetime(2025, 3, 1), datetime(2026, 1, 1), "REG-SET-2025-Q1"),
    (datetime(2026, 1, 1), None,                 "REG-SET-2026-Q1"),
]

def upcast_credit_analysis_completed_v1_to_v2(payload: dict, recorded_at: datetime) -> dict:
    """Upcasts CreditAnalysisCompleted from v1 to v2."""
    new_payload = dict(payload)

    # model_version: infer from recorded_at timestamp ranges
    model_version = None
    for start, end, version in MODEL_VERSION_TIMELINE:
        if recorded_at >= start and (end is None or recorded_at < end):
            model_version = version
            break
    new_payload["model_version"] = model_version  # may be None if outside known ranges

    # confidence_score: always null for v1 events — never fabricate
    new_payload["confidence_score"] = None

    # regulatory_basis: infer from regulation versions active at recorded_at
    regulatory_basis = None
    for start, end, reg_version in REGULATION_VERSION_TIMELINE:
        if recorded_at >= start and (end is None or recorded_at < end):
            regulatory_basis = reg_version
            break
    new_payload["regulatory_basis"] = regulatory_basis  # may be None if outside known ranges

    return new_payload
```

### Inference Strategy Documentation

**`model_version`** — Inferred by mapping `recorded_at` (the database-assigned write timestamp)
to known model deployment date ranges. The deployment timeline is maintained as a static lookup
table in the upcaster module. Error rate is approximately 5–15% during rollout windows, when
traffic was split between the outgoing and incoming model versions. For events recorded outside
any known range (e.g., before 2024-01-01), `model_version` is set to `null`. A null is honest;
a fabricated version string would corrupt `AgentPerformanceLedger` metrics by attributing
analyses to the wrong model.

**`confidence_score`** — Always set to `null`. This field was not computed by the v1 model and
cannot be reconstructed from any available signal. Fabricating a value — even a plausible
average — would corrupt the `avg_confidence_score` aggregate in `AgentPerformanceLedger` and
constitutes regulatory perjury: the compliance record would assert a confidence level that was
never actually computed. A null value is the only honest representation of missing data. Callers
must handle null `confidence_score` gracefully (e.g., exclude from averages, flag in UI).

**`regulatory_basis`** — Inferred from the regulation version effective at `recorded_at`, using
a static timeline of regulation set effective dates. Error rate is approximately 2–8% for events
recorded during periods when regulations were retroactively amended or when the effective date
of a new regulation set was disputed. For events outside known coverage, `regulatory_basis` is
set to `null`. The `ComplianceAuditView` must treat null `regulatory_basis` as "unknown at time
of analysis" rather than "no regulation applied."

**General principle**: when inference is uncertain, prefer `null` over a best-guess value.
Downstream consumers can handle `null` explicitly. A wrong non-null value silently corrupts
metrics, audit records, and regulatory reports in ways that may not be detected until examination.

---

## 0.6 Marten Async Daemon Parallel

### Distributed Projection Execution Across Multiple Nodes in Python

In .NET, Marten's Async Daemon provides built-in leader election and distributed projection
execution. In Python with PostgreSQL, the equivalent is achieved using **PostgreSQL Advisory
Locks** as the coordination primitive.

### The Coordination Primitive: PostgreSQL Advisory Locks

Each `Worker_Service` node, on startup, attempts to acquire a session-level advisory lock:

```sql
SELECT pg_try_advisory_lock(hashtext('projection_daemon_leader'))
```

`pg_try_advisory_lock` is non-blocking: it returns `true` if the lock was acquired, `false` if
another session already holds it. The lock is identified by a 64-bit integer derived from the
string `'projection_daemon_leader'` via `hashtext`.

- The node that acquires the lock becomes the **leader** and starts the `ProjectionDaemon`.
- All other nodes receive `false` and enter a polling loop, retrying every 5 seconds.
- If the leader node crashes or its database connection closes, PostgreSQL automatically releases
  the session-level advisory lock. The next node to poll successfully acquires it and starts the
  `ProjectionDaemon`.

This gives single-active-consumer semantics without a separate coordination service (no ZooKeeper,
no Redis, no etcd required — the same PostgreSQL instance used for the event store provides the
lock).

### The Failure Mode It Guards Against: Double-Processing

Without this coordination primitive, two `Worker_Service` nodes running `ProjectionDaemon`
simultaneously would both poll the `events` table from their respective checkpoints and both
process the same event batches. This causes **double-processing**.

For projections that use pure upsert semantics (`INSERT ... ON CONFLICT DO UPDATE SET
state = EXCLUDED.state`), double-processing is idempotent — the second write produces the same
result as the first. However, the `AgentPerformanceLedger` projection uses **increment
operations**:

```sql
UPDATE agent_performance_ledger
SET analyses_completed = analyses_completed + 1,
    avg_confidence_score = (avg_confidence_score * analyses_completed + $new_score)
                           / (analyses_completed + 1)
WHERE agent_id = $1 AND model_version = $2
```

This is **not idempotent**. Processing the same `CreditAnalysisCompleted` event twice doubles
`analyses_completed` and corrupts `avg_confidence_score`. Similarly, `ComplianceAuditView`
could accumulate duplicate compliance rule rows, and `AgentPerformanceLedger` would report
double the actual override rate.

### Why Idempotency Alone Is Insufficient

Making all projections fully idempotent would require tracking which `event_id` values have
already been processed per projection — effectively a processed-event log. This adds a table
write per event per projection, increasing write amplification by 3× (one per projection). The
advisory lock approach is simpler: prevent the second processor from running at all.

### Alternative: Per-Projection Partitioning

Rather than a single leader lock, each projection can have its own advisory lock:

```sql
SELECT pg_try_advisory_lock(hashtext('projection_daemon:application_summary'))
SELECT pg_try_advisory_lock(hashtext('projection_daemon:compliance_audit_view'))
SELECT pg_try_advisory_lock(hashtext('projection_daemon:agent_performance_ledger'))
```

This allows node 1 to own `ApplicationSummary`, node 2 to own `ComplianceAuditView`, and node 3
to own `AgentPerformanceLedger` — true parallel projection execution across nodes without
double-processing risk. Each node only processes events for its owned projections, and lock
release on crash triggers failover for that specific projection only.

### The Marten Parallel

Marten's Async Daemon uses an internal leader-election mechanism with its own coordination
tables. The Python equivalent using PostgreSQL advisory locks achieves the same guarantee:
exactly one active `ProjectionDaemon` instance per projection at any time, with automatic
failover on node crash, using only the infrastructure already present in the deployment (the
PostgreSQL database itself).
