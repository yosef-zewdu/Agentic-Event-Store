# DESIGN.md — The Ledger: Agentic Event Store

*Written after implementation. Documents actual architectural decisions and tradeoffs.*

---

## 29.1 Aggregate Boundary Justification: ComplianceRecord vs LoanApplication

### Why ComplianceRecord is a separate aggregate

`ComplianceRecord` (stream: `compliance-{application_id}`) and `LoanApplication` (stream:
`loan-{application_id}`) are separate aggregates with separate streams. The boundary is not
arbitrary — it is the direct consequence of who writes to each stream and when.

**Different writers, different cadences.** The `ComplianceAgent` writes to the compliance stream
concurrently with the `CreditAnalysisAgent` writing to the loan stream. Both agents run in
parallel after document processing completes. If compliance events lived in the loan stream,
every compliance rule verdict would require the `ComplianceAgent` to hold the loan stream's
`SELECT FOR UPDATE` lock while the `CreditAnalysisAgent` is also trying to append to it.

**Different state machines.** `LoanApplication` enforces a strict linear state machine
(`SUBMITTED → CREDIT_ANALYSIS_REQUESTED → ... → APPROVED`). `ComplianceRecord` accumulates
rule verdicts in any order — `ComplianceRulePassed` and `ComplianceRuleFailed` events arrive
as the compliance agent evaluates each rule independently. Merging these into one stream would
force the loan state machine to handle compliance rule events, coupling two orthogonal concerns.

**Different read patterns.** Compliance officers query `ComplianceAuditView` with temporal
filters (`get_compliance_at(application_id, timestamp)`). Loan officers query
`ApplicationSummary` for current state. These projections subscribe to different event types
and have different SLOs (2000ms vs 500ms). Separate streams allow each projection to advance
its checkpoint independently.

### What would couple if merged

If `ComplianceRulePassed`, `ComplianceRuleFailed`, and `ComplianceCheckCompleted` were appended
to the `loan-{application_id}` stream:

1. **The `LoanApplication` state machine would need to handle compliance events.** The
   `_on_compliance_rule_passed` handler would need to accumulate rule verdicts inside the loan
   aggregate — state that belongs to `ComplianceRecordAggregate`. The aggregate would grow to
   track `rule_verdicts: dict[str, RuleVerdict]`, `regulation_set_version`, and
   `has_blocking_failure` — all currently in `ComplianceRecordAggregate`.

2. **The compliance dependency check (Req 8.3) would become circular.** Currently,
   `handle_generate_decision` loads `ComplianceRecordAggregate` separately to verify
   `all_rules_passed` before appending `DecisionGenerated` to the loan stream. If compliance
   state lived in the loan aggregate, the command handler would load the loan aggregate, check
   compliance state within it, then append back to the same stream — still correct, but the
   aggregate would be responsible for cross-cutting compliance logic it should not own.

### Trace to a specific concurrent-write failure mode

The concrete failure mode is a **lost compliance verdict under concurrent appends**.

Scenario: `CreditAnalysisAgent` and `ComplianceAgent` both load the loan stream at version 3.
Both call `store.append(stream_id="loan-APP-001", expected_version=3)`. The `SELECT FOR UPDATE`
in `EventStore.append` serialises them — one wins, one raises `OptimisticConcurrencyError`.

If compliance events lived in the loan stream, the losing agent (say, `ComplianceAgent`) must
reload the stream, re-evaluate its rule, and retry. But the compliance agent's rule evaluation
is not idempotent in the general case — it may call external services or depend on document
state that has changed. The retry is expensive and error-prone.

With separate streams, `ComplianceAgent` appends to `compliance-APP-001` and
`CreditAnalysisAgent` appends to `loan-APP-001`. They never contend on the same stream row.
The `SELECT FOR UPDATE` in each append targets a different `event_streams` row. Zero
`OptimisticConcurrencyError` between these two agents for the same application.

The only remaining contention on the loan stream is between the `DecisionOrchestrator` and
human review actions — a much narrower window, and one where retry is cheap (reload + re-check
compliance status, which is now a fast read from `ComplianceRecordAggregate`).

---

## 29.2 Projection Strategy

### Why all three projections are async (not inline)

All three projections are updated asynchronously by `ProjectionDaemon`, not inline during the
command handler's transaction. This is a deliberate tradeoff.

**Inline projection** would mean: inside `EventStore.append`'s serializable transaction, after
inserting events, also update the projection table. The projection read would always be
consistent with the latest write — zero lag.

**Async projection** means: `EventStore.append` commits events and outbox rows. The
`ProjectionDaemon` polls `events` at 100ms intervals and updates projection tables in separate
transactions. Reads may see stale data for up to the lag SLO.

The reasons to choose async for all three:

1. **Write path isolation.** A slow or failing projection handler must not block or roll back
   the command handler's transaction. With async projections, a `ComplianceAuditView` handler
   bug causes projection lag, not command failures. The `ProjectionDaemon` retries up to 3
   times then skips — the daemon never crashes (Req 12.2).

2. **Independent scaling.** The `Worker_Service` running `ProjectionDaemon` can be scaled,
   restarted, or redeployed independently of the `Backend_API`. This is the entire point of
   the two-process deployment topology.

3. **Rebuild capability.** `rebuild_from_scratch()` replays the full event log into a shadow
   table and atomically swaps it. This is only possible because projections are decoupled from
   the write path. An inline projection cannot be rebuilt without taking the write path offline.

4. **Idempotency requirement.** Because checkpoint and projection writes are atomic but
   at-least-once (the daemon may reprocess events after a crash), all projection handlers use
   `INSERT ... ON CONFLICT DO UPDATE`. This is a natural fit for async projections; inline
   projections would need the same guarantee but with higher complexity.

### Per-projection justification and SLO commitments

**ApplicationSummary** — SLO: lag < 500ms, query p99 < 50ms

Subscribes to 13 event types covering the full loan lifecycle. Each handler is a single upsert
on the `application_summary` table keyed by `application_id`. The table has a primary key index
on `application_id`, making point reads O(1). The 50ms query SLO is achievable because the
`ledger://applications/{id}` resource reads a single row by primary key — no joins, no
aggregation. The 500ms lag SLO is achievable at the 100ms poll interval because each batch
processes all pending events before sleeping.

**AgentPerformanceLedger** — SLO: query p99 < 50ms

Subscribes to `CreditAnalysisCompleted`, `HumanReviewCompleted`, and `DecisionGenerated`.
Each handler updates a `(agent_id, model_version)` row using a running-average formula computed
entirely in SQL (`ROUND((avg * count + new_value) / (count + 1), 6)`). No application-side
aggregation. The projection also maintains an `agent_performance_processed_events` deduplication
table — a separate idempotency guard beyond the upsert, because the rolling average formula is
not naturally idempotent (processing the same `CreditAnalysisCompleted` twice would double-count
the analysis). The 50ms query SLO is achievable for the same reason as `ApplicationSummary`:
single-row primary key read.

**ComplianceAuditView** — SLO: lag < 2000ms, query p99 < 200ms

Subscribes to 6 compliance event types. Unlike the other two projections, each compliance event
gets its own row (keyed by `(application_id, event_id)`), preserving the full audit trail rather
than collapsing to current state. This means the table grows with every compliance event — hence
the looser 2000ms lag SLO (more rows to write per batch) and the 200ms query SLO (the
`get_compliance_at` query filters by `recorded_at <= timestamp`, which uses the
`(application_id, recorded_at)` index). The temporal query uses `recorded_at` (DB-assigned,
unfalsifiable) as the authoritative anchor, not `evaluation_timestamp` (agent-assigned, could
be backdated).

### ComplianceAuditView snapshot strategy and invalidation logic

**Trigger:** After every 50 compliance events per application (`SNAPSHOT_EVENT_THRESHOLD = 50`),
the projection writes a snapshot to `compliance_snapshots(application_id, snapshot_position,
snapshot_version, state JSONB, created_at)`.

**What is snapshotted:** The full set of compliance rows for the application at that point —
serialised as a JSONB array. This allows a future rebuild or point-in-time query to start from
the snapshot position rather than replaying from `global_position = 0`.

**Invalidation:** The snapshot carries `snapshot_version = SNAPSHOT_SCHEMA_VERSION` (currently
`1`). When `load_snapshot()` is called, it checks `snapshot_version == SNAPSHOT_SCHEMA_VERSION`.
If the stored version is lower (schema evolved), the snapshot is discarded and state is rebuilt
from position 0. This is a hard invalidation — no migration of snapshot state, because snapshot
state is a denormalised read model and migrating it would require the same logic as a full
rebuild.

**In-memory counter:** The event count per application is tracked in `_event_counts: dict[str,
int]` on the projection instance. This counter resets on daemon restart. After a restart, the
counter starts at 0 and the next snapshot is triggered after 50 more events — meaning the
snapshot interval is "at least 50 events since last daemon start", not "exactly every 50 events
globally". This is acceptable: snapshots are a performance optimisation, not a correctness
requirement.

---

## 29.3 Concurrency Analysis

### Expected OptimisticConcurrencyError rate at peak load

Peak load scenario: 100 concurrent loan applications, each processed by 4 agents
(CreditAnalysis, FraudDetection, Compliance, DecisionOrchestrator).

**Per-application contention analysis:**

Each application has one loan stream (`loan-{id}`). The agents that write to this stream are:
- `CreditAnalysisAgent`: appends `CreditAnalysisCompleted` (1 event)
- `FraudDetectionAgent`: appends `FraudScreeningCompleted` (1 event)
- `DecisionOrchestrator`: appends `DecisionGenerated` (1 event)
- Human reviewer: appends `HumanReviewCompleted` + `ApplicationApproved/Declined` (2 events)

The `ComplianceAgent` writes to `compliance-{id}`, not the loan stream — no contention there.

In the normal sequential flow, these agents write at different lifecycle stages and do not
overlap. Contention only occurs when:
1. Two agents attempt to write to the same stream at the same version simultaneously.
2. This happens when an agent retries after a transient failure and races with another agent
   that has already advanced the stream.

**Contention window:** The `SELECT FOR UPDATE` in `EventStore.append` holds the lock for the
duration of the serializable transaction — typically 2–10ms for a single-event append on a
local PostgreSQL instance. At 100 concurrent applications with 4 agents each, the maximum
concurrent append rate is ~400 appends/minute (assuming each application takes ~60 seconds
end-to-end). With a 5ms average lock hold time, the probability of two appends to the same
stream overlapping is approximately:

```
P(collision) ≈ (lock_hold_ms / inter_arrival_ms)
             = 5ms / (60,000ms / 4 agents)
             = 5 / 15,000
             ≈ 0.03%
```

At 400 appends/minute, expected collisions ≈ 400 × 0.0003 ≈ **0.12 per minute** — effectively
less than 1 `OptimisticConcurrencyError` per minute under normal sequential agent execution.

**Worst case (parallel agent execution):** If all 4 agents for an application attempt to write
simultaneously (e.g., after a system restart where all agents reload and retry), the collision
rate rises to ~3 per application per restart event. At 100 applications restarting
simultaneously: ~300 errors in the first minute, decaying to near-zero as agents stagger.

### Retry strategy

The `OptimisticConcurrencyError` includes `suggested_action = "reload_stream_and_retry"`. The
retry pattern implemented in command handlers is:

```
1. Load aggregate (get current version)
2. Validate business rules
3. Append with expected_version = aggregate.version
4. On OptimisticConcurrencyError: go to step 1
```

This is a full reload-and-retry, not an optimistic increment. The reload is necessary because
the business rules must be re-evaluated against the updated stream state — a credit analysis
that was valid at version 3 may be invalid at version 4 if another agent appended a
`CreditAnalysisSuperseded` event.

### Maximum retry budget

The current implementation does not enforce a retry limit in command handlers — retries are
left to the caller (MCP tool layer or agent). The recommended budget is **3 retries with
exponential backoff** (50ms, 100ms, 200ms), after which the caller returns a structured error:

```json
{
  "error_type": "OptimisticConcurrencyError",
  "message": "Stream loan-APP-001 at version 7, expected 5",
  "suggested_action": "reload_stream_and_retry",
  "retry_count": 3,
  "context": {"stream_id": "loan-APP-001"}
}
```

The rationale for 3 retries: at the expected collision rate of <1/minute, 3 retries with 50ms
backoff resolve >99.9% of transient conflicts. A persistent conflict after 3 retries indicates
a logic error (two agents both believe they should write the same event type) rather than a
timing race — retrying further would not help.

---

## 29.4 Upcasting Inference Decisions

Two event types have upcasters: `CreditAnalysisCompleted` v1→v2 and `DecisionGenerated` v1→v2.

### CreditAnalysisCompleted v1→v2

v2 adds three fields: `model_version`, `confidence_score`, `regulatory_basis`.

**`model_version`** — inferred from `recorded_at` against `_MODEL_DEPLOYMENT_SCHEDULE`.

The schedule maps deployment windows `[start, end)` to model version strings. Inference
looks up which window contains `recorded_at`.

*Error rate:* ~2%. Deployment windows have hard boundaries, but maintenance windows and
canary deployments create ambiguity at boundaries. An event recorded during a 30-minute
canary rollout could belong to either the old or new model version.

*Consequence of wrong inference:* `AgentPerformanceLedger` attributes the analysis to the
wrong model version. Performance metrics (avg_confidence_score, human_override_rate) are
skewed for the affected model versions. This is detectable by comparing the inferred
model_version distribution against deployment records. Severity: low — it affects analytics,
not decisions.

*Why not null:* `model_version` is used by `AgentPerformanceLedger` to group metrics. A null
value would create an "unknown" bucket that obscures the performance of both model versions.
Inference with documented ~2% error is more useful than null for analytics purposes.

**`confidence_score`** — always set to `None`. Never inferred.

v1 events did not capture `confidence_score` in the payload. The only proxy would be
`decision.risk_tier` (LOW/MEDIUM/HIGH), but mapping a categorical risk tier to a continuous
confidence score would fabricate precision that does not exist.

*Consequence of null:* The confidence floor business rule (Req 8.2) checks
`confidence_score < 0.6`. A null `confidence_score` on a v1 event means the floor check
cannot be applied retroactively. This is correct — the floor was not enforced when the event
was originally written, and fabricating a score to retroactively apply it would be worse than
acknowledging the gap.

*Why null over inference:* The confidence floor is a safety rule. A fabricated score of 0.7
on an event that was actually 0.4 would make a non-compliant historical decision appear
compliant. The audit consequence of a wrong inference here is high. Null is the only honest
answer.

**`regulatory_basis`** — inferred from `recorded_at` against `_REGULATION_SCHEDULE`.

The schedule maps regulation activation intervals to version strings. Returns `[]` if
`recorded_at` falls outside all known intervals.

*Error rate:* <1%. Regulation schedules have hard effective dates (e.g., "CFPB Rule 2023-01
effective 2023-07-01"). The only ambiguity is events recorded on the exact boundary date.

*Consequence of wrong inference:* A compliance audit cites the wrong regulation version for
a historical credit analysis. Severity: medium — a regulator examining the audit trail would
see a regulation version that was not actually in effect. This is detectable by cross-referencing
the regulation schedule.

*Why not null:* An empty `regulatory_basis` list is auditable — it signals "regulation version
unknown for this historical event". A wrong regulation citation is worse than an empty one.
The implementation returns `[]` when `recorded_at` falls outside all schedule entries, which
is the correct fallback.

### DecisionGenerated v1→v2

v2 adds `model_versions: dict[session_id, model_version]`.

**`model_versions`** — reconstructed by loading each contributing session's `AgentSessionStarted`
event and extracting `model_version` from its payload.

*Error rate:* ~0% for sessions that exist in the store. The `AgentSessionStarted` event is
always the first event in a session stream (Gas Town ordering), and `model_version` is a
required field. The only failure case is a session stream that was deleted or never written
(e.g., a session ID referenced in a v1 event that predates the Gas Town requirement).

*Consequence of wrong inference:* `model_versions{}` maps session IDs to wrong model versions.
The `AgentPerformanceLedger` attributes decisions to wrong model versions. Same severity as
`model_version` in `CreditAnalysisCompleted` — analytics impact, not decision impact.

*Cache design:* Session lookups are cached in `_session_model_version_cache: dict[str, str |
None]` at module level. During bulk replay via `load_all()`, `resolve_decision_model_versions()`
pre-populates the cache for all session IDs before the replay loop starts, avoiding N+1 DB
queries. The cache is never invalidated during a process lifetime — session model versions are
immutable once written.

*Sessions not in cache:* Map to `None` (not omitted). The key is always present in
`model_versions{}` so downstream code can detect the gap. A missing key would be ambiguous
(not yet resolved vs. genuinely unknown).

### General principle: when to choose null over inference

Null is correct when:
1. The inferred value would be used in a safety or compliance rule (confidence floor, regulatory
   citation). Wrong inference here has audit consequences.
2. The error rate of inference exceeds the tolerance of the downstream consumer. For analytics,
   ~2% error is acceptable. For compliance records, <0.1% is the threshold.
3. The field is continuous or high-cardinality and the only proxy is categorical. Mapping
   `risk_tier` → `confidence_score` fabricates precision.

Inference is acceptable when:
1. The field is used for analytics/grouping, not safety rules.
2. The inference source (deployment schedule, regulation schedule) has hard boundaries with
   documented error rates.
3. The alternative (null) creates an "unknown" bucket that is less useful than a ~2% wrong
   value for the intended use case.

---

## 29.5 EventStoreDB Comparison

### Concept mapping

| The Ledger (PostgreSQL) | EventStoreDB equivalent | Notes |
|---|---|---|
| `events` table | Event log | Both are append-only, ordered by position |
| `global_position GENERATED ALWAYS AS IDENTITY` | `$all` stream position | EventStoreDB uses a 64-bit log position; PostgreSQL uses a BIGINT identity |
| `stream_id` TEXT | Stream name | EventStoreDB uses `/` as a category separator (`loan-APP-001` → category `loan`) |
| `event_streams` table | Stream metadata | EventStoreDB stores this internally; no separate table needed |
| `UNIQUE (stream_id, stream_position)` | Optimistic concurrency | EventStoreDB has native `ExpectedVersion` in the append API |
| `SELECT ... FOR UPDATE` on `event_streams` | EventStoreDB's internal locking | EventStoreDB handles this in its storage engine; we implement it explicitly |
| `EventStore.load_all(from_position, event_types)` | `$all` stream subscription with filter | EventStoreDB's `$all` stream is a first-class concept; we simulate it with `WHERE global_position > $1` |
| `ProjectionDaemon` polling loop | Persistent subscriptions | EventStoreDB persistent subscriptions push events to consumers; our daemon polls |
| `projection_checkpoints` table | Persistent subscription checkpoint | EventStoreDB stores the checkpoint server-side; we store it in PostgreSQL |
| `UpcasterRegistry` | Not built-in | EventStoreDB has no native upcasting; this is always application-side |
| `outbox` table | Not needed | EventStoreDB can serve as the message bus directly via subscriptions |
| `archive_stream()` sets `archived_at` | `$deleted` stream | EventStoreDB has a soft-delete concept via `$deleted` metadata |
| `AuditLedger` stream | Any stream | EventStoreDB has no special governance stream type; it's a naming convention |
| `compliance_snapshots` table | Not built-in | EventStoreDB has no native snapshot support; this is always application-side |

### What EventStoreDB provides that PostgreSQL must work harder to achieve

**1. Push-based subscriptions vs. polling.**
EventStoreDB persistent subscriptions push events to consumers as they are written. The
`ProjectionDaemon` polls at 100ms intervals — meaning up to 100ms of additional lag beyond
the write latency, plus the overhead of a `SELECT` query on every poll cycle even when there
are no new events. EventStoreDB eliminates this overhead entirely. At high event rates (>1000
events/second), polling becomes a bottleneck; push-based delivery does not.

**2. Category streams (`$ce-loan`).**
EventStoreDB automatically maintains category streams: all events from streams named `loan-*`
are also available in `$ce-loan`. This allows a projection to subscribe to all loan events
without knowing individual stream IDs. In the PostgreSQL implementation, `load_all(event_types=
["ApplicationSubmitted", "CreditAnalysisCompleted", ...])` achieves the same result, but
requires the caller to enumerate all relevant event types. A new event type added to the loan
aggregate must be explicitly added to the filter list — a maintenance burden that EventStoreDB's
category streams eliminate.

**3. Native optimistic concurrency in the protocol.**
EventStoreDB's append API accepts `ExpectedVersion` as a first-class parameter. The server
enforces it atomically without requiring a `SELECT FOR UPDATE` + application-level check. The
PostgreSQL implementation uses a serializable transaction with `SELECT FOR UPDATE` plus the
`UNIQUE (stream_id, stream_position)` constraint as a secondary safety net — two layers of
enforcement that EventStoreDB collapses into one.

**4. Built-in projections engine.**
EventStoreDB has a server-side JavaScript projections engine that can maintain state across
streams without a separate daemon process. The `ProjectionDaemon` + `projection_checkpoints`
table is a reimplementation of this capability in Python. The PostgreSQL approach is more
flexible (projections can use any Python logic, including ML models) but requires more
operational overhead (the `Worker_Service` process, checkpoint management, fault tolerance
logic).

**5. No outbox needed.**
Because EventStoreDB can serve as the message bus directly (consumers subscribe to streams),
the transactional outbox pattern is unnecessary. The PostgreSQL implementation needs the
`outbox` table to guarantee at-least-once delivery to Redis Streams — an extra table, an extra
process (`OutboxProcessor`), and an extra failure mode (outbox rows that fail to publish).

**6. Guaranteed global ordering without identity columns.**
EventStoreDB's `$all` stream position is a monotonically increasing log offset maintained by
the storage engine. The PostgreSQL `global_position GENERATED ALWAYS AS IDENTITY` achieves the
same result, but identity columns have a known gap risk: if a transaction inserts a row and
then rolls back, the identity value is consumed and the sequence has a gap. This does not
affect correctness (gaps in `global_position` are handled by `WHERE global_position > $1`),
but it means `global_position` cannot be used as a count of total events.

---

## 29.6 What I Would Do Differently

### The single most significant decision to reconsider: polling vs. LISTEN/NOTIFY

The `ProjectionDaemon` polls the `events` table at a fixed 100ms interval. This is the
simplest implementation that meets the SLO, but it is not the best version of the architecture.

**What the current implementation does:**

```python
async def run_forever(self, poll_interval_ms: int = 100) -> None:
    while self._running:
        await self._process_batch()
        await asyncio.sleep(poll_interval_ms / 1000)
```

Every 100ms, regardless of whether any new events exist, the daemon executes:
1. A `SELECT` on `events WHERE global_position > $1` (the min checkpoint query)
2. For each new event: a transaction with projection writes + checkpoint update

When the system is idle (no new events), this is 10 `SELECT` queries per second per daemon
instance, returning empty result sets. At scale with multiple `Worker_Service` instances, this
becomes significant read load on PostgreSQL.

**The gap between "meets the SLO" and "best version":**

The 500ms lag SLO for `ApplicationSummary` is met with a 100ms poll interval — there is a
400ms margin. But the SLO is met by burning CPU and DB connections on empty polls. The
`ProjectionDaemon` does not distinguish between "no new events" and "new events available" —
it always polls.

**What the best version looks like:**

PostgreSQL's `LISTEN/NOTIFY` mechanism allows the `EventStore.append` to send a notification
on a channel (e.g., `NOTIFY new_events, 'global_position=42'`) after committing. The
`ProjectionDaemon` would `LISTEN` on that channel and wake up only when new events are
available:

```python
async def run_forever(self) -> None:
    async with self._pool.acquire() as conn:
        await conn.add_listener("new_events", self._on_notify)
        while self._running:
            await asyncio.sleep(1)  # heartbeat only; real work triggered by NOTIFY

async def _on_notify(self, conn, pid, channel, payload):
    await self._process_batch()
```

This eliminates empty polls entirely. The daemon wakes up within milliseconds of a new event
being committed — reducing lag from up to 100ms to near-zero, while also reducing idle DB load
by ~99%.

**Why polling was chosen instead:**

`LISTEN/NOTIFY` introduces a new failure mode: if the `Worker_Service` loses its connection
to PostgreSQL (network blip, connection pool exhaustion), it stops receiving notifications and
falls behind silently. The polling approach is self-healing — on reconnect, the next poll
catches up from the last checkpoint. `LISTEN/NOTIFY` requires explicit reconnection logic and
a fallback poll on reconnect to catch any events missed during the outage.

The polling implementation is correct, operationally simple, and meets all SLOs. The
`LISTEN/NOTIFY` approach is the right long-term architecture for a production system where
idle DB load matters and sub-100ms projection lag is required. The migration path is
straightforward: add `NOTIFY` to `EventStore.append`, add `LISTEN` to `ProjectionDaemon`,
keep the 100ms poll as a fallback heartbeat. The checkpoint mechanism and fault tolerance
logic remain unchanged.

**The architectural lesson:**

The gap between "meets the SLO" and "best version" is not about correctness — the polling
implementation is correct. It is about the cost of correctness at scale. At 100 applications
with 4 agents, 10 empty polls/second is negligible. At 10,000 applications with 20 agents,
it is a meaningful fraction of PostgreSQL's query capacity. The right time to make this change
is before the system reaches that scale, not after — because `LISTEN/NOTIFY` requires changes
to both `EventStore.append` (the write path) and `ProjectionDaemon` (the read path), and
those changes are easier to make before the codebase has grown around the polling assumption.
