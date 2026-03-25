# The Ledger — Agentic Event Store

PostgreSQL-backed, append-only event store and enterprise audit infrastructure for multi-agent AI
systems operating in regulated environments. Five AI agents collaborate on commercial loan
applications; every credit decision, compliance check, fraud screening, and human review is
permanently recorded as an immutable event and cryptographically auditable.

---

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) — `pip install uv` or `brew install uv`
- PostgreSQL 14+ running locally (or a managed instance)

---

## Installation

```bash
# Clone and enter the project
git clone <repo-url>
cd agentic-event-store

# Install all dependencies into an isolated .venv
uv sync

# Install dev dependencies (pytest, hypothesis, faker)
uv sync --dev

# Copy environment config
cp .env.example .env
```

Edit `.env` with your database credentials:

```dotenv
DATABASE_URL=postgresql://postgres:yourpassword@localhost/apexledger
TEST_DATABASE_URL=postgresql://postgres:yourpassword@localhost/apexledger_test
OPENROUTER_API_KEY=your_openrouter_key
OPENROUTER_MODEL=google/gemini-2.0-flash-001
REGULATION_VERSION=2026-Q1
LOG_LEVEL=INFO
```

---

## Database Provisioning

Create the application and test databases:

```bash
psql -U postgres -c "CREATE DATABASE apexledger;"
psql -U postgres -c "CREATE DATABASE apexledger_test;"
```

Apply the schema to both databases:

```bash
psql -U postgres -d apexledger      -f src/schema.sql
psql -U postgres -d apexledger_test -f src/schema.sql
```

The schema creates:

| Table | Purpose |
|---|---|
| `events` | Append-only event log; `UNIQUE (stream_id, stream_position)` |
| `event_streams` | One row per aggregate instance; tracks `current_version` for OCC |
| `outbox` | Transactional outbox for downstream messaging |
| `projection_checkpoints` | Daemon resume positions per projection |
| `application_summary` | Projection — current state of each loan application |
| `agent_performance_ledger` | Projection — per-agent model performance metrics |
| `compliance_audit_view` | Projection — regulatory read model with temporal query support |
| `compliance_snapshots` | Snapshot store for `ComplianceAuditView` (event-count trigger) |

Projection tables are created automatically by the MCP server and API on startup via
`CREATE TABLE IF NOT EXISTS`. You do not need to create them manually.

To reset the database during development:

```bash
psql -U postgres -d apexledger -c "
  TRUNCATE TABLE outbox, events, event_streams, projection_checkpoints RESTART IDENTITY CASCADE;
"
```

---

## Running the Pipeline

Submit an application and run all five agents in sequence:

```bash
PYTHONPATH=. uv run python run_pipeline.py \
  --app APEX-0012 \
  --submit \
  --applicant COMP-012 \
  --amount 500000
```

Resume from a specific agent (e.g. after a failure):

```bash
PYTHONPATH=. uv run python run_pipeline.py --app APEX-0012 --from-agent credit
```

---

## Running Tests

### Phase 1 — Event Store Core

```bash
uv run pytest tests/test_event_store.py tests/test_concurrency.py -v
```

Key assertions:
- Two concurrent `append(expected_version=N)` calls → exactly one succeeds, loser raises
  `OptimisticConcurrencyError`.
- Round-trip: appended events equal loaded events in order and count.

### Phase 2 — Domain Logic

```bash
uv run pytest tests/test_aggregates.py tests/test_command_handlers.py -v
```

Key assertions:
- `LoanApplication` enforces the 8-state machine; invalid transitions raise `DomainError`.
- `AgentSession` enforces Gas Town ordering: `AgentSessionStarted` → `AgentContextLoaded` →
  decision events.
- `handle_generate_decision` loads `ComplianceRecord` at the service layer.

### Phase 3 — Projections & Async Daemon

```bash
uv run pytest tests/test_projections.py tests/test_projection_daemon.py \
              tests/test_application_summary.py -v
```

Key assertions:
- `ApplicationSummary` lag < 500ms under 50 concurrent command handlers.
- `ComplianceAuditView` lag < 2000ms.
- `rebuild_from_scratch()` serves live reads during rebuild; result is consistent after atomic swap.
- Processing the same event batch twice produces identical projection state (idempotency).

### Phase 4 — Upcasting, Integrity & Gas Town

```bash
uv run pytest tests/test_upcasting.py tests/test_audit_chain.py tests/test_gas_town.py -v
```

Key assertions:
- `CreditAnalysisCompleted` v1 loaded via `load_stream()` returns v2 payload; raw DB row unchanged.
- `run_integrity_check()` appends `AuditIntegrityCheckRun` and returns `chain_valid=True`.
- Tampered event payload → `chain_valid=False`, `tamper_detected=True`.
- `reconstruct_agent_context()` returns `session_health_status="NEEDS_RECONCILIATION"` when a
  partial decision exists.

### Phase 5 — MCP Server

```bash
uv run pytest tests/test_mcp_lifecycle.py -v
```

### Narrative Scenario Tests

```bash
uv run pytest tests/test_narratives.py -v
```

Tests five end-to-end scenarios:
- NARR-01: Concurrent OCC collision — two agents race to write the same stream
- NARR-02: Document extraction with missing EBITDA
- NARR-04: Montana compliance hard block (REG-003)
- NARR-05: Human override (DECLINE → APPROVE)

### Full Suite

```bash
uv run pytest -v
```

---

## Application Viewer (Frontend)

A browser-based viewer lets you inspect any loan application across nine tabs.

### Start the viewer

```bash
PYTHONPATH=. uv run python -m uvicorn src.api.app:app --port 8000
```

Open **http://localhost:8000**, enter an application ID (e.g. `APEX-0012`), and hit Load.

### Tabs

| Tab | Description |
|---|---|
| Loan Stream | Full event timeline for `loan-{id}` stream |
| Agent Streams | Per-agent session event timelines |
| All Events | Every event across all streams, sorted by global position |
| Compliance | Compliance audit records with point-in-time query |
| Agent Sessions | Agent session summaries with event details |
| Token Usage | Per-agent LLM token counts, costs, fallbacks |
| Integrity | SHA-256 chain verification with baseline management |
| Audit Trail | `audit-{id}` stream with from/to position pagination |
| Ledger Health | Projection checkpoint lag |

### REST API Endpoints

| Endpoint | Description |
|---|---|
| `GET /applications/{id}` | Summary from `application_summary` projection |
| `GET /applications/{id}/events` | Full loan event stream |
| `GET /applications/{id}/compliance` | Compliance audit records |
| `GET /applications/{id}/compliance?as_of=<ISO8601>` | Point-in-time compliance state |
| `GET /applications/{id}/audit-trail` | Audit ledger stream |
| `GET /applications/{id}/audit-trail?from_pos=N&to_pos=M` | Paginated audit trail |
| `GET /applications/{id}/agents` | Agent sessions for this application |
| `GET /applications/{id}/all-events` | All events across all related streams |
| `GET /applications/{id}/token-usage` | Per-agent token and cost breakdown |
| `GET /applications/{id}/integrity` | Read-only chain verification (no writes) |
| `POST /applications/{id}/integrity/run` | Run full integrity check, write baseline |
| `GET /ledger/health` | Projection checkpoint lag |
| `GET /ledger/pricing` | Active model and pricing table |

### Integrity Tab

The integrity tab uses a two-phase approach:

- `GET /integrity` — read-only, verifies against a stored baseline if one exists. Returns
  `verified: false` with reason `"no baseline"` on first load.
- `POST /integrity/run` — explicitly establishes or extends the baseline by writing an
  `AuditIntegrityCheckRun` event to the audit stream. Click "Run Check" in the UI.

This prevents the audit stream from being polluted on every page load.

---

## MCP Server

The MCP server exposes all commands as Tools and all projections as Resources.

### Start the server

```bash
PYTHONPATH=. uv run fastmcp run src/mcp/server.py:mcp
```

To open the interactive MCP Inspector UI:

```bash
PYTHONPATH=. uv run fastmcp dev inspector src/mcp/server.py:mcp --with-editable
```

### Available Tools (Commands)

| # | Tool | Description |
|---|---|---|
| 1 | `submit_application` | Submit a new commercial loan application |
| 2 | `request_credit_analysis` | Transition application to CREDIT_ANALYSIS_REQUESTED |
| 3 | `record_credit_analysis` | Record a completed credit analysis from an AI agent |
| 4 | `request_fraud_screening` | Transition application to FRAUD_SCREENING_REQUESTED |
| 5 | `record_fraud_screening` | Record fraud screening result (validates `fraud_score ∈ [0,1]`) |
| 6 | `record_compliance_check` | Record compliance rule verdicts |
| 7 | `generate_decision` | Generate loan decision (confidence < 0.6 → REFER) |
| 8 | `request_human_review` | Transition application to PENDING_HUMAN_REVIEW |
| 9 | `record_human_review` | Record loan officer's APPROVE or DECLINE |
| 10 | `start_agent_session` | Start agent session (Gas Town ordering) |
| 11 | `run_integrity_check` | Run cryptographic hash-chain check (requires compliance role, rate-limited 1/min) |

### Available Resources (Queries)

| Resource URI | Description | SLO p99 |
|---|---|---|
| `ledger://applications/{id}` | Current state from `ApplicationSummary` projection | < 50ms |
| `ledger://applications/{id}/compliance` | Compliance audit view (current state) | < 200ms |
| `ledger://applications/{id}/compliance/{as_of}` | Point-in-time compliance state at ISO 8601 timestamp | < 200ms |
| `ledger://applications/{id}/audit-trail` | Direct `AuditLedger` stream load | < 500ms |
| `ledger://agents/{id}/performance` | Agent performance metrics | < 50ms |
| `ledger://agents/{id}/sessions/{session_id}` | Full `AgentSession` stream | < 300ms |
| `ledger://ledger/health` | Projection lag metrics | < 10ms |

---

## Stream Naming Convention

| Stream prefix | Aggregate |
|---|---|
| `loan-{application_id}` | LoanApplication |
| `session-{session_id}` | AgentSession |
| `credit-{application_id}` | Credit analysis record |
| `fraud-{application_id}` | Fraud screening record |
| `compliance-{application_id}` | ComplianceRecord |
| `docpkg-{application_id}` | Document package |
| `audit-{application_id}` | AuditLedger |

---

## Key Design Decisions

**Optimistic concurrency control** — `INSERT ... ON CONFLICT DO NOTHING` ensures the stream row
exists before acquiring a `SELECT FOR UPDATE` lock. This prevents the race condition where two
concurrent transactions both attempt to create the same new stream — exactly one wins, the other
raises `OptimisticConcurrencyError`.

**Self-contained payloads** — every event payload includes all fields needed to understand it
without joining other streams.

**Gas Town ordering** — `AgentSessionStarted` must be the first event in a session stream,
`AgentContextLoaded` must be second. Any decision event before context is loaded raises
`DomainError`.

**Compliance at the service layer** — aggregates never load other aggregates' streams. The
`handle_generate_decision` command handler loads `ComplianceRecordAggregate` separately.

**Projection-only reads** — all MCP resources read from projection tables except two named
exceptions: `audit-trail` (direct `AuditLedger` stream) and `sessions/{id}` (direct
`AgentSession` stream).

**LLM response parsing** — `_parse_json` in `BaseApexAgent` strips markdown code fences
(` ```json ... ``` `) before parsing, with regex fallback to extract the first `{...}` block.
This handles models that wrap JSON in markdown.

**Idempotent document agent** — the document agent checks for an existing `CreditAnalysisRequested`
event before writing one, preventing duplicates when the pipeline is re-run on an already-processed
application.

---

## Project Structure

```
agentic-event-store/
├── src/
│   ├── event_store.py              # EventStore (PostgreSQL) + InMemoryEventStore
│   ├── schema.sql                  # Full DB schema
│   ├── llm_factory.py              # LLM provider factory (OpenRouter, Groq, Gemini, Ollama)
│   ├── aggregates/
│   │   ├── loan_application.py     # 8-state machine + business rules
│   │   ├── agent_session.py        # Gas Town ordering enforcement
│   │   ├── compliance_record.py    # Rule verdicts + cleared check
│   │   └── audit_ledger.py         # Append-only governance stream
│   ├── agents/
│   │   ├── base_agent.py           # LangGraph base + session lifecycle
│   │   ├── document_processor.py   # PDF/XLSX extraction + quality assessment
│   │   ├── credit_analysis_agent.py
│   │   ├── fraud_detection_agent.py
│   │   ├── compliance_agent.py
│   │   ├── decision_orchestrator_agent.py
│   │   └── pipeline.py             # Sequential pipeline runner
│   ├── commands/
│   │   └── handlers.py             # load → validate → produce events → append
│   ├── models/
│   │   ├── events.py               # Domain event types + EVENT_REGISTRY
│   │   └── exceptions.py           # OptimisticConcurrencyError, DomainError
│   ├── projections/
│   │   ├── daemon.py               # ProjectionDaemon — polls events, routes to projections
│   │   ├── application_summary.py  # ApplicationSummary projection
│   │   ├── agent_performance.py    # AgentPerformanceLedger projection
│   │   └── compliance_audit.py     # ComplianceAuditView + temporal queries + snapshots
│   ├── upcasting/
│   │   ├── registry.py             # UpcasterRegistry — transparent schema migration on read
│   │   └── upcasters.py            # CreditAnalysisCompleted v1→v2, DecisionGenerated v1→v2
│   ├── integrity/
│   │   ├── audit_chain.py          # Hash-chain integrity check + read-only verify
│   │   └── gas_town.py             # Agent context reconstruction after crash
│   ├── mcp/
│   │   ├── server.py               # FastMCP server entry point + lifecycle
│   │   ├── tools.py                # 11 command tools
│   │   └── resources.py            # 7 query resources (incl. temporal compliance)
│   ├── api/
│   │   ├── app.py                  # FastAPI viewer backend
│   │   └── frontend.html           # Single-page application viewer
│   └── registry/
│       └── client.py               # ApplicantRegistryClient
├── tests/
│   ├── conftest.py
│   ├── test_event_store.py
│   ├── test_concurrency.py
│   ├── test_aggregates.py
│   ├── test_command_handlers.py
│   ├── test_projections.py
│   ├── test_projection_daemon.py
│   ├── test_upcasting.py
│   ├── test_audit_chain.py
│   ├── test_gas_town.py
│   ├── test_mcp_lifecycle.py
│   └── test_narratives.py
├── datagen/
│   └── generate_all.py
├── data/
│   ├── applicant_profiles.json
│   └── seed_events.jsonl
├── documents/
│   └── COMP-001 … COMP-080/
├── .env.example
├── pyproject.toml
├── Dockerfile
└── README.md
```

---

## Generating Seed Data

```bash
uv run python datagen/generate_all.py
```

Produces `data/applicant_profiles.json`, `data/seed_events.jsonl`, and populates
`documents/COMP-001` through `COMP-080` with PDFs and XLSX financial documents.
