# The Ledger — Agentic Event Store

PostgreSQL-backed event store and enterprise audit infrastructure for an AI-driven loan origination platform. Agents write decisions as immutable events; every credit decision, compliance check, and fraud screening is permanently recorded and cryptographically auditable.

---

## Prerequisites

- Python 3.12
- [uv](https://docs.astral.sh/uv/) — `pip install uv` or `brew install uv`
- PostgreSQL 14+ running locally

---

## Installation

```bash
# Clone and enter the project
git clone https://github.com/yosef-zewdu/Agentic-Event-Store.git
cd Agentic-Event-Store

# Install all dependencies (creates .venv automatically)
uv sync

# Copy environment config
cp .env.example .env
```

Edit `.env` and set your database credentials:

```dotenv
DATABASE_URL=postgresql://postgres:yourpassword@localhost/apexledger
TEST_DATABASE_URL=postgresql://postgres:yourpassword@localhost/apexledger_test
```

---

## Database Setup

Create the databases:

```bash
psql -U postgres -c "CREATE DATABASE apexledger;"
psql -U postgres -c "CREATE DATABASE apexledger_test;"
```

Run the migration (applies schema to both):

```bash
psql -U postgres -d apexledger -f src/schema.sql
psql -U postgres -d apexledger_test -f src/schema.sql
```

The schema creates:

- `events` — append-only event log with `UNIQUE (stream_id, stream_position)`
- `event_streams` — one row per aggregate instance, tracks `current_version`
- `outbox` — transactional outbox for downstream messaging
- `projection_checkpoints` — daemon resume positions
- `snapshots` — optional aggregate snapshots
- `applicant_registry.*` — read-only reference data (companies, financials, compliance flags)

---

## Running the Test Suite

```bash
# Full suite (requires DB)
uv run pytest

# Fast smoke check — in-memory only, no DB needed
uv run pytest -k "inmemory"

# Verbose with live output (useful for concurrency test)
uv run pytest tests/test_concurrency.py -v -s
```

### What each suite covers

| File | Phase | What it tests |
|---|---|---|
| `tests/test_event_store.py` | 1 | EventStore contract — runs against both `InMemoryEventStore` and PostgreSQL |
| `tests/test_concurrency.py` | 1 | Optimistic concurrency control — double-decision race, rollback, retry |
| `tests/test_aggregates.py` | 2 | State machines, business rules, aggregate load/replay |
| `tests/est_command_handlers.py` | 2 | Command handlers — full DB round-trips |

---

## Project Structure

```
agentic-event-store/
│
├── src/
│   ├── event_store.py                   # EventStore (PostgreSQL) + InMemoryEventStore
│   ├── schema.sql                       # Full DB schema — run this to migrate
│   │
│   ├── aggregates/
│   │   ├── loan_application.py          # State machine + business rules (Reqs 6, 8)
│   │   ├── agent_session.py             # Gas Town ordering enforcement (Req 7)
│   │   ├── compliance_record.py         # Rule verdicts + cleared check (Req 8.3)
│   │   └── audit_ledger.py              # Append-only governance stream (Req 14.6)
│   │
│   ├── commands/
│   │   └── handlers.py                  # load → validate → produce events → append
│   │
│   ├── models/
│   │   ├── events.py                    # 45 domain event types + EVENT_REGISTRY
│   │   └── exceptions.py                # OptimisticConcurrencyError, DomainError
│   │
│   ├── upcasting/
│   │   └── registry.py                  # Transparent schema migration on read
│   │
│   ├── agents/
│   │   ├── base_agent.py
│   │   └── credit_analysis_agent.py
│   │
│   └── registry/
│       └── client.py                    # Applicant registry client
│
├── tests/
│   ├── conftest.py                      # DB pool, store fixture, table truncation
│   ├── test_event_store.py              # Phase 1 — contract tests (inmemory + postgres)
│   ├── test_concurrency.py              # Phase 1 — OCC double-decision race
│   ├── test_aggregates.py           # Aggregate unit tests (no DB required)
│   └── test_command_handlers.py     # Command handler integration tests (DB)
│
├── datagen/
│   ├── generate_all.py                  # Entry point — generates all seed data
│   ├── company_generator.py
│   ├── event_simulator.py
│   ├── pdf_generator.py
│   └── excel_generator.py
│
├── data/
│   ├── applicant_profiles.json          # 80 synthetic companies
│   └── seed_events.jsonl                # Seed event stream
│
├── documents/
│   └── COMP-001 … COMP-080/             # PDF + XLSX financial docs per company
│       ├── application_proposal.pdf
│       ├── balance_sheet_2024.pdf
│       ├── financial_statements.xlsx
│       ├── financial_summary.csv
│       └── income_statement_2024.pdf
│
├── .env.example                         # Environment variable template
├── pyproject.toml                       # Dependencies + pytest config
├── Dockerfile
└── README.md
```

---

## Stream Naming Convention

Each aggregate type has a dedicated stream prefix:

| Stream prefix | Aggregate |
|---|---|
| `loan-{application_id}` | LoanApplication |
| `docpkg-{application_id}` | DocumentPackage |
| `session-{session_id}` | AgentSession |
| `credit-{application_id}` | CreditRecord |
| `compliance-{application_id}` | ComplianceRecord |
| `fraud-{application_id}` | FraudScreening |
| `audit-{entity_type}-{entity_id}` | AuditLedger |

---

## Version Convention

Stream versions are **1-based** and represent the count of events in the stream:

- Empty stream → `stream_version() == -1` (sentinel for "no events yet")
- After 1 event → `stream_version() == 1`
- After N events → `stream_version() == N`
- `stream_position` of the first event in any stream is `1`

Pass `expected_version=-1` to create a new stream. Pass the current version to append to an existing one. A mismatch raises `OptimisticConcurrencyError` with `suggested_action="reload_stream_and_retry"`.

---

## Key Design Decisions

**Optimistic concurrency control** — `SELECT FOR UPDATE` on `event_streams` inside a serializable transaction. Two agents racing to append at the same `expected_version` will have exactly one winner; the loser gets `OptimisticConcurrencyError` and must reload and retry.

**Self-contained payloads** — every event payload includes all fields needed to understand it without joining other streams (e.g. `CreditAnalysisCompleted` carries `application_id`, `session_id`, `risk_tier`, `recommended_limit_usd` directly).

**Gas Town ordering** — `AgentSessionStarted` must be the first event in a session stream, `AgentContextLoaded` must be second. Any decision event before context is loaded raises `DomainError`.

**Compliance at the service layer** — aggregates never load other aggregates' streams. The `handle_generate_decision` command handler loads `ComplianceRecordAggregate` separately and passes `compliance_cleared` into the decision logic.

---

## Generating Seed Data

```bash
uv run python datagen/generate_all.py
```

This produces `data/applicant_profiles.json` and `data/seed_events.jsonl`, and populates `documents/COMP-001` through `COMP-080` with PDFs and XLSX files.
