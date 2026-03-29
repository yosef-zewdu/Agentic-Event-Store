"""
src/api/app.py — FastAPI HTTP wrapper for The Ledger frontend viewer.

Exposes read-only endpoints that the browser-based frontend can call directly.
Shares the same EventStore + projections as the MCP server.

Run with:
    PYTHONPATH=. uvicorn src.api.app:app --port 8000
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

from src.event_store import EventStore
from src.upcasting.registry import registry as upcaster_registry

load_dotenv(Path(__file__).parent.parent.parent / ".env")

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

_store: EventStore | None = None
_pool: asyncpg.Pool | None = None


@asynccontextmanager
async def _lifespan(app: FastAPI):
    import asyncio
    global _store, _pool
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    _store = EventStore(db_url=db_url, upcaster_registry=upcaster_registry)
    await _store.connect()
    _pool = _store._pool

    # Start projection daemon so application_summary stays current
    from src.projections.application_summary import ApplicationSummaryProjection
    from src.projections.agent_performance import AgentPerformanceLedgerProjection
    from src.projections.compliance_audit import ComplianceAuditViewProjection
    from src.projections.daemon import ProjectionDaemon

    app_proj = ApplicationSummaryProjection()
    agent_proj = AgentPerformanceLedgerProjection()
    compliance_proj = ComplianceAuditViewProjection()

    async with _pool.acquire() as conn:
        await app_proj.ensure_table_exists(conn)
        await agent_proj.ensure_table_exists(conn)
        await compliance_proj.ensure_table_exists(conn)

    daemon = ProjectionDaemon(store=_store, pool=_pool)
    daemon.register(app_proj)
    daemon.register(agent_proj)
    daemon.register(compliance_proj)
    _daemon_task = asyncio.create_task(daemon.run_forever(poll_interval_ms=200))

    # Start outbox processor so transactional outbox rows are delivered
    from src.outbox.processor import OutboxProcessor
    outbox_processor = OutboxProcessor(pool=_pool)
    _outbox_task = asyncio.create_task(outbox_processor.run_forever(poll_interval_ms=500))

    yield

    _daemon_task.cancel()
    _outbox_task.cancel()
    await _store.close()
    _store = None
    _pool = None


app = FastAPI(title="The Ledger — Application Viewer", lifespan=_lifespan)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dt(v: Any) -> str | None:
    if v is None:
        return None
    return v.isoformat() if hasattr(v, "isoformat") else str(v)


def _row(r: asyncpg.Record) -> dict:
    return dict(r)


# ---------------------------------------------------------------------------
# Serve the single-page frontend
# ---------------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = Path(__file__).parent / "frontend.html"
    return HTMLResponse(content=html_path.read_text())


# ---------------------------------------------------------------------------
# GET /applications/{id}  — summary from projection
# ---------------------------------------------------------------------------


@app.get("/applications/{app_id}")
async def get_application(app_id: str):
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1", app_id
        )
    if not row:
        raise HTTPException(404, detail=f"Application {app_id!r} not found")
    d = dict(row)
    d["last_event_at"] = _dt(d.get("last_event_at"))
    d["final_decision_at"] = _dt(d.get("final_decision_at"))
    # Normalize state field name
    if "state" in d and "current_state" not in d:
        d["current_state"] = d["state"]
    # agent_sessions_completed is JSONB — asyncpg returns it as a string
    asc = d.get("agent_sessions_completed")
    if isinstance(asc, str):
        d["agent_sessions_completed"] = json.loads(asc)
    return d


# ---------------------------------------------------------------------------
# GET /applications/{id}/events  — full event stream
# ---------------------------------------------------------------------------


@app.get("/applications/{app_id}/events")
async def get_events(app_id: str):
    stream_id = f"loan-{app_id}"
    events = await _store.load_stream(stream_id)
    if not events:
        raise HTTPException(404, detail=f"No events found for stream {stream_id!r}")
    return [
        {
            "event_id": str(e.event_id),
            "stream_position": e.stream_position,
            "global_position": e.global_position,
            "event_type": e.event_type,
            "event_version": e.event_version,
            "recorded_at": _dt(e.recorded_at),
            "payload": e.payload,
            "metadata": e.metadata,
        }
        for e in events
    ]


# ---------------------------------------------------------------------------
# GET /applications/{id}/compliance  — compliance audit view
# Optional ?as_of=<ISO timestamp> for point-in-time query (Req 11.3)
# ---------------------------------------------------------------------------


@app.get("/applications/{app_id}/compliance")
async def get_compliance(app_id: str, as_of: str | None = None):
    from src.projections.compliance_audit import ComplianceAuditViewProjection
    from datetime import datetime, timezone

    proj = ComplianceAuditViewProjection()

    if as_of:
        try:
            ts = datetime.fromisoformat(as_of)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(400, detail=f"Invalid as_of timestamp: {as_of!r}. Use ISO 8601 format.")
        rows = await proj.get_compliance_at(app_id, ts, _pool)
        return {"as_of": ts.isoformat(), "application_id": app_id, "records": rows}

    # Current state — no timestamp filter
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT application_id, event_id, event_type, rule_id, rule_version,
                   verdict, evaluation_timestamp, evidence_hash,
                   regulation_set_version, session_id, payload, recorded_at
            FROM compliance_audit_view
            WHERE application_id = $1
            ORDER BY recorded_at ASC
            """,
            app_id,
        )
    if not rows:
        raise HTTPException(404, detail=f"No compliance records for {app_id!r}")
    return [
        {
            "event_id": str(r["event_id"]),
            "event_type": r["event_type"],
            "rule_id": r["rule_id"],
            "rule_version": r["rule_version"],
            "verdict": r["verdict"],
            "evaluation_timestamp": _dt(r["evaluation_timestamp"]),
            "evidence_hash": r["evidence_hash"],
            "regulation_set_version": r["regulation_set_version"],
            "session_id": r["session_id"],
            "recorded_at": _dt(r["recorded_at"]),
            "payload": r["payload"] if isinstance(r["payload"], dict)
                       else json.loads(r["payload"]),
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# GET /applications/{id}/audit-trail  — audit ledger stream
# ---------------------------------------------------------------------------


@app.get("/applications/{app_id}/audit-trail")
async def get_audit_trail(app_id: str, from_pos: int = 0, to_pos: int | None = None):
    stream_id = f"audit-loan-{app_id}"
    events = await _store.load_stream(stream_id, from_position=from_pos, to_position=to_pos)
    return {
        "application_id": app_id,
        "stream_id": stream_id,
        "from_position": from_pos,
        "to_position": to_pos,
        "event_count": len(events),
        "events": [
            {
                "event_id": str(e.event_id),
                "stream_position": e.stream_position,
                "event_type": e.event_type,
                "recorded_at": _dt(e.recorded_at),
                "payload": e.payload,
            }
            for e in events
        ],
    }


# ---------------------------------------------------------------------------
# GET /applications/{id}/integrity  — read-only verify against stored baseline
# ---------------------------------------------------------------------------


@app.get("/applications/{app_id}/integrity")
async def get_integrity(app_id: str):
    from src.integrity.audit_chain import verify_integrity
    result = await verify_integrity(_store, entity_type="loan", entity_id=app_id)
    return {
        "application_id": app_id,
        "chain_valid": result.chain_valid,
        "tamper_detected": result.tamper_detected,
        "verified": result.verified,
        "reason": result.reason,
        "event_count": result.events_verified,
        "current_hash": result.current_hash,
        "baseline_hash": result.baseline_hash,
    }


# ---------------------------------------------------------------------------
# POST /applications/{id}/integrity/run  — write baseline to audit stream
# ---------------------------------------------------------------------------


@app.post("/applications/{app_id}/integrity/run")
async def run_integrity(app_id: str):
    from src.integrity.audit_chain import run_integrity_check
    result = await run_integrity_check(_store, entity_type="loan", entity_id=app_id)
    return {
        "application_id": app_id,
        "chain_valid": result.chain_valid,
        "tamper_detected": result.tamper_detected,
        "events_verified": result.events_verified,
        "integrity_hash": result.integrity_hash,
    }


# ---------------------------------------------------------------------------
# GET /applications/{id}/agents  — agent sessions that touched this application
# ---------------------------------------------------------------------------


@app.get("/applications/{app_id}/agents")
async def get_agents(app_id: str):
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT e.stream_id, e.payload
            FROM events e
            WHERE e.stream_id LIKE 'session-%'
              AND e.event_type = 'AgentSessionStarted'
              AND e.payload->>'application_id' = $1
            ORDER BY e.stream_id
            """,
            app_id,
        )

    sessions = []
    for r in rows:
        p = r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"])
        session_id = r["stream_id"]

        # Load full session stream
        session_events = await _store.load_stream(r["stream_id"])
        sessions.append(
            {
                "session_id": session_id,
                "agent_id": p.get("agent_id"),
                "agent_type": p.get("agent_type"),
                "model_version": p.get("model_version"),
                "started_at": _dt(session_events[0].recorded_at) if session_events else None,
                "event_count": len(session_events),
                "events": [
                    {
                        "event_type": e.event_type,
                        "recorded_at": _dt(e.recorded_at),
                        "payload": e.payload,
                    }
                    for e in session_events
                ],
            }
        )
    return sessions


# ---------------------------------------------------------------------------
# GET /ledger/health  — projection lag
# ---------------------------------------------------------------------------


@app.get("/applications/{app_id}/token-usage")
async def get_token_usage(app_id: str):
    """Return per-agent and total token usage for an application."""
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT e.stream_id, e.payload
            FROM events e
            WHERE e.stream_id IN (
                SELECT DISTINCT stream_id FROM events
                WHERE stream_id LIKE 'session-%'
                  AND event_type = 'AgentSessionStarted'
                  AND payload->>'application_id' = $1
            )
            AND e.event_type = 'AgentSessionCompleted'
            ORDER BY e.recorded_at
            """,
            app_id,
        )
    agents = []
    total_tokens = 0
    total_tokens_input = 0
    total_tokens_output = 0
    total_calls = 0
    total_cost = 0.0
    total_fallbacks = 0
    for r in rows:
        p = r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"])
        tok = p.get("total_tokens_used") or 0
        tok_in = p.get("total_tokens_input") or 0
        tok_out = p.get("total_tokens_output") or 0
        calls = p.get("total_llm_calls") or 0
        cost = p.get("total_cost_usd") or 0.0
        fallbacks = p.get("llm_fallback_count") or 0
        total_tokens += tok
        total_tokens_input += tok_in
        total_tokens_output += tok_out
        total_calls += calls
        total_cost += cost
        total_fallbacks += fallbacks
        agents.append({
            "agent_type": p.get("agent_type"),
            "session_id": p.get("session_id"),
            "total_tokens_used": tok,
            "total_tokens_input": tok_in,
            "total_tokens_output": tok_out,
            "total_llm_calls": calls,
            "total_cost_usd": cost,
            "total_duration_ms": p.get("total_duration_ms") or 0,
            "llm_fallback_count": fallbacks,
            "llm_errors": p.get("llm_errors") or [],
        })
    return {
        "application_id": app_id,
        "agents": agents,
        "totals": {
            "total_tokens_used": total_tokens,
            "total_tokens_input": total_tokens_input,
            "total_tokens_output": total_tokens_output,
            "total_llm_calls": total_calls,
            "total_cost_usd": round(total_cost, 6),
            "total_fallbacks": total_fallbacks,
        },
    }


@app.get("/applications/{app_id}/all-events")
async def get_all_events(app_id: str):
    """Return every event across all streams related to this application, sorted by global_position."""
    prefixes = [
        f"loan-{app_id}",
        f"credit-{app_id}",
        f"fraud-{app_id}",
        f"compliance-{app_id}",
        f"docpkg-{app_id}",
        f"audit-loan-{app_id}",
    ]
    async with _pool.acquire() as conn:
        # Also grab all agent-* streams that reference this application_id
        agent_stream_rows = await conn.fetch(
            """
            SELECT DISTINCT stream_id FROM events
            WHERE stream_id LIKE 'session-%'
              AND payload->>'application_id' = $1
            """,
            app_id,
        )
        agent_streams = [r["stream_id"] for r in agent_stream_rows]
        all_streams = prefixes + agent_streams

        rows = await conn.fetch(
            """
            SELECT stream_id, event_id, global_position, stream_position,
                   event_type, event_version, recorded_at, payload, metadata
            FROM events
            WHERE stream_id = ANY($1::text[])
            ORDER BY global_position ASC
            """,
            all_streams,
        )
    return [
        {
            "stream_id": r["stream_id"],
            "event_id": str(r["event_id"]),
            "global_position": r["global_position"],
            "stream_position": r["stream_position"],
            "event_type": r["event_type"],
            "event_version": r["event_version"],
            "recorded_at": _dt(r["recorded_at"]),
            "payload": r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"]),
            "metadata": r["metadata"] if isinstance(r["metadata"], dict) else (json.loads(r["metadata"]) if r["metadata"] else {}),
        }
        for r in rows
    ]


@app.post("/applications/{app_id}/complete-review")
async def complete_review(
    app_id: str,
    reviewer_id: str,
    decision: str,
    override: bool = False,
):
    """
    Complete a human review for an application in PENDING_HUMAN_REVIEW state.

    decision: "APPROVE" or "DECLINE"
    """
    from src.commands.handlers import handle_human_review_completed

    if decision.upper() not in {"APPROVE", "DECLINE"}:
        raise HTTPException(400, detail="decision must be APPROVE or DECLINE")
    try:
        version = await handle_human_review_completed(
            store=_store,
            application_id=app_id,
            reviewer_id=reviewer_id,
            decision=decision,
            override=override,
        )
        return {
            "success": True,
            "application_id": app_id,
            "stream_version": version,
            "final_decision": decision.upper(),
        }
    except Exception as exc:
        raise HTTPException(400, detail=str(exc))


@app.get("/ledger/pricing")
async def get_pricing():
    """Return the model pricing table from llm_factory."""
    from src.llm_factory import MODEL_PRICING, get_llm_config
    cfg = get_llm_config()
    return {
        "active_provider": cfg["provider"],
        "active_model": cfg["model"],
        "pricing": MODEL_PRICING,
    }


@app.get("/ledger/health")
async def get_health():
    # Only show the projections the daemon actually manages
    active = {"application_summary", "agent_performance_ledger", "compliance_audit_view"}
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT projection_name, last_position, updated_at FROM projection_checkpoints"
        )
        latest = await conn.fetchval(
            "SELECT global_position FROM events ORDER BY global_position DESC LIMIT 1"
        )
    return {
        "latest_global_position": latest,
        "projections": [
            {
                "name": r["projection_name"],
                "last_position": r["last_position"],
                "lag_events": (latest or 0) - (r["last_position"] or 0),
                "updated_at": _dt(r["updated_at"]),
            }
            for r in rows
            if r["projection_name"] in active
        ],
    }


# ---------------------------------------------------------------------------
# POST /demo/occ-collision  — live OCC collision demo for Step 2
# ---------------------------------------------------------------------------

@app.post("/demo/occ-collision")
async def demo_occ_collision():
    import time as _time
    from src.models.exceptions import OptimisticConcurrencyError

    test_stream = f"occ-demo-{int(_time.time())}"
    log = []

    seed = [
        {"event_type": "ApplicationSubmitted", "event_version": 1,
         "payload": {"application_id": test_stream, "seq": i}}
        for i in range(3)
    ]
    await _store.append(test_stream, seed, expected_version=-1, aggregate_type="demo")
    version = await _store.stream_version(test_stream)
    log.append({"event": "stream_seeded", "stream": test_stream, "version": version})

    results = {}

    async def agent(label: str):
        try:
            await _store.append(
                stream_id=test_stream,
                events=[{"event_type": "CreditAnalysisCompleted", "event_version": 2,
                         "payload": {"agent_id": f"agent-{label}", "stream": test_stream}}],
                expected_version=version,
                aggregate_type="demo",
            )
            results[label] = {"status": "success", "appended_at_version": version + 1}
        except OptimisticConcurrencyError as exc:
            results[label] = {
                "status": "occ_error",
                "expected_version": exc.expected_version,
                "actual_version": exc.actual_version,
                "suggested_action": exc.suggested_action,
            }

    await asyncio.gather(agent("A"), agent("B"))

    winner = next((k for k, v in results.items() if v["status"] == "success"), None)
    loser  = next((k for k, v in results.items() if v["status"] == "occ_error"), None)
    log.append({"event": "concurrent_append", "agent_A": results.get("A"), "agent_B": results.get("B")})

    retry_result = None
    if loser:
        new_version = await _store.stream_version(test_stream)
        await _store.append(
            stream_id=test_stream,
            events=[{"event_type": "CreditAnalysisCompleted", "event_version": 2,
                     "payload": {"agent_id": f"agent-{loser}-retry", "stream": test_stream}}],
            expected_version=new_version,
            aggregate_type="demo",
        )
        retry_result = {"agent": loser, "retried_at_version": new_version + 1, "status": "success"}
        log.append({"event": "retry", **retry_result})

    final_version = await _store.stream_version(test_stream)
    await _store.archive_stream(test_stream)

    return {
        "stream": test_stream,
        "initial_version": version,
        "final_version": final_version,
        "winner": winner,
        "loser": loser,
        "agent_A": results.get("A"),
        "agent_B": results.get("B"),
        "retry": retry_result,
        "log": log,
    }


# ---------------------------------------------------------------------------
# POST /demo/gas-town-recovery  — Step 5: Gas Town crash + context reconstruction
# ---------------------------------------------------------------------------

@app.post("/demo/gas-town-recovery")
async def demo_gas_town_recovery():
    """
    Simulates a Gas Town crash scenario:
    1. Creates a demo agent session with realistic events
    2. Appends a CreditAnalysisRequested (partial decision — no completion)
    3. Simulates crash (session stream left incomplete)
    4. Calls reconstruct_agent_context() to show recovery
    """
    import time as _time
    import src.upcasting.upcasters  # noqa
    from src.integrity.gas_town import reconstruct_agent_context

    session_id = f"demo-crash-{int(_time.time())}"
    stream_id  = f"session-{session_id}"
    app_id     = f"demo-app-{int(_time.time())}"
    agent_id   = "credit-agent-demo"

    # Build a realistic session event sequence
    now = datetime.now(timezone.utc).isoformat()
    session_events = [
        {"event_type": "AgentSessionStarted", "event_version": 1, "payload": {
            "session_id": session_id, "agent_id": agent_id,
            "agent_type": "CreditAnalysis", "application_id": app_id,
            "model_version": "google/gemini-2.0-flash", "context_source": "fresh",
            "context_token_count": 1000, "started_at": now}},
        {"event_type": "AgentContextLoaded", "event_version": 1, "payload": {
            "session_id": session_id, "agent_type": "CreditAnalysis",
            "application_id": app_id, "inputs_validated": ["loan_stream", "docpkg"],
            "validation_duration_ms": 12, "validated_at": now}},
        {"event_type": "AgentNodeExecuted", "event_version": 1, "payload": {
            "session_id": session_id, "node_name": "validate_inputs",
            "node_sequence": 1, "duration_ms": 15, "executed_at": now}},
        {"event_type": "AgentNodeExecuted", "event_version": 1, "payload": {
            "session_id": session_id, "node_name": "load_applicant_registry",
            "node_sequence": 2, "duration_ms": 45, "executed_at": now}},
        {"event_type": "AgentNodeExecuted", "event_version": 1, "payload": {
            "session_id": session_id, "node_name": "load_extracted_facts",
            "node_sequence": 3, "duration_ms": 22, "executed_at": now}},
        # Crash happens here — analyze_credit_risk node never completes
        # This leaves a CreditAnalysisRequested without a CreditAnalysisCompleted
        {"event_type": "CreditAnalysisRequested", "event_version": 1, "payload": {
            "session_id": session_id, "application_id": app_id,
            "requested_at": now}},
        # AgentSessionFailed — the crash event
        {"event_type": "AgentSessionFailed", "event_version": 1, "payload": {
            "session_id": session_id, "agent_type": "CreditAnalysis",
            "application_id": app_id,
            "error_type": "ProcessKilled",
            "error_message": "SIGKILL received — process terminated",
            "last_successful_node": "node_3",
            "recoverable": True, "failed_at": now}},
    ]

    await _store.append(stream_id, session_events, expected_version=-1, aggregate_type="agent_session")

    # Now reconstruct — gas_town uses agent-{agent_id}-{session_id} format
    # but our agents use session-{session_id}, so we load directly
    from src.integrity.gas_town import (
        _find_partial_decisions, _is_pending_or_error,
        _token_count, _summarise_events, _format_verbatim,
        AgentContext,
    )
    from src.models.events import StoredEvent

    events = await _store.load_stream(stream_id)

    partial = _find_partial_decisions(events)
    health  = "NEEDS_RECONCILIATION" if partial else "OK"

    verbatim_ids   = {e.event_id for e in events[-3:]}
    verbatim_ids  |= {e.event_id for e in events if _is_pending_or_error(e)}
    verbatim_events = [e for e in events if e.event_id in verbatim_ids]
    older_events    = [e for e in events if e.event_id not in verbatim_ids]

    summary_budget = max(0, 8000 - _token_count(verbatim_events))
    summary        = _summarise_events(older_events, token_budget=summary_budget)
    verbatim_text  = _format_verbatim(verbatim_events)
    context_text   = summary + verbatim_text

    # Archive the demo stream
    await _store.archive_stream(stream_id)

    return {
        "session_id": session_id,
        "stream_id": stream_id,
        "application_id": app_id,
        "events_written": len(session_events),
        "crash_at_node": "analyze_credit_risk",
        "crash_event": "AgentSessionFailed (ProcessKilled)",
        "reconstruction": {
            "session_health_status": health,
            "last_event_position": events[-1].stream_position if events else 0,
            "pending_work": [
                {
                    "request_event_type": pw.request_event_type,
                    "completion_event_type": pw.completion_event_type,
                    "stream_position": pw.stream_position,
                }
                for pw in partial
            ],
            "context_text_preview": context_text[:600],
            "verbatim_event_count": len(verbatim_events),
            "summarised_event_count": len(older_events),
        },
    }


@app.get("/demo/upcasting/{app_id}")
async def demo_upcasting(app_id: str):
    import src.upcasting.upcasters  # noqa — ensure upcasters are registered

    upcasted_event = None
    for stream_id in [f"loan-{app_id}", f"credit-{app_id}"]:
        events = await _store.load_stream(stream_id)
        upcasted_event = next(
            (e for e in events if e.event_type == "CreditAnalysisCompleted"), None
        )
        if upcasted_event:
            break

    if not upcasted_event:
        raise HTTPException(404, detail=f"No CreditAnalysisCompleted found for {app_id!r}. Run the pipeline first.")

    async with _pool.acquire() as conn:
        raw = await conn.fetchrow(
            "SELECT payload, event_version FROM events WHERE event_id = $1",
            upcasted_event.event_id,
        )

    raw_payload = raw["payload"] if isinstance(raw["payload"], dict) else json.loads(raw["payload"])
    raw_version = raw["event_version"]

    added_by_upcasting = {
        k: v for k, v in upcasted_event.payload.items()
        if k not in raw_payload
    }

    return {
        "event_id": str(upcasted_event.event_id),
        "stream_id": upcasted_event.stream_id,
        "raw_version": raw_version,
        "upcasted_version": upcasted_event.event_version,
        "raw_payload": raw_payload,
        "upcasted_payload": upcasted_event.payload,
        "added_by_upcasting": added_by_upcasting,
        "db_row_unchanged": True,  # raw_payload is read directly from DB, never modified
    }


# ---------------------------------------------------------------------------
# GET /demo/what-if/{app_id}  — Step 6: What-If Counterfactual demo
# ---------------------------------------------------------------------------

@app.get("/demo/what-if/{app_id}")
async def demo_what_if(app_id: str):
    """
    Substitutes HIGH risk tier for the real CreditAnalysisCompleted event.
    Replays the loan stream under both scenarios and returns the divergence.
    No writes to the real store — purely in-memory replay.
    """
    from src.aggregates.loan_application import LoanApplicationAggregate

    real_events = await _store.load_stream(f"loan-{app_id}")
    if not real_events:
        raise HTTPException(404, detail=f"No events found for {app_id!r}")

    branch_event = next(
        (e for e in real_events if e.event_type == "CreditAnalysisCompleted"), None
    )
    if not branch_event:
        raise HTTPException(404, detail=f"No CreditAnalysisCompleted found for {app_id!r}. Run the pipeline first.")

    branch_idx = next(i for i, e in enumerate(real_events) if e.event_type == "CreditAnalysisCompleted")

    # Real outcome
    real_agg = LoanApplicationAggregate(application_id=app_id)
    for e in real_events:
        real_agg._apply(e)

    real_decision = branch_event.payload.get("decision") or {}
    if isinstance(real_decision, str):
        real_decision = json.loads(real_decision)

    real_risk  = real_decision.get("risk_tier", "UNKNOWN")
    real_limit = float(real_decision.get("recommended_limit_usd") or 0)
    real_conf  = float(real_decision.get("confidence") or branch_event.payload.get("confidence_score") or 0)

    # Counterfactual — swap risk_tier to HIGH, halve the limit
    cf_events = []
    for e in real_events:
        if e.event_type == "CreditAnalysisCompleted":
            cf_dec = dict(real_decision)
            cf_dec["risk_tier"] = "HIGH"
            cf_dec["recommended_limit_usd"] = real_limit * 0.5
            cf_dec["rationale"] = "[COUNTERFACTUAL] Risk tier overridden to HIGH"
            cf_payload = {**e.payload, "decision": cf_dec}
            cf_events.append(e.model_copy(update={"payload": cf_payload}))
        else:
            cf_events.append(e)

    cf_agg = LoanApplicationAggregate(application_id=app_id)
    for e in cf_events:
        cf_agg._apply(e)

    # Identify business rules triggered by the substitution
    rules = []
    rules.append({
        "rule": "HIGH_RISK_POLICY",
        "description": f"risk_tier changed {real_risk} → HIGH",
        "effect": f"recommended_limit_usd halved: ${real_limit:,.0f} → ${real_limit * 0.5:,.0f}",
    })
    if real_conf < 0.6:
        rules.append({
            "rule": "CONFIDENCE_FLOOR",
            "description": f"confidence {real_conf:.2f} < 0.6",
            "effect": "Recommendation forced to REFER",
        })
    requested = float(real_agg.requested_amount_usd or 0)
    if real_limit * 0.5 < requested:
        rules.append({
            "rule": "APPROVED_AMOUNT_CAP",
            "description": f"Counterfactual limit ${real_limit * 0.5:,.0f} < requested ${requested:,.0f}",
            "effect": "Approved amount capped below requested",
        })

    return {
        "application_id": app_id,
        "branch_event_type": "CreditAnalysisCompleted",
        "branch_event_id": str(branch_event.event_id),
        "pre_branch_events": branch_idx,
        "real": {
            "risk_tier": real_risk,
            "recommended_limit_usd": real_limit,
            "confidence": real_conf,
            "final_state": real_agg.state.value,
            "recommendation": real_agg.recommendation,
        },
        "counterfactual": {
            "risk_tier": "HIGH",
            "recommended_limit_usd": real_limit * 0.5,
            "confidence": real_conf,
            "final_state": cf_agg.state.value,
            "recommendation": cf_agg.recommendation,
        },
        "business_rules_triggered": rules,
        "db_unchanged": True,
        "divergence_event_count": 1,
    }
