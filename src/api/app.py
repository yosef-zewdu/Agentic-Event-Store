"""
src/api/app.py — FastAPI HTTP wrapper for The Ledger frontend viewer.

Exposes read-only endpoints that the browser-based frontend can call directly.
Shares the same EventStore + projections as the MCP server.

Run with:
    PYTHONPATH=. uvicorn src.api.app:app --port 8000
"""
from __future__ import annotations

import hashlib
import json
import os
from contextlib import asynccontextmanager
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

    yield

    _daemon_task.cancel()
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
    from datetime import timezone

    proj = ComplianceAuditViewProjection()

    if as_of:
        try:
            ts = datetime.fromisoformat(as_of)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(400, detail=f"Invalid as_of timestamp: {as_of!r}. Use ISO 8601 format.")
        rows = await proj.get_compliance_at(app_id, ts, _pool)
        if not rows:
            raise HTTPException(404, detail=f"No compliance records for {app_id!r} at {as_of}")
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
async def get_audit_trail(app_id: str):
    stream_id = f"audit-{app_id}"
    events = await _store.load_stream(stream_id)
    return [
        {
            "event_id": str(e.event_id),
            "stream_position": e.stream_position,
            "event_type": e.event_type,
            "recorded_at": _dt(e.recorded_at),
            "payload": e.payload,
        }
        for e in events
    ]


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
        f"audit-{app_id}",
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
