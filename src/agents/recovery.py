# src/agents/recovery.py -- Agent crash recovery infrastructure

from __future__ import annotations
import asyncio
from datetime import datetime, timedelta
from typing import List

from src.event_store import EventStore


async def detect_incomplete_sessions(store: EventStore, stale_threshold_minutes: int = 10) -> List[str]:
    """
    Detect agent sessions that have started but not completed, and are older than threshold.

    Returns list of session_ids that are incomplete and stale.
    """
    # This is a simplified version. In real implementation, query the database directly.
    # For now, since EventStore has load_stream, we can scan all session streams, but that's inefficient.
    # Assuming PostgreSQL, we can query the events table.

    # For the test, since it's MockStore, we can implement it there.

    # Placeholder: return empty list for now
    return []


async def recover_stale_sessions(store: EventStore, registry, client, model: str):
    """
    Find stale incomplete sessions and attempt to resume them.
    """
    stale_sessions = await detect_incomplete_sessions(store)
    for session_id in stale_sessions:
        # Parse agent_type from session_id or from the event
        # For simplicity, assume credit_analysis
        from src.agents.credit_analysis_agent import CreditAnalysisAgent
        agent = CreditAnalysisAgent(
            agent_id="recovery-agent",
            agent_type="credit_analysis",
            store=store,
            registry=registry,
            client=client,
        )
        # Extract app_id from session events
        session_events = await store.load_stream(f"session-{session_id}")
        started = next((e for e in session_events if e["event_type"] == "AgentSessionStarted"), None)
        if started:
            app_id = started["payload"]["application_id"]
            try:
                await agent.process_application(app_id, resume_session_id=session_id)
            except Exception as e:
                # Log error, perhaps mark as unrecoverable
                pass