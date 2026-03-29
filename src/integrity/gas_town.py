"""
src/integrity/gas_town.py — Gas Town context reconstruction (Reqs 7.4–7.5).

reconstruct_agent_context() loads the full agent session stream, detects
Partial_Decision states (unmatched request/completion pairs), preserves
verbatim the last 3 events and any PENDING/ERROR events, and summarises
older events into prose within the given token budget.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.event_store import EventStore
    from src.models.events import StoredEvent

# ---------------------------------------------------------------------------
# Request/completion pairs for Partial_Decision detection (Req 7.5)
# ---------------------------------------------------------------------------

_DECISION_PAIRS: list[tuple[str, str]] = [
    ("CreditAnalysisRequested", "CreditAnalysisCompleted"),
    ("FraudScreeningRequested", "FraudScreeningCompleted"),
    ("ComplianceCheckRequested", "ComplianceCheckCompleted"),
]

# Event types that indicate a PENDING or ERROR state
_PENDING_OR_ERROR_TYPES: frozenset[str] = frozenset({
    "AgentSessionFailed",
    "AgentInputValidationFailed",
    "DocumentUploadFailed",
    "ExtractionFailed",
    "DocumentFormatRejected",
    "CreditAnalysisDeferred",
})

# Payload status values that indicate PENDING or ERROR
_PENDING_OR_ERROR_STATUSES: frozenset[str] = frozenset({
    "PENDING", "ERROR", "FAILED", "DEFERRED",
})


# ---------------------------------------------------------------------------
# Result model
# ---------------------------------------------------------------------------

@dataclass
class PartialDecision:
    """Represents an unmatched request/completion pair."""
    request_event_type: str
    completion_event_type: str
    request_event_id: str
    stream_position: int


@dataclass
class AgentContext:
    """Reconstructed agent context returned by reconstruct_agent_context."""
    context_text: str
    last_event_position: int
    pending_work: list[PartialDecision] = field(default_factory=list)
    session_health_status: str = "OK"  # "OK" or "NEEDS_RECONCILIATION"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _find_partial_decisions(events: list) -> list[PartialDecision]:
    """
    Detect unmatched request/completion pairs in the event stream.

    For each known request/completion pair, check whether a request event
    exists without a corresponding completion event. Returns a list of
    PartialDecision instances for each unmatched pair found.
    """
    partial: list[PartialDecision] = []

    for request_type, completion_type in _DECISION_PAIRS:
        request_events = [e for e in events if e.event_type == request_type]
        completion_events = [e for e in events if e.event_type == completion_type]

        # Each request should have a matching completion; unmatched ones are partial
        unmatched_count = len(request_events) - len(completion_events)
        if unmatched_count > 0:
            # The unmatched requests are the last N request events (most recent)
            unmatched_requests = request_events[len(completion_events):]
            for req_event in unmatched_requests:
                partial.append(PartialDecision(
                    request_event_type=request_type,
                    completion_event_type=completion_type,
                    request_event_id=str(req_event.event_id),
                    stream_position=req_event.stream_position,
                ))

    return partial


def _is_pending_or_error(event) -> bool:
    """
    Return True if the event represents a PENDING or ERROR state.

    Checks both the event type (for known failure event types) and the
    payload for status fields containing PENDING/ERROR values.
    """
    if event.event_type in _PENDING_OR_ERROR_TYPES:
        return True

    # Check payload for status fields
    payload = event.payload or {}
    for key in ("status", "state", "health_status"):
        val = payload.get(key)
        if isinstance(val, str) and val.upper() in _PENDING_OR_ERROR_STATUSES:
            return True

    return False


def _token_count(events: list) -> int:
    """
    Estimate token count for a list of events.
    Approximation: 1 token ≈ 4 characters of formatted text.
    """
    total_chars = sum(len(_format_single_event(e)) for e in events)
    return max(1, total_chars // 4)


def _format_single_event(event) -> str:
    """Format a single event as a human-readable line."""
    ts = ""
    if event.recorded_at:
        ts = event.recorded_at.strftime("%Y-%m-%dT%H:%M:%SZ")

    payload = event.payload or {}

    # Build a concise description based on event type
    event_type = event.event_type
    parts = [f"[pos={event.stream_position}]"]
    if ts:
        parts.append(f"at {ts}")
    parts.append(event_type)

    # Add key payload fields for context
    for key in ("application_id", "session_id", "agent_id", "agent_type",
                "error_type", "error_message", "status", "state"):
        val = payload.get(key)
        if val:
            parts.append(f"{key}={val}")

    return " ".join(parts)


def _format_verbatim(events: list) -> str:
    """Format a list of events as verbatim readable text."""
    if not events:
        return ""
    lines = ["\n--- Verbatim Events ---"]
    for event in sorted(events, key=lambda e: e.stream_position):
        lines.append(_format_single_event(event))
        # Include full payload for verbatim events
        payload = event.payload or {}
        for k, v in payload.items():
            if v is not None and v != "" and v != [] and v != {}:
                lines.append(f"  {k}: {v}")
    return "\n".join(lines)


def _summarise_events(events: list, token_budget: int) -> str:
    """
    Produce a prose summary of older events within the token budget.

    Uses a simple word-count approximation: 1 token ≈ 4 chars.
    Builds a human-readable narrative of the session history.
    """
    if not events:
        return ""

    # Sort by stream position to build chronological narrative
    sorted_events = sorted(events, key=lambda e: e.stream_position)

    sentences: list[str] = []
    char_budget = token_budget * 4  # convert tokens to approximate chars
    used_chars = 0

    for event in sorted_events:
        sentence = _event_to_prose(event)
        if not sentence:
            continue

        sentence_chars = len(sentence) + 1  # +1 for space/newline
        if used_chars + sentence_chars > char_budget:
            # Add truncation notice and stop
            truncation = f"[{len(sorted_events) - len(sentences)} earlier events omitted due to token budget]"
            sentences.append(truncation)
            break

        sentences.append(sentence)
        used_chars += sentence_chars

    if not sentences:
        return ""

    return "--- Session Summary ---\n" + " ".join(sentences) + "\n"


def _event_to_prose(event) -> str:
    """Convert a single event to a human-readable prose sentence."""
    event_type = event.event_type
    payload = event.payload or {}

    ts = ""
    if event.recorded_at:
        ts = event.recorded_at.strftime("%Y-%m-%dT%H:%M:%SZ")

    app_id = payload.get("application_id", "")
    session_id = payload.get("session_id", "")
    agent_type = payload.get("agent_type", "")

    prose_map: dict[str, str] = {
        "AgentSessionStarted": (
            f"Session started at {ts}"
            + (f" for application {app_id}" if app_id else "")
            + (f" by {agent_type} agent" if agent_type else "")
            + "."
        ),
        "AgentInputValidated": (
            f"Agent input validated at {ts}"
            + (f" for application {app_id}" if app_id else "")
            + "."
        ),
        "AgentInputValidationFailed": (
            f"Agent input validation FAILED at {ts}"
            + (f" for application {app_id}" if app_id else "")
            + (f": {payload.get('validation_errors', '')}" if payload.get("validation_errors") else "")
            + "."
        ),
        "AgentNodeExecuted": (
            f"Node '{payload.get('node_name', '')}' executed at {ts}."
        ),
        "AgentToolCalled": (
            f"Tool '{payload.get('tool_name', '')}' called at {ts}."
        ),
        "AgentOutputWritten": (
            f"Agent output written at {ts}"
            + (f" for application {app_id}" if app_id else "")
            + "."
        ),
        "AgentSessionCompleted": (
            f"Session completed at {ts}"
            + (f" for application {app_id}" if app_id else "")
            + "."
        ),
        "AgentSessionFailed": (
            f"Session FAILED at {ts}"
            + (f": {payload.get('error_type', '')}" if payload.get("error_type") else "")
            + "."
        ),
        "AgentSessionRecovered": (
            f"Session recovered at {ts} from session {payload.get('recovered_from_session_id', '')}."
        ),
        "CreditAnalysisRequested": (
            f"Credit analysis requested at {ts}"
            + (f" for application {app_id}" if app_id else "")
            + "."
        ),
        "CreditAnalysisCompleted": (
            f"Credit analysis completed at {ts}"
            + (f" for application {app_id}" if app_id else "")
            + "."
        ),
        "FraudScreeningRequested": (
            f"Fraud screening requested at {ts}"
            + (f" for application {app_id}" if app_id else "")
            + "."
        ),
        "FraudScreeningCompleted": (
            f"Fraud screening completed at {ts}"
            + (f" for application {app_id}" if app_id else "")
            + "."
        ),
        "ComplianceCheckRequested": (
            f"Compliance check requested at {ts}"
            + (f" for application {app_id}" if app_id else "")
            + "."
        ),
        "ComplianceCheckCompleted": (
            f"Compliance check completed at {ts}"
            + (f" for application {app_id}" if app_id else "")
            + "."
        ),
    }

    return prose_map.get(event_type, f"Event {event_type} occurred at {ts}.")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def reconstruct_agent_context(
    store: "EventStore",
    agent_id: str,
    session_id: str,
    token_budget: int = 8000,
) -> AgentContext:
    """
    Reconstruct the agent context for a given session.

    Req 7.4: Load full session stream, summarise older events into prose
    within token_budget, preserve verbatim the last 3 events and any
    PENDING/ERROR events.

    Req 7.5: If the last event represents a Partial_Decision (unmatched
    request/completion pair), set session_health_status = "NEEDS_RECONCILIATION".

    Args:
        store: The EventStore instance to load events from.
        agent_id: The agent identifier.
        session_id: The session identifier.
        token_budget: Maximum tokens for the reconstructed context (default 8000).

    Returns:
        AgentContext with context_text, last_event_position, pending_work,
        and session_health_status.
    """
    events = await store.load_stream(f"session-{session_id}")

    if not events:
        return AgentContext(
            context_text="",
            last_event_position=0,
            pending_work=[],
            session_health_status="OK",
        )

    # Req 7.5 — detect Partial_Decision (unmatched request/completion pairs)
    partial = _find_partial_decisions(events)
    health = "NEEDS_RECONCILIATION" if partial else "OK"

    # Req 7.4 — preserve verbatim: last 3 events + any PENDING/ERROR events
    verbatim_ids = {e.event_id for e in events[-3:]}
    verbatim_ids |= {e.event_id for e in events if _is_pending_or_error(e)}

    verbatim_events = [e for e in events if e.event_id in verbatim_ids]
    older_events = [e for e in events if e.event_id not in verbatim_ids]

    # Reserve token budget for verbatim events, use remainder for summary
    verbatim_token_cost = _token_count(verbatim_events)
    summary_budget = max(0, token_budget - verbatim_token_cost)

    # Req 7.4 — summarise older events into prose within token budget
    summary = _summarise_events(older_events, token_budget=summary_budget)
    verbatim_text = _format_verbatim(verbatim_events)

    return AgentContext(
        context_text=summary + verbatim_text,
        last_event_position=events[-1].stream_position if events else 0,
        pending_work=partial,
        session_health_status=health,
    )
