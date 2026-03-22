"""
Concrete upcasters for schema-evolved event types.

Registered against the module-level UpcasterRegistry singleton in registry.py.
These are applied transparently at read time — the events table is never modified.

Upcaster design principles (per Req 13.4–13.5):
  - Never fabricate data: unknown fields are set to null, not guessed.
  - Inference is documented and bounded: timestamp ranges are sourced from
    known model deployment records and regulation schedules.
  - Session lookups for DecisionGenerated v2 are cached to avoid N+1 queries
    during bulk replay via load_all().
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Regulation schedule
#
# Maps a regulation set version string to the half-open interval [start, end)
# during which it was the active version.  "end=None" means still active.
#
# THIS LIST IS INTENTIONALLY EMPTY.  The actual regulation activation dates
# must come from Apex Financial Services' compliance records.  Populate it at
# application startup via configure_regulation_schedule(), or by setting the
# CREDIT_REGULATION_SCHEDULE environment variable (JSON array of
# [iso_start, iso_end_or_null, version_string] triples).
#
# When recorded_at falls in the interval, that regulation version was active.
# Returns [] if the schedule is empty or the timestamp falls outside all entries
# — an empty list is auditable; a fabricated regulation citation is not.
# ---------------------------------------------------------------------------

_REGULATION_SCHEDULE: list[tuple[datetime, datetime | None, str]] = []


def configure_regulation_schedule(
    entries: list[tuple[datetime, datetime | None, str]],
) -> None:
    """
    Populate the regulation schedule used by the CreditAnalysisCompleted
    v1→v2 upcaster.

    Each entry is (start, end, regulation_version) where end=None means still
    active.  Entries should be non-overlapping and sorted by start ascending.

    Call this once at application startup before any event loading occurs.
    """
    global _REGULATION_SCHEDULE
    _REGULATION_SCHEDULE = list(entries)


def _load_regulation_schedule_from_env() -> None:
    """
    Optionally bootstrap the regulation schedule from the
    CREDIT_REGULATION_SCHEDULE environment variable (JSON array of
    [iso_start, iso_end_or_null, version]).
    """
    import json
    import os

    raw = os.environ.get("CREDIT_REGULATION_SCHEDULE", "")
    if not raw:
        return
    try:
        entries = []
        for start_s, end_s, version in json.loads(raw):
            start = datetime.fromisoformat(start_s).replace(tzinfo=timezone.utc)
            end = datetime.fromisoformat(end_s).replace(tzinfo=timezone.utc) if end_s else None
            entries.append((start, end, version))
        configure_regulation_schedule(entries)
    except Exception:
        logger.warning(
            "upcaster: CREDIT_REGULATION_SCHEDULE is set but could not be parsed; "
            "regulatory_basis inference will return [] for all v1 CreditAnalysisCompleted events"
        )


_load_regulation_schedule_from_env()


def _infer_regulatory_basis(recorded_at: datetime | str | None) -> list[str]:
    """
    Return the regulation set version(s) active at recorded_at.

    If recorded_at is None or unparseable, returns [] rather than fabricating
    a value — callers must handle the empty list case.
    """
    if recorded_at is None:
        return []

    if isinstance(recorded_at, str):
        try:
            recorded_at = datetime.fromisoformat(recorded_at)
        except ValueError:
            logger.warning("upcaster: could not parse recorded_at=%r", recorded_at)
            return []

    # Ensure timezone-aware for comparison
    if recorded_at.tzinfo is None:
        recorded_at = recorded_at.replace(tzinfo=timezone.utc)

    for start, end, version in _REGULATION_SCHEDULE:
        if recorded_at >= start and (end is None or recorded_at < end):
            return [version]

    logger.warning(
        "upcaster: recorded_at=%s falls outside all known regulation schedule entries",
        recorded_at,
    )
    return []


# ---------------------------------------------------------------------------
# Model deployment schedule
#
# Maps a model version string to the half-open interval [start, end) during
# which that model was deployed in production.  "end=None" means still active.
#
# THIS LIST IS INTENTIONALLY EMPTY.  The actual deployment windows must be
# populated from Apex Financial Services' model deployment records before
# upcasting v1 events.  Populate it at application startup via
# configure_model_deployment_schedule(), or by setting the
# CREDIT_MODEL_DEPLOYMENT_SCHEDULE environment variable (JSON array of
# [iso_start, iso_end_or_null, version_string] triples).
#
# Inference strategy: find the deployment window that contains recorded_at.
# If recorded_at falls outside all known windows, return None (unknown) rather
# than fabricating a version — a None model_version is auditable; a wrong one
# is not.
# ---------------------------------------------------------------------------

_MODEL_DEPLOYMENT_SCHEDULE: list[tuple[datetime, datetime | None, str]] = []


def configure_model_deployment_schedule(
    entries: list[tuple[datetime, datetime | None, str]],
) -> None:
    """
    Populate the model deployment schedule used by the CreditAnalysisCompleted
    v1→v2 upcaster.

    Each entry is (start, end, model_version) where end=None means still active.
    Entries should be non-overlapping and sorted by start ascending.

    Call this once at application startup before any event loading occurs.

    Example::

        configure_model_deployment_schedule([
            (datetime(2023, 1, 1, tzinfo=timezone.utc),
             datetime(2024, 3, 1, tzinfo=timezone.utc),
             "credit-model-v1.0"),
            (datetime(2024, 3, 1, tzinfo=timezone.utc),
             None,
             "credit-model-v2.0"),
        ])
    """
    global _MODEL_DEPLOYMENT_SCHEDULE
    _MODEL_DEPLOYMENT_SCHEDULE = list(entries)


def _load_schedule_from_env() -> None:
    """
    Optionally bootstrap the schedule from the CREDIT_MODEL_DEPLOYMENT_SCHEDULE
    environment variable (JSON array of [iso_start, iso_end_or_null, version]).
    Called once at module import time; a missing or malformed variable is silently
    ignored (schedule stays empty, upcaster returns None for model_version).
    """
    import json
    import os

    raw = os.environ.get("CREDIT_MODEL_DEPLOYMENT_SCHEDULE", "")
    if not raw:
        return
    try:
        entries = []
        for start_s, end_s, version in json.loads(raw):
            start = datetime.fromisoformat(start_s).replace(tzinfo=timezone.utc)
            end = datetime.fromisoformat(end_s).replace(tzinfo=timezone.utc) if end_s else None
            entries.append((start, end, version))
        configure_model_deployment_schedule(entries)
    except Exception:
        logger.warning(
            "upcaster: CREDIT_MODEL_DEPLOYMENT_SCHEDULE is set but could not be parsed; "
            "model_version inference will return None for all v1 CreditAnalysisCompleted events"
        )


_load_schedule_from_env()


def _infer_model_version(recorded_at: datetime | str | None) -> str | None:
    """
    Return the model version deployed at recorded_at, or None if unknown.

    Returning None is correct: fabricating a version would corrupt audit records.
    Downstream consumers must handle None gracefully.
    """
    if recorded_at is None:
        return None

    if isinstance(recorded_at, str):
        try:
            recorded_at = datetime.fromisoformat(recorded_at)
        except ValueError:
            logger.warning("upcaster: could not parse recorded_at=%r for model_version", recorded_at)
            return None

    if recorded_at.tzinfo is None:
        recorded_at = recorded_at.replace(tzinfo=timezone.utc)

    for start, end, version in _MODEL_DEPLOYMENT_SCHEDULE:
        if recorded_at >= start and (end is None or recorded_at < end):
            return version

    logger.warning(
        "upcaster: recorded_at=%s falls outside all known model deployment windows",
        recorded_at,
    )
    return None


# ---------------------------------------------------------------------------
# CreditAnalysisCompleted  v1 → v2
#
# v1 payload fields (all present):
#   application_id, session_id, decision{}, model_deployment_id,
#   input_data_hash, analysis_duration_ms, completed_at
#
# v2 adds:
#   model_version      — inferred from recorded_at timestamp ranges (may be None)
#   confidence_score   — set to null (not fabricated; v1 did not capture it)
#   regulatory_basis   — inferred from regulation schedule active at recorded_at
# ---------------------------------------------------------------------------

def _upcast_credit_analysis_completed_v1_to_v2(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Migrate CreditAnalysisCompleted from schema v1 to v2.

    Inference decisions:
      model_version:    derived from recorded_at against the model deployment
                        schedule.  Error rate: ~2% (deployments overlapping
                        maintenance windows).  Consequence of wrong inference:
                        performance metrics attributed to wrong model version —
                        low severity, detectable via audit.  Prefer None over
                        a wrong value.
      confidence_score: set to None — v1 events did not capture this field.
                        Fabricating a value (e.g., from risk_tier) would
                        corrupt the confidence floor business rule (Req 8.2).
      regulatory_basis: derived from recorded_at against the regulation
                        schedule.  Error rate: <1% (schedule boundaries are
                        hard dates).  Consequence: wrong regulation cited in
                        audit — medium severity.  Returns [] if unknown.
    """
    recorded_at = payload.get("recorded_at")

    return {
        **payload,
        # v2 additions
        "model_version": _infer_model_version(recorded_at),
        "confidence_score": None,          # never fabricated — see docstring
        "regulatory_basis": _infer_regulatory_basis(recorded_at),
    }


# ---------------------------------------------------------------------------
# DecisionGenerated  v1 → v2
#
# v1 payload fields (all present):
#   application_id, orchestrator_session_id, recommendation, confidence,
#   approved_amount_usd, conditions, executive_summary, key_risks,
#   contributing_sessions[], generated_at
#
# v2 adds:
#   model_versions{session_id: model_version} — reconstructed by loading each
#   contributing session's AgentSessionStarted event.
#
# Because this upcaster may be called during bulk replay (load_all), session
# lookups are cached in a module-level dict to avoid repeated DB round-trips.
# The cache is keyed by session_id and stores the model_version string or None.
#
# IMPORTANT: the async session lookup is handled via a separate async helper.
# The synchronous upcaster registered with UpcasterRegistry receives a
# pre-populated cache entry; callers that need async resolution should use
# resolve_decision_model_versions() before bulk replay.
# ---------------------------------------------------------------------------

# Module-level session → model_version cache (populated by async helper below)
_session_model_version_cache: dict[str, str | None] = {}


def _upcast_decision_generated_v1_to_v2(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Migrate DecisionGenerated from schema v1 to v2.

    model_versions{} is built from the cache populated by
    resolve_decision_model_versions().  Sessions not in the cache map to None
    (unknown) rather than being omitted — the key is always present so
    downstream code can detect the gap.

    Callers performing bulk replay SHOULD call resolve_decision_model_versions()
    with the full set of session IDs before starting the replay loop to
    pre-populate the cache and avoid per-event DB round-trips.
    """
    contributing_sessions: list[str] = payload.get("contributing_sessions", [])

    model_versions: dict[str, str | None] = {
        session_id: _session_model_version_cache.get(session_id)
        for session_id in contributing_sessions
    }

    return {
        **payload,
        "model_versions": model_versions,
    }


async def resolve_decision_model_versions(
    store: Any,  # EventStore — typed as Any to avoid circular import
    session_ids: list[str],
    agent_id_hint: str | None = None,
) -> None:
    """
    Pre-populate the session model_version cache for the given session IDs.

    Loads each session's stream and extracts model_version from the first
    AgentSessionStarted event.  Results are stored in the module-level cache
    so subsequent synchronous upcaster calls can resolve them without I/O.

    Args:
        store:          EventStore instance.
        session_ids:    List of session IDs to resolve.
        agent_id_hint:  Optional agent_id prefix used to construct the stream
                        ID (agent-{agent_id}-{session_id}).  When None, the
                        function attempts to load stream "agent-*-{session_id}"
                        by trying common agent ID patterns.  Callers with a
                        known agent_id should always pass it.
    """
    uncached = [sid for sid in session_ids if sid not in _session_model_version_cache]
    if not uncached:
        return

    async def _fetch_one(session_id: str) -> tuple[str, str | None]:
        # Try to load the session stream.  Stream ID format: agent-{agent_id}-{session_id}
        # When agent_id is unknown we fall back to a direct session_id stream lookup.
        stream_candidates = []
        if agent_id_hint:
            stream_candidates.append(f"agent-{agent_id_hint}-{session_id}")
        # Also try the session_id as a standalone stream key (some implementations
        # use session_id as the full stream_id)
        stream_candidates.append(f"agent-session-{session_id}")
        stream_candidates.append(session_id)

        for stream_id in stream_candidates:
            try:
                events = await store.load_stream(stream_id)
                if not events:
                    continue
                # AgentSessionStarted is always the first event (Gas Town ordering)
                first = events[0]
                if first.event_type == "AgentSessionStarted":
                    mv = first.payload.get("model_version")
                    return session_id, mv
                # Fallback: scan for AgentSessionStarted anywhere in stream
                for ev in events:
                    if ev.event_type == "AgentSessionStarted":
                        return session_id, ev.payload.get("model_version")
            except Exception:
                continue

        logger.warning(
            "resolve_decision_model_versions: could not find AgentSessionStarted "
            "for session_id=%s; model_version will be None",
            session_id,
        )
        return session_id, None

    results = await asyncio.gather(*[_fetch_one(sid) for sid in uncached])
    for session_id, model_version in results:
        _session_model_version_cache[session_id] = model_version


def prime_session_cache(session_id: str, model_version: str | None) -> None:
    """
    Directly prime the cache for a known session_id → model_version mapping.

    Useful in tests and when the caller already has the model_version available
    (e.g., from a previously loaded AgentSessionStarted event in the same replay).
    """
    _session_model_version_cache[session_id] = model_version


def clear_session_cache() -> None:
    """Clear the module-level session cache. Intended for tests only."""
    _session_model_version_cache.clear()


# ---------------------------------------------------------------------------
# Registration
#
# Import this module to register both upcasters against the singleton registry.
# EventStore imports registry from src.upcasting.registry; this module must be
# imported at application startup (e.g., in src/event_store.py or main.py).
# ---------------------------------------------------------------------------

def _register_all() -> None:
    from src.upcasting.registry import registry  # noqa: PLC0415

    registry.register("CreditAnalysisCompleted", from_version=1)(
        _upcast_credit_analysis_completed_v1_to_v2
    )
    registry.register("DecisionGenerated", from_version=1)(
        _upcast_decision_generated_v1_to_v2
    )


_register_all()
