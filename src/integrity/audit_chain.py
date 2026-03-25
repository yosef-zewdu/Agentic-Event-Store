"""
src/integrity/audit_chain.py — Cryptographic audit chain (Req 14).

run_integrity_check(store, entity_type, entity_id) computes a sha256 hash
chain over the primary stream's events and appends an AuditIntegrityCheckRun
event to the audit stream.  Any tampering with stored events breaks the chain
and is surfaced via IntegrityCheckResult.chain_valid=False.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from src.event_store import EventStore
from src.models.events import AuditIntegrityCheckRun


class _LedgerEncoder(json.JSONEncoder):
    """Serialize Decimal and datetime to stable strings for hashing."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def _json_dumps(obj: dict) -> str:
    return json.dumps(obj, sort_keys=True, cls=_LedgerEncoder)


# ---------------------------------------------------------------------------
# Result model
# ---------------------------------------------------------------------------

@dataclass
class IntegrityCheckResult:
    """Returned by run_integrity_check."""
    chain_valid: bool
    tamper_detected: bool
    events_verified: int
    integrity_hash: str


@dataclass
class IntegrityVerifyResult:
    """Returned by verify_integrity — read-only, no writes."""
    chain_valid: bool
    tamper_detected: bool
    events_verified: int
    current_hash: str
    baseline_hash: str | None
    verified: bool          # False when no baseline exists yet
    reason: str             # human-readable explanation


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def run_integrity_check(
    store: EventStore,
    entity_type: str,
    entity_id: str,
) -> IntegrityCheckResult:
    """
    Load the primary stream and the audit stream, compute the hash chain,
    append an AuditIntegrityCheckRun event, and return an IntegrityCheckResult.

    Req 14.1 — sha256(previous_hash + concatenated_event_payload_hashes)
    Req 14.2 — append AuditIntegrityCheckRun with required fields
    Req 14.3 — return chain_valid=False / tamper_detected=True on mismatch
    Req 14.4 — first-run: previous_hash = ""
    Req 14.5 — AuditIntegrityCheckRun events are included in subsequent checks
    """
    primary_stream = f"{entity_type}-{entity_id}"
    audit_stream = f"audit-{entity_type}-{entity_id}"

    # Load primary stream (includes AuditIntegrityCheckRun events via Req 14.5
    # — those live in the audit stream, not the primary stream, so we load both)
    events = await store.load_stream(primary_stream)

    # Load audit stream to find the last integrity check
    audit_events = await store.load_stream(audit_stream)
    last_check = next(
        (e for e in reversed(audit_events) if e.event_type == "AuditIntegrityCheckRun"),
        None,
    )

    # Req 14.4 — first run uses empty string as previous_hash
    previous_hash = last_check.payload["integrity_hash"] if last_check else ""

    # Events since the last check (or all events on first run)
    since_pos = last_check.payload["events_verified_count"] if last_check else 0
    events_to_check = events[since_pos:]

    # Compute hash of new events
    event_hashes = "".join(
        hashlib.sha256(_json_dumps(e.payload).encode()).hexdigest()
        for e in events_to_check
    )
    new_hash = hashlib.sha256((previous_hash + event_hashes).encode()).hexdigest()

    # Verify the full chain when a prior check exists (Req 14.3)
    chain_valid = True
    if last_check:
        chain_valid = _verify_full_chain(events, audit_events)

    # Determine expected_version for the audit stream append
    # -1 for a brand-new stream, otherwise the current version (= number of events)
    audit_expected_version = len(audit_events) if audit_events else -1

    # Req 14.2 — append AuditIntegrityCheckRun to the audit stream
    await store.append(
        stream_id=audit_stream,
        events=[
            AuditIntegrityCheckRun(
                entity_type=entity_type,
                entity_id=entity_id,
                check_timestamp=datetime.now(tz=timezone.utc),
                events_verified_count=len(events),
                integrity_hash=new_hash,
                previous_hash=previous_hash,
                chain_valid=chain_valid,
                tamper_detected=not chain_valid,
            )
        ],
        expected_version=audit_expected_version,
        aggregate_type="audit",
    )

    return IntegrityCheckResult(
        chain_valid=chain_valid,
        tamper_detected=not chain_valid,
        events_verified=len(events_to_check),
        integrity_hash=new_hash,
    )


async def verify_integrity(
    store: EventStore,
    entity_type: str,
    entity_id: str,
) -> IntegrityVerifyResult:
    """
    Read-only integrity check — no writes to the audit stream.

    - If no baseline AuditIntegrityCheckRun exists: returns verified=False,
      reason="no baseline — run a full integrity check first"
    - If a baseline exists: replays the full chain and compares against the
      stored hash, returning chain_valid=True/False accordingly.
    """
    primary_stream = f"{entity_type}-{entity_id}"
    audit_stream = f"audit-{entity_type}-{entity_id}"

    events = await store.load_stream(primary_stream)
    audit_events = await store.load_stream(audit_stream)

    last_check = next(
        (e for e in reversed(audit_events) if e.event_type == "AuditIntegrityCheckRun"),
        None,
    )

    # Recompute current hash over all primary events from scratch
    running = ""
    for e in events:
        running = hashlib.sha256(
            (running + _compute_event_hash(e.payload)).encode()
        ).hexdigest()
    current_hash = running

    if not last_check:
        return IntegrityVerifyResult(
            chain_valid=False,
            tamper_detected=False,
            events_verified=len(events),
            current_hash=current_hash,
            baseline_hash=None,
            verified=False,
            reason="no baseline — run a full integrity check first",
        )

    baseline_hash = last_check.payload.get("integrity_hash", "")
    baseline_count = last_check.payload.get("events_verified_count", 0)

    # Verify the stored chain is internally consistent
    chain_valid = _verify_full_chain(events, audit_events)

    # Also check whether new events have been added since the last baseline
    new_events_since = len(events) - baseline_count
    if new_events_since > 0:
        reason = (
            f"chain intact up to baseline ({baseline_count} events); "
            f"{new_events_since} new event(s) since last check — re-run to extend baseline"
        )
    elif chain_valid:
        reason = f"chain verified across all {len(events)} events"
    else:
        reason = "tamper detected — stored hash does not match recomputed hash"

    return IntegrityVerifyResult(
        chain_valid=chain_valid,
        tamper_detected=not chain_valid,
        events_verified=len(events),
        current_hash=current_hash,
        baseline_hash=baseline_hash,
        verified=True,
        reason=reason,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _compute_event_hash(payload: dict) -> str:
    """sha256 of the JSON-serialised payload (keys sorted for determinism)."""
    return hashlib.sha256(_json_dumps(payload).encode()).hexdigest()


def _verify_full_chain(primary_events, audit_events) -> bool:
    """
    Replay the entire hash chain from scratch to detect tampering.

    For each AuditIntegrityCheckRun in the audit stream (in order), recompute
    the hash using the primary events it covered and the previous hash, then
    compare against the stored integrity_hash.  Any mismatch means the chain
    is broken (Req 14.3).

    Req 14.5 — AuditIntegrityCheckRun events are themselves included in
    subsequent computations via the events_verified_count cursor.
    """
    running_hash = ""
    cursor = 0  # how many primary events have been covered so far

    for audit_event in audit_events:
        if audit_event.event_type != "AuditIntegrityCheckRun":
            continue

        p = audit_event.payload
        stored_hash = p.get("integrity_hash", "")
        stored_count = p.get("events_verified_count", 0)
        stored_previous = p.get("previous_hash", "")

        # The previous_hash stored in this record must match our running hash
        if stored_previous != running_hash:
            return False

        # Recompute hash for the slice of primary events this check covered
        slice_events = primary_events[cursor:stored_count]
        event_hashes = "".join(_compute_event_hash(e.payload) for e in slice_events)
        expected_hash = hashlib.sha256((running_hash + event_hashes).encode()).hexdigest()

        if expected_hash != stored_hash:
            return False

        running_hash = stored_hash
        cursor = stored_count

    return True
