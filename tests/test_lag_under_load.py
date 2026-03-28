"""
tests/test_lag_under_load.py

Measures projection lag *during* the 50-concurrent-handler write burst,
not after the daemon has caught up.

Strategy:
  1. Start the daemon polling loop as a background asyncio task (100ms interval).
  2. Fire 50 concurrent ApplicationSubmitted appends in batches of 10.
  3. After each batch commits, immediately sample lag for both projections
     before the daemon has had a chance to process the new events.
  4. Record the peak lag observed across all mid-burst samples.
  5. Assert peak lag is within SLO.
  6. Print a per-batch lag table for the report.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio

from src.event_store import EventStore
from src.models.events import ApplicationSubmitted
from src.models.exceptions import OptimisticConcurrencyError
from src.projections.application_summary import (
    ApplicationSummaryProjection,
    _CREATE_TABLE_SQL as _APP_SUMMARY_DDL,
)
from src.projections.compliance_audit import (
    ComplianceAuditViewProjection,
    _CREATE_COMPLIANCE_AUDIT_SQL,
    _CREATE_SNAPSHOTS_SQL,
)
from src.projections.daemon import ProjectionDaemon
from tests.conftest import DATABASE_URL


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


async def _ensure_tables(pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(_APP_SUMMARY_DDL.format(table="application_summary"))
        await conn.execute(_CREATE_COMPLIANCE_AUDIT_SQL.format(table="compliance_audit_view"))
        await conn.execute(_CREATE_SNAPSHOTS_SQL)


@pytest_asyncio.fixture(autouse=True)
async def setup_tables(db_pool):
    await _ensure_tables(db_pool)
    yield
    async with db_pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE application_summary")
        await conn.execute("TRUNCATE TABLE compliance_audit_view")
        await conn.execute("TRUNCATE TABLE compliance_snapshots")


@pytest.mark.asyncio
async def test_lag_sampled_during_write_burst(store, db_pool):
    """
    Measures peak lag for ApplicationSummary and ComplianceAuditView
    *during* 50 concurrent appends, sampled immediately after each batch
    of 10 commits — before the daemon has processed the new events.

    SLOs:
      ApplicationSummary  < 500ms  (Req 9.3)
      ComplianceAuditView < 2000ms (Req 11.2)
    """
    N = 50
    BATCH = 10
    POLL_INTERVAL_MS = 100

    # --- Build daemon but do NOT start run_forever yet ---
    app_proj = ApplicationSummaryProjection()
    compliance_proj = ComplianceAuditViewProjection()
    daemon = ProjectionDaemon(store=store, pool=db_pool, max_retries=3)
    daemon.register(app_proj)
    daemon.register(compliance_proj)
    await daemon._load_checkpoints()

    # Dedicated pool for concurrent appends
    concurrent_pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=BATCH, max_size=BATCH
    )

    async def _append_with_retry(stream_id: str, event: ApplicationSubmitted) -> None:
        s = EventStore(DATABASE_URL)
        s._pool = concurrent_pool
        for attempt in range(15):
            try:
                await s.append(stream_id, [event], expected_version=-1)
                return
            except OptimisticConcurrencyError as e:
                if e.actual_version >= 0:
                    return
                await asyncio.sleep(0.01 * (attempt + 1))
            except Exception:
                if attempt == 14:
                    raise
                await asyncio.sleep(0.01 * (attempt + 1))

    # --- Start daemon polling in background ---
    daemon_task = asyncio.create_task(
        daemon.run_forever(poll_interval_ms=POLL_INTERVAL_MS)
    )

    lag_samples: list[dict] = []

    try:
        for batch_start in range(0, N, BATCH):
            batch_indices = range(batch_start, min(batch_start + BATCH, N))

            # Commit the batch
            tasks = []
            for i in batch_indices:
                event = ApplicationSubmitted(
                    application_id=f"load-app-{i:03d}",
                    applicant_id=f"applicant-{i:03d}",
                    requested_amount_usd=Decimal("100000"),
                    loan_purpose="working_capital",
                    loan_term_months=12,
                    submission_channel="api",
                    contact_email=f"load{i}@example.com",
                    contact_name=f"Load Corp {i}",
                    submitted_at=_now(),
                    application_reference=f"LOAD-{i:03d}",
                )
                tasks.append(
                    _append_with_retry(f"loan-load-app-{i:03d}", event)
                )
            await asyncio.gather(*tasks)

            # Sample lag immediately after batch commits.
            # The daemon is running concurrently at 100ms intervals, so this
            # sample may catch the daemon mid-cycle or between cycles.
            app_lag = await daemon.get_lag(app_proj.name)
            comp_lag = await daemon.get_lag(compliance_proj.name)

            sample = {
                "batch_end": batch_start + BATCH,
                "events_committed": min(batch_start + BATCH, N),
                "app_summary_lag_ms": app_lag,
                "compliance_lag_ms": comp_lag,
            }
            lag_samples.append(sample)

            print(
                f"  batch {batch_start + BATCH:3d}/{N} committed | "
                f"ApplicationSummary lag: {app_lag:4d}ms | "
                f"ComplianceAuditView lag: {comp_lag:4d}ms"
            )

    finally:
        daemon.stop()
        daemon_task.cancel()
        try:
            await daemon_task
        except asyncio.CancelledError:
            pass
        await concurrent_pool.close()

    # --- Compute peak lag across all mid-burst samples ---
    peak_app_lag = max(s["app_summary_lag_ms"] for s in lag_samples)
    peak_comp_lag = max(s["compliance_lag_ms"] for s in lag_samples)

    print(f"\n  Peak ApplicationSummary lag during burst : {peak_app_lag}ms  (SLO < 500ms)")
    print(f"  Peak ComplianceAuditView lag during burst: {peak_comp_lag}ms  (SLO < 2000ms)")

    # --- SLO assertions ---
    assert peak_app_lag < 500, (
        f"ApplicationSummary peak lag {peak_app_lag}ms exceeded 500ms SLO (Req 9.3)"
    )
    assert peak_comp_lag < 2000, (
        f"ComplianceAuditView peak lag {peak_comp_lag}ms exceeded 2000ms SLO (Req 11.2)"
    )

    # Sanity: all 50 events committed — verify via events table, not projection
    # (projection may still be catching up; events table is the source of truth)
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(DISTINCT stream_id) FROM events "
            "WHERE stream_id LIKE 'loan-load-app-%'"
        )
    assert count == N, f"Expected {N} distinct streams in events table, got {count}"
