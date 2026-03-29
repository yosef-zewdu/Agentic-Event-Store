"""
src/pipeline_runner.py — Async pipeline job queue (Phase 8A, Task 30.2)

Two public functions:

  enqueue_pipeline(pool, application_id, from_agent) -> job_id
      Inserts a pipeline_jobs row with status='queued' and returns the UUID.

  run_pending_jobs(store, pool)
      Claims one queued job (status='running'), runs the full agent pipeline,
      then sets status='completed' or status='failed' with error_message.
      Safe to call concurrently — uses SELECT ... FOR UPDATE SKIP LOCKED.
"""
from __future__ import annotations

import logging
import os
import traceback
from typing import Optional
from uuid import UUID

import asyncpg
from openai import AsyncOpenAI

from src.event_store import EventStore

logger = logging.getLogger(__name__)

# Ordered list of agent stages used by _agents_from
_AGENT_ORDER = ["document", "credit", "fraud", "compliance", "decision"]


async def enqueue_pipeline(
    pool: asyncpg.Pool,
    application_id: str,
    from_agent: Optional[str] = None,
) -> UUID:
    """Insert a pipeline_jobs row and return the new job_id."""
    row = await pool.fetchrow(
        """
        INSERT INTO pipeline_jobs (application_id, from_agent)
        VALUES ($1, $2)
        RETURNING job_id
        """,
        application_id,
        from_agent,
    )
    job_id: UUID = row["job_id"]
    logger.info("Enqueued pipeline job %s for application %s (from_agent=%s)",
                job_id, application_id, from_agent)
    return job_id


async def run_pending_jobs(store: EventStore, pool: asyncpg.Pool) -> int:
    """
    Claim and execute up to one pending job.

    Returns the number of jobs processed (0 or 1).
    Uses FOR UPDATE SKIP LOCKED so multiple workers can call this safely.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT job_id, application_id, from_agent
            FROM pipeline_jobs
            WHERE status = 'queued'
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """
        )
        if not row:
            return 0

        job_id = row["job_id"]
        application_id = row["application_id"]
        from_agent = row["from_agent"]

        await conn.execute(
            "UPDATE pipeline_jobs SET status='running', started_at=now() WHERE job_id=$1",
            job_id,
        )

    logger.info("Running pipeline job %s for application %s (from_agent=%s)",
                job_id, application_id, from_agent)

    try:
        await _run_pipeline(application_id, from_agent, store, pool)
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE pipeline_jobs SET status='completed', completed_at=now() WHERE job_id=$1",
                job_id,
            )
        logger.info("Pipeline job %s completed for application %s", job_id, application_id)
    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE pipeline_jobs
                SET status='failed', completed_at=now(), error_message=$2
                WHERE job_id=$1
                """,
                job_id,
                error_message[:4000],  # guard against oversized tracebacks
            )
        logger.error("Pipeline job %s failed for application %s: %s", job_id, application_id, exc)

    return 1


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _run_pipeline(
    application_id: str,
    from_agent: Optional[str],
    store: EventStore,
    pool: asyncpg.Pool,
) -> None:
    """Run agent pipeline starting from from_agent (defaults to 'document')."""
    from src.registry.client import ApplicantRegistryClient
    from src.agents import (
        DocumentProcessingAgent,
        CreditAnalysisAgent,
        FraudDetectionAgent,
        ComplianceAgent,
        DecisionOrchestratorAgent,
    )

    registry = ApplicantRegistryClient(pool)
    client = AsyncOpenAI(
        api_key=os.environ["OPENROUTER_API_KEY"],
        base_url="https://openrouter.ai/api/v1",
    )
    model = os.environ.get("OPENROUTER_MODEL", "google/gemini-2.0-flash-001")

    def make(cls, agent_id, agent_type):
        return cls(
            agent_id=agent_id,
            agent_type=agent_type,
            store=store,
            registry=registry,
            client=client,
            model=model,
        )

    # Determine which stages to run
    start = from_agent or "document"
    try:
        start_idx = _AGENT_ORDER.index(start)
    except ValueError:
        logger.warning("Unknown from_agent %r — defaulting to 'document'", start)
        start_idx = 0

    stages = _AGENT_ORDER[start_idx:]

    if "document" in stages:
        logger.info("[1/5] DocumentProcessingAgent for %s", application_id)
        await make(DocumentProcessingAgent, "doc-agent-01", "document_processing").process_application(application_id)

    if "credit" in stages:
        logger.info("[2/5] CreditAnalysisAgent for %s", application_id)
        await make(CreditAnalysisAgent, "credit-agent-01", "credit_analysis").process_application(application_id)

    if "fraud" in stages:
        logger.info("[3/5] FraudDetectionAgent for %s", application_id)
        await make(FraudDetectionAgent, "fraud-agent-01", "fraud_detection").process_application(application_id)

    if "compliance" in stages:
        logger.info("[4/5] ComplianceAgent for %s", application_id)
        await make(ComplianceAgent, "compliance-agent-01", "compliance").process_application(application_id)

    if "decision" in stages:
        logger.info("[5/5] DecisionOrchestratorAgent for %s", application_id)
        await make(DecisionOrchestratorAgent, "decision-agent-01", "decision_orchestrator").process_application(application_id)
