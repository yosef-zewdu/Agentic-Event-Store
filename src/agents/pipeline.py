"""
src/agents/pipeline.py — Full loan application pipeline runner.

Chains all 5 agents in sequence:
    DocumentProcessing → CreditAnalysis → FraudDetection →
    Compliance → DecisionOrchestrator

Usage:
    PYTHONPATH=. uv run python run_pipeline.py --app app-001
    PYTHONPATH=. uv run python run_pipeline.py --app app-001 --from-agent fraud
    PYTHONPATH=. uv run python run_pipeline.py --app app-001 --submit --applicant COMP-001 --amount 500000
"""
from __future__ import annotations

import asyncio
import logging
import os
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

logger = logging.getLogger(__name__)

AGENT_ORDER = [
    "document",
    "credit",
    "fraud",
    "compliance",
    "decision",
]


async def run_pipeline(
    application_id: str,
    from_agent: str = "document",
    store=None,
    registry=None,
    client: AsyncOpenAI | None = None,
) -> dict:
    """
    Run the full (or partial) agent pipeline for a loan application.

    Args:
        application_id: e.g. "app-001"
        from_agent:     start from this agent ("document", "credit", "fraud",
                        "compliance", "decision")
        store:          EventStore instance (created if None)
        registry:       ApplicantRegistryClient instance (created if None)
        client:         AsyncOpenAI client (created if None)

    Returns:
        dict with results from each agent that ran.
    """
    from src.event_store import EventStore
    from src.upcasting.registry import registry as upcaster_registry
    from src.registry.client import ApplicantRegistryClient
    import asyncpg

    # Build shared infrastructure if not provided
    _owned_store = False
    if store is None:
        db_url = os.environ["DATABASE_URL"]
        store = EventStore(db_url=db_url, upcaster_registry=upcaster_registry)
        await store.connect()
        _owned_store = True

    if registry is None:
        reg_url = os.environ.get("APPLICANT_REGISTRY_URL") or os.environ["DATABASE_URL"]
        reg_pool = await asyncpg.create_pool(reg_url, min_size=1, max_size=3)
        registry = ApplicantRegistryClient(reg_pool)
    else:
        reg_pool = None

    if client is None:
        from src.llm_factory import get_async_client
        client, model = get_async_client()
    else:
        model = os.environ.get("OPENROUTER_MODEL", "arcee-ai/trinity-large-preview:free")

    model = os.environ.get("OPENROUTER_MODEL", model)

    # Determine which agents to run
    try:
        start_idx = AGENT_ORDER.index(from_agent)
    except ValueError:
        raise ValueError(f"Unknown agent '{from_agent}'. Choose from: {AGENT_ORDER}")

    agents_to_run = AGENT_ORDER[start_idx:]
    results = {}

    try:
        for agent_name in agents_to_run:
            agent = _build_agent(agent_name, store, registry, client, model)
            logger.info("Running %s agent for %s…", agent_name, application_id)
            print(f"\n{'='*60}")
            print(f"  Running: {agent_name.upper()} AGENT")
            print(f"{'='*60}")
            try:
                await agent.process_application(application_id)
                results[agent_name] = "completed"
                print(f"  ✓ {agent_name} agent completed")
            except Exception as e:
                results[agent_name] = f"failed: {e}"
                print(f"  ✗ {agent_name} agent failed: {e}")
                logger.exception("%s agent failed for %s", agent_name, application_id)
                # Stop pipeline on failure
                break
    finally:
        if _owned_store:
            await store.close()
        if reg_pool:
            await reg_pool.close()

    return results


def _build_agent(name: str, store, registry, client, model: str):
    """Instantiate the correct agent class."""
    from src.agents.document_processor import DocumentProcessingAgent
    from src.agents.credit_analysis_agent import CreditAnalysisAgent
    from src.agents.fraud_detection_agent import FraudDetectionAgent
    from src.agents.compliance_agent import ComplianceAgent
    from src.agents.decision_orchestrator_agent import DecisionOrchestratorAgent

    agent_map = {
        "document":    (DocumentProcessingAgent, "doc-agent-01",    "DocumentProcessing"),
        "credit":      (CreditAnalysisAgent,     "credit-agent-01", "CreditAnalysis"),
        "fraud":       (FraudDetectionAgent,     "fraud-agent-01",  "FraudDetection"),
        "compliance":  (ComplianceAgent,         "compliance-agent-01", "Compliance"),
        "decision":    (DecisionOrchestratorAgent, "decision-agent-01", "DecisionOrchestrator"),
    }

    cls, agent_id, agent_type = agent_map[name]
    return cls(
        agent_id=agent_id,
        agent_type=agent_type,
        store=store,
        registry=registry,
        client=client,
        model=model,
    )


async def submit_and_run(
    application_id: str,
    applicant_id: str,
    requested_amount_usd: float,
    loan_purpose: str = "working_capital",
    from_agent: str = "document",
) -> dict:
    """
    Submit a new application then run the full pipeline.
    Skips submission if the application already exists.
    """
    from src.event_store import EventStore
    from src.upcasting.registry import registry as upcaster_registry
    from src.commands.handlers import handle_submit_application

    db_url = os.environ["DATABASE_URL"]
    store = EventStore(db_url=db_url, upcaster_registry=upcaster_registry)
    await store.connect()

    try:
        ver = await store.stream_version(f"loan-{application_id}")
        if ver == -1:
            print(f"Submitting application {application_id}…")
            await handle_submit_application(
                store=store,
                application_id=application_id,
                applicant_id=applicant_id,
                requested_amount_usd=Decimal(str(requested_amount_usd)),
                loan_purpose=loan_purpose,
            )
            print(f"✓ Application {application_id} submitted")
        else:
            print(f"Application {application_id} already exists (version {ver}), skipping submit")
    finally:
        await store.close()

    return await run_pipeline(application_id, from_agent=from_agent)
