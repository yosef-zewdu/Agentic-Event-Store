# -*- coding: utf-8 -*-
"""
scripts/run_pipeline.py -- Process one application through all agents, or submit a new application.
Usage: 
  Process existing: python scripts/run_pipeline.py --app APEX-0007 [--phase all|document|credit|fraud|compliance|decision]
  Submit new: python scripts/run_pipeline.py --submit --app APP-001 --applicant COMP-001 --amount 500000 [--purpose working_capital]
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv()

import asyncpg
from openai import AsyncOpenAI
from src.event_store import EventStore
from src.registry.client import ApplicantRegistryClient
from src.commands.handlers import handle_submit_application
from src.agents import (
    DocumentProcessingAgent,
    CreditAnalysisAgent,
    FraudDetectionAgent,
    ComplianceAgent,
    DecisionOrchestratorAgent,
)


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--application", "--app", help="e.g. APEX-0007 (required unless --submit is used)")
    p.add_argument("--submit", action="store_true", help="Submit a new application instead of processing existing one")
    p.add_argument("--applicant", "--aplicant", help="Company ID for new application (required with --submit)")
    p.add_argument("--amount", type=int, help="Loan amount in USD for new application (required with --submit)")
    p.add_argument("--purpose", default="working_capital", help="Loan purpose (default: working_capital)")
    p.add_argument("--phase", default="all",
                   choices=["all", "document", "credit", "fraud", "compliance", "decision"])
    p.add_argument("--db-url", default=os.environ.get(
        "DATABASE_URL", "postgresql://postgres:apex@localhost/apexledger"))
    args = p.parse_args()

    # Validation
    if args.submit:
        if not args.application:
            p.error("--application is required when using --submit")
        if not args.applicant:
            p.error("--applicant is required when using --submit")
        if not args.amount:
            p.error("--amount is required when using --submit")
    elif not args.application:
        p.error("--application is required unless --submit is used")

    store = EventStore(args.db_url)
    await store.connect()
    registry_pool = await asyncpg.create_pool(args.db_url, min_size=1, max_size=5)
    registry = ApplicantRegistryClient(registry_pool)
    client = AsyncOpenAI(
        api_key=os.environ["OPENROUTER_API_KEY"],
        base_url="https://openrouter.ai/api/v1",
    )
    model = os.environ.get("OPENROUTER_MODEL", "arcee-ai/trinity-large-preview:free")

    def make(cls, agent_id, agent_type):
        return cls(agent_id=agent_id, agent_type=agent_type,
                   store=store, registry=registry, client=client, model=model)

    app = args.application
    phase = args.phase

    # Submit new application if requested
    if args.submit:
        print(f"Submitting new application {app} for applicant {args.applicant} with amount ${args.amount:,}")
        await handle_submit_application(
            store=store,
            application_id=app,
            applicant_id=args.applicant,
            requested_amount_usd=args.amount,
            loan_purpose=args.purpose,
        )
        print(f"Application {app} submitted successfully.\n")

    print(f"Processing {app} | phase={phase} | model={model}\n")

    try:
        if phase in ("document", "all"):
            print("[1/5] DocumentProcessingAgent...")
            await make(DocumentProcessingAgent, "doc-agent-01", "document_processing").process_application(app)
            print("      done.")

        if phase in ("credit", "all"):
            print("[2/5] CreditAnalysisAgent...")
            await make(CreditAnalysisAgent, "credit-agent-01", "credit_analysis").process_application(app)
            print("      done.")

        if phase in ("fraud", "all"):
            print("[3/5] FraudDetectionAgent...")
            await make(FraudDetectionAgent, "fraud-agent-01", "fraud_detection").process_application(app)
            print("      done.")

        if phase in ("compliance", "all"):
            print("[4/5] ComplianceAgent...")
            await make(ComplianceAgent, "compliance-agent-01", "compliance").process_application(app)
            print("      done.")

        if phase in ("decision", "all"):
            print("[5/5] DecisionOrchestratorAgent...")
            await make(DecisionOrchestratorAgent, "decision-agent-01", "decision_orchestrator").process_application(app)
            print("      done.")

        print(f"\nPipeline complete for {app}.")
    finally:
        await store.close()
        await registry_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
