"""
tests/test_document_extraction.py

Standalone test for DocumentProcessingAgent extraction — no database required.
Uses a mock store so nothing is written to PostgreSQL.

Run:
    PYTHONPATH=. uv run python tests/test_document_extraction.py [COMP-001]
    PYTHONPATH=. uv run python tests/test_document_extraction.py [COMP-001] --all-docs
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv()

# ---------------------------------------------------------------------------
# Mock store — captures all appended events in memory, never touches DB
# ---------------------------------------------------------------------------

class MockStore:
    def __init__(self):
        self.streams: dict[str, list] = {}

    async def load_stream(self, stream_id: str):
        return self.streams.get(stream_id, [])

    async def stream_version(self, stream_id: str) -> int:
        events = self.streams.get(stream_id, [])
        return len(events) - 1  # -1 if empty

    async def append(self, stream_id: str, events: list, **kwargs):
        if stream_id not in self.streams:
            self.streams[stream_id] = []
        self.streams[stream_id].extend(events)
        return len(self.streams[stream_id]) - 1

    def get_events(self, stream_id: str) -> list:
        return self.streams.get(stream_id, [])

    def all_event_types(self, stream_id: str) -> list[str]:
        return [e.get("event_type", "?") for e in self.get_events(stream_id)]


# ---------------------------------------------------------------------------
# Fake ApplicationSubmitted event so the agent can find applicant_id
# ---------------------------------------------------------------------------

def _make_submitted_event(applicant_id: str, app_id: str):
    class FakeEvent:
        event_type = "ApplicationSubmitted"
        payload = {"applicant_id": applicant_id, "application_id": app_id,
                   "requested_amount_usd": 500000.0}
    return FakeEvent()


# ---------------------------------------------------------------------------
# Pretty-print helpers
# ---------------------------------------------------------------------------

def _sep(title=""):
    w = 70
    if title:
        pad = (w - len(title) - 2) // 2
        print("─" * pad + f" {title} " + "─" * (w - pad - len(title) - 2))
    else:
        print("─" * w)

def _fmt_val(v):
    if v is None:
        return "\033[90mnull\033[0m"
    if isinstance(v, float):
        if abs(v) > 1000:
            return f"\033[96m${v:,.0f}\033[0m"
        return f"\033[96m{v:.4f}\033[0m"
    if isinstance(v, int):
        return f"\033[96m{v:,}\033[0m"
    return f"\033[93m{v}\033[0m"

def _print_facts(doc_type: str, facts: dict):
    _sep(doc_type.replace("_", " ").title())
    if "extraction_error" in facts:
        print(f"  \033[91m✗ Extraction error: {facts['extraction_error']}\033[0m")
        return
    for k, v in facts.items():
        status = "✓" if v is not None else "·"
        color = "\033[32m" if v is not None else "\033[90m"
        print(f"  {color}{status}\033[0m  {k:<35} {_fmt_val(v)}")

def _print_quality(qa: dict):
    _sep("Quality Assessment")
    conf = qa.get("overall_confidence", 0)
    coherent = qa.get("is_coherent", False)
    conf_color = "\033[32m" if conf >= 0.7 else "\033[33m" if conf >= 0.5 else "\033[31m"
    print(f"  Confidence:    {conf_color}{conf:.2f}\033[0m")
    print(f"  Coherent:      {'✓' if coherent else '✗'}")
    anomalies = qa.get("anomalies") or []
    missing = qa.get("critical_missing_fields") or []
    if anomalies:
        print(f"  Anomalies:")
        for a in anomalies:
            print(f"    \033[33m⚠ {a}\033[0m")
    if missing:
        print(f"  Missing fields:")
        for m in missing:
            print(f"    \033[31m✗ {m}\033[0m")
    notes = qa.get("auditor_notes", "")
    if notes:
        print(f"  Notes:         {notes}")
    if qa.get("reextraction_recommended"):
        print(f"  \033[33m⚠ Re-extraction recommended\033[0m")


# ---------------------------------------------------------------------------
# Known ground-truth values for validation (from actual PDFs)
# ---------------------------------------------------------------------------

GROUND_TRUTH: dict[str, dict[str, dict]] = {
    "COMP-001": {
        "income_statement": {
            "fiscal_year": 2024,
            "total_revenue": 6376032,
            "cost_of_goods_sold": 4880954,
            "gross_profit": 1495078,
            "operating_expenses": 1207266,
            "depreciation_amortization": 226523,
            "operating_income": 287812,
            "interest_expense": 131037,
            "income_before_tax": 156775,
            "tax_expense": 36633,
            "net_income": 120142,
            "ebitda": 514335,
        },
        "balance_sheet": {
            "fiscal_year": 2024,
            "total_assets": 14965437,
            "current_assets": 5350573,
            "cash_and_equivalents": 1038244,
            "accounts_receivable": 1413614,
            "inventory": 2364051,
            "other_current_assets": 534664,
            "property_plant_equipment_net": 9614864,
            "total_liabilities": 10463720,
            "current_liabilities": 3255770,
            "accounts_payable": 1465096,
            "accrued_liabilities": 976731,
            "current_portion_long_term_debt": 813942,
            "long_term_debt": 2221420,
            "other_long_term_liabilities": 4986530,
            "total_equity": 4501717,
        },
    },
}

def _validate_against_truth(doc_type: str, facts: dict, applicant_id: str):
    truth = GROUND_TRUTH.get(applicant_id, {}).get(doc_type)
    if not truth:
        return
    _sep(f"Validation vs Ground Truth ({doc_type})")
    passed = failed = missing = 0
    for field, expected in truth.items():
        actual = facts.get(field)
        if actual is None:
            print(f"  \033[90m·  {field:<40} expected={_fmt_val(expected)}  got=null\033[0m")
            missing += 1
        else:
            # Allow 1% tolerance for float rounding
            tol = abs(expected) * 0.01 if isinstance(expected, (int, float)) else 0
            match = abs(float(actual) - float(expected)) <= max(tol, 1) if isinstance(expected, (int, float)) else actual == expected
            if match:
                print(f"  \033[32m✓  {field:<40} {_fmt_val(actual)}\033[0m")
                passed += 1
            else:
                print(f"  \033[31m✗  {field:<40} expected={_fmt_val(expected)}  got={_fmt_val(actual)}\033[0m")
                failed += 1
    print(f"\n  Result: \033[32m{passed} correct\033[0m  \033[31m{failed} wrong\033[0m  \033[90m{missing} missing\033[0m")



async def run_extraction_test(applicant_id: str, show_raw: bool = False):
    from src.agents.document_processor import DocumentProcessingAgent

    app_id = f"test-{applicant_id.lower()}"

    # Build mock store with a pre-seeded ApplicationSubmitted event
    store = MockStore()
    store.streams[f"loan-{app_id}"] = [_make_submitted_event(applicant_id, app_id)]

    # Build OpenAI client
    api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("OPENROUTER_API_KEY")
    base_url = os.environ.get("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
    client = AsyncOpenAI(api_key=api_key, base_url=base_url)

    # Mock registry (not used by document processor but required by BaseApexAgent)
    registry = MagicMock()

    agent = DocumentProcessingAgent(
        agent_id="doc-test-agent",
        agent_type="DocumentProcessing",
        store=store,
        registry=registry,
        client=client,
    )

    print(f"\n\033[1mDocument Extraction Test\033[0m")
    print(f"  Applicant:  {applicant_id}")
    print(f"  App ID:     {app_id}")
    print(f"  Model:      {agent.model}")
    _sep()

    # Run the agent (writes only to mock store)
    try:
        await agent.process_application(app_id)
    except Exception as e:
        print(f"\033[91mAgent failed: {e}\033[0m")
        import traceback; traceback.print_exc()
        return

    # Pull results from mock store
    docpkg_events = store.get_events(f"docpkg-{app_id}")
    event_types = [e.get("event_type") for e in docpkg_events]

    print(f"\n  Events written to docpkg stream: {len(docpkg_events)}")
    for et in event_types:
        print(f"    · {et}")

    # Show extracted facts per document
    extraction_events = [e for e in docpkg_events if e.get("event_type") == "ExtractionCompleted"]
    for ev in extraction_events:
        p = ev.get("payload", {})
        doc_type = p.get("document_type", "unknown")
        facts = p.get("facts", {})
        _print_facts(doc_type, facts)
        _validate_against_truth(doc_type, facts, applicant_id)
        if show_raw:
            print(f"\n  Raw text length: {p.get('raw_text_length', 0)} chars")

    # Show quality assessment
    qa_events = [e for e in docpkg_events if e.get("event_type") == "QualityAssessmentCompleted"]
    if qa_events:
        _print_quality(qa_events[0].get("payload", {}))

    # Show LLM usage
    _sep("LLM Usage")
    print(f"  Calls:     {agent._llm_calls}")
    print(f"  Tokens:    {agent._tokens:,}")
    if agent._llm_errors:
        print(f"  Errors:")
        for err in agent._llm_errors:
            print(f"    \033[31m✗ {err}\033[0m")
    else:
        print(f"  \033[32m✓ No LLM errors\033[0m")

    # Show loan stream events
    loan_events = store.get_events(f"loan-{app_id}")
    triggered = [
        e.get("event_type") if isinstance(e, dict) else e.event_type
        for e in loan_events
        if (e.get("event_type") if isinstance(e, dict) else e.event_type) != "ApplicationSubmitted"
    ]
    if triggered:
        _sep("Triggered Next Steps")
        for et in triggered:
            print(f"  → {et}")

    _sep()
    print()


if __name__ == "__main__":
    applicant_id = sys.argv[1] if len(sys.argv) > 1 else "COMP-001"
    show_raw = "--raw" in sys.argv
    asyncio.run(run_extraction_test(applicant_id, show_raw=show_raw))
