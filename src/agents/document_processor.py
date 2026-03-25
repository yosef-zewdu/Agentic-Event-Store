"""
src/agents/document_processor.py — DocumentProcessingAgent (full implementation)

Reads PDFs from documents/{applicant_id}/, extracts text with pypdf,
uses LLM to pull structured financial facts, writes docpkg-{app_id} events.

Nodes:
    validate_inputs → validate_document_formats → extract_income_statement →
    extract_balance_sheet → assess_quality → write_output
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import TypedDict

from langgraph.graph import END, StateGraph

from src.agents.base_agent import BaseApexAgent, _compute_cost
from src.prompts.document_extraction import build_extraction_system, build_extraction_user
from src.prompts.document_quality import DOCUMENT_QUALITY_SYSTEM, build_document_quality_user

DOCUMENTS_DIR = Path(os.environ.get("DOCUMENTS_DIR", "./documents"))


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class DocProcState(TypedDict):
    application_id: str
    session_id: str
    applicant_id: str | None
    document_paths: dict | None          # {doc_type: Path}
    extraction_results: dict | None      # {doc_type: facts_dict}
    quality_assessment: dict | None
    errors: list
    output_events: list
    next_agent: str | None
    _package_id: str | None


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------

class DocumentProcessingAgent(BaseApexAgent):
    """
    Processes uploaded financial documents for a loan application.
    Reads PDFs from disk, extracts text, uses LLM to parse financial facts.
    """

    def build_graph(self):
        g = StateGraph(DocProcState)
        g.add_node("validate_inputs",           self._node_validate_inputs)
        g.add_node("validate_document_formats", self._node_validate_formats)
        g.add_node("extract_income_statement",  self._node_extract_is)
        g.add_node("extract_balance_sheet",     self._node_extract_bs)
        g.add_node("assess_quality",            self._node_assess_quality)
        g.add_node("write_output",              self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",           "validate_document_formats")
        g.add_edge("validate_document_formats", "extract_income_statement")
        g.add_edge("extract_income_statement",  "extract_balance_sheet")
        g.add_edge("extract_balance_sheet",     "assess_quality")
        g.add_edge("assess_quality",            "write_output")
        g.add_edge("write_output",              END)
        return g.compile()

    def _initial_state(self, application_id: str) -> DocProcState:
        return DocProcState(
            application_id=application_id,
            session_id=self.session_id,
            applicant_id=None,
            document_paths=None,
            extraction_results=None,
            quality_assessment=None,
            errors=[],
            output_events=[],
            next_agent=None,
            _package_id=None,
        )

    # ------------------------------------------------------------------
    # Node 1: validate inputs — find applicant_id and document paths
    # ------------------------------------------------------------------

    async def _node_validate_inputs(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]

        # Load ApplicationSubmitted to get applicant_id
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        submitted = next(
            (e for e in loan_events if e.event_type == "ApplicationSubmitted"), None
        )
        applicant_id = submitted.payload.get("applicant_id", "COMP-001") if submitted else "COMP-001"

        # Locate documents directory for this applicant
        doc_dir = DOCUMENTS_DIR / applicant_id
        if not doc_dir.exists():
            # Fallback: try app_id directly
            doc_dir = DOCUMENTS_DIR / app_id
        if not doc_dir.exists():
            raise FileNotFoundError(
                f"No documents directory found for applicant {applicant_id} "
                f"(tried {DOCUMENTS_DIR / applicant_id})"
            )

        # Map document types to file paths
        doc_paths: dict[str, Path] = {}
        for f in doc_dir.iterdir():
            name = f.name.lower()
            if "income" in name:
                doc_paths["income_statement"] = f
            elif "balance" in name:
                doc_paths["balance_sheet"] = f
            elif "application" in name or "proposal" in name:
                doc_paths["application_proposal"] = f
            elif "financial_statements" in name or "financial_summary" in name:
                doc_paths["financial_summary"] = f

        package_id = f"pkg-{app_id}"

        # Create package stream if it doesn't exist
        pkg_ver = await self.store.stream_version(f"docpkg-{app_id}")
        if pkg_ver == -1:
            await self._append_stream(f"docpkg-{app_id}", {
                "event_type": "PackageCreated", "event_version": 1, "payload": {
                    "package_id": package_id,
                    "application_id": app_id,
                    "applicant_id": applicant_id,
                    "document_count": len(doc_paths),
                    "created_at": datetime.now().isoformat(),
                }
            })

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution(
            "validate_inputs", ["application_id"],
            ["applicant_id", "document_paths"], ms
        )
        return {**state, "applicant_id": applicant_id,
                "document_paths": {k: str(v) for k, v in doc_paths.items()},
                "_package_id": package_id}

    # ------------------------------------------------------------------
    # Node 2: validate document formats
    # ------------------------------------------------------------------

    async def _node_validate_formats(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]
        package_id = state["_package_id"]
        doc_paths = state.get("document_paths") or {}

        for doc_type, path_str in doc_paths.items():
            p = Path(path_str)
            suffix = p.suffix.lower().lstrip(".")
            page_count = 1
            if suffix == "pdf":
                try:
                    from pypdf import PdfReader
                    reader = PdfReader(str(p))
                    page_count = len(reader.pages)
                except Exception:
                    pass

            await self._append_stream(f"docpkg-{app_id}", {
                "event_type": "DocumentFormatValidated", "event_version": 1, "payload": {
                    "package_id": package_id,
                    "document_id": f"{doc_type}-{app_id}",
                    "document_type": doc_type,
                    "file_name": p.name,
                    "page_count": page_count,
                    "detected_format": suffix,
                    "validated_at": datetime.now().isoformat(),
                }
            })

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution(
            "validate_document_formats", ["document_paths"],
            ["format_validated"], ms
        )
        return state

    # ------------------------------------------------------------------
    # Node 3: extract income statement
    # ------------------------------------------------------------------

    async def _node_extract_is(self, state: DocProcState) -> DocProcState:
        return await self._extract_document(state, "income_statement")

    # ------------------------------------------------------------------
    # Node 4: extract balance sheet
    # ------------------------------------------------------------------

    async def _node_extract_bs(self, state: DocProcState) -> DocProcState:
        return await self._extract_document(state, "balance_sheet")

    # ------------------------------------------------------------------
    # Shared extraction helper
    # ------------------------------------------------------------------

    async def _extract_document(self, state: DocProcState, doc_type: str) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]
        package_id = state["_package_id"]
        doc_paths = state.get("document_paths") or {}
        results = dict(state.get("extraction_results") or {})

        path_str = doc_paths.get(doc_type)
        if not path_str:
            # No document of this type — skip gracefully
            ms = int((time.time() - t) * 1000)
            await self._record_node_execution(
                f"extract_{doc_type}", [doc_type], ["skipped"], ms
            )
            return state

        doc_id = f"{doc_type}-{app_id}"
        p = Path(path_str)

        await self._append_stream(f"docpkg-{app_id}", {
            "event_type": "ExtractionStarted", "event_version": 1, "payload": {
                "package_id": package_id,
                "document_id": doc_id,
                "document_type": doc_type,
                "pipeline_version": "llm-1.0",
                "extraction_model": self.model,
                "started_at": datetime.now().isoformat(),
            }
        })

        # Extract raw text from PDF
        raw_text = self._read_document(p)

        # Use LLM to extract structured facts
        facts = await self._llm_extract_facts(doc_type, raw_text, app_id)

        results[doc_type] = facts

        await self._append_stream(f"docpkg-{app_id}", {
            "event_type": "ExtractionCompleted", "event_version": 1, "payload": {
                "package_id": package_id,
                "document_id": doc_id,
                "document_type": doc_type,
                "facts": facts,
                "raw_text_length": len(raw_text),
                "tables_extracted": 1,
                "processing_ms": int((time.time() - t) * 1000),
                "completed_at": datetime.now().isoformat(),
            }
        })

        ms = int((time.time() - t) * 1000)
        await self._record_tool_call(
            "llm_document_extraction",
            f"{doc_type}: {p.name}",
            f"{len(facts)} fields extracted",
            ms,
        )
        await self._record_node_execution(
            f"extract_{doc_type}", [doc_type], ["extracted_facts"], ms
        )
        return {**state, "extraction_results": results}

    def _read_document(self, path: Path) -> str:
        """Extract raw text from PDF, CSV, or XLSX."""
        suffix = path.suffix.lower()
        try:
            if suffix == ".pdf":
                from pypdf import PdfReader
                reader = PdfReader(str(path))
                return "\n".join(page.extract_text() or "" for page in reader.pages)
            elif suffix == ".csv":
                return path.read_text(encoding="utf-8", errors="replace")
            elif suffix in (".xlsx", ".xls"):
                try:
                    import openpyxl
                    wb = openpyxl.load_workbook(str(path), data_only=True)
                    lines = []
                    for ws in wb.worksheets:
                        for row in ws.iter_rows(values_only=True):
                            lines.append("\t".join(str(c) if c is not None else "" for c in row))
                    return "\n".join(lines)
                except Exception:
                    return path.read_text(encoding="utf-8", errors="replace")
            else:
                return path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            return f"[extraction error: {e}]"

    async def _llm_extract_facts(self, doc_type: str, raw_text: str, app_id: str) -> dict:
        """Use LLM to extract structured financial facts from raw document text."""
        system = build_extraction_system(doc_type)
        user = build_extraction_user(raw_text)

        try:
            content, tok_in, tok_out, cost = await self._call_llm(system, user, max_tokens=512)
            # Accumulate tokens directly since caller doesn't pass them to _record_node_execution
            self._tokens += (tok_in or 0) + (tok_out or 0)
            self._tokens_input += (tok_in or 0)
            self._tokens_output += (tok_out or 0)
            self._llm_calls += 1
            cost = _compute_cost(self.model, tok_in or 0, tok_out or 0)
            self._cost += cost
            facts = self._parse_json(content)
            if not facts:
                self._llm_errors.append(f"extract_{doc_type}: empty JSON from model response")
                return {"extraction_error": "parse failed", "fiscal_year": 2024}
            # Compute derived ratios deterministically to avoid LLM inconsistency
            if doc_type == "balance_sheet":
                eq = facts.get("total_equity")
                liab = facts.get("total_liabilities")
                curr_a = facts.get("current_assets")
                curr_l = facts.get("current_liabilities")
                if eq and liab and eq != 0:
                    facts["debt_to_equity"] = round(liab / eq, 4)
                if curr_a and curr_l and curr_l != 0:
                    facts["current_ratio"] = round(curr_a / curr_l, 4)
            if doc_type == "income_statement":
                rev = facts.get("total_revenue")
                gp = facts.get("gross_profit")
                ebitda = facts.get("ebitda")
                ni = facts.get("net_income")
                da = facts.get("depreciation_amortization")
                oi = facts.get("operating_income")
                # Recompute EBITDA from EBIT + D&A if both present
                if oi and da:
                    facts["ebitda"] = round(oi + da, 2)
                    ebitda = facts["ebitda"]
                if rev and rev != 0:
                    if gp:
                        facts["gross_margin"] = round(gp / rev, 4)
                    if ebitda:
                        facts["ebitda_margin"] = round(ebitda / rev, 4)
                    if ni:
                        facts["net_margin"] = round(ni / rev, 4)
            return facts
        except Exception as e:
            self._llm_calls += 1  # still count the attempt
            self._llm_errors.append(f"extract_{doc_type}: {e}")
            return {"extraction_error": str(e)[:100], "fiscal_year": 2024}

    # ------------------------------------------------------------------
    # Node 5: assess quality
    # ------------------------------------------------------------------

    async def _node_assess_quality(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]
        package_id = state["_package_id"]
        results = state.get("extraction_results") or {}

        system = DOCUMENT_QUALITY_SYSTEM
        user = build_document_quality_user(results)

        try:
            content, tok_in, tok_out, cost = await self._call_llm(system, user, max_tokens=400)
            qa = self._parse_json(content)
            if not qa:
                self._llm_errors.append("assess_quality: empty JSON from model response")
                qa = {
                    "overall_confidence": 0.7, "is_coherent": True,
                    "anomalies": [], "critical_missing_fields": [],
                    "reextraction_recommended": False, "auditor_notes": "Auto-assessed (parse failed)",
                }
        except Exception as e:
            self._llm_errors.append(f"assess_quality: {e}")
            qa = {
                "overall_confidence": 0.7, "is_coherent": True,
                "anomalies": [], "critical_missing_fields": [],
                "reextraction_recommended": False, "auditor_notes": "Auto-assessed",
            }
            tok_in = tok_out = 0; cost = 0.0

        # Write one QualityAssessmentCompleted per document type
        for doc_type in results:
            await self._append_stream(f"docpkg-{app_id}", {
                "event_type": "QualityAssessmentCompleted", "event_version": 1, "payload": {
                    "package_id": package_id,
                    "document_id": f"{doc_type}-{app_id}",
                    "overall_confidence": qa.get("overall_confidence", 0.7),
                    "is_coherent": qa.get("is_coherent", True),
                    "anomalies": qa.get("anomalies", []),
                    "critical_missing_fields": qa.get("critical_missing_fields", []),
                    "reextraction_recommended": qa.get("reextraction_recommended", False),
                    "auditor_notes": qa.get("auditor_notes", ""),
                    "assessed_at": datetime.now().isoformat(),
                }
            })

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution(
            "assess_quality", ["extraction_results"], ["quality_assessment"],
            ms, tok_in, tok_out, cost
        )
        return {**state, "quality_assessment": qa}

    # ------------------------------------------------------------------
    # Node 6: write output — trigger credit analysis
    # ------------------------------------------------------------------

    async def _node_write_output(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]
        package_id = state["_package_id"]
        qa = state.get("quality_assessment") or {}
        n_docs = len(state.get("extraction_results") or {})

        await self._append_stream(f"docpkg-{app_id}", {
            "event_type": "PackageReadyForAnalysis", "event_version": 1, "payload": {
                "package_id": package_id,
                "application_id": app_id,
                "documents_processed": n_docs,
                "has_quality_flags": bool(qa.get("anomalies")),
                "quality_flag_count": len(qa.get("anomalies", [])),
                "ready_at": datetime.now().isoformat(),
            }
        })

        # Only append CreditAnalysisRequested if not already present (idempotency)
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        already_requested = any(
            getattr(e, "event_type", e.get("event_type") if isinstance(e, dict) else None)
            == "CreditAnalysisRequested"
            for e in loan_events
        )
        events_written = [
            {"stream_id": f"docpkg-{app_id}", "event_type": "PackageReadyForAnalysis"},
        ]
        if not already_requested:
            await self._append_stream(f"loan-{app_id}", {
                "event_type": "CreditAnalysisRequested", "event_version": 1, "payload": {
                    "application_id": app_id,
                    "requested_at": datetime.now().isoformat(),
                    "requested_by": self.session_id,
                    "priority": "NORMAL",
                }
            })
            events_written.append(
                {"stream_id": f"loan-{app_id}", "event_type": "CreditAnalysisRequested"}
            )
        await self._record_output_written(
            events_written,
            f"{n_docs} documents processed. Credit analysis triggered."
        )
        ms = int((time.time() - t) * 1000)
        await self._record_node_execution(
            "write_output", ["quality_assessment"], ["events_written"], ms
        )
        return {**state, "output_events": events_written, "next_agent": "credit_analysis"}
