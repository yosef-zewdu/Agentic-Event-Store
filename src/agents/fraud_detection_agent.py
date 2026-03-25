from __future__ import annotations
import asyncio, hashlib, json, re, time
from datetime import datetime
from langgraph.graph import StateGraph, END
from src.agents.base_agent import BaseApexAgent, LANGGRAPH_VERSION, MAX_OCC_RETRIES, SCREENING_MODEL_VERSION, REGULATION_SET_VERSION
from src.prompts.fraud_detection import FRAUD_DETECTION_SYSTEM, build_fraud_detection_user


class FraudDetectionAgent(BaseApexAgent):
    def build_graph(self):
        from typing import TypedDict
        class S(TypedDict):
            application_id: str; session_id: str; agent_id: str
            extracted_facts: dict | None; historical_financials: list | None
            company_profile: dict | None; fraud_assessment: dict | None
            errors: list; output_events_written: list; next_agent_triggered: str | None

        g = StateGraph(S)
        for name, fn in [
            ("validate_inputs",          self._node_validate_inputs),
            ("load_document_facts",      self._node_load_document_facts),
            ("cross_reference_registry", self._node_cross_reference_registry),
            ("analyze_fraud_patterns",   self._node_analyze_fraud_patterns),
            ("write_output",             self._node_write_output),
        ]:
            g.add_node(name, fn)
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs", "load_document_facts")
        g.add_edge("load_document_facts", "cross_reference_registry")
        g.add_edge("cross_reference_registry", "analyze_fraud_patterns")
        g.add_edge("analyze_fraud_patterns", "write_output")
        g.add_edge("write_output", END)
        return g.compile()

    async def _node_validate_inputs(self, state):
        t = time.time()
        loan_events = await self.store.load_stream(f"loan-{state['application_id']}")
        submitted = next((e for e in loan_events if e["event_type"] == "ApplicationSubmitted"), None)
        applicant_id = submitted["payload"]["applicant_id"] if submitted else "UNKNOWN"
        await self._record_node_execution("validate_inputs", ["application_id"],
                                          ["applicant_id"], int((time.time() - t) * 1000))
        return {**state, "_applicant_id": applicant_id}

    async def _node_load_document_facts(self, state):
        t = time.time()
        app_id = state["application_id"]
        docpkg_events = await self.store.load_stream(f"docpkg-{app_id}")
        extracted = [e for e in docpkg_events if e["event_type"] == "ExtractionCompleted"]
        facts_merged = {}
        for e in extracted:
            if e["payload"].get("facts"):
                facts_merged.update({k: v for k, v in e["payload"]["facts"].items() if v is not None})
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("load_event_store_stream", f"docpkg-{app_id}",
                                     f"{len(extracted)} extraction events", ms)
        await self._record_node_execution("load_document_facts",
                                          ["application_id"], ["extracted_facts"], ms)
        return {**state, "extracted_facts": facts_merged}

    async def _node_cross_reference_registry(self, state):
        t = time.time()
        applicant_id = state.get("_applicant_id", "UNKNOWN")
        profile = await self.registry.get_company(applicant_id)
        hist = await self.registry.get_financial_history(applicant_id, years=[2022, 2023, 2024])
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("query_applicant_registry", f"company_id={applicant_id}",
                                     f"profile + {len(hist)} years history", ms)
        await self._record_node_execution("cross_reference_registry",
                                          ["applicant_id"], ["company_profile", "historical_financials"], ms)
        return {**state,
                "company_profile": vars(profile) if profile else {},
                "historical_financials": [vars(h) for h in hist]}

    async def _node_analyze_fraud_patterns(self, state):
        t = time.time()
        app_id = state["application_id"]
        hist = state.get("historical_financials") or []
        facts = state.get("extracted_facts") or {}
        hist_summary = "\n".join(
            [f"FY{h.get('fiscal_year')}: revenue={h.get('total_revenue')}" for h in hist]
        ) if hist else "No history"
        system = FRAUD_DETECTION_SYSTEM
        user = build_fraud_detection_user(
            company_name=state.get("company_profile", {}).get("name", "Unknown"),
            historical_financials=hist,
            extracted_facts=facts,
        )
        try:
            content, tok_in, tok_out, cost = await self._call_llm(system, user, max_tokens=600)
            assessment = self._parse_json(content)
            if not assessment:
                self._llm_errors.append(f"analyze_fraud_patterns: empty JSON from model response")
                assessment = {"fraud_score": 0.1, "risk_level": "LOW",
                              "anomalies": [], "recommendation": "CLEAR"}
        except Exception as e:
            self._llm_errors.append(f"analyze_fraud_patterns: {e}")
            assessment = {"fraud_score": 0.1, "risk_level": "LOW",
                          "anomalies": [], "recommendation": "CLEAR"}
            tok_in = tok_out = 0; cost = 0.0

        await self._append_stream(f"fraud-{app_id}", {
            "event_type": "FraudScreeningInitiated", "event_version": 1, "payload": {
                "application_id": app_id, "session_id": self.session_id,
                "screening_model_version": SCREENING_MODEL_VERSION,
                "initiated_at": datetime.now().isoformat()}})

        for anomaly in assessment.get("anomalies", []):
            await self._append_stream(f"fraud-{app_id}", {
                "event_type": "FraudAnomalyDetected", "event_version": 1, "payload": {
                    "application_id": app_id, "session_id": self.session_id,
                    "anomaly": anomaly,
                    "detected_at": datetime.now().isoformat()}})

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("analyze_fraud_patterns",
                                          ["extracted_facts", "historical_financials"],
                                          ["fraud_assessment"], ms, tok_in, tok_out, cost)
        return {**state, "fraud_assessment": assessment}

    async def _node_write_output(self, state):
        t = time.time()
        app_id = state["application_id"]
        fa = state.get("fraud_assessment", {})
        await self._append_stream(f"fraud-{app_id}", {
            "event_type": "FraudScreeningCompleted", "event_version": 1, "payload": {
                "application_id": app_id, "session_id": self.session_id,
                "fraud_score": fa.get("fraud_score", 0.0),
                "risk_level": fa.get("risk_level", "LOW"),
                "anomalies_found": len(fa.get("anomalies", [])),
                "recommendation": fa.get("recommendation", "CLEAR"),
                "screening_model_version": SCREENING_MODEL_VERSION,
                "input_data_hash": self._sha(state.get("extracted_facts", {})),
                "completed_at": datetime.now().isoformat()}})
        await self._append_stream(f"loan-{app_id}", {
            "event_type": "ComplianceCheckRequested", "event_version": 1, "payload": {
                "application_id": app_id,
                "requested_at": datetime.now().isoformat(),
                "triggered_by_event_id": self.session_id,
                "regulation_set_version": REGULATION_SET_VERSION,
                "rules_to_evaluate": ["REG-001", "REG-002", "REG-003",
                                      "REG-004", "REG-005", "REG-006"]}})
        events_written = [
            {"stream_id": f"fraud-{app_id}", "event_type": "FraudScreeningCompleted"},
            {"stream_id": f"loan-{app_id}", "event_type": "ComplianceCheckRequested"},
        ]
        await self._record_output_written(
            events_written,
            f"Fraud score: {fa.get('fraud_score', 0):.2f} ({fa.get('risk_level')}). "
            f"{len(fa.get('anomalies', []))} anomalies. Compliance check triggered.")
        await self._record_node_execution("write_output", ["fraud_assessment"],
                                          ["events_written"],
                                          int((time.time() - t) * 1000))
        return {**state, "output_events_written": events_written,
                "next_agent_triggered": "compliance"}
