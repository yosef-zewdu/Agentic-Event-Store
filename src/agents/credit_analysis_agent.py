from __future__ import annotations
import asyncio, hashlib, json, re, time
from datetime import datetime
from langgraph.graph import StateGraph, END
from src.agents.base_agent import BaseApexAgent, LANGGRAPH_VERSION, MAX_OCC_RETRIES, SCREENING_MODEL_VERSION, REGULATION_SET_VERSION
from src.prompts.credit_analysis import CREDIT_ANALYSIS_SYSTEM, build_credit_analysis_user


class CreditAnalysisAgent(BaseApexAgent):
    def build_graph(self):
        from typing import TypedDict
        class S(TypedDict):
            application_id: str; session_id: str; agent_id: str
            applicant_id: str | None; requested_amount_usd: float | None
            loan_purpose: str | None; historical_financials: list | None
            company_profile: dict | None; compliance_flags: list | None
            loan_history: list | None; extracted_facts: dict | None
            quality_flags: list | None; credit_decision: dict | None
            policy_violations: list | None; errors: list
            output_events_written: list; next_agent_triggered: str | None

        g = StateGraph(S)
        for name, fn in [
            ("validate_inputs",          self._node_validate_inputs),
            ("open_credit_record",       self._node_open_credit_record),
            ("load_applicant_registry",  self._node_load_registry),
            ("load_extracted_facts",     self._node_load_facts),
            ("analyze_credit_risk",      self._node_analyze),
            ("apply_policy_constraints", self._node_policy),
            ("write_output",             self._node_write),
        ]:
            g.add_node(name, fn)
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs", "open_credit_record")
        g.add_edge("open_credit_record", "load_applicant_registry")
        g.add_edge("load_applicant_registry", "load_extracted_facts")
        g.add_edge("load_extracted_facts", "analyze_credit_risk")
        g.add_edge("analyze_credit_risk", "apply_policy_constraints")
        g.add_edge("apply_policy_constraints", "write_output")
        g.add_edge("write_output", END)
        return g.compile()

    async def _node_validate_inputs(self, state):
        t = time.time()
        loan_events = await self.store.load_stream(f"loan-{state['application_id']}")
        submitted = next((e for e in loan_events if e["event_type"] == "ApplicationSubmitted"), None)
        applicant_id = submitted["payload"]["applicant_id"] if submitted else "UNKNOWN"
        requested = float(submitted["payload"].get("requested_amount_usd", 0)) if submitted else 0.0
        purpose = submitted["payload"].get("loan_purpose", "unknown") if submitted else "unknown"
        state = {**state, "applicant_id": applicant_id,
                 "requested_amount_usd": requested, "loan_purpose": purpose}
        await self._record_node_execution("validate_inputs", ["application_id"],
                                          ["applicant_id", "requested_amount_usd", "loan_purpose"],
                                          int((time.time() - t) * 1000))
        return state

    async def _node_open_credit_record(self, state):
        if await self._is_node_completed("open_credit_record"):
            return state
        t = time.time()
        # Check if CreditRecordOpened already exists
        credit_events = await self.store.load_stream(f"credit-{state['application_id']}")
        if any(e.get("event_type") == "CreditRecordOpened" for e in credit_events):
            await self._record_node_execution("open_credit_record", ["applicant_id"],
                                              ["credit_stream_opened"],
                                              int((time.time() - t) * 1000))
            return state
        await self._append_stream(f"credit-{state['application_id']}", {
            "event_type": "CreditRecordOpened", "event_version": 1, "payload": {
                "application_id": state["application_id"],
                "applicant_id": state["applicant_id"],
                "opened_at": datetime.now().isoformat()}})
        await self._record_node_execution("open_credit_record", ["applicant_id"],
                                          ["credit_stream_opened"],
                                          int((time.time() - t) * 1000))
        return state

    async def _node_load_registry(self, state):
        if await self._is_node_completed("load_applicant_registry"):
            # Load the data again to set state
            profile = await self.registry.get_company(state["applicant_id"])
            hist = await self.registry.get_financial_history(state["applicant_id"], years=[2022, 2023, 2024])
            flags = await self.registry.get_compliance_flags(state["applicant_id"])
            loans = await self.registry.get_loan_relationships(state["applicant_id"])
            hist_dicts = [vars(h) for h in hist]
            profile_dict = vars(profile) if profile else {}
            flags_dicts = [vars(f) for f in flags]
            return {**state, "company_profile": profile_dict,
                    "historical_financials": hist_dicts,
                    "compliance_flags": flags_dicts,
                    "loan_history": loans}
        t = time.time()
        profile = await self.registry.get_company(state["applicant_id"])
        hist = await self.registry.get_financial_history(state["applicant_id"], years=[2022, 2023, 2024])
        flags = await self.registry.get_compliance_flags(state["applicant_id"])
        loans = await self.registry.get_loan_relationships(state["applicant_id"])
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("query_applicant_registry",
                                     f"company_id={state['applicant_id']}",
                                     f"{len(hist)} fiscal years loaded", ms)
        hist_dicts = [vars(h) for h in hist]
        fiscal_years = [h.fiscal_year for h in hist]
        has_defaults = any(l.get("default_occurred") for l in loans)
        # Check if HistoricalProfileConsumed already exists
        credit_events = await self.store.load_stream(f"credit-{state['application_id']}")
        if not any(e.get("event_type") == "HistoricalProfileConsumed" for e in credit_events):
            await self._append_stream(f"credit-{state['application_id']}", {
                "event_type": "HistoricalProfileConsumed", "event_version": 1, "payload": {
                    "application_id": state["application_id"],
                    "session_id": self.session_id,
                    "fiscal_years_loaded": fiscal_years,
                    "has_prior_loans": len(loans) > 0,
                    "has_defaults": has_defaults,
                    "revenue_trajectory": profile.trajectory if profile else "UNKNOWN",
                    "data_hash": self._sha(hist_dicts),
                    "consumed_at": datetime.now().isoformat()}})
        await self._record_node_execution("load_applicant_registry", ["applicant_id"],
                                          ["historical_financials", "compliance_flags", "loan_history"], ms)
        profile_dict = vars(profile) if profile else {}
        flags_dicts = [vars(f) for f in flags]
        return {**state, "company_profile": profile_dict,
                "historical_financials": hist_dicts,
                "compliance_flags": flags_dicts,
                "loan_history": loans}

    async def _node_load_facts(self, state):
        if await self._is_node_completed("load_extracted_facts"):
            # Reload the data
            docpkg_events = await self.store.load_stream(f"docpkg-{state['application_id']}")
            extracted = [e for e in docpkg_events if e["event_type"] == "ExtractionCompleted"]
            facts_merged = {}
            quality_flags = []
            for e in extracted:
                p = e["payload"]
                if p.get("facts"):
                    facts_merged.update({k: v for k, v in p["facts"].items() if v is not None})
            qa_events = [e for e in docpkg_events if e["event_type"] == "QualityAssessmentCompleted"]
            for qa in qa_events:
                quality_flags.extend(qa["payload"].get("anomalies", []))
            return {**state, "extracted_facts": facts_merged, "quality_flags": quality_flags}
        t = time.time()
        docpkg_events = await self.store.load_stream(f"docpkg-{state['application_id']}")
        extracted = [e for e in docpkg_events if e["event_type"] == "ExtractionCompleted"]
        facts_merged = {}
        doc_ids = []
        quality_flags = []
        for e in extracted:
            p = e["payload"]
            doc_ids.append(p.get("document_id", ""))
            if p.get("facts"):
                facts_merged.update({k: v for k, v in p["facts"].items() if v is not None})
        qa_events = [e for e in docpkg_events if e["event_type"] == "QualityAssessmentCompleted"]
        for qa in qa_events:
            quality_flags.extend(qa["payload"].get("anomalies", []))
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("load_event_store_stream",
                                     f"docpkg-{state['application_id']}",
                                     f"{len(extracted)} ExtractionCompleted events", ms)
        # Check if ExtractedFactsConsumed already exists
        credit_events = await self.store.load_stream(f"credit-{state['application_id']}")
        if not any(e.get("event_type") == "ExtractedFactsConsumed" for e in credit_events):
            await self._append_stream(f"credit-{state['application_id']}", {
                "event_type": "ExtractedFactsConsumed", "event_version": 1, "payload": {
                    "application_id": state["application_id"],
                    "session_id": self.session_id,
                    "document_ids_consumed": doc_ids,
                    "facts_summary": f"{len(facts_merged)} fields extracted",
                    "quality_flags_present": len(quality_flags) > 0,
                    "consumed_at": datetime.now().isoformat()}})
        await self._record_node_execution("load_extracted_facts",
                                          ["document_package_events"],
                                          ["extracted_facts", "quality_flags"], ms)
        return {**state, "extracted_facts": facts_merged, "quality_flags": quality_flags}

    async def _node_analyze(self, state):
        if await self._is_node_completed("analyze_credit_risk"):
            # Load decision from session
            session_events = await self.store.load_stream(self._session_stream)
            decision_event = next((e for e in session_events if e.get("event_type") == "CreditDecisionMade"), None)
            if decision_event:
                decision = decision_event["payload"]["decision"]
                return {**state, "credit_decision": decision}
            else:
                return {**state, "credit_decision": {}}
        t = time.time()
        hist = state.get("historical_financials") or []
        fin_table = "\n".join(
            [f"FY{f.get('fiscal_year')}: revenue={f.get('total_revenue')}, "
             f"ebitda={f.get('ebitda')}, net_income={f.get('net_income')}" for f in hist]
        ) if hist else "No historical data"
        system = CREDIT_ANALYSIS_SYSTEM
        user = build_credit_analysis_user(
            company_name=state.get("company_profile", {}).get("name", "Unknown"),
            requested_amount_usd=state.get("requested_amount_usd", 0),
            loan_purpose=state.get("loan_purpose", "unknown"),
            historical_financials=hist,
            extracted_facts=state.get("extracted_facts", {}),
            quality_flags=state.get("quality_flags", []),
            compliance_flags=state.get("compliance_flags", []),
            loan_history=state.get("loan_history", []),
        )
        try:
            content, tok_in, tok_out, cost = await self._call_llm(system, user, max_tokens=800)
            decision = self._parse_json(content)
            if not decision:
                self._llm_errors.append(f"analyze_credit_risk: empty JSON from model response")
                decision = {
                    "risk_tier": "MEDIUM",
                    "recommended_limit_usd": int(state.get("requested_amount_usd", 0) * 0.8),
                    "confidence": 0.45,
                    "rationale": "JSON parse failed - human review required",
                    "key_concerns": ["LLM response unparseable"],
                    "data_quality_caveats": [], "policy_overrides_applied": []
                }
        except Exception as e:
            self._llm_errors.append(f"analyze_credit_risk: {e}")
            decision = {
                "risk_tier": "MEDIUM",
                "recommended_limit_usd": int(state.get("requested_amount_usd", 0) * 0.8),
                "confidence": 0.45,
                "rationale": f"Analysis deferred: {e}",
                "key_concerns": ["LLM analysis failed - human review required"],
                "data_quality_caveats": [], "policy_overrides_applied": []
            }
            tok_in = tok_out = 0; cost = 0.0
        # Store decision in session for resume
        await self._append_session({"event_type": "CreditDecisionMade", "event_version": 1, "payload": {
            "session_id": self.session_id, "decision": decision, "made_at": datetime.now().isoformat()}})
        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("analyze_credit_risk",
                                          ["historical_financials", "extracted_facts"],
                                          ["credit_decision"], ms, tok_in, tok_out, cost)
        return {**state, "credit_decision": decision}

    async def _node_policy(self, state):
        if await self._is_node_completed("apply_policy_constraints"):
            return state
        t = time.time()
        d = dict(state.get("credit_decision") or {})
        violations = []
        hist = state.get("historical_financials") or []
        if hist:
            rev = hist[-1].get("total_revenue", 0) or 0
            if rev > 0 and d.get("recommended_limit_usd", 0) > float(rev) * 0.35:
                d["recommended_limit_usd"] = int(float(rev) * 0.35)
                violations.append("REV_CAP")
        if any(l.get("default_occurred") for l in (state.get("loan_history") or [])):
            d["risk_tier"] = "HIGH"
            violations.append("PRIOR_DEFAULT")
        if any(f.get("severity") == "HIGH" and f.get("is_active")
               for f in (state.get("compliance_flags") or [])):
            d["confidence"] = min(d.get("confidence", 1.0), 0.50)
            violations.append("COMPLIANCE_FLAG")
        if violations:
            d["policy_overrides_applied"] = d.get("policy_overrides_applied", []) + violations
        await self._record_node_execution("apply_policy_constraints",
                                          ["credit_decision"], ["credit_decision"],
                                          int((time.time() - t) * 1000))
        return {**state, "credit_decision": d, "policy_violations": violations}

    async def _node_write(self, state):
        if await self._is_node_completed("write_output"):
            return state
        from src.commands.handlers import (
            handle_credit_analysis_completed,
            handle_request_fraud_screening,
        )

        t = time.time()
        app_id = state["application_id"]
        d = state["credit_decision"]

        # Check if already completed
        credit_events = await self.store.load_stream(f"credit-{app_id}")
        if any(e.get("event_type") == "CreditAnalysisCompleted" for e in credit_events):
            await self._record_node_execution("write_output", ["credit_decision"],
                                              ["events_written"],
                                              int((time.time() - t) * 1000))
            return state

        await handle_credit_analysis_completed(
            self.store,
            application_id=app_id,
            agent_id=self.agent_id,
            session_id=self.session_id,
            risk_tier=d.get("risk_tier"),
            recommended_limit_usd=d.get("recommended_limit_usd"),
            model_version=self.model,
            confidence_score=d.get("confidence"),
            regulatory_basis="GAAP",
            duration_ms=int((time.time() - self._t0) * 1000),
            input_data=state.get("extracted_facts", {}),
            model_deployment_id="openrouter-01",
        )

        await handle_request_fraud_screening(self.store, application_id=app_id)

        await self._append_stream(f"credit-{app_id}", {
            "event_type": "CreditAnalysisCompleted", "event_version": 2, "payload": {
                "application_id": app_id, "session_id": self.session_id,
                "decision": d, "model_version": self.model,
                "model_deployment_id": "openrouter-01",
                "input_data_hash": self._sha(state.get("extracted_facts", {})),
                "analysis_duration_ms": int((time.time() - self._t0) * 1000),
                "regulatory_basis": ["GAAP", "ECOA"],
                "completed_at": datetime.now().isoformat()}})

        events_written = [
            {"stream_id": f"credit-{app_id}", "event_type": "CreditAnalysisCompleted"},
            {"stream_id": f"loan-{app_id}", "event_type": "FraudScreeningRequested"},
        ]
        summary = (f"Credit: {d.get('risk_tier')} risk, "
                   f"${d.get('recommended_limit_usd', 0):,.0f} limit, "
                   f"{d.get('confidence', 0):.0%} confidence. Fraud screening triggered.")
        await self._record_output_written(events_written, summary)
        await self._record_node_execution("write_output", ["credit_decision"],
                                          ["events_written"],
                                          int((time.time() - t) * 1000))
        return {**state, "output_events_written": events_written,
                "next_agent_triggered": "fraud_detection"}
