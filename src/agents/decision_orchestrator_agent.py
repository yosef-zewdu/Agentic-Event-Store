from __future__ import annotations
import asyncio, hashlib, json, re, time
from datetime import datetime
from langgraph.graph import StateGraph, END
from src.agents.base_agent import BaseApexAgent, LANGGRAPH_VERSION, MAX_OCC_RETRIES, SCREENING_MODEL_VERSION, REGULATION_SET_VERSION
from src.prompts.decision_orchestrator import DECISION_ORCHESTRATOR_SYSTEM, build_decision_orchestrator_user


class DecisionOrchestratorAgent(BaseApexAgent):
    def build_graph(self):
        from typing import TypedDict
        class S(TypedDict):
            application_id: str; session_id: str; agent_id: str
            credit_analysis: dict | None; fraud_screening: dict | None
            compliance_record: dict | None; orchestrator_decision: dict | None
            errors: list; output_events_written: list; next_agent_triggered: str | None

        g = StateGraph(S)
        for name, fn in [
            ("validate_inputs",        self._node_validate_inputs),
            ("load_all_analyses",      self._node_load_all_analyses),
            ("synthesize_decision",    self._node_synthesize_decision),
            ("apply_hard_constraints", self._node_apply_hard_constraints),
            ("write_output",           self._node_write_output),
        ]:
            g.add_node(name, fn)
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs", "load_all_analyses")
        g.add_edge("load_all_analyses", "synthesize_decision")
        g.add_edge("synthesize_decision", "apply_hard_constraints")
        g.add_edge("apply_hard_constraints", "write_output")
        g.add_edge("write_output", END)
        return g.compile()

    async def _node_validate_inputs(self, state):
        t = time.time()
        loan_events = await self.store.load_stream(f"loan-{state['application_id']}")
        has_decision_req = any(e["event_type"] == "DecisionRequested" for e in loan_events)
        if not has_decision_req:
            raise ValueError("DecisionRequested event not found on loan stream")
        await self._record_node_execution("validate_inputs", ["application_id"],
                                          ["decision_requested_confirmed"],
                                          int((time.time() - t) * 1000))
        return state

    async def _node_load_all_analyses(self, state):
        t = time.time()
        app_id = state["application_id"]
        credit_events = await self.store.load_stream(f"credit-{app_id}")
        fraud_events = await self.store.load_stream(f"fraud-{app_id}")
        compliance_events = await self.store.load_stream(f"compliance-{app_id}")

        credit = next((e["payload"] for e in reversed(credit_events)
                       if e["event_type"] == "CreditAnalysisCompleted"), {})
        fraud = next((e["payload"] for e in reversed(fraud_events)
                      if e["event_type"] == "FraudScreeningCompleted"), {})
        compliance = next((e["payload"] for e in reversed(compliance_events)
                           if e["event_type"] == "ComplianceCheckCompleted"), {})

        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("load_analysis_streams", app_id,
                                     "credit + fraud + compliance loaded", ms)
        await self._record_node_execution("load_all_analyses", ["application_id"],
                                          ["credit_analysis", "fraud_screening", "compliance_record"], ms)
        return {**state, "credit_analysis": credit,
                "fraud_screening": fraud, "compliance_record": compliance}

    async def _node_synthesize_decision(self, state):
        t = time.time()
        credit = state.get("credit_analysis") or {}
        fraud = state.get("fraud_screening") or {}
        compliance = state.get("compliance_record") or {}
        decision_obj = credit.get("decision") or {}

        system = DECISION_ORCHESTRATOR_SYSTEM
        user = build_decision_orchestrator_user(
            risk_tier=decision_obj.get("risk_tier"),
            credit_confidence=decision_obj.get("confidence"),
            recommended_limit_usd=decision_obj.get("recommended_limit_usd", 0),
            fraud_score=fraud.get("fraud_score", 0),
            fraud_risk_level=fraud.get("risk_level"),
            anomalies_found=fraud.get("anomalies_found", 0),
            compliance_verdict=compliance.get("overall_verdict"),
            has_hard_block=compliance.get("has_hard_block", False),
        )
        try:
            content, tok_in, tok_out, cost = await self._call_llm(system, user, max_tokens=600)
            decision = self._parse_json(content)
            if not decision:
                self._llm_errors.append(f"synthesize_decision: empty JSON from model response")
                decision = {
                    "recommendation": "REFER",
                    "confidence": 0.5,
                    "approved_amount_usd": None,
                    "executive_summary": "Automated synthesis failed - referred for human review.",
                    "key_risks": ["LLM response unparseable"],
                    "conditions": []
                }
        except Exception as e:
            self._llm_errors.append(f"synthesize_decision: {e}")
            decision = {
                "recommendation": "REFER",
                "confidence": 0.5,
                "approved_amount_usd": None,
                "executive_summary": "Automated synthesis failed - referred for human review.",
                "key_risks": ["LLM synthesis error"],
                "conditions": []
            }
            tok_in = tok_out = 0; cost = 0.0

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("synthesize_decision",
                                          ["credit_analysis", "fraud_screening", "compliance_record"],
                                          ["orchestrator_decision"], ms, tok_in, tok_out, cost)
        return {**state, "orchestrator_decision": decision}

    async def _node_apply_hard_constraints(self, state):
        t = time.time()
        d = dict(state.get("orchestrator_decision") or {})
        credit = state.get("credit_analysis") or {}
        fraud = state.get("fraud_screening") or {}
        compliance = state.get("compliance_record") or {}
        decision_obj = credit.get("decision") or {}

        if compliance.get("has_hard_block") or compliance.get("overall_verdict") == "BLOCKED":
            d["recommendation"] = "DECLINE"
            d["key_risks"] = d.get("key_risks", []) + ["Compliance hard block"]
        elif float(decision_obj.get("confidence", 1.0)) < 0.60:
            d["recommendation"] = "REFER"
            d["key_risks"] = d.get("key_risks", []) + ["Low confidence score"]
        elif float(fraud.get("fraud_score", 0)) > 0.60:
            d["recommendation"] = "REFER"
            d["key_risks"] = d.get("key_risks", []) + ["High fraud score"]

        await self._record_node_execution("apply_hard_constraints",
                                          ["orchestrator_decision"],
                                          ["orchestrator_decision"],
                                          int((time.time() - t) * 1000))
        return {**state, "orchestrator_decision": d}

    async def _node_write_output(self, state):
        t = time.time()
        app_id = state["application_id"]
        d = state.get("orchestrator_decision") or {}
        credit = state.get("credit_analysis") or {}
        decision_obj = credit.get("decision") or {}
        recommendation = d.get("recommendation", "REFER")

        await self._append_stream(f"loan-{app_id}", {
            "event_type": "DecisionGenerated", "event_version": 2, "payload": {
                "application_id": app_id,
                "orchestrator_session_id": self.session_id,
                "recommendation": recommendation,
                "confidence": d.get("confidence", 0.5),
                "approved_amount_usd": d.get("approved_amount_usd"),
                "conditions": d.get("conditions", []),
                "executive_summary": d.get("executive_summary", ""),
                "key_risks": d.get("key_risks", []),
                "contributing_sessions": [self.session_id],
                "model_versions": {self.agent_type: self.model},
                "generated_at": datetime.now().isoformat()}})

        events_written = [{"stream_id": f"loan-{app_id}", "event_type": "DecisionGenerated"}]

        if recommendation == "APPROVE":
            limit = d.get("approved_amount_usd") or decision_obj.get("recommended_limit_usd", 0)
            await self._append_stream(f"loan-{app_id}", {
                "event_type": "ApplicationApproved", "event_version": 1, "payload": {
                    "application_id": app_id,
                    "approved_amount_usd": limit,
                    "interest_rate_pct": 6.5,
                    "term_months": 60,
                    "conditions": d.get("conditions", []),
                    "approved_by": self.session_id,
                    "effective_date": datetime.now().strftime("%Y-%m-%d"),
                    "approved_at": datetime.now().isoformat()}})
            events_written.append({"stream_id": f"loan-{app_id}", "event_type": "ApplicationApproved"})

        elif recommendation == "DECLINE":
            await self._append_stream(f"loan-{app_id}", {
                "event_type": "ApplicationDeclined", "event_version": 1, "payload": {
                    "application_id": app_id,
                    "decline_reasons": d.get("key_risks", ["Risk threshold exceeded"]),
                    "declined_by": self.session_id,
                    "adverse_action_notice_required": True,
                    "adverse_action_codes": ["RISK_THRESHOLD"],
                    "declined_at": datetime.now().isoformat()}})
            events_written.append({"stream_id": f"loan-{app_id}", "event_type": "ApplicationDeclined"})

        else:  # REFER
            await self._append_stream(f"loan-{app_id}", {
                "event_type": "HumanReviewRequested", "event_version": 1, "payload": {
                    "application_id": app_id,
                    "reason": "; ".join(d.get("key_risks", ["Referred for review"])),
                    "decision_event_id": self.session_id,
                    "assigned_to": None,
                    "requested_at": datetime.now().isoformat()}})
            events_written.append({"stream_id": f"loan-{app_id}", "event_type": "HumanReviewRequested"})

        await self._record_output_written(
            events_written,
            f"Decision: {recommendation}. {d.get('executive_summary', '')[:100]}")
        await self._record_node_execution("write_output", ["orchestrator_decision"],
                                          ["events_written"],
                                          int((time.time() - t) * 1000))
        return {**state, "output_events_written": events_written,
                "next_agent_triggered": None}
