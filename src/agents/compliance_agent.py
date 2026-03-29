from __future__ import annotations
import asyncio, hashlib, json, re, time
from datetime import datetime
from langgraph.graph import StateGraph, END
from src.agents.base_agent import BaseApexAgent, LANGGRAPH_VERSION, MAX_OCC_RETRIES, SCREENING_MODEL_VERSION, REGULATION_SET_VERSION


class ComplianceAgent(BaseApexAgent):
    def build_graph(self):
        from typing import TypedDict
        class S(TypedDict):
            application_id: str; session_id: str; agent_id: str
            company_profile: dict | None; requested_amount_usd: float | None
            rules_results: list | None; hard_block: bool | None
            overall_verdict: str | None; errors: list
            output_events_written: list; next_agent_triggered: str | None

        g = StateGraph(S)
        for name, fn in [
            ("validate_inputs", self._node_validate_inputs),
            ("check_reg001",    self._node_check_reg001),
            ("check_reg002",    self._node_check_reg002),
            ("check_reg003",    self._node_check_reg003),
            ("check_reg004",    self._node_check_reg004),
            ("check_reg005",    self._node_check_reg005),
            ("check_reg006",    self._node_check_reg006),
            ("write_output",    self._node_write_output),
        ]:
            g.add_node(name, fn)
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs", "check_reg001")
        g.add_edge("check_reg001", "check_reg002")
        g.add_conditional_edges("check_reg002",
                                 lambda s: "write_output" if s.get("hard_block") else "check_reg003")
        g.add_conditional_edges("check_reg003",
                                 lambda s: "write_output" if s.get("hard_block") else "check_reg004")
        g.add_edge("check_reg004", "check_reg005")
        g.add_conditional_edges("check_reg005",
                                 lambda s: "write_output" if s.get("hard_block") else "check_reg006")
        g.add_edge("check_reg006", "write_output")
        g.add_edge("write_output", END)
        return g.compile()

    def _rule_passed(self, rule_id, rule_name, notes=""):
        return {"event_type": "ComplianceRulePassed", "event_version": 1, "payload": {
            "application_id": self.application_id, "session_id": self.session_id,
            "rule_id": rule_id, "rule_name": rule_name, "rule_version": "1.0",
            "evidence_hash": self._sha(rule_id),
            "evaluation_notes": notes,
            "evaluated_at": datetime.now().isoformat()}}

    def _rule_failed(self, rule_id, rule_name, reason, hard_block=False, remediation=False, remediation_desc=None):
        return {"event_type": "ComplianceRuleFailed", "event_version": 1, "payload": {
            "application_id": self.application_id, "session_id": self.session_id,
            "rule_id": rule_id, "rule_name": rule_name, "rule_version": "1.0",
            "failure_reason": reason, "is_hard_block": hard_block,
            "remediation_available": remediation,
            "remediation_description": remediation_desc,
            "evidence_hash": self._sha(rule_id),
            "evaluated_at": datetime.now().isoformat()}}

    def _rule_noted(self, rule_id, rule_name, note_type, note_text):
        return {"event_type": "ComplianceRuleNoted", "event_version": 1, "payload": {
            "application_id": self.application_id, "session_id": self.session_id,
            "rule_id": rule_id, "rule_name": rule_name,
            "note_type": note_type, "note_text": note_text,
            "evaluated_at": datetime.now().isoformat()}}

    async def _node_validate_inputs(self, state):
        t = time.time()
        loan_events = await self.store.load_stream(f"loan-{state['application_id']}")
        submitted = next((e for e in loan_events if e["event_type"] == "ApplicationSubmitted"), None)
        applicant_id = submitted["payload"]["applicant_id"] if submitted else "UNKNOWN"
        requested = float(submitted["payload"].get("requested_amount_usd", 0)) if submitted else 0.0
        profile = await self.registry.get_company(applicant_id)
        flags = await self.registry.get_compliance_flags(applicant_id)

        await self._append_stream(f"compliance-{state['application_id']}", {
            "event_type": "ComplianceCheckInitiated", "event_version": 1, "payload": {
                "application_id": state["application_id"],
                "session_id": self.session_id,
                "regulation_set_version": REGULATION_SET_VERSION,
                "rules_to_evaluate": ["REG-001", "REG-002", "REG-003",
                                      "REG-004", "REG-005", "REG-006"],
                "initiated_at": datetime.now().isoformat()}})

        await self._record_node_execution("validate_inputs", ["application_id"],
                                          ["company_profile"], int((time.time() - t) * 1000))
        return {**state,
                "company_profile": vars(profile) if profile else {},
                "_compliance_flags": [vars(f) for f in flags],
                "requested_amount_usd": requested,
                "rules_results": [], "hard_block": False}

    async def _node_check_reg001(self, state):
        t = time.time()
        app_id = state["application_id"]
        flags = state.get("_compliance_flags", [])
        aml_active = any(f["flag_type"] == "AML_WATCH" and f["is_active"] for f in flags)
        if aml_active:
            ev = self._rule_failed("REG-001", "BSA Anti-Money Laundering",
                                   "Active AML_WATCH flag", hard_block=False)
        else:
            ev = self._rule_passed("REG-001", "BSA Anti-Money Laundering", "No active AML flags")
        await self._append_stream(f"compliance-{app_id}", ev)
        await asyncio.sleep(2)
        results = state.get("rules_results", []) + [{"rule": "REG-001", "passed": not aml_active}]
        await self._record_node_execution("check_reg001", ["company_profile"],
                                          ["rules_results"], int((time.time() - t) * 1000))
        return {**state, "rules_results": results}

    async def _node_check_reg002(self, state):
        t = time.time()
        app_id = state["application_id"]
        flags = state.get("_compliance_flags", [])
        sanctions_active = any(f["flag_type"] == "SANCTIONS_REVIEW" and f["is_active"] for f in flags)
        if sanctions_active:
            ev = self._rule_failed("REG-002", "OFAC Sanctions Screening",
                                   "Active SANCTIONS_REVIEW flag", hard_block=True)
            hard_block = True
        else:
            ev = self._rule_passed("REG-002", "OFAC Sanctions Screening", "No sanctions flags")
            hard_block = state.get("hard_block", False)
        await self._append_stream(f"compliance-{app_id}", ev)
        await asyncio.sleep(2)
        results = state.get("rules_results", []) + [{"rule": "REG-002", "passed": not sanctions_active}]
        await self._record_node_execution("check_reg002", ["company_profile"],
                                          ["rules_results", "hard_block"],
                                          int((time.time() - t) * 1000))
        return {**state, "rules_results": results, "hard_block": hard_block}

    async def _node_check_reg003(self, state):
        t = time.time()
        app_id = state["application_id"]
        profile = state.get("company_profile", {})
        jurisdiction = profile.get("jurisdiction", "")
        if jurisdiction == "MT":
            ev = self._rule_failed("REG-003", "Jurisdiction Check",
                                   "Montana jurisdiction not eligible", hard_block=True)
            hard_block = True
        else:
            ev = self._rule_passed("REG-003", "Jurisdiction Check",
                                   f"Jurisdiction {jurisdiction} is eligible")
            hard_block = state.get("hard_block", False)
        await self._append_stream(f"compliance-{app_id}", ev)
        await asyncio.sleep(2)
        results = state.get("rules_results", []) + [{"rule": "REG-003", "passed": jurisdiction != "MT"}]
        await self._record_node_execution("check_reg003", ["company_profile"],
                                          ["rules_results", "hard_block"],
                                          int((time.time() - t) * 1000))
        return {**state, "rules_results": results, "hard_block": hard_block}

    async def _node_check_reg004(self, state):
        t = time.time()
        app_id = state["application_id"]
        profile = state.get("company_profile", {})
        legal_type = profile.get("legal_type", "")
        amount = state.get("requested_amount_usd", 0) or 0
        failed = legal_type == "Sole Proprietor" and float(amount) > 250_000
        if failed:
            ev = self._rule_failed("REG-004", "Legal Entity Loan Limit",
                                   "Sole Proprietor exceeds $250K limit",
                                   hard_block=False, remediation=True,
                                   remediation_desc="Incorporate as LLC to qualify for higher limits")
        else:
            ev = self._rule_passed("REG-004", "Legal Entity Loan Limit",
                                   f"Legal type {legal_type} within limits")
        await self._append_stream(f"compliance-{app_id}", ev)
        await asyncio.sleep(2)
        results = state.get("rules_results", []) + [{"rule": "REG-004", "passed": not failed}]
        await self._record_node_execution("check_reg004", ["company_profile", "requested_amount_usd"],
                                          ["rules_results"], int((time.time() - t) * 1000))
        return {**state, "rules_results": results}

    async def _node_check_reg005(self, state):
        t = time.time()
        app_id = state["application_id"]
        profile = state.get("company_profile", {})
        founded = profile.get("founded_year", 2020)
        if int(founded) > 2022:
            ev = self._rule_failed("REG-005", "Operating History Requirement",
                                   f"Founded {founded} - less than 3 years operating history",
                                   hard_block=True)
            hard_block = True
        else:
            ev = self._rule_passed("REG-005", "Operating History Requirement",
                                   f"Founded {founded} - meets 3-year requirement")
            hard_block = state.get("hard_block", False)
        await self._append_stream(f"compliance-{app_id}", ev)
        await asyncio.sleep(2)
        results = state.get("rules_results", []) + [{"rule": "REG-005", "passed": int(founded) <= 2022}]
        await self._record_node_execution("check_reg005", ["company_profile"],
                                          ["rules_results", "hard_block"],
                                          int((time.time() - t) * 1000))
        return {**state, "rules_results": results, "hard_block": hard_block}

    async def _node_check_reg006(self, state):
        t = time.time()
        app_id = state["application_id"]
        ev = self._rule_noted("REG-006", "CRA Community Reinvestment",
                              "CRA_CONSIDERATION",
                              "Application noted for CRA reporting purposes")
        await self._append_stream(f"compliance-{app_id}", ev)
        await asyncio.sleep(2)
        results = state.get("rules_results", []) + [{"rule": "REG-006", "passed": True, "noted": True}]
        await self._record_node_execution("check_reg006", ["company_profile"],
                                          ["rules_results"], int((time.time() - t) * 1000))
        return {**state, "rules_results": results}

    async def _node_write_output(self, state):
        t = time.time()
        app_id = state["application_id"]
        results = state.get("rules_results", [])
        hard_block = state.get("hard_block", False)
        passed = sum(1 for r in results if r.get("passed") and not r.get("noted"))
        failed = sum(1 for r in results if not r.get("passed"))
        noted = sum(1 for r in results if r.get("noted"))
        verdict = "BLOCKED" if hard_block else ("CONDITIONAL" if failed > 0 else "CLEAR")

        await self._append_stream(f"compliance-{app_id}", {
            "event_type": "ComplianceCheckCompleted", "event_version": 1, "payload": {
                "application_id": app_id, "session_id": self.session_id,
                "rules_evaluated": len(results),
                "rules_passed": passed, "rules_failed": failed, "rules_noted": noted,
                "has_hard_block": hard_block, "overall_verdict": verdict,
                "completed_at": datetime.now().isoformat()}})

        if verdict == "BLOCKED":
            await self._append_stream(f"loan-{app_id}", {
                "event_type": "ApplicationDeclined", "event_version": 1, "payload": {
                    "application_id": app_id,
                    "decline_reasons": ["Compliance hard block - see compliance stream"],
                    "declined_by": self.session_id,
                    "adverse_action_notice_required": True,
                    "adverse_action_codes": ["COMPLIANCE_BLOCK"],
                    "declined_at": datetime.now().isoformat()}})
            next_trigger = None
        else:
            await self._append_stream(f"loan-{app_id}", {
                "event_type": "DecisionRequested", "event_version": 1, "payload": {
                    "application_id": app_id,
                    "requested_at": datetime.now().isoformat(),
                    "all_analyses_complete": True,
                    "triggered_by_event_id": self.session_id}})
            next_trigger = "decision_orchestrator"

        events_written = [
            {"stream_id": f"compliance-{app_id}", "event_type": "ComplianceCheckCompleted"},
            {"stream_id": f"loan-{app_id}",
             "event_type": "ApplicationDeclined" if verdict == "BLOCKED" else "DecisionRequested"},
        ]
        await self._record_output_written(
            events_written,
            f"Compliance verdict: {verdict}. {passed} passed, {failed} failed, {noted} noted.")
        await self._record_node_execution("write_output", ["rules_results"],
                                          ["events_written"],
                                          int((time.time() - t) * 1000))
        return {**state, "overall_verdict": verdict,
                "output_events_written": events_written,
                "next_agent_triggered": next_trigger}
