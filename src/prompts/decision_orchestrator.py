"""
src/prompts/decision_orchestrator.py

Prompt for DecisionOrchestratorAgent._node_synthesize_decision()
Synthesises credit, fraud, and compliance analyses into a final loan recommendation.

Note: The LLM recommendation may be overridden by hard constraints in
_node_apply_hard_constraints() after this call:
  - compliance BLOCKED  → force DECLINE
  - confidence < 0.60   → force REFER
  - fraud_score > 0.60  → force REFER
"""
from __future__ import annotations

DECISION_ORCHESTRATOR_SYSTEM = (
    "You are a loan decision orchestrator. Given credit, fraud, and compliance analyses, "
    "produce a final recommendation. Return ONLY JSON:\n"
    '{"recommendation":"APPROVE"|"DECLINE"|"REFER",'
    '"confidence":<0-1>,"approved_amount_usd":<int or null>,'
    '"executive_summary":"<2-3 sentences>","key_risks":[],"conditions":[]}'
)


def build_decision_orchestrator_user(
    risk_tier: str | None,
    credit_confidence: float | None,
    recommended_limit_usd: float,
    fraud_score: float,
    fraud_risk_level: str | None,
    anomalies_found: int,
    compliance_verdict: str | None,
    has_hard_block: bool,
) -> str:
    return (
        f"Credit: risk_tier={risk_tier}, "
        f"confidence={credit_confidence}, "
        f"limit=${recommended_limit_usd:,.0f}\n"
        f"Fraud: score={fraud_score}, "
        f"risk={fraud_risk_level}, "
        f"anomalies={anomalies_found}\n"
        f"Compliance: verdict={compliance_verdict}, "
        f"hard_block={has_hard_block}"
    )
