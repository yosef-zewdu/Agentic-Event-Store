"""
src/prompts/credit_analysis.py

Prompt for CreditAnalysisAgent._node_analyze()
Evaluates loan application risk and produces a structured credit decision.
"""
from __future__ import annotations
import json

CREDIT_ANALYSIS_SYSTEM = (
    "You are a commercial credit analyst at Apex Financial Services. "
    "Evaluate the loan application and return ONLY a JSON object:\n"
    '{"risk_tier":"LOW"|"MEDIUM"|"HIGH","recommended_limit_usd":<int>,'
    '"confidence":<float 0-1>,"rationale":"<3-5 sentences>",'
    '"key_concerns":[],"data_quality_caveats":[],"policy_overrides_applied":[]}\n'
    "Hard rules:\n"
    "- recommended_limit_usd must not exceed annual_revenue * 0.35\n"
    "- prior loan default -> risk_tier = HIGH\n"
    "- active HIGH severity compliance flag -> confidence <= 0.50"
)


def build_credit_analysis_user(
    company_name: str,
    requested_amount_usd: float,
    loan_purpose: str,
    historical_financials: list[dict],
    extracted_facts: dict,
    quality_flags: list,
    compliance_flags: list,
    loan_history: list,
) -> str:
    fin_table = "\n".join(
        f"FY{f.get('fiscal_year')}: revenue={f.get('total_revenue')}, "
        f"ebitda={f.get('ebitda')}, net_income={f.get('net_income')}"
        for f in historical_financials
    ) if historical_financials else "No historical data"

    return (
        f"Applicant: {company_name}\n"
        f"Requested: ${requested_amount_usd:,.0f} for {loan_purpose}\n"
        f"Historical financials:\n{fin_table}\n"
        f"Extracted facts: {json.dumps(extracted_facts, default=str)[:800]}\n"
        f"Quality flags: {quality_flags}\n"
        f"Compliance flags: {compliance_flags}\n"
        f"Prior loans: {loan_history}"
    )
