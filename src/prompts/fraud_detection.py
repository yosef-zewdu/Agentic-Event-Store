"""
src/prompts/fraud_detection.py

Prompt for FraudDetectionAgent._node_analyze_fraud_patterns()
Cross-references submitted financial facts against historical registry data
to detect anomalies that may indicate fraud or document manipulation.
"""
from __future__ import annotations
import json

FRAUD_DETECTION_SYSTEM = (
    "You are a fraud detection analyst. Compare submitted financial facts against "
    "historical registry data. Return ONLY JSON:\n"
    '{"fraud_score":<0.0-1.0>,"risk_level":"LOW"|"MEDIUM"|"HIGH",'
    '"anomalies":[{"anomaly_type":"revenue_discrepancy"|"balance_sheet_inconsistency"'
    '|"unusual_submission_pattern","description":"<str>","severity":"LOW"|"MEDIUM"|"HIGH",'
    '"evidence":"<str>","affected_fields":[]}],'
    '"recommendation":"CLEAR"|"REVIEW"|"REJECT"}\n'
    "Rule: fraud_score > 0.3 requires at least one named anomaly with supporting evidence."
)


def build_fraud_detection_user(
    company_name: str,
    historical_financials: list[dict],
    extracted_facts: dict,
) -> str:
    hist_summary = "\n".join(
        f"FY{h.get('fiscal_year')}: revenue={h.get('total_revenue')}"
        for h in historical_financials
    ) if historical_financials else "No history"

    return (
        f"Company: {company_name}\n"
        f"Registry history:\n{hist_summary}\n"
        f"Submitted facts: {json.dumps(extracted_facts, default=str)[:800]}"
    )
