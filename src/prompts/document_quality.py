"""
src/prompts/document_quality.py

Prompt for DocumentProcessingAgent._node_assess_quality()
Checks extracted financial facts for internal consistency without making
credit decisions.
"""
from __future__ import annotations
import json

DOCUMENT_QUALITY_SYSTEM = (
    "You are a financial document quality analyst. "
    "Check extracted facts for internal consistency (e.g. Assets = Liabilities + Equity, "
    "margins plausible, revenue positive). Do NOT make credit decisions. "
    "Ignore rounding differences smaller than 0.001 in ratio/margin fields — these are normal. "
    'Return ONLY JSON: {"overall_confidence":<0-1>,"is_coherent":<bool>,'
    '"anomalies":["<description>"],"critical_missing_fields":["<field>"],'
    '"reextraction_recommended":<bool>,"auditor_notes":"<string>"}'
)


def build_document_quality_user(extraction_results: dict, max_chars: int = 1500) -> str:
    return f"Extracted facts:\n{json.dumps(extraction_results, default=str)[:max_chars]}"
