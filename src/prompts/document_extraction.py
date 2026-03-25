"""
src/prompts/document_extraction.py

Prompts for DocumentProcessingAgent._llm_extract_facts()
Extracts structured financial data from raw PDF text for income statements
and balance sheets.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Schema definitions — what fields to extract per document type
# ---------------------------------------------------------------------------

INCOME_STATEMENT_SCHEMA = (
    '{"fiscal_year":<int>,"total_revenue":<float>,"cost_of_goods_sold":<float>,'
    '"gross_profit":<float>,"operating_expenses":<float>,'
    '"depreciation_amortization":<float>,"operating_income":<float>,'
    '"interest_expense":<float>,"income_before_tax":<float>,'
    '"tax_expense":<float>,"net_income":<float>,"ebitda":<float>,'
    '"gross_margin":<float 0-1>,"ebitda_margin":<float 0-1>,"net_margin":<float 0-1>}'
)

BALANCE_SHEET_SCHEMA = (
    '{"fiscal_year":<int>,"total_assets":<float>,"current_assets":<float>,'
    '"cash_and_equivalents":<float>,"accounts_receivable":<float>,"inventory":<float>,'
    '"other_current_assets":<float>,"property_plant_equipment_net":<float>,'
    '"total_liabilities":<float>,"current_liabilities":<float>,'
    '"accounts_payable":<float>,"accrued_liabilities":<float>,'
    '"current_portion_long_term_debt":<float>,"long_term_debt":<float>,'
    '"other_long_term_liabilities":<float>,"total_equity":<float>,'
    '"debt_to_equity":<float>,"current_ratio":<float>,"debt_to_ebitda":<float>}'
)

SCHEMAS: dict[str, str] = {
    "income_statement": INCOME_STATEMENT_SCHEMA,
    "balance_sheet": BALANCE_SHEET_SCHEMA,
}


def build_extraction_system(doc_type: str) -> str:
    schema = SCHEMAS.get(doc_type, "{}")
    doc_label = doc_type.replace("_", " ")
    return (
        f"You are a financial document parser. Extract structured data from the {doc_label}. "
        f"Return ONLY a JSON object matching this schema (use null for missing fields):\n{schema}\n"
        "All monetary values in USD. Ratios as decimals (e.g. 0.35 not 35%). "
        "If a value is not present in the document, use null."
    )


def build_extraction_user(raw_text: str, max_chars: int = 3000) -> str:
    return f"Document text (truncated to {max_chars} chars):\n{raw_text[:max_chars]}"
