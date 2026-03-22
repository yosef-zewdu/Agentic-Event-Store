# -*- coding: utf-8 -*-
"""
src/registry/client.py -- Applicant Registry read-only client
"""
from __future__ import annotations
from dataclasses import dataclass
import asyncpg


@dataclass
class CompanyProfile:
    company_id: str
    name: str
    industry: str
    naics: str
    jurisdiction: str
    legal_type: str
    founded_year: int
    employee_count: int
    risk_segment: str
    trajectory: str
    submission_channel: str
    ip_region: str


@dataclass
class FinancialYear:
    fiscal_year: int
    total_revenue: float
    gross_profit: float
    operating_income: float
    ebitda: float
    net_income: float
    total_assets: float
    total_liabilities: float
    total_equity: float
    long_term_debt: float
    cash_and_equivalents: float
    current_assets: float
    current_liabilities: float
    accounts_receivable: float
    inventory: float
    debt_to_equity: float
    current_ratio: float
    debt_to_ebitda: float
    interest_coverage_ratio: float
    gross_margin: float
    ebitda_margin: float
    net_margin: float


@dataclass
class ComplianceFlag:
    flag_type: str
    severity: str
    is_active: bool
    added_date: str
    note: str


class ApplicantRegistryClient:
    """READ-ONLY access to the Applicant Registry."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_company(self, company_id: str) -> CompanyProfile | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM applicant_registry.companies WHERE company_id = $1",
                company_id)
            if not row:
                return None
            return CompanyProfile(
                company_id=row["company_id"],
                name=row["name"],
                industry=row["industry"],
                naics=row["naics"],
                jurisdiction=row["jurisdiction"],
                legal_type=row["legal_type"],
                founded_year=row["founded_year"],
                employee_count=row["employee_count"],
                risk_segment=row["risk_segment"],
                trajectory=row["trajectory"],
                submission_channel=row["submission_channel"],
                ip_region=row["ip_region"],
            )

    async def get_financial_history(
        self, company_id: str, years: list[int] | None = None
    ) -> list[FinancialYear]:
        async with self._pool.acquire() as conn:
            if years:
                rows = await conn.fetch(
                    "SELECT * FROM applicant_registry.financial_history"
                    " WHERE company_id = $1 AND fiscal_year = ANY($2)"
                    " ORDER BY fiscal_year ASC",
                    company_id, years)
            else:
                rows = await conn.fetch(
                    "SELECT * FROM applicant_registry.financial_history"
                    " WHERE company_id = $1 ORDER BY fiscal_year ASC",
                    company_id)
            return [
                FinancialYear(
                    fiscal_year=r["fiscal_year"],
                    total_revenue=float(r["total_revenue"] or 0),
                    gross_profit=float(r["gross_profit"] or 0),
                    operating_income=float(r["operating_income"] or 0),
                    ebitda=float(r["ebitda"] or 0),
                    net_income=float(r["net_income"] or 0),
                    total_assets=float(r["total_assets"] or 0),
                    total_liabilities=float(r["total_liabilities"] or 0),
                    total_equity=float(r["total_equity"] or 0),
                    long_term_debt=float(r["long_term_debt"] or 0),
                    cash_and_equivalents=float(r["cash_and_equivalents"] or 0),
                    current_assets=float(r["current_assets"] or 0),
                    current_liabilities=float(r["current_liabilities"] or 0),
                    accounts_receivable=float(r["accounts_receivable"] or 0),
                    inventory=float(r["inventory"] or 0),
                    debt_to_equity=float(r["debt_to_equity"] or 0),
                    current_ratio=float(r["current_ratio"] or 0),
                    debt_to_ebitda=float(r["debt_to_ebitda"] or 0),
                    interest_coverage_ratio=float(r["interest_coverage_ratio"] or 0),
                    gross_margin=float(r["gross_margin"] or 0),
                    ebitda_margin=float(r["ebitda_margin"] or 0),
                    net_margin=float(r["net_margin"] or 0),
                )
                for r in rows
            ]

    async def get_compliance_flags(
        self, company_id: str, active_only: bool = False
    ) -> list[ComplianceFlag]:
        async with self._pool.acquire() as conn:
            if active_only:
                rows = await conn.fetch(
                    "SELECT * FROM applicant_registry.compliance_flags"
                    " WHERE company_id = $1 AND is_active = TRUE",
                    company_id)
            else:
                rows = await conn.fetch(
                    "SELECT * FROM applicant_registry.compliance_flags"
                    " WHERE company_id = $1",
                    company_id)
            return [
                ComplianceFlag(
                    flag_type=r["flag_type"],
                    severity=r["severity"],
                    is_active=r["is_active"],
                    added_date=str(r["added_date"]),
                    note=r["note"],
                )
                for r in rows
            ]

    async def get_loan_relationships(self, company_id: str) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM applicant_registry.loan_relationships"
                " WHERE company_id = $1",
                company_id)
            return [dict(r) for r in rows]
