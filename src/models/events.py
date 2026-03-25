"""
Domain event models for The Ledger — Agentic Event Store.

Single source of truth for every event in the system.
7 Aggregates:
  1. LoanApplication   stream: "loan-{application_id}"
  2. DocumentPackage   stream: "docpkg-{application_id}"
  3. AgentSession      stream: "agent-{agent_type}-{session_id}"
  4. CreditRecord      stream: "credit-{application_id}"
  5. ComplianceRecord  stream: "compliance-{application_id}"
  6. FraudScreening    stream: "fraud-{application_id}"
  7. AuditLedger       stream: "audit-{entity_id}"
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import ClassVar, Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class RiskTier(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"

class ApplicationState(str, Enum):
    SUBMITTED = "SUBMITTED"
    DOCUMENTS_PENDING = "DOCUMENTS_PENDING"
    DOCUMENTS_UPLOADED = "DOCUMENTS_UPLOADED"
    DOCUMENTS_PROCESSED = "DOCUMENTS_PROCESSED"
    CREDIT_ANALYSIS_REQUESTED = "CREDIT_ANALYSIS_REQUESTED"
    CREDIT_ANALYSIS_COMPLETE = "CREDIT_ANALYSIS_COMPLETE"
    FRAUD_SCREENING_REQUESTED = "FRAUD_SCREENING_REQUESTED"
    FRAUD_SCREENING_COMPLETE = "FRAUD_SCREENING_COMPLETE"
    COMPLIANCE_CHECK_REQUESTED = "COMPLIANCE_CHECK_REQUESTED"
    COMPLIANCE_CHECK_COMPLETE = "COMPLIANCE_CHECK_COMPLETE"
    PENDING_DECISION = "PENDING_DECISION"
    PENDING_HUMAN_REVIEW = "PENDING_HUMAN_REVIEW"
    APPROVED = "APPROVED"
    DECLINED = "DECLINED"
    DECLINED_COMPLIANCE = "DECLINED_COMPLIANCE"
    REFERRED = "REFERRED"

class DocumentType(str, Enum):
    APPLICATION_PROPOSAL = "application_proposal"
    INCOME_STATEMENT = "income_statement"
    BALANCE_SHEET = "balance_sheet"
    CASH_FLOW_STATEMENT = "cash_flow_statement"
    BANK_STATEMENTS = "bank_statements"
    TAX_RETURNS = "tax_returns"

class DocumentFormat(str, Enum):
    PDF = "pdf"
    XLSX = "xlsx"
    CSV = "csv"

class AgentType(str, Enum):
    DOCUMENT_PROCESSING = "document_processing"
    CREDIT_ANALYSIS = "credit_analysis"
    FRAUD_DETECTION = "fraud_detection"
    COMPLIANCE = "compliance"
    DECISION_ORCHESTRATOR = "decision_orchestrator"

class LoanPurpose(str, Enum):
    WORKING_CAPITAL = "working_capital"
    EQUIPMENT_FINANCING = "equipment_financing"
    REAL_ESTATE = "real_estate"
    EXPANSION = "expansion"
    REFINANCING = "refinancing"
    ACQUISITION = "acquisition"
    BRIDGE = "bridge"

class FraudAnomalyType(str, Enum):
    REVENUE_DISCREPANCY = "revenue_discrepancy"
    BALANCE_SHEET_INCONSISTENCY = "balance_sheet_inconsistency"
    UNUSUAL_SUBMISSION_PATTERN = "unusual_submission_pattern"
    IDENTITY_MISMATCH = "identity_mismatch"
    DOCUMENT_ALTERATION_SUSPECTED = "document_alteration_suspected"

class ComplianceVerdict(str, Enum):
    CLEAR = "CLEAR"
    BLOCKED = "BLOCKED"
    CONDITIONAL = "CONDITIONAL"


# ---------------------------------------------------------------------------
# Value Objects
# ---------------------------------------------------------------------------

class FinancialFacts(BaseModel):
    """Structured facts extracted from a financial statement PDF."""
    # Income Statement (GAAP)
    total_revenue: Decimal | None = None
    gross_profit: Decimal | None = None
    operating_expenses: Decimal | None = None
    operating_income: Decimal | None = None
    ebitda: Decimal | None = None
    depreciation_amortization: Decimal | None = None
    interest_expense: Decimal | None = None
    income_before_tax: Decimal | None = None
    tax_expense: Decimal | None = None
    net_income: Decimal | None = None
    # Balance Sheet (GAAP)
    total_assets: Decimal | None = None
    current_assets: Decimal | None = None
    cash_and_equivalents: Decimal | None = None
    accounts_receivable: Decimal | None = None
    inventory: Decimal | None = None
    total_liabilities: Decimal | None = None
    current_liabilities: Decimal | None = None
    long_term_debt: Decimal | None = None
    total_equity: Decimal | None = None
    # Cash Flow
    operating_cash_flow: Decimal | None = None
    investing_cash_flow: Decimal | None = None
    financing_cash_flow: Decimal | None = None
    free_cash_flow: Decimal | None = None
    # Computed ratios
    debt_to_equity: float | None = None
    current_ratio: float | None = None
    debt_to_ebitda: float | None = None
    interest_coverage: float | None = None
    gross_margin: float | None = None
    net_margin: float | None = None
    # Provenance
    fiscal_year_end: str | None = None
    currency: str = "USD"
    gaap_compliant: bool = True
    # Extraction quality metadata
    field_confidence: dict[str, float] = Field(default_factory=dict)
    page_references: dict[str, str] = Field(default_factory=dict)
    extraction_notes: list[str] = Field(default_factory=list)
    balance_sheet_balances: bool | None = None
    balance_discrepancy_usd: Decimal | None = None


class FraudAnomaly(BaseModel):
    anomaly_type: FraudAnomalyType
    description: str
    severity: str
    evidence: str
    affected_fields: list[str] = Field(default_factory=list)


class CreditDecision(BaseModel):
    risk_tier: RiskTier
    recommended_limit_usd: Decimal
    confidence: float
    rationale: str
    key_concerns: list[str] = Field(default_factory=list)
    data_quality_caveats: list[str] = Field(default_factory=list)
    policy_overrides_applied: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Base models
# ---------------------------------------------------------------------------

_BASE_FIELDS = frozenset({"event_id", "event_type", "event_version", "payload", "metadata", "recorded_at"})


class BaseEvent(BaseModel):
    """
    Base class for all domain events.
    Supports both the ClassVar pattern (src/) and the direct string pattern (starter/).
    """
    model_config = ConfigDict(populate_by_name=True)

    event_id: UUID = Field(default_factory=uuid4)
    event_type: str = ""
    event_version: int = 1
    payload: dict = Field(default_factory=dict)
    metadata: dict = Field(default_factory=dict)
    recorded_at: datetime | None = None

    def model_post_init(self, __context: object) -> None:
        cls = type(self)
        if hasattr(cls, "__event_type__"):
            object.__setattr__(self, "event_type", cls.__event_type__)
        if hasattr(cls, "__event_version__"):
            object.__setattr__(self, "event_version", cls.__event_version__)
        if not self.payload:
            domain_data = {
                k: v
                for k, v in self.model_dump().items()
                if k not in _BASE_FIELDS
            }
            object.__setattr__(self, "payload", domain_data)

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        annotations = {}
        for klass in reversed(cls.__mro__):
            annotations.update(getattr(klass, "__annotations__", {}))
        if "event_type" in annotations:
            val = getattr(cls, "event_type", None)
            if isinstance(val, str) and val:
                cls.__event_type__ = val
        if "event_version" in annotations:
            val = getattr(cls, "event_version", None)
            if isinstance(val, int):
                cls.__event_version__ = val

    def to_payload(self) -> dict:
        d = self.model_dump(mode="json")
        for k in _BASE_FIELDS:
            d.pop(k, None)
        return d

    def to_store_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "event_version": self.event_version,
            "payload": self.to_payload(),
        }


class StoredEvent(BaseModel):
    """A persisted event row as returned from the event store."""
    model_config = ConfigDict(populate_by_name=True)

    event_id: UUID
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int
    payload: dict
    metadata: dict
    recorded_at: datetime

    def __getitem__(self, key: str):
        return getattr(self, key)

    def get(self, key: str, default=None):
        return getattr(self, key, default)

    def with_payload(self, new_payload: dict, version: int) -> "StoredEvent":
        return self.model_copy(update={"payload": new_payload, "event_version": version})


class StreamMetadata(BaseModel):
    """Metadata for an event stream."""
    model_config = ConfigDict(populate_by_name=True)

    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: datetime | None = None
    metadata: dict = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Aggregate 1: LoanApplication  stream: "loan-{application_id}"
# ---------------------------------------------------------------------------

class ApplicationSubmitted(BaseEvent):
    event_type: str = "ApplicationSubmitted"
    application_id: str
    applicant_id: str
    requested_amount_usd: Decimal
    loan_purpose: LoanPurpose
    loan_term_months: int
    submission_channel: str
    contact_email: str
    contact_name: str
    submitted_at: datetime
    application_reference: str

class DocumentUploadRequested(BaseEvent):
    event_type: str = "DocumentUploadRequested"
    application_id: str
    required_document_types: list[DocumentType]
    deadline: datetime
    requested_by: str

class DocumentUploaded(BaseEvent):
    event_type: str = "DocumentUploaded"
    application_id: str
    document_id: str
    document_type: DocumentType
    document_format: DocumentFormat
    filename: str
    file_path: str
    file_size_bytes: int
    file_hash: str
    fiscal_year: int | None = None
    uploaded_at: datetime
    uploaded_by: str

class DocumentUploadFailed(BaseEvent):
    event_type: str = "DocumentUploadFailed"
    application_id: str
    document_type: DocumentType
    error_type: str
    error_message: str
    attempted_filename: str
    attempted_at: datetime

class CreditAnalysisRequested(BaseEvent):
    event_type: str = "CreditAnalysisRequested"
    application_id: str
    requested_at: datetime
    requested_by: str = "system"
    priority: str = "NORMAL"

class FraudScreeningRequested(BaseEvent):
    event_type: str = "FraudScreeningRequested"
    application_id: str
    requested_at: datetime
    triggered_by_event_id: str = ""

class ComplianceCheckRequested(BaseEvent):
    event_type: str = "ComplianceCheckRequested"
    application_id: str
    requested_at: datetime
    triggered_by_event_id: str = ""
    regulation_set_version: str = ""
    rules_to_evaluate: list[str] = Field(default_factory=list)

class DecisionRequested(BaseEvent):
    event_type: str = "DecisionRequested"
    application_id: str
    requested_at: datetime
    all_analyses_complete: bool
    triggered_by_event_id: str

class DecisionGenerated(BaseEvent):
    event_type: str = "DecisionGenerated"
    event_version: int = 2
    application_id: str
    orchestrator_session_id: str
    recommendation: str
    confidence: float
    approved_amount_usd: Decimal | None = None
    conditions: list[str] = Field(default_factory=list)
    executive_summary: str
    key_risks: list[str] = Field(default_factory=list)
    contributing_sessions: list[str] = Field(default_factory=list)
    model_versions: dict[str, str] = Field(default_factory=dict)
    generated_at: datetime

class HumanReviewRequested(BaseEvent):
    event_type: str = "HumanReviewRequested"
    application_id: str
    reason: str
    decision_event_id: str
    assigned_to: str | None = None
    requested_at: datetime

class HumanReviewCompleted(BaseEvent):
    event_type: str = "HumanReviewCompleted"
    application_id: str
    reviewer_id: str
    override: bool
    original_recommendation: str
    final_decision: str
    override_reason: str | None = None
    reviewed_at: datetime

class ApplicationApproved(BaseEvent):
    event_type: str = "ApplicationApproved"
    application_id: str
    approved_amount_usd: Decimal
    interest_rate_pct: float = 0.0
    term_months: int = 0
    conditions: list[str] = Field(default_factory=list)
    approved_by: str = "system"
    effective_date: str = ""
    approved_at: datetime

class ApplicationDeclined(BaseEvent):
    event_type: str = "ApplicationDeclined"
    application_id: str
    decline_reasons: list[str]
    declined_by: str
    adverse_action_notice_required: bool
    adverse_action_codes: list[str] = Field(default_factory=list)
    declined_at: datetime

class ApplicationWithdrawn(BaseEvent):
    event_type: str = "ApplicationWithdrawn"
    application_id: str
    withdrawn_at: datetime
    reason: str

class CreditAnalysisSuperseded(BaseEvent):
    event_type: str = "CreditAnalysisSuperseded"
    application_id: str
    superseded_at: datetime
    reason: str


# ---------------------------------------------------------------------------
# Aggregate 2: DocumentPackage  stream: "docpkg-{application_id}"
# ---------------------------------------------------------------------------

class PackageCreated(BaseEvent):
    event_type: str = "PackageCreated"
    package_id: str
    application_id: str
    required_documents: list[DocumentType]
    created_at: datetime

class DocumentAdded(BaseEvent):
    event_type: str = "DocumentAdded"
    package_id: str
    document_id: str
    document_type: DocumentType
    document_format: DocumentFormat
    file_hash: str
    added_at: datetime

class DocumentFormatValidated(BaseEvent):
    event_type: str = "DocumentFormatValidated"
    package_id: str
    document_id: str
    document_type: DocumentType
    page_count: int
    detected_format: str
    validated_at: datetime

class DocumentFormatRejected(BaseEvent):
    event_type: str = "DocumentFormatRejected"
    package_id: str
    document_id: str
    rejection_reason: str
    rejected_at: datetime

class ExtractionStarted(BaseEvent):
    event_type: str = "ExtractionStarted"
    package_id: str
    document_id: str
    document_type: DocumentType
    pipeline_version: str
    extraction_model: str
    started_at: datetime

class ExtractionCompleted(BaseEvent):
    event_type: str = "ExtractionCompleted"
    package_id: str
    document_id: str
    document_type: DocumentType
    facts: FinancialFacts | None = None
    raw_text_length: int
    tables_extracted: int
    processing_ms: int
    completed_at: datetime

class ExtractionFailed(BaseEvent):
    event_type: str = "ExtractionFailed"
    package_id: str
    document_id: str
    error_type: str
    error_message: str
    partial_facts: FinancialFacts | None = None
    failed_at: datetime

class QualityAssessmentCompleted(BaseEvent):
    event_type: str = "QualityAssessmentCompleted"
    package_id: str
    document_id: str
    overall_confidence: float
    is_coherent: bool
    anomalies: list[str] = Field(default_factory=list)
    critical_missing_fields: list[str] = Field(default_factory=list)
    reextraction_recommended: bool
    auditor_notes: str
    assessed_at: datetime

class PackageReadyForAnalysis(BaseEvent):
    event_type: str = "PackageReadyForAnalysis"
    package_id: str
    application_id: str
    documents_processed: int
    has_quality_flags: bool
    quality_flag_count: int
    ready_at: datetime


# ---------------------------------------------------------------------------
# Aggregate 3: AgentSession  stream: "agent-{agent_type}-{session_id}"
# ---------------------------------------------------------------------------

class AgentSessionStarted(BaseEvent):
    event_type: str = "AgentSessionStarted"
    session_id: str
    agent_type: AgentType
    agent_id: str
    application_id: str
    model_version: str
    langgraph_graph_version: str
    context_source: str | None = None
    context_token_count: int | None = None
    started_at: datetime

class AgentInputValidated(BaseEvent):
    event_type: str = "AgentInputValidated"
    session_id: str
    agent_type: AgentType
    application_id: str
    inputs_validated: list[str]
    validation_duration_ms: int
    validated_at: datetime

class AgentInputValidationFailed(BaseEvent):
    event_type: str = "AgentInputValidationFailed"
    session_id: str
    agent_type: AgentType
    application_id: str
    missing_inputs: list[str]
    validation_errors: list[str]
    failed_at: datetime

class AgentNodeExecuted(BaseEvent):
    event_type: str = "AgentNodeExecuted"
    session_id: str
    agent_type: AgentType
    node_name: str
    node_sequence: int
    input_keys: list[str]
    output_keys: list[str]
    llm_called: bool
    llm_tokens_input: int | None = None
    llm_tokens_output: int | None = None
    llm_cost_usd: float | None = None
    duration_ms: int
    executed_at: datetime

class AgentToolCalled(BaseEvent):
    event_type: str = "AgentToolCalled"
    session_id: str
    agent_type: AgentType
    tool_name: str
    tool_input_summary: str
    tool_output_summary: str
    tool_duration_ms: int
    called_at: datetime

class AgentOutputWritten(BaseEvent):
    event_type: str = "AgentOutputWritten"
    session_id: str
    agent_type: AgentType
    application_id: str
    events_written: list[dict]
    output_summary: str
    written_at: datetime

class AgentSessionCompleted(BaseEvent):
    event_type: str = "AgentSessionCompleted"
    session_id: str
    agent_type: AgentType
    application_id: str
    total_nodes_executed: int
    total_llm_calls: int
    total_tokens_used: int
    total_cost_usd: float
    total_duration_ms: int
    next_agent_triggered: str | None = None
    completed_at: datetime

class AgentSessionFailed(BaseEvent):
    event_type: str = "AgentSessionFailed"
    session_id: str
    agent_type: AgentType
    application_id: str
    error_type: str
    error_message: str
    last_successful_node: str | None = None
    recoverable: bool
    failed_at: datetime

class AgentSessionRecovered(BaseEvent):
    event_type: str = "AgentSessionRecovered"
    session_id: str
    agent_type: AgentType
    application_id: str
    recovered_from_session_id: str
    recovery_point: str
    recovered_at: datetime

# Legacy alias kept for backward compat
AgentContextLoaded = AgentInputValidated


# ---------------------------------------------------------------------------
# Aggregate 4: CreditRecord  stream: "credit-{application_id}"
# ---------------------------------------------------------------------------

class CreditRecordOpened(BaseEvent):
    event_type: str = "CreditRecordOpened"
    application_id: str
    applicant_id: str
    opened_at: datetime

class HistoricalProfileConsumed(BaseEvent):
    event_type: str = "HistoricalProfileConsumed"
    application_id: str
    session_id: str
    fiscal_years_loaded: list[int]
    has_prior_loans: bool
    has_defaults: bool
    revenue_trajectory: str
    data_hash: str
    consumed_at: datetime

class ExtractedFactsConsumed(BaseEvent):
    event_type: str = "ExtractedFactsConsumed"
    application_id: str
    session_id: str
    document_ids_consumed: list[str]
    facts_summary: str
    quality_flags_present: bool
    consumed_at: datetime

class CreditAnalysisCompleted(BaseEvent):
    event_type: str = "CreditAnalysisCompleted"
    event_version: int = 2
    application_id: str
    session_id: str
    decision: CreditDecision
    model_version: str
    model_deployment_id: str
    input_data_hash: str
    analysis_duration_ms: int
    regulatory_basis: list[str] = Field(default_factory=list)
    completed_at: datetime

class CreditAnalysisDeferred(BaseEvent):
    event_type: str = "CreditAnalysisDeferred"
    application_id: str
    session_id: str
    deferral_reason: str
    quality_issues: list[str]
    deferred_at: datetime


# ---------------------------------------------------------------------------
# Aggregate 5: ComplianceRecord  stream: "compliance-{application_id}"
# ---------------------------------------------------------------------------

class ComplianceCheckInitiated(BaseEvent):
    event_type: str = "ComplianceCheckInitiated"
    application_id: str
    session_id: str
    regulation_set_version: str
    rules_to_evaluate: list[str]
    initiated_at: datetime

class ComplianceRulePassed(BaseEvent):
    event_type: str = "ComplianceRulePassed"
    application_id: str
    session_id: str
    rule_id: str
    rule_name: str
    rule_version: str
    evidence_hash: str
    evaluation_notes: str
    evaluated_at: datetime

class ComplianceRuleFailed(BaseEvent):
    event_type: str = "ComplianceRuleFailed"
    application_id: str
    session_id: str
    rule_id: str
    rule_name: str
    rule_version: str
    failure_reason: str
    is_hard_block: bool
    remediation_available: bool
    remediation_description: str | None = None
    evidence_hash: str
    evaluated_at: datetime

class ComplianceRuleNoted(BaseEvent):
    event_type: str = "ComplianceRuleNoted"
    application_id: str
    session_id: str
    rule_id: str
    rule_name: str
    note_type: str
    note_text: str
    evaluated_at: datetime

class ComplianceCheckCompleted(BaseEvent):
    event_type: str = "ComplianceCheckCompleted"
    application_id: str
    session_id: str
    rules_evaluated: int
    rules_passed: int
    rules_failed: int
    rules_noted: int
    has_hard_block: bool
    overall_verdict: ComplianceVerdict
    completed_at: datetime


# ---------------------------------------------------------------------------
# Aggregate 6: FraudScreening  stream: "fraud-{application_id}"
# ---------------------------------------------------------------------------

class FraudScreeningInitiated(BaseEvent):
    event_type: str = "FraudScreeningInitiated"
    application_id: str
    session_id: str
    screening_model_version: str
    initiated_at: datetime

class FraudAnomalyDetected(BaseEvent):
    event_type: str = "FraudAnomalyDetected"
    application_id: str
    session_id: str
    anomaly: FraudAnomaly
    detected_at: datetime

class FraudScreeningCompleted(BaseEvent):
    event_type: str = "FraudScreeningCompleted"
    application_id: str
    session_id: str
    fraud_score: float
    risk_level: str
    anomalies_found: int
    recommendation: str
    screening_model_version: str
    input_data_hash: str
    completed_at: datetime


# ---------------------------------------------------------------------------
# Aggregate 7: AuditLedger  stream: "audit-{entity_id}"
# ---------------------------------------------------------------------------

class AuditIntegrityCheckRun(BaseEvent):
    event_type: str = "AuditIntegrityCheckRun"
    entity_type: str = ""
    entity_id: str
    check_timestamp: datetime
    events_verified_count: int
    integrity_hash: str
    previous_hash: str | None = None
    chain_valid: bool = True
    tamper_detected: bool = False


# ---------------------------------------------------------------------------
# Event Registry
# ---------------------------------------------------------------------------

EVENT_REGISTRY: dict[str, type[BaseEvent]] = {
    # LoanApplication
    "ApplicationSubmitted": ApplicationSubmitted,
    "DocumentUploadRequested": DocumentUploadRequested,
    "DocumentUploaded": DocumentUploaded,
    "DocumentUploadFailed": DocumentUploadFailed,
    "CreditAnalysisRequested": CreditAnalysisRequested,
    "FraudScreeningRequested": FraudScreeningRequested,
    "ComplianceCheckRequested": ComplianceCheckRequested,
    "DecisionRequested": DecisionRequested,
    "DecisionGenerated": DecisionGenerated,
    "HumanReviewRequested": HumanReviewRequested,
    "HumanReviewCompleted": HumanReviewCompleted,
    "ApplicationApproved": ApplicationApproved,
    "ApplicationDeclined": ApplicationDeclined,
    "ApplicationWithdrawn": ApplicationWithdrawn,
    "CreditAnalysisSuperseded": CreditAnalysisSuperseded,
    # DocumentPackage
    "PackageCreated": PackageCreated,
    "DocumentAdded": DocumentAdded,
    "DocumentFormatValidated": DocumentFormatValidated,
    "DocumentFormatRejected": DocumentFormatRejected,
    "ExtractionStarted": ExtractionStarted,
    "ExtractionCompleted": ExtractionCompleted,
    "ExtractionFailed": ExtractionFailed,
    "QualityAssessmentCompleted": QualityAssessmentCompleted,
    "PackageReadyForAnalysis": PackageReadyForAnalysis,
    # AgentSession
    "AgentSessionStarted": AgentSessionStarted,
    "AgentInputValidated": AgentInputValidated,
    "AgentInputValidationFailed": AgentInputValidationFailed,
    "AgentNodeExecuted": AgentNodeExecuted,
    "AgentToolCalled": AgentToolCalled,
    "AgentOutputWritten": AgentOutputWritten,
    "AgentSessionCompleted": AgentSessionCompleted,
    "AgentSessionFailed": AgentSessionFailed,
    "AgentSessionRecovered": AgentSessionRecovered,
    # CreditRecord
    "CreditRecordOpened": CreditRecordOpened,
    "HistoricalProfileConsumed": HistoricalProfileConsumed,
    "ExtractedFactsConsumed": ExtractedFactsConsumed,
    "CreditAnalysisCompleted": CreditAnalysisCompleted,
    "CreditAnalysisDeferred": CreditAnalysisDeferred,
    # ComplianceRecord
    "ComplianceCheckInitiated": ComplianceCheckInitiated,
    "ComplianceRulePassed": ComplianceRulePassed,
    "ComplianceRuleFailed": ComplianceRuleFailed,
    "ComplianceRuleNoted": ComplianceRuleNoted,
    "ComplianceCheckCompleted": ComplianceCheckCompleted,
    # FraudScreening
    "FraudScreeningInitiated": FraudScreeningInitiated,
    "FraudAnomalyDetected": FraudAnomalyDetected,
    "FraudScreeningCompleted": FraudScreeningCompleted,
    # AuditLedger
    "AuditIntegrityCheckRun": AuditIntegrityCheckRun,
}


def deserialize_event(event_type: str, payload: dict) -> BaseEvent:
    cls = EVENT_REGISTRY.get(event_type)
    if not cls:
        raise ValueError(f"Unknown event_type: {event_type!r}")
    return cls(event_type=event_type, **payload)


# Legacy aliases for backward compatibility
AgentSessionClosed = AgentSessionCompleted
