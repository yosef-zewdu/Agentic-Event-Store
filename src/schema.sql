-- =============================================================================
-- APEX FINANCIAL SERVICES — THE LEDGER
-- Full Database Schema (generated from ledger/schema/events.py)
-- =============================================================================
-- 7 Aggregates / Stream Types:
--   loan-{application_id}          LoanApplication
--   docpkg-{application_id}        DocumentPackage
--   agent-{agent_type}-{session}   AgentSession
--   credit-{application_id}        CreditRecord
--   compliance-{application_id}    ComplianceRecord
--   fraud-{application_id}         FraudScreening
--   audit-{entity_id}              AuditLedger
-- =============================================================================

-- ---------------------------------------------------------------------------
-- ENUMS
-- ---------------------------------------------------------------------------

CREATE TYPE risk_tier AS ENUM ('LOW', 'MEDIUM', 'HIGH');

CREATE TYPE application_state AS ENUM (
    'SUBMITTED',
    'DOCUMENTS_PENDING',
    'DOCUMENTS_UPLOADED',
    'DOCUMENTS_PROCESSED',
    'CREDIT_ANALYSIS_REQUESTED',
    'CREDIT_ANALYSIS_COMPLETE',
    'FRAUD_SCREENING_REQUESTED',
    'FRAUD_SCREENING_COMPLETE',
    'COMPLIANCE_CHECK_REQUESTED',
    'COMPLIANCE_CHECK_COMPLETE',
    'PENDING_DECISION',
    'PENDING_HUMAN_REVIEW',
    'APPROVED',
    'DECLINED',
    'DECLINED_COMPLIANCE',
    'REFERRED'
);

CREATE TYPE document_type AS ENUM (
    'application_proposal',
    'income_statement',
    'balance_sheet',
    'cash_flow_statement',
    'bank_statements',
    'tax_returns'
);

CREATE TYPE document_format AS ENUM ('pdf', 'xlsx', 'csv');

CREATE TYPE agent_type AS ENUM (
    'document_processing',
    'credit_analysis',
    'fraud_detection',
    'compliance',
    'decision_orchestrator'
);

CREATE TYPE loan_purpose AS ENUM (
    'working_capital',
    'equipment_financing',
    'real_estate',
    'expansion',
    'refinancing',
    'acquisition',
    'bridge'
);

CREATE TYPE fraud_anomaly_type AS ENUM (
    'revenue_discrepancy',
    'balance_sheet_inconsistency',
    'unusual_submission_pattern',
    'identity_mismatch',
    'document_alteration_suspected'
);

CREATE TYPE compliance_verdict AS ENUM ('CLEAR', 'BLOCKED', 'CONDITIONAL');


-- ---------------------------------------------------------------------------
-- CORE EVENT STORE TABLES
-- ---------------------------------------------------------------------------

-- All streams (one row per aggregate instance)
CREATE TABLE event_streams (
    stream_id        TEXT        NOT NULL,
    aggregate_type   TEXT        NOT NULL,          -- "loan", "docpkg", "agent", etc.
    current_version  BIGINT      NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at      TIMESTAMPTZ,
    metadata         JSONB       NOT NULL DEFAULT '{}',
    CONSTRAINT event_streams_pkey PRIMARY KEY (stream_id)
);

CREATE INDEX idx_streams_type ON event_streams (aggregate_type);

-- Append-only event log (all 45 event types stored here as JSONB payloads)
CREATE TABLE events (
    event_id         UUID        NOT NULL DEFAULT gen_random_uuid(),
    stream_id        TEXT        NOT NULL,
    stream_position  BIGINT      NOT NULL,
    global_position  BIGINT      NOT NULL GENERATED ALWAYS AS IDENTITY,
    event_type       TEXT        NOT NULL,
    event_version    SMALLINT    NOT NULL DEFAULT 1,
    payload          JSONB       NOT NULL,
    metadata         JSONB       NOT NULL DEFAULT '{}',
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT events_pkey PRIMARY KEY (event_id),
    CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);

CREATE INDEX idx_events_stream   ON events (stream_id, stream_position);
CREATE INDEX idx_events_global   ON events (global_position);
CREATE INDEX idx_events_type     ON events (event_type);
CREATE INDEX idx_events_recorded ON events (recorded_at);

-- Transactional outbox for downstream messaging
CREATE TABLE outbox (
    id           UUID        NOT NULL DEFAULT gen_random_uuid(),
    event_id     UUID        NOT NULL REFERENCES events(event_id),
    destination  TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts     SMALLINT    NOT NULL DEFAULT 0,
    CONSTRAINT outbox_pkey PRIMARY KEY (id)
);

CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;

-- Aggregate snapshots (optional, for long-lived streams)
CREATE TABLE snapshots (
    snapshot_id       UUID        NOT NULL DEFAULT gen_random_uuid(),
    stream_id         TEXT        NOT NULL REFERENCES event_streams(stream_id),
    stream_position   BIGINT      NOT NULL,
    aggregate_type    TEXT        NOT NULL,
    snapshot_version  INTEGER     NOT NULL,
    state             JSONB       NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT snapshots_pkey PRIMARY KEY (snapshot_id)
);

-- Async pipeline job queue
CREATE TABLE IF NOT EXISTS pipeline_jobs (
    job_id          UUID        NOT NULL DEFAULT gen_random_uuid(),
    application_id  TEXT        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'queued'
                                CHECK (status IN ('queued', 'running', 'completed', 'failed')),
    from_agent      TEXT,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    CONSTRAINT pipeline_jobs_pkey PRIMARY KEY (job_id)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_active
    ON pipeline_jobs (status)
    WHERE status IN ('queued', 'running');

-- Projection daemon checkpoints
CREATE TABLE projection_checkpoints (
    projection_name  TEXT        NOT NULL,
    last_position    BIGINT      NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT projection_checkpoints_pkey PRIMARY KEY (projection_name)
);


-- ---------------------------------------------------------------------------
-- APPLICANT REGISTRY SCHEMA (read-only reference data)
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS applicant_registry;

CREATE TABLE applicant_registry.companies (
    company_id          TEXT        NOT NULL,
    name                TEXT        NOT NULL,
    industry            TEXT        NOT NULL,
    naics               TEXT        NOT NULL,
    jurisdiction        TEXT        NOT NULL,   -- 2-letter state code, e.g. "MT"
    legal_type          TEXT        NOT NULL,
    founded_year        INTEGER     NOT NULL,
    employee_count      INTEGER     NOT NULL,
    ein                 TEXT        NOT NULL,
    address_city        TEXT        NOT NULL,
    address_state       TEXT        NOT NULL,
    relationship_start  DATE        NOT NULL,
    account_manager     TEXT        NOT NULL,
    risk_segment        TEXT        NOT NULL CHECK (risk_segment IN ('LOW','MEDIUM','HIGH')),
    trajectory          TEXT        NOT NULL,   -- GROWTH | STABLE | DECLINING | RECOVERING | VOLATILE
    submission_channel  TEXT        NOT NULL,
    ip_region           TEXT        NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT companies_pkey PRIMARY KEY (company_id),
    CONSTRAINT companies_ein_key UNIQUE (ein)
);

CREATE TABLE applicant_registry.financial_history (
    id              SERIAL      NOT NULL,
    company_id      TEXT        NOT NULL REFERENCES applicant_registry.companies(company_id),
    fiscal_year     INTEGER     NOT NULL,
    -- Income Statement (GAAP)
    total_revenue           NUMERIC(18,2),
    gross_profit            NUMERIC(18,2),
    operating_expenses      NUMERIC(18,2),
    operating_income        NUMERIC(18,2),
    ebitda                  NUMERIC(18,2),
    depreciation_amortization NUMERIC(18,2),
    interest_expense        NUMERIC(18,2),
    income_before_tax       NUMERIC(18,2),
    tax_expense             NUMERIC(18,2),
    net_income              NUMERIC(18,2),
    -- Balance Sheet (GAAP)
    total_assets            NUMERIC(18,2),
    current_assets          NUMERIC(18,2),
    cash_and_equivalents    NUMERIC(18,2),
    accounts_receivable     NUMERIC(18,2),
    inventory               NUMERIC(18,2),
    total_liabilities       NUMERIC(18,2),
    current_liabilities     NUMERIC(18,2),
    long_term_debt          NUMERIC(18,2),
    total_equity            NUMERIC(18,2),
    -- Cash Flow
    operating_cash_flow     NUMERIC(18,2),
    investing_cash_flow     NUMERIC(18,2),
    financing_cash_flow     NUMERIC(18,2),
    free_cash_flow          NUMERIC(18,2),
    -- Computed ratios
    debt_to_equity              DOUBLE PRECISION,
    current_ratio               DOUBLE PRECISION,
    debt_to_ebitda              DOUBLE PRECISION,
    interest_coverage_ratio     DOUBLE PRECISION,
    gross_margin                DOUBLE PRECISION,
    ebitda_margin               DOUBLE PRECISION,
    net_margin                  DOUBLE PRECISION,
    balance_sheet_check         BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT financial_history_pkey PRIMARY KEY (id),
    CONSTRAINT financial_history_company_year UNIQUE (company_id, fiscal_year)
);

CREATE TABLE applicant_registry.compliance_flags (
    id          SERIAL      NOT NULL,
    company_id  TEXT        NOT NULL REFERENCES applicant_registry.companies(company_id),
    flag_type   TEXT        NOT NULL CHECK (flag_type IN ('AML_WATCH','SANCTIONS_REVIEW','PEP_LINK')),
    severity    TEXT        NOT NULL CHECK (severity IN ('LOW','MEDIUM','HIGH')),
    is_active   BOOLEAN     NOT NULL DEFAULT TRUE,
    added_date  DATE        NOT NULL,
    note        TEXT,
    CONSTRAINT compliance_flags_pkey PRIMARY KEY (id)
);

CREATE TABLE applicant_registry.loan_relationships (
    id                  SERIAL        NOT NULL,
    company_id          TEXT          NOT NULL REFERENCES applicant_registry.companies(company_id),
    -- Seeder / pipeline fields
    loan_amount         NUMERIC(18,2) NOT NULL,
    loan_year           INTEGER       NOT NULL,
    was_repaid          BOOLEAN       NOT NULL,
    default_occurred    BOOLEAN       NOT NULL DEFAULT FALSE,
    note                TEXT,
    -- Extended tracking fields (populated by richer data sources)
    loan_id             TEXT,
    loan_type           TEXT,
    original_amount     NUMERIC(18,2),
    outstanding_balance NUMERIC(18,2),
    status              TEXT,           -- ACTIVE | PAID_OFF | DEFAULTED
    originated_at       DATE,
    closed_at           DATE,
    CONSTRAINT loan_relationships_pkey PRIMARY KEY (id)
);


-- =============================================================================
-- EVENT PAYLOAD REFERENCE
-- All 45 event types stored in the events table. Payload shapes documented here.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- AGGREGATE 1: LoanApplication  (stream: loan-{application_id})
-- ---------------------------------------------------------------------------

COMMENT ON TABLE events IS
$$All 45 event types are stored here as JSONB payloads.

--- AGGREGATE 1: LoanApplication (stream: loan-{application_id}) ---

ApplicationSubmitted (v1):
  application_id          TEXT
  applicant_id            TEXT
  requested_amount_usd    NUMERIC
  loan_purpose            loan_purpose
  loan_term_months        INTEGER
  submission_channel      TEXT
  contact_email           TEXT
  contact_name            TEXT
  submitted_at            TIMESTAMPTZ
  application_reference   TEXT

DocumentUploadRequested (v1):
  application_id            TEXT
  required_document_types   TEXT[]
  deadline                  TIMESTAMPTZ
  requested_by              TEXT

DocumentUploaded (v1):
  application_id    TEXT
  document_id       TEXT
  document_type     document_type
  document_format   document_format
  filename          TEXT
  file_path         TEXT
  file_size_bytes   INTEGER
  file_hash         TEXT
  fiscal_year       INTEGER (nullable)
  uploaded_at       TIMESTAMPTZ
  uploaded_by       TEXT

DocumentUploadFailed (v1):
  application_id      TEXT
  document_type       document_type
  error_type          TEXT
  error_message       TEXT
  attempted_filename  TEXT
  attempted_at        TIMESTAMPTZ

CreditAnalysisRequested (v1):
  application_id  TEXT
  requested_at    TIMESTAMPTZ
  requested_by    TEXT
  priority        TEXT  -- "NORMAL" | "HIGH"

FraudScreeningRequested (v1):
  application_id        TEXT
  requested_at          TIMESTAMPTZ
  triggered_by_event_id TEXT

ComplianceCheckRequested (v1):
  application_id          TEXT
  requested_at            TIMESTAMPTZ
  triggered_by_event_id   TEXT
  regulation_set_version  TEXT
  rules_to_evaluate       TEXT[]

DecisionRequested (v1):
  application_id          TEXT
  requested_at            TIMESTAMPTZ
  all_analyses_complete   BOOLEAN
  triggered_by_event_id   TEXT

DecisionGenerated (v2):
  application_id            TEXT
  orchestrator_session_id   TEXT
  recommendation            TEXT  -- "APPROVE" | "DECLINE" | "REFER"
  confidence                FLOAT
  approved_amount_usd       NUMERIC (nullable)
  conditions                TEXT[]
  executive_summary         TEXT
  key_risks                 TEXT[]
  contributing_sessions     TEXT[]
  model_versions            JSONB  -- {"agent_type": "model_version"}
  generated_at              TIMESTAMPTZ

HumanReviewRequested (v1):
  application_id    TEXT
  reason            TEXT
  decision_event_id TEXT
  assigned_to       TEXT (nullable)
  requested_at      TIMESTAMPTZ

HumanReviewCompleted (v1):
  application_id          TEXT
  reviewer_id             TEXT
  override                BOOLEAN
  original_recommendation TEXT
  final_decision          TEXT
  override_reason         TEXT (nullable)
  reviewed_at             TIMESTAMPTZ

ApplicationApproved (v1):
  application_id      TEXT
  approved_amount_usd NUMERIC
  interest_rate_pct   FLOAT
  term_months         INTEGER
  conditions          TEXT[]
  approved_by         TEXT
  effective_date      TEXT
  approved_at         TIMESTAMPTZ

ApplicationDeclined (v1):
  application_id                TEXT
  decline_reasons               TEXT[]
  declined_by                   TEXT
  adverse_action_notice_required BOOLEAN
  adverse_action_codes          TEXT[]
  declined_at                   TIMESTAMPTZ

--- AGGREGATE 2: DocumentPackage (stream: docpkg-{application_id}) ---

PackageCreated (v1):
  package_id          TEXT
  application_id      TEXT
  required_documents  TEXT[]
  created_at          TIMESTAMPTZ

DocumentAdded (v1):
  package_id      TEXT
  document_id     TEXT
  document_type   document_type
  document_format document_format
  file_hash       TEXT
  added_at        TIMESTAMPTZ

DocumentFormatValidated (v1):
  package_id      TEXT
  document_id     TEXT
  document_type   document_type
  page_count      INTEGER
  detected_format TEXT
  validated_at    TIMESTAMPTZ

DocumentFormatRejected (v1):
  package_id        TEXT
  document_id       TEXT
  rejection_reason  TEXT
  rejected_at       TIMESTAMPTZ

ExtractionStarted (v1):
  package_id        TEXT
  document_id       TEXT
  document_type     document_type
  pipeline_version  TEXT
  extraction_model  TEXT
  started_at        TIMESTAMPTZ

ExtractionCompleted (v1):
  package_id        TEXT
  document_id       TEXT
  document_type     document_type
  facts             JSONB  -- FinancialFacts (all fields nullable)
  raw_text_length   INTEGER
  tables_extracted  INTEGER
  processing_ms     INTEGER
  completed_at      TIMESTAMPTZ

ExtractionFailed (v1):
  package_id      TEXT
  document_id     TEXT
  error_type      TEXT
  error_message   TEXT
  partial_facts   JSONB (nullable)
  failed_at       TIMESTAMPTZ

QualityAssessmentCompleted (v1):
  package_id                  TEXT
  document_id                 TEXT
  overall_confidence          FLOAT
  is_coherent                 BOOLEAN
  anomalies                   TEXT[]
  critical_missing_fields     TEXT[]
  reextraction_recommended    BOOLEAN
  auditor_notes               TEXT
  assessed_at                 TIMESTAMPTZ

PackageReadyForAnalysis (v1):
  package_id          TEXT
  application_id      TEXT
  documents_processed INTEGER
  has_quality_flags   BOOLEAN
  quality_flag_count  INTEGER
  ready_at            TIMESTAMPTZ

--- AGGREGATE 3: AgentSession (stream: agent-{agent_type}-{session_id}) ---

AgentSessionStarted (v1):
  session_id              TEXT
  agent_type              agent_type
  agent_id                TEXT
  application_id          TEXT
  model_version           TEXT
  langgraph_graph_version TEXT
  context_source          TEXT  -- "fresh" | "prior_session_replay:{session_id}"
  context_token_count     INTEGER
  started_at              TIMESTAMPTZ

AgentInputValidated (v1):
  session_id            TEXT
  agent_type            agent_type
  application_id        TEXT
  inputs_validated      TEXT[]
  validation_duration_ms INTEGER
  validated_at          TIMESTAMPTZ

AgentInputValidationFailed (v1):
  session_id          TEXT
  agent_type          agent_type
  application_id      TEXT
  missing_inputs      TEXT[]
  validation_errors   TEXT[]
  failed_at           TIMESTAMPTZ

AgentNodeExecuted (v1):
  session_id        TEXT
  agent_type        agent_type
  node_name         TEXT
  node_sequence     INTEGER
  input_keys        TEXT[]
  output_keys       TEXT[]
  llm_called        BOOLEAN
  llm_tokens_input  INTEGER (nullable)
  llm_tokens_output INTEGER (nullable)
  llm_cost_usd      FLOAT (nullable)
  duration_ms       INTEGER
  executed_at       TIMESTAMPTZ

AgentToolCalled (v1):
  session_id          TEXT
  agent_type          agent_type
  tool_name           TEXT
  tool_input_summary  TEXT
  tool_output_summary TEXT
  tool_duration_ms    INTEGER
  called_at           TIMESTAMPTZ

AgentOutputWritten (v1):
  session_id      TEXT
  agent_type      agent_type
  application_id  TEXT
  events_written  JSONB  -- [{"stream_id": TEXT, "event_type": TEXT}]
  output_summary  TEXT
  written_at      TIMESTAMPTZ

AgentSessionCompleted (v1):
  session_id            TEXT
  agent_type            agent_type
  application_id        TEXT
  total_nodes_executed  INTEGER
  total_llm_calls       INTEGER
  total_tokens_used     INTEGER
  total_cost_usd        FLOAT
  total_duration_ms     INTEGER
  next_agent_triggered  TEXT (nullable)
  completed_at          TIMESTAMPTZ

AgentSessionFailed (v1):
  session_id            TEXT
  agent_type            agent_type
  application_id        TEXT
  error_type            TEXT
  error_message         TEXT
  last_successful_node  TEXT (nullable)
  recoverable           BOOLEAN
  failed_at             TIMESTAMPTZ

AgentSessionRecovered (v1):
  session_id                TEXT
  agent_type                agent_type
  application_id            TEXT
  recovered_from_session_id TEXT
  recovery_point            TEXT
  recovered_at              TIMESTAMPTZ

--- AGGREGATE 4: CreditRecord (stream: credit-{application_id}) ---

CreditRecordOpened (v1):
  application_id  TEXT
  applicant_id    TEXT
  opened_at       TIMESTAMPTZ

HistoricalProfileConsumed (v1):
  application_id      TEXT
  session_id          TEXT
  fiscal_years_loaded INTEGER[]
  has_prior_loans     BOOLEAN
  has_defaults        BOOLEAN
  revenue_trajectory  TEXT
  data_hash           TEXT
  consumed_at         TIMESTAMPTZ

ExtractedFactsConsumed (v1):
  application_id        TEXT
  session_id            TEXT
  document_ids_consumed TEXT[]
  facts_summary         TEXT
  quality_flags_present BOOLEAN
  consumed_at           TIMESTAMPTZ

CreditAnalysisCompleted (v2):
  application_id      TEXT
  session_id          TEXT
  decision            JSONB  -- CreditDecision: {risk_tier, recommended_limit_usd, confidence, rationale, key_concerns[], data_quality_caveats[], policy_overrides_applied[]}
  model_version       TEXT
  model_deployment_id TEXT
  input_data_hash     TEXT
  analysis_duration_ms INTEGER
  regulatory_basis    TEXT[]
  completed_at        TIMESTAMPTZ

CreditAnalysisDeferred (v1):
  application_id  TEXT
  session_id      TEXT
  deferral_reason TEXT
  quality_issues  TEXT[]
  deferred_at     TIMESTAMPTZ

--- AGGREGATE 5: ComplianceRecord (stream: compliance-{application_id}) ---

ComplianceCheckInitiated (v1):
  application_id          TEXT
  session_id              TEXT
  regulation_set_version  TEXT
  rules_to_evaluate       TEXT[]
  initiated_at            TIMESTAMPTZ

ComplianceRulePassed (v1):
  application_id    TEXT
  session_id        TEXT
  rule_id           TEXT
  rule_name         TEXT
  rule_version      TEXT
  evidence_hash     TEXT
  evaluation_notes  TEXT
  evaluated_at      TIMESTAMPTZ

ComplianceRuleFailed (v1):
  application_id            TEXT
  session_id                TEXT
  rule_id                   TEXT
  rule_name                 TEXT
  rule_version              TEXT
  failure_reason            TEXT
  is_hard_block             BOOLEAN
  remediation_available     BOOLEAN
  remediation_description   TEXT (nullable)
  evidence_hash             TEXT
  evaluated_at              TIMESTAMPTZ

ComplianceRuleNoted (v1):
  application_id  TEXT
  session_id      TEXT
  rule_id         TEXT
  rule_name       TEXT
  note_type       TEXT
  note_text       TEXT
  evaluated_at    TIMESTAMPTZ

ComplianceCheckCompleted (v1):
  application_id  TEXT
  session_id      TEXT
  rules_evaluated INTEGER
  rules_passed    INTEGER
  rules_failed    INTEGER
  rules_noted     INTEGER
  has_hard_block  BOOLEAN
  overall_verdict compliance_verdict
  completed_at    TIMESTAMPTZ

--- AGGREGATE 6: FraudScreening (stream: fraud-{application_id}) ---

FraudScreeningInitiated (v1):
  application_id          TEXT
  session_id              TEXT
  screening_model_version TEXT
  initiated_at            TIMESTAMPTZ

FraudAnomalyDetected (v1):
  application_id  TEXT
  session_id      TEXT
  anomaly         JSONB  -- FraudAnomaly: {anomaly_type, description, severity, evidence, affected_fields[]}
  detected_at     TIMESTAMPTZ

FraudScreeningCompleted (v1):
  application_id          TEXT
  session_id              TEXT
  fraud_score             FLOAT
  risk_level              TEXT
  anomalies_found         INTEGER
  recommendation          TEXT
  screening_model_version TEXT
  input_data_hash         TEXT
  completed_at            TIMESTAMPTZ

--- AGGREGATE 7: AuditLedger (stream: audit-{entity_id}) ---

AuditIntegrityCheckRun (v1):
  entity_type           TEXT
  entity_id             TEXT
  check_timestamp       TIMESTAMPTZ
  events_verified_count INTEGER
  integrity_hash        TEXT
  previous_hash         TEXT (nullable)
  chain_valid           BOOLEAN
  tamper_detected       BOOLEAN
$$;
