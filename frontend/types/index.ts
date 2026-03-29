// frontend/types/index.ts — TypeScript interfaces matching backend response shapes

export interface ApplicationSummary {
  application_id: string;
  state: string;
  applicant_id: string | null;
  requested_amount_usd: number | null;
  approved_amount_usd: number | null;
  risk_tier: string | null;
  fraud_score: number | null;
  compliance_status: string | null;
  decision: string | null;
  agent_sessions_completed: string[];
  last_event_type: string | null;
  last_event_at: string | null;
  human_reviewer_id: string | null;
  final_decision_at: string | null;
}

export interface PipelineJob {
  job_id: string;
  status: "queued" | "running" | "completed" | "failed";
  error_message: string | null;
  created_at: string | null;
  started_at: string | null;
  completed_at: string | null;
}

export interface ApplicationStatus {
  application_id: string;
  state: string;
  last_event_type: string | null;
  last_event_at: string | null;
  pipeline_job: PipelineJob | null;
}

export interface SubmitApplicationResponse {
  application_id: string;
  job_id: string;
  status: string;
}

export interface ComplianceRecord {
  event_id: string;
  application_id: string;
  event_type: string;
  rule_id: string | null;
  rule_version: string | null;
  verdict: string | null;
  evaluation_timestamp: string | null;
  evidence_hash: string | null;
  regulation_set_version: string | null;
  session_id: string | null;
  recorded_at: string | null;
}

export interface AgentSession {
  session_id: string;
  application_id: string | null;
  agent_type: string | null;
  agent_id: string | null;
  total_llm_calls: number | null;
  total_tokens_used: number | null;
  total_cost_usd: number | null;
  total_duration_ms: number | null;
  last_event_at: string | null;
}

export interface AnalysisSummary {
  application_id: string;
  risk_tier: string | null;
  fraud_score: number | null;
  compliance_verdict: string | null;
  recommendation: string | null;
  confidence_score: number | null;
  contributing_sessions: string[];
  state: string;
}

export interface ReviewQueueItem {
  application_id: string;
  state: string;
  applicant_id: string | null;
  requested_amount_usd: number | null;
  risk_tier: string | null;
  fraud_score: number | null;
  compliance_status: string | null;
  decision: string | null;
  last_event_at: string | null;
}

export interface ReviewContextResponse {
  application: ApplicationSummary;
  compliance_records: ComplianceRecord[];
  agent_sessions: AgentSession[];
  decision_events: Array<{
    event_type: string;
    recorded_at: string | null;
    payload: Record<string, unknown>;
  }>;
}

export interface DocumentRecord {
  filename: string;
  document_type: string;
  size_bytes: number;
}
