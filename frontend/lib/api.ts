// frontend/lib/api.ts — Typed fetch wrapper for the Ledger backend API

import type {
  AgentSession,
  AnalysisSummary,
  ApplicationStatus,
  ApplicationSummary,
  ComplianceRecord,
  DocumentRecord,
  PipelineJob,
  ReviewContextResponse,
  ReviewQueueItem,
  SubmitApplicationResponse,
} from "@/types";

const API_URL =
  process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

async function request<T>(
  path: string,
  options?: RequestInit
): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return res.json() as Promise<T>;
}

// ---------------------------------------------------------------------------
// Application lifecycle
// ---------------------------------------------------------------------------

export async function submitApplication(data: {
  application_id?: string;
  applicant_id: string;
  requested_amount_usd: number;
  loan_purpose?: string;
  loan_term_months?: number;
  contact_email?: string;
  contact_name?: string;
}): Promise<SubmitApplicationResponse> {
  return request("/api/applications", {
    method: "POST",
    body: JSON.stringify(data),
  });
}

export async function listApplications(
  status?: string
): Promise<ApplicationSummary[]> {
  const qs = status ? `?status=${encodeURIComponent(status)}` : "";
  return request(`/api/applications${qs}`);
}

export async function getApplication(id: string): Promise<ApplicationSummary> {
  return request(`/api/applications/${id}`);
}

export async function pollStatus(id: string): Promise<ApplicationStatus> {
  return request(`/api/applications/${id}/status`);
}

export async function getAnalysis(id: string): Promise<AnalysisSummary> {
  return request(`/api/applications/${id}/analysis`);
}

export async function getCompliance(
  id: string,
  asOf?: string
): Promise<ComplianceRecord[]> {
  const qs = asOf ? `?as_of=${encodeURIComponent(asOf)}` : "";
  return request(`/api/applications/${id}/compliance${qs}`);
}

export async function getAgents(id: string): Promise<AgentSession[]> {
  return request(`/api/applications/${id}/agents`);
}

// ---------------------------------------------------------------------------
// Documents
// ---------------------------------------------------------------------------

export async function uploadDocument(
  applicationId: string,
  file: File,
  documentType: string
): Promise<DocumentRecord & { application_id: string; applicant_id: string }> {
  const form = new FormData();
  form.append("file", file);
  form.append("document_type", documentType);

  const res = await fetch(`${API_URL}/api/applications/${applicationId}/documents`, {
    method: "POST",
    body: form,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`${res.status}: ${text}`);
  }
  return res.json();
}

export async function listDocuments(id: string): Promise<DocumentRecord[]> {
  return request(`/api/applications/${id}/documents`);
}

// ---------------------------------------------------------------------------
// Human review
// ---------------------------------------------------------------------------

export async function getReviewQueue(): Promise<ReviewQueueItem[]> {
  return request("/api/review/queue");
}

export async function getReviewContext(id: string): Promise<ReviewContextResponse> {
  return request(`/api/applications/${id}/review-context`);
}

export async function submitReview(
  id: string,
  data: {
    reviewer_id: string;
    decision: "APPROVE" | "DECLINE";
    override?: boolean;
    override_reason?: string;
  }
): Promise<ApplicationSummary> {
  return request(`/api/applications/${id}/review`, {
    method: "POST",
    body: JSON.stringify(data),
  });
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

export async function retryPipeline(
  id: string
): Promise<{ application_id: string; job_id: string; status: string }> {
  return request(`/api/applications/${id}/pipeline/retry`, { method: "POST" });
}

export async function getPipelineStatus(id: string): Promise<PipelineJob> {
  return request(`/api/applications/${id}/pipeline/status`);
}
