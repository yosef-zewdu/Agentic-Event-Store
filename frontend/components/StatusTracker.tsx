"use client";

import Badge, { stateVariant } from "@/components/ui/Badge";
import type { ApplicationStatus } from "@/types";

const STAGES = [
  { label: "Document Processing", states: ["DOCUMENTS_PENDING", "DOCUMENTS_UPLOADED", "DOCUMENTS_PROCESSED", "Submitted"] },
  { label: "Credit Analysis",     states: ["CREDIT_ANALYSIS_REQUESTED", "CREDIT_ANALYSIS_COMPLETE", "AwaitingAnalysis", "AnalysisComplete"] },
  { label: "Fraud Detection",     states: ["FRAUD_SCREENING_REQUESTED", "FRAUD_SCREENING_COMPLETE"] },
  { label: "Compliance Check",    states: ["COMPLIANCE_CHECK_REQUESTED", "COMPLIANCE_CHECK_COMPLETE"] },
  { label: "Decision",            states: ["PENDING_DECISION", "PENDING_HUMAN_REVIEW", "APPROVED", "DECLINED", "DECLINED_COMPLIANCE", "PendingDecision", "ApprovedPendingHuman", "DeclinedPendingHuman", "FinalApproved", "FinalDeclined"] },
];

const TERMINAL_STATES = new Set(["APPROVED", "DECLINED", "DECLINED_COMPLIANCE", "FinalApproved", "FinalDeclined", "Withdrawn"]);
const FINAL_STATES: Record<string, string> = {
  APPROVED: "APPROVED", FinalApproved: "APPROVED",
  DECLINED: "DECLINED", FinalDeclined: "DECLINED",
  DECLINED_COMPLIANCE: "DECLINED (Compliance)",
};

function stageStatus(state: string, stageIdx: number): "done" | "active" | "idle" {
  const stageStates = STAGES[stageIdx].states;
  if (stageStates.some((s) => s === state)) return "active";

  // Find which stage the current state belongs to
  const currentStageIdx = STAGES.findIndex((s) => s.states.includes(state));
  if (currentStageIdx > stageIdx) return "done";
  if (TERMINAL_STATES.has(state) && stageIdx <= STAGES.length - 1) return "done";
  return "idle";
}

interface StatusTrackerProps {
  status: ApplicationStatus;
  pipelineFailed?: boolean;
}

export default function StatusTracker({ status, pipelineFailed }: StatusTrackerProps) {
  const state = status.state ?? "";
  const isFinal = TERMINAL_STATES.has(state);
  const finalLabel = FINAL_STATES[state];

  return (
    <div
      className="rounded-xl p-6"
      style={{ background: "var(--surface)", border: "1px solid var(--border)" }}
    >
      <div className="flex items-center justify-between mb-6">
        <h2 className="font-semibold" style={{ color: "var(--text)" }}>
          Pipeline Progress
        </h2>
        <div className="flex items-center gap-2">
          {isFinal && finalLabel && (
            <Badge variant={stateVariant(state)}>
              {finalLabel}
            </Badge>
          )}
          {pipelineFailed && !isFinal && (
            <Badge variant="red">Pipeline Failed</Badge>
          )}
          {!isFinal && !pipelineFailed && (
            <Badge variant="blue">Running</Badge>
          )}
        </div>
      </div>

      <div className="space-y-3">
        {STAGES.map((stage, idx) => {
          const s = stageStatus(state, idx);
          return (
            <div key={stage.label} className="flex items-center gap-3">
              {/* Icon */}
              <div
                className="w-8 h-8 rounded-full flex items-center justify-center text-sm flex-shrink-0"
                style={{
                  background:
                    s === "done"
                      ? "rgba(34,197,94,0.2)"
                      : s === "active"
                      ? "rgba(108,99,255,0.2)"
                      : "var(--border)",
                  color:
                    s === "done"
                      ? "var(--success)"
                      : s === "active"
                      ? "var(--accent)"
                      : "var(--text-muted)",
                }}
              >
                {s === "done" ? "✓" : s === "active" ? "●" : idx + 1}
              </div>

              {/* Label + progress bar */}
              <div className="flex-1">
                <div className="flex items-center justify-between mb-1">
                  <span
                    className="text-sm"
                    style={{
                      color: s === "idle" ? "var(--text-muted)" : "var(--text)",
                      fontWeight: s === "active" ? 600 : 400,
                    }}
                  >
                    {stage.label}
                  </span>
                  <span className="text-xs" style={{ color: "var(--text-muted)" }}>
                    {s === "done" ? "Complete" : s === "active" ? "In Progress" : ""}
                  </span>
                </div>
                <div
                  className="h-1 rounded-full"
                  style={{ background: "var(--border)" }}
                >
                  <div
                    className="h-1 rounded-full transition-all"
                    style={{
                      width: s === "done" ? "100%" : s === "active" ? "55%" : "0%",
                      background:
                        s === "done"
                          ? "var(--success)"
                          : s === "active"
                          ? "var(--accent)"
                          : "transparent",
                    }}
                  />
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {status.last_event_type && (
        <p className="mt-4 text-xs" style={{ color: "var(--text-muted)" }}>
          Last event: <span style={{ color: "var(--accent)" }}>{status.last_event_type}</span>
          {status.last_event_at && (
            <> at {new Date(status.last_event_at).toLocaleTimeString()}</>
          )}
        </p>
      )}
    </div>
  );
}
