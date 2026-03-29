"use client";

import { useEffect, useState } from "react";
import { getReviewContext, submitReview } from "@/lib/api";
import Badge, { stateVariant } from "@/components/ui/Badge";
import LoadingSpinner from "@/components/ui/LoadingSpinner";
import ErrorBox from "@/components/ui/ErrorBox";
import type { ReviewContextResponse } from "@/types";

interface ReviewPanelProps {
  applicationId: string;
  onReviewed: () => void;
}

export default function ReviewPanel({ applicationId, onReviewed }: ReviewPanelProps) {
  const [ctx, setCtx] = useState<ReviewContextResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Review form state
  const [reviewerId, setReviewerId] = useState("");
  const [pendingDecision, setPendingDecision] = useState<"APPROVE" | "DECLINE" | null>(null);
  const [confirming, setConfirming] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);

  useEffect(() => {
    getReviewContext(applicationId)
      .then(setCtx)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [applicationId]);

  async function handleConfirm() {
    if (!pendingDecision) return;
    setSubmitting(true);
    setSubmitError(null);
    try {
      await submitReview(applicationId, {
        reviewer_id: reviewerId || "human-reviewer",
        decision: pendingDecision,
      });
      onReviewed();
    } catch (e: unknown) {
      setSubmitError(String(e));
    } finally {
      setSubmitting(false);
      setConfirming(false);
    }
  }

  const cardStyle = {
    background: "var(--surface)",
    border: "1px solid var(--border)",
    borderRadius: 12,
    padding: 24,
  };

  if (loading) return (
    <div style={cardStyle} className="flex items-center gap-3">
      <LoadingSpinner />
      <span style={{ color: "var(--text-muted)" }}>Loading review context…</span>
    </div>
  );
  if (error) return <div style={cardStyle}><ErrorBox message={error} /></div>;
  if (!ctx) return null;

  const app = ctx.application;
  const decisionEvent = ctx.decision_events.find((e) => e.event_type === "DecisionGenerated") ?? null;

  return (
    <div style={cardStyle}>
      <div className="flex items-center justify-between mb-5">
        <h2 className="font-semibold" style={{ color: "var(--text)" }}>Human Review Required</h2>
        <Badge variant="yellow">Pending Review</Badge>
      </div>

      {/* Key metrics */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-6 p-4 rounded-lg" style={{ background: "var(--bg)" }}>
        <ReviewStat label="Risk Tier" value={<Badge variant={stateVariant(app.risk_tier)}>{app.risk_tier ?? "—"}</Badge>} />
        <ReviewStat label="Fraud Score" value={app.fraud_score != null ? `${(app.fraud_score * 100).toFixed(0)}%` : "—"} />
        <ReviewStat label="Compliance" value={<Badge variant={stateVariant(app.compliance_status)}>{app.compliance_status ?? "—"}</Badge>} />
        <ReviewStat
          label="Agent Rec."
          value={decisionEvent
            ? <Badge variant={stateVariant(String(decisionEvent.payload.recommendation))}>{String(decisionEvent.payload.recommendation)}</Badge>
            : <Badge variant={stateVariant(app.decision)}>{app.decision ?? "—"}</Badge>
          }
        />
      </div>

      {/* Agent rationale (from DecisionGenerated) */}
      {decisionEvent?.payload.executive_summary != null && (
        <div className="mb-5 p-4 rounded-lg text-sm" style={{ background: "var(--bg)", color: "var(--text-muted)" }}>
          <p className="text-xs font-medium mb-1" style={{ color: "var(--text)" }}>Agent Rationale</p>
          <p>{String(decisionEvent.payload.executive_summary)}</p>
        </div>
      )}

      {/* Compliance records summary */}
      {ctx.compliance_records.length > 0 && (
        <div className="mb-5">
          <p className="text-xs font-medium mb-2" style={{ color: "var(--text-muted)" }}>
            Compliance Records ({ctx.compliance_records.length})
          </p>
          <div className="space-y-1 max-h-32 overflow-y-auto">
            {ctx.compliance_records.slice(0, 8).map((r, i) => (
              <div key={i} className="flex items-center gap-2 text-xs">
                <Badge variant={stateVariant(r.verdict ?? r.event_type)}>
                  {r.verdict ?? r.event_type}
                </Badge>
                <span style={{ color: "var(--text-muted)" }}>{r.rule_id ?? "—"}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Reviewer ID */}
      <div className="mb-5">
        <label className="block text-xs mb-1" style={{ color: "var(--text-muted)" }}>Reviewer ID</label>
        <input
          value={reviewerId}
          onChange={(e) => setReviewerId(e.target.value)}
          placeholder="your-reviewer-id"
          className="w-full rounded px-3 py-2 text-sm"
          style={{ background: "var(--bg)", border: "1px solid var(--border)", color: "var(--text)" }}
        />
      </div>

      {submitError && <div className="mb-4"><ErrorBox message={submitError} /></div>}

      {/* Action buttons */}
      {!confirming ? (
        <div className="flex gap-3">
          <button
            onClick={() => { setPendingDecision("APPROVE"); setConfirming(true); }}
            className="flex-1 py-2.5 rounded font-medium text-sm"
            style={{ background: "var(--success)", color: "#fff", border: "none", cursor: "pointer" }}
          >
            Approve
          </button>
          <button
            onClick={() => { setPendingDecision("DECLINE"); setConfirming(true); }}
            className="flex-1 py-2.5 rounded font-medium text-sm"
            style={{ background: "var(--danger)", color: "#fff", border: "none", cursor: "pointer" }}
          >
            Decline
          </button>
        </div>
      ) : (
        <div className="rounded-lg p-4" style={{ background: "var(--bg)", border: "1px solid var(--border)" }}>
          <p className="text-sm mb-4" style={{ color: "var(--text)" }}>
            Confirm{" "}
            <strong style={{ color: pendingDecision === "APPROVE" ? "var(--success)" : "var(--danger)" }}>
              {pendingDecision}
            </strong>{" "}
            for application <code style={{ color: "var(--accent)" }}>{applicationId}</code>?
          </p>
          <div className="flex gap-2">
            <button
              onClick={handleConfirm}
              disabled={submitting}
              className="px-5 py-2 rounded text-sm font-medium"
              style={{
                background: pendingDecision === "APPROVE" ? "var(--success)" : "var(--danger)",
                color: "#fff",
                border: "none",
                cursor: submitting ? "not-allowed" : "pointer",
                opacity: submitting ? 0.6 : 1,
              }}
            >
              {submitting ? "Submitting…" : "Confirm"}
            </button>
            <button
              onClick={() => { setConfirming(false); setPendingDecision(null); }}
              className="px-5 py-2 rounded text-sm"
              style={{ background: "transparent", border: "1px solid var(--border)", color: "var(--text-muted)", cursor: "pointer" }}
            >
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function ReviewStat({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div>
      <p className="text-xs mb-1" style={{ color: "var(--text-muted)" }}>{label}</p>
      <div className="text-sm font-medium" style={{ color: "var(--text)" }}>{value}</div>
    </div>
  );
}
