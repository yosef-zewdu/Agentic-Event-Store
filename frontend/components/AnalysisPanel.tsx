"use client";

import { useEffect, useState } from "react";
import { getAnalysis } from "@/lib/api";
import Badge, { stateVariant } from "@/components/ui/Badge";
import LoadingSpinner from "@/components/ui/LoadingSpinner";
import type { AnalysisSummary } from "@/types";

interface AnalysisPanelProps {
  applicationId: string;
}

export default function AnalysisPanel({ applicationId }: AnalysisPanelProps) {
  const [analysis, setAnalysis] = useState<AnalysisSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    getAnalysis(applicationId)
      .then(setAnalysis)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [applicationId]);

  const cardStyle = {
    background: "var(--surface)",
    border: "1px solid var(--border)",
    borderRadius: 12,
    padding: 24,
  };

  if (loading) return (
    <div style={cardStyle} className="flex items-center gap-3">
      <LoadingSpinner /> <span style={{ color: "var(--text-muted)" }}>Loading analysis…</span>
    </div>
  );
  if (error) return null; // silently skip if not available yet
  if (!analysis) return null;

  const fraudPct = analysis.fraud_score != null ? Math.round(analysis.fraud_score * 100) : null;

  return (
    <div style={cardStyle}>
      <h2 className="font-semibold mb-4" style={{ color: "var(--text)" }}>Analysis Summary</h2>

      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4 mb-6">
        <Stat
          label="Risk Tier"
          value={analysis.risk_tier ? <Badge variant={stateVariant(analysis.risk_tier)}>{analysis.risk_tier}</Badge> : "—"}
        />
        <Stat
          label="Compliance"
          value={analysis.compliance_verdict ? <Badge variant={stateVariant(analysis.compliance_verdict)}>{analysis.compliance_verdict}</Badge> : "—"}
        />
        <Stat
          label="Recommendation"
          value={analysis.recommendation ? <Badge variant={stateVariant(analysis.recommendation)}>{analysis.recommendation}</Badge> : "—"}
        />
        <Stat
          label="Confidence"
          value={analysis.confidence_score != null ? `${(analysis.confidence_score * 100).toFixed(0)}%` : "—"}
        />
      </div>

      {/* Fraud score bar */}
      {fraudPct != null && (
        <div className="mb-4">
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs" style={{ color: "var(--text-muted)" }}>Fraud Score</span>
            <span className="text-xs font-medium" style={{ color: fraudPct > 70 ? "var(--danger)" : fraudPct > 30 ? "var(--warning)" : "var(--success)" }}>
              {fraudPct}%
            </span>
          </div>
          <div className="h-2 rounded-full" style={{ background: "var(--border)" }}>
            <div
              className="h-2 rounded-full transition-all"
              style={{
                width: `${fraudPct}%`,
                background: fraudPct > 70 ? "var(--danger)" : fraudPct > 30 ? "var(--warning)" : "var(--success)",
              }}
            />
          </div>
        </div>
      )}

      {/* Contributing sessions */}
      {analysis.contributing_sessions.length > 0 && (
        <div>
          <p className="text-xs mb-2" style={{ color: "var(--text-muted)" }}>Contributing Agent Sessions</p>
          <div className="flex flex-wrap gap-1">
            {analysis.contributing_sessions.map((s) => (
              <span
                key={s}
                className="text-xs px-2 py-0.5 rounded font-mono"
                style={{ background: "rgba(108,99,255,0.12)", color: "var(--accent)" }}
              >
                {s}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div>
      <p className="text-xs mb-1" style={{ color: "var(--text-muted)" }}>{label}</p>
      <div className="font-medium text-sm" style={{ color: "var(--text)" }}>{value}</div>
    </div>
  );
}
