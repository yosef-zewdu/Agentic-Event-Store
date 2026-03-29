"use client";

import { use, useCallback, useEffect, useRef, useState } from "react";
import { pollStatus, retryPipeline } from "@/lib/api";
import StatusTracker from "@/components/StatusTracker";
import AnalysisPanel from "@/components/AnalysisPanel";
import ReviewPanel from "@/components/ReviewPanel";
import LoadingSpinner from "@/components/ui/LoadingSpinner";
import ErrorBox from "@/components/ui/ErrorBox";
import type { ApplicationStatus } from "@/types";
import Link from "next/link";

const POLL_INTERVAL_MS = 3000;

const TERMINAL_PIPELINE_STATES = new Set([
  "APPROVED", "DECLINED", "DECLINED_COMPLIANCE", "Withdrawn",
  "FinalApproved", "FinalDeclined",
]);

const PENDING_REVIEW_STATES = new Set([
  "PENDING_HUMAN_REVIEW", "ApprovedPendingHuman", "DeclinedPendingHuman",
]);

const ANALYSIS_VISIBLE_STATES = new Set([
  "PENDING_DECISION", "PENDING_HUMAN_REVIEW", "APPROVED", "DECLINED",
  "DECLINED_COMPLIANCE", "PendingDecision", "ApprovedPendingHuman",
  "DeclinedPendingHuman", "FinalApproved", "FinalDeclined",
]);

export default function ApplicationDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = use(params);

  const [status, setStatus] = useState<ApplicationStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [retrying, setRetrying] = useState(false);
  const pollingRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const fetch = useCallback(async () => {
    try {
      const s = await pollStatus(id);
      setStatus(s);
      setError(null);
      return s;
    } catch (e: unknown) {
      setError(String(e));
      return null;
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    let cancelled = false;

    async function tick() {
      const s = await fetch();
      if (cancelled) return;

      const pipelineDone =
        s?.pipeline_job == null ||
        s.pipeline_job.status === "completed" ||
        s.pipeline_job.status === "failed";
      const stateDone = s ? TERMINAL_PIPELINE_STATES.has(s.state ?? "") : false;
      const pendingReview = s ? PENDING_REVIEW_STATES.has(s.state ?? "") : false;

      if (!pipelineDone || (!stateDone && !pendingReview)) {
        pollingRef.current = setTimeout(tick, POLL_INTERVAL_MS);
      }
    }

    tick();
    return () => {
      cancelled = true;
      if (pollingRef.current) clearTimeout(pollingRef.current);
    };
  }, [fetch]);

  async function handleRetry() {
    setRetrying(true);
    try {
      await retryPipeline(id);
      // Resume polling
      const s = await fetch();
      if (s && !TERMINAL_PIPELINE_STATES.has(s.state ?? "")) {
        pollingRef.current = setTimeout(async function tick() {
          const latest = await fetch();
          if (!latest) return;
          const done = TERMINAL_PIPELINE_STATES.has(latest.state ?? "") ||
            latest.pipeline_job?.status === "completed";
          if (!done) pollingRef.current = setTimeout(tick, POLL_INTERVAL_MS);
        }, POLL_INTERVAL_MS);
      }
    } catch (e: unknown) {
      setError(String(e));
    } finally {
      setRetrying(false);
    }
  }

  async function handleReviewed() {
    // Fetch once immediately after review completes
    await fetch();
  }

  if (loading) return (
    <div className="flex items-center gap-3 mt-12 justify-center">
      <LoadingSpinner size={28} />
      <span style={{ color: "var(--text-muted)" }}>Loading application…</span>
    </div>
  );

  if (error && !status) return (
    <div className="max-w-xl mx-auto mt-12">
      <ErrorBox message={error} />
      <Link href="/" className="mt-4 inline-block text-sm" style={{ color: "var(--accent)" }}>
        ← Dashboard
      </Link>
    </div>
  );

  if (!status) return null;

  const isPendingReview = PENDING_REVIEW_STATES.has(status.state ?? "");
  const showAnalysis = ANALYSIS_VISIBLE_STATES.has(status.state ?? "");
  const pipelineFailed = status.pipeline_job?.status === "failed";

  return (
    <div className="max-w-3xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <Link href="/" className="text-sm" style={{ color: "var(--text-muted)", textDecoration: "none" }}>
            ← Dashboard
          </Link>
          <h1 className="text-2xl font-bold mt-1" style={{ color: "var(--text)" }}>
            {id}
          </h1>
        </div>
        {pipelineFailed && (
          <button
            onClick={handleRetry}
            disabled={retrying}
            className="px-4 py-2 rounded text-sm font-medium"
            style={{
              background: "var(--accent)",
              color: "#fff",
              border: "none",
              cursor: retrying ? "not-allowed" : "pointer",
              opacity: retrying ? 0.6 : 1,
            }}
          >
            {retrying ? "Retrying…" : "Retry Pipeline"}
          </button>
        )}
      </div>

      {/* Pipeline status tracker */}
      <StatusTracker status={status} pipelineFailed={pipelineFailed} />

      {/* Pipeline error detail */}
      {pipelineFailed && status.pipeline_job?.error_message && (
        <ErrorBox message={`Pipeline error: ${status.pipeline_job.error_message.split("\n")[0]}`} />
      )}

      {/* Analysis panel — shown once enough data is available */}
      {showAnalysis && <AnalysisPanel applicationId={id} />}

      {/* Human review panel */}
      {isPendingReview && (
        <ReviewPanel applicationId={id} onReviewed={handleReviewed} />
      )}
    </div>
  );
}
