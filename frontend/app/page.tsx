import Link from "next/link";
import { listApplications, getReviewQueue } from "@/lib/api";
import ApplicationList from "@/components/ApplicationList";
import ReviewQueue from "@/components/ReviewQueue";
import type { ApplicationSummary, ReviewQueueItem } from "@/types";

// Re-fetch on every request (no caching for live dashboard)
export const dynamic = "force-dynamic";

const APPROVED_STATES = new Set(["APPROVED", "FinalApproved"]);
const DECLINED_STATES = new Set(["DECLINED", "DECLINED_COMPLIANCE", "FinalDeclined"]);

function isToday(isoString: string | null): boolean {
  if (!isoString) return false;
  const d = new Date(isoString);
  const now = new Date();
  return d.getFullYear() === now.getFullYear() &&
    d.getMonth() === now.getMonth() &&
    d.getDate() === now.getDate();
}

export default async function DashboardPage() {
  let applications: ApplicationSummary[] = [];
  let reviewQueue: ReviewQueueItem[] = [];
  let fetchError: string | null = null;

  try {
    [applications, reviewQueue] = await Promise.all([
      listApplications(),
      getReviewQueue(),
    ]);
  } catch (e: unknown) {
    fetchError = String(e);
  }

  const total = applications.length;
  const pendingReview = reviewQueue.length;
  const approvedToday = applications.filter(
    (a) => APPROVED_STATES.has(a.state ?? "") && isToday(a.final_decision_at)
  ).length;
  const declinedToday = applications.filter(
    (a) => DECLINED_STATES.has(a.state ?? "") && isToday(a.final_decision_at)
  ).length;

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold" style={{ color: "var(--text)" }}>
          Dashboard
        </h1>
        <Link
          href="/applications/new"
          className="px-4 py-2 rounded font-medium text-sm"
          style={{ background: "var(--accent)", color: "#fff", textDecoration: "none" }}
        >
          + New Application
        </Link>
      </div>

      {fetchError && (
        <div
          className="rounded p-4 text-sm"
          style={{ background: "rgba(239,68,68,0.1)", border: "1px solid rgba(239,68,68,0.3)", color: "#ef4444" }}
        >
          Could not connect to backend: {fetchError}
        </div>
      )}

      {/* Stats */}
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        {[
          { label: "Total Applications", value: total, color: "var(--accent)" },
          { label: "Pending Review",     value: pendingReview, color: "var(--warning)" },
          { label: "Approved Today",     value: approvedToday, color: "var(--success)" },
          { label: "Declined Today",     value: declinedToday, color: "var(--danger)" },
        ].map(({ label, value, color }) => (
          <div
            key={label}
            className="rounded-xl p-5"
            style={{ background: "var(--surface)", border: "1px solid var(--border)" }}
          >
            <p className="text-xs mb-1" style={{ color: "var(--text-muted)" }}>{label}</p>
            <p className="text-3xl font-bold" style={{ color }}>{value}</p>
          </div>
        ))}
      </div>

      {/* Review queue */}
      {reviewQueue.length > 0 && (
        <section>
          <h2 className="text-lg font-semibold mb-3" style={{ color: "var(--text)" }}>
            Awaiting Review ({reviewQueue.length})
          </h2>
          <ReviewQueue items={reviewQueue} />
        </section>
      )}

      {/* All applications */}
      <section>
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-lg font-semibold" style={{ color: "var(--text)" }}>
            All Applications
          </h2>
          <Link href="/review" className="text-sm" style={{ color: "var(--accent)", textDecoration: "none" }}>
            Review Queue →
          </Link>
        </div>
        <ApplicationList applications={applications} />
      </section>
    </div>
  );
}
