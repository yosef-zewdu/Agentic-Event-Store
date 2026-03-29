import { getReviewQueue } from "@/lib/api";
import ReviewQueue from "@/components/ReviewQueue";
import type { ReviewQueueItem } from "@/types";

export const dynamic = "force-dynamic";

export default async function ReviewQueuePage() {
  let items: ReviewQueueItem[] = [];
  let error: string | null = null;

  try {
    items = await getReviewQueue();
  } catch (e: unknown) {
    error = String(e);
  }

  return (
    <div className="max-w-3xl mx-auto space-y-6">
      <h1 className="text-2xl font-bold" style={{ color: "var(--text)" }}>
        Review Queue
      </h1>
      {error && (
        <div
          className="rounded p-4 text-sm"
          style={{ background: "rgba(239,68,68,0.1)", border: "1px solid rgba(239,68,68,0.3)", color: "#ef4444" }}
        >
          {error}
        </div>
      )}
      <ReviewQueue items={items} />
    </div>
  );
}
