"use client";

import Link from "next/link";
import Badge, { stateVariant } from "@/components/ui/Badge";
import type { ReviewQueueItem } from "@/types";

interface ReviewQueueProps {
  items: ReviewQueueItem[];
}

export default function ReviewQueue({ items }: ReviewQueueProps) {
  if (items.length === 0) {
    return (
      <p className="text-sm py-4" style={{ color: "var(--text-muted)" }}>
        No applications pending review.
      </p>
    );
  }

  return (
    <div className="space-y-3">
      {items.map((item) => (
        <div
          key={item.application_id}
          className="rounded-xl p-4 flex items-center justify-between gap-4"
          style={{ background: "var(--bg)", border: "1px solid var(--border)" }}
        >
          <div className="min-w-0">
            <div className="flex items-center gap-2 mb-1 flex-wrap">
              <span className="font-mono text-sm" style={{ color: "var(--accent)" }}>
                {item.application_id}
              </span>
              <span className="text-xs" style={{ color: "var(--text-muted)" }}>
                {item.applicant_id ?? ""}
              </span>
            </div>
            <div className="flex items-center gap-2 flex-wrap">
              {item.requested_amount_usd != null && (
                <span className="text-sm font-medium" style={{ color: "var(--text)" }}>
                  ${item.requested_amount_usd.toLocaleString()}
                </span>
              )}
              {item.risk_tier && (
                <Badge variant={stateVariant(item.risk_tier)}>{item.risk_tier}</Badge>
              )}
              {item.fraud_score != null && (
                <span className="text-xs px-2 py-0.5 rounded" style={{ background: "var(--surface)", color: "var(--text-muted)" }}>
                  Fraud {Math.round(item.fraud_score * 100)}%
                </span>
              )}
              {item.compliance_status && (
                <Badge variant={stateVariant(item.compliance_status)}>
                  {item.compliance_status}
                </Badge>
              )}
            </div>
          </div>

          <Link
            href={`/applications/${item.application_id}`}
            className="flex-shrink-0 px-4 py-2 rounded text-sm font-medium"
            style={{
              background: "var(--accent)",
              color: "#fff",
              textDecoration: "none",
            }}
          >
            Review →
          </Link>
        </div>
      ))}
    </div>
  );
}
