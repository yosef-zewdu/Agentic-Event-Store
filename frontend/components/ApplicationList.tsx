"use client";

import Link from "next/link";
import Badge, { stateVariant } from "@/components/ui/Badge";
import type { ApplicationSummary } from "@/types";

interface ApplicationListProps {
  applications: ApplicationSummary[];
}

export default function ApplicationList({ applications }: ApplicationListProps) {
  if (applications.length === 0) {
    return (
      <div className="text-center py-12" style={{ color: "var(--text-muted)" }}>
        No applications yet.{" "}
        <Link href="/applications/new" style={{ color: "var(--accent)" }}>
          Submit one →
        </Link>
      </div>
    );
  }

  return (
    <div
      className="rounded-xl overflow-hidden"
      style={{ border: "1px solid var(--border)" }}
    >
      <table className="w-full text-sm">
        <thead>
          <tr style={{ background: "var(--surface)", borderBottom: "1px solid var(--border)" }}>
            <th className="text-left px-4 py-3 font-medium" style={{ color: "var(--text-muted)" }}>ID</th>
            <th className="text-left px-4 py-3 font-medium hidden sm:table-cell" style={{ color: "var(--text-muted)" }}>Applicant</th>
            <th className="text-right px-4 py-3 font-medium hidden md:table-cell" style={{ color: "var(--text-muted)" }}>Amount</th>
            <th className="text-left px-4 py-3 font-medium" style={{ color: "var(--text-muted)" }}>State</th>
            <th className="text-left px-4 py-3 font-medium hidden lg:table-cell" style={{ color: "var(--text-muted)" }}>Last Updated</th>
            <th className="px-4 py-3" />
          </tr>
        </thead>
        <tbody>
          {applications.map((app, idx) => (
            <tr
              key={app.application_id}
              style={{
                background: idx % 2 === 0 ? "var(--bg)" : "var(--surface)",
                borderBottom: "1px solid var(--border)",
              }}
            >
              <td className="px-4 py-3 font-mono text-xs" style={{ color: "var(--accent)" }}>
                {app.application_id}
              </td>
              <td className="px-4 py-3 hidden sm:table-cell" style={{ color: "var(--text)" }}>
                {app.applicant_id ?? "—"}
              </td>
              <td className="px-4 py-3 text-right hidden md:table-cell" style={{ color: "var(--text)" }}>
                {app.requested_amount_usd != null
                  ? `$${app.requested_amount_usd.toLocaleString()}`
                  : "—"}
              </td>
              <td className="px-4 py-3">
                <Badge variant={stateVariant(app.state)}>{app.state ?? "—"}</Badge>
              </td>
              <td className="px-4 py-3 text-xs hidden lg:table-cell" style={{ color: "var(--text-muted)" }}>
                {app.last_event_at
                  ? new Date(app.last_event_at).toLocaleString()
                  : "—"}
              </td>
              <td className="px-4 py-3 text-right">
                <Link
                  href={`/applications/${app.application_id}`}
                  className="text-xs px-3 py-1.5 rounded"
                  style={{
                    background: "rgba(108,99,255,0.15)",
                    color: "var(--accent)",
                    textDecoration: "none",
                  }}
                >
                  View →
                </Link>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
