"use client";

import { useState } from "react";
import { submitApplication } from "@/lib/api";

const LOAN_PURPOSES = [
  "working_capital",
  "equipment_financing",
  "real_estate",
  "expansion",
  "refinancing",
  "acquisition",
  "bridge",
];

interface ApplicationFormProps {
  onSubmitted: (applicationId: string) => void;
}

export default function ApplicationForm({ onSubmitted }: ApplicationFormProps) {
  const [form, setForm] = useState({
    applicant_id: "",
    requested_amount_usd: "",
    loan_purpose: "working_capital",
    loan_term_months: "12",
    contact_email: "",
    contact_name: "",
  });
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  function set(field: string, value: string) {
    setForm((prev) => ({ ...prev, [field]: value }));
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    const amount = parseFloat(form.requested_amount_usd);
    if (!form.applicant_id.trim()) { setError("Applicant ID is required"); return; }
    if (isNaN(amount) || amount <= 0) { setError("Requested amount must be greater than 0"); return; }

    setSubmitting(true);
    try {
      const result = await submitApplication({
        applicant_id: form.applicant_id.trim(),
        requested_amount_usd: amount,
        loan_purpose: form.loan_purpose,
        loan_term_months: parseInt(form.loan_term_months, 10),
        contact_email: form.contact_email,
        contact_name: form.contact_name,
      });
      onSubmitted(result.application_id);
    } catch (err: unknown) {
      setError(String(err));
    } finally {
      setSubmitting(false);
    }
  }

  const inputStyle = {
    background: "var(--bg)",
    border: "1px solid var(--border)",
    color: "var(--text)",
    borderRadius: 6,
    padding: "8px 12px",
    width: "100%",
    fontSize: 14,
    outline: "none",
  };

  const labelStyle = {
    display: "block",
    fontSize: 13,
    fontWeight: 500,
    color: "var(--text-muted)",
    marginBottom: 4,
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-5">
      <div className="grid grid-cols-1 gap-5 sm:grid-cols-2">
        <div>
          <label style={labelStyle}>Applicant ID <span style={{ color: "var(--danger)" }}>*</span></label>
          <input
            style={inputStyle}
            value={form.applicant_id}
            onChange={(e) => set("applicant_id", e.target.value)}
            placeholder="COMP-001"
          />
        </div>
        <div>
          <label style={labelStyle}>Contact Name</label>
          <input
            style={inputStyle}
            value={form.contact_name}
            onChange={(e) => set("contact_name", e.target.value)}
            placeholder="Jane Smith"
          />
        </div>
        <div>
          <label style={labelStyle}>
            Requested Amount (USD) <span style={{ color: "var(--danger)" }}>*</span>
          </label>
          <input
            style={inputStyle}
            type="number"
            min="1"
            step="1000"
            value={form.requested_amount_usd}
            onChange={(e) => set("requested_amount_usd", e.target.value)}
            placeholder="500000"
          />
        </div>
        <div>
          <label style={labelStyle}>Contact Email</label>
          <input
            style={inputStyle}
            type="email"
            value={form.contact_email}
            onChange={(e) => set("contact_email", e.target.value)}
            placeholder="jane@example.com"
          />
        </div>
        <div>
          <label style={labelStyle}>Loan Purpose</label>
          <select
            style={inputStyle}
            value={form.loan_purpose}
            onChange={(e) => set("loan_purpose", e.target.value)}
          >
            {LOAN_PURPOSES.map((p) => (
              <option key={p} value={p}>
                {p.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label style={labelStyle}>Loan Term (months)</label>
          <input
            style={inputStyle}
            type="number"
            min="1"
            max="360"
            value={form.loan_term_months}
            onChange={(e) => set("loan_term_months", e.target.value)}
          />
        </div>
      </div>

      {error && (
        <p className="text-sm" style={{ color: "var(--danger)" }}>{error}</p>
      )}

      <button
        type="submit"
        disabled={submitting}
        className="w-full py-2.5 rounded font-medium text-sm transition-opacity"
        style={{
          background: "var(--accent)",
          color: "#fff",
          opacity: submitting ? 0.6 : 1,
          border: "none",
          cursor: submitting ? "not-allowed" : "pointer",
        }}
      >
        {submitting ? "Submitting…" : "Submit Application →"}
      </button>
    </form>
  );
}
