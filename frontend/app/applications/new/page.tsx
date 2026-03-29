"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import ApplicationForm from "@/components/ApplicationForm";
import DocumentUpload from "@/components/DocumentUpload";
import Link from "next/link";

type Step = 1 | 2 | 3;

export default function NewApplicationPage() {
  const router = useRouter();
  const [step, setStep] = useState<Step>(1);
  const [applicationId, setApplicationId] = useState<string | null>(null);
  const [requiredDocsDone, setRequiredDocsDone] = useState(false);

  function handleSubmitted(id: string) {
    setApplicationId(id);
    setStep(2);
  }

  const cardStyle = {
    background: "var(--surface)",
    border: "1px solid var(--border)",
    borderRadius: 12,
    padding: 32,
  };

  return (
    <div className="max-w-2xl mx-auto">
      {/* Step indicator */}
      <div className="flex items-center gap-2 mb-8">
        {(["1 Application Details", "2 Upload Documents", "3 Submitted"] as const).map(
          (label, idx) => {
            const s = (idx + 1) as Step;
            const isActive = step === s;
            const isDone = step > s;
            return (
              <div key={label} className="flex items-center gap-2">
                <div
                  className="w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold"
                  style={{
                    background: isDone
                      ? "var(--success)"
                      : isActive
                      ? "var(--accent)"
                      : "var(--border)",
                    color: isDone || isActive ? "#fff" : "var(--text-muted)",
                  }}
                >
                  {isDone ? "✓" : idx + 1}
                </div>
                <span
                  className="text-sm hidden sm:block"
                  style={{ color: isActive ? "var(--text)" : "var(--text-muted)" }}
                >
                  {label.substring(2)}
                </span>
                {idx < 2 && (
                  <div
                    className="flex-1 h-px mx-2"
                    style={{ background: "var(--border)", minWidth: 24 }}
                  />
                )}
              </div>
            );
          }
        )}
      </div>

      {/* Step 1 — Application details */}
      {step === 1 && (
        <div style={cardStyle}>
          <h1 className="text-xl font-semibold mb-6" style={{ color: "var(--text)" }}>
            New Loan Application
          </h1>
          <ApplicationForm onSubmitted={handleSubmitted} />
        </div>
      )}

      {/* Step 2 — Document upload */}
      {step === 2 && applicationId && (
        <div style={cardStyle}>
          <div className="mb-6">
            <h1 className="text-xl font-semibold" style={{ color: "var(--text)" }}>
              Upload Documents
            </h1>
            <p className="text-sm mt-1" style={{ color: "var(--text-muted)" }}>
              Application ID: <code style={{ color: "var(--accent)" }}>{applicationId}</code>
            </p>
          </div>
          <DocumentUpload
            applicationId={applicationId}
            onAllRequiredUploaded={setRequiredDocsDone}
          />
          <div className="mt-6 flex gap-3">
            <button
              onClick={() => setStep(3)}
              disabled={!requiredDocsDone}
              className="flex-1 py-2.5 rounded font-medium text-sm transition-opacity"
              style={{
                background: "var(--accent)",
                color: "#fff",
                opacity: requiredDocsDone ? 1 : 0.4,
                border: "none",
                cursor: requiredDocsDone ? "pointer" : "not-allowed",
              }}
            >
              Continue →
            </button>
            <button
              onClick={() => setStep(3)}
              className="px-4 py-2.5 rounded text-sm"
              style={{
                background: "transparent",
                color: "var(--text-muted)",
                border: "1px solid var(--border)",
                cursor: "pointer",
              }}
            >
              Skip
            </button>
          </div>
        </div>
      )}

      {/* Step 3 — Confirmation */}
      {step === 3 && applicationId && (
        <div style={{ ...cardStyle, textAlign: "center" }}>
          <div
            className="w-16 h-16 rounded-full flex items-center justify-center text-3xl mx-auto mb-4"
            style={{ background: "rgba(108,99,255,0.15)" }}
          >
            ✓
          </div>
          <h1 className="text-xl font-semibold mb-2" style={{ color: "var(--text)" }}>
            Application Submitted
          </h1>
          <p className="text-sm mb-1" style={{ color: "var(--text-muted)" }}>
            Application ID
          </p>
          <p className="font-mono text-lg mb-6" style={{ color: "var(--accent)" }}>
            {applicationId}
          </p>
          <p className="text-sm mb-8" style={{ color: "var(--text-muted)" }}>
            The agent pipeline has been queued. You can track its progress on the status page.
          </p>
          <div className="flex gap-3 justify-center">
            <Link
              href={`/applications/${applicationId}`}
              className="px-6 py-2.5 rounded font-medium text-sm"
              style={{
                background: "var(--accent)",
                color: "#fff",
                textDecoration: "none",
              }}
            >
              Track Status →
            </Link>
            <Link
              href="/"
              className="px-6 py-2.5 rounded text-sm"
              style={{
                background: "transparent",
                color: "var(--text-muted)",
                border: "1px solid var(--border)",
                textDecoration: "none",
              }}
            >
              Dashboard
            </Link>
          </div>
        </div>
      )}
    </div>
  );
}
