"use client";

import { useRef, useState } from "react";
import { uploadDocument } from "@/lib/api";

const DOCUMENT_TYPES = [
  { value: "income_statement", label: "Income Statement", required: true },
  { value: "balance_sheet", label: "Balance Sheet", required: true },
  { value: "application_proposal", label: "Application Proposal", required: false },
];

interface UploadState {
  file: File | null;
  status: "idle" | "uploading" | "done" | "error";
  error?: string;
  savedFilename?: string;
}

interface DocumentUploadProps {
  applicationId: string;
  onAllRequiredUploaded: (done: boolean) => void;
}

export default function DocumentUpload({
  applicationId,
  onAllRequiredUploaded,
}: DocumentUploadProps) {
  const [uploads, setUploads] = useState<Record<string, UploadState>>(() =>
    Object.fromEntries(DOCUMENT_TYPES.map((d) => [d.value, { file: null, status: "idle" }]))
  );

  const refs = useRef<Record<string, HTMLInputElement | null>>({});

  function setUpload(type: string, update: Partial<UploadState>) {
    setUploads((prev) => {
      const next = { ...prev, [type]: { ...prev[type], ...update } };
      const requiredDone = DOCUMENT_TYPES.filter((d) => d.required).every(
        (d) => next[d.value].status === "done"
      );
      onAllRequiredUploaded(requiredDone);
      return next;
    });
  }

  async function handleFileChange(type: string, file: File | null) {
    if (!file) return;
    setUpload(type, { file, status: "uploading", error: undefined });
    try {
      const result = await uploadDocument(applicationId, file, type);
      setUpload(type, { status: "done", savedFilename: result.filename });
    } catch (err: unknown) {
      setUpload(type, { status: "error", error: String(err) });
    }
  }

  return (
    <div className="space-y-4">
      {DOCUMENT_TYPES.map(({ value, label, required }) => {
        const u = uploads[value];
        return (
          <div
            key={value}
            className="rounded-lg p-4"
            style={{ background: "var(--surface)", border: "1px solid var(--border)" }}
          >
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm font-medium" style={{ color: "var(--text)" }}>
                {label}
                {required && <span style={{ color: "var(--danger)" }}> *</span>}
              </span>
              {u.status === "done" && (
                <span className="text-xs" style={{ color: "var(--success)" }}>
                  Uploaded: {u.savedFilename}
                </span>
              )}
            </div>

            {/* Drop zone / file picker */}
            <div
              className="rounded border-2 border-dashed p-6 text-center cursor-pointer transition-colors"
              style={{
                borderColor: u.status === "done" ? "var(--success)" : "var(--border)",
                background: u.status === "done" ? "rgba(34,197,94,0.05)" : "transparent",
              }}
              onClick={() => refs.current[value]?.click()}
              onDragOver={(e) => e.preventDefault()}
              onDrop={(e) => {
                e.preventDefault();
                const file = e.dataTransfer.files[0] ?? null;
                handleFileChange(value, file);
              }}
            >
              <input
                ref={(el) => { refs.current[value] = el; }}
                type="file"
                className="hidden"
                accept=".pdf,.xlsx,.csv"
                onChange={(e) => handleFileChange(value, e.target.files?.[0] ?? null)}
              />
              {u.status === "uploading" ? (
                <p className="text-sm" style={{ color: "var(--text-muted)" }}>Uploading…</p>
              ) : u.status === "done" ? (
                <p className="text-sm" style={{ color: "var(--success)" }}>
                  {u.file?.name} — {((u.file?.size ?? 0) / 1024).toFixed(1)} KB
                </p>
              ) : (
                <p className="text-sm" style={{ color: "var(--text-muted)" }}>
                  Drag & drop or click to select (.pdf, .xlsx, .csv)
                </p>
              )}
            </div>

            {u.status === "error" && (
              <p className="mt-1 text-xs" style={{ color: "var(--danger)" }}>
                {u.error}
              </p>
            )}
          </div>
        );
      })}
    </div>
  );
}
