// frontend/components/ui/Badge.tsx — Reusable state/tier badge

type BadgeVariant = "green" | "red" | "yellow" | "blue" | "gray" | "purple";

const VARIANT_STYLES: Record<BadgeVariant, { bg: string; color: string }> = {
  green:  { bg: "rgba(34,197,94,0.15)",  color: "#22c55e" },
  red:    { bg: "rgba(239,68,68,0.15)",   color: "#ef4444" },
  yellow: { bg: "rgba(245,158,11,0.15)",  color: "#f59e0b" },
  blue:   { bg: "rgba(59,130,246,0.15)",  color: "#3b82f6" },
  gray:   { bg: "rgba(148,163,184,0.15)", color: "#94a3b8" },
  purple: { bg: "rgba(108,99,255,0.15)",  color: "#6c63ff" },
};

interface BadgeProps {
  children: React.ReactNode;
  variant?: BadgeVariant;
}

export default function Badge({ children, variant = "gray" }: BadgeProps) {
  const { bg, color } = VARIANT_STYLES[variant];
  return (
    <span
      className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium"
      style={{ background: bg, color }}
    >
      {children}
    </span>
  );
}

// Helper: map backend state/tier strings to a variant
export function stateVariant(state: string | null | undefined): BadgeVariant {
  if (!state) return "gray";
  const s = state.toUpperCase();
  if (s.includes("APPROVED") || s.includes("FINAL_APPROVED") || s === "FINALAPPROVED") return "green";
  if (s.includes("DECLINED") || s.includes("FINAL_DECLINED") || s === "FINALDECLINED") return "red";
  if (s.includes("PENDING_HUMAN") || s.includes("PENDINGHUMAN") || s.includes("REVIEW")) return "yellow";
  if (s === "CLEAR") return "green";
  if (s === "BLOCKED") return "red";
  if (s === "CONDITIONAL") return "yellow";
  if (s === "LOW") return "green";
  if (s === "HIGH") return "red";
  if (s === "MEDIUM") return "yellow";
  if (s.includes("SUBMITTED") || s.includes("PROCESSING") || s.includes("ANALYSIS")) return "blue";
  return "gray";
}
