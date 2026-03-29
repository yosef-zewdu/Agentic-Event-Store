// frontend/components/ui/ErrorBox.tsx

export default function ErrorBox({ message }: { message: string }) {
  return (
    <div
      className="rounded p-4 text-sm"
      style={{
        background: "rgba(239,68,68,0.1)",
        border: "1px solid rgba(239,68,68,0.3)",
        color: "#ef4444",
      }}
    >
      {message}
    </div>
  );
}
