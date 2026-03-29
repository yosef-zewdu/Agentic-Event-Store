import type { Metadata } from "next";
import "./globals.css";
import NavHeader from "@/components/NavHeader";

export const metadata: Metadata = {
  title: "Apex Financial Services — Ledger",
  description: "Agentic loan origination platform",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="min-h-screen" style={{ background: "var(--bg)" }}>
        <NavHeader />
        <main className="max-w-7xl mx-auto px-4 py-8">{children}</main>
      </body>
    </html>
  );
}
