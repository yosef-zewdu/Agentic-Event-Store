"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV_LINKS = [
  { href: "/", label: "Dashboard" },
  { href: "/applications/new", label: "New Application" },
  { href: "/review", label: "Review Queue" },
];

export default function NavHeader() {
  const pathname = usePathname();

  return (
    <header
      style={{
        background: "var(--surface)",
        borderBottom: "1px solid var(--border)",
      }}
    >
      <div className="max-w-7xl mx-auto px-4 h-16 flex items-center justify-between">
        {/* Logo */}
        <Link href="/" className="flex items-center gap-2 no-underline">
          <span
            className="text-lg font-bold"
            style={{ color: "var(--accent)" }}
          >
            ◈
          </span>
          <span className="font-semibold text-sm tracking-wide" style={{ color: "var(--text)" }}>
            Apex Financial Services
          </span>
        </Link>

        {/* Nav links */}
        <nav className="flex gap-1">
          {NAV_LINKS.map(({ href, label }) => {
            const isActive =
              href === "/" ? pathname === "/" : pathname.startsWith(href);
            return (
              <Link
                key={href}
                href={href}
                className="px-3 py-1.5 rounded text-sm transition-colors"
                style={{
                  color: isActive ? "var(--accent)" : "var(--text-muted)",
                  background: isActive ? "rgba(108,99,255,0.12)" : "transparent",
                  fontWeight: isActive ? 600 : 400,
                  textDecoration: "none",
                }}
              >
                {label}
              </Link>
            );
          })}
        </nav>
      </div>
    </header>
  );
}
