import type { Metadata } from "next";
import { AppShell } from "@/components/layout";
import "./globals.css";

export const metadata: Metadata = {
  title: {
    default: "STREAMX — Control Plane",
    template: "%s · STREAMX",
  },
  description: "Dashboard for the STREAMX distributed event streaming platform",
};

export const viewport = {
  themeColor: "#0a0e14",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className="app-bg min-h-screen text-slate-300">
        <AppShell>{children}</AppShell>
      </body>
    </html>
  );
}