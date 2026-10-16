import type { NextConfig } from "next";

// Rewrite destination for /api/*:
//  - API_TARGET: runtime override (used by the docker-compose web container
//    to reach the `api` service on the compose network).
//  - NEXT_PUBLIC_API_URL: build-time override.
//  - default: local dev, where the API runs on the host at :8080.
const API_TARGET =
  process.env.API_TARGET || process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080";

const nextConfig: NextConfig = {
  output: "standalone",
  // Dev convenience: proxy control-plane API through Next so the browser
  // never deals with CORS. In production the API should be served behind
  // the same origin (or with CORS configured for the web origin).
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${API_TARGET}/api/:path*`,
      },
    ];
  },
  // The dashboard talks to the API client-side (fetch + EventSource).
  // Keep the standalone output lean; no image optimization needed.
  images: { unoptimized: true },
};

export default nextConfig;