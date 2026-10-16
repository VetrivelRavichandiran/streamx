# STREAMX web dashboard — Next.js 15 (standalone output)
# Build context: repo root is NOT used; this is built from apps/web
# (see docker-compose.yml: build: { context: ./apps/web }).

FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

FROM node:22-alpine AS build
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
ENV NEXT_TELEMETRY_DISABLED=1
# next.config.ts rewrites /api/* → API_TARGET; rewrites are evaluated at
# BUILD time, so the destination is baked in via this build arg.
#   docker compose: API_TARGET=http://api:8080
#   local dev:      (unset → http://localhost:8080)
ARG API_TARGET=http://localhost:8080
ENV API_TARGET=$API_TARGET
# NEXT_PUBLIC_API_URL stays unset: the browser talks to the same origin and
# the server-side rewrite proxies /api/* to the control plane.
RUN npm run build

FROM node:22-alpine AS run
WORKDIR /app
ENV NODE_ENV=production NEXT_TELEMETRY_DISABLED=1 PORT=3000 HOSTNAME=0.0.0.0
COPY --from=build /app/.next/standalone ./
COPY --from=build /app/.next/static ./.next/static
COPY --from=build /app/public ./public
EXPOSE 3000
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
  CMD node -e "fetch('http://localhost:3000').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
CMD ["node", "server.js"]