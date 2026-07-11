---
title: Goosetower And Gooseweb Deployment
description: "Deploy the Gooseweb browser app and Goosetower realtime gateway with Gooselake as the source of truth."
order: 56
category: "Operators"
summary: "The production topology, config expectations, preflight checks, and reverse proxy notes for Gooseweb and Goosetower."
---

# Goosetower And Gooseweb Deployment

Gooseweb and Goosetower are deployed as separate pieces:

- Gooseweb runs on Vercel and serves the TanStack Start app only.
- The browser connects directly to Goosetower with `wss://goosetower.example.com/v1/realtime?ticket=...`.
- Goosetower runs on the VPS near `gg-runtime-server`.
- Goosetower routes source-of-truth commands to Gooselake runtime HTTP APIs and consumes runtime event replay/SSE.
- Goosetower keeps upstream runtime bearer tokens server-side only. Do not put the Gooselake runtime token in Gooseweb, Vercel env vars that are exposed to the client, browser storage, or WebSocket query strings.

## Files

Recommended starting points:

- `examples/goosetower.toml` for production-style config.
- `examples/goosetower.local.toml` for local development.
- `deploy/systemd/gg-goosetower.service.example` for a user service.
- `deploy/systemd/gg-goosetower.env.example` for non-secret environment overrides.
- `scripts/preflight-goosetower.sh` for config and endpoint checks.

## Production Config

Use exact origins in `server.allowed_gooseweb_origins`. Include the production Vercel origin and every preview origin pattern as explicit URLs. Do not use wildcard origin matching.

Keep secrets in files referenced by config:

- `auth.api_token_file` for protected Goosetower HTTP endpoints.
- `tickets.signing_key_file` for short-lived Gooseweb connection tickets.
- `runtimes.sources[].bearer_token_file` for the upstream Gooselake runtime token.

Goosetower should usually bind to localhost behind a TLS reverse proxy:

```toml
[server]
bind_address = "127.0.0.1:8090"
public_base_url = "https://goosetower.example.com"
allowed_gooseweb_origins = [
  "https://gooseweb.example.com",
  "https://gooseweb-git-main-example.vercel.app",
]
```

## Reverse Proxy

Terminate TLS at nginx, Caddy, Cloudflare Tunnel, or another VPS proxy that supports WebSocket upgrades. The proxy must forward:

- `Upgrade`
- `Connection`
- `Host`
- `X-Forwarded-Proto`

For nginx, the relevant location shape is:

```nginx
location / {
    proxy_pass http://127.0.0.1:8090;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_read_timeout 3600s;
}
```

Goosetower validates the browser `Origin` header on the WebSocket upgrade. CORS settings for fetch routes do not protect WebSocket connections, so the exact origin allowlist is a production requirement.

## Local Development

One local development layout:

```bash
gg-runtime-server --config examples/runtime-server.toml
gg-goosetower --config examples/goosetower.local.toml
make gooseweb-dev
```

The local Gooseweb dev server runs from `apps/gooseweb` and connects to the local gateway. The local Goosetower example enables debug endpoints and uses the development API token `dev-goosetower-token`.

## Preflight

Validate the config only:

```bash
make goosetower-check-config \
  GOOSETOWER_BIN=target/debug/gg-goosetower \
  GOOSETOWER_CONFIG=examples/goosetower.local.toml
```

Validate a running gateway:

```bash
make goosetower-preflight \
  GOOSETOWER_BIN=target/debug/gg-goosetower \
  GOOSETOWER_CONFIG=examples/goosetower.local.toml \
  BASE_URL=http://127.0.0.1:8090 \
  TOKEN=dev-goosetower-token
```

The preflight checks:

- `gg-goosetower --check-config`
- `GET /health`
- authenticated `GET /v1/health`
- authenticated `GET /v1/sources`
- authenticated `GET /v1/metrics`

Debug endpoints are intentionally separate and require both bearer auth and `[debug].endpoints_enabled = true`:

- `GET /v1/debug/protocol`
- `GET /v1/debug/sources`
- `GET /v1/debug/subscriptions`
- `GET /v1/debug/materializer`
- `GET /v1/debug/audit`

## Observability

Goosetower emits structured tracing fields for connection, command, source, resume, and subscription audit events. The authenticated `/v1/metrics` endpoint returns current in-process counters for:

- connection open/close totals and active connections
- source health and stale age
- browser RTT
- command accepted/rejected counts and admission/upstream latency
- event ingest lag and materializer reduce time
- outbound messages by critical/state/token/bulk lane
- coalesced state messages and dropped bulk/backpressure messages
- WebSocket buffered messages
- resume success, partial replay, gap, and snapshot resync counts

For production, scrape or poll `/v1/metrics` from the VPS side or from trusted infrastructure only. Do not expose the Goosetower API bearer token to browser code.
