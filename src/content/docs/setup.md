---
title: Setup
description: Install Gooselake, run the runtime locally, and verify the first control-plane loop.
order: 1
category: "Start Here"
summary: Install the runtime, create a config, check provider readiness, and launch the service.
---

## What you are starting

Gooselake is a host-side runtime. It exposes HTTP and SSE APIs for agent sessions while keeping provider auth, process lifetime, worktrees, event replay, and recovery on the machine where the work happens.

Your first success criterion is not a pretty chat screen. It is a running control plane that can answer health checks, authenticate protected routes, create a session, and stream durable events.

## Install

From a checkout:

```bash
make install
```

Or install a pinned release:

```bash
make install VERSION=v0.1.2
```

The install bundle places the server, sidecars, config example, and systemd examples under `~/.local`.

## Create a config

```bash
cp "$HOME/.local/runtime-server.toml.example" ./runtime-server.toml
```

For local development, prefer a localhost bind and an explicit token:

```toml
[server]
bind_address = "127.0.0.1:8080"
public_base_url = "http://127.0.0.1:8080"

[auth]
mode = "static_bearer"
token = "replace-with-local-token"
```

The full configuration reference lives in [Configuration](/docs/configuration).

## Login providers

Use whichever providers you plan to run:

```bash
codex login
claude login
```

ACP is configured differently: set `[providers.acp].command`, `args`, and `env` for an ACP-compatible stdio agent. ACP auth is agent-managed in the current runtime.

## Validate and start

```bash
gg-runtime-server --check-config --config ./runtime-server.toml
gg-runtime-server --config ./runtime-server.toml
```

In another terminal:

```bash
BASE_URL="http://127.0.0.1:8080"
TOKEN="replace-with-local-token"

curl -fsS "$BASE_URL/health"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/health"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/diagnostics/providers"
```

## First session smoke

```bash
SESSION_JSON=$(curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"provider":"codex","model":"gpt-5.4-mini"}' \
  "$BASE_URL/v1/sessions")

SESSION_ID=$(echo "$SESSION_JSON" | jq -r '.id')

curl -N -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/sessions/$SESSION_ID/events/stream?after_seq=0"
```

Send a turn from another terminal:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"input":[{"type":"text","text":"Reply with ok."}]}' \
  "$BASE_URL/v1/sessions/$SESSION_ID/turns"
```

If the stream shows replayed and live events, the runtime boundary is working.

## Always-on deployment

For a Linux VPS or long-lived host:

```bash
make vps-deploy
```

That path stages releases under `~/.local/share/gg-runtime`, keeps mutable state outside the release, validates the config, and starts a systemd service.
