---
title: Setup
description: Install Gooselake, register your first workspace, check provider readiness, and inspect durable runtime state.
order: 1
category: "Start Here"
summary: Bring up the runtime and exercise the workspace-first operator API without requiring a provider launch.
---

## What you are starting

Gooselake runs on the machine that owns the project files, provider credentials, processes and Git worktrees. The **canonical public workflow** is: register an existing workspace root, create provider-backed agents **inside** that workspace, follow their durable turns and events, and use the workspace's roster as their team.

A workspace registration and capability lookup can be verified **without starting a provider session**. Creating an agent or sending a live turn requires the selected provider to be installed, configured and authenticated.

## Install and configure

From a source checkout:

```bash
make install
```

Or install a pinned release:

```bash
make install VERSION=v0.1.2
```

The verified bundle contains `gg-runtime-server`, the Claude and GG MCP sidecars, an OpenAPI snapshot, and runtime harness/tool/model contracts. See [Install](/docs/install) for release and source installation details.

Create a local config from the installed template:

```bash
cp "$HOME/.local/runtime-server.toml.example" ./runtime-server.toml
```

For development, prefer localhost and a unique operator credential:

```toml
[server]
bind_address = "127.0.0.1:8080"
public_base_url = "http://127.0.0.1:8080"

[auth]
mode = "static_bearer"
token = "replace-with-a-private-operator-token"
```

Never put a real credential in committed examples or send it to a model tool. [Configuration](/docs/configuration) covers file-based secrets and other settings.

## Check the runtime first

```bash
gg-runtime-server --check-config --config ./runtime-server.toml
gg-runtime-server --config ./runtime-server.toml
```

In a second shell:

```bash
BASE_URL="http://127.0.0.1:8080"
TOKEN="replace-with-a-private-operator-token"

curl -fsS "$BASE_URL/health"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v2/capabilities"
```

The first is public; the second requires the **operator** Bearer token. `/v2/mcp/*` is a distinct **agent-scoped** credential surface, not an operator API shortcut. API errors under operator `/v2` include machine-readable `error.code` and `request_id`.

## Register an existing workspace root

Choose a real directory **on the runtime host**; for a source
checkout, the current directory is already a valid example:

```bash
PROJECT_ROOT="$PWD"
test -d "$PROJECT_ROOT"  # must exist on the machine running Gooselake

WORKSPACE_RESULT=$(curl -fsS -X POST "$BASE_URL/v2/workspaces" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: setup-project-001" \
  -d "$(jq -nc --arg root "$PROJECT_ROOT" \
    '{canonical_root:$root,display_name:"My Project"}')")

WORKSPACE_ID=$(printf '%s' "$WORKSPACE_RESULT" | jq -r '.workspace.workspace_id')

curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID"
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents"
```

The second response is initially an empty roster, **not** an error about a missing legacy team. Repeating the same canonical-root registration converges on the same workspace identity; reusing an idempotency key with *different* input is a conflict. Workspace lists use bounded keyset pagination and an optional `X-Next-Cursor` response header.

## Optional: launch a provider-backed agent

Install/authenticate the provider you intend to use, for example:

```bash
codex login
codex app-server --help >/dev/null
# Or authenticate Claude through its supported host-machine/runtime-managed mode
```

ACP instead uses an agent-managed stdio command configured under `[providers.acp]`; it has no universal runtime login flow. Check [Providers](/docs/providers) for capability and credential differences.

With a ready Codex adapter:

```bash
AGENT_RESULT=$(curl -fsS -X POST \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: setup-agent-001" \
  -d '{"provider":"codex","model":"gpt-6-astra","title":"Builder","worktree":{"mode":"root"}}')

AGENT_ID=$(printf '%s' "$AGENT_RESULT" | jq -r '.agent_id')

curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents/$AGENT_ID/onboarding"
```

The agent is automatically attached to the workspace roster. A **mandatory system onboarding notice** is queued and may still be `pending` or `injecting`. Do not race it with your first user turn: until onboarding has safely reached the provider, a user-turn request can return `409 onboarding_pending`.

When that notice is delivered, submit a user turn:

```bash
curl -fsS -X POST "$BASE_URL/v2/agents/$AGENT_ID/turns" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: setup-turn-001" \
  -d '{"input":[{"type":"text","text":"Summarize the repository."}]}'
```

A successful response admits the turn; it **does not** guarantee terminal provider completion. Inspect the durable workspace events:

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/events/stream?after_seq=0"
```

Use `Last-Event-ID` or `after_seq` on reconnect rather than starting from an arbitrary guessed cursor. Provider/device live acceptance is separate from these documented setup steps.

## Always-on deployment

For a Linux VPS or persistent host:

```bash
make vps-deploy
```

See [Deployment](/docs/deployment), [API](/docs/api), [Teams](/docs/teams) and [Troubleshooting](/docs/troubleshooting). Legacy `/v1/sessions` and `/v1/teams` remain migration compatibility paths; they are not the recommended new setup flow.
