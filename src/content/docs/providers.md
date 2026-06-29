---
title: Providers
description: Configure and diagnose Codex, Claude, and ACP behind the shared runtime provider contract.
order: 7
category: Reference
summary: How each provider is registered, authenticated, modeled, and diagnosed.
---

## Shared contract

Every provider becomes a runtime adapter for sessions, turns, approvals, interrupts, terminal results, and close/resume behavior. Clients should use runtime session IDs and not provider-native IDs.

## Codex

Use `provider: "codex"`.

Models:

- `gpt-5.5`
- `gpt-5.4`
- `gpt-5.4-mini`

Setup:

```bash
codex login
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/providers/codex/auth/status"
```

The runtime stages host auth from `~/.gg/codex/auth.json` into the runtime Codex home when available.

## Claude

Use `provider: "claude"`.

Models:

- `claude-sonnet-4-6`
- `claude-opus-4-8`
- `claude-haiku-4-5`

Setup:

```bash
claude login
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/providers/claude/auth/status"
```

Claude runs through the bundled Claude bridge and can use host-machine auth or runtime-managed imports.

## ACP

Use `provider: "acp"`.

ACP is configured with an external stdio agent command:

```toml
[providers.acp]
enabled = true
command = "/absolute/path/to/acp-agent"
args = ["serve", "--stdio"]
transport = "stdio"
```

ACP auth is agent-managed. A blank model list is valid. ACP permission requests are not supported in the current runtime and fail the active turn clearly.

## Diagnostics

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/diagnostics/providers"
```

Use provider diagnostics before blaming client code. Most failures are missing host credentials, bad service environment, bad sidecar paths, or an ACP command that cannot start.
