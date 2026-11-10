---
title: "Operations runbook"
description: "Run health checks, inspect recovery, debug providers, manage processes and worktrees, and recover runtime state after restarts."
order: 51
category: "Operators"
summary: "Day-two commands and troubleshooting for real agent work."
---

This runbook is for day-two operation: verifying health, inspecting
workspace-owned agents, replaying events, debugging providers, managing
processes/worktrees, and recovering after restarts. **The canonical
collaboration API is workspace-first `/v2`**; the independent `/v1`
session/team commands further below remain useful only for legacy
operator clients and migration.

## Baseline environment

```bash
BASE_URL="http://127.0.0.1:8080"
TOKEN="replace-with-runtime-token"
AUTH=(-H "Authorization: Bearer $TOKEN")
```

Public checks:

```bash
curl -fsS "$BASE_URL/health"
curl -fsS "$BASE_URL/openapi.yaml" | head
```

Protected checks:

```bash
curl -fsS "${AUTH[@]}" "$BASE_URL/v2/capabilities"
curl -fsS "${AUTH[@]}" "$BASE_URL/v2/workspaces?limit=50"
curl -fsS "${AUTH[@]}" "$BASE_URL/v2/operations?limit=50"
curl -fsS "${AUTH[@]}" "$BASE_URL/v1/health"
curl -fsS "${AUTH[@]}" "$BASE_URL/v1/version"
curl -fsS "${AUTH[@]}" "$BASE_URL/v1/diagnostics"
```

## Service lifecycle

For a user service:

```bash
systemctl --user status gg-runtime.service
systemctl --user restart gg-runtime.service
journalctl --user -u gg-runtime.service -f
```

For a system service:

```bash
systemctl status gg-runtime.service
systemctl restart gg-runtime.service
journalctl -u gg-runtime.service -f
```

If systemd refuses to restart because of repeated failures:

```bash
systemctl --user reset-failed gg-runtime.service
systemctl --user restart gg-runtime.service
```

Use the non-user equivalents for system services.

## Startup recovery

At bootstrap the runtime hydrates durable state from SQLite and reconciles unfinished work. The recovery summary is exposed through diagnostics:

```bash
curl -fsS "${AUTH[@]}" "$BASE_URL/v1/diagnostics/recovery"
```

The recovery path scans sessions, turns, approvals, and providers. It can:

- mark sessions failed when provider resume fails
- clear stale active turn pointers
- reconcile terminal turns
- preserve pending approvals when still valid
- mark orphaned waiting states failed
- retry deferred team deliveries
- preserve queued process admission and safely re-queue interrupted launch claims
- reconcile previously running process groups only after matching persisted PID + OS start identity, then mark them `interrupted`
- resume pending terminal-process completion delivery without duplicating already-dispatched completion turns
- recover pre-admission system-notice claims and resume pending member onboarding and lifecycle notifications without duplicating ambiguous provider dispatch

A failed recovery note is usually a real operational signal: inspect provider status, runtime logs, and the durable session/turn records before retrying work.

## Canonical workspace and agent operations

Register an existing host project root, then create a provider-backed
agent under that workspace (after confirming provider credentials).
An agent automatically joins its workspace roster, and the workspace
may be **leadless**; no separate team creation/join is required.

```bash
WORKSPACE_JSON=$(curl -fsS -X POST "${AUTH[@]}" \
  "$BASE_URL/v2/workspaces" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: runbook-root-001" \
  -d "$(jq -nc --arg root "$PWD" \
    '{canonical_root:$root,display_name:"Runbook project"}')")
WORKSPACE_ID=$(printf '%s' "$WORKSPACE_JSON" | jq -r '.workspace.workspace_id')

curl -fsS "${AUTH[@]}" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents"

AGENT_JSON=$(curl -fsS -X POST "${AUTH[@]}" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: runbook-agent-001" \
  -d '{"provider":"codex","model":"gpt-6-astra","worktree":{"mode":"root"}}')
AGENT_ID=$(printf '%s' "$AGENT_JSON" | jq -r '.agent_id')
curl -fsS "${AUTH[@]}" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/agents/$AGENT_ID/onboarding"
```

Mandatory system onboarding must be safely delivered before a normal
user turn. `POST /v2/agents/{agent_id}/turns` admits ordered
`{"input":[{"type":"text","text":"..."}]}` blocks; the HTTP response
is not terminal provider proof. Check `/v2/operations/{operation_id}`
for durable native-effect state and use
`/v2/workspaces/{workspace_id}/events/stream` or agent SSE to follow
the journal. An ambiguous provider result needs **independent evidence**
before retrying; a new idempotency key is not a recovery procedure.

The model-facing GG tools are `gg_team`, `gg_message`, `gg_process`
and use a **different per-agent scoped credential** or trusted Codex
native identity; the operator token cannot call `/v2/mcp`.

## Legacy `/v1` sessions and turns

Create a session:

```bash
SESSION_JSON=$(curl -fsS -X POST \
  "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{"provider":"codex","model":"gpt-6-astra","cwd":"/workspace/repo"}' \
  "$BASE_URL/v1/sessions")

SESSION_ID=$(echo "$SESSION_JSON" | jq -r '.id')
```

Send a turn:

```bash
TURN_JSON=$(curl -fsS -X POST \
  "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{"input":[{"type":"text","text":"Summarize this repo."}]}' \
  "$BASE_URL/v1/sessions/$SESSION_ID/turns")

TURN_ID=$(echo "$TURN_JSON" | jq -r '.turn_id')
```

Stream the session:

```bash
curl -N "${AUTH[@]}" \
  "$BASE_URL/v1/sessions/$SESSION_ID/events/stream?after_seq=0"
```

Interrupt a running turn:

```bash
curl -fsS -X POST "${AUTH[@]}" \
  "$BASE_URL/v1/sessions/$SESSION_ID/turns/$TURN_ID/interrupt"
```

Close a session:

```bash
curl -fsS -X POST \
  "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{"reason":"operator cleanup"}' \
  "$BASE_URL/v1/sessions/$SESSION_ID/close"
```

## Legacy `/v1` event replay and SSE

For current operator history, use `GET /v2/events?limit=50`
(redacted index, conditional `X-Next-Cursor` header), or
`GET /v2/events/{event_id}` for full detail. Global/workspace/agent/process
`/v2` SSE streams use `after_seq` or `Last-Event-ID` for replay, not
keyset list cursors.

Global replay:

```bash
curl -fsS "${AUTH[@]}" \
  "$BASE_URL/v1/events?after_seq=0&limit=200"
```

Global stream:

```bash
curl -N "${AUTH[@]}" \
  "$BASE_URL/v1/events/stream?after_seq=0"
```

Rules to remember:

- replay endpoints return JSON arrays
- stream endpoints return SSE
- stream endpoints replay first, then continue live
- `after_seq` query param wins over `Last-Event-ID`
- invalid `Last-Event-ID` returns `400`
- streams send keepalive pings every 10 seconds

## Provider diagnostics

```bash
curl -fsS "${AUTH[@]}" "$BASE_URL/v1/providers"
curl -fsS "${AUTH[@]}" "$BASE_URL/v1/diagnostics/providers"
curl -fsS "${AUTH[@]}" "$BASE_URL/v1/providers/codex/auth/status"
curl -fsS "${AUTH[@]}" "$BASE_URL/v1/providers/claude/auth/status"
curl -fsS "${AUTH[@]}" "$BASE_URL/v1/providers/acp/auth/status"
```

Common fixes:

| Symptom | Likely cause | Check |
| --- | --- | --- |
| Protected routes return `401`. | Missing or wrong bearer token. | Read configured `auth.token` or generated `auth.token_file`. |
| Codex unauthenticated. | Host auth was never staged or `codex login` not run. | Check `~/.gg/codex/auth.json` and runtime provider dir. |
| Claude bridge cannot start. | Missing sidecar, bad bundle layout, bad env, or missing auth. | Check release layout, logs, `GG_CLAUDE_*` overrides. |
| ACP health fails. | Provider enabled without `command`, non-stdio transport, or command cannot execute. | Check `[providers.acp]` and service environment. |
| MCP tools report backend unavailable. | Missing `GG_MCP_GATEWAY_URL` or `GG_MCP_GATEWAY_TOKEN` in sidecar env. | Usually provider injection issue or bad `public_base_url`. |

## Operator process controls (legacy `/v1` management)

Start a process:

```bash
PROCESS_JSON=$(curl -fsS -X POST \
  "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{"command":"pwd && ls -la","cwd":"/workspace/repo","timeout_ms":30000}' \
  "$BASE_URL/v1/processes")

PROCESS_ID=$(echo "$PROCESS_JSON" | jq -r '.process.process_id')
```

Process admission is durable before spawn, so `PROCESS_JSON` may report `queued`. Queue wait is not part of the execution timeout.

Inspect the durable scheduler and active queue:

```bash
curl -fsS "${AUTH[@]}" "$BASE_URL/v1/processes/scheduler"
```

Pause/resume or change capacity with `POST /v1/processes/scheduler`. Reorder queued work with `POST /v1/processes/queue/reorder`; the queue order and scheduler settings survive restart.

Inspect it:

```bash
curl -fsS "${AUTH[@]}" "$BASE_URL/v1/processes/$PROCESS_ID"
curl -fsS "${AUTH[@]}" "$BASE_URL/v1/processes/$PROCESS_ID/logs?tail_lines=100"
curl -N "${AUTH[@]}" "$BASE_URL/v1/processes/$PROCESS_ID/events/stream?after_seq=0"
```

Kill it:

```bash
curl -fsS -X POST \
  "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{"reason":"operator stop"}' \
  "$BASE_URL/v1/processes/$PROCESS_ID/kill"
```

Ownership rules:

- HTTP process endpoints can optionally receive `session_id` to scope access.
- MCP list/status visibility follows the caller's workspace; cancellation is limited to the submitting session.
- Closed or failed sessions cannot invoke MCP tools.
- MCP process completion is pushed automatically to the owning model session; do not poll status waiting for completion.
- Queued cancellation is atomic, prevents launch, and intentionally does not inject a completion turn.

## Legacy `/v1` worktree controls

For canonical worktree inventory/create and provider-verified agent
rebind, use `/v2/workspaces/{workspace_id}/worktrees` and
`/v2/workspaces/{workspace_id}/agents/{agent_id}/worktree`. Do not
manually claim or set the cwd of a workspace agent through these
legacy endpoints.

Create a managed worktree:

```bash
curl -fsS -X POST \
  "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{
    "source_session_id":"'"$SESSION_ID"'",
    "repo_root":"/workspace/repo",
    "worktree_name":"docs-pass",
    "branch_prefix":"gg",
    "base_ref":"main",
    "run_init_script":false
  }' \
  "$BASE_URL/v1/worktrees"
```

Claim, release, and cleanup are explicit operations:

```bash
curl -fsS -X POST "${AUTH[@]}" -H "Content-Type: application/json" \
  -d '{"session_id":"'"$SESSION_ID"'","claim_role":"owner"}' \
  "$BASE_URL/v1/worktrees/$WORKTREE_ID/claims"

curl -fsS -X POST "${AUTH[@]}" -H "Content-Type: application/json" \
  -d '{"session_id":"'"$SESSION_ID"'","cleanup_if_last_claim":true}' \
  "$BASE_URL/v1/worktrees/$WORKTREE_ID/release"

curl -fsS -X POST "${AUTH[@]}" -H "Content-Type: application/json" \
  -d '{"reason":"manual cleanup"}' \
  "$BASE_URL/v1/worktrees/$WORKTREE_ID/cleanup"
```

Cleanup can delete the worktree path and branch depending on policy and active claims. Diagnostics are returned in the cleanup response; do not ignore them.

## Legacy `/v1` teams and deliveries

New agents already form a workspace roster; `POST /v2/messages` sends
global direct messages across workspaces or workspace-local broadcasts,
with `GET /v2/messages/{message_id}/deliveries` for per-recipient
status. The legacy example below is only for existing independent
`TeamRecord` clients during migration.

Create a team around an existing session:

```bash
TEAM_JSON=$(curl -fsS -X POST \
  "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{"name":"Docs crew","lead_agent_id":"'"$SESSION_ID"'","member_agent_ids":["'"$SESSION_ID"'"]}' \
  "$BASE_URL/v1/teams")

TEAM_ID=$(echo "$TEAM_JSON" | jq -r '.team.id')
```

Send a direct message:

```bash
curl -fsS -X POST \
  "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{
    "sender_agent_id":"'"$SESSION_ID"'",
    "recipient_agent_id":"'"$OTHER_SESSION_ID"'",
    "input":{"type":"text","text":"Please review the install guide."},
    "priority":"normal",
    "policy":"non_interrupting"
  }' \
  "$BASE_URL/v1/teams/$TEAM_ID/messages"
```

Inspect team state:

```bash
curl -fsS "${AUTH[@]}" "$BASE_URL/v1/teams/$TEAM_ID/view?include_delivery_map=true"
curl -fsS "${AUTH[@]}" "$BASE_URL/v1/teams/$TEAM_ID/deliveries"
curl -N "${AUTH[@]}" "$BASE_URL/v1/teams/$TEAM_ID/events/stream?after_seq=0"
```

Delivery states are part of the runtime contract. Use retries/cancellation instead of inventing client-only state:

```bash
curl -fsS -X POST "${AUTH[@]}" \
  "$BASE_URL/v1/teams/$TEAM_ID/deliveries/$DELIVERY_ID/retry"

curl -fsS -X POST "${AUTH[@]}" \
  "$BASE_URL/v1/teams/$TEAM_ID/messages/$MESSAGE_ID/cancel"
```

## API/doc sync before merging API work

When route behavior changes:

```bash
make api-docs-refresh
make api-docs-status
make api-docs-check
```

Then update the human docs that explain the behavior. OpenAPI is
generated from Axum's shared declarative Rust route table and
handler/core `JsonSchema` types, including typed `/v2` query/header/auth
contracts. Only the two dynamic model MCP JSON response bodies remain
intentionally broad.
