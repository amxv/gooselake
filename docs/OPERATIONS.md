# Operations Runbook

This runbook is for day-two operation: verifying health, inspecting sessions, replaying events, debugging providers, managing processes/worktrees, and recovering after restarts.

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
- mark previously running process records failed after restart

A failed recovery note is usually a real operational signal: inspect provider status, runtime logs, and the durable session/turn records before retrying work.

## Sessions and turns

Create a session:

```bash
SESSION_JSON=$(curl -fsS -X POST \
  "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{"provider":"codex","model":"gpt-5.4-mini","cwd":"/workspace/repo"}' \
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

## Event replay and SSE

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

## Processes

Start a process:

```bash
PROCESS_JSON=$(curl -fsS -X POST \
  "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{"command":"pwd && ls -la","cwd":"/workspace/repo","timeout_ms":30000}' \
  "$BASE_URL/v1/processes")

PROCESS_ID=$(echo "$PROCESS_JSON" | jq -r '.process.process_id')
```

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
- MCP process calls are owned by the caller session.
- Closed or failed sessions cannot invoke MCP tools.

## Worktrees

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

## Teams and deliveries

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

Then update the human docs that explain the behavior. OpenAPI currently captures route/method/content-type coverage better than field-level JSON schemas.
