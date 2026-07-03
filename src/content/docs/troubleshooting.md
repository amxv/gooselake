---
title: "Troubleshooting"
description: "Debug common Gooselake failures around auth, providers, sessions, streams, teams, processes, worktrees, MCP, and docs sync."
order: 54
category: "Operators"
summary: "Failure playbooks for the runtime's most common sharp edges."
---

When Gooselake feels confusing, start with the runtime's own explanation before guessing. Most important state is inspectable through health, diagnostics, records, events, and logs.

## Baseline commands

```bash
BASE_URL="http://127.0.0.1:8080"
TOKEN="replace-with-runtime-token"
AUTH=(-H "Authorization: Bearer $TOKEN")

curl "$BASE_URL/health"
curl "$BASE_URL/v1/health" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/recovery" "${AUTH[@]}"
```

## `401 missing or invalid bearer token`

Protected routes require the exact bearer token.

Check:

- did you include `Authorization: Bearer ...`?
- are you using the token from the current config/data root?
- did the service create a new token file in a different data root?
- are you accidentally calling `/v1/**` through a proxy that strips auth headers?

Public `/health` working only proves the process is alive. It does not prove protected auth is correct.

## Provider not registered

If a provider route or session create returns provider-not-registered behavior, check provider config and diagnostics:

```bash
curl "$BASE_URL/v1/providers" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/providers" "${AUTH[@]}"
```

Common causes:

- provider disabled in config
- missing provider CLI or sidecar binary
- invalid provider command/path
- ACP disabled by default
- Claude bridge not available in the expected release/source layout

## Provider auth not ready

Check provider-specific auth status:

```bash
curl "$BASE_URL/v1/providers/codex/auth/status" "${AUTH[@]}"
curl "$BASE_URL/v1/providers/claude/auth/status" "${AUTH[@]}"
curl "$BASE_URL/v1/providers/acp/auth/status" "${AUTH[@]}"
```

Codex depends on staged host auth. Claude depends on host-machine or runtime-managed auth mode. ACP auth is agent-managed and may only report what the configured ACP process exposes.

## Session already has an active turn

This is expected runtime protection. A session can only have one active turn.

Inspect the session and session events:

```bash
curl "$BASE_URL/v1/sessions/$SESSION_ID" "${AUTH[@]}"
curl "$BASE_URL/v1/sessions/$SESSION_ID/events" "${AUTH[@]}"
```

Options:

- wait for completion
- respond to a pending approval
- interrupt the active turn
- use a different session

## SSE stream reconnect issues

Use explicit cursors when debugging:

```bash
curl "$BASE_URL/v1/sessions/$SESSION_ID/events?after_seq=0" "${AUTH[@]}"
curl -N "$BASE_URL/v1/sessions/$SESSION_ID/events/stream?after_seq=0" "${AUTH[@]}"
```

Remember:

- `after_seq` wins over `Last-Event-ID`
- invalid `Last-Event-ID` returns `400`
- scoped streams use scoped sequence IDs
- global streams use global row IDs

## Team message created but not delivered

A message and a delivery are different. Inspect deliveries:

```bash
curl "$BASE_URL/v1/teams/$TEAM_ID/deliveries" "${AUTH[@]}"
curl "$BASE_URL/v1/teams/$TEAM_ID/view" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/comms" "${AUTH[@]}"
```

Common causes:

- recipient session is busy
- delivery policy defers injection
- recipient is not an active team member
- recipient session is closed/failed
- earlier delivery is blocking per-recipient order

Retry when safe:

```bash
curl -X POST "$BASE_URL/v1/teams/$TEAM_ID/deliveries/$DELIVERY_ID/retry" "${AUTH[@]}"
```

## Spawn failed halfway

Spawn is a multi-step operation. Inspect the journal:

```bash
curl "$BASE_URL/v1/diagnostics/team-operations?team_id=$TEAM_ID" "${AUTH[@]}"
```

Look for the last successful stage and any rollback diagnostics. Failures can happen in worktree creation, session creation, team join, claim assignment, or onboarding delivery.

## Process output missing from stream

Process output events are sampled. Read logs for full output:

```bash
curl "$BASE_URL/v1/processes/$PROCESS_ID/logs" "${AUTH[@]}"
```

If a process appears stuck, inspect:

```bash
curl "$BASE_URL/v1/processes/$PROCESS_ID" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/processes" "${AUTH[@]}"
```

## Worktree cleanup blocked

Cleanup respects active claims. Inspect the worktree and claims:

```bash
curl "$BASE_URL/v1/worktrees/$WORKTREE_ID" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/worktrees" "${AUTH[@]}"
```

Release claims before expecting deletion.

## MCP tool call rejected

Check:

- is the caller session ID present?
- is the caller session active and known to the runtime?
- does `/v1/mcp/capabilities` list the tool namespace?
- is the process owned by another session?

Current runtime gateway capability is primarily `gg_process`. Use HTTP routes for team/worktree services that are not exposed by the gateway.

## Docs/API drift

If you changed runtime route behavior, run the sync workflow:

```bash
make api-docs-refresh
make api-docs-status
make api-docs-check
```

Then update human docs when schemas are broad or behavior changed in ways OpenAPI cannot express.
