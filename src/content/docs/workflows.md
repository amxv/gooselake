---
title: Operator workflows
description: "Repeatable workspace-first operator workflows for agent execution, messaging, worktrees and recovery."
order: 55
category: "Operators"
summary: "The operator's day-two playbook for durable workspace and agent operations."
---

## Workspace and agent lifecycle

A normal operator loop is:

1. Register an existing project directory with `POST /v2/workspaces`.
2. Create a provider-backed agent inside it with `POST /v2/workspaces/{workspace_id}/agents`; it joins the roster automatically.
3. Check the agent's mandatory onboarding, then submit a user turn through `POST /v2/agents/{agent_id}/turns`.
4. Follow durable workspace or agent SSE, inspect pending provider approvals, and wait for an authoritative terminal turn.
5. Archive or restore the agent through its workspace-scoped routes when needed. Restoring a former lead does **not** automatically reassign leadership.

Keep operation IDs and `Idempotency-Key` values for native-effect recovery. A workspace's zero/one lead is set separately by a revisioned operator mutation; no background election occurs.

## Event recovery

After a disconnect, resume with the last durable cursor:

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/workspaces/$WORKSPACE_ID/events/stream?after_seq=$LAST_SEQ"
```

The stream subscribes before replay and follows committed events. Scoped streams use per-resource sequence IDs; the global `/v2/events/stream` cursor is a journal row ID. Do not use a list endpoint's `X-Next-Cursor` value as an SSE cursor.

## Messaging and member coordination

An operator can submit agent-first direct or workspace broadcast messages with `POST /v2/messages`, then inspect per-recipient delivery state. Direct crosses workspace boundaries; broadcast stays within the sender's own workspace and snapshots its active roster.

Model agents use `gg_team` and `gg_message` with caller authority supplied by their provider bridge. The operator can reassign/clear leadership, interrupt a workspace's active member turns, archive/restore agents and review durable operations. None of those actions require manually joining a separate final-domain team.

## Long-running commands

`gg_process` schedules owner-agent work and pushes terminal completion back as automation context. The operator's process history, scheduler and log surfaces remain available separately. Treat log files as authoritative for complete output; event samples are bounded. Queue wait and execution timeout are separate, and a terminal process may still have a completion notice pending.

## Managed worktrees

A workspace owns its managed Git worktree inventory, route choices and retention policy. The runtime keeps claims and Git-operation evidence, not just filesystem paths. Assigning an existing agent to a different managed checkout requires **provider workspace-rebind proof**. A native error with ambiguous outcome must be reviewed before cleanup or a new assignment.

## Native uncertainty and retirement

When an effect is reported as `outcome_unresolved` or `manual_review`:

1. Read `GET /v2/operations/{operation_id}` and the affected agent/worktree/process state.
2. Check durable claims, provider/native identity, phase and effect observations; do not automatically repeat the native action with a fresh key.
3. Use only the specific evidence-backed resolution endpoint for that operation, with the independently verified identity/receipt it requires.
4. Resume normal work after the resource is safely classified.

Workspace retirement is a durable saga across turns, provider attachments, processes, messages, hooks, worktrees and notices. A `202 Accepted` retirement response is **not** a declaration that everything closed. Later registration of the same root can reactivate the same identity only after terminal proof and conflict checks.

## Diagnostics and legacy compatibility

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/diagnostics/recovery"
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/events?limit=50"
```

The diagnostics route is a supported legacy operator surface; the newer operator event index returns a redacted bounded history with optional `X-Next-Cursor` and a full-record lookup.

`/v1/sessions`, `/v1/teams` and historical team-spawn routes remain for migration. Do not build new independent team authority on them; unresolved legacy sessions stay classified until an operator resolves them.

See [API](/docs/api), [Teams](/docs/teams), [Events and recovery](/docs/events-and-recovery), and [Troubleshooting](/docs/troubleshooting).
