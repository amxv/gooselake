---
title: "Processes"
description: "Durably schedule host commands, inspect authoritative logs, and understand completion delivery and process ownership."
order: 32
category: "Runtime Services"
summary: "Runtime-managed command execution for agents and operators."
---

Processes are Gooselake's bridge from agent intent to host execution. They let the runtime durably accept commands, schedule them under global/workspace capacity, capture logs, stream sampled output, recover across restarts, and tie work back to a session.

Think of the process manager as a supervised workshop bench: the runtime starts the tool, labels the job, captures the debris, and records whether it finished cleanly.

## What the process manager owns

The runtime process service owns:

- stable opaque `process_id` values
- durable admission before native spawn
- command, cwd, timeout, and shell mode
- session ownership when provided
- admission, execution-start, and terminal timestamps
- durable queue order, scheduler settings, and launch claims
- exit status and terminal state
- per-stream stdout/stderr capture limits and log files
- sampled output events
- cancellation requests
- terminal completion delivery back to the owning model session
- startup recovery for queued, claimed, running, and completion-pending records

## Start a process

```bash
PROCESS_JSON=$(curl -fsS -X POST "$BASE_URL/v1/processes" \
  "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{
    "session_id": "sess_codex_...",
    "command": "git status --short",
    "cwd": "/path/to/repo"
  }')

PROCESS_ID=$(echo "$PROCESS_JSON" | jq -r '.process.process_id')
```

Admission is durable before the runtime attempts native spawn. The response therefore returns promptly with a stable `process_id`, and the initial status may be `queued`. Queue wait does not consume the execution timeout; timeout starts after the process has actually launched.

If shell execution is enabled in config, command strings run through the configured shell path. Otherwise commands are split into executable and args.

## Durable scheduler

Inspect queue, active launches, and persisted scheduler settings:

```bash
curl -fsS "$BASE_URL/v1/processes/scheduler" "${AUTH[@]}"
```

Update global capacity, per-workspace overrides, capture limits, or pause/resume state:

```bash
curl -fsS -X POST "$BASE_URL/v1/processes/scheduler" \
  "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{
    "max_concurrent": 32,
    "workspace_max_concurrent": {"ws_example": 4},
    "paused": false
  }'
```

Queued work is ordered deterministically and survives restart. Operators can move one queued process relative to another:

```bash
curl -fsS -X POST "$BASE_URL/v1/processes/queue/reorder" \
  "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"process_id":"proc_a","before_process_id":"proc_b"}'
```

## Logs are authoritative

Process output events are intentionally sampled and bounded. They are good for live feedback, not complete archival output. Each stdout/stderr stream is captured up to its persisted capture limit; truncation is reported truthfully when that limit is reached.

For full output, read logs:

```bash
curl "$BASE_URL/v1/processes/$PROCESS_ID/logs" "${AUTH[@]}"
```

This distinction matters. A UI should show output events as a live tail, then use logs for exact inspection.

## Stream process events

```bash
curl "$BASE_URL/v1/processes/$PROCESS_ID/events" "${AUTH[@]}"
curl -N "$BASE_URL/v1/processes/$PROCESS_ID/events/stream" "${AUTH[@]}"
```

Process stream handoff subscribes before replay so live output emitted during reconnect is not lost between backlog and stream.

## Terminal states

A runtime process can finish as:

- `completed`
- `failed`
- `timed_out`
- `killed`
- `canceled`
- `interrupted`

The exact state depends on exit code, timeout, cancellation, and restart reconciliation. Terminal process state and model completion delivery are separate durable facts: a process can already be terminal while its completion turn is still pending delivery.

For a session-owned process, every terminal outcome is delivered back to the owning model through the normal durable turn-admission path. The completion includes status/reason, exit or signal data, bounded stdout/stderr previews, truncation flags, and authoritative log paths. Models should not poll status waiting for completion; `gg_process_run` completion is pushed automatically.

## Kill a process

```bash
curl -X POST "$BASE_URL/v1/processes/$PROCESS_ID/kill" "${AUTH[@]}"
```

Cancellation is tracked by the runtime. Canceling a queued process atomically prevents launch and is intentionally silent to the model because no background execution began. Canceling a running process produces the normal terminal completion delivery.

## Ownership rules

When a process is associated with a session, model-facing access is identity-aware. Status/list visibility follows the caller's workspace, while model cancellation is restricted to the submitting session. Authenticated HTTP endpoints without a `session_id` act as the operator control plane.

This matters for MCP calls. Provider sessions can call process tools through the MCP gateway, but the gateway requires a valid caller session identity and enforces ownership.

## Config knobs

Process behavior is controlled by `[processes]`:

- `enabled`
- `max_concurrent`
- `default_timeout_ms`
- `max_output_bytes_per_process`
- `allow_shell`

See [Configuration reference](/docs/configuration) for exact fields.

## Startup recovery

Queued work remains queued across restart, and an interrupted launch reservation is safely re-queued. For a record that had reached `running`, the runtime only terminates the old process group after matching both the persisted PID and an OS start identity. A PID identity mismatch fails closed rather than risking termination of a reused PID. Successfully reconciled running work becomes `interrupted`, and its terminal completion is delivered through the same durable completion path.

Completion delivery also recovers independently: pending delivery is retried, while an already-dispatched completion turn is recognized by its stable correlation ID and is not duplicated.

Inspect:

```bash
curl "$BASE_URL/v1/diagnostics/processes" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/recovery" "${AUTH[@]}"
```

## MCP process tools

The runtime MCP gateway exposes `gg_process_run`, `gg_process_status`, `gg_process_logs`, and `gg_process_kill`. All model-facing process operations use the stable `process_id`; PID is runtime/diagnostic metadata, not the control handle.

`gg_process_status` is for explicit inspection, not completion polling. `gg_process_logs` reads captured output when the model specifically needs it. Terminal completion is pushed automatically to the owning session.

Use `/v1/mcp/capabilities` to see the active gateway surface:

```bash
curl "$BASE_URL/v1/mcp/capabilities" "${AUTH[@]}"
```
