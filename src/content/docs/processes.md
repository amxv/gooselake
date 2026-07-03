---
title: "Processes"
description: "Run host commands through the runtime, stream sampled output, read authoritative logs, and understand process ownership."
order: 32
category: "Runtime Services"
summary: "Runtime-managed command execution for agents and operators."
---

Processes are Gooselake's bridge from agent intent to host execution. They let the runtime start commands, track lifecycle, capture logs, stream sampled output, enforce concurrency, and tie work back to a session.

Think of the process manager as a supervised workshop bench: the runtime starts the tool, labels the job, captures the debris, and records whether it finished cleanly.

## What the process manager owns

The runtime process service owns:

- process IDs
- command, args, cwd, and shell mode
- session ownership when provided
- start/end timestamps
- exit status and terminal state
- stdout/stderr log files
- sampled output events
- kill requests
- startup recovery for records left running

## Start a process

```bash
curl -X POST "$BASE_URL/v1/processes"   "${AUTH[@]}"   -H 'Content-Type: application/json'   -d '{
    "session_id": "sess_codex_...",
    "command": "git status --short",
    "cwd": "/path/to/repo"
  }'
```

If shell execution is enabled in config, shell-mode commands can run through `sh -lc`. Otherwise commands are split into executable and args.

## Logs are authoritative

Process output events are intentionally sampled and bounded. They are good for live feedback, not complete archival output.

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

The exact state depends on exit code, timeout, and kill requests.

## Kill a process

```bash
curl -X POST "$BASE_URL/v1/processes/$PROCESS_ID/kill" "${AUTH[@]}"
```

Killing is tracked by the runtime. Do not model it as a client-side UI action only.

## Ownership rules

When a process is associated with a session, process ownership is enforced. A caller cannot use runtime process tools to inspect or control a process owned by another session.

This matters for MCP calls. Provider sessions can call process tools through the MCP gateway, but the gateway requires a valid caller session identity and enforces ownership.

## Config knobs

Process behavior is controlled by `[processes]`:

- `enabled`
- `max_concurrent`
- `default_timeout_secs`
- `max_output_bytes`
- `allow_shell`
- log retention settings

See [Configuration reference](/docs/configuration) for exact fields.

## Startup recovery

If the runtime restarts while records are `running` or `queued`, those records are marked failed during startup recovery. The new runtime process cannot safely assume ownership of an old child process from the previous server instance.

Inspect:

```bash
curl "$BASE_URL/v1/diagnostics/processes" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/recovery" "${AUTH[@]}"
```

## MCP process tools

The runtime MCP gateway currently exposes process tools through the `gg_process` namespace. It does not currently expose every team/worktree runtime operation as a gateway tool. Those services are still available through HTTP APIs.

Use `/v1/mcp/capabilities` to see the active gateway surface:

```bash
curl "$BASE_URL/v1/mcp/capabilities" "${AUTH[@]}"
```
