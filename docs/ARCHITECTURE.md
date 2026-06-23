# Architecture

`gg-runtime-server` is the HTTP/SSE control plane around runtime-core.

## Components

- `crates/runtime-server`:
  - bootstraps providers/runtime services
  - exposes HTTP/SSE API surface
- `crates/runtime-core`:
  - session lifecycle
  - event stream/replay
  - team/process/worktree orchestration
- `crates/runtime-provider-codex`:
  - Codex adapter runtime
- `crates/runtime-provider-claude`:
  - Claude adapter runtime + bridge integration
- `crates/runtime-provider-acp`:
  - ACP adapter runtime over stdio
- `sidecars/claude-bridge`:
  - Claude SDK bridge process
- `sidecars/gg-mcp-server`:
  - GG MCP server process

## Provider Auth Model

- Codex:
  - expects local `codex login` on machine
  - runtime can stage auth material from `~/.gg/codex/auth.json`
- Claude:
  - default: `host_machine` (use machine login material)
  - optional: `runtime_managed` (runtime-owned staged/imported files)
- ACP:
  - configured ACP agent command over stdio
  - auth is agent-managed in the first landing
  - runtime exposes status only via `GET /v1/providers/acp/auth/status`
  - ACP `session/request_permission` is unsupported in v1 and fails the active turn clearly

## Data + State

Configured by `data.root_dir` in `runtime-server.toml`:

- SQLite state
- provider runtime directories
- process logs
- generated auth token file (if `auth.token` omitted)

## Why Sidecars

- Claude bridge isolates SDK/runtime behavior from the core server process.
- MCP server enables team/runtime tool surface via a stable process boundary.
- ACP does not add a runtime-owned sidecar in the first release; the runtime launches the configured ACP agent command directly over stdio.
