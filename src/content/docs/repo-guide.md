---
title: "Repo guide"
description: "Use the repository layout and existing docs to move from orientation into implementation and operations."
order: 80
category: "Reference"
summary: "Where to look in the repo when you need code, API behavior, deployment assets, or deeper operational detail."
---

This page is for contributors who need to connect docs to implementation.

## Code layout

| Path | Responsibility |
| --- | --- |
| `crates/runtime-core` | provider contracts, runtime records, session orchestration, team comms, shared service traits |
| `crates/runtime-server` | config, bootstrap, HTTP/SSE routes, diagnostics, OpenAPI generation, server binary |
| `crates/runtime-store-sqlite` | durable SQLite implementation for runtime records/events |
| `crates/runtime-provider-codex` | Codex provider adapter |
| `crates/runtime-provider-claude` | Claude provider adapter and bridge integration |
| `crates/runtime-provider-acp` | ACP stdio provider adapter |
| `crates/runtime-tools` | process manager, MCP gateway, worktree service, team spawn service |
| `sidecars/claude-bridge` | Claude JSON-lines bridge sidecar |
| `sidecars/gg-mcp-server` | MCP sidecar that calls back into the runtime gateway |
| `examples/runtime-server.toml` | complete config reference example |
| `openapi/runtime-server-openapi.yaml` | generated OpenAPI snapshot |
| `src/content/docs` | canonical docs source for site and release-bundle Markdown docs |
| `scripts` | install, upgrade, deploy, package, preflight, and API-doc-sync scripts |

## Implementation reading order

For the core runtime path:

1. `crates/runtime-core/src/provider.rs`
2. `crates/runtime-core/src/state.rs`
3. `crates/runtime-core/src/runtime.rs`
4. `crates/runtime-store-sqlite/src/lib.rs`
5. `crates/runtime-server/src/bootstrap.rs`
6. `crates/runtime-server/src/http/`

For team/worktree/process services:

1. `crates/runtime-core/src/team_comms.rs`
2. `crates/runtime-tools/src/lib.rs`
3. related HTTP handlers in `crates/runtime-server/src/http/`

For provider behavior:

1. `crates/runtime-core/src/provider.rs`
2. the relevant `crates/runtime-provider-*` crate
3. sidecars when provider behavior crosses a process boundary

## High-value docs

- [Architecture](/docs/architecture): system map.
- [Runtime lifecycle](/docs/runtime-lifecycle): session/turn flow.
- [Events and recovery](/docs/events-and-recovery): event/replay/recovery invariants.
- [Teams and comms](/docs/teams): durable team coordination.
- [Processes](/docs/processes): host process service.
- [Worktrees](/docs/worktrees): workspace ownership and cleanup.
- [MCP and sidecars](/docs/mcp-and-sidecars): bridge boundaries.

## API change rule

If a change touches runtime API behavior, treat docs as part of the change.

Start with:

```bash
make api-docs-refresh
make api-docs-status
```

Then update human docs under `src/content/docs/` when behavior changed in ways the generated OpenAPI snapshot cannot fully express.

Finish with:

```bash
make api-docs-check
```

## Practical grep targets

Useful searches:

```bash
rg "route\(" crates/runtime-server/src/http/mod.rs
rg "RuntimeProvider" crates/runtime-core crates/runtime-provider-*
rg "append_runtime_event" crates
rg "StartupRecovery" crates
rg "RuntimeTeamCommsService" crates
rg "RuntimeProcessManager" crates
rg "RuntimeWorktreeService" crates
rg "RuntimeToolGateway" crates
```

These searches map docs concepts back to code quickly.
