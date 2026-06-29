---
title: Repo guide
description: Use the repository layout and existing docs to move from orientation into implementation and operations.
order: 6
category: Operator Workflows
summary: Where to look in the repo when you need code, API behavior, deployment assets, or deeper operational detail.
---

## Code layout

- `crates/runtime-core` defines provider contracts, runtime records, session orchestration, and team comms.
- `crates/runtime-server` owns config, bootstrap, HTTP/SSE routes, diagnostics, and OpenAPI generation.
- `crates/runtime-store-sqlite` persists runtime records.
- `crates/runtime-provider-*` implement provider adapters.
- `crates/runtime-tools` implements process, worktree, MCP gateway, and spawn services.
- `sidecars/claude-bridge` isolates Claude SDK/CLI behavior.
- `sidecars/gg-mcp-server` exposes MCP tools that call back into the runtime gateway.
- `src/content/docs` is the canonical docs source for the Astro website and release-bundle Markdown docs.

## High-value docs

- [Install](/docs/install) for local install and release install.
- [Configuration](/docs/configuration) for every config section and environment override.
- [Providers](/docs/providers) for Codex, Claude, and ACP setup.
- [API guide](/docs/api) and [Endpoint catalog](/docs/endpoint-catalog) for HTTP/SSE usage.
- [Operations](/docs/operations) for health checks, recovery, process/worktree/team runbooks.
- [Architecture](/docs/architecture) for internal structure.
- [MCP and sidecars](/docs/mcp-and-sidecars) for bridge and MCP boundaries.

## Implementation reading order

1. Start at `crates/runtime-server/src/bootstrap.rs` to see how the app is composed.
2. Read `crates/runtime-core/src/provider.rs` for the provider contract.
3. Read `crates/runtime-core/src/runtime.rs` for session/turn orchestration.
4. Read `crates/runtime-core/src/team_comms.rs` for team message delivery behavior.
5. Read `crates/runtime-tools/src/lib.rs` for processes, worktrees, and spawn flows.
6. Read the provider crate you are changing.
7. Read `crates/runtime-server/src/http.rs` for API shape.

## API change rule

When route behavior changes, update code, OpenAPI, and human docs together:

```bash
make api-docs-refresh
make api-docs-status
make api-docs-check
```

OpenAPI alone is not enough because many JSON schemas are intentionally broad today.
