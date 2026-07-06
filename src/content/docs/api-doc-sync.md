---
title: "API doc sync workflow"
description: "Keep the generated OpenAPI artifact and human API docs synchronized when runtime route behavior changes."
order: 81
category: "Reference"
summary: "The guardrail for API changes that need documentation changes."
---

Use this when changing runtime API behavior.

## Why this exists

Route/method coverage in OpenAPI is generated from source parsing, while many request/response shapes remain intentionally broad (`JsonObject`). API docs sync therefore requires both:

- regenerating `openapi/runtime-server-openapi.yaml`
- updating human docs when behavior changes are not fully captured by schema detail

## Command path

From repo root:

```bash
make api-docs-refresh
make api-docs-status
make api-docs-check
```

What each command does:

- `api-docs-refresh`: runs `./scripts/api-doc-sync.sh refresh` and regenerates the OpenAPI artifact
- `api-docs-status`: runs `./scripts/api-doc-sync.sh status` and prints API/doc sync-relevant file status
- `api-docs-check`: runs `./scripts/api-doc-sync.sh check` and fails if API files changed without doc file changes

## Sync-relevant files

API-signal files:

- `crates/runtime-server/src/http/`
- `crates/runtime-server/src/openapi.rs`
- `openapi/runtime-server-openapi.yaml`

Docs-signal files:

- `README.md`
- `/docs/overview`
- `/docs/api`
- `/docs/endpoint-catalog`
- `/docs/api-doc-sync`
- `/docs/architecture`
- `/docs/configuration`
- `/docs/providers`
- `/docs/operations`
- `/docs/mcp-and-sidecars`
- `src/content/docs/*.md`

## Suggested PR checklist

1. Regenerate OpenAPI (`make api-docs-refresh`).
2. Inspect API/docs diff (`make api-docs-status`).
3. Update `/docs/api` for route behavior changes.
4. Update `/docs/endpoint-catalog` for endpoint/query/auth changes.
5. Update deeper narrative docs when behavior affects configuration, providers, architecture, MCP, or operations.
6. Update website docs under `src/content/docs/` when user-facing onboarding changes.
7. Run sync check (`make api-docs-check`).
8. Run the relevant Rust/site checks for the changed area.

## What OpenAPI captures well

- route path coverage
- HTTP method coverage
- path parameters
- basic request content type for known POST endpoints
- SSE vs JSON response content type
- public vs bearer-protected grouping via route placement

## What human docs must still explain

- concrete JSON request/response fields when the generated schema is broad
- provider-specific auth/model behavior
- ACP v1 limitations
- SSE replay semantics and cursor precedence
- operational examples and runbooks
- process/worktree ownership rules
- team delivery policy and recovery semantics
- deployment and config pitfalls

## Agent skill

Reusable local skill:

- `.agents/skills/runtime-api-doc-sync/SKILL.md`

Example invocation in an agent prompt:

- `Use $runtime-api-doc-sync at .agents/skills/runtime-api-doc-sync to keep docs in sync with this API change.`
