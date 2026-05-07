# API Doc Sync Workflow

Use this when changing runtime API behavior.

## Why This Exists

Route/method coverage in OpenAPI is generated from source parsing, while many request/response shapes remain intentionally broad (`JsonObject`).
That means API docs sync requires both:
- regenerating `openapi/runtime-server-openapi.yaml`
- updating human docs when behavior changes are not fully captured by schema detail

## Command Path

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

## Sync-Relevant Files

API-signal files:
- `crates/runtime-server/src/http.rs`
- `crates/runtime-server/src/openapi.rs`
- `openapi/runtime-server-openapi.yaml`

Docs-signal files:
- `docs/API.md`
- `docs/API_DOC_SYNC.md`
- `docs/README.md`
- `README.md`

## Suggested PR Checklist

1. Regenerate OpenAPI (`make api-docs-refresh`)
2. Inspect API/docs diff (`make api-docs-status`)
3. Update `docs/API.md` for behavior changes
4. Update docs index links if discoverability changed
5. Run sync check (`make api-docs-check`)

## Agent Skill

Reusable local skill:
- `.agents/skills/runtime-api-doc-sync/SKILL.md`

Example invocation in an agent prompt:
- `Use $runtime-api-doc-sync at .agents/skills/runtime-api-doc-sync to keep docs synced with this API change.`
