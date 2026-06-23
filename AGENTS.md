# AGENTS.md

## Project Working Directory

The canonical working directory for this repo is:
- `~/code/amxv/gg-agent-runtime`

For future team agents, attach/use this project path as the working directory unless the user explicitly provides a different worktree or path.

## API Docs Sync Expectations

When you touch runtime API behavior, treat docs as part of the same change.

API-impacting edits include (at minimum):
- `crates/runtime-server/src/http.rs`
- `crates/runtime-server/src/openapi.rs`
- `openapi/runtime-server-openapi.yaml`

When any of these change:
1. Run `make api-docs-refresh` to regenerate the OpenAPI artifact.
2. Review API/doc deltas with `make api-docs-status`.
3. Update docs under `docs/` (usually `docs/API.md`, and docs index links if needed).
4. Run `make api-docs-check` before finishing.

## Skill For API Doc Sync

Use the local skill at:
- `.agents/skills/runtime-api-doc-sync/SKILL.md`

Invoke it explicitly in prompts when doing API work, for example:
- `Use $runtime-api-doc-sync at .agents/skills/runtime-api-doc-sync to keep docs in sync with this API change.`

## Scope Reminder

The generated OpenAPI in this repo intentionally uses broad `JsonObject` schemas for much of the surface.
Do not assume the OpenAPI artifact fully captures every request/response nuance.
When runtime behavior changes but schema breadth hides it, update narrative docs in `docs/API.md` anyway.
