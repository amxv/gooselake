# AGENTS.md

## Project Working Directory

The canonical working directory for this repo is:
- `~/code/amxv/gooselake`

For future team agents, attach/use this project path as the working directory unless the user explicitly provides a different worktree or path.

## API Docs Sync Expectations

When you touch runtime API behavior, treat docs as part of the same change.

API-impacting edits include (at minimum):
- `crates/runtime-server/src/http/`
- `crates/runtime-server/src/openapi.rs`
- `openapi/runtime-server-openapi.yaml`

When any of these change:
1. Run `make api-docs-refresh` to regenerate the OpenAPI artifact.
2. Review API/doc deltas with `make api-docs-status`.
3. Update the Astro docs source under `src/content/docs/` (usually `src/content/docs/api.md`, and docs index links if needed).
4. Run `make api-docs-check` before finishing.

## Skill For API Doc Sync

Use the local skill at:
- `.agents/skills/runtime-api-doc-sync/SKILL.md`

Invoke it explicitly in prompts when doing API work, for example:
- `Use $runtime-api-doc-sync at .agents/skills/runtime-api-doc-sync to keep docs in sync with this API change.`

## Scope Reminder

The generated OpenAPI in this repo intentionally uses broad `JsonObject` schemas for much of the surface.
Do not assume the OpenAPI artifact fully captures every request/response nuance.
When runtime behavior changes but schema breadth hides it, update narrative docs in `src/content/docs/api.md` anyway.

## Changelog Guidelines

When cutting a release, update `src/content/docs/changelog.md` before tagging.

- Add a new section for the exact version tag being released.
- Keep the newest version at the top.
- Skip versions that do not have git tags.
- Use commit history and diffs on `main` to summarize code changes.
- This is an OSS project, so internal code changes may be included when useful.
- Do not include docs-site-only changes such as site styling, docs package bumps, deploy plumbing, footer/layout changes, or documentation navigation changes.
- Rewrite commit subjects into clear release notes instead of pasting raw commit messages.
- If a release contains only tagging/release metadata, write: `Maintenance release. No direct code behavior changes beyond release preparation.`
