# AGENTS.md

## Project Working Directory

The canonical working directory for this repo is:
- `~/code/amxv/gooselake`

For future team agents, attach/use this project path as the working directory unless the user explicitly provides a different worktree or path.

## Local Gooseweb Live Dev Stack

Use the single repo target to run the local Gooselake runtime, Goosetower gateway, and Gooseweb app together:

```bash
make dev
```

Equivalent Bun alias:

```bash
bun run dev:gooseweb
```

Default local endpoints:

- Gooseweb: `http://127.0.0.1:13001`
- Goosetower: `http://127.0.0.1:18090`
- Runtime server: `http://127.0.0.1:18080`

`make dev` runs `scripts/dev-gooseweb-stack.sh`, writes generated local configs under `tmp/gooseweb-dev/`, starts all three processes, waits for HTTP readiness, and stops all child processes on `Ctrl-C`.

Before starting, `make dev` automatically stops existing listeners on the requested runtime, Goosetower, and Gooseweb ports. This lets agents restart a crashed or stale dev stack without manually running `lsof` and `kill`. To keep the old fail-fast behavior, run:

```bash
GOOSEWEB_DEV_AUTO_STOP_PORTS=false make dev
```

Common overrides:

```bash
make dev DEV_GOOSEWEB_PORT=13002
make dev DEV_RUNTIME_PORT=18081 DEV_GOOSETOWER_PORT=18091 DEV_GOOSEWEB_PORT=13002
```

Use `make gooseweb-dev` only when runtime and Goosetower are already running; it starts just the Vite app pointed at `DEV_GOOSETOWER_PORT` and `DEV_GOOSEWEB_PORT`.

When starting `make dev` as an agent, use the long-running process tool (`gg_process_run`) so the stack can stay alive while browser QA continues. Re-running `make dev` on the same ports is acceptable; it will stop stale listeners first. Override ports when you need two independent stacks at the same time.

## Repo-Wide Validation

Use the repo-wide check target before pushing broad Rust changes:

```bash
make check
```

This runs the Rust file-length lint, `cargo fmt --check`, workspace check/test, standalone `gg-mcp-server` check/test, and the API docs sync check.

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
