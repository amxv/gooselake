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

## Gooseweb Contributor Runbook

Gooseweb is a client for Gooselake/Goosetower, not a standalone mock UI. Product-facing state flows through this chain:

```text
runtime command/API -> Goosetower materializer -> Gooseweb realtime view -> React UI
```

Do not assume a successful runtime command means Gooseweb will update. If a UI action mutates sessions, teams, team messages, processes, source health, model capabilities, worktrees, or approvals, verify the relevant Goosetower materialized entity is refreshed and patched to Gooseweb. A correct fix often lives in Goosetower materialization/gateway code rather than the React component.

For clean Gooseweb QA, prefer an isolated stack with its own dev directory and ports:

```bash
DEV_DIR="$PWD/tmp/gooseweb-qa-stack" \
  make dev DEV_RUNTIME_PORT=18087 DEV_GOOSETOWER_PORT=18097 DEV_GOOSEWEB_PORT=13007
```

Use this for first-run flows, empty-state behavior, and message visibility tests. It avoids depending on or damaging the default `tmp/gooseweb-dev/` state.

### Gooseweb Browser QA

Use a named `agent-browser` session for every browser pass. Do not run `agent-browser close --all` unless the user explicitly asks; other headed sessions may belong to the user.

Save browser evidence under a feature-specific `tmp/` directory, for example:

```bash
tmp/gooseweb-team-setup-qa/
tmp/gooseweb-message-visibility-qa/
```

Minimum QA for Gooseweb UI changes:

- Test the user workflow through real UI clicks, not only HTTP/API calls.
- Capture desktop evidence, then check at least `820x1000` and `520x900`.
- Verify no document-level horizontal overflow: `document.documentElement.scrollWidth === document.documentElement.clientWidth`.
- For composer/workspace surfaces, measure that the composer bottom is inside the viewport.
- For Team Comms, verify both runtime state and visible stream text. A message that is stored in runtime but not visible in Gooseweb is a failed workflow.
- Check the browser console/network when behavior is surprising.

### Gooseweb Design Direction

Preserve the accepted operator UI direction:

- High-level views live in the top header navigation.
- The left sidebar is for active agents, sessions, teams, and operational actions.
- The Agents center pane is the selected agent thread and composer, not a metrics dashboard.
- Team Comms should read like a chronological chat/control stream.
- Avoid reintroducing verbose empty placeholders, center metric cards, nested timeline panels, permanent right rails in the default Agents view, or old left-sidebar view tabs.
- When matching desktop-app behavior, prefer the existing patterns for roster rows, prompt composer, process cards, tool cards, thinking blocks, settings panels, changes/commits inspectors, and Team Comms.

### Gooseweb Fixture Convention

Dev-only visual fixtures are allowed for QA when live runtime data is unavailable, but they must be guarded by both `import.meta.env.DEV` and an explicit query parameter.

Common fixture query parameters include:

- `goosewebThreadFixture=1`
- `goosewebRosterFixture=1`
- `goosewebCommitsFixture=1`
- `goosewebProcessFixture=1`
- `goosewebChangesFixture=1`
- `goosewebMarkdownFixture=1`
- `goosewebTodosFixture=1`
- `goosewebContextFixture=1`
- `goosewebComposerAttachmentFixture=1`
- `goosewebNotificationFixture=1`
- `goosewebModelPresetFixture=1`

Always verify the default non-fixture path has no fixture text or fake data leakage.

### Gooseweb Source-Of-Truth Rules

- Model and reasoning options come from runtime provider catalogs, then Goosetower source-health materialization, then Gooseweb `modelCapabilities`. Do not hardcode provider reasoning levels in the UI.
- Prompt-composer image attachments should be sent as structured native image input parts, not markdown paths or local filesystem strings.
- Team message sends must be visible in Team Comms after the relevant source/team materialization refresh.
- Settings that are currently local-only should be clearly scoped as Gooseweb preferences until a Gooselake preferences API exists.

### Gooseweb Validation Matrix

Choose checks by blast radius:

- CSS-only polish: `bun run --cwd apps/gooseweb typecheck`; add build if the source graph or generated CSS path changed.
- React/UI behavior: `bun run --cwd apps/gooseweb typecheck` and usually `bun run --cwd apps/gooseweb build`.
- Gooseweb protocol or worker behavior: `bun run --cwd apps/gooseweb test`.
- Protobuf changes: `bun run proto`, then Gooseweb typecheck and relevant Rust checks.
- Goosetower gateway/materializer changes: focused `cargo test -p goosetower <filter>`, `cargo check -p goosetower`, and `cargo fmt --check`.
- Broad Rust changes before shipping: `make check`.

Known warning baseline:

- Vite may warn that `astro/tsconfigs/strict` is missing from the root tsconfig chain.
- Streamdown/Mermaid/Shiki may produce large chunk warnings.
- TanStack external packages may emit existing unused import warnings during build.

Treat these as non-blocking only when the command exits successfully and the task did not target those warnings.

## Repo-Wide Validation

Use the repo-wide check target before pushing broad Rust changes:

```bash
make check
```

This runs the Rust file-length lint, `cargo fmt --check`, workspace check/test, standalone `gg-mcp-server` check/test, and the API docs sync check.

The Rust file-length lint is a shipping gate. Keep every Rust source file at or below 1000 lines. If a file grows past the limit, prefer mechanical module extraction that preserves public module paths with re-exports.

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
