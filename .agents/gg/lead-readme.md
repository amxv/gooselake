# Gooselake Lead Notes

This file captures repo-local lessons for leads working in Gooselake. It supplements the global lead playbook; do not duplicate global team rules here.

## Validation

- Use `make check` as the broad green gate before pushing integrated Rust work.
- `make check` currently runs:
  - Rust file-length lint
  - `cargo fmt --check`
  - `cargo check --workspace`
  - `cargo test --workspace`
  - standalone `gg-mcp-server` check/test
  - API docs sync check
- For narrow branch validation, prefer package-level checks first. Use `make check` after integration or before push.
- If a reviewer makes no file changes and the implementer already ran the same relevant checks, do not rerun duplicate checks solely because the review happened.

## Large-File Refactor Workflow

- Use the global `$refactor-large-files` skill for future large-file cleanup work.
- Keep the workflow sequential: one oversized file, one implementer, one reviewer, then integrate.
- Require implementers to preserve public import/module paths and report LOC for every touched/resulting source file.
- Send a reviewer to inspect each refactor branch before integration. Reviewers should fix only issues they find; if they do not touch files, they should not create empty commits.
- Do not integrate a large-file branch unless every touched/resulting source file is under the configured limit.

## Rust Refactor Patterns That Worked Here

- For `foo.rs`, prefer `foo/mod.rs` plus focused sibling modules to preserve `crate::foo` imports.
- Keep `mod.rs` as the public/stable boundary and re-export existing public types from there.
- Use `pub(super)` or `pub(crate)` for sibling-module access instead of widening public API.
- Move inline tests into `tests.rs` or `tests/` topic files when test modules are part of the large file.
- For standalone sidecar crates, validate with `cargo check --manifest-path .../Cargo.toml` and `cargo test --manifest-path .../Cargo.toml`; they may not be workspace packages.

## Sidecar Test Harness Lesson

- The `gg-mcp-server` integration tests should not assume `env!("CARGO_BIN_EXE_gg-mcp-server")` always points to a materialized binary in every local layout.
- Keep the explicit binary target in `sidecars/gg-mcp-server/Cargo.toml`.
- Keep the integration-test binary resolver fallback that checks the standalone sidecar target path derived from `CARGO_MANIFEST_DIR`.

## Release Flow

- The release matrix should stay intentionally small:
  - Linux x86_64
  - Linux arm64
  - macOS Apple silicon
- Do not add macOS x86_64 release jobs unless the user explicitly asks for that artifact.
- When cutting a release, update `src/content/docs/changelog.md` before tagging.

## API Docs

- Runtime API behavior changes must include the API docs sync workflow.
- Run `make api-docs-refresh`, inspect `make api-docs-status`, update narrative docs when schema breadth hides behavior changes, then run `make api-docs-check`.
- `make check` includes `make api-docs-check`, but it does not replace `api-docs-refresh` when API behavior actually changed.

## Team Hygiene

- Ignore unrelated untracked research/report files unless the user asks to include or clean them.
- Preserve agent commit boundaries during integration. Cherry-pick branches rather than manually restaging product-code diffs on `main`.
- After a branch is green, pushed, and included in `main`, remove only implementation agents that shipped. Do not remove research/review agents by default unless instructed.
