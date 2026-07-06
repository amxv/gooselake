---
name: runtime-api-doc-sync
description: Keep GG Runtime API documentation synchronized with route/code changes and OpenAPI artifact updates. Use when editing runtime HTTP routes, OpenAPI generation logic, or API-facing behavior so `openapi/` and `src/content/docs/` stay aligned.
---

# Runtime API Doc Sync

Execute this workflow whenever API behavior changes.

## 1) Identify API-impacting changes

Prioritize these files:
- `crates/runtime-server/src/http/`
- `crates/runtime-server/src/openapi.rs`
- `openapi/runtime-server-openapi.yaml`

Also consider adjacent API behavior changes that may not fully appear in the broad generated schema.

## 2) Refresh generated OpenAPI

Run:

```bash
make api-docs-refresh
```

This regenerates `openapi/runtime-server-openapi.yaml` using `--write-openapi`.

## 3) Review API/docs diff together

Run:

```bash
make api-docs-status
```

Inspect API and docs files in one place before finalizing edits.

## 4) Update narrative docs

Update the Astro docs source under `src/content/docs/` to reflect real behavior changes.
At minimum, verify:
- `src/content/docs/api.md`
- `src/content/docs/endpoint-catalog.md`
- `src/content/docs/api-doc-sync.md`
- `src/content/docs/overview.md`

If discoverability changed, also update root `README.md` docs links.

## 5) Run the sync gate

Run:

```bash
make api-docs-check
```

If API files changed without docs changes, this command fails and tells you to update docs.

## Reference

For command behavior and policy details, read:
- `references/sync-workflow.md`
