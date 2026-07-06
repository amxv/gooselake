# Runtime API Doc Sync Reference

## Commands

- `make api-docs-refresh`: Regenerate `openapi/runtime-server-openapi.yaml` from runtime server route sources.
- `make api-docs-status`: Show `git status --short` for API-signal files and docs-signal files.
- `make api-docs-check`: Fail if API-signal files changed but docs-signal files did not.

## Signal Files

API-signal files:
- `crates/runtime-server/src/http/`
- `crates/runtime-server/src/openapi.rs`
- `openapi/runtime-server-openapi.yaml`

Docs-signal files:
- `src/content/docs/api.md`
- `src/content/docs/endpoint-catalog.md`
- `src/content/docs/api-doc-sync.md`
- `src/content/docs/overview.md`
- `README.md`

## Guardrail

OpenAPI generation is intentionally broad for many request/response payloads (`JsonObject`).
When behavior changes are not fully reflected in the schema snapshot, still update narrative docs in `src/content/docs/api.md`.
