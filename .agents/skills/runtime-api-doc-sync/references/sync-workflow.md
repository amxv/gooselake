# Runtime API Doc Sync Reference

## Commands

- `make api-docs-refresh`: Regenerate `openapi/runtime-server-openapi.yaml` from runtime server route sources.
- `make api-docs-status`: Show `git status --short` for API-signal files and docs-signal files.
- `make api-docs-check`: Fail if API-signal files changed but docs-signal files did not.

## Signal Files

API-signal files:
- `crates/runtime-server/src/http.rs`
- `crates/runtime-server/src/openapi.rs`
- `openapi/runtime-server-openapi.yaml`

Docs-signal files:
- `docs/API.md`
- `docs/API_DOC_SYNC.md`
- `docs/README.md`
- `README.md`

## Guardrail

OpenAPI generation is intentionally broad for many request/response payloads (`JsonObject`).
When behavior changes are not fully reflected in the schema snapshot, still update narrative docs in `docs/API.md`.
