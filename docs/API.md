# API Guide

This document is the human-facing reference for the runtime HTTP/SSE API.

Source of truth for this guide:
- generated artifact: [`openapi/runtime-server-openapi.yaml`](../openapi/runtime-server-openapi.yaml)
- route + handler code: [`crates/runtime-server/src/http.rs`](../crates/runtime-server/src/http.rs)
- OpenAPI generator implementation: [`crates/runtime-server/src/openapi.rs`](../crates/runtime-server/src/openapi.rs)

If this guide disagrees with runtime behavior, treat server code as authoritative.

## Quick API Start

```bash
BASE_URL="http://127.0.0.1:8080"
TOKEN="<runtime-bearer-token>"

curl -fsS "$BASE_URL/health"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/health"
```

## Authentication Model

Public routes (no bearer required):
- `GET /health`
- `GET /openapi.yaml`

All `/v1/**` routes require:
- `Authorization: Bearer <token>`

On auth failure, runtime returns:
- HTTP `401`
- JSON body: `{"error":"missing or invalid bearer token"}`

## API Surface By Group

Reference: [Endpoint Catalog](./API_ENDPOINTS.md)

Top-level groups:
- Runtime/meta: health, version, OpenAPI, diagnostics
- Providers/auth: provider list/models plus Codex, Claude, and ACP auth endpoints
- Sessions: create/list/get/resume/close, turns, approvals
- Teams/comms: team lifecycle, message delivery, retries, snapshots, interrupts
- Processes: run/list/get/logs/kill, process event replay + streaming
- Worktrees: create/list/get/claim/release/cleanup
- Runtime events: global replay + global stream
- MCP gateway: capabilities + invoke

## Provider Notes

The runtime now exposes three provider identities:
- `codex`
- `claude`
- `acp`

Current ACP v1 behavior is intentionally narrow:
- `GET /v1/providers/acp/auth/status` is the only ACP auth route in the first landing.
- ACP auth remains agent-managed. The runtime reports readiness/config state but does not expose ACP logout, API-key, JSON import, or file import mutations in v1.
- `GET /v1/providers/acp/models` may return an empty list. ACP model selection can be session-config driven by the configured ACP agent rather than a provider-global catalog.
- ACP permission requests are unsupported in v1. If an ACP agent issues `session/request_permission` during a turn, the runtime fails that active turn with a clear unsupported error instead of creating an approval flow.

## SSE and Replay Model

SSE endpoints:
- `GET /v1/events/stream`
- `GET /v1/sessions/{session_id}/events/stream`
- `GET /v1/teams/{team_id}/events/stream`
- `GET /v1/processes/{process_id}/events/stream`

Behavior:
- stream endpoints are replay-first, then live events
- server subscribes before replay handoff on session/process streams to reduce missed events during handoff
- replay cursor is chosen from:
  - `after_seq` query param (takes precedence)
  - otherwise `Last-Event-ID` header
- keepalive pings are sent every 10 seconds

SSE event envelope:
- `id`: runtime sequence id
- `event`: runtime event kind
- `data`: JSON-serialized event payload

Cursor constraints:
- `Last-Event-ID` must parse as a non-negative integer
- invalid header value yields HTTP `400`

### SSE Example

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/sessions/$SESSION_ID/events/stream?after_seq=0"
```

## Request/Response Shape Reality

Current generated OpenAPI intentionally uses broad schemas for most bodies:
- request/response bodies are usually `JsonObject` (`type: object`, `additionalProperties: true`)
- only a few special cases are typed in schema shape (for example multipart upload form)

Implication:
- OpenAPI is accurate for endpoint/method coverage and basic content types
- exact JSON field-level contracts should be confirmed from handler structs in `http.rs` and runtime core types

## Common Workflows

### 1) Create a session and send a turn

```bash
SESSION_JSON=$(curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"provider":"codex"}' \
  "$BASE_URL/v1/sessions")

SESSION_ID=$(echo "$SESSION_JSON" | jq -r '.id')

curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"input":[{"type":"text","text":"Run ls"}]}' \
  "$BASE_URL/v1/sessions/$SESSION_ID/turns"
```

### 2) Replay then stream global runtime events

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/events?after_seq=0&limit=200"

curl -N -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/events/stream?after_seq=200"
```

### 3) Start and inspect a runtime-managed process

```bash
PROCESS_JSON=$(curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"command":"echo hello","timeout_ms":30000}' \
  "$BASE_URL/v1/processes")

PROCESS_ID=$(echo "$PROCESS_JSON" | jq -r '.id')

curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/processes/$PROCESS_ID"

curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/processes/$PROCESS_ID/logs?tail_lines=100"
```

### 4) Check provider auth status

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/providers/codex/auth/status"

curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/providers/acp/auth/status"

curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/providers/claude/auth/status"
```

## OpenAPI Generation and Sync Expectations

How OpenAPI is produced today:
- runtime serves OpenAPI from `generated_openapi_yaml()` in `openapi.rs`
- generator parses route declarations in `http.rs` source
- generation is source-parsing based, not runtime route introspection

Regenerate artifact in repo:

```bash
make api-docs-refresh
```

Review sync-relevant file changes:

```bash
make api-docs-status
```

Fail fast when API files changed but docs files did not:

```bash
make api-docs-check
```

Workflow reference: [API Doc Sync Workflow](./API_DOC_SYNC.md)

What this gives you reliably:
- route path coverage
- HTTP method coverage
- path params
- basic request content type for known POST endpoints
- SSE vs JSON response content type

What may remain coarse until further typing is added:
- concrete JSON field schemas for request/response bodies
- query parameter schemas
- non-200/non-204 status coverage (for example `202`, `400`, `401`, `404`, `500`)

## Error Mapping (Current Behavior)

Common behavior in handlers:
- validation errors: HTTP `400`, body `{"error":"..."}`
- not found / unknown entities: HTTP `404`, body `{"error":"..."}`
- internal/io/bootstrap errors: HTTP `500`, body `{"error":"..."}`

Special status codes used by specific endpoints:
- `POST /v1/sessions/{session_id}/turns/{turn_id}/interrupt` returns `202 Accepted`
- `DELETE /v1/teams/{team_id}` returns `204 No Content`

## Runtime MCP Notes

MCP routes:
- `GET /v1/mcp/capabilities`
- `POST /v1/mcp/invoke`

Important constraints from server code:
- request body limit for MCP routes: `64 KiB`
- invoke requires non-empty `caller_agent_id` and `tool_name`
- invoke rejects closed/failed caller sessions with `400`
