---
title: API reference
description: Use the HTTP and SSE API for sessions, events, providers, teams, processes, worktrees, diagnostics, and MCP gateway calls.
order: 8
category: Reference
summary: The practical API map for building clients on top of the runtime.
---

## Auth

`GET /health` and `GET /openapi.yaml` are public. Every `/v1/**` route requires:

```bash
Authorization: Bearer <token>
```

## Sessions

```bash
SESSION_JSON=$(curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"provider":"codex","model":"gpt-5.4-mini"}' \
  "$BASE_URL/v1/sessions")
```

Send a turn:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"input":[{"type":"text","text":"Reply with ok."}]}' \
  "$BASE_URL/v1/sessions/$SESSION_ID/turns"
```

## Streams

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/sessions/$SESSION_ID/events/stream?after_seq=0"
```

Streams replay first and then continue live. `after_seq` takes precedence over `Last-Event-ID`.

## Main route families

- `/v1/providers/*`
- `/v1/sessions/*`
- `/v1/events*`
- `/v1/teams/*`
- `/v1/processes/*`
- `/v1/worktrees/*`
- `/v1/diagnostics*`
- `/v1/mcp/*`

The detailed API guide is `docs/API.md`; the full route catalog is `docs/API_ENDPOINTS.md`; generated OpenAPI is served at `/openapi.yaml` and `/v1/openapi.yaml`.
