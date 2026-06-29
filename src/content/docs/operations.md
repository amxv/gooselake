---
title: Operations
description: Run health checks, inspect recovery, follow service logs, debug providers, and manage runtime-owned work.
order: 9
category: Reference
summary: Day-two commands for running Gooselake as an operator rather than just launching it once.
---

## Health checks

```bash
curl -fsS "$BASE_URL/health"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/health"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/diagnostics"
```

## Service logs

```bash
systemctl --user status gg-runtime.service
journalctl --user -u gg-runtime.service -f
```

Use the system-level commands if deployed as a system service.

## Recovery

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/diagnostics/recovery"
```

Recovery reconciles sessions, turns, approvals, deferred deliveries, provider health, and process records after restart.

## Provider checks

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/diagnostics/providers"
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/providers/claude/auth/status"
```

Provider failures usually point to missing host credentials, bad service environment, bad sidecar paths, or bad ACP command config.

## Day-two docs

Use `docs/OPERATIONS.md` for full runbooks covering sessions, SSE replay, processes, worktrees, teams, deliveries, recovery, and API/doc sync.
