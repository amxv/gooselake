---
title: "Usage model"
description: "Operate the workspace-first runtime through durable HTTP operations, scoped model tools, and replayable events."
order: 20
category: "Client Builders"
summary: "How thin clients register workspaces, manage agents, handle accepted work, and recover after reconnect."
---

Gooselake has two complementary channels: **HTTP operations** admit intent, and **durable records/events** tell a client what really happened. A successful request is not proof that a native provider effect or queued message delivery has finished.

## Canonical workspace workflow

1. Register an existing canonical directory with `POST /v2/workspaces`. Repeating the same root converges on one workspace ID.
2. Create agents through `POST /v2/workspaces/{workspace_id}/agents`. Each agent is automatically on that workspace's team roster, with a stable `sess_*` ID and friendly alias; there is no manual join.
3. Check the new agent's mandatory onboarding via `GET /v2/workspaces/{workspace_id}/agents/{agent_id}/onboarding`. Normal user turns cannot bypass an onboarding admission fence.
4. Send an ordered user input array via `POST /v2/agents/{agent_id}/turns` after onboarding. Track its runtime-owned logical turn ID, not the provider's native turn reference.
5. Rebuild status from GET/history pages, then follow `/v2/events/stream` or workspace/agent/process event streams; preserve the returned cursor across reconnects.

For concrete `curl` commands and credentials, begin with [Setup](/docs/setup) and the [API guide](/docs/api).

## Operator HTTP versus model-facing tools

An operator Bearer token can manage workspace authority: nullable lead, provider/agent lifecycle, messages/deliveries, process history, worktree inventory, revisioned policy and manual-review recovery. Operator commands that can outlive the response return an operation coordinate; inspect `GET /v2/operations/{operation_id}` before retrying an uncertain native effect.

An agent's model sees only the enabled `gg_team`, `gg_message`, `gg_process` control plane. The trusted runtime binds its identity; model JSON cannot supply `sender_agent_id`, caller token, workspace or team authority. Direct messages can cross workspaces; broadcasts cannot. Process completions are injected automatically, so model-side process polling is unnecessary.

Claude/ACP reach these tools through per-agent scoped GG MCP; Codex uses native app-server dynamic-tool calls. `/v2/mcp/*` credentials and operator `/v2/*` tokens are **not interchangeable**.

## Design for asynchronous work

A native-effect mutation can be `requested`, `in_progress`, `completed` or in an explicit reconciliation state. A retry with the same valid `Idempotency-Key` and normalized body returns the original receipt; a changed request conflicts. A lost response **after** an uncertain provider dispatch must not be treated like a harmless network retry. Check operation/effect evidence and follow the documented manual-review route.

Provider-originated approvals attach to admitted turns, separately from optional runtime pre-dispatch gates. Treat approvals, onboarding, messages, process completions and compaction as durable state with distinct input provenance.

## Build a thin client

A well-behaved client:

- Keeps workspace IDs, opaque agent IDs, operation IDs and event cursors; does **not** persist provider-native session refs as its own authority.
- Lists and paginates canonical `/v2` records; respects `X-Next-Cursor` for keyset list responses and JSON `next_cursor` for the message-list response.
- Displays messages **with per-recipient delivery state** and separate retry/cancel results; never assumes “message created” means “recipient saw it.”
- Shows provider-supported capabilities from `GET /v2/capabilities` rather than assuming every provider has native image/skill input, model discovery, edit-rerun, compaction or rebind.
- Reads authoritative process log files for complete stdout/stderr; streaming previews can be bounded.
- Exposes recovery/manual-review states instead of silently replaying dangerous external work.

`GET /v2/workspaces/{workspace_id}/agents` and `gg_team status` have bounded results; missing provider-bound context observations are `null`, not a guess. Worktree reassignment completes only with provider evidence of the effective cwd.

## Reconnect and legacy compatibility

SSE uses `after_seq` or `Last-Event-ID` to replay from the durable journal before live events. Global stream cursors are journal row IDs; scoped streams use per-resource sequence. Streams are **not** keyset list pages. In-process commit wakeups do not by themselves promise cross-process replication.

The historical `/v1/sessions`, `/v1/teams`, and related tools are operator-facing compatibility surfaces during migration; new clients should not build a second independent team authority around them. Legacy unresolved sessions stay explicit until mapped/archived.

**Design test:** if your UI disappears, could a second client reconstruct what happened from runtime records and cursor replay? If not, keep less authoritative state in the UI.

See [Client design](/docs/client-design), [Teams](/docs/teams), [Events and recovery](/docs/events-and-recovery), and [Security](/docs/security).
