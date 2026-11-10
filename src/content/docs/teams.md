---
title: "Workspace teams and messages"
description: "Coordinate a workspace's automatic agent roster, optional lead, cross-workspace direct messages, local broadcasts, and delivery recovery."
order: 31
category: "Runtime Services"
summary: "The canonical workspace-first collaboration model and its operator/model authority split."
---

A Gooselake **team is the active agent roster of one workspace**. Creating an agent through `POST /v2/workspaces/{workspace_id}/agents` automatically adds it; there is no separate team object or join operation in the canonical API.

Each agent has a stable opaque ID and friendly alias. A workspace can be **leadless** or have exactly one active lead; only the operator can change lead authority. Without a lead, active members may use allowed membership controls. With a lead, configured policies constrain non-lead mutations. Removing/archiving a lead clears leadership rather than electing a successor.

## Two authorities: operator HTTP and model tools

The **operator** uses its runtime Bearer token on `/v2/workspaces/**` and `/v2/messages/**`. It can create/archive/restore agents, set leads, interrupt members, inspect deliveries and resolve uncertain operations. The **model** instead receives the three semantic GG tools through a trusted native Codex bridge or a provider's agent-scoped MCP credential. A model never authors its sender, workspace or Bearer token.

| Task | Operator API | Model tool |
| --- | --- | --- |
| Register workspace | `POST /v2/workspaces` | None |
| Add member | `POST /v2/workspaces/{workspace_id}/agents` | `gg_team` / `add` |
| Inspect roster | `GET /v2/workspaces/{workspace_id}/agents` | `gg_team` / `status` |
| Archive member | `POST /v2/workspaces/{workspace_id}/agents/{agent_id}/archive` | `gg_team` / `remove` |
| Set or clear lead | `POST /v2/workspaces/{workspace_id}/lead` | Operator only |
| Interrupt active workspace turns | `POST /v2/workspaces/{workspace_id}/interrupt` | Operator only |
| Send direct/broadcast | `POST /v2/messages` | `gg_message` / `direct` or `broadcast` |
| Inspect delivery/retry | `GET /v2/messages/{message_id}/deliveries`, `POST /v2/deliveries/{delivery_id}/retry` | No raw delivery authority |

## Direct messages cross workspaces

Any active, routable agent may send to another active, routable agent on this runtime, even **across workspace boundaries**. Cross-workspace direct messages are recorded with `global_direct` context; messages inside one workspace use `workspace_team` context. Agent authorization depends on the trusted caller, **not** the recipient's friendly alias.

For an operator, `POST /v2/messages` uses the canonical sender and recipient agent IDs:

```bash
curl -fsS -X POST "$BASE_URL/v2/messages" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: review-request-001" \
  -d '{
    "mode":"direct",
    "sender_agent_id":"sess_sender",
    "recipient_agent_id":"sess_reviewer",
    "input":[{"type":"text","text":"Please review the worktree changes."}]
  }'
```

The corresponding **model-authored** `gg_message` argument is smaller:

```json
{"mode":"direct","agent_id":"reviewer","message":"Please review the worktree changes.","image_paths":[]}
```

Here the runtime resolves the model's authenticated sender and accepts the recipient's durable friendly alias or opaque ID. Do **not** include `sender_agent_id`, `workspace_id`, `team_id` or a caller token in model JSON.

## Broadcasts stay inside the caller's workspace

A broadcast **snapshots the sender's currently active workspace members**, excluding the sender, at admission. It never crosses into other workspaces and historical recipients never change when the roster changes.

Operator request:

```bash
curl -fsS -X POST "$BASE_URL/v2/messages" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: workspace-update-001" \
  -d '{
    "mode":"broadcast",
    "sender_agent_id":"sess_sender",
    "input":[{"type":"text","text":"New migration constraint: preserve existing history."}]
  }'
```

Model `gg_message` request:

```json
{"mode":"broadcast","message":"New migration constraint: preserve existing history."}
```

No recipient or workspace selector is needed. Operator requests must also omit `recipient_agent_id` and `workspace_id` in broadcast input.

## Message receipts are not delivery receipts

One durable message can have multiple **per-recipient deliveries**. Delivery may be pending, deferred for a busy recipient, admitted to the canonical automation/transport turn, cancelled, or failed. Mandatory new-agent onboarding must go first; messages cannot be used to jump that queue.

To inspect or recover from an operator client:

```bash
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/messages?sender_agent_id=$SENDER_AGENT_ID&limit=50"

curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v2/messages/$MESSAGE_ID/deliveries"

curl -fsS -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: retry-delivery-001" \
  "$BASE_URL/v2/deliveries/$DELIVERY_ID/retry"
```

`GET /v2/messages` returns a JSON object with `messages` and `next_cursor`. `GET /v2/messages/{message_id}/deliveries` returns a bounded JSON array and a conditional **`X-Next-Cursor` response header**. Cancellation is `POST /v2/messages/{message_id}/cancel`. Only eligible delivery states can be retried or cancelled; never infer safety merely from a network timeout. Replaying the same operator idempotency key and normalized request returns the original outcome, while changing its input conflicts.

Ordered native `image_paths` can reach image-capable Codex/Claude recipients, with exact path/type/quantity checks. Unsupported provider images fail explicitly; Gooselake does not silently turn them into text.

## Model-visible status and system notices

`gg_team` with `{"mode":"status"}` gives a bounded roster view with alias, role, title/provenance, activity, worktree route, and **provider-bound** context remaining where available. It also reports the optional lead and a bounded set of eligible worktrees. Missing/stale context observations are `null`, not a guessed percentage.

The runtime delivers compact `<agent_msg>` envelopes with the sender alias, `kind="dm"| "broadcast"`, `context="global_direct"|"workspace_team"` and attachment count. This is authenticated model transport context, not a forged human prompt.

Approved workspace notices cover onboarding, lead changes, membership and provider-proven worktree moves; process completions are also pushed as automation context. Hooks and notifications have durable receipts/recovery semantics, so replay is not permission to send a second copy.

## Legacy compatibility and migration

The older `/v1/teams/*` endpoints still exist **temporarily** for legacy team rows and migration-era operator workflows. They have separately created teams, joins and historical team message views; **they are not the canonical workspace roster or model-facing GG tool path**. New integrations should use `/v2` and the three GG tools instead of recreating an independent legacy team.

See [Core concepts](/docs/concepts), [API guide](/docs/api), [MCP and sidecars](/docs/mcp-and-sidecars), and [Events and recovery](/docs/events-and-recovery) for native-effect reconciliation and scoped authorization details.
