---
title: "Client design guide"
description: "Build workspace-first frontends and automation that render runtime authority rather than reconstructing it."
order: 21
category: "Client Builders"
summary: "UI patterns for workspaces, agents, events, approvals, deliveries, managed worktrees, and recovery."
---

A Gooselake client is a remote control and dashboard, **not an independent team manager or provider-session database**. The runtime owns canonical workspace identity, active rosters, native effect receipts, and durable event history.

## What belongs in client state

Keep selected workspace/agent IDs, last event cursors, in-progress editor text, UI filters and view preferences. Fetch all authoritative operation, lead, activity, session policy, message, process and worktree state from the runtime.

Never treat a provider-native session reference, process PID, friendly alias or checked-out Git path as an authorization credential or stable ownership key.

## First screen: workspaces and readiness

A good operator landing page shows:

- `GET /health` and `GET /v2/capabilities`
- active/retired workspaces from `GET /v2/workspaces` (bounded pages)
- each workspace's active agent roster, nullable lead and recent operation states
- provider readiness and supported models/skills, where advertised
- recovery or manual-review operations requiring investigation

A workspace can be **leadless**; the UI should not invent a lead. New `/v2` agents appear automatically as roster members.

## Agent detail and turn input

An agent view should load the actual workspace agent record, provider policy/revision, onboarding status, active logical turn, pending approvals and recent durable events.

```text
POST /v2/workspaces/{workspace_id}/agents
  -> stable agent ID / mandatory onboarding
  -> GET /v2/workspaces/{workspace_id}/agents/{agent_id}/onboarding
  -> POST /v2/agents/{agent_id}/turns
  -> GET /v2/agents/{agent_id}/events/stream
```

A successful turn request **admits work** but does not imply completion. If onboarding is pending, show that state rather than retrying a blocked user turn. If another turn owns the session, show the existing turn. Use runtime-owned logical IDs, not native provider turn IDs, in UI routes and retry bookkeeping.

Provide explicit approval and revision-conflict states. A provider-origin approval is a durable runtime record attached to an admitted turn; a lost response or uncertain provider dispatch may require review rather than resubmission.

## Events and reconnect

Persist the last event cursor **per selected stream**. Reconnect with `after_seq` or `Last-Event-ID`, replay backlog, then follow live events. Global stream IDs are journal row IDs; scoped workspace/agent/process streams use scoped sequence values. SSE streams do not paginate through `X-Next-Cursor`; keyset JSON list endpoints do.

Build UI status from authoritative snapshot/list APIs plus the event tail. An event notification may be delayed or coalesced, but the journal replay closes the gap.

## Teams and message delivery

Render the active workspace roster, optional lead, and messages with **per-recipient delivery states**. Direct messages may target an agent outside the sender's workspace; a broadcast never does. Operator `POST /v2/messages` accepts a sender ID because the operator is trusted; model `gg_message` obtains the sender from scoped provider authority.

Never collapse “message created,” “delivery enqueued” and “recipient turn admitted” into one checkmark. Busy agents can defer messages, and onboarding may take priority over message injection.

## Processes and worktrees

Managed processes have durable `proc_*` IDs, scheduler state, owner, exit/timeout classification, bounded live previews and **authoritative log files**. The model receives completion via push context; UI users can still inspect logs and history. Do not key a process by raw PID.

Worktree inventory belongs to the workspace. Show canonical repo/branch/path, active agent attachments, retention and eligibility blockers separately. Rebinding an agent to an existing managed worktree is complete only after **native provider evidence** proves the destination, not merely after a SQLite route field changed.

## Provider-specific UI only where required

Codex/Claude have curated supported model policy; ACP may expose **no catalog** because its configured agent owns model and skill choice. Present capability-gated controls for images, skills, thinking, permissions, compaction, edit-rerun and workspace rebinding. Never infer support from a provider name alone.

## Errors, authorization, and recovery

Operator `/v2` errors have stable `error.code`, `retryable`, `details` and `request_id`. Display an operation's stable ID and provide inspection when `outcome_unresolved` or `manual_review` appears. Honor `If-Match` and the actual resource revision for conditional authority writes.

The operator token is **not** a substitute for the agent-scoped Bearer protecting `/v2/mcp/*`. A frontend should not expose a model's internal GG MCP credential or let model-authored JSON impersonate another caller.

## Legacy state

Legacy `/v1/sessions` and independent `/v1/teams` remain transitional operator surfaces. Treat unresolved legacy ownership as an explicit migration diagnostic, not a reason to create two competing active roster models.

**Golden rule:** a client can be killed, restarted and reconnected without changing the runtime's understanding of the work.
