---
title: "Core concepts"
description: "Understand workspace-owned agents, optional leadership, durable turns, cross-workspace messaging, provider capabilities, and runtime recovery."
order: 10
category: "Mental Model"
summary: "The workspace-first model for Gooselake's runtime authority, agent collaboration, and machine-side execution."
---

Gooselake is a **host-owned runtime**, not a chat UI. Your desktop app, script, or web console sends intent over HTTP and follows durable records/events; the runtime owns provider execution, authority, recovery, and work on the machine.

## Workspaces own collaboration

A **workspace** is the durable identity of an existing canonical project root. Registering that root again converges on the same workspace. A workspace owns its agent roster, optional lead, managed worktrees, workspace-visible processes, local broadcasts, and lifecycle.

`POST /v2/workspaces` registers a workspace. New public agents are created under `POST /v2/workspaces/{workspace_id}/agents`. They **automatically join its active roster**: there is no separate team creation or join request. The workspace's **team** is simply that active roster, not a second independent object.

A workspace can have **no lead** or **one active lead**. Leadless collaboration is intentional. Only operator authority can set, reassign, or clear a lead; removing the lead does not elect a replacement. With a lead, membership-management permissions follow the configured lead/member policy.

## Agents have durable, scoped identities

The public agent ID is a runtime-owned opaque `sess_*` ID. An agent has one immutable owning workspace and a durable friendly alias; aliases help people and models address colleagues, but **never grant authority**. A provider-native Codex/Claude/ACP session reference is a separate adapter detail.

An agent's recreation policy records its current provider, model, permission/setting sources, system and tool policy, worktree route, and harness version. Supported explicit preferences (including thinking effort) update authoritative state. Restart/resume must not silently substitute defaults.

Older `/v1/sessions` and `/v1/teams` interfaces remain temporary **legacy compatibility** for existing state and clients. Unresolved legacy sessions are classified for migration instead of being guessed into a workspace; the normal `/v2` agent workflow does not create standalone agents.

## Turns are admitted before execution

A **logical turn** is runtime-owned, not a provider turn ID. Gooselake stores its input, provenance, correlation and relevant dispatch policy **before** asking the provider to do work. User-visible input, agent-message transport, onboarding, and process/lifecycle automation have distinct provenance. User snapshots keep the text and durable attachment/skill references needed for recovery or edit-and-rerun.

A turn may wait for **mandatory onboarding** before a normal user prompt can be admitted. An accepted turn is not a completed turn: inspect status and events. Provider-native approval requests attach to that admitted turn and survive restart; they are distinct from an optional pre-dispatch approval gate.

A known-not-dispatched turn may be retried safely. An *unknown* provider dispatch outcome is quarantined for reconciliation, **not resent blindly**.

## Messages follow agent and workspace authority

- **Direct:** an active, routable agent can message another active, routable agent **in any workspace** in the same runtime. A cross-workspace direct message has `global_direct` context.
- **Broadcast:** a sender targets a snapshot of **their own workspace's active roster**, excluding themselves. Later membership changes do not change past recipients.
- **Delivery:** the durable message and each recipient's delivery state are separate. Busy recipients may defer input; retry/cancel has explicit conditions and receipts.

Incoming model context uses a compact `<agent_msg>` envelope with sender alias, direct/broadcast kind, workspace/global context and attachment count. It is not fabricated human input.

## Three model-facing GG tools

When enabled, providers expose exactly **`gg_team`, `gg_message`, and `gg_process`**:

- `gg_team`: `status`, `add`, `remove`, `assign_worktree`; the trusted caller's current workspace is implicit.
- `gg_message`: `direct` to an agent ID/alias or `broadcast` within the caller's workspace; ordered `image_paths` where the recipient supports images.
- `gg_process`: `run`, `list`, `status`, `cancel`. Completion is **pushed** into the owner agent's context; models should not poll for it.

The tool payload cannot set the caller's identity. Codex uses trusted native dynamic tools; Claude and ACP use scoped GG MCP credentials. Disabled capabilities do not appear as broken tools.

## Providers are adapters, with real differences

Gooselake integrates Codex through its persistent app-server, Claude through its SDK bridge, and generic external agents through ACP stdio. All share runtime authority for agents, turns, events, approvals, messages and processes, but optional skills, model catalogs, image support, manual compaction, rebind and edit-rerun are **capabilities**, not promises.

Codex and Claude use curated built-in model policies. An ACP agent may own its own model/skills/private harness and expose **no discoverable catalog**; Gooselake reports agent-managed/unsupported rather than inventing one. Read [Providers](/docs/providers) and `GET /v2/capabilities` before depending on advanced controls.

## Worktrees and processes are host resources

A managed worktree is a tracked checkout tied to the workspace's repository identity. An agent's effective route can be the workspace root or an eligible managed worktree. Moving an existing agent requires **provider-proven rebind evidence**; merely editing a database cwd is not a successful move. Cleanup protects preserved/external worktrees and active claims.

Runtime-managed processes are durably admitted before spawn, with stable `proc_*` IDs, bounded previews and authoritative stdout/stderr log files. A process can finish before its completion notification reaches the model. Scheduler, ownership and restart rules keep those states separate.

## Events, migrations and recovery are first-class

SQLite holds the authoritative rows: workspace/agent ownership, operations, turns, approvals, messages/deliveries, processes, worktrees, and the durable event journal. Operator streams support global/workspace/agent/process scopes, with cursor replay and a subscribed-before-replay live handoff. SSE is **not** a source of write authority.

A workspace retirement is a recoverable operation that classifies active native effects rather than simply setting a deleted flag. Re-registering a *fully* retired root can reactivate the same workspace identity, without restoring a former lead. Unresolved migration/provider outcomes block destructive cutover until independently reconciled.

**Client design rule:** if the UI vanishes, the runtime must still be able to explain what happened. Begin with [Setup](/docs/setup), then [Teams and comms](/docs/teams), [API](/docs/api) and [Events and recovery](/docs/events-and-recovery).
