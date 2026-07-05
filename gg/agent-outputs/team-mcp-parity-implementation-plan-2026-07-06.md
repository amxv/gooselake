# Team MCP Parity Implementation Plan

## State of Current System

Gooselake already has most of the standalone runtime foundations needed for team control:

- HTTP team APIs exist in `crates/runtime-server/src/http.rs` for creating/listing/getting/deleting teams, joining members, spawning team members, removing members, changing lead, direct messaging, broadcast messaging, message listing, delivery retry/cancel, and team event replay/streaming.
- `RuntimeTeamCommsService` in `crates/runtime-core/src/team_comms.rs` implements team membership, delivery records, message fanout, team events, and delivery injection.
- `RuntimeWorktreeService` in `crates/runtime-tools/src/lib.rs` implements worktree-backed `spawn_team_member`, including worktree create/reuse, session creation, joining the spawned session to the team, onboarding metadata, rollback, and best-effort cleanup.
- The bundled `sidecars/gg-mcp-server` already exposes the provider-facing MCP tools `gg_team_status`, `gg_team_message`, and `gg_team_manage`, and forwards them to `/v1/mcp/invoke` under namespace `gg_team`.
- Claude and ACP provider paths already inject the bundled `gg-mcp-server` into provider sessions. Claude bridge code also recognizes `gg_team_*` names as GG-scoped MCP tools.

The gap is the runtime gateway:

- `RuntimeToolGateway` currently owns only `RuntimeProcessManager`.
- `RuntimeToolGateway::invoke_tool` only executes `gg_process_*`.
- `/v1/mcp/capabilities` only reports `supportedNamespaces: ["gg_process"]`.
- `namespace_matches_tool` rejects `gg_team`.
- Therefore agents can see the team MCP tools from the sidecar, but calls are not backed by the runtime gateway.

Desktop implementation research from `~/code/amxv/gg-desktop`:

- Desktop has an MCP gateway in `src-tauri/src/agent_runtime/mcp_tool_gateway/mod.rs` that owns routers for `gg_team`, `gg_agent`, and `gg_process`.
- That gateway authorizes via bearer token, validates `namespace` against `tool_name`, then dispatches `gg_team_*` to `GgTeamToolRouter::invoke_tool_with_metadata`.
- `GgTeamToolRouter` supports exactly the desired team surface:
  - `gg_team_status`
  - `gg_team_message`
  - `gg_team_manage`
- `gg_team_status` validates the caller is a team member, then returns lead/member state, last activity, last message, context remaining percentage, worktree cwd/name, and `added_by`.
- `gg_team_message` rejects caller identity spoofing, accepts `team_id`, `recipient_agent_id`, `message`, optional `image_paths`, and sends either direct or broadcast messages through shared team operations.
- `gg_team_manage` rejects caller identity spoofing and removed legacy fields, uses `remove_agent_ids` to choose remove mode, otherwise add mode, and enforces configurable non-lead permissions.
- Desktop has two runtime settings:
  - `team_non_lead_can_add_members`
  - `team_non_lead_can_remove_members`
- Desktop defaults both to `false`, meaning the lead can manage membership and non-leads cannot unless explicitly enabled.

The Gooselake implementation should not port desktop’s whole `GgTeamToolRouter`. The extracted runtime already has equivalent service layers. The correct implementation is to add `gg_team` dispatch into `RuntimeToolGateway` and call Gooselake’s existing services directly.

## State of Ideal System

HTTP and MCP expose the same team control semantics:

- Human/API clients can control teams through HTTP.
- Agents can control teams through MCP.
- Both paths use the same underlying runtime services, persistence, event emission, delivery system, worktree spawn flow, and cleanup behavior.

Provider behavior is uniform:

- Codex, Claude Code, and ACP sessions all get the same bundled `gg-mcp-server`.
- The sidecar exposes the same `gg_team_*` tools regardless of provider.
- `/v1/mcp/invoke` backs those tools through one runtime gateway implementation.

Initial MCP team tool contract:

- `gg_team_status`
  - Input: `{ team_id }`.
  - Caller identity comes only from gateway metadata, not tool args.
  - Requires caller to be an active member of the team.
  - Returns a compact status object with `team_id`, `lead_agent_id`, `generated_at_ms`, and `members`.
  - Member rows include at least `agent_id`, `session_id`, `title`, `state`, `last_activity_at_ms`, `last_message`, `context_window_remaining_percentage`, `worktree_cwd`, `worktree_name`, and `added_by` where available.

- `gg_team_message`
  - Input: `{ team_id, recipient_agent_id, message, image_paths? }`.
  - Caller identity is sender.
  - `recipient_agent_id: "broadcast"` sends broadcast, excluding sender by default.
  - Any other recipient sends direct.
  - Uses `TeamCommsService::broadcast` / `send_direct`.
  - Returns `{ message_id, delivery_ids, recipient_count, scope, image_count, image_paths? }`.

- `gg_team_manage`
  - Input: `{ team_id, title?, prompt?, image_paths?, model_preset?, creator_compaction_subscription?, worktree_name?, use_existing_worktree?, remove_agent_ids? }`.
  - Add mode: `remove_agent_ids` omitted/null.
  - Remove mode: `remove_agent_ids` is non-empty.
  - Add mode spawns one new provider session by calling `WorktreeService::spawn_team_member`.
  - Remove mode removes one or more existing team members by calling `TeamCommsService::remove_team_member`, then best-effort `WorktreeService::on_member_removed`.
  - Lead can add/remove by default.
  - Non-leads can add/remove only if runtime config permits the specific operation.

Runtime config should include explicit, server-friendly policy:

```toml
[teams]
enabled = true
non_lead_can_add_members = false
non_lead_can_remove_members = false
```

This mirrors desktop defaults while making the standalone hosted runtime’s policy auditable in config.

## Cross-provider Requirements

- Do not add provider-specific team semantics. `gg_team_*` must remain provider-agnostic.
- Claude and ACP already receive external MCP server config with gateway URL/token. Keep that path.
- Codex support should be verified separately:
  - If Codex in Gooselake already uses MCP server config, ensure `gg_team_*` remains visible and callable.
  - If Codex currently relies on a different dynamic-tool path, add/verify equivalent bundled `gg-mcp-server` injection so Codex gets the same MCP-backed surface as Claude and ACP.
- Tool result envelopes must stay consistent across providers:
  - Success: `{ "ok": true, "result": ... }`.
  - Failure: `{ "ok": false, "error": { "code": "...", "message": "...", "details"?: ... } }`.
- The sidecar already serializes hidden caller metadata as `__gg_caller_agent_id` and invocation metadata as `__gg_tool_invocation_id`; runtime code should continue treating those as trusted gateway metadata, not model-authored payload fields.

## Plan Phases

### Phase 1 - Gateway Shape and Team Policy

#### Files to read before starting

- `crates/runtime-server/src/config.rs`
- `crates/runtime-server/src/bootstrap.rs`
- `crates/runtime-core/src/app.rs`
- `crates/runtime-core/src/services.rs`
- `crates/runtime-tools/src/lib.rs`
- `crates/runtime-server/src/http.rs`

#### What to do

- Add `TeamsConfig` to `RuntimeServerConfig`.
- Include:
  - `enabled: bool`, default `true`.
  - `non_lead_can_add_members: bool`, default `false`.
  - `non_lead_can_remove_members: bool`, default `false`.
- Decide whether `enabled` should gate only MCP team tools or all team services. Recommendation: do not introduce a new global team-service disable in this phase unless the existing runtime already has one; use `enabled` as the MCP team-tool exposure/authorization gate only, because HTTP team routes already exist and changing them broadens scope.
- Change `RuntimeToolGateway` from process-only to a shared control-plane gateway.
- Inject:
  - process manager
  - runtime/session service if needed for session status/context/worktree lookup
  - `Arc<dyn TeamCommsService>`
  - `Arc<dyn WorktreeService>`
  - team MCP policy/config
- Keep the public trait as `ToolGateway`.
- Keep `invoke_process_tool` as-is, but add `invoke_team_tool`.
- Update `namespace_matches_tool`:

```rust
match namespace.trim() {
    "gg_process" => tool_name.starts_with("gg_process_"),
    "gg_team" => tool_name.starts_with("gg_team_"),
    _ => false,
}
```

- Update `capabilities` to report:

```json
{
  "supportedNamespaces": ["gg_process", "gg_team"],
  "tools": [
    "gg_process_run",
    "gg_process_status",
    "gg_process_logs",
    "gg_process_kill",
    "gg_team_status",
    "gg_team_message",
    "gg_team_manage"
  ],
  "ggTeamEnabled": true,
  "ggTeamManagePermissions": {
    "nonLeadCanAddMembers": false,
    "nonLeadCanRemoveMembers": false
  }
}
```

- Preserve `ggProcessEnabled`.
- Preserve desktop semantics: lead is always allowed; non-leads require the matching flag.
- Keep team tools visible only if policy says team MCP is enabled. If disabled, either omit from capabilities and return `feature_disabled`, or keep visible with a clear disabled error. Recommendation: omit from capabilities and return `feature_disabled` when called.

#### Validation strategy

- Add config default tests for the new fields.
- Add config parse tests for setting add/remove flags.
- Add bootstrap test asserting `RuntimeToolGateway` receives the expected policy.
- Unit test namespace validation:
  - `gg_team` + `gg_team_status` accepted.
  - `gg_process` + `gg_team_status` rejected.
  - unsupported namespace rejected.
- Unit test capabilities include `gg_team` and all three desired tools when enabled.
- Unit test disabled policy returns `feature_disabled`.

#### Risks / fallbacks

- Risk: a generic `teams.enabled` can be misread as disabling all HTTP team APIs.
- Fallback: name it more explicitly, for example `[mcp.team] enabled`, `non_lead_can_add_members`, `non_lead_can_remove_members`.
- Recommendation: prefer a clear scoped config if this runtime already expects HTTP teams to always be available.
- Risk: `RuntimeToolGateway::new(process_manager)` is used in many tests.
- Fallback: introduce `RuntimeToolGateway::process_only_for_tests` or a builder so existing process tests stay cheap.
- Recommendation: use a builder or `RuntimeToolGateway::new(RuntimeToolGatewayDeps { ... })` and update test harness helpers once.

### Phase 2 - Team Status and Messaging Tools

#### Files to read before starting

- Desktop reference:
  - `/Users/ashray/code/amxv/gg-desktop/src-tauri/src/agent_runtime/gg_team_tools/router/status.rs`
  - `/Users/ashray/code/amxv/gg-desktop/src-tauri/src/agent_runtime/gg_team_tools/router/message.rs`
- Gooselake:
  - `crates/runtime-core/src/team_comms.rs`
  - `crates/runtime-core/src/services.rs`
  - `crates/runtime-core/src/runtime.rs`
  - `crates/runtime-core/src/state.rs`
  - `crates/runtime-tools/src/lib.rs`
  - `sidecars/gg-mcp-server/src/tool_params.rs`

#### What to do

- Implement `gg_team_status`.
- Parse status args as object with required `team_id`.
- Reject identity-spoofing fields if present:
  - `caller_agent_id`
  - `sender`
  - `sender_agent_id`
  - `agent_id`
- Load team via `TeamCommsService::get_team`.
- Ensure `request.caller_session_id` is a team member.
- Build output modeled on desktop:
  - `team_id`
  - `lead_agent_id`
  - `generated_at_ms`
  - `members`
- For each member, include:
  - `agent_id` / `session_id`
  - `title`
  - `state`
  - `last_activity_at_ms`
  - `last_message`
  - `context_window_remaining_percentage`
  - `worktree_cwd`
  - `worktree_name`
  - `added_by`
- Source available fields from existing Gooselake state:
  - team membership from `TeamWithMembers`.
  - session status/active turn from runtime/session manager if exposed.
  - worktree reference from member `worktree_id` and `WorktreeService::get_worktree`, or from hydrated records if easier.
  - last message from `TeamCommsService::list_messages` or `get_view_snapshot`.
- Keep first pass pragmatic: if context percentage or last-message details are not cleanly available through traits, return `null`/omit only those fields and add follow-up trait methods in the same phase if tests require parity.
- Implement `gg_team_message`.
- Parse message args as object with:
  - required `team_id`
  - required `recipient_agent_id`
  - required `message`
  - optional `image_paths`
- Reject identity-spoofing fields:
  - `caller_agent_id`
  - `sender`
  - `sender_agent_id`
- Normalize `message` into a runtime `input` shape compatible with existing HTTP.
- Preserve `image_paths` as a parallel field as HTTP does.
- If `recipient_agent_id == "broadcast"` case-insensitive:
  - call `TeamCommsService::broadcast`
  - `sender_agent_id = caller_session_id`
  - `include_sender = false`
  - `priority = "normal"`
  - `policy = "non_interrupting"`
  - `idempotency_key = invocation_id` if desirable
- Otherwise:
  - call `TeamCommsService::send_direct`
  - sender is caller
  - recipient is `recipient_agent_id`
- Return:
  - `message_id`
  - sorted `delivery_ids`
  - `recipient_count`
  - `scope`
  - `image_count`
  - `image_paths` when present

#### Validation strategy

- Runtime gateway status test:
  - create sessions
  - create team
  - call `/v1/mcp/invoke` with `namespace: "gg_team"`, `tool_name: "gg_team_status"`
  - assert success and member rows.
- Status unauthorized test:
  - caller not in team returns `ok: false` with unauthorized-style error.
- Status spoofing test:
  - args containing `sender_agent_id` are rejected.
- Direct message gateway test with two members.
- Broadcast gateway test with three members, sender excluded.
- Rejection test for blank message/team/recipient.
- Rejection test for spoofed sender fields.
- Event/delivery assertion that HTTP and MCP both produce normal `TeamMessageRecord` / delivery events.
- Sidecar integration already verifies tool schema; add a runtime gateway test that sidecar-forwarded payloads execute.

#### Risks / fallbacks

- Risk: exact desktop status fields require manager methods that Gooselake does not expose yet.
- Fallback: add focused runtime-core service methods instead of reaching through store internals from `runtime-tools`.
- Recommendation: implement essential fields now, then tighten parity with a dedicated `TeamStatusSnapshot` helper if the code becomes too broad.
- Risk: `image_paths` validation semantics differ between desktop and Gooselake.
- Fallback: initially pass paths through as HTTP does, then add strict validation only if already present elsewhere in Gooselake.
- Recommendation: preserve current HTTP behavior for parity between humans and agents.

### Phase 3 - Team Manage Add and Remove

#### Files to read before starting

- Desktop reference:
  - `/Users/ashray/code/amxv/gg-desktop/src-tauri/src/agent_runtime/gg_team_tools/router/manage.rs`
  - `/Users/ashray/code/amxv/gg-desktop/src-tauri/src/agent_runtime/gg_team_tools/router/status.rs`
  - `/Users/ashray/code/amxv/gg-desktop/src-tauri/src/agent_runtime/gg_team_tools/router/onboarding.rs`
  - `/Users/ashray/code/amxv/gg-desktop/src-tauri/src/agent_runtime/gg_team_tools/router/idempotency.rs`
  - `/Users/ashray/code/amxv/gg-desktop/src-tauri/src/agent_runtime/gg_team_tools/router/native_worktree.rs`
- Gooselake:
  - `crates/runtime-core/src/team_comms.rs`
  - `crates/runtime-core/src/services.rs`
  - `crates/runtime-tools/src/lib.rs`
  - `crates/runtime-server/src/http.rs`
  - `sidecars/gg-mcp-server/src/tool_params.rs`

#### What to do

- Implement `gg_team_manage`.
- Parse args and reject caller identity spoofing and removed legacy fields in runtime too, even though the sidecar schema already denies unknown fields.
- Select remove mode when `remove_agent_ids` exists:
  - require non-empty array.
  - reject add-only fields that should not be meaningful in remove mode if already rejected by sidecar schema or runtime parser, especially `worktree_name` and `use_existing_worktree`.
- Enforce remove permissions:
  - caller must be a member.
  - caller can remove if caller is team lead.
  - non-lead caller can remove only if `non_lead_can_remove_members` is true.
- For each requested removal id:
  - trim and validate.
  - call `TeamCommsService::remove_team_member`.
  - call `WorktreeService::on_member_removed` best-effort, with `removed_by = Some(caller_session_id)`.
  - collect per-member result objects.
- Match desktop remove result shape:

```json
{
  "operation": "remove",
  "team": { ... },
  "results": [
    { "agent_id": "sess_member", "ok": true }
  ],
  "removed_agent_ids": ["sess_member"],
  "failed_agent_ids": []
}
```

- Preserve partial success semantics: one bad id should not prevent valid ids from being attempted unless permission/team lookup fails before loop.
- Select add mode when `remove_agent_ids` is absent/null.
- Enforce add permissions:
  - caller must be a member.
  - caller can add if caller is team lead.
  - non-lead caller can add only if `non_lead_can_add_members` is true.
- Build `TeamMemberSpawnRequest`:
  - `team_id`: tool input team id.
  - `source_session_id`: caller session id.
  - `provider`: optional, probably omitted initially to inherit caller provider.
  - `model`: resolved from `model_preset` if supported, otherwise omitted to inherit caller model.
  - `title`: tool input title.
  - `prompt`: tool input prompt.
  - `permission_mode`: omitted to inherit source/runtime defaults.
  - `metadata`: include onboarding metadata if needed, but rely on existing `spawn_team_member` onboarding if present.
  - `creator_agent_id`: caller session id unless a stronger creator-lineage field already exists.
  - `creator_compaction_subscription`: tool input or `"auto"`.
  - `worktree`: if `worktree_name` present:
    - `name = worktree_name`
    - `mode = "reuse"` when `use_existing_worktree == true`, otherwise `"create"`
    - branch prefix from runtime worktree config.
- Call `WorktreeService::spawn_team_member`.
- Return a result shape that is stable for agents:

```json
{
  "operation": "add",
  "team": { ... },
  "spawned_agent_id": "...",
  "spawned_session_id": "...",
  "member": { ... },
  "worktree": { ... },
  "worktree_assignment_mode": "...",
  "worktree_created_by_operation": true,
  "onboarding": { ... }
}
```

- Add idempotency based on `invocation_id`:
  - Desktop caches add results by caller/tool/invocation id to avoid duplicate spawns from provider replay.
  - Gooselake should do the same for MCP add mode.
  - Start with in-memory TTL cache in `RuntimeToolGateway`; later persist to the existing worktree spawn journal if needed.

#### Validation strategy

- Lead removes one member via MCP.
- Lead removes multiple members with one unknown id and gets partial failure.
- Non-lead remove denied by default.
- Non-lead remove allowed when config flag is true.
- Removing last member follows existing service behavior and returns failure.
- Worktree cleanup best-effort behavior matches HTTP remove path.
- Lead adds member via MCP with no worktree.
- Lead adds member via MCP with `worktree_name`.
- Lead adds member via MCP with `worktree_name` and `use_existing_worktree`.
- Non-lead add denied by default.
- Non-lead add allowed when config flag is true.
- Duplicate invocation id does not spawn duplicate members.
- Spawn failure rolls back session/worktree as existing `RuntimeWorktreeService` already promises.

#### Risks / fallbacks

- Risk: current `remove_team_member` changes lead if lead is removed, but permission checks are based on pre-removal caller state.
- Fallback: capture team snapshot and permission before loop.
- Recommendation: deny self-removal of last/only effective controller only if existing service does; otherwise keep parity with current HTTP behavior.
- Risk: `model_preset` exists in sidecar schema but Gooselake may not have the desktop preset catalog.
- Fallback: return `unknown_model_preset` until a runtime model-preset config is added, or initially accept only null/absent `model_preset`.
- Recommendation: include `model_preset` support as a subtask only if Gooselake already has provider model catalog wiring; otherwise document it as a follow-up and ensure the tool returns a clear error.
- Risk: image-backed onboarding may not be fully implemented in `spawn_team_member`.
- Fallback: pass `image_paths` through metadata or return a clear unsupported error for `image_paths` in add mode.
- Recommendation: keep the schema, but do not silently drop images.

### Phase 4 - Provider, Sidecar, and HTTP Parity

#### Files to read before starting

- `crates/runtime-server/src/http.rs`
- `crates/runtime-tools/src/lib.rs`
- `crates/runtime-core/src/services.rs`
- `crates/runtime-core/src/team_comms.rs`
- `sidecars/gg-mcp-server/src/server.rs`
- `sidecars/gg-mcp-server/src/tool_params.rs`
- `sidecars/gg-mcp-server/tests/mcp_stdio_integration.rs`
- `crates/runtime-provider-claude/src/lib.rs`
- `crates/runtime-provider-acp/src/lib.rs`
- `crates/runtime-provider-codex/src/lib.rs`
- `sidecars/claude-bridge/src/claude-client/client.ts`
- `sidecars/claude-bridge/src/claude-client/sdk-runtime.ts`

#### What to do

- Ensure HTTP and MCP use the same service methods and policy assumptions.
- Keep HTTP routes human-capable:
  - create/list/get/delete teams
  - join/spawn/remove members
  - set lead
  - direct/broadcast messaging
  - message/delivery lifecycle
- Decide how HTTP should expose team-manage policy:
  - Human clients should still be allowed to do administrative operations through authenticated HTTP.
  - The non-lead add/remove policy is specifically about agent-initiated `gg_team_manage`, not root HTTP admin authority.
- Add policy inspection to `/v1/mcp/capabilities` first because agents and provider bridges already query it.
- Keep the sidecar tool surface unchanged:
  - `gg_team_status`
  - `gg_team_message`
  - `gg_team_manage`
- Ensure sidecar `tools/list` remains accurate:
  - If runtime team MCP is disabled, decide whether sidecar should still list team tools. Recommendation: list them only when runtime capabilities include `gg_team`, or keep them listed but include disabled metadata only if provider SDK supports it cleanly.
- Ensure sidecar `/capabilities` refresh handles new fields:
  - `ggTeamModelPresets`
  - `ggTeamManagePermissions`
  - `supportedNamespaces`
- Verify all providers receive the MCP server:
  - Claude: already configured in provider config.
  - ACP: already configured in provider config.
  - Codex: inspect and add equivalent config if absent.

#### Validation strategy

- Tests show HTTP spawn/remove still works regardless of non-lead MCP policy.
- Tests show MCP non-lead policy gates only `gg_team_manage`, not messaging/status.
- Tests show status/message require membership.
- Sidecar integration test that `gg_team_status` can proxy to a fake gateway that supports `gg_team`.
- Runtime HTTP test invoking `/v1/mcp/invoke` directly for each `gg_team_*`.
- Provider-level smoke or unit tests:
  - Claude allowed tool names include `mcp__gg__gg_team_manage`.
  - ACP mcp server config includes gateway URL/token and `GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID=1`.
  - Codex receives the same server/tool path or a documented equivalent.

#### Risks / fallbacks

- Risk: conflating authenticated HTTP admin authority with agent authority.
- Fallback: explicitly name policy `agent_team_manage_permissions` in config/capabilities.
- Recommendation: do not make HTTP clients subject to non-lead agent tool policy unless the HTTP request explicitly acts "as" an agent.
- Risk: sidecar currently lists team tools even when runtime gateway cannot execute them; this bug should disappear once gateway implements `gg_team`.
- Fallback: if full gateway implementation is delayed, temporarily hide team tools based on capabilities to avoid false affordance.

### Phase 5 - Acceptance Tests and API Docs Sync

#### Files to read before starting

- `.agents/skills/runtime-api-doc-sync/SKILL.md`
- `crates/runtime-server/src/http.rs`
- `crates/runtime-server/src/openapi.rs`
- `openapi/runtime-server-openapi.yaml`
- `src/content/docs/api.md`
- `sidecars/gg-mcp-server/tests/mcp_stdio_integration.rs`
- provider test files for Claude, ACP, and Codex.

#### What to do

- Add end-to-end-ish runtime tests:
  - Create lead session.
  - Create team via HTTP.
  - Call `gg_team_status` via `/v1/mcp/invoke`.
  - Call `gg_team_message` direct via `/v1/mcp/invoke`.
  - Call `gg_team_message` broadcast via `/v1/mcp/invoke`.
  - Call `gg_team_manage` add via `/v1/mcp/invoke`.
  - Call `gg_team_manage` remove via `/v1/mcp/invoke`.
  - Repeat add/remove from non-lead with policy disabled and enabled.
- Add idempotency test:
  - same caller + same invocation id + add mode returns cached result and creates one member.
- Add provider config tests for MCP server injection.
- Use the local `runtime-api-doc-sync` skill because this touches `/v1/mcp/capabilities`, `/v1/mcp/invoke`, and MCP-facing behavior.
- Update narrative API docs to state:
  - HTTP and MCP share team semantics.
  - MCP supports `gg_process` and `gg_team`.
  - `gg_team_manage` add/remove policy is configurable.
  - Team tools are provider-agnostic across Codex, Claude, and ACP.
- Regenerate OpenAPI if any HTTP route shape or schema text changes.

#### Validation strategy

- Fast checks:
  - `cargo test -p runtime-tools`
  - `cargo test -p runtime-server mcp`
  - `cargo test -p gg-mcp-server`
- API docs checks:
  - `make api-docs-refresh`
  - `make api-docs-status`
  - `make api-docs-check`
- Broader checks before merge:
  - workspace `cargo test` or the repo’s documented check suite.

#### Risks / fallbacks

- Risk: OpenAPI schemas are intentionally broad and may not capture `gg_team_*` nuance.
- Fallback: put exact tool contracts in narrative docs.
- Risk: full workspace tests may be expensive.
- Fallback: start with package-scoped tests and run broader checks overnight or in CI.

## Recommendation

Implement this as a gateway parity change, not a new team subsystem.

The preferred architecture is:

- Keep `sidecars/gg-mcp-server` as the universal provider-facing MCP server.
- Extend `RuntimeToolGateway` to support `gg_team`.
- Route `gg_team_*` directly into `TeamCommsService` and `WorktreeService`.
- Add explicit config for agent-initiated membership control:
  - lead can add/remove by default.
  - non-lead add/remove disabled by default.
  - independent flags enable delegated add/remove.
- Keep HTTP as the human/admin control plane and MCP as the provider-agnostic agent control plane, both backed by the same services.

This gives Gooselake the same practical semantics as the desktop app while keeping the standalone runtime smaller and cleaner than a direct port of desktop internals.
