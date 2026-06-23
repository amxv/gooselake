# ACP Provider Research for GG Runtime

Date: 2026-06-23

## Executive Summary

ACP (Agent Client Protocol) is an open protocol for connecting editors and other clients to coding agents. It began with Zed, but the maintained protocol home, schema, SDKs, and governance now live under the broader `agentclientprotocol` project. The current stable wire protocol is ACP v1, built on JSON-RPC 2.0, with stdio as the only stable agent transport today and remote transports still incomplete or draft-level. Official ACP docs explicitly say remote support is still a work in progress, which matters for GG Runtime because GG is already a remote-capable machine-side runtime rather than an editor-side subprocess integration.

For this repo, ACP should be treated as a third provider transport/protocol adapter, not as a replacement for GG’s provider contract. The cleanest near-term addition is a direct Rust integration using the official Rust SDK as an ACP client that launches or connects to an ACP agent subprocess and maps ACP session lifecycle + updates into GG’s existing `RuntimeProvider` contract. A secondary sidecar bridge is optional later if a specific ACP agent or transport needs stronger process isolation, but it is not the best first step because GG already has a provider abstraction and Rust-side transport/process control.

The biggest integration mismatch is that ACP was designed around editor-client <-> agent interactions, while GG Runtime is a machine-local orchestration backend that already owns tools, processes, worktrees, team messaging, durable events, and HTTP/SSE APIs. That mismatch is manageable if ACP remains provider-scoped: GG should continue to own persistence, replay, approvals, team/process/worktree/MCP services, and diagnostics, while the ACP provider focuses on ACP connection/session management, auth negotiation, prompt turns, update normalization, tool-call/permission mapping, and optional model/config discovery.

## What ACP Is

ACP standardizes communication between code editors/IDEs and coding agents, using JSON-RPC 2.0 messages exchanged over a bidirectional transport. Official overview and intro:

- https://agentclientprotocol.com/get-started/introduction
- https://agentclientprotocol.com/protocol/v1/overview

The current official protocol positioning:

- Stable wire protocol version is `1`: https://github.com/agentclientprotocol/agent-client-protocol
- JSON-RPC 2.0 message model: https://agentclientprotocol.com/protocol/v1/overview
- Stable local transport is stdio; streamable HTTP is still draft/in progress: https://agentclientprotocol.com/protocol/v1/transports
- Remote support is explicitly still a work in progress: https://agentclientprotocol.com/get-started/introduction

### Current protocol shape

Lifecycle from the official v1 docs:

1. `initialize`
2. optional `authenticate`
3. `session/new` or `session/load` or `session/resume`
4. `session/prompt`
5. `session/update` notifications during execution
6. optional `session/request_permission` round-trips
7. `session/prompt` response with a `stopReason`
8. optional `session/cancel`
9. optional `session/close`, `session/list`, `session/set_config_option`, `logout`

Primary docs:

- Initialization: https://agentclientprotocol.com/protocol/v1/initialization
- Session setup/load/resume/close: https://agentclientprotocol.com/protocol/v1/session-setup
- Prompt lifecycle: https://agentclientprotocol.com/protocol/v1/prompt-turn
- Tool calls + permissions: https://agentclientprotocol.com/protocol/v1/tool-calls
- Terminals: https://agentclientprotocol.com/protocol/v1/terminals
- Authentication: https://agentclientprotocol.com/protocol/v1/authentication
- Session list: https://agentclientprotocol.com/protocol/v1/session-list
- Session config options: https://agentclientprotocol.com/protocol/v1/session-config-options

### Transport model

ACP v1 transport rules today are narrower than GG’s runtime model:

- Stable: stdio, newline-delimited UTF-8 JSON-RPC messages
- Draft only: streamable HTTP
- Custom transports are allowed, but not standardized beyond preserving ACP lifecycle semantics

Source: https://agentclientprotocol.com/protocol/v1/transports

That means the most production-ready ACP shape for GG today is subprocess-backed ACP agents over stdio, not native networked ACP sessions.

### Tool/model/session concepts

ACP’s core unit is an agent session. A session has:

- `cwd` and optional `additionalDirectories`
- optional MCP server definitions supplied by the client
- optional session discovery/history via `session/list`
- optional mutable session-level config options such as model or mode selectors

Model exposure is not a top-level protocol method. Instead, model selection is expected to appear as a session config option, usually with category `model`.

Sources:

- Session setup and MCP server injection: https://agentclientprotocol.com/protocol/v1/session-setup
- Session list/history: https://agentclientprotocol.com/protocol/v1/session-list
- Config/model selectors: https://agentclientprotocol.com/protocol/v1/session-config-options

Tool calls are reported through `session/update` notifications as `tool_call` and `tool_call_update` records. Permission requests are explicit request/response RPCs via `session/request_permission`.

Source: https://agentclientprotocol.com/protocol/v1/tool-calls

### Lifecycle and streaming model

ACP does not use SSE or server-driven durable replay. Instead:

- request/response RPC is used for phase boundaries
- `session/update` notifications are pushed over the same live connection
- `session/load` can replay conversation history by streaming `session/update` notifications before its final response
- `session/resume` restores session context without replay

Sources:

- https://agentclientprotocol.com/protocol/v1/prompt-turn
- https://agentclientprotocol.com/protocol/v1/session-setup

This is a live connection protocol, not a persistence protocol. GG would still need to own durable storage and fanout.

## Maintainers, maturity, and stability

ACP appears to have moved beyond a Zed-only experiment:

- protocol/spec home: https://github.com/agentclientprotocol/agent-client-protocol
- Rust SDK home: https://github.com/agentclientprotocol/rust-sdk
- official Rust docs page: https://agentclientprotocol.com/libraries/rust
- protocol governance/docs index: https://agentclientprotocol.com/llms.txt

Signals that it is increasingly real but still evolving:

- stable v1 protocol docs are published
- multiple official SDKs are listed in the protocol repo README
- specific features were stabilized recently in 2026, including:
  - session config options on 2026-02-04: https://agentclientprotocol.com/announcements/session-config-options-stabilized.md
  - session resume on 2026-04-22: https://agentclientprotocol.com/announcements/session-resume-stabilized.md
  - logout on 2026-05-21: https://agentclientprotocol.com/announcements/logout-method-stabilized.md
- the docs still state remote support is a work in progress: https://agentclientprotocol.com/get-started/introduction
- the transport docs still mark streamable HTTP as draft: https://agentclientprotocol.com/protocol/v1/transports
- the docs index includes many active RFDs, including ACP v2 and transport proposals: https://agentclientprotocol.com/llms.txt

Assessment: ACP is mature enough for a third-provider experiment in GG, but not mature enough to treat remote ACP transport and long-term wire stability as fully solved. The stable center today is local stdio ACP agents plus the official Rust SDK.

## Official SDKs and Rust options

### Official Rust path

The official Rust SDK is the strongest implementation choice:

- Rust SDK repo: https://github.com/agentclientprotocol/rust-sdk
- Rust SDK docs: https://docs.rs/agent-client-protocol/latest/agent_client_protocol/
- Rust library page: https://agentclientprotocol.com/libraries/rust
- schema crate: https://docs.rs/agent-client-protocol-schema

The Rust SDK currently exposes:

- `agent-client-protocol`: core roles, connection builders, protocol types
- `agent-client-protocol-tokio`: Tokio helpers for spawning agents / stdio wiring
- `agent-client-protocol-rmcp`: ACP + MCP integration
- `agent-client-protocol-conductor`: proxy-chain orchestration

Source: https://github.com/agentclientprotocol/rust-sdk

This is the best fit for GG because this repo is already Rust-first and already has provider adapters that own subprocesses and event normalization.

### Canonical implementation evidence

The protocol and SDK ecosystem show a canonical direction:

- the agent-client-protocol org now owns both protocol and Rust SDK
- Zed’s older `codex-acp` repo now points to `agentclientprotocol/codex-acp` for new installs: https://github.com/zed-industries/codex-acp
- official docs say the Rust crate powers Zed’s external-agent integration: https://agentclientprotocol.com/libraries/rust

That strongly suggests the protocol is no longer just “a Zed-specific idea”; the `agentclientprotocol` org is now the canonical home.

### Other notable implementation surfaces

Useful, but secondary for GG:

- official schema crate for lower-level typed wire messages: https://docs.rs/agent-client-protocol-schema
- possible proxy/conductor patterns in the Rust SDK for future extension chains: https://github.com/agentclientprotocol/rust-sdk
- community adapters and clients exist, but they should not be the primary dependency choice for GG unless an official SDK gap appears

## Repo-specific implementation surface

### Workspace and crate layout

The workspace root currently includes six crates and no ACP-specific provider crate:

- `crates/runtime-core`
- `crates/runtime-store-sqlite`
- `crates/runtime-provider-codex`
- `crates/runtime-provider-claude`
- `crates/runtime-tools`
- `crates/runtime-server`

Source: [Cargo.toml](/Users/ashray/code/amxv/gg-agent-runtime/Cargo.toml:1)

Notable consequence: `sidecars/gg-mcp-server` is not a workspace member. It is a separate Cargo package with its own local `[workspace]` section and Edition 2024. That matters because ACP can either reuse this existing sidecar as an external MCP server process, or introduce another out-of-workspace sidecar if ACP transport isolation is ever needed.

Source: [sidecars/gg-mcp-server/Cargo.toml](/Users/ashray/code/amxv/gg-agent-runtime/sidecars/gg-mcp-server/Cargo.toml:1)

Current provider crates are both single-module crates with all behavior in `src/lib.rs`:

- `runtime-provider-codex`: [crates/runtime-provider-codex/Cargo.toml](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-codex/Cargo.toml:1)
- `runtime-provider-claude`: [crates/runtime-provider-claude/Cargo.toml](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-claude/Cargo.toml:1)

An ACP provider added in the same style would fit the repo’s current conventions:

- new workspace member: `crates/runtime-provider-acp`
- crate manifest parallel to Codex/Claude
- initial implementation in `crates/runtime-provider-acp/src/lib.rs`

### Exact files/modules implicated by adding ACP

At minimum, ACP touches these repo surfaces:

- workspace membership and shared deps: [Cargo.toml](/Users/ashray/code/amxv/gg-agent-runtime/Cargo.toml:1)
- provider enum + trait contract: [crates/runtime-core/src/provider.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/provider.rs:9)
- runtime session manager/provider dispatch: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:420)
- server config/provider config tree: [crates/runtime-server/src/config.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/config.rs:1)
- bootstrap/provider registration: [crates/runtime-server/src/bootstrap.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/bootstrap.rs:46)
- HTTP routes for provider list/models/auth: [crates/runtime-server/src/http.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:50)
- OpenAPI generator route catalog and summaries: [crates/runtime-server/src/openapi.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/openapi.rs:40)
- docs/API narrative and endpoint catalog:
  - [docs/API.md](/Users/ashray/code/amxv/gg-agent-runtime/docs/API.md:1)
  - [docs/API_ENDPOINTS.md](/Users/ashray/code/amxv/gg-agent-runtime/docs/API_ENDPOINTS.md:1)
- generated artifact: [openapi/runtime-server-openapi.yaml](/Users/ashray/code/amxv/gg-agent-runtime/openapi/runtime-server-openapi.yaml:1)

## Current provider/runtime shape in this repo

### Provider contract and enum changes ACP would have to fit

The provider enum currently has only:

- `Codex`
- `Claude`

Source: [crates/runtime-core/src/provider.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/provider.rs:9)

The `RuntimeProvider` trait already contains the exact ACP-shaped lifecycle surface:

- `list_models`
- `auth_status`
- `auth_set_api_key`
- `auth_import_json`
- `auth_import_json_text`
- `auth_logout`
- `create_session`
- `resume_session`
- `send_turn`
- `interrupt_turn`
- `respond_approval`
- `wait_for_turn`
- `close_session`

Source: [crates/runtime-core/src/provider.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/provider.rs:177)

From a code shape perspective, ACP does not require a new runtime trait. It requires a new adapter that can faithfully implement this existing trait.

### Runtime session manager behavior ACP must match

`RuntimeSessionManager` is where provider results become runtime records and events. Important behaviors:

- `create_session` persists `SessionRecord`, then emits `session.created`: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:478)
- `resume_session` overwrites provider refs, marks session `ready`, emits `session.resumed`: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:582)
- `send_turn` allocates runtime turn IDs and, optionally, approval IDs before calling the provider: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:650)
- approval gating is runtime-owned when permission mode is `require_approval`: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:702)
- successful non-gated sends emit `turn.started` and spawn a waiter: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:818)
- `interrupt_turn` only appends `turn.interrupt_requested`; terminal reconciliation still comes from provider wait completion: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:856)
- `respond_approval` accepts/declines at runtime level, then calls provider `respond_approval`: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:893)
- terminal results are applied centrally in `apply_terminal_result`: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:1105)

ACP therefore does not need to persist GG turns or events itself. It only needs to deliver correct `ProviderTurnAck` and `ProviderTurnResult` semantics back to runtime-core.

### Assistant text persistence path today

The runtime currently persists assistant text indirectly from the provider’s `usage` payload:

- `apply_terminal_result` extracts user text from turn input and assistant text from `usage`
- assistant text is looked up from `last_message`, `lastMessage`, `assistant_text`, or `assistantText`
- transcript rows are appended into `session.metadata.session_transcript`

Sources:

- `apply_terminal_result`: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:1105)
- assistant text extraction: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:1197)
- transcript append: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:1210)

This is the most concrete ACP implication in runtime-core: if ACP provider updates stream assistant text incrementally, but only terminal `ProviderTurnResult.usage` is persisted into transcript, the ACP provider will need to stash final assistant text into `usage` in one of those accepted keys. Otherwise GG will lose assistant transcript persistence even if the live turn UI looked correct.

## Exact repo additions and wiring likely required

### Workspace and dependency surface

New workspace member:

- `crates/runtime-provider-acp`

Likely workspace dependency additions at the root:

- `agent-client-protocol`
- possibly `agent-client-protocol-tokio`

Reason: existing provider crates do not duplicate dependency versions in local manifests when a dependency can live in `[workspace.dependencies]`. ACP dependencies would fit the same pattern as `async-trait`, `serde`, `tokio`, and `tracing`.

Source: [Cargo.toml](/Users/ashray/code/amxv/gg-agent-runtime/Cargo.toml:14)

### New provider crate manifest shape

The ACP crate would likely parallel existing provider manifests:

- `[package]` inheriting workspace version/edition/license/authors
- dependency on `runtime-core`
- `async-trait`, `serde`, `serde_json`, `tokio`
- likely `tracing`
- ACP SDK crates added here or at workspace root

Reference manifests:

- [crates/runtime-provider-codex/Cargo.toml](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-codex/Cargo.toml:1)
- [crates/runtime-provider-claude/Cargo.toml](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-claude/Cargo.toml:1)

### Core enum/metadata changes

Exact runtime-core changes implied by the current code:

- add `ProviderKind::Acp` to [crates/runtime-core/src/provider.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/provider.rs:9)
- update `ProviderKind::as_str()` with `"acp"`: [crates/runtime-core/src/provider.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/provider.rs:15)
- update `ProviderKind::from_str()` to parse `"acp"`: [crates/runtime-core/src/provider.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/provider.rs:22)

No other trait-level change is clearly required from current ACP research.

### Server config changes

The server config currently has:

- `providers.codex`
- `providers.claude`
- `providers.claude_auth_mode`

Source: [crates/runtime-server/src/config.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/config.rs:195)

An ACP provider slot would require at least:

- `providers.acp: ProviderConfig`

and likely one ACP-specific config group rather than overloading generic provider config, because ACP needs launch details that Codex/Claude currently bake in differently:

- command / args for the ACP agent subprocess
- transport preference if more than stdio is ever supported
- perhaps an optional fixed agent identity / registry entry / adapter command
- maybe provider home/config dir if auth/session state is runtime-managed

Why a dedicated ACP config block is likely justified:

- Codex gets staging via `resolve_provider_dir("codex").join("home")` and local auth copy: [crates/runtime-server/src/bootstrap.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/bootstrap.rs:46)
- Claude gets bridge command, bridge args, config dir, auth mode, heartbeat tuning, and GG MCP settings: [crates/runtime-provider-claude/src/lib.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-claude/src/lib.rs:87)

ACP will almost certainly need more than plain `ProviderConfig`.

### Bootstrap changes

Current bootstrap creates providers explicitly and registers them one by one:

- Codex provider init: [crates/runtime-server/src/bootstrap.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/bootstrap.rs:46)
- Claude provider init: [crates/runtime-server/src/bootstrap.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/bootstrap.rs:91)
- provider registration: [crates/runtime-server/src/bootstrap.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/bootstrap.rs:118)

ACP would require:

- a new `use runtime_provider_acp::...`
- ACP provider construction near Codex/Claude construction
- conditional registration when `config.providers.acp.enabled`
- provider home/config directory resolution via `config.resolve_provider_dir("acp")`

Because `bootstrap_runtime` runs startup recovery immediately after registry creation, ACP provider `healthcheck()` and `resume_session()` semantics must already be coherent enough for startup recovery:

- recovery entry point: [crates/runtime-server/src/bootstrap.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/bootstrap.rs:132)

## ACP auth/config exposure in this repo

### What current HTTP/API surface assumes

The provider HTTP surface is currently asymmetric:

- generic:
  - `GET /v1/providers`
  - `GET /v1/providers/{provider}/models`
- Codex-specific auth route:
  - `GET /v1/providers/codex/auth/status`
- Claude-specific auth routes:
  - `GET /v1/providers/claude/auth/status`
  - `POST /v1/providers/claude/auth/api-key`
  - `POST /v1/providers/claude/auth/import-json`
  - `POST /v1/providers/claude/auth/import-file`
  - `POST /v1/providers/claude/auth/logout`

Sources:

- endpoint routing: [crates/runtime-server/src/http.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:50)
- API docs: [docs/API_ENDPOINTS.md](/Users/ashray/code/amxv/gg-agent-runtime/docs/API_ENDPOINTS.md:21)

### What ACP auth should probably expose at first

Based on ACP spec shape, the safest initial auth exposure is narrower than Claude’s:

- `GET /v1/providers/acp/auth/status`

with `ProviderAuthStatus` derived from:

- whether the ACP agent advertises any `authMethods`
- whether the provider has an established authenticated state or cached credential state for the specific configured ACP agent
- whether auth is “agent managed”, “runtime managed”, or “not_applicable”

Why not expose Claude-style mutation endpoints by default:

- ACP standardizes `authenticate` and `logout`, but not a universal “set API key” or “import auth JSON” mutation surface
- different ACP agents may want browser login, device login, API key env, local config files, or no auth
- adding generic `api-key` or `import-json` HTTP endpoints for ACP would falsely imply protocol-wide support

Sources:

- ACP auth model: https://agentclientprotocol.com/protocol/v1/authentication
- current provider auth trait methods are optional and can return `Unsupported`: [crates/runtime-core/src/provider.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/provider.rs:188)

This suggests a repo-consistent first exposure:

- implement `auth_status`
- likely leave `auth_set_api_key`, `auth_import_json`, `auth_import_json_text`, and maybe even `auth_logout` unsupported unless the chosen ACP agent has a stable provider-specific story

If ACP logout is supported by the configured agent, then `POST /v1/providers/acp/auth/logout` would be honest and aligned with the protocol. The other mutation endpoints should be considered adapter-specific, not ACP-generic.

### Config options vs current runtime model

ACP’s `session/set_config_option` is session-scoped, not provider-global. GG’s current HTTP API does not have any session config-option endpoint. That means:

- ACP config options can be used internally by the provider
- exposing them cleanly would require new runtime HTTP/API surface, not just provider implementation

Source:

- ACP config options: https://agentclientprotocol.com/protocol/v1/session-config-options
- no matching runtime endpoint exists in [docs/API_ENDPOINTS.md](/Users/ashray/code/amxv/gg-agent-runtime/docs/API_ENDPOINTS.md:1)

Research conclusion: ACP config options should be treated as internal provider machinery at first unless the runtime product explicitly decides to add session config APIs.

## Lifecycle mapping into current runtime records

### Session create/resume/close

ACP session mapping fits these runtime fields:

- `ProviderSession.provider_session_ref` should hold ACP `sessionId`
- `canonical_provider_session_ref` can stay `None` unless a chosen ACP adapter exposes a second canonical session identity
- runtime session `cwd` should remain the primary ACP `cwd`
- if GG later wants ACP `additionalDirectories`, it will have to source them from worktree/runtime context because `CreateSessionInput` currently only carries `cwd`, model, permission mode, and metadata

Sources:

- create session input: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:19)
- provider session fields: [crates/runtime-core/src/provider.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/provider.rs:90)

Important nuance: runtime recovery already prefers `resume_session` and marks sessions failed when provider resume cannot reconstruct state:

- startup recovery behavior: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:126)

ACP agents that only support `session/load` replay but not `session/resume` would not match GG recovery semantics as cleanly.

### Turn start/wait/interrupt

ACP prompt execution maps to current provider lifecycle as follows:

- `send_turn` should issue ACP `session/prompt`, then return `ProviderTurnAck`
- `wait_for_turn` should be responsible for waiting on ACP completion and returning one terminal `ProviderTurnResult`
- `interrupt_turn` should send ACP `session/cancel`

This matches current Claude shape more than current Codex shape:

- Claude: `send_turn` stores bridge/runtime turn ID maps, `wait_for_turn` does final resolution: [crates/runtime-provider-claude/src/lib.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-claude/src/lib.rs:1508)
- Codex: provider itself owns child process lifecycle and waiter fanout: [crates/runtime-provider-codex/src/lib.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-codex/src/lib.rs:529)

ACP’s RPC/notification model argues for a Claude-like provider structure:

- a live connection/session handle
- provider-local maps for ACP session IDs, request IDs, and turn IDs
- incremental update ingestion while `wait_for_turn` is pending

### `ProviderTurnStatus` mapping

Current runtime terminal statuses are:

- `InProgress`
- `Completed`
- `Interrupted`
- `Failed`

Source: [crates/runtime-core/src/provider.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/provider.rs:106)

ACP stop reasons map approximately as:

- `end_turn` -> `Completed`
- `cancelled` -> `Interrupted`
- `refusal` -> likely `Failed` in runtime terms unless GG wants a richer semantic later
- `max_tokens` -> likely `Failed`
- `max_turn_requests` -> likely `Failed`

Source: https://agentclientprotocol.com/protocol/v1/prompt-turn

This is a semantic compression. Runtime-core currently has no richer terminal enum for “completed with refusal” or “completed with token limit hit.”

### Approval mapping

Current runtime approval flow:

- runtime allocates approval ID before or during turn send for `require_approval`: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:680)
- runtime persists `ApprovalRecord`
- runtime calls provider `respond_approval` once UI/API accepts or declines: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:893)

ACP flow is inverted:

- agent asks client for permission mid-turn using `session/request_permission`
- client responds immediately with chosen option

Source: https://agentclientprotocol.com/protocol/v1/tool-calls

That means ACP provider cannot rely only on GG’s existing “predeclared approval ID before execution” model. It will need a bridge layer that can:

- create `ApprovalRecord`s when ACP requests arrive
- suspend ACP permission responses until runtime approval is resolved
- map GG accept/decline into ACP `selected` / `cancelled` / reject options

This is materially different from current Codex and Claude providers:

- Codex uses provider-side pending approval turns keyed by the approval ID runtime passed in ahead of execution: [crates/runtime-provider-codex/src/lib.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-codex/src/lib.rs:557)
- Claude passes approval responses through a bridge RPC keyed by existing runtime approval ID: [crates/runtime-provider-claude/src/lib.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-claude/src/lib.rs:1572)

ACP is more dynamic because approvals originate inside the protocol stream rather than only from runtime turn policy.

### Assistant text persistence and update normalization

Current runtime persistence happens only when `ProviderTurnResult` becomes terminal. Runtime does not have a provider hook for incremental assistant chunks. It only emits runtime events after provider wait completes.

Sources:

- terminal result application: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:1105)
- turn event emission uses `assistant_text` only from terminal result payload: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:1148)

This has two concrete ACP consequences:

- if ACP provider wants live `session/update` chunks reflected in runtime event history before turn end, runtime-core currently has no provider-facing incremental event API
- if ACP provider only surfaces final assistant text via terminal `usage`, transcript persistence will still work, but granular intra-turn event fidelity will be weaker than ACP itself

Research conclusion: ACP can fit today’s runtime contract, but the fit is terminal-result-centric, not stream-native.

## Interaction with team spawn, worktrees, processes, and MCP

### Team spawn and provider selection

Team/member spawn currently routes through runtime-tools, not through any provider-specific API:

- `spawn_team_member` creates a runtime session via `CreateSessionInput` and joins it to the team: [crates/runtime-tools/src/lib.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-tools/src/lib.rs:2484)
- spawn-time provider choice comes from the request and is already mixed-provider capable
- there is an existing HTTP integration test named `mixed_provider_team_flow_uses_shared_runtime_services`: [crates/runtime-server/src/http.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:3946)

This is important: ACP does not need any team-specific architecture. If ACP can behave like any other provider session, team spawn should work unchanged.

### Worktree assignment

Spawn/member and worktree flow are runtime-owned:

- worktree creation/claim/release/cleanup live in `RuntimeWorktreeService`: [crates/runtime-tools/src/lib.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-tools/src/lib.rs:2081)
- spawn flow picks `spawn_cwd` from claimed/created worktree and uses that when creating the provider session: [crates/runtime-tools/src/lib.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-tools/src/lib.rs:2614)

Implication for ACP:

- ACP should receive the assigned worktree path as `cwd`
- ACP does not need to know about GG worktree IDs
- ACP-specific logic only becomes necessary if GG later wants to expose multiple roots as ACP `additionalDirectories`

### Processes and terminals

GG’s native process model is runtime-owned and already exposed through MCP/gateway:

- runtime process manager: [crates/runtime-tools/src/lib.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-tools/src/lib.rs:61)
- runtime tool gateway dispatch for `gg_process_*`: [crates/runtime-tools/src/lib.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-tools/src/lib.rs:866)
- GG MCP process tool wrappers: [sidecars/gg-mcp-server/src/server.rs](/Users/ashray/code/amxv/gg-agent-runtime/sidecars/gg-mcp-server/src/server.rs:126)

ACP terminal methods are a parallel capability, not a free win. If ACP provider advertises client `terminal` capability and implements it by delegating into GG process manager, that creates a second process-control surface with different semantics and fewer persisted guarantees than the existing GG process tooling.

Research conclusion: ACP should preferentially consume GG via MCP server injection, not via ACP client terminal emulation.

### MCP injection path

Claude already proves the pattern GG uses to hand provider sessions a GG MCP server:

- provider-side GG MCP config builder: [crates/runtime-provider-claude/src/lib.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-claude/src/lib.rs:241)
- bootstrap composes gateway URL and bearer token for provider sessions: [crates/runtime-server/src/bootstrap.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/bootstrap.rs:87)

ACP session setup explicitly supports client-supplied MCP server definitions:

- https://agentclientprotocol.com/protocol/v1/session-setup

This is the cleanest bridge between ACP and the rest of GG.

## Existing tests that define the likely ACP test surface

### Runtime-core tests already exercising provider contract behavior

`crates/runtime-core/src/runtime.rs` already has strong contract tests around provider integration, including:

- one active turn per session
- duplicate terminal event idempotency/conflict handling
- send failure recovery
- approval requested/resolved transitions
- explicit resume path updates
- startup recovery with approvals and stale turns

Representative test area: [crates/runtime-core/src/runtime.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-core/src/runtime.rs:1239)

These are the most important surfaces ACP must satisfy because they validate runtime semantics rather than provider internals.

### Provider-local test styles

Codex provider tests cover prompt building, command arg construction, auth status, approval path, and waiting:

- test block starts around [crates/runtime-provider-codex/src/lib.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-codex/src/lib.rs:803)

Claude provider tests are broader and closer to ACP needs because they cover:

- auth transitions
- bridge/config behavior
- create/resume/close/send/interrupt/wait
- GG MCP wiring and real smokes

Test block starts around [crates/runtime-provider-claude/src/lib.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-provider-claude/src/lib.rs:3087)

ACP provider tests would likely need to resemble Claude’s shape more than Codex’s.

### HTTP/runtime integration tests already present

`crates/runtime-server/src/http.rs` already contains integration coverage for:

- public/protected OpenAPI routes: [crates/runtime-server/src/http.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:2857)
- diagnostics routes: [crates/runtime-server/src/http.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:3246)
- Claude auth endpoints: [crates/runtime-server/src/http.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:3360)
- mixed-provider teams: [crates/runtime-server/src/http.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:3946)
- team lifecycle and events: [crates/runtime-server/src/http.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:5584)
- spawn with worktrees and cleanup: [crates/runtime-server/src/http.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:6518)
- real Codex and real Claude smokes: [crates/runtime-server/src/http.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:3495)

These existing tests define what “ACP is actually integrated” would mean in this repo:

- ACP appears in provider list/diagnostics/models/auth surface
- ACP sessions can be created through HTTP
- ACP sessions can participate in mixed-provider team flows
- ACP sessions can be spawned into worktree-backed teammates
- OpenAPI and docs stay synchronized

### Bootstrap/config tests already present

Bootstrap tests currently cover:

- failing when all providers disabled
- registering enabled providers
- disabling processes/worktrees
- wiring worktree deletion policy

Source: [crates/runtime-server/src/bootstrap.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/bootstrap.rs:280)

Config tests currently cover default scaffolding, auth bootstrap, and relative-path resolution:

- [crates/runtime-server/src/config.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/config.rs:287)

Adding ACP expands both surfaces:

- bootstrap test expectations for provider count and provider registration
- config default assertions for `providers.acp`

## Docs and OpenAPI sync surfaces

This repo explicitly treats API docs as part of the same change whenever runtime API behavior changes:

- policy: [AGENTS.md](/Users/ashray/code/amxv/gg-agent-runtime/AGENTS.md:1)
- workflow: [docs/API_DOC_SYNC.md](/Users/ashray/code/amxv/gg-agent-runtime/docs/API_DOC_SYNC.md:1)

If ACP adds or changes HTTP provider routes, the following repo surfaces are implicated:

- route source: [crates/runtime-server/src/http.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/http.rs:50)
- source-parsing OpenAPI generator: [crates/runtime-server/src/openapi.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/openapi.rs:40)
- generated artifact: [openapi/runtime-server-openapi.yaml](/Users/ashray/code/amxv/gg-agent-runtime/openapi/runtime-server-openapi.yaml:1)
- narrative docs:
  - [docs/API.md](/Users/ashray/code/amxv/gg-agent-runtime/docs/API.md:1)
  - [docs/API_ENDPOINTS.md](/Users/ashray/code/amxv/gg-agent-runtime/docs/API_ENDPOINTS.md:1)

Important repo-specific nuance: the OpenAPI generator is source-parsing based, not runtime reflection based. It hardcodes route summaries and request-body recognition by path. So adding ACP routes is not only “update http.rs and rerun generator”; if new provider auth routes are introduced, `openapi.rs` may also need explicit summary and request-body updates.

Evidence:

- route collection by parsing `http.rs`: [crates/runtime-server/src/openapi.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/openapi.rs:136)
- hardcoded operation summaries for existing provider routes: [crates/runtime-server/src/openapi.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/openapi.rs:76)
- hardcoded JSON body route list: [crates/runtime-server/src/openapi.rs](/Users/ashray/code/amxv/gg-agent-runtime/crates/runtime-server/src/openapi.rs:222)

## Risks and unresolved questions that are specific to this repo

### 1. ACP is more stream-oriented than runtime-core’s provider contract

Runtime-core currently wants a terminal `ProviderTurnResult`, then persists transcript and emits final turn events. ACP can stream message chunks, tool-call updates, plan updates, usage updates, and session info updates before the prompt result resolves.

Repo-specific question:

- Is ACP allowed to fit into today’s terminal-result contract only, or does GG eventually need a provider-facing incremental event API?

### 2. Approval origin mismatch is not cosmetic here

In this repo, approval IDs are often allocated by runtime before provider execution begins. ACP permissions originate mid-stream from the agent. That mismatch is deeper than simply mapping a field name.

Repo-specific question:

- Should runtime-core gain a provider callback path for provider-originated approvals, or should ACP provider emulate them internally and only surface final approval records to runtime after the fact?

### 3. Model list endpoint may become misleading

This repo already exposes `/v1/providers/{provider}/models`. ACP may only have per-session config options, not a provider-global model catalog.

Repo-specific question:

- Is returning an empty model list acceptable for ACP in this product, or does the product expect models to be first-class for every provider?

### 4. Config-option support has no first-class runtime API

ACP’s session config options are real protocol surface, but current runtime HTTP routes have nowhere to expose them.

Repo-specific question:

- Should ACP provider silently consume session config options internally, or should the absence of config-option APIs be treated as a product mismatch that needs broader runtime design later?

### 5. Startup recovery expectations are strict

This repo’s startup recovery will call provider `resume_session` and may respawn waits for active turns. ACP agents that cannot resume in-flight prompt work cleanly could cause sessions to be marked failed more often than Codex/Claude.

Repo-specific question:

- Is “resume session context but fail active turns” acceptable behavior for ACP in this runtime?

### 6. Sidecar choice is architectural, not cosmetic

Because this repo already has one bridge-style provider and one direct provider, ACP can plausibly go either direction. But the choice affects:

- bootstrap config shape
- diagnostics
- recovery complexity
- test style
- MCP wiring path

Repo-specific observation:

- The current codebase has a stronger precedent for direct provider logic when the provider is subprocess-local and line-oriented, and a stronger precedent for sidecars when provider lifecycle is bridge/RPC-heavy and auth/config are richer.

ACP’s official Rust SDK makes direct integration technically credible, but if the selected ACP agent is itself an adapter binary with its own opinions, the repo may end up looking more Claude-like than Codex-like.

## Bottom line

ACP is a plausible third provider for this repo without redesigning GG Runtime, but the fit is best understood as “another adapter into the existing runtime contract,” not as “GG becomes an ACP-native client.” The exact repo work is concentrated in workspace membership, `ProviderKind`, runtime-server config/bootstrap/HTTP/OpenAPI wiring, a new provider crate, and careful mapping of ACP prompt/update/permission semantics into the terminal-result-centric runtime-core contract.

The sharpest codebase-specific constraints are these:

- assistant transcript persistence currently depends on terminal `usage` payload fields, not streamed deltas
- approval flow is runtime-originated today, while ACP approval flow is provider-originated
- model/config discovery in ACP does not match the current provider-global HTTP surface cleanly
- OpenAPI/docs sync will require explicit `openapi.rs` adjustments if ACP adds auth endpoints
- team/worktree/process behavior should remain provider-agnostic and be reached through existing GG MCP/server/runtime services rather than ACP client filesystem or terminal capabilities
