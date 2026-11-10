---
title: "Security model"
description: "Understand bearer auth, local trust boundaries, provider credentials, process execution, MCP caller identity, and deployment precautions."
order: 53
category: "Operators"
summary: "The security assumptions behind a host-owned agent runtime."
---

Gooselake is powerful because it runs on the machine where real work happens. That means the security model should be understood before exposing it beyond localhost.

The short version: treat Gooselake like an operator console with filesystem and process access, not like a public chat endpoint.

## Trust boundary

Gooselake is designed as a single-user or tightly controlled host runtime. It is not a multi-tenant SaaS control plane.

The runtime can:

- create provider-backed sessions
- stage provider auth
- run host commands
- read process logs
- create Git worktrees
- let provider sessions call runtime tools through MCP
- send messages between agents

Anyone with API access can potentially cause meaningful machine-side work.

## HTTP auth

Public routes:

- `GET /health`
- `GET /openapi.yaml`

Protected routes:

- all `/v1/**` routes
- operator `/v2/**` routes other than the independently authenticated `/v2/mcp/**` gateway

The `/v2/mcp/**` gateway uses an agent-scoped credential rather than the
operator bearer secret; an operator token alone cannot impersonate an agent.
The scoped MCP gateway also rejects duplicate `Authorization` headers with
HTTP `401`, including when one or both supplied agent credentials are valid.

Operator-protected routes require:

```http
Authorization: Bearer <token>
```

The operator API requires **exactly one** `Authorization` header. Duplicate
headers are rejected with HTTP `401` even if one or both contain a valid
token, preventing disagreement between proxies and the runtime over which
credential should be trusted. This is enforced on both `/v1` and operator
`/v2` routes; their respective legacy/typed error bodies remain unchanged.

The token can come from inline config or a token file. If no token is configured, the runtime can create token material under its data directory during bootstrap depending on configuration.

## Binding and reverse proxy stance

For production, bind the runtime to localhost or a private interface and put a trusted reverse proxy in front when needed.

Avoid exposing the raw runtime port directly to the public internet unless you fully understand the risk and have additional network controls.

## Provider credentials

Provider auth is local host state.

- Codex auth is staged from host credentials into the runtime provider home.
- Claude supports host-machine and runtime-managed auth modes.
- ACP is agent-managed; the runtime exposes status/configuration but does not own a universal ACP login flow.

Keep runtime data directories out of public backups and logs.

## Process execution risk

Runtime processes are intentionally powerful. If `[processes].enabled = true`, callers can start commands according to runtime config.

Review:

- whether shell mode is allowed
- maximum concurrency
- timeouts
- output size limits
- which host user runs the service
- what filesystem paths that user can access

Run the service as the least-privileged user that still has the project access you need.

## MCP caller identity

The **model-facing `/v2/mcp` gateway** does not accept a body-supplied
caller identity. Its runtime-issued scoped Bearer token binds one active
canonical agent and its allowed GG tools; the gateway derives the
workspace and caller from that credential. Model-authored JSON may only
select the advertised `gg_team`, `gg_message` or `gg_process` mode and
its allowed fields, never `sender_agent_id`, `workspace_id`, `team_id`
or a custom principal. Archived agents, closed native sessions and
retired workspaces are not routable.

For MCP-backed providers the sidecar forwards an independently minted
`x-gg-tool-invocation-id` header; native Codex tooling binds the provider
tool-call identity inside the trusted runtime. Mutating requests without
one valid trusted invocation ID fail before admission. Duplicate headers
are rejected rather than choosing an arbitrary value. Exact invocation
identity enables idempotent result replay; an ambiguous effect returns
`outcome_unresolved`, not an instruction to retry blindly.

`/v1/mcp` still exists for operator-authenticated legacy compatibility.
The operator token cannot impersonate an agent at `/v2/mcp`, and an
agent-scoped MCP token cannot authorize workspace administration.

## Worktree safety

Worktrees are real Git workspaces on disk. The runtime tracks claims and cleanup policy, but Git operations still affect real repositories.

Use separate worktree roots, clear branch prefixes, and conservative cleanup policies for important repos.

## Logs and data retention

Process logs and runtime SQLite state may contain sensitive prompts, command output, file paths, provider errors, and agent messages.

Protect:

- SQLite database path
- provider directories
- process log directories
- release bundle config files
- systemd environment files
- reverse proxy logs

## Practical checklist

Before running outside a local dev shell:

- bind to localhost or private network
- use a strong bearer token
- run as a dedicated OS user
- confirm provider auth paths
- limit shell/process behavior as needed
- keep data root permissions tight
- back up SQLite and config intentionally
- inspect `/v1/diagnostics` after boot
