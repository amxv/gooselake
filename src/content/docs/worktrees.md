---
title: "Worktrees"
description: "Manage workspace-owned Git worktrees, provider-verified agent routes, and safe cleanup."
order: 33
category: "Runtime Services"
summary: "Managed Git workspaces with durable ownership and cleanup policy."
---

Worktrees give agents a safe room to work in. Claims are the keys to those rooms.

That separation is the core idea: a worktree can exist independently, and one or more records can explain who currently owns or used it. Cleanup can then be based on durable claims rather than guesswork.

## Workspace-owned worktrees (preferred `/v2` API)

Normal agents belong to one workspace. Use `GET /v2/workspaces/{workspace_id}/worktrees` to inspect managed checkouts for that workspace's canonical Git repository, including native Git identity, attached agents, retention policy, revision, and any assignment blockers. Create or reuse a named checkout with `POST /v2/workspaces/{workspace_id}/worktrees`:

```bash
curl -X POST "$BASE_URL/v2/workspaces/$WORKSPACE_ID/worktrees" \
  "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"worktree_name":"agent-branch","run_init_script":false}'
```

On `POST /v2/workspaces/{workspace_id}/agents`, the `worktree` selector can choose the `root`, an `existing` managed worktree ID, or a `new` named checkout. The runtime commits the selected cwd and managed-worktree claim along with agent ownership. No separate manual claim is required for this flow.

To move an **idle** agent, call `POST /v2/workspaces/{workspace_id}/agents/{agent_id}/worktree` with `worktree_id` (or `null` for the workspace root), the agent's `expected_revision`, and an `Idempotency-Key`. The provider must confirm the effective cwd and session identity before the runtime commits the changed route; unsupported providers cannot silently rebind. Inspect the durable rebind through `GET /v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}`. An ambiguous native outcome blocks further agent work until recovery rather than guessing its cwd or blindly repeating provider dispatch.

Previous checkouts are preserved unless `cleanup_previous_worktree:true` was explicitly requested. After a verified rebind, cleanup refuses dirty, unmerged, foreign, or occupied paths. If cleanup remains pending, `POST /v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}/cleanup` retries **only** native cleanup, never the provider rebind. See the [API guide](/docs/api#workspace-worktree-routes) for complete request examples and recovery semantics.

## Legacy `/v1` worktree operations

The endpoints below remain for existing clients during migration. Workspace-owned agent creation and reassignment should use the `/v2` authority above rather than manually manipulating claims or cwd.

## What the worktree service owns

The worktree service owns:

- managed worktree records
- repo root and worktree cwd
- branch name and base ref
- creator session/team/member references
- cleanup policy
- claims
- claim release timestamps
- startup repair of inconsistent records
- best-effort cleanup diagnostics

## Create a worktree

```bash
curl -X POST "$BASE_URL/v1/worktrees"   "${AUTH[@]}"   -H 'Content-Type: application/json'   -d '{
    "source_session_id": "sess_codex_...",
    "repo_root": "/path/to/repo",
    "worktree_name": "agent-branch",
    "branch_prefix": "gg",
    "base_ref": "main",
    "deletion_policy": "delete_on_last_claim"
  }'
```

The runtime stores the resulting worktree path and branch metadata. If configured, it can also run an init script after creation.

## Claim a worktree

```bash
curl -X POST "$BASE_URL/v1/worktrees/$WORKTREE_ID/claims"   "${AUTH[@]}"   -H 'Content-Type: application/json'   -d '{
    "session_id": "sess_codex_...",
    "claim_role": "owner"
  }'
```

A claim says a session is using the worktree. The runtime can enforce ownership decisions and later release or clean up safely.

## Release a claim

```bash
curl -X POST "$BASE_URL/v1/worktrees/$WORKTREE_ID/release"   "${AUTH[@]}"   -H 'Content-Type: application/json'   -d '{
    "session_id": "sess_codex_...",
    "cleanup_if_last_claim": true
  }'
```

Releasing a claim does not necessarily delete the worktree. Cleanup policy decides what happens next.

## Cleanup

```bash
curl -X POST "$BASE_URL/v1/worktrees/$WORKTREE_ID/cleanup" "${AUTH[@]}"
```

Cleanup respects active claims. This is important: the runtime should not delete a room while someone still holds a key.

## Spawn integration

Team spawn can create or reuse a worktree for a new member. The spawn operation then:

1. prepares the worktree
2. creates the provider session with the worktree cwd
3. joins the session to the team
4. claims the worktree
5. sends onboarding instructions

If a later step fails, the runtime attempts rollback and records diagnostics.

## Startup repair

The worktree service performs unusually careful startup repair. It can:

- normalize stored paths and fields
- merge duplicate worktree records by identity
- rewrite claims from duplicate records to the surviving record
- release impossible claims
- merge duplicate claims
- enforce one active claim per session
- preserve diagnostics for operator inspection

This makes worktree state resilient to earlier bugs, interrupted operations, and manual host changes.

## Path model

`worktrees.root_dir` is resolved under the runtime data root unless it is absolute. Branch names are sanitized into path-safe components.

See [Configuration reference](/docs/configuration) for path resolution details.

## Diagnostics

Inspect worktree state with:

```bash
curl "$BASE_URL/v1/worktrees" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/worktrees" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/recovery" "${AUTH[@]}"
```

When cleanup does not happen, check active claims first.
