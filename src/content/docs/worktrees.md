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

The SQLite inventory reads only matching repository worktrees and their live claims in one database snapshot, while native Git checks still determine whether each route is actually usable. Historical aliases of the repository root remain visible; a released claim cannot block assignment, but a live claim outside the active roster can.

When assigning a single managed checkout or revalidating a provider's rebinding evidence, the runtime looks up that exact persisted worktree ID rather than scanning unrelated runtime history. This indexed read is not a substitute for native Git identity verification: missing, stale or foreign-repository routes still fail closed.

```bash
curl -X POST "$BASE_URL/v2/workspaces/$WORKSPACE_ID/worktrees" \
  "${AUTH[@]}" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: create-agent-branch' \
  -d '{"worktree_name":"agent-branch","run_init_script":false}'
```

The idempotency key replays the original Git/initializer result. Native creation
records a durable operation before Git executes; an unknown outcome returns
non-retryable `409 outcome_unresolved` with `X-Operation-ID`. Inspect that exact
operation before recovery, rather than using a new key to repeat Git or an init
script. An omitted deletion policy preserves an existing checkout's retention
setting, and uses the server default for newly created checkouts.

The runtime treats all paths sharing one Git repository identity as a single
native lifecycle boundary, even if a workspace is registered in a subdirectory
or a linked worktree. Retirement of a workspace fences legacy `/v1` native
checkout and provider-session creation against that same Git repository; using
another checkout or an active sibling workspace does not bypass the freeze.

On `POST /v2/workspaces/{workspace_id}/agents`, the `worktree` selector can choose the `root`, an `existing` managed worktree ID, or a `new` named checkout. The runtime commits the selected cwd and managed-worktree claim along with agent ownership. No separate manual claim is required for this flow.

To move an **idle** agent, call `POST /v2/workspaces/{workspace_id}/agents/{agent_id}/worktree` with `worktree_id` (or `null` for the workspace root), the agent's `expected_revision`, and an `Idempotency-Key`. The provider must confirm the effective cwd and session identity before the runtime commits the changed route; unsupported providers cannot silently rebind. Inspect the durable rebind through `GET /v2/workspaces/{workspace_id}/agents/{agent_id}/rebinds/{operation_id}`. An ambiguous native outcome blocks further agent work until recovery rather than guessing its cwd or blindly repeating provider dispatch.

Previous checkouts are preserved unless `cleanup_previous_worktree:true` was explicitly requested. After a verified rebind, cleanup refuses dirty, unmerged, foreign, or occupied paths. The explicit rebind-cleanup endpoint can reconsider a pre-admission safety blocker or replay an existing completed cleanup receipt. Once native cleanup has been admitted, an unknown outcome remains fenced and neither Git action is redispatched. It never repeats the provider rebind. See the [API guide](/docs/api#workspace-worktree-routes) for complete request examples and recovery semantics.

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
- persisted claim lifetime tokens
- claim release timestamps
- startup repair of inconsistent records
- best-effort cleanup diagnostics

## Create a worktree

Use the registered workspace's durable creation authority:

```bash
curl -X POST "$BASE_URL/v2/workspaces/$WORKSPACE_ID/worktrees" \
  "${AUTH[@]}" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: worktree-agent-branch' \
  -d '{"worktree_name":"agent-branch","branch_prefix":"gg","base_ref":"main","deletion_policy":"retain_on_last_claim","run_init_script":false}'
```

The runtime freezes Git/init intent before native work and returns an operation
ID for exact receipt replay or unresolved-outcome inspection. The workspace
supplies repository identity. `POST /v1/worktrees` is unsupported and never runs
Git or an init script. Legacy claim/release/cleanup endpoints below retain their
separate guarded contracts.

## Claim a worktree

```bash
curl -X POST "$BASE_URL/v1/worktrees/$WORKTREE_ID/claims"   "${AUTH[@]}"   -H 'Content-Type: application/json'   -d '{
    "session_id": "sess_codex_...",
    "claim_role": "owner"
  }'
```

A claim says a session is using the worktree. The runtime can enforce ownership decisions and later release or clean up safely.
Public `claim_role` values are `owner` or `consumer` (trimmed and case-insensitive).
`rebind_reservation` is **internal**: only the native provider-rebind admission
transaction may reserve a destination worktree. A public claim cannot create
one of these recovery-sensitive reservations.

## Release a claim

Read the exact current claim once to prepare a release. Its `claim_generation`
is required as `expected_claim_generation`; the accepted range is
`1..=9223372036854775807`. The authenticated GET is read-only and returns 404
for a missing/tombstoned worktree or missing exact-session claim.

```bash
RELEASE_BODY="$(
  set -o pipefail
  curl -fsS "${AUTH[@]}" \
    "$BASE_URL/v1/worktrees/$WORKTREE_ID/claims/$SESSION_ID" |
  python3 -c '
import json, sys
claim = json.load(sys.stdin)
generation = claim.get("claim_generation")
if type(generation) is not int or not 1 <= generation <= 9223372036854775807:
    raise SystemExit("The observed claim has no valid lifetime token")
print(json.dumps({
    "session_id": claim["session_id"],
    "expected_claim_generation": generation,
    "cleanup_if_last_claim": True
}))
'
)" || exit 1

curl -fsS -X POST "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d "$RELEASE_BODY" \
  "$BASE_URL/v1/worktrees/$WORKTREE_ID/release"
```

This GET prepares a new release request. Retain `RELEASE_BODY` unchanged when
retrying that request. HTTP 409 means its observed lifetime is stale or another
safety fence applies; do not silently refresh the token and release a newer
claim. Even identical role and timestamp values can belong to a later lifetime.
An exact terminal retry preserves the original release and appends no additional release event.
Claim release does not itself prove that checkout cleanup completed.

## Cleanup

```bash
curl -X POST "$BASE_URL/v1/worktrees/$WORKTREE_ID/cleanup" "${AUTH[@]}"
```

Cleanup respects active claims, live native bindings and retention policy. It
requires its original checkout-removal and branch-deletion acknowledgments in
order. Unknown or incomplete outcomes retain their operation and reservations
for inspection. Later path or branch absence does not authorize completion or
another deletion attempt. A completed receipt replays without running Git.

## Agent creation and historical spawn

Workspace-owned agent creation uses the durable workspace operation described
above. Git, initializer and provider actions retain separate original outcome
evidence. An unknown result does not authorize another execution or destructive
compensation. The retired legacy team-spawn path only inspects original keyed
receipts; it does not create new members or replay uncertain native work.

## Startup repair

Historical startup normalization can:

- normalize stored paths and fields
- merge duplicate worktree records by identity
- rewrite claims from duplicate records to the surviving record
- release impossible claims
- merge duplicate claims
- enforce one active primary claim per session while preserving unresolved rebind reservations
- preserve diagnostics for operator inspection

Changed record and claim repairs commit together only while their original
SQLite snapshot and claim lifetimes still match. The complete claim keyset is
checked before and after the repair. Existing destination keys retain their own
tokens; source tokens do not become destination authority. Unchanged rows remain
untouched, and stale or conflicting plans fail as a whole. This normalization
does not establish unknown provider or Git outcomes or authorize cleanup.

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
