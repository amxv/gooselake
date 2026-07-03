---
title: "Teams and comms"
description: "Use Gooselake teams, members, durable messages, delivery policies, retries, cancellation, and spawn operations."
order: 31
category: "Runtime Services"
summary: "The durable transport layer for multi-agent coordination."
---

Teams are one of the most important parts of Gooselake. They turn “multiple agents” from a prompt trick into runtime state.

A team is like a dispatch room. Members sit at desks, messages enter a delivery queue, and the runtime decides when and how to inject those messages without corrupting active work.

## Core objects

| Object | Meaning |
| --- | --- |
| Team | A named coordination space |
| Member | A runtime session that belongs to a team |
| Message | A durable direct message or broadcast |
| Delivery | A per-recipient attempt to inject a message |
| Operation journal | A record of multi-step team operations such as spawn |
| Diagnostic | A structured note when a team operation needs explanation |

Messages and deliveries are separate on purpose. A message can exist even if one recipient is busy, failed, or waiting for a safer injection point.

## Delivery states

Deliveries move through a small state machine:

```text
pending -> deferred -> injecting -> injected
        -> cancelled
        -> failed
```

A delivery may be deferred when the recipient has an active turn and the policy does not allow immediate interruption.

## Delivery policies

Delivery policy controls how aggressive the runtime may be when a recipient is busy.

| Policy | Behavior |
| --- | --- |
| `non_interrupting` | Do not interrupt active work; defer instead. |
| `start_new_turn_only` | Deliver only when the recipient can accept a new turn. |
| `interrupt_after_tool_boundary` | Wait for a turn boundary, then interrupt/inject when safe. |
| `immediate_interrupt` | Interrupt active work immediately to inject the message. |

The default stance should usually be conservative. Interrupting an agent mid-turn is powerful and should be explicit.

## Direct messages

A direct message targets one recipient session in the team.

```bash
curl -X POST "$BASE_URL/v1/teams/$TEAM_ID/messages"   "${AUTH[@]}"   -H 'Content-Type: application/json'   -d '{
    "sender_agent_id": "sess_codex_...",
    "recipient_agent_id": "sess_claude_...",
    "input": {"text": "Please review the migration plan before implementation."},
    "policy": "non_interrupting",
    "idempotency_key": "review-request-001"
  }'
```

Use idempotency keys when an external client might retry the same send.

## Broadcasts

A broadcast creates one message and one delivery per recipient.

```bash
curl -X POST "$BASE_URL/v1/teams/$TEAM_ID/broadcasts"   "${AUTH[@]}"   -H 'Content-Type: application/json'   -d '{
    "sender_agent_id": "sess_codex_...",
    "input": {"text": "New constraint: do not run tests on this branch."},
    "policy": "start_new_turn_only"
  }'
```

The team snapshot endpoint is useful for rendering messages and delivery state together:

```bash
curl "$BASE_URL/v1/teams/$TEAM_ID/view" "${AUTH[@]}"
```

## Retry and cancellation

Deliveries can be retried when the operator believes conditions have changed:

```bash
curl -X POST "$BASE_URL/v1/teams/$TEAM_ID/deliveries/$DELIVERY_ID/retry" "${AUTH[@]}"
```

A message can be cancelled when it should no longer be delivered:

```bash
curl -X POST "$BASE_URL/v1/teams/$TEAM_ID/messages/$MESSAGE_ID/cancel" "${AUTH[@]}"
```

Cancellation is a runtime record. It should not be simulated by hiding the message in a UI.

## Spawn operations

Team spawn creates a new provider session, optionally creates or reuses a worktree, joins the new session to the team, claims the worktree, and sends onboarding instructions.

That is a multi-step operation, so the implementation treats it like a saga:

1. journal the planned operation
2. prepare worktree if requested
3. create provider-backed session
4. join the member to the team
5. claim the worktree if needed
6. send onboarding message
7. record completion or rollback diagnostics

If a later step fails, the runtime attempts cleanup and records diagnostics instead of leaving the operator guessing.

## Interrupt all

A team can interrupt all active member turns:

```bash
curl -X POST "$BASE_URL/v1/teams/$TEAM_ID/interrupt-all" "${AUTH[@]}"
```

This is an operational control, not a chat feature. Use it when the team needs to stop work because instructions changed, a branch is wrong, or a provider is stuck.

## Events

Team events are replayable:

```bash
curl "$BASE_URL/v1/teams/$TEAM_ID/events" "${AUTH[@]}"
curl -N "$BASE_URL/v1/teams/$TEAM_ID/events/stream" "${AUTH[@]}"
```

A good team UI renders both messages and deliveries. A message without delivery state is only half the story.

## Diagnostics

Use team diagnostics when spawn, delivery, or membership behavior is surprising:

```bash
curl "$BASE_URL/v1/diagnostics/comms" "${AUTH[@]}"
curl "$BASE_URL/v1/diagnostics/team-operations?team_id=$TEAM_ID" "${AUTH[@]}"
```

The operation journal is especially useful for spawn failures because it shows which stage succeeded before rollback began.
