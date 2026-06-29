---
title: Core concepts
description: Understand the runtime boundary, the event model, and the relationship between clients, providers, and the host machine.
order: 2
category: Core Concepts
summary: The mental model for why the runtime exists and where its responsibilities begin and end.
---

## The central boundary

The important system boundary is not between your client and a model provider.

It is between your client and the runtime.

Once that boundary is real, the frontend stops owning concerns it is bad at carrying:

- provider auth staging
- stream lifetime
- durable turn history
- process lifetime
- worktree and filesystem control
- recovery after disconnects

## Clients are remote controls

A browser, desktop UI, CLI, or ops console should all be able to speak to the same runtime contract. They are different cockpits, not different engines.

This makes the UI replaceable and lets agent behavior survive client churn.

## Providers are adapters

Gooselake supports multiple provider backends, but the product architecture should not be rewritten every time a provider adds a feature or changes a transport detail.

The runtime absorbs those differences and exposes a stable operating model for:

- sessions
- turns
- approvals
- streamed assistant output
- recovery and replay

## Events are durable receipts

When sessions are persisted and streamed from the runtime, the system can explain what happened:

- which turn started
- which output was emitted
- whether a tool ran
- what delivery state a team message reached
- what the operator needs to replay or inspect later

That is a major difference from frontend state that quietly disappears when the tab reloads.

## Machine-side work is first-class

The runtime is designed for agents that operate on the machine itself. That includes:

- filesystem mutations
- process management
- worktree allocation
- tool routing through MCP sidecars

If the host is where the work occurs, the host is where the control plane should live.
