---
title: Usage model
description: Learn how frontends, operators, and runtime APIs fit together in daily use.
order: 3
category: Core Concepts
summary: A practical view of how consumers talk to the runtime and what the runtime is expected to own.
---

## The thin-client pattern

The healthiest way to use Gooselake is to keep clients deliberately thin.

A client should focus on:

- presenting session state
- initiating turns
- rendering stream updates
- surfacing approvals and operator controls

The runtime should own the behavior that must remain true even when the client disconnects.

## HTTP for control, SSE for flow

Gooselake exposes HTTP endpoints for command and control, with SSE for the event stream that clients consume in real time.

That combination gives you:

- explicit request boundaries
- low-friction stream consumption
- reconnectable delivery
- a clean separation between control messages and event playback

## A practical frontend stance

When building on top of the runtime, prefer this question:

> Can the UI disappear right now without corrupting the session’s truth?

If the answer is no, too much orchestration has leaked into the client.

## Good uses of the runtime

- A desktop client that launches and observes turns against the same host over time.
- A web app that reconnects to a live session and can replay missing events.
- An internal operator console that needs to inspect agent state without reimplementing provider integrations.

## Bad uses of the runtime

- Treating it like a token proxy while session truth still lives in React state.
- Hiding long-running machine operations behind a frontend lifecycle.
- Building one-off provider logic in the UI and using the runtime only for whatever is convenient that day.
