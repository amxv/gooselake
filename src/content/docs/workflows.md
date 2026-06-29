---
title: Operator workflows
description: The workflows that matter once Gooselake is running real agent work on a real machine.
order: 4
category: Operator Workflows
summary: A field guide to the repeatable operational motions around sessions, execution, and recovery.
---

## Session lifecycle

Treat sessions as durable runtime objects, not UI conveniences.

An operator workflow usually looks like this:

1. Create or reconnect to the session.
2. Launch or continue turns through the runtime.
3. Observe streamed events and approvals.
4. Replay or inspect history when something needs explanation.

This is what lets multiple clients or operators interact with the same ongoing work without inventing custom sync logic.

## Long-running execution

The runtime is where long-lived work belongs.

That includes:

- turns that outlive one client connection
- host processes that need inspection or later cleanup
- file and repo mutations that must be attributable

In other words, if it needs receipts, retries, or later inspection, let the runtime own it.

## Team-style coordination

Gooselake also supports team communication patterns for multi-agent or operator-assisted flows. That matters when:

- one agent researches while another implements
- a lead needs handoff evidence
- delivery status has to be explicit

Once coordination becomes part of the runtime contract, it stops being hidden app glue.

## Recovery mindset

Operationally, the right question after any interruption is:

> What does the runtime already know, and how do I resume from there?

Because state is persisted, the answer should come from replay and inspection, not operator memory.
