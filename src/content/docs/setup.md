---
title: Setup
description: Install Gooselake, run the runtime locally, and get to the first working control plane.
order: 1
category: Start Here
summary: Install the runtime, create a config, and launch the service with the smallest possible path.
---

## What you are starting

Gooselake is not a theme, SDK snippet, or browser widget. You are starting a machine-side runtime that exposes agent control over HTTP and SSE while keeping execution close to the host.

That means your first success criterion is simple:

- the runtime installs cleanly
- you can launch it with a local config
- a frontend or operator can connect without embedding orchestration logic in the client

## Local install

Use the repo helpers for the fastest local path:

```bash
make install
cp "$HOME/.local/runtime-server.toml.example" ./runtime-server.toml
gg-runtime-server --config ./runtime-server.toml
```

The runtime server becomes the machine-local entrypoint for session creation, streaming, and execution.

## What to verify first

Before introducing a custom UI or workflow, confirm three things:

1. The server boots with your config and does not fail on provider initialization.
2. You can reach the HTTP surface and observe SSE output.
3. The machine has the provider and host permissions your agents will actually need.

## Deployment path

For a Linux VPS or long-lived host, the repo already includes deployment guidance and systemd examples:

```bash
make vps-deploy
```

Once deployed, treat the runtime host as the durable execution boundary. Your client can come and go. The service should not.

## Related repo docs

The repository also ships deeper operational docs under the existing `docs/` directory, including install, deployment, API, and architecture notes. This website is the guided front door; the repo docs are the detailed operating manual.
