---
title: Repo guide
description: Use the repository layout and existing docs to move from orientation into implementation and operations.
order: 5
category: Operator Workflows
summary: Where to look in the repo when you need code, API behavior, deployment assets, or deeper operational detail.
---

## The code layout

The repository is organized around the runtime and its sidecars:

- `crates/` contains the core runtime, providers, storage layers, and server.
- `sidecars/` contains companion integrations such as the MCP server and Claude bridge.
- `docs/` contains deeper engineering and operator documentation.
- `openapi/` contains the generated runtime OpenAPI artifact.
- `deploy/` contains systemd and deployment scaffolding.

## High-value docs already in the repo

Once you are past first-contact onboarding, these repo docs matter most:

- `docs/INSTALL.md` for install and local run details
- `docs/DEPLOYMENT.md` for VPS and service deployment
- `docs/API.md` and `docs/API_ENDPOINTS.md` for runtime surface area
- `docs/ARCHITECTURE.md` for internal structure and reasoning

## Suggested reading order

1. Start with the site pages in this docs area.
2. Move to the repo docs for the specific operating concern you have.
3. Read the relevant crate or sidecar source once you are changing behavior.

That sequence keeps orientation lightweight without leaving engineers shallow when they need exact behavior.
