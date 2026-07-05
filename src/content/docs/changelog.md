---
title: Changelog
description: "Release notes for Gooselake."
order: 99
category: "Reference"
summary: Version-by-version changes for the Gooselake runtime.
---

This changelog tracks code and product changes in Gooselake. It intentionally skips docs-site-only updates.

## 0.1.4 — 2026-07-06

- Added runtime-backed `gg_team_status`, `gg_team_message`, and `gg_team_manage` MCP tools.
- Added configurable team MCP policy for lead and non-lead manage permissions.
- Added config-driven model presets for spawned team agents.
- Added team image attachment handling for messages and spawned agents, with explicit unsupported-provider errors where needed.
- Surfaced context-window remaining percentage in team status where provider/runtime state supports it.
- Aligned Codex, Claude, ACP, and the bundled `gg-mcp-server` sidecar around the shared runtime MCP gateway.
- Expanded MCP acceptance coverage and documented HTTP/MCP team control-plane behavior.

## 0.1.3 — 2026-07-03

- Switched the project license to Apache 2.0.
- Updated the Claude Sonnet preset to Sonnet 5.
- Added advanced Claude reasoning-effort support.
- Added Claude Fable 5 to the provider model catalog.

## 0.1.2 — 2026-06-23

- Fixed packaged deploy template path resolution.
- Fixed spawned Codex teammate permission-mode propagation.
- Ignored generated example runtime state.
- Added the ACP provider skeleton.
- Wired ACP configuration and bootstrap.
- Implemented ACP provider lifecycle handling.
- Added ACP provider status API.
- Validated ACP integration flows.
- Expanded ACP end-to-end coverage.
- Fixed ACP review findings.
- Hardened ACP session and child lifecycle.
- Cleaned up ACP startup failures.
- Made ACP close best-effort.
- Updated provider model catalogs for the next release.

## 0.1.1 — 2026-05-08

- Improved VPS deployment workflow support.
- Added API doc sync workflow support.
- Moved the runtime API doc sync skill into `.agents/skills`.

## 0.1.0 — 2026-05-07

- Bootstrapped the standalone runtime workspace.
- Added the SQLite runtime store and event model.
- Added the Codex-backed runtime session slice.
- Added the runtime MCP gateway and process manager.
- Ported team lifecycle and communications runtime.
- Ported managed worktrees and teammate spawning.
- Ported the Claude provider and standalone sidecars.
- Added recovery diagnostics and an acceptance demo.
- Added release workflow support plus session handling fixes.
