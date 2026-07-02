---
title: Changelog
description: "Release notes for Gooselake."
order: 99
category: Reference
summary: Version-by-version changes for the Gooselake runtime.
---

This changelog tracks code and product changes in Gooselake. It intentionally skips docs-site-only updates.

## 0.1.2 — 2026-06-23

- Fixed packaged deploy template path resolution.
- Fixed spawned Codex teammate permission-mode propagation.
- Ignored generated example runtime state.
- Switched the project license to Apache 2.0.
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
