#!/usr/bin/env bash
set -euo pipefail

# This script runs from the root of each newly created worktree.
bun install --frozen-lockfile
bun install --cwd apps/gooseweb --frozen-lockfile
bun install --cwd sidecars/claude-bridge --frozen-lockfile
