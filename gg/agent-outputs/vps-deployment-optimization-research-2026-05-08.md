# VPS Deployment Optimization Research (2026-05-08)

## Scope

Focused research pass for optimizing `gg-runtime-server` deployment to one primary target:
- Linux VPS, resilient always-on single-user service

With a secondary requirement:
- Local full-filesystem machine install/start remains easy for developer/personal use

This report is repo-backed (current code + docs/scripts), with concrete recommendations for next implementation/docs pass.

---

## Executive Recommendation

Primary deployment story should be **non-Docker first** (systemd user service on host), with optional containerization later as an advanced profile.

Why:
- Runtime is explicitly machine-integrated (provider CLIs, host auth material, filesystem/worktrees, spawned sidecars/processes).
- Current auth and sidecar model is optimized for host paths and host process execution, not container isolation.
- Existing packaging already ships a cohesive bundle layout that systemd can run directly with minimal moving parts.

Containerization is possible, but with this codebase today it introduces additional credential, process, and filesystem complexity that works against the “simple reliable VPS” goal.

---

## Repo-Backed Current State

### 1) Packaging model is bundle-oriented and already suitable for host install

Bundle shape is explicit and stable:
- `bin/gg-runtime-server`
- `sidecars/claude-bridge/claude-bridge`
- `sidecars/gg-mcp-server/gg-mcp-server`

References:
- `README.md:236-244`
- `docs/INSTALL.md:64-75`
- `scripts/package-release.sh:69-105`
- `scripts/install-runtime.sh:100-111`

The install script already performs release-asset download/extract/install to `~/.local` (or custom prefix).

### 2) Sidecar path resolution is designed around install-root-relative layout

Sidecars are discovered relative to current executable/install root, with dev/workspace fallbacks.

References:
- `crates/runtime-provider-claude/src/lib.rs:1863-1993`

This strongly favors the current tarball + filesystem layout model. It can still work in containers, but only if that exact layout is preserved.

### 3) Auth is machine-local by default and partly copied/staged into runtime data dirs

Codex:
- On bootstrap, runtime copies `~/.gg/codex/auth.json` into provider runtime home when present.
- Provider auth status checks for staged `auth.json` under runtime provider home.

References:
- `crates/runtime-server/src/bootstrap.rs:53-63`
- `crates/runtime-provider-codex/src/lib.rs:473-490`
- `crates/runtime-provider-codex/src/lib.rs:789-797`

Claude:
- Supports `host_machine` and `runtime_managed` modes.
- Bridge spawn validates OAuth/API-key readiness and depends on resolved HOME/config paths.
- Runtime can import/write auth JSON into runtime-owned paths.

References:
- `examples/runtime-server.toml:27-30`
- `crates/runtime-provider-claude/src/lib.rs:425-508`
- `crates/runtime-provider-claude/src/lib.rs:587-651`
- `crates/runtime-provider-claude/src/lib.rs:679-750`
- `crates/runtime-provider-claude/src/lib.rs:1093-1124`

### 4) Service/API auth is static bearer, with bootstrap token file fallback

If `auth.token` is not set, a token file is generated under data root.

References:
- `crates/runtime-server/src/config.rs:102-150`
- `examples/runtime-server.toml:5-10`
- `crates/runtime-server/src/http.rs:1930-1951`

Operational implication: persistent data volume/path is required for stable token continuity if inline token is not used.

### 5) Resilience behavior exists, but mostly as recovery/reconciliation (not restart of in-flight child work)

Startup recovery:
- Session/turn/approval reconciliation and provider resume attempts.
- Emits `runtime.startup_recovered` event.

References:
- `crates/runtime-core/src/runtime.rs:126-386`
- `crates/runtime-server/src/bootstrap.rs:139-143`
- `crates/runtime-server/src/bootstrap.rs:238-241`

Process manager recovery:
- Any process records in `running/queued` at startup are marked `failed`.
- Process logs are persisted under data/logs path.

References:
- `crates/runtime-tools/src/lib.rs:118-127`
- `crates/runtime-tools/src/lib.rs:575-577`
- `crates/runtime-server/src/bootstrap.rs:154-167`

Net: strong durability for state/events and reconciliation, but no resumption of in-flight OS processes across runtime crash/restart.

### 6) Existing deployment docs are minimal and systemd-first

Current VPS deployment doc:
- user-level systemd service with `Restart=on-failure`

Reference:
- `docs/DEPLOYMENT.md:19-35`

Good baseline, but missing important hardening/ops detail (detailed below).

---

## Docker vs Non-Docker Tradeoffs (This Runtime Specifically)

## Non-Docker install (recommended default)

Pros:
- Matches current architecture (machine-side runtime + host CLIs + filesystem/worktrees).
- Minimal auth friction with `codex login` / `claude login` machine flow.
- No container boundary issues for spawned child tools/processes.
- Existing docs/scripts already support this path.

Cons:
- Host dependency management is less hermetic.
- Harder to freeze exact runtime environment vs image pinning.

## Dockerized runtime (optional advanced profile)

Pros:
- Reproducible runtime image, easier immutable rollouts.
- Potentially cleaner backup/redeploy lifecycle around volumes.

Cons for current code/design:
- Auth and provider CLI materials must be mounted/injected correctly (HOME, config, token files).
- Worktree/process tooling expects meaningful host filesystem and toolchain access; container may not have equivalent environment.
- Sidecars and spawned subprocesses run inside container, so container image must include all required dependencies and exact bundle layout.
- Extra operational complexity (volumes, bind mounts, UID/GID ownership, CLI login flows inside container).

Conclusion:
- Docker increases complexity more than reliability for current single-user VPS target.
- Move to Docker only after explicit “container profile” implementation work (not as default path now).

---

## What Fits vs Resists Containerization

### Fits reasonably well

- Bundle layout itself is portable (tarball with runtime + sidecars).
- SQLite + logs + providers dirs can map to persistent volumes (`data.root_dir`).
- HTTP service is self-contained with bearer auth.

References:
- `scripts/package-release.sh:65-105`
- `crates/runtime-server/src/config.rs:66-100`

### Resists or adds friction

- Host-oriented auth assumptions (Codex source auth path, Claude HOME/config resolution).
- Runtime’s value proposition includes host filesystem and process orchestration; containers constrain this unless many mounts/capabilities are added.
- Provider CLIs/logins are documented as machine actions, not container-native setup.

References:
- `crates/runtime-server/src/bootstrap.rs:53-63`
- `crates/runtime-provider-claude/src/lib.rs:1093-1124`
- `docs/INSTALL.md:37-43`
- `docs/DEPLOYMENT.md:12-17`

---

## Packaging `gg-runtime-server` + Sidecars for Reliable VPS Deployment

Current packaging is close to what we need. Recommended packaging/installation strategy:

1. Keep one runtime bundle per version (already done).
2. Install under versioned directories (for example `/opt/gg-runtime/releases/<version>/...`) instead of replacing binaries in place.
3. Maintain stable symlink `/opt/gg-runtime/current` used by systemd `ExecStart`.
4. Store mutable runtime data outside release dir (for example `/var/lib/gg-runtime` or `$HOME/.gg-runtime`).
5. Keep config outside release dir (for example `/etc/gg-runtime/runtime-server.toml` or `$HOME/runtime-server.toml`).
6. Restart service after symlink switch; rollback by moving symlink back.

Why this matters:
- Atomic upgrade/rollback without risking partial overwrite.
- Cleaner operational audits and reproducibility.

Gap: repo scripts currently install directly into single prefix and overwrite current binaries (`scripts/install-runtime.sh:100-111`).

---

## Resilient Always-On Service Model (Recommended)

Use **systemd** as the primary supervisor.

### Baseline unit improvements beyond current docs

Current doc includes only `Restart=on-failure` and `RestartSec=2`.

Reference:
- `docs/DEPLOYMENT.md:26-31`

Recommended additions:
- `StartLimitIntervalSec` / `StartLimitBurst` to avoid restart storms.
- `TimeoutStopSec` and `KillMode=mixed` for controlled shutdown with child processes.
- `WorkingDirectory` set to stable config/data context.
- `EnvironmentFile=` for token/base-url/bind overrides.
- `LimitNOFILE` bump for SSE-heavy or many concurrent process sessions.
- `UMask=0077` to harden token/auth file permissions.

### Logs

Two log planes should be documented distinctly:
- service logs: systemd journal (`journalctl --user -u ...`)
- runtime-managed process logs: under `${data.root_dir}/${logs_dir}/processes`

References:
- `docs/DEPLOYMENT.md:41-42`
- `crates/runtime-server/src/bootstrap.rs:154-157`
- `crates/runtime-tools/src/lib.rs:575-577`

### Persistent storage

Must persist at least:
- SQLite DB (`data.sqlite_path`)
- auth token file if using generated token mode
- providers dir (staged/imported auth materials)
- logs dir (for postmortem and process output)
- worktrees root if worktree lifecycle matters across restarts

References:
- `crates/runtime-server/src/config.rs:86-100`
- `crates/runtime-server/src/config.rs:102-150`
- `examples/runtime-server.toml:11-15`
- `examples/runtime-server.toml:44-48`

---

## Operational Failure Modes That Matter Most

1. Runtime process crash
- Behavior: systemd can restart service; runtime startup recovery reconciles sessions/turns/approvals.
- Risk: in-flight provider/process work may not fully continue; some state marked failed and requires user retry.
- References: `crates/runtime-core/src/runtime.rs:126-386`, `crates/runtime-tools/src/lib.rs:118-127`

2. Sidecar crash (Claude bridge / MCP)
- Behavior: Claude provider manages bridge handles and re-spawns as needed for new sessions; failures surface through provider errors.
- Risk: active sessions may fail and need retry/resume path; not externally supervised as separate OS services.
- References: `crates/runtime-provider-claude/src/lib.rs:753-930`

3. Network interruption
- Behavior: SSE replay/stream model supports reconnection from stored events.
- Risk: clients need robust reconnect logic and cursor handling.
- References: `crates/runtime-server/src/http.rs` routes for `/events` + `/events/stream` + scoped streams

4. Provider auth issues
- Behavior: explicit auth-status endpoints and detailed auth diagnostics available.
- Risk: auth files drift, token expiry, HOME/config mismatch in claude host/runtime modes.
- References: `crates/runtime-provider-codex/src/lib.rs:473-490`, `crates/runtime-provider-claude/src/lib.rs:679-750`

5. Filesystem path/permission issues
- Behavior: runtime creates data dirs on bootstrap; auth writes enforce unix permissions in Claude paths.
- Risk: incorrect ownership or readonly paths break startup or auth import.
- References: `crates/runtime-server/src/config.rs:53-63`, `crates/runtime-provider-claude/src/lib.rs:605`, `crates/runtime-provider-claude/src/lib.rs:621-636`

6. Upgrade errors / binary drift
- Behavior: current script performs in-place overwrite.
- Risk: partial/bad upgrades harder to roll back; path breakage if sidecar layout deviates.
- References: `scripts/install-runtime.sh:100-111`, `crates/runtime-provider-claude/src/lib.rs:1863-1993`

7. Data persistence corruption/loss
- Behavior: SQLite is the system of record for sessions/events/process metadata.
- Risk: no documented backup/restore policy yet.
- References: `crates/runtime-server/src/bootstrap.rs:36-43`, `crates/runtime-store-sqlite/src/lib.rs`

---

## Recommended Simple Deployment Stories

## A) Linux VPS production-ish single-user (primary)

Recommended default: **host install + systemd user service**.

Minimal opinionated flow:
1. Install release bundle to host.
2. Use explicit config file in stable location.
3. Set explicit `auth.token` (do not rely on generated token for production-ish setup).
4. Set `data.root_dir` to persistent absolute path.
5. Perform provider logins/imports once on host.
6. Run under systemd user unit with restart policy and added hardening options.
7. Place reverse proxy/TLS in front if exposed externally.
8. Upgrade via staged release + symlink switch + restart (add script support in next pass).

Why this is the right default now:
- Lowest operational complexity for this runtime’s machine-oriented behavior.
- Maximum alignment with existing code and docs.

## B) Local full-filesystem machine development/personal use (secondary)

Recommended default: **current existing flow stays unchanged**.

1. `./scripts/install-runtime.sh latest`
2. `cp ~/.local/runtime-server.toml.example ./runtime-server.toml`
3. `codex login` + `claude login`
4. `gg-runtime-server --config ./runtime-server.toml`

For always-on local use:
- systemd user service (Linux) or LaunchAgent/tmux (macOS), as currently documented.

References:
- `docs/INSTALL.md:10-43`
- `docs/DEPLOYMENT.md:45-51`

---

## Specific Gaps / Awkwardness in Current Docs and Scripts

1. No Docker guidance at all
- No `Dockerfile`, no compose example, no explicit “why we recommend host install first”.
- For users expecting containers on VPS, this is ambiguous.

2. Deployment guide is too minimal for reliable operations
- Missing recommended absolute `data.root_dir`, token strategy, backup guidance, restart storm controls, and upgrade rollback approach.
- Reference: `docs/DEPLOYMENT.md`

3. Upgrade flow is non-atomic
- `install-runtime.sh` overwrites binaries in-place; no staged release directory or rollback helper.
- Reference: `scripts/install-runtime.sh:100-111`

4. Auth model is powerful but not operationalized in docs
- Claude host/runtime-managed modes and auth import endpoints exist, but docs don’t provide practical decision matrix + examples.
- References: `examples/runtime-server.toml:27-30`, `crates/runtime-server/src/http.rs` Claude auth routes

5. Persistence/backup expectations are not documented
- SQLite and provider/auth artifacts are central, but there is no backup/restore runbook.
- References: `crates/runtime-server/src/config.rs`, `crates/runtime-store-sqlite/src/lib.rs`

6. Sidecar lifecycle is implicit
- Docs mention sidecars but not what happens on sidecar failure and expected operator response.
- References: `README.md:261-277`, `crates/runtime-provider-claude/src/lib.rs`

---

## Concrete Recommendations for Next Implementation + Docs Pass

## Priority 1 (docs + operator experience)

1. Expand `docs/DEPLOYMENT.md` into a full VPS runbook
- Include hardened systemd unit template.
- Include explicit persistent path recommendations.
- Include token strategy (`auth.token` vs token-file).
- Include health checks (`/health`, `/v1/health`, `/v1/diagnostics/*`).
- Include log troubleshooting for both journal + process logs.
- Include crash/sidecar/auth failure playbooks.

2. Add “deployment mode decision” section
- Host install (recommended) vs container (advanced).
- Explicitly explain why host-first is current default for this runtime.

3. Add backup/restore section
- Define what to back up: SQLite + providers dir + auth token file + optional logs/worktrees.

## Priority 2 (release/install hardening)

4. Add staged upgrade helper script
- Example: `scripts/upgrade-runtime.sh` with release dir + symlink + restart + rollback command.

5. Add config validator/check command
- Example: `gg-runtime-server --check-config --config <path>` to fail early on bad paths/auth assumptions.

6. Add optional preflight script
- Verify sidecar binaries present/executable.
- Verify provider auth status endpoints return expected results.
- Verify writable data root.

## Priority 3 (optional container profile, not default)

7. Introduce explicit container profile docs + artifacts
- Dockerfile and compose only after deciding required mounts/env/auth patterns.
- Keep it “advanced” until auth/tooling/path assumptions are codified for container runs.

8. Add container-specific auth and path guidance
- HOME/CLAUDE_CONFIG_DIR/CODEX_HOME mapping patterns.
- Persisted volume layout mirroring current data root semantics.

---

## Suggested “Primary Deployment Contract” (Short Form)

For 2026-05 codebase state, the simplest reliable contract is:
- host-installed release bundle
- systemd user supervision
- persistent absolute data root
- explicit bearer token in config
- provider auth done/imported on that machine
- staged upgrades with rollback plan

That aligns with the runtime’s current architecture and minimizes operational surprises.
