# Configuration Reference

The runtime config is a TOML file loaded with `gg-runtime-server --config <path>`. The complete example lives at [`examples/runtime-server.toml`](../examples/runtime-server.toml).

Relative paths are resolved deliberately:

- `data.root_dir` is resolved relative to the config file directory when a config file path is provided.
- other relative data paths are resolved under `data.root_dir`.
- `worktrees.root_dir` is resolved under `data.root_dir` unless it is absolute.
- if no config path is supplied, the default data root is `$HOME/.gg-runtime`.

## Minimal local config

```toml
[server]
bind_address = "127.0.0.1:8080"
public_base_url = "http://127.0.0.1:8080"

[auth]
mode = "static_bearer"
token = "replace-with-local-development-token"

[data]
root_dir = ".gg-runtime"
sqlite_path = "runtime.sqlite3"
logs_dir = "logs"
providers_dir = "providers"

[providers]
claude_auth_mode = "host_machine"

[providers.codex]
enabled = true
max_instances = 4
max_sessions_per_instance = 8

[providers.claude]
enabled = true
max_instances = 4
max_sessions_per_instance = 4

[providers.acp]
enabled = false
transport = "stdio"
request_timeout_secs = 30
wait_timeout_secs = 300

[processes]
enabled = true
max_concurrent = 32
default_timeout_ms = 600000
max_output_bytes_per_process = 20000000
allow_shell = true

[worktrees]
enabled = true
root_dir = "worktrees"
init_script_path = ".agents/gg/worktree-init.sh"
deletion_policy_default = "delete_on_last_claim"
```

## `[server]`

| Key | Default | Meaning |
| --- | --- | --- |
| `bind_address` | `0.0.0.0:8080` | Socket the Axum server binds to. Use `127.0.0.1:8080` when running behind a local reverse proxy. |
| `public_base_url` | `http://localhost:8080` | URL injected into provider-side GG MCP config. It must be reachable from sidecars and provider processes running on the same host. |

For a VPS, bind to localhost unless a proxy, firewall, and TLS plan are already in place.

## `[auth]`

| Key | Default | Meaning |
| --- | --- | --- |
| `mode` | `static_bearer` | The only supported mode today. |
| `token` | unset | Inline bearer token. If present, it takes precedence over `token_file`. |
| `token_file` | `auth/api-token` under `data.root_dir` | File used for generated or existing token material when `token` is omitted. |

When `auth.token` is omitted, the server will create a random token file if one does not already exist. This is useful for local installs, but stable automation should set an explicit strong token or persist the generated token file.

Protected requests use:

```bash
Authorization: Bearer <token>
```

## `[data]`

| Key | Default | Meaning |
| --- | --- | --- |
| `root_dir` | `$HOME/.gg-runtime` | Mutable runtime state root. |
| `sqlite_path` | `runtime.sqlite3` | SQLite file path, relative to `root_dir` unless absolute. |
| `logs_dir` | `logs` | Log directory, relative to `root_dir` unless absolute. Process logs are written below `logs/processes`. |
| `providers_dir` | `providers` | Provider-specific runtime directories. |

Typical layout after startup:

```text
<data.root_dir>/
  runtime.sqlite3
  auth/api-token
  logs/processes/
  providers/
    codex/home/
    claude/config/
    acp/
  worktrees/
```

Keep `data.root_dir` outside release directories. Release directories should be replaceable; data directories are durable state.

## `[providers]`

| Key | Default | Meaning |
| --- | --- | --- |
| `claude_auth_mode` | `host_machine` | Claude auth resolution mode. Use `runtime_managed` if credentials should be imported into runtime-owned files instead of relying on the host login. |

Place `claude_auth_mode` under `[providers]`, not under `[providers.acp.env]`.

## `[providers.codex]`

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Register the Codex provider. |
| `max_instances` | `4` | Maximum Codex transport/process instances. |
| `max_sessions_per_instance` | `8` | Capacity per transport. |

At bootstrap, if `~/.gg/codex/auth.json` exists, the runtime stages it into `data.providers_dir/codex/home` and the Codex provider reports auth status against that staged home.

## `[providers.claude]`

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Register the Claude provider. |
| `max_instances` | `4` | Maximum Claude bridge processes. |
| `max_sessions_per_instance` | `4` | Sessions per bridge. |

Claude runs through the `sidecars/claude-bridge` process. The runtime also injects the GG MCP server into Claude sessions so Claude can call back into runtime-owned tools.

Claude auth modes:

- `host_machine`: use the machine's existing Claude login/config when available; also considers runtime-managed files and API key fallback.
- `runtime_managed`: prefer runtime-owned imported files under the runtime provider directory; can still use explicit bridge overrides.

Claude auth mutations currently exist for Claude only:

- `POST /v1/providers/claude/auth/api-key`
- `POST /v1/providers/claude/auth/import-json`
- `POST /v1/providers/claude/auth/import-file`
- `POST /v1/providers/claude/auth/logout`

## `[providers.acp]`

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Register the ACP provider. |
| `max_instances` | `4` | Maximum ACP agent processes. |
| `max_sessions_per_instance` | `4` | Sessions per ACP agent process. |
| `command` | unset | Absolute path to an ACP-compatible agent command. Required when enabled. |
| `args` | `[]` | Arguments for the ACP command, for example `['serve', '--stdio']`. |
| `transport` | `stdio` | ACP v1 supports `stdio` only in this runtime. |
| `request_timeout_secs` | `30` | Timeout for request/response operations. |
| `wait_timeout_secs` | `300` | Timeout for waiting on a turn. |
| `[providers.acp.env]` | empty | Environment variables passed to the ACP agent. |

ACP v1 constraints:

- no streamable HTTP transport
- no runtime-owned ACP login/import/logout flow
- auth is negotiated by the configured ACP agent
- `GET /v1/providers/acp/models` can validly return an empty list
- ACP permission requests fail the active turn because runtime approval bridging is not implemented for ACP yet

Example:

```toml
[providers.acp]
enabled = true
command = "/usr/local/bin/my-acp-agent"
args = ["serve", "--stdio"]
transport = "stdio"
request_timeout_secs = 30
wait_timeout_secs = 300

[providers.acp.env]
ACP_AGENT_API_TOKEN = "replace-if-required"
```

## `[events]`

| Key | Default | Meaning |
| --- | --- | --- |
| `live_queue_capacity` | `4096` | Session/global live event broadcast capacity. |
| `critical_queue_capacity` | `16384` | Queue capacity for critical runtime events. |
| `team_queue_capacity` | `8192` | Team event capacity. |

Do not set these to zero. The runtime validates positive capacities at startup.

## `[processes]`

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Enable runtime process manager and MCP process tools. |
| `max_concurrent` | `32` | Maximum concurrent managed processes. |
| `default_timeout_ms` | `600000` | Default process timeout when request omits one. |
| `max_output_bytes_per_process` | `20000000` | Stored stdout/stderr limit per process. |
| `allow_shell` | `true` | Run command strings through the shell when true. |

Process records, lifecycle events, and log paths are persisted. HTTP process access can be scoped with `session_id`; MCP process calls are scoped to the caller session.

## `[worktrees]`

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Enable managed git worktree APIs and team spawn worktree support. |
| `root_dir` | `worktrees` | Managed worktree root, relative to `data.root_dir` unless absolute. |
| `init_script_path` | `.agents/gg/worktree-init.sh` | Optional repo-local script path to run after worktree creation when requested. |
| `deletion_policy_default` | `delete_on_last_claim` | Default cleanup behavior. |

Worktree records track repo root, worktree path, branch name, unified workspace path, deletion policy, and active claims.

## Environment overrides

| Variable | Used by | Meaning |
| --- | --- | --- |
| `GG_RUNTIME_REPO` | install/upgrade scripts | GitHub repo to download release assets from. Default is `amxv/gooselake`. |
| `GG_RUNTIME_INSTALL_ROOT` | install scripts | Prefix for direct install, default `~/.local`. |
| `GG_RUNTIME_RELEASES_ROOT` | upgrade/deploy scripts | Staged release root, default `~/.local/share/gg-runtime`. |
| `GG_RUNTIME_SYSTEMD_SERVICE` | upgrade script | Optional service to restart after activation. |
| `GG_RUNTIME_SYSTEMD_SCOPE` | upgrade script | `user` or `system`; default `user`. |
| `GG_CLAUDE_AUTH_MODE` | runtime bootstrap | Overrides `[providers].claude_auth_mode`. |
| `GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR` | runtime bootstrap | Passes `CLAUDE_CONFIG_DIR` to the Claude bridge. |
| `GG_CLAUDE_BRIDGE_HOME` | runtime bootstrap | Passes `HOME` to the Claude bridge. |
| `GG_MCP_SERVER_PATH` | runtime bootstrap | Overrides the discovered `gg-mcp-server` sidecar path. |
| `GG_MCP_GATEWAY_URL` | MCP sidecar | Runtime gateway URL, usually injected by provider config. |
| `GG_MCP_GATEWAY_TOKEN` | MCP sidecar | Bearer token for runtime gateway calls, usually injected by provider config. |
| `GG_MCP_CALLER_AGENT_ID` | MCP sidecar | Default caller session ID when tool metadata omits it. |
| `GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID` | MCP sidecar | Require explicit caller metadata when truthy. |
| `GG_MCP_ENABLE_PROCESS_TOOLS` | MCP sidecar | Disable `gg_process_*` tools when set to `0`, `false`, or `off`. |

## Validation

Run config validation before starting a service:

```bash
gg-runtime-server --check-config --config ./runtime-server.toml
```

Run repo preflight around an installed staged binary:

```bash
make preflight CONFIG=./runtime-server.toml RUNTIME_BIN="$HOME/.local/share/gg-runtime/current/bin/gg-runtime-server"
```
