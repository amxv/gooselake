#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUNTIME_HOST="${RUNTIME_HOST:-127.0.0.1}"
RUNTIME_PORT="${RUNTIME_PORT:-18080}"
GOOSETOWER_HOST="${GOOSETOWER_HOST:-127.0.0.1}"
GOOSETOWER_PORT="${GOOSETOWER_PORT:-18090}"
GOOSEWEB_HOST="${GOOSEWEB_HOST:-127.0.0.1}"
GOOSEWEB_PORT="${GOOSEWEB_PORT:-13001}"

DEV_DIR="${DEV_DIR:-${ROOT_DIR}/tmp/gooseweb-dev}"
RUNTIME_CONFIG="${RUNTIME_CONFIG:-${DEV_DIR}/runtime-server.toml}"
GOOSETOWER_CONFIG="${GOOSETOWER_CONFIG:-${DEV_DIR}/goosetower.toml}"
RUNTIME_DATA_DIR="${RUNTIME_DATA_DIR:-${DEV_DIR}/runtime-data}"
RUNTIME_TOKEN_FILE="${RUNTIME_TOKEN_FILE:-${RUNTIME_DATA_DIR}/auth/api-token}"
RUNTIME_TOKEN="${RUNTIME_TOKEN:-dev-runtime-token}"
GOOSETOWER_TOKEN="${GOOSETOWER_TOKEN:-dev-goosetower-token}"
GOOSETOWER_TICKET_KEY="${GOOSETOWER_TICKET_KEY:-dev-ticket-signing-key}"

RUNTIME_URL="http://${RUNTIME_HOST}:${RUNTIME_PORT}"
GOOSETOWER_HTTP_URL="http://${GOOSETOWER_HOST}:${GOOSETOWER_PORT}"
GOOSETOWER_WS_URL="ws://${GOOSETOWER_HOST}:${GOOSETOWER_PORT}/v1/realtime"
GOOSEWEB_URL="http://${GOOSEWEB_HOST}:${GOOSEWEB_PORT}"

pids=()

cleanup() {
  local status=$?
  trap - INT TERM EXIT
  if ((${#pids[@]})); then
    echo
    echo "Stopping Gooseweb dev stack..."
    for pid in "${pids[@]}"; do
      if kill -0 "${pid}" 2>/dev/null; then
        kill "${pid}" 2>/dev/null || true
      fi
    done
    wait "${pids[@]}" 2>/dev/null || true
  fi
  exit "${status}"
}

trap cleanup INT TERM EXIT

write_configs() {
  mkdir -p "${DEV_DIR}" "$(dirname "${RUNTIME_TOKEN_FILE}")"
  printf '%s\n' "${RUNTIME_TOKEN}" > "${RUNTIME_TOKEN_FILE}"

  cat > "${RUNTIME_CONFIG}" <<EOF
[server]
bind_address = "${RUNTIME_HOST}:${RUNTIME_PORT}"
public_base_url = "${RUNTIME_URL}"

[auth]
mode = "static_bearer"
token_file = "${RUNTIME_TOKEN_FILE}"

[data]
root_dir = "${RUNTIME_DATA_DIR}"
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
max_instances = 4
max_sessions_per_instance = 4
transport = "stdio"
request_timeout_secs = 30
wait_timeout_secs = 300

[providers.acp.env]

[events]
live_queue_capacity = 4096
critical_queue_capacity = 16384
team_queue_capacity = 8192

[processes]
enabled = true
max_concurrent = 32
default_timeout_ms = 600000
max_output_bytes_per_process = 20000000
allow_shell = true

[teams]
enabled = true
non_lead_can_add_members = false
non_lead_can_remove_members = false

[[teams.model_presets]]
name = "fast"
provider = "codex"
model = "gpt-5.4-mini"
thinking_effort = "low"

[[teams.model_presets]]
name = "deep"
provider = "claude"
model = "claude-opus-4-8"
thinking_effort = "high"

[worktrees]
enabled = true
root_dir = "worktrees"
init_script_path = ".agents/gg/worktree-init.sh"
deletion_policy_default = "delete_on_last_claim"
EOF

  cat > "${GOOSETOWER_CONFIG}" <<EOF
[server]
bind_address = "${GOOSETOWER_HOST}:${GOOSETOWER_PORT}"
public_base_url = "${GOOSETOWER_HTTP_URL}"
allowed_gooseweb_origins = [
  "${GOOSEWEB_URL}",
  "http://localhost:${GOOSEWEB_PORT}",
]

[auth]
api_token = "${GOOSETOWER_TOKEN}"

[tickets]
issuer = "gooseweb-local"
audience = "goosetower-local"
signing_key = "${GOOSETOWER_TICKET_KEY}"
ttl_secs = 60

[[runtimes.sources]]
source_id = "local"
source_epoch = "local-dev-0"
source_kind = "gooselake-runtime"
base_url = "${RUNTIME_URL}"
bearer_token_file = "${RUNTIME_TOKEN_FILE}"
enabled = true
display_name = "Local Gooselake Runtime"
workspace_id = "default"

[websocket]
max_message_bytes = 1048576
heartbeat_interval_ms = 15000

[replay]
max_events_per_request = 1000
source_stale_after_ms = 30000

[materializer]
event_buffer_size = 8192
snapshot_cache_size = 1024

[lanes]
critical_capacity = 4096
state_capacity = 8192
tokens_capacity = 16384
bulk_capacity = 2048

[debug]
endpoints_enabled = true
EOF
}

require_free_port() {
  local port="$1"
  local label="$2"
  if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Port ${port} is already in use; stop the existing ${label} process or override its port." >&2
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN >&2 || true
    exit 1
  fi
}

wait_for_http() {
  local url="$1"
  local label="$2"
  local pid="${3:-}"
  local attempts="${4:-1200}"
  local delay="${5:-0.5}"

  for _ in $(seq 1 "${attempts}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "${label} is ready at ${url}"
      return 0
    fi
    if [[ -n "${pid}" ]] && ! kill -0 "${pid}" 2>/dev/null; then
      echo "${label} process exited before ${url} became ready" >&2
      exit 1
    fi
    sleep "${delay}"
  done

  echo "Timed out waiting for ${label} at ${url}" >&2
  exit 1
}

wait_for_stack() {
  while :; do
    for pid in "${pids[@]}"; do
      if ! kill -0 "${pid}" 2>/dev/null; then
        echo "A Gooseweb dev stack process exited; stopping the remaining processes." >&2
        return 1
      fi
    done
    sleep 1
  done
}

write_configs

require_free_port "${RUNTIME_PORT}" "runtime"
require_free_port "${GOOSETOWER_PORT}" "Goosetower"
require_free_port "${GOOSEWEB_PORT}" "Gooseweb"

echo "Starting Gooseweb live dev stack"
echo "  runtime:    ${RUNTIME_URL}"
echo "  goosetower: ${GOOSETOWER_HTTP_URL}"
echo "  gooseweb:   ${GOOSEWEB_URL}"
echo "  configs:    ${DEV_DIR}"
echo

(
  cd "${ROOT_DIR}"
  exec cargo run -p runtime-server --bin gg-runtime-server -- --config "${RUNTIME_CONFIG}"
) &
pids+=("$!")
runtime_pid="$!"

wait_for_http "${RUNTIME_URL}/health" "Runtime" "${runtime_pid}"

(
  cd "${ROOT_DIR}"
  exec cargo run -p goosetower --bin gg-goosetower -- --config "${GOOSETOWER_CONFIG}"
) &
goosetower_pid="$!"
pids+=("${goosetower_pid}")

wait_for_http "${GOOSETOWER_HTTP_URL}/health" "Goosetower" "${goosetower_pid}"

(
  cd "${ROOT_DIR}"
  exec env \
    VITE_GOOSETOWER_URL="${GOOSETOWER_WS_URL}" \
    VITE_GOOSETOWER_HTTP_URL="${GOOSETOWER_HTTP_URL}" \
    VITE_GOOSEWEB_DEV_TICKET_ROUTE_ENABLED=true \
    bun run --cwd apps/gooseweb dev --host "${GOOSEWEB_HOST}" --port "${GOOSEWEB_PORT}"
) &
gooseweb_pid="$!"
pids+=("${gooseweb_pid}")

wait_for_http "${GOOSEWEB_URL}" "Gooseweb" "${gooseweb_pid}"

cat <<EOF

Gooseweb live dev stack is running.

  Open:       ${GOOSEWEB_URL}
  Runtime:    ${RUNTIME_URL}
  Goosetower: ${GOOSETOWER_HTTP_URL}

Press Ctrl-C to stop all three processes.
EOF

wait_for_stack
