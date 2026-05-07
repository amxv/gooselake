#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Run preflight checks for gg-runtime-server deployment.

Usage:
  scripts/preflight-runtime.sh --config <path> [options]

Required:
  --config <path>          Runtime config file path

Options:
  --runtime-bin <path>     Runtime binary path. Default: gg-runtime-server (PATH lookup)
  --base-url <url>         If set, checks health + diagnostics HTTP endpoints
  --token <token>          Bearer token for authenticated endpoint checks
  --skip-http              Skip HTTP checks even if --base-url is provided
  -h, --help               Show help

Examples:
  ./scripts/preflight-runtime.sh --config ~/runtime-server.toml
  ./scripts/preflight-runtime.sh --config ~/runtime-server.toml --runtime-bin ~/.local/share/gg-runtime/current/bin/gg-runtime-server
  ./scripts/preflight-runtime.sh --config ~/runtime-server.toml --base-url http://127.0.0.1:8080 --token "$GG_RUNTIME_TOKEN"
USAGE
}

CONFIG_PATH=""
RUNTIME_BIN="gg-runtime-server"
BASE_URL=""
TOKEN=""
SKIP_HTTP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_PATH="${2:-}"
      shift 2
      ;;
    --runtime-bin)
      RUNTIME_BIN="${2:-}"
      shift 2
      ;;
    --base-url)
      BASE_URL="${2:-}"
      shift 2
      ;;
    --token)
      TOKEN="${2:-}"
      shift 2
      ;;
    --skip-http)
      SKIP_HTTP=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${CONFIG_PATH}" ]]; then
  echo "--config is required" >&2
  usage
  exit 1
fi

if [[ ! -f "${CONFIG_PATH}" ]]; then
  echo "Config file not found: ${CONFIG_PATH}" >&2
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi

resolve_runtime_bin() {
  local candidate="$1"
  if [[ "${candidate}" == */* ]]; then
    if [[ ! -x "${candidate}" ]]; then
      echo "Runtime binary is not executable: ${candidate}" >&2
      return 1
    fi
    echo "${candidate}"
    return
  fi
  local resolved
  resolved="$(command -v "${candidate}" || true)"
  if [[ -z "${resolved}" ]]; then
    echo "Unable to resolve runtime binary on PATH: ${candidate}" >&2
    return 1
  fi
  echo "${resolved}"
}

EXPECTED_BIN="$(resolve_runtime_bin "${RUNTIME_BIN}")"
EXPECTED_ROOT="$(cd "$(dirname "${EXPECTED_BIN}")/.." && pwd)"

echo "[1/4] Checking runtime config"
"${EXPECTED_BIN}" --check-config --config "${CONFIG_PATH}" >/dev/null

echo "[2/4] Verifying sidecar binaries"
CLAUDE_BRIDGE_BIN="${EXPECTED_ROOT}/sidecars/claude-bridge/claude-bridge"
MCP_BIN="${EXPECTED_ROOT}/sidecars/gg-mcp-server/gg-mcp-server"
for path in "${CLAUDE_BRIDGE_BIN}" "${MCP_BIN}"; do
  if [[ ! -x "${path}" ]]; then
    echo "Missing or non-executable sidecar binary: ${path}" >&2
    exit 1
  fi
done

if [[ "${SKIP_HTTP}" -eq 1 || -z "${BASE_URL}" ]]; then
  echo "[3/4] HTTP checks skipped"
  echo "[4/4] Preflight passed"
  exit 0
fi

BASE_URL="${BASE_URL%/}"
echo "[3/4] Checking public health endpoint"
curl -fsS "${BASE_URL}/health" >/dev/null

echo "[4/4] Checking authenticated diagnostics"
if [[ -z "${TOKEN}" ]]; then
  echo "--token is required when --base-url is set" >&2
  exit 1
fi
AUTH_HEADER="Authorization: Bearer ${TOKEN}"
curl -fsS -H "${AUTH_HEADER}" "${BASE_URL}/v1/health" >/dev/null
curl -fsS -H "${AUTH_HEADER}" "${BASE_URL}/v1/diagnostics/providers" >/dev/null
curl -fsS -H "${AUTH_HEADER}" "${BASE_URL}/v1/providers/codex/auth/status" >/dev/null
curl -fsS -H "${AUTH_HEADER}" "${BASE_URL}/v1/providers/claude/auth/status" >/dev/null

echo "Preflight passed"
