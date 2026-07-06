#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Run preflight checks for gg-goosetower deployment.

Usage:
  scripts/preflight-goosetower.sh --config <path> [options]

Required:
  --config <path>           Goosetower config file path

Options:
  --goosetower-bin <path>   Goosetower binary path. Default: gg-goosetower
  --base-url <url>          If set, checks health + protected endpoints
  --token <token>           Goosetower API bearer token for protected checks
  --skip-http               Skip HTTP checks even if --base-url is provided
  -h, --help                Show help

Examples:
  ./scripts/preflight-goosetower.sh --config examples/goosetower.local.toml
  ./scripts/preflight-goosetower.sh --config ~/.config/gg-goosetower/goosetower.toml --goosetower-bin ~/.local/share/gg-runtime/current/bin/gg-goosetower
  ./scripts/preflight-goosetower.sh --config ~/.config/gg-goosetower/goosetower.toml --base-url https://goosetower.example.com --token "$GOOSETOWER_TOKEN"
USAGE
}

CONFIG_PATH=""
GOOSETOWER_BIN="gg-goosetower"
BASE_URL=""
TOKEN=""
SKIP_HTTP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_PATH="${2:-}"
      shift 2
      ;;
    --goosetower-bin)
      GOOSETOWER_BIN="${2:-}"
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

resolve_bin() {
  local candidate="$1"
  if [[ "${candidate}" == */* ]]; then
    if [[ ! -x "${candidate}" ]]; then
      echo "Goosetower binary is not executable: ${candidate}" >&2
      return 1
    fi
    echo "${candidate}"
    return
  fi
  local resolved
  resolved="$(command -v "${candidate}" || true)"
  if [[ -z "${resolved}" ]]; then
    echo "Unable to resolve Goosetower binary on PATH: ${candidate}" >&2
    return 1
  fi
  echo "${resolved}"
}

EXPECTED_BIN="$(resolve_bin "${GOOSETOWER_BIN}")"

echo "[1/3] Checking goosetower config"
"${EXPECTED_BIN}" --check-config --config "${CONFIG_PATH}" >/dev/null

if [[ "${SKIP_HTTP}" -eq 1 || -z "${BASE_URL}" ]]; then
  echo "[2/3] HTTP checks skipped"
  echo "[3/3] Preflight passed"
  exit 0
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required for HTTP checks" >&2
  exit 1
fi

BASE_URL="${BASE_URL%/}"
echo "[2/3] Checking public health endpoint"
curl -fsS "${BASE_URL}/health" >/dev/null

echo "[3/3] Checking authenticated gateway endpoints"
if [[ -z "${TOKEN}" ]]; then
  echo "--token is required when --base-url is set" >&2
  exit 1
fi
AUTH_HEADER="Authorization: Bearer ${TOKEN}"
curl -fsS -H "${AUTH_HEADER}" "${BASE_URL}/v1/health" >/dev/null
curl -fsS -H "${AUTH_HEADER}" "${BASE_URL}/v1/sources" >/dev/null
curl -fsS -H "${AUTH_HEADER}" "${BASE_URL}/v1/metrics" >/dev/null

echo "Preflight passed"
