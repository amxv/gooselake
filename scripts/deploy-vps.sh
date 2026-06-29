#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Automate host-based Linux VPS deployment for gg-runtime-server.

This script performs:
1) staged upgrade activation
2) config bootstrap (if missing)
3) systemd unit/env template install (or refresh)
4) preflight checks
5) systemd daemon-reload + enable/start
6) optional post-start HTTP verification

Usage:
  scripts/deploy-vps.sh [options]

Options:
  --version <tag>          Release version/tag. Default: latest
  --config <path>          Config path. Default: ~/.config/gg-runtime/runtime-server.toml
  --service <name>         Systemd service name. Default: gg-runtime.service
  --scope <user|system>    Systemd scope. Default: user
  --base-url <url>         Optional runtime base URL for post-start HTTP checks
  --token <token>          Bearer token for authenticated HTTP checks
  --refresh-unit-files     Overwrite existing service/env files from release templates
  --skip-upgrade           Skip release upgrade/symlink activation step
  -h, --help               Show help

Environment:
  GG_RUNTIME_REPO            Override GitHub repo for release artifacts
  GG_RUNTIME_RELEASES_ROOT   Override staged release root

Examples:
  ./scripts/deploy-vps.sh
  ./scripts/deploy-vps.sh --version v0.1.2 --base-url http://127.0.0.1:8080 --token "$GG_RUNTIME_TOKEN"
  GG_RUNTIME_RELEASES_ROOT=/opt/gg-runtime ./scripts/deploy-vps.sh --scope system --service gg-runtime.service
USAGE
}

VERSION="latest"
CONFIG_PATH="${HOME}/.config/gg-runtime/runtime-server.toml"
SERVICE_NAME="gg-runtime.service"
SYSTEMD_SCOPE="user"
BASE_URL=""
TOKEN=""
REFRESH_UNIT_FILES=0
SKIP_UPGRADE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --config)
      CONFIG_PATH="${2:-}"
      shift 2
      ;;
    --service)
      SERVICE_NAME="${2:-}"
      shift 2
      ;;
    --scope)
      SYSTEMD_SCOPE="${2:-}"
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
    --refresh-unit-files)
      REFRESH_UNIT_FILES=1
      shift
      ;;
    --skip-upgrade)
      SKIP_UPGRADE=1
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

if [[ -z "${VERSION}" || -z "${CONFIG_PATH}" || -z "${SERVICE_NAME}" || -z "${SYSTEMD_SCOPE}" ]]; then
  echo "Invalid empty argument value." >&2
  exit 1
fi

if [[ "${SYSTEMD_SCOPE}" != "user" && "${SYSTEMD_SCOPE}" != "system" ]]; then
  echo "--scope must be 'user' or 'system'." >&2
  exit 1
fi

RELEASES_ROOT="${GG_RUNTIME_RELEASES_ROOT:-${HOME}/.local/share/gg-runtime}"
CURRENT_ROOT="${RELEASES_ROOT}/current"
RUNTIME_BIN="${CURRENT_ROOT}/bin/gg-runtime-server"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
UPGRADE_SCRIPT="${ROOT_DIR}/scripts/upgrade-runtime.sh"
PREFLIGHT_SCRIPT="${ROOT_DIR}/scripts/preflight-runtime.sh"

if [[ ! -x "${UPGRADE_SCRIPT}" ]]; then
  echo "Missing upgrade script: ${UPGRADE_SCRIPT}" >&2
  exit 1
fi
if [[ ! -x "${PREFLIGHT_SCRIPT}" ]]; then
  echo "Missing preflight script: ${PREFLIGHT_SCRIPT}" >&2
  exit 1
fi

run_systemctl() {
  if [[ "${SYSTEMD_SCOPE}" == "user" ]]; then
    systemctl --user "$@"
  else
    systemctl "$@"
  fi
}

if [[ "${SKIP_UPGRADE}" -eq 0 ]]; then
  echo "[1/6] Activating release ${VERSION}"
  GG_RUNTIME_REPO="${GG_RUNTIME_REPO:-amxv/gooselake}" \
  GG_RUNTIME_RELEASES_ROOT="${RELEASES_ROOT}" \
  "${UPGRADE_SCRIPT}" "${VERSION}"
else
  echo "[1/6] Skipping upgrade step"
fi

if [[ ! -x "${RUNTIME_BIN}" ]]; then
  echo "Runtime binary not found after activation: ${RUNTIME_BIN}" >&2
  exit 1
fi

echo "[2/6] Ensuring config + systemd template paths"
CONFIG_DIR="$(dirname "${CONFIG_PATH}")"
mkdir -p "${CONFIG_DIR}"
CONFIG_PATH="$(cd "${CONFIG_DIR}" && pwd)/$(basename "${CONFIG_PATH}")"

RUNTIME_EXAMPLE="${CURRENT_ROOT}/runtime-server.toml.example"
if [[ ! -f "${CONFIG_PATH}" ]]; then
  if [[ ! -f "${RUNTIME_EXAMPLE}" ]]; then
    echo "Missing runtime example config: ${RUNTIME_EXAMPLE}" >&2
    exit 1
  fi
  cp "${RUNTIME_EXAMPLE}" "${CONFIG_PATH}"
  echo "Created config from template: ${CONFIG_PATH}"
fi

if [[ "${SYSTEMD_SCOPE}" == "user" ]]; then
  SERVICE_DIR="${HOME}/.config/systemd/user"
  ENV_PATH="${HOME}/.config/gg-runtime/runtime.env"
else
  SERVICE_DIR="/etc/systemd/system"
  ENV_PATH="/etc/gg-runtime/runtime.env"
fi
SERVICE_PATH="${SERVICE_DIR}/${SERVICE_NAME}"
mkdir -p "${SERVICE_DIR}" "$(dirname "${ENV_PATH}")"

SERVICE_TEMPLATE="${CURRENT_ROOT}/deploy/systemd/gg-runtime.service.example"
ENV_TEMPLATE="${CURRENT_ROOT}/deploy/systemd/gg-runtime.env.example"

if [[ ! -f "${SERVICE_PATH}" || "${REFRESH_UNIT_FILES}" -eq 1 ]]; then
  if [[ ! -f "${SERVICE_TEMPLATE}" ]]; then
    echo "Missing service template: ${SERVICE_TEMPLATE}" >&2
    exit 1
  fi
  cp "${SERVICE_TEMPLATE}" "${SERVICE_PATH}"
  echo "Installed service file: ${SERVICE_PATH}"
fi

if [[ ! -f "${ENV_PATH}" || "${REFRESH_UNIT_FILES}" -eq 1 ]]; then
  if [[ -f "${ENV_TEMPLATE}" ]]; then
    cp "${ENV_TEMPLATE}" "${ENV_PATH}"
    echo "Installed env file: ${ENV_PATH}"
  else
    : > "${ENV_PATH}"
    echo "Created empty env file: ${ENV_PATH}"
  fi
fi

echo "[3/6] Running preflight filesystem/config checks"
"${PREFLIGHT_SCRIPT}" \
  --config "${CONFIG_PATH}" \
  --runtime-bin "${RUNTIME_BIN}" \
  --skip-http

echo "[4/6] Reloading/enabling/starting service"
run_systemctl daemon-reload
run_systemctl enable --now "${SERVICE_NAME}"

echo "[5/6] Restarting service to ensure current release path is active"
run_systemctl restart "${SERVICE_NAME}"

echo "[6/6] Optional post-start HTTP verification"
if [[ -n "${BASE_URL}" ]]; then
  if [[ -z "${TOKEN}" ]]; then
    echo "--base-url provided without --token; running public-only check"
    curl -fsS "${BASE_URL%/}/health" >/dev/null
  else
    "${PREFLIGHT_SCRIPT}" \
      --config "${CONFIG_PATH}" \
      --runtime-bin "${RUNTIME_BIN}" \
      --base-url "${BASE_URL}" \
      --token "${TOKEN}"
  fi
else
  echo "Skipping HTTP verification (no --base-url provided)."
fi

cat <<EOF
Deployment complete.

Service: ${SERVICE_NAME} (${SYSTEMD_SCOPE})
Config:  ${CONFIG_PATH}
Binary:  ${RUNTIME_BIN}

Useful commands:
  $( [[ "${SYSTEMD_SCOPE}" == "user" ]] && echo "systemctl --user status ${SERVICE_NAME}" || echo "systemctl status ${SERVICE_NAME}" )
  $( [[ "${SYSTEMD_SCOPE}" == "user" ]] && echo "journalctl --user -u ${SERVICE_NAME} -f" || echo "journalctl -u ${SERVICE_NAME} -f" )
EOF
