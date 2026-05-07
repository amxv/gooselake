#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Perform staged GG Runtime upgrade with atomic symlink switch.

Usage:
  scripts/upgrade-runtime.sh [version]

Arguments:
  version     Release tag (for example: v0.1.0). Defaults to "latest".

Environment:
  GG_RUNTIME_REPO            GitHub repo in owner/name form.
                             Default: amxv/gg-agent-runtime.
  GG_RUNTIME_RELEASES_ROOT   Root directory for staged releases.
                             Default: ~/.local/share/gg-runtime
  GG_RUNTIME_SYSTEMD_SERVICE Optional systemd service name to restart
                             after activation (example: gg-runtime.service).
  GG_RUNTIME_SYSTEMD_SCOPE   "user" (default) or "system".

Examples:
  ./scripts/upgrade-runtime.sh latest
  GG_RUNTIME_RELEASES_ROOT=/opt/gg-runtime ./scripts/upgrade-runtime.sh v0.1.0
  GG_RUNTIME_SYSTEMD_SERVICE=gg-runtime.service ./scripts/upgrade-runtime.sh latest
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

VERSION="${1:-latest}"
REPO="${GG_RUNTIME_REPO:-amxv/gg-agent-runtime}"
RELEASES_ROOT="${GG_RUNTIME_RELEASES_ROOT:-${HOME}/.local/share/gg-runtime}"
RELEASES_DIR="${RELEASES_ROOT}/releases"
CURRENT_LINK="${RELEASES_ROOT}/current"
SYSTEMD_SERVICE="${GG_RUNTIME_SYSTEMD_SERVICE:-}"
SYSTEMD_SCOPE="${GG_RUNTIME_SYSTEMD_SCOPE:-user}"

if [[ "${SYSTEMD_SCOPE}" != "user" && "${SYSTEMD_SCOPE}" != "system" ]]; then
  echo "GG_RUNTIME_SYSTEMD_SCOPE must be 'user' or 'system'." >&2
  exit 1
fi

OS="$(uname -s)"
ARCH="$(uname -m)"
case "${OS}" in
  Linux)
    PLATFORM="linux"
    ;;
  Darwin)
    PLATFORM="darwin"
    ;;
  *)
    echo "Unsupported OS: ${OS}" >&2
    exit 1
    ;;
esac

case "${ARCH}" in
  x86_64|amd64)
    ARCH_SLUG="x86_64"
    ;;
  arm64|aarch64)
    ARCH_SLUG="arm64"
    ;;
  *)
    echo "Unsupported architecture: ${ARCH}" >&2
    exit 1
    ;;
esac

ASSET="gg-runtime-${PLATFORM}-${ARCH_SLUG}.tar.gz"
if [[ "${VERSION}" == "latest" ]]; then
  DOWNLOAD_URL="https://github.com/${REPO}/releases/latest/download/${ASSET}"
else
  DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${ASSET}"
fi

TMP_DIR="$(mktemp -d)"
cleanup() {
  /bin/rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

echo "Downloading ${DOWNLOAD_URL}"
curl -fsSL "${DOWNLOAD_URL}" -o "${TMP_DIR}/runtime.tar.gz"
tar -xzf "${TMP_DIR}/runtime.tar.gz" -C "${TMP_DIR}"

PACKAGE_DIR="$(find "${TMP_DIR}" -maxdepth 1 -type d -name 'gg-runtime-*' | head -n 1)"
if [[ -z "${PACKAGE_DIR}" ]]; then
  echo "Invalid archive layout: package directory not found." >&2
  exit 1
fi

mkdir -p "${RELEASES_DIR}"
RELEASE_ID="${VERSION}-$(date +%Y%m%d%H%M%S)"
TARGET_DIR="${RELEASES_DIR}/${RELEASE_ID}"
if [[ -e "${TARGET_DIR}" ]]; then
  echo "Release target already exists: ${TARGET_DIR}" >&2
  exit 1
fi

/bin/cp -R "${PACKAGE_DIR}" "${TARGET_DIR}"
PREVIOUS_TARGET=""
if [[ -L "${CURRENT_LINK}" ]]; then
  PREVIOUS_TARGET="$(readlink "${CURRENT_LINK}" || true)"
fi

ln -sfn "${TARGET_DIR}" "${CURRENT_LINK}"

if [[ -n "${SYSTEMD_SERVICE}" ]]; then
  if [[ "${SYSTEMD_SCOPE}" == "user" ]]; then
    systemctl --user daemon-reload
    systemctl --user restart "${SYSTEMD_SERVICE}"
  else
    systemctl daemon-reload
    systemctl restart "${SYSTEMD_SERVICE}"
  fi
fi

cat <<MSG
Activated release at ${TARGET_DIR}
Current symlink: ${CURRENT_LINK} -> ${TARGET_DIR}

Use this runtime binary path in systemd ExecStart:
  ${CURRENT_LINK}/bin/gg-runtime-server
MSG

if [[ -n "${PREVIOUS_TARGET}" ]]; then
  cat <<MSG

Rollback command:
  ln -sfn "${PREVIOUS_TARGET}" "${CURRENT_LINK}"
MSG
  if [[ -n "${SYSTEMD_SERVICE}" ]]; then
    if [[ "${SYSTEMD_SCOPE}" == "user" ]]; then
      cat <<MSG
  systemctl --user restart "${SYSTEMD_SERVICE}"
MSG
    else
      cat <<MSG
  systemctl restart "${SYSTEMD_SERVICE}"
MSG
    fi
  fi
fi
