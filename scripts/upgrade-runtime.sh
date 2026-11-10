#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Perform staged GG Runtime upgrade with atomic symlink switch.

Usage:
  scripts/upgrade-runtime.sh [version]

Arguments:
  version     Release tag (for example: v0.1.2). Defaults to "latest".

Environment:
  GG_RUNTIME_REPO            GitHub repo in owner/name form.
                             Default: amxv/gooselake.
  GG_RUNTIME_RELEASES_ROOT   Root directory for staged releases.
                             Default: ~/.local/share/gg-runtime
  GG_RUNTIME_SYSTEMD_SERVICE Optional systemd service name to restart
                             after activation (example: gg-runtime.service).
  GG_RUNTIME_SYSTEMD_SCOPE   "user" (default) or "system".
  GG_RUNTIME_ARCHIVE_PATH   Optional existing local release archive; skips GitHub download.

Examples:
  ./scripts/upgrade-runtime.sh latest
  GG_RUNTIME_RELEASES_ROOT=/opt/gg-runtime ./scripts/upgrade-runtime.sh v0.1.2
  GG_RUNTIME_SYSTEMD_SERVICE=gg-runtime.service ./scripts/upgrade-runtime.sh latest
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

VERSION="${1:-latest}"
REPO="${GG_RUNTIME_REPO:-amxv/gooselake}"
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

if [[ -n "${GG_RUNTIME_ARCHIVE_PATH:-}" ]]; then
  if [[ ! -f "${GG_RUNTIME_ARCHIVE_PATH}" ]]; then
    echo "Local release archive not found: ${GG_RUNTIME_ARCHIVE_PATH}" >&2
    exit 1
  fi
  echo "Loading local release archive: ${GG_RUNTIME_ARCHIVE_PATH}"
  cp "${GG_RUNTIME_ARCHIVE_PATH}" "${TMP_DIR}/runtime.tar.gz"
else
  echo "Downloading ${DOWNLOAD_URL}"
  curl -fsSL "${DOWNLOAD_URL}" -o "${TMP_DIR}/runtime.tar.gz"
fi

PACKAGE_NAME="gg-runtime-${PLATFORM}-${ARCH_SLUG}"
tar -tzf "${TMP_DIR}/runtime.tar.gz" > "${TMP_DIR}/archive-members.txt"
if [[ ! -s "${TMP_DIR}/archive-members.txt" ]]; then
  echo "Release archive is empty" >&2
  exit 1
fi
while IFS= read -r entry; do
  case "${entry}" in
    "${PACKAGE_NAME}"|"${PACKAGE_NAME}/"|"${PACKAGE_NAME}/"*) ;;
    *) echo "Release archive has an unexpected member: ${entry}" >&2; exit 1 ;;
  esac
  normalized_entry="${entry%/}"
  case "/${normalized_entry}/" in
    *"/../"*|*"/./"*|*"//"*)
      echo "Release archive contains an unsafe member: ${entry}" >&2
      exit 1
      ;;
  esac
done < "${TMP_DIR}/archive-members.txt"

tar -xzf "${TMP_DIR}/runtime.tar.gz" -C "${TMP_DIR}"
PACKAGE_DIR="${TMP_DIR}/${PACKAGE_NAME}"
if [[ ! -d "${PACKAGE_DIR}" ]]; then
  echo "Invalid archive layout: expected ${PACKAGE_NAME} directory." >&2
  exit 1
fi
if [[ -n "$(find "${PACKAGE_DIR}" -type l -print -quit)" ]]; then
  echo "Release archive contains links; refusing to upgrade" >&2
  exit 1
fi
for executable in \
  bin/gg-runtime-server \
  sidecars/claude-bridge/claude-bridge \
  sidecars/gg-mcp-server/gg-mcp-server; do
  if [[ ! -x "${PACKAGE_DIR}/${executable}" ]]; then
    echo "Missing or non-executable release binary: ${executable}" >&2
    exit 1
  fi
done
for asset in \
  contracts/runtime-contracts.json \
  contracts/gooselake-harness-v1.md \
  contracts/codex-model-policy.json openapi.yaml; do
  if [[ ! -f "${PACKAGE_DIR}/${asset}" ]]; then
    echo "Missing release asset: ${asset}" >&2
    exit 1
  fi
done
"${PACKAGE_DIR}/bin/gg-runtime-server" \
  --verify-contracts "${PACKAGE_DIR}/contracts/runtime-contracts.json"

if [[ -e "${CURRENT_LINK}" && ! -L "${CURRENT_LINK}" ]]; then
  echo "Refusing to replace non-symlink runtime current path: ${CURRENT_LINK}" >&2
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
"${TARGET_DIR}/bin/gg-runtime-server" \
  --verify-contracts "${TARGET_DIR}/contracts/runtime-contracts.json"
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
