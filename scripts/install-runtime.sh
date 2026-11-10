#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Install GG Runtime from GitHub release artifacts.

Usage:
  scripts/install-runtime.sh [version]

Arguments:
  version     Release tag (for example: v0.1.2). Defaults to "latest".

Environment:
  GG_RUNTIME_REPO          GitHub repo in owner/name form.
                           Default: amxv/gooselake.
                           If set, overrides the default.
  GG_RUNTIME_INSTALL_ROOT  Install prefix. Default: ~/.local
  GG_RUNTIME_ARCHIVE_PATH  Optional existing local release archive; skips GitHub download.

Examples:
  ./scripts/install-runtime.sh latest
  GG_RUNTIME_INSTALL_ROOT=/opt/gg-runtime ./scripts/install-runtime.sh v0.1.2
  GG_RUNTIME_REPO=owner/repo ./scripts/install-runtime.sh latest
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

VERSION="${1:-latest}"
INSTALL_ROOT="${GG_RUNTIME_INSTALL_ROOT:-${HOME}/.local}"

detect_repo() {
  if [[ -n "${GG_RUNTIME_REPO:-}" ]]; then
    echo "${GG_RUNTIME_REPO}"
    return
  fi
  echo "amxv/gooselake"
}

REPO="$(detect_repo || true)"
if [[ -z "${REPO}" ]]; then
  echo "Unable to determine GitHub repo. Set GG_RUNTIME_REPO=owner/repo." >&2
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
# Reject unexpected roots and traversal before extracting into this newly
# allocated temp directory. Do not use a loose first-match package search.
tar -tzf "${TMP_DIR}/runtime.tar.gz" > "${TMP_DIR}/archive-members.txt"
if [[ ! -s "${TMP_DIR}/archive-members.txt" ]]; then
  echo "Release archive is empty" >&2
  exit 1
fi
while IFS= read -r entry; do
  case "${entry}" in
    "${PACKAGE_NAME}"|"${PACKAGE_NAME}/"|"${PACKAGE_NAME}/"*) ;;
    *)
      echo "Release archive has an unexpected member: ${entry}" >&2
      exit 1
      ;;
  esac
  # Tar lists directory entries with a trailing slash; strip that single
  # marker before checking for genuinely doubled separators.
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
  echo "Release archive contains links; refusing to install" >&2
  exit 1
fi

# Verify all critical contents and semantic/source contract parity with the
# candidate binary before writing *any* existing installation files.
for binary in \
  bin/gg-runtime-server \
  sidecars/claude-bridge/claude-bridge \
  sidecars/gg-mcp-server/gg-mcp-server; do
  if [[ ! -x "${PACKAGE_DIR}/${binary}" ]]; then
    echo "Missing or non-executable release binary: ${binary}" >&2
    exit 1
  fi
done
for file in runtime-server.toml.example README.md openapi.yaml \
  contracts/runtime-contracts.json \
  contracts/gooselake-harness-v1.md \
  contracts/codex-model-policy.json; do
  if [[ ! -f "${PACKAGE_DIR}/${file}" ]]; then
    echo "Missing release asset: ${file}" >&2
    exit 1
  fi
done
"${PACKAGE_DIR}/bin/gg-runtime-server" \
  --verify-contracts "${PACKAGE_DIR}/contracts/runtime-contracts.json"

mkdir -p "${INSTALL_ROOT}/bin"
mkdir -p "${INSTALL_ROOT}/sidecars/claude-bridge"
mkdir -p "${INSTALL_ROOT}/sidecars/gg-mcp-server"
mkdir -p "${INSTALL_ROOT}/docs"
mkdir -p "${INSTALL_ROOT}/contracts"

install -m 0755 "${PACKAGE_DIR}/bin/gg-runtime-server" "${INSTALL_ROOT}/bin/gg-runtime-server"
install -m 0755 "${PACKAGE_DIR}/sidecars/claude-bridge/claude-bridge" "${INSTALL_ROOT}/sidecars/claude-bridge/claude-bridge"
install -m 0755 "${PACKAGE_DIR}/sidecars/gg-mcp-server/gg-mcp-server" "${INSTALL_ROOT}/sidecars/gg-mcp-server/gg-mcp-server"
install -m 0644 "${PACKAGE_DIR}/runtime-server.toml.example" "${INSTALL_ROOT}/runtime-server.toml.example"
install -m 0644 "${PACKAGE_DIR}/openapi.yaml" "${INSTALL_ROOT}/openapi.yaml"
install -m 0644 "${PACKAGE_DIR}/contracts/runtime-contracts.json" "${INSTALL_ROOT}/contracts/runtime-contracts.json"
install -m 0644 "${PACKAGE_DIR}/contracts/gooselake-harness-v1.md" "${INSTALL_ROOT}/contracts/gooselake-harness-v1.md"
install -m 0644 "${PACKAGE_DIR}/contracts/codex-model-policy.json" "${INSTALL_ROOT}/contracts/codex-model-policy.json"
install -m 0644 "${PACKAGE_DIR}/README.md" "${INSTALL_ROOT}/README.md"
/bin/cp -R "${PACKAGE_DIR}/docs/." "${INSTALL_ROOT}/docs/"
"${INSTALL_ROOT}/bin/gg-runtime-server" \
  --verify-contracts "${INSTALL_ROOT}/contracts/runtime-contracts.json"
if [[ -d "${PACKAGE_DIR}/deploy" ]]; then
  mkdir -p "${INSTALL_ROOT}/deploy"
  /bin/cp -R "${PACKAGE_DIR}/deploy/." "${INSTALL_ROOT}/deploy/"
fi

cat <<EOF
Installed GG Runtime to ${INSTALL_ROOT}

Next steps:
1. Add runtime binaries to PATH if needed:
   export PATH="${INSTALL_ROOT}/bin:\$PATH"
2. Copy config:
   cp "${INSTALL_ROOT}/runtime-server.toml.example" ./runtime-server.toml
3. Login providers on this machine:
   codex login
   claude login
4. Validate config:
   ${INSTALL_ROOT}/bin/gg-runtime-server --check-config --config ./runtime-server.toml
5. Start server:
   ${INSTALL_ROOT}/bin/gg-runtime-server --config ./runtime-server.toml
EOF

if [[ -d "${INSTALL_ROOT}/deploy/systemd" ]]; then
  cat <<EOF

Optional systemd templates:
  ${INSTALL_ROOT}/deploy/systemd/gg-runtime.service.example
  ${INSTALL_ROOT}/deploy/systemd/gg-runtime.env.example
EOF
fi
