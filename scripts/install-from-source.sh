#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Build and install GG Runtime bundle from source on this machine.

Usage:
  scripts/install-from-source.sh [install-root]

Arguments:
  install-root   Install prefix. Default: ~/.local

Example:
  ./scripts/install-from-source.sh
  ./scripts/install-from-source.sh /opt/gg-runtime
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

INSTALL_ROOT="${1:-${GG_RUNTIME_INSTALL_ROOT:-${HOME}/.local}}"

OS="$(uname -s)"
ARCH="$(uname -m)"
case "${OS}" in
  Linux) PLATFORM="linux" ;;
  Darwin) PLATFORM="darwin" ;;
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

case "${PLATFORM}-${ARCH_SLUG}" in
  linux-x86_64) BUN_TARGET="bun-linux-x64" ;;
  darwin-arm64) BUN_TARGET="bun-darwin-arm64" ;;
  darwin-x86_64) BUN_TARGET="bun-darwin-x64" ;;
  *)
    echo "No bun compile target mapping for ${PLATFORM}-${ARCH_SLUG}" >&2
    exit 1
    ;;
esac

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "${ROOT_DIR}/tmp"
TMP_OUTPUT="$(mktemp -d "${ROOT_DIR}/tmp/.gg-release-local.XXXXXXXX")"
trap '/bin/rm -rf "${TMP_OUTPUT}"' EXIT
REL_OUTPUT="${TMP_OUTPUT#"${ROOT_DIR}/"}"
chmod +x "${ROOT_DIR}/scripts/package-release.sh"
"${ROOT_DIR}/scripts/package-release.sh" \
  --platform "${PLATFORM}" \
  --arch "${ARCH_SLUG}" \
  --bun-target "${BUN_TARGET}" \
  --output "${REL_OUTPUT}"

ARCHIVE_PATH="${TMP_OUTPUT}/gg-runtime-${PLATFORM}-${ARCH_SLUG}.tar.gz"
TMP_EXTRACT="$(mktemp -d)"
trap '/bin/rm -rf "${TMP_EXTRACT}" "${TMP_OUTPUT}"' EXIT
tar -xzf "${ARCHIVE_PATH}" -C "${TMP_EXTRACT}"

PACKAGE_DIR="${TMP_EXTRACT}/gg-runtime-${PLATFORM}-${ARCH_SLUG}"
"${ROOT_DIR}/scripts/verify-release-bundle.sh" "${PACKAGE_DIR}"
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
Installed from source to ${INSTALL_ROOT}

Next steps:
1. export PATH="${INSTALL_ROOT}/bin:\$PATH"
2. cp "${INSTALL_ROOT}/runtime-server.toml.example" ./runtime-server.toml
3. codex login
4. claude login
5. gg-runtime-server --check-config --config ./runtime-server.toml
6. gg-runtime-server --config ./runtime-server.toml
EOF

if [[ -d "${INSTALL_ROOT}/deploy/systemd" ]]; then
  cat <<EOF

Optional systemd templates:
  ${INSTALL_ROOT}/deploy/systemd/gg-runtime.service.example
  ${INSTALL_ROOT}/deploy/systemd/gg-runtime.env.example
EOF
fi
