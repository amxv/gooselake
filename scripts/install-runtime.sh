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

echo "Downloading ${DOWNLOAD_URL}"
curl -fsSL "${DOWNLOAD_URL}" -o "${TMP_DIR}/runtime.tar.gz"

tar -xzf "${TMP_DIR}/runtime.tar.gz" -C "${TMP_DIR}"
PACKAGE_DIR="$(find "${TMP_DIR}" -maxdepth 1 -type d -name 'gg-runtime-*' | head -n 1)"
if [[ -z "${PACKAGE_DIR}" ]]; then
  echo "Invalid archive layout: package directory not found." >&2
  exit 1
fi

mkdir -p "${INSTALL_ROOT}/bin"
mkdir -p "${INSTALL_ROOT}/sidecars/claude-bridge"
mkdir -p "${INSTALL_ROOT}/sidecars/gg-mcp-server"
mkdir -p "${INSTALL_ROOT}/docs"

install -m 0755 "${PACKAGE_DIR}/bin/gg-runtime-server" "${INSTALL_ROOT}/bin/gg-runtime-server"
if [[ -x "${PACKAGE_DIR}/bin/gg-goosetower" ]]; then
  install -m 0755 "${PACKAGE_DIR}/bin/gg-goosetower" "${INSTALL_ROOT}/bin/gg-goosetower"
fi
install -m 0755 "${PACKAGE_DIR}/sidecars/claude-bridge/claude-bridge" "${INSTALL_ROOT}/sidecars/claude-bridge/claude-bridge"
install -m 0755 "${PACKAGE_DIR}/sidecars/gg-mcp-server/gg-mcp-server" "${INSTALL_ROOT}/sidecars/gg-mcp-server/gg-mcp-server"
install -m 0644 "${PACKAGE_DIR}/runtime-server.toml.example" "${INSTALL_ROOT}/runtime-server.toml.example"
if [[ -f "${PACKAGE_DIR}/goosetower.toml.example" ]]; then
  install -m 0644 "${PACKAGE_DIR}/goosetower.toml.example" "${INSTALL_ROOT}/goosetower.toml.example"
fi
install -m 0644 "${PACKAGE_DIR}/openapi.yaml" "${INSTALL_ROOT}/openapi.yaml"
install -m 0644 "${PACKAGE_DIR}/README.md" "${INSTALL_ROOT}/README.md"
/bin/cp -R "${PACKAGE_DIR}/docs/." "${INSTALL_ROOT}/docs/"
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

if [[ -x "${INSTALL_ROOT}/bin/gg-goosetower" ]]; then
  cat <<EOF

Optional Goosetower browser gateway:
  cp "${INSTALL_ROOT}/goosetower.toml.example" ./goosetower.toml
  ${INSTALL_ROOT}/bin/gg-goosetower --check-config --config ./goosetower.toml
  ${INSTALL_ROOT}/bin/gg-goosetower --config ./goosetower.toml
EOF
fi

if [[ -d "${INSTALL_ROOT}/deploy/systemd" ]]; then
  cat <<EOF

Optional systemd templates:
  ${INSTALL_ROOT}/deploy/systemd/gg-runtime.service.example
  ${INSTALL_ROOT}/deploy/systemd/gg-runtime.env.example
EOF
fi
