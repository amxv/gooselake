#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/package-release.sh --platform <linux|darwin> --arch <x86_64|arm64> --bun-target <bun-target> [--output <dir>]

Example:
  scripts/package-release.sh --platform linux --arch x86_64 --bun-target bun-linux-x64 --output dist
EOF
}

PLATFORM=""
ARCH=""
BUN_TARGET=""
OUTPUT_DIR="dist"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --platform)
      PLATFORM="${2:-}"
      shift 2
      ;;
    --arch)
      ARCH="${2:-}"
      shift 2
      ;;
    --bun-target)
      BUN_TARGET="${2:-}"
      shift 2
      ;;
    --output)
      OUTPUT_DIR="${2:-}"
      shift 2
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

if [[ -z "${PLATFORM}" || -z "${ARCH}" || -z "${BUN_TARGET}" ]]; then
  echo "Missing required arguments." >&2
  usage
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo must be installed." >&2
  exit 1
fi
if ! command -v bun >/dev/null 2>&1; then
  echo "bun must be installed." >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PACKAGE_NAME="gg-runtime-${PLATFORM}-${ARCH}"
PACKAGE_ROOT="${ROOT_DIR}/${OUTPUT_DIR}/${PACKAGE_NAME}"
ARCHIVE_PATH="${ROOT_DIR}/${OUTPUT_DIR}/${PACKAGE_NAME}.tar.gz"

/bin/rm -rf "${PACKAGE_ROOT}"
mkdir -p "${PACKAGE_ROOT}/bin"
mkdir -p "${PACKAGE_ROOT}/sidecars/claude-bridge"
mkdir -p "${PACKAGE_ROOT}/sidecars/gg-mcp-server"
mkdir -p "${PACKAGE_ROOT}/deploy"
mkdir -p "${ROOT_DIR}/${OUTPUT_DIR}"

pushd "${ROOT_DIR}" >/dev/null

cargo build --release --bin gg-runtime-server
cargo build --release --manifest-path sidecars/gg-mcp-server/Cargo.toml --bin gg-mcp-server
bun install --cwd sidecars/claude-bridge --frozen-lockfile
bun build sidecars/claude-bridge/src/main.ts \
  --compile \
  --target "${BUN_TARGET}" \
  --outfile "${PACKAGE_ROOT}/sidecars/claude-bridge/claude-bridge"

cp target/release/gg-runtime-server "${PACKAGE_ROOT}/bin/gg-runtime-server"
GG_MCP_SERVER_BIN=""
if [[ -x "${ROOT_DIR}/target/release/gg-mcp-server" ]]; then
  GG_MCP_SERVER_BIN="${ROOT_DIR}/target/release/gg-mcp-server"
elif [[ -x "${ROOT_DIR}/sidecars/gg-mcp-server/target/release/gg-mcp-server" ]]; then
  GG_MCP_SERVER_BIN="${ROOT_DIR}/sidecars/gg-mcp-server/target/release/gg-mcp-server"
else
  echo "Unable to locate gg-mcp-server release binary." >&2
  exit 1
fi
cp "${GG_MCP_SERVER_BIN}" "${PACKAGE_ROOT}/sidecars/gg-mcp-server/gg-mcp-server"
cp examples/runtime-server.toml "${PACKAGE_ROOT}/runtime-server.toml.example"
cp README.md "${PACKAGE_ROOT}/README.md"
cp openapi/runtime-server-openapi.yaml "${PACKAGE_ROOT}/openapi.yaml"
mkdir -p "${PACKAGE_ROOT}/docs"
/bin/cp -R src/content/docs/. "${PACKAGE_ROOT}/docs/"
/bin/cp -R deploy/. "${PACKAGE_ROOT}/deploy/"

chmod +x "${PACKAGE_ROOT}/bin/gg-runtime-server"
chmod +x "${PACKAGE_ROOT}/sidecars/gg-mcp-server/gg-mcp-server"
chmod +x "${PACKAGE_ROOT}/sidecars/claude-bridge/claude-bridge"

tar -czf "${ARCHIVE_PATH}" -C "${ROOT_DIR}/${OUTPUT_DIR}" "${PACKAGE_NAME}"

popd >/dev/null

echo "Created ${ARCHIVE_PATH}"
