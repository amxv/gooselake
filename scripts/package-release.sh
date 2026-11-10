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

case "${PLATFORM}:${ARCH}:${BUN_TARGET}" in
  linux:x86_64:bun-linux-x64|linux:arm64:bun-linux-arm64|darwin:arm64:bun-darwin-arm64|darwin:x86_64:bun-darwin-x64) ;;
  *)
    echo "Unsupported platform, architecture or Bun target combination: ${PLATFORM}:${ARCH}:${BUN_TARGET}" >&2
    exit 1
    ;;
esac

# Keep the release under the selected repository output root and never remove
# an existing package tree. Each build stages in its own new directory and
# refuses to replace a prior archive even if a concurrent job used the name.
case "/${OUTPUT_DIR}/" in
  *"/../"*|*"/./"*|*"//"*)
    echo "Output path contains an unsafe component" >&2
    exit 1
    ;;
esac
if [[ "${OUTPUT_DIR}" == /* || "${OUTPUT_DIR}" == -* || -z "${OUTPUT_DIR}" ]]; then
  echo "Output must be a relative repository directory" >&2
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
mkdir -p "${ROOT_DIR}/${OUTPUT_DIR}"
OUTPUT_ROOT="$(cd "${ROOT_DIR}/${OUTPUT_DIR}" && pwd -P)"
case "${OUTPUT_ROOT}/" in
  "${ROOT_DIR}/"*) ;;
  *)
    echo "Output path must remain inside the repository" >&2
    exit 1
    ;;
esac
ARCHIVE_PATH="${OUTPUT_ROOT}/${PACKAGE_NAME}.tar.gz"
if [[ -e "${ARCHIVE_PATH}" || -L "${ARCHIVE_PATH}" ]]; then
  echo "Refusing to replace existing release archive: ${ARCHIVE_PATH}" >&2
  exit 1
fi
STAGING_ROOT="$(mktemp -d "${OUTPUT_ROOT}/.gg-release-staging.XXXXXXXX")"
trap '/bin/rm -rf "${STAGING_ROOT}"' EXIT
PACKAGE_ROOT="${STAGING_ROOT}/${PACKAGE_NAME}"

mkdir -p "${PACKAGE_ROOT}/bin"
mkdir -p "${PACKAGE_ROOT}/sidecars/claude-bridge"
mkdir -p "${PACKAGE_ROOT}/sidecars/gg-mcp-server"
mkdir -p "${PACKAGE_ROOT}/deploy"

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
"${PACKAGE_ROOT}/bin/gg-runtime-server" \
  --write-contracts "${PACKAGE_ROOT}/contracts/runtime-contracts.json"
mkdir -p "${PACKAGE_ROOT}/docs"
/bin/cp -R src/content/docs/. "${PACKAGE_ROOT}/docs/"
/bin/cp -R deploy/. "${PACKAGE_ROOT}/deploy/"

chmod +x "${PACKAGE_ROOT}/bin/gg-runtime-server"
chmod +x "${PACKAGE_ROOT}/sidecars/gg-mcp-server/gg-mcp-server"
chmod +x "${PACKAGE_ROOT}/sidecars/claude-bridge/claude-bridge"

"${ROOT_DIR}/scripts/verify-release-bundle.sh" "${PACKAGE_ROOT}"
tar -czf "${STAGING_ROOT}/${PACKAGE_NAME}.tar.gz" -C "${STAGING_ROOT}" "${PACKAGE_NAME}"
# A hardlink to an absent output name is atomic and fails closed on an
# existing archive; unlike mv it cannot accidentally overwrite a prior run.
ln "${STAGING_ROOT}/${PACKAGE_NAME}.tar.gz" "${ARCHIVE_PATH}"

popd >/dev/null

echo "Created ${ARCHIVE_PATH}"
