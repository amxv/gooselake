#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ! -d "$1" ]]; then
  echo "Usage: scripts/verify-release-bundle.sh <extracted-package-directory>" >&2
  exit 2
fi

PACKAGE_ROOT="$(cd "$1" && pwd -P)"
check_file() {
  local file="$1"
  if [[ ! -f "${PACKAGE_ROOT}/${file}" ]]; then
    echo "Missing release asset: ${file}" >&2
    exit 1
  fi
}
check_executable() {
  local file="$1"
  check_file "$file"
  if [[ ! -x "${PACKAGE_ROOT}/${file}" ]]; then
    echo "Release executable lacks executable permission: ${file}" >&2
    exit 1
  fi
}

check_executable bin/gg-runtime-server
check_executable sidecars/claude-bridge/claude-bridge
check_executable sidecars/gg-mcp-server/gg-mcp-server
for file in \
  runtime-server.toml.example README.md openapi.yaml \
  contracts/runtime-contracts.json \
  contracts/gooselake-harness-v1.md \
  contracts/codex-model-policy.json \
  docs/api.md docs/endpoint-catalog.md; do
  check_file "$file"
done
if [[ ! -d "${PACKAGE_ROOT}/deploy" ]]; then
  echo "Missing deployment templates" >&2
  exit 1
fi

"${PACKAGE_ROOT}/bin/gg-runtime-server" \
  --verify-contracts "${PACKAGE_ROOT}/contracts/runtime-contracts.json"

if ! grep -q '^openapi: 3.1.0$' "${PACKAGE_ROOT}/openapi.yaml"; then
  echo "Invalid or missing OpenAPI 3.1.0 artifact" >&2
  exit 1
fi
echo "Release bundle validated: ${PACKAGE_ROOT}"
