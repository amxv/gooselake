#!/usr/bin/env bash
# Fully offline release + installation regression. Uses the exact debug server
# binary but inert stand-in sidecar files; it never starts a provider/server or
# touches an existing runtime install.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
RUNTIME_BIN="${ROOT_DIR}/target/debug/gg-runtime-server"
if [[ ! -x "${RUNTIME_BIN}" ]]; then
  echo "Build the server first: cargo build -p runtime-server --bin gg-runtime-server" >&2
  exit 1
fi

case "$(uname -s):$(uname -m)" in
  Linux:x86_64|Linux:amd64) PLATFORM=linux; ARCH=x86_64 ;;
  Linux:arm64|Linux:aarch64) PLATFORM=linux; ARCH=arm64 ;;
  Darwin:arm64|Darwin:aarch64) PLATFORM=darwin; ARCH=arm64 ;;
  Darwin:x86_64) PLATFORM=darwin; ARCH=x86_64 ;;
  *) echo "Unsupported test host" >&2; exit 1 ;;
esac

TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/gg-release-fixture.XXXXXXXX")"
trap '/bin/rm -rf "${TEST_ROOT}"' EXIT
NAME="gg-runtime-${PLATFORM}-${ARCH}"
PACKAGE_ROOT="${TEST_ROOT}/bundle/${NAME}"
INSTALL_ROOT="${TEST_ROOT}/isolated-install"
mkdir -p \
  "${PACKAGE_ROOT}/bin" \
  "${PACKAGE_ROOT}/sidecars/claude-bridge" \
  "${PACKAGE_ROOT}/sidecars/gg-mcp-server" \
  "${PACKAGE_ROOT}/docs" \
  "${PACKAGE_ROOT}/deploy"

cp "${RUNTIME_BIN}" "${PACKAGE_ROOT}/bin/gg-runtime-server"
for file in sidecars/claude-bridge/claude-bridge sidecars/gg-mcp-server/gg-mcp-server; do
  printf '#!/bin/sh\nexit 0\n' > "${PACKAGE_ROOT}/${file}"
  chmod 0755 "${PACKAGE_ROOT}/${file}"
done
cp "${ROOT_DIR}/examples/runtime-server.toml" "${PACKAGE_ROOT}/runtime-server.toml.example"
cp "${ROOT_DIR}/README.md" "${PACKAGE_ROOT}/README.md"
cp "${ROOT_DIR}/openapi/runtime-server-openapi.yaml" "${PACKAGE_ROOT}/openapi.yaml"
cp "${ROOT_DIR}/src/content/docs/api.md" "${PACKAGE_ROOT}/docs/api.md"
cp "${ROOT_DIR}/src/content/docs/endpoint-catalog.md" "${PACKAGE_ROOT}/docs/endpoint-catalog.md"
cp "${ROOT_DIR}/deploy/systemd/gg-runtime.service.example" "${PACKAGE_ROOT}/deploy/gg-runtime.service.example"
"${RUNTIME_BIN}" --write-contracts "${PACKAGE_ROOT}/contracts/runtime-contracts.json"

"${ROOT_DIR}/scripts/verify-release-bundle.sh" "${PACKAGE_ROOT}"

# Mutating the source-copy or semantic manifest must fail verification.
printf 'tampered\n' >> "${PACKAGE_ROOT}/contracts/gooselake-harness-v1.md"
if "${ROOT_DIR}/scripts/verify-release-bundle.sh" "${PACKAGE_ROOT}" >/dev/null 2>&1; then
  echo "Release verifier accepted tampered harness asset" >&2
  exit 1
fi
"${RUNTIME_BIN}" --write-contracts "${PACKAGE_ROOT}/contracts/runtime-contracts.json"
printf 'tampered\n' >> "${PACKAGE_ROOT}/contracts/runtime-contracts.json"
if "${ROOT_DIR}/scripts/verify-release-bundle.sh" "${PACKAGE_ROOT}" >/dev/null 2>&1; then
  echo "Release verifier accepted malformed contract JSON" >&2
  exit 1
fi
"${RUNTIME_BIN}" --write-contracts "${PACKAGE_ROOT}/contracts/runtime-contracts.json"
chmod a-x "${PACKAGE_ROOT}/sidecars/claude-bridge/claude-bridge"
if "${ROOT_DIR}/scripts/verify-release-bundle.sh" "${PACKAGE_ROOT}" >/dev/null 2>&1; then
  echo "Release verifier accepted non-executable sidecar" >&2
  exit 1
fi
chmod 0755 "${PACKAGE_ROOT}/sidecars/claude-bridge/claude-bridge"

# Install the accepted archive using the same entry point as GitHub releases,
# but force a local archive and a disposable prefix, with zero network I/O.
tar -czf "${TEST_ROOT}/${NAME}.tar.gz" -C "${TEST_ROOT}/bundle" "${NAME}"
GG_RUNTIME_ARCHIVE_PATH="${TEST_ROOT}/${NAME}.tar.gz" \
  GG_RUNTIME_INSTALL_ROOT="${INSTALL_ROOT}" \
  "${ROOT_DIR}/scripts/install-runtime.sh" latest > "${TEST_ROOT}/install.log"

"${INSTALL_ROOT}/bin/gg-runtime-server" \
  --verify-contracts "${INSTALL_ROOT}/contracts/runtime-contracts.json"
(cd "${TEST_ROOT}" && \
  "${ROOT_DIR}/scripts/preflight-runtime.sh" \
    --config "${INSTALL_ROOT}/runtime-server.toml.example" \
    --runtime-bin "${INSTALL_ROOT}/bin/gg-runtime-server" \
    --skip-http > "${TEST_ROOT}/preflight.log")

# The independently staged release/upgrade entry point must also verify the
# candidate before publishing the `current` symlink, and must leave the old
# current link unchanged after an invalid candidate.
STAGED_ROOT="${TEST_ROOT}/isolated-staged"
GG_RUNTIME_ARCHIVE_PATH="${TEST_ROOT}/${NAME}.tar.gz" \
  GG_RUNTIME_RELEASES_ROOT="${STAGED_ROOT}" \
  "${ROOT_DIR}/scripts/upgrade-runtime.sh" latest > "${TEST_ROOT}/upgrade.log"
if [[ ! -L "${STAGED_ROOT}/current" ]]; then
  echo "Staged upgrade did not activate a current symlink" >&2
  exit 1
fi
"${STAGED_ROOT}/current/bin/gg-runtime-server" \
  --verify-contracts "${STAGED_ROOT}/current/contracts/runtime-contracts.json"
STAGED_BEFORE="$(readlink "${STAGED_ROOT}/current")"
cmp "${RUNTIME_BIN}" "${INSTALL_ROOT}/bin/gg-runtime-server"
cmp "${PACKAGE_ROOT}/contracts/gooselake-harness-v1.md" \
  "${INSTALL_ROOT}/contracts/gooselake-harness-v1.md"
cmp "${PACKAGE_ROOT}/openapi.yaml" "${INSTALL_ROOT}/openapi.yaml"

# A corrupted incoming archive must be rejected before touching a valid
# installed binary. Use the same canonical package path, with a bad contract.
printf 'bad-policy' >> "${PACKAGE_ROOT}/contracts/codex-model-policy.json"
tar -czf "${TEST_ROOT}/${NAME}-corrupt.tar.gz" -C "${TEST_ROOT}/bundle" "${NAME}"
if GG_RUNTIME_ARCHIVE_PATH="${TEST_ROOT}/${NAME}-corrupt.tar.gz" \
   GG_RUNTIME_INSTALL_ROOT="${INSTALL_ROOT}" \
   "${ROOT_DIR}/scripts/install-runtime.sh" latest > "${TEST_ROOT}/rejected.log" 2>&1; then
  echo "Installer accepted corrupted contract bundle" >&2
  exit 1
fi
if GG_RUNTIME_ARCHIVE_PATH="${TEST_ROOT}/${NAME}-corrupt.tar.gz" \
   GG_RUNTIME_RELEASES_ROOT="${STAGED_ROOT}" \
   "${ROOT_DIR}/scripts/upgrade-runtime.sh" latest > "${TEST_ROOT}/rejected-upgrade.log" 2>&1; then
  echo "Staged upgrade accepted corrupted contract bundle" >&2
  exit 1
fi
if [[ "$(readlink "${STAGED_ROOT}/current")" != "${STAGED_BEFORE}" ]]; then
  echo "Failed staged upgrade modified the active symlink" >&2
  exit 1
fi
cmp "${RUNTIME_BIN}" "${INSTALL_ROOT}/bin/gg-runtime-server"
"${INSTALL_ROOT}/bin/gg-runtime-server" \
  --verify-contracts "${INSTALL_ROOT}/contracts/runtime-contracts.json"

echo "Offline release layout, tamper detection, isolated install and staged upgrade: PASS"
