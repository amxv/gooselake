#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPENAPI_ARTIFACT_REL="openapi/runtime-server-openapi.yaml"
OPENAPI_ARTIFACT_PATH="${ROOT_DIR}/${OPENAPI_ARTIFACT_REL}"

API_SIGNAL_FILES=(
  "crates/runtime-server/src/http.rs"
  "crates/runtime-server/src/openapi.rs"
  "${OPENAPI_ARTIFACT_REL}"
)

DOC_SIGNAL_FILES=(
  "docs/API.md"
  "docs/API_DOC_SYNC.md"
  "docs/README.md"
  "README.md"
)

usage() {
  cat <<'EOF'
Usage: ./scripts/api-doc-sync.sh <command>

Commands:
  refresh   Regenerate openapi/runtime-server-openapi.yaml from runtime-server sources.
  status    Show git status for API-signal and docs-signal files.
  check     Fail if API-signal files changed but docs-signal files did not.
EOF
}

run_refresh() {
  (
    cd "${ROOT_DIR}"
    cargo run -p runtime-server --bin gg-runtime-server -- --write-openapi "${OPENAPI_ARTIFACT_PATH}"
  )
  echo "Regenerated ${OPENAPI_ARTIFACT_REL}"
  echo "Next: run './scripts/api-doc-sync.sh status' and update docs if needed."
}

show_status() {
  (
    cd "${ROOT_DIR}"
    echo "API-signal files:"
    git status --short -- "${API_SIGNAL_FILES[@]}" || true
    echo
    echo "Docs-signal files:"
    git status --short -- "${DOC_SIGNAL_FILES[@]}" || true
  )
}

collect_changed_files() {
  (
    cd "${ROOT_DIR}"
    git status --porcelain -- "$@" | awk '{print $2}'
  )
}

run_check() {
  local api_changed
  local docs_changed

  api_changed="$(collect_changed_files "${API_SIGNAL_FILES[@]}")"
  if [[ -z "${api_changed}" ]]; then
    echo "No API-signal file changes detected."
    return 0
  fi

  docs_changed="$(collect_changed_files "${DOC_SIGNAL_FILES[@]}")"
  if [[ -z "${docs_changed}" ]]; then
    echo "API-signal files changed, but no docs-signal files changed."
    echo "Run: make api-docs-refresh"
    echo "Then update docs/API.md (and docs index links if needed)."
    return 1
  fi

  echo "API and docs changes detected."
}

main() {
  local command="${1:-help}"
  case "${command}" in
    refresh)
      run_refresh
      ;;
    status)
      show_status
      ;;
    check)
      run_check
      ;;
    help|-h|--help)
      usage
      ;;
    *)
      echo "Unknown command: ${command}" >&2
      usage >&2
      return 2
      ;;
  esac
}

main "$@"
