#!/usr/bin/env bash
set -euo pipefail

LIMIT="${1:-1000}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${ROOT_DIR}"

oversized="$(
  find crates sidecars/gg-mcp-server \
    -path '*/target' -prune -o \
    -type f -name '*.rs' -print \
    | xargs wc -l \
    | awk -v limit="${LIMIT}" '$2 != "total" && $1 > limit { printf "%s %s\n", $1, $2 }' \
    | sort -nr
)"

if [[ -n "${oversized}" ]]; then
  echo "Rust files over ${LIMIT} lines:" >&2
  printf '%s\n' "${oversized}" | sed 's/^/  /' >&2
  exit 1
fi

echo "All Rust files are at or below ${LIMIT} lines."
