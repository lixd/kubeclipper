#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT"

if [[ -n "${KC_OCI_PUBLISH_BIN:-}" ]]; then
  exec "$KC_OCI_PUBLISH_BIN" "$@"
fi

if [[ -x "$ROOT/bin/oci-publish" ]]; then
  exec "$ROOT/bin/oci-publish" "$@"
fi

if command -v oci-publish >/dev/null 2>&1; then
  exec "$(command -v oci-publish)" "$@"
fi

export GOCACHE="${GOCACHE:-$ROOT/.gocache}"
export GOMODCACHE="${GOMODCACHE:-$ROOT/.gomodcache}"

exec go run ./tools/oci-publish "$@"
