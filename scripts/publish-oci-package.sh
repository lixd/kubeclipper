#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT"

if [[ -z "${KC_SOURCE_REVISION:-}" ]]; then
  if ! command -v git >/dev/null 2>&1 || ! KC_SOURCE_REVISION="$(git rev-parse HEAD 2>/dev/null)"; then
    echo "error: KC_SOURCE_REVISION is required when publishing outside a Git checkout" >&2
    exit 1
  fi
  export KC_SOURCE_REVISION
fi

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
