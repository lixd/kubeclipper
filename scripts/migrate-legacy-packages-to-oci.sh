#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT"

if [[ -n "${KC_OCI_MIGRATE_BIN:-}" ]]; then
  exec "$KC_OCI_MIGRATE_BIN" "$@"
fi

if [[ -x "$ROOT/bin/oci-migrate" ]]; then
  exec "$ROOT/bin/oci-migrate" "$@"
fi

export GOCACHE="${GOCACHE:-$ROOT/.gocache}"
export GOMODCACHE="${GOMODCACHE:-$ROOT/.gomodcache}"

exec go run ./tools/oci-migrate "$@"
