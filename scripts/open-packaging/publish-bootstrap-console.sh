#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/bootstrap-packages/common.sh"

registry_prefix=""
version=""
arch="$(go env GOARCH 2>/dev/null || echo amd64)"

usage() {
  cat <<'EOF'
Usage:
  publish-bootstrap-console.sh --version <version> [flags]

Publishes:
  <registry-prefix>/kubeclipper/packages/bootstrap/console:<version>

Contents:
  caddy
  kc-console

Flags:
  --registry-prefix <ref>  Registry prefix used to publish the package image. Default: ghcr.io/kubeclipper/kubeclipper.
  --version <version>      KubeClipper/console version, e.g. v2.0.0.
  --arch <arch>            Target architecture. Default: go env GOARCH.
  -h, --help               Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --registry-prefix) need_value "$@"; registry_prefix="$2"; shift 2 ;;
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

# The configured qualification version must always match its pinned frontend.
# Custom versions may provide their own expected revision explicitly.
configured_revision="$(python3 - "$ROOT/packaging/resources.yaml" "$version" <<'PY'
import sys, yaml
with open(sys.argv[1], encoding="utf-8") as stream:
    bootstrap = (yaml.safe_load(stream) or {}).get("bootstrap") or {}
if bootstrap.get("consoleVersion") == sys.argv[2]:
    revision = bootstrap.get("consoleSourceRevision")
    if not revision:
        raise SystemExit("configured console version has no source revision")
    print(revision)
PY
)"
if [[ -n "$configured_revision" ]]; then
  if [[ -n "${KC_CONSOLE_EXPECTED_REVISION:-}" && "$KC_CONSOLE_EXPECTED_REVISION" != "$configured_revision" ]]; then
    die "console revision differs from packaging/resources.yaml"
  fi
  export KC_CONSOLE_EXPECTED_REVISION="$configured_revision"
fi

init_bootstrap_publish_workspace

caddy_version="2.10.2"

download_caddy_binary

caddy_path="$(find_asset caddy)" || die "missing caddy after download"
console_pkg="$(prepare_console_archive)"

publish_bootstrap_package "console" \
  "$caddy_path" "caddy" \
  "$console_pkg" "kc-console"
