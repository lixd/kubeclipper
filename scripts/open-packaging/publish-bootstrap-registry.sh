#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/bootstrap-packages/common.sh"

registry_prefix=""
version="3.1.1"
arch="$(go env GOARCH 2>/dev/null || echo amd64)"

usage() {
  cat <<'EOF'
Usage:
  publish-bootstrap-registry.sh [flags]

Publishes:
  <registry-prefix>/kubeclipper/packages/bootstrap/registry:<version>

Contents:
  registry

Flags:
  --registry-prefix <ref>  Registry prefix used to publish the package image. Default: ghcr.io/kubeclipper/kubeclipper.
  --version <version>      distribution registry version. Default: 3.1.1.
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

init_bootstrap_publish_workspace

download_registry_binary
registry_path="$(find_asset registry)" || die "missing registry after download"

publish_bootstrap_package "registry" \
  "$registry_path" "registry"
