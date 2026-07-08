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
  publish-bootstrap-kubeclipper.sh --version <version> [flags]

Publishes:
  <registry-prefix>/kubeclipper/packages/bootstrap/kubeclipper:<version>

Contents:
  kubeclipper-server
  kubeclipper-agent

Flags:
  --registry-prefix <ref>  Registry prefix used to publish the package image. Default: ghcr.io/lixd/kubeclipper.
  --version <version>      Package image version, e.g. v1.8.0.
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

build_core_binaries

server_path="$(find_asset kubeclipper-server)" || die "missing kubeclipper-server after build"
agent_path="$(find_asset kubeclipper-agent)" || die "missing kubeclipper-agent after build"

publish_bootstrap_package "kubeclipper" \
  "$server_path" "kubeclipper-server" \
  "$agent_path" "kubeclipper-agent"
