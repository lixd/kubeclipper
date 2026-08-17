#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/bootstrap-packages/common.sh"

registry_prefix=""
version="3.5.21"
arch="$(go env GOARCH 2>/dev/null || echo amd64)"

usage() {
  cat <<'EOF'
Usage:
  publish-bootstrap-etcd.sh [flags]

Publishes:
  <registry-prefix>/kubeclipper/packages/bootstrap/etcd:<version>

Contents:
  etcd
  etcdctl
  etcdutl

Flags:
  --registry-prefix <ref>  Registry prefix used to publish the package image. Default: ghcr.io/kubeclipper/kubeclipper.
  --version <version>      etcd version. Default: 3.5.21.
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

download_etcd_binaries

etcd_path="$(find_asset etcd)" || die "missing etcd after download"
etcdctl_path="$(find_asset etcdctl)" || die "missing etcdctl after download"
etcdutl_path="$(find_asset etcdutl)" || die "missing etcdutl after download"

publish_bootstrap_package "etcd" \
  "$etcd_path" "etcd" \
  "$etcdctl_path" "etcdctl" \
  "$etcdutl_path" "etcdutl"
