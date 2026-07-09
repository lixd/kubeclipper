#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/resource-packages/common.sh"

registry_prefix="ghcr.io/lixd/kubeclipper"
version="2.2.4"
arch="amd64"
runc_version="1.3.3"
crictl_version="1.35.0"

usage() {
  cat <<'EOF'
Usage:
  publish-resource-containerd.sh [flags]

Builds and publishes:
  <registry-prefix>/kubeclipper/packages/cri/containerd:<version>

Flags:
  --registry-prefix <ref>  Package Registry prefix. Default: ghcr.io/lixd/kubeclipper.
  --version <version>      containerd version. Default: 2.2.4.
  --arch <amd64|arm64>     Target architecture. Default: amd64.
  --runc-version <version> runc version. Default: 1.3.3.
  --crictl-version <ver>   crictl version. Default: 1.35.0.
  -h, --help               Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --registry-prefix) need_value "$@"; registry_prefix="${2%/}"; shift 2 ;;
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --runc-version) need_value "$@"; runc_version="$2"; shift 2 ;;
  --crictl-version) need_value "$@"; crictl_version="$2"; shift 2 ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

[[ "$arch" == "amd64" || "$arch" == "arm64" ]] || die "--arch must be amd64 or arm64"

resource_dir="$(mktemp -d -t kc-resource-containerd.XXXXXX)"
resource_dir_cleanup="$resource_dir"
registry="$registry_prefix"
dry_run=false

"$SCRIPT_DIR/resource-builders/build-containerd-package.sh" \
  --version "$version" \
  --arch "$arch" \
  --output "$resource_dir" \
  --runc-version "$runc_version" \
  --crictl-version "$crictl_version"

init_resource_publish_workspace

leaf="$(resource_leaf containerd)"
[[ -f "$leaf/configs.tar.gz" ]] || die "missing $leaf/configs.tar.gz"
publish_resource_package "containerd" "cri" "containerd" "runtime" "$leaf"
