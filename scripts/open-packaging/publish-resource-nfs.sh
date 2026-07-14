#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/resource-packages/common.sh"

image_registry_prefix="ghcr.io/lixd/kubeclipper"
version="v4.1.0"
arch="amd64"
images_file=""

usage() {
  cat <<'EOF'
Usage:
  publish-resource-nfs.sh [flags]

Builds and publishes the standard runtime images used by the embedded NFS
provisioner (v4.0.2) or NFS CSI (v4.1.0) manifests.

Flags:
  --image-registry-prefix <ref> Runtime image Registry prefix. Default: ghcr.io/lixd/kubeclipper.
  --version <version>           v4.0.2 or v4.1.0. Default: v4.1.0.
  --arch <amd64|arm64>          Target architecture. Default: amd64.
  -h, --help                    Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --image-registry-prefix) need_value "$@"; image_registry_prefix="${2%/}"; shift 2 ;;
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --images-file) need_value "$@"; images_file="$2"; shift 2 ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

[[ "$arch" == "amd64" || "$arch" == "arm64" ]] || die "--arch must be amd64 or arm64"
[[ "$version" == v4.0.2 || "$version" == v4.1.0 ]] || die "--version must be v4.0.2 or v4.1.0"

resource_dir="${KC_RESOURCE_DIR:-}"
if [[ -z "$resource_dir" ]]; then
  resource_dir="$(mktemp -d -t kc-resource-nfs.XXXXXX)"
  resource_dir_cleanup="$resource_dir"
else
  mkdir -p "$resource_dir"
  resource_dir_cleanup=""
fi
trap 'rm -rf "${resource_dir_cleanup:-}"' EXIT

builder_args=(--name nfs --version "$version" --arch "$arch" --output "$resource_dir")
[[ -z "$images_file" ]] || builder_args+=(--images-file "$images_file")
"$SCRIPT_DIR/resource-builders/build-runtime-image-set.sh" "${builder_args[@]}"
"$SCRIPT_DIR/generate-resource-metadata.sh" --resource-dir "$resource_dir" --image-registry "$image_registry_prefix"
"$SCRIPT_DIR/push-runtime-images.sh" \
  --images-lock "$resource_dir/images.lock" \
  --image-registry "$image_registry_prefix" \
  --component nfs \
  --version "$version" \
  --arch "$arch"
