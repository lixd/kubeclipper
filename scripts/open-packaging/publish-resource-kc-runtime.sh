#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/resource-packages/common.sh"

image_registry_prefix="ghcr.io/lixd/kubeclipper"
version="v1.8.0"
arch="amd64"
images_file=""

usage() {
  cat <<'EOF'
Usage:
  publish-resource-kc-runtime.sh [flags]

Builds and publishes:
  KubeClipper runtime helper images such as lvscare and kubectl.

Flags:
  --image-registry-prefix <ref> Runtime image Registry prefix. Default: ghcr.io/lixd/kubeclipper.
  --version <version>           Runtime image-list version. Default: v1.8.0.
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
[[ "$version" == v* ]] || version="v$version"

resource_dir="$(mktemp -d -t kc-resource-kc-runtime.XXXXXX)"
trap 'rm -rf "$resource_dir"' EXIT

builder_args=(--version "$version" --arch "$arch" --output "$resource_dir")
[[ -z "$images_file" ]] || builder_args+=(--images-file "$images_file")

"$SCRIPT_DIR/resource-builders/build-kc-runtime-package.sh" "${builder_args[@]}"
"$SCRIPT_DIR/generate-resource-metadata.sh" --resource-dir "$resource_dir" --image-registry "$image_registry_prefix"
"$SCRIPT_DIR/push-runtime-images.sh" \
  --images-lock "$resource_dir/images.lock" \
  --image-registry "$image_registry_prefix" \
  --component kc-runtime \
  --version "$version" \
  --arch "$arch"
