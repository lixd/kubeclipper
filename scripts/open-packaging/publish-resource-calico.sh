#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/resource-packages/common.sh"

registry_prefix="ghcr.io/lixd/kubeclipper"
image_registry_prefix=""
version="v3.31.5"
arch="amd64"
chart_name="tigera-operator"
chart_version=""
chart_file=""
images_file=""
chart_repository_prefix="kubeclipper/charts"
helm_oci_publish_bin="${HELM_OCI_PUBLISH_BIN:-}"

usage() {
  cat <<'EOF'
Usage:
  publish-resource-calico.sh [flags]

Builds and publishes:
  oci://<registry-prefix>/kubeclipper/charts/tigera-operator
  Calico runtime images from the bundled image list.

Flags:
  --registry-prefix <ref>       Chart Registry prefix. Default: ghcr.io/lixd/kubeclipper.
  --image-registry-prefix <ref> Runtime image Registry prefix. Default: --registry-prefix.
  --version <vX.Y.Z>            Calico version. Default: v3.31.5.
  --arch <amd64|arm64>          Target architecture. Default: amd64.
  -h, --help                    Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --registry-prefix) need_value "$@"; registry_prefix="${2%/}"; shift 2 ;;
  --image-registry-prefix) need_value "$@"; image_registry_prefix="${2%/}"; shift 2 ;;
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --chart-name) need_value "$@"; chart_name="$2"; shift 2 ;;
  --chart-version) need_value "$@"; chart_version="$2"; shift 2 ;;
  --chart-file) need_value "$@"; chart_file="$2"; shift 2 ;;
  --images-file) need_value "$@"; images_file="$2"; shift 2 ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

[[ "$arch" == "amd64" || "$arch" == "arm64" ]] || die "--arch must be amd64 or arm64"
[[ "$version" == v* ]] || version="v$version"

image_registry_prefix="${image_registry_prefix:-$registry_prefix}"
resource_dir="$(mktemp -d -t kc-resource-calico.XXXXXX)"
resource_dir_cleanup="$resource_dir"
registry="$registry_prefix"
image_registry="$image_registry_prefix"
dry_run=false

builder_args=(
  --version "$version"
  --arch "$arch"
  --output "$resource_dir"
  --chart-name "$chart_name"
)
[[ -z "$chart_version" ]] || builder_args+=(--chart-version "$chart_version")
[[ -z "$chart_file" ]] || builder_args+=(--chart-file "$chart_file")
[[ -z "$images_file" ]] || builder_args+=(--images-file "$images_file")

"$SCRIPT_DIR/resource-builders/build-calico-package.sh" "${builder_args[@]}"
"$SCRIPT_DIR/generate-resource-metadata.sh" --resource-dir "$resource_dir" --image-registry "$image_registry"

init_resource_publish_workspace

leaf="$(resource_leaf calico)"
[[ -f "$leaf/charts.tgz" ]] || die "missing $leaf/charts.tgz"
chart_descriptor="$(push_chart "calico" "$leaf/charts.tgz")"
echo "published calico chart artifact: $chart_descriptor"

"$SCRIPT_DIR/push-runtime-images.sh" \
  --images-lock "$resource_dir/images.lock" \
  --image-registry "$image_registry" \
  --component calico \
  --version "$version" \
  --arch "$arch"
