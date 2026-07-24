#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/resource-packages/common.sh"

registry_prefix="ghcr.io/kubeclipper/kubeclipper"
image_registry_prefix=""
version="v1"
arch="amd64"
helm_version="3.18.6"
etcd_version="3.5.21"
nerdctl_version="1.4.0"
cni_plugins_version="1.3.0"
calico_version="3.31.5"
conntrack_version="1.4.9"
images_file=""

usage() {
  cat <<'EOF'
Usage:
  publish-resource-k8s-extension.sh [flags]

Builds and publishes:
  <registry-prefix>/kubeclipper/packages/k8s-extension/k8s-extension:<version>
  k8s-extension runtime images from the bundled image list, when present.

Flags:
  --registry-prefix <ref>       Package Registry prefix. Default: ghcr.io/kubeclipper/kubeclipper.
  --image-registry-prefix <ref> Runtime image Registry prefix. Default: --registry-prefix.
  --version <version>           Package version. Default: v1.
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
  --helm-version) need_value "$@"; helm_version="$2"; shift 2 ;;
  --etcd-version) need_value "$@"; etcd_version="$2"; shift 2 ;;
  --nerdctl-version) need_value "$@"; nerdctl_version="$2"; shift 2 ;;
  --cni-plugins-version) need_value "$@"; cni_plugins_version="$2"; shift 2 ;;
  --calico-version) need_value "$@"; calico_version="$2"; shift 2 ;;
  --conntrack-version) need_value "$@"; conntrack_version="$2"; shift 2 ;;
  --images-file) need_value "$@"; images_file="$2"; shift 2 ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

[[ "$arch" == "amd64" || "$arch" == "arm64" ]] || die "--arch must be amd64 or arm64"
[[ "$version" == v* ]] || version="v$version"

image_registry_prefix="${image_registry_prefix:-$registry_prefix}"
resource_dir="${KC_RESOURCE_DIR:-}"
if [[ -z "$resource_dir" ]]; then
  resource_dir="$(mktemp -d -t kc-resource-k8s-extension.XXXXXX)"
  resource_dir_cleanup="$resource_dir"
else
  mkdir -p "$resource_dir"
  resource_dir_cleanup=""
fi
registry="$registry_prefix"
image_registry="$image_registry_prefix"
dry_run=false

builder_args=(
  --version "$version"
  --arch "$arch"
  --output "$resource_dir"
  --helm-version "$helm_version"
  --etcd-version "$etcd_version"
  --nerdctl-version "$nerdctl_version"
  --cni-plugins-version "$cni_plugins_version"
  --calico-version "$calico_version"
  --conntrack-version "$conntrack_version"
)
[[ -z "$images_file" ]] || builder_args+=(--images-file "$images_file")

"$SCRIPT_DIR/resource-builders/build-k8s-extension-package.sh" "${builder_args[@]}"
"$SCRIPT_DIR/generate-resource-metadata.sh" --resource-dir "$resource_dir" --image-registry "$image_registry"

init_resource_publish_workspace

leaf="$(resource_leaf k8s-extension)"
[[ -f "$leaf/configs.tar.gz" ]] || die "missing $leaf/configs.tar.gz"
publish_resource_package "k8s-extension" "k8s-extension" "k8s-extension" "extension" "$leaf"

"$SCRIPT_DIR/push-runtime-images.sh" \
  --images-lock "$resource_dir/images.lock" \
  --image-registry "$image_registry" \
  --component k8s-extension \
  --version "$version" \
  --arch "$arch"
