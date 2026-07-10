#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/resource-packages/common.sh"

registry_prefix="ghcr.io/lixd/kubeclipper"
image_registry_prefix=""
version="v1.36.1"
arch="amd64"
kubeadm_conf_version="0.16.2"
kubelet_service_file=""
kubelet_pre_start_file=""
image_list_file=""
kubernetes_file=""

usage() {
  cat <<'EOF'
Usage:
  publish-resource-k8s.sh [flags]

Builds and publishes:
  <registry-prefix>/kubeclipper/packages/k8s/k8s:<version>
  Kubernetes runtime images listed by kubeadm.

Flags:
  --registry-prefix <ref>       Package Registry prefix. Default: ghcr.io/lixd/kubeclipper.
  --image-registry-prefix <ref> Runtime image Registry prefix. Default: --registry-prefix.
  --version <vX.Y.Z>            Kubernetes version. Default: v1.36.1.
  --arch <amd64|arm64>          Target architecture. Default: amd64.
  --kubeadm-conf-version <ver>  kubernetes/release tag for 10-kubeadm.conf. Default: 0.16.2.
  -h, --help                    Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --registry-prefix) need_value "$@"; registry_prefix="${2%/}"; shift 2 ;;
  --image-registry-prefix) need_value "$@"; image_registry_prefix="${2%/}"; shift 2 ;;
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --kubeadm-conf-version) need_value "$@"; kubeadm_conf_version="$2"; shift 2 ;;
  --kubelet-service-file) need_value "$@"; kubelet_service_file="$2"; shift 2 ;;
  --kubelet-pre-start-file) need_value "$@"; kubelet_pre_start_file="$2"; shift 2 ;;
  --image-list-file) need_value "$@"; image_list_file="$2"; shift 2 ;;
  --kubernetes-file) need_value "$@"; kubernetes_file="$2"; shift 2 ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

[[ "$arch" == "amd64" || "$arch" == "arm64" ]] || die "--arch must be amd64 or arm64"
[[ "$version" == v* ]] || version="v$version"

image_registry_prefix="${image_registry_prefix:-$registry_prefix}"
resource_dir="${KC_RESOURCE_DIR:-}"
if [[ -z "$resource_dir" ]]; then
  resource_dir="$(mktemp -d -t kc-resource-k8s.XXXXXX)"
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
  --kubeadm-conf-version "$kubeadm_conf_version"
)
[[ -z "$kubelet_service_file" ]] || builder_args+=(--kubelet-service-file "$kubelet_service_file")
[[ -z "$kubelet_pre_start_file" ]] || builder_args+=(--kubelet-pre-start-file "$kubelet_pre_start_file")
[[ -z "$image_list_file" ]] || builder_args+=(--image-list-file "$image_list_file")
[[ -z "$kubernetes_file" ]] || builder_args+=(--kubernetes-file "$kubernetes_file")

"$SCRIPT_DIR/resource-builders/build-k8s-package.sh" "${builder_args[@]}"
"$SCRIPT_DIR/generate-resource-metadata.sh" --resource-dir "$resource_dir" --image-registry "$image_registry"

init_resource_publish_workspace

leaf="$(resource_leaf k8s)"
[[ -f "$leaf/configs.tar.gz" ]] || die "missing $leaf/configs.tar.gz"
publish_resource_package "k8s" "k8s" "k8s" "k8s" "$leaf"

"$SCRIPT_DIR/push-runtime-images.sh" \
  --images-lock "$resource_dir/images.lock" \
  --image-registry "$image_registry" \
  --component k8s \
  --version "$version" \
  --arch "$arch"
