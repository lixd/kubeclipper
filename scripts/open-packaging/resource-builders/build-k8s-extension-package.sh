#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

arch="amd64"
version="v1"
output="./resource"
helm_version="3.18.6"
nerdctl_version="1.4.0"
cni_plugins_version="1.3.0"
calico_version="3.31.5"
image_runtime=""
images_file=""
skip_images=false

usage() {
  cat <<'EOF'
Usage:
  build-k8s-extension-package.sh [flags]

Flags:
  --version <version>             Package version. Default: v1.
  --arch <amd64|arm64|all>        Target architecture. Default: amd64.
  --output <dir>                  Resource output root. Default: ./resource.
  --helm-version <version>        Helm version. Default: 3.18.6.
  --nerdctl-version <version>     nerdctl version. Default: 1.4.0.
  --cni-plugins-version <version> CNI plugins version. Default: 1.3.0.
  --calico-version <version>      calicoctl version. Default: 3.31.5.
  --images-file <file>            Image list file. Defaults to bundled list.
  --image-runtime <tool>          podman or docker. Default: auto-detect.
  --skip-images                   Build configs.tar.gz only.
  -h, --help                      Show this help.

Output:
  <output>/k8s-extension/<version>/<arch>/configs.tar.gz
  <output>/k8s-extension/<version>/<arch>/images.tar.gz
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
  --helm-version) need_value "$@"; helm_version="$2"; shift 2 ;;
  --nerdctl-version) need_value "$@"; nerdctl_version="$2"; shift 2 ;;
  --cni-plugins-version) need_value "$@"; cni_plugins_version="$2"; shift 2 ;;
  --calico-version) need_value "$@"; calico_version="$2"; shift 2 ;;
  --images-file) need_value "$@"; images_file="$2"; shift 2 ;;
  --image-runtime) need_value "$@"; image_runtime="$2"; shift 2 ;;
  --skip-images) skip_images=true; shift ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

validate_arch "$arch"
[[ "$version" == v* ]] || version="v$version"
need_cmd tar
need_cmd gzip

default_images_file="$SCRIPT_DIR/images/k8s-extension-$version.txt"
if [[ -z "$images_file" && -f "$default_images_file" ]]; then
  images_file="$default_images_file"
fi

read_images() {
  [[ -n "$images_file" ]] || die "no bundled image list for k8s-extension $version; pass --images-file or --skip-images"
  [[ -f "$images_file" ]] || die "images file not found: $images_file"
  grep -vE '^[[:space:]]*(#|$)' "$images_file"
}

build_images() {
  local target_arch=$1
  local package_dir=$2
  local tool images=()

  tool="$(image_tool "$image_runtime")"
  while IFS= read -r image; do
    pull_image_for_arch "$tool" "$image" "$target_arch"
    images+=("$image")
  done < <(read_images)
  save_images "$tool" "$package_dir/images.tar.gz" "${images[@]}"
}

build_one() {
  local target_arch=$1
  local work package_dir build_dir down_dir

  log "building k8s-extension $version for $target_arch"
  work="$(mktemp -d -t kc-k8s-extension.XXXXXX)"
  trap 'rm -rf "$work"' RETURN
  build_dir="$work/build"
  down_dir="$work/down"
  package_dir="$output/k8s-extension/$version/$target_arch"
  mkdir -p "$build_dir/usr/local/bin" "$build_dir/opt/cni/bin" "$down_dir" "$package_dir"
  rm -f "$package_dir/configs.tar.gz" "$package_dir/images.tar.gz" "$package_dir/manifest.json"

  download "https://get.helm.sh/helm-v$helm_version-linux-$target_arch.tar.gz" "$down_dir/helm.tar.gz"
  tar -xzf "$down_dir/helm.tar.gz" -C "$down_dir"
  cp -f "$down_dir/linux-$target_arch/helm" "$build_dir/usr/local/bin/helm"

  download "https://github.com/containerd/nerdctl/releases/download/v$nerdctl_version/nerdctl-$nerdctl_version-linux-$target_arch.tar.gz" "$down_dir/nerdctl.tar.gz"
  tar -xzf "$down_dir/nerdctl.tar.gz" -C "$down_dir"
  cp -f "$down_dir/nerdctl" "$build_dir/usr/local/bin/nerdctl"

  download "https://github.com/containernetworking/plugins/releases/download/v$cni_plugins_version/cni-plugins-linux-$target_arch-v$cni_plugins_version.tgz" "$down_dir/cni-plugins.tgz"
  tar -xzf "$down_dir/cni-plugins.tgz" -C "$build_dir/opt/cni/bin"

  download "https://github.com/projectcalico/calico/releases/download/v$calico_version/calicoctl-linux-$target_arch" "$build_dir/usr/local/bin/calicoctl"
  chmod +x "$build_dir/usr/local/bin/helm" "$build_dir/usr/local/bin/nerdctl" "$build_dir/usr/local/bin/calicoctl"

  generate_manifest "$build_dir" "$build_dir/opt/kc/manifest/k8s-extension/$version/$target_arch/config/manifest.json"
  pack_configs "$build_dir" "$package_dir"

  if [[ "$skip_images" != true ]]; then
    build_images "$target_arch" "$package_dir"
  fi

  generate_package_manifest "$package_dir"
  log "wrote $package_dir"
}

if [[ "$skip_images" != true ]]; then
  [[ -n "$images_file" ]] || die "no bundled image list for k8s-extension $version; pass --images-file or --skip-images"
fi

while IFS= read -r target_arch; do
  build_one "$target_arch"
done < <(arch_list "$arch")
