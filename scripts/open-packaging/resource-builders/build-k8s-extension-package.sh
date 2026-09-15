#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

arch="amd64"
version="v1"
output="./resource"
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
  build-k8s-extension-package.sh [flags]

Flags:
  --version <version>             Package version. Default: v1.
  --arch <amd64|arm64|all>        Target architecture. Default: amd64.
  --output <dir>                  Resource output root. Default: ./resource.
  --helm-version <version>        Helm version. Default: 3.18.6.
  --etcd-version <version>        etcd version for etcdctl. Default: 3.5.21.
  --nerdctl-version <version>     nerdctl version. Default: 1.4.0.
  --cni-plugins-version <version> CNI plugins version. Default: 1.3.0.
  --calico-version <version>      calicoctl version. Default: 3.31.5.
  --conntrack-version <version>   conntrack-tools version. Default: 1.4.9.
  --images-file <file>            Image list file. Defaults to bundled list.
  -h, --help                      Show this help.

Output:
  <output>/k8s-extension/<version>/<arch>/configs.tar.gz
  <output>/k8s-extension/<version>/<arch>/images.txt

Notes:
  Runtime images are not embedded in this package; images.txt is consumed by
  push-runtime-images.sh.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
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

validate_arch "$arch"
[[ "$version" == v* ]] || version="v$version"
need_cmd tar
need_cmd gzip

default_images_file="$SCRIPT_DIR/images/k8s-extension-$version.txt"
if [[ -z "$images_file" && -f "$default_images_file" ]]; then
  images_file="$default_images_file"
fi

read_images() {
  [[ -n "$images_file" ]] || die "no bundled image list for k8s-extension $version; pass --images-file"
  [[ -f "$images_file" ]] || die "images file not found: $images_file"
  grep -vE '^[[:space:]]*(#|$)' "$images_file"
}

write_image_list() {
  local target_arch=$1
  local package_dir=$2

  read_images > "$package_dir/images.txt"
}

build_one() {
  local target_arch=$1
  local work package_dir build_dir down_dir etcd_download_url conntrack_file

  log "building k8s-extension $version for $target_arch"
  work="$(mktemp -d -t kc-k8s-extension.XXXXXX)"
  trap 'rm -rf "$work"' RETURN
  build_dir="$work/build"
  down_dir="$work/down"
  package_dir="$output/k8s-extension/$version/$target_arch"
  mkdir -p "$build_dir/usr/bin" "$build_dir/usr/local/bin" "$build_dir/opt/cni/bin" "$down_dir" "$package_dir"
  rm -f "$package_dir/configs.tar.gz" "$package_dir/images.txt" "$package_dir/images.tar.gz" "$package_dir/manifest.json"

  download "https://get.helm.sh/helm-v$helm_version-linux-$target_arch.tar.gz" "$down_dir/helm.tar.gz"
  tar -xzf "$down_dir/helm.tar.gz" -C "$down_dir"
  cp -f "$down_dir/linux-$target_arch/helm" "$build_dir/usr/local/bin/helm"

  etcd_download_url="https://github.com/etcd-io/etcd/releases/download/v$etcd_version/etcd-v$etcd_version-linux-$target_arch.tar.gz"
  download "$etcd_download_url" "$down_dir/etcd.tar.gz"
  tar -xzf "$down_dir/etcd.tar.gz" -C "$down_dir"
  cp -f "$down_dir/etcd-v$etcd_version-linux-$target_arch/etcdctl" "$build_dir/usr/local/bin/etcdctl"

  "$SCRIPT_DIR/build-conntrack-binary.sh" \
    --version "$conntrack_version" \
    --arch "$target_arch" \
    --output "$work/conntrack-resource"
  conntrack_file="$work/conntrack-resource/conntrack/$conntrack_version/$target_arch/conntrack"
  cp -f "$conntrack_file" "$build_dir/usr/bin/conntrack"

  download "https://github.com/containerd/nerdctl/releases/download/v$nerdctl_version/nerdctl-$nerdctl_version-linux-$target_arch.tar.gz" "$down_dir/nerdctl.tar.gz"
  tar -xzf "$down_dir/nerdctl.tar.gz" -C "$down_dir"
  cp -f "$down_dir/nerdctl" "$build_dir/usr/local/bin/nerdctl"

  download "https://github.com/containernetworking/plugins/releases/download/v$cni_plugins_version/cni-plugins-linux-$target_arch-v$cni_plugins_version.tgz" "$down_dir/cni-plugins.tgz"
  tar -xzf "$down_dir/cni-plugins.tgz" -C "$build_dir/opt/cni/bin"

  download "https://github.com/projectcalico/calico/releases/download/v$calico_version/calicoctl-linux-$target_arch" "$build_dir/usr/local/bin/calicoctl"
  chmod +x "$build_dir/usr/bin/conntrack" "$build_dir/usr/local/bin/helm" "$build_dir/usr/local/bin/etcdctl" "$build_dir/usr/local/bin/nerdctl" "$build_dir/usr/local/bin/calicoctl"

  generate_manifest "$build_dir" "$build_dir/opt/kc/manifest/k8s-extension/$version/$target_arch/config/manifest.json"
  pack_configs "$build_dir" "$package_dir"
  write_image_list "$target_arch" "$package_dir"

  generate_package_manifest "$package_dir"
  log "wrote $package_dir"
}

[[ -n "$images_file" ]] || die "no bundled image list for k8s-extension $version; pass --images-file"

while IFS= read -r target_arch; do
  build_one "$target_arch"
done < <(arch_list "$arch")
