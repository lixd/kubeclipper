#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

arch="amd64"
version="v1.36.1"
etcd_version="3.5.21"
helm_version="3.18.6"
kubeadm_conf_version="0.16.2"
output="./resource"
kubernetes_url=""
kubeadm_conf_url=""
etcd_url=""
helm_url=""
conntrack_file=""
conntrack_url=""
conntrack_version="1.4.9"
conntrack_build_method="docker-source"
kubelet_service_file=""
kubelet_service_url=""
kubelet_pre_start_file=""
kubelet_pre_start_url=""
image_list_file=""
image_runtime=""
skip_images=false

usage() {
  cat <<'EOF'
Usage:
  build-k8s-package.sh [flags]

Flags:
  --version <vX.Y.Z>              Kubernetes version. Default: v1.36.1.
  --etcd-version <version>        etcd version for etcdctl. Default: 3.5.21.
  --helm-version <version>        Helm version. Default: 3.18.6.
  --kubeadm-conf-version <ver>    kubernetes/release tag for 10-kubeadm.conf. Default: 0.16.2.
  --arch <amd64|arm64|all>        Target architecture. Default: amd64.
  --output <dir>                  Resource output root. Default: ./resource.
  --kubernetes-url <url>          Override Kubernetes server tarball URL for a single arch.
  --kubeadm-conf-url <url>        Override 10-kubeadm.conf URL.
  --etcd-url <url>                Override etcd tarball URL for a single arch.
  --helm-url <url>                Override Helm tarball URL for a single arch.
  --conntrack-file <file>         Local conntrack binary. Skips the default source build.
  --conntrack-url <url>           conntrack binary URL. Skips the default source build.
  --conntrack-version <version>   conntrack-tools version for the default source build. Default: 1.4.9.
  --conntrack-build-method <mode> conntrack build method. Default: docker-source.
  --kubelet-service-file <file>   Use an existing kubelet.service file.
  --kubelet-service-url <url>     Download kubelet.service from URL.
  --kubelet-pre-start-file <file> Use an existing kubelet-pre-start.sh file.
  --kubelet-pre-start-url <url>   Download kubelet-pre-start.sh from URL.
  --image-list-file <file>        Use an explicit runtime image list instead of kubeadm.
  --image-runtime <tool>          podman or docker. Default: auto-detect.
  --skip-images                   Build configs.tar.gz only.
  -h, --help                      Show this help.

Output:
  <output>/k8s/<version>/<arch>/configs.tar.gz
  <output>/k8s/<version>/<arch>/images.tar.gz

Notes:
  This script uses public release URLs by default. conntrack is built from
  public netfilter sources unless --conntrack-file or --conntrack-url is set.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --etcd-version) need_value "$@"; etcd_version="$2"; shift 2 ;;
  --helm-version) need_value "$@"; helm_version="$2"; shift 2 ;;
  --kubeadm-conf-version) need_value "$@"; kubeadm_conf_version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
  --kubernetes-url) need_value "$@"; kubernetes_url="$2"; shift 2 ;;
  --kubeadm-conf-url) need_value "$@"; kubeadm_conf_url="$2"; shift 2 ;;
  --etcd-url) need_value "$@"; etcd_url="$2"; shift 2 ;;
  --helm-url) need_value "$@"; helm_url="$2"; shift 2 ;;
  --conntrack-file) need_value "$@"; conntrack_file="$2"; shift 2 ;;
  --conntrack-url) need_value "$@"; conntrack_url="$2"; shift 2 ;;
  --conntrack-version) need_value "$@"; conntrack_version="$2"; shift 2 ;;
  --conntrack-build-method) need_value "$@"; conntrack_build_method="$2"; shift 2 ;;
  --kubelet-service-file) need_value "$@"; kubelet_service_file="$2"; shift 2 ;;
  --kubelet-service-url) need_value "$@"; kubelet_service_url="$2"; shift 2 ;;
  --kubelet-pre-start-file) need_value "$@"; kubelet_pre_start_file="$2"; shift 2 ;;
  --kubelet-pre-start-url) need_value "$@"; kubelet_pre_start_url="$2"; shift 2 ;;
  --image-list-file) need_value "$@"; image_list_file="$2"; shift 2 ;;
  --image-runtime) need_value "$@"; image_runtime="$2"; shift 2 ;;
  --skip-images) skip_images=true; shift ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

validate_arch "$arch"
[[ "$version" == v* ]] || version="v$version"
k8s_number="${version#v}"
need_cmd tar
need_cmd gzip

write_default_kubelet_service() {
  local dst=$1
  write_file "$dst" \
    "[Unit]" \
    "Description=kubelet: The Kubernetes Node Agent" \
    "Documentation=https://kubernetes.io/docs/" \
    "Wants=network-online.target" \
    "After=network-online.target" \
    "" \
    "[Service]" \
    "ExecStartPre=/usr/bin/kubelet-pre-start.sh" \
    "ExecStart=/usr/bin/kubelet" \
    "Restart=always" \
    "StartLimitInterval=0" \
    "RestartSec=10" \
    "" \
    "[Install]" \
    "WantedBy=multi-user.target"
}

write_default_pre_start() {
  local dst=$1
  write_file "$dst" \
    "#!/usr/bin/env bash" \
    "set -e" \
    "modprobe br_netfilter || true" \
    "modprobe nf_conntrack || true"
  chmod +x "$dst"
}

prepare_image_list() {
  local target_arch=$1
  local down_dir=$2
  local out=$3

  if [[ -n "$image_list_file" ]]; then
    [[ -f "$image_list_file" ]] || die "image list file not found: $image_list_file"
    grep -vE '^[[:space:]]*(#|$)' "$image_list_file" > "$out"
    return
  fi

  local ha prepare_url
  ha="$(host_arch)"
  if [[ "$target_arch" == "$ha" && -x "$down_dir/kubernetes/server/bin/kubeadm" ]]; then
    "$down_dir/kubernetes/server/bin/kubeadm" config images list --kubernetes-version "$k8s_number" > "$out"
    printf '%s\n' "fanux/lvscare:v1.1.1" "kubeclipper/kubectl:latest" >> "$out"
    return
  fi
  prepare_url="https://dl.k8s.io/$version/kubernetes-server-linux-$ha.tar.gz"
  download "$prepare_url" "$down_dir/kubernetes-server-host.tar.gz"
  tar -xzf "$down_dir/kubernetes-server-host.tar.gz" -C "$down_dir" kubernetes/server/bin/kubeadm --strip-components=3
  "$down_dir/kubeadm" config images list --kubernetes-version "$k8s_number" > "$out"
  printf '%s\n' "fanux/lvscare:v1.1.1" "kubeclipper/kubectl:latest" >> "$out"
}

build_images() {
  local target_arch=$1
  local down_dir=$2
  local package_dir=$3
  local tool images=()

  prepare_image_list "$target_arch" "$down_dir" "$down_dir/images.txt"
  cp -f "$down_dir/images.txt" "$package_dir/images.txt"
  tool="$(image_tool "$image_runtime")"
  while IFS= read -r image; do
    pull_image_for_arch "$tool" "$image" "$target_arch"
    images+=("$image")
  done < "$down_dir/images.txt"
  save_images "$tool" "$package_dir/images.tar.gz" "${images[@]}"
}

write_image_list_only() {
  local target_arch=$1
  local down_dir=$2
  local package_dir=$3

  prepare_image_list "$target_arch" "$down_dir" "$down_dir/images.txt"
  cp -f "$down_dir/images.txt" "$package_dir/images.txt"
}

build_one() {
  local target_arch=$1
  local work package_dir build_dir down_dir url etcd_download_url helm_download_url effective_conntrack_file effective_conntrack_url

  log "building Kubernetes $version for $target_arch"
  work="$(mktemp -d -t kc-k8s.XXXXXX)"
  trap 'rm -rf "$work"' RETURN
  build_dir="$work/build"
  down_dir="$work/down"
  package_dir="$output/k8s/$version/$target_arch"
  mkdir -p "$build_dir/usr/bin" "$build_dir/usr/local/bin" "$build_dir/etc/systemd/system/kubelet.service.d" "$down_dir" "$package_dir"
  rm -f "$package_dir/configs.tar.gz" "$package_dir/images.tar.gz" "$package_dir/manifest.json"

  url="${kubernetes_url:-https://dl.k8s.io/$version/kubernetes-server-linux-$target_arch.tar.gz}"
  download "$url" "$down_dir/kubernetes-server.tar.gz"
  tar -xzf "$down_dir/kubernetes-server.tar.gz" -C "$down_dir"
  cp -f "$down_dir/kubernetes/server/bin/kubeadm" "$build_dir/usr/bin/"
  cp -f "$down_dir/kubernetes/server/bin/kubectl" "$build_dir/usr/bin/"
  cp -f "$down_dir/kubernetes/server/bin/kubelet" "$build_dir/usr/bin/"

  effective_conntrack_file="$conntrack_file"
  effective_conntrack_url="$conntrack_url"
  if [[ -z "$effective_conntrack_file" && -z "$effective_conntrack_url" ]]; then
    "$SCRIPT_DIR/build-conntrack-binary.sh" \
      --version "$conntrack_version" \
      --arch "$target_arch" \
      --output "$work/conntrack-resource" \
      --method "$conntrack_build_method"
    effective_conntrack_file="$work/conntrack-resource/conntrack/$conntrack_version/$target_arch/conntrack"
  fi
  copy_or_download "$effective_conntrack_file" "$effective_conntrack_url" "$build_dir/usr/bin/conntrack"
  chmod +x "$build_dir/usr/bin/conntrack"

  download "${kubeadm_conf_url:-https://raw.githubusercontent.com/kubernetes/release/v$kubeadm_conf_version/cmd/kubepkg/templates/latest/deb/kubeadm/10-kubeadm.conf}" \
    "$build_dir/etc/systemd/system/kubelet.service.d/10-kubeadm.conf"

  if [[ -n "$kubelet_service_file" || -n "$kubelet_service_url" ]]; then
    copy_or_download "$kubelet_service_file" "$kubelet_service_url" "$build_dir/etc/systemd/system/kubelet.service"
  else
    write_default_kubelet_service "$build_dir/etc/systemd/system/kubelet.service"
  fi

  if [[ -n "$kubelet_pre_start_file" || -n "$kubelet_pre_start_url" ]]; then
    copy_or_download "$kubelet_pre_start_file" "$kubelet_pre_start_url" "$build_dir/usr/bin/kubelet-pre-start.sh"
  else
    write_default_pre_start "$build_dir/usr/bin/kubelet-pre-start.sh"
  fi

  etcd_download_url="${etcd_url:-https://github.com/etcd-io/etcd/releases/download/v$etcd_version/etcd-v$etcd_version-linux-$target_arch.tar.gz}"
  download "$etcd_download_url" "$down_dir/etcd.tar.gz"
  tar -xzf "$down_dir/etcd.tar.gz" -C "$down_dir"
  cp -f "$down_dir/etcd-v$etcd_version-linux-$target_arch/etcdctl" "$build_dir/usr/local/bin/"

  helm_download_url="${helm_url:-https://get.helm.sh/helm-v$helm_version-linux-$target_arch.tar.gz}"
  download "$helm_download_url" "$down_dir/helm.tar.gz"
  tar -xzf "$down_dir/helm.tar.gz" -C "$down_dir"
  cp -f "$down_dir/linux-$target_arch/helm" "$build_dir/usr/local/bin/"

  generate_manifest "$build_dir" "$build_dir/opt/kc/manifest/k8s/$version/$target_arch/config/manifest.json"
  pack_configs "$build_dir" "$package_dir"
  if [[ "$skip_images" != true ]]; then
    build_images "$target_arch" "$down_dir" "$package_dir"
  else
    write_image_list_only "$target_arch" "$down_dir" "$package_dir"
  fi
  generate_package_manifest "$package_dir"
  log "wrote $package_dir"
}

while IFS= read -r target_arch; do
  build_one "$target_arch"
done < <(arch_list "$arch")
