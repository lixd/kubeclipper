#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

arch="amd64"
version="v1.36.1"
kubeadm_conf_version="0.16.2"
output="./resource"
kubelet_service_file=""
kubelet_pre_start_file=""
image_list_file=""
kubernetes_file=""

usage() {
  cat <<'EOF'
Usage:
  build-k8s-package.sh [flags]

Flags:
  --version <vX.Y.Z>              Kubernetes version. Default: v1.36.1.
  --kubeadm-conf-version <ver>    kubernetes/release tag for 10-kubeadm.conf. Default: 0.16.2.
  --arch <amd64|arm64|all>        Target architecture. Default: amd64.
  --output <dir>                  Resource output root. Default: ./resource.
  --kubelet-service-file <file>   Use an existing kubelet.service file.
  --kubelet-pre-start-file <file> Use an existing kubelet-pre-start.sh file.
  --image-list-file <file>        Use an explicit runtime image list instead of kubeadm.
  -h, --help                      Show this help.

Output:
  <output>/k8s/<version>/<arch>/configs.tar.gz
  <output>/k8s/<version>/<arch>/images.txt

Notes:
  This script only packages native Kubernetes binaries and kubelet service
  files. Cluster helper tools such as helm, etcdctl, and conntrack belong to
  build-k8s-extension-package.sh. Runtime images are not embedded in this
  package; images.txt is consumed by push-runtime-images.sh.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --kubeadm-conf-version) need_value "$@"; kubeadm_conf_version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
  --kubelet-service-file) need_value "$@"; kubelet_service_file="$2"; shift 2 ;;
  --kubelet-pre-start-file) need_value "$@"; kubelet_pre_start_file="$2"; shift 2 ;;
  --image-list-file) need_value "$@"; image_list_file="$2"; shift 2 ;;
  --kubernetes-file) need_value "$@"; kubernetes_file="$2"; shift 2 ;;
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

write_default_kubeadm_conf() {
  local dst=$1
  write_file "$dst" \
    "[Service]" \
    "Environment=\"KUBELET_KUBEADM_ARGS=--config=/var/lib/kubelet/config.yaml\"" \
    "EnvironmentFile=-/var/lib/kubelet/kubeadm-flags.env" \
    "EnvironmentFile=-/etc/default/kubelet" \
    "ExecStart=" \
    "ExecStart=/usr/bin/kubelet \$KUBELET_KUBEADM_ARGS \$KUBELET_EXTRA_ARGS"
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
  if [[ "$target_arch" == "$ha" && -x "$down_dir/kubeadm" ]]; then
    "$down_dir/kubeadm" config images list --kubernetes-version "$k8s_number" > "$out"
    return
  fi
  prepare_url="https://dl.k8s.io/$version/kubernetes-server-linux-$ha.tar.gz"
  download "$prepare_url" "$down_dir/kubernetes-server-host.tar.gz"
  tar -xzf "$down_dir/kubernetes-server-host.tar.gz" -C "$down_dir" kubernetes/server/bin/kubeadm --strip-components=3
  "$down_dir/kubeadm" config images list --kubernetes-version "$k8s_number" > "$out"
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
  local work package_dir build_dir down_dir url

  log "building Kubernetes $version for $target_arch"
  work="$(mktemp -d -t kc-k8s.XXXXXX)"
  trap 'rm -rf "$work"' RETURN
  build_dir="$work/build"
  down_dir="$work/down"
  package_dir="$output/k8s/$version/$target_arch"
  mkdir -p "$build_dir/usr/bin" "$build_dir/usr/local/bin" "$build_dir/etc/systemd/system/kubelet.service.d" "$down_dir" "$package_dir"
  rm -f "$package_dir/configs.tar.gz" "$package_dir/images.txt" "$package_dir/images.tar.gz" "$package_dir/manifest.json"

  if [[ -n "$kubernetes_file" ]]; then
    [[ -f "$kubernetes_file" ]] || die "kubernetes file not found: $kubernetes_file"
    cp -f "$kubernetes_file" "$down_dir/kubernetes-server.tar.gz"
    tar -xzf "$down_dir/kubernetes-server.tar.gz" -C "$down_dir"
    cp -f "$down_dir/kubernetes/server/bin/kubeadm" "$build_dir/usr/bin/"
    cp -f "$down_dir/kubernetes/server/bin/kubectl" "$build_dir/usr/bin/"
    cp -f "$down_dir/kubernetes/server/bin/kubelet" "$build_dir/usr/bin/"
    cp -f "$down_dir/kubernetes/server/bin/kubeadm" "$down_dir/kubeadm"
  else
    for binary in kubeadm kubectl kubelet; do
      url="https://dl.k8s.io/$version/bin/linux/$target_arch/$binary"
      download "$url" "$build_dir/usr/bin/$binary"
      chmod +x "$build_dir/usr/bin/$binary"
    done
    cp -f "$build_dir/usr/bin/kubeadm" "$down_dir/kubeadm"
  fi

  if ! download "https://raw.githubusercontent.com/kubernetes/release/v$kubeadm_conf_version/cmd/kubepkg/templates/latest/deb/kubeadm/10-kubeadm.conf" \
    "$build_dir/etc/systemd/system/kubelet.service.d/10-kubeadm.conf"; then
    log "falling back to bundled 10-kubeadm.conf"
    write_default_kubeadm_conf "$build_dir/etc/systemd/system/kubelet.service.d/10-kubeadm.conf"
  fi

  if [[ -n "$kubelet_service_file" ]]; then
    cp -f "$kubelet_service_file" "$build_dir/etc/systemd/system/kubelet.service"
  else
    write_default_kubelet_service "$build_dir/etc/systemd/system/kubelet.service"
  fi

  if [[ -n "$kubelet_pre_start_file" ]]; then
    cp -f "$kubelet_pre_start_file" "$build_dir/usr/bin/kubelet-pre-start.sh"
    chmod +x "$build_dir/usr/bin/kubelet-pre-start.sh"
  else
    write_default_pre_start "$build_dir/usr/bin/kubelet-pre-start.sh"
  fi

  generate_manifest "$build_dir" "$build_dir/opt/kc/manifest/k8s/$version/$target_arch/config/manifest.json"
  pack_configs "$build_dir" "$package_dir"
  write_image_list_only "$target_arch" "$down_dir" "$package_dir"
  generate_package_manifest "$package_dir"
  log "wrote $package_dir"
}

while IFS= read -r target_arch; do
  build_one "$target_arch"
done < <(arch_list "$arch")
