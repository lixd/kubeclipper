#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

arch="amd64"
version="2.2.4"
runc_version="1.3.3"
crictl_version="1.35.0"
output="./resource"

usage() {
  cat <<'EOF'
Usage:
  build-containerd-package.sh [flags]

Flags:
  --version <version>          containerd version. Default: 2.2.4.
  --runc-version <version>     runc version. Default: 1.3.3.
  --crictl-version <version>   crictl version. Default: 1.35.0.
  --arch <amd64|arm64|all>     Target architecture. Default: amd64.
  --output <dir>               Resource output root. Default: ./resource.
  -h, --help                   Show this help.

Output:
  <output>/containerd/<version>/<arch>/configs.tar.gz
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --runc-version) need_value "$@"; runc_version="$2"; shift 2 ;;
  --crictl-version) need_value "$@"; crictl_version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

validate_arch "$arch"
need_cmd tar
need_cmd gzip

write_default_containerd_service() {
  local dst=$1
  write_file "$dst" \
    "[Unit]" \
    "Description=containerd container runtime" \
    "Documentation=https://containerd.io" \
    "After=network.target local-fs.target" \
    "" \
    "[Service]" \
    "ExecStartPre=-/sbin/modprobe overlay" \
    "ExecStart=/usr/local/bin/containerd" \
    "Type=notify" \
    "Delegate=yes" \
    "KillMode=process" \
    "Restart=always" \
    "RestartSec=5" \
    "LimitNPROC=infinity" \
    "LimitCORE=infinity" \
    "LimitNOFILE=infinity" \
    "TasksMax=infinity" \
    "OOMScoreAdjust=-999" \
    "" \
    "[Install]" \
    "WantedBy=multi-user.target"
}

build_one() {
  local target_arch=$1
  local work package_dir build_dir down_dir url

  log "building containerd $version for $target_arch"
  work="$(mktemp -d -t kc-containerd.XXXXXX)"
  trap 'rm -rf "$work"' RETURN
  build_dir="$work/build"
  down_dir="$work/down"
  package_dir="$output/containerd/$version/$target_arch"
  mkdir -p "$build_dir/usr/local/bin" "$build_dir/usr/local/sbin" "$build_dir/etc/systemd/system" "$down_dir" "$package_dir"

  url="https://github.com/containerd/containerd/releases/download/v$version/containerd-$version-linux-$target_arch.tar.gz"
  download "$url" "$down_dir/containerd.tar.gz"
  tar -xzf "$down_dir/containerd.tar.gz" -C "$down_dir"
  cp -f "$down_dir/bin/containerd" "$build_dir/usr/local/bin/"
  cp -f "$down_dir/bin/containerd-shim-runc-v2" "$build_dir/usr/local/bin/"
  cp -f "$down_dir/bin/containerd-stress" "$build_dir/usr/local/bin/" 2>/dev/null || true
  cp -f "$down_dir/bin/ctr" "$build_dir/usr/local/bin/"

  download "https://github.com/opencontainers/runc/releases/download/v$runc_version/runc.$target_arch" "$build_dir/usr/local/sbin/runc"
  chmod +x "$build_dir/usr/local/sbin/runc"

  download "https://github.com/kubernetes-sigs/cri-tools/releases/download/v$crictl_version/crictl-v$crictl_version-linux-$target_arch.tar.gz" "$down_dir/crictl.tar.gz"
  tar -xzf "$down_dir/crictl.tar.gz" -C "$down_dir"
  cp -f "$down_dir/crictl" "$build_dir/usr/local/bin/crictl"

  if ! download "https://raw.githubusercontent.com/containerd/containerd/v$version/containerd.service" "$build_dir/etc/systemd/system/containerd.service"; then
    log "falling back to bundled containerd.service"
    write_default_containerd_service "$build_dir/etc/systemd/system/containerd.service"
  fi

  generate_manifest "$build_dir" "$build_dir/opt/kc/manifest/containerd/$version/$target_arch/config/manifest.json"
  rm -f "$package_dir/configs.tar.gz" "$package_dir/manifest.json"
  pack_configs "$build_dir" "$package_dir"
  generate_package_manifest "$package_dir"
  log "wrote $package_dir"
}

while IFS= read -r target_arch; do
  build_one "$target_arch"
done < <(arch_list "$arch")
