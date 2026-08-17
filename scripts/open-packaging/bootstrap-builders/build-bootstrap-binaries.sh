#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

arch="$(go env GOARCH 2>/dev/null || echo amd64)"
output_dir="$ROOT/.kc-bootstrap-bin"
kc_version=""
etcd_version="3.5.21"
caddy_version="2.10.2"
caddy_url=""
registry_url=""
registry_file=""
skip_core=false
skip_external=false

usage() {
  cat <<'EOF'
Usage:
  build-bootstrap-binaries.sh [flags]

Flags:
  --output-dir <dir>       Directory for generated binaries. Default: ./.kc-bootstrap-bin.
  --arch <amd64|arm64>     Target architecture. Default: go env GOARCH.
  --kc-version <version>   KubeClipper version label for logs.
  --etcd-version <ver>     etcd version. Default: 3.5.21.
  --caddy-version <ver>    Caddy version. Default: 2.10.2.
  --caddy-url <url>        Override Caddy release archive URL.
  --registry-url <url>     Download a registry binary or archive from this URL.
  --registry-file <file>   Copy a local registry binary.
  --skip-core              Do not build kcctl/server/agent from this source tree.
  --skip-external          Do not download etcd/caddy/registry.
  -h, --help               Show this help.

Output:
  <output-dir>/kcctl
  <output-dir>/kubeclipper-server
  <output-dir>/kubeclipper-agent
  <output-dir>/etcd
  <output-dir>/etcdctl
  <output-dir>/etcdutl
  <output-dir>/caddy
  <output-dir>/registry   when --registry-url or --registry-file is provided

Notes:
  kcctl registry deploy should normally bootstrap the first Registry from a
  registry image such as registry:2. The registry binary artifact is optional
  here and only built when an explicit public URL or local file is provided.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

need_value() {
  [[ $# -ge 2 && -n "$2" ]] || die "$1 requires a value"
}

download() {
  local url=$1
  local dst=$2
  local retries="${KC_DOWNLOAD_RETRIES:-3}"
  local connect_timeout="${KC_DOWNLOAD_CONNECT_TIMEOUT:-20}"
  local max_time="${KC_DOWNLOAD_MAX_TIME:-900}"

  mkdir -p "$(dirname "$dst")"
  if command -v curl >/dev/null 2>&1; then
    local curl_retry_all_errors=()
    if curl --help all 2>/dev/null | grep -q -- '--retry-all-errors'; then
      curl_retry_all_errors=(--retry-all-errors)
    fi
    curl -fL \
      --retry "$retries" \
      --retry-delay 2 \
      --connect-timeout "$connect_timeout" \
      --max-time "$max_time" \
      "${curl_retry_all_errors[@]}" \
      "$url" -o "$dst"
    return
  fi
  if command -v wget >/dev/null 2>&1; then
    wget --tries "$retries" \
      --connect-timeout "$connect_timeout" \
      --timeout "$max_time" \
      -O "$dst" "$url"
    return
  fi
  die "curl or wget is required"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --output-dir) need_value "$@"; output_dir="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --kc-version) need_value "$@"; kc_version="$2"; shift 2 ;;
  --etcd-version) need_value "$@"; etcd_version="$2"; shift 2 ;;
  --caddy-version) need_value "$@"; caddy_version="$2"; shift 2 ;;
  --caddy-url) need_value "$@"; caddy_url="$2"; shift 2 ;;
  --registry-url) need_value "$@"; registry_url="$2"; shift 2 ;;
  --registry-file) need_value "$@"; registry_file="$2"; shift 2 ;;
  --skip-core) skip_core=true; shift ;;
  --skip-external) skip_external=true; shift ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

case "$arch" in
amd64 | arm64) ;;
*) die "--arch must be amd64 or arm64" ;;
esac

mkdir -p "$output_dir"
work="$(mktemp -d -t kc-bootstrap-build.XXXXXX)"
trap 'rm -rf "$work"' EXIT

if [[ "$skip_core" != true ]]; then
  echo "==> building KubeClipper core binaries ${kc_version:+($kc_version) }for linux/$arch"
  (
    cd "$ROOT"
    GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o "$output_dir/kcctl" ./cmd/kcctl
    GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o "$output_dir/kubeclipper-server" ./cmd/kubeclipper-server
    GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o "$output_dir/kubeclipper-agent" ./cmd/kubeclipper-agent
  )
fi

if [[ "$skip_external" != true ]]; then
  echo "==> downloading etcd $etcd_version for linux/$arch"
  etcd_url="https://github.com/etcd-io/etcd/releases/download/v$etcd_version/etcd-v$etcd_version-linux-$arch.tar.gz"
  download "$etcd_url" "$work/etcd.tar.gz"
  tar -xzf "$work/etcd.tar.gz" -C "$work"
  cp -f "$work/etcd-v$etcd_version-linux-$arch/etcd" "$output_dir/etcd"
  cp -f "$work/etcd-v$etcd_version-linux-$arch/etcdctl" "$output_dir/etcdctl"
  cp -f "$work/etcd-v$etcd_version-linux-$arch/etcdutl" "$output_dir/etcdutl"

  echo "==> downloading caddy $caddy_version for linux/$arch"
  caddy_url="${caddy_url:-https://github.com/caddyserver/caddy/releases/download/v$caddy_version/caddy_${caddy_version}_linux_${arch}.tar.gz}"
  download "$caddy_url" "$work/caddy.tar.gz"
  mkdir -p "$work/caddy"
  tar -xzf "$work/caddy.tar.gz" -C "$work/caddy"
  cp -f "$work/caddy/caddy" "$output_dir/caddy"

  if [[ -n "$registry_file" ]]; then
    [[ -f "$registry_file" ]] || die "registry file not found: $registry_file"
    if [[ "$(cd "$(dirname "$registry_file")" && pwd -P)/$(basename "$registry_file")" != "$(cd "$output_dir" && pwd -P)/registry" ]]; then
      cp -f "$registry_file" "$output_dir/registry"
    fi
  elif [[ -n "$registry_url" ]]; then
    echo "==> downloading registry binary"
    download "$registry_url" "$work/registry.download"
    if tar -tzf "$work/registry.download" >/dev/null 2>&1; then
      mkdir -p "$work/registry"
      tar -xzf "$work/registry.download" -C "$work/registry"
      found="$(find "$work/registry" -type f -name registry -perm -111 | head -n 1)"
      [[ -n "$found" ]] || found="$(find "$work/registry" -type f -name registry | head -n 1)"
      [[ -n "$found" ]] || die "registry archive did not contain a registry binary"
      cp -f "$found" "$output_dir/registry"
    else
      cp -f "$work/registry.download" "$output_dir/registry"
    fi
  else
    echo "==> skipping registry binary; use registry:2 image for first Registry bootstrap"
  fi
fi

chmod +x "$output_dir"/* 2>/dev/null || true
echo "wrote bootstrap binaries to $output_dir"
