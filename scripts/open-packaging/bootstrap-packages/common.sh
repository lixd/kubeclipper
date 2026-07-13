#!/usr/bin/env bash

set -euo pipefail

KC_BOOTSTRAP_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$KC_BOOTSTRAP_COMMON_DIR/../../.." && pwd)"
DEFAULT_REGISTRY_PREFIX="ghcr.io/lixd/kubeclipper"

# Keep package archives clean when scripts are run from macOS developer
# machines. BSD tar otherwise may add AppleDouble files such as ._etcd.
export COPYFILE_DISABLE=1
export COPY_EXTENDED_ATTRIBUTES_DISABLE=1

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
  local partial="${dst}.part"

  mkdir -p "$(dirname "$dst")"
  if command -v curl >/dev/null 2>&1; then
    local curl_retry_all_errors=()
    if curl --help all 2>/dev/null | grep -q -- '--retry-all-errors'; then
      curl_retry_all_errors=(--retry-all-errors)
    fi
    curl --http1.1 -fL \
      --continue-at - \
      --retry "$retries" \
      --retry-delay 2 \
      --connect-timeout "$connect_timeout" \
      --max-time "$max_time" \
      "${curl_retry_all_errors[@]}" \
      "$url" -o "$partial"
    mv -f "$partial" "$dst"
    return
  fi
  if command -v wget >/dev/null 2>&1; then
    wget --continue --tries "$retries" \
      --connect-timeout "$connect_timeout" \
      --timeout "$max_time" \
      -O "$partial" "$url"
    mv -f "$partial" "$dst"
    return
  fi
  die "curl or wget is required"
}

init_bootstrap_publish_workspace() {
  [[ -n "${version:-}" ]] || die "--version is required"
  [[ "${arch:-}" == "amd64" || "${arch:-}" == "arm64" ]] || die "--arch must be amd64 or arm64"
  registry_prefix="${registry_prefix:-$DEFAULT_REGISTRY_PREFIX}"

  workdir="$(mktemp -d -t kc-bootstrap-oci.XXXXXX)"
  trap 'rm -rf "$workdir"' EXIT

  artifact_dir="$workdir/artifacts"
  build_dir="$workdir/bin"
  mkdir -p "$artifact_dir" "$build_dir"
}

find_asset() {
  local name=$1
  if [[ -x "$build_dir/$name" || -f "$build_dir/$name" ]]; then
    echo "$build_dir/$name"
    return 0
  fi
  return 1
}

docker_network_args() {
  if [[ -n "${KC_DOCKER_NETWORK:-}" ]]; then
    printf '%s\n' "--network=${KC_DOCKER_NETWORK}"
  fi
}

docker_proxy_args() {
  if [[ "${KC_DOCKER_PROXY:-true}" == "false" ]]; then
    return
  fi
  for name in HTTP_PROXY HTTPS_PROXY NO_PROXY http_proxy https_proxy no_proxy; do
    if [[ -n "${!name:-}" ]]; then
      printf '%s\n' "-e"
      printf '%s\n' "$name=${!name}"
    fi
  done
}

docker_go_cache_args() {
  local cache_root="${KC_GO_CACHE_DIR:-${TMPDIR:-/tmp}/kc-go-build-cache}"
  mkdir -p "$cache_root/pkg/mod" "$cache_root/build"
  printf '%s\n' "-v"
  printf '%s\n' "$cache_root/pkg/mod:/go/pkg/mod"
  printf '%s\n' "-v"
  printf '%s\n' "$cache_root/build:/root/.cache/go-build"
}

docker_go_env_args() {
  for name in GOPROXY GOSUMDB GOPRIVATE GONOSUMDB GONOPROXY; do
    if [[ -n "${!name:-}" ]]; then
      printf '%s\n' "-e"
      printf '%s\n' "$name=${!name}"
    fi
  done
}

build_core_binaries() {
  echo "building core binaries for linux/$arch"
  if command -v go >/dev/null 2>&1; then
    (
      cd "$ROOT"
      for attempt in 1 2 3; do
        if GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o "$build_dir/kubeclipper-server" ./cmd/kubeclipper-server &&
          GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o "$build_dir/kubeclipper-agent" ./cmd/kubeclipper-agent; then
          exit 0
        fi
        echo "go build failed; retrying ($attempt/3)" >&2
        sleep "$((attempt * 2))"
      done
      exit 1
    )
    return
  fi

  local engine
  engine="$(command -v docker || command -v podman || true)"
  [[ -n "$engine" ]] || die "go is not installed and docker/podman is not available for containerized build"
  local image="${KC_GO_BUILDER_IMAGE:-golang:1.24.2}"
  echo "go not found; building with $image"
  "$engine" run --rm \
    $(docker_network_args) \
    $(docker_proxy_args) \
    $(docker_go_cache_args) \
    $(docker_go_env_args) \
    -v "$ROOT:/workspace" \
    -v "$build_dir:/out" \
    -w /workspace \
    -e GOOS=linux \
    -e GOARCH="$arch" \
    -e CGO_ENABLED=0 \
    "$image" \
    sh -c 'for attempt in 1 2 3; do go build -o /out/kubeclipper-server ./cmd/kubeclipper-server && go build -o /out/kubeclipper-agent ./cmd/kubeclipper-agent && exit 0; echo "go build failed; retrying ($attempt/3)" >&2; sleep $((attempt * 2)); done; exit 1'
}

download_etcd_binaries() {
  echo "downloading etcd $version for linux/$arch"
  local archive="$workdir/etcd.tar.gz"
  local url="https://github.com/etcd-io/etcd/releases/download/v$version/etcd-v$version-linux-$arch.tar.gz"
  download "$url" "$archive"
  tar -xzf "$archive" -C "$workdir"
  cp -f "$workdir/etcd-v$version-linux-$arch/etcd" "$build_dir/etcd"
  cp -f "$workdir/etcd-v$version-linux-$arch/etcdctl" "$build_dir/etcdctl"
  cp -f "$workdir/etcd-v$version-linux-$arch/etcdutl" "$build_dir/etcdutl"
}

download_caddy_binary() {
  echo "downloading caddy $caddy_version for linux/$arch"
  local archive="$workdir/caddy.tar.gz"
  local caddy_arch="$arch"
  local url="https://github.com/caddyserver/caddy/releases/download/v$caddy_version/caddy_${caddy_version}_linux_${caddy_arch}.tar.gz"
  download "$url" "$archive"
  mkdir -p "$workdir/caddy"
  tar -xzf "$archive" -C "$workdir/caddy"
  cp -f "$workdir/caddy/caddy" "$build_dir/caddy"
}

download_registry_binary() {
  echo "downloading distribution registry $version for linux/$arch"
  local archive="$workdir/registry.tar.gz"
  local url="https://github.com/distribution/distribution/releases/download/v$version/registry_${version}_linux_${arch}.tar.gz"
  download "$url" "$archive"
  mkdir -p "$workdir/registry"
  tar -xzf "$archive" -C "$workdir/registry"
  local found
  found="$(find "$workdir/registry" -type f -name registry -perm -111 | head -n 1)"
  [[ -n "$found" ]] || found="$(find "$workdir/registry" -type f -name registry | head -n 1)"
  [[ -n "$found" ]] || die "registry archive did not contain a registry binary"
  cp -f "$found" "$build_dir/registry"
}

make_bootstrap_package() {
  local name=$1
  local pkg="$artifact_dir/${name}-${version}-${arch}.tar.gz"
  local root="$workdir/pkg-$name"
  shift

  rm -rf "$root"
  mkdir -p "$root/$name/$version/$arch"
  while [[ $# -gt 0 ]]; do
    local source=$1
    local filename=$2
    shift 2
    cp "$source" "$root/$name/$version/$arch/$filename"
    chmod 0755 "$root/$name/$version/$arch/$filename" 2>/dev/null || true
  done
  tar -C "$root" -zcf "$pkg" "$name"
  echo "$pkg"
}

publish_bootstrap_package() {
  local name=$1
  shift
  local pkg
  pkg="$(make_bootstrap_package "$name" "$@")"
  echo "publishing bootstrap package image: $name"
  "$ROOT/scripts/publish-oci-package.sh" \
    --package "$pkg" \
    --kind bootstrap \
    --name "$name" \
    --version "$version" \
    --arch "$arch" \
    --registry "$registry_prefix" \
    --profile binary
}

prepare_console_archive() {
  normalize_console_archive() {
    local source=$1
    local extract_dir="$workdir/kc-console-extract"
    local normalize_dir="$workdir/kc-console-normalized"
    local archive="$workdir/kc-console.tar"
    rm -rf "$extract_dir" "$normalize_dir"
    mkdir -p "$extract_dir" "$normalize_dir/kc-console"
    tar -xf "$source" -C "$extract_dir"
    if [[ -d "$extract_dir/kc-console" ]]; then
      cp -R "$extract_dir/kc-console"/. "$normalize_dir/kc-console/"
    else
      cp -R "$extract_dir"/. "$normalize_dir/kc-console/"
    fi
    tar -C "$normalize_dir" -cf "$archive" kc-console
    echo "$archive"
  }

  local downloaded="$workdir/kc-console-release.tar.gz"
  local url="https://github.com/kubeclipper/console/releases/download/$version/kc-console.tar.gz"
  echo "downloading kc-console $version" >&2
  download "$url" "$downloaded"
  normalize_console_archive "$downloaded"
}
