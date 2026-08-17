#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

arch="amd64"
version="1.4.9"
output="./resource/.cache"
method="docker-source"
file=""
url=""
image="debian:bookworm-slim"

usage() {
  cat <<'EOF'
Usage:
  build-conntrack-binary.sh [flags]

Flags:
  --version <version>        conntrack-tools version. Default: 1.4.9.
  --arch <amd64|arm64|all>   Target architecture. Default: amd64.
  --output <dir>             Output root. Default: ./resource/.cache.
  --method <method>          docker-source, file, or url. Default: docker-source.
  --file <file>              Copy an existing conntrack binary. Implies --method file.
  --url <url>                Download an existing conntrack binary. Implies --method url.
  --image <image>            Builder image for --method docker-source. Default: debian:bookworm-slim.
  -h, --help                 Show this help.

Output:
  <output>/conntrack/<version>/<arch>/conntrack

Notes:
  KubeClipper packages conntrack inside the k8s configs artifact so offline
  installs do not depend on apt/yum being available on target nodes.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
  --method) need_value "$@"; method="$2"; shift 2 ;;
  --file) need_value "$@"; file="$2"; method="file"; shift 2 ;;
  --url) need_value "$@"; url="$2"; method="url"; shift 2 ;;
  --image) need_value "$@"; image="$2"; shift 2 ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

validate_arch "$arch"

build_from_file() {
  local target_arch=$1
  local dst="$output/conntrack/$version/$target_arch/conntrack"
  [[ -f "$file" ]] || die "conntrack file not found: $file"
  mkdir -p "$(dirname "$dst")"
  cp -f "$file" "$dst"
  chmod +x "$dst"
  log "wrote $dst"
}

build_from_url() {
  local target_arch=$1
  local dst="$output/conntrack/$version/$target_arch/conntrack"
  local resolved_url="${url//\$\{arch\}/$target_arch}"
  mkdir -p "$(dirname "$dst")"
  download "$resolved_url" "$dst"
  chmod +x "$dst"
  log "wrote $dst"
}

build_from_source_with_docker() {
  local target_arch=$1
  local dst_dir="$output/conntrack/$version/$target_arch"
  local platform="linux/$target_arch"
  local uid gid
  local docker_args=()
  local proxy_env

  need_cmd docker
  uid="$(id -u)"
  gid="$(id -g)"
  mkdir -p "$dst_dir"

  docker_args=(run --rm)
  if [[ -n "${KC_DOCKER_NETWORK:-}" ]]; then
    docker_args+=(--network "$KC_DOCKER_NETWORK")
  fi
  export http_proxy="${http_proxy:-${HTTP_PROXY:-}}"
  export https_proxy="${https_proxy:-${HTTPS_PROXY:-}}"
  export no_proxy="${no_proxy:-${NO_PROXY:-}}"
  export HTTP_PROXY="${HTTP_PROXY:-${http_proxy:-}}"
  export HTTPS_PROXY="${HTTPS_PROXY:-${https_proxy:-}}"
  export NO_PROXY="${NO_PROXY:-${no_proxy:-}}"
  for proxy_env in HTTP_PROXY HTTPS_PROXY NO_PROXY http_proxy https_proxy no_proxy; do
    if [[ -n "${!proxy_env:-}" ]]; then
      docker_args+=(-e "$proxy_env")
    fi
  done

  docker "${docker_args[@]}" \
    --platform "$platform" \
    -e CONNTRACK_VERSION="$version" \
    -e KC_DOWNLOAD_RETRIES="${KC_DOWNLOAD_RETRIES:-3}" \
    -e KC_DOWNLOAD_CONNECT_TIMEOUT="${KC_DOWNLOAD_CONNECT_TIMEOUT:-20}" \
    -e KC_DOWNLOAD_MAX_TIME="${KC_DOWNLOAD_MAX_TIME:-900}" \
    -v "$dst_dir:/out" \
    "$image" \
    sh -euxc '
      if command -v apk >/dev/null 2>&1; then
        apk add --no-cache bash build-base autoconf automake libtool pkgconf bison flex linux-headers curl tar xz bzip2
      elif command -v apt-get >/dev/null 2>&1; then
        export DEBIAN_FRONTEND=noninteractive
        apt-get -o Acquire::Retries=3 update
        for attempt in 1 2 3; do
          if apt-get -o Acquire::Retries=3 install -y --fix-missing --no-install-recommends \
            bash build-essential autoconf automake libtool pkg-config bison flex \
            ca-certificates curl tar xz-utils bzip2; then
            break
          fi
          if [ "$attempt" = 3 ]; then
            exit 1
          fi
          apt-get -o Acquire::Retries=3 install -f -y --fix-missing || true
          sleep $((attempt * 5))
          apt-get -o Acquire::Retries=3 update
        done
        rm -rf /var/lib/apt/lists/*
      else
        echo "apk or apt-get is required in the builder image" >&2
        exit 1
      fi
      work="$(mktemp -d)"
      prefix="$work/prefix"
      mkdir -p "$prefix"
      export PKG_CONFIG_PATH="$prefix/lib/pkgconfig"
      export CPPFLAGS="-I$prefix/include"
      export LDFLAGS="-L$prefix/lib"

      download_src() {
        archive="$1"
        url="$2"
        curl -fL \
          --retry "$KC_DOWNLOAD_RETRIES" \
          --retry-delay 2 \
          --retry-all-errors \
          --connect-timeout "$KC_DOWNLOAD_CONNECT_TIMEOUT" \
          --max-time "$KC_DOWNLOAD_MAX_TIME" \
          "$url" -o "$work/$archive"
        tar -C "$work" -xf "$work/$archive"
      }

      build_autotools() {
        dir="$1"
        shift
        cd "$work/$dir"
        ./configure --prefix="$prefix" --enable-static --disable-shared "$@"
        make -j"$(getconf _NPROCESSORS_ONLN)"
        make install
      }

      base="https://www.netfilter.org/pub"
      download_src libmnl-1.0.5.tar.bz2 "$base/libmnl/libmnl-1.0.5.tar.bz2"
      build_autotools libmnl-1.0.5

      download_src libnfnetlink-1.0.2.tar.bz2 "$base/libnfnetlink/libnfnetlink-1.0.2.tar.bz2"
      build_autotools libnfnetlink-1.0.2

      download_src libnetfilter_conntrack-1.1.1.tar.xz "$base/libnetfilter_conntrack/libnetfilter_conntrack-1.1.1.tar.xz"
      build_autotools libnetfilter_conntrack-1.1.1

      download_src libnetfilter_queue-1.0.5.tar.bz2 "$base/libnetfilter_queue/libnetfilter_queue-1.0.5.tar.bz2"
      build_autotools libnetfilter_queue-1.0.5

      download_src libnetfilter_cthelper-1.0.1.tar.bz2 "$base/libnetfilter_cthelper/libnetfilter_cthelper-1.0.1.tar.bz2"
      build_autotools libnetfilter_cthelper-1.0.1

      download_src libnetfilter_cttimeout-1.0.1.tar.bz2 "$base/libnetfilter_cttimeout/libnetfilter_cttimeout-1.0.1.tar.bz2"
      build_autotools libnetfilter_cttimeout-1.0.1

      download_src "conntrack-tools-$CONNTRACK_VERSION.tar.xz" "$base/conntrack-tools/conntrack-tools-$CONNTRACK_VERSION.tar.xz"
      cd "$work/conntrack-tools-$CONNTRACK_VERSION"
      LDFLAGS="-static -L$prefix/lib" ./configure --prefix="$prefix" --enable-static --disable-shared
      make -j"$(getconf _NPROCESSORS_ONLN)" LDFLAGS="-static -L$prefix/lib"
      strip src/conntrack || true
      cp -f src/conntrack /out/conntrack
      chmod +x /out/conntrack
    '
  chown "$uid:$gid" "$dst_dir/conntrack" 2>/dev/null || true
  log "wrote $dst_dir/conntrack"
}

while IFS= read -r target_arch; do
  case "$method" in
  file)
    build_from_file "$target_arch"
    ;;
  url)
    [[ -n "$url" ]] || die "--url is required for --method url"
    build_from_url "$target_arch"
    ;;
  docker-source)
    build_from_source_with_docker "$target_arch"
    ;;
  *)
    die "unsupported method: $method"
    ;;
  esac
done < <(arch_list "$arch")
