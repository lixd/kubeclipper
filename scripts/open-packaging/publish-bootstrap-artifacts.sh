#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

registry=""
version=""
arch="$(go env GOARCH 2>/dev/null || echo amd64)"
bin_dir=""
console_dir=""
console_archive=""
output_dir=""
build_core=false
keep_workdir=false
dry_run=false

usage() {
  cat <<'EOF'
Usage:
  publish-bootstrap-artifacts.sh --registry <host:port> --version <version> [flags]

Flags:
  --registry <host:port>        Target OCI Registry.
  --version <version>           Package image version, e.g. v1.8.0.
  --arch <arch>                 Target architecture. Default: go env GOARCH.
  --bin-dir <dir>               Directory containing external bootstrap binaries.
  --console-dir <dir>           Directory containing kc-console files.
  --console-archive <file>      Prebuilt kc-console tar or tar.gz archive.
  --output-dir <dir>            Directory for generated package tarballs.
  --build-core                  Build kcctl, kubeclipper-server, kubeclipper-agent from source.
  --dry-run                     Validate inputs and create package tarballs without pushing.
  --keep-workdir                Keep temporary work directory for debugging.
  -h, --help                    Show this help.

Required bootstrap assets:
  kcctl, kubeclipper-server, kubeclipper-agent, caddy, registry, etcd,
  etcdctl, etcdutl, kc-console.

Examples:
  scripts/open-packaging/publish-bootstrap-artifacts.sh \
    --registry 10.0.0.10:5000 \
    --version v1.8.0 \
    --arch amd64 \
    --build-core \
    --bin-dir /data/kc-bootstrap-bin \
    --console-dir /data/kc-console
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

need_value() {
  [[ $# -ge 2 && -n "$2" ]] || die "$1 requires a value"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --registry)
    need_value "$@"
    registry="$2"
    shift 2
    ;;
  --version)
    need_value "$@"
    version="$2"
    shift 2
    ;;
  --arch)
    need_value "$@"
    arch="$2"
    shift 2
    ;;
  --bin-dir)
    need_value "$@"
    bin_dir="$2"
    shift 2
    ;;
  --console-dir)
    need_value "$@"
    console_dir="$2"
    shift 2
    ;;
  --console-archive)
    need_value "$@"
    console_archive="$2"
    shift 2
    ;;
  --output-dir)
    need_value "$@"
    output_dir="$2"
    shift 2
    ;;
  --build-core)
    build_core=true
    shift
    ;;
  --dry-run)
    dry_run=true
    shift
    ;;
  --keep-workdir)
    keep_workdir=true
    shift
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    die "unknown argument: $1"
    ;;
  esac
done

[[ -n "$registry" ]] || die "--registry is required"
[[ -n "$version" ]] || die "--version is required"
[[ "$arch" == "amd64" || "$arch" == "arm64" ]] || die "--arch must be amd64 or arm64"

workdir="$(mktemp -d -t kc-bootstrap-oci.XXXXXX)"
if [[ "$keep_workdir" != true ]]; then
  trap 'rm -rf "$workdir"' EXIT
else
  echo "keeping workdir: $workdir"
fi

artifact_dir="${output_dir:-$workdir/artifacts}"
build_dir="$workdir/bin"
mkdir -p "$artifact_dir" "$build_dir"

find_asset() {
  local name=$1
  if [[ -x "$build_dir/$name" || -f "$build_dir/$name" ]]; then
    echo "$build_dir/$name"
    return 0
  fi
  if [[ -n "$bin_dir" && -f "$bin_dir/$name" ]]; then
    echo "$bin_dir/$name"
    return 0
  fi
  if command -v "$name" >/dev/null 2>&1; then
    command -v "$name"
    return 0
  fi
  return 1
}

build_core_binaries() {
  echo "building core binaries for linux/$arch"
  (
    cd "$ROOT"
    GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o "$build_dir/kcctl" ./cmd/kcctl
    GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o "$build_dir/kubeclipper-server" ./cmd/kubeclipper-server
    GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o "$build_dir/kubeclipper-agent" ./cmd/kubeclipper-agent
  )
}

make_single_file_package() {
  local name=$1
  local source=$2
  local filename=$3
  local pkg="$artifact_dir/${name}-${version}-${arch}.tar.gz"
  local root="$workdir/pkg-$name"

  rm -rf "$root"
  mkdir -p "$root/$name/$version/$arch"
  cp "$source" "$root/$name/$version/$arch/$filename"
  chmod 0755 "$root/$name/$version/$arch/$filename" 2>/dev/null || true
  tar -C "$root" -zcf "$pkg" "$name"
  echo "$pkg"
}

publish_binary_package() {
  local name=$1
  local source=$2
  local filename=$3
  local pkg
  pkg="$(make_single_file_package "$name" "$source" "$filename")"
  echo "publishing bootstrap package image: $name"
  if [[ "$dry_run" == true ]]; then
    echo "dry-run: would publish $pkg"
    return 0
  fi
  "$ROOT/scripts/publish-oci-package.sh" \
    --package "$pkg" \
    --kind binary \
    --name "$name" \
    --version "$version" \
    --arch "$arch" \
    --registry "$registry" \
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

  if [[ -n "$console_archive" ]]; then
    [[ -f "$console_archive" ]] || die "console archive not found: $console_archive"
    normalize_console_archive "$console_archive"
    return 0
  fi
  if [[ -n "$console_dir" ]]; then
    [[ -d "$console_dir" ]] || die "console dir not found: $console_dir"
    local normalize_dir="$workdir/kc-console-dir"
    local archive="$workdir/kc-console.tar"
    rm -rf "$normalize_dir"
    mkdir -p "$normalize_dir/kc-console"
    cp -R "$console_dir"/. "$normalize_dir/kc-console/"
    tar -C "$normalize_dir" -cf "$archive" kc-console
    echo "$archive"
    return 0
  fi
  if [[ -n "$bin_dir" && -f "$bin_dir/kc-console.tar.gz" ]]; then
    normalize_console_archive "$bin_dir/kc-console.tar.gz"
    return 0
  fi
  if [[ -n "$bin_dir" && -f "$bin_dir/kc-console.tar" ]]; then
    normalize_console_archive "$bin_dir/kc-console.tar"
    return 0
  fi
  die "kc-console is required; pass --console-dir or --console-archive"
}

if [[ "$build_core" == true ]]; then
  build_core_binaries
fi

required_binaries=(
  kcctl
  kubeclipper-server
  kubeclipper-agent
  caddy
  registry
  etcd
  etcdctl
  etcdutl
)

for name in "${required_binaries[@]}"; do
  path="$(find_asset "$name")" || die "missing bootstrap binary $name; pass --bin-dir or enable --build-core for KubeClipper binaries"
  publish_binary_package "$name" "$path" "$name"
done

console_pkg="$(prepare_console_archive)"
publish_binary_package "kc-console" "$console_pkg" "kc-console.tar.gz"

if [[ "$dry_run" == true ]]; then
  echo "dry-run complete; generated package tarballs are in $artifact_dir"
else
  echo "bootstrap package image publishing complete"
fi
