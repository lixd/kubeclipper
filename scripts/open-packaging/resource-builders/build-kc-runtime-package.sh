#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

arch="amd64"
version="v2.0.0"
output="./resource"
images_file=""

usage() {
  cat <<'EOF'
Usage:
  build-kc-runtime-package.sh [flags]

Flags:
  --version <version>       Package version. Default: v2.0.0.
  --arch <amd64|arm64|all>  Target architecture. Default: amd64.
  --output <dir>            Resource output root. Default: ./resource.
  --images-file <file>      Override the bundled image list.
  -h, --help                Show this help.

Output:
  <output>/kc-runtime/<version>/<arch>/images.txt

Notes:
  This package keeps KubeClipper runtime helper images separate from native
  Kubernetes images. The default OCI flow mirrors these entries as normal
  registry images from images.lock; it does not embed images.tar.gz.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
  --images-file) need_value "$@"; images_file="$2"; shift 2 ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

validate_arch "$arch"
[[ "$version" == v* ]] || version="v$version"

default_images_file="$SCRIPT_DIR/images/kc-runtime-$version.txt"
if [[ -z "$images_file" && -f "$default_images_file" ]]; then
  images_file="$default_images_file"
fi
if [[ -z "$images_file" ]]; then
  images_file="$SCRIPT_DIR/images/kc-runtime-default.txt"
fi

read_images() {
  [[ -f "$images_file" ]] || die "images file not found: $images_file"
  grep -vE '^[[:space:]]*(#|$)' "$images_file"
}

build_one() {
  local target_arch=$1
  local package_dir

  log "building kc-runtime $version for $target_arch"
  package_dir="$output/kc-runtime/$version/$target_arch"
  mkdir -p "$package_dir"
  rm -f "$package_dir/images.txt" "$package_dir/images.tar.gz" "$package_dir/manifest.json"

  read_images > "$package_dir/images.txt"

  generate_package_manifest "$package_dir"
  log "wrote $package_dir"
}

while IFS= read -r target_arch; do
  build_one "$target_arch"
done < <(arch_list "$arch")
