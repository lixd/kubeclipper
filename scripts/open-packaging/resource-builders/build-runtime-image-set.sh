#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

name=""
version=""
arch="amd64"
output="./resource"
images_file=""

usage() {
  cat <<'EOF'
Usage:
  build-runtime-image-set.sh --name <name> --version <version> [flags]

Flags:
  --name <name>             Runtime image set name.
  --version <version>       Runtime image set version.
  --arch <amd64|arm64|all>  Target architecture. Default: amd64.
  --output <dir>            Resource output root. Default: ./resource.
  --images-file <file>      Override the repository image list.
  -h, --help                Show this help.

Output:
  <output>/<name>/<version>/<arch>/images.txt

This builder records standard OCI runtime images only. It does not create a
KubeClipper package image or embed image archives.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --name) need_value "$@"; name="$2"; shift 2 ;;
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
  --images-file) need_value "$@"; images_file="$2"; shift 2 ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

[[ -n "$name" ]] || die "--name is required"
[[ "$name" =~ ^[a-z0-9][a-z0-9.-]*$ ]] || die "invalid --name: $name"
[[ -n "$version" ]] || die "--version is required"
validate_arch "$arch"

if [[ -z "$images_file" ]]; then
  images_file="$SCRIPT_DIR/images/$name-$version.txt"
fi
[[ -f "$images_file" ]] || die "images file not found: $images_file"

build_one() {
  local target_arch=$1
  local package_dir="$output/$name/$version/$target_arch"

  log "building runtime image set $name $version for $target_arch"
  mkdir -p "$package_dir"
  rm -f "$package_dir/images.txt" "$package_dir/images.tar.gz" "$package_dir/manifest.json"
  grep -vE '^[[:space:]]*(#|$)' "$images_file" > "$package_dir/images.txt"
  [[ -s "$package_dir/images.txt" ]] || die "image list is empty: $images_file"
  generate_package_manifest "$package_dir"
  log "wrote $package_dir"
}

while IFS= read -r target_arch; do
  build_one "$target_arch"
done < <(arch_list "$arch")
