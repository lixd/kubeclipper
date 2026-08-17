#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

arch="amd64"
version="v3.31.5"
chart_version=""
output="./resource"
images_file=""
image_runtime=""
chart_file=""
chart_url=""
helm_repo="https://docs.tigera.io/calico/charts"
skip_images=false

usage() {
  cat <<'EOF'
Usage:
  build-calico-package.sh [flags]

Flags:
  --version <vX.Y.Z>          Calico version. Default: v3.31.5.
  --chart-version <version>   Tigera operator chart version. Default: Calico version.
  --arch <amd64|arm64|all>    Target architecture. Default: amd64.
  --output <dir>              Resource output root. Default: ./resource.
  --images-file <file>        Image list file. Defaults to images/calico-<version>.txt when present.
  --image-runtime <tool>      podman or docker. Default: auto-detect.
  --chart-file <file>         Use an existing tigera-operator chart archive.
  --chart-url <url>           Download an existing tigera-operator chart archive.
  --helm-repo <url>           Helm repo for tigera-operator. Default: https://docs.tigera.io/calico/charts.
  --skip-images               Build charts.tgz only.
  -h, --help                  Show this help.

Output:
  <output>/calico/<version>/<arch>/charts.tgz
  <output>/calico/<version>/<arch>/images.tar.gz
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --chart-version) need_value "$@"; chart_version="$2"; shift 2 ;;
  --operator-version) need_value "$@"; chart_version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
  --images-file) need_value "$@"; images_file="$2"; shift 2 ;;
  --image-runtime) need_value "$@"; image_runtime="$2"; shift 2 ;;
  --chart-file) need_value "$@"; chart_file="$2"; shift 2 ;;
  --chart-url) need_value "$@"; chart_url="$2"; shift 2 ;;
  --helm-repo) need_value "$@"; helm_repo="$2"; shift 2 ;;
  --skip-images) skip_images=true; shift ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

validate_arch "$arch"
[[ "$version" == v* ]] || version="v$version"
if [[ -z "$chart_version" ]]; then
  chart_version="$version"
fi

default_images_file="$SCRIPT_DIR/images/calico-$version.txt"
if [[ -z "$images_file" && -f "$default_images_file" ]]; then
  images_file="$default_images_file"
fi

build_chart() {
  local dst=$1

  if [[ -n "$chart_file" || -n "$chart_url" ]]; then
    copy_or_download "$chart_file" "$chart_url" "$dst"
    return
  fi
  need_cmd helm
  local work
  work="$(mktemp -d -t kc-calico-chart.XXXXXX)"
  run_with_retries "${KC_HELM_RETRIES:-3}" "${KC_HELM_RETRY_DELAY:-5}" \
    run_with_optional_timeout "${KC_HELM_TIMEOUT:-900}" \
    helm pull tigera-operator --repo "$helm_repo" --version "$chart_version" --destination "$work"
  cp -f "$work/tigera-operator-$chart_version.tgz" "$dst"
  rm -rf "$work"
}

read_images() {
  [[ -n "$images_file" ]] || die "--images-file is required unless --skip-images is set"
  [[ -f "$images_file" ]] || die "images file not found: $images_file"
  grep -vE '^[[:space:]]*(#|$)' "$images_file"
}

build_one() {
  local target_arch=$1
  local package_dir tool images=()

  log "building calico $version for $target_arch"
  package_dir="$output/calico/$version/$target_arch"
  mkdir -p "$package_dir"
  rm -f "$package_dir/charts.tgz" "$package_dir/images.tar.gz" "$package_dir/manifest.json"

  build_chart "$package_dir/charts.tgz"

  if [[ "$skip_images" == true ]]; then
    read_images > "$package_dir/images.txt"
  else
    tool="$(image_tool "$image_runtime")"
    read_images > "$package_dir/images.txt"
    while IFS= read -r image; do
      pull_image_for_arch "$tool" "$image" "$target_arch"
      images+=("$image")
    done < "$package_dir/images.txt"
    save_images "$tool" "$package_dir/images.tar.gz" "${images[@]}"
  fi

  generate_package_manifest "$package_dir"
  log "wrote $package_dir"
}

if [[ "$skip_images" != true ]]; then
  [[ -n "$images_file" ]] || die "no bundled image list for $version; pass --images-file or --skip-images"
fi

while IFS= read -r target_arch; do
  build_one "$target_arch"
done < <(arch_list "$arch")
