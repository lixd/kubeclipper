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
image_archive=""
image_runtime=""
chart_file=""
chart_url=""
chart_repo=""
chart_name=""
chart_version=""
skip_images=false
skip_chart=false

usage() {
  cat <<'EOF'
Usage:
  build-addon-package.sh --name <component> [flags]

Components:
  kc-extension
  kubectl-terminal
  nvidia-dra-driver-gpu
  nvidia-gpu-operator

Flags:
  --name <component>         Component package name.
  --version <version>        Component version. Defaults depend on --name.
  --arch <amd64|arm64|all>   Target architecture. Default: amd64.
  --output <dir>             Resource output root. Default: ./resource.
  --images-file <file>       Image list file. Defaults to bundled lists when present.
  --image-archive <file>     Use an existing docker/podman save archive as images.tar.gz.
  --image-runtime <tool>     podman or docker. Default: auto-detect.
  --chart-file <file>        Use an existing chart archive as charts.tgz.
  --chart-url <url>          Download an existing chart archive as charts.tgz.
  --chart-repo <url>         Helm repo URL.
  --chart-name <name>        Helm chart name.
  --chart-version <version>  Helm chart version.
  --skip-images              Build without images.tar.gz.
  --skip-chart               Build without charts.tgz.
  -h, --help                 Show this help.

Output:
  <output>/<component>/<version>/<arch>/images.tar.gz
  <output>/<component>/<version>/<arch>/charts.tgz

Notes:
  If a component chart is not available from a stable public Helm repo, pass
  --chart-file or --chart-url. The script never falls back to private static
  content servers.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --name) need_value "$@"; name="$2"; shift 2 ;;
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
  --images-file) need_value "$@"; images_file="$2"; shift 2 ;;
  --image-archive) need_value "$@"; image_archive="$2"; shift 2 ;;
  --image-runtime) need_value "$@"; image_runtime="$2"; shift 2 ;;
  --chart-file) need_value "$@"; chart_file="$2"; shift 2 ;;
  --chart-url) need_value "$@"; chart_url="$2"; shift 2 ;;
  --chart-repo) need_value "$@"; chart_repo="$2"; shift 2 ;;
  --chart-name) need_value "$@"; chart_name="$2"; shift 2 ;;
  --chart-version) need_value "$@"; chart_version="$2"; shift 2 ;;
  --skip-images) skip_images=true; shift ;;
  --skip-chart) skip_chart=true; shift ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

validate_arch "$arch"
[[ -n "$name" ]] || die "--name is required"

default_component() {
  case "$name" in
  kc-extension)
    version="${version:-v1.0.0}"
    skip_chart=true
    ;;
  kubectl-terminal)
    version="${version:-v1.0.0}"
    skip_chart=true
    ;;
  nvidia-dra-driver-gpu)
    version="${version:-25.8.0}"
    chart_repo="${chart_repo:-https://helm.ngc.nvidia.com/nvidia}"
    chart_name="${chart_name:-nvidia-dra-driver-gpu}"
    chart_version="${chart_version:-$version}"
    ;;
  nvidia-gpu-operator)
    version="${version:-v25.10.0}"
    chart_repo="${chart_repo:-https://helm.ngc.nvidia.com/nvidia}"
    chart_name="${chart_name:-gpu-operator}"
    chart_version="${chart_version:-${version#v}}"
    ;;
  *)
    die "unsupported component: $name"
    ;;
  esac
}

default_component

default_images_file="$SCRIPT_DIR/images/$name-$version.txt"
if [[ -z "$images_file" && -f "$default_images_file" ]]; then
  images_file="$default_images_file"
fi

read_images() {
  [[ -n "$images_file" ]] || die "no bundled image list for $name $version; pass --images-file or --skip-images"
  [[ -f "$images_file" ]] || die "images file not found: $images_file"
  grep -vE '^[[:space:]]*(#|$)' "$images_file"
}

build_chart() {
  local dst=$1
  if [[ -n "$chart_file" || -n "$chart_url" ]]; then
    copy_or_download "$chart_file" "$chart_url" "$dst"
    return
  fi
  [[ -n "$chart_repo" && -n "$chart_name" && -n "$chart_version" ]] || die "chart input is required for $name; pass --chart-file, --chart-url, or Helm chart flags"
  need_cmd helm
  local work
  work="$(mktemp -d -t kc-addon-chart.XXXXXX)"
  run_with_retries "${KC_HELM_RETRIES:-3}" "${KC_HELM_RETRY_DELAY:-5}" \
    run_with_optional_timeout "${KC_HELM_TIMEOUT:-900}" \
    helm pull "$chart_name" --repo "$chart_repo" --version "$chart_version" --destination "$work"
  cp -f "$work/$chart_name-$chart_version.tgz" "$dst"
  rm -rf "$work"
}

build_one() {
  local target_arch=$1
  local package_dir tool images=()

  log "building $name $version for $target_arch"
  package_dir="$output/$name/$version/$target_arch"
  mkdir -p "$package_dir"
  rm -f "$package_dir/images.tar.gz" "$package_dir/charts.tgz" "$package_dir/manifest.json"

  if [[ "$skip_chart" != true ]]; then
    build_chart "$package_dir/charts.tgz"
  fi

  if [[ "$skip_images" == true ]]; then
    if [[ -n "$images_file" ]]; then
      read_images > "$package_dir/images.txt"
    fi
  else
    if [[ -n "$image_archive" ]]; then
      [[ -f "$image_archive" ]] || die "image archive not found: $image_archive"
      cp -f "$image_archive" "$package_dir/images.tar.gz"
    else
      tool="$(image_tool "$image_runtime")"
      read_images > "$package_dir/images.txt"
      while IFS= read -r image; do
        pull_image_for_arch "$tool" "$image" "$target_arch"
        images+=("$image")
      done < "$package_dir/images.txt"
      save_images "$tool" "$package_dir/images.tar.gz" "${images[@]}"
    fi
  fi

  generate_package_manifest "$package_dir"
  log "wrote $package_dir"
}

if [[ "$skip_images" != true ]]; then
  [[ -n "$images_file" || -n "$image_archive" ]] || die "no bundled image list for $name $version; pass --images-file, --image-archive, or --skip-images"
fi

while IFS= read -r target_arch; do
  build_one "$target_arch"
done < <(arch_list "$arch")
