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
chart_file=""
chart_repo=""
chart_name=""
chart_version=""
skip_chart=false

usage() {
  cat <<'EOF'
Usage:
  build-addon-package.sh --name <component> [flags]

Components:
  nvidia-dra-driver-gpu
  nvidia-gpu-operator

Flags:
  --name <component>         Component package name.
  --version <version>        Component version. Defaults depend on --name.
  --arch <amd64|arm64|all>   Target architecture. Default: amd64.
  --output <dir>             Resource output root. Default: ./resource.
  --images-file <file>       Image list file. Defaults to bundled lists when present.
  --chart-file <file>        Use an existing chart archive as charts.tgz.
  --chart-name <name>        Helm chart name.
  --chart-version <version>  Helm chart version.
  --skip-chart               Build without charts.tgz.
  -h, --help                 Show this help.

Output:
  <output>/<component>/<version>/<arch>/images.txt
  <output>/<component>/<version>/<arch>/charts.tgz

Notes:
  If a component chart is not available from a stable public Helm repo, pass
  --chart-file. Runtime images are not embedded in this package; images.txt is
  consumed by push-runtime-images.sh. The script never falls back to private
  static content servers.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --name) need_value "$@"; name="$2"; shift 2 ;;
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
  --images-file) need_value "$@"; images_file="$2"; shift 2 ;;
  --chart-file) need_value "$@"; chart_file="$2"; shift 2 ;;
  --chart-name) need_value "$@"; chart_name="$2"; shift 2 ;;
  --chart-version) need_value "$@"; chart_version="$2"; shift 2 ;;
  --skip-chart) skip_chart=true; shift ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

validate_arch "$arch"
[[ -n "$name" ]] || die "--name is required"

default_component() {
  case "$name" in
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
  [[ -n "$images_file" ]] || die "no bundled image list for $name $version; pass --images-file"
  [[ -f "$images_file" ]] || die "images file not found: $images_file"
  grep -vE '^[[:space:]]*(#|$)' "$images_file"
}

build_chart() {
  local dst=$1
  if [[ -n "$chart_file" ]]; then
    cp -f "$chart_file" "$dst"
    return
  fi
  [[ -n "$chart_repo" && -n "$chart_name" && -n "$chart_version" ]] || die "chart input is required for $name; pass --chart-file or Helm chart flags"
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
  local package_dir

  log "building $name $version for $target_arch"
  package_dir="$output/$name/$version/$target_arch"
  mkdir -p "$package_dir"
  rm -f "$package_dir/images.txt" "$package_dir/images.tar.gz" "$package_dir/charts.tgz" "$package_dir/manifest.json"

  if [[ "$skip_chart" != true ]]; then
    build_chart "$package_dir/charts.tgz"
  fi

  if [[ -n "$images_file" ]]; then
    read_images > "$package_dir/images.txt"
  fi

  generate_package_manifest "$package_dir"
  log "wrote $package_dir"
}

while IFS= read -r target_arch; do
  build_one "$target_arch"
done < <(arch_list "$arch")
