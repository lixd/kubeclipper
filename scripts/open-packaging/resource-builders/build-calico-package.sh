#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

arch="amd64"
version="v3.31.5"
chart_version=""
chart_name="tigera-operator"
output="./resource"
images_file=""
chart_file=""
helm_repo="https://docs.tigera.io/calico/charts"
helm_version="3.18.6"

usage() {
  cat <<'EOF'
Usage:
  build-calico-package.sh [flags]

Flags:
  --version <vX.Y.Z>          Calico version. Default: v3.31.5.
  --chart-name <name>         Helm chart name. Default: tigera-operator.
  --chart-version <version>   Tigera operator chart version. Default: Calico version.
  --arch <amd64|arm64|all>    Target architecture. Default: amd64.
  --output <dir>              Resource output root. Default: ./resource.
  --images-file <file>        Image list file. Defaults to images/calico-<version>.txt when present.
  --chart-file <file>         Use an existing tigera-operator chart archive.
  -h, --help                  Show this help.

Output:
  <output>/calico/<version>/<arch>/charts.tgz
  <output>/calico/<version>/<arch>/images.txt

Notes:
  Runtime images are not embedded in this package; images.txt is consumed by
  push-runtime-images.sh.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --version) need_value "$@"; version="$2"; shift 2 ;;
  --chart-name) need_value "$@"; chart_name="$2"; shift 2 ;;
  --chart-version) need_value "$@"; chart_version="$2"; shift 2 ;;
  --operator-version) need_value "$@"; chart_version="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
  --images-file) need_value "$@"; images_file="$2"; shift 2 ;;
  --chart-file) need_value "$@"; chart_file="$2"; shift 2 ;;
  --helm-version) need_value "$@"; helm_version="$2"; shift 2 ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

validate_arch "$arch"
[[ "$version" == v* ]] || version="v$version"
if [[ -z "$chart_version" ]]; then
  chart_version="$version"
fi
chart_version="${chart_version#v}"

default_images_file="$SCRIPT_DIR/images/calico-$version.txt"
if [[ -z "$images_file" && -f "$default_images_file" ]]; then
  images_file="$default_images_file"
fi

build_chart() {
  local dst=$1

  if [[ -n "$chart_file" ]]; then
    cp -f "$chart_file" "$dst"
    return
  fi
  local work helm_bin
  work="$(mktemp -d -t kc-calico-chart.XXXXXX)"
  if command -v helm >/dev/null 2>&1; then
    helm_bin="$(command -v helm)"
  else
    download "https://get.helm.sh/helm-v$helm_version-linux-$(host_arch).tar.gz" "$work/helm.tar.gz"
    tar -xzf "$work/helm.tar.gz" -C "$work"
    helm_bin="$work/linux-$(host_arch)/helm"
  fi
  run_with_retries "${KC_HELM_RETRIES:-3}" "${KC_HELM_RETRY_DELAY:-5}" \
    run_with_optional_timeout "${KC_HELM_TIMEOUT:-900}" \
    "$helm_bin" pull "$chart_name" --repo "$helm_repo" --version "$chart_version" --destination "$work"
  local pulled_chart=""
  for candidate in "$work/$chart_name-$chart_version.tgz" "$work/$chart_name-v$chart_version.tgz"; do
    if [[ -f "$candidate" ]]; then
      pulled_chart="$candidate"
      break
    fi
  done
  if [[ -z "$pulled_chart" ]]; then
    pulled_chart="$(find "$work" -maxdepth 1 -type f -name "$chart_name-*.tgz" | sort | head -n 1)"
  fi
  [[ -n "$pulled_chart" ]] || die "helm pull did not produce $chart_name chart archive in $work"
  cp -f "$pulled_chart" "$dst"
  rm -rf "$work"
}

read_images() {
  [[ -n "$images_file" ]] || die "--images-file is required"
  [[ -f "$images_file" ]] || die "images file not found: $images_file"
  grep -vE '^[[:space:]]*(#|$)' "$images_file"
}

build_one() {
  local target_arch=$1
  local package_dir

  log "building calico $version for $target_arch"
  package_dir="$output/calico/$version/$target_arch"
  mkdir -p "$package_dir"
  rm -f "$package_dir/charts.tgz" "$package_dir/images.txt" "$package_dir/images.tar.gz" "$package_dir/manifest.json"

  build_chart "$package_dir/charts.tgz"
  read_images > "$package_dir/images.txt"

  generate_package_manifest "$package_dir"
  log "wrote $package_dir"
}

[[ -n "$images_file" ]] || die "no bundled image list for $version; pass --images-file"

while IFS= read -r target_arch; do
  build_one "$target_arch"
done < <(arch_list "$arch")
