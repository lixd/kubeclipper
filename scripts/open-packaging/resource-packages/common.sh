#!/usr/bin/env bash

set -euo pipefail

KC_RESOURCE_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$KC_RESOURCE_COMMON_DIR/../../.." && pwd)"

die() {
  echo "error: $*" >&2
  exit 1
}

need_value() {
  [[ $# -ge 2 && -n "$2" ]] || die "$1 requires a value"
}

init_resource_publish_workspace() {
  [[ -n "${resource_dir:-}" ]] || die "--resource-dir is required"
  [[ -d "$resource_dir" ]] || die "resource dir not found: $resource_dir"
  [[ -n "${registry:-}" ]] || die "--registry is required"
  [[ -n "${version:-}" ]] || die "--version is required"
  [[ "${arch:-}" == "amd64" || "${arch:-}" == "arm64" ]] || die "--arch must be amd64 or arm64"

  resource_dir_cleanup="${resource_dir_cleanup:-}"
  workdir="$(mktemp -d -t kc-resource-oci.XXXXXX)"
  trap 'rm -rf "$workdir" "$resource_dir_cleanup"' EXIT

  artifact_dir="${output_dir:-$workdir/artifacts}"
  mkdir -p "$artifact_dir"
}

resource_leaf() {
  local source_name=$1
  local leaf="$resource_dir/$source_name/$version/$arch"
  [[ -d "$leaf" ]] || die "resource package directory not found: $leaf"
  echo "$leaf"
}

make_resource_package() {
  local source_name=$1
  local source_path=$2
  local pkg="$artifact_dir/${source_name}-${version}-${arch}.tar.gz"
  local root="$workdir/pkg-$source_name-$version-$arch"

  rm -rf "$root"
  mkdir -p "$root/$source_name/$version/$arch"
  find "$source_path" -maxdepth 1 -type f ! -name images.tar.gz ! -name images.txt ! -name charts.tgz ! -name manifest.json -exec cp -f {} "$root/$source_name/$version/$arch/" \;
  tar -C "$root" -zcf "$pkg" "$source_name"
  echo "$pkg"
}

has_package_payload_files() {
  local source_name=${2:-}
  local source_path=$1
  find "$source_path" -maxdepth 1 -type f ! -name images.tar.gz ! -name images.txt ! -name charts.tgz ! -name manifest.json | grep -q .
}

publish_resource_package() {
  local source_name=$1
  local kind=$2
  local name=$3
  local profile=$4
  local source_path=$5
  shift 5

  local pkg=""
  if has_package_payload_files "$source_path" "$source_name"; then
    pkg="$(make_resource_package "$source_name" "$source_path")"
  fi
  if [[ -z "$pkg" && $# -eq 0 ]]; then
    die "$source_path has no package payload files and no external content"
  fi

  echo "publishing resource package image: source=$source_name kind=$kind name=$name version=$version arch=$arch"
  args=(
    --kind "$kind"
    --name "$name"
    --version "$version"
    --arch "$arch"
    --registry "$registry"
  )
  if [[ -n "$profile" ]]; then
    args+=(--profile "$profile")
  fi
  if [[ -n "$pkg" ]]; then
    args=(--package "$pkg" "${args[@]}")
  fi
  if [[ $# -gt 0 ]]; then
    args+=("$@")
  fi
  if [[ "${dry_run:-false}" == true ]]; then
    echo "dry-run: would publish ${pkg:-manifest-only package image} ${args[*]}"
    return 0
  fi
  "$ROOT/scripts/publish-oci-package.sh" "${args[@]}"
}

helm_chart_name() {
  local source_name=$1
  case "$source_name" in
  calico)
    echo "tigera-operator"
    ;;
  *)
    echo "$source_name"
    ;;
  esac
}

chart_digest() {
  local chart_archive=$1
  if command -v sha256sum >/dev/null 2>&1; then
    echo "sha256:$(sha256sum "$chart_archive" | awk '{print $1}')"
  else
    echo "sha256:$(shasum -a 256 "$chart_archive" | awk '{print $1}')"
  fi
}

push_chart() {
  local source_name=$1
  local chart_archive=$2
  local repository_prefix="${chart_repository_prefix:-}"
  local resolved_chart_name repository ref digest descriptor bin

  [[ -n "$repository_prefix" ]] || die "chart_repository_prefix must be set"
  resolved_chart_name="${chart_name:-$(helm_chart_name "$source_name")}"
  repository="$registry/$repository_prefix/$resolved_chart_name"
  ref="oci://$registry/$repository_prefix"
  echo "pushing helm chart: $chart_archive -> $ref" >&2
  if [[ "${dry_run:-false}" != true ]]; then
    bin="${helm_oci_publish_bin:-}"
    if [[ -z "$bin" && -x "$ROOT/bin/helm-oci-publish" ]]; then
      bin="$ROOT/bin/helm-oci-publish"
    fi
    if [[ -z "$bin" ]] && command -v helm-oci-publish >/dev/null 2>&1; then
      bin="$(command -v helm-oci-publish)"
    fi
    if [[ -n "$bin" ]]; then
      "$bin" --chart "$chart_archive" --registry "$registry" --repository-prefix "$repository_prefix" --name "$resolved_chart_name" >&2
    else
      command -v helm >/dev/null 2>&1 || die "helm or helm-oci-publish is required to push chart"
      if ! helm push "$chart_archive" "$ref" >&2; then
        if ! helm push "$chart_archive" "$ref" --plain-http >&2; then
          die "push helm chart failed: $chart_archive"
        fi
      fi
    fi
  else
    echo "dry-run: would publish helm chart $chart_archive to $ref with helm-oci-publish or helm push" >&2
  fi
  digest="$(chart_digest "$chart_archive")"
  descriptor="name=charts,file=$(basename "$chart_archive"),mediaType=application/vnd.cncf.helm.chart.content.v1.tar+gzip,transport=helm-oci,ref=$repository,digest=$digest"
  echo "$descriptor"
}
