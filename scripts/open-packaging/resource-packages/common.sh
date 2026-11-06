#!/usr/bin/env bash

set -euo pipefail

KC_RESOURCE_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$KC_RESOURCE_COMMON_DIR/../../.." && pwd)"

# Mirror the bootstrap packaging flow: publish from a git checkout records
# the checked-out commit as the package source revision unless the caller
# pinned one explicitly.
if [[ -z "${KC_SOURCE_REVISION:-}" ]]; then
  if ! command -v git >/dev/null 2>&1 || ! KC_SOURCE_REVISION="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null)"; then
    die "KC_SOURCE_REVISION is required when publishing outside a Git checkout"
  fi
  export KC_SOURCE_REVISION
fi

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
  find "$source_path" -maxdepth 1 -type f ! -name images.tar.gz ! -name images.txt ! -name manifest.json -exec cp -f {} "$root/$source_name/$version/$arch/" \;
  tar -C "$root" -zcf "$pkg" "$source_name"
  echo "$pkg"
}

has_package_payload_files() {
  local source_name=${2:-}
  local source_path=$1
  find "$source_path" -maxdepth 1 -type f ! -name images.tar.gz ! -name images.txt ! -name manifest.json | grep -q .
}

publish_resource_package() {
  local source_name=$1
  local kind=$2
  local name=$3
  local profile=$4
  local source_path=$5
  shift 5

  if [[ -n "${KC_PUBLISHED_PACKAGE_DIGEST:-}" ]]; then
    local target="$registry/package-$name:$version"
    local digest
    digest="$(crane digest "$target")"
    [[ "$digest" == "$KC_PUBLISHED_PACKAGE_DIGEST" ]] || die "published package digest mismatch: $target"
    echo "reusing verified resource package: $target@$digest"
    return
  fi

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
