#!/usr/bin/env bash

set -euo pipefail

# Fixed-version dependencies retain their original package and provenance
# across KC candidates. Resource builders still prepare runtime image lists.
[[ $# == 4 ]] || { echo "usage: $0 <component> <canonical-prefix> <target-prefix> <version>" >&2; exit 2; }
component="$1"
canonical_prefix="${2%/}"
target_prefix="${3%/}"
version="$4"
unset KC_PUBLISHED_PACKAGE_DIGEST
case "$component" in
  bootstrap-etcd|bootstrap-console|bootstrap-registry) kind=bootstrap; name="${component#bootstrap-}"; repository="package-$component" ;;
  resource-k8s) kind=k8s; name=k8s; repository=package-k8s ;;
  resource-containerd) kind=cri; name=containerd; repository=package-containerd ;;
  resource-k8s-extension) kind=k8s-extension; name=k8s-extension; repository=package-k8s-extension ;;
  resource-calico) kind=cni; name=calico; repository=package-calico ;;
  *) echo "unsupported versioned package component: $component" >&2; exit 2 ;;
esac
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
canonical="$canonical_prefix/$repository:$version"
target="$target_prefix/$repository:$version"
workdir="$(mktemp -d -t kc-component-release.XXXXXX)"
trap 'rm -rf "$workdir"' EXIT

die() { echo "error: $*" >&2; exit 1; }

digest_if_present() {
  local ref=$1 digest
  if digest="$(crane digest "$ref" 2>"$workdir/digest.err")"; then
    printf '%s\n' "$digest"
  elif grep -Eqi 'MANIFEST[_ ]UNKNOWN|NAME[_ ]UNKNOWN|404[[:space:]]+Not Found|status( code)?:?[[:space:]]*404|unexpected status.*404' "$workdir/digest.err"; then
    return 0
  else
    die "cannot inspect versioned package $ref: $(cat "$workdir/digest.err")"
  fi
}

verify_package() {
  local ref=$1 selected_arch=${2:-} expected_revision=${3:-} arch
  local architectures="${selected_arch:-amd64 arm64}"
  crane manifest "$ref" > "$workdir/index.json" || return $?
  python3 - "$workdir/index.json" "$selected_arch" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    index = json.load(stream)
platforms = [(item.get("platform", {}).get("os"), item.get("platform", {}).get("architecture"))
             for item in index.get("manifests", [])]
allowed = {("linux", "amd64"), ("linux", "arm64")}
if any(platform not in allowed for platform in platforms) or len(set(platforms)) != len(platforms):
    raise SystemExit("versioned package has unsupported or duplicate platforms")
if sys.argv[2]:
    if ("linux", sys.argv[2]) not in platforms:
        raise SystemExit(3)
elif set(platforms) != allowed:
    raise SystemExit("versioned package must contain exactly linux/amd64 and linux/arm64")
PY
  local status=$?
  [[ "$status" == 0 ]] || return "$status"
  for arch in $architectures; do
    crane config --platform "linux/$arch" "$ref" > "$workdir/$arch.json" || return $?
  done
  python3 - "$workdir" "$version" "${KC_SOURCE_URL:?KC_SOURCE_URL is required}" "$kind" "$name" "$architectures" "$expected_revision" <<'PY'
import json, re, sys
directory, version, source, kind, name, architectures, expected_revision = sys.argv[1:]
revisions = set()
for arch in architectures.split():
    with open(f"{directory}/{arch}.json", encoding="utf-8") as stream:
        labels = (json.load(stream).get("config") or {}).get("Labels") or {}
    expected = {"title": f"{kind}/{name}", "version": version, "source": source}
    for key, value in expected.items():
        if labels.get("org.opencontainers.image." + key) != value:
            raise SystemExit(f"versioned package {arch} has unexpected {key}")
    revision = labels.get("org.opencontainers.image.revision", "")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise SystemExit(f"versioned package {arch} has no full source revision")
    if expected_revision and revision != expected_revision:
        raise SystemExit(f"candidate package {arch} source revision mismatch")
    revisions.add(revision)
if len(revisions) != 1:
    raise SystemExit("versioned package has mixed platform source revisions")
PY
}

copy_package() {
  local source=$1 destination=$2 expected=$3 existing
  existing="$(digest_if_present "$destination")"
  if [[ -n "$existing" ]]; then
    [[ "$existing" == "$expected" ]] || die "versioned package tag conflict: $destination already points to $existing, refusing $expected"
    echo "versioned package already matches, skip: $destination"
    return
  fi
  # No platform selection: retain the complete original index and its digest.
  crane copy "$source" "$destination"
  [[ "$(crane digest "$destination")" == "$expected" ]] || die "versioned package digest changed after copy: $destination"
}

digest="$(digest_if_present "$canonical")"
build_prefix="$target_prefix"
if [[ -n "$digest" ]]; then
  echo "reusing published package: $canonical@$digest"
  verify_package "${canonical%:*}@$digest"
  copy_package "${canonical%:*}@$digest" "$target" "$digest"
  if [[ "$kind" == bootstrap ]]; then
    exit 0
  fi
  export KC_PUBLISHED_PACKAGE_DIGEST="$digest"
else
  # Build both architectures under a candidate reference first. Only a verified
  # complete index is promoted to the shared immutable version tag.
  if [[ "$build_prefix" == "$canonical_prefix" ]]; then
    build_prefix="$canonical_prefix/qualification-${KC_SOURCE_REVISION:?KC_SOURCE_REVISION is required}"
  fi
  if [[ -n "${KC_PACKAGE_REGISTRY_CONFIG:-}" ]]; then
    (umask 077; jq -e --arg registry "$build_prefix" --arg target "$target_prefix" \
      'if .registry == $target then .registry = $registry else error("package Registry config does not match target") end' \
      "$KC_PACKAGE_REGISTRY_CONFIG" > "$workdir/registry.json")
    export KC_PACKAGE_REGISTRY_CONFIG="$workdir/registry.json"
  fi
fi

build_ref="$build_prefix/$repository:$version"
for arch in amd64 arm64; do
  if [[ -z "$digest" ]]; then
    unset KC_PUBLISHED_PACKAGE_DIGEST
    staged_digest="$(digest_if_present "$build_ref")"
    if [[ -n "$staged_digest" ]]; then
      if verify_package "${build_ref%:*}@$staged_digest" "$arch" "${KC_SOURCE_REVISION:?KC_SOURCE_REVISION is required}"; then
        if [[ "$kind" == bootstrap ]]; then
          echo "reusing published candidate platform: $build_ref linux/$arch"
          continue
        fi
        export KC_PUBLISHED_PACKAGE_DIGEST="$staged_digest"
      else
        status=$?
        [[ "$status" == 3 ]] || exit "$status"
      fi
    fi
  fi
  args=(--registry-prefix "$build_prefix" --version "$version" --arch "$arch")
  case "$component" in
    resource-k8s|resource-k8s-extension|resource-calico) args+=(--image-registry-prefix "$target_prefix") ;;
  esac
  "$SCRIPT_DIR/publish-$component.sh" "${args[@]}"
done

if [[ -z "$digest" ]]; then
  digest="$(crane digest "$build_ref")"
  pinned="${build_ref%:*}@$digest"
  verify_package "$pinned"
  copy_package "$pinned" "$canonical" "$digest"
fi

copy_package "${canonical%:*}@$digest" "$target" "$digest"
