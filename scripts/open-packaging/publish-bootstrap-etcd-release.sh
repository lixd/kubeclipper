#!/usr/bin/env bash

set -euo pipefail

# Release candidates share the original versioned etcd package. KC's current
# source revision must never cause this dependency to be rebuilt under its tag.
[[ $# == 3 ]] || { echo "usage: $0 <canonical-prefix> <target-prefix> <version>" >&2; exit 2; }
canonical_prefix="${1%/}"
target_prefix="${2%/}"
version="$3"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
canonical="$canonical_prefix/package-bootstrap-etcd:$version"
target="$target_prefix/package-bootstrap-etcd:$version"
workdir="$(mktemp -d -t kc-etcd-release.XXXXXX)"
trap 'rm -rf "$workdir"' EXIT

die() { echo "error: $*" >&2; exit 1; }

digest_if_present() {
  local ref=$1 digest
  if digest="$(crane digest "$ref" 2>"$workdir/digest.err")"; then
    printf '%s\n' "$digest"
  elif grep -Eqi 'MANIFEST[_ ]UNKNOWN|NAME[_ ]UNKNOWN|404[[:space:]]+Not Found|status( code)?:?[[:space:]]*404|unexpected status.*404' "$workdir/digest.err"; then
    return 0
  else
    die "cannot inspect etcd package $ref: $(cat "$workdir/digest.err")"
  fi
}

verify_package() {
  local ref=$1 arch
  crane manifest "$ref" > "$workdir/index.json"
  python3 - "$workdir/index.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    index = json.load(stream)
platforms = [(item.get("platform", {}).get("os"), item.get("platform", {}).get("architecture"))
             for item in index.get("manifests", [])]
if sorted(platforms) != [("linux", "amd64"), ("linux", "arm64")]:
    raise SystemExit("etcd package must contain exactly linux/amd64 and linux/arm64")
PY
  for arch in amd64 arm64; do
    crane config --platform "linux/$arch" "$ref" > "$workdir/$arch.json"
  done
  python3 - "$workdir" "$version" "${KC_SOURCE_URL:?KC_SOURCE_URL is required}" <<'PY'
import json, re, sys
directory, version, source = sys.argv[1:]
revisions = set()
for arch in ("amd64", "arm64"):
    with open(f"{directory}/{arch}.json", encoding="utf-8") as stream:
        labels = (json.load(stream).get("config") or {}).get("Labels") or {}
    expected = {"title": "bootstrap/etcd", "version": version, "source": source}
    for key, value in expected.items():
        if labels.get("org.opencontainers.image." + key) != value:
            raise SystemExit(f"etcd package {arch} has unexpected {key}")
    revision = labels.get("org.opencontainers.image.revision", "")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise SystemExit(f"etcd package {arch} has no full source revision")
    revisions.add(revision)
if len(revisions) != 1:
    raise SystemExit("etcd package has mixed platform source revisions")
PY
}

copy_package() {
  local source=$1 destination=$2 expected=$3 existing
  existing="$(digest_if_present "$destination")"
  if [[ -n "$existing" ]]; then
    [[ "$existing" == "$expected" ]] || die "etcd package tag conflict: $destination already points to $existing, refusing $expected"
    echo "etcd package already matches, skip: $destination"
    return
  fi
  # No platform selection: retain the complete original index and its digest.
  crane copy "$source" "$destination"
  [[ "$(crane digest "$destination")" == "$expected" ]] || die "etcd package digest changed after copy: $destination"
}

digest="$(digest_if_present "$canonical")"
if [[ -z "$digest" ]]; then
  # Build both architectures under a candidate reference first. Only a verified
  # complete index is promoted to the shared immutable version tag.
  build_prefix="$target_prefix"
  if [[ "$build_prefix" == "$canonical_prefix" ]]; then
    build_prefix="$canonical_prefix/qualification-${KC_SOURCE_REVISION:?KC_SOURCE_REVISION is required}"
  fi
  build_ref="$build_prefix/package-bootstrap-etcd:$version"
  if [[ -n "${KC_PACKAGE_REGISTRY_CONFIG:-}" ]]; then
    (umask 077; jq --arg registry "$build_prefix" '.registry = $registry' "$KC_PACKAGE_REGISTRY_CONFIG" > "$workdir/registry.json")
    export KC_PACKAGE_REGISTRY_CONFIG="$workdir/registry.json"
  fi
  for arch in amd64 arm64; do
    "$SCRIPT_DIR/publish-bootstrap-etcd.sh" --registry-prefix "$build_prefix" --version "$version" --arch "$arch"
  done
  digest="$(crane digest "$build_ref")"
  pinned="${build_ref%:*}@$digest"
  verify_package "$pinned"
  copy_package "$pinned" "$canonical" "$digest"
else
  echo "reusing published etcd package: $canonical@$digest"
  verify_package "${canonical%:*}@$digest"
fi

copy_package "${canonical%:*}@$digest" "$target" "$digest"
