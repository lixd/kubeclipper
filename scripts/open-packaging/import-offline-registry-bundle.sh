#!/usr/bin/env bash

set -euo pipefail

bundle=""
registry=""
manifest_output=""
insecure_destination=false

usage() {
  cat <<'EOF'
Usage:
  import-offline-registry-bundle.sh --bundle <file> --registry <prefix> [flags]

Verifies and imports a KubeClipper offline Registry bundle into any
OCI-compatible Registry or Harbor instance. Repository paths and tags come
from the release manifest used to create the bundle.

Flags:
  --bundle <file>          Offline Registry bundle tar.gz.
  --registry <prefix>      Destination Registry prefix.
  --manifest-output <file> Write the bundled release manifest to this path.
  --insecure-destination   Allow plain HTTP / untrusted TLS for destination.
  -h, --help               Show this help.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

need_value() {
  [[ $# -ge 2 && -n "$2" ]] || die "$1 requires a value"
}

verify_checksums() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -c SHA256SUMS
  else
    while read -r expected file; do
      actual="$(shasum -a 256 "$file" | awk '{print $1}')"
      [[ "$actual" == "$expected" ]] || return 1
      echo "$file: OK"
    done < SHA256SUMS
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --bundle) need_value "$@"; bundle="$2"; shift 2 ;;
  --registry) need_value "$@"; registry="${2%/}"; shift 2 ;;
  --manifest-output) need_value "$@"; manifest_output="$2"; shift 2 ;;
  --insecure-destination) insecure_destination=true; shift ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

[[ -f "$bundle" ]] || die "bundle not found: $bundle"
[[ -n "$registry" ]] || die "--registry is required"
command -v skopeo >/dev/null 2>&1 || die "skopeo is required"

workdir="$(mktemp -d -t kc-offline-import.XXXXXX)"
trap 'rm -rf "$workdir"' EXIT
tar -xzf "$bundle" -C "$workdir"
bundle_root="$workdir/kubeclipper-offline-registry-bundle"
[[ -f "$bundle_root/SHA256SUMS" ]] || die "bundle SHA256SUMS is missing"
[[ -f "$bundle_root/bundle-artifacts.tsv" ]] || die "bundle artifact index is missing"
manifest_output="${manifest_output:-${bundle%.tar.gz}-release-manifest.yaml}"

echo "verifying bundle checksums"
(
  cd "$bundle_root"
  verify_checksums
)
mkdir -p "$(dirname "$manifest_output")"
cp -f "$bundle_root/release-manifest.yaml" "$manifest_output"

count=0
# All manifest columns must be consumed so storage and path keep their positions.
# shellcheck disable=SC2034
while IFS=$'\t' read -r id type target source source_digest digest platforms storage path; do
  [[ "$id" != "id" ]] || continue
  [[ -n "$id" && -n "$target" && -n "$storage" && -n "$path" ]] || die "invalid bundle artifact index entry"
  args=(copy --preserve-digests)
  [[ "$insecure_destination" == false ]] || args+=(--dest-tls-verify=false)
  echo "importing $type: $registry/$target"
  case "$storage" in
  oci) source_transport="oci:$bundle_root/$path:bundle" ;;
  dir) source_transport="dir:$bundle_root/$path" ;;
  *) die "unsupported bundle storage $storage for artifact $id" ;;
  esac
  skopeo "${args[@]}" "$source_transport" "docker://$registry/$target"
  count=$((count + 1))
done < "$bundle_root/bundle-artifacts.tsv"

echo "imported $count artifact(s) into $registry"
echo "release manifest: $manifest_output"
