#!/usr/bin/env bash

set -euo pipefail

manifest=""
output=""
arch="amd64"
insecure_source=false

usage() {
  cat <<'EOF'
Usage:
  export-offline-registry-bundle.sh --manifest <file> [flags]

Exports every package image, Helm OCI chart, and runtime image selected from a
release manifest into lossless Registry seed data. The resulting tar.gz can be
imported into any OCI-compatible Registry or Harbor instance.

Flags:
  --manifest <file>       release-manifest.yaml.
  --output <file>         Output tar.gz. Default includes release and arch.
  --arch <amd64|arm64>    Export one target architecture. Default: amd64.
  --insecure-source       Allow plain HTTP / untrusted TLS for source Registry.
  -h, --help              Show this help.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

need_value() {
  [[ $# -ge 2 && -n "$2" ]] || die "$1 requires a value"
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --manifest) need_value "$@"; manifest="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --insecure-source) insecure_source=true; shift ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

[[ -f "$manifest" ]] || die "manifest not found: $manifest"
[[ "$arch" == "amd64" || "$arch" == "arm64" ]] || die "--arch must be amd64 or arm64"
command -v python3 >/dev/null 2>&1 || die "python3 is required"
command -v skopeo >/dev/null 2>&1 || die "skopeo is required"

release="$(python3 - "$manifest" <<'PY'
import sys
try:
    import yaml
except ImportError:
    raise SystemExit("PyYAML is required; install python3-yaml or pyyaml")
with open(sys.argv[1], "r", encoding="utf-8") as stream:
    document = yaml.safe_load(stream) or {}
if document.get("kind") != "ReleaseManifest":
    raise SystemExit("kind must be ReleaseManifest")
print(str((document.get("metadata") or {}).get("version") or "development"))
PY
)"
output="${output:-kubeclipper-offline-registry-bundle-${release}-${arch}.tar.gz}"
mkdir -p "$(dirname "$output")"

workdir="$(mktemp -d -t kc-offline-bundle.XXXXXX)"
trap 'rm -rf "$workdir"' EXIT
bundle_root="$workdir/kubeclipper-offline-registry-bundle"
mkdir -p "$bundle_root/layouts"
cp -f "$manifest" "$bundle_root/release-manifest.yaml"

selection="$workdir/selection.tsv"
python3 - "$manifest" "$arch" > "$selection" <<'PY'
import sys
try:
    import yaml
except ImportError:
    raise SystemExit("PyYAML is required; install python3-yaml or pyyaml")

manifest, arch = sys.argv[1:]
with open(manifest, "r", encoding="utf-8") as stream:
    document = yaml.safe_load(stream) or {}
if document.get("kind") != "ReleaseManifest":
    raise SystemExit("kind must be ReleaseManifest")

selected = []
for index, artifact in enumerate(document.get("artifacts") or []):
    artifact_type = str(artifact.get("type") or "")
    source = str(artifact.get("source") or "").removeprefix("oci://")
    target = str(artifact.get("target") or "").lstrip("/")
    digest = str(artifact.get("digest") or "")
    platforms = [str(value) for value in artifact.get("platforms") or []]
    if not artifact_type or not source or not target:
        raise SystemExit(f"artifact[{index}] requires type, source, and target")
    if platforms and f"linux/{arch}" not in platforms:
        continue
    selected.append((artifact_type, source, target, digest or "-", ",".join(platforms) or "-"))

if not selected:
    raise SystemExit(f"release manifest has no artifacts for linux/{arch}")
for index, values in enumerate(selected, start=1):
    print(f"{index:04d}\t" + "\t".join(values))
PY

index_file="$bundle_root/bundle-artifacts.tsv"
printf 'id\ttype\ttarget\tsource\tsourceDigest\tdigest\tplatforms\tstorage\tpath\n' > "$index_file"
count=0
while IFS=$'\t' read -r id type source target digest platforms; do
  [[ -n "$id" ]] || continue
  src="$source"
  [[ "$digest" == "-" ]] || src="${source%@*}@$digest"
  args=(copy --preserve-digests)
  [[ "$insecure_source" == false ]] || args+=(--src-tls-verify=false)
  if [[ "$platforms" != "-" ]]; then
    args+=(--override-os linux --override-arch "$arch")
  fi
  echo "exporting $type: $src"
  if [[ "$type" == "runtime-image" ]]; then
    storage="dir"
    path="layouts/$id"
    skopeo "${args[@]}" "docker://$src" "dir:$bundle_root/$path"
    bundle_digest="sha256:$(sha256_file "$bundle_root/$path/manifest.json")"
  else
    storage="oci"
    path="layouts/$id"
    skopeo "${args[@]}" "docker://$src" "oci:$bundle_root/$path:bundle"
    bundle_digest="$(python3 - "$bundle_root/$path/index.json" <<'PY'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as stream:
    index = json.load(stream)
manifests = index.get("manifests") or []
if len(manifests) != 1 or not manifests[0].get("digest"):
    raise SystemExit(f"expected one manifest in {sys.argv[1]}")
print(manifests[0]["digest"])
PY
)"
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$id" "$type" "$target" "$source" "$digest" "$bundle_digest" "$platforms" "$storage" "$path" >> "$index_file"
  count=$((count + 1))
done < "$selection"

python3 - "$manifest" "$index_file" "$bundle_root/release-manifest.yaml" "$arch" <<'PY'
import csv
import sys
try:
    import yaml
except ImportError:
    raise SystemExit("PyYAML is required; install python3-yaml or pyyaml")

source_manifest, index_file, output, arch = sys.argv[1:]
with open(source_manifest, "r", encoding="utf-8") as stream:
    document = yaml.safe_load(stream) or {}
with open(index_file, "r", encoding="utf-8", newline="") as stream:
    exported = {row["target"]: row for row in csv.DictReader(stream, delimiter="\t")}

artifacts = []
for artifact in document.get("artifacts") or []:
    platforms = [str(value) for value in artifact.get("platforms") or []]
    if platforms and f"linux/{arch}" not in platforms:
        continue
    target = str(artifact.get("target") or "").lstrip("/")
    row = exported.get(target)
    if row is None:
        continue
    item = dict(artifact)
    item["digest"] = row["digest"]
    if item.get("platforms"):
        item["platforms"] = [f"linux/{arch}"]
    artifacts.append(item)
document["artifacts"] = artifacts
with open(output, "w", encoding="utf-8") as stream:
    yaml.safe_dump(document, stream, sort_keys=False, default_flow_style=False)
PY

# Tar preserves hardlinks, so identical content-addressed blobs are stored only
# once even when several images share layers.
python3 - "$bundle_root/layouts" <<'PY'
import os
import re
import sys

digest_name = re.compile(r"^[0-9a-f]{64}$")
seen = {}
for root, _, files in os.walk(sys.argv[1]):
    for name in files:
        if not digest_name.match(name):
            continue
        path = os.path.join(root, name)
        previous = seen.get(name)
        if previous is None:
            seen[name] = path
            continue
        if os.path.getsize(previous) != os.path.getsize(path):
            raise SystemExit(f"digest-named blob size mismatch: {name}")
        os.unlink(path)
        os.link(previous, path)
PY

(
  cd "$bundle_root"
  find . -type f ! -name SHA256SUMS -print | LC_ALL=C sort | while IFS= read -r file; do
    printf '%s  %s\n' "$(sha256_file "$file")" "${file#./}"
  done > SHA256SUMS
)

tar -C "$workdir" -zcf "$output" kubeclipper-offline-registry-bundle
printf '%s  %s\n' "$(sha256_file "$output")" "$(basename "$output")" > "$output.sha256"
echo "wrote $output with $count artifact(s) for linux/$arch"
echo "wrote $output.sha256"
