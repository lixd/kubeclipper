#!/usr/bin/env bash

set -euo pipefail

resource_dir=""
image_registry=""

usage() {
  cat <<'EOF'
Usage:
  generate-resource-metadata.sh --resource-dir <dir> [flags]

Flags:
  --resource-dir <dir>       Resource output root.
  --image-registry <prefix>  Target runtime image Registry used in images.lock.
  -h, --help                 Show this help.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

need_value() {
  [[ $# -ge 2 && -n "$2" ]] || die "$1 requires a value"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --resource-dir) need_value "$@"; resource_dir="$2"; shift 2 ;;
  --image-registry) need_value "$@"; image_registry="${2%/}"; shift 2 ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

[[ -n "$resource_dir" ]] || die "--resource-dir is required"
[[ -d "$resource_dir" ]] || die "resource dir not found: $resource_dir"

target_image() {
  local source=$1
  if [[ -z "$image_registry" ]]; then
    echo ""
    return
  fi
  local path="$source"
  local first="${path%%/*}"
  if [[ "$first" == *.* || "$first" == *:* || "$first" == "localhost" ]]; then
    path="${path#*/}"
  fi
  case "$source" in
  registry.k8s.io/coredns/coredns:*)
    path="coredns:${source##*:}"
    ;;
  esac
  echo "$image_registry/$path"
}

images_lock="$resource_dir/images.lock"
charts_lock="$resource_dir/charts.lock"
report="$resource_dir/build-report.json"

printf 'resource\tversion\tarch\tsourceImage\ttargetImage\n' > "$images_lock"
while IFS= read -r -d '' list; do
  rel="${list#$resource_dir/}"
  IFS="/" read -r resource version arch file extra <<<"$rel"
  [[ "$file" == "images.txt" && -z "${extra:-}" ]] || continue
  while IFS= read -r image; do
    [[ -n "$image" && "$image" != \#* ]] || continue
    printf '%s\t%s\t%s\t%s\t%s\n' "$resource" "$version" "$arch" "$image" "$(target_image "$image")" >> "$images_lock"
  done < "$list"
done < <(find "$resource_dir" -mindepth 4 -maxdepth 4 -type f -name images.txt -print0 | sort -z)

printf 'resource\tversion\tarch\tchartArchive\n' > "$charts_lock"
while IFS= read -r -d '' chart; do
  rel="${chart#$resource_dir/}"
  IFS="/" read -r resource version arch file extra <<<"$rel"
  [[ "$file" == "charts.tgz" && -z "${extra:-}" ]] || continue
  printf '%s\t%s\t%s\t%s\n' "$resource" "$version" "$arch" "$chart" >> "$charts_lock"
done < <(find "$resource_dir" -mindepth 4 -maxdepth 4 -type f -name charts.tgz -print0 | sort -z)

python3 - "$resource_dir" > "$report" <<'PY'
import datetime
import glob
import json
import os
import sys

root = sys.argv[1]
packages = []
for path in sorted(p for p in glob.glob(os.path.join(root, "*", "*", "*")) if os.path.isdir(p)):
    rel = os.path.relpath(path, root)
    resource, version, arch = rel.split(os.sep, 2)
    files = set(os.listdir(path))
    packages.append({
        "resource": resource,
        "version": version,
        "arch": arch,
        "hasConfigs": "configs.tar.gz" in files,
        "hasChart": "charts.tgz" in files,
        "hasImagesArchive": "images.tar.gz" in files,
        "hasImagesList": "images.txt" in files,
        "hasBinary": any(os.path.isfile(os.path.join(path, name)) and name not in {
            "configs.tar.gz",
            "charts.tgz",
            "images.tar.gz",
            "images.txt",
            "manifest.json",
        } for name in files),
    })

print(json.dumps({
    "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
    "packages": packages,
}, indent=2))
PY

echo "wrote $images_lock"
echo "wrote $charts_lock"
echo "wrote $report"
