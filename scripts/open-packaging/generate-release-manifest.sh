#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

build_manifest="$ROOT/packaging/resources.yaml"
resource_dir="$ROOT/resource"
output=""
package_registry=""
image_registry=""
include_bootstrap=false
arch_override=""

usage() {
  cat <<'EOF'
Usage:
  generate-release-manifest.sh [flags]

Generates a release-side synchronization manifest. The file is not consumed by
the KubeClipper server and does not participate in cluster creation.

Flags:
  --build-manifest <file>       Build manifest. Default: packaging/resources.yaml.
  --resource-dir <dir>          Built resource directory. Default: ./resource.
  --output <file>               Output file. Default: <resource-dir>/release-manifest.yaml.
  --package-registry <prefix>   Override registries.package from the build manifest.
  --image-registry <prefix>     Override registries.image from the build manifest.
  --include-bootstrap           Include the four bootstrap package images.
  --arch <amd64|arm64|all>      Override bootstrap platforms from the build manifest.
  -h, --help                    Show this help.
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
  --build-manifest) need_value "$@"; build_manifest="$2"; shift 2 ;;
  --resource-dir) need_value "$@"; resource_dir="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
  --package-registry) need_value "$@"; package_registry="${2%/}"; shift 2 ;;
  --image-registry) need_value "$@"; image_registry="${2%/}"; shift 2 ;;
  --include-bootstrap) include_bootstrap=true; shift ;;
  --arch) need_value "$@"; arch_override="$2"; shift 2 ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

[[ -f "$build_manifest" ]] || die "build manifest not found: $build_manifest"
[[ -d "$resource_dir" ]] || die "resource directory not found: $resource_dir"
case "$arch_override" in
"" | amd64 | arm64 | all) ;;
*) die "--arch must be amd64, arm64, or all" ;;
esac
command -v python3 >/dev/null 2>&1 || die "python3 is required"
output="${output:-$resource_dir/release-manifest.yaml}"
mkdir -p "$(dirname "$output")"

python3 - "$build_manifest" "$resource_dir" "$output" "$package_registry" "$image_registry" "$include_bootstrap" "$arch_override" "$ROOT/scripts/open-packaging" <<'PY'
import json
import os
import sys

try:
    import yaml
except ImportError:
    raise SystemExit("PyYAML is required; install python3-yaml or pyyaml")

build_manifest, resource_dir, output, package_override, image_override, include_bootstrap_raw, arch_override, packaging_dir = sys.argv[1:]
sys.path.insert(0, packaging_dir)
from image_repository import target_image
include_bootstrap = include_bootstrap_raw == "true"
with open(build_manifest, "r", encoding="utf-8") as stream:
    build = yaml.safe_load(stream) or {}

registries = build.get("registries") or {}
package_registry = (package_override or registries.get("package") or "").rstrip("/")
image_registry = (image_override or registries.get("image") or package_registry).rstrip("/")
if not package_registry:
    raise SystemExit("package registry is required")
if not image_registry:
    raise SystemExit("image registry is required")

artifacts = []

resources = build.get("resources") or {}
allowed_versions = {
    "k8s": {str(value) for value in (resources.get("k8s") or {}).get("versions") or []},
    "k8s-extension": {str(value) for value in (resources.get("k8sExtension") or {}).get("versions") or []},
    "kc-runtime": {str(value) for value in (resources.get("kcRuntime") or {}).get("versions") or []},
    "containerd": {str(value) for value in ((resources.get("cri") or {}).get("containerd") or {}).get("versions") or []},
    "calico": {str(value) for value in ((resources.get("cni") or {}).get("calico") or {}).get("versions") or []},
}
for name, image_set in (resources.get("runtimeImageSets") or {}).items():
    allowed_versions[str(name)] = {str(value) for value in (image_set or {}).get("versions") or []}
for name, addon in (build.get("addons") or {}).items():
    addon = addon or {}
    if addon.get("enabled") is not False:
        allowed_versions[str(name)] = {str(value) for value in addon.get("versions") or []}

def add_artifact(kind, component_kind, name, version, source, target, platforms=None, upstream=None):
    item = {
        "type": kind,
        "component": {"kind": component_kind, "name": name, "version": str(version)},
        "source": source,
        "target": target,
    }
    if platforms:
        item["platforms"] = sorted(set(platforms))
    if upstream and upstream != source:
        item["upstream"] = upstream
    artifacts.append(item)

package_map = {
    "k8s": ("k8s", "k8s"),
    "containerd": ("cri", "containerd"),
    "k8s-extension": ("k8s-extension", "k8s-extension"),
    "calico": ("cni", "calico"),
}
package_groups = {}

for component in sorted(os.listdir(resource_dir)):
    if component not in allowed_versions:
        continue
    component_dir = os.path.join(resource_dir, component)
    if not os.path.isdir(component_dir):
        continue
    for version in sorted(os.listdir(component_dir)):
        if version not in allowed_versions[component]:
            continue
        version_dir = os.path.join(component_dir, version)
        if not os.path.isdir(version_dir):
            continue
        for arch in sorted(os.listdir(version_dir)):
            leaf = os.path.join(version_dir, arch)
            if not os.path.isdir(leaf):
                continue
            platform = f"linux/{arch}"
            has_payload = any(os.path.isfile(os.path.join(leaf, filename))
                              for filename in ("configs.tar.gz", "charts.tgz"))
            if has_payload and component in package_map:
                component_kind, name = package_map[component]
                ref = f"{package_registry}/package-{name}:{version}"
                package_groups.setdefault((component_kind, name, version, ref), []).append(platform)

for (component_kind, name, version, ref), platforms in sorted(package_groups.items()):
    target = ref[len(package_registry) + 1:]
    add_artifact("package-image", component_kind, name, version, ref, target, platforms)

images_lock = os.path.join(resource_dir, "images.lock")
if not os.path.isfile(images_lock):
    raise SystemExit(f"images lock not found: {images_lock}")
runtime_targets = {}
with open(images_lock, "r", encoding="utf-8") as stream:
    header = stream.readline().rstrip("\n").split("\t")
    expected = ["resource", "version", "arch", "sourceImage", "targetImage"]
    if header != expected:
        raise SystemExit(f"invalid images.lock header: {header!r}")
    for line_number, line in enumerate(stream, start=2):
        if not line.strip():
            continue
        fields = line.rstrip("\n").split("\t")
        if len(fields) != len(expected):
            raise SystemExit(f"invalid images.lock line {line_number}")
        resource, version, arch, upstream, published = fields
        if resource not in allowed_versions or version not in allowed_versions[resource]:
            continue
        source = published or upstream
        if published and published.startswith(image_registry + "/"):
            target = published[len(image_registry) + 1:]
        else:
            target = target_image(upstream, image_registry)[len(image_registry) + 1:]
        component_kind = {
            "k8s": "k8s",
            "calico": "cni",
            "k8s-extension": "k8s-extension",
            "kc-runtime": "kc-runtime",
        }.get(resource, "addon")
        component_name = "calico" if resource == "calico" else resource
        platform = f"linux/{arch}"
        key = (target, platform)
        previous = runtime_targets.get(key)
        if previous:
            if previous != source:
                raise SystemExit(
                    f"conflicting runtime image source for {target} {platform}: "
                    f"{previous} != {source}"
                )
            continue
        runtime_targets[key] = source
        add_artifact(
            "runtime-image", component_kind, component_name, version,
            source, target, [platform], upstream,
        )

if include_bootstrap:
    bootstrap = build.get("bootstrap") or {}
    bootstrap_refs = [
        ("kubeclipper", bootstrap.get("kubeclipperVersion")),
        ("etcd", bootstrap.get("etcdVersion")),
        ("console", bootstrap.get("consoleVersion")),
        ("registry", bootstrap.get("registryVersion")),
    ]
    if arch_override:
        bootstrap_arches = ["amd64", "arm64"] if arch_override == "all" else [arch_override]
    else:
        bootstrap_arches = [str(arch) for arch in (build.get("architectures") or [])]
    architectures = [f"linux/{arch}" for arch in bootstrap_arches]
    for name, version in bootstrap_refs:
        if not version:
            raise SystemExit(f"bootstrap.{name} version is required")
        ref = f"{package_registry}/package-bootstrap-{name}:{version}"
        add_artifact("package-image", "bootstrap", name, version, ref, ref[len(package_registry) + 1:], architectures)

artifacts.sort(key=lambda item: (
    item["type"], item["component"]["kind"], item["component"]["name"],
    item["component"]["version"], item["target"],
))
document = {
    "apiVersion": "delivery.kubeclipper.io/v1alpha1",
    "kind": "ReleaseManifest",
    "metadata": {
        "name": "kubeclipper-resources",
        "version": str(build.get("release") or "development"),
    },
    "registries": {"package": package_registry, "image": image_registry},
    "artifacts": artifacts,
}
with open(output, "w", encoding="utf-8") as stream:
    yaml.safe_dump(document, stream, sort_keys=False, default_flow_style=False)
PY

echo "wrote $output"
