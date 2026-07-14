#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

build_manifest="$ROOT/packaging/resources.yaml"
resource_dir="$ROOT/resource"
output=""
package_registry=""
image_registry=""
resolve_digests=false
insecure=false
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
  --resolve-digests             Resolve immutable digests from the published source Registry.
  --insecure                    Use plain HTTP when resolving digests with crane.
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
  --resolve-digests) resolve_digests=true; shift ;;
  --insecure) insecure=true; shift ;;
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
if [[ "$resolve_digests" == true ]]; then
  command -v crane >/dev/null 2>&1 || die "crane is required for --resolve-digests"
fi
output="${output:-$resource_dir/release-manifest.yaml}"
mkdir -p "$(dirname "$output")"

python3 - "$build_manifest" "$resource_dir" "$output" "$package_registry" "$image_registry" "$resolve_digests" "$insecure" "$include_bootstrap" "$arch_override" <<'PY'
import os
import subprocess
import sys
import tarfile

try:
    import yaml
except ImportError:
    raise SystemExit("PyYAML is required; install python3-yaml or pyyaml")

build_manifest, resource_dir, output, package_override, image_override, resolve_raw, insecure_raw, include_bootstrap_raw, arch_override = sys.argv[1:]
resolve_digests = resolve_raw == "true"
insecure = insecure_raw == "true"
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
}
package_groups = {}
chart_groups = {}

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
            if os.path.isfile(os.path.join(leaf, "configs.tar.gz")) and component in package_map:
                component_kind, name = package_map[component]
                ref = f"{package_registry}/kubeclipper/packages/{component_kind}/{name}:{version}"
                package_groups.setdefault((component_kind, name, version, ref), []).append(platform)
            chart_archive = os.path.join(leaf, "charts.tgz")
            if os.path.isfile(chart_archive):
                chart_name = component
                chart_version = str(version).lstrip("v")
                try:
                    with tarfile.open(chart_archive, "r:gz") as archive:
                        chart_yaml = next(
                            member for member in archive.getmembers()
                            if member.isfile() and member.name.count("/") == 1 and member.name.endswith("/Chart.yaml")
                        )
                        chart = yaml.safe_load(archive.extractfile(chart_yaml)) or {}
                        chart_name = str(chart.get("name") or chart_name)
                        chart_version = str(chart.get("version") or chart_version)
                except (OSError, StopIteration, tarfile.TarError):
                    pass
                ref = f"{package_registry}/kubeclipper/charts/{chart_name}:{chart_version}"
                component_kind = "cni" if component == "calico" else "addon"
                component_name = "calico" if component == "calico" else component
                chart_groups[(component_kind, component_name, version, ref)] = True

for (component_kind, name, version, ref), platforms in sorted(package_groups.items()):
    target = ref[len(package_registry) + 1:]
    add_artifact("package-image", component_kind, name, version, ref, target, platforms)

for component_kind, name, version, ref in sorted(chart_groups):
    target = ref[len(package_registry) + 1:]
    add_artifact("helm-chart", component_kind, name, version, ref, target)

images_lock = os.path.join(resource_dir, "images.lock")
if not os.path.isfile(images_lock):
    raise SystemExit(f"images lock not found: {images_lock}")
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
            path = upstream.split("/", 1)[-1]
            target = path
        component_kind = {
            "k8s": "k8s",
            "calico": "cni",
            "k8s-extension": "k8s-extension",
            "kc-runtime": "kc-runtime",
        }.get(resource, "addon")
        component_name = "calico" if resource == "calico" else resource
        add_artifact(
            "runtime-image", component_kind, component_name, version,
            source, target, [f"linux/{arch}"], upstream,
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
        ref = f"{package_registry}/kubeclipper/packages/bootstrap/{name}:{version}"
        add_artifact("package-image", "bootstrap", name, version, ref, ref[len(package_registry) + 1:], architectures)

artifacts.sort(key=lambda item: (
    item["type"], item["component"]["kind"], item["component"]["name"],
    item["component"]["version"], item["target"],
))
if resolve_digests:
    for artifact in artifacts:
        command = ["crane", "digest"]
        if insecure:
            command.append("--insecure")
        if artifact["type"] == "runtime-image" and len(artifact.get("platforms") or []) == 1:
            command.extend(["--platform", artifact["platforms"][0]])
        command.append(artifact["source"].removeprefix("oci://"))
        try:
            artifact["digest"] = subprocess.check_output(command, text=True, stderr=subprocess.PIPE).strip()
        except subprocess.CalledProcessError as error:
            message = error.stderr.strip() or str(error)
            raise SystemExit(f"resolve digest failed for {artifact['source']}: {message}")
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
