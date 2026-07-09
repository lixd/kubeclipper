#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

manifest="$ROOT/packaging/resources.yaml"
output="$ROOT/resource"
registry=""
image_registry=""
arch_override=""
push=false
dry_run=false
include_disabled=false
components=()

usage() {
  cat <<'EOF'
Usage:
  build-offline-resources.sh [flags]

Flags:
  --manifest <file>          Build manifest. Default: packaging/resources.yaml.
  --output <dir>             Resource output root. Default: ./resource.
  --registry <host/path>     Package and chart OCI Registry. Defaults to manifest registries.package.
  --image-registry <host/path>
                             Runtime image Registry. Defaults to manifest registries.image or --registry.
  --arch <amd64|arm64|all>   Override manifest architectures.
  --component <name>         Build only this component. Can be repeated.
  --include-disabled         Also build manifest entries with enabled: false.
  --push                     Publish package images, Helm OCI charts, and runtime images after build.
  --dry-run                  Print commands and generate metadata without downloading or pushing.
  -h, --help                 Show this help.

Examples:
  scripts/open-packaging/build-offline-resources.sh \
    --manifest packaging/resources.yaml \
    --output /tmp/kc-resource

  scripts/open-packaging/build-offline-resources.sh \
    --manifest packaging/resources.yaml \
    --output /tmp/kc-resource \
    --registry ghcr.io/lixd/kubeclipper \
    --image-registry ghcr.io/lixd/kubeclipper \
    --push
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

need_value() {
  [[ $# -ge 2 && -n "$2" ]] || die "$1 requires a value"
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --manifest) need_value "$@"; manifest="$2"; shift 2 ;;
  --output) need_value "$@"; output="$2"; shift 2 ;;
  --registry) need_value "$@"; registry="$2"; shift 2 ;;
  --image-registry) need_value "$@"; image_registry="$2"; shift 2 ;;
  --arch) need_value "$@"; arch_override="$2"; shift 2 ;;
  --component) need_value "$@"; components+=("$2"); shift 2 ;;
  --include-disabled) include_disabled=true; shift ;;
  --push) push=true; shift ;;
  --dry-run) dry_run=true; shift ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

[[ -f "$manifest" ]] || die "manifest not found: $manifest"
case "$arch_override" in
"" | amd64 | arm64 | all) ;;
*) die "--arch must be amd64, arm64, or all" ;;
esac
need_cmd python3

component_filter() {
  if [[ ${#components[@]} -eq 0 ]]; then
    return 0
  fi
  local item=$1
  local wanted
  for wanted in "${components[@]}"; do
    [[ "$wanted" == "$item" ]] && return 0
  done
  return 1
}

shell_join() {
  local out="" arg
  for arg in "$@"; do
    printf -v arg "%q" "$arg"
    out+="${out:+ }$arg"
  done
  printf '%s\n' "$out"
}

manifest_defaults() {
  python3 - "$manifest" <<'PY'
import sys
try:
    import yaml
except ImportError:
    raise SystemExit("PyYAML is required; install python3-yaml or pyyaml")

with open(sys.argv[1], "r", encoding="utf-8") as f:
    doc = yaml.safe_load(f) or {}
regs = doc.get("registries") or {}
print(f"{regs.get('package', '')}\t{regs.get('image', '')}")
PY
}

read -r manifest_registry manifest_image_registry < <(manifest_defaults)
registry="${registry:-$manifest_registry}"
image_registry="${image_registry:-${manifest_image_registry:-$registry}}"
if [[ "$push" == true ]]; then
  [[ -n "$registry" ]] || die "--registry is required for --push"
  [[ -n "$image_registry" ]] || die "--image-registry is required for --push"
fi

manifest_matrix() {
  python3 - "$manifest" "$output" "$arch_override" "$include_disabled" <<'PY'
import sys
try:
    import yaml
except ImportError:
    raise SystemExit("PyYAML is required; install python3-yaml or pyyaml")

manifest, output, arch_override, include_disabled_raw = sys.argv[1:5]
include_disabled = include_disabled_raw == "true"
with open(manifest, "r", encoding="utf-8") as f:
    doc = yaml.safe_load(f) or {}

if arch_override:
    archs = ["amd64", "arm64"] if arch_override == "all" else [arch_override]
else:
    archs = [str(v) for v in doc.get("architectures", [])]

def versions(node):
    return [str(v) for v in (node or {}).get("versions", [])]

def emit(kind, name, version, arch, args=None):
    row = [kind, name, version, arch]
    row.extend(str(v) for v in (args or []) if v is not None and str(v) != "")
    print("\t".join(row))

def add_if_present(args, flag, value, arch=None):
    if value is None or str(value) == "":
        return
    value = str(value)
    if arch:
        value = value.replace("${arch}", arch)
    args.extend([flag, value])

resources = doc.get("resources") or {}
k8s = resources.get("k8s") or {}
for version in versions(k8s):
    for arch in archs:
        args = [
            "--kubeadm-conf-version", k8s.get("kubeadmConfVersion"),
        ]
        add_if_present(args, "--kubelet-service-file", k8s.get("kubeletServiceFile"))
        add_if_present(args, "--kubelet-pre-start-file", k8s.get("kubeletPreStartFile"))
        add_if_present(args, "--image-list-file", k8s.get("imagesFile"))
        emit("resource", "k8s", version, arch, args)

ext = resources.get("k8sExtension") or {}
for version in versions(ext):
    for arch in archs:
        conntrack_cfg = ext.get("conntrack") or {}
        conntrack_version = str(conntrack_cfg.get("version") or "1.4.9")

        args = [
            "--etcd-version", ext.get("etcdVersion"),
            "--helm-version", ext.get("helmVersion"),
            "--nerdctl-version", ext.get("nerdctlVersion"),
            "--cni-plugins-version", ext.get("cniPluginsVersion"),
            "--calico-version", ext.get("calicoVersion"),
            "--conntrack-version", conntrack_version,
        ]
        add_if_present(args, "--images-file", ext.get("imagesFile"))
        emit("resource", "k8s-extension", version, arch, args)

cri = resources.get("cri") or {}
containerd = cri.get("containerd") or {}
for version in versions(containerd):
    for arch in archs:
        args = [
            "--runc-version", containerd.get("runcVersion"),
            "--crictl-version", containerd.get("crictlVersion"),
        ]
        emit("resource", "containerd", version, arch, args)

calico = ((resources.get("cni") or {}).get("calico") or {})
for version in versions(calico):
    for arch in archs:
        args = []
        add_if_present(args, "--chart-name", calico.get("chartName"))
        add_if_present(args, "--chart-version", calico.get("chartVersion"))
        add_if_present(args, "--chart-file", calico.get("chartFile"))
        add_if_present(args, "--images-file", calico.get("imagesFile"))
        emit("resource", "calico", version, arch, args)

kc_runtime = resources.get("kcRuntime") or {}
for version in versions(kc_runtime):
    for arch in archs:
        args = []
        add_if_present(args, "--images-file", kc_runtime.get("imagesFile"))
        emit("resource", "kc-runtime", version, arch, args)

for name, addon in (doc.get("addons") or {}).items():
    addon = addon or {}
    if addon.get("enabled") is False and not include_disabled:
        continue
    for version in versions(addon):
        for arch in archs:
            args = []
            if addon.get("chartName"):
                args.extend(["--chart-name", addon.get("chartName")])
            if addon.get("chartVersion"):
                args.extend(["--chart-version", addon.get("chartVersion")])
            add_if_present(args, "--chart-file", addon.get("chartFile"))
            add_if_present(args, "--images-file", addon.get("imagesFile"))
            emit("addon", str(name), version, arch, args)
PY
}

run_cmd() {
  if [[ "$dry_run" == true ]]; then
    echo "dry-run: $(shell_join "$@")"
    return 0
  fi
  echo "==> $(shell_join "$@")"
  "$@"
}

build_line() {
  local type=$1 name=$2 version=$3 arch=$4
  shift 4

  component_filter "$name" || return 0

  local cmd=()
  case "$type/$name" in
  resource/k8s)
    cmd=("$ROOT/scripts/open-packaging/resource-builders/build-k8s-package.sh" --version "$version" --arch "$arch" --output "$output" "$@")
    ;;
  resource/k8s-extension)
    cmd=("$ROOT/scripts/open-packaging/resource-builders/build-k8s-extension-package.sh" --version "$version" --arch "$arch" --output "$output" "$@")
    ;;
  resource/containerd)
    cmd=("$ROOT/scripts/open-packaging/resource-builders/build-containerd-package.sh" --version "$version" --arch "$arch" --output "$output" "$@")
    ;;
  resource/calico)
    cmd=("$ROOT/scripts/open-packaging/resource-builders/build-calico-package.sh" --version "$version" --arch "$arch" --output "$output" "$@")
    ;;
  resource/kc-runtime)
    cmd=("$ROOT/scripts/open-packaging/resource-builders/build-kc-runtime-package.sh" --version "$version" --arch "$arch" --output "$output" "$@")
    ;;
  addon/*)
    cmd=("$ROOT/scripts/open-packaging/resource-builders/build-addon-package.sh" --name "$name" --version "$version" --arch "$arch" --output "$output" "$@")
    ;;
  *)
    die "unsupported manifest row: $type/$name"
    ;;
  esac
  run_cmd "${cmd[@]}"
}

mkdir -p "$output"

matrix_file="$(mktemp -t kc-offline-matrix.XXXXXX)"
trap 'rm -f "$matrix_file"' EXIT
manifest_matrix > "$matrix_file"

publish_line() {
  local type=$1 name=$2 version=$3 arch=$4
  shift 4

  component_filter "$name" || return 0

  local cmd=()
  case "$type/$name" in
  resource/k8s)
    cmd=("$ROOT/scripts/open-packaging/publish-resource-k8s.sh" --registry-prefix "$registry" --image-registry-prefix "$image_registry" --version "$version" --arch "$arch" "$@")
    ;;
  resource/containerd)
    cmd=("$ROOT/scripts/open-packaging/publish-resource-containerd.sh" --registry-prefix "$registry" --version "$version" --arch "$arch" "$@")
    ;;
  resource/k8s-extension)
    cmd=("$ROOT/scripts/open-packaging/publish-resource-k8s-extension.sh" --registry-prefix "$registry" --image-registry-prefix "$image_registry" --version "$version" --arch "$arch" "$@")
    ;;
  resource/calico)
    cmd=("$ROOT/scripts/open-packaging/publish-resource-calico.sh" --registry-prefix "$registry" --image-registry-prefix "$image_registry" --version "$version" --arch "$arch" "$@")
    ;;
  resource/kc-runtime)
    cmd=("$ROOT/scripts/open-packaging/publish-resource-kc-runtime.sh" --image-registry-prefix "$image_registry" --version "$version" --arch "$arch" "$@")
    ;;
  addon/*)
    echo "skipping disabled or optional addon publish path: $name/$version/$arch"
    return 0
    ;;
  *)
    die "unsupported manifest row: $type/$name"
    ;;
  esac
  run_cmd "${cmd[@]}"
}

if [[ "$push" == true ]]; then
  while IFS=$'\t' read -r type name version arch rest; do
    [[ -n "${type:-}" ]] || continue
    args=()
    if [[ -n "${rest:-}" ]]; then
      IFS=$'\t' read -r -a args <<<"$rest"
    fi
    publish_line "$type" "$name" "$version" "$arch" "${args[@]}"
  done < "$matrix_file"
else
  while IFS=$'\t' read -r type name version arch rest; do
    [[ -n "${type:-}" ]] || continue
    args=()
    if [[ -n "${rest:-}" ]]; then
      IFS=$'\t' read -r -a args <<<"$rest"
    fi
    build_line "$type" "$name" "$version" "$arch" "${args[@]}"
  done < "$matrix_file"

  "$ROOT/scripts/open-packaging/generate-resource-metadata.sh" \
    --resource-dir "$output" \
    --image-registry "$image_registry"
fi
