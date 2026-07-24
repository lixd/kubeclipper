#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
workdir="$(mktemp -d -t kc-release-assembly.XXXXXX)"
trap 'rm -rf "$workdir"' EXIT

mkdir -p "$workdir/bin" "$workdir/resource"

python3 - "$ROOT/packaging/resources.yaml" "$workdir/resource" <<'PY'
import io
import os
import sys
import tarfile

import yaml

manifest_path, root = sys.argv[1:]
with open(manifest_path, encoding="utf-8") as stream:
    build = yaml.safe_load(stream)

architectures = build["architectures"]
resources = build["resources"]
versions = {
    "k8s": resources["k8s"]["versions"],
    "containerd": resources["cri"]["containerd"]["versions"],
    "k8s-extension": resources["k8sExtension"]["versions"],
    "calico": resources["cni"]["calico"]["versions"],
    "kc-runtime": resources["kcRuntime"]["versions"],
}
for name, image_set in resources["runtimeImageSets"].items():
    versions[name] = image_set["versions"]

package_components = {"k8s", "containerd", "k8s-extension"}
runtime_components = {"k8s", "k8s-extension", "calico", "kc-runtime", "nfs", "metallb"}

for component, component_versions in versions.items():
    for version in component_versions:
        for arch in architectures:
            leaf = os.path.join(root, component, str(version), arch)
            os.makedirs(leaf, exist_ok=True)
            if component in package_components:
                open(os.path.join(leaf, "configs.tar.gz"), "wb").close()
            if component in runtime_components:
                image_version = str(version).replace("/", "-")
                with open(os.path.join(leaf, "images.txt"), "w", encoding="utf-8") as stream:
                    stream.write(f"registry.example.com/upstream/{component}:{image_version}-{arch}\n")
            if component == "calico":
                chart = (
                    "apiVersion: v2\n"
                    "name: tigera-operator\n"
                    f"version: {version}\n"
                ).encode()
                info = tarfile.TarInfo("tigera-operator/Chart.yaml")
                info.size = len(chart)
                with tarfile.open(os.path.join(leaf, "charts.tgz"), "w:gz") as archive:
                    archive.addfile(info, io.BytesIO(chart))
PY

cat > "$workdir/bin/crane" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
digest)
  printf 'sha256:%064d\n' 1
  ;;
config)
  revision=dependency-revision
  if [[ " $* " == *"/bootstrap/kubeclipper@"* ]]; then
    revision=release-revision
  fi
  printf '{"config":{"Labels":{"org.opencontainers.image.revision":"%s"}}}\n' "$revision"
  ;;
*)
  exit 2
  ;;
esac
EOF
chmod +x "$workdir/bin/crane"

"$ROOT/scripts/open-packaging/generate-resource-metadata.sh" \
  --resource-dir "$workdir/resource" \
  --image-registry ghcr.io/kubeclipper/kubeclipper

PATH="$workdir/bin:$PATH" \
  "$ROOT/scripts/open-packaging/generate-release-manifest.sh" \
  --build-manifest "$ROOT/packaging/resources.yaml" \
  --resource-dir "$workdir/resource" \
  --output "$workdir/release.yaml" \
  --package-registry ghcr.io/kubeclipper/kubeclipper \
  --image-registry ghcr.io/kubeclipper/kubeclipper \
  --include-bootstrap \
  --arch all \
  --resolve-digests \
  --source-revision release-revision

python3 - "$ROOT/packaging/resources.yaml" "$workdir/release.yaml" <<'PY'
import sys

import yaml

build_path, release_path = sys.argv[1:]
with open(build_path, encoding="utf-8") as stream:
    build = yaml.safe_load(stream)
with open(release_path, encoding="utf-8") as stream:
    release = yaml.safe_load(stream)

assert release["metadata"]["version"] == "v2.0.0"
assert release["metadata"]["sourceRevision"] == "release-revision"
assert release["registries"] == {
    "package": "ghcr.io/kubeclipper/kubeclipper",
    "image": "ghcr.io/kubeclipper/kubeclipper",
}

artifacts = release["artifacts"]
packages = {
    (item["component"]["kind"], item["component"]["name"], item["component"]["version"])
    for item in artifacts if item["type"] == "package-image"
}
expected_packages = {
    ("bootstrap", "kubeclipper", build["bootstrap"]["kubeclipperVersion"]),
    ("bootstrap", "etcd", build["bootstrap"]["etcdVersion"]),
    ("bootstrap", "console", build["bootstrap"]["consoleVersion"]),
    ("bootstrap", "registry", build["bootstrap"]["registryVersion"]),
}
expected_packages.update(("k8s", "k8s", version) for version in build["resources"]["k8s"]["versions"])
expected_packages.update(("cri", "containerd", version) for version in build["resources"]["cri"]["containerd"]["versions"])
expected_packages.update(("k8s-extension", "k8s-extension", version) for version in build["resources"]["k8sExtension"]["versions"])
assert packages == expected_packages

for item in artifacts:
    if item["type"] == "package-image":
        assert item["platforms"] == ["linux/amd64", "linux/arm64"]
        assert item["sourceRevision"]
        assert item["digest"].startswith("sha256:")

charts = {
    (item["component"]["kind"], item["component"]["name"], item["component"]["version"])
    for item in artifacts if item["type"] == "helm-chart"
}
assert charts == {
    ("cni", "calico", version)
    for version in build["resources"]["cni"]["calico"]["versions"]
}

runtime = {
    (item["component"]["name"], item["component"]["version"], item["platforms"][0])
    for item in artifacts if item["type"] == "runtime-image"
}
runtime_versions = {
    "k8s": build["resources"]["k8s"]["versions"],
    "k8s-extension": build["resources"]["k8sExtension"]["versions"],
    "calico": build["resources"]["cni"]["calico"]["versions"],
    "kc-runtime": build["resources"]["kcRuntime"]["versions"],
}
runtime_versions.update({
    name: image_set["versions"]
    for name, image_set in build["resources"]["runtimeImageSets"].items()
})
expected_runtime = {
    (name, version, f"linux/{arch}")
    for name, versions in runtime_versions.items()
    for version in versions
    for arch in build["architectures"]
}
assert runtime == expected_runtime
assert len(artifacts) == len(expected_packages) + len(charts) + len(expected_runtime)
PY

PATH="$workdir/bin:$PATH" \
  "$ROOT/scripts/open-packaging/verify-release-manifest.sh" \
  --manifest "$workdir/release.yaml" \
  --registry ghcr.io/kubeclipper/kubeclipper

echo "release assembly test passed"
