#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
workdir="$(mktemp -d -t kc-release-provenance.XXXXXX)"
trap 'rm -rf "$workdir"' EXIT

mkdir -p \
  "$workdir/bin" \
  "$workdir/resource/k8s/v1.36.1/amd64" \
  "$workdir/resource/k8s/v1.36.1/arm64"
touch "$workdir/resource/k8s/v1.36.1/amd64/configs.tar.gz"
touch "$workdir/resource/k8s/v1.36.1/arm64/configs.tar.gz"
printf 'resource\tversion\tarch\tsourceImage\ttargetImage\n' > "$workdir/resource/images.lock"
printf 'k8s\tv1.36.1\tamd64\tregistry.k8s.io/pause:3.10\tregistry.example.com/kubeclipper/pause:3.10\n' >> "$workdir/resource/images.lock"
printf 'k8s\tv1.36.1\tamd64\tregistry.k8s.io/pause:3.10\tregistry.example.com/kubeclipper/pause:3.10\n' >> "$workdir/resource/images.lock"

cat > "$workdir/build.yaml" <<'EOF'
apiVersion: packaging.kubeclipper.io/v1alpha1
kind: OfflineResourceBuild
release: v1.8.0
architectures: [amd64]
registries:
  package: registry.example.com/kubeclipper
  image: registry.example.com/kubeclipper
bootstrap:
  kubeclipperVersion: v1.8.0
  etcdVersion: 3.5.21
  consoleVersion: v1.6.0
  registryVersion: 3.1.1
resources:
  k8s:
    versions: [v1.36.1]
EOF

"$ROOT/scripts/open-packaging/generate-release-manifest.sh" \
  --build-manifest "$workdir/build.yaml" \
  --resource-dir "$workdir/resource" \
  --output "$workdir/release.yaml" \
  --include-bootstrap \
  --arch all

python3 - "$workdir/release.yaml" <<'CHECK'
import sys
import yaml
with open(sys.argv[1], encoding="utf-8") as stream:
    document = yaml.safe_load(stream)
assert "sourceRevision" not in document["metadata"]
packages = {(item["component"]["kind"], item["component"]["name"]): item
            for item in document["artifacts"] if item["type"] == "package-image"}
for item in document["artifacts"]:
    assert "digest" not in item
    assert "sourceRevision" not in item
    assert ":" in item["target"] and "@" not in item["target"]
assert packages[("bootstrap", "kubeclipper")]["target"] == "package-bootstrap-kubeclipper:v1.8.0"
assert packages[("k8s", "k8s")]["platforms"] == ["linux/amd64", "linux/arm64"]
assert len([item for item in document["artifacts"] if item["type"] == "runtime-image"]) == 1
CHECK
printf '%s\n' 'release tag inventory generation checks passed'
