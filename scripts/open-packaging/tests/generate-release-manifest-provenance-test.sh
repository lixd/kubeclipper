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

cat > "$workdir/bin/crane" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
digest) printf 'sha256:%064d\n' 1 ;;
config)
  revision="dependency-rev"
  if [[ " $* " == *"/bootstrap/kubeclipper@"* ]]; then
    revision="${FAKE_REVISION:-}"
  fi
  if [[ " $* " == *"/bootstrap/kubeclipper@"* && " $* " == *" --platform ${FAKE_STALE_PLATFORM:-__none__} "* ]]; then
    revision=stale
  fi
  printf '{"config":{"Labels":{"org.opencontainers.image.revision":"%s"}}}\n' "$revision"
  ;;
*) exit 2 ;;
esac
EOF
chmod +x "$workdir/bin/crane"

if "$ROOT/scripts/open-packaging/generate-release-manifest.sh" \
  --build-manifest "$workdir/build.yaml" \
  --resource-dir "$workdir/resource" \
  --output "$workdir/unresolved.yaml" \
  --source-revision abc123 >"$workdir/unresolved.out" 2>"$workdir/unresolved.err"; then
  echo "expected provenance without digest resolution to be rejected" >&2
  exit 1
fi
grep -q -- '--source-revision requires --resolve-digests' "$workdir/unresolved.err"

FAKE_REVISION=abc123 PATH="$workdir/bin:$PATH" \
  "$ROOT/scripts/open-packaging/generate-release-manifest.sh" \
  --build-manifest "$workdir/build.yaml" \
  --resource-dir "$workdir/resource" \
  --output "$workdir/release.yaml" \
  --resolve-digests \
  --include-bootstrap \
  --arch all \
  --source-revision abc123

python3 - "$workdir/release.yaml" <<'PY'
import sys
import yaml

with open(sys.argv[1], encoding="utf-8") as stream:
    document = yaml.safe_load(stream)
assert document["metadata"]["sourceRevision"] == "abc123"
packages = {
    (item["component"]["kind"], item["component"]["name"]): item
    for item in document["artifacts"] if item["type"] == "package-image"
}
assert packages[("bootstrap", "kubeclipper")]["sourceRevision"] == "abc123"
assert packages[("bootstrap", "kubeclipper")]["platforms"] == ["linux/amd64", "linux/arm64"]
assert packages[("k8s", "k8s")]["sourceRevision"] == "dependency-rev"
assert packages[("k8s", "k8s")]["platforms"] == ["linux/amd64", "linux/arm64"]
runtime_images = [item for item in document["artifacts"] if item["type"] == "runtime-image"]
assert len(runtime_images) == 1
PY

if FAKE_REVISION=abc123 FAKE_STALE_PLATFORM=linux/arm64 PATH="$workdir/bin:$PATH" \
  "$ROOT/scripts/open-packaging/generate-release-manifest.sh" \
  --build-manifest "$workdir/build.yaml" \
  --resource-dir "$workdir/resource" \
  --output "$workdir/stale.yaml" \
  --resolve-digests \
  --include-bootstrap \
  --arch all \
  --source-revision abc123 >"$workdir/stale.out" 2>"$workdir/stale.err"; then
  echo "expected stale package provenance to be rejected" >&2
  exit 1
fi
grep -q 'mixed platform source revisions' "$workdir/stale.err"
grep -q 'linux/arm64=stale' "$workdir/stale.err"

printf 'resource\tversion\tarch\tsourceImage\ttargetImage\n' > "$workdir/resource/images.lock"
printf 'k8s\tv1.36.1\tamd64\tregistry-one.example/pause:3.10\t\n' >> "$workdir/resource/images.lock"
printf 'k8s\tv1.36.1\tamd64\tregistry-two.example/pause:3.10\t\n' >> "$workdir/resource/images.lock"
if "$ROOT/scripts/open-packaging/generate-release-manifest.sh" \
  --build-manifest "$workdir/build.yaml" \
  --resource-dir "$workdir/resource" \
  --output "$workdir/conflict.yaml" >"$workdir/conflict.out" 2>"$workdir/conflict.err"; then
  echo "expected conflicting runtime image sources to be rejected" >&2
  exit 1
fi
grep -q 'conflicting runtime image source' "$workdir/conflict.err"
echo "release manifest provenance test passed"
