#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
workdir="$(mktemp -d -t kc-release-skopeo.XXXXXX)"
trap 'rm -rf "$workdir"' EXIT

mkdir -p "$workdir/bin"
chart_manifest='{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{},"layers":[]}'
amd64_digest="sha256:$(printf 'a%.0s' {1..64})"
arm64_digest="sha256:$(printf 'b%.0s' {1..64})"
runtime_manifest="$(printf '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"digest":"%s","platform":{"os":"linux","architecture":"amd64"}},{"digest":"%s","platform":{"os":"linux","architecture":"arm64"}}]}' "$amd64_digest" "$arm64_digest")"

printf '%s' "$runtime_manifest" > "$workdir/package.json"
printf '%s' "$chart_manifest" > "$workdir/chart.json"
printf '%s' "$runtime_manifest" > "$workdir/runtime.json"
package_digest="sha256:$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())' "$workdir/package.json")"
chart_digest="sha256:$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())' "$workdir/chart.json")"

cat > "$workdir/bin/skopeo" <<'EOF'
#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
assert "inspect" in args and args[args.index("--retry-times") + 1] == "3"
name = args[-1].rsplit("/", 1)[-1].split(":", 1)[0]
if "--raw" in args:
    sys.stdout.write(Path(os.environ[f"FAKE_{name.upper()}_MANIFEST"]).read_text())
else:
    assert "--config" in args
    assert args[args.index("--override-os") + 1] == "linux"
    arch = args[args.index("--override-arch") + 1]
    config = {} if name == "package" else {"os": "linux", "architecture": os.getenv("FAKE_RUNTIME_ARCH") or arch}
    print(json.dumps(config))
EOF
chmod +x "$workdir/bin/skopeo"

cat > "$workdir/release.yaml" <<EOF
kind: ReleaseManifest
artifacts:
- type: package-image
  target: package:v1
  platforms: [linux/amd64, linux/arm64]
  digest: $package_digest
- type: helm-chart
  target: chart:v1
  digest: $chart_digest
- type: runtime-image
  target: runtime:v1
  platforms: [linux/amd64]
  digest: $amd64_digest
- type: runtime-image
  target: runtime:v1
  platforms: [linux/arm64]
  digest: $arm64_digest
EOF

export FAKE_PACKAGE_MANIFEST="$workdir/package.json"
export FAKE_CHART_MANIFEST="$workdir/chart.json"
export FAKE_RUNTIME_MANIFEST="$workdir/runtime.json"
output="$(PATH="$workdir/bin:$PATH" "$ROOT/scripts/open-packaging/verify-release-manifest.sh" \
  --manifest "$workdir/release.yaml" \
  --registry registry.example.com/project \
  --tool skopeo)"
grep -Fq 'verified 4 artifact(s); failures: 0' <<< "$output"

python3 - "$workdir/release.yaml" "$workdir/tags.yaml" <<'PY'
from pathlib import Path
import sys
import yaml
document = yaml.safe_load(Path(sys.argv[1]).read_text())
for artifact in document["artifacts"]:
    artifact.pop("digest", None)
Path(sys.argv[2]).write_text(yaml.safe_dump(document))
PY
printf '{"schemaVersion":2,"manifests":[{"digest":"%s","platform":{"os":"linux","architecture":"amd64"}}]}' "$amd64_digest" > "$workdir/package.json"
if PATH="$workdir/bin:$PATH" "$ROOT/scripts/open-packaging/verify-release-manifest.sh" \
  --manifest "$workdir/tags.yaml" --registry registry.example.com/project --tool skopeo > "$workdir/failure.log" 2>&1; then
  echo 'verification accepted a package missing arm64' >&2
  exit 1
fi
grep -Fq 'source index does not contain platform linux/arm64' "$workdir/failure.log"
printf '%s' "$runtime_manifest" > "$workdir/package.json"
printf '%s' "$chart_manifest" > "$workdir/runtime.json"
if PATH="$workdir/bin:$PATH" FAKE_RUNTIME_ARCH=amd64 "$ROOT/scripts/open-packaging/verify-release-manifest.sh" \
  --manifest "$workdir/tags.yaml" --registry registry.example.com/project --tool skopeo > "$workdir/failure.log" 2>&1; then
  echo 'verification accepted an amd64 runtime as arm64' >&2
  exit 1
fi
grep -Fq 'source platform mismatch: expected=linux/arm64 actual=linux/amd64' "$workdir/failure.log"

echo "release manifest skopeo verification test passed"
