#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
workdir="$(mktemp -d -t kc-release-skopeo.XXXXXX)"
trap 'rm -rf "$workdir"' EXIT

mkdir -p "$workdir/bin"
package_manifest='{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}'
chart_manifest='{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{},"layers":[]}'
amd64_digest="sha256:$(printf 'a%.0s' {1..64})"
arm64_digest="sha256:$(printf 'b%.0s' {1..64})"
runtime_manifest="$(printf '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"digest":"%s","platform":{"os":"linux","architecture":"amd64"}},{"digest":"%s","platform":{"os":"linux","architecture":"arm64"}}]}' "$amd64_digest" "$arm64_digest")"

printf '%s' "$package_manifest" > "$workdir/package.json"
printf '%s' "$chart_manifest" > "$workdir/chart.json"
printf '%s' "$runtime_manifest" > "$workdir/runtime.json"
package_digest="sha256:$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())' "$workdir/package.json")"
chart_digest="sha256:$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())' "$workdir/chart.json")"

cat > "$workdir/bin/skopeo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == inspect && " $* " == *" --raw "* && " $* " == *" --retry-times 3 "* ]]
ref="${@: -1}"
case "$ref" in
*/package:v1) cat "$FAKE_PACKAGE_MANIFEST" ;;
*/chart:v1) cat "$FAKE_CHART_MANIFEST" ;;
*/runtime:v1) cat "$FAKE_RUNTIME_MANIFEST" ;;
*) exit 1 ;;
esac
EOF
chmod +x "$workdir/bin/skopeo"

cat > "$workdir/release.yaml" <<EOF
kind: ReleaseManifest
artifacts:
- type: package-image
  target: package:v1
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

echo "release manifest skopeo verification test passed"
