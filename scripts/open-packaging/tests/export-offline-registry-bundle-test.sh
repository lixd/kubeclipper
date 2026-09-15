#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
workdir="$(mktemp -d -t kc-export-bundle-test.XXXXXX)"
trap 'rm -rf "$workdir"' EXIT

mkdir -p "$workdir/bin"
cat > "$workdir/bin/skopeo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
source_ref="${@: -2:1}"
destination="${@: -1}"
printf '%s\n' "$source_ref" >> "$SKOPEO_LOG"
case "$destination" in
dir:*)
  output="${destination#dir:}"
  mkdir -p "$output"
  printf '{}\n' > "$output/manifest.json"
  ;;
*)
  echo "unexpected fake skopeo destination: $destination" >&2
  exit 1
  ;;
esac
EOF
chmod +x "$workdir/bin/skopeo"

digest="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
cat > "$workdir/release-manifest.yaml" <<EOF
apiVersion: delivery.kubeclipper.io/v1alpha1
kind: ReleaseManifest
metadata:
  name: export-test
  version: v1.0.0
artifacts:
  - type: runtime-image
    component:
      kind: test
      name: image
      version: v1
    source: registry.example.com:5000/team/image:v1
    target: team/image:v1
    digest: $digest
    platforms:
      - linux/amd64
EOF

export SKOPEO_LOG="$workdir/skopeo.log"
PATH="$workdir/bin:$PATH" \
  "$ROOT/scripts/open-packaging/export-offline-registry-bundle.sh" \
  --manifest "$workdir/release-manifest.yaml" \
  --output "$workdir/bundle.tar.gz" \
  --arch amd64

expected="docker://registry.example.com:5000/team/image@$digest"
actual="$(cat "$SKOPEO_LOG")"
[[ "$actual" == "$expected" ]] || {
  echo "unexpected digest-pinned source: $actual" >&2
  echo "expected: $expected" >&2
  exit 1
}
tar -tzf "$workdir/bundle.tar.gz" | grep -q 'kubeclipper-offline-registry-bundle/bundle-artifacts.tsv'
echo "export offline Registry bundle test passed"
