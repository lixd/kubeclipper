#!/usr/bin/env bash
# Self-test for release-gate.sh: exercises the pass path and every block
# path against local fixtures, without git or network access.
#
# Usage: test-release-gate.sh [path-to-release-gate.sh]

set -euo pipefail

GATE="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/release-gate.sh}"
GATE_DIR="$(cd "$(dirname "$GATE")" && pwd)"
GATE="$GATE_DIR/$(basename "$GATE")"

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/release-gate-selftest.XXXXXXXX")"
trap 'rm -rf "$WORKDIR"' EXIT

CANDIDATE="e9d9afe0f2c1aabbccddeeff0011223344556677"
OTHER_CANDIDATE="0123456789abcdef0123456789abcdef01234567"
RELEASE_TAG="v2.0.3"

mkdir -p "$WORKDIR/docs/testing/acceptance"

write_manifest() {
  local file="$1" version="$2" revision="$3" bootstrap_revision="$4"
  cat >"$file" <<EOF
apiVersion: delivery.kubeclipper.io/v1alpha1
kind: ReleaseManifest
metadata:
  name: kubeclipper-resources
  version: $version
  sourceRevision: $revision
registries:
  package: ghcr.io/example/kubeclipper
  image: ghcr.io/example/kubeclipper
artifacts:
- component:
    kind: bootstrap
    name: kubeclipper
  source: ghcr.io/example/kubeclipper/qualification-abc123/kubeclipper:kubeclipper
  sourceRevision: $bootstrap_revision
- component:
    kind: k8s
    name: kubernetes
  sourceRevision: $bootstrap_revision
EOF
}

write_record() {
  local file="$1" candidate="$2" result="$3" manifest_sha="$4" release_tag="${5:-}"
  {
    echo "candidate_sha: $candidate"
    echo "result: $result"
    echo "release_manifest_sha256: $manifest_sha"
    if [[ -n "$release_tag" ]]; then
      echo "release_tag: $release_tag"
    fi
  } >"$file"
}

manifest_sha() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

PASS_COUNT=0
FAIL_COUNT=0

expect_block() {
  local case_name="$1"
  shift
  local output
  if output="$("$@" 2>&1)"; then
    echo "FAIL: $case_name (expected block, gate passed)" >&2
    echo "$output" >&2
    FAIL_COUNT=$((FAIL_COUNT + 1))
    return
  fi
  if ! grep -q "RELEASE GATE BLOCKED" <<<"$output"; then
    echo "FAIL: $case_name (blocked without the gate banner)" >&2
    echo "$output" >&2
    FAIL_COUNT=$((FAIL_COUNT + 1))
    return
  fi
  echo "ok: $case_name blocked"
  PASS_COUNT=$((PASS_COUNT + 1))
}

expect_pass() {
  local case_name="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "ok: $case_name passed"
    PASS_COUNT=$((PASS_COUNT + 1))
    return
  fi
  echo "FAIL: $case_name (expected pass)" >&2
  "$@" >&2 || true
  FAIL_COUNT=$((FAIL_COUNT + 1))
}

gate() {
  "$GATE" "$@"
}

GOOD_MANIFEST="$WORKDIR/good-manifest.yaml"
write_manifest "$GOOD_MANIFEST" "$RELEASE_TAG" "$CANDIDATE" "$CANDIDATE"
GOOD_SHA="$(manifest_sha "$GOOD_MANIFEST")"
GOOD_RECORD="$WORKDIR/docs/testing/acceptance/$CANDIDATE.yaml"
write_record "$GOOD_RECORD" "$CANDIDATE" passed "$GOOD_SHA"

# 1. positive path
expect_pass "full pass" \
  gate --release-tag "$RELEASE_TAG" --candidate-sha "$CANDIDATE" \
  --qualification-manifest "$GOOD_MANIFEST" \
  --acceptance-dir "$WORKDIR/docs/testing/acceptance"

# 2. acceptance record missing
expect_block "missing acceptance record" \
  gate --release-tag "$RELEASE_TAG" --candidate-sha "$OTHER_CANDIDATE" \
  --qualification-manifest "$GOOD_MANIFEST" \
  --acceptance-dir "$WORKDIR/docs/testing/acceptance"

# 3. record result: failed
FAILED_RECORD_DIR="$WORKDIR/failed/docs/testing/acceptance"
mkdir -p "$FAILED_RECORD_DIR"
write_record "$FAILED_RECORD_DIR/$CANDIDATE.yaml" "$CANDIDATE" failed "$GOOD_SHA"
expect_block "acceptance result failed" \
  gate --release-tag "$RELEASE_TAG" --candidate-sha "$CANDIDATE" \
  --qualification-manifest "$GOOD_MANIFEST" \
  --acceptance-dir "$WORKDIR/failed/docs/testing/acceptance"

# 4. record candidate mismatch
MISMATCH_RECORD_DIR="$WORKDIR/mismatch/docs/testing/acceptance"
mkdir -p "$MISMATCH_RECORD_DIR"
write_record "$MISMATCH_RECORD_DIR/$CANDIDATE.yaml" "$OTHER_CANDIDATE" passed "$GOOD_SHA"
expect_block "record candidate mismatch" \
  gate --release-tag "$RELEASE_TAG" --candidate-sha "$CANDIDATE" \
  --qualification-manifest "$GOOD_MANIFEST" \
  --acceptance-dir "$WORKDIR/mismatch/docs/testing/acceptance"

# 5. record manifest checksum mismatch
BAD_CHECKSUM_DIR="$WORKDIR/badchecksum/docs/testing/acceptance"
mkdir -p "$BAD_CHECKSUM_DIR"
write_record "$BAD_CHECKSUM_DIR/$CANDIDATE.yaml" "$CANDIDATE" passed \
  "0000000000000000000000000000000000000000000000000000000000000000"
expect_block "record manifest checksum mismatch" \
  gate --release-tag "$RELEASE_TAG" --candidate-sha "$CANDIDATE" \
  --qualification-manifest "$GOOD_MANIFEST" \
  --acceptance-dir "$WORKDIR/badchecksum/docs/testing/acceptance"

# 6. manifest sourceRevision mismatch
REV_MANIFEST="$WORKDIR/rev-manifest.yaml"
write_manifest "$REV_MANIFEST" "$RELEASE_TAG" "$OTHER_CANDIDATE" "$CANDIDATE"
expect_block "manifest sourceRevision mismatch" \
  gate --release-tag "$RELEASE_TAG" --candidate-sha "$CANDIDATE" \
  --qualification-manifest "$REV_MANIFEST" \
  --acceptance-record "$GOOD_RECORD"

# 7. manifest version mismatch
VERSION_MANIFEST="$WORKDIR/version-manifest.yaml"
write_manifest "$VERSION_MANIFEST" "v2.0.2" "$CANDIDATE" "$CANDIDATE"
expect_block "manifest version mismatch" \
  gate --release-tag "$RELEASE_TAG" --candidate-sha "$CANDIDATE" \
  --qualification-manifest "$VERSION_MANIFEST" \
  --acceptance-record "$GOOD_RECORD"

# 8. bootstrap artifact revision mismatch
BOOTSTRAP_MANIFEST="$WORKDIR/bootstrap-manifest.yaml"
write_manifest "$BOOTSTRAP_MANIFEST" "$RELEASE_TAG" "$CANDIDATE" "$OTHER_CANDIDATE"
expect_block "bootstrap artifact revision mismatch" \
  gate --release-tag "$RELEASE_TAG" --candidate-sha "$CANDIDATE" \
  --qualification-manifest "$BOOTSTRAP_MANIFEST" \
  --acceptance-record "$GOOD_RECORD"

# 9. non-stable release tag
expect_block "non-stable release tag" \
  gate --release-tag "v2.0.2" --candidate-sha "$CANDIDATE" \
  --qualification-manifest "$GOOD_MANIFEST" \
  --acceptance-record "$GOOD_RECORD"

# 10. record release_tag mismatch
TAGGED_RECORD_DIR="$WORKDIR/tagged/docs/testing/acceptance"
mkdir -p "$TAGGED_RECORD_DIR"
write_record "$TAGGED_RECORD_DIR/$CANDIDATE.yaml" "$CANDIDATE" passed "$GOOD_SHA" "v2.0.2"
expect_block "record release_tag mismatch" \
  gate --release-tag "$RELEASE_TAG" --candidate-sha "$CANDIDATE" \
  --qualification-manifest "$GOOD_MANIFEST" \
  --acceptance-dir "$WORKDIR/tagged/docs/testing/acceptance"

# 11. missing qualification manifest
expect_block "missing qualification manifest" \
  gate --release-tag "$RELEASE_TAG" --candidate-sha "$CANDIDATE" \
  --qualification-manifest "$WORKDIR/does-not-exist.yaml" \
  --acceptance-record "$GOOD_RECORD"

echo
echo "release-gate self-test: $PASS_COUNT passed, $FAIL_COUNT failed"
[[ "$FAIL_COUNT" -eq 0 ]]
