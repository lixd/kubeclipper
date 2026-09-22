#!/usr/bin/env bash
# Release gate: block a stable release unless the exact candidate commit was
# qualified by CI and accepted by a recorded testing round.
#
# Checks (fail closed on any problem):
#   1. the release tag is a stable vX.Y.Z version;
#   2. the qualification manifest is a ReleaseManifest for that tag whose
#      sourceRevision and bootstrap/kubeclipper artifact revisions match the
#      candidate commit;
#   3. an acceptance record docs/testing/acceptance/<candidate-sha>.yaml
#      exists, records result: passed, and pins the sha256 of exactly this
#      qualification manifest.
#
# Git-side bindings (tag commit == candidate commit, existence of a successful
# qualification run for the candidate) are enforced by the release workflow
# before this script runs.
#
# Usage:
#   release-gate.sh --release-tag vX.Y.Z --candidate-sha <40-hex> \
#     --qualification-manifest <file> [--acceptance-record <file>] \
#     [--acceptance-dir <dir>] [--qualification-manifest-sha256 <hex>]
#
# Self-test: scripts/open-packaging/test-release-gate.sh

set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: release-gate.sh --release-tag vX.Y.Z --candidate-sha <sha> \
         --qualification-manifest <file> [--acceptance-record <file>] \
         [--acceptance-dir <dir>] [--qualification-manifest-sha256 <hex>]
USAGE
  exit 2
}

die() {
  echo "RELEASE GATE BLOCKED: $*" >&2
  exit 1
}

info() {
  echo "[release-gate] $*"
}

compute_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

RELEASE_TAG=""
CANDIDATE_SHA=""
QUALIFICATION_MANIFEST=""
ACCEPTANCE_RECORD=""
ACCEPTANCE_DIR=""
EXPECT_MANIFEST_SHA256=""

while [[ $# -gt 0 ]]; do
  case "$1" in
  --release-tag)
    RELEASE_TAG="${2:-}"
    shift 2
    ;;
  --candidate-sha)
    CANDIDATE_SHA="${2:-}"
    shift 2
    ;;
  --qualification-manifest)
    QUALIFICATION_MANIFEST="${2:-}"
    shift 2
    ;;
  --acceptance-record)
    ACCEPTANCE_RECORD="${2:-}"
    shift 2
    ;;
  --acceptance-dir)
    ACCEPTANCE_DIR="${2:-}"
    shift 2
    ;;
  --qualification-manifest-sha256)
    EXPECT_MANIFEST_SHA256="${2:-}"
    shift 2
    ;;
  *)
    usage
    ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [[ "$RELEASE_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  info "release tag $RELEASE_TAG is a stable version"
else
  die "release tag $RELEASE_TAG is not a stable vX.Y.Z version"
fi

if [[ "$CANDIDATE_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  info "candidate commit $CANDIDATE_SHA"
else
  die "candidate sha $CANDIDATE_SHA is not a full 40-hex git commit"
fi

if [[ -z "$QUALIFICATION_MANIFEST" ]]; then
  usage
fi
if [[ ! -f "$QUALIFICATION_MANIFEST" ]]; then
  die "qualification manifest $QUALIFICATION_MANIFEST does not exist; a successful qualification run for this commit is required"
fi
command -v python3 >/dev/null 2>&1 || die "python3 is required"
python3 -c "import yaml" >/dev/null 2>&1 || die "python3 PyYAML is required (apt-get install python3-yaml)"

check_manifest_contract() {
  python3 - "$1" "$2" "$3" <<'PY'
import sys

import yaml

manifest_path, release_tag, candidate_sha = sys.argv[1:4]


def die(msg):
    print(f"RELEASE GATE BLOCKED: {msg}")
    sys.exit(1)


try:
    with open(manifest_path, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
except Exception as exc:  # noqa: BLE001
    die(f"qualification manifest {manifest_path} is not valid YAML: {exc}")
if not isinstance(doc, dict):
    die("qualification manifest is not a YAML mapping")
if doc.get("kind") != "ReleaseManifest":
    die(f"qualification manifest kind = {doc.get('kind')!r}, want ReleaseManifest")
metadata = doc.get("metadata") or {}
if not isinstance(metadata, dict):
    die("qualification manifest metadata is not a mapping")
version = metadata.get("version")
if not isinstance(version, str) or version != release_tag:
    die(f"qualification manifest version {version!r} does not match release tag {release_tag!r}; "
        "re-run qualification on the release commit")
revision = metadata.get("sourceRevision")
if not isinstance(revision, str):
    die(f"qualification manifest metadata.sourceRevision is missing or malformed ({revision!r}); "
        "re-run qualification so provenance is recorded")
if revision != candidate_sha:
    die(f"qualification manifest sourceRevision {revision!r} does not match candidate commit {candidate_sha!r}")
bootstrap = [
    artifact
    for artifact in (doc.get("artifacts") or [])
    if isinstance(artifact, dict)
    and isinstance(artifact.get("component"), dict)
    and artifact["component"].get("kind") == "bootstrap"
    and artifact["component"].get("name") == "kubeclipper"
]
if not bootstrap:
    die("qualification manifest lists no bootstrap/kubeclipper artifact")
for artifact in bootstrap:
    got = artifact.get("sourceRevision")
    if not isinstance(got, str) or got != candidate_sha:
        die(f"bootstrap/kubeclipper artifact sourceRevision {got!r} does not match candidate commit {candidate_sha!r}")
print(f"[release-gate] manifest ok: version={version} "
      f"sourceRevision={revision[:12]} bootstrap_artifacts={len(bootstrap)}")
PY
}

MANIFEST_SHA256="$(compute_sha256 "$QUALIFICATION_MANIFEST")"

if [[ -n "$EXPECT_MANIFEST_SHA256" && "$EXPECT_MANIFEST_SHA256" != "$MANIFEST_SHA256" ]]; then
  die "qualification manifest checksum $MANIFEST_SHA256 does not match the expected $EXPECT_MANIFEST_SHA256"
fi

check_manifest_contract "$QUALIFICATION_MANIFEST" "$RELEASE_TAG" "$CANDIDATE_SHA"
info "qualification manifest checksum $MANIFEST_SHA256"

record_value() {
  awk -v key="$1" '
    $0 ~ "^"key":" {
      sub("^"key":[[:space:]]*", "")
      sub(/[[:space:]]+$/, "")
      print
      exit
    }' "$2"
}

if [[ -z "$ACCEPTANCE_RECORD" ]]; then
  if [[ -z "$ACCEPTANCE_DIR" ]]; then
    ACCEPTANCE_DIR="$REPO_ROOT/docs/testing/acceptance"
  fi
  ACCEPTANCE_RECORD="$ACCEPTANCE_DIR/$CANDIDATE_SHA.yaml"
fi

if [[ ! -f "$ACCEPTANCE_RECORD" ]]; then
  die "acceptance record $ACCEPTANCE_RECORD does not exist; the candidate must pass a recorded testing round before release (see docs/testing/acceptance/README.md)"
fi

RECORD_CANDIDATE="$(record_value candidate_sha "$ACCEPTANCE_RECORD")"
[[ "$RECORD_CANDIDATE" =~ ^[0-9a-f]{40}$ ]] || die "acceptance record $ACCEPTANCE_RECORD has no valid candidate_sha"
[[ "$RECORD_CANDIDATE" == "$CANDIDATE_SHA" ]] || die "acceptance record $ACCEPTANCE_RECORD covers candidate $RECORD_CANDIDATE, not this release candidate $CANDIDATE_SHA"

RECORD_RESULT="$(record_value result "$ACCEPTANCE_RECORD")"
case "$RECORD_RESULT" in
passed) ;;
failed)
  die "acceptance record $ACCEPTANCE_RECORD records result: failed; fix the reported issues and re-run acceptance"
  ;;
"")
  die "acceptance record $ACCEPTANCE_RECORD has no result field (want: passed)"
  ;;
*)
  die "acceptance record $ACCEPTANCE_RECORD records result: $RECORD_RESULT, want passed"
  ;;
esac

RECORD_TAG="$(record_value release_tag "$ACCEPTANCE_RECORD")"
if [[ -n "$RECORD_TAG" && "$RECORD_TAG" != "$RELEASE_TAG" ]]; then
  die "acceptance record $ACCEPTANCE_RECORD was taken for release $RECORD_TAG, not $RELEASE_TAG"
fi

RECORD_MANIFEST_SHA256="$(record_value release_manifest_sha256 "$ACCEPTANCE_RECORD")"
[[ "$RECORD_MANIFEST_SHA256" =~ ^[0-9a-f]{64}$ ]] || die "acceptance record $ACCEPTANCE_RECORD has no valid release_manifest_sha256 (64-hex)"
[[ "$RECORD_MANIFEST_SHA256" == "$MANIFEST_SHA256" ]] || die "acceptance record pins qualification manifest $RECORD_MANIFEST_SHA256 but this release would publish $MANIFEST_SHA256; re-run qualification and acceptance on this exact build"

info "acceptance record ok: result=passed candidate=$CANDIDATE_SHA manifest_sha256=${MANIFEST_SHA256:0:12}"
echo "RELEASE GATE PASSED: $RELEASE_TAG ($CANDIDATE_SHA) may publish"
