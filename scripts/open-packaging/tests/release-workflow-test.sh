#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

python3 - "$ROOT/.github/workflows/release.yml" <<'PY'
import sys

import yaml

with open(sys.argv[1], encoding="utf-8") as stream:
    workflow = yaml.safe_load(stream)

jobs = workflow["jobs"]
assert workflow["permissions"]["packages"] == "read"
assert all(job.get("permissions", {}).get("packages") != "write" for job in jobs.values())
assert "publish" not in jobs, "stable release must not republish immutable package tags"

gate = jobs["release-gate"]
assert gate["outputs"]["qualification_run_id"] == "${{ steps.qualification.outputs.run_id }}"
gate_download = next(
    step["with"]
    for step in gate["steps"]
    if step.get("uses", "").startswith("actions/download-artifact@")
)
assert gate_download["pattern"] == "oci-release-manifest-*"
assert gate_download["run-id"] == "${{ steps.qualification.outputs.run_id }}"

manifest = jobs["manifest"]
assert manifest["needs"] == "release-gate"
assert manifest["permissions"]["packages"] == "read"
assert manifest["env"]["REGISTRY_PREFIX"] == (
    "ghcr.io/${{ github.repository_owner }}/kubeclipper/qualification-${{ github.sha }}"
)
manifest_download = next(
    step["with"]
    for step in manifest["steps"]
    if step.get("uses", "").startswith("actions/download-artifact@")
)
assert manifest_download["pattern"] == "oci-release-manifest-*"
assert manifest_download["run-id"] == "${{ needs.release-gate.outputs.qualification_run_id }}"

print("release workflow provenance and immutable-tag checks passed")
PY
