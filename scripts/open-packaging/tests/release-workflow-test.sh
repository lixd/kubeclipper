#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

python3 - "$ROOT/.github/workflows/release.yml" <<'PY'
import sys
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile

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

release = jobs["github-release"]
stamp = next(step["run"] for step in release["steps"] if step.get("name") == "Stamp installer version")
root = Path(sys.argv[1]).resolve().parents[2]
with tempfile.TemporaryDirectory() as tmp:
    Path(tmp, "release").mkdir()
    env = dict(os.environ, RUNNER_TEMP=tmp, GITHUB_REF_NAME="v9.8.7")
    subprocess.run(["bash", "-eu", "-c", stamp], cwd=root, env=env, check=True)
    published = Path(tmp, "release/get-kubeclipper.sh").read_text()
    assert 'DEFAULT_VERSION="v9.8.7"' in published

oss = jobs["oss-release"]
assert oss["needs"] == "github-release"
assert oss["if"] == "github.repository == 'kubeclipper/kubeclipper'"
upload = next(step["run"] for step in oss["steps"] if step.get("name") == "Upload release files and publish installer last")
# Execute the upload command with a recording ossutil, not a real OSS endpoint.
with tempfile.TemporaryDirectory() as tmp:
    dest = Path(tmp)
    (dest / "oss-release").mkdir()
    log = dest / "uploads"
    tool = dest / "ossutil"
    tool.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$UPLOAD_LOG"\n')
    tool.chmod(0o755)
    env = dict(os.environ, PATH=f"{tmp}:{os.environ['PATH']}", RUNNER_TEMP=tmp,
               GITHUB_REF_NAME="v9.8.7", OSS_BUCKET="fixture-bucket", UPLOAD_LOG=str(log))
    subprocess.run(["bash", "-eu", "-c", upload], env=env, check=True)
    commands = log.read_text().splitlines()
    assets = {"kcctl-linux-amd64", "kcctl-linux-arm64", "kcctl-darwin-amd64",
              "kcctl-darwin-arm64", "kcctl-checksums.txt",
              "release-manifest-v9.8.7.yaml", "release-manifest-v9.8.7.yaml.sha256"}
    assert len(commands) == 8
    assert {line.split()[-1] for line in commands[:-1]} == {
        f"oss://fixture-bucket/kc/v9.8.7/{asset}" for asset in assets
    }
    assert commands[-1] == 'cp -f get-kubeclipper.sh oss://fixture-bucket/get-kubeclipper.sh'
# Both files must be requested from the same published GitHub release.
download = next(step["run"] for step in oss["steps"] if step.get("name") == "Download the published GitHub assets")
with tempfile.TemporaryDirectory() as tmp:
    log = Path(tmp, "downloads")
    gh = Path(tmp, "gh")
    gh.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$@" > "$DOWNLOAD_LOG"\n')
    gh.chmod(0o755)
    env = dict(os.environ, PATH=f"{tmp}:{os.environ['PATH']}", RUNNER_TEMP=tmp,
               GITHUB_REF_NAME="v9.8.7", GITHUB_REPOSITORY="fixture/repo", DOWNLOAD_LOG=str(log))
    subprocess.run(["bash", "-eu", "-c", download], env=env, check=True)
    args = log.read_text().splitlines()
    assert {args[i + 1] for i, value in enumerate(args) if value == "--pattern"} == assets | {"get-kubeclipper.sh"}

verify = next(step["run"] for step in oss["steps"] if step.get("name") == "Verify release checksums")
with tempfile.TemporaryDirectory() as tmp:
    dest = Path(tmp, "oss-release")
    dest.mkdir()
    binary = b"kcctl fixture"
    (dest / "kcctl-linux-amd64").write_bytes(binary)
    (dest / "kcctl-checksums.txt").write_text(
        f"{hashlib.sha256(binary).hexdigest()}  kcctl-linux-amd64\n")
    manifest = dest / "release-manifest-v9.8.7.yaml"
    manifest.write_bytes(b"release manifest fixture")
    checksum = dest / (manifest.name + ".sha256")
    checksum.write_text(f"{hashlib.sha256(manifest.read_bytes()).hexdigest()}  {manifest.name}\n")
    env = dict(os.environ, RUNNER_TEMP=tmp, GITHUB_REF_NAME="v9.8.7")
    result = subprocess.run(["bash", "-eu", "-c", verify], env=env, capture_output=True)
    assert result.returncode == 0, result.stderr
    manifest.write_bytes(b"corrupt fixture")
    result = subprocess.run(["bash", "-eu", "-c", verify], env=env, capture_output=True)
    assert result.returncode != 0, "corrupt manifest must fail before OSS upload"
    checksum.unlink()
    result = subprocess.run(["bash", "-eu", "-c", verify], env=env, capture_output=True)
    assert result.returncode != 0, "missing manifest checksum must fail before OSS upload"
assert not (root / ".github/workflows/sync-script.yml").exists()
print("release workflow provenance, installer stamping and OSS upload checks passed")
PY
