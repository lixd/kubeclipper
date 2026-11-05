#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

python3 - "$ROOT/.github/workflows/release.yml" <<'PY'
import sys
import hashlib
import os
from pathlib import Path
import shlex
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

cli = jobs["build-cli"]
assert cli["needs"] == "manifest"
embedded = next(step["with"] for step in cli["steps"]
                if step.get("name") == "Embed verified release inventory")
assert embedded["name"] == "release-manifest-${{ github.run_id }}"
assert embedded["path"] == "pkg/delivery/releasemanifest/embedded"
qualification = yaml.safe_load((Path(sys.argv[1]).parent / "publish-oci-qualification.yml").read_text())
steps = qualification["jobs"]["sync-roundtrip"]["steps"]
build = next(step["run"] for step in steps if step.get("name") == "Build kcctl")
assert "pkg/delivery/releasemanifest/embedded/release-manifest.yaml" in build
assert any(step.get("name") == "Verify embedded export inventory" for step in steps)

release = jobs["github-release"]
stamp_step = next(step for step in release["steps"]
                  if step.get("name") == "Stamp installer release sources")
assert "vars.OSS_DOWNLOAD_URL" in workflow["env"]["OSS_DOWNLOAD_URL"]
assert "kubeclipper.oss-ap-southeast-1.aliyuncs.com/kc" in workflow["env"]["OSS_DOWNLOAD_URL"]
stamp = stamp_step["run"]
root = Path(sys.argv[1]).resolve().parents[2]
with tempfile.TemporaryDirectory() as tmp:
    Path(tmp, "release").mkdir()
    for repository, domestic, expected in (
        ("kubeclipper/kubeclipper", "https://downloads.example.com/kc", "https://downloads.example.com/kc"),
        ("fixture/repo", "https://downloads.example.com/kc/", "https://downloads.example.com/kc"),
    ):
        env = dict(os.environ, RUNNER_TEMP=tmp, GITHUB_REF_NAME="v9.8.7",
                   GITHUB_REPOSITORY=repository, OSS_DOWNLOAD_URL=domestic)
        subprocess.run(["bash", "-eu", "-c", stamp], cwd=root, env=env, check=True)
        published = Path(tmp, "release/get-kubeclipper.sh").read_text()
        assert 'DEFAULT_VERSION=v9.8.7' in published
        assert f'DEFAULT_DOWNLOAD_URL=https://github.com/{repository}/releases/download' in published
        assert f'DEFAULT_CN_DOWNLOAD_URL={shlex.quote(expected)}' in published
        subprocess.run(["bash", "-n", str(Path(tmp, "release/get-kubeclipper.sh"))], check=True)

build_release = next(step["run"] for step in cli["steps"] if step.get("name") == "Build kcctl")
with tempfile.TemporaryDirectory() as tmp:
    dest = Path(tmp)
    inventory = dest / "pkg/delivery/releasemanifest/embedded/release-manifest.yaml"
    inventory.parent.mkdir(parents=True)
    inventory.write_text("registries:\n  package: ghcr.io/fixture/kubeclipper/qualification-rev\n")
    log = dest / "build-flags"
    make = dest / "make"
    make.write_text('#!/usr/bin/env bash\nprintf "%s\\n%s\\n" "$GOLDFLAGS" "$KUBE_BUILD_PLATFORMS" > "$BUILD_LOG"\n')
    make.chmod(0o755)
    env = dict(os.environ, PATH=f"{tmp}:{os.environ['PATH']}", BUILD_LOG=str(log),
               GITHUB_REPOSITORY="fixture/repo", OSS_DOWNLOAD_URL="https://downloads.example.com/kc/")
    subprocess.run(["bash", "-eu", "-c", build_release], cwd=dest, env=env, check=True)
    flags, platforms = log.read_text().splitlines()
    for assignment in (
        "GlobalPackageRegistry=ghcr.io/fixture/kubeclipper/qualification-rev",
        "GlobalReleaseBaseURL=https://github.com/fixture/repo/releases/download",
        "CNReleaseBaseURL=https://downloads.example.com/kc",
    ):
        assert "github.com/kubeclipper/kubeclipper/pkg/delivery/source." + assignment in shlex.split(flags)
    assert platforms == "linux/amd64 linux/arm64 darwin/amd64 darwin/arm64"

oss = jobs["oss-release"]
assert oss["needs"] == "github-release"
assert "if" not in oss, "the shared installer entry requires OSS publishing"
preflight = next(step for step in jobs["prepare"]["steps"]
                 if step.get("name") == "Validate OSS release configuration")
assert "if" not in preflight
required = ("OSS_ENDPOINT", "OSS_BUCKET", "OSS_ACCESS_KEY", "OSS_ACCESS_SECRET")
assert all("secrets." + name in preflight["env"][name] for name in required)
env = dict(os.environ, GITHUB_REPOSITORY="fixture/repo",
           OSS_ENDPOINT="fixture-endpoint", OSS_BUCKET="fixture-bucket",
           OSS_ACCESS_KEY="fixture-key", OSS_ACCESS_SECRET="fixture-secret",
           OSS_DOWNLOAD_URL="https://downloads.example.com/kc")
subprocess.run(["bash", "-eu", "-c", preflight["run"]], env=env, check=True)
for missing in (*required, "OSS_DOWNLOAD_URL"):
    result = subprocess.run(["bash", "-eu", "-c", preflight["run"]],
                            env=dict(env, **{missing: ""}), capture_output=True)
    assert result.returncode != 0, f"missing {missing} must fail before publishing"
    assert missing.encode() in result.stderr
for invalid in ("http://downloads.example.com/kc", "https://user:secret@example.com/kc",
                "https://example.com/kc?token=value", "https://example.com/kc#anchor",
                "https://example.com/kc\nmalformed"):
    result = subprocess.run(["bash", "-eu", "-c", preflight["run"]],
                            env=dict(env, OSS_DOWNLOAD_URL=invalid), capture_output=True)
    assert result.returncode != 0, "invalid public download URL must fail before publishing"
upload = next(step["run"] for step in oss["steps"] if step.get("name") == "Upload release files and publish installer last")
# Execute the upload command with a recording ossutil, not a real OSS endpoint.
with tempfile.TemporaryDirectory() as tmp:
    dest = Path(tmp)
    (dest / "oss-release").mkdir()
    log = dest / "uploads"
    tool = dest / "ossutil"
    tool.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$UPLOAD_LOG"\n'
                    '[[ -z "${FAIL_UPLOAD:-}" || "$3" != "$FAIL_UPLOAD" ]]\n')
    tool.chmod(0o755)
    env = dict(os.environ, PATH=f"{tmp}:{os.environ['PATH']}", RUNNER_TEMP=tmp,
               GITHUB_REF_NAME="v9.8.7", OSS_BUCKET="fixture-bucket", UPLOAD_LOG=str(log))
    subprocess.run(["bash", "-eu", "-c", upload], env=env, check=True)
    commands = log.read_text().splitlines()
    assets = {"kcctl-linux-amd64", "kcctl-linux-arm64", "kcctl-darwin-amd64",
              "kcctl-darwin-arm64", "kcctl-checksums.txt",
              "release-manifest-v9.8.7.yaml", "release-manifest-v9.8.7.yaml.sha256",
              "get-kubeclipper.sh"}
    assert len(commands) == 9
    assert {line.split()[-1] for line in commands[:-1]} == {
        f"oss://fixture-bucket/kc/v9.8.7/{asset}" for asset in assets
    }
    assert commands[-1] == 'cp -f get-kubeclipper.sh oss://fixture-bucket/get-kubeclipper.sh'
    log.write_text("")
    result = subprocess.run(["bash", "-eu", "-c", upload],
                            env=dict(env, FAIL_UPLOAD="kcctl-linux-arm64"), capture_output=True)
    assert result.returncode != 0
    assert commands[-1] not in log.read_text().splitlines(), "failed upload must not update root installer"
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
    assert {args[i + 1] for i, value in enumerate(args) if value == "--pattern"} == assets

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
