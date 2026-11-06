#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

python3 - "$ROOT/.github/workflows/release.yml" <<'PY'
import sys
import hashlib
import http.server
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import threading

import yaml

with open(sys.argv[1], encoding="utf-8") as stream:
    workflow = yaml.safe_load(stream)

jobs = workflow["jobs"]
assert workflow["concurrency"] == {"group": "stable-release", "cancel-in-progress": False}
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
    assert platforms == "linux/amd64 linux/arm64"

cn = jobs["cn-registry"]
assert cn["needs"] == ["build-cli", "manifest"]
assert release["needs"] == ["build-cli", "manifest", "cn-registry"]
cn_registry = workflow["env"]["CN_PACKAGE_REGISTRY"]
assert f'CNPackageRegistry = "{cn_registry}"' in (root / "pkg/delivery/source/source.go").read_text()
cn_import = next(step["run"] for step in cn["steps"] if step.get("name") == "Import and verify domestic registry")
# Recording clients exercise workflow wiring and failure propagation without a registry.
with tempfile.TemporaryDirectory() as tmp:
    dest = Path(tmp)
    (dest / "cn-cli").mkdir()
    (dest / "cn-manifest").mkdir()
    (dest / "cn-manifest/release-manifest.yaml").write_text("manifest fixture")
    client = dest / "cn-cli/kcctl-linux-amd64"
    client.write_text('''#!/usr/bin/env python3
import json, os, pathlib, stat, sys
args = sys.argv[1:]
assert args[:2] == ["registry", "import"]
assert args[args.index("--arch") + 1] == "all"
assert os.environ["KCZONE"] == "global"
password = pathlib.Path(args[args.index("--target-password-file") + 1])
assert password.read_text() == "fixture-acr-password"
assert stat.S_IMODE(password.stat().st_mode) == 0o600
pathlib.Path(os.environ["CLI_LOG"]).write_text(json.dumps(args))
sys.exit(int(os.environ["FAIL_IMPORT"]))
''')
    (dest / "cn-cli/kcctl-linux-arm64").write_bytes(b"arm64 fixture")
    (dest / "cn-cli/kcctl-checksums.txt").write_text(''.join(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
        for path in (client, dest / "cn-cli/kcctl-linux-arm64")))
    validator = dest / "scripts/open-packaging/verify-release-manifest.sh"
    validator.parent.mkdir(parents=True)
    validator.write_text('''#!/usr/bin/env bash
set -eu
[[ ! -e "$DOCKER_CONFIG/config.json" ]]
printf '%s\\n' "$@" > "$VERIFY_LOG"
[[ "$FAIL_VERIFY" == 0 ]]
''')
    validator.chmod(0o755)
    env = dict(os.environ, RUNNER_TEMP=tmp, GITHUB_ACTOR="fixture-actor", GITHUB_TOKEN="fixture-token",
               ACR_USERNAME="fixture-user", ACR_PASSWORD="fixture-acr-password", CN_PACKAGE_REGISTRY=cn_registry,
               CLI_LOG=str(dest / "cli-log"), VERIFY_LOG=str(dest / "verify-log"))
    for fail_import, fail_verify in ((0, 0), (1, 0), (0, 1)):
        (dest / "verify-log").unlink(missing_ok=True)
        result = subprocess.run(["bash", "-eu", "-c", cn_import], cwd=tmp,
                                env=dict(env, FAIL_IMPORT=str(fail_import), FAIL_VERIFY=str(fail_verify)), capture_output=True)
        assert (result.returncode == 0) == (fail_import == fail_verify == 0), result.stderr
        args = json.loads((dest / "cli-log").read_text())
        assert args[args.index("--target") + 1] == cn_registry
        assert args[args.index("--source-username") + 1] == "fixture-actor"
        assert args[args.index("--target-username") + 1] == "fixture-user"
        assert (dest / "verify-log").exists() == (fail_import == 0)
        assert not list(dest.glob("acr-credentials.*")), "credentials must be removed after success or failure"

build_workflow = yaml.safe_load((root / ".github/workflows/build-kc.yml").read_text())
check_workflow = yaml.safe_load((root / ".github/workflows/check-build.yml").read_text())
for workflow_file in (build_workflow, check_workflow):
    build_step = next(step["run"] for step in workflow_file["jobs"]["build"]["steps"]
                      if step.get("name") == "Build")
    assert 'KUBE_BUILD_PLATFORMS="linux/amd64 linux/arm64"' in build_step
for package_step in (
    next(step["run"] for step in cli["steps"] if step.get("name") == "Package kcctl artifacts"),
    next(step["run"] for step in build_workflow["jobs"]["build"]["steps"]
         if step.get("name") == "Package kcctl-only artifacts"),
):
    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp, "dist")
        (dest / "linux_arm64").mkdir(parents=True)
        (dest / "kcctl").write_bytes(b"amd64 fixture")
        (dest / "linux_arm64/kcctl").write_bytes(b"arm64 fixture")
        (dest / "kcctl-darwin-amd64").write_bytes(b"stale fixture")
        subprocess.run(["bash", "-eu", "-c", package_step], cwd=tmp, check=True, capture_output=True)
        assert {line.split()[1] for line in (dest / "kcctl-checksums.txt").read_text().splitlines()} == {
            "kcctl-linux-amd64", "kcctl-linux-arm64"
        }

oss = jobs["oss-release"]
assert oss["needs"] == "github-release"
assert "if" not in oss, "the shared installer entry requires OSS publishing"
preflight = next(step for step in jobs["prepare"]["steps"]
                 if step.get("name") == "Validate OSS release configuration")
assert "if" not in preflight
required = ("OSS_ENDPOINT", "OSS_BUCKET", "OSS_ACCESS_KEY", "OSS_ACCESS_SECRET", "ACR_USERNAME", "ACR_PASSWORD")
assert all("secrets." + name in preflight["env"][name] for name in required)
env = dict(os.environ, GITHUB_REPOSITORY="fixture/repo",
           OSS_ENDPOINT="fixture-endpoint", OSS_BUCKET="fixture-bucket",
           OSS_ACCESS_KEY="fixture-key", OSS_ACCESS_SECRET="fixture-secret",
           ACR_USERNAME="fixture-user", ACR_PASSWORD="fixture-password",
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
upload = next(step["run"] for step in oss["steps"] if step.get("name") == "Upload versioned release files")
public_check = next(step["run"] for step in oss["steps"] if step.get("name") == "Verify public OSS release files")
promote = next(step["run"] for step in oss["steps"] if step.get("name") == "Publish root installer")
step_names = [step.get("name") for step in oss["steps"]]
assert step_names.index("Upload versioned release files") < step_names.index("Verify public OSS release files")
assert step_names.index("Verify public OSS release files") < step_names.index("Publish root installer")
assert all("if" not in step and not step.get("continue-on-error", False) for step in oss["steps"])
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
    assets = {"kcctl-linux-amd64", "kcctl-linux-arm64", "kcctl-checksums.txt",
              "release-manifest-v9.8.7.yaml", "release-manifest-v9.8.7.yaml.sha256",
              "get-kubeclipper.sh"}
    assert len(commands) == 6
    assert set(commands) == {
        f"cp -f {asset} oss://fixture-bucket/kc/v9.8.7/{asset}" for asset in assets
    }
    subprocess.run(["bash", "-eu", "-c", promote], env=env, check=True)
    root_upload = 'cp -f get-kubeclipper.sh oss://fixture-bucket/get-kubeclipper.sh'
    assert log.read_text().splitlines()[-1] == root_upload
    for failed in ("kcctl-linux-arm64", "release-manifest-v9.8.7.yaml.sha256"):
        log.write_text("")
        result = subprocess.run(["bash", "-eu", "-c", upload],
                                env=dict(env, FAIL_UPLOAD=failed), capture_output=True)
        assert result.returncode != 0
        assert root_upload not in log.read_text().splitlines(), "failed upload must not update root installer"
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

# A local HTTP fixture exercises the public download/install gate, including corrupt files.
with tempfile.TemporaryDirectory() as tmp:
    dest = Path(tmp)
    original = dest / "oss-release"
    original.mkdir()
    (dest / "release").mkdir()
    tools = dest / "tools"
    tools.mkdir()
    (tools / "uname").write_text('#!/usr/bin/env bash\ncase "$1" in -s) echo Linux;; -m) echo x86_64;; esac\n')
    (tools / "ossutil").write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$UPLOAD_LOG"\n')
    for path in tools.iterdir():
        path.chmod(0o755)
    payload = b'''#!/usr/bin/env bash
set -eu
printf '%s\\n' "$*" >> "$CLI_LOG"
case "$*" in
  'version -o json') printf 'kcctl version:\\n{"gitVersion":"%s"}\\n' "${REPORTED_VERSION:-v9.8.7}" ;;
  'registry import --target verification.invalid/kubeclipper --dry-run') ;;
  *) exit 1 ;;
esac
'''
    documents = {"kcctl-linux-amd64": payload, "kcctl-linux-arm64": b"arm64 fixture",
                 "release-manifest-v9.8.7.yaml": b"manifest fixture"}
    documents["kcctl-checksums.txt"] = ''.join(
        f"{hashlib.sha256(documents[name]).hexdigest()}  {name}\n"
        for name in ("kcctl-linux-amd64", "kcctl-linux-arm64")).encode()
    documents["release-manifest-v9.8.7.yaml.sha256"] = (
        f'{hashlib.sha256(documents["release-manifest-v9.8.7.yaml"]).hexdigest()}  release-manifest-v9.8.7.yaml\n'.encode())
    fault = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            name = self.path[len("/v9.8.7/"):]
            if not self.path.startswith("/v9.8.7/") or name not in documents or fault.get("missing") == name:
                self.send_error(404)
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"corrupt fixture" if fault.get("corrupt") == name else documents[name])

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        env = dict(os.environ, RUNNER_TEMP=tmp, GITHUB_REF_NAME="v9.8.7", GITHUB_REPOSITORY="fixture/repo",
                   OSS_DOWNLOAD_URL=f"http://127.0.0.1:{server.server_port}", OSS_BUCKET="fixture-bucket",
                   PATH=f"{tools}:{os.environ['PATH']}", UPLOAD_LOG=str(dest / "uploads"), CLI_LOG=str(dest / "cli-log"))
        subprocess.run(["bash", "-eu", "-c", stamp], cwd=root, env=env, check=True)
        documents["get-kubeclipper.sh"] = (dest / "release/get-kubeclipper.sh").read_text().replace(
            "https://github.com/fixture/repo/releases/download", env["OSS_DOWNLOAD_URL"]).encode()
        for name, body in documents.items():
            (original / name).write_bytes(body)
        for failure in ({}, {"missing": "kcctl-linux-arm64"}, {"corrupt": "kcctl-linux-amd64"},
                        {"corrupt": "release-manifest-v9.8.7.yaml"}, {"corrupt": "get-kubeclipper.sh"},
                        {"reported_version": "v0.0.0"}):
            fault.clear()
            fault.update(failure)
            (dest / "uploads").write_text("")
            (dest / "cli-log").write_text("")
            result = subprocess.run(["bash", "-eu", "-c", upload + "\n" + public_check + "\n" + promote],
                                    cwd=root, env=dict(env, REPORTED_VERSION=failure.get("reported_version", "v9.8.7")),
                                    capture_output=True)
            assert (result.returncode == 0) == (not failure), result.stderr
            assert (root_upload in (dest / "uploads").read_text().splitlines()) == (not failure)
            if not failure:
                assert (dest / "cli-log").read_text().splitlines() == [
                    "version -o json", "registry import --target verification.invalid/kubeclipper --dry-run"] * 2
            assert not list(dest.glob("oss-check.*")), "download workspace must be cleaned on success and failure"
    finally:
        server.shutdown()
        server.server_close()
        worker.join()

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
