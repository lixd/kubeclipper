#!/usr/bin/env bash

set -euo pipefail

manifest=""
registry=""
arch=""
insecure=false
tool=""

usage() {
  cat <<'EOF'
Usage:
  verify-release-manifest.sh --manifest <file> --registry <prefix> [flags]

Checks the exact package, chart, and runtime image references declared by a
release manifest. It is an optional delivery/CI check and never blocks kcctl
cluster creation.

Flags:
  --manifest <file>       release-manifest.yaml.
  --registry <prefix>     Destination Registry prefix.
  --arch <amd64|arm64>    Verify only artifacts supporting this architecture.
  --tool <crane|skopeo>   Registry inspection tool. Default: auto-detect.
  --insecure              Use plain HTTP / skip TLS verification where supported.
  -h, --help              Show this help.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

need_value() {
  [[ $# -ge 2 && -n "$2" ]] || die "$1 requires a value"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --manifest) need_value "$@"; manifest="$2"; shift 2 ;;
  --registry) need_value "$@"; registry="${2%/}"; shift 2 ;;
  --arch) need_value "$@"; arch="$2"; shift 2 ;;
  --tool) need_value "$@"; tool="$2"; shift 2 ;;
  --insecure) insecure=true; shift ;;
  -h | --help) usage; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
done

[[ -f "$manifest" ]] || die "manifest not found: $manifest"
[[ -n "$registry" ]] || die "--registry is required"
case "$arch" in
"" | amd64 | arm64) ;;
*) die "--arch must be amd64 or arm64" ;;
esac
command -v python3 >/dev/null 2>&1 || die "python3 is required"

if [[ -z "$tool" ]]; then
  if command -v crane >/dev/null 2>&1; then
    tool=crane
  elif command -v skopeo >/dev/null 2>&1; then
    tool=skopeo
  else
    die "crane or skopeo is required"
  fi
fi
command -v "$tool" >/dev/null 2>&1 || die "$tool is required"

python3 - "$manifest" "$registry" "$arch" "$tool" "$insecure" <<'PY'
import hashlib
import json
import subprocess
import sys

try:
    import yaml
except ImportError:
    raise SystemExit("PyYAML is required; install python3-yaml or pyyaml")

manifest, registry, arch, tool, insecure = sys.argv[1:]
with open(manifest, "r", encoding="utf-8") as stream:
    document = yaml.safe_load(stream) or {}
if document.get("kind") != "ReleaseManifest":
    raise SystemExit("kind must be ReleaseManifest")
if tool not in ("crane", "skopeo"):
    raise SystemExit(f"unsupported tool: {tool}")


def inspect(reference, mode, platform=None):
    if tool == "crane":
        args = ["crane", mode]
        if insecure == "true":
            args.append("--insecure")
        if platform:
            args.extend(["--platform", platform])
        args.append(reference)
    else:
        args = ["skopeo"]
        if platform:
            os_name, architecture = platform.split("/", 1)
            args.extend(["--override-os", os_name, "--override-arch", architecture])
        args.extend(["inspect", "--raw" if mode == "manifest" else "--config", "--retry-times", "3"])
        if insecure == "true":
            args.append("--tls-verify=false")
        args.append("docker://" + reference)
    try:
        return subprocess.run(args, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout
    except subprocess.CalledProcessError as error:
        detail = error.stderr.decode("utf-8", errors="replace").strip() or str(error)
        raise ValueError(f"inspect {reference}: {detail}") from error


def verify(artifact, reference, platforms):
    raw = inspect(reference, "manifest")
    image = json.loads(raw)
    digest = "sha256:" + hashlib.sha256(raw).hexdigest()
    descriptors = image.get("manifests")
    indexed = isinstance(descriptors, list)
    if platforms and not indexed and len(platforms) > 1:
        raise ValueError("single image does not contain all requested platforms")
    for platform in platforms:
        if platform not in ("linux/amd64", "linux/arm64"):
            raise ValueError(f"unsupported platform {platform}")
        if indexed:
            os_name, architecture = platform.split("/", 1)
            child = next((item for item in descriptors
                          if (item.get("platform") or {}).get("os") == os_name
                          and (item.get("platform") or {}).get("architecture") == architecture), None)
            if child is None:
                raise ValueError(f"source index does not contain platform {platform}")
            if artifact["type"] == "runtime-image":
                digest = child["digest"]
        config = json.loads(inspect(reference, "config", platform))
        actual = f'{config.get("os", "")}/{config.get("architecture", "")}'
        # Data-only package configs use their index descriptors for platform metadata.
        if artifact["type"] == "package-image" and indexed and actual == "/":
            continue
        if actual != platform:
            raise ValueError(f"source platform mismatch: expected={platform} actual={actual}")
    expected = artifact.get("digest")
    if expected and expected != digest:
        raise ValueError(f"digest mismatch: expected={expected} actual={digest}")
    return digest


checked = 0
failed = 0
for index, artifact in enumerate(document.get("artifacts") or []):
    reference = f'{registry}/{str(artifact.get("target") or "").lstrip("/")}'
    try:
        if not artifact.get("target"):
            raise ValueError(f"artifact[{index}] target is required")
        artifact_type = artifact.get("type")
        platforms = artifact.get("platforms") or []
        if artifact_type == "runtime-image" and len(platforms) != 1:
            raise ValueError("runtime-image must declare exactly one platform")
        if artifact_type == "package-image" and not platforms:
            raise ValueError("package-image platforms are required")
        if arch and platforms:
            if f"linux/{arch}" not in platforms:
                continue
            platforms = [f"linux/{arch}"]
        digest = verify(artifact, reference, platforms)
        print(f"ok: {artifact_type} {reference}@{digest}")
        checked += 1
    except (ValueError, KeyError, TypeError, OSError) as error:
        print(f"invalid: {artifact.get('type')} {reference}: {error}", file=sys.stderr)
        failed += 1
print(f"verified {checked} artifact(s); failures: {failed}")
if failed:
    raise SystemExit(1)
PY
