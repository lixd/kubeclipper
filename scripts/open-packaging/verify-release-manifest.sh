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

refs_file="$(mktemp -t kc-release-verify.XXXXXX)"
trap 'rm -f "$refs_file"' EXIT
python3 - "$manifest" "$registry" "$arch" > "$refs_file" <<'PY'
import sys
try:
    import yaml
except ImportError:
    raise SystemExit("PyYAML is required; install python3-yaml or pyyaml")

manifest, registry, arch = sys.argv[1:]
with open(manifest, "r", encoding="utf-8") as stream:
    document = yaml.safe_load(stream) or {}
if document.get("kind") != "ReleaseManifest":
    raise SystemExit("kind must be ReleaseManifest")
for index, artifact in enumerate(document.get("artifacts") or []):
    target = str(artifact.get("target") or "").lstrip("/")
    if not target:
        raise SystemExit(f"artifact[{index}] target is required")
    platforms = [str(value) for value in artifact.get("platforms") or []]
    if arch and platforms and f"linux/{arch}" not in platforms:
        continue
    digest = str(artifact.get("digest") or "")
    artifact_type = str(artifact.get("type") or "")
    platform = "-"
    if artifact_type == "runtime-image":
        if len(platforms) != 1:
            raise SystemExit(f"artifact[{index}] runtime-image must declare exactly one platform")
        platform = platforms[0]
    print(f"{artifact_type}\t{registry}/{target}\t{digest}\t{platform}")
PY

checked=0
failed=0
while IFS=$'\t' read -r type ref expected_digest platform; do
  [[ -n "$ref" ]] || continue
  actual_digest=""
  case "$tool" in
  crane)
    args=(digest)
    [[ "$insecure" == false ]] || args+=(--insecure)
    if [[ "$type" == "runtime-image" ]]; then
      args+=(--platform "$platform")
    fi
    if ! actual_digest="$(crane "${args[@]}" "$ref" 2>/dev/null)"; then
      echo "missing: $type $ref" >&2
      failed=$((failed + 1))
      continue
    fi
    ;;
  skopeo)
    if [[ "$type" == "helm-chart" ]]; then
      if ! command -v crane >/dev/null 2>&1; then
        echo "unsupported: skopeo cannot inspect Helm OCI chart $ref; install crane" >&2
        failed=$((failed + 1))
        continue
      fi
      args=(digest)
      [[ "$insecure" == false ]] || args+=(--insecure)
      if ! actual_digest="$(crane "${args[@]}" "$ref" 2>/dev/null)"; then
        echo "missing: $type $ref" >&2
        failed=$((failed + 1))
        continue
      fi
      if [[ -n "$expected_digest" && "$expected_digest" != "$actual_digest" ]]; then
        echo "digest mismatch: $ref expected=$expected_digest actual=$actual_digest" >&2
        failed=$((failed + 1))
        continue
      fi
      echo "ok: $type $ref@$actual_digest"
      checked=$((checked + 1))
      continue
    fi
    args=(inspect --format '{{.Digest}}')
    [[ "$insecure" == false ]] || args+=(--tls-verify=false)
    if [[ "$type" == "runtime-image" ]]; then
      args+=(--override-os "${platform%%/*}" --override-arch "${platform#*/}")
    fi
    if ! actual_digest="$(skopeo "${args[@]}" "docker://$ref" 2>/dev/null)"; then
      echo "missing: $type $ref" >&2
      failed=$((failed + 1))
      continue
    fi
    ;;
  *) die "unsupported tool: $tool" ;;
  esac
  if [[ -n "$expected_digest" && "$expected_digest" != "$actual_digest" ]]; then
    echo "digest mismatch: $ref expected=$expected_digest actual=$actual_digest" >&2
    failed=$((failed + 1))
    continue
  fi
  echo "ok: $type $ref@$actual_digest"
  checked=$((checked + 1))
done < "$refs_file"

echo "verified $checked artifact(s); failures: $failed"
[[ "$failed" -eq 0 ]]
