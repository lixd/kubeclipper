#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

cat > "$workdir/images.lock" <<'EOF'
resource	version	arch	source	target
k8s	v1.36.1	amd64	registry.k8s.io/kube-apiserver:v1.36.1	unused
EOF

cat > "$workdir/crane" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
digest)
  case "$2" in
    registry.k8s.io/*) echo "${SOURCE_DIGEST:?}" ;;
    *) [[ -n "${TARGET_DIGEST:-}" ]] || { echo 'MANIFEST_UNKNOWN: manifest unknown' >&2; exit 1; }; echo "$TARGET_DIGEST" ;;
  esac
  ;;
copy) printf '%s\n' "$2 -> $3" >> "${COPY_LOG:?}" ;;
*) exit 2 ;;
esac
EOF
chmod +x "$workdir/crane"

export PATH="$workdir:$PATH"
export COPY_LOG="$workdir/copies"
export SOURCE_DIGEST="sha256:1111111111111111111111111111111111111111111111111111111111111111"

TARGET_DIGEST="$SOURCE_DIGEST" "$ROOT/scripts/open-packaging/push-runtime-images.sh" \
  --images-lock "$workdir/images.lock" --image-registry registry.example/kubeclipper --tool crane \
  > "$workdir/skip.out"
grep -Fq 'already matches source, skip' "$workdir/skip.out"
[[ ! -e "$COPY_LOG" ]] || { echo "matching target was unexpectedly copied" >&2; exit 1; }

export TARGET_DIGEST="sha256:2222222222222222222222222222222222222222222222222222222222222222"
if "$ROOT/scripts/open-packaging/push-runtime-images.sh" \
  --images-lock "$workdir/images.lock" --image-registry registry.example/kubeclipper --tool crane \
  > "$workdir/conflict.out" 2> "$workdir/conflict.err"; then
  echo "expected an existing different runtime tag to be rejected" >&2
  exit 1
fi
grep -Fq 'runtime image tag conflict' "$workdir/conflict.err"
[[ ! -e "$COPY_LOG" ]] || { echo "conflicting target was unexpectedly overwritten" >&2; exit 1; }

export TARGET_DIGEST_ERROR='UNAUTHORIZED: authentication required'
cat > "$workdir/crane" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
digest)
  case "$2" in
    registry.k8s.io/*) echo "${SOURCE_DIGEST:?}" ;;
    *) echo "${TARGET_DIGEST_ERROR:?}" >&2; exit 1 ;;
  esac
  ;;
copy) printf '%s\n' "$2 -> $3" >> "${COPY_LOG:?}" ;;
*) exit 2 ;;
esac
EOF
chmod +x "$workdir/crane"
if "$ROOT/scripts/open-packaging/push-runtime-images.sh" \
  --images-lock "$workdir/images.lock" --image-registry registry.example/kubeclipper --tool crane \
  > "$workdir/auth.out" 2> "$workdir/auth.err"; then
  echo "expected target inspection errors to stop publication" >&2
  exit 1
fi
grep -Fq 'cannot inspect target image' "$workdir/auth.err"
[[ ! -e "$COPY_LOG" ]] || { echo "target with an inspection error was unexpectedly copied" >&2; exit 1; }

unset TARGET_DIGEST_ERROR
cat > "$workdir/crane" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
digest)
  case "$2" in
    registry.k8s.io/*) echo "${SOURCE_DIGEST:?}" ;;
    *) echo 'MANIFEST_UNKNOWN: manifest unknown' >&2; exit 1 ;;
  esac
  ;;
copy) printf '%s\n' "$2 -> $3" >> "${COPY_LOG:?}" ;;
*) exit 2 ;;
esac
EOF
chmod +x "$workdir/crane"

unset TARGET_DIGEST
"$ROOT/scripts/open-packaging/push-runtime-images.sh" \
  --images-lock "$workdir/images.lock" --image-registry registry.example/kubeclipper --tool crane \
  > "$workdir/copy.out"
grep -Fq 'registry.k8s.io/kube-apiserver:v1.36.1 -> registry.example/kubeclipper/kube-apiserver:v1.36.1' "$COPY_LOG"

echo "push runtime image immutability test passed"
