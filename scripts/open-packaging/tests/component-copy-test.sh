#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
workdir="$(mktemp -d -t kc-component-copy.XXXXXX)"
trap 'rm -rf "$workdir"' EXIT
mkdir -p "$workdir/bin"
export COPY_TEST_DIR="$workdir"

cat > "$workdir/bin/crane" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
digest="sha256:$(printf '%064d' 1)"
case "$1" in
digest)
  if [[ "$2" == canonical/* || -f "$COPY_TEST_DIR/copied" ]]; then
    printf '%s\n' "$digest"
  elif [[ "$COPY_TEST_MODE" == conflict ]]; then
    printf 'sha256:%064d\n' 2
  else
    echo 'MANIFEST_UNKNOWN: 404 Not Found' >&2
    exit 1
  fi
  ;;
manifest)
  echo '{"manifests":[{"platform":{"os":"linux","architecture":"amd64"}},{"platform":{"os":"linux","architecture":"arm64"}}]}'
  ;;
config)
  printf '{"config":{"Labels":{"org.opencontainers.image.title":"bootstrap/registry","org.opencontainers.image.version":"3.1.1","org.opencontainers.image.source":"https://github.com/example/kubeclipper","org.opencontainers.image.revision":"%040d"}}}\n' 1
  ;;
copy)
  count=0
  [[ ! -f "$COPY_TEST_DIR/count" ]] || read -r count < "$COPY_TEST_DIR/count"
  count=$((count + 1))
  echo "$count" > "$COPY_TEST_DIR/count"
  if [[ "$COPY_TEST_MODE" == permanent || "$count" -lt 3 ]]; then
    echo 'DENIED: requested access to the resource is denied' >&2
    exit 1
  fi
  touch "$COPY_TEST_DIR/copied"
  ;;
*) exit 2 ;;
esac
EOF
printf '#!/usr/bin/env bash\nexit 0\n' > "$workdir/bin/sleep"
chmod +x "$workdir/bin/"*
export PATH="$workdir/bin:$PATH"
export KC_SOURCE_URL=https://github.com/example/kubeclipper

for mode in transient permanent conflict; do
  rm -f "$workdir/copied" "$workdir/count"
  export COPY_TEST_MODE="$mode"
  status=0
  bash "$ROOT/scripts/open-packaging/publish-component-release.sh" \
    bootstrap-registry canonical target 3.1.1 > "$workdir/$mode.log" 2>&1 || status=$?
  if [[ "$mode" == transient ]]; then
    [[ "$status" == 0 && -f "$workdir/copied" && "$(cat "$workdir/count")" == 3 ]]
  elif [[ "$mode" == permanent ]]; then
    [[ "$status" != 0 && ! -f "$workdir/copied" && "$(cat "$workdir/count")" == 3 ]]
  else
    [[ "$status" != 0 && ! -f "$workdir/count" ]]
  fi
done

echo "component copy retry, failure and tag conflict checks passed"

# Run the top-level publisher with registry and bootstrap-builder stubs. These
# contract checks verify write destinations and promotion ordering.
mkdir -p "$workdir/publisher"
cp "$ROOT/scripts/open-packaging/publish-component-release.sh" "$workdir/publisher/"
export KC_SOURCE_REVISION="$(printf '%040d' 1)"
export COPY_TEST_VERSION=v1.6.1-test

cat > "$workdir/bin/crane" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
repository=package-bootstrap-console
canonical="canonical/$repository:$COPY_TEST_VERSION"
built="$COPY_TEST_BUILD_PREFIX/$repository:$COPY_TEST_VERSION"
case "$1" in
digest)
  if [[ "$2" == "$built" && -f "$COPY_TEST_DIR/platforms" ]]; then
    printf 'sha256:%064d\n' "$(grep -c . "$COPY_TEST_DIR/platforms")"
  elif [[ "$2" == "$canonical" && -f "$COPY_TEST_DIR/promoted" ]]; then
    printf 'sha256:%064d\n' 2
  else
    echo 'MANIFEST_UNKNOWN: 404 Not Found' >&2
    exit 1
  fi
  ;;
manifest)
  echo "manifest $2" >> "$COPY_TEST_DIR/events"
  python3 - "$COPY_TEST_DIR/platforms" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    architectures = stream.read().splitlines()
print(json.dumps({"manifests": [
    {"platform": {"os": "linux", "architecture": arch}}
    for arch in architectures
]}))
PY
  ;;
config)
  [[ "$2" == --platform ]]
  echo "config $3 $4" >> "$COPY_TEST_DIR/events"
  printf '{"config":{"Labels":{"org.opencontainers.image.title":"bootstrap/console","org.opencontainers.image.version":"%s","org.opencontainers.image.source":"%s","org.opencontainers.image.revision":"%s"}}}\n' \
    "$COPY_TEST_VERSION" "$KC_SOURCE_URL" "$KC_SOURCE_REVISION"
  ;;
copy)
  echo "copy $2 $3" >> "$COPY_TEST_DIR/events"
  if [[ "$COPY_TEST_TARGET_PREFIX" != canonical || "$3" != "$canonical" ]]; then
    echo 'DENIED: canonical package repository is read-only' >&2
    exit 1
  fi
  [[ "$(grep -c . "$COPY_TEST_DIR/platforms")" == 2 ]]
  for arch in amd64 arm64; do
    grep -Fxq "config linux/$arch $2" "$COPY_TEST_DIR/events"
  done
  touch "$COPY_TEST_DIR/promoted"
  ;;
*) exit 2 ;;
esac
STUB

cat > "$workdir/publisher/publish-bootstrap-console.sh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
prefix=""
version=""
arch=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry-prefix) prefix=$2; shift 2 ;;
    --version) version=$2; shift 2 ;;
    --arch) arch=$2; shift 2 ;;
    *) exit 2 ;;
  esac
done
[[ "$prefix" == "$COPY_TEST_BUILD_PREFIX" && "$version" == "$COPY_TEST_VERSION" ]]
[[ "$arch" == amd64 || "$arch" == arm64 ]]
echo "build $arch $prefix" >> "$COPY_TEST_DIR/events"
printf '%s\n' "$arch" >> "$COPY_TEST_DIR/platforms"
STUB
chmod +x "$workdir/bin/crane" "$workdir/publisher/publish-bootstrap-console.sh"

for target_prefix in candidate canonical; do
  rm -f "$workdir/platforms" "$workdir/promoted" "$workdir/events"
  export COPY_TEST_TARGET_PREFIX="$target_prefix"
  export COPY_TEST_BUILD_PREFIX="$target_prefix"
  if [[ "$target_prefix" == canonical ]]; then
    export COPY_TEST_BUILD_PREFIX="canonical/qualification-$KC_SOURCE_REVISION"
  fi
  bash "$workdir/publisher/publish-component-release.sh" \
    bootstrap-console canonical "$target_prefix" "$COPY_TEST_VERSION" > "$workdir/fresh-$target_prefix.log" 2>&1
  [[ "$(cat "$workdir/platforms")" == $'amd64\narm64' ]]
  [[ "$(grep -c '^build ' "$workdir/events")" == 2 ]]
  if [[ "$target_prefix" == canonical ]]; then
    [[ -f "$workdir/promoted" && "$(grep -c '^copy ' "$workdir/events")" == 1 ]]
    grep -Fq "copy $COPY_TEST_BUILD_PREFIX/package-bootstrap-console@" "$workdir/events"
  else
    [[ ! -f "$workdir/promoted" ]]
    if grep -q '^copy ' "$workdir/events"; then
      echo 'isolated candidate unexpectedly copied into another repository' >&2
      exit 1
    fi
  fi
done

echo "component candidate isolation and verified canonical promotion checks passed"
