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
