#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
revision="$(python3 - "$ROOT/packaging/resources.yaml" <<'PY'
import sys, yaml
with open(sys.argv[1], encoding='utf-8') as stream:
    print(yaml.safe_load(stream)['bootstrap']['consoleSourceRevision'])
PY
)"

mkdir -p "$tmp/input" "$tmp/work"
printf '{"commit_ref":"%s"}\n' "$revision" > "$tmp/input/version.json"
tar -czf "$tmp/good.tar.gz" -C "$tmp/input" .
printf '{"sourceRevision":"%040d"}\n' 0 > "$tmp/input/version.json"
tar -czf "$tmp/wrong.tar.gz" -C "$tmp/input" .
rm "$tmp/input/version.json"
tar -czf "$tmp/missing.tar.gz" -C "$tmp/input" .

check_archive() {
  local source=$1
  ROOT="$ROOT" workdir="$tmp/work" KC_CONSOLE_ARCHIVE="$source" KC_CONSOLE_EXPECTED_REVISION="$revision" \
    bash -c 'set -euo pipefail; source "$ROOT/scripts/open-packaging/bootstrap-packages/common.sh"; archive="$(prepare_console_archive)"; test -f "$archive"'
}
check_archive "$tmp/good.tar.gz"
if check_archive "$tmp/wrong.tar.gz"; then
  echo 'wrong console revision was accepted' >&2
  exit 1
fi
if check_archive "$tmp/missing.tar.gz"; then
  echo 'missing console revision was accepted' >&2
  exit 1
fi
printf 'console revision validation passed\n'
