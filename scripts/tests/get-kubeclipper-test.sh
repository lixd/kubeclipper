#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
workdir="$(mktemp -d -t kc-installer-test.XXXXXX)"
trap 'rm -rf "$workdir"' EXIT

mkdir -p "$workdir/bin" "$workdir/install"
printf 'kcctl fixture\n' > "$workdir/kcctl"

cat > "$workdir/bin/id" <<'EOF'
#!/usr/bin/env bash
printf '0\n'
EOF
cat > "$workdir/bin/uname" <<'EOF'
#!/usr/bin/env bash
if [[ "${1:-}" == "-m" ]]; then
  printf 'x86_64\n'
else
  printf 'Linux\n'
fi
EOF
cat > "$workdir/bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
saw_fail_and_follow=false
while [[ $# -gt 0 ]]; do
  case "$1" in
  -fL) saw_fail_and_follow=true; shift ;;
  -o) output=$2; shift 2 ;;
  http*) url=$1; shift ;;
  *) shift ;;
  esac
done
[[ "$saw_fail_and_follow" == true ]] || {
  echo "curl must use -fL" >&2
  exit 1
}
cp "$KC_INSTALLER_FIXTURE" "$output"
printf '%s\n' "$url" > "$KC_INSTALLER_URL_LOG"
EOF
cat > "$workdir/bin/tput" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$workdir/bin/id" "$workdir/bin/uname" "$workdir/bin/curl" "$workdir/bin/tput"

run_case() {
  local version=$1
  local expected_url=$2
  rm -f "$workdir/install/kcctl" "$workdir/url.log"
  PATH="$workdir/bin:/usr/bin:/bin" \
    KC_VERSION="$version" \
    KC_DOWNLOAD_URL="https://releases.example.test/kubeclipper" \
    KC_BIN_DIR="$workdir/install" \
    KC_INSTALLER_FIXTURE="$workdir/kcctl" \
    KC_INSTALLER_URL_LOG="$workdir/url.log" \
    bash "$ROOT/scripts/get-kubeclipper.sh" >/dev/null
  cmp "$workdir/kcctl" "$workdir/install/kcctl"
  [[ -x "$workdir/install/kcctl" ]]
  [[ "$(cat "$workdir/url.log")" == "$expected_url" ]]
}

run_case v2.0.0 "https://releases.example.test/kubeclipper/download/v2.0.0/kcctl-linux-amd64"
run_case latest "https://releases.example.test/kubeclipper/latest/download/kcctl-linux-amd64"

echo "get-kubeclipper test passed"
