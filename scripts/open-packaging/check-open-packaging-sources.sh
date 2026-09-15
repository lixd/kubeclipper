#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

cd "$ROOT"

patterns=(
  'oss\.kubeclipper\.io/packages'
  'OFFLINE_URL'
  '/data/tarball'
  '/data/tarballs'
  'scp .*tarball'
  '192\.168\.'
  'registry\.cn-'
  'aliyuncs\.com/.+99cloud'
  'sh-package'
)

failed=false
for pattern in "${patterns[@]}"; do
  if command -v rg >/dev/null 2>&1; then
    if rg -n "$pattern" scripts/open-packaging packaging .github \
      --glob '!*.md' \
      --glob '!scripts/open-packaging/check-open-packaging-sources.sh'; then
      failed=true
    fi
  else
    if find scripts/open-packaging packaging .github \
      -type f ! -name '*.md' ! -path '*/check-open-packaging-sources.sh' \
      -exec grep -nE "$pattern" {} +; then
      failed=true
    fi
  fi
done

if [[ "$failed" == true ]]; then
  echo "open packaging source check failed: internal/static-server dependency detected" >&2
  exit 1
fi

echo "open packaging source check passed"
