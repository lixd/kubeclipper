#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
source "$ROOT/scripts/open-packaging/bootstrap-packages/common.sh"

work="$(mktemp -d -t kc-bootstrap-version-test.XXXXXX)"
trap 'rm -rf "$work"' EXIT

version=v2.0.0
arch="$(go env GOARCH)"
KC_SOURCE_REVISION="$(git -C "$ROOT" rev-parse HEAD)"
KUBE_GIT_TREE_STATE=clean
export KC_SOURCE_REVISION KUBE_GIT_TREE_STATE

ldflags="$(kubeclipper_build_ldflags)"
grep -Fq "gitCommit=$KC_SOURCE_REVISION" <<<"$ldflags"
grep -Fq "gitTreeState=clean" <<<"$ldflags"
grep -Fq "gitVersion=$version" <<<"$ldflags"

(
  cd "$ROOT"
  go build -ldflags "$ldflags" -o "$work/kubeclipper-server" ./cmd/kubeclipper-server
)
verify_core_binary_metadata "$work/kubeclipper-server"

dirty_ldflags="${ldflags/gitTreeState=clean/gitTreeState=dirty}"
(
  cd "$ROOT"
  go build -ldflags "$dirty_ldflags" -o "$work/kubeclipper-server-dirty" ./cmd/kubeclipper-server
)
if (verify_core_binary_metadata "$work/kubeclipper-server-dirty" >/dev/null 2>&1); then
  echo "dirty bootstrap binary unexpectedly passed metadata verification" >&2
  exit 1
fi

grep -Fq 'fetch-depth: 0' "$ROOT/.github/workflows/_publish-oci-component.yml"
# shellcheck disable=SC2016 # Match the workflow's literal shell variable references.
grep -Fq 'go build -o "$tools_dir/oci-publish"' "$ROOT/.github/workflows/_publish-oci-component.yml"
# shellcheck disable=SC2016 # Match the workflow's literal shell variable references.
grep -Fq 'KC_OCI_PUBLISH_BIN=$tools_dir/oci-publish' "$ROOT/.github/workflows/_publish-oci-component.yml"
if grep -Fq 'go build -o bin/oci-publish' "$ROOT/.github/workflows/_publish-oci-component.yml"; then
  echo "publish workflow still writes build tools into the source tree" >&2
  exit 1
fi

echo "bootstrap version metadata test passed"
