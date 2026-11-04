#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
source "$ROOT/scripts/open-packaging/resource-packages/common.sh"
workdir="$(mktemp -d -t kc-chart-package-test.XXXXXX)"
trap 'rm -rf "$workdir"' EXIT
artifact_dir="$workdir/artifacts"
version=v3.31.5
arch=amd64
leaf="$workdir/resource/calico/$version/$arch"
mkdir -p "$artifact_dir" "$leaf" "$workdir/chart/tigera-operator"
printf 'apiVersion: v2\nname: tigera-operator\nversion: v3.31.5\n' > "$workdir/chart/tigera-operator/Chart.yaml"
tar -czf "$leaf/charts.tgz" -C "$workdir/chart" tigera-operator
printf 'runtime image metadata\n' > "$leaf/images.txt"
printf 'runtime image archive\n' > "$leaf/images.tar.gz"
printf '{}\n' > "$leaf/manifest.json"

has_package_payload_files "$leaf" calico
package="$(make_resource_package calico "$leaf")"
mkdir "$workdir/extracted"
tar -xzf "$package" -C "$workdir/extracted"
cmp "$leaf/charts.tgz" "$workdir/extracted/calico/$version/$arch/charts.tgz"
[[ "$(find "$workdir/extracted" -type f | wc -l | tr -d ' ')" == 1 ]]
rm "$leaf/charts.tgz"
if has_package_payload_files "$leaf" calico; then
  echo "runtime metadata must not create an empty chart package" >&2
  exit 1
fi
echo "chart package archive test passed"
