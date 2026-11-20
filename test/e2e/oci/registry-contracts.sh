#!/usr/bin/env bash
set -euo pipefail

kcctl=$1
registry=$2
version=$3

digest=$(curl --fail --silent --show-error --head --max-time 15 \
  -H 'Accept: application/vnd.oci.image.manifest.v1+json' \
  "http://$registry/v2/package-k8s/manifests/$version" \
  | awk 'tolower($1) == "docker-content-digest:" {sub(/\r$/, "", $2); print $2}')
[[ "$digest" =~ ^sha256:[a-f0-9]{64}$ ]]

filters=(--registry "$registry" --type k8s --name k8s --arch amd64 -o json)
"$kcctl" resource list "${filters[@]}" --version "$version" | jq -es --arg version "$version" '
  . == [{type: "k8s", name: "k8s", version: $version, arch: "amd64"}]
' > /dev/null
"$kcctl" resource list "${filters[@]}" --version v0.0.0 | jq -e '
  (.items.addons // []) == [] and (.items.kc_versions // []) == []
' > /dev/null
printf '[3-20] OCI package list selects only the requested version and architecture\n'

"$kcctl" resource inspect "${filters[@]}" --version "$version" \
  | jq -e --arg ref "$registry/package-k8s:$version" --arg digest "$digest" '
    length == 1 and .[0].arch == "amd64" and .[0].transport.type == "oci"
    and .[0].transport.ref == $ref and .[0].transport.digest == $digest
    and (.[0].contents | any(.name == "configs"))
  ' > /dev/null
if output=$("$kcctl" resource inspect "${filters[@]}" --version v0.0.0 2>&1); then
  printf 'missing OCI package was accepted\n' >&2
  exit 1
fi
[[ "$output" == *'not found in registry inventory'* ]]
printf '[3-21] OCI package inspection matches registry manifest %s and rejects an absent version\n' "$digest"
