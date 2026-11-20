#!/usr/bin/env bash
set -euo pipefail

kcctl=$1
revision=$2
version=$3
platform=$4

"$kcctl" version -o json | REVISION="$revision" VERSION="$version" PLATFORM="$platform" python3 -c '
import json, os, sys
header, _, body = sys.stdin.read().partition("\n")
assert header == "kcctl version:", header
actual = json.loads(body)
assert actual["gitCommit"] == os.environ["REVISION"], actual
assert actual["gitVersion"] == os.environ["VERSION"], actual
assert actual["platform"] == os.environ["PLATFORM"], actual
'
printf '[3-28] Built CLI identity matches the candidate\n'

"$kcctl" deploy config | python3 -c '
import sys, yaml
config = yaml.safe_load(sys.stdin)
for field in ("password", "pkFile", "pkPassword"):
    assert config["ssh"][field] == "", field
for field in ("serverIPs", "agents", "etcd", "packageRegistry"):
    assert field in config, field
'
printf '[3-27] Deploy template is valid and leaves credentials empty\n'

"$kcctl" completion bash | bash -n
"$kcctl" completion zsh | zsh -n
if output=$("$kcctl" completion unsupported 2>&1); then
  printf 'unsupported shell was accepted\n' >&2
  exit 1
fi
[[ "$output" == *unsupported* ]]
printf '[7-02] Bash and zsh completions parse, unknown shell is rejected\n'

policy=$("$kcctl" delivery-policy template -o yaml)
printf '%s\n' "$policy" | "$kcctl" delivery-policy validate -f /dev/stdin | grep -q 'delivery support policy is valid'
for invalid in 'duplicate component slot' 'default option' 'is not allowed'; do
  malformed=$(printf '%s\n' "$policy" | python3 -c '
import sys, yaml
policy = yaml.safe_load(sys.stdin)
slots = policy["spec"]["policies"][0]["componentSlots"]
slot = next(slot for slot in slots if "default" in slot)
match sys.argv[1]:
    case "duplicate component slot":
        slots.append(dict(slot))
    case "default option":
        slot["default"]["name"] = "not-declared"
    case "is not allowed":
        slot["default"]["version"] = "999.0.0"
print(yaml.safe_dump(policy))
' "$invalid")
  if output=$(printf '%s\n' "$malformed" | "$kcctl" delivery-policy validate -f /dev/stdin 2>&1); then
    printf 'invalid delivery policy was accepted (%s)\n' "$invalid" >&2
    exit 1
  fi
  [[ "$output" == *"$invalid"* ]]
done
printf '[3-24] Delivery policy validation accepts the rendered policy and rejects the three invalid variants\n'
