#!/usr/bin/env bash
set -euo pipefail

binary="$(realpath "${1:?provide the candidate Ginkgo test binary}")"
mkdir -p "${2:?provide the evidence directory}"
evidence="$(realpath "$2")"
: "${KC_E2E_BACKEND_REVISION:?set the fixture backend revision}"
: "${KC_E2E_MASTERS:?assign the AIO Agent}"
: "${KC_E2E_KUBERNETES_VERSION:?set the Kubernetes version}"
: "${KC_E2E_PROBE_IMAGE:?provide the workload probe image}"
: "${KC_E2E_IMAGE_REGISTRY:?set the candidate image Registry resource}"
: "${KC_E2E_CONTROL_PLANE_CA_FILE:?set the fixture control-plane public CA path}"
: "${KC_E2E_SERVER_ENDPOINT:?set the direct kc-server TLS endpoint}"
: "${KC_E2E_SERVER_CA_FILE:?set the kc-server public CA path}"

# Each selected spec creates its own resources. Cluster specs on the shared
# Agent must remain serial and start after the previous workload has gone.
specs=(
  Tokens Accounts NamedWatch DeliveryPolicy RegistryAudit PlatformDiagnostics DNSRecords
  ClusterAccess ClusterSettings DNSSync UpgradeAdmission Certificates BackupReplay BackupDeleteRetry CronBackup
)
preserve=KC_E2E_BACKEND_REVISION,KC_E2E_MASTERS,KC_E2E_KUBERNETES_VERSION,KC_E2E_PROBE_IMAGE,KC_E2E_IMAGE_REGISTRY,KC_E2E_CALICO_IPV4_DETECTION,KC_E2E_CONTROL_PLANE_CA_FILE,KC_E2E_JUNIT
failed=0
for spec in "${specs[@]}"; do
  export KC_E2E_JUNIT="$evidence/$spec.xml"
  logfile="$evidence/$spec.log"
  focus="\\[OCI-$spec\\]"
  command=("$binary" -ginkgo.noColor -ginkgo.focus="$focus")
  case "$spec" in
    BackupReplay|BackupDeleteRetry|CronBackup)
      command=(sudo --preserve-env="$preserve" env HOME="$HOME" "${command[@]}")
      ;;
  esac
  result=0
  "${command[@]}" 2>&1 | tee "$logfile" || result=$?
  if [[ "$result" == 0 ]] && ! grep -Eq '(^|[^0-9])1 Passed.*0 Failed' "$logfile"; then
    result=1
  fi
  printf '%s\n' "$result" > "$evidence/$spec.exit"
  if [[ "$result" != 0 ]]; then
    failed=1
  fi
done
exit "$failed"
