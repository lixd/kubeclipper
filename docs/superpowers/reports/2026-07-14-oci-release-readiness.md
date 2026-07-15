# Pure OCI release readiness report (2026-07-14)

## Release candidate

- Branch: `codex/oci-static-server-replacement`
- Release commit: `cf2f967e6ddbf2b81354096b88216c44d253021c`
- Qualification tag: `oci-qualification-20260715-cf2f967`
- Test hosts: `sh-dev-3` (`172.16.131.146`) and `sh-dev-2` (`172.16.131.208`)
- Package and runtime-image Registry: host-native Distribution Registry at `sh-dev-3:5000`
- Kubernetes stack: Kubernetes `v1.35.0`, containerd `1.7.29`, Calico `v3.29.6`

The release candidate was built from a clean detached worktree. `kcctl`,
`kubeclipper-server`, and the OCI bootstrap package all carried the exact release commit.

## Blockers closed

1. Joined-agent cleanup
   - `kcctl clean` merges online node inventory when using the API.
   - Join now persists the planned cleanup inventory before installing agents, so an
     interrupted client cannot leave an untracked agent.
   - The join SSH transport is persisted separately from the server transport. AIO agents
     use server SSH; joined agents use agent SSH.
   - Cached private-key contents are not persisted when a reusable key path exists.
   - Remote cleanup failures are returned instead of printing false success.
2. Target architecture
   - Bootstrap architecture is detected on each target with an injectable SSH runner.
   - Unit coverage includes amd64, arm64, and mixed target groups.
3. Join transport isolation
   - `serverSSH` and `--server-ssh-*` are independent of agent `ssh`/`--pk-file`.
   - The two-host test used two distinct keys: the server key was authorized only on
     `sh-dev-3`, and the agent key only on `sh-dev-2`.
4. Multi-NIC safety
   - `first-found` excludes Docker, CNI, Podman, and nerdctl bridge patterns.
   - Qualification used explicit `interface=ens3` for both management and node addresses.
5. Publication/support-policy consistency
   - `tools/release-policy-verify` validates both directions: every advertised policy
     default is published for supported architectures, and every published release
     component is represented by policy.
6. CI runtime deprecations
   - Workflows use `actions/checkout@v6`, `actions/setup-go@v6`,
     `azure/setup-helm@v5`, and `docker/login-action@v4`.
7. OCI provenance
   - Package config labels and release-manifest entries include `sourceRevision`.
   - Manifest generation verifies the platform bootstrap revision before promotion.

## Local validation

The following passed on the release line:

```text
go test -race (clean, join, deploy, create, delivery API/publisher,
              autodetection, release-policy verifier)
go vet ./...
golangci-lint run ./...
actionlint
shellcheck (changed release/open-packaging scripts)
scripts/open-packaging/tests/*.sh
bash -n (release/open-packaging scripts)
tools/release-policy-verify
Linux amd64 and arm64 builds: kcctl, kubeclipper-server, kubeclipper-agent
release manifest/digest/sourceRevision verification
offline Registry bundle export/import verification
git diff --check
```

Repository-wide `go test ./...` was also run locally. The only local failures were
host-environment-specific and are covered authoritatively by the successful Linux Action:

- macOS has no systemd/dbus for `pkg/utils/systemctl`;
- the desktop sandbox blocks host-info calls used by `pkg/utils/sysutil`;
- local E2E requires a live `~/.kc/config`.

## GitHub Actions at the release commit

- Go tests/coverage: [run 29388107973](https://github.com/lixd/kubeclipper/actions/runs/29388107973) — success
- Offline resource/provenance validation: [run 29388107992](https://github.com/lixd/kubeclipper/actions/runs/29388107992) — success
- Registry-native AIO deployment and fast E2E: [run 29388107991](https://github.com/lixd/kubeclipper/actions/runs/29388107991) — success
- Full 16-component publication and release manifest: [run 29388108045](https://github.com/lixd/kubeclipper/actions/runs/29388108045) — success (all 16 publication jobs plus manifest)

The AIO workflow publishes bootstrap kubeclipper/etcd/console to a local Registry,
deploys from OCI, verifies login and fast E2E (including Web Terminal SSH), and requires
`kcctl clean` to succeed.

The manifest job verified 12 release artifacts with zero failures. Its evidence was:

```text
release-manifest.yaml sha256: d051ca7c362d9490deb2efe1cdc901df2fccd7583f4c247cb05ef63fbd10ae2e
metadata.sourceRevision: cf2f967e6ddbf2b81354096b88216c44d253021c
bootstrap/kubeclipper OCI digest: sha256:39bdc402f35dd2ac440556f11fa83fb5ca3f650e6cda45002d162fe8a2b9e7d1
verified artifacts: 12; failures: 0
```

## OCI evidence

The isolated two-host bootstrap artifact was:

```text
ref: 172.16.131.146:5000/kubeclipper/packages/bootstrap/kubeclipper:e2e-cf2f967-20260715
index digest: sha256:6be035f206ae36fa645c5850620ce86dd8a9bde199c84759d49f35c1ffc55315
sourceRevision: cf2f967e6ddbf2b81354096b88216c44d253021c
```

The tag was deleted after verification; only the pre-existing `v1.8.0` tag remains in
the host Registry.

## Two-host pure OCI lifecycle

1. Deployed the platform to `sh-dev-3` from OCI with explicit `ens3` detection.
2. Logged in successfully. Client and server both reported Git commit
   `cf2f967e6ddbf2b81354096b88216c44d253021c` and a clean tree.
3. Joined `sh-dev-2` from the OCI bootstrap package using distinct server/agent SSH keys.
   Both platform nodes reached Ready. No private-key body appeared in deploy configuration.
4. Created `e2e-cf2f967` using only Registry packages/images:
   Kubernetes `v1.35.0`, containerd `1.7.29`, Calico `v3.29.6`. The cluster reached
   `Running`; all control-plane and system pods reached Running/Ready.
5. `cluster add-node` added `sh-dev-2`. Both Kubernetes nodes were Ready and reported
   containerd `1.7.29`.
6. `cluster remove-node` removed `sh-dev-2`. The cluster returned to Running with only
   `sh-dev-3`; kubelet/containerd on `sh-dev-2` were inactive.
7. Cluster deletion completed through the normal API.
8. One `kcctl clean --all --force --assumeyes` invocation completed successfully and
   cleaned both the AIO server/agent and the joined agent without manual follow-up.

The initial qualification pass intentionally exposed and then fixed two cleanup edge
cases (interrupted join inventory and AIO server/agent transport selection). The complete
lifecycle above is the clean rerun at the final release commit.

## Final cleanup evidence

On both hosts, all of the following were absent or inactive after the successful final
clean: `kc-server`, `kc-agent`, `kc-etcd`, `kc-console`, `kubelet`, Kubernetes static-pod
processes, the KubeClipper server/agent configuration trees and binaries,
`/etc/kubernetes`, `/var/lib/kubelet`, `/var/lib/etcd`, `/etc/containerd`, and
`/var/lib/containerd`.

The temporary SSH authorizations and keys, Registry tags, qualification Git tags, source
copies, binaries, and package archives were deleted. All 216 GHCR packages under the
temporary `qualification-*` prefixes were deleted through the signed-in package UI; a
nine-page package-list rescan returned zero remaining qualification packages. The
host-native `kc-registry.service` remained active.
The unrelated `sprout-postgres-v2` container was not modified.

## Remaining gaps

- P0: none.
- P1: none.
- P2: the test hosts do not run chronyd/ntpd; the existing deploy precheck reports this
  clearly and continued because measured host skew was below one second.
- P2: Distribution Registry blob reclamation remains the Registry operator's normal
  garbage-collection responsibility after manifest deletion.

## Release recommendation

All P0 gates are closed, every required Action is green at the release commit, the clean
two-host pure OCI lifecycle passed without manual remediation, and the test environment
has no KubeClipper/Kubernetes test residue. **可以正式发布。**
