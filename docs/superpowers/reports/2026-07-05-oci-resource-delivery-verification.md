# OCI Resource Delivery Verification Report

Date: 2026-07-05
Host: `sh-dev-3` (`lixd-dev-3`, `x86_64`)
Workspace: `/tmp/kc-oci-verify`
Registry: `127.0.0.1:5500`

## Summary

Result: OCI mechanics PASS; clean-node full install PENDING

The OCI resource delivery path was verified on `sh-dev-3` with a real Docker registry. The verification covered OCI package publishing, registry-derived inventory scanning, digest-pinned resolve, architecture miss handling, OCI fetch by digest, local materialization, bootstrap binary packages, extension packages, `kcctl resource` list/inspect, delivery policy template validation, and legacy static-resource keyword audit.

This is not yet a full completion claim for the design acceptance criteria because a destructive clean-node install was not executed.

One issue was found during the first server run and fixed:

- `pkg/delivery/fetcher/oci.go` materialized `layer.Compressed()` bytes, which caused payload digest mismatch against package manifest content digests. It now materializes `layer.Uncompressed()` bytes, matching the payload bytes declared in the package manifest.

## Environment

Remote checks:

- `docker`: available, server version `29.1.3`
- `go`: not installed on `sh-dev-3`
- `registry:2`: pulled and started as `kc-oci-verify-registry`
- Registry health: `curl -sf http://127.0.0.1:5500/v2/` succeeded

Because Go is not installed on the remote host, Linux/amd64 verification binaries were cross-compiled locally and copied to `sh-dev-3`.

## Commands

Local build:

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/kc-oci-verify-bin/kcctl ./cmd/kcctl
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/kc-oci-verify-bin/oci-verify ./tools/oci-verify
```

Remote registry:

```bash
docker run -d --name kc-oci-verify-registry -p 5500:5000 registry:2
curl -sf http://127.0.0.1:5500/v2/
```

Remote validation:

```bash
./bin/oci-verify 127.0.0.1:5500
./bin/kcctl resource list --registry 127.0.0.1:5500 --refresh -o yaml
./bin/kcctl resource inspect --registry 127.0.0.1:5500 --type extension --name kubectl-terminal --version v1.0.0 --arch amd64 -o yaml
./bin/kcctl delivery-policy template -o yaml
./bin/kcctl delivery-policy validate -f reports/delivery-policy-template-2026-07-05.yaml
```

Legacy audit:

```bash
grep -nE "metadata\.json|catalog\.json|legacy-http|TransportLegacyHTTP|TransportLocalFile|local-file|LocalFileFetcher|CloudStaticServer|staticServer:|StaticServer|staticresource|simple/staticserver" \
  $(find pkg cmd -type f ! -name "*_test.go"; printf "kubeclipper-server.yaml\n")
```

## OCI Runtime Verification

The verification published these OCI package artifacts:

| kind | name | version | arch |
| --- | --- | --- | --- |
| `k8s` | `k8s` | `v1.36.0` | `amd64` |
| `cri` | `containerd` | `2.1.0` | `amd64` |
| `cni` | `calico` | `v3.30.0` | `amd64` |
| `binary` | `kubeclipper-agent` | `v1.8.0` | `amd64` |
| `binary` | `etcdctl` | `v3.5.15` | `amd64` |
| `extension` | `kubectl-terminal` | `v1.0.0` | `amd64` |

Registry scan result:

- Inventory count: `6`
- Every package had `transport.type: oci`
- Every package had a `sha256:` platform manifest digest
- `arm64` resolve returned `ArtifactArchUnavailable`

Resolved install plan:

| slot | kind | name | version | digest present |
| --- | --- | --- | --- | --- |
| `k8s` | `k8s` | `k8s` | `v1.36.0` | yes |
| `cri` | `cri` | `containerd` | `2.1.0` | yes |
| `cni` | `cni` | `calico` | `v3.30.0` | yes |

Special resource resolve:

| flow | kind | name | version | proof |
| --- | --- | --- | --- | --- |
| bootstrap | `binary` | `kubeclipper-agent` | `v1.8.0` | selected by `SupportPolicy` and inventory |
| bootstrap | `binary` | `etcdctl` | `v3.5.15` | selected by `SupportPolicy` and inventory |
| extension | `extension` | `kubectl-terminal` | `v1.0.0` | selected by `SupportPolicy` and inventory |

The verification also checked that an inventory-only extension candidate was rejected by `SupportPolicy`.

Fetcher materialization:

| kind | name | contents |
| --- | --- | --- |
| `k8s` | `k8s` | `configs`, `images` |
| `cri` | `containerd` | `configs`, `images` |
| `cni` | `calico` | `charts`, `images` |
| `binary` | `kubeclipper-agent` | `binary` |
| `binary` | `etcdctl` | `binary` |
| `extension` | `kubectl-terminal` | `images` |

All `oci-verify` checks passed: `72/72`.

## CLI Verification

`kcctl resource list --registry 127.0.0.1:5500 --refresh -o yaml` returned:

- `totalCount: 6`
- `binary/kubeclipper-agent:v1.8.0 amd64`
- `binary/etcdctl:v3.5.15 amd64`
- `cni/calico:v3.30.0 amd64`
- `cri/containerd:2.1.0 amd64`
- `extension/kubectl-terminal:v1.0.0 amd64`
- `k8s/k8s:v1.36.0 amd64`

`kcctl resource inspect --registry 127.0.0.1:5500 --type extension --name kubectl-terminal --version v1.0.0 --arch amd64 -o yaml` returned:

- `contentProfile: extension`
- `kind: extension`
- `name: kubectl-terminal`
- `transport.type: oci`
- `transport.ref: 127.0.0.1:5500/kubeclipper/packages/extension/kubectl-terminal:v1.0.0`
- `contents`: `images.tar.gz`

`kcctl delivery-policy validate` returned:

```text
delivery support policy is valid
```

## Legacy Static Resource Audit

Result: PASS

The legacy audit output file was empty. No non-test hits were found for:

- `metadata.json`
- `catalog.json`
- legacy HTTP/local-file transport markers
- static server/staticresource wiring markers

## Local Regression Tests

The following local regression suite passed:

```bash
go test ./pkg/delivery/... ./pkg/apis/config/v1 ./pkg/apis/core/v1 ./pkg/component/common ./pkg/scheme/core/v1/... ./pkg/cli/deploy ./pkg/cli/join ./pkg/cli/registry/... ./pkg/cli/resource ./pkg/cli/deliverypolicy ./pkg/clustermanage/kubeadm ./pkg/simple/downloader ./tools/doc-gen ./tools/oci-verify
```

Packages:

- `pkg/delivery/apis`
- `pkg/delivery/fetcher`
- `pkg/delivery/indexer`
- `pkg/delivery/publisher`
- `pkg/simple/downloader`
- `tools/oci-verify`

## Remaining Scope

This report verifies the OCI delivery mechanics and CLI surfaces on `sh-dev-3`. A full cluster install was not executed because this verification used synthetic package payloads rather than real installable Kubernetes/containerd/calico/kubectl-terminal offline packages.

## Follow-up Installer Input Audit

Date: 2026-07-05

A follow-up audit found and fixed an installer input mismatch for resolved addon packages:

- `common.Imager` and `common.Chart` now preserve the resolved package `kind/name/version`.
- Generic image and chart fetches now use the real package identity, such as `lb/metallb`, `csi/nfs`, or `cni/calico`, instead of synthetic `image` or `chart` kinds.
- Calico operator chart installation now reads from the fetcher materialization path under `packages/cni/calico/.../linux-<arch>/contents/charts.tgz`.
- Resolved package cleanup paths now use the same `linux-<arch>` platform directory as OCI fetcher materialization.

Verification:

```bash
go test ./pkg/component/common ./pkg/scheme/core/v1/cni ./pkg/scheme/core/v1/cri ./pkg/scheme/core/v1/k8s ./pkg/delivery/fetcher
go test ./pkg/delivery/... ./pkg/apis/config/v1 ./pkg/apis/core/v1 ./pkg/component/common ./pkg/scheme/core/v1/... ./pkg/cli/deploy ./pkg/cli/join ./pkg/cli/registry/... ./pkg/cli/resource ./pkg/cli/deliverypolicy ./pkg/clustermanage/kubeadm ./pkg/simple/downloader ./tools/doc-gen ./tools/oci-verify
```

Both suites passed.

`sh-dev-3` was also checked for full-install readiness. It currently has an existing Ready Kubernetes cluster:

- node: `lixd-dev-3`
- Kubernetes: `v1.36.1`
- runtime: `containerd://2.2.4`

Because the host is not a clean install target, a destructive full reinstall was not executed.

## Follow-up Main-Chain Audit

Date: 2026-07-05

Additional audit after inventory model cleanup:

- Removed deprecated inventory presentation fields from `PackageEntry`: `deprecated`, `capabilities`, and `labels`.
- Confirmed `SupportPolicy` remains the support-matrix source and does not carry ref/digest/url/status fields.
- Confirmed `PackageInventory` entries carry only package identity, platform, OCI transport, content profile, and artifact contents.
- Confirmed `kcctl resource` exposes OCI inventory commands `list`, `inspect`, and `refresh`; no `push` or `delete` subcommands are registered.
- Removed unused `ResourceOptions.client` state so `kcctl resource` is driven by the Registry inventory indexer rather than unused API client state.
- Confirmed registry-derived inventory `Get` and `Refresh` use the current request context instead of retaining delivery-source creation context.
- Confirmed generated deploy config exposes required `packageRegistry: ""` as a real YAML field, not a commented example.
- Confirmed default `delivery-policy template` covers bootstrap binary packages and the kubectl-terminal extension package.
- Confirmed addon package profile validation accepts `configs`, `images`, or `charts` content, matching config/chart based addon delivery while preserving image-only CNI loads.
- Removed stale bootstrap comments that described downloading agent binaries from kc-server; bootstrap delivery is described as PackageInventory based.

Verification:

```bash
rg -n "metadata.json|catalog.json|legacy-http|TransportLegacyHTTP|TransportLocalFile|local-file|LocalFileFetcher|CloudStaticServer|staticServer:|StaticServer|staticresource|simple/staticserver" pkg cmd kubeclipper-server.yaml --glob '!**/*_test.go'
rg -n "Use:.*push|Use:.*delete|NewCmdResourcePush|NewCmdResourceDelete|ResourcePush|ResourceDelete|metadata.json|catalog.json" pkg/cli/resource pkg/delivery pkg/apis/config/v1 pkg/apis/core/v1 --glob '!**/*_test.go'
go test ./pkg/cli/resource
go test ./pkg/delivery/... ./pkg/apis/config/v1 ./pkg/apis/core/v1 ./pkg/cli/resource
go test ./pkg/delivery/... ./pkg/apis/config/v1 ./pkg/apis/core/v1 ./pkg/cli/deploy ./pkg/cli/join ./pkg/cli/resource ./pkg/cli/deliverypolicy
go test ./pkg/delivery/... ./pkg/apis/config/v1 ./pkg/apis/core/v1 ./pkg/cli/deploy ./pkg/cli/join ./pkg/cli/resource ./pkg/cli/deliverypolicy ./pkg/clustermanage/kubeadm
go test ./pkg/delivery/apis ./pkg/delivery/indexer ./pkg/apis/config/v1 ./pkg/apis/core/v1
go test ./pkg/clustermanage/kubeadm ./pkg/delivery/apis
git diff --check
```

Results:

- Static-resource grep had no non-test hits.
- Resource push/delete and metadata/catalog grep had no non-test hits.
- `go test ./pkg/cli/resource` passed.
- `go test ./pkg/delivery/... ./pkg/apis/config/v1 ./pkg/apis/core/v1 ./pkg/cli/resource` passed.
- `go test ./pkg/delivery/... ./pkg/apis/config/v1 ./pkg/apis/core/v1 ./pkg/cli/deploy ./pkg/cli/join ./pkg/cli/resource ./pkg/cli/deliverypolicy` passed.
- `go test ./pkg/delivery/... ./pkg/apis/config/v1 ./pkg/apis/core/v1 ./pkg/cli/deploy ./pkg/cli/join ./pkg/cli/resource ./pkg/cli/deliverypolicy ./pkg/clustermanage/kubeadm` passed.
- `go test ./pkg/delivery/apis ./pkg/delivery/indexer ./pkg/apis/config/v1 ./pkg/apis/core/v1` passed.
- `go test ./pkg/clustermanage/kubeadm ./pkg/delivery/apis` passed.
- `git diff --check` passed.

Latest signed-off commits from this audit:

- `ecb7e3d550294d148d350065b3225c1c35450273` - `fix: remove inventory presentation fields`
- `81b41140e6c434b7294aa4aa061fd2383ba9c837` - `chore: drop unused resource client state`
- `c472e70964a1ba31f9eb46e86ea10a6a2a08c929` - `feat: add resource inventory refresh command`
- `4f3b4bf5f48d4090e77744641f702f9c5d0c3c13` - `fix: use request context for registry inventory`
- `4a572ad6554070ab95bfbaf3a0c94aa9c958f6b1` - `fix: expose package registry in deploy template`
- `e2c49efd58435d731d05d1d4f7db49d358fc83ea` - `test: cover special delivery policy slots`
- `f205a63ad4eec1f063ce59cf0572c4efa322fe94` - `fix: allow config-only addon packages`
- `2fba7cc7cc635554f547758de1e9e9884104e953` - `docs: align bootstrap delivery comments`

## Follow-up Content Uniqueness Audit

Date: 2026-07-05

Additional audit after fetcher materialization review:

- `PackageInventory` validation now rejects duplicate content names within a package.
- Fetcher plan validation now rejects empty or duplicate content names before materialization.
- This keeps the fetched `Files` mapping deterministic for packages that carry multiple content archives.

Verification:

```bash
rg -n "metadata.json|catalog.json|legacy-http|TransportLegacyHTTP|TransportLocalFile|local-file|LocalFileFetcher|CloudStaticServer|staticServer:|StaticServer|staticresource|simple/staticserver" pkg cmd kubeclipper-server.yaml --glob '!**/*_test.go'
git diff --check
go test ./pkg/delivery/apis ./pkg/delivery/fetcher ./pkg/delivery/indexer ./pkg/apis/config/v1 ./pkg/apis/core/v1
go test ./pkg/delivery/... ./pkg/apis/config/v1 ./pkg/apis/core/v1 ./pkg/cli/deploy ./pkg/cli/join ./pkg/cli/resource ./pkg/cli/deliverypolicy ./pkg/clustermanage/kubeadm
```

Results:

- Static-resource grep had no non-test hits.
- `git diff --check` passed.
- Focused delivery/API regression passed.
- Broader delivery/CLI/kubeadm regression passed.

Latest signed-off commit from this audit:

- `4fae80f` - `fix: reject duplicate artifact contents`

## Follow-up ComponentMeta Availability Audit

Date: 2026-07-05

Additional audit after checking the ComponentMeta projection requirement:

- `ComponentMetaProjection` now includes an `unavailable` view for policy-supported packages that are not currently installable from the Registry inventory.
- The existing `rules` and `addons` projections continue to expose installable policy/inventory intersections.
- `unavailable.reason` distinguishes packages that are not published at all from packages that exist but do not have the requested target architecture.
- `/componentmeta` returns the unavailable view alongside the existing rules and addons response.

Verification:

```bash
go test ./pkg/delivery/apis ./pkg/apis/config/v1 ./pkg/cli/resource
go test ./pkg/simple/client/kc
go test ./pkg/delivery/... ./pkg/apis/config/v1 ./pkg/apis/core/v1 ./pkg/cli/deploy ./pkg/cli/join ./pkg/cli/resource ./pkg/cli/deliverypolicy ./pkg/clustermanage/kubeadm
rg -n "metadata.json|catalog.json|legacy-http|TransportLegacyHTTP|TransportLocalFile|local-file|LocalFileFetcher|CloudStaticServer|staticServer:|StaticServer|staticresource|simple/staticserver" pkg cmd kubeclipper-server.yaml --glob '!**/*_test.go'
git diff --check
```

Results:

- ComponentMeta tests cover `notPublished` and `archUnavailable` unavailable states.
- Focused delivery/config/resource regression passed.
- `pkg/simple/client/kc` passed when run outside the local sandbox restriction that blocks `httptest` listening.
- Broader delivery/CLI/kubeadm regression passed.
- Static-resource grep had no non-test hits.
- `git diff --check` passed.

Latest signed-off commit from this audit:

- `3f31240` - `feat: report unavailable component packages`

## Follow-up Installer Content Contract Audit

Date: 2026-07-05

Additional audit after checking the installer input contract:

- OCI fetcher plan validation now rejects resolved components with empty `contents`.
- `common.Imager` no longer infers an `images` content entry when the resolved component omits contents.
- `common.Chart` no longer infers a `charts` content entry when the resolved component omits contents.
- Installer-side image/chart steps must receive content metadata from the resolved artifact plan and fail before fetch if it is missing.

Verification:

```bash
go test ./pkg/component/common ./pkg/delivery/fetcher
go test ./pkg/delivery/... ./pkg/apis/config/v1 ./pkg/apis/core/v1 ./pkg/component/common ./pkg/scheme/core/v1/... ./pkg/cli/deploy ./pkg/cli/join ./pkg/cli/resource ./pkg/cli/deliverypolicy ./pkg/clustermanage/kubeadm
rg -n "metadata.json|catalog.json|legacy-http|TransportLegacyHTTP|TransportLocalFile|local-file|LocalFileFetcher|CloudStaticServer|staticServer:|StaticServer|staticresource|simple/staticserver" pkg cmd kubeclipper-server.yaml --glob '!**/*_test.go'
git diff --check
```

Results:

- Focused common/fetcher regression passed.
- Broader delivery/API/component/scheme/CLI/kubeadm regression passed.
- Static-resource grep had no non-test hits.
- `git diff --check` passed.

Latest signed-off commit from this audit:

- `70ac527` - `fix: require resolved package contents`

## Follow-up Latest Version Rejection Audit

Date: 2026-07-05

Additional audit after checking the resolver requirement that `latest` is not supported:

- `PackageInventory` validation now rejects packages whose logical version is `latest`.
- `SupportPolicy` validation now rejects component options whose allowed versions contain `latest`.
- Registry-derived entries therefore cannot produce digest-pinned plans from mutable `latest` tags.
- Policy defaults cannot select `latest` because allowed versions reject it before default validation can pass.

Verification:

```bash
go test ./pkg/delivery/apis ./pkg/delivery/indexer ./pkg/apis/config/v1 ./pkg/cli/deliverypolicy
go test ./pkg/delivery/... ./pkg/apis/config/v1 ./pkg/apis/core/v1 ./pkg/component/common ./pkg/scheme/core/v1/... ./pkg/cli/deploy ./pkg/cli/join ./pkg/cli/resource ./pkg/cli/deliverypolicy ./pkg/clustermanage/kubeadm
rg -n "metadata.json|catalog.json|legacy-http|TransportLegacyHTTP|TransportLocalFile|local-file|LocalFileFetcher|CloudStaticServer|staticServer:|StaticServer|staticresource|simple/staticserver" pkg cmd kubeclipper-server.yaml --glob '!**/*_test.go'
git diff --check
```

Results:

- Focused delivery/indexer/config/delivery-policy regression passed.
- Broader delivery/API/component/scheme/CLI/kubeadm regression passed.
- Static-resource grep had no non-test hits.
- `git diff --check` passed.

## Follow-up Design-Doc Acceptance Audit

Date: 2026-07-05

This follow-up audit treats [docs/superpowers/specs/2026-07-03-oci-resource-delivery-design.md](/Users/lixueduan/17x/kc-release/kubeclipper/docs/superpowers/specs/2026-07-03-oci-resource-delivery-design.md) as the acceptance baseline for "use OCI to replace the existing static server".

Additional audit after checking the current workspace against the design document:

- `kcctl resource` is now registry-only at the CLI surface; it no longer exposes a legacy `--transport` selector and always requires `--registry`.
- `docs/dev-guide.md` now describes the OCI-only flow: publish packages to Registry, maintain `delivery-policy`, set `packageRegistry` for deploy/join, and inspect registry-derived inventory with `kcctl resource`.
- `kcctl deploy` and `kcctl join` continue to require `packageRegistry`, matching the design requirement that deploy/join only pass the package registry instead of any static-server address.
- The design-doc cleanup grep still returns no non-test hits for `metadata.json`, `catalog.json`, `staticresource`, `simple/staticserver`, or legacy HTTP/local-file transport markers.
- The current focused regression suite still covers inventory scan, policy validation, digest-pinned resolve, OCI-only fetch, bootstrap/extension package resolve, and installer consumption of fetched files.

Verification:

```bash
git diff --check
rg -n "metadata.json|catalog.json|legacy-http|TransportLegacyHTTP|TransportLocalFile|local-file|LocalFileFetcher|CloudStaticServer|staticServer:|StaticServer|staticresource|simple/staticserver" pkg cmd kubeclipper-server.yaml --glob '!**/*_test.go'
GOCACHE=/Users/lixueduan/17x/kc-release/kubeclipper/.gocache go test ./pkg/delivery/... ./pkg/apis/config/v1 ./pkg/apis/core/v1 ./pkg/component/common ./pkg/scheme/core/v1/... ./pkg/cli/deploy ./pkg/cli/join ./pkg/cli/resource ./pkg/cli/deliverypolicy ./pkg/clustermanage/kubeadm ./pkg/simple/downloader ./tools/doc-gen ./tools/oci-verify
GOCACHE=/Users/lixueduan/17x/kc-release/kubeclipper/.gocache go test ./pkg/simple/client/kc
GOCACHE=/Users/lixueduan/17x/kc-release/kubeclipper/.gocache GOMODCACHE=/Users/lixueduan/17x/kc-release/kubeclipper/.gomodcache go build ./cmd/kcctl ./tools/oci-verify
```

Results:

- `git diff --check` passed.
- Static-resource grep had no non-test hits.
- Delivery/API/component/scheme/CLI/kubeadm regression passed.
- `pkg/simple/client/kc` passed in the current local workspace.
- `kcctl` and `tools/oci-verify` both compiled successfully after allowing a networked module download for the fresh local module cache.

Current validated workflow after the OCI replacement:

1. Deploy an OCI Registry with `kcctl registry deploy`.
2. Publish offline packages as OCI artifacts under `kubeclipper/packages/{kind}/{name}:{version}`.
3. Maintain compatibility through `SupportPolicy` / `kcctl delivery-policy`.
4. Configure `packageRegistry` for `kcctl deploy` and `kcctl join`.
5. Use `kcctl resource list|inspect|refresh --registry <registry>` to inspect registry-derived inventory.
6. Let install flows resolve a digest-pinned artifact plan from policy + inventory.
7. Fetch by OCI digest and install only from the local materialized files.

## Follow-up sh-dev-3 Pure OCI Workflow Verification

Date: 2026-07-05

Host: `sh-dev-3` / `lixd-dev-3`
Registry: `127.0.0.1:5500`
Workspace: `/tmp/kc-oci-global`

Additional tooling added for the executable workflow:

- `tools/oci-publish` publishes one legacy offline package tarball as an OCI artifact by reusing `pkg/delivery/publisher`.
- `scripts/publish-oci-package.sh` wraps `tools/oci-publish`.
- `tools/oci-migrate` reads a manifest, downloads URL sources when needed, reuses local tarballs directly, and publishes each package to the target Registry.
- `scripts/migrate-legacy-packages-to-oci.sh` wraps `tools/oci-migrate`.

Local verification before copying binaries to `sh-dev-3`:

```bash
gofmt -w tools/oci-publish/main.go tools/oci-migrate/main.go
git diff --check
rg -n "metadata.json|catalog.json|legacy-http|TransportLegacyHTTP|TransportLocalFile|local-file|LocalFileFetcher|CloudStaticServer|staticServer:|StaticServer|staticresource|simple/staticserver" pkg cmd kubeclipper-server.yaml --glob '!**/*_test.go'
GOCACHE=/Users/lixueduan/17x/kc-release/kubeclipper/.gocache GOMODCACHE=/Users/lixueduan/17x/kc-release/kubeclipper/.gomodcache go test ./pkg/delivery/publisher ./pkg/delivery/indexer ./pkg/delivery/fetcher ./pkg/delivery/apis ./pkg/cli/resource ./pkg/cli/deliverypolicy ./pkg/cli/deploy ./pkg/cli/join ./tools/oci-publish ./tools/oci-migrate ./tools/oci-verify
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOCACHE=/Users/lixueduan/17x/kc-release/kubeclipper/.gocache GOMODCACHE=/Users/lixueduan/17x/kc-release/kubeclipper/.gomodcache go build -o /private/tmp/kc-oci-global-bin/kcctl ./cmd/kcctl
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOCACHE=/Users/lixueduan/17x/kc-release/kubeclipper/.gocache GOMODCACHE=/Users/lixueduan/17x/kc-release/kubeclipper/.gomodcache go build -o /private/tmp/kc-oci-global-bin/oci-publish ./tools/oci-publish
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOCACHE=/Users/lixueduan/17x/kc-release/kubeclipper/.gocache GOMODCACHE=/Users/lixueduan/17x/kc-release/kubeclipper/.gomodcache go build -o /private/tmp/kc-oci-global-bin/oci-migrate ./tools/oci-migrate
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOCACHE=/Users/lixueduan/17x/kc-release/kubeclipper/.gocache GOMODCACHE=/Users/lixueduan/17x/kc-release/kubeclipper/.gomodcache go build -o /private/tmp/kc-oci-global-bin/oci-verify ./tools/oci-verify
```

Server setup and migration commands:

```bash
ssh sh-dev-3 'curl -sf http://127.0.0.1:5500/v2/'
rsync -av /private/tmp/kc-oci-global-bin/ sh-dev-3:/tmp/kc-oci-global/bin/

cat > /tmp/kc-oci-global/legacy-packages.yaml <<EOF
registry: 127.0.0.1:5500
packages:
  - source: /tmp/kc-oci-verify-1218015596/k8s-v1.36.0-amd64.tar.gz
    kind: k8s
    name: k8s
    version: v1.36.0
    arch: amd64
  - source: /tmp/kc-oci-verify-1218015596/containerd-2.1.0-amd64.tar.gz
    kind: cri
    name: containerd
    version: 2.1.0
    arch: amd64
  - source: /tmp/kc-oci-verify-1218015596/calico-v3.30.0-amd64.tar.gz
    kind: cni
    name: calico
    version: v3.30.0
    arch: amd64
  - source: /tmp/kc-oci-verify-1218015596/kubeclipper-agent-v1.8.0-amd64.tar.gz
    kind: binary
    name: kubeclipper-agent
    version: v1.8.0
    arch: amd64
EOF

/tmp/kc-oci-global/bin/oci-migrate --file /tmp/kc-oci-global/legacy-packages.yaml --workdir /tmp/kc-oci-global/work
```

Migration result:

- Published `k8s/k8s:v1.36.0` to `127.0.0.1:5500/kubeclipper/packages/k8s/k8s:v1.36.0`.
- Published `cri/containerd:2.1.0` to `127.0.0.1:5500/kubeclipper/packages/cri/containerd:2.1.0`.
- Published `cni/calico:v3.30.0` to `127.0.0.1:5500/kubeclipper/packages/cni/calico:v3.30.0`.
- Published `binary/kubeclipper-agent:v1.8.0` to `127.0.0.1:5500/kubeclipper/packages/binary/kubeclipper-agent:v1.8.0`.

Server verification:

```bash
/tmp/kc-oci-global/bin/kcctl resource list --registry 127.0.0.1:5500 --refresh -o yaml
/tmp/kc-oci-global/bin/kcctl resource inspect --registry 127.0.0.1:5500 --name k8s --version v1.36.0 --arch amd64 -o yaml
/tmp/kc-oci-global/bin/kcctl delivery-policy template -o yaml > /tmp/kc-oci-global/reports/policy-template.yaml
/tmp/kc-oci-global/bin/kcctl delivery-policy validate -f /tmp/kc-oci-global/reports/policy-template.yaml
/tmp/kc-oci-global/bin/oci-verify 127.0.0.1:5500
/tmp/kc-oci-global/bin/kcctl deploy config | grep -n "packageRegistry"
/tmp/kc-oci-global/bin/kcctl create cluster --help | grep -E "k8s-version|cri-version|cni-version|offline|local-registry"
```

Results:

- Registry health check passed.
- `kcctl resource list --refresh` returned six OCI packages, including `k8s/k8s:v1.36.0`, `cri/containerd:2.1.0`, `cni/calico:v3.30.0`, `binary/kubeclipper-agent:v1.8.0`, `binary/etcdctl:v3.5.15`, and `extension/kubectl-terminal:v1.0.0`.
- `kcctl resource inspect` returned `transport.type: oci`, `transport.ref`, platform digest, and `configs/images` content digests for `k8s/k8s:v1.36.0`.
- `kcctl delivery-policy validate` returned `delivery support policy is valid`.
- `oci-verify` passed `72/72` checks on `sh-dev-3`, covering publish, registry inventory, digest-pinned resolve, bootstrap binary resolve, extension resolve, and fetch materialization.
- New `kcctl deploy config` exposes `packageRegistry: ""`.
- New `kcctl create cluster --help` exposes offline install flags and explicit `--k8s-version`, `--cri-version`, and `--cni-version`.

Server-side limitations:

- The currently running KubeClipper server on `sh-dev-3` is `v1.6.0`, while the tested `kcctl` was built from this OCI-delivery workspace.
- `/root/.kc/deploy-config.yaml` on the existing deployment still contains legacy `staticServerPath` and does not contain `packageRegistry`.
- `/tmp/kc-oci-global/bin/kcctl delivery-policy get` against the existing server failed with `invalid character ':' after top-level value`, which is consistent with the remote control plane not yet serving the new policy API contract expected by the workspace CLI.
- A full KubeClipper clean/redeploy and Kubernetes cluster reinstall was not executed because `sh-dev-3` already has a Running cluster named `demo`; replacing it would be destructive.

Conclusion:

- The pure OCI resource mechanics and reusable migration path are verified on `sh-dev-3`.
- The remaining unverified part is destructive: redeploying the KubeClipper control plane from this workspace with `packageRegistry` and then creating a fresh offline Kubernetes cluster from the published OCI packages.

## Follow-up sh-dev-3 Script and Join Registry Verification

Date: 2026-07-06

Additional changes after the pure OCI workflow was rechecked:

- `scripts/publish-oci-package.sh` and `scripts/migrate-legacy-packages-to-oci.sh` now prefer `KC_OCI_PUBLISH_BIN` / `KC_OCI_MIGRATE_BIN`, then `./bin/oci-publish` / `./bin/oci-migrate`, and only fall back to `go run`. This lets the same scripts run on servers without Go when prebuilt binaries are copied next to them.
- `kcctl join` now exposes `--package-registry` and `join-config.yaml` may specify `packageRegistry`, while still inheriting the deployed server config by default.

Server verification:

```bash
rsync -av /private/tmp/kc-oci-global-bin/ sh-dev-3:/tmp/kc-oci-global/bin/
rsync -av scripts/publish-oci-package.sh scripts/migrate-legacy-packages-to-oci.sh sh-dev-3:/tmp/kc-oci-global/scripts/

KC_OCI_MIGRATE_BIN=/tmp/kc-oci-global/bin/oci-migrate \
  /tmp/kc-oci-global/scripts/migrate-legacy-packages-to-oci.sh \
  --file /tmp/kc-oci-global/legacy-packages.yaml \
  --workdir /tmp/kc-oci-global/work

KC_OCI_PUBLISH_BIN=/tmp/kc-oci-global/bin/oci-publish \
  /tmp/kc-oci-global/scripts/publish-oci-package.sh \
  --pkg /tmp/kc-oci-verify-1218015596/k8s-v1.36.0-amd64.tar.gz \
  --kind k8s \
  --name k8s \
  --version v1.36.0 \
  --arch amd64 \
  --registry 127.0.0.1:5500

/tmp/kc-oci-global/bin/kcctl resource list --registry 127.0.0.1:5500 --refresh -o yaml
/tmp/kc-oci-global/bin/kcctl resource inspect --registry 127.0.0.1:5500 --name k8s --version v1.36.0 --arch amd64 -o yaml
/tmp/kc-oci-global/bin/kcctl resource inspect --registry 127.0.0.1:5500 --name containerd --version 2.1.0 --arch amd64 -o yaml
/tmp/kc-oci-global/bin/kcctl resource inspect --registry 127.0.0.1:5500 --name calico --version v3.30.0 --arch amd64 -o yaml
/tmp/kc-oci-global/bin/kcctl delivery-policy template -o yaml > /tmp/kc-oci-global/reports/policy-template-latest.yaml
/tmp/kc-oci-global/bin/kcctl delivery-policy validate -f /tmp/kc-oci-global/reports/policy-template-latest.yaml
/tmp/kc-oci-global/bin/oci-verify 127.0.0.1:5500
/tmp/kc-oci-global/bin/kcctl deploy config | grep -n "packageRegistry"
/tmp/kc-oci-global/bin/kcctl join --help | grep -E "package-registry|packageRegistry"
```

Results:

- Batch migration script published `k8s/k8s:v1.36.0`, `cri/containerd:2.1.0`, `cni/calico:v3.30.0`, and `binary/kubeclipper-agent:v1.8.0` to `127.0.0.1:5500/kubeclipper/packages/...`.
- Single-package publish script republished `k8s/k8s:v1.36.0` and printed the OCI ref, manifest digest, and `configs`/`images` content digests.
- `resource list --refresh` returned six OCI packages from the Registry.
- `resource inspect` for `k8s`, `containerd`, and `calico` returned `transport.type: oci`, Registry refs, manifest digests, and expected content digests.
- `delivery-policy validate` returned `delivery support policy is valid`.
- `oci-verify` passed `72/72` checks.
- `deploy config` exposes `packageRegistry: ""`.
- `join --help` exposes `--package-registry` and the join-config example includes `packageRegistry`.

Current destructive boundary:

- `sh-dev-3` still has a Running cluster named `demo` and a Ready node `lixd-dev-3`.
- The currently running KubeClipper server is still `v1.6.0`; `delivery-policy get` with the workspace CLI returns `invalid character ':' after top-level value`.
- Full redeploy and fresh cluster creation remain intentionally unexecuted because they require cleaning or replacing the existing running environment.

## Follow-up sh-dev-2 Pure OCI Registry Deploy and Cluster Attempt

Date: 2026-07-06

This pass used `sh-dev-2` as a clean target and installed the Registry through KubeClipper's
existing registry command instead of starting a standalone registry container.

Registry install:

```bash
/tmp/kc-oci-global/bin/kcctl registry deploy \
  --node 127.0.0.1 \
  --pk-file /root/.ssh/kc_oci_self \
  --pkg /tmp/kc-oci-global/kc-amd64.tar.gz \
  --registry-port 5500 \
  --skip-image-load \
  -y

curl -sf http://127.0.0.1:5500/v2/
```

Legacy package migration:

```bash
KC_OCI_MIGRATE_BIN=/tmp/kc-oci-global/bin/oci-migrate \
  /tmp/kc-oci-global/scripts/migrate-legacy-packages-to-oci.sh \
  --file /tmp/kc-oci-global/legacy-packages.yaml \
  --workdir /tmp/kc-oci-global/work
```

Published package refs:

- `127.0.0.1:5500/kubeclipper/packages/k8s/k8s:v1.36.0`
- `127.0.0.1:5500/kubeclipper/packages/cri/containerd:2.1.0`
- `127.0.0.1:5500/kubeclipper/packages/cni/calico:v3.30.0`
- `127.0.0.1:5500/kubeclipper/packages/binary/kubeclipper-agent:v1.8.0`

Control-plane deploy:

```bash
/tmp/kc-oci-global/bin/kcctl deploy \
  --server 127.0.0.1 \
  --agent 127.0.0.1 \
  --pk-file /root/.ssh/kc_oci_self \
  --pkg /tmp/kc-oci-global/kc-amd64.tar.gz \
  --package-registry 127.0.0.1:5500 \
  -y
```

The deployed `deploy-config` contains `packageRegistry: 127.0.0.1:5500` and no `staticServer`
field. `kc-etcd`, `kc-server`, `kc-agent`, `kc-console`, and `kc-registry` were active.

Additional code fix from this pass:

- `pkg/apis/core/v1/delivery_source.go` now falls back to the online `deploy-config` configmap's
  `packageRegistry` when resolving the config API delivery source.
- This fixes the gap where `kcctl deploy --package-registry ...` succeeded, but
  `kcctl create cluster` saw `support []` because `/api/config.kubeclipper.io/v1/componentmeta`
  only checked platform settings.
- `pkg/apis/core/v1/delivery_resolver_test.go` now covers the deploy-config packageRegistry
  fallback.

Server verification after replacing `kubeclipper-server` and `kubeclipper-agent` with current
workspace builds:

```bash
/tmp/kc-oci-global/bin/kcctl delivery-policy apply -f /tmp/kc-oci-global/reports/policy-template-sh-dev-2.yaml
/tmp/kc-oci-global/bin/kcctl delivery-policy get -o yaml
/tmp/kc-oci-global/bin/kcctl resource list --registry 127.0.0.1:5500 --refresh -o yaml
/tmp/kc-oci-global/bin/kcctl resource inspect --registry 127.0.0.1:5500 --name k8s --version v1.36.0 --arch amd64 -o yaml
/tmp/kc-oci-global/bin/kcctl resource inspect --registry 127.0.0.1:5500 --name containerd --version 2.1.0 --arch amd64 -o yaml
/tmp/kc-oci-global/bin/kcctl resource inspect --registry 127.0.0.1:5500 --name calico --version v3.30.0 --arch amd64 -o yaml
/tmp/kc-oci-global/bin/oci-verify 127.0.0.1:5500
```

Results:

- `delivery-policy apply` succeeded and `delivery-policy get` returned `metadata.name: default`.
- `resource list --refresh` returned six OCI packages.
- `resource inspect` returned `transport.type: oci`, OCI refs, manifest digests, and content digests.
- `oci-verify` passed `72/72` checks.

Cluster creation attempt:

```bash
/tmp/kc-oci-global/bin/kcctl create cluster \
  --name oci-demo \
  --master 5dee3efc-c26e-4a45-8c81-a1e131d693ae \
  --offline=true \
  --cri containerd \
  --cri-version 2.1.0 \
  --k8s-version v1.36.0 \
  --cni calico \
  --cni-version v3.30.0 \
  --local-registry 127.0.0.1:5500 \
  -y
```

Result:

- `kcctl create cluster` succeeded in creating `oci-demo` and moved it into `Installing`.
- The operation contained OCI-resolved refs and digests for containerd, Kubernetes, and Calico.
- Installation then failed at step `installRuntime`.

Failure summary:

```text
gzip: stdin: not in gzip format
tar: Child returned status 1
tar -zxvf /tmp/kc-downloader/packages/cri/containerd/2.1.0/linux-amd64/contents/configs.tar.gz -C /
```

Conclusion:

- The pure OCI control-plane path is verified through Registry installation, legacy package
  migration, inventory projection, delivery-policy apply/get, deploy with `packageRegistry`, and
  cluster create plan generation.
- The final Kubernetes installation failed because the copied demo package content is not a valid
  installable KubeClipper offline package. The failed step consumed the OCI-resolved artifact and
  then failed while extracting `configs.tar.gz`.
- This is a package-content blocker, not a static-server or OCI delivery-source blocker.

## Follow-up sh-dev-2 Real Legacy Packages and Full Cluster Success

Date: 2026-07-06

This pass used real legacy packages from the existing old KubeClipper deployment on
`sh-dev-3:/opt/kubeclipper-server/resource`. No packages were downloaded from
`https://oss.kubeclipper.io/packages/`.

Real legacy package source:

- `sh-dev-3:/opt/kubeclipper-server/resource/k8s/v1.36.1/amd64`
- `sh-dev-3:/opt/kubeclipper-server/resource/containerd/2.2.4/amd64`
- `sh-dev-3:/opt/kubeclipper-server/resource/calico/v3.31.5/amd64`

Published package refs:

- `127.0.0.1:5500/kubeclipper/packages/k8s/k8s:v1.36.1`
- `127.0.0.1:5500/kubeclipper/packages/cri/containerd:2.2.4`
- `127.0.0.1:5500/kubeclipper/packages/cni/calico:v3.31.5`

Runtime image preparation:

- The local Registry must contain kubeadm and CNI runtime images before `kcctl create cluster`
  uses `--local-registry`.
- The intended user flow is to push `images.tar.gz` archives with `kcctl registry push` after
  `kcctl registry deploy` and before cluster creation.
- An intermediate experiment pushed runtime images during the install step. That experiment proved
  the image-not-found root cause, but it was removed from the final code because it made install
  semantics too implicit.

Cluster creation command:

```bash
/tmp/kc-oci-global/bin/kcctl create cluster \
  --name oci-real \
  --master 5dee3efc-c26e-4a45-8c81-a1e131d693ae \
  --offline=true \
  --cri containerd \
  --cri-version 2.2.4 \
  --k8s-version v1.36.1 \
  --cni calico \
  --cni-version v3.31.5 \
  --local-registry 127.0.0.1:5500 \
  -y
```

Successful operation:

- Operation ID: `f385b849-dcc2-4a69-95df-9e2c797c32af`
- Status: `successful`
- Cluster: `oci-real`
- Cluster phase: `Running`

Key step results:

- `installRuntime`: successful
- `installPackages`: successful
- `renderKubeadmConfig`: successful
- `initControlPlane`: successful
- `cniImageLoader`: successful
- `calico-chartLoad`: successful
- `renderCniYaml`: successful
- `installCalicoRelease`: successful
- `checkHealth`: successful
- `registerServiceAccount`: successful
- `applyKubectlPod`: successful

Runtime image availability evidence:

```text
[1/9] pushed image registry.k8s.io/kube-apiserver:v1.36.1 to 127.0.0.1:5500
[9/9] pushed image docker.io/kubeclipper/kubectl:latest to 127.0.0.1:5500
[1/9] pushed image docker.io/calico/typha:v3.31.5 to 127.0.0.1:5500
[9/9] pushed image quay.io/tigera/operator:v1.40.8 to 127.0.0.1:5500
```

Registry catalog included both OCI package artifacts and preloaded runtime image repositories:

```text
kubeclipper/packages/k8s/k8s
kubeclipper/packages/cri/containerd
kubeclipper/packages/cni/calico
kube-apiserver
kube-controller-manager
kube-scheduler
kube-proxy
coredns
pause
etcd
calico/node
calico/cni
calico/kube-controllers
tigera/operator
```

Final Kubernetes evidence:

```text
NAME         STATUS   ROLES           VERSION   CONTAINER-RUNTIME
lixd-dev-2   Ready    control-plane   v1.36.1   containerd://2.2.4
```

All Kubernetes and Calico pods were `Running`, including:

- `kube-apiserver-lixd-dev-2`
- `kube-controller-manager-lixd-dev-2`
- `kube-scheduler-lixd-dev-2`
- `kube-proxy-6c9wt`
- both `coredns` pods
- `calico-node-gxj7n`
- `calico-kube-controllers-799c7bf97b-7n7zn`
- `tigera-operator-6ddc956597-nx86l`

Single-node note:

- Because this verification used one control-plane node and no worker nodes, CoreDNS was initially
  Pending behind control-plane/master `NoSchedule` taints.
- The taints were removed manually for this single-node validation:

```bash
kubectl taint node lixd-dev-2 node-role.kubernetes.io/control-plane:NoSchedule- || true
kubectl taint node lixd-dev-2 node-role.kubernetes.io/master:NoSchedule- || true
```

Conclusion:

- The full path is verified with real legacy packages from `sh-dev-3`: old package directory,
  package tarball, OCI artifact publish, Registry inventory, delivery policy, `packageRegistry`,
  digest-pinned fetch, preloaded runtime images in the local Registry, and successful offline
  Kubernetes cluster install.
- The remaining single-node taint action is an environment/test-topology adjustment, not an OCI
  delivery blocker.
