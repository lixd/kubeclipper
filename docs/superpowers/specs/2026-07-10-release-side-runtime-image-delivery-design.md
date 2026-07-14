# Release-Side Runtime Image Delivery

Date: 2026-07-10

## Decision

Runtime image metadata belongs to release engineering, not to the KubeClipper
control plane. KubeClipper does not keep a Kubernetes/Calico runtime image map
and does not block cluster creation with Registry `HEAD` checks.

The delivery model has three independent sources of truth:

1. `DeliveryPolicy` defines supported Kubernetes, CRI, and CNI combinations.
2. Package/Helm Registry inventory describes available KubeClipper packages and charts.
3. `release-manifest.yaml` describes package, chart, and runtime image objects for publishing, mirroring, offline bundle construction, and optional verification.

Only the first two participate in cluster plan resolution. The third is not
stored in the KubeClipper API.

## Rationale

The authoritative runtime image test is a real CRI pull on the target node.
A control-plane Registry probe can return false failures when credentials,
platform selection, Registry replication, or node-local cache differ from the
server environment. Maintaining a second runtime image catalog in server code
or `DeliveryPolicy` also couples release content to control-plane upgrades.

Kubeadm pulls its required images during cluster initialization. Kubelet pulls
CNI and addon images when their workloads start. Their failures remain visible
in operation logs and workload status.

## Build Outputs

Component builders generate `images.txt`. `generate-resource-metadata.sh`
aggregates these files into `images.lock`, which records:

- component name and version;
- target architecture;
- upstream image reference;
- published image reference.

`images.lock` is consumed by `push-runtime-images.sh`. It is never embedded in
package images and is never read by KubeClipper server.

After a resource matrix is built, `generate-release-manifest.sh` creates one
release manifest containing:

- standard OCI package images;
- native Helm OCI charts;
- standard runtime container images;
- source references and relative destination references;
- supported platforms;
- optional immutable Registry digests.

For release assembly, package images also carry the source commit in
`kc-package-manifest.json` and the OCI
`org.opencontainers.image.revision` label. Running the generator with
`--resolve-digests --source-revision <commit>` records each package's own build
revision, rejects missing provenance and mixed platform revisions, and verifies
that `bootstrap/kubeclipper` matches the kcctl release commit. Independently
maintained third-party packages may retain a different revision. A stale
KubeClipper bootstrap or internally mixed multi-architecture tag is a release
error, even when mutable tags and their current digests are otherwise
internally consistent.

The build manifest is the allowlist. Files left in an output directory from an
older build are not added unless their component and version are still declared.

## Delivery Verification

`verify-release-manifest.sh` checks exact destination references. It supports
architecture filtering, insecure local registries, and optional digest
comparison. Standard package/runtime images can be inspected by `crane` or
`skopeo`. Helm OCI entries use `crane`, because skopeo versions that reject Helm
media types are still common.

Verification is intended for release CI, Harbor replication acceptance, and
air-gap import acceptance. A failed optional verification does not change
`kcctl create cluster` behavior.

## Cluster Creation

Offline creation still requires `localRegistry`; loading `images.tar.gz` on
nodes is not restored. The create path performs these checks:

1. selected component versions satisfy `DeliveryPolicy`;
2. required package OCI images and Helm OCI charts resolve from `packageRegistry`;
3. package contents can be fetched by digest.

Runtime images are then pulled by kubeadm, containerd, and kubelet. Missing
runtime images fail the actual installation operation rather than an earlier
server-side approximation.

## Offline Bundles

A future offline bundle command must consume `release-manifest.yaml` and store
objects as OCI layouts. The bundle may use a compressed tar envelope, but it
must not reintroduce the old static Resource package or use Docker archive as
the universal format, because Helm OCI charts must retain their media types.
