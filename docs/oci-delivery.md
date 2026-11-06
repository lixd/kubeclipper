# OCI Package Delivery

Status: implemented on the OCI migration branch · Audience: operators, release
engineers, contributors

This document is the system-level overview of how KubeClipper ships and consumes
its install materials through an OCI registry. For the execution model that
carries these artifacts to nodes, see
[Operation Engine v2 design](superpowers/specs/2026-07-26-operation-engine-v2-design.md).

## 1. Goal

Replace the legacy "static file server + tarball" delivery (download
`kubeclipper-package-*.tar`, unpack, serve over HTTP) with content-addressed
OCI artifacts published to a registry, so that every install/upgrade is:

- **verifiable** — pinned to immutable digests, with source provenance labels;
- **mirrorable** — one command reproduces an entire release in a private
  registry; fully usable offline;
- **uniform** — the same registry serves platform bootstrap binaries, cluster
  install materials, Helm charts and runtime image sets.

## 2. Artifact model

Everything lives under one repository prefix, `kubeclipper/packages/`, keyed by
a *slot* (what the artifact is) and a *name/version* pair:

```text
<registry>/package-bootstrap-kubeclipper:<release>   kc-server/kcctl binaries
<registry>/package-bootstrap-etcd:<ver>
<registry>/package-bootstrap-registry:<ver>
<registry>/package-bootstrap-console:<release>
<registry>/package-k8s:v1.37.0                   kubeadm/kubelet/kubectl bundle
<registry>/package-containerd:<ver>
<registry>/package-calico:<ver>
<registry>/package-k8s-extension:v1    kubectl etc. (a cluster component)
<registry>/package-nfs:<ver>                     addon resource packages (csi/app kinds)
<registry>/package-metallb:<ver>
<registry>/package-kubectl-terminal:<ver>  standalone extensions
```

Runtime images (the `nfs` v4.1.0 / `metallb` v0.13.7 / k8s pause image sets,
etc.) are *not* packaged artifacts: they are mirrored to
`<image-registry>/<upstream repo path>:<tag>` (keeping the upstream path under
the registry prefix) and pinned per platform in the release manifest as
`runtime-image` entries.

Conventions that the tooling enforces:

- Each artifact is a gzipped tar (or Helm OCI chart / image index) published
  with `org.opencontainers.image.source`, `.revision` (the git SHA via
  `KC_SOURCE_REVISION`) and `.version` labels.
- Publishing is **immutable**: a repository/tag that already exists is never
  silently overwritten; the publish tool refuses and demands a version bump.
- Fixed-version bootstrap dependencies and resource packages are published with
  both Linux architectures under `ghcr.io/<owner>/kubeclipper`. Subsequent KC
  candidates reuse the complete index, including its original digest and
  packaging source revision. Resource builders still prepare runtime image lists
  for release assembly. A first publication verifies both architectures before
  promoting the shared tag. Package content updates require a new component
  package version. The KubeClipper bootstrap package follows the KC release.
- A **release manifest** (`release-manifest.yaml`) lists every artifact by
  `repository:tag` and platform. Every kcctl build embeds the checked-in tag
  inventory. `generate-release-manifest.sh` rebuilds it from resource inputs.
  Qualification compares it with the embedded inventory and verifies tag and
  platform availability. OCI transport verifies content digests internally.

## 3. Resolution pipeline (server side)

```text
 SupportPolicy (ConfigMap kubeclipper-delivery-policy)
        │ selects allowed versions/slots
        ▼
 PackageInventory   ←── registry catalog + tag indexer (pkg/delivery/indexer)
        │              tolerant of missing repositories (404/NAME_UNKNOWN = empty)
        ▼
 Resolver (pkg/delivery/apis)
        │ ResolveArtifactsFromStores: OS/arch/version → {slot: digest}
        ▼
 ResolvedArtifactPlan  — persisted on the Cluster as `status.packagePlan`
```

Key properties:

- **Resolve once, reuse forever.** The plan is frozen when the cluster is
  created or upgraded and is carried by every later operation of that cluster:
  AddNodes copies the stored plan (it never re-resolves tags); manual
  operation `/retry` replays the fixed digests already embedded in task
  payloads. This is what makes registry-tag churn unable to affect an
  in-flight installation.
- **Slot gating.** The cluster install/upgrade plan only covers cluster
  material. Slots prefixed `bootstrap*` (platform-deployment inputs for
  `kcctl deploy`) and `extension*` (standalone addons) are excluded; note that
  `k8s-extension` is a cluster component and *does* participate.
- **No blobs in etcd.** Only references (`repository@sha256:...`) and bounded
  metadata are stored; the agents pull content at execution time.
- Credentials are read from `/etc/kubeclipper-{server,agent}/delivery/package-registry.json`
  (mode 0600, path overridable with `KC_PACKAGE_REGISTRY_CONFIG`) and never
  enter operations, task outputs or logs.

## 4. Consumption paths

### 4.1 Platform deployment (`kcctl deploy`)

- Release kcctl binaries use their verified release inventory's GHCR prefix by
  default. `KCZONE=cn` selects `registry.cn-hangzhou.aliyuncs.com/kubeclipper-release`.
  `--package-registry` and a saved deploy configuration take precedence over this
  preset. The effective value and its source are logged.
- A loud **precheck** verifies `<registry>/package-bootstrap-kubeclipper`
  is reachable and lists tags before any remote mutation; on failure it tells
  you to mirror the release with `kcctl registry import`.
- Deploy uploads bootstrap materials to targets from the registry (no static
  server), selects the KubeClipper package by its version tag, seeds the
  delivery-policy ConfigMap, and probes the etcd
  **write path** (put+delete roundtrip, retried) before uploading config — the
  cold-start race between `kc-server` boot and etcd readiness is handled here.

### Default cluster image registry

Deploy resolves its package registry from the explicit flag, deployment configuration,
or zone preset. It creates the `kc-package-registry` Registry with the configured
host, repository prefix, authentication and TLS settings, and records its name
as `defaultImageRegistry` in the deploy configuration. An identical Registry
is reused; a conflicting resource is rejected without deleting or replacing it.

New clusters use the platform default unless an explicit `--image-registry`
Registry name is given. The resolved reference is saved on the cluster.
Kubernetes, pause, Calico and platform addons use that one registry. Upgrade
and scale use the cluster's saved reference; changing the platform default
does not move existing clusters. Upgrade accepts only the target version, and
ordinary edits cannot change or clear the saved registry. Installation packages and Charts continue to
come from the deployment's package registry.

Use a complete address when creating an image Registry:

```bash
kcctl create registry --name internal --address https://harbor.example.com/kubeclipper
```

The API stores the endpoint in `host` and its namespace in `repositoryPrefix`.
The creation UI shows one required image Registry; it does not expose package
source configuration. A Registry resource describes configuration, not a
verified complete mirror. Sync the release manifest and required versions and
architectures before selecting an internal registry.

### 4.2 Cluster create / upgrade

The cluster handler resolves the plan (section 3), stores it on the cluster,
and embeds the digest references into the operation steps. The agent fetcher
(`pkg/delivery/fetcher`) pulls each artifact by digest, verifies the content,
and unpacks to the node's cache; a digest already present in a validated local
cache short-circuits the fetch (works fully offline).

### 4.3 Addons

Charts are stored as `charts.tgz` inside standard component package images
(for example, `<registry>/package-calico:<ver>`). Agents verify
and extract the package, then install the original archive with Helm. Runtime
images remain separate and are pinned via runtime image sets. Native Helm OCI
media types are not required. Registry repository naming restrictions, such as
ACR Personal Edition's single-level repository names, are a separate constraint.

### 4.4 Platform upgrade (`kcctl upgrade`)

For upgrades between OCI releases, `--version` downloads `release-manifest-<version>.yaml` and its
`.sha256` file from GitHub Releases by default and OSS with `KCZONE=cn`. OSS contains the same files
under `kc/<version>/`, alongside the kcctl binaries and checksums. Both channels serve identical
release files; the manifest is not rewritten for the mirror.

```bash
# Replace vX.Y.Z with the published target version.
kcctl upgrade all --version vX.Y.Z
# Use OSS for manifests and the domestic OCI preset.
KCZONE=cn kcctl upgrade all --version vX.Y.Z
```

The shared installation script is served from OSS. It selects GitHub or OSS binaries using
`KCZONE`. `KC_DOWNLOAD_URL` overrides the installer's binary download base and upgrade manifest
download base. Upgrade's explicit `--release-base-url` takes precedence over that variable.
Package locations use an explicit `--package-registry`, the existing deployment registry,
or the zone preset. Package versions and payload integrity are verified before rollout. Offline upgrades use
`--manifest` and can explicitly select their local `--package-registry`.

The stable release workflow verifies the kcctl and manifest checksums before uploading to OSS.
It downloads the six public versioned files, compares them with the approved release files,
verifies their checksums, installs kcctl from both GitHub and OSS into temporary directories
using the versioned script, and checks the native CLI and embedded inventory. It updates the
root installer after these checks pass. Stable release runs are serialized.
Stable publishing runs only in `kubeclipper/kubeclipper`. Forks run CI and GHCR
qualification against their own packages. Production OSS and ACR credentials
belong to the stable publishing repository.
It reuses the existing `OSS_ENDPOINT`, `OSS_BUCKET`, `OSS_ACCESS_KEY` and `OSS_ACCESS_SECRET` repository secrets.
The version directory contains six files with the same names as the GitHub Release assets.

```text
kc/<version>/
├── kcctl-linux-amd64
├── kcctl-linux-arm64
├── kcctl-checksums.txt
├── release-manifest-<version>.yaml
├── release-manifest-<version>.yaml.sha256
└── get-kubeclipper.sh
```

The installer supports Linux amd64 and arm64. A versioned script defaults to its
release tag, while the root script defaults to the latest promoted stable release.
`KC_VERSION` overrides either default. Binary and checksum paths are identical
for GitHub, OSS and custom download sites.

Before creating the GitHub Release, the workflow uses `kcctl registry import --arch all`
to copy the approved inventory to `registry.cn-hangzhou.aliyuncs.com/kubeclipper-release`.
The sync preserves digests and multi-platform indexes and rejects conflicting tags.
The existing manifest verifier then checks the destination references using an empty Docker
configuration so anonymous reads must succeed. This job requires `ACR_USERNAME` and
`ACR_PASSWORD` repository Secrets. A sync or verification failure stops release promotion.

OSS publishing is a required release job because the common installation entry is hosted there.
The existing bucket is `kubeclipper` in `oss-ap-southeast-1`. Its public HTTPS endpoint serves
the root `get-kubeclipper.sh` and the existing `kc/<version>/` directories. The workflow defaults
to that bucket and endpoint, with `OSS_BUCKET` and `OSS_ENDPOINT` Secrets available as overrides.
Publishing requires `OSS_ACCESS_KEY` and `OSS_ACCESS_SECRET`. Repository variable
`OSS_DOWNLOAD_URL` can override the version download base, whose default is
`https://kubeclipper.oss-ap-southeast-1.aliyuncs.com/kc`.

Each released installer is stamped with its version, its own GitHub repository's Release
download URL, and the OSS download base. The standalone kcctl embeds these same HTTP defaults
and the verified inventory's global OCI prefix. The package locations in a manifest can be
resolved against `KCZONE` or `--package-registry`, preserving repository names, tags and
platforms. Existing deployment and cluster registry selections remain authoritative for their
operations.

## 5. Air-gapped and offline delivery

Three supported shapes, from lightest to heaviest:

1. **`kcctl registry import`** — manifest-driven mirror of a published release
   into an internal registry:
   ```bash
   kcctl registry import --manifest release-manifest.json \
     --target registry.corp.example.com/kubeclipper \
     --arch amd64 \
     --source-username <user> --source-password <token>
   ```
   Copies are digest-preserving and idempotent (a second run reports `0`
   copied). `--dry-run` prints the copy plan. HTTP/private-CA targets are
   supported via `--target-scheme/--target-ca-file/--target-skip-tls-verify`.
2. **Offline OCI bundles** — `kcctl registry export` uses the release inventory
   embedded in the standalone CLI and writes `kubeclipper-offline-<version>.tar.gz`
   and a separate small `registry-<version>.tar.gz`. Bootstrap using
   `kcctl registry deploy --bundle registry-<version>.tar.gz`, then import using
   `kcctl registry import --bundle kubeclipper-offline-<version>.tar.gz --target <registry-prefix>`.
   Original indexes, manifests, compressed layers and digests are preserved;
   missing local content fails without online fallback. Export and import need
   only kcctl. See the [offline flow](../scripts/open-packaging/README.md#offline-release-export-and-import).
3. **Build-from-source** — `scripts/open-packaging/build-offline-resources.sh`
   rebuilds resource packages when the official release does not cover your
   matrix.

## 6. Packaging tooling and CI

- `scripts/open-packaging/` — per-package build/publish scripts
  (`bootstrap-packages/`, `resource-builders/`, `resource-packages/`,
  images lists under `resource-builders/images/`), release-manifest
  generation/verification, offline bundle export/import, and self-tests under
  `tests/`.
- `tools/oci-publish`, `tools/helm-oci-publish`, `tools/oci-verify`,
  `tools/release-policy-verify` — Go implementations of publish (with
  immutability guard), chart push, artifact verification and manifest policy
  gates; shared by scripts and CI.
- GitHub workflows: `_publish-oci-component.yml` (generic pipeline),
  `publish-bootstrap-*.yml`, `publish-resource-*.yml`,
  `offline-resource-validate.yml`, and `publish-oci-qualification.yml`
  (end-to-end gates including a **sync-roundtrip job** that runs
  `kcctl registry import` from the real published release into a local registry
  and proves digest/idempotence behavior).

## 7. Compatibility notes

- OCI delivery is a **breaking** change for installs: nodes must be newly
  deployed or re-joined; there is no upgrade path from a static-server
  deployment in place. The legacy static server/downloader stack was removed.
  The one-time `scripts/migrate-legacy-packages-to-oci.sh` import tool was
  removed as well; re-publish from the upstream manifest with
  `scripts/open-packaging/publish-oci.sh` instead.
- The retired `nfs-provisioner` addon was removed; use `nfs-csi` (OCI
  runtime-image-set based) instead.
- Supported Kubernetes matrix (aligned with README): v1.35.x, v1.36.x, v1.37.x
  — each published as `k8s/<version>` resource packages.

## 8. Troubleshooting quick reference

| Symptom | Likely cause | Action |
|---|---|---|
| deploy precheck fails with `NAME_UNKNOWN` | release not published/synced for that registry | run `kcctl registry import` or point `--package-registry` at the mirrored registry |
| resolver "no artifact found for slot X" | tag missing from SupportPolicy/inventory | check ConfigMap `kubeclipper-delivery-policy` and `crane ls <repo>` |
| agent task fails with digest mismatch | cache corruption or proxy rewriting | clear the node artifact cache; the fetcher never falls back to tag re-resolution by design |
| re-deploy says "registry precheck: request timed out" | network to registry flapping | verify with `crane ls <registry>/package-bootstrap-kubeclipper` |

Package repositories use one flat name: `package-bootstrap-<name>` for platform
bootstrap assets and `package-<name>` for component packages, including charts.
For a namespace-scoped Registry such as
`registry.cn-hangzhou.aliyuncs.com/kubeclipper-release`, runtime repositories are
also flat (`calico-node`, `nfs-plugin`, `metallb-controller`, etc.). The publisher
and agent templates share the aliases in
`pkg/delivery/registry/image_repositories.json`. Registries configured without a
namespace retain upstream runtime image paths; CoreDNS keeps its kubeadm name
`coredns`. Package kind and platform are verified from the package manifest.
