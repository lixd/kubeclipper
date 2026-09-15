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
<registry>/kubeclipper/packages/bootstrap/kubeclipper:<release>   kc-server/kcctl binaries
<registry>/kubeclipper/packages/bootstrap/etcd:<ver>
<registry>/kubeclipper/packages/bootstrap/registry:<ver>
<registry>/kubeclipper/packages/bootstrap/console:<release>
<registry>/kubeclipper/packages/k8s/k8s:v1.37.0                   kubeadm/kubelet/kubectl bundle
<registry>/kubeclipper/packages/cri/containerd:<ver>
<registry>/kubeclipper/packages/cni/calico:<ver>
<registry>/kubeclipper/packages/k8s-extension/k8s-extension:v1    kubectl etc. (a cluster component)
<registry>/kubeclipper/packages/csi/nfs:<ver>                     addon resource packages (csi/app kinds)
<registry>/kubeclipper/packages/app/metallb:<ver>
<registry>/kubeclipper/packages/extension/kubectl-terminal:<ver>  standalone extensions
<registry>/kubeclipper/charts/<chart>:<ver>                       Helm charts as OCI charts (calico, addons)
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
- A **release manifest** (`release-manifest.json`) lists, for one release, every
  artifact's `repository:tag` plus its resolved digest (multi-arch included).
  `generate-release-manifest.sh --resolve-digests` produces it;
  `verify-release-manifest.sh` re-validates digests, provenance labels and
  policy rules (`tools/release-policy-verify`).

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

- `--package-registry` defaults to `ghcr.io/kubeclipper/kubeclipper`; the
  effective value and its source (flag / deploy config / built-in default) are
  logged.
- A loud **precheck** verifies `<registry>/kubeclipper/packages/bootstrap/kubeclipper`
  is reachable and lists tags before any remote mutation; on failure it tells
  you to mirror the release with `kcctl registry sync`.
- Deploy uploads bootstrap materials to targets from the registry (no static
  server), seeds the delivery-policy ConfigMap, and probes the etcd
  **write path** (put+delete roundtrip, retried) before uploading config — the
  cold-start race between `kc-server` boot and etcd readiness is handled here.

### 4.2 Cluster create / upgrade

The cluster handler resolves the plan (section 3), stores it on the cluster,
and embeds the digest references into the operation steps. The agent fetcher
(`pkg/delivery/fetcher`) pulls each artifact by digest, verifies the content,
and unpacks to the node's cache; a digest already present in a validated local
cache short-circuits the fetch (works fully offline).

### 4.3 Addons

Helm addons are published as OCI charts (`oci://<registry>/kubeclipper/charts/...`)
and their side-effect images are pinned via runtime image sets; install steps
resolve charts from the same registry as everything else.

## 5. Air-gapped and offline delivery

Three supported shapes, from lightest to heaviest:

1. **`kcctl registry sync`** — manifest-driven mirror of a published release
   into an internal registry:
   ```bash
   kcctl registry sync --manifest release-manifest.json \
     --target registry.corp.example.com/kubeclipper \
     --arch amd64 \
     --source-username <user> --source-password-file <token>
   ```
   Copies are digest-preserving and idempotent (a second run reports `0`
   copied). `--dry-run` prints the copy plan. HTTP/private-CA targets are
   supported via `--target-scheme/--target-ca-file/--target-skip-tls-verify`.
2. **Offline registry bundle** — `export-offline-registry-bundle.sh` produces a
   tarball of the registry content; `import-offline-registry-bundle.sh` restores
   it into a local `registry:2` (what the AIO/qualification pipelines run).
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
  `kcctl registry sync` from the real published release into a local registry
  and proves digest/idempotence behavior).

## 7. Compatibility notes

- OCI delivery is a **breaking** change for installs: nodes must be newly
  deployed or re-joined; there is no upgrade path from a static-server
  deployment in place. The legacy static server/downloader stack was removed.
- `scripts/migrate-legacy-packages-to-oci.sh` assists one-time import of
  previously published tarballs into a registry.
- The retired `nfs-provisioner` addon was removed; use `nfs-csi` (OCI
  runtime-image-set based) instead.
- Supported Kubernetes matrix (aligned with README): v1.35.x, v1.36.x, v1.37.x
  — each published as `k8s/<version>` resource packages.

## 8. Troubleshooting quick reference

| Symptom | Likely cause | Action |
|---|---|---|
| deploy precheck fails with `NAME_UNKNOWN` | release not published/synced for that registry | run `kcctl registry sync` or point `--package-registry` at the mirrored registry |
| resolver "no artifact found for slot X" | tag missing from SupportPolicy/inventory | check ConfigMap `kubeclipper-delivery-policy` and `crane ls <repo>` |
| agent task fails with digest mismatch | cache corruption or proxy rewriting | clear the node artifact cache; the fetcher never falls back to tag re-resolution by design |
| re-deploy says "registry precheck: request timed out" | network to registry flapping | verify with `crane ls <registry>/kubeclipper/packages/bootstrap/kubeclipper` |
