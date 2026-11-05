# Open OCI Packaging Scripts

This directory contains open, directly runnable packaging helpers for the OCI
delivery flow. They replace the old internal scripts that downloaded content
from private content servers before assembling release packages.

The scripts intentionally use public or local inputs:

- Local KubeClipper source tree or a local binary directory.
- Official upstream release URLs for Kubernetes, containerd, Docker, Helm,
  runc, crictl, etcd, and Calico charts.
- Source-built helper binaries for assets that do not have a stable upstream
  static release, such as `conntrack`.
- A target OCI Registry.

They do not depend on `OFFLINE_URL_PREFIX`, static content servers, or private
download URLs.

## 0. Manifest-Driven Build

The preferred entry point is the manifest-driven publisher:

```bash
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --registry 10.0.0.10:5000 \
  --image-registry 10.0.0.10:5000 \
  --include-bootstrap \
  --push
```

It reads `packaging/resources.yaml`, expands the version/architecture matrix,
and calls the per-component publishers. Each publisher fetches upstream
resources, builds a temporary package payload, pushes OCI package images or Helm
charts, mirrors runtime images, and cleans up local temporary files.
`--include-bootstrap` additionally publishes kubeclipper, etcd, console, and
registry package images and includes them in the generated release manifest.

For local debugging without pushing, omit `--push`:

```bash
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --output /data/kc-resource
```

Publishing is split by content type:

- `configs.tar.gz` becomes a standard OCI package image under
  `package-*`. The image filesystem stores files under
  `/opt/kubeclipper/resource/`.
- `charts.tgz` is included unchanged in the component package image under
  `package-*`; it is extracted before Helm installation.
- `images.lock` is copied with `crane` or `skopeo` by
  `push-runtime-images.sh`. It is release-side metadata and is never consumed
  by KubeClipper server during cluster creation.
- `release-manifest.yaml` aggregates package images and runtime
  images into one synchronization/verification input.

Generate or regenerate the release manifest from an existing resource build:

```bash
scripts/open-packaging/generate-release-manifest.sh \
  --build-manifest packaging/resources.yaml \
  --resource-dir /data/kc-resource
```

After all artifacts are published, release CI can pin their Registry digests:

```bash
scripts/open-packaging/generate-release-manifest.sh \
  --build-manifest packaging/resources.yaml \
  --resource-dir /data/kc-resource \
  --resolve-digests \
  --source-revision "$(git rev-parse HEAD)"
```

Package publishing records `KC_SOURCE_REVISION` in both the package manifest
and the standard `org.opencontainers.image.revision` OCI label. Component
workflows set it to `github.sha` automatically. Release assembly must pass the
kcctl release commit to `--source-revision`; generation then requires every
package platform to expose provenance, rejects mixed revisions inside a
multi-architecture tag, and requires `bootstrap/kubeclipper` to match that
release commit. Independently maintained third-party packages may come from a
different recorded commit. The provenance gate requires `--resolve-digests`
because it verifies the actual remote image config rather than trusting a local
declaration.

`KC_SOURCE_URL` supplies the `org.opencontainers.image.source` OCI label.
Component workflows set it to the publishing GitHub repository URL, including
forks. Manual publishers should set it to their actual source repository; when
unset, the source label is omitted rather than attributed to an unrelated repo.

The revision is also part of Registry inventory. `kcctl deploy` selects the
KubeClipper server/agent package built from the same commit as `kcctl`, and
`kcctl join` selects an agent built from the running server's commit. This is
intentional: publishing a newer mutable tag must not silently change an older
installation. Deploy consumes the kubeclipper, etcd, and console bootstrap
packages; the registry package is consumed only by `kcctl registry deploy` or
offline Registry self-bootstrap.

Verification is optional and does not block `kcctl create cluster`:

```bash
scripts/open-packaging/verify-release-manifest.sh \
  --manifest release-manifest.yaml \
  --registry 10.0.0.10:5000 \
  --arch amd64 \
  --insecure
```

`skopeo` can verify and mirror the standard package/runtime images. Helm OCI
media types are not supported by every skopeo version; the verification script
uses `crane` for Helm chart entries, and chart mirroring should use
`crane`, `oras`, or Helm-compatible tooling.

## GitHub Actions Publishing

Each publishable component has its own workflow. The workflows share only the
internal setup/publish implementation in `_publish-oci-component.yml`; they do
not run an aggregate release build:

| Workflow | Trigger | Output |
| --- | --- | --- |
| `publish-bootstrap-kubeclipper.yml` | Push to `main`, `master`, `release-*`, a `v*` tag, or manual | KubeClipper server/agent package image |
| `publish-bootstrap-etcd.yml` | Manual | etcd package image |
| `publish-bootstrap-console.yml` | Manual | Caddy/console package image |
| `publish-bootstrap-registry.yml` | Manual | Distribution Registry package image |
| `publish-resource-k8s.yml` | Manual | Kubernetes package image and runtime images |
| `publish-resource-containerd.yml` | Manual | containerd package image |
| `publish-resource-k8s-extension.yml` | Manual | Kubernetes helper package image and runtime images |
| `publish-resource-calico.yml` | Manual | Tigera operator Helm OCI chart and Calico runtime images |
| `publish-resource-kc-runtime.yml` | Manual | KubeClipper helper runtime images |
| `publish-resource-nfs.yml` | Manual | NFS provisioner or NFS CSI runtime images |
| `publish-resource-metallb.yml` | Manual | MetalLB runtime images |

The automatic KubeClipper workflow derives its package tag from the Git ref:

```text
Git tag v2.0.0       -> v2.0.0
main or master       -> latest
release-1.8          -> release-1.8
```

Other branch names are converted to valid OCI tags by replacing unsupported
characters such as `/` with `-`. `latest` is accepted only for the
`bootstrap/kubeclipper` package; release policy and cluster resource packages
still require explicit versions. All component workflows publish directly to
GHCR with `GITHUB_TOKEN` and verify the resulting package, chart, and runtime
image references. Leave `registry_prefix` empty to use
`ghcr.io/<repository-owner>/kubeclipper`.

`kcctl` remains a GitHub Release binary and is not included in a bootstrap
package image. For an organization, make the resulting GHCR packages public or
grant pull access before using the Registry as an installation source.

Package images are built by `tools/oci-publish` through go-containerregistry.
They do not need a Dockerfile. The generated image is still a normal OCI image:

```text
manifest: application/vnd.oci.image.manifest.v1+json
config:   application/vnd.oci.image.config.v1+json
layer:    application/vnd.oci.image.layer.v1.tar+gzip

/opt/kubeclipper/resource/kc-package-manifest.json
/opt/kubeclipper/resource/configs.tar.gz
/opt/kubeclipper/resource/kubeclipper-agent
```

Use a Dockerfile only for real runnable images, for example
`kubeclipper-server` as a container image or a registry helper image. For
KubeClipper package images, Dockerfile-based builds would add an unnecessary
dependency on Docker BuildKit and make GitHub Actions harder to run on minimal
runners.

Use `--component <name>` to build only one component and `--dry-run` for CI
validation without downloads or pushes.

Requirements:

- `python3` with `PyYAML` for `packaging/resources.yaml` parsing.
- `curl` or `wget`, `tar`, and `gzip` for download/build steps.
- `helm` for chart-based components when `--chart-file` or `chartFile` is not
  supplied.
- `crane` or `skopeo` for `push-runtime-images.sh` real image copy. Dry-run
  does not require either tool.

Useful network controls:

```bash
KC_DOWNLOAD_RETRIES=3
KC_DOWNLOAD_CONNECT_TIMEOUT=20
KC_DOWNLOAD_MAX_TIME=900
KC_HELM_TIMEOUT=900
```

Versions and architectures are the normal inputs. Public upstream download
locations are intentionally fixed inside each component script so the manifest
does not become a second URL templating layer. If the build environment cannot
reach an upstream source directly, use a network proxy/mirror at the environment
level, or provide one of the supported local inputs such as `chartFile`,
`imagesFile`, `kubeletServiceFile`, or `kubeletPreStartFile`.

## 1. Build Resource Offline Packages

Build the packages that used to live under `resource/`:

```bash
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --arch amd64 \
  --output /data/kc-resource
```

You can also call individual builders when debugging one component:

```bash
scripts/open-packaging/resource-builders/build-containerd-package.sh \
  --version 2.2.4 \
  --runc-version 1.3.3 \
  --crictl-version 1.35.0 \
  --arch amd64 \
  --output /data/kc-resource

scripts/open-packaging/resource-builders/build-calico-package.sh \
  --version v3.31.5 \
  --arch amd64 \
  --output /data/kc-resource

scripts/open-packaging/resource-builders/build-addon-package.sh \
  --name nvidia-gpu-operator \
  --version v25.10.0 \
  --arch amd64 \
  --output /data/kc-resource
```

The output layout is the legacy resource layout consumed by the publisher.
The publisher splits this layout into native OCI outputs instead of wrapping
everything into one large static-server package:

```text
/data/kc-resource/
  k8s/v1.37.0/amd64/configs.tar.gz
  k8s/v1.37.0/amd64/images.txt
  kc-runtime/v2.0.0/amd64/images.txt
  nfs/v4.1.0/amd64/images.txt
  metallb/v0.13.7/amd64/images.txt
  containerd/2.2.4/amd64/configs.tar.gz
  calico/v3.31.5/amd64/charts.tgz
  calico/v3.31.5/amd64/images.txt
```

Notes:

- The manifest-driven entry point builds `conntrack` from public netfilter
  sources inside `build-k8s-extension-package.sh`, then packages that binary
  into the k8s-extension `configs.tar.gz`. There is no prebuilt-binary input;
  each k8s-extension package build compiles conntrack from source.
- The conntrack source builder installs compiler dependencies from the host
  package manager and builds natively when the target architecture matches the
  builder. Docker is only a cross-architecture fallback. The standard proxy
  variables work for the native build; `KC_APT_MIRROR`, `KC_DOCKER_PROXY`, and
  `KC_DOCKER_NETWORK` apply to the Docker fallback.
- `build-calico-package.sh` ships image lists for `v3.26.1`, `v3.29.6`, and
  `v3.31.5`. Use `--images-file` for other versions.
- `build-addon-package.sh` covers optional image/chart style addons such as
  `nvidia-dra-driver-gpu` and `nvidia-gpu-operator`. These are not part of the
  default core cluster release manifest.
- `build-runtime-image-set.sh` covers components whose Kubernetes manifests are
  embedded in KubeClipper and therefore need only standard runtime images. NFS
  and MetalLB use this path and do not publish package images.
- `build-k8s-extension-package.sh` builds the Kubernetes helper tool package
  from public upstream downloads: Helm, etcdctl, conntrack, nerdctl, CNI
  plugins, calicoctl, and the bundled debug image list.
- Some component charts are not available from a stable public Helm repo. For
  those components, pass `--chart-file`; the scripts do not fall back to
  private static servers.
- The manifest-driven entry point writes `images.txt` and `images.lock`, then
  `push-runtime-images.sh` mirrors the listed runtime images directly into the
  target Registry with `crane` or `skopeo`. It also writes
  `release-manifest.yaml` for Registry mirroring, offline bundle construction,
  and optional delivery verification.
- Standard runtime images are never embedded into KubeClipper package images.
  Installation never loads `images.tar.gz` locally and does not perform a
  server-side runtime image precheck; actual node image pulls are authoritative.
- The chart archives are build artifacts for `helm push`; installation pulls
  charts from Helm OCI instead of downloading `charts.tgz` from a KubeClipper
  package layer.
- Legacy `manifest.json` files from the static-server layout are ignored; OCI
  package manifests are generated during publish.

Old script mapping:

| Old package script | Open builder |
| --- | --- |
| `tarball-kubernetes.sh` | `build-k8s-package.sh` |
| `tarball-containerd.sh` | `build-containerd-package.sh` |
| `tarball-calico.sh` | `build-calico-package.sh` |
| `tarball-k8s-extension.sh` | `build-k8s-extension-package.sh` |
| `nfs/v4.0.2` and `nfs/v4.1.0` image archives | `build-runtime-image-set.sh --name nfs` |
| `metallb` image archive | `build-runtime-image-set.sh --name metallb` |
| `tarball-nvidia-dra-driver-gpu.sh` | `build-addon-package.sh --name nvidia-dra-driver-gpu` |
| `tarball-nvidia-gpu-operator.sh` | `build-addon-package.sh --name nvidia-gpu-operator` |

The old `kc-extension` package only wrapped `fanux/lvscare:v1.1.1` and
`kubeclipper/kubectl:latest`. These are now split into the dedicated
`kc-runtime` image list so KubeClipper helper images do not mix with native
Kubernetes images and are still mirrored as normal runtime images.

## 2. Publish Bootstrap Package Images

Build and publish the four standard OCI package images independently. Each
script prepares its own inputs from source or public release assets, then pushes
the resulting package image. `--registry-prefix` defaults to
`ghcr.io/kubeclipper/kubeclipper`.

```bash
scripts/open-packaging/publish-bootstrap-kubeclipper.sh \
  --version v2.0.0 \
  --arch amd64

scripts/open-packaging/publish-bootstrap-etcd.sh \
  --arch amd64

scripts/open-packaging/publish-bootstrap-console.sh \
  --version v2.0.0 \
  --arch amd64

scripts/open-packaging/publish-bootstrap-registry.sh \
  --arch amd64
```

Use `--registry-prefix 10.0.0.10:5000` when publishing to a private Registry
instead of the default `ghcr.io/kubeclipper/kubeclipper`.

Default inputs:

- `publish-bootstrap-kubeclipper.sh` builds `kubeclipper-server` and
  `kubeclipper-agent` from this source tree.
- `publish-bootstrap-etcd.sh` downloads `etcd`, `etcdctl`, and `etcdutl` from
  the etcd GitHub release.
- `publish-bootstrap-console.sh` downloads `caddy` from the Caddy GitHub
  release and downloads `kc-console.tar.gz` from the matching
  `kubeclipper/console` GitHub release. The package content name is
  `kc-console`.
- `publish-bootstrap-registry.sh` downloads `registry` from
  `distribution/distribution` GitHub release.

`kcctl registry deploy` detects the target node architecture over SSH when
`--arch` is omitted, then selects the matching platform from the bootstrap
Registry image. `--arch` remains available as an explicit override.

The published image refs follow the KubeClipper package layout:

```text
<registry>/package-bootstrap-kubeclipper:<version> # server + agent
<registry>/package-bootstrap-etcd:<version>        # etcd + etcdctl + etcdutl
<registry>/package-bootstrap-console:<version>     # caddy + kc-console
<registry>/package-bootstrap-registry:<version>    # registry
```

## 3. Publish Cluster Resource Packages

Cluster resource publishers follow the bootstrap style: one script fetches
upstream resources, builds the package payload, and publishes it to Registry.
The local `resource/` tree is only an internal temporary directory.

```bash
scripts/open-packaging/publish-resource-k8s.sh \
  --registry-prefix 10.0.0.10:5000 \
  --version v1.37.0

scripts/open-packaging/publish-resource-k8s-extension.sh \
  --registry-prefix 10.0.0.10:5000 \
  --version v1

scripts/open-packaging/publish-resource-containerd.sh \
  --registry-prefix 10.0.0.10:5000 \
  --version 2.2.4

scripts/open-packaging/publish-resource-calico.sh \
  --registry-prefix 10.0.0.10:5000 \
  --version v3.31.5

scripts/open-packaging/publish-resource-kc-runtime.sh \
  --image-registry-prefix 10.0.0.10:5000 \
  --version v2.0.0

scripts/open-packaging/publish-resource-nfs.sh \
  --image-registry-prefix 10.0.0.10:5000 \
  --version v4.1.0

scripts/open-packaging/publish-resource-metallb.sh \
  --image-registry-prefix 10.0.0.10:5000 \
  --version v0.13.7
```

`publish-resource-calico.sh` publishes `charts.tgz` inside the standard package
image `package-calico:<version>`. The Registry inventory reads
the package manifest and its architecture. Agents verify and extract the package,
then pass the original chart archive to Helm. No native Helm OCI repository is
required.

Runtime images are mirrored separately from `images.lock`, also by component:

```bash
scripts/open-packaging/push-runtime-images.sh \
  --images-lock /opt/kubeclipper-server/resource/images.lock \
  --image-registry 10.0.0.10:5000 \
  --component k8s \
  --arch amd64 \
  --version v1.37.0

scripts/open-packaging/push-runtime-images.sh \
  --images-lock /opt/kubeclipper-server/resource/images.lock \
  --image-registry 10.0.0.10:5000 \
  --component kc-runtime \
  --arch amd64 \
  --version v2.0.0

scripts/open-packaging/push-runtime-images.sh \
  --images-lock /opt/kubeclipper-server/resource/images.lock \
  --image-registry 10.0.0.10:5000 \
  --component calico \
  --arch amd64 \
  --version v3.31.5
```

The publisher deliberately separates the old static-server payloads:

- `configs.tar.gz` and bootstrap binaries are pushed as standard OCI package
  images under `package-*`. Each image contains
  `/opt/kubeclipper/resource/kc-package-manifest.json` plus
  `/opt/kubeclipper/resource/<file>`.
- Runtime images are not embedded in package images. In the open build flow,
  `images.lock` is mirrored as normal runtime images with
  `push-runtime-images.sh`.
- `charts.tgz` is stored unchanged inside the component package image. Calico
  uses `package-calico:<version>`, with an architecture descriptor
  and source revision like other packages. Agents extract and verify the chart
  archive using the package fetcher before invoking Helm. The registry only needs
  to support ordinary OCI image config and layer media types.
- The old `kc-extension` and `kubectl-terminal` resource packages are replaced
  by the `kc-runtime` image list. It keeps `fanux/lvscare:v1.1.1` and
  `kubeclipper/kubectl:latest` separate from native Kubernetes images while
  still mirroring them as normal runtime images. No `kc-runtime` package image
  is published.
- NFS and MetalLB manifests remain embedded in KubeClipper. Their Actions mirror
  only the exact standard runtime images referenced by those manifests, so the
  offline Registry bundle supports installing these existing addons without
  restoring static-server image archives.

The resource publish scripts support `--dry-run` for local validation without
pushing to a Registry. Bootstrap publish scripts are release-style entry points:
they build temporary package tarballs internally and push standard OCI images
directly to the configured Registry prefix.

## 3.1 Native Registry Sync

Because package outputs are standard OCI images, users can sync them to any
Registry implementation, including Harbor:

```bash
skopeo sync \
  --src docker \
  --dest docker \
  ghcr.io/kubeclipper/kubeclipper/package-k8s \
  harbor.local/kubeclipper/package-k8s
```

Or copy one package image by digest/tag:

```bash
skopeo copy --all \
  docker://ghcr.io/kubeclipper/kubeclipper/package-k8s:v1.37.0 \
  docker://harbor.local/kubeclipper/package-k8s:v1.37.0
```

For fully offline environments, users can also use native image archives:

```bash
docker pull ghcr.io/kubeclipper/kubeclipper/package-k8s:v1.37.0
docker save ghcr.io/kubeclipper/kubeclipper/package-k8s:v1.37.0 \
  -o kubeclipper-package-images.tar
docker load -i kubeclipper-package-images.tar
docker tag ghcr.io/kubeclipper/kubeclipper/package-k8s:v1.37.0 \
  harbor.local/kubeclipper/package-k8s:v1.37.0
docker push harbor.local/kubeclipper/package-k8s:v1.37.0
```

Charts are included in ordinary package images such as `package-calico`, so
they use the same Registry mirroring commands as other packages.

For an official release, the preferred connected-site mirror command is:

```bash
kcctl registry sync \
  --manifest release-manifest.yaml \
  --target harbor.example.com/kubeclipper \
  --target-username 'robot$kubeclipper-writer' \
  --target-password-file harbor-token \
  --target-ca-file harbor-ca.pem
```

The command reads the selected release manifest and verifies source digests
and package `sourceRevision`. A matching target digest is
skipped; an existing tag with a different digest is not overwritten.

## Offline release export and import

Use a kcctl binary from the release being exported. No Docker, containerd,
Skopeo or Python installation is required for this flow.

Release kcctl binaries embed the verified inventory with package and image
references, platforms and digests. The release workflow builds the standalone
CLI after those package digests are final. `--manifest <file>` selects an
external inventory, available as `release-manifest-<version>.yaml` in the
corresponding GitHub Release assets. Source builds without an embedded inventory
use this explicit flag. The kcctl copy inside the bootstrap package also uses
an external manifest because the inventory includes that package's own digest.
For the current v2.0.11 test candidate, the
[tested manifest](../../docs/testing/evidence/offline-bundles-20261105/release-manifest.yaml)
records the validated ACR source references.

```bash
# On a connected machine: export every artifact and its complete image index.
kcctl registry export
# Use the domestic mirror with the same embedded inventory.
KCZONE=cn kcctl registry export
# Override the source registry explicitly.
kcctl registry export --package-registry harbor.example.com/kubeclipper
```

This writes two independent OCI Image Layout archives:

- `kubeclipper-offline-<version>.tar.gz`: all release package images, charts and runtime images.
- `registry-<version>.tar.gz`: only the bootstrap/registry package, including
  its published architectures, for starting the first offline Registry.

The release archive contains `release-manifest.yaml`, `oci-layout`, `index.json`,
`blobs/` and `SHA256SUMS`. Repository/tag mappings use OCI reference annotations;
there is no custom TSV inventory. All original index and manifest bytes, media
types and compressed layers are preserved. Complete source indexes may include
platforms beyond Linux amd64/arm64; these are included to preserve their digests.
The output directory defaults to `./offline` and is created when needed.
Export rejects existing archive paths. Credentials use the same
`--source-username` and `--source-password` flags as online sync.

Online `kcctl registry sync --target <registry-prefix>` also defaults to the embedded inventory.
`KCZONE=cn` selects the Aliyun source, while `--package-registry <registry-prefix>` explicitly
selects another source. Local bundle import reads the archive's manifest and content directly.

```bash
kcctl registry export \
  --source-username your-user --source-password 'your-password'
```

Copy kcctl and both archives into the offline environment:

```bash
# Bootstrap from the small Registry-only archive.
kcctl registry deploy --node 10.0.0.10 --pk-file ~/.ssh/id_ed25519 \
  --bundle ./offline/registry-3.1.1.tar.gz

# Seed the Registry from the complete release archive.
kcctl registry sync --bundle ./offline/kubeclipper-offline-v2.0.11.tar.gz \
  --target 10.0.0.10:5000/kubeclipper-release --target-scheme http

# Install through the normal package Registry flow.
kcctl deploy --deploy-config deploy.yaml \
  --package-registry 10.0.0.10:5000/kubeclipper-release \
  --package-registry-scheme http
```

Registry deploy detects the target architecture over SSH; `--arch` can specify
amd64 or arm64 explicitly. Online deploy selects the standard bootstrap/registry
package using `--package-registry` (the official Registry by default).
Local archives must contain all referenced blobs. Missing or corrupt content
fails without falling back to any online source. Offline sync shares online
sync's target authentication, digest verification, identical-content skip and
conflicting-tag rejection. Use `--arch all` (the default) to preserve complete
multi-platform indexes. The Registry-only archive is not a release sync source.
The obsolete `registry push` command and loose-binary/Docker-archive bootstrap
source flags have been removed.

## 4. Typical End-to-End Flow

```bash
# 1. Prepare a Registry.
kcctl registry deploy \
  --node 10.0.0.10 \
  --registry-port 5000 \
  --pk-file ~/.ssh/id_ed25519 \
  --package-registry ghcr.io/kubeclipper/kubeclipper

# 2. Build and push Kubernetes/CRI/CNI package images (including charts) and runtime images.
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --arch amd64 \
  --output /data/kc-resource \
  --registry 10.0.0.10:5000 \
  --image-registry 10.0.0.10:5000 \
  --push

# 3. Publish bootstrap package images.
scripts/open-packaging/publish-bootstrap-kubeclipper.sh \
  --registry-prefix 10.0.0.10:5000 \
  --version v2.0.0 \
  --arch amd64
scripts/open-packaging/publish-bootstrap-etcd.sh \
  --registry-prefix 10.0.0.10:5000 \
  --arch amd64
scripts/open-packaging/publish-bootstrap-console.sh \
  --registry-prefix 10.0.0.10:5000 \
  --version v2.0.0 \
  --arch amd64
scripts/open-packaging/publish-bootstrap-registry.sh \
  --registry-prefix 10.0.0.10:5000 \
  --arch amd64

# 4. Deploy KubeClipper with OCI.
kcctl deploy \
  --server 10.0.0.20 \
  --agent 10.0.0.20 \
  --pk-file ~/.ssh/id_rsa \
  --package-registry 10.0.0.10:5000
```

For a production Harbor project, package publishers, verifiers, and `kcctl` use
strict HTTPS by default and accept the same Registry client inputs: username or
robot account, password/token file, custom CA, and an explicit TLS verification
override. Keep the token file at mode `0600`; do not place it directly on the
command line. A typical deployment is:

```bash
kcctl deploy \
  --server 10.0.0.20 \
  --agent 10.0.0.20 \
  --pk-file ~/.ssh/id_rsa \
  --package-registry harbor.example.com/kubeclipper \
  --package-registry-username 'robot$kubeclipper-reader' \
  --package-registry-password-file harbor-robot-token \
  --package-registry-ca-file harbor-ca.pem

```

For `kcctl registry sync`, grant a project-scoped writer robot only repository
Pull and Push. Pull is needed for the pre-copy digest/conflict check; Push is
needed to upload missing manifests and blobs. Use a different project-scoped
runtime robot with repository Pull only for `kcctl deploy`, `kcctl join`,
inventory refresh, and server/agent artifact fetches. Neither robot needs
Delete, project administration, scanner, replication, or system administration
permissions.

`--registry-scheme http` / `--package-registry-scheme http` are only for an
explicit plain-HTTP test Registry. `--registry-skip-tls-verify` and
`--package-registry-skip-tls-verify` are qualification escape hatches, not the
production policy. Dynamic inventory uses the Registry Catalog API and filters
repositories to the configured project prefix; use Harbor or another Registry
that implements `/v2/_catalog` because GHCR does not expose that API.

### Console fork 制品

当前 fork 候选的 Console 版本和前端提交固定在 `packaging/resources.yaml` 的 `bootstrap.consoleVersion` 和 `bootstrap.consoleSourceRevision`。GitHub Actions 默认从当前仓库 owner 的 `console` 仓库检出该提交并构建；可用 `KC_CONSOLE_RELEASE_REPOSITORY` 覆盖，用户 fork 对应 `lixd/console`。发布脚本校验归档中 `version.json` 的前端提交与配置一致，不一致或缺失时拒绝发布。手动发布可通过 `KC_CONSOLE_ARCHIVE` 指定已构建归档；未设置时下载对应版本的 GitHub release，并执行相同校验。新版本仍需实际发布并完成 qualification 后才能作为制品使用。

Package repositories use one flat name: `package-bootstrap-<name>` for platform
bootstrap assets and `package-<name>` for component packages, including charts.
For a namespace-scoped Registry such as
`registry.cn-hangzhou.aliyuncs.com/kubeclipper-release`, runtime repositories are
also flat (`calico-node`, `nfs-plugin`, `metallb-controller`, etc.). The publisher
and agent templates share the aliases in
`pkg/delivery/registry/image_repositories.json`. Registries configured without a
namespace retain upstream runtime image paths; CoreDNS keeps its kubeadm name
`coredns`. Package kind and platform are verified from the package manifest.
