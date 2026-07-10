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
  `kubeclipper/packages/...`. The image filesystem stores files under
  `/opt/kubeclipper/resource/`.
- `charts.tgz` becomes a native Helm OCI chart under `kubeclipper/charts/...`.
- `images.lock` is copied with `crane` or `skopeo` by
  `push-runtime-images.sh`. It is release-side metadata and is never consumed
  by KubeClipper server during cluster creation.
- `release-manifest.yaml` aggregates package images, Helm charts, and runtime
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
  --resolve-digests
```

Verification is optional and does not block `kcctl create cluster`:

```bash
scripts/open-packaging/verify-release-manifest.sh \
  --manifest /data/kc-resource/release-manifest.yaml \
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

The automatic KubeClipper workflow derives its package tag from the Git ref:

```text
Git tag v1.8.0       -> v1.8.0
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
  k8s/v1.36.1/amd64/configs.tar.gz
  k8s/v1.36.1/amd64/images.txt
  kc-runtime/v1.8.0/amd64/images.txt
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
`ghcr.io/lixd/kubeclipper`.

```bash
scripts/open-packaging/publish-bootstrap-kubeclipper.sh \
  --version v1.8.0 \
  --arch amd64

scripts/open-packaging/publish-bootstrap-etcd.sh \
  --arch amd64

scripts/open-packaging/publish-bootstrap-console.sh \
  --version v1.8.0 \
  --arch amd64

scripts/open-packaging/publish-bootstrap-registry.sh \
  --arch amd64
```

Use `--registry-prefix 10.0.0.10:5000` when publishing to a private Registry
instead of the default `ghcr.io/lixd/kubeclipper`.

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

The published image refs follow the KubeClipper package layout:

```text
<registry>/kubeclipper/packages/bootstrap/kubeclipper:<version> # server + agent
<registry>/kubeclipper/packages/bootstrap/etcd:<version>        # etcd + etcdctl + etcdutl
<registry>/kubeclipper/packages/bootstrap/console:<version>     # caddy + kc-console
<registry>/kubeclipper/packages/bootstrap/registry:<version>    # registry
```

## 3. Publish Cluster Resource Packages

Cluster resource publishers follow the bootstrap style: one script fetches
upstream resources, builds the package payload, and publishes it to Registry.
The local `resource/` tree is only an internal temporary directory.

```bash
scripts/open-packaging/publish-resource-k8s.sh \
  --registry-prefix 10.0.0.10:5000 \
  --version v1.36.1

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
  --version v1.8.0
```

`publish-resource-calico.sh` publishes `charts.tgz` as the native Helm OCI
chart `kubeclipper/charts/tigera-operator:<version>`. KubeClipper keeps
`cni/calico:<version>` as the user-facing component name and maps it to that
chart in the Registry inventory resolver; no manifest-only Calico package image
is published.

Runtime images are mirrored separately from `images.lock`, also by component:

```bash
scripts/open-packaging/push-runtime-images.sh \
  --images-lock /opt/kubeclipper-server/resource/images.lock \
  --image-registry 10.0.0.10:5000 \
  --component k8s \
  --arch amd64 \
  --version v1.36.1

scripts/open-packaging/push-runtime-images.sh \
  --images-lock /opt/kubeclipper-server/resource/images.lock \
  --image-registry 10.0.0.10:5000 \
  --component kc-runtime \
  --arch amd64 \
  --version v1.8.0

scripts/open-packaging/push-runtime-images.sh \
  --images-lock /opt/kubeclipper-server/resource/images.lock \
  --image-registry 10.0.0.10:5000 \
  --component calico \
  --arch amd64 \
  --version v3.31.5
```

The publisher deliberately separates the old static-server payloads:

- `configs.tar.gz` and bootstrap binaries are pushed as standard OCI package
  images under `kubeclipper/packages/...`. Each image contains
  `/opt/kubeclipper/resource/kc-package-manifest.json` plus
  `/opt/kubeclipper/resource/<file>`.
- Runtime images are not embedded in package images. In the open build flow,
  `images.lock` is mirrored as normal runtime images with
  `push-runtime-images.sh`.
- `charts.tgz` is pushed as a native Helm OCI chart under
  `kubeclipper/charts/...`.
- Chart publishing prefers `bin/helm-oci-publish` (or `HELM_OCI_PUBLISH_BIN`)
  so HTTP/insecure registries work even when the installed Helm does not
  support `--plain-http`; if that binary is unavailable, the script falls back
  to `helm push`.
- Chart-only components are resolved from Helm OCI directly. For example,
  `cni/calico:v3.31.5` maps to
  `kubeclipper/charts/tigera-operator:v3.31.5`; no empty package image is
  needed.
- The old `kc-extension` and `kubectl-terminal` resource packages are replaced
  by the `kc-runtime` image list. It keeps `fanux/lvscare:v1.1.1` and
  `kubeclipper/kubectl:latest` separate from native Kubernetes images while
  still mirroring them as normal runtime images. No `kc-runtime` package image
  is published.

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
  ghcr.io/lixd/kubeclipper/kubeclipper/packages/k8s/k8s \
  harbor.local/kubeclipper/packages/k8s
```

Or copy one package image by digest/tag:

```bash
skopeo copy --all \
  docker://ghcr.io/lixd/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1 \
  docker://harbor.local/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1
```

For fully offline environments, users can also use native image archives:

```bash
docker pull ghcr.io/lixd/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1
docker save ghcr.io/lixd/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1 \
  -o kubeclipper-package-images.tar
docker load -i kubeclipper-package-images.tar
docker tag ghcr.io/lixd/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1 \
  harbor.local/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1
docker push harbor.local/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1
```

Helm charts remain Helm OCI artifacts, but they were verified with
`skopeo copy/sync` and can be mirrored with the same Registry tooling.

## 4. Typical End-to-End Flow

```bash
# 1. Prepare a Registry.
kcctl registry deploy \
  --node 10.0.0.10 \
  --registry-port 5000 \
  --package-registry ghcr.io/lixd/kubeclipper

# 2. Build and push Kubernetes/CRI/CNI package images, Helm charts, and runtime images.
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
  --version v1.8.0 \
  --arch amd64
scripts/open-packaging/publish-bootstrap-etcd.sh \
  --registry-prefix 10.0.0.10:5000 \
  --arch amd64
scripts/open-packaging/publish-bootstrap-console.sh \
  --registry-prefix 10.0.0.10:5000 \
  --version v1.8.0 \
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
