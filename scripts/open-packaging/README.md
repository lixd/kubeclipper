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
- Local legacy `resource` directory, such as `/opt/kubeclipper-server/resource`,
  when migrating an already-built package set.
- A target OCI Registry.

They do not depend on `OFFLINE_URL_PREFIX`, static content servers, or private
download URLs.

## 0. Manifest-Driven Build

The preferred entry point is the manifest-driven builder:

```bash
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --output /data/kc-resource
```

It reads `packaging/resources.yaml`, expands the version/architecture matrix,
calls the per-component builders, and writes release metadata:

```text
/data/kc-resource/
  k8s/v1.36.1/amd64/configs.tar.gz
  k8s/v1.36.1/amd64/images.txt
  containerd/2.2.4/amd64/configs.tar.gz
  calico/v3.31.5/amd64/charts.tgz
  calico/v3.31.5/amd64/images.txt
  images.lock
  charts.lock
  build-report.json
```

To publish the split OCI outputs in one command:

```bash
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --output /data/kc-resource \
  --registry 10.0.0.10:5000 \
  --image-registry 10.0.0.10:5000 \
  --push
```

Publishing is split by content type:

- `configs.tar.gz` becomes a standard OCI package image under
  `kubeclipper/packages/...`. The image filesystem stores files under
  `/package/`.
- `charts.tgz` becomes a native Helm OCI chart under `kubeclipper/charts/...`.
- `images.lock` is copied with `crane` or `skopeo` by
  `push-runtime-images.sh`; the install path expects runtime images to already
  exist in the image Registry.

Package images are built by `tools/oci-publish` through go-containerregistry.
They do not need a Dockerfile. The generated image is still a normal OCI image:

```text
manifest: application/vnd.oci.image.manifest.v1+json
config:   application/vnd.oci.image.config.v1+json
layer:    application/vnd.oci.image.layer.v1.tar+gzip

/package/kc-package-manifest.json
/package/configs.tar.gz
/package/kubeclipper-agent
```

Use a Dockerfile only for real runnable component images, for example
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
- `docker` or `podman` only when `--build-image-archives` is set for legacy
  migration/debug artifacts.

Useful network controls:

```bash
KC_DOWNLOAD_RETRIES=3
KC_DOWNLOAD_CONNECT_TIMEOUT=20
KC_DOWNLOAD_MAX_TIME=900
KC_HELM_TIMEOUT=900
```

If the build environment cannot reach an upstream source directly, put a public
mirror URL or local input in `packaging/resources.yaml`, for example
`chartFile`, `chartUrl`, `imagesFile`, `containerdUrlTemplate`,
`etcdUrlTemplate`, or `helmUrlTemplate`.

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
  --output /data/kc-resource \
  --skip-images
```

The output layout is the legacy resource layout consumed by the publisher.
The publisher splits this layout into native OCI outputs instead of wrapping
everything into one large static-server package:

```text
/data/kc-resource/
  k8s/v1.36.1/amd64/configs.tar.gz
  k8s/v1.36.1/amd64/images.txt
  containerd/2.2.4/amd64/configs.tar.gz
  calico/v3.31.5/amd64/charts.tgz
  calico/v3.31.5/amd64/images.txt
```

Notes:

- The manifest-driven entry point builds `conntrack` from public netfilter
  sources inside `build-k8s-package.sh`, then packages that binary into the
  Kubernetes `configs.tar.gz`. If you call `build-k8s-package.sh` directly,
  it uses the same default build path; pass `--conntrack-file` or
  `--conntrack-url` only when you want to override the input.
- `build-calico-package.sh` ships image lists for `v3.26.1`, `v3.29.6`, and
  `v3.31.5`. Use `--images-file` for other versions.
- `build-addon-package.sh` covers image/chart style components:
  `kc-extension`, `kubectl-terminal`, `nvidia-dra-driver-gpu`, and
  `nvidia-gpu-operator`. These are optional and are not part of the default
  core cluster release manifest.
- `build-k8s-extension-package.sh` builds the debug-tool extension package
  from public upstream downloads: Helm, nerdctl, CNI plugins, calicoctl, and
  the bundled debug image list.
- Some component charts are not available from a stable public Helm repo. For
  those components, pass `--chart-file` or `--chart-url`; the scripts do not
  fall back to private static servers.
- The manifest-driven entry point skips image archives by default. It writes
  `images.txt` and `images.lock`, then `push-runtime-images.sh` mirrors the
  listed runtime images directly into the target Registry with `crane` or
  `skopeo`.
- Pass `--build-image-archives` only when you intentionally need legacy
  `images.tar.gz` migration/debug artifacts. Installation never loads
  `images.tar.gz` locally.
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
| `tarball-kc-extension.sh` | `build-addon-package.sh --name kc-extension` |
| default `kubectl-terminal` extension | `build-addon-package.sh --name kubectl-terminal` |

## 2. Publish Bootstrap Binary Artifacts

Build bootstrap binaries from public/source inputs first:

```bash
scripts/open-packaging/bootstrap-builders/build-bootstrap-binaries.sh \
  --output-dir /data/kc-bootstrap-bin \
  --arch amd64 \
  --kc-version v1.8.0
```

This builds `kcctl`, `kubeclipper-server`, and `kubeclipper-agent` from the
current source tree, then downloads `etcd`, `etcdctl`, `etcdutl`, and `caddy`
from public releases. The first Registry is still best bootstrapped from a
registry image such as `registry:2`; if you also need a `binary/registry`
artifact, pass `--registry-url` or `--registry-file`.

Build or collect KubeClipper bootstrap binaries and publish each binary as a
standard OCI package image:

```bash
scripts/open-packaging/publish-bootstrap-artifacts.sh \
  --registry 10.0.0.10:5000 \
  --version v1.8.0 \
  --arch amd64 \
  --build-core \
  --bin-dir /data/kc-bootstrap-bin \
  --console-dir /data/kc-console \
  --dry-run
```

`--build-core` builds these binaries from the current source tree:

- `kcctl`
- `kubeclipper-server`
- `kubeclipper-agent`

Other bootstrap assets must be provided in `--bin-dir`:

- `caddy`
- `registry`
- `etcd`
- `etcdctl`
- `etcdutl`

`kc-console` can be provided by either:

- `--console-archive /path/to/kc-console.tar.gz`
- `--console-dir /path/to/kc-console`

The published image refs follow the KubeClipper package layout:

```text
<registry>/kubeclipper/packages/binary/kcctl:<version>
<registry>/kubeclipper/packages/binary/kubeclipper-server:<version>
<registry>/kubeclipper/packages/binary/kubeclipper-agent:<version>
```

## 3. Publish Resource Directory

Convert a generated or existing local resource directory to OCI-backed
delivery images:

```bash
scripts/open-packaging/publish-resource-artifacts.sh \
  --resource-dir /opt/kubeclipper-server/resource \
  --registry 10.0.0.10:5000 \
  --arch amd64 \
  --dry-run
```

Supported input layout:

```text
resource/
  k8s/v1.36.1/amd64/configs.tar.gz
  k8s/v1.36.1/amd64/images.tar.gz
  containerd/2.2.4/amd64/configs.tar.gz
  calico/v3.31.5/amd64/charts.tgz
  calico/v3.31.5/amd64/images.tar.gz
```

To also publish `charts.tgz` archives as Helm OCI charts:

```bash
scripts/open-packaging/publish-resource-artifacts.sh \
  --resource-dir /opt/kubeclipper-server/resource \
  --registry 10.0.0.10:5000 \
  --arch amd64 \
  --push-charts
```

`--push-images` still exists only for migrating an existing legacy resource
directory that already contains `images.tar.gz`. The open build flow does not
use it; it mirrors runtime images from `images.lock` with
`push-runtime-images.sh`.

The publisher deliberately separates the old static-server payloads:

- `configs.tar.gz` and bootstrap binaries are pushed as standard OCI package
  images under `kubeclipper/packages/...`. Each image contains
  `/package/kc-package-manifest.json` plus `/package/<file>`.
- Runtime images are not embedded in package images. In the open build flow,
  `images.lock` is mirrored as normal runtime images with
  `push-runtime-images.sh`.
- `charts.tgz` is pushed as a native Helm OCI chart under
  `kubeclipper/charts/...`.
- Chart publishing prefers `bin/helm-oci-publish` (or `HELM_OCI_PUBLISH_BIN`)
  so HTTP/insecure registries work even when the installed Helm does not
  support `--plain-http`; if that binary is unavailable, the script falls back
  to `helm push`.
- When a component only needs a chart, the KubeClipper package image is a tiny
  manifest-only descriptor that points to the Helm OCI chart. It does not
  contain the chart bytes.
- Legacy extension resources (`k8s-extension`, `kc-extension`, and
  `kubectl-terminal`) are skipped by default because they are not part of the
  core cluster install path. Use `--include-extensions` only when publishing
  those legacy resources intentionally.

Both scripts support `--dry-run` for local validation without pushing to a
Registry.

## 3.1 Native Registry Sync

Because package outputs are standard OCI images, users can sync them to any
Registry implementation, including Harbor:

```bash
skopeo sync \
  --src docker \
  --dest docker \
  docker.io/kubeclipper/kubeclipper/packages/k8s/k8s \
  harbor.local/kubeclipper/packages/k8s
```

Or copy one package image by digest/tag:

```bash
skopeo copy --all \
  docker://docker.io/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1 \
  docker://harbor.local/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1
```

For fully offline environments, users can also use native image archives:

```bash
docker pull docker.io/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1
docker save docker.io/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1 \
  -o kubeclipper-package-images.tar
docker load -i kubeclipper-package-images.tar
docker tag docker.io/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1 \
  harbor.local/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1
docker push harbor.local/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1
```

Helm charts remain Helm OCI artifacts, but they were verified with
`skopeo copy/sync` and can be mirrored with the same Registry tooling.

## 4. Typical End-to-End Flow

```bash
# 1. Prepare a Registry.
kcctl registry deploy --node 10.0.0.10 --registry-port 5000

# 2. Build and push Kubernetes/CRI/CNI package images, Helm charts, and runtime images.
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --arch amd64 \
  --output /data/kc-resource \
  --registry 10.0.0.10:5000 \
  --image-registry 10.0.0.10:5000 \
  --push

# 3. Publish bootstrap binaries.
scripts/open-packaging/publish-bootstrap-artifacts.sh \
  --registry 10.0.0.10:5000 \
  --version v1.8.0 \
  --arch amd64 \
  --build-core \
  --bin-dir /data/kc-bootstrap-bin \
  --console-dir /data/kc-console

# 4. Deploy KubeClipper with OCI.
kcctl deploy \
  --server 10.0.0.20 \
  --agent 10.0.0.20 \
  --pk-file ~/.ssh/id_rsa \
  --package-registry 10.0.0.10:5000
```
