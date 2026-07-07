---
comet_change: zero-to-oci-offline-package-build
role: package-build-matrix
canonical_spec: openspec
---

# KubeClipper OCI 打包清单

本文整理 OCI 替换 static server 后，需要从零构建的包、资源来源、构建脚本、产物和发布方式。

目标是让后续 GitHub Actions 或本地 release 环境只依赖公开 upstream、本仓库脚本和目标 Registry，不再依赖内部 content server、旧 static server 或已经存在的离线包目录。

## 1. 总入口

推荐入口是 manifest 驱动构建：

```bash
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --arch amd64 \
  --output /data/kc-resource
```

构建并发布到 Registry：

```bash
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --arch amd64 \
  --output /data/kc-resource \
  --registry ghcr.io/kubeclipper \
  --image-registry ghcr.io/kubeclipper \
  --push
```

Bootstrap 二进制单独构建和发布：

```bash
scripts/open-packaging/bootstrap-builders/build-bootstrap-binaries.sh \
  --output-dir /data/kc-bootstrap-bin \
  --arch amd64 \
  --kc-version v1.8.0

scripts/open-packaging/publish-bootstrap-artifacts.sh \
  --registry ghcr.io/kubeclipper \
  --version v1.8.0 \
  --arch amd64 \
  --bin-dir /data/kc-bootstrap-bin
```

## 2. 发布后的内容形态

| 类型 | Registry 位置 | 说明 |
| --- | --- | --- |
| KubeClipper package image | `<registry>/kubeclipper/packages/<kind>/<name>:<version>` | 标准 OCI image。镜像文件系统内保存 `/package/kc-package-manifest.json` 和 `/package/<file>`，用于承载 `configs.tar.gz` 或 bootstrap binary 这类非运行镜像内容。 |
| Helm OCI chart | `<registry>/kubeclipper/charts/<chart>:<version>` | 保存 chart 本体，安装阶段按 Helm OCI 拉取。 |
| runtime image | `<image-registry>/<source-image>:<tag>` | 保存 kubeadm、Calico、CSI、GPU 等运行时镜像。 |

KubeClipper package image 对外是标准 OCI image，可以被 `docker pull/save/load`、`skopeo copy/sync`、Harbor replication 处理。`images.tar.gz` 和 `charts.tgz` 不再二次封装进 package image；它们只是构建/迁移过程的中间产物。

### 2.1 打包机制和 Dockerfile

KubeClipper package image 不需要 Dockerfile。当前脚本的打包链路是：

```text
resource builder
  -> 生成 configs.tar.gz / binary / charts.tgz / images.tar.gz
  -> publish-resource-artifacts.sh 或 publish-bootstrap-artifacts.sh
  -> scripts/publish-oci-package.sh
  -> tools/oci-publish
  -> 直接写入标准 OCI image 到 Registry
```

`tools/oci-publish` 使用 go-containerregistry 生成标准 OCI image：

```text
manifest: application/vnd.oci.image.manifest.v1+json
config:   application/vnd.oci.image.config.v1+json
layer:    application/vnd.oci.image.layer.v1.tar+gzip

/package/kc-package-manifest.json
/package/configs.tar.gz
/package/<binary>
```

因此这些 package image 可以被 Docker、skopeo、crane、Harbor 当普通镜像搬运，但安装时不会用 `docker run` 启动它们。Dockerfile 只适合真正需要容器运行的镜像，例如 server container image、agent container image、console image 或 registry image。

## 3. 默认核心包

这些是当前 `packaging/resources.yaml` 默认启用、集群安装主路径需要的包。

| 包 | 默认版本 | 构建脚本 | 资源来源 | 本地产物 | 发布形态 |
| --- | --- | --- | --- | --- | --- |
| `binary/kcctl` | `v1.8.0` | `bootstrap-builders/build-bootstrap-binaries.sh` + `publish-bootstrap-artifacts.sh` | 当前源码 `./cmd/kcctl` | `/data/kc-bootstrap-bin/kcctl` | package image；正式 release 可只放 GitHub Release |
| `binary/kubeclipper-server` | `v1.8.0` | `bootstrap-builders/build-bootstrap-binaries.sh` + `publish-bootstrap-artifacts.sh` | 当前源码 `./cmd/kubeclipper-server` | `/data/kc-bootstrap-bin/kubeclipper-server` | package image |
| `binary/kubeclipper-agent` | `v1.8.0` | `bootstrap-builders/build-bootstrap-binaries.sh` + `publish-bootstrap-artifacts.sh` | 当前源码 `./cmd/kubeclipper-agent` | `/data/kc-bootstrap-bin/kubeclipper-agent` | package image |
| `binary/etcd` | `3.5.21` | `bootstrap-builders/build-bootstrap-binaries.sh` + `publish-bootstrap-artifacts.sh` | etcd GitHub Release | `/data/kc-bootstrap-bin/etcd` | package image |
| `binary/etcdctl` | `3.5.21` | `bootstrap-builders/build-bootstrap-binaries.sh` + `publish-bootstrap-artifacts.sh` | etcd GitHub Release | `/data/kc-bootstrap-bin/etcdctl` | package image |
| `binary/etcdutl` | `3.5.21` | `bootstrap-builders/build-bootstrap-binaries.sh` + `publish-bootstrap-artifacts.sh` | etcd GitHub Release | `/data/kc-bootstrap-bin/etcdutl` | package image |
| `binary/caddy` | `2.10.2` | `bootstrap-builders/build-bootstrap-binaries.sh` + `publish-bootstrap-artifacts.sh` | Caddy GitHub Release | `/data/kc-bootstrap-bin/caddy` | package image |
| `k8s/k8s` | `v1.36.1` | `resource-builders/build-k8s-package.sh` | `dl.k8s.io`、etcd GitHub Release、Helm Release、Kubernetes release repo、netfilter source | `k8s/v1.36.1/amd64/configs.tar.gz` | package image |
| `cri/containerd` | `2.2.4` | `resource-builders/build-containerd-package.sh` | containerd GitHub Release、runc GitHub Release、cri-tools GitHub Release | `containerd/2.2.4/amd64/configs.tar.gz` | package image |
| `cni/calico` | `v3.31.5` | `resource-builders/build-calico-package.sh` | Tigera Helm repo、本仓库 image list、公开镜像仓库 | `calico/v3.31.5/amd64/charts.tgz`、`images.tar.gz`、`images.txt` | Helm OCI chart + runtime images |

### 3.1 k8s 包内容

`k8s/k8s:<version>` 的 `configs.tar.gz` 包含：

```text
usr/bin/kubeadm
usr/bin/kubectl
usr/bin/kubelet
usr/bin/conntrack
usr/bin/kubelet-pre-start.sh
usr/local/bin/etcdctl
usr/local/bin/helm
etc/systemd/system/kubelet.service
etc/systemd/system/kubelet.service.d/10-kubeadm.conf
opt/kc/manifest/k8s/<version>/<arch>/config/manifest.json
```

`conntrack` 的处理方式：

1. 独立维护 `resource-builders/build-conntrack-binary.sh`。
2. 打 k8s 包时，如果没有传 `--conntrack-file` 或 `--conntrack-url`，`build-k8s-package.sh` 会自动调用它。
3. 生成的 `conntrack` 只进入 k8s `configs.tar.gz`。
4. 不单独发布 `binary/conntrack` package image。

## 4. 默认不启用但仍可开源构建的包

这些脚本已经具备公开来源，但当前 manifest 默认不启用，适合按需发布或后续 release matrix 单独开启。

| 包 | 默认/目标版本 | 构建脚本 | 资源来源 | 本地产物 | 发布形态 | 状态 |
| --- | --- | --- | --- | --- | --- | --- |
| `app/nvidia-dra-driver-gpu` | `25.8.0` | `resource-builders/build-addon-package.sh --name nvidia-dra-driver-gpu` | NVIDIA Helm repo、本仓库 image list、公开镜像仓库 | `nvidia-dra-driver-gpu/25.8.0/<arch>/charts.tgz`、`images.tar.gz` | Helm OCI chart + runtime images | `enabled: false` |
| `app/nvidia-gpu-operator` | `v25.10.0` | `resource-builders/build-addon-package.sh --name nvidia-gpu-operator` | NVIDIA Helm repo、本仓库 image list、公开镜像仓库 | `nvidia-gpu-operator/v25.10.0/<arch>/charts.tgz`、`images.tar.gz` | Helm OCI chart + runtime images | `enabled: false` |

以下组件不进入开源打包范围，已从 manifest 和脚本入口移除：`cri/docker`、`csi/ceph`、`csi/cinder`、`csi/kc-csi`、`csi/csi-driver-wekafs`。

## 5. Legacy/扩展包

这些不是当前核心集群安装主路径，建议不要放进默认 release，后续按扩展能力单独整理。

| 包 | 构建脚本 | 内容 | 建议 |
| --- | --- | --- | --- |
| `extension/k8s-extension` | `resource-builders/build-k8s-extension-package.sh` | Helm、nerdctl、CNI plugins、calicoctl、debug 镜像列表 | 不默认发布；如仍需要调试工具扩展，单独发布。 |
| `extension/kc-extension` | `resource-builders/build-addon-package.sh --name kc-extension` | legacy extension 镜像 | 不默认发布。 |
| `extension/kubectl-terminal` | `resource-builders/build-addon-package.sh --name kubectl-terminal` | legacy terminal 镜像 | 不默认发布。 |

`publish-resource-artifacts.sh` 默认跳过 legacy extension。只有显式传 `--include-extensions` 才会发布。

## 6. 构建顺序建议

推荐 GitHub Actions 拆成四段：

1. Bootstrap binaries：构建并发布 `kcctl`、`kubeclipper-server`、`kubeclipper-agent`、`etcd`、`etcdctl`、`etcdutl`、`caddy`。正式用户入口中 `kcctl` 可只放 GitHub Release，其他二进制放 package image。
2. Core resources：构建并发布 `k8s/k8s`、`cri/containerd`。
3. Network resources：构建 Calico。其他 addon 只在按需发布时单独开启。
4. Runtime image mirror：根据 `images.lock` 把所有运行时镜像同步到 `image-registry`。

本地一键构建时可以直接跑：

```bash
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --arch amd64 \
  --output /data/kc-resource \
  --registry ghcr.io/kubeclipper \
  --image-registry ghcr.io/kubeclipper \
  --push
```

## 7. 当前缺口

| 缺口 | 说明 | 建议 |
| --- | --- | --- |
| sha256 lock | 当前下载源没有统一 checksum lock。 | 后续增加 `checksums.lock`，CI 校验下载产物。 |
| arm64 验证 | 脚本支持 `arm64/all`，但当前 manifest 只默认 `amd64`。 | 先跑通 amd64，再在 GitHub Actions matrix 开启 arm64。 |
| 可选 addon chart 来源 | 新增可选 addon 前必须确认公开来源、许可证和稳定 chart 来源。 | 不满足开源条件的组件不进入 manifest 或脚本入口。 |
| registry binary | 默认使用 `registry:2` 镜像启动本地 Registry，不默认构建 registry 二进制。 | 只有需要 `binary/registry` 审计或特殊部署时才传 `--registry-url` 或 `--registry-file`。 |
