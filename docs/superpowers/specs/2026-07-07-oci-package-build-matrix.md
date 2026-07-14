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
  --registry ghcr.io/lixd/kubeclipper \
  --image-registry ghcr.io/lixd/kubeclipper \
  --push
```

Bootstrap 二进制单独构建和发布：

```bash
scripts/open-packaging/publish-bootstrap-kubeclipper.sh \
  --registry-prefix ghcr.io/lixd/kubeclipper \
  --version v1.8.0 \
  --arch amd64

scripts/open-packaging/publish-bootstrap-etcd.sh \
  --registry-prefix ghcr.io/lixd/kubeclipper \
  --arch amd64

scripts/open-packaging/publish-bootstrap-console.sh \
  --registry-prefix ghcr.io/lixd/kubeclipper \
  --version v1.8.0 \
  --arch amd64

scripts/open-packaging/publish-bootstrap-registry.sh \
  --registry-prefix ghcr.io/lixd/kubeclipper \
  --arch amd64
```

## 2. 发布后的内容形态

| 类型 | Registry 位置 | 说明 |
| --- | --- | --- |
| KubeClipper package image | `<registry>/kubeclipper/packages/<kind>/<name>:<version>` | 标准 OCI image。镜像文件系统内保存 `/opt/kubeclipper/resource/kc-package-manifest.json` 和 `/opt/kubeclipper/resource/<file>`，用于承载 `configs.tar.gz` 或 bootstrap binary 这类非运行镜像内容。 |
| Helm OCI chart | `<registry>/kubeclipper/charts/<chart>:<version>` | 保存 chart 本体，安装阶段按 Helm OCI 拉取。 |
| runtime image | `<image-registry>/<source-image>:<tag>` | 保存 kubeadm、Calico、CSI、GPU 等运行时镜像。 |

KubeClipper package image 对外是标准 OCI image，可以被 `docker pull/save/load`、`skopeo copy/sync`、Harbor replication 处理。标准 runtime images 和 `charts.tgz` 不再二次封装进 package image；runtime images 只通过 `images.txt`/`images.lock` 同步为普通镜像。

### 2.1 打包机制和 Dockerfile

KubeClipper package image 不需要 Dockerfile。当前脚本的打包链路是：

```text
resource builder
  -> 生成 configs.tar.gz / binary / charts.tgz / images.txt
  -> publish-resource-*.sh 或 publish-bootstrap-*.sh
  -> scripts/publish-oci-package.sh
  -> tools/oci-publish
  -> 直接写入标准 OCI image 到 Registry
```

`tools/oci-publish` 使用 go-containerregistry 生成标准 OCI image：

```text
manifest: application/vnd.oci.image.manifest.v1+json
config:   application/vnd.oci.image.config.v1+json
layer:    application/vnd.oci.image.layer.v1.tar+gzip

/opt/kubeclipper/resource/kc-package-manifest.json
/opt/kubeclipper/resource/configs.tar.gz
/opt/kubeclipper/resource/<binary>
```

因此这些 package image 可以被 Docker、skopeo、crane、Harbor 当普通镜像搬运，但安装时不会用 `docker run` 启动它们。Dockerfile 只适合真正需要容器运行的镜像，例如 server container image、agent container image、console image 或 registry image。

## 3. 默认核心包

这些是当前 `packaging/resources.yaml` 默认启用、集群安装主路径需要的包。

| 包 | 默认版本 | 构建脚本 | 资源来源 | 本地产物 | 发布形态 |
| --- | --- | --- | --- | --- | --- |
| `bootstrap/kubeclipper` | `v1.8.0` | `publish-bootstrap-kubeclipper.sh` | 当前源码 `./cmd/kubeclipper-server`、`./cmd/kubeclipper-agent` | `kubeclipper-server`、`kubeclipper-agent` | package image |
| `bootstrap/etcd` | `3.5.21` | `publish-bootstrap-etcd.sh` | etcd GitHub Release | `etcd`、`etcdctl`、`etcdutl` | package image |
| `bootstrap/console` | `v1.6.0` | `publish-bootstrap-console.sh` | Caddy GitHub Release、同版本 `kubeclipper/console` GitHub Release | `caddy`、`kc-console` | package image |
| `bootstrap/registry` | `3.1.1` | `publish-bootstrap-registry.sh` | distribution GitHub Release | `registry` | package image |
| `k8s/k8s` | `v1.36.1` | `resource-builders/build-k8s-package.sh` | `dl.k8s.io`、Kubernetes release repo | `k8s/v1.36.1/amd64/configs.tar.gz` | package image |
| `k8s-extension/k8s-extension` | `v1` | `resource-builders/build-k8s-extension-package.sh` + `publish-resource-k8s-extension.sh` | etcd GitHub Release、Helm Release、netfilter source、nerdctl/CNI plugins/Calico Release | `k8s-extension/v1/amd64/configs.tar.gz` | package image |
| `kc-runtime` image list | `v1.8.0` | `resource-builders/build-kc-runtime-package.sh` + `push-runtime-images.sh` | `fanux/lvscare:v1.1.1`、`kubeclipper/kubectl:latest` | `kc-runtime/v1.8.0/amd64/images.txt` | runtime images |
| `cri/containerd` | `2.2.4` | `resource-builders/build-containerd-package.sh` | containerd GitHub Release、runc GitHub Release、cri-tools GitHub Release | `containerd/2.2.4/amd64/configs.tar.gz` | package image |
| `cni/calico` | `v3.31.5` | `resource-builders/build-calico-package.sh` | Tigera Helm repo、本仓库 image list、公开镜像仓库 | `calico/v3.31.5/amd64/charts.tgz`、`images.txt` | Helm OCI chart + runtime images |
| `nfs` runtime images | `v4.0.2`、`v4.1.0` | `resource-builders/build-runtime-image-set.sh` + `publish-resource-nfs.sh` | 公开镜像仓库 | `nfs/<version>/amd64/images.txt` | runtime images |
| `metallb` runtime images | `v0.13.7` | `resource-builders/build-runtime-image-set.sh` + `publish-resource-metallb.sh` | 公开镜像仓库 | `metallb/v0.13.7/amd64/images.txt` | runtime images |

### 3.1 k8s 包内容

`k8s/k8s:<version>` 的 `configs.tar.gz` 包含：

```text
usr/bin/kubeadm
usr/bin/kubectl
usr/bin/kubelet
usr/bin/kubelet-pre-start.sh
etc/systemd/system/kubelet.service
etc/systemd/system/kubelet.service.d/10-kubeadm.conf
opt/kc/manifest/k8s/<version>/<arch>/config/manifest.json
```

`conntrack` 的处理方式：

1. 独立维护 `resource-builders/build-conntrack-binary.sh`。
2. 打 k8s-extension 包时，`build-k8s-extension-package.sh` 每次都会自动调用它从源码编译。
3. 生成的 `conntrack` 只进入 k8s-extension `configs.tar.gz`。
4. 不单独发布 `binary/conntrack` package image。

## 4. 默认不启用但仍可开源构建的包

这些脚本已经具备公开来源，但当前 manifest 默认不启用，适合按需发布或后续 release matrix 单独开启。

| 包 | 默认/目标版本 | 构建脚本 | 资源来源 | 本地产物 | 发布形态 | 状态 |
| --- | --- | --- | --- | --- | --- | --- |
| `app/nvidia-dra-driver-gpu` | `25.8.0` | `resource-builders/build-addon-package.sh --name nvidia-dra-driver-gpu` | NVIDIA Helm repo、本仓库 image list、公开镜像仓库 | `nvidia-dra-driver-gpu/25.8.0/<arch>/charts.tgz`、`images.txt` | Helm OCI chart + runtime images | `enabled: false` |
| `app/nvidia-gpu-operator` | `v25.10.0` | `resource-builders/build-addon-package.sh --name nvidia-gpu-operator` | NVIDIA Helm repo、本仓库 image list、公开镜像仓库 | `nvidia-gpu-operator/v25.10.0/<arch>/charts.tgz`、`images.txt` | Helm OCI chart + runtime images | `enabled: false` |

以下组件不进入开源打包范围，已从 manifest 和脚本入口移除：`cri/docker`、`csi/ceph`、`csi/cinder`、`csi/kc-csi`、`csi/csi-driver-wekafs`。

## 5. Legacy/扩展包

这些不是当前核心集群安装主路径，建议不要放进默认 release，后续按扩展能力单独整理。

| 包 | 构建脚本 | 内容 | 建议 |
| --- | --- | --- | --- |
| `k8s-extension/k8s-extension` | `resource-builders/build-k8s-extension-package.sh` | Helm、etcdctl、conntrack、nerdctl、CNI plugins、calicoctl、debug 镜像列表 | 默认发布；作为 k8s 安装前置工具包。 |

旧 `kc-extension` 和 `kubectl-terminal` 包不再构建。它们原来包含的
`fanux/lvscare:v1.1.1`、`kubeclipper/kubectl:latest` 改为独立
`kc-runtime` image list 管理，镜像本体仍作为标准 runtime image 同步到
Registry，不再发布 `extension/kc-runtime` package image。

默认 release 提供 K8s、containerd、k8s-extension、Calico 的 package/chart 发布入口；kc-runtime、NFS 和 MetalLB 只同步标准 runtime images，不发布空 package image。Legacy/扩展包后续如仍需要，应单独设计对应发布入口，不复用默认发布流程。

## 6. 构建顺序建议

GitHub Actions 按组件提供十一个独立发布入口。`bootstrap/kubeclipper` 跟随
`main`、`master`、`release-*` 和 Git tag 自动构建；其他 bootstrap、K8s、
containerd、k8s-extension、Calico、kc-runtime、NFS 和 MetalLB 组件按版本手动构建。
runtime image 同步属于对应组件 Action 的一部分，不再单独运行一个全量
release workflow。

本地一键构建时可以直接跑：

```bash
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --arch amd64 \
  --output /data/kc-resource \
  --registry ghcr.io/lixd/kubeclipper \
  --image-registry ghcr.io/lixd/kubeclipper \
  --push
```

## 7. 当前缺口

| 缺口 | 说明 | 建议 |
| --- | --- | --- |
| sha256 lock | 当前下载源没有统一 checksum lock。 | 后续增加 `checksums.lock`，CI 校验下载产物。 |
| arm64 验证 | `bootstrap/kubeclipper` Action 自动构建 amd64/arm64；其他组件可手动选择 `arm64/all`。 | 新增上游版本时分别确认其 arm64 下载和 runtime image 可用性。 |
| 可选 addon chart 来源 | 新增可选 addon 前必须确认公开来源、许可证和稳定 chart 来源。 | 不满足开源条件的组件不进入 manifest 或脚本入口。 |
| registry binary source | `bootstrap/registry` 使用 `distribution/distribution` GitHub Release，例如 `registry_3.1.1_linux_amd64.tar.gz`。 | 脚本内置官方下载地址，只传版本和架构。 |
