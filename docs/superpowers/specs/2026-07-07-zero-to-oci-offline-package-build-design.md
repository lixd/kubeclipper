---
comet_change: zero-to-oci-offline-package-build
role: technical-design
canonical_spec: openspec
---

# KubeClipper 从零构建 OCI 离线包设计

## 1. 背景

OCI 替换 static server 后，安装阶段已经不再依赖 `/opt/kubeclipper-server/resource`
这类静态资源目录，也不应该再依赖旧的内容服务器。

但当前 open packaging 脚本仍然保留了两类入口：

1. 从已有 legacy `resource/` 目录迁移并发布到 Registry。
2. 通过若干 builder 从公开源或本地文件生成 legacy resource layout，再发布到 Registry。

后续要开源并放到 GitHub Actions 自动打包，需要把第二条路径设计成主路径：

**不依赖旧 static server、不依赖内部内容服务器、不依赖已经存在的离线包目录，从版本清单出发，直接生成标准 OCI package image、Helm OCI chart 和 runtime images。**

本文只设计打包与发布流水线，不重复描述安装阶段如何消费 OCI 包。

## 2. 目标

目标终态：

1. 每个离线包都有独立、可复用、可本地运行、可在 GitHub Actions 运行的打包脚本。
2. 打包输入来自公开上游、GitHub Release、Helm repository、容器镜像仓库或显式配置的镜像源。
3. 不再从 `oss.kubeclipper.io/packages/`、内部 content server、tarball server 或旧 static server 下载资源。
4. 不再要求先生成一个完整 legacy resource tree；legacy layout 只作为中间产物或迁移兼容输入。
5. `configs.tar.gz` 发布为 KubeClipper 标准 OCI package image。
6. `charts.tgz` 发布为 Helm OCI chart，不再二次包进 KubeClipper package image。
7. 不再生成或发布 `images.tar.gz`；runtime images 通过 `images.txt`/`images.lock` 直接同步成普通镜像。
8. GitHub Actions 可以通过一个版本矩阵完整构建、校验、发布所有目标资源。
9. 打包脚本作为 KubeClipper 开源仓库的一部分发布，外部贡献者可以在没有公司内部网络权限的情况下复现 release 构建。
10. 旧 `caas-cd-node` 中 KubeClipper 相关打包逻辑迁入本仓库后，内部路径、内部域名、内部 tarball 节点和未公开脚本依赖必须全部删除。

非目标：

1. 不设计安装时 fallback 到本地 `docker load` / `nerdctl load`。
2. 不把 runtime images 再封装进 KubeClipper OCI package。
3. 不保证所有历史 caas-cd-node 组件都进入第一阶段开源打包范围。
4. 不把私有闭源 chart、私有镜像或客户环境专用资源内置到开源脚本。

## 3. 开源发布约束

这次改造的核心不是把内部脚本原样搬进仓库，而是把 KubeClipper release 构建变成开源项目可审计、可复现、可贡献的公共流水线。

### 3.1 禁止依赖

开源打包脚本和 GitHub Actions 默认配置中禁止出现：

1. 公司内部服务器、内部 IP、内部域名、内部对象存储。
2. `oss.kubeclipper.io/packages/` 这类旧包下载入口。
3. 旧 static server 或 `/opt/kubeclipper-server/resource` 作为默认输入。
4. `scp` 到 tarball 节点、SSH 到打包机、依赖固定主机目录。
5. 未开源仓库中的脚本、chart、image list 或二进制。
6. 私有镜像仓库作为默认 upstream。

允许出现的输入只有：

1. 官方 upstream release。
2. GitHub Release / source archive。
3. 公开 Helm repository。
4. 公开容器镜像仓库。
5. 本仓库内已开源的脚本、模板、image list。
6. KubeClipper 官方维护的公开 mirror release，且必须带 checksum。

### 3.2 可复现要求

外部开发者应该能执行：

```bash
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --output /tmp/kc-resource
```

并得到和 CI 相同结构的产物：

```text
/tmp/kc-resource/
  k8s/<version>/<arch>/configs.tar.gz
  containerd/<version>/<arch>/configs.tar.gz
  calico/<version>/<arch>/charts.tgz
  ...
```

如果开发者没有权限推送到 KubeClipper 官方 Registry，也应该可以改成自己的 Registry：

```bash
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --output /tmp/kc-resource \
  --registry ghcr.io/<user-or-org> \
  --image-registry ghcr.io/<user-or-org> \
  --push
```

### 3.3 内部脚本迁移原则

从 `caas-cd-node` 迁移脚本时按下面规则处理：

1. 只迁 KubeClipper 仍使用或计划支持的资源。
2. 脚本命名改为 open packaging 风格，例如 `tarball-kubernetes.sh` 迁为 `build-k8s-package.sh`。
3. 参数从短选项和隐式环境变量改为显式长参数。
4. 下载源从内部内容服务器改为公开 upstream 或 build manifest 配置。
5. 上传方式从 `scp`/tarball 节点改为 OCI Registry publish。
6. 旧脚本中的临时目录、固定路径、公司环境变量全部移除。
7. 每个迁移脚本必须有 `--help`、`bash -n`、dry-run 或 smoke test。

### 3.4 CI 防回归检查

GitHub Actions PR validation 必须增加敏感依赖扫描：

```bash
rg -n \
  "oss\\.kubeclipper\\.io/packages|OFFLINE_URL|tarball server|/data/tarball|/data/tarballs|scp .*tarball|192\\.168\\.|10\\.[0-9]+\\.|registry\\.cn-|aliyuncs\\.com/.+99cloud|sh-package" \
  scripts/open-packaging packaging .github
```

命中后 PR 失败。示例 IP 如 `10.0.0.10` 只允许出现在文档示例中，不允许出现在脚本默认值或 workflow 默认配置中。

## 4. 第一性原则

### 4.1 打包流水线只生产，不参与安装

打包阶段负责生产以下三类输出：

| 输出 | 保存位置 | 消费方 |
| --- | --- | --- |
| KubeClipper package image | `packageRegistry/kubeclipper/packages/...` | `kcctl deploy/create/join` |
| Helm OCI chart | `packageRegistry/kubeclipper/charts/...` | addon/CNI 安装步骤 |
| runtime image | `imageRegistry/<image>:<tag>` | kubelet/container runtime |

安装阶段只消费这些输出，不能再补生产缺失镜像。

### 4.2 包内容必须最小化

KubeClipper package image 只保存非容器镜像、非 Helm chart 的文件内容，例如：

1. Kubernetes / CRI 二进制和 systemd 文件。
2. KubeClipper bootstrap 二进制。
3. manifest、脚本、配置模板等小文件。

不进入 KubeClipper package image 的内容：

1. runtime image 归档或镜像本体
2. `charts.tgz`
3. Docker save 归档
4. 整个 legacy resource tree

### 4.3 版本矩阵是打包事实来源

GitHub Actions 不应该靠手写一串命令决定打哪些包。推荐新增一份 release build manifest，例如：

```yaml
apiVersion: packaging.kubeclipper.io/v1alpha1
kind: OfflineResourceBuild
release: v1.8.0
architectures:
  - amd64
  - arm64
registries:
  package: ghcr.io/lixd/kubeclipper
  image: ghcr.io/lixd/kubeclipper
resources:
  k8s:
    versions:
      - v1.36.1
  k8sExtension:
    versions:
      - v1
    etcdVersion: 3.5.21
    helmVersion: 3.18.6
    conntrack:
      version: 1.4.9
  cri:
    containerd:
      versions:
        - 2.2.4
      runcVersion: 1.3.3
      crictlVersion: 1.35.0
  cni:
    calico:
      versions:
        - v3.31.5
      chartVersion: v3.31.5
addons: {}
```

这份 manifest 后续应成为 GitHub Actions、开发者本地打包和 release 文档共同使用的输入。

### 4.4 仓库归属

第一阶段打包脚本应放在 KubeClipper 主仓库，不单独拆分新仓库。

原因：

1. 打包脚本和 KubeClipper package manifest、OCI media type、delivery policy、`kcctl` 命令行为强绑定。
2. 主仓库 PR 可以同时修改代码、包格式、脚本和文档，避免跨仓库版本漂移。
3. GitHub Actions 可以直接使用当前提交构建 `kcctl/server/agent`，不需要额外 checkout 或约定分支。
4. 开源贡献者只需要 clone 一个仓库即可复现核心 release 构建。
5. 当前还处于包格式和发布流程稳定期，过早拆仓会增加 review、兼容和权限管理成本。

后续只有在以下条件都满足时，再考虑拆出 `kubeclipper-packaging` 或 `kubeclipper-release` 仓库：

1. OCI package spec 已稳定，短期不会频繁随 KubeClipper 代码变化。
2. 打包脚本需要独立生命周期，例如支持多个 KubeClipper 大版本并行维护。
3. 需要给 release 工程团队单独授权，而不希望给主仓库写权限。
4. 打包内容明显扩展到 KubeClipper 主项目之外。

即使未来拆仓，主仓库也应保留最小入口文档和 workflow 调用方式，避免用户不知道官方打包逻辑在哪里。

### 4.5 是否需要 Dockerfile

KubeClipper package image 不需要 Dockerfile。

原因是这些镜像不是用来运行容器的，而是用 Registry 分发文件。发布工具
`tools/oci-publish` 直接通过 go-containerregistry 生成标准 OCI image：

```text
manifest: application/vnd.oci.image.manifest.v1+json
config:   application/vnd.oci.image.config.v1+json
layer:    application/vnd.oci.image.layer.v1.tar+gzip

/opt/kubeclipper/resource/kc-package-manifest.json
/opt/kubeclipper/resource/configs.tar.gz
/opt/kubeclipper/resource/kubeclipper-agent
```

这种方式的优点：

1. 不依赖 Docker daemon 或 BuildKit，GitHub Actions、本地和受限构建环境都更容易跑。
2. 不需要为 `k8s/k8s`、`cri/containerd`、`bootstrap/kubeclipper` 维护大量重复 Dockerfile。
3. 产物仍然是标准 OCI image，可以用 `docker pull/save/load`、`skopeo copy/sync`、Harbor replication。
4. `kcctl` 安装阶段可以稳定地从 `/opt/kubeclipper/resource` 目录提取文件，不受基础镜像、入口点、rootfs 布局影响。

只有真正要以容器方式运行的组件才需要 Dockerfile，例如：

1. `kubeclipper-server` 作为 server container image 发布。
2. `kubeclipper-agent` 作为 agent container image 发布。
3. `kc-console` 作为 nginx/caddy 静态站点镜像发布。
4. registry helper 或其他需要 `docker run` 的基础设施镜像。

这些 runnable image 和 package image 是两类产物，不能混在一起理解。

### 4.6 Kubekey 参考

Kubekey 的开源打包方式可以作为 release 工程参考，但不应直接照搬。

当前 Kubekey 主要有三条发布/打包线：

1. `kk` 二进制 release：使用 `.goreleaser.yaml` 构建 `linux/windows/darwin`、`amd64/arm64` 的 `kk`，产物发布到 GitHub Release。
2. 组件镜像 release：使用 GitHub Actions + Docker Buildx 构建并推送 controller/executor 等多架构镜像。
3. OS 依赖包 ISO：`hack/gen-repository-iso` 中按发行版维护 Dockerfile、`packages.yaml` 和下载逻辑，通过 `gen-repository-iso.yaml` workflow 构建 `.iso`，上传到 GitHub Release 的 `iso-latest` tag。

可借鉴的点：

1. 把打包脚本放在主仓库，和代码、配置、文档一起演进。
2. 用 GitHub Actions matrix 明确列出要构建的 OS / arch / component 组合。
3. 用 Docker Buildx 的 `outputs: type=local,dest=...` 从容器构建环境导出离线产物，避免污染 runner。
4. 用一份清单文件描述依赖，例如 Kubekey 的 `packages.yaml`；KubeClipper 对应的是 `packaging/resources.yaml`。
5. 产物同时生成 checksum，便于用户下载后校验。
6. Release 中提供一个固定入口，例如 Kubekey 的 `iso-latest`；KubeClipper 可以保留稳定的 `resources.yaml`、`images.lock` 和 release report。

不照搬的点：

1. Kubekey 的 OS 依赖包是 `.iso`，适合 rpm/deb 仓库离线安装；KubeClipper 的 Kubernetes/CRI/CNI 资源应发布到 OCI Registry，而不是 GitHub Release ISO。
2. Kubekey 的 offline artifact 仍是 `artifact.tgz` 大包，并通过 `kk artifact images --push` 把镜像推入私有仓库；KubeClipper 目标是拆成 package image、Helm OCI chart、runtime image，不重新合成大包。
3. Kubekey workflow 中同步到对象存储的步骤依赖项目自己的 secret 和外部对象存储工具；KubeClipper 开源 workflow 默认只发布到 GitHub Release / OCI Registry，不依赖公司内部或私有对象存储。
4. Kubekey 的 `iso-latest` 是移动 tag；KubeClipper 的可安装 package image 应以版本 tag 和 digest 为准，避免安装输入随时间漂移。

因此 KubeClipper 可以采用类似目录结构和 CI 组织方式：

```text
packaging/resources.yaml
scripts/open-packaging/resource-builders/
scripts/open-packaging/bootstrap-builders/
.github/workflows/offline-resource-validate.yaml
.github/workflows/offline-resource-release.yaml
```

但最终发布面保持 OCI-native：

```text
GitHub Release:
  kcctl
  resources.yaml
  images.lock
  build-report.json
  checksums.txt

OCI package registry:
  kubeclipper/packages/...
  kubeclipper/charts/...

OCI image registry:
  kubeadm / calico / addon runtime images
```

## 5. 离线包分类

### 5.1 Bootstrap binary packages

用于部署 KubeClipper 自身。

| 包 | OCI kind/name | 内容 | 来源 |
| --- | --- | --- | --- |
| `kubeclipper` | `bootstrap/kubeclipper` | server、agent 二进制 | 当前仓库 `go build ./cmd/kubeclipper-server`、`./cmd/kubeclipper-agent` |
| `etcd` | `bootstrap/etcd` | etcd、etcdctl、etcdutl 二进制 | etcd GitHub Release |
| `console` | `bootstrap/console` | caddy 二进制、console 静态文件 tar | Caddy GitHub Release、同版本 `kubeclipper/console` GitHub Release |
| `registry` | `bootstrap/registry` | distribution registry 二进制 | distribution GitHub Release，例如 `registry_3.1.1_linux_amd64.tar.gz` |

Bootstrap package 合并为 4 个标准 OCI package image：

```text
<registry>/kubeclipper/packages/bootstrap/kubeclipper:<kc-version>
<registry>/kubeclipper/packages/bootstrap/etcd:<etcd-version>
<registry>/kubeclipper/packages/bootstrap/console:<kc-version>
<registry>/kubeclipper/packages/bootstrap/registry:<registry-version>
```

只合并到四类，不再做单二进制 package 的原因：

1. `server/agent`、`etcd`、`caddy`、`registry` 的生命周期不同。
2. 四个 package 已经足够减少镜像数量，同时不会把所有 bootstrap 内容重新绑成一个大包。
3. `kcctl deploy` 可以通过 resolver 按需选择组件，Registry inventory 就是组件库存。
4. 这避免把新的 OCI 流程重新退化成旧的大包耦合模型。

`kcctl` 自身只作为用户下载入口发布到 GitHub Release，不放入 package registry。

`kcctl registry deploy` 是特例。部署第一个本地 Registry 时，可以从公网或已有私有 package registry 拉取 `bootstrap/registry` package image，并从 `/opt/kubeclipper/resource/registry` 提取 registry 二进制；完全离线时再使用 image archive 或本地 binary。它应支持：

1. `--package-registry` 指向公开或已有私有 package registry。
2. `--registry-image` 指向完整 `kubeclipper/packages/bootstrap/registry:<version>` package image。
3. 本地 registry image archive。
4. 本地 registry binary。

当用户已经有可用 Registry 时，`bootstrap/registry` 可以作为后续部署、复用或审计的 package image。

当前使用四个独立脚本分别构建并发布 bootstrap package image。脚本参数使用
`--registry-prefix` 指定发布目标前缀，默认值是 `ghcr.io/lixd/kubeclipper`。
脚本产出是推送到 Registry 的标准 OCI image；本地 tar 只是内部临时中间产物，
运行结束会自动清理。

```text
scripts/open-packaging/publish-bootstrap-kubeclipper.sh
scripts/open-packaging/publish-bootstrap-etcd.sh
scripts/open-packaging/publish-bootstrap-console.sh
scripts/open-packaging/publish-bootstrap-registry.sh
```

后续可以继续把 bootstrap builder 也拆成更细的独立构建脚本：

```text
scripts/open-packaging/bootstrap-builders/build-kubeclipper-server.sh
scripts/open-packaging/bootstrap-builders/build-kubeclipper-agent.sh
scripts/open-packaging/bootstrap-builders/build-etcd.sh
scripts/open-packaging/bootstrap-builders/build-caddy.sh
scripts/open-packaging/bootstrap-builders/build-registry.sh
scripts/open-packaging/bootstrap-builders/build-kc-console.sh
```

### 5.2 Kubernetes package

| 项 | 说明 |
| --- | --- |
| 包名 | `k8s/k8s:<kubernetes-version>` |
| 当前脚本 | [build-k8s-package.sh](/Users/lixueduan/17x/kc-release/kubeclipper/scripts/open-packaging/resource-builders/build-k8s-package.sh) |
| package image 内容 | `configs.tar.gz` |
| `configs.tar.gz` 内容 | `kubeadm`、`kubelet`、`kubectl`、`kubelet.service`、`10-kubeadm.conf`、`kubelet-pre-start.sh`、文件 manifest |
| runtime images | kubeadm image list |
| upstream source | `dl.k8s.io`、`github.com/kubernetes/release`、镜像仓库 |

关键问题是 `conntrack`。它没有像 Kubernetes/etcd/Helm 那样稳定的官方静态二进制归档。

推荐方案是保留独立 `conntrack` 构建脚本，但不把它放进 k8s 原生包：

1. `build-conntrack-binary.sh` 单独维护、单独测试；目标架构与构建机一致时直接从 netfilter 官方源码构建 `conntrack`，跨架构时才使用 Docker fallback。
2. `build-k8s-extension-package.sh` 每次直接调用 `build-conntrack-binary.sh`，不接收预构建二进制。
3. 构建产物只落在 k8s-extension 打包临时目录中，然后被拷入 `configs.tar.gz` 的 `usr/bin/conntrack`。
4. `publish-resource-k8s-extension.sh` 不发布 `binary/conntrack`；最终 Registry 中通过 `k8s-extension/k8s-extension:<version>` 分发。

Kubernetes runtime images 不进入 package image。打包阶段可以生成 `images.lock` 并推送到 image registry。

### 5.3 CRI packages

#### containerd

| 项 | 说明 |
| --- | --- |
| 包名 | `cri/containerd:<containerd-version>` |
| 当前脚本 | [build-containerd-package.sh](/Users/lixueduan/17x/kc-release/kubeclipper/scripts/open-packaging/resource-builders/build-containerd-package.sh) |
| package image 内容 | `configs.tar.gz` |
| `configs.tar.gz` 内容 | `containerd`、`containerd-shim-runc-v2`、`ctr`、`runc`、`crictl`、`containerd.service`、文件 manifest |
| runtime images | 无 |
| upstream source | containerd GitHub Release、runc GitHub Release、cri-tools GitHub Release、containerd.service from GitHub |

`cri/docker` 不进入开源打包范围。默认 CRI 只保留 containerd，避免维护多套 CRI 离线资源。

### 5.4 CNI packages

#### Calico

| 项 | 说明 |
| --- | --- |
| KC 组件名 | `cni/calico:<calico-version>` |
| 当前脚本 | [build-calico-package.sh](/Users/lixueduan/17x/kc-release/kubeclipper/scripts/open-packaging/resource-builders/build-calico-package.sh) |
| 发布产物 | Helm OCI chart，不发布 `packages/cni/calico` 空包 |
| Helm chart | `kubeclipper/charts/tigera-operator:<chart-version>`，Registry indexer 映射为 `cni/calico:<calico-version>` |
| runtime images | `calico/*`、`quay.io/tigera/operator` |
| upstream source | Tigera Helm repo、DockerHub、Quay |

Calico 不再包含 `charts.tgz` 或 `images.tar.gz` 的 package image。最终输出应为：

```text
packageRegistry/kubeclipper/charts/tigera-operator:v3.31.5
imageRegistry/calico/node:v3.31.5
imageRegistry/calico/cni:v3.31.5
imageRegistry/quay.io/tigera/operator:v1.40.8 或重命名后的镜像路径
```

### 5.5 Extension and debug packages

这些不是核心集群安装必需资源，默认不进入最小 release，但脚本需要能单独构建。

| 包 | 当前脚本 | 内容 | 来源 |
| --- | --- | --- | --- |
| `kc-runtime` image list | [build-kc-runtime-package.sh](/Users/lixueduan/17x/kc-release/kubeclipper/scripts/open-packaging/resource-builders/build-kc-runtime-package.sh) | `fanux/lvscare:v1.1.1`、`kubeclipper/kubectl:latest` image list | 镜像仓库 |
| `k8s-extension/k8s-extension:v1` | [build-k8s-extension-package.sh](/Users/lixueduan/17x/kc-release/kubeclipper/scripts/open-packaging/resource-builders/build-k8s-extension-package.sh) | `helm`、`etcdctl`、`conntrack`、`nerdctl`、CNI plugins、`calicoctl`、debug image list | GitHub Release、get.helm.sh、netfilter source、镜像仓库 |

旧 `kc-extension` 包只包含 `fanux/lvscare:v1.1.1` 和
`kubeclipper/kubectl:latest`。这两张镜像仍然是必需镜像：前者用于 VIP
`kube-lvscare`，后者用于 Web 终端访问集群。新方案将它们拆成独立的
`kc-runtime` image list；镜像本体仍作为标准 runtime image 同步到
Registry，不再发布 `extension/kc-runtime` package，并由离线集群预检确认存在。

GitHub Actions 中建议把 extension 放到独立 job，并默认 `workflow_dispatch` 手动触发。

### 5.6 CSI and addon packages

这类组件一般由 chart + images 组成。新的目标是：

1. chart 发布为 Helm OCI。
2. images 推送为 runtime images。
3. 只有存在 KubeClipper 专用配置模板时才发布 package image；chart-only 组件直接通过 resolver/catalog 映射到 Helm OCI。

| 包 | OCI kind/name | 当前脚本 | Chart 来源 | Image 来源 |
| --- | --- | --- | --- | --- |
| NVIDIA DRA | `app/nvidia-dra-driver-gpu:25.8.0` | `build-addon-package.sh --name nvidia-dra-driver-gpu` | NVIDIA Helm repo | `nvcr.io/nvidia/*` |
| NVIDIA GPU Operator | `app/nvidia-gpu-operator:v25.10.0` | `build-addon-package.sh --name nvidia-gpu-operator` | NVIDIA Helm repo | `nvcr.io/nvidia/*`、`registry.k8s.io/nfd/*` |

`csi/ceph`、`csi/cinder`、`csi/kc-csi`、`csi/csi-driver-wekafs` 不进入开源打包范围，已从 manifest、builder 默认入口和内置 image list 中移除。

对于 chart 来源不稳定或没有公共 Helm repo 的组件，不能回退到内部内容服务器。必须采用下面其中一种公开输入：

1. 组件官方 release asset。
2. KubeClipper 自己维护的 chart mirror release。
3. 当前仓库内可开源 chart。
4. 用户在 build manifest 中显式配置 `chartFile`。

## 6. 资源获取规范

资源获取遵循一个原则：版本和架构作为输入，公开上游下载地址固定在对应组件脚本里。这样可以同时打多个版本，又避免在 build manifest 中维护 URL 模板。manifest 只保留版本矩阵、镜像清单、本地文件覆盖和发布 Registry。

| source type | 例子 | 适用内容 |
| --- | --- | --- |
| `github-release` | `https://github.com/containerd/containerd/releases/...` | containerd、runc、crictl、etcd、CNI plugins |
| `official-url` | `https://dl.k8s.io/...`、`https://get.helm.sh/...` | Kubernetes、Helm、Docker static binary |
| `helm-repo` | `helm pull tigera-operator --repo ...` | Calico、NVIDIA charts |
| `container-image` | `docker.io/calico/node:v3.31.5` | runtime images |
| `source-build` | `https://www.netfilter.org/pub/...` | conntrack 这类无稳定官方静态二进制的小工具 |
| `local-file` | `/path/to/chart.tgz` | 本地开发或私有构建，不用于官方 GitHub Actions 默认配置 |

所有可下载文件建议支持 sha256 校验。第一阶段可以先记录来源类型和版本，第二阶段把 sha256 作为强制字段。

## 7. 单包打包脚本设计

每个单包脚本都应符合统一接口：

```bash
scripts/open-packaging/resource-builders/build-<package>.sh \
  --version <version> \
  --arch <amd64|arm64|all> \
  --output <resource-output> \
  [package-specific flags]
```

统一行为：

1. 官方下载地址固定在脚本中，普通使用只传版本和架构。
2. 所有包含 chart 的脚本优先从固定公开 Helm repo 拉取，必要时支持 `--chart-file` 本地输入。
3. 所有包含 images 的脚本支持 `--images-file`，image list 固化在仓库中并可 review。
4. runtime images 只写入 `images.txt`/`images.lock`，通过 `push-runtime-images.sh` 同步到镜像仓库。
5. 脚本输出可以是 legacy resource layout，但 publish 阶段必须拆分成 standard OCI package image、Helm OCI 和 runtime images。
6. 脚本不能访问内部域名，不能内置 `oss.kubeclipper.io/packages/`，不能 `scp` 到 tarball 节点。

推荐新增统一编排脚本：

```text
scripts/open-packaging/build-offline-resources.sh
```

职责：

1. 读取 `packaging/resources.yaml`。
2. 展开版本和架构矩阵。
3. 不带 `--push` 时调用内部 builder 并生成 `build-report.json`、`images.lock`、`charts.lock`，用于本地调试。
4. 带 `--push` 时调用一段式 publish 脚本；每个 publish 脚本自己获取资源、构建临时 payload、推送 package image/Helm OCI/runtime images。

示例：

```bash
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --output /tmp/kc-resource \
  --registry ghcr.io/lixd/kubeclipper \
  --image-registry ghcr.io/lixd/kubeclipper \
  --push
```

## 8. 发布设计

发布入口按组件拆分，但每个入口都是一段式：获取资源、构建临时 payload、发布到 Registry。

### 8.1 发布 package image

```bash
scripts/open-packaging/publish-resource-k8s.sh \
  --registry-prefix ghcr.io/lixd/kubeclipper \
  --version v1.36.1

scripts/open-packaging/publish-resource-k8s-extension.sh \
  --registry-prefix ghcr.io/lixd/kubeclipper \
  --version v1

scripts/open-packaging/publish-resource-containerd.sh \
  --registry-prefix ghcr.io/lixd/kubeclipper \
  --version 2.2.4

scripts/open-packaging/publish-resource-calico.sh \
  --registry-prefix ghcr.io/lixd/kubeclipper \
  --version v3.31.5

scripts/open-packaging/publish-resource-kc-runtime.sh \
  --image-registry-prefix ghcr.io/lixd/kubeclipper \
  --version v1.8.0
```

发布结果：

```text
ghcr.io/lixd/kubeclipper/kubeclipper/packages/k8s/k8s:v1.36.1
ghcr.io/lixd/kubeclipper/kubeclipper/packages/k8s-extension/k8s-extension:v1
ghcr.io/lixd/kubeclipper/kubeclipper/packages/cri/containerd:2.2.4
ghcr.io/lixd/kubeclipper/kubeclipper/charts/tigera-operator:v3.31.5
```

### 8.2 发布 Helm OCI chart

Calico 的 `charts.tgz` 由 `publish-resource-calico.sh` 同步发布为 Helm OCI chart。KubeClipper Registry indexer 通过内置 catalog 把 `kubeclipper/charts/tigera-operator:<version>` 投影为用户侧组件 `cni/calico:<version>`，不再发布 manifest-only package image。

发布结果：

```text
oci://ghcr.io/lixd/kubeclipper/kubeclipper/charts/tigera-operator:v3.31.5
```

### 8.3 发布 runtime images

发布 runtime images 不依赖 KubeClipper package image，使用 `push-runtime-images.sh` 按组件从 `images.lock` 同步。

职责：

1. 读取 `images.lock`。
2. 按架构 pull upstream image。
3. retag 到目标 image registry。
4. push 多架构 manifest 或单架构 image。
5. 输出 `image-publish-report.json`。

GitHub Actions 主路径使用 `crane` 或 `skopeo` 做镜像复制，避免先 `docker save` 再 `docker load/push`。

## 9. GitHub Actions 设计

发布 workflow 按组件拆分。组件之间不共享一次构建任务，只复用安装工具、
GHCR 登录和结果校验逻辑。

### 9.1 PR validation

触发：

```yaml
on:
  pull_request:
    paths:
      - scripts/open-packaging/**
      - packaging/**
      - docs/superpowers/specs/*offline-package*
```

执行：

1. `bash -n` 检查所有脚本。
2. shellcheck。
3. `--help` smoke test。
4. 小型 dry-run fixture，验证 package manifest、chart descriptor、image list 解析。

不拉取大镜像，不发布。

### 9.2 Component publish workflows

| Workflow | 触发方式 | 产物 |
| --- | --- | --- |
| `publish-bootstrap-kubeclipper.yml` | `main`、`master`、`release-*`、`v*` tag 或手动 | server/agent package image |
| `publish-bootstrap-etcd.yml` | 手动 | etcd package image |
| `publish-bootstrap-console.yml` | 手动 | Caddy/console package image |
| `publish-bootstrap-registry.yml` | 手动 | Registry package image |
| `publish-resource-k8s.yml` | 手动 | K8s package image + runtime images |
| `publish-resource-containerd.yml` | 手动 | containerd package image |
| `publish-resource-k8s-extension.yml` | 手动 | helper package image + runtime images |
| `publish-resource-calico.yml` | 手动 | Helm OCI chart + runtime images |
| `publish-resource-kc-runtime.yml` | 手动 | KubeClipper helper runtime images |

`bootstrap/kubeclipper` 跟随代码提交自动构建。其 tag 规则为：Git tag 原样
使用；`main`/`master` 使用 `latest`；其他允许自动构建的分支使用清理后的
分支名，例如 `release-1.6`。其他组件只有在上游版本或打包内容发生变化时
手动触发，不随 KubeClipper 每次提交重复构建。

内部 `_publish-oci-component.yml` 是复用实现，不是聚合发布入口。每个组件
workflow 独立下载、构建、推送和验证自己的产物。

## 10. 当前脚本覆盖与缺口

当前已有脚本覆盖：

| 类型 | 状态 |
| --- | --- |
| `k8s` | 已有 builder；只包含 Kubernetes 原生二进制、systemd 配置和 kubeadm runtime image list |
| `containerd` | 已有 builder；适合 GitHub Actions |
| `calico` | 已有 builder；已支持 chart/image 分离 |
| `k8s-extension` | 已有 builder；建议从核心 release 中拆出去 |
| CSI/addon | 已有统一 builder；部分 chart 来源需要显式配置 |
| bootstrap binaries | 已有四个独立 publish 脚本；后续可继续拆出更细的单包 builder |
| third-party binaries | 已有 `conntrack` builder；作为 k8s-extension 打包内部构建器使用，不单独发布 |
| runtime image push | 已有 `push-runtime-images.sh`；按 `images.lock` 和 component/version/arch 过滤同步 |
| build manifest | 已有 `packaging/resources.yaml` |
| GitHub Actions workflow | 已有 PR validation 和九个独立组件发布 workflow |
| sha256 lock/checksum | 未强制 |

优先级建议：

1. P0：新增 `packaging/resources.yaml`，覆盖 `k8s/containerd/calico/bootstrap`。
2. P0：实现 `build-offline-resources.sh`，从 manifest 调度现有 builder。
3. P0：解决 `conntrack` 默认公开来源。已采用源码构建并打包进 k8s-extension package。
4. P0：实现 GitHub Actions PR validation。
5. P1：实现按组件独立发布到 GHCR 的 workflow。已完成。
6. P1：实现 `push-runtime-images.sh`，替代 `images.tar.gz` 推送主路径。
7. P1：为所有下载产物增加 sha256 lock。
8. P2：完善 CSI/GPU/addon chart 来源和 release manifest。

## 11. 推荐落地顺序

第一阶段：让核心路径从零可构建。

```text
resources.yaml
  -> build k8s/containerd/calico
  -> publish package images
  -> publish tigera-operator Helm OCI
  -> mirror kubeadm/calico runtime images
  -> oci-verify
```

第二阶段：把 bootstrap 也纳入同一套 pipeline。

```text
build server/agent
download etcd/caddy/registry
build/collect kc-console
publish four bootstrap package images
```

第三阶段：按需扩展 addon。

```text
nvidia-gpu-operator
nvidia-dra-driver-gpu
```

第四阶段：去掉 legacy resource layout 中间产物。

最终 builder 直接产生：

```text
package layer tar.gz
helm chart archive
images.lock
package manifest
publish report
```

而不是先落到：

```text
resource/<name>/<version>/<arch>/{configs.tar.gz,charts.tgz,images.txt}
```

## 12. 验收标准

一条 release workflow 通过后，应能证明：

1. 不访问 internal content server、tarball server、static server。
2. 不访问 `oss.kubeclipper.io/packages/`。
3. 所有 package image 可被 Registry indexer 扫描。
4. 所有 package image 可被 `kcctl resource inspect` 查看。
5. 所有 Helm OCI chart 可被 `helm pull oci://...` 或 KubeClipper fallback 拉取。
6. 所有 runtime images 已存在于目标 image registry。
7. `kcctl deploy --package-registry` 不需要 `--pkg`。
8. `kcctl create cluster --offline --local-registry` 可以完整安装 Kubernetes + containerd + Calico。
9. release report 中记录每个资源的 upstream URL、版本、digest、目标 ref。

## 13. 后续需要修改的代码/脚本

建议新增：

```text
packaging/resources.yaml
scripts/open-packaging/build-offline-resources.sh
scripts/open-packaging/push-runtime-images.sh
scripts/open-packaging/publish-resource-k8s.sh
scripts/open-packaging/publish-resource-containerd.sh
scripts/open-packaging/publish-resource-calico.sh
scripts/open-packaging/bootstrap-builders/
.github/workflows/offline-resource-validate.yaml
.github/workflows/offline-resource-release.yaml
```

建议调整：

```text
scripts/open-packaging/resource-builders/build-k8s-package.sh
scripts/open-packaging/README.md
```

其中 `build-offline-resources.sh` 会分别调度 `build-k8s-package.sh` 和 `build-k8s-extension-package.sh`；后者会在需要时内部调用 `build-conntrack-binary.sh`，并把生成的二进制打进 k8s-extension package。
