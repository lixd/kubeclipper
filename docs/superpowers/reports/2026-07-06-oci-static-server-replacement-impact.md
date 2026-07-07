# OCI 替换 Static Server 功能影响说明

## 背景

本次改造的目标是用 OCI Registry 替换 KubeClipper 原来的 static server / 离线大包交付方式。

改造后，KubeClipper 不再依赖 `kc-amd64.tar.gz` 这类大包作为部署、扩容、安装集群的主入口。用户需要先准备 Registry，并把 KubeClipper bootstrap 二进制、Kubernetes/CRI/CNI package artifact、运行镜像放到 Registry 中。安装阶段只从 Registry 拉取和消费这些内容，不再隐式解压大包、不再本地 load `images.tar.gz`。

## 影响范围

| 功能 | 影响 | 新行为 |
| --- | --- | --- |
| `kcctl deploy` | 移除 `--pkg` 主路径 | 通过 `--package-registry` 从 OCI Registry 获取 bootstrap 二进制和离线 package。 |
| `kcctl join` | 移除 `--pkg` 主路径 | 新 agent 节点从 `--package-registry` 获取所需 bootstrap 二进制。 |
| `kcctl registry deploy` | 不再依赖 KubeClipper 大包 | Registry 作为基础设施组件单独部署，后续 package artifact 和 runtime image 都推到 Registry。 |
| `kcctl registry push` | 语义调整 | 不再 push KubeClipper package，改为 `--image-archive`，用于导入 `docker save` 生成的镜像归档。 |
| `kcctl resource list/inspect/refresh` | 数据源变更 | 从 OCI Registry inventory 读取 package 信息，不再依赖 static server 目录索引。 |
| `kcctl create cluster` | 离线安装更严格 | 离线模式必须指定 `--local-registry`，并要求运行镜像已经在 Registry 中。 |
| Kubernetes/CNI/addon 安装 | 移除 image tarball fallback | 不再执行 `docker load` / `nerdctl load`，镜像必须由 kubelet/container runtime 从 Registry 拉取。 |
| `kubectl-terminal` 扩展 | 镜像来源变更 | 使用 `localRegistry/kubeclipper/kubectl`，不再从 extension artifact 中加载镜像包。 |
| release/install 脚本 | 发行物变小 | 发布物向 kcctl-only 收敛，server/agent/registry 等不再塞进用户下载的大包。 |
| `kcctl upgrade` | 旧包升级入口禁用 | 旧 `--pkg` / `--online` 升级包路径移除，OCI-native upgrade 后续单独设计实现。 |

## 使用方式变化

### 1. 部署 KubeClipper

之前：

```bash
kcctl deploy \
  --server 10.0.0.10 \
  --agent 10.0.0.10 \
  --pk-file ~/.ssh/id_rsa \
  --pkg kc-amd64.tar.gz
```

或者：

```bash
kcctl deploy \
  --server 10.0.0.10 \
  --agent 10.0.0.10 \
  --pk-file ~/.ssh/id_rsa \
  --pkg https://example.com/kc-amd64.tar.gz
```

之后：

```bash
kcctl deploy \
  --server 10.0.0.10 \
  --agent 10.0.0.10 \
  --pk-file ~/.ssh/id_rsa \
  --package-registry 10.0.0.10:5000
```

新模式要求 `10.0.0.10:5000` 中已经存在 KubeClipper bootstrap 二进制 artifact，例如 `kcctl`、`kubeclipper-server`、`kubeclipper-agent`、`etcd`、`caddy`、`registry`、`kc-console` 等。

### 2. 扩容 agent 节点

之前：

```bash
kcctl join \
  --agent 10.0.0.11 \
  --pk-file ~/.ssh/id_rsa \
  --pkg kc-amd64.tar.gz
```

之后：

```bash
kcctl join \
  --agent 10.0.0.11 \
  --pk-file ~/.ssh/id_rsa \
  --package-registry 10.0.0.10:5000
```

如果 deploy config 中已经保存了 `packageRegistry`，`join` 可以继承该配置；显式传入 `--package-registry` 更清晰。

### 3. 准备离线 package

之前：

```text
/opt/kubeclipper-server/resource/
  k8s/v1.x.x/amd64/configs.tar.gz
  k8s/v1.x.x/amd64/images.tar.gz
  containerd/x.y.z/amd64/configs.tar.gz
  calico/vx.y.z/amd64/charts.tgz
```

static server 暴露这个目录，安装时通过目录结构查找资源。

之后：

```bash
scripts/publish-oci-package.sh \
  --package /data/packages/k8s-v1.36.1-amd64.tar.gz \
  --kind k8s \
  --name k8s \
  --version v1.36.1 \
  --arch amd64 \
  --registry 10.0.0.10:5000
```

或者批量迁移：

```bash
scripts/migrate-legacy-packages-to-oci.sh \
  --file legacy-packages.yaml
```

旧 static resource 目录仍然可以作为迁移输入，但最终会被发布成 OCI artifact，安装阶段不再直接读取 static server 文件目录。

### 4. 准备运行镜像

之前：

安装阶段可能从 package 里的 `images.tar.gz` 做本地导入：

```text
images.tar.gz -> docker load / nerdctl load -> 节点本地镜像
```

之后：

用户需要提前把运行镜像推送到 Registry：

```bash
kcctl registry push \
  --node 10.0.0.10 \
  --registry-port 5000 \
  --image-archive runtime-images.tar.gz
```

然后安装集群时显式指定本地镜像仓库：

```bash
kcctl create cluster \
  --name demo \
  --master 10.0.0.20 \
  --offline=true \
  --k8s-version v1.36.1 \
  --cri containerd \
  --cri-version 2.2.4 \
  --cni calico \
  --cni-version v3.31.5 \
  --local-registry 10.0.0.10:5000 \
  --insecure-registry 10.0.0.10:5000
```

安装前会检查 `localRegistry` 中是否存在 kubeadm、etcd、pause、coredns、Calico 等运行镜像。缺失时会在创建 operation 前失败，并提示先同步或推送镜像。

### 5. 查看可用资源

之前：

```bash
kcctl resource list
```

资源信息来自 static server 或部署配置中的静态资源目录。

之后：

```bash
kcctl resource list --registry 10.0.0.10:5000 --refresh
kcctl resource inspect --registry 10.0.0.10:5000 --name k8s --version v1.36.1 --arch amd64 -o yaml
```

资源信息来自 Registry 中的 OCI artifact inventory。

### 6. 发布和安装 kcctl

之前：

发布物通常是包含多个组件的大包：

```text
kc-linux-amd64.tar.gz
  kcctl
  kubeclipper-server
  kubeclipper-agent
  ...
```

之后：

发布物向 kcctl-only 收敛：

```text
kcctl-linux-amd64
kcctl-linux-arm64
kcctl-darwin-amd64
kcctl-darwin-arm64
kcctl-checksums.txt
```

安装方式：

```bash
install -m 0755 kcctl-linux-amd64 /usr/local/bin/kcctl
```

server、agent、registry 等组件不再要求用户下载到本地大包中，而是通过 Registry 交付。

## 新流程总览

```text
准备 Registry
  -> 发布 KubeClipper bootstrap binary artifacts
  -> 发布 Kubernetes/CRI/CNI package OCI artifacts
  -> 推送 runtime images 到同一个或另一个 Registry
  -> kcctl deploy --package-registry
  -> kcctl create cluster --local-registry
```

其中：

| Registry 内容 | 作用 | 典型命令 |
| --- | --- | --- |
| bootstrap binary artifacts | 部署 KubeClipper server/agent 等二进制 | `kcctl deploy --package-registry` |
| package OCI artifacts | 安装 Kubernetes/CRI/CNI 配置和 chart | `kcctl create cluster` |
| runtime container images | kubelet/container runtime 拉取镜像 | `kcctl create cluster --local-registry` |

`packageRegistry` 和 `localRegistry` 可以是同一个地址，但职责不同：

```text
10.0.0.10:5000/kubeclipper/packages/...  -> package artifacts
10.0.0.10:5000/kube-apiserver:v1.36.1   -> runtime images
10.0.0.10:5000/calico/node:v3.31.5      -> runtime images
```

## 优点

### 1. 下载包更小

用户不再需要下载 1GB 左右的大包。bootstrap 入口可以收敛成一个 `kcctl` 单二进制，server、agent、registry、console、Kubernetes 资源都通过 Registry 按需获取。

### 2. 职责更清晰

旧模式中，大包同时承担：

1. KubeClipper 自身部署。
2. Kubernetes/CRI/CNI 离线资源。
3. 运行镜像分发。
4. static server 文件索引。

新模式拆成：

1. `kcctl` 负责 bootstrap。
2. OCI package artifact 负责离线 package。
3. Registry runtime image 负责镜像拉取。
4. delivery policy 负责版本支持矩阵。

### 3. 安装行为更可预测

安装阶段不再偷偷修改 Registry，也不再在节点本地 load 镜像。所有运行镜像必须提前存在于 Registry，缺失时 precheck 直接失败，避免 kubeadm/Calico 执行到一半才出现 `image not found` 或 `ImagePullBackOff`。

### 4. 更适合企业内网和 Harbor

用户可以把官方镜像同步到 Harbor，也可以用 KubeClipper 自带 registry 做轻量私有仓库。KubeClipper 不再假设必须有 static server，也不再绑定某个文件目录结构。

### 5. 版本和内容可以 digest 化

OCI artifact 天然支持 digest。package inventory、delivery policy、runtime image precheck 可以逐步做到更严格的版本和内容校验，比旧目录文件更容易追踪和审计。

### 6. CI/CD 和发布链路更清楚

发布侧可以分别处理：

1. `kcctl` release。
2. component binary artifacts。
3. Kubernetes/CRI/CNI package artifacts。
4. runtime images。

不需要每次都重新压缩和分发一个混合所有内容的大包。

## 注意事项和剩余工作

1. `kcctl upgrade` 的旧包升级路径已经移除，但 OCI-native upgrade 还没有实现，需要后续单独设计。
2. runtime image precheck 当前只覆盖已配置的 Kubernetes/Calico 版本，后续建议引入正式 Image BOM。
3. Registry 必须提前准备好 package artifact 和 runtime images，否则部署或创建集群会失败。
4. 旧 static resource 包可以作为迁移输入，但不再是安装阶段的直接依赖。
5. 如果 `packageRegistry` 和 `localRegistry` 使用同一个地址，需要通过 repository namespace 区分 package artifact 和 runtime image。

## 推荐用户路径

全离线或内网场景推荐按下面顺序操作：

```bash
# 1. 部署或准备 Registry
kcctl registry deploy --node 10.0.0.10 --registry-port 5000

# 2. 发布旧 package 为 OCI artifact
scripts/migrate-legacy-packages-to-oci.sh --file legacy-packages.yaml

# 3. 推送 runtime images
kcctl registry push --node 10.0.0.10 --registry-port 5000 --image-archive runtime-images.tar.gz

# 4. 部署 KubeClipper
kcctl deploy \
  --server 10.0.0.20 \
  --agent 10.0.0.20 \
  --pk-file ~/.ssh/id_rsa \
  --package-registry 10.0.0.10:5000

# 5. 创建 Kubernetes 集群
kcctl create cluster \
  --name demo \
  --master 10.0.0.20 \
  --offline=true \
  --k8s-version v1.36.1 \
  --cri containerd \
  --cri-version 2.2.4 \
  --cni calico \
  --cni-version v3.31.5 \
  --local-registry 10.0.0.10:5000 \
  --insecure-registry 10.0.0.10:5000
```

