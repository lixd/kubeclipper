# OCI 替换 Static Server 功能影响说明

## 背景

本次改造的目标是用 OCI Registry 替换 KubeClipper 原来的 static server / 离线大包交付方式。

改造后，KubeClipper 不再依赖 `kc-amd64.tar.gz` 这类大包作为部署、扩容、安装集群的主入口。用户需要先准备 Registry，并把 KubeClipper bootstrap package image、Kubernetes/CRI package image、Helm OCI Chart 和运行镜像放到 Registry 中。安装阶段只从 Registry 拉取和消费这些内容，不再隐式解压大包、不再本地 load `images.tar.gz`。

## 影响范围

| 功能 | 影响 | 新行为 |
| --- | --- | --- |
| `kcctl deploy` | 移除 `--pkg` 主路径 | 通过 `--package-registry` 从 OCI Registry 获取 bootstrap 二进制和离线 package。 |
| `kcctl join` | 移除 `--pkg` 主路径 | 新 agent 节点从 `--package-registry` 获取所需 bootstrap 二进制。 |
| `kcctl registry deploy` | 不再依赖 KubeClipper 大包 | Registry 作为基础设施组件单独部署，后续 package image、Helm OCI Chart 和 runtime image 都推到 Registry。 |
| 完全离线 Registry 自举 | 单一离线 bundle | `kcctl registry deploy --offline-bundle` 校验并提取 bundle 内的 Registry package image，再用同一 bundle 填充 Registry。 |
| `kcctl registry push` | 不再承担 package 发布 | 仅作为通用容器镜像归档导入工具；KubeClipper release 主路径使用标准 Registry 同步工具和 `release-manifest.yaml`。 |
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

新模式要求 `10.0.0.10:5000` 中已经存在四个 bootstrap package image：`bootstrap/kubeclipper`、`bootstrap/etcd`、`bootstrap/console` 和 `bootstrap/registry`。镜像内资源统一位于 `/opt/kubeclipper/resource`。`kcctl` 由用户从 GitHub Release 下载，不放入 package Registry。

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

将已经加入 KubeClipper 的 agent 节点加入或移出 Kubernetes 集群：

```bash
kcctl cluster add-node \
  --cluster-name demo \
  --worker 10.0.0.11

kcctl cluster remove-node \
  --cluster-name demo \
  --worker 10.0.0.11
```

`--worker` 支持 KubeClipper node ID 或节点主 IPv4 地址，也支持重复指定以批量操作。添加节点复用创建集群时的 OCI 解析流程，按目标节点架构解析并固定 package digest；删除节点只卸载已有组件，不再下载 package。`kcctl join` 与 `cluster add-node` 职责不同：前者部署 KubeClipper agent，后者才把该 agent 加入 Kubernetes 集群。

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

之后直接从公开上游构建并发布：

```bash
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --output /data/kubeclipper-resources \
  --registry 10.0.0.10:5000 \
  --image-registry 10.0.0.10:5000 \
  --arch amd64 \
  --include-bootstrap \
  --push
```

Kubernetes、containerd 和 k8s-extension 的二进制/config 资源封装为标准 OCI image；Calico 使用原生 Helm OCI Chart；运行镜像保持为标准容器镜像。打包流程不依赖公司内部 static server。

### 4. 准备运行镜像

之前：

安装阶段可能从 package 里的 `images.tar.gz` 做本地导入：

```text
images.tar.gz -> docker load / nerdctl load -> 节点本地镜像
```

之后由各组件构建脚本生成 `images.txt`，再汇总为发布侧 `images.lock`。发布脚本使用 `crane` 或 `skopeo` 将这些标准镜像同步到目标 Registry：

```bash
scripts/open-packaging/push-runtime-images.sh \
  --images-lock /data/kubeclipper-resources/images.lock \
  --image-registry 10.0.0.10:5000
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

KubeClipper server 不维护运行镜像清单，也不在创建 operation 前执行 Registry `HEAD` 预检。kubeadm、containerd 和 kubelet 在目标节点上执行真实拉取；镜像缺失时，安装 operation 或 Pod 状态会报告拉取失败。

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

资源信息来自 Registry 中的 package image 和 Helm OCI inventory。

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
  -> 发布 KubeClipper bootstrap package images
  -> 发布 Kubernetes/CRI package images 和 CNI Helm OCI Charts
  -> 推送 runtime images 到同一个或另一个 Registry
  -> kcctl deploy --package-registry
  -> kcctl create cluster --local-registry
```

其中：

| Registry 内容 | 作用 | 典型命令 |
| --- | --- | --- |
| bootstrap package images | 部署 KubeClipper server/agent 等二进制 | `kcctl deploy --package-registry` |
| package images / Helm OCI Charts | 安装 Kubernetes/CRI/CNI 配置和 Chart | `kcctl create cluster` |
| runtime container images | kubelet/container runtime 拉取镜像 | `kcctl create cluster --local-registry` |

`packageRegistry` 和 `localRegistry` 可以是同一个地址，但职责不同：

```text
10.0.0.10:5000/kubeclipper/packages/...  -> package images
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
2. 标准 OCI package image 和 Helm OCI Chart 负责离线 package。
3. Registry runtime image 负责镜像拉取。
4. delivery policy 负责版本支持矩阵。

### 3. 安装行为更可预测

安装阶段不再偷偷修改 Registry，也不再在节点本地 load 镜像。所有运行镜像必须提前存在于 Registry，但 KubeClipper server 不再维护第二份镜像清单或执行近似的 Registry `HEAD` 预检。kubeadm、containerd 和 kubelet 在目标节点上的真实拉取结果是权威校验，缺失镜像会在安装操作日志或工作负载状态中明确失败。

### 4. 更适合企业内网和 Harbor

用户可以把官方镜像同步到 Harbor，也可以用 KubeClipper 自带 registry 做轻量私有仓库。KubeClipper 不再假设必须有 static server，也不再绑定某个文件目录结构。

### 5. 版本和内容可以 digest 化

OCI 对象天然支持 digest。package inventory 和 delivery policy 负责安装计划解析；发布侧的 `release-manifest.yaml` 可以固定 package、Chart 和 runtime image digest，用于发布审计、Harbor 同步及离线导入验收。

### 6. CI/CD 和发布链路更清楚

发布侧可以分别处理：

1. `kcctl` release。
2. component binary artifacts。
3. Kubernetes/CRI package images 和 CNI Helm OCI Charts。
4. runtime images。

不需要每次都重新压缩和分发一个混合所有内容的大包。

## 注意事项和剩余工作

1. `kcctl upgrade` 的旧包升级路径已经移除，但 OCI-native upgrade 还没有实现，需要后续单独设计。
2. 运行镜像 BOM 位于发布侧的 `images.lock` 和 `release-manifest.yaml`，不写入 delivery policy，也不由 server API 消费。
3. Registry 必须提前准备好 package images、Helm OCI Charts 和 runtime images，否则部署或创建集群会失败。
4. 旧 static resource 包可以作为迁移输入，但不再是安装阶段的直接依赖。
5. 如果 `packageRegistry` 和 `localRegistry` 使用同一个地址，需要通过 repository namespace 区分 package image、Chart 和 runtime image。

## 推荐用户路径

全离线或内网场景推荐按下面顺序操作：

```bash
# 1. 部署或准备 Registry
kcctl registry deploy --node 10.0.0.10 --registry-port 5000

# 2. 从公开源构建并发布 bootstrap、集群 package、Chart 和 runtime images
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --output /data/kubeclipper-resources \
  --registry 10.0.0.10:5000 \
  --image-registry 10.0.0.10:5000 \
  --arch amd64 \
  --include-bootstrap \
  --push

# 3. 可选：按 release manifest 验收目标 Registry
scripts/open-packaging/verify-release-manifest.sh \
  --manifest /data/kubeclipper-resources/release-manifest.yaml \
  --registry 10.0.0.10:5000 \
  --arch amd64 \
  --insecure

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
