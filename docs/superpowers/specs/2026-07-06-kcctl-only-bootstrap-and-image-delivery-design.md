---
comet_change: kcctl-only-bootstrap-image-delivery
role: technical-design
canonical_spec: openspec
---

# KubeClipper kcctl-only Bootstrap 与镜像交付设计

## 1. 背景与目标

OCI package delivery 已经把 KubeClipper 离线资源包从 static server 迁移到 OCI
Registry。下一步需要继续缩小 KubeClipper 自身发行物：

**用户只下载 `kcctl`，不再下载包含 server、agent、registry、console、Kubernetes
镜像的大型 `kc-amd64.tar.gz`。**

目标终态：

1. 发布物只包含 `kcctl` 单二进制和校验文件。
2. `kubeclipper-server`、`kubeclipper-agent`、`kc-console`、`registry` 等 KubeClipper
   自身组件以 OCI image 发布。
3. Kubernetes、CRI、CNI、extension 运行镜像提前发布到在线镜像仓库，或由用户同步到
   Harbor / 私有 Registry。
4. `kcctl registry deploy` 不再依赖 `--pkg kc-amd64.tar.gz`，而是从 OCI image 或本地
   image archive / binary bootstrap 第一个 Registry。
5. `kcctl deploy` 不再依赖大包提取 server/agent，而是从组件 image 提取二进制并以
   systemd 方式部署。
6. 安装 Kubernetes 集群时，`--local-registry` / 后续 `--image-registry` 默认已经包含
   kubeadm、pause、etcd、coredns、CNI 等运行镜像；安装阶段不负责把 package 内镜像推到
   Registry。

非目标：

1. 不在安装 Kubernetes 集群时偷偷 push 运行镜像。
2. 不让 `packageRegistry` 负责运行镜像。
3. 不要求 KubeClipper server/agent 立刻容器化运行；第一阶段仍以提取二进制 +
   systemd 部署为主。
4. 不强制用户使用 KubeClipper 自带 registry；用户可以使用 Harbor、ACR、DockerHub、
   GHCR 或其他 Registry。

## 2. 第一性原则

### 2.1 发布物应该最小化

用户真正需要先拿到的是一个可信 bootstrap 工具。大包把很多不同生命周期的内容绑在
一起：

| 内容 | 生命周期 | 是否应放进 kcctl 下载包 |
| --- | --- | --- |
| `kcctl` | bootstrap 入口 | 是 |
| `kubeclipper-server` | KubeClipper 组件版本 | 否，放 OCI image |
| `kubeclipper-agent` | KubeClipper 组件版本 | 否，放 OCI image |
| `registry` | 可选基础设施组件 | 否，放 OCI image 或 binary source |
| Kubernetes / Calico 镜像 | 集群运行时版本 | 否，放 image registry |
| Kubernetes / CRI / CNI 配置包 | 离线安装包 | 否，放 packageRegistry |

因此发行物应拆成：

```text
kcctl binary            -> bootstrap tool
component images        -> KubeClipper self runtime
package OCI artifacts   -> offline package files
runtime images          -> kubeadm/CNI/addon container images
```

### 2.2 第一个 Registry 不能依赖它自己

`kcctl registry deploy` 经常用于部署第一个本地 Registry。此时本地 Registry 还不存在，
所以 registry bootstrap source 不能默认来自本地 Registry。

合法来源必须是：

1. 在线组件镜像，例如 `docker.io/kubeclipper/registry:<version>`。
2. 用户指定的已有镜像仓库，例如 `harbor.example.com/kubeclipper/registry:<version>`。
3. 本地 image archive，例如 `registry-vX.Y.Z.tar.gz`。
4. 本地 registry binary，例如 `/path/to/registry`。

### 2.3 包和镜像必须分离

`packageRegistry` 和 `imageRegistry` 是不同概念：

| 字段 | 负责内容 | 示例 |
| --- | --- | --- |
| `packageRegistry` | KubeClipper package OCI artifacts | `registry.local:5500/kubeclipper/packages/k8s/k8s:v1.36.1` |
| `imageRegistry` / `localRegistry` | Kubernetes/CNI/addon 运行镜像 | `registry.local:5500/kube-apiserver:v1.36.1` |
| `componentRegistry` | KubeClipper 自身组件镜像 | `docker.io/kubeclipper/kubeclipper-server:v1.8.0` |

短期可以复用现有 `--local-registry` 表达运行镜像仓库，但文档必须明确它不是
`packageRegistry`。中长期建议引入 `--image-registry`，并保留 `--local-registry` 作为兼容
alias。

### 2.4 安装阶段只消费，不生产

Kubernetes 安装步骤应该只消费以下已经存在的输入：

1. 已解析的 package artifact。
2. 已存在的运行镜像仓库。
3. 已存在的 KubeClipper agent/server 二进制。

安装阶段不应再：

1. 修改远端镜像仓库内容。
2. 根据 package 内 `images.tar.gz` 做隐式 push。
3. 承担镜像同步职责。

如果镜像不存在，应快速失败并给出“请先同步/推送镜像”的诊断。

## 3. 术语与事实来源

| 术语 | 事实来源 | 说明 |
| --- | --- | --- |
| Bootstrap tool | GitHub release / 下载站 | `kcctl` 单二进制 |
| Component image | DockerHub / GHCR / Harbor | KubeClipper 自身组件镜像 |
| Package artifact | packageRegistry | k8s/cri/cni/binary/extension package |
| Runtime image | imageRegistry | kubeadm、pause、etcd、coredns、Calico 等容器镜像 |
| Delivery policy | KubeClipper API | 版本支持矩阵 |
| Image BOM | release artifact 或 package manifest 派生 | 某版本组合需要哪些运行镜像 |

## 4. 目标架构

```text
                         +----------------------+
                         | kcctl                |
                         | single bootstrap bin |
                         +----------+-----------+
                                    |
          +-------------------------+--------------------------+
          |                         |                          |
          v                         v                          v
+---------+----------+   +----------+-----------+   +----------+-----------+
| componentRegistry  |   | packageRegistry      |   | imageRegistry        |
| server/agent/etc   |   | kubeclipper/packages |   | kubeadm/CNI images   |
+---------+----------+   +----------+-----------+   +----------+-----------+
          |                         |                          |
          v                         v                          v
+---------+----------+   +----------+-----------+   +----------+-----------+
| extract binaries   |   | fetch packages by    |   | kubelet/containerd   |
| install systemd    |   | digest               |   | pull images          |
+--------------------+   +----------------------+   +----------------------+
```

部署 KubeClipper 自身：

```text
kcctl -> pull/export component image -> extract binary -> copy to target -> write systemd -> start
```

安装 Kubernetes 集群：

```text
SupportPolicy + PackageInventory -> resolved package plan -> fetch package files
imageRegistry preflight -> kubeadm/CNI pull runtime images -> install
```

## 5. 组件镜像规范

### 5.1 命名

推荐默认命名：

```text
docker.io/kubeclipper/kubeclipper-server:<kc-version>
docker.io/kubeclipper/kubeclipper-agent:<kc-version>
docker.io/kubeclipper/kc-console:<kc-version>
docker.io/kubeclipper/registry:<registry-version-or-kc-version>
docker.io/kubeclipper/etcdctl:<etcdctl-version>
```

镜像也可以被同步到：

```text
harbor.example.com/kubeclipper/kubeclipper-server:<kc-version>
harbor.example.com/kubeclipper/registry:<kc-version>
```

### 5.2 镜像内容

每个组件 image 至少包含：

```text
/usr/local/bin/<component-binary>
/etc/kubeclipper/component-manifest.json
```

`component-manifest.json` 示例：

```json
{
  "apiVersion": "delivery.kubeclipper.io/v1alpha1",
  "kind": "ComponentImageManifest",
  "component": "kubeclipper-server",
  "version": "v1.8.0",
  "binary": "/usr/local/bin/kubeclipper-server",
  "os": "linux",
  "arch": "amd64",
  "sha256": "..."
}
```

约束：

1. `kcctl` 必须校验镜像 digest 或 manifest 中的 binary digest。
2. 多架构通过 OCI manifest index 表达，不把 arch 放进 tag。
3. 提取二进制时只允许白名单路径，防止恶意 layer 覆盖任意文件。

## 6. kcctl registry deploy 设计

### 6.1 推荐命令

最简在线用法：

```bash
kcctl registry deploy \
  --node 10.0.0.10 \
  --pk-file ~/.ssh/id_rsa \
  --registry-port 5500
```

默认解析：

```text
registry component image = docker.io/kubeclipper/registry:<kcctl-version>
```

指定组件镜像仓库：

```bash
kcctl registry deploy \
  --node 10.0.0.10 \
  --pk-file ~/.ssh/id_rsa \
  --registry-port 5500 \
  --component-registry harbor.example.com/kubeclipper
```

指定完整 registry image：

```bash
kcctl registry deploy \
  --node 10.0.0.10 \
  --pk-file ~/.ssh/id_rsa \
  --registry-port 5500 \
  --registry-image docker.io/kubeclipper/registry:v1.8.0
```

全离线用法：

```bash
kcctl registry deploy \
  --node 10.0.0.10 \
  --pk-file ~/.ssh/id_rsa \
  --registry-port 5500 \
  --registry-image-archive ./registry-v1.8.0-linux-amd64.tar.gz
```

本地二进制兜底：

```bash
kcctl registry deploy \
  --node 10.0.0.10 \
  --pk-file ~/.ssh/id_rsa \
  --registry-port 5500 \
  --registry-binary ./registry-linux-amd64
```

### 6.2 Source 优先级

`kcctl registry deploy` 的 source 互斥，优先级仅用于默认推导：

1. `--registry-binary`
2. `--registry-image-archive`
3. `--registry-image`
4. `--component-registry + --version`
5. 默认 `docker.io/kubeclipper/registry:<kcctl-version>`

如果用户显式指定多个 source，命令应失败，避免悄悄选择。

### 6.3 执行流程

```text
resolve source
  -> obtain registry binary locally
  -> copy binary to target node
  -> write registry config
  -> write systemd unit
  -> systemctl enable --now kc-registry
  -> health check /v2/
  -> save registry-config.yaml
```

`obtain registry binary locally` 可以有两种实现：

1. 初期：在本机使用 go-containerregistry 拉取 image layer 并提取 binary，然后通过 SSH 复制。
2. 后续：支持远端节点直接拉取镜像并提取，减少本机网络依赖。

推荐第一阶段由 `kcctl` 本机完成提取，因为：

1. 目标节点不需要 Docker / nerdctl / crane。
2. 目标节点只需要 SSH、systemd、基础 Linux 能力。
3. 更符合 kcctl-only bootstrap 的自包含目标。

## 7. kcctl deploy 设计

### 7.1 推荐命令

在线：

```bash
kcctl deploy \
  --server 10.0.0.10 \
  --agent 10.0.0.10,10.0.0.11 \
  --pk-file ~/.ssh/id_rsa \
  --component-registry docker.io/kubeclipper \
  --package-registry 10.0.0.10:5500 \
  --version v1.8.0
```

内网 Harbor：

```bash
kcctl deploy \
  --server 10.0.0.10 \
  --agent 10.0.0.10,10.0.0.11 \
  --pk-file ~/.ssh/id_rsa \
  --component-registry harbor.example.com/kubeclipper \
  --package-registry harbor.example.com \
  --version v1.8.0
```

旧 `--pkg kc-amd64.tar.gz` 部署入口移除。旧包只能作为迁移输入，用脚本发布为 OCI
artifact 或显式导入 runtime image archive，不能再作为 `kcctl deploy` 的输入。

### 7.2 部署流程

```text
resolve component images
  -> kubeclipper-server image
  -> kubeclipper-agent image
  -> console image or console bundle
extract binaries
copy binaries to nodes
render config
write systemd units
start server/agent
write deploy-config packageRegistry/componentRegistry/imageRegistry
```

第一阶段仍保持 systemd 部署，不要求 server/agent 容器化运行。

原因：

1. 对现有部署模型侵入小。
2. 复用当前运维、日志、升级、清理路径。
3. 组件 image 只是 binary carrier，不改变运行形态。

## 8. 运行镜像交付设计

### 8.1 Image BOM

必须显式记录某个版本组合需要哪些运行镜像。

示例：

```yaml
apiVersion: delivery.kubeclipper.io/v1alpha1
kind: ImageBOM
metadata:
  name: k8s-v1.36.1-containerd-2.2.4-calico-v3.31.5
spec:
  kubernetesVersion: v1.36.1
  cri:
    name: containerd
    version: 2.2.4
  cni:
    name: calico
    version: v3.31.5
  images:
    - source: registry.k8s.io/kube-apiserver:v1.36.1
      target: kube-apiserver:v1.36.1
      digest: sha256:...
    - source: registry.k8s.io/pause:3.10.2
      target: pause:3.10.2
      digest: sha256:...
    - source: quay.io/tigera/operator:v1.40.8
      target: tigera/operator:v1.40.8
      digest: sha256:...
    - source: docker.io/calico/node:v3.31.5
      target: calico/node:v3.31.5
      digest: sha256:...
```

`target` 是写入用户 image registry 后的 repository:tag。安装模板只拼：

```text
{imageRegistry}/{target}
```

### 8.2 Image 命令

建议新增：

```bash
kcctl image list \
  --k8s-version v1.36.1 \
  --cri containerd \
  --cri-version 2.2.4 \
  --cni calico \
  --cni-version v3.31.5
```

输出当前组合所需镜像。

```bash
kcctl image sync \
  --from docker.io/kubeclipper \
  --to harbor.example.com/kubeclipper \
  --bom image-bom.yaml
```

在线同步到 Harbor / 本地 Registry。

```bash
kcctl image save \
  --bom image-bom.yaml \
  --output images-v1.36.1.tar.gz
```

全离线导出。

```bash
kcctl registry push \
  --image-archive images-v1.36.1.tar.gz \
  --node 10.0.0.10 \
  --registry-port 5500
```

导入到 KubeClipper 部署的 registry。

### 8.3 安装前校验

`kcctl create cluster --offline --image-registry ...` 应在创建 operation 前校验：

1. Image BOM 可解析。
2. 目标 image registry 中存在所有必要 image tag。
3. 如果 BOM 有 digest，目标镜像 digest 必须匹配。
4. 如果无法访问 registry，应给出明确错误和修复命令。

示例错误：

```text
image registry preflight failed:
  missing 127.0.0.1:5500/kube-apiserver:v1.36.1
  missing 127.0.0.1:5500/calico/node:v3.31.5

Run:
  kcctl image sync --bom image-bom.yaml --to 127.0.0.1:5500
or:
  kcctl registry push --image-archive images-v1.36.1.tar.gz --node ...
```

## 9. Package artifacts 与 runtime images 的关系

历史 package 里可能包含 `images.tar.gz`。终态不应要求 package 携带运行镜像。

迁移策略：

| 场景 | package 中 `images.tar.gz` | 安装阶段行为 |
| --- | --- | --- |
| 旧包迁移 | 允许存在 | 忽略，不自动 push，不本地 load |
| 新包发布 | 默认不携带 | 必须从 imageRegistry 拉取 |

这可以让新发行包明显变小，同时保留旧包作为迁移输入的能力。运行镜像同步必须通过
Harbor sync、`kcctl image sync/save`，或 `kcctl registry push --image-archive` 显式完成。

## 10. 三种用户场景

### 10.1 在线用户

```bash
curl -L -o kcctl https://release.kubeclipper.io/kcctl-linux-amd64
chmod +x kcctl

kcctl deploy \
  --server 10.0.0.10 \
  --agent 10.0.0.10 \
  --component-registry docker.io/kubeclipper \
  --package-registry registry.kubeclipper.io \
  --image-registry docker.io/kubeclipper \
  --version v1.8.0

kcctl create cluster \
  --name demo \
  --offline=true \
  --image-registry docker.io/kubeclipper \
  --k8s-version v1.36.1 \
  --cri containerd \
  --cri-version 2.2.4 \
  --cni calico \
  --cni-version v3.31.5
```

### 10.2 内网 Harbor 用户

```bash
kcctl image sync \
  --from docker.io/kubeclipper \
  --to harbor.example.com/kubeclipper \
  --bom image-bom.yaml

kcctl image sync \
  --from docker.io/kubeclipper \
  --to harbor.example.com/kubeclipper \
  --components kubeclipper-server,kubeclipper-agent,registry,kc-console \
  --version v1.8.0

kcctl deploy \
  --server 10.0.0.10 \
  --agent 10.0.0.10 \
  --component-registry harbor.example.com/kubeclipper \
  --package-registry harbor.example.com \
  --image-registry harbor.example.com/kubeclipper \
  --version v1.8.0
```

### 10.3 完全离线用户

在有网环境：

```bash
kcctl image save --bom image-bom.yaml --output runtime-images.tar.gz
kcctl image save --components kubeclipper-server,kubeclipper-agent,registry --version v1.8.0 --output component-images.tar.gz
```

在离线环境：

```bash
kcctl registry deploy \
  --node 10.0.0.10 \
  --registry-image-archive registry-v1.8.0-linux-amd64.tar.gz \
  --registry-port 5500

kcctl registry push --node 10.0.0.10 --registry-port 5500 --image-archive runtime-images.tar.gz
kcctl registry push --node 10.0.0.10 --registry-port 5500 --image-archive component-images.tar.gz

kcctl deploy \
  --server 10.0.0.10 \
  --agent 10.0.0.10 \
  --component-registry 10.0.0.10:5500/kubeclipper \
  --package-registry 10.0.0.10:5500 \
  --image-registry 10.0.0.10:5500 \
  --version v1.8.0
```

## 11. CLI 变更建议

### 11.1 `kcctl registry deploy`

新增：

```text
--registry-image string
--registry-image-archive string
--registry-binary string
--component-registry string
--version string
```

移除：

```text
--pkg string   # registry deploy 不再接受 kc-amd64.tar.gz
```

### 11.2 `kcctl registry push`

新增：

```text
--image-archive string   # docker save 导出的 .tar 或 .tar.gz
```

移除：

```text
--pkg string   # registry push 不再使用 package 语义
```

### 11.3 `kcctl deploy`

新增：

```text
--component-registry string
--image-registry string
--version string
```

移除：

```text
--pkg string   # kcctl deploy 不再接受 kc-amd64.tar.gz
```

### 11.4 `kcctl create cluster`

新增：

```text
--image-registry string
```

兼容：

```text
--local-registry string  # alias to imageRegistry, deprecate name later
```

### 11.4 `kcctl image`

新增命令组：

```text
kcctl image list
kcctl image sync
kcctl image save
kcctl image verify
```

## 12. 对抗性审查

### 12.1 如果用户没有外网，第一台 registry 怎么来？

必须支持 `--registry-image-archive` 和 `--registry-binary`。不能让第一个 registry 依赖
它自己。

### 12.2 如果目标节点没有 Docker / nerdctl / crane，怎么从 image 提取 binary？

`kcctl` 本机内置 go-containerregistry 读取 OCI image，提取二进制后通过 SSH 复制到目标
节点。目标节点只需要 Linux + SSH + systemd。

### 12.3 如果 component image 被篡改怎么办？

需要支持 digest pin：

```bash
kcctl deploy --server-image docker.io/kubeclipper/kubeclipper-server@sha256:...
```

并校验 `component-manifest.json` 中 binary digest。默认 tag 只用于便捷，生产文档推荐
digest。

### 12.4 如果 imageRegistry 里缺镜像怎么办？

安装前 preflight 必须失败，不能等 kubeadm 报一堆 `ImagePull`。错误必须列出缺失镜像和
修复命令。

### 12.5 如果 packageRegistry 和 imageRegistry 是同一个地址，会不会混乱？

可以是同一个 Registry，但 repository namespace 必须分离：

```text
kubeclipper/packages/...   package artifacts
kube-apiserver:v1.36.1    runtime images
kubeclipper/server:v1.8.0 component images
```

语义上仍是三个事实来源，不能因为地址相同而混成一个职责。

### 12.6 如果用户使用 Harbor 同步镜像，tag 重写怎么办？

Image BOM 必须记录 `source` 与 `target`。安装模板只使用 `target`。同步工具负责从
`source` 复制到 `{imageRegistry}/{target}`。

### 12.7 如果同一版本组合需要不同 pause/coredns 版本怎么办？

不要推导，全部来自 Image BOM。BOM 是运行镜像集合的事实来源。

### 12.8 如果继续保留旧 `kc-amd64.tar.gz` 会不会拖慢演进？

会，所以不保留 legacy fallback。旧包只允许作为迁移输入，转换为 package OCI artifacts
或显式导入 runtime image archive；部署、安装、升级主流程不再接受 `--pkg`。

## 13. 迁移计划

### Phase 0: 文档与语义纠偏

1. 明确 `packageRegistry` 不等于 `imageRegistry`。
2. 明确安装阶段不负责 push 运行镜像。
3. 文档推荐 `kcctl registry push` 或 Harbor sync 预置镜像。

### Phase 1: registry deploy 移除 `--pkg`

1. registry 组件发布为 OCI image。
2. `kcctl registry deploy --registry-image` 从 image 提取 binary。
3. `kcctl registry deploy` 不再接受 `--pkg kc-amd64.tar.gz`。

### Phase 2: deploy 移除 `--pkg`

1. server/agent 发布为 component images。
2. `kcctl deploy --component-registry --version` 提取 server/agent 二进制。
3. `kcctl join` 同步支持 component image source。
4. `kcctl deploy` / `kcctl join` 不再接受 `--pkg`。

### Phase 3: image BOM 与 image 命令

1. 引入 Image BOM。
2. 新增 `kcctl image list/sync/save/verify`。
3. `kcctl create cluster` 增加 image preflight。

### Phase 4: 小包终态

1. Release 只发布 `kcctl-linux-amd64`、`kcctl-linux-arm64`、checksum、signature。
2. `kc-amd64.tar.gz` 停止作为主路径发布。
3. 旧 static resource 包只作为迁移输入。

## 14. 验收标准

只有满足以下条件，才能认为 kcctl-only bootstrap 设计实现完成：

1. 用户只下载 `kcctl` 即可部署 KubeClipper control plane。
2. `kcctl registry deploy` 不需要 `kc-amd64.tar.gz`。
3. `kcctl deploy` 不需要 `kc-amd64.tar.gz`。
4. server/agent/registry 二进制来自 component image，并经过 digest 校验。
5. Kubernetes 运行镜像由 imageRegistry 提供，安装阶段不 push 镜像。
6. 缺失运行镜像时，preflight 在 operation 创建前给出明确错误。
7. packageRegistry 仍只负责 package OCI artifacts。
8. `kcctl image sync/save/verify` 能支撑 Harbor 和全离线场景。
9. 旧 `--pkg` 路径不可用，命令帮助、示例、测试都不再暴露该入口。

## 15. 推荐结论

推荐采用 **kcctl-only + component image as binary carrier + explicit imageRegistry** 的设计。

这比继续维护大包更简单，也比安装阶段隐式 push 镜像更可靠：

1. bootstrap 最小化。
2. 组件版本可 digest 化。
3. 镜像同步变成显式准备步骤。
4. 在线、内网 Harbor、完全离线三种场景都能解释清楚。
5. `packageRegistry`、`imageRegistry`、`componentRegistry` 边界清晰，不再复活 static
   server 心智。
