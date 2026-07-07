---
comet_change: oci-resource-delivery
role: technical-design
canonical_spec: openspec
---

# KubeClipper OCI 资源分发设计

## 1. 目标

本文只设计一个终态：

**使用 OCI Registry 替换 KubeClipper static server。**

替换完成后，KubeClipper 的离线资源分发域只保留三类事实来源：

1. **OCI Registry**
   - 保存所有离线资源包。
   - 是资源库存的唯一事实来源。
2. **PackageInventory**
   - 由 Registry 扫描动态生成。
   - 是运行时派生视图，不落成中心索引文件。
3. **SupportPolicy**
   - 保存 Kubernetes 与组件版本的支持矩阵。
   - 是产品支持知识的唯一事实来源。

不设计、不保留、不依赖：

1. static server
2. static resource tree
3. `metadata.json`
4. `catalog.json`
5. HTTP resource URL
6. local file transport
7. push/delete 时维护索引文件
8. 安装失败后的其他下载路径

最终链路固定为：

```text
Registry scan -> PackageInventory -> resolve with SupportPolicy -> fetch OCI by digest -> install from local materialized files
```

## 2. 第一性原则

static server 今天混在一起承担了四个不同职责：

1. 保存资源字节内容
2. 展示当前有哪些资源
3. 保存组件版本支持矩阵
4. 提供 bootstrap / extension 等特殊资源下载路径

这四件事本质不同。用 OCI 替换 static server 时，不能只是把 HTTP 地址换成 OCI 地址，而要把职责重新拆开：

| 职责 | 新归属 | 原因 |
| --- | --- | --- |
| 保存资源字节 | OCI Registry | Registry 天然负责 blob、manifest、digest、tag |
| 资源库存 | Registry 扫描生成 PackageInventory | 库存应该来自仓库真实内容 |
| 版本支持矩阵 | SupportPolicy | 发包和产品支持是两件事 |
| 下载与物化 | OCI Fetcher | 下载必须按 digest，结果必须可校验 |
| 安装决策 | Resolver | 安装前一次性决定版本和 artifact |

这个拆分带来一个核心约束：

**Registry 里有包，只代表包存在；SupportPolicy 允许，才代表产品支持；Resolver 命中二者交集，才代表本次可以安装。**

## 3. 总体架构

```text
                        +----------------------+
                        | SupportPolicy        |
                        | 版本支持矩阵         |
                        +----------+-----------+
                                   |
                                   v
+-----------------------+   +------+-------------------+
| OCI Registry          |   | Resolver                 |
| kubeclipper/packages/ |-->| policy + inventory       |
+-----------+-----------+   | -> resolved plan         |
            |               +------+-------------------+
            v                      |
+-----------+-----------+          v
| Registry Indexer      |   +------+-------------------+
| scan + validate       |   | OCI Fetcher              |
| -> PackageInventory   |   | fetch by digest          |
+-----------+-----------+   | -> materialized files    |
            |               +------+-------------------+
            |                      |
            +----------+-----------+
                       v
              +--------+---------+
              | Install Steps    |
              | consume local    |
              | files only       |
              +------------------+
```

角色边界必须保持清晰：

1. **OCI Registry**
   - 只保存 artifact。
   - 不保存 KubeClipper 的版本支持矩阵。
   - 不保存全局 catalog。
2. **Registry Indexer**
   - 扫描 `kubeclipper/packages/`。
   - 读取 OCI manifest / index。
   - 读取每个 artifact 内部的 KubeClipper package manifest。
   - 生成内存态 `PackageInventory`。
3. **SupportPolicy**
   - 只描述版本支持关系。
   - 不包含 URL、digest、状态、资源是否已上传。
4. **Resolver**
   - 只做选择与校验。
   - 输出 digest-pinned plan。
5. **Fetcher**
   - 只按 plan 拉取和物化。
   - 不做版本选择。
6. **Installer**
   - 只消费 fetcher 产出的本地文件。
   - 不拼下载地址。

## 4. static server 职责替换表

| static server 旧职责 | OCI-only 新设计 |
| --- | --- |
| 保存离线包 | OCI artifact 保存在 Registry |
| `/resource/...` HTTP 下载 | fetcher 使用 `ref + digest` 拉取 |
| `metadata.json` 记录资源库存 | Registry scan 动态生成 `PackageInventory` |
| `metadata.json` 记录支持矩阵 | `SupportPolicy` 独立维护 |
| `catalog.json` 或类似索引 | 不存在中心 catalog 文件 |
| bootstrap binary 下载 | `binary` package |
| extension 下载 | `extension` package |
| deploy/join 注入 static server 地址 | deploy/join 只配置 package registry |
| server 启动 staticresource 服务 | 删除资源分发相关 static service |

判断某段设计是否正确，可以用一句话检查：

**如果它需要 static server、static path、HTTP resource URL、metadata 文件或 catalog 文件才能工作，它就不属于目标架构。**

## 5. OCI 包命名

统一命名规则：

```text
{registry}/kubeclipper/packages/{kind}/{name}:{version}
```

示例：

```text
registry.local:5000/kubeclipper/packages/k8s/k8s:v1.36.0
registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0
registry.local:5000/kubeclipper/packages/cni/calico:v3.30.0
registry.local:5000/kubeclipper/packages/extension/kubectl-terminal:v1.0.0
registry.local:5000/kubeclipper/packages/binary/kubeclipper-agent:v1.8.0
registry.local:5000/kubeclipper/packages/binary/etcdctl:v3.5.15
```

字段语义：

| 字段 | 来源 | 语义 |
| --- | --- | --- |
| registry | 配置 | Registry 地址 |
| `kubeclipper/packages` | 固定前缀 | 限制扫描范围 |
| kind | repository path | 包类别 |
| name | repository path | 包名称 |
| version | tag | 逻辑版本 |
| arch | OCI platform | 平台架构，不进入 tag |

`arch` 不放进 tag，原因是 OCI 已经用 manifest index 表达多平台。

正确结构是：

```text
kubeclipper/packages/cri/containerd:2.1.0
  -> OCI index
      -> linux/amd64 manifest
      -> linux/arm64 manifest
```

扫描后再投影成：

```text
(cri, containerd, 2.1.0, amd64)
(cri, containerd, 2.1.0, arm64)
```

不引入 channel。是否支持、是否默认、是否可选，由 `SupportPolicy` 决定。

## 6. OCI artifact 结构

每个 package version 可以是：

1. 单平台 manifest
2. 多平台 index

推荐统一使用 index，这样即使当前只有一个架构，后续补充架构时语义也稳定。

每个平台 manifest 必须包含：

1. 一个 KubeClipper package manifest layer
2. 一个或多个 payload layer

推荐 media type：

```text
application/vnd.kubeclipper.package.manifest.v1+json
application/vnd.kubeclipper.configs.layer.v1.tar+gzip
application/vnd.kubeclipper.images.layer.v1.tar+gzip
application/vnd.kubeclipper.charts.layer.v1.tgz
application/vnd.kubeclipper.binary.layer.v1
```

### 6.1 KubeClipper package manifest

每个平台 artifact 内必须有一份自描述 manifest。

它只描述当前 artifact 自己，不是全局 catalog。

示例：

```json
{
  "schemaVersion": 1,
  "kind": "cri",
  "name": "containerd",
  "version": "2.1.0",
  "profile": "runtime",
  "platform": {
    "os": "linux",
    "arch": "amd64"
  },
  "contents": [
    {
      "name": "configs",
      "file": "configs.tar.gz",
      "mediaType": "application/vnd.kubeclipper.configs.layer.v1.tar+gzip",
      "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "size": 1024
    },
    {
      "name": "images",
      "file": "images.tar.gz",
      "mediaType": "application/vnd.kubeclipper.images.layer.v1.tar+gzip",
      "digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "size": 2048
    }
  ]
}
```

强制校验：

1. repository 中的 `kind/name` 必须与 manifest 一致。
2. tag 中的 `version` 必须与 manifest 一致。
3. OCI descriptor platform 必须与 manifest platform 一致。
4. manifest 中声明的 content digest 必须能在 artifact layers 中找到。
5. profile 要求的 content 必须存在。

### 6.2 profile

profile 用于 package 内部早期校验，不参与版本支持判断。

建议 profile：

| profile | 适用 kind | 最低 content 要求 |
| --- | --- | --- |
| `k8s` | `k8s` | `configs`、`images` |
| `runtime` | `cri` | `configs`、`images` |
| `network` | `cni` | `configs` 或 `charts`，以及需要的 `images` |
| `storage` | `csi` | `configs` 或 `charts`，以及需要的 `images` |
| `addon` | addon 类组件 | `charts` 或 `configs`，以及需要的 `images` |
| `extension` | `extension` | extension 定义的显式 content |
| `binary` | `binary` | `binary` |

profile 只回答：

**这个包自身是不是完整。**

它不回答：

**这个版本是否被当前产品支持。**

## 7. PackageInventory

`PackageInventory` 是 Registry 扫描结果。

它是内存态派生对象，可以缓存，可以通过 API 返回，但不能作为人工维护的中心文件。

示例结构：

```json
{
  "apiVersion": "delivery.kubeclipper.io/v1alpha1",
  "kind": "PackageInventory",
  "metadata": {
    "name": "registry-derived"
  },
  "spec": {
    "registry": "registry.local:5000",
    "packages": [
      {
        "kind": "cri",
        "name": "containerd",
        "version": "2.1.0",
        "os": "linux",
        "arch": "amd64",
        "transport": {
          "type": "oci",
          "ref": "registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0",
          "digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111"
        },
        "contents": [
          {
            "name": "configs",
            "file": "configs.tar.gz",
            "mediaType": "application/vnd.kubeclipper.configs.layer.v1.tar+gzip",
            "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "size": 1024
          },
          {
            "name": "images",
            "file": "images.tar.gz",
            "mediaType": "application/vnd.kubeclipper.images.layer.v1.tar+gzip",
            "digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "size": 2048
          }
        ]
      }
    ]
  }
}
```

### 7.1 包身份

包身份固定为：

```text
(kind, name, version, os, arch)
```

约束：

1. 同一个 identity 只能对应一个有效平台 manifest。
2. identity 相同但 digest 不同，扫描失败。
3. 不根据上传时间、tag 时间或字典序选择。
4. 不维护 active/inactive/deleted 状态。

状态语义只有一个：

```text
扫描到并校验通过 = 可用
扫描不到或校验失败 = 不可用
```

### 7.2 TransportRef

`TransportRef` 只支持 OCI：

```json
{
  "type": "oci",
  "ref": "registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0",
  "digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111"
}
```

语义：

1. `ref` 是 tag 形式的逻辑引用。
2. `digest` 是最终平台 manifest digest。
3. 如果 tag 指向 index，resolver/indexer 必须先选出平台 manifest digest。
4. fetcher 不再根据 index 重新选平台。

这样 plan 一旦生成，就已经固定到不可变对象。

## 8. Registry 扫描器

扫描范围固定为：

```text
kubeclipper/packages/
```

不扫描整个 Registry。

### 8.1 扫描流程

固定流程：

1. 列出 repositories。
2. 过滤 `kubeclipper/packages/` 前缀。
3. 校验 repository path 必须是 `kubeclipper/packages/{kind}/{name}`。
4. 列出 tags。
5. 读取每个 tag 指向的 manifest 或 index。
6. 如果是 index，枚举 platform descriptors。
7. 读取每个平台 manifest。
8. 找到 KubeClipper package manifest layer。
9. 解析并校验 package manifest。
10. 生成 `PackageInventory` 条目。
11. 校验 identity 唯一性。

### 8.2 错误策略

建议策略：

| 场景 | 结果 |
| --- | --- |
| Registry 不可达 | 扫描失败 |
| 无权限列 repository/tag | 扫描失败 |
| 单个 repository 命名非法 | 跳过并记录 warning |
| 单个 tag manifest 读取失败 | 跳过并记录 warning |
| 单个 artifact 缺少 package manifest | 跳过并记录 warning |
| package manifest 与 repository/tag/platform 不一致 | 跳过并记录 warning |
| content digest 与 layer 不一致 | 跳过并记录 warning |
| 同一 identity 出现多个有效 digest | 扫描失败 |

原因：

1. 仓库不可达或无权限时，无法建立库存视图。
2. 单个坏包不应该让整个 Registry 不可用。
3. identity 冲突会导致安装结果不确定，必须失败。

### 8.3 缓存

扫描可以缓存，但缓存不能成为另一套真相。

建议缓存：

1. inventory cache
   - key: `registry`
   - TTL: 5 分钟
2. manifest cache
   - key: `ref + digest`
   - digest 不变时可复用

必须提供强制刷新：

1. API refresh
2. CLI `--refresh`

缓存刷新后，新的扫描结果覆盖旧缓存。

## 9. SupportPolicy

`SupportPolicy` 替代 `metadata.json` 中的版本支持矩阵部分。

它只表达产品支持知识，不表达资源库存。

### 9.1 Policy 包含什么

包含：

1. 支持哪些 Kubernetes 版本范围。
2. 每个 Kubernetes 版本范围需要哪些 component slot。
3. 每个 slot 允许哪些 kind/name/version。
4. 默认选项。
5. 跨组件约束。

不包含：

1. OCI ref
2. digest
3. URL
4. artifact 上传状态
5. active/deleted 状态
6. package contents

### 9.2 示例

```json
{
  "apiVersion": "delivery.kubeclipper.io/v1alpha1",
  "kind": "SupportPolicy",
  "metadata": {
    "name": "default"
  },
  "spec": {
    "rules": [
      {
        "name": "kubernetes-v1.36",
        "match": {
          "kubernetesVersion": "v1.36.*"
        },
        "slots": [
          {
            "slot": "cri",
            "required": true,
            "default": {
              "kind": "cri",
              "name": "containerd",
              "version": "2.1.0"
            },
            "options": [
              {
                "kind": "cri",
                "name": "containerd",
                "versions": ["2.1.0"]
              }
            ]
          },
          {
            "slot": "cni",
            "required": true,
            "default": {
              "kind": "cni",
              "name": "calico",
              "version": "v3.30.0"
            },
            "options": [
              {
                "kind": "cni",
                "name": "calico",
                "versions": ["v3.30.0"]
              }
            ]
          }
        ],
        "constraints": [
          {
            "when": {
              "slot": "cri",
              "name": "containerd",
              "version": "2.1.0"
            },
            "require": {
              "kubernetesVersion": ">=v1.33.0"
            }
          }
        ]
      }
    ]
  }
}
```

### 9.3 Policy 存储

建议作为 KubeClipper 控制面配置保存，例如：

1. ConfigMap
2. API resource
3. 内置默认 policy 文件

无论采用哪种存储，都必须满足：

1. policy 更新不依赖 package push/delete。
2. package push/delete 不修改 policy。
3. resolver 每次只读取当前 policy 与当前 inventory 的交集。

## 10. Resolver

Resolver 是安装决策中心。

输入：

1. `kubernetesVersion`
2. `os`
3. `arch`
4. 用户显式选择的组件
5. `SupportPolicy`
6. `PackageInventory`

输出：

```go
type ResolvedArtifactPlan struct {
    KubernetesVersion string
    OS                string
    Arch              string
    Components        []ResolvedComponent
}

type ResolvedComponent struct {
    Slot      string
    Kind      string
    Name      string
    Version   string
    Required  bool
    Transport TransportRef
    Contents  []ArtifactContent
}
```

解析顺序固定：

1. 用 Kubernetes version 匹配 policy rule。
2. 计算每个 slot 的最终选择。
3. 用户未指定时使用 policy default。
4. 校验用户指定值是否在 policy options 内。
5. 校验跨组件约束。
6. 到 inventory 查找 `(kind, name, version, os, arch)`。
7. 生成带平台 manifest digest 的 plan。

约束：

1. 先判断产品是否支持，再判断仓库是否有包。
2. 不支持 `latest`。
3. 不根据 Registry 中的最高版本自动选择。
4. 不根据 artifact 上传时间自动选择。
5. plan 生成后，安装步骤不能改写组件版本。

建议错误类型：

| 错误 | 含义 |
| --- | --- |
| `UnsupportedKubernetesVersion` | policy 不支持该 Kubernetes 版本 |
| `UnsupportedComponentSlot` | policy 中不存在该 slot |
| `UnsupportedComponentChoice` | kind/name 不被该 slot 允许 |
| `UnsupportedComponentVersion` | version 不被该 slot 允许 |
| `ComponentConstraintViolation` | 跨组件约束冲突 |
| `ArtifactNotPublished` | policy 支持，但 Registry 没有对应包 |
| `ArtifactArchUnavailable` | 有该版本，但没有目标架构 |

## 11. Fetcher

Fetcher 只负责把 resolved plan 中的 artifact 拉下来并物化。

固定输入：

```text
ref + platform manifest digest + contents
```

固定流程：

1. 使用 `ref@digest` 拉取平台 manifest。
2. 校验拉取对象 digest。
3. 读取 KubeClipper package manifest。
4. 校验 package manifest 与 plan 中的 component 一致。
5. 按 `contents` 拉取 payload layers。
6. 校验每个 payload digest。
7. 物化到本地目录。
8. 返回本地路径映射。

建议物化目录：

```text
{workdir}/packages/{kind}/{name}/{version}/{os}-{arch}/
  manifest.json
  contents/
    configs.tar.gz
    images.tar.gz
    charts.tgz
    binary
```

返回结构：

```go
type FetchedPackage struct {
    Kind      string
    Name      string
    Version   string
    OS        string
    Arch      string
    BaseDir   string
    Contents  map[string]string
    Transport TransportRef
}
```

失败策略：

1. Registry 拉取失败，安装失败。
2. digest mismatch，安装失败。
3. package manifest 非法，安装失败。
4. payload 缺失，安装失败。
5. installer 需要的 content 不存在，安装失败。

Fetcher 不做：

1. 版本选择
2. 默认值推断
3. policy 校验
4. 其他传输方式尝试

## 12. Installer 输入契约

这次替换 static server，目标是替换资源来源和传输模型，不要求同时重写每个 installer 的业务语义。

因此 installer 可以继续使用当前熟悉的 content 名称：

1. `configs`
2. `images`
3. `charts`
4. `binary`

但来源必须变成 fetcher 物化结果。

安装步骤中的规则：

1. 不拼 URL。
2. 不读 static 目录。
3. 不读 `metadata.json`。
4. 不判断组件版本是否支持。
5. 不自己从 Registry 重新 resolve。
6. 只消费 `FetchedPackage.Contents`。

## 13. ComponentMeta

`ComponentMeta` 仍然可以作为对外展示视图存在，但它不再是事实来源。

生成方式固定为：

```text
SupportPolicy + PackageInventory -> projector -> ComponentMeta response
```

投影规则：

1. rules 来自 policy。
2. addons/package availability 来自 inventory。
3. 默认版本来自 policy。
4. 当前是否可安装来自 policy 与 inventory 的交集。
5. arch 过滤基于 inventory。

示例语义：

1. policy 支持 `containerd 2.1.0`，inventory 有 `amd64` 和 `arm64`，则两个架构都可展示。
2. policy 支持 `calico v3.30.0`，inventory 只有 `amd64`，则 `arm64` 视图不可安装。
3. inventory 有 `containerd 2.2.0`，policy 不支持，则不作为可选安装项展示。

## 14. Bootstrap 与 Extension

特殊资源必须进入统一 package 模型。

### 14.1 Bootstrap binary

这些资源使用 `binary` kind：

```text
kubeclipper/packages/binary/kubeclipper-agent:{version}
kubeclipper/packages/binary/etcdctl:{version}
```

安装流程：

1. provider 提交 binary resolve request。
2. resolver 从 policy + inventory 得到 binary package。
3. fetcher 拉取并物化 binary。
4. provider 使用本地 binary 文件。

provider 不再生成 static HTTP 地址。

### 14.2 Extension

extension 使用 `extension` kind：

```text
kubeclipper/packages/extension/kubectl-terminal:{version}
```

流程与普通组件一致：

1. policy 决定 extension 是否支持及默认版本。
2. inventory 决定该 extension 是否已发布。
3. resolver 产出 digest-pinned plan。
4. fetcher 物化 extension 内容。

## 15. 用户工作流

目标工作流：

1. 部署 OCI Registry。
2. 将离线 package 作为 OCI artifact 推送到 `kubeclipper/packages/...`。
3. 维护 `SupportPolicy`。
4. KubeClipper 扫描 Registry 生成 inventory。
5. 安装集群时执行 resolve。
6. 根据 resolved plan fetch by digest。
7. installer 使用本地物化文件安装。

上传和删除资源是 Registry 行为。

KubeClipper 不需要通过 `resource push/delete` 来维护自己的资源索引，因为资源索引来自扫描。

如果后续仍保留 `kcctl resource`，它只能是 Registry 操作的薄封装或排障入口：

1. `list`
   - 扫描 Registry 并展示 inventory。
2. `inspect`
   - 展示 OCI manifest/index/package manifest。
3. `refresh`
   - 刷新 inventory cache。

核心架构不依赖 `kcctl resource push/delete`。

## 16. 配置

资源分发相关配置只需要表达 Registry。

建议字段：

```yaml
packageRegistry: registry.local:5000
```

语义：

1. KubeClipper offline package Registry。
2. server、deploy、join、agent 使用同一个 Registry 来源。
3. 不再配置 static server 地址。
4. 不再配置 resource HTTP URL。

如果需要认证，认证信息属于 Registry client 配置，不改变资源模型。

## 17. 代码落点

建议落点：

| 模块 | 路径 | 职责 |
| --- | --- | --- |
| Delivery API | [pkg/delivery/apis](/Users/lixueduan/17x/kc-release/kubeclipper/pkg/delivery/apis) | PackageInventory、SupportPolicy、KCPackageManifest、resolver types |
| Registry Indexer | [pkg/delivery/indexer](/Users/lixueduan/17x/kc-release/kubeclipper/pkg/delivery/indexer) | scan Registry -> inventory |
| OCI Fetcher | [pkg/delivery/fetcher](/Users/lixueduan/17x/kc-release/kubeclipper/pkg/delivery/fetcher) | fetch by digest -> local files |
| Component projector | [pkg/delivery/apis](/Users/lixueduan/17x/kc-release/kubeclipper/pkg/delivery/apis) | policy + inventory -> ComponentMeta |
| Config API | [pkg/apis/config/v1](/Users/lixueduan/17x/kc-release/kubeclipper/pkg/apis/config/v1) | componentmeta、policy read/apply、inventory refresh |
| Core API | [pkg/apis/core/v1](/Users/lixueduan/17x/kc-release/kubeclipper/pkg/apis/core/v1) | install resolve source、resolved plan 注入 |
| Component common | [pkg/component/common](/Users/lixueduan/17x/kc-release/kubeclipper/pkg/component/common) | consume fetched packages |
| K8s/CRI/CNI schemes | [pkg/scheme/core/v1](/Users/lixueduan/17x/kc-release/kubeclipper/pkg/scheme/core/v1) | 不再拼 static URL |
| Bootstrap provider | [pkg/clustermanage/kubeadm/provider.go](/Users/lixueduan/17x/kc-release/kubeclipper/pkg/clustermanage/kubeadm/provider.go) | binary/extension package resolve |
| Deploy/Join CLI | [pkg/cli/deploy](/Users/lixueduan/17x/kc-release/kubeclipper/pkg/cli/deploy), [pkg/cli/join](/Users/lixueduan/17x/kc-release/kubeclipper/pkg/cli/join) | 注入 packageRegistry |
| Server wiring | [pkg/server](/Users/lixueduan/17x/kc-release/kubeclipper/pkg/server) | 删除 staticresource wiring |

## 18. 实现顺序

这是编码顺序，不是双轨迁移设计。

### Step 1: 定义模型

交付：

1. `KCPackageManifest`
2. `PackageInventory`
3. `SupportPolicy`
4. `TransportRef{type, ref, digest}`
5. validation
6. resolver typed errors

验收：

1. package identity 可唯一表达。
2. policy 不包含 ref/digest/url/status。
3. inventory 只包含从 artifact 扫描得到的信息。

### Step 2: 实现 Registry Indexer

交付：

1. `kubeclipper/packages/` 扫描。
2. repository/tag 校验。
3. manifest/index 解析。
4. package manifest 解析。
5. multi-arch 投影。
6. cache + refresh。

验收：

1. 标准包能进入 inventory。
2. 非法包被跳过并记录 warning。
3. identity 冲突会失败。
4. `arch` 来自 OCI platform，不来自 tag。

### Step 3: 实现 SupportPolicy 与 ComponentMeta 投影

交付：

1. policy read/apply。
2. policy validation。
3. projector。
4. componentmeta handler 改为 policy + inventory。

验收：

1. 不再读取 `metadata.json`。
2. inventory 中存在但 policy 不支持的包不会成为可安装项。
3. policy 支持但 inventory 缺失的包能明确显示为未发布或不可安装。

### Step 4: 实现 Resolver

交付：

1. cluster install resolve。
2. addon/extension resolve。
3. bootstrap binary resolve。
4. digest-pinned plan。

验收：

1. resolve 发生在 install step 之前。
2. plan 中每个 component 都有 OCI ref 和平台 manifest digest。
3. installer 不再做版本选择。

### Step 5: 实现 OCI Fetcher

交付：

1. `ref@digest` 拉取。
2. package manifest 校验。
3. payload digest 校验。
4. 本地物化目录。
5. fetch result。

验收：

1. fetcher 只支持 OCI。
2. fetch 失败直接返回错误。
3. installer 只使用本地物化文件。

### Step 6: 改造安装步骤

交付：

1. k8s 安装使用 fetched `configs/images`。
2. cri 安装使用 fetched `configs/images`。
3. cni/csi/addon 使用 fetched `configs/charts/images`。
4. bootstrap 使用 fetched `binary`。
5. extension 使用 fetched package。

验收：

1. step 中没有 static URL 拼接。
2. step 中没有 `metadata.json` 读取。
3. step 中没有传输方式选择。

### Step 7: 删除 static server 资源分发代码

交付：

1. 删除 staticresource service。
2. 删除 simple staticserver 配置与 wiring。
3. 删除 legacy HTTP/local file fetcher。
4. 删除 deploy/join 注入 static server 地址。
5. 删除 resource metadata/catalog 读写。

验收：

1. server 不再启动资源静态服务。
2. deploy/join 只传递 package registry。
3. 运行时资源链路只剩 OCI。

## 19. 测试设计

### 19.1 Model / Validation

覆盖：

1. repository 与 manifest kind/name 不一致。
2. tag 与 manifest version 不一致。
3. OCI platform 与 manifest platform 不一致。
4. content digest 找不到对应 layer。
5. profile 必需 content 缺失。
6. policy 中 default 不在 options 内。
7. policy 中出现 ref/digest/url/status 字段时失败。

### 19.2 Registry Indexer

覆盖：

1. 单平台 manifest 扫描。
2. 多平台 index 扫描。
3. 非 `kubeclipper/packages/` repository 被忽略。
4. 非法 repository path 被忽略。
5. 缺少 package manifest 的 artifact 被忽略。
6. identity 冲突导致扫描失败。
7. cache 命中。
8. refresh 强制重扫。

### 19.3 Resolver

覆盖：

1. policy default 解析成功。
2. 用户显式选择解析成功。
3. 不支持的 Kubernetes 版本。
4. 不支持的 component slot。
5. 不支持的 kind/name。
6. 不支持的 version。
7. policy 支持但 artifact 未发布。
8. artifact 存在但目标架构缺失。
9. 跨组件约束冲突。

### 19.4 Fetcher

覆盖：

1. `ref@digest` 成功拉取。
2. digest mismatch 失败。
3. package manifest 缺失失败。
4. content layer 缺失失败。
5. payload digest mismatch 失败。
6. 物化目录和 contents map 正确。

### 19.5 End-to-end

覆盖：

1. k8s + cri + cni 完整 resolve/fetch/install 输入链路。
2. bootstrap binary resolve/fetch。
3. extension resolve/fetch。
4. componentmeta 来自 policy + inventory。
5. Registry 新增 package 后 refresh 可见。
6. Registry 删除 package 后 refresh 不可见。

## 20. 删除清单

最终必须删除或移出资源分发主链路：

1. `metadata.json` 读写逻辑。
2. `catalog.json` 读写逻辑。
3. static resource tree。
4. staticresource service。
5. simple staticserver resource wiring。
6. legacy HTTP fetcher。
7. local file fetcher。
8. deploy/join 中 static server 地址注入。
9. installer 中 static URL 拼接。
10. push/delete 修改 metadata/catalog 的逻辑。

## 21. 验收标准

只有同时满足下面条件，才算 OCI 替换 static server 完成：

1. Registry 是资源库存唯一来源。
2. PackageInventory 只由 Registry 扫描生成。
3. SupportPolicy 是版本支持矩阵唯一来源。
4. Resolver 输出 digest-pinned plan。
5. Fetcher 只支持 OCI，并按平台 manifest digest 拉取。
6. Installer 只消费本地物化文件。
7. bootstrap binary 使用 `binary` package。
8. extension 使用 `extension` package。
9. server 不启动资源 static service。
10. deploy/join 不注入 static server 地址。
11. 主链路不读取 `metadata.json` 或 `catalog.json`。
12. push/delete 不再维护任何中心索引文件。

建议保留工程化 grep：

```bash
rg -n "metadata.json|catalog.json|legacy-http|TransportLegacyHTTP|TransportLocalFile|local-file|LocalFileFetcher|CloudStaticServer|staticServer:|StaticServer|staticresource|simple/staticserver" pkg cmd kubeclipper-server.yaml --glob '!**/*_test.go'
```

期望：

1. 资源分发主链路无命中。
2. 如果命中普通 Kubernetes metadata、测试名或非资源分发语义，需要在审计中说明。

## 22. 最终结论

OCI 替换 static server 的核心不是“换下载协议”，而是重新定义资源分发事实来源：

1. **包存在性来自 Registry。**
2. **版本支持关系来自 SupportPolicy。**
3. **安装输入来自 resolver 生成的 digest-pinned plan。**
4. **安装文件来自 OCI fetcher 的本地物化结果。**

因此最终架构中，KubeClipper 不维护 static resource tree，不维护 metadata/catalog 文件，也不依赖 resource push/delete 来同步资源状态。

KubeClipper 只维护支持矩阵，并从 OCI Registry 动态发现、解析、拉取离线 package。
