---
comet_change: zero-to-oci-offline-package-build
role: validation-report
canonical_spec: openspec
---

# OCI 同步能力验证记录

本文记录三类 Registry 内容的实测结果，用于决定 KubeClipper 离线分发格式。

测试目标：

1. 用户从在线 Registry 获取 KubeClipper 所需内容。
2. 用户可以下载离线包或使用同步工具导入本地 Registry。
3. `kcctl registry deploy` 先部署本地 Registry。
4. 后续安装使用本地 Registry 作为组件、package、chart、runtime image 来源。

## 1. 测试环境

测试机器：`sh-dev-3`

工具版本：

```text
Docker 29.1.3
skopeo 1.13.3
helm v3.10.3
crane devel, from local cross build
registry:2
```

测试拓扑：

```text
source registry: 127.0.0.1:5501
target registry: 127.0.0.1:5502
```

## 2. 测试对象

| 类型 | 示例 | 说明 |
| --- | --- | --- |
| 标准 OCI image | `kubeclipper/test-standard:v0` | 模拟 `kubeclipper-server`、`kubeclipper-agent` 这种二进制载体镜像。 |
| Helm OCI chart | `kubeclipper/charts/testchart:0.1.0` | 模拟 Calico、CSI NFS chart。 |
| 旧 KubeClipper custom package artifact | `kubeclipper/packages/k8s/k8s:v0.0.0` | 旧版 `oci-publish --profile k8s` 生成，模拟迁移前的 `k8s/k8s`、`cri/containerd`。 |
| 新 KubeClipper package image | `kubeclipper/packages/k8s/k8s:v0.0.0` | 标准 OCI image，镜像内放 `/package/kc-package-manifest.json` 和 `/package/<file>`。 |

## 3. 实测结果

| 能力 | 标准 OCI image | Helm OCI chart | 旧 custom package artifact | 新 package image |
| --- | --- | --- | --- | --- |
| `docker pull` | 通过 | 通过，Docker 29 可拉取 | 失败，解包时报 `mismatched image rootfs and manifest layers` | 通过，等同标准 image |
| `docker save/load` | 通过 | 通过，Docker 29 可保存和加载 | 不通过，因为 `docker pull` 已失败 | 通过，等同标准 image |
| `crane copy` | 通过 | 通过 | 失败，Registry 返回 `MANIFEST_INVALID` | 通过，等同标准 image |
| `skopeo copy` | 通过 | 通过 | 失败，`unsupported docker v2s2 media type: "application/vnd.kubeclipper.package.manifest.v1+json"` | 通过，等同标准 image |
| `skopeo sync` | 通过 | 通过 | 失败，同样不支持 KubeClipper 自定义 package media type | 通过，等同标准 image |
| 原生消费工具 | `docker pull/save/load` | `helm pull/push` | 只能由 KubeClipper delivery fetcher 消费 | `docker pull/save/load`、`skopeo copy/sync`；KC fetcher 从 `/package` 提取文件 |

## 4. 关键错误

### 4.1 旧 KubeClipper custom package artifact 无法被 Docker 当镜像处理

```text
failed to unpack image on snapshotter overlayfs:
mismatched image rootfs and manifest layers
```

### 4.2 旧 KubeClipper custom package artifact 无法被 skopeo copy/sync

```text
unsupported docker v2s2 media type:
"application/vnd.kubeclipper.package.manifest.v1+json"
```

### 4.3 旧 KubeClipper custom package artifact 无法被 crane copy

```text
MANIFEST_INVALID: manifest invalid
```

## 5. 结论

如果目标是：

```text
用户用 skopeo sync/copy、docker save/load、Harbor replication
把内容从在线 Registry 搬到本地 Registry
```

那么应优先使用 **标准 OCI image** 作为离线分发载体。

推荐分发方式调整为：

| 内容 | 推荐形态 | 原因 |
| --- | --- | --- |
| `kcctl` | GitHub Release 单二进制 | 用户第一步只需要 bootstrap 工具。 |
| `kubeclipper-server` | 标准 OCI image | 需要 `skopeo sync`、`docker save/load`；安装时可以只提取二进制，不容器运行。 |
| `kubeclipper-agent` | 标准 OCI image | 同上。 |
| `registry` | 标准 OCI image | `kcctl registry deploy` 可从 image 或 image archive 提取 registry binary。 |
| `kc-console` | 标准 OCI image | 静态文件可以用 image 分发，也便于同步。 |
| Kubernetes/Calico/addon runtime images | 标准 OCI image | 原本就是运行镜像。 |
| Helm chart | Helm OCI chart 可保留 | 已验证 `skopeo copy/sync`、`crane copy`、`helm pull` 可用；但 `docker save/load` 依赖 Docker 版本能力，不建议作为唯一承诺。 |
| `k8s/k8s`、`cri/containerd` 安装资源 | 标准 OCI package image | 镜像内保存 `/package/kc-package-manifest.json` 和 `/package/configs.tar.gz`，KC 安装时提取文件；同步/离线搬运按普通镜像处理。 |

## 6. 已采用的 KubeClipper package image 方案

当前自定义 package artifact 不适合直接作为“用户用 skopeo/docker 离线同步”的主路径，因此已改为标准 OCI package image。

新格式：

```text
manifest: application/vnd.oci.image.manifest.v1+json
config:   application/vnd.oci.image.config.v1+json
layer:    application/vnd.oci.image.layer.v1.tar+gzip

/package/kc-package-manifest.json
/package/configs.tar.gz
/package/kubeclipper-agent
```

KC fetcher/indexer 不再按自定义 layer mediaType 查找内容，只从标准镜像 rootfs 的 `/package` 目录读取。

## 7. 当前推荐

结合用户路径：

```text
DockerHub/公开 Registry
  -> OSS 离线镜像包
  -> 用户下载
  -> kcctl registry deploy
  -> 导入本地 Registry
  -> 使用本地 Registry 部署
```

推荐把需要进入离线镜像包的内容尽量统一为标准 OCI image。

第一阶段至少应调整：

1. `kubeclipper-server`、`kubeclipper-agent`、`registry`、`kc-console` 发布为标准 OCI image。
2. 保留 Helm OCI chart，但同步脚本里明确使用 `skopeo sync/copy` 或 `helm pull/push` 验证。
3. `k8s/k8s`、`cri/containerd`、bootstrap binary 统一使用标准 OCI package image。

## 8. 新 package image 回归验证

迁移后在 `sh-dev-3` 重新验证标准 package image：

```text
source registry: 127.0.0.1:5601 / 127.0.0.1:5611
target registry: 127.0.0.1:5602 / 127.0.0.1:5612
ref: 127.0.0.1:5601/kubeclipper/packages/k8s/k8s:v0.0.0-standard
sync ref: 127.0.0.1:5611/kubeclipper/packages/k8s/k8s:v0.0.0-sync
```

验证结果：

| 能力 | 结果 |
| --- | --- |
| `oci-publish --profile k8s` 发布 package image | 通过 |
| manifest/index mediaType | `application/vnd.oci.image.index.v1+json` + `application/vnd.oci.image.manifest.v1+json` |
| `docker pull` | 通过 |
| `docker save` + `docker load` | 通过 |
| `skopeo copy docker://source docker://target` | 通过 |
| `crane copy source target` | 通过 |
| `skopeo sync --src docker --dest docker source-repo target-namespace` | 通过 |
| 从 sync 后的目标 Registry 再 `docker pull` | 通过 |
