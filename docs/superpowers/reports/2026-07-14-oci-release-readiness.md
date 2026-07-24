# 纯 OCI 发布就绪报告（2026-07-24）

> **当前结论：最新候选 `146f5713` 已完成本地实现和验证，但暂不建议直接正式发布。**
> `kcctl registry sync`、2.0.0 发布矩阵、正式 release manifest 资产和官方 GHCR 默认值已经落地；
> 远程认证 TLS Registry 的真实同步 E2E 已通过。按要求本轮没有推送，因此该提交还没有真实
> GitHub Actions、正式 GHCR digest 和生产 Harbor 最小权限证据。下方 `7332bac` 的双机纯 OCI
> 资格结果仍然有效，但不能替代新提交的发布验证。

## 2026-07-24 `kcctl registry sync` 与 2.0.0 发布增量

### 已完成实现

- 功能提交：`d1fe0c7 feat(registry): sync release artifacts from OCI manifest`。
- 发布架构门禁提交：`146f571 fix(release): derive architectures from release manifest`。
- 新命令支持默认下载当前 `kcctl` 版本对应的 GitHub Release manifest 和 `.sha256`，或使用
  `--manifest` 指定本地文件；没有增加 `--version`、`--manifest-sha256` 和源端认证参数。
- 目标 Registry 支持 Basic Auth、密码文件、HTTPS、自定义 CA、skip TLS verify 和显式 HTTP。
- manifest 使用严格 schema，拒绝未知字段、非官方源、非法相对路径、非 SHA256 digest、重复或
  冲突 target，并校验 package 每个平台的 OCI `sourceRevision`。
- `--arch all` 保留 OCI index；`amd64`/`arm64` 写入单平台 manifest；相同 digest 跳过，冲突
  tag 失败且不覆盖。runtime image 会按 source/target 分组，生成阶段对重复引用去重并拒绝歧义。
- 正式 `release.yml` 从 `packaging/resources.yaml` 和 SupportPolicy 生成 16 项发布矩阵，发布完成后
  合并真实 assembly metadata，生成并上传 `release-manifest-v2.0.0.yaml` 及其 SHA256 文件。
- qualification workflow 使用同一 assembly 路径，不再使用缺失 runtime image 的空 `images.lock`；
  standalone bootstrap workflow 不再重复响应 `v*` tag。
- 正式默认值已切换为 `v2.0.0` 和 `ghcr.io/kubeclipper/kubeclipper`，默认 SupportPolicy 同步选择
  `bootstrap/kubeclipper:v2.0.0`。
- `packaging/resources.yaml` 现在明确声明 amd64 和 arm64；发布矩阵的 `architecture` 由该清单生成，
  `release.yml` 不再在清单外硬编码 `all`。verifier 会拒绝缺少任一正式架构、重复架构和不支持的
  架构，避免发布清单、workflow 和正式产物的平台集合漂移。

### 本地验证证据

```text
go test -race ./pkg/delivery/releasemanifest ./pkg/cli/registry \
  ./pkg/cli/deliverypolicy ./pkg/delivery/registry ./pkg/delivery/apis \
  ./tools/release-policy-verify                              PASS
go list ./pkg/... | 排除 pkg/utils/systemctl 后执行 go test -race
                                                               PASS
go list ./... | 排除 pkg/utils/systemctl、test/e2e 后执行 go test
                                                               PASS
go test ./...                                                  环境失败，分类见下文
go vet ./...                                                   PASS
golangci-lint run ./...                                        PASS，0 issues
actionlint .github/workflows/*.yml                              PASS（1.7.7）
ShellCheck 0.11.0（本提交修改的全部 shell 脚本）               PASS
bash scripts/open-packaging/tests/export-offline-registry-bundle-test.sh
                                                               PASS
bash scripts/open-packaging/tests/generate-release-manifest-provenance-test.sh
                                                               PASS
go run ./tools/release-policy-verify --manifest packaging/resources.yaml
                                                               PASS，16 项发布矩阵
go test -race ./tools/release-policy-verify                    PASS
发布矩阵 16 项均由 resources.yaml 生成 architecture=all       PASS
linux/amd64、linux/arm64 的 kcctl/server/agent 六个交叉构建
                                                               PASS，file 架构检查正确
git diff --check                                               PASS
```

进程内真实 OCI Registry 集成测试覆盖：公开匿名源、Basic Auth 目标、HTTPS 自定义 CA、完整
amd64/arm64 index、单架构选择、package provenance、runtime child digest、第二次同步跳过、错误
凭据拒绝和冲突 tag 拒绝。测试 Registry、证书和凭据均随测试进程销毁。

### sh-dev-3 真实 Registry 同步 E2E

在 sh-dev-3 上直接运行 Linux amd64 `kcctl`，不依赖 Docker、containerd、crane 或 skopeo 执行
同步。测试源、目标和 GitHub Release mock 分别只监听 `127.0.0.2:443`、`127.0.0.3:443` 和
`127.0.0.4:443`；`kcctl` 在独立 chroot 中使用测试 hosts 映射，因此没有修改宿主
`/etc/hosts`，也没有触碰现有 `kc-registry.service`。目标端启用 Basic Auth 和独立自签名 CA。

测试 `kcctl` 版本为 `v2.0.0`，Git commit 和 package provenance 均为：

```text
d1fe0c7b794e4b58806cbca2ad921cad16d5afc4
```

默认模式没有传 `--manifest`，成功从下列正式格式 URL 下载 manifest 和 SHA256 文件并在解析前
完成校验：

```text
https://github.com/kubeclipper/kubeclipper/releases/download/v2.0.0/release-manifest-v2.0.0.yaml
manifest sha256: 20b17ab8daf623314c6dec4635172e019e3350e3263e8de1f13110ef1572187f
```

源端和目标端 digest 证据：

```text
package platform sourceRevision:
  amd64 = d1fe0c7b794e4b58806cbca2ad921cad16d5afc4
  arm64 = d1fe0c7b794e4b58806cbca2ad921cad16d5afc4

package index: sha256:6644ad8d60e15d37e4c906c8a30757c5258e9ff4615238fccac120f488614dfa
package amd64: sha256:aebe8ef8d7d981a278a706402b6715f874f03c90f3919d18836da3f3896ede2d
package arm64: sha256:babaa8f37a5c27a1557706ae40065c23e1e9fd46050bc0300ac53f273c8bf350
Helm chart:     sha256:827131eaac575b0caa713eef40df9a986655345d415ded3b38148d3947914e7a
```

验收结果：

1. `--arch all` 首次同步 package、runtime image 和 Helm chart 共 3 个 target，全部为 `copy`；
   package/runtime 保留完整 index，两个 child digest 与源端逐平台一致。
2. 对同一目标再次执行，3 个 target 全部为 `skip`，统计为 `synced 0; skipped 3`。
3. `--arch amd64` 向独立项目同步成功；package/runtime tag digest 均为 amd64 child digest，
   Helm chart digest 不变。
4. 错误密码以 HTTP 401 失败，没有写入目标。
5. 预先写入不同 digest 后，同步返回 `target tag conflict`；冲突前后实际 digest 一致，确认没有覆盖。
6. 停止 GitHub Release mock 后，显式 `--manifest /manifest.yaml` 仍同步成功，证明受限网络下本地
   manifest 路径不依赖 Release 下载。

测试完成后删除了所有测试项目/tag、Registry 数据、CA、证书、密码、crane 临时认证文件、chroot、
日志和本机/远端 `/tmp/kc-sync-e2e-d1fe0c7`。三个隔离监听端口均为 0；sh-dev-3 原有
`kc-registry.service` 和 Docker 保持 active，system containerd 保持 inactive。sh-dev-2 的
kc-agent、kubelet、containerd 均保持 inactive；残留审计发现并删除了 2026-07-09 旧资格测试的
`oci-current` kubeconfig，随后 KubeClipper/Kubernetes 测试路径复查全部为空。

全仓测试的环境失败与原报告一致，不是本轮回归：macOS 没有 system D-Bus，`test/e2e` 在已清理
qualification 环境中没有 `~/.kc/config` 和运行中的平台。修改过的 shell 脚本均通过 ShellCheck、
`bash -n` 和对应 open-packaging 测试。

### 当前发布门禁

- Actions run：无。本轮遵守“不推送”要求，没有为最新候选 `146f5713` 触发远端工作流。
- OCI digest / sourceRevision：尚无正式 v2.0.0 产物；必须由该提交发布后记录，不能复用
  `7332bac` qualification digest。
- 双机生命周期：已有 `7332bac` 的完整通过证据；`d1fe0c7` 和 `146f571` 未改变 server/agent 生命周期逻辑，
  但仍需对最终 release commit 重跑必需 Actions。认证 TLS Registry sync 已在 sh-dev-3 通过，
  正式发布时仍需用生产 Harbor robot account 和最小权限项目确认厂商权限策略。
- 清理：本轮远程隔离 Registry、tag、证书、密码、chroot 和日志已全部删除，没有创建 GHCR package、
  SSH 授权、隧道或双机平台资源；两台测试机的相关残留复查通过。

剩余分级：**P0** 为当前提交尚未完成真实 Actions、正式 manifest/digest/provenance，以及生产
Harbor robot account/最小权限策略验收；**P1 无**；**P2 无**。
在获得推送授权并关闭这些 P0 前，结论为：**暂不建议直接正式发布 2.0.0**。

## 2026-07-24 最终发布候选复审

- 分支：`codex/oci-static-server-replacement`。
- 发布代码 revision：`7332bac5a34cb0379cc74680b6e44b33beea59be`。
- 关键修复提交：
  - `7fb130c`：在修改 CRI 运行时前预取 OCI artifact。
  - `7332bac`：resolver 缓存身份不再包含临时 `Slot`，重建 component 后仍复用预取结果。
- 分支只推送到用户 fork `lixd/kubeclipper`；未推 upstream、未强推或覆盖远端历史。
- qualification Git 标签 `oci-qualification-7332bac-20260724` 已在验证结束后从 fork 和本地删除。
- `packageRegistry` 只负责 KubeClipper OCI package/Helm Chart；`--image-registry` 选择
  Kubernetes/CNI 镜像来源；`--cri-registry` 选择其他业务镜像 Registry。后两类 Registry
  都会写入 containerd，API、kcctl 和前端模型保持一致。

## 发布阻断项关闭情况

| 阻断项 | 结论和证据 |
|---|---|
| `kcctl clean` 未覆盖 join agent | 已关闭；一次 `kcctl -y clean -A -f` 同时清理 server 和 join agent，双机残留审计通过。 |
| 目标节点架构检测缺少可注入 runner 测试 | 已关闭；SSH runner 可注入，amd64、arm64 和混合架构单元测试通过。 |
| join 共用 server/agent SSH 配置 | 已关闭；双机使用独立 server SSH key 和 agent SSH key 完成 join。 |
| 多网卡可能选择 Docker/CNI bridge | 已关闭；默认地址发现排除 Docker、CNI、Podman、nerdctl bridge，实机选择 `ens3`。 |
| 发布产物与 support policy 可能漂移 | 已关闭；`tools/release-policy-verify` 对 manifest 和 support policy 做双向一致性校验。 |
| GitHub Actions Node.js 20 弃用警告 | 已关闭；相关 Actions 已升级，当前 workflow 运行无该阻断。 |
| Registry 认证/TLS/凭据下发 | 已关闭；认证 HTTPS Harbor、robot account、自定义 CA、最小项目权限和读写路径均实测通过。 |
| CRI 变更后无法再从 Registry 拉取 package | 已关闭；create/install/upgrade/add-node 在运行时变更前预取，缓存按 artifact 身份跨 resolver slot 复用。 |
| 单 master 默认无法调度普通工作负载 | 已关闭；API 在单 master 且无 worker、用户未显式设置时持久化 `untaintMaster: true`。 |

## 本地代码和构建验证

以下验证对应 release commit `7332bac`：

```text
go list ./pkg/... | rg -v 'pkg/utils/systemctl$' | xargs go test -race
                                                              PASS
go vet ./...                                                  PASS
golangci-lint run ./...                                       PASS（0 issues）
go test ./...                                                 仅环境依赖失败，见下文
git ls-files '*.sh' | xargs -n1 bash -n                       PASS
bash scripts/open-packaging/tests/export-offline-registry-bundle-test.sh
                                                              PASS
bash scripts/open-packaging/tests/generate-release-manifest-provenance-test.sh
                                                              PASS
go run ./tools/release-policy-verify --manifest packaging/resources.yaml
                                                              PASS
actionlint 1.7.7                                              PASS
git diff --name-only master...HEAD -- '*.sh' | xargs shellcheck -x -P SCRIPTDIR
                                                              PASS
git diff --check                                              PASS
```

全仓 `go test ./...` 的失败已复现并按环境分类，不是产品回归：

- `pkg/utils/systemctl`：本机为 macOS，没有 system D-Bus。
- `test/e2e`：本机在 qualification 清理后没有 `~/.kc/config` 和正在运行的平台，17 个 setup 失败。
- 排除上述明确依赖宿主环境的包后，其余 Go 包及 `./pkg/...` race 测试全部通过。

Linux 交叉构建完成：kcctl、kubeclipper-server、kubeclipper-agent 分别构建 linux/amd64 和
linux/arm64，共六个产物；`file` 确认均为对应架构的 statically linked ELF。构建临时目录已删除。

## 当前 release commit 的 GitHub Actions

| 必需工作流 | Run | 结果 |
|---|---|---|
| Go tests/coverage | [30075941260](https://github.com/lixd/kubeclipper/actions/runs/30075941260) | success |
| offline-resource-validate | [30075941258](https://github.com/lixd/kubeclipper/actions/runs/30075941258) | success |
| OCI AIO deploy + Fast E2E | [30075941272](https://github.com/lixd/kubeclipper/actions/runs/30075941272) | success；7 passed，0 failed；web terminal SSH 通过；最终 clean successful |
| 16 组件发布 + release manifest/provenance | [30075941457](https://github.com/lixd/kubeclipper/actions/runs/30075941457) | success |

release manifest job ID 为 `89429042238`：

```text
release manifest sha256: c3a773bb77422e9004b76d87bdbf10cbd900d326980b8559acbd0a53945b5253
sourceRevision:             7332bac5a34cb0379cc74680b6e44b33beea59be
```

关键发布 digest：

```text
bootstrap/kubeclipper:v1.8.0  sha256:18c184bcffe9e3ec7d1a83183b876dcc9fff0459e1b6d4ad3f5c6b6a5f9e92bb
bootstrap/etcd:3.5.21          sha256:e274ae3a81df8bf88dee5083dd074554b9b199ef93c8c77f6f2f713841a1d0b9
bootstrap/console:v1.6.0       sha256:5c38d1aa11d0f10d04fb1263db385f33cbd0d3c694a181a57ef5c959522c81a2
bootstrap/registry:3.1.1       sha256:1a0f2bef6491085184a9e7d8fbcfd07bc548e607d73dc29f7005c5d68daacd69
cri/containerd:1.7.29          sha256:1e454cb4e46bbf4a60ce4f00e3d335c90d7393130127f6feaa6dd31533499345
cri/containerd:2.2.4           sha256:6f750ebe2a8f26a995caf2ee818d33dc7c22f7113ab5d45540ef7eb8e54c119e
k8s:v1.34.2                    sha256:f898b3e98eb5900729be01fe06fd65308a811f8497d01de312d7587ff36d36e8
k8s:v1.35.0                    sha256:15e343aa81f227613185a846a4591e9100fafdf83e66ce3309c13b8547423e69
k8s:v1.36.1                    sha256:5b1b1b849fdc3656cc9f20b959a5502b041b7924a1f42116c1f4ac41d0a0993e
k8s-extension:v1               sha256:e621bc3bbac015b15db6d796597e8fa7e98f5da3bf6336288df037b8626d5f48
tigera chart v3.29.6           sha256:e70d51dd2ff6d0d2a8013a112fe1f13faddd186b9582f11fe8375e86b088610f
tigera chart v3.31.5           sha256:9b5aa9e439479fc17633b859136871c18d836e5277c6ac4b3b5473628a27af18
```

## 认证 Harbor 和 provenance

本轮在 sh-dev-3 使用隔离 HTTPS Harbor，采用 robot account、自定义 CA 和隔离项目权限完成
发布及读取；错误凭据、未知 CA 和越权项目均不会被当作有效来源。bootstrap/kubeclipper 的
本地多架构 index 证据为：

```text
index: sha256:fdd2b3b4cd9d73204d95595be026cc24cd9490baa2d5ec3e6d3d054e8f093232
amd64: sha256:0ba6d4f62ffc76b53c5b940a2828e82e5ccae40809a8d48f612dd31a95d226f0
arm64: sha256:ccbda4374d1ea9df095170b8ff4dc7021fcd4a39aa4bef43be5b5e505611e178
```

两个平台的 package metadata `sourceRevision` 均为 `7332bac5a34cb0379cc74680b6e44b33beea59be`。
Registry 配置和凭据以 `0600` 下发到 server/agent，clean 后已删除。

## 双机纯 OCI 生命周期

测试主机：sh-dev-3（`172.16.131.146`）和 sh-dev-2（`172.16.131.208`）；集群
`oci-qualification-7332bac`；Kubernetes `v1.34.2`、containerd `1.7.29`、Calico `v3.29.6`。

1. sh-dev-3 使用 release commit 的 Linux amd64 产物完成 AIO deploy，登录和版本一致性通过。
2. 使用独立 server SSH key 和 agent SSH key 将 sh-dev-2 join 到平台。
3. 未传 `--untaint-master` 创建单 master 集群，API 自动持久化 `untaintMaster: true`；master
   taint 为空。创建操作 `0c9a4259-8d4f-4e8c-994f-29f3a44fd6d0` 成功。
4. add-node 操作 `aca8c0e4-0b8d-47c1-97b8-015fa1a05ac0`：
   `prefetchOCIArtifacts` 成功后主动停止隔离 Harbor，确认 5443 不可达；随后
   `installRuntime`、`installExtension`、`installPackages`、`joinNode` 仍全部成功，证明运行时
   变更后没有隐式回源依赖。
5. 两节点均 Ready；`worker-check` Pod 明确调度到 sh-dev-2（节点 `lixd-dev-2`），日志为
   `worker-check-ok`。
6. remove-node 操作 `c21391a4-57d1-4add-9873-b68427bd0468` 的 14 个清理步骤全部成功。
7. 删除集群操作 `97fb153f-2b81-4c7b-bbc9-7f0ba826c354` 成功。
8. 一次执行 `kcctl -y clean -A -f` 返回 `clean successful`，server 和所有通过 join 加入的
   agent 同时被完整清理，无人工补救。

## 真实离线 bundle 往返

从 GHCR qualification 的 digest-pinned bootstrap/console 工件执行：导出 amd64 bundle、
校验 bundle 及内部所有文件 SHA256、导入临时 Registry、校验导入 manifest digest 和
`sourceRevision`。结果：

```text
源多架构 index digest:  sha256:5c38d1aa11d0f10d04fb1263db385f33cbd0d3c694a181a57ef5c959522c81a2
bundle amd64 digest:     sha256:d8b4a2f858386cd47bcdee91d03304c91b0f9973f1b7ad040e6ba82beb8e1a6b
导入后 Registry digest: sha256:d8b4a2f858386cd47bcdee91d03304c91b0f9973f1b7ad040e6ba82beb8e1a6b
sourceRevision:          7332bac5a34cb0379cc74680b6e44b33beea59be
结果:                    OFFLINE_ROUNDTRIP_VERIFIED
```

多架构 index 按 `--arch amd64` 导出后，release manifest 正确改写为 amd64 平台 manifest
digest；`bundle-artifacts.tsv` 同时保留源 index digest，避免把平台选择误判为 digest 漂移。

## 清理结果

- 两台机器的 kc-server、kc-agent、kubelet、containerd 均为 inactive。
- 两台机器均无 Kubernetes、etcd、kubelet、containerd、CNI、Calico 和 KubeClipper 配置/数据路径，
  无 `cali*`、`vxlan.calico`、`kube-ipvs0` 网络设备。
- 本轮及历史 qualification 测试公钥、临时私钥、CA、密码、授权、隧道、unit、Registry 容器、
  bundle 和临时文件均已删除。
- sh-dev-2 的 `/root/.kc` 时间为 2026-07-08/09，早于本轮测试，作为用户既有内容保留。
- 两机已有 `/usr/local/bin/kcctl` 时间早于本轮 qualification，不属于本轮运行时或集群残留，未擅自删除。
- sh-dev-3 的 `kc-registry.service` 和 Docker 保持 active；`sprout-postgres-v2` 保持 running，
  `StartedAt` 仍为 `2026-07-14T03:17:33.452118082Z`，未触碰无关服务。
- qualification Git 标签已从 fork 和本地删除；本地工作区测试产物和交叉构建目录已删除。
- GHCR 中完整匹配 `qualification-7332bac5a34cb0379cc74680b6e44b33beea59be` 命名空间的
  36 个 container packages 已通过 GitHub Packages API 删除；再次分页查询结果为 0，抽查
  bootstrap/console package 返回 HTTP 404 `Package not found`。清理时临时增加的
  `read:packages`、`delete:packages` scopes 已撤销，`gh` token 恢复为原始权限集合。

## P0/P1/P2 与发布建议

- **P0：无。** 所有发布阻断逻辑、当前提交 Actions、认证 Harbor、双机生命周期、离线交付和主机清理均有直接证据。
- **P1：无。** 未发现需要在 2.0.0 发布前修复的功能或可靠性缺口。
- **P2：无。** qualification Git/OCI 标签、packages、临时认证材料、Registry 服务和主机文件均已清理。

最终建议：**可以正式发布 2.0.0。** 正式发布必须从代码 revision
`7332bac5a34cb0379cc74680b6e44b33beea59be` 构建，并保持 release manifest、OCI digest 和
`sourceRevision` 校验；不要把后续仅更新报告的文档提交误当成新的二进制 sourceRevision。

## 历史复审（2026-07-23 及更早）

- 分支：`codex/oci-static-server-replacement`。
- 双机资格代码提交：`2224c551862b0c087fe8ce71533568581c6f383a`；当前 HEAD 为
  `b1991d0`，在该提交之后增加了单节点默认取消 control-plane taint 的 CLI 行为。
  因此下方双机 OCI digest 和生命周期证据严格对应 `2224c55`，不能替代当前 HEAD
  重新生成工件后的资格验证。
- 关键提交：`8262a7d`（认证 package Registry）、`30c7a69`（运行时/CNI 清理）、
  `fc97039`（控制面 finalizer）、`2224c55`（CRI 卸载后执行控制面 finalizer）。
- 工作区在验证结束时 clean；没有推送、强推或覆盖已有 stash。

### 已关闭阻断项

1. `kcctl clean` 追踪并清理所有通过 join 加入的 agent，删除 server/agent 的凭据和 CA。
2. 架构检测支持可注入 SSH runner，覆盖 amd64、arm64 和混合架构测试。
3. join 的 agent SSH 与读取 server 证书的 server SSH 独立；双机使用两把不同密钥验证。
4. 多网卡 `first-found` 排除 Docker/CNI/Podman/nerdctl bridge；实机使用 `ens3`。
5. `tools/release-policy-verify` 对发布产物和 support policy 做双向一致性校验。
6. GitHub Actions 已使用升级后的 action 版本，消除 Node.js 20 runtime 弃用配置。
7. package/image Registry 模型已分离：`packageRegistry` 只负责 OCI artifact；
   `--image-registry` 选择 Kubernetes/CNI 镜像来源；`--cri-registry` 选择写入
   containerd 的其他镜像来源；两类镜像 Registry 都写入 containerd 配置。
8. 单 master 且无 worker 的集群会默认取消 control-plane taint，使单节点集群能够
   调度普通工作负载；单 master 带 worker、多 master 拓扑仍保留 taint，显式
   `--untaint-master` 保持兼容。该行为由 `b1991d0` 实现并通过 CLI race 单元测试。

### 认证 TLS Registry 与 provenance

隔离 package Registry 为 `https://172.16.131.146:5001`（Basic Auth 用户 `kcrobot`、自签名
CA），隔离 image Registry 为 `https://172.16.131.146:5002`（用户 `kcimage`、独立 CA）。
deploy/join 使用 CA、用户名和密码文件，server/agent package 配置权限实测为 `0600`；
错误凭据和未知 CA 会被拒绝。image Registry 资源 `qualification-2224c55-images` 的
scheme 为 `https`、CA/auth 均存在、skip TLS verify 为 false；Kubernetes、Calico 和
`kubeclipper/kubectl` 均实际由 containerd 从该 Registry 拉取。

```text
sourceRevision: 2224c551862b0c087fe8ce71533568581c6f383a
ref: 172.16.131.146:5001/kubeclipper/packages/bootstrap/kubeclipper:v2.0.0-qualification-2224c55
digest: sha256:50b6ae687067d9a2b2b81df486e3926a7dce4aff3c19b2ba6a762a67f2dac5f0
kubeclipper-agent: sha256:01b1228c3d052634a984efd19a48a34e07fbb701d1a61fefe6998faf921b84d2
kubeclipper-server: sha256:e36725bb57c17f158ea96132907b74c01d9398941e62f54463e5a021dff7c80b
```

Registry 测试服务、标签和临时文件已在验证后删除；以上 digest 是发布时记录的不可变证据。

### 双机纯 OCI 生命周期

测试主机为 sh-dev-3（`172.16.131.146`）和 sh-dev-2（`172.16.131.208`），测试对象均使用
`qualification-2224c55` 前缀。

1. 使用当前 commit 的 Linux amd64 kcctl/server/agent，从认证 HTTPS package Registry 完成 deploy；server/agent commit 和 sourceRevision 一致且 tree clean。
2. 用独立 server/agent SSH 配置完成 sh-dev-2 join；package 配置权限为 `0600`。
3. 使用 Kubernetes `v1.35.0`、containerd `1.7.29`、Calico `v3.29.6` 建立纯 OCI 集群；image Registry 和 cri Registry 均选用 `qualification-2224c55-images`，集群和系统 Pod 达到 Running/Ready。
4. `kcctl cluster add-node` 后 sh-dev-2 Ready；containerd 成功拉取认证 image Registry 中的 Calico 和 `kc-kubectl` 镜像。
5. `kcctl cluster remove-node` 后 sh-dev-2 的 kubelet/containerd inactive、kc-agent 保留；Kubernetes、containerd、CNI、Calico 配置和数据路径均不存在，且无 `cali*`/`vxlan.calico` 设备。
6. 删除集群后 sh-dev-3 的 kubelet/containerd inactive，Kubernetes、containerd、CNI、Calico、etcd 路径全部不存在；`2224c55` 的 finalizer 已在 CRI 卸载之后执行，未再出现 image-volume `Device or resource busy`。
7. 执行 `kcctl clean --all --force --assumeyes` 后两台机器的 kc-server、kc-agent、kc-etcd、kc-console、kubelet、containerd 均 inactive，平台配置和测试凭据均不存在。

### 清理与保留项

- 两套隔离 Registry systemd 服务已停止、禁用并删除临时目录；两台机器 authorized_keys 中的测试公钥、临时 CA、密码、私钥、二进制和归档均已删除。
- `kc-registry.service` 保持 active；`sprout-postgres-v2` 保持运行；未触碰无关服务或容器。

### 本地验证与剩余缺口

通过：定向 race 测试、`go vet ./...`、`golangci-lint run ./...`（0 issues）、两个 open-packaging 测试、open-packaging source check、Bash 语法检查、`go run ./tools/release-policy-verify --manifest packaging/resources.yaml`、Actionlint、Linux amd64/arm64 静态构建和 `git diff --check`。

全仓 `go test ./...` 的环境相关失败已单独区分：macOS 无 system D-Bus 导致 `pkg/utils/systemctl` 失败；本机无 `~/.kc/config` 和已部署平台，`test/e2e` 的 17 个 setup 失败。排除这两个明确环境依赖包后，其余 `go list ./... | rg -v 'pkg/utils/systemctl|test/e2e' | xargs go test` 全部通过。ShellCheck 因当前 macOS 预发布环境没有可用 bottle，源码构建其依赖 `pkg-config` 时又遇到旧 GLib/Clang 编译错误，未记为通过。

资格代码提交及其后的报告提交都没有 GitHub Actions run 链接，因为本轮明确不推送远端；Go tests/coverage、offline-resource-validate、bootstrap、resource package、release manifest/provenance 和 OCI AIO 的当前分支证据仍缺。正式 Harbor 的生产 robot account、最小权限项目、正式 CA/TLS 组合也尚未重跑。

单节点默认自动取消 taint 的代码缺口已关闭；但本轮双机资格工件来自 `2224c55`，
建群命令也显式使用了 `--untaint-master`。正式发布前仍需基于当前 HEAD 重新生成带
`sourceRevision` 的 OCI 工件，并在不传 `--untaint-master` 的情况下验证单节点系统 Pod
和普通工作负载均能正常调度。剩余 P2：测试主机未运行 chronyd/ntpd（时钟偏差实测
小于一秒），以及 Registry blob 垃圾回收由运维方负责。

### 发布建议

本地代码、认证 TLS package/image Registry 分离、双机 deploy/join/create/add/remove/delete/clean 和残留清理均已通过；在没有当前 release commit 的 Actions 与正式 Harbor 权限策略证据前，不能宣称正式发布就绪。建议授权推送后在该 commit 上完成所有 Actions，并用正式 Harbor 重跑同一套双机证据，再决定发布 2.0.0。

## 历史记录（2026-07-22 及更早）

以下内容保留旧提交的审查和 Actions 证据，不代表当前 `2224c55` 的发布状态。

### 2026-07-22 复审

### 最新 master Registry 模型已完成集成

- 当前已提交基线：`f248da4a8334a0c1238d8207c0682b83162e2f0f`。
- 合入的 master 提交：`8371495`，包含 Registry API、kcctl 和前端的统一调整。
- `--image-registry <Registry resource>` 选择 Kubernetes 控制面/CNI 镜像来源；
  `--cri-registry <Registry resource>[,...]` 配置其他 containerd 镜像来源。
- `deployConfig.packageRegistry` 仅解析 KubeClipper OCI package 和 Helm Chart。
  即使两类 Registry 位于同一个 Harbor，它们仍按独立资源、路径和凭据处理。
- 旧的 `--local-registry` 和镜像侧 `--insecure-registry` 已删除，不再参与新模型。

### package Registry 认证与 TLS 本地实现

当前未提交实现提供统一 Registry 客户端配置，覆盖 indexer、fetcher、运行时 Helm
OCI Chart、发布工具、验证工具以及 `kcctl resource`：

1. 默认严格 HTTPS；HTTP 必须通过 `--package-registry-scheme http` 显式启用。
2. 支持用户名/密码、Harbor robot account、自定义 CA 和显式跳过 TLS 校验。
3. 密码只能通过文件输入，不提供明文密码参数；本地及远端配置权限为 `0600`。
4. deploy 在服务启动前向全部 server/agent 安全下发配置；配置不会进入 deploy
   ConfigMap、delivery plan 或普通日志。
5. join 默认通过独立的 server SSH 继承现有凭据，再通过 agent SSH 下发给新节点。
6. 显式切换 Registry 或轮换认证/TLS 时，会更新全部现有 server/agent；任一节点
   更新失败或最终 deploy ConfigMap 提交失败，均反向恢复已经修改的节点。
7. clean 删除 `/etc/kubeclipper-server` 和 `/etc/kubeclipper-agent`，同时删除其中的
   Registry 凭据与 CA。
8. Registry 配置绑定完整 host/project prefix，拒绝向其他 Registry 或项目发送凭据。

本地集成测试使用带 Basic Auth 的自签名 TLS Registry 验证了正确 robot 凭据与
自定义 CA、错误凭据拒绝、未知 CA 的严格 TLS 拒绝，以及显式 HTTP；还完成了
amd64/arm64 manifest index 合并和 Helm OCI Chart 按 digest 发布/拉取往返。

### 2026-07-22 本地验证结果

通过：

```text
go test -race ./pkg/delivery/registry ./pkg/delivery/indexer \
  ./pkg/delivery/fetcher ./pkg/delivery/publisher ./pkg/component/common \
  ./pkg/cli/deploy ./pkg/cli/join ./pkg/cli/clean ./pkg/cli/resource \
  ./pkg/utils/sshutils ./pkg/apis/core/v1 ./pkg/clustermanage/kubeadm
go vet ./...
golangci-lint run ./...                         # 0 issues
bash scripts/open-packaging/tests/generate-release-manifest-provenance-test.sh
bash scripts/open-packaging/tests/export-offline-registry-bundle-test.sh
go run ./tools/release-policy-verify
bash -n scripts/open-packaging/*.sh hack/*.sh
git diff --check
GOOS=linux GOARCH=amd64/arm64 CGO_ENABLED=0 go build：
  kcctl、kubeclipper-server、kubeclipper-agent
```

6 个交叉编译产物均经 `file` 确认为静态 Linux ELF；amd64 为 x86-64，arm64 为
aarch64。Go 在构建结束时尝试写入只读的全局 module stat cache，产生非致命警告，
不影响构建退出状态或产物格式。

`go test ./...` 的普通 Go 包全部通过，命令整体仅在以下环境依赖处失败：

- `pkg/utils/systemctl`：macOS 没有 systemd/dbus；
- `pkg/utils/sysutil`：桌面沙箱拒绝读取宿主系统信息；
- `test/e2e`：本机没有已部署平台及 `~/.kc/config`。

本机没有 `actionlint`、`shellcheck`、`skopeo`、Docker 和 `yq`。因此本轮不能把
Actionlint、ShellCheck 或依赖 Skopeo 的真实导入验证记为本地通过；必须由最终提交的
Linux Actions 和后续 Harbor 资格测试补齐。

### 当前证据边界

- 下文 `cf2f967` 的 Actions、digest、sourceRevision 和双机生命周期结果属于历史
  匿名 HTTP 资格测试，不能作为当前实现的最终发布证据。
- 当前未推送，尚无当前最终提交的 Actions run 链接。
- 当前实现尚未发布到隔离 Harbor 标签，因此没有可记录的最终 OCI digest 和
  sourceRevision 对。
- 当前尚未在 `sh-dev-3`、`sh-dev-2` 使用 Harbor robot account、自定义 CA 和严格
  TLS 重跑 deploy、join、建群、增删节点、删除、clean 以及残留审计。

### 历史发布候选版本

- 分支：`codex/oci-static-server-replacement`
- 发布提交：`cf2f967e6ddbf2b81354096b88216c44d253021c`
- 验证标签：`oci-qualification-20260715-cf2f967`
- 测试主机：`sh-dev-3`（`172.16.131.146`）和 `sh-dev-2`（`172.16.131.208`）
- 软件包及运行时镜像 Registry：`sh-dev-3:5000` 上由宿主机原生运行的 Distribution Registry
- Kubernetes 技术栈：Kubernetes `v1.35.0`、containerd `1.7.29`、Calico `v3.29.6`

发布候选版本在干净的 detached worktree 中构建。`kcctl`、`kubeclipper-server`
及 OCI bootstrap 软件包均包含准确一致的发布提交信息。

这次资格测试证明了匿名 HTTP Registry 下纯 OCI 发布、部署和双机生命周期流程的
可行性，不代表私有 Harbor、Registry 认证、自定义 CA 或严格 TLS 校验已经通过
生产发布验证。

### 2026-07-16 发布复审

> 本节保留当时的审查背景和 `cf2f967` 历史证据。关于 package/image Registry
> 边界、`--local-registry` 参数和 package Registry 认证/TLS 的结论，均已由
> 2026-07-22 的“最新复审”更新；请勿将本节中的“尚未实现”表述视为当前状态。

### 最终发布提交尚未形成

- 本次复审的分支基线为 `6485480c75ced9bf212fa460556fceed32f8db2b`，相对
  `cf2f967` 仅新增了上一版资格报告提交。
- 当前 OCI 分支相对最新本地 `master` 少 5 个提交、多 39 个提交。尚未合入的
  master 变更包括 Actions 运行时升级和 OSS 下载地址修复。
- `localRegistry`/`insecureRegistry` 参数及 API 模型调整正在独立 PR 中处理；该
  变更完成后仍需集成到 OCI 分支。
- 本文列出的 GitHub Actions 和双机生命周期结果对应 `cf2f967`，不能替代未来
  最终 release commit 的重新验证。

### package Registry 与 image Registry 仍存在混用

当前 `pkg/apis/core/v1/delivery_source.go` 会优先使用集群的 `LocalRegistry` 作为
OCI package inventory 来源。这会在 image Registry 和 package Registry 使用不同
地址时，从 Kubernetes 镜像仓库错误查找 kubeadm、containerd、Chart 等 OCI package。

复审期间已形成一个本地修复并通过以下 race 测试：

```text
go test -race ./pkg/apis/core/v1 ./pkg/apis/config/v1
```

该修复将 package inventory 来源限定为 `deployConfig.packageRegistry`，并增加了
image Registry 与 package Registry 地址不同的回归覆盖。为避免与正在开发的
Registry API PR 产生不必要冲突，源码修改已单独保存，未包含在本次文档提交中；
待该 PR 完成后统一整合和复测。

### 生产级 package Registry 认证与 TLS 尚未闭环

当前 package inventory 和 OCI package 拉取路径仍普遍硬编码使用
`crane.Insecure`，现有 qualification 没有覆盖私有 Harbor。正式发布前至少需要：

1. kcctl 支持 package Registry 的用户名、密码/令牌、自定义 CA 和 TLS 策略。
2. deploy 将凭据安全下发给 server 和 AIO agent，join 将凭据下发给新 agent。
3. server 能认证读取 package inventory，agent 能认证拉取 digest-pinned package。
4. 凭据不能进入普通 ConfigMap、日志、resolved delivery plan 或命令行明文记录。
5. `kcctl clean` 必须删除两台机器上的测试凭据、CA 和 Registry 配置。
6. 使用 Harbor robot account 和最小权限策略验证 bootstrap、建群、增删节点、
   删除集群和 clean 全生命周期。

### 历史已关闭阻断项

1. join 节点的 agent 清理
   - 通过 API 执行 `kcctl clean` 时，会合并在线节点清单。
   - join 现在会在安装 agent 前持久化计划清理的节点清单，因此客户端即使中断，也不会留下未被追踪的 agent。
   - join 使用的 SSH 传输配置与 server 的 SSH 传输配置分别持久化。AIO agent 使用 server SSH，join 加入的 agent 使用 agent SSH。
   - 当存在可复用的密钥路径时，不会持久化缓存的私钥内容。
   - 远程清理失败会作为错误返回，不再输出误导性的成功信息。
2. 目标节点架构
   - 通过可注入的 SSH runner 分别检测每个目标节点的 bootstrap 架构。
   - 单元测试覆盖 amd64、arm64 以及混合架构节点组。
3. join 传输配置隔离
   - `serverSSH` 和 `--server-ssh-*` 独立于 agent 的 `ssh`/`--pk-file`。
   - 双机测试使用了两把不同的密钥：server 密钥仅授权给 `sh-dev-3`，agent 密钥仅授权给 `sh-dev-2`。
4. 多网卡安全性
   - `first-found` 会排除 Docker、CNI、Podman 和 nerdctl 的 bridge 接口模式。
   - 发布验证中，管理地址和节点地址均显式使用 `interface=ens3`。
5. 发布产物与支持策略一致性
   - `tools/release-policy-verify` 执行双向校验：支持策略中声明的每个默认版本，都必须为受支持架构发布；每个已发布的 release 组件，也都必须存在于支持策略中。
6. CI 运行环境弃用问题
   - 工作流使用 `actions/checkout@v6`、`actions/setup-go@v6`、`azure/setup-helm@v5` 和 `docker/login-action@v4`。
7. OCI 来源追溯
   - 软件包 config label 和 release manifest 条目均包含 `sourceRevision`。
   - manifest 生成流程会在提升产物前校验平台 bootstrap 的 revision。

### 历史本地验证

在发布分支上，以下验证均已通过：

```text
go test -race（clean、join、deploy、create、delivery API/publisher、
              autodetection、release-policy verifier）
go vet ./...
golangci-lint run ./...
actionlint
shellcheck（本次修改涉及的 release/open-packaging 脚本）
scripts/open-packaging/tests/*.sh
bash -n（release/open-packaging 脚本）
tools/release-policy-verify
Linux amd64 和 arm64 构建：kcctl、kubeclipper-server、kubeclipper-agent
release manifest/digest/sourceRevision 校验
离线 Registry bundle 导出/导入校验
git diff --check
```

本地还执行了全仓 `go test ./...`。仅有的本地失败均由宿主环境所致，且已有成功的 Linux Action 作为权威验证结果：

- macOS 不提供 `pkg/utils/systemctl` 所需的 systemd/dbus；
- 桌面沙箱阻止了 `pkg/utils/sysutil` 使用的宿主机信息调用；
- 本地 E2E 需要可用的 `~/.kc/config`。

### 历史发布提交对应的 GitHub Actions

- Go 测试/覆盖率：[run 29388107973](https://github.com/lixd/kubeclipper/actions/runs/29388107973) — 成功
- 离线资源/来源追溯校验：[run 29388107992](https://github.com/lixd/kubeclipper/actions/runs/29388107992) — 成功
- Registry 原生 AIO 部署及快速 E2E：[run 29388107991](https://github.com/lixd/kubeclipper/actions/runs/29388107991) — 成功
- 全部 16 个组件发布及 release manifest：[run 29388108045](https://github.com/lixd/kubeclipper/actions/runs/29388108045) — 成功（16 个发布 job 及 manifest job 全部通过）

AIO 工作流将 bootstrap kubeclipper/etcd/console 发布到本地 Registry，
从 OCI 完成部署，验证登录及快速 E2E（包括 Web Terminal SSH），并要求
`kcctl clean` 执行成功。

manifest job 校验了 12 个发布产物，失败数为零。验证证据如下：

```text
release-manifest.yaml sha256: d051ca7c362d9490deb2efe1cdc901df2fccd7583f4c247cb05ef63fbd10ae2e
metadata.sourceRevision: cf2f967e6ddbf2b81354096b88216c44d253021c
bootstrap/kubeclipper OCI digest: sha256:39bdc402f35dd2ac440556f11fa83fb5ca3f650e6cda45002d162fe8a2b9e7d1
已验证产物：12；失败：0
```

### 历史 OCI 证据

隔离的双机 bootstrap 产物如下：

```text
ref: 172.16.131.146:5000/kubeclipper/packages/bootstrap/kubeclipper:e2e-cf2f967-20260715
index digest: sha256:6be035f206ae36fa645c5850620ce86dd8a9bde199c84759d49f35c1ffc55315
sourceRevision: cf2f967e6ddbf2b81354096b88216c44d253021c
```

验证完成后已删除该标签；宿主机 Registry 中仅保留原有的 `v1.8.0` 标签。

### 历史双机纯 OCI 生命周期验证

1. 使用 OCI 在 `sh-dev-3` 部署平台，并显式指定 `ens3` 进行地址检测。
2. 登录成功。客户端与服务端报告的 Git commit 均为 `cf2f967e6ddbf2b81354096b88216c44d253021c`，且工作树状态均为 clean。
3. 使用相互独立的 server/agent SSH 密钥，从 OCI bootstrap 软件包将 `sh-dev-2` 加入平台。两个平台节点均达到 Ready 状态。部署配置中未出现私钥正文。
4. 仅使用 Registry 中的软件包/镜像创建 `e2e-cf2f967`：Kubernetes `v1.35.0`、containerd `1.7.29`、Calico `v3.29.6`。集群达到 `Running` 状态；所有 control-plane 和系统 Pod 均达到 Running/Ready 状态。
5. 通过 `cluster add-node` 添加 `sh-dev-2`。两个 Kubernetes 节点均为 Ready，并报告 containerd 版本为 `1.7.29`。
6. 通过 `cluster remove-node` 移除 `sh-dev-2`。集群恢复为仅包含 `sh-dev-3` 的 Running 状态；`sh-dev-2` 上的 kubelet/containerd 均处于 inactive 状态。
7. 通过正常 API 流程完成集群删除。
8. 一次执行 `kcctl clean --all --force --assumeyes` 即成功完成清理，同时清理了 AIO server/agent 和 join 加入的 agent，无需人工补救。

首次发布验证有意暴露并随后修复了两个清理边界问题（join 中断时的节点清单，以及 AIO server/agent 的传输配置选择）。上述完整生命周期是基于当时的资格测试提交执行的干净重测结果。由于最终 2.0 release commit 尚未形成，合并 master、Registry API 和认证/TLS 改造后必须重新执行。

### 历史最终清理证据

最终 clean 成功后，两台主机上的下列项目均已不存在或处于 inactive 状态：`kc-server`、`kc-agent`、`kc-etcd`、`kc-console`、`kubelet`、Kubernetes static-pod 进程、KubeClipper server/agent 配置目录和二进制文件、`/etc/kubernetes`、`/var/lib/kubelet`、`/var/lib/etcd`、`/etc/containerd` 以及 `/var/lib/containerd`。

临时 SSH 授权和密钥、Registry 标签、发布验证 Git 标签、源码副本、二进制文件及软件包归档均已删除。所有 216 个带临时 `qualification-*` 前缀的 GHCR 软件包均已通过已登录的软件包页面删除；重新扫描九页软件包列表后，确认剩余验证软件包数量为零。宿主机原生的 `kc-registry.service` 保持运行。
未修改无关的 `sprout-postgres-v2` 容器。

### 历史剩余缺口

- P0：最终 release commit 尚未形成且未经授权推送。当前提交的 Go tests/coverage、
  offline-resource-validate、bootstrap、resource package、release manifest/provenance
  和 OCI AIO 必需 Actions 尚无结果。
- P0：生产级认证 TLS Harbor 的双机资格测试尚未执行。必须用 robot account、
  自定义 CA 和项目最小权限验证 deploy、login/version、join、纯 OCI 建群、
  add/remove node、删除、clean 及残留审计。
- P1：补齐最终 v2.0 升级说明和 Harbor 最小权限 support policy；当前开发指南已记录
  Registry 分工、凭据输入、轮换、清理和 GHCR Catalog 限制。
- P2：测试主机未运行 chronyd/ntpd；现有部署预检已明确报告此情况。由于实测主机时钟偏差小于一秒，验证继续执行。
- P2：删除 manifest 后回收 Distribution Registry blob，仍属于 Registry 运维方的常规垃圾回收职责。

### 历史发布建议

`cf2f967` 已证明匿名 HTTP Registry 下的纯 OCI 主流程可以工作。最新 master Registry
模型已经集成，package/image Registry 混用已关闭，生产 package Registry 认证/TLS
及凭据传播也已有本地实现和测试。但是当前最终提交的 Actions 和真实 Harbor 双机
资格测试尚未完成，不能将本地通过等同于正式发布通过。

当前发布结论：**暂不建议正式发布**。

重新达到“可以正式发布”至少需要：

1. 形成并在明确授权后推送最终 release commit。
2. 在该提交上执行全部必需 GitHub Actions，并记录 run 链接、OCI digest 和
   sourceRevision。
3. 使用相互独立的认证 TLS package Registry 和 image Registry 重跑双机纯 OCI
   部署、join、建群、增删节点、删除和 clean。
4. 删除测试标签、robot account/授权、临时 CA/密钥、隧道和文件，确认两台主机无
   KubeClipper、Kubernetes、containerd 或测试凭据残留。
