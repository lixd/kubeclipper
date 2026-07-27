# 纯 OCI 发布就绪报告（2026-07-27）

> **当前结论：最终发布代码候选 `c9d35b37d36d9285a4f0007c3267b486956312d2` 的必需
> GitHub Actions、112 项 OCI manifest/provenance，以及 Kubernetes v1.34.2、v1.35.0、
> v1.36.1 三版本双机纯 OCI 生命周期均已通过。remove-node、delete-cluster 和最终
> `kcctl clean --all` 后无需人工补救；qualification package/tag、临时授权、代理、缓存和文件
> 已全部清理。P0/P1 为零，建议正式发布 2.0.0。**

## 2026-07-27 最终发布候选 `c9d35b3`（三版本矩阵）

### 候选提交和修复范围

- 分支：`codex/oci-static-server-replacement`。
- 发布代码 revision：`c9d35b37d36d9285a4f0007c3267b486956312d2`。
- 最终提交：`c9d35b3 fix(k8s): remove stale CNI network namespaces`。
- 本轮在前一候选上继续关闭了 Calico 清理顺序、canonical runtime path、IPIP module、
  API node operation optimistic-lock retry 和 `/run/netns/cni-*` 残留问题。
- 代码分支和 qualification tag 只普通推送到用户 fork `lixd/kubeclipper`；未推 upstream、
  未强推或改写远端历史。报告提交不属于发布代码，不得替代上述 `sourceRevision`。
- 双机门禁覆盖 Kubernetes、containerd 和必需 CNI Calico；MetalLB、NFS 是扩展组件，不作为
  双机生命周期门禁，但仍由正式 release workflow 按 support policy 发布和校验。

### 当前 release commit 的 GitHub Actions

| 必需工作流 | Run | 结果 |
|---|---|---|
| Go tests/coverage | [30306588151](https://github.com/lixd/kubeclipper/actions/runs/30306588151) | completed/success |
| offline-resource-validate | [30306588093](https://github.com/lixd/kubeclipper/actions/runs/30306588093) | completed/success |
| OCI AIO deploy + Fast E2E | [30306588159](https://github.com/lixd/kubeclipper/actions/runs/30306588159) | completed/success |
| 16 组件发布 + manifest/provenance | [30306588286](https://github.com/lixd/kubeclipper/actions/runs/30306588286) | completed/success |

2026-07-27 使用 `gh run view` 和 Actions API 再次复核：四个 run 的 `headSha` 都精确等于
`c9d35b37d36d9285a4f0007c3267b486956312d2`。publish workflow 的 16 个 publish job 和
manifest job `90114997247` 全部成功。

### Release manifest、digest 和 provenance

manifest job 原始日志记录：

```text
version:                       v2.0.0
artifact count:                112
verification:                  verified 112 artifact(s); failures: 0
manifest sha256:               435ad9cfdbb2c81a07e000374945cd4f699f26e0c7e0d4032a552703fb8cdd6d
metadata.sourceRevision:       c9d35b37d36d9285a4f0007c3267b486956312d2
bootstrap/kubeclipper:v2.0.0: sha256:58682f36f0f62cfe1be68fd56270dd506bf8e6792c869ad3f01de125a3b10a43
bootstrap/etcd:3.5.21:        sha256:ca70f46efc12232bdb5a5fd3f9f31d5845ac9b43565981c852c564f742ed5ba1
bootstrap/console:v1.6.0:     sha256:ab7547d803b5fd8f38d29e3b44f91f0efc06efe30b1cb537ed906746b75c8481
cri/containerd:1.7.29:       sha256:87d214e2b248d55200370dd0494bb3c63bc7229e9596d48f195deddbd6d8e101
cri/containerd:2.2.4:        sha256:703eb8a55fa8fd11b0c14fce73aefc6391d16a242d5ba10ad0d3bd3dcc7cd5a1
k8s/k8s:v1.34.2:             sha256:1cfb04ed52a42c59a5674ae5efaef14412052bb8f3c6b1a732a8c26545d52324
k8s/k8s:v1.35.0:             sha256:6e2fa5e54b9e49ab8f272613abf08878d41c05f129d006a2fb5e4e0d0e97b144
k8s/k8s:v1.36.1:             sha256:de906ae79df1f892bd795e07b88b953cc12b53b7bc93e23a0f09fa50e7560d71
tigera chart:v3.29.6:        sha256:e70d51dd2ff6d0d2a8013a112fe1f13faddd186b9582f11fe8375e86b088610f
tigera chart:v3.31.5:        sha256:9b5aa9e439479fc17633b859136871c18d836e5277c6ac4b3b5473628a27af18
```

manifest 包含 package、Helm chart 和 runtime image 的 amd64/arm64 产物；校验器逐项读取远端
digest、runtime child digest 和 package OCI revision label。qualification package 在验收结束后
按计划删除，上述证据保留在不可变的 Actions run/job 日志中。

### 本地代码验证

最终增量执行：

```text
go test -race ./pkg/scheme/core/v1/k8s ./pkg/clusteroperation ./pkg/apis/core/v1  PASS
go vet ./...                                                                PASS
make lint                                                                   PASS，0 issues
git diff --check                                                            PASS
```

前一候选同一代码链路已完成修改包 race、全仓 Go test/vet/lint、ShellCheck、Actionlint、全部
open-packaging 测试、linux/amd64 和 linux/arm64 构建；当前 SHA 的 Go、offline validation 和
publish Actions 又重新执行这些发布门禁并全部成功。

### 三版本双机纯 OCI 生命周期

测试机为 sh-dev-3（`172.16.131.146`）和 sh-dev-2（`172.16.131.208`），隔离 namespace 为
`ghcr.io/lixd/kubeclipper/qualification-c9d35b37d36d9285a4f0007c3267b486956312d2`。
候选 Linux amd64 `kcctl`、server 和两个 agent 均报告 `v2.0.0`、同一 Git commit、tree clean。
sh-dev-2 使用独立 agent SSH key join，未复用 server SSH transport。

| Kubernetes | containerd | Calico | create / 单 master | add/remove worker | Pod、DNS、API、Service | delete 后残留 |
|---|---|---|---|---|---|---|
| v1.34.2 | 1.7.29 | v3.29.6 | 通过；未传 `--untaint-master`，API 自动持久化 `true` | 通过，无 resourceVersion conflict | 通过；PodIP `172.25.58.194`，ServiceIP `10.108.52.101` | 两机均为 0 |
| v1.35.0 | 1.7.29 | v3.29.6 | 通过；master 无 taint | 通过，无 resourceVersion conflict | 通过；PodIP `172.25.58.194`，ServiceIP `10.97.156.113` | 两机均为 0 |
| v1.36.1 | 2.2.4 | v3.31.5 | 通过；API `untaintMaster: true` | 通过；AddNodes `6d07f2d6...`、RemoveNodes `89b2e78b...` successful | 通过；PodIP `172.25.58.194`，ServiceIP `10.108.229.170` | 两机均为 0 |

每个版本都从已删除上一集群的基线重新 create，完成 master Ready、全部系统 Pod Ready、Pod 内
DNS 和 Kubernetes API `/version`、worker add-node、worker Ready、master 到 worker PodIP、
Service ClusterIP、remove-node、delete-cluster。三个版本均无需 force、手工 `kubeadm reset`、
手工卸载 netns 或重启服务。

v1.36.1 的补充证据：

- master 和 worker 都报告 kubelet `v1.36.1`、`containerd://2.2.4`，Calico 和 kube-proxy
  DaemonSet 完整覆盖两节点。
- worker Pod 内 CoreDNS 将 `kubernetes.default.svc.cluster.local` 解析为 `10.96.0.1`，API
  `/version` 返回 `v1.36.1`；master 到 worker PodIP 和 Service ClusterIP 均返回 `worker-ok`。
- remove-node 后 worker 的 kubelet/containerd service 和进程为 0；Kubernetes、containerd、
  CNI、Calico 目录、Calico 接口、`/run/netns/cni-*` 文件和挂载全部为 0。
- DeleteCluster operation `c29a8a1f-320d-4015-9c5f-167fcb2cca08` successful；master 执行
  同级审计，所有计数为 0。

环境告警单独分类：deploy 预检报告 `chronyd or ntpd service not running`，但两机实测时间差仅
`0.178s`；v1.36.1 首次拉取 kube-proxy 时 GHCR token 请求出现一次 EOF，containerd/kubeadm
自动重试后继续成功。两项均未造成 digest、认证、安装或运行错误，不属于产品阻断。

### 最终 clean 和测试资源清理

- 一次执行 `kcctl clean --all --assumeyes` 返回 `clean successful`；日志明确枚举
  `server`、`agent (deploy)`、`agent (join-cd07701e...)`，证明通过 join 加入的 agent 被覆盖。
- clean 后两机 kubeclipper-server/agent、kubelet、containerd 均 inactive且无进程；产品
  二进制、systemd unit、Kubernetes/CRI/CNI/Calico 配置、目录、接口和 netns 均为 0。
- `clean --all` 按当前设计保留 OCI 下载缓存；确认其只属于本轮后定向删除。两机最终
  `/var/lib/kubeclipper/cache` 和历次验收 `/tmp/kc-*` 测试产物数量均为 0。
- 从两机 `authorized_keys` 按 key blob 精确删除本轮及前序矩阵测试公钥，各删除 4 条；复查
  `kc-`、`qualification`、`matrix` 测试标记命中为 0。
- 6 个 `oci-qualification-*` tag 已从 fork 和本地删除，`git ls-remote` 复查为 0；GitHub
  Packages API 删除 216 个严格匹配 `kubeclipper/qualification-` 的 package，完整分页复查为 0。
- 两条 SSH 反向代理、所有 `/private/tmp/kc-*` 测试缓存/日志/工具目录、临时 key 和证书均已删除。
- 未停止或删除 sh-dev-3 上与 KubeClipper 无关的 Docker/Registry/nginx 和受保护容器
  `sprout-postgres-v2`；按用户要求继续保留 `gh` 的 package 权限，未泄露 token。

### P0/P1/P2 和发布建议

- **P0：无。** 最终 release code commit 的四条必需 Actions、112 项 provenance、三版本双机
  create/add/remove/delete、最终 clean 和测试环境清理全部有直接证据。
- **P1：无。** clean 多 join、SSH transport 隔离、网卡检测、架构检测、support policy、Actions
  runtime、Registry 认证/TLS、单 master 调度和 CNI netns 清理均已关闭。
- **P2：clean 缓存语义。** `clean --all` 保留无凭据的 OCI 下载缓存以便重装，名称可能让用户
  误解为完全清理；后续可增加 `--purge-cache` 或完善帮助文本，不阻断 2.0.0。
- **P2：删除操作日志可观测性。** 删除末段资源回收可能产生预期的 not-found/optimistic-lock
  日志；生命周期结果正确，后续可降低预期终态日志级别，不阻断发布。

最终建议：**可以正式发布 2.0.0。** 正式 tag、二进制、OCI package 和 GitHub Release manifest
必须从 `c9d35b37d36d9285a4f0007c3267b486956312d2` 生成；不要把后续仅包含本报告的提交作为新的
`sourceRevision`。

## 2026-07-27 历史发布候选 `47d9ef1`

### 候选提交和边界

- 分支：`codex/oci-static-server-replacement`。
- 发布代码 revision：`47d9ef11f1d5e3f3d8f85bdd6eed266993ab3051`。
- 当前代码提交：`47d9ef1 test(e2e): wait for node disable state`；上一提交
  `0d2fbff fix(oci): harden delivery and multi-join cleanup` 包含本轮最终产品修复。
- 分支和 qualification 标签只推送到用户 fork `lixd/kubeclipper`；未推 upstream、未强推或覆盖历史。
- 本轮双机门禁只要求 Kubernetes、containerd 和必需 CNI Calico；MetalLB、NFS 不作为双机门禁，
  但正式 release workflow 仍按 support policy 发布完整 catalog 并校验 manifest。
- 本节之后创建的报告提交仅记录证据，不能替代 `47d9ef1` 作为二进制和 OCI
  `sourceRevision`。

### 当前 release commit 的 GitHub Actions

| 必需工作流 | Run | 结果 |
|---|---|---|
| Go tests/coverage | [30257358359](https://github.com/lixd/kubeclipper/actions/runs/30257358359) | success；head SHA 为 `47d9ef1` |
| offline-resource-validate | [30257358757](https://github.com/lixd/kubeclipper/actions/runs/30257358757) | success；policy、Shell、workflow、manifest、bundle、provenance 和 assembly 门禁通过 |
| OCI AIO deploy + Fast E2E | [30257359187](https://github.com/lixd/kubeclipper/actions/runs/30257359187) | success；deploy、login、Fast E2E、clean 通过 |
| 16 组件发布 + manifest/provenance | [30257359028](https://github.com/lixd/kubeclipper/actions/runs/30257359028) | success；全部 publish job 和 manifest job 通过 |

2026-07-27 再次通过 `gh run view` 复核：四个 run 均为 `completed/success`，且
`headSha` 全部精确等于 `47d9ef11f1d5e3f3d8f85bdd6eed266993ab3051`。

### Release manifest、digest 和 provenance

qualification workflow 生成并严格校验的 manifest：

```text
version:                       v2.0.0
artifact count:                112
manifest sha256:               d7fce4c5654f2631503f142eb4758feeca49ef581e095b5a1b8256af4d5516d2
metadata.sourceRevision:       47d9ef11f1d5e3f3d8f85bdd6eed266993ab3051
bootstrap/kubeclipper:v2.0.0: sha256:5f9016d1c9706a24b73c231713bfd76566beceae86a51469dfc593f542a7a368
bootstrap/etcd:3.5.21:        sha256:46d269c4f6acb180d4edfbb2b856b2a84dd22afa72ea5de577353173902230c7
bootstrap/console:v1.6.0:     sha256:d31b9b9b8bd5aca7bc6028151e17aeb14fcd095439fd7a60507c80eed35be366
cri/containerd:1.7.29:       sha256:7e6e20ec797d0f2f42612bfcca105eef502811b97a33570397ed74eb53884ccc
k8s/k8s:v1.34.2:             sha256:9e6f70fb1a17b47186af8ca2e125663aa3e4076c448c100218dc5d439056266e
tigera chart:v3.29.6:        sha256:e70d51dd2ff6d0d2a8013a112fe1f13faddd186b9582f11fe8375e86b088610f
```

manifest job 输出 `verified 112 artifact(s); failures: 0`。所有 package 的 amd64/arm64
平台、runtime image child digest、Helm chart digest 和 package OCI revision label 均由同一
workflow 远端解析；bootstrap/kubeclipper 等 package 的 `sourceRevision` 与 metadata 完全一致。

### 当前增量的本地和 CI 验证

当前增量再次执行：

```text
go test -race ./cmd/kcctl/app/options ./pkg/cli/clean ./pkg/cli/deploy \
  ./pkg/cli/join ./pkg/component/common ./pkg/delivery/apis \
  ./pkg/delivery/fetcher ./pkg/delivery/publisher ./pkg/simple/downloader
                                                               PASS
git diff --check                                                 PASS
```

其中 race 测试覆盖：多 transport clean、join 独立 SSH 配置、OCI publisher 并发锁和失败清理、
Helm OCI 下载、严格 delivery 校验、HTTP 下载临时文件和错误传播。全仓 Go test/coverage、vet、
lint、Actionlint、ShellCheck、open-packaging、release assembly、linux/amd64 与 linux/arm64 构建、
manifest/digest/sourceRevision/provenance 验证由上表两个代码门禁 workflow 在同一 SHA 上完成。

### 双机纯 OCI 生命周期

测试机为 sh-dev-3（`172.16.131.146`）和 sh-dev-2（`172.16.131.208`），隔离 Registry
namespace 为
`ghcr.io/lixd/kubeclipper/qualification-47d9ef11f1d5e3f3d8f85bdd6eed266993ab3051`，
集群名为 `oci-qualification-47d9ef1`。

1. 从干净基线使用候选 Linux amd64 `kcctl` 和 OCI bootstrap package 在 sh-dev-3 完成 AIO
   deploy；kcctl、server、agent 均为 `v2.0.0`、commit `47d9ef1`、tree clean。
2. 默认网卡检测选择 `ens3`，没有选择 Docker/CNI bridge。deploy config 权限为 `0600`，
   `deploy` 与 `join-7fe03deb-0771-4a51-ae4d-7c2d8bc257e4` transport 分离且不持久化私钥正文。
3. 使用独立 server key 和 agent key 将 sh-dev-2 join；两个 agent 均 Ready。
4. 未传 `--untaint-master` 创建单 master 集群，API 持久化 `untaintMaster: true`。CreateCluster
   operation `62cd28d2-cba3-4af8-be91-b38cce0b3539` 的 14 步全部 successful。
5. master 为 Ready；Kubernetes `v1.34.2`、containerd `1.7.29`、Calico `v3.29.6`，全部系统
   Pod Running/Ready。master 普通 Pod 从 qualification GHCR 拉取成功，DNS 返回 `10.96.0.1`，
   Pod 内 API `/version` 返回 `v1.34.2`。
6. AddNodes operation `26069e4c-ef8f-4b4f-b90d-99ee6b65e3d4` 的 8 步全部 successful；worker
   Ready，containerd `1.7.29`，Calico node/CSI/kube-proxy 均 Running/Ready。worker 实际从 GHCR
   拉取 pause、kube-proxy、Calico CNI/node/flexvol/CSI/registrar 镜像。
7. worker 定向普通 Pod 使用 qualification GHCR 中已拉取的 `calico/node:v3.29.6` 正常 Running；
   Pod 内通过 `kubernetes.default.svc.cluster.local:443` 建立连接，验证 DNS 和 Service 网络。
8. RemoveNodes operation `a3c93682-a637-4e89-8f6a-6c1928857f61` 的 14 步全部 successful；worker
   kubelet/containerd inactive，Kubernetes、containerd、CNI、Calico 路径、接口和挂载全部清除，
   平台 agent 按设计保留到最终 clean。
9. 正常执行 `kcctl delete cluster oci-qualification-47d9ef1`，DeleteCluster operation
   `17f37b9b-5948-491e-82ca-b3f02c5713d8` 启动后集群进入 404；master 的 Kubernetes、containerd、
   CNI 和 Calico 运行态全部清除，无 force、kubeadm reset 或人工修复。
10. 一次执行 `kcctl clean --all --force --assumeyes` 返回 `clean successful`，日志明确完成
    `server`、`agent (deploy)` 和 `agent (join-7fe03deb...)` 三组 transport，AIO server/agent 和
    join agent 均被清理，无第二次 clean 或人工服务修复。

测试机到 GitHub CDN 存在严重丢包，构建和大镜像下载阶段使用了隔离 SSH 反向代理。一次 worker
`kubeclipper/kubectl` smoke pull 因 CDN TLS handshake timeout 失败；错误为
`pkg-containers.githubusercontent.com` 超时，不是 Registry 404、401/403、digest 或产品配置错误。
Kubernetes/Calico 的 qualification GHCR 镜像已在 worker 真实拉取并运行，普通 Pod、DNS 和 Service
网络验证仍完成。代理仅用于环境绕行，没有进入代码、manifest 或正式部署配置。

### 最终清理与残留审计

- 两机 `kc-agent`、`kc-server`、`kubelet`、KubeClipper 安装的 `containerd` 均 inactive，无对应
  运行进程；`/etc/kubernetes`、`/var/lib/kubelet`、`/etc/containerd`、`/var/lib/containerd`、
  CNI/Calico 路径、网络设备和挂载均不存在。
- `kcctl clean` 自动清除了服务、unit、配置、运行数据和安装二进制；它按当前语义保留
  `/var/lib/kubeclipper/cache` 下载缓存。本轮测试收尾确认缓存只属于本轮 qualification 后，定向删除
  两机该目录；最终 `/var/lib/kubeclipper` 均不存在。
- sh-dev-2 的 `/root/.kc` 创建于 2026-07-08/09，早于本轮且属于机器既有环境，按保护基线原则保留；
  本轮两把临时 SSH 公钥按 key blob 从两机 `authorized_keys` 精确删除，复查命中为 0。
- systemd manager 中临时 `HTTP_PROXY`、`HTTPS_PROXY`、`NO_PROXY` 已撤销；反向代理和 relay
  `17890`/`17891`/`17892`/`17893` 均已停止且无监听；本机和两机 `/tmp` 测试文件已删除。
- 两个隔离 Git 标签 `oci-qualification-0d2fbff-20260727`、
  `oci-qualification-47d9ef1-20260727` 已从 fork 和本地删除，`git ls-remote` 复查为 0。
- fork GHCR 中两个 qualification namespace 共 72 个 packages 已通过 GitHub Packages API 删除；
  完整分页复查名称包含这两个 commit 的 package 数量为 0。
- 未停止或修改 sh-dev-3 原有 Docker、Registry、nginx、`sprout-postgres-v2` 等无关服务；保留用户
  明确要求暂不撤销的 `gh` package 权限，token 本身未写入文件、日志或报告。

### P0/P1/P2 与发布建议

- **P0：无。** 当前 release code commit 的四条必需 Actions、112 项 manifest/provenance、
  create/add/remove/delete/clean 和最终环境清理均有直接证据。
- **P1：无。** join agent clean、独立 SSH transport、多网卡、架构检测、support policy、Actions
  runtime、Registry 认证/TLS 和 OCI delivery 稳定性阻断项均已关闭。
- **P2-1：删除操作可观测性。** 删除末段 operation 与 cluster 一起回收，server journal 出现一次
  optimistic-lock 更新冲突和若干 `cluster ... is being deleted` controller error；最终资源正常 404、
  节点清理完整且无需补救。后续可将预期终态降级日志级别，或在回收前保留最终 operation 状态。
- **P2-2：clean 缓存语义。** `clean --all` 会保留 `/var/lib/kubeclipper/cache`，便于重装复用，但
  “all” 对用户可能意味着完全删除。后续可增加明确的 `--purge-cache` 或在帮助中说明；缓存不含
  SSH key、Registry 密码或 Kubernetes 配置，不阻断发布。

最终建议：**可以正式发布 2.0.0。** 正式 tag、二进制、OCI package 和 GitHub Release manifest
必须从 `47d9ef11f1d5e3f3d8f85bdd6eed266993ab3051` 生成；本报告的后续文档提交不能作为新的
`sourceRevision`。

## 2026-07-27 历史发布候选 `3a1ad28`

> 本节保留早期完整 Harbor/TLS 和双机证据用于审计；当前发布 revision 以上一节的
> `47d9ef11f1d5e3f3d8f85bdd6eed266993ab3051` 为准。

### 候选提交和发布边界

- 分支：`codex/oci-static-server-replacement`。
- 发布代码 revision：`3a1ad28d13334a5a46e06162b2ad1442e5f8bc74`。
- 最后一个代码提交：`3a1ad28 fix(k8s): make kubelet config cleanup idempotent`。
- 分支只推送到用户 fork `lixd/kubeclipper`；未推 upstream、未强推或覆盖历史。
- 本次双机 E2E 只验收 Kubernetes、containerd 和作为必需 CNI 的 Calico；没有把 MetalLB、NFS
  等扩展组件作为双机发布门禁。发布工作流仍按 support policy 发布完整 catalog。

### 当前 commit 的 GitHub Actions

| 必需工作流 | Run | 结果 |
|---|---|---|
| Go tests/coverage | [30204655540](https://github.com/lixd/kubeclipper/actions/runs/30204655540) | success |
| offline-resource-validate | [30204655544](https://github.com/lixd/kubeclipper/actions/runs/30204655544) | success；policy、Shell、workflow、manifest、bundle、provenance、assembly 和 Skopeo 验证全部通过 |
| OCI AIO deploy + Fast E2E | [30204655547](https://github.com/lixd/kubeclipper/actions/runs/30204655547) | success；OCI deploy、login、Fast E2E 和 clean 全部通过 |
| 16 组件发布 + release manifest/provenance | [30204655615](https://github.com/lixd/kubeclipper/actions/runs/30204655615) | success；全部 publish job 和 manifest job 通过 |

同一提交较早的 run `30204645751` 被 workflow concurrency 取消；随后完整 run
`30204655540` 成功，因此它不是测试失败，也不替代上表的成功 run。

### Release manifest 和 provenance

qualification workflow 生成的 manifest 为 `/tmp/release-manifest-3a1ad28.yaml`：

```text
version:                 v2.0.0
artifact count:          112
manifest sha256:         1112337b9216e216c6f100a98f33159809fceb7d33583473fc39de7c77ddf434
metadata.sourceRevision: 3a1ad28d13334a5a46e06162b2ad1442e5f8bc74
bootstrap/kubeclipper:   sha256:b9b3564681271592e19e25f9626b095e0f12fc3bd8ec03c7441969ba683c3041
bootstrap/etcd:          sha256:0bdf75ccaa974ffb9e7eb81599507f3f11e74a0038e4bd782f385382a2218080
bootstrap/console:       sha256:bd8bab7fb86372ad2a5f7f6ef22c1a3d3bdc259a633f40895caea8e9cbd1bd7e
cri/containerd:1.7.29:   sha256:8a63861942d7b8874b96d9323ac34c564d8dc1611fea64a524730f545a911083
tigera chart v3.29.6:    sha256:e70d51dd2ff6d0d2a8013a112fe1f13faddd186b9582f11fe8375e86b088610f
```

Harbor 同步后的 6 个 package 顶层 digest 全部匹配；6 个 package 的 amd64/arm64 共 12 个
平台 manifest，其 OCI revision label 均为 `3a1ad28d13334a5a46e06162b2ad1442e5f8bc74`。
用于本次 Kubernetes `v1.34.2` + Calico `v3.29.6` 的 Tigera chart 和 18 个 amd64 runtime
image 共 19 项，digest 全部匹配。

### 生产方式 Harbor 权限和 TLS 验收

隔离 Harbor 为 `https://172.16.131.105:5443/qualification-3a1ad28`，使用 IP SAN 证书、
自定义 CA、项目级 reader/writer robot 和严格 TLS：

- 匿名访问返回 401；错误凭据返回 401；未提供自定义 CA 时 TLS 校验失败。
- reader 可以 pull，但不能 push；writer 可以 push；权限没有扩大到项目管理或系统管理。
- server/agent 的 Registry 配置权限均为 `0600`，密码没有进入普通日志。
- Kubernetes image Registry 资源 `qualification-3a1ad28-images` 使用 HTTPS、CA 和认证，
  `skipTLSVerify=false`。
- Harbor 证书轮换后，节点上的 CA 指纹与服务端一致；containerd 从同一 Registry 成功拉取
  Kubernetes、Calico 和 `kubeclipper/kubectl` 镜像。

一次普通 Pod 最初使用了并不存在的 `busybox:1.36` tag。containerd 先对带 CA 的 host 得到
真实 404，再在 fallback host 上报告 x509，后一个错误覆盖了 404。改用 release catalog 已包含的
`kubeclipper/kubectl` 后，Registry pull、Pod、DNS 和 API smoke 全部成功；这是测试数据错误，
没有通过关闭 TLS、跳过校验或修改产品代码规避。

### 双机纯 OCI 生命周期

测试主机为 sh-dev-3（`172.16.131.146`）和 sh-dev-2（`172.16.131.208`）；集群名为
`oci-qualification-3a1ad28`，Kubernetes `v1.34.2`、containerd `1.7.29`、Calico `v3.29.6`。

1. 使用当前 commit 的 Linux amd64 `kcctl` 和 OCI bootstrap package 在 sh-dev-3 完成 AIO
   deploy；kcctl/server/agent 均报告 `v2.0.0`、同一 commit、tree clean、linux/amd64。
2. 默认地址检测在多网卡主机选择 `ens3`，没有选择 Docker/CNI bridge。
3. 使用互不通用的 server SSH key 和 agent SSH key 将 sh-dev-2 join；两个平台节点 Ready。
4. 未传 `--untaint-master` 创建单 master 集群，API 持久化 `untaintMaster: true`；创建 operation
   `3fb5a74f-f92e-46c4-9087-18e5ec345528` 成功，master 无 control-plane taint，所有 Kubernetes
   和 Calico 系统 Pod Running/Ready。
5. master 上普通 Pod 成功从认证 Harbor 拉取，CoreDNS 将
   `kubernetes.default.svc.cluster.local` 解析为 `10.96.0.1`，Pod 内访问 API 返回 v1.34.2。
6. add-node operation `c3f973f6-e1e7-4350-8628-8d70639249c7` 将 sh-dev-2 加入；两节点均
   Ready，worker 运行 containerd `1.7.29`。
7. worker 定向 Pod 在 sh-dev-2 成功完成 Registry pull、DNS 和 API 访问；master Pod 到 worker
   Pod IP `172.25.58.194` 的 3 次 ping 全部成功，0% 丢包，证明 Calico 跨节点数据面正常。
8. remove-node operation `fe422d42-9857-4815-b73c-ac57d241e77f` 的 14 个步骤全部 successful；
   日志中没有 `remove config.yaml failed`，worker 的 kubelet/containerd inactive，Kubernetes、
   containerd、CNI 和 Calico 路径及网络设备均已清除。
9. 正常 API 删除集群后，资源从 `Terminating` 进入 404，不需要 force 或人工 kubeadm 清理。
10. 一次执行 `kcctl -y clean -A -f` 返回 `clean successful`，同时清理 AIO server/agent 和通过
    join 加入的 agent，无人工补救。

### 清理和残留审计

- 两机 `kc-server`、`kc-agent`、`kc-etcd`、`kc-console`、`kubelet`、KubeClipper 安装的
  `containerd` 均 inactive；没有对应运行进程。
- 两机无 `/etc/kubeclipper`、`/var/lib/kubeclipper`、`/etc/kubernetes`、`/var/lib/kubelet`、
  `/var/lib/etcd`、KubeClipper 的 `/etc/containerd`/`/var/lib/containerd`、CNI/Calico 路径，
  无 `cali*`、`vxlan.calico`、`kube-ipvs0` 设备和 `apiserver.cluster.local` hosts 项。
- 两条 qualification SSH 公钥已从 authorized_keys 删除；reader 密码在文件系统和 journal
  精确命中均为 0。robot 用户名在 sh-dev-3 journal 中有 1 条正常审计记录，不属于秘密泄漏。
- sh-dev-3 预装 apt 包 `containerd` 的 `/usr/bin/containerd` 和 systemd unit 时间为
  `2026-06-22`，早于本轮测试且被原有 Docker 使用；服务为 disabled/inactive，按“不碰无关服务”
  保留，不计为 KubeClipper 残留。原有 Docker/Harbor 服务未被停止或修改。
- Harbor 项目中的 26 个 repository 全部通过 API 删除，reader/writer robot ID 29/30 删除后
  返回 404，项目 `qualification-3a1ad28` 删除后返回 404；未执行 Registry blob GC。
- 14 个 `oci-qualification-*` Git 标签已从 fork 和本地删除；复查结果为 0。
- sh-package 上 `kc-final-control-*`、`kc-range-*`、`kc-sync-*`、`kc-harbor-source-*`、
  `kc-qualification-*` 临时密钥、CA、密码和工作目录已删除。
- fork GHCR 中本轮完整 namespace
  `ghcr.io/lixd/kubeclipper/qualification-3a1ad28d13334a5a46e06162b2ad1442e5f8bc74`
  的 36 个 packages 已删除。分页复查又发现此前 16 次 qualification workflow 各残留 36 个
  package；这些严格匹配 `kubeclipper/qualification-<40 位 commit>/...` 的历史测试包共 576 个，
  也已全部删除。本次合计删除 612 个 GHCR qualification packages。
- 删除后完整分页复查：`kubeclipper/qualification-` 严格前缀数量为 0，任意名称包含
  `qualification` 的 container package 数量为 0；代表 package
  `qualification-3a1ad28d.../pause` 的 GitHub Packages REST endpoint 返回 HTTP 404，匿名
  Registry 读取失败。
- 经用户明确要求，`gh` 登录保留 `read:packages` 和 `delete:packages` scope，便于后续正式发布
  和资格测试清理；token 本身没有写入报告、日志或仓库。

### P0/P1/P2 和发布建议

- **P0（产品和发布链路）：无。** 当前 release commit 的代码门禁、四条必需 Actions、112 项
  manifest/provenance、认证 TLS Harbor、双机 create/add/remove/delete/clean 均有直接证据。
- **P1：无。** fork GHCR 当前及历史 qualification packages 已全部删除并完成 0/404 复查。
- **P2：无新增项。** Registry blob GC 仍由 Registry 运维方按常规策略执行。

最终建议：**可以正式发布 2.0.0。** 正式二进制和 OCI 产物必须继续对应
`3a1ad28d13334a5a46e06162b2ad1442e5f8bc74`；后续仅更新报告的文档提交不能作为新的二进制
`sourceRevision`。

## 2026-07-24 `kcctl registry sync` 与 2.0.0 发布增量

### 已完成实现

- 功能提交：`d1fe0c7 feat(registry): sync release artifacts from OCI manifest`。
- 发布架构门禁提交：`146f571 fix(release): derive architectures from release manifest`。
- 官方 namespace 门禁提交：`ea6d1e3 fix(release): pin official OCI namespace`。
- 完整 assembly 回归提交：`93d5edb test(release): verify complete OCI assembly`。
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
- 正式 `release.yml` 固定发布到 `ghcr.io/kubeclipper/kubeclipper`；fork qualification 仍使用隔离的
  fork namespace。verifier 同时拒绝把正式 package/image Registry 改成个人或其他组织地址，确保
  GitHub Release manifest 与 `kcctl registry sync` 的官方源约束一致。
- 生产 Harbor 最小权限已明确：sync writer 仅授予项目级 Repository Pull + Push；平台运行时
  reader 仅授予 Repository Pull。两者均不需要 Delete、项目管理、扫描、复制或系统管理权限。
- `offline-resource-validate` 新增完整 release assembly 测试：从正式 `packaging/resources.yaml`
  构造 16 个发布组件的双架构 metadata，重建 `images.lock`，生成并验证包含 bootstrap package、
  普通 package、Helm chart 和 runtime image 的 manifest。测试共校验 32 个最小代表 artifact，
  同时确认所有 package 的双平台和 `sourceRevision`，防止并行 artifact 合并后漏掉 runtime image。

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
bash scripts/open-packaging/tests/release-assembly-test.sh       PASS，32 artifacts
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

- Actions run：无。本轮遵守“不推送”要求，没有为最新代码候选 `93d5edb4` 触发远端工作流。
- OCI digest / sourceRevision：尚无正式 v2.0.0 产物；必须由该提交发布后记录，不能复用
  `7332bac` qualification digest。
- 双机生命周期：已有 `7332bac` 的完整通过证据；`d1fe0c7` 之后的 registry sync、release gate
  和测试提交均未改变 server/agent 生命周期逻辑，
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
