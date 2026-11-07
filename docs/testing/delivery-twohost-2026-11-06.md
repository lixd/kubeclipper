# 2026-11-06 两机交付场景验证

## 范围与候选

- 代码候选为 `4d8bf810d8f3d1ebb73fe7423398fca0b8aa670f`，版本 `v2.0.11`。
  代码构建、回归在 `sh-dev` 的独立目录执行，源文件逐项 SHA256 核对。
  运行中的 Server、Agent 二进制与该候选一致。
- 现场使用 `sh-dev-3` 和 `sh-dev-4`，均为 Ubuntu 24.04 amd64。
  每轮部署一个 Server、两个 Agent，创建一个 Master、一个 Worker 的 Kubernetes 集群。
  借出的 `sh-dev-2` 未执行远程操作，测试配置的目标集合始终限定为前述两台。
- 只在 `lixd/kubeclipper` fork 处理代码和 CI。OSS 全程只读。
  本轮覆盖交付专项，完整长期基准现有 194 个编号。
- 逐步操作见[交付场景测试步骤](delivery-scenarios.md)，长期编号见[核心功能清单](core-feature-checklist.md)。

## 交付结果

| 子步骤 | 国外在线 | 国内在线 | 完整离线 |
|---|---|---|---|
| 同提交 kcctl 准备 | 通过，精确源码构建 | 通过，同一二进制 | 通过，跨机传输及 SHA256 校验 |
| 正式发行入口下载新版 kcctl | 待发布，GitHub Release v2.0.11 为 404 | 待发布，OSS 同版本附件为 404 | 联网准备机器同样受发行状态约束 |
| Registry 自举 | 通过，GHCR package | 通过，阿里云 package | 通过，独立小包及空数据目录 |
| KC 部署及第二 Agent join | 通过 | 通过 | 通过 |
| 原生创建 Kubernetes 集群 | Running | Running | Running |
| 双节点及全部系统 Pod Ready | 通过 | 通过 | 通过 |
| 双向 Pod、HTTP、Service、DNS、API | 通过 | 通过 | 通过，Service 使用绝对域名 |
| 平台 etcd 真实写入、读取、删除 | 通过 | 通过 | 通过 |
| `kcctl doctor` | Unknown，16 passed / 1 failed | Unknown，16 passed / 1 failed | Unknown，16 passed / 1 failed |
| 正常删除集群及平台清理 | 已完成 | 已完成 | 保留运行中的离线来源集群 |

两个在线集群均使用 Kubernetes `v1.37.0`、containerd `2.2.4`、Calico `v3.31.5`。
健康验收时每轮 18 个 Pod Ready，其中 16 个系统 Pod、2 个网络测试 Pod。
网络双向 ping 各 3 包零丢失，跨节点 HTTP 返回对端节点名，Service 请求成功，
DNS 返回 Kubernetes Service IP，带 ServiceAccount 证书和令牌的 API `/healthz` 返回 `ok`。

### 国外在线

使用 VPN HTTP 代理访问 fork qualification 的 GHCR 制品，源前缀为
`ghcr.io/lixd/kubeclipper/qualification-4d8bf810d8f3d1ebb73fe7423398fca0b8aa670f`。
CLI、Server、Agent 和 containerd 的代理环境分别检查生效值，内网地址加入 `NO_PROXY`。
独立 Registry 从新的空缓存重新执行自举，最终调用者也是本轮 kcctl。

KC 原生部署、登录、`join` 和建群完成，运行中二进制提交及镜像来源核对通过。
证据见[状态](evidence/delivery-20261106/global-state-proof.json)和[网络](evidence/delivery-20261106/global-network-proof.json)。

### 国内在线

关闭本轮代理隧道，清除 CLI 大小写代理变量，使用 `KCZONE=cn`。
使用新配置、新交付缓存和原生删除后重新创建的 containerd 存储。
实际 Package Registry 和默认运行镜像 Registry 均为
`registry.cn-hangzhou.aliyuncs.com/kubeclipper-release`。

Native Registry、KC 部署、`join` 和建群完成。直接执行了操作文档中的网络 Bash 段，
随后收集结构化网络证据。恢复性复验连续完成五轮 API 真实写读删，每轮约 0.7–1.5 秒，
期间各 Pod 的重启计数保持一致。
证据见[状态](evidence/delivery-20261106/cn-state-proof.json)、[网络](evidence/delivery-20261106/cn-network-proof.json)、
[稳定性](evidence/delivery-20261106/cn-stability-proof.json)和[服务环境](evidence/delivery-20261106/cn-service-environment.txt)。

阿里云全量比对有 61 个去重 tag，60 个已匹配，更新了
`package-bootstrap-kubeclipper:v2.0.11` 的双架构索引。
随后分别通过国外源和 `KCZONE=cn` 原生执行 `registry import --arch all`，
两次均为 `0 copied, 61 skipped`。

### 完整离线

联网的 `sh-dev-4` 使用同版本 kcctl、`KCZONE=cn` 和内置清单直接导出。
导出时大小写代理变量为空。原生 export 从 13:18:41 UTC 运行到 13:54:57 UTC，
完成全部 61 个去重 tag，生成两个归档。

| 文件 | 字节数 | 大小 |
|---|---|---|
| `kubeclipper-offline-v2.0.11.tar.gz` | 7625787966 | 约 7.1 GiB |
| `registry-3.1.1.tar.gz` | 34613370 | 约 33 MiB |

原始物料目录为 `sh-dev-4:/root/kc-twohost-4ececf5c-20261106.U2WULM/export/offline/`。

同版本 kcctl 和两个归档从 `sh-dev-4` 经 SSH/SCP 传到 `sh-dev-3`，逐文件 SHA256
一致。两台机器通过独立 inet nftables 表阻断 IPv4/IPv6 公网出站，覆盖主机进程和 Pod
转发流量。隔离前后均用预先解析的公网地址执行 `curl --resolve`，GHCR、阿里云、OSS
直连请求全部失败，内网 Registry 可达。关闭了测试代理隧道，CLI 和服务代理变量为空。

原生小包部署启动端口 5627 的新 Registry，导入前 catalog 为空。运行二进制与小包
amd64 ELF 摘要一致，独立检查也验证了 arm64 ELF。共享实例共用的安装路径保留同版本
二进制，启动服务使用本轮新数据目录。
证据见[跨机校验](evidence/delivery-20261106/offline-transfer-proof.json)和
[Registry 自举](evidence/delivery-20261106/offline-registry-bootstrap-proof.json)。

原生完整导入为 `61 copied, 0 skipped`，从 14:19:54 UTC 运行至 14:28:02 UTC。
独立校验读取目标端 277 个 manifest、831 个仓库 blob 链接、698 个去重 blob，
总计 7944190444 字节，大小和 SHA256 全部一致，每个 tag 均有 linux/amd64 和 linux/arm64。
重复导入从 14:29:54 UTC 运行至 14:39:45 UTC，结果为 `0 copied, 61 skipped`。
证据见[完整内容校验](evidence/delivery-20261106/offline-import-content-proof.json)。

使用操作文档的最小部署配置，在 `KCZONE=cn` 下显式指定
`--package-registry 172.16.131.146:5627/kubeclipper-release --package-registry-scheme http`。
KC、第二个 Agent join 和真实 Kubernetes 建群完成，默认运行镜像仓库也自动使用
该 HTTP 前缀，packagePlan 与实际 Pod 镜像全部匹配。
证据见[离线状态](evidence/delivery-20261106/offline-state-proof.json)和
[离线网络](evidence/delivery-20261106/offline-network-proof.json)。

五项负例包括不存在的归档、不支持的架构、损坏 gzip、小包误用于完整导入及目标 tag
冲突。均核对到预期错误类型和非零退出码，冲突占位 tag 摘要一致，正式源 tag 摘要
复核通过，测试占位 tag 已删除。证据见[负例](evidence/delivery-20261106/offline-negative-proof.json)。

## 发现与验收边界

### doctor 的口令登录上下文

口令登录后当前客户端上下文只有令牌，没有 doctor 直接探测 etcd 所需的 CA 和客户端证书。
`doctor` 显示 `Overall Unknown`，汇总为 16 passed、1 failed，问题为
`etcd client credentials are unavailable`。Server、Agent 和 etcd 服务本身均正常，
独立 etcd endpoint health 及真实 KV 写读删已验证。该轮记录了此诊断缺陷。
原始输出见[国内 doctor](evidence/delivery-20261106/cn-doctor.txt)。

2026-11-07，候选 `eb286042` 改为在 Server 上使用专用健康检查证书执行 etcdctl。
保留的双节点环境中新口令登录复验为 Healthy、17 passed / 0 failed、exit 0。
本报告表格保留 2026-11-06 的当轮结果，修复后的证据见[复验报告](doctor-2026-11-07.md)。

### 国内环境的瞬时 I/O 和旧 peer 流量

导出大包的后台全文件校验与国内集群启动同时运行时，主机 I/O pressure 较高。
首次创建网络测试 namespace 返回 `etcdserver: request timed out`，请求稍后实际完成。
Controller Manager、Scheduler 和 Operator 曾重启，后续稳定性验收期间重启计数未增长。

暂停本轮大文件校验后继续排查，另观察到旧 HA peer 持续向新 etcd 端口发起连接，
新证书拒绝连接并产生约 694 条日志 / 30 秒。在 `sh-dev-3` 增加仅限旧 peer 来源和
TCP 12380 的独立入站规则，阻断该类连接。该现象与 I/O 同期发生，当前证据不足以
将全部瞬时延迟归因于单一因素。恢复后连续五轮 API 写读删通过。

首次失败日志保留在现场任务目录。操作文档的 namespace 改为声明式 apply，
增加 I/O 检查、顺序执行大文件校验和失败后的稳定性验收。

### 离线目标的磁盘容量

`sh-dev-3` 的 Registry 数据和大包副本位于同一根盘，部署后剩余可用比例低于 kubelet
的 `imagefs.available: 15%`，网络测试 Pod 被 DiskPressure 驱逐。
完成全量内容校验和重复导入后，删除该主机本轮 7625787966 字节的大包副本，
原始两个归档和校验记录保留在 `sh-dev-4` 的 `export/offline`。

可用空间从约 32.6 GB 恢复至约 40.2 GB，等待实际配置的 5 分钟压力恢复周期，
DiskPressure 恢复 False 后重建测试 Pod，网络验收通过。容量要求和恢复步骤已加入
操作文档。证据见[磁盘恢复](evidence/delivery-20261106/offline-disk-pressure-proof.json)。

### 离线 DNS 搜索域

现场 Pod 的搜索域包含 `openstacklocal`，并使用 `ndots:5`。公网 DNS 被阻断时，
省略末尾点的长 Service 名称在搜索扩展过程中解析失败，原始失败记录保留。
同一网络下短服务名及末尾带 `.` 的绝对名称均解析成功，HTTP 请求成功，
验收脚本统一使用绝对名称。Kubernetes 文档说明了 Pod 的 DNS 搜索与名称扩展规则。
[DNS 规则](https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/)

该环境行为作为使用限制记录。实际对比见[DNS 验证](evidence/delivery-20261106/offline-dns-proof.json)。

### 测试前置配置修正

- 最初管理员测试口令超过当前 16 位上限，在预检阶段被拒绝，改为合规测试口令后部署成功。
- 国外场景发现旧 systemd 无代理 drop-in 覆盖本轮代理设置，调整本轮文件优先级并核对
  大小写环境变量后，原生 Operation 继续完成。
- 首次在线场景清理使用了旧两 Server 配置，Worker 上缺失 Server/etcd/Console unit，
  CLI 明确非零退出。后续清理配置按真实一个 Server、两个 Agent 修正，国内清理返回 0。
- tag 冲突负例的首次清单缺少必须的 `bootstrap/kubeclipper`，在清单校验处被拒绝。
  修正为有效 core 清单后，重新验证到真实 `target tag conflict`，判据同时核对错误类型
  和非零退出码。

### 下载和架构

OSS 的当前脚本入口可读取，本轮未向 OSS 写入。正式 v2.0.11 的二进制、checksum 和
manifest 附件均返回 404，正式 GitHub Release 同样未发布。
现场部署使用已核对源码和 SHA256 的候选 kcctl，正式发行下载子步骤保留待发布验证。
只读请求见[OSS 探测](evidence/delivery-20261106/oss-publication-probes.json)。

两台现场主机都是 amd64。双架构索引、package 和运行镜像的交付结构由全量
qualification 验证，原生 arm64 主机执行仍待补充。

## 同候选 CI

以下四条运行均为同一候选提交，状态为 success。

- 全量 Go 测试和 OCI contract。[运行记录](https://github.com/lixd/kubeclipper/actions/runs/42219481052)
- AIO 原生部署和 Fast E2E。[运行记录](https://github.com/lixd/kubeclipper/actions/runs/42219480925)
- 离线打包和发布约束验证。[运行记录](https://github.com/lixd/kubeclipper/actions/runs/42219481228)
- 完整多架构 qualification 和原生 import 往返验证。[运行记录](https://github.com/lixd/kubeclipper/actions/runs/42219480893)

## 最终保留状态

保留 `delivery-offline-4d8bf810` 集群和端口 5627 的本轮 Registry，Kubernetes 为
一个 Master、一个 Worker，KC 为一个 Server、两个 Agent。网络验收 namespace 和
冲突占位仓库已清理。

两台机器的本轮公网阻断表已撤销，`sh-dev-3` 的旧 peer 隔离表也已撤销。
公网阿里云 `/v2/` 返回预期认证挑战 401，共享 Registry 的 5003、5605、5607 以及
本轮 5627 健康检查均为 200。网络恢复证据分别位于
[-3](evidence/delivery-20261106/network-restored-lixd-dev-3.json)和
[-4](evidence/delivery-20261106/network-restored-lixd-dev-4.json)。

恢复网络后再次连续五轮 API 写读删通过，每轮约 0.7–3.7 秒，Pod 重启计数未增长。
最终两节点 Ready，压力 condition 均为 False，16 个系统 Pod Ready，平台 etcd 真实
KV 写读删通过。证据见[最终状态](evidence/delivery-20261106/final-state-proof.json)和
[最终稳定性](evidence/delivery-20261106/final-stability-proof.json)。
