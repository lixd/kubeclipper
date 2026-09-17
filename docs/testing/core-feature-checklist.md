# KubeClipper 核心功能测试清单（长期覆盖基准）

本文档是平台核心功能的**长期测试基准**：每轮验证（E2E、回归、发布前）都应以此清单为
起点圈定范围，测完回填状态。它不属于某一次测试计划，而是所有测试轮次共同维护的覆盖台账。

功能基线为 **OCI 交付架构合并后的 KubeClipper**。OCI Registry、Delivery Policy、
PackageInventory、PackagePlan 和 Agent 按 digest 消费制品属于平台正常工作方式，不作为一套
独立的“OCI 模式”重复列举。

## 使用说明

- **编号即身份**：本文已随 OCI 合并基线完成一次性重整；自本基线起，`x.y-NN` 一经分配
  不再变更或复用。功能下线时把状态标为 `🗑 废弃` 并保留编号，新增 Case 在小节末尾追加。
- **状态含义**：✅ 已实测通过 · ⚠️ 部分验证 · ❌ 未验证 · 🗑 废弃。
  状态必须来自**真实运行**，单测覆盖不算 ✅（可在备注注明 "unit-only"）。
- **轮次记号**：备注中 R1～R6 指验证发生的轮次；每轮详细证据记录在
  `docs/superpowers/issues/` 的轮次报告里，本文档只留结论与指针。
- 三机实测报告：R4 [`2026-09-17-core-feature-e2e-sh-dev-2-3-4.md`](../superpowers/issues/2026-09-17-core-feature-e2e-sh-dev-2-3-4.md)，
  R5 [`2026-09-17-core-feature-e2e-r5-sh-dev-2-3-4.md`](../superpowers/issues/2026-09-17-core-feature-e2e-r5-sh-dev-2-3-4.md)，
  R6 [`2026-09-17-core-feature-e2e-r6-sh-dev-2-3-4.md`](../superpowers/issues/2026-09-17-core-feature-e2e-r6-sh-dev-2-3-4.md)。
- **每轮测试的闭环动作**：① 圈定本轮要覆盖的编号 → ② 执行 → ③ 回填状态与轮次 →
  ④ 未覆盖项转入缺口文档 → ⑤ 新发现的功能面补编号。
- 当前缺口、优先级和待确认移除项统一维护在
  [`round-2026-09-gaps.md`](round-2026-09-gaps.md)，本清单不重复维护缺口摘要。
- **范围分层**：第 1～5 章是发布必须关注的核心功能；第 6 章是发布工程与可靠性门禁；
  第 7 章是扩展能力。扩展能力保留覆盖记录，但不能用其数量稀释核心主链路的通过率。

## 1. 平台本身的部署

### 1.1 部署物料来源

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 1.1-01 | 在线部署：默认 ghcr 直装 | ⚠️ | 预检✅；从未真从 ghcr 直装（都先 sync） |
| 1.1-02 | 半离线：ghcr → `kcctl registry sync` → 本地仓库 → deploy | ✅ | R3 全链路（5 copied/56 skipped 幂等） |
| 1.1-03 | 纯离线：bundle export → 拷贝 → import 进仓库 → deploy | ❌ | 脚本 CI 绿，真机未演练；必须在断公网环境验证 |
| 1.1-04 | 私有仓库 http | ✅ | R3 全程 :5003 |
| 1.1-05 | 私有仓库 https + 自签 CA | ❌ | |
| 1.1-06 | Package Registry 账号密码认证（deploy/join/agent 消费侧） | ❌ | sync 源认证✅，部署侧未测 |
| 1.1-07 | `registry sync` 重复同步幂等 | ✅ | R3：5 copied / 56 skipped；digest 不变 |
| 1.1-08 | 离线 bundle 重复 import 幂等 | ❌ | 不得以已验证的 sync 代替 import |
| 1.1-09 | 错误仓库地址、凭据、CA 或缺失制品 | ❌ | 变更前明确失败；修正后可重试；日志不泄露凭据 |
| 1.1-10 | HTTPS 私有仓库 + 公共 CA | ❌ | TLS 正常校验，不依赖 insecure 或 skip verify |

### 1.2 部署拓扑

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 1.2-01 | 单 server + 多 agent | ✅ | R2/R3，3 个 agent |
| 1.2-02 | server 与 agent/k8s 节点混部同机 | ✅ | R2/R3 |
| 1.2-03 | **多 server HA（3× server，etcd 奇数集群）** | ✅ | R4：sh-dev-2/3/4 三 Server、三 etcd、三 Agent 实测；etcd 成员均 started/non-learner，三端点写入和 `/healthz` 通过。运行包 revision 为 `1a70255b...`，见 R4 报告的版本边界 |
| 1.2-04 | console 组件部署与访问 | ⚠️ | 服务起✅，页面未验 |
| 1.2-05 | 单 server + 单 agent | ✅ | R2/R3，server/agent/k8s 节点混部 |
| 1.2-06 | server 与 agent 分离部署 | ⚠️ | 多机环境隐含覆盖，缺少独立验收证据 |
| 1.2-07 | HA server/etcd 单点故障与滚动重启 | ⚠️ | R4：停止/恢复 dev4 的 `kc-server`、`kc-etcd`；R6 在运行中 CreateCluster 期间停止 dev4 `kc-server`，Operation 仍成功且 API/quorum 可用。故障窗口 Watch、Console 尚未验证 |

### 1.3 容错与增量运维

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 1.3-01 | 预检失败引导（不可达/缺包报错） | ✅ | R2/R3 |
| 1.3-02 | etcd 冷启动竞态（写探针+重试） | ✅ | R3 |
| 1.3-03 | clean --all 后重 deploy 幂等 | ✅ | R3 ×2 |
| 1.3-04 | 不 clean 直接重复 deploy 的行为与平台安全性 | ⚠️ | R5：已有平台执行 `kcctl deploy` 在预检阶段明确拒绝，平台仍为 Healthy。已验证拒绝路径，不是重复 deploy 幂等成功 |
| 1.3-05 | `kcctl join` 独立纳管新节点 | ⚠️ | R6：dev4 空闲节点独立 join 成功，Package Registry HTTP 地址生效并恢复 Ready；重复 join、认证失败、自签 CA 和失败清理未测 |
| 1.3-06 | `clean --all --force --deploy-config` 异常恢复 | ❌ | 命令可用但 R4 未覆盖；应在 kc-server 不可达时使用本地 deploy-config 完成全量清理，并验证无半残服务 |
| 1.3-07 | **`kcctl upgrade all --pkg` 平台离线升级** | ❌ | 保留数据和配置，失败可恢复 |
| 1.3-08 | `kcctl doctor` | ✅ | R3/R4（R4：25 项） |
| 1.3-09 | **`kcctl upgrade all --online --version` 平台在线升级** | ❌ | 下载正确版本，升级后 doctor 通过 |
| 1.3-10 | `kcctl upgrade kcctl/agent/server/console` 组件独立升级 | ❌ | 只升级目标组件，版本和运行状态准确 |
| 1.3-11 | SSH key/password、非 root sudo 与自定义端口 | ⚠️ | 已使用部分 SSH 配置；需分别验证认证失败、sudo 失败和修正后重试 |
| 1.3-12 | 初始化管理员密码 | ❌ | 自定义初始密码可登录；敏感值不出现在配置回显和日志中 |

## 2. 集群相关操作

### 2.1 创建集群

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.1-01 | 1 master + 2 worker | ✅ | R3 |
| 2.1-02 | 单节点（master 兼 worker）最小规格 | ✅ | R5：`aio-core-20260917` 为 1 Master/0 Worker；Node Ready、CoreDNS/Calico、API、DNS 和 Pod 烟测通过，删除后节点可复用 |
| 2.1-03 | 3 master + worker HA（lvscare workerNodeVip） | ✅ | R2 已验证 3M+Worker；R4 另验证 3M/0W 边界（需 untaint 才能调度），不替代有 Worker 的 HA 验收 |
| 2.1-04 | 版本矩阵 v1.35.8 / v1.36.4 / v1.37.0 | ✅ | R2/R3 |
| 2.1-05 | CRI containerd 1.7.29 / 2.2.4 | ✅ | R2/R3 |
| 2.1-06 | CNI calico v3.29.6 / v3.31.5 | ✅ | R2/R3 |
| 2.1-07 | 镜像/物料 100% 来自指定仓库（离线保证） | ✅ | R3 验证法可复用 |
| 2.1-08 | proxyMode ipvs | ✅ | R2/R3 |
| 2.1-09 | proxyMode iptables | ❌ | |
| 2.1-10 | 创建集群时拒绝支持矩阵外版本 | ✅ | R3；同版本/降级属于升级校验，见 2.3-08 |
| 2.1-11 | 网络自定义（pod/service 网段、DNS 域） | ✅ | R3 即用即验（172.25/16 + cluster.local） |
| 2.1-12 | apiserver 对外发布（cert-sans / external-domain / external-ip / external-port） | ⚠️ | R6：合法参数落库，apiserver 证书 SAN 含外部 IP/域名，kubeconfig 使用 external-ip；域名 DNS/代理端口连通性未验证 |
| 2.1-14 | untaint-master（master 允许调度） | ✅ | R5：AIO 创建使用 `--untaint-master`，CoreDNS 和测试 Pod 均成功调度到 Master |
| 2.1-16 | 自带 CA（ca-cert / ca-key 复用已有根证书） | ❌ | |
| 2.1-18 | Calico 默认 VXLAN 网络模式 | ✅ | R4：集群对象为 `Overlay-Vxlan-All`，1M1W 与 3M/0W 均完成跨节点 Pod ping、Service DNS 和 apiserver 访问 |
| 2.1-19 | Calico IPIP/BGP 或 cross-subnet 网络模式 | ✅ | R5：`Overlay-Vxlan-Cross-Subnet` AIO 建群和网络烟测通过；非法网络模式在创建前被 CLI 拒绝，未留下 Cluster |
| 2.1-20 | Calico IPv4 自动探测（first-found/interface/can-reach） | ✅ | R4 覆盖 `interface=ens3`；R5 覆盖 `first-found` 与 `can-reach=172.16.131.146`，各自完成 1M1W 建群、配置落库、跨节点 Pod ping 和删除 |
| 2.1-21 | 集群镜像 Registry 与 Package Registry 分离配置 | ❌ | Kubernetes/CNI 镜像源与 OCI package 源各自生效，不得串用 |
| 2.1-22 | 私有 CRI Registry 配置下发 | ❌ | HTTP、认证、自签 CA 配置正确下发到 containerd，Pod 可拉取镜像 |
| 2.1-23 | 集群真实断网创建 | ⚠️ | R3 已验证指定仓库来源；缺少网络封锁证据，不标记为完全离线通过 |
| 2.1-24 | 1 master + 1 worker 最小多节点规格 | ✅ | R4：`min-core-20260917` 两节点 Ready，跨节点 Pod 网络通过，删除后平台 3/3 Agent 恢复健康 |
| 2.1-25 | 集群在线安装 | ❌ | 与平台在线部署分开验证；目标集群按配置在线获取所需物料 |
| 2.1-26 | 节点已被其他集群占用或 Master/Worker 重复 | ✅ | R5：已占用节点创建前拒绝且无新 Operation/Cluster；R6：同一 IP 同时指定 Master/Worker 返回 `master and worker conflict`，无对象残留 |
| 2.1-27 | Master/Worker 跨 Region | ❌ | 按当前同 Region 约束拒绝，并指出冲突节点和 Region |
| 2.1-28 | Pod/Service CIDR 非法、重叠或与主机网络冲突 | ❌ | R6：`10.96.0.0/16` 与 `10.96.0.0/12` 被接受并创建 Installing Cluster，未在创建前拒绝；健康检查未收敛 |
| 2.1-29 | 端口、磁盘、时间同步、主机名等集群预检失败 | ⚠️ | R6：非法 external/domain 端口和域名均在 CLI 前置拒绝且无对象；磁盘、时间同步、主机名等主机检查未覆盖 |
| 2.1-30 | 创建中断后的 retry 或安全删除 | ⚠️ | R6：取消 CIDR 创建后 Cluster/Operation、节点标签和主机副作用未自动清理，需 reset/精确清理；retry 未验证 |
| 2.1-31 | 创建成功后的固定健康验收 | ✅ | API Server、etcd、controller、scheduler、CoreDNS、CNI、kube-proxy、Node Ready |

### 2.2 节点操作

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.2-01 | worker 添加（含 packagePlan 不变性） | ✅ | R2/R3 |
| 2.2-02 | worker 移除（含不可 drain 容错） | ✅ | R2/R3 |
| 2.2-03 | **master 添加 / 移除** | ❌ | R4 尝试 Master add 被 API 以 `invalid node role` 拒绝；当前实现不支持该路径，未安排 Master E2E |
| 2.2-04 | master↔worker 角色转换（convertNodes） | ❌ | |
| 2.2-05 | 节点 disable / enable | ✅ | R3/R4：节点从集群移除后执行 disable/enable，HTTP 200，禁用标签出现并清除；尚未验证仍被其他集群占用时的保护 |
| 2.2-06 | 节点失联后操作收敛（agent down） | ⚠️ | R2 自然样本，无系统注入 |
| 2.2-07 | agent 节点注销（`DELETE /nodes/{name}`） | ⚠️ | R6：空闲 dev4 通过 `kcctl drain` 删除 Node 后独立 join 恢复；集群占用保护、Lease、证书残留和自动重注册未完整验证 |
| 2.2-08 | Worker 添加时复用原 `status.packagePlan` | ✅ | R2/R3；各 slot digest 不随 Registry tag 漂移 |
| 2.2-09 | Master 添加后 etcd/control-plane quorum | ❌ | 新成员健康，API 高可用，packagePlan 不漂移 |
| 2.2-10 | Master 移除后的 etcd 成员与 VIP 收敛 | ❌ | 不破坏 quorum；成员、证书和负载入口无残留 |
| 2.2-11 | Agent 注册身份与 Node 状态更新保护 | ❌ | Agent 只能注册/更新自身 Node；UID/resourceVersion 不匹配应拒绝 |
| 2.2-12 | Node Lease、Ready/Unknown 与重连恢复 | ⚠️ | R2 有自然掉线样本；需验证超时、恢复及列表状态一致性 |
| 2.2-13 | Region 归属、列表和同 Region 调度约束 | ⚠️ | Region 基础接口存在；多 Region 完整场景未验证 |

### 2.3 升级与证书

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.3-01 | 真实滚动升级 1.36.4→1.37.0（master→worker drain） | ✅ | R2 + R3 新 tip 复验（修 3 bug） |
| 2.3-02 | 升级中途失败 → op retry | ❌ | |
| 2.3-03 | 升级失败 → 集群状态恢复（reset status） | ✅ | R3 实际使用 |
| 2.3-04 | **集群证书更新（/certification）** | ⚠️ | R4：`UpdateCertifications` Operation 成功且集群回到 Running，但 `status.certifications` 为空，尚无 serial/有效期前后对比，不能标满通过 |
| 2.3-05 | agent 证书重新签发 | ⚠️ | 依赖 join 测试 |
| 2.3-06 | 升级前后 `packagePlan` 变更边界 | ⚠️ | R3 升级已通过；需确认只更新目标版本相关 slot，其他 digest 不漂移 |
| 2.3-07 | Registry tag 变化后 Operation retry 仍使用原 digest | ❌ | retry 必须复用 Task/Plan 固定引用，不重新解析 tag |
| 2.3-08 | 同版本、降级、跨越不支持版本升级拒绝 | ✅ | R3；必须在创建升级 Operation 前失败 |
| 2.3-09 | Master/Worker 滚动顺序与业务可用性 | ⚠️ | R3 升级成功；需固定验证顺序、drain、PDB 和服务连续性 |

### 2.4 删除集群

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.4-01 | 正常删除（含与 SyncKubeConfig 并发死锁回归） | ✅ | R2/R3；应固定验证 K8s 组件清理、平台状态和节点可复用 |
| 2.4-02 | InstallFailed 状态删除 | ✅ | R3 |
| 2.4-03 | 删除失败 TerminateFailed → 重试 | ✅ | R2 |
| 2.4-04 | 有备份时删除保护（引导先删备份） | ✅ | R3 |
| 2.4-05 | 删除后 ops 账本清理 | ✅ | R3 |

### 2.5 备份与恢复

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.5-01 | backuppoint fs 型（含非法类型拒绝） | ✅ | R3；R5 另复验 FS 手动备份 |
| 2.5-02 | backuppoint **S3 型（MinIO）** | ✅ | R5：备份 Operation 成功、Backup 为 Available、对象已写入 MinIO；删除 Backup 后 API 记录和 S3 对象均消失 |
| 2.5-03 | 手动备份 → 恢复（marker 回滚证明） | ✅ | R2/R3 |
| 2.5-04 | 备份删除（连带存储文件清除） | ✅ | R3（FS）；R5 另验证 S3 对象随 Backup 删除 |
| 2.5-05 | cronbackup runAt 单次触发 | ✅ | R3 |
| 2.5-06 | cronbackup 真实周期命中 | ✅ | R5：分钟级 Cron 实际创建 Backup，不只检查 next schedule 滚动 |
| 2.5-07 | cronbackup enable / disable 子资源 | ✅ | R5：enable/disable 均生效；禁用期间不再创建，重新启用后恢复调度 |
| 2.5-08 | **maxBackupNum 超限自动轮转** | ❌ | R5：`maxBackupNum=1` 只轮转 Backup 对象，旧 FS 备份文件仍残留；需连带清理 FS/S3 存储对象 |
| 2.5-09 | 恢复后集群可用性（addons/节点完整） | ✅ | R3 |
| 2.5-10 | 备份损坏或错误 S3/FS 凭据 | ❌ | 明确失败，不产生 Available 假状态，不破坏原集群 |
| 2.5-11 | Backup 详情查询 API（`GET /backups/{name}`） | ❌ | R5：已有 Backup 仍返回 404；列表和集群范围查询正常。当前 `DescribeBackup` 把 path name 和 resourceVersion 错传给 `GetBackupEx`，需修复并补成功/不存在用例 |

### 2.6 OCI 制品解析、缓存与消费

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.6-01 | 创建集群解析并持久化 `status.packagePlan` | ✅ | R2/R3；Kubernetes/CRI/CNI/extension 使用 `repository@sha256` |
| 2.6-02 | packagePlan 不保存 blob 或 Registry 凭据 | ⚠️ | 代码/对象检查需形成固定证据 |
| 2.6-03 | Agent 首次按 digest 拉取、校验、解包和执行 | ✅ | R2/R3 建群主路径 |
| 2.6-04 | Agent 命中本地校验缓存 | ⚠️ | 隐含覆盖，需日志证明相同 digest 不重复下载且缓存有效 |
| 2.6-05 | 缓存损坏或 digest 不符 | ❌ | 不得执行损坏制品；重新拉取或明确失败；不得回退到 tag |
| 2.6-06 | Registry 暂时不可达时的缓存行为 | ❌ | 已缓存 digest 可继续，未缓存 digest 明确失败 |
| 2.6-07 | Package Registry 配置优先级与文件权限 | ❌ | flag/deploy config/default 优先级明确；敏感配置权限 0600 |
| 2.6-08 | Delivery Policy 默认策略初始化 | ✅ | R3 默认策略已实际用于制品解析 |
| 2.6-09 | Delivery Policy 自定义版本白名单 | ✅ | R6：白名单内 v1.37 建群成功；白名单外 v1.36 在创建 Operation 前拒绝且无对象；策略精确恢复 |
| 2.6-10 | Delivery Policy 缺失 slot/repository | ⚠️ | R6：移除 `cni` slot 后创建前报 `UnsupportedComponentSlot` 且无 Cluster/Operation；缺失 repository/blob/冲突选择未测 |
| 2.6-11 | bootstrap/standalone extension 与集群 packagePlan 隔离 | ⚠️ | 单测/代码有门禁，缺真实命令和对象证据 |

## 3. `kcctl` 命令覆盖（每条至少跑通一次核心路径）

> 命令面以待测版本的 `kcctl --help` 为准。平台 Registry 资源、独立 Docker Registry 管理、
> OCI package Registry 是三个不同概念，测试记录中必须写清目标对象。

| 编号 | 命令/用法 | 状态 | 最小验收点 |
|---|---|---|---|
| 3-01 | `kcctl deploy` | ⚠️ | 在线、同步仓库、纯离线分别记录；目前纯离线未通过，不能整体标 ✅ |
| 3-02 | `kcctl clean --all` | ✅ | R3；清理后可重部署。当前不支持单节点/按角色 clean |
| 3-03 | `kcctl doctor` | ✅ | R3/R4；R4 为 25 项，异常项、节点和退出码准确 |
| 3-04 | `kcctl join` | ⚠️ | R6：dev4 独立 join 成功并 Ready；重复 join、错误凭据、Package Registry 认证/CA 失败路径未测 |
| 3-05 | `kcctl create cluster` | ✅ | R4：CLI 实建 `ha-core-20260917`（3M/0W）和 `min-core-20260917`（1M/1W），参数与 packagePlan 落库；3M/0W 需显式 untaint 才能调度 CoreDNS |
| 3-06 | `kcctl create/delete user`、`role` | ❌ | CRUD、重复名称、绑定关系和错误退出码 |
| 3-07 | `kcctl create/delete registry` | ✅ | 管理平台中的集群镜像 Registry 资源；基础 CRUD 已验证 |
| 3-08 | `kcctl delete cluster` | ✅ | R4：CLI 删除 `ha-core-20260917`、`min-core-20260917` 均返回成功并最终 NotFound；删除后平台 doctor 仍为 Healthy |
| 3-09 | `kcctl get cluster/node/user/role/configmap/registry` | ⚠️ | cluster/node 基础查询已用；其余资源、输出格式和 selector 需逐项扫尾 |
| 3-10 | `kcctl get --watch` | ❌ | R4 复测：`kcctl get cluster -w`/列表形式均一次输出后 rc=0 退出；`pkg/cli/get/get.go` 只声明 Watch flag，未传入 query 或建立 watch 流，长连接/断线恢复未实现 |
| 3-11 | `kcctl operation list/describe/logs/retry` | ✅ | list 按集群筛选；logs follow 增量不重复；retry 终态限制正确 |
| 3-12 | `kcctl operation cancel` | ⚠️ | R5：`e290a5f7-ddba-4994-b3d3-8bf03eb088af` 重启 Server 后才 Canceled；R6 CIDR 创建取消后又出现孤立 Running Operation/Installing Cluster，需人工清理 |
| 3-13 | `kcctl cluster upgrade` | ✅ | R3；参数传递、滚动顺序和最终状态正确 |
| 3-14 | `kcctl set cluster` | ✅ | R3/R4：external IP/port 设置与 clear 均成功，get 输出中的 labels 随之出现/清除 |
| 3-15 | `kcctl drain` | ⚠️ | R6：空闲 Agent drain rc=0 并删除 Node；当前命令只支持 KubeClipper Agent，不是 Kubernetes Pod eviction，used/force/重复执行未测 |
| 3-16 | `kcctl upgrade all --pkg/--online` | ❌ | 平台离线/在线升级，配置数据保留，失败可恢复 |
| 3-17 | `kcctl upgrade kcctl/agent/server/console` | ❌ | 各组件独立升级，未选组件不受影响 |
| 3-18 | `kcctl registry sync` | ✅ | R2/R3；Release Manifest、首次/增量同步、认证和 digest 一致 |
| 3-19 | `kcctl registry list/deploy/clean/push/delete` | ⚠️ | R6：共享 Registry list/image、缺失仓库和非法 push 已测；无 Docker Engine 且不能占用共享 Registry，valid push 与 deploy/clean/delete 未测 |
| 3-20 | `kcctl resource list` | ✅ | R3/R4：针对 `172.16.131.146:5003` 列出 14 个 OCI package，结果含仓库/版本/平台信息 |
| 3-21 | `kcctl resource inspect` | ✅ | R3/R4：指定 package 可展示 repository、tag、digest、platform 和内容画像 |
| 3-22 | `kcctl resource refresh` | ✅ | R3/R4：强制扫描指定 Registry，输出 `refreshed 14 OCI packages`；不将其解释为持久缓存更新 |
| 3-23 | `kcctl delivery-policy template/get/apply/diff` | ✅ | R4：模板生成、custom `allowedVersions` apply/get、diff 和恢复均通过；策略文件的非法 selection 被拒绝 |
| 3-24 | `kcctl delivery-policy validate` | ⚠️ | R4：合法模板通过，`selection: many` 被拒绝；未知 slot 名称当前按可扩展字段接受，白名单制品存在性/冲突尚未覆盖 |
| 3-25 | `kcctl login` | ⚠️ | R3/R4 只验证错误密码返回 unauthorized、rc=1 且不落配置；正确登录、token 过期及 TLS 校验仍未完成 |
| 3-26 | `kcctl status` | ✅ | R4：准确报告三台 `kc-server`、三台 `kc-etcd`、三台 `kc-agent`，平台 Healthy |
| 3-27 | `kcctl deploy config` | ⚠️ | 生成配置、读取、覆盖优先级和非法配置；不存在顶层 `kcctl config` |
| 3-28 | `kcctl version` | ✅ | client/server 版本和 Git revision 准确 |

## 4. 其他核心平台功能

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 4-01 | addons：nfs-csi 安装/动态供给读写/卸载 | ✅ | R2/R3 |
| 4-02 | addons：metallb L2 安装/卸载 | ✅ | R3 |
| 4-05 | addons：uninstall 容错（空 config / ErrIgnore 链） | ✅ | R3 三修复合验 |
| 4-06 | 可观测：operation logs、失败原因展示（API 侧） | ✅ | R3 |
| 4-07 | **console UI 端到端（含任务失败展示）** | ❌ | fork console 分支未配镜验证 |
| 4-08 | 用户 / 角色 CRUD、enable/disable、改密、登录记录 | ❌ | 基础身份管理属于核心平台能力 |
| 4-08b | RBAC 鉴权拦截（非管理员越权应 403） | ⚠️ | R6：内置只读用户可读 Cluster，创建 Registry 返回 403；自定义 role binding/删除保护和 WebSocket 一致性未闭环 |
| 4-08c | 密码登录与验证码登录 | ⚠️ | R6：正确密码登录路径已成功；验证码过期/重复使用、失败限流及第三方 OAuth 未测 |
| 4-08d | 长期 token 创建、查询、撤销与过期 | ❌ | token 不得出现在普通日志和审计正文 |
| 4-12 | kubeconfig 下载 | ⚠️ | 文件可用、权限正确；集群未就绪或凭据过期时明确失败 |
| 4-13 | 平台自省：/configz、/status、/components、/componentmeta | ✅ | R6：管理员 mTLS 直查四个 endpoint 均 200，status 返回 Healthy |
| 4-14 | 审计事件查询（/events，auditing 组） | ✅ | R6：列表与详情 200，不存在事件 404；分页参数已带 limit/page |
| 4-16 | 敏感信息脱敏 | ❌ | CLI、server、agent、Operation 和审计日志不泄露密码、token、CA key |
| 4-17 | `/healthz` 与 `/metrics` | ⚠️ | R6：`/healthz` 和 `/metrics` 均 200，108 行指标未命中 password/token/secret/private-key；标签约束未专项验证 |
| 4-18 | 登录失败限流与恢复 | ❌ | 连续错误密码触发限流；窗口结束或成功登录后行为符合设计 |
| 4-19 | Addon OCI chart/runtime-image-set 来源与 digest | ✅ | R2/R3；NFS CSI、MetalLB 主路径来源检查可复用 |
| 4-20 | Addon 安装失败后的 retry/卸载清理 | ❌ | 状态准确；已创建资源可重试或安全清理 |

## 5. Operation V2 核心可靠性

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 5-01 | 创建、扩缩、升级、备份、恢复使用 Operation V2 | ✅ | R2/R3 主路径；Operation/Task/ExecutionLock 可追踪 |
| 5-02 | Operation/Task 展示节点 UID、IP、步骤和错误 | ✅ | R3 排障实际使用 |
| 5-03 | Server/Agent 重启后的 Operation 恢复 | ✅ | R2/R3；已完成 Task 不重复，锁最终释放 |
| 5-04 | Agent Watch 410/EOF 后 relist | ✅ | R2 有真实 410 样本，不漏任务、不重复并发执行 |
| 5-05 | 多节点 Step barrier 与输出传递 | ⚠️ | 单测存在，真机故障注入不足 |
| 5-06 | Operation cancel | ⚠️ | R5：需重启 `kc-server` 才归约到 Canceled；R6 取消 CIDR 创建后留下孤立 Cluster/Operation 并需精确清理，自动收敛和锁释放仍未闭环 |
| 5-07 | 自动 retry 与人工 retry | ⚠️ | retry 已实测；副作用安全分类、generation 和 digest 固定仍需专项验证 |
| 5-08 | 同集群危险操作互斥 | ⚠️ | ExecutionLock 已实现，需覆盖 create/add/upgrade/backup/delete 组合 |
| 5-09 | Task 终态写入成功但响应丢失 | ⚠️ | 单测覆盖；真机网络注入未做，不得重复执行 executor |
| 5-10 | 无法确认副作用时 fail-closed | ❌ | 保持互斥并转人工，不得猜测成功或自动重放 |
| 5-11 | Operation API `limit/continue` 稳定游标分页 | ✅ | R3 API 已验证；无重复遗漏。当前 `kcctl operation list` 未暴露分页参数 |
| 5-12 | Task 日志 offset 与 Task 切换 | ⚠️ | API/单测有覆盖；真机需验证增量不重复、切换 Task 不串日志 |
| 5-13 | 相同业务请求重复提交 | ❌ | 不得创建可并行产生重复副作用的 Operation；返回已有结果或明确拒绝 |
| 5-14 | Operation/Task timeout | ❌ | 超时后 Task、进程、Cluster 状态和 ExecutionLock 按既定语义安全收敛 |

## 6. 发布工程与交付门禁

> 本章不是用户功能菜单，但决定发布物能否被前述核心链路可靠消费。CI/单测通过只记 ⚠️，
> 只有发布候选制品被真实部署和使用后，才能把相应端到端结果记为 ✅。

| 编号 | 门禁 | 状态 | 备注 |
|---|---|---|---|
| 6-01 | 支持策略与 `packaging/resources.yaml` 一致 | ⚠️ | `release-policy-verify` CI 已覆盖，需保留发布候选证据 |
| 6-02 | bootstrap、Kubernetes、CRI、CNI、extension、addon OCI 制品完整 | ⚠️ | 构建/发布 CI 通过；真实消费分别见第 1、2、4 章 |
| 6-03 | Release Manifest 包含 package、chart、runtime image 和 bootstrap | ⚠️ | qualification CI 覆盖 |
| 6-04 | digest、source、revision、version provenance 正确 | ⚠️ | 篡改或来源不一致必须阻断发布 |
| 6-05 | 制品不可变性与重复发布保护 | ⚠️ | 相同 repo:tag 不得被静默覆盖 |
| 6-06 | amd64/arm64 Manifest 与架构过滤 | ⚠️ | amd64/all 有 CI；arm64 真机需复验 |
| 6-07 | Registry sync 与目标仓库消费 | ✅ | R2/R3 真实同步并用于部署；每个发布候选仍需保存证据 |
| 6-08 | 完整 qualification 发布 | ⚠️ | Workflow 已实现；输出必须能完成真实部署和建群 |
| 6-09 | linux/amd64 主路径 | ✅ | 当前主要实测架构；覆盖平台部署、建群、升级和删除 |
| 6-10 | linux/arm64 主路径 | ⚠️ | 有构建和历史使用记录，缺当前 OCI 基线整轮证据 |
| 6-11 | Tier 1 OS 矩阵 | ⚠️ | 需先固定正式支持 OS 清单，再逐项跑部署、建群和删除 |
| 6-12 | Docker CRI 废弃入口清理 | ❌ | 当前 CLI 仍接受 `--cri docker`，与正式支持矩阵冲突；应从 help、校验、策略和运行分支移除 |
| 6-13 | legacy static server 与 `nfs-provisioner` 不再暴露 | ⚠️ | 代码/发布清单已有静态门禁；需检查部署进程、默认策略和 Console 无旧入口 |

## 7. 扩展能力

> 以下能力需要测试，但不计入平台部署、建群和生命周期管理的核心通过率。若某次发布明确承诺
> 其中一项，则应把该项提升为当次发布的阻断用例。

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 4-03 | addons：metallb BGP | ❌ | 需邻居 AS 环境 |
| 4-09 | 集群模板 templates | ❌ | CRUD、实例化和引用删除保护 |
| 4-10 | DNS domains / records | ❌ | 域名、记录 CRUD 与非法值拒绝 |
| 4-11 | cloudproviders / 外部集群纳管 | ❌ | kubeconfig 预检、同步、异常和移除 |
| 4-12a | Web 节点/集群终端 | ❌ | 鉴权、窗口 resize、断连和重连 |
| 4-12b | Pod exec | ❌ | namespace/pod/container 选择、鉴权和断连 |
| 4-15 | PlatformSetting（镜像仓库模板、Web 终端密钥） | ❌ | CRUD、权限、持久化和敏感字段保护 |
| 7-01 | 第三方 OAuth/OIDC 登录 | ❌ | 回调、用户映射、token 过期和登出 |
| 7-02 | `kcctl completion` | ❌ | bash/zsh/fish 生成结果可加载 |
| 7-03 | 多节点并发任务基础容量 | ❌ | 先定义目标规模和资源上限，再执行性能验证 |
| 2.1-13 | Kubernetes feature-gates 透传 | ⚠️ | R1 验过 20 项；R3 未复跑 |
| 2.1-15 | `--only-install-kubernetes-component` 跳过 CNI | ❌ | 自带网络场景；安装第三方 CNI 后恢复 Ready |
| 2.1-17 | kubeadm preflight ignore 定制 | ⚠️ | R2 隐含使用，无显式用例 |
| 4-04 | Addon 同组件多实例 | ❌ | 以 StorageClassName/实例名区分，状态和卸载互不影响 |
