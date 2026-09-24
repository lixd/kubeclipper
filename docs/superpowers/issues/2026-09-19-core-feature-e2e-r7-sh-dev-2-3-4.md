# KubeClipper R7 核心功能实测报告：sh-dev-2/3/4（新 OCI 候选 e9e95f4）

日期：2026-09-19

状态图例：✅ 已实测通过 · ⚠️ 部分验证或发现收敛缺陷 · ❌ 已复现失败

本轮目的：R4～R6 的运行包 provenance 停留在 `1a70255b`，不能代表当前分支。本轮按发布审计
（B6）要求，**用当前 OCI 分支 HEAD `e9e95f4` 重新走完“CI 打包 → Registry 同步 → 清理旧环境 →
重新部署 → 完整可执行矩阵”**，并复测审计确认未修复的缺陷（B2/B3/B4 相关 Case）。

## 1. 版本与 provenance（本轮核心差异）

| 项目 | 实测值 |
|---|---|
| Git 分支 | `feat/oci-operation-v2-migration` |
| 候选 revision | `e9e95f4cff4ee49fc396b4a35db09f6739b2fef6`（含 Addon/ErrIgnore/升级 registry-conversion/CronBackup 等 5 个 `1a70255b` 之后的源码修复） |
| 打包方式 | 推送 tag `oci-qualification-20260919` → GitHub Actions `publish-oci-qualification` run [35413305174](https://github.com/lixd/kubeclipper/actions/runs/35413305174) **success**（含 manifest 与 sync-roundtrip） |
| Release Manifest | sourceRevision=`e9e95f4c...`，version=v2.0.0，61 个 artifact（amd64） |
| 测试 Registry | `172.16.131.146:5003`（aio-img-reg）：旧 `kubeclipper/*` 包按 digest 删除后重新同步；最终 `kcctl registry sync` 返回 `0 copied, 61 skipped (already present with matching digest)` |
| kcctl（客户端） | `v1.7.0-72+e9e95f4c`，GitCommit `e9e95f4c...`（本地交叉编译，分发至三机 `/tmp/kcctl-r7`） |
| Server/Agent 运行包 | `v2.0.0`，**GitCommit `e9e95f4c...`**（部署后 `kcctl version` 实测，替代 R4～R6 的 `1a70255b`） |
| 主机 | sh-dev-2/172.16.131.208、sh-dev-3/172.16.131.146、sh-dev-4/172.16.131.230 |
| 集群物料 | Kubernetes v1.35.8/v1.36.4/v1.37.0、containerd 1.7.29/2.2.4、Calico v3.29.6/v3.31.5 |

同步路径备注：三机到 ghcr 的公网出口仅 ~17KB/s（blob PATCH 10 分钟仅传 10MiB 后断开，
PROTOCOL_ERROR/unexpected EOF），`registry sync` 直连 ghcr 无法完成大 blob。实际采用
「Mac 下载字节级 manifest+blobs → LAN 传输 → dev-3 本机 curl 推送（POST→PUT 单体上传）→
逐 tag digest 校验」完成，再跑官方 `registry sync` 获得幂等通过记录。同步计划与三次运行日志
保存在 `sh-dev-2:/tmp/kc-evidence-20260919/r7-01-*.txt`。

## 2. 执行前基线与清理

三机原平台（`1a70255b` 运行包）状态 Healthy、无集群、无 Operation。执行：

1. 保存基线证据（status/doctor/version/cluster/node/operation，权限 0600）；
2. `kcctl clean --all -y`；三机 kc-* 服务全部 inactive，`/var/lib/{etcd,kubelet}`、`/etc/cni`、
   `/root/.kc` 均无残留；
3. 人工清理 dev-3 两处旧残留：空目录 `/etc/kubernetes/pki`、已 dead 的 `kc-registry.service`
   旧 unit（r3 共享 Registry `kc-oci-r3-registry`:5003 全程保留，caas4/* 等其他物料未动）。

## 3. 平台部署与验收（新候选）

`kcctl deploy -y --pk-file ... --server ×3 --agent ×3 --package-registry 172.16.131.146:5003
--package-registry-scheme http --ip-detect cidr=172.16.131.0/24 --node-ip-detect cidr=...`：

- `kcctl version`：server/agent GitCommit = `e9e95f4c...` ✅（1.2-03、3-01、3-28）；
- status 3/3/3 Healthy（3-26）；doctor `25 passed, 0 warnings, 0 failed`（1.3-08、3-03）；
- 三台 `https://<ip>:8080/healthz` 均为 `ok`；
- etcd 3 成员 started、非 learner（客户端端口 12379）。

## 4. 测试结果

### 4.1 部署容错与 HA

| Case | 结果 | 证据 |
|---|---|---|
| 1.3-04 重复 deploy 拒绝 | ✅ | 预检报 `kc-etcd.service already exists`，平台保持 Healthy |
| 1.2-07 运行中 Operation + dev4 kc-server 停止 | ✅ | CreateCluster `73f098ed` 运行中停止 dev4 kc-server；dev2/dev3 `/healthz` ok、`operation list` 可用、Operation 未失败；恢复后 3/3 |
| 1.2-07 dev4 kc-etcd 停止 | ✅ | doctor 正确报 `2/3` + FAIL 项；dev2/dev3 endpoint 写入提交成功；恢复后 25/25 |

### 4.2 集群生命周期

| Case | 结果 | 证据 |
|---|---|---|
| 2.1-24 1M+1W + 2.1-31 健康验收 | ✅ | `r7-min-20260919`（v1.37.0/2.2.4/v3.31.5/interface=ens3）Running；节点 Ready、系统 Pod 正常、DNS→10.96.0.1、跨节点双向 ping 0% 丢包 |
| 2.1-18 默认 VXLAN | ✅ | 同上，`Overlay-Vxlan-All` |
| 2.1-08 proxyMode ipvs | ✅ | kube-proxy ConfigMap `mode: ipvs` |
| 2.1-04/05/06 版本矩阵抽测 | ✅ | `r7-matrix-135-20260919`：v1.35.8 + containerd 1.7.29 + calico v3.29.6，节点实测 runtime `containerd://1.7.29`，烟测通过 |
| 2.1-20 can-reach 探测 | ✅ | Cluster `IPv4AutoDetection: can-reach=172.16.131.146` 落库 |
| 2.1-02/14/19 AIO + untaint + cross-subnet + first-found | ✅ | `r7-aio-20260919`：污点被移除、CoreDNS 调度到 Master、`Overlay-Vxlan-Cross-Subnet` + first-found 落库、烟测通过 |
| 2.1-26 占用节点拒绝 | ✅ | `some nodes in used or disabled`，无对象 |
| 2.1-29 前置拒绝组 | ✅ | master/worker 同 IP、external-port 70000、非法 domain、未知 image-registry、`--cri docker`（`unsupported cri version,support [] now`）均 rc=1 且无对象 |
| 2.1-28 CIDR 重叠创建前校验 | ❌ **未修复** | `pod-subnet 10.96.0.0/16` + `service-subnet 10.96.0.0/12` 仍被接受并创建 Installing 集群（按约定未执行破坏性全流程；该集群经 cancel→delete 收敛清理，见 §6） |
| 2.1-12 external apiserver | ⚠️ 未完成 | 两轮尝试均被 §5.2 默认版本缺陷阻断，未取得新证据；维持 R6 状态 |
| 2.1-25 在线安装 | ⚠️ 未完成 | 创建卡 Installing 后清理；当日环境到 registry.k8s.io 不可达，无法区分产品问题与网络条件 |

### 4.3 节点操作

| Case | 结果 | 证据 |
|---|---|---|
| 2.2-01 worker 添加 | ✅ | API `PUT /clusters/{name}/nodes`（add），Operation `35b749ef` Succeeded，dev4 K8s Node Ready |
| 2.2-08 packagePlan 不变性 | ✅ | 添加前后 4 个 slot digest 完全一致（`1d3da4e0`/`6f8242b2`/`9b5aa9e4`/`c431adcc`），且 AddNodes Operation 内嵌 ResolvedArtifactPlan 使用相同 digest |
| 2.2-02 worker 移除 | ✅ | Operation `d8e1358c` 20 秒收敛，节点从集群移除 |
| 2.2-03 master 添加拒绝 | ✅ | API 返回 400（invalid node role 边界） |
| 2.2-05 节点 disable/enable | ✅ | `PATCH /nodes/{name}/disable|enable` 均 200；禁用期间建群被拒（disabled 路径生效）；注意 label 实际键为 `kubeclipper.io/nodeDisable` |
| 2.2-07/3-04 drain + join | ✅ | `kcctl drain` 后 Node NotFound；`kcctl join --package-registry 5003` 重新纳管，3/3 恢复 |
| 2.3-05 Agent 证书重签 | ✅ | serial `33644819B666ED24`（旧 Node 4db3b6d1）→ `35C8E2A87BEB075B`（新 Node 049e40a2），CN 与新 Node 一致 |

### 4.4 升级与证书

| Case | 结果 | 证据 |
|---|---|---|
| 2.3-08 同版本/降级拒绝 | ✅ | `already at version` / `cannot downgrade` 均在创建 Operation 前拒绝 |
| 2.3-01 滚动升级 1.36.4→1.37.0 | ✅ | ~90 秒完成，两节点 v1.37.0 Ready，集群 Running |
| 2.3-04 证书更新 | ✅ | apiserver serial `2F3488595FA57564`→`3E1B08278426788C`，有效期刷新一年，节点保持 Ready |

### 4.5 备份与恢复

| Case | 结果 | 证据 |
|---|---|---|
| 2.5-01 FS backuppoint + 手动备份 | ✅ | `available`，11,440,160 bytes（需先在集群节点建 `/var/kc-backups`，报错信息已明确指引） |
| 2.5-02 S3 备份 | ✅ | 临时 S3 目标改用 seaweedfs（MinIO 官方已停止分发二进制），备份 `available` 同尺寸；endpoint 必须为裸 host:port，见 §5.8 |
| 2.5-03 恢复 | ✅ | 恢复 Operation `756c1de8` Succeeded；备份后创建的 marker ConfigMap 恢复后 NotFound，节点 Ready |
| 2.5-04 备份删除连带清理 | ✅ | 删除 Backup 对象后 FS 文件同步消失 |
| 2.5-05/06 Cron 单次/周期触发 | ✅ | 每分钟 schedule 实际触发多次（05:26～05:29） |
| 2.5-07 enable/disable | ✅ | 禁用 90 秒无新增，重新启用后恢复 |
| 2.5-08 maxBackupNum 轮转 | ❌ **未修复** | `maxBackupNum=1`：Backup 对象按上限轮转，但旧 FS 文件全部残留（4 个 cron 文件+手动文件累积；集群删除后仍留存，需人工清理）。CronBackup 源码修复未覆盖存储对象轮转 |
| 2.5-11 Backup 详情 API | ❌ **未修复** | `GET /backups/{已存在}` 仍 404（不存在对象同样 404） |

### 4.6 OCI 消费与策略

| Case | 结果 | 证据 |
|---|---|---|
| 2.6-01/03 packagePlan 落库与 digest 消费 | ✅ | 各 slot `repository@sha256` 与新构建 digest 一致，建群/扩容按 digest 消费成功 |
| 2.6-09 白名单拒绝 | ✅ | 修改策略 `v1.36.*`→`v1.34.*` 后创建 v1.36.4 在 Operation 前拒绝、无对象；策略精确恢复 |
| 3-20/3-21/3-22 resource 命令 | ✅ | `resource list` 列出 14 个 OCI package；inspect 展示新 digest `ebe86ff7...`（v1.37.0） |
| 3-19 registry list | ✅（部分） | `--registry-port 5003` 下 repository/image 列表正常；valid push 仍无 Docker 环境未测 |
| 2.6-07 配置权限 0600 | ❌ **未修复** | `/root/.kc/config`、`deploy-config.yaml` 实测 0644（B3） |

### 4.7 kcctl 命令与身份

| Case | 结果 | 证据 |
|---|---|---|
| 3-09 get selector | ✅（部分） | label selector 正确过滤（R6 的 User selector 问题本轮未复测） |
| 3-10 `get --watch` | ❌ **未修复** | 一次输出后 rc=0 退出，无长连接 |
| 3-25 login 错误密码 | ✅（部分） | `Unauthorized due to reason incorrect username or password`；正确登录+token 过期未测 |
| 3-06/4-08 user/role CRUD | ✅（部分） | role/user 创建、查询、删除成功；重复 user 名拒绝但以 500 返回（宜 409）；自定义 role 授权见下 |
| 4-08b 自定义 role RBAC | ❌ **未修复** | `r7-viewer`（role-template-view-clusters）登录成功后 `get cluster`/`get node` 仍 403（Forbidden） |
| 7-02 completion | ✅ | bash 生成并通过 `bash -n`；zsh 因 dev-2 无 zsh 二进制未复测（R6 已过） |
| 3-27 deploy config | ✅ | 生成成功 |
| 1.3-08/3-03/3-26 doctor/status | ✅ | 25/25、3/3/3，多轮故障注入前后一致 |

### 4.8 可观测性

| Case | 结果 | 证据 |
|---|---|---|
| 4-13 自省端点 | ✅ | configz/status/components/componentmeta 均 200 |
| 4-14 审计事件 | ✅ | `/audit.kubeclipper.io/v1/events?limit=&page=` 200，含本轮全部操作记录 |
| 4-17 healthz/metrics | ✅ | 根路径 `/healthz`、`/metrics` 200；metrics 108 行，无 password/secret/token/private 字段名 |
| 4-01/4-02 Addon | ⚠️ 未执行 | 三机无 NFS 服务端与 BGP 邻居条件（与 R6 相同），本轮未安排 |

## 5. 新发现问题（本轮新增，均建议修复）

1. **部署后无默认 image Registry 资源，离线建群无限挂起（P0 级）**
   新部署平台 `kcctl get registry` 为空（deploy 只初始化 delivery policy）。未显式
   `--image-registry` 时 kubeadm.yaml 使用默认 `registry.k8s.io`，离线机器上拉取超时，
   创建 Operation 在 `kubeadm init` 无限等待（containerd 日志可见对
   `europe-west3-docker.pkg.dev` i/o timeout 反复重试）。期望：deploy 初始化与 Package
   Registry 同端的 image Registry 资源，或创建前明确报错。R4～R6 的 `aio-img-reg` 为历史
   环境手工遗留，掩盖了该问题。
2. **CLI 默认版本组合与 delivery policy 不联动（P1）**
   `--k8s-version v1.37.0` 未显式传 `--cri-version/--cni-version` 时，CLI 取 inventory
   首个版本（1.7.29/v3.29.6，仅 v1.35.* 规则允许），服务端以
   `Internal server error ... UnsupportedComponentVersion` 拒绝。期望 CLI 按 k8s 版本匹配
   规则取默认值，服务端给出可读的 400。
3. **建群失败不回滚节点占用标签（P1）**
   节点在创建早期被打上 `kubeclipper.io/cluster`+`nodeRole` 标签；后续任何失败
   （UnsupportedComponentVersion 500、InstallFailed 删除、取消）都不清理，节点永久
   `in used`。Running 集群的正常删除会清理；InstallFailed 删除不清理。三机均需人工
   etcd 手术恢复。
4. **失败的备份 Operation 使集群卡 UpdateFailed（P1）**
   备份失败后集群 phase 停留 UpdateFailed，不能再次备份（500），无自动恢复；需
   `PATCH /clusters/{name}/status` 手工复位（该复位 API 本身工作正常）。
5. **卡在健康检查重试环的创建 Operation 无法取消（P1，B2 最严重表现）**
   1M/0W 未 untaint 时 CoreDNS Pending，健康检查 `RetryFunc` 无限循环（仅 ctx 超时退出）；
   cancel 请求后 Operation/Cluster/节点标签均不收敛，多次重启 kc-server 也无效，
   只能 kubeadm reset + etcd 精确删键。R5 的“重启一个 kc-server 即收敛”在本场景不成立。
6. **被取消的 Operation 不释放 ExecutionLock（P2）**
   取消的 CreateCluster 持锁，后续 DeleteCluster 永久 Pending；手工删锁后控制器能自动
   重跑删除（该恢复路径有效）。
7. **PUT /backuppoints 静默不生效（P2）**
   更新 s3Config.endpoint 返回 200 但值不变，只能删除重建（被 Backup 对象引用时删除
   亦受阻）。
8. **S3 endpoint 缺少入口校验（P3）**
   接受 `http://host:port` 形式，agent 运行时才以 minio-go
   `Endpoint url cannot have fully qualified paths` 失败。

## 6. 已知未修复项复测汇总（对审计 B 项的回应）

- **B2（CIDR 校验/取消/失败恢复）**：CIDR 创建前校验仍未实现（❌）；取消收敛在本轮三种
  场景表现不一——CIDR 取消本次无需重启即收敛（较 R6 改进）、备份失败后集群卡
  UpdateFailed、健康检查死循环完全不可收敛（恶化）；节点占用标签残留是新收敛缺口。
- **B3（敏感配置 0600）**：仍未修复（0644）。
- **B4（备份轮转/详情查询）**：2.5-08、2.5-11 均未修复；本轮另发现备份失败置集群
  UpdateFailed 的新缺陷。
- **B6（最终候选验收）**：本轮已完成“新候选构建→发布→部署→主链路 E2E”，主链路在新
  revision 上可用；但上述 ❌ 项与 §5 新问题应作为发布前修复或明确降级承诺的输入。

## 7. 恢复过程记录（环境恢复成本）

本轮为恢复卡死环境共执行 3 次人工 etcd 手术 + 1 次 kubeadm reset（删除孤儿
cluster/operation/task/executionlock 键并清理节点占用标签），另重启 kc-server 多次。
所有操作仅涉及本轮测试对象，共享 Registry 与其他团队物料未动。证据目录
`sh-dev-2:/tmp/kc-evidence-20260919/`（76 个文件，r7-00～r7-39），未含凭据明文。

## 8. 终态

```text
KubeClipper Platform Status: Healthy
kc-server  Healthy
kc-etcd    Healthy
kc-agent   Healthy 3/3
Cluster    none
Operation  none
doctor     25 passed, 0 warnings, 0 failed
```

测试用户/角色已删除，临时 MinIO 替代（seaweedfs）进程与数据已清除，孤儿备份文件已清理，
`aio-img-reg` Registry 资源保留（与历史轮次一致）。

## 9. Batch-1/Batch-2 修复与复验（2026-09-20 追加）

R7 报告完成后，按报告结论实施了修复并重新打包验证。修复分支 commit：

- `a989b14f`（batch-1）：B3 配置 0600、2.5-11 备份详情、N1 默认 Registry、2.1-28 CIDR 校验、N7 backuppoint。
- `3348bd34` + `5f694a0e`（batch-2）：N4 备份失败状态复位、N5 取消有界收敛（服务端 deadline 收缩 + agent 端任务终结观察）、2.5-08 轮转顺序。

候选包 `v2.0.3-rc.1`（revision `5f694a0e...`）经相同路径发布到 5003 并重新部署，逐项复验：

| 项 | 复验结果 |
|---|---|
| N1 | 部署日志确认 `default image registry "kc-package-registry" (http 172.16.131.146:5003) initialized`；未传 `--image-registry` 建群自动选中该资源并 Running |
| 2.1-28 | 重叠网段创建前拒绝（`pod subnet 10.96.0.0/16 overlaps service subnet 10.96.0.0/12`），零对象 |
| B3 | `/root/.kc/config`、`deploy-config.yaml` 实测 0600 |
| 2.5-11 | 已有备份 `GET /backups/{name}` 返回 200，不存在 404 |
| N7 | 带 scheme 的 endpoint 创建即 400（含指引文案）；PUT 更新 endpoint/bucket 持久化生效 |
| N4 | 强制造备份失败（移除备份目录）后集群自动回到 Running（不再卡 UpdateFailed），目录修复后重试立即成功 |
| N5/N6 | 1M/0W 楔死场景取消后**无重启**约 4 分钟收敛：Operation Canceled、集群 InstallFailed、锁释放、后续删除/建群正常 |
| 2.5-08 | `maxBackupNum=1` 每分钟 Cron 多次触发后，仅保留最新一个 Backup 对象与对应存储文件，旧对象与 FS 文件同步清理（R5/R7 的孤儿文件不再产生） |

最终状态：Healthy 3/3/3、doctor 25/25、无 Cluster/Operation、测试用户/备份/临时设施全部清理。
**注意**：本报告 §4 的 ❌ 结论（2.5-08、2.5-11、2.1-28、3-10、4-08b、2.6-07）为修复前基线；
其中 2.5-08、2.5-11、2.1-28、2.6-07 已由 batch-1/batch-2 修复并复验。

### 9.1 Batch-3 修复与复验（2026-09-20 追加）

- `12d59d9b`（batch-3）：实现 `kcctl get --watch`（3-10）；重复 user 名从 500 改为 400
  Bad request（N 项，§4.7）。
- `dfc2b49e`：移除 watch 路径的临时 DEBUG 输出。
- 候选包 `v2.0.3-rc.2`（revision `12d59d9b...`）发布部署后复验：

| 项 | 复验结果 |
|---|---|
| 3-10 get --watch | ✅ `kcctl get cluster --watch` 持续输出 watch 事件；服务端流在 watch 超时后关闭时客户端 2 秒退避重连并继续，Ctrl-C 正常退出。服务端 watch 流快速关闭现象（audit 记录 watch 请求在 ~0.4ms 内 ResponseComplete，Go 客户端常收 1-2 个事件后 EOF）**根因已定位并修复，见 §9.2**（此前"go-restful chunked 流与 HTTP/2 组合问题"的猜测不成立） |
| 重复 user 400 | ✅ 重复创建 r8user3 返回 `Bad request due to reason users.iam.kubeclipper.io "r8user3" already exists`（400），不再 500 |
| 4-08b 自定义 role RBAC | ✅ **改判**：R7 的 403 归因于测试时使用了错误注解键（`kubeclipper.io/role`，服务端读 `iam.kubeclipper.io/role`）。用正确键创建 `r8user3`+binding 后，登录可 `GET /clusters`、`/nodes` 200，越权创建 Registry 403；服务端 role 聚合（aggregation-roles 注解 → 3 条规则）确认正常 |

另：R7 排查中曾出现 curl `/api/core.kubeclipper.io/v1/users` 404 而 nodes/clusters 200 的
现象，归因为 users 注册在 `iam.kubeclipper.io` 组（`pkg/server/registry/user/rest.go`），
测试用了错误组路径，非产品缺陷。

### 9.2 N9 根因与修复：watch 流立即关闭（2026-09-20 追加）

§9.1 记录的服务端 watch 流快速关闭（~0.4ms ResponseComplete、Go 客户端收 1-2 事件后
EOF）已排查出根因并修复，**不再是独立专项**。

**根因**：核心/IAM 资源 handler 在客户端未传 `timeoutSeconds` 时计算默认 watch 超时的
写法是
`time.Duration(float64(query.MinTimeoutSeconds) * (rand.Float64() + 1.0))`——
`MinTimeoutSeconds`=1800，浮点结果 1800~3600 被直接当作 `time.Duration`（纳秒），
**漏乘 `time.Second`**。`pkg/server/restplus/watch.go` 的 ServeWatch 用该值
`time.NewTimer(timeout)`，timer 首次 select 即触发，watch 流建立后 ~0.5ms 内被服务端
主动关闭。对照证据：`pkg/apis/operations/v1alpha1/handler.go` 用的是正确的
`30 * time.Minute`，因此 agent 的 operationtasks watch 能存活数分钟（R7 记录 415s/442s），
同一 ServeWatch 路径下两种行为并存即指向 handler 传入值差异。

**修复**（commit `b23a9ab2`）：

- `pkg/apis/core/v1/handler.go` 等 12 处替换为抽出的 `defaultWatchTimeout(q)` helper
  （显式 `TimeoutSeconds` 优先，否则 `(MinTimeoutSeconds ~ 2×MinTimeoutSeconds)` 秒，
  显式乘 `time.Second`，保留 apiserver 式随机化防惊群），含注释说明纳秒陷阱。
- `pkg/apis/iam/v1/handler.go` watchToken 处同类错误同步修复。
- `pkg/apis/core/v1/handler_test.go` 新增 `TestDefaultWatchTimeout` 回归测试
  （默认值 ≥ MinTimeoutSeconds、显式 TimeoutSeconds 生效）。

**发布与验证**：候选包 `v2.0.3-rc.3`（revision `b23a9ab2`）经相同路径发布到 5003
（digest `sha256:bd851a27...`）并三机清空重部署（Healthy 3/3/3、doctor 25/25、
default image registry 自动初始化），复验：

| 验证项 | 结果 |
|---|---|
| 流持续时长 | 修复前 curl/Go watch 流 45-48ms 关闭、0 事件；修复后 nodes/clusters watch 的 audit 时长 24.98s / 19.98s（达到 curl max-time 上限，流保持打开） |
| 实时事件 | `GET /api/iam.kubeclipper.io/v1/users?watch=true` 收到初始 ADDED 后，测试用户 n9user3 的创建/删除以实时 `ADDED`/`DELETED` 事件送达 |
| CLI | `kcctl get user --watch` 输出初始表 + `ADDED`/`DELETED` 事件，Ctrl-C 正常退出 |
| 语义 | 无 resourceVersion 时服务端先送当前状态为 ADDED，之后增量 MODIFIED/DELETED，符合预期 |

现场已清理（测试用户全删、临时凭据/证书删除、/tmp 构建暂存清除）；共享 Registry
5003 未受影响。

## 10. 未执行边界

纯离线 bundle、HTTPS/自签 CA/认证 Registry、Console E2E、arm64 真机、IPv6/双栈、MetalLB BGP、
valid registry push（无 Docker Engine）、master 增删（产品不支持）维持既有缺口记录。
平台自身升级原属本节缺口（B1 契约缺口），已于 2026-09-20 按 OCI 契约实施并通过三机 E2E，
见 §11；升级失败注入与中断恢复的破坏性场景仍未实测（§11.4）。

## 11. B1 平台升级（OCI 契约）实施与 E2E 复验（2026-09-20 追加）

平台自身升级按 [remediation plan §2.3](../testing/oci-release-remediation-plan.md) 八条契约实施：
`kcctl upgrade <all|server|agent> --version/--manifest`（互斥、必选其一），复用 ReleaseManifest/
OCI fetcher/digest 校验，manifest 驱动（targetRef、targetVersion、targetRevision）、tag 绑定验证、
固定 Server→Agent 逐节点滚动（stop→backup→install→start→healthz，失败 restore 并保留 staging）、
版本策略（semver 比较，同 revision 幂等，拒隐式降级）。旧 `--pkg/--online/--binary` 入口移除，
console/kcctl 组件明确报 "not supported yet"（step 2 交付）。

### 11.1 实施与修复 commit

- `057f45e1`：B1 主体（upgrade 重写为 manifest/OCI 契约，fetch/verify/rollout）。
- `301c6629`：E2E 期间修复——每节点执行前重探测 revision、plan 节点去重（`dedupNodes`）、
  重复执行不再覆盖原始 backup（`[ -f backup ] || cp -a`）、版本策略降级比较先于 revision
  幂等短路（否则"声称低版本但 pin 当前 revision"的 manifest 会被当幂等放行）。
- `f7d82d7e`：**SSH 配置接线修复（本轮最严重缺陷）**。`UpgradeOptions.SSHConfig` 停留在
  `NewSSH()` 空默认值（User=root、无 PkFile/Password/PrivateKey），`sshutils.SSHToCmd` 将所有
  节点判定为本地回退，probe/stop/install/start/传包**全部在 kcctl 本机执行**而日志仍打印目标
  节点名——造成 14:14/14:51 两轮"假成功"：三个 plan 节点的替换全部落在发起机 dev-2 自身
  （kc-server 被重启 3 次、backup 被已升级二进制覆盖），146/230 从未被触碰，幂等复跑因本地
  probe 全返回 dev-2 revision 而 6 节点全"skip"。修复为 `Complete()` 中
  `o.SSHConfig = o.deployConfig.SSHConfig`（与 join 相同接线），并在代码注释固化该约束。
  修复前的全部 T 系列节点操作类结论作废，下述证据均为修复后二进制（gitCommit `f7d82d7e`）重跑。

### 11.2 正向 E2E（三机，manifest 驱动，registry 5003）

前置快照：208 kc-server=rc.4/`057f45e1`（14:14:42）、146/230=rc.3/`b23a9ab2`（12:40:10/12:40:14），
三台 agent 均 rc.3。

| 用例 | 结果 |
|---|---|
| `upgrade server --manifest rc.4.yaml` | ✅ 计划输出三节点真实 revision（208=`057f45e1` skip；146/230=`b23a9ab2`），107MB 传包走真实 SSH，逐台 stop→install→start→healthz；EXIT=0，verifyPlatform 报 `v2.0.3-rc.4 (057f45e1)` |
| 节点侧核对 | ✅ 146/230 二进制 md5 变为 rc.4（`33070976`）、ActiveEnterTimestamp 各变化一次（14:59:59/15:00:08），208 时间戳不变（14:14:42，skip 未触碰）；成功后 staging（含 backup）按设计清理 |
| `upgrade agent --manifest` | ✅ 三台 agent rc.3→rc.4（82MB/台，`41c03b1e`），时间戳各变化一次 |
| 幂等复跑 `upgrade all --manifest` | ✅ 6 个节点槽位全部实测 `current=057f45e1` → 全 skip，EXIT=0，无任何重启 |

### 11.3 负向用例（均为修复后二进制实测）

| 用例 | 结果 |
|---|---|
| T5 repointed tag（manifest digest/revision 全 0，tag 真实 digest `879d9e34`/revision `057f45e1`） | ✅ `verifyTagBinding` 在触碰任何节点前拒绝：`refusing to upgrade from a repointed tag`，EXIT=1，三节点零操作 |
| digest 不符但 revision label 与 manifest 一致 | ✅ 告警放行（`digest ... differs ...; source revision ... matches`），随后各节点已达标幂等 skip——回退分支语义符合 §2.3-3 |
| T6 降级（manifest v2.0.2/`b23a9ab2`，平台 rc.4/`057f45e1`） | ✅ Validate 阶段（访问 Registry 前）拒绝：`refusing implicit downgrade: platform v2.0.3-rc.4 is newer than target v2.0.2`，EXIT=1 |
| console/kcctl 组件 | ✅ 明确报 `not supported yet; supported components are [ all | server | agent ]` |

### 11.4 终态与边界

终态：Healthy 3/3/3、doctor 25/25、etcd 3 成员（:12379）同步无 learner、三台 /healthz=ok；
升级产生的时间戳证据见 §11.2。测试 manifest/kcctl 临时产物已从 dev-2/dev-3 清除，
共享 Registry 未改动。

未实测边界：节点启动失败/升级中中断后的恢复与续升（§2.3-7 的 restore 路径仅单测覆盖，
未做故障注入）。

带 repository 前缀 Registry 的 packageRef 解析观察项已关闭（2026-09-20 全链路审计，结论为
误报、无代码改动）：`pkg/delivery/indexer/registry.go` 的 `packageRef` "仅取 host" 与**全路径
repository** 配对使用（`IndexRepositories` 先 `path.Join(prefix, logical)`、catalog 路径
`scopedRepository` 裁前缀后传全路径），前缀不会丢失；`pkg/delivery/apis/registry_indexer.go` 的
同名 `packageRef` 则与含前缀 registry + 逻辑 repository 配对（两个同名函数契约不同但各自自洽）。
同步核对了 upgrade（`targetRef = registry + "/" + Target`，`Target` 为 manifest 定义的
registry 相对逻辑路径，`generate-release-manifest.sh` 以 `ref[len(package_registry)+1:]` 生成）、
registry sync（`destination = target + Target`）、fetcher（`ValidateReference` 按含前缀
`Config.Registry` 校验）与 release manifest 校验，均一致。前缀场景已有回归测试锁定：
`TestRegistryPackageIndexerScopesProjectPrefix`（断言完整 Transport.Ref 且 prefix 外仓库被过滤）、
`TestRegistryPackageIndexerIndexesKnownRepositoriesWithoutCatalog` 及 helm 变体，
`go test ./pkg/delivery/... ./pkg/cli/upgrade/... ./pkg/cli/registry/... ./pkg/cli/resource/...`
全绿。

`--version` 在线下载路径经代理隧道补充实测（2026-09-21）：SSH 反向隧道 + `HTTPS_PROXY`
下，下载器穿透到真实 GitHub——不存在的 stable 版本收到干净的
`HTTP 404 Not Found (offline environments must sync the release bundle and use --manifest)`，
而直连（无代理）被防火墙掐断为 `unexpected EOF`，即网络链路、代理透传与错误处理均正常。
上游仓库尚无 v2 OCI stable 发布（最新 release 仍为 v1.7.0，仅旧 tar 包；v2.0.x manifest 均
404），正向下载+checksum 校验待首个 stable 发布后补测。另记录运维要求：给 kcctl 设
`HTTPS_PROXY` 时必须同时配 `NO_PROXY` 排除平台内网地址（如 `172.16.131.0/24`），否则
`Complete()` 拉取平台 deploy-config 的 API 请求也被送进代理而失败（EOF），命令在下载
manifest 之前就断连。

## 12. R8 追加轮（2026-09-20/21）：失败路径收敛 E2E 与 agent worker 死锁定位修复

本轮在 R7 修复批次之后执行，聚焦两个新缺口的真机闭环：N3（建群失败/删除不释放节点占用标签，
`978b1b43`）与 N2（CLI 默认 cri/cni 版本不随 k8s 版本配对，服务端 500，`fdac85f2`）。
平台经 B1 升级路径升级到 `v2.0.3-rc.5`（`c5ccb367`，含上述修复；实测 `kcctl version` 确认）。
共享 Registry 5003 未做任何改动。

### 12.1 场景 S1～S5 结果

| 场景 | 结果 | 证据 |
|---|---|---|
| S1 负向校验返回 400 | ✅ | `fdac85f2` 后，delivery ResolverError 类拒绝（如 cri/cni 与 k8s 版本不配对）由 500 变为可读 400，CLI EXIT=1，无对象残留 |
| S2 CIDR 重叠仍被接受（2.1-28） | ❌→**更正（2026-09-20）：本条误报** | 原记录"rc.5 上 Pod/Service CIDR 重叠仍通过创建前校验并接受创建"与事实不符：重叠校验自 batch-1 `a989b14f`（rc.3 起）已在 API/CLI 生效，且 §9 记录过 rc.1 复验通过（重叠 400 零对象）。R9 dryRun 探针在 rc.5 复测确证：重叠 400（`pod subnet 10.96.0.0/16 overlaps service subnet 10.96.0.0/12`），CLI 本地 fail-fast EXIT=1；真实缺口为列表内嵌套/每地址族多条/主机网段冲突（200 通过），已在本轮补齐（见 remediation plan §3.2 更新）。当轮误报成因不可考，按不静默改史原则保留原行并加更正。**R9 终态（2026-09-21，rc.6 `29a9bf8a`）：边界校验随 rc.6 真机负向矩阵闭环——8 项非法输入 400+理由逐项匹配、.144/28 边界放行、双栈/单栈 dryRun 200、CLI exit 1、零残留（checklist 2.1-28 ✅）；`1413e849` 两复验点同轮通过（无 10s 尾延迟、create→delete 后立即再建群不饿死，见 gaps P0 行 7/N8）** |
| S3 同节点重群（删除后复用） | ✅ | 重叠集群删除后，同批节点再次建群 `r8-verify` 成功 Running（见 §12.2 死锁插曲与 §12.3 手动 untaint 记录），节点标签复用闭环 |
| S4 失败路径删除释放标签（N3） | ✅ | 重叠集群删除后，节点 `kubeclipper.io/cluster`/`nodeRole` 标签立即释放，节点可被新集群占用——`978b1b43` 修复实测生效 |
| S5 force 删除逃生门 | ✅ | `echo yes \| kcctl delete cluster r8-verify -F`（AskForConfirmation 在非 TTY 读 stdin EOF 会 Fatal，必须管道注入）约 30 秒完成：Cluster/Operation 列表清空、标签释放；agent 卸载步骤被跳过，dev-4 残留 k8s 文件属预期，由运维清理（见 §12.4） |

### 12.2 发现并修复 agent worker 死锁（R7 遗留操作停滞的 agent 侧根因）

S3 期间 `r8-verify` 的 CreateCluster 任务在 dev-4 上 Pending 超过 30 分钟：agent 日志显示任务
03:32:05 已派发（`command 1/1: custom`）但无任何子进程输出。经 SIGQUIT goroutine dump
（`kill -QUIT` 后 journald 抓取，systemd `Restart=always` 顺带解锁重启）实锤：`execute` 阻塞在
`[chan receive, 30 minutes]`（`worker.go` 的 `defer <-watchDone`），`watchTerminal` 阻塞在 10s
ticker select。

根因是 `execute()` 的 defer 声明顺序（LIFO 执行反转为"先等 watcher 退出、再关 stop"）与
server 侧任务历史清理（finalize 后 CleanupByTargetUID）的竞争窗口：

1. 每个任务完成都要等 `watchTerminal` 自己轮询到 terminal 或超时——正常路径白付一个最长 10s
   的轮询尾延迟（基线测试实测 10.01s/任务）；
2. 若 server 在该窗口内 finalize 并 purge 了任务历史，后续轮询永远 404，`execute` 挂到 spec
   deadline；单任务 worker 被饿死，该节点后续所有任务 Pending（informer store 中还留着错过
   delete 事件的 stale 条目，每次选中都命中同一个幽灵任务）。

修复 `1413e849`（四件套，附 3 个单测，套件 0.55s 全绿）：defer 对调（先关 stop 再等 watchDone）；
`watchTerminal` 轮询 NotFound → cancel 本地执行器；`finish` 兜底 Get NotFound → 清 informer
store 并 requeue；`getLiveTask` NotFound → 删 stale 条目并 requeue。`terminalPollInterval`
改为可注入（默认仍 10s）。

**部署边界**：rc.5（`c5ccb367`）不含该修复（提交在其后）。S3 的解锁靠重启 agent（informer
relist 清掉 stale 条目），不是修复本身生效；`1413e849` 需随下一个 rc 发布后在真机复验
（复验点：任务完成后无 10s 尾延迟；purge 竞争下 worker 不再饿死）。

### 12.3 测试配置记录：master taint 与 coredns Pending

`r8-verify` 创建时未带 `--untaint-master`，`spec.taints` 下发了
`node-role.kubernetes.io/master:NoSchedule`；kubeadm 默认 coredns 只容忍
`control-plane:NoSchedule`，coredns 无处调度、`allNodeReady` 健康检查无限重试。判定为测试
配置问题（产品已提供 `--untaint-master`），手动
`kubectl taint node lixd-dev-4 node-role.kubernetes.io/master:NoSchedule-` 解锁。1M 拓扑
建群必须带该参数。

### 12.4 dev-4 残留清理与终态

S5 force 删除按设计跳过 agent 卸载，dev-4 保留 k8s 残留。随后运维清理：`kubeadm reset` +
手工移除 `/etc/kubernetes`、`/var/lib/kubelet`、CNI 目录、kube-ipvs0 链路并确认相关服务
inactive；**平台数据目录 `/var/lib/kc-etcd` 与共享 Registry（dev-3 :5003）全程未动**。
终态（2026-09-21 实测复核）：

- `kcctl status` Healthy 3/3，三台 kc-agent/kc-server/kc-etcd/kc-console active；
- Cluster/Operation 列表为空，三节点 `kubeclipper.io/cluster`/`nodeRole` 标签全部为空；
- 分支构建 kcctl doctor 25/25 通过（旧版 /root/kcctl v1.6.0-era 的 static-resource-health
  检查在无集群时报 port 0 失败，为工具版本假警报，非平台问题）；
- 临时产物清理：Mac 侧 stub/bootstrap/manifest/registry 描述文件、dev-2 侧 .r8-* 凭据与
  证书临时文件（含 /tmp/.r8-certs3）已全部删除。

### 12.5 R9 追加轮（2026-09-21）：rc.6 真机复验，2.1-28 与 N8 双闭环

平台经 B1 路径升级到 `v2.0.3-rc.6`（`29a9bf8a`，含 CIDR 边界校验与 `1413e849` agent worker
修复）。发布链：Mac 交叉构建 linux/amd64 包（gitTreeState=clean），oci-publish 经 LAN 机 dev-2
以 http scheme 推入共享 Registry（仅增 tag `v2.0.3-rc.6`，digest
`sha256:5b740ad092a7...`，存量 9 个 tag 未动）；`kcctl upgrade all --manifest` 六个节点槽位
（3 server+3 agent）逐台 stop→backup→install→start→healthz 滚动升级，platform API 报
`v2.0.3-rc.6 (29a9bf8a7623)`。一个教训：手写 manifest 的 digest 用了 oci-publish stdout 的
digest（非 tag 顶层 index digest），被 `verifyTagBinding` 以 repointed tag 拒绝（该防护再次
生效）；按 generate-release-manifest.sh 同法以 tag 实际 digest 修正后通过。

**2.1-28 负向矩阵（dryRun 探针，零对象落库）**：

| 输入 | 结果 |
|---|---|
| 重叠 10.96.0.0/16 vs 10.96.0.0/12 | ✅ 400 `pod subnet ... overlaps service subnet ...; use disjoint subnets` |
| 列表内嵌套 10.0.0.0/8+10.1.0.0/16 | ✅ 400 `at most one ipv4 pod subnet is allowed` |
| 双 v4 pod 172.20.0.0/16+172.21.0.0/16 | ✅ 400 同上 |
| 双 v6 service fd00:10:96::/64+fd00:10:97::/64 | ✅ 400 `at most one ipv6 service subnet is allowed` |
| v4-mapped `::ffff:10.96.0.0/112` | ✅ 400 `IPv4-mapped IPv6 CIDRs are not allowed, use the IPv4 form` |
| 主机冲突 pod 172.16.131.0/24 | ✅ 400 `conflicts with node ... address 172.16.131.208` |
| 主机冲突 service 172.16.131.208/29 | ✅ 400 `service subnet ... conflicts with node ...` |
| 边界：service 172.16.131.144/28（不含节点 IP） | ✅ 200 放行（不误伤） |
| 合法双栈 v4+v6 / 合法单栈 | ✅ 200 |
| CLI 重叠 | ✅ 本地 fail-fast exit 1（请求未发出）；CLI 主机冲突经服务端 400 可读报错 exit 1 |

全程 Cluster/Operation 零残留。2.1-28 终态 ✅（双栈真机组网仍受无 IPv6 环境限制，见 2.1-32）。

**`1413e849` 两复验点**：①1M（1 master，带 `--untaint-master`）建群 13 步任务时长分布
1s×7、5-8s×4、31s/34s×2（长步为镜像准备/安装的真实耗时），无 10s 轮询尾延迟（修复前基线
10.01s/任务）；②create→delete 收敛（Cluster/Operation 清空、标签释放）后立即再建群正常派发
并 Running，不饿死。

终态：`kcctl status` Healthy 3/3、分支构建 kcctl doctor 25/25、Cluster/Operation 空、三节点
占用标签空、三主机 /etc/kubernetes 与 /var/lib/kubelet 无残留（空目录已删）；共享 Registry
与 `/var/lib/kc-etcd` 未动；Mac 侧 manifest/wrapper/registry 凭据与 dev-2 侧 client 证书
（/tmp/r9-admin.*）、临时 kcctl 均已删除。证据文件：dev-2 /tmp/r9-00～r9-05。

### 12.6 R10 追加轮（2026-09-21）：rc.7 发布，N9/B3 修复复验与协作式 cancel 语义矩阵

**修复**（随 rc.7 `e7d99421` 发布）：

- N9（R9 探针发现）：API 直调建群/dryRun 缺省可选 `cni.calico` 子对象时 InitStep nil 解引用
  panic（500）。修复 `75ed938f`：InitStep 经 `defaultCalico` 对 nil 块按 kcctl 同款默认值填充
  （first-found/Overlay-Vxlan-All/IPManger/MTU 1440），非 nil 块空字段兜底，深拷贝不改写请求
  对象；模板直接解引用 `.CNI.Calico.*`，渲染链一并修复。2 单测。
- B3（2.6-07）：batch-1 已 0600 但为就地 O_TRUNC 写——中途失败丢原文件、跟随符号链接。修复
  `e7d99421`：`WriteToFile` 原子替换（同目录 0600 临时文件+Sync+rename，失败保留原文件、零临时
  遗留）、拒绝经符号链接写配置、`Config.Dump` 收敛到同一入口，umask 无关。§4.4 回归测试落地。

**发布与升级**：`publish-bootstrap-kubeclipper.sh --version v2.0.3-rc.7 --registry-prefix
172.16.131.146:5003 --arch amd64`（KC_OCI_PUBLISH_BIN wrapper 经 dev-2 发布，Mac 不可达
Registry）。三机升级 `kcctl upgrade all --manifest --package-registry-scheme http`，6 槽位至
rc.7，platform API 报 v2.0.3-rc.7 `e7d99421`，doctor 25/25。digest 教训追加：该 Registry 的
manifest HEAD 必须带与 media type 匹配的 Accept 头（`application/vnd.oci.image.index.v1+json`），
否则 404——tag 顶层 digest `sha256:12454850...`（≠ oci-publish stdout digest `77dc949b...`）。

**N9 复验**：缺省 calico 块 dryRun **200**（修复前 500 panic）；合法全块 200、重叠 400 理由
匹配回归通过。

**B3 复验**：dev-2 `/root/.kc/config` 与 `deploy-config.yaml` 实测 0600（重写时收紧生效）；
dev-3/dev-4 无 .kc 目录。R6 遗留 /tmp 临时证书（kc-r6-admin-client.*）已补删。

**协作式 cancel 语义矩阵（§3.3/§3.4）与 2.1-30 retry**（1M 拓扑 `--untaint-master`，集群
r10-c1/c2/c3/final2）：

| 用例 | 取消时点 | 结果 |
|---|---|---|
| C1 Running 中取消 | 6/13 步（1 步 Running 2m33s） | 在途步自然完成→停止派发，7 步 Canceled，op Canceled，集群 InstallFailed；~2.5 min 收敛 |
| C2b 最早取消 | +3s，仅第 1 步在途 | 1 步完成+12 步 Canceled，<23 s 收敛 |
| C3a 终态后重复取消 | op 已 Canceled | CLI 干净拒绝（"cannot be canceled from phase Canceled"，exit 1） |
| C3b Running 中并发双取消 | 两连发 | 第 1 次受理、第 2 次 API Conflict 拒绝，无状态污染 |
| retry（2.1-30） | Canceled op 上 retry | 前 6 步保留原时间戳未重做（§3.3-5），剩余步 ~60s Succeeded，集群 InstallFailed → Running |
| C4a/C4b 取消后安全删除+同节点重建 | delete 后 20～30 s | Cluster/Operation/标签全清；重建 2 min Running；再删后 kubelet inactive、无 /etc/kubernetes、doctor 25/25 |

§3.4 剩余子项：超时（spec deadline 到期）、Watch 重连与 Server/Agent 重启注入未在本轮覆盖。

终态：Cluster/Operation 空、三节点占用标签空、doctor 25/25；Registry 仅新增 v2.0.3-rc.7 tag
（11 个 v2.0* tag 与升级前一致），存量未动；`/var/lib/kc-etcd` 未动；dev-2 临时 kcctl（r9/r10/
doctor）、oci-publish 二进制、rc.6/rc.7 manifest 均已删除（§12.5 所述"临时 kcctl 已删除"在
R9 当时未彻底，本轮补齐）。证据文件：dev-2 /tmp/r10-00～r10-final2（16 份）。

### 12.7 R11 追加轮（2026-09-22）：rc.8 发布，B4 备份持久化删除流真机复验

**修复**（`e9d9afeb`，随 rc.8 发布）：B4 备份删除与存储清理一致性。手动删除与 Cron 轮转统一
进入持久化删除流程：新增 `deleting`/`deleteFailed` 状态并持久化删除 Operation 引用，Backup
记录保留到删除 Operation Succeeded 才由 backupcontroller 移除；重复删除经 activeDeletionOp
复用在途任务；删除任务用备份自身 BackupPointName 构造，不依赖集群当前默认备份点；终态失败
穿透为新 retry（新 Operation）；恢复入口拒绝 deleting/deleteFailed；轮转过滤
rotatable（available/deleteFailed）；DescribeBackup 按 URL name 查询修复 404。含单测。

**发布链重建**：dev-2 /tmp 被例行清理（oci-publish/kcctl 旧二进制消失），本地交叉编译
`tools/oci-publish` 与 `cmd/kcctl` 上传重建；Registry tag 查询 HEAD 须带
`application/vnd.oci.image.index.v1+json` Accept（index digest `sha256:ba950f5a...` ≠
platform manifest digest）。发布 `v2.0.3-rc.8`（只增 tag），三机升级 `kcctl upgrade all
--manifest`，6 槽位 upgraded to revision `e9d9afebb2a8`，platform API 报 rc.8，doctor 25/25。

**B4 九项复验**（集群 r11-b4-cluster，备份点 r11-bp，CronBackup 周期 */2）：

1. 详情 2.5-11：已有 Backup `GET /backups/{name}` 200 全字段（backupStatus/clusterNodes/
   preferredNode），不存在 404。
2. 手动删除新流：status 置 `deleting` + `delete-operation-name` 标签可见 → 删除 Operation
   Succeeded → 记录 404、FS 文件同步消失。
3. 顺序重复删除幂等 200（复用在途任务，不建新 Operation）。
4. 恢复守卫：deleting 窗口恢复 400（"backup ... is deleting now, can't recovery"）；
   available 备份恢复 200 → RecoveryCluster Succeeded → 集群回 Running、节点 Ready（2.5-09）。
5. 轮转 2.5-08：`maxBackupNum=2` 周期 Cron 连续 4+ 轮，Backup 记录与 FS 文件一一对应、无孤儿。
6. S3 备份点全链路：seaweedfs S3 临时起于 dev-2（4.47，端口 19333/18081/8333，避开
   kc-server/caddy 占用的 8080/18080），创建 available + 删除记录消失。
7. 错误凭据/不存在 bucket（N4/2.5-10）：Operation 明确 Failed、Backup `error`、集群保持
   Running 可再次备份。
8. deleteFailed 主场景：S3 停机时删除 → `deleteFailed` 记录保留；S3 恢复后重试删除 → 新
   Operation（`60738b78`）成功、记录消失。
9. N7：合法 S3/FS 更新生效（bucket/endpoint 均可改）；带 scheme endpoint 400（"must be
   host[:port] without a scheme"）；storage type 不可变（请求体须与存储值同用小写
   `s3`/`fs`，大写即 400）。

**已知边角**：同毫秒并发双 DELETE 同一备份，一个 200 一个 500（createOperationV2 冲突，
日志无 reason）——无重复副作用、不影响一致性，记为低优先改进项。

**测试配置记录（后续轮次可复用）**：备份点从 cluster label `kubeclipper.io/backupPoint`
（驼峰、大小写敏感）读取，经 PUT cluster 打 label；创建备份 POST /clusters/{name}/backups
仅需 metadata.name（响应名自动变为 {cluster}-{name}-{rand6}）；恢复 POST
/clusters/{cluster}/recovery 用 `useBackupName` 字段；API 认证用
/root/.kc/config 的 client-certificate-data（curl --cert/--key 合并 pem）；手工 JSON 建群
缺 packagePlan 时 containerd 直连 registry.k8s.io 拉镜像超时卡死（kill kubeadm 需 -9），建群
一律走 `kcctl create cluster`。

终态清理：CronBackup/2 个残留备份/坏备份点/测试集群删除；三节点 kubeadm reset +
`rm -rf /var/lib/etcd`（集群 etcd）+ `rm -rf /var/kc-backups-r11`；节点无 cluster 标签、
Ready；doctor 25/25；Registry 仅增 rc.8 tag（12 个 v2.0* tag），存量未动；`/var/lib/kc-etcd`
未动；dev-2 /tmp（seaweed-r11、weed socket、kc-r11-*、oci-publish-r8、kcctl-r8、
seaweedfs.tar.gz）与 Mac /tmp（oci-publish*、wrapper、tar 等 9 个）全清；临时证书/token
即用即删。checklist 2.5-04/08/09/10/11、gaps N4/N7 与 remediation plan B4 行已同步回填。

### 12.8 R12 追加轮（2026-09-22）：B5 认证 Registry 与缓存故障真机验证

范围经用户批准：认证 Registry 正/负向、断连重头拉、最小化篡改探针；纯离线/arm64/GITHUB_TOKEN
与 B6 留待后续。rc.8（`e9d9afeb`）三机，零代码改动，纯消费侧验收。

**环境**：dev-2 自建 distribution 3.0.0，`172.16.131.208:8443`，HTTPS 自签证书（CN/SAN=
IP 172.16.131.208 + DNS sh-dev-2）+ htpasswd（r12user，24 位随机 bcrypt 密码）。Mac 经
ghproxy 拉取二进制（直连 GitHub 不可用）。`kcctl registry sync` 从共享 146:5003（HTTP，
go-containerregistry 自动 HTTP 回退实测可用）镜像 6 artifact → 6 copied/0 skipped，
digest 逐项一致（sync 即 1.1-02 性质证据）。ReleaseManifest 顶层 sourceRevision 与
bootstrap/kubeclipper 一致（e9e95f4），全部 target 带 tag。

**① 正向（1.1-05/06）**：三节点 `/etc/kubeclipper-{server,agent}/delivery/package-registry.json`
（0600：registry/scheme=https/username/password/ca）+ deploy-config ConfigMap
`packageRegistry` 双侧切换，动态生效无重启。r12-auth-cluster 建群 Operation Succeeded
（15 步）、集群 Running、三节点 Ready=True；registry 日志三节点真实拉取（blob GET
208=54/146=12/230=12），6 artifact manifest 各 8 次 GET。换 registry → 缓存按 Transport 键
miss 全量重拉，符合设计。

**② 错误密码负向（1.1-09 部分）**：dev-4 密码改错+清缓存 → r12-neg1 建群 Operation Failed，
错误消息 `GET https://172.16.131.208:8443/v2/kubeclipper/packages/cri/containerd/manifests/
sha256:4feac2b3...: UNAUTHORIZED: authentication required`（URL 定位、无凭据）；registry 侧
230 的 manifest GET 携带凭据仍 401（`invalid authorization credential`）；三节点
`/var/logs/kubeclipper` + journalctl（kc-server/kc-agent）grep 错误密码与真实密码均 0 命中。
恢复配置后 `operation retry` → Succeeded、集群 Running（同时实证 Failed 态 retry：失败任务
重跑 7s 真实拉取，成功任务不重做）。

**③ 未信 CA 负向（1.1-09 部分）**：dev-3 配置去掉 CA → r12-neg5 建群失败，dev-3 拉取任务
[1s] `Get "https://...": tls: failed to verify certificate: x509: certificate signed by
unknown authority`（附 HTTP 回退得 400 `Client sent an HTTP request to an HTTPS server`），
无凭据泄漏。

**④ 缓存篡改探针（2.6-05）**：master（dev-2）calico chart 缓存 `charts.tgz` 翻一字节
（sha256 3486055d→eb2eb525，`.source.json` payloadDigest 仍为原值）→ 删除集群（篡改文件跨
删除幸存：chart 缓存不受 uninstall `CleanupPackage` 影响）→ 重建 r12-neg4：calico 安装步
`validCachedHelmChart` 校验拒绝 → digest-pinned 重拉（registry `GET /v2/kubeclipper/charts/
tigera-operator/blobs/sha256:3486055d... 200`，06:01:52）→ sha 恢复 3486055d、Operation
Succeeded、集群 Running。包 contents 路径的同构校验在代码确认（`loadCachedComponent` 逐文件
`packageFilePayloadDigest` 对照 + `validatePulledImageDigest`），真机行为由 chart 路径与
Transport 换源全量重拉间接实证。

**⑤ 断连与恢复（2.6-06）**：neg5（CA 已恢复）retry 中途 kill registry：在途 containerd 拉取
经 distribution graceful shutdown 完成（manifest mtime 06:06:31），其余任务
`dial tcp 172.16.131.208:8443: connect: connection refused`（https/http 双路）明确失败；
registry 重启后再次 retry → 三节点 k8s 包全部从头重拉（manifest+56MB layer ×3，
06:07:53），半成品/部分缓存未被信任，Operation Succeeded、集群 Running、三节点 Ready。

**行为记录**：删除集群时 uninstall 步骤对该集群组件执行 `CleanupPackage`（k8s/CRI/k8s-ext
包目录 RemoveAll），bootstrap 包豁免（etcd/kubeclipper 缓存跨删除幸存且建群不重拉——bootstrap
非建群消费路径，46 次 manifest GET 为 server 侧 inventory 解析）；calico chart 仅 master
消费，worker 上的 chart 缓存为死存储。

**终态清理**：全部测试集群删除（r12-auth-cluster/neg1～neg5），三节点 cluster 标签 free、
Ready=True；三节点 server/agent 配置从 `.r12bak` 还原（registry=146:5003）；deploy-config
ConfigMap `packageRegistry` 还原 146:5003（PUT 200 复读确认）；测试 registry 进程停止，
dev-2 `/tmp/registry-r12`、`/tmp/r12-api` 与全部临时密码/证书文件删除（正/错密码临时 json、
CA/证书/key）；Mac `/tmp/r12-auth`（证书/htpasswd/密码）、`/tmp/kcctl-r12`、
`/tmp/registry-mirror.tar.gz` 删除；证据留存 dev-2 `/tmp/r12-evidence/`（nodes.json、
registry-gets.log、registry-full-neg5.log、key-lines.txt，无凭据内容）。共享 Registry
146:5003 只读未动（_catalog 35 repos 与 caas4/* 原样），`/var/lib/kc-etcd` 未动。
checklist 1.1-05/06/09/10、2.6-04/05/06、gaps P0 行 8/9 与 R12 段、remediation plan 状态行
与 §6.3/§6.4 已同步回填。

### 12.9 R13 追加轮（2026-09-22）：R12 余量专项——token 映射、1.1-09 收口、公共 CA 等价、join 认证、纯离线 bundle

范围经用户批准（C1～C6；arm64 用户明确排除）。rc.8（`e9d9afeb`）三机，除 C1 外零代码改动，
纯消费侧验收。

**C1（qualification workflow）**：`publish-oci-qualification.yml` sync job 补 `secrets.GITHUB_TOKEN`
映射，发布临时凭据文件 0600+清理（commit `d2ca8df8`）。

**C2（1.1-09 收口）**：探针注入点统一为 deploy-config ConfigMap `.packageRegistry`（resolve
每次实时读 delivery json，indexer 缓存按 registry 地址键控）。①错误仓库地址（:9999 死端口）：
CLI EXIT=1 报 dial refused（https→http 双路）+ "no valid cri-version"；API 500 透传原始传输
错误——质量低于 400 typed 路径，记录观察项。②缺失制品（空仓库 5000，rules=0）：CLI 本地
guard EXIT=1；API 400 `ArtifactNotPublished: artifact k8s/k8s:v1.35.8 is not published`。
③修正后重试：PUT 回 5003 → componentmeta rules=3 → 同 payload POST 建群 200（探针集群随后
删除）。零集群/操作残留。附带清理了 R12 遗留 `.r12bak` 文件。

**C3（1.1-10 公共 CA 等价）**：自签 r13-kc-test-ca（3 天效期，SAN=IP 172.16.131.208+DNS
sh-dev-2）加入三节点系统信任库，`package-registry.json` **零凭据零 CA 字段**（等价公共 CA
环境），7443 TLS-only registry → 探针集群 r13-ca 建群 Running，纯 TLS 拉取 231 次
（208=218/146=13）走系统信任池，etcd tags/list 404 属预期。**运维发现：Go crypto/x509
首用时加载并进程内缓存系统根池——加 CA 前启动的 kc-server/kc-agent 报 500 x509 unknown
authority，`systemctl restart kc-server kc-agent`（三节点各 agent）后立即恢复；向系统信任库
加 CA 后必须重启平台进程。**

**C4（1.3-05 join 认证）**：drain 230 → 8443 HTTPS+htpasswd（r13，bcrypt）→ r13-auth
两节点集群认证拉取 → join 230 下发 0600 `package-registry.json` → PUT /clusters/r13-auth/
nodes 扩容 230 → agent **直连**认证拉取 9×200（auth.user.name=r13）→ k8s 三节点 Ready。
错误口令 join：EXIT=1 可读 `UNAUTHORIZED: authentication required`，**零部分安装**（agent
二进制/服务均未创建）。过程发现：①join 多网卡 precheck 对显式 `first-found` 仍报
"--ip-detect not specified"（sudo.go:131 判断 ipDetect != "" && != MethodFirst 才豁免），
后台交互确认 EOF → FATAL；合法语法 `--ip-detect interface=ens3`（`ifname=` 无效）。②嵌套
SSH 多层引号内插剥引号 → agent 端 JSON 解析失败（r13-auth 首跑 InstallFailed 根因）；跨节点
写配置应 base64/python 落盘。

**C5（1.1-03/08 纯离线 bundle）**：5003 export（skopeo --preserve-digests，5 制品 368MB；
重写 bundled manifest digest 为单 manifest 子 digest：bootstrap ba950f5a→836f20d5）→ scp
离线拷贝+sha256 校验 → 空白 9443 import ×2，Inventory 逐字节全等。iptables OUTPUT 专用链
R13_OFFLINE（两节点；lo/RFC1918/ESTABLISHED RETURN，其余 REJECT）：外网 DNS/连接全拒
（REJECT 计数 dev-2=329/dev-3=269 包，含 6×bootstrap/etcd tags 探测 404 属预期），
146:5003 与 208:9443 均 200。ConfigMap+三节点 0600 json 切 9443 → componentmeta 9443 →
r13-bundle 建群 Running（10:52:17 起 ~6 分钟；9443 拉取 183 条：208=171/146=12，五类仓库
全覆盖，177×200+6×404）；2 节点 Ready，calico-apiserver×2/calico-node×2/typha/controllers/
csi 全 Running，无非 Running pod。**Addon**：componentmeta addons 仅平台自带四类
（cni calico×2/cri containerd/k8s/k8s-extension）——bundle 无第三方 addon 包，addon 生态
不在 release manifest 制品范围（已知边界）。`kcctl delete cluster r13-bundle` 清空。

**升级（同轮验证，防护双拦截）**：①bundled manifest（digest 836f20d5）对 5003 tag
（实际 ba950f5a）：EXIT=1 "refusing to upgrade from a repointed tag"——digest 防护在触碰
节点前生效；②retarget 9443：rollout 6 节点完成（备份 mtime 实证 `cp -a` 保留；节点级
stop→backup→install→start），平台 Healthy/healthz 200，最终校验 EXIT=1：平台 API 报
e9d9afe ≠ manifest 期望 e9e95f4。**根因（非升级缺陷）**：5003 rc.8 包 `kc-package-manifest.json`
sourceRevision=None（publisher 仅在非空时写 revision 标签/标签链），包内二进制实际构建自
e9d9afe（BuildDate 2026-09-22T01:53:53Z；与升级前平台二进制 md5 逐字节相同：staged==backup==
installed），而 release manifest 手写声明 e9e95f4 → 制品元数据缺失使 entry 级校验（空
sourceRevision 放行）无从比对，rollout 后平台 API 比对成为唯一防线并正确拦截。
防再发：发布时设 `KC_SOURCE_REVISION`（`verify_core_binary_metadata` 强校验三二进制
gitCommit 与声明一致）；建议 bootstrap 类包 SourceRevision 必填。勘误：早期记录"agent
version 报 e9e95f4"系误读升级日志 `(revision e9e95f4cff4e)` 回显（该值为 manifest 目标，
非二进制内容）。

**终态清理**：r13-kc-test-ca 三节点信任库移除+`update-ca-certificates --fresh` 复验 bundle
0 命中；7443/8443/9443 测试 registry 停止+`/tmp/r13-reg*`（含 htpasswd、口令文件、数据）
删除；`/tmp/r13-ca`（CA 私钥）、`/tmp/.r13-cert.*`（API 客户端证书）、`/tmp/r13-bundle*`、
`/tmp/kubeclipper-upgrade`（staging+备份）、dev-3 `/tmp/r13-bundle-work`、`/tmp/r13-import1`、
230 残留全部删除；`/tmp/r13-evidence/` 存档并脱敏（basic-auth b64 → `[REDACTED]`），另拷贝
至工作区 `r13-raw-evidence/`；配置（三节点 0600 json → 146:5003）、ConfigMap（PUT 200，
rv→174911）、iptables（-D/-F/-X）全还原，baidu/github 200 复验外网恢复；componentmeta 复验
146:5003 rules=3；平台 3 node Ready、healthz ok、无集群。共享 Registry 146:5003 只增 tag
未动（caas4/* 原样），`/var/lib/kc-etcd` 未动。checklist 1.1-03/08/09/10、1.3-05、
gaps P0 行 2/9 与 R13 段、remediation plan §6.3/§6.4 已同步回填。

### 12.10 R14 代码轮（2026-09-22）：B6 发布门禁实施与 bootstrap SourceRevision 必填

R13 §12.9 定性的 5003 rc.8 `sourceRevision=None` 成因（手工发布路径忘设 `KC_SOURCE_REVISION`
时 publisher 静默接受空值）与 plan §7.3-3 的发布门禁缺口，本轮以代码收口（无真机操作）：

**发布侧 fail closed**：`pkg/delivery/publisher` `Publish()` 对 bootstrap 类包在
`SourceRevision` 为空时直接拒绝（错误信息指引 `KC_SOURCE_REVISION`）；消费侧 indexer 保持
宽松兼容存量包，`upgrade fetchPlatformPackage` 对空 `sourceRevision` 增加 rollout 前告警。
CI 与 bootstrap 脚本已设值不受影响；`oci-migrate`（legacy 迁移工具，不传 revision）迁移
bootstrap 会被正确拒绝。单测 `TestPublishBootstrapRequiresSourceRevision`（缺 revision 拒绝/
带 revision 落 `org.opencontainers.image.revision` 标签/非 bootstrap 兼容），
`go test ./pkg/delivery/publisher/... ./pkg/cli/upgrade/...` 全绿。

**B6 门禁**：`scripts/open-packaging/release-gate.sh`（稳定 tag 格式；qualification manifest
契约 kind/version/sourceRevision/bootstrap artifact revision 与候选 SHA 全等；验收记录
`docs/testing/acceptance/<SHA>.yaml` 存在、result: passed、release_manifest_sha256 与下载的
qualification manifest 全等）+ 自测 `test-release-gate.sh` 11 例（1 正向+10 阻断）离线全绿。
release workflow 新增 `release-gate` job：tag 未移动校验（`git rev-parse tag^{commit}` ==
GITHUB_SHA）、Actions API 解析同 head_sha 成功 qualification run（无则阻断）、下载
`oci-release-manifest-*` artifact、执行门禁脚本；`publish`/`build-cli` 均 `needs: release-gate`。
验收记录格式与流程约定见 `docs/testing/acceptance/README.md`。制品 digest 校验职责保持在
qualification（verify-release-manifest.sh）与发布后 manifest job，门禁只校验绑定。

**关闭余量**：门禁正向放行/负向真机阻断（缺记录、错 SHA 的真实 release 触发）待下一候选
发布轮；checklist 6-04 保持 ⚠️。文档同步：plan 头部 R13/R14 更新、B5/B6 表行、§7.6，
checklist 6-04。

### 12.11 R15 追加轮（2026-09-22）：B5 包 contents 路径真机篡改探针闭环

B5 矩阵最后一项可真机执行项（"包 contents 路径的等价真机探针"）闭环。环境：rc.8 三机，
dev-2 :9443 测试 registry（distribution 3.1.1，由共享 5003 skopeo 逐 tag 拷贝，共享源只读），
deploy-config `packageRegistry` 与四节点 0600 delivery json 切 9443，componentmeta rules=3。

**三层防御实证**（篡改 `kubeclipper/packages/cri/containerd:1.7.29` 的 configs 层 blob
`b8c094e2…`，69064877 字节；registry 对 blob 内容与路径 digest 不符不做在线校验、仍 200，
为本轮观察项）：

1. **浅篡改（tar 头损坏）→ server indexer 剔除**：下载后解析归档失败，warn
   `skip invalid OCI package image ...: read package manifest failed: archive/tar: invalid tar header`，
   包从清单剔除，componentmeta 中 containerd 1.7.29 消失。
2. **解析期 fail-fast**：显式指定被剔除包建群 → CLI
   `k8s version v1.35.8 unavailable, missing packages: containerd 1.7.29; publish or sync
   them to OCI package registry ... first`，EXIT=1，零对象落库。
3. **深篡改（20MiB 数据区翻转，tar 头可解析）→ agent 侧 gzip CRC 阻断**：
   两节点 `installRuntime` 步骤（step 51fdc2e7）attempt 0/1 均失败，任务状态回写审计逐字
   `"message":"gzip: invalid checksum","reason":"ExecutionFailed"`（4 条：task-88481d4e/
   5b45c5e3/a3ac789e/d99183d5，nodeRef 208+146），3 秒内 Failed、无半装；operation
   7b6b9271 Failed，cluster InstallFailed → force delete 零残留。错误分类为 gzip 校验
   （解压阶段 CRC）而非 digest mismatch——agent 未在解压前做 blob digest 比对，实际完整性
   由 gzip/tar 解析链兜底；防御结果正确（错误内容未被执行），报错可读。

**正向对照（排除误伤好包）**：恢复 blob（删损坏文件后从 5003 skopeo 重拷，sha256 复原
b8c094e2…、size 69064877、HEAD 200）+ 两节点清 1.7.29 缓存 + 显式版本建群（v1.35.8 /
containerd 1.7.29 / calico v3.29.6）→ **Running**（CreateCluster Succeeded 15 步，
9443 access log 实拉 GET 200，agent UA go-containerregistry/v0.20.2，k8s blob
2265495c 56MB 在列）→ 删除零残留。

**终态还原**：deploy-config `packageRegistry: 172.16.131.146:5003`（rv 183118→186582）、
四份 delivery json（dev-2 server+agent、dev-3、dev-4）还原 0600、componentmeta 复验
registry=5003 rules=3；9443 registry 停止+`/tmp/r15-reg` 删除、`/tmp/.r15` 客户端证书删除、
R13 遗留（dev-2 `/tmp/r13-evidence`、dev-3 `/tmp/r13-bundle`、`r13-sync-manifest.yaml`、
`r15-copy.log`）清理；共享 5003 与 `/var/lib/kc-etcd` 未触碰。

**证据保存注记**：dev-2 `/tmp/r15-evidence/`（23 文件，tar sha256
`357f234c…d512b439e`）在收尾清理时被误删（未先解包留存），关键输出已由执行会话记录逐字
重建为 `kc-fix2/r15-raw-evidence/r15-evidence-reconstructed.md`（含来源说明与原始文件清单），
本文引用的错误文本/SHA/访问日志均出自该重建文件。

文档同步：plan §6.3 矩阵"缓存篡改"行、§6.4 R15 段、B5 表行；checklist 2.6-05（补 R15 探针）、
2.6-06（补缓存清除后真实重拉佐证）；gaps P0 行 8。

### 12.12 R16 追加轮（2026-09-23，rc.8 e9d9afeb）：2.1-30 子项收口 + 2.6-02/07/10 余量

**范围**（用户批准四项）：①2.1-30 剩余子项（超时注入 spec deadline + Watch 重连 + kc-server/kc-agent 重启注入）；②2.6-02 packagePlan 凭据不落库固化取证；③2.6-10 余量（缺失 blob + 多候选冲突）；④2.6-07 收口（配置优先级矩阵 + 凭据脱敏）。

**T1 Server 重启注入**：kc-server 于 41s 安装任务中段重启（journal Stopping/Stopped/Started 同秒，健康探测循环 60s 误报下线——实际 <1s），HA 三副本在途任务无感知照常完成（operation Succeeded / cluster Running），重启后任务写回 20 条；agent 各连本地副本无错误。

**T2 Agent 重启注入**：两轮"空闲窗口"重启均无副作用（留证）；T2c 确定性踩中恢复路径——iptables OUTPUT DROP 5003 使 dev-3 下载步挂起 → `systemctl restart kc-agent` → worker 自动重新 reconcile 同一 Running 任务（reconcile#1 01:54:39 挂起，reconcile#2 01:55:22 重跑）→ 解封后 01:55:34 完成，Operation Succeeded、零残留。实证 worker 恢复链：eligibleTasks（本节点非终态）→ getLiveTask（非 terminal）→ startPendingTask/继续执行；Watch 重连由 client-go reflector 自动 relist 覆盖。

**T3 超时注入**：kcctl create cluster 无 timeout flag，改 API `POST /clusters?timeout=120`（dryRun 先行 200 验证 payload）。实测：Operation `Spec.Timeout=2m0s`、deadline=02:15:04=start+2m 精确落 status；deadline 恰到时运行中 `kubeadm init` 步被 SIGTERM（agent 日志 `run command failed(signal: terminated)`、`run kubeadm init error`），任务回写 `phase=TimedOut reason=DeadlineExceeded message="task deadline exceeded"`（server 发起，audit 佐证），Operation 终态 `phase=TimedOut reason=DeadlineExceeded message="operation deadline exceeded"` finishedAt==deadline，Cluster→InstallFailed，Pending 步不派发。force 删除后按卸载语义手动清理双节点（kubeadm reset、kubelet/containerd stop+disable、/var/lib/{etcd,kubelet,containerd,dockershim}/etc/{kubernetes,containerd,cni}/tmp/.k8s/包缓存 k8s+cri 版本目录），复验 6 目录不存在、双节点 enabled=0、平台 kc-server/kc-agent/kc-etcd 与 /var/lib/kc-etcd 完好。

**2.6-02 packagePlan 取证**：GET T1 创建的 Cluster 对象（6930 字节）——packagePlan 字段集合与 schema 完全一致（ResolvedArtifactPlan/ResolvedComponent/TransportRef/ArtifactContent），整对象 `password|credential|token|secret|username` 大小写不敏感扫描 0 命中，4 components 全指向 5003。checklist 2.6-02 升 ✅。

**2.6-10a 缺失 blob**：dev-2 9443 测试 registry（distribution 3.1.1，delete 不需要——直接删数据文件）+ dev-3 skopeo 从 5003 拷入 k8s/k8s:v1.35.8、k8s-extension:v1、cni(calico v3.29.6 走 charts `kubeclipper/charts/tigera-operator`，packages/cni/calico 不存在)、cri/containerd:1.7.29。删 layer blob（sha256:b8c094e2…）数据文件：registry GET 404（日志 `blob unknown to registry`）、manifest 仍 200 → server 索引器读包清单需取 blob 失败 → tag 记 `skip invalid OCI package image` 剔除 → POST 显式 containerd 1.7.29 返回 400 `ArtifactNotPublished: artifact cri/containerd:1.7.29 is not published`，零对象；componentmeta `unavailable[]` 记 `reason: notPublished`。R15 观察项（distribution 对 blob 路径 digest 不符不在线校验）不改变结论。

**2.6-10b 多候选冲突**：policy `k8s-v1.35` 追加第二个允许同版本 containerd 的 slot（cri-alt）→ dryRun 与真实 POST 均 400 `DuplicateResolvedComponent: component cri/containerd selected by slots "cri" and "cri-alt"`；同 slot 重名 PUT 即 400 `duplicate component slot "cri"`；单 slot 双 option（name 不同）不冲突——`matchesPackageCandidate` 按 option.name 匹配，非错误路径。policy 字节级还原后 dryRun 200。

**2.6-07 优先级矩阵 + 脱敏**：①server delivery json=9443 + deploy-config=5003（重启 kc-server 生效）→ componentmeta `registry: 172.16.131.146:5003`、dryRun plan 全 5003——deploy-config 单一事实源；据此**更正**此前"json 覆盖 deploy-config"记录（R13 双写同值无法区分）。②8443 htpasswd registry（bcrypt，python3 bcrypt 生成——htpasswd 工具缺失、{PLAIN} 不被 3.1.1 接受）：正确口令 componentmeta 200、错误口令 401（server 侧 401 日志仅 `authentication failure` 无凭据）；口令在 server json、GET API、kc-server/kc-agent journal、/etc /root /var/lib/kubeclipper 全量 grep 0 泄漏；探针后 htpasswd/凭据 json 即删。注：componentmeta 探针实际未走到 8443（indexer 5min TTL 缓存 + registry 解析时序），以 9443 前序同构矩阵 cell + curl 直连 8443 认证矩阵为凭。deploy-config 与双 json 全部还原 5003。

**终态**：双测试 registry 停止（ss 0 监听）、/var/lib/r16-registry、/tmp/r16-evidence（含 /tmp/.r16 客户端证书）、/tmp/r16-8443-auth 删除；零集群、零 Operation、节点 3/3；共享 Registry 146:5003 只读未动、/var/lib/kc-etcd 未触碰。R16 原始证据目录在终态清理中随会话结束删除（延续 R15 处理方式），逐字结论转录本节与 checklist/gaps/plan。

文档同步：plan §3.2 R16 段、§6.3 新增"blob 缺失"行、§6.4 R16 段、B2/B5 表行；checklist 2.1-30（超时注入）、2.6-02 ✅、2.6-07 ✅、2.6-10 ✅；gaps P0 行 8。

### 12.13 R17 追加轮（2026-09-23，rc.8 e9d9afeb）：平台重 deploy + 部署侧断连（B5 余量）+ Master 增删（2.2-03/09/10）

**范围**（用户批准两项，含 /var/lib/kc-etcd 重建授权；arm64 继续排除）：①B5 余量——平台本体从 OCI registry 重 deploy + 部署侧断连注入三场景；②Master 增删真机验证（2.2-03/09/10）。平台为 clean 后全新重 deploy 的 rc.8（见场景 C），此前 /var/lib/kc-etcd 被整体重建。

**前置护栏**：bootstrap 清单前置核验（4 kinds 齐全才准 clean）、registry.bin sha256 双侧一致守卫（3162d930…）、kc-etcd snapshot + deploy-config 备份（/root/r17-backup）后才执行 `kcctl clean -A`。clean 范围实证：卸载服务、/etc/kubeclipper-*、/etc/kc-console、/var/lib/kc-etcd、/usr/local/bin/{kubeclipper*,etcd*,caddy}、~/.kc（含 deploy-config.yaml）；不动 /var/lib/kubeclipper/cache 与 registry 二进制。

**场景 A——precheck 期断连**：clean 后以不可达 registry 地址 deploy（scheme 经 `KC_PACKAGE_REGISTRY_CONFIG` 环境变量回退 http——clean 删除已安装 package-registry.json，解析链 flags > env > 已安装文件 > 默认 https）→ 15s 硬超时内双 scheme（https+http）探测快速失败，锚点日志 `PACKAGE-REGISTRY PRECHECK OK!`（deploy.go:1764）未出现，报错含 `kcctl registry sync` 提示，零节点影响（precheck 先于任何节点动作）。

**场景 B——sendPackage 期断连**：precheck 通过（5003 恢复可达）→ 证书已分发到节点后 iptables tcp-reset REJECT 5003 → sendPackage 阶段 `refresh bootstrap assets from registry ... connection refused` abort（exit 1），无重试；半安装态=证书已分发+包缓存；恢复网络后无需 clean 直接重 deploy 成功——错误可观察、可重试、半装态不被误用。

**场景 C——完整重 deploy**：`kcctl deploy -c /root/r17-backup/deploy-config.yaml`（配置模式复用 agentID，节点身份保持）→ ~4 分钟成功：4 服务 active（kc-etcd 12379/12380/12381 全新 3 成员、kc-server、kc-console、kc-agent）、3 节点 Healthy、bootstrap 包取 5003 最高 semver tag（rc.8）、healthz/console 200、package-registry.json 重新生成（http scheme）、dumpConfig 重新生成等价 deploy-config。**deploy 非幂等实证**：对已部署平台直接 deploy 被 preCheck 拒绝（"clean old environment before deploying"）→ 重 deploy 必须 clean -A 前置。

**P0 新发现——离线建群必须显式 imageRegistry（gaps P0 行 10）**：v1 建群 payload（无 `imageRegistry`）双 master（dev-3+dev-4）创建 → kubeadm.yaml 不渲染 `imageRepository`（默认 registry.k8s.io）、containerd certs.d 无本地镜像映射 → 离线环境 `kubeadm init` 从公网拉 7 镜像 i/o timeout，单 attempt ~18 分钟、5400s deadline 内每 ~18 分钟循环重试后 Failed，创建前零校验零提示。代码链：`getClusterMetadata → ResolveClusterImageRegistry` 空 name → 不渲染 imageRepository；`DownloadImage` 仅 Upgrade 路径消费且 OCI 交付下直接报错（pkg/scheme/core/v1/k8s/cluster.go:397 `"upgrade Kubernetes image archives are not supported with OCI package delivery"`），创建路径无镜像 load 步骤。**修复实证**：v2 payload 加 `"imageRegistry": "kc-package-registry"`（Registry 对象名，deploy 时自动创建，http/5003）→ `ResolveImageRegistry` 同源驱动 kubeadm `imageRepository: 172.16.131.146:5003` 与 containerd certs.d hosts.toml（http endpoint）→ 20/20 OperationTask Succeeded、双 master Ready。R15/R16 建群成功系节点 containerd 遗留镜像掩护，R16 清理 /var/lib/containerd 后暴露本缺口。

**Master 增删——产品缺口复证（2.2-03/09/10，gaps P0 行 5）**：双 master 集群 Ready 后，PUT `/api/core.kubeclipper.io/v1/clusters/{name}/nodes` role=master 恒 400 `invalid node role`——`makeMasterCompare`（pkg/clusteroperation/node.go:101-104）无条件 `return ErrInvalidNodesRole`（注释 `// support later`），doMakeOperation master 分支同拒；etcd member remove/证书清理代码路径不存在。定性：产品缺口而非用例未测。

**Worker 增删——v2 operation 正向首证**：add（dev-2 入双 master 集群）→ Ready，Operation 下 28 个 OperationTask（task-XXXX 独立资源、spec.operationRef/stepID/nodeRef 关联，按 stepID×节点派发）全部 Succeeded；remove → 节点移出、集群标签清空、/etc/hosts 集群条目清除、无 kubelet/containerd 二进制残留。NEG1/NEG2 负向均 400。小观察项：`/tmp/.k8s` 不随 worker remove 与 cluster delete 清理。

**cancel/删除顺序语义**：Installing 中 force delete 400 拒绝（"can't delete cluster when cluster is Installing"），徒劳重试步每 ~18 分钟循环 → 先 `POST /operations/{name}/cancel`，body 须**平铺** `{"uid":"...","resourceVersion":"..."}`（readControlRequest，handler.go:505；`{}` 与 `{"metadata":{...}}` 均 400）→ phase Canceled → Cluster InstallFailed → force delete 成功。

**终态**：测试集群删除、3 节点 Healthy 无集群标签、平台 rc.8 全绿（4 服务 active、healthz/console 200）、共享 Registry 146:5003 未动（sha 守卫一致、5003 200）、/var/lib/kc-etcd 为重 deploy 后全新、/tmp/r17、/root/r17-evidence、/root/r17-backup 及 dev-3 备份全删。原始证据在终态清理中删除（延续 R15/R16 处理方式），逐字结论转录本节与 checklist/gaps/plan。

文档同步：plan 摘要 R17 更新、B5 表行、§6.3"纯离线/拉取途中断连"两行、§6.4 R17 段、矩阵余量行；checklist 2.2-01/02（R17 复测）、2.2-03（产品缺口定性）、2.2-09/10（同 2.2-03）；gaps P0 行 5 更新、P0 行 10 新增、R17 轮段落。

### 12.14 R18 追加轮（2026-09-24，rc.9 5e4cfb4b）：Fix A imageRegistry 创建前拒绝 + Fix B master add/remove 真机闭环

**范围**（用户批准实施 R17 定性的两项 P0；arm64 继续排除）：①Fix A（gaps P0 行 10）——离线建群 payload 未显式 `imageRegistry` 时创建前秒级 400 拒绝（替代 ~18 分钟 kubeadm init 慢失败）；②Fix B（gaps P0 行 5）——master add/remove 完整实现（替代无条件 400 `invalid node role`）。

**代码与单测**（commit `5e4cfb4b`）：
- Fix B：`pkg/clusteroperation/node.go` makeMasterCompare（add：Complement 过滤+append；remove：Intersect+quorum 守卫——剩余 <2 报 wrapped `ErrInvalidNodesTopology`，handler errors.Is 映射 400）+ makeMasterOperation 完整 add/remove 步骤序列（与 worker 对称）；`pkg/scheme/core/v1/k8s/node.go` MakeInstallSteps master 分支（`JoinCmd.ControlPlane=true`，unwrapAvailableMasters 按 patchNodes ID 排除被操作节点——AvailableKubeMasters TCP 6443 探测）与 MakeUninstallSteps master 分支（drain 前 `EtcdMemberRemoveSteps(masters[0], patchNodes)`、KubeadmReset 后 removeEtcdDataDir、clearVIPDomain 后 LvsCareRefreshSteps）；`masterscale.go` 新建（EtcdMemberRemove：etcdctl 经 /etc/kubernetes/pki/etcd 证书按 `https://<ip>:2380` peer addr 匹配成员 ID 显式 `member remove`，找不到 warn 继续；LvsCareRefreshSteps reconcile，**leaving 参数过滤离开节点**——修复 Builder append-back 使离开节点泄漏进 IPVS rs 列表的真 bug，单测抓到）；JoinCmd.Install ControlPlane 分支：`kubeadm token create --print-join-command` + `kubeadm init phase upload-certs --upload-certs` 刷新 kubeadm-certs 并以 `certificateKeyRegex` 提取证书密钥，手工拼 `kubeadm join EP --token T --discovery-token-ca-cert-hash H --control-plane --certificate-key K`（无 --cri-socket，保证 KubeadmConfig.Install 按空格 split 解析）。
- Fix A：`pkg/apis/core/v1/handler.go` createClusterCheck 在 masters 检查后拒绝 offline+空 imageRegistry（400，文案 `offline cluster requires an explicit imageRegistry (a Registry object name); otherwise kubeadm pulls control-plane images from registry.k8s.io which is unreachable offline`）。
- 单测：masterscale_test.go（extractCertificateKey 表测、findEtcdMemberID、TestGenNodeMasterInstall/UninstallSteps——含 availableMasterListener 6443 监听与 leaving 过滤断言、uninstall 需存活 control-plane 否则报错）、node_test.go（add/removeMaster、quorum 400 哨兵、per-case fixture 防测试污染）、handler_test.go（offline 无 registry 400 含 imageRegistry、有 registry 放行、online 空值放行）。全量回归仅 systemctl dbus 环境性失败。

**发布与升级**：本地构建（gitTreeState=clean + KC_SOURCE_REVISION 经 wrapper 注入）→ KC_OCI_PUBLISH_BIN wrapper 经 dev-2 发布 `v2.0.3-rc.9`（Mac 不可达 5003；共享 Registry 仅增 tag）→ /tmp/kc-r9-manifest.yaml 写 tag 顶层 index digest `sha256:222eb5d3…`（HEAD Docker-Content-Digest 确认；oci-publish stdout 的 `a6e5e15f` 是子 manifest digest，不能写 manifest——重申 §12.6 digest 教训）→ `kcctl upgrade all --manifest --package-registry-scheme http` 三机 6 槽位 server+agent `e9d9afeb` → `5e4cfb4b`（~66s），platform API 报 rc.9，doctor 25/25。升级 WARN `skip invalid OCI tag v2.0.3-rc.5`（该存量 tag MANIFEST_UNKNOWN）非阻塞——升级逻辑正确跳过坏 tag，记录为观察项。运维失误记录：升级后误用无版本 ldflags 的本地二进制覆盖 /usr/local/bin/kcctl，已从 /var/lib/kubeclipper/cache/packages/bootstrap/kubeclipper/v2.0.3-rc.9/…/contents/kcctl 恢复官方版本（version 报 v2.0.3-rc.9 `5e4cfb4b`、doctor 25/25 复验）。

**Fix A 真机复验**：dev-2 API（客户端证书，/tmp/kc-r18-api-*，0600，用后即删）真实 POST clusters（离线 annotation、无 imageRegistry）→ **48ms** 400 文案精确；dryRun 同 400；clusters 数组 0（零对象）。对照组：同 payload 加 `"imageRegistry": "kc-package-registry"` dryRun 200。

**Fix B 真机复验**：r18-cluster（双 master dev-3+dev-4，v1.35.8/calico v3.29.6/containerd 1.7.29，离线+imageRegistry=kc-package-registry，**API 创建**——kcctl CLI 守卫拒绝偶数 master "master node must be odd"，API 无此限制）~8 分钟 Running、etcd 2 成员。
- add dev-2：PUT /clusters/r18-cluster/nodes `{"operation":"add","role":"master"}` → Operation `68edfcb8` Succeeded，8/8 OperationTasks Succeeded；步骤计划（v2 spec，payload.step.name/action）：installRuntime/installExtension/installPackages/nodeEnvSetup（208 加入节点）→ **getJoinCommand（146 存活 master）** → renderMasterJoinConfig（payload IsControlPlane=true）/joinNode（208）→ waitForAddedNodesReady（146）；dev-2 Ready control-plane（join ~105s），etcd member list **3 成员全部 started**（146/230/208），集群 Masters 含 3 节点、packagePlan 不漂移。
- remove dev-2：PUT remove → Operation `2f1960bf` Succeeded，13/13 OperationTasks Succeeded；步骤计划：**removeEtcdMember（146 存活 master，Action=uninstall）** → drainNode（146）→ kubeadmReset/removeEtcdDataDir/removeKubeletDataDir/removeDockershimDataDir/clearIPVS/removeDummyInterface/removeKubernetesConfig/clearVIPDomain/unInstallPackages/unInstallExtension/uninstallRuntime（208）；etcd 回 **2 成员**（146/230 started），集群回 Running、controlPlaneHealth 两节点 Healthy、pendingOperations 清空。
- 离开节点清理核验（dev-2）：/etc/kubernetes、/var/lib/etcd（集群 etcd 目录）、/etc/kubernetes/manifests 全不存在；dummy 接口 kube-lvscare-vip 不存在、169.254.169.100 地址 0 处；ipvsadm 空；kubelet/containerd inactive；kubectl `nodes "lixd-dev-2" not found`。
- 负向：remove 剩余 2 master 之一 → 400 `invalid nodes topology: removing 1 of 2 master nodes would break etcd quorum, at least 2 masters must remain`（直调与 `?dryRun=true` 均拒）；dryRun 拒后集群 Running/2 master/0 pending 零副作用。另证：nodes 端点对已移除节点 id 报 400 `nodes is already removed`。

**边界标注**：本轮拓扑 0 worker——worker 侧 kube-lvscare static pod 的 refreshLvsCare reconcile 步骤按设计不生成（VIP 刷新仅在有 worker 时需要），该路径未真机覆盖；单测覆盖 joining/leaving 节点过滤。master 自身 VIP 侧清理（clearIPVS/removeDummyInterface/clearVIPDomain）已在 remove 后核验。API 操作对象查询备忘：operations 路径 `/api/operations.kubeclipper.io/v1alpha1/`（非 /apis/）；无 per-operation /tasks 子路径，用 `/operationtasks` 列表按 spec.operationRef.name 过滤；v2 operation step 的 name/action 在 `payload.step` 内（payload 为 dict 非 base64 字符串）；`?dryRun=true` 为 query 参数（body 内 dryRun 字段不识别）。

**终态**：r18-cluster 删除（clusters 空、dev-3/dev-4 /etc/kubernetes 与 /var/lib/etcd 清空、kubelet/etcd/containerd inactive——kubectl 不可用属测试集群整体卸载预期）、3 节点 Healthy 无集群标签、doctor 25/25、platform rc.9；共享 Registry 146:5003 仅增 v2.0.3-rc.9 tag（13 个 v2.0* tag），caas4/* 等存量未动；/var/lib/kc-etcd 三节点未动；dev-2 /tmp/kc-r18-api-*（证书）与全部 /tmp/kc-r18-*、kc-r9-* 产物删除，Mac 侧 wrapper/二进制删除；证据逐字转录本节与 checklist/gaps/plan。

### 12.15 R19 追加轮（2026-09-24，rc.10 c356fbafdb70）：废弃入口清理（Docker CRI + 迁移工具）+ B1 step 2 console/kcctl 升级 + P0 行 1 故障窗口

**范围**（用户批准："docker 移除，如果还在保留的话就处理了……其他的问题也按顺序处理"；arm64 无环境不验证，用户确认理论无碍）：①Docker CRI 废弃入口移除（保留显式拒绝门禁）与 legacy 迁移工具删除；②console/kcctl 组件升级实现（B1 step 2）；③rc.10 发布 + 三机 `upgrade all` + 组件独立升级真机 + 负向；④P0 行 1 故障窗口验证（Watch + Console 入口证据）。

**代码与单测**（commit `018a59fd`、`c356fbaf`）：

- `018a59fd` refactor!（Docker CRI + 迁移工具，21 文件）：CLI `allowedCRI` 仅 containerd、`--cri docker` 显式拒绝（"Docker CRI is not supported, use containerd"）、Use 串与 flag help 清理 docker 字样；服务端 `AllowedCRIType` 仅 containerd、`CRIDocker` 常量与 enum 删除、**`createClusterCheck` 新增 CRI 类型显式校验**（docker → 400 可读拒绝，不再漏到 operation build 才 500）；`cri_util.go` GetCriStep 删 docker case（落入 "not supported" 运行时门禁）、clustercontroller 删 DockerInsecureRegistryConfigure 分支、kubeadm provider patchCRI 删 docker case、删除 `pkg/scheme/core/v1/cri/docker.go` 全文件与 cri.go 注册；schema_test fixture docker→containerd。保留 `removeDockershimDataDir` 通用卸载清理与 `kcctl registry` 独立命令。删除 `scripts/migrate-legacy-packages-to-oci.sh`，docs/oci-delivery.md §7 改"已移除"说明。负向单测：服务端 TestCreateClusterCheckRejectsDockerCRI（docker/cri-o 双例）、CLI TestCreateClusterRejectsDockerCRI（直接调 ValidateArgs——完整 cmd.Execute 会先连服务器且 CheckErr os.Exit(1) 不可测；help 断言只查 `--cri string` 行，全文件含 docker 会误伤 calico docker0 bridge 合法文案）。
- `c356fbaf` feat(upgrade)（console/kcctl 组件升级，3 文件 +550 行）：固定执行顺序 `roleOrder = Server→Agent→Console→kcctl`（§2.3-6）；`requiredArtifactsFor`：console→bootstrap/console 单制品、all→kubeclipper+console 双制品、server/agent/kcctl→kubeclipper——缺制品在触碰节点前拒绝；`fetchPackageFiles` 按 artifact 泛化，strictRevision 仅对 kubeclipper 包严格，**console 包 sourceRevision mismatch 降为警告**（console v1.6.0 独立版本流与平台 semver 不可比，manifest digest 绑定是完整性保证）；console 节点步骤 stop kc-console→backup caddy+dist.tar→replace→tar 解压到 dist-extract 子目录再 `cp -a`（避免归档与解压目录同名冲突）→start→ConsolePort HTTP 2xx 探测；kcctl 节点替换 /usr/local/bin/kcctl（无服务重启）后 `kcctl version` 校验 revision；restore 按角色分派（kcctl/console 恢复用 `[ -f ]` 守卫）；`parseVersionJSON`/`firstJSONObject` brace-matching 取第一个平衡 JSON——kcctl `version -o json` 带 "kcctl version:" banner 前缀且配置可达时输出 client+server **双对象**，跨两对象取 first-{ 到 last-} 会解析失败（自查发现并修正，附回归测试）；console 组件跳过 semver 版本策略（v1.6.0 配平台 v2.0.3-rc.x 会被误判为隐式降级拒绝一切 console 升级）；kcctl 复用 revision 守卫；`releasemanifest` 新增 `BootstrapConsoleArtifact()`。单测：TestBootstrapConsoleArtifact、TestRequiredArtifactsFor（server/agent/kcctl→kubeclipper 单、console→console 单、all→双且 kubeclipper 在前、no-console manifest 拒绝）、TestParseVersionJSON（纯 JSON/banner 前缀/双对象取第一/无 JSON）。

**发布链**：Mac 本地构建（构建门禁要求 gitTreeState=clean——`verify_core_binary_metadata` 死校验 gitCommit==KC_SOURCE_REVISION ×2 且 clean ×2，必须 commit 后构建；实测 revision=c356fbafdb7009cfa67f06b19a3a14ca8c03ca06、三二进制 metadata 校验通过）→ `KC_OCI_PUBLISH_BIN` wrapper 经 dev-2 发布 `bootstrap/kubeclipper:v2.0.3-rc.10`（Mac 不可达 5003；共享 Registry 仅增 tag，14 个 v2.0* tag）→ manifest（sha256 前 16 位 `aa30fac92c930d6d`，34 行）写 tag **顶层 index digest** `sha256:2845d0da079e…0856527`（oci-publish stdout 的 `42f350b8…` 是子 manifest digest，重申 §12.6/§12.5 教训；HEAD 须带 `Accept: application/vnd.oci.image.index.v1+json`）+ **console artifact**（bootstrap/console:v1.6.0，digest `sha256:084c4eade76d…adef284e`，sourceRevision=`e9e95f4…` 事实值——package artifact 强制 sourceRevision 必填，§12.10）。

**upgrade all 三机 12 槽位**：`kcctl upgrade all --manifest`，Server×3→Agent×3→Console×3→kcctl×3 全部成功（~106s）；platform API 报 `v2.0.3-rc.10 (c356fbafdb70)`；doctor 25/25；三节点 kc-server/kc-agent/kc-etcd/kc-console 全 active、console 全 200、`kcctl version` 全 rc.10。**附带修复**：dev-3/dev-4 /usr/local/bin/kcctl 残留漂移（原 `c27e61c0`，R18 无版本二进制事件遗留）被本次 kcctl 槽位统一至 rc.10 官方构建。caddy sha256 `4ef1f68c…` 升级前后一致（console 幂等重装同内容）。

**组件独立升级**：`upgrade kcctl --manifest` → 三节点全部 `already at revision c356fbafdb70` **skip**（同 revision 幂等跳过真机实证，EXIT=0）；`upgrade console --manifest` → 三节点同版本重装 v1.6.0 成功（/etc/kc-console/dist 与 caddy sha256 前后一致、HTTP 2xx 探测通过、console 200）；platform API revision 未校验（设计——console 无平台 revision 概念）。

**负向用例**：

| 用例 | 结果 |
|---|---|
| N1 缺 console artifact 的 manifest（head -22 裁剪，须以 kubeclipper sourceRevision 行收尾——裁多裁少都会先报别的错） | ✅ `upgrade console` 与 `upgrade all` 均 `release manifest contains no bootstrap/console package artifact` EXIT=1，preflight 拒绝零触碰 |
| N2 console 错 digest（尾字符 4e→4f） | ✅ `registry tag ... does not match the release manifest digest ... refusing to upgrade from a repointed tag` EXIT=1，kc-console 保持 active（preflight 拒绝零触碰） |
| N3 kubeclipper 包错 digest（尾 27→28） | ✅ 警告放行（digest 不符但 image config label `org.opencontainers.image.revision`==targetRevision，re-tag 回退设计语义，与 R13 bundle 场景同构），随后全 skip |

**P0 行 1 故障窗口**（rc.10 三机，全程零集群副作用，watch 目标 node）：

1. **停 dev-3 kc-server（客户端连 dev-2）**：dev-2 上 `kcctl get node --watch` 流不受影响（无 "watch stream ended"、进程存活）、`kcctl get node` 正常——quorum 2/3 服务。恢复 dev-3 active。
2. **停 dev-2 kc-server（被连接节点）**：watch 日志先见既有 MODIFIED 事件（agent 心跳，流确实活着）→ `watch stream ended; reconnecting...` → 重连尝试 `dial tcp 172.16.131.208:8080: connect: connection refused` → **进程退出**（实现为连接失败即 return err，仅流正常结束后单次重连）；`kcctl get node` 同错误。**发现：kcctl 客户端单地址无 failover**——`/root/.kc/config` 单 server 地址，不尝试 dev-3/dev-4。
3. **窗口期 console（caddy `health_uri /healthz`、`health_interval 10s`、round_robin 三上游）**：dev-2 console 登录页 200（caddy 本地 file_server 静态服务）；`http://172.16.131.208:80/api/core.kubeclipper.io/v1/nodes` 返回 **403 JSON `{"code":403,"message":"Forbidden"}`——来自存活上游 kc-server 而非 caddy 502**，即健康检查已摘除 208 坏上游、/api 代理落 dev-3/dev-4；dev-3/dev-4 console 均 200。
4. **恢复 dev-2**：kc-server active、healthz 200、`kcctl get node` 恢复；新建 `get node --watch` 立即 list + ADDED×3 + MODIFIED（dev-3 心跳）流式正常——重连（重建立）成功。

**终态**：三节点 kc-server active、console 全 200、healthz 200、doctor 25/25（upgrade all 后复验）；共享 Registry 仅增 v2.0.3-rc.10 tag（14 个 v2.0* tag），存量与 caas4/* 未动；/var/lib/kc-etcd 未动；dev-2 /tmp/kc-r19-publish（oci-publish/kcctl/两份 manifest）与 watch 日志删除，Mac /tmp/kc-r19-publish 删除；无临时凭据残留。证据逐字转录本节与 checklist/gaps/plan。

文档同步：plan 头部 R19 段、B1 表行 step 2 收口、§2.5 执行状态；checklist 1.2-07 ✅、1.3-07 补注、1.3-10 ✅、1.3-09 补注；gaps P0 行 1 关闭、P0 行 4 console/kcctl 关闭（余量两项如实保留）、R19 轮段落。

### 12.16 R20 追加轮（2026-09-24，rc.11 9a1dbb7563262f1cf509bcb1a78364b9072f0c0b）：B1 余量收口——升级故障注入矩阵 + `--version` 在线正向下载

**范围**（用户指令："继续吧，处理全部遗留问题"）：①rc.11 发布；②升级故障注入矩阵四场景
（T-A/T-C/T-D/T-B）；③1.3-09 `--version` 在线正向下载+checksum（本地 GitHub 镜像法）；
④文档回填。无代码变更（纯验收轮）。

**发布链**：Mac 本地构建（构建门禁 gitTreeState=clean，revision=9a1dbb7563262f1cf509bcb1a78364b9072f0c0b，
三二进制 metadata 校验通过）→ `KC_OCI_PUBLISH_BIN` wrapper 经 dev-2 发布
`bootstrap/kubeclipper:v2.0.3-rc.11`（共享 Registry 仅增 tag，15 个 v2.0* tag）→ 顶层 index
digest `sha256:c28ebeedc60d22fba4a6eb451c624df5a5c8ce28f6e54cfc7cec98d15ccc649f`（stdout 的
`202062f7…` 是子 manifest，重申 §12.6/§12.5 教训）；内容 digest kcctl=`1763e362…`、
agent=`5427e2c5…`、server=`9c4ebe15…`。manifest（sha256=`545c307c11177a33…`）：
**manifest 布局教训（新增）**——`PackageRepositoryPrefix` = `kubeclipper/packages`
（registry_indexer.go），target 必须写全前缀
`kubeclipper/packages/bootstrap/kubeclipper:v2.0.3-rc.11`、registries.package 只写 host
（172.16.131.146:5003）；首版 target 写 `bootstrap/kubeclipper:tag` 被 ParsePackageRepository
拒绝（`repository "bootstrap/kubeclipper" is not under kubeclipper/packages/{kind}/{name}`），
preflight 拒绝零触碰。`--dry-run` flag 不存在（unknown flag），无法干跑、直接真机执行。

**预跑插曲（中断韧性实证）**：60s 超时的 manifest 预跑意外进入真实升级——dev-2/dev-3 server
完成升级至 rc.11 后本地客户端被 timeout 杀死（远端 SSH 步骤序列不受影响地执行完毕，dev-3
staging 保留 backup+新二进制供检查）。dev-2/3 由此处于 rc.11 中间态，后续 T-A 重跑对这两台
幂等 skip；T-A 注入器原触发条件（等 "upgraded server 172.16.131.146" 日志行）相应改为
**轮询 dev-4 staging backup 文件出现**（install 前一步，距 180s 健康窗口足够远）。

**故障注入矩阵**（全部确定性注入；设计原则：iptables 只阻断发起机→目标节点的探测路径、
不碰节点本身；restore 仅恢复 backup 内旧文件，因此任何注入都不会破坏 restore）：

| 场景 | 注入 | 结果 |
|---|---|---|
| T-A server 健康探测失败→自动恢复 | `upgrade all` rc.10→rc.11；dev-2（发起机）`iptables -I OUTPUT -d 230 --dport 8080 -j REJECT` 在 dev-4 backup 出现后 3s 插入 | ✅ rc=1：dev-2/3 skip（already at revision）；dev-4 install+start 成功但 `waitServerHealthy` 180s 超时 → `restoreNodeBinary`（"restored previous kubeclipper-server on 172.16.131.230"）→ "upgrade stopped at server 172.16.131.230 … already upgraded nodes were not rolled back"，后续 agent/console/kcctl 槽位未执行；删规则后 dev-4 md5=`1a27eab5`（rc.10 基线）、kc-server active、healthz ok |
| T-C kcctl 替换后探针失败→restoreKcctl | `upgrade kcctl`；watcher 在 dev-2 staging backup/kcctl 出现瞬间（备份步骤与 install 之间 ~1s 窗口）用垃圾覆盖 staging/kcctl | ✅ rc=1：steps 全成功（install 装入垃圾）→ 替换后 probe revision `""` ≠ target → "upgrade stopped at kcctl 172.16.131.208" → "restored previous kcctl on 172.16.131.208"；三节点 kcctl md5 全=`53a93b62`（rc.10 基线）、dev-3/4 未触碰、`kcctl version` 正常 |
| T-D console 探针失败→restoreConsole | `upgrade console`；预插 dev-2→230:80 REJECT（console 槽位无 healthz preflight，preflight 不受影响） | ✅ rc=1：208/146 console 重装 v1.6.0 成功；230 替换+start OK 但 `consoleOK`（http://IP:80/ 2xx 探测）180s 超时（错误信息明示 "probing http port 80"）→ "restored previous caddy and web dist on 172.16.131.230"；删规则后 dev-4 caddy md5=`e1c73bd7…` 与 dist 树哈希 `94c7c1c4…` 与注入前**逐字节一致**、console 200×3 |
| T-B 中断后再执行→skip+补齐收敛 | 四场景后（server 208/146 已 rc.11、230 已恢复 rc.10，agent/console/kcctl 未动）重跑 `upgrade all`，无注入 | ✅ rc=0：server 208/146 skip、230 补齐；agent×3 补齐；console×3 幂等重装（无 revision 可探，重装即幂等路径）；kcctl×3 替换；"platform API reports v2.0.3-rc.11 (revision 9a1dbb756326)"。终态三节点 server=`00c6ff30`/agent=`b93afc0a`/kcctl=`a847bf6f` 全一致、服务全 active、healthz ok、console 全 200、doctor **25/25** |

**1.3-09 `--version` 在线正向下载+checksum（本地 GitHub 镜像法）**：dev-2（Ubuntu 24.04，
443 空闲）模拟 github.com——①自签 CA（basicConstraints CA:TRUE，2 天效期）进系统信任
（/usr/local/share/ca-certificates/kc-r20-test-ca.crt + update-ca-certificates）；②SNI 服务器
证书 SAN=github.com；③/etc/hosts `127.0.0.1 github.com`；④python3 HTTPS 443 供给与真实
release 同构的 URL
`/kubeclipper/kubeclipper/releases/download/v2.0.3/release-manifest-v2.0.3.yaml(+.sha256)`。
Downloader（download.go）用 http.DefaultClient（系统根校验）+ 强制 sha256 比对，镜像即满足
其全部前提。**正向**：`kcctl upgrade all --version v2.0.3` → "upgrade plan: target
v2.0.3-rc.11 … from manifest downloaded for v2.0.3"（下载+校验+Parse/Validate+版本策略幂等
短路）→ 9 槽位 skip（server×3+agent×3+kcctl×3；console 无 revision 恒重装）→ platform API
复核 → rc=0。**负向**：`.sha256` 首字符 5→0 篡改 → `release manifest checksum mismatch:
expected=045c307c… actual=545c307c…` 拒绝（下载层 checksum 强制实证）。**拆除**（临时证书
按约束即用即删）：kill 镜像进程（教训：`pkill -f "python3 server.py"` 模式含命令字面量会
匹配承载命令的远程 shell 自身，ssh 会话被杀 exit 255；改用 `server[.]py`）、删
/tmp/kc-r20-mirror、hosts 恢复（getent 解析回真实公网 IP）、CA 移除 + update-ca-certificates
--fresh 重建信任库、443 释放。

**终态**：三节点全 rc.11（9a1dbb7563262f1cf509bcb1a78364b9072f0c0b）、doctor 25/25、
staging /tmp/kubeclipper-upgrade 三节点清理；iptables 规则清零；hosts/CA 恢复原状；共享
Registry 仅增 v2.0.3-rc.11 tag（15 个 v2.0* tag），存量与 caas4/* 未动；/var/lib/kc-etcd
未动。**B1 关闭条件全部满足（§2.5）：在线和内网 OCI 升级、组件独立升级、重复执行、错误
digest/架构、启动失败和中断恢复全部通过。**证据逐字转录本节与 checklist/gaps/plan。

文档同步：plan 头部 R20 段、B1 表行关闭、§2.5 执行状态（B1 关闭）；checklist 1.3-09 ✅
（镜像法正向+checksum 负向）、1.3-10 补故障注入注；gaps P0 行 4 两项余量收口（B1 关闭）、
R20 轮段落。
