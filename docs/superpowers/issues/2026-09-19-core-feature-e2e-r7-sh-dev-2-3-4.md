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
