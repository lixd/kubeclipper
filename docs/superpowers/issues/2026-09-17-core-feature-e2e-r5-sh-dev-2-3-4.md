# KubeClipper R5 核心功能实测报告：sh-dev-2/3/4

日期：2026-09-17

状态图例：✅ 已实测通过 · ⚠️ 部分验证或发现收敛缺陷 · ❌ 已复现失败

本轮接续 [R4 三机报告](2026-09-17-core-feature-e2e-sh-dev-2-3-4.md)，针对当时仍未覆盖且可在
现有三机环境执行的核心 Case 做真实运行验证。长期状态已回填至
[核心功能测试清单](../../testing/core-feature-checklist.md)，未闭环项见
[当前缺口](../../testing/round-2026-09-gaps.md)。

## 1. 环境与证据边界

| 项目 | 实测值 |
|---|---|
| Git 分支 | `feat/oci-operation-v2-migration` |
| 主机 | `sh-dev-2`、`sh-dev-3`、`sh-dev-4` |
| 平台基线 | 接续 R4 的 3 Server、3 etcd、3 Agent OCI 部署 |
| 集群物料 | Kubernetes `v1.37.0`、containerd `2.2.4`、Calico `v3.31.5` |
| Package Registry | R4 已部署的共享 HTTP OCI Package Registry；本轮未删除或修改其中的共享物料 |

本轮运行包 provenance 与 R4 相同：它证明已部署 OCI 发布包在三机环境中的运行结果，不能替代以
当前分支 HEAD 重新构建、发布后进行的最终发布验收。原始远端证据保留在测试机临时目录；其中可能
含 kubeconfig 或临时对象配置，因此未提交到仓库。

## 2. 已通过的 Case

### 2.1 AIO 最小拓扑和 Master 调度（2.1-02、2.1-14）

创建 `aio-core-20260917`，拓扑为 1 Master、0 Worker，并传入 `--untaint-master` 和
`Overlay-Vxlan-Cross-Subnet`。集群最终 Running，Node Ready，CoreDNS 和 Calico 正常，Kubernetes
API、Service DNS、测试 Pod 均可用。测试完成后删除集群，平台 Agent 恢复为 3/3 Healthy，节点可
复用。

这同时证明 AIO 中 Master 可调度工作负载；不把它误算为多节点网络覆盖。

### 2.2 Calico 非默认模式和 IPv4 自动探测（2.1-19、2.1-20）

- `Overlay-Vxlan-Cross-Subnet` 在 AIO 建群和网络烟测中生效；非法 Calico 网络模式被 CLI 在创建前
  拒绝，未留下 Cluster 或 Operation。
- `first-found` 与 `can-reach=172.16.131.146` 分别完成 1 Master + 1 Worker 建群。每次均确认
  探测参数已写入 Cluster/Calico 配置，随后完成跨节点 Pod ping，最后删除集群并确认平台恢复。
- R4 已覆盖 `interface=ens3`，所以当前清单中的 `first-found`、`interface`、`can-reach` 三类
  IPv4 自动探测方法均有真实运行证据。

### 2.3 FS/S3 备份与 Cron（2.5-01、2.5-02、2.5-04、2.5-06、2.5-07）

FS 手动备份、分钟级 Cron 实际触发、Cron enable/disable 均通过。禁用期间不再创建备份，重新启用后
调度恢复。

S3 路径使用临时 MinIO Backuppoint `s3-r5-20260917` 完成手动备份。Backup
`backup-core-20260917-manual-r5-s3-k2l8hj` 的 Operation
`ce0cb4e3-d45e-477f-af3a-92b4a4a19a26` 为 Succeeded，Backup 状态为 Available；MinIO 对象大小为
`11,771,936` bytes，ETag 和回填的 MD5 均存在。删除该 Backup 后，API 记录与 MinIO 对象都已消失。

## 3. 已运行但未闭环的 Case

### 3.1 已有平台重复 deploy（1.3-04）

对已健康的平台再次执行 `kcctl deploy`，预检阶段明确拒绝，服务和 Agent 保持 Healthy。该结果验证了
拒绝路径不会破坏已部署的平台，但不等同于重复 deploy 的幂等成功，因此 Case 保持 ⚠️。后续需要先
明确产品契约是“安全拒绝”还是“允许幂等重入”。

### 3.2 Operation cancel 需重启 Server 才收敛（3-12、5-06）

取消 Operation `e290a5f7-ddba-4994-b3d3-8bf03eb088af` 后，运行中的 Task 自然完成，后续 Pending
Task 最终被取消，Operation 最终为 Canceled。取消的创建流程使 Cluster 进入 InstallFailed，但可以
被正常删除。

问题是取消请求本身没有让控制器继续归约；重启一个 `kc-server` 后才推进到上述终态。协作式取消的
最终方向得到部分验证，但“无需人工重启即可收敛”的可靠性要求不满足，两个 Case 均保持 ⚠️。

### 3.3 `maxBackupNum` 未清理旧 FS 文件（2.5-08）

设置 `maxBackupNum=1` 后，Backup 对象会按上限轮转，但旧 FS 备份文件没有被删除。本轮留下的 4 个
旧文件已人工清理，以免污染后续测试。该行为会持续占用存储并形成孤儿制品，Case 维持 ❌，修复验收
必须同时验证 Backup 记录、FS 文件和 S3 对象的同步轮转。

### 3.4 Backup 详情 API 对已有对象返回 404（2.5-11）

`GET /api/core.kubeclipper.io/v1/backups/{name}` 对已经存在的 Backup 返回 404；列表以及
`/clusters/{name}/backups` 查询正常。源码中 `DescribeBackup` 将 path 中的 Backup name 作为
`GetBackupEx` 的 cluster 参数，并将 `resourceVersion` 作为 Backup name 传入，和
`GetBackupEx(ctx, cluster, name)` 的接口契约不符。此处应修复路由/查询语义，并补已有对象返回 200 与
不存在对象返回 404 的回归测试。

## 4. 清理与最终状态

测试结束后完成以下清理：

- 删除 `backup-core-20260917` 测试集群，并确认 API 返回 NotFound；
- 删除 FS 与 S3 Backuppoint、R5 FS 备份残留和临时 MinIO 数据；
- 集群删除后终止唯一确认属于本轮临时 MinIO 的孤儿进程；未触碰共享 OCI Package Registry。

最终平台状态：

```text
kc-server  Healthy
kc-etcd    Healthy
kc-agent   Healthy 3/3
doctor     25 passed, 0 warnings, 0 failed
```

## 5. 本轮未执行的边界

本轮没有把环境条件不具备或风险显著的 Case 伪装为通过，包括纯离线 bundle、HTTPS/认证 Package
Registry、平台自身升级、独立 `kcctl join`、HA 故障窗口中的 Watch/运行中 Operation、Console/RBAC，
以及 Master 增删和证书 serial/有效期对比。这些项目仍保留在缺口文档，需在具备相应物料、证书、隔离
网络或额外节点条件后执行。
