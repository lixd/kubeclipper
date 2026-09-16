# KubeClipper 当前核心功能缺口与执行建议（2026-09）

本文档只维护当前缺口、执行优先级、废弃残留和迁移边界。完整功能范围、状态及稳定 Case 编号见
[`core-feature-checklist.md`](core-feature-checklist.md)。完成一项后，应先回填核心清单的状态、
轮次和证据，再从本文档移除或降级该缺口。

状态判断必须基于真实运行证据；CI、单测、代码路径存在或其他流程隐含经过，只能作为部分验证。

## P0：发布主链路缺口

| 顺序 | Case | 缺口 | 完成条件 |
|---:|---|---|---|
| 1 | `1.2-03`、`1.2-07` | 多 Server HA | 3 Server/奇数 etcd 部署成功；分别注入单 Server、单 etcd 故障和滚动重启，API、Watch、Console 入口及运行中 Operation 持续可用 |
| 2 | `1.1-03`、`1.1-08`、`2.1-23` | 纯离线 bundle 真机闭环 | export、拷贝、重复 import 后，在断公网环境完成平台部署、建群、Addon、升级和删除；保存网络封锁与 digest 证据 |
| 3 | `2.6-09`、`2.6-10`、`3-23`、`3-24` | Delivery Policy 自定义策略 | template、validate、diff、apply 全部跑通；白名单内可建，白名单外及缺失 slot 在创建 Operation 前拒绝 |
| 4 | `1.3-07`、`1.3-09`、`1.3-10`、`3-16`、`3-17` | 平台自身升级 | `all --pkg`、`all --online --version` 以及组件独立升级至少各跑一次；数据、配置和已有集群保持可用，失败可恢复 |
| 5 | `2.2-03`、`2.2-09`、`2.2-10` | Master 增删 | 添加后 control-plane/etcd quorum 正常；移除后 etcd member、证书、VIP 和节点角色正确收敛 |
| 6 | `2.3-04`、`2.3-05` | 集群与 Agent 证书更新 | 更新前后证书 serial/有效期可证明变化；API、kubelet、Agent 重连正常；旧证书行为符合设计 |
| 7 | `2.5-02`、`2.5-06`～`2.5-08` | S3 备份与周期轮转 | MinIO Backuppoint、真实周期触发、enable/disable、`maxBackupNum` 文件和对象同步轮转全部通过 |
| 8 | `5-06`、`3-12` | Operation cancel | Pending Operation 不执行；Running Task 允许完成但不再调度后续 Task；Operation、Cluster 和 ExecutionLock 最终一致 |
| 9 | `2.6-05`～`2.6-07` | OCI 缓存和 Registry 故障 | 覆盖缓存损坏、digest 不符、Registry 断连、配置优先级、0600 权限和凭据脱敏；不得回退到 tag |
| 10 | `1.1-05`、`1.1-06`、`1.1-10` | HTTPS/认证 Package Registry | 公共 CA、自签 CA、账号密码分别覆盖 deploy、join、Agent 拉取及失败重试，日志不泄露凭据 |

## P1：核心能力补全

| 顺序 | Case | 缺口 | 完成条件 |
|---:|---|---|---|
| 1 | `1.3-05`、`3-04` | `kcctl join` 独立纳管 | 对未安装 KC 组件的节点完成 join、重复 join、失败清理及 Package Registry 认证/CA 验证 |
| 2 | `2.1-02`、`2.1-24` | 最小集群拓扑 | AIO 和 1 Master + 1 Worker 均完成创建、工作负载、网络、删除和节点复用 |
| 3 | `2.1-19`、`2.1-20` | Calico 非默认网络 | 至少覆盖一个 IPIP/BGP/cross-subnet 模式及 first-found/interface/can-reach 自动探测；非法值应拒绝 |
| 4 | `2.1-21`、`2.1-22` | 镜像 Registry 与 Package Registry 分工 | 两类 Registry 分别配置并生效；私有 CRI Registry 的 HTTP、认证和自签 CA 正确下发到 containerd |
| 5 | `2.1-26`～`2.1-30` | 创建集群负向与恢复 | 覆盖节点占用、跨 Region、CIDR 冲突、主机预检失败和创建中断，不产生危险的半成品状态 |
| 6 | `2.2-05`～`2.2-07`、`2.2-11`～`2.2-13` | 节点管理边界 | disable/enable、掉线恢复、注销残留、Agent 身份保护、Lease 和 Region 约束形成完整证据 |
| 7 | `2.3-02`、`2.3-07`、`2.3-09` | 集群升级故障与可用性 | 注入中断后安全 retry；Registry tag 变化不影响固定 digest；滚动顺序、PDB 和业务连续性明确 |
| 8 | `4-08`～`4-08d`、`4-18` | 用户、登录和 RBAC | 用户/角色 CRUD、enable/disable、密码/验证码、Token、越权 403 和登录限流闭环 |
| 9 | `4-07` | Console 核心 E2E | 登录、建群、升级、备份、删除、Operation 进度和失败原因展示与 API 状态一致 |
| 10 | `3-05`、`3-08`、`3-09`、`3-14`、`3-15`、`3-19`～`3-27` | 未覆盖的 `kcctl` 命令 | 每条命令至少验证一次成功、一次典型失败和退出码；不得用 API 测试代替 CLI 通过；`kcctl login` 还需修复服务端 TLS 校验 |
| 11 | `5-13`、`5-14` | 重复提交与超时收敛 | 重复请求不产生并发副作用；timeout 后 Task、Cluster 和 ExecutionLock 按既定语义收敛 |

## P2：扩展能力与环境矩阵

- `4-03`：MetalLB BGP，需要可控的 BGP 邻居环境。
- `4-04`：Addon 同组件多实例及实例隔离。
- `4-09`～`4-11`：模板、DNS、CloudProvider/外部集群纳管。
- `4-12a`、`4-12b`、`4-15`：Web Terminal、Pod exec、PlatformSetting。
- `7-01`～`7-03`：OAuth/OIDC、CLI completion 和容量基线。
- `6-10`、`6-11`：arm64 真机及正式 Tier 1 OS 矩阵；当前不能只凭构建结果判定通过。
- 大于 3 Worker 的批量增删及多任务容量，执行前先定义规模目标和通过阈值。

## 已废弃残留与迁移边界

这些项目不应作为“尚待补测的正式功能”长期挂在核心清单中。

| 项目 | 当前事实 | 建议 |
|---|---|---|
| Docker CRI（`6-12`） | 当前产品和 Kubernetes 支持基线均不支持 Docker CRI；但 `kcctl create cluster --cri docker` 仍被参数校验接受，代码中也保留了 Docker 分支，而 OCI 发布矩阵只有 containerd | 这是应删除的废弃入口，不是待补测能力：从 CLI help/校验、策略输出和运行分支中移除 Docker；只验证传入 Docker 会明确拒绝，不安排 Docker E2E |
| legacy static server/tar downloader | 当前正式交付路径已经移除，旧节点不支持原地混用 | 不恢复兼容路径；只验证全新部署不依赖旧服务，以及旧节点必须清理后重新 deploy/join 的边界 |
| `nfs-provisioner` | 已退休，由 `nfs-csi` 替代 | 不保留功能 Case；只做默认策略、资源清单和 Console 不再暴露旧组件的静态门禁 |
| legacy package 迁移工具 | 仓库仍保留 `migrate-legacy-packages-to-oci.sh` 一次性导入工具 | 若当前版本不承诺旧包迁移，直接删除脚本和相关说明，不保留长期兼容或迁移流程；不计入核心回归 |

## 本轮证据要求

每个完成项至少保存：Git revision、主机与拓扑、物料来源、组件版本、命令或请求、Operation ID、
关键日志、最终对象状态、必要的 digest/证书/网络证据，以及清理结果。没有这些信息时只能回填 ⚠️。
