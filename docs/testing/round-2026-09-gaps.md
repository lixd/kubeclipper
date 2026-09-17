# KubeClipper 当前核心功能缺口与执行建议（2026-09）

本文档只维护当前缺口、执行优先级、废弃残留和迁移边界。完整功能范围、状态及稳定 Case 编号见
[`core-feature-checklist.md`](core-feature-checklist.md)。完成一项后，应先回填核心清单的状态、
轮次和证据，再从本文档移除或降级该缺口。

状态判断必须基于真实运行证据；CI、单测、代码路径存在或其他流程隐含经过，只能作为部分验证。

## P0：发布主链路缺口

| 顺序 | Case | 缺口 | 完成条件 |
|---:|---|---|---|
| 1 | `1.2-07` | HA 故障窗口完整验收 | R6 已在运行中 CreateCluster 期间停止/恢复 dev4 `kc-server`，Operation、API 和 quorum 仍可用；仍缺故障窗口中的 Watch、Console 入口证据 |
| 2 | `1.1-03`、`1.1-08`、`2.1-23` | 纯离线 bundle 真机闭环 | export、拷贝、重复 import 后，在断公网环境完成平台部署、建群、Addon、升级和删除；保存网络封锁与 digest 证据 |
| 3 | `2.1-28`、`2.1-30` | 非法 CIDR 与创建中断的安全收敛 | R6 复现 Pod/Service CIDR 重叠仍可创建 Installing Cluster；取消后 Cluster/Operation、节点标签和主机副作用未自动清理，需修复创建前校验、cancel、retry 和安全删除 |
| 4 | `1.3-07`、`1.3-09`、`1.3-10`、`3-16`、`3-17` | 平台自身升级 | `all --pkg`、`all --online --version` 以及组件独立升级至少各跑一次；数据、配置和已有集群保持可用，失败可恢复 |
| 5 | `2.2-03`、`2.2-09`、`2.2-10` | Master 增删 | 添加后 control-plane/etcd quorum 正常；移除后 etcd member、证书、VIP 和节点角色正确收敛 |
| 6 | `2.3-05` | Agent 证书重新签发 | 集群证书更新已由 R6 用 serial/有效期前后对比证明；仍需覆盖 Agent 证书重新签发、重连、旧证书行为和有效期 |
| 7 | `2.5-08` | `maxBackupNum` 存储对象轮转 | R5 已验证 Backup 对象轮转，但旧 FS 备份文件仍残留；需同时轮转 Backup 记录、FS 文件和 S3 对象，且重试不留下孤儿文件 |
| 8 | `5-06`、`3-12` | Operation cancel 自动收敛 | R5 需重启一个 `kc-server` 才继续推进；R6 取消 CIDR 创建后出现孤立 Running Operation/Installing Cluster，必须无需重启地让 Operation、Cluster 和 ExecutionLock 一致收敛 |
| 9 | `2.6-05`～`2.6-07` | OCI 缓存和 Registry 故障 | 覆盖缓存损坏、digest 不符、Registry 断连、配置优先级、0600 权限和凭据脱敏；不得回退到 tag |
| 10 | `1.1-05`、`1.1-06`、`1.1-10` | HTTPS/认证 Package Registry | 公共 CA、自签 CA、账号密码分别覆盖 deploy、join、Agent 拉取及失败重试，日志不泄露凭据 |

R4/R5/R6 已完成或部分完成的 HA、最小拓扑、Calico 自动探测、S3 备份、Cron、Policy 白名单和独立 join
主路径不再重复列为“未执行”；详细命令、
Operation ID、故障注入和清理证据见
[`R4 报告`](../superpowers/issues/2026-09-17-core-feature-e2e-sh-dev-2-3-4.md) 和
[`R5 报告`](../superpowers/issues/2026-09-17-core-feature-e2e-r5-sh-dev-2-3-4.md)、
[`R6 报告`](../superpowers/issues/2026-09-17-core-feature-e2e-r6-sh-dev-2-3-4.md)。

## P1：核心能力补全

| 顺序 | Case | 缺口 | 完成条件 |
|---:|---|---|---|
| 1 | `1.3-05`、`3-04` | `kcctl join` 独立纳管的负向与安全边界 | R6 已完成空闲 dev4 的独立 join 主路径；仍需重复 join、错误凭据、HTTPS/自签 CA 和失败清理 |
| 2 | `2.1-21`、`2.1-22` | 镜像 Registry 与 Package Registry 分工 | 两类 Registry 分别配置并生效；私有 CRI Registry 的 HTTP、认证和自签 CA 正确下发到 containerd |
| 3 | `2.1-12`、`2.1-27`～`2.1-30` | 创建集群负向与恢复 | R6 已验证合法外部 IP/SAN、占用节点、Master/Worker 重复和非法端口/域名前置拒绝；仍需域名代理连通性、跨 Region、CIDR 冲突修复、完整主机预检及中断 retry/安全删除 |
| 4 | `2.2-06`、`2.2-07`、`2.2-11`～`2.2-13` | 节点管理边界 | R6 已验证空闲 Agent drain/delete 后 join 恢复；仍需掉线注入、集群占用保护、Lease/证书残留、Agent 身份保护和 Region 约束；disable/enable 已在 R4 覆盖 |
| 5 | `2.3-02`、`2.3-07`、`2.3-09` | 集群升级故障与可用性 | 注入中断后安全 retry；Registry tag 变化不影响固定 digest；滚动顺序、PDB 和业务连续性明确 |
| 6 | `2.5-11` | Backup 详情查询 API | 已有 Backup 的 `GET /backups/{name}` 必须返回对应对象，不存在才返回 404；R5 复现已有 Backup 也 404，列表和集群范围查询不受影响 |
| 7 | `4-08`～`4-08d`、`4-18` | 用户、登录和 RBAC | 用户/角色 CRUD、enable/disable、密码/验证码、Token、越权 403 和登录限流闭环 |
| 8 | `4-07` | Console 核心 E2E | 登录、建群、升级、备份、删除、Operation 进度和失败原因展示与 API 状态一致 |
| 9 | `3-09`、`3-10`、`3-12`、`3-15`、`3-19`、`3-24`、`3-25`、`3-27` | 未覆盖或未闭环的 `kcctl` 命令 | R6 已补 registry list/image/非法 push、drain 和部分登录/RBAC；仍需每条命令成功/典型失败闭环，修复 `get --watch`，补 cancel 自动收敛、valid registry 生命周期、login TLS 和 deploy config 优先级 |
| 10 | `5-13`、`5-14` | 重复提交与超时收敛 | 重复请求不产生并发副作用；timeout 后 Task、Cluster 和 ExecutionLock 按既定语义收敛 |

补充的命令缺陷：`3-10 kcctl get --watch` 已在 R4 复测为失败项。CLI 虽然展示 `-w/--watch`，
但当前实现未把该 flag 传入查询或建立 watch 流，命令一次输出后退出；应修复或从 CLI 暴露面移除，
在修复前不能按“部分验证”统计。R6 另确认 `3-15 drain` 的真实语义仅是 KubeClipper Agent
节点注销，不是 Kubernetes Pod 驱逐；清单和命令帮助不得继续写成 PDB/Pod eviction。

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
| Docker CRI（`6-12`） | 当前产品不支持 Docker CRI（包括 dockershim/外置 Docker），现行 Kubernetes 版本也没有可用的 Docker CRI 支持；但 `kcctl create cluster --cri docker` 仍被 help/参数校验接受，代码还保留 Docker 分支，而 OCI 发布矩阵只有 containerd | 这是应删除的废弃入口，不是待补测能力：从 CLI help/校验、策略输出和运行分支中移除 Docker；只验证传入 Docker 会明确拒绝，不安排 Docker E2E。独立 Docker Registry 管理命令与 Docker CRI 不是同一功能 |
| legacy static server/tar downloader | 当前正式交付路径已经移除，旧节点不支持原地混用 | 不恢复兼容路径；只验证全新部署不依赖旧服务，以及旧节点必须清理后重新 deploy/join 的边界 |
| `nfs-provisioner` | 已退休，由 `nfs-csi` 替代 | 不保留功能 Case；只做默认策略、资源清单和 Console 不再暴露旧组件的静态门禁 |
| legacy package 迁移工具 | 仓库仍保留 `migrate-legacy-packages-to-oci.sh` 一次性导入工具 | 若当前版本不承诺旧包迁移，直接删除脚本和相关说明，不保留长期兼容或迁移流程；不计入核心回归 |

## 本轮证据要求

每个完成项至少保存：Git revision、主机与拓扑、物料来源、组件版本、命令或请求、Operation ID、
关键日志、最终对象状态、必要的 digest/证书/网络证据，以及清理结果。没有这些信息时只能回填 ⚠️。
