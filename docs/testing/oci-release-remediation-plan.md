# KubeClipper OCI 发布阻塞项修复建议

日期：2026-09-18。基线：`feat/oci-operation-v2-migration`，
`e9e95f4cff4ee49fc396b4a35db09f6739b2fef6`。

**文档状态：B1 已实施并通过三机 E2E（2026-09-20，见
[R7 报告 §11](../superpowers/issues/2026-09-19-core-feature-e2e-r7-sh-dev-2-3-4.md)）；
B2 部分实施（2026-09-21：失败/删除收敛与操作停滞 agent 侧根因已修复并 E2E 验证。更正
（2026-09-20）：前文"CIDR 创建前校验未实施"系误报，重叠校验自 `a989b14f`（rc.3）已生效并经
R9 dryRun 探针实测确认；R9 补齐嵌套/每族数量/主机冲突校验，见 §3.2 更新）；B3～B6 待实施。
稳定版发布结论仍为 Blocked。**

> **决策记录（2026-09-20）**：B1 选择「实施」而非收缩承诺——平台升级按 OCI 契约改造
> （`kcctl upgrade <component> --version/--manifest`，复用 ReleaseManifest/OCI fetcher/digest
> 校验，移除旧 OSS/tar 入口），按 §2.3 方案实施，可分两步交付（先 server/agent 核心路径，
> 后 Console/kcctl 与中断恢复语义）。R7 三轮缺陷修复（batch-1/2/3）已完成并复验，见
> [R7 报告](../superpowers/issues/2026-09-19-core-feature-e2e-r7-sh-dev-2-3-4.md) §9。

本文只交付修复建议，不代表业务代码已修改、测试集群已操作或发布已完成。后续实施统一采用 OCI
交付，移除旧 OSS/tar 升级入口，不新增两套并行交付方式。

## 1. 依据、状态和交付原则

相关文档：

- [核心功能测试清单](core-feature-checklist.md)：功能范围与稳定 Case 编号。
- [当前功能缺口](round-2026-09-gaps.md)：已有问题与运行验证边界。
- [R4 实测报告](../superpowers/issues/2026-09-17-core-feature-e2e-sh-dev-2-3-4.md)、
  [R5 实测报告](../superpowers/issues/2026-09-17-core-feature-e2e-r5-sh-dev-2-3-4.md)、
  [R6 实测报告](../superpowers/issues/2026-09-17-core-feature-e2e-r6-sh-dev-2-3-4.md)。
- [本次发布检查报告](/Users/lixueduan/17x/tmp/kc-fix2/output/oci-release-audit-2026-09-18.md)：
  工作区内的本地审计产物，不属于本仓库；换机后可使用下述基线摘要和仓库内实测报告。

2026-09-18 检查中，13 个相关 Go 包测试、6 个打包脚本测试、39 个 shell 语法检查以及 Linux/amd64
server、agent、kcctl 构建通过。远端只读检查为 Healthy、3/3 Agent；但 Server 仍运行
`1a70255b`，当前分支在其后还有 5 个源码修复。最新 qualification/AIO 也对应旧
revision，不能据此放行当前候选。R5/R6 故障来自已有实测记录，本次审计未重新执行破坏性 E2E。

| 编号 | 内容                          | 证据性质                                   | 初始实施状态 |
| ---- | ----------------------------- | ------------------------------------------ | ------------ |
| B1   | 平台升级产物与 OCI 发布不一致 | 已确认代码和发布契约缺陷                   | 已实施（2026-09-20，E2E 复验见 R7 报告 §11；console/kcctl 与中断恢复为 step 2） |
| B2   | CIDR 校验、取消与失败恢复     | CIDR 缺陷已确认；取消停滞 agent 侧根因已定位（R8） | 部分实施（2026-09-21，见 §3.2 更新；更正（2026-09-20）：CIDR 校验已实施并补齐边界，协作式 cancel 全语义仍待验收） |
| B3   | 敏感配置权限                  | 写入代码与远端权限已确认                   | 待实施       |
| B4   | 备份删除、轮转和详情查询      | 已有失败记录；删除时序和查询参数问题已确认 | 待实施       |
| B5   | OCI 真实消费矩阵              | 关键 E2E 未执行或未完整验收                | 待实施       |
| B6   | 最终候选与发布门禁            | 候选错位已确认；最终候选验收未执行         | 待实施       |

现有清单中的旧 `--pkg`、`--online` 用例描述的是当前实现；实施 B1 时同步替换为本文确定的 OCI
升级契约，保留 Case 编号，不把删除旧入口记作通过旧用例。Master 动态增删等不支持能力
按支持边界处理，不为了清单全绿临时扩展实现。

## 2. B1：平台升级统一消费 OCI

### 2.1 问题与证据

关联 Case：`1.3-07`、`1.3-09`、`1.3-10`、`3-16`、`3-17`。

当前在线升级仍拼接旧 OSS 的 `kc-upgrade-<arch>.tar.gz`，发布流水线却只发布 OCI 包、 四平台
kcctl、manifest 和校验和，没有生产对应升级 tar 的入口。升级还通过 map 遍历决定
组件替换顺序，不适合作为可验证的滚动升级顺序。

代码入口：

- [升级 CLI](../../pkg/cli/upgrade/upgrade.go)：`checkVersion`、`sendPackage`、
  `replaceAllService`、`replaceServiceCmds`。
- [ReleaseManifest 下载](../../pkg/delivery/releasemanifest/download.go)、
  [manifest 校验](../../pkg/delivery/releasemanifest/manifest.go)、
  [OCI fetcher](../../pkg/delivery/fetcher/oci.go)、
  [Registry 配置](../../pkg/delivery/registry/config.go)。
- [bootstrap 选择与安装](../../pkg/cli/deploy/bootstrap.go)、
  [bootstrap/kubeclipper 发布](../../scripts/open-packaging/publish-bootstrap-kubeclipper.sh)、
  [正式发布流水线](../../.github/workflows/release.yml)。

### 2.2 根因

平台部署已采用 OCI，而升级的输入、解包和替换流程仍按旧 tar 结构实现；发布端与消费端没有
共享同一个版本及制品契约。现有 bootstrap 选择逻辑会选最新版本，不能直接用于指定版本升级。

### 2.3 修改方案与接口

1. 命令统一为 `kcctl upgrade <component> --version <版本>` 或
   `kcctl upgrade <component> --manifest <本地文件>`，二者互斥且必须提供一个。组件保留
   `all/server/agent/console/kcctl`；移除 `--online`、`--pkg`、`--binary`，同步帮助、示例和测试。
2. `--version` 复用 ReleaseManifest 下载与 checksum 验证；`--manifest` 读取本地文件。 离线环境先将
   bundle 导入内网 Registry，再使用 manifest 和该 Registry 完成升级。 仓库地址及认证/CA 参数沿用已有
   Package Registry 配置方式，不另建凭据体系。
3. 复用 manifest、OCI fetcher、Registry 配置及 digest/provenance 校验。组件必须来自指定
   manifest；镜像同步后只映射仓库前缀，保留目标路径和 digest，禁止重新解析“最新”tag。
4. bootstrap/kubeclipper 增加对应架构的 Linux kcctl 内容，并纳入包元数据、校验和及发布测试。
   Server/Agent 仍来自该包，Console 来自独立 bootstrap 包；四平台独立 kcctl 继续用于首次安装。
5. 按目标节点架构选择制品，不能使用发起命令的机器架构代替。替换前完成全部目标物料下载、
   校验、磁盘/权限/连通性检查；任何物料缺失或校验失败都不得先停服务。
6. 固定执行顺序：Server 逐台替换并检查健康，再逐台更新 Agent、Console 和远端 kcctl。
   节点排序确定且输出执行计划；`all` 不顺带升级 etcd 或 Registry。
7. 保存旧程序、配置、目标 revision 和升级阶段。单节点失败立即停止后续节点；恢复该节点旧
   文件后验证健康，明确报告升级失败、已完成节点和恢复结果。不得把回滚成功报告为升级成功，
   也不能假定已升级节点自动全部回退。
8. 使用现有 semver 依赖比较版本。相同 revision 且健康时幂等跳过；恢复中断升级时先核对实际 revision
   和健康状态，不能仅凭阶段记录跳过。拒绝隐式降级，不承诺旧静态部署原地迁移。

### 2.4 回归测试

| 场景                                            | 必须断言                                           |
| ----------------------------------------------- | -------------------------------------------------- |
| 参数互斥、缺参及旧入口                          | 执行前明确失败，不操作节点                         |
| 指定版本、不同节点架构、仓库存在更新 tag        | 使用 manifest 固定制品，不升级到其他版本或错误架构 |
| 错误 checksum/digest/revision、缺少包或 payload | 服务停止前失败，配置和程序未替换                   |
| 在线、内网 Registry、各组件独立升级             | 版本正确，仅更新目标组件，已有集群可用             |
| 节点启动失败、中断后再执行                      | 后续节点停止推进；恢复结果可见，旧配置和数据保留   |
| 相同 revision 重复升级、版本降级                | 健康目标幂等跳过；降级明确拒绝                     |

### 2.5 关闭条件

在线和内网 OCI 升级、组件独立升级、重复执行、错误 digest/架构、启动失败和中断恢复全部通过；
已有集群及配置不丢失，最终运行程序与 manifest 一致。旧 OSS/tar 消费路径和文档入口均已移除。

## 3. B2：网络校验与取消操作收敛

### 3.1 问题与证据

关联 Case：`2.1-28`、`2.1-30`、`3-12`、`5-06`、`5-13`、`5-14`。

R6 记录 Pod CIDR `10.96.0.0/16` 与 Service CIDR `10.96.0.0/12` 重叠仍能创建对象，随后 卡在
Installing；取消后状态、节点占用和主机副作用不能自动收敛。R5 另记录取消后需要重启 一个 Server
才推进。上述行为尚未形成当前候选的关闭证据。

代码入口：

- [CLI 创建参数](../../pkg/cli/create/create_cluster.go)：`ValidateArgs`；
  [API handler](../../pkg/apis/core/v1/handler.go)：`CreateClusters`、`DeleteCluster`。
- [取消/重试 API](../../pkg/apis/operations/v1alpha1/handler.go)、
  [Operation store](../../pkg/models/operationv2/store.go)。
- [Operation Watch 注册](../../pkg/controller/operationv2/setup.go)、
  [状态协调](../../pkg/controller/operationv2/controller.go)、
  [业务状态回写](../../pkg/controller/operationv2/business.go)。

### 3.2 根因与待定位边界

CIDR 创建前校验缺失已确认。取消停滞的准确断点尚未确认：现有代码已经注册 Operation/Task
Watch，并有最多 5 秒的等待后重新检查，不能直接声称“缺少 Watch/轮询”。直接调用 reconciler
的单测也不能证明实际事件链能推进。

**更新（2026-09-21，R8，rc.5 `c5ccb367`）**：操作停滞的一类 agent 侧根因已定位并修复——
worker `execute()` 的 defer 声明顺序（LIFO 反转为先等 terminal watcher 退出再关 stop）使每个
任务白付最长 10s 轮询尾延迟；若 server 在该窗口 finalize 并 purge 任务历史，轮询永远 404，
`execute` 挂到 spec deadline，单任务 worker 饿死该节点后续任务（SIGQUIT goroutine dump 实锤，
见 [R7 报告 §12.2](../superpowers/issues/2026-09-19-core-feature-e2e-r7-sh-dev-2-3-4.md)）。
修复 `1413e849`（defer 对调 + NotFound 清 informer store/requeue，附单测）待随下一 rc 真机复验。
失败/删除收敛侧已实施并验证：`978b1b43` 全删除路径释放节点占用标签、force 删除逃生门、
InstallFailed 僵尸补偿（R8 S3～S5 实测：同节点重群成功、删除后标签释放、force 删除约 30 秒收敛）。
**CIDR 创建前校验——更正（2026-09-20）与实施更新**：前文"重叠网段第三轮复测仍被接受"系误报。
代码核查、单测与 R7 §9 rc.1 复验一致：重叠校验自 `a989b14f`（batch-1，rc.3 起）已在 API
（`createClusterCheck` → 400）与 CLI（fail-fast）生效；R9 dryRun 探针在 rc.5 实测重叠 400 拒绝
确证。R9 探针同时确认三个真实边界缺口（列表内嵌套、每地址族多条、网段覆盖主机网络均被接受）
并已补齐：`parseCIDRs` 每列表每地址族最多 1 条 + 拒绝 IPv4-mapped IPv6 字面量；
`ValidateSubnetOverlap` 扩展同族全对全不相交；新增 `ValidateCIDRHostConflict` 在
`createClusterCheck` 节点占用检查后按请求节点 `NodeIpv4DefaultIP`（回退 `Ipv4DefaultIP`）校验，
CLI 不拉节点列表故主机冲突仅服务端。待随 rc.6 真机负向矩阵复验。本节关闭条件（§3.5）中协作式
cancel 全链路语义与 retry 仍按 §3.3/§3.4 验收。

### 3.3 修改方案

1. 提供 CLI/API 共用网络校验，使用 `net/netip` 解析和规范化 CIDR。在写入 Cluster、 Operation
   及节点占用之前，拒绝非法地址、重复和同地址族的 Pod/Service 网段重叠； API
   不能依赖客户端校验。保留单双栈数据模型，不扩大双栈支持承诺。
   **实施偏离（R9，2026-09-20）**：沿用现有 `net.ParseCIDR`（其规范化已满足需求，Go 1.26 对
   全地址族返回 16 字节 IP，IPv4-mapped 检测改按字面量含 `:` 判断），未迁 `net/netip`；
   校验函数为 `netutil.ValidateSubnetOverlap`/`ValidateCIDRHostConflict`，主机冲突经用户确认
   纳入（仅服务端）。
2. 先以真实 API、持久化和控制器集成测试复现取消停滞。记录取消请求的 UID/resourceVersion、 store
   更新、Informer 事件、入队、reconcile、业务回写和锁释放，定位第一个不推进环节，
   将复现固定为回归后再修改。日志只记录必要标识与状态，不能输出任务里的凭据。
3. 保留协作式取消：停止新任务派发，取消 Pending 任务；Running 任务按现有完成、超时和
   结果不确定语义处理，不因收到取消请求就提前解锁并允许冲突操作。
4. 创建取消后进入可解释的失败终态，允许显式安全删除。取消不等于自动回滚；删除完成前
   保留节点占用，清理成功后再释放，避免未清理节点被其他集群复用。
5. 复用 retry generation 和 ExecutionLock。重试不得无条件重做成功步骤；无法确认副作用的
   任务保留明确错误及处置要求，禁止重启服务、直接修改 etcd、无限重试来伪造收敛。

### 3.4 回归测试

- CLI 和 API 分别覆盖合法 CIDR、非法格式、重复、包含式重叠及同族网段冲突；非法请求无
  Cluster/Operation/节点标签残留。不同地址族不误判为重叠。
- 覆盖尚未派发、Pending Task、Running Task、任务结束与取消并发、重复取消及超时。 同时断言
  Operation、Task、Cluster 和 ExecutionLock，不能只验证 API 返回成功。
- 覆盖 Watch 重连、Server/Agent 重启和并发请求；重启用于故障注入，不能作为让取消生效的步骤。
- 创建失败或取消后安全删除，再使用同一节点建群；验证节点标签、运行物和任务均无残留。

### 3.5 关闭条件

在正常健康测试环境，无运行任务时取消请求成功后 10 秒内收敛；有运行任务时，任务终态后 10
秒内完成业务状态回写及相应锁处理。结果不确定必须显式暴露，不能以强制解锁达成时限。
随后可安全删除或按既有规则重试，不留下孤儿任务及已清理节点的占用。

## 4. B3：敏感配置强制安全权限

### 4.1 问题与证据

关联 Case：`2.6-07`、`4-16`。

审计确认远端 `.kc/config`、`.kc/deploy-config.yaml` 为 0644。该环境 `/root` 为 0700，
因此不能宣称凭据已经泄露，但写入代码依赖 umask，不能保证自定义路径或其他 home 安全。

代码入口：[kcctl Config.Dump](../../pkg/cli/config/config.go)、
[DeployConfig.Write](../../cmd/kcctl/app/options/options.go)、
[普通文件写入](../../pkg/cli/utils/file.go)、
[已有 Registry 安全配置写入](../../pkg/delivery/registry/config.go)。

### 4.2 根因

两个配置入口通过 `os.Create` 写文件，创建权限依赖 umask，覆盖既有文件也不会自动将权限 收紧到
0600。OCI 专用配置已有安全权限，不能据此推断其他含认证资料的配置同样安全。

### 4.3 修改方案

- 为含凭据配置提供共用安全写入方法；普通 `WriteToFile` 不全局改权限。
- 同目录临时文件以 0600 创建，完整写入并检查写入、同步、关闭错误后原子替换。替换已有 0644
  配置后权限也必须为 0600；失败返回错误，保留原文件并清理临时文件。
- 工具自建私有目录使用 0700，不修改用户指定共享目录的权限；拒绝通过符号链接覆盖其他文件。
- 校验日志、错误和命令回显，不包含密码、token、私钥或完整含敏感字段的配置。

### 4.4 回归测试

覆盖 umask 000/022/077、新建、覆盖 0644 文件、自定义路径、目标符号链接以及写入失败。 改变 umask
的测试使用隔离子进程，避免影响并行测试。失败时断言原配置完整、无半文件或
遗留临时文件；用哨兵凭据检查捕获日志中没有泄露。

### 4.5 关闭条件

两份配置所有成功写入路径最终均为 0600；私有目录权限符合要求，共享目录未被改动。
异常路径不丢配置、不吞错误，脱敏测试通过。

## 5. B4：备份删除与存储清理保持一致

### 5.1 问题与证据

关联 Case：`2.5-08`、`2.5-11`。

R5 已复现 `maxBackupNum=1` 轮转记录但保留旧 FS 文件，以及已有 Backup 详情返回 404。

代码入口：

- [CronBackup 控制器](../../pkg/controller/cronbackupcontroller/controller.go)：轮转先调用
  `DeleteBackup`，随后 `deleteBackup` 又从 lister 读取该对象。
- [备份 API](../../pkg/apis/core/v1/handler.go)：`DeleteBackup`、`DescribeBackup`；
  [备份读取模型](../../pkg/models/cluster/cluster.go)：`GetBackupEx(ctx, cluster, name)`。
- [Backup 控制器](../../pkg/controller/backupcontroller/controller.go)、
  [Backup 状态类型](../../pkg/scheme/core/v1/backup_types.go)。

### 5.2 根因

轮转先删除记录再构造物理删除任务，产生依赖 informer 缓存时序的失败窗口；手动删除也没有
等待存储清理成功再移除记录。丢失记录后缺少可靠重试依据，仅交换两行调用顺序不能解决问题。

详情接口把 path name/resourceVersion 传给 cluster/name 参数，接口契约明确不匹配。

### 5.3 修改方案与状态变化

1. 手动删除和 Cron 统一进入持久化删除流程。新增 `deleting`、`deleteFailed` 状态，持久化 删除
   Operation 引用；用 Backup UID 确定任务身份，重复请求取得同一任务，不创建重复副作用。
2. 删除任务使用备份关联的 BackupPoint 和文件信息，不依赖集群当前默认备份点；构造任务时
   保留完整清理依据。Backup 记录留到 FS/S3 删除成功再移除。
3. 删除失败保留记录、错误和重试关联；重试复用 Operation 的执行机制，不能因确定性名称
   已存在就把失败当成功。恢复/下载入口对删除中的备份给出明确状态错误。
4. 目标对象确实不存在按幂等成功处理；权限错误、超时和网络错误必须失败并可重试。
5. Cron 从最旧可轮转备份开始请求删除，跳过创建、恢复或删除中的对象；计数考虑已提交删除
   的对象，避免重复调度时多删仍需保留的备份。
6. 详情接口按 URL 中 Backup name 查询；resourceVersion 独立传入正确读取层，不改变其含义。
   保留原路由及成功对象形状，已有对象 200、不存在 404，其他存储错误不伪装为 404。

### 5.4 回归测试

| 场景                                      | 必须断言                                                 |
| ----------------------------------------- | -------------------------------------------------------- |
| FS/S3 手动删除及 `maxBackupNum=1` 轮转    | Operation 成功后记录与制品同时消失，最新备份保留         |
| 重复请求、并发 Cron、缓存及时更新         | 同一 Backup 不重复创建清理任务，不依赖旧缓存对象         |
| 创建删除任务前后控制器重启                | 持久化状态足以继续，不能遗失清理依据                     |
| 存储不可达、无权限、恢复后重试            | 记录和错误保留；恢复后可清理成功                         |
| 文件已不存在、集群默认 BackupPoint 已变更 | 幂等成功或清理原备份位置，不误删其他存储对象             |
| Backup 详情                               | 正确名称 200；不存在 404；resourceVersion 不作为名称使用 |

### 5.5 关闭条件

FS/S3 记录与制品删除一致；重复删除、缓存变化、控制器重启及存储故障后重试均通过。
没有因删除记录提前而无法恢复的孤儿文件，详情 API 契约测试通过。

## 6. B5：补齐真实 OCI 消费矩阵

### 6.1 问题与证据

关联 Case：`1.1-03`、`1.1-05`～`1.1-10`、`2.1-23`、`2.6-05`～`2.6-07`、 `6-10`、`6-11`。现有脚本及
Registry 单测有覆盖，但断公网、真实部署消费侧认证、节点故障和 arm64
运行证据不足，性质是未完成验收，不能统一断言相关功能没有实现。

入口：[离线导出](../../scripts/open-packaging/export-offline-registry-bundle.sh)、
[离线导入](../../scripts/open-packaging/import-offline-registry-bundle.sh)、
[Registry sync](../../pkg/cli/registry/sync.go)、[E2E](../../test/e2e/)、
[qualification](../../.github/workflows/publish-oci-qualification.yml)。

### 6.2 根因与修改方案

现有验证偏重脚本、局部传输逻辑和 HTTP Registry，未贯穿正式支持的实际部署与故障环境。
应建立隔离测试矩阵，重用当前测试工具与断言，不以另写一套模拟器代替真实消费。

qualification 的 sync job 使用 `$GITHUB_TOKEN`，但未在该 job/step 显式映射
`secrets.GITHUB_TOKEN`。补齐注入，凭据临时文件保持 0600 且清理；公共仓库成功不能独立证明
私有仓库认证。使用需要认证的仓库验证无凭据拒绝、正确凭据通过。

### 6.3 必须执行的矩阵

| 环境或故障                       | 执行范围                                                              | 验收断言                                           |
| -------------------------------- | --------------------------------------------------------------------- | -------------------------------------------------- |
| 纯离线                           | export、拷贝、import、重复 import；断公网 deploy→建群→Addon→升级→删除 | 留存公网封锁证据；无外网依赖；重复导入 digest 不变 |
| HTTPS 公共 CA、自签 CA、账号密码 | deploy、独立 join、Agent 真实拉取                                     | 证书校验生效，正确凭据成功；错误 CA/密码明确拒绝   |
| 拉取途中断连、恢复仓库           | 平台部署和集群消费失败后恢复                                          | 错误可观察、修复后可重试，无半成品被误用           |
| 缓存篡改、digest 不符            | 已拉取节点再次消费                                                    | 校验拒绝，不回退到可变 tag，不执行错误内容         |
| amd64/arm64 及正式 OS            | 实际安装、最小集群和清理                                              | 架构、版本、Node/Pod 健康与清理结果正确            |

### 6.4 关闭条件与证据格式

每个声明支持的矩阵项都有真实通过证据；环境缺失项保持未验收，不用构建结果代替。
每条记录至少包含以下字段，配置和日志先脱敏：

```yaml
case_id: "对应核心清单编号"
candidate_sha: "完整 Git SHA"
release_manifest_sha256: "manifest 校验和"
artifact_digests: []
topology: "节点、架构、OS、Registry 协议"
operation_ids: []
result: "passed | failed | not-run"
final_state: "Operation、Cluster、Node、Pod、锁和存储状态"
cleanup_result: "清理结果及剩余对象"
evidence_artifacts: []
```

单测、构建、平台健康与 E2E 分别记录；Operation 成功也要验证真实节点和工作负载。

## 7. B6：候选、发布物和验收证据一致

### 7.1 问题与证据

关联 Case：发布工程章节及 `6-04` provenance 门禁。审计基线为 `e9e95f4c`，最新 qualification/AIO
与远端 Server 为 `1a70255b`；后续源码涉及 Addon、ErrIgnore、升级和
CronBackup，旧程序的运行结果不能覆盖这些修复。

入口：[release](../../.github/workflows/release.yml)、
[qualification](../../.github/workflows/publish-oci-qualification.yml)、
[AIO](../../.github/workflows/test-aio.yml)、 [资源清单](../../packaging/resources.yaml)、
[manifest verifier](../../scripts/open-packaging/verify-release-manifest.sh)。

### 7.2 根因

构建、包发布和业务验收没有绑定同一最终候选。稳定标签流程依赖构建与 manifest，未要求该 SHA
的完整验收结果，因此流水线成功不能自动推出发布门禁满足。

### 7.3 修改方案

1. B1～B4 修复后冻结 clean SHA，统一构建 kcctl/server/agent、发布 OCI 包；核心运行程序、 package
   sourceRevision、manifest revision 和对应校验和可追溯一致。第三方组件仍记录其独立版本。
2. 使用该候选重跑 qualification、AIO、B5 矩阵和核心生命周期/故障恢复；旧报告仅作历史参考。
3. 稳定标签发布前检查标签 SHA 的成功 qualification、自动化检查和完整验收记录，核对 manifest
   checksum/digest。缺失、失败或 SHA 不一致均阻断，不接受其他提交的绿色结果。
4. 验收记录保存为可下载 CI artifact，并覆盖发布审核周期；不要为提交报告再改变候选 SHA。
   正式命名空间最终 manifest 仍需完整校验，不能只检查 qualification 的物料。
5. Console 核心流程、自定义 RBAC、`kcctl get --watch` 分别复现、定位及验收，不能写成同一个
   已确认根因。已有失败记录未关闭前保留发布风险，不因 OCI 拉取正常而忽略。
6. Master 动态增删等当前不支持能力同步产品说明及拒绝测试，不以缺少正向用例默认为支持。

### 7.4 回归测试

- 使用缺少记录、错误 SHA、错误 manifest checksum、失败 qualification 验证发布门禁拒绝。
- 用完整同 SHA 的证据通过门禁，并在发布前核对各核心运行程序版本和制品来源。
- 验证构建通过而业务 E2E 失败时不能发布；证据不可下载或被替换时不能默认为通过。
- Console、RBAC、watch 各自记录成功/失败，不合并统计掩盖缺口。

### 7.5 关闭条件

同一候选的所有必要验收通过，正式产物与证据对应；稳定标签前置检查能实际阻断上述负向场景。
支持边界明确，发布结论由可追溯证据决定，而不是由清单总通过率决定。

## 8. 实施顺序、评审拆分与完成标准

| 顺序 | 可独立评审的工作                           | 先决条件与交付                                     |
| ---- | ------------------------------------------ | -------------------------------------------------- |
| 1    | 网络校验、敏感配置权限、备份详情分别修复   | 各自先有失败回归；接口和错误语义可判定             |
| 2    | 取消链路定位修复、备份删除收敛分别实施     | 取消必须先确认断点；备份覆盖手动和 Cron 两入口     |
| 3    | OCI 升级、bootstrap 内容和发布产物调整     | 新 CLI 契约、恢复流程及文档同步；不保留旧 tar 分支 |
| 4    | 隔离候选发布、消费矩阵、故障恢复、最终门禁 | 冻结候选；保存同 SHA 证据；全部必要项通过后再放行  |

未来每项关闭时，补充实现提交、回归结果、真实运行证据和清理结果，再回填核心清单与缺口文档。
待定位问题必须补根因说明；未执行项不能标为通过。本文完成不改变任何 Case 的验证状态。

本文档自身验收只检查：B1～B6 是否完整覆盖已确认方案，代码和证据链接是否有效，已确认缺陷、
待定位根因和未执行验收是否区分，以及每项是否给出可判定的关闭条件。文档交付不代表修复完成。
