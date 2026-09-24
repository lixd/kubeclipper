# KubeClipper OCI 发布阻塞项修复建议

日期：2026-09-18。基线：`feat/oci-operation-v2-migration`，
`e9e95f4cff4ee49fc396b4a35db09f6739b2fef6`。

**文档状态：B1 已实施并通过三机 E2E（2026-09-20，见
[R7 报告 §11](../superpowers/issues/2026-09-19-core-feature-e2e-r7-sh-dev-2-3-4.md)）；
B2 大部分关闭（2026-09-21：失败/删除收敛与 N8 饿死修复经 E2E+rc.6 复验，CIDR 校验含边界与主机
冲突经 rc.6 真机负向矩阵通过（checklist 2.1-28 ✅）。更正（2026-09-20）：前文"CIDR 创建前校验
未实施"系误报，重叠校验自 `a989b14f`（rc.3）已生效并经 R9 dryRun 探针实测确认；R9 补齐嵌套/
每族数量/主机冲突校验，见 §3.2 更新；协作式 cancel 全语义与 retry 仍待验收）；B3 大部分实施
（0600 写入自 batch-1 `a989b14f`，R9 补齐原子替换与符号链接防护及回归测试，rc.7 远端复验两
配置文件 0600，见 §4 实施更新）；B4～B6 待实施。**更新（R10，rc.7 `e7d99421`，2026-09-21）**：
协作式 cancel 语义矩阵与 2.1-30 retry 真机验收通过（超时、重启注入子项于 R16 闭环，见 §3.2），
N9 修复复验通过。B4～B6 待实施。稳定版发布结论仍为 Blocked。**更新（R11，rc.8 `e9d9afeb`，
2026-09-22）**：B4 已实施并真机复验通过——持久化删除流九项证据（见 §5.3 实施更新），
checklist 2.5-04/08/09/10/11 闭环、gaps N4/N7 关闭。**更新（R12，rc.8 `e9d9afeb`，2026-09-22）**：
B5 核心项真机闭环（认证 Registry 正向/负向、缓存篡改、断连恢复，见 §6.3 矩阵更新与
gaps P0 行 8/9）——1.1-05/06、2.6-05/06 ✅，1.1-09 升 ⚠️，2.6-04 补充间接实证；B5 余量（纯离线
bundle、arm64、公共 CA、join 入口、GITHUB_TOKEN 映射）与 B6 待实施。稳定版发布结论仍为 Blocked。**
**更新（R13，rc.8 `e9d9afeb`，2026-09-22）**：qualification `secrets.GITHUB_TOKEN` 映射已实施
（`d2ca8df8`，C1）；§6.3 纯离线 bundle 行、公共 CA 与 join 入口认证行真机闭环（见 §6.4 R13
实施更新与 gaps P0 行 2/9）。**更新（R14，2026-09-22）**：B6 门禁机制已实施——
`scripts/open-packaging/release-gate.sh`（fixture 自测 1 正向+10 阻断全绿）+ release workflow
`release-gate` job（tag 绑定、同 SHA 成功 qualification 解析、manifest 契约与验收记录校验，
publish/build-cli 均依赖门禁，见 §7.6）+ 验收记录目录 `docs/testing/acceptance/`（格式见其
README）；发布侧 bootstrap 类包 `SourceRevision` 必填（fail closed，单测覆盖）+ 升级侧空
revision 告警。门禁真实放行/阻断复验与首个验收记录待下一候选发布轮；B5 余量（平台重
deploy、contents 篡改探针、部署侧断连、arm64）与稳定版发布结论仍为 Blocked。**
**更新（R17，rc.8，2026-09-23）**：B5 余量再收两项——平台本体重 deploy（precheck 快速失败/
sendPackage 断连/完整重 deploy 恢复三场景，含 /var/lib/kc-etcd 重建，用户批准）与部署侧
断连注入真机闭环（见 §6.3 两行与 §6.4 R17 段），B5 余量仅剩 arm64（用户明确排除）；
2.2-03/09/10 Master 增删经真机复证为产品缺口（代码无条件拒绝，gaps P0 行 5）；新发现
P0 缺口"离线建群必须显式 imageRegistry"（离线环境无该字段时 kubeadm init 从公网拉镜像
~18 分钟慢失败且无创建前校验，gaps P0 行 10）。稳定版发布结论仍为 Blocked。
**更新（R18，rc.9 `5e4cfb4b`，2026-09-24）**：两项 P0 缺口修复并真机闭环——master add/remove
完整实现（add 8/8 OperationTasks Succeeded etcd 3 成员、remove 13/13 Succeeded etcd 回 2 成员、
quorum 负向 400，checklist 2.2-03/09/10 ✅，gaps P0 行 5 关闭）；离线建群缺省 imageRegistry
创建前 400 拒绝（48ms 秒级暴露、零对象，checklist 2.1-33 ✅，gaps P0 行 10 关闭）。
稳定版发布结论仍为 Blocked（其余 P0 行 1/4 与 2.1-30 交付余量未清）。
**更新（R19，rc.10 `c356fbaf`，2026-09-24）**：B1 step 2 console/kcctl 组件升级实施并真机
闭环（`upgrade all` 12 槽位 rc.10、`upgrade kcctl` 幂等 skip、`upgrade console` 幂等重装、
负向三例，checklist 1.3-10 ✅，gaps P0 行 4 console/kcctl 部分关闭）；同轮 Docker CRI 废弃
入口与 legacy 迁移工具删除（`018a59fd`）；P0 行 1 故障窗口 Watch+Console 证据补齐关闭
（checklist 1.2-07 ✅，发现 kcctl 客户端单地址无 failover）。B1 余量仅剩 `--version` 在线
正向下载（待上游 v2 stable）与升级中故障注入恢复。稳定版发布结论仍为 Blocked。

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
| B1   | 平台升级产物与 OCI 发布不一致 | 已确认代码和发布契约缺陷                   | 已实施（2026-09-20，E2E 复验见 R7 报告 §11）；step 2 console/kcctl 已实施并真机闭环（2026-09-24 R19，rc.10 12 槽位+幂等 skip/重装+负向三例，见 R7 报告 §12.15）；余量：`--version` 在线正向下载（待上游 stable）与升级中故障注入恢复 |
| B2   | CIDR 校验、取消与失败恢复     | CIDR 缺陷已确认；取消停滞 agent 侧根因已定位（R8） | 大部分关闭（2026-09-21，rc.6 复验：CIDR 校验含边界+主机冲突真机负向矩阵通过（checklist 2.1-28 ✅）、N8 饿死修复复验通过（无 10s 尾延迟、不饿死）、失败/删除收敛 R8 已验证；rc.7 复验：协作式 cancel 语义矩阵与 2.1-30 retry 通过；rc.8/R16 复验：超时注入、Server/Agent 重启注入子项闭环 ✅） |
| B3   | 敏感配置权限                  | 写入代码与远端权限已确认                   | 大部分实施（2026-09-21：0600 写入自 batch-1 `a989b14f`；R9 补齐原子替换+拒绝符号链接+umask 无关性与 §4.4 回归测试；rc.7 远端复验：dev-2 两配置文件实测 0600） |
| B4   | 备份删除、轮转和详情查询      | 已有失败记录；删除时序和查询参数问题已确认 | 已实施并复验（2026-09-22，rc.8 `e9d9afeb` 持久化删除流真机复验通过，见 §5.3 实施更新；已知边角：同毫秒并发双 DELETE 同一备份一个 500，无重复副作用，记低优先改进项）       |
| B5   | OCI 真实消费矩阵              | 关键 E2E 未执行或未完整验收                | 大部分闭环（R12/R13：认证 Registry 正负向、缓存篡改、断连恢复、纯离线 bundle、公共 CA、join 认证已闭环；R15：包 contents 路径真机篡改探针闭环——server 剔除/CLI fail-fast/agent gzip 阻断三层防御+恢复后正向 Running；R17（2026-09-23）：平台本体重 deploy 三场景与部署侧断连注入真机闭环，见 §6.3/§6.4 R17；余量：arm64（用户明确排除））       |
| B6   | 最终候选与发布门禁            | 候选错位已确认；最终候选验收未执行         | 已实施（2026-09-22：`release-gate.sh`+release workflow `release-gate` job+验收记录约定，见 §7.6；真实发布轮放行/阻断复验待下一候选）       |

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

> **执行状态（2026-09-24，R19，R7 报告 §12.15）**：内网 OCI 升级（`upgrade all` 12 槽位
> rc.9→rc.10）、组件独立升级（server/agent R7；console/kcctl R19，含幂等 skip 与幂等重装）、
> 错误 digest/repointed tag/隐式降级拒绝（触碰节点前）均真机通过；旧 tar 消费路径与文档入口
> 已移除。余量两项：`--version` 在线正向下载+checksum（上游尚无 v2 stable，链路与 404 处理
> 已实测）；升级中节点启动失败/中断恢复的故障注入（restore 路径仅单测覆盖）。

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
修复 `1413e849`（defer 对调 + NotFound 清 informer store/requeue，附单测）已随 rc.6
（`29a9bf8a`）发布并真机复验通过（2026-09-21）：①1M 建群 13 步任务时长 1s×7、5-8s×4、
31/34s×2，无 10s 轮询尾延迟（修复前基线 10.01s/任务）；②快速 create→delete 收敛后立即再建群
正常派发并 Running，不饿死。
失败/删除收敛侧已实施并验证：`978b1b43` 全删除路径释放节点占用标签、force 删除逃生门、
InstallFailed 僵尸补偿（R8 S3～S5 实测：同节点重群成功、删除后标签释放、force 删除约 30 秒收敛）。
**CIDR 创建前校验——更正（2026-09-20）与实施更新**：前文"重叠网段第三轮复测仍被接受"系误报。
代码核查、单测与 R7 §9 rc.1 复验一致：重叠校验自 `a989b14f`（batch-1，rc.3 起）已在 API
（`createClusterCheck` → 400）与 CLI（fail-fast）生效；R9 dryRun 探针在 rc.5 实测重叠 400 拒绝
确证。R9 探针同时确认三个真实边界缺口（列表内嵌套、每地址族多条、网段覆盖主机网络均被接受）
并已补齐：`parseCIDRs` 每列表每地址族最多 1 条 + 拒绝 IPv4-mapped IPv6 字面量；
`ValidateSubnetOverlap` 扩展同族全对全不相交；新增 `ValidateCIDRHostConflict` 在
`createClusterCheck` 节点占用检查后按请求节点 `NodeIpv4DefaultIP`（回退 `Ipv4DefaultIP`）校验，
CLI 不拉节点列表故主机冲突仅服务端。**rc.6（2026-09-21）真机负向矩阵复验通过**：8 项非法输入
全部 400 且理由逐项匹配、`.144/28` 未含节点 IP 正确放行、合法双栈/单栈 dryRun 200、CLI 重叠
exit 1、零 Cluster/Operation 残留（见 checklist 2.1-28 行，终态 ✅）。本节关闭条件（§3.5）中协作式
cancel 全链路语义与 retry 已于 R10（rc.7）真机验收通过（见下）。

**R9 探针附带发现 N9（缺省 `cni.calico` 块 500 panic），同日修复（`75ed938f`）**：API 直调
建群/dryRun 请求 `cni.type=calico` 但缺省可选 `calico` 子对象时，`calico.go` InitStep 对 nil
指针 `cni.Calico.IPv4AutoDetection` 解引用 panic，go-restful recover 兜为 500（CLI 自行填充
默认值不受影响，正是"前端直调 API 不应报错"要求的一类残留）。修复：InitStep 引入
`defaultCalico`——nil 块按 kcctl create 同款默认值填充（first-found/first-found/
Overlay-Vxlan-All/IPManger/MTU 1440），非 nil 块的空字段亦兜底，并深拷贝避免改写请求对象；
manifest 模板直接解引用 `.CNI.Calico.*`，默认值填充同时修复渲染链。2 单测（nil 块默认值+
render 回归、部分块保留显式值）。**rc.7（`e7d99421`，2026-09-21）真机复验通过**：缺省 calico
块 dryRun 200（原 500），合法全块 200、重叠 400 理由匹配回归通过。

**R10（rc.7，2026-09-21）协作式 cancel 语义矩阵（§3.3/§3.4）真机验收通过**：①Running 中取消
（6/13 步）：在途步自然完成后停止派发、剩余 7 步 Canceled，Operation Canceled、Cluster
InstallFailed，~2.5 分钟收敛；②最早取消（+3s，仅第 1 步在途）：1 步完成+12 步 Canceled，
<23 秒收敛；③终态后重复取消被 CLI 干净拒绝（exit 1）；④Running 中并发双取消：第 1 次受理、
第 2 次 API
Conflict 拒绝，无状态污染。**retry（2.1-30）通过**：对 Canceled 创建操作 `kcctl operation
retry`，前 6 步保留原时间戳未重做（§3.3-5"重试不得无条件重做成功步骤"），仅执行剩余步 ~60s
回 Succeeded，集群 InstallFailed → Running；取消/失败后安全删除 20～30 秒清空
Cluster/Operation/节点标签，同节点重建 2 分钟 Running，kubelet inactive、无 /etc/kubernetes
残留。§3.4 剩余子项：超时（spec deadline 到期）、Watch 重连与 Server/Agent 重启注入未覆盖。
证据：dev-2 /tmp/r10-*.txt。

**R16（rc.8，2026-09-23）§3.4 剩余子项闭环**：①**超时注入**——API `POST /clusters?timeout=120`（kcctl create cluster 无该 flag，须走 API）显式压缩期限：Operation `Spec.Timeout=2m0s`、deadline=start+2m 精确写入 status；deadline 到达时运行中 `kubeadm init` 步被 SIGTERM（agent 日志 `signal: terminated`），任务回写 `TimedOut/DeadlineExceeded`（"task deadline exceeded"），Operation 终态 `TimedOut/DeadlineExceeded`（"operation deadline exceeded"），Cluster→InstallFailed；Pending 步不再派发；force 删除后按卸载语义手动清理（kubeadm reset、kubelet/containerd disable、/var/lib/{etcd,kubelet,containerd}、/etc/{kubernetes,containerd,cni}、/tmp/.k8s、包缓存 k8s/cri 版本目录）恢复零残留；②**Server 重启注入**——kc-server 在 41s 安装任务中段重启（Stopping/Stopped/Started 同秒，<1s 完成且未中断 quorum），在途任务无感知照常 Succeeded（HA 三副本各节点 agent 连本地副本），任务写回 20 条全部成功；③**Agent 重启注入**——T2 两轮空闲窗口重启无副作用；T2c 确定性踩中：iptables OUTPUT DROP 5003 令 dev-3 下载步挂起 → 重启 kc-agent → worker 重新 reconcile Running 任务（reconcile#1 01:54:39 挂起、reconcile#2 01:55:22 重跑）→ 解封后任务 01:55:34 完成、Operation Succeeded，零残留（§3.3-2 worker 恢复路径实证：eligibleTasks 按 NodeRef 匹配、getLiveTask 非 terminal、startPendingTask 翻 Pending→Running）；Watch 断连重连由 client-go reflector 自动 relist 覆盖（任务恢复即重连证据）。证据原存 dev-2 /tmp/r16-evidence（终态清理删除，逐字结论转录本节与 R7 报告 §12.12）

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

**实施更新（2026-09-21，R9）**：两处凭据配置的 0600 写入自 batch-1 `a989b14f` 已完成
（`Config.Dump` 与 `WriteToFile`，私有目录 0700，显式 Chmod 使 umask 无法放宽）；审计所见的
0644 是旧版本写入的存量文件，路径被重写时自动收紧。本节剩余项已在 R9 补齐：`WriteToFile`
改为原子替换（同目录 0600 临时文件，写入+Sync+Close 全部成功后才 rename，失败保留原文件并
清理临时文件），拒绝经符号链接写配置（Lstat 检查，防误覆盖链接目标），`Config.Dump` 收敛到
同一共用安全写入入口。§4.4 回归测试落地：新建 0600/目录 0700、覆盖 0644 收紧、umask 000 无关
（包内无并行测试，进程内设置）、符号链接拒绝且目标不被改写、rename 失败保留原文件、零临时
文件遗留。错误信息只含路径，不含配置内容。**rc.7（`e7d99421`，2026-09-21）远端复验完成**：
升级流程重写 dev-2 `/root/.kc/config` 与 `deploy-config.yaml` 后实测均 0600（旧 0644 存量在
重写时收紧生效）；dev-3/dev-4 无 .kc 目录（kcctl 仅在 dev-2 执行）。§4.5 关闭条件中"两份配置
所有成功写入路径最终均为 0600"代码+真机双侧达成；配置优先级与凭据脱敏专项用例仍未单独成轮。

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

**实施与复验（`e9d9afeb`，随 rc.8 于 2026-09-22 三机真机复验通过）**：按 §5.3 方案实施——
`deleting`/`deleteFailed` 状态与持久化删除 Operation，Backup 记录保留到删除 Operation
Succeeded 才由 backupcontroller 移除；重复删除经 activeDeletionOp 复用在途任务；删除任务用
备份自身 BackupPointName 构造；终态失败穿透为新 retry（新 Operation）；恢复入口拒绝
deleting/deleteFailed（400）；轮转过滤 rotatable（available/deleteFailed）；DescribeBackup
按 URL name 查询，已有 200/不存在 404。§5.4 矩阵对应实测：FS/S3 手动删除与 `maxBackupNum=2`
+ 2 分钟周期 Cron 轮转 4+ 轮，Backup 记录与 FS 文件一一对应无孤儿；S3 停机删除 →
deleteFailed 记录保留，S3 恢复后重试删除新 Operation 成功；错误凭据/不存在 bucket →
Operation Failed、Backup `error`、集群保持 Running；顺序重复删除幂等 200；恢复 Succeeded 后
集群回 Running；详情 200/404。未覆盖说明：『控制器重启续跑』『文件已不存在/默认备份点变更
幂等』两场景真机未单列复验，留待 B6 门禁轮补充。已知边角：同毫秒并发双 DELETE 同一备份，
一个 200 一个 500（createOperationV2 冲突），无重复副作用、不影响一致性，记为低优先改进项。

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
| 纯离线                           | export、拷贝、import、重复 import；断公网 deploy→建群→Addon→升级→删除 | 留存公网封锁证据；无外网依赖；重复导入 digest 不变。**R13 已执行（2026-09-22，rc.8 三机；arm64 用户明确排除）**：5003 export（skopeo --preserve-digests，5 制品 368MB，bootstrap index digest 重写为子 manifest digest）→ scp 离线拷贝+sha256 校验 → 空白 9443 import ×2 Inventory 逐字节全等 → iptables OUTPUT 专用链断公网（外网 DNS/连接全 REJECT，REJECT 计数 dev-2=329/dev-3=269 包）→ ConfigMap+三节点 0600 json 切 9443 → componentmeta 9443 → 建群 Running（9443 拉取 183 条，五类仓库全覆盖）→ calico/coredns 全 Running → 删除清空。升级经 bundle manifest 实证防护双拦截：digest 不一致拒（repointed tag 防护）+ rollout 后 revision 比对拒（5003 rc.8 包 sourceRevision=None 且包内二进制实际构建 e9d9afe 而 manifest 声明 e9e95f4——制品元数据缺失，非升级缺陷；发布侧防再发 KC_SOURCE_REVISION 强校验）。**R17 已执行（平台本体重 deploy，2026-09-23，rc.8；5003 直连未叠加断公网——bundle 导入路径与集群消费侧 R13 已证同构）**：kcctl deploy 非幂等（preCheck 拒绝已有服务，"clean old environment before deploying"）→ 前置护栏（bootstrap 清单 4 kinds 核验、registry.bin sha256 双侧一致守卫、kc-etcd snapshot+deploy-config 备份）后 `kcctl clean -A` → 以 `KC_PACKAGE_REGISTRY_CONFIG` 回退 http scheme 重 deploy ~4 分钟成功：4 服务 active（kc-etcd 12379/12380/12381、kc-server、kc-console、kc-agent）、3 节点 Healthy、agentID 经 -c 配置模式保持、bootstrap 包取 5003 最高 semver tag、healthz/console 200、package-registry.json 与 deploy-config 重新生成、kc-etcd 重建全新 3 成员。**余量：bundle 无第三方 addon 包（componentmeta addons 仅平台自带四类）** |
| HTTPS 公共 CA、自签 CA、账号密码 | deploy、独立 join、Agent 真实拉取                                     | 证书校验生效，正确凭据成功；错误 CA/密码明确拒绝。**R12 已执行（自签 CA+账号密码，2026-09-22，rc.8 三机）**：①自建 distribution 3.0.0 HTTPS+自签 CA+htpasswd，`registry sync` 从 HTTP 共享源镜像 6 artifact（digest 一致）；②三节点 0600 配置+deploy-config 双侧切换动态生效，建群 Succeeded、三节点 Ready、agent 真实拉取；③错误密码 → `UNAUTHORIZED` 明确失败、零凭据泄漏、修正后 retry Succeeded；④未配 CA → `x509: certificate signed by unknown authority` 明确拒绝。**R13 已执行（公共 CA 等价+join 入口，2026-09-22）**：⑤自签 CA 加入三节点系统信任库（等价公共 CA：`package-registry.json` 零凭据零 CA 字段）→ 7443 TLS-only 建群 Running、TLS 拉取 231 次走系统信任池；运维发现 Go x509 进程内缓存系统根池，加 CA 后必须重启 kc-server/kc-agent；⑥join 入口：auth registry 8443 下 join 下发 0600 凭据、被加入节点直连认证拉取 9×200、错误口令 EXIT=1 可读报错零部分安装 |
| 拉取途中断连、恢复仓库           | 平台部署和集群消费失败后恢复                                          | 错误可观察、修复后可重试，无半成品被误用。**R12 已执行（集群消费侧，rc.8 三机）**：retry 中途 kill registry → `connection refused` 明确失败（在途请求经 graceful shutdown 完成）→ registry 恢复后 retry → 三节点 k8s 包全部从头重拉（56MB layer ×3）、Succeeded、集群 Running；半成品缓存未被信任。**R17 已执行（平台部署侧，2026-09-23，rc.8）**：①precheck 期断连：registry 不可达 → 15s 硬超时双 scheme（https+http）探测快速失败，报错含 `kcctl registry sync` 提示，零节点影响；②sendPackage 期断连：证书已分发后 tcp-reset 注入 → `refresh bootstrap assets from registry ... connection refused` abort（exit 1）无重试，半安装态=证书+包缓存；③恢复网络后无需 clean 直接重 deploy 成功——错误可观察、可重试，半装态不被误用 |
| 缓存篡改、digest 不符            | 已拉取节点再次消费                                                    | 校验拒绝，不回退到可变 tag，不执行错误内容。**R12 已执行（最小探针，rc.8 三机）**：master `charts.tgz` 翻一字节 → `validCachedHelmChart` payloadDigest 拒绝 → digest-pinned 重拉 → sha 恢复原值、Operation Succeeded；包 contents 路径同构校验（`loadCachedComponent` 逐文件 digest）。**R15 已执行（包 contents 路径真机探针，2026-09-22，rc.8 三机；registry 侧 blob 篡改，9443 测试源）**：containerd 1.7.29 configs 层 blob 篡改（registry 对 blob 内容与路径 digest 不符不做在线校验，仍 200）后实测三层防御——①浅篡改（tar 头损坏）：server indexer 下载后解析归档失败 warn `archive/tar: invalid tar header` 并从清单剔除，componentmeta 中该包消失；②显式指定被剔除包建群：CLI fail-fast `missing packages: containerd 1.7.29; publish or sync them to OCI package registry ... first`（EXIT=1，零对象）；③深篡改（20MiB 数据区翻转，tar 头可解析）：两节点建群 installRuntime 步骤 attempt 0/1 均被 agent 侧 gzip CRC 阻断，任务状态回写 `"message":"gzip: invalid checksum","reason":"ExecutionFailed"`，3 秒内 Failed、无半装（cluster InstallFailed → force delete 零残留）。正向对照：恢复 blob（重拷后 sha256 复原 b8c094e2…、69064877 字节）+ 清缓存显式建群 → Running（9443 实拉 GET 200，go-containerregistry UA），排除误伤好包 |
| **blob 缺失**（manifest 在、blob 404）  | 拉取途中或创建前          | 创建前拒绝，零对象。**R16 已执行（2026-09-23，rc.8）**：9443 拷入 containerd:1.7.29 后删除 layer blob 数据文件（registry GET 404、manifest 仍 200）→ server 索引器为读包清单取 blob 失败 → tag 记 `skip invalid OCI package image`（`Warnf`）从清单剔除 → POST 显式 containerd 1.7.29 返回 400 `ArtifactNotPublished: artifact cri/containerd:1.7.29 is not published`，零 Cluster/Operation；componentmeta `unavailable[]` 记该包 `reason: notPublished`。对比 R15 观察项（distribution 对 blob 路径 digest 不符不在线校验）不改变拦截结论：消费入口在创建前解析即被拒 |
| amd64/arm64 及正式 OS            | 实际安装、最小集群和清理                                              | 架构、版本、Node/Pod 健康与清理结果正确。**未执行** |

### 6.4 关闭条件与证据格式

**实施更新（R12，2026-09-22，rc.8 `e9d9afeb` 三机）**：§6.3 矩阵的"自签 CA+账号密码""拉取途中
断连""缓存篡改"三行已在真机闭环（明细见矩阵内 R12 标注与
[gaps P0 行 8/9](round-2026-09-gaps.md)）。R12 环境记录：dev-2 distribution 3.0.0
（`172.16.131.208:8443`，自签 CA SAN=IP、htpasswd）；测试凭据/证书/数据即用即删，共享 Registry
（146:5003）与平台 etcd 数据目录未动。qualification workflow 的 `secrets.GITHUB_TOKEN` 映射
（本节 CI 小修复）仍未实施。

**实施更新（R13，2026-09-22，rc.8 `e9d9afeb` 三机）**：qualification workflow `secrets.GITHUB_TOKEN`
映射+发布临时文件 0600/清理已实施（commit `d2ca8df8`，C1）；§6.3 矩阵"纯离线"行与
"HTTPS 公共 CA/自签 CA/账号密码"行的公共 CA 等价验证、join 入口认证均真机闭环
（明细见矩阵内 R13 标注）。R13 环境记录：dev-2 三个测试 registry（7443 TLS-only/8443
TLS+htpasswd/9443 纯 HTTP，r13-kc-test-ca 3 天效期 SAN=IP）；终态清理：测试 CA 三节点信任库
移除并 update-ca-certificates 复验 0、三个 registry 停止+数据删除、全部 /tmp r13 临时产物
（含 CA 私钥、API 客户端证书、8443 口令文件）删除、证据日志存档并脱敏 basic-auth；
配置/ConfigMap/iptables 全还原；共享 Registry（146:5003）与平台 etcd 数据目录未动。
**实施更新（R15，2026-09-22，rc.8 `e9d9afeb` 三机）**：§6.3 矩阵"缓存篡改、digest 不符"行的
包 contents 路径真机探针闭环（registry 侧 blob 篡改 + agent 消费，三层防御实证见矩阵内
R15 标注；digest-pinned 重拉正向对照 Running）。R15 环境记录：dev-2 9443 测试 registry
（distribution 3.1.1，/tmp/r15-reg，由 5003 skopeo 逐 tag 拷贝）；篡改仅作用于测试源，
共享 Registry（146:5003）只读未动、/var/lib/kc-etcd 未触碰；终态：blob 复原、
deploy-config 与四节点 0600 delivery json 还原 5003、componentmeta 复验 rules=3、
9443 registry 停止+数据删除、/tmp 全部 r15/r13 临时物与客户端证书删除。
证据摘要（原始文件清理后由会话记录逐字重建，含来源说明）：
`kc-fix2/r15-raw-evidence/r15-evidence-reconstructed.md`。
**矩阵余量**：arm64 真机（用户明确排除）。平台本体重 deploy 与部署侧断连已于 R17 闭环（见下）。

**实施更新（R16，2026-09-23，rc.8 `e9d9afeb` 三机）**：①§6.3 新增"**blob 缺失**"行闭环（见矩阵内 R16 标注）：blob 404 → 索引剔除 → `ArtifactNotPublished` 400 创建前拦截，componentmeta `unavailable[]` 记 `notPublished`；②2.6-10b 多候选冲突闭环：policy 双 slot 同版本 → `DuplicateResolvedComponent` 400（同 slot 重名 `duplicate component slot`，单 slot 双 option 按 option.name 匹配不冲突）；③2.6-07 优先级矩阵（json=9443/dc=5003 → deploy-config 胜出，plan 全 5003）与 8443 htpasswd 认证探针（正确口令 200/错误口令 401、json/API/journal/文件系统 grep 0 泄漏）闭环；④2.1-30 超时/重启注入子项闭环（见 §3.2 R16 段）。R16 环境记录：dev-2 9443（同 R15 拓扑）与 8443 htpasswd（9443 数据目录共享）两个测试 registry（distribution 3.1.1）、skopeo 在 dev-3 逐 tag 拷入 k8s/cri/cni(calico 走 charts tigera-operator)/k8s-extension；测试期间 deploy-config 与 server/agent delivery json 临时改指测试源，终态全部字节级还原 5003 并复验 componentmeta registry=5003；凭据探针后 htpasswd/凭据 json 即删、双 registry 停止、/var/lib/r16-registry 与 /tmp/r16-evidence（含 /tmp/.r16 客户端证书）删除；共享 Registry（146:5003）只读未动、/var/lib/kc-etcd 未触碰；T3 force 删除后按卸载语义手动清理双节点（kubeadm reset、kubelet/containerd disable 与目录清理、包缓存版本目录移除）复验 6 项目录不存在、双节点 0 enabled。证据原存 dev-2 /tmp/r16-evidence（终态清理删除，逐字结论转录 R7 报告 §12.12 与 checklist/gaps）。

**实施更新（R17，2026-09-23，rc.8 `e9d9afeb` 三机，用户批准含 /var/lib/kc-etcd 重建）**：
①§6.3"纯离线"行余量收口——平台本体重 deploy：护栏（bootstrap 清单 4 kinds 前置核验、
registry.bin sha256 3162d930… 双侧一致、kc-etcd snapshot+deploy-config 备份）→ `kcctl clean -A`
→ `KC_PACKAGE_REGISTRY_CONFIG='{"registry":"172.16.131.146:5003","scheme":"http"}'` 重 deploy
（clean 删除已安装 package-registry.json，scheme 解析链 flags > env > 已安装文件 > 默认 https）
→ ~4 分钟 4 服务 active、3 节点 Healthy、agentID 全保持、dumpConfig 重新生成等价
deploy-config；deploy 非幂等实证（preCheck 拒绝已有服务）；②§6.3"拉取途中断连"行余量收口
——部署侧两场景：precheck 15s 双 scheme 探测快速失败（报错含 `kcctl registry sync` 提示、
零节点影响）、sendPackage 期 tcp-reset → `refresh bootstrap assets from registry ...
connection refused` abort（exit 1）无重试、半装态=证书+包缓存，恢复后无需 clean 直接重 deploy；
③2.2-03/09/10 Master 增删确认为产品缺口（`makeMasterCompare`
pkg/clusteroperation/node.go:101-104 无条件 `return ErrInvalidNodesRole`，etcd member
remove/证书清理代码不存在），非测试余量；worker add/remove v2 operation 正向首证
（28 OperationTasks 按 stepID×节点派发，移除后标签/hosts/二进制清理干净，`/tmp/.k8s` 残留记
小观察项）；④新 P0 缺口（gaps P0 行 10）：离线建群必须显式 imageRegistry——payload 未指定时
kubeadm.yaml 无 imageRepository（默认 registry.k8s.io）、containerd 无本地镜像映射，离线 init
单 attempt ~18 分钟 i/o timeout、5400s deadline 内循环重试后 Failed，创建前零校验零提示；
`DownloadImage` 仅 Upgrade 路径且 OCI 交付下直接报错（pkg/scheme/core/v1/k8s/cluster.go:397）。
修复实证：payload `"imageRegistry": "kc-package-registry"`（deploy 自动创建的 Registry 对象名）
→ ResolveImageRegistry 同源驱动 kubeadm imageRepository=5003 与 containerd hosts.toml
（http endpoint）→ 双 master 建群 20/20 Succeeded；R15/R16 建群成功系 containerd 遗留镜像
掩护，R16 清理 /var/lib/containerd 后暴露。终态：测试集群删除、3 节点 Healthy 无集群标签、
共享 Registry sha 守卫未动、/var/lib/kc-etcd 为重 deploy 后全新、/tmp/r17 与
/root/r17-evidence、/root/r17-backup 及 dev-3 备份全删（证据逐字转录 R7 报告 §12.13 与
checklist/gaps）。

**实施更新（R18，2026-09-24，rc.9 `5e4cfb4b` 三机）**：R17 定性的两项 P0 缺口实施修复并真机
闭环。①Fix A（gaps P0 行 10）：`createClusterCheck` 新增离线+空 `imageRegistry` 前置拒绝
（400，文案含 registry.k8s.io 不可达解释），真实 POST 48ms、dryRun 同拒、零对象，对照组带
`kc-package-registry` 200；②Fix B（gaps P0 行 5）：`pkg/clusteroperation/node.go`
makeMasterCompare/makeMasterOperation 实现 master add/remove（add：Builder unwrap
AvailableKubeMasters 排除被操作节点→getJoinCommand 在存活 master 产出 control-plane join
命令（upload-certs 刷新证书密钥）→renderMasterJoinConfig/joinNode→waitForAddedNodesReady；
remove：EtcdMemberRemove 步骤在存活 master 按 peer addr 匹配成员 ID 显式摘除→drain→
kubeadmReset→removeEtcdDataDir→clearIPVS/removeDummyInterface/clearVIPDomain→卸载链；
quorum 守卫：remove 后剩余 <2 → 400 `invalid nodes topology`）；③真机复验：r18-cluster
（双 master，API 创建——kcctl CLI 守卫拒绝偶数 master）add dev-2 8/8 OperationTasks
Succeeded、etcd 3 成员、control-plane Ready；remove dev-2 13/13 Succeeded、etcd 回 2 成员、
离开节点 /etc/kubernetes 与 /var/lib/etcd 清空、kubelet/containerd inactive；负向 remove 1 of 2
直调与 `?dryRun=true` 均 400、零副作用；④边界标注：0 worker 拓扑下 worker 侧 lvscare
refreshLvsCare 按设计不生成步骤、未真机覆盖（单测覆盖 leaving 过滤——Builder append-back
副作用被 LvsCareRefreshSteps 的 leaving 参数修正）。发布与升级：本地构建+wrapper 经 dev-2
发布 v2.0.3-rc.9（5003 仅增 tag）→ `kcctl upgrade all --manifest` 三机 6 槽位 rc.9（~66s），
doctor 25/25；升级 WARN skip v2.0.3-rc.5 tag（存量 MANIFEST_UNKNOWN，非阻塞，升级逻辑正确
跳过坏 tag）。终态：r18-cluster 删除、3 节点 Healthy 无集群标签、共享 Registry 仅增 rc.9 tag、
/var/lib/kc-etcd 未动、API 客户端证书与全部临时产物即用即删。证据逐字转录 R7 报告 §12.14。

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

### 7.6 实施更新（R14，2026-09-22）

**§7.3-1/§7.3-3 已实施**：

1. **发布侧 SourceRevision 必填**：`pkg/delivery/publisher` `Publish()` 对 `bootstrap` 类包
   在缺少 `SourceRevision` 时直接拒绝（错误信息指引 `KC_SOURCE_REVISION`），堵住手工发布
   路径（`tools/oci-publish` 读环境变量，忘设即空——5003 rc.8 包 `sourceRevision=None` 的
   成因）静默产出无源包。消费侧（indexer）保持宽松以兼容存量包；`upgrade fetchPlatformPackage`
   对空 `sourceRevision` 增加 rollout 前告警。CI（`_publish-oci-component.yml` 已设
   `KC_SOURCE_REVISION=${{ github.sha }}`）与 bootstrap 脚本（`common.sh` 自动补 git HEAD）
   不受影响；`oci-migrate`（不传 revision 的 legacy 迁移工具）迁移 bootstrap 会被正确拒绝。
   单测 `TestPublishBootstrapRequiresSourceRevision`：缺 revision 拒绝、带 revision 落
   `org.opencontainers.image.revision` 标签、非 bootstrap 类包保持兼容。
2. **门禁脚本** `scripts/open-packaging/release-gate.sh`，任一不满足即阻断：①稳定 `vX.Y.Z`
   tag；②qualification manifest 契约——kind=ReleaseManifest、metadata.version=tag、
   metadata.sourceRevision=候选 SHA、bootstrap/kubeclipper artifact revision=候选 SHA；
   ③验收记录 `docs/testing/acceptance/<候选SHA>.yaml` 存在且 candidate_sha 匹配、
   `result: passed`、release_manifest_sha256 与下载的 qualification manifest sha256 全等、
   可选 release_tag 匹配。制品 digest 校验保持在 qualification（`verify-release-manifest.sh`）
   与发布后 manifest job，门禁只校验绑定。自测 `test-release-gate.sh` 11 例
   （正向+缺记录/失败结果/候选错位/校验和不符/manifest revision 错/版本错/bootstrap
   revision 错/非稳定 tag/记录 tag 错/缺 manifest 全阻断）全绿，离线可跑。
3. **release workflow 接线**：新增 `release-gate` job——checkout 全历史校验 tag 未移动
   （tag commit == GITHUB_SHA）、Actions API 解析同 head_sha 的成功
   `publish-oci-qualification.yml` 运行（无则阻断）、下载其 `oci-release-manifest-*` artifact、
   执行门禁脚本；`publish` 与 `build-cli` 均 `needs: release-gate`（manifest、github-release
   传递依赖），缺记录/错 SHA/qualification 失败时发布不再继续。
4. **验收记录约定**：`docs/testing/acceptance/README.md` 定义记录格式与流程（qualification
   成功 → 同 SHA 构建上执行验收轮 → 记录落库 → 稳定 tag）；`result: failed` 的记录同样阻断。

**关闭余量**：门禁正向放行与负向真机阻断（缺记录/错 SHA 的真实 release 触发）待下一候选
发布轮验证，届时产生首个真实验收记录；对应 checklist 6-04 保持 ⚠️ 直至该轮完成。

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
