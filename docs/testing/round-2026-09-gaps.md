# KubeClipper 当前核心功能缺口与执行建议（2026-09）

本文档只维护当前缺口、执行优先级、废弃残留和迁移边界。完整功能范围、状态及稳定 Case 编号见
[`core-feature-checklist.md`](core-feature-checklist.md)。完成一项后，应先回填核心清单的状态、
轮次和证据，再从本文档移除或降级该缺口。

状态判断必须基于真实运行证据；CI、单测、代码路径存在或其他流程隐含经过，只能作为部分验证。

## P0：发布主链路缺口

| 顺序 | Case | 缺口 | 完成条件 |
|---:|---|---|---|
| 1 | `1.2-07` | HA 故障窗口完整验收 | R6 已在运行中 CreateCluster 期间停止/恢复 dev4 `kc-server`，Operation、API 和 quorum 仍可用；仍缺故障窗口中的 Watch、Console 入口证据 |
| 2 | `1.1-03`、`1.1-08`、`2.1-23` | 纯离线 bundle 真机闭环 | export、拷贝、重复 import 后，在断公网环境完成平台部署、建群、Addon、升级和删除；保存网络封锁与 digest 证据。**更新（R13，2026-09-22，rc.8 三机）：真机闭环（arm64 除外，用户明确排除）**——5003 export（skopeo --preserve-digests，5 制品 368MB，bootstrap index digest 重写为子 manifest digest）→ scp 离线拷贝+sha256 校验 → 空白 9443 import ×2 Inventory 全等 → iptables OUTPUT 专用链断公网（两节点外网 DNS/连接全 REJECT，REJECT 计数 dev-2=329/dev-3=269 包实证）→ ConfigMap+三节点 0600 json 切 9443 → componentmeta 9443 → 建群 Running（9443 拉取 183 条、五类仓库全覆盖，208=171/146=12）→ calico/coredns 全 Running → `kcctl delete cluster` 清空。升级经 bundle manifest 实证防护双拦截：digest 不一致拒（repointed tag 防护）+ rollout 后 revision 比对拒（5003 rc.8 包 sourceRevision=None、包内二进制实际构建 e9d9afe 而 manifest 声明 e9e95f4——制品元数据缺失，非升级缺陷；见 R13-C5 证据 §4）。边界：平台本体未从 bundle 重新 deploy（平台已运行 rc.8，同源制品经 upgrade --manifest 消费）；bundle 无第三方 addon 包（componentmeta addons 仅平台自带 cni/cri/k8s/k8s-extension） |
| 3 | `2.1-28`、`2.1-30` | 非法 CIDR 与创建中断的安全收敛 | R6 复现 Pod/Service CIDR 重叠仍可创建 Installing Cluster；取消后 Cluster/Operation、节点标签和主机副作用未自动清理，需修复创建前校验、cancel、retry 和安全删除。**更新（R8）：2.1-30 的删除收敛已修复（`978b1b43`，失败路径释放标签+force 逃生门实测生效）。更正（2026-09-20）：前文"2.1-28 创建前校验仍未实施"系误报——重叠校验自 `a989b14f`（rc.3 起）已在 API/CLI 生效，R9 dryRun 探针实测 rc.5 重叠 400 拒绝；R9 补齐列表内嵌套、每地址族数量与主机网段冲突校验（API 层，`ValidateCIDRHostConflict`）。2.1-28 部分已闭环（rc.6 `29a9bf8a` 真机负向矩阵 8 项 400+边界放行+双栈 200+CLI exit1+零残留，见 checklist 2.1-28 行）；2.1-30 的 retry 仍未验证** |
| 4 | `1.3-09`、`1.3-10` 部分 | 平台自身升级收尾 | B1 已按 OCI 契约实施：`all/server/agent --manifest` 三机实测通过（含幂等、降级/repointed tag 拒绝，R7 报告 §11）；`--version` 网络链路经代理隧道实测正常（GitHub 可达、404 处理正确），正向下载待首个 v2 stable 发布；仍缺 console/kcctl 组件升级（step 2）与升级中故障注入恢复 |
| 5 | `2.2-03`、`2.2-09`、`2.2-10` | Master 增删 | 添加后 control-plane/etcd quorum 正常；移除后 etcd member、证书、VIP 和节点角色正确收敛 |
| 6 | `2.5-08` | `maxBackupNum` 存储对象轮转 | **已闭环（R11，rc.8 `e9d9afeb` B4 持久化删除流）**：手动删除与 Cron 轮转统一进入持久化删除流程——deleting/deleteFailed 状态、Backup 记录保留到删除 Operation Succeeded 才由 backupcontroller 移除；真机复验 `maxBackupNum=2` + 2 分钟周期 Cron 连续 4+ 轮，Backup 记录与 FS 文件逐轮一一对应、无孤儿文件（见 checklist 2.5-08） |
| 7 | `5-06`、`3-12` | Operation cancel 自动收敛 | R5 需重启一个 `kc-server` 才继续推进；R6 取消 CIDR 创建后出现孤立 Running Operation/Installing Cluster，必须无需重启地让 Operation、Cluster 和 ExecutionLock 一致收敛。**更新（R8）：定位到 agent 侧饿死根因——worker `execute` defer LIFO 顺序 + server purge 竞争使单任务 worker 挂到 spec deadline（详见 R7 报告 §12.2），修复 `1413e849`。更新（R9，rc.6 `29a9bf8a` 真机复验通过）：①1M 建群 13 步任务时长 1s×7、5-8s×4、31/34s×2，无 10s 轮询尾延迟（修复前基线 10.01s/任务）；②快速 create→delete 收敛（集群/操作清空、标签释放）后立即再建群正常派发并 Running，不饿死。更新（R10，rc.7 `e7d99421` 真机复验）：协作式 cancel 语义矩阵与 2.1-30 retry 通过**——①Running 中取消（6/13 步）：在途步自然完成后停止派发、剩余 7 步 Canceled，Cluster InstallFailed，~2.5 分钟收敛；②最早取消（+3s，仅第 1 步在途）：1 步完成+12 步 Canceled，<23 秒收敛；③终态后重复取消被 CLI 干净拒绝（exit 1）；④Running 中并发双取消：第 2 次 API Conflict 拒绝，无状态污染；⑤retry 对 Canceled 创建操作：前 6 步保留原时间戳未重做，剩余步 ~60s 回 Succeeded，集群 InstallFailed → Running；⑥取消/失败后安全删除 20～30 秒清空 Cluster/Operation/标签，同节点重建 2 分钟 Running（kubelet inactive、无 /etc/kubernetes 残留、doctor 25/25）。§3.4 剩余子项：超时（spec deadline 到期）与 Watch 重连/Server、Agent 重启注入未覆盖。证据：dev-2 /tmp/r10-*.txt |
| 8 | `2.6-05`～`2.6-07` | OCI 缓存和 Registry 故障 | R6 已确认 `/root/.kc/config`、`deploy-config.yaml` 均为 0644，未达到敏感配置 0600；仍需覆盖缓存损坏、digest 不符、Registry 断连、配置优先级和凭据脱敏，不得回退到 tag。**更新（R9）**：写入侧自 batch-1 `a989b14f` 已 0600（存量 0644 为旧版遗留，重写时收紧）；R9 补齐原子替换+拒绝符号链接+umask 无关性及回归测试（B3 代码侧完成），远端重写后权限复验待下一 rc；2.6-05/06 缓存故障注入仍未测。**更新（R12，2026-09-22，rc.8 三机）：2.6-05/06 真机闭环**——①缓存篡改：master `charts.tgz` 翻一字节 → `validCachedHelmChart` payloadDigest 拒绝 → digest-pinned 重拉、sha 恢复（registry blob GET 佐证，Operation Succeeded，未执行坏内容未回退 tag）；②断连：retry 中途 kill registry → 在途 containerd 经 graceful shutdown 完成、后续 `connection refused` 明确失败 → registry 恢复后 retry 三节点从头重拉（56MB layer ×3）、Succeeded；③另发现删除集群时 uninstall 步骤清理组件包缓存（`CleanupPackage`），bootstrap 包豁免——无陈旧缓存残留问题。**更新（R15，2026-09-22）：2.6-05 包 contents 路径真机探针闭环（registry 侧 blob 篡改，9443 测试源）**——①浅篡改（tar 头损坏）：server indexer 解析失败 warn `archive/tar: invalid tar header`、包从清单剔除（componentmeta 消失）；②显式指定被剔除包：CLI fail-fast `missing packages: containerd 1.7.29`（EXIT=1 零对象）；③深篡改（20MiB 数据区翻转）：两节点 installRuntime attempt 0/1 均被 agent 侧 gzip CRC 阻断（任务回写 `"gzip: invalid checksum"`），3 秒 Failed 无半装，delete 零残留；④恢复 blob（sha256 复原）+清缓存显式建群 → Running（9443 实拉 GET 200）。registry 对 blob 内容与路径 digest 不符不做在线校验（仍 200）为本轮观察项；2.6-07 配置优先级/凭据脱敏专项仍 ⚠️ |
| 9 | `1.1-05`、`1.1-06`、`1.1-10` | HTTPS/认证 Package Registry | 公共 CA、自签 CA、账号密码分别覆盖 deploy、join、Agent 拉取及失败重试，日志不泄露凭据。**更新（R12，2026-09-22，rc.8 三机）：自签 CA + 账号密码已闭环**——dev-2 自建 distribution 3.0.0（HTTPS+自签 CA+htpasswd），三节点 0600 配置+deploy-config 双侧切换后建群成功（agent 真实拉取、三节点 Ready）；错误密码 → `UNAUTHORIZED: authentication required` 明确失败、修正后 retry Succeeded；未配 CA → `x509: certificate signed by unknown authority` 明确失败（并暴露 HTTP 回退得 400）；三节点日志/operation 全文 grep 错误与真实密码均 0 泄漏；缓存篡改探针与断连恢复见 P0 行 8。**更新（R13，2026-09-22）：缺项全部闭环**——公共 CA 环境独立证据=C3（自签 CA 加入三节点系统信任库等价公共 CA 环境：`package-registry.json` 零凭据零 CA 字段，建群 Running，TLS 拉取 231 次走系统信任池；运维发现：Go x509 进程内缓存系统根池，加 CA 后必须 `systemctl restart kc-server kc-agent`，否则 componentmeta 500 unknown authority）；join 入口认证=C4（auth registry 8443：join 下发 0600 凭据、被加入节点直连认证拉取 9×200、错误口令 EXIT=1 可读报错零残留） |

R4/R5/R6 已完成或部分完成的 HA、最小拓扑、Calico 自动探测、S3 备份、Cron、Policy 白名单和独立 join
主路径不再重复列为“未执行”；详细命令、
Operation ID、故障注入和清理证据见
[`R4 报告`](../superpowers/issues/2026-09-17-core-feature-e2e-sh-dev-2-3-4.md) 和
[`R5 报告`](../superpowers/issues/2026-09-17-core-feature-e2e-r5-sh-dev-2-3-4.md)、
[`R6 报告`](../superpowers/issues/2026-09-17-core-feature-e2e-r6-sh-dev-2-3-4.md)。

**R7（2026-09-19，新候选 `e9e95f4`）**：已完成“CI 打包→Registry 同步→清旧环境→重部署→
可执行矩阵”的新候选验收（见
[`R7 报告`](../superpowers/issues/2026-09-19-core-feature-e2e-r7-sh-dev-2-3-4.md)）。
2.1-28 在新候选的基线复测 ❌（该基线先于 batch-1 修复），batch-1 后 rc.1 复验已通过（见 R7 报告
§9）；2.5-08、2.5-11、2.6-07、3-10、4-08b 在新候选上复测仍未修复。新增缺口：

| 顺序 | 缺口 | 完成条件 |
|---:|---|---|
| N1 | 部署后无默认 image Registry 资源；未显式 `--image-registry` 的建群用 `registry.k8s.io` 并在离线机无限挂起 | deploy 初始化同端 Registry 资源或创建前明确报错 |
| N2 | CLI 默认 cri/cni 版本不随 `--k8s-version` 匹配 delivery policy，服务端以 500 拒绝 | CLI 按规则取默认值，服务端返回可读 400 |
| N3 | 建群失败（500/取消/InstallFailed 删除）不回滚节点 `kubeclipper.io/cluster`+`nodeRole` 标签，节点永久占用 | 任一失败路径均恢复节点空闲 |
| N4 | 备份 Operation 失败后集群卡 UpdateFailed，不能再次备份 | **已修复并复验通过**（R11，rc.8 `e9d9afeb` B4 持久化删除流真机复验：错误凭据/不存在 bucket → 备份 Operation 明确 Failed、Backup `error`、集群保持 Running 不再卡 UpdateFailed，可重新备份；失败记录经持久化删除流幂等清理，见 checklist 2.5-10） |
| N5 | 卡在健康检查重试环的创建 Operation 取消完全不收敛（重启无效），只能 etcd 手术 | cancel/timeout/重启任一路径可靠收敛并释放锁 |
| N6 | 被取消 Operation 不释放 ExecutionLock，删除集群卡 Pending | 终态 Operation 必须释放锁 |
| N7 | `PUT /backuppoints` 返回 200 但 s3Config 更新不生效；S3 endpoint 无入口校验 | **已修复并复验通过**（R11，rc.8 `e9d9afeb` 真机复验：合法 S3/FS 更新生效（bucket/endpoint 均可改）；入口拒绝带 scheme 的 endpoint（400 `must be host[:port] without a scheme`）；另验证 storage type 不可变——请求体与存储值不一致 400，须用小写 `s3`/`fs`） |
| N8 | agent worker 单任务饿死：任务被 finalize+purge 后 worker 的 informer store 留 stale 条目且 `execute` 挂到 spec deadline，该节点后续任务全部 Pending | **已修复并复验通过**（`1413e849`，defer LIFO 对调+NotFound 清 store/requeue，3 单测；rc.6 `29a9bf8a` 真机复验：1M 建群无 10s 尾延迟——13 步中 7 步 1s、最长非安装步 8s，修复前基线 10.01s/任务；create→delete 收敛后立即再建群正常派发 Running，不饿死，见 P0 行 7） |
| N9 | API 直调建群/dryRun 缺省可选 `cni.calico` 子对象即 500 panic：`calico.go` InitStep 对 nil 指针解引用（R9 探针 plain-legal 用例两次触发，栈经 kubeadm_step/utils/handler） | **已修复并复验通过**（R9 同日 `75ed938f`：`defaultCalico` nil 防护+CLI 同款默认值填充，空字段亦兜底，模板渲染链一并修复，2 单测；rc.7 `e7d99421` 真机复验：缺省 calico 块 dryRun 200（原 500），合法 200/重叠 400 回归通过） |

**R8（2026-09-20/21，rc.5 `c5ccb367`）**：失败路径收敛专项（见
[R7 报告 §12](../superpowers/issues/2026-09-19-core-feature-e2e-r7-sh-dev-2-3-4.md)）。
**N2 已修复**（`fdac85f2`：CLI 按 k8s 版本从 delivery policy 配对取默认 cri/cni，服务端
ResolverError 映射为可读 400；S1 实测 400+EXIT=1）。**N3 已修复**（`978b1b43`：全删除路径
释放节点占用标签+force 删除逃生门；S4/S5 实测标签释放、`echo yes | kcctl delete cluster -F`
约 30 秒收敛）。2.1-28 的 R8 "rc.5 仍接受重叠网段"记录系误报（更正见 P0 行 3）：重叠校验在
rc.5 已生效，R9 dryRun 探针实测 400 拒绝。新增 N8（agent worker 饿死）。

**R9（2026-09-21，rc.6 `29a9bf8a`）**：N8 复验通过（P0 行 7/N8 行）；2.1-28 边界补齐并随
rc.6 真机负向矩阵闭环（P0 行 3）。测试配置记录：1M 拓望建群必须带 `--untaint-master`，否则
master taint 使 coredns Pending、健康检查无限重试（详见 R7 报告 §12.3）。R9 探针另发现 N9
（缺省 `cni.calico` 块 dryRun/创建 500 panic），同日修复待随下一 rc 复验。

**R10（2026-09-21，rc.7 `e7d99421`）**：N9 真机复验通过（缺省 calico 块 dryRun 200）；B3 远端
复验通过（dev-2 两配置文件 0600）；协作式 cancel 语义矩阵与 2.1-30 retry 通过（P0 行 7 与
checklist 2.1-30 行）。R6 轮遗留的 /tmp 临时证书（kc-r6-admin-client.*）与各轮临时 kcctl/
manifest 已清理；Registry 仅新增 rc.7 tag，存量未动。

**R11（2026-09-22，rc.8 `e9d9afeb`）**：B4 备份一致性专项（持久化删除流）真机复验。三机升级
rc.8（`kcctl upgrade all --manifest`，6 节点槽位 upgraded to revision `e9d9afebb2a8`，doctor
25/25）。2.5-08/09/10/11 四项闭环（P0 行 6、P1 行 6、N4、N7），九项证据：①手动删除进入
deleting（status + `delete-operation-name` 标签可见），删除 Operation Succeeded 后记录 404、
FS 文件同步消失；②顺序重复删除幂等 200（复用在途任务，不建新 Operation）；③deleting 窗口
恢复请求 400（"backup ... is deleting now, can't recovery"），正常 available 备份恢复 200 →
集群回 Running、节点 Ready；④maxBackupNum=2 轮转无孤儿（P0 行 6）；⑤S3 备份点全链路
（seaweedfs S3 临时起于 dev-2，创建 available + 删除记录消失）；⑥错误凭据/不存在 bucket →
Operation Failed、Backup `error`、集群 Running（N4）；⑦S3 停机删除 → deleteFailed 记录保留，
S3 恢复后重试删除新 Operation 成功（`e9d9afeb` 主场景）；⑧N7 三项（更新生效/scheme 400/
storage type immutable）；⑨2.5-11 describe 200/404。已知边角：同毫秒并发双 DELETE 同一备份，
一个 200 一个 500（createOperationV2 冲突，日志无 reason），无重复副作用、不影响一致性，记为
低优先改进项。测试配置记录：备份点从 cluster label `kubeclipper.io/backupPoint` 读取；创建
备份 POST /clusters/{name}/backups 仅需 metadata.name；恢复用 POST /clusters/{cluster}/
recovery 的 `useBackupName`；backuppoint storageType 存储值为小写 `s3`/`fs`，immutable 校验
须用同值请求体。终态清理：CronBackup/残留备份/坏备份点/测试集群删除，三节点 kubeadm reset +
集群 etcd 目录清理，Registry 仅增 rc.8 tag（12 tags），临时 weed、manifest、二进制与证书均已删除。

**R12（2026-09-22，rc.8 `e9d9afeb`）**：B5 认证 Registry 与缓存故障专项（用户批准范围：认证
Registry、断连重头拉、最小化篡改探针）。环境：dev-2 自建 distribution 3.0.0（`172.16.131.208:8443`，
HTTPS 自签 CA SAN=IP+htpasswd r12user），`kcctl registry sync` 从共享 146:5003（HTTP）镜像
6 个 artifact（digest 逐项一致，6 copied 0 skipped——sync 源 HTTP 回退与目标侧完整认证实测）；
三节点 `package-registry.json`（0600）+ deploy-config ConfigMap `packageRegistry` 双侧切换，
动态生效无重启。①正向：r12-auth-cluster 建群 Succeeded（15 步）、集群 Running、三节点
Ready=True，三节点真实拉取（blob GET 208=54/146=12/230=12）。②错误密码（P0 行 9）：dev-4
改坏密码+清缓存 → 建群 Operation Failed，错误消息 `UNAUTHORIZED: authentication required`
（URL 定位、不含凭据）；registry 侧确认 230 携带凭据仍 401；三节点日志/operation 全文 grep
错误与真实密码均 0 泄漏；修正后 retry Succeeded、集群 Running（同时实证 Failed 态 retry 语义
与缓存重建）。③未信 CA（P0 行 9）：dev-3 配置去掉 CA → 拉取 [1s] 失败，错误
`x509: certificate signed by unknown authority`（并暴露 go-containerregistry 的 HTTP 回退尝试得
400，无凭据泄漏）。④缓存篡改（2.6-05，P0 行 8）：master `charts.tgz` 翻一字节 → 建群
`validCachedHelmChart` payloadDigest 校验拒绝 → digest-pinned 重拉（registry tigera-operator
blob GET 06:01:52 佐证）→ sha 恢复原值、Operation Succeeded；篡改文件跨集群删除幸存（chart
缓存不受 uninstall 清理）。⑤断连（2.6-06）：retry 中途 kill registry → 在途 containerd 拉取经
graceful shutdown 完成，其余任务 `dial tcp: connect: connection refused` 明确失败；registry 恢复
后 retry → 三节点 k8s 包全部从头重拉（56MB layer ×3，半成品未被信任）、Succeeded、集群
Running。行为记录：删除集群时 uninstall 的 `CleanupPackage` 清理该集群组件包缓存（k8s/CRI/
k8s-ext/calico 包路径），bootstrap 包豁免；calico chart 仅 master 消费（worker 上的 chart 缓存
不被使用）。终态清理：全部测试集群删除、三节点 free+Ready、配置与 ConfigMap 还原
146:5003、测试 registry 进程停止并删除 `/tmp/registry-r12`、Mac 侧 `/tmp/r12-auth`/`kcctl-r12`
删除、全部临时凭据/证书即用即删（共享 Registry 与 /var/lib/kc-etcd 未动）。B5 余量：纯离线
bundle、arm64、公共 CA、join 入口认证及 qualification workflow `secrets.GITHUB_TOKEN` 映射
（见 remediation plan §6）。

**R13（2026-09-22，rc.8 `e9d9afeb`）**：R12 余量专项（用户批准范围：C1 qualification workflow
token 映射+临时凭据权限、C2 1.1-09 剩余负向、C3 公共 CA 等价验证、C4 join 入口认证、
C5 纯离线 bundle 全链路；arm64 用户明确排除）。①C1：qualification workflow `secrets.GITHUB_TOKEN`
映射+发布临时文件 0600/清理（commit `d2ca8df8`）。②C2（1.1-09 补全）：错误仓库地址探针
（dial refused 明确失败、零残留；服务端 500 透传原始传输错误，低于 400 typed 路径质量，观察项）
与缺失制品探针（空仓库 rules=0 → `ArtifactNotPublished` typed 400），修正后 PUT 回 5003 → 建群
成功→删除，retry 语义实证。③C3（1.1-10）：r13-kc-test-ca 加入三节点系统信任库（等价公共 CA：
凭据文件零配置），7443 TLS-only registry 建群 Running、TLS 拉取 231 次走系统信任池；**运维发现：
Go crypto/x509 首用时加载并进程内缓存系统根池，加 CA 后必须重启 kc-server/kc-agent**
（否则 componentmeta 500 x509 unknown authority，重启即恢复）。④C4（1.3-05 认证负向）：
drain 230 → 8443 HTTPS+htpasswd → 两节点集群认证拉取 → join 230 下发 0600 凭据 → 扩容 230
入群，agent 直连认证拉取 9×200 → 错误口令 join EXIT=1 可读 `UNAUTHORIZED` 零部分安装；
过程发现：join 多网卡 precheck 文案对显式 first-found 误导（sudo.go:131）、ip-detect 合法语法
`interface=<nic>`、跨节点写配置须 base64 落盘（多层引号内插剥引号致 JSON 解析失败）。
⑤C5（1.1-03/08）：见 P0 行 2——断公网建群 Running、import ×2 幂等、升级防护双拦截
（repointed tag 拒+rollout 后 revision 比对拒；根因为 5003 rc.8 包 sourceRevision=None 且
包内二进制构建自 e9d9afe 而 manifest 声明 e9e95f4，制品元数据缺失非升级缺陷；发布侧防再发：
KC_SOURCE_REVISION 强校验三个二进制 gitCommit）。终态清理：测试 CA 三节点信任库移除
（update-ca-certificates 复验 0）、7443/8443/9443 测试 registry 停止+数据删除、8443 口令文件
随目录删除、/tmp 全部 r13 临时产物清除（证据日志存档并脱敏 basic-auth b64）、配置/ConfigMap/
iptables 全还原，平台 3 node Ready、无集群、5003 与 /var/lib/kc-etcd 未动。

## P1：核心能力补全

| 顺序 | Case | 缺口 | 完成条件 |
|---:|---|---|---|
| 1 | `1.3-05`、`3-04` | `kcctl join` 独立纳管的负向与安全边界 | R6 已完成空闲 dev4 的独立 join 主路径。**更新（R13-C4，2026-09-22）：错误凭据与失败清理闭环**——auth registry 8443 下 join：凭据 0600 下发、被加入节点直连认证拉取、错误口令 EXIT=1 可读 `UNAUTHORIZED` 且零部分安装（agent 二进制/服务均未创建）；HTTPS+自签 CA 场景经系统信任隐式覆盖（join 资产拉取在 kcctl 所在节点经系统信任池校验 8443 TLS）。仍缺：重复 join 边界、join 前自签 CA 未入信任库的显式负向 |
| 2 | `2.1-21`、`2.1-22` | 镜像 Registry 与 Package Registry 分工 | R6 已用不同资源名分别落库并确认 CRI `hosts.toml` 下发，但两者仍指向同一 HTTP 端点；还需不同端点、认证和自签 CA，并验证 Pod 拉取 |
| 3 | `2.1-12`、`2.1-27`～`2.1-30` | 创建集群负向与恢复 | R6 已验证合法外部 IP/SAN、占用节点、Master/Worker 重复和非法端口/域名前置拒绝；仍需域名代理连通性、跨 Region、CIDR 冲突修复、完整主机预检及中断 retry/安全删除 |
| 4 | `2.2-06`、`2.2-07`、`2.2-11`～`2.2-13` | 节点管理边界 | R6 已验证空闲 Agent drain/delete 后 join 恢复；仍需掉线注入、集群占用保护、Lease/证书残留、Agent 身份保护和 Region 约束；disable/enable 已在 R4 覆盖 |
| 5 | `2.3-02`、`2.3-07`、`2.3-09` | 集群升级故障与可用性 | 注入中断后安全 retry；Registry tag 变化不影响固定 digest；滚动顺序、PDB 和业务连续性明确 |
| 6 | `2.5-11` | Backup 详情查询 API | **已闭环（R11，rc.8）**：已有 Backup 的 `GET /backups/{name}` 200 返回全字段（backupStatus/clusterNodes/preferredNode），不存在对象 404；列表和集群范围查询不受影响（见 checklist 2.5-11） |
| 7 | `4-08`～`4-08d`、`4-18` | 用户、登录和 RBAC | R6 已完成临时 user/role CLI CRUD、重复名拒绝和正确密码登录；内置只读用户越权 403 通过，但自定义 role 登录后读取 Cluster/Node 仍 403，需修复绑定授权并补 enable/disable、验证码、Token 和限流闭环 |
| 8 | `4-07` | Console 核心 E2E | 登录、建群、升级、备份、删除、Operation 进度和失败原因展示与 API 状态一致 |
| 9 | `3-09`、`3-10`、`3-12`、`3-15`、`3-19`、`3-24`、`3-25`、`3-27` | 未覆盖或未闭环的 `kcctl` 命令 | R6 已补 get 资源扫尾、registry list/image/非法 push、drain、completion、无效升级包和部分登录/RBAC；仍需每条命令成功/典型失败闭环，修复 `get --watch`，补 cancel 自动收敛、valid registry 生命周期、login TLS 和 deploy config 优先级 |
| 10 | `5-13`、`5-14` | 重复提交与超时收敛 | 重复请求不产生并发副作用；timeout 后 Task、Cluster 和 ExecutionLock 按既定语义收敛 |

补充的命令缺陷：`3-10 kcctl get --watch` 已在 R4 复测为失败项。CLI 虽然展示 `-w/--watch`，
但当前实现未把该 flag 传入查询或建立 watch 流，命令一次输出后退出；应修复或从 CLI 暴露面移除，
在修复前不能按“部分验证”统计。R6 另确认 `3-15 drain` 的真实语义仅是 KubeClipper Agent
节点注销，不是 Kubernetes Pod 驱逐；清单和命令帮助不得继续写成 PDB/Pod eviction。
**更新（2026-09-20）**：`3-10` 已修复（R7 batch-3，`12d59d9b`），`kcctl get --watch` 持续输出
watch 事件并在服务端流关闭后自动重连。4-08b 的 R7 403 复测改判为测试方法
错误（注解键 `kubeclipper.io/role` 应为 `iam.kubeclipper.io/role`），正确键下授权放行与越权
403 均验证通过。
**更新二（2026-09-20，N9 闭环）**：服务端 watch 流快速关闭现象（audit ~0.4ms 内
ResponseComplete，Go 客户端常收 1-2 事件后 EOF）根因已定位并修复——核心/IAM 资源
handler 的默认 watch 超时计算漏乘 `time.Second`（1800~3600 纳秒的 timer 立即触发），
非 go-restful chunked 流或 HTTP/2 问题；修复见 R7 报告 §9.2（commit `b23a9ab2`，
`v2.0.3-rc.3` 部署验证流持续、实时事件送达、CLI --watch 正常）。

## P2：扩展能力与环境矩阵

- `4-03`：MetalLB BGP，需要可控的 BGP 邻居环境。
- `2.1-32`：IPv4/IPv6 dual-stack，需要双栈主机、双 Pod/Service CIDR、Calico 双栈和跨节点/Service 验收；当前三机没有可控 IPv6 环境。
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
| Docker CRI（`6-12`） | 当前产品不支持 Docker CRI（包括 dockershim/外置 Docker），现行 Kubernetes 版本也没有可用的 Docker CRI 支持；R6 传入 `--cri docker --cri-version 20.10.24` 已 rc=1 拒绝，但 help/参数校验和代码仍保留 Docker 分支，而 OCI 发布矩阵只有 containerd | 这是应删除的废弃入口，不是待补测能力：从 CLI help/校验、策略输出和运行分支中移除 Docker；保留“传入 Docker 明确拒绝”的静态/负向门禁，不安排 Docker E2E。独立 Docker Registry 管理命令与 Docker CRI 不是同一功能 |
| legacy static server/tar downloader | 当前正式交付路径已经移除，旧节点不支持原地混用 | 不恢复兼容路径；只验证全新部署不依赖旧服务，以及旧节点必须清理后重新 deploy/join 的边界 |
| `nfs-provisioner` | 已退休，由 `nfs-csi` 替代 | 不保留功能 Case；只做默认策略、资源清单和 Console 不再暴露旧组件的静态门禁 |
| legacy package 迁移工具 | 仓库仍保留 `migrate-legacy-packages-to-oci.sh` 一次性导入工具 | 若当前版本不承诺旧包迁移，直接删除脚本和相关说明，不保留长期兼容或迁移流程；不计入核心回归 |

## 本轮证据要求

每个完成项至少保存：Git revision、主机与拓扑、物料来源、组件版本、命令或请求、Operation ID、
关键日志、最终对象状态、必要的 digest/证书/网络证据，以及清理结果。没有这些信息时只能回填 ⚠️。
