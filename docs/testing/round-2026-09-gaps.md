# KubeClipper 当前核心功能缺口与执行建议（2026-09）

本文档只维护当前缺口、执行优先级、废弃残留和迁移边界。完整功能范围、状态及稳定 Case 编号见
[`core-feature-checklist.md`](core-feature-checklist.md)。完成一项后，应先回填核心清单的状态、
轮次和证据，再从本文档移除或降级该缺口。

状态判断必须基于真实运行证据；CI、单测、代码路径存在或其他流程隐含经过，只能作为部分验证。

## P0：发布主链路缺口

| 顺序 | Case | 缺口 | 完成条件 |
|---:|---|---|---|
| 1 | `1.2-07` | HA 故障窗口完整验收 | **已闭环（R19，2026-09-24，rc.10 三机，全程零集群副作用）**——①停 dev-3 kc-server：dev-2 上 `kcctl get node --watch` 流不断（无重连输出）、`kcctl get node` 正常（quorum 2/3 服务）；②停 dev-2 kc-server（被连接节点）：流断（"watch stream ended; reconnecting..."）→ 重连 `connection refused` → 进程退出（连接失败即 return，仅流正常结束后单次重连），**发现：kcctl 客户端单地址无 failover**（`/root/.kc/config` 单 server 地址，不尝试其余 server）；③窗口期 console：dev-2 登录页 200（caddy 本地静态），`/api` 请求经 caddy 健康检查（health_interval 10s）摘除坏上游、由存活上游 kc-server 应答 403 JSON（非 502），dev-3/dev-4 console 200；④恢复 dev-2 后新建 watch 立即 list+事件流。证据转录 R7 报告 §12.15 |
| 2 | `1.1-03`、`1.1-08`、`2.1-23` | 纯离线 bundle 真机闭环 | export、拷贝、重复 import 后，在断公网环境完成平台部署、建群、Addon、升级和删除；保存网络封锁与 digest 证据。**更新（R13，2026-09-22，rc.8 三机）：真机闭环（arm64 除外，用户明确排除）**——5003 export（skopeo --preserve-digests，5 制品 368MB，bootstrap index digest 重写为子 manifest digest）→ scp 离线拷贝+sha256 校验 → 空白 9443 import ×2 Inventory 全等 → iptables OUTPUT 专用链断公网（两节点外网 DNS/连接全 REJECT，REJECT 计数 dev-2=329/dev-3=269 包实证）→ ConfigMap+三节点 0600 json 切 9443 → componentmeta 9443 → 建群 Running（9443 拉取 183 条、五类仓库全覆盖，208=171/146=12）→ calico/coredns 全 Running → `kcctl delete cluster` 清空。升级经 bundle manifest 实证防护双拦截：digest 不一致拒（repointed tag 防护）+ rollout 后 revision 比对拒（5003 rc.8 包 sourceRevision=None、包内二进制实际构建 e9d9afe 而 manifest 声明 e9e95f4——制品元数据缺失，非升级缺陷；见 R13-C5 证据 §4）。边界：平台本体未从 bundle 重新 deploy（平台已运行 rc.8，同源制品经 upgrade --manifest 消费）；bundle 无第三方 addon 包（componentmeta addons 仅平台自带 cni/cri/k8s/k8s-extension） |
| 3 | `2.1-28`、`2.1-30` | 非法 CIDR 与创建中断的安全收敛 | R6 复现 Pod/Service CIDR 重叠仍可创建 Installing Cluster；取消后 Cluster/Operation、节点标签和主机副作用未自动清理，需修复创建前校验、cancel、retry 和安全删除。**更新（R8）：2.1-30 的删除收敛已修复（`978b1b43`，失败路径释放标签+force 逃生门实测生效）。更正（2026-09-20）：前文"2.1-28 创建前校验仍未实施"系误报——重叠校验自 `a989b14f`（rc.3 起）已在 API/CLI 生效，R9 dryRun 探针实测 rc.5 重叠 400 拒绝；R9 补齐列表内嵌套、每地址族数量与主机网段冲突校验（API 层，`ValidateCIDRHostConflict`）。2.1-28 部分已闭环（rc.6 `29a9bf8a` 真机负向矩阵 8 项 400+边界放行+双栈 200+CLI exit1+零残留，见 checklist 2.1-28 行）；2.1-30 的 retry 已在 R10（rc.7 `e7d99421`）真机闭环、超时/重启注入子项 R16 闭环，本项全部关闭** |
| 4 | `1.3-09`、`1.3-10` 部分 | 平台自身升级收尾 | B1 已按 OCI 契约实施：`all/server/agent --manifest` 三机实测通过（含幂等、降级/repointed tag 拒绝，R7 报告 §11）；`--version` 网络链路经代理隧道实测正常（GitHub 可达、404 处理正确）。**console/kcctl 组件升级（step 2）已闭环（R19，2026-09-24，rc.10 `c356fbaf`）**：`upgrade all` 三机 12 槽位（server×3→agent×3→console×3→kcctl×3）~106s 全成功（platform API 报 rc.10、doctor 25/25、console 全 200）；`upgrade kcctl` 同 revision 三节点幂等 skip；`upgrade console` 同版本幂等重装（dist/caddy sha256 前后一致）；负向：缺 console artifact manifest EXIT=1（preflight 拒绝）、console 错 digest 触碰节点前拒绝（repointed tag 防护）、kubeclipper 错 digest+revision label 匹配警告放行（re-tag 回退语义）。**两项余量已收口（R20，2026-09-24，rc.11 `9a1dbb75`，R7 报告 §12.16）**：①升级中故障注入矩阵四场景真机通过——T-A server 安装+启动成功但健康探测被阻断 180s → 自动恢复 rc.10 基线（md5/healthz 核验）且后续槽位停止；T-C staging kcctl 被腐化 → 替换后 probe 失败 → restoreKcctl（三节点 md5=基线）；T-D console 探针 180s 超时 → restoreConsole（caddy+dist 树哈希逐字节一致）；T-B 中断后重跑 → 已完成节点 skip、余下补齐、platform API 复核 + doctor 25/25；②`--version v2.0.3` 在线正向下载+checksum 以本地 GitHub 镜像法闭环（自签 CA 进系统信任 + /etc/hosts + 443 HTTPS，URL 与真实 release 同构；全链路 rc=0，`.sha256` 篡改报 checksum mismatch 拒绝；临时证书即用即删）。**B1 关闭** |
| 5 | `2.2-03`、`2.2-09`、`2.2-10` | Master 增删 | 添加后 control-plane/etcd quorum 正常；移除后 etcd member、证书、VIP 和节点角色正确收敛。R17（rc.8）定性为产品缺口（`makeMasterCompare` 无条件 `return ErrInvalidNodesRole`）。**已闭环（R18，rc.9 `5e4cfb4b`，2026-09-24）**：master add/remove 完整实现——双 master 集群 Ready 后 add dev-2：8/8 OperationTasks Succeeded（installRuntime→getJoinCommand(存活 master)→renderMasterJoinConfig controlPlane=true→joinNode→waitForAddedNodesReady），etcd 3 成员 started、节点 Ready control-plane；remove dev-2：13/13 OperationTasks Succeeded（removeEtcdMember(存活 master)→drainNode→kubeadmReset→removeEtcdDataDir→clearVIPDomain→uninstallRuntime 等），etcd 回 2 成员、离开节点 /etc/kubernetes 与 /var/lib/etcd 清空、VIP/IPVS 清理、kubelet/containerd inactive；负向 remove 1 of 2 → 400 quorum 文案（直调+`?dryRun=true` 均拒，零副作用）。边界：0 worker 拓扑下 worker 侧 lvscare refreshLvsCare 按设计不生成步骤、未真机覆盖（单测覆盖） |
| 6 | `2.5-08` | `maxBackupNum` 存储对象轮转 | **已闭环（R11，rc.8 `e9d9afeb` B4 持久化删除流）**：手动删除与 Cron 轮转统一进入持久化删除流程——deleting/deleteFailed 状态、Backup 记录保留到删除 Operation Succeeded 才由 backupcontroller 移除；真机复验 `maxBackupNum=2` + 2 分钟周期 Cron 连续 4+ 轮，Backup 记录与 FS 文件逐轮一一对应、无孤儿文件（见 checklist 2.5-08） |
| 7 | `5-06`、`3-12` | Operation cancel 自动收敛 | R5 需重启一个 `kc-server` 才继续推进；R6 取消 CIDR 创建后出现孤立 Running Operation/Installing Cluster，必须无需重启地让 Operation、Cluster 和 ExecutionLock 一致收敛。**更新（R8）：定位到 agent 侧饿死根因——worker `execute` defer LIFO 顺序 + server purge 竞争使单任务 worker 挂到 spec deadline（详见 R7 报告 §12.2），修复 `1413e849`。更新（R9，rc.6 `29a9bf8a` 真机复验通过）：①1M 建群 13 步任务时长 1s×7、5-8s×4、31/34s×2，无 10s 轮询尾延迟（修复前基线 10.01s/任务）；②快速 create→delete 收敛（集群/操作清空、标签释放）后立即再建群正常派发并 Running，不饿死。更新（R10，rc.7 `e7d99421` 真机复验）：协作式 cancel 语义矩阵与 2.1-30 retry 通过**——①Running 中取消（6/13 步）：在途步自然完成后停止派发、剩余 7 步 Canceled，Cluster InstallFailed，~2.5 分钟收敛；②最早取消（+3s，仅第 1 步在途）：1 步完成+12 步 Canceled，<23 秒收敛；③终态后重复取消被 CLI 干净拒绝（exit 1）；④Running 中并发双取消：第 2 次 API Conflict 拒绝，无状态污染；⑤retry 对 Canceled 创建操作：前 6 步保留原时间戳未重做，剩余步 ~60s 回 Succeeded，集群 InstallFailed → Running；⑥取消/失败后安全删除 20～30 秒清空 Cluster/Operation/标签，同节点重建 2 分钟 Running（kubelet inactive、无 /etc/kubernetes 残留、doctor 25/25）。超时（spec deadline 到期）与 Server/Agent 重启注入子项已于 R16（rc.8）真机闭环（见 checklist 2.1-30 行 R16 标注），本项全部关闭；Watch 重连证据归入 P0 行 1（1.2-07）继续跟踪。证据：dev-2 /tmp/r10-*.txt（终态清理删除，结论转录 R7 报告 §12.6） |
| 8 | `2.6-05`～`2.6-07` | OCI 缓存和 Registry 故障 | R6 已确认 `/root/.kc/config`、`deploy-config.yaml` 均为 0644，未达到敏感配置 0600；仍需覆盖缓存损坏、digest 不符、Registry 断连、配置优先级和凭据脱敏，不得回退到 tag。**更新（R9）**：写入侧自 batch-1 `a989b14f` 已 0600（存量 0644 为旧版遗留，重写时收紧）；R9 补齐原子替换+拒绝符号链接+umask 无关性及回归测试（B3 代码侧完成），远端重写后权限复验待下一 rc；2.6-05/06 缓存故障注入仍未测。**更新（R12，2026-09-22，rc.8 三机）：2.6-05/06 真机闭环**——①缓存篡改：master `charts.tgz` 翻一字节 → `validCachedHelmChart` payloadDigest 拒绝 → digest-pinned 重拉、sha 恢复（registry blob GET 佐证，Operation Succeeded，未执行坏内容未回退 tag）；②断连：retry 中途 kill registry → 在途 containerd 经 graceful shutdown 完成、后续 `connection refused` 明确失败 → registry 恢复后 retry 三节点从头重拉（56MB layer ×3）、Succeeded；③另发现删除集群时 uninstall 步骤清理组件包缓存（`CleanupPackage`），bootstrap 包豁免——无陈旧缓存残留问题。**更新（R15，2026-09-22）：2.6-05 包 contents 路径真机探针闭环（registry 侧 blob 篡改，9443 测试源）**——①浅篡改（tar 头损坏）：server indexer 解析失败 warn `archive/tar: invalid tar header`、包从清单剔除（componentmeta 消失）；②显式指定被剔除包：CLI fail-fast `missing packages: containerd 1.7.29`（EXIT=1 零对象）；③深篡改（20MiB 数据区翻转）：两节点 installRuntime attempt 0/1 均被 agent 侧 gzip CRC 阻断（任务回写 `"gzip: invalid checksum"`），3 秒 Failed 无半装，delete 零残留；④恢复 blob（sha256 复原）+清缓存显式建群 → Running（9443 实拉 GET 200）。registry 对 blob 内容与路径 digest 不符不做在线校验（仍 200）为本轮观察项；2.6-07 配置优先级/凭据脱敏专项仍 ⚠️。**更新（R16，2026-09-23，rc.8）：2.6-07 专项闭环 + 2.6-10 余量补齐**——①2.6-07 优先级：server json=9443 + deploy-config=5003 时 componentmeta 与 dryRun packagePlan 均解析至 5003（deploy-config 单一事实源；json 仅按地址匹配供 scheme/凭据），同时更正了此前"json 覆盖 deploy-config"的记录（双写同值无法区分，R16 以不等值矩阵判定）；②2.6-07 凭据脱敏：8443 htpasswd registry 正确口令 componentmeta 200 / 错误口令 401，口令在 server json、GET API、kc-server/kc-agent journal 与 /etc /root /var/lib/kubeclipper 全量 grep 0 泄漏（探针后凭据文件即删）；③2.6-10 缺失 blob：9443 拷入 containerd:1.7.29 后删 layer blob 数据文件 → 索引器读包清单失败剔除 tag → POST 400 `ArtifactNotPublished: artifact cri/containerd:1.7.29 is not published` 零对象（blob 404 于 registry 日志佐证）；④2.6-10 多候选冲突：policy 双 slot（cri+cri-alt）同版本 → dryRun 与真实 POST 均 400 `DuplicateResolvedComponent: component cri/containerd selected by slots "cri" and "cri-alt"`；同 slot 重名 400 `duplicate component slot "cri"`，单 slot 双 option（name 不同）不冲突——按 option.name 匹配。policy/deploy-config 全还原，dryRun 200 验证 |
| 9 | `1.1-05`、`1.1-06`、`1.1-10` | HTTPS/认证 Package Registry | 公共 CA、自签 CA、账号密码分别覆盖 deploy、join、Agent 拉取及失败重试，日志不泄露凭据。**更新（R12，2026-09-22，rc.8 三机）：自签 CA + 账号密码已闭环**——dev-2 自建 distribution 3.0.0（HTTPS+自签 CA+htpasswd），三节点 0600 配置+deploy-config 双侧切换后建群成功（agent 真实拉取、三节点 Ready）；错误密码 → `UNAUTHORIZED: authentication required` 明确失败、修正后 retry Succeeded；未配 CA → `x509: certificate signed by unknown authority` 明确失败（并暴露 HTTP 回退得 400）；三节点日志/operation 全文 grep 错误与真实密码均 0 泄漏；缓存篡改探针与断连恢复见 P0 行 8。**更新（R13，2026-09-22）：缺项全部闭环**——公共 CA 环境独立证据=C3（自签 CA 加入三节点系统信任库等价公共 CA 环境：`package-registry.json` 零凭据零 CA 字段，建群 Running，TLS 拉取 231 次走系统信任池；运维发现：Go x509 进程内缓存系统根池，加 CA 后必须 `systemctl restart kc-server kc-agent`，否则 componentmeta 500 unknown authority）；join 入口认证=C4（auth registry 8443：join 下发 0600 凭据、被加入节点直连认证拉取 9×200、错误口令 EXIT=1 可读报错零残留） |
| 10 | （新增）创建 payload `imageRegistry` | 离线建群未显式指定 imageRegistry 时慢失败且无创建前校验 | **新发现（R17，rc.8 真机）**：离线建群 payload 无 `imageRegistry` → kubeadm.yaml 不渲染 `imageRepository`（默认 registry.k8s.io）→ 离线 `kubeadm init` 拉 7 镜像 i/o timeout 慢失败（单 attempt ~18 分钟），创建前零校验零提示；`DownloadImage` 仅 Upgrade 路径且 OCI 交付下直接报错（pkg/scheme/core/v1/k8s/cluster.go:397）。R15/R16 建群成功系节点 containerd 遗留镜像掩护。**已闭环（R18，rc.9 `5e4cfb4b`，2026-09-24）**：选择"创建前显式拒绝"方案——`createClusterCheck` 对离线+空 imageRegistry 400 拒绝，错误文案解释 registry.k8s.io 不可达与需 Registry 对象名；真机复验：真实 POST 无 imageRegistry **48ms** 400、dryRun 同 400、clusters 数组 0（零对象），对照组带 `kc-package-registry` dryRun 200；单测正负两例（handler_test.go）。显式 `imageRegistry: kc-package-registry` 正向建群由 R17 已证（20/20 Succeeded） |
| 11 | （新增 R21）`cluster upgrade` 派发 | 升级操作 Pending 死锁 | 提交时 agent 离线 → operation 永久 Pending：任务不派发、agent 恢复不补派、cancel/retry 拒绝 Pending、须重启 kc-server 才能取消。**已修复（R22,`9f61c944`,rc.13）**：acquireLock 驱逐失效持锁者（terminal/消失的 op 永远不会自释放）；复验:agent 离线提交升级 → 立即 Running、取消收敛（R7 报告 §12.18）。**残留跟进已闭环（R23,rc.16,`3be0ffe8`,R7 报告 §12.19）**："步骤执行丢失"立项复查结论为**双重误诊**——R22 监控用错 etcd 前缀（`/registry/operations.kubeclipper.io/`，实际 `/registry/kc-server/`）且 operationtasks API list 无参返回空，任务对象一直存在（16 个：15 Succeeded+1 Running）；真正缺陷=单节点升级 `kubectl drain` 遭 PDB 拒绝（calico-apiserver）无限重试挂死。已修：两处升级 drain 加 `--timeout=120s \|\| true`；rc.16 复验：卡 drain op 取消收敛 → retry 复用不可变 plan（steps 哈希 c0d395a025a134c8 不变）→ Succeeded、节点 v1.37.0 |
| 12 | （新增 R21）已删集群操作泄漏 | InstallComponents 无限增殖 | clustercontroller CRI-registry 对账对已删集群持续创建孤儿 Pending 操作（实测 16,595 个）；operations API 无 DELETE 路由无法清理。**已修复（R22,`9d987785`,rc.12）**：DeletionTimestamp 即跳过对账 + 存在非终结 op 不再新建；复验:删集群后 op 总数 4→0 且稳定（R21 同场景 16,595 持续增长）；发布器 tag 冲突拒绝同轮实证（R7 报告 §12.18） |

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

**R17（2026-09-23，rc.8 `e9d9afeb`，平台 clean 后重 deploy，用户批准含 /var/lib/kc-etcd 重建；
arm64 继续排除）**：范围两项——①B5 余量：平台本体从 OCI registry 重 deploy + 部署侧断连注入；
②Master 增删真机（2.2-03/09/10）。工程护栏：bootstrap 清单前置核验（4 kinds 齐全才准 clean）、
registry.bin sha256 双侧一致守卫（3162d930…）、kc-etcd snapshot+deploy-config 备份后才执行
`kcctl clean -A`（连带 /var/lib/kc-etcd 与 ~/.kc/deploy-config；不动 /var/lib/kubeclipper/cache
与 registry 二进制）。①场景 A（precheck 期断连）：registry 不可达 → 15s 硬超时双 scheme
（https+http）探测快速失败，报错含 `kcctl registry sync` 提示，零节点影响；②场景 B
（sendPackage 期断连）：证书已分发后 tcp-reset 注入 → `refresh bootstrap assets from registry
... connection refused` abort（exit 1），无重试，半安装态=证书+包缓存，恢复网络后无需 clean
直接重 deploy 成功；③场景 C（完整重 deploy）：~4 分钟，4 服务 active（kc-etcd 12379/12380/12381、
kc-server、kc-console、kc-agent），3 节点 Healthy、agentID 经 `-c` 配置模式全部保持，bootstrap
包取 5003 最高 semver tag，healthz/console 200，package-registry.json 重新生成（http scheme），
kc-etcd 重建全新 3 成员。deploy 非幂等实证：preCheck 拒绝已有服务（"clean old environment
before deploying"），重 deploy 必须 clean -A 前置；clean 后 package-registry.json 消失 →
scheme 解析链回退须经 `KC_PACKAGE_REGISTRY_CONFIG` 环境变量（flags > env > 已安装文件 >
默认 https）。**产品发现（P0 行 10）**：离线建群 payload 未显式 `imageRegistry` 时 kubeadm init
从 registry.k8s.io 拉镜像 ~18 分钟 i/o timeout 慢失败且无创建前校验，payload 加
`kc-package-registry` 后同源驱动 kubeadm+containerd，20/20 Succeeded。Master add/remove 400
产品缺口复证（P0 行 5）；worker add/remove v2 operation 正向首证。终态：测试集群删除、
3 节点 Healthy 无集群标签、共享 Registry 未动（sha 守卫）、/var/lib/kc-etcd 为重 deploy 后
全新、/tmp/r17 与 /root/r17-*（证据/备份）及 dev-3 备份全删。证据原存 dev-2
/root/r17-evidence 与 /root/r17-backup（终态清理删除，逐字结论转录本节、R7 报告 §12.13 与
checklist/plan）。

**R18（2026-09-24，rc.9 `5e4cfb4b`）**：两项 P0 修复与真机闭环（用户批准实施；arm64 继续排除）。
①Fix A（P0 行 10，checklist 2.1-33）：`createClusterCheck` 对离线+空 `imageRegistry` 创建前 400
拒绝（错误文案含 registry.k8s.io 不可达解释与 Registry 对象名要求）——真实 POST **48ms** 400、
dryRun 同 400、零对象，对照组带 `kc-package-registry` dryRun 200；②Fix B（P0 行 5，
checklist 2.2-03/09/10）：master add/remove 完整实现——r18-cluster（双 master，API 创建因 CLI
守卫拒绝偶数 master）Ready 后 add dev-2（8/8 OperationTasks：getJoinCommand 在存活 master 执行
`kubeadm token create --print-join-command`+`upload-certs` 刷新证书密钥、手工拼 control-plane
join 命令、renderMasterJoinConfig `controlPlane=true`、joinNode、waitForAddedNodesReady），
etcd 3 成员 started、dev-2 Ready control-plane；remove dev-2（13/13 OperationTasks：
removeEtcdMember 在存活 master 按 `https://<ip>:2380` 匹配成员 ID 显式摘除、drainNode、
kubeadmReset、removeEtcdDataDir、clearIPVS/removeDummyInterface/clearVIPDomain、卸载链），
etcd 回 2 成员、离开节点 /etc/kubernetes 与 /var/lib/etcd 清空、kubelet/containerd inactive；
负向 remove 1 of 2 → 400 `invalid nodes topology: ... would break etcd quorum`（直调+
`?dryRun=true` 均拒，零副作用）。发布链：本地构建（gitTreeState=clean+KC_SOURCE_REVISION）
→ wrapper 经 dev-2 发布 `v2.0.3-rc.9`（5003 仅增 tag，13 个 v2.0* tag）→ `kcctl upgrade all
--manifest` 三机 6 槽位 rc.9（~66s，doctor 25/25）。边界标注：0 worker 拓扑下 worker 侧
lvscare refreshLvsCare 按设计不生成步骤、未真机覆盖（单测覆盖 joining/leaving 过滤）；升级
中 WARN skip invalid OCI tag v2.0.3-rc.5（该 tag MANIFEST_UNKNOWN，非阻塞）。运维失误记录：
升级后误用无版本 ldflags 的本地二进制覆盖 /usr/local/bin/kcctl，已从 bootstrap 包缓存恢复
官方 rc.9（version+doctor 复验）。终态：r18-cluster 删除、3 节点 Healthy 无集群标签、
doctor 25/25、共享 Registry 仅增 rc.9 tag、/var/lib/kc-etcd 未动、API 客户端证书与全部
/tmp 临时产物即用即删（dev-2 与 Mac 侧）。证据逐字转录 R7 报告 §12.14 与 checklist/gaps/plan。

**R19（2026-09-24，rc.10 `c356fbaf`）**：用户批准"docker 移除，如果还在保留的话就处理了……
其他的问题也按顺序处理"。①**Docker CRI 废弃入口移除**（commit `018a59fd`）：CLI
`allowedCRI` 仅 containerd、`--cri docker` 与服务端 createClusterCheck 均 400 显式拒绝
（"Docker CRI is not supported, use containerd"，docker 集群不再漏到 operation build 才 500）；
`cri/docker.go` 全文件与注册/分支删除；保留 `removeDockershimDataDir` 通用卸载清理与
`kcctl registry` 独立命令；CLI/服务端负向单测补齐。**legacy 迁移工具删除**：
`migrate-legacy-packages-to-oci.sh` 删除，docs/oci-delivery.md §7 改"已移除"说明（本表
已废弃残留表两行标注）。②**console/kcctl 组件升级实现**（commit `c356fbaf`，B1 step 2，
见 P0 行 4）。③**rc.10 发布与升级**：Mac 本地构建（gitTreeState=clean，
revision=c356fbafdb7009cfa67f06b19a3a14ca8c03ca06）→ wrapper 经 dev-2 发布（5003 仅增 tag，
14 个 v2.0* tag）→ manifest 顶层 index digest `sha256:2845d0da…`+console artifact
（bootstrap/console:v1.6.0 `sha256:084c4ead…`，sourceRevision=e9e95f4 事实值）→
`upgrade all` 12 槽位 ~106s 全成功、doctor 25/25；`upgrade kcctl` 全 skip（幂等）、
`upgrade console` 幂等重装（hash 前后一致）；N1/N2/N3 负向（缺制品拒/错 digest 拒零触碰/
digest 不符+revision label 匹配警告放行）。**附带修复**：dev-3/dev-4 kcctl 残留漂移
（`c27e61c0`，R18 无版本二进制事件遗留）经 kcctl 槽位统一至 rc.10 官方构建。
④**P0 行 1 故障窗口**（见行 1，含"kcctl 客户端单地址无 failover"发现与窗口期 console
caddy 摘坏上游证据）。arm64 无环境不验证（用户确认，理论无碍）。终态：三节点 kc-server
active、console 全 200、doctor 25/25、无集群；dev-2/Mac /tmp/kc-r19-publish 与 watch
临时产物全删；共享 Registry 与 /var/lib/kc-etcd 未动。证据转录 R7 报告 §12.15。

**R20（2026-09-24，rc.11 `9a1dbb75`）**：用户指令"继续吧，处理全部遗留问题"——B1 最后
两项余量收口（见 P0 行 4）。①**rc.11 发布**：Mac 本地构建（revision=9a1dbb756326…、
tree=clean，文档回填 commit 后构建）→ wrapper 经 dev-2 发布（5003 仅增 tag，15 个 v2.0*
tag）→ manifest（sha256=`545c307c…`）顶层 index digest `sha256:c28ebeed…`（stdout
`202062f7…` 仍是子 manifest）。**manifest 布局教训**：`PackageRepositoryPrefix` =
`kubeclipper/packages`——target 必须写全前缀（`kubeclipper/packages/bootstrap/kubeclipper:v2.0.3-rc.11`）、
registries.package 只写 host，首版 `bootstrap/kubeclipper:tag` 被
ParsePackageRepository 拒绝（preflight 拒绝零触碰）；`--dry-run` flag 不存在无法干跑。
②**故障注入矩阵四场景**（T-A/T-C/T-D/T-B，iptables 只阻断发起机探测路径、restore 仅恢复
backup 内旧文件故注入不破坏 restore，详见行 4 与 R7 报告 §12.16）：健康探测失败自动恢复、
kcctl 替换后探针失败 restoreKcctl、console 探针失败 restoreConsole（恢复逐字节一致）、
中断后重跑 skip+补齐。**预跑插曲**：60s 超时的预跑意外进入真实升级把 dev-2/3 server 升至
rc.11（客户端被杀但节点上 SSH 步骤序列执行完成——本身即是中断韧性证据），T-A 注入器触发
条件相应改为 dev-4 staging backup 文件出现。③**1.3-09 `--version` 正向下载+checksum**：
本地 GitHub 镜像法（自签 CA 进系统信任+/etc/hosts+443 HTTPS，URL 同构），`--version v2.0.3`
全链路 rc=0（9 槽位 skip，console 无 revision 恒重装），`.sha256` 篡改报 checksum mismatch
拒绝；镜像拆除（pkill 自匹配教训：模式含命令字面量会杀掉承载 shell，exit 255）。
终态：三节点全 rc.11（server=00c6ff30/agent=b93afc0a/kcctl=a847bf6f）、服务全 active、
healthz ok、console 全 200、doctor 25/25、staging 三节点清理；iptables 规则清零、hosts/CA
恢复原状；共享 Registry 与 /var/lib/kc-etcd 未动。证据转录 R7 报告 §12.16。

**R21（2026-09-25，rc.11 平台）**：用户指令"好，按计划执行吧"——checklist ⚠️/❌ 项集中收口
（A/B/C/D/E 五组）。闭环 14 项（1.2-04、1.2-06、1.3-04、1.3-06、2.1-12、2.1-21、2.1-22、
2.1-27、2.1-29、2.2-06、2.2-07、2.2-11、2.2-12、2.2-13、2.6-04，另 1.3-11 部分闭环），维持 4 项
（1.3-11、1.3-12、2.2-04、2.6-11、2.3-02/06/07——均附缺陷/缺口定性）。**新发现 8 项**：
①**P1 升级操作派发死锁**：`cluster upgrade` 提交时 agent 离线 28s 内 → operation 永久 Pending
（任务不派发、agent 恢复 30min+ 不补派、cancel/retry 均拒绝 Pending、须重启 kc-server 才能
取消；agent 在线时新建的 upgrade op 同样 Pending——集群卡 Upgrading 无恢复入口）；
②**P1 已删集群操作无限泄漏**：clustercontroller CRI-registry 对账（controller.go:568
registriesEqual spec≠status → 建 InstallComponents op → status 更新失败 → 重入）对已删集群
持续创建孤儿 Pending 操作至 16,595 个；operations API 无 DELETE 路由无法清理，仅等 etcd 重建；
泄漏在对象转 Terminating 后停止（对象曾僵尸 Running 且无 deletionTimestamp）；
③**P1 1M 单 master 默认 taint 死锁**：CLI 给 master 打遗留 master taint + kubeadm 打
control-plane taint，v1.36 coredns 只容忍后者 → coredns 无法调度 → Health 空转至 90min deadline
（--untaint-master 3.5min 成功）；④join 无 sudo 用户 → 无限密码提示循环（1.5GB 日志）；
⑤非 root+sudo join → /tmp/etc mkdir 失败（步骤不带 sudo）；⑥agent 跨节点读未隔离
（GET /nodes/{name} RBAC 未按 resourceNames 限定）；⑦--initial-password flag 被 config 解析
覆盖失效；⑧deploy-config 带 authentication 段 → deploy panic（Validate 空指针）。
**正向发现**：drain 集群占用保护、join 防重入、deploy 重写 config 自动剔除 initialPassword。
**2.1-29 缺口确认**：kubeadm 1.36 preflight 无磁盘检查（50MB tmpfs 实证 init 通过）——磁盘耗尽
在运行时以 DiskPressure Evicted+Health 空转呈现（全链路捕获）；时间同步无检查；主机名由 OS 层防护。
证据逐字转录 R7 报告 §12.17。

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
| Docker CRI（`6-12`） | R19 已执行移除：CLI `allowedCRI` 仅 containerd、`--cri docker` 显式拒绝("Docker CRI is not supported")、服务端 `AllowedCRIType` 仅 containerd 且 createClusterCheck 增加 CRI 类型显式 400、`cri/docker.go` 与 Docker 注册/分支删除；保留 `removeDockershimDataDir` 通用卸载清理与 `kcctl registry` 独立命令；CLI/服务端负向单测补齐 | 已按建议执行：废弃入口删除，负向门禁保留，不安排 Docker E2E |
| legacy static server/tar downloader | 当前正式交付路径已经移除，旧节点不支持原地混用 | 不恢复兼容路径；只验证全新部署不依赖旧服务，以及旧节点必须清理后重新 deploy/join 的边界 |
| `nfs-provisioner` | 已退休，由 `nfs-csi` 替代 | 不保留功能 Case；只做默认策略、资源清单和 Console 不再暴露旧组件的静态门禁 |
| legacy package 迁移工具 | R19 已删除 `migrate-legacy-packages-to-oci.sh` 脚本，docs/oci-delivery.md 引用改为“已移除”并指向 `publish-oci.sh` | 已按建议执行：脚本与说明删除，不保留长期兼容或迁移流程；不计入核心回归 |

## 本轮证据要求

每个完成项至少保存：Git revision、主机与拓扑、物料来源、组件版本、命令或请求、Operation ID、
关键日志、最终对象状态、必要的 digest/证书/网络证据，以及清理结果。没有这些信息时只能回填 ⚠️。
