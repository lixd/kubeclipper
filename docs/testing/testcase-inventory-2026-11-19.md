# KubeClipper 测试用例分类目录

在本页按功能找编号、按优先级选用例、按拓扑准备环境。具体执行步骤和证据要求见[执行规格](release-case-specs.md)，过去的实测结果见[功能台账](core-feature-checklist.md)。

## 优先级

| 级别 | 判断依据 | 何时执行 |
|---|---|---|
| P0 | 核心生命周期会中断，或可能破坏数据、节点隔离、权限、制品完整性。 | 发布前跑完本次适用的 P0。日常先跑 [AIO 主路径和快速契约](#automation)，再按改动加跑专项。 |
| P1 | 常用功能、可选模式或边界场景会回归。 | 全量回归及相关改动时执行。发布明确承诺的能力也列入该次验收。 |
| P2 | 输出格式、辅助入口、少用配置或退役能力的负向断言。 | 随相关改动或全量回归跑快速契约。实机专项复用已有集群或按变更选择。 |

优先级按业务影响定，执行成本按入口和拓扑定。HTTP/HTTPS、S3、国内镜像渠道、原生 ARM64、HA 等用发布范围和变体筛选。承诺范围内缺主机或依赖记 `BLOCKED`，范围外记 `N/A` 并注明原因。优先级、自动化实现进度和本轮运行结果分别判断。

| 功能域 | 总计 | P0 | P1 | P2 | 合并 / 退役 |
|---|---:|---:|---:|---:|---:|
| [安装交付与发布物料](#安装交付与发布物料) | 37 | 22 | 10 | 4 | 1 |
| [KC 平台部署与运维](#kc-平台部署与运维) | 18 | 12 | 3 | 3 | 0 |
| [Kubernetes 创建与网络](#kubernetes-创建与网络) | 28 | 13 | 12 | 3 | 0 |
| [节点管理与拓扑变更](#节点管理与拓扑变更) | 11 | 9 | 2 | 0 | 0 |
| [Kubernetes 升级与证书](#kubernetes-升级与证书) | 6 | 6 | 0 | 0 | 0 |
| [备份恢复与插件存储](#备份恢复与插件存储) | 13 | 4 | 6 | 1 | 2 |
| [集群删除与节点复用](#集群删除与节点复用) | 6 | 6 | 0 | 0 | 0 |
| [Operation 与任务可靠性](#operation-与任务可靠性) | 16 | 10 | 6 | 0 | 0 |
| [认证、授权与集群访问](#认证授权与集群访问) | 12 | 6 | 6 | 0 | 0 |
| [平台管理、Console 与辅助命令](#平台管理console-与辅助命令) | 10 | 1 | 5 | 4 | 0 |
| **合计** | **157** | **89** | **50** | **15** | **3** |

现行目录有 **154 项有效用例**，另列 1 个已合并编号和 2 个退役编号供查历史。旧编号见[用例归并记录](testcase-review-2026-11-19.md#case-dedup)。同一次操作可回填多个用例的独立断言。日常回归和需要额外环境的场景见[执行规格](release-case-specs.md#每次改动怎么跑)。

<a id="topology-matrix"></a>

## 拓扑与组合矩阵

`M` 指 Kubernetes 控制面节点（Master），`W` 指独立 Worker。AIO 是一台 Master 也承载工作负载，对应 `1M0W` 加可调度配置。“Submaster”对应同一集群后加入的控制面成员。验收时分别识别第一台初始化控制面、后续控制面、独立 Worker 的步骤和实际角色。KC 平台的 Server、平台 etcd、Agent 是另一层拓扑，即使和 K8s 混部也单独验收。[控制面初始化与加入代码](../../pkg/scheme/core/v1/k8s/kubeadm_step.go)

每个用例编号对应一个功能及明确的通过判据。OS、CPU 架构、Worker 数量、下载渠道和配置模式可以作为同一用例的执行变体。按编号和实际变体记录结果。

| Kubernetes 场景 | 级别 / 最少 K8s 节点 | 重点检查 | 关联编号 |
|---|---|---|---|
| AIO（1M0W，可调度） | P0 · 1 台 | Master 可调度测试业务。系统、Pod/Service/DNS 就绪。单节点升级必须显式确认维护停机，完成后恢复调度和业务。可复用做 FS 恢复、删除重建。 | 2.1-02、2.1-14、2.1-31、2.3-01、2.5-03、2.4-01 |
| 仅 Master（1M0W，保留污点） | P1 · 1 台 | 验证保留 Master 污点。无容忍的测试 Pod 按预期无法落到该节点，并检查调度原因。有容忍的探针另测控制面可用性。 | 2.1-14 |
| 1M1W（单控制面 + Worker） | P0 · 2 台 | 初始 Master 与 Worker 分别完成对应步骤。跨节点探针可在 Master 带容忍运行。验证双向 Pod IP、ClusterIP、DNS 和 Worker 的业务调度。 | 2.1-24、2.1-18、2.1-31 |
| 1M2W（双 Worker） | P0 专项 · 3 台 | 从 1M1W 加入第二个 Worker。将副本分布在两个 Worker，验移除时的驱逐与接管、逐 Worker 升级和业务连续性。涉及批量加入或多目标 Task 时再冷建 1M2W。 | 2.2-01、2.2-02、2.2-08、2.3-01。冷建专项用 2.1-24 |
| 3M0W（只有控制面） | P0 · 3 台 | 第一台 init，后两台走控制面 join。核对三套控制面和 etcd 成员。一次停一个成员，从存活端点写读，恢复后核对成员及数据。 | 2.1-03 |
| 3M1W（多控制面 + Worker） | 涉及 HA Worker/VIP 时 P0 · 4 台 | 冷建群验证两种 join。停一台 Master 时从 Worker 的实际 API 入口探测和写读，检查 VIP 后端。另做 Master 增删，检查 etcd 成员和 Worker 入口收敛。 | 2.1-31、2.2-09、2.2-10 |
| 3M2W（多控制面 + 双 Worker） | P1 扩展 · 5 台 | 涉及 HA 与双 Worker 的组合路径时补测，复用控制面入口、跨 Worker 流量、PDB 与逐节点升级探针。 | 2.1-31、2.3-01 |
| 拓扑变更中的 2 Master | P0 拓扑变更 · 3–4 台 | 分别验证 3→2 的正常成员移除、普通移除后只剩 1 台时拒绝，以及 Worker ↔ Master 转换。逐步记录角色、etcd 成员、计划和业务。 | 2.2-04、2.2-09、2.2-10 |
| 无 Master、节点重复或已占用 | P0 准入 · 可先跑 Go/API 契约 | 以合法请求为对照，分别测试只有 Worker、同节点同时选作 Master 和 Worker、节点属于另一集群。拒绝时没有新增对象、Operation 或占用。 | 2.1-26 |
| 两个独立集群 | P0 隔离 / P1 并发容量 · 至少 2 台 | 用两个 AIO 或 1M1W + AIO，节点集合互不重叠。A 的控制操作与 B 的写读/备份并行。检查 A 的互斥、B 的持续可用及无副作用。 | 5-08、5-13、2.6-11、7-03 |

`2.1-03` 的原发布基线是 3M0W。3M1W 和 3M2W 是补充变体，独立登记结论。有 3 台时可串行换拓扑运行 AIO、1M1W、1M2W、3M0W 和 2+1 双集群。Worker 访问 HA API 需要额外节点。两个 Master 可用于验证成员变更，单点故障时两个成员中只剩一个，达不到多数派。[历史范围决定](core-feature-checklist.md#当前测试范围调整2026-10-13用户确认) · [拓扑变更实现](../../pkg/clusteroperation/node.go)

| 其他变化轴 | 基线和扩展组合 |
|---|---|
| KC 平台拓扑 | 先跑单 Server 与 Agent 混部。补单 Server + 独立 Agent、单 Server + 多 Agent、三 Server 及三个**平台 etcd** 成员。单点故障分别在平台 API 和 Kubernetes API 上取证。 |
| 版本 / CRI / CNI | 实装当前策略中的 K8s、containerd、Calico 三组发布组合。复用 1.35.8 的 1M1W 覆盖旧 CRI/CNI 和升级拒绝。在 1.36.4 的升级基线和 1.37.0 的交付新装上取其余版本证据。版本外、无匹配或不允许的组合由准入测试覆盖。[默认策略](../../pkg/delivery/apis/default_policy.go) · [物料定义](../../packaging/resources.yaml) |
| 交付渠道 / 缓存 | `1.1-01` 按 global 和 `KCZONE=cn` 分别执行在线安装。另跑真实离线交付，使用冷缓存且无公网出口。私有 HTTP、私有 CA 和认证按承诺验证。记录各变体的下载源和拉取 digest。 |
| 网络 / 节点角色 | 默认 VXLAN 先跑 1M1W。IPVS/iptables、IPIP/BGP、cross-subnet、多网卡和显式地址探测在真正能区分行为的环境跑。拓扑变化、网络模式和 OS 不做全排列。 |
| OS / 架构 | `6-09` 在声明支持的每个原生 OS/架构组合上跑同一套发布生命周期，首批 Ubuntu 24.04/amd64。`6-06` 验各架构的制品和 manifest/index。 |
| 入口 / 存储 / 身份 | API、kcctl、Console 用同一共享业务场景时，分开记录各入口的实际请求与结果。FS/S3 验 etcd 恢复，NFS 验 RWX。管理员、只读用户、匿名、Agent 使用独立身份。 |

<a id="case-catalog"></a>

## 逐项用例

“执行层”是此项应使用的自动化入口。Go 契约覆盖校验和状态机，Go E2E/API/CLI 访问候选平台，SSH/kubectl 读取实际环境结果，Cypress 负责浏览器交互，发布 CI 核验正式产物。同一条可同时需要多层证据。各编号的 Ginkgo 验收能力与环境要求见 [Go + Ginkgo 覆盖评估](testcase-ginkgo-coverage.md)。

### 安装交付与发布物料

[安装、OCI 及候选验收](release-case-specs.md#delivery)

#### 安装来源与私有仓库

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 1.1-01 | P0 | 在线安装与渠道选择 | 安装脚本从选定渠道获取并校验 CLI，部署、join、完成 1M1W 建群与跨节点业务，对照仓库及实拉 digest | 默认 global/GHCR、`KCZONE=cn` 的 OSS/国内 Registry。fork 和正式入口分别记录 | CLI + Go E2E |
| 1.1-02 | P1 | 在线导入再部署 | 用 `registry import` 同步到本地仓库，部署节点实际消费同步目标 digest | 在线源 → 内网 Registry | CLI + Go E2E |
| 1.1-03 | P0 | 离线包生成与断网交付 | 用 `registry export` 生成全量包和 Registry 自举小包。校验、转运、自举及导入后，在冷缓存且公网断开的 1M1W 部署、join、建群并验证内网来源与跨节点业务 | 保留内网管理链路。校验包内索引与声明架构 | CLI + Go E2E + SSH |
| 1.1-04 | P0 | 私有 HTTP 仓库 | deploy/join/Agent 按显式 HTTP 前缀实际获取物料 | 私有 HTTP Registry | Go E2E + 仓库记录 |
| 1.1-05 | P0 | 私有 HTTPS 与自签 CA | 指定私有 CA 完成真实消费，错误 CA 明确失败 | 私有 HTTPS Registry | Go E2E + 仓库记录 |
| 1.1-06 | P0 | Package Registry 认证 | 正确凭据消费成功，错误凭据被拒，恢复凭据后可重试 | 认证 Registry。deploy/join/Agent | Go E2E + 仓库记录 |
| 1.1-07 | P1 | 导入与重复导入 | 首次导入同步目标物料。同内容再次导入复用既有 manifest 和 layer，统计与对象一致；无效参数及同步错误返回非零 | 在线 `--manifest` 或离线 `--bundle`。冷启动的隔离目标 Registry | CLI + 仓库记录 |
| 1.1-09 | P0 | 仓库不可达与物料缺失 | 不可达的地址和缺失的包分别报可定位错误。核对真实访问来源与操作状态，无意外回退 | 可控仓库。逐个改变变量 | Go 契约 + Go E2E |
| 1.1-10 | 合并 | 公共 CA 信任 | 作为 global/GHCR 实际 HTTPS 访问的一项断言 | 同 1.1-01 | 并入 1.1-01 |

#### OCI 计划、缓存与仓库工具

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 2.6-01 | P0 | 创建时固定 packagePlan | 可装组合解析成具体平台及 digest，保存到该集群 `status.packagePlan` | 代表版本。AIO 或 1M1W | Go 契约 + Go E2E |
| 2.6-02 | P0 | packagePlan 数据边界 | 以含认证配置的仓库解析，plan 只存引用、摘要等必要字段，敏感内容不落计划 | 认证 Registry | Go 契约 + API |
| 2.6-03 | P0 | Agent 冷拉取并执行 | 冷缓存按固定 digest 拉取、校验、解包、执行本轮 Task | 新 Agent + 专用 Registry | Go E2E + SSH |
| 2.6-04 | P1 | Agent 缓存命中 | 同 digest 后续任务实际命中校验后的缓存，仓库无重复 blob 下载 | 同节点、同制品再次执行 | Go 契约 + 仓库记录 |
| 2.6-05 | P0 | 缓存损坏和 digest 校验 | 内容不符不能进入执行，来源恢复后重新校验并收敛 | 一次性缓存目录、可控仓库 | Go 契约 + Go E2E |
| 2.6-06 | P1 | 仓库暂不可达 | 已校验的完整本地物料可消费，冷缓存明确失败，恢复连接后重试 | 暖/冷缓存及断网 | Go 契约 + Go E2E |
| 2.6-07 | P0 | 仓库配置优先级及权限 | 显式/已保存/zone 来源选取正确，持久化配置权限与输出脱敏符合规格 | 不同配置指向可区分的仓库 | Go 契约 + CLI + SSH |
| 2.6-08 | P1 | 默认 Delivery Policy | 初始化得到与候选物料一致的版本、槽位和默认项 | 全新 KC 平台 | Go 契约 + API |
| 2.6-09 | P1 | 自定义 Delivery Policy | 新请求遵守白名单，已有 plan 保持固定，恢复配置后可正常准入 | 测试平台 + 备份原策略 | API |
| 2.6-10 | P0 | 策略缺失槽位或 repository | 无法解析的请求被明确拦截，未创建可执行的错误计划 | 允许组合做对照 | Go 契约 + API |
| 2.6-11 | P0 | bootstrap / extension 隔离 | 执行平台交付或 A 的 extension 时，既有集群的 plan 不漂移。扩展按原 digest 安装 | 两个独立集群 | Go E2E |
| 3-19 | P1 | 局部 Registry 生命周期 | deploy、list 后可拉取。删除指定 tag 后同仓库其他对象可用，clean 只清本轮服务和目录 | 专用部署目标 | CLI + SSH |
| 3-20 | P2 | `resource list` | 候选仓库版本、平台和过滤结果一致 | 固定 inventory 和轻量 Registry 夹具 | Go 契约 + CLI |
| 3-21 | P2 | `resource inspect` | 返回的制品详情及 digest 匹配仓库，缺失项退出非零 | 固定 inventory 和轻量 Registry 夹具 | Go 契约 + CLI |
| 3-22 | P1 | `resource refresh` | 真实新增制品后刷新，证明读取新 metadata | 可写隔离 Registry | CLI |
| 3-23 | P1 | `delivery-policy template/get/apply/diff` | 模板可读取、diff 不写、apply 改变新请求实际解析结果 | 测试策略作用域 | CLI + API |
| 3-24 | P1 | `delivery-policy validate` | 合法策略接受，错误槽位、白名单或默认值被拒且不写状态 | 合法与非法本地策略 | Go 契约 + CLI |

#### 发布物料与发布门禁

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 6-01 | P0 | 支持策略和物料一致 | 支持的 K8s/CRI/CNI、OS/arch 均有匹配资产，公开说明与候选策略一致 | 当前发布组合 | 发布 CI |
| 6-02 | P0 | 启用的 OCI 制品完整 | 清单中 bootstrap、K8s、CRI、CNI、extension 和启用插件逐项可解析和拉取 | 候选仓库 | 发布 CI |
| 6-03 | P0 | ReleaseManifest 内容契约 | 按物料类型验证 package、启用的 chart、runtime image 和 bootstrap 条目 | 候选 manifest | Go 契约 + 发布 CI |
| 6-04 | P0 | 制品来源和构建身份 | 逐件对照 digest、源 revision、版本、运行中的二进制和 Console 资源 | 正式候选制品 | 发布 CI + Go E2E |
| 6-05 | P0 | 制品不可变性 | 相同 tag/digest 重发可复用，冲突 digest 发布失败且原内容可拉 | 专用 Registry | 发布 CI |
| 6-06 | P0 | 多架构索引和过滤 | index 子 manifest、层、选择结果一致。不可用 arch 明确拒绝 | amd64/arm64 物料 | Go 契约 + 发布 CI |
| 6-07 | P0 | Registry sync 消费 | 目标 digest 可拉，真实 Agent 从目标消费并执行 | 独立目标 Registry | 发布 CI + Go E2E |
| 6-08 | P0 | qualification 与 stable gate | 候选 SHA、manifest 原始字节和 acceptance 精确匹配，误绑拒绝 | 候选发布工作流 | 发布 CI |
| 6-09 | P0 | 发布制品实机生命周期 | 用发布制品部署 KC、建群、验证网络与业务、执行适用升级并正常删除。记录实测 OS/架构及实际制品 | 每个承诺的原生 OS/架构组合。首批 Ubuntu 24.04/amd64 | Go E2E + SSH |
| 6-12 | P2 | Docker CRI 退役契约 | 旧 CLI 参数和 API 输入有明确拒绝且零变更 | 准入对照 | Go 契约 + CLI |
| 6-13 | P2 | 旧安装入口与 nfs-provisioner 退役契约 | 当前帮助、Console 可选项与发布物料不再暴露退役入口 | 正式包、对应 Console | 发布 CI + 浏览器契约 |

### KC 平台部署与运维

[平台部署、HA、升级和平台清理步骤](release-case-specs.md#platform-upgrade)

#### 安装拓扑与 Agent 接入

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 1.2-02 | P0 | Server 与 Agent/K8s 混部 | 平台与宿主 Kubernetes 同时健康，端口和数据目录互不覆盖 | 混部节点。AIO | Go E2E + SSH |
| 1.2-03 | P0 | KC 平台 HA 与单点故障恢复 | 从各入口写读并核对三成员。逐个停启 Server/平台 etcd（含 leader），剩余入口继续写读，重启后恢复 3/3 且数据一致 | 3 KC Server + 3 平台 etcd | Go E2E + SSH |
| 1.2-04 | P0 | Console 发布组件部署 | 访问正确 revision 的静态资源，浏览器完成登录并访问对应 API | 随平台部署 | Cypress |
| 1.2-05 | P0 | 单 Server 平台部署与任务分发 | 用候选 `kcctl deploy` 核对参数、退出码与实际服务布局。平台 etcd 可写读，每个 Agent Ready、身份正确并完成对应任务 | 单 Server；Agent 同机、异机或多 Agent 作为部署变体 | CLI + Go E2E + SSH |
| 1.3-01 | P0 | 部署预检 | 不可达、缺工具或缺包时标明失败项。修复后同目标可部署 | 一次性目标机 | Go 契约 + CLI |
| 1.3-02 | P1 | 平台 etcd 冷启动竞态 | 从冷启至写读成功，记录启动等待和瞬时故障收敛 | 新平台 etcd 数据目录 | Go E2E + SSH |
| 1.3-05 | P0 | 独立 Agent 接入（`kcctl join`） | 从 CLI 注册一次，验证证书、部署配置、新节点 Ready 和任务执行，其他节点保持原状态 | 已有 Server + 新 Agent | CLI + Go E2E |
| 1.3-11 | P1 | SSH 认证与 sudo | key、密码、受支持的非 root sudo 及自定义端口均用真实连接校验 | 专用 SSH 目标 | CLI + SSH |
| 1.3-12 | P0 | 初始管理员和配置 | 指定初始口令能登录，错误口令拒绝。JWT 配置有效且对外脱敏 | 干净 KC 平台 | Go 契约 + Go E2E |

#### 平台升级、诊断与清理

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 1.3-03 | P0 | 平台清理并重新部署 | 从 `kcctl clean --all` 核对目标、退出码和服务/数据目录清理。二次部署可登录、join 和建群 | 专用平台 | CLI + Go E2E + SSH |
| 1.3-04 | P0 | 直接重复 deploy | 拒绝直接重复覆盖，已有服务和数据可读 | 已健康平台 | CLI + SSH |
| 1.3-06 | P0 | API 不可达时按部署清单 clean | 按指定部署配置完成强制清理，可再部署且同机其他服务未受影响 | 一次性平台。留存原清单 | CLI + SSH |
| 1.3-08 | P1 | `kcctl doctor` 健康诊断 | 实际探测 API、etcd、Agent 和 SSH。健康返回 0，单项故障可定位且返回非零 | 健康与单项故障平台 | CLI + SSH |
| 1.3-09 | P0 | 平台整体升级 | 从真实 `kcctl upgrade all` 升级。在线取并校验清单，离线断公网。按实际组件版本、顺序及原有业务、配置、用户和备份判断 | 有业务的旧 OCI 平台。在线 `--version` 或离线 `--manifest` | CLI + Go E2E |
| 1.3-10 | P0 | 平台组件独立升级与回滚 | 分别调用 `kcctl upgrade server/agent/console/kcctl`，仅目标组件变化。新 Agent 与旧 Server 可工作，健康检查失败时验证回滚 | 旧 OCI 平台。按目标组件和失败回滚取变体 | CLI + Go E2E + SSH + 浏览器 |
| 3-26 | P2 | `kcctl status` | 列出的平台组件、主机和状态与实机一致 | 一次健康/异常探测 | CLI |
| 3-27 | P2 | `kcctl deploy config` | 输出可解析默认模板，配置样例无运行凭据 | 无集群依赖 | CLI |
| 3-28 | P2 | `kcctl version` | 显示的是本次正在运行的 CLI 构建版本 | 候选 CLI | CLI |

### Kubernetes 创建与网络

[建群、数据面和 Registry 步骤](release-case-specs.md#network-storage)

#### 建群拓扑、准入与生命周期

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 2.1-02 | P0 | AIO 建群 | 单节点承载控制面和测试工作负载，API、系统组件和业务探针可用 | AIO | Go E2E + kubectl |
| 2.1-03 | P0 | 纯控制面 HA 建群 | 区分第一个 Master init 和两个后续 Master join，验 etcd 三成员、单点故障后的实际写读和恢复 | 3M0W | Go E2E + kubectl + SSH |
| 2.1-04 | P0 | 受支持的 K8s、CRI、CNI 组合实装 | 实际 Server/kubelet、containerd 和 Calico 版本与本轮策略组合一致，容器可启动，节点 Ready，跨节点数据面验证后正常删除 | 每个承诺组合选代表拓扑。跨节点用 1M1W 或多节点的已有生命周期 | Go E2E + kubectl + SSH |
| 2.1-10 | P0 | 建群版本与组件组合准入 | 未支持版本、组合不匹配分别拒绝，合法组合可建 | 支持策略可控的测试平台 | Go 契约 + API |
| 2.1-24 | P0 | 单控制面与独立 Worker 建群 | 冷建并绑定本次 Operation。各节点身份、角色和 Ready 正确，每个 Worker 都能调度并访问 Service | 常规冷建 1M1W。涉及批量 Worker 加入或多目标 Task 时加跑 1M2W 冷建 | Go E2E + kubectl |
| 2.1-26 | P0 | 节点占用及拓扑准入 | 无 Master、重复角色、已被其他集群占用分别拒绝，无对象/Operation/占用变更 | 合法 1M1W 对照。占用场景需另一集群 | Go 契约 + API |
| 2.1-28 | P0 | CIDR 校验 | 分别测试非法、Pod/Service 重叠及与主机网段冲突，拒绝无副作用 | 合法网段对照 | Go 契约 + API |
| 2.1-29 | P1 | K8s 节点预检 | 用可复现的占用端口、磁盘、时间或 hostname 问题定位到节点和步骤，修复后可继续 | 一次性 VM | Go E2E + SSH |
| 2.1-30 | P0 | 失败安装的恢复 | 中断已受理的安装，保留 Task 错误。正式 retry 后可用，或正常清理失败群 | AIO 或 1M1W | Go E2E + SSH |
| 2.1-31 | P0 | 建群后的共用健康检查 | 绑定本轮 Operation，校验实际节点、系统 Pod、Service、DNS、写读及多节点 Pod 互通 | 随每个实际建群执行。HA Worker 入口单独登记 | Go E2E + kubectl |
| 3-05 | P0 | `kcctl create cluster` | CLI 的版本、节点、镜像源和拓扑与后台 plan、运行集群一致 | 复用各建群变体 | CLI + Go E2E |

#### 网络、调度及自定义配置

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 2.1-08 | P1 | kube-proxy 模式选择与转发 | 核对请求的模式实际生效，采集相应 IPVS/iptables 规则，从另一节点实测 ClusterIP 和 DNS | 1M1W。分别选 IPVS、iptables，宿主机满足对应模式要求 | Go E2E + kubectl + SSH |
| 2.1-11 | P1 | 自定义网段与 DNS 域 | Pod/Service 获取指定网段地址，并能解析自定义域内 Service FQDN | 1M1W | Go E2E + kubectl |
| 2.1-12 | P1 | API 外部地址和证书 SAN | HTTPS 客户端从外部地址建立信任并访问正确集群，SAN、域名和端口对应 | 有外部访问路径 | API + kubectl |
| 2.1-13 | P2 | feature gates 透传 | 解析生成的 kubeadm 配置和组件参数，指定的 feature gate 到达实际消费字段 | Go 配置渲染。变更相关字段或发布 K8s 版本时实机抽测 | Go 契约 + 按需 SSH |
| 2.1-14 | P1 | Master 调度设置 | 去污点时普通 Pod 可落到 Master，保留污点时需相应容忍。分别查询调度事件 | 1M0W 两个变体 | Go E2E + kubectl |
| 2.1-15 | P2 | 仅安装 Kubernetes | 按请求跳过 CNI，记录阶段和 Pod 状态。补装匹配 CNI 后另验数据面 | 缺 CNI 的专用集群 | Go E2E + kubectl |
| 2.1-16 | P1 | 自带根 CA | 校验新叶证书由指定 CA 签发，外部实际访问通过 | 测试 CA + 证书专用环境 | Go 契约 + kubectl |
| 2.1-17 | P2 | kubeadm 预检忽略项 | kubeadm init/join 只收到配置的豁免项，未选项不受影响 | Go 配置渲染。变更命令参数时用可控预检故障的 Linux 节点抽测 | Go 契约 + 按需实机 |
| 2.1-18 | P0 | 默认 Calico VXLAN | 核对实际 Calico 配置和跨节点双向通信 | 1M1W 或 1M2W | Go E2E + kubectl |
| 2.1-19 | P1 | IPIP/BGP / cross-subnet | 匹配声明的网络模式，验证真实隧道/路由和业务。cross-subnet 两端分属不同子网 | 相应子网/路由拓扑 | Go E2E + kubectl + SSH |
| 2.1-20 | P1 | 节点地址自动探测 | 检查多网卡默认选择、interface/can-reach 的实际网卡与跨主机可达性 | 多 NIC、容器网桥或多地址节点 | Go 契约 + Go E2E + SSH |
| 3-14 | P1 | `kcctl set cluster` | 更新和置空所选外部地址，保留无关字段，非法端口拒绝 | 复用 2.1-12 | CLI + API |

#### 组件镜像和实际仓库消费

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 2.1-07 | P0 | 安装镜像与物料来源 | 校验 bootstrap、镜像和 K8s 物料实际拉取位置及 digest | 复用冷缓存离线交付 | Go E2E + 仓库记录 |
| 2.1-21 | P1 | 包仓库与集群镜像源隔离 | 用两个可区分源验证包、集群镜像与引用选择分别生效。编辑/升级受原镜像源约束 | 独立的两个 Registry 来源 | Go E2E + 仓库记录 |
| 2.1-22 | P1 | 私有 CRI Registry | 新增/修改认证配置后用未缓存镜像实拉，撤销唯一配置后新拉取按预期失败 | 运行集群 + 私有认证 Registry | Go E2E + kubectl |
| 2.1-33 | P0 | 离线 imageRegistry 引用 | 默认与显式引用解析正确，缺默认或无效引用明确拒绝 | 内网 Registry | Go 契约 + API |
| 3-07 | P1 | 平台 Registry 对象 CRUD | CLI/API 增删改对象，检查保存后的字段、查询和真实使用，非法地址被拒绝 | 独立 Registry 记录 | CLI + API |

### 节点管理与拓扑变更

[节点增删、角色与 Agent 步骤](release-case-specs.md#节点增删角色与-agent-身份j04)

#### 扩缩容与角色转换

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 2.2-01 | P0 | 扩 Worker | 新 Worker 与目标集群身份匹配，Ready、可调度、访问 Service | AIO → 1M1W 或 1M1W → 1M2W | Go E2E + kubectl |
| 2.2-02 | P0 | 移除 Worker | Ready 节点按驱逐/PDB 处理，非 Ready 节点按恢复流程收敛。清理目标主机并验证剩余业务 | 1M2W → 1M1W。另测失联 | Go E2E + kubectl + SSH |
| 2.2-04 | P0 | Master ↔ Worker 转换 | 真实角色、taint、etcd 成员、节点及 plan 对应。最后一台 Master 的降级拒绝 | 有两种角色。按步骤留足节点 | Go E2E + kubectl + SSH |
| 2.2-08 | P0 | 扩容复用原 plan | 更改隔离仓库标签或平台默认后新增 Worker，拉取仍使用已固定 digest | 1M1W → 1M2W | Go E2E + 仓库记录 |
| 2.2-09 | P0 | 增加 Master 后控制面一致 | 新成员实际运行 API Server、etcd 并加入原控制面，成员身份及配置一致，旧数据可从新端点读回 | 从已有控制面扩至 3 Master，含 2M0W → 3M0W。带 Worker 时用对应拓扑 | Go E2E + SSH |
| 2.2-10 | P0 | 移除 Master 后收敛与最小成员保护 | 移除指定 etcd 成员，普通移除后至少保留两个 Master。拒绝时成员不变。有 Worker 时验证 API 的 VIP 后端与任务 | 3M0W → 2M0W。VIP 子项用 3M1W → 2M1W。再验证 2→1 拒绝 | Go E2E + kubectl + SSH |

#### Agent 注册、状态及运维

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 2.2-05 | P1 | 禁用/启用节点 | 禁用节点不参与正常建群选择与准入，启用后可用 | KC 平台 + 空闲 Agent | Go 契约 + API |
| 2.2-06 | P0 | Agent 失联与任务恢复 | 停续约跨宽限期后 Ready 变为 Unknown，记录在途任务目标及结果。重启后同一 Agent 恢复续约和 Ready，能重新执行任务 | 有在途 Task | Go E2E + SSH |
| 2.2-07 | P0 | Agent 注销与重新注册 | `kcctl drain --agent` 验取消及占用保护，释放后可注销。重新 join 时身份和新证书匹配，旧身份不能修改新节点，其余 Agent 仍可执行任务 | 独立 Agent | CLI + Go E2E + SSH |
| 2.2-11 | P0 | Agent 注册和状态授权 | 只允许各 Agent 写自身节点/任务状态，过期更新或其他身份不能覆盖 | 两个独立 Agent 身份 | Go 契约 + API |
| 2.2-13 | P1 | Region 归属和调度约束 | 注册后 Region 标签和筛选正确；同 Region 可以建群，混合 Region 拒绝且无新 Operation 或占用变更 | 两 Region | Go 契约 + API |

### Kubernetes 升级与证书

[升级、PDB、失败重试及证书步骤](release-case-specs.md#upgrade)

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 2.3-01 | P0 | Kubernetes 实际升级与维护行为 | `kcctl cluster upgrade` 发起。AIO 显式确认 PDB 和停机后恢复。多节点先 Master 再逐台 Worker，遵守 PDB 和业务探测阈值。核对真实 API/节点版本、Operation 和数据 | AIO 与 1M2W。1.36.4→1.37.0 | CLI + Go E2E + kubectl |
| 2.3-02 | P0 | 升级失败后重试 | 在受保护节点故障后重试同一 Operation，检查恢复、终态及已成功步骤的副作用 | 有 PDB 的 1M2W | Go E2E + kubectl |
| 2.3-03 | P0 | 升级失败后重置阶段 | dry-run 无变更，正式 reset 只按契约改状态，原操作证据和真实版本保留 | 失败且无活动 Task 的专用集群 | Go 契约 + API |
| 2.3-04 | P0 | K8s 服务证书更新 | 目标节点叶证书确实更新，CA 信任和 TLS API 连通保持正确 | AIO。多控制面时看各节点 | Go E2E + kubectl + SSH |
| 2.3-06 | P0 | 升级 plan 与重试的固定 digest | 新升级解析目标版本及既有合法配置。移动可变 tag 后原 Operation retry，实际执行仍使用该操作固定的 digest | 两个候选 K8s 版本，专用仓库及失败升级 | Go 契约 + Go E2E + 仓库记录 |
| 2.3-08 | P0 | 升级准入边界 | 低版本起点、同版本、降级、跳 minor、错误拓扑开关分别拒绝且无主机变化 | 低版本群和 1.36.4。AIO/多节点 | Go 契约 + API |

### 备份恢复与插件存储

[备份、恢复及 NFS 步骤](release-case-specs.md#backup-storage)

#### 集群备份和恢复

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 2.5-01 | P0 | 备份点与快照实物 | 真实 FS 文件或 S3 对象生成后，记录、大小和摘要与真实字节一致；非法类型拒绝 | FS 基线。发布范围包含 S3 时用真实 S3 或 MinIO | API + Go E2E + 后端实物查询 |
| 2.5-03 | P0 | 手动备份与恢复验收 | 写入 A→备份→改为 B 且实读→恢复指定快照后实读 A，并检查实际节点、应用、插件和 Service 可用 | FS 基线，S3 按承诺复用。有插件的多节点变体按专项 | Go E2E + kubectl |
| 2.5-04 | P0 | 删除快照和失败恢复 | 成功时删除记录和实物。后端删除失败保留关联，修复后可重试 | 本轮专用 FS 或 S3 | Go E2E + 存储查询 |
| 2.5-05 | P1 | 定时任务单次触发 | `runAt` 到点产生指定备份和 Operation，并验证下一窗口不重复 | 可控制的时间窗口 | Go 契约 + Go E2E |
| 2.5-06 | P1 | 周期备份调度 | 跨实际周期多次生成不同快照，记录父任务及触发时间 | 有备份点的专用集群 | Go E2E |
| 2.5-07 | P1 | 定时任务开关 | disable 窗口无新备份，enable 后按周期继续 | 至少两个实际触发窗口 | API + Go E2E |
| 2.5-08 | P1 | `maxBackupNum` 轮转 | 只保留约定数量的任务产物，过期实际文件消失，手工备份仍在 | 真实 FS/S3。跨多个周期 | Go E2E + 存储查询 |
| 2.5-10 | P0 | 坏快照和存储故障 | 试损坏快照、FS 不可写与 S3 凭据无效。写入或恢复失败有可定位的存储错误，不标成功 | 一次性集群和测试快照 | Go E2E + 存储查询 |
| 2.5-11 | P2 | 备份描述变更 | 更新、查询描述只改元数据，关联、快照状态、大小和摘要保持一致 | 现有快照或独立 handler 契约 | Go 契约 + API |

#### NFS 与插件

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 4-01 | P1 | NFS CSI 生命周期 | 安装及运行镜像 digest 正确，RWX PVC Bound，不同节点双向读写；用自定义 NFS 配置验证空 config 卸载后清理 | 两可调度节点 + NFS export。按配置处理改动补显式卸载 | Go E2E + kubectl + 仓库记录 |
| 4-02 | 退役 | 内置 MetalLB L2 | 保留历史编号 | 内置组件已移除 | 退役 |
| 4-03 | 退役 | 内置 MetalLB BGP | 保留历史编号 | 内置组件已移除 | 退役 |
| 4-20 | P1 | 插件失败后重试 | 制造可恢复故障，按原生流程重试，状态正确且能清理 | 专用 NFS 试验环境 | Go E2E + kubectl |

### 集群删除与节点复用

[正常删除、失败恢复及宿主机检查](release-case-specs.md#cleanup)

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 2.4-01 | P0 | 正常删除和并发同步 | 与 SyncKubeConfig 交叠后操作能收敛。资源清理、同节点重建、业务可用 | 先 AIO，再 1M1W | Go E2E + SSH |
| 2.4-02 | P0 | 删除失败安装的集群 | `InstallFailed` 下执行正常删除，主机和平台对象均收敛 | 复用安装失败 fixture | Go E2E + SSH |
| 2.4-03 | P0 | 删除失败后重试 | `TerminateFailed` 有可定位错误，恢复条件后重试不重复卸载且能收敛 | 一次受控卸载失败 | Go E2E + SSH |
| 2.4-04 | P0 | 有备份的集群删除保护 | 有备份拒绝普通删除且数据保留。删备份后允许删除 | 复用真实快照 | Go 契约 + Go E2E |
| 2.4-05 | P0 | 删除后 Operation 清理 | 目标关联 Operation/Task 及占用释放，另一集群记录和业务保持正确 | 双集群对照 | Go E2E + API |
| 3-08 | P0 | `kcctl delete cluster` | 普通删除输出已提交，由测试跟踪本次 Operation 和真实清理。force 提示、取消和宿主机清理另行核对 | 一次性集群。正常与 force 分开 | CLI + Go E2E + SSH |

### Operation 与任务可靠性

[操作控制、并发及失败恢复步骤](release-case-specs.md#operations)

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 5-01 | P0 | 核心操作使用 Operation V2 | 建群、扩缩、升级、备份、恢复关联正确的 Cluster UID、Operation 和任务结果 | 复用各生命周期 | Go E2E + API |
| 5-02 | P0 | 任务节点、步骤与失败诊断 | 用 API/日志将 Task UID、实际 IP、步骤和错误定位到本次失败目标，以成功 Task 作对照 | 多目标操作 | Go 契约 + API |
| 5-03 | P0 | Server / Agent 重启恢复 | 受控中断后，同一操作延续或如实失败。实际副作用次数与预期一致 | 分别重启两类进程的专用环境 | Go E2E + SSH |
| 5-04 | P1 | Agent Watch 中断恢复 | EOF、410 后重新 list/watch，不漏收后续任务 | 可控 watch 服务及进程重连 | Go 契约 + Go E2E |
| 5-05 | P1 | 多节点步骤屏障及输出传递 | 必需目标都完成才开始下一步。输出正确传递，缺失时可观察失败 | Go 控制器契约。涉及多目标调度或任务执行时在 1M2W 冷建补证据 | Go 契约 + 按需 Go E2E |
| 5-06 | P0 | 操作取消 | API 携正确 UID/RV 可取消受支持阶段，CLI 取消正确目标并返回正确退出码；过期/已终态拒绝且无后续副作用 | 可停在中间步骤的操作 | Go 契约 + API + CLI |
| 5-07 | P0 | 自动与人工重试 | 自动重试生成新 attempt。合法人工重试保持 Operation UID、更新 generation 且保留成功步骤 | 本轮可重试失败 | Go 契约 + Go E2E |
| 5-08 | P0 | 同集群危险操作互斥 | A 的危险步骤时间不重叠。并行时 B 可正常写读、备份 | 两个独立集群 | Go 契约 + Go E2E |
| 5-09 | P1 | 回包丢失后的持久任务终态 | 写入已提交但回包丢失时读回终态，确认没有重复副作用 | 可控存储故障。重启回合 | Go 契约 + Go E2E |
| 5-10 | P0 | 未知副作用时安全失败 | 无法确认执行结果时停在可解释的状态，重启/重试不会无条件重放危险动作 | 可控故障及已确认结果对照 | Go 契约 + Go E2E |
| 5-11 | P1 | Operation/Task 游标分页 | 同一快照翻页无重无漏，过期或非法 continue 明确报错 | 可持续新增/删除的测试对象 | Go 契约 + API |
| 5-12 | P1 | 任务日志 offset 和切换 | 多页字节游标、含多字节字符的日志和 Task 切换，结果无重复、无乱码、无串流 | 不同节点/任务的独立日志 | Go 契约 + API |
| 5-13 | P0 | 请求幂等与去重 | 手工备份同键同参收敛到同 Backup/Operation 和单份快照。异参冲突返回 409。备份已删但操作历史还在时，重放返回 410 | 可控备份过程和双集群对照 | Go 契约 + Go E2E |
| 5-14 | P0 | 任务/操作超时 | 触发 Step/Operation 超时确认原因、终态和锁释放。非法 timeout 请求无状态变更 | 可延时的专用任务 | Go 契约 + Go E2E |
| 3-11 | P0 | `kcctl operation list/describe/logs/retry` | CLI 定位本次操作和失败步骤，原生 retry 后核对 UID 与真实结果 | 复用 5-02 / 5-07 / 5-12 | CLI + API |
| 7-03 | P1 | 两个独立集群并行创建 | 实际执行窗口交叠且分别成功。按各自节点、存储和业务标记核对目标，记录实测并发数 | 两个 AIO 或更多专用节点 | Go E2E + kubectl |

### 认证、授权与集群访问

[角色、凭据、kubeconfig、终端和 Kubernetes 代理](release-case-specs.md#access)

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 4-08 | P0 | 用户与角色管理 | 用 `kcctl create/delete` 和 API 验用户与角色生命周期，启停、改密、登录记录和受保护请求均对应正确对象 | 管理员 + 临时账户 | CLI + API + Go E2E |
| 4-08b | P0 | RBAC 权限边界 | 获授权用户可操作，最小查看/无角色的用户按规则拒绝受限请求。拒绝时零副作用 | admin/只读/无角色/匿名。实际同对象 | Go 契约 + API |
| 4-08c | P1 | MFA 登录挑战 | 通过实际口令流触发 MFA。正确挑战后可登录，错码、过期及重放失败 | 测试 MFA provider。按发布范围 | Go 契约 + API |
| 4-08d | P0 | Token 生命周期 | 签发、查询、刷新、撤销和过期后，实际保护资源返回符合契约 | 独立 token 上下文 | Go 契约 + API |
| 4-12 | P0 | kubeconfig 下载及使用 | 获授权下载直连/代理配置分别实连正确集群，写读可验证。无权下载被拒 | 代表集群。两种已支持入口 | API + kubectl |
| 4-12a | P1 | 节点/集群 Web 终端 | 正确凭据下验证远端执行结果和身份，错误凭据、断线和重连收敛 | 可登录的测试节点 | WebSocket + Cypress |
| 4-12b | P1 | Pod exec | 在指定 namespace、Pod、容器实际执行，另测错误路径与拒绝 | 带多容器 Pod 的测试群 | API + kubectl |
| 4-14 | P1 | 审计事件 | 真实操作后的 event list/detail 对应相同主体、目标、动作和时间 | 一次获授权的操作 | API |
| 4-16 | P0 | 敏感数据保护 | 列表/详情/watch、配置、日志、审计的测试凭据均不泄漏，消费端可用 | 专用凭据与各类测试身份 | Go 契约 + API + 日志 |
| 4-18 | P1 | 登录限流 | 窗口内按阈值拒绝、另一账号可登录，窗口后允许恢复 | 专用限流阈值和两个账号 | Go 契约 + API |
| 7-01 | P1 | OIDC 登录 | 真实 IdP 回调生成预期身份与角色，错误回调或重放无法登录 | 启用 OIDC 的专用环境 | API + Cypress |
| 3-25 | P0 | `kcctl login` | 正确口令建立当前身份上下文，错误口令不覆盖或伪造成功 | 两个临时账号 | CLI + API |

### 平台管理、Console 与辅助命令

[平台管理与诊断](release-case-specs.md#平台管理与诊断j11j13) · [Console 子场景](release-case-specs.md#console)

| 编号 | 级别 | 测试功能 | 关键验收点 | 拓扑或前置条件 | 建议执行层 |
|---|---|---|---|---|---|
| 4-07 | P0 | Console 端到端业务 | 分别验登录/权限、建群与仓库、升级、备份恢复、失败操作、删除和集群访问，校对实际请求和后端结果 | 复用相应集群与节点。按 UI 开关 | Cypress + API |
| 4-09 | P2 | 集群模板 | 模板创建和使用有真实来源。更新不改已建对象，引用占用时删除拒绝 | 模板和引用集群 | API + Cypress |
| 4-10 | P2 | DNS domains / records | CRUD、A/AAAA 范围与非法地址。启用同步时验证 CoreDNS 配置及 Pod 读取 | 独立域名和测试群 | Go 契约 + API + kubectl |
| 4-11 | P1 | 外部 K8s 纳管 | 独立平台纳管、同步并解除。取消纳管后源集群仍可读写和运行 | 源集群 + 独立 KC 平台 | Go E2E + kubectl |
| 4-13 | P1 | 平台自省接口 | configz/status/components/componentmeta 与实际配置、服务和物料匹配 | 已启动平台 | Go 契约 + API |
| 4-15 | P1 | 平台设置 | 镜像模板认证凭据保留且展示脱敏，测试终端公钥轮换可用 | 专用平台设置 | API |
| 4-17 | P1 | 健康与指标 | healthz 对应服务状态，metrics 可解析并观测实际服务行为 | 健康和故障对照 | Go 契约 + API |
| 3-09 | P2 | `kcctl get` | 有名详情与列表格式正确，0/1/多条、selector 命中和空集合均可解析 | 测试对象 | Go 契约 + CLI |
| 3-10 | P1 | `kcctl get --watch` | 指定资源事件可解析，坏帧报错，取消时不残留会话 | 两个独立测试对象 | Go 契约 + CLI + API |
| 7-02 | P2 | Shell 补全 | bash/zsh 实际生成且通过语法检查，不支持的 shell 返回错误 | 对应 shell | CLI |

<a id="automation"></a>

## P0 自动化实施顺序

从已存在的 Go test、Ginkgo 和 Cypress 入手。先补能查出真实回归的断言，再扩到更多拓扑。下表中的编号是每条首批路径要挂接的检查点。编号还有其他适用变体时，继续按上文补齐。

| 顺序与场景 | 首批挂接 | 现有基础与应补断言 |
|---|---|---|
| 1. 准入、操作状态机和制品校验 | 2.1-10、2.1-26、2.1-28、2.3-08、2.6-05、5-06、5-07、5-08、5-10、5-13、5-14、6-01 | 复用 `make test`。把非法输入、过期操作、并发、坏包等放进 Go 表驱动或小型集成测试。已有创建校验、升级版本检查、内容校验和 Operation 控制器测试可按断言挂接。发布物料贯通到当前 [Go/发布契约 CI](../../.github/workflows/go-test-coverage.yml)。 |
| 2. AIO 主路径 | 1.2-02、1.2-05、2.1-02、3-05、2.1-31、4-12、2.5-01、2.5-03、2.4-01、3-08 | 复用 AIO 安装和现有建群/恢复 spec。加上 kubeconfig 实际使用、备份前后 marker 和正常删除同节点复用。常规改动先跑此链路。单跑 `[Fast]` 需要另验是否选择到这些断言。[AIO CI](../../.github/workflows/test-aio.yml) |
| 3. Worker 与拓扑差异 | 1.3-05、2.1-24、2.1-18、2.1-31、2.2-01、2.2-02、2.2-08 | 一个两节点 fixture 验证独立 Worker 加入和跨节点业务。将同一套 1M1W fixture 扩到 1M2W，验证调度、PDB、固定 digest 和主机清理。批量 Worker 加入或多目标 Task 变更时补 `2.1-24` 的 1M2W 冷建。专项按需跑 3M0W 和 3M1W。 |
| 4. Kubernetes 升级与失败恢复 | 2.3-01、2.3-02、2.3-06、2.3-08、5-01 | 复用升级流程，分别验 AIO 停机确认与 1M2W 驱逐、PDB 和滚动顺序。绑定 Operation UID、实际版本、业务探针和 retry 结果。 |
| 5. 权限边界与 Console | 3-25、2.2-11、4-08b、4-08d、4-12、4-16，及 4-07 的登录/建群子项 | 用 Go/API 做身份矩阵，浏览器运行真实登录、建群、进度显示。复用现有 Cypress 入口时保留原始退出码，并用 API 对照身份、创建请求和真实对象。逐步补齐 4-07 的所有适用子项。 |
| 6. 在线/离线交付与候选绑定 | 1.1-01、1.1-03、2.1-07、2.6-03、6-02、6-04、6-06、6-08、6-09 | 用现有 [qualification](../../.github/workflows/publish-oci-qualification.yml) 和制品验证。从同一候选取件，真实内网、冷缓存、单次可重复生命周期。记录离线时的出口隔离，以及 `6-09` 每个实际 OS/架构变体的证据。 |

下一轮按上表的 **编号 × 变体 × 断言** 登记自动化映射。建议每个测试引用用稳定 spec 名或测试名，例如以 `2.5-03 / aio-fs / 恢复旧值` 定位到 Go E2E，以 `4-07 / console-create / 表单拓扑` 定位到 Cypress。以下入口当前只有局部实现。[Go 套件入口](../../test/e2e/e2e_test.go) · [现有自动化缺口](release-case-specs.md#现有自动化能证明什么)

- `make test` 负责快速 Go 契约。当前 AIO CI 只选 `[Fast]`。核对实际执行的 spec 和断言后再登记。NFS 空用例、仅等待阶段的用例、未注册的 kcctl 测试均缺相应的断言证据。
- Go E2E 统一编排 API、CLI、kubectl 与 SSH，尽量共享真实环境和检查逻辑。CLI 参数/退出码单独记录，浏览器用 Cypress 验页面动作及请求。CI 负责选择候选、拓扑、超时、并发及证据归档。
- 自动化覆盖按“适用 P0 中，全部必需变体与证据层已绑定到启用的测试”的编号统计。每轮通过率另对候选结果计算。`SKIP`、`BLOCKED`、未接入的测试、旧候选结果在报告里单列。

## Markdown 与机器可读格式

| 载体 | 用途 |
|---|---|
| 本页 Markdown | 当前主目录。能在 Git 审查里直接看到用例、优先级、变体和断言变化，适合设计和讨论。 |
| YAML 清单 | 开始由 CI 按编号、拓扑和标签选择后，把 `id / domain / priority / variants / checks / test_ref` 做成结构化源，再从它生成 Markdown。每个 `test_ref` 对应实际 Go、Cypress 或发布任务，保留业务判据的文档链接。 |
| JSON 运行结果 | 由 runner 产出 `id / variant / test_ref / revision / status / evidence`，追加轮次和平台配置。现有 [R46 JSON 证据](evidence/r46/cases.json) 已按编号保存结果，可作为报表的数据入口。 |

现阶段直接维护本页。首批测试接进 CI 后，从真实运行得到每台机器、每个拓扑和用例的覆盖数据，再决定是否生成筛选报表。已有 Git、测试工具和 CI 就能开始。
