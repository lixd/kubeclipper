# KubeClipper 用例的 Go + Ginkgo 覆盖评估

以当前 OCI 工作树 `fe0b59fe`、[用例目录](testcase-inventory-2026-11-19.md#case-catalog)和[执行规格](release-case-specs.md)为准。逐条看**完整通过判据**，包括需要用真实命令发起、检查数据、观测页面或对照正式制品的子项。以下结论是实施可行性，运行结果按具体候选和变体记录。

| 主要验收方式 | P0 | P1 | P2 | 有效项 | Go + Ginkgo 能做到什么 |
| --- | ---: | ---: | ---: | ---: | --- |
| Ginkgo 主测 | 72 | 43 | 4 | 119 | 能从候选 API、CLI、Registry、宿主机或 Kubernetes 客户端取得完整证据。Ginkgo 可以调用这些真实入口。 |
| Go 契约优先 | 6 | 5 | 10 | 21 | 也可以写成 Ginkgo spec。参数、序列化、拒绝及存储边界首选 Go `testing` 单测或小型集成测试。涉及真实成功路径时引用相同候选的实机证据。 |
| 组合验收 | 4 | 2 | 0 | 6 | Ginkgo 验后端或实机，浏览器或发布 CI 提供其余必需证据。 |
| 浏览器或发布 CI 主测 | 7 | 0 | 1 | 8 | 主要判据是浏览器行为或候选制品与发布流程。Go 可读取响应、调用工具或准备环境。 |
| **合计** | **89** | **50** | **15** | **154** | 另列 1 个已合并编号和 2 个退役编号供查历史。 |

**“Ginkgo 主测”包含真实 API、CLI 和实机三种粒度。** 例如查询审计或验证 CLI 输出只要候选平台或测试仓库，建群、升级和 NFS 要真实节点。可以把同一流程的多个断言挂到多个编号，每个编号按实际通过的变体记结果。

**有 14 项建议接入浏览器或发布 CI。** 其中 6 项同时用 Ginkgo 验后端或实机。浏览器检查在 Console 仓库维护，由候选验收调用同一 Console revision 的用例。WebSocket 可以用 Go 测远端执行，终端的输入和渲染要在真实浏览器验。发布校验也能用 Go 编写，适合由发布 CI 对实际候选执行。正式发布门禁还需要同一候选的 tag、qualification 与验收记录。Ginkgo 可以调起浏览器或校验工具，完成判据仍取决于这些环节产出的证据。[Console 检查点](release-case-specs.md#console) · [候选联跑方式](e2e-automation-plan.md#console-e2e) · [发布工作流](../../.github/workflows/release.yml)

- **实机环境。** 首批执行 `6-09` 的 Ubuntu 24.04/amd64 变体。后续增加 OS 或 ARM64 环境时用同一编号和断言，在报告中记录实际变体。AIO 可用一台 Linux VM。双向网络可用 1M1W。Worker 移除后的业务接管和多 Worker 滚动升级用 1M2W，从 1M1W 扩容准备。控制面 HA 需 3M0W，HA 的 Worker/VIP 子项另配 3M1W。断网冷装、跨子网、S3/NFS 使用对应的真实网络和存储服务。首批适用环境缺失记为 `BLOCKED`。[拓扑和组合矩阵](testcase-inventory-2026-11-19.md#topology-matrix)
- **现有接入。** AIO CI 选择 `[Fast]`，当前匹配 7 条，包括继承父级标签的 Region spec。真实建群、恢复和升级在 Slow 路径，NFS spec 缺少实际断言。以下分类以需要补足的判据为准。[AIO 工作流](../../.github/workflows/test-aio.yml) · [现有 Ginkgo spec](../../test/e2e/cluster/spec.go) · [Region spec](../../test/e2e/region/region.go) · [OCI E2E 实施方案](e2e-automation-plan.md)

## 安装交付与发布物料

[对应执行规格](release-case-specs.md#delivery)

| 编号 | 优先级 | 用例 | 判定 | 理由和取证条件 |
| --- | --- | --- | --- | --- |
| `1.1-01` | P0 | 在线安装与渠道选择 | Ginkgo 主测 | 用实际候选安装脚本完成取件、CLI 摘要校验、deploy、join 及 1M1W 建群。两个渠道分别核对系统信任、物料来源与真实数据面。 |
| `1.1-02` | P1 | 在线导入再部署 | Ginkgo 主测 | 执行实际 registry import 后从独立目标部署。Registry 摘要和 Agent 拉取记录能证明用到了同步后的内容。 |
| `1.1-03` | P0 | 离线包生成与断网交付 | Ginkgo 主测 | 候选 CLI 生成并校验全量包和 Registry 自举小包，转运至离线环境并实际消费两包。冷缓存 1M1W 保持公网不可达，检查实际拉取来源和数据面。 |
| `1.1-04` | P0 | 私有 HTTP 仓库 | Ginkgo 主测 | 用候选 deploy、join 及 Agent 消费专用 HTTP Registry，直接核对其访问记录和组件来源。 |
| `1.1-05` | P0 | 私有 HTTPS 与自签 CA | Ginkgo 主测 | 在独立 HTTPS Registry 做正确私有 CA 与错误 CA 对照，从实际 TLS 会话和拉取结果判断信任。 |
| `1.1-06` | P0 | Package Registry 认证 | Ginkgo 主测 | 使用具备认证的测试 Registry，验证错误凭据失败、恢复凭据后真实消费成功。 |
| `1.1-07` | P1 | 导入与重复导入 | Ginkgo 主测 | 在线清单和离线包均走实际 CLI。对照首次复制、二次复用、manifest/layer digest 与统计，错误输入和同步失败检查退出码。 |
| `1.1-09` | P0 | 仓库不可达与物料缺失 | Ginkgo 主测 | 候选 CLI、Agent 或 API 遇到不可达地址或缺少工件时应显式失败，以请求记录确认未回退到其他仓库。错误 CA 和口令分别由 1.1-05、1.1-06 检查。 |
| `1.1-10` | 合并 | 公共 CA 信任 | 历史项 | 公共 CA 信任通过 1.1-01 的真实 HTTPS 下载和拉取断言验收。 |
| `2.6-01` | P0 | 创建时固定 packagePlan | Ginkgo 主测 | 直接读新集群的 status.packagePlan，与该节点架构和 Registry 中选定的制品摘要逐槽位对照。 |
| `2.6-02` | P0 | packagePlan 数据边界 | Go 契约优先 | 以带测试认证的 Registry 触发解析，检查 API 对象和持久化 plan 的字段及凭据边界。可用候选 handler 集成测试。 |
| `2.6-03` | P0 | Agent 冷拉取并执行 | Ginkgo 主测 | 冷 Agent 的真实拉取、解包和 Task 执行形成证据链，需要新缓存目录及可查询的 Registry。 |
| `2.6-04` | P1 | Agent 缓存命中 | Ginkgo 主测 | 保留校验通过的本地缓存再次执行，用实际 Task 结果及 Registry 请求计数判断是否重复下载。 |
| `2.6-05` | P0 | 缓存损坏和 digest 校验 | Ginkgo 主测 | 用损坏的测试缓存或制品夹具验证拒绝及恢复，Go 可检查任务、摘要核验和安装副作用。 |
| `2.6-06` | P1 | 仓库暂不可达 | Ginkgo 主测 | 在同一测试环境对比暖缓存和冷缓存的断连行为，恢复连接后观察实际重试。 |
| `2.6-07` | P0 | 仓库配置优先级及权限 | Ginkgo 主测 | 用可区别的 Registry 验证配置优先级，并用 SSH 对照实际配置文件权限、日志和消费记录。 |
| `2.6-08` | P1 | 默认 Delivery Policy | Ginkgo 主测 | 在干净 KC 实例读取启动时种入的 Delivery Policy，对照本轮物料与 ComponentMeta。 |
| `2.6-09` | P1 | 自定义 Delivery Policy | Ginkgo 主测 | 通过实际 API 切换测试平台的白名单，对照下一次请求准入和已有集群保存的 plan。 |
| `2.6-10` | P0 | 策略缺失槽位或 repository | Go 契约优先 | 用可控策略与仓库清单逐个缺失槽位或 repository，验证 resolver 和 API 拒绝以及 Operation 存储无变更。 |
| `2.6-11` | P0 | bootstrap / extension 隔离 | Ginkgo 主测 | 在两个不同集群做实际平台交付或扩展安装，对照固定 digest、工具可执行性与 B 的 plan。 |
| `3-19` | P1 | 局部 Registry 生命周期 | Ginkgo 主测 | 在专用节点执行仓库部署和清理，通过 SSH、可拉取 tag 和对照 tag 确认影响范围。 |
| `3-20` | P2 | `resource list` | Go 契约优先 | 用固定 inventory/测试 Registry 验证 CLI 按版本和架构筛选的结果。可复用 pkg/cli/resource 测试，无需部署 KC。 |
| `3-21` | P2 | `resource inspect` | Go 契约优先 | 使用同一库存夹具核对详情与摘要，检查缺失项和退出码。真实制品可用性使用 6-02 的结果。 |
| `3-22` | P1 | `resource refresh` | Ginkgo 主测 | 向隔离 Registry 加入一个真实测试制品，再刷新并查新元数据，因果可由 Go 直接核对。 |
| `3-23` | P1 | `delivery-policy template/get/apply/diff` | Ginkgo 主测 | 调用候选 CLI 读模板与 diff，再 apply 到专用平台，用后续解析结果和配置快照核实变更。 |
| `3-24` | P1 | `delivery-policy validate` | Go 契约优先 | 给真实 validate 子命令合法和非法本地策略，直接检查退出码、错误与基线无写入。 |
| `6-01` | P0 | 支持策略和物料一致 | 浏览器或发布 CI 主测 | 发布 CI 对照当前策略、正式清单、物料和公开支持表。公开承诺及完整的候选物料矩阵由发布流程提供。 |
| `6-02` | P0 | 启用的 OCI 制品完整 | 浏览器或发布 CI 主测 | 发布 CI 遍历候选仓库的所有启用条目，核对可解析、可拉取和组件类型。需要完整候选清单。 |
| `6-03` | P0 | ReleaseManifest 内容契约 | 浏览器或发布 CI 主测 | Go 契约检查 manifest 结构，发布 CI 用生成的本轮清单检查所有启用物料和类型。来源是候选发布产物。 |
| `6-04` | P0 | 制品来源和构建身份 | 组合验收 | 发布 CI 证明同一候选的 digest 与构建来源，Ginkgo 用节点进程、实际镜像和已服务的 Console 版本核对运行身份。 |
| `6-05` | P0 | 制品不可变性 | 浏览器或发布 CI 主测 | 发布 CI 用专用 Registry 检查重复/冲突发布及原 digest 保留，复用打包工具已有契约即可。 |
| `6-06` | P0 | 多架构索引和过滤 | 浏览器或发布 CI 主测 | OCI 校验工具与发布 CI 检查父 index、子 manifest 及层和过滤结果。这些断言来自仓库对象，原生运行另由生命周期用例验证。 |
| `6-07` | P0 | Registry sync 消费 | 组合验收 | 发布 CI 证明完整候选同步及目标 digest，Ginkgo 再用真实 Agent 从目标拉取并执行任务。 |
| `6-08` | P0 | qualification 与 stable gate | 浏览器或发布 CI 主测 | 发布 CI 核对 tag、qualification、manifest 原始字节与 acceptance。Go 或 shell 契约可模拟误绑输入，候选门禁由实际工作流取证。 |
| `6-09` | P0 | 发布制品实机生命周期 | Ginkgo 主测 | 每个原生 OS/架构变体使用对应制品跑同一套安装、建群、业务、适用升级和清理断言。Go 核对运行版本、OS 与架构。 |
| `6-12` | P2 | Docker CRI 退役契约 | Go 契约优先 | 把旧 Docker CRI 输入送到真实 CLI 校验和 API 准入，检查错误、退出码及零对象和任务变化。 |
| `6-13` | P2 | 旧安装入口与 nfs-provisioner 退役契约 | 浏览器或发布 CI 主测 | 发布工具检查正式包及 CLI 帮助，Cypress 检查当前 Console 选项是否仍展示退役入口。两个发布面都需观察。 |

## KC 平台部署与运维

[对应执行规格](release-case-specs.md#platform-upgrade)

| 编号 | 优先级 | 用例 | 判定 | 理由和取证条件 |
| --- | --- | --- | --- | --- |
| `1.2-02` | P0 | Server 与 Agent/K8s 混部 | Ginkgo 主测 | 在同一 VM 同时运行 KC 与受管 Kubernetes，用 API 和 SSH 对照端口、运行目录及业务健康。 |
| `1.2-03` | P0 | KC 平台 HA 与单点故障恢复 | Ginkgo 主测 | 三节点真实 KC Server 和平台 etcd。一次停启一个成员（含 leader），对照各入口写读、存活多数派、成员恢复与数据一致性。 |
| `1.2-04` | P0 | Console 发布组件部署 | 浏览器或发布 CI 主测 | 用 Cypress 从实际入口加载指定 revision 并登录，观察页面加载的资源和登录后请求。Go 可查静态文件或 API。 |
| `1.2-05` | P0 | 单 Server 平台部署与任务分发 | Ginkgo 主测 | 从候选 deploy 发起，验证参数、退出码、服务布局和 etcd 写读。单 Server 按同机、分离和多 Agent 布局取变体，逐个 Agent 用任务 UID 核对执行。 |
| `1.3-01` | P0 | 部署预检 | Ginkgo 主测 | 用候选 CLI 对一次性目标做不可达、缺工具或缺包对照，修好后对同一目标部署并检查状态。 |
| `1.3-02` | P1 | 平台 etcd 冷启动竞态 | Ginkgo 主测 | 以新的平台 etcd 数据目录冷启动，实际发起写读见证初始化竞态和最终收敛。 |
| `1.3-05` | P0 | 独立 Agent 接入（`kcctl join`） | Ginkgo 主测 | 用本轮 join 命令加入指定独立节点，对照新身份的证书、部署配置和实际任务回执，并核对其他节点。 |
| `1.3-11` | P1 | SSH 认证与 sudo | Ginkgo 主测 | Go 分别调用 CLI 或 SSH 做密钥、密码、非 root sudo 和自定义端口，每种以真实远端执行取证。 |
| `1.3-12` | P0 | 初始管理员和配置 | Ginkgo 主测 | 用真实 KC 登录和受保护 API 验证指定初始凭据。JWT 默认值、输入合法性可先由 Go 契约检验。 |
| `1.3-03` | P0 | 平台清理并重新部署 | Ginkgo 主测 | 执行候选 clean --all，核对服务、目录、退出码及非目标服务。用同节点实测重新部署、登录和建群。 |
| `1.3-04` | P0 | 直接重复 deploy | Ginkgo 主测 | 在健康平台再次调用 deploy，比较退出原因、原进程、原数据和接口可用性。 |
| `1.3-06` | P0 | API 不可达时按部署清单 clean | Ginkgo 主测 | 用独立 VM 上的保存清单，设定 API 不可用后走 CLI 清理，核对同机对照服务和二次部署。 |
| `1.3-08` | P1 | `kcctl doctor` 健康诊断 | Ginkgo 主测 | 走实际候选 CLI，对照独立测到的 API、etcd、Agent 和 SSH 状态，检查健康与故障时的诊断及退出码。 |
| `1.3-09` | P0 | 平台整体升级 | Ginkgo 主测 | 在等价旧平台基线上分别用真实发布版本在线升级、使用本地清单和内网制品断公网升级，核对清单校验、实际版本及既有资源。 |
| `1.3-10` | P0 | 平台组件独立升级与回滚 | 组合验收 | Ginkgo 校验单组件参数、进程、混跑、其他组件版本和失败回滚。Console 子项用浏览器核对加载的实际资源。 |
| `3-26` | P2 | `kcctl status` | Ginkgo 主测 | 在健康和异常两种实机状态下，将 CLI 列表与实际组件进程及主机对应。 |
| `3-27` | P2 | `kcctl deploy config` | Go 契约优先 | 本地调用 deploy config 并解析输出模板，检查默认字段以及测试凭据未被带入。 |
| `3-28` | P2 | `kcctl version` | Go 契约优先 | 直接执行候选 CLI 的 version，将返回版本与候选的构建元数据对照。 |

## Kubernetes 创建与网络

[对应执行规格](release-case-specs.md#network-storage)

| 编号 | 优先级 | 用例 | 判定 | 理由和取证条件 |
| --- | --- | --- | --- | --- |
| `2.1-02` | P0 | AIO 建群 | Ginkgo 主测 | 用真实 AIO 节点完成候选建群，通过下载的 kubeconfig 查 Ready、普通 Pod 调度和 Service 读写。 |
| `2.1-03` | P0 | 纯控制面 HA 建群 | Ginkgo 主测 | 三台 VM 可见第一台 init、后两台控制面 join，以 etcd 成员、单点故障后的写读和恢复取证。 |
| `2.1-04` | P0 | 受支持的 K8s、CRI、CNI 组合实装 | Ginkgo 主测 | 每个发布组合取代表拓扑，读取实际 API Server、kubelet、containerd 和 Calico 版本，验证容器启动、数据面及删除。利用交付、建群和升级前的基线集群。 |
| `2.1-10` | P0 | 建群版本与组件组合准入 | Go 契约优先 | 以当前 resolver 和 API 准入对照允许和非法组合，核对无新对象或 Operation。成功实装复用 2.1-04。 |
| `2.1-24` | P0 | 单控制面与独立 Worker 建群 | Ginkgo 主测 | 常规冷建 1M1W，验证真实成员、Worker 调度和 Service。批量加入或多目标 Task 变更时冷建 1M2W，检查两个 Worker 的加入 Task 都成功后 Operation 才进入下一步，并确认每个 Worker 均 Ready、可调度。 |
| `2.1-26` | P0 | 节点占用及拓扑准入 | Go 契约优先 | 向候选准入 handler 提交无 Master、重复和占用样本，对照真实存储里的对象、Operation 和节点占用。 |
| `2.1-28` | P0 | CIDR 校验 | Go 契约优先 | 以真实接口所用的节点网段和合法请求为对照，逐项检查 CIDR 错误及拒绝后的存储状态。 |
| `2.1-29` | P1 | K8s 节点预检 | Ginkgo 主测 | 在专用 VM 提供可控预检条件，按端口、磁盘、时间及主机名错误观察节点与步骤，恢复后继续。 |
| `2.1-30` | P0 | 失败安装的恢复 | Ginkgo 主测 | 让一项已受理的安装在测试目标失败，关联真实 Task，再验证原生重试或正常清理的最终效果。 |
| `2.1-31` | P0 | 建群后的共用健康检查 | Ginkgo 主测 | 在各建群场景调用同一组 Go 探针，检查 /readyz、系统 Pod、真实读写及跨节点数据面。HA Worker 的 VIP 证据单列。 |
| `3-05` | P0 | `kcctl create cluster` | Ginkgo 主测 | 用候选命令发起真实建群，参数、服务端 plan、运行版本和节点角色可逐个对照。 |
| `2.1-08` | P1 | kube-proxy 模式选择与转发 | Ginkgo 主测 | 两个模式共用节点与 Service 探针，按所选模式核对实际规则，并从另一节点验证 ClusterIP 和 DNS。 |
| `2.1-11` | P1 | 自定义网段与 DNS 域 | Ginkgo 主测 | 用实际自定义 CIDR 和 DNS 域建群，观察 Pod/Service 分配并通过业务探针核对 FQDN。 |
| `2.1-12` | P1 | API 外部地址和证书 SAN | Ginkgo 主测 | 由预期外部客户端对真实地址发起 HTTPS，校验签发链、SAN 与目标集群数据。 |
| `2.1-13` | P2 | feature gates 透传 | Go 契约优先 | 复用 kubeadm 渲染测试，核对 API Server、controller-manager、scheduler 和 kubelet 的消费字段。配置渲染或 K8s 版本变化时核对实际运行参数。 |
| `2.1-14` | P1 | Master 调度设置 | Ginkgo 主测 | 在可调度 AIO 和保留污点的 1M0W 分别提交普通 Pod，以实际调度结果和 FailedScheduling 事件作对照。 |
| `2.1-15` | P2 | 仅安装 Kubernetes | Ginkgo 主测 | 在仅安装 Kubernetes 的环境读真实 Pod、Node 和 CNI 状态，再补装匹配 CNI 并重跑数据面探针。 |
| `2.1-16` | P1 | 自带根 CA | Ginkgo 主测 | 用测试 CA 建群，直接验证真实叶证书签发链和受信 TLS 连接。 |
| `2.1-17` | P2 | kubeadm 预检忽略项 | Go 契约优先 | 检查 CLI 注解到 kubeadm 配置的渲染，仅传入所选项。相关 CLI 或 kubeadm 步骤变化时，用真机对照所选项被豁免且另一项仍拒绝。 |
| `2.1-18` | P0 | 默认 Calico VXLAN | Ginkgo 主测 | 用真实两节点核对 Calico VXLAN 配置，并从两个节点 Pod 互访及验证 Service。 |
| `2.1-19` | P1 | IPIP/BGP / cross-subnet | Ginkgo 主测 | Go 可协调 IPIP、BGP 或 cross-subnet 变体，结论取自真实路由、隧道和双向流量，需要对应网络拓扑。 |
| `2.1-20` | P1 | 节点地址自动探测 | Ginkgo 主测 | 在真实多网卡节点对照自动及显式选择，检查实际通信地址、underlay 与跨机可达性。 |
| `3-14` | P1 | `kcctl set cluster` | Ginkgo 主测 | 实际调用 set 命令修改和置空字段，用 API 对照持久化状态和非法端口的拒绝结果。 |
| `2.1-07` | P0 | 安装镜像与物料来源 | Ginkgo 主测 | 通过 cold-cache Registry 日志、Agent digest 与 Pod 实际镜像证明安装包和运行镜像来源。 |
| `2.1-21` | P1 | 包仓库与集群镜像源隔离 | Ginkgo 主测 | 以两个可区分的仓库搭环境，实际拉取及集群保存的引用可以分别核对包源和镜像源。 |
| `2.1-22` | P1 | 私有 CRI Registry | Ginkgo 主测 | 在实际 containerd 新增、修改、移除私有认证，用不同未缓存镜像对照 Pod 拉取结果。 |
| `2.1-33` | P0 | 离线 imageRegistry 引用 | Go 契约优先 | 用内网 Registry 记录检查默认、显式、缺默认或无效引用的解析和持久化。实际消费取 2.1-21、1.1-03 的实机证据。 |
| `3-07` | P1 | 平台 Registry 对象 CRUD | Ginkgo 主测 | 从真实 CLI/API 操作测试 Registry 对象，核对保存字段、非法值拒绝及实际查询或使用。 |

## 节点管理与拓扑变更

[对应执行规格](release-case-specs.md#节点增删角色与-agent-身份j04)

| 编号 | 优先级 | 用例 | 判定 | 理由和取证条件 |
| --- | --- | --- | --- | --- |
| `2.2-01` | P0 | 扩 Worker | Ginkgo 主测 | 扩入指定空闲节点，记录本次 AddNodes 和实际 Pod 调度及 Service 访问。 |
| `2.2-02` | P0 | 移除 Worker | Ginkgo 主测 | 分别复现健康受 PDB 保护和失联目标，读取迁移 Pod、成员变化、目标主机残留与剩余业务。 |
| `2.2-04` | P0 | Master ↔ Worker 转换 | Ginkgo 主测 | 用多节点实际转换角色，观察 taint、静态 Pod、etcd 和 plan。最后一台控制面的对照用真实 API 验拒绝。 |
| `2.2-08` | P0 | 扩容复用原 plan | Ginkgo 主测 | 只在独立仓库和平台设置中变更选择前提，查询新 Worker 实际拉取的摘要与保存 plan 是否一致。 |
| `2.2-09` | P0 | 增加 Master 后控制面一致 | Ginkgo 主测 | 从现有控制面新增成员，通过新端点读取旧数据，用 SSH 和 etcd 核对成员身份、健康与实际静态 Pod。 |
| `2.2-10` | P0 | 移除 Master 后收敛与最小成员保护 | Ginkgo 主测 | 收集 etcd 成员及实际端点结果，检查 3→2 目标移除、2→1 拒绝及零成员变更，含 Worker 时从 Worker 查到真实 VIP 收敛。 |
| `2.2-05` | P1 | 禁用/启用节点 | Ginkgo 主测 | 在同一真实空闲 Agent 上执行禁用、尝试准入、启用及再准入，可用 API 和任务确认差异。 |
| `2.2-06` | P0 | Agent 失联与任务恢复 | Ginkgo 主测 | 在真实在途 Task 上中断 Agent 并跨过 Lease 宽限期，核对目标、Task 结果与 Ready。恢复后同一身份继续续约，完成实际任务。 |
| `2.2-07` | P0 | Agent 注销与重新注册 | Ginkgo 主测 | 走候选 drain 的取消、占用保护及真实注销。对照 Node、部署清单和目标配置清理。重新 join 生成匹配的身份与证书，查真实任务及旧身份拒绝。 |
| `2.2-11` | P0 | Agent 注册和状态授权 | Ginkgo 主测 | 以两个专属 Agent 的真实身份调节点和任务状态 API，检查各自持久化范围和旧更新拒绝。 |
| `2.2-13` | P1 | Region 归属和调度约束 | Ginkgo 主测 | 在两个测试 Region 注册 Agent，通过 Node API 对照标签与筛选。合法同 Region 对照可复用一次建群，混合 Region 请求验证零新增 Operation 和占用。 |

## Kubernetes 升级与证书

[对应执行规格](release-case-specs.md#upgrade)

| 编号 | 优先级 | 用例 | 判定 | 理由和取证条件 |
| --- | --- | --- | --- | --- |
| `2.3-01` | P0 | Kubernetes 实际升级与维护行为 | Ginkgo 主测 | AIO 和 1M2W 共用候选升级流程。核对 CLI 参数及维护提示，分别检查 AIO 停机恢复、逐节点 Task 顺序、真实驱逐/PDB 和业务连续性。两种变体均查 API Server、kubelet 版本及实际数据。 |
| `2.3-02` | P0 | 升级失败后重试 | Ginkgo 主测 | 在受测试 PDB 保护的集群等待可重试失败，用同一 UID 复测并查已执行步骤的实际副作用。 |
| `2.3-03` | P0 | 升级失败后重置阶段 | Ginkgo 主测 | 在真实失败且无活动任务的集群对照 dry-run 和 reset，比较 Operation 历史和主机实际版本。 |
| `2.3-04` | P0 | K8s 服务证书更新 | Ginkgo 主测 | 直接读取各控制面实际服务证书和健康 TLS 端点，比较更新前后的序列号、CA 指纹及连接。 |
| `2.3-06` | P0 | 升级 plan 与重试的固定 digest | Ginkgo 主测 | 核对升级操作按目标版本解析并保留原有合法配置。在测试仓库移动 tag 后重试原 Operation，按 Agent 拉取及 Task 对照固定 digest。 |
| `2.3-08` | P0 | 升级准入边界 | Ginkgo 主测 | 版本规则可用 Go 表驱动，完整判据还需真实低版本群及 AIO/多节点对照，核对拒绝前后主机和 Operation。 |

## 备份恢复与插件存储

[对应执行规格](release-case-specs.md#backup-storage)

| 编号 | 优先级 | 用例 | 判定 | 理由和取证条件 |
| --- | --- | --- | --- | --- |
| `2.5-01` | P0 | 备份点与快照实物 | Ginkgo 主测 | 先用 FS 备份点，声明 S3 时加用真实 S3 或 MinIO。按本次返回名称核对快照状态、实际文件或对象、字节大小和摘要，检查非法类型拒绝。 |
| `2.5-03` | P0 | 手动备份与恢复验收 | Ginkgo 主测 | 通过真实备份名和恢复 Operation 核对 A→备份→B→恢复后读 A，再复查节点、插件、Service 与业务。FS 默认，S3 按发布范围单独取结果。 |
| `2.5-04` | P0 | 删除快照和失败恢复 | Ginkgo 主测 | 使用专用快照在可控存储上验证删除失败保留记录并可重试，成功时对象与实际文件均消失。 |
| `2.5-05` | P1 | 定时任务单次触发 | Ginkgo 主测 | 在短的真实调度窗口指定 runAt，观察 Cron 与本轮快照、Operation 关联，并等待下一窗口证明只触发一次。 |
| `2.5-06` | P1 | 周期备份调度 | Ginkgo 主测 | 跨至少两个真实周期查按时生成的子备份及独立快照，时序由 Go 记录。 |
| `2.5-07` | P1 | 定时任务开关 | Ginkgo 主测 | 跨完整禁用窗口查无新增备份，重新启用后关联下一份实际快照。 |
| `2.5-08` | P1 | `maxBackupNum` 轮转 | Ginkgo 主测 | 对真实 FS/S3 等待轮转，分别检查过期实物、保留数量和另一手工备份。 |
| `2.5-10` | P0 | 坏快照和存储故障 | Ginkgo 主测 | 对专用备份点使用临时 FS 或 MinIO/S3，验证损坏快照或凭据错误下的真实失败、Operation 关联和数据状态。 |
| `2.5-11` | P2 | 备份描述变更 | Go 契约优先 | 覆盖更新只接受描述字段。大小和摘要的实物证据由 2.5-01 提供，实机变更校验复用已有快照。 |
| `4-01` | P1 | NFS CSI 生命周期 | Ginkgo 主测 | 两节点真实安装 nfs-csi，用 Registry/runtime 对照镜像 digest，再验证 RWX 跨主机读写。卸载时对照已安装配置，空 config 路径清理自定义 SC、Addon 和挂载。 |
| `4-02` | 退役 | 内置 MetalLB L2 | 历史项 | 内置 MetalLB L2 已退役，保留历史编号。 |
| `4-03` | 退役 | 内置 MetalLB BGP | 历史项 | 内置 MetalLB BGP 已退役，保留历史编号。 |
| `4-20` | P1 | 插件失败后重试 | Ginkgo 主测 | 在专用 NFS 环境模拟可恢复故障，关联原 Operation、实际插件状态、容器和清理。 |

## 集群删除与节点复用

[对应执行规格](release-case-specs.md#cleanup)

| 编号 | 优先级 | 用例 | 判定 | 理由和取证条件 |
| --- | --- | --- | --- | --- |
| `2.4-01` | P0 | 正常删除和并发同步 | Ginkgo 主测 | 在有 SyncKubeConfig 活动的窗口触发删除，观察并发收敛、真实主机回收和同节点重建。 |
| `2.4-02` | P0 | 删除失败安装的集群 | Ginkgo 主测 | 先达到一次真实 InstallFailed，再用普通删除验证平台对象和宿主残留都被回收。 |
| `2.4-03` | P0 | 删除失败后重试 | Ginkgo 主测 | 使用可恢复的卸载失败条件验证 TerminateFailed、原生重试及实际清理结果，记录副作用次数。 |
| `2.4-04` | P0 | 有备份的集群删除保护 | Ginkgo 主测 | 用真实快照对照有备份拒绝、快照保留和释放保护后正常删除。 |
| `2.4-05` | P0 | 删除后 Operation 清理 | Ginkgo 主测 | 利用双集群分别查询关联 Operation/Task、占用和业务，通过 UID 跟踪目标范围。 |
| `3-08` | P0 | `kcctl delete cluster` | Ginkgo 主测 | 执行候选 CLI，确认普通删除报告提交，再由测试观察本次 Operation 和清理。隔离环境另查 force 确认/取消、退出码和宿主机残留。 |

## Operation 与任务可靠性

[对应执行规格](release-case-specs.md#operations)

| 编号 | 优先级 | 用例 | 判定 | 理由和取证条件 |
| --- | --- | --- | --- | --- |
| `5-01` | P0 | 核心操作使用 Operation V2 | Ginkgo 主测 | 在各真实生命周期用 Cluster UID、动作和 Operation UID 关联 Task，再核对本次实际效果。 |
| `5-02` | P0 | 任务节点、步骤与失败诊断 | Ginkgo 主测 | 复用一个受控失败的真实多目标操作，按 UID、实际 IP、步骤及 API 日志定位错误，与成功目标作对照。身份参数边界另用 Go 契约。 |
| `5-03` | P0 | Server / Agent 重启恢复 | Ginkgo 主测 | 在隔离环境分别重启实际 Server/Agent，比较原操作、恢复状态和宿主标记写入次数。 |
| `5-04` | P1 | Agent Watch 中断恢复 | Ginkgo 主测 | 用可控 watch 服务或代理复现测试断连和 410，验证真实 Agent 重列且收到后续任务。精确协议分支可先用 Go 契约。 |
| `5-05` | P1 | 多节点步骤屏障及输出传递 | Go 契约优先 | 复用控制器的屏障与输出测试，核对所有必需目标完成后才推进、输出正确传递及缺失显式失败。任务编排或下发改动时补 2.1-24 的多目标实测。 |
| `5-06` | P0 | 操作取消 | Ginkgo 主测 | 在可观测阶段从候选 CLI 取消，核对退出码、目标 UID、终态和未执行步骤。API 的 UID/RV、冲突及各阶段用 Go 契约覆盖。 |
| `5-07` | P0 | 自动与人工重试 | Ginkgo 主测 | 触发一次可重试测试失败，从候选 API 验自动 attempt、人工 retryGeneration、UID 和已经完成的步骤。 |
| `5-08` | P0 | 同集群危险操作互斥 | Ginkgo 主测 | 两独立集群并发运行，用任务时间窗和实际写读验证 A 的互斥及 B 的独立进展。 |
| `5-09` | P1 | 回包丢失后的持久任务终态 | Ginkgo 主测 | 用可控持久层或代理注入已提交后回包丢失，重启后查询持久终态和副作用计数。需要能控制故障时机。 |
| `5-10` | P0 | 未知副作用时安全失败 | Ginkgo 主测 | 用专属任务和标记制造结果无法确认的测试条件，对照已确认结果，观察真实重启或重试后的执行次数。 |
| `5-11` | P1 | Operation/Task 游标分页 | Go 契约优先 | 用隔离的真实测试存储制造多个快照页，稳定验证 resourceVersion、continue、集合无重漏和过期拒绝。 |
| `5-12` | P1 | 任务日志 offset 和切换 | Go 契约优先 | 用真实测试日志文件/流和 API handler 对照字节 offset、UTF-8 和不同 Task。实际 Agent 的失败日志由 5-02 覆盖。 |
| `5-13` | P0 | 请求幂等与去重 | Ginkgo 主测 | 发真实手工备份并核对并发重放的 Backup、Operation 和后端快照数，另用双集群核查键作用域。 |
| `5-14` | P0 | 任务/操作超时 | Ginkgo 主测 | 用可控时长的真实 Task 分别触发 Step 和 Operation 超时，验终态、锁释放与后续操作。 |
| `3-11` | P0 | `kcctl operation list/describe/logs/retry` | Ginkgo 主测 | 执行候选操作 CLI 读取本次对象和失败日志，实际 retry 后通过 API 核对同 UID 及结果。 |
| `7-03` | P1 | 两个独立集群并行创建 | Ginkgo 主测 | 两套独立节点触发真实建群，记录操作时间交叠、双成功及各自数据面的验证结果。结论限于本轮实测并发数。 |

## 认证、授权与集群访问

[对应执行规格](release-case-specs.md#access)

| 编号 | 优先级 | 用例 | 判定 | 理由和取证条件 |
| --- | --- | --- | --- | --- |
| `4-08` | P0 | 用户与角色管理 | Ginkgo 主测 | 从实际 CLI 创建、删除角色及用户并核对 API 状态；真实登录、改密、禁用/启用及访问受保护资源验证角色生效。 |
| `4-08b` | P0 | RBAC 权限边界 | Ginkgo 主测 | 用不同真实身份访问同一已存在对象，校验允许、拒绝及操作前后的状态和任务。可复用轻量平台。 |
| `4-08c` | P1 | MFA 登录挑战 | Ginkgo 主测 | Go 能验证真实口令、MFA 挑战和受保护 API。测试 provider 只承担本轮挑战流程，承诺的外部渠道要另取真实结果。 |
| `4-08d` | P0 | Token 生命周期 | Ginkgo 主测 | 从候选 API 签发、刷新、撤销 token，在有效期内外请求同一受保护端点，按行为判定。 |
| `4-12` | P0 | kubeconfig 下载及使用 | Ginkgo 主测 | 通过真实路由取得直连/代理 kubeconfig，用实际客户端分别写读并测试无下载权限的账号。 |
| `4-12a` | P1 | 节点/集群 Web 终端 | 组合验收 | Go WebSocket 客户端可核对远端输出、身份、resize 和连接生命周期，浏览器侧的输入、显示与重连由 Cypress 验。 |
| `4-12b` | P1 | Pod exec | Ginkgo 主测 | 在实际多容器 Pod 调用 exec，用远端结果确认 namespace、Pod 和容器，另测错误路径和权限。 |
| `4-14` | P1 | 审计事件 | Ginkgo 主测 | 触发真实可审计操作，用事件 list/detail 验证主体、目标、动作、时间之间的对应。 |
| `4-16` | P0 | 敏感数据保护 | Ginkgo 主测 | 使用仅对测试资源有效的凭据，验证候选实际消费，再按身份检查响应、日志和审计。报告只保留比对结论。 |
| `4-18` | P1 | 登录限流 | Go 契约优先 | 通过可控时钟或专用短窗口测试实际认证 handler，覆盖达到阈值、账号隔离和过期恢复。 |
| `7-01` | P1 | OIDC 登录 | 组合验收 | 需要可登录的真实测试 IdP。Cypress 完成跳转、授权和回跳，Go 验证真实角色映射和拒绝路径。 |
| `3-25` | P0 | `kcctl login` | Ginkgo 主测 | 用两账户和隔离的 CLI 配置验证真实 login，检查失败前后的当前身份和后续请求。 |

## 平台管理、Console 与辅助命令

[对应执行规格](release-case-specs.md#平台管理与诊断j11j13)

| 编号 | 优先级 | 用例 | 判定 | 理由和取证条件 |
| --- | --- | --- | --- | --- |
| `4-07` | P0 | Console 端到端业务 | 组合验收 | Cypress 观察登录、向导、维护确认和 Operation 进度等真实页面行为。Go 对照请求归属、实际操作和 Kubernetes 数据。 |
| `4-09` | P2 | 集群模板 | Ginkgo 主测 | 后端判据可经 API 建模板并实际建群，核对复制后的配置与引用保护。页面交互可复用 Console 测试。 |
| `4-10` | P2 | DNS domains / records | Ginkgo 主测 | API 对照 A/AAAA 边界。启用同步的变体用真实 CoreDNS 配置和 Pod 解析证明新增及删除生效。 |
| `4-11` | P1 | 外部 K8s 纳管 | Ginkgo 主测 | 使用独立源 Kubernetes 和目标 KC 做真实纳管、同步及解除，解除后从独立 kubeconfig 读写源业务。 |
| `4-13` | P1 | 平台自省接口 | Ginkgo 主测 | 在候选平台读取自省接口，将返回字段与实际服务、策略及 Registry 可用物料逐项比较。 |
| `4-15` | P1 | 平台设置 | Ginkgo 主测 | 用专用设置保存凭据，查 API 脱敏及实际仓库认证，再验证轮换后的终端公钥可用。 |
| `4-17` | P1 | 健康与指标 | Ginkgo 主测 | 在健康及受控故障环境调用真实探针和 metrics，比较独立的实际服务状态和指标变化。 |
| `3-09` | P2 | `kcctl get` | Go 契约优先 | 直接测试候选 CLI 与查询/输出链路，用空、单和多项数据覆盖 selector、详情和机器可解析格式。 |
| `3-10` | P1 | `kcctl get --watch` | Ginkgo 主测 | 在实际 API 上对目标和对照对象产生已知 watch 事件，并观测取消时的连接回收。坏帧用可控输入覆盖。 |
| `7-02` | P2 | Shell 补全 | Go 契约优先 | 调用候选 CLI 本地生成脚本，直接用 bash 和 zsh 校验实际语法及不支持类型的退出码。 |

## 执行规格中的补充检查

| 检查点 | Go + Ginkgo 怎么验 |
| --- | --- |
| Kubernetes 资源代理 | Go 调实际代理 API 做 apply、get/watch、update 和 delete，用独立 kubeconfig 查目标集群和对照集群。启用 Console 资源页时，页面动作由 Cypress 验。 |
| 平台 HEAD 存在性、ConfigMap CRUD/watch 和 dry-run | HEAD 的状态和响应头适合 Go handler 契约。平台 ConfigMap 持久化、watch 事件、dry-run 和 Kubernetes 同名对象隔离可在真实候选 API 上查证。 |
| 发布前与发布后下载入口 | Ginkgo 可消费真实的候选及正式下载入口。候选产物先验，正式发布后再查实际附件、来源和摘要，分别保留证据。 |

以上检查按[执行规格](release-case-specs.md)执行，编号统计以用例目录为准。
