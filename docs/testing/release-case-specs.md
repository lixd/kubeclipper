# KubeClipper 发布与回归测试执行规格

日常改动从第一张表开始，准备候选和环境，再按变动模块加跑对应场景。每个表格行是一组可以共用环境的检查点，第一列使用当前用例编号。功能分类、优先级、拓扑及自动化选择见[用例目录](testcase-inventory-2026-11-19.md)，历史轮次见[功能台账](core-feature-checklist.md)。本页覆盖当前 154 项有效用例。每个功能用例按实际变体记录结果，旧编号归属见[用例归并记录](testcase-review-2026-11-19.md#case-dedup)。以下预期适用于后端 `fe0b59fe` 和物料锁定的 Console `2169a475`；后续候选要重新核对支持策略、路由和测试入口。

## 每次改动怎么跑

建群、升级和恢复使用共用探针。把查询、CLI 参数和诊断断言放进已经发生的操作，交付后的实装集群同时覆盖相应版本和拓扑。只有变更或发布范围需要不同驱动、拓扑、故障或初始版本时，另起对应变体。各断言分别记结果。[用例复用表](testcase-review-2026-11-19.md#case-reuse)

| 轮次 | 执行顺序 | 可以据此作出的判断 |
|---|---|---|
| 每次代码改动 | ① 在包含本次补丁的 Linux 源码上执行 `make test` 及受影响的构建或发布契约。② 在同一套独立 AIO 环境跑 Fast 检查及一条业务链：登录、建群、用下载的 kubeconfig 写读 ConfigMap、FS 备份、改值并恢复旧值、正常删群并核对节点释放。③ 加跑本次受影响的专项。 | 逐条记录这次改动及其依赖链路的结果。AIO 可检验单节点业务，跨节点网络留给多节点环境。纯文档改动可核对引用、命令、判据和受影响的文档契约。 |
| 全量回归 | 从发布物料检查起步，依次跑交付、建群、节点、升级、备份/插件和删除。权限、CLI、浏览器检查穿插在同一批操作中。分配独立集群给破坏性和并发用例。 | 跑完本次范围内全部 P0、P1、P2 检查点。支持但缺环境、跳过、未实现且未手测的项分别列出，不能记全量通过。 |
| 发布候选 | 使用同一候选的 qualification 制品执行所有适用 P0、受改动影响和本次明确承诺的专项。需要“全量通过”结论时执行上一行。最后核对正式渠道、证据与 acceptance。 | 必测项全部通过且清理完成才能记录候选通过。发布前可验证的候选入口和发布后才生成的正式下载附件分别登记，后者发布后补验。 |

| 改动位置 | 追加的最短真实链路 |
|---|---|
| 安装脚本、OCI、Registry、支持策略、打包 | 冷缓存取件和真实消费、坏件拒绝。源选择或离线物料变化再跑对应渠道直至建群。 |
| Server、Agent、控制器、存储、Operation、API | 使用受影响业务做一次成功、失败、原生恢复；涉及并发、持久化或身份时加跑跨集群、重启、过期请求。 |
| kubeadm、节点、CNI/CRI、IP 探测、升级 | 至少两个不同宿主节点验证数据面，再跑受影响的扩缩、滚动或单节点维护。变动涉及控制面成员时使用多 Master。 |
| 备份、调度或插件 | 真 FS/S3 或 NFS 上验证数据、失败、重试和清理。定时器改动必须跨实际触发窗口。 |
| IAM、代理、终端、Console 或 CLI | 用受影响的真实用户入口发起，查服务端结果；执行允许和拒绝两条路径。UI 表单同时核对请求载荷，CLI 同时核对退出码。 |

### 环境、证据与判定

1. 开跑前记录候选完整 SHA、有关的本地补丁摘要、来源与运行平台。涉及交付或实机时，再记录 Console SHA、各节点二进制 SHA256 和所用 ReleaseManifest SHA256。补丁构建单独标记。记录本轮承诺的 OS、架构、网络模式、渠道和可选能力。依赖缺失记 `BLOCKED`，只有明确在本轮范围外才记 `N/A` 并写原因。
2. 为本轮分配专用平台、节点、Registry 命名空间、备份目录/桶前缀及测试账号。先拍下基线：KC 对象 UID、节点占用、服务、端口、挂载、运行时目录、路由与出口策略。使用唯一轮次名；已有 Go E2E 含固定资源名，给它独占环境。建群/缩容、断网/恢复、轮换密钥、clean 只在该轮分配的对象上执行。
3. 异步请求先记录请求前的 Operation 集合，再用响应中的名称/关联标签或按目标 UID、动作和提交窗口从增量中唯一定位，记录 Operation UID、retry generation、相关 Task UID 与节点。检查该轮最终阶段和产品效果。拒绝请求要同时有正确错误、零新增副作用和同环境合法对照。计划检查、dry-run、mock、集成故障注入、实机分别标记证据层级。
4. 每行的多个变体分别记结果。缺证据记 `NOT RUN`；测试代码失效时单列原因，另用有效入口补测。同一前置条件失败导致的后续用例记 `BLOCKED`；产品违反预期或清理失败记 `FAIL`；保留首次失败、修复动作与复测，避免重跑抹掉失败。适用且有本轮断言证据才记 `PASS`。同一次操作可引用同一份证据，另一个入口须有其实际调用记录。运行前固定超时、探测频率和业务容许中断，超时保留现场，禁止无限等待。
5. 每次结果最少填下表字段。例中 A、B 是同一个 ConfigMap 在备份前和备份后的两个不同值。附件使用脱敏的命令结果、事件或对象摘要。token、kubeconfig、私钥、带认证参数的 URL 只留在私有环境。结束时先采证，再核对基线和清理。

| 轮次 / SHA | 编号·检查点 / 变体 | 入口 / 拓扑 / 证据层级 | 预期与实测（含时间、错误、计数） | Cluster / Operation / Task 标识 | 结果 / 证据路径 / 清理 |
|---|---|---|---|---|---|
| 待填 | 如 `2.5-03` FS 恢复 | 如 API、1M1W、实机 | 写入 A，备份后读到 B，恢复后读到 ___ | 待填 | 待填 |

### 现有自动化能证明什么

- 在一次性 Linux runner 或 sh-dev 上隔离的 systemd VM 中，从与候选一致的仓库根目录跑 `make test`（含 race）。套件中有真实启停 journald 的测试，普通共享宿主机不适合作为目标。实机入口是 `make build-e2e` 后运行 `./dist/e2e.test -cluster-install-short-timeout=10m -ginkgo.focus='\[Fast\]'`。须先按 AIO workflow 部署、登录并准备 SSH 测试账号；Go 套件读取运行用户的 `~/.kc/config`。核对实际执行的 spec 名、通过、失败及跳过数。当前 Fast 共匹配 7 条，Region spec 继承父级标签。建群、恢复、升级和 HA 建群的 Slow spec 需另外选择。发布契约的工具依赖与完整命令见 [Makefile](../../Makefile)、[Go 和 OCI 契约 CI](../../.github/workflows/go-test-coverage.yml)、[AIO CI](../../.github/workflows/test-aio.yml)。
- 需要完整 Go E2E 时，在有足够专用节点、匹配物料和账号的测试平台运行 `./dist/e2e.test`，保存 Ginkgo seed 与实际执行列表。补充的 API/主机/浏览器断言仍逐项执行。
- 当前主套件导入 cluster、node、region 三个包，静态登记 16 个 `It`。`test/e2e/kcctl` 未导入。两条 AIO 升级使用相同建群和升级路径，`WaitForUpgrade` 只凭 Running 判成功，还以 UpdateFailed 判断失败，升级实际失败态是 UpgradeFailed。恢复也只等 Running；Registry 检查看 status；NFS spec 缺少实际断言；节点终端只读第一条非空消息。这些缺少的断言按下文手测或用已补齐的自动化取证。[套件入口](../../test/e2e/e2e_test.go) · [集群 spec](../../test/e2e/cluster/spec.go) · [等待函数](../../test/framework/cluster/wait.go) · [节点 spec](../../test/e2e/node/spec.go)
- 在物料锁定的 Console 源码及配好的测试平台上运行 `CI=true yarn test:unit` 和 `yarn cypress run --headless`。检查本轮报告的实际 spec、failures、pending/skipped 及进程退出码。`package.json` 的 `test:e2e:run` 会吞掉 Cypress 失败；已登记的 `online-offline-install.cy.js` 还点击旧 `offline` 单选框，部分备份与纳管用例为 `it.skip`。将过时脚本记作测试缺陷，用当前向导完成相应业务验证。

<a id="delivery"></a>

## 发布物料与安装交付（J01、J14）

从 [packaging/resources.yaml](../../packaging/resources.yaml) 和[默认策略](../../pkg/delivery/apis/default_policy.go)取本次发布组合。当前安装基线为 Kubernetes / containerd / Calico：`v1.35.8 / 1.7.29 / v3.29.6`、`v1.36.4 / 2.2.4 / v3.31.5`、`v1.37.0 / 2.2.4 / v3.31.5`。策略匹配 minor，实际可装 patch 还须存在于清单与仓库。每轮把上述组合与发布给用户的支持表逐行对照。当前中英文 README 都把 v1.35.8 列成 Calico v3.31.5 / containerd v2.2.4，与默认策略不一致；将此项记作发布说明差异，实际安装以候选策略和可取物料为准。[中文支持表](../../README_zh.md#支持的版本) · [英文说明](../../README.md)

| 台账编号 | 准备与操作 | 通过判据 |
|---|---|---|
| 6-01、6-02、6-03、6-04 | 比对支持策略、发布清单、仓库中的 index/子 manifest 及层。按每个组件声明的类型检查 bootstrap、K8s、CRI、CNI、extension、Chart、runtime image 及启用插件。再从候选取 CLI、实际运行 Server/Agent/Console。 | 启用项逐个可解析，OS/arch、digest、组件版本和各自源 revision 对应。NFS 查运行镜像；两项 disabled 的 NVIDIA addon 不要求实装。记录实际进程和 Console 资源对应的构建。 |
| 6-05、6-06、6-07 | 在本轮专用仓库重复发布/同步相同物料，再引入同 tag 不同 digest；分别选 all、amd64、arm64 和不可用架构。让真实 Agent 从同步目标消费一件物料。 | 相同内容复用，冲突失败且原 digest 可读；各架构 index/层与清单逐一相符，不可用架构无错误替代。目标仓库有该次拉取及执行证据。 |
| 6-08 | 用 qualification 的完整 SHA 和 manifest 原始字节摘要核对本轮产物。在契约测试中分别替换 SHA、manifest、acceptance 结果。 | stable gate 只接受同提交、同 manifest、通过的验收记录及仍指向候选的 tag。错误绑定被拦截。按[验收协议](acceptance/README.md)归档。 |
| 6-09 | 从发布制品部署 KC、建群、验业务和网络，执行适用升级并正常删群。在每个承诺的原生 OS/架构组合上跑同一用例，首批执行 Ubuntu 24.04/amd64。 | 记录主机 OS 与架构、实际消费的制品、版本、业务和清理结果。每个 OS/架构变体各有实测结论。 |
| 1.1-01、1.2-05 | 从发布的安装脚本分别用默认 global 和 `KCZONE=cn` 获取候选 CLI，各完成 deploy → join → 1M1W 建群和数据面检查，同一实际集群回填拓扑、版本及调度结果。单独测试错误 zone、缺附件、checksum 不匹配。 | 校验脚本实际下载的二进制与清单，坏件未覆盖已有 CLI。GHCR/国内渠道的 Package Registry、镜像与运行节点拉取记录相符。公共 CA 使用系统信任。显式 fork/下载地址只计对应渠道。 |
| 1.1-02、1.1-07 | 在线从源 import 到隔离仓库，重复导入，再从目标部署。 | 第二次跳过相同内容，无缺层；部署实际消费目标 digest。命令退出码与同步统计相符。用错误参数和同步失败样本核对非零退出、准确报错。 |
| 1.1-03、2.1-07、1.1-07 | 联网用 `registry export`，跨机核对 Registry 自举小包和全量包摘要，以及全量包内声明的架构索引。离线端使用冷缓存、阻断公网，保留管理和内网链路；先用小包自举 Registry，再导入大包并重复导入，完成 deploy、join 和 1M1W 建群。 | 隔离前后有出站探测、缓存/镜像起始清单与拉取日志。重复导入复用相同 digest；损坏/缺层/错误架构失败；内网来源覆盖安装包和组件镜像。建群后真实网络通过，结束恢复出站并复查。[分渠道命令和数据面步骤](delivery-scenarios.md) |
| 1.1-04、1.1-05、1.1-06、1.1-09、2.6-07 | 私有 HTTP、受信私有 CA 的 HTTPS、认证仓库分别由 deploy/join/Agent 消费。交换显式配置、已保存仓库和 zone；分别在对应变体换错口令或 CA。不可达地址与缺失制品各测一次，随后恢复。 | 各入口实际访问预期前缀和协议；坏配置显式失败且没有回退上游，恢复可消费。含凭据的 KC 配置仅本用户可读（0600），脱敏输出；同名全局 Registry 配置冲突时保留既有配置。 |
| 2.6-01、2.6-02、2.6-08、2.6-09、2.6-10、3-23、3-24 | 保存原 Delivery Policy，用 `delivery-policy template/get/validate/diff/apply` 对照默认策略、更窄白名单、缺 slot/repository 及未发布版本。用允许输入实际建群，恢复策略再检查已有群。 | 默认策略可读取，diff/validate 不写状态，apply 后新请求按白名单解析。无效策略或无法解析的组合在对应阶段报错，未创建可执行的错误计划。`status.packagePlan` 固定组件、平台和 digest，排除凭据及 blob；既有计划不随策略变化。 |
| 2.6-03、2.6-04、2.6-05、2.6-06 | 用同一固定 digest 走 Agent 冷拉、暖命中、断开仓库读取已校验内容、保留原始层并移除提取快照、本地与远端均缺有效内容。契约层再测断流/Range、取消续传、双进程取同层和磁盘写满。 | 首次执行拿到已校验字节。暖命中不再下载层；断网可由完整原始内容重建；坏摘要/半包永不进入安装，写满有错误；并发清理保留活动消费者使用的文件，恢复来源后可重试。[缓存验证范围](../oci-acquisition-verification.md) · [获取器](../../pkg/delivery/fetcher) |
| 3-19、3-20、3-21、3-22 | 在专用 Registry 运行 `registry deploy/list/delete/clean`；另以 `resource list/inspect/refresh` 对同一测试仓库查版本和 digest，在仓库增加新物料后刷新。 | 列表和详情匹配真实 tag/digest，刷新读到新增内容；删除指定 tag 后不可读，对照 tag 可读；clean 回收本轮服务及目录。仓库服务和平台 Registry 对象分别取证。 |

<a id="platform-upgrade"></a>

## 平台部署、HA 和组件升级（J02）

平台升级后如需检查 standalone extension 与既有集群计划的隔离，复用下文[双集群 extension 用例](#operations)。

| 台账编号 | 准备与操作 | 通过判据 |
|---|---|---|
| 1.2-02、1.2-04、1.2-05、1.3-05 | 用 `kcctl deploy` 从单 Server 与 Agent 同机的 AIO 开始，访问 Console。多 Agent 与 Server/Agent 分离部署可合用一个直接 deploy 的布局。再从已有 Server 用 `kcctl join` 新增 Agent。核对 deploy 参数与退出码，为每个 Agent 提交可区分的任务。 | 部署清单、服务和节点角色及地址相符。校验新 Agent 证书和节点身份，核对部署配置中的注册成员。平台 etcd 实际可写读，每个身份只注册一次且 Ready，按节点 UID 验各自任务执行。Console 经真实入口登录可用。 |
| 1.2-03 | 3 个 KC Server/平台 etcd 成员冷启动，从各入口写读。逐个停一个 Server/etcd 成员（含 leader），从存活平台入口创建并回读唯一的平台 ConfigMap，恢复该成员后再重启下一个。 | 多数派存活时 API 在预设窗口内可写。每次故障后检查 leader、成员恢复至 3/3 及数据一致。保存 endpoint health/status 与请求时序。Kubernetes 控制面 HA 另测。 |
| 1.3-02 | 在一次性平台的冷部署中按需延迟 etcd 就绪，分别记录首次写探针失败、等待/重试和恢复。 | 未就绪时准确报告失败，恢复后部署收敛且平台实际可写读。复用已有冷部署环境即可验证。 |
| 1.3-01、1.3-04、1.3-11、1.3-12 | 分别以 SSH key、密码、受支持的非 root sudo 和自定义端口部署。对不可达、缺工具或端口占用执行预检；正常部署后尝试原地重复 deploy。用本轮指定的初始管理员密码登录。 | 预检指明节点与失败项，修复后可部署；已有平台拒绝重复覆盖，原数据和服务可访问。指定密码可登录，错误/已替代的默认口令被拒绝。配置契约另测部署时自动生成 JWT 签名密钥、保留用户有效密钥；Server 启动时拒绝缺失或公开默认值。 |
| 1.3-08、3-26、3-27、3-28 | 同一口令登录上下文执行 `doctor`、`status`、`deploy config`、`version`。先取健康结果，再停专用 Agent，并使一次诊断的 SSH/etcd 探针不可用，然后恢复。 | `deploy config` 输出可解析的默认模板，不带现网凭据；status 和版本对得上各主机。doctor 健康时返回 0，失败检查有非零结果，无法探测不报 Healthy；记录口令登录下真实平台 etcd 检查。 |
| 1.3-09 | 有真实业务、用户/角色、Delivery Policy、Registry、可用备份的旧平台，分别从可重复的旧基线走 `kcctl upgrade all --version` 和断公网的 `--manifest`。先测清单校验失败，再执行有效升级。 | 坏清单在停服务前拒绝。按 server → agent → console → kcctl 顺序核对实际二进制/静态资源和服务，kc-etcd/Registry 未被更新。业务恢复，权限和既有集群 UID、plan、备份可用性与升级前一致。 |
| 1.3-10 | 分别运行 `kcctl upgrade agent/server/console/kcctl`（每次只选一种组件）。特别用旧 Server 配新 Agent 再重复升级；一次 all 滚动中在已完成首节点后使下个节点的替换后健康检查失败。 | Agent 核对运行进程文件摘要、Ready 及真实任务；Server 核对 API 版本；Console 核对浏览器加载的版本资源；kcctl 核对目标文件和命令输出。组件独立升级只改其目标。失败节点恢复备份、已完成节点保持新版本、后续未执行；排障后重跑收敛。[升级实现](../../pkg/cli/upgrade/upgrade.go) |
| 1.3-03、1.3-06 | 在本轮所有集群正常删除后做平台 `clean --all`，再 deploy。另在 API 已不可达的专属拓扑上用已保存的 `--deploy-config` 做强制 clean。 | 检查 CLI 退出码和目标范围。对照部署基线核对服务、端口、数据目录和非目标服务；第二次全新部署可登录、join、建群，遗留身份/配置未串入新环境。 |

<a id="network-storage"></a>

## 建群、网络与镜像（J03）

M 表示 Kubernetes 控制面节点，W 表示独立 Worker。AIO 使用可调度的 1M0W。另用保留污点的 1M0W 验证普通 Pod 的调度约束。每个完整建群场景都执行下面的健康检查，跨节点断言使用多节点环境。节点增减、升级和恢复后复用相同探针，保留前后读数。[拓扑与组合矩阵](testcase-inventory-2026-11-19.md#topology-matrix) · [备份、恢复与 NFS](#backup-storage)

| 台账编号 | 准备与操作 | 通过判据 |
|---|---|---|
| 2.1-02、2.1-03、2.1-04、2.1-24、3-05 | 用交付、常规建群和升级前的已有集群覆盖三组版本，每个组合保留实测。常规冷建用 1M0W、1M1W、3M0W。`2.1-24` 以 1M1W 验 Worker 调度与 Service。修改批量 Worker 加入或多目标 Task 时追加 1M2W 冷建，检查同一次建群中两个 Worker 的加入 Task 都成功后 Operation 才进入下一步，并在各 Worker 上调度探针。记录各组合的运行实例。从 `kcctl create cluster` 发起至少一次。3M0W 另停一个控制面，在存活端点对 K8s 写读，恢复后再查成员。冷建多控制面时查第一台初始化、后续 Master 加入控制面与 Worker 加入工作节点的各自步骤。覆盖 HA + Worker 路径时另用 3M1W，停一台 Master，从 Worker 的实际 API 入口持续探测并写读，恢复后核对 VIP 后端和数据。要验两个 Worker 的跨节点流量与多控制面滚动，再配 3M2W。 | KC 和 Kubernetes 节点身份/角色/数量一致，读取真实 API Server、kubelet、containerd 和 Calico 版本，对照请求组合。运行测试容器，验证跨节点数据面，最终确认正常删除。3M0W 有 3 个健康的 Kubernetes etcd 成员，单点故障后保有多数派并能读回写入；记录每个实际验证的拓扑。 |
| 2.1-08、2.1-11、2.1-18、2.1-19、2.1-20、2.1-31 | 在多节点上分别覆盖 ipvs/iptables、默认 VXLAN、承诺的 IPIP/BGP 或跨子网模式。记录 KC 通信 IP、Node IP、Calico underlay IP，分别核对实际选择的网卡和跨主机可达性；在含容器网桥的多网卡环境验证默认选择及显式 interface/can-reach。自定义 Pod/Service CIDR 和 DNS 域再测。 | API `/readyz` 成功、全部预期节点 Ready、控制面和系统工作负载就绪。记录两端 Pod IP 与 nodeName，双向直连、ClusterIP、Service FQDN 均到达预期后端，再做带轮次标记的写读。检查实际代理规则、Calico 配置、接口/路由及分配的地址。跨子网需两端实际处于不同子网。 |
| 2.1-10、2.1-26、2.1-28、2.2-13 | 在可成功请求上每次只改一项：未支持的版本组合、只有 Worker、重复角色节点、另一个集群已占节点、跨 Region 节点、非法/重叠 CIDR 或与主机网段冲突。按 Region 列出节点，用同 Region 合法组合做对照。 | 各负向请求指出相应原因，无新群/Operation、无占用变更；列表归属准确，同 Region 合法请求成功。混合/未知节点架构在物料解析层单测并验证拒绝，不据此声明支持混合架构集群。 |
| 2.1-12、2.1-14、2.1-16、3-14 | 配置外部 IP/域名/端口、cert SAN、自带 CA 和 master 调度。由集群外客户端验证 TLS 并访问；用 `kcctl set cluster` 更新、置空外部地址字段，再传非法端口。 | 实际证书由指定 CA 签发且包含所用 SAN，所配置可达端点返回该集群数据；去除 Master 污点时测试 Pod 可调度。保留污点的变体须核对无容忍 Pod 的调度事件，再用有容忍的探针检查控制面。set 只修改所选字段，空值可清除，非法值保留原配置。 |
| 2.1-13、2.1-17 | 用 Go 检查配置渲染。选择当前 K8s 支持的 feature gate，单独传一项 kubeadm 预检豁免。参数、渲染或相关 K8s 版本变化时补一次对应实机检查。 | 所选值传入组件消费字段。Linux 实机对照所选预检项被豁免、另一项仍拒绝，feature gate 以实际进程参数和 kubelet 配置为证。 |
| 2.1-15 | 调整 kubeadm/CNI 安装路径时测试仅安装 Kubernetes 的变体。记录原生建群 Operation 和无 CNI 时的节点状态，再补装匹配 CNI。 | 未生成 CNI 安装步骤、实际无 CNI。补装后用完整网络探针验证可用。 |
| 2.1-29、2.1-30 | 在一次性 VM 分别验证被占用端口、缺空间及发行要求的时间/主机名预检；另让一次已受理的安装中断。解除单项故障后原生 retry，或在 InstallFailed 上正常删除。 | 区分准入前拒绝和安装时失败，后者能定位到节点与步骤；修复后同一 Operation 可收敛或可安全删除。保留主机恢复证据。 |
| 2.1-21、2.1-22、2.1-33、3-07 | 单独配置安装包源、集群组件镜像 Registry、containerd 私有 Registry。省略 imageRegistry、有有效默认值、缺默认/无效引用、显式另选分别建群或验证准入。平台 Registry 对象经 CLI 创建/删除、API 更新；给运行集群增改/移除 CRI Registry，用不同未缓存 digest 拉测试 Pod。 | 省略时解析并保存配置的默认引用；显式引用优先；无法解析时拒绝。kubeadm、pause、Calico 的实际镜像和 Registry 拉取前缀正确，包来源独立。私有镜像首次认证拉取成功；撤掉唯一认证配置后新拉取失败，恢复后成功。集群编辑清空/替换原 imageRegistry、升级请求带仓库覆盖字段均被拒绝。 |

## 节点增删、角色与 Agent 身份（J04）

| 台账编号 | 准备与操作 | 通过判据 |
|---|---|---|
| 2.2-01、2.2-02、2.2-08 | 保存已有 1M1W 集群的 plan，改隔离仓库中可变 tag 或平台默认项后加入第二个 Worker。验证新节点调度，并将业务副本分布到两个 Worker。待另一个 Worker 有余量再移除目标。分别测允许驱逐时的业务接管、Ready 节点受 PDB 阻塞和运行时失效的非 Ready 节点。 | 加入时沿用已存计划，节点实际 Ready、能调度、访问 Service；健康节点遵守驱逐，故障节点按支持流程收敛。核对成员与目标主机清理。允许驱逐时，替代 Pod 在剩余 Worker 就绪并能通过 Service 响应。 |
| 2.2-04、2.2-09、2.2-10 | 在可变拓扑上增删 Master，用 `kcctl cluster convert` 走 Worker → Master → Worker，尝试转换最后一台 Master，另尝试普通移除后仅剩一台 Master。离线回合保持断公网。记录每步预期成员列表。 | etcd 成员、端点健康、控制面 static Pod、标签/污点及节点角色与预期一致。转换至少保留一个控制面，普通移除至少保留两个 Master，拒绝时成员集合不变；尚有 Worker 时从它检查 API VIP 后端已增删并连接存活 Master。无独立 Worker 的 3M0W 只能回填控制面结果。 |
| 2.2-05、2.2-06 | 禁用/启用一台空闲节点，检查节点选择与实际准入。用主机采集的 hostname、地址、OS/arch 对照节点详情。记录指定 Agent 的 Lease，停服务跨过配置的失联窗口，在它有任务时观察状态，再启同一服务。 | 禁用节点不参与正常选取，启用后可选。Lease 停止续约，Ready 随过期变为 Unknown，任务有可观察结果；重连后原身份恢复、Lease 前进且实际任务可完成。 |
| 2.2-07 | 在专用 Agent 上验证占用保护和取消确认，再对已释放的节点执行 `kcctl drain --agent`，核对 CLI 退出码、Node、部署清单及目标配置，再用 join 重新注册。记录两次证书序列号、身份和节点 UID。 | 平台注销只移除目标 Agent，原 Node 查询不存在。重新注册有新凭据且可执行任务，旧身份不能修改新节点，其余 Agent 仍可执行任务。 |

<a id="upgrade"></a>

## Kubernetes 升级、PDB 与证书（J05）

升级前写入一个带轮次标记的测试 ConfigMap，并用独立客户端从首次变更前持续访问 Service，直到所有节点稳定。建议测试基线为每秒一次、单次超时 2 秒，记录时间、成功数、失败数、最长连续中断；这是本轮判据，可在开跑前按环境另定。1M2W 上用分布在不同 Worker 的副本和可驱逐的 PDB，另准备一个升级期间仍可用的探测入口。单控制面滚动时业务 Service 和 Kubernetes API 分开计数。

| 台账编号 | 准备与操作 | 通过判据 |
|---|---|---|
| 2.3-01、2.3-08 | AIO v1.36.4 有受 PDB 保护的应用；先省略维护选项，再以 `kcctl cluster upgrade --cluster-name NAME --version v1.37.0 --single-node-maintenance` 提交。另用 1M2W 先带错该开关，再无开关升级。单节点原先已 cordon 的分支单独验恢复调度属性。 | AIO 漏确认与多节点误用均拒绝，无新操作或节点改动。AIO 显示停机和绕过全部受影响 PDB 的提示，记录实际中断及恢复、节点调度属性。多节点按 Master → 各 Worker 执行，保留 eviction/PDB；测试 Service 全部探测成功（或满足预先登记的阈值）。两条成功路径均须本次 Operation 成功，所有控制面的 serverVersion、全部 kubelet、KC 显示版本为 v1.37.0。升级后仍能读到已写的标记，系统 Pod、DNS 和数据面正常。 |
| 2.3-02、2.3-06 | 多节点用阻止驱逐的测试 PDB 让升级停在目标节点，观察至可重试失败。记下每台节点实际版本和初始/目标 plan，在专用仓库移动测试 tag。恢复可驱逐条件，从原操作 retry。 | 阻塞期间受保护 Pod 未被强删，未执行节点没有提前升级。升级为目标版本解析新 plan；重试沿用该 Operation 的 UID/固定 digest，不重放成功节点的有副作用步骤。修复后核对实际目标版本与应用就绪，恢复测试 tag。 |
| 2.3-03、2.3-08 | 用真实 v1.35.8 群完成管理读写，再请求升至 v1.36.4。v1.36.4 上测同版本、降级、跨 minor 及畸形版本。独立失败 fixture 在没有活跃任务后执行 `PATCH /clusters/{name}/status`，先 dryRun 后实际 reset。 | 版本拒绝无新 Operation、主机和计划无变更。CLI 对目录外版本可先报“版本不可用”，API/校验测试另证跨 minor 规则。reset 在支持状态只改平台阶段为 Running，dryRun 不写；原 Operation 的失败记录、真实二进制版本保留，升级成功仍须另行验证。[校验](../../pkg/scheme/core/validation/validation_upgrade.go) |
| 2.3-04 | 保存 K8s 非 CA 服务证书序列号/有效期、CA 指纹及健康端点，提交集群 certification 操作。 | 本次操作成功，节点实际叶证书更新、签发链有效、CA 信任和 TLS API 访问正常；KC 展示到期时间与证书对应。 |

<a id="backup-storage"></a>

## 备份、恢复与 NFS（J06、J10）

| 台账编号 | 准备与操作 | 通过判据 |
|---|---|---|
| 2.5-01、2.5-03 | 建专用 FS 备份点；声明 S3 时用真实 MinIO/S3 再跑。非法类型做负对照。先写 ConfigMap 值 A，调用 `POST /clusters/{name}/backups`，用返回的备份名查 `GET /backups/{name}` 并关联 `X-Operation-Name`。备份完成后写 B 且确认已读到 B，再从这份备份恢复。 | 受理后等待 `backupStatus.status=available`、`backupFileSize`、`backupFileMD5` 与真实后端快照字节匹配。恢复操作成功并从 Kubernetes 读回 A；节点、插件、Service 可用。此处验收 Kubernetes etcd 状态。外部卷读写用 NFS 用例取证。 |
| 2.5-11 | 用轻量 API 契约检查修改描述，实机检查可借用上行快照。 | 描述正确更新，关联、状态、文件大小及校验和保持一致。 |
| 2.5-04、2.5-10 | 用错误 S3 凭据/不可写 FS 触发备份失败；在一次性集群用损坏的测试快照或错误的 S3 凭据验证恢复报错。删除有效备份时先让后端删除失败，再恢复依赖重试。 | 失败阶段及错误可定位，无伪造的可用快照或恢复成功。删除失败保留 `deleteFailed` 记录及删除操作关联，后端恢复后记录和文件/对象均消失；其余备份可读。被集群或备份占用的备份点删除受保护。 |
| 2.5-05、2.5-06、2.5-07、2.5-08 | 在有余量的备份点分别配未来 `runAt`、周期任务（例：每分钟、`maxBackupNum=2`）；等至少三次真实周期完成。disable 后跨一个完整触发窗口，再 enable；预置另一手工备份作对照。 | runAt 只触发一次；周期产生不同备份，最新两个可用，过期记录和实际文件被清除。禁用窗口无新触发，启用后继续。按 cron UID 查子备份，另一手工备份保留。 |
| 4-01、4-20 | 两个可调度节点+专用 NFS export。安装 nfs-csi，用新 RWX PVC 在不同节点的 Pod 间双向写读不同标记；另测重复相同配置、可恢复的安装失败与 retry。先释放 Pod/PVC，再用空 config 卸载一份自定义 NFS 配置。卸载配置处理变动时，再对照显式传入配置的路径。 | 安装 Operation 和实际驱动可追溯，运行镜像 digest 与同架构的锁定物料一致。PVC Bound，两端读回对方写入的字节；重复相同安装不产生第二份资源。按 StorageClass 回收策略核对 PV/后端目录，卸载后驱动/SC 和挂载回到预期。失败、重试和卸载均能追踪本轮操作。 |

复测恢复时可用下面的探针。`KUBECONFIG` 指向本轮目标，`NS` 为本轮已创建的独占 namespace，`RUN_ID` 唯一。三个阶段分别执行，每步检查退出码；中间的备份与恢复必须走 KC 的真实入口。

```sh
# 备份前
kubectl -n "$NS" create configmap restore-probe --from-literal=value="before-$RUN_ID"
# 指定备份已 available 后
kubectl -n "$NS" patch configmap restore-probe --type merge -p "{\"data\":{\"value\":\"after-$RUN_ID\"}}"
test "$(kubectl -n "$NS" get configmap restore-probe -o jsonpath='{.data.value}')" = "after-$RUN_ID"
# 本次恢复 Operation 成功后
test "$(kubectl -n "$NS" get configmap restore-probe -o jsonpath='{.data.value}')" = "before-$RUN_ID"
```

<a id="operations"></a>

## 操作控制、失败恢复与并发（J08）

在专用集群创建可控的多步骤任务，步骤在目标上写入带轮次和节点 ID 的标记。让一个步骤停在可观察的位置，便于安排取消、故障和重启。直接 POST Operation 的契约用例用错误 Cluster/Node UID 验证 409，再用合法目标比对服务端记录的权威 Node IP。在受控存储或协议测试中制造“请求已提交、响应丢失”的确定窗口；同时用真实 Agent 重启和业务任务覆盖进程边界。[控制 API](../../pkg/apis/operations/v1alpha1/handler.go) · [控制器测试](../../pkg/controller/operationv2/controller_test.go)

| 台账编号 | 准备与操作 | 通过判据 |
|---|---|---|
| 5-01、5-02、3-11 | 在建群、扩缩、升级、备份、恢复时关联本次 Operation。选其中一次真实失败，调用 `operation list/describe/logs`，从 API 查同 UID 的失败步骤和成功 Task。 | 对象指向正确的集群、节点和实际 IP。CLI/API 的错误、步骤和日志能定位到失败目标。 |
| 5-05 | 用 Go 控制器契约延迟多目标步骤中的一个任务，让下一步骤读取已知输出，再做缺失输出的对照。涉及多节点任务编排或下发变更时，补 1M2W 初始建群的 Task 时序。 | 所有必需目标完成后才开始下一步，步骤读取前序原值。缺输出时依赖步骤失败。 |
| 5-03、5-04、5-09、5-10 | 执行中分别重启 Server 和 Agent；协议测试覆盖 watch EOF/410 后重新 list/watch、Task 终态写入但响应丢失、无法证明副作用是否已发生。恢复后检查任务、宿主标记和实际对象数。 | 已确认的副作用只执行一次，已有终态可读回；无法确认时保留显式失败/待处理原因，不擅自重放危险步骤。恢复后无遗失任务、重复成员或第二份快照。记录每个故障点与恢复证据。 |
| 5-06、5-07 | 用候选 `kcctl operation cancel` 提交一次取消，核对目标和退出码。用 API 契约对 cancel/retry 校验缺字段、错 UID、过期 resourceVersion，以及对同一活动操作并发 cancel。另备最新且 Task 全终态的 Failed、TimedOut、Canceled 样本做 retry，再试 Succeeded、尚有活动 Task、目标已有更新 Operation。 | 缺前置条件 400，合法阶段的 UID/RV 冲突 409。取消只接受 Pending/Running 且 Active；已运行步骤允许协作收尾，后续未启动步骤无副作用。合法 retry 保持 UID、retryGeneration 增一；自动重试新建 attempt，人工重试只重做未成功目标。不允许的阶段 400，活动 Task/非最新操作 409。 |
| 5-08、7-03、2.6-11 | 用不重叠的节点分别建 A、B（如三节点按 2+1 分配）。在 A 运行时提交第二个互斥操作，在 B 同时读写和备份。两台独立空闲节点另做两次建群同时启动。再在 Running 的 A 执行 `kcctl cluster extension --cluster-name A`，记录 A、B 的 plan 和扩展工具文件。 | 同目标可在准入被拒或排队，按该业务契约检验，同目标危险步骤无时间重叠；B 在 A 未结束时能完成独立任务。并发建群有两个目标 UID、重叠时间窗及两次成功；只报告实测并发数。extension 只按 A 原 plan 重装并实际可用，A、B 的 plan 均不漂移。 |
| 5-13 | 对手工备份 POST 使用同一 `Idempotency-Key`，顺序重放、并发重放及受理后重放；同键改描述，删除备份但保留 Operation 历史后再重放。用第二个集群对照键作用域。插件安装另查相同配置的重复准入。 | 同集群同键同参数返回同一 Backup/Operation，仅一份快照；不同参数 409，已删且有历史 410，新键可创建。非法键报错，无键的独立请求独立记账。备份排队后旧 owner 不能覆盖后续操作阶段。插件按组件准入去重。[手工备份契约](../backup-admission.md) |
| 5-11、5-12、3-10 | 建多页 Operation/Task，记下首个快照，再翻页期间增删其他对象。测试非法 continue；指定资源名启动 `kcctl get --watch`，对目标和对照资源分别产生已知事件；用含中文的多段 Task 日志分段并 follow，切换至另一个 Task。 | 同一有效快照分页集合无重无漏，错误/失效游标有明确错误；命名 watch 只收目标事件，JSON/YAML 事件可解析；坏帧或服务端错误显式失败，取消后终止监听。日志按 Task UID 与字节 offset 拼接不丢字、不重复、不串任务；故障日志能定位节点，结束 follow 后释放连接。 |
| 5-14 | 分别触发步骤超时与 Operation 总超时。提交不合法的删除/插件操作 timeout；对已经受理的组件更新在发布 Operation 前后中断控制器，再重复观察和重试。 | 准入校验失败不改集群；运行逾期后有实际 Task 结果和 Operation 终态，锁可释放。组件受理的稳定名称只发布一个操作，失败/超时/取消投影 UpdateFailed；新 generation 或新 owner 不被旧结果覆盖，成功恢复 Running。后台 CRI 同步不改用户操作的归属。[组件状态契约](component-operation-convergence.md) |

<a id="access"></a>

## 身份、权限、kubeconfig 和终端（J09、J11）

先用测试管理员建立一个**只具有集群查看规则**的角色和一个无角色用户，保留规则快照。匿名、各用户及 Agent 使用独立上下文，口令/token 会话不携带管理员客户端证书。测试允许和拒绝时都指向同一个已存在对象，避免把目标不存在、网络不通或认证失败当成鉴权通过。[角色定义](../../pkg/server/config.go) · [IAM 路由](../../pkg/apis/iam/v1/registry.go) · [OAuth 路由](../../pkg/apis/oauth/registry.go)

| 台账编号 | 准备与操作 | 通过判据 |
|---|---|---|
| 4-08、4-08b、3-25 | 用 CLI 创建角色/用户并 login，API 查角色规则、用户角色和登录记录；测试管理员写、只含查看规则的用户 list/get/watch 集群和提交升级、无角色用户读集群，匿名读受保护端点。再走改密、禁用/启用及重新登录，最后由 CLI 删除测试用户和角色。 | 授权读写返回本轮对象。只读用户可读但升级 403、无角色对该受限资源 403；未认证请求在相应认证层被拒绝。拒绝时无对象或 Task 变更。旧口令/禁用用户不能建立新会话，恢复后新口令可用；删除后详情不存在。 |
| 4-08c、4-08d、4-18、7-01 | 密码登录签发 token；通过 IAM 查询、OAuth refresh、logout 和 TTL 到期前后分别访问受保护 API。专用账号达到配置的失败阈值，窗口内、窗口后及另一账号分别试登录。启用 MFA/OIDC 时分别完成有效挑战/回调；测错码、过期/已消费挑战、重放回调。 | 新 token 可用，已撤销/到期 token 被拒；限流窗口生效、另一账号不受影响、到期恢复。MFA 启用时只凭密码不能访问受保护资源；OIDC 真实 IdP 回调映射到预期账号和角色。测试验证码 provider 只证明挑战流程，实际短信渠道另记适用范围。 |
| 2.2-11、4-16 | 用 Agent A 真证书注册/更新自身节点和 Task 状态，并访问另一测试 Agent 的同类资源；另测旧 UID/RV。给 Registry、Provider 和平台配置写本轮唯一测试凭据，用外部身份检查 list/detail/watch、配置、日志和审计，覆盖普通及内部 informer 标识请求的响应裁剪。 | Agent 只能按其授权取得任务和提交状态，他人任务/节点拒绝，过期更新未覆盖新值。测试凭据在配置的真实消费端可用，但不出现在外部响应、操作日志、审计或导出的证据中。 |
| 4-12 | 用有 kubeconfig 权限的身份下载普通和 `?proxy=true` 两种配置，各自原样从预期客户端连入专用集群读写唯一标记；用缺该权限的身份请求下载。单测未就绪与不存在的群。 | 下载路由拒绝未授权用户。已签发的两种文件分别实测 server 地址、目标及 Kubernetes 身份能做的动作。直连还未生成 kubeconfig 时 503，未知集群 404；代理配置地址不可达即记录失败。 |
| 4-12a、4-12b | 分别建立节点 SSH 终端、集群终端和指定 Pod/容器的 exec，生成只可能来自远端的结果（例如比较远端计算的随机输入摘要）。另测错误 SSH 凭据、错误容器/不存在的 Pod、无权身份、resize 及中断重连。 | 输出摘要和远端身份/容器匹配，resize 在支持的终端生效，关闭后临时用户/连接可回收。失败请求不建立可执行会话；提示符或输入回显不足以通过。 |

**Kubernetes 资源代理**。用获得 `clusters/proxy` 权限的用户，通过实际对外入口，向 `/cluster-proxy/{cluster}/kubectl-apply` POST 一份带唯一字段的 ConfigMap YAML，再更新、经普通资源路径 GET/watch、向 `kubectl-delete` POST 删除。用独立 kubeconfig 核对每次值、事件和最终 404。同名对象放在 B 作对照。走 Console 网关时使用该网关配置的对外前缀（模板把 `/apis/cluster/` 转发至 `/cluster-proxy/`）。分别提交单份非法 YAML（失败且无对象）、正确动作配错误 HTTP 方法（405），以及无相应权限身份（拒绝）。多文档请求按对象核对实际结果和清理；该处理器逐对象执行。浏览器资源页启用时同测一次。[代理处理器](../../pkg/apis/proxy/handler.go) · [YAML 操作实现](../../pkg/apis/proxy/utils.go)

## 平台管理与诊断（J11、J13）

公共 API 改动时，从注册路由提取本次增删的 method/path 和子资源，逐个指定允许身份、前置状态及实测结果。现有轻量契约包括：在 users/roles/templates/domains 的集合路由用各接口支持的查询值发送 HEAD，分别取已存在和不存在的名称，检查 HTTP 200 及 `X-CHECK-EXIST: true/false`；在平台 core API 用专用 ConfigMap 做 CRUD/watch，分别验证更新与删除的 dryRun 不写入；同名 Kubernetes ConfigMap 作为独立对照。审计事件的 list 与 detail 应对应同一条写操作。[核心路由](../../pkg/apis/core/v1/registry.go) · [审计路由](../../pkg/apis/auditing/v1/registry.go)

| 台账编号 | 准备与操作 | 通过判据 |
|---|---|---|
| 4-09、4-10 | 建模板、用模板创建带引用的群，再更新模板、删除仍被引用的模板；删群后再删。对测试 DNS 域做域/记录 CRUD、A/AAAA 更新，并分别传非法地址、错地址族、未支持记录类型。 | 集群保留创建时配置，未知引用及删除被引用模板受拒；释放引用后可删。DNS 管理 API 返回指定值。启用同步时，在选定集群查看 CoreDNS 配置并从 Pod 解析该域，删除后记录从配置和解析结果消失，另一测试域仍可解析；非法更新保留旧值。 |
| 4-11 | 在独立目标平台用 kubeadm Provider 对带业务的外部源群 precheck → 创建 → sync → 解除纳管，先试无效 kubeconfig 和不可达节点。 | 正确 CA/SAN 可联通，纳管与同步的节点、Agent 和集群身份相符；真实错误可诊断，无误报成功。注销后目标平台记录收敛，源集群节点仍 Ready、业务仍可写读，按基线恢复 Agent。 |
| 4-13、4-14、4-15、4-17 | 查 `/healthz`、`/metrics`、配置组的 configz/status/components/componentmeta。做一次可审计的测试写入并在 auditing events 列表与详情查回。用隔离平台测试镜像模板保存、遮蔽、同 host 省略口令更新，以及 terminal.key 轮换。 | 健康结果对应现场，metrics 可解析、组件元数据与实际策略相符；事件的主体、目标、动作、时间正确。省略口令仍能真实认证，新公钥可用、私钥不可读。保留脱敏结果，恢复配置。 |
| 3-09、7-02 | 用 `kcctl get` 查询 cluster/node/user/role/configmap/registry，检查详情、支持的输出格式、选择器命中/无命中；用 `completion bash/zsh` 生成脚本并作语法检查。 | `get` 和 `operation list` 的 JSON 在 0/1/多项时始终保留 items 数组，详情保持单对象。输出能直接解析；无效格式或多余参数使退出码非零、错误写 stderr。检查 `operation logs` 的失败也传递至调用者。[CLI 契约测试](../../cmd/kcctl/app/release_cli_test.go)两种支持的 shell 均可解析生成物，不支持的 shell 返回错误。 |

<a id="console"></a>

## Console 浏览器业务（J12，台账 4-07）

浏览器用例在 Console 仓库的 `cypress/e2e/pages/` 维护。全局用例编号和通过判据以本页及目录为准。页面提交后可复用同一次操作，由后端用例按 UID 核对 Operation 和实际数据。下列检查点逐项记录结果，任何适用项缺证据时，父编号保持未完成。[候选联跑方式](e2e-automation-plan.md#console-e2e)

| 关联用例 | Console 浏览器检查 | 后端或发布验证 |
|---|---|---|
| 1.2-04、6-04 | 实际部署入口能加载候选页面，登录后发起有效 API 请求 | 启动与代理路由、锁定 revision、归档和运行制品身份 |
| 4-12a、7-01 | 终端输入、显示、resize、重连，OIDC 跳转及回跳 | 远端执行结果与授权、IdP 身份及角色映射 |
| 1.3-10、6-13 | Console 升级后的页面可用性，当前选择器的组件选项 | 组件替换与回滚、正式命令和发布物料的退役检查 |
| 4-07 | 下表中的完整用户操作和页面判据 | 请求归属、Operation 和实际业务结果 |

| 检查点 | 浏览器操作 | 对照证据 |
|---|---|---|
| 登录与授权 | 管理员与集群只读用户分别登录；登出后返回受保护页面。查看节点详情、Region、角色/用户及审计页，执行一项获授权的变更，尝试一项受限操作。 | 列表、详情和操作可见性与账户实际权限相符，后端拒绝受限请求。刷新/重新登录后仍显示正确身份与资源。 |
| 建群与仓库 | 选版本、拓扑、镜像仓库并核对确认页；覆盖默认、显式另选、列表加载失败重试及失效选择。提交后从列表重新进入详情。 | 向导在规定条件下允许或阻止提交；确认页、请求中的 Registry 引用、版本、节点与实际创建结果一致，集群详情/API 展示正确。 |
| 升级 | 在 AIO 上先关闭升级表单，再确认维护影响并提交；在多节点上检查同一表单，提交正常升级。 | 取消表单无新 Operation；AIO 明示业务 PDB 与停机影响，提交 `singleNodeMaintenance=true`；多节点无该可用开关。页面版本、操作进度与实际节点一致。 |
| 备份、存储与恢复 | 选择本轮备份点和真实生成的备份执行恢复。打开对应插件/存储页查看状态。 | UI 所选名称能关联真实快照及恢复操作，独立读回备份时的旧值。插件页与实际安装状态相符。 |
| 失败、控制与删除 | 进入失败 Operation 的节点/步骤/日志；按允许状态取消或重试，刷新页面。对测试群先取消删除，再确认删除，观察列表、详情和任务。 | 页面跟踪正确 UID/重试轮次，错误原因和后端一致；取消删除保留集群，删除完成后目标从列表移除，和本次 Operation 的实际结果一致。 |
| kubeconfig、终端及纳管 | 下载并实际使用 kubeconfig；从已启用的终端/资源页完成一次真实动作。纳管界面启用时从 precheck 走到 sync 和注销。 | 命令/写入命中目标集群或容器；外部集群注销后源业务仍可用。已承诺的入口缺失记失败，缺独立源群则记阻塞。 |

<a id="cleanup"></a>

## 删除、并发清理和节点复用（J07）

| 台账编号 | 准备与操作 | 通过判据 |
|---|---|---|
| 2.4-01、2.4-02、2.4-04、3-08 | 对有备份的群提交普通删除；先删备份，再用 `kcctl delete cluster` 删除。另对 InstallFailed 的群正常删除，并覆盖 SyncKubeConfig 尚在处理的时刻。 | 有备份的拒绝请求保留群及快照。释放保护后，CLI 仅报告请求已提交，继续观察本次 Operation 和实际清理。安装失败路径走普通删除。并发同步在限定时间内退出，没有卡锁或重建已删对象。 |
| 2.4-03、2.4-05、3-08 | 让一次已受理的删除在专用节点失败，观察 TerminateFailed，恢复条件后重试。另在契约层测提交结果未知、重复提交、非法 timeout。强制删除单用可销毁的 fixture，先取消后确认，记录命令提示、残留和人工清理结果。对照 B 集群和安装前的主机清单，在原节点重建一次。 | 合法重试收敛且没有两套删除副作用。force 明示跳过 Kubernetes 卸载、取消无副作用；强删仅计平台记录移除，主机经单独清理后方可复用。普通删除后集群、关联 Operation/Task、节点占用被清理，B 及平台服务健康。检查 kubelet、CNI 接口/路由、etcd、挂载以及从实际 containerd 配置解析的 root。重建后真实 Pod 网络、DNS 和新业务标记均正常。 |

## 去重、退役与下轮维护

- 每个编号对应一个功能及通过判据。同一操作下的版本、存储后端、节点布局和交付渠道用变体表示。CLI 的参数、交互或退出码随相应操作验，UI 的表单、提示和请求载荷用浏览器取证。线上/离线用物料来源、缓存和实际出站条件区分。[已归并编号](testcase-review-2026-11-19.md#case-dedup)
- 已移出清单的双栈及同 Addon 多实例沿用原范围决定。其余处置如下。

| 台账编号 | 处理 | 通过判据或归属 |
|---|---|---|
| 1.1-10 | 合并 | 公共 CA 随 1.1-01 的系统信任路径验收。 |
| 4-02、4-03 | 退役 | 内置 MetalLB L2/BGP 不安排安装用例。 |
| 6-12、6-13 | 保留负向契约 | CLI/API 拒绝 Docker CRI；正式命令、Console 选项及发布物料中无退役的静态安装服务或 nfs-provisioner。只查实际发布入口与产物。 |

- 下一次出现新增命令、公共 API、状态迁移、UI 入口或启用物料时，先对照其正向、拒绝和失败恢复路径，给它一个台账归属及可观察的通过判据。修过的缺陷留一条在修复前会失败的断言。结论按“编号 × 实际变体 × 候选”汇总，并保留仍未验证的入口、平台和故障条件。[CLI 命令树](../../cmd/kcctl/app/root.go) · [核心 API](../../pkg/apis/core/v1/registry.go) · [平台配置 API](../../pkg/apis/config/v1/registry.go)
