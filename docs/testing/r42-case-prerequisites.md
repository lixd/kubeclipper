# R42 未闭环 Case 前置条件

本表按 Case ID 对应 `core-feature-checklist.md` 和 `r42-case-ledger.md`。目标是让下一轮拿到依赖后可以直接开跑。上面的分类表保留可复用测试方法，最新未通过列表见文末“当前还未完全通过的13项”。状态为“部分”的 Case 只列尚未覆盖的分支；不要把已通过的子场景重复算成失败。除特别说明外，故障注入只在隔离集群、临时 Registry 或可恢复 VM 上进行，禁止修改共享 Registry `172.16.131.146:5003` 的 tag、凭据或 TLS 配置。

每轮开跑前保存当前 PlatformSetting、Registry 配置及节点状态；收尾删除临时集群、账号、证书、备份和 Registry 数据，恢复代理/防火墙/系统服务配置，并确认 3 台 Agent Healthy、无遗留 Kubernetes 服务。

## 1. Registry、交付与 SSH

| Case | 下次执行前置条件与操作 | 通过标准与清理 |
|---|---|---|
| 1.1-01 | 当前允许显式使用 fork GHCR 仓库及其配套 v2.0.3 kcctl；详见文末操作示例。官方默认路径缺制品单独记录；代理设置正确 NO_PROXY。 | 直接从 GHCR 拉取 bootstrap 完成 deploy，doctor Healthy，保存实际仓库/revision；同步到内网再部署的证据归1.1-02。 |
| 1.1-02 | 准备同一候选的 ReleaseManifest 和所有依赖制品；启动临时目标 Registry，执行 GHCR→`registry sync`→目标 Registry→deploy。 | 目标仓库有清单要求的全部 digest，节点只从目标仓库消费；删除临时仓库及凭据。 |
| 1.1-03 | 准备当前候选的离线 bundle、校验和及独立目标 Registry；在联网工作区 export，复制到断网测试区后 import 并 deploy。 | bundle 校验、导入和离线部署均成功；清理临时 bundle 与 Registry 数据。 |
| 1.1-05 | 启动隔离 HTTPS Registry，使用临时自签 CA；把 CA 通过受测配置下发到 deploy、join 和 Agent 消费节点。 | TLS 校验成功且无 `skip-verify` 绕过；删除临时 CA、证书、Registry 与节点信任配置。 |
| 1.1-06 | 准备只供本轮使用的认证 Registry 和低权限账号；配置 deploy、join、Agent 各自的凭据入口，并覆盖错误凭据负向场景。 | 正确凭据三条链路可拉取、错误凭据明确拒绝且不泄漏密码；撤销临时账号并清理凭据文件。 |
| 1.1-07 | 准备可重复同步的 ReleaseManifest 和完整制品；只对隔离目标 Registry 连续执行两次 `registry sync`。 | 两次后 repository、tag、digest 和数量一致，无重复/损坏对象；删除隔离 Registry。 |
| 1.1-08 | 准备当前候选 bundle 和空的隔离 Registry；对同一 bundle 连续 import 两次。 | 第二次导入幂等，内容/digest 与首次一致；删除临时 bundle 和 Registry。 |
| 1.1-09 | 在隔离 Registry 上分别提供错误地址、错误密码、错误 CA 和缺失制品；每次使用新的临时配置执行 deploy/sync。 | 每种输入都有明确错误、无半部署节点或错误制品写入；清除临时凭据、CA 和 Registry。 |
| 1.1-10 | 准备带公共 CA 签发域名证书的 Registry（可以只在内网开放）；测试节点系统默认信任签发链，域名解析指向该仓库，证书 SAN 与域名一致。 | 不传额外 CA 文件、不设置 skip-verify，deploy/join/Agent TLS 拉取成功；账号认证可照常启用。自签 CA 手工加入系统信任库仅覆盖系统信任路径，不替代公共 CA 签发证明。 |
| 1.3-01 | 在不触碰现有节点的临时 deploy 配置中，分别指定 TEST-NET 不可达主机和隔离 Registry 中不存在的制品。 | CLI 明确指出目标/缺失制品并以非零退出；无节点变更、无 Operation 残留。 |
| 1.3-02 | 用一次性 3 节点平台或 VM 快照，安排可控的 etcd 启动延迟/短暂连接失败；保存 server health、etcd 探针和 Operation 时间线。 | 探针在短暂失败后重试并建群成功；持续失败时给出可读终态；还原 VM 快照。 |
| 1.3-07 | 准备与目标版本匹配的离线平台 ReleaseManifest、server/agent/console/kcctl OCI digest 和隔离 Registry；先留存当前二进制/配置快照。 | `kcctl upgrade all --manifest` 逐槽位成功、平台健康且数据保留；失败注入后能回滚；恢复原平台二进制。 |
| 1.3-09 | 准备可访问的在线 ReleaseManifest 下载源及校验和；节点需有公网或本轮代理和正确 `NO_PROXY`。 | `kcctl upgrade all --version` 下载、校验并完成升级；撤销代理与临时下载源。 |
| 1.3-10 | 准备同一兼容目标版本的 server、agent、console、kcctl 制品和 manifest；每次只升级一个组件并保留回滚副本。 | 目标组件变化、其余组件版本不变；健康探测通过；恢复基线文件和版本。 |

## 2. 集群、网络、节点与升级

| Case | 下次执行前置条件与操作 | 通过标准与清理 |
|---|---|---|
| 2.1-03 | 按用户确认范围使用现有3台节点全部作master，0 worker；检查三节点Ready、etcd三成员，隔离单成员后其余两个仍可提交API写入。 | 3 master Ready、etcd多数派读写正常、故障撤销后3成员健康；正常删除集群。独立worker/VIP不属于调整后的验收。 |
| 2.1-19 | 准备真实跨子网路由；若测 BGP，提供可控 peer 与路由策略。单纯在同一二层网段改 YAML 不足以验收。 | 从跨子网节点/Pod 验证所选封装、路由和双向流量；撤销路由/BGP 临时配置并删集群。 |
| 2.1-20 | 为 `first-found` 准备只有目标数据网卡或网卡顺序可控的节点；`interface` 使用实际承载节点流量的接口；`can-reach` 指定可路由目标。每种方式各建隔离多节点集群并做跨节点 Pod ping/TCP。 | Calico 实际选中预期 IPv4，Node IP 与可达路由匹配，Pod 流量成功；多网卡节点若 `first-found` 选错接口须记录为不适用，不得仅凭 Node Ready 判通过。 |
| 2.1-21 | 提供两个不同端点：Package Registry 与带镜像的 CRI Registry；最好使用隔离、可记录请求的仓库。 | 建群期间包从 Package Registry、镜像从 CRI Registry 拉取；请求日志证明没有混用；清理两端临时数据。 |
| 2.1-22 | 准备私有 CRI Registry、临时账号、CA 和镜像；以隔离节点下发配置。 | `hosts.toml`/containerd 配置权限正确，CA/认证生效，镜像实际拉取成功；删除凭据及信任文件。 |
| 2.1-29 | 端口冲突与停止 chrony 后 NTP 服务缺失拒绝已通过。代码复核：CLI 时间预检只检查服务 active，不检测真实时钟偏差；NodePreflight只检查执行文件/路径。磁盘不足与时钟偏差尚无现场验收，应先核对预检实现与 case 约定，不能只归因于缺测试 VM。 | 预检应在创建集群前报告对应磁盘/时间问题且不留下 Cluster/Operation；还原快照、磁盘和时间服务。 |
| 2.2-11 | 准备隔离 Agent 证书及至少两个测试 Node 身份；通过 API 测伪造注册、跨节点状态更新、旧 resourceVersion/UID 和合法自身更新。 | 越权请求 403/409、合法请求成功且不改其他 Node；删除临时身份/证书并恢复 Agent。 |
| 2.3-03 | 准备隔离升级集群及可控的中途失败点（step 执行故障，不是创建前校验失败）；记录失败前后 Cluster status、Node、kubeadm-config。 | 使用正式 reset/retry 路径恢复到明确状态，节点可用且无半升级；清理失败 Operation 和集群。 |
| 2.3-05 | 选择可排空的测试 Agent，记录旧 Agent 证书序列号/身份；执行 drain、证书重新签发和 join。 | 新证书序列号/身份发生预期变化，旧 Node 身份更新新 Node 被403拒绝，新 Agent Ready；不声明 X509吊销；恢复节点并删临时证书。 |
| 2.3-07 | 使用能记录 tag 变更的隔离 OCI Registry；创建含固定 digest 的 Operation 后移动测试 tag，再触发 retry。禁止在共享 Registry 重指 tag。 | retry 消费创建 Operation 时锁定的 digest，而非新 tag 目标；删除临时 tag、manifest 和 Registry。 |
| 2.3-09 | 使用 1 master + 2 workers 隔离集群，至少 3 个跨节点业务副本，预留能在节点维护时承载替代副本的容量。PDB 使用 `maxUnavailable: 1` 或等价 `minAvailable: replicas-1`；记录每次 drain 前后的 `currentHealthy`、`desiredHealthy`、`disruptionsAllowed`、Pod 所在节点和 EndpointSlice，并观察 Calico/API 等系统 PDB。若使用 `DoNotSchedule` 拓扑约束，应预期目标节点 cordon 时替代 Pod 暂时 Pending，等 uncordon 后确认 Ready/PDB 恢复再继续下一个节点。轮询每个 NodePort 或稳定 VIP/LB，保留请求目标、时间和结果。 | `disruptionsAllowed=0` 时不删/绕过 PDB；确认 Ready 副本和调度容量，等待预算恢复或让 drain 明确失败，禁止失败后继续停 kubelet。按 master→worker 顺序完成升级；探针零失败或达到预先约定 SLO，最终节点、系统 Pod、业务副本均 Ready。删除测试工作负载/PDB 和集群，并核验节点服务与平台基线。 |

## 3. OCI 本地缓存与 Delivery Policy

| Case | 下次执行前置条件与操作 | 通过标准与清理 |
|---|---|---|
| 2.6-04 | 固定同一 Agent、transport、package digest 和 Package Registry；先清空/记录该测试缓存，再连续执行两次相同包消费，并开启 Registry 请求计数或 Agent 缓存日志。 | 首次读取 Registry，第二次由本地校验缓存命中且内容 digest 不变；删临时缓存/集群。 |
| 2.6-05 | 使用一次性 Agent VM/快照和隔离 Registry；仅篡改该 VM 的目标 cache 文件或 manifest，绝不改共享节点可复用缓存。 | digest 校验明确拒绝损坏缓存，不产生伪成功；恢复快照并清理临时包。 |
| 2.6-06 | 在隔离 Registry 预热同 digest 缓存后，只阻断测试 Agent 到该 Registry 的流量；不要停止共享 Registry 服务。 | 观察并记录缓存命中、回源失败或明确失败语义；恢复网络并清理测试规则。 |
| 2.6-07 | 使用临时节点分别配置 deploy/join CLI 参数与 Package Registry 配置文件，制造明确优先级冲突和权限不足输入。 | 实际选用来源符合文档优先级，文件 owner/mode 符合安全要求；恢复文件并删临时凭据。 |
| 2.6-09 | 保存现有 Delivery Policy；在隔离环境修改允许版本白名单，对白名单内外版本分别执行解析/建群 dry-run。 | 允许版本解析成功、未允许版本在创建前拒绝；恢复原 Policy。 |
| 2.6-10 | 保存 Policy；在隔离环境分别移除必需 slot 和 repository 后，提交 dry-run/临时建群负向请求。 | 服务端指出缺失项且不创建集群/Operation；立即恢复原 Policy。 |
| 2.6-11 | 准备当前候选 bootstrap 与 standalone extension OCI 制品；在独立节点执行 standalone 安装，同时建一个带集群 packagePlan 的隔离集群作对照。 | standalone 消费不改写集群 packagePlan，集群版本/slots 哈希不变；清理扩展和集群。 |

## 4. CLI、UI、Addon 与集成

| Case | 下次执行前置条件与操作 | 通过标准与清理 |
|---|---|---|
| 3-16 | 同 1.3-07/09：提供当前候选离线 manifest 和在线 ReleaseManifest/校验和，分别走两条 `upgrade all` 路径。 | 全组件升级、版本与健康检查一致；保存并按需恢复基线。 |
| 3-17 | 同 1.3-10：为四个独立组件准备匹配版本制品，记录升级前后每个组件版本。 | 只改变目标组件且回滚可用；恢复基线文件。 |
| 3-18 | 同 1.1-02：当前候选 ReleaseManifest、完整依赖制品、可写的隔离目标 Registry。 | `registry sync` 清单全部到位且 digest 一致；删除目标仓库数据。 |
| 3-19 | 启动只用于本轮的 OCI Registry，配置空 namespace 和临时账号；使用独立测试 repository 依次 list/deploy/clean/push/delete。 | 每个命令对目标仓库生效，清理后无测试对象；确认共享 Registry 仓库/标签未变化。 |
| 4-07 | 使用管理员账号登录当前 Console；准备可控的失败 Operation（仅影响隔离集群），并记录 API 状态与任务日志。 | UI 能正确展示成功/失败状态、错误原因和任务步骤；删临时 Operation/集群并退出账号。 |
| 4-08c | 密码登录/改密/禁用已通过。代码复核：当前仅注册 fake_sms，没有真实短信/TOTP provider；真实验证码验收需要先实现或接入真实 provider，再配置凭据及测试收码端。仅补号码不能解决；fake成功不能证明真实短信登录。 | 正确验证码登录成功、错误/过期验证码拒绝；恢复 MFA 配置并删除临时用户/消息。 |
| 4-08d | 准备测试账号及可配置短 TTL 的隔离认证设置；登录签发 token，查询、注销，再等待 TTL 过期查询。 | 有效 token 可用，撤销和到期 token 均拒绝，过期记录按契约清理；恢复 TTL 和账号状态。 |
| 4-11 | 先用 KC 创建集群，再通过另一套隔离 KC 平台按 kubeadm CloudProvider 纳管。将节点 IP 配置为 API 证书 SAN（已有时无需重签），生成以 `https://<节点IP>:6443` 为 server 的独立 kubeconfig；保留 CA 校验，不依赖 `apiserver.cluster.local` DNS。同时准备节点 SSH 权限；无需云厂商账号。 | 纳管、列表、状态同步和解除纳管符合契约；删云端凭据/外部测试资源。 |
| 4-15 | 导出并保存当前 PlatformSetting；准备隔离镜像仓库模板和 Web 终端测试密钥，避免覆盖共享 Registry。 | 设置读取/更新/恢复准确，终端密钥不明文泄漏；恢复设置快照并轮换临时密钥。 |
| 4-19 | 提供当前候选 ReleaseManifest 和 NFS/MetalLB runtime image digest 集；按现有发布设计使用内嵌 YAML，在隔离集群安装。无需另造空 Addon package/chart。 | 实际 Addon 容器的镜像来源/digest 与清单逐项一致；卸载 Addon 并删除测试集群。 |
| 4-20 | 准备可成功安装的 Addon chart，并在隔离集群注入可控的拉取或健康检查失败；单纯非法配置被 400 拒绝不覆盖失败重试。 | 首次失败可见，retry 使用正确 Operation/配置并成功或按契约卸载，最终无残留资源。 |

## 5. Operation 与任务可靠性

| Case | 下次执行前置条件与操作 | 通过标准与清理 |
|---|---|---|
| 5-04 | 在测试 Agent 前放置可控制 watch stream 的代理，或使用专用 watcher 测试服务注入 410/EOF；不要中断共享平台所有 Agent。 | Agent relist 后任务无丢失、无重复执行；移除代理并恢复连接。 |
| 5-05 | 使用真实多节点 Operation，选择有跨 Step 产物依赖的流程；同步采集每个 NodeRef 的 Task 输入/输出和时间戳。 | 下游步骤只在所有上游节点完成后开始，并收到正确输出；删测试集群及任务数据。 |
| 5-07 | 在隔离 Operation 注入一次可恢复瞬时失败，配置 RetryTimes>0，另做耗尽次数负向。 | 自动 retry 创建新 Attempt、最终成功；耗尽后 Operation 明确 Failed；清理注入点和 Operation。 |
| 5-09 | 使用仅限测试的 API 代理，在 Task 终态 PUT 已持久化后丢弃响应；准备可计数副作用。 | 重读终态后不重复副作用，Operation 正确收敛；移除代理并清理测试任务。 |
| 5-10 | 使用能在副作用结果不确定时断连的测试执行器；记录外部副作用计数。 | 状态不确定时 fail-closed，不伪报成功或盲目重放；恢复执行器并清理外部对象。 |
| 5-14 | Operation timeout 已验证。补测 Task/Step deadline：隔离执行器中可控阻塞单个 Step，设短 Task deadline 且 Operation deadline 更长。 | Task 进入预期超时终态，Operation 错误/重试语义符合契约；恢复 deadline 设置并清理任务。 |

## 6. 发布物与架构矩阵

| Case | 下次执行前置条件与操作 | 通过标准与清理 |
|---|---|---|
| 6-02 | 提供当前候选完整 bootstrap、Kubernetes、CRI、CNI、extension 及 NFS/MetalLB runtime OCI 制品；使用匹配 CLI revision 的 bootstrap。现有 Addon 内嵌 YAML，仅发布标准 runtime images，与 scripts/open-packaging/README.md 的契约一致。 | 所有必需 kind/slot 均存在且可消费，Addon 安装成功；保留 digest 报告并删除测试集群。 |
| 6-03 | 提供真实 Release Manifest，包含 package、chart、runtime-image、bootstrap refs/digest/source/version/revision；按现有 schema 验证。 | schema 和全部引用验证通过，无悬空/错误 digest；仅读取共享 Registry。 |
| 6-04 | 提供真实 Release Manifest、checksum 和发布候选 source revision；核对 OCI config labels、digest、version 与真实安装对象。 | 每个 provenance 字段一致；现有 case 不要求签名，不把未执行的签名检查写成通过，节点实际消费 digest 与计划一致；保存验证证据。 |
| 6-05 | 准备可写隔离 Registry，启用不可变 tag/重复发布保护；使用全新测试 repository/tag。 | 重复发布或重指 tag 被拒绝，原 digest 不变；删除测试 repository，不触碰共享仓库。 |
| 6-06 | 准备同一 OCI index 下 amd64/arm64 manifest 和配套制品；架构过滤使用真实双架构 index，原生运行另见6-10。 | 各架构解析到正确子 manifest，错误架构明确拒绝；清理同步目标；不以架构解析代替 arm64 真机运行。 |
| 6-07 | 准备当前候选 Registry sync manifest 或 offline bundle、可写目标仓库和可断网消费节点。 | 同步/import 后目标节点只从目标仓库消费全部 refs；移除目标仓库和断网规则。 |
| 6-08 | 提供完整 qualification Release Manifest、Addon 资产、目标操作系统/架构节点和外部依赖。 | qualification 清单每项有本轮现场证据并汇总通过；清理临时环境与制品。 |
| 6-10 | 提供真实 arm64 主机/VM 和匹配 arm64 OCI 制品；amd64 模拟器不作为硬件路径证据。 | 完成部署、建群、网络和核心操作验收；销毁临时 arm64 VM/集群。 |

## 7. 身份与并发容量

| Case | 下次执行前置条件与操作 | 通过标准与清理 |
|---|---|---|
| 7-01 | 配置隔离 OIDC Provider tenant、客户端凭据、HTTPS callback URL 和测试用户；确认时钟/DNS/TLS 正常。 | 登录、用户映射、错误回调/凭据拒绝符合契约；撤销客户端并删除测试用户。 |
| 7-03 | 先约定并发基线 N、任务类型和资源上限；使用独立集群/Agent 槽位或独立备份目标，同时提交互不冲突的 Operation。 | N 个 Operation 同时运行时无丢失/重复、资源和延迟满足预设阈值；清理任务、备份和集群。 |

## 2026-10-13 再补测后的新增条件

最新状态以 [逐 Case 台账](r42-case-ledger.md) 为准：179 通过、6 部分、7 阻塞、0 失败、2 范围外。上面的行是可复用测试方法，已通过项不再表示当前仍阻塞。

- **隔离 Registry 可自行准备**：HTTP 与自签 CA + htpasswd HTTPS 仓库已在本轮实际搭建并清理，无须改造共享 5003。临时仓库准备真实 OCI 制品后可测试重复同步、bundle 导入、tag 冲突、错误凭据/CA 和缓存故障。自签证书须带正确 SAN；正向校验不使用 skip-verify；凭据文件 0600、工作目录 0700。
- **Package Registry 与 CRI Registry 分开验收**：CRI CA/认证实际拉取成功不替代 deploy/join/Agent 包消费；`1.1-05/06` 本次已补齐 Package 链，`2.1-22` 已闭环。含认证的 containerd config.toml 必须 0600；CA 与 hosts.toml 无秘密内容时可 0644。
- **IPIP 必须验证 underlay**：安全组/虚拟网络应允许 IP protocol 4；BGP 还需 TCP179。发送端 ens3 抓到 IPIP、接收端抓不到时先检查虚拟网络/安全组，Node Ready 和 BGP Established 均不能证明 Pod 数据面通过。IPIP 模式的 Calico Installation 必须启用 BGP。本轮 VXLAN 双向 Pod 流量通过，IPIP underlay 丢包仍未闭环。
- **first-found 的适用节点**：本轮 dev2/dev3 实际 first-found 选择可达数据接口且双向流量通过；dev4 额外 lxdbr0 可被 first-found 选中，不要为通过测试删除共享网桥，改用 interface=ens3/can-reach 或选择网卡顺序可控节点。
- **缓存故障只作用于临时源**：先备份精确 cache 目录与归档 SHA；带唯一标识的 OUTPUT REJECT 只拦临时 Registry 端口。确认好缓存仍可用、坏缓存拒绝，恢复网络后正式 retry 重拉并核对 SHA。结束恢复文件与规则；不要停共享仓库。
- **自动 retry/barrier/响应丢失**：使用真实 shell/NodePreflight 执行器；断开一台 Agent 验证下游未生成；自动失败须确认 attempt0/1 真实终态。终态响应丢失要在上游 PUT 已返回200后只丢响应，再核对副作用次数，不能用整段断网代替。
- **Addon 故障与恢复**：本轮 MetalLB 使用临时不存在的镜像 repository 产生真实 ImagePull/安装失败；补齐实际镜像后走正式 API retry，确认所有容器 Ready，再卸载检查资源清空。不要用 dry-run 或非法参数拒绝替代安装中途失败。
- **Token/PlatformSetting**：短 TTL 验证须同时考虑 clock skew，使用不带管理员 mTLS 的 Bearer 请求证明200→401；结束恢复认证配置、logout 撤销 refresh token、删测试账户。本轮 PlatformSetting 原本为空，轮换测试后仅删除本轮生成的设置；若已有设置，先取得完整受保护备份，包含私钥，不能以脱敏 GET 当回滚备份。
- **并发容量**：本轮验收基线 N=2 个独立单 master 建群，须保存 Operation 起止时间并证明交叠、节点 Ready 和平台 Healthy。不要把单 Operation 多节点执行算成多集群并发容量。
- **正式 Release 与子集资格测试**：本轮 7 个实际制品清单可用于工具测试；不是官方完整 Release。本次已取得并验证真实已发布 v2.0.3（474ba45d）全部110引用、双架构制品以及半离线/离线部署升级。当前 dirty 修复的发布资格、Addon OCI 与 arm64 原生运行仍须分别补齐；不额外把签名设为原 case 的通过条件。需要公网拉取时可使用代理并正确设置 NO_PROXY，代理不能解决制品未发布。

## 2026-10-13 本次续测方法与剩余条件

1. **代理与真实资产**：GHCR 读取/sync 可使用本地7890代理经 SSH remote forward 暴露到测试节点；配置 HTTP_PROXY/HTTPS_PROXY，并在 NO_PROXY 排除内网节点与 Registry。先运行真实 manifest verifier，再同步。fork v2.0.3 的 qualification 资产存在，默认 upstream/canonical 资产仍缺；代理不能解决 manifest unknown/Release404。
2. **CLI 与 bootstrap revision 一致**：使用 Release 配套 kcctl 与 checksum，核对 GitCommit；不要让 rc.34 CLI 消费474ba45d组件归档。bootstrap bundle 必须包含 kubeclipper、etcd、console、registry；export/import 节点需 skopeo、Python/PyYAML，并验证跨机 SHA。Registry base 为host/namespace，避免重复追加 kubeclipper。
3. **隔离平台注册与恢复**：备份原 Agent 配置/证书/二进制及 CLI 配置，先排空所属集群；join 前清除本轮残留 unit 与 enable 链并 daemon-reload/reset-failed，否则 dangling not-found service 也会导致预检拒绝。预检失败仍可能写本地 deploy-config，clean 前必须再次核对目标，不依赖旧配置。
4. **升级恢复与 PDB**：使用严格失败传播的当前补丁测试构建；旧 rc.34 的升级命令可能隐藏 kubeadm 失败。本次单节点维护窗口临时调整 Calico PDB，使 drain 可进行，结束恢复 maxUnavailable=1；单节点维护不声明无中断。正式 reset/retry 后同时核对 Operation、节点 kubelet、实际 apiserver 镜像与 plan SHA。
5. **证书重新签发**：正式 drain→join 核对 NodeID/CN/serial 与新 Agent Ready；旧身份更新新 Node 应403。这里不等于旧 X509 证书被 CA 吊销，若产品要求 CRL/OCSP，需要另行实现与验收。
6. **真实 OIDC 可自建**：使用官方 Dex 实例、专用客户端/静态测试用户，配置 callback、issuer、scopes、映射；验证错误密码、code replay、平台 token 访问。此次隔离 IdP HTTP，仅用于认证流程；生产 TLS/企业 IdP 另需对应环境。
7. **故障证据**：Task 事实篡改只操作测试 Operation 的精确 etcd key；payload 与固化计划应 JSON 语义一致，冲突应 InvalidExecutionFacts、取消 Pending 且无副作用。UI 故障用真实退出码/日志验证，预期 Failed Operation 不计为 case 失败。
8. **时间预检**：可短暂停止隔离节点 chrony 验证 NTP 服务缺失，finally 恢复；磁盘不足与真实时间偏差需一次性 VM，不填共享根盘、不改共享时钟。

### 当前还未完全通过的6项（R43后）

当前汇总：通过185、部分4、失败0、阻塞2、范围外3。已关闭条目的新增证据见 [R43 报告](status-2026-10-13-r43.md)。

| Case | 当前状态 | 还缺什么 / 是否可继续跑 |
|---|---|---|
| `1.3-09` | 阻塞 | 实际 fork v2.0.3 Release 可下载，CLI --version 下载器固定 upstream kubeclipper/kubeclipper，该 upstream v2.0.3 Release 不存在。需 upstream 正式发布或明确支持可配置 Release 源，不能将本地 --manifest 算在线升级。 |
| `2.1-19` | 部分 | Calico IPIP BGP 配置缺陷已修复；前轮发送端捕获 protocol4、接收端为0，跨节点 Pod 不通。需 underlay 放行 IP protocol4/TCP179，并提供跨子网可路由拓扑后补数据面。 |
| `2.1-29` | 部分 | 端口冲突与停止 chrony 后 NTP 服务缺失拒绝已通过。代码复核：CLI 时间预检只检查服务 active，不检测真实时钟偏差；NodePreflight只检查执行文件/路径。磁盘不足与时钟偏差尚无现场验收，应先核对预检实现与 case 约定，不能只归因于缺测试 VM。 |
| `3-16` | 部分 | 离线 upgrade all --manifest 已完整通过1.3-07；在线 --version 仍受1.3-09 upstream Release 下载源限制。 |
| `4-08c` | 部分 | 密码登录/改密/禁用已通过。代码复核：当前仅注册 fake_sms，没有真实短信/TOTP provider；真实验证码验收需要先实现或接入真实 provider，再配置凭据及测试收码端。仅补号码不能解决；fake成功不能证明真实短信登录。 |
| `6-10` | 阻塞 | 已拿到真实 arm64 制品；仍无 linux/arm64 原生节点。需真机/原生 VM 后部署、建群及网络验证。 |

## 1.1-01 当前允许的 fork GHCR 测试入口

当前可使用 v2.0.3 元数据的最新 fork 资格候选制品，不必等待官方默认仓库补齐再测试。使用该 Release 配套的 Linux kcctl，先校验 checksum 与 `kcctl version` 的 GitCommit 为 `b4a5886e8e897528fd394a6fc7ef358162d444c3`。其他版本必须改为其对应的 qualification 仓库，不能混用 CLI/归档 revision。

```sh
# 参数中的占位符替换为本轮隔离节点与已有 SSH 密钥。
./kcctl-linux-amd64 deploy \
  --server <SERVER_IP> \
  --agent <AGENT_IP> \
  --pk-file <SSH_PRIVATE_KEY_FILE> \
  --package-registry ghcr.io/lixd/kubeclipper/qualification-b4a5886e8e897528fd394a6fc7ef358162d444c3 \
  --ip-detect interface=ens3 \
  --node-ip-detect interface=ens3
```

公网受限时使用可达代理设置 HTTP_PROXY/HTTPS_PROXY；NO_PROXY 至少包含部署节点 IP、内网 Registry、localhost 与127.0.0.1。SSH远端转发只绑定回环地址，测试结束关闭。`ens3` 仅适用于本轮主机，其他环境使用实际数据网卡。记录 fork 直装结果与官方默认路径缺失为两个独立观察；本次文档更新没有新增直装执行证据，不回算历史统计。

## 1.1-10 公共 CA 测试说明

这是证书信任方式测试：仓库使用系统默认信任的公共 CA 签发的 HTTPS 域名证书，客户端无需另外指定自签 CA 文件就能验证身份。它不要求仓库向公网开放，也不要求关闭账号密码认证。

- `1.1-05`：自建 CA，客户端通过 CA 文件验证；这条已经通过。
- `1.1-10`：由系统默认信任的公共 CA 签发，客户端无需额外 CA 文件；这条尚缺对应仓库域名/证书。
- 把自建 CA 手工加入系统信任库，可以验证系统信任池读取，但不能证明证书由公共 CA 签发。

## 本轮执行范围

公共 CA 验证合并到 fork GHCR 在线部署，不另准备域名/仓库；3 master HA使用三台现有主机。外部集群允许由源KC创建，但在独立目标KC通过CloudProvider纳管，不把源平台已有Cluster列表当作纳管证据。保留原Agent配置、证书、CLI配置与二进制，解除目标纳管后恢复源Agent，再由源KC正常删除集群。发布仅限fork，arm64原生运行保持未测。

### R43 纳管补测进展（2026-10-13）

- 已为 `r43-three-master` 重新生成以 `https://172.16.131.208:6443` 为 API 地址的独立 kubeconfig；API 证书包含该 IP SAN，保留 CA 校验，`/readyz` 返回 `ok`。不要把私钥、token 或完整 kubeconfig 写入测试报告。
- 首次外部纳管通过连接与 precheck 后，在下发 Agent 证书时失败：服务端误读 CLI 的 `/root/.kc/pki/ca.crt`。修复为读取 `/etc/kubeclipper-server/pki/ca.crt`，并补充真实 CA 签名与 IP SAN 单元测试。当前修复 revision 为 `b4a5886e8e897528fd394a6fc7ef358162d444c3`，修复候选的完整纳管、重新同步和解除纳管复测已通过；解除后源集群仍可实际API写读删。

- R43 纠正 Addon 前置条件：现有 NFS/MetalLB YAML 按设计内嵌，Actions 发布其标准 runtime images；不应因没有独立 Addon package/chart 阻塞 `4-19`、`6-02`。历史状态表保留当时判断，最终状态以本轮当前候选真实镜像消费证据为准。

### R43 发布及镜像 Registry 配置

- 测试候选固定为 `b4a5886e8e897528fd394a6fc7ef358162d444c3`；使用该 qualification 的 ReleaseManifest 与配套二进制，不能把旧候选的发布验收结果套到当前代码。
- Package Registry 可带 namespace；创建集群使用的镜像 Registry `host` 仅接受裸主机及端口，不包含 scheme/path。镜像重用必须逐个核对 digest 与当前候选清单一致；实际 Pod imageID 也应复核。本轮61个amd64制品均完成正式同步，重同步61 skipped，49个运行时镜像及27个实际容器digest均匹配。
- NFS数据面测试需可访问的真实NFS导出；使用独立导出目录、StorageClass、PVC和Pod完成写读，先删PVC并等待PV回收，再卸载CSI，最后移除测试导出；不覆盖原导出。

- 正常删群后另核对CNI接口、网络命名空间及Kubernetes/CNI防火墙链。只清理已确认属于测试Kubernetes的残留，保留SSH、共享Registry和其他防火墙规则；本轮发现正常删除仍有这类宿主机残留，已额外清理。
