# R42 全量 Case 执行台账

本台账逐项对应 `core-feature-checklist.md` 的 194 个 Case。R42 结果必须来自本轮新证据；R41 状态只作历史参考。执行状态使用：`待执行`、`通过`、`失败`、`部分`、`阻塞`、`范围外`。对通过/失败/部分/阻塞的条目，结束时必须补简短证据或原因。

## 执行计划

1. 基线盘点三台主机、平台、Kubernetes、Registry、SSH 与代理；确认独立 Registry 的保留边界。
2. 清理旧 KubeClipper 测试环境，从共享 HTTP Package Registry 部署候选 HA 平台；运行 Go/E2E、发布策略与打包脚本检查。
3. 按 Case 清单依次执行平台拓扑、CLI、Kubernetes 创建/升级/扩缩/删除、备份恢复、PackagePlan/OCI、用户/RBAC/API/UI、Addon 和 Operation 场景；每项记录状态及证据，缺少依赖时标阻塞。
4. 为单节点、离线/在线、不同运行时/CNI、代理模式和跳过 CNI 场景切换隔离集群；每次使用正常删除或明确失败清理。
5. 汇总 194 条结果，清理临时对象与测试残留；逐机核验服务、Kubernetes/CNI 状态、SSH 与共享 Registry。

## Case 台账

注：rc.34 初测、后续补丁复测和本次续测是不同快照；逐行状态只按对应现场证据更新，不回算初测计数。截至 2026-10-13，本台账 194 行当前汇总为：通过 187、部分 2、失败 0、阻塞 2、范围外 3（含 R45 补测与用户确认的范围调整）。各未闭环 Case 的下次执行前置条件、验收点和清理要求见 [R42 Case 前置条件矩阵](./r42-case-prerequisites.md)，按 Case ID 查找。

| Case | 功能 | R41 历史状态 | R42 本轮状态 | R42 证据 / 阻塞原因 |
|---|---|---:|---|---|
| 1.1-01 | 在线部署：GHCR 直装（当前允许 fork 制品） | ✅ | 通过 | R43（2026-10-13）：用户允许 fork 制品。当前候选 b4a5886e 配套 kcctl 从 fork qualification GHCR 直接部署成功，系统 CA 信任链校验开启，无额外 CA 文件；纳管后 doctor 19/0/0。官方默认 v2.0.3 的 manifest unknown 尚未修复，不把 fork 成功写成官方路径成功。 证据见 [R43 报告](status-2026-10-13-r43.md)及 evidence/r43。 |
| 1.1-02 | 半离线：ghcr → `kcctl registry sync` → 本地仓库 → deploy | ✅ | 通过 | R42 续测（2026-10-13）：真实 GitHub Release v2.0.3（474ba45d）GHCR 源经正式 registry sync 复制 61 个 amd64 制品至隔离仓库；配套 Release kcctl 从目标仓库完整 deploy，doctor 15/0/0。 证据见 r42-next-2026-10-13.json/.log。 |
| 1.1-03 | 纯离线：bundle export → 拷贝 → import 进仓库 → deploy | ✅ | 通过 | R42 续测（2026-10-13）：真实 4 个 bootstrap 组件 export→跨机拷贝 SHA 校验→import；CLI 与部署节点公共 IPv4 出口阻断、无 IPv6 默认路由时完整 deploy，doctor 15/0/0；规则已撤销。 证据见 r42-next-2026-10-13.json/.log。 |
| 1.1-04 | 私有仓库 http | ✅ | 通过 | R42：三机部署和真实建群均使用 HTTP Package Registry 172.16.131.146:5003；制品扫描、拉取和 Agent 消费成功。 |
| 1.1-05 | 私有仓库 https + 自签 CA | ✅ | 通过 | R42 续测（2026-10-13）：严格自签 CA HTTPS Registry（skipTLS=false）完整 deploy、join 与真实 1M/1W 建群通过；Registry 记录三个客户端实际认证拉取，节点 Ready。 证据见 r42-next-2026-10-13.json/.log。 |
| 1.1-06 | Package Registry 账号密码认证（deploy/join/agent 消费侧） | ✅ | 通过 | R42 续测（2026-10-13）：真实 htpasswd Package Registry 完整 deploy/join/Agent 包消费通过；使用专用账号、0600 密码文件，1M/1W 建群 Operation Succeeded；保留前轮错误密码拒绝证据。 证据见 r42-next-2026-10-13.json/.log。 |
| 1.1-07 | `registry sync` 重复同步幂等 | ✅ | 通过 | R42 2026-10-13 再补测：隔离 HTTP Registry 同步 7 个真实 OCI 制品：首次 7 copied/0 skipped，第二次 0 copied/7 skipped；全部 digest 不变。 |
| 1.1-08 | 离线 bundle 重复 import 幂等 | ✅ | 通过 | R42 2026-10-13 再补测：真实 export 的 bundle 连续 import 两次；7 个制品校验和及导入后的选定架构 digest 均匹配。 |
| 1.1-09 | 错误仓库地址、凭据、CA 或缺失制品 | ✅ | 通过 | R42 2026-10-13 再补测：隔离 Registry 的错误地址、密码、CA、缺制品分支均 exit=1；deploy 在预检拒绝，无半部署；密码未写入日志。 |
| 1.1-10 | HTTPS 公共 CA 信任路径（合并至 1.1-01） | ✅ | 范围外 | R43 用户确认合并至 GHCR 直装，不单独建设公共 CA 私有仓库；当前 fork GHCR 直装已验证系统默认公共 CA 信任链。 证据见 [R43 报告](status-2026-10-13-r43.md)及 evidence/r43。 |
| 1.2-01 | 单 server + 多 agent | ✅ | 通过 | R42：clean 后单 server + 2 agents 拓扑 Healthy；本次另验证 3 server + 3 agents。 |
| 1.2-02 | server 与 agent/k8s 节点混部同机 | ✅ | 通过 | R42：三台主机均同时运行 kc-server、kc-agent、kc-etcd、kc-console；doctor 25/25。 |
| 1.2-03 | **多 server HA（3× server，etcd 奇数集群）** | ✅ | 通过 | R42：3 server + 3 etcd + 3 agents 部署；doctor 3/3 server、3/3 etcd、3/3 agent；逐台 server/etcd 滚动重启均恢复 25/25。 |
| 1.2-04 | console 组件部署与访问 | ✅ | 通过 | R42：三台 console 的 HTTP 根页和 /version 均返回 200。 |
| 1.2-05 | 单 server + 单 agent | ✅ | 通过 | R42：clean 后部署 1 server（sh-dev-2）+1 agent（sh-dev-3）；Healthy，doctor 15 passed、0 warning、0 failed。 |
| 1.2-06 | server 与 agent 分离部署 | ✅ | 通过 | R42：前序 clean 后部署为 server/agent 分离拓扑，Healthy 且 2/2 agents；随后清理并切换至 HA 拓扑。 |
| 1.2-07 | HA server/etcd 单点故障与滚动重启 | ✅ | 通过 | R42：单停 dev4 kc-server 或 kc-etcd 时 dev2 /healthz=200、node list 3 条；恢复后健康；逐台滚动重启 3 server 和 3 etcd 均恢复 doctor 25/25。 |
| 1.3-01 | 预检失败引导（不可达/缺包报错） | ✅ | 通过 | R42 2026-10-13 再补测：结合前轮 SSH 不可达，本轮不存在的 OCI repository 明确报缺制品并 exit=1；节点未进入部署。 |
| 1.3-02 | etcd 冷启动竞态（写探针+重试） | ✅ | 通过 | R42 续测（2026-10-13）：隔离 Server 先启动、etcd 延迟 5 秒后恢复；另在真实 deploy 中阻断 CLI→etcd，日志记录 write probe 超时与 waitEtcdReady 重试，连接恢复后 deploy 成功、doctor 15/0/0。 证据见 r42-next-2026-10-13.json/.log。 |
| 1.3-03 | clean --all 后重 deploy 幂等 | ✅ | 通过 | R42：执行 clean --all 后从共享 HTTP Registry 重部署，平台 Healthy，3/3 nodes 注册。 |
| 1.3-04 | 不 clean 直接重复 deploy 的行为与平台安全性 | ✅ | 通过 | R42：HA 平台运行时直接重复 kcctl deploy，在 kc-etcd 预检以 service already exists 拒绝；未进入部署阶段。 |
| 1.3-05 | `kcctl join` 独立纳管新节点 | ✅ | 通过 | R42：kcctl join --agent 172.16.131.230 完成独立纳管；平台 Healthy，Agent 数从 1 增至 2。 |
| 1.3-06 | `clean --all --force --deploy-config` 异常恢复 | ✅ | 通过 | R42：停止 kc-server 后执行 kcctl clean --all --force --deploy-config，返回 clean successful。 |
| 1.3-07 | **`kcctl upgrade all --manifest` 平台离线升级（OCI manifest + 内网 Registry）** | ✅ | 通过 | R42 续测（2026-10-13）：完整 bootstrap bundle 导出/传递/import 后，阻断三机公共出口，正式 upgrade all --manifest 从真实 rc.33 升至 rc.34；Server/2 Agents/Console/kcctl 全完成，doctor 17/0/0、节点身份保持。 证据见 r42-next-2026-10-13.json/.log。 |
| 1.3-08 | `kcctl doctor` | ✅ | 通过 | R42：kcctl doctor 25 passed、0 warnings、0 failed。 |
| 1.3-09 | **`kcctl upgrade all --version` 在线升级（ReleaseManifest 下载）** | ✅ | 阻塞 | R42 续测（2026-10-13）：实际 fork v2.0.3 Release 可下载，CLI --version 下载器固定 upstream kubeclipper/kubeclipper，该 upstream v2.0.3 Release 不存在。需 upstream 正式发布或明确支持可配置 Release 源，不能将本地 --manifest 算在线升级。 |
| 1.3-10 | `kcctl upgrade server/agent/console/kcctl` 组件独立升级 | ✅ | 通过 | R42 续测（2026-10-13）：真实 rc.33 基线通过正式 CLI 分别升级 Server、2 Agents、kcctl 至 rc.34；Console 正式独立升级完成；版本/revision 与日志齐全。手工部署补丁另列，不计本项。 证据见 r42-next-2026-10-13.json/.log。 |
| 1.3-11 | SSH key/password、非 root sudo 与自定义端口 | ✅ | 通过 | R42：临时 `r42-sshcase` 用户以密码、NOPASSWD sudo 和 SSH 2222 端口在 dev2/dev4 完成 join；平台 3/3 Agent Healthy。测试账户、sudoers、sshd 配置和 2222 端口均已清理。 |
| 1.3-12 | 初始化管理员密码 | ✅ | 通过 | 补丁构建现场复测：重编 kcctl 提交弱口令，服务以固定“password format error”拒绝，输出不含测试输入串；与 4-16 同一缺陷。 |
| 2.1-01 | 1 master + 2 worker | ✅ | 通过 | R42：r42-m1w2 使用 1 master + 2 workers 建群；CreateCluster 15 步和 SyncKubeConfig 均 Succeeded，三节点 Ready。 |
| 2.1-02 | 单节点（master 兼 worker）最小规格 | ✅ | 通过 | R42：sh-dev-3 单节点集群 v1.35.8/containerd 1.7.29/Calico 3.29.6 Running；节点 Ready，API readyz ok。 |
| 2.1-03 | 3 master HA（0 worker；当前范围不含独立 worker/VIP） | ✅ | 通过 | R43 按用户确认的3 master/0 worker范围补测：三节点 Ready、etcd三成员；隔离单成员2379/2380，其余两成员健康且真实API写入成功；撤销规则后三成员恢复健康；正常删除集群。源平台保留原基线，当前候选独立验收见6-08。 证据见 [R43 报告](status-2026-10-13-r43.md)及 evidence/r43。 |
| 2.1-04 | 版本矩阵 v1.35.8 / v1.36.4 / v1.37.0 | ✅ | 通过 | R42：真实建群覆盖 v1.35.8 和 v1.36.4；随后完成 v1.36.4→v1.37.0 升级，三个目标版本均有现场证据。 |
| 2.1-05 | CRI containerd 1.7.29 / 2.2.4 | ✅ | 通过 | R42：v1.35.8 集群使用 containerd 1.7.29；v1.36.4/v1.37.0 场景使用 containerd 2.2.4，两种运行时均实测。 |
| 2.1-06 | CNI calico v3.29.6 / v3.31.5 | ✅ | 通过 | R42：v1.35.8 集群使用 Calico v3.29.6；v1.37.0 场景使用 Calico v3.31.5，两种版本均实测。 |
| 2.1-07 | 镜像/物料 100% 来自指定仓库（离线保证） | ✅ | 通过 | R42：对 dev3/dev4 屏蔽公网出口、保留内网 Registry 后，离线建群并从 v1.36.4 升级到 v1.37.0 成功；packagePlan 中 Kubernetes、Calico、CRI、extension refs 均指向 `172.16.131.146:5003`，节点 Ready。 |
| 2.1-08 | proxyMode ipvs | ✅ | 通过 | R42：实际 kube-proxy ConfigMap mode=ipvs，单节点工作负载就绪。 |
| 2.1-09 | proxyMode iptables | ✅ | 通过 | R42：实际 kube-proxy ConfigMap mode=iptables；单节点 Ready、系统 Pod 全部 Running、readyz ok。 |
| 2.1-10 | 创建集群时拒绝支持矩阵外版本 | ✅ | 通过 | R42：创建支持矩阵外 Kubernetes v9.99.99 被拒绝，HTTP 400，未创建集群。 |
| 2.1-11 | 网络自定义（pod/service 网段、DNS 域） | ✅ | 通过 | R42：kubeadm-config 持久化 podSubnet=172.26.0.0/16、serviceSubnet=10.97.0.0/12、dnsDomain=r42.local。 |
| 2.1-12 | apiserver 对外发布（cert-sans / external-domain / external-ip / external-port） | ✅ | 通过 | R42 补测：`r42-api-publish` 保存 external IP `172.16.131.146`、domain `r42-api.example`、两种 port `6443`；apiserver 证书 SAN 含 `r42-extra.example`、外部 IP 与 domain；CA 校验后经 IP 和 `--resolve` 域名请求 `/readyz` 均返回 `ok`。 |
| 2.1-13 | Kubernetes feature-gates 透传 | ✅ | 通过 | R42：kubeadm-config 的 API server/controller/scheduler 包含 RotateKubeletServerCertificate=true。 |
| 2.1-14 | untaint-master（master 允许调度） | ✅ | 通过 | R42：untaint-master 集群中 master 节点 taints 为 none。 |
| 2.1-15 | `--only-install-kubernetes-component` 跳过 CNI | ✅ | 通过 | R42：--only-install-kubernetes-component 创建成功；CreateCluster 步骤无 Calico 安装步骤，CoreDNS Pending、节点 NotReady，符合跳过 CNI 状态。 |
| 2.1-16 | 自带 CA（ca-cert / ca-key 复用已有根证书） | ✅ | 通过 | R42：使用临时自签 CA 建群；外部 CA 与节点 /etc/kubernetes/pki/ca.crt 的 SHA-256 指纹一致，集群 Ready。 |
| 2.1-17 | kubeadm preflight ignore 定制 | ✅ | 通过 | R42：NumCPU ignore 配置进入 renderKubeadmConfig Task 输入；CreateCluster Operation 成功。 |
| 2.1-18 | Calico 默认 VXLAN 网络模式 | ✅ | 通过 | R42：Calico Installation 的 IPPool encapsulation=VXLAN、BGP=Disabled，Calico v3.29.6 Ready。 |
| 2.1-19 | Calico IPIP/BGP 或 cross-subnet 网络模式 | ✅ | 部分 | R42 续测（2026-10-13）：Calico IPIP BGP 配置缺陷已修复；前轮发送端捕获 protocol4、接收端为0，跨节点 Pod 不通。需 underlay 放行 IP protocol4/TCP179，并提供跨子网可路由拓扑后补数据面。 |
| 2.1-20 | Calico IPv4 自动探测（first-found/interface/can-reach） | ✅ | 通过 | R42 2026-10-13 再补测：dev3/master + dev2/worker 的 first-found/VXLAN 创建成功，两节点 Ready；双向 Pod ping 各 3 包零丢失，双向 HTTP 200。结合前轮 interface/can-reach 实测。dev4 的 lxdbr0 多网卡误选仍须显式 interface=ens3。 |
| 2.1-21 | 集群镜像 Registry 与 Package Registry 分离配置 | ✅ | 通过 | R42 2026-10-13 再补测：真实集群包计划来自 172.16.131.146:5003，Kubernetes/Calico 运行时镜像来自独立 172.16.131.230:5105；15 步 CreateCluster 成功、节点 Ready，仓库实际请求对应各自用途。 |
| 2.1-22 | 私有 CRI Registry 配置下发 | ✅ | 通过 | R42 2026-10-13 再补测：自签 CA + htpasswd 私有 CRI Registry 下发到两节点；CA 及 hosts.toml 生效、无 skip_verify，实际 netshoot Pod 拉取运行，Registry 记录 401 challenge→授权成功。修复后含密码 config.toml 两节点均 0600。 |
| 2.1-23 | 集群真实断网创建 | ✅ | 通过 | R42：在 dev3/dev4 对所有公网出口设 REJECT、保留 loopback/LAN 后真实创建离线集群并完成 v1.36.4→v1.37.0 升级；操作成功，节点和系统 Pod Ready。隔离规则已移除。 |
| 2.1-24 | 1 master + 1 worker 最小多节点规格 | ✅ | 通过 | R42：前序真实 1 master + 1 worker v1.37.0/containerd 2.2.4/Calico v3.31.5 建群并删除；本轮另跑 1M+2W。 |
| 2.1-25 | 集群在线安装 | ✅ | 通过 | 初测及首次代理尝试因出口/NO_PROXY 配置失败，均已取消并正常删除集群。用户确认使用本地 127.0.0.1:7890 后，经 SSH loopback 反向隧道和临时 systemd 代理设置复测：CreateCluster Operation 04cda3ff-2098-4eb5-b3a9-925f754e5e5e、SyncKubeConfig Operation sync-kubeconfig-79614bd0-7e9d-4d10-9305-5fda08a14c4f 均 Succeeded；sh-dev-3/4 Ready，Calico、CoreDNS Running，registry.k8s.io 与 Quay 镜像拉取成功。集群已正常删除，临时代理配置已清理。 |
| 2.1-26 | 节点已被其他集群占用或 Master/Worker 重复 | ✅ | 通过 | R42：对已占用节点提交第二集群 dry-run，HTTP 400 reason=some nodes in used or disabled；无新对象。 |
| 2.1-27 | Master/Worker 跨 Region | ✅ | 通过 | R42：将 dev4 Agent 临时加入 `r42-b`，提交 default master + r42-b worker 的建群请求，API 返回 HTTP 400 `nodes belongs to different region` 且未创建 Cluster/Operation；随后 dev4 已恢复 default。 |
| 2.1-28 | Pod/Service CIDR 非法、重叠或与主机网络冲突 | ✅ | 通过 | R42：重叠 CIDR 的建群请求在创建前被拒；另测非法 CIDR/版本组合未写入集群对象。 |
| 2.1-29 | 端口、磁盘、时间同步、主机名等集群预检失败 | ✅ | 通过 | R45（2026-10-13，86a23bec）：修复NTP只查active而未验证同步，补部署/建群磁盘预检与建群首步只读检查。真实64MB文件系统（含自定义etcd目录）拒绝；未同步chronyd、真实约60s修正量、服务缺失均拒绝；非法UTS主机名拒绝；正式TIME-LAG对KVM guest真实+60/-60秒偏差拒绝、恢复后通过。真实Agent CommandStep执行器传播失败；实际kubeadm预检拒绝6443占用。原平台doctor19/0/0、共享Registry正常，测试VM/服务/挂载清理。更正：原代码已有独立TIME-LAG检查，此前漏看。见[R45报告](status-2026-10-13-r45.md)。 |
| 2.1-30 | 创建中断后的 retry 或安全删除 | ✅ | 通过 | R42：在线建群镜像拉取失败后取消 Operation，集群转为 InstallFailed；正常 kcctl delete cluster 成功清理。 |
| 2.1-31 | 创建成功后的固定健康验收 | ✅ | 通过 | R42：1M+2W 三节点 Ready、19 个系统 Pod Running，apiserver /readyz check passed。 |
| 2.1-32 | IPv4/IPv6 dual-stack 集群网络 | ➖ | 范围外 | R42：所有节点仅有 IPv4 可路由地址及 IPv6 link-local；缺少可路由 IPv6 underlay，按测试矩阵前置条件标为范围外。 |
| 2.1-33 | 离线建群缺省 `imageRegistry` 创建前拒绝 | ✅ | 通过 | R42：offline=true 且缺省 imageRegistry 的建群 dry-run 返回 HTTP 400，明确要求显式 Registry。 |
| 2.2-01 | worker 添加（含 packagePlan 不变性） | ✅ | 通过 | R42：真实 RemoveNodes 后 AddNodes 成功，回到 1M+2W；add 前后 status.packagePlan 哈希一致。Add operation d5085b8a-7f07-48b8-a7ad-9d844d8281c1。 |
| 2.2-02 | worker 移除（含不可 drain 容错） | ✅ | 通过 | R42：dev4 上单副本工作负载配 `PDB minAvailable=1`（disruptionsAllowed=0）后执行 RemoveNodes `c0993b4b-4172-4f99-9e70-b309d5c5f1da`，操作最终 Succeeded、节点及 kubelet/containerd 清理；替代 Pod 因集群无剩余 worker 保持 Pending，未将其算作业务可用性通过。测试 Deployment/PDB 已删除。 |
| 2.2-03 | **master 添加 / 移除** | ✅ | 通过 | R42：将 worker 转 master 后用 AddNodes 增加第三个 master，3 个 control-plane 节点及 etcd Pod 均 Ready/Running；再 RemoveNodes 移除该 master，剩余 2 个 master 和 etcd 正常。随后正常删除测试集群。 |
| 2.2-04 | master↔worker 角色转换（convertNodes） | ✅ | 通过 | R42：dev2 worker→master→worker 两个 ConvertNodes Operation 均 Succeeded；最终 1M+2W、三节点 Ready。 |
| 2.2-05 | 节点 disable / enable | ✅ | 通过 | R42：按 Node ID disable=200，禁用节点加入集群 dryRun 被拒；enable=200 后 AddNodes 成功。 |
| 2.2-06 | 节点失联后操作收敛（agent down） | ✅ | 通过 | R42：dev3 Agent 停止期间提交 BackupCluster，Operation 保持 Running；恢复 Agent 后 Operation Succeeded。另在 Agent 离线且备份 Operation Running 时重启 dev2 kc-server，服务恢复、Agent 重连后 Operation Succeeded。 |
| 2.2-07 | agent 节点注销（`DELETE /nodes/{name}`） | ✅ | 通过 | R42：DELETE /nodes/{id} 返回 200 且节点记录消失；重启 sh-dev-4 kc-agent 后同一节点重新注册。 |
| 2.2-08 | Worker 添加时复用原 `status.packagePlan` | ✅ | 通过 | R42：Remove/Add worker 前后 status.packagePlan 哈希一致。 |
| 2.2-09 | Master 添加后 etcd/control-plane quorum | ✅ | 通过 | R42：dev2 转 master 后 Kubernetes etcd member list 显示 dev2/dev3 两个 started 成员。 |
| 2.2-10 | Master 移除后的 etcd 成员与 VIP 收敛 | ✅ | 通过 | R42：dev2 master 转回 worker 后 etcd 回到一个 started member；未发现 169.254.169.100 VIP 残留。 |
| 2.2-11 | Agent 注册身份与 Node 状态更新保护 | ✅ | 通过 | R42 2026-10-13 再补测：真实 dev2 Agent 证书请求：跨节点 GET/status PUT、伪造注册均 403；合法自身 status PUT=200；旧 resourceVersion/UID=409，无伪造 Node。 |
| 2.2-12 | Node Lease、Ready/Unknown 与重连恢复 | ✅ | 通过 | R42：停 dev2 kc-agent 后 Node Ready 条件转 Unknown；重启 Agent 后回到 AgentReady，doctor 25/25。 |
| 2.2-13 | Region 归属、列表和同 Region 调度约束 | ✅ | 通过 | R42：节点列表确认 dev4 临时属于 `r42-b`，跨 Region 建群被 HTTP 400 拒绝；恢复后 3 个 Agent 均显示 `default`，平台 Healthy。 |
| 2.3-01 | 真实滚动升级 1.36.4→1.37.0（master→worker drain） | ✅ | 通过 | R42 补丁复测：新增 kubelet 重启后的连续稳定就绪门禁；Operation `2ed1c3b0-1fa6-48a3-a6be-a016c06d9552` 六步全部 Succeeded，master 与 worker 均 Ready v1.37.0，kubeadm-config 记录 v1.37.0，`/readyz=ok`，系统 Pod 全 Ready。 |
| 2.3-02 | 升级中途失败 → op retry | ✅ | 通过 | R42：worker drain 首次因 master kubelet 重启造成的 API 短暂不可用而失败；`kcctl operation retry e93a45d9-d8a1-40af-a90c-c81c666cb665` 后失败 Task 重试成功、Operation 收敛。另有后续稳定性门禁复测 `2ed1c3b0-1fa6-48a3-a6be-a016c06d9552` 全步骤通过。 |
| 2.3-03 | 升级失败 → 集群状态恢复（reset status） | ✅ | 通过 | R42 续测（2026-10-13）：补丁测试构建上移走测试集群 admin.conf，真实 kubeadm 步骤失败、Upgrade Operation Failed/StepFailed；恢复文件并正式 reset status→Running，再正式 retry 成功、节点及 apiserver 实际 v1.37.0。 证据见 r42-next-2026-10-13.json/.log。 |
| 2.3-04 | **集群证书更新（/certification）** | ✅ | 通过 | R42：`/certification` Operation `1f1c1bab-9ccc-480d-bbb0-54d0316a96bf` Succeeded；apiserver cert serial 从 `40F3990A964B78B0` 变为 `4C8BB830D4485CE9`，有效期到 2027-10-12；节点 Ready、系统 Pod Running。 |
| 2.3-05 | agent 证书重新签发 | ✅ | 通过 | R42 续测（2026-10-13）：正式 drain Agent→join 重新签发；NodeID/CN 更新，证书 serial 从 75212C4D96631F68 至 01DA5409D87EB423，新 Agent Healthy；旧身份更新新 Node 返回403。未声称 X509/CRL 吊销。 证据见 r42-next-2026-10-13.json/.log。 |
| 2.3-06 | 升级前后 `packagePlan` 变更边界 | ✅ | 通过 | R42 补丁复测：升级只指定 Kubernetes v1.37.0；升级后 packagePlan slots 为 k8s v1.37.0、containerd 2.2.4、Calico v3.31.5、extension v1，OCI digest/ref 均可见，未新增或丢失其他 slot。 |
| 2.3-07 | Registry tag 变化后 Operation retry 仍使用原 digest | ✅ | 通过 | R42 续测（2026-10-13）：真实 Upgrade Operation 失败后，临时仓库将 v1.37.0 tag 指向 v1.36.4 digest；正式 retryGeneration=1 Succeeded，固化 plan SHA 不变，节点及 apiserver 实际 v1.37.0。tag/PDB 已恢复。 证据见 r42-next-2026-10-13.json/.log。 |
| 2.3-08 | 同版本、降级、跨越不支持版本升级拒绝 | ✅ | 通过 | 补丁构建现场 API dry-run 复测：当前版本 v1.37.0 返回 HTTP 400；降级到 v1.36.4 返回 HTTP 400。跨越不支持版本在 R42 初测也已由 API 返回 HTTP 400。失败子场景均拒绝，未创建升级 Operation。 |
| 2.3-09 | Master/Worker 滚动顺序与业务可用性 | ✅ | 通过 | R42 补丁构建复测：1 master + 2 workers 从 v1.36.4 滚动升级到 v1.37.0，UpgradeCluster Operation `394db9b7-10b7-42d2-b6cb-9151df2801de` 的 9 步全部 Succeeded。Operation 窗口 NodePort 轮询 1,973/1,973 成功（0 失败）；末态三节点 Ready/v1.37.0，系统 Pod 和业务副本全 Ready，三个 EndpointSlice 地址 ready，PDB `maxUnavailable=1` 且 `disruptionsAllowed=1`。worker-4 drain 遇到一次 PDB 拒绝后按 5 秒间隔重试，drain 在 41 秒内成功，未绕过 PDB。集群已正常删除；三机 KubeClipper 基线已恢复并通过 doctor。 |
| 2.4-01 | 正常删除（含与 SyncKubeConfig 并发死锁回归） | ✅ | 通过 | R42：创建中重启 server、取消 CreateCluster 后立即走正常 delete；DeleteCluster `de3e7b34-23ff-4bf2-a339-1f7134dff695` 收敛，Terminating 阶段未再生成 SyncKubeConfig，Cluster/Operation 清空，节点 Kubernetes 服务 inactive。 |
| 2.4-02 | InstallFailed 状态删除 | ✅ | 通过 | R42：InstallFailed 在线集群通过普通 kcctl delete cluster 删除成功。 |
| 2.4-03 | 删除失败 TerminateFailed → 重试 | ✅ | 通过 | R42 补测：隔离集群 `r42-feasibility-podexec` 的 DeleteCluster 设 60s timeout，停止 dev4 Agent 后 Operation `d2d53886-df73-4202-ba7e-834c07b7a653` TimedOut、集群进入 TerminateFailed；恢复 Agent 后 `kcctl operation retry`，集群和其 Operation/Task 记录被清理，dev3/dev4 kubelet、containerd inactive，平台仍 Healthy。 |
| 2.4-04 | 有备份时删除保护（引导先删备份） | ✅ | 通过 | R42：存在 Backup 时集群删除 dry-run 返回 400；删除 Backup 后对象和文件清除，再删除保护点。 |
| 2.4-05 | 删除后 ops 账本清理 | ✅ | 通过 | R42 补测：正常删除 `r42-feasibility-metallb` 后 `kcctl operation list -c r42-feasibility-metallb` 无记录；API 查询集群返回 NotFound，最终删除清理了目标 Operation/Task 账本。 |
| 2.5-01 | backuppoint fs 型（含非法类型拒绝） | ✅ | 通过 | R42：非法 NFS BackupPoint HTTP 400；FS BackupPoint 创建成功并绑定到集群。 |
| 2.5-02 | backuppoint **S3 型（MinIO）** | ✅ | 通过 | R42 补测：在 dev4 启动隔离 SeaweedFS S3 endpoint（非 MinIO），S3 BackupPoint 创建约 8.6 MB 集群备份，Backup 状态 Available 且含 MD5；删除备份 Operation 成功，测试对象与临时服务已清理。覆盖 S3 API 路径，不代表 MinIO 产品兼容性专项。 |
| 2.5-03 | 手动备份 → 恢复（marker 回滚证明） | ✅ | 通过 | R42：两次 FS 备份均 Available；修改 ConfigMap marker 后恢复，值从 after-backup 回滚到 before-backup；RecoveryCluster 3 步成功。 |
| 2.5-04 | 备份删除（连带存储文件清除） | ✅ | 通过 | R42：本轮 10 个 CronBackup 快照经 DeleteBackup Operation 移除；Backup 列表为空且 /opt/kc/backups/r42-cron 已在三机删除。 |
| 2.5-05 | cronbackup runAt 单次触发 | ✅ | 通过 | R42：在独立备份目录配置未来 runAt 后，CronBackup 仅触发一次并生成 7,032,864 字节备份；Operation `8942a7d1-44ea-4554-85c4-0d6fcd0b1c98` Succeeded，临时备份对象及目录已清理。 |
| 2.5-06 | cronbackup 真实周期命中 | ✅ | 通过 | R42：*/1 * * * * CronBackup 连续触发，LastSuccessfulTime 多次推进，新增备份记录均 Available。 |
| 2.5-07 | cronbackup enable / disable 子资源 | ✅ | 通过 | R42：PATCH disable 后观察 90 秒未新增快照；enable 后恢复周期触发，收尾时再次 disable。 |
| 2.5-08 | **maxBackupNum 超限自动轮转** | ✅ | 通过 | 补丁构建现场复测：maxBackupNum=2、每分钟触发，6 分钟内 7 次 BackupCluster 均 Succeeded，5 次自动 DeleteBackup 均 Succeeded；每轮后仅保留 2 条 Available 记录和 2 个对应 FS 文件，ErrNoReachableMaster=0。最终两份测试备份也通过正常删除流程清除。 |
| 2.5-09 | 恢复后集群可用性（addons/节点完整） | ✅ | 通过 | R42：恢复后 3 节点 Ready、19 个系统 Pod Running，/readyz readyz check passed。 |
| 2.5-10 | 备份损坏或错误 S3/FS 凭据 | ✅ | 通过 | R42 补测：隔离 S3 BackupPoint 使用错误凭据，BackupCluster Operation `a6375be2-ae32-409e-bb5b-caa5b6537de0` Failed，备份对象状态 error/大小 0，集群仍 Running；失败对象删除成功，未触碰 FS 快照或共享 Registry。 |
| 2.5-11 | Backup 详情查询 API（`GET /backups/{name}`） | ✅ | 通过 | R42：对创建的 Backup 执行详情 GET，HTTP 200 且返回该对象。 |
| 2.6-01 | 创建集群解析并持久化 `status.packagePlan` | ✅ | 通过 | R42：r42-m1w2 Cluster 对象持久化 packagePlan，含 k8s v1.37.0、cri 2.2.4、cni v3.31.5、extension v1。 |
| 2.6-02 | packagePlan 不保存 blob 或 Registry 凭据 | ✅ | 通过 | R42：PackagePlan 仅保存 OCI refs、digest、版本与 contents；敏感键扫描未发现 password/token/credential/private.key。 |
| 2.6-03 | Agent 首次按 digest 拉取、校验、解包和执行 | ✅ | 通过 | R42：r42-m1w2 CreateCluster 15 步 Succeeded，三个 Kubernetes 节点 Ready；Agent 首次按计划消费 OCI 制品完成建群。 |
| 2.6-04 | Agent 命中本地校验缓存 | ✅ | 通过 | R42 2026-10-13 再补测：同一 Agent 重复安装真实 extension 包；隔离目标 Registry 不可达时仍命中经校验的本地缓存，Operation Succeeded、归档字节 SHA 不变。 |
| 2.6-05 | 缓存损坏或 digest 不符 | ✅ | 通过 | R42 2026-10-13 再补测：翻转真实 configs.tar.gz 一字节；目标 Registry 隔离时拒绝坏缓存并失败，恢复后正式 retry 重新拉取，Operation Succeeded、文件 SHA 恢复原值。 |
| 2.6-06 | Registry 暂时不可达时的缓存行为 | ✅ | 通过 | R42 2026-10-13 再补测：只对临时 5103 Registry 添加带标识 OUTPUT REJECT：好缓存继续成功、坏缓存拒绝；撤销规则后重拉成功，恢复原缓存。共享 5003 始终可用。 |
| 2.6-07 | Package Registry 配置优先级与文件权限 | ✅ | 通过 | R42 2026-10-13 再补测：真实 kcctl deploy 验证显式 flags > KC_PACKAGE_REGISTRY_CONFIG > Agent 文件 > Server 文件；错误文件明确拒绝、显式参数覆盖后到达服务已存在预检；两配置文件 0600，字节与权限恢复。 |
| 2.6-08 | Delivery Policy 默认策略初始化 | ✅ | 通过 | R42：新部署自动初始化 default Delivery Policy 3 条规则，并在建群版本解析中使用。 |
| 2.6-09 | Delivery Policy 自定义版本白名单 | ✅ | 通过 | R42 2026-10-13 再补测：临时白名单只允许 v1.36.4：合法 dry-run=200，v1.37.0=400 UnsupportedKubernetesVersion；Policy 精确恢复。 |
| 2.6-10 | Delivery Policy 缺失 slot/repository | ✅ | 通过 | R42 2026-10-13 再补测：删除 cri slot 后 dry-run=400 UnsupportedComponentSlot；声明不存在的 k8s-extension/r42-missing-extension repository 后=400 ArtifactNotPublished，零集群对象，Policy 恢复。 |
| 2.6-11 | bootstrap/standalone extension 与集群 packagePlan 隔离 | ✅ | 通过 | R42 2026-10-13 再补测：两个真实 Running 集群的 packagePlan 前后完全相等：正式 kcctl upgrade console --manifest 成功，正式 /clusters/.../extension Operation Succeeded；bootstrap 与独立 extension 均未污染任一集群计划。Console/部署配置恢复。 |
| 3-01 | `kcctl deploy` | ✅ | 通过 | R42：kcctl deploy 在三台主机完成 3 server + 3 agent HA 部署。 |
| 3-02 | `kcctl clean --all` | ✅ | 通过 | R42：部署前执行 kcctl clean --all --force --assumeyes；三台旧平台服务停止后成功重部署。 |
| 3-03 | `kcctl doctor` | ✅ | 通过 | R42：kcctl doctor 25/25，无 warning/failure。 |
| 3-04 | `kcctl join` | ✅ | 通过 | R42：kcctl join 使用 SSH key 将 sh-dev-4 纳管，节点 Ready 且平台 Healthy。 |
| 3-05 | `kcctl create cluster` | ✅ | 通过 | R42：kcctl create cluster r42-m1w2 成功提交并执行 CreateCluster Operation。 |
| 3-06 | `kcctl create/delete user`、`role` | ✅ | 通过 | R42：用 `kcctl create role/user` 建立临时对象，`kcctl get user/role` 可见；`kcctl delete user/role` 成功，随后列表确认无残留。 |
| 3-07 | `kcctl create/delete registry` | ✅ | 通过 | R42：Registry 对象 API create/update/get/delete 已验证；另用 `kcctl create/get/delete registry` 现场完成命令行 CRUD，删除后确认对象不存在。共享 Registry 服务和制品未修改。`kcctl delete --help` 文案未列 Registry，但实现可用。 |
| 3-08 | `kcctl delete cluster` | ✅ | 通过 | R42：前序 r42-core-cleanup 集群经正常 kcctl delete cluster 流程移除；节点侧 Kubernetes/CNI 清理并核验。 |
| 3-09 | `kcctl get cluster/node/user/role/configmap/registry` | ✅ | 通过 | R42：kcctl get node/cluster/user/role/configmap/registry 均成功返回资源。 |
| 3-10 | `kcctl get --watch` | ✅ | 通过 | R42：kcctl get node --watch -o json 收到 7,324 字节初始数据流；4 秒超时按预期结束。 |
| 3-11 | `kcctl operation list/describe/logs/retry` | ✅ | 通过 | R42：`kcctl operation list/describe/logs/retry` 均已执行；升级失败 Operation 经 CLI retry 后 Task 与 Operation 收敛成功。 |
| 3-12 | `kcctl operation cancel` | ✅ | 通过 | R42：对镜像拉取挂起的在线 CreateCluster Operation 执行 kcctl operation cancel，最终状态 Canceled。 |
| 3-13 | `kcctl cluster upgrade` | ✅ | 通过 | R42 补丁复测：`kcctl cluster upgrade` 从 v1.36.4 升至 v1.37.0，Operation 六步成功，节点与 kubeadm-config 版本一致。 |
| 3-14 | `kcctl set cluster` | ✅ | 通过 | R42：kcctl set cluster 设置并清空 external IP/domain/port 元数据，返回更新后的集群对象。 |
| 3-15 | `kcctl drain` | ✅ | 通过 | R42：kcctl drain --agent sh-dev-4 节点 ID 完成；节点对象移除、kc-agent inactive，平台保持 Healthy。 |
| 3-16 | `kcctl upgrade all --manifest/--version` | ✅ | 部分 | R42 续测（2026-10-13）：离线 upgrade all --manifest 已完整通过1.3-07；在线 --version 仍受1.3-09 upstream Release 下载源限制。 |
| 3-17 | `kcctl upgrade server/agent/console/kcctl` | ✅ | 通过 | R42 续测（2026-10-13）：同 1.3-10：正式 CLI 独立 Server/Agent/Console/kcctl 升级均已现场验证。 证据见 r42-next-2026-10-13.json/.log。 |
| 3-18 | `kcctl registry sync` | ✅ | 通过 | R42 2026-10-13 再补测：kcctl registry sync 对 7 个真实制品执行 HTTP 与严格 CA/账号认证 HTTPS 同步成功，并验证幂等与目标 tag 冲突拒绝。清单为本轮资格测试清单，不代表正式 Release。 |
| 3-19 | `kcctl registry list/deploy/clean/push/delete` | ✅ | 通过 | R42 2026-10-13 再补测：dev4 隔离 Registry 由 kcctl deploy 创建；真实 pause 镜像 archive push、list、delete 成功，删除后 manifest 404；kcctl clean 成功，服务 inactive、配置/数据目录消失。 |
| 3-20 | `kcctl resource list` | ✅ | 通过 | R42：kcctl resource list 查询出 5003 中 amd64/k8s/v1.37.0；resource refresh 报 refreshed 51 OCI packages。 |
| 3-21 | `kcctl resource inspect` | ✅ | 通过 | R42：kcctl resource inspect 返回合法 JSON，含 name/kind/version/sourceRevision/os/arch/contentProfile/transport/contents 字段。 |
| 3-22 | `kcctl resource refresh` | ✅ | 通过 | R42：kcctl resource refresh --registry 172.16.131.146:5003 返回 refreshed 51 OCI packages。 |
| 3-23 | `kcctl delivery-policy template/get/apply/diff` | ✅ | 通过 | R42：delivery-policy template 输出可解析 YAML；当前策略 get/diff/apply/validate 成功，diff 无变更。 |
| 3-24 | `kcctl delivery-policy validate` | ✅ | 通过 | R42：kcctl delivery-policy validate 对当前策略返回 delivery support policy is valid。 |
| 3-25 | `kcctl login` | ✅ | 通过 | R42：kcctl login 临时用户成功写入 token 配置；kcctl get cluster 可返回；随后恢复 admin 配置并删除临时输出。 |
| 3-26 | `kcctl status` | ✅ | 通过 | R42：kcctl status 返回 Healthy，3/3 agents running。 |
| 3-27 | `kcctl deploy config` | ✅ | 通过 | R42：kcctl deploy config 已执行并生成默认 YAML，敏感字段在输出侧遮蔽。 |
| 3-28 | `kcctl version` | ✅ | 通过 | R42：kcctl version 显示 v2.0.3-rc.34 / commit 5ce8f2dc。 |
| 4-01 | addons：nfs-csi 安装/动态供给读写/卸载 | ✅ | 通过 | R42 补测：隔离 1M+1W 集群安装 `nfs-csi-v1`，动态 PVC Bound；dev3 writer 写入标记，dev4 reader 读回一致；Pods/PVC/PV 清理后完整配置卸载成功。NFS export 仅使用 `/srv/kc-backup/r42-feasibility-20261012` 子目录，已移除，原 export 保留。 |
| 4-02 | addons：metallb L2 安装/卸载 | ✅ | 通过 | R42 补测：MetalLB L2 InstallComponents `2d147ed0-6e6e-41b4-b9d0-d39526a3484a` Succeeded，controller/speaker Ready，IP pool 与 L2Advertisement 创建；卸载 Operation `3199a741-6d78-40b4-bc10-c97143ec0fa3` Succeeded，资源清空。 |
| 4-03 | addons：metallb BGP | ✅ | 通过 | R42 补测：按 2026-10-10 已确认口径，只验 Addon 安装。BGP InstallComponents `82635f4a-5715-47e1-b06f-f12211e52a4e` Succeeded，controller/speaker Ready、IP pool/BGPAdvertisement 创建；卸载 `4ec40d76-b44a-41bc-830e-5ba8c6dec3bd` Succeeded。未配置 peer 或声称外部 VIP 数据面通过。 |
| 4-04 | Addon 同组件多实例 | ➖ | 范围外 | R42：按 R41 维护的测试范围，本项未要求验收多实例 Addon；inventory 也无可安装 Addon 制品。 |
| 4-05 | addons：uninstall 容错（空 config / ErrIgnore 链） | ✅ | 通过 | R42 补测：使用默认 `nfs-sc` 安装后，以空 config 卸载，Operation `348edd76-3a65-47ce-bc42-056b2127534f` Succeeded，StorageClass 与 CSI Pods 均清理。边界：自定义 `scName` 的卸载需传回原 config；空 config 会按默认名 `nfs-sc` 查找。 |
| 4-06 | 可观测：operation logs、失败原因展示（API 侧） | ✅ | 通过 | R42：API 查询 OperationTask steps 和日志成功；offset 0/300 返回不同日志片段，失败原因可从 Task log 读取。 |
| 4-07 | **console UI 端到端（含任务失败展示）** | ✅ | 通过 | R42 续测（2026-10-13）：管理员真实登录 Console；真实 Shell 故障 Operation 显示失败、失败步骤与 R42_REQUIRED_INPUT_MISSING 日志，含重试入口；截图/DOM 已保存。该预期负向场景通过。 证据见 r42-next-2026-10-13.json/.log。 |
| 4-08 | 用户 / 角色 CRUD、enable/disable、改密、登录记录 | ✅ | 通过 | R42：用户登录、禁用/启用、改密和登录记录查询已验证；本轮补用 `kcctl` 创建并删除临时 user/role，列表确认无残留。 |
| 4-08b | RBAC 鉴权拦截（非管理员越权应 403） | ✅ | 通过 | R42：platform-view 用户 POST Registry 得到 HTTP 403；管理员证书请求可执行对应管理操作。 |
| 4-08c | 密码登录与验证码登录 | ✅ | 通过 | R44（2026-10-13）：当前代码真机隔离服务+真实etcd验证普通密码200/错误401、MFA challenge428、fake_sms申请200/立即重发429、错码401/正确码200、token访问API200、challenge复用与已消费码换新challenge均401；实际等待65秒后重发成功，验证码过期401、新码登录成功。fake_sms仅输出日志，不代表真实短信送达；真实短信不是本case前置条件。临时服务、账号和测试存储前缀已清理。见[R44报告](status-2026-10-13-r44.md)。 |
| 4-08d | 长期 token 创建、查询、撤销与过期 | ✅ | 通过 | R42 2026-10-13 再补测：补测真实 20 秒登录 token：有效期内请求 200，23 秒后同 token 请求 401，access_token 对象删除。测试 refresh token 经 logout 撤销，账户与临时 TTL 配置清理恢复。 |
| 4-09 | 集群模板 templates | ✅ | 通过 | R42：模板创建 HTTP 201、查询 200；被集群引用时删除 400，解除引用后删除 200。 |
| 4-10 | DNS domains / records | ✅ | 通过 | R42：domain/record create/get/update/delete 均成功；无效 AAAA 请求返回 400，删除后 GET 404。 |
| 4-11 | 外部集群纳管 | ✅ | 通过 | R43：KC创建的3 master集群，通过独立当前候选平台的kubeadm CloudProvider纳管；新kubeconfig使用172.16.131.208:6443，证书IP SAN匹配且保留CA校验。发现并修复服务端误读CLI CA路径（b4a5886e）。precheck/create/sync成功，3 Agents健康；解除纳管后Provider/Cluster/Agent记录为0，源集群3节点Ready且实际API写读删成功。 证据见 [R43 报告](status-2026-10-13-r43.md)及 evidence/r43。 |
| 4-12 | kubeconfig 下载 | ✅ | 通过 | R42：/clusters/{name}/kubeconfig 返回 HTTP 200，成功用于 kubectl get nodes/pods 与 /readyz。 |
| 4-12a | Web 节点/集群终端 | ✅ | 通过 | 补丁 E2E 现场复测：sh-dev-2 运行聚焦 SSH terminal 的 TestRunE2ETests，1 passed；通过 TLS WebSocket 连接 sh-dev-4 节点，完成连接、检查与关闭。测试环境将 Agent IPDetect 固定到 ens3，并用临时账户验证；账户已删除。 |
| 4-12b | Pod exec | ✅ | 通过 | R42 补测：临时 Pod `r42-exec-probe` Ready；经平台 Pod exec WebSocket 路径执行 `echo R42_POD_EXEC_OK`，收到相同输出。Pod 随隔离集群删除。 |
| 4-13 | 平台自省：/configz、/status、/components、/componentmeta | ✅ | 通过 | R42：/configz、/status、/components、/componentmeta 均以管理员证书请求 HTTP 200。 |
| 4-14 | 审计事件查询（/events，auditing 组） | ✅ | 通过 | R42：/api/audit.kubeclipper.io/v1/events?limit=10 返回 HTTP 200，含 10 条记录、totalCount 854。 |
| 4-15 | PlatformSetting（镜像仓库模板、Web 终端密钥） | ✅ | 通过 | R42 2026-10-13 再补测：原平台无 PlatformSetting；实际模板 PUT/GET、密码脱敏及空密码更新保留通过；正式 terminal.key POST 轮换公钥，GET 不暴露私钥。仅删除本轮生成的设置，恢复空基线。 |
| 4-16 | 敏感信息脱敏 | ✅ | 通过 | 补丁构建现场复测：弱口令拒绝输出固定错误文本，不回显测试输入串；与 1.3-12 同一修复，options 定向测试也通过。 |
| 4-17 | `/healthz` 与 `/metrics` | ✅ | 通过 | R42：三个 server /healthz 均 HTTP 200；/metrics HTTP 200、108 行，指标名无 password/token/secret/private.key。 |
| 4-18 | 登录失败限流与恢复 | ✅ | 通过 | R42：错误登录累计到 HTTP 429；等待限流窗口后正确凭据恢复 HTTP 200。 |
| 4-19 | Addon OCI runtime 镜像来源与 digest | ✅ | 通过 | R43 按现有发布契约修正前置条件：NFS/MetalLB YAML内嵌，Actions发布标准runtime镜像，无需独立空package/chart。当前候选实际安装两种Addon，4个Addon Pod就绪，27个运行/初始化容器digest均匹配当前qualification；NFS PVC Bound、真实卷写读成功，正常卸载。 证据见 [R43 报告](status-2026-10-13-r43.md)及 evidence/r43。 |
| 4-20 | Addon 安装失败后的 retry/卸载清理 | ✅ | 通过 | R42 2026-10-13 再补测：MetalLB 指向临时缺镜像 repository，真实 InstallComponents Failed、Pod ImagePull 事件可见；补齐真实 controller/speaker/frr 后正式 API retry Succeeded、Pod Ready；卸载 Succeeded，Pod/Deployment/DaemonSet/CRD 无残留。 |
| 5-01 | 创建、扩缩、升级、备份、恢复使用 Operation V2 | ✅ | 通过 | R42：Create/Add/Remove/Convert、Backup、Recovery 与补丁复测的 UpgradeCluster 均由 Operation V2 执行并成功；升级六步全部 Succeeded。 |
| 5-02 | Operation/Task 展示节点 UID、IP、步骤和错误 | ✅ | 通过 | R42：CreateCluster Operation 25 Tasks/15 unique steps；Task nodeRef 含节点 name、UID、IP，终态与错误字段可查。 |
| 5-03 | Server/Agent 重启后的 Operation 恢复 | ✅ | 通过 | R42：BackupCluster `2e268c4b-1455-4f2b-b811-dc084387972e` Running 时重启 dev2 kc-server，并在 Agent 离线期间等待；server 恢复 Healthy、Agent 重连后 Operation Succeeded。 |
| 5-04 | Agent Watch 410/EOF 后 relist | ✅ | 通过 | R42 2026-10-13 再补测：实际 Agent 经 mTLS 故障代理收到 Watch 410 与 HTTP 200/EOF，随后 relist 并恢复任务；代理与 Agent endpoint 已恢复。 |
| 5-05 | 多节点 Step barrier 与输出传递 | ✅ | 通过 | R42 2026-10-13 再补测：真实双节点 NodePreflight：一台 Agent 暂停，另一台完成时下游 Task 未生成；恢复后 barrier 放行，下游 shell Task 接收到真实 hostname 输出，创建时间晚于全部上游完成。 |
| 5-06 | Operation cancel | ✅ | 通过 | R42：运行中的在线建群 Operation 收到 cancel 请求，活动 Task 收敛后 Operation 为 Canceled。 |
| 5-07 | 自动 retry 与人工 retry | ✅ | 通过 | R42 2026-10-13 再补测：真实 shell 首次 exit17 后自动 attempt1 成功；永久 exit19 耗尽两个 attempts 后 Failed。损坏缓存与 Addon 均通过正式人工 retry 恢复。 |
| 5-08 | 同集群危险操作互斥 | ✅ | 通过 | R42 补测：MetalLB InstallComponents 处于 Running、集群 Updating 时执行 `delete cluster`，服务端返回 400 `can't delete cluster when cluster is Updating`；没有创建 DeleteCluster Operation，集群未被删除。 |
| 5-09 | Task 终态写入成功但响应丢失 | ✅ | 通过 | R42 2026-10-13 再补测：实际 Task 终态 PUT 已持久化 200，故障代理仅丢弃该响应；Operation Succeeded，真实副作用文件仅 1 行 execution，没有重放。 |
| 5-10 | 无法确认副作用时 fail-closed | ✅ | 通过 | R42 续测（2026-10-13）：真实 etcd 测试 Task executor 冲突→InvalidExecutionFacts、Pending 取消、无副作用；发现 payload 校验缺失并修复，payload-only 篡改复测同样 fail-closed，相关单测通过。 证据见 r42-next-2026-10-13.json/.log。 |
| 5-11 | Operation API limit/continue 稳定游标分页 | ✅ | 通过 | 补丁构建现场复测：以 limit=1 从 API 连续遍历 10 页、10 条唯一 Operation 记录；每页 HTTP 200、游标递进且无 504。 |
| 5-12 | Task 日志 offset 与 Task 切换 | ✅ | 通过 | R42：Task logs offset 0 与 offset 300 返回不同片段，并验证多个 Task 日志切换。 |
| 5-13 | 相同业务请求重复提交 | ✅ | 通过 | R42 补测：同一 NFS Addon 安装请求连续提交，第二次 HTTP 400 `nfs-csi-v1 component has been installed in the current cluster`，没有生成重复 Operation。 |
| 5-14 | Operation/Task timeout | ✅ | 通过 | R42 2026-10-13 再补测：补测实际 sleep30/step timeout2s；修复 shell 丢失 deadline 原因后 Task=TimedOut/DeadlineExceeded、Operation Failed。结合前轮 Operation deadline 覆盖。 |
| 6-01 | 支持策略与 `packaging/resources.yaml` 一致 | ✅ | 通过 | R42：go run ./tools/release-policy-verify --manifest packaging/resources.yaml 返回通过。 |
| 6-02 | bootstrap、Kubernetes、CRI、CNI、extension、addon OCI 制品完整 | ✅ | 通过 | R43 当前b4a5886e候选qualification成功：110制品（10 package-image、2 helm-chart、98 runtime-image），双架构清单校验与sync roundtrip通过。61个amd64制品正式同步，重同步0 copied/61 skipped；配套Server/Agent部署、建群及NFS CSI/MetalLB真实消费通过。Addon按现有契约内嵌YAML并发布runtime镜像。 证据见 [R43 报告](status-2026-10-13-r43.md)及 evidence/r43。 |
| 6-03 | Release Manifest 包含 package、chart、runtime image 和 bootstrap | ✅ | 通过 | R42 续测（2026-10-13）：实际已发布 v2.0.3 ReleaseManifest SHA e864b6f6，110 制品：10 package-image、2 helm-chart、98 runtime-image，含 bootstrap；正式 verifier 对 amd64/arm64 各61引用均通过。仅覆盖474ba45d候选。 证据见 r42-next-2026-10-13.json/.log。 |
| 6-04 | digest、source、revision、version provenance 正确 | ✅ | 通过 | R42 续测（2026-10-13）：实际 ReleaseManifest/校验和、OCI digest、10 个包 config 的 sourceRevision/version 与发布候选474ba45d一致；配套 kcctl 同 revision，目标镜像同步未漂移。原 case 不要求签名；不声明签名验证。 证据见 r42-next-2026-10-13.json/.log。 |
| 6-05 | 制品不可变性与重复发布保护 | ✅ | 通过 | R42 2026-10-13 再补测：实际 oci-publish 连续发布同内容成功且 child digest 一致；换 /usr/bin/false 后同 tag 重发 exit1/refusing，index 和 amd64 child digest 保持原值；只改隔离仓库。 |
| 6-06 | amd64/arm64 Manifest 与架构过滤 | ⚠️ | 通过 | R42 续测（2026-10-13）：真实 Release 的10个 package OCI index 均有 linux/amd64 与 linux/arm64；正式 verifier 两个架构各61引用通过。架构解析/过滤闭环；arm64 真机运行另属6-10。 证据见 r42-next-2026-10-13.json/.log。 |
| 6-07 | Registry sync 与目标仓库消费 | ✅ | 通过 | R42 2026-10-13 再补测：7 个制品同步到隔离 5103 仓库，真实 Agent extension Operation 按固定 digest 从目标仓库消费成功；重复 sync、缓存及目标冲突验证通过。全发布制品资格检查另见 6-08。 |
| 6-08 | 完整 qualification 发布 | ✅ | 通过 | R43 当前候选b4a5886e，qualification run38364805938成功，manifest SHA256 d567fe021cbd3bb1a69daf1142db62480484d6ca5a14b4985a3259aebd4eb2e3。同revision的干净CI构建Server/Agent/kcctl完成fork GHCR直装、隔离仓库加入Agent、v1.37.0建群；CreateCluster47051181及SyncKubeConfig Succeeded，14系统Pod Running/0重启；Addon及镜像digest验证、正常卸载/删群完成。arm64运行未测；发布限fork候选。 证据见 [R43 报告](status-2026-10-13-r43.md)及 evidence/r43。 |
| 6-09 | linux/amd64 主路径 | ✅ | 通过 | R42：三机 amd64 平台部署和 v1.37.0 Kubernetes 建群/验收通过。 |
| 6-10 | linux/arm64 主路径 | ⚠️ | 阻塞 | R42 续测（2026-10-13）：已拿到真实 arm64 制品；仍无 linux/arm64 原生节点。需真机/原生 VM 后部署、建群及网络验证。 |
| 6-11 | Tier 1 OS 矩阵 | ✅ | 通过 | R40 已将 Tier 1 OS 明确限定为 Ubuntu 24.04 amd64；R42 三台测试主机均为 Ubuntu 24.04，候选平台在三机完成部署与验证，无其他承诺中的 Tier 1 OS 待测。 |
| 6-12 | Docker CRI 废弃入口清理 | ✅ | 通过 | R42：kcctl create cluster --cri docker 被 CLI 以 unsupported/deprecated 入口拒绝；支持容器运行时只有 containerd。 |
| 6-13 | legacy static server 与 `nfs-provisioner` 不再暴露 | ✅ | 通过 | R42 补测：三机无匹配的 legacy static-server/nfs-provisioner/tar-downloader 服务、进程或二进制；Console 独立升级到 commit `50f1a4a8`，HTTP 200，dist bundle 无 `nfs-provisioner`。共享 NFS export 未改动。 |
| 7-01 | 第三方 OAuth/OIDC 登录 | ✅ | 通过 | R42 续测（2026-10-13）：官方 Dex v2.46.0 实例真实 OIDC 登录→KC callback200→无管理员证书的 Bearer 用户 API200；错误密码401、code replay401，auto 用户映射成功。隔离 HTTP IdP 测试，不声明生产 IdP TLS 验收。 证据见 r42-next-2026-10-13.json/.log。 |
| 7-02 | `kcctl completion` | ✅ | 通过 | R42：kcctl completion bash/zsh 均生成成功，并在本机分别通过 bash -n、zsh -n。 |
| 7-03 | 多节点并发任务基础容量 | ✅ | 通过 | R42 2026-10-13 再补测：容量基线 N=2：dev3/dev4 两个独立 1 master 集群并发创建，两个 CreateCluster 均 Succeeded，运行时间窗口交叠，两节点各自 Ready；平台 doctor 19 passed/3 Agents Healthy。不声称更大并发上限。 |

## 继续复测：升级竞态与 Console 组件升级

- 复查 `2.3-01/02/06/09`：根因是 master kubelet 重启后 kube-apiserver 静态 Pod 会短暂重建；旧步骤在单次 `/readyz` 恢复后过早进入 worker drain，造成连接拒绝。`pkg/scheme/core/v1/k8s/cluster.go` 现在要求 `/readyz`、master Node Ready、`kube-apiserver-$node` Pod Ready 连续 7 次采样（间隔 5 秒），最长等待 240 秒；未稳定时明确失败。相关定向测试及 `go test ./pkg/scheme/core/v1/k8s -count=1` 通过。
- 用补丁构建新建 `r42-upgrade-stablecheck`（1 master + 1 worker，初始 v1.36.4），UpgradeCluster Operation `2ed1c3b0-1fa6-48a3-a6be-a016c06d9552` 全部六步 Succeeded。控制面等待稳定就绪门禁通过后才执行 drain；两节点最终 Ready v1.37.0，kubeadm-config 为 v1.37.0，系统 Pod 全 Ready，`/readyz=ok`。升级后的 packagePlan 为 k8s v1.37.0、containerd 2.2.4、Calico v3.31.5、extension v1。
- Console 临时 ReleaseManifest 首次误填 amd64 子 manifest digest；Registry tag 实际是 OCI index。核对 index 为 `sha256:f49f55382f072490366b0df21d87c0b8a38fa4f9395596d5797a10ed73639e94`，其中 amd64 子 manifest 为清单记录的 `sha256:adce2b3d7d5861511f2da16a16da892706c0f9f2bab61ef9f7be5e205dee26b3`。改用 index digest 后，`kcctl upgrade console --manifest ... --package-registry-scheme http -y` 成功；dev2 Console 为 commit `50f1a4a8d3b0c6bccc2b9b2a97aef2829d8c5549`、HTTP 200，dist 中无 `nfs-provisioner`。
- 按正常删除路径清理测试集群；恢复 dev2 server 与 dev3/dev4 agent 的 rc.34 原始二进制并核验 SHA-256，平台 Healthy、2/2 agents。共享 Registry `kc-oci-r3-registry` 仍 active、`/v2/` HTTP 200；没有改 Registry tag 或制品。Console 修复版本保留在 dev2。
- 额外复核 `2.1-20`：`r42-firstfound` 使用离线 Registry 创建成功，Calico `IP_AUTODETECTION_METHOD=first-found` 已落到 DaemonSet；Calico 日志显示 dev4 选中 `lxdbr0=10.8.39.1`，与 Kubernetes Node IP 不同，dev3 到该地址 ping 不通，故保留部分并记录多网卡配置要求。集群已正常删除。
- 额外复核 `1.3-01`：部署目标设为保留网段 `192.0.2.1` 后，在 SSH 预检阶段返回 `i/o timeout`，未触碰三台测试机；错误包含地址和原因，但带 Go stack trace。缺包失败分支仍未覆盖，故保留部分。
- 额外复测 `2.1-12`：`r42-api-publish` 的 external IP/domain/port 标签已存储；实际 apiserver 证书 SAN 覆盖自定义 SAN、外部 IP 和外部 domain，经 CA 校验从 IP 与域名入口访问 `/readyz` 均返回 `ok`。集群已正常删除。

## 2026-10-13 未通过项再补测（前次快照）

本次从 20 条部分、38 条阻塞出发，新增 28 条通过，4 条由阻塞推进到部分。当前尚未闭环 30 条：14 部分、16 阻塞。逐项原因已更新在上表；新增现场证据、4 处修复、剩余项分类与清理见 [续测报告](status-2026-10-12-r42.md#未通过项再补测2026-10-13)。

## 2026-10-13 本次续测

本次从162通过、14部分、16阻塞出发，新增17项通过、1项阻塞推进为部分。当前为179通过、6部分、0失败、7阻塞、2范围外，共194项；未完全通过13项。详见 [续测报告](status-2026-10-12-r42.md#未通过项继续补测2026-10-13本次续测) 与 [前置条件](r42-case-prerequisites.md#当前还未完全通过的13项)。

## GHCR 测试入口说明

当前测试方法已按用户要求允许 fork GHCR 制品，具体仓库、配套 kcctl 与操作示例见 [前置条件](r42-case-prerequisites.md#11-01-当前允许的-fork-ghcr-测试入口)。1.1-01 的历史阻塞仅指官方默认路径；文档更新不冒充本轮 fork 直装结果。
