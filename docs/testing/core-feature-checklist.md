# KubeClipper 核心功能测试清单（长期覆盖基准）

本文档是平台核心功能的**长期测试基准**：每轮验证（E2E、回归、发布前）都应以此清单为
起点圈定范围，测完回填状态。它不属于某一次测试计划，而是所有测试轮次共同维护的覆盖台账。

功能基线为 **OCI 交付架构合并后的 KubeClipper**。OCI Registry、Delivery Policy、
PackageInventory、PackagePlan 和 Agent 按 digest 消费制品属于平台正常工作方式，不作为一套
独立的“OCI 模式”重复列举。

## 使用说明

- **编号即身份**：本文已随 OCI 合并基线完成一次性重整；自本基线起，`x.y-NN` 一经分配
  不再变更或复用。功能下线时把状态标为 `🗑 废弃` 并保留编号，新增 Case 在小节末尾追加。
- **状态含义**：✅ 已实测通过 · ⚠️ 部分验证 · ❌ 未验证 · 🗑 废弃。
  状态必须来自**真实运行**，单测覆盖不算 ✅（可在备注注明 "unit-only"）。
- **轮次记号**：备注中 R1～R6 指验证发生的轮次；每轮详细证据记录在
  `docs/superpowers/issues/` 的轮次报告里，本文档只留结论与指针。
- 三机实测报告：R4 [`2026-09-17-core-feature-e2e-sh-dev-2-3-4.md`](../superpowers/issues/2026-09-17-core-feature-e2e-sh-dev-2-3-4.md)，
  R5 [`2026-09-17-core-feature-e2e-r5-sh-dev-2-3-4.md`](../superpowers/issues/2026-09-17-core-feature-e2e-r5-sh-dev-2-3-4.md)，
  R6 [`2026-09-17-core-feature-e2e-r6-sh-dev-2-3-4.md`](../superpowers/issues/2026-09-17-core-feature-e2e-r6-sh-dev-2-3-4.md)。
- **每轮测试的闭环动作**：① 圈定本轮要覆盖的编号 → ② 执行 → ③ 回填状态与轮次 →
  ④ 未覆盖项转入缺口文档 → ⑤ 新发现的功能面补编号。
- 当前缺口、优先级和待确认移除项统一维护在
  [`round-2026-09-gaps.md`](round-2026-09-gaps.md)，本清单不重复维护缺口摘要。
- **范围分层**：第 1～5 章是发布必须关注的核心功能；第 6 章是发布工程与可靠性门禁；
  第 7 章是扩展能力。扩展能力保留覆盖记录，但不能用其数量稀释核心主链路的通过率。

## 1. 平台本身的部署

### 1.1 部署物料来源

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 1.1-01 | 在线部署：默认 ghcr 直装 | ⚠️ | 预检✅；从未真从 ghcr 直装（都先 sync） |
| 1.1-02 | 半离线：ghcr → `kcctl registry sync` → 本地仓库 → deploy | ✅ | R3 全链路（5 copied/56 skipped 幂等） |
| 1.1-03 | 纯离线：bundle export → 拷贝 → import 进仓库 → deploy | ✅ | R13-C5（2026-09-22，rc.8 三机）：5003 export（skopeo --preserve-digests，5 制品 368MB）→ scp 离线拷贝 → 空白 9443 registry import ×2 → iptables 断公网（外网 DNS/连接全 REJECT，REJECT 计数实证）→ componentmeta 9443 → 建群 Running（9443 拉取 183 条，五类仓库全覆盖），calico/coredns 全 Running |
| 1.1-04 | 私有仓库 http | ✅ | R3 全程 :5003 |
| 1.1-05 | 私有仓库 https + 自签 CA | ✅ | R12（2026-09-22，rc.8 三机）：dev-2 自建 distribution 3.0.0（HTTPS+自签 CA SAN=IP），三节点 `package-registry.json`（0600）配 CA 后建群成功、三节点 Ready，agent 真实拉取 |
| 1.1-06 | Package Registry 账号密码认证（deploy/join/agent 消费侧） | ✅ | R12：htpasswd 基本认证；正确凭据建群成功；错误密码 → Operation Failed、错误消息 `UNAUTHORIZED: authentication required`（不含凭据），修正后 retry Succeeded；日志/operation 全文 grep 错误与真实密码均 0 泄漏。R13-C4 补 join 入口：join 下发 0600 凭据、扩容节点 agent 直连认证拉取 9×200、错误口令 join EXIT=1 可读报错零残留 |
| 1.1-07 | `registry sync` 重复同步幂等 | ✅ | R3：5 copied / 56 skipped；digest 不变 |
| 1.1-08 | 离线 bundle 重复 import 幂等 | ✅ | R13-C5：空白 9443 连续 import ×2，两次 Inventory 对比逐字节全等（digest 不变） |
| 1.1-09 | 错误仓库地址、凭据、CA 或缺失制品 | ✅ | 四类全闭环：凭据/CA=R12（UNAUTHORIZED/x509 明确失败、retry 可恢复、不泄凭据）；错误仓库地址=R13-C2 探针 A（dial refused 明确失败、零残留；服务端 500 透传原始错误，质量低于 400 typed 路径，已记录观察项）；缺失制品=R13-C2 探针 B（空仓库 rules=0 → `ArtifactNotPublished` typed 400）；修正后重试（PUT 回 5003 → 建群成功→删除）R13-C2 实证 |
| 1.1-10 | HTTPS 私有仓库 + 公共 CA | ✅ | R13-C3（2026-09-22）：自签 CA 加入三节点系统信任库（等价公共 CA：凭据文件零配置，`package-registry.json` 不配 ca 字段）→ 探针集群建群 Running，纯 TLS 拉取 231 次走系统信任池；未信 CA 路径 R12 已证 x509 拒绝。**运维发现：向系统信任库加 CA 后必须 `systemctl restart kc-server kc-agent`**——Go crypto/x509 首用时加载并进程内缓存系统根池，重启前 componentmeta 500 x509 unknown authority，重启后即恢复 |

### 1.2 部署拓扑

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 1.2-01 | 单 server + 多 agent | ✅ | R2/R3，3 个 agent |
| 1.2-02 | server 与 agent/k8s 节点混部同机 | ✅ | R2/R3 |
| 1.2-03 | **多 server HA（3× server，etcd 奇数集群）** | ✅ | R4：三 Server/三 etcd/三 Agent 实测；R7：新候选 `e9e95f4` 重新部署并验收（doctor 25/25、etcd 3 成员 non-learner、三台 /healthz ok），运行包 provenance 已更新 |
| 1.2-04 | console 组件部署与访问 | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17）：SSH 隧道+真实浏览器渲染登录页（欢迎语/表单/背景图完整，截图留档）；/version 返回平台版本、/api 未认证 403 JSON、静态资源 200 |
| 1.2-05 | 单 server + 单 agent | ✅ | R2/R3，server/agent/k8s 节点混部 |
| 1.2-06 | server 与 agent 分离部署 | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17）：单 server(208)+双 agent(146/230) 分离拓扑部署成功——server 节点 kc-agent inactive、agent 节点 kc-server inactive、平台 Healthy、节点正常纳管；随后按计划恢复 3+3 拓扑 |
| 1.2-07 | HA server/etcd 单点故障与滚动重启 | ✅ | R4/R6 停止/恢复 dev4 `kc-server`、`kc-etcd`；R7 在新候选上复测 server/etcd 两种故障窗口均通过；R19（rc.10，2026-09-24）补齐 Watch 与 Console 入口证据：停非连接节点（dev-3）watch 流与 kcctl 不受影响（quorum 2/3）；停被连接节点（dev-2）流断、重连被拒、进程退出——**发现：kcctl 客户端单地址无 failover**；窗口期 dev-2 console 登录页 200、`/api` 经 caddy 健康检查摘坏上游由存活上游应答（非 502）、dev-3/dev-4 console 200；恢复后 watch 重建立成功。另发现：卡在健康检查重试环的创建 Operation 无法取消（R7 新问题 5，已随 P0 行 7 于 R10/R16 闭环） |

### 1.3 容错与增量运维

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 1.3-01 | 预检失败引导（不可达/缺包报错） | ✅ | R2/R3 |
| 1.3-02 | etcd 冷启动竞态（写探针+重试） | ✅ | R3 |
| 1.3-03 | clean --all 后重 deploy 幂等 | ✅ | R3 ×2 |
| 1.3-04 | 不 clean 直接重复 deploy 的行为与平台安全性 | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17） rc.11 复测一致：不带 clean 再次 deploy → 三节点 kc-etcd.service already exists PRECHECK FAILED（逐节点列出）+"clean old environment before deploying"，零触碰、平台保持 Healthy（doctor 25/25） |
| 1.3-05 | `kcctl join` 独立纳管新节点 | ✅ | R6：dev4 空闲节点独立 join 成功，Package Registry HTTP 地址生效并恢复 Ready。R13-C4（2026-09-22）补认证/负向：join 下发 0600 `package-registry.json`（凭据经 base64 落盘）、被加入节点直连认证拉取、错误口令 join EXIT=1 可读 `UNAUTHORIZED` 且零部分安装；多网卡需 `--ip-detect interface=<nic>`（first-found 触发交互确认，后台 EOF 崩溃，见 R13-C4 过程发现） |
| 1.3-06 | `clean --all --force --deploy-config` 异常恢复 | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17）：两轮 clean --all --force --deploy-config（3+3 拓扑与分离拓扑各一）——清理后三节点 kc-server/kc-agent/kc-etcd/kc-console 全 inactive、二进制/配置//var/lib/kc-etcd 移除、端口释放，零半残；随后重部署均成功（含 etcd 重建） |
| 1.3-07 | **`kcctl upgrade all --manifest` 平台离线升级（OCI manifest + 内网 Registry）** | ✅ | B1 E2E（2026-09-20，R7 报告 §11）：三机 server+agent 实际升级 rc.3→`057f45e1`，逐台 stop→backup→install→start→healthz，成功后 staging 清理；错 digest/repointed tag 在触碰节点前拒绝；升级后 Healthy、doctor 25/25、配置数据保留。R19（rc.10 `c356fbaf`）：all 语义扩展至四组件 12 槽位（server×3→agent×3→console×3→kcctl×3）~106s 全成功，platform API 报 rc.10、doctor 25/25（R7 报告 §12.15） |
| 1.3-08 | `kcctl doctor` | ✅ | R3/R4（R4：25 项） |
| 1.3-09 | **`kcctl upgrade all --version` 在线升级（ReleaseManifest 下载）** | ✅ | 下载器行为已经代理隧道实测：可达 GitHub、不存在的版本正确返回 404+离线指引。另：设 `HTTPS_PROXY` 必须配 `NO_PROXY` 排除平台内网地址，否则平台 API 请求也被送进代理而失败。**R20（rc.11 `9a1dbb75`，2026-09-24）正向下载+checksum 闭环**（R7 报告 §12.16）：本地 GitHub 镜像法——dev-2 自签 CA 进系统信任（/usr/local/share/ca-certificates + update-ca-certificates）+ SNI 证书 SAN=github.com + /etc/hosts + 443 HTTPS，供给与真实 release 同构的 `…/releases/download/v2.0.3/release-manifest-v2.0.3.yaml(+.sha256)`；`kcctl upgrade all --version v2.0.3` 全链路 rc=0（"manifest downloaded for v2.0.3"→sha256 校验→版本策略幂等→9 槽位 skip→platform API 复核）；负向：`.sha256` 篡改报 `release manifest checksum mismatch` 拒绝。镜像拆除：443 释放、hosts 恢复、CA 移除 + update-ca-certificates --fresh（临时证书即用即删）。`--manifest` 离线路径见 1.3-07/1.3-10 |
| 1.3-10 | `kcctl upgrade server/agent/console/kcctl` 组件独立升级 | ✅ | `server`/`agent` 独立升级三机实测（B1 E2E：server 先、agent 后，逐台替换，只更新目标组件）。R19（rc.10 `c356fbaf`，2026-09-24）console/kcctl 闭环（R7 报告 §12.15）：`upgrade kcctl` 同 revision 三节点幂等 skip；`upgrade console` 同版本幂等重装（dist/caddy sha256 前后一致、ConsolePort HTTP 2xx 探测）；负向：缺 bootstrap/console artifact 的 manifest EXIT=1、console 错 digest 触碰节点前拒绝（repointed tag 防护）、console 版本策略豁免（v1.6.0 独立版本流与平台 semver 不可比，sourceRevision mismatch 仅警告）。**R20（rc.11 `9a1dbb75`）故障注入矩阵四场景**（R7 报告 §12.16）：T-A server 安装+启动成功但健康探测被 iptables 阻断 → waitServerHealthy 180s 超时 → restoreNodeBinary 自动恢复 rc.10 基线、后续槽位停止（EXIT=1）；T-C staging kcctl 腐化 → 替换后 probe 失败 → restoreKcctl（三节点 md5=基线）；T-D console 探针 180s 超时（错误信息明示 probing http port 80）→ restoreConsole（caddy md5+dist 树哈希逐字节一致、console 200）；T-B 中断后重跑 upgrade all → 已完成节点 skip、余下补齐、platform API 复核 + doctor 25/25 |
| 1.3-11 | SSH key/password、非 root sudo 与自定义端口 | ✅ | R22（rc.15,2026-09-25,commit `9f61c944`+`a064cffe`,R7 报告 §12.18）两缺陷修复并复验：①无 sudo 用户——非交互 sudo 前置探测+提示上限 3 次+非交互 stdin 快速失败（1s 内干净报错,不再死循环刷日志）；②非 root+NOPASSWD sudo join——sudo 前置探测通过后免提示,中转文件改用户 home 相对路径（kc-transit/,/tmp 非 1777 的主机也可用）,join 15s 完成。认证失败报错与自定义端口 2222 复验一致 |
| 1.3-12 | 初始化管理员密码 | ✅ | R22（rc.15,2026-09-25,commit `9d987785`,R7 报告 §12.18）修复并复验：①--initial-password flag 绑定独立字段,Complete 在 config 加载后重施——flag 密码登录 200、config 段密码 401（优先级实证）、默认密码 429 拒绝；②authentication 段 panic 修复（Validate nil 防护）——带段 deploy rc=0（边界:段内 loginHistory* 字段必须给有效值）。泄露面:三次部署密码值在日志/kcctl config/console 0 命中。遗留:deploy-config 顶层 initialPassword 键仍被忽略（正确位置=authentication 段,文档已述） |

## 2. 集群相关操作

### 2.1 创建集群

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.1-01 | 1 master + 2 worker | ✅ | R3 |
| 2.1-02 | 单节点（master 兼 worker）最小规格 | ✅ | R5：AIO 建群+烟测通过；R7：`r7-aio-20260919`（untaint+cross-subnet+first-found）复测通过并删除 |
| 2.1-03 | 3 master + worker HA（lvscare workerNodeVip） | ✅ | R2 已验证 3M+Worker；R4 另验证 3M/0W 边界（需 untaint 才能调度），不替代有 Worker 的 HA 验收 |
| 2.1-04 | 版本矩阵 v1.35.8 / v1.36.4 / v1.37.0 | ✅ | R2/R3 |
| 2.1-05 | CRI containerd 1.7.29 / 2.2.4 | ✅ | R2/R3 |
| 2.1-06 | CNI calico v3.29.6 / v3.31.5 | ✅ | R2/R3 |
| 2.1-07 | 镜像/物料 100% 来自指定仓库（离线保证） | ✅ | R3 验证法可复用 |
| 2.1-08 | proxyMode ipvs | ✅ | R2/R3；R7 复测 kube-proxy ConfigMap `mode: ipvs` |
| 2.1-09 | proxyMode iptables | ✅ | R6：API dry-run 与真实创建均接受 `proxyMode=iptables`；CreateCluster `a16b1a64-02cb-46ce-bdf1-f2a6ff46df87` 成功，kube-proxy ConfigMap 的 `mode` 为 `iptables`。当前 CLI 未暴露该参数，因此用 API 验证，删除已完成 |
| 2.1-10 | 创建集群时拒绝支持矩阵外版本 | ✅ | R3；同版本/降级属于升级校验，见 2.3-08 |
| 2.1-11 | 网络自定义（pod/service 网段、DNS 域） | ✅ | R3 即用即验（172.25/16 + cluster.local） |
| 2.1-12 | apiserver 对外发布（cert-sans / external-domain / external-ip / external-port） | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17） 补齐连通性：--external-domain/--external-ip/--external-port/--cert-sans 建群 → apiserver 证书 SAN 含 DNS:kc-r21.example.com 与外部 IP；hosts 模拟 DNS → https://域名:6443/healthz 200 ok 且 TLS verify=0（外部 IP 直连同样 verify=0）；admin.conf 用内部名，外部 kubeconfig 走 SyncKubeConfig 通道 |
| 2.1-14 | untaint-master（master 允许调度） | ✅ | R5：AIO 创建使用 `--untaint-master`，CoreDNS 和测试 Pod 均成功调度到 Master |
| 2.1-16 | 自带 CA（ca-cert / ca-key 复用已有根证书） | ✅ | R23（2026-09-26，rc.16，R7 报告 §12.19）：r23-ca 自带根 CA 建群 → apiserver/server 证书链由该 CA 签发（openssl verify chain OK），集群 Ready；用后 CA 临时文件即删 |
| 2.1-18 | Calico 默认 VXLAN 网络模式 | ✅ | R4：`Overlay-Vxlan-All` 建群+烟测；R7 复测（r7-min 1M1W，跨节点 ping 0% 丢包、DNS、API via svc） |
| 2.1-19 | Calico IPIP/BGP 或 cross-subnet 网络模式 | ✅ | R5：cross-subnet AIO 通过；R7 复测 `Overlay-Vxlan-Cross-Subnet` 落库+烟测通过 |
| 2.1-20 | Calico IPv4 自动探测（first-found/interface/can-reach） | ✅ | R4/R5 三方法通过；R7 复测 interface（r7-min）、can-reach（r7-matrix，落库验证）、first-found（r7-aio） |
| 2.1-21 | 集群镜像 Registry 与 Package Registry 分离配置 | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17）：packages 走 5003 + images 走自建认证 distribution(9443, TLS+htpasswd) 分离端点建群——建群期间 5003 零镜像 blob GET、9443 日志 containerd authorized 拉取（15 镜像全套） |
| 2.1-22 | 私有 CRI Registry 配置下发 | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17）：--cri-registry 认证 Registry → hosts.toml（server+CA 文件下发+capabilities）与 containerd config.toml registry.configs.<host>.auth 双文件齐备并被消费（401 challenge→authorized 拉取）；注：hosts.toml skip_verify=true 与 CA 并存（registry 资源默认 skip-tls-verify=true，语义冗余）；--ca 接受 PEM 内容而非路径 |
| 2.1-23 | 集群真实断网创建 | ✅ | R3 指定仓库来源；R23（2026-09-26，rc.16，R7 报告 §12.19）：r23-off 断网真机——iptables OUTPUT 仅放行 lo+LAN、其余 REJECT（30,724 包）下建群照常收敛 Succeeded（物料齐备离线通道），取消封锁后集群 Running/Ready |
| 2.1-24 | 1 master + 1 worker 最小多节点规格 | ✅ | R4 通过；R7 复测 `r7-min-20260919`（新候选包，~2.5 分钟 Running），删除后节点复用 |
| 2.1-25 | 集群在线安装 | ✅ | R6：`r6-online-cluster-20260917` 使用 `--offline=false` 建群，Cluster 无 offline annotation，CreateCluster `e11f819e-05ed-403c-8ccf-01a3c2c1263f` 与 SyncKubeConfig 均 Succeeded，packagePlan 落库后删除 |
| 2.1-26 | 节点已被其他集群占用或 Master/Worker 重复 | ✅ | R5/R6 通过；R7 复测占用节点拒绝 `some nodes in used or disabled` |
| 2.1-27 | Master/Worker 跨 Region | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17）：join --agent <region>:<ip> 改节点 region → master(default)+worker(r21-b) 混合创建 400 "nodes belongs to different region"，零对象创建；已删集群 drain 有占用保护、已部署节点 join 有防重入 |
| 2.1-28 | Pod/Service CIDR 非法、重叠或与主机网络冲突 | ✅ | 真实历史：R6 基线 ❌（重叠网段被接受创建 Installing Cluster）；R7 batch-1 修复（`a989b14f`，`netutil.ValidateSubnetOverlap` 接入 API 400 + CLI fail-fast），rc.1 复验重叠 400 拒绝零对象（R7 报告 §9），此后 rc.3/4/5 均生效。**更正（2026-09-20）：R7 新候选基线 ❌ 与 R8 "rc.5 仍接受重叠"两条记录有误**——R9 dryRun 探针实测 rc.5 重叠 400，与代码/单测/§9 复验一致。R9 补齐边界缺口：列表内嵌套、每地址族最多 1 条、IPv4-mapped IPv6 拒绝、主机网段冲突（`ValidateCIDRHostConflict`，API 层取请求节点 `NodeIpv4DefaultIP` 校验；CLI 不拉节点列表故仅服务端），单测覆盖。**rc.6（`29a9bf8a`，2026-09-21）真机负向矩阵复验通过**：8 项非法输入（重叠/列表内嵌套/双 v4 pod/双 v6 service/v4-mapped/主机冲突 pod .0/24/主机冲突 service .208/29）全部 400 且错误理由逐项匹配，.144/28 未含节点 IP 正确放行（边界不误伤），合法双栈与单栈 dryRun 200；CLI 重叠本地 exit 1、CLI 主机冲突转发服务端 400；全程零 Cluster/Operation 残留。2.1-32 双栈仅在单测与 dryRun 层面覆盖（真机无 IPv6 环境），见该行 |
| 2.1-29 | 端口、磁盘、时间同步、主机名等集群预检失败 | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17）：端口占用 → kubeadm preflight [ERROR Port-6443] 快速失败+清晰报错；磁盘 ❌ 缺口确认——kubeadm 1.36 preflight 无磁盘检查（50MB tmpfs 实证 init 通过），磁盘耗尽在运行时以 DiskPressure Evicted+node NotReady+Health 空转呈现（全链路捕获）；时间同步无检查（代码确认）；主机名由 OS 层拒绝非法值（kubeadm 检查实际不可达，正向防护） |
| 2.1-30 | 创建中断后的 retry 或安全删除 | ✅ | R6：取消 CIDR 创建后 Cluster/Operation、节点标签和主机副作用未自动清理，需 reset/精确清理；R8（rc.5，`978b1b43`）失败路径删除已释放节点占用标签、force 删除逃生门可用（`echo yes \| kcctl delete cluster <name> -F`，跳过 agent 卸载、主机残留属预期需运维清理）。**rc.7（`e7d99421`，2026-09-21）真机 retry 验证通过**：Running 中取消（协作式收敛 Canceled/InstallFailed，在途步自然完成、Pending 步取消）后 `kcctl operation retry` 重试同一 Operation——前 6 步保留原时间戳未重做（§3.3-5 语义），仅执行剩余步骤 ~60s 回 Succeeded，集群 InstallFailed → Running；重复取消（终态后与 Running 中并发双取消）分别被 CLI 与 API Conflict 干净拒绝；取消后安全删除 20～30 秒清空（Cluster/Operation/节点标签），同节点重建 2 分钟 Running，kubelet inactive、无 /etc/kubernetes 残留、doctor 25/25。证据：dev-2 /tmp/r10-*.txt。**R16（rc.8，2026-09-23）超时注入补充**：API `POST /clusters?timeout=120` 显式压缩 Operation 期限（Spec.Timeout=2m0s，deadline=02:15:04）——运行中的 `kubeadm init` 步在 deadline 恰好被 SIGTERM（agent 日志 `signal: terminated`），任务回写 `TimedOut`/`DeadlineExceeded` "task deadline exceeded"，Operation 终态 `TimedOut`/`DeadlineExceeded` "operation deadline exceeded"，Cluster→InstallFailed；force 删除后手动镜像卸载语义清理（kubeadm reset、kubelet/containerd disable、缓存与配置目录移除）即达零残留。证据原存 dev-2 /tmp/r16-evidence（终态清理时删除，结论见 R7 报告 §12.12） |
| 2.1-31 | 创建成功后的固定健康验收 | ✅ | API Server、etcd、controller、scheduler、CoreDNS、CNI、kube-proxy、Node Ready |
| 2.1-33 | 离线建群缺省 `imageRegistry` 创建前拒绝 | ✅ | **R17（rc.8）发现产品缺口**：离线 payload 未显式 `imageRegistry` 时 kubeadm.yaml 不渲染 `imageRepository`（默认 registry.k8s.io），离线 init 拉 7 镜像 i/o timeout 慢失败（单 attempt ~18 分钟），创建前零校验。**修复并真机闭环（R18，rc.9 `5e4cfb4b`，2026-09-24）**：`createClusterCheck` 新增离线+空 imageRegistry 前置校验——真实 POST（无 imageRegistry，离线 annotation）**48ms** 400，文案 `offline cluster requires an explicit imageRegistry (a Registry object name); otherwise kubeadm pulls control-plane images from registry.k8s.io which is unreachable offline`；dryRun 同 400、clusters 数组 0（零对象）；对照组带 `kc-package-registry` 的 dryRun 200 正常放行。单测覆盖（handler_test.go 正负两例） |

### 2.2 节点操作

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.2-01 | worker 添加（含 packagePlan 不变性） | ✅ | R2/R3；R7 复测（API add，Operation Succeeded，4 个 slot digest 前后一致）；R17 复测（rc.8 v2 operation 模型真机）：dev-2 入双 master 集群 Ready，Operation 下 28 个 OperationTask（task-XXXX 独立资源，按 stepID×节点派发，spec.operationRef 关联）全部 Succeeded |
| 2.2-02 | worker 移除（含不可 drain 容错） | ✅ | R2/R3；R7 复测（remove Operation 20 秒收敛）；R17 复测（rc.8 真机）：remove Operation 收敛，节点移出集群——集群标签清空、/etc/hosts 集群条目清除、无 kubelet/containerd 二进制残留（注：`/tmp/.k8s` 不随 remove 与 cluster delete 清理，记小观察项） |
| 2.2-03 | **master 添加 / 移除** | ✅ | R4 尝试 Master add 被 API 以 `invalid node role` 拒绝。R17（rc.8）定性为产品缺口：`makeMasterCompare`（pkg/clusteroperation/node.go:101-104）无条件 `return ErrInvalidNodesRole`。**修复并真机闭环（R18，rc.9 `5e4cfb4b`，2026-09-24）**：master add/remove 完整实现（v2 operation 步骤序列与 worker 对称）——双 master 集群（r18-cluster，dev-3+dev-4，离线+`imageRegistry=kc-package-registry`）Ready 后 API add dev-2 role=master → Operation Succeeded、8/8 OperationTasks Succeeded（getJoinCommand 在存活 master、renderMasterJoinConfig `controlPlane=true`、joinNode、waitForAddedNodesReady），dev-2 Ready control-plane，etcd 3 成员全部 started；remove dev-2 → 13/13 OperationTasks Succeeded（removeEtcdMember 在存活 master、drainNode、kubeadmReset、removeEtcdDataDir、clearVIPDomain 等），etcd 回 2 成员、kubectl 无该 Node、/etc/kubernetes 与 /var/lib/etcd（集群 etcd，非 /var/lib/kc-etcd）清空、dummy VIP 接口与 IPVS 规则清除、kubelet/containerd inactive；负向：remove 1 of 2 → 400 `invalid nodes topology: ... would break etcd quorum, at least 2 masters must remain`（直调与 `?dryRun=true` 均拒绝，零副作用） |
| 2.2-04 | master↔worker 角色转换（convertNodes） | ❌ | R21（2026-09-25，rc.11，R7 报告 §12.17） 代码评审确认产品缺口：API 仅 NodesOperationAdd/Remove 两种操作类型，ConvertNodes 是 add/remove 流内子机制（转换节点并入 Masters/Workers），无独立 master↔worker 转换操作入口 |
| 2.2-05 | 节点 disable / enable | ✅ | R3/R4 通过；R7 复测（`PATCH /nodes/{name}/disable|enable` 均 200，禁用期建群被拒，label 实际键 `kubeclipper.io/nodeDisable`） |
| 2.2-06 | 节点失联后操作收敛（agent down） | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17）：建群 T+47s 停 kc-agent——operation 保持 Running、集群 Installing，无假失败无状态污染；agent 恢复后在途步骤不自动续跑（等 90min deadline，与 R16 一致）；cancel→retry 续跑全部步骤成功。注意：升级类操作在 agent 离线时提交会 Pending 死锁（见 gaps P1 新发现） |
| 2.2-07 | agent 节点注销（`DELETE /nodes/{name}`） | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17）：该版本 drain=停 agent 服务+删 Node 对象（一条命令完成注销）；join --pk-file --ip-detect 重建产生新 Node ID；3/3 恢复+doctor 25/25；drain 有集群占用保护、join 有防重入 |
| 2.2-08 | Worker 添加时复用原 `status.packagePlan` | ✅ | R2/R3；各 slot digest 不随 Registry tag 漂移 |
| 2.2-09 | Master 添加后 etcd/control-plane quorum | ✅ | **R18（rc.9 `5e4cfb4b`）真机闭环**：双 master 集群 add dev-2 后 etcd member list 3 成员全部 started（146/230/208），kubectl 3 节点 Ready control-plane，集群 `controlPlaneHealth` 三节点 Healthy、packagePlan 四 slot digest 不漂移（复用既有 5003 物料）；add Operation 8/8 OperationTasks Succeeded |
| 2.2-10 | Master 移除后的 etcd 成员与 VIP 收敛 | ✅ | **R18（rc.9 `5e4cfb4b`）真机闭环**：remove dev-2 后 etcd member list 回 2 成员（146/230 started，被移除成员经 `etcdctl member remove` 显式摘除）、集群回 Running 且 `controlPlaneHealth` 两节点 Healthy；离开节点清理彻底——/etc/kubernetes、/var/lib/etcd、static pod manifests、dummy VIP 接口（kube-lvscare-vip）与 169.254.169.100 地址、IPVS 规则全部不存在，kubelet/containerd inactive，kubectl Node 对象消失。边界标注：本轮拓扑 0 worker，worker 侧 lvscare static pod 的 refreshLvsCare reconcile 路径按设计不生成步骤、未真机覆盖（代码单测覆盖 joining/leaving 过滤，见 masterscale_test.go）；quorum 守卫真机验证（remove 1 of 2 → 400） |
| 2.2-11 | Agent 注册身份与 Node 状态更新保护 | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17）：agent 证书跨节点伪造——注册他节点名 403（cannot register Node）、跨节点状态更新 403（cannot update Node）、自身 resourceVersion 篡改 409（node UID or resourceVersion changed）、正控 200。缺口：GET /nodes/{name} 无名字域隔离（RBAC 未按 resourceNames 限定，任意 agent 可读任意节点全量数据） |
| 2.2-12 | Node Lease、Ready/Unknown 与重连恢复 | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17）：停 kc-agent → 最后心跳+4m00s 精确转 Unknown（nodeMonitorGracePeriod），其余节点不受影响；agent 重启秒级回 Ready；列表状态一致 |
| 2.2-13 | Region 归属、列表和同 Region 调度约束 | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17）：同 2.1-27 场次——跨 Region 创建 400 拒绝即同 Region 调度约束实证；节点列表 region 列正确显示（join --agent <region>:<ip> 语法）；集群 REGION 字段随 master 归属 |

### 2.3 升级与证书

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.3-01 | 真实滚动升级 1.36.4→1.37.0（master→worker drain） | ✅ | R2/R3；R7 在新候选上复测 ~90 秒完成，两节点 v1.37.0 Ready |
| 2.3-02 | 升级中途失败 → op retry | ✅ | R22（rc.15）修 Pending 死锁；R23（2026-09-26，rc.16，R7 报告 §12.19）主流程闭环：R22 "步骤执行丢失"经立项复查为**误诊**（R22 监控用错 etcd 前缀 + API list 无参返回空——16 个任务对象一直在；真正缺陷=drain PDB 无限重试死锁，已修 `3be0ffe8`：`kubectl drain --ignore-daemonsets --timeout=120s \|\| true`）。真机 retry：卡 drain 的升级 op 强杀收敛（Canceled）→ retry 新 spec op → 复用不可变 plan（steps 哈希 c0d395a025a134c8 前后一致）→ Succeeded、节点 v1.37.0 |
| 2.3-03 | 升级失败 → 集群状态恢复（reset status） | ✅ | R3 实际使用 |
| 2.3-04 | **集群证书更新（/certification）** | ✅ | R6 通过；R7 复测 serial `2F3488595FA57564`→`3E1B08278426788C`、有效期+1y，节点 Ready |
| 2.3-05 | agent 证书重新签发 | ✅ | R6 通过；R7 复测 drain→join 后 serial `33644819B666ED24`→`35C8E2A87BEB075B`，CN 与新 Node 一致 |
| 2.3-06 | 升级前后 `packagePlan` 变更边界 | ✅ | R23（2026-09-26，rc.16，R7 报告 §12.19）：retry 前后 op.Spec.Steps 哈希一致（c0d395a025a134c8）——plan 在创建时物化，后续 manifest 版本演进（rc.15→rc.16）与 tag 重指均不回写既有 op；平台升级 12 槽位解析正常（R22 §12.18） |
| 2.3-07 | Registry tag 变化后 Operation retry 仍使用原 digest | ✅ | R23（2026-09-26，rc.16，R7 报告 §12.19）证据级闭环：retry 消费创建时物化的不可变 plan（steps 哈希 c0d395a025a134c8 前后一致），digest 已在 plan 中定死、与 registry 侧 tag 后续指向无关；发布器 tag 冲突防护（拒绝重指）同轮实证（R22 §12.18）。残留：真 tag 重指的 registry 侧场景仍受共享 5003 只增 tag 约束未实跑，但机制上已排除影响 |
| 2.3-08 | 同版本、降级、跨越不支持版本升级拒绝 | ✅ | R3；R7 复测同版本/降级均在创建 Operation 前拒绝 |
| 2.3-09 | Master/Worker 滚动顺序与业务可用性 | ✅ | R23（2026-09-26，rc.16，R7 报告 §12.19）：2 节点（master→worker）滚动升级 62s 完成——master 先行、drain 期间业务负载收敛、ErrIgnore 容忍 drain 阶段性失败、升级后双节点 v1.37.0 Ready；单节点 1M 拓扑的 drain PDB 死锁边界同轮修复（2.3-02 行） |

### 2.4 删除集群

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.4-01 | 正常删除（含与 SyncKubeConfig 并发死锁回归） | ✅ | R2/R3；应固定验证 K8s 组件清理、平台状态和节点可复用 |
| 2.4-02 | InstallFailed 状态删除 | ✅ | R3 |
| 2.4-03 | 删除失败 TerminateFailed → 重试 | ✅ | R2 |
| 2.4-04 | 有备份时删除保护（引导先删备份） | ✅ | R3 |
| 2.4-05 | 删除后 ops 账本清理 | ✅ | R3 |

### 2.5 备份与恢复

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.5-01 | backuppoint fs 型（含非法类型拒绝） | ✅ | R3/R5；R7 复测 FS 手动备份 `available`（11,440,160 bytes） |
| 2.5-02 | backuppoint **S3 型（MinIO）** | ✅ | R5：MinIO 全链路通过；R7 因 MinIO 停止分发二进制改用 seaweedfs S3，手动备份 `available`；endpoint 须为裸 host:port（入口校验缺失见 R7 新问题 8） |
| 2.5-03 | 手动备份 → 恢复（marker 回滚证明） | ✅ | R2/R3；R7 复测（恢复 Operation Succeeded，marker ConfigMap 回滚消失） |
| 2.5-04 | 备份删除（连带存储文件清除） | ✅ | R3/R5；R7 复测 FS 文件随对象删除同步消失；R11（rc.8 `e9d9afeb`）复验持久化删除流：deleting → 删除 Operation Succeeded 后记录 404、文件消失（2.5-08 轮转不清理的历史问题已一并闭环） |
| 2.5-05 | cronbackup runAt 单次触发 | ✅ | R3 |
| 2.5-06 | cronbackup 真实周期命中 | ✅ | R5；R7 复测分钟级 Cron 连续触发多次 |
| 2.5-07 | cronbackup enable / disable 子资源 | ✅ | R5；R7 复测（禁用 90 秒无新增，启用后恢复） |
| 2.5-08 | **maxBackupNum 超限自动轮转** | ✅ | R5/R7 曾 ❌（对象轮转但旧 FS 文件残留）；R11（rc.8 `e9d9afeb` B4 持久化删除流）真机复验：`maxBackupNum=2` + 2 分钟周期 Cron 连续 4+ 轮触发，Backup 记录与 FS 文件逐轮一一对应、无孤儿文件（删除统一由 backupcontroller 在删除 Operation Succeeded 后执行） |
| 2.5-09 | 恢复后集群可用性（addons/节点完整） | ✅ | R3；R11 复验：恢复 Operation Succeeded 后集群回 Running（r11-b4-cluster），节点 Ready |
| 2.5-10 | 备份损坏或错误 S3/FS 凭据 | ✅ | R11（rc.8）真机复验：错误凭据/不存在 bucket → Operation 明确 Failed、Backup `error`、集群保持 Running（N4 回归通过）；删除该记录经持久化删除流幂等清理后消失 |
| 2.5-11 | Backup 详情查询 API（`GET /backups/{name}`） | ✅ | R5/R7 曾 ❌（已有 Backup 仍 404）；R11（rc.8）真机复验：已有 Backup 200 全字段（backupStatus/clusterNodes/preferredNode），不存在对象 404 |

### 2.6 OCI 制品解析、缓存与消费

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.6-01 | 创建集群解析并持久化 `status.packagePlan` | ✅ | R2/R3；R7 验证新候选包 digest（`ebe86ff7` 等）落库并被扩容 Operation 复用 |
| 2.6-02 | packagePlan 不保存 blob 或 Registry 凭据 | ✅ | R16（rc.8，2026-09-23）取证：直接 GET API 创建的 Cluster 对象（6930 字节），`packagePlan` 字段集合与 schema（ResolvedArtifactPlan/ResolvedComponent/TransportRef/ArtifactContent）完全一致，整对象对 `password|credential|token|secret|username` 大小写不敏感扫描 0 命中；plan 仅含 version/os/arch/slot/kind/name/required/transport{type,ref,digest}/contents[{name,file,digest,mediaType}]，blob 与凭据不落 etcd 的设计成立（types.go 编码注释同义）。注：dangling blob 的 Agent 拉取路径（R15 gzip CRC 阻断同源）与计划内"创建前预检 404 拒绝"互补 |
| 2.6-03 | Agent 首次按 digest 拉取、校验、解包和执行 | ✅ | R2/R3 建群主路径 |
| 2.6-04 | Agent 命中本地校验缓存 | ✅ | R21（2026-09-25，rc.11，R7 报告 §12.17） 直接证据：冷/暖两次建群 Registry 访问对比——冷：charts/tigera-operator 1 manifest+1 blob GET；暖：零请求 + "calico chart packages offline install successfully"（agent 本地缓存命中）；边界：k8s 二进制包在集群删除后重拉（delete 清节点包目录，chart 缓存为 agent 生命周期） |
| 2.6-05 | 缓存损坏或 digest 不符 | ✅ | R12 篡改探针（rc.8 三机）：master `charts.tgz` 翻一字节（sha256 变化）→ 建群时 `validCachedHelmChart` payloadDigest 校验拒绝 → digest-pinned 重拉（registry 侧 tigera-operator blob GET 佐证）→ sha 恢复原值，Operation Succeeded；包 contents 路径同构校验（`loadCachedComponent`/`packageFilePayloadDigest`）。R15 包 contents 路径真机探针（registry 侧 blob 篡改，9443 测试源）：浅篡改 → server indexer `archive/tar: invalid tar header` 剔除出清单；显式指定 → CLI fail-fast `missing packages: containerd 1.7.29`（EXIT=1 零对象）；深篡改（数据区）→ 两节点 agent 侧 gzip CRC 阻断（`gzip: invalid checksum`）3 秒 Failed 无半装；恢复 blob 后清缓存显式建群 Running（排除误伤）。未回退 tag |
| 2.6-06 | Registry 暂时不可达时的缓存行为 | ✅ | R12 断连：retry 中途 kill registry → in-flight containerd 拉取经 graceful shutdown 完成，后续任务 `dial tcp: connect: connection refused` 明确失败；registry 恢复后 retry → 三节点 k8s 包全部从头重拉（56MB layer ×3，半成品未被信任），Succeeded、集群 Running。删除集群时组件包缓存随 uninstall 清理（无陈旧缓存残留）。R15 佐证：两节点 1.7.29 缓存手动清除后建群真实走 registry 重拉（9443 GET 200），恢复后的 blob 消费正常 |
| 2.6-07 | Package Registry 配置优先级与文件权限 | ✅ | R6/R7 实测两个配置文件 0644。R9 更正：写入侧自 batch-1 `a989b14f` 已强制 0600（Config.Dump 与 deploy-config WriteToFile，目录 0700），存量 0644 是旧版本写入的遗留，重写时收紧；R9 补齐原子替换（同目录 0600 临时文件+Sync+rename，失败保留原文件）、拒绝符号链接、umask 无关性与 §4.4 回归测试。**rc.7（`e7d99421`，2026-09-21）远端复验**：dev-2 `/root/.kc/config` 与 `deploy-config.yaml` 实测 0600（升级流程重写后收紧生效）；dev-3/dev-4 无 .kc 目录（kcctl 仅在 dev-2 执行）。**R16（rc.8，2026-09-23）专项补测**：①优先级——server 侧 delivery/package-registry.json 改指 9443 而 deploy-config `packageRegistry` 保持 5003 时，componentmeta 与 dryRun packagePlan 全部仍解析至 **5003**（deploy-config 单一事实源成立，json 按地址匹配仅提供 scheme/凭据）；反向（json=5003/dc=9443）时 componentmeta 返回 `registry: 172.16.131.146:5003` 即 json 覆盖 deploy-config 的记录**作废**——R16 证实以 deploy-config 为准，此前 dc=json 双写相同值无法区分；②凭据脱敏——8443 htpasswd 认证 registry：正确口令 componentmeta 200、错误口令 401 并留在 registry 侧；口令写入 server json、GET API 与 kc-server/kc-agent journal 及 /etc /root /var/lib/kubeclipper 全量 grep 均 0 泄漏（R16 补注：本轮部署-config 优先级下 server json 凭据随 json 与 deploy-config 解耦，不再下发到 agent 路径）。2.6-07 关闭 ✅ |
| 2.6-08 | Delivery Policy 默认策略初始化 | ✅ | R3 默认策略已实际用于制品解析 |
| 2.6-09 | Delivery Policy 自定义版本白名单 | ✅ | R6 通过；R7 复测（`v1.36.*`→`v1.34.*` 后创建 v1.36.4 在 Operation 前拒绝，策略精确恢复） |
| 2.6-10 | Delivery Policy 缺失 slot/repository | ✅ | R6：移除 `cni` slot、将 `calico` 改为不存在的 `missing-calico` 后，均在创建前拒绝且无 Cluster/Operation。**R16（rc.8，2026-09-23）余量补齐**：①缺失 blob——9443 测试 registry（distribution 3.1.1）skopeo 拷入 containerd:1.7.29 后删除 layer blob 数据文件：server 索引器读包清单（需取 blob）失败→tag 记 `skip invalid OCI package image` 从清单剔除→POST 显式容器运行时返回 `ArtifactNotPublished: artifact cri/containerd:1.7.29 is not published` 400，零对象（distribution 对 blob 路径 digest 不符不做在线校验、200 的观察项不改变创建前拦截结论）；②多候选冲突——policy `k8s-v1.35` 复制出第二个允许同版本 containerd 的 slot（cri-alt）：dryRun 与真实 POST 均 400 `DuplicateResolvedComponent: component cri/containerd selected by slots "cri" and "cri-alt"`（同 slot 重名直接 400 `duplicate component slot "cri"`；单 slot 双 option 名字不同不触发——按 name 匹配 option）。policy/deloy-config 双还原，验证 dryRun 200 |
| 2.6-11 | bootstrap/standalone extension 与集群 packagePlan 隔离 | ⚠️ | R21（2026-09-25，rc.11，R7 报告 §12.17） 代码评审维持：k8s-extension 是节点操作内嵌步骤（clusteroperation/node.go 四处 InstallStepsWithContext），resolver 有参与过滤逻辑，但无独立用户命令入口（CLI 无 extension 安装命令），真实命令证据不可达 |

## 3. `kcctl` 命令覆盖（每条至少跑通一次核心路径）

> 命令面以待测版本的 `kcctl --help` 为准。平台 Registry 资源、独立 Docker Registry 管理、
> OCI package Registry 是三个不同概念，测试记录中必须写清目标对象。

| 编号 | 命令/用法 | 状态 | 最小验收点 |
|---|---|---|---|
| 3-01 | `kcctl deploy` | ⚠️ | 在线、同步仓库、纯离线分别记录；目前纯离线未通过，不能整体标 ✅ |
| 3-02 | `kcctl clean --all` | ✅ | R3；清理后可重部署。当前不支持单节点/按角色 clean |
| 3-03 | `kcctl doctor` | ✅ | R3/R4；R4 为 25 项，异常项、节点和退出码准确 |
| 3-04 | `kcctl join` | ✅ | R6 独立 join 主路径；R13-C4 错误凭据与失败清理（认证 registry 下 0600 凭据下发、错误口令 EXIT=1 零残留）；R21 已部署节点 join 防重入 + 已删集群 drain 占用保护（2.1-27）；2.2-07 drain→join 重建闭环（3/3 恢复 + doctor 25/25） |
| 3-05 | `kcctl create cluster` | ✅ | R4：CLI 实建 `ha-core-20260917`（3M/0W）和 `min-core-20260917`（1M/1W），参数与 packagePlan 落库；3M/0W 需显式 untaint 才能调度 CoreDNS |
| 3-06 | `kcctl create/delete user`、`role` | ✅ | R6：CRUD/重名拒绝通过；R7 复测 CRUD 通过；R7 batch-3（v2.0.3-rc.2）重名 user 改为 400 Bad request `already exists`（原 500），自定义 role 授权见 4-08b 改判 |
| 3-07 | `kcctl create/delete registry` | ✅ | 管理平台中的集群镜像 Registry 资源；基础 CRUD 已验证 |
| 3-08 | `kcctl delete cluster` | ✅ | R4：CLI 删除 `ha-core-20260917`、`min-core-20260917` 均返回成功并最终 NotFound；删除后平台 doctor 仍为 Healthy |
| 3-09 | `kcctl get cluster/node/user/role/configmap/registry` | ✅ | R6 六类资源 singular list/JSON 形状/Node 双 selector；**R24 补验 User selector**：labelSelector `r24=a` 精确命中、fieldSelector `spec.email=` 命中、无匹配空集（旧行"User label selector 未按预期"结论已作废） |
| 3-10 | `kcctl get --watch` | ✅ | R7 batch-3（`12d59d9b`）修复：持续输出 watch 事件；服务端流在 watch 超时后关闭时 2 秒退避重连并继续，Ctrl-C 正常退出。服务端流快速关闭的根因（watch 超时漏乘 time.Second）已在 R7 N9 修复（`b23a9ab2`，rc.3 验证流持续、实时事件送达） |
| 3-11 | `kcctl operation list/describe/logs/retry` | ✅ | list 按集群筛选；logs follow 增量不重复；retry 终态限制正确 |
| 3-12 | `kcctl operation cancel` | ✅ | R5/R6 旧缺口（需重启 Server、孤立 Running）已由 R8 饿死根因修复（`1413e849`）+ R9/R10 真机矩阵闭环：Running 中取消协作式收敛、最早取消 <23s、终态后重复取消 CLI 干净拒绝、并发双取消 API Conflict、无需重启。R23 复验：卡 drain 的升级 op 取消收敛（Running→Canceled）。证据见 gaps P0 行 7（R7 报告 §12.6） |
| 3-13 | `kcctl cluster upgrade` | ✅ | R3；参数传递、滚动顺序和最终状态正确 |
| 3-14 | `kcctl set cluster` | ✅ | R3/R4：external IP/port 设置与 clear 均成功，get 输出中的 labels 随之出现/清除 |
| 3-15 | `kcctl drain` | ✅ | R6 空闲 Agent drain；R24 used 保护与不存在节点报错；**R26 文案/交互修正**（"drainded"→"drained"、`-y` 生效、无 TTY 干净拒绝）；**R27 完整生命周期闭环（rc.24）**：drain dev-4 → "agent node drain completed"（2/3）；**重复 drain 同 ID** → 无 `-y` 时确认提示+干净非交互提示，`-y` 时 `drain agent node failed: ... not found`（幂等）；`kcctl join`（server 侧执行，`--ip-detect interface=ens3 --node-ip-detect cidr=... --package-registry-scheme http`）重新纳管 → **新 Node ID**（4e362f79→a1ac1855）、3/3 agents Healthy、**doctor 25/25**。边界：`-F` 强制删在用节点未单独跑（R18 master remove 已覆盖同族强制路径） |
| 3-16 | `kcctl upgrade all --manifest/--version` | ✅ | B1 按 OCI 契约重写并移除旧 `--pkg/--online`：`--manifest` 三机实测通过（幂等复跑全 skip、降级明确拒绝、错 digest 拒绝，R7 报告 §11；R19-R23 各轮平台升级持续复跑）；`--version` 在线正向下载 R20（§12.16）以本地 GitHub 镜像法闭环（自签 CA 进系统信任 + /etc/hosts + 443 HTTPS，URL 与真实 release 同构；全链路 rc=0、`.sha256` 篡改报 checksum mismatch 拒绝、临时证书即用即删） |
| 3-17 | `kcctl upgrade server/agent/console/kcctl` | ✅ | `server`/`agent` 独立升级实测通过（节点级幂等：已达标 revision 跳过，不重启）；`console`/`kcctl` step 2 已于 R19（§12.15）实施并真机验证（console：停服→备份→替换→起服→HTTP 探活；kcctl：替换后 `kcctl version` 校验；同版本重装幂等、降级拒绝），R22/R23 各轮 `upgrade all` 四组件链路持续复跑 |
| 3-18 | `kcctl registry sync` | ✅ | R2/R3；Release Manifest、首次/增量同步、认证和 digest 一致 |
| 3-19 | `kcctl registry list/deploy/clean/push/delete` | ✅ | R6/R7 list/image/非法 push/`--registry-port`；**R24 补验**：repository 列表（`--node` + `--registry-port 5003`，只读）、delete 不存在 tag → fetch-first 干净报错（`MANIFEST_UNKNOWN`，零变更）。边界：对共享 5003 的真实 clean/delete 受"只增 tag"约束未做（策略性排除） |
| 3-20 | `kcctl resource list` | ✅ | R3/R4：14 个 OCI package；R7 复测（新候选包同步后仍 14 个，digest 为新构建） |
| 3-21 | `kcctl resource inspect` | ✅ | R3/R4：指定 package 可展示 repository、tag、digest、platform 和内容画像 |
| 3-22 | `kcctl resource refresh` | ✅ | R3/R4：强制扫描指定 Registry，输出 `refreshed 14 OCI packages`；不将其解释为持久缓存更新 |
| 3-23 | `kcctl delivery-policy template/get/apply/diff` | ✅ | R4：模板生成、custom `allowedVersions` apply/get、diff 和恢复均通过；策略文件的非法 selection 被拒绝 |
| 3-24 | `kcctl delivery-policy validate` | ✅ | R4 合法模板/`selection: many` 拒绝；**R24 补验**：`delivery-policy template -o yaml` → `validate` "delivery support policy is valid"；白名单制品存在性/冲突由 R16（2.6-10）经投递 dryRun 实证（`ArtifactNotPublished`/`DuplicateResolvedComponent` 400） |
| 3-25 | `kcctl login` | ✅ | R3/R4/R7：错误密码 unauthorized 一致；正确登录 R22（§12.18，deploy `--initial-password` 后登录 200、config 段密码 401 的 flag 优先级矩阵）与 R23 复验闭环 |
| 3-26 | `kcctl status` | ✅ | R4：准确报告三台 `kc-server`、三台 `kc-etcd`、三台 `kc-agent`，平台 Healthy |
| 3-27 | `kcctl deploy config` | ✅ | R6/R7 生成/非法 YAML 拒绝；**R24 补验**：生成模板 OK、非法 YAML 秒级干净报错、顶层 initialPassword 拒绝（R23 文案）、authentication 段零值回填（R24 单测 + 平台配置实证，见 4-18） |
| 3-28 | `kcctl version` | ✅ | client/server 版本和 Git revision 准确 |

## 4. 其他核心平台功能

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 4-01 | addons：nfs-csi 安装/动态供给读写/卸载 | ✅ | R2/R3 |
| 4-02 | addons：metallb L2 安装/卸载 | ✅ | R3 |
| 4-05 | addons：uninstall 容错（空 config / ErrIgnore 链） | ✅ | R3 三修复合验 |
| 4-06 | 可观测：operation logs、失败原因展示（API 侧） | ✅ | R3 |
| 4-07 | **console UI 端到端（含任务失败展示）** | ❌ | fork console 分支未配镜验证 |
| 4-08 | 用户 / 角色 CRUD、enable/disable、改密、登录记录 | ✅ | R24（2026-09-26，rc.17-rc.20，R7 报告 §12.20）全矩阵真机：创建/重复名 400/读取（不回显口令）/fieldSelector 列表/HEAD/更新（含 URL 名不匹配 400）/改密（旧口令 401、新口令 200、错当前口令 400）/disable→登录 403→enable→200/删除（幂等 200、404 复核、admin 受保护 400、未带密码 400、弱口令被接受=记录项）/角色聚合建（`--rules=role-template-*` 语义）与重复 400/更新（无 resourceVersion 亦可）/internal 角色删改 400 保护/用户角色查询。**登录记录修复前恒 0 条（namespace 缺失写入全败），rc.17 起真实落库**（type/provider/sourceIP/userAgent/success/reason 全字段） |。**R28 追加**：口令策略落地——用户创建/改密现按 8-16 位且含大小写字母与数字校验（与 admin 初始口令同规则），弱口令 400 并给出说明（真机：`123456` 建用户 400、`Abcd1234` 200、弱口令改密 400）
| 4-08b | RBAC 鉴权拦截（非管理员越权应 403） | ✅ | R7 判 ❌ 后于 batch-3 复验改判：403 归因于测试用错注解键（应为 `iam.kubeclipper.io/role`）；正确键下创建 binding 后授权内 `GET /clusters`、`/nodes` 200，越权创建 Registry 403 |
| 4-08c | 密码登录与验证码登录 | ✅ | R24（rc.19，§12.20）验证码全流程真机（fake_sms provider）：口令正确→**428 返回 provider 列表**→发送验证码（journal 落码）→校验→签发 token；错码 401、**一次性**（复用 401）、**过期拒绝**（ttl 60s，65s 后 401）、**重发限流**（间隔内被拒、窗口后成功）。修复链：provider 包未导入导致配置即 crash-loop、验证码路径 url.Values nil map panic、限流标记与验证码共用 key 撞 AlreadyExists、透明 token 不清理——均已修复并复验。第三方 OAuth 登录属第 7 章扩展项（7-01），不在本行 |
| 4-08d | 长期 token 创建、查询、撤销与过期 | ✅ | R24（rc.17/rc.20，§12.20）：token 由登录签发（JWT，`accessTokenMaxAge=2h`，exp 声明实测；无独立创建路由，CreateTokens handler 未注册=记录项），`GET /tokens` 列表 + describe 可用，logout 撤销后旧 token 401（Content-Type/Accept 需 JSON，否则 406=记录项），TTL 透明对象由 tokencontroller 清理——修复前过期对象永不删除（MFA 码键长生），rc.19 起实测过期键被修剪。**审计泄漏已修复**：用户创建（spec.password）与改密（currentPassword/newPassword）及 token 值原先明文进审计事件，rc.20 起 `[REDACTED]`（真机 grep 0 明文） |
| 4-12 | kubeconfig 下载 | ✅ | R6 文件可用/权限；R24 补验三态；**R26 状态码修正**：不存在集群 → **404**、建群未就绪 → **503**（`cluster kubeconfig is not ready` / 控制面 6443 不可达，typed error 映射；修复前均 500），就绪集群 → 200 返回 kubeconfig |
| 4-13 | 平台自省：/configz、/status、/components、/componentmeta | ✅ | R6 四端点 200；R7 复测一致 |
| 4-14 | 审计事件查询（/events，auditing 组） | ✅ | R6 通过；R7 复测（本轮全部操作均有审计记录） |
| 4-16 | 敏感信息脱敏 | ✅ | 旧缺口已闭环：写入侧权限收紧自 batch-1（`a989b14f`，R9 B3：0600+原子替换+拒绝符号链接）；**R23 复核（rc.16）**：三节点 `/root/.kc/config` 与 `/root/.kc/deploy-config.yaml` 实测均 **0600**（R6 的 0644 为旧版遗留）；日志泄漏证据：R12/R16 htpasswd 口令在 server json / GET API / kc-server/kc-agent journal / /etc /root /var/lib/kubeclipper 全量 grep 0 泄漏，`/metrics` 无 password/token/secret/private-key 字段名 |。**R24 追加（rc.20，§12.20）**：审计事件原先明文保留用户创建 `spec.password`、改密 `currentPassword/newPassword` 与 token 值（真机审计流与持久化事件均可见），已加入 redactAuditFields 白名单 → `[REDACTED]`，真机复验 journal grep 0 明文
| 4-17 | `/healthz` 与 `/metrics` | ✅ | R6 均 200、metrics 108 行无 password/token/secret/private-key 字段名；R7 复测一致；R24 全轮多次复查 `/healthz` 200（各候选升级后健康核验均用此端点） |
| 4-18 | 登录失败限流与恢复 | ✅ | R24（rc.17，§12.20）真机全矩阵：5 次错口令 → 计数达阈值（第 5 次 401 且 reason "auth rate limit exceeded"，其后 429 文案含窗口时长）、正确口令窗口内同样 429、按用户隔离、窗口过期（2m）后 200、成功登录清零。**重大修复**：部署侧零值回填 + 运行时 MaxTries<=0 视为禁用 + Duration<=0 回退 10m（修复前 `=0` 使首次错误即永久锁死，实测平台 admin 被锁）。R26：MFA 重发限流由 500 修正为 **429**（`mfa.ErrRateLimited` 映射，真机 send1 200→send2 429） |
| 4-19 | Addon OCI chart/runtime-image-set 来源与 digest | ✅ | R2/R3；NFS CSI、MetalLB 主路径来源检查可复用 |
| 4-20 | Addon 安装失败后的 retry/卸载清理 | ✅ | R24（§12.21，r24-1m）真机：①非法 config（scName `Bad_Name!`）→ **R26 起 400** `invalid component config: invalid name of storage class`（`validation.ErrInvalidComponentConfig` 统一映射；修复前 500），零副作用；②正向安装 nfs-csi → SC + csi-nfs controller/node pods Running；③安装 op 末步 checkCSIHealth 曾长时间 Running（R25 已修 step timeout，现 ~3min/次超时收敛）；④排队卸载 op 获锁后 Succeeded，集群侧 SC 消失、nfs pods 0 残留 |

## 5. Operation V2 核心可靠性

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 5-01 | 创建、扩缩、升级、备份、恢复使用 Operation V2 | ✅ | R2/R3 主路径；Operation/Task/ExecutionLock 可追踪 |
| 5-02 | Operation/Task 展示节点 UID、IP、步骤和错误 | ✅ | R3 排障实际使用 |
| 5-03 | Server/Agent 重启后的 Operation 恢复 | ✅ | R2/R3；已完成 Task 不重复，锁最终释放 |
| 5-04 | Agent Watch 410/EOF 后 relist | ✅ | R2 有真实 410 样本，不漏任务、不重复并发执行 |
| 5-05 | 多节点 Step barrier 与输出传递 | ✅ | R25（§12.22，r25-1m1w）真机：CreateCluster 15 步中 4 步双 target → 每节点各 1 个 task（NodeRef 区分 master/worker），19 tasks 全 Succeeded、无 step×node 重复；**输出传递**：worker 侧 join 任务的 payload 携带 master 步骤产出的 `kubeadm join` 材料，集群 1M1W Running |
| 5-06 | Operation cancel | ✅ | R5/R6 旧缺口（需重启、孤立对象）已由 R8 饿死根因修复（`1413e849`）+ R9/R10 真机闭环（协作式收敛语义矩阵、ExecutionLock 释放、无重启）；R22 追加幽灵锁驱逐（acquireLock 失效持锁者驱逐）；R23 复验升级 op 取消收敛（卡 drain → Canceled）。证据见 gaps P0 行 7（R7 报告 §12.6/§12.18） |
| 5-07 | 自动 retry 与人工 retry | ✅ | R25：`RetryLimit=min(RetryTimes, 3)`（validation 0..3）、`nextAttempt` 判 `attempts < 1+RetryLimit` 否则 "exhausted its retry limit"；真机两次自动重试耗尽实证（坏 gate 建群、nfs 健康检查各 2 次尝试后 op Failed）。人工 retry=R23（复用不可变 plan，steps 哈希 c0d395a025a134c8 不变；digest 在 packagePlan 物化）。记录项：scheme 的 `AutomaticRetry` 字段未被消费（重试只由 RetryTimes 驱动） |
| 5-08 | 同集群危险操作互斥 | ✅ | R25 真机双形态：①集群删除在 InstallComponents 在飞时被拒（400 `can't delete cluster when cluster is Updating`）；②同目标 op 由 ExecutionLock 串行（排队卸载 op 在安装 op 释放锁后 Succeeded）。叠加 R22 幽灵锁驱逐（失效持锁者永不堵队） |
| 5-09 | Task 终态写入成功但响应丢失 | ✅ | 代码级+单测（§12.22/§12.24）：派发前 `getLiveTask` 重读服务端任务，**终态即跳过**（`live.Status.Phase.IsTerminal()` → 不入队执行）；`finish` 在 PUT 失败后 GET 复核，服务端已终态视为成功（响应丢失）、任务被 purge 视为已决；任务名确定性（opUID/generation/stepID/nodeUID/attempt）且 AlreadyExists 时比对 spec 否则 `invalidExecutionFacts` fail-closed。**单测直击该场景**：`TestWorkerAcceptsPersistedTerminalAfterLostResponse`（fake `loseTerminalResponse`：终态已持久化但响应丢弃 → 视为成功、不重放）、`TestWorkerReturnsPromptlyWhenTaskPurgedAfterTerminalUpdate`。说明：真机 iptables 无法复现"写入落地仅响应丢失"的时序（只能整段阻断，属"写入未落地"的另一场景，由 R2/R3 重启恢复覆盖），故以单测闭环 |
| 5-10 | 无法确认副作用时 fail-closed | ✅ | 代码级（§12.22）：`invalidExecutionFactsError`→`failInvalidFacts`——取消同批 Pending、op 以 `InvalidExecutionFacts` Failed，绝不猜测成功；尚有 Running 任务时只 requeue 等待不推进；锁随终态释放。配合 5-09 的 spec 比对，不确定性一律转失败而非重放 |
| 5-11 | Operation API `limit/continue` 稳定游标分页 | ✅ | R3 API 已验证；无重复遗漏。当前 `kcctl operation list` 未暴露分页参数 |
| 5-12 | Task 日志 offset 与 Task 切换 | ✅ | R25 真机：同一 task `offset=0&limit=300` 与 `offset=300&limit=300` 两段内容不同（1147B 全量切片）、另一 task 的日志前缀与之不同（不串流）。API 经 `operationtasks/{name}/logs` 代理到节点 agent 的日志服务 |
| 5-13 | 相同业务请求重复提交 | ✅ | R25 真机：连续两次相同的 nfs-csi 安装请求——第二次 400 `nfs-csi-v1 component has been installed in the current cluster`（未产生并行重复 op）；叠加 R22 的 CRI-registry 对账去重（存在非终结 op 不再新建）。注：直接 POST Operation 需完整 spec（uid/steps），非用户路径 |
| 5-14 | Operation/Task timeout | ✅ | R16 已验操作期限（task TimedOut/DeadlineExceeded→op TimedOut→Cluster InstallFailed）。**R25 发现并修复 step 级 timeout 从未被执行**（agent 只挂 operation deadline，`utils.RetryFunc` 无界循环 → nfs checkCSIHealth 声明 3m 实挂 >6min 且 cancel 只能等）：命令执行器现以 `payload.Step.Timeout` 派生 ctx，超时经 worker 映射为 TaskTimedOut、ErrIgnore 步骤仍容忍任意终态。真机：健康检查步 ~3m/次超时、op ~4min 自动收敛 Failed（"exhausted its retry limit"），修复前会挂到 90min 操作期限；单测：150ms step timeout 截断 30s 任务期限 |

## 6. 发布工程与交付门禁

> 本章不是用户功能菜单，但决定发布物能否被前述核心链路可靠消费。CI/单测通过只记 ⚠️，
> 只有发布候选制品被真实部署和使用后，才能把相应端到端结果记为 ✅。

| 编号 | 门禁 | 状态 | 备注 |
|---|---|---|---|
| 6-01 | 支持策略与 `packaging/resources.yaml` 一致 | ✅ | `release-policy-verify` 在 CI 双处执行：qualification workflow prepare job（`--publish-matrix`，run 36216971172 通过）与 release.yml prepare job；候选证据=R23 qualification 成功轮 + 各 rc 升级矩阵仅消费策略内版本（R21-R23） |
| 6-02 | bootstrap、Kubernetes、CRI、CNI、extension、addon OCI 制品完整 | ✅ | R23 qualification 候选 manifest（run 36216971172，sha256 2e5bf6be…）枚举 **110 个制品**：package-image 10（bootstrap kubeclipper/console/etcd/registry、cri containerd、cni calico、k8s、k8s-extension、kc-runtime、addon）+ helm-chart 2（tigera-operator）+ runtime-image 98，组件种类齐全；真实消费见第 1/2/4 章（rc.16-rc.22 部署/升级/建群） |
| 6-03 | Release Manifest 包含 package、chart、runtime image 和 bootstrap | ✅ | 同一候选 manifest 四类齐备：bootstrap（kubeclipper/console/etcd/registry package-image）、package-image（k8s/cri/cni/extension/addon/kc-runtime）、helm-chart（tigera-operator）、runtime-image（98 条，k8s-extension 附带）；`verify-release-manifest.sh` 在 qualification CI 内通过 |
| 6-04 | digest、source、revision、version provenance 正确 | ⚠️ | 升级侧 revision 防护已真机验证（R13-C5 双拦截）；R14：发布侧 bootstrap SourceRevision 必填（单测）+ `release-gate.sh` 门禁+release workflow `release-gate` job（fixture 自测 11 例，见 plan §7.6）。**R23（§12.19）：门禁+首个验收记录已闭环**——qualification run 36216971172（sourceRevision=候选 sha）、验收记录钉板 manifest sha256、gate 对真实 manifest PASS + 两类 BLOCK（未 bump tag / 篡改记录）实证。**剩余**：真实 stable 发布轮（resources.yaml bump → tag → release workflow 实跑）后升 ✅ |
| 6-05 | 制品不可变性与重复发布保护 | ✅ | **R22 真机实证**：重发已存在的 repo:tag 被发布器拒绝（`package tag conflict ... refusing`），据此顺延版本号（rc.12→rc.13）；R23 复验 tag 冲突防护仍在生效；共享 Registry 运维策略=只增 tag |
| 6-06 | amd64/arm64 Manifest 与架构过滤 | ⚠️ | amd64/all 有 CI；arm64 真机需复验 |
| 6-07 | Registry sync 与目标仓库消费 | ✅ | R2/R3 真实同步并用于部署；每个发布候选仍需保存证据 |
| 6-08 | 完整 qualification 发布 | ⚠️ | Workflow 已实现；输出必须能完成真实部署和建群 |
| 6-09 | linux/amd64 主路径 | ✅ | 当前主要实测架构；覆盖平台部署、建群、升级和删除 |
| 6-10 | linux/arm64 主路径 | ⚠️ | 有构建和历史使用记录，缺当前 OCI 基线整轮证据 |
| 6-11 | Tier 1 OS 矩阵 | ⚠️ | 需先固定正式支持 OS 清单，再逐项跑部署、建群和删除 |
| 6-12 | Docker CRI 废弃入口清理 | ✅ | **R19（2026-09-24，§12.15）已执行**：CLI 侧 `--cri docker` 落入显式拒绝（`Docker CRI is not supported, use containerd`，create_cluster.go）；服务端 `AllowedCRIType` 仅 containerd、`CRIDocker` 常量与 enum 已删、createClusterCheck 前置拒绝；`pkg/scheme/core/v1/cri/docker.go` 整文件删除；`scripts/migrate-legacy-packages-to-oci.sh` 同步移除。Docker CRI 不安排 E2E（已无入口） |
| 6-13 | legacy static server 与 `nfs-provisioner` 不再暴露 | ⚠️ | **R28 现场核查（rc.25 三节点）**：平台侧全清——无 legacy 进程、systemd 仅 kc-{server,agent,etcd,console}（+dev-3 受保护的 kc-oci-r3-registry）、无 legacy 端口监听、`/usr/local/bin` 无 static/provisioner 二进制、默认 delivery policy 无旧组件（cri/cni/extension/bootstrap/k8s 槽位）、registry 包清单无旧包。**未通过项（记录）**：Console 产物仍含旧入口——`/etc/kc-console/dist/main.bundle.*.js` 的 storage addon 选项含 `nfs-provisioner`、`page.bundle.*.js` 仍定义 `{name:"nfs-provisioner", schema:{...}}` 安装表单；修复需改 console 源码分支（不在本仓库范围） |

## 7. 扩展能力

> 以下能力需要测试，但不计入平台部署、建群和生命周期管理的核心通过率。若某次发布明确承诺
> 其中一项，则应把该项提升为当次发布的阻断用例。

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 4-03 | addons：metallb BGP | ❌ | 需邻居 AS 环境 |
| 4-09 | 集群模板 templates | ❌ | CRUD、实例化和引用删除保护 |
| 4-10 | DNS domains / records | ❌ | 域名、记录 CRUD 与非法值拒绝 |
| 4-11 | cloudproviders / 外部集群纳管 | ❌ | kubeconfig 预检、同步、异常和移除 |
| 4-12a | Web 节点/集群终端 | ❌ | 鉴权、窗口 resize、断连和重连 |
| 4-12b | Pod exec | ❌ | namespace/pod/container 选择、鉴权和断连 |
| 4-15 | PlatformSetting（镜像仓库模板、Web 终端密钥） | ❌ | CRUD、权限、持久化和敏感字段保护 |
| 7-01 | 第三方 OAuth/OIDC 登录 | ❌ | 回调、用户映射、token 过期和登出 |
| 7-02 | `kcctl completion` | ✅ | R6：bash、zsh 生成 rc=0，分别通过 `bash -n`/`zsh -n`；当前 help 明确只支持 bash/zsh，fish 返回 Unsupported shell，不再作为产品能力要求 |
| 7-03 | 多节点并发任务基础容量 | ✅ | R28（rc.25，§12.25）定义并实测基线：3 节点平台（4c/8c/8c）+ **2 个 1M 离线建群并发**（dev-3 与 dev-4，同一共享 registry 取包）→ 两个 CreateCluster op 均 Succeeded、两集群 **Running（13:53:50→约 13:58:2x，~4.6min）**、期间 `kcctl status` 全程 3/3 Healthy（server/etcd/agent）、无 step 失败与锁争用。上限说明：≥3 并发受现有主机数量约束（每节点同时最多承载 1 个测试集群），更大规模需扩机器后按同法复测 |
| 2.1-13 | Kubernetes feature-gates 透传 | ✅ | R1 验过 20 项。**R24（rc.21，§12.21）发现并修复产品缺陷**：kubeadm v1.37 拒绝 ClusterConfiguration 的 featureGates map（实测其自身 kubelet 认识的 alpha/beta gate 亦被判 "not a valid feature name"），任何 `--feature-gates` 建群都在 kubeadm init 失败回滚。修复：渲染为 apiServer/controllerManager/scheduler `extraArgs` + KubeletConfiguration.featureGates。真机复验：r24-1m（v1.37.0，`APIServingWithRoutine=true`）**Running**，三组件 static pod `--feature-gates=` 与 kubelet config 均实测命中 |
| 2.1-15 | `--only-install-kubernetes-component` 跳过 CNI | ❌ | 自带网络场景；安装第三方 CNI 后恢复 Ready |
| 2.1-17 | kubeadm preflight ignore 定制 | ✅ | R24（rc.21，§12.21）：`--kubeadm-init-ignore-preflight-errors=Swap` → Cluster 注解 `kubeclipper.io/ignore-preflight-errors: Swap` 落库（2.1-27 同场建群实证） |
| 2.1-32 | IPv4/IPv6 dual-stack 集群网络 | ❌ | API/CNI 路径要求同时提供 IPv4、IPv6 Pod CIDR；现有 sh-dev 主机无可控 IPv6 环境，未安排真机 E2E |
| 4-04 | Addon 同组件多实例 | ❌ | R28（rc.25，§12.25）定性为**产品缺口**（与 2.2-04 同类）：API 按组件去重——同一集群第二次安装 nfs-csi 即使 scName 不同也 400 `nfs-csi-v1 component has been installed in the current cluster`，无实例名/多实例入口；属第 7 章扩展能力，发布若承诺需先实现 |
