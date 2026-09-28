# R34 遗留 Case 推进计划（2026-09-28）

## 边界与验收口径

- 代码、测试和记录只改 `lixd/kubeclipper` fork 的 `feat/oci-operation-v2-migration`；不修改源仓库。
- 继续使用 sh-dev-2/3/4；部署前确认现场，部署后按用户既有授权执行 `kcctl clean -A -f` 并核对服务与数据目录。
- 临时系统依赖、网络规则、代理隧道和测试对象逐项登记、仅删除本轮创建的内容；保留现有 SSH 授权和用户代理配置。
- 用户于 2026-09-28 明确授权清理 R32 留下的 v1.37.0 Kubernetes 集群，包括三台上的 `kubeadm reset` 和该集群专属 CNI/IPVS 网络状态。
- 负向 API/单测、真实外部依赖 E2E、产品能力缺失分别记录；部分覆盖不提升为完整通过。

## Case 分解

| Case | R33 状态 | R34 推进 | 完整验收门槛 |
|---|---|---|---|
| `1.1-01` | ⚠️ | 复用本机 7890 代理复核 GHCR package token；只记录 HTTP 状态，不读取或输出凭据 | 能读取目标 GHCR 制品的凭据/权限，并以默认 Registry 完成真实 deploy |
| `2.6-11` | ⚠️ | 明确当前 standalone extension 入口边界；评估 fork 实现和测试点 | 有独立入口，并证明 standalone 与集群 `packagePlan` 隔离 |
| `4-07` | ⚠️ | R34 重试 CUA 浏览器清单和隔离 tab，浏览器 provider 仍不可达；后续需恢复浏览器控制，再准备 BackupPoint、较 `v1.37.0` 更新的离线包和受控失败操作 | UI 删除、升级、备份和真实 Failed Operation 展示均有 UI 与 API 对照证据 |
| `4-09` | ⚠️ | 保留已通过 CRUD 证据；定义 `templateRef` 与引用删除保护的语义 | 模板实例化、引用关系和删除保护完整验证 |
| `6-06` | ⚠️ | 可先复核 OCI manifest/架构过滤逻辑，明确静态证据范围 | arm64 真机 manifest 选择和制品消费验证 |
| `6-10` | ⚠️ | 等待 arm64 主机；现有 amd64 结果不折算为 arm64 | arm64 上完整部署、建群、升级和删除 |
| `6-11` | ⚠️ | 对照 README 与开发指南列出 OS 文档冲突，不自行推定正式支持矩阵 | 确认 Tier 1 OS 清单后，每种 OS 完成部署、建群和删除 |
| `2.2-04` | ❌ | 先定义 Master/Worker 转换约束、并发及失败回滚语义 | fork 提供独立转换操作/API，完成正反向及失败恢复验证 |
| `4-03` | ❌ | fork Addon 安装生成的两个 BGPPeer CRD 版本均为 int64，最大 ASN server-side dry-run 通过；FRR 学到 VIP `/32` 且 Service 删除后路由撤回。外部 NodePort 可达，但 LoadBalancer VIP SYN-ACK 未到达 FRR 主机，疑似需 OpenStack allowed address pair；保留 ⚠️ | 安装生成的 CRD 无需手工修改；邻居、路由发布/撤回和 LoadBalancer VIP 完整外部往返均有实测证据并完成清理 |
| `4-04` | ❌ | 先定义同组件实例标识、配置隔离和定向卸载语义 | fork 支持两个同组件实例并分别升级/卸载 |
| `4-11` | ❌ | 先补无 provider 时可执行的 kubeconfig/schema 负向预检；不把它算作纳管通过 | 可用 CloudProvider 与外部 kubeconfig 完成预检、同步、异常和移除 |
| `7-01` | ❌ | R34 使用本地 Dex v2.42.0 完成授权码回调、自动用户映射、15 秒 token 过期与登出撤销；fork 已补 kcctl provider 工厂注册及 server auth 配置模板传递 | 真实回调、用户映射、token 过期和登出均验证 |
| `2.1-32` | ❌ | 先验证能否建立跨节点 ULA IPv6 测试路径；配置变更仅限可回滚的测试网络 | 双栈 Pod/Service 地址分配、跨节点通信、Service 访问及清理均实测 |

## 执行顺序

1. 推进 `4-11` 的无 provider 负向预检，用 handler 测试断言不支持类型/非法 kubeconfig 返回 400；它只补部分证据，不模拟完整纳管成功。已完成。
2. 按用户授权清理 R32 遗留的三机集群；清理核验通过后推进 `4-03`。已清理集群并完成 schema、邻居、路由发布/撤回及 NodePort 验证；完整 VIP 往返仍受测试网络前置限制。
3. 检查 `7-01` IdP 镜像/网络可达性及 `2.1-32` IPv6 跨节点条件；只在前置可满足时开展完整 E2E。
4. 汇总 `2.2-04`、`2.6-11`、`4-04`、`4-09` 的最小产品语义与实现边界，再决定是否在 fork 补功能。
5. `6-06`/`6-10` 等待 arm64 资源；`6-11` 等待正式 OS 清单；`1.1-01` 等待 GHCR read 权限；`4-07` 等待可用 UI 控制并补齐真实操作前置。

## 当前执行记录

- 2026-09-28：确认工作区是 fork 分支且干净；sh-dev-2/3/4 均为 Ubuntu 24.04 `x86_64`；sh-dev-3 有 Docker 但无 FRR 镜像/服务；当前无全局 IPv6 路由，sh-dev-4 仅见 LXD ULA。
- 2026-09-28：本机 `127.0.0.1:7890` 可连 GHCR；`/v2/` 返回未认证 Registry challenge（401），目标 package token endpoint 返回 403；不代表已获得制品读取权限。
- 2026-09-28：推进 `4-03` 前的现场核对发现三台仍组成 Kubernetes `v1.37.0` 集群，节点 `lixd-dev-2/3/4` 均 Ready，Namespace 只有 Kubernetes/Calico 系统项。sh-dev-3 control-plane 和 etcd、三台 kubelet/Calico 都在运行，集群创建时间为 `2026-09-27T14:30Z`。暂停 FRR 安装及网络变更。
- 代码检查确认 `kcctl clean -A -f` 只卸载 KubeClipper 平台组件，不执行 `kubeadm reset`，因此 R32 所称“清除临时 Cluster”并未被该命令证明。清除现存 Kubernetes 集群需要单独确认；在此之前不对三台做会影响它的部署、网络改动或 reset。
- sh-dev-3 的 FRR apt 包当前未安装，缓存候选为 `8.4.4-1.1ubuntu6.7`；仅执行 apt 模拟安装，没有安装软件或改网络。
- 2026-09-28：按计划在 sh-dev-3 启动 FRR 8.4.4，MetalLB v0.13.7 使用 control-plane speaker `.208` / AS64512，对端 FRR `.146` / AS64513。旧 rc.30 生成的 BGPPeer CRD 在 Kubernetes 1.37 因 `int32` 与 `uint32` 最大值不兼容而拒绝；临时 CRD schema 改为 fork 修复所用的 `int64` 后建立 Established 邻居。FRR 学到 VIP `172.16.131.250/32`，当时从 FRR 所在节点 curl 返回 `R34_BGP_OK`；该请求受本机 IPVS/路由影响，不能作为外部 VIP 往返证据。删除 Service 后 FRR 显示路由不在表中，curl 超时。后续以外部 FRR `.230` 抓包验证为准。
- 2026-09-28：`7-01` 使用 Dex v2.42.0 真实完成 OAuth authorization code 回调；admin API 查到 `r34_oidc_user` 与 email 自动映射；配置 `accessTokenMaxAge=15s`，超过 TTL 后访问受保护 API 返回 401；带 `application/json` 和 `{}` 调用 logout 返回 200，随后同一 token 返回 401。无请求体的 logout 首次得到 406，按 route 的 JSON 消费契约重试后通过。
- 2026-09-28：OIDC 部署测试暴露 kcctl 未加载 OIDC provider 工厂、server 配置模板遗漏 `oauthOptions` 两个缺口。fork 增加 CLI side-effect import、将完整 `oauth.Options` 写入 server YAML，并添加注册校验和 YAML 解析/secret/TTL 回归测试。审计也暴露 `ConfigMap.data.DeployConfig` 内嵌 YAML 不会被原字段脱敏；fork 现在对该数据项整体脱敏并添加回归测试。
- 2026-09-28：临时给三台 `ens3` 配置 `fd42:131:131::/64` ULA 后，跨节点 ping 均为 `Destination unreachable`，随后移除全部测试地址；`2.1-32` 仍缺 IPv6 underlay 和双栈集群实测。
- 2026-09-28：复跑 `6-06` OCI multi-arch 与过滤相关包测试均通过（releasemanifest/apis/indexer/fetcher/publisher/registry client）；仍只有 amd64 主机，保留 arm64 真机 warning。`6-11` 文档对照发现 README 写 CentOS 7.x / Ubuntu 18.04 / 20.04，开发指南样例为 Ubuntu 22.04 / 24.04；未找到权威 Tier 1 OS 清单。
- 2026-09-28：R34 重试 CUA 浏览器 inventory / tab 创建仍失败（browser inventory `nodeRepl.fetch request failed`，tab 操作超时），因此 4-07 没有新增 UI 证据；既有备份/升级/Failed Operation/UI 删除缺口保留。
- 2026-09-28：测试部署的 kc-server Audit Request 事件曾包含 ConfigMap 的完整 DeployConfig 文本，其中有 SSH 私钥及初始口令。没有轮换或撤销 SSH 授权；fork 已补针对 `DeployConfig` 字段的审计脱敏回归修复，但当前运行中的 rc.30 server 仍是旧制品，历史日志未在本轮清除，详见 R34 报告。
- 在 `pkg/apis/core/v1/handler_test.go` 添加 `4-11` 负向预检覆盖：非法 base64 kubeconfig 与不支持 provider 均在 API 层返回 400。`go test ./pkg/apis/core/v1 -run 'TestPreCheckCloudProvider' -count=1` 和整个 `go test ./pkg/apis/core/v1 -count=1` 均通过；`git diff --check` 通过。该证据保持 `4-11` 为未完成，不模拟外部集群同步成功。
- 2026-09-28：用户授权后在 sh-dev-2/3/4 执行 `kubeadm reset -f`；停止并禁用 kubelet，移除该集群的 Kubernetes manifests/etcd 数据、CNI 配置与结果、Calico 接口/路由和 KUBE/CALI 防火墙链。sh-dev-3 有 6 个停止的 CRI sandbox；确认没有其他 containerd namespace/task 后，按 sandbox ID 定向移除，重启 containerd 后 `crictl pods/ps -a`、`ctr sandboxes/containers/tasks` 均为空。最终三台 API 6443 未监听、KUBE/CALI 规则和 Calico/Pod CIDR 路由为 0、无 CNI 文件/结果、IPVS 服务为 0；KubeClipper 服务与 kubelet 均 inactive，kubelet disabled。sh-dev-3 的共享 Registry 5003 `/v2/` 仍返回 200；未清理 Docker bridge 或 Registry 数据。
- 2026-09-28：旧集群清理后复核，sh-dev-3 主地址为 `172.16.131.146/24`，sh-dev-2/4 为 `172.16.131.208/24`、`172.16.131.230/24`；Registry 5003 只读 catalog 有 MetalLB v0.13.7 runtime images，但共享仓库没有 `v2.0.3-rc.30` bootstrap 包，后续 `4-03` 先确认可复建当前 fork 版本部署包和 BGP 互联前置。
- 2026-09-28：继续推进 `4-03`，在 fork 部署平台上重建 1 master + 1 worker 的 v1.37.0 集群。初次 Addon 安装因固定 10 秒等待后 webhook 尚未 ready 而失败；fork Agent 健康检查改为等待 `deployment/controller` Available。失败的安装操作 `e7d08609-1a27-472f-a6a2-91406a72123c` 重试后成功，但只重新执行失败的 IPAddressPool 步骤，先前已成功的 `checkMetalLBHealth` 没有重跑；故本轮 live retry 没有验证新健康检查等待逻辑。新增单测检查 `kubectl wait --for=condition=Available deployment/controller -n metallb-system --timeout=5s` 参数和错误传播；linux/amd64 Agent 构建通过，三台更新 Agent 的 SHA256 一致。安装生成的 `v1beta1`/`v1beta2` ASN 字段均为 `int64`，最大值 `4294967295` 的 server-side dry-run 通过，没有手工改 CRD。
- 2026-09-28：外部 FRR `.230` / AS64513 与 MetalLB speaker `.146` / AS64512 Established；FRR 学到 `172.16.131.250/32`，Linux 下一跳为 `.146`。外部 NodePort 返回 `R34_BGP_OK`；访问 LoadBalancer VIP 超时。抓包确认 SYN 到达 speaker、speaker 发出源地址为 VIP 的 SYN-ACK，但 FRR 主机没有收到；节点 MAC 为 OpenStack 风格，可能需要网络侧 allowed address pair，未修改云端策略。删除 Service 后 FRR 撤回 `/32`、前缀归零。故 `4-03` 保持 ⚠️：安装/schema 和 BGP 路由路径通过，LoadBalancer VIP 外部往返未通过。
- 2026-09-28：按用户授权删除测试集群并运行三机 `kubeadm reset -f`、`kcctl clean -A -f`；清除本轮 Calico 网络命名空间/接口/路由及 KUBE/CALI iptables 链。停止 containerd 后确认 CRI pod/container/task 列表为空。临时 Registry 5004 及其数据、FRR 和 task-only 依赖、Agent 备份/暂存与临时配置均已删除；共享 Registry 5003 `/v2/` 仍为 200；SSH 授权及本机代理配置保留。
- 2026-09-28 收尾复查：三台 KubeClipper 服务、kubelet、containerd、FRR、Dex 与临时 Registry unit 均 inactive；6443/5004/5556 无监听，KUBE/CALI 规则、`172.25.*`/proto 80 路由、CNI 文件和网络命名空间均无残留。sh-dev-3 的共享 Registry 5003 `/v2/` 返回 200；三机 `/tmp` 不再含 `kc-r34-*`。历史 Audit Request 记录未清除，详见状态报告。
- 清单计数核对：核心表格共有 194 个 Case（含 4-08b/c/d、4-12a/b），R34 状态为 182 ✅、8 ⚠️、4 ❌；本次 `4-03` 由无真实部署制品证据推进为部分通过，但 VIP 返回路径未闭环，计数不变。
- fork 代码、测试和报告均未提交或推送；测试节点上的 R34 平台已清理，源仓库未修改。完整状态见 [`status-2026-09-28-r34.md`](status-2026-09-28-r34.md)。

## R35 执行记录（2026-09-28，接手 agent）

- 复核并提交 R34 遗留的 13 文件工作区改动（fork 构建通过、metallb/auditing/options/deploy/apis 测试通过、凭据扫描干净），commit `fa372984`；同步推送 origin（补齐 13 个落后提交）与 fork。
- 计划第 4 步完成：四个产品缺口（2.2-04/2.6-11/4-04/4-09）的最小语义与实现边界汇总见 [`product-gap-semantics.md`](product-gap-semantics.md)——4-09 建议优先做（小-中工作量，快照式 templateRef+删除保护），2.6-11 次优，2.2-04/4-04 建议暂缓；是否实施待用户决策。
- 源仓库 `/Users/lixueduan/17x/kc-test/kubeclipper` 的 `pkg/scheme/types.go`、`types_test.go` 未提交改动确认存在并保持原样（未触碰）。
- 待用户决策事项：①是否授权真实 stable 发布轮（收口 6-04/6-08/1.1-01）；②是否实施上表建议的功能项；③历史审计事件中的旧 DeployConfig 秘密（SSH 私钥+初始口令）保留在旧 etcd 数据中（R34 按用户指示未清理、SSH 授权未轮换）——如需消除需清理该数据目录并轮换密钥。
