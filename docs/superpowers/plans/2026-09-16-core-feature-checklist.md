# KubeClipper 平台核心功能清单与覆盖对账（2026-09-16）

用途：round 4+ 测试的覆盖基准。事实来源 = 代码命令树/API 路由（kcctl `cmd/kcctl/app/root.go`、
`pkg/apis/*/registry.go`）+ 两轮实测记录。

状态图例：✅ 已实测通过 · ⚠️ 部分验证 · ❌ 未验证 · ➖ 本轮范围外（非 OCI 迁移重点）

## A. 平台部署与生命周期（kcctl deploy/join/clean/upgrade/doctor）

| # | 功能 | 状态 | 证据/缺口 |
|---|---|---|---|
| A1 | deploy 3 节点（server+agent+etcd+console） | ✅ | R2/R3 各 2 次 |
| A2 | deploy 指定 http 私有 registry | ✅ | R3（:5003 全程） |
| A3 | deploy 默认 ghcr + 预检失败路径 | ✅ | R2/R3 + 单测 |
| A4 | deploy **多 server HA**（etcd 奇数集群） | ❌ | 两轮全部单 server —— **重点缺口** |
| A5 | console 部署（服务/静态资源自 OCI bootstrap） | ⚠️ | 服务起得来；页面未验 |
| A6 | kcctl doctor | ✅ | R3（19 项） |
| A7 | clean --all 全节点（含响亮失败） | ✅ | R3 |
| A8 | clean 单组件/部分节点 | ❌ | |
| A9 | join 纳管新节点到运行中平台 | ⚠️ | 集群 add nodes 隐含装过 agent；独立 join 未测 |
| A10 | kcctl upgrade（平台自身组件热升级） | ❌ | **缺口**（binary/online 两模式） |
| A11 | 重新 deploy 幂等（clean 后/未 clean） | ⚠️ | clean 后 ✅；直接重跑未测 |
| A12 | deploy 自定义端口/数据目录 | ➖ | |

## B. 计算集群生命周期（create/upgrade/delete/nodes）

| # | 功能 | 状态 | 证据/缺口 |
|---|---|---|---|
| B1 | 创建 1M+2W（v1.36.4 / v1.37.0） | ✅ | R3 |
| B2 | 创建 v1.35.8（containerd 1.7.29+calico v3.29.6 组合） | ✅ | R2 |
| B3 | 创建 3M HA（workerNodeVip/lvscare） | ✅ | R2 P2′-5 |
| B4 | 创建失败 → 删除 → 重建（InstallFailed 路径） | ✅ | R3（真实失败 3 次走通） |
| B5 | 版本矩阵外拒绝 | ✅ | R3（v9.9.9） |
| B6 | worker add / remove（含不可 drain） | ✅ | R3 / R2 |
| B7 | master add / remove、master↔worker 转换 | ❌ | **缺口** |
| B8 | 集群升级（真实滚动 1.36.4→1.37.0） | ✅ | R2 + R3 补跑（修 3 个 bug） |
| B9 | 同版本/降级拒绝 | ✅ | R3 |
| B10 | 集群删除（并发 SyncKubeConfig 死锁回归） | ✅ | R2/R3 |
| B11 | 删除失败重试（TerminateFailed→retry） | ✅ | R2 |
| B12 | node disable/enable | ❌ | API 存在未测 |
| B13 | kubeconfig 获取（/kubeconfig、CLI 下载） | ⚠️ | admin.conf 一直在用；API 路由未显式测 |
| B14 | 证书更新（/certification） | ❌ | **缺口** |
| B15 | Web 终端（/terminal、terminal.key）、pod exec | ❌ | ➖ UI 侧 |
| B16 | proxyMode ipvs ✅ / iptables ❌ | ⚠️ | |
| B17 | CRI containerd 1.7.29 ✅ / 2.2.4 ✅ / docker ❌ | ⚠️ | docker 无发布物料，可能已事实下线 |
| B18 | CNI calico 双版本 ✅ / 其他 CNI ➖ | | |

## C. OCI 制品分发（本轮主题）

| # | 功能 | 状态 | 证据/缺口 |
|---|---|---|---|
| C1 | ghcr→本地 registry sync（含幂等重跑） | ✅ | R2/R3 + CI roundtrip |
| C2 | bootstrap 包驱动平台安装 | ✅ | R3 |
| C3 | 集群安装物料按 plan 解析（digest 固定） | ✅ | R2/R3（AddNodes/删除后 plan 不变） |
| C4 | 缺包/坏 registry 的报错清晰度 | ✅ | R2 P5′ |
| C5 | 离线 bundle export/import 真机演练 | ❌ | CI ✅，lab 未复跑 |
| C6 | delivery-policy **自定义**（版本白名单收紧→创建被拒） | ❌ | **缺口**（只测过默认策略） |
| C7 | kcctl resource refresh / componentmeta | ⚠️ | 索引器行为 R2 验过；CLI 命令未跑 |
| C8 | https+认证私有仓库 | ❌ | 全程 http |
| C9 | addon 物料（chart+runtime image set）自 registry | ✅ | R3（nfs/metallb） |

## D. 备份与恢复

| # | 功能 | 状态 | 证据/缺口 |
|---|---|---|---|
| D1 | backuppoint（fs；非法类型拒绝） | ✅ | R3（含校验修复回归） |
| D2 | 手动备份 + 恢复（marker 回滚证明） | ✅ | R2/R3 |
| D3 | 备份删除（NFS 文件清掉） | ✅ | R3 |
| D4 | 删除集群前的备份保护 | ✅ | R3 |
| D5 | cronbackup runAt 单次触发 | ✅ | R3 |
| D6 | cronbackup **周期调度真实命中**（等到 schedule 时刻） | ⚠️ | 只看 NextScheduleTime 滚动，未等真实周期 |
| D7 | cronbackup disable/enable | ❌ | |
| D8 | maxBackupNum 超限自动轮转旧备份 | ❌ | **缺口** |
| D9 | S3 backuppoint（minio） | ❌ | **缺口** |
| D10 | 恢复期间的 operation 可观测（logs/cancel） | ⚠️ | logs ✅；cancel ❌ |

## E. Addons / 组件

| # | 功能 | 状态 | 证据/缺口 |
|---|---|---|---|
| E1 | nfs-csi 安装/动态供给读写/卸载 | ✅ | R2/R3（读写闭环 R3） |
| E2 | metallb L2 安装/卸载 | ✅ | R3 |
| E3 | metallb BGP | ❌ | 需邻居 AS 环境 |
| E4 | uninstall 不校验连接参数（4705fbf0）+ 空 config（46b42953） | ✅ | R2 修/R3 复验+新用例 |
| E5 | 多组件同时卸载的 steps 链（ErrIgnore 输入 6b760692） | ✅ | R3 |
| E6 | 同 addon 多实例（不同 scName，checkComponents 唯一性） | ❌ | |

## F. 用户/RBAC/OAuth

| # | 功能 | 状态 | 证据/缺口 |
|---|---|---|---|
| F1 | 用户/角色 CRUD、密码修改、enable/disable | ❌ | 全程 admin 证书 —— ➖ 非本轮主题但属核心功能 |
| F2 | RBAC 实际鉴权拦截 | ❌ | ➖ |
| F3 | OAuth 登录（/oauth、tokens） | ❌ | ➖ |

## G. 其他资源面

| # | 功能 | 状态 | 证据/缺口 |
|---|---|---|---|
| G1 | 集群模板 templates CRUD | ❌ | |
| G2 | DNS domains/records 管理 | ❌ | ➖（与 K8s 集群解耦的平台功能） |
| G3 | cloudproviders（云厂商纳管） | ❌ | ➖ |
| G4 | regions | ⚠️ | 单一 default region 隐式在用 |
| G5 | events/审计查询 | ❌ | ➖ |
| G6 | configmaps 直读（策略/配置） | ⚠️ | delivery-policy 注入验证过 |
| G7 | 注册镜像仓库 /registries CRUD + 集群绑定 | ✅ | R3（含缺省沿用修复 c2773127） |

## H. 可观测/操作面

| # | 功能 | 状态 | 证据/缺口 |
|---|---|---|---|
| H1 | operation list/describe | ✅ | R2/R3 |
| H2 | operation logs（server→agent mTLS 代理） | ✅ | R3 全程排障在用 |
| H3 | operation retry | ✅ | R3 |
| H4 | operation cancel | ❌ | 仅单测 |
| H5 | --watch/彩色输出/分页 cursor | ⚠️ | 分页 ✅ R2；watch 未长跑 |
| H6 | 失败展示（message/result.reason）→ console | ⚠️ | API 侧 ✅；UI ❌ |

---

## 汇总：下一轮优先补的空白
- **A4 多 server HA deploy**、**A10 平台自升级**、**B7 master 增删/转换**
- **C6 delivery-policy 自定义生效**、**C5 离线 bundle 真机**
- **D8 maxBackupNum 轮转**、**D9 S3 备份**、D6 真实周期触发
- H4 cancel、B14 证书更新、B12 disable/enable 节点
- F/G 大块（RBAC、模板、DNS、云厂商）属平台既有能力但与 OCI 迁移弱相关，建议单独立项
