# KubeClipper 核心功能测试清单（长期覆盖基准）

本文档是平台核心功能的**长期测试基准**：每轮验证（e2e/回归/发布前）都应以此清单为
起点圈定范围，测完回填状态。它不属于某一次测试计划，而是所有测试轮次共同的服务对象。

## 使用说明

- **编号即身份**：`x.y-NN` 一经分配永不变更、永不复用；功能下线时把状态标为 `🗑 废弃`
  并保留编号（历史报告仍可引用）。新增 case 在对应小节末尾追加编号。
- **状态含义**：✅ 已实测通过 · ⚠️ 部分验证 · ❌ 未验证 · 🗑 废弃。
  状态必须来自**真实运行**，单测覆盖不算 ✅（可在备注注明 "unit-only"）。
- **轮次记号**：备注中 R1/R2/R3 指验证发生的轮次；每轮详细证据记录在
  `docs/superpowers/issues/` 的轮次报告里，本文档只留结论与指针。
- **每轮测试的闭环动作**：① 圈定本轮要覆盖的编号 → ② 执行 → ③ 回填状态与轮次 →
  ④ 未覆盖项转入缺口文档 → ⑤ 新发现的功能面补编号。
- 缺口/待办轮次的文档见同目录（如 `round-2026-09-gaps.md`）。

## 1. 平台本身的部署

### 1.1 部署物料来源

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 1.1-01 | 在线部署：默认 ghcr 直装 | ⚠️ | 预检✅；从未真从 ghcr 直装（都先 sync） |
| 1.1-02 | 半离线：ghcr → `kcctl registry sync` → 本地仓库 → deploy | ✅ | R3 全链路（5 copied/56 skipped 幂等） |
| 1.1-03 | 纯离线：bundle export → 拷贝 → import 进仓库 → deploy | ❌ | 脚本 CI 绿，真机未演练 |
| 1.1-04 | 私有仓库 http | ✅ | R3 全程 :5003 |
| 1.1-05 | 私有仓库 https + 自签 CA | ❌ | |
| 1.1-06 | 仓库账号密码认证（deploy 侧） | ❌ | sync 源认证✅，部署侧未测 |

### 1.2 部署拓扑

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 1.2-01 | 单 server + 多 agent | ✅ | R2/R3 |
| 1.2-02 | server 与 agent/k8s 节点混部同机 | ✅ | R2/R3 |
| 1.2-03 | **多 server HA（3× server，etcd 奇数集群）** | ❌ | 头号缺口 |
| 1.2-04 | console 组件部署与访问 | ⚠️ | 服务起✅，页面未验 |

### 1.3 容错与增量运维

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 1.3-01 | 预检失败引导（不可达/缺包报错） | ✅ | R2/R3 |
| 1.3-02 | etcd 冷启动竞态（写探针+重试） | ✅ | R3 |
| 1.3-03 | clean --all 后重 deploy 幂等 | ✅ | R3 ×2 |
| 1.3-04 | 不 clean 直接重复 deploy | ❌ | |
| 1.3-05 | `kcctl join` 独立纳管新节点 | ⚠️ | add nodes 隐含走过，独立命令未测 |
| 1.3-06 | clean 单节点/部分清理 | ❌ | |
| 1.3-07 | **`kcctl upgrade` 平台自身升级**（--pkg / --online） | ❌ | |
| 1.3-08 | `kcctl doctor` | ✅ | R3（19 项） |

## 2. 集群相关操作

### 2.1 创建集群

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.1-01 | 1 master + 2 worker | ✅ | R3 |
| 2.1-02 | 单节点（master 兼 worker）/ 1M1W 最小规格 | ❌ | |
| 2.1-03 | 3 master HA（lvscare workerNodeVip） | ✅ | R2 |
| 2.1-04 | 版本矩阵 v1.35.8 / v1.36.4 / v1.37.0 | ✅ | R2/R3 |
| 2.1-05 | CRI containerd 1.7.29 / 2.2.4 | ✅ | R2/R3 |
| 2.1-06 | CNI calico v3.29.6 / v3.31.5 | ✅ | R2/R3 |
| 2.1-07 | 镜像/物料 100% 来自指定仓库（离线保证） | ✅ | R3 验证法可复用 |
| 2.1-08 | proxyMode ipvs | ✅ | R2/R3 |
| 2.1-09 | proxyMode iptables | ❌ | |
| 2.1-10 | 矩阵外版本 / 同版本 / 降级拒绝 | ✅ | R3 |
| 2.1-11 | 网络自定义（pod/service 网段、DNS 域） | ✅ | R3 即用即验（172.25/16 + cluster.local） |
| 2.1-12 | apiserver 对外发布（cert-sans / external-domain / external-ip / external-port） | ❌ | |
| 2.1-13 | feature-gates | ⚠️ | R1 验过 20 项（前 agent）；R3 未复跑 |
| 2.1-14 | untaint-master（master 允许调度） | ❌ | |
| 2.1-15 | `--only-install-kubernetes-component`：建集群跳过 CNI（自带网络场景，建后手动装 CNI 恢复 Ready） | ❌ | CLI flag → annotation → step 跳过链路存在 |
| 2.1-16 | 自带 CA（ca-cert / ca-key 复用已有根证书） | ❌ | |
| 2.1-17 | kubeadm preflight ignore 定制（annotation） | ⚠️ | R2 场景隐含，无显式用例 |

### 2.2 节点操作

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.2-01 | worker 添加（含 packagePlan 不变性） | ✅ | R2/R3 |
| 2.2-02 | worker 移除（含不可 drain 容错） | ✅ | R2/R3 |
| 2.2-03 | **master 添加 / 移除** | ❌ | |
| 2.2-04 | master↔worker 角色转换（convertNodes） | ❌ | |
| 2.2-05 | 节点 disable / enable | ❌ | |
| 2.2-06 | 节点失联后操作收敛（agent down） | ⚠️ | R2 自然样本，无系统注入 |
| 2.2-07 | agent 节点注销（/nodes 记录删除与残留清理） | ❌ | 纯 REST（DELETE /nodes/{name}），kcctl delete 无 node 入口；API 无集群占用校验，可直接删在集群中的节点（危险操作） |

### 2.3 升级与证书

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.3-01 | 真实滚动升级 1.36.4→1.37.0（master→worker drain） | ✅ | R2 + R3 新 tip 复验（修 3 bug） |
| 2.3-02 | 升级中途失败 → op retry | ❌ | |
| 2.3-03 | 升级失败 → 集群状态恢复（reset status） | ✅ | R3 实际使用 |
| 2.3-04 | **集群证书更新（/certification）** | ❌ | |
| 2.3-05 | agent 证书重新签发 | ⚠️ | 依赖 join 测试 |

### 2.4 删除集群

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.4-01 | 正常删除（含与 SyncKubeConfig 并发死锁回归） | ✅ | R2/R3 |
| 2.4-02 | InstallFailed 状态删除 | ✅ | R3 |
| 2.4-03 | 删除失败 TerminateFailed → 重试 | ✅ | R2 |
| 2.4-04 | 有备份时删除保护（引导先删备份） | ✅ | R3 |
| 2.4-05 | 删除后 ops 账本清理 | ✅ | R3 |

### 2.5 备份与恢复

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 2.5-01 | backuppoint fs 型（含非法类型拒绝） | ✅ | R3 |
| 2.5-02 | backuppoint **S3 型（minio）** | ❌ | |
| 2.5-03 | 手动备份 → 恢复（marker 回滚证明） | ✅ | R2/R3 |
| 2.5-04 | 备份删除（连带存储文件清除） | ✅ | R3 |
| 2.5-05 | cronbackup runAt 单次触发 | ✅ | R3 |
| 2.5-06 | cronbackup 真实周期命中 | ⚠️ | 只验了调度时间滚动 |
| 2.5-07 | cronbackup enable / disable 子资源 | ❌ | |
| 2.5-08 | **maxBackupNum 超限自动轮转** | ❌ | |
| 2.5-09 | 恢复后集群可用性（addons/节点完整） | ✅ | R3 |

## 3. kcctl 命令覆盖（每条至少跑通一次核心路径）

| 编号 | 命令 | 子命令/用法 | 状态 |
|---|---|---|---|
| 3-01 | deploy / clean / doctor | — | ✅ |
| 3-02 | join | 独立纳管 | ❌（→ 1.3-05） |
| 3-03 | create | cluster（CLI 入口） | ⚠️（API 建过，CLI 未跑） |
| 3-04 | create / delete | registry | ✅ |
| 3-05 | delete | cluster（CLI）/ user / role | ⚠️ / ❌ |
| 3-06 | get | clusters / nodes / operations | ✅ |
| 3-07 | get | --watch 长跑 / 分页 | ⚠️ / ✅ |
| 3-08 | operation | list / describe / logs / retry | ✅ |
| 3-09 | operation | **cancel** | ❌（仅单测） |
| 3-10 | cluster | upgrade | ✅ R3 |
| 3-11 | set / drain | cluster / 独立驱逐 | ❌ / ❌ |
| 3-12 | upgrade | **平台自升级** | ❌（→ 1.3-07） |
| 3-13 | registry | sync | ✅ R2/R3 |
| 3-14 | registry | list / deploy / clean / push | ❌ |
| 3-15 | resource | list / inspect / refresh | ❌ |
| 3-16 | delivery-policy | template / get / apply / diff / validate | ❌（整组；默认策略注入✅） |
| 3-17 | login / status / config / version | — | ❌ / ❌ / ⚠️ / ✅ |

## 4. 其他常用功能

| 编号 | 功能 | 状态 | 备注 |
|---|---|---|---|
| 4-01 | addons：nfs-csi 安装/动态供给读写/卸载 | ✅ | R2/R3 |
| 4-02 | addons：metallb L2 安装/卸载 | ✅ | R3 |
| 4-03 | addons：metallb BGP | ❌ | 需邻居 AS 环境 |
| 4-04 | addons：同组件多实例（scName 唯一性） | ❌ | |
| 4-05 | addons：uninstall 容错（空 config / ErrIgnore 链） | ✅ | R3 三修复合验 |
| 4-06 | 可观测：operation logs、失败原因展示（API 侧） | ✅ | R3 |
| 4-07 | **console UI 端到端（含任务失败展示）** | ❌ | fork console 分支未配镜验证 |
| 4-08 | 用户 / 角色 CRUD、enable/disable、改密、登录记录 | ❌ | 建议与 UI 单独立项 |
| 4-08b | RBAC 鉴权拦截（非管理员越权应 403） | ❌ | 同上 |
| 4-08c | 登录方式：密码 / 验证码 / 第三方 OAuth 回调 | ❌ | 同上 |
| 4-08d | 长期 token / kubeconfig 签发（/tokens） | ❌ | 同上 |
| 4-09 | 集群模板 templates | ❌ | 同上 |
| 4-10 | DNS domains / records | ❌ | 同上 |
| 4-11 | cloudproviders 云厂商纳管 | ❌ | 同上 |
| 4-12 | Web 终端 / pod exec / kubeconfig 下载 | ❌ / ❌ / ⚠️ | UI 侧 |
| 4-13 | 平台自省：/configz、/status、/components、/componentmeta | ⚠️ | doctor/config 间接用过，无直查用例 |
| 4-14 | 审计事件查询（/events，auditing 组） | ❌ | |
| 4-15 | PlatformSetting（镜像仓库模板、web 终端密钥） | ❌ | |

## 当前缺口速览

未覆盖项的汇总与优先级排序属于轮次文档，见同目录 `round-2026-09-gaps.md`。
