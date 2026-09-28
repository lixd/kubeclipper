# 产品缺口最小语义与实现边界（2.2-04 / 2.6-11 / 4-04 / 4-09）

> 依据 R34 计划第 4 步产出，供决策"是否在 fork 补功能"。基于 2026-09-28 代码现状
> （HEAD `fa372984`）。每项给出：现状代码事实 → 最小可用语义 → 实现边界（触点/工作量/风险）→ 建议。

---

## 1. `2.2-04` master↔worker 转换（convertNodes）独立入口

**代码事实**：`pkg/apis/core/v1/handler.go:253` 把待操作节点塞进 `pn.ConvertNodes`，
`pkg/clusteroperation/node.go:78-80` 在 add/remove 流内把转换节点追加进
`extra.Workers/Masters`——即转换是节点增删流的**内嵌子机制**，API 仅暴露
`NodesOperationAdd/Remove`（R21 定性）。无独立转换入口。

**约束（kubeadm 决定的硬边界）**：kubeadm 不支持"原地改角色"。worker→master 实际需要
etcd 加成员 + 控制面二进制/证书 + control-plane taint，等价于"以新角色重新加入"；
master→worker 等价于"移除后以 worker 加入"。因此"转换"只能是**编排出来的组合操作**
（drain → reset → 以新角色 join），不存在真正的原地转换。

**最小可用语义**：
- 在现有 NodesOperation 上新增第三种动作 `Convert`（payload 带 `targetRole`），而不是新端点——
  复用 handler 现成的节点解析/校验（`getNodeInfo`、region、in-use、disabled 检查都在）。
- 校验复用现有 quorum 守卫：master→worker 仅当剩余 master ≥2（`makeMasterCompare` 同源）；
  worker→master 不改变 quorum，但需检查该节点不在其它集群。
- 步骤编排 = 既有 remove 步骤序列 + 既有 add 步骤序列（含证书/etcd/VIP 收敛），转换失败
  回滚语义 = 失败即停、保留原角色（节点此时已被 reset，需按 add 流程重新加入原角色）。
- ExecutionLock 已覆盖并发；`packagePlan` 复用集群既有 plan（同 add 路径的
  `ResolvedArtifactPlan` 复用逻辑）。

**实现边界**：`clusteroperation/node.go` 新增 convert 步骤组装（复用 add/remove 构建器）、
API 枚举+handler 分支、CLI `kcctl cluster convert`（或扩展现有 nodes op 参数）、
quorum/region/in-use 单测、真机 E2E（转换后 etcd 成员/证书/标签/taint 断言）。
**工作量：大（≈2-3 天 + 真机）；风险：高**（etcd 成员变更与半失败状态恢复）。

**建议**：除非有明确用户场景，暂缓。它本质是 add/remove 的编排糖，手工"remove+add"已可达成
同一结果且两条路径均已真机验证（R18/R21）。

---

## 2. `2.6-11` k8s-extension 与集群 packagePlan 隔离（独立入口）

**代码事实**：k8s-extension 是节点操作的内嵌步骤——`pkg/clusteroperation/node.go` 四处
（join/upgrade/install 路径）按集群 packagePlan 内嵌安装；实现本体
`pkg/scheme/core/v1/k8s/extension.go` 是注册在 agent 的 `StepRunnable`（按 k8s 版本取
k8s-extension 包）。无独立 CLI/API 入口；extension 版本隐式绑定 k8s 版本。

**最小可用语义**：
- 定义边界：k8s-extension 属"节点生命周期包"，版本跟随 k8s 版本（不独立选版）。
- 独立入口：新增 `kcctl extension install|upgrade --cluster <name>`（或 API
  `POST /clusters/{name}/extension`），只组装修复/升级 extension 的步骤（复用
  `extension.go` 的 StepRunnable + InstallComponents 骨架），不再要求整节点操作。
- 隔离证明：对集群 A 执行 extension upgrade 后，集群 B 的 `status.packagePlan` 哈希
  不变（对齐 2.3-06 的 steps 哈希取证法）；集群 A 自身 plan 中 extension 槽位版本前进。

**实现边界**：CLI 命令 + 复用现有 op 组装（InstallComponents 骨架去掉其它 addon）、
handler 校验集群存在/plan 含 extension 槽位、单测（步骤只含 extension）+ 真机隔离取证。
**工作量：中（≈1 天 + 真机）；风险：低-中**（复用面大，新代码主要是入口与校验）。

**建议**：值得做——它是四个缺口里唯一"小改动、独立价值"的项；也是 2.6-11 唯一能转 ✅ 的路径。

---

## 3. `4-04` Addon 同组件多实例

**代码事实**：`pkg/apis/core/v1/schema.go:55` 按组件 `(name, version)` 去重
（"component has been installed"）；组件注册表以 name-version 为键
（`component.Register`）；Addon 身份 = `{name, version, config}`，无实例名；
cluster 侧状态只有"已安装"一个布尔位。

**最小可用语义**：
- 引入**实例名**：Addon 增加必填 `instance`（或复用 metadata.name），同组件多实例以
  实例名为身份；`checkComponents` 去重键改为实例名，重名才 400。
- 每实例状态与定向操作：`cluster.status.addons[]` 记录 `{instance, name, version, config}`；
  uninstall/upgrade 按实例名定位。
- **真正的代价在各组件模板**：实例隔离要求组件的集群内资源名（namespace、SA、
  CSI driver 名、SC 名）按实例加前缀——nfs-csi 尚可（SC 名/manifests 目录本就来自 config），
  metallb 的 namespace/CRD 名是硬编码，需要模板化。逐组件改造 + 每组件真机
  （双实例共存、分别卸载、互不影响）。

**实现边界**：API 身份与去重改造（小）+ 每个需要多实例的组件的模板参数化（每组件 0.5-1 天）
+ 状态结构与定向升级/卸载（中）+ 双实例真机矩阵。
**工作量：大；风险：中**（组件间差异大，容易漏掉硬编码资源名）。

**建议**：暂缓。当前无真实多实例需求；若未来需要，优先只对 nfs-csi 一类"配置天然可隔离"
的组件开放（identity 改造先行）。

---

## 4. `4-09` 集群模板 templateRef 与删除保护

**代码事实**：`Template`（`pkg/scheme/core/v1/template_types.go`）= name + `Config`
RawExtension，CRUD 路由齐全（GET/POST/PUT/DELETE /templates）；Cluster 无 `templateRef`
字段（建群 payload 不记录来源模板）；模板删除无引用保护；实例化=调用方手工复制 config。

**最小可用语义**：
- 创建集群时若 payload 来自模板：在 Cluster 上记录 `metadata.annotations[
  kubeclipper.io/templateRef]=<template name>`（沿用现有注解风格，与
  `kubeclipper.io/actual-name` 同类；不改 Spec 结构、不破坏旧对象）。
- 语义选择（推荐 **快照式**）：模板更新不回溯已建集群（集群建好后与模板解耦）——与
  `packagePlan` 物化语义一致，实现最简单、行为可预期。
- 删除保护：`DELETE /templates/{name}` 先按注解反查引用中的集群（need an index/list
  查询 `labelSelector` 不适用——注解查询用 ListTemplates + 客户端过滤，或给集群加
  对应 label 双写以便 selector 过滤），存在引用 → 400 并列出集群名。

**实现边界**：Cluster 创建路径写注解（+可选 label 双写）、模板删除 handler 反查、
单测（引用存在 400 / 无引用 200 / 快照不回溯）、真机一轮（建模板→建群→改模板→
确认集群不变→删模板 400→删集群→删模板 200）。
**工作量：小-中（≈0.5-1 天 + 真机）；风险：低**。

**建议**：四个缺口中**优先做这项**——改动最小、直接把 4-09 转 ✅，且快照式语义与产品
既有"plan 物化"哲学一致。

---

## 5. 汇总与建议顺序

| Case | 最小方案 | 工作量 | 风险 | 建议 |
|---|---|---|---|---|
| 4-09 templateRef | 注解记录来源 + 删除引用保护（快照式） | 小-中 | 低 | **优先** |
| 2.6-11 extension 独立入口 | 独立 CLI/API，只组装 extension 步骤 + 隔离取证 | 中 | 低-中 | 次优 |
| 4-04 addon 多实例 | 实例名身份 + 组件模板参数化 | 大 | 中 | 暂缓（无场景） |
| 2.2-04 convertNodes | add/remove 编排糖（kubeadm 无原地转换） | 大 | 高 | 暂缓（手工 remove+add 已可达） |

共同前置：平台当前已按用户授权清理（无运行中的 KubeClipper），任何真机验证需先从
fork 构建重新 deploy 平台（R34 已产出 linux/amd64 server/agent 构建）。
