# Operation v2：取消创建后集群删除死锁（SyncKubeConfig + 排序门 + 执行锁）

- 日期: 2026-09-13
- 分支: feat/oci-operation-v2-migration
- 发现环境: OCI E2E Round 2（P4-01/04 注入测试：创建中重启 server → 取消创建 → 删除集群）
- 状态: 已修复（963460b8 + 251b88f5），测试环境现场验证通过

## 现象

取消一个进行中的 CreateCluster 后再删除该集群，DeleteCluster operation 永远停在
Running：17 个步骤全部 Pending、0 个 OperationTask 被创建，server 重启也无法恢复，
集群成为不可删除的僵尸。

## 复现路径

1. 创建集群（≥1 master）
2. 创建过程中重启 kc-server（Operation 存活并继续推进——本身符合预期）
3. 取消创建（协作式取消：等待运行中的步骤结束后进入 Canceled 终态——符合预期）
4. `kcctl delete cluster <name>` → DeleteCluster 永久停滞

注意第 2/3 步都是符合设计的操作；死锁在随后的删除中成形。

## 根因（两级循环等待）

删除提交时 cluster-controller 发现集群没有可用 kubeconfig（创建被取消），
于是创建 `SyncKubeConfig` operation 作为删除的前置依赖。该 op 的创建时间
排在 DeleteCluster **之前**（同一秒内先落 etcd，resourceVersion 更小），
而它自己又被 DeleteCluster 持有的执行锁挡住，形成互等：

```
DeleteCluster（Running，持锁，deadline 已过期）
  └─ 被 isEarliestRunnable 排序门 requeue：
     "有更早创建的非终态 op（SyncKubeConfig）"
     → 永远到不了 reconcileDeadline（deadline 兜底失效）
     → 永远不释放锁、不创建 task
SyncKubeConfig（Pending）
  └─ acquireLock: 锁 holder = DeleteCluster → owned=false
     → 永远 Pending（且 Pending 状态不可 retry）
```

关键代码位置（修复前）：

- `pkg/controller/operationv2/controller.go` reconcileOperation：
  排序门 `isEarliestRunnable` 对所有非终态 op 生效，包括已 Running 的 op。
  Running op 被 requeue 后，通往 finish/cancel/deadline 的全部路径被切断，
  而这些路径正是锁唯一的释放出口。
- `pkg/controller/clustercontroller/controller.go` getKubeConfig（:439）：
  对非 Succeeded 的 sync op 永久报错
  `kubeconfig sync operation %q finished with phase %q`，不区分
  "尚未执行（应等待）"与"已失败（应重建）"。

## 修复

1. **251b88f5** `fix(operationv2): let running operations reach their terminal paths`
   排序门只约束 Pending（未获得执行权）的操作。Running op 已持有执行锁，
   必须始终可达 finish/cancel/deadline 路径，否则锁随 op 一起被饿死。
   终态后锁释放（finish 内联 + releaseTerminalLock 兜底），被阻塞的
   更早 op 随即获得执行权。
2. **963460b8** `fix(cluster): skip kubeconfig sync for terminating clusters`
   Terminating / TerminateFailed 相位的集群跳过 kubeconfig client 同步
   （与 Installing / InstallFailed 的既有跳过一致）：删除的清理步骤全部在
   agent 侧执行，不依赖集群 clientset，从源头消除删除时创建 sync op。

## 验证

- 单测：
  - `TestRunningOperationProceedsDespiteEarlierPendingOperation`
    （复刻死锁：Running+持锁+更早 Pending+deadline 已过 → 期望 TimedOut、
    锁释放、随后被阻塞 op 获得执行权）
  - 既有排序/同秒/rv 决序测试全部保持通过（Pending 串行语义不变）
  - `TestSyncClusterClientSkipsPhasesWithoutUsableKubeconfig`
- 现场验证（3 节点测试环境，卡死现场保留 1.5h 后部署修复版 server）：
  - DeleteCluster: Running → **TimedOut**（deadline 兜底生效，锁释放）
  - SyncKubeConfig: Pending → 获得执行权 → Failed（集群控制面已被部分
    重置，符合预期的合理终态）
  - 重新 `kcctl delete cluster` → 删除成功，集群对象与节点残留清理干净

## 遗留 / 后续

1. 【可观测性】运行失败 task 的 `status.reason` / `status.message` 为空，
   排查只能翻 agent journal。建议把自定义步骤的失败原因写回 task 状态。
2. 【设计改进，未在本修复范围】删除路径依赖集群 clientset 的步骤梳理：
   彻底做到"删除不依赖 kubeconfig"，使任意失败态集群都可一键清理。
3. 【回归防线】建议 P4 回归集固定本场景：创建中重启 server → 取消 → 删除，
   断言 DeleteCluster 到达终态且集群对象被移除。
