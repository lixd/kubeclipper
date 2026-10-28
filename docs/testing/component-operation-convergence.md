# 组件操作的状态收敛

适用动作：`UpgradeExtension`、用户发起的 `InstallComponents` 和 `UninstallComponents`。

请求先构建并校验完整 Operation V2 计划，再以一次 Cluster 条件更新保存预期组件配置、`Updating`、稳定的 Operation 名称及执行计划。Cluster 控制器从该持久记录创建 Operation；创建失败会重试同一名称，Server 重启也从同一记录继续。操作尚未发布时，Cluster 的 `kubeclipper.io/component-operation-owner` 标识受理身份，`kubeclipper.io/component-operation-error` 显示最近的发布错误。组件增删接口同时返回 `X-Operation-Name`。

校验不通过时不会写入 Cluster。Cluster 写入响应丢失或请求被取消时，使用有超时限制的独立上下文强一致核查受理身份；不能确认时返回明确的结果未知错误。Operation 创建响应丢失时，控制器先强读同名 Operation 并比对完整不可变计划，再决定是否重试创建。存储恢复后继续受理，不通过读不到 Operation 就立即回滚，避免迟到提交与配置回滚竞争。

Operation 必须拥有目标 Cluster UID 和本轮更新归属才能开始执行。成功后 Cluster 为 `Running`；失败、超时或取消后为 `UpdateFailed`，Operation 和 Task 的真实终态、错误保留。`UpdateFailed` 表示本次更新未成功，不表示 Kubernetes 控制面必然不可用。终态投影只修改拥有的更新状态和内部受理注解，不回滚整个 Cluster，因此不会覆盖同时更新的无关字段，也不会将部分执行当成没有发生。

重试沿用既有的最新 Operation、UID、resourceVersion 和 Task 全部终态检查；取得执行锁后，先以新 generation 预留 `Updating`，再重新执行。旧 generation 的终态不能结束新一轮更新。新操作或备份已经占用状态时，拒绝该重试并保留错误，释放锁。普通 Cluster 编辑不能覆写内部受理注解。

后台 CRI Registry 同步虽然复用 `InstallComponents` 动作名，但没有用户更新归属，不投影上述 Cluster 状态。没有新归属标记的历史 Operation 也不会仅凭动作名改写 Cluster；历史残留状态需要结合实际 Operation、Cluster UID 和当前任务核查，不能猜测归属。

本机制保持 Cluster 先写入、Operation 后创建的顺序，只覆盖上述三种用户操作。存储持续不可用时无法承诺完成；发布错误保留在受理记录和控制器日志中，存储恢复后由控制器继续。整个 Operation 生命周期统一改造不在本次范围。
