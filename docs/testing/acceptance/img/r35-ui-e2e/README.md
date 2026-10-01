# R35 Console UI 端到端取证截图（4-07，2026-10-01）

浏览器自动化（IAB）经 Mac→dev-2 端口转发访问 Console（caddy :80），专用用户登录后
依次完成创建态核验、UI 删除、Failed Operation 展示检查。截图按时间序编号。
两个探针集群：`r35-ui-probe`（成功链路）、`r35-fail-probe`（错误 image-registry 注入失败）。

| 文件 | 内容 |
|---|---|
| 01-login-page.png | Console 登录页（欢迎登录 KubeClipper） |
| 02-cluster-running.png | 集群列表：`r35-ui-probe` 运行中（1M/1W，2026-10-01 12:30:23，共 1 项） |
| 03-delete-confirm-and-toast.png | 删除确认弹窗（含回收站策略文案）与“删除集群 r35-ui-probe 成功。”toast 同框 |
| 04-deleting-state.png | 集群列表：`r35-ui-probe` 状态“删除中” |
| 05-delete-complete-empty.png | 删除完成：列表“暂无数据/共 0 项”（约 3 分钟完成） |
| 06-failed-cluster-list.png | 集群列表：`r35-fail-probe` 状态“安装失败”（kc-broken registry 建群注入） |
| 07-failed-detail-oplog-empty.png | `r35-fail-probe` 详情页“操作日志”tab：**暂无数据/共 0 项**（产品发现③证据：console 查询旧 API group `core.kubeclipper.io/v1/operations` 返回 404，而操作对象实际在 `operations.kubeclipper.io/v1alpha1`，服务端带 labelSelector 查询正常返回 Failed 操作） |
| 08-failed-detail-info.png | `r35-fail-probe` 详情页“详情”tab：基本信息/网络信息完整（v1.35.8、离线、containerd 1.7.29、calico 等） |

对应 API 侧事实：CreateCluster `c173414e-364a-4222-81ef-b59534134f58` phase=Failed
reason=StepFailed（installRuntime 步骤重试耗尽，04:44:48→04:47:28 UTC，15 步）；
DeleteCluster 操作完成并与集群对象一同 GC。
