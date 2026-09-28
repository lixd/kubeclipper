# R33 遗留 Case 处理计划（2026-09-28）

## 范围与约束

- 工作目录：`lixd/kubeclipper` fork 的 `feat/oci-operation-v2-migration`；源仓库不修改。
- 目标：闭环 `3-01` 纯离线首次部署，修复并验证 deploy/join 的 bootstrap source revision 选择；复查 `4-07` 可推进子项。
- 主机：sh-dev-2/3/4。共享 Registry `172.16.131.146:5003` 只读；测试包只写入 sh-dev-3 的临时 5004 Registry。
- 每次部署前后执行全平台 clean；保留独立 Registry、现有 SSH 授权和用户的代理配置。

## 步骤与结果

| 步骤 | Case | 验收条件 | 结果 |
|---|---|---|---|
| 1 | `3-01`、`3-04` | deploy 按 kcctl commit 选 kubeclipper 包；join 按 server commit 选 agent；第三方包仍按自身版本选择 | fork commit `0eee6cd5` 完成修复；`go test ./pkg/cli/deploy ./pkg/cli/join` 通过 |
| 2 | `3-01` | 空白三机、无公网出口、临时 Registry 上做首次 deploy；client/server revision 一致，Healthy 且 doctor 无失败 | rc.30 从隔离 5004 部署成功，commit 一致，doctor 25/25；3-01 记 ✅ |
| 3 | `3-04` | Registry 同时含旧 legacy tag、异 revision 与匹配 revision 包；空闲 Linux 节点 join 后 agent revision 与 server 一致 | 最小拓扑 live join 成功，doctor 17/17；3-04 保持 ✅ |
| 4 | `4-07` | 尝试读取 Console 浏览器状态；仅在浏览器可用且有真实前置时继续 UI 子流程 | 浏览器状态读取超时并重置，未新增 UI 证据；4-07 保持 ⚠️ |
| 5 | 清理与回填 | 停止临时 Registry，删除临时配置和端口转发，撤销本轮网络规则，确认三台 clean、5003 可用并更新台账 | 已完成；三台 KubeClipper 服务 inactive，`/var/lib/kc-etcd` 不存在，5003 `/v2/`=200 |

## R33 后仍未闭环

- `1.1-01`：R32 经 7890 代理到达 GHCR token endpoint 后返回 `DENIED`；仍需能读取目标 GHCR 制品的凭据/权限。
- `4-07`：仍缺 UI 删除、升级、备份和安全可控的 Failed Operation 展示；本轮浏览器状态读取不可用。备份需要 BackupPoint，升级需要高于当前版本的离线目标包。
- 其他 5 个 ⚠️、6 个 ❌ 依赖 arm64/正式 OS 矩阵、外部 BGP/OIDC/CloudProvider 环境或待定义的产品语义；详细条件沿用 [`status-2026-09-27-r32.md`](status-2026-09-27-r32.md)。

完整执行证据见 [`status-2026-09-28-r33.md`](status-2026-09-28-r33.md)。
