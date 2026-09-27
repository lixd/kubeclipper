# R32 遗留 Case 处理计划（2026-09-27）

## 基线与目标

- R31 基线为 194 项：177 ✅、7 ⚠️、10 ❌。工作目录是 `lixd/kubeclipper` fork 的
  `feat/oci-operation-v2-migration`；R32 KubeClipper 候选 revision 为
  `cff7e1a075ae6563a1da5202b4bf502e313ac49a`（`v2.0.3-rc.29`）。
- 按用户指令使用 sh-dev-2/3/4；若部署了 KubeClipper，轮次结束执行 `kcctl clean -A -f`。
  共享 Registry 与本机 7890 代理隧道单独保留。
- 目标：完成可在当前三台 amd64 主机上真实运行的遗留用例；外部 IdP、BGP 对端、双栈路由、
  arm64 和产品语义缺口不以模拟环境替代。

## 执行顺序与验收条件

| 阶段 | Case | 处理方式 | 完成条件 |
|---|---|---|---|
| 1. 修复后建群复验 | 2.1-15 | 使用 `--only-install-kubernetes-component` 建临时集群；确认 kubeconfig 同步/`kc-server-secret` 注册；手动装真实 Calico chart 并验 Ready | CreateCluster、SyncKubeConfig Operation 成功；三节点与系统 Pod Ready；集群删除 |
| 2. 终端与 exec | 4-12a、4-12b | 在隔离集群内分别验集群终端、节点 SSH 终端与 Pod exec 的鉴权、命令、resize、断连/重连 | WebSocket 状态码、命令回显、resize、生效的断连重连与临时账号清理 |
| 3. Console 核心 E2E | 4-07 | 浏览器登录、建群、Operation 进度/成功/失败展示、升级、备份、删除；缺少前置时记录 UI 明确阻塞原因 | UI 页面、API/Operation 状态一致；每个子流程有结果；临时资源清理 |
| 4. 其余真实前置 | 1.1-01、2.6-11、3-01、4-09、6-06、6-10、6-11、2.2-04、4-03、4-04、4-11、7-01、2.1-32 | 核实既有结果和当前主机条件；只在具备真实网络、硬件、身份凭据或明确产品语义时运行 | 保留精确缺失条件；不把代码检查、dry-run 或非目标环境当成 E2E |
| 5. 清理与回填 | 全部 | 按最新指示从 sh-dev-2 执行一次全平台 clean；核实三台服务与 etcd 数据状态、Registry 和代理隧道；回填清单、缺口台账与报告 | 三台 KubeClipper 组件已停止；独立 Registry、代理隧道仍可用；统计总和为 194 |

## R32 结果

- 阶段 1、2 完成，关闭 2.1-15、4-12a、4-12b。
- 阶段 3 部分完成：Console 登录、离线 Registry 前置校验、成功建群和 Operation 展示已实测；临时集群未绑定 BackupPoint，离线升级版本目录没有高于 v1.37.0 的目标版本，未产生安全可控的失败 Operation；UI 删除确认未提交。4-07 保持 ⚠️。
- 阶段 4 仍有 8 个 ⚠️、6 个 ❌；具体依赖与状态见 [`status-2026-09-27-r32.md`](status-2026-09-27-r32.md)。
- 阶段 5 按用户要求完成：`kcctl clean -A -f --assumeyes` 从 sh-dev-2 执行成功；三台的 KubeClipper 服务和 etcd 数据已清理，sh-dev-3 的 Registry `/v2/` 仍返回 200，本机 7890 隧道仍运行。

本轮代码与报告只在 KubeClipper fork 工作树内更新；未改源仓库或 Console fork。
