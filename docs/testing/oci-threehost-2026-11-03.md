# OCI 三机全量复验（执行中）

候选：用户 fork `lixd/kubeclipper` 的 `5ffb13235d29ca02c65254c6267e892303b1fecd`；Console 为 `ee51883a2126324a567fa17cb1020e39916d17aa`。本轮从零执行 192 条清单，历史通过不计入本轮。台账：`evidence/oci-20261103/cases.json`。只有新日志与实际行为符合用例要求才记通过。

环境为 sh-dev-2/3/4（172.16.131.208/146/230），Ubuntu 24.04 amd64；共享 Registry 5003 和 NFS 数据保留。允许替换旧测试平台和删除旧测试集群。执行顺序：备份与来源核对、升级与现有集群检查、正常删除与残留检查、在线重建、生命周期/备份/Addon/API/UI/故障注入、不同拓扑与离线消费、逐项复核。

## 基线

2026-11-03 实际三机仍运行 71319775。集群 online-ens3-71319775 为 Running，三节点 Ready，Kubernetes v1.37.0 / containerd 2.2.4。dev-2 doctor 为 25 passed、0 warnings、0 failed。dev-3/4 默认 doctor 由于缺少 /root/.kc/config 失败；该检查结果不能解释为服务失败，也不能记为新版本通过。

每台保存了配置、旧二进制及 SHA256；dev-2 已额外保存 etcd 快照。含认证信息的恢复资料保存在各机 /root/kc-oci-5ffb1323-20261103/private，权限 0700/0600，不进入 Git。

## 制品准备

已在 fork 推送 qualification 标签 `oci-qualification-5ffb1323-20261103`，触发 [41615765140](https://github.com/lixd/kubeclipper/actions/runs/41615765140)。标签固定到本候选，不修改上游。工作流执行中，尚不能记为通过。

当前仅完成环境盘点与备份。新版本部署、在线建群、192 项测试尚未完成。

## 发布版本前置检查

首轮 qualification 按既有配置构建 v2.0.9，而现场 Server 为 v2.0.10-e2e.71319775；正常升级会触发降级拒绝。首轮执行已取消，不计为通过。将 release 与 kubeclipperVersion 同步为测试候选 v2.0.11-oci.20261103，保持降级保护不变，重新生成 qualification。dev-3/4 的 doctor --config 复查因旧 CLI 无此选项退出，保留为未完成；未复制认证资料修饰结果。
