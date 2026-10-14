# 本轮新发现

1. E2E Registry 用例错误假定全局仓库数量为0，且 cleanup 删除所有仓库。已修改测试代码，仅清理initRegistries声明的四个对象，计数加上已有对象。真实Fast suite从6/7恢复7/7，预置r46-existing-registry保留。仅测试代码变更，不属于候选7250c886生产二进制。
2. 自定义聚合view角色：API GET clusters返回200，CLI login成功，CLI get cluster却403。真实TLS转发记录发现CLI首个请求为GET /api/core.kubeclipper.io/v1/configmaps?paging=limit%3D-1%2Cpage%3D0。FromConfig用列举ConfigMap验证客户端，导致本无需读取平台配置的普通命令依赖该权限。未扩大角色权限绕过；待根因修复与复测。

## Console Operation V2 未适配

本轮实际 Chrome：console v1.6.0 操作日志/查看日志仍请求 `/api/core.kubeclipper.io/v1/operations`（404），不是当前 `/api/operations.kubeclipper.io/v1alpha1/operations`。页面空列表；真实 V2 API 可见完整执行账本。需修改 console 源码和发布制品；不能将 API 通过代替 UI 通过。

## join 预检失败退出码

本轮 `join --agent 192.0.2.1` 的真实 SSH 超时记录日志，但命令退出 0；NewCmdJoin 的 preCheck=false 分支直接 return。deploy 对不可达 SSH 则退出 255。待修复 join 退出码。

## R46 修复与复验进展

- `b985a0b1`：移除 CLI 普通访问的 ConfigMap 探针、拒绝重复节点、识别当前 OCI Registry 包二进制路径。fork qualification 38393393159 成功；实际 CI kcctl 的 readonly 登录/列群和 Registry 部署均通过。
- `4283e666`：join 预检失败返回非零；Operation 翻页必须设置 Exact resourceVersion。分页修复前发生快照变化，修复后真机 12 页固定同一 RV、无遗漏重复。join CLI 负向路径还需当前 CI 复测。
- `c0018d6a`：空配置卸载采用已安装 addon 的唯一配置，歧义实例要求明确配置。NFS 自定义 StorageClass 原先残留；修复后删除 SC/addon，ErrIgnore 的失败步骤保留真实 Failed 状态。
- 当前 c001 CI qualification 38408214697 完整成功；`upgrade all --manifest` 已实际升级 Server、3 Agent、Console 和 kcctl。API/CLI 均 clean 同 revision，doctor 19/0/0，源码 dirty 临时二进制已被替换。
- Console `be744081` 兼容 Operation V2 与节点 IP 展示，发布到 fork `v1.6.0-r46.1`，官方 v1.6.0 的 operations 404 已消除。
- Console `982b1b6b`：快速进入配置步骤可在 metadata 返回前固化空版本列表。版本请求移至该步骤 init 并等待；生产构建通过，fork `v1.6.0-r46.2` 已发布，真机 Chrome 创建 1M2W 成功、端口预检失败及 retry 成功。平台打包引用改为 1447d7cc，qualification 38418406540 正在运行。
- 上述修复阶段和候选来源会分别列入报告；尚未执行的用例保持待执行，不将此前轮次结果计入本轮。

## R46 后续复验（2026-10-14）

- Calico `first-found` 在多网卡节点 dev-4 选择 LXD 网桥 `10.8.39.1`，导致跨节点业务 Pod 超时。真实 Installation 配置改为 `interface: ens3` 后 VXLAN underlay 为 `172.16.131.230`，三节点 NodePort 请求均 200。失败基线保留在 `business-probe-before-interface-fix.json`，持续修正基线与升级窗口另记，不抹除超时结果。前置条件已补入 checklist。
- 用户同意增加 `upgrade --release-base-url`。`eb2d90e2` 接入现有 Downloader 的 BaseURL；真实 HTTP 测试服务器验证路径与 checksum 正反向，upgrade/releasemanifest 单测通过。fork 发布候选版本更新为 v2.0.4。
- `66a57cfd` 同步默认 Delivery Policy bootstrap 版本，修复升级版本后 CI policy consistency 拒绝；本地 policy consistency 验证通过。当前 fork qualification run 38494779724，结果待完成，尚不计在线 --version 通过。
- 真实 TLS/auth Registry 的生产 Fetcher 隔离 Linux helper 复验缓存：仓库断连时有效缓存成功；文件损坏后明确失败；恢复 Registry 后重新下载并匹配原 SHA256。证据 `cache-proof.json` 与三个日志。测试规则在 finally 删除。

- `d2532cf7` qualification 38496629970 完整成功，go-test/AIO/offline CI 均成功。fork `v2.0.4` 为测试候选 Release，真实 `--version --release-base-url` 已全组件升级；live SHA 与同 SHA qualification bootstrap 一致，Console `07f527e/r46.3`，doctor19/0/0。证据 `fork-version-live-proof.json`。
- stable-tag release workflow 38498033932 在验收门禁正确拒绝：缺 `acceptance/d2532cf7....yaml`；门禁尚未复跑，不能将该工作流记成功。后续必须完成当前候选实际验收、添加准确 scoped record，再复跑；测试候选 Release 不等于已完成稳定发布门禁。
- 升级测试脚本首次从 CLI Upgrade Operation 复制 steps/spec，漏复制 metadata 的 upgrade-version label，导致真机升级完成但平台版本 projection 为空。已用完整原 metadata 真实重跑所有升级步骤（`upgrade-projection-repair.json`），恢复平台137/Running与实际3节点137 Ready一致。不修改步骤或伪造终态。
- 证书更新互斥为排队执行而非拒绝第二个 API：`cert-update.json` 两次真实请求均接受，第二次 Operation/Task 的开始时间不早于第一次结束。证书 serial/fingerprint变化，真实HTTPS readyz通过。原测试脚本期待 API400 的断言已纠正。

- 扩容复验发现 Server 重启后，历史 Operation 被重新投影：无 upgrade-version 的旧测试 Operation 将版本清空，未知 R46Validation 的取消/超时将状态改为 UpdateFailed。不是扩容步骤失败，实际扩容 API 在预检阶段 400。`d21bee5f` 修复历史业务状态覆盖、忽略无业务投影的动作、缺升级版本明确拒绝；operationv2 单测通过。fork qualification 38503048276 正在运行，尚未真机验证修复。

- `d3278b8a` 将升级版本与集群阶段分别投影：更新后的业务动作负责阶段，最新成功升级负责版本；避免升级完成时已有其他动作排队导致漏写版本。包含历史重放、缺版本、后续删除运行中仍恢复升级版本且不改 Terminating 的单测通过。
- fork 稳定发布 workflow 原输出 `release-manifest.yaml`，而版本 Downloader 请求 `release-manifest-<version>.yaml`。已修复 github-release 的自动制品命名与 checksum（实际执行该 shell 步骤，输出内容及 checksum 匹配）；新候选 `v2.0.5`，不移动 `v2.0.4` 已发布 tag。CLI upgrade/deliverypolicy、delivery/apis、operationv2 单测及 policy consistency 通过；真机与完整 qualification 待继续。

### R46 external unimport deletes borrowed Kubernetes credentials

After the actual independent-platform import/unimport, source Kubernetes Nodes stayed Ready and a real ConfigMap write/read/delete succeeded. However the importer unconditionally deleted the pre-existing `kube-system/kc-server` ServiceAccount and `kc-server` ClusterRoleBinding during cleanup. Kubernetes removed the source platform's `kc-server-secret`; source Server restart generated SyncKubeConfig that failed with `secrets "kc-server-secret" not found`, preventing pending role conversion from being built. This is a product defect, not an image/proxy prerequisite. Ownership-aware creation and deletion plus a new real import/unimport/source-operation regression are required. R46 `4-11` is reopened until that regression passes.

### R46 demotion/removal tears down local API before Pod termination

Current v2.0.6 demotion operation `8f77804a-d4bc-4fa5-b105-c1032b874a90` removed the node's etcd member before drain. While drain was Running, the leaving node's kubelet repeatedly reported local API connection refused, its local etcd/API Pods CrashLooped, and evicted Typha/business Pods remained Terminating. The operation eventually succeeded, but this ordering makes graceful termination depend on disconnected-node cleanup. Plan regression tests failed before the fix for both ConvertNodes demotion and GenNode master removal. Both builders now drain with normal PDB eviction before removing etcd membership, followed by reset. Current candidate CI/live verification is still required. The test helper's subsequent Ready check separately failed because its old ServiceAccount token had been invalidated by the unimport defect; the helper now uses the freshly signed real-CA IP kubeconfig.

### R46 offline control-plane join certificate upload queries public version

On v2.0.8, the actual worker-to-master promotion exhausted getJoinCommand retries. The existing 10-second task ran kubeadm token creation and then `kubeadm init phase upload-certs --upload-certs` without a configuration. Agent logs show `signal: terminated`; reproducing the same command in the same airgapped environment succeeded only after waiting for `dl.k8s.io/release/stable-1.txt` to time out and falling back to the local v1.37.0 version. This is a product offline-path defect. Candidate `0c37f1d7` / v2.0.9 reads the live kubeadm-config ClusterConfiguration, writes a private temporary configuration, uploads using `--config`, and removes the temporary file. Missing or empty configuration fails explicitly. The new regression test failed before the fix and targeted k8s/delivery-policy tests pass afterward. Current qualification and real promotion retry remain required; old v2.0.8 qualification is not reused for the new revision.

The exact-source v2.0.9 Agent on dev3 was applied as a temporary local-build verification, with Go1.26.1, source SHA 0c37f1d7 and archive tree state explicitly recorded. Formal retry of the original promotion Operation 5fc26f60-9a88-434d-9b22-08b5faec46dd succeeded with observedRetryGeneration=1; the successful upload uses a private temporary live configuration and contains no dl.k8s.io lookup. Three Nodes are Ready, three etcd endpoints are healthy, and the two-replica PDB-protected workload is ready. This is real repair evidence, not final CI-artifact qualification proof; final same-revision artifact rollout remains pending. See v209-local-promotion-retry.json.

### Final v2.0.9 CI artifact regression

Qualification 38529958987 completed successfully for exact candidate `0c37f1d761823b34511c21209cb0d7c42ee36bbe`; the same revision Go, AIO and offline-resource CI runs succeeded. All nine live Server/Agent/kcctl binary hashes on three machines match the actual amd64 CI artifact. Native upgrade skipped the temporary exact-source Agent because its revision matched; that Agent was explicitly replaced by the real CI binary before final role regression, and the strict nine-hash check passed.

The actual CI artifact completed demotion, promotion, master removal and master addition under blocked public egress. Final three Nodes and etcd endpoints are healthy, the business PDB remains minAvailable=1 and two business replicas are ready, and the original package plan is unchanged. After removing the local master, the verification script could no longer resolve its kubeconfig hostname; the private kubeconfig was corrected to a surviving master IP and verification continued. This script interruption is retained in the evidence and is not classified as a failed product operation. See `v209-upgrade-ha.json` and `v209-master-lifecycle.json`.

The borrowed-resource ownership defect was separately verified fixed on actual v2.0.8 artifacts: independent import/unimport retained the original six Kubernetes resource UIDs, real API writes succeeded, and a subsequent source native role-conversion operation succeeded. See `v208-external-import.json`.

### R46 final closeout

Final fork stable release workflow 38534627843 succeeded for exact candidate 0c37f1d7/v2.0.9. Real native fork --version consumption succeeded; the released versioned manifest/checksum matches the original qualification manifest checksum. Three Console components were actually upgraded; Server/Agent/kcctl at the same revision were correctly skipped. Initial consumption helper used the wrong working directory and could not find login configuration; rerunning from /root succeeded.

All 194 case dispositions are recorded: 190 passed, zero partial/failed/pending, four out of scope and zero blockers (native ARM64 explicitly excluded by the user; native execution remains untested). Original single-Server baseline, three original Agent identities, zero clusters, saved binary hashes, exact Server config and doctor19/0/0 were restored and independently checked against private baseline files. Owned fixtures and NFS mounts were cleaned while protected original Registry5003 and NFS root remain.
