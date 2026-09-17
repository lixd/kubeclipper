# KubeClipper R4 核心功能实测报告：sh-dev-2/3/4

日期：2026-09-17
状态图例：✅ 已实测通过 · ⚠️ 部分验证 · ❌ 未验证或发现缺陷

本轮按照 [`docs/testing/core-feature-checklist.md`](../../testing/core-feature-checklist.md) 对 OCI
合并基线的核心部署、集群生命周期、`kcctl` 和 HA 能力进行真实运行验证。结果已回填长期清单；
未覆盖项仍保留在 [`round-2026-09-gaps.md`](../../testing/round-2026-09-gaps.md)。

## 1. 范围、版本和证据边界

| 项目 | 实测值 |
|---|---|
| Git 分支 | `feat/oci-operation-v2-migration` |
| 本地 `kcctl` | `v1.7.0-71+45ca67b86148b2`，linux/amd64，GitCommit `45ca67b86148b2476063460a2e1b30061f818755` |
| 主机 | `sh-dev-2` / `172.16.131.208`、`sh-dev-3` / `172.16.131.146`、`sh-dev-4` / `172.16.131.230` |
| 平台 Package Registry | `http://172.16.131.146:5003`，OCI，三台主机共用 |
| Registry manifest revision | `1a70255b70d420bf3159e853d6aa8e8bc928056c` |
| Server/Agent 运行包 | `v2.0.0`，GitCommit `1a70255b70d420bf3159e853d6aa8e8bc928056c` |
| 集群物料 | Kubernetes `v1.37.0`、containerd `2.2.4`、Calico `v3.31.5` |

Server/Agent 运行包是 Registry 中的 OCI 发布包，属于本地分支的祖先 revision；它不是当前
分支 HEAD 的重新发布包。因此本报告证明了 OCI 发布包在三台主机上的部署和核心链路接受情况，
不替代“用当前 HEAD 重新构建并发布 Server/Agent 后”的最终发布验收。报告和提交文件不包含
kubeconfig、token、私钥或 Registry 凭据。

远端原始证据保存在 `sh-dev-2:/tmp/kc-evidence-20260917/`。含 kubeconfig 的 YAML/JSON 已限制
为 `0600`，只保留在远端，不提交到 Git。关键证据文件包括：

- `ha-doctor-final.txt`、`ha-status-final.txt`、`ha-etcd-members-final.txt`、`ha-etcd-status-final.txt`；
- `ha-doctor-server4-down.txt`、`ha-doctor-etcd4-down.txt`、`ha-doctor-after-fault.txt`；
- `ha-k8s-nodes-final.txt`、`ha-k8s-pods-final.txt`、`ha-network-smoke.txt`；
- `min-create-cli.txt`、`min-k8s-nodes.txt`、`min-smoke-pods-final.txt`、`min-smoke-results.txt`；
- `ha-policy-custom-validate.txt`、`ha-policy-custom-diff.txt`、`ha-policy-custom-get.yaml`；
- `registry-delete-cli.txt`、`registry-recreate.yaml`、`registry-final-delete-cli.txt`。

## 2. 执行前基线和清理

三台主机原先已有平台、Agent 和 `p-up` 集群。为避免把旧拓扑状态误算到 HA 部署：

1. 先保存远端基线和 Agent 配置（权限 0600）；
2. 删除原 `p-up` 集群并执行 `kcctl clean --all -y`；
3. 以三 Server、三 Agent、HTTP OCI Package Registry 重新部署平台；
4. 测试结束删除 `ha-core-20260917`、`min-core-20260917` 及临时网络测试 Namespace。

最终三台主机上的 `kc-server`、`kc-etcd`、`kc-agent`、`kc-console` 均为 active；Kubernetes
`kubelet`、`containerd` 和 `/etc/kubernetes/admin.conf` 均已由集群删除流程清理；平台最终
`status` 和 `doctor` 均为 Healthy（Server 3/3、etcd 3/3、Agent 3/3）。共享的独立 Package
Registry 服务 `kc-oci-r3-registry` 保留运行，未删除其他团队物料。

## 3. 已完成的测试

### 3.1 平台 HA 部署（1.2-03、3-26、1.3-08）

使用三台主机作为 Server 和 Agent 部署：

```text
kcctl deploy -y \
  --server 172.16.131.208 --server 172.16.131.146 --server 172.16.131.230 \
  --agent 172.16.131.208 --agent 172.16.131.146 --agent 172.16.131.230 \
  --package-registry 172.16.131.146:5003 \
  --package-registry-scheme http \
  --ip-detect cidr=172.16.131.0/24 \
  --node-ip-detect cidr=172.16.131.0/24
```

结果：

- `kcctl status`：`kc-server`、`kc-etcd`、`kc-agent` 全部 Healthy；
- `kcctl doctor`：`25 passed, 0 warnings, 0 failed, 0 skipped`；
- 三个 Server IP 的 `/healthz` 均返回 `ok`；
- `etcdctl member list` 显示 3 个 started、非 learner 成员，三端点健康且写入提交成功；
- Console 服务 active，但本轮没有进行浏览器 UI 登录和页面 E2E。

### 3.2 Server/etcd 单点故障（1.2-07）

在 HA 平台稳定后分别停止、恢复 `sh-dev-4` 上的 `kc-server` 和 `kc-etcd`：

- Server 停止时，dev2/dev3 `/healthz` 和集群查询保持可用；doctor 正确显示 Server `2/3` 和
  Overall Unhealthy；恢复后回到 Server `3/3`、Healthy；
- etcd 停止时，dev2/dev3 endpoint 仍健康，dev4 endpoint 连接被拒绝（符合预期）；doctor
  正确显示 etcd `2/3`；恢复后 3 个 endpoint 全部健康；
- 故障窗口没有同时运行长时间 Watch 或正在执行的 Operation，也没有做 Console 入口验证，
  所以该 Case 维持 ⚠️，不能宣称完整 HA 故障验收。

### 3.3 CLI 创建集群和固定健康验收（2.1-18、2.1-20、2.1-24、2.1-31、3-05）

#### 三 Master 边界拓扑

通过 CLI 创建 `ha-core-20260917`：3 Master、0 Worker、Kubernetes `v1.37.0`、containerd
`2.2.4`、Calico `v3.31.5`、`interface=ens3`。CreateCluster Operation
`e2d9f4ae-ea5d-405e-87a9-95106d850e9f` 最终成功，集群 phase 为 Running，etcd/control-plane
和 Calico 均健康。

由于三个 Master 都带 `NoSchedule` 污点且没有 Worker，CoreDNS 会保持 Pending。手动去除三个
Master 污点后 CoreDNS 才调度并完成健康检查。这是“3M/0W 无 Worker”的边界现象，不应当被
当成普通 3M+Worker HA 通过；`--untaint-master` CLI 用例仍未单独验证。

#### 1 Master + 1 Worker 最小拓扑

通过 CLI 创建 `min-core-20260917`：dev2 为 Master、dev3 为 Worker，使用同一 OCI Registry
和版本矩阵。两台 Kubernetes Node 均 Ready，集群 phase Running。跨节点网络测试从两个 Pod
分别验证了：

- `kubernetes.default.svc.cluster.local` DNS 解析到 `10.96.0.1`；
- Kubernetes `/version` 返回 `v1.37.0`；
- 两个 Pod IP 之间双向 ping，0% 丢包。

该集群随后删除，平台 3/3 Agent 恢复 Healthy，证明最小拓扑和节点复用闭环通过。

两个集群对象均记录 `Overlay-Vxlan-All` 和 `interface=ens3`；HA/最小拓扑均完成跨节点
Pod、Service DNS 和 apiserver 烟测。因此 `2.1-18` 标记 ✅，`2.1-20` 仅覆盖 interface
方法，first-found/can-reach 仍是 ⚠️。

### 3.4 Worker 生命周期和节点开关（2.2-01、2.2-02、2.2-05）

在清理前的 `p-up` 集群上完成 dev4 Worker 移除、重新加入、再次移除和最终加入：

- 移除 Operation：`b3569992-7aef-4a8c-a7ac-ebb0a8b222e1`、`9c861c38-5f31-4128-b9ac-6674b4562be4`；
- 加入 Operation：`3d505248-d0ce-4de9-be85-fffe1cbc3e76`、`b698557f-963c-4e46-8cfb-ee98ffcc2901`；
- 移除和恢复前后 `status.packagePlan` digest 不变；
- 节点角色标签收敛后，disable/enable 请求均 HTTP 200，禁用标签出现并清除；
- 最终集群回到 Running，所有 Kubernetes Node Ready。

### 3.5 证书更新（2.3-04）

对 `p-up` 调用 `/api/core.kubeclipper.io/v1/clusters/p-up/certification`，
`UpdateCertifications` Operation `9906f254-e7c9-4604-a00e-3012d3990583` 成功，集群恢复
Running。但对象的 `status.certifications` 仍为空，没有可审计的 serial/有效期前后证据，
因此只记录 ⚠️；Agent 重新签发仍未独立验证。

### 3.6 `kcctl` 查询、资源和策略命令

| Case | 实测 | 结果 |
|---|---|---|
| 3-07 | 创建、查询、删除平台 Registry 资源 `oci-test-registry`；删除后 NotFound，再重建并最终删除 | ✅ |
| 3-08 | CLI 删除 `ha-core-20260917`；对象最终 NotFound，平台保持 Healthy | ✅ |
| 3-14 | `set cluster` 设置/清除 external IP/port；get 输出中的 labels 随之出现/清除 | ✅ |
| 3-20 | `resource list` 扫描 `172.16.131.146:5003`，列出 14 个 OCI package | ✅ |
| 3-21 | `resource inspect` 展示 repository、tag、digest、platform 和内容画像 | ✅ |
| 3-22 | `resource refresh` 输出 `refreshed 14 OCI packages` | ✅ |
| 3-23 | template、custom `allowedVersions` apply/get、diff、恢复；恢复后 diff 无变化 | ✅ |
| 3-24 | 合法模板通过，`selection: many` 被拒绝；未知 slot 名称当前被接受 | ⚠️ |
| 3-26 | HA 部署后和所有清理完成后 `kcctl status` 均准确显示 3/3/3 | ✅ |

策略命令的自定义变更没有宣称已经改变集群建群白名单；`2.6-09/10` 仍需用白名单内、
白名单外和缺失 slot 的实际建群 Operation 证明。

## 4. 发现的问题和边界

### 4.1 `kcctl get --watch` 是当前 CLI 缺陷（3-10）

运行 `kcctl get cluster -w` 以及列表形式时，命令只打印一次结果并以 rc=0 退出，没有长连接、
事件增量、断线恢复或 Ctrl-C 退出行为。源码 `pkg/cli/get/get.go` 声明了 `Watch` flag，
但 `RunGet/list/describe` 没有把它传入查询或建立 watch 流。清单已将该项从 ⚠️ 降为 ❌，
并纳入缺口文档；应修复实现或移除 CLI 暴露面。

### 4.2 Docker CRI 不是待补测功能（6-12）

当前产品和现行 Kubernetes 支持基线都不支持 Docker CRI（包括 dockershim/外置 Docker），
本轮没有安排 Docker E2E。实测 `kcctl create cluster --help` 和参数校验仍展示/接受
`--cri docker`，源码还保留 Docker 分支；这只是应删除的废弃入口。独立 Docker Registry 管理
命令与 Docker CRI 是不同功能，不能混为一谈。

### 4.3 发布物 provenance

本地 `kcctl` 是当前分支构建物，但 Server/Agent 来自 Registry 的 `v2.0.0` 包，revision 为
`1a70255b...`。后续发布前应使用当前 HEAD 重新生成 OCI 包、同步到测试 Registry，并重跑
至少 HA、建群、升级、删除和失败恢复主链路。

## 5. 本轮仍未完成的优先项

详见 [`round-2026-09-gaps.md`](../../testing/round-2026-09-gaps.md)，本轮确认仍包括：

- 纯离线 bundle export/import、HTTPS/自签 CA/认证 Registry；
- HA 故障窗口中的 Watch、运行中 Operation、Console E2E；
- Delivery Policy 白名单实际约束、缺失 slot 和远端制品存在性；
- 平台自身离线/在线升级及组件独立升级；
- Master 添加/移除、角色转换、完整证书/Agent 证书轮换；
- S3 备份、真实周期、enable/disable 和 `maxBackupNum` 轮转；
- Operation cancel、OCI 缓存损坏/断连、`kcctl join` 独立流程；
- 用户/RBAC、Console、模板、DNS、CloudProvider 以及其他扩展能力。
