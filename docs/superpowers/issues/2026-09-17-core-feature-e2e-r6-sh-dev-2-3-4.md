# KubeClipper R6 核心功能实测报告：sh-dev-2/3/4

日期：2026-09-17

状态图例：✅ 已实测通过 · ⚠️ 部分验证或发现收敛缺陷 · ❌ 已复现失败

本轮接续 [R5 三机报告](2026-09-17-core-feature-e2e-r5-sh-dev-2-3-4.md)，继续执行
[核心功能测试清单](../../testing/core-feature-checklist.md) 中可在现有三机环境安全运行的项。
OCI 交付已作为唯一基线，不另设 OCI 模式清单。

## 1. 环境与边界

| 项目 | 实测值 |
|---|---|
| 主机 | `sh-dev-2`（172.16.131.208）、`sh-dev-3`（172.16.131.146）、`sh-dev-4`（172.16.131.230） |
| 平台拓扑 | 3 Server、3 etcd、3 Agent；共享 HTTP OCI Package Registry `aio-img-reg`（172.16.131.146:5003） |
| 集群制品 | Kubernetes `v1.37.0`、containerd `2.2.4`、Calico `v3.31.5`，amd64 |
| 客户端 | `/tmp/kcctl`，`v1.7.0-71+45ca67b86148b2`；server package `v2.0.0` |
| 证据边界 | 运行包沿用 R4/R5 已部署 provenance，证明三机运行行为，不替代当前分支重新构建后的发布验收 |

测试开始前平台为 Healthy。测试结束后再次确认：无 Cluster、无 Operation，3/3 Agent，
`kc-server`/`kc-etcd` Healthy；dev2 上 kubelet 已停止、6443 端口关闭，未留下本轮 Kubernetes
运行文件。

## 2. Delivery Policy 与 OCI 消费

### 2.1 白名单允许与拒绝（2.6-09）

临时策略把 v1.36 的匹配改为 v1.34，保留 v1.35/v1.37。`delivery-policy validate/apply/diff/get`
均成功，创建 `policy-reject-r6-20260917`（v1.36.4）在创建 Operation 前返回：

```text
unsupported k8s version,support [v1.35.8 v1.37.0] now
```

返回码为 1，无 Cluster/Operation 残留；默认策略恢复后的 SHA-256 与备份值一致：
`13a55a29ead6099245498d1fb923b9dde3154a2544253a9de6307ba2427a4d28`。

随后按白名单创建 `policy-allow-r6b-20260917`（dev2 Master、dev3 Worker，v1.37.0/containerd
2.2.4/Calico 3.31.5），CreateCluster Operation
`4a0cd8b7-19ee-49fa-b487-dba8f7a0c925` 成功，集群 Running、两节点 Ready，系统 Pod 和 Calico
均 Running。Cluster `status.packagePlan` 保存了各 slot 的 OCI ref/digest；节点上的 kubeadm
配置和 Calico 配置均指向 `172.16.131.146:5003`。删除集群后平台恢复 Healthy。

结论：白名单内/外路径均得到真实证据，Case 2.6-09 ✅。

### 2.2 缺失 slot 拒绝（2.6-10）

从默认策略副本移除 `cni` slot 后，`delivery-policy validate/apply` 成功；创建
`policy-missing-cni-r6-20260917` 在 Operation 前失败：

```text
UnsupportedComponentSlot: slot "cni" is not declared
```

`get cluster` 返回 NotFound，Operation 列表为空。默认策略已恢复且 diff 无变化。
缺失 repository、内容 blob 和冲突选择尚未单独验证，因此 Case 2.6-10 保持 ⚠️。

### 2.3 集群证书更新（2.3-04）

临时创建 `r6-cert-20260917`（dev2 Master、dev3 Worker）并记录更新前证书：serial
`5278093305A4E031`，有效期 `2026-09-17 09:11:38`～`2027-09-17 09:16:38` UTC。调用
`POST /api/core.kubeclipper.io/v1/clusters/r6-cert-20260917/certification` 后，
UpdateCertifications Operation `4c17aa37-8e17-4df7-929a-91712c493676` 为 Succeeded；更新后 serial
为 `53A1E953A93DDC13`，有效期刷新为 `2026-09-17 09:14:27`～`2027-09-17 09:19:27` UTC，Cluster
回到 Running，两个节点 Ready，16 个系统 Pod 均 Running。删除集群后平台恢复 Healthy。

Cluster 的 `status.certifications` 仍为空，但直接读取 apiserver 证书已证明 serial/有效期确实变化，
Case 2.3-04 ✅；Agent 证书重新签发仍未覆盖。

## 3. HA 故障窗口（1.2-07）

创建 `ha-op-r6-20260917`（dev2 Master、dev3 Worker），在 CreateCluster Operation
`7694e55b-fdd2-4f65-9d59-ebccccaa9bb3` 运行期间停止 dev4 的 `kc-server`。故障窗口内：

- sh-dev-2 的 `kcctl operation list` 和 `kcctl status` 仍可用；
- Operation 未因单 Server 停止而失败，最终成功完成；
- 恢复 dev4 `kc-server` 后三 Server/三 etcd 回到 Healthy，集群节点保持 Ready；
- 删除临时集群后无对象残留。

这补齐了“运行中 Operation + 单 Server 故障”证据；故障窗口内的 Watch 长连接、Console 入口仍未
验证，Case 1.2-07 继续保持 ⚠️。

## 4. 创建参数与负向校验

### 4.1 apiserver 外部发布参数（2.1-12）

创建 `r6-ext-api-20260917` 时传入 `external-ip=172.16.131.208`、`external-port=16443`、
`external-domain=api.r6.example`、`external-domain-port=16443` 和额外 `cert-sans`。Operation
`591658c6-a0f4-4a43-b386-a0686dc70463` 成功，Cluster labels 正确落库；`apiserver.crt` 的 SAN
包含 `api.r6.example` 和 `172.16.131.208`，下载的 kubeconfig 使用
`https://172.16.131.208:6443`。域名端点的 DNS/连通性以及 domain-port 对外代理未验证，Case 2.1-12
保持 ⚠️。集群已删除。

### 4.2 前置拒绝

| 场景 | 结果 |
|---|---|
| `--external-port 70000` | rc=1，提示端口必须在 1～65535；无 Cluster/Operation |
| Master 与 Worker 指定同一 IP | rc=1，提示 `master and worker conflict`；无 Cluster/Operation |
| `--external-domain bad_domain` | rc=1，按 RFC 1123 校验拒绝；无 Cluster/Operation |
| `--external-domain-port 70000` | rc=1，提示端口必须在 1～65535；无 Cluster/Operation |
| `--image-registry does-not-exist` | rc=1，明确列出可用 `aio-img-reg`；无 Cluster/Operation |
| 已占用节点再次建群 | R5 已验证 rc=1 `some nodes in used or disabled`，无新对象，原集群不受影响 |

端口、域名和角色冲突前置路径已覆盖；磁盘、时间同步、主机名等主机预检仍未形成完整矩阵，故
2.1-29 保持 ⚠️。2.1-26 的占用和角色冲突均已通过。

### 4.3 CIDR 重叠缺陷（2.1-28、2.1-30）

使用 `pod-subnet=10.96.0.0/16` 与 `service-subnet=10.96.0.0/12` 创建
`r6-invalid-cidr-20260917`。CLI 返回 rc=0 并创建 Cluster/Operation，未在创建前拒绝；集群进入
Installing，Calico/CoreDNS 健康检查未形成正常完成。随后取消 Operation，取消接口虽返回已请求，
但 Cluster/Operation 一度分别保持 Installing/Running，节点标签和 dev2 上的 kubeadm/Calico 运行
物也未自动清理，常规 `delete cluster` 也因 Installing 状态被拒绝。

为恢复共享测试环境，执行了 `kubeadm reset`、显式清理本次产生的 CNI/iptables/containerd 残留，
并删除 etcd 中本次三个精确孤儿键；没有触碰共享 Package Registry。该过程确认：

- 非法重叠 CIDR 缺少创建前校验；
- cancel 后的 Cluster、Operation、节点占用和主机副作用不能自动收敛；
- 需要增加失败后 retry/安全删除的幂等路径。

这是本轮最重要的新增缺陷，2.1-28 保持 ❌，2.1-30 保持 ⚠️。

## 5. Agent 注销、drain 与独立 join

对空闲 dev4（旧 Node ID `61baec0e-8319-4cb5-8081-49df7cbd119d`）执行：

```text
kcctl drain --agent 61baec0e-8319-4cb5-8081-49df7cbd119d
```

命令 rc=0，随后 `get node` 为 NotFound。使用共享 Package Registry（HTTP）执行独立
`kcctl join --agent 172.16.131.230`，预检和 bootstrap asset 刷新成功；新 Node ID
`0d34b764-711d-4899-89e1-bfb96569ae53` Ready，平台恢复 3/3 Agent。

这证明了空闲节点的注销/重新纳管主路径。当前 `drain` help 明确只支持 KubeClipper Agent 节点，
并非 Kubernetes Pod 驱逐；used/force、Lease/证书残留和重复 join 尚未覆盖。因此：

- 1.3-05、3-04：成功路径已跑通，但认证/CA、重复和失败清理仍缺，保持 ⚠️；
- 2.2-07：空闲节点路径部分通过，保持 ⚠️；
- 3-15：应按 Agent drain 重新描述，不能声称支持 PDB/Pod eviction，保持 ⚠️。

## 6. 平台自省、审计与指标

使用管理员 mTLS 客户端直接访问：

| Endpoint | 结果 |
|---|---|
| `/api/config.kubeclipper.io/v1/configz` | 200 |
| `/api/config.kubeclipper.io/v1/status` | 200，返回 Healthy |
| `/api/config.kubeclipper.io/v1/components` | 200 |
| `/api/config.kubeclipper.io/v1/componentmeta` | 200 |
| `/api/audit.kubeclipper.io/v1/events?limit=1&page=1` | 200；事件详情 200，不存在事件 404 |
| `/healthz` | 200，正文 `ok` |
| `/metrics` | 200，108 行；未命中 password/token/secret/private-key 等敏感字段名 |

因此 4-13、4-14 可标记 ✅；4-17 仅完成可抓取和基础脱敏检查，指标标签约束仍需专项验证，
保持 ⚠️。

## 7. `kcctl` Registry 与身份路径补测

- `registry list --type repository` 成功列出共享 Registry 的 35 个仓库；`--type image --name
  kube-apiserver` 成功列出版本标签；不存在仓库返回预期 `NAME_UNKNOWN`。
- `registry push` 使用非法归档返回 rc=1、`load manifest: unexpected EOF`。三机没有运行 Docker
  Engine，且不能占用共享 Registry，因此未执行 valid push、独立 registry deploy/clean/delete；
  3-19 保持 ⚠️。
- Delivery Policy 另做缺失 repository 负向：将 v1.37 策略的 `calico` 替换为不存在的
  `missing-calico` 后，`validate/apply` 均成功，创建请求在 Operation 前返回
  `k8s version v1.37.0 unavailable, missing packages: missing-calico v3.31.5`，rc=1，
  无 Cluster/Operation；默认策略 SHA-256 恢复为
  `13a55a29ead6099245498d1fb923b9dde3154a2544253a9de6307ba2427a4d28`。缺失 blob 和冲突选择
  仍未覆盖，2.6-10 保持 ⚠️。
- 临时 registry `r6-cri-reg-2-20260917` 与镜像 Registry `aio-img-reg` 分别用于同一建群
  `r6-reg-separation-2-20260917`；CreateCluster Operation
  `92fcb071-b324-4c99-b452-3d3b48ce2e6d` 成功，Cluster 同时保留 `imageRegistry` 和 CRI
  `registryRef`，dev2 的 containerd 生成对应 HTTP `hosts.toml`。两资源当前同端点，不能替代
  不同端点、认证和 CA 的隔离验证。
- API 直接创建 `r6-iptables-api-20260917`，dry-run/真实 POST 均为 200；CreateCluster Operation
  `a16b1a64-02cb-46ce-bdf1-f2a6ff46df87` 成功，Cluster `networking.proxyMode=iptables`，
  `/etc/kubernetes` 中 kube-proxy ConfigMap 的 `mode: iptables`，删除后无残留，2.1-09 可标记 ✅。
- 使用非法 `--cri docker --cri-version 20.10.24` 创建请求返回 rc=1、
  `unsupported cri version,support [] now`，无 Cluster/Operation。该结果只证明 OCI 矩阵拒绝
  Docker，不恢复 Docker CRI；help 和参数校验仍暴露废弃入口。
- `kcctl get` 已扫过 cluster/node/user/role/configmap/registry 的 singular list、JSON 形状、Node
  label selector 和 User field selector；User label selector 未按预期过滤，且 registry/user 的
  JSON 输出存在单对象与列表形状差异，3-09 继续保持 ⚠️。
- 临时 role/user 的 CLI CRUD、重复 user 名称拒绝和正确密码登录均通过；但自定义 role 登录后
  `get cluster`、`get node` 仍为 403，说明 role annotation 未产生预期授权，4-08b 记录为问题。
- `kcctl upgrade all --pkg /tmp/kc-r6-no-such-upgrade-package.tar.gz` 在本地返回 rc=1，平台
  仍 Healthy；这只覆盖无效包拒绝，不替代平台升级 E2E。`kcctl deploy config` 生成成功，非法
  YAML 在解析阶段 rc=1；顶层 `kcctl config` 命令不存在。
- `kcctl completion bash/zsh` 均生成成功并通过 shell 语法检查，`fish` 按 help 返回不支持；
  清单已按实际支持面改为 bash/zsh。另查得 `/root/.kc/config` 和 `deploy-config.yaml` 权限均为
  0644，Package Registry/mTLS 敏感配置未达到 0600，2.6-07/4-16 保持 ❌。
- 内置只读用户登录后可读取 Cluster，创建 Registry 被 403 拒绝，证明内置 RBAC 拦截生效；完整
  CRUD、验证码、token、限流和自定义 role 修复仍是缺口。

## 8. 最终状态与未执行边界

最终确认：

```text
KubeClipper Platform Status: Healthy
kc-server  Healthy
kc-etcd    Healthy
kc-agent   Healthy 3/3
Cluster    none
Operation  none
```

以下项目本轮没有伪装为通过：纯离线 bundle/export-import、HTTPS/公共 CA/认证 Package Registry、
平台自身 `upgrade`、Master 增删和角色转换、Agent 证书重新签发、升级中断 retry、
缓存损坏/Registry 断连、maxBackupNum 孤儿文件修复、Console E2E，以及模板/DNS/CloudProvider
等扩展能力。Docker CRI 仍按废弃入口处理，不安排 Docker E2E。
