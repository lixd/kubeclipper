# 纯 OCI 发布就绪报告（2026-07-14）

> 2026-07-16 复审结论：本文记录的 `cf2f967` 资格测试仍然是有效的历史证据，
> 但其验证范围是未启用认证的 HTTP Distribution Registry。该提交不再视为最终
> 2.0 发布提交。当前仍有 package/image Registry 边界、生产级 package Registry
> 认证与 TLS、最新 master/Registry API 变更集成以及最终提交重验等发布阻断项，
> 因此当前结论调整为：**暂不建议正式发布**。

## 发布候选版本

- 分支：`codex/oci-static-server-replacement`
- 发布提交：`cf2f967e6ddbf2b81354096b88216c44d253021c`
- 验证标签：`oci-qualification-20260715-cf2f967`
- 测试主机：`sh-dev-3`（`172.16.131.146`）和 `sh-dev-2`（`172.16.131.208`）
- 软件包及运行时镜像 Registry：`sh-dev-3:5000` 上由宿主机原生运行的 Distribution Registry
- Kubernetes 技术栈：Kubernetes `v1.35.0`、containerd `1.7.29`、Calico `v3.29.6`

发布候选版本在干净的 detached worktree 中构建。`kcctl`、`kubeclipper-server`
及 OCI bootstrap 软件包均包含准确一致的发布提交信息。

这次资格测试证明了匿名 HTTP Registry 下纯 OCI 发布、部署和双机生命周期流程的
可行性，不代表私有 Harbor、Registry 认证、自定义 CA 或严格 TLS 校验已经通过
生产发布验证。

## 2026-07-16 发布复审

### 最终发布提交尚未形成

- 本次复审的分支基线为 `6485480c75ced9bf212fa460556fceed32f8db2b`，相对
  `cf2f967` 仅新增了上一版资格报告提交。
- 当前 OCI 分支相对最新本地 `master` 少 5 个提交、多 39 个提交。尚未合入的
  master 变更包括 Actions 运行时升级和 OSS 下载地址修复。
- `localRegistry`/`insecureRegistry` 参数及 API 模型调整正在独立 PR 中处理；该
  变更完成后仍需集成到 OCI 分支。
- 本文列出的 GitHub Actions 和双机生命周期结果对应 `cf2f967`，不能替代未来
  最终 release commit 的重新验证。

### package Registry 与 image Registry 仍存在混用

当前 `pkg/apis/core/v1/delivery_source.go` 会优先使用集群的 `LocalRegistry` 作为
OCI package inventory 来源。这会在 image Registry 和 package Registry 使用不同
地址时，从 Kubernetes 镜像仓库错误查找 kubeadm、containerd、Chart 等 OCI package。

复审期间已形成一个本地修复并通过以下 race 测试：

```text
go test -race ./pkg/apis/core/v1 ./pkg/apis/config/v1
```

该修复将 package inventory 来源限定为 `deployConfig.packageRegistry`，并增加了
image Registry 与 package Registry 地址不同的回归覆盖。为避免与正在开发的
Registry API PR 产生不必要冲突，源码修改已单独保存，未包含在本次文档提交中；
待该 PR 完成后统一整合和复测。

### 生产级 package Registry 认证与 TLS 尚未闭环

当前 package inventory 和 OCI package 拉取路径仍普遍硬编码使用
`crane.Insecure`，现有 qualification 没有覆盖私有 Harbor。正式发布前至少需要：

1. kcctl 支持 package Registry 的用户名、密码/令牌、自定义 CA 和 TLS 策略。
2. deploy 将凭据安全下发给 server 和 AIO agent，join 将凭据下发给新 agent。
3. server 能认证读取 package inventory，agent 能认证拉取 digest-pinned package。
4. 凭据不能进入普通 ConfigMap、日志、resolved delivery plan 或命令行明文记录。
5. `kcctl clean` 必须删除两台机器上的测试凭据、CA 和 Registry 配置。
6. 使用 Harbor robot account 和最小权限策略验证 bootstrap、建群、增删节点、
   删除集群和 clean 全生命周期。

## 已关闭的阻断项

1. join 节点的 agent 清理
   - 通过 API 执行 `kcctl clean` 时，会合并在线节点清单。
   - join 现在会在安装 agent 前持久化计划清理的节点清单，因此客户端即使中断，也不会留下未被追踪的 agent。
   - join 使用的 SSH 传输配置与 server 的 SSH 传输配置分别持久化。AIO agent 使用 server SSH，join 加入的 agent 使用 agent SSH。
   - 当存在可复用的密钥路径时，不会持久化缓存的私钥内容。
   - 远程清理失败会作为错误返回，不再输出误导性的成功信息。
2. 目标节点架构
   - 通过可注入的 SSH runner 分别检测每个目标节点的 bootstrap 架构。
   - 单元测试覆盖 amd64、arm64 以及混合架构节点组。
3. join 传输配置隔离
   - `serverSSH` 和 `--server-ssh-*` 独立于 agent 的 `ssh`/`--pk-file`。
   - 双机测试使用了两把不同的密钥：server 密钥仅授权给 `sh-dev-3`，agent 密钥仅授权给 `sh-dev-2`。
4. 多网卡安全性
   - `first-found` 会排除 Docker、CNI、Podman 和 nerdctl 的 bridge 接口模式。
   - 发布验证中，管理地址和节点地址均显式使用 `interface=ens3`。
5. 发布产物与支持策略一致性
   - `tools/release-policy-verify` 执行双向校验：支持策略中声明的每个默认版本，都必须为受支持架构发布；每个已发布的 release 组件，也都必须存在于支持策略中。
6. CI 运行环境弃用问题
   - 工作流使用 `actions/checkout@v6`、`actions/setup-go@v6`、`azure/setup-helm@v5` 和 `docker/login-action@v4`。
7. OCI 来源追溯
   - 软件包 config label 和 release manifest 条目均包含 `sourceRevision`。
   - manifest 生成流程会在提升产物前校验平台 bootstrap 的 revision。

## 本地验证

在发布分支上，以下验证均已通过：

```text
go test -race（clean、join、deploy、create、delivery API/publisher、
              autodetection、release-policy verifier）
go vet ./...
golangci-lint run ./...
actionlint
shellcheck（本次修改涉及的 release/open-packaging 脚本）
scripts/open-packaging/tests/*.sh
bash -n（release/open-packaging 脚本）
tools/release-policy-verify
Linux amd64 和 arm64 构建：kcctl、kubeclipper-server、kubeclipper-agent
release manifest/digest/sourceRevision 校验
离线 Registry bundle 导出/导入校验
git diff --check
```

本地还执行了全仓 `go test ./...`。仅有的本地失败均由宿主环境所致，且已有成功的 Linux Action 作为权威验证结果：

- macOS 不提供 `pkg/utils/systemctl` 所需的 systemd/dbus；
- 桌面沙箱阻止了 `pkg/utils/sysutil` 使用的宿主机信息调用；
- 本地 E2E 需要可用的 `~/.kc/config`。

## 发布提交对应的 GitHub Actions

- Go 测试/覆盖率：[run 29388107973](https://github.com/lixd/kubeclipper/actions/runs/29388107973) — 成功
- 离线资源/来源追溯校验：[run 29388107992](https://github.com/lixd/kubeclipper/actions/runs/29388107992) — 成功
- Registry 原生 AIO 部署及快速 E2E：[run 29388107991](https://github.com/lixd/kubeclipper/actions/runs/29388107991) — 成功
- 全部 16 个组件发布及 release manifest：[run 29388108045](https://github.com/lixd/kubeclipper/actions/runs/29388108045) — 成功（16 个发布 job 及 manifest job 全部通过）

AIO 工作流将 bootstrap kubeclipper/etcd/console 发布到本地 Registry，
从 OCI 完成部署，验证登录及快速 E2E（包括 Web Terminal SSH），并要求
`kcctl clean` 执行成功。

manifest job 校验了 12 个发布产物，失败数为零。验证证据如下：

```text
release-manifest.yaml sha256: d051ca7c362d9490deb2efe1cdc901df2fccd7583f4c247cb05ef63fbd10ae2e
metadata.sourceRevision: cf2f967e6ddbf2b81354096b88216c44d253021c
bootstrap/kubeclipper OCI digest: sha256:39bdc402f35dd2ac440556f11fa83fb5ca3f650e6cda45002d162fe8a2b9e7d1
已验证产物：12；失败：0
```

## OCI 证据

隔离的双机 bootstrap 产物如下：

```text
ref: 172.16.131.146:5000/kubeclipper/packages/bootstrap/kubeclipper:e2e-cf2f967-20260715
index digest: sha256:6be035f206ae36fa645c5850620ce86dd8a9bde199c84759d49f35c1ffc55315
sourceRevision: cf2f967e6ddbf2b81354096b88216c44d253021c
```

验证完成后已删除该标签；宿主机 Registry 中仅保留原有的 `v1.8.0` 标签。

## 双机纯 OCI 生命周期验证

1. 使用 OCI 在 `sh-dev-3` 部署平台，并显式指定 `ens3` 进行地址检测。
2. 登录成功。客户端与服务端报告的 Git commit 均为 `cf2f967e6ddbf2b81354096b88216c44d253021c`，且工作树状态均为 clean。
3. 使用相互独立的 server/agent SSH 密钥，从 OCI bootstrap 软件包将 `sh-dev-2` 加入平台。两个平台节点均达到 Ready 状态。部署配置中未出现私钥正文。
4. 仅使用 Registry 中的软件包/镜像创建 `e2e-cf2f967`：Kubernetes `v1.35.0`、containerd `1.7.29`、Calico `v3.29.6`。集群达到 `Running` 状态；所有 control-plane 和系统 Pod 均达到 Running/Ready 状态。
5. 通过 `cluster add-node` 添加 `sh-dev-2`。两个 Kubernetes 节点均为 Ready，并报告 containerd 版本为 `1.7.29`。
6. 通过 `cluster remove-node` 移除 `sh-dev-2`。集群恢复为仅包含 `sh-dev-3` 的 Running 状态；`sh-dev-2` 上的 kubelet/containerd 均处于 inactive 状态。
7. 通过正常 API 流程完成集群删除。
8. 一次执行 `kcctl clean --all --force --assumeyes` 即成功完成清理，同时清理了 AIO server/agent 和 join 加入的 agent，无需人工补救。

首次发布验证有意暴露并随后修复了两个清理边界问题（join 中断时的节点清单，以及 AIO server/agent 的传输配置选择）。上述完整生命周期是基于当时的资格测试提交执行的干净重测结果。由于最终 2.0 release commit 尚未形成，合并 master、Registry API 和认证/TLS 改造后必须重新执行。

## 最终清理证据

最终 clean 成功后，两台主机上的下列项目均已不存在或处于 inactive 状态：`kc-server`、`kc-agent`、`kc-etcd`、`kc-console`、`kubelet`、Kubernetes static-pod 进程、KubeClipper server/agent 配置目录和二进制文件、`/etc/kubernetes`、`/var/lib/kubelet`、`/var/lib/etcd`、`/etc/containerd` 以及 `/var/lib/containerd`。

临时 SSH 授权和密钥、Registry 标签、发布验证 Git 标签、源码副本、二进制文件及软件包归档均已删除。所有 216 个带临时 `qualification-*` 前缀的 GHCR 软件包均已通过已登录的软件包页面删除；重新扫描九页软件包列表后，确认剩余验证软件包数量为零。宿主机原生的 `kc-registry.service` 保持运行。
未修改无关的 `sprout-postgres-v2` 容器。

## 剩余缺口

- P0：OCI package resolver 仍会把集群 image Registry 当作 package Registry；本地修复已通过定向 race 测试，但尚待 Registry API PR 完成后整合。
- P0：生产 package Registry 的认证、自定义 CA、严格 TLS 校验及 server/agent 凭据安全下发尚未实现和验证。
- P0：最终 release commit 尚未形成。最新 master 和 Registry API/kcctl/前端变更合入后，必须重新运行全部必需 Actions 和双机纯 OCI 生命周期。
- P1：需要补充 v2.0 Registry 模型、旧配置升级影响、凭据轮换和 Harbor 最小权限 support policy 文档。
- P2：测试主机未运行 chronyd/ntpd；现有部署预检已明确报告此情况。由于实测主机时钟偏差小于一秒，验证继续执行。
- P2：删除 manifest 后回收 Distribution Registry blob，仍属于 Registry 运维方的常规垃圾回收职责。

## 发布建议

`cf2f967` 已证明匿名 HTTP Registry 下的纯 OCI 主流程可以工作，相关 Actions、双机生命周期和清理证据仍然有效。但是生产级私有 Registry 支持和最终提交资格验证尚未完成，且最终代码还依赖最新 master 与 Registry API PR 的集成。

当前发布结论：**暂不建议正式发布**。

重新达到“可以正式发布”至少需要：

1. 合入 Registry API/kcctl/前端变更并关闭 package/image Registry 混用。
2. 完成 package Registry 认证、CA/TLS 和凭据安全下发/清理。
3. 在最终 release commit 上重新执行全仓验证、全部必需 GitHub Actions、双架构构建、manifest/provenance/bundle 校验。
4. 使用相互独立的认证 TLS package Registry 和 image Registry 重跑双机纯 OCI 部署、join、建群、增删节点、删除和 clean，确认无需人工补救且无测试残留。
