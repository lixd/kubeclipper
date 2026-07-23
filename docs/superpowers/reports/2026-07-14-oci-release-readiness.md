# 纯 OCI 发布就绪报告（2026-07-24）

> **当前结论：暂不建议正式发布。** 当前分支的本地验证和双机 HTTP OCI 生命周期已经完成，
> 但用户明确要求不推送远端，因此没有当前 release commit 的真实 GitHub Actions；本轮也没有
> 在正式 Harbor 上重跑生产 robot/TLS/最小权限组合。发布门禁尚未全部满足。

## 2026-07-24 当前 HEAD 复审

- 分支：`codex/oci-static-server-replacement`。
- 代码候选 revision：`b44d9b6dc3f2ad78dff614c62c9bb6a098dab64d`；本地 `master` 已包含在该提交中。
  本报告后续提交只修改文档，不改变二进制代码；工作区 clean，未推送、未强推。
- `packageRegistry` 仅用于 OCI artifact；`--image-registry` 选择 Kubernetes/CNI 镜像来源；
  `--cri-registry` 选择额外 containerd Registry；镜像侧两个资源均写入 containerd 配置。

### 当前 HEAD 本地验证

通过：定向 `go test -race`（delivery、deploy/join/clean/resource、SSH、API、kubeadm）、`go vet ./...`、
排除环境依赖包后的全仓 Go 测试、`golangci-lint run ./...`（0 issues）、Bash 语法检查、两个
open-packaging 测试、`release-policy-verify`、`git diff --check`，以及 Linux amd64/arm64 的
kcctl/server/agent 静态构建。全仓 `go test ./...` 仅有环境失败：macOS 无 system D-Bus 导致
`pkg/utils/systemctl` 失败；本机无 `~/.kc/config` 和已部署平台导致 `test/e2e` 的 17 个 setup 失败。
ShellCheck 0.11.0（`-x -P SCRIPTDIR`）和 Actionlint 1.7.7 均通过。

上述代码 revision 的隔离 package 工件（测试 tag 已删除）为：

```text
sourceRevision: b44d9b6dc3f2ad78dff614c62c9bb6a098dab64d
ref: 172.16.131.146:5000/qualification-b44d9b6/kubeclipper/packages/bootstrap/kubeclipper:v2.0.0-qualification-b44d9b6
package digest: sha256:c1c766269611d253ec95f2a7241b488ec4bdd5a8f3801153ae6762236a900ba7
kubeclipper-agent layer: sha256:3d8742b8a3aa236b6d0279abcc80c504423c259553a760466f8fe57e7d0ce62e
kubeclipper-server layer: sha256:b510f7fccc7beb1b9dcd4eaeaa62a94025684415ca71a907e28bd541726f394f
```

### 双机纯 OCI 生命周期

测试主机为 sh-dev-3（`172.16.131.146`）和 sh-dev-2（`172.16.131.208`），使用隔离前缀
`qualification-b44d9b6`。本轮采用 HTTP 测试 Registry，不是正式 Harbor 认证 TLS 证据。

1. 当前 HEAD 的 Linux amd64 kcctl/server/agent 完成 AIO deploy，登录成功，packageRegistry 指向隔离 OCI 前缀。
2. 使用独立 server SSH key 和 agent SSH key 完成 sh-dev-2 join；节点注册为 amd64、`ens3` 地址并 Ready。
3. 创建单 master 集群时未传 `--untaint-master`。API 持久化 `untaintMaster: true`，操作日志生成
   `master-` 和 `control-plane-` 两条幂等 taint 删除命令；控制面、CoreDNS、Calico 从 image/CRI Registry 拉取并 Ready。
4. `kcctl cluster add-node` 添加 sh-dev-2 后节点 Ready；使用隔离标签命名空间和
   `nicolaka/netshoot:v0.13` 在 worker 上运行 workload，验证调度和 containerd 拉取。
5. 删除 workload 后执行 `kcctl cluster remove-node`；sh-dev-2 的 kubelet/containerd 停止，kc-agent 保留。
6. 删除集群后 sh-dev-3 的 kubelet/containerd 停止，Kubernetes、etcd、CNI、containerd 配置路径均不存在。
7. 执行 `kcctl clean --all --force --assumeyes` 后，两台机器的 KubeClipper/Kubernetes/runtime 服务和配置均清理。

建群中的可选 `applyKubectlPod` 步骤因固定 10 秒超时、且标记 `errIgnore: true`，在 API server 尚未稳定时失败；
核心控制面和 CNI Ready，但不能据此声称 `kc-kubectl` 辅助 Pod 已部署。

### 清理和剩余缺口

- 两台机器 authorized_keys 中的测试公钥、临时私钥、CA、密码、二进制和日志均已删除；KubeClipper 配置路径不存在。
- 当前 qualification 前缀的 Registry tags 已逐一按 digest 删除，查询结果为 `tags: null`；catalog 名称和未引用 blob
  仍需运维窗口执行垃圾回收，未对共享 Registry 执行 GC。
- 未停止或修改无关 Docker、`kc-registry.service`、`sprout-postgres-v2` 等服务。
- **P0：** 没有当前 HEAD 的 GitHub Actions run。Go tests/coverage、offline-resource-validate、bootstrap、resource package、
  release manifest/provenance、OCI AIO 等真实 workflow 尚未执行，原因是本轮明确禁止推送。
- **P0：** 正式 Harbor robot account、正式 CA/TLS、最小权限和凭据轮换尚未在当前 HEAD 重跑；历史认证证据不能替代当前 HEAD。
- **P1：** `kc-kubectl` 的 10 秒 apply 超时和 `errIgnore` 语义需要单独修复或明确为非发布门禁。
- **P2：** Registry 未引用 blob 的 GC 由运维负责。

因此当前结论是：**本地代码和匿名 HTTP 双机生命周期通过，但尚不满足“可以正式发布”的完整门禁，不建议现在发布 2.0.0。**
授权推送后，应在该精确 commit 上等待必需 Actions 全绿，再用正式 Harbor 重跑同一套生命周期和残留审计。

## 历史复审（2026-07-23 及更早）

- 分支：`codex/oci-static-server-replacement`。
- 双机资格代码提交：`2224c551862b0c087fe8ce71533568581c6f383a`；当前 HEAD 为
  `b1991d0`，在该提交之后增加了单节点默认取消 control-plane taint 的 CLI 行为。
  因此下方双机 OCI digest 和生命周期证据严格对应 `2224c55`，不能替代当前 HEAD
  重新生成工件后的资格验证。
- 关键提交：`8262a7d`（认证 package Registry）、`30c7a69`（运行时/CNI 清理）、
  `fc97039`（控制面 finalizer）、`2224c55`（CRI 卸载后执行控制面 finalizer）。
- 工作区在验证结束时 clean；没有推送、强推或覆盖已有 stash。

### 已关闭阻断项

1. `kcctl clean` 追踪并清理所有通过 join 加入的 agent，删除 server/agent 的凭据和 CA。
2. 架构检测支持可注入 SSH runner，覆盖 amd64、arm64 和混合架构测试。
3. join 的 agent SSH 与读取 server 证书的 server SSH 独立；双机使用两把不同密钥验证。
4. 多网卡 `first-found` 排除 Docker/CNI/Podman/nerdctl bridge；实机使用 `ens3`。
5. `tools/release-policy-verify` 对发布产物和 support policy 做双向一致性校验。
6. GitHub Actions 已使用升级后的 action 版本，消除 Node.js 20 runtime 弃用配置。
7. package/image Registry 模型已分离：`packageRegistry` 只负责 OCI artifact；
   `--image-registry` 选择 Kubernetes/CNI 镜像来源；`--cri-registry` 选择写入
   containerd 的其他镜像来源；两类镜像 Registry 都写入 containerd 配置。
8. 单 master 且无 worker 的集群会默认取消 control-plane taint，使单节点集群能够
   调度普通工作负载；单 master 带 worker、多 master 拓扑仍保留 taint，显式
   `--untaint-master` 保持兼容。该行为由 `b1991d0` 实现并通过 CLI race 单元测试。

### 认证 TLS Registry 与 provenance

隔离 package Registry 为 `https://172.16.131.146:5001`（Basic Auth 用户 `kcrobot`、自签名
CA），隔离 image Registry 为 `https://172.16.131.146:5002`（用户 `kcimage`、独立 CA）。
deploy/join 使用 CA、用户名和密码文件，server/agent package 配置权限实测为 `0600`；
错误凭据和未知 CA 会被拒绝。image Registry 资源 `qualification-2224c55-images` 的
scheme 为 `https`、CA/auth 均存在、skip TLS verify 为 false；Kubernetes、Calico 和
`kubeclipper/kubectl` 均实际由 containerd 从该 Registry 拉取。

```text
sourceRevision: 2224c551862b0c087fe8ce71533568581c6f383a
ref: 172.16.131.146:5001/kubeclipper/packages/bootstrap/kubeclipper:v2.0.0-qualification-2224c55
digest: sha256:50b6ae687067d9a2b2b81df486e3926a7dce4aff3c19b2ba6a762a67f2dac5f0
kubeclipper-agent: sha256:01b1228c3d052634a984efd19a48a34e07fbb701d1a61fefe6998faf921b84d2
kubeclipper-server: sha256:e36725bb57c17f158ea96132907b74c01d9398941e62f54463e5a021dff7c80b
```

Registry 测试服务、标签和临时文件已在验证后删除；以上 digest 是发布时记录的不可变证据。

### 双机纯 OCI 生命周期

测试主机为 sh-dev-3（`172.16.131.146`）和 sh-dev-2（`172.16.131.208`），测试对象均使用
`qualification-2224c55` 前缀。

1. 使用当前 commit 的 Linux amd64 kcctl/server/agent，从认证 HTTPS package Registry 完成 deploy；server/agent commit 和 sourceRevision 一致且 tree clean。
2. 用独立 server/agent SSH 配置完成 sh-dev-2 join；package 配置权限为 `0600`。
3. 使用 Kubernetes `v1.35.0`、containerd `1.7.29`、Calico `v3.29.6` 建立纯 OCI 集群；image Registry 和 cri Registry 均选用 `qualification-2224c55-images`，集群和系统 Pod 达到 Running/Ready。
4. `kcctl cluster add-node` 后 sh-dev-2 Ready；containerd 成功拉取认证 image Registry 中的 Calico 和 `kc-kubectl` 镜像。
5. `kcctl cluster remove-node` 后 sh-dev-2 的 kubelet/containerd inactive、kc-agent 保留；Kubernetes、containerd、CNI、Calico 配置和数据路径均不存在，且无 `cali*`/`vxlan.calico` 设备。
6. 删除集群后 sh-dev-3 的 kubelet/containerd inactive，Kubernetes、containerd、CNI、Calico、etcd 路径全部不存在；`2224c55` 的 finalizer 已在 CRI 卸载之后执行，未再出现 image-volume `Device or resource busy`。
7. 执行 `kcctl clean --all --force --assumeyes` 后两台机器的 kc-server、kc-agent、kc-etcd、kc-console、kubelet、containerd 均 inactive，平台配置和测试凭据均不存在。

### 清理与保留项

- 两套隔离 Registry systemd 服务已停止、禁用并删除临时目录；两台机器 authorized_keys 中的测试公钥、临时 CA、密码、私钥、二进制和归档均已删除。
- `kc-registry.service` 保持 active；`sprout-postgres-v2` 保持运行；未触碰无关服务或容器。

### 本地验证与剩余缺口

通过：定向 race 测试、`go vet ./...`、`golangci-lint run ./...`（0 issues）、两个 open-packaging 测试、open-packaging source check、Bash 语法检查、`go run ./tools/release-policy-verify --manifest packaging/resources.yaml`、Actionlint、Linux amd64/arm64 静态构建和 `git diff --check`。

全仓 `go test ./...` 的环境相关失败已单独区分：macOS 无 system D-Bus 导致 `pkg/utils/systemctl` 失败；本机无 `~/.kc/config` 和已部署平台，`test/e2e` 的 17 个 setup 失败。排除这两个明确环境依赖包后，其余 `go list ./... | rg -v 'pkg/utils/systemctl|test/e2e' | xargs go test` 全部通过。ShellCheck 因当前 macOS 预发布环境没有可用 bottle，源码构建其依赖 `pkg-config` 时又遇到旧 GLib/Clang 编译错误，未记为通过。

资格代码提交及其后的报告提交都没有 GitHub Actions run 链接，因为本轮明确不推送远端；Go tests/coverage、offline-resource-validate、bootstrap、resource package、release manifest/provenance 和 OCI AIO 的当前分支证据仍缺。正式 Harbor 的生产 robot account、最小权限项目、正式 CA/TLS 组合也尚未重跑。

单节点默认自动取消 taint 的代码缺口已关闭；但本轮双机资格工件来自 `2224c55`，
建群命令也显式使用了 `--untaint-master`。正式发布前仍需基于当前 HEAD 重新生成带
`sourceRevision` 的 OCI 工件，并在不传 `--untaint-master` 的情况下验证单节点系统 Pod
和普通工作负载均能正常调度。剩余 P2：测试主机未运行 chronyd/ntpd（时钟偏差实测
小于一秒），以及 Registry blob 垃圾回收由运维方负责。

### 发布建议

本地代码、认证 TLS package/image Registry 分离、双机 deploy/join/create/add/remove/delete/clean 和残留清理均已通过；在没有当前 release commit 的 Actions 与正式 Harbor 权限策略证据前，不能宣称正式发布就绪。建议授权推送后在该 commit 上完成所有 Actions，并用正式 Harbor 重跑同一套双机证据，再决定发布 2.0.0。

## 历史记录（2026-07-22 及更早）

以下内容保留旧提交的审查和 Actions 证据，不代表当前 `2224c55` 的发布状态。

### 2026-07-22 复审

### 最新 master Registry 模型已完成集成

- 当前已提交基线：`f248da4a8334a0c1238d8207c0682b83162e2f0f`。
- 合入的 master 提交：`8371495`，包含 Registry API、kcctl 和前端的统一调整。
- `--image-registry <Registry resource>` 选择 Kubernetes 控制面/CNI 镜像来源；
  `--cri-registry <Registry resource>[,...]` 配置其他 containerd 镜像来源。
- `deployConfig.packageRegistry` 仅解析 KubeClipper OCI package 和 Helm Chart。
  即使两类 Registry 位于同一个 Harbor，它们仍按独立资源、路径和凭据处理。
- 旧的 `--local-registry` 和镜像侧 `--insecure-registry` 已删除，不再参与新模型。

### package Registry 认证与 TLS 本地实现

当前未提交实现提供统一 Registry 客户端配置，覆盖 indexer、fetcher、运行时 Helm
OCI Chart、发布工具、验证工具以及 `kcctl resource`：

1. 默认严格 HTTPS；HTTP 必须通过 `--package-registry-scheme http` 显式启用。
2. 支持用户名/密码、Harbor robot account、自定义 CA 和显式跳过 TLS 校验。
3. 密码只能通过文件输入，不提供明文密码参数；本地及远端配置权限为 `0600`。
4. deploy 在服务启动前向全部 server/agent 安全下发配置；配置不会进入 deploy
   ConfigMap、delivery plan 或普通日志。
5. join 默认通过独立的 server SSH 继承现有凭据，再通过 agent SSH 下发给新节点。
6. 显式切换 Registry 或轮换认证/TLS 时，会更新全部现有 server/agent；任一节点
   更新失败或最终 deploy ConfigMap 提交失败，均反向恢复已经修改的节点。
7. clean 删除 `/etc/kubeclipper-server` 和 `/etc/kubeclipper-agent`，同时删除其中的
   Registry 凭据与 CA。
8. Registry 配置绑定完整 host/project prefix，拒绝向其他 Registry 或项目发送凭据。

本地集成测试使用带 Basic Auth 的自签名 TLS Registry 验证了正确 robot 凭据与
自定义 CA、错误凭据拒绝、未知 CA 的严格 TLS 拒绝，以及显式 HTTP；还完成了
amd64/arm64 manifest index 合并和 Helm OCI Chart 按 digest 发布/拉取往返。

### 2026-07-22 本地验证结果

通过：

```text
go test -race ./pkg/delivery/registry ./pkg/delivery/indexer \
  ./pkg/delivery/fetcher ./pkg/delivery/publisher ./pkg/component/common \
  ./pkg/cli/deploy ./pkg/cli/join ./pkg/cli/clean ./pkg/cli/resource \
  ./pkg/utils/sshutils ./pkg/apis/core/v1 ./pkg/clustermanage/kubeadm
go vet ./...
golangci-lint run ./...                         # 0 issues
bash scripts/open-packaging/tests/generate-release-manifest-provenance-test.sh
bash scripts/open-packaging/tests/export-offline-registry-bundle-test.sh
go run ./tools/release-policy-verify
bash -n scripts/open-packaging/*.sh hack/*.sh
git diff --check
GOOS=linux GOARCH=amd64/arm64 CGO_ENABLED=0 go build：
  kcctl、kubeclipper-server、kubeclipper-agent
```

6 个交叉编译产物均经 `file` 确认为静态 Linux ELF；amd64 为 x86-64，arm64 为
aarch64。Go 在构建结束时尝试写入只读的全局 module stat cache，产生非致命警告，
不影响构建退出状态或产物格式。

`go test ./...` 的普通 Go 包全部通过，命令整体仅在以下环境依赖处失败：

- `pkg/utils/systemctl`：macOS 没有 systemd/dbus；
- `pkg/utils/sysutil`：桌面沙箱拒绝读取宿主系统信息；
- `test/e2e`：本机没有已部署平台及 `~/.kc/config`。

本机没有 `actionlint`、`shellcheck`、`skopeo`、Docker 和 `yq`。因此本轮不能把
Actionlint、ShellCheck 或依赖 Skopeo 的真实导入验证记为本地通过；必须由最终提交的
Linux Actions 和后续 Harbor 资格测试补齐。

### 当前证据边界

- 下文 `cf2f967` 的 Actions、digest、sourceRevision 和双机生命周期结果属于历史
  匿名 HTTP 资格测试，不能作为当前实现的最终发布证据。
- 当前未推送，尚无当前最终提交的 Actions run 链接。
- 当前实现尚未发布到隔离 Harbor 标签，因此没有可记录的最终 OCI digest 和
  sourceRevision 对。
- 当前尚未在 `sh-dev-3`、`sh-dev-2` 使用 Harbor robot account、自定义 CA 和严格
  TLS 重跑 deploy、join、建群、增删节点、删除、clean 以及残留审计。

### 历史发布候选版本

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

### 2026-07-16 发布复审

> 本节保留当时的审查背景和 `cf2f967` 历史证据。关于 package/image Registry
> 边界、`--local-registry` 参数和 package Registry 认证/TLS 的结论，均已由
> 2026-07-22 的“最新复审”更新；请勿将本节中的“尚未实现”表述视为当前状态。

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

### 历史已关闭阻断项

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

### 历史本地验证

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

### 历史发布提交对应的 GitHub Actions

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

### 历史 OCI 证据

隔离的双机 bootstrap 产物如下：

```text
ref: 172.16.131.146:5000/kubeclipper/packages/bootstrap/kubeclipper:e2e-cf2f967-20260715
index digest: sha256:6be035f206ae36fa645c5850620ce86dd8a9bde199c84759d49f35c1ffc55315
sourceRevision: cf2f967e6ddbf2b81354096b88216c44d253021c
```

验证完成后已删除该标签；宿主机 Registry 中仅保留原有的 `v1.8.0` 标签。

### 历史双机纯 OCI 生命周期验证

1. 使用 OCI 在 `sh-dev-3` 部署平台，并显式指定 `ens3` 进行地址检测。
2. 登录成功。客户端与服务端报告的 Git commit 均为 `cf2f967e6ddbf2b81354096b88216c44d253021c`，且工作树状态均为 clean。
3. 使用相互独立的 server/agent SSH 密钥，从 OCI bootstrap 软件包将 `sh-dev-2` 加入平台。两个平台节点均达到 Ready 状态。部署配置中未出现私钥正文。
4. 仅使用 Registry 中的软件包/镜像创建 `e2e-cf2f967`：Kubernetes `v1.35.0`、containerd `1.7.29`、Calico `v3.29.6`。集群达到 `Running` 状态；所有 control-plane 和系统 Pod 均达到 Running/Ready 状态。
5. 通过 `cluster add-node` 添加 `sh-dev-2`。两个 Kubernetes 节点均为 Ready，并报告 containerd 版本为 `1.7.29`。
6. 通过 `cluster remove-node` 移除 `sh-dev-2`。集群恢复为仅包含 `sh-dev-3` 的 Running 状态；`sh-dev-2` 上的 kubelet/containerd 均处于 inactive 状态。
7. 通过正常 API 流程完成集群删除。
8. 一次执行 `kcctl clean --all --force --assumeyes` 即成功完成清理，同时清理了 AIO server/agent 和 join 加入的 agent，无需人工补救。

首次发布验证有意暴露并随后修复了两个清理边界问题（join 中断时的节点清单，以及 AIO server/agent 的传输配置选择）。上述完整生命周期是基于当时的资格测试提交执行的干净重测结果。由于最终 2.0 release commit 尚未形成，合并 master、Registry API 和认证/TLS 改造后必须重新执行。

### 历史最终清理证据

最终 clean 成功后，两台主机上的下列项目均已不存在或处于 inactive 状态：`kc-server`、`kc-agent`、`kc-etcd`、`kc-console`、`kubelet`、Kubernetes static-pod 进程、KubeClipper server/agent 配置目录和二进制文件、`/etc/kubernetes`、`/var/lib/kubelet`、`/var/lib/etcd`、`/etc/containerd` 以及 `/var/lib/containerd`。

临时 SSH 授权和密钥、Registry 标签、发布验证 Git 标签、源码副本、二进制文件及软件包归档均已删除。所有 216 个带临时 `qualification-*` 前缀的 GHCR 软件包均已通过已登录的软件包页面删除；重新扫描九页软件包列表后，确认剩余验证软件包数量为零。宿主机原生的 `kc-registry.service` 保持运行。
未修改无关的 `sprout-postgres-v2` 容器。

### 历史剩余缺口

- P0：最终 release commit 尚未形成且未经授权推送。当前提交的 Go tests/coverage、
  offline-resource-validate、bootstrap、resource package、release manifest/provenance
  和 OCI AIO 必需 Actions 尚无结果。
- P0：生产级认证 TLS Harbor 的双机资格测试尚未执行。必须用 robot account、
  自定义 CA 和项目最小权限验证 deploy、login/version、join、纯 OCI 建群、
  add/remove node、删除、clean 及残留审计。
- P1：补齐最终 v2.0 升级说明和 Harbor 最小权限 support policy；当前开发指南已记录
  Registry 分工、凭据输入、轮换、清理和 GHCR Catalog 限制。
- P2：测试主机未运行 chronyd/ntpd；现有部署预检已明确报告此情况。由于实测主机时钟偏差小于一秒，验证继续执行。
- P2：删除 manifest 后回收 Distribution Registry blob，仍属于 Registry 运维方的常规垃圾回收职责。

### 历史发布建议

`cf2f967` 已证明匿名 HTTP Registry 下的纯 OCI 主流程可以工作。最新 master Registry
模型已经集成，package/image Registry 混用已关闭，生产 package Registry 认证/TLS
及凭据传播也已有本地实现和测试。但是当前最终提交的 Actions 和真实 Harbor 双机
资格测试尚未完成，不能将本地通过等同于正式发布通过。

当前发布结论：**暂不建议正式发布**。

重新达到“可以正式发布”至少需要：

1. 形成并在明确授权后推送最终 release commit。
2. 在该提交上执行全部必需 GitHub Actions，并记录 run 链接、OCI digest 和
   sourceRevision。
3. 使用相互独立的认证 TLS package Registry 和 image Registry 重跑双机纯 OCI
   部署、join、建群、增删节点、删除和 clean。
4. 删除测试标签、robot account/授权、临时 CA/密钥、隧道和文件，确认两台主机无
   KubeClipper、Kubernetes、containerd 或测试凭据残留。
