# KubeClipper OCI 版 E2E 自动化落地方案

以 OCI 工作树 `fe0b59fe` 和打包输入 `v2.0.12` 为基线。沿用 **Go + Ginkgo/Gomega、Cypress 和 GitHub Actions**，从当前候选的 `kcctl`、Go client、API handler 和 Registry 制品重建 E2E 主流程。先自动验 OCI AIO 冷部署、建群、业务和清理，再加备份恢复、多节点和升级。每项断言回填 154 条有效用例的台账。[当前打包输入](../../packaging/resources.yaml) · [OCI 交付说明](../oci-delivery.md) · [用例台账](testcase-inventory-2026-11-19.md#case-catalog)

首批实机回归用 Ubuntu 24.04/amd64 执行 `6-09`。按 OS、架构和候选制品参数复用同一发布生命周期。后续补 ARM64 时用该架构的节点、测试二进制、组件包和镜像，结果记在 `6-09` 对应变体下。

## 当前 OCI 分支能复用什么

- CI 已运行 Go 测试和发布契约。准入规则、版本矩阵、错误输入、操作状态机和制品验证优先补在这些入口。保留少量真实拒绝路径，确认 API/CLI 返回错误，且集群和节点状态符合预期。[Go 测试工作流](../../.github/workflows/go-test-coverage.yml) · [用例执行规格](release-case-specs.md)
- **AIO CI 已能 OCI 部署 KC。** 现有 `ubuntu-24.04` 工作流编译候选、打包锁定 revision 的 Console、推送四个 bootstrap 包、部署 KC 并执行 `make build-e2e`。按当前注册和名称筛选，`[Fast]` 匹配 7 条，覆盖模板、Registry 对象、节点管理和 Region 查询。创建 Kubernetes 的 spec 标成 `[Slow]`。首次接入时用 `-ginkgo.dryRun` 校验选择范围，再执行真实流程。[AIO 工作流](../../.github/workflows/test-aio.yml) · [套件注册](../../test/e2e/e2e_test.go) · [Region spec](../../test/e2e/region/region.go) · [Ginkgo v1 参数](https://github.com/onsi/ginkgo/blob/v1.16.4/config/config.go)
- **复用 Go client 和部分 fixture。** 建群 fixture 已能读取 `ComponentMeta` 来找可装组合。旧测试中的固定资源名、任选空闲 Agent、只等 `Running` 的断言需要替换。两个 AIO 升级 spec 都从同一配置选择版本、用相同维护选项请求升级，`[Online]` 标签对应的镜像来源也相同。把它们收敛成有明确环境和业务判据的场景，受理成功后按本轮 Operation 及 Task 等待。现有 `WaitForUpgrade` 检查 `UpdateFailed`，当前升级失败状态是 `UpgradeFailed`。[KC client](../../pkg/simple/client/kc/api.go) · [建群 fixture](../../test/e2e/cluster/utils.go) · [集群 spec](../../test/e2e/cluster/spec.go) · [状态等待](../../test/framework/cluster/wait.go) · [状态定义](../../pkg/scheme/core/v1/cluster_types.go)
- **候选制品已有 qualification。** 工作流发布完整 OCI 清单并验证镜像同步、重复导入，后续接上干净 Linux 主机真实消费同步目标。`94c69e7c` 的两机历史验收覆盖了 `v1.35.8` 拒绝升级、AIO 显式维护以及双节点 PDB 阻塞后重试，可直接提炼断言。当前工作树的运行结论由新的 CI 产出。[qualification 工作流](../../.github/workflows/publish-oci-qualification.yml) · [OCI 升级验收](../comet/archive/2026-11-17-oci-upgrade-readiness/verification.md)

## OCI 版的调用与验收依据

下表中的集群接口使用 `/api/core.kubeclipper.io/v1` 前缀。Go 编排从当前仓库导入 client 和请求类型，CLI 用例调用本轮构建或发布的 `kcctl`。

| 环节 | 当前用法与应核验的结果 |
| --- | --- |
| 平台安装与交付 | `kcctl deploy --package-registry <prefix>`，本地 HTTP fixture 显式传 `--package-registry-scheme http`。部署创建默认 `kc-package-registry`。安装包来自部署指定的 Package Registry，Kubernetes 和 Calico 等运行镜像由集群 `--image-registry <Registry名称>` 选择，缺省使用平台默认值。先核对地址、服务、进程版本和 Agent 注册，再创建集群。候选在线同步用 `registry import --manifest ... --target ...`。离线用 `registry export --manifest ... --output ...` 生成两个包，通过 `registry deploy --node ... --bundle <Registry小包>` 自举仓库，再 `registry import --bundle <完整大包> --target ...`。冷节点阻断公网后实测部署、join 和建群。关联 `1.2-02`、`1.2-05`、`1.1-03`、`1.1-07`、`6-07`。[交付说明](../oci-delivery.md) · [Registry CLI](../../pkg/cli/registry/registry.go) · [导出](../../pkg/cli/registry/export.go) · [导入](../../pkg/cli/registry/import.go) |
| 实际建群与制品 | 先查 `GET /api/config.kubeclipper.io/v1/componentmeta?arch=amd64`，确认本轮固定的版本和默认镜像源可用。AIO 用 `kcctl create cluster --name <name> --master <id> --untaint-master --k8s-version v1.37.0`，API 用 `POST /clusters`。多节点明确传入 `--master`、`--worker`。核对 Cluster 的节点、版本、`status.packagePlan` 各槽位的 `repository@sha256:…`，以及对应 Task 或 Agent 拉取记录。运行镜像另查 Pod 和容器运行时的 RepoDigest，与同架构预期镜像摘要对照。关联 `3-05`、`2.6-01`、`2.6-03`、`2.1-07`。[建群 CLI](../../pkg/cli/create/create_cluster.go) · [版本查询](../../pkg/apis/config/v1/handler.go) · [Cluster 字段](../../pkg/scheme/core/v1/cluster_types.go) |
| 异步操作 | 建群返回 Cluster，普通删除和 Kubernetes 升级返回 HTTP 状态。提交前记同类操作基线，提交后用本轮 Cluster UID 查询 `/api/operations.kubeclipper.io/v1alpha1/operations` 的 `fieldSelector=spec.targetRef.uid=<UID>`，再匹配本次 `spec.action` 和新增 UID。`ListOperationTasks` 已支持按 Operation UID 查询。对成功场景等待 `Succeeded` 并核对实际步骤与目标节点的 Task。重试保留历史失败，核对本轮尝试和最终状态。普通删除可以先以同一 selector 订阅 Operation watch，及时记录终结器清理前的证据。[集群 handler](../../pkg/apis/core/v1/handler.go) · [Operation 查询字段](../../pkg/server/registry/operationv2/strategy.go) · [Operation API](../../pkg/apis/operations/v1alpha1/handler.go) · [KC client](../../pkg/simple/client/kc/api.go) · [集群终结器](../../pkg/controller/clustercontroller/controller.go) |
| Kubernetes 访问 | `GET /clusters/{name}/kubeconfig` 返回直连配置，`?proxy=true` 返回代理配置。测试从该公开接口取得文件，使用原配置实连本轮群，核对 `/readyz`、节点、系统 Pod、Service、DNS 和实际读写。按直连、代理、权限不足分别记录 `4-12` 的结果。`2.1-31` 还需按建群拓扑登记。[集群路由](../../pkg/apis/core/v1/registry.go) · [kubeconfig handler](../../pkg/apis/core/v1/handler.go#L986) |
| 备份、恢复与删除 | `POST /clusters/{name}/backups` 从响应取实际备份名，以 `X-Operation-Name` 或返回对象的 `kubeclipper.io/operation-name` 标签关联任务。`GET /backups/{name}` 核对 `backupStatus.status=available`、实际文件大小和 MD5。`POST /clusters/{name}/recovery` 请求体的 `useBackupName` 指向这份快照，使用响应标签关联 `RecoveryCluster` Operation。写入 A、备份、实读 B、恢复后实读 A。用 `kcctl delete cluster <name>` 或 `DELETE /clusters/{name}` 验证备份保护，再删备份和集群，确认主机复位并用同一节点重建。关联 `2.5-01`、`2.5-03`、`2.4-04`、`2.4-01`。[恢复类型](../../pkg/scheme/core/v1/recovery_types.go) · [集群 handler](../../pkg/apis/core/v1/handler.go) · [删除 CLI](../../pkg/cli/delete/delete.go) |
| 扩缩容与重试 | `PUT /clusters/{name}/nodes` 传当前 `PatchNodes` 的 `operation=add` 或 `operation=remove`、节点和角色。扩容实际消费已存 `status.packagePlan`，检查新增节点 Ready、调度及 digest。失败场景用 `POST /operations/{name}/retry` 走 Operation API，核对同一 UID 的恢复和任务尝试。关联 `2.2-01`、`2.2-02`、`2.2-08`、`5-01`。[扩缩容入参](../../pkg/clusteroperation/node.go) · [集群 handler](../../pkg/apis/core/v1/handler.go) · [Operation 路由](../../pkg/apis/operations/v1alpha1/registry.go) |
| 两类升级 | 平台升级从已有 OCI 版本起步，用 `kcctl upgrade all --manifest <candidate manifest>`，已正式发布的版本可走 `--version` 加发行清单校验。核对主机上的实际组件版本、健康度及回滚结果。旧静态包部署切到 OCI 要重新部署或重新 join。Kubernetes 使用 `kcctl cluster upgrade --cluster-name <name> --version v1.37.0` 或 `POST /clusters/{name}/upgrade`，AIO 额外显式传 `--single-node-maintenance`，API 对应 `singleNodeMaintenance=true`。首批跑 `v1.36.4 → v1.37.0`，`v1.35.8` 作为仍可建群但拒绝升级的对照。普通多节点升级用 PDB 验证真实驱逐和失败重试，检查各节点的实际 kubelet 版本、API 版本及业务恢复。关联 `2.3-01`、`2.3-02`、`2.3-06`、`2.3-08`。[平台升级](../../pkg/cli/upgrade/upgrade.go) · [Kubernetes 升级](../../pkg/cli/cluster/cluster_upgrade.go) · [兼容性说明](../oci-delivery.md#7-compatibility-notes) |

### CI 怎么准备 OCI 制品

- **PR AIO。** 现有临时仓库只有四个 bootstrap 包。继续构建本次 `kcctl`、Server、Agent 和锁定 revision 的 Console，额外复制 amd64 K8s、containerd、Calico、`k8s-extension` 包、运行镜像和探针镜像。用 `packaging/resources.yaml` 对照依赖版本。与已验证的清单一起固定实际平台摘要，只有 tag 的条目在建立依赖基线时核验摘要。每轮把固定内容复制到预期 tag 并核对结果。打包输入变更时重建受影响制品或使用新候选 qualification，遇到缺件明确报错。旧清单的 bootstrap tag 可能与本地候选冲突，完整清单校验要求 `bootstrap/kubeclipper`。PR 准备脚本逐件复制依赖，保留本次构建的 bootstrap。[AIO 工作流](../../.github/workflows/test-aio.yml) · [制品定义](../../packaging/resources.yaml) · [清单校验](../../pkg/delivery/releasemanifest/manifest.go) · [同步实现](../../pkg/delivery/releasemanifest/sync.go)
- **候选验收。** 使用同一候选 qualification 的完整清单和候选 bootstrap 包中的 `kcctl`，通过产品的 import/export、deploy 和 upgrade 入口接入。以候选 Git SHA、清单文件 SHA256 和实际 Registry digest 关联 qualification 结果和实机报告。冷启动查 Registry 拉取和 Agent 的执行结果，后续扩容与重试验证原计划的固定 digest。[qualification 工作流](../../.github/workflows/publish-oci-qualification.yml) · [交付说明](../oci-delivery.md) · [发布 gate](../../scripts/open-packaging/release-gate.sh)
- **单机 Registry。** 当前 AIO 临时 Registry 由 Docker 启动。建删真实集群会重启、停止及清理宿主机 containerd。首条真 E2E 将 Registry 作为独立服务启动，例如用已有 Registry 小包启动原生服务，避免依赖受测节点的 containerd。预检和删群后都检查 Registry 可访问，确保删除后可用相同制品重建。[容器运行时安装与清理](../../pkg/scheme/core/v1/cri/containerd.go) · [Registry 部署命令](../../pkg/cli/registry/registry.go)

## 每类用例用什么实现

| 用例 | 实现方式 |
| --- | --- |
| 安装、集群、节点、升级、数据恢复、删除 | 在现有 Go/Ginkgo 套件复用 KC client 和公共 fixture。测试部署交付和 CLI 独有行为时调用候选 `kcctl`。通过 KC API 查本次操作，通过 Kubernetes 客户端查工作负载，通过 SSH 查实际主机服务、身份、运行时和清理。两三台独立 Linux VM 足够起步，后续按产品承诺增加原生架构、不同网络和真实故障。 |
| 校验、拒绝路径和制品规则 | `make test`、Go 表驱动测试、现有发布契约与真实候选制品核验。每条用例只在需要的边界上造错，E2E 选代表性的真实请求做接线验证。 |
| 登录、表单提交、进度展示和 Web 终端 | 在 Console 仓库维护单测和 `cypress/e2e/pages/` 下的浏览器用例。E2E 用当前 OCI 页面核对用户操作、请求和显示结果。后端用 Go E2E 校验实际 Operation、网络、版本和数据。[Console 检查点](release-case-specs.md#console) |
| 已有 Kubernetes 集群上的对象、探针或导入 | 可以在 kind 上独立验证已建集群的用例，例如运行探针、查询并修改 Kubernetes 对象，实际支持导入时再核对 KC 导入路径。kind 支持多节点配置。节点是容器，由 kind 自己启动集群，覆盖结论限于该环境。KubeClipper 从干净节点安装、独立 Agent 注册、宿主机路由或 systemd 生命周期继续用 Linux 主机。`kubernetes-sigs/e2e-framework` 提供已建集群的环境回调和对象助手，现阶段已有套件能承载这些断言。[kind 快速入门](https://kind.sigs.k8s.io/docs/user/quick-start/) · [Kubernetes E2E Framework](https://github.com/kubernetes-sigs/e2e-framework) |

首批沿用实际解析到 `v1.16.4` 的 Ginkgo 和 `make build-e2e`，用 `[OCI-AIO]` 选择新增场景。JUnit 使用 v1 自带的 reporter，逐项断言由 JSON 记录。首条 OCI 流程稳定后，再评估按整个 E2E binary 单独迁移 Ginkgo v2，使用 `Label`、`--label-filter`、JUnit 和 JSON 报告。Cluster API 的套件也将拓扑配置、筛选与结果目录作为运行参数，可参考其组织方式。[现有依赖](../../go.mod) · [Ginkgo v1 JUnit Reporter](https://github.com/onsi/ginkgo/blob/v1.16.4/reporters/junit_reporter.go) · [Ginkgo v2 文档](https://onsi.github.io/ginkgo/) · [v2 迁移指南](https://onsi.github.io/ginkgo/MIGRATING_TO_V2) · [Cluster API 测试指南](https://cluster-api.sigs.k8s.io/developer/core/e2e)

<a id="console-e2e"></a>

## Console 测试与发布候选联跑

- **Console 仓库。** 单测覆盖表单校验、组件行为和状态渲染。Cypress 覆盖登录、向导、维护确认、Operation 进度和错误、kubeconfig 下载、终端及 OIDC 页面流程。首条用当前 `imageRegistry` 表单登录并提交建群，核对实际请求和任务状态展示。已有 `online-offline-install.cy.js` 使用旧 `offline` 选择器，需要更新。
- **KubeClipper 仓库。** Go/Ginkgo 验实际 API 鉴权、任务与节点、版本和数据正确性。UI 发起的实际操作可以用同一个 Cluster/Operation UID 给两套断言取证。用例目录和通过判据共用，浏览器子项引用 Console 的 spec 名。
- **候选验收。** 从 `bootstrap.consoleSourceRevision` 检出对应 Console 的用例，对已部署的候选页面运行 Cypress。现有 AIO 和 OCI 打包已按该字段构建 Console，打包脚本校验归档内的 `version.json`。联跑时另从实际入口读取版本信息并比对，记录后端 SHA、Console SHA、候选 manifest 摘要、用例编号及结果。Console PR 用自己的提交和固定测试后端验证页面。[打包配置](../../packaging/resources.yaml) · [AIO 中的 Console 构建](../../.github/workflows/test-aio.yml) · [OCI 打包校验](../../scripts/open-packaging/bootstrap-packages/common.sh)
- **Cypress 入口。** 用 Console 测试配置指定本轮部署地址和测试账号，直接执行 `yarn cypress run --headless`。现有 `test:e2e:run` 的 `|| true` 会屏蔽失败。当前 `specPattern` 按文件列出，新增 spec 时一并登记。记录实际执行、失败和跳过的 spec，保留 JSON 报告与失败截图。

## 首批场景与通过条件

复用一次环境里的多项业务断言，并为每条台账用例写出独立结果。组合矩阵先选会改变安装、调度、网络或维护路径的代表组合。在交付和升级前顺带检查真实版本组合，查询和格式放在轻量 Go 契约层。每个变体各自登记结果。[台账拓扑矩阵](testcase-inventory-2026-11-19.md#topology-matrix)

| 场景 | 用例与通过条件 |
| --- | --- |
| AIO 建群、业务和清理 | 首条用 Ubuntu 24.04 amd64、K8s v1.37.0、containerd 2.2.4、Calico v3.31.5 VXLAN。在指定节点部署 KC 并冷建单控制面，普通 Pod 可调度，校验 OCI plan、`CreateCluster` Operation、kubeconfig 直连和实际业务。正常删除后检查 kubelet、containerd、节点占用及本轮文件，再按同配置重建。先挂接 `2.1-02`、`3-05`、`2.6-01`、`2.6-03`、`2.1-31` 和 `3-08` 的普通删除。`4-12` 代理和拒绝分支、`2.4-01` 的 `SyncKubeConfig` 并发分支各有用例。仅 Master 且保留污点为独立变体，检查普通 Pod 的调度事件。 |
| AIO FS 恢复与升级 | 在上述 AIO 集群删除前接入 `2.5-01`、`2.5-03`、`2.4-04`，断言真实快照、恢复后的数据和业务健康、删除约束。AIO 升级使用 v1.36.4 基线，依次验证默认拒绝、显式维护允许，观察业务中断与恢复，以及实际组件版本。 |
| 两台机器冷建 1M1W | `2.1-24`、`2.1-18`、`2.1-31`。指定独立 Master、Worker，确认两种角色及 Worker 实际调度，查询 Calico VXLAN 配置。将探针放到不同节点，Master 探针可带容忍。双向实测 Pod IP，校验 Service 和 DNS，记录实际节点名、Pod IP 和响应。 |
| 三台机器的 1M2W、扩缩容与升级 | 先从 1M1W 扩出第二个 Worker，两个 Worker 分布业务副本。`2.2-01`、`2.2-02`、`2.2-08` 检查扩容沿用的 digest、新节点调度、移除时的驱逐、剩余 Worker 接管及主机清理。移除后重新补足两个 Worker，再跑升级。`2.3-01` 的多 Worker 变体先验证多节点携带单节点维护选项被拒，普通滚动升级先升 Master，再逐个升 Worker。持续实测 Service，并验证 PDB、实际版本和升级后数据。改动涉及批量加入或多目标 Task 时，追加 `2.1-24` 的 1M2W 冷建。 |
| 发布拓扑和制品 | `2.1-03` 验 3M0W 的初始化、后续控制面 join、Kubernetes etcd 三成员与单点失效后的多数派读写。按发布范围另跑 3M1W 的 Worker/VIP，3M2W 可复用跨 Worker 和滚动维护断言。平台三 Server HA 也要构造实际入口与平台 etcd 多数派测试。在线安装、私有 CA/认证、冷缓存离线交付和平台升级按候选支持范围在首批 amd64 环境运行。[发布执行规格](release-case-specs.md#delivery) · [升级执行规格](release-case-specs.md#upgrade) |

[P0 执行顺序及其余用例](testcase-inventory-2026-11-19.md#automation) · [两节点验收记录](delivery-twohost-2026-11-06.md) · [三节点验收记录](oci-threehost-2026-11-03.md)

## 环境准备、触发和报告

**环境。** 一次运行拿到候选和一份实际节点清单，包含主机身份、预期角色、OS/架构和端点。AIO 用同一个 GitHub-hosted job 编译、部署和运行。如果拆 job，使用 GitHub Actions artifacts 传递同一份候选，在测试 job 重新部署。多机从可重置的 Linux VM 或专用测试池取得整组租约，先检查资源容量、系统和网络符合产品规格，再分配 Master/Worker，按节点清单创建和清理。历史验收记录适合提炼步骤，机器和自动化权限需要单独核实。GitHub-hosted Runner 的 VM 生命周期和测试集群生命周期应各自管理。[GitHub-hosted Runner](https://docs.github.com/en/actions/concepts/runners/github-hosted-runners) · [GitHub Actions artifacts](https://docs.github.com/en/actions/concepts/workflows-and-actions/workflow-artifacts)

**运行。** 每次生成 run ID，使用隔离的 `kcctl` 配置、唯一集群名、备份名和业务标记。拿到节点后登记归还。按上表关联并保留本轮 Operation UID，用有截止时间的轮询查状态。失败时先抓 Task 日志、事件和主机诊断，再清理本轮资源。备用回收流程处理取消或超时。初期同一组节点上的用例串行，验证复位并聚合报告后释放租约。[现有 fixture 生命周期](../../test/framework/framework.go) · [GitHub Actions concurrency](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency)

| 什么时候 | 默认执行 |
| --- | --- |
| KubeClipper PR | 运行 Go/发布契约和 OCI AIO 生命周期。AIO 稳定后设为合并检查，并把工作流的目标分支配置覆盖 OCI PR。首条用例先在 GitHub-hosted AIO 上通过，再用一个人为错误的预期值确认测试和 CI 确实失败。 |
| Console PR | 沿用 Console 的单测检查。浏览器检查接入后，用此 PR 的前端与固定测试后端跑受影响页面的 Cypress。 |
| 可信分支合并后、夜间或手动 | 先用 `workflow_dispatch` 接通独占 Linux 测试环境，冷建 1M1W，再按专项扩到 1M2W 跑扩缩容及滚动升级。需要 Console 验收时运行对应 Cypress spec。稳定后启用夜间或合并触发。外部 PR 走 GitHub-hosted 测试，不接触这个专用机器池。 |
| 正式发布候选 | 用同一候选执行制品校验、amd64 平台与集群实机用例，以及当前发布范围内的 HA 和 Console 检查。部署候选中的 Console，调用对应 revision 的 Cypress。逐项记录执行范围及结果，接入现有验收记录。 |

[GitHub Actions 触发事件](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows) · [矩阵配置](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/run-job-variations) · [Self-hosted Runner 指南](https://docs.github.com/en/actions/reference/security/secure-use#hardening-for-self-hosted-runners)

**报告。** 保留 Markdown 台账和执行规格供评审。机器侧沿用已有证据样例的 `cases.json`，建议补充变体、操作 ID、断言结果和证据相对路径，并增一份运行元数据，记录运行 ID、候选 SHA、Console ref、适用时的 manifest SHA256、实际 digest、拓扑、OS/架构、超时和清理结果。每个期望用例 ID 加变体生成一条结果。一条 spec 覆盖多个 ID 时，分别输出实际完成的断言。报告中明确区分 `PASS`、`FAIL`、`BLOCKED`、`SKIP` 和有理由的 `N/A`。缺依赖、缺证据或未运行的适用 P0 无法通过当轮门禁。现有发布 gate 继续消费候选验收记录，新增 JSON 作为其证据。正常退出或用例失败都上传 JUnit、JSON 和脱敏日志，排除 kubeconfig、认证信息和含凭据的 URL。机器报告和人工记录使用同一个用例 ID。[历史证据格式](evidence/r46/cases.json) · [执行规格的判定规则](release-case-specs.md) · [GitHub Actions artifacts](https://docs.github.com/en/actions/concepts/workflows-and-actions/workflow-artifacts)

## 实施验收

1. **AIO 回归。** 在现有工作流补齐依赖、操作关联和 AIO 建群探针。报告记录实际 OCI digest、API Ready、Pod 调度、Service/DNS、真实写读和正常清理。CI 校验选中了新场景且产出必需断言，检查正常运行通过及错误预期时 CI 失败。再补同环境 FS 恢复和同节点重建。
2. **多机可重复。** 在两台 Linux VM 上跑完 1M1W 两次，核对第二次可以干净重建。再加入第三台形成 1M2W，验证增删 Worker、PDB 和滚动升级。批量加入或多目标 Task 变更时追加 1M2W 冷建。运行一次受控失败或取消，确认租约可回收，诊断可下载。
3. **按 P0 扩展。** 将适用的 P0 ID 和实际环境变体登记到跑批清单。自动化覆盖率按已实现且在 CI 启用全部必需变体的适用 P0 编号计算。候选通过率用当轮实测结果，便于同时追踪接入进度和发布质量。先补尚未有证据的发布、操作控制、权限场景，再加 P1/P2。正式候选生成后走既有 qualification 和发布验收，版本、架构与拓扑承诺依照本轮范围。

本轮核对了当前 OCI CLI、API、已有 CI 和历史实机记录。此文是首批用例与 CI 的实施设计，接入结果以首次运行的候选报告为准。
