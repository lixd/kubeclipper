# Outcome

按用户已确认的六步计划统一 OCI 安装资源和全局容器镜像仓库体验。在线使用官方 GHCR；内网使用 registry sync 准备的完整仓库。部署记录默认 Registry，新建集群默认选中它，升级扩容继承集群保存的选择。删除旧模式和兼容实现。用户于 2026-10-29 明确要求按该计划开始，并调用 comet-native。

# Scope

两个既有工作区共同交付，保持现有分支和未提交改动：后端为本 change 的 projectRoot；前端为 /Users/lixueduan/17x/tmp/kc-fix2/console/.worktrees/combined-regression-20261028。原始界面截图作为历史体验背景，当前有效需求以本轮确认计划及后续“旧代码删除、不做兼容”为准。

## 目录结构

### 新建

- docs/comet/changes/oci-registry-experience/specs/registry-experience/spec.md — 本次完整目标规格。
- 两仓现有测试目录中的针对性测试 — 验证统一默认来源、完整地址和错误行为。

### 修改

- scripts/get-kubeclipper.sh — 下载镜像脚本参数，不产生持久 env。
- cmd/kcctl/app/options/、pkg/cli/deploy/、pkg/cli/create/、pkg/cli/cluster/、pkg/cli/join/、pkg/cli/upgrade/ — 来源参数、默认 Registry、继承、帮助和自动补全。
- pkg/apis/config/v1/、pkg/apis/core/v1/、pkg/scheme/、pkg/component/、pkg/agent/ — 默认引用、API 校验、统一镜像解析和安装路径。
- pkg/delivery/、pkg/simple/、scripts/open-packaging/、相关测试文档 — 仅调整本次来源规则和废弃路径所需内容。
- 前端 src/pages/cluster/containers/Cluster/、Registry/、src/stores/cluster.js、src/utils/registry.js、src/locales/、相关测试 — 全局选择、升级、确认和完整地址。

### 删除

- 上述模块内旧 online/offline 安装分支、HTTP 静态安装资源及镜像归档自动导入旧实现、隐藏镜像 env 读取、失效函数字段文案测试；按引用核对后删除。

### 明确不建

- 阿里云预置源、组件独立仓库、仓库用途分类、安装来源展示面板、下载日志增强、新兼容层、新页面。

# Non-goals

不新增镜像下载日志或进度条；不新增国内完整发布服务；不改用户工作负载镜像；不发布正式版本，不自动 commit、push、merge 或 PR；不覆盖用户已有未提交改动，不清理运行中资源。

# Acceptance examples

- A1：下载脚本默认从 GitHub 获取 kcctl，--mirror cn 从国内站获取；KC_REGION、KC_DOWNLOAD_URL 及持久镜像 env 入口删除，帮助示例同步。
- A2：deploy 默认使用官方 GHCR，指定 --package-registry 则使用该仓库；正确登记含路径、认证、TLS 配置的默认 Registry 并保存明确引用；重复部署不删除重建或静默覆盖冲突资源。
- A3：Web、CLI、API 新建集群共享默认解析；未指定时解析平台默认并保存引用，显式选择优先；无有效 Registry 明确失败，不按唯一仓库或上游回退。
- A4：Kubernetes、pause、Calico 及此次安装涉及的平台组件使用统一选定仓库，路径与发布同步布局匹配；安装包和 Chart 仍使用平台资源仓库；已知资源缺失、版本架构不匹配明确失败，不伪造完整性状态。
- A5：创建与确认页只有一个明确镜像仓库，默认选中平台默认；删除组件默认来源、在线离线、组件级及 Package Registry 说明；问号仅为“用于拉取 Kubernetes、Calico 等组件的容器镜像。”。
- A6：Registry UI 和 CLI 使用完整地址含协议端口路径，解析展示一致；CLI --address 替代旧地址参数；认证和证书保留；错误地址清晰拒绝。
- A7：升级和扩容固定继承集群保存的仓库，不随平台默认变化；移除升级时的仓库切换 UI、CLI 参数及 API 字段，服务端拒绝普通编辑变更集群仓库；目标版本资源仍须校验。
- A8：删除旧 --offline、--online、--kc-image-repo-mirror、KC_IMAGE_REPO_MIRROR、/etc/kc/kc.env 镜像链路及旧 HTTP/镜像归档安装回退，不做兼容；相关帮助、补全、文档和测试同步。
- A9：源码和必要测试覆盖在线 GHCR、sync 后内网、路径、缺失错误及升级扩容；资源密集检查在 sh-dev 使用包含本地改动的准确源码，分别记录清单检查、拉取及实际安装结果，不互相替代。
- A10：保留两仓既有未提交工作；测试环境数据修改和服务替换前检查资源引用与活跃操作，备份可回滚；预览与真实后端候选匹配；未获得提交推送授权不提交推送，后续推送仅用户 fork。

# Constraints and invariants

- 日志增强明确排除。无阿里云内置项，无组件级配置。没有新用户决定时自主连续推进。
- 现有前端 worktree 的离线必填校验和上一轮预览改动是保留的输入，不 reset、不覆盖；随着新规则更新它们及对应测试。
- 平台和集群来源默认同址，但资源与镜像仍是两个配置职责；界面不暴露安装资源细节。
- 对旧测试数据做显式一次性规范化，不增加运行时兼容路径；无法明确处理的运行数据先保留并报告。
- 独立只读 Verifier 验收全部 A1—A10；宿主具备 collaboration 子 Agent 及 Runtime CLI 回报能力。采用单个 change，因为默认引用、API、CLI 和两仓 UI 紧密耦合，Supervisor 多工作树集成不适合当前既有两仓目录。

# Confirmed scope refinement

用户于 2026-10-29 明确选择“第一版升级固定沿用集群原仓库（推荐）”。本版不实现仓库迁移；升级只更改版本，仓库只读展示，并拒绝通过普通集群编辑改变该引用。A7 和目标规格据此替换先前显式升级切换仓库的描述。A1—A6、A8—A10、范围、非目标及既有授权保持用户已确认内容，不存在待确认问题。
