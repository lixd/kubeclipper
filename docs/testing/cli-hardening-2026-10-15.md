# OCI 发布前 CLI 修复与验证

日期：2026-10-15。源码基线：`3f0d69f3`，分支 `feat/oci-operation-v2-migration`。
本轮验证对象为该基线加工作区改动；本地构建标记为 dirty，不作为正式发布制品。

## 执行计划

1. 同步 JWT 默认密钥修复，验证缺失和公开默认密钥被拒绝、正常部署生成密钥、已有有效密钥不被改写。
2. 修复退出码、标准输出和错误输出、列表结构、参数校验及帮助信息，增加命令级回归。
3. 修复命名 watch、结构化事件输出和取消行为；明确创建、删除的异步提交语义及 force 的主机清理责任。
4. 构建 Linux amd64 Server、Agent、kcctl，运行普通 Go 包回归、针对性 race 检查及真实平台 CLI 冒烟。

TLS 按用户要求不调整。多 context、资源别名、批量操作、dry-run、wait 不在本轮范围内。

## 已完成的修复

| 范围 | 新行为 |
| --- | --- |
| JWT | 默认不提供密钥；缺失或使用公开默认值时明确失败。保留 OCI 对部分配置和空嵌套选项的处理。 |
| 日志失败 | Operation、Task 或 Task 日志请求失败会返回错误，由入口输出到 stderr 并非零退出；不再打印错误后返回成功。 |
| 结构化列表 | `get` 与 `operation list` 在 0、1、多条时都输出列表，空列表为 `items: []`；详情和创建保留单对象输出。 |
| Registry 列表 | 补充 `items` JSON 标签，避免序列化为大写 `Items`。 |
| 参数 | get/delete 拒绝额外名称，未实现批量操作；force 只允许用于 cluster。create user/registry 和 operation list 拒绝位置参数。 |
| 输出格式 | 本轮涉及的 get、create、operation list 在读取配置或发出请求前拒绝非法输出格式。 |
| 输出通道 | 通用错误处理和 CLI logger 写入 stderr；本轮涉及的命令通过 RunE 向入口返回错误。 |
| watch | 指定名称会订阅该资源；JSON 保留完整 type/object 事件，YAML 用文档分隔符输出；正常 EOF 重连提示写 stderr，服务端 ERROR 和损坏帧明确失败；Ctrl-C 取消请求和等待。 |
| 异步提示 | 表格模式创建成功后提示 creation requested 及查询进度命令；普通集群删除提示 deletion requested。 |
| force 删除 | 帮助、确认和结果明确说明跳过 Kubernetes 卸载，主机进程和数据需要人工清理；未确认返回错误。 |
| 帮助 | 移除未实现的 TYPE/NAME 用法声明，Operation 示例改为现有 operation 子命令，字段选择示例使用 metadata.name。 |

watch 用于观察资源变化。本轮没有新增基于 resourceVersion 的连续恢复机制，也不将其作为可靠的审计事件消费接口。

## 回归测试

新增命令级测试位于 `cmd/kcctl/app/release_cli_test.go`，使用真实 Cobra 参数处理与 HTTP 客户端连接本地可控响应服务：

- 七类资源 × JSON/YAML × 0/1/2 条，共 42 种列表结构组合，包含服务端 items 为 null 的响应。
- 详情仍为单对象；非法格式、多余参数在配置加载前失败。
- Operation、Task、日志三个请求层级的失败向上传递。
- 普通/force 删除只报告提交，不冒充卸载已完成。
- 命名 watch 的服务器过滤、完整结构化事件、ERROR 帧、损坏帧及上下文取消。
- 创建提示只进入表格模式，不污染 JSON/YAML。
- JWT 的配置加载、验证和部署生成；加载或验证有效的持久化密钥不会旋转密钥。

针对性包测试和 race 检查通过。`git diff --check` 通过。Markdown 的 dprint 检查因仓库指定的旧插件下载超时未完成；未更改格式工具配置。生产发布策略一致性检查通过：

```bash
go test -p 2 ./cmd/kcctl/app ./pkg/cli/... ./pkg/server/config ./pkg/authentication/options ./tools/release-policy-verify -count=1
go test -race -p 2 ./cmd/kcctl/app ./pkg/cli/create ./pkg/cli/operation ./pkg/authentication/options ./pkg/server/config -count=1
go run ./tools/release-policy-verify
KUBE_BUILD_PLATFORMS=linux/amd64 make build-cli build-server build-agent
```

全量 `go test -p 2 ./... -count=1` 曾暴露以下问题，保留失败事实：

- `pkg/utils/systemctl`：本机为 macOS，无 systemd/DBus。该包现有测试会启停及禁用 journald，未将其直接移到共享测试机执行。
- `test/e2e`：本机配置指向的 `127.0.0.1:18080` 无平台服务，16 项在 BeforeEach 的版本查询阶段失败；不能计为通过。
- `tools/release-policy-verify`：原有测试夹具仍声明 v2.0.3，当前默认策略已为 v2.0.9。已只更新测试夹具并复测通过，没有放宽生产校验。

普通包回归覆盖 `./pkg/... ./cmd/... ./tools/...` 中的 220 个包，退出码 0（包含无测试文件的包）。显式排除依赖 systemd 的 `pkg/utils/systemctl`；不包含需要独立环境的 `test/e2e`。

## 实机冒烟

在 sh-dev-2 使用临时 Linux amd64 kcctl，连接原有健康平台。未替换已安装的 Server、Agent 或 kcctl。
基线为零集群、三节点、零 Operation。证据见 [live-smoke.json](evidence/cli-hardening-2026-10-15/live-smoke.json)。

- status、集群/节点/Operation 列表解析通过，节点详情保持单对象。
- 查询不存在的 Operation 日志退出 1，stdout 为空，stderr 有错误。
- 非法格式和多余删除名称被拒绝，没有创建或删除业务集群。
- 创建专属临时 ConfigMap，命名 watch 接收到真实 MODIFIED 事件且完整保留 Data；Ctrl-C 后退出 0。
- 专属 ConfigMap 删除后 GET 为 404，平台仍 Healthy，集群/节点数量不变，已安装 kcctl 的 SHA256 不变。
- 临时测试程序首次错误假设登录使用 Token、ConfigMap 返回小写 data；修正为使用已有客户端证书及实际 Data 字段后重跑。这两次辅助脚本失败不计为产品失败或通过证据。
- 远端临时目录、二进制和私有证书副本已清理。

本轮没有部署新的 Server/Agent，没有重新执行完整集群生命周期，也没有运行新候选的 CI qualification、发布门禁或发布工作流。
R46 是历史基线，不作为本次工作区改动的完整验收证明。正式发布前仍须提交并固定候选 SHA，使用该候选的 CI 制品完成受影响路径验收。

## 使用者需要注意的变化

- 脚本读取列表时统一使用 `items`；只有一条记录时也不会自动解包为单对象。
- 指定名称的 get 仍输出单对象。watch 输出为初始查询结果，随后为连续事件；JSON 消费方应逐文档解析，YAML 消费方应按文档分隔符解析。
- 原先忽略多余名称或非法格式的调用现在会明确失败。
- 日志读取失败不再返回成功，日志和诊断分别使用 stdout、stderr。
- 手工部署若仍使用缺失或公开默认 JWT 密钥，需要先配置随机密钥；已使用有效持久化密钥的安装无需更换密钥。没有自动密钥轮换。
