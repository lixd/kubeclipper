---
generated_from_state_version: 7
---

# 验证

## 当前结果

- 结果: **验收通过，需要你确认**
- 验证情况: **已在实现会话中完成验收**
- 目标周期: 1
- 迭代: 1
- 验证器尝试次数: 1
- 完成时间: 2026-11-17T09:54:06.932Z
- 摘要: 三项修复和六项验收全部通过，代码及文档推送独立fork分支。完整报告 /Users/lixueduan/17x/tmp/kc-fix2/output/kc-upgrade-94c69e7c-20261117/results.md

## 验收

| 编号 | 结果 | 来源 | 验收项 | 原因 |
| --- | --- | --- | --- | --- |
| A1 | passed | brief.md | 旧 Server 下单独升级两台 Agent，命令成功，Agent 实际目标版本且服务健康，重复执行成功。 | 旧Server v2.0.11保留，两台Agent从v2.0.11升至94c69e7c，命令成功、服务健康、Server文件摘要不变；重复执行成功。evidence/sh-dev-3/evidence/agent-old-server-upgrade.json |
| A2 | passed | brief.md | CLI 和 API 对低于 v1.36.4 的升级请求返回清晰原因，零升级任务与节点文件变更，v1.35.8 安装和管理可用。 | 节点4实装v1.35.8/containerd1.7.29/Calico3.29.6，管理ConfigMap读写删通过。CLI/API分别拒绝升1364/1370，无新增升级Operation且节点二进制及静态Pod文件摘要不变。legacy-kubernetes-install-and-upgrade-admission.json |
| A3 | passed | brief.md | 单节点维护模式通过 API/CLI/Console 明确提示中断和全部 PDB 绕过影响，默认调用保留保护并清晰拒绝无法完成的排空。 | API未开启/false单节点维护均400且无任务，多节点true也400。CLI明确停机和全部PDB影响。Console三项测试、真实发布提交及默认Checkbox false检查通过。 |
| A4 | passed | brief.md | 单节点 v1.36.4 升至 v1.37.0 后控制面、kubelet 实际版本一致，Calico 与系统 Pod 恢复 Ready，API 和业务探针正常。 | 节点3 AIO1364至1370成功，显式维护下业务minAvailable1 PDB与默认Calico PDB共存，业务Pod更新。API与kubelet1370，活动系统Pod Ready、HTTP/DNS探针成功、调度恢复。aio-maintenance-upgrade.json |
| A5 | passed | brief.md | 升级失败保留可观察的实际状态，原生重试可完成目标升级，调度状态恢复且多节点保留 PDB 驱逐语义。 | 双节点正常驱逐被业务PDB阻止，旧业务Pod UID保留。Failed后节点恢复调度，实际控制面1370/kubelet1364和日志可观测。移除测试PDB后原生operation retry成功，两个kubelet与API1370，最新Task成功。multi-pdb-protected-failure.json、multi-offline-retry-success.json |
| A6 | passed | brief.md | sh-dev-3 和 sh-dev-4 在线及离线升级复验通过，所有最新 Operation Task 成功，测试后恢复原 Registry 和其他业务服务，保护 sh-dev-2。 | AIO在线升级与双节点定向断网升级/失败重试通过。KC进程、CRI、kubelet、CLI与kubepods受公网隔离，基线可达GHCR/OSS及两Pod公网请求被阻止。前后8项网络、5轮API稳定检查通过。清理测试K8s、覆盖与临时连接，Registry及其他业务服务恢复，保护sh-dev-2。final-audit.json |

## 检查

| Agent 报告的检查 | 结果 | 执行与证据 |
| --- | --- | --- |
| 后端定向测试与核心race | passed | sh-dev Go1.24.2，精确功能源码94c69e7c。定向Go测试、E2E cluster包单元测试、核心race及release-policy-verify通过。806个Go及关键依赖/打包文件摘要一致。证据 /Users/lixueduan/17x/tmp/kc-fix2/output/kc-upgrade-94c69e7c-20261117/evidence/ |
| 可运行race包回归与HTTP环境复验 | passed | 全仓库原命令失败记录保留。补齐Alpine工具后，各可运行包除Baidu公网HTTP请求超时均通过，该失败HTTP包随后在同一开发机host网络以-race -count=1通过。4个真实systemd/D-Bus用例和全量E2E主入口跳过，真实核心E2E另验。此项按各包最新结果采信，未将失败的整仓命令报告为退出0。详见results.md。 |
| Console新参数回归、构建与实际部署元数据 | passed | Node22隔离容器frozen lockfile安装，3/3 Jest、生产构建成功。发布制品部署后version.json为2169a47，实际Checkbox默认false已复核。 |
| 完整fork qualification | passed | 44051598730全部成功，含双架构、发行清单、同步往返。构建代码94c69e7c，后续dd7d34b5仅补9行API与验证范围文档，Go和打包输入一致。https://github.com/lixd/kubeclipper/actions/runs/44051598730 |
| 两节点Agent、升级起点、AIO维护、断网PDB失败及原生重试 | passed | 所有六项验收的实机证据已收集，详情 /Users/lixueduan/17x/tmp/kc-fix2/output/kc-upgrade-94c69e7c-20261117/results.md。双节点升级含29个最新Succeeded Task、1次历史失败恢复，8项双向网络和5轮API读写成功。 |
| 环境恢复、实际运行二进制及服务保护 | passed | 最终doctor17/17，Server与两台Agent运行进程二进制摘要匹配候选文件。测试K8s原生删除，两节点6443/2379无监听、kubelet/containerd非活动，断网覆盖清除。节点3原Registry及四个业务服务PID/重启数一致，节点4原Registry服务/配置/二进制一致，sh-dev-2未访问。构建任务缓存与转发连接已清理。 |
| 静态OpenAPI和使用说明 | passed | dd7d34b5静态schema JSON解析与singleNodeMaintenance布尔、默认false、version必填约束检查通过。使用说明包含停机、全部PDB绕过、备份、源版本边界和实测平台范围。 |

### Builder 报告的证据

以下为 Builder 报告，不等同于 Runtime 检查凭据或独立验收结果。

- sh-dev Go1.24.2 定向测试和E2E包单元测试: passed — 精确候选源码，go test -p2 ./pkg/scheme/core/validation ./pkg/scheme/core/v1/k8s ./pkg/apis/core/v1 ./pkg/cli/upgrade ./pkg/cli/cluster ./test/e2e/cluster。/tmp/kc-upgrade-go-tests-final.log
- Console frozen-lockfile独立安装、3项回归测试与生产构建: passed — sh-dev Node22隔离容器，yarn install --frozen-lockfile --ignore-scripts，Jest 3/3，yarn build:run成功。/tmp/kc-upgrade-console-remote.log
- qualification及3/4真实升级: not-run — qualification44051598730执行中，真实主机验收将在Verify完成。
- 已知限制: 本次候选真实主机升级、race与qualification仍在执行或待验收
- 已知限制: 保留既有组件版本组合的有限环境支持范围，未实现containerd1至2迁移，正式稳定发布未执行

## 阻塞项

- **user**: Verification passed; user acceptance is required before Archive. — next: `await-user`

## 风险与跳过的工作

- 实机范围为Ubuntu24.04 amd64和Calico VXLAN，ARM64仅构建/制品验证，4个依赖systemd的单测未执行。完整E2E入口含三节点场景，本次核心场景在授权两节点另行完成。
- 保留containerd2.2.4/Calico3.31.5的有限环境验证范围，未实施1.x至2.x迁移。实机复用已校验的K8s/CRI/CNI Registry物料，新平台四组件使用本次制品，新完整物料往返由qualification验证。
- 正式稳定版发布待执行。Comet自动生成的本地配置与状态保留等待用户接受和归档。

## 之前的迭代

| 目标周期 | 迭代 | 尝试 | 结果 | 未解决项 | 摘要 | 完成时间 |
| ---: | ---: | ---: | --- | --- | --- | --- |
| 1 | 1 | 1 | pass | — | 三项修复和六项验收全部通过，代码及文档推送独立fork分支。完整报告 /Users/lixueduan/17x/tmp/kc-fix2/output/kc-upgrade-94c69e7c-20261117/results.md | 2026-11-17T09:54:06.932Z |



## 结论

三项修复和六项验收全部通过，代码及文档推送独立fork分支。完整报告 /Users/lixueduan/17x/tmp/kc-fix2/output/kc-upgrade-94c69e7c-20261117/results.md
