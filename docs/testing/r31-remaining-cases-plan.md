# R31 遗留 Case 验收计划（2026-09-27）

## 基线与边界

- 以 R30 状态报告和核心功能清单为准：174 ✅、10 ⚠️、10 ❌；工作树为
  `lixd/kubeclipper` fork 的 `feat/oci-operation-v2-migration`，起始 HEAD `bd269340`。
- sh-dev-2/3/4 已按用户指令执行一次 `kcctl clean -A -f`。该命令按部署配置删除
  `EtcdConfig.DataDir`；本分支默认值为 `/var/lib/kc-etcd`，R29 报告记载的旧目录现已不存在。
  不尝试恢复旧平台数据。dev-3 的 `kc-oci-r3-registry.service` 仍 active，HTTP `/v2/` 返回 200。
- 只在 fork 工作树修改代码和文档。共享 Registry `172.16.131.146:5003` 只允许追加不可变 tag；
  不改动其他团队物料、不再清理平台 etcd 数据目录、不把凭据写入仓库或报告。按既有约束不安排
  arm64 真机验收，也不做外部 stable tag / release 发布。

## 执行顺序

| 阶段 | Case | 处理方式 | 完成证据 |
|---|---|---|---|
| 1. 恢复验收环境 | 通用前置 | 核实清理状态和 Registry 健康；在已清空的平台数据目录上使用 fork 构建恢复三节点平台；若需要发布内部 bootstrap 制品，先确认 tag 未存在且只追加新 tag。后续不用全平台 clean 收尾 | fork revision、制品 digest、平台版本、3/3 agents、doctor、集群为空 |
| 2. API 修复 live 复验 | 4-09、4-10、4-15 | 部署 R30 fork 修复；覆盖模板 NotFound 三路由、模板实例化/引用删除保护、DNS A/AAAA 与非法记录负向矩阵、PlatformSetting 权限/敏感字段/公钥与轮换边界 | HTTP 状态与错误体、临时对象恢复、敏感值不出现在响应/日志 |
| 3. Console 部署和端到端 | 6-13、4-07 | 构建并部署 Console fork；确认 retired addon 不再出现在 UI；以临时账号运行 UI 登录、集群创建、Operation 成功/失败展示、升级/备份/删除及清理 | Console commit、bundle 静态检查、浏览器步骤与对应 API/Operation 状态 |
| 4. 活跃集群扩展路径 | 4-09、4-12a、4-12b、2.1-15 | 使用隔离临时集群验收模板引用与删除保护、终端鉴权/resize/断连重连、Pod exec 选择/鉴权/断连；若可获得第三方 CNI 制品则验证跳过 CNI 后安装并恢复 Ready | 节点与 Pod 状态、WebSocket/exec 结果、Operation、删除后零临时对象 |
| 5. 部署来源矩阵 | 1.1-01、3-01 | 在空白节点先测默认 GHCR 在线部署（按用户提供的本机 7890 代理建立 SSH 转发）；另做断公网条件下的完整纯离线首次部署 | 物料来源与 digest、代理/断网证据、部署/建群/删除结果、doctor 和清理 |
| 6. 环境依赖评估与可行子项 | 6-06、6-10、6-11、2.1-32、4-03、4-11、7-01 | 核实 OS 支持政策、主机架构、IPv6/路由、BGP 邻居、CloudProvider fixture、OIDC IdP 是否实际可用；只有真实环境满足前置时才跑验收，不用模拟结果替代真实路径 | 前置环境清单；可运行项保存完整证据，不满足项记录具体缺失条件。arm64 真机按用户既定约束不执行 |
| 7. 产品语义缺口 | 2.2-04、4-04、2.6-11 | 检索现有 API/产品文档，复核当前行为并整理最小语义决策项；在没有明确产品约束前不擅自扩展转换事务、多实例身份或 standalone extension 入口 | 当前行为复现/代码证据、所需产品决策和后续实现边界 |
| 8. 收尾 | 全部 20 项 | 清理临时集群、账号、脚本、配置和凭据；核实 Registry 服务、Registry 内容、`/var/lib/kc-etcd` 和远端状态；同步 checklist、缺口台账及 R31 状态报告 | 每个可闭环 Case 指向命令/响应/Operation/日志；剩余项明确标记环境或产品前置 |

## 停止条件

- 共享 Registry 上目标 tag 已存在且 digest 不同：不覆盖，改用下一个未占用的新 tag。
- 测试会要求修改其他团队物料、`/var/lib/kc-etcd` 或外部 stable 发布：立即跳过该动作并记录原因。
- 产品语义、第三方凭据或真实网络/硬件前置不可得：保留未完成状态，不用单测、dry-run 或模拟服务宣称真机验收通过。
