# Round 4 — Remaining Gaps (carried over, do later)

来源：round 2/3 覆盖对账（见 core-feature-checklist.md）。按建议执行顺序排列。

## 高优先（核心场景缺口）
1. **kcctl join 单节点纳管**：平台运行中新纳管一台无 kc 组件的节点（非集群 add nodes）。
2. **kc 平台自升级**：`kcctl upgrade`（binary/online 两模式）升级运行中的 server/agent/console。
3. **operation 交互面**：`kcctl get operation --watch`、`operation cancel`、`operation logs --follow`
   在线验证（retry 已验；cancel 只有单测）。
4. **delivery-policy 命令组**：template → 自定义版本白名单 → apply → 集群创建受约束验证
   （目前只吃过 deploy 注入的默认策略；策略生效路径未做定制变更测试）。
5. **备份保留轮转**：cronbackup maxBackupNum 超限时旧备份自动删除（含 NFS 文件）。

## 中优先（能力面补全）
6. **master 节点增删 + master↔worker 角色转换**（convertNodes）。
7. **S3 型 backuppoint**（起一个 minio 即可与 fs 型对称）。
8. **https + 凭据的私有 package registry**（自签 CA、username/password-file；现在全程 http）。
9. **离线 registry bundle export/import** 在真机重跑（CI 已绿，lab 未复跑）。
10. **用户/RBAC**：建用户/角色、console 登录权限面（API 测试全程用 admin 证书）。
11. **cronbackup disable/enable 子资源**与删除幂等。

## 低优先（声明性/长尾）
12. **console UI 端到端**：fork console 分支 `fix/operation-v2-console` 配镜像跑核心页面
    （集群列表/创建向导/操作详情/备份）；任务失败展示（此前用户明示暂缓）。
13. **arm64 运行时**：需要 arm64 机器（当前仅 build 验证）。
14. **iptables proxyMode**、**docker CRI**（AllowedCRIType 里保留但无资源包，需确认是否应下线）。
15. **较大规模**：>3 worker 的批量 add/remove（锁竞争、step barrier 性能）。
16. **网络故障注入**：watch 断线 410 relist、agent 掉线 Running Task 超时收敛
    （round 2 有自然事故样本，无系统性注入）。
