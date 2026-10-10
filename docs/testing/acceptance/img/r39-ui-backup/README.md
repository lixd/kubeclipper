# R39 Console UI 备份全链路取证截图（4-07 收尾，2026-10-10）

浏览器自动化（IAB）经 Mac→dev-2 端口转发访问 Console（caddy :80），以平台管理员用户
登录后完成"备份空间创建 → 集群绑定 → 触发备份 → 恢复到可用 → 删除备份"的 UI 全链路。
目标集群为一次性探针 `r39-bak`（1M/1W：dev-3 + dev-4；k8s v1.37.0 / containerd 2.2.4 /
calico v3.31.5；离线，镜像仓库 kc-5003）。

| 文件 | 内容 |
|---|---|
| 01-backuppoint-list-r39-fs.png | 备份空间页：UI 创建 FS 类型 `r39-fs` 成功（`storageType=fs`，`fsConfig.backupRootDir=/opt/kc/backups`），列表 1 项 |
| 02-cluster-detail-backupspace-r39-fs.png | `r39-bak` 详情"详情"tab：**备份空间 `r39-fs`**（经集群"编辑"弹窗绑定后生效，原为 `-`）；同屏可见 k8s v1.37.0 / containerd 2.2.4 / 控制节点健康 |
| 03-backup-tab-available.png | 集群详情"备份"tab：备份 `r39-bak-r39-backup-1-p2k62k`（备份空间 `r39-fs`、K8S 版本 v1.37.0、状态 **可用**、创建时间 2026-10-10 22:29:12） |
| 04-oplog-backup-recovery-succeeded.png | 集群详情"操作日志"tab：`RecoveryCluster 成功`（22:31:37）与 `BackupCluster 成功`（22:29:12）等 4 条 v2 operation 正常渲染 |

## 对应 API / 节点侧事实（同一时间窗）

- Backup 对象（`GET /clusters/r39-bak/backups`）：`backupStatus.status=available`，
  `backupFileSize=10952736`，`backupFileMD5=eed5815b…`，`backupPointName=r39-fs`，
  `clusterNodes={dev-3, dev-4}`，label `kubeclipper.io/cluster=r39-bak`。
- 备份文件实数存在于 dev-3 `/opt/kc/backups/r39-bak-r39-backup-1-p2k62k`（10.9 MB）。
- Operations：`BackupCluster 6e0cf31d…` Succeeded（14:29:12Z 创建）、
  `RecoveryCluster 6a184179…` Succeeded（14:31:37Z 创建，恢复后集群回 Running）。
- 删除保护：备份存在时 `kcctl delete cluster` 返回 `please delete the cluster backup file first`；
  UI 行内"删除"删除备份后集群正常删除。

## 清理（轮次收尾）

探针全部清除：集群 `r39-bak`（0 残留）、BackupPoint `r39-fs`（API totalCount=0）、
dev-3 备份文件、临时用户 `r39-admin`/`r39-ui`（平台仅剩 `admin`）、Mac 端口转发 18089 已关、
两端 `/tmp/r39-*` 与密码文件已删、dev-4 残留空 `/etc/kubernetes` 目录已清。

## 附带观察（非 4-07 阻塞）

内置聚合角色（如 `cluster-manager`）在**授权期不展开** `kubeclipper.io/aggregation-roles`
注解：`pkg/authorization/rbac/rbac.go` 的 `getRoleReferenceRules` 仅取 `globalRole.Rules`，
而内置角色的 `rules` 字段只有基础规则（聚合注解在创建/更新 API 路径才会展开进 `rules`）。
因此非 admin 用户访问备份空间 API 得 403。本轮以管理员用户完成 4-07 UI 验证；该行为属
RBAC 权限模型问题（有独立 Case 7 章覆盖范围），不影响 4-07 结论。
