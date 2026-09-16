# KubeClipper 核心功能测试清单（按四大块，2026-09-16）

用途：round 4+ 覆盖基准。状态：✅ 已实测 · ⚠️ 部分 · ❌ 未测。

## 1. 平台本身的部署（deploy）

### 1.1 部署物料来源（三种方式）
| 功能 | 状态 | 备注 |
|---|---|---|
| 在线部署：默认 ghcr 直装 | ⚠️ | 预检✅；真从 ghcr 直装未走（都先 sync） |
| 半离线：ghcr → `kcctl registry sync` → 本地仓库 → deploy | ✅ | R3 全链路 |
| 纯离线：bundle export → 拷贝 → import 进仓库 → deploy | ❌ | 脚本 CI 绿，真机未演练（带箱入网场景） |
| 自定义私有仓库：http | ✅ | R3 |
| 自定义私有仓库：https + 自签 CA | ❌ | |
| 仓库带账号密码认证（deploy 侧 username/password） | ❌ | sync 源认证✅，部署侧未测 |

### 1.2 部署拓扑
| 功能 | 状态 | 备注 |
|---|---|---|
| 单 server + 多 agent（3 台） | ✅ | R2/R3 |
| server 与 agent/k8s 节点混部同机 | ✅ | R2/R3（dev-2） |
| **多 server HA（3× kc-server，etcd 奇数集群）** | ❌ | **头号缺口** |
| console 组件部署与访问 | ⚠️ | 服务起✅；页面未验 |

### 1.3 容错与增量运维
| 功能 | 状态 | 备注 |
|---|---|---|
| 预检失败引导（不可达/缺包报错） | ✅ | R2/R3 |
| etcd 冷启动竞态（写探针+重试） | ✅ | R3 |
| clean --all 后重 deploy 幂等 | ✅ | R3 ×2 |
| 不 clean 直接重复 deploy | ❌ | |
| `kcctl join` 向运行中平台纳管新节点 | ⚠️ | 集群 add nodes 隐含走过，独立命令未测 |
| `kcctl clean` 单节点/部分清理 | ❌ | |
| **`kcctl upgrade` 平台自身升级**（--pkg binary / --online） | ❌ | **缺口** |
| `kcctl doctor` | ✅ | R3 |

## 2. 集群相关操作

### 2.1 创建集群
| 功能 | 状态 | 备注 |
|---|---|---|
| 1 master + 2 worker | ✅ | R3 |
| 1 master + 1 worker / 单节点（master 兼 worker） | ❌ | 最小规格未测 |
| 3 master HA（含 lvscare workerNodeVip） | ✅ | R2 |
| 版本矩阵 v1.35.8 / 1.36.4 / 1.37.0 | ✅ | R2/R3 |
| CRI containerd 1.7.29 / 2.2.4 | ✅ | R2/R3 |
| CNI calico v3.29.6 / v3.31.5 | ✅ | R2/R3 |
| 镜像/物料 100% 来自指定仓库（离线保证） | ✅ | R3 验证法可复用 |
| proxyMode ipvs | ✅ | R2/R3 |
| proxyMode iptables | ❌ | |
| 矩阵外版本/降级/同版本拒绝 | ✅ | R3 |

### 2.2 节点操作
| 功能 | 状态 | 备注 |
|---|---|---|
| worker 添加 | ✅ | R2/R3（plan 不变性同步验证） |
| worker 移除（含不可 drain 节点容错） | ✅ | R2/R3 |
| **master 添加 / 移除** | ❌ | 缺口 |
| master↔worker 角色转换（convertNodes） | ❌ | 缺口 |
| 节点 disable / enable | ❌ | |
| 节点失联后操作收敛（agent down） | ⚠️ | R2 自然样本，无系统注入 |

### 2.3 升级
| 功能 | 状态 | 备注 |
|---|---|---|
| 真实滚动升级 1.36.4→1.37.0（master→worker drain） | ✅ | R2 + R3 新 tip 复验（修 3 bug） |
| 升级中途失败 → op retry | ❌ | |
| 升级失败 → 集群状态恢复（reset status） | ✅ | R3（逃生门实际使用） |

### 2.4 证书
| 功能 | 状态 | 备注 |
|---|---|---|
| **集群证书更新（/certification）** | ❌ | 缺口 |
| agent 重新签发（重 join） | ⚠️ | join 未独立测 |

### 2.5 删除集群
| 功能 | 状态 | 备注 |
|---|---|---|
| 正常删除（含与 SyncKubeConfig 并发，死锁回归） | ✅ | R2/R3 |
| InstallFailed 状态删除 | ✅ | R3 |
| 删除失败 TerminateFailed → 重试 | ✅ | R2 |
| 有备份时删除保护（引导先删备份） | ✅ | R3 |
| 删除后 ops 账本清理 | ✅ | R3 |

### 2.6 备份与恢复
| 功能 | 状态 | 备注 |
|---|---|---|
| backuppoint：fs 型 | ✅ | R3 |
| backuppoint：**S3 型（minio）** | ❌ | 缺口 |
| 手动备份 → 恢复（etcd 回滚证据） | ✅ | R2/R3 |
| 备份删除（文件连带清除） | ✅ | R3 |
| cronbackup runAt 单次触发 | ✅ | R3 |
| cronbackup **真实周期命中** | ⚠️ | 只验调度时间滚动 |
| cronbackup enable/disable 子资源 | ❌ | |
| **maxBackupNum 超限自动轮转** | ❌ | 缺口 |
| 恢复后集群可用性（addons/节点完整） | ✅ | R3 |

## 3. kcctl 命令覆盖（每条至少跑通一次核心路径）

| 命令 | 子命令 | 状态 |
|---|---|---|
| deploy / clean / doctor | — | ✅（clean 单节点模式 ❌） |
| join | — | ❌ 独立未测 |
| create | cluster（CLI 方式） | ⚠️（建集群走的 API，CLI 入口未跑） |
| create | registry / role / user | ❌（registry ✅） |
| delete | cluster / 其他资源 | ⚠️（CLI `kcctl delete cluster` 未跑，API ✅） |
| get | clusters/nodes/operations（含 --watch、分页） | ⚠️ watch 未长跑 |
| operation | list/describe/logs/retry | ✅ |
| operation | **cancel** | ❌ 仅单测 |
| cluster | upgrade | ✅ R3 |
| set | cluster | ❌ |
| drain | （节点驱逐独立使用） | ❌ |
| upgrade | **平台自升级** | ❌ |
| registry | sync | ✅ R2/R3 |
| registry | list/deploy/clean/push（本地仓库管理） | ❌ |
| resource | list/inspect/refresh | ❌ |
| delivery-policy | template/get/apply/diff/validate | ❌（默认策略注入✅，命令面全未测） |
| status / config / login / version / completion | — | ⚠️ version/config✅，login 用证书绕过 |

## 4. 其他常用功能

| 功能 | 状态 | 备注 |
|---|---|---|
| addons：nfs-csi 安装/读写/卸载 | ✅ | R2/R3 |
| addons：metallb L2 | ✅ | R3 |
| addons：metallb BGP | ❌ | 需邻居环境 |
| addons：同组件多实例（scName 唯一性） | ❌ | |
| addons：uninstall 容错（空 config/参数校验跳过/ErrIgnore 链） | ✅ | R3 三修复合验 |
| 可观测：operation logs、失败原因展示（API 侧） | ✅ | R3 |
| console UI 端到端（含任务失败展示） | ❌ | fork console 分支未配镜验证 |
| 用户/RBAC/OAuth（users/roles/tokens/鉴权拦截） | ❌ | 平台既有能力，建议与 UI 一起单独立项 |
| 集群模板 templates | ❌ | 同上 |
| DNS domains/records | ❌ | 同上（与 apiserver 域名发布相关） |
| cloudproviders 云厂商纳管 | ❌ | 同上 |
| Web 终端 / pod exec | ❌ | UI 侧 |
| kubeconfig 下载接口 | ⚠️ | 隐式在用 |

---

## Round 4 优先级建议（对应缺口）
1. **1.2 多 server HA 部署**（三台机器正好够）
2. **2.6 maxBackupNum 轮转 + cron 真实周期**（低成本，改 schedule 等触发）
3. **3. delivery-policy 命令组 + 自定义策略生效**（OCI 主题核心）
4. **1.1 纯离线 bundle 真机演练**
5. **2.2/2.4 master 增删 + 证书更新**
6. **1.3 kcctl upgrade 平台自升级**
7. 3 章其余未测命令扫一遍（cancel、set、drain、resource、registry 子命令）
8. 4 章 UI/RBAC/模板/DNS 单独立项
