# OCI 获取实现与验证记录

本文记录提交前候选实现的验收结果，不替代已定稿的 `oci-acquisition-design.md`。Git 交付不代表已完成平台部署或集群创建验证。

## 实现范围

- 使用现有 containerd v1.6.19 独立本地 content store；不依赖 daemon，不新增依赖。
- 已验证原始平台 manifest、压缩层和按选择集合生成的提取快照分开保存。原始内容以 SHA256 寻址；快照标识包含平台 manifest digest、排序后的选择集合及提取格式版本。
- 从原 Registry blob 入口获取，支持严格 Range 续传；每层单次调用最多五次内容尝试，首次计入，1 秒指数退避并加抖动，等待最多 30 秒。取消或可恢复断流保留 ingest，长度或摘要错误不能提交。
- 只扫描一次本地 rootfs tar；先完成所选文件及摘要校验，再发布整个目录。manifest 可以位于 tar 末尾。
- lifecycle 共享锁覆盖 Fetch、SSH 分发、解包和 Helm 使用；幂等 Release 后才允许独占清理。快照锁、blob 锁位于清理范围之外。
- deploy 只提取所需平台资产，排除 kcctl；join 只提取 agent。升级 CLI 同样持有返回结果到分发结束。Calico Helm 使用返回的精确 Chart 路径，获取和消费处于同一保护期。
- Registry 引导安装使用独立 `extractRegistryBinaryToTemp` 路径，首期仅审计，未改造。原生 Helm OCI、registry sync、运行时镜像和索引读取未改写。

## 缓存布局与策略

缓存根目录保持已有规则：显式 `KUBECLIPPER_CACHE_DIR` 优先；root 默认 `/var/lib/kubeclipper/cache`；普通用户使用系统用户缓存目录下的 `kubeclipper`。

```text
<cache>/
  content/blobs/sha256/<digest>          # 完整、长度和摘要通过的原始内容
  content/ingest/<transaction>/         # containerd 管理的可恢复事务
  packages/<kind>/<name>/<version>/<os-arch>/<manifest-digest>/<selection-hash>/
    contents/<selected-file>
    receipt.json                       # 完整发布标志及校验信息
  locks/<key-hash>.lock                 # 稳定锁文件，清理不删除
  access/<object-hash>                  # 使用记录，不改不可变快照
```

缓存入口为 0700，提取文件及本层维护的元数据为 0600；content store 内部目录由现有库创建，受私有父目录保护。原始 blob 不通过硬链接暴露给消费者。

完整缓存没有按时间过期；20 GiB 是保留软预算。获取前和结果释放后尝试 LRU 回收，活动消费者使清理跳过。本次已知物料在获取前受到保护。半包和崩溃提取暂存保留 7 天；空间预检保留 1 GiB，仍明确传播实际写入错误。卸载仅清理提取目录，保留原始 content store。

## 读取上限及实际物料

| 对象 | 上限 | 处理方式 |
| --- | --- | --- |
| 平台 manifest / 包 manifest / 快照 receipt | 4 MiB | 有界读取和解析 |
| 单个选中安装文件或压缩包 | 8 GiB | 流式落盘及校验 |
| 解压后单文件 payload | 16 GiB | 流式摘要校验 |
| 单个压缩层及整个展开 rootfs | 各 32 GiB | 描述符预检及展开读取上限 |

依据真实 bootstrap v2.0.9 amd64 物料：压缩层 134,400,499 字节；提取 server 为 112,795,829 字节，agent 为 86,930,992 字节。上限为解析与展开的保护边界，不是支持版本清单。边界测试使用超限 tar header、受限复制和损坏包，未为测试分配几十 GiB 文件。20 GiB/LRU 策略用缩小预算的同一实现验证；1 GiB 余量在真实文件系统执行预检，不宣称杜绝 ENOSPC。

## 验证环境与证据

重型测试在 sh-dev 的专用 `/tmp/kc-oci-acquisition-20261030` 目录执行。源码包含本地未提交及新增 Go 文件，按逐文件 SHA256 清单核对；镜像为已存在的 Go 1.26.1 Alpine 工具环境。未覆盖已有远端工作区，未更改平台配置、节点或用户集群。

最终候选：`candidate-6`，源码清单 `source-manifest.json`。证据位于同目录旁的 `evidence/`。源码清单 SHA256：`6391b50b7bb63c2573c1428843a3ab9abe1a8cb02c65e9a9456ef0bedab2e544`，共 762 个源文件。普通回归、竞态检查及 kcctl/server/agent 构建均通过。

| 检查 | 结果与记录 |
| --- | --- |
| kcctl、server、agent 构建 | 通过；`candidate-6-build.log`，退出码 0 |
| 九个受影响包的普通回归 | 通过；`candidate-6-tests.log`；fetcher 65.78 秒 |
| fetcher/downloader 竞态检查 | 通过；`candidate-6-race.log`；fetcher 117.26 秒 |
| HTTP/HTTP2 故障 | 通过；断流、Range、416、忽略 Range、摘要失败、五次耗尽及跨调用取消恢复 |
| 缓存与文件系统 | 通过；并发共用层、跨进程锁、延迟消费与清理、LRU、7 天半包、无网络重建、`/dev/full` 写入失败 |
| 进程退出边界 | 通过；半包及完整 ingest 未提交、提取完成未发布、发布后未 Release |
| HTTP/TLS 来源规则 | 通过；Basic、错误认证、自定义 CA、跳过校验、未信任证书、路径前缀及跨主机不转发凭据 |
| 最终候选真实 GHCR | 通过；`candidate-6-ghcr-proxy.log`，冷获取 6 分 37 秒，暖缓存约 0.786 秒 |

独立验收第一轮发现 A2/A7/A8 缺口，现已修正并新增回归：默认 File 与显式同名先归一化；metadata-before-data 退出窗口在无 payload 时明确记录并重建；整个 ingest 目录计费/过期，包括空目录；旧包的完整和中断目录都在排他锁下回收，保留原生 Helm 已有来源标记缓存。

候选六第一次普通回归误用 host 网络，假 API Server 绑定现有 6443 端口失败。未改变占用服务；恢复容器隔离网络后，同一候选完整回归通过。原失败日志为 `candidate-6-tests-host-network-failed.log`。候选五在替代候选生成后停止 GHCR 测试，不记为通过。

早期候选测试有失败和修正记录；候选三普通回归在并行构建/竞态测试期间于 300 秒总时限超时，堆栈位于 `fsync`。相同代码单独重跑通过（fetcher 115.8 秒）。这些失败和超时不算作通过，原始日志保留。

## 网络边界

直接连接 GHCR 的测试在 12 分钟超时，不能记作成功。使用本机 7890 代理经 sh-dev 回环 SSH 隧道的候选一测试完成真实完整层获取、server/agent payload 校验和暖缓存复用；冷获取 6 分 33 秒，暖获取约 0.86 秒。最终候选六也已完成相同固定 digest 的完整拉取、提取校验和暖缓存验证。

这些测试验证物料获取，不代表本次已执行平台安装或创建集群。HTTP/HTTP2 故障服务器用于明确验证断流、续传和失败语义，与真实 GHCR 传输证据分别记录。

## 仍未覆盖的环境边界

未执行本次代码的平台部署、集群创建、真实断电或磁盘硬件故障；进程退出测试不等于断电恢复证明。未分配 20 GiB 或 32 GiB 大文件做边界测试。Registry 引导安装、sync 全图复制、原生 Helm OCI 和运行时容器镜像不是本次完成范围。独立 Verifier 的结果另由 Comet Runtime 记录。
