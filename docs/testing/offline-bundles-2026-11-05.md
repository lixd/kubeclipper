# 独立 Registry 小包与多架构离线交付验证

实现提交为用户 fork 的 `63d057378ce74a16aa78d00d8c005868866f2f89`。
本轮验证 kcctl 的导出、Registry 自举和离线导入；不代表完整三机建群或全量用例重跑。
未向上游写入，也未发布新的正式 Release。

## 使用流程

联网端准备对应版本的 kcctl 二进制和 ReleaseManifest，然后执行：

```bash
kcctl registry export --manifest release-manifest.yaml --output kc-offline.tar.gz
```

将 kcctl、`kc-offline.tar.gz` 和同目录的 `registry-3.1.1.tar.gz` 带入离线环境：

```bash
kcctl registry deploy --node 10.0.0.10 --pk-file ~/.ssh/id_ed25519 \
  --bundle registry-3.1.1.tar.gz
kcctl registry sync --bundle kc-offline.tar.gz \
  --target 10.0.0.10:5000/kubeclipper-release --target-scheme http
```

之后通过正常的 `--package-registry` 安装平台和集群。
导出、导入和 Registry 自举只依赖 kcctl，不要求 Docker、Skopeo 或 Python。
chart 随所属 package 镜像交付。原始索引中的全部平台、manifest 和压缩层均保留；
本地内容缺失或损坏时直接失败，不回源。旧 `registry push` 及散装二进制、Docker
归档等 Registry 自举入口已经移除。

## 输入与源码

- 制品清单为既有 `v2.0.11` 候选，`sourceRevision` 为
  `9cddb493b7189d7fe54c845ccc73b0785235e8c3`；包含 12 条 package 和 98 条 runtime
  记录，合并为 61 个 tag、35 个仓库。该制品 revision 与本轮 kcctl 实现提交不同。
- `sh-dev` 使用独立源码目录和 Docker Go 1.26.1 构建与测试；逐文件核对了源码快照。
  既有 `.gitignore` 修改和 `.comet/` 保留，不进入本轮提交。
- Linux amd64 kcctl SHA256 为
  `1e74e67557c549f41f25c377eda4688f064500bb4c0c89c5e73c24223ef02688`。
  Linux arm64 构建也通过，ELF machine 分别为 62、183；这不代表 arm64 真机运行。

## 已验证结果

- `go test ./pkg/delivery/... ./pkg/cli/registry/...` 的 9 个包通过；goimports 无差异，
  对本轮变更执行的 golangci-lint 2.12.2 为 0 问题。另通过 Bash、workflow YAML、
  packaging 外部依赖检查和 `git diff --check`。
- 回归包含真实测试 Registry 双架构往返、原始 Docker index 媒体类型保留、foreign
  layer 内容上传、重复跳过、tag 冲突，以及缺失、损坏、截断、危险路径和取消场景。
  导出后实际关闭源 Registry，并禁止源传输，随后完成本地导入。
- 在 `sh-dev-3` 的专用 5617 端口，用独立小包成功自举 Registry，健康检查为 200。
  `systemd-run` 对相关 kcctl 进程设置 `IPAddressDeny=any`、
  `IPAddressAllow=localhost`；已实测本机可达、公网 ACR 不可达。
- 全量首次离线导入为 **61 copied / 0 skipped**。独立核对原始导出索引中的全部 61
  个 tag、277 个 manifest、831 个仓库 blob 链接，并完整 GET 和 SHA256 核对 698
  个唯一 blob，共 **7,943,959,692 字节**。全部 tag 均含 Linux amd64、arm64。
- 最终实现提交再次从上述本地 Registry 原生导出全部内容，同时继续限制公网访问。
  临时 OCI Layout 使用专用 `TMPDIR=/dev/shm/kc-offline-bundles-20261105`，以减少测试机
  磁盘等待；首次导入使用普通磁盘临时目录。没有使用模拟结果或改写制品摘要。
- 最终生成的小包再次自举 Registry 成功；最终大包再次导入为 **0 copied / 61 skipped**，
  全部 tag 摘要一致。该轮仍仅允许本机网络，未读取公网源。
- 小包内实际 Registry 二进制的 ELF 架构、包内平台和文件摘要一致：amd64 为 62，
  arm64 为 183。数据 package 的镜像 config 平台字段为空，平台由 OCI index 和
  `kc-package-manifest.json` 提供；首次辅助检查误把 config 字段作为必填，核对现有
  publisher 后修正了辅助检查，没有修改制品或产品校验来规避失败。

最初大包的冗余重复导入在解压阶段主动取消，后续改用最终实现重新导出的包验证重复导入。
该取消不计为产品失败，也不计为一次完成的重复导入。

## 包体积与证据

最终实现导出的两份归档为：

| 归档 | 字节 | 体积 |
|---|---:|---:|
| `registry-3.1.1.tar.gz` | 34,613,370 | 33.01 MiB |
| `kubeclipper-offline-v2.0.11.tar.gz` | 7,625,408,839 | 7.10 GiB |

完整包包括源索引中除 Linux amd64/arm64 外的已有平台，以保留原始 digest。
归档保留在 `sh-dev-3:/root/kc-offline-bundles-20261105/roundtrip/`，没有复制到本地 Mac。
这些是交付链路测试物料；平台 package 仍为上述既有候选，kcctl 单独使用本轮构建。

证据位于 [offline-bundles-20261105](evidence/offline-bundles-20261105/)：

- [独立全内容核对](evidence/offline-bundles-20261105/import-proof.json)与
  [原始根索引](evidence/offline-bundles-20261105/export-index.json)。
- [Registry 二进制架构与摘要](evidence/offline-bundles-20261105/registry-architectures.json)、
  [kcctl 构建架构](evidence/offline-bundles-20261105/build-architectures.json)与
  [检查结果](evidence/offline-bundles-20261105/checks.txt)。
- [最终归档 SHA256](evidence/offline-bundles-20261105/archives.sha256)、
  [最终重复导入日志](evidence/offline-bundles-20261105/roundtrip-sync.txt)与
  [现场恢复核对](evidence/offline-bundles-20261105/restore-proof.json)。
- [源码逐文件摘要](evidence/offline-bundles-20261105/source-files.sha256)覆盖实现提交的
  1,441 个文件，排除用户原有 `.gitignore` 修改；清单 SHA256 为
  `16bb4c46c311123bffd613bdecad08d1bf2958fea9c7118da78c61b60602eed6`。

## 现场恢复与边界

专用 `kc-registry.service` 已停止、禁用和删除；原 Registry 二进制已恢复并按摘要比较。
共享 5003、5605 Registry 仍返回 200，既有 `kubeclipper-registry-r3.yaml` 保留。
CLI Registry 配置恢复为原有条目，并与恢复基准逐字节比较。首轮备份路径选择错误，
该基准由保留下来的原条目移除本轮新增项、恢复原 current 后重建；它不是测试前独立保存
的原始字节备份。没有把这一项描述为独立原始备份校验。

arm64 仅验证构建、平台选择、包内二进制架构及全量内容完整性，未执行 arm64 真机运行。
本轮没有重部署 KubeClipper 平台或 Kubernetes 集群，也没有重跑完整功能台账。
