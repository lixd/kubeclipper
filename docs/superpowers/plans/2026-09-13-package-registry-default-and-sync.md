# 实现计划：package-registry 默认值 + kcctl registry sync

日期: 2026-09-13 ｜ 分支: fix/oci-inventory-missing-repo → fork feat/oci-operation-v2-migration

## 目标

1. `kcctl deploy` 不指定 `--package-registry` 时默认官方 `ghcr.io/kubeclipper/kubeclipper`（在线零配置）
2. `kcctl registry sync`：release-manifest 驱动的仓库间镜像工具（内嵌 go-containerregistry，无外部 skopeo/crane 依赖）

## Feature A：默认值（提交 1）

决策记录：
- **所有构建一律默认 ghcr**（不做 release/dev 构建区分）：dev 构建 ghcr 上无对应物料时，由下述 precheck 以清晰报错兜底；实现简单，不引入构建矩阵耦合
- 默认值 ≠ 静默回退旧静态服务：仍是单一 OCI 路径，符合 P0-06 原则

改动点：
1. `pkg/delivery/registry`：`const DefaultPackageRegistry = "ghcr.io/kubeclipper/kubeclipper"`
2. `cmd/kcctl/app/options`：NewDeployOptions 注入默认；`deploy.go` 空值校验改为"空则置默认"
3. deploy 前新增 **PACKAGE-REGISTRY PRECHECK**：有界超时（15s）对 `<registry>/kubeclipper/packages/bootstrap/kubeclipper` 做一次 tags 探测；失败报错格式：
   `package registry "<addr>" (<default|flag>): <err>; offline environments: mirror first (kcctl registry sync) or pass --package-registry`
4. 生效值（标注 default/flag/config）打入 deploy 日志
5. 测试：默认应用、显式覆盖、探测失败文案；README/README_zh 部署示例更新

## Feature B：registry sync（提交 2 引擎 + 提交 3 CLI）

B1 引擎 `pkg/registrysync`：
- 输入：release manifest（文件）、source/target registry config（各自 scheme/auth/超时）、arch 过滤、并发度、重试
- 对每个 artifact：source 引用 → crane/remote 全量 copy 到 target → **digest 比对**（不等即失败）
- 输出结构化结果（每 artifact：ok/skipped/failed、digest）；任何失败 → 非零退出
- 不猜测清单：manifest 是唯一权威来源（复用 `pkg/delivery/releasemanifest` 类型）

B2 CLI `kcctl registry sync`：
```
kcctl registry sync --manifest <release-manifest.yaml> \
  [--source <registry-prefix>（默认取 manifest.registries.package/image）] \
  --target <registry-prefix> \
  [--source-username/--source-password-file/--target-.../--*-scheme/--*-skip-tls-verify] \
  [--arch amd64|arm64] [--concurrency 4] [--dry-run]
```
- `--dry-run` 只解析+打印计划；auth 两侧独立；参数风格对齐现有 deploy/join
- 本期不做：GitHub Release 资产自动获取（`--release v2.0.0` 语法糖）、bundle 文件模式（`--bundle-out/in`）——列为后续
- 脚本 export/import 保留（CI 仍用），用户面工具收敛到 kcctl

B3 测试：
- 引擎：httptest + `pkg/registry` 内存 registry 做 fake source/target，覆盖 copy+digest 校验、缺失失败、arch 过滤
- CLI：flag 绑定/manifest 解析/ dry-run 输出

## 验收

- A：省略 `--package-registry` 的 deploy 在可出网环境直接成功；断网/错误地址在 precheck 阶段秒级失败且文案含出路
- B：一条命令把 qualification/正式仓库全量镜像到私有 registry（fork CI 环境可先自测：ghcr → ghcr 另一 namespace）
