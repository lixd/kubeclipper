---
generated_from_state_version: 27
---

# Verification

## Current result

- Result: **Archived**
- Verification status: **Checks completed; result confirmed**
- Goal cycle: 2
- Iteration: 4
- Verifier attempt: 1
- Completed: 2026-10-29T08:43:40.373Z
- Summary: Independent readonly A1-A10 verification passes for committed source candidate. Source bytes match tested manifests, remote receipts match, and fresh candidate gate is now nonnull after normal implementation commits; no Runtime evidence or tool source was modified.

## Acceptance

| ID | Result | Source | Criterion | Reason |
| --- | --- | --- | --- | --- |
| A1 | passed | brief.md | A1：下载脚本默认从 GitHub 获取 kcctl，--mirror cn 从国内站获取；KC_REGION、KC_DOWNLOAD_URL 及持久镜像 env 入口删除，帮助示例同步。 | Inspected installer GitHub default, invocation-only --mirror cn and removed region/download inputs. Fresh Runtime installer check passed for committed source. |
| A2 | passed | brief.md | A2：deploy 默认使用官方 GHCR，指定 --package-registry 则使用该仓库；正确登记含路径、认证、TLS 配置的默认 Registry 并保存明确引用；重复部署不删除重建或静默覆盖冲突资源。 | Inspected deploy explicit flag override, default registry seeding with separated host/path, credential digest and conflict rejection. Matching source deploy tests retained. |
| A3 | passed | brief.md | A3：Web、CLI、API 新建集群共享默认解析；未指定时解析平台默认并保存引用，显式选择优先；无有效 Registry 明确失败，不按唯一仓库或上游回退。 | Inspected CompleteClusterImageRegistry and handler invocation: explicit source wins; omitted uses persisted default; missing default errors; concrete reference persisted. FE store consumes API default. |
| A4 | passed | brief.md | A4：Kubernetes、pause、Calico 及此次安装涉及的平台组件使用统一选定仓库，路径与发布同步布局匹配；安装包和 Chart 仍使用平台资源仓库；已知资源缺失、版本架构不匹配明确失败，不伪造完整性状态。 | Inspected metadata propagation to Kubernetes, Calico, containerd pause and NFS/MetalLB; chart package resolution remains delivery registry. Read actual single-node proof: Ready node and Pods using internal prefix. This is not pure-online/airgap proof. |
| A5 | passed | brief.md | A5：创建与确认页只有一个明确镜像仓库，默认选中平台默认；删除组件默认来源、在线离线、组件级及 Package Registry 说明；问号仅为“用于拉取 Kubernetes、Calico 等组件的容器镜像。”。 | Inspected current frontend registry wiring and concise Chinese tooltip, readonly upgrade source, error handling; read matching browser receipt showing one required Registry and full address. Source manifest matches 104-test candidate. |
| A6 | passed | brief.md | A6：Registry UI 和 CLI 使用完整地址含协议端口路径，解析展示一致；CLI --address 替代旧地址参数；认证和证书保留；错误地址清晰拒绝。 | Inspected FE URL parser and BE address tests including path/port and invalid credential/query/traversal inputs. Full address renders with separately stored prefix. |
| A7 | passed | brief.md | A7：升级和扩容固定继承集群保存的仓库，不随平台默认变化；移除升级时的仓库切换 UI、CLI 参数及 API 字段，服务端拒绝普通编辑变更集群仓库；目标版本资源仍须校验。 | Inspected immutable ordinary update, strict unknown upgrade fields, controller saved registry metadata; old upgrade source flag/field absent. Matching targeted tests pass. |
| A8 | passed | brief.md | A8：删除旧 --offline、--online、--kc-image-repo-mirror、KC_IMAGE_REPO_MIRROR、/etc/kc/kc.env 镜像链路及旧 HTTP/镜像归档安装回退，不做兼容；相关帮助、补全、文档和测试同步。 | Independent current source search and committed deletion confirm legacy proxy/env and obsolete image helpers removed. Source bytes match previously independently verified cleanup candidate. |
| A9 | passed | brief.md | A9：源码和必要测试覆盖在线 GHCR、sync 后内网、路径、缺失错误及升级扩容；资源密集检查在 sh-dev 使用包含本地改动的准确源码，分别记录清单检查、拉取及实际安装结果，不互相替代。 | Independently recomputed BE1201/FE545 file manifests: 9aa6121d / 6c516c72 match. Read sh-dev logs: targeted BE tests, delivery tests, 15 FE suites/104 tests passed. Fresh Runtime checks passed with nonnull candidate gate8e2c71de and identical before/after input fingerprint. Pure-online and isolated offline runs remain separate follow-up. |
| A10 | passed | brief.md | A10：保留两仓既有未提交工作；测试环境数据修改和服务替换前检查资源引用与活跃操作，备份可回滚；预览与真实后端候选匹配；未获得提交推送授权不提交推送，后续推送仅用户 fork。 | Inspected Git: BEa0f3623b, FE77edd54 committed; BE fork oci remote SHA matches. FE original babel transform remains dirty and excluded; BE workflow files remain uncommitted. Read runtime backup/binary receipts and preview/cluster proof. No upstream submission evidence or destructive environment edits by verifier. |

## Checks

| Check | Command | Working directory | Status | Exit | Duration |
| --- | --- | --- | --- | ---: | ---: |
| Fresh candidate backend diff check | diff --check | . | passed | 0 | 51 ms |
| Installer invocation isolation tests | scripts/open-packaging/tests/installer-test.py | . | passed | 0 | 3644 ms |

### Builder-reported evidence

These are Builder reports, not Runtime check receipts or independent verification results.

- sh-dev exact-source backend compile and related unit tests/builds: passed — source 36616a1b16b3429c190fe65b34dc8f103f6796538762830a4c387f402275c30e; backend-checks.sh exit 0
- frontend unit/lint/build: passed — source 6c516c723cd6d8d1a1977602bdcc24ff5c8429eb7dea771dbf0abbee22e4c454; 15 suites/104 tests, lint zero errors with five preexisting warnings
- installer: passed — 5 Python tests
- delivery and sync: passed — sh-dev go test ./pkg/cli/registry/... ./pkg/delivery/... exit 0
- cleanup exact-source upgrade checks and build: passed — BE9aa6121d3bf7fdd5d99a825387f0a0901852aca57320885adfe81e35449e5e87; sh-dev backend-cleanup-checks.sh exit0; previous broader checks apply to unchanged code
- Known limitation: No full new-cluster install, upgrade or scale was performed; API/unit coverage is not field validation.
- Known limitation: No complete GHCR or internal registry pull or network-isolated air-gap install was performed.
- Known limitation: Swagger generation has preexisting schema warnings; only affected schemas updated.

## Blockers

_None._

## Risks and skipped work

- Pure-online GHCR and network-isolated offline deployments are still follow-up; do not count them as completed.
- Actual cluster proof is single-node installation; no scale/upgrade or uncached full pull proof.
- Five preexisting frontend lint warnings remain.

## Previous iterations

| Goal cycle | Iteration | Attempt | Outcome | Unresolved | Summary | Completed |
| ---: | ---: | ---: | --- | --- | --- | --- |
| 1 | 1 | 0 | recovery | — | User explicitly chose v1 upgrades fixed to the saved cluster registry; preserve all partial work and bind this confirmed scope refinement. | 2026-10-29T07:14:32.164Z |
| 2 | 1 | 1 | fail | A8 | Independent verification found one mandatory obsolete-code cleanup gap in A8; remaining A1-A7/A9-A10 pass at source, unit/build and preview validation level. Full lifecycle and air-gap field validation remains explicitly unperformed. | 2026-10-29T07:50:00.896Z |
| 2 | 2 | 1 | pass | — | Independently verified all A1-A10 against current owning sources, exact-source manifests, test/build logs and installed preview hashes. The prior A8 dead upgrade source-switch branch, field and test references are removed. Acceptance passes at implemented-source/test/preview scope, with explicit field-validation limits. | 2026-10-29T08:01:42.183Z |
| 2 | 2 | 2 | pass | — | Independent readonly verification passes A1-A10 for unchanged candidate cb24bea993e5e30e0ba36eba8a6f81040760edcd114787f2ab681f4674e9f6b1. Source and sh-dev receipts match, installer rerun passes, and new live evidence proves one actual cluster install. This is not a claim that newly requested pure-online or airgap flows already passed. | 2026-10-29T08:29:07.857Z |
| 2 | 2 | 2 | recovery | — | The user chose to revise the implementation after Verify stopped. | 2026-10-29T08:30:45.557Z |
| 2 | 3 | 1 | pass | — | Independently verified unchanged source hashes BE9aa612/FE6c516 and A1-A10 evidence; source/API/UI inspection and matching remote tests support product pass. Fresh Runtime checks pass, but candidate gate recording defect persists and archive repair is not claimed complete. Online/offline greenfield runs remain separate pending work. | 2026-10-29T08:37:10.218Z |
| 2 | 3 | 1 | recovery | — | The user rejected the Skill-coordinated Verifier pass. | 2026-10-29T08:37:55.149Z |
| 2 | 4 | 1 | pass | — | Independent readonly A1-A10 verification passes for committed source candidate. Source bytes match tested manifests, remote receipts match, and fresh candidate gate is now nonnull after normal implementation commits; no Runtime evidence or tool source was modified. | 2026-10-29T08:43:40.373Z |



## Conclusion

Independent readonly A1-A10 verification passes for committed source candidate. Source bytes match tested manifests, remote receipts match, and fresh candidate gate is now nonnull after normal implementation commits; no Runtime evidence or tool source was modified.
