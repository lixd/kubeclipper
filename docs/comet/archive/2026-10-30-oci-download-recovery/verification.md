---
generated_from_state_version: 15
---

# Verification

## Current result

- Result: **Archived**
- Verification status: **Checks completed; result confirmed**
- Goal cycle: 1
- Iteration: 2
- Verifier attempt: 1
- Completed: 2026-10-30T08:49:14.948Z
- Summary: Independent second-round read-only verification passes all A1-A12 for candidate 07f8be49cfbe34b6ca484867d215ac8d1d81696b0d469eb49b2326f3f2f76755. Prior A2/A7/A8 gaps are corrected and covered by regression. Reused valid candidate-bound Runtime evidence and independently confirmed exact local/remote source plus actual zero-exit remote receipts; no source, formal document, test or user resource was modified.

## Acceptance

| ID | Result | Source | Criterion | Reason |
| --- | --- | --- | --- | --- |
| A1 | passed | brief.md | Cold fetch verifies the pinned platform manifest and each required compressed layer once, then scans the supported single rootfs layer locally once. Persist verified manifest bytes and rebuild deleted extraction snapshots with a fixed digest and no network. Valid warm snapshots perform no pull. | Read snapshot.go fetchComponent/extractSnapshot and TestRegistrySingleLayerSubsetsAndLocalRebuild: pinned raw platform manifest stored by digest, descriptor verified before one local tar scan, one cold layer GET, warm reuse and fixed-digest offline rebuild after snapshot deletion. Candidate-bound Runtime check :12 passed and independently revalidated exact-source remote receipts. |
| A2 | passed | brief.md | Immutable extraction identity includes normalized selected contents and extraction format version. Order does not affect identity; deploy and join coexist and share raw blobs. Publish the entire synchronized snapshot atomically. | Read snapshot.go:62-87: File is normalized through contentFile before sorting and hashing; extractionVersion included, duplicate targets rejected. TestSnapshotSelectionNormalizesEquivalentFiles proves implicit and explicit names share identity; subset/order/local-rebuild test proves order invariance and separate deploy/join snapshots sharing blob. Complete files/receipt fsync then directory rename and parent fsync publish atomically. |
| A3 | passed | brief.md | Bounded streaming extraction validates platform manifest, layer descriptor and selected payload digests separately, preserving configs/charts decompressed payload semantics. Reject duplicate manifest/targets, missing assets, links, unsafe paths, unsupported layer layout and oversize data; document and test concrete limits using real package sizes. | Read extractSnapshot, validateFetchedManifest, verifyPayload and rejection/payload tests: raw platform manifest, compressed layer, binary payload and decompressed configs/charts digests remain distinct; bounded streams reject duplicate manifest/target, missing assets, links, unsafe paths and oversize data. Limits 4 MiB manifest, 8 GiB file, 16 GiB payload, 32 GiB rootfs/compressed layer documented alongside real 134400499-byte layer and 86930992/112795829-byte assets. Large limits tested using headers/scaled inputs, not multi-GiB allocation. |
| A4 | passed | brief.md | Real HTTP and HTTP2 body interruptions recover across calls using source/digest/size transactions and the original Registry endpoint. At most five content attempts including the first, exponential jittered backoff starting at one second capped at 30 seconds. No transparent HTTP decompression or uncontrolled nested retry; waits and bodies support cancellation. | Read content.go fetch/blobClient and HTTP1/HTTP2 fault tests: at most five original-endpoint attempts, exponential base-one-second jitter capped at 30 seconds, context-aware backoff/body and cross-call resume. DisableCompression and identity encoding preserve byte offsets; transport authentication reuses current Registry settings. Passed exact-source fetcher/race receipts cover body interruption, ignored Range, exhaustion and cancellation. |
| A5 | passed | brief.md | Validate complete 206 ranges; 200 ignoring Range restarts without append. Equal local size is independently verified; oversize data and invalid ranges fail. 416 is not success without complete local verification. Digest, disk and permanent authentication failures stay explicit. Logs/state exclude secrets and signed URLs. | Read strict 206 start/end/total and canonical Content-Range comparison, ContentLength checks, ignored-200 truncate, equal-length digest Commit, oversize Abort, explicit incomplete-416/auth/digest/disk failures. Recovery key hashes source/digest/size with no credentials; URL wrappers sanitized. Fault, completed-ingest and /dev/full tests are in passed candidate-6 regression. |
| A6 | passed | brief.md | Cancelable cross-process lifecycle, snapshot and blob locks follow documented order. Results retain protection through every consumer and expose idempotent Release. Multi-component failure releases everything; cleanup cannot delete paths before release. Installed files do not mutate cache through hardlinks. | Read LockCache flock polling and lifecycle -> snapshot -> blob lock order; stable lock inodes and sync.Once Release. Returned FetchResult/ComponentFetchResult hold shared lease until bootstrap distribution, upgrade rollout, runtime/Kubernetes extraction and Chart Helm command finish. Failure paths release protection; delayed-cleanup, concurrent fetch and cross-process tests passed in hash-matched regression/race. Consumers copy/unpack rather than mutate cache hardlinks. |
| A7 | passed | brief.md | Private standalone content store passes resume, commit, permission and crash contracts without a daemon. Partial data never becomes ready. Cache corruption repair drops all locks before exclusive recheck/removal and retries from the start once, then fails explicitly. | Read openContent private parents and standalone containerd store usage; verified/resume/Commit and corruption-repair path drops shared/snapshot/blob locks before cancellable exclusive recheck/removal and restarts once. content.go:270-293 now explicitly aborts metadata-only ingest with no data and recreates it; TestContentIngestSurvivesProcessExit covers metadata-only, partial and complete pre-Commit exits. Snapshot staged/published exit tests reject incomplete visibility. All are in successful candidate-6 checks. |
| A8 | passed | brief.md | Cleanup excludes active users and stable lock files; uninstall removes only component snapshots. Account all cache data: 20 GiB soft budget, 7-day abandoned staging eligibility, pressure reclamation, 1 GiB reserve, oversized worksets allowed with space, access metadata outside snapshots and no atime dependency. Test LRU and legacy directory recovery without compatibility paths. | Read lifecycle.go cacheObjects/pruneCache: whole ingest directory is now a partial eviction unit, including metadata-only/empty directories; all metadata bytes accounted and empty directories age from directory mtime. New TestPruneMetadataOnlyAndEmptyIngest and TestPruneLegacyCompleteAndInterruptedPackages cover prior gaps. Existing LRU/access/staging/protection tests cover 20 GiB soft budget, 7-day eligibility, pressure collection, 1 GiB reserve and stable locks. CleanupPackage uses exclusive lifecycle lock and removes component snapshots, retaining shared blobs; native Helm source-marker cache preserved. |
| A9 | passed | brief.md | Deploy extracts/distributes only required assets excluding kcctl; join only agent, while transferring the complete shared layer. All package and Chart consumers use exact result paths and protection. Preserve missing-asset failures and existing HTTP/auth/CA/TLS/redirect source rules. | Read selectedBootstrapContents and bootstrap assertions: deploy excludes kcctl, join selects only agent; complete shared layer still transferred. Searched every production Fetch/FetchComponent caller and verified Release spans actual distribution/unpack/Helm consumption; Calico uses returned Chart path within acquireChart protection. Transport tests cover HTTP, Basic/wrong password, CA, skip verification, untrusted TLS, prefix and cross-host redirect credential isolation. Missing assets remain explicit failures. |
| A10 | passed | brief.md | Frontend, source policy, metadata indexing, native Helm OCI, sync graph copy and runtime-image cache remain outside this change. Audit Registry binary acquisition but do not change independent paths. No fallback, global HTTP2 disable, new daemon/database/public cache knobs, commits, pushes or live deployments. | Reviewed tracked diff and untracked implementation paths: no frontend/source policy/indexing/sync graph/runtime-image rewrites, added dependencies, daemon/database/public cache knobs or global HTTP2 disable. Registry extraction remains independent extractRegistryBinaryToTemp and is outside first-phase rewrite. Existing .gitignore/.comet modifications preserved; verifier performed no commit, push, deployment or user-resource mutation. |
| A11 | passed | brief.md | Build and appropriate fault, concurrency, crash and race checks run on sh-dev using an isolated allowlisted snapshot verified by file hashes including relevant local dirty/new files. Independent read-only Verifier covers all confirmed criteria; failed, blocked or unrun checks cannot pass. | Independently ran /tmp/kc-oci-runtime-check.py successfully: local and sh-dev 762-file allowlist matches source-manifest SHA256 6391b50b7bb63c2573c1428843a3ab9abe1a8cb02c65e9a9456ef0bedab2e544; remote source verification mismatches=[]. Actual tests/race/build/ghcr-proxy status files all zero. Reviewed current Runtime :12 candidate-bound receipt and actual logs. Host-network port-collision failure retained separately; it is not counted as passing. Independent review covered every A1-A12 without changing source/docs/tests. |
| A12 | passed | brief.md | Candidate code completes a real public GHCR package pull and raw/payload verification. Distinguish metadata, transfer, installation and cluster creation evidence; preserve current preview, user nodes and dirty worktrees. | Read actual candidate-6-ghcr-proxy.log and opt-in TestRealGHCRPackage: real public pinned manifest sha256:8f3a2d996fbad6a7513f7c29de8351d381b84c0040aba91046c87b6f0b778d85 completed raw-layer and extracted agent/server verification; cold 6m37.24985781s, warm 786.428228ms, exit zero. This proves proxy-route acquisition only. Direct GHCR timeout, platform installation and cluster creation are explicitly not counted as success; preview/user nodes were untouched. |

## Checks

| Check | Command | Working directory | Status | Exit | Duration |
| --- | --- | --- | --- | ---: | ---: |
| Verify exact local/remote source and sh-dev regression/race/build/GHCR receipts | /tmp/kc-oci-runtime-check.py | . | passed | 0 | 17179 ms |

### Builder-reported evidence

These are Builder reports, not Runtime check receipts or independent verification results.

- affected-package-regressions: passed — sh-dev candidate-6; runtime check validates source hash and exit-zero record
- content-and-lifecycle-race: passed — sh-dev candidate-6; runtime check validates source hash and exit-zero record
- kcctl-server-agent-build: passed — sh-dev candidate-6; runtime check validates source hash and exit-zero record
- real-GHCR-cold-and-warm-package: passed — sh-dev candidate-6; runtime check validates source hash and exit-zero record
- Known limitation: Direct sh-dev GHCR route timed out; proxy route succeeded. No claim of proxy/upstream root cause or network-wide availability.
- Known limitation: No platform deployment, cluster creation, power-loss testing or sync/native-Helm/runtime-image rewrite.
- Known limitation: Capacity boundaries use scaled budgets and oversize headers rather than allocating tens of GiB.
- Known limitation: Earlier failed/timeout runs remain in evidence; only final candidate matching checks are reported passed.

## Blockers

_None._

## Risks and skipped work

- Real GHCR acquisition succeeded through proxy route; direct-route timeout remains a separate failure. No platform installation or cluster creation was performed.
- Process-exit tests do not prove real power-loss or disk hardware failure recovery; large limits and cache budgets use bounded/scaled tests, not tens-of-GiB allocations.

## Previous iterations

| Goal cycle | Iteration | Attempt | Outcome | Unresolved | Summary | Completed |
| ---: | ---: | ---: | --- | --- | --- | --- |
| 1 | 1 | 1 | fail | A2, A7, A8 | Independent read-only verification completed for all A1-A12. Exact-source remote build, regression, race and real GHCR receipts are valid, but candidate fails A2 normalized identity and A7/A8 pre-data ingest crash recovery/accounting. No implementation, formal documentation or tests were changed. | 2026-10-30T08:02:29.634Z |
| 1 | 2 | 1 | pass | — | Independent second-round read-only verification passes all A1-A12 for candidate 07f8be49cfbe34b6ca484867d215ac8d1d81696b0d469eb49b2326f3f2f76755. Prior A2/A7/A8 gaps are corrected and covered by regression. Reused valid candidate-bound Runtime evidence and independently confirmed exact local/remote source plus actual zero-exit remote receipts; no source, formal document, test or user resource was modified. | 2026-10-30T08:49:14.948Z |



## Conclusion

Independent second-round read-only verification passes all A1-A12 for candidate 07f8be49cfbe34b6ca484867d215ac8d1d81696b0d469eb49b2326f3f2f76755. Prior A2/A7/A8 gaps are corrected and covered by regression. Reused valid candidate-bound Runtime evidence and independently confirmed exact local/remote source plus actual zero-exit remote receipts; no source, formal document, test or user resource was modified.
