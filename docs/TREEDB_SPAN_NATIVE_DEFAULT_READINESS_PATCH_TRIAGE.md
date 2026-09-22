# TreeDB span-native default-readiness human-patch triage

Issue: #2783 (M0). Parent tracker: #2782.

This is a docs/triage-only artifact for the local read-only snapshot at
`/Users/michaelseiler/dev/snissn/gomap-human-2`.

## Inputs reviewed

- Current main/base: `5120a5c6f3a76f830f4059347d3ea418485d7fca`.
- Human snapshot branch/head: `codex/2760-parallel-flush-m7` at
  `a8c7dce95c6e3ddc3603206866d03eeda47a3fc6`, plus uncommitted changes.
- Snapshot changed files: `TODO.md`, `TreeDB/caching/db.go`,
  `TreeDB/caching/leaf_page_log.go`, `TreeDB/caching/memory_stats_test.go`,
  `TreeDB/db/api.go`, `TreeDB/db/api_test.go`, `TreeDB/db/leaf_page_log.go`,
  `TreeDB/db/leaf_page_read_cache.go`,
  `TreeDB/db/leaf_page_read_cache_test.go`, `TreeDB/zipper/zipper.go`,
  `cmd/unified_bench/main.go`, and untracked
  `docs/TREEDB_RANDOM_WRITE_CEILING_BREAKER_TODO.md`.
- Current evidence baseline: `docs/TREEDB_RANDOM_WRITE_CEILING_BREAKER_M14_REPORT.md`.
- Detailed inventory diffs were saved outside repo under `/tmp/gomap_2783_triage/`.

This review treats the snapshot as input only. No runtime hunk is accepted for
this PR, and any later runtime port is performance-sensitive by default under
#2782's same-host 10MM unified-bench gate.

## Apply / compile read

The exact uncommitted patch does **not** cleanly apply to current main:

- `git apply --check /tmp/gomap_2783_triage/human_uncommitted.diff` fails in
  `TreeDB/zipper/zipper.go`.
- `git apply --3way --check ...` reports all other files as clean and
  `TreeDB/zipper/zipper.go` with conflicts, which is expected because M8-M14
  added more `mergeScratch` span-native ownership/fallback scratch state.

Conceptually, the non-doc runtime ideas are still understandable against current
main, but they must be rebased and reviewed hunk-by-hunk. The patch predates the
merged M8-M14 contract and must not be copied wholesale.

## Triage table

| Patch area | Current-main status | Decision | Rationale | Target issue/PR | Required tests/benchmarks |
| --- | --- | --- | --- | --- | --- |
| Write-side outer-leaf read-cache admission (`storeWrite`, `shouldAdmitWriteStore`, write-admission counters, stats tests, unified-bench stats) | Current main has read-miss admission and counters, but write-side stores still call `storeLeafPageReadCache` -> `leafPageReadCache.store` immediately. M14 shows `span_native_c4_cache_disabled` improves write-only throughput, but read/scan safety was not proven. | **In spirit for #2784; do not copy/port wholesale.** Counter names and some unit-test shapes may be ported if #2784 keeps those semantics, but the always-on heuristic itself needs redesign/guardrails. | The idea directly targets the M14 cache-disabled signal, but the snapshot changes default write-cache behavior with fixed warmup/sample/read-ratio constants. It can affect point reads, scans, read-after-write cache hit behavior, and cache checksum-state observations. A default-on static cache policy is not acceptable from write throughput alone. | #2784 adaptive outer-leaf cache admission. | Unit tests for cold write stream, hot-read re-admission, disabled cache, invalid page sizes, small capacities, lock-skips, and checksum/read-integrity state. Focused `go test ./TreeDB/db ./TreeDB/caching ./cmd/unified_bench -count=1`. Same-host 10MM rows for default, `span_native_c4`, `span_native_c4_cache_disabled`, and candidate policy, plus read/scan or mixed guardrails and cache admission/hit/miss counters. |
| `AppendLeafPagesInto` / `LeafPageBatchLogInto` / leaf-log pointer-slice scratch reuse | Current main batch leaf-log append returns a freshly allocated `[]page.LeafLogPtr`; `Zipper` already reuses leaf-page and child-ref scratch but has no leaf-log pointer scratch. Exact patch conflicts only in `TreeDB/zipper/zipper.go` because current main has newer span-native scratch fields. | **Copy/port narrowly, but not in M0 and not as cache-policy proof.** Treat as an allocation optimization that can be ported after profiling shows pointer-slice allocation is material. | The API shape preserves persistent leaf-log semantics if it keeps positional pointer mapping, record-length hints, cache population, and caller-owned scratch lifetimes. It does not by itself decide cache defaults. Because it touches `Zipper` scratch/ownership code, it must be rebased around M8-M14 span-native scratch ownership and fallback invariants. | Prefer a separate optimization PR under #2782 if profiles justify it; fold into #2784 only if #2784 profiles show this allocation on the cache-admission hot path. | Tests for positional pointer mapping, batch/single/empty paths, record-length hints, cache population, and no aliasing of pooled value-log pointer slices. Focused `go test ./TreeDB/db ./TreeDB/caching ./TreeDB/zipper -count=1`. Allocation benchmark/profile deltas and, because it touches runtime write/apply paths, #2782 before/after 10MM evidence including checkpoint time. |
| Single-record prepared frame allocation polish (`prepareAppendOneFrame`) | Current main still prepares a single leaf-log block-compression frame through `prepareAppendFrames`, allocating/reusing a one-element `[]preparedDictFrame`. The snapshot's direct `preparedDictFrame` helper otherwise matches current symbols. | **Copy/port narrowly only after profiling; otherwise defer.** | This is a hot write-path allocation polish, not a default-readiness policy. It must preserve prepared-frame release on success/error, value-log keep-policy behavior, and the persistent-output invariant that prepared bytes are not durable until appended. It should not be mixed into #2784 unless profiles show it is necessary to interpret cache-admission evidence. | Separate optimization PR under #2782, or #2785 only if checkpoint/leaf-log profiles identify single-record frame allocation as a checkpoint-inclusive latency contributor. | Focused tests around value-log/leaf-log append paths already covering block compression and reopen/readback, plus allocation profiles. Same-host before/after benchmark evidence with `B/op`, `allocs/op`, CPU tops, append-frame counters, and checkpoint timing; 10MM gate if runtime code is changed. |
| Old random-write TODO doc snapshot and `TODO.md` link | Current main already contains the M8 span-run contract in `docs/TREEDB_RANDOM_WRITE_CEILING_BREAKER_TODO.md` and the M14 final gate in `docs/TREEDB_RANDOM_WRITE_CEILING_BREAKER_M14_REPORT.md`. The human untracked doc is older and diverges substantially. | **Not at all.** Do not copy the old content. Link only current docs/triage artifacts. | The old snapshot is superseded by the merged M8-M14 contract and M14 evidence. Copying it would reintroduce stale planning language and obscure the current default-off/readiness blockers. | #2783 docs-only. | `git diff --check`; no runtime benchmark required. |

## Required review questions

- **Does the patch still compile conceptually against current main after #2781?**
  Mostly, but not as-is. The exact patch fails `git apply --check` in
  `TreeDB/zipper/zipper.go`; other hunks are mechanically closer. Any port must
  rebase across M8-M14 scratch and span-native ownership changes.
- **Does it duplicate work already merged in M8-M14?**
  The old TODO doc is superseded. The cache-admission idea complements, but does
  not replace, merged read-miss admission and M14 cache-disabled evidence.
  Pointer-slice and single-record frame polish are not already merged, but they
  overlap with M8-M14 scratch/prepared-output hot paths.
- **Does it interact with persistent leaf-log/value-log semantics?**
  Cache admission should only affect in-memory cache population, not pointer
  validity. `AppendLeafPagesInto` and prepared-frame polish touch persistent
  append paths and must preserve positional pointer mapping, durable output
  ownership, abandoned-output accounting, and no truncation of valid leaf/value
  log pointer targets.
- **Does it preserve read integrity/checksum cache semantics?**
  Not proven. The cache-admission port must keep record/page checksum state
  conservative when write stores are skipped and when read-miss admission later
  fills the cache. Existing checksum verification counters/tests must remain
  meaningful.
- **Does it affect read/scan workloads or only write-side stores?**
  The cache-admission area affects read/scan cache warmth and read-after-write
  cache hit behavior. The pointer-scratch and single-frame areas are write/apply
  allocation changes and should be read-path neutral except through cache/store
  interactions.
- **Does it reduce allocation/cache pressure without increasing checkpoint time?**
  The snapshot asserts likely reductions but provides no current-main evidence.
  M14's cache-disabled row improved write-only throughput while checkpoint time
  remained above default, so every runtime port needs checkpoint-inclusive
  before/after evidence.
- **Can it be guarded/adaptive, or is it just a new static cache default?**
  The cache hunk has adaptive signals, but it would still become a new static
  default heuristic if copied. #2784 should make the policy opt-in/adaptive or
  fail-closed, with rollback knobs and counters proving which branch ran.

## Follow-up scheduling read

M0 produces no runtime contract churn. After this artifact merges, #2784 and
#2785 can start in parallel if their managers keep the overlap bounded:

- #2784 owns cache-admission policy, cache stats, read/cache guardrails, and
  any directly necessary benchmark-stat plumbing.
- #2785 owns checkpoint-inclusive profiling/fixes and should not absorb cache
  policy work except as an already-merged input row.

Potential overlap is limited to `TreeDB/caching/db.go` stats/checkpoint
instrumentation and benchmark reporting. Both issues remain performance-sensitive
and must use same-host before/after evidence before mergeability.

## M0 validation scope

This M0 PR is documentation/triage only. It does not touch TreeDB runtime code,
benchmark semantics, tests, scripts, defaults, or on-disk formats. The #2782
10MM unified-bench gate is therefore explicitly not applicable to this PR; it is
required for any follow-up that ports or adapts runtime code from the snapshot.
