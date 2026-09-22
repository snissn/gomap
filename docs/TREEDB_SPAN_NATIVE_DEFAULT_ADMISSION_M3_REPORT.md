# TreeDB span-native default admission M3 report

Issue: #2786. Parent tracker: #2782.

## Decision

M3 is a **default-off / opt-in-only** decision. It does not change TreeDB
runtime defaults, persistent formats, benchmark boundaries, or hot-path code.

Span-native apply and backlog coalescing remain explicit workload-specific
knobs. They are not admitted for unconfigured/default use in this milestone
because M2 left checkpoint-inclusive latency debt unresolved, M14 proved the
low-concurrency `span_native_c1` shape regresses, and M4 read/scan/cache
guardrails have not run yet.

## Inputs consumed

- M0 triage artifact:
  `docs/TREEDB_SPAN_NATIVE_DEFAULT_READINESS_PATCH_TRIAGE.md`.
- M1 adaptive cache-admission contract from #2784 / PR #2790:
  - `Options.LeafPageReadCacheWriteAdmission`;
  - `-treedb-leaf-page-read-cache-write-admission`;
  - `write_admission_*` stats;
  - default remains `immediate`, adaptive remains opt-in.
- M2 checkpoint report:
  `docs/TREEDB_CHECKPOINT_LATENCY_M2_REPORT.md`.
- M14 final gate:
  `docs/TREEDB_RANDOM_WRITE_CEILING_BREAKER_M14_REPORT.md`.

Remote evidence roots used as decision inputs:

- M14 final matrix:
  `/mnt/fast4tb/gomap-profiles/2774_m14_matrix_20260616_132256`.
- M1 cache-admission clean matrix:
  `/mnt/fast4tb/gomap-profiles/2784_cache_admission_10mm_20260616_175721`.
- M1 c4 recheck:
  `/mnt/fast4tb/gomap-profiles/2784_cache_admission_c4_recheck_20260616_181941`.
- M2 checkpoint experiment:
  `/mnt/fast4tb/gomap-profiles/2785_checkpoint_10mm_20260616_173405`.

## Admission model for defaults

This milestone records the policy model downstream gates should use. It is not
a runtime selector yet.

| State | Decision | Reason |
| --- | --- | --- |
| Unconfigured/default options | **Decline span-native/backlog** | M2 checkpoint debt remains a default-on blocker. |
| Explicit span-native/backlog opt-in with c4-ish concurrency | **Allowed as experimental opt-in** | M14/M1 show strong write throughput and rollback knobs exist, but checkpoint/read guardrails remain workload-specific. |
| Explicit c1 / low-concurrency span-native | **Decline as default candidate** | M14 `span_native_c1` regressed `random_write` by 20.19% and increased foreground-assist wait. |
| c8/c16 throughput-ceiling rows | **Do not universalize** | Higher throughput comes with higher CPU/contention profile totals; use only with workload evidence. |
| Backlog coalescing during checkpoint/close drains | **Fallback/decline** | Existing `close_or_checkpoint` fallback and backlog skip counters are the safe behavior. |
| Range deletes, lane/command-WAL barriers, root mismatch, output ownership failure, reducer validation failure | **Fallback/decline** | Existing M12 fallback correctness/accounting must remain fail-closed. |
| Adaptive outer-leaf cache write admission | **Optional axis, not default proof** | M1 proves opt-in write-cache pressure reduction; M4 must still prove read/scan/cache guardrails before any default cache change. |

## Required rollback knobs

The rollback surface remains unchanged and explicit:

- disable span-native apply: leave `FlushApplySpanNative` false or omit
  `-treedb-flush-apply-span-native`;
- disable backlog coalescing: leave `FlushBacklogCoalescing` false or omit
  `-treedb-flush-backlog-coalescing`;
- lower/override apply concurrency with `FlushApplyConcurrency` or
  `-treedb-flush-apply-concurrency`;
- disable adaptive write-side cache admission by leaving
  `LeafPageReadCacheWriteAdmission` at the default `immediate` policy.

No data migration is required for these toggles because they only affect
in-memory apply/cache policy. Leaf-log and value-log output remain persistent
storage.

## Observability contract for downstream gates

#2787 and #2788 should continue to report these counters when evaluating any
candidate default:

- span-native path and fallback accounting:
  - `treedb.flush_apply.span_native.candidate_ops_total`
  - `treedb.flush_apply.span_native.eligible_ops_total`
  - `treedb.flush_apply.span_native.used_ops_total`
  - `treedb.flush_apply.span_native.fallbacks_total`
  - `treedb.flush_apply.span_native.fallback.reason.*.{ops,spans}_total`
- checkpoint debt:
  - `treedb.cache.checkpoint.flushmu_wait_*`
  - `treedb.cache.checkpoint.active_background_flush_wait_ns_*`
  - `treedb.cache.checkpoint.stage.flush_all.*`
  - `treedb.cache.checkpoint.stage.value_log_flush.*`
  - `treedb.cache.checkpoint.stage.leaf_value_log_sync.*`
  - `treedb.cache.checkpoint.stage.reducer_publish.*`
- backlog coalescing:
  - `treedb.cache.flush_backlog_coalescing.admitted_runs_total`
  - `treedb.cache.flush_backlog_coalescing.skip.reason.*`
  - selected memtable/ops/bytes maxima
- write/read cache guardrails from M1:
  - `treedb.process.read_path.outer_leaf.cache.write_admission_policy`
  - `write_admission_attempts`, `write_admission_stores`,
    `write_admission_skips`, and `write_admission_lock_skips`
  - cache hits, misses, stores, evictions, entries, capacity, and bytes

## Test and validation scope

This M3 PR is intentionally documentation/policy only. It adds no runtime
selector, no default behavior change, and no new stats implementation. Therefore
new runtime policy-selection tests are not meaningful in this PR; they are
required for any later PR that implements an actual selector or default-on
candidate.

Existing relevant coverage remains the guardrail baseline:

- fallback classification and stats:
  `TreeDB/db/flush_apply_stats_test.go` and
  `TreeDB/db/span_native_apply_stats_test.go`;
- span-native apply/fallback correctness:
  `TreeDB/db/flush_apply_parallel_test.go`;
- backlog coalescing skip/admission behavior:
  `TreeDB/caching/flush_backlog_coalescing_test.go`;
- adaptive cache admission and option compatibility:
  `TreeDB/db/leaf_page_read_cache_test.go` and `cmd/unified_bench` tests.

Validation for this docs-only PR:

```sh
git diff --check HEAD~1..HEAD
```

The #2782 10MM runtime gate is not applicable to this PR because it makes no
hot-path, benchmark-semantics, default, or persistent-format change. #2788 must
still run the final same-host gate before any default-readiness claim.

## Handoff

- #2787 can rely on M1's opt-in adaptive cache-admission option/counters and on
  this M3 decision that no span-native/backlog default is being enabled before
  read/scan/cache guardrails.
- #2788 should expect the final gate to start from **default-off / opt-in-only**.
  Default-on remains blocked by M2 checkpoint debt unless a later PR removes the
  debt without throughput/storage/read regressions or the coordinator explicitly
  accepts the tradeoff.
