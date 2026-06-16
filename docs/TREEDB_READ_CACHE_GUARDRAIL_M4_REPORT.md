# TreeDB read/cache guardrail M4 report

Issue: #2787. Parent tracker: #2782.

This is a **preflight / matrix-contract** report for #2787. It does not close
#2787 by itself. After the coordinator no-go review, #2794 and #2795 were
inserted before the actual M4 guardrail run; #2787 remains open until those
blockers are resolved and the read/scan/reopen matrix is executed or explicitly
waived by an opt-in-only/default-off decision.

## Scope and outcome

This preflight PR is a **guardrail report / matrix definition** pass. It does not change
TreeDB runtime behavior, benchmark semantics, public defaults, hot-path code, or
on-disk formats.

The downstream decision remains conservative:

- span-native apply and backlog coalescing remain **default-off / opt-in-only**;
- adaptive outer-leaf read-cache write admission remains **opt-in**;
- no default-on claim is made from write-only or point-read-only evidence;
- #2794 must resolve checkpoint debt, #2795 must resolve low-concurrency admission,
  #2787 must run/record the actual read/cache guardrails, and #2788 must run the
  final same-host gate before any default-readiness decision.

This report is intentionally not a new 10MM runtime run. M3 already recorded a
default-off decision because M2 left checkpoint-inclusive latency debt
unresolved. Running a partial M4 matrix now would not remove that blocker and
would risk over-claiming from incomplete read/scan/reopen evidence. Instead,
this preflight report makes the read/cache guardrail contract explicit and
identifies exactly what #2787 must run after #2794/#2795, plus what #2788 must
consume or rerun before any final decision.

## Inputs consumed

Predecessor artifacts and contracts on current `main`:

- M0 triage: `docs/TREEDB_SPAN_NATIVE_DEFAULT_READINESS_PATCH_TRIAGE.md`.
- M1 adaptive cache admission from #2784 / PR #2790:
  - `Options.LeafPageReadCacheWriteAdmission`;
  - `-treedb-leaf-page-read-cache-write-admission`;
  - `write_admission_*` stats;
  - default remains `immediate`, adaptive remains opt-in;
  - evidence roots:
    - `/mnt/fast4tb/gomap-profiles/2784_cache_admission_10mm_20260616_175721`;
    - `/mnt/fast4tb/gomap-profiles/2784_cache_admission_c4_recheck_20260616_181941`.
- M2 checkpoint report: `docs/TREEDB_CHECKPOINT_LATENCY_M2_REPORT.md`, evidence
  root `/mnt/fast4tb/gomap-profiles/2785_checkpoint_10mm_20260616_173405`.
- M3 admission decision:
  `docs/TREEDB_SPAN_NATIVE_DEFAULT_ADMISSION_M3_REPORT.md`.
- M14 final gate:
  `docs/TREEDB_RANDOM_WRITE_CEILING_BREAKER_M14_REPORT.md`, evidence root
  `/mnt/fast4tb/gomap-profiles/2774_m14_matrix_20260616_132256`.

## Candidate identity for future gates

Because M3 kept defaults off, there is no new unconfigured runtime candidate in
M4. The only candidate worth carrying into #2788 as an **experimental opt-in
axis** is:

```text
FlushApplySpanNative=true
FlushBacklogCoalescing=true
FlushApplyConcurrency=4
LeafPageReadCacheWriteAdmission=adaptive
```

The comparison rows must also include current defaults, explicit forced-off
rollback, c1 low-concurrency guardrail, c4 immediate-cache behavior, and
cache-disabled behavior.

## Evidence already available

### M1 point-read/cache guardrail

M1 proved the adaptive cache-admission branch can reduce write-side cache stores
without materially regressing its focused point-read guardrail:

| Row | random_read ops/s | random-read checkpoint | post-run checkpoint | cache hits / misses | cache stores / evictions | write attempts / stores / skips |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| base `span_native_c4` read guardrail | 73,771 | 809.77ms | 323µs | 918,375 / 9,081,949 | 1,018,230 / 985,462 | n/a |
| candidate `span_native_c4` immediate | 74,043 | 861.61ms | 285µs | 918,375 / 9,081,949 | 1,018,230 / 985,462 | 0 / 0 / 0 |
| candidate `span_native_c4` adaptive | 74,217 | 698.13ms | 281µs | 915,617 / 9,084,707 | 698,995 / 666,227 | 357,467 / 37,841 / 319,626 |

M1's write rows also showed that adaptive c4 cut write-side stores/evictions by
roughly 88% versus immediate c4 and landed near the cache-disabled
old-leaf/merge/frame profile while preserving read-miss admission.

This is useful evidence, but it is not sufficient for a default-on decision: it
covers point reads, not settled scans, mixed read/write under flush debt, or a
full reopen/read-after-reopen guardrail matrix.

### Checkpoint blocker remains

M2 showed that the useful span-native rows still carry checkpoint-boundary debt.
The rejected checkpoint-yield experiment reduced some active-wait counters but
regressed c4 random-write throughput and c16 post-run checkpoint time, so no
runtime fix was kept. This remains a default-on blocker for #2788 unless a later
change removes the debt without throughput/storage/read regressions or the
coordinator explicitly accepts an opt-in-only tradeoff.

### Existing correctness coverage to keep in the gate

Current tests already cover the important correctness families that #2788 should
rerun on its final head:

- reopen after checkpoint / WriteSync and leaf/value-log pointer durability:
  `TreeDB/reopen_verify_test.go` (`TestReopenVerify_WALOn_Checkpoint`,
  `TestReopenVerify_WALOn_WriteSync`,
  `TestReopenVerify_WALOn_Checkpoint_LeafPagesInValueLog`, and related reopen
  parity tests);
- leaf-log grouped-frame CRC/read-integrity modes:
  `TestReopenVerify_LeafPageLogGroupedFrameCRCIntegrityModes`;
- adaptive cache admission behavior and checksum state:
  `TreeDB/db/leaf_page_read_cache_test.go`, including
  `TestLeafPageReadCacheAdaptiveWriteAdmissionPreservesChecksumState`;
- span-native fallback/accounting and backlog behavior:
  `TreeDB/db/flush_apply_stats_test.go`,
  `TreeDB/db/span_native_apply_stats_test.go`, and
  `TreeDB/caching/flush_backlog_coalescing_test.go`.

If #2788 proposes a runtime default selector or any new cache/checkpoint code,
it must add focused tests for that exact selector instead of relying only on
these existing tests.

## What is not proven yet

The following guardrails are still missing from same-host M4/final evidence:

- settled point-read results after a checkpoint/reopen boundary for all candidate
  cache/span rows;
- settled `full_scan` and `prefix_scan` behavior for the same rows;
- mixed/debt read behavior when reads start before all write debt is naturally
  paid;
- cache-hit/miss/store/eviction/admission counters for scans, not just point
  reads;
- read-after-reopen smoke tied to the same option set that #2788 evaluates;
- checksum/read-integrity behavior on the final candidate head if any cache
  policy changes beyond M1's opt-in adaptive policy are introduced.

Therefore this preflight dependency-ready conclusion is: **#2787 can consume the
M1 point-read/cache evidence and this matrix contract, but #2788 cannot claim
default-on until #2794/#2795 are resolved and #2787 runs or explicitly waives
the missing read/scan/reopen/checkpoint guardrails.**

## Required #2787/#2788 artifact root and setup

Use the remote 4TB host for final gates, and avoid overlap with other large
runs:

```sh
ssh <remote-4tb-benchmark-host>
cd /mnt/fast4tb/gomap-worktrees/<2788-worktree>
git fetch origin
# checkout the final #2788 candidate head here
make unified-bench
make benchprof
RUN_ROOT=/mnt/fast4tb/gomap-profiles/2788_final_gate_$(date +%Y%m%d_%H%M%S)
mkdir -p "$RUN_ROOT"
```

Every row below should write a row-local `command.sh`, `variant.env`,
`benchprof_results.{json,md}`, `insights.{json,md}`, pprof files, and final
disk-usage artifacts under `$RUN_ROOT/<row-name>/`. Record the candidate and
baseline SHAs in the PR body.

Common 10MM shape:

```sh
COMMON_FLAGS="\
  -dbs treedb \
  -keys 10000000 \
  -valsize 128 \
  -batchsize 8000 \
  -path-label m8-m14-10mm-gate \
  -treedb-journal-lanes=1 \
  -progress=false"

SPAN_FLAGS="\
  -treedb-flush-apply-min-entries=1 \
  -treedb-flush-apply-min-spans=1 \
  -treedb-flush-apply-min-bytes=1 \
  -treedb-flush-apply-span-native \
  -treedb-flush-backlog-coalescing"
```

## Required #2787/#2788 write/checkpoint rows

Run the write subset first so #2787/#2788 can compare with M14/M1/M2:

```sh
./bin/unified-bench $COMMON_FLAGS \
  -test sequential_write,batch_random,random_write \
  -checkpoint-between-tests \
  -profile-dir "$RUN_ROOT/default_unconfigured_write"

./bin/unified-bench $COMMON_FLAGS \
  -test sequential_write,batch_random,random_write \
  -checkpoint-between-tests \
  -treedb-flush-apply-concurrency=1 \
  -treedb-flush-apply-span-native=false \
  -treedb-flush-backlog-coalescing=false \
  -treedb-leaf-page-read-cache-write-admission=immediate \
  -profile-dir "$RUN_ROOT/forced_off_write"

./bin/unified-bench $COMMON_FLAGS $SPAN_FLAGS \
  -test sequential_write,batch_random,random_write \
  -checkpoint-between-tests \
  -treedb-flush-apply-concurrency=1 \
  -profile-dir "$RUN_ROOT/span_native_c1_immediate_write"

./bin/unified-bench $COMMON_FLAGS $SPAN_FLAGS \
  -test sequential_write,batch_random,random_write \
  -checkpoint-between-tests \
  -treedb-flush-apply-concurrency=4 \
  -treedb-leaf-page-read-cache-write-admission=immediate \
  -profile-dir "$RUN_ROOT/span_native_c4_immediate_write"

./bin/unified-bench $COMMON_FLAGS $SPAN_FLAGS \
  -test sequential_write,batch_random,random_write \
  -checkpoint-between-tests \
  -treedb-flush-apply-concurrency=4 \
  -treedb-leaf-page-read-cache-write-admission=adaptive \
  -profile-dir "$RUN_ROOT/span_native_c4_adaptive_write"

./bin/unified-bench $COMMON_FLAGS $SPAN_FLAGS \
  -test sequential_write,batch_random,random_write \
  -checkpoint-between-tests \
  -treedb-flush-apply-concurrency=4 \
  -treedb-leaf-page-read-cache-entries=-1 \
  -profile-dir "$RUN_ROOT/span_native_c4_cache_disabled_write"
```

If #2787/#2788 considers c8/c16 as anything more than throughput-ceiling opt-ins, it
must add equivalent c8/c16 rows and treat the M2 checkpoint debt as blocking
unless fixed or explicitly accepted.

## Required #2787/#2788 read/scan/mixed rows

For each of these row families, run at least:

- `default_unconfigured`;
- `span_native_c4_immediate`;
- `span_native_c4_adaptive`;
- `span_native_c4_cache_disabled`.

Add c1 only if #2788 needs to prove a runtime selector declines low-concurrency
span-native; otherwise the write row is enough to preserve the M14 c1 guardrail.

Use these row-flag expansions for `<ROW_FLAGS>` and the matching `<row>` path
component in the commands below:

```sh
# <row>=default_unconfigured
ROW_FLAGS=""

# <row>=span_native_c4_immediate
ROW_FLAGS="$SPAN_FLAGS \
  -treedb-flush-apply-concurrency=4 \
  -treedb-leaf-page-read-cache-write-admission=immediate"

# <row>=span_native_c4_adaptive
ROW_FLAGS="$SPAN_FLAGS \
  -treedb-flush-apply-concurrency=4 \
  -treedb-leaf-page-read-cache-write-admission=adaptive"

# <row>=span_native_c4_cache_disabled
ROW_FLAGS="$SPAN_FLAGS \
  -treedb-flush-apply-concurrency=4 \
  -treedb-leaf-page-read-cache-entries=-1"
```

### Settled point-read after write/checkpoint/reopen

```sh
./bin/unified-bench $COMMON_FLAGS <ROW_FLAGS> \
  -test sequential_write,random_write,random_read \
  -checkpoint-between-tests \
  -settle-before-scans \
  -treedb-cache-stats-before-reads \
  -read-require-hit \
  -profile-dir "$RUN_ROOT/<row>_settled_point_read"
```

`-settle-before-scans` currently settles before the first read/scan-family test,
including `random_read`, so this row exercises a close/open boundary before the
read phase. Keep `-checkpoint-between-tests` to record the durability-boundary
costs and to reduce background-flush interference.

### Settled full/prefix scans

```sh
./bin/unified-bench $COMMON_FLAGS <ROW_FLAGS> \
  -test sequential_write,random_write,full_scan,prefix_scan \
  -checkpoint-between-tests \
  -settle-before-scans \
  -treedb-cache-stats-before-reads \
  -range-queries 200 \
  -range-span 100 \
  -profile-dir "$RUN_ROOT/<row>_settled_scan"
```

This is the primary scan guardrail. Report full-scan ops/sec, prefix-scan
ops/sec, cache hits/misses/stores/evictions/admission counters, allocation
profiles, and final disk usage.

### Mixed/debt read behavior

```sh
./bin/unified-bench $COMMON_FLAGS <ROW_FLAGS> \
  -test sequential_write,random_write,random_read,full_scan,prefix_scan \
  -treedb-cache-stats-before-reads \
  -range-queries 200 \
  -range-span 100 \
  -profile-dir "$RUN_ROOT/<row>_mixed_debt_read_scan"
```

This intentionally omits settle/checkpoint boundaries before the read phase so
#2787/#2788 can see whether a candidate merely moves write work into subsequent reads
or scans. Any material read/scan regression here is blocking unless explicitly
accepted as opt-in-only.

## Required #2787/#2788 focused tests

Minimum local validation on the final #2787/#2788 candidate head:

```sh
git diff --check
go test ./TreeDB/db ./TreeDB/caching ./TreeDB/zipper ./cmd/unified_bench -count=1
go test ./TreeDB -run 'TestReopenVerify_(WALOn_Checkpoint|WALOn_WriteSync|WALOn_Checkpoint_LeafPagesInValueLog|LeafPageLogGroupedFrameCRCIntegrityModes)$' -count=1
go test ./TreeDB/db -run 'TestLeafPageReadCache(AdaptiveWriteAdmission|ConcurrentSetAssociativeAccess|WriteAdmissionImmediate)' -count=1
go test -race ./TreeDB/db -run 'TestLeafPageReadCache(AdaptiveWriteAdmissionSkipsWhenSlotLockContended|ConcurrentSetAssociativeAccess)' -count=1
python3 scripts/treedb_m14_matrix_summary.py --self-test
```

If #2787/#2788 changes runtime behavior, adds a selector, changes cache admission, or
changes benchmark semantics, add and run tests that exercise that exact behavior
under the candidate options. Docs-only final decisions can waive new runtime
tests with an explicit rationale.

## Metrics #2787/#2788 must report

For every row, include at least:

- throughput and latency for writes, point reads, full scans, and prefix scans;
- checkpoint timing: `flushmu_wait`, active background wait, `flush_all`,
  `value_log_flush`, `leaf_value_log_sync`, and reducer publish;
- old-leaf decode bytes/op, leaf merges/op, replacement pages/op, and append
  frames/op;
- span-native candidate/eligible/used/fallback counters and fallback reasons;
- backlog coalescing admitted/skipped counters and selected queue maxima;
- cache hits, misses, stores, evictions, entries, capacity, bytes, and
  `write_admission_*` counters;
- foreground assist/stall/backpressure counters;
- CPU, allocation, block, mutex, and checkpoint CPU top rows;
- disk usage for `index.db`, WAL/journal, `value_vlog`, and `leaf_vlog`.

## M4 handoff

#2787 can rely on this preflight contract after #2794/#2795 resolve. #2788 can
consume the resulting #2787 evidence and rely on:

- M1's opt-in adaptive cache-admission option, flag, and counters;
- M1's same-host point-read guardrail showing no material point-read regression
  for adaptive c4 in that focused shape;
- M2's conclusion that c4/c16 checkpoint debt remains unresolved;
- M3's default-off / opt-in-only decision for span-native/backlog;
- this report's row definitions and required metrics for final read/scan/reopen
  guardrails.

#2787 must still run after #2794/#2795, and #2788 must consume or rerun before
any final decision:

- same-host write/checkpoint rows for default, forced-off, c1, c4 immediate,
  c4 adaptive, and cache-disabled;
- settled point-read and settled scan rows for default/c4 immediate/c4
  adaptive/cache-disabled;
- mixed/debt read+scan rows for the same cache/span axes;
- focused reopen/read-integrity/cache tests on the final candidate head.

Until those rows pass without unaccepted read/scan/cache/checkpoint regressions,
and until #2794/#2795 resolve the checkpoint and admission no-gos, TreeDB
span-native/backlog/cache-admission changes remain workload-specific opt-ins and
must not become defaults.
