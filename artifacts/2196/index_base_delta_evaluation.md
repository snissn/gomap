# Issue #2196 TreeDB internal base-delta/index optimization evaluation

Date: 2026-06-02
Host/context: local darwin/arm64 manager worktree.
Base: `origin/main` at `94e322a0727f7a0d07154a006e9fa4ec4e3936f4`.
Candidate: `snissn/2196-manager`; exact latest PR head SHA is recorded in the PR body/final handoff to avoid a self-referential artifact SHA.

## Scope and conclusion

This pass keeps mmap and codec policy constant and evaluates only TreeDB index flags.

Conclusion: keep `index_internal_base_delta=false` for native fastpath runs that use
`index_outer_leaves_in_vlog=true`. The CLI accepts explicit
`-treedb-index-internal-base-delta=true`, but resolved options still disable it
because leaf-log child pages use explicit `LogRecordRef` child refs rather than
base-delta page child IDs. The aggregate `-treedb-index-optimizations` bundle can
still enable prefix-compressed/columnar/packed-pointer leaf encodings, but its
resolved `index_optimizations` line remains false whenever internal base-delta is
forced off by outer-leaf-vlog compatibility.

No default/profile policy change is recommended in this PR. The pager-leaf
control run where base-delta actually resolves on shows a random-read/full-scan
regression and visible `internalBaseDeltaMeta` CPU, which supports leaving #357
as future narrow search/decode work rather than enabling base-delta here.

## Commands and environment

Benchmark artifact root:

```text
/tmp/gomap_2196_index_eval_20260602_093447
```

Environment relevant to this matrix: no `TREEDB_*`, `GOMAP_*`, or `GOWORK`
variables were set in the benchmark shell (`env.txt` files are empty). Codec was
held explicit/declarative with `-treedb-vlog-compression=auto
-treedb-vlog-block-codec=snappy -treedb-vlog-auto-policy=balanced`; mmap policy
was default (no leaf mmap budget env overrides).

Native 1M-key common command shape:

```sh
./bin/unified-bench -dbs=treedb -keys=1000000 -profile fast \
  -path-label=native-fastpath -progress=false -read-workers=12 \
  -test dataset_write_sorted,batch_write,batch_random,random_read_parallel,full_scan,prefix_scan \
  -treedb-vlog-compression=auto -treedb-vlog-block-codec=snappy -treedb-vlog-auto-policy=balanced \
  <index flags> -profile-dir=<case-dir>
./bin/benchprof -profiles-dir <case-dir>
```

Pager-leaf control command shape used `-keys=300000` plus
`-treedb-index-outer-leaves-in-vlog=false -treedb-index-optimizations=true` and
toggled explicit `-treedb-index-internal-base-delta`.

Each case directory contains `command.txt`, `env.txt`, `run.log`,
`benchprof_results.{json,md}`, CPU/alloc/block/mutex pprof files, and focused
`cpu_*_focus_treedb.txt`/`allocs_*_top60.txt` summaries for selected tests.

## Matrix

Throughput is ops/sec from `benchprof_results.json`.

| case | keys | outer leaves | aggregate result | internal base-delta | index.db | leaf_vlog | sorted write | batch write | batch random | parallel read | full scan | prefix scan |
|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `native_aggregate_off` | 1,000,000 | true | false | false | 9 MiB | total=126 MiB files=8 value=126 MiB other=876 B | 2,228,791 | 5,904,193 | 2,301,382 | 1,074,291 | 4,066,718 | 567,892 |
| `native_aggregate_on` | 1,000,000 | true | false | false | 9 MiB | total=91 MiB files=6 value=91 MiB other=665 B | 2,464,819 | 2,774,796 | 1,695,524 | 1,800,924 | 2,264,561 | 1,098,912 |
| `native_internal_off` | 1,000,000 | true | false | false | 9 MiB | total=88 MiB files=6 value=88 MiB other=665 B | 2,557,402 | 6,150,273 | 2,462,836 | 2,672,713 | 1,623,467 | 834,565 |
| `native_internal_on` | 1,000,000 | true | false | false | 9 MiB | total=99 MiB files=8 value=99 MiB other=876 B | 593,915 | 3,956,635 | 1,243,613 | 2,627,074 | 5,846,655 | 1,522,822 |
| `pager_internal_off` | 300,000 | false | false | false | 125 MiB | n/a | 4,378,451 | 5,941,521 | 2,653,100 | 13,205,890 | 6,278,519 | 1,161,770 |
| `pager_internal_on` | 300,000 | false | true | true | 124 MiB | n/a | 4,429,305 | 5,765,710 | 3,913,839 | 5,922,339 | 3,608,295 | 927,300 |

Interpretation:

- In native outer-leaf-vlog mode, explicit internal base-delta on/off resolves to
  the same effective format (`index_internal_base_delta=false`). Differences
  among those rows are run-to-run noise, not a code-path delta.
- The aggregate leaf/index bundle reduces `leaf_vlog` bytes versus aggregate-off
  runs, but read/write/scan results are mixed in this single-run matrix. This PR
  therefore does not change aggregate defaults.
- In the pager-leaf control where internal base-delta is truly enabled, index.db
  shrinks only ~1 MiB at this scale while parallel random read and scans regress.

## CPU and allocation attribution

Selected pprof text summaries are under the case directories. Representative
random-read CPU attribution:

| case/test | selected attribution |
|---|---|
| `native_aggregate_off/random_read_parallel` | `SearchInternalChildRef` 1.00s cum / 6.13%; `SearchInternalChildID` 0.59s cum / 3.62%; `valuelog.File.ReadUnsafeTo` 0.65s cum / 3.99%; `leafPageReadCache.getViewLocked` 0.56s cum / 3.44%. |
| `native_internal_off/random_read_parallel` | `valueReader.ReadUnsafeTo` 1.57s cum / 13.00%; `SearchInternalChildRef` 1.03s cum / 8.53%; `SearchInternalChildID` 0.68s cum / 5.63%; `leafPageReadCache.getViewLocked` 0.61s cum / 5.05%. |
| `pager_internal_off/random_read_parallel` | `SearchInternalChildID` 0.19s cum / 15.83%. |
| `pager_internal_on/random_read_parallel` | `SearchInternalChildID` 0.37s cum / 26.81%; `internalBaseDeltaMeta` 0.11s cum / 7.97%; `internalBaseDeltaChildIDAtPtr` 0.01s cum / 0.72%. |

Representative scan attribution in native aggregate-on (`native_internal_off`):
`tree.Iterator.Next/loadCurrent/loadNodeRef` and value-log leaf reads dominate
full scan (`Iterator.Next` 0.33s cum / 26.61%, `valueReader.ReadUnsafeTo` 0.21s
cum / 16.94%, `GetLeafKeyFlagsView` 0.04s cum / 3.23%). Prefix scan profiles
were short/noisy; attribution was iterator construction/read-cache lookup, not a
new decode-table allocation path.

Allocation profiles did not show any new base-delta decode table/cache behavior
because this PR introduces no such cache. Native random-read allocation profiles
were dominated by memtable recycle/zipper setup samples; pager-control allocation
profiles were dominated by pprof/gzip snapshot writing.

## Correctness coverage added/confirmed

Added tests:

- `cmd/unified_bench`: explicit `-treedb-index-internal-base-delta=true` resolves
  false with outer leaves and reports the compatibility note; the same explicit
  flag resolves true when pager leaves are selected.
- `TreeDB`: reopen/churn test with `IndexOuterLeavesInValueLog=true`, leaf
  prefix/columnar/packed pointer flags enabled, and explicit
  `IndexInternalBaseDelta=true`; verifies persisted `format.json` forces
  `index_internal_base_delta=false`, confirms `leaf_vlog` bytes exist, checks
  gets, deletes, range delete, overwrites, checkpoint/reopen, and forward scan
  order.

Existing tests covering node-level base-delta search boundaries, split/compact,
fanout, reopen, and rewrite/vacuum compatibility remain in
`TreeDB/node/internal_base_delta_test.go`, `TreeDB/reopen_verify_test.go`, and
`TreeDB/db/*`.

Focused local validation:

```sh
GOWORK=off go test ./cmd/unified_bench -run 'TestBuildTreeDBOptions_(ExplicitInternalBaseDelta|IndexOptimizationsPerFlagOverride|ExplicitCompositeFalse)' -count=1
GOWORK=off go test ./TreeDB -run 'TestReopenVerify_(InternalBaseDelta_WALOn_Checkpoint|OuterLeavesExplicitInternalBaseDeltaDisabled_Churn)' -count=1
GOWORK=off go test ./TreeDB/node ./TreeDB ./cmd/unified_bench -count=1
GOWORK=off make unified-bench benchprof
```
