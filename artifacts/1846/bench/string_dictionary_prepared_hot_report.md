# Issue 1846 S4: string/dictionary prepared-hot benchmark evidence

Environment: `goos=darwin`, `goarch=arm64`, Apple M3, package `./TreeDB/collections`, branch `snissn/1846-manager`.

Benchmark command:

```sh
TREEDB_TYPED_COLUMN_STRING_BENCH_ROWS_PER_PART=500000 \
  go test -run '^$' -bench 'BenchmarkTypedColumnStringPredicatePreparedHot' \
  -benchmem -benchtime=100x -count=1 ./TreeDB/collections
```

The timed loop reuses prepared typed-column state and calls the shared prepared dictionary kernel path (`scanTypedColumnStringPreparedPartWithVisibility` -> `typedkernel.SelectDictionaryCode` / `SelectDictionaryCodesIn`). Setup, asset reads, dictionary decode, and result-slice allocation are excluded from timing.

| shape | ns/op | ops/sec | rows/sec | matches/sec | B/op | allocs/op | dict bytes/session | dict bytes/op | mapped bytes/op | decoded bytes/op | physical bytes/op | rows scanned/op | rows matched/op | diagnostics |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| equality_selective | 455,262 | 2,196 | 287,799,924 | 35,132 | 0 | 0 | 542 | 0 | 0 | 1,179,680 | 1,179,680 | 131,072 | 16 | parts 2 considered / 1 decoded / 1 pruned; blocks 124 considered / 16 decoded / 108 pruned; kernel blocks 16; selection ranges 16; row/document materializations 0 |
| equality_all_match | 9,569,178 | 104.5 | 104,500,246 | 104,500,246 | 0 | 0 | 502 | 0 | 0 | 9,000,248 | 9,000,248 | 1,000,000 | 1,000,000 | parts 2 decoded; blocks 124 decoded; kernel blocks 124; selection all blocks 124; row/document materializations 0 |
| equality_all_pruned | 123.3 | 4,116,582 | 0 | 0 | 0 | 0 | 542 | 0 | 0 | 0 | 0 | 0 | 0 | parts 2 pruned before payload decode; blocks 124 considered / 124 pruned; kernel blocks 0; row/document materializations 0 |
| in_list_category | 3,787,502 | 264.0 | 132,007,015 | 66,003,507 | 0 | 0 | 562 | 0 | 0 | 4,500,124 | 4,500,124 | 500,000 | 250,000 | targets `kind_003,kind_007`; parts 1 decoded / 1 pruned; blocks 124 considered / 62 decoded / 62 pruned; kernel blocks 62; selection bitmap blocks 62; row/document materializations 0 |

Primary hot profile command:

```sh
ABS="$PWD/artifacts/1846/profiles/string_prepared_hot_1m_equality_selective_hot"
TREEDB_TYPED_COLUMN_STRING_BENCH_ROWS_PER_PART=500000 \
TREEDB_TYPED_COLUMN_STRING_HOT_PROFILE_PREFIX="$ABS" \
TREEDB_TYPED_COLUMN_STRING_HOT_PROFILE_MATCH=equality_selective \
  go test -run '^$' \
  -bench 'BenchmarkTypedColumnStringPredicatePreparedHot/rows_1000000/path_prepared_dictionary_kernel/equality_selective$' \
  -benchmem -benchtime=10000x -count=1 ./TreeDB/collections
```

CPU hot-path summary (`artifacts/1846/profiles/string_prepared_hot_1m_equality_selective_hot_cpu_top.txt`):

- `typedColumnStringPredicatePreparedHotRunner.scan` / `scanTypedColumnStringPreparedPartWithVisibility`: 95.93% cumulative.
- `typedkernel.SelectDictionaryCode` / `typedcolumn.(*GranuleReader).SelectUint32Code`: 79.13% / 78.88% cumulative.
- `typedcolumn.applyCodeRangeRows`: 52.93% cumulative; `typedcolumn.readValidUint32Code`: 25.70% cumulative while preserving fail-closed stored-code cardinality validation.
- `typedcolumn.(*GranuleReader).DecodeInt64Into`: 16.28% cumulative for primary-id materialization of selected blocks.
- No per-row string materialization, document materialization, or document reconstruction appears in the hot CPU profile or benchmark counters.

Allocation notes:

- The stable 100x prepared-hot benchmark reports `0 B/op` and `0 allocs/op` for all four shapes.
- Dictionary sections are session/prepared-state scoped (`dictionary_bytes/session`), with `dictionary_bytes_decoded/op=0` in the hot loop.
- The optional hot-profile run uses `runtime.MemProfileRate=1`; that instrumentation perturbs B/op (`1 B/op`, `0 allocs/op`) and the emitted alloc-space profile still includes setup/calibration samples from Go's benchmark process. Treat benchmem as authoritative for hot-loop allocations.

Artifacts:

- Raw 1M matrix: `artifacts/1846/bench/string_prepared_hot_1m_bench.txt`
- Primary hot profile run: `artifacts/1846/bench/string_prepared_hot_1m_equality_selective_hot_profile_run.txt`
- CPU profile/top: `artifacts/1846/profiles/string_prepared_hot_1m_equality_selective_hot_cpu.pprof`, `artifacts/1846/profiles/string_prepared_hot_1m_equality_selective_hot_cpu_top.txt`
- Allocation profile/top (instrumented; see note): `artifacts/1846/profiles/string_prepared_hot_1m_equality_selective_hot_allocs.pprof`, `artifacts/1846/profiles/string_prepared_hot_1m_equality_selective_hot_allocs_top.txt`

Capability/fallback stance:

- This chunk covers prepared-hot string equality and in-list/category dictionary scans using benchmark-local harness plumbing around the reviewed shared prepared dictionary kernel path.
- No new public query/group/count/count-distinct API was introduced. Group/count/count-distinct evidence remains limited to the already-supported internal dictionary count kernels; public API expansion is out of scope for #1846.
