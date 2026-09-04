# TreeDB Typed-Storage Performance Guide

This guide explains how to choose TreeDB collection storage layouts, run the
current typed-storage benchmarks/profiles, and interpret counters honestly.
It is benchmark-adjacent documentation for a **pre-alpha** subsystem: APIs,
metadata, and on-disk formats may change, and the current numbers are not a
production performance claim.

All shell commands are runnable from the repository root unless explicitly
marked illustrative.

## Decision guide

| Workload/data shape | Recommended layout | Why |
| --- | --- | --- |
| Flexible document, point reads | Retained document | Simplest, schema-flexible, and avoids premature typed-storage policy. |
| Declared scalar fields, point reconstruction | Typed-row asset | Compact typed row path; good when full rows/documents are normally returned. |
| Int64 range aggregate/filter | Typed-column part | Min/max pruning and aggregate loops can avoid document and row materialization. |
| Vector search payloads | Typed-column dense section | Contiguous vector data and direct-view eligibility after validation. |
| Mixed metadata + vector | Typed-column vectors + typed-row/document metadata | Separates scoring/candidate search from final document fetch. |

Recommended starting point for new performance work:

1. Keep the document payload retained until you know which fields are hot.
2. Move row-shaped declared metadata to `typed_row_asset`.
3. Move scan/aggregate/vector payloads to `typed_column_part`.
4. Measure the direct typed path separately from setup, load, checkpoint/reopen,
   final document fetch, and public response materialization.

## Current optimized or landed paths

| Path | Current status | Evidence command |
| --- | --- | --- |
| Non-nullable int64 predicate scan | Landed scoped MVP; explicit API, not broad planner routing. | `BenchmarkTypedColumnInt64PredicateScan` and aggregate benchmark below. |
| Non-nullable int64 count/sum/avg aggregate | Landed benchmark/API path across no-filter, equality, tiny/range, wide range, all-pruned/all-match, tail, and clustered/reverse/partial/random/hotspot distributions; still allocation-heavy and not final. | `BenchmarkTypedColumnInt64PredicateAggregate`. |
| Grouped count/group-hour/min/max/span aggregate metadata over typed-column parts | Landed scoped MVP for insert-only string-group aggregate metadata when referenced columns are `typed_column_part` owners; count metadata may omit the value column, group-hour/min/max/span metadata uses an int64 value column, metadata is derived, generation/schema/part-bound, can declare exact string predicate coverage, and q1/q3/q4/q5-style metadata paths scan metadata entries instead of data rows. | `BenchmarkAggregateMetadataTypedColumnPart1786`, `BenchmarkPredicateQualifiedAggregateMetadataQ3DirectPrepared2892`, `BenchmarkPredicateQualifiedAggregateMetadataQ4Q5DirectPrepared1951`. |
| Dense fixed-dimension `float32_vector` typed-column sections | Landed durable publication/reconstruction and direct-view tests. | `BenchmarkTypedColumnVectorDense...` under `TreeDB/internal/typedcolumn`. |
| Dense fixed-degree `adjacency_list` typed-column sections | Landed durable publication/reconstruction for non-nullable owners with positive `adjacency_degree`; legacy dense direct-view reads remain fallback-only while explicit offsets-list variable adjacency uses the adapter direct-view path. | `BenchmarkTypedColumnAdjacencyDenseFallbackScan`, `BenchmarkTypedColumnAdapterVariableAdjacencyScan1917`. |
| Vector graph typed-column reads | Landed native-reader path for `column_graph`; HNSW adjacency now uses typed-column `uint32_list` vector-index state on the healthy path, with legacy row/source fallback quarantined. | [#1782](https://github.com/snissn/gomap/issues/1782), `cmd/treedb_column_graph_demo`, and column graph benchmarks. |

## Current caveats and linked work

| Area | What to say today | Tracker |
| --- | --- | --- |
| Aggregate hot path | The scalar aggregate path avoids document/row materialization, but still allocates and spends time in per-scan asset lifecycle, full-asset mapping/checking, metadata/dictionary decode, and int64 decode buffers. | [#1806](https://github.com/snissn/gomap/issues/1806) |
| Benchmark matrix | The aggregate benchmark matrix from #1808 is landed. Defaults cover `4096,65536` rows, `selective_range_1pct`, `all_pruned_no_match`, and `all_match` on `clustered_monotonic`; environment variables select no-filter/exact/tiny/wide/tail shapes, reverse/partial/random/hotspot distributions, optional fallback rows, and opt-in large rows. | [#1808](https://github.com/snissn/gomap/issues/1808) |
| Dictionary/string predicates | String dictionary data exists, but production string predicate scan MVP is separate work. | [#1785](https://github.com/snissn/gomap/issues/1785) |
| Aggregate metadata query integration | The #1786/#1951 path covers forced grouped min/max/span metadata queries for insert-only typed-column string+int64 parts, including exact predicate-qualified JSONBench q4/q5 metadata. Broader aggregate planning, mixed-owner metadata, nullable semantics, and SQL-style aggregates remain follow-ups. | [#1786](https://github.com/snissn/gomap/issues/1786), [#1951](https://github.com/snissn/gomap/issues/1951) |
| Resource/lifetime substrate | Mmap direct views must use mappedresource lifetime/range/endian/length/absolute-offset/alignment validation; heap-copy typed views are safe fallbacks but not zero-copy evidence. | [#1736](https://github.com/snissn/gomap/issues/1736), [#1893](https://github.com/snissn/gomap/issues/1893) |
| Row+column COW maintenance | Typed-row and typed-column asset GC/rewrite now use shared reachability, automatic active mappedresource pin protection, and fail-closed incomplete-plan checks. See the maintenance contract before running destructive cleanup. | [#1788](https://github.com/snissn/gomap/issues/1788), [typed asset maintenance spec](../spec/typed-asset-maintenance-1788.md) |
| Vector adjacency query switching | Authoritative dense and explicit offsets-list `adjacency_list` adapter publication/read paths are available, but graph/search direct-view routing is deferred. | [#1782](https://github.com/snissn/gomap/issues/1782), [#1901](https://github.com/snissn/gomap/issues/1901) |
| Vector kernels | SIMD/vectorized dense-section kernels are future optimization work, not a current requirement. | [#1790](https://github.com/snissn/gomap/issues/1790) |

## Benchmark commands

### Default typed-column int64 aggregate benchmark

Use this for a repeatable local signal. It runs the landed #1808 default matrix:
row counts `4096,65536`, distribution `clustered_monotonic`, shapes
`selective_range_1pct`, `all_pruned_no_match`, and `all_match`, with document
fallback rows included for the default row counts.

```sh
go test -run '^$' \
  -bench '^BenchmarkTypedColumnInt64PredicateAggregate$' \
  -benchmem \
  -benchtime=100x \
  -count=5 \
  ./TreeDB/collections
```

Fast smoke version:

```sh
go test -run '^$' \
  -bench '^BenchmarkTypedColumnInt64PredicateAggregate$' \
  -benchmem \
  -benchtime=1x \
  -count=1 \
  ./TreeDB/collections
```

Expected output shape:

```text
BenchmarkTypedColumnInt64PredicateAggregate/rows_4096/dist_clustered_monotonic/path_typed_column_part/shape_selective_range_1pct/timed_one_shot_api/read_integrity_cached_verify/execution_serial/predicate_count_sum_avg-8 ... dataset_rows ... setup_ns ... ns/op ... rows_scanned/op ... rows_matched/op ... blocks_pruned/op ... mapped_bytes/op ... decoded_bytes/op ... document_materializations/op ... fallback_count ... B/op ... allocs/op
BenchmarkTypedColumnInt64PredicateAggregate/rows_4096/dist_clustered_monotonic/path_document_full_scan_fallback/shape_selective_range_1pct/timed_one_shot_api/read_integrity_not_applicable/execution_serial/predicate_count_sum_avg-8 ... document_materializations/op ... row_materializations/op ... fallback_count ... fallback_reason_column_store_unavailable_count ... B/op ... allocs/op
```

Read the typed-column row and fallback row separately. The direct typed-column
row should show `document_materializations/op=0`, `row_materializations/op=0`,
`physical_row_asset_reads/op=0`, `physical_row_id_lookups/op=0`, and
`row_locator_decodes/op=0` for the aggregate path.

### Typed-column aggregate metadata MVP benchmark

Use this to measure the scoped metadata path for grouped
count/group-hour/min/max/span over `typed_column_part` string group columns and,
for group-hour/min/max/span, int64 value columns. The `prepared` variant
separates setup/metadata decode from the warmed reduce loop and should remain
low allocation.

```sh
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkAggregateMetadataTypedColumnPart1786$' \
  -benchmem \
  -benchtime=200ms \
  -count=1
```

Key counters: `metadata_hits/op`, `metadata_decoded_bytes/op`,
`mapped_bytes/op`, `heap_copy_bytes/op`, `rows_scanned/op`,
`row_materializations/op`, `document_materializations/op`,
`reconstruction_rows/op`, `topk_limit/op`, `topk_candidates/op`, and
`result_shape_ns/op`. Prepared aggregate metadata queries may request explicit
Top-K result shaping for int64 min/max/span results; this keeps all-groups
semantics unchanged unless callers set `TopK` and `TopKOrder`. Grouped count
metadata targets full q1-style group-count results. Grouped-hour metadata targets
q3-style `(collection, UTC hour)` counts with exact predicate coverage.

```sh
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkPredicateQualifiedAggregateMetadataQ3DirectPrepared2892$' \
  -benchmem \
  -benchtime=200ms \
  -count=1
```

### JSONBench q3 grouped-hour physical reducer

`ColumnPhysicalQueryGroupHourCount` covers the narrow q3-shaped physical reducer:
dictionary string predicates, a dictionary string group column, and UTC hour
extracted from an int64 timestamp column. The supported insert-only sidecar path
scans dictionary-code and int64 assets directly, reports zero document/row
materializations, and emits groups keyed by `Key` plus `Hour`. Dictionary-code
sidecars use a version-2 row-code payload: manifest-style metadata/dictionary
headers followed by zero padding and an aligned little-endian `uint32` stream.
The matching int64 value sidecars use a version-2 manifest-style header followed
by zero padding and an aligned little-endian `int64` stream. Row-touching
dictionary and int64 reducers use sidecar-specific payload-view helpers instead
of per-row manifest big-endian decoding.

The low-level comparison harness exposes this as
`q3_group_hour_count`:

```sh
TREEDB_JSONBENCH_COMPARE_ROWS=1000000 \
  go test ./experiments/colgranule \
    -run '^$' \
    -bench '^BenchmarkJSONBenchColumnStoreCompare/rows_1000000/treedb_column_store_prepared/q3_group_hour_count$' \
    -benchmem -benchtime=5x -count=1
```

### Selecting #1808 matrix shapes and distributions

Use the benchmark environment variables to cover shapes beyond the default
clustered selective/all-pruned/all-match signal:

```sh
TREEDB_TYPED_COLUMN_BENCH_ROWS=4096 \
TREEDB_TYPED_COLUMN_BENCH_SHAPES=no_filter,exact,tiny,range_1pct,range_10pct,all_pruned,all_match,tail \
TREEDB_TYPED_COLUMN_BENCH_DISTS=clustered,reverse,partial_clustered,random,hotspot \
TREEDB_TYPED_COLUMN_BENCH_INCLUDE_FALLBACK=false \
go test -run '^$' \
  -bench '^BenchmarkTypedColumnInt64PredicateAggregate$' \
  -benchmem \
  -benchtime=1x \
  -count=1 \
  ./TreeDB/collections
```

Canonical shape names in output are `no_filter_full_aggregate`, `exact_value`,
`tiny_range`, `selective_range_1pct`, `wide_range_10pct`,
`all_pruned_no_match`, `all_match`, and `tail_range`. Canonical distribution
names are `clustered_monotonic`, `reverse_monotonic`, `partially_clustered`,
`random_uniform`, and `hotspot_skewed`. Set
`TREEDB_TYPED_COLUMN_BENCH_LAYOUTS=delta,raw` (or `all`) to run the #1838
layout matrix: default delta-varint rows keep `path_typed_column_part`, while
raw fixed-width int64 rows use `path_typed_column_part_raw_int64`.

Rows `10,000,000` and `50,000,000` are intentionally gated. Only use the large
matrix when you have enough local time and disk space:

```sh
TREEDB_TYPED_COLUMN_BENCH_LARGE=true \
TREEDB_TYPED_COLUMN_BENCH_ROWS=10000000,50000000 \
TREEDB_TYPED_COLUMN_BENCH_SHAPES=range_1pct,all_match \
TREEDB_TYPED_COLUMN_BENCH_DISTS=clustered,random \
TREEDB_TYPED_COLUMN_BENCH_INCLUDE_FALLBACK=false \
go test -run '^$' \
  -bench '^BenchmarkTypedColumnInt64PredicateAggregate$' \
  -benchmem \
  -benchtime=3x \
  -count=1 \
  ./TreeDB/collections
```

### Repeatable script runbook

The repo-local wrapper below writes stable artifact names under `RUN_DIR` and is
safe for quick validation. It uses `GOWORK=off` by default.

Small smoke (direct typed-column path plus the small document fallback sample for
reason counters):

```sh
RUN_DIR=/tmp/treedb_typed_column_smoke_$(date +%Y%m%d_%H%M%S) \
ROWS=4096 BENCHTIME=1x COUNT=1 RUN_1M=false \
scripts/treedb_typed_column_bench_profile.sh
```

Primary smoke artifacts:

```text
$RUN_DIR/README.md
$RUN_DIR/smoke/typed_column_int64_aggregate_bench.txt
$RUN_DIR/hot_query_profile/hot_query_bench.txt
$RUN_DIR/hot_query_profile/hot_query_cpu.pprof
$RUN_DIR/hot_query_profile/hot_query_cpu_top.txt
$RUN_DIR/hot_query_profile/process_allocs.pprof
$RUN_DIR/hot_query_profile/process_allocs_top.txt
```

1M int64 matrix (all current shapes and distributions, direct typed-column path,
three execution labels, cached-verify read integrity, fallback disabled to keep
runtime bounded):

```sh
RUN_DIR=/tmp/treedb_typed_column_1m_$(date +%Y%m%d_%H%M%S) \
RUN_SMOKE=false RUN_1M=true RUN_HOT_PROFILE=false \
LAYOUTS_1M=delta,raw BENCHTIME_1M=3x COUNT_1M=1 \
scripts/treedb_typed_column_bench_profile.sh
```

Primary 1M artifact:

```text
$RUN_DIR/matrix_1m/typed_column_int64_aggregate_bench.txt
```

Optional 1M matrix expansions:

```sh
# Include unsafe checksum-skipping ceiling rows next to cached-verify rows.
RUN_DIR=/tmp/treedb_typed_column_1m_read_integrity_$(date +%Y%m%d_%H%M%S) \
RUN_SMOKE=false RUN_1M=true RUN_HOT_PROFILE=false READ_INTEGRITY_1M=all \
scripts/treedb_typed_column_bench_profile.sh

# Include document fallback rows at 1M only when you explicitly want the cost.
RUN_DIR=/tmp/treedb_typed_column_1m_fallback_$(date +%Y%m%d_%H%M%S) \
RUN_SMOKE=false RUN_1M=true RUN_HOT_PROFILE=false INCLUDE_FALLBACK_1M=true \
scripts/treedb_typed_column_bench_profile.sh
```

### Medium 1M-row selective aggregate

```sh
TREEDB_TYPED_COLUMN_BENCH_ROWS=1048576 \
TREEDB_TYPED_COLUMN_BENCH_SHAPES=selective_range_1pct \
TREEDB_TYPED_COLUMN_BENCH_DISTS=clustered_monotonic \
TREEDB_TYPED_COLUMN_BENCH_LAYOUTS=delta,raw \
TREEDB_TYPED_COLUMN_BENCH_INCLUDE_FALLBACK=false \
go test -run '^$' \
  -bench 'BenchmarkTypedColumnInt64PredicateAggregate/rows_1048576/dist_clustered_monotonic/path_(typed_column_part|typed_column_part_raw_int64)/shape_selective_range_1pct/timed_one_shot_api/read_integrity_cached_verify/execution_serial/predicate_count_sum_avg$' \
  -benchmem \
  -benchtime=20x \
  -count=3 \
  ./TreeDB/collections
```

Current interpretation for this shape:

- dataset rows: `1,048,576`;
- query is a clustered ~1% selective range, not a full aggregate;
- matches about `10,485` rows;
- decodes/scans `16,384` candidate rows;
- prunes `126/128` blocks on clustered data;
- currently maps/checks roughly `43 MiB` to decode roughly `16 KiB`;
- optimization is tracked by [#1806](https://github.com/snissn/gomap/issues/1806).

Do not extrapolate production performance from only this clustered/selective
shape. Compare it with the landed matrix shapes and distributions above,
especially `no_filter_full_aggregate`, `all_match`, `random_uniform`, and
`hotspot_skewed`, before drawing conclusions.

### Focused aggregate CPU and allocation profile

Prefer the script's prepared-session hot-query CPU profile when investigating the
scan loop boundary:

```sh
RUN_DIR=/tmp/treedb_typed_column_hot_$(date +%Y%m%d_%H%M%S) \
RUN_SMOKE=false RUN_1M=false RUN_HOT_PROFILE=true \
PROFILE_ROWS=65536 PROFILE_BENCHTIME=30000x PROFILE_COUNT=1 \
scripts/treedb_typed_column_bench_profile.sh
```

Inspect profiles:

```sh
go tool pprof -top $RUN_DIR/hot_query_profile/hot_query_cpu.pprof
go tool pprof -top -sample_index=alloc_space $RUN_DIR/hot_query_profile/process_allocs.pprof
```

The CPU profile uses the benchmark-owned
`TREEDB_TYPED_COLUMN_BENCH_HOT_CPU_PROFILE` boundary around the prepared-session
`Run` loop after setup, prepare, and warmup. Set it only with an exact single
sub-benchmark regex, as repeated matching sub-benchmarks would overwrite the same
profile path. The allocation profile is still Go
test process-wide (`-memprofile` cannot be scoped to only the hot loop here), so
use benchmark `B/op` and `allocs/op` as the hot-loop allocation signal and treat
`process_allocs.pprof` as attribution context. A fully hot-only allocation
profile remains follow-up work.

Manual equivalent for the hot CPU profile:

```sh
TREEDB_TYPED_COLUMN_BENCH_ROWS=65536 \
TREEDB_TYPED_COLUMN_BENCH_SHAPES=selective_range_1pct \
TREEDB_TYPED_COLUMN_BENCH_DISTS=clustered_monotonic \
TREEDB_TYPED_COLUMN_BENCH_INCLUDE_FALLBACK=false \
TREEDB_TYPED_COLUMN_BENCH_HOT_CPU_PROFILE=/tmp/typedscan_hot_cpu.pprof \
go test -run '^$' \
  -bench 'BenchmarkTypedColumnInt64PredicateAggregate/rows_65536/dist_clustered_monotonic/path_typed_column_part/shape_selective_range_1pct/timed_prepared_session_hot_scan/read_integrity_cached_verify/execution_serial/predicate_count_sum_avg$' \
  -benchmem \
  -benchtime=30000x \
  -count=1 \
  ./TreeDB/collections
```

Expected current profile themes are targeted section/range reads, metadata decode
that remains inside the prepared-session hot loop, and int64 decode/aggregate
work. If a profile instead shows document materialization dominating the direct
typed-column row, re-check that you are running the `path_typed_column_part` and
`timed_prepared_session_hot_scan` sub-benchmark, not fallback or one-shot setup.

### Dense vector typed-column smoke

```sh
go test -run '^$' \
  -bench 'BenchmarkTypedColumnVectorDense(DirectView|Section)Scan' \
  -benchmem \
  -benchtime=100x \
  -count=1 \
  ./TreeDB/internal/typedcolumn
```

Expected counters/columns include zero allocation for direct-view scan variants
and non-zero allocation for the safe decode/section scan variant. Direct views
are valid only while the mappedresource handle lifetime is live.

### Column graph native-reader demo

```sh
go run ./cmd/treedb_column_graph_demo \
  -dir /tmp/treedb-column-graph-doc-smoke \
  -reset \
  -rows 64 \
  -dims 8 \
  -degree 4 \
  -top-k 5 \
  -ef-search 32 \
  -max-decoded-blocks 2
```

Expected output shape:

```text
TreeDB column_graph native-reader demo
db_dir=/tmp/treedb-column-graph-doc-smoke rows=64 dims=8 degree=4 top_k=5 ef_search=32
rebuild status=column_graph_loaded loaded=true reason=
search path=column_graph_native_reader status=column_graph_loaded loaded=true results=5 include_docs=false doc_projection=none
stats candidates=... edges=... row_fetches=... cache_hits=... cache_misses=... decoded_blocks=... granules_touched=... physical_B=... max_resident_B=... docs_fetched=0 doc_output_B=0 doc_fields_skipped=0
```

`docs_fetched=0` is the expected search/scoring boundary when documents are not
requested. Use `-include-docs` when you want projected final document fetch
included; the demo excludes the embedding field by default. Add
`-include-doc-embedding` only for explicit full-document/embedding-echo
comparison runs.

### Unified-bench profile-dir workflow

Use this for raw TreeDB engine profile capture. It is not a typed-storage
collection benchmark by itself, but it is the standard profile artifact flow.

```sh
make unified-bench benchprof
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)
BENCH_PROFILE=fast # cross-DB benchmark preset, not a TreeDB server profile

./bin/unified-bench \
  -dbs treedb \
  -keys 80000 \
  -profile "$BENCH_PROFILE" \
  -checkpoint-between-tests \
  -test random_read,full_scan,prefix_scan \
  -profile-dir "$OUT" \
  -path-label native-fastpath \
  -progress=false

./bin/benchprof -profiles-dir "$OUT"
```

Expected artifacts:

```text
$OUT/benchprof_results.json
$OUT/benchprof_results.md
$OUT/cpu_<test>_<db>.pprof
$OUT/allocs_<test>_<db>.pprof
$OUT/checkpoint_cpu_checkpoint_<test>_<db>.pprof
$OUT/block.pprof
$OUT/mutex.pprof
$OUT/trace.out
$OUT/insights.md
$OUT/insights.json
$OUT/insights.html
```

## Counter interpretation

| Counter | Meaning | Best-practice interpretation |
| --- | --- | --- |
| `dataset_rows` | Rows loaded into the fixture before timing starts. | Use with benchmark name and env vars to compare like with like. |
| `setup_batches` | Insert batches used to build the fixture. | Setup metric only; not part of query-loop timing. |
| `setup_ns` | Fixture/load/setup duration before `ResetTimer`. | Keep separate from `ns/op`; useful for load-cost context, not hot-loop proof. |
| `typed_column_asset_bytes` | Bytes under the typed-column asset directory for typed-column rows. | Compare with decoded/mapped bytes to understand storage footprint. |
| `db_dir_bytes` | Total DB directory bytes for the fixture. | Includes more than typed-column payload; use for footprint context. |
| `rows_scanned/op` | Candidate rows whose predicate/aggregate payload was scanned or decoded. | Compare with total rows and `rows_matched/op`; low is good for selective/pruned shapes. |
| `rows_matched/op` | Rows satisfying the predicate. | Confirms selectivity; use with matches/sec. |
| `parts_considered/op` | Typed-column parts inspected by the query. | High counts can mean many generations/parts. |
| `parts_pruned/op` | Parts skipped by metadata. | Good for selective sorted data; not expected for all-match/no-filter. |
| `parts_decoded/op` | Parts that needed payload decode/view. | Should be less than considered when pruning works. |
| `blocks_considered/op` | Blocks/granules considered inside decoded parts. | Pairs with `blocks_pruned/op`. |
| `blocks_pruned/op` | Blocks skipped by min/max metadata. | Strong on clustered monotonic ranges; weak on random distributions. |
| `blocks_decoded/op` | Blocks whose payload was decoded/viewed. | Drives decode bytes and aggregate-loop work. |
| `mapped_bytes/op` | Bytes mapped or checked through typed asset read paths. | Current #1806 bottleneck: can scale with full asset size even when decoded bytes are tiny. |
| `physical_bytes_scanned/op` | Physical bytes touched/scanned for the path. | Should move toward section/block-targeted bytes in future optimizations. |
| `decoded_bytes/op` | Candidate payload bytes decoded into heap/scratch. | Compare with `mapped_bytes/op`; large gaps reveal validation/mapping overhead. |
| `heap_copy_bytes/op` | Bytes copied to heap fallback buffers. | Non-zero may be safe fallback or mapping denial; inspect reason/counters. |
| `document_materializations/op` | Public documents materialized in the timed path. | Should be zero for direct aggregate/scoring phases; non-zero belongs in final fetch benchmarks. |
| `row_materializations/op` | Typed rows materialized in the timed path. | Should be zero for direct aggregate hot loops. |
| `physical_row_asset_reads/op` | Typed-row physical asset reads. | Should be zero for direct typed-column aggregate paths. |
| `physical_row_id_lookups/op` | Row-ID lookup work. | Aggregate path should avoid it; point/document fetch may need it. |
| `row_locator_decodes/op` | Row locator decode work. | Should be zero for the direct aggregate path; locator work is tracked separately from column payload scans. |
| `fallback_count` | Numeric 0/1 indicator for whether the measured aggregate result used a fallback path. | Direct typed-column aggregate rows should report 0. Fallback rows report 1 and should be read as a separate baseline, not mixed with direct rows. |
| `fallback_reason_<reason>_count` | Reason-specific 0/1 fallback marker derived from `Diagnostics.FallbackReason`, for example `fallback_reason_column_store_unavailable_count` or `fallback_reason_typed_column_not_selected_count`. | Present only when a fallback reason is available in the result. Current int64 aggregate benchmark exposes fallback reasons, but semantic capability failures generally fail closed with an error instead of producing fallback rows. |

### Fallback and capability reason limits

For the int64 aggregate benchmark, small document fallback rows report
`fallback_count=1` plus a sanitized `fallback_reason_<reason>_count` metric when
`TREEDB_TYPED_COLUMN_BENCH_INCLUDE_FALLBACK=true`. Direct typed-column rows report
`fallback_count=0`.

Typed-column semantic capability reasons (for example nullable aggregate carrier
semantics or unsupported scalar operations) are not counted as benchmark fallback
reasons today because these paths fail closed before producing a timed fallback
row. Treat those as correctness/planner diagnostics from tests and errors, not as
performance fallback counters, until broader selection-engine integration lands.

### Mapped bytes vs decoded bytes

`mapped_bytes/op` is not the same as useful decoded payload. On the current
1M-row selective aggregate, `mapped_bytes/op` is about `43 MiB` while
`decoded_bytes/op` is about `16 KiB`. That means pruning is working logically,
but the read/validation path still touches too much physical asset data. This is
precisely the [#1806](https://github.com/snissn/gomap/issues/1806) optimization
space; do not hide it in docs or benchmark summaries.

## Read-integrity modes

| Mode | Use when | Caveat |
| --- | --- | --- |
| `verify` | Correctness-first validation and corruption checks. | More CPU work per read. |
| `cached_verify` | Repeated benchmark/query reads of immutable refs where first-use verification is acceptable. | Post-verification corruption may be missed until cache eviction or process restart. |
| `skip_checksums` | Unsafe ceiling benchmark only. | Do not use as correctness evidence; it can hide corruption. |

Best practice: report which mode you used. If you include `skip_checksums`, label
it a ceiling and keep verified/cached-verify evidence nearby.

## Timing boundaries to keep separate

| Phase | Include in benchmark? | Notes |
| --- | --- | --- |
| Setup/load | Usually no for hot query loops; yes for load benchmarks. | Report row count, seed/distribution, parts, DB dir size. |
| Flush/checkpoint/reopen | Include when proving durability or cold/reopened discovery. | Keep separate from steady-state query timing. |
| Search/scan/aggregate | Yes for query hot path. | Counters should prove document fetch/materialization boundaries. |
| Final document fetch/materialization | Only for public API end-to-end benchmarks. | Report `docs_fetched`, document materializations, B/op, allocs/op. |
| Profiling capture overhead | No special exclusion, but keep commands stable. | Use enough iterations for useful CPU/allocation samples. |

## Best-practice playbooks

### Document app

- **Persona/problem:** product collection with evolving JSON shape and mostly
  point reads.
- **Recommended layout:** retained document.
- **Command/snippet:** use the document-only metadata snippet in
  [collections-quickstart](collections-quickstart.md); the hybrid quickstart
  command in that guide is the runnable end-to-end smoke.
- **Expected counters:** no typed-column counters; point-get output should match
  stored JSON.
- **Why:** avoids schema churn and ownership mistakes.
- **When not:** range aggregates or vector search payloads dominate.

### Event analytics / int64 aggregate

- **Persona/problem:** time/event rows queried by range count/sum/avg.
- **Recommended layout:** `time_us` as non-nullable int64 `typed_column_part`,
  metadata as typed-row or retained payload.
- **Runnable command:** default aggregate benchmark above.
- **Expected counters:** direct row has zero document/row materialization;
  clustered selective runs prune blocks.
- **Why:** direct aggregate path avoids public document fetch for the count/sum/avg phase.
- **When not:** nullable semantics, string predicates, or broad aggregate query
  integration are required before the linked follow-ups land.

### Schema-aware typed-row

- **Persona/problem:** declared fields are reconstructed frequently with the
  document.
- **Recommended layout:** `typed_row_asset` for declared scalars plus retained
  payload for residual JSON.
- **Command/snippet:** use the typed-row metadata snippet in
  [collections-quickstart](collections-quickstart.md); keep the aggregate
  package benchmark for comparison when measuring against typed-column paths.
- **Expected counters:** typed-row reads may materialize rows/documents; do not
  compare them directly to aggregate-only loops.
- **Why:** good for point reconstruction without forcing column-major scan semantics.
- **When not:** a field is primarily scanned/aggregated without returning rows.

### Hybrid layout

- **Persona/problem:** event/search app with range filters, metadata, and final
  document fetch.
- **Recommended layout:** typed-column for `time_us` or vectors, typed-row or
  retained document for metadata and final payload.
- **Runnable command:** hybrid quickstart in
  [collections-quickstart](collections-quickstart.md).
- **Expected counters:** aggregate/search phase avoids document materialization;
  final point get/reopened get returns JSON.
- **Why:** separates candidate/aggregate work from final fetch.
- **When not:** all queries always need the entire document and there is no scan
  or scoring phase.

### Vector/RAG-style workload

- **Persona/problem:** embeddings plus metadata and final source document fetch.
- **Write-path check:** binary transport and typed storage metadata alone do not
  prove native ingestion. Verify that scalar/text/vector indexing and replay
  consume accepted typed values, rather than extracting them from retained JSON.
  Profile insert, replacement, and deletion as well as search; old-value
  reconstruction is still indexed-document work even when deferred to mutation.
- **Recommended layout:** fixed-dimension `float32_vector` as typed-column dense
  section, metadata in typed-row or retained document, derived graph as a
  generation-bound accelerator.
- **Runnable command:** `cmd/treedb_column_graph_demo` smoke above.
- **Expected counters:** search output should show `search path=column_graph_native_reader`
  and `docs_fetched=0` unless `-include-docs` is set; with `-include-docs`,
  `doc_projection=exclude_embedding` is the preferred response shape.
- **Why:** keeps vector scoring/candidate traversal separate from document fetch.
- **When not:** authoritative adjacency typed-column storage or SIMD kernels are
  required today; those are follow-ups.

For the typed indexed-write substrate, run this bounded collection-level check
from the repository root (place `TMPDIR` on the benchmark disk):

```sh
GOWORK=off go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkTypedMinimaPublic(Batch|Mutation)$' \
  -benchmem -benchtime=1x -count=5 -timeout=120s
```

It compares JSON and typed inputs under the explicit `command_wal_durable`
production profile, for equivalent typed-storage schemas and logical rows.
Batch sizes are 1/32/128; `indexes0` through `indexes3` add two
scalar indexes and then text indexing, with the vector column/index declaration
present throughout. Fixture preparation and mutation seeding are outside the
timer; admission plus the final collection flush are inside. Replacement and
deletion include old indexed-state maintenance. Normalize `B/op` and `allocs/op`
by batch size; repeated short samples can still vary with allocator/pool state.
Disk growth is incremental file size, not peak process memory or reclaimed-space
efficiency. These are write-substrate measurements, not prepared ANN readiness,
query throughput, or end-to-end Minima qualification.

Keep the existing vector-only guardrail in a separate process so combined-suite
warm-up does not silently change its measurement boundary:

```sh
GOWORK=off go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkInsertBatchValidatedFloat32ProjectionCohere768RetainedPayload$' \
  -benchmem -benchtime=1x -count=5 -timeout=120s
```

All three vector guardrail variants use the legacy validated-FP32 API; they are
not generic `InsertBatch` measurements. This historical fixture uses an
unprofiled backend with staged command-WAL append and excludes the final flush
from its timer. It is a matched legacy guardrail, not durable-at-ack throughput;
do not compare its absolute timings to the durable Minima batch benchmark.
Full-retained compatibility mode must
log the accepted FP32 plane independently of the document's vector, which can
differ. That extra durable representation is a correctness cost, not the
recommended new adapter layout. Supplying residual JSON, or using the complete
typed batch API for indexed strings and vectors, avoids retaining a full JSON
embedding solely as an intermediate representation.

The legacy implicit-residual buffered path can expose its submitted full JSON
while pending, then reconstruct from typed field owners after publication. A
conflicting document vector is therefore not a stable output oracle for that
transient state. Replay comparisons must check accepted typed values and compare
published output; new typed adapters should supply residual-only payloads.

### Performance-engineering workflow

- **Persona/problem:** engineer investigating a typed-column regression.
- **Recommended layout:** keep the workload shape fixed; collect benchmark and
  CPU/allocation profiles before changing code.
- **Runnable commands:** aggregate profile command above plus `go tool pprof`.
- **Expected counters:** record `ns/op`, `ops/sec`, `B/op`, `allocs/op`, rows/sec,
  matches/sec, mapped bytes, decoded bytes, and materialization counters.
- **Why:** prevents overfitting to one favorable shape and keeps bottlenecks visible.
- **When not:** if you are on an older branch that lacks a benchmark shape,
  mark the command as planned/illustrative instead of implying it was run.

## Troubleshooting

| Symptom | What it means | Action |
| --- | --- | --- |
| `document_materializations/op` is non-zero in a direct aggregate row | You may be measuring fallback or public response materialization. | Use the `typed_column_part` aggregate sub-benchmark and inspect the benchmark name. |
| `mapped_bytes/op` is much larger than `decoded_bytes/op` | Current read/validation path touches more asset data than the useful candidate payload. | Link to #1806; do not describe it as final optimized column-store performance. |
| `blocks_pruned/op` is zero on random data | Min/max pruning is ineffective when every block overlaps the predicate. | Report distribution; do not optimize only for clustered/selective data. |
| `allocs/op` remains high | Current metadata/decode/read lifecycle still allocates. | Capture allocation profile and identify whether decode, metadata, or materialization dominates. |
| Benchmark output has no typed-column rows | Regex may be matching the pre-#1808 sub-benchmark path or the branch may be stale. | Use `path_typed_column_part` in the regex and run `rg BenchmarkTypedColumnInt64PredicateAggregate TreeDB/collections`. |
| Profile files are missing from unified-bench | `-profile-dir` defaults to `-path-label native-fastpath`; explicit profile flags can override profile output defaults. | Use the exact unified-bench workflow above. |

## Glossary

- **Retained document / `document_payload`:** JSON/document bytes or residual
  payload used for reconstruction.
- **Typed storage:** umbrella term for typed-row and typed-column storage.
- **Typed-row storage / `typed_row_asset`:** row-record physical asset for
  declared typed fields and row locator/tombstone data.
- **Typed-column storage / `typed_column_part`:** sectioned column-major asset for
  explicit scalar/vector owners.
- **Derived accelerator:** non-authoritative sidecar/cache/index metadata derived
  from an authoritative owner; examples include dictionary-code assets,
  aggregate metadata, and vector graph assets.
- **Mapped bytes:** bytes mapped/checked through file-backed resource paths.
- **Decoded bytes:** useful candidate payload decoded or copied for the query.
- **Materialization:** creating rows or public documents; keep it out of pure
  aggregate/scoring timings unless measuring end-to-end API behavior.
- **COW maintenance:** copy-on-write reachability, rewrite, and deletion over
  immutable assets; full row+column typed-asset maintenance is tracked in #1788.
