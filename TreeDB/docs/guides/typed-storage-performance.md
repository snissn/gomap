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
| Non-nullable int64 count/sum/avg with range/equality predicate | Landed benchmark/API path; still allocation-heavy and not final. | `BenchmarkTypedColumnInt64PredicateAggregate`. |
| Dense fixed-dimension `float32_vector` typed-column sections | Landed durable publication/reconstruction and direct-view tests. | `BenchmarkTypedColumnVectorDense...` under `TreeDB/internal/typedcolumn`. |
| Vector graph typed-column reads | Landed native-reader path for `column_graph`; adjacency remains derived graph data, not authoritative typed-column `adjacency_list`. | [#1782](https://github.com/snissn/gomap/issues/1782), `cmd/treedb_column_graph_demo`, and column graph benchmarks. |

## Current caveats and linked work

| Area | What to say today | Tracker |
| --- | --- | --- |
| Aggregate hot path | The scalar aggregate path avoids document/row materialization, but still allocates and spends time in per-scan asset lifecycle, full-asset mapping/checking, metadata/dictionary decode, and int64 decode buffers. | [#1806](https://github.com/snissn/gomap/issues/1806) |
| Benchmark matrix | The original aggregate benchmark is a favorable clustered ~1% selective range; broader no-filter/exact/tiny/wide/all-pruned/all-match/tail/random/skew shapes are tracked separately. | [#1808](https://github.com/snissn/gomap/issues/1808) |
| Dictionary/string predicates | String dictionary data exists, but production string predicate scan MVP is separate work. | [#1785](https://github.com/snissn/gomap/issues/1785) |
| Aggregate metadata query integration | Aggregate metadata descriptors exist, but broader query integration is separate work. | [#1786](https://github.com/snissn/gomap/issues/1786) |
| Resource/lifetime substrate | Direct views must use mappedresource lifetime/range/endian/length/alignment validation; broad DB-owned adoption remains in the parent tracker. | [#1736](https://github.com/snissn/gomap/issues/1736) |
| Row+column COW maintenance | Full destructive maintenance over typed-row and typed-column assets is not complete. Do not assume unsafe delete/rewrite of pinned assets is allowed. | [#1788](https://github.com/snissn/gomap/issues/1788) |
| Vector adjacency | `adjacency_list` authoritative typed-column publication remains fail-closed until a physical shape is specified. | [#1783](https://github.com/snissn/gomap/issues/1783) |
| Vector kernels | SIMD/vectorized dense-section kernels are future optimization work, not a current requirement. | [#1790](https://github.com/snissn/gomap/issues/1790) |

## Benchmark commands

### Default typed-column int64 aggregate benchmark

Use this for a repeatable local signal. It runs default row counts and includes
both the direct typed-column path and document fallback comparison.

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
BenchmarkTypedColumnInt64PredicateAggregate/rows_4096/typed_column_part/predicate_count_sum_avg-8 ... ns/op ... rows_scanned/op ... rows_matched/op ... blocks_pruned/op ... mapped_bytes/op ... decoded_bytes/op ... document_materializations/op ... B/op ... allocs/op
BenchmarkTypedColumnInt64PredicateAggregate/rows_4096/document_full_scan_fallback/predicate_count_sum_avg-8 ... document_materializations/op ... row_materializations/op ... B/op ... allocs/op
```

Read the typed-column row and fallback row separately. The direct typed-column
row should show `document_materializations/op=0`, `row_materializations/op=0`,
`physical_row_asset_reads/op=0`, `physical_row_id_lookups/op=0`, and
`row_locator_decodes/op=0` for the aggregate path.

### Medium 1M-row selective aggregate

```sh
TREEDB_TYPED_COLUMN_BENCH_ROWS=1048576 \
go test -run '^$' \
  -bench '^BenchmarkTypedColumnInt64PredicateAggregate/rows_1048576/typed_column_part/predicate_count_sum_avg$' \
  -benchmem \
  -benchtime=20x \
  -count=3 \
  ./TreeDB/collections
```

Current interpretation for this shape:

- dataset rows: `1,048,576`;
- query is a ~1% selective range, not a full aggregate;
- matches about `10,485` rows;
- decodes/scans `16,384` candidate rows;
- prunes `126/128` blocks on clustered data;
- currently maps/checks roughly `43 MiB` to decode roughly `16 KiB`;
- optimization is tracked by [#1806](https://github.com/snissn/gomap/issues/1806).

Do not extrapolate production performance from only this clustered/selective
shape. Use [#1808](https://github.com/snissn/gomap/issues/1808) matrix shapes
when that benchmark coverage is present in your branch.

### Focused aggregate CPU and allocation profile

This command profiles the current aggregate path with setup outside the timed
loop but includes the per-operation read/validation work that the benchmark is
measuring.

```sh
TREEDB_TYPED_COLUMN_BENCH_ROWS=65536 \
go test -run '^$' \
  -bench '^BenchmarkTypedColumnInt64PredicateAggregate/rows_65536/typed_column_part/predicate_count_sum_avg$' \
  -benchmem \
  -benchtime=30000x \
  -count=1 \
  -cpuprofile /tmp/typedscan_aggregate_cpu.pprof \
  -memprofile /tmp/typedscan_aggregate_mem.pprof \
  ./TreeDB/collections
```

Inspect profiles:

```sh
go tool pprof -top /tmp/typedscan_aggregate_cpu.pprof
go tool pprof -top -sample_index=alloc_space /tmp/typedscan_aggregate_mem.pprof
```

Expected current profile themes are per-scan asset read/open/fstat/checksum,
metadata/dictionary decode, and int64 decode allocation. If a profile instead
shows document materialization dominating the direct typed-column row, re-check
that you are running the `typed_column_part` sub-benchmark and not the fallback.

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
search path=column_graph_native_reader status=column_graph_loaded loaded=true results=5 include_docs=false
stats candidates=... edges=... row_fetches=... cache_hits=... cache_misses=... decoded_blocks=... granules_touched=... physical_B=... max_resident_B=... docs_fetched=0
```

`docs_fetched=0` is the expected search/scoring boundary when documents are not
requested. Use `-include-docs` only when you want final document fetch included.

### Unified-bench profile-dir workflow

Use this for raw TreeDB engine profile capture. It is not a typed-storage
collection benchmark by itself, but it is the standard profile artifact flow.

```sh
make unified-bench benchprof
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -keys 80000 \
  -profile fast \
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
- **Recommended layout:** fixed-dimension `float32_vector` as typed-column dense
  section, metadata in typed-row or retained document, derived graph as a
  generation-bound accelerator.
- **Runnable command:** `cmd/treedb_column_graph_demo` smoke above.
- **Expected counters:** search output should show `search path=column_graph_native_reader`
  and `docs_fetched=0` unless `-include-docs` is set.
- **Why:** keeps vector scoring/candidate traversal separate from document fetch.
- **When not:** authoritative adjacency typed-column storage or SIMD kernels are
  required today; those are follow-ups.

### Performance-engineering workflow

- **Persona/problem:** engineer investigating a typed-column regression.
- **Recommended layout:** keep the workload shape fixed; collect benchmark and
  CPU/allocation profiles before changing code.
- **Runnable commands:** aggregate profile command above plus `go tool pprof`.
- **Expected counters:** record `ns/op`, `ops/sec`, `B/op`, `allocs/op`, rows/sec,
  matches/sec, mapped bytes, decoded bytes, and materialization counters.
- **Why:** prevents overfitting to one favorable shape and keeps bottlenecks visible.
- **When not:** if the branch lacks the benchmark shape, mark the command as
  planned/illustrative instead of implying it was run.

## Troubleshooting

| Symptom | What it means | Action |
| --- | --- | --- |
| `document_materializations/op` is non-zero in a direct aggregate row | You may be measuring fallback or public response materialization. | Use the `typed_column_part` aggregate sub-benchmark and inspect the benchmark name. |
| `mapped_bytes/op` is much larger than `decoded_bytes/op` | Current read/validation path touches more asset data than the useful candidate payload. | Link to #1806; do not describe it as final optimized column-store performance. |
| `blocks_pruned/op` is zero on random data | Min/max pruning is ineffective when every block overlaps the predicate. | Report distribution; do not optimize only for clustered/selective data. |
| `allocs/op` remains high | Current metadata/decode/read lifecycle still allocates. | Capture allocation profile and identify whether decode, metadata, or materialization dominates. |
| Benchmark output has no typed-column rows | Branch may not contain the landed benchmark, or regex is wrong. | Run `rg BenchmarkTypedColumnInt64PredicateAggregate TreeDB/collections`. |
| Profile files are missing from unified-bench | `-profile-dir` requires `-path-label`; explicit profile flags can override defaults. | Use the exact unified-bench workflow above. |

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
