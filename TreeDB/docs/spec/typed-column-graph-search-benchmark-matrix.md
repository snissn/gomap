# Typed-Column Graph-Search Benchmark Truth Matrix (#2037)

Status: pre-alpha benchmark contract. This document owns the stable labels and
reporting boundaries for comparing legacy/direct graph-row search, current
TVIS/base typed-column search, and the #2045 combined prepared typed-column graph
search. #2038 admits prepared adjacency CSR counters, #2040 admits prepared
base-vector and inverse-norm scoring counters, #2041 admits row-ref and
document-ID side-channel counters, and #2045 reports combined prepared routing
with `prepared_graph_search_views/search` and `graph_row_fallbacks/search`.

## Command

Use the truth-matrix benchmark when collecting comparable graph-search evidence:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphSearchTruthMatrix2037$' \
  -benchmem \
  -benchtime=500ms \
  -count=5
```

For smoke validation, `-benchtime=1x -count=1` is acceptable, but publish only
full runs for performance claims. Use the same hardware, fixture, Go version,
`GOMAXPROCS`, and command when comparing commits.

The #1926/#2454/#2845 quantized score-plane closeout has its own
mode-comparison harness because it compares exact/default scoring, legacy
scalar_u8, explicit per-granule-alpha scalar_u8, and pure-Go `rabitq_1bit`
`quantized_only` / `quantized_rerank` rows within the current `column_graph`
route rather than comparing legacy/current source paths:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^(BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926|BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414|BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedAlphaSearchWithBuffer2414|BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8QuantizedAlpha2415|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452|BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926|BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450)$' \
  -benchmem -benchtime=100x -count=3
```

Those rows report `ns/op`, `ops/sec`, `B/op`, `allocs/op`, `recall_at_k_pct`,
`candidates/search`, `quantized_rerank_candidates/search`,
`quantized_rerank_exact_score_calls/search`, `quantized_code_B/search`,
`vector_B/search`, `norm_B/search`, `quantized_asset_B/vector`,
`quantized_score_codec_scalar_u8_alpha/search`, and rebuild storage metrics.
They are quantized score-plane evidence and must not be relabeled as
batch/control-flow/windowing speedup evidence; #2454 also records that RaBitQ
go-highway acceleration did not land, and #2845 records that alpha remains
explicit rather than the default.

## Stable row labels

Sub-benchmark names are the contract:

```text
mode=<mode>/boundary=<boundary>/concurrency=<serial|parallel>/fixture=<fixture>
```

Modes:

- `legacy_direct_graph_row`: compatibility/control path backed by explicit
  physical graph rows where the fixture can still publish them. Healthy current
  promotion evidence must not rely on this mode.
- `current_tvis_base_typed_column`: current source path using TVIS/vector-index
  typed-column state and base typed-column vectors. After #2045, healthy
  current-format readers route through the combined prepared view and report
  `prepared_graph_search_views/search=1`.
- `combined_prepared_typed_column`: explicit #2045 evidence rows for the same
  healthy current-format combined prepared route. These rows are supported and
  must show zero graph-row/source fallback counters.

Boundaries:

- `setup_open_prepare`: searcher open, manifest discovery, current direct-source
  binding, validation/certification work, and prepared-view construction once it
  exists; no search.
- `graph_only`: lower-level HNSW traversal/scoring/top-k with final result-ID and
  row-ref materialization omitted by the #2037 benchmark hook.
- `result_id`: opened public searcher, search plus final top-k result-ID/row-ref
  materialization, no documents.
- `document_materialization`: opened public searcher, search plus final document
  fetch/materialization.

Fixtures:

- `production8192`: 8192 rows, 128 dimensions, degree 16, `topK=10`,
  `efSearch=128`; used for graph-only serial/parallel source comparisons.
- `serving1024`: 1024 rows, 128 dimensions, degree 16, `topK=10`,
  `efSearch=128`; used for setup/open, result-ID, and document-materialization
  serving-boundary rows.

## Current row matrix

| Mode | Boundary | Concurrency | Fixture | Current behavior |
| --- | --- | --- | --- | --- |
| `legacy_direct_graph_row` | `setup_open_prepare` | serial | `serving1024` | Supported control row. |
| `current_tvis_base_typed_column` | `setup_open_prepare` | serial | `serving1024` | Supported current row. |
| `combined_prepared_typed_column` | `setup_open_prepare` | serial | `serving1024` | Supported #2045 combined prepared setup/open row. |
| `legacy_direct_graph_row` | `graph_only` | serial/parallel | `production8192` | Supported control rows where explicit graph rows are published by the fixture. |
| `current_tvis_base_typed_column` | `graph_only` | serial/parallel | `production8192` | Supported current rows; #2040 vector/norm prepared counters should cover healthy scoring when mmap direct views are available. |
| `combined_prepared_typed_column` | `graph_only` | serial/parallel | `production8192` | Supported #2045 combined prepared graph-only rows. |
| `current_tvis_base_typed_column` | `result_id` | serial/parallel | `serving1024` | Supported current rows. |
| `combined_prepared_typed_column` | `result_id` | serial/parallel | `serving1024` | Supported #2045 combined prepared result-ID rows. |
| `current_tvis_base_typed_column` | `document_materialization` | serial/parallel | `serving1024` | Supported current rows. |
| `combined_prepared_typed_column` | `document_materialization` | serial/parallel | `serving1024` | Supported #2045 combined prepared document-materialization rows. |

Legacy/direct graph-row result-ID and document-materialization rows are omitted
unless a future fixture can expose that path without silently using the current
TVIS/base typed-column source. Do not relabel current typed-column public rows as
legacy controls.

## Required reported metrics

Every supported row must report Go's `ns/op`, `B/op`, and `allocs/op`, plus the
explicit `ops/sec` metric emitted by the benchmark helper. Benchmark summaries
must also carry the relevant domain/source counters:

- fixture/search shape: `rows`, `dims`, `degree`, `top_k`, `ef_search`,
  `parallel_workers` where applicable;
- graph work: `graph_rows`, `candidate_rows/search`, `candidates/search`,
  `edges/search`, `visited_edges/search`, `result_fetches/search`;
- direct/fallback source counters: `vector_mmap_direct/search`,
  `vector_heap_copy_typed_view/search`, `vector_scratch_decodes/search`,
  `vector_prepared_direct/search`, `vector_prepared_identity_mapping/search`,
  `vector_prepared_row_ref_mapping/search`,
  `typed_column_vector_fallbacks/search`, `norm_mmap_direct/search`,
  `norm_heap_copy_typed_view/search`, `norm_scratch_decodes/search`,
  `norm_prepared_direct/search`, `norm_source_fallbacks/search`,
  `prepared_score_calls/search`, `score_float64_fallbacks/search`,
  `prepared_graph_search_views/search`, `graph_row_fallbacks/search`,
  `adjacency_prepared_csr_mmap_direct/search`,
  `adjacency_prepared_csr_direct_views/search`,
  `adjacency_typed_list_mmap_direct/search`,
  `adjacency_typed_list_heap_copy_typed_view/search`,
  `adjacency_typed_list_scratch_decodes/search`,
  `adjacency_legacy_fallbacks/search`, and
  `adjacency_source_fallbacks/search`;
- result/document side channels: `result_id_prepared_bytes_views/search`,
  `result_id_typed_bytes_state/search`, `result_id_graph_fallbacks/search`,
  `row_ref_vector_source_state/search`,
  `row_ref_vector_source_legacy_graph_ids/search`,
  `row_ref_state_prepared_views/search`,
  `row_ref_state_mmap_direct_fields/search`,
  `row_ref_state_result_refs/search`, `row_ref_state_source_fallbacks/search`,
  `docs_fetched/search`, `doc_fetch_ns/search`,
  `doc_row_ref_state_fetches/search`, and
  `doc_row_ref_lookup_fallbacks/search`.

Healthy current-format evidence must keep graph-row/fallback counters at zero
where those counters apply: `graph_rows=0` for current typed-column graph-only
rows, `prepared_graph_search_views/search=1`, `graph_row_fallbacks/search=0`,
`typed_column_vector_fallbacks/search=0`,
`row_ref_vector_source_legacy_graph_ids/search=0`,
`result_id_graph_fallbacks/search=0`, `adjacency_legacy_fallbacks/search=0`, and
`adjacency_source_fallbacks/search=0`.

## #2043 closeout evidence

Closeout run context:

- Code benchmarked: `af003dfaf255ae217dbec6eb4a3afae08c2aa4aa`
  (`origin/main` after #2042, branch `snissn/2043-manager`; this closeout PR is
  docs/test-only after the run).
- Command: `GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections -run '^$' -bench '^BenchmarkColumnVectorGraphSearchTruthMatrix2037$' -benchmem -benchtime=500ms -count=5`.
- Hardware/context: macOS 26.2, Apple M3, 8 logical CPUs, 16 GiB memory,
  `go version go1.25.5 darwin/arm64`.
- Fixture shapes: `production8192` graph-only rows use 8192 rows, 128 dims,
  degree 16, `topK=10`, `efSearch=128`; `serving1024` setup/result/document rows
  use 1024 rows with the same dims/degree/top-k/ef.
- Local artifact directory: `/tmp/gomap_2043_final_20260531_110824/` with
  `truth_matrix_2037_bench.txt`, `truth_matrix_2037_benchstat.txt`, focused CPU
  profiles, and allocation profiles. The table below records medians from the
  five-count matrix run; the raw artifacts carry every emitted counter.

| Mode | Boundary | Conc. | Fixture/stats | ns/op | ops/sec | B/op | allocs/op | Key admission counters (median/search) |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| `legacy graph-row` | `setup_open_prepare` | `serial` | `serving1024` | 351.266 µs | 2,847 | 473,034 | 562 | `graph_rows=1024`, `prepared_graph_search_views=0`, `graph_row_fallbacks=0` |
| `current TVIS/base typed-column` | `setup_open_prepare` | `serial` | `serving1024` | 1.447 ms | 691 | 1,659,896 | 2,052 | `graph_rows=0`, `prepared_graph_search_views=1`, `graph_row_fallbacks=0` |
| `combined prepared` | `setup_open_prepare` | `serial` | `serving1024` | 1.149 ms | 870 | 1,673,748 | 2,052 | `graph_rows=0`, `prepared_graph_search_views=1`, `graph_row_fallbacks=0` |
| `legacy graph-row` | `graph_only` | `serial` | `production8192` | 8.129 µs | 123,015 | 0 | 0 | compatibility control: `graph_rows=8192`, `graph_row_fallbacks=128`, `visited_edges=612` |
| `legacy graph-row` | `graph_only` | `parallel` | `production8192` | 1.967 µs | 508,284 | 0 | 0 | compatibility control: `graph_rows=8192`, `graph_row_fallbacks=128`, `visited_edges=612` |
| `current TVIS/base typed-column` | `graph_only` | `serial` | `production8192` | 15.069 µs | 66,362 | 0 | 0 | `graph_rows=0`, `prepared=1`, `graph_row_fallbacks=0`, `vec/norm=182/182`, `adj=108`, `row_ref=1`, `id_view=1`, `visited_edges=3340` |
| `current TVIS/base typed-column` | `graph_only` | `parallel` | `production8192` | 3.438 µs | 290,868 | 0 | 0 | `graph_rows=0`, `prepared=1`, `graph_row_fallbacks=0`, `vec/norm=182/182`, `adj=108`, `row_ref=1`, `id_view=1`, `visited_edges=3340` |
| `combined prepared` | `graph_only` | `serial` | `production8192` | 14.420 µs | 69,348 | 0 | 0 | `graph_rows=0`, `prepared=1`, `graph_row_fallbacks=0`, `vec/norm=182/182`, `adj=108`, `row_ref=1`, `id_view=1`, `visited_edges=3340` |
| `combined prepared` | `graph_only` | `serial` | `production8192/minimal` | 12.266 µs | 81,527 | 0 | 0 | `graph_rows=0`, `prepared=1`, `graph_row_fallbacks=0`, `vec/norm=182/182`, `adj=108`, `row_ref=1`, `id_view=1`, `visited_edges=3340` |
| `combined prepared` | `graph_only` | `parallel` | `production8192` | 3.384 µs | 295,482 | 0 | 0 | `graph_rows=0`, `prepared=1`, `graph_row_fallbacks=0`, `vec/norm=182/182`, `adj=108`, `row_ref=1`, `id_view=1`, `visited_edges=3340` |
| `combined prepared` | `graph_only` | `parallel` | `production8192/minimal` | 3.229 µs | 309,716 | 0 | 0 | `graph_rows=0`, `prepared=1`, `graph_row_fallbacks=0`, `vec/norm=182/182`, `adj=108`, `row_ref=1`, `id_view=1`, `visited_edges=3340` |
| `current TVIS/base typed-column` | `result_id` | `serial` | `serving1024` | 14.880 µs | 67,206 | 784 | 2 | `graph_rows=0`, `prepared=1`, `result_id_prepared_bytes_views=1`, `result_id_typed_bytes_state=10`, `result_id_graph_fallbacks=0`, `row_ref=1` |
| `current TVIS/base typed-column` | `result_id` | `parallel` | `serving1024` | 3.430 µs | 291,584 | 784 | 2 | `graph_rows=0`, `prepared=1`, `result_id_prepared_bytes_views=1`, `result_id_typed_bytes_state=10`, `result_id_graph_fallbacks=0`, `row_ref=1` |
| `combined prepared` | `result_id` | `serial` | `serving1024` | 14.717 µs | 67,951 | 784 | 2 | `graph_rows=0`, `prepared=1`, `result_id_prepared_bytes_views=1`, `result_id_typed_bytes_state=10`, `result_id_graph_fallbacks=0`, `row_ref=1` |
| `combined prepared` | `result_id` | `parallel` | `serving1024` | 3.607 µs | 277,257 | 784 | 2 | `graph_rows=0`, `prepared=1`, `result_id_prepared_bytes_views=1`, `result_id_typed_bytes_state=10`, `result_id_graph_fallbacks=0`, `row_ref=1` |
| `current TVIS/base typed-column` | `document_materialization` | `serial` | `serving1024` | 104.664 µs | 9,554 | 92,156 | 319 | `graph_rows=0`, `prepared=1`, `docs_fetched=10`, `doc_row_ref_state_fetches=10`, `doc_row_ref_lookup_fallbacks=0` |
| `current TVIS/base typed-column` | `document_materialization` | `parallel` | `serving1024` | 26.618 µs | 37,569 | 92,163 | 319 | `graph_rows=0`, `prepared=1`, `docs_fetched=10`, `doc_row_ref_state_fetches=10`, `doc_row_ref_lookup_fallbacks=0` |
| `combined prepared` | `document_materialization` | `serial` | `serving1024` | 119.841 µs | 8,344 | 92,156 | 319 | `graph_rows=0`, `prepared=1`, `docs_fetched=10`, `doc_row_ref_state_fetches=10`, `doc_row_ref_lookup_fallbacks=0` |
| `combined prepared` | `document_materialization` | `parallel` | `serving1024` | 26.901 µs | 37,173 | 92,165 | 319 | `graph_rows=0`, `prepared=1`, `docs_fetched=10`, `doc_row_ref_state_fetches=10`, `doc_row_ref_lookup_fallbacks=0` |

The current-format rows also emitted zero for
`typed_column_vector_fallbacks/search`, `adjacency_legacy_fallbacks/search`,
`adjacency_source_fallbacks/search`,
`row_ref_vector_source_legacy_graph_ids/search`,
`row_ref_state_source_fallbacks/search`, and `result_id_graph_fallbacks/search`.
After #2045 the `current_tvis_base_typed_column` label is no longer an
unprepared hot-loop source route in healthy current-format readers; it is the
continuity label proving that current TVIS/base typed-column search now selects
one combined prepared view. Use the historical #2035/#2040/#2042 artifacts, not
this final same-codebase row, when comparing against the pre-#2045 unprepared
source implementation.

The `graph_only/production8192` truth-matrix throughput rows are not
apples-to-apples storage-path evidence by themselves. The legacy/direct control
uses the synthetic physical-asset ring fixture and visits 612 edges/search; the
current-format and combined-prepared rows rebuild HNSW state and visit 3340
edges/search. Keep those rows as admission/fallback telemetry, not as proof that
prepared CSR adjacency is slower than graph-row adjacency.

### #2043 adjacency-access microbenchmark evidence

A focused permanent microbenchmark times adjacency lookup/expansion only. It uses
the same physical-asset plus vector-index-state fixture for both rows, fixes the
same 128 graph ordinals, and reads the same 2048 layer-0 edges/op. The prepared
row calls vector-index-state prepared CSR `Neighbors(layer, ordinal)` directly;
the graph-row compatibility row calls the raw graph-row adjacency accessor and
extracts layer 0. The benchmark does not call `SearchCosine` and does not execute
scoring, frontier/top-k, result materialization, or prepared vector/norm paths.

```sh
OUT=/tmp/gomap_2043_adjacency_micro_20260531_122457
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphAdjacencyAccessApplesToApples2043' \
  -benchmem -benchtime=1s -count=5 | tee "$OUT/adjacency_access_apples_to_apples_2043_bench.txt"
```

| Row | ns/op median | ops/sec median | B/op | allocs/op | expansions/op | edges/op | edges/expansion | Adjacency-access counters |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `state_prepared_csr_adjacency` | 1.252 µs | 798,843 | 0 | 0 | 128 | 2048 | 16 | `prepared_csr_mmap_direct=128`, `direct_views=128`, `scratch_decodes=0`, `legacy_graph_row_decodes=0`, `source_fallbacks=0` |
| `graph_row_adjacency_decode` | 3.331 µs | 300,205 | 0 | 0 | 128 | 2048 | 16 | `prepared_csr_mmap_direct=0`, `direct_views=0`, `scratch_decodes=128`, `legacy_graph_row_decodes=128`, `source_fallbacks=0` |

Interpretation: when topology, ordinals, and edge count are held constant for
adjacency access alone, prepared CSR adjacency is faster than graph-row adjacency
decode in this local run and remains zero-allocation. This narrower evidence
shows prepared CSR adjacency access is not the cause of the truth-matrix 5.5x
visited-edge mismatch. It is not an end-to-end search benchmark and must not be
used as a full `SearchCosine` throughput claim. The larger full truth-matrix
wall-time gap remains a separate topology/search-work comparison for #1979 and
follow-up optimization work.

### #2043 profile and allocation evidence

Focused graph-only profile commands used the combined prepared minimal-stats row:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphSearchTruthMatrix2037/mode=combined_prepared_typed_column/boundary=graph_only/concurrency=serial/fixture=production8192/stats=minimal$' \
  -benchmem -benchtime=30s -count=1 \
  -cpuprofile "$OUT/combined_prepared_graph_only_serial_minimal_cpu.pprof" \
  -memprofile "$OUT/combined_prepared_graph_only_serial_minimal_mem.pprof"

GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphSearchTruthMatrix2037/mode=combined_prepared_typed_column/boundary=graph_only/concurrency=parallel/fixture=production8192/stats=minimal$' \
  -benchmem -benchtime=30s -count=1 \
  -cpuprofile "$OUT/combined_prepared_graph_only_parallel_minimal_cpu.pprof" \
  -memprofile "$OUT/combined_prepared_graph_only_parallel_minimal_mem.pprof"
```

Go benchmark CPU and allocation profiles include fixture setup/open/rebuild as
well as timed search. The profile artifacts therefore must not be cited as pure
hot-loop percentages unless filtered to search stacks. The long-benchtime serial
profile still showed setup/rebuild allocations and CPU; the parallel profile was
mostly timed search.

Search-stack CPU summaries from `go tool pprof -top -focus=SearchCosine -relative_percentages`:

| Profile | Timed benchmark row | Search-stack observations |
| --- | --- | --- |
| Serial minimal graph-only | 13.142 µs/op, 76,091 ops/sec, 0 B/op, 0 allocs/op | SIMD dot product remains the largest flat bucket (`dotProductFloat32NEON` 18.5% of filtered samples; `scoreOrdinal` 37.6% cumulative). Frontier/top-k is secondary but visible (`popFrontier` 5.1% cum, `frontierSiftDown` 4.4% cum, `insertTop` 0.6% flat). |
| Parallel minimal graph-only | 4.093 µs/op under profiling, 244,341 ops/sec, 0 B/op, 0 allocs/op | Timed search dominated the profile. Scoring remains largest (`scoreOrdinal` 41.4% cum, NEON dot 12.5% flat). Frontier/top-k is a real secondary bucket (`popFrontier` 7.9% cum, `frontierSiftDown` 6.5% cum, `frontierSiftUp` 2.6% flat, `insertTop` 1.3% cum). |

Allocation profiles are dominated by fixture build, JSON ingestion, typed-column
asset construction, and commitlog setup; the timed graph-only search rows report
`0 B/op` and `0 allocs/op` in both full-diagnostics and minimal-stats modes.
Result-ID materialization reports 784 B/op and 2 allocs/op; document
materialization reports about 92 KiB/op and 319 allocs/op for ten fetched
documents.

### #2043 closeout and follow-up decision

The combined prepared typed-column route is admitted as the primary healthy
current-format execution route from an architecture, readiness, and correctness
standpoint: all required typed-column graph-search roles are prepared/direct,
graph-only search is zero-allocation, and healthy current-format fallback
counters are zero. This is not a full wall-time promotion over the old legacy
graph-row direct control and should not close #2035 as fully performance
satisfied. The final truth matrix still shows net throughput gaps, and its
legacy-versus-current graph-only rows also expose the documented topology/search
work mismatch: legacy/direct visits 612 edges/search while current-format
prepared rows visit 3340 edges/search, about 5.5x more. Treat that as a finding
that follow-up benchmarks and instrumentation must explain, not as a blocker to
merging #2043 and not as storage-path proof.

Follow-ups:

- #1979 is the natural owner for explaining the 5.5x visited-edge mismatch and
  making HNSW search work/batchability visible before optimizing storage,
  frontier, or dot-kernel buckets. If exact topology-parity search benchmarks are
  still needed after #2043, propose them there or in a focused follow-up issue.
- #1980 remains an evidence-backed follow-up for frontier/top-k scratch
  optimization if future apples-to-apples profiles continue to justify it. This
  closeout does not implement runtime optimization.
- #1977 remains deferred. The current raw-vector plus inverse-norm path is
  admitted and zero-allocation; the final profiles do not prove that derived
  normalized vector payloads would offset their extra storage/rebuild cost or
  preserve scoring/tie behavior by default.

## #2091 topology-parity search benchmark

`BenchmarkColumnVectorGraphSearchTopologyParity2091` is the end-to-end
`SearchCosine` follow-up to the #2043 adjacency-access microbenchmark. It uses
one deterministic production fixture (`rows=8192`, `dims=128`, degree 16,
`topK=10`, `efSearch=128`, `query_ordinal=4096`) and publishes the same vectors,
document IDs, and synthetic ring adjacency payloads into two readers:

- `legacy_graph_row_direct`: a legacy compatibility physical graph-row asset plus
  vector-index state. This row matches the #2037 legacy/direct graph-only control
  topology: graph rows are present, vector/result-ID compatibility fallback is
  counted, and adjacency is served from the direct prepared-CSR state published
  beside the compatibility asset.
- `current_prepared_typed_column`: a current-format graph manifest with no
  physical graph-row asset. Base typed-column vectors, inverse norms, adjacency,
  row refs, and document IDs are prepared from the same rows; healthy searches
  must report `graph_rows=0`, `prepared_graph_search_views/search=1`, and zero
  graph-row/source fallbacks.

`TestColumnVectorGraphSearchTopologyParity2091` is the parity gate. It decodes
legacy graph-row adjacency and prepared current CSR adjacency for every fixture
ordinal, checks prepared vectors and document IDs against the source rows, runs
both graph-only and result-ID searches, and asserts equivalent results plus equal
search-work counters (`candidate_rows/search`, `candidates/search`,
`edges/search`, `visited_edges/search`, `visited_nodes/search`, score-batch
candidate counts, fetch counts, and adjacency prepared-CSR counters). After #2103,
these historical topology-parity rows explicitly pin scalar scoring so
the gate continues to isolate topology/search-work parity rather than the
prepared indexed-scoring default.

Run context:

- Code benchmarked: local #2091 worktree on top of `ccdc2e5fa5a8eed44687190c91c2b2b8477ee4ca`
  (latest `origin/main` after #2043; benchmark harness changes only).
- Command: `GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections -run '^$' -bench '^BenchmarkColumnVectorGraphSearchTopologyParity2091$' -benchmem -benchtime=500ms -count=5`.
- Hardware/context: macOS 26.2, Apple M3, 8 logical CPUs, 16 GiB memory,
  `go version go1.25.5 darwin/arm64`.
- Local artifact directory: `/tmp/gomap_2091_topology_parity_20260531_125522/`
  with `topology_parity_2091_bench.txt`.

Median rows from the five-count run:

| Boundary | Mode | ns/op | ops/sec | B/op | allocs/op | Work/parity counters | Source/fallback counters |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| `graph_only` | `legacy_graph_row_direct` | 8.006 µs | 124,908 | 0 | 0 | `candidates=128`, `visited_nodes=128`, `edges=612`, `visited_edges=612`, `adjacency_expansions=39` | `graph_rows=8192`, `prepared_graph_search_views=0`, `graph_row_fallbacks=128`, `vector_scratch_decodes=128`, `adjacency_prepared_csr_mmap_direct=40`, `adjacency_legacy_fallbacks=0` |
| `graph_only` | `current_prepared_typed_column` | 8.626 µs | 115,925 | 0 | 0 | `candidates=128`, `visited_nodes=128`, `edges=612`, `visited_edges=612`, `adjacency_expansions=39` | `graph_rows=0`, `prepared_graph_search_views=1`, `graph_row_fallbacks=0`, `prepared_score_calls=128`, `vector_prepared_direct=128`, `norm_prepared_direct=128`, `adjacency_prepared_csr_mmap_direct=40` |
| `result_id` | `legacy_graph_row_direct` | 8.208 µs | 121,830 | 0 | 0 | `candidates=128`, `visited_nodes=128`, `edges=612`, `visited_edges=612`, `result_fetches=10` | `graph_rows=8192`, `graph_row_fallbacks=138`, `result_id_graph_fallbacks=10`, `vector_scratch_decodes=128`, `adjacency_prepared_csr_mmap_direct=40` |
| `result_id` | `current_prepared_typed_column` | 9.548 µs | 104,738 | 0 | 0 | `candidates=128`, `visited_nodes=128`, `edges=612`, `visited_edges=612`, `result_fetches=10` | `graph_rows=0`, `prepared_graph_search_views=1`, `graph_row_fallbacks=0`, `result_id_prepared_bytes_views=1`, `result_id_typed_bytes_state=10`, `row_ref_state_result_refs=10` |

All four rows report `candidate_rows/search=8192`, `candidate_fetches/search=128`,
`score_batch_calls/search=128`, `score_batch_candidates/search=128`,
`adjacency_B/search=2560`, `vector_B/search=65536`, `norm_B/search=512`, and zero
`adjacency_source_fallbacks/search`. The current prepared rows additionally
report zero `typed_column_vector_fallbacks/search`, zero
`row_ref_vector_source_legacy_graph_ids/search`, zero
`row_ref_state_source_fallbacks/search`, and zero `result_id_graph_fallbacks/search`.

Interpretation for #2035: the topology mismatch is now removed for this fixture;
both paths visit exactly 612 edges/search and return equivalent results. Under
this full-diagnostics parity run, the prepared typed-column path is close but is
not yet throughput-superior: it is about 7.7% slower at the graph-only boundary
and about 16% slower at the result-ID boundary. This is a materially different
finding from the #2043 truth-matrix rows with 612 versus 3340 edges/search: the
large wall-time gap was mostly fixture topology/search work, while the remaining
smaller gap is inside the equal-work search loop and result-ID side channel. Do
not promote #2035 on this evidence alone. The next promotion work should use
issue #1979 to explain batchability, source-counter overhead, and
visited-edge/control flow under parity before assigning any remaining optimized
implementation to #1980 frontier/top-k or #1977 normalized-vector
storage/scoring.

## #1979 HNSW batchability/control-flow instrumentation

`BenchmarkColumnVectorGraphSearchBatchability1979` reuses the #2091 production
fixture and runs the current prepared typed-column graph-only path with
`StatsMode=benchmark_debug`. The benchmark is evidence/instrumentation only: it
does not change traversal semantics, fixture topology, default scoring mode, or
persistent formats. Rows report the normal `ns/op`, `ops/sec`, `B/op`, and
`allocs/op` columns plus stable control-flow counters for neighbor tile sizes,
score-batch histograms, scored-versus-skipped neighbors, visited-mark
hits/misses, frontier/top-k operations, layer work, and exact-mode candidate
order summaries.

Command used for the local #1979 evidence run:

```sh
OUT=/tmp/gomap_1979_batchability_final_20260531_140228
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphSearchBatchability1979$' \
  -benchmem -benchtime=500ms -count=3 | tee "$OUT/batchability_1979_bench.txt"
```

Run context: macOS 26.2, Apple M3, 8 logical CPUs, 16 GiB memory,
`go version go1.25.5 darwin/arm64`. Median rows from the three-count run:

| Search mode | Score mode | ns/op | ops/sec | B/op | allocs/op | Work counters | Batchability/control-flow counters |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| `ef_search=128` | `scalar` | 9.846 µs | 101,561 | 0 | 0 | `candidates=128`, `visited_nodes=128`, `visited_edges=612`, `edges_per_visited_node=4.781` | `neighbor_tiles=39`, `neighbor_tile_avg_size=16`, `score_batch_calls=128`, `score_batch_singletons=128`, `scored_neighbors=127`, `already_visited_skips=485`, `frontier_pushes=128`, `frontier_pops=39`, `top_k_insert_attempts=128`, `top_k_insert_successes=22`, `top_k_insert_rejections=106` |
| `ef_search=128` | `indexed` | 12.232 µs | 81,751 | 0 | 0 | `candidates=128`, `visited_nodes=128`, `visited_edges=612`, `edges_per_visited_node=4.781` | `neighbor_tiles=39`, `neighbor_tile_avg_size=16`, `score_batch_calls=25`, `score_batch_singletons=4`, `score_batch_size_2_4=8`, `score_batch_size_5_8=12`, `score_batch_size_9_16=1`, `already_visited_skips=485`, `frontier_pushes=128`, `top_k_rejections=106` |
| `exact` (`ef_search=8192`) | `scalar` | 1.091 ms | 916.7 | 0 | 0 | `candidates=8192`, `visited_nodes=8192`, `visited_edges=100748`, `edges_per_visited_node=12.30` | `neighbor_tiles=6297`, `neighbor_tile_avg_size=16`, `score_batch_calls=8192`, `score_batch_singletons=8192`, `scored_neighbors=8191`, `already_visited_skips=92557`, `frontier_pushes=8192`, `frontier_pops=6297`, `top_k_rejections=8132`, `exact_order_observations=8192`, `exact_order_backward_jumps=5977` |
| `exact` (`ef_search=8192`) | `indexed` | 1.277 ms | 782.9 | 0 | 0 | `candidates=8192`, `visited_nodes=8192`, `visited_edges=100748`, `edges_per_visited_node=12.30` | `neighbor_tiles=6297`, `neighbor_tile_avg_size=16`, `score_batch_calls=1797`, `score_batch_singletons=364`, `score_batch_size_2_4=502`, `score_batch_size_5_8=930`, `score_batch_size_9_16=1`, `already_visited_skips=92557`, `frontier_pushes=8192`, `top_k_rejections=8132`, `exact_order_observations=8192`, `exact_order_backward_jumps=5977` |

Interpretation of #1979 historical evidence:

- The equal-topology #2091 fixture's `ef_search=128` row explains its 612
  visited edges as layer-0 expansion work over 39 degree-16 neighbor tiles. The
  high skip bucket is already-visited neighbors (`485/search`), not metadata
  filtering (`filter_skips=0`) and not a storage-source fallback
  (`graph_row_fallbacks=0`, `adjacency_source_fallbacks=0`).
- The exact row is intentionally much more search work: it scores all 8192
  candidates and visits 100748 edges/search. That high visited-edge count is
  topology/frontier revisitation (`92557 already_visited_skips/search`) plus the
  expected exact traversal budget, not typed-column adjacency overhead.
- Natural neighbor tiles are full degree-16 tiles in this fixture
  (`neighbor_tile_avg_size=16`). The scalar control performs singleton scoring
  (`score_batch_singletons == score_batch_calls`), so batchability exists in the
  traversal. At the time of #1979 this was also the default; #2103 later
  promoted indexed/gathered scoring only for the eligible prepared path.
- The indexed diagnostic row demonstrates available score grouping without
  changing result order: exact-mode score calls drop from 8192 singleton calls
  to 1797 calls, mostly size 2-8 tiles. At #1979 time it was not a promotion
  claim because this row ran under debug instrumentation and the indexed scoring
  hook was not enabled by default; #2103 later runs the promotion matrix and
  changes only the eligible prepared default.
- Exact candidate order is not mostly sequential (`5977 backward jumps` versus
  `2199 adjacent-forward transitions` in the exact row), so a renewed exact-mode
  indexed-scoring ticket should expect graph-order/gathered batches rather than
  long contiguous row runs.

Historical #1979 follow-up recommendation: keep #2035 open as not performance-satisfied.
Use this #1979 evidence to prioritize #1980 frontier/top-k and already-visited
handling for control-flow overhead, and use a separate narrow indexed-scoring
ticket if exact-mode gathered score batching becomes a goal. #1977
normalized-vector storage remains a separate scoring/storage-format follow-up
and is not implemented here.

## #2098 exact-mode gathered scoring evaluation

The later #2098 work optimized the opt-in prepared typed-column indexed scoring path for the
common single-part base-vector view. On fallback-only batch backends, the
indexed path now avoids repeated ordinal-location passes and the extra
`DotFloat32Indexed` wrapper validation while preserving the same gathered score
batch counters. This does not change default scoring mode, traversal/frontier
semantics, `ef_search`, `topK`, fixture topology, result ordering, or persistent
formats.

Command used for the local before/after #2098 evidence run:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphSearchBatchability1979/(ef_search_128|exact)/score=(scalar|indexed)$' \
  -benchmem -benchtime=1s -count=3
```

Median rows on macOS/Apple M3, `go version go1.25.5 darwin/arm64`:

| Search mode | Score mode | Before #2098 ns/op | After #2098 ns/op | B/op | allocs/op | Counter summary |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `ef_search=128` | `scalar` | 9,275 | 9,279 | 0 | 0 | unchanged scalar singleton scoring: `score_batch_calls=128`, `score_batch_singletons=128`, `visited_edges=612` |
| `ef_search=128` | `indexed` | 11,775 | 7,033 | 0 | 0 | same work and fallback-free sources; `score_batch_calls=25`, `score_batch_fallback=25`, `score_batch_max_tile_size=16`, `visited_edges=612` |
| `exact` (`ef_search=8192`) | `scalar` | 1,086,851 | 1,148,337 | 0 | 0 | unchanged scalar singleton scoring: `score_batch_calls=8192`, `visited_edges=100748`, `exact_order_observations=8192` |
| `exact` (`ef_search=8192`) | `indexed` | 1,238,033 | 963,815 | 0 | 0 | same exact work and fallback-free sources; `score_batch_calls=1797`, `score_batch_fallback=1797`, `score_batch_max_tile_size=16`, `exact_order_backward_jumps=5977` |

Interpretation of #2098: exact-mode gathered/indexed scoring is now a measured win
for the prepared single-part benchmark hook and remains exactly equivalent to
scalar/default results in focused fixed-fixture tests, including tie-order
coverage. #2103 owns the follow-up promotion matrix and default-gating decision.
The #1981 wavefront traversal and #1977 normalized-vector storage remain separate
evidence-gated follow-ups.

## #2103 prepared indexed-scoring promotion matrix

The #2103 work promotes prepared indexed/gathered scoring as the **default only for the
eligible prepared typed-column path**. The runtime gate resolves
`ScoreBatchMode=default` to indexed only when native search has a healthy
combined prepared graph-search view with a single-part prepared vector view and
ready inverse norms. Explicit scalar and explicit indexed modes continue to be
honored. The legacy graph-row/direct compatibility path, non-prepared typed-column
source routes, and fallback paths resolve default to scalar, so this does not
force legacy/graph-row search into gathered scoring.

The default change is intentionally narrow. It does not change HNSW traversal,
frontier behavior, candidate order/tie behavior, `ef_search`, `topK`, fixture
topology, result ordering, public result IDs, or persistent formats. The #2091
topology-parity benchmark remains scalar-pinned as a search-work parity control;
The #2103 work adds `BenchmarkColumnVectorGraphSearchPromotion2103` for the
`default`/`scalar`/`indexed` comparison.

Commands used for local #2103 evidence on `0e114572f10e43377e7e07f055af1905ca5e7d16`
plus the #2103 branch changes:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphSearchTopologyParity2091' \
  -benchmem -benchtime=1s -count=3

GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphSearchBatchability1979/(ef_search_128|exact)/score=(scalar|indexed)$' \
  -benchmem -benchtime=1s -count=3

GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkColumnVectorGraphSearchPromotion2103$' \
  -benchmem -benchtime=1s -count=3
```

Run context: macOS 26.2, Apple M3, 8 logical CPUs, 16 GiB memory,
`go version go1.25.5 darwin/arm64`. Local artifacts are under
`/tmp/gomap_2103_evidence/`.

Median rows from the post-#2098/#2103 matrix:

| Benchmark row | ns/op | ops/sec | B/op | allocs/op | Work counters | Score-batch / order counters | Source/fallback counters |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| #2091 `graph_only` legacy scalar control | 8,572 | 116,653 | 0 | 0 | `visited_edges=612`, `candidate_fetches=128` | `score_batch_calls=128`, max tile `1` | `graph_rows=8192`, `graph_row_fallbacks=128`, `adjacency_source_fallbacks=0` |
| #2091 `graph_only` current prepared scalar control | 8,463 | 118,158 | 0 | 0 | `visited_edges=612`, `candidate_fetches=128` | `score_batch_calls=128`, max tile `1` | `graph_rows=0`, `prepared_graph_search_views=1`, all graph-row/source fallbacks `0` |
| #2091 `result_id` legacy scalar control | 8,581 | 116,530 | 0 | 0 | `visited_edges=612`, `result_fetches=10` | `score_batch_calls=128`, max tile `1` | `graph_rows=8192`, `result_id_graph_fallbacks=10` |
| #2091 `result_id` current prepared scalar control | 9,369 | 106,737 | 0 | 0 | `visited_edges=612`, `result_fetches=10` | `score_batch_calls=128`, max tile `1` | `graph_rows=0`, `result_id_graph_fallbacks=0`, row-ref/doc-ID prepared counters healthy |
| #1979 `ef_search=128` current prepared scalar | 9,412 | 106,247 | 0 | 0 | `visited_edges=612`, `already_visited_skips=485` | `score_batch_singletons=128`, exact-order counters `0` | prepared/source fallbacks `0` |
| #1979 `ef_search=128` current prepared indexed | 7,183 | 139,223 | 0 | 0 | `visited_edges=612`, `already_visited_skips=485` | `score_batch_calls=25`, singleton/2-4/5-8/9-16 = `4/8/12/1`, max tile `16`, exact-order counters `0` | prepared/source fallbacks `0` |
| #1979 exact current prepared scalar | 1,076,556 | 929 | 0 | 0 | `visited_edges=100748`, `already_visited_skips=92557` | `score_batch_singletons=8192`, `exact_order_observations=8192`, `backward_jumps=5977` | prepared/source fallbacks `0` |
| #1979 exact current prepared indexed | 931,805 | 1,073 | 0 | 0 | `visited_edges=100748`, `already_visited_skips=92557` | `score_batch_calls=1797`, singleton/2-4/5-8/9-16 = `364/502/930/1`, max tile `16`, `exact_order_observations=8192`, `backward_jumps=5977` | prepared/source fallbacks `0` |
| #2103 `graph_only` legacy default | 9,468 | 105,618 | 0 | 0 | `visited_edges=612`, `candidate_fetches=128` | scalar: `score_batch_singletons=128`, max tile `1` | `graph_rows=8192`, `graph_row_fallbacks=128`, `adjacency_source_fallbacks=0` |
| #2103 `graph_only` current prepared default | 7,242 | 138,079 | 0 | 0 | `visited_edges=612`, `candidate_fetches=128` | indexed: `score_batch_calls=25`, singleton/2-4/5-8/9-16 = `4/8/12/1`, max tile `16` | `graph_rows=0`, `prepared_graph_search_views=1`, all source/result fallbacks `0` |
| #2103 `graph_only` current prepared scalar | 9,346 | 107,002 | 0 | 0 | `visited_edges=612`, `candidate_fetches=128` | scalar: `score_batch_singletons=128`, max tile `1` | `graph_rows=0`, source fallbacks `0` |
| #2103 `graph_only` current prepared indexed | 6,857 | 145,839 | 0 | 0 | `visited_edges=612`, `candidate_fetches=128` | indexed: `score_batch_calls=25`, singleton/2-4/5-8/9-16 = `4/8/12/1`, max tile `16` | `graph_rows=0`, source fallbacks `0` |
| #2103 `result_id` legacy default | 9,537 | 104,851 | 0 | 0 | `visited_edges=612`, `result_fetches=10` | scalar: `score_batch_singletons=128`, max tile `1` | `graph_rows=8192`, `result_id_graph_fallbacks=10` |
| #2103 `result_id` current prepared default | 7,786 | 128,432 | 0 | 0 | `visited_edges=612`, `result_fetches=10` | indexed: `score_batch_calls=25`, singleton/2-4/5-8/9-16 = `4/8/12/1`, max tile `16` | `graph_rows=0`, `result_id_graph_fallbacks=0`, row-ref/doc-ID prepared counters healthy |
| #2103 `result_id` current prepared scalar | 10,259 | 97,471 | 0 | 0 | `visited_edges=612`, `result_fetches=10` | scalar: `score_batch_singletons=128`, max tile `1` | `graph_rows=0`, source/result fallbacks `0` |
| #2103 `result_id` current prepared indexed | 7,768 | 128,732 | 0 | 0 | `visited_edges=612`, `result_fetches=10` | indexed: `score_batch_calls=25`, singleton/2-4/5-8/9-16 = `4/8/12/1`, max tile `16` | `graph_rows=0`, source/result fallbacks `0` |

Decision for #2103 (promotion matrix):

- Enable the indexed/gathered scorer as the default for eligible prepared
  typed-column search only. In the bounded promotion rows, the prepared default
  now tracks the indexed row and is materially faster than the scalar control at
  both graph-only and result-ID boundaries while keeping `visited_edges=612`,
  `topK=10`, `ef_search=128`, result order, and zero allocations fixed.
- Exact-mode evidence remains favorable for the same prepared path: indexed
  scoring preserves exact candidate-order counters and reduces score calls from
  `8192` to `1797` without source fallbacks.
- Legacy/graph-row default remains scalar and keeps its existing fallback/source
  counters. Explicit scalar remains available and is used by #2091 parity tests.
- #2035 should remain open as the broader promotion tracker until public-search
  and larger/real-workload evidence catches up, but its prepared scoring blocker
  is resolved by #2103. #1981 wavefront traversal and #1977 normalized-vector
  storage remain deferred/evidence-gated and are not prerequisites for this
  narrow default.

## #2105 public VectorIndexSearcher promotion matrix

`BenchmarkVectorIndexSearcherSearchPromotion2105` is the public-search follow-up
to the #2103 internal `SearchCosine` promotion matrix. It uses the deterministic #2091
production topology-parity fixture (`rows=8192`, `dims=128`, degree 16,
`topK=10`, bounded `efSearch=128`, exact `efSearch=8192`,
`query_ordinal=4096`) and times an already-opened public
`VectorIndexSearcher.Search` handle. Setup/open/rebuild are outside the timed
search loop; `OpenVectorIndexSearcher` setup remains covered by the #2037
`setup_open_prepare` boundary when that boundary is the measurement target.

Command:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkVectorIndexSearcherSearchPromotion2105$' \
  -benchmem -benchtime=500ms -count=5
```

Smoke validation may use `-benchtime=1x -count=1`. Published comparisons should
use one hardware/Go/GOMAXPROCS context and should keep the #2091 shape, query,
`topK`, `efSearch`, and materialization boundary fixed within each table.

Permanent rows:

| Search | Boundary | Mode | Score mode | Fixture/materialization contract |
| --- | --- | --- | --- | --- |
| `ef_search_128` | `result_id` | `legacy_graph_row_direct` | `default` | Public result-ID response from the legacy/direct compatibility fixture; default must remain scalar. |
| `ef_search_128` | `result_id` | `current_prepared_typed_column` | `default` | Public result-ID response from the prepared current-format fixture; default should select indexed/gathered scoring when eligible. |
| `ef_search_128` | `result_id` | `current_prepared_typed_column` | `scalar` | Same public result-ID boundary and fixture, explicit scalar control. |
| `ef_search_128` | `result_id` | `current_prepared_typed_column` | `indexed` | Same public result-ID boundary and fixture, explicit indexed/gathered control. |
| `ef_search_128` | `document_materialization` | `current_prepared_typed_column` | `default` | Public response plus post-top-k document fetch/materialization; legacy is omitted because the physical-asset control fixture does not publish comparable base documents. |
| `ef_search_128` | `document_materialization` | `current_prepared_typed_column` | `scalar` | Same document boundary, explicit scalar control. |
| `ef_search_128` | `document_materialization` | `current_prepared_typed_column` | `indexed` | Same document boundary, explicit indexed/gathered control. |
| `exact` | `result_id` | `legacy_graph_row_direct` | `default` | Public result-ID exact-mode control; default must remain scalar. |
| `exact` | `result_id` | `current_prepared_typed_column` | `default` | Public result-ID exact-mode prepared default; default should select indexed/gathered scoring when eligible. |
| `exact` | `result_id` | `current_prepared_typed_column` | `scalar` | Same exact public result-ID boundary, explicit scalar control. |
| `exact` | `result_id` | `current_prepared_typed_column` | `indexed` | Same exact public result-ID boundary, explicit indexed/gathered control. |

All #2105 rows run with `StatsMode=benchmark_debug` and emit the public metric
labels from `reportVectorIndexSearchBenchMetricsV4`, including the #1979
control-flow counters now exposed for public searcher benchmarks:
`benchmark_debug_searches/search`, `neighbor_tile_avg_size`,
`score_batch_singletons/search`, `score_batch_size_2_4/search`,
`score_batch_size_5_8/search`, `score_batch_size_9_16/search`,
`score_batch_size_17_plus/search`, `already_visited_skips/search`,
`frontier_pushes/search`, `frontier_pops/search`,
`top_k_insert_rejections/search`, `visited_mark_hits/search`,
`visited_mark_misses/search`, `exact_candidate_order_observations/search`, and
`exact_candidate_order_backward_jumps/search`. The regular public source and
fallback labels still apply: healthy prepared rows must report
`graph_rows=0`, `prepared_graph_search_views/search=1`,
`graph_row_fallbacks/search=0`, `typed_column_vector_fallbacks/search=0`,
`adjacency_source_fallbacks/search=0`, `result_id_graph_fallbacks/search=0`, and
`row_ref_state_source_fallbacks/search=0`.

Local #2105 evidence run:

```sh
OUT=/tmp/gomap_2105_public_matrix_rebased_20260531_190806
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkVectorIndexSearcherSearchPromotion2105$' \
  -benchmem -benchtime=1s -count=3 | tee "$OUT/public_promotion_2105_bench.txt"
benchstat "$OUT/public_promotion_2105_bench.txt" > "$OUT/public_promotion_2105_benchstat.txt"
```

Run context: macOS 26.2, Apple M3, 8 logical CPUs, 16 GiB memory,
`go version go1.25.5 darwin/arm64`. The tables below record medians from the
three-count run. Raw artifacts contain every emitted counter; all rows below use
`rows=8192`, `dims=128`, degree 16, `topK=10`, `query_ordinal=4096`, and
`StatsMode=benchmark_debug`.

Bounded public result-ID rows (`efSearch=128`, setup/open/rebuild excluded):

| Mode | Score | ns/op | ops/sec | B/op | allocs/op | Work counters | Score/debug counters | Source/fallback counters |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| `legacy_graph_row_direct` | `default` | 10.149 µs | 98,528 | 816 | 2 | `candidate_rows=8192`, `candidate_fetches=128`, `result_fetches=10`, `visited_edges=612`, `visited_mark_hits/misses=485/127` | scalar: `score_batch_calls=128`, max tile `1`, hist singleton `128`, exact obs/back `0/0` | `graph_rows=8192`, `graph_row_fallbacks=138`, `vector_scratch_decodes=128`, `typed_column_vector_fallbacks=1`, `result_id_graph_fallbacks=10`, `adjacency_source_fallbacks=0` |
| `current_prepared_typed_column` | `default` | 8.870 µs | 112,734 | 816 | 2 | same fixed work: `candidate_fetches=128`, `result_fetches=10`, `visited_edges=612`, `visited_mark_hits/misses=485/127` | indexed/gathered: `score_batch_calls=25`, candidates `128`, max tile `16`, hist singleton/2-4/5-8/9-16 = `4/8/12/1`, exact obs/back `0/0` | `graph_rows=0`, `prepared_graph_search_views=1`, graph/vector/adjacency/result fallbacks `0`, vector/norm scratch decodes `0` |
| `current_prepared_typed_column` | `scalar` | 11.440 µs | 87,414 | 816 | 2 | same fixed work | scalar: `score_batch_calls=128`, max tile `1`, hist singleton `128`, exact obs/back `0/0` | prepared path, all source/result fallbacks `0` |
| `current_prepared_typed_column` | `indexed` | 8.473 µs | 118,027 | 816 | 2 | same fixed work | indexed/gathered: `score_batch_calls=25`, candidates `128`, max tile `16`, hist `4/8/12/1`, exact obs/back `0/0` | prepared path, all source/result fallbacks `0` |

Bounded public document-materialization rows (`efSearch=128`, current prepared
fixture only because the legacy physical-asset control does not publish
comparable base documents):

| Mode | Score | ns/op | ops/sec | B/op | allocs/op | Work/doc counters | Score/source counters |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| `current_prepared_typed_column` | `default` | 106.451 µs | 9,394 | 100,308 | 321 | `result_fetches=10`, `docs_fetched=10`, `doc_fetch_ns=108,042`, `visited_edges=612` | default indexed: `score_batch_calls=25`, hist `4/8/12/1`; prepared/source fallbacks `0` |
| `current_prepared_typed_column` | `scalar` | 109.582 µs | 9,126 | 100,308 | 321 | `result_fetches=10`, `docs_fetched=10`, `doc_fetch_ns=108,667`, `visited_edges=612` | scalar: `score_batch_calls=128`, singleton hist `128`; prepared/source fallbacks `0` |
| `current_prepared_typed_column` | `indexed` | 109.724 µs | 9,114 | 100,308 | 321 | `result_fetches=10`, `docs_fetched=10`, `doc_fetch_ns=109,500`, `visited_edges=612` | indexed: `score_batch_calls=25`, hist `4/8/12/1`; prepared/source fallbacks `0` |

Exact public result-ID rows (`efSearch=8192`, setup/open/rebuild excluded):

| Mode | Score | ns/op | ops/sec | B/op | allocs/op | Exact/work counters | Score/debug counters | Source/fallback counters |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| `legacy_graph_row_direct` | `default` | 1.053 ms | 949.3 | 816 | 2 | `candidate_fetches=8192`, `result_fetches=10`, `visited_edges=100748`, `visited_mark_hits/misses=92557/8191`, exact obs/back `8192/5977` | scalar: `score_batch_calls=8192`, singleton hist `8192`, max tile `1` | `graph_rows=8192`, `graph_row_fallbacks=8202`, `vector_scratch_decodes=8192`, `typed_column_vector_fallbacks=1`, `result_id_graph_fallbacks=10`, `adjacency_source_fallbacks=0` |
| `current_prepared_typed_column` | `default` | 919.201 µs | 1,088 | 816 | 2 | same exact work: `candidate_fetches=8192`, `result_fetches=10`, `visited_edges=100748`, exact obs/back `8192/5977` | indexed/gathered: `score_batch_calls=1797`, candidates `8192`, max tile `16`, hist singleton/2-4/5-8/9-16 = `364/502/930/1` | `graph_rows=0`, `prepared_graph_search_views=1`, graph/vector/adjacency/result fallbacks `0`, vector/norm scratch decodes `0` |
| `current_prepared_typed_column` | `scalar` | 1.084 ms | 922.9 | 816 | 2 | same exact work | scalar: `score_batch_calls=8192`, singleton hist `8192`, exact obs/back `8192/5977` | prepared path, all source/result fallbacks `0` |
| `current_prepared_typed_column` | `indexed` | 929.601 µs | 1,076 | 816 | 2 | same exact work | indexed/gathered: `score_batch_calls=1797`, hist `364/502/930/1`, exact obs/back `8192/5977` | prepared path, all source/result fallbacks `0` |

Decision for #2105: recommend closing #2035 as performance-satisfied. On the public
larger topology-parity matrix, prepared `default` is faster than the valid
legacy/direct public result-ID controls in bounded and exact rows, tracks the
explicit indexed/gathered counter shape, remains equivalent to scalar/indexed
results in the focused public test, and keeps healthy prepared fallback counters
at zero. The document-materialization boundary is dominated by post-top-k
fetch/reconstruction work; prepared `default` remains competitive while preserving
the same search work and document counters. No narrow performance follow-up is
needed from this matrix. #1981 wavefront traversal and #1977 normalized-vector
storage remain separate deferred experiments, not blockers to #2035 closeout.

`TestColumnVectorGraphPublicVectorIndexSearcherSearchPromotion2105` is the
focused correctness/admission gate. It uses the exact #2091 test fixture to prove
that public prepared `default`, `scalar`, and `indexed` rows return equivalent
result IDs/order/scores at both public result-ID and document-materialization
boundaries. It also asserts that prepared `default` follows indexed score-batch
counters, explicit scalar uses singleton batches, healthy prepared
fallback/source counters remain zero, and legacy/direct `default` remains scalar
at the public result-ID boundary.

This #2105 harness and evidence are docs/tests/benchmarks only. They must not
change traversal semantics, frontier behavior, candidate/tie/result ordering,
`efSearch`, `topK`, fixture topology, public response ownership, or persistent
formats.

## Interpretation rules

- `combined_prepared_typed_column` rows are #2045 prepared-routing evidence;
  they are comparable only to rows with identical boundary/concurrency/fixture
  labels.
- Compare only rows with identical `boundary`, `concurrency`, and `fixture`
  labels unless the report explicitly explains why a boundary change is the
  measurement target.
- `graph_only` rows intentionally omit final ID/row-ref materialization; use
  `result_id` rows when comparing public no-document result production.
- Document materialization is post-top-k side-channel work. It must be reported
  with `docs_fetched/search`, allocation counters, and document timing counters,
  not blended into graph-only claims.
- A PR claiming a prepared typed-column speedup must show which profile bucket
  moved: scoring/dot kernel, vector/norm lookup, adjacency retrieval,
  frontier/top-k, stats/telemetry, result ID, document materialization, or
  open/prepare.
