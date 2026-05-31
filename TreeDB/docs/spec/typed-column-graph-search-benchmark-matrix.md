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
| `legacy graph-row` | `graph_only` | `serial` | `production8192` | 8.129 µs | 123,015 | 0 | 0 | compatibility control: `graph_rows=8192`, `graph_row_fallbacks=128` |
| `legacy graph-row` | `graph_only` | `parallel` | `production8192` | 1.967 µs | 508,284 | 0 | 0 | compatibility control: `graph_rows=8192`, `graph_row_fallbacks=128` |
| `current TVIS/base typed-column` | `graph_only` | `serial` | `production8192` | 15.069 µs | 66,362 | 0 | 0 | `graph_rows=0`, `prepared=1`, `graph_row_fallbacks=0`, `vec/norm=182/182`, `adj=108`, `row_ref=1`, `id_view=1` |
| `current TVIS/base typed-column` | `graph_only` | `parallel` | `production8192` | 3.438 µs | 290,868 | 0 | 0 | `graph_rows=0`, `prepared=1`, `graph_row_fallbacks=0`, `vec/norm=182/182`, `adj=108`, `row_ref=1`, `id_view=1` |
| `combined prepared` | `graph_only` | `serial` | `production8192` | 14.420 µs | 69,348 | 0 | 0 | `graph_rows=0`, `prepared=1`, `graph_row_fallbacks=0`, `vec/norm=182/182`, `adj=108`, `row_ref=1`, `id_view=1` |
| `combined prepared` | `graph_only` | `serial` | `production8192/minimal` | 12.266 µs | 81,527 | 0 | 0 | `graph_rows=0`, `prepared=1`, `graph_row_fallbacks=0`, `vec/norm=182/182`, `adj=108`, `row_ref=1`, `id_view=1` |
| `combined prepared` | `graph_only` | `parallel` | `production8192` | 3.384 µs | 295,482 | 0 | 0 | `graph_rows=0`, `prepared=1`, `graph_row_fallbacks=0`, `vec/norm=182/182`, `adj=108`, `row_ref=1`, `id_view=1` |
| `combined prepared` | `graph_only` | `parallel` | `production8192/minimal` | 3.229 µs | 309,716 | 0 | 0 | `graph_rows=0`, `prepared=1`, `graph_row_fallbacks=0`, `vec/norm=182/182`, `adj=108`, `row_ref=1`, `id_view=1` |
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
`adjacency_source_fallbacks/search`, `row_ref_vector_source_legacy_graph_ids`,
`row_ref_state_source_fallbacks/search`, and `result_id_graph_fallbacks/search`.
After #2045 the `current_tvis_base_typed_column` label is no longer an
unprepared hot-loop source route in healthy current-format readers; it is the
continuity label proving that current TVIS/base typed-column search now selects
one combined prepared view. Use the historical #2035/#2040/#2042 artifacts, not
this final same-codebase row, when comparing against the pre-#2045 unprepared
source implementation.

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

### #2043 promotion and deferral decision

The combined prepared typed-column route is admitted as the primary healthy
current-format execution route: all required typed-column graph-search roles are
prepared/direct, graph-only search is zero-allocation, and healthy current-format
fallback counters are zero. This is not a full wall-time promotion over the old
legacy graph-row direct control. The minimal-stats combined prepared graph-only
row is still slower than the legacy control in this run (serial +50.9% ns/op;
parallel +64.2% ns/op), and setup/open remains slower than the legacy setup row.
Close #2035 as fully promoted only if the coordinator explicitly accepts that
architectural/current-format trade-off; otherwise keep the remaining wall-time
gap tracked separately.

Conditional follow-ups:

- #1980 remains an evidence-backed follow-up, not a no-op. Frontier/top-k work is
  not the largest bucket, but the final parallel profile still shows it as a
  real secondary hotspot. This closeout does not implement it.
- #1977 remains deferred. The current raw-vector plus inverse-norm path is
  admitted and zero-allocation; the final profiles do not prove that derived
  normalized vector payloads would offset their extra storage/rebuild cost or
  preserve scoring/tie behavior by default.

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
