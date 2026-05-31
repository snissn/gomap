# Typed-Column Graph-Search Benchmark Truth Matrix (#2037)

Status: pre-alpha benchmark contract. This document owns the stable labels and
reporting boundaries for comparing legacy/direct graph-row search, current
TVIS/base typed-column search, and future prepared typed-column graph search.
Prepared-view implementations remain owned by #2038/#2040/#2041 and routing by
#2045; this matrix must not be treated as prepared-path performance evidence
until those rows stop being skipped placeholders.

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
  typed-column state and base typed-column vectors.
- `prepared_typed_column_placeholder`: future prepared-view path. These rows are
  skipped with a message naming #2038/#2040/#2041/#2045 and are not performance
  data.

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
| `prepared_typed_column_placeholder` | `setup_open_prepare` | serial | `serving1024` | Skipped placeholder; not performance data. |
| `legacy_direct_graph_row` | `graph_only` | serial/parallel | `production8192` | Supported control rows where explicit graph rows are published by the fixture. |
| `current_tvis_base_typed_column` | `graph_only` | serial/parallel | `production8192` | Supported current rows. |
| `prepared_typed_column_placeholder` | `graph_only` | serial/parallel | `production8192` | Skipped placeholders; not performance data. |
| `current_tvis_base_typed_column` | `result_id` | serial/parallel | `serving1024` | Supported current rows. |
| `prepared_typed_column_placeholder` | `result_id` | serial/parallel | `serving1024` | Skipped placeholders; not performance data. |
| `current_tvis_base_typed_column` | `document_materialization` | serial/parallel | `serving1024` | Supported current rows. |
| `prepared_typed_column_placeholder` | `document_materialization` | serial/parallel | `serving1024` | Skipped placeholders; not performance data. |

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
  `typed_column_vector_fallbacks/search`, `norm_mmap_direct/search`,
  `norm_heap_copy_typed_view/search`, `norm_scratch_decodes/search`,
  `norm_source_fallbacks/search`, `adjacency_prepared_csr_mmap_direct/search`,
  `adjacency_prepared_csr_direct_views/search`,
  `adjacency_typed_list_mmap_direct/search`,
  `adjacency_typed_list_heap_copy_typed_view/search`,
  `adjacency_typed_list_scratch_decodes/search`,
  `adjacency_legacy_fallbacks/search`, and
  `adjacency_source_fallbacks/search`;
- result/document side channels: `result_id_typed_bytes_state/search`,
  `result_id_graph_fallbacks/search`, `row_ref_vector_source_state/search`,
  `row_ref_vector_source_legacy_graph_ids/search`,
  `row_ref_state_result_refs/search`, `row_ref_state_source_fallbacks/search`,
  `docs_fetched/search`, `doc_fetch_ns/search`,
  `doc_row_ref_state_fetches/search`, and
  `doc_row_ref_lookup_fallbacks/search`.

Healthy current-format evidence must keep graph-row/fallback counters at zero
where those counters apply: `graph_rows=0` for current typed-column graph-only
rows, `typed_column_vector_fallbacks/search=0`,
`row_ref_vector_source_legacy_graph_ids/search=0`,
`result_id_graph_fallbacks/search=0`, `adjacency_legacy_fallbacks/search=0`, and
`adjacency_source_fallbacks/search=0`.

## Interpretation rules

- A skipped `prepared_typed_column_placeholder` row is a contract placeholder,
  not a win, loss, or throughput number.
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
