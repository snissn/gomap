# Graph-Search Typed-Column Optimized-State Admission Gate (#2044)

Status: pre-alpha admission and readiness policy for TreeDB `column_graph`
typed-column graph-search state. This document consumes the #2047 optimized
consumer tier vocabulary in `typed-column-optimized-consumer-capabilities.md`
and the #2036 prepared runtime-view contract in
`typed-column-graph-search-prepared-views.md`.

This document owns the repo-level readiness table that descendants must update
before adding, changing, or promoting graph-search typed-column state roles. The
reusable #2046 direct-view certifier substrate lives in
`TreeDB/internal/typeddecode`. The #2037 benchmark truth-matrix labels and
command contract live in `typed-column-graph-search-benchmark-matrix.md`. This
table does not implement type-specific prepared views (#2038/#2040/#2041),
graph-search routing (#2045), hot-loop telemetry reduction (#2042), or benchmark
truth-matrix collection (#2037).

## Admission status vocabulary

| Status | Meaning for graph-search state |
| --- | --- |
| `pending` | Known current-format role whose required optimized state is specified but not yet fully implemented, certified, benchmarked, and promoted. Pending rows must still name the required #2047 tier, prepared shape, hot-loop boundary, fallback rule, counters/tests, and benchmark evidence fields. |
| `admitted` | Prepared-view-ready for healthy current-format graph search. Promotion to this status requires certifiers/prepared views, fail-closed tests, zero healthy fallback counters, and #2037 benchmark evidence with no unaccepted material regression. |
| `deferred` | Recognized state or durable asset role that is not a healthy current-format dependency. Healthy search must fail closed or ignore it unless a later PR promotes it through this table. |
| `experimental` | Non-default experiment only. It must be gated, counted, and excluded from healthy current-format evidence until promoted. |
| `fallback-only` | Legacy or compatibility path only. It must remain explicitly counted and must not be silently used by healthy current-format search. |

Only `pending` and `admitted` rows may describe required current-format roles.
`pending` is not performance-promotion evidence; it is a blocker list for the
implementation tickets that must promote the row to `admitted`. Rows marked
`deferred`, `experimental`, or `fallback-only` are unadmitted for healthy search
and MUST fail closed, stay non-default, or remain compatibility-only.

## Admission rules

- Every healthy current-format graph-search role must have exactly one row in the
  table below before the role can be added to or changed in vector-index state,
  base typed-column vector state, or search routing.
- Current hot/state roles require #2047 `mmap_direct`. A weaker tier such as
  `heap_typed_view` may be admitted only by changing this table and presenting
  memory, allocation, and wall-time evidence that the weaker tier has no
  unaccepted material regression.
- A row is incomplete if it does not name the owner or manifest role, canonical
  logical type and physical encoding, prepared runtime shape, hot-loop boundary,
  fail-closed or compatibility fallback rule, direct/fallback counters, tests,
  and benchmark/admission evidence fields.
- Graph-row payloads, legacy graph-specific adjacency sources, and dense
  adjacency row-image compatibility paths are not healthy current-format state.
  They may remain only as explicit compatibility/fallback rows with counters.
- Healthy evidence must show no silent graph-row fallback: `graph_rows=0`,
  `adjacency_legacy_fallbacks/search=0`, `adjacency_source_fallbacks/search=0`,
  `typed_column_vector_fallbacks/search=0`,
  `row_ref_vector_source_legacy_graph_ids=0`, and
  `result_id_graph_fallbacks=0` where those counters apply.

## Current graph-search optimized-state readiness table

| Graph-search role | State key / owner | Canonical format | #2047 tier | Admission status | Prepared runtime shape | Hot-loop boundary | Fallback/fail-closed rule | Counters and tests | Benchmark/admission evidence |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Base vectors | Base `typed_column_part` vector field for the indexed collection field; ordinal mapping may consume `row_refs` when graph ordinals differ from base rows. | `float32_vector` / `raw_float32_vector` | `mmap_direct` | `pending` for #2040 | Row-major `[]float32` plus `dims`, immutable asset identity, and optional ordinal-to-row mapping. | Candidate scoring only. Query validation and finite-value proof happen outside traversal. | Current-format search must fail closed or require rebuild if direct prepared vector state is absent, stale, corrupt, or below `mmap_direct`; it MUST NOT fetch candidate vectors from legacy graph rows or a generic adapter fallback. | Direct and fallback counters include `vector_mmap_direct/search`, `vector_heap_copy_typed_view/search`, `vector_scratch_decode/search`, `vector_scratch_decodes/search`, `typed_column_vector_fallbacks/search`, `vector_certification_failures/search`, and `graph_rows=0`. Tests must cover reopen, dimension mismatch, row-count mismatch, wrong endian, length, alignment, stale handle, finite-value proof or rejection, and zero healthy fallback. | #2037 must report `BenchmarkColumnVectorGraphNativeSearchCosineV3`, `BenchmarkColumnVectorGraphNativeSearchCosineParallelV3`, setup/open rows, `ns/op`, `ops/sec`, `B/op`, `allocs/op`, candidates/search, score counters, and direct/fallback counters before #2040 may promote this row to `admitted`. |
| HNSW adjacency | Vector-index state role `adjacency` with one asset per HNSW layer. | `uint32_list` / `raw_uint32_offsets_list` | `mmap_direct` | `pending` for #2038 | Per-layer CSR view with `Offsets []uint64` and `Values []uint32` bound to one searcher manifest identity. | Edge traversal and neighbor expansion only. Each expanded ordinal reads one layer slice directly. | Current-format search must fail closed or require rebuild if typed-list state is missing, corrupt, stale, or below `mmap_direct`; it MUST NOT read legacy graph-row adjacency, legacy `adjacency_list`, or graph-specific adjacency-source payloads on the healthy path. | Direct and fallback counters include `adjacency_typed_list_mmap_direct/search`, `adjacency_typed_list_heap_copy_typed_view/search`, `adjacency_typed_list_scratch_decodes/search`, `adjacency_legacy_fallbacks/search`, `adjacency_source_fallbacks/search`, `adjacency_validation_failures/search`, and `adjacency_stale_handles/search`. Tests must cover reopen, missing layer, layer gap, stale TVIS identity, offset and value corruption, neighbor out-of-bounds, all-layer direct views, and zero legacy fallback. | #2037 must report serial and parallel graph-only search, edges/search, visited_edges/search, candidates/search, `ns/op`, `ops/sec`, `B/op`, `allocs/op`, setup/open cost, and direct/fallback counters before #2038 may promote this row to `admitted`. |
| Inverse norms | Vector-index state role `inverse_norm` for cosine scoring. | `float32` / `raw_float32` | `mmap_direct` | `pending` for #2040 | Direct `[]float32` indexed by graph ordinal with immutable manifest identity and metric metadata. | Candidate scoring for cosine only; norm lookup is adjacent to vector scoring and must not decode per candidate. | Current-format cosine search must fail closed or require rebuild if norm state is missing, stale, corrupt, non-finite, or below `mmap_direct`; it MUST NOT read inverse norms from legacy graph rows or scratch decode in healthy search. | Direct and fallback counters include `norm_mmap_direct/search`, `norm_heap_copy_typed_view/search`, `norm_scratch_decode/search`, `norm_scratch_decodes/search`, `norm_source_fallbacks/search`, `norm_validation_failures/search`, and `norm_stale_handles/search`. Tests must cover reopen, row-count mismatch, length mismatch, alignment failure, finite-value proof or rejection, metric gating, and zero healthy fallback. | #2037 must keep norm counters inside the search/scoring boundary and report serial and parallel graph-only search, `ns/op`, `ops/sec`, `B/op`, `allocs/op`, and direct/fallback counters before #2040 may promote this row to `admitted`. |
| Row refs | Vector-index state role `row_refs` carrying `DocumentRowRef` coordinate assets. | `int64` / `raw_int64` | `mmap_direct` | `admitted` by #2041 | Direct row-ref arrays that are assembled into row-ref records only at result or document-fetch boundaries. | Result and document-fetch side channel only, except a prepared ordinal-to-base-row map may be consulted during scoring after open-time preparation. | Current-format search must fail closed or disable result/document materialization until rebuild when row-ref state is missing, stale, corrupt, or below `mmap_direct`; it MUST NOT scan legacy graph row IDs to map ordinals or fetch documents in healthy evidence. | Benchmark labels include `row_ref_state_prepared_views/search`, `row_ref_state_mmap_direct_fields/search`, `row_ref_state_result_refs/search`, `row_ref_state_source_fallbacks/search`, `doc_row_ref_state_fetches/search`, `doc_row_ref_lookup_fallbacks/search`, and `doc_row_ref_validation_failures/search`; search stats also expose `row_ref_vector_source_state` and `row_ref_vector_source_legacy_graph_ids`. Tests cover reopen, duplicate asset role, missing asset role, coordinate bounds, stale manifest identity, final top-k fetch, no legacy ID scan, corrupt field rejection, and fail-closed validation. | #2041 evidence uses the #2037 matrix to report row-ref counters separately from traversal/scoring, includes result/doc-fetch timing boundaries, `ns/op`, `ops/sec`, `B/op`, `allocs/op`, and direct/fallback counters. |
| Document IDs | Vector-index state role `document_ids` with one opaque bytes value per graph ordinal. | `bytes` / `raw_bytes_offsets` | `mmap_direct` | `admitted` by #2041 | Direct `Offsets []uint64` plus `Bytes []byte`; public result IDs are copied or buffer-owned only at the API boundary. | Final top-k result-ID materialization only. Traversal and scoring must not touch document ID bytes. | Current-format search must fail closed or require rebuild if document-ID state is missing, stale, corrupt, or below `mmap_direct`; it MUST NOT return IDs from legacy graph rows in healthy evidence. | Benchmark labels include `result_id_prepared_bytes_views/search`, `result_id_typed_bytes_state/search`, `result_id_graph_fallbacks/search`, `result_id_state_validation_failures/search`, and `docs_fetched/search`, plus result-ID byte/output counters. Tests cover reopen, arbitrary binary IDs, offset/value corruption, missing state fail-closed, final top-k-only access, and zero graph fallback. | #2041 evidence uses the #2037 matrix split between no-document search, result-ID, and with-documents rows, and reports `BenchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsV4`, `docs_fetched/search`, `ns/op`, `ops/sec`, `B/op`, `allocs/op`, and direct/fallback counters. |
| Optional normalized vectors | Vector-index state role `normalized_vectors`; optional derived state from #1977 and not a required current healthy dependency. | `float32_vector` / `raw_float32_vector` | `mmap_direct` if ever admitted | `deferred` | A future admitted row would use row-major normalized `[]float32` plus `dims` and immutable asset identity. | Not in the healthy current-format loop by default; only a future admitted scoring path may touch it per candidate. | Healthy search must ignore or fail closed on this role unless a later PR explicitly promotes it; it MUST NOT silently replace base-vector evidence or bypass #2040 scoring admission. | Promotion must add role-specific counters such as `normalized_vector_mmap_direct/search`, fallback counters, reopen and mismatch tests, finite-value tests, zero healthy fallback proof, and `graph_rows=0`. | Future promotion must update this row with #2037 serial and parallel graph-only benchmarks, setup/open cost, `ns/op`, `ops/sec`, `B/op`, `allocs/op`, memory residency, and direct/fallback counters before changing status. |
| Legacy graph-row vector, norm, row-ref, and result-ID payloads | Legacy physical `column_graph` graph-row assets and graph row ID bytes from pre-alpha records. | Legacy graph-row physical asset, not typed-column optimized state | not a #2047 typed-column tier | `fallback-only` | No prepared shape for healthy search; compatibility readers may expose decoded row payloads only for old fixtures. | Legacy compatibility or diagnostics only, excluded from healthy current-format traversal, scoring, and result-ID evidence. | Compatibility fallback must be explicit and counted; healthy current-format search MUST NOT silently fall back to graph-row vectors, norms, row refs, or IDs. | Counters and tests include `graph_rows=0`, `row_ref_vector_source_legacy_graph_ids`, `result_id_graph_fallbacks`, legacy compatibility fixtures, graph-row fallback quarantine tests, and zero fallback assertions on current-format rebuilds. | Compatibility benchmarks, if run, must be labeled legacy and excluded from promotion evidence. No #2037 healthy-path row may rely on this fallback for `ns/op`, `ops/sec`, `B/op`, or `allocs/op` claims. |
| Legacy graph-specific adjacency sources | Quarantined `TCGA` or `TCGL` graph-specific adjacency-source metadata, `adjacency_layout`, `column_graph_layer0_adjacency`, and `column_graph_adjacency_layer`. | `adjacency_list` / `raw_uint32_offsets_list` compatibility selector | `unsupported/experimental` | `fallback-only` | No prepared shape for healthy search; the generic offsets/value mechanics must be consumed through `uint32_list` vector-index state instead. | Legacy compatibility only, outside healthy edge traversal evidence. | Compatibility fallback must be explicit and counted; healthy current-format search MUST NOT read graph-specific adjacency-source payloads or treat this row as admitted adjacency state. | Counters and tests include `adjacency_source_fallbacks/search`, `adjacency_legacy_fallbacks/search`, `adjacency_validation_failures/search`, #1989 quarantine tests, no new rebuild publication tests, and zero fallback assertions. | Compatibility benchmarks, if run, must be labeled legacy. Healthy #2037 adjacency evidence must use `uint32_list` and report `ns/op`, `ops/sec`, `B/op`, `allocs/op`, edges/search, and typed-list direct counters. |
| Legacy dense adjacency or row-image adjacency | Legacy dense `adjacency_list` row images, row-image adjacency, or row-asset adjacency compatibility payloads. | `adjacency_list` / `raw_uint32_dense` or physical row-image adjacency | `scratch_decode` for dense typed-column compatibility, otherwise not a #2047 typed-column tier | `fallback-only` | No prepared shape for healthy search; compatibility readers may scratch-decode old dense rows only when explicitly requested by old fixtures. | Legacy compatibility only, outside healthy edge traversal evidence. | Compatibility fallback must be explicit and counted; healthy current-format search MUST NOT use dense row-image adjacency as a silent fallback for missing typed-list state. | Counters and tests include `adjacency_legacy_fallbacks/search`, scratch-decode fallback counters, dense adjacency compatibility tests, quarantine tests, and zero fallback assertions on current-format rebuilds. | Compatibility benchmarks, if run, must be labeled legacy or diagnostic. No healthy #2037 promotion row may rely on this fallback for `ns/op`, `ops/sec`, `B/op`, or `allocs/op` claims. |

## Evidence required to promote a row to `admitted`

A descendant PR that changes a `pending`, `deferred`, or `experimental` row to
`admitted` must update this document and provide all of the following in the PR
body or linked evidence:

1. The #2047 tier row and #2046 certifier coverage that proves the direct-view
   eligibility used by the graph-search role.
2. Open/searcher-time certification tests for identity, bounds, endianness,
   alignment, lifetime, checksum/read integrity, row counts, wrappers,
   graph-owned invariants, and stale/corrupt refs.
3. Reopen and compatibility tests, plus fail-closed tests for missing, stale,
   corrupt, below-tier, or unsupported states.
4. Direct/prepared counters, heap/scratch/generic fallback counters, graph-row
   fallback counters, validation-failure counters, and stale/lifetime counters.
5. #2037 benchmark evidence with identical fixtures and command lines for the
   relevant setup/open/search/result/doc-fetch boundaries, reporting `ns/op`,
   `ops/sec`, `B/op`, `allocs/op`, memory residency, and role counters.
6. A performance-regression assessment. Any material regression in wall time,
   allocations, memory, direct counters, fallback counters, storage/rebuild
   overhead, or result materialization is blocking unless explicitly minimized,
   documented, and accepted.

Future graph-search typed-column roles must be added here as `pending`,
`deferred`, or `experimental` before code depends on them. If a role is omitted,
docs lint fails for new vector-index state roles and reviewers should treat the
missing row as a fail-closed admission blocker.
