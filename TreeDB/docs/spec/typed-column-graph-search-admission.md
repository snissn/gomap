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
table records #2038-admitted adjacency prepared CSR views, #2040-admitted
base-vector and inverse-norm prepared scoring views, #2041-admitted row-ref and
document-ID side-channel views, #2045 combined prepared graph-search routing,
minimal hot-loop telemetry from #2042, and the #2043 closeout decision backed by
the #2037 benchmark truth matrix.

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
  `row_ref_vector_source_legacy_graph_ids=0`,
  `result_id_graph_fallbacks=0`, `graph_row_fallbacks/search=0`, and
  `prepared_graph_search_views/search=1` where those counters apply.

## #2045 combined prepared routing

A healthy current-format `column_graph` searcher is admitted to the optimized
path only when all admitted state rows below are simultaneously certified for one
immutable manifest identity: base vectors, HNSW adjacency, inverse norms, row
refs, and document IDs. Reader/searcher open builds one combined prepared
runtime view and the HNSW traversal consumes that view directly. If any required
state is missing, stale, corrupt, below `mmap_direct`, or not certifiable, the
current-format open/search path fails closed or uses an explicitly labeled
compatibility path; it must not silently dispatch back to graph-row selectors in
the healthy loop.

Evidence counters for #2045:

- `prepared_graph_search_views/search=1` for healthy current-format searches;
- `graph_row_fallbacks/search=0`, plus the role-specific fallback counters below
  remaining zero;
- `graph_rows=0` for current-format graph-only/result/document rows.

## #2043 final type readiness summary

The #2043 closeout run on `af003dfaf255ae217dbec6eb4a3afae08c2aa4aa` keeps all
required healthy current-format graph-search roles admitted. It records the
combined prepared route as the primary current-format execution route while also
recording that #2035 is not fully performance-satisfied: the final graph-only
wall-time matrix still has net throughput gaps, and the legacy/current rows are
not apples-to-apples storage-path evidence because they visit 612 versus 3340
edges/search (about 5.5x). Therefore this table is an admission/readiness
promotion, not an unconditional claim that #2035 beat the legacy control. The
focused #2043 adjacency-access microbenchmark adds a narrower finding: with
ordinals and edge count held constant and without running `SearchCosine`,
prepared CSR adjacency access is zero-allocation and faster than graph-row
compatibility adjacency decode in the local run, so adjacency access is not the
cause of the 5.5x visited-edge mismatch.

| State role | Logical / physical type | Prepared runtime shape | Admission status | #2043 evidence |
| --- | --- | --- | --- | --- |
| Base vectors | `float32_vector` / `raw_float32_vector` | Row-major direct `[]float32` with optional row-ref mapping. | `admitted` | `vector_prepared_direct/search=182`, `typed_column_vector_fallbacks/search=0`, and `graph_rows=0` on production8192 combined prepared graph-only rows. |
| HNSW adjacency | `uint32_list` / `raw_uint32_offsets_list` | Prepared CSR offsets/values for every layer. | `admitted` | `adjacency_prepared_csr_mmap_direct/search=108`, `adjacency_typed_list_mmap_direct/search=0`, `adjacency_legacy_fallbacks/search=0`, and `adjacency_source_fallbacks/search=0` on production8192 rows. |
| Inverse norms | `float32` / `raw_float32` | Direct `[]float32` indexed by graph ordinal. | `admitted` | `norm_prepared_direct/search=182`, `norm_source_fallbacks/search=0`, and zero graph-row fallback on combined prepared graph-only rows. |
| Row refs | `int64` / `raw_int64` | Direct row-ref arrays assembled only at result/document boundaries. | `admitted` | `row_ref_state_prepared_views/search=1`, `row_ref_state_result_refs/search=10` on result rows, `doc_row_ref_state_fetches/search=10` on document rows, and row-ref source fallbacks zero. |
| Document IDs | `bytes` / `raw_bytes_offsets` | Direct offsets plus bytes, copied/exposed only for final top-k IDs. | `admitted` | `result_id_prepared_bytes_views/search=1`, `result_id_typed_bytes_state/search=10`, `result_id_graph_fallbacks/search=0`, with 784 B/op and 2 allocs/op at the result-ID API boundary. |
| Optional normalized vectors | `float32_vector` / `raw_float32_vector` | Future row-major normalized `[]float32` view if admitted. | `deferred` | #2043 profiles do not justify adding derived normalized payload storage over the admitted raw-vector plus inverse-norm scoring path; #1977 remains evidence-gated. |

## Current graph-search optimized-state readiness table

| Graph-search role | State key / owner | Canonical format | #2047 tier | Admission status | Prepared runtime shape | Hot-loop boundary | Fallback/fail-closed rule | Counters and tests | Benchmark/admission evidence |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Base vectors | Base `typed_column_part` vector field for the indexed collection field; ordinal mapping may consume `row_refs` when graph ordinals differ from base rows. | `float32_vector` / `raw_float32_vector` | `mmap_direct` | `admitted` by #2040 | Row-major `[]float32` plus `dims`, immutable asset identity, and optional ordinal-to-row mapping. | Candidate scoring only. Query validation and finite-value proof happen outside traversal; prepared scoring slices the certified direct view without a generic adapter. | Current-format search must fail closed or require rebuild if direct prepared vector state is absent, stale, corrupt, or below `mmap_direct`; it MUST NOT fetch candidate vectors from legacy graph rows or a generic adapter fallback. | Direct, prepared, and fallback counters include `vector_mmap_direct/search`, `vector_prepared_direct/search`, `vector_prepared_identity_mapping/search`, `vector_prepared_row_ref_mapping/search`, `vector_heap_copy_typed_view/search`, `vector_scratch_decodes/search`, `typed_column_vector_fallbacks/search`, `vector_certification_failures/search`, and `graph_rows=0`. Tests cover reopen, identity mapping, row-ref mapping, dimension mismatch, row-count mismatch, wrong endian, length, alignment, stale handle, finite-value proof or rejection, and zero healthy fallback. | #2040 admission evidence uses #2037 `BenchmarkColumnVectorGraphNativeSearchCosineV3`, `BenchmarkColumnVectorGraphNativeSearchCosineParallelV3`, setup/open rows, `ns/op`, `ops/sec`, `B/op`, `allocs/op`, candidates/search, prepared score counters, and direct/prepared/fallback counters with zero healthy graph-row fallback and no unaccepted material regression. |
| HNSW adjacency | Vector-index state role `adjacency` with one asset per HNSW layer. | `uint32_list` / `raw_uint32_offsets_list` | `mmap_direct` | `admitted` by #2038 | Per-layer CSR view with `Offsets []uint64` and `Values []uint32` bound to one searcher manifest identity. | Edge traversal and neighbor expansion only. Each expanded ordinal reads one layer slice directly. | Current-format search must fail closed or require rebuild if prepared CSR typed-list state is missing, corrupt, stale, or below `mmap_direct`; it MUST NOT read legacy graph-row adjacency, legacy `adjacency_list`, graph-specific adjacency-source payloads, or generic typed-list source reads on the healthy path. | Direct and fallback counters include `adjacency_prepared_csr_mmap_direct/search`, `adjacency_prepared_csr_direct_views/search`, generic fallback counters `adjacency_typed_list_mmap_direct/search`, `adjacency_typed_list_heap_copy_typed_view/search`, `adjacency_typed_list_scratch_decodes/search`, plus `adjacency_legacy_fallbacks/search`, `adjacency_source_fallbacks/search`, `adjacency_validation_failures/search`, and `adjacency_stale_handles/search`. Tests cover reopen, missing layer, layer gap, stale TVIS identity, offset and value corruption, neighbor out-of-bounds, all-layer direct views, zero generic typed-list fallback on mmap-capable current-format search, and zero legacy fallback. | #2037 reports serial and parallel graph-only search, edges/search, visited_edges/search, candidates/search, `ns/op`, `ops/sec`, `B/op`, `allocs/op`, setup/open cost, prepared-CSR counters, generic typed-list fallback counters, and graph-row fallback counters for #2038 evidence. |
| Inverse norms | Vector-index state role `inverse_norm` for cosine scoring. | `float32` / `raw_float32` | `mmap_direct` | `admitted` by #2040 | Direct `[]float32` indexed by graph ordinal with immutable manifest identity and metric metadata. | Candidate scoring for cosine only; norm lookup is a direct index operation adjacent to vector scoring and must not decode per candidate. | Current-format cosine search must fail closed or require rebuild if norm state is missing, stale, corrupt, non-finite, or below `mmap_direct`; it MUST NOT read inverse norms from legacy graph rows or scratch decode in healthy search. | Direct, prepared, and fallback counters include `norm_mmap_direct/search`, `norm_prepared_direct/search`, `norm_heap_copy_typed_view/search`, `norm_scratch_decodes/search`, `norm_source_fallbacks/search`, `norm_validation_failures/search`, and `norm_stale_handles/search`. Tests cover reopen, row-count mismatch, length mismatch, alignment failure, stale handle, finite-value proof or rejection, metric gating, and zero healthy fallback. | #2040 admission evidence keeps norm counters inside the search/scoring boundary and reports serial and parallel graph-only search, `ns/op`, `ops/sec`, `B/op`, `allocs/op`, prepared score counters, and direct/prepared/fallback counters with zero healthy graph-row fallback and no unaccepted material regression. |
| Row refs | Vector-index state role `row_refs` carrying `DocumentRowRef` coordinate assets. | `int64` / `raw_int64` | `mmap_direct` | `admitted` by #2041 | Direct row-ref arrays that are assembled into row-ref records only at result or document-fetch boundaries. | Result and document-fetch side channel only, except a prepared ordinal-to-base-row map may be consulted during scoring after open-time preparation. | Current-format search must fail closed or disable result/document materialization until rebuild when row-ref state is missing, stale, corrupt, or below `mmap_direct`; it MUST NOT scan legacy graph row IDs to map ordinals or fetch documents in healthy evidence. | Benchmark labels include `row_ref_state_prepared_views/search`, `row_ref_state_mmap_direct_fields/search`, `row_ref_state_result_refs/search`, `row_ref_state_source_fallbacks/search`, `doc_row_ref_state_fetches/search`, `doc_row_ref_lookup_fallbacks/search`, and `doc_row_ref_validation_failures/search`; search stats also expose `row_ref_vector_source_state` and `row_ref_vector_source_legacy_graph_ids`. Tests cover reopen, duplicate asset role, missing asset role, coordinate bounds, stale manifest identity, final top-k fetch, no legacy ID scan, corrupt field rejection, and fail-closed validation. | #2041 evidence uses the #2037 matrix to report row-ref counters separately from traversal/scoring, includes result/doc-fetch timing boundaries, `ns/op`, `ops/sec`, `B/op`, `allocs/op`, and direct/fallback counters. |
| Document IDs | Vector-index state role `document_ids` with one opaque bytes value per graph ordinal. | `bytes` / `raw_bytes_offsets` | `mmap_direct` | `admitted` by #2041 | Direct `Offsets []uint64` plus `Bytes []byte`; public result IDs are copied or buffer-owned only at the API boundary. | Final top-k result-ID materialization only. Traversal and scoring must not touch document ID bytes. | Current-format search must fail closed or require rebuild if document-ID state is missing, stale, corrupt, or below `mmap_direct`; it MUST NOT return IDs from legacy graph rows in healthy evidence. | Benchmark labels include `result_id_prepared_bytes_views/search`, `result_id_typed_bytes_state/search`, `result_id_graph_fallbacks/search`, `result_id_state_validation_failures/search`, and `docs_fetched/search`, plus result-ID byte/output counters. Tests cover reopen, arbitrary binary IDs, offset/value corruption, missing state fail-closed, final top-k-only access, and zero graph fallback. | #2041 evidence uses the #2037 matrix split between no-document search, result-ID, and with-documents rows, and reports `BenchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsV4`, `docs_fetched/search`, `ns/op`, `ops/sec`, `B/op`, `allocs/op`, and direct/fallback counters. |
| Optional normalized vectors | Vector-index state role `normalized_vectors`; optional derived state from #1977 and not a required current healthy dependency. | `float32_vector` / `raw_float32_vector` | `mmap_direct` if ever admitted | `deferred` | A future admitted row would use row-major normalized `[]float32` plus `dims` and immutable asset identity. | Not in the healthy current-format loop by default; only a future admitted scoring path may touch it per candidate. | Healthy search must ignore or fail closed on this role unless a later PR explicitly promotes it; it MUST NOT silently replace base-vector evidence or bypass #2040 scoring admission. | Promotion must add role-specific counters such as `normalized_vector_mmap_direct/search`, fallback counters, reopen and mismatch tests, finite-value tests, zero healthy fallback proof, and `graph_rows=0`. | Future promotion must update this row with #2037 serial and parallel graph-only benchmarks, setup/open cost, `ns/op`, `ops/sec`, `B/op`, `allocs/op`, memory residency, and direct/fallback counters before changing status. |
| HNSW search pack | Existing vector-index state role `hnsw_search_pack`, asset id `hnsw_search_pack_v1`. | `hnsw_search_pack` / `hnsw_search_pack_v1` | Prepared `mmap_direct`, or explicitly counted `heap_copy` platform/resource fallback. | `admitted` for the existing eligible base-only buffered route; this corrects stale prepared-view-only wording, not an M2 route promotion. | One immutable prepared pack of vectors, levels, adjacency CSR, row refs, and document IDs, bound to the base identity and mappedresource owner. | Existing `VectorIndexSearcher.SearchWithBuffer` dispatch via `hnswSearchPackSearchWithBufferRoute`; candidate scoring/traversal, then final-result identity access. Eligibility excludes unsupported query/stats modes. | Preserve existing public route-specific fallback/fail-closed behavior. The experimental bounded M2 consumer below requires an eligible pack and has no generic traversal fallback. | Existing format, identity, integrity, lifetime, rewrite, and direct/heap accounting tests remain applicable. `TestVectorIndexSearcherSearchWithBufferRouteStatsAndNoDocumentBoundary2311` and `TestVectorIndexSearcherSearchWithBufferResultEquivalenceAndZeroAllocs2124` cover the landed route. Pack active/missing/invalid/stale/closed and mmap-direct/heap-copy counters remain distinct. | Existing base-only buffered benchmarks report timing, allocations, and route counters. M2 bounded-layer0 search changes the traversal budget regime and requires its own recall, work, allocation, and residency evidence; it cannot borrow base-only results as enabled-overlay qualification. |
| Quantized scalar_u8 and RaBitQ codes | Vector-index state role `quantized_codes` with asset ids `quantized/<name>/codes` for legacy scalar_u8, config-hashed scalar_u8 ids for calibrated modes, and `quantized/<name>/packed_codes` for `rabitq_1bit`; calibrated scalar_u8 also has a sibling alpha metadata asset. | scalar_u8 codes: `byte_vector` / `raw_fixed_bytes`; scalar_u8 alpha metadata: `scalar_u8_alpha` / `raw_float32_uint32`; RaBitQ: `packed_bit_vector` / `raw_packed_bit_vector` plus `code_count` and `quantized_dot_product_inv` side arrays | `mmap_direct` or explicit heap-copy prepared asset residency for code rows; alpha metadata is prepared with the same fail-closed identity checks | `admitted` for explicit legacy scalar_u8 and pure-Go RaBitQ quantized modes by #1926/#2451/#2452/#2454 closeout; explicit per-granule-alpha scalar_u8 scoring admitted by #2844 but not promoted as the default by #2845 | Prepared `quantizedasset` fixed-byte or packed-code view with codec identity, immutable base-graph identity, alpha granule metadata when selected, and RaBitQ LSB-first bits/zero padding/side arrays when selected. | Explicit legacy scalar_u8/RaBitQ `quantized_only` traversal/final ranking and explicit `quantized_rerank` traversal/candidate collection only; exact/default traversal and scoring must not touch it. `quantized_rerank` traverses the normalized `ef_search` candidate pool, trims to `QuantizedRerankCandidates`, and exact-scores only the validated shortlist through the authoritative float32 vector/norm path. Explicit per-granule-alpha scalar_u8 uses matching code and alpha assets; omitted scalar_u8 calibration remains legacy by default after #2845. | Quantized modes must fail closed when this role, selected asset id, codec config, alpha/side arrays, graph identity, row count, checksum, or prepared scorer is missing/stale/corrupt/mismatched/closed; they MUST NOT silently fall back to exact candidate collection. | Counters and tests cover rebuild, reopen, `quantizedasset.Prepare` validation, missing/stale/mismatch/closed failure, exact-mode unchanged, scalar_u8 and RaBitQ code score/read counters, scalar_u8 alpha metadata validation, `quantized_score_codec_scalar_u8_alpha/search`, no exact vector/norm scoring in `quantized_only`, exact final scores in `quantized_rerank`, normalized `ef_search` traversal before shortlist trim, `quantized_rerank_exact_score_calls/search`, collection buffered cache/lifecycle, and `0` steady-state allocations. | #1926/#2454 closeout uses `BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926`, `BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414`, `BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedAlphaSearchWithBuffer2414`, `BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451`, `BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415`, `BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8QuantizedAlpha2415`, `BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452`, `BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926`, and `BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450` to report exact/scalar/RaBitQ `ns/op`, `ops/sec`, `B/op`, `allocs/op`, recall@K, route counters, exact-read counters, logical code bytes/vector, asset bytes/vector, storage bytes, and c=1/c=8 profile artifacts. Current scalar Go scorer evidence makes no speedup claim, go-highway acceleration did not land, and #2845 kept alpha explicit because quality improved but collection hot-row runtime evidence was mixed. |
| Quantized scalar_u8 alpha metadata | Vector-index state role `quantized_alpha` with config-hashed scalar_u8 alpha asset ids `quantized/<name>/scalar_u8/<config_hash>/alpha`. | `scalar_u8_alpha` / `raw_float32_uint32` with one positive finite float32 alpha and one uint32 row count per storage-layout granule. | `mmap_direct` or explicit heap-copy prepared asset residency; alpha metadata is prepared with the same fail-closed identity checks as quantized codes | `admitted` for metadata persistence by #2843 and scorer use by #2844; default remains explicit/off after #2845 | Prepared `quantizedasset` scalar payloads plus a row-to-granule lookup bound to codec config, base graph identity, granule count, row-count sequence, asset ref, and checksum. | Prepared-open validation and scalar_u8 alpha-aware scoring only; exact/default traversal must not touch alpha metadata. | Quantized alpha modes must fail closed when this role, asset id, codec config, graph identity, granule count, row-count sequence, checksum, alpha finiteness, or prepared handle is missing/stale/corrupt/mismatched/closed; they MUST NOT silently fall back to legacy scalar_u8 or exact candidate collection. | Counters and tests cover rebuild, reopen, `quantizedasset.Prepare` validation, missing/stale/mismatch/closed failure, invalid zero/negative/NaN/Inf alpha rejection, storage-layout row-count identity, `quantized_score_codec_scalar_u8_alpha/search`, hot-row `0` allocations, and `quantized_alpha` state status validation. | #2843 reports rebuild/storage overhead, alpha bytes/vector, code bytes/vector, mmap/direct vs heap-copy status; #2844/#2845 report `ns/op`, `ops/sec`, `B/op`, `allocs/op`, route counters, recall/quality, and default-gate evidence. |

| Legacy graph-row vector, norm, row-ref, and result-ID payloads | Legacy physical `column_graph` graph-row assets and graph row ID bytes from pre-alpha records. | Legacy graph-row physical asset, not typed-column optimized state | not a #2047 typed-column tier | `fallback-only` | No prepared shape for healthy search; compatibility readers may expose decoded row payloads only for old fixtures. | Legacy compatibility or diagnostics only, excluded from healthy current-format traversal, scoring, and result-ID evidence. | Compatibility fallback must be explicit and counted; healthy current-format search MUST NOT silently fall back to graph-row vectors, norms, row refs, or IDs. | Counters and tests include `graph_rows=0`, `row_ref_vector_source_legacy_graph_ids`, `result_id_graph_fallbacks`, legacy compatibility fixtures, graph-row fallback quarantine tests, and zero fallback assertions on current-format rebuilds. | Compatibility benchmarks, if run, must be labeled legacy and excluded from promotion evidence. No #2037 healthy-path row may rely on this fallback for `ns/op`, `ops/sec`, `B/op`, or `allocs/op` claims. |
| Legacy graph-specific adjacency sources | Quarantined `TCGA` or `TCGL` graph-specific adjacency-source metadata, `adjacency_layout`, `column_graph_layer0_adjacency`, and `column_graph_adjacency_layer`. | `adjacency_list` / `raw_uint32_offsets_list` compatibility selector | `unsupported/experimental` | `fallback-only` | No prepared shape for healthy search; the generic offsets/value mechanics must be consumed through `uint32_list` vector-index state instead. | Legacy compatibility only, outside healthy edge traversal evidence. | Compatibility fallback must be explicit and counted; healthy current-format search MUST NOT read graph-specific adjacency-source payloads or treat this row as admitted adjacency state. | Counters and tests include `adjacency_source_fallbacks/search`, `adjacency_legacy_fallbacks/search`, `adjacency_validation_failures/search`, #1989 quarantine tests, no new rebuild publication tests, and zero fallback assertions. | Compatibility benchmarks, if run, must be labeled legacy. Healthy #2037 adjacency evidence must use `uint32_list` prepared CSR state and report `ns/op`, `ops/sec`, `B/op`, `allocs/op`, edges/search, `adjacency_prepared_csr_mmap_direct/search`, generic typed-list fallback counters, and graph-row fallback counters. |
| Legacy dense adjacency or row-image adjacency | Legacy dense `adjacency_list` row images, row-image adjacency, or row-asset adjacency compatibility payloads. | `adjacency_list` / `raw_uint32_dense` or physical row-image adjacency | `scratch_decode` for dense typed-column compatibility, otherwise not a #2047 typed-column tier | `fallback-only` | No prepared shape for healthy search; compatibility readers may scratch-decode old dense rows only when explicitly requested by old fixtures. | Legacy compatibility only, outside healthy edge traversal evidence. | Compatibility fallback must be explicit and counted; healthy current-format search MUST NOT use dense row-image adjacency as a silent fallback for missing typed-list state. | Counters and tests include `adjacency_legacy_fallbacks/search`, scratch-decode fallback counters, dense adjacency compatibility tests, quarantine tests, and zero fallback assertions on current-format rebuilds. | Compatibility benchmarks, if run, must be labeled legacy or diagnostic. No healthy #2037 promotion row may rely on this fallback for `ns/op`, `ops/sec`, `B/op`, or `allocs/op` claims. |

## M2 internal typed mutation suffix (experimental)

| Graph-search role | State key / owner | Canonical format | #2047 tier | Admission status | Prepared runtime shape | Hot-loop boundary | Fallback/fail-closed rule | Counters and tests | Benchmark/admission evidence |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Typed mutation suffix | Existing published typed row/vector parts after the pinned graph base; no new durable state key. | Declared strings, FP32 vectors, and existing typed tombstone rows. | `heap_typed_view` for the bounded suffix only; immutable base requirements remain unchanged. | `experimental`, internal-only under #4617 | Newest-ID typed rows plus retained tombstones, prepared from a checked current suffix while borrowing the old accelerator and current query pins. | Internal prepared-pack search and bound scalar filtering, bounded exact suffix scoring, and buffered result merge; no ordinary mutable graph dispatch. | Fail closed on graph/schema/index or base-asset mismatch, future parts, source limits, missing pack, and exhausted candidate budget. MUST NOT use native heap HNSW or indexed retained-JSON extraction. | `TestTypedGraphOverlayExistingBaseMutation`, `TestTypedGraphOverlaySuffixLineageAndBounds`, `TestTypedGraphOverlayRepeatedMutationAndSnapshot`, and `TestTypedGraphOverlaySearchShadowsAndBudget`; separate cumulative source, delta-score, base-work, and residency counters. | Preparation/search allocation, decoded working-set, retained residency, wall-time, candidate-work, and large-filter recall evidence remain required before promotion. No healthy-route performance claim. |

The current snapshot is the sole logical query view. The old snapshot is only
an immutable graph accelerator, not an alternative scalar/text/document view.
Original row and vector assets must remain exactly reachable; later append-only
base parts count toward the suffix just like replacements and tombstones.
Limits count overwritten versions too, before allocating the suffix ID map.
Physical source bytes, decoder working space (including value headers), and
retained payload are distinct accounting terms. Existing typed decoding owns
FP32 output arrays; preparation reuses those arrays and row-header scratch.

The internal unfiltered search primitive reuses that existing prepared pack and
the existing buffered score/ID merge. It retrieves base top-K plus the bounded
suffix ID count before excluding shadowed result IDs, so deleting/replacing the
old top-K does not silently underfill merely because retrieval was truncated.
Document IDs are read only at this bounded result boundary, not per candidate.
Suffix scoring is bounded exact work, counted separately from base graph work.

A positive pack candidate limit uses its existing bounded layer-0 regime and
skips upper-layer greedy descent. This is not algorithmically identical to the
default base-only search benchmark. Hitting a cap below corpus size returns an
explicit search-budget error, with the output buffer invalidated, even if K rows
were collected. Candidate counts, native/auxiliary edge counts, result-ID work,
and mmap-direct versus heap-copy residency are separate evidence. Per-node
expansion is bounded by the validated native degree plus the existing V3
auxiliary bound. Search exhaustion is distinct from suffix fold debt: folding
cannot necessarily cure a base ANN work cap.

The internal filtered consumer prepares complete persisted scalar posting sets
once from the current pin. It intersects complete leaves before classifying the
final set: at most 4,096 rows use typed exact scoring, larger sets use prepared
pack ANN plus separately counted exact suffix work. Thus two large leaves may
correctly produce a small exact intersection. Incomplete probes fail with an
explicit budget error. Source ID count and bytes are checked before copying;
physical inspected entries (including tombstones), mapping work and retained
ordinal bytes have separate caller limits. Array/multikey dedupe is unsupported
in this internal typed-scalar seam. A single leaf streams borrowed posting IDs
through a bounded 512-ID locator chunk, without an owning string set. Multiple
leaves retain complete owning sets for intersection. Single-leaf ordinal slice
capacity is checked before growth; the separate ordinal growth-peak counter
includes old plus new buffers and the final exact-rank copy, including buffers
released by all/range canonicalization. It is not a total Go heap bound.

An additional experimental internal seam prepares a caller-owned immutable base
filter from the searcher's existing snapshot and lazy document view. Current
bindings borrow its base selection unchanged, map only bounded suffix IDs through
the base locator/inverse, and evaluate owned compiled scalar string predicates on
current typed values. No current posting scan or corpus selection subtraction is
performed during binding. Count is base eligibility minus eligible shadows plus
matching current suffix rows. Exact threshold transitions enumerate at most
4,096 plus the bounded exclusions, under a separate enumeration limit. ANN uses
bounded base-result overfetch before discarding shadows; expanded base result IDs
are counted, not described as final-K-only reads. Delta-only sets at most4,096
use typed exact scoring; larger sets without surviving eligible base rows remain
unavailable rather than being relabeled ANN. Base work exhaustion is distinct
from fold debt.

The base plan owns encoded predicate bounds and pins logical scalar definitions;
changed/missing definitions fail binding. Identical-definition physical scalar
recreation does not alter predicate semantics and is not detected as a new
incarnation (scalar definitions have no incarnation epoch). Existing graph and
typed-asset lineage checks still reject graph/schema changes. Searcher, lazy view
initialization, binding and Close retain the existing non-concurrent ownership
contract. Cold preparation, bounded binding, same-pin query and complete
changing-pin costs require separate evidence before promotion.

Preparation rejects closed base/current views before scans or cache creation,
including borrowed views whose underlying snapshot is still open. All FP32
columns in the suffix schema must be owned by `typed_column_part`; extra
row-asset-owned FP32 columns are unsupported because their decoded vectors borrow
row-reader scratch. Supported typed-part vectors retain their owning decoder
storage without an additional clone.

| Graph-search role | Owner / tier | Admission status | Prepared shape and query boundary | Evidence required |
| --- | --- | --- | --- | --- |
| Current-pin scalar eligibility | Borrowed current read pin and checked base; `heap_typed_view`, no durable state key or global cache. | `experimental`, internal-only under #4617 | Compact base `RowSelection`, bounded suffix ordinals, and an exact-only ID-ranked ordinal slice of at most 4,096 entries. Queries reuse these without posting, locator or per-candidate ID reads. | `TestTypedGraphPreparedFilterFinalIntersectionAndBounds` and `TestTypedGraphOverlaySearchShadowsAndBudget`; preparation bytes/work, repeated-query allocation, residency, dispersed ANN recall and matched wall-time qualification remain required. |

The exact ID-rank slice preserves document-ID cutoff ties through the existing
ordinal heap comparator. Its extra word per base row is charged before
allocation; mapped ID comparisons happen only during preparation. ANN retains
the existing approximate traversal tie semantics. Ineligible graph nodes remain
traversable but cannot enter retained results. A positional sparse/all/range
cursor enumerates selected seeds without scanning the corpus: every inspected
ordinal is already visited or immediately scored, so seed inspection is bounded
by scored candidates. Candidate/seed exhaustion returns no partial result and
preserves performed work counters. Wrong-overlay or closed-pin plans fail.

Final-K materialization uses the same current read view, tested across later
delete/reinsert and publication. `TestTypedGraphBaseFilterBindingNewIDAndCurrentOutput`
checks new-ID and conjunctive eligibility against current postings and verifies
content, metadata, residual output and FP32 bits from the held query pin after a
later write. `TestTypedGraphBaseFilterIndependentReadersDuringPublication` uses
independently owned searchers, plans and buffers; it does not promise concurrent
use or Close of one searcher. `TestTypedGraphBaseFilterBindingDeltaOnlyThreshold`
distinguishes 4,096 typed-exact suffix results from unavailable delta-only ANN.

`BenchmarkTypedGraphBaseFilterBindingBoundaries` separates cold preparation,
overlay preparation, bounded binding, warm query and actual new-pin reads. The
new-pin boundary excludes preceding write/ack and final document materialization;
its ordered cumulative physical versions are not matched cross-filter samples.
`TestTypedGraphBaseFilterRepresentativeSuffixResidency` covers empty, 16-row,
256-row and 128-tombstone suffixes, source-cap failures, genuine base graph work,
query allocations and separately retained preparation copies. Its signed live
heap deltas include pool/GC effects and must not be interpreted as negative
memory use. Source string payload, ordinal capacities and locator chunk payload
are not total heap bounds. The dispersed 50,000-row engine quality diagnostic
does not replace final Minima application qualification. Public installation,
incremental current-pin publication and fold policy remain separate M3 work;
no public filtered route or concurrent-writer latency qualification is claimed.

New rebuilds also publish optional `row_refs` asset
`base_row_ref/ordinal_by_physical_row`: an `int64` / `raw_int64` graph-ordinal
permutation sorted by `(generation, part_id, row_index)` through the existing
forward coordinates. Its payload costs exactly eight bytes per graph row, plus
existing typed-asset framing; sparse physical row indexes do not enlarge it.
Writer and mapped open reject duplicate physical coordinates, out-of-range
ordinals and non-strict order. Binary lookup additionally requires identical
applied LSN. The asset uses the existing graph/schema/base-manifest identity and
prepared handle ownership. Existing four-coordinate base readers remain valid;
internal filtered search requires this inverse asset and otherwise requests a
rebuild, never synthesizing a corpus heap map during queries. This new mapping
role is experimental pending build/open/residency and filtered ANN evidence;
the existing base-only admission rules are unchanged. Tests:
`TestTypedGraphInversePermutation` and `TestTypedGraphInverseMappedAndOptional`.

The internal preparation/search does not install a mutable graph route. M1 typed
mutations remain durable under their selected profile, while ordinary graph
search after mutation remains explicitly unavailable. M3 owns durable graph
coverage/frontier installation, reopen, and fold lifecycle. Index drop/recreate
is still rejected by the selected command-WAL root-publication barrier; this is
not a claim that public recreate is supported or tested through completion.

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
