# Prepared Typed-Column Graph-Search Runtime Views (#2036)

Status: pre-alpha graph-search policy. This document owns the role-specific
prepared runtime-view contract for current `column_graph` typed-column search.
It consumes the tier vocabulary and generic type matrix in
`typed-column-optimized-consumer-capabilities.md`; it does not redefine those
`mmap_direct`, `heap_typed_view`, `scratch_decode`, `predicate_only`,
`generic_only`, or `unsupported/experimental` tiers.

The #2044 readiness/admission table and docs-lint enforcement live in
`typed-column-graph-search-admission.md`; the #2037 benchmark truth-matrix labels
and command contract live in `typed-column-graph-search-benchmark-matrix.md`.
Runtime graph-search admission enforcement is owned by #2044 and benchmark
truth-matrix reporting by #2037. The reusable #2046 primitive certifier substrate
lives in `TreeDB/internal/typeddecode` and remains a prerequisite, not
row-admission evidence by itself. #2038 admits adjacency prepared CSR views,
while #2040 admits base-vector and inverse-norm prepared scoring views, #2041
admits row-ref and document-ID side-channel views, and #2045 routes healthy
current-format search through one combined prepared graph-search view.

## Scope and ownership

The healthy current-format graph-search path is:

```text
authoritative typed-column and vector-index state assets
  -> open/searcher-time certification against one immutable manifest identity
  -> one combined prepared graph-search runtime view (#2045)
  -> zero-allocation HNSW traversal/scoring/top-k loop
  -> optional final result-ID and document materialization
```

## #2043 closeout status

The combined prepared view is the admitted primary healthy current-format
execution route because the final matrix shows prepared base-vector, adjacency,
inverse-norm, row-ref, and document-ID counters with zero current-format
graph-row/source fallbacks and `0 B/op`, `0 allocs/op` for graph-only search. It
is not an unconditional wall-time win over the old legacy graph-row direct
control and does not close #2035 as fully performance-satisfied: the final matrix
still shows net throughput gaps, and the legacy/current graph-only rows also
expose a topology/search-work mismatch (`612` versus `3340` visited_edges/search,
about 5.5x). #1979 now adds opt-in benchmark-debug control-flow counters on the
#2091 topology-parity fixture: the bounded equal-topology row visits 612 edges
mostly through already-visited layer-0 skips, while exact mode visits 100748
edges/search by scoring all 8192 candidates. #2098 adds an opt-in prepared
single-part indexed-scoring fast path with scalar/default result-equivalence
coverage, but does not make indexed scoring default-on. #1980 remains a
profile-backed frontier/top-k or already-visited follow-up if future
apples-to-apples profiles justify it; #1977 normalized-vector payloads remain
deferred.

Normative boundaries:

- Typed-column assets remain the canonical persisted state for current-format
  graph search. Legacy graph-row payloads are compatibility inputs only.
- Certification is an open/searcher-time operation. The HNSW candidate, edge,
  and score loops MUST NOT perform generic typed-column admission or direct-view
  certification per candidate or per edge.
- Healthy current-format graph-search state requires the #2047 `mmap_direct`
  tier for every current per-candidate, per-edge, per-score, and result-side
  state listed below unless `typed-column-graph-search-admission.md` admits a
  weaker tier for one named role with tests, counters, allocation evidence,
  memory evidence, and benchmark evidence.
- #2046 owns reusable certifier APIs. The shared `typeddecode` substrate
  certifies primitive owner/role/column identity, logical type and encoding,
  row counts, endian, wrapper, offset/bounds, alignment, direct-view handle, and
  lifetime conditions; this spec names the additional role-specific facts those
  certifiers and callers must prove before prepared views are trusted.
- #2037 owns the benchmark matrix. This spec defines the timing boundaries and
  counters that benchmark matrix must report.

## Common certification rules

### Experimental typed suffix and bound scalar filter (#4617)

The internal mutable consumer borrows two physical pins representing one logical
current view: immutable graph/schema/base identity must match checked lineage,
while current typed values, scalar visibility and output come from the current
pin. New graph builds include a compact sorted physical-row-to-ordinal
permutation in the existing row-ref asset (`8 * base_rows` payload bytes).
Filtered suffix preparation requires this mapped inverse and exact row-ref LSN;
an older base-only reader may omit it. Missing inverse state requires rebuild,
not a query-time corpus ID map.

A caller-owned cold scalar plan owns encoded string bounds and borrows a compact
base selection. Each current binding maps only bounded changed IDs, retains
bounded exclusions and matching suffix ordinals, and computes the complete final
count. Exact ranking owns at most 4,096 ID-ranked ordinals; ANN overfetch counts
expanded result IDs before shadow removal. Reusable query scratch is invalidated
on every error, and output remains buffer/pin-bound. Final materialization uses
the same current view, never the old accelerator's document state.

This `heap_typed_view` suffix is an explicitly experimental role in the
[admission table](typed-column-graph-search-admission.md); it does not weaken
immutable base-role requirements or install a public mutable route. Independent
workers own their searchers, plans and buffers; concurrent Close/lazy preparation
of one searcher is not supported. Source rows/bytes/tombstones, bound ownership,
predicate work, exact enumeration and graph candidate work have separate limits.
Fold debt and base search-budget exhaustion are different errors.

A current-format searcher may expose a prepared runtime view only after it has
certified all common conditions below for the immutable snapshot/manifest it is
bound to:

1. **Identity and ownership**: collection, index name, field path, metric,
   vector encoding, dimensions, HNSW parameters, base manifest generation,
   base manifest checksum, schema hash, vector-index state row count, asset role,
   asset id, asset ref kind, namespace, generation, part id, length, byte count,
   and checksum all match the active manifest and TVIS record.
2. **Tier eligibility**: the logical type plus physical encoding is classified
   as `mmap_direct` by `typed-column-optimized-consumer-capabilities.md` for the
   role being prepared, and wrappers or owners do not lower the effective tier.
3. **Direct-view safety**: the asset is non-null, non-default, uncompressed,
   little-endian where relevant, checksum/read-integrity compatible, mmap/view
   backed, live for the searcher lifetime, and aligned by both absolute storage
   offset and actual Go pointer before any unsafe typed slice is built.
4. **Shape and bounds**: fixed-width byte lengths, row counts, vector
   dimensions, offsets-list sentinels, monotonic offsets, final offsets, host
   `int` bounds, and values-section lengths are validated once before search.
5. **Graph-owned invariants**: graph ordinal bounds, expected HNSW layer count,
   deleted-row visibility, row-ref coordinate validity, and result-ID item count
   stay graph/search-owned even when the primitive typed-column certifier passes.
6. **Numeric scoring values**: scoring floats used by base vectors, optional
   normalized vectors, query vectors, and inverse norms must be finite before the
   score loop. Build/rebuild should reject or quarantine non-finite persisted
   values before publication; searcher open may certify a finite-value proof or
   run an explicitly counted compatibility scan outside the per-candidate loop.
   Per-candidate NaN/Inf checks are not healthy-path behavior unless #2044 admits
   the measured rare/error path with benchmark evidence.

If any required current-format state is missing, stale, corrupt, not
`mmap_direct`, or not certifiable, the current-format path MUST fail closed with
an unavailable/rebuild-needed/error status. Compatibility fallback is allowed
only for explicitly legacy pre-alpha assets, must be counted, and must not be
used as healthy-path evidence.

## Current graph-search admission/readiness matrix

| Graph-search state | Canonical persisted format and owner/state role | Required #2047 tier | Open/searcher-time certification highlights | Prepared runtime shape | Hot-loop boundary | Healthy fallback rule and graph-row prohibition | Required counters, tests, and benchmarks |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Base vectors | Authoritative collection vector field stored as `float32_vector` / `raw_float32_vector` `typed_column_part`; optional vector-index `normalized_vectors` state uses the same logical/encoding pair only when separately admitted. | `mmap_direct` | Index dimensions and row-count identity match the active manifest and TVIS row domain; fixed-width bytes are exactly `rows*dims*4`; 4-byte absolute and pointer alignment; little-endian finite `float32` payloads; non-null/non-default/uncompressed; optional ordinal-to-base-row mapping is certified from `row_refs`. | Row-major `[]float32` plus `dims`, immutable asset identity, and optional ordinal-to-row mapping. | Candidate scoring only: the score loop slices `values[row*dims:(row+1)*dims]` directly. Query validation happens once per search before traversal. | Current-format search MUST NOT fetch candidate vectors from legacy graph rows or a generic adapter fallback. Missing/stale vector state fails closed or requires rebuild; explicit legacy fallback must increment vector fallback counters and is not healthy. | Counters include `vector_mmap_direct/search`, `vector_prepared_direct/search`, `vector_prepared_identity_mapping/search`, `vector_prepared_row_ref_mapping/search`, `vector_heap_copy_typed_view/search`, `vector_scratch_decodes/search`, `typed_column_vector_fallbacks/search`, `vector_certification_failures/search`, and `vector_stale_handles/search`. Tests cover reopen, identity and row-ref mapping, dimension/row-count mismatch, wrong endian/length/alignment, stale handle, finite-value rejection/proof, zero healthy fallback, and `graph_rows=0`. #2037 benchmarks report setup/open/search/doc-fetch boundaries, `ns/op`, `ops/sec`, `B/op`, `allocs/op`, direct/prepared/fallback counters, candidates/search, and score counters. |
| HNSW adjacency | Vector-index state role `adjacency`, one typed-column asset per HNSW layer, `uint32_list` / `raw_uint32_offsets_list`. | `mmap_direct` | TVIS expected layer count, role/asset-id layer, row count, base identity, offsets length `rows+1`, `offsets[0]==0`, monotonic offsets, host-int-bounded final offset, values length, offsets 8-byte alignment, values 4-byte alignment, mappedresource owner/role/section identity, and every neighbor ordinal `< row_count` are certified before traversal using the #2046 graph certifier substrate. | Per-layer prepared CSR view: `[]struct{Offsets []uint64; Values []uint32}` bound to the searcher and manifest identity. | Edge traversal/expansion only: each expanded ordinal reads `Values[Offsets[ordinal]:Offsets[ordinal+1]]` for the requested layer. | Current-format search MUST NOT read legacy graph-row adjacency, legacy `adjacency_list`, graph-specific adjacency-source payloads, or generic typed-list source reads. Missing/corrupt prepared CSR state fails closed; compatibility fallback is legacy-only and counted. | Counters include `adjacency_prepared_csr_mmap_direct/search`, `adjacency_prepared_csr_direct_views/search`, generic fallback counters `adjacency_typed_list_mmap_direct/search`, `adjacency_typed_list_heap_copy_typed_view/search`, `adjacency_typed_list_scratch_decodes/search`, `adjacency_legacy_fallbacks/search`, `adjacency_source_fallbacks/search`, `adjacency_validation_failures/search`, and `adjacency_stale_handles/search`. Tests cover reopen, missing layer, stale TVIS identity, offsets/value corruption, neighbor out-of-bounds, zero generic/legacy fallback on mmap-capable current-format state, and all-layer direct views. #2037 benchmarks report edges/search, visited_edges/search, candidates/search, prepared-CSR and fallback counters, `B/op`, and `allocs/op`. |
| Inverse norms | Vector-index state role `inverse_norm`, `float32` / `raw_float32`. | `mmap_direct` | Row count and manifest identity match TVIS; bytes are exactly `rows*4`; 4-byte absolute and pointer alignment; little-endian finite `float32` values; non-null/non-default/uncompressed; cosine zero-vector policy is certified before scoring. | `[]float32` indexed by graph ordinal, plus immutable identity and metric metadata. | Candidate scoring for cosine only. Norm lookup is a direct index operation adjacent to vector scoring and must not decode per candidate. | Current-format cosine search MUST NOT read inverse norms from legacy graph rows or scratch decode. Missing/stale/corrupt norm state fails closed or rebuilds; any compatibility use is counted and not healthy. | Counters include `norm_mmap_direct/search`, `norm_prepared_direct/search`, `norm_heap_copy_typed_view/search`, `norm_scratch_decodes/search`, `norm_source_fallbacks/search`, `norm_validation_failures/search`, and `norm_stale_handles/search`. Tests cover reopen, row-count/length/alignment failures, stale handle, finite-value proof or rejection, zero healthy fallback, and metric-specific gating. #2037 benchmarks keep norm counters in the search/scoring boundary and report no added `B/op`/`allocs/op`. |
| Row refs | Vector-index state role `row_refs`, multiple `int64` / `raw_int64` assets carrying `DocumentRowRef` coordinates for ordinal-to-base-row mapping. | `mmap_direct` | Every row-ref asset has TVIS/base identity, row count, exact `rows*8` payload, 8-byte alignment, little-endian non-null raw int64 values, and role-specific coordinate bounds for generation, part id, row index, and applied command LSN before result/doc fetch uses them. | Direct row-ref arrays, assembled into row-ref records only at result or document-fetch boundaries. | Result/doc-fetch side channel only. Traversal and scoring may use row refs only to map ordinals to base vector rows when that mapping was prepared before search; document materialization is outside graph traversal timing. | Current-format search MUST NOT scan legacy graph row IDs to map ordinals or fetch documents. `row_ref_vector_source_legacy_graph_ids` and row-ref lookup fallbacks must be zero for healthy current-format graph-search evidence. Missing/stale row-ref state fails closed or disables current-format result/doc materialization until rebuild. | Benchmark labels include `row_ref_state_prepared_views/search`, `row_ref_state_mmap_direct_fields/search`, `row_ref_state_result_refs/search`, `row_ref_state_source_fallbacks/search`, `doc_row_ref_state_fetches/search`, `doc_row_ref_lookup_fallbacks/search`, and `doc_row_ref_validation_failures/search`; search stats also expose `row_ref_vector_source_state` and `row_ref_vector_source_legacy_graph_ids`. Tests cover reopen, duplicate/missing asset roles, coordinate bounds, stale manifest identity, final top-k fetch with row refs, no legacy ID scan, and fail-closed validation. #2037 benchmarks report row-ref counters separately from candidate scoring and document fetch timing. |
| Document IDs | Vector-index state role `document_ids`, `bytes` / `raw_bytes_offsets` with one opaque byte value per graph ordinal. | `mmap_direct` | TVIS/base identity, row count, offsets length `rows+1`, `offsets[0]==0`, monotonic host-int-bounded offsets, final offset matching values byte length, offsets 8-byte alignment, live values bytes, and opaque byte lifetime are certified before result IDs are exposed. | Direct `Offsets []uint64` plus `Bytes []byte`; top-k result IDs are copied or buffer-owned according to the public API boundary. | Final top-k/result materialization only. The traversal and scoring loops must not touch document ID bytes. | Current-format search MUST NOT return IDs from legacy graph rows. `result_id_graph_fallbacks` must be zero for healthy evidence; missing/corrupt document-ID state fails closed or rebuilds. | Benchmark labels include `result_id_prepared_bytes_views/search`, `result_id_typed_bytes_state/search`, `result_id_graph_fallbacks/search`, `result_id_state_validation_failures/search`, and `docs_fetched/search`, plus result-ID byte/output counters. Tests cover reopen, arbitrary binary IDs, missing/corrupt state fail-closed, zero graph fallback, and final top-k-only access. #2037 benchmarks split no-document search from with-documents/result-materialization rows and report `docs_fetched/search`, `B/op`, and `allocs/op`. |

## Graph-row fallback prohibition

For healthy current-format graph-search evidence, all graph-row fallback counters
MUST be zero and graph-row physical assets MUST NOT be the source of vectors,
adjacency, inverse norms, row refs, or returned IDs. #2045 additionally reports
`prepared_graph_search_views/search=1` and `graph_row_fallbacks/search=0` for the
combined prepared route. Benchmark and status output must make this visible with
counters such as:

- `graph_rows=0` and no graph-row `physical_B/search` for search-only typed-column
  runs;
- `adjacency_legacy_fallbacks/search=0` and
  `adjacency_source_fallbacks/search=0`;
- `typed_column_vector_fallbacks/search=0`;
- `row_ref_vector_source_legacy_graph_ids=0`;
- `result_id_graph_fallbacks=0`;
- `prepared_graph_search_views/search=1` and `graph_row_fallbacks/search=0`.

Old fixtures may continue to exercise explicit compatibility readers, but those
runs must be labeled legacy/compatibility, excluded from healthy current-format
promotion evidence, and covered by quarantine/fallback tests.

## Future graph-search type admission gate

No future typed-column logical type, physical type, encoding, or wrapper may
become a healthy current-format graph-search dependency until all of the
following are true:

1. `typed-column-optimized-consumer-capabilities.md` classifies the generic
   logical/physical/encoding pair and any wrappers/owners involved.
2. This document, or a successor #2036-owned role document, adds an admission
   row naming the graph-search state role, owner, manifest field, canonical
   persisted format, certification rules, prepared runtime shape, hot-loop
   boundary, fallback behavior, and graph-row prohibition for the role.
3. #2044 enforcement recognizes the role and fails closed when the documented
   optimized state is absent, stale, corrupt, or below the admitted tier; the
   role/status row lives in `typed-column-graph-search-admission.md`.
4. Counters exist for direct/prepared use, heap/scratch/generic fallback,
   graph-row fallback, validation failures, and stale/lifetime failures.
5. Tests cover reopen, manifest/ref mismatch, corrupt payload, unsupported
   wrappers, direct-view/lifetime failures, zero healthy fallback, and any
   allowed compatibility fallback.
6. #2037 benchmark evidence compares the same fixture, hardware, commands,
   baseline commit/branch, and candidate commit/branch, and reports `ns/op`,
   `ops/sec`, `B/op`, `allocs/op`, memory residency, setup/open/search/doc-fetch
   timing boundaries, direct-view/prepared/fallback/source counters, and any
   domain counters such as candidates/search and edges/search.

Until those conditions are met, the type or role is experimental,
compatibility-only, docs-only, or behind an explicit non-default path. A PR may
propose a weaker tier such as `heap_typed_view` only by updating #2044 admission
rules and proving no unaccepted material regression in allocation, memory
resident bytes, and wall time.

## Benchmark boundary required by #2037

Benchmark reports that claim graph-search readiness or performance must split
these boundaries:

1. **Setup/build/rebuild**: collection load, graph rebuild, typed-column asset
   publication, and TVIS publication.
2. **Open/prepare**: searcher open, manifest discovery, direct-view
   certification, finite-value proof/scan if any, and prepared view construction.
3. **Search**: one query's HNSW traversal, scoring, and top-k over prepared
   runtime views, with no document fetch.
4. **Result ID materialization**: final top-k ID bytes copied or exposed through
   the public response contract.
5. **Document fetch/materialization**: optional retained/typed-storage document
   fetch after top-k.

Every benchmark table must include `ns/op`, `ops/sec`, `B/op`, `allocs/op`, the
commit or branch pair, identical fixture parameters, hardware/context, and the
role counters above. Docs-only PRs may state that no hot path changed and omit
new benchmark runs, but they must preserve this benchmark plan.
