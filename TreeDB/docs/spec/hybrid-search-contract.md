# TreeDB Hybrid Search Contract (#2502)

Status: design gate for the #2501 hybrid lexical + vector search stack. TreeDB
is pre-alpha, so this API/contract may change, but downstream work should treat
this note as the current source of truth until a later PR updates it.

Parent tracker: <https://github.com/snissn/gomap/issues/2501>.

## Scope and non-goals

This contract defines the first shared TreeDB collection vocabulary for a
bounded hybrid retrieval pipeline:

```text
scalar/metadata filters
+ ranked lexical candidates
+ vector candidates
+ deterministic rank fusion
+ bounded final document fetch
```

It does **not** implement the text index (#1764), vector optimizations
(#2490/#2475 lanes), gateway syntax, or the hybrid executor (#2505). Normal
hybrid search MUST NOT scan or fetch every document as a fallback.

Text-only and vector-only APIs/benchmarks remain separate evidence lanes:
#1764 owns indexed lexical `SearchText`/BM25/BM25F behavior, and the existing
vector API/evidence owns vector-only search. Hybrid benchmarks must measure the
combined executor separately from those single-source paths.

## Public naming direction

The collection package now reserves these names for downstream implementation:

- entry point: `Collection.SearchHybrid(HybridSearchOptions)`;
- response/result types: `HybridSearchResponse`, `HybridSearchResult`;
- shared candidate type: `HybridSearchCandidate`;
- source contribution type: `HybridSourceContribution`;
- stats/counter type: `HybridSearchStats`;
- planner/debug metadata: `HybridSearchPlan`, `HybridSearchSnapshot`;
- fail-closed errors: `ErrHybridSearchUnsupported`,
  `ErrHybridSearchIndexUnavailable`, and `ErrHybridSearchStaleIndex`.

`SearchHybrid` is only a fail-closed stub until #2505 implements the executor.
It exists so #1764/#2503/#2504/#2505 have stable names to target without
inventing incompatible seams.

## Query contract

`HybridSearchOptions` is the public contract surface:

- `TopK`: final fused result count. Final document fetches are bounded by this
  value.
- `Text *HybridTextQuery`: lexical candidate source.
  - `IndexName`: text index name.
  - `Query`: text-query string. Grammar/analyzer semantics are owned by #1764.
  - `CandidateLimit`: lexical candidate budget before fusion.
- `Vector *HybridVectorQuery`: vector candidate source.
  - `IndexName`, `Query`, `EfSearch`, `QueryMode`, `QuantizedIndexName`, and
    `QuantizedRerankCandidates` mirror the existing vector-index vocabulary.
  - `CandidateLimit` is the vector candidate budget before fusion.
  - No document materialization knobs are present on the vector clause; hybrid
    materialization is a final bounded phase.
- `ScalarFilter *HybridScalarFilter`: bounded scalar-index filter. Equality uses
  `Value`; ranges use `Range *IndexRangeOptions`.
- `ScalarFilterStrategy`: one of `prefilter`, `postfilter`, `text_first`,
  `vector_first`, or `union_fusion`.
- `Fusion`: deterministic fusion options. The first supported method is
  reciprocal-rank fusion (`rrf`). `RRFK=0` means the implementation default of
  `60`.
- `IncludeDocuments` and `DocumentFetchOptions`: apply only after final top-k
  fusion.
- `Consistency`: snapshot binding mode. The zero value means
  `current_snapshot`.
- `Debug.IncludeCandidates`: optional candidate echo. Counters must remain
  available without enabling candidate echo.

Normal product hybrid queries are expected to include both `Text` and `Vector`.
Single-source text-only and vector-only user flows should continue to use their
own APIs and benchmark rows. Candidate adapters from #2503 may still use
`HybridSearchCandidate` as a shared internal shape.

## Candidate and result shape

Every `HybridSearchCandidate` MUST carry:

- `ID []byte`: the stable opaque collection document ID shared by text, vector,
  scalar, and primary storage;
- `Source`: `text` or `vector`;
- `IndexName`: source index name;
- `SourceRank`: one-based rank within that source result list;
- `Score`: source-native score after conversion to the shared orientation;
- `ScoreKind`: `bm25`, `bm25f`, or `vector_similarity`;
- `TextMatches`: matched text fields/terms where the lexical source can provide
  them.

Final `HybridSearchResult` values carry:

- `ID`, final one-based `Rank`, and higher-is-better `FusedScore`;
- one `HybridSourceContribution` per source that contributed to the document;
- optional `Document`/`DocumentFound`, populated only when `IncludeDocuments` is
  true and only after top-k fusion.

Candidate-generation paths MUST produce response-owned/caller-safe document IDs
at the API boundary. They MUST NOT require fetching full documents to discover
IDs during normal candidate generation.

## Score orientation and rank semantics

All shared candidate scores use **higher-is-better** orientation:

- BM25/BM25F scores are already higher-is-better.
- Vector adapters MUST convert lower-is-better distances into a
  higher-is-better similarity before creating a `HybridSearchCandidate`.
- Existing vector-index cosine scores are treated as `vector_similarity` when
  they are already returned in descending score order.

`SourceRank` is one-based, assigned after each source applies its own stable
source ordering and tie policy. Rank fusion uses ranks, not raw BM25/vector
score normalization, for the default method. Raw scores are retained only for
attribution, debugging, reranking seams, and future fusion methods.

## Fusion contract

The first default fusion method is reciprocal-rank fusion:

```text
rrf_contribution = source_weight / (RRFK + SourceRank)
fused_score = sum(rrf_contribution for every contributing source)
```

Initial source weights are `1.0` for text and `1.0` for vector unless a later
PR extends `HybridFusionOptions` with explicit weighting. `RRFK=0` in options
means use `60`.

`HybridFusionTiePolicyScoreBestRankSourceID`
(`fused_score_best_rank_source_order_id`) is the deterministic total order for
#2504 to implement:

1. higher `FusedScore` first;
2. lower best contributing `SourceRank` first;
3. higher contributing-source count first;
4. source-order tie breaker using `Fusion.SourceOrder`, defaulting to
   `[text, vector]`;
5. lexicographic opaque document ID bytes ascending.

Fusion MUST deduplicate candidates by exact document ID bytes. A document that
appears in both sources has one final result with two source contributions.

## Scalar filter strategy vocabulary

`HybridScalarFilterStrategy` values are planner vocabulary and counter labels:

- `prefilter`: use a scalar index to build a bounded allow-set before text/vector
  candidate generation. If the filter cannot be served by a scalar index, fail
  closed; do not scan primary documents.
- `postfilter`: generate/fuse candidates under explicit budgets, then apply the
  scalar predicate to candidate IDs or snapshot-bound scalar state. It may return
  fewer than `TopK` and report truncation/exhaustion; it MUST NOT expand into a
  full-document scan.
- `text_first`: run lexical candidates first, then use their bounded IDs to
  restrict scalar/vector work where the available source APIs support it.
- `vector_first`: run vector candidates first, then use their bounded IDs to
  restrict scalar/text work where the available source APIs support it.
- `union_fusion`: run text and vector candidate generation independently under
  budgets, fuse the union, then apply scalar filtering/final fetch bounds.

The planner records the actual chosen strategy in `HybridSearchPlan` and uses
`HybridSearchStats` counters such as `scalar_prefilter_ids`,
`scalar_postfilter_checks`, `scalar_filter_matched`, and
`scalar_filter_rejected`.

## Snapshot and epoch consistency

A normal hybrid query binds text, vector, scalar, fusion, and final fetch to one
collection snapshot:

- `current_snapshot` flushes buffered collection writes first, acquires the
  current backend snapshot, and opens all candidate/fetch state against that
  snapshot.
- `bound_snapshot` is reserved for future explicit read-view/searcher APIs where
  the caller controls snapshot lifetime.

Text index state, vector index state, scalar roots, and primary document fetch
MUST all match the same catalog/root snapshot or expose source epochs that are
validated against it. If an index is missing, unavailable, stale, corrupt, or
from a different epoch, the query MUST fail closed with the appropriate error and
`HybridSearchStats.FailClosedReason`; it MUST NOT silently use another source,
legacy fallback, or primary-document scan as a substitute.

`HybridSearchSnapshot` reports the snapshot identity (`commit_seq`,
`system_root_page_id`) and source epochs when the implementation can expose them.
Zero epoch fields mean unavailable/not applicable, not proof of freshness.

## Counters and fail-closed behavior

`HybridSearchStats` is the common debug vocabulary for follow-on PRs. Required
counter families:

- text: `text_candidates_requested`, `text_candidates_returned`,
  `text_postings_scanned`, `text_candidates_scored`;
- vector: `vector_candidates_requested`, `vector_candidates_returned`,
  `vector_candidates_examined`, `vector_edges_visited`;
- scalar: `scalar_prefilter_ids`, `scalar_postfilter_checks`,
  `scalar_filter_matched`, `scalar_filter_rejected`;
- fusion/finalization: `candidates_fused`, `candidates_after_fusion`,
  `candidates_after_filter`, `truncated`;
- final fetch: `documents_fetched`, `documents_missing`;
- guardrails: `full_document_scan_fallbacks`, `fail_closed`, and
  `fail_closed_reason`.

For normal hybrid queries, `full_document_scan_fallbacks` MUST remain zero and
`documents_fetched` MUST be bounded by final `TopK`. Missing or unsupported
source paths report fail-closed reasons such as `text_index_unavailable`,
`vector_index_unavailable`, `text_index_stale`, `vector_index_stale`,
`scalar_filter_unbounded`, `snapshot_mismatch`,
`document_fetch_unavailable`, or `full_document_scan_forbidden`.

## Dependencies on #1764 text-search milestones

#1764 remains the owner of text-only indexed lexical search. Hybrid work depends
on #1764 exposing, by or after its M4/M6 seams:

- persistent text index metadata/analyzers/postings/state/stats;
- bounded ranked text candidate generation without scanning all documents;
- BM25 or BM25F score output with higher-is-better orientation;
- stable opaque collection document IDs;
- one-based lexical rank;
- source index name;
- matched fields/terms where available;
- counters for postings scanned, candidates scored, documents fetched,
  truncation, unavailable/fallback/fail-closed status;
- document fetch only for final text-only results, with a candidate-only seam for
  hybrid adapters.

#1764 should not invent alternative hybrid candidate/result/counter names. It
may keep text-specific public types, but the candidate-only handoff to #2503
should convert or directly emit `HybridSearchCandidate`-compatible data.

## Handoff to #2503 and #2504

#2503 candidate-only paths should consume this contract as follows:

- text candidate adapters return `HybridSearchCandidate` with `Source=text`,
  `ScoreKind=bm25` or `bm25f`, and zero full-document fetches;
- vector candidate adapters return `HybridSearchCandidate` with
  `Source=vector`, `ScoreKind=vector_similarity`, and zero full-document fetches;
- both adapters preserve stable document ID bytes, one-based source rank,
  source/index name, and source counters.

#2504 fusion primitives should implement the RRF formula and deterministic tie
policy above over slices of `HybridSearchCandidate`, returning fused result or
source-contribution values without opening storage, fetching documents, or
consulting text/vector indexes.

#2505 owns the executor that binds scalar filtering, source candidate APIs,
fusion, and bounded final document fetch under the snapshot/epoch contract.
