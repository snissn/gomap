# TreeDB Hybrid Search Contract (#2502)

Status: design contract plus initial #2505 collection executor for the #2501
hybrid lexical + vector search stack. TreeDB is pre-alpha, so this API/contract
may change, but downstream work should treat this note as the current source of
truth until a later PR updates it.

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
(#2490/#2475 lanes), gateway syntax, or reranking beyond deterministic rank
fusion. The initial hybrid executor is implemented by #2505. Normal hybrid
search MUST NOT scan or fetch every document as a fallback.

Text-only and vector-only APIs/benchmarks remain separate evidence lanes:
Issue `#1764` owns indexed lexical `SearchText`/BM25/BM25F behavior, and the existing
vector API/evidence owns vector-only search. Hybrid benchmarks must measure the
combined executor separately from those single-source paths. Issue `#2506`
adds the same-fixture closeout runbook in
`docs/benchmarks/treedb_hybrid_search_runbook.md` and the user-facing guide in
`TreeDB/docs/guides/hybrid-search.md`.

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

`SearchHybrid` now routes through the #2505 bounded executor. Unsupported query
shapes and unavailable sources still fail closed so callers cannot accidentally
depend on a scan-all-documents fallback.

## Query contract

`HybridSearchOptions` is the public contract surface:

- `TopK`: final fused result count. Final document fetches are bounded by this
  value.
- `Text *HybridTextQuery`: lexical candidate source.
  - `IndexName`: text index name.
  - `Query`: text-query string. Grammar/analyzer semantics are owned by #1764.
  - `CandidateLimit`: lexical candidate budget before fusion.
  - `IncludeTextMatches`: optional compact field/term attribution. The zero
    value keeps text candidate generation score-only.
- `Vector *HybridVectorQuery`: vector candidate source.
  - `IndexName`, `Query`, `EfSearch`, `QueryMode`, `QuantizedIndexName`, and
    `QuantizedRerankCandidates` mirror the existing vector-index vocabulary.
  - `CandidateLimit` is the vector candidate budget before fusion.
  - No document materialization knobs are present on the vector clause; hybrid
    materialization is a final bounded phase.
- `ScalarFilter *HybridScalarFilter`: bounded scalar-index filter. Equality uses
  `Value`; ranges use `Range *IndexRangeOptions`. The #2505 executor serves
  these filters only from existing secondary indexes and fails closed when the
  bounded lookup truncates or the index is absent.
- `ScalarFilterStrategy`: one of `prefilter`, `postfilter`, `text_first`,
  `vector_first`, or `union_fusion`.
- `Fusion`: deterministic fusion options. Supported methods are reciprocal-rank
  fusion (`rrf`), weighted reciprocal-rank fusion (`weighted_rrf`), and exact
  per-source min/max normalized score fusion (`normalized_score`). `RRFK=0`
  means the implementation default of `60`; source weights are ignored by plain
  `rrf` and apply only to `weighted_rrf`/`normalized_score`, where zero weights
  default to `1`.
- `ResultMode`: `score_only`, `compact`, or `full`. The zero value preserves the
  legacy behavior: `compact` unless `IncludeDocuments=true`, which selects
  `full`.
- `IncludeDocuments` and `DocumentFetchOptions`: legacy/full-mode controls that
  apply only after final top-k fusion.
- `Consistency`: snapshot binding mode. The zero value means
  `current_snapshot`.
- `Debug.IncludeCandidates`: optional candidate echo. Counters must remain
  available without enabling candidate echo.

Normal product hybrid queries are expected to include both `Text` and `Vector`.
Single-source text-only and vector-only user flows should continue to use their
own APIs and benchmark rows. Candidate adapters from #2503 may still use
`HybridSearchCandidate` as a shared internal shape.

## Bounded budget defaults

`HybridTextQuery.CandidateLimit` is the returned lexical source budget. The text
adapter may use a larger internal postings/scoring guardrail so modest common
terms can still produce an exact top-N candidate list without fetching documents.
That internal guardrail remains finite and fail-closed: if postings or unique
candidate work exceeds the implementation's safe budget, the query returns an
unavailable/unsupported diagnostic rather than a partial ranking or primary scan.

Scalar filters build finite indexed allow-sets. `HybridScalarFilter` preserves
the original one-leaf `{IndexName, Value|Range}` shape and adds one flat ordered
`And` of 2..16 equality or one/two-sided range leaves. Nested conjunctions and
`OR`, `NOT`, `!=`, or membership are unsupported. The document service may
accept nested `AND` in its filter AST, but flattens it, merges same-field bounds,
and emits one collection leaf per declared scalar index in first-appearance
order.

Every leaf uses the existing bounded scalar lookup limit. The executor resolves
all leaves, including those following an empty set, so a missing/corrupt/stale
index or truncation can never be hidden by an earlier empty predicate. Aggregate
retained input is bounded by `lookup_limit * lookup_count`; complete sets are
stable-sorted by cardinality and intersected smallest-first. Any incomplete
lookup or snapshot/root change fails closed with no candidates. A complete empty
intersection succeeds before text/vector work.

v2 text search consumes the final allow-set during posting-block scans so
scalar-filtered candidate generation can score only allowed documents while
preserving the single-snapshot, zero-document-fetch contract. Vector candidate
generation consumes selective bounded allow-sets on a no-document exact score
route only when both the allow-set cardinality is no larger than the configured
vector candidate budget and the vector index row count is within the
implementation's exact allow-set scan budget. Broader allow-sets or larger
vector indexes stay on the existing global vector candidate route plus bounded
ID filtering rather than fetching documents. In that route, scalar-allowed
vector hits outside the global vector candidate budget may be omitted; callers
that need stricter scalar-filtered vector recall must choose a larger vector
candidate budget or wait for an ID-to-vector-ordinal lookup route.

## Candidate and result shape

Every `HybridSearchCandidate` MUST carry:

- `ID []byte`: the stable opaque collection document ID shared by text, vector,
  scalar, and primary storage;
- `Source`: `text` or `vector`;
- `IndexName`: source index name;
- `SourceRank`: one-based rank within that source result list;
- `Score`: source-native score after conversion to the shared orientation;
- `ScoreKind`: `bm25`, `bm25f`, or `vector_similarity`;
- `TextMatches`: matched text fields/terms only when the lexical source was
  explicitly asked to include compact match summaries.

Final `HybridSearchResult` values carry:

- `ID`, final one-based `Rank`, and higher-is-better `FusedScore`;
- one `HybridSourceContribution` per source in `compact` and `full` result
  modes; `score_only` omits source payloads after fusion;
- optional `Document`/`DocumentFound`, populated only in `full` mode (or legacy
  `IncludeDocuments=true`) and only after top-k fusion.

Candidate-generation paths MUST produce response-owned/caller-safe document IDs
at the API boundary. Newly-created collection text indexes use the text-v2
candidate path by default, while explicit v1 indexes remain supported for
compatibility. Neither path may require fetching full documents to discover IDs
during normal candidate generation.

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

The default fusion method is reciprocal-rank fusion:

```text
rrf_contribution = 1 / (RRFK + SourceRank)
fused_score = sum(rrf_contribution for every contributing source)
```

Plain `rrf` ignores `TextWeight` and `VectorWeight`. `weighted_rrf` uses the
same denominator with explicit source weights:

```text
weighted_rrf_contribution = source_weight / (RRFK + SourceRank)
```

Source weights are `1.0` for text and `1.0` for vector when their option fields
are zero for weighted methods.

`normalized_score` is exact over the supplied bounded candidate lists. It builds
one finite min/max native-score range per source, normalizes each source score as
`(score-min)/(max-min)`, uses `1` when all scores for that source are equal, and
sums weighted normalized contributions. It does not estimate unseen candidates or
fetch documents to expand a source list; callers must choose candidate budgets
that cover their desired recall boundary.

`HybridFusionTiePolicyScoreBestRankSourceID`
(`fused_score_best_rank_source_order_id`) is the deterministic total order for
Issue `#2504` to implement:

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

The planner records the actual chosen strategy plus
`scalar_filter_lookup_count`, `scalar_filter_lookup_limit`, and
`scalar_filter_aggregate_limit` in `HybridSearchPlan`. `HybridSearchStats`
reports `scalar_filter_lookups`, `scalar_filter_input_ids`,
`scalar_filter_intersection_steps`, and `scalar_filter_final_ids` in addition to
the compatible `scalar_prefilter_ids`, `scalar_postfilter_checks`,
`scalar_filter_matched`, and `scalar_filter_rejected` counters. A one-leaf
request reports one lookup, zero intersection steps, and unchanged existing
counter semantics. When a selective allow-set is pushed into the vector
candidate adapter, `scalar_filter_rejected` also includes vector rows pruned
before vector scoring; matched/check counters for returned candidates are still
recorded once by the executor's bounded ID filter. No primary-document predicate
scan is implied.

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
- scalar: `scalar_filter_lookups`, `scalar_filter_input_ids`,
  `scalar_filter_intersection_steps`, `scalar_filter_final_ids`,
  `scalar_prefilter_ids`, `scalar_postfilter_checks`,
  `scalar_filter_matched`, `scalar_filter_rejected`;
- fusion/finalization: `candidates_fused`, `candidates_after_fusion`,
  `fusion_text_only`, `fusion_vector_only`, `fusion_both`,
  `fusion_duplicate_candidates`, `candidates_after_filter`, `truncated`;
- candidate-budget policy: `text_candidate_budget_effective`,
  `vector_candidate_budget_effective`, `candidate_budget_policy`,
  `candidate_budget_stop_reason`, `candidate_budget_fallbacks`,
  `candidate_budget_fallback_reason`, and `candidate_budget_iterations`;
- final fetch: `documents_fetched`, `documents_missing`;
- guardrails: `full_document_scan_fallbacks`, `fail_closed`, and
  `fail_closed_reason`.

For normal hybrid queries, `full_document_scan_fallbacks` MUST remain zero and
`documents_fetched` MUST be bounded by final `TopK`. Adaptive candidate budgets
are exact-only: RRF/weighted-RRF queries may request smaller effective source
budgets only when source exhaustion, scalar allow-set emptiness, or strict RRF
upper bounds prove the same final top-k scores/sources as the fixed requested
budgets. Unsupported fusion methods, postfilter scalar shapes, unresolved ties,
or insufficient bounds keep fixed-budget behavior and set the budget fallback
counters instead of dropping hidden candidates. Missing or unsupported
source paths report fail-closed reasons such as `text_index_unavailable`,
`vector_index_unavailable`, `text_index_stale`, `vector_index_stale`,
`scalar_filter_unbounded`, `snapshot_mismatch`,
`document_fetch_unavailable`, or `full_document_scan_forbidden`.

## Dependencies on #1764 text-search milestones

Issue `#1764` remains the owner of text-only indexed lexical search. Hybrid work depends
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

Issue `#1764` should not invent alternative hybrid candidate/result/counter names. It
may keep text-specific public types, but the candidate-only handoff to #2503
should convert or directly emit `HybridSearchCandidate`-compatible data.

## Handoff to #2503 and #2504

Issue `#2503` candidate-only paths should consume this contract as follows:

- text candidate adapters return `HybridSearchCandidate` with `Source=text`,
  `ScoreKind=bm25` or `bm25f`, and zero full-document fetches;
- vector candidate adapters return `HybridSearchCandidate` with
  `Source=vector`, `ScoreKind=vector_similarity`, and zero full-document fetches;
- both adapters preserve stable document ID bytes, one-based source rank,
  source/index name, and source counters.

The #2503 candidate-only source splits expose:

- `Collection.SearchHybridTextCandidates(HybridTextQuery)`, which adapts ranked
  `SearchText` results into text-source `HybridSearchCandidate` values without
  requesting documents and fails closed if text search is unsupported,
  unavailable, corrupt, truncated by bounded generation, or reports document
  materialization/fallback work.
- `Collection.SearchHybridVectorCandidates(HybridVectorQuery)`, which adapts
  vector-index search into vector-source `HybridSearchCandidate` values and
  fails closed if the backing vector path is unavailable or reports document
  materialization.

Both return `HybridCandidateResponse` with shared `HybridSearchStats`. These
splits provide source candidates only; final fusion/planning/document fetch
remain deferred to #2504/#2505.

Issue `#2504` fusion primitives should implement the RRF formula and deterministic tie
policy above over slices of `HybridSearchCandidate`. The helper surface is
`FuseHybridSearchCandidates(candidates, fusion, topK)`, which returns bounded
`HybridSearchResult` values and fusion counters without opening storage,
fetching documents, or consulting text/vector indexes.

Issue `#2505` owns the executor that binds scalar filtering, source candidate APIs,
fusion, and bounded final document fetch under the snapshot/epoch contract. The
executor uses the #2503 text/vector candidate adapters, #2504 RRF fusion,
secondary-index-only scalar filtering, and final document materialization only
after fusion/filter/top-k selection. Scalar equality uses
`FindByIndexValueLimit`; scalar ranges use `FindByIndexRange` with an explicit
executor limit derived from the final top-k and source candidate budgets. Issue
`#4292` extends this path to a flat bounded conjunction: all leaves resolve
completely under unchanged root/commit identity, finite sets intersect
smallest-first, and the final allow-set enters the existing text/vector/fusion
paths. If any scalar lookup truncates, the query fails closed with
`scalar_filter_unbounded`; missing/corrupt indexes retain the typed unavailable
error and snapshot changes report `snapshot_mismatch`.

`prefilter`, `text_first`, `vector_first`, and `union_fusion` are bounded
planning/reporting labels unless a source API can accept an ID restriction; the
executor still builds the scalar allow-set before source generation and applies
it before fusion for non-`postfilter` strategies. Empty allow-sets short-circuit
all strategies, including `postfilter`. `bound_snapshot` remains reserved for a
future explicit read-view/searcher API; the current executor supports
`current_snapshot` and fails closed on root/commit changes observed between
bounded phases.
