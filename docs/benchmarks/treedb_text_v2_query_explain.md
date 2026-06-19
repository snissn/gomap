# TreeDB text-v2 query explain runbook (#2838)

TreeDB text-v2 search now has an opt-in explain payload for diagnostics and
benchmark runbooks. Normal `SearchText` calls leave explain disabled; the
response `Explain` field is `nil`, and score-component materialization is not
performed.

## Enabling explain

```go
resp, err := col.SearchText(collections.TextSearchOptions{
    IndexName:  "lexical",
    Query:      "refund policy",
    Operator:   collections.TextSearchOperatorOR,
    TopK:       10,
    ResultMode: collections.TextSearchResultModeScoreOnly,
    Explain:    true,
})
if err != nil {
    // resp.Stats.FailClosedReason and resp.Explain.FailClosedReason carry the
    // low-cardinality fail-closed reason when available.
}
fmt.Printf("path=%s terms=%v blocks=%d/%d\n",
    resp.Explain.Serving.Path,
    resp.Explain.Terms,
    resp.Explain.Serving.PostingBlocksVisited,
    resp.Explain.Serving.PostingBlocksSkipped,
)
```

The explain shape is intentionally bounded and stable enough for diagnostics:

- analyzed terms plus per-term document frequency, total term frequency, and
  posting-block count;
- text-v2 snapshot/status identity (`root_generation`, `stats_generation`,
  `term_generation`, `live_documents`, active root names, etc.);
- selected serving path:
  - `blockmax_single_term`
  - `blockmax_and`
  - `blockmax_or_wand`
  - `exact_postings_scan`
  - `phrase_validation`
  - `fail_closed`
- block-max and WAND counters (`posting_blocks_visited`,
  `posting_blocks_skipped`, `block_max_thresholds`, `wand_pivots`);
- scalar allow-set pruning hooks used by hybrid text+scalar serving
  (`text_scalar_prefilter_ids`, scalar skipped blocks, scalar rejected postings);
- phrase validation counters (`position_lookups`, candidates checked/matched);
- BM25F score components for returned top-K results only.

## Fail-closed examples

Unsupported bounded phrase search without `StorePositions`:

```go
resp, err := col.SearchText(collections.TextSearchOptions{
    IndexName: "lexical",
    Phrase:   &collections.TextSearchPhraseQuery{Query: "refund policy"},
    TopK:     10,
    Explain:  true,
})
// err wraps collections.ErrTextIndexUnavailable.
// resp.Stats.FailClosedReason == "unsupported_text_query"
// resp.Explain.FailClosedReason == "unsupported_text_query"
```

Budget exhaustion:

```go
resp, err := col.SearchText(collections.TextSearchOptions{
    IndexName:      "lexical",
    Query:          "refund",
    TopK:           10,
    CandidateLimit: 1,
    Explain:        true,
})
// resp.Explain.FailClosedReason == "candidate_limit_exceeded"
```

## Interpreting score components

Each returned explain result contains term-level BM25F components. The sum of
`result.terms[*].score` equals the returned result score within floating-point
rounding. Field components expose term frequency, field length, field average
length, normalized TF, and weighted TF. Explain computes these components only
when requested and only for returned top-K results.

## Scalar-aware pruning contract for #2836

Hybrid text+scalar serving passes a snapshot-bound scalar allow-set into text-v2
candidate generation. The explain contract exposes:

- `Serving.ScalarPruning.Enabled` and `AllowSetSize`;
- `Stats.TextScalarPrefilterIDs` / `HybridSearchStats.TextScalarPrefilterIDs`;
- scalar-pruned posting blocks and rejected postings.

Issue `#2836` can extend these counters with scalar-aware WAND decisions without
changing the public query DSL or ranking semantics.

## Analyzer/relevance contract for #2839

Explain reports analyzer-normalized terms and phrase gaps. #2839 can add
bounded analyzer/relevance diagnostics by extending the explain payload while
keeping unsupported stemming/synonym or phrase states fail-closed with explicit
reasons.

## Focused validation and benchmark commands

```sh
go test ./TreeDB/collections -run 'TestTextV2QueryExplain'
go test ./TreeDB/collections -run '^$' -bench 'BenchmarkTextV2QueryExplain2838' -benchmem -count=5
```

Use the `no_explain` benchmark row as the normal-search disabled-overhead gate
and the `explain` / `phrase_explain` rows as the allocation/diagnostic overhead
rows.
