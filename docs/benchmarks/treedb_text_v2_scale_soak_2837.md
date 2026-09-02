# TreeDB text-v2 phase-2 scale/soak refresh (#2837)

This note records the phase-2 scale/soak status for parent #2833 after the
footprint (#2835), scalar-aware pruning (#2836), and explain/analyzer work in
the stacked branch.

## Status

The checked-in scale harness is now validated beyond the original #2837 10k
smoke: a 1M local scale run completed on `main` at
`7f0e68d9c46afb9d54152d8bc558ebcdc89fd468` and is summarized below. A 10M
selected run was started with the documented approval-gated command at
`/tmp/gomap_text_hybrid_scale_10m_20260619_085923`; do not cite the 10M run as
passing evidence until its `scale_report.md` exists and the guardrails are
checked.

The 1M run is scale evidence, not an industry-parity claim: the corpus is
synthetic, the host was an interactive laptop, and external Lucene/Tantivy/Bleve
comparison rows are still unavailable.

## 10k smoke artifact

Artifact root: `/tmp/gomap_issue_2837_scale_smoke_20260618_200105`

Command:

```sh
GOWORK=off TREEDB_TEXT_V2_SCALE_DOCS=10000 TREEDB_TEXT_V2_SCALE_QUERIES=64 \
  go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkTextV2ContractSearchScale2623/docs_10000/(score_only_common_no_docs|multi_term_or_no_docs|multi_term_and_no_docs)$' \
  -benchmem -benchtime=3x -count=1
```

| row | ns/op | B/op | allocs/op | docs fetched | fail closed | candidates scored | postings scanned |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| score_only_common_no_docs | 15,085,012 | 5,686,114 | 30,120 | 0 | 0 | 10,000 | 10,000 |
| multi_term_or_no_docs | 19,410,268 | 5,926,320 | 40,128 | 0 | 0 | 10,000 | 20,000 |
| multi_term_and_no_docs | 18,427,475 | 5,926,338 | 40,129 | 0 | 0 | 10,000 | 20,000 |

## 1M local scale artifact

Artifact root: `/tmp/gomap_text_hybrid_scale_1m_20260619_085339`

Command:

```sh
RUN_DIR=/tmp/gomap_text_hybrid_scale_1m_20260619_085339 \
RUN_1M=true RUN_SMOKE=false \
ONE_M_ROWS=1000000 ONE_M_QUERIES=25 ONE_M_BATCH_SIZE=16384 \
ONE_M_BACKFILL_ROWS=100000 \
ONE_M_MAINTENANCE_UPDATES=10000 ONE_M_MAINTENANCE_DELETES=5000 \
ONE_M_CANDIDATE_LIMIT=65536 \
DIMS=16 M=8 EF_CONSTRUCTION=128 EF_SEARCH=128 \
TOP_K=10 READERS=4 \
scripts/bench_text_hybrid_scale.sh
```

Host/context: Apple M3, 8 CPUs, `go1.26.0 darwin/arm64`, branch `main`, commit
`7f0e68d9c46afb9d54152d8bc558ebcdc89fd468`.

### 1M load/storage

| phase | seconds | rows/s | storage bytes | bytes/doc | text bytes/doc | vector native bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| load | 73.187 | 13,663.6 | 1,087,374,509 | 1,087.4 | 109.5 | 594,042,904 |
| backfill fixture | 0.793 | 126,128.7 | 64,225,653 | 642.3 | 96.3 | 0 |

Load breakdown: generation `1.121s`, insert `10.474s`, vector rebuild
`60.077s`, checkpoint `0.294s`. Vector rebuild dominated this mixed
text/vector scale row, so text-only ingest conclusions should use a text-only
companion benchmark.

Text-v2 lane bytes/doc after load: docid `21.0`, docmap `19.1`, postings
`62.2`, norms `7.1`, positions `0.0`, terms `0.0`, status/format `0.0`.

### 1M retrieval latency

| row | p50 | p95 | p99 | mean | ops/sec | guardrail | key counters |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| text_common_score_only | 579.960ms | 665.410ms | 672.957ms | 591.458ms | 1.7 | PASS | docs_fetched=0, fail_closed=0, postings=500,000, blocks_visited=15,625, scored=500,000 |
| text_rare_score_only | 6.639ms | 7.960ms | 7.965ms | 6.954ms | 143.8 | PASS | docs_fetched=0, fail_closed=0, postings=1,004, blocks_visited=62, scored=1,004 |
| text_multi_term_and_score_only | 817.554ms | 1.155s | 1.453s | 870.334ms | 1.1 | PASS | docs_fetched=0, fail_closed=0, postings=1,000,000, blocks_visited=31,250, scored=500,000 |
| text_multi_term_or_score_only | 742.687ms | 954.785ms | 1.129s | 758.433ms | 1.3 | PASS | docs_fetched=0, fail_closed=0, postings=1,000,000, blocks_visited=31,250, scored=500,000 |
| hybrid_text_only_no_docs | 757.478ms | 901.653ms | 914.127ms | 773.759ms | 1.3 | PASS | docs_fetched=0, fail_closed=0, text_candidates=65,536 |
| hybrid_text_scalar_no_docs | 217.200ms | 236.491ms | 242.767ms | 218.762ms | 4.6 | PASS | docs_fetched=0, fail_closed=0, text_candidates=62,500, scalar_prefilter=62,500 |
| hybrid_text_vector_no_docs | 1.561s | 1.831s | 1.849s | 1.595s | 0.6 | PASS | docs_fetched=0, fail_closed=0, text_candidates=65,536, vector_candidates=65,536 |
| hybrid_text_vector_scalar_no_docs | 1.077s | 1.796s | 2.034s | 1.199s | 0.8 | PASS | docs_fetched=0, fail_closed=0, text_candidates=62,500, vector_candidates=65,536 |

Reopen: close `0.005s`, open `1.577s`, open collection `0.001s`, probe
`3.377s`, total `4.962s`.

Concurrent serving/write sanity: readers `4`, queries `25`, writes `1024`,
throughput `2.7 ops/s`, p50 `1.336s`, p95 `1.840s`, p99 `1.843s`, writer
`0.129s`, guardrail PASS.

Maintenance/rewrite: updates `10,000` in `2.164s`, deletes `5,000` in `0.061s`,
rewrite `4.996s`, checkpoint `0.121s`; read `350,369` blocks, wrote `58,724`,
deleted `350,369`, purged `112,516` stale postings; postcondition PASS.

### 1M interpretation

The guardrails passed across all reported rows, so the run is valid
zero-document candidate-generation evidence. The main performance warning is
that high-document-frequency text rows still score very large candidate sets at
1M (`500k` scored for common and OR/AND rows). Rare-term latency is much better,
and scalar filtering materially reduces the text-only hybrid row, but high-DF
common/multi-term serving remains the primary scale optimization target.

## Full-scale reproduction plan

Use a dedicated long-running shell/tmux session and capture artifacts outside
this interactive PR loop:

```sh
RUN_DIR=/tmp/gomap_issue_2837_scale_10m_$(date +%Y%m%d_%H%M%S)
mkdir -p "$RUN_DIR"
TREEDB_TEXT_V2_SCALE_DOCS=10000000 \
TREEDB_TEXT_V2_SCALE_QUERIES=10000 \
GOWORK=off go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkTextV2ContractSearchScale2623/docs_10000000/' \
  -benchmem -benchtime=1x -count=1 | tee "$RUN_DIR/search_scale_10m.txt"
```

Recommended companion captures:

- ingest/build benchmark with storage bytes/doc;
- reopen/search sanity after checkpoint;
- phrase/analyzer rows when `StorePositions=true`;
- scalar-selectivity sweep from #2836;
- heap profile and `/usr/bin/time -v` high-water RSS.

## Risk notes

- This note does not claim 10M+ parity until a completed 10M `scale_report.md`
  is linked and reviewed.
- The 1M run confirms no-document-fetch and fail-closed guardrails for the
  reported synthetic scale shapes.
- Any future claim about 10M/50M behavior should link the full artifact root and
  update this note with p50/p95/p99, storage growth, rewrite debt, and memory
  high-water data.
