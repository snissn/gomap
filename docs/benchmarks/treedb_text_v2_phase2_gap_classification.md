# TreeDB text-v2 phase-2 scoreboard synthesis (#2834)

This is the phase-2 baseline readout for parent #2833. It synthesizes the
completed #2810/#2809 scoreboard and hardening evidence into a durable target
taxonomy for TreeDB collection text-v2 work. It does **not** claim industry
parity unless a comparable external row is present with pinned corpus, analyzer,
query semantics, storage boundary, and zero-document candidate counters.

## Source artifacts and commands

Primary #2810 scoreboard source:

- artifact root: `/tmp/gomap_2727_scoreboard_candidate_20260613_090425`
- report: `/tmp/gomap_2727_scoreboard_candidate_20260613_090425/scoreboard.md`
- JSON: `/tmp/gomap_2727_scoreboard_candidate_20260613_090425/scoreboard.json`
- command:

```sh
RUN_DIR=/tmp/gomap_2727_scoreboard_candidate_20260613_090425 \
BENCHTIME=1x COUNT=1 RUN_100K=true TEXT_100K_BENCHTIME=1x TEXT_100K_COUNT=1 \
SQLITE_FTS5_QUERIES=1000 \
scripts/bench_text_hybrid_scoreboard.sh
```

Phase-1 follow-up evidence roots from #2809/#2808 closeout:

| Area | Issue / PR | Source artifact root |
| --- | --- | --- |
| scoreboard foundation | #2810 / PR #2735 | `/tmp/gomap_2727_scoreboard_candidate_20260613_090425` |
| posting allocation and rewrite follow-up | #2811 / PR #2738 | `/tmp/gomap_2728_finalizer_bench_20260613_125335`, `/tmp/gomap_2728_rewrite_rerun_20260613_125509` |
| exact multi-term OR/WAND | #2812 / PR #2739 | `/tmp/gomap_2730_candidate_bench_10k_latest_20260613_145109.log`, `/tmp/gomap_2730_candidate_bench_100k_latest_20260613_145115.log` |
| 1M scale harness | #2813 / PR #2740 | `/tmp/gomap_2731_final_20260613_160144`, `/tmp/gomap_2731_scale_1m_candidate65536_20260613_160325/scale_report.md` |
| bounded phrase/analyzer rows | #2815 / PR #2741 | `/tmp/gomap_2733_validation_20260613_154526`, `/tmp/gomap_2733_bench_final_20260613_154513` |
| maintenance policy | #2814 / PR #2742 | `/tmp/gomap_2732_validation_20260613_164404/benchmark_summary.md` |
| hardening | #2808 | merged hardening tests; no benchmark run required because the PR was test-only |

Current capture entry points remain:

```sh
RUN_DIR=/tmp/gomap_text_hybrid_scoreboard_$(date +%Y%m%d_%H%M%S) \
  scripts/bench_text_hybrid_scoreboard.sh

RUN_DIR=/tmp/gomap_text_hybrid_scale_smoke_$(date +%Y%m%d_%H%M%S) \
SMOKE_ROWS=128 SMOKE_QUERIES=3 SMOKE_BATCH_SIZE=64 \
DIMS=4 M=4 EF_CONSTRUCTION=32 EF_SEARCH=32 READERS=2 \
  scripts/bench_text_hybrid_scale.sh
```

The scoreboard parser now emits `phase2_synthesis` in `scoreboard.json` with
schema `treedb_text_v2_phase2_gap_synthesis/v1`, plus a matching Markdown
section in `scoreboard.md`.

## Classification vocabulary

| Class | Meaning |
| --- | --- |
| `ahead` | TreeDB has comparable external evidence and is materially better on the stated boundary. |
| `near_parity` | TreeDB has comparable external evidence and is in the same practical band. |
| `behind_but_tractable` | TreeDB has a feature or harness path, but still lacks comparable rows, scale refresh, counters, or a bounded optimization. |
| `far_behind` | A parity claim is blocked by a major evidence/coverage gap or missing durable metric. This is not an unmeasured latency ratio. |

As of this baseline, no shape is classified `ahead` or `near_parity` for
industry parity because Lucene, Tantivy, and Bleve rows are unavailable and the
SQLite FTS5 row is only an embedded text baseline with non-equivalent analyzer
and query semantics.

## External baseline coverage

| Baseline | Current status | Notes |
| --- | --- | --- |
| SQLite FTS5 | available in #2810 smoke | Useful embedded rowid + `bm25()` baseline. Not a Lucene/Tantivy/Bleve semantics match and not a hybrid engine by itself. |
| Bleve | unavailable / not captured | Requires a pinned Go harness with the same synthetic corpus, analyzer, explicit query operators, top-k, and result-order checks. |
| Tantivy | unavailable / not captured | Requires a pinned Rust harness with the same corpus, analyzer/tokenizer, query operators, top-k, build/storage boundary, and result-order checks. |
| Lucene | unavailable / not captured | Requires pinned Lucene/JMH or OpenSearch evidence with identical corpus/query contract before any Lucene parity claim. |

## Phase-2 gap classification table

| Priority | Target ID | Shape | Classification | Current TreeDB evidence | External evidence | Required follow-up rows |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | `index_size` | index size | `far_behind` | Text-v2 storage exists in B-tree-native roots and persistent value-log values, but #2810 does not publish durable text-v2 bytes/doc with WAL excluded. | SQLite FTS5 storage exists; Lucene/Tantivy/Bleve unavailable. | TreeDB durable text-v2 bytes/doc after checkpoint and rewrite; external index bytes/doc after equivalent optimize/vacuum; separate primary payload, WAL, value-log, and text roots. |
| 2 | `single_term_common` | single-term common | `behind_but_tractable` | #2810/#2809 include exact block-max common-term rows with zero document fetch. | No comparable Lucene/Tantivy/Bleve common-term row; SQLite row is not enough for parity. | 10k/100k/1M TreeDB `blockmax_common_topk`; matching SQLite/Lucene/Tantivy/Bleve common-term top-k with analyzer notes. |
| 3 | `multi_term_or_wand` | multi-term OR/WAND | `behind_but_tractable` | #2812 landed exact OR/WAND block-max evidence. | External OR/WAND rows unavailable. | TreeDB `or_common`, `or_common_rare`, and `or_high_frequency` blockmax/exhaustive rows; Lucene/Tantivy/Bleve explicit OR rows; scalar-aware pruning counters for #2836. |
| 4 | `hybrid_text_scalar` | hybrid text+scalar | `behind_but_tractable` | #2810 has zero-document hybrid/scalar rows and scalar-prefilter counters. | SQLite FTS5 is not a hybrid text+scalar comparator; Lucene/Tantivy/Bleve filter semantics not captured. | Rare and broad scalar selectivity rows with `scalar_prefilter_ids/search`, `text_candidates/search`, block skip counters, and explain output once #2838 lands. |
| 5 | `index_build_ingest` | index build/ingest | `behind_but_tractable` | Build/backfill/write rows exist across #2810/#2813, but text-only ingest and bytes/doc are not yet the scoreboard headline. | SQLite FTS5 build seconds exist; Lucene/Tantivy/Bleve build rows unavailable. | Text-v2 CreateTextIndex/backfill, InsertBatch, update/delete, write amp, and external build rows with checkpoint/optimize boundary. |
| 6 | `phrase` | phrase/proximity | `behind_but_tractable` | #2815 landed bounded structured phrase/proximity over `StorePositions=true`. | Lucene/Tantivy/Bleve phrase/proximity rows unavailable and have broader query/analyzer semantics. | Exact phrase and slop rows at 10k/100k with position counters; Lucene/Tantivy/Bleve phrase rows with stopword/slop notes. |
| 7 | `reopen` | reopen/recovery | `behind_but_tractable` | #2813 scale harness includes close/open/probe rows. | External open-reader/reopen rows unavailable. | 10k/1M/10M close/open/probe rows, post-reopen zero-doc search, and external open-reader rows where feasible. |
| 8 | `maintenance_rewrite` | maintenance/rewrite | `behind_but_tractable` | #2814 landed bounded logical rewrite/maintenance through normal TreeDB maintenance. | External optimize/merge comparators unavailable. | Rewrite debt, duration, stale posting purge, post-rewrite search, and physical reclamation rows at scale. |
| 9 | `multi_term_and` | multi-term AND | `behind_but_tractable` | Exact AND serving exists and is in the scale harness target set. | No external explicit AND rows captured. | TreeDB AND score-only rows at 10k/100k/1M plus matching SQLite/Lucene/Tantivy/Bleve explicit AND rows. |
| 10 | `single_term_rare` | single-term rare | `behind_but_tractable` | Rare-term rows are defined by the scale harness, but not represented in the #2810 headline table. | No external rare-term rows captured. | Rare-term 10k/100k/1M rows with allocation/decode counters and matching external rare-term rows. |

## Recommended phase-2 order

1. **#2835 footprint first**: publish `index_size` and build/ingest bytes/doc
   rows, then compress posting/index footprint without weakening exact BM25F.
2. **#2835 common-term hot path**: use the same rows to reduce allocations,
   cache churn, and high-document-frequency block overhead.
3. **#2836 OR/WAND + scalar pruning**: extend exact block-max evidence with
   scalar-aware pruning counters and broad/rare scalar selectivity rows.
4. **#2838 explain/observability**: make every gap row explainable, including
   terms, blocks visited/skipped, scalar pruning, fail-closed reasons, and score
   components. See `docs/benchmarks/treedb_text_v2_query_explain.md` for the
   query-explain runbook and benchmark gate.
5. **#2837 scale refresh**: refresh 1M/10M, reopen, maintenance, and tail rows
   after footprint/scalar-pruning changes land.
6. **#2839 analyzer/relevance expansion**: keep phrase/analyzer features
   bounded and fail-closed; add quality labels only after the benchmark semantics
   are pinned.
7. **#2840 hardening**: close with fuzz/crash/race coverage over the expanded
   phase-2 feature set.

## Non-equivalent analyzer/query semantics notes

- TreeDB text-v2 uses persisted collection analyzer options and BM25F field
  weights; external rows must document analyzer/tokenizer, field weights, query
  operator, top-k, and tie ordering.
- SQLite FTS5 `MATCH 'refund policy'` plus `bm25()` is not automatically the
  same as TreeDB default BM25F OR, explicit AND, or phrase semantics.
- Lucene/Tantivy/Bleve phrase, slop, stemming, synonyms, stopwords, and query DSL
  support are broader than TreeDB's bounded structured phrase/proximity API.
- Candidate-generation rows must keep full-document fetch counters at zero.
  Rows that fetch final documents are valid only as separate final-fetch rows.
- Storage rows must say whether WAL is excluded and whether optimize/vacuum,
  checkpoint, TreeDB value-log rewrite, and normal storage compaction have run.
