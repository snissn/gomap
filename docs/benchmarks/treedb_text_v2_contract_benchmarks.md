# TreeDB Text v2 Contract Benchmarks (#2623)

This runbook is the M0 benchmark contract for the #2622 text-v2 stack. It keeps
current v1 baselines reproducible and defines the rows/counters later v2 PRs
must compare before claiming performance readiness.

Canonical contract: `TreeDB/docs/spec/collection-text-v2-contract.md`.
Continuity runbooks: `docs/benchmarks/treedb_index_insert_search_benchmarks.md`
and `docs/benchmarks/treedb_hybrid_search_runbook.md`.

## Evidence fields required in every table

Include all of the following in PR bodies/issues:

- exact command and artifact root;
- hardware/OS/Go version and load caveat;
- baseline commit/branch and candidate commit/branch;
- dataset shape and measured boundary;
- `ns/op`, derived `ops/sec`, `B/op`, `allocs/op`, and before/after delta;
- domain counters: postings scanned, posting blocks visited/skipped,
  block-max fallbacks, threshold updates, candidates scored, state/norm lookups,
  position lookups, phrase candidates checked/matched, docs fetched, match
  details built, scalar filter selectivity, fail-closed count/reason, write
  amplification, index bytes/doc, and rewrite/merge state when applicable.

Candidate-generation rows must keep `docs_fetched/search=0` and
`full_doc_fallbacks/search=0`. Final-fetch rows must keep
`docs_fetched/search <= topk/search`.

## Host context capture

```sh
OUT=/tmp/gomap_text_v2_m0_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT"
{
  git rev-parse HEAD
  git branch --show-current
  go version
  uname -a
  sysctl -n machdep.cpu.brand_string 2>/dev/null || true
  sysctl -n hw.ncpu 2>/dev/null || true
  uptime
} | tee "$OUT/context.txt"
```

## Existing #2589 / #2564 continuity matrix

Search guardrails:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkIndexInsertSearch2564/(search_text_candidates_no_docs|search_hybrid_no_docs_scalar_filter|search_hybrid_fetch_topk_scalar_filter)$' \
  -benchmem \
  -benchtime=5x \
  -count=3 \
  | tee "$OUT/index_insert_search_guardrail.txt"
```

The #2564 matrix also exposes v2/default-v2 score-only rows. The v2 text rows use the same synthetic #2564 documents and scalar distribution and create the v2 text index after insert so v1/v2 guardrails remain directly comparable; the existing command-WAL/vector guardrail rows remain the vector no-regression lane.

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkIndexInsertSearch2564/(search_text_v2_candidates_no_docs|search_hybrid_v2_no_docs_scalar_filter|search_vector_candidates_no_docs)$' \
  -benchmem \
  -benchtime=5x \
  -count=3 \
  | tee "$OUT/index_insert_search_v2_guardrail.txt"
```

Indexed insert/readiness row:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkIndexInsertSearch2564/indexed_insert_batch_flush_vector_rebuild$' \
  -benchmem \
  -benchtime=5x \
  -count=3 \
  | tee "$OUT/index_insert_readiness.txt"
```

## New isolated text write/backfill/update/delete rows

Benchmark name: `BenchmarkTextV2ContractWritePaths2623`.

Measured boundaries:

- `insert_batch_no_text`: `InsertBatch` on the same JSON fixture without text
  indexes; setup excludes DB/collection creation.
- `insert_batch_text_indexed`: v1 `InsertBatch` with a declared text index;
  setup excludes DB/collection creation.
- `insert_batch_text_v2_indexed`: v2/default-v2 `InsertBatch` after empty-index
  `CreateTextIndex` setup; setup excludes DB/collection/index creation.
- `create_text_index_backfill`: v1 `CreateTextIndex` over already inserted
  primary documents; setup excludes primary insert.
- `create_text_v2_index_backfill`: v2/default-v2 `CreateTextIndex` over already
  inserted primary documents; setup excludes primary insert.
- `update_batch_text_indexed`: v1 `UpdateBatch` replacements with maintained
  text roots; setup excludes initial insert.
- `update_batch_text_v2_indexed`: v2/default-v2 `UpdateBatch` replacements with
  maintained micro-block/docmap/norm roots; setup excludes initial insert.
- `delete_batch_text_indexed`: v1 `DeleteBatch` with maintained text roots;
  setup excludes initial insert.
- `delete_batch_text_v2_indexed`: v2/default-v2 `DeleteBatch` with generation and
  tombstone maintenance; setup excludes initial insert.

Small/default command:

```sh
GOWORK=off \
TREEDB_TEXT_V2_WRITE_DOCS=256 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2ContractWritePaths2623$' \
  -benchmem \
  -benchtime=5x \
  -count=3 \
  | tee "$OUT/text_v2_write_paths.txt"
```

Larger local write-budget row for default-readiness PRs:

```sh
GOWORK=off \
TREEDB_TEXT_V2_WRITE_DOCS=1024 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2ContractWritePaths2623/(insert_batch_text_v2_indexed|create_text_v2_index_backfill|update_batch_text_v2_indexed|delete_batch_text_v2_indexed)$' \
  -benchmem \
  -benchtime=3x \
  -count=3 \
  | tee "$OUT/text_v2_write_paths_1024_v2.txt"
```

Report text write counters:

- `posting_entries/op`, `state_entries/op`, `stats_entries/op` for v1 rows;
- `v2_docid_entries/op`, `v2_docmap_blocks/op`, `v2_posting_blocks/op`,
  `v2_norm_blocks/op`, `v2_position_entries/op`, and `v2_term_stats/op` for v2 rows;
- `posting_blocks/doc`, `high_df_posting_blocks/op`,
  `high_df_posting_blocks/doc`, and `rewritten_blocks/doc` (M3 rewrite count is `0`);
- `index_entries/doc` for live text-root entries after the operation;
- `write_amp_entries/doc` for estimated text-root entry writes emitted by the
  operation. For maintained v2 insert/update/delete rows this is the emitted
  root-delta estimate (docID/docmap/norm/term-status/posting-block deltas plus
  the status generation), not the net post-mutation root size;
- `index_bytes/doc`, plus v2 lane bytes/doc (`v2_docid_bytes/doc`,
  `v2_docmap_bytes/doc`, `v2_posting_bytes/doc`, `v2_norm_bytes/doc`,
  `v2_position_bytes/doc`, `v2_term_bytes/doc`, and
  `v2_status_format_bytes/doc`).

## New text search scale rows

Benchmark name: `BenchmarkTextV2ContractSearchScale2623`.

Default subbenchmarks include `docs_256`, `docs_10000`, and a skipped
`docs_100000` row. Run the >=100k local artifact by setting
`TREEDB_TEXT_V2_RUN_100K=1` or by explicitly setting `TREEDB_TEXT_V2_SEARCH_DOCS`.

Query rows:

- `score_only_common_no_docs`: common term, score-only internal boundary;
- `detailed_common_no_docs`: common term with current match-detail materializer;
- `rare_no_docs`: rare term;
- `multi_term_and_no_docs`: multi-term `AND` query.

Small/default command:

```sh
GOWORK=off \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2ContractSearchScale2623/docs_(256|10000)/' \
  -benchmem \
  -benchtime=3x \
  -count=3 \
  | tee "$OUT/text_v2_search_scale_256_10k.txt"
```

Required >=100k local artifact command:

```sh
GOWORK=off \
TREEDB_TEXT_V2_RUN_100K=1 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2ContractSearchScale2623/docs_100000/' \
  -benchmem \
  -benchtime=1x \
  -count=1 \
  | tee "$OUT/text_v2_search_scale_100k.txt"
```

Report text search counters:

- `postings_scanned/search`;
- `posting_blocks_visited/search` and `posting_blocks_skipped/search` (v1 = 0; M4 v2 exhaustive search reports visited blocks and skipped=0; M5 v2 block-max reports decoded versus skipped blocks);
- `blockmax_fallbacks/search`, `threshold_updates/search`, and `wand_pivots/search` when exposed by focused explain/OR-WAND rows;
- scalar-pruning hooks for hybrid text+scalar serving: `scalar_prefilter_ids/search`, `scalar_posting_blocks_skipped/search`, and `scalar_postings_rejected/search`;
- `candidates_scored/search`;
- `state_lookups/search` and `norm_lookups/search`;
- `match_details/search`;
- `position_lookups/search`, `phrase_candidates_checked/search`, and
  `phrase_candidates_matched/search` for phrase/proximity rows;
- `docs_fetched/search`, `full_doc_fallbacks/search`, `fail_closed/search`.

M4 (#2627) adds a v2 score-only search benchmark with the same corpus
and query cases. It creates a v2 index with `CreateTextIndex`, scans compressed
posting blocks under one snapshot, scores from norm/docmap blocks, and asserts
`state_lookups=0`, `match_details=0`, and `docs_fetched=0`. As of M5 (#2628),
single-term common rows use exact block-max skipping while multi-term rows report
exact exhaustive `blockmax_fallbacks/search`:

```sh
GOWORK=off \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2ScoreSearchScale2627/docs_(256|10000)/' \
  -benchmem \
  -benchtime=3x \
  -count=3 \
  | tee "$OUT/text_v2_score_search_scale_256_10k.txt"
```

## Bounded phrase/proximity and analyzer rows (#2733)

Phrase/proximity uses structured `TextSearchOptions.Phrase` over v2
`StorePositions` indexes. The measured boundary is `SearchText` score-only
serving after the fixture/index is built; candidate generation must keep
`docs_fetched/search=0`, `state_lookups/search=0`, `match_details/search=0`, and
`full_doc_fallbacks/search=0`. Position-lane work is reported separately.

Default 10k local command:

```sh
GOWORK=off \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2PhraseProximity2733/' \
  -benchmem \
  -benchtime=3x \
  -count=3 \
  | tee "$OUT/text_v2_phrase_proximity_10k.txt"
```

Required feasible 100k artifact command:

```sh
GOWORK=off \
TREEDB_TEXT_V2_RUN_100K=1 \
TREEDB_TEXT_V2_PHRASE_DOCS=100000 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2PhraseProximity2733/' \
  -benchmem \
  -benchtime=1x \
  -count=1 \
  | tee "$OUT/text_v2_phrase_proximity_100k.txt"
```

Analyzer-option write overhead uses the same insert boundary as the write-path
rows, comparing the simple analyzer with and without persisted stopwords:

```sh
GOWORK=off \
TREEDB_TEXT_V2_ANALYZER_DOCS=256 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2AnalyzerStopwordsWrite2733/' \
  -benchmem \
  -benchtime=5x \
  -count=3 \
  | tee "$OUT/text_v2_analyzer_stopwords_write.txt"
```

Report `position_lookups/search`, `phrase_candidates_checked/search`,
`phrase_candidates_matched/search`, `v2_position_entries`, `index_bytes/doc`,
`v2_position_bytes/doc`, and the standard runtime/allocation counters. Stemming
and synonym options are reserved extension seams and must fail closed until a
future ticket defines their indexing/query expansion semantics and rebuild
compatibility.

>=100k local artifact:

```sh
GOWORK=off \
TREEDB_TEXT_V2_RUN_100K=1 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2ScoreSearchScale2627/docs_100000/' \
  -benchmem \
  -benchtime=1x \
  -count=1 \
  | tee "$OUT/text_v2_score_search_scale_100k.txt"
```

## M5 block-max common-term rows

Benchmark name: `BenchmarkTextV2BlockMaxCommonTerm2628`.

This row runs the M5 exact single-term common top-K path against the v2
fixture and includes an in-process `exhaustive_common_topk` comparison with
block-max disabled. The measured boundary is `SearchText` score-only candidate
generation: no full documents, no v1 text-state lookups, and no match-detail
builds.

```sh
GOWORK=off \
TREEDB_TEXT_V2_BLOCKMAX_DOCS=10000 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2BlockMaxCommonTerm2628/(blockmax_common_topk|exhaustive_common_topk)$' \
  -benchmem \
  -benchtime=5x \
  -count=3 \
  | tee "$OUT/text_v2_blockmax_common_10k.txt"
```

Required >=100k local artifact command for #2628:

```sh
GOWORK=off \
TREEDB_TEXT_V2_BLOCKMAX_DOCS=100000 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2BlockMaxCommonTerm2628/(blockmax_common_topk|exhaustive_common_topk)$' \
  -benchmem \
  -benchtime=1x \
  -count=1 \
  | tee "$OUT/text_v2_blockmax_common_100k.txt"
```

Report the same text counters as the search scale rows, especially
`postings_scanned/search`, `posting_blocks_visited/search`,
`posting_blocks_skipped/search`, `candidates_scored/search`,
`threshold_updates/search`, and `blockmax_fallbacks/search`.

## M6 lazy details rows

Benchmark name: `BenchmarkTextV2LazyDetails2629`.

This row compares the exact same explicit-v2 common-term fixture in score-only
mode and detailed mode. The score-only subbenchmark must keep
`match_details/search=0`, `docs_fetched/search=0`, and `state_lookups/search=0`.
The detailed subbenchmark must keep `docs_fetched/search=0` and
`state_lookups/search=0`, with `match_details/search == topk/search` (or the
returned result count when fewer than top-K results exist), proving detail work is
bounded to final results rather than all scored candidates.

```sh
GOWORK=off \
TREEDB_TEXT_V2_LAZY_DETAILS_DOCS=10000 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2LazyDetails2629/(score_only|detailed_topk)$' \
  -benchmem \
  -benchtime=5x \
  -count=3 \
  | tee "$OUT/text_v2_lazy_details_10k.txt"
```

Optional larger local artifact:

```sh
GOWORK=off \
TREEDB_TEXT_V2_LAZY_DETAILS_DOCS=100000 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2LazyDetails2629/(score_only|detailed_topk)$' \
  -benchmem \
  -benchtime=1x \
  -count=1 \
  | tee "$OUT/text_v2_lazy_details_100k.txt"
```

## Concurrent serving/load row

Benchmark names: `BenchmarkTextV2ContractConcurrentServing2623` for the M0
contract row and `BenchmarkTextV2BlockMaxConcurrentServing2628` for the M5 v2
block-max serving row.

The rows serve warmed candidate-generation calls concurrently and report
p50/p95/p99 latency, steady-state heap, reader concurrency, and whether mixed
write/snapshot churn was enabled. M0's default row uses read-only snapshot churn
`0`; later implementation PRs should add a mixed write/snapshot-churn variant
when their mutation lifecycle is in scope.

```sh
GOWORK=off \
TREEDB_TEXT_V2_CONCURRENT_DOCS=10000 \
TREEDB_TEXT_V2_CONCURRENT_READERS=8 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2ContractConcurrentServing2623$' \
  -benchmem \
  -benchtime=200x \
  -count=3 \
  | tee "$OUT/text_v2_concurrent_serving.txt"
```

M5 v2 block-max concurrent row:

```sh
GOWORK=off \
TREEDB_TEXT_V2_BLOCKMAX_CONCURRENT_DOCS=10000 \
TREEDB_TEXT_V2_BLOCKMAX_CONCURRENT_READERS=8 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2BlockMaxConcurrentServing2628$' \
  -benchmem \
  -benchtime=200x \
  -count=3 \
  | tee "$OUT/text_v2_blockmax_concurrent_serving.txt"
```

Counters include `p50_ns/search`, `p95_ns/search`, `p99_ns/search`,
`steady_heap_bytes`, `readers`, `cache_warm`, and
`mixed_write_snapshot_churn` where the row exposes it. The M5 row must also show
non-zero `posting_blocks_skipped/search` in the warm stats or fail the benchmark.

## M7 rewrite/merge lifecycle row

Benchmark name: `BenchmarkTextV2RewriteMerge2630`.

This row creates a v2 text index, applies updates and deletes to create
micro blocks, stale generations, and deleted-document tombstones, then measures
only `Collection.RewriteTextIndex`. Setup, primary writes, index creation,
updates, and deletes are outside the timed boundary. The row reports rewrite
maintenance counters so PR evidence can show logical compaction overhead and
block/tombstone reclamation candidates before normal TreeDB physical
maintenance.

```sh
GOWORK=off \
TREEDB_TEXT_V2_REWRITE_DOCS=512 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkTextV2RewriteMerge2630$' \
  -benchmem \
  -benchtime=5x \
  -count=3 \
  | tee "$OUT/text_v2_rewrite_merge.txt"
```

Report:

- `posting_blocks_read/op`, `posting_blocks_written/op`, and
  `posting_blocks_deleted/op`;
- `stale_postings_purged/op` and `tombstones_purged/op`;
- post-rewrite search rows proving no hidden latency/allocation regression;
- `TextIndexStorageStats` before/after state: `rewrite_merge_pending` should
  become `compacted`, with micro/delta blocks and deleted-document tombstones
  purged from the live root set;
- storage maintenance evidence showing live posting/norm/docmap/positions values
  survive `ValueLogGC`/`CompactStorage`, while old root payloads are unreachable
  after snapshots release.

Maintenance-budget interpretation for default-readiness PRs:

- `TextIndexStorageStats` is a scanning validation/audit tool. Use it in
  maintenance, tests, and benchmark setup/teardown; do not place it in serving
  health paths.
- Treat `rewrite_merge_pending` as logical maintenance debt. Schedule
  `RewriteTextIndex` when micro/delta posting blocks, deleted ordinals,
  `posting_blocks/doc`, or `write_amp_entries/doc` materially drift above the
  fresh backfill/write rows for the workload.
- `RewriteTextIndex` is logical root maintenance only. Physical reclamation must
  remain a normal TreeDB sequence after old snapshots release: checkpoint,
  `ValueLogGC`, value-log rewrite when the rewrite plan shows stale source
  bytes, leaf maintenance/index vacuum, or `CompactStorage`.

Default selection after #2690 is v2 for newly-created TreeDB collection text indexes. PR evidence should state when a row uses the explicit v1 compatibility path versus the default/v2 path, and should preserve v1/v2/vector/hybrid guardrails when changing this runbook or benchmark harness.

## Profiles for current v1 hot spots and M5 block-max path

Search CPU/alloc profile:

```sh
BENCH='^BenchmarkTextV2ContractSearchScale2623/docs_10000/score_only_common_no_docs$'
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench "$BENCH" \
  -benchmem \
  -benchtime=10x \
  -count=1 \
  -cpuprofile "$OUT/text_search_cpu.pprof" \
  -memprofile "$OUT/text_search_mem.pprof" \
  | tee "$OUT/text_search_profile_bench.txt"
go tool pprof -top "$OUT/text_search_cpu.pprof" > "$OUT/text_search_cpu_top.txt"
go tool pprof -top "$OUT/text_search_mem.pprof" > "$OUT/text_search_mem_top.txt"
```

M5 block-max CPU profile for common terms:

```sh
BENCH='^BenchmarkTextV2BlockMaxCommonTerm2628/blockmax_common_topk$'
GOWORK=off TREEDB_TEXT_V2_BLOCKMAX_DOCS=100000 go test ./TreeDB/collections \
  -run '^$' \
  -bench "$BENCH" \
  -benchmem \
  -benchtime=3x \
  -count=1 \
  -cpuprofile "$OUT/text_v2_blockmax_cpu.pprof" \
  -memprofile "$OUT/text_v2_blockmax_mem.pprof" \
  | tee "$OUT/text_v2_blockmax_profile_bench.txt"
go tool pprof -top "$OUT/text_v2_blockmax_cpu.pprof" > "$OUT/text_v2_blockmax_cpu_top.txt"
go tool pprof -top "$OUT/text_v2_blockmax_mem.pprof" > "$OUT/text_v2_blockmax_mem_top.txt"
```

Write CPU/alloc profile:

```sh
BENCH='^BenchmarkTextV2ContractWritePaths2623/insert_batch_text_indexed$'
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench "$BENCH" \
  -benchmem \
  -benchtime=10x \
  -count=1 \
  -cpuprofile "$OUT/text_write_cpu.pprof" \
  -memprofile "$OUT/text_write_mem.pprof" \
  | tee "$OUT/text_write_profile_bench.txt"
go tool pprof -top "$OUT/text_write_cpu.pprof" > "$OUT/text_write_cpu_top.txt"
go tool pprof -top "$OUT/text_write_mem.pprof" > "$OUT/text_write_mem_top.txt"
```

Known current v1 hot spots to look for include `executeTextSearchAtSnapshot`,
`scanTextSearchPostingsTerm`, `collectionGetAppendAtCatalogRoot`,
`decodeTextDocumentStateFieldLengths`, `textSearchCandidateMatchDetails`,
`hybridTextMatchesFromSearchResult`, `appendTextIndex*Deltas`,
`analyzeTextIndexField`, `addTextPostingsForDocument`, and text
posting/state/stats encoders. After M6, default hybrid candidate rows should no
longer allocate `textSearchCandidateMatchDetails` / `hybridTextMatchesFromSearchResult`
work unless `IncludeTextMatches` is explicitly enabled.

## Interpreting M0 rows

M0 rows are contract and baseline evidence, not a claim that v1 satisfies v2
production targets. In particular, current v1 common-term rows are expected to
scan O(df) postings and perform per-candidate state/norm lookups. Later v2 PRs
must reduce those counters with compressed blocks, packed norms, score-only
candidate generation, and exact block skipping while preserving deterministic
ranking and zero-document candidate generation.
