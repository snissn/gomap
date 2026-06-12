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
- domain counters: postings scanned, posting blocks visited/skipped, candidates
  scored, state/norm lookups, docs fetched, match details built, scalar filter
  selectivity, fail-closed count/reason, write amplification, index bytes/doc,
  and rewrite/merge state when applicable.

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

M4 also exposes explicit-v2 #2564-shape score-only rows. The v2 text rows use the
same synthetic #2564 documents and scalar distribution but create the v2 text
index after insert, because inline v2 metadata is intentionally rejected; the
existing command-WAL/vector guardrail rows remain the vector no-regression lane.

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
- `insert_batch_text_v2_indexed`: explicit v2 `InsertBatch` after empty-index
  `CreateTextIndex` setup; setup excludes DB/collection/index creation.
- `create_text_index_backfill`: v1 `CreateTextIndex` over already inserted
  primary documents; setup excludes primary insert.
- `create_text_v2_index_backfill`: explicit v2 `CreateTextIndex` over already
  inserted primary documents; setup excludes primary insert.
- `update_batch_text_indexed`: v1 `UpdateBatch` replacements with maintained
  text roots; setup excludes initial insert.
- `update_batch_text_v2_indexed`: explicit v2 `UpdateBatch` replacements with
  maintained micro-block/docmap/norm roots; setup excludes initial insert.
- `delete_batch_text_indexed`: v1 `DeleteBatch` with maintained text roots;
  setup excludes initial insert.
- `delete_batch_text_v2_indexed`: explicit v2 `DeleteBatch` with generation and
  tombstone maintenance; setup excludes initial insert.

Command:

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

Report text write counters:

- `posting_entries/op`, `state_entries/op`, `stats_entries/op` for v1 rows;
- `v2_docid_entries/op`, `v2_docmap_blocks/op`, `v2_posting_blocks/op`,
  `v2_norm_blocks/op`, and `v2_term_stats/op` for v2 rows;
- `posting_blocks/doc`, `high_df_posting_blocks/op`,
  `high_df_posting_blocks/doc`, and `rewritten_blocks/doc` (M3 rewrite count is `0`);
- `index_entries/doc` for live text-root entries after the operation;
- `write_amp_entries/doc` for estimated text-root entry writes emitted by the
  operation. For maintained v2 insert/update/delete rows this is the emitted
  root-delta estimate (docID/docmap/norm/term-status/posting-block deltas plus
  the status generation), not the net post-mutation root size;
- `index_bytes/doc`.

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
- `posting_blocks_visited/search` and `posting_blocks_skipped/search` (v1 = 0; M4 v2 exhaustive search reports visited blocks and skipped=0);
- `candidates_scored/search`;
- `state_lookups/search` and `norm_lookups/search`;
- `match_details/search`;
- `docs_fetched/search`, `full_doc_fallbacks/search`, `fail_closed/search`.

M4 (#2627) adds an explicit v2 score-only search benchmark with the same corpus
and query cases. It creates a v2 index with `CreateTextIndex`, scans compressed
posting blocks under one snapshot, scores from norm/docmap blocks, and asserts
`state_lookups=0`, `match_details=0`, and `docs_fetched=0`:

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

## Concurrent serving/load row

Benchmark name: `BenchmarkTextV2ContractConcurrentServing2623`.

The row serves warmed `SearchHybridTextCandidates` calls concurrently and reports
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

Counters include `p50_ns/search`, `p95_ns/search`, `p99_ns/search`,
`steady_heap_bytes`, `readers`, `cache_warm`, and
`mixed_write_snapshot_churn`.

## Profiles for current v1 hot spots

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
`appendTextIndex*Deltas`, `analyzeTextIndexField`, `addTextPostingsForDocument`,
and text posting/state/stats encoders.

## Interpreting M0 rows

M0 rows are contract and baseline evidence, not a claim that v1 satisfies v2
production targets. In particular, current v1 common-term rows are expected to
scan O(df) postings and perform per-candidate state/norm lookups. Later v2 PRs
must reduce those counters with compressed blocks, packed norms, score-only
candidate generation, and exact block skipping while preserving deterministic
ranking and zero-document candidate generation.
