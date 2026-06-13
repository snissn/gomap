# TreeDB text-v2/hybrid industry comparison scoreboard (#2727)

This runbook is the stable #2727 scoreboard contract for downstream text-v2 and
hybrid optimization issues. It measures TreeDB text-only, vector-only,
text+vector, and text+vector+scalar retrieval rows against local external
baselines where practical. It is a benchmark/report harness, not an optimization
or industry-parity claim.

## Stable commands and artifacts

Default local smoke/current matrix:

```sh
RUN_DIR=/tmp/gomap_text_hybrid_scoreboard_$(date +%Y%m%d_%H%M%S) \
  scripts/bench_text_hybrid_scoreboard.sh
```

Primary outputs:

- `$RUN_DIR/scoreboard.md`
- `$RUN_DIR/scoreboard.json`
- `$RUN_DIR/context.txt`
- raw TreeDB Go benchmark logs under `$RUN_DIR/treedb_*.txt`
- optional external JSON artifacts such as `$RUN_DIR/sqlite_fts5_10k.json`

The report generator can also be used directly with raw artifacts:

```sh
go run ./cmd/treedb_text_hybrid_scoreboard \
  -out-dir "$RUN_DIR" \
  -context "$RUN_DIR/context.txt" \
  -go-bench "treedb_index_insert_search_10k=$RUN_DIR/treedb_index_insert_search_10k.txt" \
  -go-bench "treedb_hybrid_closeout_10k=$RUN_DIR/treedb_hybrid_closeout_10k.txt" \
  -go-bench "treedb_text_blockmax_10k=$RUN_DIR/treedb_text_blockmax_10k.txt" \
  -external "sqlite_fts5_10k=$RUN_DIR/sqlite_fts5_10k.json"
```

When `DOCS_10K` is overridden for smoke or scaled-down runs, the script uses an
exact-doc hybrid source label such as `treedb_hybrid_closeout_docs_64` so the
report dataset metadata reflects the actual fixture size rather than the default
10k label.

The generator fails closed by default when a TreeDB no-document candidate row has
non-zero `docs_fetched/search`, `full_doc_fallbacks/search`, `fail_closed/search`,
or text-v2 state/match-detail counters where score-only rows require zero. Use
`-allow-counter-failures` only to inspect a broken run; do not cite that report as
passing evidence.

## Corpus and query contract

### TreeDB 10k current matrix

The default script runs these TreeDB rows with `DOCS_10K=10000`, `DIMS=16`,
`M=8`, `topK=10`, and candidate budgets of `64` unless a benchmark row specifies
otherwise:

```sh
GOWORK=off TREEDB_INDEX_BENCH_DOCS=10000 TREEDB_INDEX_BENCH_DIMS=16 TREEDB_INDEX_BENCH_M=8 \
  go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkIndexInsertSearch2564/(search_text_v2_candidates_no_docs|search_vector_candidates_no_docs|search_hybrid_v2_no_docs_scalar_filter|indexed_insert_batch_flush_vector_rebuild)$' \
  -benchmem -benchtime=3x -count=1

GOWORK=off TREEDB_HYBRID_BENCH_DOCS=10000 TREEDB_HYBRID_BENCH_DIMS=16 TREEDB_HYBRID_BENCH_M=8 \
  go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkSearchHybridCloseout2506/mode_(text_only_no_docs|vector_only_no_docs|hybrid_no_docs)/topK_10/candidates_64/filter_(none_100pct|rare_06pct)$' \
  -benchmem -benchtime=3x -count=1

GOWORK=off TREEDB_TEXT_V2_BLOCKMAX_DOCS=10000 \
  go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkTextV2BlockMaxCommonTerm2628/(blockmax_common_topk|exhaustive_common_topk)$' \
  -benchmem -benchtime=3x -count=1
```

The corpus is deterministic synthetic customer-support text plus vectors:

- text fields: `title` weight `3`, `body` weight `1`;
- analyzer: TreeDB simple analyzer/default text-v2 analyzer assumptions used by
  collection text indexes;
- text query: `refund policy` for hybrid closeout rows; common single term
  `refund` for block-max rows;
- vector query: deterministic vector near the refund-topic documents;
- scalar filter: indexed tenant value `tenant-rare-06pct` where used;
- relevance labels: synthetic/topic-derived only. Use recall/quality labels from
  a real corpus before making relevance-quality claims.

### 100k rows

The default smoke run does not start heavier 100k jobs automatically. Run:

```sh
RUN_100K=true TEXT_100K_BENCHTIME=1x TEXT_100K_COUNT=1 \
  RUN_DIR=/tmp/gomap_text_hybrid_scoreboard_100k_$(date +%Y%m%d_%H%M%S) \
  scripts/bench_text_hybrid_scoreboard.sh
```

This adds the 100k text-v2 blockmax/common-term rows. 100k hybrid/vector rows
remain opt-in and should be planned explicitly before long local or service
runs.

## External baselines

### SQLite FTS5 embedded text baseline

The default script runs the stdlib Python SQLite FTS5 runner when FTS5 is
available:

```sh
python3 benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py \
  --docs 10000 --queries 1000 --top-k 10 \
  --out "$RUN_DIR/sqlite_fts5_10k.json"
```

Boundary: `rowid` + `bm25()` retrieval only from an FTS5 virtual table; no primary
JSON document fetch is timed. Storage is the SQLite DB file after WAL checkpoint
and truncate. If the local SQLite build lacks FTS5, the script emits an explicit
`status=unavailable` JSON artifact that the scoreboard records without inventing
numbers.

### Vector and hybrid service baselines

Use the existing vector external comparison harness for durable vector-only
comparators when a run is needed:

```sh
RUN_DIR=/tmp/gomap_vector_db_compare_$(date +%Y%m%d_%H%M%S) \
BACKENDS=treedb_column_graph,vectorlite,pgvector \
DOCS=10000 DIMS=1536 QUERIES=10000 VALIDATE_QUERIES=64 TOP_K=10 \
SEARCH_CONCURRENCY=1,8 M=16 EF_CONSTRUCTION=128 EF_SEARCH=128 \
scripts/bench_vector_db_compare.sh
```

The scoreboard generator accepts the resulting JSON files with repeated
`-external name=path` flags and maps `search_benchmarks` rows into vector-only
scoreboard rows.

Lucene, Tantivy, Bleve, Qdrant, Weaviate, Milvus, and OpenSearch hybrid rows are
not run by the default smoke harness. Add pinned service/library commands and
artifacts before citing them; otherwise record an explicit unavailable reason.

## Metrics every scoreboard row must report

For Go benchmark rows, include:

- exact command and artifact root;
- branch/commit, base ref/SHA, Go version, host/OS/load context;
- dataset shape, query shape, topK, candidate budget, and measured boundary;
- `ns/op`, derived `ops/sec`, `B/op`, and `allocs/op`;
- relevant counters: `docs_fetched/search`, `full_doc_fallbacks/search`,
  `fail_closed/search`, text posting/block/state/norm counters, scalar counters,
  vector candidate/edge counters;
- build/storage context when available (`indexed_insert_batch_flush_vector_rebuild`,
  `create_text_v2_index_backfill`, `index_bytes/doc`, external build/storage
  fields).

Candidate-generation rows must keep full document fetches at zero. Final-fetch
rows must keep document fetches bounded by top-K and should stay separate from
candidate rows.

## Downstream contract surface

Downstream issues #2728, #2729, #2731, and #2734 can cite:

- `scripts/bench_text_hybrid_scoreboard.sh` as the stable capture entry point;
- `cmd/treedb_text_hybrid_scoreboard` as the parser/report generator;
- `scoreboard.json` schema `treedb_text_hybrid_scoreboard/v1`;
- `scoreboard.md` retrieval and zero-doc counter validation tables.

Avoid changing these names or table meanings without updating this runbook and
the parser/report tests.
