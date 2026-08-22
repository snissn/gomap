# TreeDB RAG Retrieval Benchmark Runbook (#4267)

This runbook makes the TreeDB end-to-end RAG retrieval benchmark reproducible
from committed commands only. It measures retrieval **quality and cost on the
same fixture**: recall@k / MRR for text-only, vector-only, and hybrid rows;
query p50/p99; ingest docs/sec; storage bytes/doc. It is a benchmark/docs
contract, not an optimization task.

Parent tracker: <https://github.com/snissn/gomap/issues/4266>. Issue:
<https://github.com/snissn/gomap/issues/4267>.

Harness: `TreeDB/cmd/treedb_rag_benchmark` (new files only; no product code
touched). Report artifact schema: `treedb_rag_benchmark/v1` (JSON + markdown).

## Fixture contract

- Corpus `treedb-rag-corpus/v1`: deterministic topic vocabulary (8 topics × 12
  terms), each document belongs to one topic and contributes exactly 2 indexed
  chunks with stable IDs `doc-NNNNNN#chunk-{0,1}` and `parent_doc` linkage.
- Embeddings: deterministic reference embedder — feature hashing of lowercase
  alphanumeric tokens into 64 buckets (FNV-1a bucket index, sign from the hash
  top bit), L2-normalized. No randomness, no external model, fully hermetic.
- Scalar filters mirror the hybrid scoreboard: `tenant` with selectivity
  ~6.25% (`tenant-rare-06pct`) and ~25% (`tenant-narrow-25pct`) plus an
  unfiltered row.
- Query set: 24 committed queries (3 per topic, 4 topic terms each), embedded
  by the same embedder.

### Ground-truth derivation (documented labeling scheme)

A chunk is **relevant** to a query iff

```
|tokens(query.text) ∩ tokens(chunk.title + " " + chunk.body)| >= 2
&& cosine(embed(query.text), embed(chunk.title + " " + chunk.body)) >= 0.35
```

using exactly the tokenizer and embedder above (constants
`ragMinLexicalOverlap`, `ragMinQueryCosine` in `fixture.go`). This is
synthetic-but-principled: the lexical gate proves shared vocabulary and the
cosine gate proves the deterministic embedder places the chunk near the query
centroid. The builder fails closed if any query has an empty relevant set or a
fully-saturated topic.

**Scaling note:** the labeled set is topical, so |relevant| grows linearly
with corpus size while per-chunk text composition stays fixed. Recall values
are judgment-set-relative and are therefore comparable across routes/filters
at a fixed corpus size, not across corpus sizes. Cross-size comparisons should
use MRR@10, latency, and the domain counters.

## Benchmark rows

Routes `text_only`, `vector_only`, `hybrid` (RRF fusion), each ×
`score_only` / `fetch_topk` result modes, each × filters
`none_100pct` / `rare_06pct` / `narrow_25pct` — 18 rows. All rows run through
the `Collection.SearchHybrid` public API (single-source rows pass only `Text`
or only `Vector`). Vector route: column-graph exact score plane
(`QueryMode=exact`), `M=8`, `ef_search=128`, dims=64. `TopK=10`,
per-source `candidate_limit=64`.

Metrics per row: recall@{5,10,100}, MRR@10, query p50/p99/mean, and averaged
domain counters (`documents_fetched`, `text_postings_scanned`,
`text_candidates_scored`, `vector_candidates_examined`, `candidates_fused`,
fusion split, `scalar_prefilter_ids`, `truncated`, fail-closed counters).
Ingest metrics: embed s and embed docs/s (embedder only), ingest docs/sec
(`InsertBatch` + `Flush` only), vector index build seconds, storage bytes and
bytes/chunk after checkpoint.

## Measurement boundary

- **Query timing**: per-query wall time around the `SearchHybrid` call only.
  Fixture build, collection create, ingest, index build, checkpoint, and
  warmup queries are excluded. Each row runs `warmup` untimed queries before
  the timed loop.
- **Embedding**: runs at fixture build; `embed_seconds` / `embed_docs_per_sec`
  are reported separately and excluded from `ingest_docs_per_sec`.
- **Ingest**: `ingest_docs_per_sec` covers `InsertBatch` + `Flush` only.
- **Index build**: `RebuildVectorIndex` duration reported separately.

## Repetition policy

Baseline runs use `-reps 3` (3 timed passes over all 24 queries per row, 72
timed samples per row) after `-warmup 8` untimed queries, on a quiet host.
p50/p99 are computed over all pooled per-query samples of the row. Re-run on
the same host class before comparing against the published baselines; treat
sub-0.1 ms p50 differences as noise.

## Host context (published baseline)

- Host: Apple MacBook (local), hostname `Michaels-Laptop.local`
- macOS (darwin) arm64, Apple M3, 8 CPUs
- Go 1.26.0 (`go1.26.0`), CGO_ENABLED=1
- Date: 2026-08-22, repo commit at baseline: this PR's head

## Exact baseline commands

```sh
export PATH="$HOME/.gvm/gos/go1.26.0/bin:$PATH"
export GOROOT="$HOME/.gvm/gos/go1.26.0"
export CGO_ENABLED=1
export GOCACHE="$HOME/orca/workspaces/gomap/.gocache-go126"
export GOWORK=off

go build -o /tmp/treedb_rag_benchmark ./TreeDB/cmd/treedb_rag_benchmark/

# Small baseline (512 docs -> 1024 chunks)
/tmp/treedb_rag_benchmark -docs 512  -reps 3 -warmup 8 \
  -out-dir <outdir>/small \
  -host-note "macOS arm64 Apple M3, quiet laptop, go1.26.0"

# Medium baseline (4096 docs -> 8192 chunks)
/tmp/treedb_rag_benchmark -docs 4096 -reps 3 -warmup 8 \
  -out-dir <outdir>/medium \
  -host-note "macOS arm64 Apple M3, quiet laptop, go1.26.0"
```

Committed artifacts:

- `treedb_rag_benchmark_baseline_small_2026-08-22.{md,json}` (fingerprint
  `86bb3e34…`)
- `treedb_rag_benchmark_baseline_medium_2026-08-22.{md,json}` (fingerprint
  `34351633…`)

## Published baseline (north-star targets)

Small: docs=512, chunks=1024, dims=64, queries=24, top_k=10, candidate_limit=64.

| metric | value |
|---|---:|
| ingest docs/s | 11,771 |
| storage bytes/chunk | 4,217 |
| vector index build s | 0.087 |
| hybrid score_only p50 / p99 ms (none_100pct) | 0.150 / 0.222 |
| hybrid fetch_topk p50 / p99 ms (none_100pct) | 0.447 / 0.638 |
| vector_only score_only p50 ms (none_100pct) | 0.028 |
| text_only score_only p50 ms (none_100pct) | 0.096 |
| hybrid recall@10 / MRR@10 (none_100pct) | 0.1761 / 1.0000 |

Medium: docs=4096, chunks=8192, dims=64, queries=24, top_k=10, candidate_limit=64.

| metric | value |
|---|---:|
| ingest docs/s | 12,557 |
| storage bytes/chunk | 3,992 |
| vector index build s | 0.591 |
| hybrid score_only p50 / p99 ms (none_100pct) | 0.671 / 1.333 |
| hybrid fetch_topk p50 / p99 ms (none_100pct) | 1.004 / 1.624 |
| vector_only score_only p50 ms (none_100pct) | 0.028 |
| text_only score_only p50 ms (none_100pct) | 0.577 |
| hybrid recall@10 / MRR@10 (none_100pct) | 0.0222 / 1.0000 |

Full 18-row matrices (all filters, all counters) live in the committed
artifacts. Filter rows behave as expected: rare ~6% selectivity cuts
`candidates_fused` sharply and lowers recall proportionally to the judgment
mass removed; `documents_fetched` stays bounded by TopK everywhere and stays 0
on every score-only row.

## Fail-closed counter contract

Report generation fails closed when:

- any `score_only` row shows `documents_fetched > 0`;
- any `fetch_topk` row exceeds `documents_fetched > top_k` per query on
  average;
- any row shows `full_document_scan_fallbacks > 0` or `fail_closed > 0`;
- the corpus is empty, or any query derives an empty/saturated relevant set.

Enforced by `validateCounterContract` + `buildReport`, and tested by
`contract_test.go` (including doctored-row violations and zero-doc corpora).

## Tests

```sh
go vet ./TreeDB/cmd/treedb_rag_benchmark/
go test ./TreeDB/cmd/treedb_rag_benchmark/ -count=1
```

- `TestGoldenFixtureStability`: builder output byte-stable across runs.
- `TestRecallAtKHandComputed` / `TestMRRAtKHandComputed` /
  `TestPercentileHandComputed` / `TestAccumulateQualityTinyFixture`:
  hand-computed metric validation.
- `TestCounterContractEndToEnd`, `TestScoreOnlyDocFetchFailsReport`,
  `TestFetchTopkBoundedByTopK`, `TestZeroDocCorpusFailsClosed`:
  counter-contract fail-closed gates.
