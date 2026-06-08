# TreeDB Indexed Insertion And Search Benchmark (#2564)

This runbook covers the collection index lifecycle boundary: inserting documents into a collection with scalar, text, and vector index definitions enabled, then searching those indexes on the same synthetic fixture.

Issue: <https://github.com/snissn/gomap/issues/2564>.

## Scope and boundaries

`BenchmarkIndexInsertSearch2564` lives in `TreeDB/collections`.

The benchmark has two timing boundaries:

1. `indexed_insert_batch_flush_vector_rebuild` times `InsertBatch`, `Flush`, and `RebuildVectorIndex` for one prepared batch. It excludes database/collection creation and JSON fixture generation. This row is a lifecycle/index-readiness row, not only an append/write row.
2. Search rows build and index the fixture before `ResetTimer`, then time only the search API call:
   - `search_text_candidates_no_docs` uses `SearchHybridTextCandidates`;
   - `search_vector_candidates_no_docs` uses `SearchHybridVectorCandidates`;
   - `search_hybrid_no_docs_scalar_filter` uses `SearchHybrid` without final document fetch;
   - `search_hybrid_fetch_topk_scalar_filter` uses `SearchHybrid` with bounded final fetch and `embedding` excluded from returned documents.

Candidate-generation rows must keep `docs_fetched/search=0` and `full_doc_fallbacks/search=0`. The final-fetch hybrid row must keep `docs_fetched/search <= topk/search`.

## Fixture shape

Default fixture knobs:

| Setting | Default | Override |
| --- | ---: | --- |
| documents | 256 | `TREEDB_INDEX_BENCH_DOCS` |
| vector dimensions | 16 | `TREEDB_INDEX_BENCH_DIMS` |
| vector graph `M` | 8 | `TREEDB_INDEX_BENCH_M` |
| scalar indexes | `tenant`, `region` | code change only |
| text index | `lexical` over `title` (weight 3) and `body` | code change only |
| vector route | exact cosine column graph | code change only |
| search query | text `refund policy`; vector query near refund docs | code change only |
| scalar filter | `tenant-rare-06pct` (~6.25% at default shape) | code change only |

## Main command

```sh
OUT=/tmp/gomap_index_insert_search_bench_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT"
set -o pipefail

GOWORK=off \
TREEDB_INDEX_BENCH_DOCS=256 \
TREEDB_INDEX_BENCH_DIMS=16 \
TREEDB_INDEX_BENCH_M=8 \
go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkIndexInsertSearch2564$' \
  -benchmem \
  -benchtime=5x \
  -count=3 \
  | tee "$OUT/index_insert_search_bench.txt"
```

For a quick compile/guardrail smoke use `-benchtime=1x -count=1` and a smaller fixture, for example `TREEDB_INDEX_BENCH_DOCS=64 TREEDB_INDEX_BENCH_DIMS=8 TREEDB_INDEX_BENCH_M=4`.

## Current context evidence

Artifact root: `/tmp/gomap_index_insert_search_bench_20260607_162015`
Raw output: `/tmp/gomap_index_insert_search_bench_20260607_162015/index_insert_search_bench.txt`
Context: `/tmp/gomap_index_insert_search_bench_20260607_162015/context.txt`

Context:

- commit: `840bff52062e3b2c1c2818cde7d94efe3b9a45ce`
- branch: `snissn/2564-index-bench`
- Go: `go1.26.0 darwin/arm64`
- host: Apple M3, 8 CPUs, Darwin arm64
- load note: active laptop/Orca environment; load averages were about `6.40 4.69 3.66`. Treat this as current-main context/smoke evidence, not a universal throughput claim.

Selected averages from `-benchtime=5x -count=3`:

| Row | ns/op avg | ops/sec | B/op | allocs/op | docs/op | docs/sec | docs fetched | text cand | vector cand | fused | scalar rejected | fail/fallback | trunc | insert ns/doc | vector rebuild ns/doc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| `indexed_insert_batch_flush_vector_rebuild` | 79,928,531 | 12.5 | 11,014,446 | 113,174 | 256 | 3,202.9 | 0 | 0 | 0 | 0 | 0 | 0/0 | 0 | 174,590 | 137,618 |
| `search_text_candidates_no_docs` | 275,850 | 3,625.2 | 425,584 | 7,591 | 0 | 0.0 | 0 | 64 | 0 | 0 | 0 | 0/0 | 64 | 0 | 0 |
| `search_vector_candidates_no_docs` | 20,475 | 48,840.0 | 36,408 | 82 | 0 | 0.0 | 0 | 0 | 64 | 0 | 0 | 0/0 | 192 | 0 | 0 |
| `search_hybrid_no_docs_scalar_filter` | 328,450 | 3,044.6 | 514,723 | 7,791 | 0 | 0.0 | 0 | 64 | 64 | 16 | 112 | 0/0 | 258 | 0 | 0 |
| `search_hybrid_fetch_topk_scalar_filter` | 546,853 | 1,828.6 | 572,528 | 8,768 | 0 | 0.0 | 10 | 64 | 64 | 16 | 112 | 0/0 | 258 | 0 | 0 |

Interpretation notes:

- Insert `docs/sec` is derived as `ops/sec * docs/op`; the Go row reports one prepared batch per op.
- The insertion row includes vector-index rebuild so the fixture is search-ready. Use `insert_batch_ns/doc`, `flush_ns/doc`, and `vector_rebuild_ns/doc` to separate the measured subphases.
- No-document candidate/search rows kept `docs_fetched/search=0` and `full_doc_fallbacks/search=0`.
- `search_hybrid_fetch_topk_scalar_filter` fetched exactly `10` documents with `topk/search=10`, preserving the bounded final-fetch contract.
- `truncated/search` reflects bounded candidate budgets, not a scan-all-documents fallback.
- These are current-main context rows only. Do not present them as before/after optimization evidence without rerunning an identical baseline/candidate comparison on the same host.

## Required PR evidence checklist

When updating this benchmark or publishing a new row, include:

- exact command and artifact path;
- commit, branch, hardware, host-load context, and Go version;
- fixture shape and timing boundary;
- `ns/op`, `ops/sec`, `B/op`, `allocs/op`;
- insertion metrics (`docs/op`, derived docs/sec, `insert_batch_ns/doc`, `flush_ns/doc`, `vector_rebuild_ns/doc`);
- search metrics (`text_candidates/search`, `vector_candidates/search`, `candidates_fused/search`, `docs_fetched/search`, scalar counters, fallback/fail/truncation counters);
- caveats for local/context-only runs.
