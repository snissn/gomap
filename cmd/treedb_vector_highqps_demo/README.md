# treedb_vector_highqps_demo

Small exact-only TreeDB vector serving demo for
`Collection.SearchVectorIndexWithBuffer`.

The command builds a fresh collection with a `column_graph` float32/cosine vector
index, checkpoints and reopens the DB, warms the collection-owned prepared
`hnsw_search_pack_v1` state, then runs a timed no-document search loop while
reusing one caller-owned `collections.VectorIndexSearchBuffer`.

It is instructional, not a benchmark replacement. Use
`TreeDB/docs/guides/vector-search-benchmark-workflow.md` for performance
evidence and TreeDB-vs-external-baseline reproduction commands.

## Run

```sh
GOWORK=off go run ./cmd/treedb_vector_highqps_demo \
  -docs 1000 \
  -dims 64 \
  -queries 1000 \
  -warmup-queries 16 \
  -top-k 10 \
  -m 16 \
  -ef-construction 128 \
  -ef-search 128
```

Use `-json` for machine-readable output and `-keep-dir` or `-dir PATH` to keep
the generated DB directory for inspection. Explicit `-dir` paths must be absent
or empty.

## What the output proves

The text output prints:

- the exact API boundary: `Collection.SearchVectorIndexWithBuffer`;
- checkpoint/reopen status;
- warmup count and timed-loop count;
- top-k IDs/scores for one route-evidence query;
- route guardrails from stable `VectorIndexSearchStats` fields documented by the
  benchmark workflow:
  - `search_route_hnsw_search_pack/search=1`
  - `hnsw_search_pack_active/search=1`
  - `docs_fetched/search=0`
  - `graph_row_fallbacks/search=0`
  - `typed_column_vector_fallbacks/search=0`
  - `vector_scratch_decodes/search=0`

The timed loop uses `VectorIndexSearchStatsModeProduction`; it does not enable
`benchmark_debug` diagnostics.

## API boundaries

- `Collection.SearchVectorIndexWithBuffer` is the production high-QPS
  no-document collection API: caller-owned `VectorIndexSearchBuffer`, reusable
  buffer, zero-allocation target after setup/warmup.
- `Collection.SearchVectorIndex` with `IncludeDocuments=false` is the
  no-document convenience API: response-owned results and allocation are
  expected.
- `Collection.SearchVectorIndex` with `IncludeDocuments=true` is a separate
  document materialization path and is intentionally not part of this demo's
  timed loop.
- `OpenVectorIndexSearcher` + `SearchWithBuffer` is the reusable low-level
  serving path when the caller owns searcher/snapshot lifetime.

## Non-goals

- No quantized example. Collection-level buffered quantized search remains a
  separate follow-up until #2415 lands.
- No document materialization path in the timed loop.
- No TreeDB-vs-USearch performance claim; follow the benchmark workflow for any
  comparison evidence.
