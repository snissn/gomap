# TreeDB Vector Search Demo

`treedb_vector_search_demo` is a first-class harness for exercising native
collection vector-index persistence end to end. By default it runs a storage
and search matrix with three cases:

1. 1558-style outer B-tree leaves stored in `index.db`,
2. 1560-style outer B-tree leaves stored in `leaf_vlog` before compaction, and
3. 1560-style outer B-tree leaves stored in `leaf_vlog` after compaction.

Each case:

1. create a TreeDB collection with a declared vector field index,
2. load deterministic synthetic JSON documents,
3. rebuild and persist the ColumnGraph HNSW assets,
4. optionally run `DB.CompactStorage(ctx, CompactStorageFull)`,
5. close and reopen the datastore,
6. validate document reads and ANN recall against exact search,
7. benchmark serial ANN search and parallel ANN search, and
8. report storage/memory usage.

The benchmark search phase is a no-document ANN/search-throughput boundary:
serial and parallel search metrics return IDs/scores and do not time final
document fetch, reconstruction, projection, or serialization. The timed
`column_graph` search loop uses caller-owned reusable result buffers and excludes
response-owned convenience allocation. Document reads in validation are correctness checks,
not the preferred vector response-shape evidence; use
`ProjectionOrientedVectorDocumentFetchPreset` in collection/vector APIs when
timing projected documents without embeddings.

`column_graph` is the default and only accepted strategy. Removed strategy flags
are rejected before opening or creating the data directory. The demo can also select explicit
TreeDB query modes with `-vector-query-mode exact|quantized_only|quantized_rerank`.
Quantized modes declare a named `scalar_u8` or `rabitq_1bit` score plane on the
TreeDB vector index, build it during `RebuildVectorIndex`, validate recall
against exact full-vector ground truth, and report per-search quantized
counters. They do not change exact/default behavior and do not affect
PostgreSQL+pgvector comparator semantics in the external harness. The
`column_graph` JSON/text search rows also report no-document guardrail counters
such as `avg_documents_fetched`,
`avg_response_owned_result_allocs`, and route/fallback averages so profile runs
can distinguish buffered search-loop cost from response-owned convenience or
document-materialization paths.

`CompactStorageFull` is intentionally used instead of manually chaining
maintenance calls. It is TreeDB's canonical full storage compaction path:
value-log rewrite/GC, leaf-generation pack/GC, index vacuum, settle passes,
zero-byte value-log cleanup, and final debt audit.

Example:

```sh
GOWORK=off go run ./cmd/treedb_vector_search_demo \
  -matrix=false \
  -docs 10000 \
  -dims 64 \
  -queries 10000 \
  -validate-queries 32 \
  -validate-docs 16 \
  -top-k 10 \
  -m 16 \
  -ef-construction 128 \
  -ef-search 128 \
  -min-recall 0.95 \
  -compact=true \
  -disable-exact-fallback=true \
  -require-leaf-vlog-bytes \
  -json
```

TreeDB column_graph scalar_u8 quantized-rerank example:

```sh
GOWORK=off go run ./cmd/treedb_vector_search_demo \
  -matrix=false \
  -vector-index-strategy column_graph \
  -vector-query-mode quantized_rerank \
  -quantized-codec scalar_u8 \
  -quantized-index-name embedding.scalar_u8.fast \
  -quantized-rerank-candidates 32 \
  -docs 10000 \
  -dims 128 \
  -queries 10000 \
  -validate-queries 64 \
  -top-k 10 \
  -m 16 \
  -ef-construction 128 \
  -ef-search 128 \
  -json
```

Use `-keep-dir` to inspect an automatically-created temporary datastore after
the run. Passing an explicit `-dir` always keeps that directory, and it must be
empty or absent before the run starts.

Use `-matrix=false` to run only the single 1560-style case; add
`-compact=true` when that single case should run `CompactStorageFull`.
The matrix search stage defaults to 10,000 ANN queries per lane and parallel
concurrency levels `2,4,8,16,32,64,128`; override those with `-queries` and
`-search-concurrency`.

TreeDB column_graph RaBitQ quantized-only example:

```sh
GOWORK=off go run ./cmd/treedb_vector_search_demo \
  -matrix=false \
  -vector-index-strategy column_graph \
  -vector-query-mode quantized_only \
  -quantized-codec rabitq_1bit \
  -quantized-index-name embedding.rabitq_1bit.fast \
  -docs 10000 \
  -dims 1536 \
  -queries 10000 \
  -validate-queries 64 \
  -top-k 10 \
  -m 16 \
  -ef-construction 128 \
  -ef-search 128 \
  -min-recall 0 \
  -json
```

Use `-search-profile-dir DIR` to write per-concurrency profiles for the
`column_graph` search stage. Each measured concurrency emits
`search_<mode>_c<N>_cpu.pprof`, plus `heap`, `allocs`, `block`, and `mutex`
runtime snapshots with the same prefix. In matrix mode, each storage case writes
under `DIR/<matrix_case>/` (for example
`DIR/leaf_vlog_after_compact/search_exact_c8_cpu.pprof`) so case artifacts are
not overwritten. CPU profiles are scoped to the measured search loop; the
runtime snapshots are supporting diagnostics and can include process state
outside the search loop. Heap and allocation profiles use the Go runtime's
current sampling rate; the demo does not change `runtime.MemProfileRate` for a
profile run. Block profiles are emitted from the runtime's current block
profiler and may be empty unless the caller/process already enabled block
profiling. Profiling changes timings and should be used for bottleneck analysis,
not as the comparison number to publish.

When `-dataset-dir` is used, `-queries` may truncate the exported query vector
file but cannot exceed the manifest query count. `-validate-queries` is a recall
sample size and is clamped to the exported query count. Recall validation uses
TreeDB exact search by default (`-validation-exact-source=treedb`). Set
`-validation-exact-source=dataset` to require `-dataset-dir` and compute exact
top-K IDs directly from the exported `documents.f32`/`queries.f32` vectors;
that mode compares IDs only and does not materialize TreeDB documents as part of
the exact baseline.

Dataset-mode TreeDB documents intentionally store the full exported JSONL
record, including the `embedding` field, while comparator backends may consume
the binary `documents.f32` vectors directly. Storage numbers should be read with
that representation difference in mind.

The demo defaults to TreeDB's `command_wal_durable` profile. ColumnGraph
construction requires a command-WAL profile; `bench_unsafe` and `no_wal_fast`
are refused. The demo also defaults to `-value-pointer-threshold 1024` and
`-leaf-generation-segment-target 4194304` for this vector-search workload, so
the leaf-vlog layout keeps ordinary vector documents in outer leaves and gives
the optional `CompactStorageFull` path sealed leaf generations to rewrite and
GC. Use
`-profile command_wal_durable|command_wal_relaxed` to select a command-WAL
profile, or pass `0` for either demo storage knob to use the selected profile
default. A relaxed profile does not provide durable-at-ACK semantics;
legacy/raw profile names are rejected.

The output includes the persisted TreeDB `format.json` knobs and storage-domain
bytes for `index.db`, `value_vlog`, and `leaf_vlog`. Use
`-require-value-log-bytes` or `-require-leaf-vlog-bytes` when a benchmark is
meant to prove that the compacted datastore actually used those storage domains.
Those flags are assertions, not format selectors; the demo fails if the selected
TreeDB settings leave the asserted domain empty.

Useful flags:

- `-compact=false`: skip `CompactStorageFull` and report uncompacted reopen/load
  behavior.
- `-compact-sync-each-phase=true`: ask compaction to fsync each rewrite/pack
  phase.
- `-dir PATH`: write into a caller-chosen empty directory and keep it after the
  run.
- `-search-profile-dir DIR`: write per-concurrency column_graph search profiles
  under `DIR`; requires `-vector-index-strategy column_graph`.
- `-validate-queries N` and `-min-recall R`: run recall validation for `N`
  queries; set `-min-recall=0` when disabling validation with
  `-validate-queries=0`.
- `-validation-exact-source treedb|dataset`: select the exact baseline for
  recall validation. The default `treedb` preserves current behavior; `dataset`
  requires `-dataset-dir` and computes exact IDs from exported dataset vectors.
- `-vector-index-strategy column_graph`: explicitly select the default persisted
  column-store graph search path; other strategies are unsupported.
- `-vector-query-mode exact|quantized_only|quantized_rerank`: select the
  column_graph score plane. The default is `exact`; quantized modes require
  `column_graph` and a quantized index name.
- `-quantized-codec scalar_u8|rabitq_1bit`: quantized score-plane codec for
  quantized modes. Empty defaults to `scalar_u8` when a quantized query mode is
  selected. Exact mode rejects this flag so it cannot accidentally declare
  quantized assets.
- `-quantized-index-name NAME`: named quantized score plane for quantized modes
  (for example `embedding.scalar_u8.fast` or `embedding.rabitq_1bit.fast`).
  Exact mode rejects this flag so it cannot accidentally declare quantized
  assets.
- `-quantized-rerank-candidates N`: exact-rerank candidate limit for
  `quantized_rerank`; `0` uses the normalized `ef_search` candidate set.
- `-json`: emit the full result object for scripts.
