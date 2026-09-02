# treedb_vector_demo

Quickstart/profile harness for TreeDB vector/RAG collection layouts. It creates a
fresh DB (explicit `-dir` values must be absent or empty), publishes deterministic embeddings as typed-column dense
`float32_vector` sections, rebuilds a `column_graph` vector index, reopens the DB,
and runs vector query smoke tests.

Example:

```sh
go run ./cmd/treedb_vector_demo \
  -rows 1000 \
  -dims 128 \
  -vectors typed-column \
  -metadata typed-row \
  -queries 10
```

Metadata can be stored in retained documents (`-metadata document`) or typed-row
fields (`-metadata typed-row`). The pre-alpha demo currently requires
`-vectors typed-column`; unsupported vector storage modes fail clearly instead
of silently using a different path.

Use `-final-fetch` to time the explicit full-document/embedding-echo
compatibility path after top-k selection; keep those numbers separate from
search-only throughput. For the preferred vector response shape that fetches
documents without returning embeddings, use the collection/vector APIs (or
`cmd/treedb_column_graph_demo -include-docs`) with
`ProjectionOrientedVectorDocumentFetchPreset`. Use `-metadata-filter` for a
deterministic tenant filter smoke; this uses exact scoring while still
publishing typed-column vector assets because public `column_graph` metadata
predicates are not wired yet.

Text, JSON, and markdown summaries include operation-specific vector search counters such as candidate rows, visited nodes/edges, vector payload bytes, adjacency payload bytes, and direct/scratch decode counts.

When `-profile-dir` is set, the command writes:

- `vector_demo_cpu.pprof`
- `vector_demo_allocs.pprof`
- `vector_demo_summary.json`
- `vector_demo_summary.md`

Useful presets: `vector-rag` and `perf-engineer`. Explicit flags such as
`-rows`, `-dims`, `-queries`, `-top-k`, `-batch-size`, and `-profile-dir`
override preset defaults.
