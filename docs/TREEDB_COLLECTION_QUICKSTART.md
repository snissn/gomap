# TreeDB Collection Quickstart Demos

TreeDB collections are **pre-alpha**: on-disk formats, command names, and public
collection APIs may change before stabilization. Rebuild demo directories between
runs unless a command explicitly documents reuse.

## Persona/use-case map

| Persona | Use case | Recommended demo/layout today |
| --- | --- | --- |
| Document application developer | Flexible JSON documents, point gets, simple scans | Use retained JSON document collections; collection demo coverage is being added under issue #1811. |
| Operational/event analytics developer | Timestamp filters and count/sum/avg over declared scalar fields | Use typed-column scalar fields for optimized int64 predicate/aggregate shapes; see TreeDB collection benchmark docs while the standalone collection demo lands. |
| Schema-aware application developer | Declared scalar fields plus predictable point reads | Use typed-row assets with retained payloads for flexible fields. |
| Hybrid product workload | Retained JSON payload plus typed metadata/metrics; separate final document fetch from scan/search | Use retained document + typed-row metadata + typed-column metrics/vectors, and time final fetch separately. |
| Vector search/RAG-style developer | Embeddings with metadata and optional final document fetch | `cmd/treedb_vector_demo` with `-vectors typed-column -metadata typed-row` or `-metadata document`. |
| Operations/performance engineer | Reproducible smoke/profile runs with counters and pprof artifacts | `cmd/treedb_vector_demo -preset perf-engineer -profile-dir ...` and the benchmark harnesses in `cmd/unified_bench`. |

## Vector/RAG quickstart

`cmd/treedb_vector_demo` creates a fresh TreeDB directory, declares a collection,
loads deterministic JSON fixtures, publishes embeddings as typed-column dense
`float32_vector` sections, rebuilds a `column_graph` vector index, closes and
reopens the DB, and then runs query smoke searches. Metadata can live in the
retained document or in typed-row fields.

Current optimized vector query shape: typed-column vectors + `column_graph`
native reader via `OpenVectorIndexSearcher`. Public metadata predicates are not
yet wired into the `column_graph` search API; `-metadata-filter` therefore uses
honest deterministic `exact_scoring_metadata_filter` while still publishing the
typed-column vector assets.

Smoke run:

```sh
go run ./cmd/treedb_vector_demo \
  -rows 1000 \
  -dims 128 \
  -vectors typed-column \
  -metadata typed-row \
  -queries 10
```

Document metadata mode:

```sh
go run ./cmd/treedb_vector_demo \
  -rows 1000 \
  -dims 128 \
  -vectors typed-column \
  -metadata document \
  -queries 10 \
  -final-fetch
```

Metadata-filter smoke (exact scoring, not ANN graph search):

```sh
go run ./cmd/treedb_vector_demo \
  -rows 1000 \
  -dims 128 \
  -vectors typed-column \
  -metadata typed-row \
  -metadata-filter \
  -queries 10
```

Profile run:

```sh
OUT=$(mktemp -d /tmp/treedb_vector_demo_profiles_XXXXXX)
go run ./cmd/treedb_vector_demo \
  -preset perf-engineer \
  -rows 10000 \
  -queries 100 \
  -profile-dir "$OUT"
```

Profile artifacts:

- `vector_demo_cpu.pprof`
- `vector_demo_allocs.pprof`
- `vector_demo_summary.json`
- `vector_demo_summary.md`

The command prints setup timing separately from vector search timing and optional
final document fetch timing. Counters include rows, dimensions, queries, top-k,
ops/sec, candidate/scored-vector counts, documents fetched, typed-column mapped
or heap-copy bytes, decoded bytes, and typed-column fallback counters where the
public search API exposes them.
