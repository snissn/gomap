# treedb_collection_demo

Quickstart/profile harness for TreeDB collections. It creates a fresh DB, loads deterministic fixture documents with `InsertBatch`, optionally checkpoints/reopens, and runs a selected read workload. Explicit `-dir` values must be absent or empty; temporary DBs are removed unless `-keep-dir` is set.

Examples:

```sh
go run ./cmd/treedb_collection_demo -mode document -rows 1000 -workload range-aggregate
go run ./cmd/treedb_collection_demo -mode typed-column -rows 1000 -workload range-aggregate
OUT=$(mktemp -d /tmp/treedb_collection_demo_profiles_XXXXXX)
go run ./cmd/treedb_collection_demo -mode typed-column -rows 10000 -workload range-aggregate -profile-dir "$OUT"
```

Supported modes: `document`, `typed-row`, `typed-column`, `hybrid-document-row`, `hybrid-document-column`, `hybrid-row-column`.

Supported workloads: `insert`, `point-get`, `range-filter`, `range-aggregate`, `full-aggregate`, `mixed-read`, `reopen-read`.

Named presets: `document-app`, `event-analytics`, `schema-aware`, `hybrid-product`, `perf-engineer`. Explicit flags such as `-rows`, `-batch-size`, `-mode`, and `-workload` override preset defaults.

When `-profile-dir` is set, the command writes:

- `cpu.pprof`
- `allocs.pprof`
- `summary.json`
- `summary.md`
