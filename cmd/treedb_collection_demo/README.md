# treedb_collection_demo

Quickstart/profile harness for TreeDB collections. It creates a fresh DB, loads deterministic fixture documents with `InsertBatch`, optionally checkpoints/reopens, and runs a selected read workload. Explicit `-dir` values must be absent or empty; temporary DBs are removed unless `-keep-dir` is set.

The default `document` mode, including the `document-app` preset, uses the shared
`cow_btree` owner through `treedb.Open` and its native semantic backend. Ordinary
collection inserts, index lookups, point reads, and scans use that same owner.
The command retains the public DB for checkpoint and close; the backend borrows
its lifetime. Inserts retain the command's `command_wal_durable` profile and
return after durable completion. An optional checkpoint remains a separate
maintenance step. On reopen the command selects the same route.

The five typed/hybrid modes retain their existing backend-root lifecycle and
typed scan/aggregate behavior. Route selection happens before opening the DB;
an operation error never triggers a switch to a different owner. Text output and
both summary files identify `publication_owner` (`native_semantic` or
`backend_roots`) and the live owner's `memtable_mode`. These fields distinguish
actual document-mode adoption from the compatibility routes.

Document mode uses the core's finite COW defaults, including 64 MiB in-flight and
512 MiB total/retired allocation charges. These account for engine-owned storage
and work, including its maintenance reservation; they are not process RSS
limits. Large user-selected batches or retained reads can exhaust admission.
The command reports the error and closes its manager and public owner; it does
not raise limits, fall back, or retry a publication with uncertain outcome.
Keep batch sizes within the available budget. Timing and materialization fields
keep their existing boundaries; route annotations alone are not a performance
comparison between document and typed modes.

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
