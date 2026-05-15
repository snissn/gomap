# TreeDB Vector Search Demo

`treedb_vector_search_demo` is a first-class harness for exercising native
collection vector-index persistence end to end:

1. create a TreeDB collection with a declared vector field index,
2. load deterministic synthetic JSON documents,
3. rebuild and persist the native HNSW graph,
4. run `DB.CompactStorage(ctx, CompactStorageFull)`,
5. close and reopen the compacted datastore,
6. validate document reads and ANN recall against exact search,
7. benchmark ANN search and report storage/memory usage.

`CompactStorageFull` is intentionally used instead of manually chaining
maintenance calls. It is TreeDB's canonical full storage compaction path:
value-log rewrite/GC, leaf-generation pack/GC, index vacuum, settle passes,
zero-byte value-log cleanup, and final debt audit.

Example:

```sh
GOWORK=off go run ./cmd/treedb_vector_search_demo \
  -docs 10000 \
  -dims 64 \
  -queries 1000 \
  -validate-queries 32 \
  -top-k 10 \
  -json
```

Use `-keep-dir` to inspect the generated datastore after the run.

The demo defaults to TreeDB's `bench` profile because this is a benchmark
harness. That profile uses the same index storage profile as `fast`: outer
index leaves are stored in the leaf value log, leaf prefix compression is
enabled, and value-log compression remains profile/default driven. Use
`-profile durable|fast|wal_on_fast|bench` to select a different first-class
TreeDB profile.

The output includes the persisted TreeDB `format.json` knobs and storage-domain
bytes for `index.db`, `value_vlog`, and `leaf_vlog`. Use
`-require-value-log-bytes` or `-require-leaf-vlog-bytes` when a benchmark is
meant to prove that the compacted datastore actually used those storage domains.
