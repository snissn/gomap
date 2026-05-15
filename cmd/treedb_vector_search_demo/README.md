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
