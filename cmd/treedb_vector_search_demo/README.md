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
  -validate-docs 16 \
  -top-k 10 \
  -m 16 \
  -ef-construction 128 \
  -ef-search 128 \
  -min-recall 0.95 \
  -compact=true \
  -disable-exact-fallback=true \
  -json
```

Use `-keep-dir` to inspect an automatically-created temporary datastore after
the run. Passing an explicit `-dir` always keeps that directory, and it must be
empty or absent before the run starts.

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
- `-disable-exact-fallback=false`: allow exact fallback during benchmark
  searches.
- `-validate-queries N` and `-min-recall R`: run recall validation for `N`
  queries; set `-min-recall=0` when disabling validation with
  `-validate-queries=0`.
- `-json`: emit the full result object for scripts.
