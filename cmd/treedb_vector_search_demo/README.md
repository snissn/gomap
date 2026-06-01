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
3. rebuild and persist the native HNSW graph,
4. optionally run `DB.CompactStorage(ctx, CompactStorageFull)`,
5. close and reopen the datastore,
6. validate document reads and ANN recall against exact search,
7. benchmark serial ANN search and parallel ANN search, and
8. report storage/memory usage.

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

Use `-keep-dir` to inspect an automatically-created temporary datastore after
the run. Passing an explicit `-dir` always keeps that directory, and it must be
empty or absent before the run starts.

Use `-matrix=false` to run only the single 1560-style case; add
`-compact=true` when that single case should run `CompactStorageFull`.
The matrix search stage defaults to 10,000 ANN queries per lane and parallel
concurrency levels `2,4,8,16,32,64,128`; override those with `-queries` and
`-search-concurrency`.

When `-dataset-dir` is used, `-queries` may truncate the exported query vector
file but cannot exceed the manifest query count. `-validate-queries` is a recall
sample size and is clamped to the exported query count.

Dataset-mode TreeDB documents intentionally store the full exported JSONL
record, including the `embedding` field, while comparator backends may consume
the binary `documents.f32` vectors directly. Storage numbers should be read with
that representation difference in mind.

The demo defaults to TreeDB's `bench` profile because this is a benchmark
harness. That profile is the explicit no-WAL benchmark ceiling: outer index
leaves are stored in the leaf value log, leaf prefix compression is enabled, and
value-log compression remains profile/default driven. The demo
also defaults to `-value-pointer-threshold 1024` and
`-leaf-generation-segment-target 4194304` for this vector-search workload, so
the leaf-vlog layout keeps ordinary vector documents in outer leaves and gives
the optional `CompactStorageFull` path sealed leaf generations to rewrite and
GC. Use
`-profile command_wal_durable|command_wal_relaxed|bench` to select a current
public TreeDB profile, or pass `0` for either demo storage knob to use the
selected profile default. Legacy/raw profile names may still parse during the
transition, but they are not recommended for new benchmark guidance.

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
