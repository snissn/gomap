# collection-load-fixture

`collection-load-fixture` creates and keeps an inspectable TreeDB collection
database using the same indexed document shape as
`BenchmarkCollectionShapeInsertBatch`.

Build:

```sh
make collection-load-fixture
```

Default load shape:

- `template-v1` documents
- two secondary indexes: unique `email_idx` and non-unique `city_idx`
- collection data/index-state outer leaves in the value log
- secondary-index outer leaves in the value log
- `fast` TreeDB profile
- final checkpoint and reopen verification
- automatic offline index vacuum when `-vlog-rewrite` or `-leafgen-pack-gc`
  is requested, so post-maintenance size comparisons include both value/leaf-log
  cleanup and index compaction

Example:

```sh
./bin/collection-load-fixture \
  -dir /tmp/treedb_two_index_template_v1_index_vlog \
  -reset \
  -docs 1000000 \
  -batch-size 8000
```

If `-dir` is omitted, the tool creates a kept OS temp directory and prints the
path. It never deletes the loaded database unless `-reset` is explicitly passed
for a named `-dir`.

Useful variants:

```sh
# JSON documents with the same two-index shape.
./bin/collection-load-fixture -format json -dir /tmp/treedb_two_index_json_index_vlog -reset

# Disable secondary-index value-log outer leaves.
./bin/collection-load-fixture -index-outer-leaves-in-vlog=false -dir /tmp/treedb_two_index_template_v1_fast_index -reset

# Generate machine-readable output and optional profiles.
./bin/collection-load-fixture \
  -json \
  -cpuprofile /tmp/collection_fixture_cpu.pprof \
  -memprofile /tmp/collection_fixture_heap.pprof

# Compact the persistent value_vlog after loading, then run index vacuum and
# report before/after disk usage.
./bin/collection-load-fixture -vlog-rewrite -dir /tmp/treedb_fixture_rewritten -reset

# Pack leaf_vlog generations after loading, then run leaf-generation GC and report
# the before/after disk usage separately from value_vlog rewrite. The default
# -index-vacuum=auto follows this with offline index vacuum.
./bin/collection-load-fixture -leafgen-pack-gc -dir /tmp/treedb_fixture_leafgen_packed -reset

# Keep the pre-vacuum index.db shape for debugging.
./bin/collection-load-fixture -leafgen-pack-gc -index-vacuum=none -dir /tmp/treedb_fixture_leafgen_no_vacuum -reset

# Force a small leaf-generation target for short local leafgen smoke tests.
./bin/collection-load-fixture -leaf-segment-target-bytes 65536 -leafgen-pack-gc -dir /tmp/treedb_fixture_leafgen_small -reset
```
