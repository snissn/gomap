# TreeDB Canonical Benchmark Runbook

This runbook defines the preferred benchmark entrypoints for TreeDB engine,
TreeDB collections, and Mongo-compatible collection workloads. Use it when a PR
or issue needs reproducible throughput, latency, profile, or disk-usage
evidence.

## Principles

- Start every report with the git commit, branch, host, OS, Go version, command,
  and output directory.
- Keep fixture shape explicit: document count, key count, value size, batch size,
  secondary index count, document format, profile, client mode, concurrency, and
  maintenance mode.
- Report both throughput and disk. For latency-sensitive phases, include p50,
  p95, and p99 when the harness emits them.
- Keep TreeDB maintenance phases named exactly. Do not call a row "compacted"
  unless the row is from `offline_rewrite`, `full_leafgen_pack_gc`,
  `sqlite_vacuum`, or another explicitly documented full-maintenance phase.
- Keep raw artifacts. Markdown tables are for review; JSON, TSV, profiles, and
  data directories are the durable evidence.

## Benchmark Layers

TreeDB has three benchmark layers. Pick the lowest layer that answers the
question, then add higher layers only when the user-facing path matters.

1. Raw TreeDB engine: `cmd/unified_bench` plus `cmd/benchprof`.
2. TreeDB collections: `cmd/collection_bench_matrix` and the canonical
   TreeDB-vs-SQLite runner.
3. Mongo-compatible collections: `cmd/mongo_gateway_bench` and
   `scripts/mongo_gateway_compare.sh`.

Raw engine results explain storage-engine ceilings and hot paths. Collection
results explain document storage, secondary indexes, maintenance, and SQLite
equivalence. Mongo gateway results explain Mongo-compatible ergonomics and the
cost of the official MongoDB driver, BSON wire handling, and gateway command
paths.

## Tiers

Use a small tier before running an expensive tier.

`smoke`

- Purpose: validate a harness change or check that the output schema still
  works.
- Size: 100 to 10,000 docs or keys.
- Expected runtime: seconds to a few minutes.
- Use before publishing benchmark harness PRs.

`pr`

- Purpose: provide reviewable PR evidence.
- Size: 100,000 to 1,000,000 docs or keys.
- Expected runtime: minutes to tens of minutes.
- Use for throughput, latency, disk, and profile comparisons.

`large`

- Purpose: answer scale questions and disk-maintenance questions.
- Size: 10,000,000 docs or more when the local machine can sustain it.
- Expected runtime: hours.
- Use only with an output directory, explicit timeout, and enough disk headroom.

## Raw TreeDB Engine

Build the tools:

```sh
make unified-bench benchprof
```

Standard profile capture:

```sh
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -keys 800000 \
  -profile wal_on_fast \
  -checkpoint-between-tests \
  -test random_write,random_delete,random_read,full_scan,prefix_scan \
  -profile-dir "$OUT" \
  -progress=false

./bin/benchprof -profiles-dir "$OUT"
```

Expected profile artifacts:

- `benchprof_results.json`
- `benchprof_results.md`
- `cpu_<test>_<db>.pprof`
- `allocs_<test>_<db>.pprof`
- `checkpoint_cpu_checkpoint_<test>_<db>.pprof`
- `block.pprof`
- `mutex.pprof`
- `trace.out`

Use this layer for write-path, read-path, checkpoint, value-log, cache,
allocator, and contention investigations. If profile names or profile-dir output
changes, update the parsers and tests documented in `AGENTS.md` and
`cmd/benchprof/README.md` in the same PR.

## TreeDB Collections vs SQLite

The canonical end-to-end TreeDB collections benchmark is:

```sh
make bench-collections-canonical
```

Equivalent direct command:

```sh
./scripts/bench_collections_canonical.sh
```

Use a stable output directory for any result that will be posted to a PR or
issue:

```sh
OUT=/tmp/collections_canonical_$(date +%Y%m%d_%H%M%S)
./scripts/bench_collections_canonical.sh -out-dir "$OUT"
```

The canonical runner emits:

- `benchmark_results.json`
- `benchmark_summary.md`
- `benchmark_matrix.csv`
- `timed_matrix/`
- `offline_rewrite/`
- `full_leafgen_pack_gc/`

For broader matrix work, build and run:

```sh
make collection-bench-matrix
OUT=/tmp/collection_matrix_$(date +%Y%m%d_%H%M%S)
./bin/collection-bench-matrix \
  -out-dir "$OUT" \
  -batch-size 16000 \
  -benchtime 100000x
```

Use a smoke matrix before changing report code:

```sh
OUT=/tmp/collection_matrix_smoke_$(date +%Y%m%d_%H%M%S)
./bin/collection-bench-matrix \
  -out-dir "$OUT" \
  -benchtime 1000x \
  -count 1
```

Collection report files to preserve:

- `README.md`
- `collections_matrix_summary.md`
- `collections_matrix_summary.html`
- `collections_user_story_summary.tsv`
- `collections_disk_usage_summary.tsv`
- `collections_maintenance_summary.tsv`
- `<cell>/collections_report.md`
- `<cell>/go_test.json`

Use `CGO_ENABLED=1` when the SQLite cells or SQLite native-column baselines are
part of the comparison.

## Collection Maintenance Semantics

The canonical collection maintenance phases are:

- `post_insert`: after insert plus the flush/checkpoint needed for correctness.
- `online_one_pass_maintenance`: bounded online maintenance; useful, but not a
  full compaction state.
- `offline_rewrite`: full/offline TreeDB value-log rewrite comparison point.
- `full_leafgen_pack_gc`: full leaf-generation pack/GC comparison point.
- `sqlite_vacuum`: SQLite compacted baseline after `VACUUM`.

Fair post-insert comparison:

- TreeDB `post_insert`
- SQLite `post_insert`

Fair compacted-state comparison:

- TreeDB `offline_rewrite`
- TreeDB `full_leafgen_pack_gc`
- SQLite `sqlite_vacuum`

When disk is the focus, report both logical file bytes from the harness and
filesystem physical usage when it is available. For TreeDB, include the
component breakdown for `index.db`, `leaf_vlog`, `value_vlog`, and `wal`. For
SQLite, report both the database file and the post-`VACUUM` size.

## Mongo-Compatible Collections vs MongoDB

Use the comparison wrapper for TreeDB-vs-MongoDB bundles:

```sh
scripts/mongo_gateway_compare.sh \
  --out /tmp/gomap_mongo_gateway_compare \
  --docs "100000 1000000" \
  --indexes "0 1 2" \
  --reads 50000 \
  --range-reads 5000 \
  --range-index \
  --updates 5000 \
  --concurrent-reader-sweep "1,2,4,8,16" \
  --concurrent-reads 50000 \
  --concurrent-writers 8 \
  --concurrent-writes 10000 \
  --insert-producers 8 \
  --mongo-max-pool-size 128 \
  --mongo-max-connecting 32 \
  --prebuild-documents \
  --mongo-mode docker \
  --timeout 120m
```

Use BSON for the current optimized TreeDB Mongo-compatible storage path:

```sh
TREEDB_DOCUMENT_FORMATS="bson" \
TREEDB_CLIENT_MODES="driver-command-raw" \
scripts/mongo_gateway_compare.sh
```

Use a client-mode matrix when measuring driver overhead and gateway ceiling:

```sh
TREEDB_DOCUMENT_FORMATS="bson" \
TREEDB_CLIENT_MODES="driver driver-command driver-command-raw driver-unack raw-wire-tcp raw-wire" \
MONGO_CLIENT_MODES="driver driver-command driver-command-raw driver-unack" \
BATCH_SIZE=5000 \
INSERT_PRODUCERS=8 \
MONGO_MAX_POOL_SIZE=128 \
MONGO_MAX_CONNECTING=32 \
PREBUILD_DOCUMENTS=true \
READS=0 \
RANGE_READS=0 \
UPDATES=0 \
CONCURRENT_READERS=0 \
CONCURRENT_WRITERS=0 \
scripts/mongo_gateway_compare.sh \
  --out /tmp/gomap_mongo_client_modes_1m_load \
  --docs "1000000" \
  --indexes "0 1 2" \
  --mongo-mode docker \
  --timeout 120m
```

Interpret client modes as separate questions:

- `driver`: ordinary official MongoDB Go driver CRUD-helper path.
- `driver-command`: official driver `RunCommand` insert path.
- `driver-command-raw`: official driver `RunCommand` with prebuilt raw BSON.
- `driver-unack`: official driver unacknowledged insert enqueue path plus
  post-load visibility wait.
- `raw-wire-tcp`: TreeDB-only raw OP_MSG load over loopback TCP.
- `raw-wire`: TreeDB-only in-process raw OP_MSG load.

Use `driver` for user-visible Mongo compatibility. Use `driver-command-raw` for
the fastest current acknowledged official-driver load path. Use raw-wire modes
only to estimate TreeDB gateway/server ceiling.

## Reader and Writer Scaling

Use the scaling wrapper when the question is concurrency plateau, update cost,
or indexed-field update overhead:

```sh
scripts/mongo_gateway_scaling_bench.sh \
  --out /tmp/gomap_mongo_gateway_scaling \
  --docs 1000000 \
  --indexes 2 \
  --batch-size 10000 \
  --insert-producers 8 \
  --writers "1 2 4 8 16 32" \
  --readers "1 2 4 8 16 32" \
  --concurrent-writes 80000 \
  --concurrent-reads 80000
```

Add `--include-mongo --mongo-uri mongodb://127.0.0.1:27017` to compare against
an existing MongoDB server. Add `--update-indexed-field` to update `city` and
exercise secondary-index maintenance instead of non-indexed document updates.

## Recommended Reference Workloads

Use deterministic documents with this shape unless a feature requires a
different fixture:

- `_id`: stable unique string or ObjectID.
- `email`: unique string secondary-index candidate.
- `city`: non-unique string secondary-index candidate.
- `age`: numeric range-index candidate.
- `active`: bool secondary-index candidate for unchanged-index update tests.
- `status` / `updated_at`: non-indexed update fields.
- `tags`: array field for future multikey and Mongo-query coverage.
- `pad`: fixed payload string for size and compression control.

Reference index counts:

- `0`: primary key only.
- `1`: primary key plus `email_1`.
- `2`: primary key plus `email_1` and `city_1`.
- `3`: adds `active_1` for unchanged-index update planning tests.

Reference phases:

- load insert throughput.
- point read by `_id`.
- point read by `email`.
- indexed age range with limit.
- fallback age range scan with limit when deliberately testing planner fallback.
- non-indexed `$set` update by `_id`.
- indexed-field `$set` update by `_id`.
- concurrent reader sweep.
- concurrent writer sweep.
- maintenance and disk phases.

## Reporting Template

Use this shape in PR and issue comments:

```md
## Benchmark

- base: `<commit>`
- branch: `<branch>`
- command: `<command or script>`
- artifacts: `<output dir>`
- host: `<machine summary>`
- docs/keys: `<count>`
- indexes: `<counts>`
- format/profile/client modes: `<values>`
- maintenance: `<phase or mode>`

### Throughput

| scenario | phase | TreeDB ops/s | baseline ops/s | ratio | TreeDB p95 | baseline p95 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |

### Disk

| scenario | phase | TreeDB bytes/doc | baseline bytes/doc | TreeDB total | baseline total |
| --- | --- | ---: | ---: | ---: | ---: |

### Notes

- State the main win or loss.
- State whether the result is post-insert, post-checkpoint, partial
  maintenance, or full maintenance.
- State any likely bottleneck and the profile artifact that supports it.
```

## Validation Before Publishing Harness Changes

For Mongo gateway harness changes:

```sh
bash -n scripts/mongo_gateway_compare.sh
GOWORK=off go test ./cmd/mongo_gateway_bench ./cmd/mongo_gateway_compare_report
```

For collection benchmark/report changes, run the narrow tests that match the
changed package, then one smoke matrix:

```sh
GOWORK=off go test ./cmd/collection_bench_matrix ./cmd/collection_bench_report
make collection-bench-matrix
OUT=/tmp/collection_matrix_smoke_$(date +%Y%m%d_%H%M%S)
./bin/collection-bench-matrix -out-dir "$OUT" -benchtime 1000x -count 1
```

When SQLite code paths are involved:

```sh
CGO_ENABLED=1 GOWORK=off go test -tags sqlite_bench ./TreeDB/collections
```

For raw TreeDB profile-output changes:

```sh
GOWORK=off go test ./cmd/unified_bench ./cmd/benchprof
make unified-bench benchprof
OUT=$(mktemp -d /tmp/gomap_profiles_smoke_XXXXXX)
./bin/unified-bench -dbs treedb -keys 200000 -profile wal_on_fast -profile-dir "$OUT" -progress=false
./bin/benchprof -profiles-dir "$OUT"
```
