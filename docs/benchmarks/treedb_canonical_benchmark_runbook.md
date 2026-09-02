# TreeDB Canonical Benchmark Runbook

This runbook defines the preferred benchmark entrypoints for TreeDB engine,
TreeDB collections, and Mongo-compatible collection workloads. Use it when a PR
or issue needs reproducible throughput, latency, profile, or disk-usage
evidence.

## One Command: Full Benchmark Run Report

Use this entrypoint when you need the full `TreeDB Benchmark Run Report` with
raw TreeDB engine profiles, TreeDB collections vs SQLite, Mongo API full sweep,
Mongo InsertMany producer scaling, Mongo client-mode load matrix, Mongo
reader/writer scaling, and the final `deep_report.html`.

The collections section runs a separate pprof capture pass for every
TreeDB/SQLite timed-matrix cell by default. Those profiles are attribution
artifacts; the canonical throughput rows still come from the unprofiled timed
benchmark pass.

```sh
scripts/treedb_benchmark_run_report.sh \
  --out /tmp/gomap_treedb_benchmark_run_$(date +%Y%m%d_%H%M%S) \
  --tier pr \
  --indexes "0 1 2" \
  --mongo-mode external \
  --mongo-uri mongodb://127.0.0.1:27017 \
  --title "TreeDB Benchmark Run Report"
```

The script writes the exact artifact layout consumed by
`cmd/benchmark_run_report`:

```text
$RUN_ROOT/
  HEAD.txt
  commands.log
  RUNBOOK.md
  raw_engine_full_matrix/
  collections_sqlite_canonical_1m/
    indexes_*/timed_matrix/*/profiles/collection_profile_manifest.json
    indexes_*/timed_matrix/*/profiles/{cpu.pprof,allocs.pprof,block.pprof,mutex.pprof,profile_go_test.txt}
  mongo_gateway_full_sweep_1m_expanded/
  mongo_client_mode_load_matrix_1m/
  mongo_gateway_load_scaling_1m/
  mongo_gateway_reader_writer_scaling_1m/
  deep_report.html
```

Size presets:

- `--tier smoke`: harness/report validation.
- `--tier pr`: reviewable PR evidence.
- `--tier large`: scale evidence, expected to take substantially longer.

Use `--skip-raw`, `--skip-collections`, `--skip-mongo`, `--skip-load-modes`,
`--skip-load-scaling`, or `--skip-scaling` only for resumable/debug runs.
Published reports should state any skipped section explicitly.

Use `--skip-collection-profiles` only when validating non-profile report logic;
it removes the collection pprof manifests from the final Profiling Follow-Up
section.

## VectorDBBench Cohere 1M

The canonical end-to-end vector-service benchmark is the
[Cohere Medium 1M VDBBench campaign](treedb_vectordbbench_cohere1m_c6i_dense_curve_2026-08-21.md).
Publish two TreeDB recall/QPS lines: FP32 HNSW graph traversal and scalar-u8
graph traversal with FP32 reranking, limited to its non-dominated points. Keep
scalar-u8-only and dominated rerank screening cells as supporting evidence.

## Principles

- Start every report with the git commit, branch, host, OS, Go version, command,
  and output directory.
- Treat `commands.log` as part of the evidence. A rendered report should make
  nonzero command exits visible near the top and should not look final when a
  section failed or was skipped.
- Keep fixture shape explicit: document count, key count, value size, batch size,
  secondary index count, document format, profile, client mode, concurrency, and
  maintenance mode.
- Report both throughput and disk. For latency-sensitive phases, include p50,
  p95, and p99 when the harness emits them.
- Keep TreeDB maintenance phases named exactly. Do not call a row "compacted"
  unless the row is from `offline_compact`, `full_leafgen_pack_gc`,
  `sqlite_vacuum`, or another explicitly documented full-maintenance phase.
- Keep raw artifacts. Markdown tables are for review; JSON, TSV, profiles, and
  data directories are the durable evidence.

## Benchmark Layers

TreeDB has four benchmark layers. Pick the lowest layer that answers the
question, then add higher layers only when the user-facing path matters.

1. Raw TreeDB engine: `cmd/unified_bench` plus `cmd/benchprof`.
2. TreeDB collections: `cmd/collection_bench_matrix` and the canonical
   TreeDB-vs-SQLite runner.
3. Mongo-compatible collections: `cmd/mongo_gateway_bench` and
   `scripts/mongo_gateway_compare.sh`.
4. End-to-end vector service: VDBBench through the TreeDB document-service
   adapter.

Raw engine results explain storage-engine ceilings and hot paths. Collection
results explain document storage, secondary indexes, maintenance, and SQLite
equivalence. Mongo gateway results explain Mongo-compatible ergonomics and the
cost of the official MongoDB driver, BSON wire handling, and gateway command
paths. VDBBench results explain the complete vector client, transport, service,
and index path at a recall target.

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

Standard profile capture runs the full raw-engine test matrix at 800k keys.
Omit `-test`; the default is `-test all`.

```sh
OUT_ROOT=/tmp/gomap_raw_engine_full_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT_ROOT"

for profile in wal_on_fast fast; do
  for checkpoint_mode in checkpoint_between_tests no_checkpoint_between_tests; do
    OUT="$OUT_ROOT/${profile}_${checkpoint_mode}"
    args=(
      -dbs treedb
      -keys 800000
      -profile "$profile"
      -path-label native-fastpath
      -profile-dir "$OUT"
      -progress=false
    )
    if [ "$checkpoint_mode" = checkpoint_between_tests ]; then
      args+=(-checkpoint-between-tests)
    fi
    ./bin/unified-bench "${args[@]}"
    ./bin/benchprof -profiles-dir "$OUT"
  done
done
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
OUT_ROOT=/tmp/collections_canonical_$(date +%Y%m%d_%H%M%S)
for indexes in 0 1 2; do
  ./scripts/bench_collections_canonical.sh \
    -out-dir "$OUT_ROOT/indexes_${indexes}" \
    -formats template-v1,bson,json \
    -indexes "$indexes"
done
```

The canonical runner emits:

- `benchmark_results.json`
- `benchmark_summary.md`
- `benchmark_matrix.csv`
- `timed_matrix/`
- `offline_compact/`
- `full_leafgen_pack_gc/` with per-format full leafgen/GC fixture summaries

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
- `<cell>/profiles/collection_profile_manifest.json`
- `<cell>/profiles/cpu.pprof`
- `<cell>/profiles/allocs.pprof`
- `<cell>/profiles/block.pprof`
- `<cell>/profiles/mutex.pprof`
- `<cell>/profiles/profile_go_test.txt`

Use `CGO_ENABLED=1` when the SQLite cells or SQLite native-column baselines are
part of the comparison.

## Collection Maintenance Semantics

The canonical collection maintenance phases are:

- `post_insert`: after insert plus the flush/checkpoint needed for correctness.
- `online_one_pass_maintenance`: bounded online maintenance; useful, but not a
  full compaction state.
- `offline_compact`: high-level `treemap compact <dir> -rw` comparison point.
- `full_leafgen_pack_gc`: full leaf-generation pack/GC comparison point.
- `sqlite_vacuum`: SQLite compacted baseline after `VACUUM`.

Fair post-insert comparison:

- TreeDB `post_insert`
- SQLite `post_insert`

Fair compacted-state comparison:

- TreeDB `offline_compact`
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
  --concurrent-read-kinds "id,email,range" \
  --concurrent-reader-sweep "1,2,4,8,16" \
  --concurrent-reads 50000 \
  --concurrent-writer-sweep "1,2,4,8,16" \
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
TREEDB_CLIENT_MODES="driver driver-find-raw driver-command driver-command-raw driver-unack direct raw-wire-tcp raw-wire-tcp-pipeline raw-wire" \
MONGO_CLIENT_MODES="driver driver-find-raw driver-command driver-command-raw driver-unack" \
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

This client-mode matrix is load-only by design. It should report insert
throughput and disk by client mode, and should not be used as reader/writer
scaling evidence.

MongoDB matrix config names intentionally keep `mongo` / `mongo_range_index`
for the ordinary single-driver case. The explicit `mongo_driver` row name is
reserved for multi-mode MongoDB client matrices so older comparison bundles and
new client-mode bundles remain readable side by side.

Interpret client modes as separate questions:

- `driver`: ordinary official MongoDB Go driver CRUD-helper path.
- `driver-find-raw`: official driver `Collection.Find` range-read path using
  `cursor.Current` raw BSON instead of decoding documents into `bson.M`.
- `driver-command`: official driver `RunCommand` insert path.
- `driver-command-raw`: official driver `RunCommand` with prebuilt raw BSON.
- `driver-unack`: official driver unacknowledged insert enqueue path plus
  post-load visibility wait.
- `direct`: TreeDB-only collection API path in the selected TreeDB document
  format. It emits the same phase names as the Mongo gateway benchmark while
  bypassing the MongoDB driver, sockets, and Mongo gateway command handling.
- `raw-wire-tcp`: TreeDB-only raw OP_MSG load over loopback TCP.
- `raw-wire-tcp-pipeline`: TreeDB-only raw TCP load plus pipelined age range
  `find` requests; use it to measure how much single-connection request/response
  latency can be hidden without the official driver. The default pipeline depth
  is `128`, which keeps enough requests queued for the gateway's buffered
  response coalescing to amortize write syscalls.
- `raw-wire`: TreeDB-only in-process raw OP_MSG load.

Use `driver` for user-visible Mongo compatibility. Use `driver-find-raw` to
separate official-driver find/cursor overhead from application `bson.M` decode.
Use `driver-command-raw` for the fastest current acknowledged official-driver
load path. Use `direct` to separate collection-engine and storage-format
bottlenecks from Mongo compatibility overhead. Use raw-wire modes only to
estimate TreeDB gateway/server ceiling.

## InsertMany Producer Scaling

Use the load-scaling wrapper when the question is how bulk insert throughput
changes as InsertMany producer count increases. This is the load-scaling
counterpart to the fixed-producer index-count chart in the full sweep.

```sh
scripts/mongo_gateway_load_scaling_bench.sh \
  --out /tmp/gomap_mongo_load_scaling_$(date +%Y%m%d_%H%M%S) \
  --docs 100000 \
  --indexes "0 1 2" \
  --batch-size 1000 \
  --producers "1,2,4,8,16,32" \
  --mongo-mode docker \
  --timeout 120m
```

Producer/batch sizing rule:

- Effective producers are capped by load batch count:
  `effective_producers = min(requested_producers, ceil(documents / batch_size))`.
- For an uncapped producer-scaling chart, choose `documents` and `batch_size`
  so `ceil(documents / batch_size)` is at least the largest requested producer
  count.
- Reports keep both requested and effective producers visible. If a small smoke
  fixture has only two batches, the 16- and 32-producer rows are useful harness
  checks but should not be interpreted as uncapped scaling evidence.

Interpret the Mongo-compatible sections as separate questions:

- Full sweep load chart: fixed producer count and batch size for the broader
  read/range/update evidence bundle. Use it for index-count penalty and storage
  comparisons under the canonical bulk-load setup.
- InsertMany producer scaling: load-only producer-count sweep. Use it to see
  whether TreeDB or MongoDB saturates as insert producers increase.
- Client-mode load matrix: load-only client-path comparison. Use it to separate
  official-driver, command, direct, and raw-wire overhead.
- Reader/writer scaling: post-load operation scaling. Use it for point reads,
  range reads, and updates versus client count, not for bulk-load scaling.

## Reader and Writer Scaling

Use the scaling wrapper when the question is concurrency plateau, update cost,
or indexed-field update overhead. Run the index-count loop when reader/writer
scaling is part of a PR evidence bundle:

```sh
OUT_ROOT=/tmp/gomap_mongo_gateway_scaling_$(date +%Y%m%d_%H%M%S)
for indexes in 0 1 2; do
  scripts/mongo_gateway_scaling_bench.sh \
    --out "$OUT_ROOT/indexes_${indexes}" \
    --docs 1000000 \
    --indexes "$indexes" \
    --batch-size 10000 \
    --insert-producers 8 \
    --writers "1 2 4 8 16 32" \
    --readers "1 2 4 8 16 32" \
    --concurrent-writes 80000 \
    --concurrent-reads 80000 \
    --include-mongo \
    --mongo-uri mongodb://127.0.0.1:27017
done
```

Omit `--include-mongo` for a TreeDB-only scaling ceiling check. Add
`--update-indexed-field` to update `city` and exercise secondary-index
maintenance instead of non-indexed document updates.

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

When a run produces the canonical artifact directories under one root, generate
the full human-readable HTML report before posting or reviewing results:

```sh
go run ./cmd/benchmark_run_report \
  -run-root "$RUN_ROOT" \
  -out "$RUN_ROOT/deep_report.html" \
  -title "TreeDB Canonical Benchmark Report"
```

The deep report preserves run status, full raw-engine, collection, Mongo
full-sweep, client-mode, InsertMany producer-scaling, and reader/writer scaling
tables. It renders inline SVG charts for TreeDB-vs-MongoDB load, disk, index
retention, insert-producer scaling, read fanout, writer scaling, and client-mode
load comparisons. The fixed bulk-load charts should show document count, batch
size, requested producers, effective producers, load batch count, and storage
basis so readers do not have to inspect raw TSV files to understand the
measurement setup.

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
./bin/unified-bench -dbs treedb -keys 200000 -profile wal_on_fast -path-label native-fastpath -profile-dir "$OUT" -progress=false
./bin/benchprof -profiles-dir "$OUT"
```
