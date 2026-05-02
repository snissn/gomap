# Mongo Gateway Benchmark Harness

`mongo_gateway_bench` runs the same MongoDB-driver workload against either the
TreeDB Mongo gateway or a MongoDB server. It is intended to make the first
Mongo-compatible product benchmarks reproducible, especially ops/sec and disk
usage comparisons.

## Build

```sh
GOWORK=off go build ./cmd/mongo_gateway_bench
```

## TreeDB Target

```sh
GOWORK=off go run ./cmd/mongo_gateway_bench \
  -target treedb \
  -treedb-dir /tmp/treedb-mongo-bench \
  -keep-treedb-dir \
  -documents 10000 \
  -batch-size 1000 \
  -insert-producers 4 \
  -mongo-max-pool-size 32 \
  -mongo-max-connecting 8 \
  -reads 10000 \
  -range-reads 1000 \
  -updates 1000 \
  -concurrent-readers 8 \
  -concurrent-reads 10000 \
  -concurrent-writers 4 \
  -concurrent-writes 1000 \
  -secondary-indexes 2 \
  -format json
```

For `-target treedb`, the command starts an in-process TreeDB Mongo gateway on a
loopback listener, connects with the official MongoDB Go driver, and reports
the sum of regular file sizes for the TreeDB directory after load, after a final
checkpoint, and by default after the full TreeDB maintenance stack. If
`-treedb-dir` is provided and already exists, the harness
removes and recreates it before loading deterministic fixtures so repeated runs
are reproducible. Obvious unsafe reset targets such as root, the current
checkout, the temp directory itself, a home directory, or an immediate child of
a home directory are rejected.

The default `-client-mode driver` uses the official MongoDB Go driver
`Collection.InsertMany` path for the load phase and all later phases.
`-client-mode driver-command` still uses the official driver but sends the load
phase as a raw `insert` command through `Database.RunCommand`, avoiding the
driver's `InsertMany` `_id` discovery and `InsertedIDs` bookkeeping. It is useful
for isolating driver CRUD-helper overhead and works against both TreeDB and
MongoDB targets. `-client-mode driver-command-raw` also uses `RunCommand`, but
passes a prebuilt raw BSON insert command to reduce driver-side command encoding
when `-prebuild-documents` is enabled. `-client-mode driver-unack` uses official-driver
`InsertMany` with unacknowledged write concern; its sampled load metric is
client enqueue cost, while the phase waits for the final inserted `_id` to
become visible before reporting wall ops/sec. `-client-mode raw-wire` is
TreeDB-only and calls the in-process gateway directly with raw OP_MSG document
sequences. `-client-mode raw-wire-tcp` sends the same raw OP_MSG traffic over
the gateway's loopback listener, isolating TreeDB gateway network/wire-server
cost from Mongo Go driver cost. Raw-wire modes use raw OP_MSG
document sequences for the insert load phase while keeping setup and later
read/update phases on the driver. Use raw-wire mode to estimate the
gateway/server ceiling without the driver's per-document marshal and `_id`
discovery overhead; use driver mode for user-visible Mongo compatibility
throughput.

When `-prebuild-documents` is enabled, the harness builds both structured BSON
documents and raw BSON bytes before the measured workload. `driver-command` and
`raw-wire` reuse the raw bytes during the load phase so their insert-call timing
does not include fixture BSON marshaling.

Use `-insert-producers N` to split the insert load phase across producer
goroutines. The effective producer count is capped at the number of insert
batches so small runs do not open unused clients. Official-driver modes share one
`mongo.Client`, so
`-mongo-max-pool-size`, `-mongo-min-pool-size`, and `-mongo-max-connecting`
control the driver pool used by those producers. When `-mongo-max-pool-size` is
left unset, validation treats the driver default max pool size as 100 for
`-mongo-min-pool-size` checks. `raw-wire-tcp` opens one fastclient connection per
effective producer, and `raw-wire` uses one in-process wire owner per effective
producer. JSON output includes `effective_producers` and `producer_results` for
the load phase plus `mongo_pool_stats_after_load` and `mongo_pool_stats_final`
when the official driver pool is involved.

The TreeDB benchmark target defaults are intended to match the optimized
collection benchmark profile:

- `-treedb-profile wal_on_fast`
- `-treedb-document-format template-v1`
- `-treedb-data-root-storage compressed`
- `-treedb-index-state-root-storage compressed`
- `-treedb-index-root-storage compressed`
- `-treedb-buffered-indexed-write-max-documents 64000`
- `-treedb-buffered-indexed-write-max-root-runs 4096`
- `-treedb-maintenance full`
- `-client-mode driver`

The TreeDB target always opens with outer leaves in the leaf value log and the
cached leaf-log backend, so collection and secondary-index roots exercise the
same leaf-vlog path as the optimized collection benchmarks. The `full`
maintenance mode reports each post-load compaction step: value-log rewrite,
value-log GC, leaf-generation pack, leaf-generation GC, and offline index
vacuum. The final vacuum closes the benchmark gateway before rewriting
`index.db`, matching the documented compacted-state maintenance command. Use
`-treedb-maintenance checkpoint` to reproduce the older checkpoint-only disk
metric, or `none` to skip final TreeDB disk reporting.

`-treedb-document-format` accepts `json`, `template-v1`, and `bson`. BSON mode
stores Mongo wire documents as native BSON collection records, avoiding the
canonical Extended JSON bridge used by the JSON/template-v1 gateway paths.
Use `-treedb-buffered-indexed-write-max-documents`,
`-treedb-buffered-indexed-write-max-bytes`, and
`-treedb-buffered-indexed-write-max-root-runs` to reproduce indexed write-domain
auto-flush threshold experiments. The benchmark report records the effective
normalized collection thresholds after index creation. For document thresholds,
`0` means use the collection default. For byte and root-run thresholds, `0`
disables that trigger unless all indexed-write thresholds are otherwise left at
their native defaults.

## MongoDB Target

```sh
GOWORK=off go run ./cmd/mongo_gateway_bench \
  -target mongo \
  -mongo-uri mongodb://127.0.0.1:27017 \
  -documents 10000 \
  -batch-size 1000 \
  -insert-producers 4 \
  -mongo-max-pool-size 32 \
  -mongo-max-connecting 8 \
  -reads 10000 \
  -range-reads 1000 \
  -updates 1000 \
  -concurrent-readers 8 \
  -concurrent-reads 10000 \
  -concurrent-writers 4 \
  -concurrent-writes 1000 \
  -secondary-indexes 2 \
  -format json
```

For `-target mongo`, the command connects to the supplied URI, drops the
benchmark database by default, and reports `dbStats` fields after load and at
the end of the run.

## Reusable Comparison Harness

Use `scripts/mongo_gateway_compare.sh` when you want a complete
TreeDB-vs-MongoDB matrix bundle instead of hand-running each target. The
harness builds `mongo_gateway_bench`, runs matching TreeDB and MongoDB cells,
writes raw JSON for every target, records physical `du` bytes where available,
and generates a Markdown report plus TSV summary.

To compare every TreeDB document format in one bundle:

```sh
TREEDB_DOCUMENT_FORMATS="json template-v1 bson" scripts/mongo_gateway_compare.sh
```

To include the raw-wire TreeDB insert load path and the driver command path
beside the normal MongoDB Go driver `InsertMany` path:

```sh
TREEDB_DOCUMENT_FORMATS="bson" \
TREEDB_CLIENT_MODES="driver driver-command driver-command-raw driver-unack raw-wire-tcp raw-wire" \
scripts/mongo_gateway_compare.sh
```

```sh
scripts/mongo_gateway_compare.sh \
  --out /tmp/gomap_mongo_gateway_compare \
  --docs "1000 10000" \
  --indexes "0 2" \
  --concurrent-readers 8 \
  --concurrent-reads 10000 \
  --concurrent-writers 4 \
  --concurrent-writes 1000 \
  --insert-producers 4 \
  --mongo-max-pool-size 32 \
  --mongo-max-connecting 8 \
  --mongo-mode docker
```

Docker mode starts a fresh MongoDB container and isolated data directory per
matrix cell, which makes MongoDB physical disk usage reproducible enough for
local comparisons. If you already have a MongoDB server, use:

```sh
scripts/mongo_gateway_compare.sh \
  --mongo-mode external \
  --mongo-uri mongodb://127.0.0.1:27017
```

The bundle contains:

- `report.md`: reviewable Markdown with highlights, disk bytes/doc, ops/sec
  ratios, and raw input paths.
- `summary.tsv`: machine-readable per-phase comparison rows.
- `matrix.tsv`: target/config/document/index/raw-json/physical-byte index.
- `raw/*.json`: unmodified `mongo_gateway_bench -format json` output.
- `profiles/`: per-phase TreeDB pprof artifacts when `--profile-treedb` is
  used.
- `treedb_data/` and, in Docker mode, `mongodb_data/`: final data directories
  for post-run inspection.

The first checked-in bundle is
`docs/benchmarks/mongo_gateway_compare_2026-04-29/`.

To regenerate only the report from an existing bundle:

```sh
GOWORK=off go run ./cmd/mongo_gateway_compare_report \
  -matrix /tmp/gomap_mongo_gateway_compare/matrix.tsv \
  -report /tmp/gomap_mongo_gateway_compare/report.md \
  -summary /tmp/gomap_mongo_gateway_compare/summary.tsv
```

Useful overrides:

- `DOCS_LIST="1000 10000 100000"`
- `INDEXES_LIST="0 1 2"`
- `TREEDB_CLIENT_MODES="driver driver-command driver-command-raw driver-unack raw-wire-tcp raw-wire"`
- `READS=50000`, `RANGE_READS=5000`, `UPDATES=5000`
- `DELETES=1000`
- `RANGE_INDEX=true` or `--range-index` to create `age_1` and report
  `age_range_indexed_limit_10` instead of scan fallback.
- `PROFILE_TREEDB=true` or `--profile-treedb` to pass `-profile-dir` for every
  TreeDB cell and retain per-phase profiles under the bundle's `profiles/`
  directory.
- `CONCURRENT_READERS=16`, `CONCURRENT_READS=50000`
- `CONCURRENT_WRITERS=8`, `CONCURRENT_WRITES=10000`
- `BATCH_SIZE=1000`
- `MONGO_IMAGE=mongo:8`

For a larger MongoDB comparison that keeps the 1M-doc cell bounded enough for a
local run, use explicit operation counts:

```sh
BATCH_SIZE=5000 scripts/mongo_gateway_compare.sh \
  --out /tmp/gomap_mongo_gateway_compare_large \
  --docs "100000 1000000" \
  --indexes "2" \
  --reads 50000 \
  --range-reads 5000 \
  --range-index \
  --updates 5000 \
  --concurrent-readers 16 \
  --concurrent-reads 50000 \
  --concurrent-writers 8 \
  --concurrent-writes 10000 \
  --timeout 120m
```

## Manual Matrix

Run both targets with the same values and keep the raw JSON outputs:

```sh
for docs in 1000 10000 100000; do
  for indexes in 0 1 2; do
    GOWORK=off go run ./cmd/mongo_gateway_bench \
      -target treedb \
      -treedb-dir "/tmp/treedb-mongo-${docs}-${indexes}" \
      -keep-treedb-dir \
      -documents "$docs" \
      -reads "$docs" \
      -range-reads "$((docs / 10))" \
      -updates "$((docs / 10))" \
      -concurrent-readers 8 \
      -concurrent-reads "$((docs / 10))" \
      -concurrent-writers 4 \
      -concurrent-writes "$((docs / 20))" \
      -secondary-indexes "$indexes" \
      -format json > "treedb-${docs}-${indexes}.json"

    GOWORK=off go run ./cmd/mongo_gateway_bench \
      -target mongo \
      -documents "$docs" \
      -reads "$docs" \
      -range-reads "$((docs / 10))" \
      -updates "$((docs / 10))" \
      -concurrent-readers 8 \
      -concurrent-reads "$((docs / 10))" \
      -concurrent-writers 4 \
      -concurrent-writes "$((docs / 20))" \
      -secondary-indexes "$indexes" \
      -format json > "mongo-${docs}-${indexes}.json"
  done
done
```

The initial workload phases are:

- `load_insert_many`: batched document inserts. The exact client call depends
  on `client_mode`: `InsertMany` for `driver`, `RunCommand({insert,
  documents})` for `driver-command`, `RunCommand` with a prebuilt raw BSON
  command for `driver-command-raw`, unacknowledged `InsertMany` plus a post-load
  visibility wait for `driver-unack`, and raw OP_MSG document sequences for
  `raw-wire`/`raw-wire-tcp`. When `-insert-producers` is greater than 1, this
  phase reports aggregate wall-clock throughput and per-producer call latency in
  `producer_results`.
- `id_find_one`: point lookup by `_id`.
- `email_find_one`: point lookup by the `email` field; emitted only when the
  email secondary index is part of the cell.
- `age_range_scan_limit_10` / `age_range_indexed_limit_10`: bounded range query
  with `limit: 10`; operations count range queries, not returned documents. The
  indexed variant is emitted when `-range-index` creates `age_1`.
- `id_update_set`: `$set` update by `_id`.
- `concurrent_id_find_one_rN`: total `_id` point reads split across `N`
  goroutines.
- `concurrent_id_update_set_wN`: total `$set` updates split across `N`
  goroutines.
- `id_delete_one`: optional deletes; disabled unless `-deletes` is non-zero.

Update phases change only non-indexed fields by default.
`-update-indexed-field` requires `-secondary-indexes=2` so the city index exists
and the indexed `city` field changes, exercising secondary-index maintenance in
the update path.
`-range-index` creates an additional `age_1` index so the age range-read phase
materially exercises indexed range planning instead of the bounded scan
fallback.

Latency samples are per MongoDB driver/gateway call. Update phases build the
filter and update document before starting the sampled timer, so update samples
focus on the driver/gateway/DB call rather than request construction. `ops_sec`
is normalized by document count over the whole phase loop; `sampled_ops_sec` and
`sampled_ns_per_op` are derived from the aggregate sampled call duration. Prefer
sampled values when investigating gateway/client overhead with prebuilt
fixtures, and wall `ops_sec` when measuring the full benchmark loop. Insert
latency percentiles are per batch call. Range-query samples include cursor
materialization with `cursor.All`.

`mongo_pool_stats_after_load` is reset immediately before the insert load phase,
so its checkout counters describe the measured insert phase rather than setup or
index creation. `mongo_pool_stats_final` is cumulative from the load phase
through the later read/update/delete phases. Pool checkout latency percentiles are
computed from a bounded sample to keep high-concurrency benchmark overhead
predictable; checkout counts and aggregate checkout duration still cover every
recorded checkout event.

Use `-timeout 0` to run without an overall benchmark deadline.

The package test `TestTreeDBProfileSmokeFastAndWALOnFast` runs a small write-only
TreeDB gateway smoke against both `fast` and `wal_on_fast` to catch large
profile regressions without making the smoke a replacement for the full matrix.

## Phase Pprof Artifacts

Use `-profile-dir` when a full benchmark run exposes a scaling wall and the
next step is to inspect the TreeDB or gateway hot path. The command writes one
CPU profile per measured phase, plus heap, allocs, block, mutex, and goroutine
profiles captured after each phase. It also writes `profile_manifest.json` and
`benchmark_result.json` into the same directory so the profile files can be tied
back to the exact benchmark config and phase throughput.
The profile directory must be empty at startup; use a fresh `mktemp -d`
directory for each run so stale artifacts cannot be mixed into a new capture.

CPU profiles are phase-scoped. Heap, allocs, block, mutex, and goroutine
profiles are runtime snapshots captured at phase end; block, mutex, and allocs
profiles are cumulative within the benchmark process rather than reset between
phases.

```sh
OUT=$(mktemp -d /tmp/gomap_mongo_gateway_pprof_XXXXXX)
GOWORK=off go run ./cmd/mongo_gateway_bench \
  -target treedb \
  -client-mode driver-command-raw \
  -treedb-document-format bson \
  -documents 1000000 \
  -batch-size 5000 \
  -insert-producers 8 \
  -reads 0 \
  -range-reads 0 \
  -updates 0 \
  -concurrent-writers 8 \
  -concurrent-writes 80000 \
  -secondary-indexes 2 \
  -prebuild-documents \
  -treedb-maintenance none \
  -profile-dir "$OUT" \
  -format json
```

Useful first-pass commands:

```sh
GOWORK=off go build -o ./bin/mongo_gateway_bench ./cmd/mongo_gateway_bench
go tool pprof -top -cum ./bin/mongo_gateway_bench "$OUT/load_insert_many.cpu.pprof"
go tool pprof -top -cum ./bin/mongo_gateway_bench "$OUT/concurrent_id_update_set_w8.cpu.pprof"
go tool pprof -top -cum ./bin/mongo_gateway_bench "$OUT/concurrent_id_update_set_w8.mutex.pprof"
```

By default, profiling mode enables block profiling at rate `1` and mutex
profiling at fraction `5`. Use `-profile-block-rate 0` or
`-profile-mutex-fraction 0` to disable either profile. Runtime traces are larger
and are off by default; add `-profile-trace` when scheduler-level detail is
needed. Heap profiles are captured without forcing a garbage collection by
default so measured phases keep their normal heap state; add `-profile-heap-gc`
when you specifically want post-GC heap snapshots.

For insert-scaling investigations, run the same command repeatedly with
`-insert-producers 1`, `2`, `4`, `8`, and `16` while keeping `-documents`,
`-batch-size`, `-client-mode`, and document format constant. For write-contention
investigations, keep the load shape fixed and vary `-concurrent-writers` /
`-concurrent-writes`.

## Reader/Writer Scaling Wrapper

Use `scripts/mongo_gateway_scaling_bench.sh` for a repeatable reader/writer
scaling sweep. It runs `mongo_gateway_bench` for each reader and writer count,
writes raw JSON for every cell, then feeds the matrix into
`mongo_gateway_compare_report` so the output shape matches the normal
TreeDB-vs-MongoDB comparison bundle.

```sh
scripts/mongo_gateway_scaling_bench.sh \
  --out /tmp/gomap_mongo_gateway_scaling \
  --docs 100000 \
  --indexes 2 \
  --batch-size 10000 \
  --insert-producers 8 \
  --writers "1 2 4 8 16" \
  --readers "1 2 4 8 16" \
  --concurrent-writes 80000 \
  --concurrent-reads 80000
```

The default sweep is TreeDB-only, using `wal_on_fast`, native BSON collection
storage, `driver-command-raw`, prebuilt BSON documents, and no final maintenance
so the measured phases focus on concurrency. Add
`--include-mongo --mongo-uri mongodb://127.0.0.1:27017` to run matching cells
against an existing MongoDB server. The bundle contains `report.md`,
`summary.tsv`, `matrix.tsv`, raw JSON, and a README that records the kept
TreeDB data path for profile follow-up. Depending on where `--out` is placed,
that kept TreeDB data directory may be outside the bundle directory.
Add `--update-indexed-field` to make writer-scaling cells update `city` and
therefore measure secondary-index update/publish work.

## Gateway Profiling Benchmarks

The package also includes benchmark-only entry points for isolating Mongo
gateway overhead from the underlying collection insert path:

```sh
OUT=$(mktemp -d /tmp/gomap_mongo_gateway_profile_XXXXXX)
MONGO_GATEWAY_PROFILE_BENCH_BATCH_SIZE=10000 \
GOWORK=off go test ./cmd/mongo_gateway_bench \
  -run '^$' \
  -bench '^(BenchmarkTreeDBGatewayLoadBSONIndexes2|BenchmarkTreeDBGatewayLoadGeneratedIDBSONIndexes2|BenchmarkTreeDBGatewayLoadObjectIDBSONIndexes2|BenchmarkTreeDBGatewayLoadUnackBSONIndexes2|BenchmarkTreeDBGatewayRunCommandLoadBSONIndexes2|BenchmarkTreeDBGatewayRunRawCommandLoadBSONIndexes2|BenchmarkTreeDBGatewayRawWireLoadBSONIndexes2|BenchmarkTreeDBGatewayRawWireTCPLoadBSONIndexes2|BenchmarkDirectCollectionLoadBSONIndexes2|BenchmarkDirectCollectionConcurrentUpdateBSONIndexes2|BenchmarkClientBSONBatchEncode)$' \
  -benchtime 2000000x \
  -count 1 \
  -timeout 0 \
  -benchmem \
  -cpuprofile "$OUT/cpu.pprof" \
  -memprofile "$OUT/mem.pprof"
```

The benchmark shapes are intentionally different:

- `BenchmarkTreeDBGatewayLoadBSONIndexes2` uses the official MongoDB Go driver
  against the in-process TreeDB gateway.
- `BenchmarkTreeDBGatewayLoadGeneratedIDBSONIndexes2` uses the same official
  driver path with documents that omit `_id`, forcing the driver to generate
  ObjectIDs and avoiding its expensive explicit-`_id` decode path. This is a
  diagnostic for workloads that do not require caller-supplied primary keys.
- `BenchmarkTreeDBGatewayLoadObjectIDBSONIndexes2` uses explicit ObjectID
  primary keys. If this remains close to the explicit string `_id` benchmark,
  the cost is the driver's explicit-`_id` bookkeeping rather than string `_id`
  encoding.
- `BenchmarkTreeDBGatewayLoadUnackBSONIndexes2` uses official-driver
  `InsertMany` with unacknowledged writes. It measures client enqueue cost, not
  completed durable load throughput, and is only a diagnostic for response-path
  overhead.
- `BenchmarkTreeDBGatewayRunCommandLoadBSONIndexes2` uses the official MongoDB
  Go driver `RunCommand` insert path against the in-process TreeDB gateway,
  bypassing `InsertMany` `_id` extraction while still using the driver transport.
- `BenchmarkTreeDBGatewayRunRawCommandLoadBSONIndexes2` sends a prebuilt raw
  BSON insert command through official-driver `RunCommand`, minimizing driver
  command encoding while preserving official-driver transport.
- `BenchmarkTreeDBGatewayRawWireLoadBSONIndexes2` sends raw OP_MSG insert
  document sequences to the gateway, bypassing the Go driver's document
  marshal and `_id` discovery work while still exercising gateway wire parsing
  and command handling.
- `BenchmarkTreeDBGatewayRawWireTCPLoadBSONIndexes2` sends the same raw OP_MSG
  insert document sequences over the gateway's TCP listener, isolating network
  and connection-serving cost from the official driver's CRUD-helper cost.
- `BenchmarkDirectCollectionLoadBSONIndexes2` inserts the same BSON document
  shape through the collection API without the Mongo gateway.
- `BenchmarkDirectCollectionConcurrentUpdateBSONIndexes2` preloads a BSON
  collection, then runs concurrent `_id` updates through `Collection.Update`
  without the Mongo gateway. This is useful when comparing gateway update
  profiles with the storage/update path directly. The benchmark enables
  collection-manager detailed update timing for its measured phase and reports
  update attribution metrics such as `update_current_read_ns/doc`,
  `update_callback_ns/doc`, `update_index_state_extract_ns/doc`,
  `update_primary_run_ns/doc`, `update_secondary_runs_ns/doc`,
  `update_buffer_stage_ns/doc`, `update_publish_ns/doc`, and
  `update_items/batch` from the collection manager's measured-phase counters.
- `BenchmarkClientBSONBatchEncode` measures client-side BSON document encoding
  alone.

The benchmark-only helpers accept these optional environment variables:
`MONGO_GATEWAY_PROFILE_BENCH_BATCH_SIZE`,
`MONGO_GATEWAY_PROFILE_BENCH_UPDATE_DOCUMENTS`, and
`MONGO_GATEWAY_PROFILE_BENCH_WRITERS`.

Use the official-driver row for user-visible Mongo compatibility throughput, the
driver-command rows to quantify the driver's CRUD-helper overhead, the raw-wire
rows to estimate the gateway/server ceiling, and the direct collection row to
estimate the storage-engine ceiling for the same document shape. For
acknowledged high-throughput ingest through the public MongoDB Go driver,
`driver-command-raw` is the fastest current path because it keeps the official
driver transport while bypassing `InsertMany`'s explicit-`_id` discovery and
`InsertedIDs` bookkeeping.

## Interpreting Results

`-secondary-indexes 2` creates `email_1` and `city_1`. The age range phase is a
bounded scan unless `-range-index` is set; benchmark output names the phase
`age_range_scan_limit_10` or `age_range_indexed_limit_10` so reports can
separate fallback cost from indexed range-search cost.

For TreeDB, prefer `treedb_disk_after_maintenance.total_bytes` when present, and
use `treedb_disk_after_checkpoint.total_bytes` for checkpoint-only runs. The
child path breakdown, especially `index.db`, `leaf_vlog`, and `value_vlog`,
shows where bytes landed. These are logical bytes: the sum of regular file
sizes, not allocated physical disk usage including block allocation, sparse-file
effects, filesystem compression, or metadata. Capture `du` separately when
physical on-disk usage is the comparison target.

For MongoDB, compare `mongodb_stats_final.storageSize`,
`mongodb_stats_final.indexSize`, and `mongodb_stats_final.totalSize`. If the
MongoDB server is local, also capture a filesystem `du` of the database path for
the final report. The comparison harness treats isolated physical `du` as the
preferred local disk metric because small WiredTiger workloads can report
`dbStats.totalSize` values that are much smaller than the actual data directory.

The benchmark intentionally keeps BSON format questions visible. If TreeDB load
or read phases spend a meaningful amount of time re-encoding documents in future
profiles, that is evidence for adding a native BSON collection document format
beside JSON and template-v1.
