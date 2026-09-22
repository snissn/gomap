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
  -concurrent-read-kinds id,email,range \
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
`Collection.InsertMany` path for the load phase and app-style decoded read
phases. `-client-mode driver-find-raw` still uses official-driver
`Collection.Find` for range reads, but iterates `cursor.Current` as raw BSON
instead of decoding documents into `bson.M`; use it to isolate official-driver
find/cursor overhead from application decode overhead. `-client-mode
driver-command` still uses the official driver but sends the load
phase as a raw `insert` command through `Database.RunCommand`, avoiding the
driver's `InsertMany` `_id` discovery and `InsertedIDs` bookkeeping. It is useful
for isolating driver CRUD-helper overhead and works against both TreeDB and
MongoDB targets. `-client-mode driver-command-raw` also uses `RunCommand`, but
passes a prebuilt raw BSON insert command to reduce driver-side command encoding
when `-prebuild-documents` is enabled; its age range-read phase also uses a raw
`find` command and parses `cursor.firstBatch` as raw BSON instead of decoding
documents into `bson.M`. `-client-mode driver-unack` is a MongoDB-target-only
diagnostic that uses official-driver `InsertMany` with unacknowledged write
concern. TreeDB configurations reject this mode explicitly because the
standalone gateway rejects `w:0` before mutation. `-client-mode raw-wire` is
TreeDB-only and calls the in-process gateway directly with raw OP_MSG document
sequences. `-client-mode raw-wire-tcp` sends the same raw OP_MSG traffic over
the gateway's loopback listener, isolating TreeDB gateway network/wire-server
cost from Mongo Go driver cost. `-client-mode raw-wire-tcp-pipeline` uses the
same raw TCP load path and pipelines age range `find` requests on one connection
up to `-raw-wire-tcp-pipeline-depth` (default `128`), which isolates
single-connection request/response latency from server execution. The gateway
coalesces already-buffered pipelined responses before writing, so deeper
pipelines can keep the socket full across server write cycles without changing
the server's per-write coalescing cap. Raw-wire modes use raw OP_MSG
document sequences for the insert load phase and raw OP_MSG `find` requests for
the age range-read phase while keeping setup and non-range phases on the driver.
`-client-mode direct` is a TreeDB-only path
that calls `collections.Collection` directly for the same phase names, using the
selected `-treedb-document-format` (`json`, `template-v1`/`collections-v1`, or
`bson`) while bypassing the MongoDB Go driver, loopback sockets, and Mongo gateway
command/response handling. Use direct mode to answer whether a slow Mongo API
phase is already slow in the collection engine and selected storage format; use
raw-wire mode to estimate the gateway/server ceiling without the driver's
per-document marshal and `_id` discovery overhead; use driver mode for
user-visible Mongo compatibility throughput.

`-client-mode native-wire-inproc` and `-client-mode native-wire-tcp` are
TreeDB-only native protocol load paths. They use TreeDB collection IDs and
stored document bytes over the native-wire server/client instead of Mongo OP_MSG,
then keep setup and later read/update phases on the Mongo compatibility driver
so they remain comparable with the existing benchmark sweep.

When `-prebuild-documents` is enabled, the harness builds both structured BSON
documents and raw BSON bytes before the measured workload. `driver-command` and
`raw-wire` reuse the raw bytes during the load phase so their insert-call timing
does not include fixture BSON marshaling. Direct mode also reuses prebuilt raw
BSON-derived stored documents for direct collection inserts.

### Local Route Evidence Mode

`-route-mode ring` is a TreeDB-only, official-driver evidence mode for
deterministic local token-ring routing. It does not enable network forwarding,
fanout/splitting, remote leader forwarding, scatter-gather reads,
cross-shard transactions, global unique-index semantics, shard keys beyond
explicit `_id`, or production horizontal scaleout. The benchmark still performs
local Mongo gateway `InsertOne` calls; before each single-document insert it
runs route preflight against a bench-local token/ring catalog and records which
logical group, leader hint, and token partition the explicit `_id` would route
to. N-group runs also issue a multi-token preflight probe that is expected to
fail with the existing fanout rejection before submit.

Use a 1-group/1-partition run as the local baseline:

```sh
GOWORK=off go run ./cmd/mongo_gateway_bench \
  -target treedb \
  -route-mode ring \
  -route-groups 1 \
  -route-partitions 1 \
  -documents 10000 \
  -batch-size 100 \
  -insert-producers 4 \
  -reads 0 -range-reads 0 -updates 0 -deletes 0 \
  -secondary-indexes 0 \
  -prebuild-documents \
  -treedb-maintenance none \
  -format json
```

Compare it with an N-group/N-partition local route run:

```sh
GOWORK=off go run ./cmd/mongo_gateway_bench \
  -target treedb \
  -route-mode ring \
  -route-groups 4 \
  -route-partitions 16 \
  -documents 10000 \
  -batch-size 100 \
  -insert-producers 4 \
  -reads 0 -range-reads 0 -updates 0 -deletes 0 \
  -secondary-indexes 0 \
  -prebuild-documents \
  -treedb-maintenance none \
  -format json
```

JSON output always includes a top-level `route_mode` field. When ring mode is
enabled, `route_group_count` and `route_partition_count` are also emitted and
`route_evidence` contains `evidence_scope=local_preflight`,
`write_shape=single_document_insert`, `local_only=true`,
`production_scale_eligible=false`, `preflight_success`, `fanout_rejected`,
`group_hits`, `leader_hits`, `partition_hits`, and any fanout rejection text.
The result also emits
`production_route_evidence_status=unavailable_local_preflight_only` and omits
`production_route_evidence`. Treat those fields as local deterministic routing
distribution evidence only, not as a cluster throughput or scaleout claim.

`-route-mode production` retains the internal route/submit/apply harness, but
the public Mongo token/ring write path now fails closed before submit. The
gateway does not yet bind authoritative collection and index metadata to the
exact owner route proof, and a gateway-local indexless collection is not
authoritative for a remote owner. Consequently production-mode runs currently
return that policy error and must not emit or claim routed-commit,
remote-redirect, or remote-forward performance evidence.

The legacy production evidence fields and static harness remain for the future
owner-bound implementation and schema compatibility. They do not describe an
enabled public path in the current build. `-route-mode ring` remains
local-preflight-only evidence, while collection-placement writes remain the
enabled routed mutation shape outside this token/ring benchmark mode.

Use `-insert-producers N` to split the insert load phase across producer
goroutines. The effective producer count is capped at the number of insert
batches so small runs do not open unused clients. Official-driver modes share one
`mongo.Client`, so
`-mongo-max-pool-size`, `-mongo-min-pool-size`, and `-mongo-max-connecting`
control the driver pool used by those producers. When `-mongo-max-pool-size` is
left unset, validation treats the driver default max pool size as 100 for
`-mongo-min-pool-size` checks. `raw-wire-tcp` and
`raw-wire-tcp-pipeline` open one fastclient connection per effective producer
for the load phase, and `raw-wire` uses one in-process wire owner per effective
producer. Raw-wire TreeDB modes use raw find commands for both `_id` and
secondary-index `email` read phases so client-mode labels do not hide ordinary
Mongo driver indexed-find overhead. JSON output includes `effective_producers`
and `producer_results` for the load phase plus `mongo_pool_stats_after_load` and
`mongo_pool_stats_final` when the official driver pool is involved.

The TreeDB benchmark target defaults to the explicit benchmark-only no-WAL
ceiling because the default cell creates secondary indexes. Use
`command_wal_relaxed` for command-WAL coverage once collection catalog index
commands are supported for this workload:

- `-treedb-profile bench_unsafe`
- `-treedb-document-format template-v1`
- `-treedb-data-root-storage compressed`
- `-treedb-index-state-root-storage compressed`
- `-treedb-index-root-storage compressed`
- `-treedb-buffered-indexed-write-max-documents 0` (use the collection default:
  256000 for default async threshold publish; 96000 when async flush is
  disabled)
- `-treedb-buffered-indexed-write-max-root-runs 0` (explicit `0` disables this
  trigger; when this flag is omitted while document or byte thresholds are
  overridden, the tool keeps the matching root-run compatibility default)
- `-treedb-maintenance full`
- `-client-mode driver`

The TreeDB target always opens with outer leaves in the leaf value log and the
cached leaf-log backend, so collection and secondary-index roots exercise the
same leaf-vlog path as the optimized collection benchmarks. The default `full`
maintenance mode runs the high-level `CompactStorage` path first, then closes
the benchmark gateway and runs offline index vacuum to shrink `index.db` for
final-footprint reporting. Use `-treedb-maintenance checkpoint` to reproduce the
older checkpoint-only disk metric, or `none` to skip final TreeDB disk
reporting. No-index YCSB-style command-WAL comparisons are covered by
`scripts/ycsb_compare_mongodb_treedb.sh`; this indexed benchmark default should
move to command WAL after catalog index commands are implemented.

`-treedb-document-format` accepts `json`, `template-v1`/`collections-v1`, and `bson`. BSON mode
stores Mongo wire documents as native BSON collection records, avoiding the
canonical Extended JSON bridge used by the JSON/template-v1 gateway paths.
Use `-treedb-buffered-indexed-write-max-documents`,
`-treedb-buffered-indexed-write-max-bytes`, and
`-treedb-buffered-indexed-write-max-root-runs` to reproduce indexed write-domain
auto-flush threshold experiments. The benchmark report records the effective
normalized collection thresholds after index creation. For document thresholds,
`0` means use the collection default. For byte and root-run thresholds, `0`
disables that trigger unless all indexed-write thresholds are otherwise left at
their native defaults. For compatibility with older threshold experiments, if a
document or byte threshold is overridden and the root-run flag is omitted, the
tool fills in the matching root-run default; pass
`-treedb-buffered-indexed-write-max-root-runs 0` explicitly to keep root-run
flushing disabled in that case.

The Go profile benchmarks in `profile_bench_test.go` use the collection default
indexed async flush mode. They can force foreground threshold publish for
baseline comparisons, or override async queue limits for focused root-publish
experiments:

```sh
MONGO_GATEWAY_PROFILE_BENCH_DISABLE_BUFFERED_INDEXED_ASYNC_FLUSH=true \
go test ./cmd/mongo_gateway_bench \
  -run '^$' \
  -bench '^BenchmarkDirectCollectionConcurrentUpdateBSONIndexes2$' \
  -benchtime=100000x \
  -benchmem
```

```sh
MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH_MAX_QUEUED_UNITS=4 \
go test ./cmd/mongo_gateway_bench \
  -run '^$' \
  -bench '^BenchmarkDirectCollectionConcurrentUpdateBSONIndexes2$' \
  -benchtime=100000x \
  -benchmem
```

When enabled, those benchmark rows include `buffered_async_flush` and the
normalized `buffered_async_max_units` so they are not compared against
synchronous-threshold rows by accident. The concurrent update profile benchmark
times a final `FlushAll()` drain before reporting docs/sec, so async rows include
deferred indexed publish work rather than enqueue latency alone.

For update-combiner ingress experiments, set
`MONGO_GATEWAY_PROFILE_BENCH_UPDATE_COMBINE_SHARDS=N`. The default `1` preserves
the single-queue combiner. Values above `1` shard request ingress by document ID
while preserving one global merged publish batch, and benchmark rows report
`update_combine_shards` so shard-count runs are not mixed accidentally.

For sharded-combiner lane-worker runs, add
`MONGO_GATEWAY_PROFILE_BENCH_UPDATE_COMBINE_LANE_WORKERS=true` to let each
document-ID shard prepare direct buffered update plans concurrently, then merge
prepared plans back into one global buffered staging path. This flag is effective
only when `MONGO_GATEWAY_PROFILE_BENCH_UPDATE_COMBINE_SHARDS` is greater than
`1`; only effective lane-worker runs report `update_combine_lane_workers`.
Stale prepared plans are admitted only when their target document IDs have not
changed in the buffered layer since the plan's read generation; conflicting
plans fall back to the ordinary per-document update path and read buffered
writes before returning.

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
  -concurrent-read-kinds id,email,range \
  -concurrent-readers 8 \
  -concurrent-reads 10000 \
  -concurrent-writers 4 \
  -concurrent-writes 1000 \
  -secondary-indexes 2 \
  -format json
```

For `-target mongo`, the command connects to the supplied URI, drops the
benchmark database by default, can compact the collection before final stats
collection with `-mongo-compact`, and reports `dbStats` fields after load and
at the end of the run.

## Reusable Comparison Harness

Use `scripts/mongo_gateway_compare.sh` when you want a complete
TreeDB-vs-MongoDB matrix bundle instead of hand-running each target. The
harness builds `mongo_gateway_bench`, runs matching TreeDB and MongoDB cells,
writes raw JSON for every target, records physical `du` bytes where available,
and generates a Markdown report plus TSV summary.

For the standard fast Mongo/native client-shape matrix used by the TreeDB
client-attribution tracker, prefer:

```sh
OUT_DIR=/tmp/treedb_fast_client_matrix_$(date +%Y%m%d_%H%M%S) \
  scripts/mongo_gateway_fast_client_matrix.sh
```

That wrapper pins gateway-shaped BSON documents, two secondary indexes, 16
insert producers/readers, `command_wal_relaxed`, settled read state, and the
current TreeDB/MongoDB client-mode sets. It also writes
`fast_client_matrix_context.md`, which explains client-mode, acknowledgement,
raw-command construction, direct-storage, and settled drain/flush boundaries.

To compare every TreeDB document format in one bundle:

```sh
TREEDB_DOCUMENT_FORMATS="json template-v1 bson" scripts/mongo_gateway_compare.sh
```

To include the raw-wire TreeDB insert load path and the driver command path
beside the normal MongoDB Go driver `InsertMany` path:

```sh
TREEDB_DOCUMENT_FORMATS="bson" \
TREEDB_CLIENT_MODES="driver driver-find-raw driver-command driver-command-raw direct raw-wire-tcp raw-wire-tcp-pipeline raw-wire native-wire-tcp native-wire-inproc" \
scripts/mongo_gateway_compare.sh
```

To run matching TreeDB and MongoDB client-mode rows where MongoDB supports the
mode:

```sh
TREEDB_DOCUMENT_FORMATS="bson" \
TREEDB_CLIENT_MODES="driver driver-find-raw driver-command driver-command-raw" \
MONGO_CLIENT_MODES="driver driver-find-raw driver-command driver-command-raw driver-unack" \
scripts/mongo_gateway_compare.sh
```

When the MongoDB side runs only the ordinary `driver` mode, the matrix keeps
the legacy baseline config names `mongo` and `mongo_range_index`. Explicit
`mongo_driver` config names are used only when `driver` is part of a multi-mode
MongoDB client matrix.

```sh
scripts/mongo_gateway_compare.sh \
  --out /tmp/gomap_mongo_gateway_compare \
  --docs "1000 10000" \
  --indexes "0 2" \
  --concurrent-read-kinds "id,email,range" \
  --concurrent-readers 8 \
  --concurrent-reads 10000 \
  --concurrent-writer-sweep "1,4" \
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

## YCSB Attribution Harness

Use `scripts/mongo_gateway_ycsb_attribution.sh` for the focused TreeDB Mongo
gateway YCSB load shape. It keeps the workload close to `go-ycsb mongodb`
load: BSON documents, batch size 1, 16 insert producers, `fast` TreeDB profile,
no post-load maintenance, and 0/1 secondary-index rows. The matrix separates
the official MongoDB Go driver path from raw Mongo wire over TCP, nativewire
over TCP, and direct collection insertion.

```sh
PROFILE=true scripts/mongo_gateway_ycsb_attribution.sh
```

The bundle contains:

- `summary.md` and `summary.tsv`: per-client throughput for every enabled
  phase, sampled ns/op, latency percentiles, producer count, and raw result
  paths.
- `index_<n>/<client_mode>/result.json`: unmodified
  `mongo_gateway_bench -format json` output for each cell.
- `index_<n>/<client_mode>/profiles/`: per-phase pprof artifacts and
  `*_top.txt` CPU summaries when `PROFILE=true` and the cell matches
  `PROFILE_MODES`/`PROFILE_INDEXES`.

Useful focused overrides:

- `DOCUMENTS=1000 CLIENT_MODES="driver direct" INDEXES_LIST=0` for a smoke run.
- `CLIENT_MODES="driver raw-wire-tcp native-wire-tcp direct"` to reproduce the
  standard attribution rows.
- `DOCUMENT_SHAPE=ycsb POINT_READ_PROJECTION=ycsb INDEXES_LIST=0
  CLIENT_MODES="driver raw-wire-tcp direct" CONCURRENT_READERS=16
  CONCURRENT_READS=100000 READS=0 RANGE_READS=0` to attribute the exact
  `go-ycsb mongodb` point-read shape: `_id` lookup, projection of
  `field0` through `field9`, `_id` excluded, and decode into
  `map[string][]byte` on the driver path.
- `PROFILE=true PROFILE_MODES="driver raw-wire-tcp"` to capture gateway and raw
  wire CPU evidence without profiling every cell.
- `READS`, `UPDATES`, `CONCURRENT_READERS`, and `CONCURRENT_WRITERS` can enable
  additional run-style phases, but exact YCSB run semantics should still be
  measured with `go-ycsb mongodb`.

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
- `TREEDB_CLIENT_MODES="driver driver-find-raw driver-command driver-command-raw direct raw-wire-tcp raw-wire-tcp-pipeline raw-wire native-wire-tcp native-wire-inproc"` (`driver-unack` is intentionally excluded because TreeDB rejects `w:0` without mutation)
- `MONGO_CLIENT_MODES="driver driver-find-raw driver-command driver-command-raw driver-unack"`
- `READS=50000`, `RANGE_READS=5000`, `UPDATES=5000`
- `DELETES=1000`
- `RANGE_INDEX=true` or `--range-index` to create `age_1` and report
  `age_range_indexed_limit_10` instead of scan fallback.
- `PROFILE_TREEDB=true` or `--profile-treedb` to pass `-profile-dir` for every
  TreeDB cell and retain per-phase profiles under the bundle's `profiles/`
  directory.
- `CONCURRENT_READERS=16`, `CONCURRENT_READS=50000`
- Use either `CONCURRENT_READ_KINDS="id,email,range"` with
  `CONCURRENT_READER_SWEEP`, or `CONCURRENT_RANGE_READER_SWEEP` with
  `CONCURRENT_RANGE_READS`; the two range-concurrency paths share phase names
  and are intentionally not combined.
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
  --concurrent-read-kinds "id,email,range" \
  --concurrent-reader-sweep "1,2,4,8,16" \
  --concurrent-reads 50000 \
  --concurrent-writer-sweep "1,2,4,8,16" \
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
      -concurrent-read-kinds id,email,range \
      -concurrent-readers 8 \
      -concurrent-reads "$((docs / 10))" \
      -concurrent-writer-sweep 1,4 \
      -concurrent-writes "$((docs / 20))" \
      -secondary-indexes "$indexes" \
      -format json > "treedb-${docs}-${indexes}.json"

    GOWORK=off go run ./cmd/mongo_gateway_bench \
      -target mongo \
      -documents "$docs" \
      -reads "$docs" \
      -range-reads "$((docs / 10))" \
      -updates "$((docs / 10))" \
      -concurrent-read-kinds id,email,range \
      -concurrent-readers 8 \
      -concurrent-reads "$((docs / 10))" \
      -concurrent-writer-sweep 1,4 \
      -concurrent-writes "$((docs / 20))" \
      -secondary-indexes "$indexes" \
      -format json > "mongo-${docs}-${indexes}.json"
  done
done
```

The initial workload phases are:

- `load_insert_many`: batched document inserts. The exact client call depends
  on `client_mode`: `InsertMany` for `driver` and `driver-find-raw`,
  `RunCommand({insert, documents})` for `driver-command`, `RunCommand` with a
  prebuilt raw BSON command for `driver-command-raw`, MongoDB-target-only
  unacknowledged `InsertMany` plus a post-load visibility wait for
  `driver-unack`, direct
  `Collection.InsertBatch` in the selected storage format for `direct`, and raw
  OP_MSG document sequences for `raw-wire`/`raw-wire-tcp`/
  `raw-wire-tcp-pipeline`. When
  `-insert-producers` is greater than 1, this
  phase reports aggregate wall-clock throughput and per-producer call latency in
  `producer_results`.
- `id_find_one`: point lookup by `_id`.
- `email_find_one`: point lookup by the `email` field; emitted only when the
  email secondary index is part of the cell.
- `age_range_scan_limit_10` / `age_range_indexed_limit_10`: bounded range query
  with `limit: 10`; operations count range queries, not returned documents. The
  indexed variant is emitted when `-range-index` creates `age_1`.
- `concurrent_age_range_scan_limit_10_rN` /
  `concurrent_age_range_indexed_limit_10_rN`: total age range queries split
  across `N` goroutines. Use `-concurrent-range-reader-sweep 1,2,4,8,16` with
  `-concurrent-range-reads` to emit multiple range-reader fanout phases from one
  loaded database. The legacy `-concurrent-range-readers N` flag emits one
  fanout phase and cannot be combined with `-concurrent-range-reader-sweep`.
- `id_update_set`: `$set` update by `_id`.
- `concurrent_id_find_one_rN`, `concurrent_email_find_one_rN`,
  `concurrent_age_range_scan_limit_10_rN`, and
  `concurrent_age_range_indexed_limit_10_rN`: total read operations split across
  `N` goroutines. Use `-concurrent-read-kinds id,email,range` or
  `-concurrent-read-kinds all` to select the saturated read shapes. Use
  `-concurrent-reader-sweep 1,2,4,8,16` with `-concurrent-reads` to emit
  multiple reader-count phases from one loaded database. The comparison report
  groups `_id`/email rows into the "Concurrent Read Sweep" table and renders
  `concurrent_age_range_*_rN` rows in the dedicated range-read section. The
  generic `range` read kind cannot be combined with
  `-concurrent-range-readers` or `-concurrent-range-reader-sweep`, because those
  options emit the same range phase names. The legacy `-concurrent-readers N`
  flag still emits one reader-count phase per selected kind and cannot be
  combined with `-concurrent-reader-sweep`.
- `concurrent_id_update_set_wN`: total `$set` updates split across `N`
  goroutines. Use `-concurrent-writer-sweep 1,2,4,8,16` with
  `-concurrent-writes` to emit multiple writer-count phases from one loaded
  database.
- `id_delete_one`: optional deletes; disabled unless `-deletes` is non-zero.

Update phases change only non-indexed fields by default.
`-update-indexed-field` requires `-secondary-indexes=2` or greater so the city
index exists and the indexed `city` field changes, exercising secondary-index
maintenance in the update path. `-secondary-indexes=3` adds an unchanged
`active_1` bool index so city-only update runs can verify that unchanged
secondary indexes are not rebuilt as total index count grows.
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
TreeDB gateway smoke against both the no-WAL fast and legacy WAL relaxed-fast
profiles to catch large profile regressions without making the smoke a
replacement for the full matrix.

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
investigations, keep the load shape fixed and use `-concurrent-writer-sweep` /
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

The default sweep is TreeDB-only, using the `bench` profile, native BSON
collection storage, `driver-command-raw`, prebuilt BSON documents, and no final
maintenance so the measured phases focus on concurrency. Add
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
  -bench '^(BenchmarkTreeDBGatewayLoadBSONIndexes2|BenchmarkTreeDBGatewayLoadGeneratedIDBSONIndexes2|BenchmarkTreeDBGatewayLoadObjectIDBSONIndexes2|BenchmarkTreeDBGatewayRunCommandLoadBSONIndexes2|BenchmarkTreeDBGatewayRunRawCommandLoadBSONIndexes2|BenchmarkTreeDBGatewayRawWireLoadBSONIndexes2|BenchmarkTreeDBGatewayRawWireTCPLoadBSONIndexes2|BenchmarkDirectCollectionLoadBSONIndexes2|BenchmarkDirectCollectionConcurrentUpdateBSONIndexes2|BenchmarkDirectCollectionConcurrentUpdateBSONCityIndex1|BenchmarkDirectCollectionConcurrentUpdateBSONIndexes2CityUpdate|BenchmarkDirectCollectionConcurrentUpdateBSONIndexes3CityUpdate|BenchmarkClientBSONBatchEncode)$' \
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
  `update_old_index_state_extract_ns/doc`,
  `update_new_index_state_extract_ns/doc`,
  `update_primary_run_ns/doc`, `update_secondary_runs_ns/doc`,
  `update_index_value_changes/doc`, `update_index_value_unchanged/doc`,
  `update_unique_checks/doc`, `update_unique_check_skips/doc`,
  per-index changed-delta metrics such as
  `update_collection_bench.docs_index_1_city_1_changed/doc`,
  `update_collection_bench.docs_index_1_city_1_secondary_runs/doc`,
  `update_collection_bench.docs_index_2_email_1_unchanged/doc`, and
  `update_collection_bench.docs_index_2_email_1_unique_check_skips/doc`
  for collections with at most eight index runtimes,
  `update_buffer_stage_ns/doc`, buffer-stage submetrics such as
  `update_buffer_precheck_ns/doc`, `update_buffer_lock_wait_ns/doc`,
  `update_buffer_lock_hold_ns/doc`, `update_buffer_validation_ns/doc`,
  `update_buffer_root_scan_ns/doc`, `update_buffer_domain_prepare_ns/doc`,
  `update_buffer_freeze_ns/doc`, `update_buffer_root_table_ns/doc`,
  `update_buffer_primary_index_ns/doc`, `update_buffer_unique_index_ns/doc`,
  `update_buffer_primary_append_ns/doc`,
  `update_buffer_secondary_append_ns/doc`,
  `update_buffer_root_append_ns/doc`, `update_buffer_flush_ns/doc`,
  combiner timings such as `update_combine_enqueue_ns/doc`,
  `update_combine_wait_ns/doc`, `update_combine_drain_ns/doc`, and
  `update_combine_run_ns/doc`, `update_publish_ns/doc`, and
  `update_items/batch` from the collection
  manager's measured-phase counters. `update_buffer_lock_hold_ns/doc` is the
  enclosing domain mutex hold time for buffered staging, not an additive sibling
  of the other buffer-stage submetrics; the narrower root-table, append, and
  freeze submetrics are nested attribution inside the broader buffer-stage
  timers. `update_buffer_flush_ns/doc` reports
  threshold-flush scheduling/publish time separately. The measured phase includes
  the final `FlushAll()` drain so background async publish work is charged to the
  same throughput row that scheduled it. The same row also reports backend
  value-log mmap counters such as `backend_vlog_mmap_hits/doc`,
  `backend_vlog_mmap_miss_dead_mapping_cap/doc`,
  `backend_vlog_mmap_fallback_readat/doc`, and `backend_vlog_mmap_hit_ratio`
  plus sealed-map budget/residency counters such as
  `backend_vlog_mmap_sealed_denied_count_cap/doc`,
  `backend_vlog_mmap_sealed_segments`, and `backend_vlog_mmap_active_bytes`, so
  value-log mmap fallbacks can be separated from other `ReadAt` sources in CPU
  profiles and tied back to mmap residency limits.
- `BenchmarkDirectCollectionConcurrentUpdateBSONCityIndex1`,
  `BenchmarkDirectCollectionConcurrentUpdateBSONIndexes2CityUpdate`, and
  `BenchmarkDirectCollectionConcurrentUpdateBSONIndexes3CityUpdate` run the same
  direct update shape while changing only the indexed `city` field. They are
  intended to guard per-index changed-delta planning: adding unchanged indexes
  such as `email_1` and `active_1` should not create extra secondary runs or
  affected secondary roots for a city-only update.
- `BenchmarkClientBSONBatchEncode` measures client-side BSON document encoding
  alone.

The benchmark-only helpers accept these optional environment variables:
`MONGO_GATEWAY_PROFILE_BENCH_BATCH_SIZE`,
`MONGO_GATEWAY_PROFILE_BENCH_UPDATE_DOCUMENTS`, and
`MONGO_GATEWAY_PROFILE_BENCH_WRITERS`. They also accept
`MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH` and
`MONGO_GATEWAY_PROFILE_BENCH_BUFFERED_INDEXED_ASYNC_FLUSH_MAX_QUEUED_UNITS` for
focused indexed async-flush experiments.

Use the official-driver row for user-visible Mongo compatibility throughput,
`driver-find-raw` to remove range-read `bson.M` decode while keeping
official-driver `Find`/cursor behavior, the driver-command rows to quantify the
driver's CRUD-helper overhead, the raw-wire rows to estimate the gateway/server
ceiling, `raw-wire-tcp-pipeline` to measure how much single-connection TCP
latency can be hidden by request pipelining, and the direct collection row to
estimate the storage-engine ceiling for the same document shape. For
acknowledged high-throughput ingest through the
public MongoDB Go driver,
`driver-command-raw` is the fastest current path because it keeps the official
driver transport while bypassing `InsertMany`'s explicit-`_id` discovery and
`InsertedIDs` bookkeeping.

## Interpreting Results

`-secondary-indexes 2` creates `email_1` and `city_1`; `-secondary-indexes 3`
adds `active_1`. The age range phase is a bounded scan unless `-range-index` is
set; benchmark output names the phase `age_range_scan_limit_10` or
`age_range_indexed_limit_10` so reports can separate fallback cost from indexed
range-search cost.

For TreeDB targets, `-treedb-read-state settled` (the default) calls
`CollectionManager.FlushAll` after load and before read phases, so reads measure
settled collection roots rather than mutable write-domain state. Use
`-treedb-read-state unsettled` to leave post-load memtables/write-domain state in
place for read phases when you want to compare settled and unsettled visibility
costs.

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
