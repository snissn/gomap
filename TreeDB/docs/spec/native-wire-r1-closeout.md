# Native Wire R1 Performance Closeout

Status: R1f implementation note.

This note records the R1 single-node native server benchmark closeout. It is a
baseline for R2 deterministic-entry work and future Raft/distributed phases, not
a final performance claim.

## Scope

R1f measures the public `TreeDB/nativewire` server/client path implemented in
R1a-R1e:

- frame dispatch, request admission, stats, and error mapping,
- metadata commands and connection-local handles,
- read commands and pull cursors,
- mutation commands and ack policy handling,
- in-process, TCP, and Unix listener transports,
- native-wire load modes in `cmd/mongo_gateway_bench`.

The benchmark modes are intentionally labeled separately from direct collection,
Mongo driver, and Mongo raw-wire paths.

## Microbenchmarks

Command:

```sh
GOWORK=off go test ./TreeDB/nativewire \
  -run Test \
  -bench 'BenchmarkNativewireCollection(InsertBatch|GetMany)' \
  -benchmem \
  -benchtime=100x \
  -count=5
```

Result on `linux/amd64`, Intel i5-11400F:

| Benchmark | time/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `InsertBatch/direct_collection` | 37.91 us | 6.399 KiB | 48 |
| `InsertBatch/native_wire_inproc` | 64.17 us | 24.27 KiB | 114 |
| `GetMany/direct_collection` | 34.75 us | 8.018 KiB | 128 |
| `GetMany/native_wire_inproc` | 49.01 us | 30.65 KiB | 154 |

Interpretation:

- R1 native in-process insert is about 1.69x direct collection latency for this
  32-document batch shape.
- R1 native in-process get-many is about 1.41x direct collection latency for
  this 64-document batch shape.
- The main remaining native-wire overhead is allocation and copying around frame
  body reads, request-body construction, byte-vector construction, duplicate-ID
  checks, and materialized result batches.

## Workload Profiles

Commands:

```sh
GOWORK=off go run ./cmd/mongo_gateway_bench \
  -target treedb \
  -client-mode native-wire-inproc \
  -documents 50000 \
  -batch-size 100 \
  -reads 1000 \
  -range-reads 200 \
  -updates 200 \
  -deletes 0 \
  -secondary-indexes 1 \
  -treedb-document-format json \
  -profile-dir /tmp/nativewire_r1f_inproc_opt \
  -profile-heap-gc \
  -format json
```

```sh
GOWORK=off go run ./cmd/mongo_gateway_bench \
  -target treedb \
  -client-mode native-wire-tcp \
  -documents 50000 \
  -batch-size 100 \
  -reads 1000 \
  -range-reads 200 \
  -updates 200 \
  -deletes 0 \
  -secondary-indexes 1 \
  -treedb-document-format json \
  -profile-dir /tmp/nativewire_r1f_tcp_opt \
  -profile-heap-gc \
  -format json
```

Load phase results:

| Mode | docs | batches | duration ms | docs/sec | sampled ns/doc | mean batch us |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `native-wire-inproc` | 50,000 | 500 | 203.262 | 245,987 | 3,473 | 347.258 |
| `native-wire-tcp` | 50,000 | 500 | 217.336 | 230,058 | 3,834 | 383.374 |

Profile artifacts:

- `/tmp/nativewire_r1f_inproc_opt`
- `/tmp/nativewire_r1f_tcp_opt`

Each directory contains `benchmark_result.json`, `profile_manifest.json`, and
CPU, allocation, heap, block, mutex, and goroutine profiles for:

- `load_insert_many`,
- `id_find_one`,
- `email_find_one`,
- `age_range_scan_limit_10`,
- `id_update_set`.

## Profile Findings

The workload CPU profiles are short but sampled enough to identify the broad
shape. Insert CPU is dominated by benchmark document generation, collection
insert planning, JSON/index extraction, value-log compression, memtable/root
publish work, and runtime allocation/GC. Native-wire CPU appears as frame
dispatch, byte-vector decode, and frame write overhead rather than a single
dominant protocol bottleneck.

The load allocation profiles show the clearest R2/R3 follow-up targets after
the R1f optimization pass:

- `nativewire.appendCommandRequestBody` and `internal/nativewire.growBytes`
  remain visible because R1 still allocates fresh request bodies and
  byte-vector section payloads.
- `internal/nativewire.DecodeByteVectorItems` remains visible because borrowed
  views still need one `[][]byte` slice header array per decoded vector.
- `nativewire.readFrame` still allocates a complete frame body per request.
- Collection planning, memtable entry buffers, root publish, and value-log open
  costs remain larger than any single native-wire server function in the full
  workload.

## Optimizations Made In R1f

- Added direct-vs-native package benchmarks for insert and get-many so
  native-wire overhead is visible without the Mongo gateway benchmark harness.
- Kept insert benchmark fixture generation outside the timed section so the
  benchmark reports collection/protocol work instead of document construction.
- Changed `writeFrame` to write a stack-built frame header and the body as
  separate writes, avoiding a combined per-frame allocation.
- Pre-sized byte-vector, section, and command-request encoders to avoid growth
  churn while preserving the existing wire bytes.
- Replaced the previous byte-vector clone decode on hot response/read paths with
  borrowed immutable frame views.
- Added `DecodeByteVectorItems`, a borrowed two-pass decoder that avoids
  offset/length table allocations when callers only need a transient `[][]byte`
  view.
- Switched server mutation input decode to borrowed frame views; collections
  still owns the copies needed for persistent state and update/delete planning.
- Fixed native-wire benchmark load IDs in R1e to use the Mongo gateway's encoded
  primary-key format, so later Mongo-driver read/update phases exercise the
  documents inserted through native wire.

## Deferred Follow-Ups

These are deliberately left for the next performance closeout or a focused
native-wire optimization PR:

- add reusable client request builders for byte-vector sections,
- add server read buffers and parse scratch scoped to a connection or request,
- consider an immutable-borrow contract for client result bytes so APIs can
  document when returned slices share a response-frame backing array,
- add per-command native-wire latency histograms so decode, dispatch, collection
  execution, and encode costs can be separated without pprof inference,
- add longer steady-state TCP benchmarks that isolate transport cost from
  benchmark document generation and collection flush behavior.

R1 is acceptable to advance to R2 because the measured overhead is concentrated
in known allocation/copy points and the protocol path has no unexplained
dominant CPU hotspot.
