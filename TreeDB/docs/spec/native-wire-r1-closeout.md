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
  -benchtime=1000x \
  -count=5
```

Result on `linux/amd64`, Intel i5-11400F:

| Benchmark | time/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `InsertBatch/direct_collection` | 39.14 us | 6.444 KiB | 49 |
| `InsertBatch/native_wire_inproc` | 54.31 us | 8.116 KiB | 54 |
| `GetMany/direct_collection` | 34.64 us | 8.007 KiB | 128 |
| `GetMany/native_wire_inproc` | 42.31 us | 14.46 KiB | 9 |

Interpretation:

- The native in-process benchmark opens a collection handle before timing and
  uses handle-based hot commands. This is the intended steady-state path for
  long-lived clients.
- R1 native in-process insert is about 1.39x direct collection latency for this
  32-document batch shape.
- R1 native in-process get-many is about 1.22x direct collection latency for
  this 64-document batch shape.
- Insert allocation is now close to the direct collection baseline: native wire
  adds five allocations/op for request/response framing and response decode.
- Get-many allocates less often than the direct `Collection.Get` loop because
  the server materializes documents with `GetInto` into one response payload and
  the client returns borrowed frame views.
- This is materially closer, but it is not throughput/speed parity yet. The
  remaining latency gap is mostly frame/transport synchronization,
  request/response copies, and generic request parsing around small batches.

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

- The hot in-process path now reuses client request buffers, server read/write
  buffers, section validation scratch, and byte-vector `[][]byte` scratch.
- Native-wire allocation is no longer dominated by duplicate-ID string
  conversion, section decode, or materialized get-many document batches.
- Remaining protocol allocation is concentrated in client response frame bodies
  whose returned slices intentionally borrow from the response backing array.
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
- Added reusable connection/client scratch for server frame reads, server
  section/schema validation, server byte-vector views, client request bodies,
  and small combined frame writes.
- Replaced the previous byte-vector clone decode on hot response/read paths with
  borrowed immutable frame views.
- Added `DecodeByteVectorItems`, a borrowed two-pass decoder that avoids
  offset/length table allocations when callers only need a transient `[][]byte`
  view.
- Added `DecodeByteVectorItemsInto` and direct byte-vector payload encoding so
  server hot paths can reuse slice-header arrays and encode response payloads
  without first materializing per-section byte-vector buffers.
- Switched server mutation input decode to borrowed frame views; collections
  still owns the copies needed for persistent state and update/delete planning.
- Removed the redundant native-wire insert duplicate-ID map; insert now relies
  on the collection planner's sorted duplicate check while preserving wire error
  codes for duplicate and empty IDs.
- Cached opened collections per connection so repeated handle/name requests do
  not reopen collection objects on every command.
- Added handle-based `InsertBatchHandle` and `GetManyHandle` client methods and
  moved the steady-state benchmarks to open the collection handle once before
  timing.
- Encoded `get_many` responses directly from `Collection.GetInto` into a single
  payload plus presence bitmap, avoiding one document allocation per returned
  document.
- Added direct hot request/response body encoders for `InsertBatch` and
  `GetMany`, including deterministic ack metadata encoding without a temporary
  string map.
- Fixed native-wire benchmark load IDs in R1e to use the Mongo gateway's encoded
  primary-key format, so later Mongo-driver read/update phases exercise the
  documents inserted through native wire.

## Parity Follow-Ups

R1f should not be treated as the final performance signoff. To target parity,
the next performance sprint should plan against these items before moving the
native-wire path into a Raft/distributed baseline:

- add a benchmark split that separates direct collection, direct in-process
  dispatch without `net.Pipe`, in-process framed transport, TCP loopback, and
  Mongo gateway paths,
- add an explicit acceptance target, for example native in-process steady-state
  latency within 1.05x direct collection for the chosen batch shapes or a
  documented reason the remaining delta is unavoidable transport cost,
- implement a lower-overhead in-process transport or dispatch path if
  `net.Pipe` is confirmed as the dominant non-collection gap,
- evaluate pipelined/asynchronous client requests for throughput parity when
  many independent operations can be in flight,
- consider an immutable-borrow contract for client result bytes so APIs can
  document when returned slices share a response-frame backing array,
- evaluate `writev`/gather-write support for TCP so small-frame coalescing does
  not require copying larger response bodies,
- decide whether the client should expose an owned-result mode for callers that
  want response buffers reused across calls,
- consider an ack-only insert mode for caller-supplied IDs so workloads that do
  not need echoed IDs avoid response byte-vector encoding and decode,
- add per-command native-wire latency histograms so decode, dispatch, collection
  execution, and encode costs can be separated without pprof inference,
- add longer steady-state TCP benchmarks that isolate transport cost from
  benchmark document generation and collection flush behavior.

R1f is close enough to identify the remaining blockers, but the parity target is
not closed. If parity is required before Raft work, add a focused R1g
performance sprint before advancing the native-wire stack into R2.
