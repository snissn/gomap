# Native Wire R2 Deterministic Entry Closeout

Status: R2e implementation note.

This note records the deterministic command-entry v1 closeout. R2 is the bridge
between the single-node R1 server and a future Raft write path; it does not add
Raft apply or distributed reads.

## Scope

R2 adds and hardens:

- the deterministic-entry v1 decode/inspect API,
- golden entry fixtures for every currently replicated v1 command,
- command-aware canonical validation for idempotency keys, document ID vectors,
  document/vector count agreement, metadata/index payload presence, and index
  names,
- SHA-256 digest helper over exact canonical entry bytes,
- stability tests proving transport-only sections, response-shaping flags,
  deadlines, trace metadata, compression choices, and shuffled sections do not
  alter canonical bytes,
- replicated-command append/decode/digest benchmarks.

The replicated v1 command fixture set is:

- `create_collection`
- `create_index`
- `drop_index`
- `insert_batch`
- `replace_batch`
- `delete_batch`

`drop_collection`, flush, checkpoint, reads, cursors, stats, and collection
handle commands remain local-only in v1.

## Validation

Correctness:

```sh
GOWORK=off go test ./TreeDB/internal/nativewire -count=1
```

Benchmark closeout:

```sh
GOWORK=off go test ./TreeDB/internal/nativewire \
  -run '^$' \
  -bench 'BenchmarkNativewireDeterministicEntry' \
  -benchmem \
  -benchtime=1000x \
  -count=3
```

Top-stack native collection parity check:

```sh
GOWORK=off go test ./TreeDB/nativewire \
  -run '^$' \
  -bench 'BenchmarkNativewireCollectionInsertBatch/(direct_collection|native_wire_inproc_no_result|native_wire_direct_dispatch_no_result)$|BenchmarkNativewireRejectDuplicateIDs' \
  -benchmem \
  -benchtime=5000x \
  -count=8
```

## Benchmark Findings

All replicated-command append, decode, and digest benchmark lanes completed with
`0 allocs/op`.

Representative R2e results on `linux/amd64`, Intel i5-11400F:

| Benchmark | time/op | allocs/op |
| --- | ---: | ---: |
| `insert_batch_64x256_template/append/preallocated` | 3.16-3.26 us | 0 |
| `replace_batch_64x256_template/append/preallocated` | 3.48-3.54 us | 0 |
| `delete_batch_128_ids/append/preallocated` | 6.26-6.36 us | 0 |
| `create_collection/append/preallocated` | 100.8-106.6 ns | 0 |
| `create_collection/decode/warm_scratch` | 82.5-84.0 ns | 0 |
| `create_collection/digest` | 78.4-80.8 ns | 0 |
| `create_index/append/preallocated` | 159.6-162.5 ns | 0 |
| `create_index/decode/warm_scratch` | 94.1-97.6 ns | 0 |
| `delete_batch/decode/warm_scratch` | 93.8-96.0 ns | 0 |

The first R2e benchmark pass exposed an avoidable duplicate-ID validation cost
in larger ID batches. The closeout replaces the repeated byte comparison scan
with stack-backed FNV-1a hashes and byte compares only on matching hash/length
pairs, reducing the 128-ID deterministic delete append path from roughly
26 us/op to roughly 6.3 us/op while preserving zero allocations.

The R2 top-stack parity check also re-ran the native collection insert lanes
from the R1 closeout after the R2 hardening changes. The first run exposed a
benchmark harness drift: the direct-dispatch lanes bypassed the client handshake
and now must mark their synthetic connection as already greeted. After fixing
that, the run exposed an avoidable hot-path allocation in native-wire
duplicate-ID validation. The R2e closeout replaces the small/normal batch path
with a stack-backed bucket table using FNV-1a hashes and falls back to a map only
for batches above 512 IDs.

Representative results after the fix:

| Benchmark | time/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `InsertBatch/direct_collection` | 39.46 us +/- 1% | 6.433 KiB | 50 |
| `InsertBatch/native_wire_inproc_no_result` | 44.49 us +/- 2% | 6.459 KiB | 50 |
| `InsertBatch/native_wire_direct_dispatch_no_result` | 45.07 us +/- 1% | 6.477 KiB | 50 |
| `RejectDuplicateIDs/32_ids` | 265.5 ns +/- 2% | 0 B | 0 |
| `RejectDuplicateIDs/128_ids` | 906.2 ns +/- 2% | 0 B | 0 |
| `RejectDuplicateIDs/512_ids` | 4.077 us +/- 2% | 0 B | 0 |

The allocation regression before this fix was visible in the same benchmark
shape: ack-only native insert used 85 allocs/op and about 8.75-8.79 KiB/op.
After the fix, ack-only native insert returns to direct-collection allocation
parity at 50 allocs/op and roughly 6.44 KiB/op. A focused CPU profile of
`native_wire_direct_dispatch_no_result` showed remaining protocol work as
request encoding, fast request decode, and duplicate-ID validation in the
sub-microsecond class per operation; collection insert/publish remains the
dominant sampled work. The remaining latency gap is therefore tracked as
intentional framed-command overhead unless a later profile finds a single
dominant protocol cost.

## Deferred To R3

- Raft log storage and apply are still unimplemented.
- Idempotency record persistence and replay policy are still unimplemented.
- Catalog guards are encoded and validated structurally, but R3 must define the
  apply-time guard state machine and deterministic conflict outcomes.
- Metadata payloads remain opaque at the internal codec layer. R3 should either
  move canonical metadata/index payload decoding into the deterministic-entry
  layer or define a separate state-machine decoder with the same fixture gate.
