# C3 external MVCC read admission fixture

`BenchmarkC3PublicReadAdmission` is a bounded actual public Store diagnostic.
It uses all three ordinary production ACK profiles (`command_wal_durable`,
`command_wal_relaxed`, `no_wal_fast`), the explicit `cow_btree` engine and legacy
`append_only`/`btree` controls, with `inline` and `forced_pointer` values. Every
leaf opens a fresh DB, disables background checkpoint and side stores, uses four
shards and a 16MiB flush threshold. Defaults retain finite COW admission limits.
The resolved profile/mode/ordinary ACK receipt must match the request.

Fixture seed: logical keys `c3-a`, `c3-ab`, timestamps 10 and 20, payload 256 bytes
of `0x63`. Replacements use those same physical versions and equal payloads;
history and logical output stay fixed throughout baseline/candidate runs.
Setup, floor warming, correctness warmup, stats reads and Close are outside
timing. Every measured call uses `CommitRelaxed`, retaining the profile's ordinary
ACK contract. No observer, artificial pause or read bypass enters timing.

| Leaf | Work per Go operation | Point calls | Scan calls | Output | Visited |
| --- | --- | ---: | ---: | ---: | ---: |
| `point` | CommitAt(20, one mutation), GetAt(100) | 1 | 0 | 1 | 0 |
| `group_versions` | CommitGroupAt(two timestamp groups, two keys each), actual exact-key all-version scan | 0 | 1 | 2 | 2 |
| `concurrent` | One ordinary group writer, one point reader, one exact-key scan reader, each N calls | 1 | 1 | 3 | 2 |

The scan uses `EntryView` to validate every record and its timestamp, payload and
exact key; it closes its iterator and checks Visited/Retained/Skipped. Point output
is owned and validated. A leaf fails on any mutation/read/Close error or output
mismatch. Concurrent workers share a start barrier and join before timing ends.
One reported Go operation is one writer call and its fixed reader share, not one
individual API call. Combined writer/reader rates use elapsed time until all three
callers join. Concurrent `point_phase_elapsed_ns`/`scan_phase_elapsed_ns` and
`point_phase_ops/s`/`scan_phase_ops/s` instead use start-barrier release until that
reader's N calls finish; these reader rates expose progress independently of a
slower durable writer. They are absent in sequential leaves. No internal
writer-phase timing is inferred.

Collect one leaf per fresh process using identical fixture bytes and separate
baseline/candidate binaries, source/module/toolchain receipts and `GOMAXPROCS=4`:

```sh
GOWORK=off GOMAXPROCS=4 go test ./TreeDB/mvcc/cowbench -run '^$' \
  -bench '^BenchmarkC3PublicReadAdmission/command_wal_relaxed/cow_btree/inline/point$' \
  -benchtime=1024x -count=1 -benchmem
```

Use 128x for separate warmup/smoke; the fixture refuses N above 4096. Go benchmark
stdout is a standalone artifact, not a `benchprof` input. Preserve every paired
repetition and failure, command, exit status, host, binary hash, input closure and
source hashes. Do not overlap measured jobs. The smaller one-operation functional
smoke establishes harness execution only and is not allocation/performance data.

Metrics include ns/op, B/op, allocs/op, writer/reader ops/s, point/scan calls/op,
output/op, visited/op, p50/p95/p99 ordinary writer and active reader call latency,
WAL append/sync and actual snapshot rotation/shard/enqueued-record deltas. Inactive
reader latency fields are absent. NoWAL WAL fields are explicitly zero. COW fields
include actual capture/preparation/publication deltas and final total/peak bytes,
views, external leases and active cuts. `close_ok=1` receipts a successful Close
outside timing; the public handle refuses Stats after Close, so this fixture
does not claim a post-Close charge measurement. Public COW retention/drain tests
verify internal accounting cleanup separately. COW reads do not rotate mutable
shards. Repeated replacements and snapshot creation can change source/generation
frontiers and trigger finite backpressure/checkpoint even though logical history
and output stay fixed; retain actual path and ACK counters.
`readers_while_writer_active/op` counts completions observed during an unresolved
ordinary writer call. It may be zero with scheduler/workload ordering; it does not
identify preparation or establish universal liveness. The separate accepted
preswap-window regression proves phase-specific old-cut reader completion.

## Allocation and copy ownership audit

| Site | Frequency and owner | Change |
| --- | --- | --- |
| MVCC physical lower/upper bounds | Each GetAt; caller-local codec buffers | Existing encoded codec retained; no alternate timestamp codec/history collection |
| Staged mutation slice, duplicate map and physical/envelope buffers | Each nonempty commit; staging owns input through existing write | Unchanged validation and point/batch optimized routes |
| Resolved atomic capability | Once per Store; interface references the existing DB | One interface field; no table-name dispatch or repeated capability allocation |
| COW cut capture / Snapshot | Once per admitted point/scan; existing finite view pin and pooled Snapshot owner | Existing primitive, no second snapshot or view framework |
| COW successor cache selection | Each GetAt on empty retained backend | Existing lower-bound selection reused; no merging iterator imposed on this fast case |
| Owned successor key/record bytes | Each successful GetAt; returned owned record backing transfers its payload | Existing copies; decode owns logical-key buffer, no added payload copy |
| Nonempty backend/physical tombstone successor fallback | As needed on the admitted cut | Internal merge sources/workspace/pins and owned key/value copies; no recapture or snapshot-bound wrapper/lease |
| Iterator option copies, VersionIterator/key buffer, raw sources/value workspace | Each scan; single VersionIterator/snapshot lifetime | Existing copies/backing and EntryView behavior; construction outside floor admission |
| Error joining | Error/Close paths (and existing deferred cleanup) | Close remains observable; no ownership retry or fallback after capture failure |

Moving GetAt's existing capture from the DB successor to Store admission preserves
one cut and existing successor allocations. Moving scan construction changes lock
scope without adding an owner or extra output copy. Capability interface dispatch
and deferred Close may affect compiler escapes; matched B/op and allocs/op must be
checked rather than inferred from this source audit. There is no zero-allocation,
whole-work bound or production speedup claim from this milestone.

The synchronous successor fallback builds and closes its internal iterator inside
the already admitted snapshot read. It does not recursively acquire the DB read
gate when Close has queued an exclusive writer. The bounded Close regression
pauses an actual captured source lower bound before physical tombstone fallback;
the nonempty disk case enters the same internal fallback under a held admission.
Both cases use actual DB Close, prove its writer is queued, validate inline/pointer
output, join read/Close and verify all retained cuts/charges release.

COW reverse iteration/pruning remain unsupported. Prune preflight refuses before
floor/WAL effects. Native maintenance, full C3 lifecycle/replay and sustained C4
qualification remain separate acceptance gates.
