# Dgraph-shaped durability benchmark

This package isolates the storage contract used by Dgraph's posting store
without importing Dgraph itself. It compares:

- Badger managed transactions with caller-assigned commit timestamps;
- TreeDB's public external-timestamp MVCC layer over the command-WAL profile.

The benchmark keeps power-loss-durable acknowledgement separate from relaxed,
crash-consistent acknowledgement:

| Class | Badger | TreeDB |
| --- | --- | --- |
| `relaxed` (crash-consistent; no per-commit sync) | `SyncWrites=false` | `ProfileCommandWALRelaxed` + `CommitRelaxed` |
| `durable` | `SyncWrites=true` | `ProfileCommandWALDurable` + `CommitDurable` |

The commit benchmark uses small independently acknowledged transactions, which
is the shape that exposes per-commit synchronization cost in the Dgraph Alpha
benchmark. Batch sizes 1 and 16 show whether that cost is amortized by a wider
atomic transaction. Values of 128 B and 4096 B cover small and moderate posting
values. Their physical placement can differ between the configured engines and
is intentionally left to each production-oriented profile.

The mixed benchmark uses the same 60% read, 20% write, 20% delete proportions as
the Dgraph harness. It intentionally runs a serial operation stream so the
measured result is the storage contract rather than Dgraph's gRPC, query, Raft,
or posting-list work. It is a diagnostic analogue, not a replacement for the
full Dgraph A/B benchmark.

Run the complete matrix with:

```sh
GOWORK=off go test ./benchmarks/dgraph_durability \
  -run '^$' -bench '^BenchmarkDgraphShaped' -benchmem -count=5
```

## Stable attribution rows

The fixed mixed rows are seeded deterministically and create a fresh database
per trial. Database open, the 256-key seed commit, stats collection, and close
are outside the timer. Run them with exactly one fixed-work trial per repeat:

```sh
GOWORK=off go test ./benchmarks/dgraph_durability \
  -run '^$' -bench '^BenchmarkDgraphShapedMixedFixed' \
  -benchmem -benchtime=1x -count=2
```

Rows are emitted for 50k, 100k, 250k, and 500k operations. The standard Go
`B/op` and `allocs/op` columns are per fixed-work *trial*; use the explicit
`workload_B/op` and `workload_allocs/op` metrics for per-operation allocation.
`operations/trial=...` and `fixture_seed=1` make this boundary machine-readable.

The concurrent durable matrix has concurrency 1/2/4/8/16, values 128 B/4 KiB,
and commit batches 1/16:

```sh
GOWORK=off go test ./benchmarks/dgraph_durability \
  -run '^$' -bench '^BenchmarkDgraphShapedConcurrentDurable' \
  -benchmem -benchtime=100x -count=5
```

It reports per-acknowledgement p50/p95/p99 latency. TreeDB rows additionally
report command-WAL flush/sync, group acknowledgements and syncs, aggregate
value-log sync, checkpoint, iterator rotation/source, and queue metrics where
the public `Stats` map exposes them. `command_wal_group_commits/sync`,
`command_wal_group_syncs/ack`, and `command_wal_group_size_max` are derived
from production coordinator counters; they are not inferred from benchmark
concurrency. The same rows also report command-WAL and value-log bytes written
per acknowledged commit, pre/post-checkpoint logical file bytes, and untimed
checkpoint, close, and verified reopen latency. These lifecycle measurements
run after the acknowledgement timer and therefore do not change `ns/op` or
the acknowledgement percentiles.

The MVCC package has a separately named write/read-alternating row:

```sh
GOWORK=off go test ./TreeDB/mvcc -run '^$' \
  -bench '^BenchmarkCommitAtGetAtInterleaved$' -benchmem -count=5
```

This is distinct from `BenchmarkGetAt`, whose history is preloaded before the
timer and whose timed phase is read-only.

### Result schema and acceptance

The schema is `dgraph-durability-v2`. Archive the raw Go benchmark output with:

- repository SHA and dirty state;
- exact command and repeat count;
- Go version, `goos`, `goarch`, package, and CPU;
- host kernel and filesystem type for the temporary database path;
- fixture seed, operation count, value size, batch size, concurrency, and
  durability class encoded in each row name;
- raw repeats. Use the median only after retaining every repeat and recording
  any exclusion with a concrete environmental or correctness reason.

Never exclude a slow run solely because it is slow. Do not aggregate relaxed
and durable rows, or fixed-work and adaptively sized rows. Profile artifacts use
`<backend>_<class>_<shape>_<sha>.{cpu,mem,block,mutex,trace}` so attribution can
be matched back to the exact candidate.

For syscall attribution on Linux, select one row at a time (Go benchmark names
are slash-separated regular expressions):

```sh
GOWORK=off strace -f -c -e trace=fsync,fdatasync,msync \
  go test ./benchmarks/dgraph_durability \
  -run '^$' \
  -bench '^BenchmarkDgraphShapedCommit$/^durable$/^Badger$/^batch=1$/^value=128$' \
  -benchtime=100x -count=1
```

Replace `Badger` with `TreeDB-command-WAL` for the matched TreeDB row. The
benchmark also reports TreeDB command-WAL sync, flush, and checkpoint deltas per
write commit; syscall traces include database open, warmup, and close work and
therefore should be interpreted as attribution rather than exact per-commit
counts.

Do not compare a `relaxed` row with a `durable` row as if they promise the same
power-loss behavior. The benchmark deliberately excludes TreeDB's WAL-off mode:
Badger has no equivalent Dgraph transaction mode, so including it would not be
a durability-matched A/B comparison.
