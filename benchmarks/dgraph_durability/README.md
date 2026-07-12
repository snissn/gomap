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
