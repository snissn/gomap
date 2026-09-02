# TreeDB Downstream Validation Checklist

Use this checklist before opening or updating downstream adapter PRs in
`cosmos-db`, Ironbird, Celestia, Cosmos SDK, or similar forks.

## Required Inputs

- Pin the gomap commit or tag in the downstream module and record it in the
  report. Do not rely on a floating branch name.
- State the TreeDB profile used by the adapter: `command_wal_durable`,
  `command_wal_relaxed`, `no_wal_fast`, or benchmark/test-only `bench_unsafe`.
- State any adapter overrides after applying the profile, including value-log
  pointer threshold, compression policy, background maintenance, flush
  threshold, cache size, and memory limits.
- Confirm the adapter reaches the current public command-WAL path. Expected
  counters include:
  - `treedb.command_wal.enabled=true`
  - `treedb.cache.redo_log.mode=external_command_wal`
  - `treedb.cache.redo_log.enabled=false`
  - `treedb.cache.command_wal.external_durability=true` for durable public
    sync coverage
  - `treedb.command_wal.live_accepted_*`
  - `treedb.command_wal.live_covered_*`
  - `treedb.applied_command_lsn`

## Required Semantics

- Treat `WriteSync` / `Batch.WriteSync` as command-WAL sync boundaries. They
  should sync command-WAL/value-log durability input, not force a backend
  `Checkpoint()` per write.
- Treat `Checkpoint()` as backend publication/settling work. It is useful for
  settled disk-footprint measurements and explicit recovery coverage, but it is
  not the normal per-transaction durability barrier.
- Treat `Close()` as a final drain/cleanup boundary. A benchmark that times
  close should report that separately from the steady-state load window.
- For public raw key/value command-WAL coverage, `Set`, `SetSync`, `Delete`,
  `DeleteSync`, `DeleteRange`, `Batch.Write`, and `Batch.WriteSync` are
  supported. Callback-based `Update` / `UpdateSync` still fail closed under
  command WAL.
- Legacy cached redo-journal replay is compatibility/forensic behavior only.
  Current command-WAL directories should fail closed on replayable legacy redo
  unless explicit legacy replay is requested.

## Required Report Counters

Include these counter groups in every TreeDB-vs-LevelDB benchmark report:

- Command-WAL frame coverage:
  - `treedb.command_wal.enabled`
  - `treedb.command_wal.required_feature`
  - `treedb.command_wal.live_accepted_frames`
  - `treedb.command_wal.live_accepted_max_lsn`
  - `treedb.command_wal.live_covered_frames`
  - `treedb.command_wal.live_covered_max_lsn`
  - `treedb.applied_command_lsn`
- Checkpoint/backend publication:
  - `treedb.commit_seq`
  - `treedb.public.checkpoint.calls_total`
  - `treedb.cache.checkpoint.runs`
  - `treedb.cache.checkpoint.total_ms`
  - `treedb.cache.checkpoint.max_ms`
  - `treedb.cache.checkpoint.stage.*`
- Load-window resource counters:
  - wall time
  - effective ops/s and runtime TPS, when both are available
  - RSS/high-water memory
  - DB directory bytes
  - command-WAL bytes before checkpoint when reported
  - durable storage bytes with command-WAL bytes excluded when the report
    claims settled storage footprint

## Minimum Benchmark Rows

Do not claim TreeDB-vs-LevelDB behavior from one short row.

At minimum include:

- A low-fanout or plain-send row that represents fixed per-operation overhead.
- A moderate/high-fanout row that puts real pressure on storage write/read
  amplification.
- A settled-state row where explicit checkpoint/close/reopen work is separated
  from the measured load window.
- A long enough load window that startup, genesis, seeding, final checkpoint,
  and close do not dominate. For Ironbird-style TPS reports this should normally
  be minutes, not a one-second burst.
- Matching LevelDB and TreeDB rows with the same validator/node count, wallet
  count, transaction shape, value size, batching, and duration gates.

## Minimum Test Evidence

Before opening an upstream adapter PR, run the downstream test or benchmark
suite that proves:

- the adapter chooses the expected TreeDB profile;
- `WriteSync` / `Batch.WriteSync` survive reopen without requiring a checkpoint;
- `DeleteRange` either routes to the supported TreeDB command-WAL raw-KV path
  or is intentionally not exposed by the adapter;
- report output includes command-WAL and checkpoint counter groups; and
- benchmark scripts fail if the accepted load window is too short to interpret.

## Non-Claims

- A benchmark-only `bench_unsafe` profile result is an unsafe ceiling, not production
  durability evidence.
- Lower checkpoint time does not by itself prove better transaction throughput.
- Higher TPS without command-WAL counters does not prove the adapter actually
  exercised TreeDB command-WAL.
- Historical `fast`, `durable`, or `wal_on_fast` unified-bench presets are
  cross-DB benchmark-runner labels, not public TreeDB server profile names.
