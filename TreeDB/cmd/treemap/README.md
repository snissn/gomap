`treemap compact DIR -rw` runs the existing storage compact operation.
`-json` emits its original backend report. `-scope=index` retains the index-only
operation; `-mode=full|quick|exhaustive` retains the existing mode policies.

For the opt-in command-WAL maintenance endpoint:

```sh
treemap compact DIR -rw -json -mode=exhaustive -sync-each-phase \
  -rewrite-batch-size=8192 -leaf-pack-max-passes=64 -command-wal-settle
```

Scope must be `all`, mode must be `full` or `exhaustive`, and the store must
already persist the command-WAL required feature and a compatible command-WAL
profile. Admission is checked before writable Open/replay. No durability profile
or public CLI default is overridden.

The fixed sequence is one applied `CompactStorage`, released state-token/WAL-LSN/
recoverable-root summary captures, `RefreshCommandWALCheckpointFallback`,
`Checkpoint`, released post-refresh captures, one `LeafGenerationGC`, the dedicated
`CompactStoragePlan` dry-run audit, then cleanup. Refresh may lawfully do nothing
or advance CommitSeq by one while preserving user/system roots, applied command
LSN, maximum entry revision and next command LSN. It creates no command frame.
There is no unlogged commit, synthetic key, second applying compact or settle loop.
Cancellation is checked at operation boundaries; refresh/checkpoint have no
context argument.

JSON emits a versioned `compact-command-wal-settle-v1` receipt. It retains the raw
initial applied report, final dry-run audit, refresh basis/result/WAL frontier/root
summaries, actual operation statuses and GC counters. Errors retain partial work
and cleanup status. `completed` describes successful execution and cleanup;
final policy completion is a separate audit result. Planned audit phases are not
executed phases. Retain initial debt and verify full values, misses, live-key
census and reopen correctness on a released independent copy.

[The maintenance capture guide](../../../scripts/quicksilver_maintenance.md)
describes raw receipt binding, matched endpoint controls, completed baseline
calibration and whole-command timing/RSS, including every operation and cleanup.
Native full-fixture rehearsal remains required before qualifying performance.
