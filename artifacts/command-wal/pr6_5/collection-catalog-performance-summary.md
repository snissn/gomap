# PR6.5 Collection/Catalog Command-WAL Performance Polish

Tracker: https://github.com/snissn/gomap/issues/1529
Follow-up blocker: https://github.com/snissn/gomap/issues/1584

PR6.5 consolidates the collection/catalog command-WAL performance evidence from PR4, PR5, and PR6 before PR7 matrix hardening and PR9 public raw KV cutover.

## Source Artifacts

- `artifacts/command-wal/pr4/collection-insert-delete-microbench-benchstat.txt`
- `artifacts/command-wal/pr5/collection-update-microbench-benchstat.txt`
- `artifacts/command-wal/pr6/bench.txt`

## Strict Throughput Gate

The default-ready gate is strict parity-plus:

- command-WAL throughput divided by the relevant WAL-off baseline throughput must be `>1.01x`;
- anything not strictly greater than `1.01x` fails;
- sub-parity evidence is recorded as a blocker, not accepted as default-ready performance.

## Current Ratios

Lane | Baseline | Command WAL | Ratio | Default-ready decision
---|---:|---:|---:|---
insert unindexed | 1.205M docs/s | 873.2k docs/s | 0.725x | blocked by #1584
insert indexed | 482.1k docs/s | 479.8k docs/s | 0.995x | blocked by #1584
delete unindexed | 866.9k docs/s | 807.8k docs/s | 0.932x | blocked by #1584
delete indexed | 407.8k docs/s | 383.2k docs/s | 0.940x | blocked by #1584
update unindexed | 793.3k docs/s | 649.0k docs/s | 0.818x | blocked by #1584
update indexed | 486.8k docs/s | 412.0k docs/s | 0.846x | blocked by #1584
catalog create | 18.223k collections/s | 51.910k collections/s | 2.848x | passes strict gate

## Decision

PR6.5 does not broaden the default command-WAL cutover to collection insert/delete/update lanes. Those lanes remain supported command kinds, but not default-ready performance lanes, until #1584 clears strict `>1.01x` evidence.

PR9 may proceed as a public raw KV command-WAL cutover because its acceptance artifact and PR body scope the default performance gate to public raw KV `Set`, `Delete`, and `Batch.Write` lanes. Collection/catalog command families must not be cited as default-ready based on PR9 raw KV performance evidence.
