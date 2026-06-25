# TreeDB cached-write default policy (#2996)

TreeDB's default cached-write policy resolves apply concurrency and journal/value-log
lanes together so raising `GOMAXPROCS` does not silently widen lane fanout and
break backlog coalescing.

## Resolved defaults

- `FlushAdmissionPolicyAuto` admits span-native apply + backlog coalescing only
  when the low-concurrency and durability guardrails pass.
- Default apply concurrency is the detected physical-core count capped by
  `GOMAXPROCS` and 8. If physical-core detection is unavailable, TreeDB falls
  back to the previous `min(GOMAXPROCS, 8)` bound.
- A configured `FlushApplyConcurrency` is an explicit override and is capped only
  by `GOMAXPROCS`; `FlushAdmissionPolicyExplicit` continues to preserve c4/c8/c16
  experiments.
- Default journal/value-log lanes are coalescing-safe:
  - hot/warm/cold value-log generation: 3 total lanes (one hot, one warm, one cold);
  - generation off: 1 hot lane.
- A configured `JournalLanes` value is authoritative. Wider lane topologies remain
  explicit benchmark controls.
- `FlushAdmissionPolicyOff` remains the rollback path and force-disables
  span-native apply, backlog coalescing, and apply worker-pool concurrency.

No on-disk format changes are involved.

## Reporting

Default rows should include these stats/output fields when comparing runs:

- `treedb.flush_admission.flush_apply_concurrency`
- `treedb.flush_admission.flush_apply_concurrency_defaulted`
- `treedb.flush_admission.gomaxprocs`
- `treedb.flush_admission.physical_cores`
- `treedb.cache.journal_lanes.configured/defaulted/effective/hot/warm/cold`
- `treedb.cache.memtable_shards`
- lane/coalescing counters such as
  `treedb.cache.flush_backlog_coalescing.skip.reason.lane_barrier_total` and
  `treedb.cache.flush_span_run.ops_per_span`

## Full benchmark rows for final gate

Use the issue's 10MM shape on the same host/commit pair for before/after rows:

```sh
GOMAXPROCS=16 ./bin/unified-bench \
  -dbs treedb \
  -test random_write \
  -keys 10000000 \
  -valsize 128 \
  -batchsize 8000 \
  -format markdown \
  -progress=false \
  -treedb-index-outer-leaves-in-vlog \
  -treedb-flush-admission-policy auto \
  -treedb-flush-apply-span-native \
  -treedb-flush-backlog-coalescing \
  -treedb-flush-apply-min-entries 1 \
  -treedb-flush-apply-min-spans 1 \
  -treedb-flush-apply-min-bytes 1
```

Safe explicit control:

```sh
GOMAXPROCS=16 ./bin/unified-bench \
  -dbs treedb \
  -test random_write \
  -keys 10000000 \
  -valsize 128 \
  -batchsize 8000 \
  -format markdown \
  -progress=false \
  -treedb-index-outer-leaves-in-vlog \
  -treedb-flush-admission-policy explicit \
  -treedb-flush-apply-span-native \
  -treedb-flush-backlog-coalescing \
  -treedb-flush-apply-min-entries 1 \
  -treedb-flush-apply-min-spans 1 \
  -treedb-flush-apply-min-bytes 1 \
  -treedb-flush-apply-concurrency 16 \
  -treedb-journal-lanes 3
```
