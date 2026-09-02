# TreeDB Out-of-Core Smoke Harness

This harness is the small implementation slice for issue #1138. It is a
CI-sized budget-pressure smoke, not a true larger-than-RAM benchmark. The goal is
to give mmap/cache work a repeatable guardrail before deeper changes in #1135
and #1136.

## Run

```bash
make bench-out-of-core-smoke
```

Equivalent direct command:

```bash
./scripts/bench_out_of_core_smoke.sh
```

Useful overrides:

```bash
./scripts/bench_out_of_core_smoke.sh \
  -raw-keys 10000 \
  -collection-docs 3000 \
  -batch-size 500 \
  -formats template-v1,bson,json \
  -indexes 0,1,2 \
  -cache-budget-bytes 32768 \
  -retired-mmap-budget-bytes 32768 \
  -max-dead-mappings 2
```

The script creates a timestamped run directory under the host `os.TempDir()`
location unless `-out-dir` is provided.

## Outputs

Each run writes:

- `out_of_core_smoke_summary.md`
- `out_of_core_smoke_results.json`
- `out_of_core_smoke_results.ndjson`
- per-workload command logs under `logs/`
- kept raw TreeDB and collection fixture directories

Every row labels the workload shape as `raw`, `collection`, or `mongo` so raw
TreeDB rows cannot be confused with collection rows.

## What The Smoke Covers

The default run includes:

- raw TreeDB batch write, parallel read, overwrite, and post-overwrite read
- collection inserts with 0, 1, and 2 indexes
- collection document formats `template-v1`, `bson`, and `json`
- small `leaf_vlog` segment targets to force churn
- tiny reported cache/retired-mmap budgets
- a low `TREEDB_VLOG_MAX_DEAD_MAPPINGS` child-process setting
- selected mmap/cache stats from TreeDB

Mongo gateway coverage is intentionally deferred to #1141 by default because it
mixes client, protocol, gateway, BSON conversion, and storage costs. Passing
`-include-mongo` adds one tiny in-process TreeDB Mongo-gateway smoke row labeled
with `shape=mongo`; keep it opt-in unless the run is explicitly checking gateway
behavior.

## Guardrails

The report warns or fails for:

- missing raw/collection/Mongo shape labels
- missing document/key count when bytes-per-item is reported
- pressure/out-of-core rows without total bytes or budgets
- datasets that do not exceed the configured budget by more than 2x
- TreeDB rows without mmap counters
- dead mmap bytes exceeding the configured retired-mmap smoke budget

The smoke harness intentionally uses configured budgets rather than relying on
physical RAM exhaustion. True large local and larger-than-memory validation is
split into #1139, #1140, and #1141.

## Relationship To Follow-Up Work

- #1135 should use this harness to prove mmap lifetime changes remain bounded
  and avoid the current-leaf `ReadAt` cliff.
- #1136 should use this harness to prove any current-leaf cache is bounded and
  eviction-safe.
- #1132 should use the later Mongo/large-dataset tracks after the storage
  read-path baseline is cleaner.
