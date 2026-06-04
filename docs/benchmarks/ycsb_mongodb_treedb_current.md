# YCSB MongoDB / TreeDB Benchmark Status

This page is the current entry point for external `go-ycsb` comparisons across:

- MongoDB through the `go-ycsb mongodb` binding.
- `treedb-native` through the `go-ycsb treedb-native` binding.
- TreeDB Mongo gateway through the `go-ycsb mongodb` binding.

It explains which checked-in reports are current, which are historical, and
which cells should be rerun next.

## Current Canonical Report

Use `docs/benchmarks/ycsb_latest_main_2026-06-03.md` as the current
checked-in external YCSB evidence for MongoDB, TreeDB nativewire, and TreeDB
Mongo gateway.

That report covers the intended public TreeDB profile surface:

- `command_wal_durable`
- `command_wal_relaxed`
- `bench`, as the explicit no-WAL benchmark-only ceiling

It records zero YCSB operation errors, uses three run repeats per target or
profile, and refreshes the June 2 post-update-path-stack numbers on latest
`origin/main` commit `d7407b81cc5712374ca8c1588cfb05f6f7d8490d`. Note that
this rerun used a temporary external `go-ycsb` compatibility patch so the
`treedb-native` binding accepts TreeDB `collection_meta` version 3.

## Current Headline

Run rows use the median total-throughput repeat from the June 3 HST / June 4
UTC latest-main report.

| target | profile | load ops/sec | run ops/sec | run avg us | run p99 us | run max us | errors |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| MongoDB | baseline | 38,755.4 | 26,494.1 | 595.0 | 1,275.0 | 2,389.0 | 0 |
| TreeDB nativewire | `command_wal_durable` | 83,217.0 | 135,318.6 | 113.0 | 367.0 | 1,211.0 | 0 |
| TreeDB Mongo gateway | `command_wal_durable` | 68,218.2 | 80,628.4 | 199.0 | 649.0 | 6,623.0 | 0 |
| TreeDB nativewire | `command_wal_relaxed` | 84,102.6 | 141,333.5 | 108.0 | 312.0 | 1,429.0 | 0 |
| TreeDB Mongo gateway | `command_wal_relaxed` | 68,372.2 | 76,859.7 | 212.0 | 757.0 | 7,751.0 | 0 |
| TreeDB nativewire | `bench` no-WAL | 92,884.1 | 141,857.8 | 108.0 | 322.0 | 1,133.0 | 0 |
| TreeDB Mongo gateway | `bench` no-WAL | 83,561.3 | 87,343.1 | 175.0 | 534.0 | 1,114.0 | 0 |

## Report Inventory

| report | status | use |
| --- | --- | --- |
| `docs/benchmarks/ycsb_latest_main_2026-06-03.md` | Current external YCSB report. | Use for current headline MongoDB / TreeDB nativewire / TreeDB Mongo gateway results on latest `origin/main` after the June 3 rerun. |
| `docs/benchmarks/ycsb_post_update_stack_2026-06-02.md` | Superseded latest-current report. | Keep for post-update-path-stack evidence before the latest main rerun and for comparison deltas. |
| `docs/benchmarks/ycsb_mongodb_treedb_2026-05-31.md` | Historical legacy-profile report. | Keep for the original MongoDB / TreeDB native / TreeDB Mongo comparison and the post-load Mongo-gateway cliff attribution. Do not use its `fast` rows as current public-profile guidance. |
| `docs/benchmarks/ycsb_treedb_profile_sweep_2026-06-01.md` | Superseded initial command-WAL sweep. | Keep as prior-run evidence. Prefer the current report for headline performance and the closeout report only for profile-surface conclusions. |
| `docs/benchmarks/ycsb_profile_closeout_2026-06-01.md` | Superseded profile-surface closeout. | Keep for profile-surface closeout evidence. Do not use as current headline performance after the latest main rerun. |
| `docs/benchmarks/mongo_gateway_compare_2026-04-29/` | Historical non-YCSB Mongo-gateway microbenchmark. | Useful for gateway-specific attribution, but not a substitute for external YCSB comparisons. |

## Standard Benchmark Boundary

Use this workload unless a report explicitly says otherwise:

- `recordcount=100000`
- `operationcount=10000`
- `threadcount=16`
- `table=usertable`
- load phase: 100,000 inserts
- run phase: 10,000 operations, roughly 95% reads and 5% updates
- local loopback TCP

The standard harness is:

```sh
scripts/ycsb_compare_mongodb_treedb.sh
```

The harness writes host metadata, exact commands, raw `go-ycsb` output,
`summary.tsv`, `summary.md`, server logs, and fresh DB directories under
`OUT_DIR`.

Any nonzero YCSB operation error counter such as `INSERT_ERROR`, `READ_ERROR`,
or `UPDATE_ERROR` invalidates that phase unless the report explicitly marks it
as exploratory known-bad evidence.

As of gomap `d7407b81cc5712374ca8c1588cfb05f6f7d8490d`, the external
`go-ycsb` `treedb-native` binding must understand `collection_meta` version 3.
If upstream `go-ycsb` has not yet been updated, apply the one-line compatibility
patch recorded in `docs/benchmarks/ycsb_latest_main_2026-06-03.md` before
rerunning the nativewire cells.

## Standard Full Rerun Matrix

Run this matrix to refresh the current headline evidence after performance,
profile, nativewire, or Mongo gateway changes.

### Baseline And Durable Cell

Run the first cell with `RUN_REPEATS=3`. That repeats the YCSB run phase for
MongoDB, TreeDB nativewire, and TreeDB Mongo gateway after each target's load
phase, which is useful for separating first-run and repeat behavior:

```sh
RUN_ROOT=/tmp/treedb_ycsb_current_$(date +%Y%m%d_%H%M%S)
mkdir -p "$RUN_ROOT"

OUT_DIR="$RUN_ROOT/command_wal_durable" \
TREEDB_PROFILE=command_wal_durable \
MONGODB_MODE=docker \
MONGODB_ADDR=127.0.0.1:27018 \
NATIVE_ADDR=127.0.0.1:17130 \
TREEDB_MONGO_ADDR=127.0.0.1:27130 \
TREEDB_MONGO_DOCUMENT_FORMAT=bson \
RUN_REPEATS=3 \
BUILD=true \
BUILD_GOYCSB=true \
scripts/ycsb_compare_mongodb_treedb.sh
```

For a cheaper informal sweep, set `RUN_REPEATS=1` in this first command and use
the resulting MongoDB row as the once-per-matrix baseline.

### Remaining TreeDB Profiles

Reuse the MongoDB baseline from the first cell and run the other TreeDB profile
cells:

```sh
OUT_DIR="$RUN_ROOT/command_wal_relaxed" \
TREEDB_PROFILE=command_wal_relaxed \
MONGODB_MODE=skip \
NATIVE_ADDR=127.0.0.1:17131 \
TREEDB_MONGO_ADDR=127.0.0.1:27131 \
TREEDB_MONGO_DOCUMENT_FORMAT=bson \
RUN_REPEATS=3 \
BUILD=false \
BUILD_GOYCSB=false \
scripts/ycsb_compare_mongodb_treedb.sh

OUT_DIR="$RUN_ROOT/bench" \
TREEDB_PROFILE=bench \
MONGODB_MODE=skip \
NATIVE_ADDR=127.0.0.1:17132 \
TREEDB_MONGO_ADDR=127.0.0.1:27132 \
TREEDB_MONGO_DOCUMENT_FORMAT=bson \
RUN_REPEATS=3 \
BUILD=false \
BUILD_GOYCSB=false \
scripts/ycsb_compare_mongodb_treedb.sh
```

Use `RUN_REPEATS=3` for TreeDB cells so reports can separate first-run-after-load
behavior from repeat or median behavior. `RUN_REPEATS` repeats the run phase on
the same loaded DB/server; it does not rebuild a fresh DB for each repeat.

## Optional Long Diagnostic Run

If command-WAL tail latency or first-run-after-load behavior looks suspicious,
add a longer durable-profile run:

```sh
OUT_DIR="$RUN_ROOT/command_wal_durable_100k_run" \
TREEDB_PROFILE=command_wal_durable \
MONGODB_MODE=skip \
NATIVE_ADDR=127.0.0.1:17133 \
TREEDB_MONGO_ADDR=127.0.0.1:27133 \
TREEDB_MONGO_DOCUMENT_FORMAT=bson \
OPERATIONCOUNT=100000 \
RUN_REPEATS=1 \
BUILD=false \
BUILD_GOYCSB=false \
scripts/ycsb_compare_mongodb_treedb.sh
```

Use that long run for profiling and tail attribution, not as a replacement for
the standard 10k-operation headline matrix.

## Report Requirements

Fresh reports should include:

- exact commands;
- host and hardware context;
- gomap commit and go-ycsb commit;
- MongoDB image and digest;
- TreeDB profile and document format;
- load and run ops/sec;
- average, p95, p99, and max latency;
- all YCSB `*_ERROR` counters;
- first-run versus repeat or median behavior;
- artifact directories.

Do not mix old `fast` profile rows into current public-profile headline tables.
If a historical row is useful for context, label it as historical and explain
which commit/profile produced it.
