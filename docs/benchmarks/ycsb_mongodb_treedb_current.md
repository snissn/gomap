# YCSB MongoDB / TreeDB Benchmark Status

This page is the current entry point for external `go-ycsb` comparisons across:

- MongoDB through the `go-ycsb mongodb` binding.
- `treedb-native` through the `go-ycsb treedb-native` binding.
- TreeDB Mongo gateway through the `go-ycsb mongodb` binding.

It explains which checked-in reports are current, which are historical, and
which cells should be rerun next.

## Current Canonical Report

Use `docs/benchmarks/ycsb_profile_closeout_2026-06-01.md` as the current
checked-in command-WAL profile evidence until a fresh post-#2164 rerun lands.

That closeout report covers the intended public TreeDB profile surface:

- `command_wal_durable`
- `command_wal_relaxed`
- `bench`, as the explicit no-WAL benchmark-only ceiling

It also records zero YCSB operation errors and separates the repeated `bench`
ceiling run from an initial slow no-WAL Mongo-gateway run that did not reproduce.

Important freshness note: PR #2164 removed mandatory command-WAL payload
SHA-256 hashing. Command-WAL throughput numbers collected before that PR remain
valid historical evidence, but should not be used as final current headline
numbers without a rerun on the new command-WAL frame format. The no-WAL `bench`
rows are less directly affected, but should still be rerun in the same matrix as
a same-host ceiling/control.

## Historical Reports

| report | status | use |
| --- | --- | --- |
| `docs/benchmarks/ycsb_mongodb_treedb_2026-05-31.md` | Historical legacy-profile report. | Keep for the original MongoDB / TreeDB native / TreeDB Mongo comparison and the post-load Mongo-gateway cliff attribution. Do not use its `fast` rows as current public-profile guidance. |
| `docs/benchmarks/ycsb_treedb_profile_sweep_2026-06-01.md` | Superseded initial command-WAL sweep. | Keep as prior-run evidence. Prefer the closeout report for profile-surface conclusions. |
| `docs/benchmarks/ycsb_profile_closeout_2026-06-01.md` | Current checked-in command-WAL/public-profile report until post-#2164 rerun. | Use for current docs and profile-surface discussion, with the #2164 freshness caveat above. |
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

## Next Full Rerun Matrix

Run this matrix to replace the current pre-#2164 command-WAL numbers with fresh
headline evidence.

### Baseline And Durable Cell

For a formal fresh report, run the first cell with `RUN_REPEATS=3`. That repeats
the YCSB run phase for MongoDB, TreeDB nativewire, and TreeDB Mongo gateway
after each target's load phase, which is useful for separating first-run and
repeat behavior:

```sh
RUN_ROOT=/tmp/treedb_ycsb_post2164_$(date +%Y%m%d_%H%M%S)
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
BUILD_GOYCSB=false \
scripts/ycsb_compare_mongodb_treedb.sh
```

For a cheaper informal sweep, set `RUN_REPEATS=1` in this first command and use
the resulting MongoDB row as the once-per-matrix baseline. Then run the durable
TreeDB cell again with `MONGODB_MODE=skip RUN_REPEATS=3` if repeated durable
TreeDB rows are needed.

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
