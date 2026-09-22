# YCSB MongoDB / TreeDB Latest Main Rerun

Status: current checked-in external `go-ycsb` evidence for MongoDB, TreeDB
nativewire, and TreeDB Mongo gateway after rerunning the standard YCSB matrix on
latest `origin/main` merge commit `d7407b81cc5712374ca8c1588cfb05f6f7d8490d`.

This report refreshes `docs/benchmarks/ycsb_post_update_stack_2026-06-02.md` on
the same developer machine. The commit range since that report includes TreeDB
leaf-cache, key-compare, batch-merge, scratch, and vector/scoring work; this
YCSB run measures whether those latest `main` changes also moved the standard
nativewire and Mongo-gateway YCSB shape.

These are developer-machine results. Treat them as current local engineering
evidence, not a formal product benchmark.

## Benchmark Boundary

Workload:

- `recordcount=100000`
- `operationcount=10000`
- `threadcount=16`
- load phase: 100,000 inserts
- run phase: 10,000 operations, approximately 95% reads and 5% updates
- all clients used local loopback TCP
- TreeDB Mongo gateway document format: `bson`

Targets:

- MongoDB 8 through the `go-ycsb mongodb` binding
- `treedb-native` through the `go-ycsb treedb-native` binding
- TreeDB Mongo gateway through the `go-ycsb mongodb` binding

Profiles:

- `command_wal_durable`
- `command_wal_relaxed`
- `bench`, as the explicit no-WAL benchmark-only ceiling

Validity:

- Every parsed YCSB phase had zero `*_ERROR` operation counts.
- MongoDB was run once in the durable artifact directory with three run repeats.
- TreeDB profiles were run with three run repeats each.
- Headline run rows use the median run throughput repeat for that target/profile.

Compatibility note:

- The checked-out `go-ycsb` master at `00a49395a30a4c1b9671200b8f0ec81a59c98546`
  failed against latest gomap before the run because TreeDB now returns
  `collection_meta` version 3. The benchmark used a local one-line compatibility
  patch in the external `go-ycsb` checkout to let the treedb-native decoder
  accept version 3 metadata. That patch only affects benchmark-client metadata
  decoding and does not change the measured gomap server binaries. Upstream
  `go-ycsb` should be updated before the next clean rerun.

## Host Context

Captured on 2026-06-03 HST / 2026-06-04 UTC.

```text
host: mikers-B560-DS3H-AC-Y1
kernel: Linux 6.8.0-111-generic x86_64
go: go version go1.25.0 linux/amd64
cpu: 11th Gen Intel(R) Core(TM) i5-11400F @ 2.60GHz
logical CPUs: 12
memory: 31Gi
clocksource: tsc
```

Source versions:

```text
gomap: d7407b81cc5712374ca8c1588cfb05f6f7d8490d
gomap branch: main
go-ycsb: 00a49395a30a4c1b9671200b8f0ec81a59c98546
go-ycsb branch: master + local collection_meta v3 decoder compatibility patch
MongoDB image: mongo:8
MongoDB image id: sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54
MongoDB image digest: mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62
```

Primary artifact root:

```text
/tmp/treedb_ycsb_current_20260603_205329
```

Artifact directories:

```text
/tmp/treedb_ycsb_current_20260603_205329/command_wal_durable
/tmp/treedb_ycsb_current_20260603_205329/command_wal_relaxed
/tmp/treedb_ycsb_current_20260603_205329/bench
/tmp/treedb_ycsb_current_20260603_205329/go-ycsb-local-compat.patch
```

## Reproduction

The sweep used the checked-in comparison script from this repo plus a temporary
external `go-ycsb` checkout with this compatibility patch:

```diff
diff --git a/db/treedbnative/nativewire.go b/db/treedbnative/nativewire.go
index 93370ae..6d82c66 100644
--- a/db/treedbnative/nativewire.go
+++ b/db/treedbnative/nativewire.go
@@ -1584,7 +1584,7 @@ func decodeCollectionMeta(src []byte) (wireCollectionMeta, error) {
     if err != nil {
         return wireCollectionMeta{}, err
     }
-    if version != 1 && version != 2 {
+    if version != 1 && version != 2 && version != 3 {
         return wireCollectionMeta{}, wireError(wireErrUnsupportedVersion, "collection_meta version %d", version)
     }
     off += n
```

Commands:

```sh
cd /home/mikers/dev/snissn/gomap

GOYCSB_DIR=/tmp/go-ycsb_treedb_meta_v3_20260603_205317
RUN_ROOT=/tmp/treedb_ycsb_current_$(date +%Y%m%d_%H%M%S)
mkdir -p "$RUN_ROOT"

GOWORK=off GOYCSB_DIR="$GOYCSB_DIR" OUT_DIR="$RUN_ROOT/command_wal_durable" \
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

GOWORK=off GOYCSB_DIR="$GOYCSB_DIR" OUT_DIR="$RUN_ROOT/command_wal_relaxed" \
TREEDB_PROFILE=command_wal_relaxed \
MONGODB_MODE=skip \
NATIVE_ADDR=127.0.0.1:17131 \
TREEDB_MONGO_ADDR=127.0.0.1:27131 \
TREEDB_MONGO_DOCUMENT_FORMAT=bson \
RUN_REPEATS=3 \
BUILD=false \
BUILD_GOYCSB=false \
scripts/ycsb_compare_mongodb_treedb.sh

GOWORK=off GOYCSB_DIR="$GOYCSB_DIR" OUT_DIR="$RUN_ROOT/bench" \
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

`GOWORK=off` was needed in this local checkout because a parent-directory
`go.work` did not include this gomap clone.

Each artifact directory includes `host.txt`, `commands.txt`, `summary.tsv`,
`summary.md`, raw YCSB outputs, server logs, and DB directories.

## Headline Results

Run rows use the median total-throughput repeat for each target/profile.

| target | profile | load ops/sec | median run repeat | run ops/sec | run avg us | run p95 us | run p99 us | run max us | errors |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MongoDB | baseline | 38,755.4 | 2 | 26,494.1 | 595.0 | 984.0 | 1,275.0 | 2,389.0 | 0 |
| TreeDB nativewire | `command_wal_durable` | 83,217.0 | 1 | 135,318.6 | 113.0 | 203.0 | 367.0 | 1,211.0 | 0 |
| TreeDB Mongo gateway | `command_wal_durable` | 68,218.2 | 1 | 80,628.4 | 199.0 | 388.0 | 649.0 | 6,623.0 | 0 |
| TreeDB nativewire | `command_wal_relaxed` | 84,102.6 | 2 | 141,333.5 | 108.0 | 191.0 | 312.0 | 1,429.0 | 0 |
| TreeDB Mongo gateway | `command_wal_relaxed` | 68,372.2 | 1 | 76,859.7 | 212.0 | 426.0 | 757.0 | 7,751.0 | 0 |
| TreeDB nativewire | `bench` no-WAL | 92,884.1 | 3 | 141,857.8 | 108.0 | 189.0 | 322.0 | 1,133.0 | 0 |
| TreeDB Mongo gateway | `bench` no-WAL | 83,561.3 | 1 | 87,343.1 | 175.0 | 352.0 | 534.0 | 1,114.0 | 0 |

## Run Repeats

| target | profile | repeat 1 ops/sec | repeat 2 ops/sec | repeat 3 ops/sec | selected median |
| --- | --- | ---: | ---: | ---: | ---: |
| MongoDB | baseline | 26,644.2 | 26,494.1 | 26,358.7 | 26,494.1 |
| TreeDB nativewire | `command_wal_durable` | 135,318.6 | 130,636.2 | 139,747.1 | 135,318.6 |
| TreeDB Mongo gateway | `command_wal_durable` | 80,628.4 | 85,137.8 | 80,258.1 | 80,628.4 |
| TreeDB nativewire | `command_wal_relaxed` | 138,637.9 | 141,333.5 | 141,334.5 | 141,333.5 |
| TreeDB Mongo gateway | `command_wal_relaxed` | 76,859.7 | 86,600.2 | 73,369.3 | 76,859.7 |
| TreeDB nativewire | `bench` no-WAL | 145,263.2 | 137,604.7 | 141,857.8 | 141,857.8 |
| TreeDB Mongo gateway | `bench` no-WAL | 87,343.1 | 89,017.3 | 86,660.5 | 87,343.1 |

The Mongo gateway `command_wal_relaxed` repeats are noisier than the durable and
bench cells, and both gateway command-WAL cells include one max-latency outlier
above 6 ms. The median throughput still remains above the June 2 rows.

## Selected Run Detail

The rows below show the operation mix for each selected median repeat.

| target | profile | op | count | ops/sec | avg us | p50 us | p95 us | p99 us | max us |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MongoDB | baseline | READ | 9,493 | 25,149.3 | 603.0 | 572.0 | 991.0 | 1,283.0 | 2,389.0 |
| MongoDB | baseline | UPDATE | 507 | 1,349.7 | 440.0 | 410.0 | 805.0 | 1,055.0 | 1,367.0 |
| MongoDB | baseline | TOTAL | 10,000 | 26,494.1 | 595.0 | 564.0 | 984.0 | 1,275.0 | 2,389.0 |
| TreeDB nativewire | `command_wal_durable` | READ | 9,472 | 128,220.5 | 109.0 | 103.0 | 188.0 | 337.0 | 748.0 |
| TreeDB nativewire | `command_wal_durable` | UPDATE | 528 | 7,230.1 | 186.0 | 151.0 | 393.0 | 886.0 | 1,211.0 |
| TreeDB nativewire | `command_wal_durable` | TOTAL | 10,000 | 135,318.6 | 113.0 | 105.0 | 203.0 | 367.0 | 1,211.0 |
| TreeDB Mongo gateway | `command_wal_durable` | READ | 9,481 | 76,317.7 | 195.0 | 155.0 | 371.0 | 633.0 | 6,623.0 |
| TreeDB Mongo gateway | `command_wal_durable` | UPDATE | 519 | 4,208.9 | 282.0 | 242.0 | 559.0 | 783.0 | 2,805.0 |
| TreeDB Mongo gateway | `command_wal_durable` | TOTAL | 10,000 | 80,628.4 | 199.0 | 158.0 | 388.0 | 649.0 | 6,623.0 |
| TreeDB nativewire | `command_wal_relaxed` | READ | 9,515 | 134,501.6 | 104.0 | 99.0 | 176.0 | 298.0 | 741.0 |
| TreeDB nativewire | `command_wal_relaxed` | UPDATE | 485 | 6,922.4 | 179.0 | 141.0 | 340.0 | 832.0 | 1,429.0 |
| TreeDB nativewire | `command_wal_relaxed` | TOTAL | 10,000 | 141,333.5 | 108.0 | 101.0 | 191.0 | 312.0 | 1,429.0 |
| TreeDB Mongo gateway | `command_wal_relaxed` | READ | 9,502 | 73,061.5 | 206.0 | 164.0 | 410.0 | 718.0 | 7,295.0 |
| TreeDB Mongo gateway | `command_wal_relaxed` | UPDATE | 498 | 3,843.3 | 321.0 | 252.0 | 579.0 | 1,093.0 | 7,751.0 |
| TreeDB Mongo gateway | `command_wal_relaxed` | TOTAL | 10,000 | 76,859.7 | 212.0 | 167.0 | 426.0 | 757.0 | 7,751.0 |
| TreeDB nativewire | `bench` no-WAL | READ | 9,541 | 135,220.6 | 105.0 | 99.0 | 184.0 | 303.0 | 707.0 |
| TreeDB nativewire | `bench` no-WAL | UPDATE | 459 | 6,617.0 | 160.0 | 126.0 | 291.0 | 905.0 | 1,133.0 |
| TreeDB nativewire | `bench` no-WAL | TOTAL | 10,000 | 141,857.8 | 108.0 | 101.0 | 189.0 | 322.0 | 1,133.0 |
| TreeDB Mongo gateway | `bench` no-WAL | READ | 9,474 | 82,702.7 | 174.0 | 153.0 | 350.0 | 530.0 | 1,114.0 |
| TreeDB Mongo gateway | `bench` no-WAL | UPDATE | 526 | 4,642.2 | 194.0 | 176.0 | 395.0 | 586.0 | 806.0 |
| TreeDB Mongo gateway | `bench` no-WAL | TOTAL | 10,000 | 87,343.1 | 175.0 | 154.0 | 352.0 | 534.0 | 1,114.0 |

## Comparison With June 2 Current Report

Previous checked-in report:
`docs/benchmarks/ycsb_post_update_stack_2026-06-02.md`.

| target | profile | previous load | current load | load delta | previous run | current run | run delta |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| MongoDB | baseline | 33,255.6 | 38,755.4 | +16.5% | 26,102.5 | 26,494.1 | +1.5% |
| TreeDB nativewire | `command_wal_durable` | 77,065.3 | 83,217.0 | +8.0% | 120,500.7 | 135,318.6 | +12.3% |
| TreeDB Mongo gateway | `command_wal_durable` | 57,370.1 | 68,218.2 | +18.9% | 74,544.7 | 80,628.4 | +8.2% |
| TreeDB nativewire | `command_wal_relaxed` | 76,411.7 | 84,102.6 | +10.1% | 126,762.5 | 141,333.5 | +11.5% |
| TreeDB Mongo gateway | `command_wal_relaxed` | 62,444.0 | 68,372.2 | +9.5% | 75,371.9 | 76,859.7 | +2.0% |
| TreeDB nativewire | `bench` no-WAL | 85,201.7 | 92,884.1 | +9.0% | 134,415.5 | 141,857.8 | +5.5% |
| TreeDB Mongo gateway | `bench` no-WAL | 75,033.3 | 83,561.3 | +11.4% | 79,357.6 | 87,343.1 | +10.1% |

## Summary

The latest `origin/main` rerun is materially faster in the nativewire run phase:
`command_wal_durable` rose from about 120.5k OPS to 135.3k OPS (+12.3%), and
`command_wal_relaxed` rose from about 126.8k OPS to 141.3k OPS (+11.5%). The
no-WAL `bench` nativewire ceiling improved more modestly, from 134.4k OPS to
141.9k OPS (+5.5%).

TreeDB Mongo gateway also improved in this rerun, but less uniformly: durable
run throughput rose about 8.2%, relaxed was effectively near the old range at
+2.0%, and no-WAL `bench` rose about 10.1%. The MongoDB run baseline was nearly
flat (+1.5%), while MongoDB load throughput moved enough (+16.5%) to treat load
rows as more host/noise-sensitive than run rows.

Net: the latest main changes do appear to move the standard YCSB numbers on this
hardware, especially for TreeDB nativewire command-WAL runs. The remaining
Mongo-gateway command-WAL shape still shows gateway/request overhead and some
repeat-to-repeat tail noise.
