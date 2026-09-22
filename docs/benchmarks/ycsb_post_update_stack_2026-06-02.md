# YCSB MongoDB / TreeDB Post Update-Path Stack

Status: current checked-in external `go-ycsb` evidence for MongoDB, TreeDB
nativewire, and TreeDB Mongo gateway after the June 2 YCSB update-path stack
landed through `main` merge commit `5017f75216c7e410c444b3736be2d44517e06c71`.

This report refreshes the previous June 1 command-WAL closeout numbers after:

- command-WAL payload SHA-256 hashing removal;
- nativewire BSON `$set` update support;
- `UpdateBSONSet` single-document fast-path work;
- command-WAL BSON set update combiner work;
- no-index BSON update mutation-lock hold-time reduction.

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

## Host Context

Captured on 2026-06-02 HST / 2026-06-02 UTC.

```text
host: mikers-B560-DS3H-AC-Y1
kernel: Linux 6.8.0-111-generic x86_64
go: go version go1.25.7 linux/amd64
cpu: 11th Gen Intel(R) Core(TM) i5-11400F @ 2.60GHz
logical CPUs: 12
memory: 31Gi
clocksource: tsc
```

Source versions:

```text
gomap: 5017f75216c7e410c444b3736be2d44517e06c71
gomap branch: main
go-ycsb: 00a49395a30a4c1b9671200b8f0ec81a59c98546
go-ycsb branch: master
MongoDB image: mongo:8
MongoDB image id: sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54
MongoDB image digest: mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62
```

Primary artifact root:

```text
/tmp/treedb_ycsb_post_update_stack_20260602_045120
```

Artifact directories:

```text
/tmp/treedb_ycsb_post_update_stack_20260602_045120/command_wal_durable
/tmp/treedb_ycsb_post_update_stack_20260602_045120/command_wal_relaxed
/tmp/treedb_ycsb_post_update_stack_20260602_045120/bench
```

## Reproduction

The sweep used the checked-in comparison script:

```sh
cd /home/mikers/dev/snissn/gomap-clean

RUN_ROOT=/tmp/treedb_ycsb_post_update_stack_$(date +%Y%m%d_%H%M%S)
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

Each artifact directory includes `host.txt`, `commands.txt`, `summary.tsv`,
`summary.md`, raw YCSB outputs, server logs, and DB directories.

## Headline Results

Run rows use the median total-throughput repeat for each target/profile.

| target | profile | load ops/sec | median run repeat | run ops/sec | run avg us | run p95 us | run p99 us | run max us | errors |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MongoDB | baseline | 33,255.6 | 3 | 26,102.5 | 601.0 | 1,048.0 | 1,461.0 | 3,401.0 | 0 |
| TreeDB nativewire | `command_wal_durable` | 77,065.3 | 2 | 120,500.7 | 128.0 | 244.0 | 477.0 | 1,475.0 | 0 |
| TreeDB Mongo gateway | `command_wal_durable` | 57,370.1 | 3 | 74,544.7 | 206.0 | 426.0 | 649.0 | 1,331.0 | 0 |
| TreeDB nativewire | `command_wal_relaxed` | 76,411.7 | 2 | 126,762.5 | 121.0 | 225.0 | 374.0 | 1,687.0 | 0 |
| TreeDB Mongo gateway | `command_wal_relaxed` | 62,444.0 | 3 | 75,371.9 | 203.0 | 409.0 | 629.0 | 1,674.0 | 0 |
| TreeDB nativewire | `bench` no-WAL | 85,201.7 | 2 | 134,415.5 | 114.0 | 212.0 | 365.0 | 969.0 | 0 |
| TreeDB Mongo gateway | `bench` no-WAL | 75,033.3 | 3 | 79,357.6 | 192.0 | 401.0 | 729.0 | 3,035.0 | 0 |

## Run Repeats

| target | profile | repeat 1 ops/sec | repeat 2 ops/sec | repeat 3 ops/sec | selected median |
| --- | --- | ---: | ---: | ---: | ---: |
| MongoDB | baseline | 26,263.9 | 25,579.7 | 26,102.5 | 26,102.5 |
| TreeDB nativewire | `command_wal_durable` | 122,093.1 | 120,500.7 | 117,078.8 | 120,500.7 |
| TreeDB Mongo gateway | `command_wal_durable` | 66,225.1 | 78,587.1 | 74,544.7 | 74,544.7 |
| TreeDB nativewire | `command_wal_relaxed` | 121,058.1 | 126,762.5 | 128,617.7 | 126,762.5 |
| TreeDB Mongo gateway | `command_wal_relaxed` | 74,298.2 | 76,338.3 | 75,371.9 | 75,371.9 |
| TreeDB nativewire | `bench` no-WAL | 133,589.4 | 134,415.5 | 138,742.9 | 134,415.5 |
| TreeDB Mongo gateway | `bench` no-WAL | 79,955.7 | 74,334.5 | 79,357.6 | 79,357.6 |

The durable Mongo gateway first run is lower than repeats 2 and 3, but this is
not the old post-load cliff shape: it had zero errors and a 7.5ms max, not a
100ms-scale foreground flush.

## Selected Run Detail

The rows below show the operation mix for each selected median repeat.

| target | profile | op | count | ops/sec | avg us | p50 us | p95 us | p99 us | max us |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MongoDB | baseline | READ | 9,533 | 24,881.8 | 604.0 | 559.0 | 1,051.0 | 1,462.0 | 3,401.0 |
| MongoDB | baseline | UPDATE | 467 | 1,221.2 | 527.0 | 486.0 | 946.0 | 1,285.0 | 2,383.0 |
| MongoDB | baseline | TOTAL | 10,000 | 26,102.5 | 601.0 | 555.0 | 1,048.0 | 1,461.0 | 3,401.0 |
| TreeDB nativewire | `command_wal_durable` | READ | 9,532 | 114,690.8 | 124.0 | 112.0 | 228.0 | 463.0 | 1,024.0 |
| TreeDB nativewire | `command_wal_durable` | UPDATE | 468 | 5,731.7 | 213.0 | 168.0 | 388.0 | 987.0 | 1,475.0 |
| TreeDB nativewire | `command_wal_durable` | TOTAL | 10,000 | 120,500.7 | 128.0 | 115.0 | 244.0 | 477.0 | 1,475.0 |
| TreeDB Mongo gateway | `command_wal_durable` | READ | 9,495 | 70,699.1 | 202.0 | 173.0 | 415.0 | 646.0 | 1,325.0 |
| TreeDB Mongo gateway | `command_wal_durable` | UPDATE | 505 | 3,773.1 | 280.0 | 253.0 | 503.0 | 771.0 | 1,331.0 |
| TreeDB Mongo gateway | `command_wal_durable` | TOTAL | 10,000 | 74,544.7 | 206.0 | 176.0 | 426.0 | 649.0 | 1,331.0 |
| TreeDB nativewire | `command_wal_relaxed` | READ | 9,505 | 120,528.6 | 117.0 | 110.0 | 209.0 | 348.0 | 1,655.0 |
| TreeDB nativewire | `command_wal_relaxed` | UPDATE | 495 | 6,335.3 | 200.0 | 158.0 | 452.0 | 865.0 | 1,687.0 |
| TreeDB nativewire | `command_wal_relaxed` | TOTAL | 10,000 | 126,762.5 | 121.0 | 112.0 | 225.0 | 374.0 | 1,687.0 |
| TreeDB Mongo gateway | `command_wal_relaxed` | READ | 9,514 | 71,724.8 | 199.0 | 172.0 | 398.0 | 621.0 | 1,674.0 |
| TreeDB Mongo gateway | `command_wal_relaxed` | UPDATE | 486 | 3,684.8 | 283.0 | 257.0 | 525.0 | 716.0 | 1,039.0 |
| TreeDB Mongo gateway | `command_wal_relaxed` | TOTAL | 10,000 | 75,371.9 | 203.0 | 175.0 | 409.0 | 629.0 | 1,674.0 |
| TreeDB nativewire | `bench` no-WAL | READ | 9,505 | 127,692.6 | 112.0 | 103.0 | 207.0 | 350.0 | 960.0 |
| TreeDB nativewire | `bench` no-WAL | UPDATE | 495 | 6,748.7 | 151.0 | 124.0 | 295.0 | 683.0 | 969.0 |
| TreeDB nativewire | `bench` no-WAL | TOTAL | 10,000 | 134,415.5 | 114.0 | 105.0 | 212.0 | 365.0 | 969.0 |
| TreeDB Mongo gateway | `bench` no-WAL | READ | 9,512 | 75,577.2 | 191.0 | 160.0 | 398.0 | 702.0 | 3,035.0 |
| TreeDB Mongo gateway | `bench` no-WAL | UPDATE | 488 | 3,869.7 | 216.0 | 182.0 | 463.0 | 850.0 | 1,496.0 |
| TreeDB Mongo gateway | `bench` no-WAL | TOTAL | 10,000 | 79,357.6 | 192.0 | 161.0 | 401.0 | 729.0 | 3,035.0 |

## Comparison With June 1 Closeout

Previous checked-in closeout:
`docs/benchmarks/ycsb_profile_closeout_2026-06-01.md`.

| target | profile | previous load | current load | load delta | previous run | current run | run delta |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| MongoDB | `baseline` | 35,611.8 | 33,255.6 | -6.6% | 26,063.3 | 26,102.5 | +0.2% |
| TreeDB nativewire | `command_wal_durable` | 77,591.9 | 77,065.3 | -0.7% | 96,947.9 | 120,500.7 | +24.3% |
| TreeDB Mongo gateway | `command_wal_durable` | 63,123.2 | 57,370.1 | -9.1% | 75,218.6 | 74,544.7 | -0.9% |
| TreeDB nativewire | `command_wal_relaxed` | 77,223.1 | 76,411.7 | -1.1% | 97,565.0 | 126,762.5 | +29.9% |
| TreeDB Mongo gateway | `command_wal_relaxed` | 63,203.2 | 62,444.0 | -1.2% | 78,130.3 | 75,371.9 | -3.5% |
| TreeDB nativewire | `bench` no-WAL | 86,430.2 | 85,201.7 | -1.4% | 131,470.0 | 134,415.5 | +2.2% |
| TreeDB Mongo gateway | `bench` no-WAL | 74,568.4 | 75,033.3 | +0.6% | 81,813.8 | 79,357.6 | -3.0% |

Nativewire run throughput is the clear win from the update-path stack because
the external client now uses a BSON `$set` command instead of the older
read-modify-replace shape. Mongo gateway total run throughput stays in the same
range because the workload is roughly 95% reads and the gateway path still pays
Mongo wire protocol, command parsing, and response overhead on every operation.

## Summary

Current TreeDB nativewire `command_wal_durable` run throughput is now about
120.5k OPS on this host, with UPDATE average latency around 213us in the
selected median repeat and no 100ms-scale stalls. `command_wal_relaxed` is
slightly faster in the run phase at about 126.8k OPS, while `bench` no-WAL is
the ceiling at about 134.4k OPS.

TreeDB Mongo gateway remains faster than the MongoDB baseline on this local
YCSB shape, but the June 2 update-path stack does not materially change its
total run throughput. The remaining gateway work is likely in per-request
Mongo protocol/gateway/read overhead rather than the collection update core.
