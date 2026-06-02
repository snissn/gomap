# YCSB TreeDB Profile Sweep: Command WAL vs No-WAL Benchmark

Status: superseded. This initial single-run profile sweep is retained as
prior-run evidence, but `docs/benchmarks/ycsb_profile_closeout_2026-06-01.md`
is the preferred checked-in profile-surface closeout. Use
`docs/benchmarks/ycsb_post_update_stack_2026-06-02.md` for current headline
YCSB performance, and use `docs/benchmarks/ycsb_mongodb_treedb_current.md` for
the report index and rerun matrix.

This report captures an external `go-ycsb` profile sweep over the current
TreeDB collection entry points:

- `command_wal_durable`
- `command_wal_relaxed`
- `bench`, used here as the current no-WAL benchmark-only profile

MongoDB was run once as a fixed baseline. TreeDB nativewire and TreeDB Mongo
gateway were run once per TreeDB profile. These are developer-machine results,
not a formal product benchmark.

## Benchmark Boundary

Workload:

- `recordcount=100000`
- `operationcount=10000`
- `threadcount=16`
- `table=usertable`
- load phase: 100,000 inserts
- run phase: 10,000 operations, observed as roughly 95% reads and 5% updates
- all clients used local loopback TCP

Targets:

- MongoDB 8 through the `go-ycsb mongodb` binding
- `treedb-native` through the `go-ycsb treedb-native` binding
- TreeDB Mongo gateway through the `go-ycsb mongodb` binding with BSON documents

Validity:

- Every parsed YCSB phase in this sweep had zero `*_ERROR` operation counts.
- MongoDB was run only in the `command_wal_durable` artifact directory and is
  reused as the baseline for the other profile comparisons.

## Host Context

Captured on 2026-06-01 HST / 2026-06-01 UTC.

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
gomap: 3b8b8fb47c7d221fc3b30a2033e4a8c504cd2596
branch: main
go-ycsb: 304788add5117f471def2e23a14f10ee46146285
go-ycsb branch: codex/treedb-native-bson-default
MongoDB image: mongo:8
MongoDB image id: sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54
MongoDB image digest: mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62
```

Primary artifact root:

```text
/tmp/treedb_ycsb_profile_sweep_20260601_082948
```

Artifact directories:

```text
/tmp/treedb_ycsb_profile_sweep_20260601_082948/command_wal_durable
/tmp/treedb_ycsb_profile_sweep_20260601_082948/command_wal_relaxed
/tmp/treedb_ycsb_profile_sweep_20260601_082948/bench_no_wal
```

## Reproduction

The sweep used the existing comparison script:

```sh
cd /home/mikers/dev/snissn/gomap-clean

OUT_ROOT=/tmp/treedb_ycsb_profile_sweep_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT_ROOT"

TREEDB_PROFILE=command_wal_durable \
  OUT_DIR="$OUT_ROOT/command_wal_durable" \
  RUN_REPEATS=1 \
  BUILD=true \
  BUILD_GOYCSB=false \
  MONGODB_MODE=docker \
  scripts/ycsb_compare_mongodb_treedb.sh

TREEDB_PROFILE=command_wal_relaxed \
  OUT_DIR="$OUT_ROOT/command_wal_relaxed" \
  RUN_REPEATS=1 \
  BUILD=false \
  BUILD_GOYCSB=false \
  MONGODB_MODE=skip \
  scripts/ycsb_compare_mongodb_treedb.sh

TREEDB_PROFILE=bench \
  OUT_DIR="$OUT_ROOT/bench_no_wal" \
  RUN_REPEATS=1 \
  BUILD=false \
  BUILD_GOYCSB=false \
  MONGODB_MODE=skip \
  scripts/ycsb_compare_mongodb_treedb.sh
```

Per-run `commands.txt`, `host.txt`, `summary.tsv`, `summary.md`, server logs,
raw YCSB outputs, and DB directories are under the artifact directories listed
above.

## Headline Results

| target | profile | load ops/sec | run ops/sec | run avg us | run p99 us | run max us | errors |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| MongoDB | baseline | 38,333.0 | 25,923.0 | 608.0 | 1,384.0 | 2,213.0 | 0 |
| TreeDB nativewire | `command_wal_durable` | 81,399.3 | 96,855.4 | 153.0 | 1,519.0 | 8,823.0 | 0 |
| TreeDB Mongo gateway | `command_wal_durable` | 66,134.6 | 81,333.9 | 198.0 | 663.0 | 6,723.0 | 0 |
| TreeDB nativewire | `command_wal_relaxed` | 80,402.8 | 95,637.4 | 148.0 | 1,596.0 | 7,099.0 | 0 |
| TreeDB Mongo gateway | `command_wal_relaxed` | 64,733.5 | 81,317.5 | 199.0 | 744.0 | 6,923.0 | 0 |
| TreeDB nativewire | `bench` no-WAL | 93,057.8 | 130,809.9 | 117.0 | 371.0 | 1,859.0 | 0 |
| TreeDB Mongo gateway | `bench` no-WAL | 83,329.4 | 88,317.5 | 174.0 | 589.0 | 1,243.0 | 0 |

## Relative Throughput

| target | profile | load vs MongoDB | run vs MongoDB |
| --- | --- | ---: | ---: |
| TreeDB nativewire | `command_wal_durable` | 2.12x | 3.74x |
| TreeDB Mongo gateway | `command_wal_durable` | 1.73x | 3.14x |
| TreeDB nativewire | `command_wal_relaxed` | 2.10x | 3.69x |
| TreeDB Mongo gateway | `command_wal_relaxed` | 1.69x | 3.14x |
| TreeDB nativewire | `bench` no-WAL | 2.43x | 5.05x |
| TreeDB Mongo gateway | `bench` no-WAL | 2.17x | 3.41x |

## Operation Detail

### MongoDB Baseline

| phase | op | count | ops/sec | avg us | p50 us | p95 us | p99 us | max us |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| load | INSERT | 100,000 | 38,329.2 | 421.0 | 277.0 | 550.0 | 774.0 | 877,567.0 |
| run | READ | 9,503 | 24,637.3 | 615.0 | 583.0 | 1,015.0 | 1,384.0 | 2,213.0 |
| run | UPDATE | 497 | 1,291.3 | 465.0 | 416.0 | 858.0 | 1,333.0 | 1,889.0 |
| run | TOTAL | 10,000 | 25,923.0 | 608.0 | 575.0 | 1,011.0 | 1,384.0 | 2,213.0 |

### TreeDB Nativewire

| profile | phase | op | count | ops/sec | avg us | p50 us | p95 us | p99 us | max us |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `command_wal_durable` | load | INSERT | 100,000 | 81,386.0 | 181.0 | 164.0 | 283.0 | 429.0 | 12,031.0 |
| `command_wal_durable` | run | READ | 9,494 | 91,890.1 | 105.0 | 88.0 | 214.0 | 523.0 | 2,031.0 |
| `command_wal_durable` | run | UPDATE | 506 | 4,953.5 | 1,039.0 | 690.0 | 3,111.0 | 5,299.0 | 8,823.0 |
| `command_wal_durable` | run | TOTAL | 10,000 | 96,855.4 | 153.0 | 90.0 | 418.0 | 1,519.0 | 8,823.0 |
| `command_wal_relaxed` | load | INSERT | 100,000 | 80,401.7 | 183.0 | 165.0 | 291.0 | 448.0 | 9,055.0 |
| `command_wal_relaxed` | run | READ | 9,458 | 90,515.1 | 92.0 | 79.0 | 182.0 | 356.0 | 969.0 |
| `command_wal_relaxed` | run | UPDATE | 542 | 5,257.6 | 1,124.0 | 814.0 | 3,157.0 | 5,555.0 | 7,099.0 |
| `command_wal_relaxed` | run | TOTAL | 10,000 | 95,637.4 | 148.0 | 81.0 | 398.0 | 1,596.0 | 7,099.0 |
| `bench` no-WAL | load | INSERT | 100,000 | 93,042.0 | 156.0 | 136.0 | 264.0 | 415.0 | 6,635.0 |
| `bench` no-WAL | run | READ | 9,476 | 123,844.8 | 109.0 | 103.0 | 191.0 | 300.0 | 857.0 |
| `bench` no-WAL | run | UPDATE | 524 | 6,954.0 | 269.0 | 241.0 | 443.0 | 842.0 | 1,859.0 |
| `bench` no-WAL | run | TOTAL | 10,000 | 130,809.9 | 117.0 | 105.0 | 241.0 | 371.0 | 1,859.0 |

### TreeDB Mongo Gateway

| profile | phase | op | count | ops/sec | avg us | p50 us | p95 us | p99 us | max us |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `command_wal_durable` | load | INSERT | 100,000 | 66,135.7 | 227.0 | 210.0 | 367.0 | 568.0 | 20,527.0 |
| `command_wal_durable` | run | READ | 9,475 | 77,104.3 | 193.0 | 155.0 | 384.0 | 651.0 | 6,723.0 |
| `command_wal_durable` | run | UPDATE | 525 | 4,280.8 | 289.0 | 237.0 | 544.0 | 857.0 | 6,495.0 |
| `command_wal_durable` | run | TOTAL | 10,000 | 81,333.9 | 198.0 | 159.0 | 400.0 | 663.0 | 6,723.0 |
| `command_wal_relaxed` | load | INSERT | 100,000 | 64,726.9 | 232.0 | 215.0 | 377.0 | 579.0 | 20,399.0 |
| `command_wal_relaxed` | run | READ | 9,539 | 77,446.9 | 195.0 | 153.0 | 399.0 | 744.0 | 6,923.0 |
| `command_wal_relaxed` | run | UPDATE | 461 | 3,787.3 | 278.0 | 242.0 | 536.0 | 735.0 | 1,322.0 |
| `command_wal_relaxed` | run | TOTAL | 10,000 | 81,317.5 | 199.0 | 156.0 | 414.0 | 744.0 | 6,923.0 |
| `bench` no-WAL | load | INSERT | 100,000 | 83,316.3 | 177.0 | 153.0 | 327.0 | 517.0 | 19,743.0 |
| `bench` no-WAL | run | READ | 9,483 | 83,632.4 | 173.0 | 149.0 | 357.0 | 592.0 | 1,216.0 |
| `bench` no-WAL | run | UPDATE | 517 | 4,597.3 | 185.0 | 162.0 | 338.0 | 561.0 | 1,243.0 |
| `bench` no-WAL | run | TOTAL | 10,000 | 88,317.5 | 174.0 | 150.0 | 356.0 | 589.0 | 1,243.0 |

## Observations

- `command_wal_durable` and `command_wal_relaxed` are very close in this YCSB
  shape. In this single run, durable was slightly faster on load and nearly tied
  on run throughput, which is within expected machine/run variance for this
  short workload.
- The no-WAL `bench` profile gives a clear ceiling above command WAL. Nativewire
  improved from about 81k to 93k load ops/sec and from about 97k to 131k run
  ops/sec. Mongo gateway improved from about 66k to 83k load ops/sec and from
  about 81k to 88k run ops/sec.
- The largest no-WAL gain is on nativewire run/update latency: nativewire update
  average dropped from about 1.0-1.1ms in command-WAL modes to 269us under
  `bench`.
- TreeDB Mongo gateway is less sensitive to the WAL profile in the run phase
  than nativewire. The gateway/protocol path likely dominates more of the
  remaining run-phase overhead.
- All TreeDB profiles substantially exceeded the MongoDB baseline on this local
  benchmark shape. This is not an equivalent durability comparison for the
  no-WAL profile; `bench` is included only as a ceiling measurement.

## Summary

For collection-backed YCSB, the two command-WAL entry points are already close
to each other and both are valid under this sweep. The no-WAL benchmark profile
still exposes meaningful headroom, especially for nativewire run/update latency,
but it should remain a benchmark-only ceiling rather than a supported server
profile. The next useful performance work is therefore not choosing relaxed over
durable, but reducing command-WAL per-operation overhead and nativewire update
tail latency while preserving command-WAL correctness.
