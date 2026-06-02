# YCSB MongoDB, TreeDB Native, and TreeDB Mongo Report

Status: historical. This report used the legacy `fast` TreeDB profile and
predates the command-WAL public-profile closeout plus PR #2164's command-WAL
frame hashing change. Keep it as artifact-backed evidence for the original
MongoDB / TreeDB comparison and post-load Mongo-gateway cliff attribution, but
use `docs/benchmarks/ycsb_mongodb_treedb_current.md` for current report status
and rerun guidance.

This report captures the external `go-ycsb` workload used during the Mongo
gateway throughput investigation. It compares:

- MongoDB 8 through the `go-ycsb mongodb` binding.
- `treedb-native` through the `go-ycsb treedb-native` binding.
- TreeDB Mongo gateway through the `go-ycsb mongodb` binding.

The goal is to document the exact benchmark shape, measured results, hardware
context, and reproducibility commands. These are developer-machine results, not
a formal product benchmark.

## Benchmark Boundary

Workload:

- `recordcount=100000`
- `operationcount=10000`
- `threadcount=16`
- `table=usertable`
- request distribution: uniform, as emitted by `go-ycsb`
- load phase: 100,000 inserts
- run phase: 10,000 operations, observed as roughly 95% reads and 5% updates

Storage and protocol boundary:

- MongoDB used `mongo:8` in Docker with default server settings and `w=1`.
- `treedb-native` used `./bin/treedb-native-server -profile fast`.
- TreeDB Mongo gateway used `./bin/treedb-mongo-gateway -profile fast -document-format bson`.
- All clients talked over local loopback TCP.
- MongoDB ran in a Docker container published on `127.0.0.1:27018` with its
  data directory bind-mounted under `/tmp`.
- TreeDB servers ran as host processes.

Important comparability note: TreeDB `fast` is a relaxed benchmark profile. It
does not provide the same durability boundary as default MongoDB. These results
compare the current benchmark shape and protocol paths, not equivalent
production durability policies.

## Host Context

Captured on 2026-05-31 HST / 2026-06-01 UTC.

```text
host: mikers-B560-DS3H-AC-Y1
kernel: Linux 6.8.0-111-generic x86_64
go: go version go1.25.7 linux/amd64
cpu: 11th Gen Intel(R) Core(TM) i5-11400F @ 2.60GHz
logical CPUs: 12
memory: 31GiB
clocksource: tsc
filesystem for /tmp: ext4 on /dev/nvme0n1p2
primary disk: Samsung SSD 980 PRO with Heatsink 2TB
free space during run: about 40G free on a 1.8T filesystem, 98% used
```

Source versions:

```text
gomap: f57b33fc92994727b2824194ba05cb0d64ebcac6
       Merge pull request #2113 from snissn/codex/mongo-insert-coalescer-2109
go-ycsb: 304788add5117f471def2e23a14f10ee46146285
         close treedb-native handles on format mismatch
MongoDB image: mongo:8
MongoDB image id: sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54
MongoDB image digest: mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62
```

Primary artifact directories:

```text
MongoDB baseline: /tmp/mongodb_ycsb_compare_20260531_193813
TreeDB native + TreeDB Mongo first comparison: /tmp/treedb_ycsb_post_coalesce_20260531_192612
TreeDB Mongo repeated run check: /tmp/treedb_mongo_exact_rerun_20260531_193056
TreeDB Mongo first-run reproduction: /tmp/treedb_mongo_first_run_repro_20260531_201950
Mongo gateway load attribution: /tmp/treedb_mongo_ycsb_post_coalesce_attr_20260531_192632
Mongo gateway run attribution: /tmp/treedb_mongo_ycsb_post_coalesce_run_attr_20260531_192751
```

## Reproduction Script

The preferred reproduction entrypoint is:

```sh
scripts/ycsb_compare_mongodb_treedb.sh
```

Useful overrides:

```sh
RUN_REPEATS=3 \
OUT_DIR=/tmp/treedb_ycsb_compare_repro \
scripts/ycsb_compare_mongodb_treedb.sh
```

The script writes:

- `host.txt`
- `commands.txt`
- `summary.tsv`
- `summary.md`
- per-target raw `go-ycsb` output
- server logs and fresh DB directories

The script treats nonzero YCSB operation error counters (`INSERT_ERROR`,
`READ_ERROR`, `UPDATE_ERROR`, and other `*_ERROR` rows) as invalid benchmark
evidence. It exits nonzero by default when such rows are present, annotates
`summary.tsv`/`summary.md` with phase error counts, and supports
`ALLOW_YCSB_ERRORS=true` only for exploratory parsing of known-bad artifacts.
Use `PARSE_ONLY=true OUT_DIR=/path/to/run` to regenerate summaries from saved
raw outputs.

The script defaults to Docker MongoDB via `mongo:8`. To use an already-running
MongoDB:

```sh
MONGODB_MODE=external \
MONGODB_ADDR=127.0.0.1:27017 \
scripts/ycsb_compare_mongodb_treedb.sh
```

To skip the MongoDB baseline:

```sh
MONGODB_MODE=skip scripts/ycsb_compare_mongodb_treedb.sh
```

## Manual Commands

Build TreeDB servers:

```sh
cd /home/mikers/dev/snissn/gomap-clean
go build -o bin/treedb-native-server ./cmd/treedb-native-server
go build -o bin/treedb-mongo-gateway ./TreeDB/mongo_gateway/server.go
```

Build `go-ycsb`:

```sh
cd /home/mikers/dev/pingcap/go-ycsb
make build
```

Run MongoDB 8 in Docker:

```sh
RUN_DIR=/tmp/mongodb_ycsb_compare_$(date +%Y%m%d_%H%M%S)
mkdir -p "$RUN_DIR/db"

docker run --rm -d --name treedb-ycsb-mongo \
  -p 127.0.0.1:27018:27017 \
  -v "$RUN_DIR/db:/data/db" \
  mongo:8 --bind_ip_all --quiet
```

Run MongoDB YCSB:

```sh
cd /home/mikers/dev/pingcap/go-ycsb

./bin/go-ycsb load mongodb \
  -p mongodb.url='mongodb://127.0.0.1:27018/ycsb?w=1' \
  -p recordcount=100000 \
  -p operationcount=10000 \
  -p threadcount=16 \
  >"$RUN_DIR/load.out"

./bin/go-ycsb run mongodb \
  -p mongodb.url='mongodb://127.0.0.1:27018/ycsb?w=1' \
  -p recordcount=100000 \
  -p operationcount=10000 \
  -p threadcount=16 \
  >"$RUN_DIR/run.out"
```

Run TreeDB native:

```sh
cd /home/mikers/dev/snissn/gomap-clean

RUN_DIR=/tmp/treedb_ycsb_$(date +%Y%m%d_%H%M%S)
mkdir -p "$RUN_DIR/native_db"

./bin/treedb-native-server \
  -dir "$RUN_DIR/native_db" \
  -profile fast \
  -addr 127.0.0.1:17130 \
  >"$RUN_DIR/native_server.log" 2>&1 &
NATIVE_PID=$!

cd /home/mikers/dev/pingcap/go-ycsb

./bin/go-ycsb load treedb-native \
  -p treedb.addr=127.0.0.1:17130 \
  -p recordcount=100000 \
  -p operationcount=10000 \
  -p threadcount=16 \
  >"$RUN_DIR/native_load.out"

./bin/go-ycsb run treedb-native \
  -p treedb.addr=127.0.0.1:17130 \
  -p recordcount=100000 \
  -p operationcount=10000 \
  -p threadcount=16 \
  >"$RUN_DIR/native_run.out"

kill "$NATIVE_PID"
wait "$NATIVE_PID" 2>/dev/null || true
```

Run TreeDB Mongo gateway:

```sh
cd /home/mikers/dev/snissn/gomap-clean

RUN_DIR=/tmp/treedb_mongo_ycsb_$(date +%Y%m%d_%H%M%S)
mkdir -p "$RUN_DIR/mongo_db"

./bin/treedb-mongo-gateway \
  -dir "$RUN_DIR/mongo_db" \
  -profile fast \
  -document-format bson \
  -addr 127.0.0.1:27130 \
  >"$RUN_DIR/mongo_server.log" 2>&1 &
MONGO_GATEWAY_PID=$!

cd /home/mikers/dev/pingcap/go-ycsb

./bin/go-ycsb load mongodb \
  -p mongodb.url='mongodb://127.0.0.1:27130/ycsb?w=1' \
  -p recordcount=100000 \
  -p operationcount=10000 \
  -p threadcount=16 \
  >"$RUN_DIR/mongo_load.out"

./bin/go-ycsb run mongodb \
  -p mongodb.url='mongodb://127.0.0.1:27130/ycsb?w=1' \
  -p recordcount=100000 \
  -p operationcount=10000 \
  -p threadcount=16 \
  >"$RUN_DIR/mongo_run.out"

kill "$MONGO_GATEWAY_PID"
wait "$MONGO_GATEWAY_PID" 2>/dev/null || true
```

## Headline Results

First run after load:

| Target | Load total ops/s | Run total ops/s | Run avg us | Run p50 us | Run p95 us | Run p99 us | Run p99.9 us | Run max us |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MongoDB 8 Docker | 39,005.3 | 27,287.4 | 575 | 544 | 970 | 1,296 | 1,789 | 2,165 |
| TreeDB nativewire | 91,706.6 | 123,002.9 | 124 | 112 | 259 | 414 | 851 | 1,293 |
| TreeDB Mongo gateway | 85,635.8 | 28,932.5 | 544 | 178 | 392 | 584 | 215,807 | 216,703 |

TreeDB Mongo repeated run check on the same loaded DB/server:

| Run | Total ops/s | Avg us | p50 us | p95 us | p99 us | p99.9 us | Max us |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 28,968.6 | 543 | 177 | 404 | 669 | 213,631 | 214,783 |
| 2 | 76,043.4 | 203 | 183 | 388 | 560 | 950 | 1,520 |
| 3 | 74,430.0 | 206 | 185 | 394 | 659 | 923 | 1,405 |

The first TreeDB Mongo run after load is therefore not representative of
steady-state point-operation throughput. The observed pattern is a reproducible
one-time post-load foreground drain: after the load phase, the first mixed
read/update run pays to flush a large buffered primary-only write backlog, and
subsequent runs avoid that cost because the backlog has already been drained.

A fresh reproduction on the same host at
`/tmp/treedb_mongo_first_run_repro_20260531_201950` showed the same shape:

| Run | Total ops/s | Avg us | p50 us | p95 us | p99 us | p99.9 us | Max us |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 26,682.2 | 591 | 200 | 446 | 768 | 230,015 | 230,911 |
| 2 | 69,855.0 | 220 | 196 | 433 | 768 | 1,218 | 1,721 |
| 3 | 72,530.6 | 213 | 186 | 436 | 706 | 1,126 | 1,668 |

## Detailed Results

### Load Phase

| Target | Insert count | Insert ops/s | Avg us | p50 us | p95 us | p99 us | p99.9 us | Max us |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MongoDB 8 Docker | 100,000 | 39,006.4 | 421 | 277 | 537 | 737 | 1,230 | 888,831 |
| TreeDB nativewire | 100,000 | 91,709.2 | 158 | 138 | 267 | 418 | 2,093 | 6,019 |
| TreeDB Mongo gateway | 100,000 | 85,634.2 | 171 | 148 | 320 | 503 | 897 | 19,983 |

### Run Phase: Reads

| Target | Read count | Read ops/s | Avg us | p50 us | p95 us | p99 us | p99.9 us | Max us |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MongoDB 8 Docker | 9,504 | 25,932.3 | 583 | 551 | 974 | 1,299 | 1,789 | 2,165 |
| TreeDB nativewire | 9,500 | 116,885.9 | 116 | 109 | 211 | 363 | 612 | 1,293 |
| TreeDB Mongo gateway, first run | 9,483 | 27,449.1 | 540 | 177 | 389 | 579 | 215,807 | 216,703 |
| TreeDB Mongo gateway, repeated run 2 | 9,493 | 72,102.4 | 202 | 183 | 384 | 552 | 948 | 1,520 |
| TreeDB Mongo gateway, repeated run 3 | 9,517 | 70,852.8 | 207 | 185 | 394 | 670 | 923 | 1,405 |

### Run Phase: Updates

| Target | Update count | Update ops/s | Avg us | p50 us | p95 us | p99 us | p99.9 us | Max us |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MongoDB 8 Docker | 496 | 1,360.4 | 437 | 403 | 746 | 1,204 | 1,395 | 1,395 |
| TreeDB nativewire | 500 | 6,226.5 | 278 | 253 | 519 | 863 | 1,254 | 1,254 |
| TreeDB Mongo gateway, first run | 517 | 3,978.0 | 629 | 192 | 429 | 648 | 824 | 216,319 |
| TreeDB Mongo gateway, repeated run 2 | 507 | 3,875.7 | 215 | 190 | 427 | 641 | 1,062 | 1,063 |
| TreeDB Mongo gateway, repeated run 3 | 483 | 3,615.1 | 205 | 186 | 390 | 546 | 842 | 842 |

## Attribution Evidence

The Mongo gateway load path improved after the single-document BSON insert
coalescer. Internal attribution from
`/tmp/treedb_mongo_ycsb_post_coalesce_attr_20260531_192632`:

| Secondary indexes | Driver ops/s | Raw wire TCP ops/s | Native wire TCP ops/s | Direct collection ops/s |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 116,919.0 | 193,049.6 | 181,146.3 | 647,315.8 |
| 1 | 93,547.7 | 125,154.4 | 87,837.0 | 194,662.1 |

The remaining first-run Mongo gateway run cliff was isolated with
`/tmp/treedb_mongo_ycsb_post_coalesce_run_attr_20260531_192751`:

- driver `id_update_set`: 500 updates took `151.473ms`
- `pending_docs` changed by `-99,500`
- `indexed_stage.docs_total=500`
- `mutation_lock.hold_ns_total=119,192,560`
- CPU profile included `Collection.UpdateBSONSet`,
  `flushBufferedNoIndexLocked`, `pointerizeCollectionRunTableValues`,
  memtable iterator/sort work, value-log append/compression, and root publish

Interpretation: the load phase leaves a large primary-only write-domain buffer.
The first mixed run update forces most of that backlog through the foreground
collection mutation path. Repeated runs avoid that cost because the backlog has
already been drained. This is not general steady-state Mongo gateway overhead;
it is a transition cost caused by running the mixed workload immediately after
the bulk load.

## Caveats

- The benchmark host was not an isolated lab machine.
- `/tmp` lived on the root filesystem, which was about 98% full.
- MongoDB ran in Docker; TreeDB servers ran directly on the host.
- MongoDB used default server behavior; TreeDB used the relaxed `fast` profile.
- The `go-ycsb` run duration is short, so one large tail event can dominate
  average and total OPS in the 10k operation run.
- Results should be compared with exact commands, artifact paths, commits, and
  host metadata, not copied as universal numbers.

## Follow-Up Work

The report points to two next benchmark-driven tasks:

- #2115: reduce the first-run Mongo YCSB stall from post-load primary flush.
- #2116: optimize steady-state Mongo gateway point-operation overhead after
  the first-run flush cliff is fixed or controlled.

## Summary

TreeDB nativewire is currently the fastest path on this exact YCSB shape:
about `91.7k` load ops/s and `123.0k` run ops/s. TreeDB Mongo gateway load is
close to nativewire at about `85.6k` ops/s and is substantially faster than the
Docker MongoDB baseline at about `39.0k` ops/s. The first TreeDB Mongo run after
load looks similar to MongoDB in total OPS, but that is misleading: it includes
a reproducible one-time `~215ms-230ms` foreground drain of the post-load buffered
write backlog. Repeated TreeDB Mongo runs on the same loaded DB reach about
`74k-76k` ops/s with sub-2ms max latency in the original run, and the fresh
reproduction reached `70k-73k` ops/s. The next high-priority fix is to prevent
or amortize that post-load foreground flush, then re-profile steady-state Mongo
protocol and BSON response overhead.
