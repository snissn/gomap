# TreeDB Nativewire YCSB Closeout Evidence

Status: external `go-ycsb` nativewire load-only closeout evidence for #2026
and #3355 refreshed on current `origin/main` commit
`1e0450fd06aaea5669150c5c744a3e1d1c6880e2`.

This refresh includes the later Raft snapshot/route-preflight merges that the
first closeout capture predated, including
`febc1992e58307d8b1edd2c68657faef9bd67c88` and
`66819cd00fe10aaae2f0da3b7dbfc1ebc0c4113d`.

This report records the final 100k and 1M nativewire YCSB load checks requested
by #2026 after the deterministic publication/readability slices landed. It is
developer-machine correctness and throughput evidence for the nativewire load
path. It does not replace the full MongoDB / TreeDB comparison matrix in
`docs/benchmarks/ycsb_latest_main_2026-06-03.md`.

## Benchmark Boundary

Workload:

- `go-ycsb load treedb-native`
- `recordcount=100000` and `recordcount=1000000`
- `operationcount=10000`
- `threadcount=16`
- collection/table: `usertable`
- local loopback TCP
- fresh TreeDB data directory per recordcount

TreeDB:

- profile: `command_wal_relaxed`
- server: `cmd/treedb-native-server`
- gomap commit: `1e0450fd06aaea5669150c5c744a3e1d1c6880e2`
- gomap branch: `codex/2026-current-head-ycsb-20260630`

External client:

- path: `/home/mikers/dev/pingcap/go-ycsb-2026-meta-v5`
- commit: `c195a9bc777b6aeb1e2f5550db96d872338f2f40`
- branch: `codex/2026-treedb-native-meta-v5`

Validity:

- Both load phases completed the requested insert count.
- Raw output contained no `INSERT_ERROR` lines; `grep -c INSERT_ERROR` returned
  `0` for both load outputs.
- The summary records `INSERT_ERROR=0` for both load phases.

## Host Context

Captured on 2026-06-29 HST / 2026-06-30 UTC.

```text
host: mikers-B560-DS3H-AC-Y1
kernel: Linux 6.8.0-124-generic x86_64
go: go version go1.25.7 linux/amd64
cpu: 11th Gen Intel(R) Core(TM) i5-11400F @ 2.60GHz
logical CPUs: 12
memory: 31Gi
clocksource: tsc
```

Primary artifact root:

```text
/tmp/treedb_native_ycsb_current_head_20260629_195546
```

Primary artifacts:

```text
/tmp/treedb_native_ycsb_current_head_20260629_195546/host.txt
/tmp/treedb_native_ycsb_current_head_20260629_195546/commands.txt
/tmp/treedb_native_ycsb_current_head_20260629_195546/summary.tsv
/tmp/treedb_native_ycsb_current_head_20260629_195546/error_counts.txt
/tmp/treedb_native_ycsb_current_head_20260629_195546/100000/load.out
/tmp/treedb_native_ycsb_current_head_20260629_195546/100000/server.log
/tmp/treedb_native_ycsb_current_head_20260629_195546/1000000/load.out
/tmp/treedb_native_ycsb_current_head_20260629_195546/1000000/server.log
```

## Focused Gates

These gates passed before the external load runs:

```sh
GOWORK=off go test ./cmd/treedb-native-server ./TreeDB/nativewire ./TreeDB/collections ./TreeDB/db -count=1

cd /home/mikers/dev/pingcap/go-ycsb-2026-meta-v5
go test ./db/treedbnative -count=1
```

Build commands:

```sh
GOWORK=off go build -o bin/treedb-native-server ./cmd/treedb-native-server

cd /home/mikers/dev/pingcap/go-ycsb-2026-meta-v5
make build
```

## Load Results

| recordcount | INSERT count | INSERT ops/sec | INSERT avg us | INSERT p50 us | INSERT p95 us | INSERT p99 us | INSERT p99.9 us | INSERT max us | INSERT_ERROR |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 100,000 | 54,929.6 | 274.0 | 224.0 | 511.0 | 1,156.0 | 4,255.0 | 19,103.0 | 0 |
| 1,000,000 | 1,000,000 | 39,299.1 | 389.0 | 233.0 | 447.0 | 700.0 | 2,623.0 | 6,946,815.0 | 0 |

The 1M load emitted periodic progress rows at 10s and 20s before the final
1,000,000-row summary. The table above uses the final post-run row.

## Reproduction

Each load used a fresh data directory and an isolated loopback port. Start each
server command and keep it running while executing the matching `go-ycsb load`
command, then stop it before starting the next recordcount:

```sh
cd /home/mikers/dev/snissn/gomap-2026-current-head-ycsb-20260630

GOWORK=off go build -o bin/treedb-native-server ./cmd/treedb-native-server

cd /home/mikers/dev/pingcap/go-ycsb-2026-meta-v5
make build

/home/mikers/dev/snissn/gomap-2026-current-head-ycsb-20260630/bin/treedb-native-server \
  -dir /tmp/treedb_native_ycsb_current_head_20260629_195546/100000/db \
  -profile command_wal_relaxed \
  -addr 127.0.0.1:17165

/home/mikers/dev/pingcap/go-ycsb-2026-meta-v5/bin/go-ycsb load treedb-native \
  -p treedb.addr=127.0.0.1:17165 \
  -p recordcount=100000 \
  -p operationcount=10000 \
  -p threadcount=16

/home/mikers/dev/snissn/gomap-2026-current-head-ycsb-20260630/bin/treedb-native-server \
  -dir /tmp/treedb_native_ycsb_current_head_20260629_195546/1000000/db \
  -profile command_wal_relaxed \
  -addr 127.0.0.1:17166

/home/mikers/dev/pingcap/go-ycsb-2026-meta-v5/bin/go-ycsb load treedb-native \
  -p treedb.addr=127.0.0.1:17166 \
  -p recordcount=1000000 \
  -p operationcount=10000 \
  -p threadcount=16
```

## Interpretation

This closeout run proves the compatible external nativewire YCSB client can load
100k and 1M records against the current `command_wal_relaxed` TreeDB nativewire
server with zero insert errors on this host. The refreshed capture removes the
stale-head caveat from the earlier `2b784debb05028f706b46127a41f6d578c3d4c13`
run. It supports #2026 closeout for the external YCSB evidence requirement,
while the deterministic publication and readability proof remains documented by
the issue and its earlier test PRs.
