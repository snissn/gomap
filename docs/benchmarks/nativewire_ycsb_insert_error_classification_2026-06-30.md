# TreeDB Nativewire YCSB INSERT_ERROR Classification

Status: diagnostic classification evidence for #3374 under parent #2026,
reconciled by merged PR #3385 as the final nativewire YCSB caveat
classification for local publication/readability closeout.

This report classifies the invalid intermediate 1M nativewire YCSB load at
`/tmp/treedb_native_ycsb_current_head_20260629_202356` and records a fresh
current-head diagnostic gate with client-side operation error logging enabled.
It only covers the remaining intermittent `INSERT_ERROR` caveat from the #3364
refresh. By itself it does not claim distributed HA, Raft
read-index/snapshots/rejoin, or routing/fanout readiness, and it does not
independently close #2026. Combined with the deterministic local
publication/readability tests, the current-head nativewire YCSB evidence, and
the #3385 verification-matrix update, it removes the nativewire YCSB caveat as
a blocker to closing #2026 as the local single-node storage
publication/readability tracker.

## #3385 Reconciliation

The diagnostic commit recorded by this artifact is
`3f2c712bb700806b29deb872ed90531d4828ab79`. #3385 did not rerun the diagnostic
script because the delta to current `origin/main` `f42a7002b` is Raft
snapshot-tail work, this diagnostic script/docs merge, and Q2 query
benchmark/reference-fill work; it does not modify the nativewire insert path,
collection catalog publication path, or value-log pointer read-boundary code
that #2026 owns.

After PR #3385 merged, this classification should be read as final for the invalid
intermediate YCSB artifact: it is a non-reproduced
client/harness/protocol/server-lifecycle interruption, not current evidence of
a TreeDB catalog/value-log publication failure. The residual distributed
cluster work remains outside #2026 and is tracked by #3044, #3045, and #3046.

## Boundary

TreeDB:

- Worktree: `/home/mikers/dev/snissn/gomap-3374-ycsb-diagnostic`
- Branch: `codex/3374-ycsb-diagnostic`
- Diagnostic commit: `3f2c712bb700806b29deb872ed90531d4828ab79`
- Server: `cmd/treedb-native-server`
- Profile: `command_wal_relaxed`

External client:

- Path: `/home/mikers/dev/pingcap/go-ycsb-2026-meta-v5`
- Commit: `c195a9bc777b6aeb1e2f5550db96d872338f2f40`
- Branch: `codex/2026-treedb-native-meta-v5`
- Error logging: `TREEDB_YCSB_LOG_ERRORS=1`
- Non-silent output: `-p silence=false`

Host:

- Captured at: `2026-06-30T08:48:41Z`
- Local time: `2026-06-29T22:48:41-1000`
- Host: `mikers-B560-DS3H-AC-Y1`
- Kernel: `Linux 6.8.0-124-generic x86_64`
- Go: `go version go1.25.7 linux/amd64`
- CPU: `11th Gen Intel(R) Core(TM) i5-11400F @ 2.60GHz`, 12 logical CPUs
- Memory: 31Gi

## Diagnostic Gate

The committed gate is:

```sh
OUT_DIR=/tmp/treedb_native_ycsb_diagnostic_$(date +%Y%m%d_%H%M%S) \
  GOYCSB_DIR=/home/mikers/dev/pingcap/go-ycsb-2026-meta-v5 \
  scripts/nativewire_ycsb_diagnostic.sh
```

It starts a fresh nativewire server and data directory per recordcount, runs
external `go-ycsb load treedb-native`, enables `TREEDB_YCSB_LOG_ERRORS=1`, uses
`-p silence=false`, preserves `load.out`, `load.err`, and `server.log`, and
scans those raw files for:

```text
INSERT_ERROR|EOF|ambiguous|panic|fatal|ERROR|Failed
```

Final artifact root:

```text
/tmp/treedb_native_ycsb_diagnostic_20260629_224839
```

Expanded commands from `commands.txt`:

```sh
/home/mikers/dev/snissn/gomap-3374-ycsb-diagnostic/bin/treedb-native-server \
  -dir /tmp/treedb_native_ycsb_diagnostic_20260629_224839/100000/db \
  -profile command_wal_relaxed \
  -addr 127.0.0.1:17370

env TREEDB_YCSB_LOG_ERRORS=1 \
  /home/mikers/dev/pingcap/go-ycsb-2026-meta-v5/bin/go-ycsb load treedb-native \
  -p treedb.addr=127.0.0.1:17370 \
  -p recordcount=100000 \
  -p operationcount=10000 \
  -p threadcount=16 \
  -p silence=false

/home/mikers/dev/snissn/gomap-3374-ycsb-diagnostic/bin/treedb-native-server \
  -dir /tmp/treedb_native_ycsb_diagnostic_20260629_224839/1000000/db \
  -profile command_wal_relaxed \
  -addr 127.0.0.1:17371

env TREEDB_YCSB_LOG_ERRORS=1 \
  /home/mikers/dev/pingcap/go-ycsb-2026-meta-v5/bin/go-ycsb load treedb-native \
  -p treedb.addr=127.0.0.1:17371 \
  -p recordcount=1000000 \
  -p operationcount=10000 \
  -p threadcount=16 \
  -p silence=false
```

## Current-Head Results

| recordcount | exit code | INSERT count | INSERT ops/sec | avg us | p95 us | p99 us | max us | `load.err` bytes | raw scan lines |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 0 | 100,000 | 51,799.5 | 290 | 485 | 864 | 95,295 | 0 | 0 |
| 1,000,000 | 0 | 1,000,000 | 41,702.6 | 363 | 605 | 1,076 | 1,042,431 | 0 | 0 |

Raw scan counts from `scan_counts.txt`:

| recordcount | INSERT_ERROR | EOF | ambiguous | panic | fatal | ERROR | Failed |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 1,000,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

The aggregate `raw_scan.txt`, `100000/raw_scan.txt`, and
`1000000/raw_scan.txt` files are empty.

## Invalid Intermediate Artifact

Artifact root:

```text
/tmp/treedb_native_ycsb_current_head_20260629_202356
```

Captured context from `host.txt`:

- Captured at: `2026-06-29T20:23:56-10:00`
- gomap commit: `f5206eaa383f9bd43c917712cda88c7128dee367`
- gomap base commit: `be2cfe6fb0ac4bec62a4b8c89915a910f8ac011a`
- gomap branch: `codex/2026-current-head-ycsb-20260630`
- go-ycsb commit: `c195a9bc777b6aeb1e2f5550db96d872338f2f40`
- go-ycsb branch: `codex/2026-treedb-native-meta-v5`

Original commands from `commands.txt`:

```sh
/home/mikers/dev/pingcap/go-ycsb-2026-meta-v5/bin/go-ycsb load treedb-native \
  -p treedb.addr=127.0.0.1:17175 \
  -p recordcount=100000 \
  -p operationcount=10000 \
  -p threadcount=16

/home/mikers/dev/pingcap/go-ycsb-2026-meta-v5/bin/go-ycsb load treedb-native \
  -p treedb.addr=127.0.0.1:17176 \
  -p recordcount=1000000 \
  -p operationcount=10000 \
  -p threadcount=16
```

Those commands did not include `TREEDB_YCSB_LOG_ERRORS=1` or
`-p silence=false`, so the artifact does not contain the client-side operation
error strings that would distinguish a remote error frame from connection
closure or another client-side failure.

Observed output:

| recordcount | result |
| ---: | --- |
| 100,000 | Completed 100,000 inserts, zero `INSERT_ERROR` lines. |
| 1,000,000 | Completed the run with 768,001 successful inserts and an `INSERT_ERROR` histogram for the remaining 231,999 operations. |

Raw scan counts over the original `load.out` and `server.log` files:

| term | count |
| --- | ---: |
| INSERT_ERROR | 2 |
| EOF | 0 |
| ambiguous | 0 |
| panic | 0 |
| fatal | 0 |
| ERROR | 2 |
| Failed | 0 |

The only raw matches are the two YCSB `INSERT_ERROR` histogram rows in
`1000000/load.out`. The server log has no `EOF`, ambiguous-commit, panic,
fatal, `ERROR`, or `Failed` text.

## Classification

| Artifact | Classification | Evidence | Residual Risk |
| --- | --- | --- | --- |
| `/tmp/treedb_native_ycsb_current_head_20260629_202356` | Invalid, non-reproduced intermediate client/harness/protocol/server-lifecycle interruption. Not evidence of a current TreeDB catalog/value-log publication/readability failure. | The 1M run reports client-side `INSERT_ERROR` counters, but no server-side `EOF`, ambiguous commit, panic, fatal error, or failure marker. The original command lacked error logging, so it cannot identify the exact client error string. Later clean #3364 evidence and the logged current-head #3374 diagnostic both complete 100k and 1M loads with zero raw scan matches. | The exact old client error is unavailable. If this shape recurs with `TREEDB_YCSB_LOG_ERRORS=1`, preserve the artifact and classify the logged error string as client harness, nativewire protocol, server lifecycle, or storage. |
| `/tmp/treedb_native_ycsb_diagnostic_20260629_224839` | Current-head diagnostic pass. | 100k and 1M loads both exit 0, complete all requested inserts, produce empty stderr, and scan clean for `INSERT_ERROR`, `EOF`, `ambiguous`, `panic`, `fatal`, `ERROR`, and `Failed`. | This is a single-host developer-machine diagnostic gate, not a distributed HA proof. After PR #3385 merged it supports closing #2026 for local storage publication/readability, while #3044/#3045/#3046 retain distributed HA, Raft read-index/snapshots/rejoin, and routing/fanout scope. |

Conclusion: the preserved intermediate `INSERT_ERROR` artifact should be treated
as a non-reproduced diagnostic caveat, not as current evidence of a TreeDB-owned
storage publication bug. The old artifact is still useful because it explains
why #2026 stayed open after #3364, but the current logged gate makes the caveat
defensible and final for #3385: if the failure recurs, the harness now captures
the missing client-side error strings needed to assign ownership.
