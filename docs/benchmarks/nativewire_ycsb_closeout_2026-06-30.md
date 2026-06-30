# TreeDB Nativewire YCSB Closeout Evidence

Status: external `go-ycsb` nativewire load-only closeout evidence for #2026
and #3355 refreshed on `origin/main` commit
`61910b8eac108c5f2c35f07a374879d1fb2dc5c8`; reconciled by merged PR #3385
with the #3374 diagnostic classification at
`3f2c712bb700806b29deb872ed90531d4828ab79`.

This refresh includes the later Raft snapshot/route-preflight merges that the
first closeout capture predated, including
`febc1992e58307d8b1edd2c68657faef9bd67c88` and
`66819cd00fe10aaae2f0da3b7dbfc1ebc0c4113d`, plus the latest
span-native command-WAL publish merge at
`12b2bf3b17ea3455551a9c47243f0194a59ee1a0` and mixed-maintenance fallback
coverage at `61910b8eac108c5f2c35f07a374879d1fb2dc5c8`.

This report records the final 100k and 1M nativewire YCSB load checks requested
by #2026 after the deterministic publication/readability slices landed. It is
developer-machine correctness and throughput evidence for the nativewire load
path, and #3385 classifies the remaining invalid intermediate YCSB caveat as
non-current TreeDB publication evidence. Together with the deterministic
publication/readability proofs and #3374 classification, this is sufficient to
close the local storage-publication/readability tracker #2026. It does not
replace the full MongoDB / TreeDB comparison matrix in
`docs/benchmarks/ycsb_latest_main_2026-06-03.md`, and it does not claim
distributed HA, Raft read-index/snapshots/rejoin, or routing/fanout readiness.
Those distributed scopes remain out of #2026 and are tracked by
#3044/#3045/#3046.

## #3385 Reconciliation

The local #2026 closeout state is:

- deterministic publication/readability tests cover collection catalog EOF
  fault boundaries, forced value-log pointers, current-writable value-log read
  barriers, snapshot/reopen readability, and the nativewire YCSB-shaped path;
- this report covers the external 100k and 1M nativewire load evidence with
  zero `INSERT_ERROR`;
- `docs/benchmarks/nativewire_ycsb_insert_error_classification_2026-06-30.md`
  classifies the invalid intermediate 1M `INSERT_ERROR` artifact as a
  non-reproduced client/harness/protocol/server-lifecycle interruption, not as
  evidence of a current catalog/value-log publication failure.

#3385 did not rerun `scripts/nativewire_ycsb_diagnostic.sh`. The diagnostic
evidence at `3f2c712bb700806b29deb872ed90531d4828ab79` already includes the
later route-preflight context that made the original `61910b8e...` evidence
worth refreshing. #3385 merged after the #3384 catalog-backed cluster route
provider delta (`2e03fb95`) and on top of `origin/main` `fef8b711`, so this
no-rerun boundary already includes that nativewire route-provider change. The
remaining delta introduced here is docs-only closeout text, not changes to the
nativewire insert path, collection catalog publication path, route-provider
selection path, or value-log pointer read-boundary code. That makes this PR a
docs-only closeout reconciliation rather than a fresh benchmark run.

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
- gomap code base: `61910b8eac108c5f2c35f07a374879d1fb2dc5c8`
- capture branch commit: `2041a2a2de55e0bb8b6d7d6201c57c989fa4e272`
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
/tmp/treedb_native_ycsb_current_head_20260629_212711
```

Primary artifacts:

```text
/tmp/treedb_native_ycsb_current_head_20260629_212711/host.txt
/tmp/treedb_native_ycsb_current_head_20260629_212711/commands.txt
/tmp/treedb_native_ycsb_current_head_20260629_212711/summary_raw.tsv
/tmp/treedb_native_ycsb_current_head_20260629_212711/error_counts.txt
/tmp/treedb_native_ycsb_current_head_20260629_212711/100000/load.out
/tmp/treedb_native_ycsb_current_head_20260629_212711/100000/server.log
/tmp/treedb_native_ycsb_current_head_20260629_212711/1000000/load.out
/tmp/treedb_native_ycsb_current_head_20260629_212711/1000000/server.log
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
| 100,000 | 100,000 | 59,711.0 | 250.0 | 222.0 | 439.0 | 714.0 | 1,523.0 | 14,607.0 | 0 |
| 1,000,000 | 1,000,000 | 45,196.8 | 334.0 | 259.0 | 538.0 | 902.0 | 2,539.0 | 939,007.0 | 0 |

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
  -dir /tmp/treedb_native_ycsb_current_head_20260629_212711/100000/db \
  -profile command_wal_relaxed \
  -addr 127.0.0.1:17282

/home/mikers/dev/pingcap/go-ycsb-2026-meta-v5/bin/go-ycsb load treedb-native \
  -p treedb.addr=127.0.0.1:17282 \
  -p recordcount=100000 \
  -p operationcount=10000 \
  -p threadcount=16

/home/mikers/dev/snissn/gomap-2026-current-head-ycsb-20260630/bin/treedb-native-server \
  -dir /tmp/treedb_native_ycsb_current_head_20260629_212711/1000000/db \
  -profile command_wal_relaxed \
  -addr 127.0.0.1:17283

/home/mikers/dev/pingcap/go-ycsb-2026-meta-v5/bin/go-ycsb load treedb-native \
  -p treedb.addr=127.0.0.1:17283 \
  -p recordcount=1000000 \
  -p operationcount=10000 \
  -p threadcount=16
```

## Interpretation

This closeout run proves the compatible external nativewire YCSB client can load
100k and 1M records against the current `command_wal_relaxed` TreeDB nativewire
server with zero insert errors on this host. The refreshed capture removes the
stale-head caveat from the earlier `2b784debb05028f706b46127a41f6d578c3d4c13`
run. Together with the deterministic publication/readability tests and the
#3374 diagnostic classification, it supports closing #2026 for the local
single-node publication/readability invariant now that PR #3385 has merged.

During the current-base refresh, an invalid intermediate 1M run at
`/tmp/treedb_native_ycsb_current_head_20260629_202356` stopped at 768,001
successful inserts and reported `INSERT_ERROR`; its server log had no panic,
fatal error, EOF, or ambiguous-commit marker. A clean 1M rerun and the final
paired capture above both completed with zero `INSERT_ERROR`. The #3374
diagnostic below classifies that artifact, so it no longer blocks local #2026
publication/readability closeout.

Follow-up #3374 classification lives in
`docs/benchmarks/nativewire_ycsb_insert_error_classification_2026-06-30.md`.
That diagnostic reran current-head 100k and 1M nativewire loads at
`3f2c712bb700806b29deb872ed90531d4828ab79` with
`TREEDB_YCSB_LOG_ERRORS=1` and `-p silence=false`; both loads completed with
zero `INSERT_ERROR`, empty stderr, and zero raw matches for `EOF`, `ambiguous`,
`panic`, `fatal`, `ERROR`, or `Failed`. The old intermediate artifact remains
invalid because it lacks client-side error strings, but it is now classified as
a non-reproduced client/harness/protocol/server-lifecycle interruption rather
than evidence of a current catalog/value-log publication failure.

This remains single-host nativewire load evidence. Distributed single-group HA,
linearizable reads/snapshots/rejoin, and multi-group routing/ring partitioning
remain out of scope for #2026 and are tracked by #3044, #3045, and #3046
respectively.
