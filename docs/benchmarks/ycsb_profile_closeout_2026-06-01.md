# TreeDB YCSB Profile Closeout: Command WAL Public Surface

Status: superseded for current headline performance by
`docs/benchmarks/ycsb_post_update_stack_2026-06-02.md`. This report remains the
checked-in closeout evidence for the profile-surface sprint and for the June 1
public-profile audit, but its pre-update-stack throughput rows should not be
used as the current MongoDB / TreeDB YCSB headline.

This report is the closeout evidence for the TreeDB profile-surface sprint. It
checks the intended public profiles:

- `command_wal_durable`
- `command_wal_relaxed`
- `bench`, as the explicit no-WAL benchmark-only ceiling

The workload is the external `go-ycsb` shape used during the nativewire and
Mongo gateway optimization work. These are developer-machine results, not a
formal product benchmark.

## Benchmark Boundary

Workload:

- `recordcount=100000`
- `operationcount=10000`
- `threadcount=16`
- load phase: 100,000 inserts
- run phase: 10,000 operations, approximately 95% reads and 5% updates
- all clients used local loopback TCP

Targets:

- MongoDB 8 through the `go-ycsb mongodb` binding
- `treedb-native` through the `go-ycsb treedb-native` binding
- TreeDB Mongo gateway through the `go-ycsb mongodb` binding with BSON documents

Validity:

- Every parsed YCSB phase had zero `*_ERROR` operation counts.
- MongoDB was run once in the `command_wal_durable` artifact directory and used
  as the baseline for the TreeDB profile comparisons.
- The initial single `bench` run had a slow TreeDB Mongo gateway run phase; a
  targeted three-repeat rerun restored the expected ceiling range, so the
  repeated `bench` results are the closeout ceiling evidence.

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
gomap: 6fab2e6aecde93045a46a8f8682d89f373ffe1e6
branch: codex/2151-profile-closeout-evidence
go-ycsb: 304788add5117f471def2e23a14f10ee46146285
go-ycsb branch: codex/treedb-native-bson-default
MongoDB image: mongo:8
MongoDB image id: sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54
MongoDB image digest: mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62
```

Primary artifact root:

```text
/tmp/treedb_ycsb_closeout_20260601_133531
```

Artifact directories:

```text
/tmp/treedb_ycsb_closeout_20260601_133531/command_wal_durable
/tmp/treedb_ycsb_closeout_20260601_133531/command_wal_relaxed
/tmp/treedb_ycsb_closeout_20260601_133531/bench
/tmp/treedb_ycsb_closeout_20260601_133531/bench_rerun3
```

## Reproduction

The sweep used the checked-in comparison script:

```sh
cd /home/mikers/dev/snissn/gomap-clean

RUN_ROOT=/tmp/treedb_ycsb_closeout_$(date +%Y%m%d_%H%M%S)
mkdir -p "$RUN_ROOT"

OUT_DIR="$RUN_ROOT/command_wal_durable" \
TREEDB_PROFILE=command_wal_durable \
MONGODB_MODE=docker \
MONGODB_ADDR=127.0.0.1:27018 \
NATIVE_ADDR=127.0.0.1:17130 \
TREEDB_MONGO_ADDR=127.0.0.1:27130 \
BUILD=true \
BUILD_GOYCSB=false \
scripts/ycsb_compare_mongodb_treedb.sh

OUT_DIR="$RUN_ROOT/command_wal_relaxed" \
TREEDB_PROFILE=command_wal_relaxed \
MONGODB_MODE=skip \
NATIVE_ADDR=127.0.0.1:17131 \
TREEDB_MONGO_ADDR=127.0.0.1:27131 \
BUILD=false \
BUILD_GOYCSB=false \
scripts/ycsb_compare_mongodb_treedb.sh

OUT_DIR="$RUN_ROOT/bench" \
TREEDB_PROFILE=bench \
MONGODB_MODE=skip \
NATIVE_ADDR=127.0.0.1:17132 \
TREEDB_MONGO_ADDR=127.0.0.1:27132 \
BUILD=false \
BUILD_GOYCSB=false \
scripts/ycsb_compare_mongodb_treedb.sh

OUT_DIR="$RUN_ROOT/bench_rerun3" \
TREEDB_PROFILE=bench \
MONGODB_MODE=skip \
NATIVE_ADDR=127.0.0.1:17133 \
TREEDB_MONGO_ADDR=127.0.0.1:27133 \
RUN_REPEATS=3 \
BUILD=false \
BUILD_GOYCSB=false \
scripts/ycsb_compare_mongodb_treedb.sh
```

Each artifact directory includes `host.txt`, `commands.txt`, `summary.tsv`,
`summary.md`, raw YCSB outputs, server logs, and DB directories.

## Headline Results

The `bench` row uses the median run from the three-repeat rerun.

| target | profile | load ops/sec | run ops/sec | run avg us | run p99 us | run max us | errors |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| MongoDB | baseline | 35,611.8 | 26,063.3 | 604.0 | 1,394.0 | 2,363.0 | 0 |
| TreeDB nativewire | `command_wal_durable` | 77,591.9 | 96,947.9 | 148.0 | 1,700.0 | 10,703.0 | 0 |
| TreeDB Mongo gateway | `command_wal_durable` | 63,123.2 | 75,218.6 | 213.0 | 839.0 | 7,019.0 | 0 |
| TreeDB nativewire | `command_wal_relaxed` | 77,223.1 | 97,565.0 | 149.0 | 1,427.0 | 6,035.0 | 0 |
| TreeDB Mongo gateway | `command_wal_relaxed` | 63,203.2 | 78,130.3 | 205.0 | 731.0 | 7,151.0 | 0 |
| TreeDB nativewire | `bench` no-WAL | 86,430.2 | 131,470.0 | 116.0 | 389.0 | 1,084.0 | 0 |
| TreeDB Mongo gateway | `bench` no-WAL | 74,568.4 | 81,813.8 | 185.0 | 591.0 | 1,921.0 | 0 |

## Comparison With Previous Profile Sweep

Previous profile sweep: `docs/benchmarks/ycsb_treedb_profile_sweep_2026-06-01.md`
at `gomap` commit `3b8b8fb47c7d221fc3b30a2033e4a8c504cd2596`.

| target | profile | previous load | closeout load | previous run | closeout run | interpretation |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| MongoDB | baseline | 38,333.0 | 35,611.8 | 25,923.0 | 26,063.3 | Baseline run throughput is flat; load varied lower. |
| TreeDB nativewire | `command_wal_durable` | 81,399.3 | 77,591.9 | 96,855.4 | 96,947.9 | Run throughput is unchanged; load varied lower. |
| TreeDB Mongo gateway | `command_wal_durable` | 66,134.6 | 63,123.2 | 81,333.9 | 75,218.6 | Valid but lower; no errors or tail cliff reproduced. |
| TreeDB nativewire | `command_wal_relaxed` | 80,402.8 | 77,223.1 | 95,637.4 | 97,565.0 | Run throughput is slightly higher; load varied lower. |
| TreeDB Mongo gateway | `command_wal_relaxed` | 64,733.5 | 63,203.2 | 81,317.5 | 78,130.3 | Valid and close to prior range. |
| TreeDB nativewire | `bench` no-WAL | 93,057.8 | 86,430.2 | 130,809.9 | 131,470.0 | Run ceiling reproduced; load varied lower. |
| TreeDB Mongo gateway | `bench` no-WAL | 83,329.4 | 74,568.4 | 88,317.5 | 81,813.8 | Rerun recovered the expected ceiling range after a slow first run. |

The profile-surface changes did not reproduce the old YCSB correctness failures
or 100ms-300ms update stalls. The remaining differences are normal
developer-machine variance or load-phase variation; they do not show a command
WAL regression that should block closeout.

## Bench Rerun Detail

The initial `bench` single run was valid but had a slow TreeDB Mongo gateway run
phase:

```text
artifact: /tmp/treedb_ycsb_closeout_20260601_133531/bench
TreeDB nativewire bench: load 89,064.6 OPS, run 117,247.3 OPS
TreeDB Mongo gateway bench: load 79,157.4 OPS, run 63,145.2 OPS
```

The targeted rerun used `RUN_REPEATS=3` against a fresh `bench` DB:

| target | repeat | run ops/sec | run avg us | run p99 us | run max us |
| --- | ---: | ---: | ---: | ---: | ---: |
| TreeDB nativewire | 1 | 130,279.5 | 117.0 | 351.0 | 1,049.0 |
| TreeDB nativewire | 2 | 131,470.0 | 116.0 | 389.0 | 1,084.0 |
| TreeDB nativewire | 3 | 133,845.7 | 114.0 | 388.0 | 1,926.0 |
| TreeDB Mongo gateway | 1 | 81,813.8 | 185.0 | 591.0 | 1,921.0 |
| TreeDB Mongo gateway | 2 | 79,039.4 | 194.0 | 752.0 | 1,574.0 |
| TreeDB Mongo gateway | 3 | 82,209.2 | 187.0 | 636.0 | 1,744.0 |

The first slow `bench` Mongo gateway run did not reproduce. The closeout ceiling
therefore uses the rerun median.

## Correctness And Public-Surface Gates

Commands run:

```sh
go test ./TreeDB -run 'TestPublicCommandWAL|TestCommandWAL.*Raw|TestCommandWAL.*Feature|TestSaveOpenFormatConfig' -count=1
go test ./cmd/unified_bench -run 'TestTreeDB.*CommandWAL|Test.*Profile' -count=1
go test ./TreeDB/nativewire ./TreeDB/collections ./TreeDB/mongo_gateway ./cmd/mongo_gateway_bench ./cmd/collection_workload_bench -count=1
go test ./TreeDB/integration/kvstoreadapter -count=1
go test ./cmd/treedb-native-server ./cmd/mongo_gateway_bench ./cmd/collection_workload_bench ./cmd/collection_load_fixture ./cmd/treedb_out_of_core_smoke ./cmd/treedb_vector_search_demo -count=1
```

Result: all passed.

## Grep Classification

Commands run:

```sh
rg -n 'fast|wal_on_fast|walonfast|legacy_wal|no_wal_fast|ProfileFast|ProfileWALOnFast|ProfileLegacyWAL|TREEDB_OPEN_PROFILE' \
  TreeDB cmd scripts docs --glob '!docs/benchmarks/**'

rg -n '(ProfileFast|ProfileWALOnFast|ProfileDurable|ProfileLegacyWAL|ProfileNoWALFast|TREEDB_OPEN_PROFILE|\b(wal_on_fast|walonfast|legacy_wal_durable|legacy_wal_relaxed_fast|no_wal_fast)\b|[-=]profile[ =](fast|durable)\b|PROFILES=fast|PROFILE=fast|TREEDB_PROFILE=fast)' \
  TreeDB cmd scripts docs --glob '!docs/benchmarks/**'
```

The broad grep intentionally finds many false positives for ordinary "fast
path" wording. The focused grep found no unclassified active server, nativewire,
Mongo gateway, collection fixture, or kvstore default that exposes the old
profile names as the normal public profile vocabulary.

Remaining hits are classified as:

| class | examples | disposition |
| --- | --- | --- |
| Public rejection tests | `cmd/collection_workload_bench/main_test.go`, `cmd/treedb_out_of_core_smoke/main_test.go`, `TreeDB/profiles_test.go` | Retained because they assert deprecated names reject at public boundaries. |
| Deprecated programmatic compatibility | `TreeDB/profiles.go`, `TreeDB/integration/kvstoreadapter/open.go` | Retained for explicit compatibility and low-level callers; public parsing uses `ParsePublicProfile` and rejects old names. |
| Internal low-level/raw tests | raw cached, iterator, checkpoint, compaction, pointer, and visibility tests using `ProfileFast`, `ProfileWALOnFast`, or `ProfileDurable` | Retained to preserve old raw behavior coverage while the public server surface is narrowed. |
| Unified-bench cross-DB presets | `cmd/unified_bench/profiles.go`, `cmd/unified_bench/README.md`, unified-bench tests | Retained as benchmark-runner presets, not TreeDB server profile names; README states this boundary explicitly. |
| Historical reports and RFCs | `docs/SPRINT_*`, `docs/TREEDB_IMMUTABLE_LEAF_GENERATIONS_RFC.md`, native fast-path design docs | Retained as historical evidence. |
| Legacy/raw benchmark scripts | force-pointer, all-flags, Celestia, and benchmark-report helper scripts | Retained as raw/internal benchmark escape hatches, not recommended server entry points. |

## Summary

The final public profile surface is aligned for current server and collection
entry points: command-WAL durable, command-WAL relaxed, and explicit
benchmark-only `bench`. The closeout YCSB matrix is valid with zero operation
errors, command-WAL throughput remains in the same range as the prior sweep, and
the repeated no-WAL `bench` rerun preserves the expected ceiling. Remaining old
profile references are either rejection tests, explicit compatibility APIs,
internal raw coverage, unified-bench cross-DB presets, historical docs, or
legacy/raw benchmark escape hatches rather than active server defaults.
