# TreeDB canonical deep benchmark, 2026-11-19

The default PR tier of `make deep-benchmark` ran on host185 at
[`0b903917c8cc`](https://github.com/snissn/gomap/commit/0b903917c8ccc256bb95113a0ee5d769cfdd0c17).
The [default HTML report](full_benchmark_report_2026-11-19/deep_report.html)
contains the workload tables, charts, command statuses, and profile inventory.
Download the HTML file and open it in a browser to view the charts.

The default run took 12m 01s and exited with `make` status 2. It started at
23:54:39 UTC on November 19 and ended at 00:06:40 UTC on November 20.
Of 20 recorded commands, 16 passed and 4 failed. All four raw
engine settings, their profile passes, indexed collection runs, and Mongo API
reader/writer sweeps completed.

| Failed command | Result |
| --- | --- |
| `collections_indexes_0` | Zero-index BSON and JSON rows failed the required producer-route evidence check. |
| `mongo_full_sweep` | TreeDB offline index vacuum rejected the benchmark-only `bench_unsafe` profile. |
| `mongo_load_modes` | Same offline-vacuum failure. |
| `mongo_load_scaling` | Same offline-vacuum failure. |

The three Mongo commands stopped on their first TreeDB case after the timed
phase. Their default-run outputs lack complete TreeDB/MongoDB pairs.

## Run configuration

| Setting | Value |
| --- | --- |
| Runner | host185, Intel Core i5-11400F, 6 cores / 12 threads, 31 GiB RAM |
| OS and storage | Linux 6.8.0-138, ext4 on the local NVMe scratch mount |
| Toolchain | Go 1.26.3, Linux/amd64, CGO enabled, `GOWORK=off` |
| Fixture scale | 200,000 raw keys and 100,000 documents per collection or Mongo cell |
| Index sweep | 0, 1, and 2 secondary indexes |
| MongoDB | MongoDB 8.2.7 on Docker 29.1.3, image `mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62` |
| TreeDB profiles | Raw engine `no_wal_fast` and `command_wal_relaxed`, collections `command_wal_relaxed`, Mongo API `bench_unsafe` |
| Isolation | Three shared benchmark locks held for the entire run. Ordinary host activity continued. |

Run this from a clean checkout of the measured commit with Go 1.26.3 on
the path, Docker available, and a writable benchmark volume.

```sh
scratch=$(mktemp -d /mnt/fast4tb/gomap-deep-benchmark.XXXXXXXX)
mkdir -p "$scratch/tmp" "$scratch/gocache"
RUN_ROOT="$scratch/report" TMPDIR="$scratch/tmp" GOTMPDIR="$scratch/tmp" \
  GOCACHE="$scratch/gocache" GOTOOLCHAIN=local GOWORK=off CGO_ENABLED=1 \
  MONGO_IMAGE=mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62 \
  make deep-benchmark
```

## Raw engine

Selected operations from `benchprof_results.json`, in operations per second.
The raw engine uses 128-byte zero-filled values and seed 1. The presets are
`fast` (`no_wal_fast`) and `wal_on_fast` (`command_wal_relaxed`).

| Preset | Checkpoint between tests | Sequential write | Random write | Batch write | Steady batch write | Random read | Parallel random read |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `fast` | Yes | 2,829,216 | 2,616,739 | 241,549 | 1,688,389 | 504,977 | 5,001,260 |
| `fast` | No | 2,892,267 | 2,142,075 | 5,148,996 | 444,392 | 2,134,172 | 22,891,956 |
| `wal_on_fast` | Yes | 312,154 | 288,304 | 1,376,096 | 128,743 | 504,284 | 5,032,083 |
| `wal_on_fast` | No | 313,017 | 301,049 | 2,057,591 | 53,274 | 2,354,270 | 34,867,528 |

## Collections vs SQLite

These are benchmark-timed `post_insert` rates in documents per second. Profile
capture ran separately. The HTML includes the storage and maintenance tables.

| Secondary indexes | TreeDB template-v1 | TreeDB BSON | TreeDB JSON | SQLite native columns | SQLite JSON |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 787,402 | 916,590 (unqualified) | 826,446 (unqualified) | 606,061 | 736,920 |
| 1 | 538,503 | 468,604 | 368,324 | 484,731 | 437,637 |
| 2 | 318,471 | 304,507 | 261,849 | 365,364 | 319,795 |

The zero-index BSON and JSON rows reported zero span candidate and used
operations and no producer-route name. The collection runner recorded
`missing_production_evidence` for both rows. Their rates are retained only
as observations from the failed cell.

## Mongo API reader/writer scaling

The default run completed all 36 reader/writer sweep points across the three
index counts at 1, 2, 4, 8, 16, and 32 clients. These are the 32-client
point-read and point-update observations. Throughput is in operations per
second and p95 latency is in microseconds.

| Secondary indexes | Operation | TreeDB ops/s | MongoDB ops/s | TreeDB p95 (µs) | MongoDB p95 (µs) |
| ---: | --- | ---: | ---: | ---: | ---: |
| 0 | ID lookup | 127,167 | 38,668 | 479 | 1,581 |
| 0 | ID update | 99,581 | 31,455 | 398 | 1,872 |
| 1 | ID lookup | 125,801 | 38,806 | 488 | 1,588 |
| 1 | ID update | 87,604 | 31,862 | 537 | 1,855 |
| 2 | ID lookup | 126,393 | 37,724 | 479 | 1,646 |
| 2 | ID update | 99,436 | 31,358 | 400 | 1,887 |

The Mongo API runs use the benchmark-only TreeDB `bench_unsafe` profile.
These are single-run observations for the recorded settings, with no
durability parity assumed between engines.

## Mongo API checkpoint follow-up

The [checkpoint follow-up HTML](full_benchmark_report_2026-11-19/mongo_checkpoint.html)
covers the three Mongo sections that failed in the default run. All four
recorded commands, including the renderer, passed in 11m 01s
(00:06:42–00:17:43 UTC on November 20). The run used the same source,
image, fixture sizes, and profiles. It used TreeDB checkpoint maintenance
after the timed workload, while MongoDB still ran compact. Compare throughput
within each case and treat the storage figures as different maintenance states.

Use the same environment and scratch directory for the follow-up.

```sh
RUN_ROOT="$scratch/mongo-checkpoint" TMPDIR="$scratch/tmp" GOTMPDIR="$scratch/tmp" \
  GOCACHE="$scratch/gocache" GOTOOLCHAIN=local GOWORK=off CGO_ENABLED=1 \
  MONGO_IMAGE=mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62 \
  SKIP_RAW=true SKIP_COLLECTIONS=true SKIP_SCALING=true TREEDB_MAINTENANCE=checkpoint \
  TITLE="TreeDB Benchmark Report - Mongo Checkpoint Follow-up" make deep-benchmark
```

### Full sweep load

Both engines used `driver-command-raw`, 8 producers, and batches of 10,000.
The full sweep adds an `age` range index to the listed secondary indexes.
The values are documents per second.

| Secondary indexes | TreeDB docs/s | MongoDB docs/s |
| ---: | ---: | ---: |
| 0 | 521,110 | 454,223 |
| 1 | 430,362 | 246,546 |
| 2 | 383,047 | 205,721 |

### Load-only client modes

Each case used 8 producers and batches of 10,000. The columns report
documents per second by secondary index count. The `driver-unack` case uses
MongoDB's unacknowledged write mode.

| Engine | Client mode | 0 indexes | 1 index | 2 indexes |
| --- | --- | ---: | ---: | ---: |
| TreeDB | `driver` | 388,154 | 347,157 | 325,616 |
| TreeDB | `driver-command` | 507,976 | 439,157 | 367,992 |
| TreeDB | `driver-command-raw` | 534,081 | 458,018 | 417,583 |
| TreeDB | `raw-wire-tcp` | 591,142 | 458,022 | 455,184 |
| TreeDB | `raw-wire` | 554,544 | 432,780 | 472,270 |
| MongoDB | `driver` | 217,051 | 226,643 | 198,104 |
| MongoDB | `driver-command` | 258,683 | 287,076 | 236,560 |
| MongoDB | `driver-command-raw` | 264,692 | 300,801 | 241,247 |
| MongoDB | `driver-unack` | 230,776 | 201,005 | 191,561 |

### Load-only producer sweep

The follow-up completed all 18 producer/index pairs using
`driver-command-raw` and batches of 1,000. These are the observed
32-producer points. The chart in the HTML includes all six producer counts.

| Secondary indexes | TreeDB docs/s | MongoDB docs/s |
| ---: | ---: | ---: |
| 0 | 342,174 | 198,093 |
| 1 | 408,516 | 301,658 |
| 2 | 325,491 | 252,811 |

Both HTML reports were regenerated from the completed command logs using the
same source commit. The run bundles retain the original HTML, raw results,
command logs, and profiles.
