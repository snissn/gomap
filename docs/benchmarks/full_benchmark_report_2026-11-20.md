# TreeDB canonical benchmark report, November 20, 2026

The default PR-tier `make deep-benchmark` ran on host185 against
[`4fa77f0f5`](https://github.com/snissn/gomap/commit/4fa77f0f54ba95f4b435866d410801ce0bb4f7fd).
**20 of 20 stages passed** in 22 min 17 s. The run used an Intel
i5-11400F (6 cores, 12 threads), Linux 6.8.0-138-generic, Go 1.26.3,
local NVMe ext4 on `/mnt/fast4tb`, and MongoDB 8.2.7 in local Docker.
Shared benchmark locks excluded other coordinated heavy jobs. Ordinary
host services remained active. No AWS infrastructure was used.

[Download the self-contained HTML report](full_benchmark_report_2026-11-20/deep_report.html) ·
[Download the raw receipts](full_benchmark_report_2026-11-20/receipts.tar.gz) ·
[SHA-256 checksums](full_benchmark_report_2026-11-20/SHA256SUMS).

## Default report coverage

| Section | Retained coverage |
| --- | --- |
| Raw engine | 4 profiled WAL and between-test checkpoint combinations, 200,000 keys each |
| Canonical collections and SQLite | 12 timed matrix configurations across 0, 1 and 2 indexes, 100,000 documents each |
| Mongo API full sweep | 6 engine/configuration results, including load, reads, range reads, updates and concurrency sweeps |
| Mongo API client modes | 27 engine/client/index results |
| Mongo API InsertMany producer scaling | 36 engine/producer/index results, from 1 through 32 producers |
| Mongo API reader and writer scaling | 72 engine/direction/width/index results |

The canonical collection matrix used the maintained buffered legacy writer.
The Mongo API suite used the benchmark-only `bench_unsafe` profile with
the command WAL disabled and default full maintenance. `no_wal_fast`
also disables the command WAL and keeps read verification. See the
separate collection timing below for the native COW comparison and
[#5592](https://github.com/snissn/gomap/issues/5592) for journaled singleton YCSB.
[Mongo API benchmark profile details](../../cmd/mongo_gateway_bench/README.md).

The zero-index BSON and JSON each completed the 100,000-document ingest
benchmark and used both ordered-root command-WAL producer candidates.

## Native COW versus legacy collection inserts

Both arms used the canonical value-log configuration, `command_wal_relaxed`,
16,000 documents and 1,000 documents per batch. Each shape ran three
pairs with fresh processes and alternating arm order. Legacy includes
final buffered publication in the insert timer. Native uses public
`Collection.InsertBatch` and verified 16 COW publications for 16 batches.
The timed window excludes fixture generation, the final explicit checkpoint
and the 16,000-ID census. Checkpoint times below are separate.

| Format | Secondary indexes | Write owner | Median ns/doc | Median B/doc | Median allocs/doc | Median checkpoint (ms) |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| BSON | 0 | Legacy | 1,450 | 1,972 | 1 | 34.17 |
| BSON | 0 | Native COW | 1,653 | 1,712 | 7 | 105.41 |
| JSON | 0 | Legacy | 1,267 | 1,937 | 1 | 34.47 |
| JSON | 0 | Native COW | 1,600 | 1,687 | 7 | 105.49 |
| BSON | 2 | Legacy | 2,469 | 4,028 | 1 | 50.10 |
| BSON | 2 | Native COW | 4,801 | 3,840 | 18 | 135.73 |

Native COW took **1.14×** legacy ns/doc for BSON with zero indexes,
**1.26×** for JSON with zero indexes and **1.94×** for BSON
with two indexes. The November 19 ratios were 1.33×, 1.41× and 2.12×.
These are separate host185 sessions with three samples per arm. The
matched native comparison keeps the 16,000-document fixture from the
earlier run. [November 19 collection report](cow_benchmark_2026-11-19.md#canonical-collection-inserts).

## Reproduce locally

From the pinned source with Go 1.26.3, a running local Docker engine
and fresh task-owned storage, use the default PR-tier target.

```sh
export GOWORK=off CGO_ENABLED=1 GOTOOLCHAIN=local GOENV=off
export TMPDIR="$BENCH_ROOT/tmp" GOTMPDIR="$BENCH_ROOT/tmp"
export GOCACHE="$BENCH_ROOT/gocache"
mkdir -p "$TMPDIR" "$GOCACHE"
export RUN_ROOT="$BENCH_ROOT/report"
export MONGO_IMAGE=mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62
make deep-benchmark
```

The archive contains the expanded runbook, exact commands, profile
receipts, stage statuses, host samples and each interleaved COW sample.
The four full Go runtime traces are retained on host185.
The archived HTML is the original renderer output, captured during stage 20.
The downloadable HTML was rendered after all 20 stage statuses had been written.
The COW replay uses the maintained canonical benchmark pair and the
same environment for both owners. The recorded `run-cow.sh` shows the
environment and order. The report records this host and configuration.
