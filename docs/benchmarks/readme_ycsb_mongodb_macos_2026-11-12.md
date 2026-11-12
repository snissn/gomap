# Local Mac YCSB and MongoDB benchmark refresh — November 12, 2026

This snapshot replaces three README tables using fixed-source measurements on
the user's Mac. It is a documentation/evidence refresh for
[#5407](https://github.com/snissn/gomap/issues/5407). No production or maintained
benchmark code changes. The requested PR stops before merge.

There are **47 valid retained samples and 1 invalid retained sample** across
48 planned slots: YCSB 11/12, client shape 18/18, full sweep 18/18. All 48 slots
were attempted once. No failed slot was retried. The journaled MongoDB YCSB
comparison remains incomplete. Smoke accounting is separate: 29 valid and
two invalid attempts, including an untimed validator-query mismatch and the
unsupported direct API setup. The archive retains all 79 attempts and their
original source/freeze versions.

## Source, host and timing boundary

| Input | Observed value |
| --- | --- |
| GoMap and maintained harness | `2590d1e9e443c3f968303ce91b3c82933a714f12` |
| External client | clean `snissn/go-ycsb@3c8148495b8d4490cf0ec30e9eb92d14a7a71bac` |
| Toolchain | Go 1.26.10, matching resolved GOROOT/compiler, CGO enabled; native darwin/arm64 |
| YCSB build tags | `libsqlite3,sqlite_omit_load_extension`; no compatibility overlay |
| Host | Mac15,13, 8 physical/logical CPUs, 16 GiB RAM; macOS 26.2 (25C56), Darwin 25.2.0 arm64 |
| Storage/power | local APFS `/dev/disk3s5`; AC power, battery 100%; initial free space about 23 GiB |
| Docker | Desktop 27.5.1; Linux aarch64 6.12.5-linuxkit, 8 CPUs, 8,217,858,048 bytes VM memory |
| MongoDB | 8.2.7 arm64; immutable image ID and digest below; public buildInfo checked per case |
| Native Go runtime | `GOMAXPROCS` unset; actual runtime value not independently observed |
| Frozen input manifest SHA-256 | `7c5900724fced59909a9f8a696307010f4d848d55308154c75ecd105d3c0ab74` |

MongoDB image ID:
`sha256:b8b32a56a1ddc4c278d68124a8f0203663018f62828cadaa995a6cd595f6c9a0`.
Repository digest:
`mongo@sha256:d65a15bd109e970d7dab5496d0a4254705793b8a5c49e344aa6126d46b0e4630`.
The tag lookup was unreliable in the local Docker API, so launch used the
observed immutable image ID. The final manifest records all six binary hashes,
build info, source tree/blob IDs, Docker resources and the host/power/process
snapshot. The initial Mac YCSB SQLite-extension link failure is preserved as
setup evidence; the successful build adds the supported omit-extension tag.

This was an active shared laptop with Codex/UI applications running, not an
isolated benchmark server. User applications were left running. Task-owned
builds, tests, compression and cleanup were kept outside timed collection;
`caffeinate -i` prevented idle sleep. GoMap and clients execute natively on
macOS while MongoDB executes inside Docker Desktop's Linux VM with a host bind
mount. CPU, memory and filesystem topology are asymmetric. These numbers do
not establish a cross-date speedup or isolate COW's contribution.

All tools were built before the retained runs. Each case starts with a fresh
DB directory. Repetition target order alternates MongoDB/GoMap, GoMap/MongoDB,
MongoDB/GoMap; the client-shape TreeDB mode order reverses in repetition two,
then its MongoDB context row follows each mode set. The exact timestamped
execution order and argv are retained. The five never-started YCSB slots were
completed through a reviewed continuation after the failed reopen; no completed
or failed slot was replayed.

The existing producer timing boundaries were preserved. YCSB TOTAL excludes
original client Close. Macrobenchmark load and operation phases exclude tool
builds, public correctness checks, reopen, storage census and maintenance.
Per-run p95/p99 values remain per-run quantiles; their medians below are not
pooled-request quantiles. Brackets show the full minimum–maximum range of the
three valid runs. Individual valid rates and tails are retained in `summary.json`.

## YCSB server

100,000 records; 10,000 uniform operations; 16 workers; 95% reads / 5% updates;
ten binary fields of 100 bytes; hashed FNV user keys; BSON; no inserts/scans/RMW
in the run phase. The same external MongoDB binding and binary serves both
arms. A complete loaded-data census warms the cache before the short run.

| ACK / profile class | MongoDB 8.2.7 ops/s | GoMap Mongo gateway ops/s | Median paired ratio |
| --- | ---: | ---: | ---: |
| Relaxed: both `w:1`, `j` absent | 14,999.7 | 46,921.6 | 3.128x |
| Different durable requests/profiles; incomplete | N/A (2/3 valid) | 5,895.0 | N/A |

Relaxed uses GoMap `command_wal_relaxed` and both clients request `w:1` with
`j` absent. The second class uses GoMap `command_wal_durable`, `w:1`, `j`
absent versus MongoDB `w:1,j:true`; these differing requests apply to both
load and run. It is not a new durable-B, crash, power-loss or same-request
qualification. The maintained Linux YCSB runtime/ELF/procfs validator cannot
run unchanged on this native Mac; this local direct-CLI evidence is lower
scoped and does not replace the
[October Linux qualification](ycsb_mongodb_gomap_final_2026-10-30.md) or reopen
the closed #5335 qualification graph.

The paired relaxed ratio is the median of the three matched run ratios.
The second class has no paired headline or MongoDB aggregate because its
first MongoDB attempt failed the required reopen gate. GoMap's three valid
durable-profile runs are reported independently. No partial failed-sample
throughput enters a selected cell.

### Valid YCSB samples and tails

All selected operation errors are zero. Rates are ops/s; avg/p95/p99/max are
microseconds from the original YCSB producer. `READ` and `UPDATE` run counts
sum to 10,000; the randomized mix varies per run.

| Request class | Target | Rep | Phase / operation | Count | Ops/s | Avg us | p95 us | p99 us | Max us |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| different-durable-requests | gomap | 1 | load/TOTAL | 100,000 | 1,993.8 | 8,000.0 | 14,911.0 | 32,559.0 | 2,185,215.0 |
| different-durable-requests | gomap | 1 | run/TOTAL | 10,000 | 9,223.7 | 1,651.0 | 4,199.0 | 44,927.0 | 50,271.0 |
| different-durable-requests | gomap | 1 | run/READ | 9,487 | 8,750.2 | 113.0 | 293.0 | 563.0 | 2,153.0 |
| different-durable-requests | gomap | 1 | run/UPDATE | 513 | 475.7 | 30,100.0 | 46,719.0 | 49,471.0 | 50,271.0 |
| different-durable-requests | gomap | 2 | load/TOTAL | 100,000 | 2,295.9 | 6,952.0 | 9,239.0 | 13,559.0 | 949,247.0 |
| different-durable-requests | gomap | 2 | run/TOTAL | 10,000 | 3,055.6 | 4,756.0 | 2,873.0 | 132,223.0 | 178,175.0 |
| different-durable-requests | gomap | 2 | run/READ | 9,528 | 2,911.3 | 118.0 | 167.0 | 582.0 | 11,095.0 |
| different-durable-requests | gomap | 2 | run/UPDATE | 472 | 144.6 | 98,382.0 | 156,927.0 | 165,119.0 | 178,175.0 |
| different-durable-requests | mongodb | 2 | load/TOTAL | 100,000 | 8,822.4 | 1,801.0 | 2,885.0 | 7,079.0 | 177,151.0 |
| different-durable-requests | mongodb | 2 | run/TOTAL | 10,000 | 15,755.7 | 1,008.0 | 1,899.0 | 3,479.0 | 8,655.0 |
| different-durable-requests | mongodb | 2 | run/READ | 9,530 | 15,016.6 | 981.0 | 1,817.0 | 3,403.0 | 8,655.0 |
| different-durable-requests | mongodb | 2 | run/UPDATE | 470 | 753.2 | 1,559.0 | 2,945.0 | 4,651.0 | 7,767.0 |
| different-durable-requests | gomap | 3 | load/TOTAL | 100,000 | 2,351.0 | 6,792.0 | 9,159.0 | 14,967.0 | 1,234,943.0 |
| different-durable-requests | gomap | 3 | run/TOTAL | 10,000 | 5,895.0 | 2,571.0 | 2,967.0 | 50,175.0 | 239,487.0 |
| different-durable-requests | gomap | 3 | run/READ | 9,495 | 5,597.5 | 92.0 | 166.0 | 332.0 | 3,239.0 |
| different-durable-requests | gomap | 3 | run/UPDATE | 505 | 337.1 | 49,182.0 | 170,495.0 | 226,943.0 | 239,487.0 |
| different-durable-requests | mongodb | 3 | load/TOTAL | 100,000 | 12,403.0 | 1,279.0 | 2,029.0 | 3,541.0 | 114,687.0 |
| different-durable-requests | mongodb | 3 | run/TOTAL | 10,000 | 11,673.4 | 1,362.0 | 2,455.0 | 7,879.0 | 46,463.0 |
| different-durable-requests | mongodb | 3 | run/READ | 9,463 | 11,045.0 | 1,292.0 | 2,311.0 | 6,487.0 | 44,863.0 |
| different-durable-requests | mongodb | 3 | run/UPDATE | 537 | 640.9 | 2,602.0 | 7,207.0 | 19,711.0 | 46,463.0 |
| relaxed | gomap | 1 | load/TOTAL | 100,000 | 14,409.0 | 1,098.0 | 534.0 | 913.0 | 647,679.0 |
| relaxed | gomap | 1 | run/TOTAL | 10,000 | 65,945.2 | 235.0 | 467.0 | 717.0 | 6,351.0 |
| relaxed | gomap | 1 | run/READ | 9,465 | 62,357.5 | 230.0 | 448.0 | 699.0 | 6,351.0 |
| relaxed | gomap | 1 | run/UPDATE | 535 | 3,578.3 | 328.0 | 628.0 | 822.0 | 923.0 |
| relaxed | mongodb | 1 | load/TOTAL | 100,000 | 11,270.6 | 1,407.0 | 2,479.0 | 6,675.0 | 205,439.0 |
| relaxed | mongodb | 1 | run/TOTAL | 10,000 | 13,102.7 | 1,204.0 | 2,353.0 | 5,055.0 | 12,919.0 |
| relaxed | mongodb | 1 | run/READ | 9,479 | 12,421.9 | 1,204.0 | 2,353.0 | 4,967.0 | 12,919.0 |
| relaxed | mongodb | 1 | run/UPDATE | 521 | 691.2 | 1,212.0 | 2,309.0 | 5,475.0 | 6,951.0 |
| relaxed | gomap | 2 | load/TOTAL | 100,000 | 17,456.4 | 905.0 | 509.0 | 850.0 | 583,167.0 |
| relaxed | gomap | 2 | run/TOTAL | 10,000 | 46,921.6 | 332.0 | 690.0 | 1,089.0 | 6,791.0 |
| relaxed | gomap | 2 | run/READ | 9,488 | 44,467.3 | 324.0 | 665.0 | 1,049.0 | 6,791.0 |
| relaxed | gomap | 2 | run/UPDATE | 512 | 2,415.7 | 475.0 | 928.0 | 1,340.0 | 1,765.0 |
| relaxed | mongodb | 2 | load/TOTAL | 100,000 | 17,445.1 | 904.0 | 1,623.0 | 2,661.0 | 173,951.0 |
| relaxed | mongodb | 2 | run/TOTAL | 10,000 | 14,999.7 | 1,058.0 | 1,894.0 | 3,973.0 | 20,879.0 |
| relaxed | mongodb | 2 | run/READ | 9,547 | 14,318.2 | 1,058.0 | 1,891.0 | 3,949.0 | 20,879.0 |
| relaxed | mongodb | 2 | run/UPDATE | 453 | 704.9 | 1,062.0 | 2,034.0 | 4,967.0 | 12,655.0 |
| relaxed | gomap | 3 | load/TOTAL | 100,000 | 18,395.3 | 859.0 | 524.0 | 973.0 | 617,983.0 |
| relaxed | gomap | 3 | run/TOTAL | 10,000 | 25,993.8 | 608.0 | 540.0 | 778.0 | 220,159.0 |
| relaxed | gomap | 3 | run/READ | 9,487 | 24,659.9 | 255.0 | 525.0 | 739.0 | 1,274.0 |
| relaxed | gomap | 3 | run/UPDATE | 513 | 1,333.8 | 7,119.0 | 783.0 | 217,599.0 | 220,159.0 |
| relaxed | mongodb | 3 | load/TOTAL | 100,000 | 18,229.8 | 864.0 | 1,585.0 | 2,415.0 | 66,815.0 |
| relaxed | mongodb | 3 | run/TOTAL | 10,000 | 16,040.0 | 980.0 | 1,891.0 | 3,591.0 | 6,751.0 |
| relaxed | mongodb | 3 | run/READ | 9,537 | 15,294.0 | 979.0 | 1,886.0 | 3,551.0 | 6,751.0 |
| relaxed | mongodb | 3 | run/UPDATE | 463 | 754.2 | 1,016.0 | 2,097.0 | 4,111.0 | 6,111.0 |

### YCSB distributions

| Request class | Target | Load ops/s median [min, max] | Run ops/s median [min, max] |
| --- | --- | ---: | ---: |
| different-durable-requests | gomap | 2,295.9 [1,993.8, 2,351.0] | 5,895.0 [3,055.6, 9,223.7] |
| relaxed | gomap | 17,456.4 [14,409.0, 18,395.3] | 46,921.6 [25,993.8, 65,945.2] |
| relaxed | mongodb | 17,445.1 [11,270.6, 18,229.8] | 14,999.7 [13,102.7, 16,040.0] |

### Invalid journaled MongoDB attempt

`runs/ycsb/ycsb-different-durable-requests-p1-mongodb` completed its original
load/run and original Close receipts, first full public data checks, deterministic
fixture and first graceful stop. The new MongoDB container failed during reopen:
exit 133, `OOMKilled=false`; WiredTiger reported ENOENT while syncing
`/data/db/_tmp/spilldb/` and creating `WiredTiger.turtle.set`, then WT_PANIC /
fatal assertion 50853. The diagnostic logs and inspect output are preserved.
The coordinator interrupted the owned runner's dead-server readiness loop;
cleanup removed the failed container and retained the DB until evidence review.
The explicit `failure-disposition.json` records the failure despite Python's
empty `KeyboardInterrupt` string. This cell is invalid and excluded in full.
The other two MongoDB journaled attempts passed; the evidence does not establish
that `journal=true` caused this Docker bind-mount reopen failure.

### YCSB correctness and lifecycle scope

The untimed public validator enumerates every expected FNV key, checks all ten
typed binary fields and their lengths, and rejects missing/extra/duplicate rows.
Canonical typed-BSON digests must match before close and after independent
reopen. A separate 64-record deterministic exact-value write/update fixture
is read back after reopen. MongoDB's fixture uses `w:1`/default even in the
journaled class, so it is not a `j:true` fixture qualification. The randomized
workload check preserves observed state; it cannot prove every acknowledged
random update's intended value or atomicity.

The original YCSB command, DB Close and workload Close are checked at their
producer with phase/build/profile identity. Gateway SIGTERM must exit zero,
covering its joined original Close and server exit; MongoDB stop must be
graceful with container absence confirmed. These are graceful lifecycle checks.
They do not establish crash or failure-semantics equivalence.

## Mongo gateway client shape

200,000 prebuilt gateway-shaped BSON documents, batch 1,000, 16 producers,
two secondary indexes (`email` unique, `city`), full-document point responses,
settled reads, 100,000 reads of each kind at 16 readers. TreeDB uses
`command_wal_relaxed`, BSON and compressed roots; maintenance is `none`.
The MongoDB driver-raw row uses the same counts and client pool limits
(maximum pool 16, maximum connecting 16, minimum pool 0) in the Linux VM.

| Access path | Load docs/s | `_id` reads/s, r16 | `email` reads/s, r16 | `_id` p95 us | `email` p95 us |
| --- | ---: | ---: | ---: | ---: | ---: |
| TreeDB / Mongo driver CRUD | 140,993 | 93,431 | 71,919 | 301 | 477 |
| TreeDB / Mongo driver command | 143,345 | 92,226 | 66,442 | 305 | 521 |
| TreeDB / Mongo driver raw command | 144,951 | 90,676 | 73,161 | 311 | 474 |
| TreeDB / Mongo BSON raw wire over TCP | 145,966 | 113,277 | 89,880 | 239 | 399 |
| TreeDB / native wire over TCP | 146,273 | 91,359 | 70,971 | 308 | 487 |
| MongoDB / driver raw command | 238,786 | 19,274 | 19,812 | 1,528 | 1,448 |
| TreeDB direct collection API | N/A | N/A | N/A | N/A | N/A |

The old direct collection ceiling is unavailable: fixed-source smoke index
setup rejects catalog index mutation under `command_wal_v2` before load.
Its failed attempt and exact error are retained in `smoke-unhinted`. A different
profile was not substituted. The old driver-unack row is removed because
current TreeDB rejects `w:0` before mutation. Native wire is a distinct
TreeDB protocol, not a MongoDB wire compatibility measurement.

### Client-shape phase distributions

Zero-operation placeholder phases are omitted. Rates are ops/s (load is docs/s).
Latency columns are medians of per-run p95/p99 in microseconds.
Load quantiles describe batch driver calls, not single-document latency.

| Target / mode | Phase | Ops/s median [min, max] | p95 us | p99 us |
| --- | --- | ---: | ---: | ---: |
| gomap / driver | `load_insert_many` | 140,993 [133,127, 148,499] | 235,641 | 247,676 |
| gomap / driver | `concurrent_id_find_one_r16` | 93,431 [92,007, 93,632] | 301 | 445 |
| gomap / driver | `concurrent_email_find_one_r16` | 71,919 [50,183, 73,851] | 477 | 693 |
| gomap / driver-command | `load_insert_many` | 143,345 [139,852, 172,062] | 241,742 | 253,594 |
| gomap / driver-command | `concurrent_id_find_one_r16` | 92,226 [89,677, 93,234] | 305 | 452 |
| gomap / driver-command | `concurrent_email_find_one_r16` | 66,442 [65,930, 74,410] | 521 | 766 |
| gomap / driver-command-raw | `load_insert_many` | 144,951 [140,215, 174,639] | 215,411 | 231,641 |
| gomap / driver-command-raw | `concurrent_id_find_one_r16` | 90,676 [43,706, 92,601] | 311 | 462 |
| gomap / driver-command-raw | `concurrent_email_find_one_r16` | 73,161 [61,107, 75,497] | 474 | 684 |
| mongodb / driver-command-raw | `load_insert_many` | 238,786 [209,870, 244,190] | 115,436 | 142,685 |
| mongodb / driver-command-raw | `concurrent_id_find_one_r16` | 19,274 [17,685, 19,784] | 1,528 | 2,395 |
| mongodb / driver-command-raw | `concurrent_email_find_one_r16` | 19,812 [17,899, 19,869] | 1,448 | 2,092 |
| gomap / native-wire-tcp | `load_insert_many` | 146,273 [76,031, 161,051] | 206,992 | 238,314 |
| gomap / native-wire-tcp | `concurrent_id_find_one_r16` | 91,359 [64,597, 92,444] | 308 | 457 |
| gomap / native-wire-tcp | `concurrent_email_find_one_r16` | 70,971 [36,987, 75,317] | 487 | 706 |
| gomap / raw-wire-tcp | `load_insert_many` | 145,966 [141,796, 189,225] | 236,591 | 253,521 |
| gomap / raw-wire-tcp | `concurrent_id_find_one_r16` | 113,277 [110,394, 113,315] | 239 | 322 |
| gomap / raw-wire-tcp | `concurrent_email_find_one_r16` | 89,880 [87,177, 91,487] | 399 | 569 |

## Mongo API full sweep

100,000 prebuilt gateway BSON documents, batch 1,000, 16 producers,
`driver-command-raw` on both arms, full responses, TreeDB relaxed and settled
reads. Each row adds the `age_1` range index regardless of the displayed
secondary count; count one adds unique `email_1`, count two also adds `city_1`.
Sequential `_id` reads total 100,000, email reads total 100,000 when present,
range reads and updates each total 10,000. Concurrent `_id` reads total 100,000
at each of 1/16/32 workers; concurrent range reads and writes total 10,000
each at each of 1/16/32 workers. Deletes are zero.

| Secondary indexes | TreeDB load docs/s | MongoDB load docs/s | TreeDB `_id` r16 ops/s | MongoDB `_id` r16 ops/s | TreeDB allocated MiB | MongoDB allocated MiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 219,210 | 231,552 | 79,668 | 18,789 | 49.90 | 214.19 |
| 1 | 170,389 | 182,436 | 79,146 | 14,014 | 50.62 | 216.61 |
| 2 | 124,506 | 156,915 | 74,425 | 17,712 | 50.07 | 216.75 |

### Physical allocation endpoint

Allocation is the sum of macOS `stat.st_blocks * 512` for every file under
the stopped DB directory after the full sweep and untimed maintenance:
TreeDB checkpoint versus MongoDB compact. It includes all redo/command
journals, WAL and persistent value logs. It excludes Docker's VM image,
program binaries, source, caches and the task-owner marker outside the DB.
This is allocated space at that endpoint, not logical size, post-insert-only
size, WAL-excluded size, or identical maintenance semantics. MiB means 2^20
bytes. Per-file logical and allocated bytes are retained.

Maintenance is separately identified by raw `treedb_maintenance_mode=checkpoint`
and `mongo_compact=true`; normal producer completion requires these steps to
succeed. Their individual durations are not separately timed by this existing
harness. No maintenance latency or equivalence is inferred from phase timings.

| Secondary indexes | Target | Allocated MiB median [min, max] |
| ---: | --- | ---: |
| 0 | gomap | 49.90 [49.90, 50.07] |
| 0 | mongodb | 214.19 [214.19, 214.23] |
| 1 | gomap | 50.62 [46.62, 50.62] |
| 1 | mongodb | 216.61 [216.20, 216.75] |
| 2 | gomap | 50.07 [46.89, 50.89] |
| 2 | mongodb | 216.75 [216.59, 217.94] |

### Full-sweep phase distributions

Rates are ops/s (load is docs/s); latency columns are medians of per-run
p95/p99 in microseconds. Exact operations, durations and every per-run tail
are retained in the summary and raw producer JSON.
Load quantiles describe batch driver calls, not single-document latency.


#### 0 displayed secondary indexes, plus age range index

| Target | Phase | Ops/s median [min, max] | p95 us | p99 us |
| --- | --- | ---: | ---: | ---: |
| gomap | `load_insert_many` | 219,210 [163,067, 240,758] | 36,778 | 40,465 |
| gomap | `id_find_one` | 22,814 [20,565, 23,016] | 55 | 101 |
| gomap | `age_range_indexed_limit_10` | 23,581 [23,390, 23,589] | 46 | 57 |
| gomap | `concurrent_age_range_indexed_limit_10_r1` | 23,583 [20,139, 23,651] | 46 | 53 |
| gomap | `concurrent_age_range_indexed_limit_10_r16` | 60,134 [43,550, 60,549] | 717 | 1,025 |
| gomap | `concurrent_age_range_indexed_limit_10_r32` | 58,207 [44,391, 59,071] | 1,002 | 1,377 |
| gomap | `id_update_set` | 13,547 [13,274, 15,453] | 83 | 139 |
| gomap | `concurrent_id_find_one_r1` | 21,377 [21,266, 22,029] | 59 | 108 |
| gomap | `concurrent_id_find_one_r16` | 79,668 [63,462, 93,951] | 406 | 710 |
| gomap | `concurrent_id_find_one_r32` | 77,825 [77,232, 78,677] | 694 | 1,051 |
| gomap | `concurrent_id_update_set_w1` | 14,633 [13,237, 14,637] | 79 | 178 |
| gomap | `concurrent_id_update_set_w16` | 43,312 [33,554, 47,825] | 423 | 661 |
| gomap | `concurrent_id_update_set_w32` | 43,110 [39,133, 45,172] | 744 | 1,066 |
| mongodb | `load_insert_many` | 231,552 [147,738, 376,357] | 112,608 | 113,736 |
| mongodb | `id_find_one` | 4,250 [3,661, 4,495] | 321 | 1,235 |
| mongodb | `age_range_indexed_limit_10` | 4,295 [4,106, 4,376] | 259 | 1,094 |
| mongodb | `concurrent_age_range_indexed_limit_10_r1` | 4,013 [3,597, 4,159] | 297 | 1,087 |
| mongodb | `concurrent_age_range_indexed_limit_10_r16` | 17,148 [17,095, 17,665] | 1,633 | 2,194 |
| mongodb | `concurrent_age_range_indexed_limit_10_r32` | 16,943 [15,044, 17,087] | 2,851 | 4,023 |
| mongodb | `id_update_set` | 4,337 [3,449, 4,696] | 325 | 1,214 |
| mongodb | `concurrent_id_find_one_r1` | 4,249 [4,090, 4,270] | 354 | 1,222 |
| mongodb | `concurrent_id_find_one_r16` | 18,789 [15,938, 19,064] | 1,520 | 2,083 |
| mongodb | `concurrent_id_find_one_r32` | 13,601 [12,759, 15,136] | 3,806 | 7,063 |
| mongodb | `concurrent_id_update_set_w1` | 4,584 [3,752, 4,734] | 242 | 1,055 |
| mongodb | `concurrent_id_update_set_w16` | 19,187 [12,330, 19,694] | 1,461 | 2,046 |
| mongodb | `concurrent_id_update_set_w32` | 18,036 [8,208, 19,643] | 2,661 | 3,693 |

#### 1 displayed secondary indexes, plus age range index

| Target | Phase | Ops/s median [min, max] | p95 us | p99 us |
| --- | --- | ---: | ---: | ---: |
| gomap | `load_insert_many` | 170,389 [110,435, 173,915] | 81,093 | 87,914 |
| gomap | `id_find_one` | 22,909 [22,023, 24,527] | 53 | 88 |
| gomap | `email_find_one` | 19,797 [19,567, 20,570] | 62 | 105 |
| gomap | `age_range_indexed_limit_10` | 22,965 [22,370, 23,140] | 48 | 63 |
| gomap | `concurrent_age_range_indexed_limit_10_r1` | 23,049 [21,617, 23,230] | 47 | 56 |
| gomap | `concurrent_age_range_indexed_limit_10_r16` | 55,362 [39,909, 57,388] | 804 | 1,166 |
| gomap | `concurrent_age_range_indexed_limit_10_r32` | 53,442 [47,760, 55,614] | 1,133 | 1,516 |
| gomap | `id_update_set` | 14,864 [10,941, 15,087] | 63 | 72 |
| gomap | `concurrent_id_find_one_r1` | 18,855 [17,105, 22,901] | 63 | 140 |
| gomap | `concurrent_id_find_one_r16` | 79,146 [75,256, 91,752] | 353 | 706 |
| gomap | `concurrent_id_find_one_r32` | 79,523 [62,176, 83,722] | 608 | 982 |
| gomap | `concurrent_id_update_set_w1` | 14,905 [7,347, 15,846] | 71 | 172 |
| gomap | `concurrent_id_update_set_w16` | 43,040 [24,733, 43,340] | 362 | 577 |
| gomap | `concurrent_id_update_set_w32` | 41,667 [25,668, 43,884] | 616 | 879 |
| mongodb | `load_insert_many` | 182,436 [149,801, 219,052] | 131,905 | 149,564 |
| mongodb | `id_find_one` | 3,869 [3,676, 4,279] | 391 | 1,280 |
| mongodb | `email_find_one` | 3,510 [3,009, 4,221] | 518 | 1,402 |
| mongodb | `age_range_indexed_limit_10` | 3,264 [3,106, 3,919] | 472 | 1,311 |
| mongodb | `concurrent_age_range_indexed_limit_10_r1` | 3,351 [2,860, 3,949] | 484 | 1,402 |
| mongodb | `concurrent_age_range_indexed_limit_10_r16` | 13,810 [8,803, 15,906] | 2,001 | 3,875 |
| mongodb | `concurrent_age_range_indexed_limit_10_r32` | 14,573 [5,986, 16,326] | 3,570 | 5,341 |
| mongodb | `id_update_set` | 4,050 [3,423, 4,780] | 402 | 1,202 |
| mongodb | `concurrent_id_find_one_r1` | 3,990 [3,314, 4,106] | 406 | 1,276 |
| mongodb | `concurrent_id_find_one_r16` | 14,014 [10,185, 18,948] | 2,077 | 3,832 |
| mongodb | `concurrent_id_find_one_r32` | 13,160 [10,432, 17,118] | 3,929 | 8,597 |
| mongodb | `concurrent_id_update_set_w1` | 3,109 [2,679, 4,168] | 523 | 1,425 |
| mongodb | `concurrent_id_update_set_w16` | 14,863 [13,999, 18,285] | 2,113 | 3,134 |
| mongodb | `concurrent_id_update_set_w32` | 12,231 [7,842, 16,087] | 4,186 | 11,101 |

#### 2 displayed secondary indexes, plus age range index

| Target | Phase | Ops/s median [min, max] | p95 us | p99 us |
| --- | --- | ---: | ---: | ---: |
| gomap | `load_insert_many` | 124,506 [119,240, 168,328] | 206,683 | 208,858 |
| gomap | `id_find_one` | 20,397 [17,978, 20,465] | 62 | 124 |
| gomap | `email_find_one` | 19,756 [19,647, 20,417] | 61 | 70 |
| gomap | `age_range_indexed_limit_10` | 23,674 [22,754, 23,813] | 46 | 60 |
| gomap | `concurrent_age_range_indexed_limit_10_r1` | 23,904 [20,387, 24,130] | 45 | 51 |
| gomap | `concurrent_age_range_indexed_limit_10_r16` | 57,024 [45,593, 63,489] | 754 | 1,093 |
| gomap | `concurrent_age_range_indexed_limit_10_r32` | 48,372 [44,288, 59,195] | 1,282 | 1,906 |
| gomap | `id_update_set` | 12,558 [12,190, 14,445] | 76 | 132 |
| gomap | `concurrent_id_find_one_r1` | 19,779 [16,282, 20,801] | 58 | 112 |
| gomap | `concurrent_id_find_one_r16` | 74,425 [58,899, 92,773] | 406 | 647 |
| gomap | `concurrent_id_find_one_r32` | 74,009 [64,931, 91,423] | 660 | 937 |
| gomap | `concurrent_id_update_set_w1` | 15,128 [12,589, 15,804] | 64 | 74 |
| gomap | `concurrent_id_update_set_w16` | 38,219 [33,725, 41,543] | 449 | 669 |
| gomap | `concurrent_id_update_set_w32` | 40,009 [34,988, 40,605] | 720 | 1,094 |
| mongodb | `load_insert_many` | 156,915 [76,076, 177,934] | 164,041 | 184,564 |
| mongodb | `id_find_one` | 4,433 [2,728, 4,452] | 301 | 1,208 |
| mongodb | `email_find_one` | 4,500 [3,834, 4,548] | 247 | 1,173 |
| mongodb | `age_range_indexed_limit_10` | 4,207 [4,092, 4,227] | 271 | 1,156 |
| mongodb | `concurrent_age_range_indexed_limit_10_r1` | 4,171 [4,058, 4,236] | 267 | 1,101 |
| mongodb | `concurrent_age_range_indexed_limit_10_r16` | 16,765 [15,551, 17,204] | 1,750 | 2,635 |
| mongodb | `concurrent_age_range_indexed_limit_10_r32` | 16,750 [15,886, 16,874] | 2,898 | 4,413 |
| mongodb | `id_update_set` | 4,529 [4,385, 4,696] | 255 | 1,118 |
| mongodb | `concurrent_id_find_one_r1` | 4,593 [4,590, 4,601] | 241 | 1,151 |
| mongodb | `concurrent_id_find_one_r16` | 17,712 [15,952, 19,162] | 1,598 | 2,436 |
| mongodb | `concurrent_id_find_one_r32` | 16,836 [15,567, 19,311] | 3,125 | 5,446 |
| mongodb | `concurrent_id_update_set_w1` | 4,751 [4,400, 5,458] | 228 | 1,032 |
| mongodb | `concurrent_id_update_set_w16` | 19,550 [19,415, 19,607] | 1,440 | 1,984 |
| mongodb | `concurrent_id_update_set_w32` | 18,706 [18,019, 19,083] | 2,537 | 3,410 |

## Macrobenchmark validation and limits

All 36 selected macrobenchmark attempts completed the declared phase counts
and independently reopened the full expected document set. The public typed
validator checks IDs, immutable base fields, typed mutation-marker pairs,
zero missing/extra/duplicate rows, declared index metadata where supported,
sample `_id`/email lookups and complete expected city/age query results.
MongoDB preclose/reopen canonical state digests also match. The initial strict
hint query was unsupported by this gateway; the corrected validator uses the
same unhinted public query form as the producer and retains the initial failed
smoke and its old source/freeze. Index metadata and result correctness do not
prove which physical index a query activated.

The existing `mongo_gateway_bench` discards its original deferred
`closeBenchTarget` return. That result is **unobserved** in every shape/sweep
sample; a later successful verifier Close cannot recover it. Raw-command insert
counts are nominal producer batch counts after command Err checks; the complete
public reopened census independently verifies final data. The validator does
not prove every intermediate update's intended result or concurrent schedule.
Verification gateways have separately checked successful original exit/Close,
and external MongoDB containers have checked graceful stop/absence. No claim
of original macrobenchmark cleanup success, crash durability, every ACK,
transaction atomicity or full MongoDB semantics follows.

## Evidence and reproduction

The [readme_ycsb_mongodb_macos_2026-11-12 evidence directory](readme_ycsb_mongodb_macos_2026-11-12/) contains:

- `summary.json`: deterministic selection, complete three-repetition summaries,
  individual valid sample metrics, excluded slot and all-attempt ledger.
- `freeze.json`: actual final precollection source/runtime/binary/host manifest.
- `raw.tar.gz` and `raw-index.json`: compact raw outputs, original command/Close/
  lifecycle receipts, typed censuses, all failed attempts/dispositions, setup
  logs, smoke source history, helper/recipe source and precollection reviews.
- `aggregate.py`, `render.py`, `verify_archive.py`: analysis and archive checks;
  `census.go.txt` preserves validation-only Go source without adding a repo Go
  package. The archive contains its original `recipe/census.go` filename.

Archive inspection and deterministic aggregation require Python 3.9+ and no
database server or Go build. From the repository root:

```sh
EVIDENCE=docs/benchmarks/readme_ycsb_mongodb_macos_2026-11-12
python3 "$EVIDENCE/verify_archive.py" --directory "$EVIDENCE"
EXTRACT=$(mktemp -d /tmp/gomap-readme-evidence.XXXXXX)
tar -xzf "$EVIDENCE/raw.tar.gz" -C "$EXTRACT"
python3 "$EXTRACT/recipe/aggregate.py" --root "$EXTRACT"
cmp "$EXTRACT/derived/summary.json" "$EVIDENCE/summary.json"
```

A new measurement campaign must build the pinned product and external client
with the recorded toolchain, tags and build commands, copy the archived
validation/runner sources into a fresh task root, and freeze the new absolute
source/binary locations using `recipe/freeze.py --root "$TASK" --repo "$PRODUCT"
--go "$GO" --mongo-image-id <observed-immutable-ID>`. The portable CLI is an
analysis/reproduction convenience; `recipe-history/freeze-original.py` preserves
the actual generator used for this campaign. The generator records host
and binary identities anew; the historical manifest must not be reused or
edited to pass validation. Build `recipe/census.go` with the product module
environment (`go build -o "$TASK/bin/census" "$TASK/recipe/census.go"`).
Then execute each suite once, serially, with the reviewed recipe:

```sh
caffeinate -i python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite smoke
caffeinate -i python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite ycsb
caffeinate -i python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite shape
caffeinate -i python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite sweep
```

Exact argv/environment/phase receipts are the authority for each historical
sample. A failure stops its suite; retain it and reassess rather than rerun to
obtain a favorable sample. The one reviewed historical continuation fills only
the five never-started YCSB slots. Raw DBs, core dumps, binaries and caches are
excluded from Git. Successful case DBs were released only after required
public/lifecycle validation and a saved validation record.
