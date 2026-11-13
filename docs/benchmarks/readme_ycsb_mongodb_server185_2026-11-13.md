# Linux server185 YCSB and MongoDB README alternative — November 13, 2026 UTC

This is a separate Linux alternative to [PR #5408](https://github.com/snissn/gomap/pull/5408), for [#5407](https://github.com/snissn/gomap/issues/5407). It measures current GoMap including merged [#5410](https://github.com/snissn/gomap/pull/5410) on the user-selected SSH server185. The Mac dataset retains its own source and host identity.

**48 valid retained samples and 0 invalid retained samples across 48 planned slots.** All fixed slots are accounted for below; no failed slot is replayed or replaced. Only complete three-repetition groups supply summary cells. Smoke is separate: 16 valid, 0 invalid, 0 incomplete. The complete attempt ledger and all raw evidence are retained.

## Source, host and timing boundary

| Input | Observed value |
| --- | --- |
| GoMap runtime | `4988738a2689b462cdb2053fa1e563f8c0091e23`; includes #5410 |
| External client | `snissn/go-ycsb@3c8148495b8d4490cf0ec30e9eb92d14a7a71bac`; same client as #5408 |
| Reviewed/landed harness | #5413; candidate `b2d6cf787f9e0dd49fb6c668085f39c6595b5fc4`; subtree `c39cf0cae9c84b237c10a1718fa7b74e32a65e46`; landing readback in raw archive |
| Toolchain | Go 1.26.10 linux/amd64, CGO enabled; native client and GoMap |
| YCSB build tags | `libsqlite3,sqlite_omit_load_extension`; no source overlays |
| Host | Intel Core i5-11400F, 6 cores / 12 logical CPUs, 32 GB installed RAM; Linux 6.8.0-138-generic x86_64 |
| Storage | Samsung SSD 990 PRO 4 TB; `/mnt/fast4tb`, ext4 `rw,noatime`; approximately 120 GiB available before collection |
| Docker | Linux engine 29.1.3; native x86_64 containers on the same kernel, no Docker Desktop VM |
| MongoDB | 8.2.7 x86_64, immutable image; per-case public buildInfo checked |
| MongoDB container user | Linux host UID:GID `1000:1000`; bound to create and inspect receipts |
| Go runtime concurrency | `GOMAXPROCS` unset for benchmark processes; effective runtime value not independently observed |
| Frozen input manifest SHA-256 | `cf01a72ab5c317225006e49aa80c61ae1d45f3549ef51b3634668ec8bc9cd4f0` |

MongoDB image ID: `sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54`. Repository digest: `mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62`.

This is a shared host with unrelated services left running. `INFRASTRUCTURE_UNAVAILABLE: runner: shared user-selected server185; unrelated services remain active`. Persistent shared Go build/module caches and durable task storage were available. All builds, tests, archive compression and bulk transfers occurred outside timed collection. Timed cases are serial and fresh-directory based. Host diagnostics and exact timestamped argv are in the archive; there is no dedicated-runner or stable-frequency claim.

The workload and target/mode ordering match #5408: three fresh repetitions, alternating MongoDB/GoMap, GoMap/MongoDB, MongoDB/GoMap; client-shape GoMap mode order reverses in repetition two, with the MongoDB context row after each mode set. The already validated 16-case diagnostic smoke is reused at identical frozen source/binary/recipe identities, with the landed-tree readback recorded separately. Retained collection starts after harness landing.

YCSB timing excludes original client Close; macrobenchmark phase timing excludes builds, correctness census, independent reopen, storage inspection and maintenance. Rates and tails below are medians with full minimum–maximum ranges. Tail medians are medians of per-run percentiles, not pooled request percentiles. Individual original statistics remain in the sample tables and `summary.json`.

## YCSB server

100,000 records; 10,000 uniform operations; 16 workers; 95% reads / 5% updates; ten binary fields of 100 bytes; hashed FNV user keys; BSON; no inserts/scans/RMW during the run phase. Both arms use the same external MongoDB client binding. A full loaded-data census warms the cache before the short run.

| Request/profile class | MongoDB run ops/s median [min, max] | GoMap run ops/s median [min, max] | Paired GoMap/Mongo ratio median [min, max] |
| --- | --- | --- | --- |
| relaxed | 26,541.3 [26,126.8, 26,551.2] | 17,161.7 [14,044.6, 17,647.6] | 0.647 [0.538, 0.665] |
| different-durable-requests | 25,470.4 [24,678.7, 25,678.5] | 51,716.1 [50,783.0, 51,890.6] | 2.030 [2.021, 2.058] |

Relaxed uses GoMap `command_wal_relaxed` and both arms request `w:1` with `j` absent. The different-durable-requests class uses GoMap `command_wal_durable`, `w:1`, `j` absent against MongoDB `w:1,j:true` for both load and run. It does not establish matched durability, durable-B qualification, crash or power-loss equivalence. This direct-CLI recipe does not replace the separate [October Linux runtime-attested qualification](ycsb_mongodb_gomap_final_2026-10-30.md).

The public validator enumerates every expected key and all ten typed binary fields, rejects missing/extra/duplicate records, and requires equal canonical typed-state digests before close and after independent reopen. A separate 64-record deterministic write/update fixture is read back after reopen. MongoDB's fixture uses `w:1`/default even in the journaled class. The random-workload census proves observed persisted state, not every acknowledged update's intended value or atomicity. Original YCSB command, DB Close and workload Close receipts are checked with phase/build/profile identity. Gateway SIGTERM exit zero and graceful MongoDB stop are required.

| Class | Target | Load ops/s median [min, max] | Run ops/s median [min, max] |
| --- | --- | --- | --- |
| relaxed | gomap | 20,172.2 [18,705.8, 20,433.8] | 17,161.7 [14,044.6, 17,647.6] |
| relaxed | mongodb | 39,585.7 [39,434.5, 41,065.1] | 26,541.3 [26,126.8, 26,551.2] |
| different-durable-requests | gomap | 4,935.7 [4,920.8, 4,946.8] | 51,716.1 [50,783.0, 51,890.6] |
| different-durable-requests | mongodb | 1,563.2 [1,562.8, 1,624.3] | 25,470.4 [24,678.7, 25,678.5] |

The reversal between GoMap profiles is present in all three repetitions: the different-durable-requests run is faster than the relaxed run, while its load is slower. In the relaxed class, GoMap UPDATE p95 spans 77,759–88,063 us, compared with MongoDB 707–1,024 us. These short runs retain the observed stalls; the campaign does not diagnose their cause or establish a monotonic cost of durability.

## Mongo gateway client shape

200,000 prebuilt gateway-shaped BSON documents, batch 1,000, 16 insert producers, two secondary indexes (`email` unique, `city`), full-document point responses and 100,000 reads per read kind at 16 workers. GoMap uses `command_wal_relaxed`, BSON, compressed data/index roots, settled reads and no maintenance. MongoDB uses `driver-command-raw`; pool maximum 16, minimum 0, maximum connecting 16 on both arms.

The five GoMap rows compare client/protocol costs with storage configuration held fixed. `native-wire-tcp` uses GoMap's own protocol. MongoDB is context for the same `driver-command-raw` path; the other GoMap modes are not identical client boundaries. Direct API and unacknowledged `w:0` modes were not selected in this rerun; #5408's unsupported-mode diagnostics are historical rather than new observations.

| Target | Client mode | Load docs/s median [min, max] | `_id` r16 ops/s median [min, max] | `email` r16 ops/s median [min, max] | `_id` p95 us median | `email` p95 us median |
| --- | --- | --- | --- | --- | --- | --- |
| gomap | driver | 250,198 [249,437, 250,541] | 111,258 [101,922, 113,171] | 48,395 [46,858, 48,671] | 225 | 939 |
| gomap | driver-command | 274,411 [268,699, 281,179] | 110,938 [109,014, 111,507] | 47,862 [47,234, 48,702] | 223 | 950 |
| gomap | driver-command-raw | 283,051 [279,052, 287,180] | 110,621 [102,469, 111,182] | 47,670 [46,615, 48,618] | 228 | 949 |
| gomap | raw-wire-tcp | 277,273 [271,046, 281,310] | 215,791 [169,101, 216,509] | 57,611 [57,566, 58,923] | 114 | 905 |
| gomap | native-wire-tcp | 290,320 [290,072, 294,607] | 111,784 [110,938, 113,199] | 48,573 [48,536, 48,674] | 220 | 937 |
| mongodb | driver-command-raw | 279,777 [277,401, 293,380] | 41,273 [38,712, 41,446] | 36,222 [35,026, 37,807] | 660 | 764 |

## Mongo API full sweep

100,000 prebuilt BSON documents; batch 1,000; 16 insert producers; `driver-command-raw` on both arms; GoMap relaxed profile, compressed roots and settled full-document reads. Displayed secondary-index counts 0/1/2 each also include the `age_1` range index; one adds unique `email`, two adds `city`. Sequential `_id` and applicable `email` reads each use 100,000 operations; range reads and updates each use 10,000. Concurrent sweeps use 1/16/32 readers or writers, with 100,000 `_id` reads or 10,000 range reads/updates per phase. Deletes are disabled.

GoMap checkpoint and MongoDB compact are untimed and are different maintenance operations. Physical storage is stopped filesystem allocation (`stat.st_blocks * 512`) of the entire owned database after the complete sweep, including redo/WAL and persistent value logs. It excludes binaries, caches, Docker images and container runtime overhead. It is neither WAL-excluded storage nor insert-only footprint. Unhinted query names and metadata checks do not prove physical index activation.

| Displayed indexes | GoMap load docs/s | MongoDB load docs/s | GoMap `_id` r16 ops/s | MongoDB `_id` r16 ops/s | GoMap allocated MiB | MongoDB allocated MiB |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | 365,930 | 552,634 | 122,135 | 39,889 | 45.82 | 207.39 |
| 1 | 284,862 | 275,701 | 121,913 | 40,928 | 46.54 | 210.07 |
| 2 | 249,161 | 231,054 | 123,523 | 41,434 | 46.81 | 210.33 |

The full per-phase distribution table below retains throughput and p95/p99 ranges for every nonempty phase, including sequential/range/update phases and all concurrency settings. Throughput uses the whole phase wall-clock duration. Load throughput counts documents, while load latency quantiles measure batch driver calls rather than individual documents. Range samples include complete cursor/result consumption.

## Relative performance versus the Mac run in #5408

The historical denominator is [the summary at #5408 head `bd68e477073a9df5bd291690e6675c888140f3ab`](https://github.com/snissn/gomap/blob/bd68e477073a9df5bd291690e6675c888140f3ab/docs/benchmarks/readme_ycsb_mongodb_macos_2026-11-12/summary.json), SHA-256 `f248c85624af45745d3f17e40299f6e3eedc853377e434bf5a8ac5d2fcde06b9`. Its exact bytes and source identity are copied under `setup/comparison-reference/` in the raw archive.

Ratios below normalize GoMap against MongoDB within each host. A ratio above 1 means GoMap has higher throughput. Relative change is `(Linux ratio / Mac ratio - 1) × 100%`: if both systems slow equally, it is zero. YCSB uses the median of matched pair ratios; macro rows use the ratio of the separate three-run medians. Do not mix these estimators.

This is not a controlled before/after test of #5410. Product source changed from `2590d1e9e443c3f968303ce91b3c82933a714f12` to the frozen source above, and hardware, OS, architecture, filesystem, container user and Docker topology also changed. The original Mac ran native GoMap against MongoDB inside a Docker Desktop Linux VM; this server runs both on Linux. The same workload/client/MongoDB version does not isolate a causal software speedup. The Mac journaled group is incomplete, so it cannot supply a durable relative-change baseline.

| Workload | Mac GoMap/Mongo | Linux GoMap/Mongo | Change in relative throughput |
| --- | --- | --- | --- |
| YCSB relaxed | 3.128x | 0.647x | -79.3% |
| Client shape raw command: load | 0.607x | 1.012x | +66.7% |
| Client shape raw command: _id r16 | 4.705x | 2.680x | -43.0% |
| Client shape raw command: email r16 | 3.693x | 1.316x | -64.4% |
| Full sweep 0 displayed indexes: load | 0.947x | 0.662x | -30.1% |
| Full sweep 0 displayed indexes: _id r16 | 4.240x | 3.062x | -27.8% |
| Full sweep 1 displayed indexes: load | 0.934x | 1.033x | +10.6% |
| Full sweep 1 displayed indexes: _id r16 | 5.648x | 2.979x | -47.3% |
| Full sweep 2 displayed indexes: load | 0.793x | 1.078x | +35.9% |
| Full sweep 2 displayed indexes: _id r16 | 4.202x | 2.981x | -29.1% |

## Attempt accounting and limitations

| Suite | Planned | Valid | Invalid |
| --- | --- | --- | --- |
| ycsb | 12 | 12 | 0 |
| shape | 18 | 18 | 0 |
| sweep | 18 | 18 | 0 |

All valid selected operation-error counters are zero. Complete public typed-state censuses and independent reopen checks are required. For macrobenchmarks, the original producer's deferred Close return remains **unobserved**; independent reopen cannot recover that discarded return. MongoDB also has a pre-close/post-reopen digest equality check. These graceful lifecycle checks do not establish crash, power-loss or durability-semantics equivalence. Raw MongoDB index metadata may be unavailable on GoMap; the report does not treat that as physical index proof.

The harness PR's first CI attempt failed the unchanged `TestLeafGenerationGC_LifetimeGateLockOrderStress` with a nonconvergent capture. Its original log and bounded affected-gate retry are retained as preparation evidence, separate from benchmark attempts; no runtime safeguards were weakened.

## YCSB individual valid samples

| Class | Target | Rep | Phase/op | Count | Ops/s | Avg us | p95 us | p99 us | Max us |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| different-durable-requests | gomap | 1 | load/TOTAL | 100000 | 4,920.8 | 3,233.0 | 3,397.0 | 3,827.0 | 431,615.0 |
| different-durable-requests | gomap | 1 | run/TOTAL | 10000 | 51,716.1 | 274.0 | 852.0 | 3,285.0 | 5,519.0 |
| different-durable-requests | gomap | 1 | run/READ | 9506 | 49,179.0 | 148.0 | 309.0 | 478.0 | 1,197.0 |
| different-durable-requests | gomap | 1 | run/UPDATE | 494 | 2,604.9 | 2,694.0 | 3,743.0 | 4,675.0 | 5,519.0 |
| different-durable-requests | mongodb | 1 | load/TOTAL | 100000 | 1,624.3 | 9,844.0 | 10,311.0 | 10,679.0 | 579,071.0 |
| different-durable-requests | mongodb | 1 | run/TOTAL | 10000 | 25,470.4 | 587.0 | 1,593.0 | 5,839.0 | 7,315.0 |
| different-durable-requests | mongodb | 1 | run/READ | 9474 | 24,142.3 | 411.0 | 755.0 | 968.0 | 1,767.0 |
| different-durable-requests | mongodb | 1 | run/UPDATE | 526 | 1,355.2 | 3,772.0 | 6,651.0 | 7,007.0 | 7,315.0 |
| different-durable-requests | gomap | 2 | load/TOTAL | 100000 | 4,946.8 | 3,217.0 | 3,409.0 | 3,667.0 | 424,191.0 |
| different-durable-requests | gomap | 2 | run/TOTAL | 10000 | 51,890.6 | 276.0 | 1,581.0 | 3,245.0 | 4,991.0 |
| different-durable-requests | gomap | 2 | run/READ | 9489 | 49,230.5 | 147.0 | 300.0 | 447.0 | 1,444.0 |
| different-durable-requests | gomap | 2 | run/UPDATE | 511 | 2,687.5 | 2,674.0 | 3,765.0 | 4,061.0 | 4,991.0 |
| different-durable-requests | mongodb | 2 | load/TOTAL | 100000 | 1,562.8 | 10,223.0 | 10,351.0 | 10,847.0 | 559,103.0 |
| different-durable-requests | mongodb | 2 | run/TOTAL | 10000 | 25,678.5 | 582.0 | 1,447.0 | 5,883.0 | 7,879.0 |
| different-durable-requests | mongodb | 2 | run/READ | 9497 | 24,392.2 | 409.0 | 717.0 | 935.0 | 2,307.0 |
| different-durable-requests | mongodb | 2 | run/UPDATE | 503 | 1,305.1 | 3,855.0 | 6,727.0 | 7,103.0 | 7,879.0 |
| different-durable-requests | gomap | 3 | load/TOTAL | 100000 | 4,935.7 | 3,224.0 | 3,401.0 | 3,649.0 | 428,287.0 |
| different-durable-requests | gomap | 3 | run/TOTAL | 10000 | 50,783.0 | 288.0 | 1,321.0 | 3,399.0 | 13,479.0 |
| different-durable-requests | gomap | 3 | run/READ | 9499 | 48,258.6 | 145.0 | 303.0 | 452.0 | 1,958.0 |
| different-durable-requests | gomap | 3 | run/UPDATE | 501 | 2,596.1 | 2,995.0 | 4,005.0 | 13,087.0 | 13,479.0 |
| different-durable-requests | mongodb | 3 | load/TOTAL | 100000 | 1,563.2 | 10,233.0 | 10,343.0 | 10,783.0 | 609,791.0 |
| different-durable-requests | mongodb | 3 | run/TOTAL | 10000 | 24,678.7 | 606.0 | 1,645.0 | 5,855.0 | 7,103.0 |
| different-durable-requests | mongodb | 3 | run/READ | 9469 | 23,373.5 | 426.0 | 778.0 | 1,004.0 | 2,073.0 |
| different-durable-requests | mongodb | 3 | run/UPDATE | 531 | 1,318.9 | 3,813.0 | 6,627.0 | 6,899.0 | 7,103.0 |
| relaxed | gomap | 1 | load/TOTAL | 100000 | 20,433.8 | 765.0 | 497.0 | 800.0 | 294,911.0 |
| relaxed | gomap | 1 | run/TOTAL | 10000 | 17,161.7 | 918.0 | 577.0 | 895.0 | 339,967.0 |
| relaxed | gomap | 1 | run/READ | 9494 | 16,292.1 | 228.0 | 537.0 | 765.0 | 1,543.0 |
| relaxed | gomap | 1 | run/UPDATE | 506 | 869.9 | 13,867.0 | 87,039.0 | 339,199.0 | 339,967.0 |
| relaxed | mongodb | 1 | load/TOTAL | 100000 | 39,434.5 | 376.0 | 593.0 | 865.0 | 493,311.0 |
| relaxed | mongodb | 1 | run/TOTAL | 10000 | 26,541.3 | 596.0 | 980.0 | 1,253.0 | 3,027.0 |
| relaxed | mongodb | 1 | run/READ | 9464 | 25,122.3 | 603.0 | 987.0 | 1,253.0 | 3,027.0 |
| relaxed | mongodb | 1 | run/UPDATE | 536 | 1,425.2 | 476.0 | 817.0 | 1,127.0 | 1,554.0 |
| relaxed | gomap | 2 | load/TOTAL | 100000 | 20,172.2 | 775.0 | 514.0 | 833.0 | 300,031.0 |
| relaxed | gomap | 2 | run/TOTAL | 10000 | 17,647.6 | 893.0 | 598.0 | 913.0 | 319,743.0 |
| relaxed | gomap | 2 | run/READ | 9478 | 16,718.9 | 231.0 | 558.0 | 799.0 | 1,510.0 |
| relaxed | gomap | 2 | run/UPDATE | 522 | 923.6 | 12,908.0 | 88,063.0 | 318,719.0 | 319,743.0 |
| relaxed | mongodb | 2 | load/TOTAL | 100000 | 41,065.1 | 373.0 | 571.0 | 793.0 | 489,983.0 |
| relaxed | mongodb | 2 | run/TOTAL | 10000 | 26,551.2 | 591.0 | 969.0 | 1,252.0 | 1,994.0 |
| relaxed | mongodb | 2 | run/READ | 9525 | 25,286.1 | 599.0 | 978.0 | 1,258.0 | 1,994.0 |
| relaxed | mongodb | 2 | run/UPDATE | 475 | 1,272.6 | 436.0 | 707.0 | 928.0 | 1,345.0 |
| relaxed | gomap | 3 | load/TOTAL | 100000 | 18,705.8 | 798.0 | 569.0 | 898.0 | 416,255.0 |
| relaxed | gomap | 3 | run/TOTAL | 10000 | 14,044.6 | 1,114.0 | 1,541.0 | 2,215.0 | 104,127.0 |
| relaxed | gomap | 3 | run/READ | 9449 | 13,273.3 | 786.0 | 1,464.0 | 1,777.0 | 2,647.0 |
| relaxed | gomap | 3 | run/UPDATE | 551 | 775.2 | 6,727.0 | 77,759.0 | 97,983.0 | 104,127.0 |
| relaxed | mongodb | 3 | load/TOTAL | 100000 | 39,585.7 | 386.0 | 561.0 | 769.0 | 502,015.0 |
| relaxed | mongodb | 3 | run/TOTAL | 10000 | 26,126.8 | 602.0 | 1,073.0 | 1,496.0 | 3,759.0 |
| relaxed | mongodb | 3 | run/READ | 9505 | 24,837.0 | 606.0 | 1,078.0 | 1,503.0 | 3,759.0 |
| relaxed | mongodb | 3 | run/UPDATE | 495 | 1,297.2 | 523.0 | 1,024.0 | 1,351.0 | 2,589.0 |

## Macro phase distributions

| Suite | Mode/indexes | Target | Phase | Ops/s median [min, max] | p95 us median [min, max] | p99 us median [min, max] |
| --- | --- | --- | --- | --- | --- | --- |
| shape | driver | gomap | load_insert_many | 250,198 [249,437, 250,541] | 66,800 [63,368, 66,823] | 75,007 [73,946, 77,268] |
| shape | driver | gomap | concurrent_id_find_one_r16 | 111,258 [101,922, 113,171] | 225 [221, 247] | 315 [305, 442] |
| shape | driver | gomap | concurrent_email_find_one_r16 | 48,395 [46,858, 48,671] | 939 [933, 973] | 1,359 [1,349, 1,389] |
| shape | driver-command | gomap | load_insert_many | 274,411 [268,699, 281,179] | 64,935 [61,592, 68,430] | 70,638 [68,242, 80,166] |
| shape | driver-command | gomap | concurrent_id_find_one_r16 | 110,938 [109,014, 111,507] | 223 [222, 225] | 322 [311, 323] |
| shape | driver-command | gomap | concurrent_email_find_one_r16 | 47,862 [47,234, 48,702] | 950 [933, 957] | 1,376 [1,334, 1,387] |
| shape | driver-command-raw | gomap | load_insert_many | 283,051 [279,052, 287,180] | 55,781 [53,495, 65,974] | 77,658 [69,588, 79,073] |
| shape | driver-command-raw | gomap | concurrent_id_find_one_r16 | 110,621 [102,469, 111,182] | 228 [225, 249] | 325 [316, 526] |
| shape | driver-command-raw | gomap | concurrent_email_find_one_r16 | 47,670 [46,615, 48,618] | 949 [934, 963] | 1,402 [1,359, 1,409] |
| shape | driver-command-raw | mongodb | load_insert_many | 279,777 [277,401, 293,380] | 78,223 [76,704, 79,348] | 104,001 [99,816, 107,189] |
| shape | driver-command-raw | mongodb | concurrent_id_find_one_r16 | 41,273 [38,712, 41,446] | 660 [655, 728] | 870 [869, 997] |
| shape | driver-command-raw | mongodb | concurrent_email_find_one_r16 | 36,222 [35,026, 37,807] | 764 [720, 794] | 1,039 [937, 1,059] |
| shape | native-wire-tcp | gomap | load_insert_many | 290,320 [290,072, 294,607] | 65,133 [58,668, 67,081] | 71,127 [68,527, 72,627] |
| shape | native-wire-tcp | gomap | concurrent_id_find_one_r16 | 111,784 [110,938, 113,199] | 220 [220, 224] | 305 [300, 317] |
| shape | native-wire-tcp | gomap | concurrent_email_find_one_r16 | 48,573 [48,536, 48,674] | 937 [933, 938] | 1,357 [1,354, 1,369] |
| shape | raw-wire-tcp | gomap | load_insert_many | 277,273 [271,046, 281,310] | 62,017 [57,488, 65,866] | 69,952 [68,813, 70,318] |
| shape | raw-wire-tcp | gomap | concurrent_id_find_one_r16 | 215,791 [169,101, 216,509] | 114 [110, 143] | 161 [145, 462] |
| shape | raw-wire-tcp | gomap | concurrent_email_find_one_r16 | 57,611 [57,566, 58,923] | 905 [881, 908] | 1,372 [1,344, 1,376] |
| sweep | indexes0 | gomap | load_insert_many | 365,930 [355,935, 372,853] | 43,693 [40,986, 46,381] | 47,966 [45,942, 48,325] |
| sweep | indexes0 | gomap | id_find_one | 14,976 [14,925, 14,991] | 80 [80, 81] | 94 [93, 98] |
| sweep | indexes0 | gomap | age_range_indexed_limit_10 | 14,558 [14,534, 15,128] | 77 [75, 78] | 86 [84, 88] |
| sweep | indexes0 | gomap | concurrent_age_range_indexed_limit_10_r1 | 14,941 [14,605, 14,985] | 76 [76, 79] | 85 [83, 93] |
| sweep | indexes0 | gomap | concurrent_age_range_indexed_limit_10_r16 | 37,462 [36,247, 37,559] | 1,262 [1,237, 1,360] | 2,007 [1,978, 2,068] |
| sweep | indexes0 | gomap | concurrent_age_range_indexed_limit_10_r32 | 37,417 [31,354, 37,870] | 1,779 [1,663, 2,252] | 2,477 [2,452, 3,188] |
| sweep | indexes0 | gomap | id_update_set | 10,932 [10,660, 10,975] | 98 [97, 102] | 112 [109, 116] |
| sweep | indexes0 | gomap | concurrent_id_find_one_r1 | 14,501 [14,462, 14,521] | 83 [82, 84] | 95 [94, 104] |
| sweep | indexes0 | gomap | concurrent_id_find_one_r16 | 122,135 [120,877, 123,389] | 200 [196, 202] | 296 [281, 316] |
| sweep | indexes0 | gomap | concurrent_id_find_one_r32 | 120,368 [106,871, 120,574] | 359 [357, 435] | 518 [502, 1,013] |
| sweep | indexes0 | gomap | concurrent_id_update_set_w1 | 11,833 [11,088, 11,848] | 96 [94, 115] | 108 [106, 151] |
| sweep | indexes0 | gomap | concurrent_id_update_set_w16 | 66,405 [65,638, 66,436] | 256 [252, 258] | 348 [310, 370] |
| sweep | indexes0 | gomap | concurrent_id_update_set_w32 | 65,510 [65,096, 66,035] | 477 [465, 505] | 673 [628, 824] |
| sweep | indexes0 | mongodb | load_insert_many | 552,634 [547,092, 566,577] | 42,830 [41,300, 44,300] | 47,500 [46,066, 50,205] |
| sweep | indexes0 | mongodb | id_find_one | 9,144 [8,961, 9,148] | 128 [128, 132] | 188 [188, 197] |
| sweep | indexes0 | mongodb | age_range_indexed_limit_10 | 7,082 [7,009, 7,169] | 157 [156, 162] | 195 [190, 197] |
| sweep | indexes0 | mongodb | concurrent_age_range_indexed_limit_10_r1 | 7,116 [7,025, 7,163] | 159 [155, 159] | 194 [185, 199] |
| sweep | indexes0 | mongodb | concurrent_age_range_indexed_limit_10_r16 | 29,970 [29,839, 30,198] | 863 [848, 870] | 1,097 [1,083, 1,101] |
| sweep | indexes0 | mongodb | concurrent_age_range_indexed_limit_10_r32 | 29,503 [29,137, 29,553] | 1,508 [1,506, 1,536] | 1,795 [1,785, 1,855] |
| sweep | indexes0 | mongodb | id_update_set | 8,142 [7,729, 8,161] | 140 [140, 157] | 157 [156, 237] |
| sweep | indexes0 | mongodb | concurrent_id_find_one_r1 | 9,012 [8,867, 9,050] | 130 [130, 132] | 208 [206, 209] |
| sweep | indexes0 | mongodb | concurrent_id_find_one_r16 | 39,889 [39,826, 41,260] | 697 [656, 701] | 985 [854, 994] |
| sweep | indexes0 | mongodb | concurrent_id_find_one_r32 | 40,396 [40,255, 41,660] | 1,166 [1,126, 1,181] | 1,473 [1,399, 1,488] |
| sweep | indexes0 | mongodb | concurrent_id_update_set_w1 | 7,717 [7,133, 7,897] | 150 [145, 206] | 181 [166, 307] |
| sweep | indexes0 | mongodb | concurrent_id_update_set_w16 | 36,727 [28,081, 40,054] | 733 [650, 1,074] | 938 [834, 1,424] |
| sweep | indexes0 | mongodb | concurrent_id_update_set_w32 | 36,659 [36,589, 39,518] | 1,239 [1,150, 1,257] | 1,478 [1,402, 1,521] |
| sweep | indexes1 | gomap | load_insert_many | 284,862 [283,723, 287,330] | 46,745 [46,294, 47,046] | 50,089 [48,997, 53,101] |
| sweep | indexes1 | gomap | id_find_one | 14,999 [14,716, 15,106] | 80 [79, 82] | 95 [93, 97] |
| sweep | indexes1 | gomap | email_find_one | 13,108 [12,799, 13,188] | 92 [90, 96] | 106 [101, 111] |
| sweep | indexes1 | gomap | age_range_indexed_limit_10 | 14,792 [14,640, 14,847] | 79 [77, 79] | 94 [86, 99] |
| sweep | indexes1 | gomap | concurrent_age_range_indexed_limit_10_r1 | 14,847 [13,757, 15,036] | 77 [75, 82] | 90 [88, 94] |
| sweep | indexes1 | gomap | concurrent_age_range_indexed_limit_10_r16 | 37,177 [36,058, 37,283] | 1,245 [1,223, 1,265] | 1,994 [1,980, 2,001] |
| sweep | indexes1 | gomap | concurrent_age_range_indexed_limit_10_r32 | 37,152 [35,689, 37,308] | 1,783 [1,745, 1,888] | 2,521 [2,502, 2,538] |
| sweep | indexes1 | gomap | id_update_set | 10,718 [10,273, 10,736] | 101 [98, 112] | 114 [112, 144] |
| sweep | indexes1 | gomap | concurrent_id_find_one_r1 | 14,689 [14,652, 14,741] | 81 [81, 81] | 92 [90, 92] |
| sweep | indexes1 | gomap | concurrent_id_find_one_r16 | 121,913 [121,005, 124,987] | 200 [196, 204] | 298 [275, 312] |
| sweep | indexes1 | gomap | concurrent_id_find_one_r32 | 119,185 [117,876, 121,217] | 362 [351, 366] | 527 [513, 542] |
| sweep | indexes1 | gomap | concurrent_id_update_set_w1 | 11,551 [11,095, 11,712] | 98 [97, 111] | 113 [109, 146] |
| sweep | indexes1 | gomap | concurrent_id_update_set_w16 | 67,393 [66,891, 67,541] | 255 [253, 255] | 321 [314, 340] |
| sweep | indexes1 | gomap | concurrent_id_update_set_w32 | 61,826 [61,209, 62,224] | 483 [471, 539] | 779 [705, 836] |
| sweep | indexes1 | mongodb | load_insert_many | 275,701 [269,206, 306,254] | 81,262 [67,728, 83,565] | 96,389 [75,735, 110,120] |
| sweep | indexes1 | mongodb | id_find_one | 9,051 [8,985, 9,123] | 132 [129, 134] | 204 [193, 206] |
| sweep | indexes1 | mongodb | email_find_one | 8,238 [8,182, 8,295] | 143 [140, 144] | 220 [200, 222] |
| sweep | indexes1 | mongodb | age_range_indexed_limit_10 | 7,142 [7,103, 7,174] | 156 [156, 157] | 190 [187, 191] |
| sweep | indexes1 | mongodb | concurrent_age_range_indexed_limit_10_r1 | 7,094 [6,776, 7,165] | 159 [156, 178] | 197 [183, 265] |
| sweep | indexes1 | mongodb | concurrent_age_range_indexed_limit_10_r16 | 29,158 [28,419, 29,720] | 905 [871, 923] | 1,144 [1,118, 1,161] |
| sweep | indexes1 | mongodb | concurrent_age_range_indexed_limit_10_r32 | 29,504 [29,223, 29,641] | 1,497 [1,484, 1,506] | 1,800 [1,768, 1,805] |
| sweep | indexes1 | mongodb | id_update_set | 8,100 [7,913, 8,111] | 141 [141, 144] | 162 [159, 167] |
| sweep | indexes1 | mongodb | concurrent_id_find_one_r1 | 9,018 [8,864, 9,073] | 129 [128, 136] | 201 [200, 214] |
| sweep | indexes1 | mongodb | concurrent_id_find_one_r16 | 40,928 [39,714, 41,244] | 666 [658, 697] | 892 [861, 989] |
| sweep | indexes1 | mongodb | concurrent_id_find_one_r32 | 41,491 [41,396, 41,694] | 1,127 [1,120, 1,128] | 1,400 [1,385, 1,420] |
| sweep | indexes1 | mongodb | concurrent_id_update_set_w1 | 7,857 [7,310, 7,899] | 147 [146, 213] | 171 [169, 278] |
| sweep | indexes1 | mongodb | concurrent_id_update_set_w16 | 39,786 [39,453, 39,803] | 652 [650, 667] | 841 [814, 882] |
| sweep | indexes1 | mongodb | concurrent_id_update_set_w32 | 38,796 [29,917, 39,547] | 1,157 [1,145, 1,705] | 1,445 [1,422, 2,180] |
| sweep | indexes2 | gomap | load_insert_many | 249,161 [238,106, 253,900] | 50,845 [48,820, 53,045] | 54,000 [51,057, 56,508] |
| sweep | indexes2 | gomap | id_find_one | 15,052 [14,511, 15,065] | 79 [79, 85] | 93 [92, 109] |
| sweep | indexes2 | gomap | email_find_one | 12,974 [12,826, 13,023] | 92 [92, 95] | 107 [106, 109] |
| sweep | indexes2 | gomap | age_range_indexed_limit_10 | 14,875 [13,958, 15,088] | 76 [75, 83] | 85 [84, 93] |
| sweep | indexes2 | gomap | concurrent_age_range_indexed_limit_10_r1 | 14,850 [14,541, 15,203] | 77 [75, 79] | 87 [84, 90] |
| sweep | indexes2 | gomap | concurrent_age_range_indexed_limit_10_r16 | 37,446 [36,524, 37,643] | 1,238 [1,204, 1,278] | 2,001 [1,954, 2,048] |
| sweep | indexes2 | gomap | concurrent_age_range_indexed_limit_10_r32 | 37,434 [36,409, 37,681] | 1,779 [1,711, 1,834] | 2,489 [2,451, 2,495] |
| sweep | indexes2 | gomap | id_update_set | 10,778 [10,570, 10,984] | 99 [96, 102] | 110 [110, 114] |
| sweep | indexes2 | gomap | concurrent_id_find_one_r1 | 14,526 [14,408, 14,602] | 82 [82, 85] | 93 [93, 106] |
| sweep | indexes2 | gomap | concurrent_id_find_one_r16 | 123,523 [111,697, 123,800] | 201 [197, 218] | 283 [280, 377] |
| sweep | indexes2 | gomap | concurrent_id_find_one_r32 | 117,952 [117,659, 120,045] | 366 [359, 367] | 562 [512, 572] |
| sweep | indexes2 | gomap | concurrent_id_update_set_w1 | 11,618 [11,504, 11,711] | 99 [97, 99] | 112 [108, 113] |
| sweep | indexes2 | gomap | concurrent_id_update_set_w16 | 67,257 [65,161, 67,826] | 261 [256, 266] | 326 [325, 352] |
| sweep | indexes2 | gomap | concurrent_id_update_set_w32 | 61,228 [58,983, 62,753] | 484 [476, 491] | 826 [804, 941] |
| sweep | indexes2 | mongodb | load_insert_many | 231,054 [210,369, 235,958] | 93,793 [91,466, 107,404] | 105,690 [102,641, 122,337] |
| sweep | indexes2 | mongodb | id_find_one | 9,092 [9,030, 9,127] | 129 [128, 131] | 190 [189, 193] |
| sweep | indexes2 | mongodb | email_find_one | 8,242 [8,226, 8,244] | 142 [141, 142] | 218 [209, 222] |
| sweep | indexes2 | mongodb | age_range_indexed_limit_10 | 7,058 [7,006, 7,076] | 158 [157, 161] | 196 [192, 199] |
| sweep | indexes2 | mongodb | concurrent_age_range_indexed_limit_10_r1 | 7,080 [6,836, 7,084] | 158 [156, 182] | 194 [192, 268] |
| sweep | indexes2 | mongodb | concurrent_age_range_indexed_limit_10_r16 | 28,978 [25,838, 29,678] | 918 [878, 1,095] | 1,193 [1,110, 1,795] |
| sweep | indexes2 | mongodb | concurrent_age_range_indexed_limit_10_r32 | 26,765 [24,945, 29,304] | 1,850 [1,524, 1,916] | 2,383 [1,849, 2,549] |
| sweep | indexes2 | mongodb | id_update_set | 8,044 [7,813, 8,109] | 145 [143, 167] | 175 [160, 228] |
| sweep | indexes2 | mongodb | concurrent_id_find_one_r1 | 9,043 [8,941, 9,072] | 129 [128, 132] | 203 [201, 212] |
| sweep | indexes2 | mongodb | concurrent_id_find_one_r16 | 41,434 [39,372, 41,677] | 656 [649, 708] | 857 [847, 1,041] |
| sweep | indexes2 | mongodb | concurrent_id_find_one_r32 | 40,666 [39,951, 41,307] | 1,155 [1,134, 1,184] | 1,437 [1,433, 1,488] |
| sweep | indexes2 | mongodb | concurrent_id_update_set_w1 | 7,867 [7,493, 7,913] | 146 [145, 161] | 172 [170, 255] |
| sweep | indexes2 | mongodb | concurrent_id_update_set_w16 | 39,870 [38,409, 40,083] | 653 [639, 690] | 842 [783, 898] |
| sweep | indexes2 | mongodb | concurrent_id_update_set_w32 | 39,230 [36,656, 39,512] | 1,140 [1,138, 1,265] | 1,425 [1,353, 1,646] |

## Evidence and reproduction

[Frozen source/host manifest](readme_ycsb_mongodb_server185_2026-11-13/freeze.json), [validated sample/distribution summary](readme_ycsb_mongodb_server185_2026-11-13/summary.json), [raw file index](readme_ycsb_mongodb_server185_2026-11-13/raw-index.json), and [raw archive](readme_ycsb_mongodb_server185_2026-11-13/raw.tar.gz). `summary.json` contains every selected per-run original metric and the complete attempt ledger. The archive contains all stdout/stderr, exact argv/environment, original Close receipts, lifecycle identities, full censuses, allocation file lists, smoke checks, build receipts, landing readback and preparation failures; it excludes database directories, binaries, source checkouts and shared caches.

Verify the checked-in archive without running databases:

```sh
python3 docs/benchmarks/readme_ycsb_mongodb_server185_2026-11-13/verify_archive.py \
  --directory docs/benchmarks/readme_ycsb_mongodb_server185_2026-11-13
```

The landed [portable recipe](../../scripts/readme_mongodb_bench/README.md) documents exact build, freeze, smoke, collection, aggregation and archiving commands. Reproduce in a **new** task root with fresh DBs; do not rerun or overwrite this campaign. The frozen acceptance code is also retained under `recipe/` in the raw archive. The prior Mac dataset remains in [PR #5408](https://github.com/snissn/gomap/pull/5408); its source-bound summary is the comparison denominator here.
