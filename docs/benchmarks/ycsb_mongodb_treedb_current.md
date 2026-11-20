# YCSB MongoDB / TreeDB Benchmark Status

This page is the current entry point for external `go-ycsb` comparisons across:

- MongoDB through the `go-ycsb mongodb` binding.
- `treedb-native` through the `go-ycsb treedb-native` binding.
- TreeDB Mongo gateway through the `go-ycsb mongodb` binding.

It distinguishes the retained October comparison from historical reports and
records the remaining qualification obligations.

## November 20 WAL placement on ext4

On host111 at `5fc6200`, placing only `maindb/wal` on a second ext4 journal
brought sustained client UPDATE p99.9 below 0.9 ms in all nine scored, uninstrumented
GoMap runs with that layout. The standard layout kept the command WAL, document value log and
index on one ext4 filesystem. In that layout a document fsync could hold a
journal transaction that a foreground WAL append needed. Both layouts used
the same NVMe and GoMap executable. [Kernel attribution](https://github.com/snissn/gomap/issues/5528#issuecomment-7029008688)

| Sustained comparison, three pairs per row | Reference UPDATE p99.9 (ms) | GoMap with separate WAL (ms) |
| --- | --- | --- |
| Native GoMap, standard layout | 3.269 / 10.183 / 13.383 | 0.797 / 0.814 / 0.790 |
| Docker GoMap, standard layout | 3.241 / 3.141 / 3.453 | 0.806 / 0.827 / 0.811 |
| Independent Docker, MongoDB 8.2.7 | 0.491 / 0.504 / 0.507 | 0.851 / 0.801 / 0.899 |

Each fresh store loaded 100,000 BSON documents, then served 6 million uniform
95% READ / 5% UPDATE operations with 16 clients, 16 CPUs, 4 GiB memory and
zero swap. GoMap used `command_wal_relaxed` and `w:1`. All cells passed typed
readback, checked shutdown and reopen. Separate observations counted 44
completed pressure-driven maintenance cuts during MIXED on the standard
layout and 42 with the separate WAL. [Native results](https://github.com/snissn/gomap/issues/5528#issuecomment-7037456507) · [Maintenance evidence](https://github.com/snissn/gomap/issues/5528#issuecomment-7038085604)

The matched Docker p99.99 increased in every pair, from 23.551–37.375 ms to
54.847–86.271 ms. Against MongoDB the separate-WAL GoMap runs served 7.92–9.34%
more mixed operations per second, took 1.628× as long to load by paired
median, and recorded a worst UPDATE of 315.647–435.199 ms per run. MongoDB's
maxima were 8.179–11.407 ms. The cumulative client histograms
first reported those GoMap maxima in the 20–30 second MIXED interval. [Matched Docker results](https://github.com/snissn/gomap/issues/5528#issuecomment-7037958435) · [Independent Docker results](https://github.com/snissn/gomap/issues/5528#issuecomment-7038502182)

The tested WAL filesystem used a 1.5 GiB sparse image, a direct-I/O loop
device with 4 KiB sectors, an eagerly initialized 16 MiB ext4 journal, 128 KiB
per inode and online discard. The native runs sampled 410–427 MiB of live
backing allocation and 20.31 MiB after shutdown. Counting the backing image
once and all store files outside the WAL mount, the largest paired stopped-space ratios
were 1.0470× standard for native and 1.0457× for Docker. The same layout cost 16.03–16.07% more physical space on the roughly
133 MB stores in the matched 10,000-operation runs. [Short comparisons and storage costs](https://github.com/snissn/gomap/issues/5528#issuecomment-7038980978)

For a new test store, place the separate filesystem at `maindb/wal` before
the first writable Open and keep it there through checked Close. The service
must verify the expected store and WAL filesystem and directory identities
at every writable Open. An empty directory at the intended mount point can
allow a successful reopen at an earlier WAL frontier. To move an existing store to this layout,
back up the stopped store and its original WAL together, verify that backup,
and restore with both destination filesystems present. Verify the restored
mount identities, typed readback and checked Close before using the new store. [Backup and restore](../../TreeDB/docs/spec/backup-restore.md) · [Recovery, held readers and retirement](https://github.com/snissn/gomap/issues/5528#issuecomment-7037834046)

## November 19 captured-publication result

On host111, the uninstrumented 95% READ / 5% UPDATE workload used 16 clients
and 100,000 BSON documents. The release binaries came from `84c1e33a` and its
integrated WAL predecessor `f3f3ee12`. Both 1M UPDATE p99.9 comparisons passed
the 30.0475 ms screen, improving from 60.095 to 2.597 ms and from 61.599 to
13.487 ms. The ceiling was half the lower matched baseline.

| Six million operations per run | Reference UPDATE p99.9 (ms) | Candidate UPDATE p99.9 (ms) |
| --- | --- | --- |
| Native GoMap A/B, pairs 1–3 | 79.103 / 83.775 / 62.591 | 4.203 / 23.487 / 5.171 |
| Docker, MongoDB 8.2.7 vs. GoMap, pairs 1–3 | 0.499 / 0.501 / 0.497 | 3.829 / 3.539 / 3.955 |

Native median paired UPDATE p99.9 fell 91.74%. Native UPDATE p99 fell 24.09%
and throughput rose 27.06% on the same long pairs. All three Docker GoMap
p99.9 observations cleared the 5 ms comparison limit. GoMap exceeded MongoDB
throughput in all six long and short pairs. All 24 ordinary cells completed
checked shutdown and a full typed reopen. All native READ, LOAD, stopped
storage, memory and whole-lifecycle ratios stayed within the 10% guard.

The earlier release recorded 21.711 ms in one Docker run. Along with the
23.487 ms native result above, this illustrates the remaining variation across
runs. The release preserves the foreground improvement while the remaining
individual stalls have a separate investigation.
[Release and physical maintenance receipts](https://github.com/snissn/gomap/issues/5467)
· [Remaining client stalls](https://github.com/snissn/gomap/issues/5485)

## November 18 sustained UPDATE comparison

A relaxed standalone BSON candidate removed most UPDATE stalls and exceeded the
gateway process memory guard. Table entries are individual six-million-operation
runs on host111. Each starts from 100,000 records and uses 16 clients issuing
95% full-document READs and 5% single-field UPDATEs. The two native binaries
were built from main `554dab30` and candidate `08d7290f`. The long-run decision
uses about 300,000 UPDATEs per run. [Exact source, storage, resource and lifecycle records](https://github.com/snissn/gomap/issues/5467#issuecomment-6995333936)

| Native mixed measurement | Matched main | Candidate |
| --- | --- | --- |
| UPDATE p99, ms | 0.692 / 0.646 / 0.670 | 0.497 / 0.499 / 0.500 |
| UPDATE p99.9, ms | 147.071 / 62.943 / 122.367 | 0.755 / 0.782 / 0.788 |
| UPDATE p99.99, ms | 544.767 / 182.783 / 274.687 | 203.007 / 243.071 / 271.871 |
| UPDATE maximum, ms | 690.687 / 195.455 / 1392.639 | 616.959 / 1325.055 / 1972.223 |
| READ p99.9, ms | 0.514 / 0.509 / 0.522 | 0.461 / 0.467 / 0.469 |
| Throughput, ops/s | 122,697 / 136,265 / 126,168 | 180,055 / 176,486 / 172,010 |
| Gateway high-water memory through mixed end, MiB | 435.6 / 431.7 / 380.7 | 485.7 / 482.4 / 504.5 |

The counterbalanced 1M screen measured UPDATE p99.9 of 119.167 and 118.783 ms
on main and 0.751 and 0.771 ms on the candidate, against a 35.0395 ms ceiling.
The native sustained median paired p99.9 reduction was 99.36%. The candidate's
three Docker p99.9 measurements were 0.842 / 0.805 / 0.827 ms, alongside MongoDB
8.2.7 at 0.496 / 0.498 / 0.502 ms. GoMap had higher throughput in all six short
and long Docker comparisons. The original checked Close and full typed reopen
passed in all 24 cells. Separate observers counted 122 and 43 natural physical
publications on main and candidate. [Original product comparison](https://github.com/snissn/gomap/issues/5467#issuecomment-6995333936)

The candidate moved medium BSON values into index pages. Gateway high-water
memory grew in every native pair, exceeding the existing 10% concern threshold.
A separate joined trace identified two groups of 16 UPDATEs, one during a
checkpoint seal and another during WAL append as deferred cleanup started.
Its maximum was 222 ms. The 617–1,972 ms maxima in the ordinary candidate
measurements still lack an exact cause. [Process memory and alternative placement](https://github.com/snissn/gomap/issues/5467#issuecomment-6995781695)
· [Client incidents](https://github.com/snissn/gomap/issues/5467#issuecomment-6995614094)

A second candidate kept medium documents in the value log and released two
foreground locks during maintenance. Fresh 1M pairs measured 151.423 → 4.207 ms
and 78.591 → 2.419 ms against the same 35.0395 ms ceiling. Process high-water
memory through mixed completion fell from 449.0 to 415.4 MiB and from 435.9
to 420.8 MiB. The closed-stream verifier separately took 73% longer to complete
cleanup even as it reduced foreground delay. [Second stock comparison](https://github.com/snissn/gomap/issues/5467#issuecomment-6996121100)
· [Closed-stream cost](https://github.com/snissn/gomap/issues/5482#issuecomment-6996084351)

Across three native 6M pairs this pointer candidate reduced UPDATE p99.9
from 93.631 / 116.543 / 77.567 ms to 3.575 / 3.523 / 13.119 ms. Maximum
UPDATE latency was 63.743 / 37.023 / 85.503 ms on the candidate. Long-run
peak memory ratios were 1.005 / 0.999 / 1.018. The three candidate Docker
p99.9 values were 1.378 / 3.915 / 4.311 ms. All six Docker throughput
comparisons favored GoMap. Two of three short-run LOAD times increased by
more than 10%. No long-run LOAD time crossed that threshold. A separate
observer confirmed 120 / 44 natural main / candidate cuts. The stream
cleanup cost keeps this revision out of production. [Sustained runs and disposition](https://github.com/snissn/gomap/issues/5467#issuecomment-6996418894)

Restoring the original closed-stream cleanup retained the UPDATE benefit on
main `679c3647`. Two matched 1M runs went from 61.919 to 3.211 ms and
66.623 to 3.581 ms. Both cleared the binding 30.9595 ms screen. Throughput
rose 27–28%, and the gateway used less peak memory in each pair. The release
comparison and any later qualifications are tracked on the production PR.
[Matched screen](https://github.com/snissn/gomap/issues/5467#issuecomment-6996506527)
· [Production qualification](https://github.com/snissn/gomap/pull/5481)

## Final-source MongoDB comparison

The [final-source report](ycsb_mongodb_gomap_final_2026-10-30.md) records twelve
valid samples on GoMap `8bea490ad5a2d89a14f02f428b4283579a6dd864` (landed
as the identical tree in #5348) versus MongoDB 8.2.7. All selected operation
errors are zero. Three fresh pairs per class give these run medians:

| ACK class | MongoDB ops/s | GoMap ops/s | Median paired GoMap/Mongo |
| --- | ---: | ---: | ---: |
| Relaxed, both `w:1`, `j` absent | 42,753.6 | 59,245.2 | 1.372250 (+37.23%) |
| Durable-B, differing timed requests below | 19,879.2 | 11,766.6 | 0.591905 (−40.81%) |

Durable-B times MongoDB `w:1,j:true` against GoMap `command_wal_durable`
with `w:1`, `j` absent, using separate scoped ACK/sync/crash controls. A common
load/census and public same-owner `j:true` sentinel are outside timing. This is
not a same-request `j:true` ingestion result or complete MongoDB failure-semantics
certification. The report retains the earlier strict-load timeout and every
failed attempt. These short 10k runs, asymmetric resource caps and cache-warming
census do not establish sustained performance or isolate COW's contribution.

The [checked-in evidence](ycsb_5335_final_evidence/) supplies all rates, tails,
Close/maintenance/runtime records and a verified compact raw archive/index.
Thirteen actual attempts include one rejected observer-era sample; twelve valid
samples are selected explicitly. The parent
[#5225 performance objective](https://github.com/snissn/gomap/issues/5225)
and separate #5238/#5241 outcomes remain open. Historical and provisional rows
below remain separate evidence, never current denominators.

## October 30 actual-MongoDB comparison: provisional, qualification open

Use [the retained October 30 report](ycsb_mongodb_gomap_2026-10-30.md) for the
actual MongoDB 8.2.7 versus repaired GoMap public Mongo BSON comparisons. It
includes every completed relaxed and separately labeled durable pair, the failed
strict-journaled load, per-operation latency, original checked Close and lifecycle
scope, source/ELF identities, resource limits and indexed raw phase evidence.

These measurements used provisional GoMap `802f7cfc...`; they are not final-head
qualification. All nine complete pairs show lower GoMap throughput. The later
final-source harness and repairs have landed; the report above supplies their
separate measurements. Older failed gates and the parent performance
objective remain retained. These provisional rows are not relabeled as final.
The June figures below are historical context and are not an October denominator.

## 2026-10 COW JSON correctness follow-up

A later native-wire JSON load on `d879a302fd8dbb7617bd1d298a31488d1a6f30cd`
failed across durable, relaxed and no-WAL COW profiles; those incomplete runs
are not valid throughput evidence. The source-bound reproduction, individual
error counts and retained outputs are tracked in [#5333](https://github.com/snissn/gomap/issues/5333).
The original client can report a successful process exit despite operation or
close errors, so acceptance must check operation counts and an independently
observed close result. The shared left-edge separator repair is covered by a
focused zipper regression and public COW lifecycle tests. The broader matched
JSON/BSON and native-wire/Mongo comparison remains owned by
[#5335](https://github.com/snissn/gomap/issues/5335); the historical reports
below are not evidence of its outcome or of an isolated COW speedup.

## Historical June Canonical Report

Use `docs/benchmarks/ycsb_latest_main_2026-06-03.md` as the historical June
checked-in external YCSB evidence for MongoDB, TreeDB nativewire, and TreeDB
Mongo gateway.

That report covers the intended public TreeDB profile surface:

- `command_wal_durable`
- `command_wal_relaxed`
- `bench`, as the explicit no-WAL benchmark-only ceiling

It records zero YCSB operation errors, uses three run repeats per target or
profile, and refreshes the June 2 post-update-path-stack numbers on latest
`origin/main` commit `d7407b81cc5712374ca8c1588cfb05f6f7d8490d`. Note that
this rerun used a temporary external `go-ycsb` compatibility patch so the
`treedb-native` binding accepts TreeDB `collection_meta` version 3.

For the #2026 publication/readability closeout, use
`docs/benchmarks/nativewire_ycsb_closeout_2026-06-30.md` as the #2026
nativewire load-only closeout evidence refreshed on current `origin/main`.
That report uses the compatible external `go-ycsb` branch for
`collection_meta` version 5, runs 100k and 1M `treedb-native` loads on commit
`61910b8eac108c5f2c35f07a374879d1fb2dc5c8`, and records zero `INSERT_ERROR`
counts. It includes the later Raft snapshot/route-preflight merges, but does
not replace the full comparison matrix above. The follow-up #3374 diagnostic
report, `docs/benchmarks/nativewire_ycsb_insert_error_classification_2026-06-30.md`,
classifies the previously invalid intermediate 1M `INSERT_ERROR` artifact with
a current-head rerun using client-side error logging. The merged PR #3385
reconciliation combines those reports with the deterministic local
publication/readability tests and the #3374 classification, and it is
sufficient to close #2026 as the local storage-publication/readability tracker.
Distributed HA, Raft read-index/snapshots/rejoin, and routing/fanout remain out
of scope for #2026 and are tracked by #3044, #3045, and #3046.

## Historical June Headline

Run rows use the median total-throughput repeat from the June 3 HST / June 4
UTC latest-main report.

| target | profile | load ops/sec | run ops/sec | run avg us | run p99 us | run max us | errors |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| MongoDB | baseline | 38,755.4 | 26,494.1 | 595.0 | 1,275.0 | 2,389.0 | 0 |
| TreeDB nativewire | `command_wal_durable` | 83,217.0 | 135,318.6 | 113.0 | 367.0 | 1,211.0 | 0 |
| TreeDB Mongo gateway | `command_wal_durable` | 68,218.2 | 80,628.4 | 199.0 | 649.0 | 6,623.0 | 0 |
| TreeDB nativewire | `command_wal_relaxed` | 84,102.6 | 141,333.5 | 108.0 | 312.0 | 1,429.0 | 0 |
| TreeDB Mongo gateway | `command_wal_relaxed` | 68,372.2 | 76,859.7 | 212.0 | 757.0 | 7,751.0 | 0 |
| TreeDB nativewire | `bench` no-WAL | 92,884.1 | 141,857.8 | 108.0 | 322.0 | 1,133.0 | 0 |
| TreeDB Mongo gateway | `bench` no-WAL | 83,561.3 | 87,343.1 | 175.0 | 534.0 | 1,114.0 | 0 |

## Report Inventory

| report | status | use |
| --- | --- | --- |
| `docs/benchmarks/nativewire_ycsb_insert_error_classification_2026-06-30.md` | #3374 classification of the invalid intermediate nativewire `INSERT_ERROR` caveat; final for the #3385 local closeout. | Use for the current-head diagnostic run with `TREEDB_YCSB_LOG_ERRORS=1`, `-p silence=false`, raw scan counts, and the owner/risk classification for `/tmp/treedb_native_ycsb_current_head_20260629_202356`. |
| `docs/benchmarks/nativewire_ycsb_closeout_2026-06-30.md` | #2026 nativewire load-only closeout evidence refreshed after later Raft merges; reconciled by #3385. | Use for the 100k and 1M external `go-ycsb treedb-native` load proof on commit `61910b8eac108c5f2c35f07a374879d1fb2dc5c8` with `collection_meta` v5 compatibility; use the #3374 classification report for the recorded invalid intermediate 1M run. |
| `docs/benchmarks/ycsb_latest_main_2026-06-03.md` | Historical June external YCSB report. | Use for the historical MongoDB / TreeDB nativewire / TreeDB Mongo gateway results on then-current `origin/main` after the June 3 rerun. |
| `docs/benchmarks/ycsb_post_update_stack_2026-06-02.md` | Superseded latest-current report. | Keep for post-update-path-stack evidence before the latest main rerun and for comparison deltas. |
| `docs/benchmarks/ycsb_mongodb_treedb_2026-05-31.md` | Historical legacy-profile report. | Keep for the original MongoDB / TreeDB native / TreeDB Mongo comparison and the post-load Mongo-gateway cliff attribution. Do not use its `fast` rows as current public-profile guidance. |
| `docs/benchmarks/ycsb_treedb_profile_sweep_2026-06-01.md` | Superseded initial command-WAL sweep. | Keep as prior-run evidence. Prefer the current report for headline performance and the closeout report only for profile-surface conclusions. |
| `docs/benchmarks/ycsb_profile_closeout_2026-06-01.md` | Superseded profile-surface closeout. | Keep for profile-surface closeout evidence. Do not use as current headline performance after the latest main rerun. |
| `docs/benchmarks/mongo_gateway_compare_2026-04-29/` | Historical non-YCSB Mongo-gateway microbenchmark. | Useful for gateway-specific attribution, but not a substitute for external YCSB comparisons. |

## Standard Benchmark Boundary

Use this workload unless a report explicitly says otherwise:

- `recordcount=100000`
- `operationcount=10000`
- `threadcount=16`
- `table=usertable`
- load phase: 100,000 inserts
- run phase: 10,000 operations, roughly 95% reads and 5% updates
- local loopback TCP

The standard harness is:

```sh
scripts/ycsb_compare_mongodb_treedb.sh
```

The harness writes host metadata, exact commands, raw `go-ycsb` output,
`summary.tsv`, `summary.md`, server logs, and fresh DB directories under
`OUT_DIR`.

Any nonzero YCSB operation error counter such as `INSERT_ERROR`, `READ_ERROR`,
or `UPDATE_ERROR` invalidates that phase unless the report explicitly marks it
as exploratory known-bad evidence.

The June 3 report was captured against gomap
`d7407b81cc5712374ca8c1588cfb05f6f7d8490d`, where the external `go-ycsb`
`treedb-native` binding needed the version 3 `collection_meta` compatibility
patch recorded in `docs/benchmarks/ycsb_latest_main_2026-06-03.md`.

Current nativewire reruns for the #2026 closeout must use a `go-ycsb` client
that accepts `collection_meta` version 5. The June 30 closeout used
`/home/mikers/dev/pingcap/go-ycsb-2026-meta-v5` on branch
`codex/2026-treedb-native-meta-v5`; use that branch or an upstream client with
equivalent v5 support before rerunning current nativewire cells.

## Historical full-rerun recipes

The recipes below describe the historical runner and its same-fixture repeats.
They are retained for interpreting those reports. The current maintained shell
refuses ordinary `TARGETS=mongodb`; a default invocation is not a current MongoDB
baseline or the final twelve-arm qualification. Use the source-bound reference
preparation/run route in [the maintained reference contract](../../scripts/ycsb_mongodb_reference.md)
and the selected final packet. Native execution also has its separately declared
census-size limit. Final qualification uses fresh matched fixtures, not repeated
runs over one loaded database.

### Baseline And Durable Cell

Run the first cell with `RUN_REPEATS=3`. That repeats the YCSB run phase for
MongoDB, TreeDB nativewire, and TreeDB Mongo gateway after each target's load
phase, which is useful for separating first-run and repeat behavior:

```sh
RUN_ROOT=/tmp/treedb_ycsb_current_$(date +%Y%m%d_%H%M%S)
mkdir -p "$RUN_ROOT"

OUT_DIR="$RUN_ROOT/command_wal_durable" \
TREEDB_PROFILE=command_wal_durable \
MONGODB_MODE=docker \
MONGODB_ADDR=127.0.0.1:27018 \
NATIVE_ADDR=127.0.0.1:17130 \
TREEDB_MONGO_ADDR=127.0.0.1:27130 \
TREEDB_MONGO_DOCUMENT_FORMAT=bson \
RUN_REPEATS=3 \
BUILD=true \
BUILD_GOYCSB=true \
scripts/ycsb_compare_mongodb_treedb.sh
```

For a cheaper informal sweep, set `RUN_REPEATS=1` in this first command and use
the resulting MongoDB row as the once-per-matrix baseline.

### Remaining TreeDB Profiles

Reuse the MongoDB baseline from the first cell and run the other TreeDB profile
cells:

```sh
OUT_DIR="$RUN_ROOT/command_wal_relaxed" \
TREEDB_PROFILE=command_wal_relaxed \
MONGODB_MODE=skip \
NATIVE_ADDR=127.0.0.1:17131 \
TREEDB_MONGO_ADDR=127.0.0.1:27131 \
TREEDB_MONGO_DOCUMENT_FORMAT=bson \
RUN_REPEATS=3 \
BUILD=false \
BUILD_GOYCSB=false \
scripts/ycsb_compare_mongodb_treedb.sh

OUT_DIR="$RUN_ROOT/bench" \
TREEDB_PROFILE=bench \
MONGODB_MODE=skip \
NATIVE_ADDR=127.0.0.1:17132 \
TREEDB_MONGO_ADDR=127.0.0.1:27132 \
TREEDB_MONGO_DOCUMENT_FORMAT=bson \
RUN_REPEATS=3 \
BUILD=false \
BUILD_GOYCSB=false \
scripts/ycsb_compare_mongodb_treedb.sh
```

Use `RUN_REPEATS=3` for TreeDB cells so reports can separate first-run-after-load
behavior from repeat or median behavior. `RUN_REPEATS` repeats the run phase on
the same loaded DB/server; it does not rebuild a fresh DB for each repeat.

## Optional Long Diagnostic Run

If command-WAL tail latency or first-run-after-load behavior looks suspicious,
add a longer durable-profile run:

```sh
OUT_DIR="$RUN_ROOT/command_wal_durable_100k_run" \
TREEDB_PROFILE=command_wal_durable \
MONGODB_MODE=skip \
NATIVE_ADDR=127.0.0.1:17133 \
TREEDB_MONGO_ADDR=127.0.0.1:27133 \
TREEDB_MONGO_DOCUMENT_FORMAT=bson \
OPERATIONCOUNT=100000 \
RUN_REPEATS=1 \
BUILD=false \
BUILD_GOYCSB=false \
scripts/ycsb_compare_mongodb_treedb.sh
```

Use that long run for profiling and tail attribution, not as a replacement for
the standard 10k-operation headline matrix.

## Report Requirements

Fresh reports should include:

- exact commands;
- host and hardware context;
- gomap commit and go-ycsb commit;
- MongoDB image and digest;
- TreeDB profile and document format;
- load and run ops/sec;
- average, p95, p99, and max latency;
- all YCSB `*_ERROR` counters;
- first-run versus repeat or median behavior;
- artifact directories.

Do not mix old `fast` profile rows into current public-profile headline tables.
If a historical row is useful for context, label it as historical and explain
which commit/profile produced it.
