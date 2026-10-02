# TreeDB representative tradeoff report (#4895)

**Final-source collection and reduction complete:160 primary captures and all 7 native gates PASS.** The selected read changes reduce measured allocation work; the opt-in negative filter helps miss-heavy workloads at a visible startup/admission cost. Higher post-reopen resident memory and unresolved update/tail noise remain accepted, explicitly bounded tradeoffs. This is a representative warm local evaluation, not Cloudflare Quicksilver parity or production capacity qualification.

The measured integrated runtime is actual H merge
[`b215d8bb6f076431375538f217982afb97073e1b`](https://github.com/snissn/gomap/commit/b215d8bb6f076431375538f217982afb97073e1b),
full tree `e02f14ec3769fe56a4598383c7c35995d4c0c919`, TreeDB tree
`b8da8df3249162c9dc872a100d71099d5f3743ab`. H passed 49 current checks and independent GPT-6.1 Sol review before freezing/collection. This artifact report has separate independent review, current-head CI and merge gates; it does not relabel measurements as collected on a later documentation head. [#4888](https://github.com/snissn/gomap/issues/4888) and [#4895](https://github.com/snissn/gomap/issues/4895) close only after those gates and durable publication.

## Source and retained-evidence contract

| Label | Runtime identity | Comparison scope |
| --- | --- | --- |
| original | `ef22ce55b85524de2707453f22c5f8cd6ba89eaa` | Pre-graph product, filtering absent; off only. |
| control | `1f0b09b8ffad0c3eaa1eae0146958f45e57d8c1c` | Maintenance-only control; filtering absent; off only. |
| final-off | `b215d8bb6f076431375538f217982afb97073e1b` | Integrated product with opt-in filter disabled. |
| final-on | Same actual source, placement binary and freeze as final-off | Paired opt-in filter enabled; no default-switch claim. |

Original/control retain their exact tracked trees. Only the five landed
canonical additions named in the [runtime-v2 baseline contract](baseline/runtime-v2/README.md)
may be added. The external seven-file bridge supplies API/reader/clock
compatibility and the honest absent-filter schema, not candidate production
changes. Both path/compiler manifests were regenerated on the actual Linux host;
the prototype `canonical_head` is historical protocol metadata, not final
landing proof. Absent filter observations remain absent, never synthesized as
zero. Track bridge replacements and canonical source bytes separately.

The accepted collection inputs are the external
`final-representative-run-order-v3.json`, SHA-256
`c41214dc9598834b76c0b7ce146c4c8bcb3705b48e2e3dd6fdb8a9adfb763a6d`,
and `final-representative-existing-helper-launch-readiness.md`, SHA-256
`260ea06880ff1c7437159bd8f1452f3e4775199d8deb7e9dbbbc581d7083e422`,
under the coordinator's retained
`tmp/treedb-quicksilver-graph-20261001/`. The source-only plan has
`execution_authorized_by_this_file=false`. Its original 96-process prefix is
preserved; repeat 4/5 append 64 processes. Actual resolved operational receipts
must retain the proposal and bind every substituted final identity.

| Canonical input | SHA-256 verified at final freeze and pre-timer observation |
| --- | --- |
| `TreeDB/memory_budget_bench_test.go` | `03b389e475374af7157111b4a8feea72a22124aa4fec5f3342d512ea6cbad462` |
| `TreeDB/quicksilver_workflow_bench_test.go` | `3796b0c5219a43371026a0f82748f4404baf5380c13246a01afc40722c5a27ec` |
| `scripts/treedb_memory_budget_capture.py` | `4a8c7750475ace639c12f15d13000f8c93602c0074d47523c9938b153f0eead3` |
| `scripts/treedb_memory_budget_overlay.py` | `156abcee681ea4de1366df75e6dfe868ead23a240334f33d331804f529730167` |
| `scripts/treedb_quicksilver_capture.py` | `f98deee21a53c11bfa89cc0423e4ef086abda1f5eea24af5b978160875e5b9ed` |

Retained evidence ledger (all hashes are SHA-256):

| Required reference | Record / durable packet |
| --- | --- |
| Actual predecessor merges and dispositions | Objective ledger below; H actual runtime b215d8bb. Report CI/review/merge are separate gates. |
| Two Linux baseline manifests and seven-file bridge | F primary packet; original f16f9f96… / control bd111b32…; complete manifests, bridge inventory and compiler binding retained. |
| Nine preparations and independent pre-timer anchor | F primary packet includes nine ELFs, freezes, complete before/after compiler inventories, all toolchain/module inputs and external anchor dc3d8e7b…. |
| Host, exclusive grant, before/after observations | F primary packet, including boot/affinity/load/mount observations; shared host limitation below. |
| All 160 captures and 320 retained/negative helper validations | primary 160-receipt.json, 309a4bd48b817e43f159b0b04d38cf316e30091441b597630876c76ef82c740c; serial ordered 160/160 PASS. |
| Complete observations and five-value reduction | final 160-analysis.json, 55af559ee9661fb2a8ece99d325174b670d94ddbf6668572930cd8478c07c6df,12,166,133,391 bytes; complete metric support and review flags retained. |
| Native fixture/CLI/oracles/API/GC/pins | Seven-launch native receipt 3516 ee 88 a 47 e 9 bc 444050 a 32 d 68 f 3 cf 13 cb 838 df 01 bafb 2 a 56 f 3 b 9 d 4477 be 82 b; all 7 PASS. |
| Historical diagnostics, actual binaries and source | Historical packets below retain distinct original/candidate identities and failed/noisy results. Source proof packet contains exact tracked original/control/final trees. |
| Failed infrastructure/analysis/sealer packets | Historical failures plus native unused-module inventory failure, reducer v 1 hash-identity failure and initial seal missing-copy failure remain retained. Original failed DBs preserved locally, not repackaged or deleted. |
| Durable publication | Release and per-packet member indexes below; issue closure waits for verified public assets and report merge. |

The exact 20-key base environment is retained in
`full-campaign-environment.json`, SHA-256
`ba36a91dd56993f18b57d179a367bb2f8bdcb95fa34b6a27ef42b13565e3a1a8`:
Go 1.26.3, CGO enabled, GOMAXPROCS=2, GOMEMLIMIT=2 GiB, GOGC=100,
GOWORK/GOENV=off and GOTOOLCHAIN=local, with declared compiler, HOME/PATH,
cache and temporary paths. Pass the entire mapping without ambient additions;
preparation adds exactly four MEMORY controls and execution four QUICKSILVER
controls. Preserve actual complete maps and execution environment hashes.
Changing temporary filesystem/compiler paths invalidates equivalence.
The final exclusive serial grant is `root-F-final-b215-20261002-exclusive`,
SHA-256 `150067c2f0d603e42ff357a151310dcaa586cf1bb718a799b93459ae4643a1c8`.
The runner is shared Linux `mikers-B560-DS3H-AC-Y1`, kernel 6.8.0-138,
Intel i 5-11400 F,32 GiB RAM, `/mnt/fast4tb` ext 4 NVMe. Other services remain
active; only graph-owned native work is serialized. Boot identity,
affinity, process inventory, load and mount observations are retained.
`INFRASTRUCTURE_UNAVAILABLE: dedicated quiet runner` records the unavailable
infrastructure. There is no quiet-host or controlled OS-cache claim.

The independently copied nine-preparation anchor has SHA-256
`dc3d8e7b9bd55cfcde3e53a82838fd2a7ec60213de636ab319a4ed2cd7a24ed3`;
the independent Sol preflight review is
`71cc6405734e4bda844342ba4de9a3ba6fe0da8fb37e55603dbf477e6ad17d01`.
Root's live pre-timer observation is
`67491ec21a22be601ffad2c205d8cec113f89e4a33cc97fcb6c6d1c28956b29c`.
The original/control Linux manifests are respectively
`f16f9f962adc4e27181947e5939243405922a038ca3f8e507e77053e1781fed9`
and `bd111b322f9c0bd143d11186df3213acd1485451db0f8c6715a918eff401c3b5`.
Their canonical metadata remains historical; the observed runtime heads are
those in the table. Actual Go executable SHA-256 is
`d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65`.

Retained paths are `/mnt/fast4tb/gomap-quicksilver-full-20261002/F-final-b215/`
and the coordinator's local `tmp/treedb-quicksilver-graph-20261001/`.
Durable references are listed below.

## Matrix and collection order

All cells have 250,000 32-byte keys with a 24-byte shared prefix, one separate
present-empty sentinel, 256,000 warm read keys, 40,000 distinct permutation
updates (stride 7919), 1000-key synchronous CommandWAL batches and four update
checkpoints. Initial load has 250 batches plus the sentinel's separate SetSync.
There are five total checkpoints including the initial checkpoint. Full-byte
checks cover all values, interleaved misses and the sentinel initially, before
close and after reopen, with default CRC verification and checked final close.
Final checkpoint applied LSN must cover the positive acknowledged LSN.

| Cell | Values B / pointer threshold B | Warm distribution | Miss % | Owned API keys/request |
| --- | --- | --- | ---: | ---: |
| c01-inline-hit | compressible 256 / 1024 (inline) | uniform | 0 | 1 |
| c02-pointer-hit | compressible 256 / 1 (persistent pointers) | uniform | 0 | 1 |
| c03-balanced | compressible 256 / 1 | uniform | 50 | 1 |
| c04-miss-heavy | compressible 256 / 1 | uniform | 90 | 1 |
| c05-miss-extreme | compressible 256 / 1 | uniform | 99 | 1 |
| c06-skew | compressible 256 / 1 | Zipf | 90 | 1 |
| c07-natural-many | compressible 256 / 1 | Zipf | 90 | 64 |
| c08-random4k | deterministic PCG random 4096 / 1024 (persistent pointers) | Zipf | 90 | 64 |

Each cell uses a 32 MiB MAIN decoded-leaf / 32 MiB MAIN grouped-frame split:
67,108,864 configured payload bytes, **not** a physical RAM/process cap.
Side-store/cache-layer limits remain unchanged. Final-on allocates another
312,504 word-rounded filter bytes for the full domain including the sentinel.
Query IDs/CRCs occupy 2,048,000 bytes; payloads are generated one update batch
at a time. Filter admission/saturation/bootstrap costs remain observable costs.

There are nine preparations: three value/threshold placements per original,
control and final product. All 160 primary captures use fresh processes and
fresh output/TempDir DBs: **8 cells × 4 labels × 5 repetitions**. Within each
repeat, visit c01 through c08, with these four-label orders:

| Repeat | Order within each cell |
| --- | --- |
| 1 | original, control, final-off, final-on |
| 2 | control, final-on, final-off, original |
| 3 | final-off, final-on, original, control |
| 4 | original, final-on, final-off, control |
| 5 | control, original, final-off, final-on |

Final off/on are adjacent. Actual launch intervals demonstrate serial
nonoverlap. Existing [capture helpers](README.md) fix the benchmark to
`BenchmarkQuicksilverWorkflow`, `-test.benchtime=1 x -test.count=1 -test.benchmem
-test.v -test.timeout=60 m`, retain the package binary and require explicit full
source/freeze/harness/grant pins plus retained/negative validation. Profiling,
untimed counters and extra memory observers use separately declared processes,
argv and freezes outside these primary timings. Do not add a generic driver or
reuse the historical T/A campaign entrypoints for this matrix.

## Measurement definitions and limits

The source contract is [the actual fixture](../../../TreeDB/quicksilver_workflow_bench_test.go)
and [retained validator](../../../scripts/treedb_quicksilver_capture.py), with
`quicksilver-workflow-v1` packets parsed from raw stderr and
`quicksilver-workflow-run-v1` execution records. The baseline bridge retains its
separate absent-filter contract. Keep those schemas intact.

| Result | Packet seam / calculation | Units and scope |
| --- | --- | --- |
| Warm throughput | `read_keys × 1e9 / owned_warm_reads.elapsed_ns` | keys/s including key preparation, API work and CRC consumption; warm after placement scan/full proof. |
| Warm average cost | `owned_warm_reads.elapsed_ns / read_keys`; divide phase `allocated_bytes`/`allocations` by `read_keys` | ns/key, B/key, objects/key for the whole warm phase. Also retain per-request figures using `read_api_requests`; batch requests contain 64 keys. |
| Warm API tails | `read_p99_ns`, `read_p999_ns`, `read_max_ns`, `latency_samples` | ns/owned API request; samples every 17 th request. Scalar: 256,000 calls/15,059 samples; batch: 4000 calls/236 samples. Batch p 99.9 and sampled max coincide; this max covers samples, not every request. |
| Update ACKs | chronological `update_ack_samples_ns`, count/sum and p 99/p 999/max | ns/1000-key WriteSync only; excludes batch construction/Set/Close/checkpoints. 40 samples, 10 per update phase; p 99 and p 99.9 both equal max. No resolved production p 99.9/SLO. |
| Update phase cost | `updates_1..4` elapsed/allocation/Stats deltas with operations=10,000 each | ns/key, B/key, objects/key include value construction and concurrent reader activity; not isolated ACK or engine cost. |
| Checkpoint cost | `initial_checkpoint`, `checkpoint_1..4` elapsed/allocation/Stats | ns/checkpoint, B/checkpoint, objects/checkpoint. **checkpoint_4 includes reader stop/join and quantile/report synchronization**; do not interpret it as isolated checkpoint service time. |
| Concurrent owned reads | `concurrent_owned_reads` raw `samples_ns`, reads/elapsed, p 99/p 999/max | One present-only scalar Get reader, even-key permutation 7919, generation 0-or 1 full-byte proof; independent of warm miss/distribution/batch settings. Quantiles sample every 17 requests; max covers all successful reads. Elapsed includes four updates/checkpoints plus join/report overhead. |
| Residency | phase `heap_bytes`, `post_reopen_gc_heap_bytes`, owner Stats and RSS/HWM | Bytes; direct HeapAlloc after one runtime.GC with reopened DB still open. Not ValueLogGC, callback-retained memory or continuous peak heap. RSS/HWM and periodic Stats peaks are process observations. `closure_stats` is captured immediately after checkpoint 4, before the final proof, close and reopen; it is not the complete workflow peak. |
| Allocations | phase `allocated_bytes`, `allocations`; raw Go benchmark B/op and allocs/op retained separately | Phase totals/explicit operation denominators; whole-workflow Go op=one complete workflow, not one read. No fixture subtraction or engine-only attribution. |
| Logical/physical work | before/after owner Stats, outer-leaf loads/bytes/checksum/cache counters and available flush/root-apply page-byte counters | Interval deltas, preserving owner/path names and eligibility. Encoded/page work is not device I/O; unavailable baseline fields remain absent. |
| Process I/O | `phase_process_io` and `process_io` after-minus-before | `rchar/wchar/read_bytes/write_bytes/cancelled_write_bytes` in bytes; `syscr/syscw` syscall counts. Complete monotone Linux kernel process observations include helper/concurrent work; no whole-device write claim. |
| Durable files/debt | phase `logical_file_bytes`, `closed_files`, separate native maintenance debt/pin evidence | Logical bytes by actual relative filename and MAIN/side-store owner: redo WAL, persistent value/leaf logs, index and other files separately. Not allocated FS blocks or proof of reclaimability. |
| Load/reopen/filter cost | `load_sync`, initial checkpoint, `reopen_ns`, per-boundary Stats and filter bytes | Publication/rebuild/admission costs stay visible in their containing phases; isolated rebuild CPU/time is unmeasured unless separately diagnosed. |

The concurrent reader caps samples at 65,536 and fails closed on overflow:
at most 1,114,112 successful requests before the next sampled call fails.
Preserve failures; do not truncate samples. Its full-value CRC/validation occurs
outside API latency but inside reader lifetime. Earlier update/checkpoint Stats
and allocations observe the concurrent process. Whole-workflow CPU profiles
also include setup, load, proof, CRC, GC and close; attribution requires engine
frames/mechanism-specific diagnostic evidence, not total test-body CPU.

Owner gauges must not be added twice through aliases: MAIN vs cache-manager
frame/scratch owners, append-only free bins vs live/reset/leased arenas, heap vs
mmap address space vs RSS are distinct. Report configured budgets, valid leaf
bytes, frame payload/backing, pool capacity, actual heap and observed RSS/HWM
separately. [Owner decision](../treedb_memory_budget/OWNER_DECISION.md) documents
the unchanged limits; the rejected 32 MiB free-bin trial is not final policy.

## Integrated read results

All table entries are medians of five separate processes. [RESULTS.json](RESULTS.json) supplies all five values, extrema, spread and paired changes for the navigation metrics. The durable v 2 projection includes all selected owner/memory/work metrics; the full 12.17 GB reduction preserves every metric and review flag, and all 160 raw packets remain available. Neither projection replaces full provenance validation. c07/c08 requests contain 64 keys.

| Cell | Original keys/s | Control keys/s | Final-off keys/s | Final-on keys/s |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 231,029 | 235,605 | 269,163 | 268,339 |
| c02-pointer-hit | 154,165 | 149,409 | 165,595 | 161,159 |
| c03-balanced | 239,156 | 234,325 | 257,379 | 299,330 |
| c04-miss-heavy | 457,423 | 447,416 | 478,503 | 857,576 |
| c05-miss-extreme | 605,477 | 582,379 | 653,471 | 1,565,836 |
| c06-skew | 626,333 | 623,714 | 679,619 | 1,159,009 |
| c07-natural-many | 1,410,783 | 1,353,149 | 1,381,843 | 3,031,219 |
| c08-random4k | 119,277 | 120,013 | 172,467 | 181,734 |

| Cell | Off/control throughput change | Positive pairs | On/off throughput change | Positive pairs | Off/control allocation B/request change |
| --- | --- | --- | --- | --- | --- |
| c01-inline-hit | +14.24% | 5/5 | -0.31% | 1/5 | -92.42% |
| c02-pointer-hit | +10.83% | 5/5 | -2.68% | 1/5 | -93.85% |
| c03-balanced | +9.84% | 5/5 | +16.30% | 5/5 | -93.97% |
| c04-miss-heavy | +6.95% | 4/5 | +79.22% | 5/5 | -95.05% |
| c05-miss-extreme | +12.21% | 5/5 | +139.62% | 5/5 | -98.12% |
| c06-skew | +8.96% | 4/5 | +70.54% | 5/5 | -92.16% |
| c07-natural-many | +2.12% | 3/5 | +119.36% | 5/5 | -21.64% |
| c08-random4k | +43.71% | 5/5 | +5.37% | 5/5 | -96.54% |

### Allocation bytes per owned request

| Cell | original B/request | control B/request | final-off B/request | final-on B/request |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 4,169.95 | 4,169.96 | 316.08 | 316.08 |
| c02-pointer-hit | 5,562.01 | 5,562.01 | 342.30 | 342.34 |
| c03-balanced | 3,070.65 | 3,070.14 | 185.12 | 185.12 |
| c04-miss-heavy | 1,116.93 | 1,116.91 | 55.29 | 55.82 |
| c05-miss-extreme | 632.47 | 632.96 | 11.93 | 11.90 |
| c06-skew | 617.68 | 617.18 | 48.37 | 53.47 |
| c07-natural-many | 15,254.36 | 15,223.35 | 11,928.71 | 11,763.58 |
| c08-random4k | 2,760,533.64 | 2,760,533.96 | 95,548.07 | 95,682.14 |

### Allocation objects per owned request

| Cell | original objects/request | control objects/request | final-off objects/request | final-on objects/request |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 3.03 | 3.03 | 1.93 | 1.93 |
| c02-pointer-hit | 3.01 | 3.01 | 1.93 | 1.93 |
| c03-balanced | 2.01 | 2.01 | 0.97 | 0.97 |
| c04-miss-heavy | 1.20 | 1.20 | 0.19 | 0.19 |
| c05-miss-extreme | 1.02 | 1.02 | 0.02 | 0.02 |
| c06-skew | 1.18 | 1.18 | 0.18 | 0.18 |
| c07-natural-many | 3.13 | 3.13 | 3.05 | 3.04 |
| c08-random4k | 14.98 | 14.98 | 9.84 | 9.84 |

### Warm owned API p 99

| Cell | original µs/request | control µs/request | final-off µs/request | final-on µs/request |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 13.09 | 12.95 | 8.66 | 8.76 |
| c02-pointer-hit | 18.19 | 19.23 | 16.09 | 16.89 |
| c03-balanced | 16.29 | 16.04 | 13.17 | 11.22 |
| c04-miss-heavy | 12.75 | 13.23 | 10.17 | 8.36 |
| c05-miss-extreme | 9.85 | 10.04 | 8.59 | 4.56 |
| c06-skew | 5.38 | 5.21 | 4.65 | 4.55 |
| c07-natural-many | 66.82 | 76.92 | 66.77 | 39.76 |
| c08-random4k | 1,195.54 | 1,393.86 | 566.90 | 585.23 |

### Warm owned API p 99.9

| Cell | original µs/request | control µs/request | final-off µs/request | final-on µs/request |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 20.07 | 18.53 | 16.60 | 15.26 |
| c02-pointer-hit | 28.34 | 30.35 | 22.29 | 22.16 |
| c03-balanced | 26.55 | 27.63 | 20.27 | 20.24 |
| c04-miss-heavy | 22.58 | 23.60 | 17.68 | 16.86 |
| c05-miss-extreme | 16.65 | 17.09 | 13.63 | 9.30 |
| c06-skew | 13.98 | 14.29 | 12.16 | 9.82 |
| c07-natural-many | 105.43 | 111.24 | 106.47 | 73.88 |
| c08-random4k | 1,945.89 | 2,126.25 | 712.16 | 625.35 |

Original→maintenance-control median throughput changes span−4.09% to+1.98%; keep that comparison distinct from read mechanisms. Final-off improves all 8 median throughputs, but c07 is only+2.12% with 3/5 positive pairs and 13.7% control spread. Warm throughput spreads above 10% include c03 off 10.6%, c04 off 11.6%/on 15.1%, c06 off 13.4%, c07 control 13.7%. These remain unresolved noise flags, not statistical confidence. Scalar samples cover 15,059 calls; batch samples 236, so batch p 99.9 equals sampled max. No production tail/SLO claim follows.

## Mechanism proof and remaining work

The shared decoder now preserves reusable backing capacity after successful decode, while retaining raw-length bounds. c01 warm small-pool allocation calls fall 26,108→0 and scratch bytes 807,703,500→0. c02 falls 19,102→96 and 1,206,352,342→6,215,680 bytes, with identical 256,000 leaf loads,133,763 frame-cache hits and 257,573 CRC checks. Scalar compressed-pointer fallback calls remain 256,000 in c02: required output/compressed work is visible. Composite timing does not isolate decoder contribution.

Ordinary backend Get/GetVersioned/GetAppend/GetVersionedAppend use the private pooled capture guard with unchanged pin/release/retry semantics; exported snapshots remain independently owned. [Matched historical private-read qualification](../../../TreeDB/docs/benchmarks/private-point-read-4890.md) removes 512 B and one allocation per read (public Get 3431→3407 ns; GetAppend 3420→3285 ns; backend append 337.5→196.4 ns). Its 101 capture/release and zero-live-pin checks remain separate evidence. Pool cold/concurrent/GC effects are not isolated by F 160.

The optional filter covers the index/root/sequence, admits insertion bits before publishing covered roots, and falls back when coverage/bootstrap/budget/key eligibility is unavailable. It adds 312,504 bit-storage bytes beyond matched cache budgets. The measured physical leaf-load reduction demonstrates useful probe avoidance:

| Cell | Final-off outer-leaf loads | Final-on outer-leaf loads | Avoided |
| --- | --- | --- | --- |
| c01-inline-hit | 256,000 | 256,000 | 0.00% |
| c02-pointer-hit | 256,000 | 256,000 | 0.00% |
| c03-balanced | 256,000 | 129,053 | 49.59% |
| c04-miss-heavy | 256,000 | 27,486 | 89.26% |
| c05-miss-extreme | 256,000 | 4,659 | 98.18% |
| c06-skew | 256,000 | 27,397 | 89.30% |
| c07-natural-many | 88,353 | 15,065 | 82.95% |
| c08-random4k | 88,353 | 15,031 | 82.99% |

c07/c08 have 4,000 grouped GetMany calls and zero fallback calls in every label. Owned output is materialized while holding the grouped-cache slot lock; borrowed cache bytes do not escape. Observed MAIN leaf/frame payload maxima each stay within 33,554,432 bytes. Those are payload budgets, not a total RAM bound. Current-mmap direct-decode counters are zero in these warm fixtures. Callback improvements use the separate five-pair #4920/#4921 evidence, not F 160 owned results: sorted pointer callback 363.113→143.948µs(−60.36%),271,574→776 B,175→7 allocations; clustered−57.18%, uniform−45.92%. Source applicability and residency/lifetime checks remain in their historical packets.

Checkpoint 1 leaf-log append bytes are unchanged across labels:2,998,388 inline,2,811,434 compressed-pointer and 2,999,435 random 4 k. F 160 elapsed changes do not establish a new reduction in encoded checkpoint/root-apply work. Broader algorithm decisions below use separately eligible fixtures.

## Load, publication, writes and reopen

### Load including synchronous batches

| Cell | original s | control s | final-off s | final-on s |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 1.45 | 1.45 | 1.45 | 1.59 |
| c02-pointer-hit | 3.87 | 3.84 | 3.58 | 3.67 |
| c03-balanced | 3.86 | 3.91 | 3.80 | 3.81 |
| c04-miss-heavy | 3.68 | 3.64 | 3.86 | 3.68 |
| c05-miss-extreme | 3.64 | 3.79 | 3.76 | 3.52 |
| c06-skew | 3.63 | 3.54 | 3.69 | 3.71 |
| c07-natural-many | 3.52 | 3.81 | 3.56 | 3.92 |
| c08-random4k | 6.52 | 6.47 | 6.51 | 6.61 |

### Initial checkpoint

| Cell | original ms | control ms | final-off ms | final-on ms |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 109.90 | 110.00 | 111.49 | 123.89 |
| c02-pointer-hit | 122.56 | 121.44 | 126.38 | 137.55 |
| c03-balanced | 128.34 | 139.64 | 137.49 | 129.66 |
| c04-miss-heavy | 129.26 | 124.86 | 119.59 | 139.46 |
| c05-miss-extreme | 121.41 | 121.42 | 124.45 | 130.33 |
| c06-skew | 123.53 | 124.50 | 123.49 | 146.34 |
| c07-natural-many | 121.02 | 121.73 | 133.83 | 138.02 |
| c08-random4k | 138.61 | 139.97 | 138.66 | 158.00 |

### Sum of 40 update ACKs

| Cell | original ms | control ms | final-off ms | final-on ms |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 252.70 | 244.06 | 273.94 | 246.13 |
| c02-pointer-hit | 601.05 | 599.22 | 565.09 | 689.54 |
| c03-balanced | 649.50 | 715.88 | 707.66 | 590.46 |
| c04-miss-heavy | 672.22 | 579.92 | 579.68 | 607.25 |
| c05-miss-extreme | 573.32 | 588.59 | 581.06 | 585.76 |
| c06-skew | 578.57 | 577.92 | 575.84 | 586.46 |
| c07-natural-many | 585.45 | 590.03 | 614.47 | 576.24 |
| c08-random4k | 765.03 | 784.87 | 746.47 | 834.95 |

### Update ACK p 99=p 99.9=max

| Cell | original ms | control ms | final-off ms | final-on ms |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 17.06 | 16.73 | 16.49 | 14.00 |
| c02-pointer-hit | 23.21 | 31.59 | 17.37 | 25.35 |
| c03-balanced | 22.72 | 31.21 | 26.80 | 18.93 |
| c04-miss-heavy | 25.11 | 25.32 | 25.50 | 19.29 |
| c05-miss-extreme | 22.83 | 27.85 | 25.80 | 21.74 |
| c06-skew | 25.90 | 19.41 | 21.81 | 25.57 |
| c07-natural-many | 25.34 | 25.87 | 29.64 | 25.25 |
| c08-random4k | 29.35 | 30.89 | 24.70 | 28.07 |

### Reopen

| Cell | original ms | control ms | final-off ms | final-on ms |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 42.05 | 46.46 | 46.18 | 81.27 |
| c02-pointer-hit | 68.30 | 60.96 | 67.40 | 86.71 |
| c03-balanced | 61.71 | 71.27 | 61.77 | 79.54 |
| c04-miss-heavy | 62.39 | 60.15 | 62.35 | 82.07 |
| c05-miss-extreme | 63.75 | 65.48 | 59.04 | 77.85 |
| c06-skew | 62.04 | 64.15 | 59.01 | 86.65 |
| c07-natural-many | 62.87 | 64.09 | 60.87 | 81.52 |
| c08-random4k | 524.78 | 532.36 | 525.28 | 540.43 |

### Concurrent owned Get p 99

| Cell | original µs | control µs | final-off µs | final-on µs |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 14.97 | 14.38 | 13.43 | 14.16 |
| c02-pointer-hit | 24.26 | 23.87 | 19.03 | 18.92 |
| c03-balanced | 25.70 | 23.54 | 18.86 | 19.66 |
| c04-miss-heavy | 23.95 | 24.02 | 19.16 | 19.33 |
| c05-miss-extreme | 23.81 | 24.84 | 19.66 | 20.88 |
| c06-skew | 25.06 | 24.38 | 20.82 | 19.98 |
| c07-natural-many | 23.66 | 24.76 | 19.66 | 20.17 |
| c08-random4k | 132.93 | 129.03 | 113.87 | 110.08 |

### Concurrent owned Get p 99.9

| Cell | original µs | control µs | final-off µs | final-on µs |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 25.73 | 25.57 | 20.62 | 22.79 |
| c02-pointer-hit | 71.51 | 73.89 | 39.36 | 31.82 |
| c03-balanced | 115.60 | 63.69 | 35.12 | 41.26 |
| c04-miss-heavy | 62.37 | 58.62 | 33.96 | 30.88 |
| c05-miss-extreme | 97.07 | 48.36 | 39.20 | 44.02 |
| c06-skew | 87.72 | 98.52 | 37.55 | 35.46 |
| c07-natural-many | 46.69 | 237.36 | 44.42 | 48.51 |
| c08-random4k | 562.28 | 465.87 | 171.05 | 163.74 |

### Concurrent owned Get max

| Cell | original ms | control ms | final-off ms | final-on ms |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 15.20 | 19.91 | 20.13 | 18.04 |
| c02-pointer-hit | 10.09 | 21.39 | 14.19 | 16.51 |
| c03-balanced | 14.67 | 17.87 | 15.52 | 15.64 |
| c04-miss-heavy | 15.85 | 15.93 | 16.09 | 15.91 |
| c05-miss-extreme | 15.84 | 15.98 | 16.08 | 15.88 |
| c06-skew | 15.92 | 18.14 | 15.75 | 13.58 |
| c07-natural-many | 16.00 | 16.08 | 17.68 | 16.43 |
| c08-random4k | 19.20 | 19.73 | 11.95 | 11.30 |

Per-update phase and all checkpoint elapsed observations, total process I/O and all five paired values are in RESULTS.json; the projection/full analysis also retain phase allocation/heap/I/O and owner deltas. Update phase allocation includes concurrent readers. Their rate is unrestricted, so a faster reader can do more calls during the same write interval. CP 4 includes reader stop/join/report synchronization. There are 40 ACK samples,10 per phase; tails and many update/checkpoint spreads exceed 10%. For example inline final-off ACK sum rises 12.24%, pointer-hit final-on rises 22.02%; inline off update phases 2–4 rise 11.1/11.3/19.0%, while pointer-hit on phases rise 18.5–23.9%. These composite noisy intervals are not resolved isolated engine regressions or wins. Concurrent p 99.9 spread reaches 9499.1% c07 on and 2933.7% c08 off. A quiet matched fixed-rate-reader test is the revisit trigger if a write/tail guarantee is needed; these packets remain unchanged.

Filter reopen rises 28.64–75.96% on c01–c07, all 5 pairs positive, and 2.88% c08(4/5). Rebuilding coverage performs a full-domain scan before return; its work is inside reopen. Hit-only c02 throughput falls 2.68% on/off and update phases also cost more amid 23–41% spread. **Coordinator disposition:** accept bounded optional bootstrap/admission and hit-path costs for the measured miss-heavy benefit; keep the default off. Enable only after evaluating misses, startup frequency and write mix. Isolated rebuild CPU and saturation scaling are unmeasured.

## Memory and durable bytes

### Heap after reopen and one runtime.GC

| Cell | original MiB | control MiB | final-off MiB | final-on MiB |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 147.44 | 147.47 | 147.41 | 147.77 |
| c02-pointer-hit | 167.14 | 167.14 | 164.92 | 165.18 |
| c03-balanced | 167.19 | 167.16 | 164.88 | 165.21 |
| c04-miss-heavy | 167.19 | 167.16 | 165.28 | 165.27 |
| c05-miss-extreme | 167.22 | 167.17 | 165.11 | 165.40 |
| c06-skew | 167.11 | 167.17 | 165.08 | 165.25 |
| c07-natural-many | 167.01 | 167.02 | 165.18 | 165.45 |
| c08-random4k | 208.04 | 208.03 | 208.00 | 208.31 |

### RSS after reopen and one runtime.GC

| Cell | original MiB | control MiB | final-off MiB | final-on MiB |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 292.91 | 292.34 | 315.29 | 310.68 |
| c02-pointer-hit | 341.44 | 355.16 | 414.05 | 408.27 |
| c03-balanced | 341.95 | 356.82 | 386.87 | 383.73 |
| c04-miss-heavy | 350.56 | 345.30 | 382.84 | 398.34 |
| c05-miss-extreme | 330.65 | 355.89 | 375.64 | 364.76 |
| c06-skew | 347.18 | 358.05 | 384.53 | 371.33 |
| c07-natural-many | 358.60 | 323.77 | 379.52 | 390.85 |
| c08-random4k | 364.51 | 346.96 | 415.92 | 459.66 |

### RSS HWM before final proof/close/reopen

| Cell | original MiB | control MiB | final-off MiB | final-on MiB |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 466.93 | 465.05 | 461.61 | 468.11 |
| c02-pointer-hit | 426.00 | 423.73 | 355.55 | 358.80 |
| c03-balanced | 412.16 | 424.60 | 356.67 | 348.84 |
| c04-miss-heavy | 411.83 | 415.47 | 347.72 | 346.27 |
| c05-miss-extreme | 412.06 | 417.36 | 344.90 | 343.34 |
| c06-skew | 420.54 | 418.95 | 355.82 | 339.19 |
| c07-natural-many | 414.74 | 406.84 | 352.08 | 351.09 |
| c08-random4k | 449.09 | 452.48 | 443.66 | 453.30 |

### Observed peak HeapAlloc before final proof/close/reopen

| Cell | original MiB | control MiB | final-off MiB | final-on MiB |
| --- | --- | --- | --- | --- |
| c01-inline-hit | 428.08 | 425.72 | 388.71 | 429.63 |
| c02-pointer-hit | 448.64 | 452.24 | 407.14 | 404.11 |
| c03-balanced | 453.02 | 456.96 | 404.09 | 405.10 |
| c04-miss-heavy | 441.37 | 448.59 | 404.77 | 414.21 |
| c05-miss-extreme | 442.57 | 451.52 | 401.67 | 398.40 |
| c06-skew | 455.53 | 449.88 | 411.73 | 388.12 |
| c07-natural-many | 465.45 | 432.23 | 408.45 | 405.66 |
| c08-random4k | 424.46 | 409.68 | 409.50 | 425.40 |

**Coordinator disposition:** explicitly accept higher boundary RSS for this bounded fixture, while making no sustained-residency or memory-ceiling guarantee. c02–c08 control→off RSS increases in all 5 pairs; c02+16.58%, c07+17.22%, c08+19.88%. c08 on adds 10.52% in all 5 pairs. This is material and is retained even though post-GC live heap is flat/lower and the earlier pre-proof/pre-reopen HWM generally improves. c08 control/off HeapAlloc 208.52/208.49 MiB, HeapSys 562.94/491.03 MiB, idle-unreleased 133.40/185.62 MiB, released 218.41/94.93 MiB, GC count 117/51, mmap 11.86 MiB unchanged. c02 shows the same fewer-GC/lower-released pattern. Less allocation and different GC/scavenger cadence are a supported explanation consistent with the observations, not a proven causal decomposition or absence of a leak. A sustained idle/scavenger run under the deployment's actual RSS budget is the revisit trigger; do not add forced scavenging to product code to disguise this result.

The configured 64 MiB MAIN cache payload, query table 1.95 MiB and optional 0.298 MiB filter do not sum to total process RAM. Owner gauges/aliases and heap/mmap/RSS are distinct. Cold, larger-than-RAM, continuous pressure and production concurrency were not exercised.

Closed logical MAIN files (median bytes; WAL is separate persistent redo, value/leaf logs remain persistent storage):

| Cell | Label | MAIN index | MAIN persistent value log | MAIN persistent leaf log | MAIN WAL |
| --- | --- | --- | --- | --- | --- |
| c01-inline-hit | control | 8,388,608 | absent | 14,668,433 | 12 |
| c01-inline-hit | final-off | 8,388,608 | absent | 14,668,433 | 12 |
| c01-inline-hit | final-on | 8,388,608 | absent | 14,668,433 | 12 |
| c02-pointer-hit | control | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c02-pointer-hit | final-off | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c02-pointer-hit | final-on | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c03-balanced | control | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c03-balanced | final-off | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c03-balanced | final-on | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c04-miss-heavy | control | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c04-miss-heavy | final-off | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c04-miss-heavy | final-on | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c05-miss-extreme | control | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c05-miss-extreme | final-off | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c05-miss-extreme | final-on | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c06-skew | control | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c06-skew | final-off | 4,194,304 | 5,188,739 | 13,616,776 | 12 |
| c06-skew | final-on | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c07-natural-many | control | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c07-natural-many | final-off | 4,194,304 | 5,188,063 | 13,617,515 | 12 |
| c07-natural-many | final-on | 4,194,304 | 5,188,739 | 13,616,776 | 12 |
| c08-random4k | control | 4,194,304 | 1,200,847,271 | 14,242,860 | 12 |
| c08-random4k | final-off | 4,194,304 | 1,200,847,271 | 14,242,860 | 12 |
| c08-random4k | final-on | 4,194,304 | 1,200,847,271 | 14,242,860 | 12 |

Other/dictionary owner files are retained in RESULTS.json and the complete closed-file inventories in raw packets. Logical sizes are essentially unchanged apart from small compressed-layout variation in c06/c07. Process-I/O deltas measure kernel accounting for this process, not whole-device writes or allocated filesystem blocks. No deletion/reclaimability inference follows from file age.

## Exact-final native integrity and maintenance

Seven serial exact-final launches succeeded outside primary timers. Fresh 100 k even keys with PCG random 4096 B values,40 k updates, CommandWAL and checkpoints passed the existing external diagnostic. Two actual `treemap compact <DB> -rw -mode exhaustive -sync-each-phase -json` passes both exited 0 and each was followed by read-only full-byte verification of 100,000 final values and 10,000 declared misses. Separate public API 100 k exhaustive/explicit ValueLogGC/LeafGenerationGC, checked-close and fresh read-only oracles passed. Stable identity, pinned zombie and pinned leaf-generation safety tests passed.

| Native gate | Exit / outcome | Wall time |
| --- | --- | ---: |
| Fresh fixture/diagnostic |0 / PASS |25.094 s |
| CLI pass 1 / read-only oracle 1 |0 / PASS;0 / PASS |7.132 s /4.757 s |
| CLI pass 2 / read-only oracle 2 |0 / PASS;0 / PASS |0.469 s /4.893 s |
| Public API/explicit GC 100 k |0 / PASS |32.349 s |
| Stable identity/zombie/generation pins |0 / PASS |27.081 s |

Fixture diagnostic logical sums 415,861,339 initial→587,224,111 updated→418,436,120 after offline compaction (~28.7% reduction of updated total). These boundaries are separate from the later two CLI totals. CLI 1 vacuum succeeded 51,611,575 ns and CLI 2 succeeded 45,296,512 ns. **Both CLI `ByteMinimized`, `FullyCompacted` and `PolicyFullyCompacted` remain false.** Leaf-GC debt falls 2 generations/1,086,635 B→1 generation/591,521 B; other policy-required rewrite/GC/leaf-pack/vacuum debts are zero. CLI total bytes 417,876,546→418,052,073→418,145,016 grow slightly. API also retains 1,020 B rewrite and 928,225 B leaf-GC debt. Successful maintenance and integrity do not prove convergence or byte minimization. This accepted limit must be revisited if an offline byte-minimized guarantee is requested; no pinned/live/recovery storage can be removed to satisfy it.

The original diagnostic labels Linux Maxrss as bytes: raw 360752 is KiB, converted 369,410,048 B. Its deferred Close is not checked-close proof; the separate API test supplies that seam. Protected original failed databases remain intact. The [persistent value-log lifecycle](../../../TreeDB/docs/spec/value-log-lifecycle.md) and [durability contract](../../../TreeDB/docs/spec/write-path-and-durability.md) still govern.

## Objective and algorithm dispositions

| Obligation | Actual landing / accepted disposition |
| --- | --- |
| Decoder #4889 /#4896 | a217ee3b: reusable backing and bounds; repeated allocation reduction with unchanged checksum/ownership tests. |
| Private reads #4890 /#4897 | a68a84c7: private guard removes 512 B/one allocation without exporting pooled aliases; matched historical qualification. |
| Maintenance #4886 /#4887 |1f0b09b8: supported exhaustive compaction failure fixed; final native gates PASS, byte-minimization/debt limit retained. |
| Owned values #4891 /#4900 |784b0170: safe owned cache/materialization route and redundant intermediate-copy removal, CRC/lifetime/reopen guards. |
| Negative lookup #342 /#4902 |50413522: coherent optional coverage; integrated miss-probe reduction and bounded accepted on/off costs. |
| Callback pointer #4920 /#4921 |af2d4003: selected pointer frame reuse meets≥50% B/alloc and≥25% sorted/clustered latency gates; historical five-pair and separate residency packet. |
| Memory #4892 /trim #4914 | H b215d8bb publishes exact owner/budget decision. Reject 32 MiB checkpoint free-bin policy: heap−22.8 MB/−9.2%, but pointer update+7.21%(4/5)/checkpoint+12.33%, inline update+15.6%/mixed+21%. Existing policy retained; #3589 remains distinct. |
| Algorithms #4893 /#4917 | H publishes rejected shared traversal: internal visits−98.4% but uniform inline owned+9.18%(5/5), view+9.07%(4/5), pointer owned+3.93%(5/5). Singleton trial benefit below spread and allocation guard failed; no production promotion. |
| Snapshot eligibility #4916 /#4918 /#4931 | e3e912be/f9152b5b: eligible rotation fixture and exact decision. Inline extra work node−10.64%/leaf−10.36%/interval−11.52%/checkpoint−22.81%(3/3), but thin p 99.9+18.28%(2/3); retain defaults. Zero extra-work cells cannot refute a general algorithm. Persistent-delta activation withheld pending recovery/economic/capacity qualification. |
| Harness/integrated T/A decisions #4894 /#4913 | b215d8bb: reviewed/landed source, bridge, checker and component decisions before official freeze/collection. Old #4930/#4932 artifact PRs superseded unmerged only after verified H publication. |
| Final #4895 |160 fresh primary captures,320 validations, all 7 native launches, full reduction and bounded independent arithmetic review; final artifact review/CI/merge/publication remain separate completion gates. |

No failed objective is silently converted into a speed claim. Measured rejected broader policies are explicit decisions; accepted RSS/filter/noise/native convergence limits have evidence and revisit triggers above. Existing #2771/#2922/#3589 ownership is preserved.

## Durable raw publication and audit

[Evidence release](https://github.com/snissn/gomap/releases/tag/treedb-quicksilver-4888-evidence-b215d8bb-20261002) targets exact b215d8bb and is a prerelease, not a product release. Every packet has a frozen exact member index with sizes/SHA-256, full streamed verification and retained original source/command boundaries. Publication is complete only after assets are publicly reachable, server identities/digests and downloaded bytes verify; #4895 records that terminal gate.

| Packet | Archive / exact member index | Members | Archive SHA-256 |
| --- | --- | --- | --- |
| Historical primary/owner/negative/value/algorithm diagnostics | [historical-evidence-local.tar.gz](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/historical-evidence-local.tar.gz) / [index](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/member-index.json) | 1923 | `4a3213767c6e1301bb37e2e670fb1d8d873ae9deaf26145f3787f50181347824` |
| Initial maintenance raw/source/6 actual binaries | [maintenance-initial-evidence-local.tar.gz](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/maintenance-initial-evidence-local.tar.gz) / [index](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/maintenance-initial-member-index.json) | 346 | `4fcf839df966ff0f54aa3f20a6fb504ade1391ee7cf623f094adb0e704784863` |
| Research profiles/background/5 actual binaries | [research-background-evidence-local.tar.gz](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/research-background-evidence-local.tar.gz) / [index](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/research-background-member-index.json) | 186 | `c73034da92e78bea17f632f65aaf26f4ea5d0ccf0a17c644d0ab47d1560ec3c9` |
| Research source/provenance supplement | [research-source-supplement-evidence-local.tar.gz](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/research-source-supplement-evidence-local.tar.gz) / [index](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/research-source-supplement-member-index.json) | 5 | `74e95d43506a4a12c36ecd078694a53013c2a61399cffdb4d253842319b23e5a` |
| Historical private-read raw/repeated results/source | [private-read-historical-evidence-local.tar.gz](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/private-read-historical-evidence-local.tar.gz) / [index](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/private-read-historical-member-index.json) | 100 | `a75ce4e70843b34a75fd48261209a92a34f4dd5d9244a063d76baf54095bbe11` |
| Historical checkpoint-bin T raw/input/27 binaries | [historical-T-raw-and-binaries.tar.gz](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/historical-T-raw-and-binaries.tar.gz) / [index](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/historical-T-member-index.json) | 769 incl.index | `a089c888e6d99b45bfb486e69f321c8917cd0c93c8e483f7591231e465862319` |
| Historical decoder/read/value/negative/memory binaries/inputs | [historical-binaries-and-inputs.tar.gz](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/historical-binaries-and-inputs.tar.gz) / [index](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/historical-binaries-member-index.json) | 147 incl.index | `5c2376f341ff62e156388219ab8491d6149523d1a333c1ef71275b1cae957e1a` |
| Exact original/control/final sources and root freeze proof | [final-source-proof-evidence-local.tar.gz](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/final-source-proof-evidence-local.tar.gz) / [index](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/final-source-proof-member-index.json) | 10038 | `61c73221acd5f528dea23b4fcfdba4195d2f84856b344efbd0cdea4ec25359f2` |
| Final 160/full reduction/all 9 binaries/native 7 | [F-final-b215-raw-freezes-and-ELFs.tar.gz](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/F-final-b215-raw-freezes-and-ELFs.tar.gz) / [index](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/F-final-b215-member-index.json) | 1724 | `a17a130f1235269952598c3583a3a15a4699a8471ce4df427e1f62b51db6345a` |

[Report supplement archive](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/report-supplement.tar.gz) and [exact member index](https://github.com/snissn/gomap/releases/download/treedb-quicksilver-4888-evidence-b215d8bb-20261002/report-supplement-member-index.json) add the complete v 2 projection, projection/reducer sources, independent reviews, coordinator cost disposition and compact navigation data. It is linked from the release; its member index is separate from the already sealed primary packet. No source/proof artifact is replaced to add later analysis.

Full reduction initially rejected inconsistent numeric support because a decimal-looking hexadecimal dictionary identity was parsed as a counter. V 2 excludes only `treedb.cache.vlog_dict.last_applied_dict_hash`; raw identity remains retained. Independent Sol review verified unchanged validation/math and synthetic hexadecimal/decimal/support guards. V 1 failed raw execution and diagnostic remain published. Full v 2 reduction passed; report projection reapplies its exact metric/math functions to hash-bound packets. Independent review recomputed 160,588 summary objects and 120,313 comparisons and matched 484,280 common v 1/v 2 metric values; it did not claim whole 12 GB analysis equivalence. Native module-list and initial seal missing-copy failures remain retained as infrastructure/publication failures, never successful measurements.

Historical actual ELFs with original pre-timer hashes are verified against those pins. Older D/R/M binary exports lacking original hashes are explicitly marked observed-now; retained source/build provenance is not an invented original executable hash. Toolchain/module dependency bytes are identified but not all vendored: this is auditable source/binary/raw evidence, not a self-contained offline rebuild image. Use the smaller projection/navigation for routine reading; the complete full analysis expands to 12.17 GB.

No quiet-host, controlled cold cache, larger-than-RAM scan, sustained RAM ceiling, production p 99.9, Quicksilver protocol/product parity or outreach claim is accepted. The supported result is a reproducible local read/maintenance tradeoff packet with selected fixes, rejected algorithms and concrete deployment-specific revisit boundaries.
