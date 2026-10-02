# TreeDB representative tradeoff report (#4895)

**Status: provisional layout; awaiting final-source collection and publication.**
No final performance, capacity, shipped-source or current-CI conclusion is
available in this document. `AWAITING` means uncollected or unaccepted, never
zero. Historical candidate packets are context only; they cannot populate the
final tables. [#4888](https://github.com/snissn/gomap/issues/4888) and
[#4895](https://github.com/snissn/gomap/issues/4895) remain qualification gates.

This layout was constructed against reviewed provisional H source
`8c54ef39d98e01922627b0a7fc6c6f1a6c6cc800`. Actual predecessor landing, final
integrated source resolution, a new root-owned Linux grant and independent
preparation anchors are mandatory before official freezes or timers. This
document authorizes no execution or provisional F merge. Final collection and
CI/review/merge decisions belong to the coordinator.

## Source and retained-evidence contract

| Label | Runtime identity | Comparison scope |
| --- | --- | --- |
| original | `ef22ce55b85524de2707453f22c5f8cd6ba89eaa` | Pre-graph product, filtering absent; off only. |
| control | `1f0b09b8ffad0c3eaa1eae0146958f45e57d8c1c` | Maintenance-only control; filtering absent; off only. |
| final-off | AWAITING actual merged final HEAD/full tree/TreeDB tree | Integrated product with opt-in filter disabled. |
| final-on | Same actual source, placement binary and freeze as final-off | Paired opt-in filter enabled; no default-switch claim. |

Original/control retain their exact tracked trees. Only the five landed
canonical additions named in the [runtime-v2 baseline contract](baseline/runtime-v2/README.md)
may be added. The external seven-file bridge supplies API/reader/clock
compatibility and the honest absent-filter schema, not candidate production
changes. Regenerate both path/compiler manifests on the actual Linux host;
the prototype `canonical_head` is historical protocol metadata, not final
landing proof. Absent filter observations remain absent, never synthesized as
zero. Track bridge replacements and canonical source bytes separately.

The construction inputs are the accepted external
`final-representative-run-order-v3.json`, SHA-256
`c41214dc9598834b76c0b7ce146c4c8bcb3705b48e2e3dd6fdb8a9adfb763a6d`,
and `final-representative-existing-helper-launch-readiness.md`, SHA-256
`260ea06880ff1c7437159bd8f1452f3e4775199d8deb7e9dbbbc581d7083e422`,
under the coordinator's retained
`tmp/treedb-quicksilver-graph-20261001/`. The source-only plan has
`execution_authorized_by_this_file=false`. Its original 96-process prefix is
preserved; repeat 4/5 append 64 processes. Actual resolved operational receipts
must retain the proposal and bind every substituted final identity.

| Canonical input | Construction SHA-256; recheck at final freeze |
| --- | --- |
| `TreeDB/memory_budget_bench_test.go` | `03b389e475374af7157111b4a8feea72a22124aa4fec5f3342d512ea6cbad462` |
| `TreeDB/quicksilver_workflow_bench_test.go` | `3796b0c5219a43371026a0f82748f4404baf5380c13246a01afc40722c5a27ec` |
| `scripts/treedb_memory_budget_capture.py` | `4a8c7750475ace639c12f15d13000f8c93602c0074d47523c9938b153f0eead3` |
| `scripts/treedb_memory_budget_overlay.py` | `156abcee681ea4de1366df75e6dfe868ead23a240334f33d331804f529730167` |
| `scripts/treedb_quicksilver_capture.py` | `f98deee21a53c11bfa89cc0423e4ef086abda1f5eea24af5b978160875e5b9ed` |

Awaiting collection/publication ledger:

| Required retained reference | Final record |
| --- | --- |
| Predecessor dispositions/actual merges; final HEAD/full tree/TreeDB tree; current required CI and independent review | AWAITING |
| Two actual Linux baseline manifests, external bridge inventory, SHA-256 and actual compiler/path binding | AWAITING |
| Nine preparation directories, complete before/after compile inputs, Go/module/toolchain/overlay identities, executable hashes and `freeze.json` hashes | AWAITING |
| Independent pre-timer nine-freeze/binary anchor outside prepared directories; unchanged postflight identity | AWAITING |
| Actual serial grant, host/boot/kernel/CPU/RAM/affinity/load/device/mount/free-space observations before/after | AWAITING |
| All 160 ordered `run.json`, `run.stdout`, `run.stderr`, hashes, PID/argv/cwd/start/finish/exit, retained validation and negative-check receipts | AWAITING |
| Complete reduced observations, all five raw values/extrema per metric, math/input hashes and any noise/failure dispositions | AWAITING |
| Separate native/profile/counter/memory diagnostic source, binaries, freezes, commands, environment, raw packets and outcomes | AWAITING |
| Every failed/invalid/infrastructure packet and protected failed DB reference, without replacement or deletion | AWAITING |
| Durable GitHub evidence index and issue/tracker ledger linking complete raw and failed packet manifests | AWAITING |

The proposed exact 20-key base environment is retained in
`full-campaign-environment.json`, SHA-256
`ba36a91dd56993f18b57d179a367bb2f8bdcb95fa34b6a27ef42b13565e3a1a8`:
Go1.26.3, CGO enabled, GOMAXPROCS=2, GOMEMLIMIT=2GiB, GOGC=100,
GOWORK/GOENV=off and GOTOOLCHAIN=local, with declared compiler, HOME/PATH,
cache and temporary paths. Pass the entire mapping without ambient additions;
preparation adds exactly four MEMORY controls and execution four QUICKSILVER
controls. Preserve actual complete maps and execution environment hashes.
Changing temporary filesystem/compiler paths invalidates equivalence.
The historical A/T grant is not an F grant. Paths below are retained locations,
not evidence that collection has happened:
`/mnt/fast4tb/gomap-quicksilver-full-20261002/` and the coordinator's local
`tmp/treedb-quicksilver-graph-20261001/`. Durable publication remains pending.

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

Final off/on are adjacent. Actual launch intervals must demonstrate serial
nonoverlap. Existing [capture helpers](README.md) fix the benchmark to
`BenchmarkQuicksilverWorkflow`, `-test.benchtime=1x -test.count=1 -test.benchmem
-test.v -test.timeout=60m`, retain the package binary and require explicit full
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
| Warm API tails | `read_p99_ns`, `read_p999_ns`, `read_max_ns`, `latency_samples` | ns/owned API request; samples every 17th request. Scalar: 256,000 calls/15,059 samples; batch: 4000 calls/236 samples. Batch p99.9 and sampled max coincide; this max covers samples, not every request. |
| Update ACKs | chronological `update_ack_samples_ns`, count/sum and p99/p999/max | ns/1000-key WriteSync only; excludes batch construction/Set/Close/checkpoints. 40 samples, 10 per update phase; p99 and p99.9 both equal max. No resolved production p99.9/SLO. |
| Update phase cost | `updates_1..4` elapsed/allocation/Stats deltas with operations=10,000 each | ns/key, B/key, objects/key include value construction and concurrent reader activity; not isolated ACK or engine cost. |
| Checkpoint cost | `initial_checkpoint`, `checkpoint_1..4` elapsed/allocation/Stats | ns/checkpoint, B/checkpoint, objects/checkpoint. **checkpoint_4 includes reader stop/join and quantile/report synchronization**; do not interpret it as isolated checkpoint service time. |
| Concurrent owned reads | `concurrent_owned_reads` raw `samples_ns`, reads/elapsed, p99/p999/max | One present-only scalar Get reader, even-key permutation7919, generation0-or1 full-byte proof; independent of warm miss/distribution/batch settings. Quantiles sample every17 requests; max covers all successful reads. Elapsed includes four updates/checkpoints plus join/report overhead. |
| Residency | phase `heap_bytes`, `post_reopen_gc_heap_bytes`, owner Stats and RSS/HWM | Bytes; direct HeapAlloc after one runtime.GC with reopened DB still open. Not ValueLogGC, callback-retained memory or continuous peak heap. RSS/HWM and periodic Stats peaks are process observations. |
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

## Results awaiting collection

Each completed row links all 20 observations and their validation receipts.
The compact throughput table reports five-process median keys/s per label;
adjacent linked metric tables must retain every metric defined above, with
five raw observations, median, min, max and spread for each cell/label.

| Cell | Original keys/s | Control keys/s | Final-off keys/s | Final-on keys/s | Complete tails/allocation/residency/work/storage evidence |
| --- | ---: | ---: | ---: | ---: | --- |
| c01-inline-hit | AWAITING | AWAITING | AWAITING | AWAITING | AWAITING |
| c02-pointer-hit | AWAITING | AWAITING | AWAITING | AWAITING | AWAITING |
| c03-balanced | AWAITING | AWAITING | AWAITING | AWAITING | AWAITING |
| c04-miss-heavy | AWAITING | AWAITING | AWAITING | AWAITING | AWAITING |
| c05-miss-extreme | AWAITING | AWAITING | AWAITING | AWAITING | AWAITING |
| c06-skew | AWAITING | AWAITING | AWAITING | AWAITING | AWAITING |
| c07-natural-many | AWAITING | AWAITING | AWAITING | AWAITING | AWAITING |
| c08-random4k | AWAITING | AWAITING | AWAITING | AWAITING | AWAITING |

Reuse existing owner-analysis math: spread=`100 × (max−min)/abs(median)`;
zero median gives undefined, not zero spread. Above 10% remains an unresolved
noise flag. Median change=`100 × (candidate median−reference median)/abs(reference
median)`, retaining all five paired differences/changes separately. State the
reference and beneficial direction: positive throughput and negative cost.
Compare original/control separately from control/final-off and paired
final-off/on; maintenance changes prevent automatic read-mechanism attribution.
Do not pool request samples across processes into a production tail estimate.
Per-process empirical quantile uses sorted index
`min(int(n × fraction), n−1)`; report medians/ranges of those observations.
Failed/invalid cells stay in the completeness ledger; no favorable-repeat
selection or silent reduction. Additional collection needs a new matched plan.

## Final native integrity and maintenance gates

All entries below await fresh exact-final-source execution, outside primary
fixtures/timings. Historical #4886 evidence is not relabeled as final evidence.
The existing external diagnostic is pinned to SHA-256
`a578806fe8b454e8e8fcfe79f1c0b2344612dfbb58201c77accd92612c030726`.
Use the reviewed Go overlay for its absent diagnostic file in a separate clean
native source directory; retain tagged compile-input/binary/environment freezes
before/after. Do not add a sixth source file to either baseline or relax guards.

| Gate | Required result | Final receipt |
| --- | --- | --- |
| Fresh native fixture | Existing `TestQuicksilverLocalEval`: 100,000 even keys, PCG random 4096 B values, CommandWAL, batches of 1000, 40,000 updates/checkpoints; closed cached owner before exclusive maintenance | AWAITING |
| Two actual CLI passes | Exact final treemap binary: `compact <DB> -rw -mode exhaustive -sync-each-phase -json`; preserve each raw result/exit, vacuum/debt and ByteMinimized | AWAITING |
| Read-only after each CLI pass | Existing `TestQuicksilverVerifyRetained` with QS_VERIFY_DIR: every final value byte for 100,000 keys and 10,000 declared odd misses; independent of compact exit | AWAITING |
| Separate public API/GC | `TestCompactStorageExhaustiveCommandWALRandom4KOffline/keys100000`: two exhaustive passes plus explicit ValueLogGC/LeafGenerationGC and fresh read-only oracles/checked closes | AWAITING |
| Stable identity/pinned deletion | `TestValueLogGC_StableIdentityPinDefersUnreferencedSegmentDelete`, `TestValueLogGC_HealthMetadata_PreservesPinnedZombieSegment`, `TestLeafGenerationGC_RetiresPinnedGenerationUntilSnapshotCloses` | AWAITING |

Both CLI exits must succeed; first vacuum must succeed, second succeed or be
not-required. Preserve `ByteMinimized=false` and remaining live/pinned/recovery
debt honestly: exit 0 and full read-only integrity do not prove byte minimization.
The original diagnostic's Linux Maxrss has a misleading bytes label; retain raw
KiB and publish the explicit ×1024 conversion separately. Its deferred Close is
not checked-close proof; the API test supplies a separate checked-close seam.
Never clean protected historical/failed DBs to make the gate pass.

The [persistent value-log lifecycle](../../../TreeDB/docs/spec/value-log-lifecycle.md)
and [durability contract](../../../TreeDB/docs/spec/write-path-and-durability.md)
remain mandatory. Pointers survive checkpoint/reopen; referenced or pinned
segments must remain readable and segments are removed only through verified
reachability/rewrite lifecycle, never age. Public correctness, default checksum
integrity, acknowledged LSN coverage, full tests and required current CI/review
are separate acceptance gates, not substitutes for performance evidence.

## Candidate choices, attribution and open disposition

| Mechanism | Existing decision/evidence | Final attribution gate |
| --- | --- | --- |
| Decoder allocation reuse | [Decoder qualification](../treedb_decode_reuse_20261001/README.md) | AWAITING final owned-read tradeoff; no inference from internal decoder-only cost. |
| Owned value transport/copy and bounded caching | [Owned-value qualification](../treedb_owned_values_20261001/README.md) | AWAITING final physical placement, CRC/public fallback/ownership and residency. |
| Negative probes | [Opt-in filter qualification](../treedb_negative_lookup_20261002/results.md) | AWAITING paired final off/on, actual support/bytes, work/fallbacks and load/rebuild cost. Default remains a separate policy decision. |
| Owner budget | [Retained-owner decision](../treedb_memory_budget/OWNER_DECISION.md) | Existing policy selected; free-bin trial rejected, no inherited memory saving. AWAITING final residency guards. |
| Read guards/traversal/coalescing/pointer view | [Algorithm decision](../treedb_algorithm_work_20261001/ALGORITHM_DECISION.md) and [snapshot decision](../treedb_algorithm_work_20261001/SNAPSHOT_DECISION.md) | Existing defaults/rejections/nonactivation are bounded dispositions. Selected pointer-view efficacy/memory requires its separate source-bound GetManyView packet; H Get/GetMany owned reads cannot proxy that callback mechanism. |

Final objective ledger: decoder allocation **AWAITING**; guard/handle work
**AWAITING**; negative-probe avoidance **AWAITING**; owned transport/bounded cache
**AWAITING**; write/checkpoint/reopen and memory non-regression **AWAITING**;
native maintenance/pin integrity **AWAITING**; complete durable publication and
current CI/review **AWAITING**. Missing objectives or hard-guard failures retain
named blockers and issue links; unmeasured or zero eligible work cannot establish
NO-GO. Any attribution requires native eligible work and equivalent public
output/ownership lifetimes; static inline/pointer or cache/filter choices alone
do not establish causal optimization efficacy.

Quiet-host confidence is **untested** until actual infrastructure provides it.
Dedicated quiet infrastructure was unavailable in the construction plan:
`INFRASTRUCTURE_UNAVAILABLE` permits only the explicitly granted, disclosed
shared-Linux fallback with actual load observations and serial graph work.
OS page cache is uncontrolled and warm. Cold-device, larger-than-RAM, scan,
capacity/saturation and production tail/SLO claims remain **untested**. A
configured 64 MiB budget/GOMEMLIMIT=2GiB is not a measured capacity bound. This
bounded local public-engine workflow does not reproduce Cloudflare replication
or establish service/replacement equivalence. No outreach/default switch or
production recommendation follows from this pending report.

## Filling this report with existing seams

Use H `load`/`check_record` and `validate --retained --negative-checks` for each
actual frozen run; use the baseline bridge's own source/absence validators for
original/control. Retain raw stdout/stderr and complete execution records first.
Existing external `memory-full-analysis-runtime-analyze.py` supplies reusable
`storage`, `numbers` and `spread` calculations; its T60 entrypoint, fixed hashes,
GC1/GC2 phase assumptions and `metrics` function are not F validators.
Read F's actual phase objects/post-reopen heap and the explicit fields above
without inventing GC phases. The existing paired analysis `compare` calculation
in `checkpoint-paired-4915-analyze-v2.py` provides raw paired deltas and ratios
of medians; its historical campaign entrypoint is not an F reducer. No new
framework is needed.
Final data filling must hash-pin the accepted inputs/reduction and reconcile
all 160 records with declared order, source/argv/env/host and nonoverlap before
publishing tables. Link complete reduced/raw/failed artifacts durably, retain
unresolved noise and guards, and update the objective/tracker ledger only after
the coordinator accepts the exact final-source packet.
