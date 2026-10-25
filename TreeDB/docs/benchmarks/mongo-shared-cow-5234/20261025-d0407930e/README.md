# Standalone Mongo shared-COW qualification at d0407930e

Runtime source: `d0407930ef138915306e1a89c2c80522174c50ad`.
Baseline: landed harness `eb9a4ab4a8e7c2c4ff34b53552b2cdde9a9c7144`, with
product identical to `f906b8a2aa1279ed6b6627028cba71d491db04c3`.
Both binaries use the same seven landed harness blobs, Go 1.26.0 and
GOMAXPROCS=4. [manifest.json](manifest.json) binds source trees, binaries,
commands, environment, pressure-review files and predeclared counts/targets.
The final publication commit may contain only documentation/evidence descendants
of this source; the recorded implementation/harness blobs must remain identical.

All **106 planned fresh processes** are valid: 64 primary, 24 held-reader
resource and 18 ordinary-vector runs. No attempt failed, was retried or excluded.
The independent audits accept provenance, membership and numerical derivation;
the coordinator separately accepts the limited performance tradeoffs below.
This qualifies the first Mongo slice, subject to current-head CI/review/merge.
Parent #5225 and broader collection/document-service adoption remain open.

## Host and sampling limits

No dedicated runner was available. These runs use the disclosed quiet shared
Linux i5-11400F/ext4 host. Graph-owned builds/tests/profiling/checkouts/cleanup
were paused throughout every retained process. One-second preflight and individual
process snapshots support this fallback; they do not establish continuous host
isolation. [host-preflight.json](host-preflight.json) records the grant and
private-snapshot digest. Published snapshots retain numeric process fields and
executable basenames, with full arguments omitted and hashed in the index.
All alternating pair order, fixed operation counts and original gates remain.

## Official-driver indexed updates

Actual standalone openers, BSON, w:1 with j unspecified, retries disabled,
64 seed rows per writer, 256-byte payloads and two secondary indexes are used.
The timer includes all requested ACKs and whole-process driver/gateway
allocations. Setup, calibration and disconnect/Close remain separately recorded.
The interval is not a storage microbenchmark or a comparison across durability
profiles. [independent-primary-review.md](independent-primary-review.md) and
[primary-summary.json](primary-summary.json) preserve every paired result.

Ratios are candidate/baseline; smaller is better. Primary gates are median
paired time <=0.90 for durable/one and <=0.75 for the other three cells, with
bytes and allocations <=0.80 in each cell. All four pass.

| Profile / writers | Time ratio | B/op ratio | Allocs/op ratio |
|---|---:|---:|---:|
| command_wal_durable/writers=1 | 0.894840 | 0.449842 | 0.557976 |
| command_wal_durable/writers=8 | 0.193399 | 0.217860 | 0.235697 |
| command_wal_relaxed/writers=1 | 0.238223 | 0.378966 | 0.566906 |
| command_wal_relaxed/writers=8 | 0.039493 | 0.198942 | 0.262225 |

Median absolute request costs below are computed independently per route; they
are not substituted for the authoritative median paired ratios above.

| Profile / writers | Baseline ns/op | Candidate ns/op | Baseline ACK/s | Candidate ACK/s |
|---|---:|---:|---:|---:|
| command_wal_durable/writers=1 | 5,783,332.5 | 5,161,710.5 | 172.91 | 193.73 |
| command_wal_durable/writers=8 | 6,624,744.5 | 1,276,958.5 | 150.95 | 783.11 |
| command_wal_relaxed/writers=1 | 535,057.0 | 127,478.5 | 1,868.96 | 7,844.46 |
| command_wal_relaxed/writers=8 | 1,151,296.5 | 45,609.0 | 868.59 | 21,925.50 |

Durable/one clears its time target by only 0.516 percentage points; individual
paired ratios span 0.8793–0.9129. The gate is the frozen median, not an every-pair
claim. Durable/eight baseline sync counts vary between 250 and 998–1000 per
measured run; candidate is 250 in all eight. That batching variation is retained.
Primary final candidate Close costs 61.4–70.8ms versus baseline 30.5–68.2ms,
outside the ACK timer. This work is disclosed and accepted as deferred physical
cleanup/publication, not silently charged away; resources and vector reopen
below exercise the longer boundary separately.

## Held-reader resources and rare request tails

All 24 runs complete 10,000 driver updates with 4KiB payloads and one opening
ordinary read view retained through ACK. The harness checks old membership for
every seed ID and the first old payload; it does not revalidate every payload.
Reader release, two seconds of natural idle, and disconnect/Close/GC are observed
separately, without a harness-inserted checkpoint. Full per-phase counters,
absolute sampled maxima, individual pairs and Close costs are retained in
[resource-analysis.json](resource-analysis.json) and
[independent-resource-vector-review.md](independent-resource-vector-review.md).

| Profile / writers | ACK ratio | p99 ratio | Max ratio | HeapAlloc ratio | RSS ratio | Logical file bytes ratio |
|---|---:|---:|---:|---:|---:|---:|
| command_wal_durable/writers=1 | 0.844907 | 0.519993 | 4.012329 | 0.593024 | 0.237811 | 0.042266 |
| command_wal_durable/writers=8 | 0.650053 | 0.505074 | 1.827265 | 0.491369 | 0.436877 | 0.302730 |
| command_wal_relaxed/writers=1 | 0.281207 | 0.074492 | 10.148182 | 0.497504 | 0.231452 | 0.036721 |
| command_wal_relaxed/writers=8 | 0.118186 | 0.079906 | 1.516044 | 0.427399 | 0.410812 | 0.182884 |

**Accepted measured cost:** candidate maxima are **125.136607–167.414703ms**,
versus baseline 12.193177–122.905687ms; every cell has a worse median maximum.
Better p99, ACK time and footprint do not erase that regression. No maximum
parity, absolute SLO or precise tail-confidence claim is made from three pairs.

One separately frozen causal diagnostic uses the exact d040 binary, the same
10,000-operation relaxed/one resource workload and standard Go CPU/runtime
tracing. It completes successfully; profiled timing is not a qualification row.
Its first slow actual client request waits 132.842048ms, including the existing
producer-pressure checkpoint's file/directory sync chain and durable-publication
WaitThrough (63.330496ms). The subsequent fixed-frontier semantic refund join
spans 12.194752ms with 193 tiny condition waits totaling only 1.028993ms; the
remainder includes interleaved scheduling/destruction work and is not a single
12ms blocked wait. runtime.Stack guard CPU is aggregate over the entire profile,
not attributed in full to this request. The largest 167.414703ms qualification
request was not traced individually; its checkpoint counters are correlation,
not exact per-request attribution.

The required physical publication chain, original cleanup worker, fixed refund
frontier and callback reentry guard preserve finite source capacity, held cuts
and Close safety. No demonstrated safe redundant sync or callback omission was
found. Earlier watermark-triggered maintenance was measured and rejected because
it worsened ACK/p99 with 104 handoffs; that failed attempt remains in the
[historical 950 packet](../20261025-950976af8/README.md). Further notification or
guard coalescing is an unproven optimization opportunity, not a reason to remove
the contracts or claim theoretical optimality. The coordinator accepts this
bounded first-slice tradeoff after source/profile review, with the original
durability, limits and broader sustained-performance obligations preserved.
See [promotion-disposition.json](promotion-disposition.json) and the diagnostic
archive's exact trace, raw log, CPU profile and causal review.

Every native run has semantic sources **6→2→0** at ACK-held, reader release and
idle, with retired bytes zero at idle. Reservations and leases remain nonzero
for live maintenance/cache/dictionary owners. The fixed maintenance charge is
256MiB; sampled HeapAlloc/RSS are different metrics. Closed-GC engine statistics
are unavailable, not measured zero. Peaks include all recorded phases and are
sampled, not continuous maxima. Logical file bytes include WAL and are neither
allocated blocks nor WAL-excluded storage certification. Resource Close costs
25.6–32.0ms candidate versus 31.7–83.7ms baseline are separately reported.

## Ordinary public vectors

These use ordinary BSON collection/vector APIs through actual standalone
openers, no_wal_fast, 512 rows, eight dimensions, cosine HNSW/M16, TopK5 and
exact fallback disabled. Three alternating pairs time ten Build/Save/hotLoad
operations, ten Save/physical-Close/reopen/Load/validating-Search operations and
10,000 warmed Search operations. Final cleanup is outside each timer. Individual
Save logical/physical returns are not asserted equivalent; the separate reopen
timer counts physical close/open work. They are not Mongo wire-vector commands.

| Operation | Time ratio | B/op ratio | Allocs/op ratio |
|---|---:|---:|---:|
| BuildSaveLoad | 0.972850 | 0.265996 | 0.407423 |
| SaveReopenLoad | 0.756951 | 0.221036 | 0.895612 |
| Search | 0.327092 | 0.470103 | 1.266667 |

| Operation | Baseline ns/op | Candidate ns/op | Baseline ops/s | Candidate ops/s |
|---|---:|---:|---:|---:|
| BuildSaveLoad | 65,657,828.0 | 64,460,625.0 | 15.23 | 15.51 |
| SaveReopenLoad | 131,595,287.0 | 98,114,816.0 | 7.60 | 10.19 |
| Search | 44,385.0 | 14,564.0 | 22,530.13 | 68,662.46 |

**Accepted allocation cost:** Search uses 19 versus 15 allocations on every run,
with 12,760B versus about 27,143B. The same-cut native read, admitted private
query workspace and independent result ownership preserve lifetime/refusal
semantics. The avoidable indirect-rerank regression to 21 objects was profiled
and removed before this complete fresh campaign. The remaining four-object
delta is explicitly accepted alongside 67.29% lower time and 52.99% lower bytes;
it is not claimed theoretically irreducible. Build/Save/hotLoad time is roughly
unchanged and one pair is 2.84% slower. Three pairs do not support a universal
5% vector timing guarantee. This does not weaken the primary allocation gates.

## Correctness, compatibility and retained failures

The complete collections owning package passes at d040 (488.271s); changed
rerank/lifetime controls and race pass (7.186s/17.843s). Gateway (59.082s) and
harness (4.792s) results remain source-bound to unchanged subtrees. Exact
commands/logs and independent source reviews are archived. Earlier complete
collections failure at e806 is retained: two refusal assertions assumed old
cleanup debt could not retire during legitimate preparation. Test-only repairs
preserve no-added-debt and logical/schema/catalog/incarnation/document/WAL
no-effects, runtime ownership and full-close zero. Pressure controls preserve
initial source-slot exhaustion and the first incomplete NoWAL repair, followed
by the reviewed fixed-frontier worker join and focused/race/public-lock checks.

The complete previous 07d 106-run campaign is retained with original source and
withheld promotion after Search reached 21 objects. Its allocation profiles
show two extra per-query entry/trace escapes caused by indirect dispatch. The
direct helper repair restores 19 objects in all final Search rows. Profiles,
initial errors and all raw rows remain. No historical row is relabeled d040.

Strict external PRIMARY at remote source 102 remains failed and unwaived:
98,788→98,788.5B/op, 372 objects on both routes, time -2.757374292%, with
runtime_input_equivalent=false. All eight pairs/16 raw rows and commands,
bindings, summary and original ZIP digest are archived. This source repair is
not asserted to explain that measurement-contract failure; #5191 retains its
external owner. New-head normal CI and exact-head review remain mandatory.

This is a pre-alpha native-catalog format cutover. Preserve an old directory
and export/reingest with its original binary; startup refuses incompatible
catalog/domain layouts with the original cause. Explicit smaller limits remain
effective. Standalone defaults of 512MiB in-flight/1GiB total-retired are engine
admission limits, not preallocated RAM or RSS caps. Text, document-service,
unconfigured vector defaults, typed/column/partition/normalized/quantized and
distributed adoption remain parent-owned and unqualified by this report.

## Reproduction and archive integrity

Use the landed maintained Go harness and its measurement driver documented in
[the maintained harness README](../../../../../cmd/mongo_gateway_bench/README.md). The frozen per-attempt commands/environment and
operation counts are in the archive's plans/receipts; vector commands were run
directly through the structured operator, not the historical private collector.
The packaging scripts only archive existing bytes and render arithmetic; they
do not generate workloads, adjust counters or retry samples.

[evidence-index.json](evidence-index.json) binds every archive/member and original
private snapshot digest. `raw-d040-all106.tar.gz` contains all 106 byte-identical
raw outputs plus plans/receipts/sanitized host snapshots and audits.
`retained-07d-all106.tar.gz` preserves the complete withheld predecessor with
[retained-07d-index.json](retained-07d-index.json).
`diagnostics-and-controls.tar.gz` includes the exact d040 trace/CPU/raw profile,
allocation diagnosis, source reviews, red/green/race/owning logs and strict102
raw evidence. The 221MB aggregate CI ZIP is retained locally with digest rather
than copied into Git; all primary raw evidence is included here. Historical
950 and earlier attempts remain source-bound in their original packet.
