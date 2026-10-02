# Algorithm physical-work decision (#4893)

This draft retains existing traversal and coalescing defaults. Root rejected the
shared-traversal product after its full uniform guard failure and unsuccessful
final bounded screen. The eligible public snapshot workload supports a
checkpoint-only work reduction with wider limits, but its sampled read-tail
guard remains unresolved. Persistent deltas have not been implemented or tested;
root's accepted bounded decision is **do not activate a storage-format change for
this measured workload yet**. This is an activation decision, not proof that
deltas are uneconomic or that the unmeasured capacity requirement has passed.

[#4893](https://github.com/snissn/gomap/issues/4893) remains open. This provisional
integration carries T/A decision components in [H #4913](https://github.com/snissn/gomap/pull/4913)
under root's published graph reassessment. [Selected pointer #4921](https://github.com/snissn/gomap/pull/4921)
actually merged at `af2d4003dc54eb491725d44383ab68264b4f4639`, from reviewed head
`dacff6cde293e995e13a5b63fad9b266dd9a79b0` on main
`c6b95440058d7f46bfc094da35cf293749c8b5bd`; all49 current checks passed with no
unresolved threads or formal change requests. The measurements retain candidate
`75fe577ecc6ab81237df2cff58454bbeb205cc71` against control
`bc9764ab92a9ceeeb4ae15706c920653e5dd8bbc`. Its accepted five-pair
objective/noise/owned-tradeoff disposition is recorded below; the measured
executables are not relabeled as merged binaries.

Snapshot decision [#4931](https://github.com/snissn/gomap/pull/4931) actually merged
at `f9152b5b64dd0e38f5bd5a46bdfbe36e1db10775`; defaults remain selected. Root
[accepted bounded persistent-delta nonactivation](https://github.com/snissn/gomap/issues/4893#issuecomment-5956567233)
for this workload. That decision does not establish delta efficacy, economics or
unmeasured capacity. Parent #4888, final A/union review, final-head H #4894 CI,
and representative/maintenance/capacity #4895 gates remain active. Original
artifact PRs #4930/#4932 remain open until actual integrated merge is verified.

## Evidence identity and interpretation

The primary packet is actual landed runtime
`a51b4795cebc194c7a158b262242668877932934`, TreeDB tree
`aef21fdda5a38e3cb4a129c0848c03f42a893f83`: three full write captures (24 cells),
three full natural GetMany captures (36 cells), and twelve untimed actual
internal-load cells. Captured normal binary identity is
`bb190f318f0a10f22c50d2f92a094b28cb35e2a3f44d734da42551b6777ec543`.
The recorder freezes actual source/dependencies/toolchain/environment and checks
them before/after each process. Actual retained primary normal and counter executable bytes are independently
rehash-bound to their respective original freezes; counter binary SHA-256 is
`390367fa96037d46282795fe61ba7baf643ad98fbd697930fe273d9c0bb070ab`.
The checker also rehashes the retained 732 normal/counter executable bytes and
reparses all ten full five-pair normal captures plus both untimed diagnostics,
including every original and extension raw-file hash in the canonical analysis.

The retained captures use Go 1.26.3 linux/amd64, GOAMD64=v1, CGO enabled,
GOWORK=off, GOMAXPROCS=2, GOMEMLIMIT=2GiB, GOGC=100, normalized environment,
250,000 keys, 40,000 distinct updates, 32-byte keys and 256-byte values.
Every 1,000-operation public synchronous CommandWAL batch remains in the interval;
four versus one checked checkpoint changes explicit durability/publication
cadence. Full byte/miss oracles, acknowledged LSN coverage, checked final close
and reopen are retained. Native timers are serial under root's grant on an
explicitly shared host; cold OS cache/quiet-host confidence is not asserted.

Tables report three-run primary medians with `(max−min)/abs(median)` spreads.
Allocation deltas include foreground/reader work, not engine-only allocations.
Post-reopen one-GC heap is not retained peak heap or RSS. Old-node/page bytes are
logical physical-work counters, not device I/O. Kernel `write_bytes` is separate
process storage-layer accounting, not total device writes; WAL/persistent logs
and logical/allocated filesystem debt remain separate owners. Stats are
before/after differences, excluding load. Cumulative maxima and last pressure
values are not interval distributions. All primary controls remain visible,
including noise and guard failures; no favorable-repeat selection is performed.

## Measured sparse-update cost and bounded delta decision

Inline CP4 loads 40,000 old leaf-log nodes (163,840,000 leaf bytes) plus 972
pager nodes, totaling 40,972 old nodes/167,821,312 bytes, or 4,195.533 bytes/update.
The primary inspection historically calls that total `old_leaf_loads`; the table
uses its actual source meaning, **all old-node loads**. Pointer CP4 loads 7,408
old leaf-log nodes/30,343,168 bytes, or 758.579 bytes/update. Both are actual
read/decode work, not a hypothetical write amplification ratio.

With unchanged default coalescing limits, CP1 reduces inline old leaf-log loads
to 17,858 and total old-node bytes to 74,166,272; pointer CP1 reduces them to
1,854 leaf-log/1,884 total nodes and 7,716,864 bytes. Inline interval median changes
1,442.529→678.130 ms, checkpoint 1,162.245→426.265 ms; pointer interval changes
1,231.885→803.221 ms, checkpoint 514.101→183.587 ms. These are explicit checkpoint
cadence/placement tradeoffs, with their own acknowledgement, read, allocation
and residency costs. They are not an engine-only delta win or a universal safe
cadence recommendation. Inline CP1 timing spreads are substantial.

Native span apply/sorting and baseline coalescing already perform the bounded
merge; there is no need to invent a duplicate span implementation/controller.
All 24 primary cells have zero admitted extra tables/bytes/ops/runs, despite
ordinary automatic hardware-aware admission. At GOMAXPROCS=2 the ordinary
baseline is 32 coalescible units and 256MiB at the primary 64MiB flush threshold;
the separate existing 1MiB diagnostic has a 32-unit/64MiB baseline. Its eight
full cells also admitted zero extra work. Widening caps was unexercised in both
cohorts and cannot be credited with primary speed differences or rejected as a
general mechanism. Queue barriers, frontier, age and real backend pressure
remain relevant; no mocked pressure or admission override is throughput proof.

A persistent delta layer would replace eager old-leaf rewrite with durable
per-key changes linked to a captured base, then pay overlay lookup/merge, replay,
fold and retention costs. The packet measures the eager work but does **not**
measure that implementation or isolate old-leaf CPU domination. The separate
pointer/CP1 diagnostic profile includes load, reader, verifier and cleanup;
its `algorithmVerify` and cached `Get` CPU cannot be reassigned to flush cost.
No claim of measured delta regression, recovery success or solved capacity
follows. The bounded reason to defer activation is that existing placement and
cadence materially reduce this measured cost, while stable net benefit of a new
persistent owner is unmeasured. If root requires a tested delta economic
comparison to satisfy #4893, this remains an explicit open decision gate.

**Concrete activation/revisit trigger:** first demonstrate on the representative
public workload, after eligible existing span/baseline/frontier budgets and
acceptable checkpoint cadence, that old-leaf read/decode/build work remains the
largest attributable write cost (declare the attribution and noise policy
before capture). Require a bounded proposal with at least 15% stable equivalent
public interval improvement in three matched fresh-process pairs, agreeing
paired direction and extension to five for >10% spread, without material
acknowledgement/read-tail/allocation/uniform/pointer regression. This is a
proposed future activation gate, not a passed gate or a new automated CI test.
Declare the retained/peak/RSS/pinned-generation debt bounds before collecting;
include fold/checkpoint/replay and maintenance cost at equal logical work,
memory/runtime/durability, rather than moving that work outside timing. Update
parent edges before an implementation child starts, then execute every
activated implementation within the graph. Capacity/historical #2771/#2922
requirements remain separately open and are neither adopted from old reports
nor waived by this bounded fixture.

## Eligible coalescing supplement: retain defaults

The real public snapshot-rotation fixture landed at
`e3e912be5501083b9ccb6ae32761bc789cfb21c0`. It performs checked WriteSync → batch
Close → AcquireSnapshot → owned boundary Get/full-byte check → snapshot Close
for each batch, retaining snapshot work in the interval. It accumulates no
snapshot pins and supplies no private queue/pressure override.

The independently accepted eligibility/matched packet has one eligibility
capture excluded from three matched rounds. Only inline CP1 expands beyond
baseline: default/wide extra 32/56 tables and 8,000/14,000 operations, identical
in all three matched pairs. All admitted runs are checkpoint admissions;
background admissions are zero. The other six cells remain ineligible.

Inline CP1 default→wide total old nodes 41,214→36,831 (−10.635%), leaf output
bytes 163,840,000→146,870,272 (−10.358%), interval median
1,296.819→1,147.486 ms (−11.515%), checkpoint 839.917→648.364 ms (−22.806%),
process allocated MiB 87.956→77.549, and post-reopen GC MiB 153.652→153.662.
Sampled read p999 is 65.068→76.964µs (+18.282%), with paired −22.03/+24.98/+20.94%
and only roughly four to six observations in the extreme 0.1% per cell.
This establishes neither a cleared tail guard nor a causal tail-regression
mechanism; retain defaults, as root and independent review conclude.

The separately reviewed `SNAPSHOT_DECISION.md` and its existing
`check_snapshot_decision.py` own the complete eight-cell, 48-cost-row and
six-physical-row arithmetic. This draft does not duplicate or edit those files.
It is landed at the actual merge above; its canonical checker remains an integrated
validation gate. Background benefit additionally requires
positive `admitted_runs − checkpoint.admitted_runs` under unchanged age/pressure
policy; checkpoint-frontier expansion cannot establish that claim.

## Shared traversal: measured rejection and finite exit

Original actual internal diagnostics show inline sorted/clustered 24,576 loads
over 128 naturally supplied 64-input batches versus potential unions 394/400.
The potential union is a lower bound, not an implemented win. Separate original
clustered CPU attribution placed per-key `findLeafRefForGetMany` at 45.0% of
owned tree-batch and 35.4% of callback tree-batch CPU, sufficient to activate
#4917 under its predeclared both-mode latency/load and guard contract.

Full candidate `732b175ee2e2d1e979e03e08bab0bfa8d80e5c92` versus landed a51,
including the matched five-pair extension, reduces sorted/clustered actual
internal loads approximately 98%. Sorted owned/view medians improve
32.685%/37.597%; clustered view improves 19.884%. Clustered owned has one slower
pair and 26.39% candidate spread. Uniform inline owned/view worsen
9.180%/9.069%, and pointer uniform medians also worsen. Required output and
consumer semantics remain equivalent, with unchanged allocation counts;
internal-load reduction cannot waive those guards. Retained/peak/RSS
qualification is missing, not zero.

Diagnostic candidate uniform profiles locate approximately 1.50/1.52s of
planner CPU, mostly interval expansion (approximately 1.29/1.34s). Historical
a51 control profile samples have approximately 1.00/0.91s per-key descent.
These are unpaired attribution samples, not causal wall-time ratios. One final
bounded singleton experiment was authorized, keeping checked loads, full
planning before callbacks, fresh Node decode state and lifetimes unchanged.

Final singleton `6b7e468caaba4be83ca3eaf63bcf3c0d37875daf` versus synchronized
732-runtime control `90f824a00f41c0f3ae58db57a94cb0b695fa0fad` uses current-e3
harness and two normal binaries. Five predeclared alternating adjacent pairs
cover all 120 **8192-key pilot** cells. Uniform inline owned/view medians change
only −0.4497%/−1.0753%, within spreads and with one reversed pair each. First-pair
pointer callback B/op rises 27.263%/31.857%/29.810% for clustered/sorted/uniform.
All failures and noisy rows are retained. This screen compares two planner
products, not selected main, and cannot be multiplied into the earlier full
comparison. No fourth tuning screen or full qualification of this failed
screen is authorized. The full backend normal180s timeout remains unresolved
with its raw failure retained; source review was not runtime acceptance.

[Root's final disposition](https://github.com/snissn/gomap/issues/4893#issuecomment-5955658007)
rejects unmerged PR #4919 and retains current per-key traversal/leaf grouping.
#4917 closure waits for a reviewed decision artifact; this draft does not close
it. Revisit only with materially new source/profile evidence removing measured
sparse interval cost, then predeclared equivalent both-mode uniform guards and
original full current-main >=15% sorted/clustered inline latency, >=90% actual
locality-load reduction, and all pointer/owned/inline/bytes/count/retained-peak
residency/lifetime/recovery guards. A nominal structure/load win is insufficient.

## Design preservation and source seams

No runtime or storage-format change is selected here. Current source keeps
[per-key checked traversal and grouped materializers](../../../TreeDB/tree/tree.go),
[public batch ownership contracts](../../../TreeDB/docs/spec/contracts.md),
[CommandWAL acknowledgement/checkpoint separation](../../../TreeDB/docs/spec/write-path-and-durability.md),
[recovery](../../../TreeDB/docs/spec/recovery.md) and
[persistent value-log lifecycle](../../../TreeDB/docs/spec/value-log-lifecycle.md).
A future persistent delta proposal must explicitly preserve:

- Point and scan merge precedence across mutable/immutable cached changes,
  durable base and deltas, deletes, revisions and range spans. Reads must use a
  coherent captured root/cutoff, not a newer global suffix under an old snapshot.
- Snapshot active-read and physical-generation pins. Base/delta resources remain
  reachable for old snapshots/iterators and every recovery-selectable root until
  their owners close; ordinary value pointers remain persistent references.
- Public synchronous acknowledgement of command frames before cached visibility,
  contiguous accepted/applied LSN coverage, replay idempotence and rejected or
  ambiguous publication. Checkpoint must fold/publish only its frozen accepted
  prefix, durably close that root and resources before covered-log cleanup.
- Reachability-based GC and durable physical file identities. Delta compaction
  cannot delete by age or confuse WAL redo with persistent data. A format proposal
  needs storage/recovery/vlog spec edits plus crash/reopen/fold/GC coverage.
- Owned Get/GetMany independent copies, nil misses versus non-nil empty values,
  duplicates/input indexes, callback errors and callback-scoped read-only views.
  Whole-call fallback must complete before callback emission; integrity checks,
  root bounds/depth/ref validation and release on every error remain mandatory.

Existing seams/tests include `TreeDB/algorithm_work_bench_test.go` and
(including `TestAlgorithmWorkFixture`, `TestAlgorithmWorkBatchConsumer`, `TestAlgorithmWorkAcknowledgedReplay` and public fixture/oracle/callback checks),
`reopen_verify_test.go`, `recovery_spec_test.go`,
`command_wal_public_test.go`, `command_wal_durable_prefix_test.go`,
`db/command_wal_recovery_test.go`, `db/snapshot_read_lifetime.go`,
`db/vlog_gc_test.go`, and `getmany_view_test.go`, `db/getmany_test.go` and `caching/getmany_view_test.go`. They are preservation
seams, not evidence that a persistent delta layer passed tests. The rejected
planner's source/correctness reviews and raw timeout are retained separately;
this documentation change executes no Go tests or native capture.

## Verification and remaining acceptance

Run the focused stdlib checker from the repository root:

```sh
python3 docs/benchmarks/treedb_algorithm_work_20261001/check_algorithm_decision.py --evidence-root /path/to/retained/evidence
```

It reuses the existing capture `validate_log` for primary write/many/counter raw
streams and the existing paired reducer's pure `spread`/`compare` arithmetic.
It verifies exact artifact-name/hash rows and tables, including every shared
and singleton cell, without invoking recorder/native code. The shared five-pair table is rebuilt from all original/extension raw sample
arrays using the canonical validator and arithmetic; singleton canonical
analysis/independent review binds its raw provenance;
this checker does not claim to re-run their Linux live-freeze validations.
Optimized Python fails before document/evidence I/O; all text is explicit UTF-8.
The raw packets, freezes, pre-timer order, normal/counter separation and profile
exclusion remain the authoritative provenance contract.

Final integration imports the verified actual #4921 merge. The provisional A and
whole-union source review is accepted; final-field/base applicability, integrated-head
CI/review inventory and unchanged parent
capacity/maintenance/representative obligations. Snapshot landing and bounded
delta nonactivation are recorded above; no pending acceptance is supplied.

## Selected pointer callback: five-pair evidence and actual landing

The selected actual-main candidate uses existing verified append readers and the
bounded callback destination/frame cache; no rejected shared traversal ships here.
Ten normal full-family captures retain all120 cells plus two separate counter
captures (24 cells). Original rounds1–3 and predeclared complete rounds4/5 are
retained, not replaced or filtered. Sorted/clustered pointer callback median
latency changes are −60.357/−57.175%; every one of five paired changes exceeds the
25% improvement target. Median B/op is 271,574/263,490→776 and allocs/op175→7,
exceeding the50% allocation target. Uniform pointer callback median latency is
−45.923%, with912B/op. Actual traversal counts stay unchanged; this is frame
reuse/allocation evidence, not a shared-tree traversal win.

The full twelve-cell table below retains every pair, spread and flag, including
inline uniform owned +1.285% median with mixed directions. Shared-host latency
spreads around10–21% remain visible. These observations support no stable exact
latency, quiet-host/cold-cache guarantee, or owned/inline performance-win claim.
Independent full actual-evidence review and root's bounded objective/noise/owned
tradeoff disposition are accepted; the retained raw gate is >=50% callback bytes/count reduction, >=25% sorted/clustered
callback latency improvement with directional agreement, plus all public safety,
uniform/owned/inline and retention/peak guards. Actual merged identity is recorded above.

[Root's accepted disposition](https://github.com/snissn/gomap/issues/4920#issuecomment-5957915606)
retains the small owned-read tradeoffs explicitly: pointer uniform owned median
latency +2.371%, four of five pairs higher, worst +7.93%; median bytes +100B/batch
(+0.274%), paired +32/−17/+100/+60/+126B and11 allocations unchanged. Inline uniform
owned median +1.285%, four of five pairs higher, worst +2.71%, median bytes/count
unchanged. Pointer clustered/sorted owned medians are +1.931/+0.460%. Root accepts
these bounded observations alongside the all-five targeted gain and separate
residency guard; they are not erased, called causal, or labelled no regression.
The predeclared >10% spread rule was met by retaining all original three rounds
and extending the complete family to five. All20 flags remain. Slowest candidate
versus fastest control is still sorted −56.50%, clustered −52.15%; these are
observations, not confidence bounds or replacement thresholds. No precise stable
inline/owned, cold-cache/SLO or capacity claim is accepted.

Independent accepted full-evidence review rechecks all129 original files, all12
raw captures, four actual ELF binaries/freezes, all36 summaries and20 flags.
The historical root source/base audit records mature candidate
`c6609e48a02535fa34e4062be9b42673a1781ead` on actual maincba. The final audit and
independent applicability review bind `dacff6cde293e995e13a5b63fad9b266dd9a79b0`
on actual mainc6b, preserving exact measured75f production/tests and canonical
harness/capture bytes; unrelated main changes affect only power-loss-oracle tests.
The actual merge receipt binds landed `af2d4003dc54eb491725d44383ab68264b4f4639`.
H imports that landed runtime by merging main. The retained measurements and
executables stay at75f/bc; source applicability is not fresh final-binary measurement.


Separate three-pair forced-GC residency diagnostics used two distinct binaries,
not primary timing executables. Root and independent review accepted zero material
paired HeapAlloc/approximate phase-end VmHWM increases, using max(4MiB,15% of
matched control) per pair. All288 cache observations stay within64MiB, maximum
63,917,824B. The table retains all12 cells, each with three matched pairs.
One explicit pre/post GC site keeps DB/batches live, full-byte/duplicate/miss and
callback verification, enabled CRC, checked Close and Linux reset-phase VmHWM.
This is an approximate observed phase guard; it is not exact peak, global capacity,
RSS ceiling, object-cause attribution or primary latency evidence. The analysis's
historical pending-acceptance label is retained unchanged; later independent
review and root disposition in the artifact rows are the acceptance authorities.

The checker rehashes all four actual normal/counter selected executables and both
separate diagnostic executables against their original freezes, reparses the
120normal/24counter cells with existing canonical validation, and reconstructs
selected tables with the same retained reducer. It hash-binds every separate
residency raw stream/sidecar, reuses only the existing recorder's pure packet
validator through AST selection (no native entry point), and recalculates every
paired heap/HWM guard. The original primary/shared/singleton contracts are retained.

## Canonical retained tables

<!-- canonical-tables:start -->
| Primary cell | Leaf-log loads Δ | All old-node loads Δ | Old-node bytes Δ | Leaf output bytes Δ | Prepare spans Δ | Old-node B/update | Interval ms (spread) | Ack ms (spread) | Checkpoint ms (spread) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| pointer=1/CP=1/wide=false | 1854 | 1884 | 7716864 | 7593984 | 1854 | 192.922 | 803.221 (7.56%) | 616.061 (7.45%) | 183.587 (8.10%) |
| pointer=1/CP=1/wide=true | 1854 | 1884 | 7716864 | 7593984 | 1854 | 192.922 | 765.977 (2.44%) | 581.401 (1.43%) | 182.303 (6.35%) |
| pointer=1/CP=4/wide=false | 7408 | 7408 | 30343168 | 30343168 | 7408 | 758.579 | 1231.885 (16.32%) | 714.555 (20.31%) | 514.101 (10.95%) |
| pointer=1/CP=4/wide=true | 7408 | 7408 | 30343168 | 30343168 | 7408 | 758.579 | 1106.575 (11.86%) | 606.627 (14.33%) | 498.409 (9.28%) |
| pointer=16384/CP=1/wide=false | 17858 | 18107 | 74166272 | 73146368 | 17858 | 1854.157 | 678.130 (21.73%) | 247.739 (24.59%) | 426.265 (20.11%) |
| pointer=16384/CP=1/wide=true | 17858 | 18107 | 74166272 | 73146368 | 17858 | 1854.157 | 691.194 (11.42%) | 250.752 (21.75%) | 425.975 (7.84%) |
| pointer=16384/CP=4/wide=false | 40000 | 40972 | 167821312 | 163840000 | 40000 | 4195.533 | 1442.529 (2.32%) | 276.202 (7.96%) | 1162.245 (4.74%) |
| pointer=16384/CP=4/wide=true | 40000 | 40972 | 167821312 | 163840000 | 40000 | 4195.533 | 1442.095 (8.15%) | 274.557 (16.95%) | 1163.367 (6.12%) |

| Primary cell | Process allocated MiB (spread) | Process allocations (spread) | Reopen GC MiB (spread) | Read p99 µs (spread) | Read p999 µs (spread) | Stride-16 samples range | Kernel write MiB Δ (spread) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| pointer=1/CP=1/wide=false | 98.244 (1.52%) | 332702.000 (4.99%) | 219.785 (0.04%) | 17.099 (0.92%) | 31.906 (39.11%) | 6438–6793 | 6.645 (0.00%) |
| pointer=1/CP=1/wide=true | 90.490 (3.72%) | 324876.000 (3.47%) | 219.925 (0.02%) | 17.306 (2.45%) | 56.277 (105.94%) | 6268–6508 | 6.645 (0.00%) |
| pointer=1/CP=4/wide=false | 127.812 (10.39%) | 474668.000 (21.63%) | 214.252 (1.62%) | 16.620 (21.89%) | 29.303 (57.33%) | 7674–9857 | 15.812 (0.00%) |
| pointer=1/CP=4/wide=true | 133.076 (6.18%) | 444513.000 (16.46%) | 213.043 (1.33%) | 16.601 (14.69%) | 27.270 (14.44%) | 8206–9766 | 15.812 (0.00%) |
| pointer=16384/CP=1/wide=false | 99.515 (5.52%) | 392442.000 (16.87%) | 144.248 (0.02%) | 12.658 (10.30%) | 20.896 (13.42%) | 7627–9002 | 18.230 (0.00%) |
| pointer=16384/CP=1/wide=true | 96.159 (8.36%) | 374651.000 (14.51%) | 144.190 (0.07%) | 12.858 (10.35%) | 18.148 (44.13%) | 7945–9059 | 18.230 (0.00%) |
| pointer=16384/CP=4/wide=false | 179.745 (2.44%) | 846853.000 (6.14%) | 131.353 (0.04%) | 13.907 (1.50%) | 20.105 (18.43%) | 17055–18272 | 27.934 (0.00%) |
| pointer=16384/CP=4/wide=true | 165.981 (4.20%) | 841584.000 (5.71%) | 131.332 (0.05%) | 13.853 (3.67%) | 19.477 (11.73%) | 16938–17933 | 27.934 (0.00%) |

| Primary natural cell | µs/64-input API+consumer (spread) | B/op (spread) | allocs/op (spread) | Actual loads/128 calls | Potential union | Repeated |
| --- | --- | --- | --- | --- | --- | --- |
| pointer=false/clustered/view=false | 39.106 (7.44%) | 35944.000 (0.02%) | 9.000 (0.00%) | 24576 | 400 | 24176 |
| pointer=false/clustered/view=true | 33.856 (1.08%) | 776.000 (0.00%) | 7.000 (0.00%) | 24576 | 400 | 24176 |
| pointer=false/sorted/view=false | 33.976 (2.75%) | 35944.000 (0.00%) | 9.000 (0.00%) | 24576 | 394 | 24182 |
| pointer=false/sorted/view=true | 31.267 (1.91%) | 782.000 (0.77%) | 7.000 (0.00%) | 24576 | 394 | 24182 |
| pointer=false/uniform/view=false | 100.463 (1.33%) | 35950.000 (0.03%) | 9.000 (0.00%) | 24576 | 7593 | 16983 |
| pointer=false/uniform/view=true | 90.174 (1.58%) | 780.000 (2.69%) | 7.000 (0.00%) | 24576 | 7593 | 16983 |
| pointer=true/clustered/view=false | 145.682 (1.62%) | 36462.000 (0.03%) | 11.000 (0.00%) | 16384 | 257 | 16127 |
| pointer=true/clustered/view=true | 331.373 (3.28%) | 265858.000 (0.89%) | 175.000 (0.00%) | 16384 | 257 | 16127 |
| pointer=true/sorted/view=false | 143.348 (0.65%) | 36462.000 (0.03%) | 11.000 (0.00%) | 16384 | 258 | 16126 |
| pointer=true/sorted/view=true | 336.675 (2.66%) | 274068.000 (0.90%) | 175.000 (0.00%) | 16384 | 258 | 16126 |
| pointer=true/uniform/view=false | 207.592 (4.68%) | 36462.000 (0.36%) | 11.000 (0.00%) | 16384 | 3088 | 13296 |
| pointer=true/uniform/view=true | 401.225 (6.78%) | 272003.000 (0.60%) | 175.000 (0.00%) | 16384 | 3088 | 13296 |

| Full shared cell | Median latency % | Paired r1–r5 % | Latency spread control/candidate % | Actual internal loads | B/op medians | allocs/op medians |
| --- | --- | --- | --- | --- | --- | --- |
| pointer=false/clustered/view=false | -15.922 | -15.92/+3.36/-11.56/-24.90/-17.84 | 14.97/26.39 | 24576→400 | 35950→35944 | 9→9 |
| pointer=false/clustered/view=true | -19.884 | -19.72/-17.78/-22.15/-21.45/-18.52 | 2.44/6.28 | 24576→400 | 776→776 | 7→7 |
| pointer=false/sorted/view=false | -32.685 | -32.79/-36.50/-32.68/-34.63/-32.26 | 6.43/1.24 | 24576→394 | 35944→35944 | 9→9 |
| pointer=false/sorted/view=true | -37.597 | -37.60/-35.17/-37.68/-36.56/-39.16 | 0.74/6.69 | 24576→394 | 776→776 | 7→7 |
| pointer=false/uniform/view=false | +9.180 | +10.42/+16.40/+8.01/+8.59/+9.61 | 11.72/19.55 | 24576→7593 | 35950→35950 | 9→9 |
| pointer=false/uniform/view=true | +9.069 | +9.82/+9.00/-0.35/+8.50/+9.82 | 9.73/2.10 | 24576→7593 | 776→782 | 7→7 |
| pointer=true/clustered/view=false | -2.730 | -1.86/-3.12/-1.82/-3.14/-1.25 | 2.39/1.96 | 16384→257 | 36462→36462 | 11→11 |
| pointer=true/clustered/view=true | -2.589 | -2.59/-1.32/-3.59/-2.61/+0.37 | 3.09/2.31 | 16384→257 | 263622→263622 | 175→175 |
| pointer=true/sorted/view=false | -4.665 | -5.86/-4.51/-4.42/+11.45/-4.59 | 1.56/19.51 | 16384→258 | 36462→36462 | 11→11 |
| pointer=true/sorted/view=true | -2.423 | -2.69/-1.39/-2.63/-3.74/+0.10 | 2.97/4.26 | 16384→258 | 271717→271580 | 175→175 |
| pointer=true/uniform/view=false | +3.925 | +3.92/+4.64/+3.69/+7.55/+3.99 | 7.70/4.48 | 16384→3088 | 36462→36592 | 11→11 |
| pointer=true/uniform/view=true | +3.284 | +3.29/+1.22/-3.88/+3.32/+5.59 | 6.88/3.16 | 16384→3088 | 270641→270630 | 175→175 |

| Singleton pilot cell | Median latency % | Paired r1–r5 % | Latency spread control/candidate % | Paired B/op r1–r5 % | allocs/op medians |
| --- | --- | --- | --- | --- | --- |
| pointer=false/clustered/view=false | +0.7759 | +2.95/-0.85/-0.22/+1.45/+0.59 | 3.07/3.53 | +0.000/-0.056/+0.000/+0.000/+0.017 | 9→9 |
| pointer=false/clustered/view=true | +1.3436 | +0.56/+2.30/-2.83/+1.88/+0.10 | 6.95/2.82 | +0.000/+0.000/+0.000/+0.000/+0.000 | 7→7 |
| pointer=false/sorted/view=false | -0.3969 | +1.61/+0.64/-6.36/+0.72/-2.14 | 6.76/2.61 | -0.036/+0.000/+0.036/+0.003/-0.036 | 9→9 |
| pointer=false/sorted/view=true | +1.6114 | +1.50/+2.95/-2.21/+0.97/+1.39 | 4.24/1.82 | +0.000/+0.000/+0.000/+0.000/+0.000 | 7→7 |
| pointer=false/uniform/view=false | -0.4497 | +0.93/-0.66/-1.26/-1.43/-4.40 | 6.19/2.26 | -0.036/+0.036/+0.000/+0.000/+0.000 | 9→9 |
| pointer=false/uniform/view=true | -1.0753 | -0.19/-3.37/-1.47/+0.40/-13.12 | 15.53/1.89 | +0.000/+0.000/+0.000/+0.000/+0.000 | 7→7 |
| pointer=true/clustered/view=false | -1.9439 | +18.17/-1.13/-5.41/-0.18/-2.30 | 10.91/26.22 | -0.047/+0.000/+0.000/+0.000/+0.000 | 11→11 |
| pointer=true/clustered/view=true | -2.3737 | +12.46/-2.37/-3.91/-1.86/-3.92 | 13.37/12.37 | +27.263/-0.100/+0.046/+0.101/+0.071 | 175→175 |
| pointer=true/sorted/view=false | +0.0577 | +3.33/+0.26/-12.94/-0.24/-1.34 | 16.30/4.02 | +0.036/+0.016/+0.000/+0.000/+0.000 | 11→11 |
| pointer=true/sorted/view=true | -0.8844 | +10.56/-0.65/-4.20/-0.88/+0.01 | 10.78/11.81 | +31.857/-0.104/+0.019/+0.097/-0.149 | 175→175 |
| pointer=true/uniform/view=false | +0.1852 | +4.00/+0.19/-3.66/+1.50/+0.66 | 5.33/2.91 | +0.016/-0.052/+0.000/+0.000/+0.000 | 11→11 |
| pointer=true/uniform/view=true | -0.9264 | +11.08/-5.04/-0.93/-0.17/-2.11 | 14.92/12.39 | +29.810/-0.189/+0.018/+0.000/+0.000 | 175→175 |

| Actual retained executable | Measured source HEAD (not final merge) | Frozen TreeDB tree | Harness SHA256 | Actual executable SHA256 |
| --- | --- | --- | --- | --- |
| control/normal | bc9764ab92a9ceeeb4ae15706c920653e5dd8bbc | a21018011ea94cc40f55ee38f2f38c296c052f40 | b7128ff9520d802fa99713187cd6ad0a4a34c9e34aa42bf862f3642c668dd7d9 | f4559a0125776a47df3ceebac35eeacbd605b3a399d9fca09be3b28baa1f5c99 |
| control/counter | bc9764ab92a9ceeeb4ae15706c920653e5dd8bbc | a21018011ea94cc40f55ee38f2f38c296c052f40 | b7128ff9520d802fa99713187cd6ad0a4a34c9e34aa42bf862f3642c668dd7d9 | b372484c49583c33685879a7a95a278dfdf8afe81f8341640204c7b0c111e14a |
| candidate/normal | 75fe577ecc6ab81237df2cff58454bbeb205cc71 | 0a180e7e7b4f415117334e2e60da29cffd781314 | b7128ff9520d802fa99713187cd6ad0a4a34c9e34aa42bf862f3642c668dd7d9 | 519cec8a30c6cd365ef2e64426faf8a7bf6bf55f8e743e540b08b9f3a94e211a |
| candidate/counter | 75fe577ecc6ab81237df2cff58454bbeb205cc71 | 0a180e7e7b4f415117334e2e60da29cffd781314 | b7128ff9520d802fa99713187cd6ad0a4a34c9e34aa42bf862f3642c668dd7d9 | 6b4a7e9f357619edc4037d546ef8f77dce741cf8b5db8ac915a60b220f968075 |
| control/separate-residency | bc9764ab92a9ceeeb4ae15706c920653e5dd8bbc | a21018011ea94cc40f55ee38f2f38c296c052f40 | b7128ff9520d802fa99713187cd6ad0a4a34c9e34aa42bf862f3642c668dd7d9 | c21bade8d841b283709c7a87c07f4a2f453e337a1d5a451d6dbd19cdd27e58fc |
| candidate/separate-residency | 75fe577ecc6ab81237df2cff58454bbeb205cc71 | 0a180e7e7b4f415117334e2e60da29cffd781314 | b7128ff9520d802fa99713187cd6ad0a4a34c9e34aa42bf862f3642c668dd7d9 | 6f63ba9ee94cf11f0e7491a502644ab169183b90f9791d8212f8f170f120e74c |

| Selected75f cell | Median latency % | Paired r1–r5 % | Latency spread control/candidate % | B/op medians | allocs/op medians | Retained guard flags |
| --- | --- | --- | --- | --- | --- | --- |
| pointer=false/clustered/view=false | -5.827 | +0.99/-10.68/+1.64/-19.02/+2.95 | 18.28/12.44 | 35944→35944 | 9→9 | control_latency_spread_above10pct; candidate_latency_spread_above10pct |
| pointer=false/clustered/view=true | -2.482 | +4.16/+1.57/-6.78/-13.31/-2.24 | 14.28/4.70 | 776→776 | 7→7 | control_latency_spread_above10pct |
| pointer=false/sorted/view=false | -0.318 | -0.42/-7.36/+1.22/-18.47/+0.26 | 21.21/2.57 | 35944→35944 | 9→9 | control_latency_spread_above10pct |
| pointer=false/sorted/view=true | -0.669 | -1.33/-0.21/-2.99/-11.48/+7.72 | 10.83/10.25 | 776→776 | 7→7 | control_latency_spread_above10pct; candidate_latency_spread_above10pct |
| pointer=false/uniform/view=false | +1.285 | +2.71/+1.50/-0.04/-11.30/+0.15 | 14.41/3.45 | 35950→35950 | 9→9 | control_latency_spread_above10pct; ns/op_positive_median_requires_root_disposition |
| pointer=false/uniform/view=true | -0.249 | +1.89/-0.99/+0.19/+3.82/+2.07 | 13.43/17.15 | 776→776 | 7→7 | control_latency_spread_above10pct; candidate_latency_spread_above10pct |
| pointer=true/clustered/view=false | +1.931 | +0.17/+0.98/+4.57/+3.17/-4.03 | 11.09/3.95 | 36456→36456 | 11→11 | control_latency_spread_above10pct; ns/op_positive_median_requires_root_disposition |
| pointer=true/clustered/view=true | -57.175 | -58.23/-56.66/-57.18/-56.89/-57.89 | 13.37/12.24 | 263490→776 | 175→7 | control_latency_spread_above10pct; candidate_latency_spread_above10pct |
| pointer=true/sorted/view=false | +0.460 | -5.47/+3.28/+2.28/+0.43/-13.39 | 18.08/4.49 | 36469→36456 | 11→11 | control_latency_spread_above10pct; ns/op_positive_median_requires_root_disposition |
| pointer=true/sorted/view=true | -60.357 | -60.79/-56.50/-60.36/-58.02/-61.98 | 11.84/3.69 | 271574→776 | 175→7 | control_latency_spread_above10pct |
| pointer=true/uniform/view=false | +2.371 | +7.93/-1.06/+2.74/+2.37/+3.34 | 6.68/3.14 | 36462→36562 | 11→11 | ns/op_positive_median_requires_root_disposition; B/op_positive_median_requires_root_disposition |
| pointer=true/uniform/view=true | -45.923 | -39.34/-46.75/-49.01/-46.14/-45.03 | 2.20/16.74 | 270630→912 | 175→7 | candidate_latency_spread_above10pct |

| Separate residency cell | Post-GC HeapAlloc MiB medians | Approx phase-end VmHWM MiB medians | Material paired heap/HWM flags |
| --- | --- | --- | --- |
| inline/clustered/owned | 143.820→143.825 | 326.559→322.766 | none |
| inline/clustered/view | 143.931→143.900 | 321.992→319.145 | none |
| inline/sorted/owned | 143.814→143.814 | 359.383→361.203 | none |
| inline/sorted/view | 143.816→143.817 | 327.875→324.812 | none |
| inline/uniform/owned | 143.933→143.902 | 311.414→316.680 | none |
| inline/uniform/view | 143.936→143.905 | 296.625→296.785 | none |
| pointer/clustered/owned | 281.812→277.666 | 394.664→359.824 | none |
| pointer/clustered/view | 282.118→277.681 | 538.520→353.871 | none |
| pointer/sorted/owned | 281.744→277.619 | 408.945→376.789 | none |
| pointer/sorted/view | 281.982→277.613 | 499.348→353.457 | none |
| pointer/uniform/owned | 282.133→277.928 | 393.324→361.328 | none |
| pointer/uniform/view | 282.131→277.936 | 538.758→355.691 | none |
<!-- canonical-tables:end -->

## Retained artifacts

Paths below are relative to the retained evidence root. SHA-256 binds the actual
bytes, not just the occurrence of a digest anywhere in this document.

| Retained artifact relative to evidence root | SHA-256 |
| --- | --- |
| `algorithm-full-primary-inspection.json` | `db4dc6bc746201b7512c4d13c9a8529e8ae697bd606f1b7f2bf3b078f954fef1` |
| `algorithm-full-normal-freeze.json` | `eb0419e74f6c11493087e408eeebf394bf008e86006ced5eb64acd303b1bd336` |
| `algorithm-full-counter-freeze.json` | `9ee522e28df829d261cd8e6a4d3a8df6edfb8ce7d3d960af7774579ddd7aaf80` |
| `algorithm-full-run-order-v2.json` | `aa1444381926f0661ea14fd2936c504b766c7a546d9bc6d53a1b5fbe06e49ec6` |
| `algorithm-paired-4919-732-five-repeat-analysis.json` | `48d5cdedc881add5761a2c20ce8f2183b0ac073651deaa0b888986bbb3d75325` |
| `algorithm-paired-4919-732-extend-reduce.py` | `66d84a92cd05d5ece76537138ecfdcf0d531c3fd8bcbaac2cc9ed45c0e6502f3` |
| `4919-732-five-repeat-independent-analysis.md` | `69cc408c5870656a84a274162e3943917a58f964b563ef01ac969c95f87ca12f` |
| `4919-singleton-five-pair-independent-screen-analysis.json` | `12222bb951c1b9debb01ebb06ebf91d1d8b70a98075aeb633dc076e1405dd4a8` |
| `4919-singleton-five-pair-independent-screen-review.md` | `4efd9a26cbde1342d6fa851568dce8e46da34a21f3f5a7d44769a2d9571783ac` |
| `4917-final-singleton-root-disposition.md` | `c2f64814de49fec11cff20100b44d85a6d8cb4a7fd16285b34ba008f60a5775a` |
| `4917-post-five-pair-architecture-decision.md` | `f0d0adcd3a0715e727ed53ea27fc48548366bdd7745b5b9ad5bb6b04a934e767` |
| `4916-e3-three-repeat-analysis.json` | `e9d8658b70843133d88e16d25fd2942c6efcd2d6aa067ab55ffc6e4e3c1233cb` |
| `4916-e3-independent-matched-decision-review.md` | `5fbd05c848d07ef8875a316a73d67852899d291941e2409eac6887f4ca43cd17` |
| `algorithm-full-primary-packet/sparse-materialization/top.txt` | `87b57a50c66f4d10899a786bb543ef54983ecfa61598519642d85374e4cf1b0d` |
| `algorithm-full-primary-packet/sparse-materialization/callpath.txt` | `01383ba932bf2621fa7de38d7541df00c0997857a39bc8e65ee68e6270b418c3` |
| `algorithm-full-primary-packet/sparse-materialization/validated-profile.json` | `de7481ae18f85c616568b9a5886234ed07a1f651ff384342ec7ebb591c50562d` |
| `algorithm-full-primary-packet/writes-r1/stdout.log` | `a1a0195b050fde60c52e20c2aaf567f4f16f152bc0383030e3b4264720c0470f` |
| `algorithm-full-primary-packet/writes-r1/stderr.log` | `7aa3fca42dae55319f16de08d8264cd7a163466e2b7ff289a56f638b6870efc6` |
| `algorithm-full-primary-packet/writes-r1/execution.json` | `fcdb331903b55d52ad280695b8ebd72f8db77019696f70ac6811ea01e33e3a65` |
| `algorithm-full-primary-packet/writes-r1/parsed.json` | `86e190c3051ffa321b417969264898444201d4467e19237e7422f30cf46c5307` |
| `algorithm-full-primary-packet/writes-r2/stdout.log` | `d587ad09663cc7b292257cdd7163eb643d692be74bbf08980fce6485c36d15d3` |
| `algorithm-full-primary-packet/writes-r2/stderr.log` | `9dbd07476ac04113563c4b11f15774d9ea1c55b2f6f574cd5c485ea377b76e77` |
| `algorithm-full-primary-packet/writes-r2/execution.json` | `0ab1a7724c0aee4461a081500857ab474fedbbb828bc8e4091dd8e56615f5678` |
| `algorithm-full-primary-packet/writes-r2/parsed.json` | `5fc99fc1198a0331bdcff2443fb7a5c2a0087a639373e3b526297f65640f9330` |
| `algorithm-full-primary-packet/writes-r3/stdout.log` | `efae122348fab3a117ac5250c8106c56512c1e8b4fa127aff5447976309158d4` |
| `algorithm-full-primary-packet/writes-r3/stderr.log` | `fe33f1f187a20c61123160f38a0b7d08746052dcd74fd9b012af9693efc0e54e` |
| `algorithm-full-primary-packet/writes-r3/execution.json` | `834b347d2de32f5d6d2e358eead9f73332328fec65dd0ffb9bb7b7c12bbd5309` |
| `algorithm-full-primary-packet/writes-r3/parsed.json` | `2e0d73ee6b64e468500e1446cdf2b1216e397c37dfd60d467203fa3225379505` |
| `algorithm-full-primary-packet/many-r1/stdout.log` | `bee3a8452691035c41ee4f30898aeb5085db1ff5dc97ec5555460e84d1ec63ad` |
| `algorithm-full-primary-packet/many-r1/stderr.log` | `b41873156b3031b09a206bbe195818954d280203e92fcc04b256add8df7d84ee` |
| `algorithm-full-primary-packet/many-r1/execution.json` | `fc06f884024f772ecd05eaff29d93a5f2f1c9e00dfe80be279da0ca0fbd87459` |
| `algorithm-full-primary-packet/many-r1/parsed.json` | `e3a537d2bae75d9c139cf5097ce24c0046b6ef594da76db622a5a0a8184003d8` |
| `algorithm-full-primary-packet/many-r2/stdout.log` | `9b495a16be54c75783742250af373e414300165c06a8a5f0b7e07cdf67618257` |
| `algorithm-full-primary-packet/many-r2/stderr.log` | `3b634bbfd549ae36a9429ea07b27561694cab99015ad2536f3f9b79c8e360ba9` |
| `algorithm-full-primary-packet/many-r2/execution.json` | `e88b518fc28bc8c5f33c30fb73692ff7a3155f69e4c36ecfa6c85e98d3c63ae8` |
| `algorithm-full-primary-packet/many-r2/parsed.json` | `e8f62796a5a82fe740e62f559b6b8875e4a1855987564b0da9f8fa884dcef411` |
| `algorithm-full-primary-packet/many-r3/stdout.log` | `1faf6015ab803c9992939d488615247d680689f8d16f189832da3d0efecf88d6` |
| `algorithm-full-primary-packet/many-r3/stderr.log` | `c3ee30fcc30e329eb4278517ef2454d9ea5c360cc4c3cf8edbc0b98ddab6f786` |
| `algorithm-full-primary-packet/many-r3/execution.json` | `bce40dd20c524901002fedffdb9ac12e4da319e4df6148c07ec001cb320bddd5` |
| `algorithm-full-primary-packet/many-r3/parsed.json` | `ec82bcb34eff06bae8d8b3eee9612e457e33a05a04506bb781b83cad39c5681e` |
| `algorithm-full-primary-packet/internal/stdout.log` | `98ad25f379eb27b4307a6ee66ac910ee059f388307c7ed6c78aff2c2f8d9efc9` |
| `algorithm-full-primary-packet/internal/stderr.log` | `f584effbb1853fab14a8c8c48907c381805012d59e448796c4e7c14d2a89be63` |
| `algorithm-full-primary-packet/internal/execution.json` | `8921b79d0b504b94f951e86828ed35cfc3523e63f0be8a2ed10f424abedcae47` |
| `algorithm-full-primary-packet/internal/parsed.json` | `ea3cefc3be99ae23730c728adfa9169f0e478edf63a2aa00fa5284f912098173` |

| `algorithm-full-preparation-anchor.json` | `b904ec72486de3c51315c33085e15a631f71232f0aa2bf09369a43cf7616c80f` |
| `algorithm-full-primary-packet/launch.json` | `4cd842589318b1bc7fdf51b62d514565b7bee9d1560e854ad6e98f9ad3eba3ca` |
| `algorithm-full-primary-packet/completed.json` | `59a53d78e47a4938c03d4926d8aa81acdc1658e82d5507d144da655471b80c48` |
| `algorithm-full-primary-packet/many-inline-clustered-owned/cpu.pprof` | `9d0671067f5b50d7cbe9f3510fab8bf53d9f2fba4f9b228605de845ed2b17979` |
| `algorithm-full-primary-packet/many-inline-clustered-owned/validated-profile.json` | `651f1b1d23eb8aaa8da94fe4b846f2ca403a433b39bba2f2fddb6e6f354c12ee` |
| `algorithm-full-primary-packet/many-inline-clustered-owned/profile.json` | `82b63e5dbb84e93fc299720b3c28a3ab537075710f8ad9a1841a15e84d8572cb` |
| `algorithm-full-primary-packet/many-inline-clustered-view/cpu.pprof` | `85d82df5ff2b9d8d5c99ab1f4cbd8cc357a4fd19bb08bd99ab1aee1a7a8731dc` |
| `algorithm-full-primary-packet/many-inline-clustered-view/validated-profile.json` | `e353cafb5c6fd3dd774046e273dac808f4c86fb86541e8fef770d7ac20d60903` |
| `algorithm-full-primary-packet/many-inline-clustered-view/profile.json` | `a2c13751dcea458043e6772c602abc9cb495bbb920eb692ebc100d28320bf30f` |
| `algorithm-full-primary-packet/many-view-internal/cpu.pprof` | `05e52cae70af44f331f0620e90161e91013249d982fbc8f995a069c12e57f669` |
| `algorithm-full-primary-packet/many-view-internal/validated-profile.json` | `3bad267bafa22d4896b8f2af28f83c496edb739becc7e8581b42901d61c44908` |
| `algorithm-full-primary-packet/many-view-internal/profile.json` | `7ff5f376b638165097101cab2b29d2a3078f93ef2c5407f175d3b62f06c6d0ca` |
| `algorithm-full-primary-packet/many-view-internal/top.txt` | `3ad4ad9538251c27d89994a28266b7eac31b9b9a22ee1a444fb1487c27aa5516` |
| `algorithm-full-primary-packet/many-view-internal/callpath.txt` | `ce38dda4aba020774b859fc6c2ba3b020705fcb655a3c150bbd5852ae3e72962` |
| `algorithm-full-normal-retained/algorithm-work.test` | `bb190f318f0a10f22c50d2f92a094b28cb35e2a3f44d734da42551b6777ec543` |
| `algorithm-full-counter-retained/algorithm-work.test` | `390367fa96037d46282795fe61ba7baf643ad98fbd697930fe273d9c0bb070ab` |
| `algorithm-paired-4919-732-packet/normal-prepared/freeze.json` | `482f64f7b9dd36f3acd8352ae3a29b31b32403dc7c5d1c25d1c71cd6542d9668` |
| `algorithm-paired-4919-732-packet/normal-prepared/algorithm-work.test` | `a1043e3ad4bc7117114575d24b9b0acc28663d010076e8e97d687c1d51ec2a2f` |
| `algorithm-paired-4919-732-packet/counter-prepared/freeze.json` | `540b79511ad6709c6a27af6d123de596968a6c0e0ebb9df5233a724f299f81dd` |
| `algorithm-paired-4919-732-packet/counter-prepared/algorithm-work.test` | `5f4841a35cdab13d3cb30dd2eb548e0489cc4516230ed38c14369daa0c925223` |
| `4919-singleton-screen-packet/control-normal-pilot-prepared/freeze.json` | `f73e788d7876df3d70cc7502d83c79ba551e8d5f3825a5395874e5fc54ebf6b5` |
| `4919-singleton-screen-packet/control-normal-pilot-prepared/algorithm-work.test` | `20e69fac0ad85bebc6dc00e13d1360dd841b2076050a8c8845298fe2abff88c7` |
| `4919-singleton-screen-packet/candidate-normal-pilot-prepared/freeze.json` | `31294d1e297c8452f11328f2635b9ea336e828457a567b285a8fa029332d7108` |
| `4919-singleton-screen-packet/candidate-normal-pilot-prepared/algorithm-work.test` | `210fb4d93677a46729d1c6bba55cf11326ad7ed4fa58ee97c68f9b4dfdc8c156` |

| `4921-selected-main-five-pair-analysis.json` | `33329d7db4892f758d8092a0f3aa4444623d1f3811f0a88322dd24427baceba9` |
| `4921-75f-independent-selected-main-source-review.md` | `6ef184a02367e66d7fb3daee3e36a49eaf8a8a773b13b655c5dbcf9b17947e8c` |
| `4921-selected-main-full-paired-plan.json` | `aedd1ed66362201ed5cc7269e0352a65aaced903c20bb1ae7f797864b893ebe4` |
| `4921-selected-main-full-extension-plan.json` | `a1e194b192cf2bd339f65136589df62ffc7d8f4d3511f2f66a3b40059bcb0188` |
| `4921-full-selected-main-packet/all4-preparation-anchor.json` | `4bf1548ebf7bd1c29d8441c671a6fbc5c84e2b9b7bac08d88df9e5314a7f059e` |
| `4920-selected-main-three-pair-residency-analysis.json` | `7908f7ecf6c2cab50ed93985c07c6a32bebe2308c7c4778a0bf6b19e144ff99d` |
| `4920-selected-main-residency-independent-evidence-review.md` | `cb008a67c3b32c623bd405f9da237757c62435a1f50bc5fb45efd018eadcd4a2` |
| `4920-root-selected-main-residency-disposition.md` | `896a2c23190d22e3100eef95ddd96f53bdcfe879236373cdfae6ee9cfd9479ff` |
| `4920-residency-recorder.py` | `0171eefeae1b80599b6cf1c91bcbf022671a75c01749cd6963a9150a198cab58` |
| `persistent-delta-root-activation-disposition.md` | `e25edc6aab176d28195c5a9c5e85378f05b289bb433d067bb3c77ae3286dac83` |
| `4921-full-selected-main-packet/control/normal-prepared/freeze.json` | `fea5745454ddb43206a637a23e4b7317296f101a40aa0b7feb1bfb20879d4349` |
| `4921-full-selected-main-packet/control/normal-prepared/algorithm-work.test` | `f4559a0125776a47df3ceebac35eeacbd605b3a399d9fca09be3b28baa1f5c99` |
| `4921-full-selected-main-packet/control/counter-prepared/freeze.json` | `8f8aed0810c4b9baf075b376390efa2d0bcbccc8c008453b5bb3d52ca8e12d6f` |
| `4921-full-selected-main-packet/control/counter-prepared/algorithm-work.test` | `b372484c49583c33685879a7a95a278dfdf8afe81f8341640204c7b0c111e14a` |
| `4920-residency-selected-main-packet/control/prepared/freeze.json` | `9d9d714c98e18ee2fbcd249d2322e69a54f58293123ba3a901385745a0cf0e1b` |
| `4920-residency-selected-main-packet/control/prepared/algorithm-work.test` | `c21bade8d841b283709c7a87c07f4a2f453e337a1d5a451d6dbd19cdd27e58fc` |
| `4921-full-selected-main-packet/candidate/normal-prepared/freeze.json` | `16b796bb06fbc25593dacf3c96de7cb917e7067afb7d93215e10248af86f658b` |
| `4921-full-selected-main-packet/candidate/normal-prepared/algorithm-work.test` | `519cec8a30c6cd365ef2e64426faf8a7bf6bf55f8e743e540b08b9f3a94e211a` |
| `4921-full-selected-main-packet/candidate/counter-prepared/freeze.json` | `845c94a226131c8fdcb4687b662d6393d5aacbb91a311bf834f4a3ead36be97f` |
| `4921-full-selected-main-packet/candidate/counter-prepared/algorithm-work.test` | `6b4a7e9f357619edc4037d546ef8f77dce741cf8b5db8ac915a60b220f968075` |
| `4920-residency-selected-main-packet/candidate/prepared/freeze.json` | `ed4740e53791ed2d04e0ce4d8ef5468fe4d0f4f99e4a0e384ee287152ed59f39` |
| `4920-residency-selected-main-packet/candidate/prepared/algorithm-work.test` | `6f63ba9ee94cf11f0e7491a502644ab169183b90f9791d8212f8f170f120e74c` |

| `4921-selected-main-five-pair-independent-evidence-review.md` | `2a26940f8380e712ed4a76ddd6343a277521510a61191dde4a93f524ed7d89f6` |
| `4921-root-selected-main-five-pair-disposition.md` | `8683a46ad73c21b6f6a79660ec26ff3788b1b3e5e2c21f630deb7e6fcdfd9d9c` |
| `4921-final-source-base-audit.json` | `69330f8d435d1ca993d9d7252cd59438d940a43ba1112cf9d30b035f1fde6c9b` |
| `4921-c6b-final-source-applicability.json` | `3f153e5ff653bd563ed65e680e2f2c4cf1a284acea03c9521744b1f6af83eabf` |
| `4921-dacff-final-base-evidence-applicability-review.md` | `aa8266317a5c5c0ec66cd8047dabfac207cdb037860fa3034a2991aadb719d56` |
| `4921-actual-merge.json` | `42bae8b8d9741790cbb0397dd1ad8296b31e70c36a33d3905bb4d95db9ae7c0c` |
