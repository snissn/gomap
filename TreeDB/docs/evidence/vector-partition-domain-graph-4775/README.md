# Logical-domain graph comparison (#4775)

The prospectively fixed domain-graph comparison passes: all five external
pairs, 50 selected treatment windows and ten CPU/allocation companions are
complete. Strict readers and final input/baseline preservation checks pass.
It compares one graph per logical domain with the earlier
graph per physical pack, at the same source, membership, router and search settings.
The sole local scaling verdict remains [#4753](https://github.com/snissn/gomap/issues/4753).

## Disposition

Accept the declared M domain-graph performance packet for this exact pinned
candidate and population. Across all selected windows, QPS ratios are
1.6635–1.8892 at c1 and 1.9019–2.2450 at c32; p95 ratios are
0.6000–0.6732 and 0.4406–0.5250 respectively. All 100 same-report
selected/exhaustive control windows also pass. No failed window was dropped.
The original stopping rule, configuration and population were unchanged.

The five external pairs are the repeated experimental units; internal windows
are not independent external trials. This acceptance does not close the broader
local or distributed qualification gates listed below.

## Frozen candidate and apparatus

The measured candidate and strict comparer are landed commit
`1cde6b84b4a2d94b84394dc5e0d949f20c831298`, tree
`1a5187a0b95c10e60c1b984f9f81c812691dd651`. The candidate includes domain
graphs (#4804), prepared inserts (#4820), sparse catalog participation (#4815),
the cross-runtime comparer (#4822), and logical-boundary report validation
(#4823). Results qualify that complete pinned candidate; they cannot isolate
the effect of one change. Later integrated candidates need their own applicable
qualification.

The baseline is the original clean producer and database from
`c6dcbcf0c0386fb156392319b2b1f10cc4b99fa3`, retained at
`mikers@192.168.0.111:/home/mikers/gomap-4775-homes-c6dcbcf0c`.
The new campaign is separate:
`/home/mikers/gomap-4775-domain-1cde6b84b` on the same host.

The [prospective plan and freeze](https://github.com/snissn/gomap/issues/4775#issuecomment-5852255951)
were published before construction. The [original collection plan](collection-plan.md)
is retained verbatim. Both source checkouts are clean, Go is
1.26.0, and the binaries report `vcs.modified=false`.

| Frozen artifact | SHA256 |
| --- | --- |
| Candidate producer | `77d1a9cd1da6d25e6ba9a80f39ef60d1357a9dfd58ef79fd52c699e2e4fe62f5` |
| Candidate resource reader | `1eeeb0496d45e89da3ba2e097a717fd9336e30ea7c75f8d462f8d34974021a84` |
| Cross-runtime comparer | `dc5b0bb160844a24fe82143b01520a0d1f9b1842d88612489381feec654c2a09` |
| Runtime/apparatus blob inventory | `5c4f21d94c09176973bd389636c38e050227cc4bdf104be9084869838f088494` |
| Harness blob inventory | `cba348b6a9cd8c7204d245bc2e37a6dc2ff85c338ff55bd06f9b4edca41378e6` |
| Input/binary pins | `ffab045669468af0aacedc1e283c16bfb49804cc5e5ea70009b43a1f9aaf963d` |
| Build dependency pins | `62c9983f71df49681af5ee87f2343a1a9e49d1890db78f4dc268f207b7a9a8a1` |
| Launch package pins | `e06237d7e737b4250e362e222666a5313c7d4c02e83c75b8c68ae1d750624642` |
| Completed freeze receipt inventory | `70edd24a9dbdfae27433e7ec15dde7c71d5c225e03dd4b4c56d0aee5bd368e6d` |

The compile freeze exited 0: 69.44s wall, 103.96s CPU, 1,918,214,144 bytes
maximum RSS and 5,867,253,760 bytes cgroup peak. Its limit was 8 GiB, with
zero task swap and OOM events. These are apparatus-build costs. Frozen
resource-reader authentication and rejection tests passed before collection.
Original baseline database, binary, fixture and dependency inventories also
verified unchanged.

## Construction result

Fresh construction exited 0. Its enforced identity comparison passes against
the baseline's exact shard generation, parent graph, source ordinals,
partition/router configuration and accepted graph profile. All 100,000 home
memberships remain in 64 nonempty packs. The producer reports zero missing,
corrupt or stale assets.

| Whole-command observation | Candidate |
| --- | --- |
| Wall / CPU seconds | 887.19 / 1,121.69 |
| Maximum RSS bytes | 3,659,960,320 |
| Cgroup peak bytes | 4,986,765,312 |
| Source physical bytes | 1,645,787,002 |
| Derived physical bytes | 339,124,076 |
| Pack payload bytes | 331,887,288 |

The separate derived-build timer is 156.441s, not whole-command construction
time. Task swaps and all cgroup limit/OOM events are zero. The local M3
microbenchmark executes 8,192 searches (16 domains × 512 queries); its local
recall is not global serving qualification.

The accepted descriptor SHA256 is
`8790fef6da1c5defa3d8bf1d25892a8b2958200d9217c2be9fe01fddee288200`.
The 18-file `construction-outcome-pins.txt` inventory has SHA256
`a3a061b017d25e3c7614ab1a94cd16823c532a845aa30d36ce679dd82902c99b`.
These post-run hashes authenticate outcomes; they were not prospective pins.

## First serving pair

Both fresh external block 1 arms exited 0. Each has 20 passing quality cells,
512 successful requests per cell, passing endpoint-loss refusal and enforced
resource limits. Their historical `experimental_gate_failures` ledgers remain
unchanged. Source-specific strict replay accepted both reports and the
cross-runtime comparison passed all ten selected windows.

| Whole-command observation | Per-pack | Per-domain |
| --- | --- | --- |
| P5 recall | 0.95703125 | 0.95703125 |
| Exhaustive P16 recall | 0.99921875 | 0.9990234375 |
| Wall / CPU seconds | 489.74 / 767.48 | 441.79 / 579.43 |
| Maximum RSS bytes | 1,946,157,056 | 1,951,498,240 |
| Cgroup peak bytes | 2,936,610,816 | 1,288,388,608 |

Both used an 8 GiB cgroup limit with task swap disabled; swaps and limit/OOM
events were zero. Whole-command times include setup and diagnostics and do
not substitute for paired serving-window comparisons. RSS and cgroup peak
are distinct observations, not interchangeable memory measures.

The per-pack report SHA256 is
`c3afd8b81ba21a2b075c41287fb3c391edcab53a219260bad9a99f0dfe1293a6`;
the per-domain report SHA256 is
`b7e63d3aa82f58c6d0daab7785eb1f3b15335b058c300f2e2c3b7e245632af92`.
Both published-report inventories verified after copying the receipts locally.
The prepared comparison plan SHA256 is
`4e0aa3f1ef5e0f14663a9dd67cddbdb7471426980f480375fe549a665713d900`.

## First strict comparison

The frozen comparer exited 0 after 712.07 seconds wall and 722.97 seconds
CPU. Both actual child runtimes accepted all 20 report rows. Original baseline
database preservation passed. All ten selected treatment windows passed
matched quality, QPS at least 1.15x and p95 no worse than baseline. Both
same-report selected/exhaustive controls also passed their ten windows.

| Domain / per-pack ratio | c1, five windows | c32, five windows |
| --- | --- | --- |
| QPS | 1.8088–1.8349 | 2.0736–2.1371 |
| p95 | 0.6003–0.6176 | 0.4465–0.5040 |

[All pair values and actual replay receipts](block1-comparison.json) retain
all windows. The compact projection omits only the large source-report
projections and pins the complete original comparator output. This is one
external pair; internal windows are not five independent external blocks.
Both bounded CPU/allocation companions and strict readers passed. No full
campaign or scaling verdict follows.

The frozen resource reader expects receipt files. A separately reviewed
exporter projects the actual comparator child receipt bytes verbatim and
labels command/exit status as derived from the successful checked child
execution. No duplicate standalone replay is launched. The original exporter
failed before writing either projection because it used `command` where the
report field is `exact_command`; the original failure is retained. The reviewed
v2 changes that field lookup only, validates both arms before writing, and
pins original comparer/source/plan/result/exit and projection provenance.

## Second strict comparison

External block 2 reverses serving order to domain then per-pack. Both 20-cell
reports pass quality/completion checks; their source-specific replays accepted
all rows. Every treatment and selected/exhaustive control window passes the
unchanged quality, QPS and p95 gates.

| Domain / per-pack ratio | c1, five windows | c32, five windows |
| --- | --- | --- |
| QPS | 1.6688–1.8061 | 2.0429–2.1604 |
| p95 | 0.6136–0.6732 | 0.4406–0.5095 |

[All block 2 pairs and replay receipts](block2-comparison.json) preserve the
original values. The comparison exited 0 in 711.16s wall / 723.40s CPU, with
2,015,019,008 bytes maximum RSS and 4,963,565,568 bytes cgroup peak. Its
8 GiB limit, zero task swap, zero memory events and baseline preservation
were independently checked. Both CPU/allocation companions pass their strict
parent-parity readers; post-resource input, dependency and baseline-database
preservation checks also pass.

## Third strict comparison

External block 3 returns to per-pack then domain order. Both 20-cell reports
pass quality/completion checks, with minimum recall 0.95703125. Source-specific
replays accepted all rows. All ten treatment windows and both sets of ten
selected/exhaustive controls pass the unchanged gates.

| Domain / per-pack ratio | c1, five windows | c32, five windows |
| --- | --- | --- |
| QPS | 1.6635–1.7370 | 1.9019–2.0750 |
| p95 | 0.6496–0.6700 | 0.4794–0.5250 |

[All block 3 pairs and replay receipts](block3-comparison.json) retain the
original values. Comparison exited 0 in 710.87s wall / 725.31s CPU, with
2,018,213,888 bytes maximum RSS and 1,626,685,440 bytes cgroup peak. The
8 GiB limit, zero task swap, zero memory events and original baseline
preservation all pass. Both CPU/allocation companions pass their strict readers;
post-resource input, dependency and baseline-database preservation also pass.

The first domain-serving launch failed admission at load 2.21 before the
producer started. Its receipt remains retained; the unchanged command passed
fresh admission at load 0.45 and completed successfully.

## Fourth strict comparison

External block 4 uses domain then per-pack order. Both 20-cell reports pass
quality/completion checks, with minimum recall 0.95703125. Actual strict replay
accepted every row in both producing runtimes. All ten treatment windows and
both sets of selected/exhaustive controls pass the unchanged gates.

| Domain / per-pack ratio | c1, five windows | c32, five windows |
| --- | --- | --- |
| QPS | 1.7725–1.8892 | 2.1197–2.2450 |
| p95 | 0.6000–0.6314 | 0.4450–0.4855 |

[All block 4 pairs and replay receipts](block4-comparison.json) preserve the
original values. Comparison exited 0 in 711.66s wall / 726.37s CPU, with
2,016,063,488 bytes maximum RSS and 1,577,603,072 bytes cgroup peak. The
8 GiB limit, zero task swap, zero memory events and original baseline
preservation all pass. Both resource companions pass their strict parent-parity
readers. Post-resource input, dependency and baseline preservation also pass.

## Fifth strict comparison

External block 5 returns to per-pack then domain order. Both 20-cell reports
pass quality/completion checks, with minimum recall 0.95703125. Both actual
producing runtimes accepted all 20 rows. All ten treatment windows and both
sets of selected/exhaustive controls pass the unchanged gates.

| Domain / per-pack ratio | c1, five windows | c32, five windows |
| --- | --- | --- |
| QPS | 1.7559–1.7778 | 2.0948–2.1386 |
| p95 | 0.6283–0.6358 | 0.4706–0.4874 |

[All block 5 pairs and replay receipts](block5-comparison.json) retain the
original values. Comparison exited 0 in 711.31s wall / 726.26s CPU, with
2,014,134,272 bytes maximum RSS and 1,506,672,640 bytes cgroup peak. The
8 GiB limit, zero task swap, zero memory events and original baseline
preservation all pass. Both resource companions pass strict parent-parity
readers. Final input, dependency and baseline-database preservation also pass.

## Selected-query work

[The recorded counters](selected-work-counters.json) agree in every selected
window in blocks 1 through 5. Both arms score 256 router representatives per query.

| Per successful P5 query | Per-pack | Per-domain |
| --- | --- | --- |
| Local graph searches | 20 | 5 |
| Navigation candidates scored | 24,108.78125 | 11,608.234375 |
| Total local scoring calls | 26,028.78125 | 12,088.234375 |
| RPCs | 4 | 3.5078125 |
| Response bytes | 9,328 | 3,334.75 |

The historical `hnsw` counter names also carry the selected Vamana traversal.
These are measured work counters, separate from physical-resource inventory.
They support the proposed mechanism without isolating individual changes in
the complete pinned candidate.

## Resource observations

Both companions preserve exact parent result parity across four complete
512-attempt cells. These are new whole-process worker observations, separate
from the original timed windows; no statistical significance claim is made.
The [unaltered reader projections](resource-observations.json) retain their
parent, header, inventory and receipt hashes.

| Domain / per-pack ratio | P5 c1 | P5 c32 | P16 c1 | P16 c32 |
| --- | --- | --- | --- | --- |
| CPU per attempt | 0.3881 | 0.4675 | 0.4290 | 0.5415 |
| Allocated bytes per attempt | 0.5640 | 0.6600 | 0.4027 | 0.4714 |
| Allocations per attempt | 0.6725 | 0.6459 | 0.5244 | 0.5241 |

| Whole companion command | Per-pack | Per-domain |
| --- | --- | --- |
| Wall / CPU seconds | 419.28 / 476.33 | 379.15 / 402.94 |
| Maximum RSS bytes | 2,207,965,184 | 2,225,725,440 |
| Cgroup peak bytes | 1,606,938,624 | 2,549,850,112 |

Task swap and limit/OOM events are zero. Whole-command figures include strict
parent replay and setup and cannot replace the worker observations.

The second pair preserves the same parent-parity and completion checks:

| Block 2 domain / per-pack ratio | P5 c1 | P5 c32 | P16 c1 | P16 c32 |
| --- | --- | --- | --- | --- |
| CPU per attempt | 0.3821 | 0.4652 | 0.4317 | 0.5397 |
| Allocated bytes per attempt | 0.5503 | 0.6008 | 0.4732 | 0.4939 |
| Allocations per attempt | 0.6696 | 0.6427 | 0.5335 | 0.5242 |

| Whole block 2 companion command | Per-pack | Per-domain |
| --- | --- | --- |
| Wall / CPU seconds | 418.58 / 475.05 | 379.15 / 403.58 |
| Maximum RSS bytes | 2,226,958,336 | 2,229,137,408 |
| Cgroup peak bytes | 1,573,724,160 | 1,584,836,608 |

The third pair also preserves exact parent parity and all completion checks:

| Block 3 domain / per-pack ratio | P5 c1 | P5 c32 | P16 c1 | P16 c32 |
| --- | --- | --- | --- | --- |
| CPU per attempt | 0.3852 | 0.4709 | 0.4270 | 0.5346 |
| Allocated bytes per attempt | 0.6329 | 0.5887 | 0.4715 | 0.4869 |
| Allocations per attempt | 0.6750 | 0.6419 | 0.5311 | 0.5225 |

| Whole block 3 companion command | Per-pack | Per-domain |
| --- | --- | --- |
| Wall / CPU seconds | 421.09 / 476.39 | 381.16 / 405.21 |
| Maximum RSS bytes | 2,227,421,184 | 2,231,549,952 |
| Cgroup peak bytes | 1,607,516,160 | 1,782,595,584 |

The fourth pair preserves the same parent-parity and completion checks:

| Block 4 domain / per-pack ratio | P5 c1 | P5 c32 | P16 c1 | P16 c32 |
| --- | --- | --- | --- | --- |
| CPU per attempt | 0.3786 | 0.4652 | 0.4292 | 0.5342 |
| Allocated bytes per attempt | 0.5847 | 0.5816 | 0.4454 | 0.4839 |
| Allocations per attempt | 0.6698 | 0.6430 | 0.5299 | 0.5250 |

| Whole block 4 companion command | Per-pack | Per-domain |
| --- | --- | --- |
| Wall / CPU seconds | 419.47 / 475.40 | 380.82 / 404.76 |
| Maximum RSS bytes | 2,226,974,720 | 2,228,985,856 |
| Cgroup peak bytes | 1,591,828,480 | 1,527,468,032 |

The fifth pair completes all ten companions, with exact parent parity and all
512 attempts successful in each of its four cells per arm:

| Block 5 domain / per-pack ratio | P5 c1 | P5 c32 | P16 c1 | P16 c32 |
| --- | --- | --- | --- | --- |
| CPU per attempt | 0.3845 | 0.4713 | 0.4297 | 0.5565 |
| Allocated bytes per attempt | 0.5816 | 0.6644 | 0.4188 | 0.5153 |
| Allocations per attempt | 0.6713 | 0.6480 | 0.5258 | 0.5261 |

| Whole block 5 companion command | Per-pack | Per-domain |
| --- | --- | --- |
| Wall / CPU seconds | 418.06 / 474.36 | 382.43 / 404.94 |
| Maximum RSS bytes | 2,236,010,496 | 2,228,916,224 |
| Cgroup peak bytes | 1,545,854,976 | 1,527,062,528 |

All 20 matched resource coordinates across five external pairs have lower
observed serving CPU, allocated bytes and allocation counts in the candidate.
These whole-process observations corroborate the reduced traversal work; they
do not isolate a single change within the pinned candidate.

All commands used the frozen 8 GiB limit, with zero task swap and memory
events. The lower allocation counts do not establish lower process RSS.

The candidate-only [prospective physical-resource supplement](https://github.com/snissn/gomap/issues/4775#issuecomment-5852957959)
landed in #4824 and completed under collector `9054f7578c58`, with the
measured candidate unchanged. Its pack/search/preflight/Close/mapped-resource
implementation blobs match the original `1cde6b84b4a2` candidate exactly.
The original independent reader failed because its expected header used the
raw manifest digest instead of the collector's existing topology projection.
The [documented post-collection reader correction](https://github.com/snissn/gomap/issues/4775#issuecomment-5853241493)
authenticates the original raw manifest and parent, derives the serving digest
through the unchanged topology function and original group order, and changes
only that expected digest. It passes the unchanged strict reader and rejects
changed raw identity, group order and an unrelated header field. The original
header, observed receipt and failed validation remain preserved in
`physical-resource-observation.json`; no measurement was recollected.

| Untimed retained candidate inventory | Observed / bound |
| --- | --- |
| Pack payload / mapped extent bytes | 331,887,288 / 332,603,392 |
| Heap-copy bytes | 0 |
| Metadata / stable-ID byte bounds | 360,960 / 0 |
| Logical handles before / after close | 176 / 0 |
| Required / opened / validated chunks | 160 / 160 / 160 |
| Sum of one-search-per-domain scratch bounds, EF96/k10 | 1,881,840 bytes |

All 16 domain readers close with zero active mappings/heap copies, balanced
acquires/releases and zero release errors. The observer exited 0 in 357.33s wall
and 361.79s CPU, with 2,009,366,528 bytes maximum RSS and 3,657,601,024 bytes
cgroup peak; 8 GiB cap, zero task swap and limit/OOM events. Corrected independent
validation exited 0 in 8.30s, including hostile expectation checks.

This clears the resource-observation hold on blocks 2–5. It supplies an untimed
candidate inventory, not telemetry from historical timed processes. Mapped
extent is not RSS; logical handles are not OS FDs; opened/validated chunks are
not query touches; metadata and scratch bounds are not measured concurrent
heap peaks. Baseline mapped extents and handles remain **unknown**, because
its historical reader maps differently; no paired improvement is claimed.
All original performance gates and five CPU/allocation observations per arm
remain required.

Baseline whole-file preservation checks pass. Candidate construction-pinned
descriptor, shard metadata and manifest-bound asset identities pass; no original
whole-file candidate database inventory exists, so that stronger preservation
claim is not made.

## Fixed experiment and stopping rule

The selection-used population is real100K×768, train rows `[0,100000)` and
512 query rows `[200,712)`, seed 4017. It is not a holdout. Construction must
preserve parent artifact
`c42c6a3437e4055178d3e41c86c8e1f82da9a4e228715ecda35191692a14fd33`
and shard generation
`e7b6f24ebb60255b5a339800406549fbdadacc2fe3b07a93a593f8a83376b41d`.

D16/Q4 means 16 logical domains and 64 colocated physical packs. Actual
overlap is zero; planned allowance remains 20%. Target hot bytes are
7,696,384. Preserve graph-home assignment and connectivity-preserving Vamana
R64/L256/alpha1.2. Serving uses P5 and exhaustive P16, C256/EF96/top10,
recall at least 95%, four native groups with three nodes each, c1/c32,
warmup64, GOMAXPROCS16 and GOMEMLIMIT3GiB.

Five external pairs run in order AB, BA, AB, BA, AB (A=per-pack, B=domain).
Each report contains five internal windows at both probes and concurrencies:
ten reports, 200 cells and 50 selected treatment ratios. Positive acceptance
requires every selected pair to achieve at least 1.15x QPS, no p95 regression
and matched passing quality. Strict replay uses each report's own pinned
producer; the comparer also checks actual four-to-one traversal reduction.

Stop on construction, identity, safety or quality rejection. After a complete,
strictly replayed pair, any failed required QPS/p95 window ends later blocks
for prospective negative futility. Retain failures and mark uncollected work
unmeasured. Collect the completed pair's bounded resource companions if safe.
No parameter, population, cap or gate changes follow an outcome. Positive
claims require all five external resource observations per arm as well.

The admitted shared LAN runner has persistent build cache and artifact storage;
a dedicated runner is unavailable. Each launch requires load below 2,
available memory at least 16 GiB, disk at least 50 GiB and no competing
benchmark. Heavy work is serialized. Construction has a 12 GiB/60 minute
limit; serving and resources use 8 GiB/40 minutes, with task swap disabled.
Whole-command costs and complete failures are retained separately from timed
worker windows. No cloud capacity is used.

## Preserved earlier failure

The [earlier campaign](https://github.com/snissn/gomap/issues/4775#issuecomment-5852080320)
stopped when report validation compared 16 logical domain boundaries with
64 physical packs. Construction and the baseline arm completed; the domain
producer exited 1 with an incomplete report. Diagnostic quality cells do
not provide an accepted performance comparison. No strict replay, resource
result or treatment ratio substitutes for that failure. #4823 fixed the
boundary check with a regression test and all 49 current-head checks passing.
This campaign starts fresh construction and both arms at external block 1.

This experiment does not establish the historical one-pack packing gate,
fourfold probe reduction, overlap tradeoff, ordinary whole-index superiority,
250K scale, lifecycle/recovery, or distributed Raft qualification.

Original baseline database, inputs, binaries and dependencies were checked again
after each completed resource pair. Preservation checks passed for blocks 1
through 5; successful exits, completion timestamps and empty mismatch outputs
remain in each `blockN-post-resource-preservation/` directory under the frozen
LAN campaign directory.
