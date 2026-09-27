# Integrated physical-packing comparison (#4753)

The first prospectively declared packing pair is a **valid performance no-go**.
Both layouts pass correctness and strict retained replay, but all ten packing
QPS windows fail the required 1.15x improvement and seven p95 windows regress.
The unchanged decisive-rejection rule stops external pairs 2–5 and all later
scaling/lifecycle axes. This result does not qualify F or release Raft.

## Candidate and population

Both arms use landed source `03a3b73dd2fc6888e603307df5c3a00f0a879342`, tree
`b55f42a991bfeb419824205a5412292adbb71fde`: accepted kRt routing, Vamana
R64/L256/alpha1.2, and M logical-domain graphs, including the merged P2
component. Runtime SHA256 is
`82ddef839768660957f3face5c15197b93e0020708703b4653f8708f93e456f6`;
strict resource-reader SHA256 is
`28124dd40f9c96bb879c730218e55611c856a83ee4de0c165f586ad6007a5db1`.

The real fixture contains 100,000 normalized Cohere FP32 vectors with 768
dimensions and 512 queries from test rows 200–711. These are selection-used
queries, not a holdout. Both fresh constructions use 16 logical domains,
seed 4017, identical assignments and graph/router settings, disjoint actual
membership, and a planned 20% overlap capacity envelope. Actual overlap is zero.

| Layout | Physical packs | Target hot-pack bytes | Graph representation |
| --- | ---: | ---: | --- |
| One | 16 | 33,554,432 | One contiguous graph per domain |
| Multi | 64 | 7,696,384 | One domain graph encoded as root/section assets |

Both builds and the pre-timing geometry check pass. Their exact logical-union
digest is `36bee3befe9e258617d364d860a740affc7a633a5b65fe8591383c6afec68403`.
The hot-pack targets do not set the encoded section chunk cap; the materializer
uses its existing 256 MiB cap. Layout labels are not evidence of smaller graph
traversals or resident memory.

## Unchanged comparison and rejection

The [original collection plan](collection-plan.md), [exact invocation order](commands.md)
and [receipt contract](receipt-schema.md) are copied unchanged from the frozen
measurement packet. The final 17-file measurement inventory has SHA256
`952bff48b2a333388a06451c3708503c78dfbe5d004a75c2496d6b05b3d5dc49`.
The plan contains compilation-stage wording; the separately retained completed
freeze and root admission records establish actual construction authority.

External block 1 ran one then multi. Each arm has five internal repetitions of
P5 selected and P16 exhaustive search at c1/c32, EF96, C256, top10 and warmup64.
All 40 rows complete all 512 declared requests without partial results, with
minimum recall 0.95703125. Resource bounds and endpoint-loss refusal checks pass.
Internal windows are not independent external trials.

| Multi / one packing ratio | c1, five windows | c32, five windows |
| --- | --- | --- |
| QPS | 0.993328–1.000377 | 0.980894–0.995761 |
| p95 | 0.993529–1.013648 | 0.979165–1.047670 |

All ten windows pass matched quality; none meets QPS >=1.15. Four c1 and three
c32 windows exceed the p95 <=1 gate. Both layouts' selected/exhaustive controls
pass all twenty control windows: QPS ratios 2.2125–2.8185 and p95 0.3596–0.4989.
These controls do not accept the packing treatment or prove superiority over an
ordinary whole-collection index.

[The compact comparison](block1-comparison.json) retains every pair, window,
replay receipt and logical-union digest. It omits only the large report
projections from the original comparer result, whose SHA256 is
`08b0aa9d8bab328c50cce96c17a90214910192b196e2dc050476dade96009b46`.
The actual comparer process exited 0; the subsequent fixed performance gate
exited 1. No accepted-result pins were emitted. Both in-process replays accepted
their authentic 20-row reports; no replay subprocess was invented.

The prospective all-pairs gate is already unsatisfiable. External blocks 2–5
remain unmeasured: eight serving reports, 160 serving rows, forty packing
windows, eighty selected/exhaustive control windows and 32 companion resource
cells. No seed, EF, probe count, router cap, query or threshold was changed.
Later structured 100K/250K domain axes, real overlap, whole-collection reference
and native lifecycle observations were not launched under this freeze.

## What the work counters establish

[All 20 matched row totals](work-counters.json) agree between layouts: router
and local score calls, visited nodes/edges, RPCs, candidates and wire bytes.
At selected P5 each 512-query row performs 2,560 local graph searches, 6,189,176
local score calls and 1,796 RPCs. Untimed attribution also agrees: 5,943,416
candidates and 15,158,538 edges. Historical attribution field names contain
`hnsw`; the selected graph implementation in this packet is Vamana.

The source explains this neutrality. Multi-pack materialization groups and
sorts domain members, uses the same Vamana builder, then splits its encoding
into sections (`vector_partition_persistent_searcher_v1.go`, domain mode at
2580, member grouping at 2601, builder at 2668/2706, section split at 2769).
The public materializer supplies the fixed 256 MiB asset cap (25, 1885–1886).
This fixture's largest normalized-vector section is only 20,155,392 bytes.
Its singleton chunk uses the reader's flat-vector path
(`column_hnsw_search_pack_reader.go:515`), preserving indexed scoring
(`column_hnsw_search_pack_search.go:1270`). The coordinator selects one anchor
per domain (`vector_partition_coordinator_v1.go:1469`). All source locations
refer to the frozen commit above.

M removed the historical multiple-searches-per-domain cost. Compared with
one-pack-per-domain, this packing treatment provides no further graph-work
reduction here. Physical-partition bookkeeping and additional asset metadata
remain possible overheads; the counters alone do not attribute timing or prove
a recoverable 15% gain. No unsupported optimization claim follows.

## Construction and whole-command costs

| Observation | One | Multi |
| --- | ---: | ---: |
| Construction wall seconds | 797.22 | 882.82 |
| Construction user + system CPU seconds | 1,032.69 | 1,116.09 |
| Construction maximum RSS, KiB | 3,579,008 | 3,559,380 |
| Construction cgroup peak bytes | 5,018,697,728 | 4,969,574,400 |
| Derived-build timer seconds | 154.6963 | 154.1775 |
| Source physical bytes | 1,645,787,004 | 1,645,787,004 |
| Final derived physical bytes | 339,031,564 | 339,124,079 |
| Serving whole-command wall seconds | 441.76 | 441.27 |
| Serving user + system CPU seconds | 577.61 | 580.54 |
| Serving maximum RSS, KiB | 1,901,964 | 1,908,480 |
| Serving cgroup peak bytes | 1,296,084,992 | 1,425,330,176 |

Both constructions use 12 GiB cgroups; serving uses 8 GiB. Task swap is disabled
and all cgroup limit/OOM counters are zero. Maximum RSS and cgroup peak are
different measures. Whole-command times include setup and diagnostics; the
derived-build timer is narrower. These single construction observations are not
repeated causal estimates. Neither whole-command CPU nor inverse concurrent
QPS is a per-query CPU measurement.

## Companion worker resources

Both prospectively frozen resource producers and their strict readers pass.
[The exact reader projections](serving-resources.json) contain all eight cells,
raw CPU/TotalAlloc/Mallocs deltas, snapshot overhead and per-attempt values.
Each cell has 512 completed requests. CPU is measured for the whole co-located
process while workers run, including runtime and background activity; allocation
counters are monotonic deltas, not retained heap. Setup, warmup and outcome
validation are excluded. Profiling is enabled; no forced GC or idle subtraction
is applied. These are one prospective observation per layout, not repeated
resource-improvement evidence.

| Probes / concurrency | One CPU ms/request | Multi CPU ms/request | One bytes/request | Multi bytes/request | One allocs/request | Multi allocs/request |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 5 / 1 | 3.6382 | 3.6576 | 187736.84 | 202650.89 | 1061.81 | 1062.64 |
| 5 / 32 | 10.5107 | 10.5937 | 285229.38 | 287528.41 | 950.09 | 954.52 |
| 16 / 1 | 11.9436 | 11.9734 | 367164.12 | 397011.05 | 1504.59 | 1510.67 |
| 16 / 32 | 29.6969 | 29.5641 | 405417.84 | 420669.53 | 1349.69 | 1352.37 |

Multi/one CPU ratios are 0.9955–1.0079; allocated bytes increase in all four
cells by 0.81%–8.13%, and allocation counts increase by 0.08%–0.47%.
These observations do not establish causal attribution or statistical
significance. They provide no resource-based reason to override the packing
no-go. Whole resource-command wall times are 373.41/377.21 seconds and maximum
RSS is 2,185,356/2,177,216 KiB; those commands also replay the full parent report.
Their cgroup peaks are 1,547,030,528/1,528,274,944 bytes, under 8 GiB/no-swap
limits with zero limit/OOM events.

The original one-layout reader failed before reduction because its shell PATH
omitted the already pinned Go executable used to decode pprof. That exit-1
receipt is preserved. Independently reviewed `f-resource-read-v2.sh` supplies
the pinned Go 1.26.0 environment and writes fresh `-v2` outputs; its reader
binary, input measurements and original 17-file manifest are unchanged.
The original and v2 one-layout parent inventories are byte-identical. Both v2
readers exit 0. `f-physical-v2.sh` only references the equivalent v2 parent
inventory. The amendment manifest SHA256 is
`ec4b7f41298a4c51387f33aee448aea135faeda4c7cde5933a5fef732d49b481`.
This infrastructure repair neither repeats the producer nor replaces a failed
performance observation.

## Retained pack physical resources

Both separately scoped collectors replay their pinned parent before opening
all 16 serving domain anchors. Their strict readers authenticate the original
parent, exact command, serving topology, manifest and observed release counters.
[The exact reader projections](physical-resources.json) retain the full headers,
per-anchor observations and totals.

| Owned pack inventory | One | Multi |
| --- | ---: | ---: |
| Pack payload bytes | 331,887,416 | 331,887,288 |
| Mapped extent bytes | 331,952,128 | 332,603,392 |
| Heap-copy bytes | 0 | 0 |
| Metadata bytes, conservative bound | 46,592 | 360,960 |
| Stable-ID bytes, conservative bound | 0 | 0 |
| Logical handles | 16 | 176 |
| Required / opened / validated section chunks | 0 / 0 / 0 | 160 / 160 / 160 |
| Sum of EF96/top10 per-anchor scratch bounds, bytes | 1,881,840 | 1,881,840 |

Mapped extents are not RSS, logical handles are not operating-system file
descriptors, and validated chunks are not query touches. The one-layout
contiguous representation has no section chunks; this does not mean it opens
no pack. Metadata and scratch bounds are not measured retained heap or peak
working memory. Zero heap-copy bytes describe only the pack reader's owned
copy buffers. The scratch sum is an inventory bound, not a concurrent workload
peak. Router, native topology and historical serving-process resources are
outside this collector's scope.

Every anchor reports zero remaining owned handles, mapped bytes and heap-copy
bytes after close, with balanced acquire/release counts and no errors. This
establishes release accounting, not Go heap reclamation. Both producer and
reader commands pass under 8 GiB/no-swap limits with zero limit/OOM events;
producer cgroup peaks are 1,427,550,208/1,420,791,808 bytes. Those whole-command
peaks include parent replay and do not estimate pack residency alone. Both
post-reader checks preserve exact original fixture, baseline and built-asset
inventories. These physical observations do not override the failed timing gate.

## Evidence retention and unresolved gates

Original reports, profiles, attempts, commands and raw resources remain at
`mikers@192.168.0.111:/home/mikers/gomap-4753-integrated-03a3b73dd`.
The earlier fixture, baseline database and rejected experiments remain intact.
All heavy collection is serialized on this shared LAN runner, with fresh
load/memory/disk admission and bounded cgroups; it is not a dedicated host.

This negative result returns the unresolved physical-expansion mechanism to
[#4772](https://github.com/snissn/gomap/issues/4772).
[#4753](https://github.com/snissn/gomap/issues/4753) remains open as the sole
local qualification owner. A future optimization needs its own prospective
evidence; any changed supported-envelope objective needs an explicit owner
decision. This packet neither changes the gate nor supplies a Raft handoff.
