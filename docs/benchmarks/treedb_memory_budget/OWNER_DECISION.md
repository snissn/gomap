# Retained-owner and cache-budget decision (#4892)

The selected policy preserves the existing owner controls and public defaults.
The checkpoint-only 32 MiB global free-entry-bin trial is rejected for promotion.
This is an evidence-backed decision, with no runtime change or baseline memory
improvement claim. The [published disposition](https://github.com/snissn/gomap/issues/4892#issuecomment-5954380609)
and [trial disposition](https://github.com/snissn/gomap/issues/4914#issuecomment-5954380126)
record the unresolved guards. Independent disposition review accepted this
rejection, not the trial's performance or readiness to merge.

## Source and measurement scope

Control/selected runtime: `a51b4795cebc194c7a158b262242668877932934`, tree
`12f816dc2ae0759c3d05c4413c2147a317a2ab42`. The original inventory inspected
`7066cd14590ffddde10e796add9d743a6f1664f7`, which has that same tree.
Rejected trial: `ac476e43addd04cb307357ba9abab10c601d39da`, tree
`65b95633ff05f997ebea72056ad52ce8dbcbdd10`; its source review accepted construction
at `099078355e33bda9132021b7d6a4f4ff99a88217`, followed only by wording corrections.
No trial runtime code is included in this decision.

Original60 contains all 20 leaf/value/threshold cohorts, three fresh processes
per cohort, forward/reverse/forward order. Paired70 contains seven representative
cohorts, five matched control/trial pairs, including all original42 and the
28-cell extension. Each full cell loads 250,000 keys, updates 40,000, reads full
values, checks pointer placement/CRC, verifies absent keys, acknowledged LSN
coverage, checked closure and reopen. All canonical cells passed their full
oracles. These are warm, sequential, validated workflows with fixture generation
included in writes and full-byte consumption included in reads; the placement
scan warms leaves. They are not cold/random-read, quiet-host, larger-than-RAM,
SLO or Cloudflare parity evidence.

Linux host: `mikers-B560-DS3H-AC-Y1`, Linux `6.8.0-138-generic` x86_64,
glibc 2.35, Go 1.26.3 at `/home/mikers/.gvm/gos/go1.26.3/bin/go`, CGO enabled.
Timed processes were serialized under the coordinator's runner grant; the host
was not claimed quiet. Frozen controls: `GOMAXPROCS=2`, `GOMEMLIMIT=2GiB`,
`GOGC=100`, `GOWORK=off`, `GOTOOLCHAIN=local`, `GOENV=off`, empty
`GOFLAGS/GODEBUG/GORACE`, `GOTRACEBACK=single`. The pre-timer anchors bind actual
compile inputs, overlay/binary/environment hashes and host identity; manifests
bind every freeze/run path and raw record hash. Those artifacts remain required
for replay, not just the source commits.

## Actual controls and owner boundaries

| Owner/control at the selected runtime | Existing policy and measured limit |
| --- | --- |
| MAIN decoded leaves | Public `Options.LeafPageReadCacheEntries`; experiment sets `leaf_MiB × 2^20 / 4096` entries, or `-1` to disable. Valid page bytes exclude spare backing and metadata. |
| MAIN grouped frames | Existing manager `SetGroupedFrameCacheMaxBytes((64-leaf_MiB) << 20)` through the [experimental overlay](../../../scripts/treedb_memory_budget_overlay.py). At zero bytes also `SetGroupedFrameCacheEntries(0)` because zero byte limit alone is unbounded. Charges raw lengths, not backing capacities. This overlay is not a new public product knob. |
| Placement | Public `Options.ValueLog.PointerThreshold`, static 1/1024 B. Both thresholds persist random4096 B as pointers; only compressible256 B changes placement. Persistent value log is separate from redo WAL. |
| Global append-only free entries | Existing 256 MiB admission ceiling and maximum admitted buffer of `2^17` entries; strong bins survive GC. Pressure/cold-mode paths can fully drop free bins. Normal checkpoint has no 32 MiB bound for this owner. |
| Generic entry pool / batch arenas | Separate existing 32 MiB checkpoint targets, each subject to its current lower budget. They do not cap the global append-only entry bins or whole process. |
| DB idle reset memtables | Existing count limit: 8 after checkpoint, 24 after ordinary flush; recycle already hard-resets under pressure/capacity hints. Four measured idle tables retain 15,378,880 B pointer or 17,169,768 B inline entry backing. The lease trim's capacity argument is unused; it is not an enforced byte budget. |
| Required live ownership | Acquired memtable/iterator backing, retired reader views and busy direct/batch leases are separate owners. They cannot be discarded as idle free backing. The trial preserved these owners. |

The 64 MiB split is a combined configured MAIN cache payload budget, not equal
actual heap/RSS or a universal 64 MiB total-memory promise. Global/process and
cache-prefixed entry/scratch gauges alias the same owner; do not sum aliases,
owner capacities into heap, or mmap address-space bytes into RSS. MAIN and
caching-manager frame caches/stashes are separate owners. At GC2 the measured
mutable payload, borrowed/in-flight batch/direct payload, queue backlog and
reusable value arenas were zero; free entry backing and four DB reset leases
were distinct nonzero owners.

Direct GC2 `HeapAlloc` is sampled immediately after `runtime.GC`, before Stats,
without fixture subtraction. RSS/HWM and engine peaks are OS/periodic samples
observed through later Stats work, not simultaneous direct heap or continuous
phase maxima. Diagnostic Stats scans can allocate; observer effects are a
limitation, not a quantified causal explanation.

## Canonical tables

Original60 rows are n=3 medians. MiB means `2^20` bytes; HWM is whole-process RSS
high-water mark. Exact free-bin charges are 38,583,600 B pointer and 52,483,112 B
inline in every original repeat. These are retained capacities, not a measured
saving from a policy change. Placement timings and allocations describe the
current fixture only and authorize no universal default change.

Paired70 rows are n=5 medians per product. Change compares product medians;
positive paired changes count individual repeat pairs, not a statistical test.
Spread is `(max-min)/abs(median) × 100` separately for control/trial. Timing
metrics are ns/op (one operation per checkpoint); allocation metrics are B/op
or objects/op as named. Raw values, extrema and all other phases/counters remain
in the hash-pinned full analyses. These seven trial cohorts do not replace the
20-cohort original budget inventory or final integrated qualification.

<!-- canonical-tables:start -->
| Original60 cohort | Heap MiB | RSS MiB | HWM MiB | Free bins MiB | DB idle backing MiB | Leaf MiB | Frame MiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| leaf0-bytes256-threshold1 | 220.27 | 282.26 | 302.92 | 36.80 | 14.67 | 0.00 | 60.94 |
| leaf0-bytes256-threshold1024 | 113.98 | 229.04 | 394.54 | 50.05 | 16.37 | 0.00 | 0.00 |
| leaf0-bytes4096-threshold1 | 199.07 | 344.69 | 398.61 | 36.80 | 14.67 | 0.00 | 40.00 |
| leaf0-bytes4096-threshold1024 | 199.07 | 339.02 | 402.24 | 36.80 | 14.67 | 0.00 | 40.00 |
| leaf16-bytes256-threshold1 | 219.79 | 271.95 | 286.10 | 36.80 | 14.67 | 7.11 | 47.95 |
| leaf16-bytes256-threshold1024 | 130.95 | 273.97 | 416.49 | 50.05 | 16.37 | 16.00 | 0.00 |
| leaf16-bytes4096-threshold1 | 207.15 | 357.30 | 421.90 | 36.80 | 14.67 | 7.12 | 40.00 |
| leaf16-bytes4096-threshold1024 | 207.16 | 359.76 | 416.09 | 36.80 | 14.67 | 7.12 | 40.00 |
| leaf32-bytes256-threshold1 | 204.62 | 255.70 | 274.14 | 36.80 | 14.67 | 7.20 | 31.96 |
| leaf32-bytes256-threshold1024 | 147.59 | 304.40 | 459.74 | 50.05 | 16.37 | 31.65 | 0.00 |
| leaf32-bytes4096-threshold1 | 191.62 | 309.89 | 387.17 | 36.80 | 14.67 | 7.26 | 32.00 |
| leaf32-bytes4096-threshold1024 | 191.62 | 315.45 | 387.62 | 36.80 | 14.67 | 7.26 | 32.00 |
| leaf48-bytes256-threshold1 | 189.21 | 251.83 | 287.99 | 36.80 | 14.67 | 7.26 | 15.94 |
| leaf48-bytes256-threshold1024 | 161.94 | 343.57 | 449.59 | 50.05 | 16.37 | 45.06 | 0.00 |
| leaf48-bytes4096-threshold1 | 156.08 | 250.46 | 317.07 | 36.80 | 14.67 | 7.26 | 16.00 |
| leaf48-bytes4096-threshold1024 | 156.08 | 248.92 | 319.35 | 36.80 | 14.67 | 7.26 | 16.00 |
| leaf64-bytes256-threshold1 | 168.18 | 237.15 | 277.74 | 36.80 | 14.67 | 7.26 | 0.00 |
| leaf64-bytes256-threshold1024 | 171.58 | 350.20 | 477.73 | 50.05 | 16.37 | 53.74 | 0.00 |
| leaf64-bytes4096-threshold1 | 115.08 | 203.83 | 270.49 | 36.80 | 14.67 | 7.26 | 0.00 |
| leaf64-bytes4096-threshold1024 | 115.08 | 194.25 | 260.99 | 36.80 | 14.67 | 7.26 | 0.00 |

| Original60 cohort | Get µs/op | Append µs/op | Get B/op | Append B/op | Load µs/op | Update µs/op | Load B/op | Update B/op |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| leaf0-bytes256-threshold1 | 14.05 | 13.66 | 598.54 | 61.53 | 15.09 | 14.56 | 1336.23 | 1270.74 |
| leaf0-bytes256-threshold1024 | 4.92 | 4.78 | 315.65 | 59.62 | 6.28 | 7.36 | 1082.03 | 1085.87 |
| leaf0-bytes4096-threshold1 | 22.97 | 22.17 | 4598.36 | 127.01 | 27.24 | 27.79 | 4986.43 | 5238.55 |
| leaf0-bytes4096-threshold1024 | 22.95 | 22.25 | 4598.37 | 127.00 | 26.14 | 27.40 | 4987.26 | 5239.98 |
| leaf16-bytes256-threshold1 | 3.97 | 3.90 | 547.01 | 63.84 | 15.31 | 15.61 | 1353.15 | 1166.27 |
| leaf16-bytes256-threshold1024 | 1.93 | 1.99 | 324.13 | 59.56 | 7.07 | 7.21 | 1136.30 | 1085.86 |
| leaf16-bytes4096-threshold1 | 11.61 | 10.92 | 4597.94 | 116.57 | 26.45 | 25.46 | 4983.31 | 5238.55 |
| leaf16-bytes4096-threshold1024 | 11.58 | 11.10 | 4597.96 | 116.57 | 27.51 | 25.99 | 4984.75 | 5238.68 |
| leaf32-bytes256-threshold1 | 4.54 | 4.48 | 479.72 | 65.51 | 14.84 | 16.07 | 1353.04 | 1166.19 |
| leaf32-bytes256-threshold1024 | 1.87 | 1.87 | 323.93 | 59.56 | 7.15 | 7.19 | 1148.27 | 1085.86 |
| leaf32-bytes4096-threshold1 | 11.60 | 11.19 | 4528.29 | 104.91 | 25.40 | 25.89 | 4986.88 | 5238.55 |
| leaf32-bytes4096-threshold1024 | 11.58 | 11.11 | 4528.29 | 104.86 | 26.90 | 25.65 | 4986.59 | 5238.56 |
| leaf48-bytes256-threshold1 | 5.26 | 5.21 | 407.96 | 66.19 | 14.63 | 14.48 | 1353.01 | 1166.16 |
| leaf48-bytes256-threshold1024 | 1.87 | 1.82 | 320.88 | 59.56 | 7.08 | 6.95 | 1149.95 | 1085.86 |
| leaf48-bytes4096-threshold1 | 11.37 | 11.08 | 4374.89 | 105.17 | 25.19 | 26.97 | 4987.64 | 5238.59 |
| leaf48-bytes4096-threshold1024 | 11.39 | 11.02 | 4374.87 | 105.19 | 25.39 | 26.47 | 4985.88 | 5238.44 |
| leaf64-bytes256-threshold1 | 5.84 | 5.76 | 317.38 | 60.42 | 14.68 | 16.15 | 1353.09 | 1166.19 |
| leaf64-bytes256-threshold1024 | 1.76 | 1.73 | 316.02 | 59.54 | 6.90 | 7.06 | 1150.60 | 1085.86 |
| leaf64-bytes4096-threshold1 | 11.07 | 10.63 | 4171.25 | 60.18 | 26.06 | 27.14 | 4986.48 | 5242.04 |
| leaf64-bytes4096-threshold1024 | 11.11 | 10.49 | 4171.25 | 60.18 | 25.15 | 25.54 | 4985.92 | 5241.92 |

| Paired70 cohort | Control heap MiB | Trial heap MiB | Trial−control MiB |
| --- | --- | --- | --- |
| leaf0-bytes256-threshold1024 | 113.97 | 91.11 | -22.86 |
| leaf16-bytes256-threshold1 | 219.79 | 210.59 | -9.20 |
| leaf16-bytes4096-threshold1 | 207.16 | 197.95 | -9.21 |
| leaf32-bytes256-threshold1024 | 147.59 | 124.70 | -22.89 |
| leaf64-bytes256-threshold1 | 168.18 | 154.94 | -13.24 |
| leaf64-bytes256-threshold1024 | 171.59 | 148.73 | -22.86 |
| leaf64-bytes4096-threshold1 | 115.08 | 105.88 | -9.20 |

| Paired70 cohort | Metric | Control median | Trial median | Change % | Spread C/T % | Positive paired changes |
| --- | --- | --- | --- | --- | --- | --- |
| leaf0-bytes256-threshold1024 | updates_sync/elapsed_ns_per_op | 6588.4975 | 5993.8536 | -9.03 | 27.33/31.36 | 3/5 |
| leaf0-bytes256-threshold1024 | update_checkpoint/elapsed_ns_per_op | 342751874.0000 | 337647298.0000 | -1.49 | 6.95/6.53 | 1/5 |
| leaf0-bytes256-threshold1024 | updates_sync/allocated_bytes_per_op | 1085.8650 | 1085.8638 | -0.00 | 0.04/0.02 | 1/5 |
| leaf0-bytes256-threshold1024 | updates_sync/allocations_per_op | 2.0367 | 2.0366 | -0.00 | 0.02/0.01 | 1/5 |
| leaf16-bytes256-threshold1 | updates_sync/elapsed_ns_per_op | 14351.6414 | 15724.8167 | +9.57 | 8.99/19.89 | 4/5 |
| leaf16-bytes256-threshold1 | update_checkpoint/elapsed_ns_per_op | 172914242.0000 | 181485929.0000 | +4.96 | 16.86/16.29 | 2/5 |
| leaf16-bytes256-threshold1 | updates_sync/allocated_bytes_per_op | 1166.1532 | 1166.0188 | -0.01 | 9.04/9.04 | 3/5 |
| leaf16-bytes256-threshold1 | updates_sync/allocations_per_op | 2.2580 | 2.2580 | -0.00 | 0.01/0.02 | 2/5 |
| leaf16-bytes4096-threshold1 | updates_sync/elapsed_ns_per_op | 26171.5710 | 28058.4568 | +7.21 | 13.49/16.55 | 4/5 |
| leaf16-bytes4096-threshold1 | update_checkpoint/elapsed_ns_per_op | 163989730.0000 | 184211937.0000 | +12.33 | 21.65/40.16 | 4/5 |
| leaf16-bytes4096-threshold1 | updates_sync/allocated_bytes_per_op | 5238.5890 | 5238.5882 | -0.00 | 0.05/0.01 | 2/5 |
| leaf16-bytes4096-threshold1 | updates_sync/allocations_per_op | 2.3091 | 2.3091 | +0.00 | 0.01/0.03 | 2/5 |
| leaf32-bytes256-threshold1024 | updates_sync/elapsed_ns_per_op | 5802.6647 | 6211.5802 | +7.05 | 24.08/17.64 | 3/5 |
| leaf32-bytes256-threshold1024 | update_checkpoint/elapsed_ns_per_op | 355329706.0000 | 351176833.0000 | -1.17 | 5.56/4.72 | 2/5 |
| leaf32-bytes256-threshold1024 | updates_sync/allocated_bytes_per_op | 1085.8576 | 1085.8646 | +0.00 | 0.01/0.00 | 3/5 |
| leaf32-bytes256-threshold1024 | updates_sync/allocations_per_op | 2.0366 | 2.0367 | +0.00 | 0.00/0.00 | 4/5 |
| leaf64-bytes256-threshold1 | updates_sync/elapsed_ns_per_op | 15300.7096 | 14555.7158 | -4.87 | 26.28/32.01 | 2/5 |
| leaf64-bytes256-threshold1 | update_checkpoint/elapsed_ns_per_op | 169824978.0000 | 166858551.0000 | -1.75 | 17.78/24.19 | 3/5 |
| leaf64-bytes256-threshold1 | updates_sync/allocated_bytes_per_op | 1166.3128 | 1270.8996 | +8.97 | 9.01/8.31 | 4/5 |
| leaf64-bytes256-threshold1 | updates_sync/allocations_per_op | 2.2580 | 2.2581 | +0.00 | 0.02/0.02 | 3/5 |
| leaf64-bytes256-threshold1024 | updates_sync/elapsed_ns_per_op | 6081.0617 | 7031.1263 | +15.62 | 26.63/22.17 | 3/5 |
| leaf64-bytes256-threshold1024 | update_checkpoint/elapsed_ns_per_op | 351053174.0000 | 351785818.0000 | +0.21 | 9.49/8.25 | 2/5 |
| leaf64-bytes256-threshold1024 | updates_sync/allocated_bytes_per_op | 1085.8588 | 1085.8572 | -0.00 | 0.00/0.00 | 2/5 |
| leaf64-bytes256-threshold1024 | updates_sync/allocations_per_op | 2.0366 | 2.0366 | -0.00 | 0.00/0.00 | 1/5 |
| leaf64-bytes4096-threshold1 | updates_sync/elapsed_ns_per_op | 28249.9834 | 25671.6842 | -9.13 | 10.75/15.97 | 2/5 |
| leaf64-bytes4096-threshold1 | update_checkpoint/elapsed_ns_per_op | 167989978.0000 | 163797501.0000 | -2.50 | 19.59/28.80 | 3/5 |
| leaf64-bytes4096-threshold1 | updates_sync/allocated_bytes_per_op | 5242.0404 | 5240.0448 | -0.04 | 0.07/0.07 | 2/5 |
| leaf64-bytes4096-threshold1 | updates_sync/allocations_per_op | 2.3104 | 2.3104 | -0.00 | 0.02/0.03 | 2/5 |
<!-- canonical-tables:end -->

## Rejected mechanism and remaining bounds

The trial used a small mutex-protected helper to drop whole free entry buffers,
largest class first and last slot first in current bin order, at the end of
checkpoint maintenance. It reused the existing 32 MiB generic-entry target and
retained/drop accounting. It changed no pressure/global admission policy,
public knob/default, ordinary flush, format, CRC, pointer reachability or
durability frontier. Its source-only ownership/regression review passed.
Even its proposed bound applied only while the trim mutex was held: later
returns can raise free capacity. It was never a total-process controller.

Targeted inline direct GC2 heap fell about 22.8 MiB; inline free-bin backing fell
52,483,112 → 28,526,872 B, and pointer backing fell
38,583,600 → 28,938,800 B. That establishes the trial's heap mechanism, not an
improvement in the selected baseline. Warm guards remain unresolved:
random4096/leaf16 median updates +7.21%, update checkpoint +12.33%, each with
4/5 positive paired changes; inline256/leaf64 median updates +15.62% with mixed
paired directions. Update allocation counts were essentially equal. Timing
variation/sign reversals do not prove noise or causation. The trim stage took
microseconds, while slower earlier checkpoint stages precede the first trim;
this does not explain the full timing gap.

Four fresh labeled CPU diagnostic processes passed full oracles. They showed
no sampled update-tagged GC assists, fewer update CPU samples in both trial
processes and no consistent extra update GC cycles. Short profiles, observer
effects and unlabelled background CPU leave attribution inconclusive. Those
profiles neither explain nor waive canonical70 guards. No supported corrective
edit or new controller follows from this packet.

Also reject deleting decode scratch reuse, draining all bins every checkpoint,
changing DB idle lease counts/its unused size argument without new capacity
proof, or choosing universal pointer/cache defaults from this warm fixture.
Maintain owned Get outputs, held iterators/snapshots and persistent pointer
reachability under the [contracts](../../../TreeDB/docs/spec/contracts.md),
[durability](../../../TreeDB/docs/spec/write-path-and-durability.md) and
[value-log lifecycle](../../../TreeDB/docs/spec/value-log-lifecycle.md).

Revisit the same seven five-pair full-workflow guards on a dedicated stable host,
or when a controlled public workload exceeds a declared process-memory
requirement. Isolate changed owner lifetime/reuse or GC pacing with exact
source/binary/env and concurrent stage counters before another policy. Keep the
trial objective of ≥16 MiB fresh post-GC heap reduction where ≥18 MiB free
backing is removed, and repeatable write/allocation/checkpoint/read/peak guards;
lower bins alone or weakened thresholds do not qualify a replacement. Revisit
DB leases only with excess backing beyond their current hard-reset baseline;
revisit decode leasing only if de-aliased scratch becomes material and current
admission/budgets fail.

[#3589](https://github.com/snissn/gomap/issues/3589) retains broader durable-writer
allocation/RSS obligations. [#4894](https://github.com/snissn/gomap/issues/4894)
integration and [#4895](https://github.com/snissn/gomap/issues/4895) final 96-cell
plus fresh maintenance/residency/write/read/durability guards remain active;
#4916/#4917/#4920 obligations remain unchanged. This decision PR still requires
independent exact-head review, current required CI, thread classification and
coordinator merge before #4892 closure. No runtime-red-test claim is made for
this documentation-only stage.

## Retained provenance and replay commands

Retained coordinator export directory: `tmp/treedb-quicksilver-graph-20261001`.
Native original60: `/mnt/fast4tb/gomap-quicksilver-full-20261002/T/{prepared,runs}`.
Native original42 trial: `/mnt/fast4tb/gomap-quicksilver-full-20261002/T/paired-4915-ac476-v1`.
The extension manifest records its separate directory and every original/new
run; preserve both original and extension packets. These retained files are
external evidence, not bundled binaries or a claim they are publicly downloadable.

| Retained artifact | SHA256 |
| --- | --- |
| `memory-full-analysis-output.json` | `e2c59b89f41827ce4ed0c69e8b083a28c9137f4a6061b8738fe0dcc23cd3a10f` |
| `checkpoint-paired-4915-analysis-v2-output.json` | `23dcc8d798882329d7e572617c83f614eedb56367142ea9ad908c50025d2ac01` |
| `checkpoint-paired-4915-five-repeat-compact-summary.json` | `65b62c98adf151f197c50a7bf4653ae99ac4b5227ddc331a18d00c1237ceb7cb` |
| `memory-full-owner-decision-review.md` | `cf403785770076762ed2c12b3fb82735ee84ce7b92f327ff99449dc96ab2312f` |
| `4915-independent-source-review.md` | `8cc9baffcb7144181ff8486191d170977feb22a58b4a2837a1fccccf0769d74a` |
| `4914-measured-disposition-independent-review.md` | `53b791781695abb3b708f882c3d6755cf1114d2e57bec4bdf7438a4694f8f6c8` |
| `4915-phase-cpu-independent-attribution-review.md` | `06c4c8f43cf21a781a4c0e6dffd8b4a2d568a0a3f9d928573065194746aaaca1` |
| `memory-full-analysis-input.json` | `9b7c28cdf9425b9223403d4cf2c943f15941d94366db5e111ca5b72a5adf999f` |
| `memory-full-all20-preparation-anchor.json` | `31fb7311e80eb3ea737b0c16e6575a091382ace193681d1f5fdc17246738e2b3` |
| `checkpoint-paired-4915-plan.json` | `3f818a5e10189aa2b7fb5ab36d81524a43e7ed43b601bc462a8d65ef1fe115b2` |
| `checkpoint-paired-4915-all14-anchor.json` | `ed5aa183fac7fe33fa70eff5ff34de82f4b3bc47229e4f37336580bdc7ea0f09` |
| `checkpoint-paired-4915-extension-plan.json` | `7ddfd2c31c4e173e9f2101f711dcd820ad0b3ac40deb45e63166feff3c4401a0` |
| `checkpoint-paired-4915-analysis-input.json` | `33daf7299ac08fcdffdb4dac1e405a2921b0dbd41744762d3bbb1eb877f4ecfe` |
| `checkpoint-paired-4915-extension-analysis-input.json` | `24baf27e53aa1d9de1be2a9fadadae0b7e09444361fdff416219ce7030e95ec6` |

The original analyzer helper SHA is
`1d9e37e1e0a4db39273a538158b72115116f28401b540947d09d3d1dc18d22aa`;
paired70 analyzer SHA is
`eaf4c6e579bb080eb9ae17134502df6a182bb33e1b5687c7d82e62e38b291e81`.
The unchanged landed `TreeDB/memory_budget_bench_test.go` SHA is
`03b389e475374af7157111b4a8feea72a22124aa4fec5f3342d512ea6cbad462`.
The original60 anchor records all20 environment/source/overlay/binary identities;
the paired anchor binds all14 control/trial preparations. Complete device/host
pre/post records and the exclusive serial grant remain in the retained packets.

Use [the canonical capture workflow](README.md#fail-closed-capture) to prepare,
run and validate each fresh process with its externally preserved freeze SHA.
The actual workload argv (with each preparation's absolute executable) was:

```sh
/path/to/prepared-cohort/memory-budget.test \
  '-test.run=^$' '-test.bench=^BenchmarkMemoryBudgetWorkflow$' \
  -test.benchtime=1x -test.count=1 -test.benchmem -test.v -test.timeout=60m
```

Full cohort controls are `TREEDB_MEMORY_PILOT=0`,
`TREEDB_MEMORY_LEAF_MIB=0|16|32|48|64`,
`TREEDB_MEMORY_VALUE_BYTES=256|4096`, and
`TREEDB_MEMORY_POINTER_THRESHOLD=1|1024`; each preparation compiles its literal
MAIN frame overlay. Replaying needs the complete pinned preparation/toolchain
inputs and fresh output directories; bare benchmark output is insufficient.

Check the committed tables and source semantics without launching Go/native
workloads (the retained export files above must be present):

```sh
python3 docs/benchmarks/treedb_memory_budget/check_owner_decision.py \
  --evidence-root /path/to/treedb-quicksilver-graph-20261001
```

This check hashes both canonical analyses, the compact70 projection and the
review reports, compares every published numeric table cell, checks local links
and current owner-policy constants. It consumes the retained canonical validator
results; it does not independently revalidate unavailable native binaries or
replace review/CI/final acceptance.
