# P1 frozen18 performance disposition

Acceptance owner: graph coordinator, under the maintainer's execute-and-merge authorization for #5139. Applies only to raw-engine packet #5140 / PR #5146, exact source tree bed3920afec44abf68dfb5736a846255c2e04f27 (measured local c23338b31cd1c3af42356d4d68a7a85a22498eb4; published bdd08a129ee202c582aaf4b37356b0cba9f14f5a).

## Decision

Accept the minimized continuous-history and owned-key feature cost for intermediate P1 landing. Explicitly accept the retained ordinary COW comparison's residual timing/tail risk at this node. This is a scoped engineering acceptance, not proof that every observed tail increase is unavoidable, a no-regression finding, or permission to weaken a numeric gate. P1 remains subject to complete current-head CI and mature review. The parent graph and Q #5145 are not performance-qualified by this decision.

Continuous mutation history is part of the accepted engine contract, including ordinary writes when no transaction is currently open. Reverting to active-reader history suppression, borrowing resettable batch key arenas, or discarding assigned publication ownership would weaken that contract. The final implementation shares one bounded FIFO/index, preadmits its fixed storage, uses by-value preparation capabilities, owns keys through final eviction, removes redundant successful statistics refresh, and removes both old active-start/recent maps. Profiles and source review found no remaining duplicated journal or identified material avoidable allocator path.

Two point-write objects remain: immutable history key backing and its existing COW allocation lease. The partial ablation removes these two objects, 513 dynamic leases and 40,992 charged retained bytes in the fixture, while keeping fixed history construction and the ordinary publication/logical-clock path. Normal-GC pointer timing supports a feature cost; inline results and tails are inconsistent. A shared resizable lease or a new key arena would introduce new ownership/accounting machinery without a measured material site to justify it. No speculative implementation is selected.

## Evidence and limitations

The unchanged hard raw-path guard passes all five paths on frozen18: eight balanced pairs; backend batch paired time +3.0449%, allocations104→104 and bytes+29.5; durable tiny write−0.9601%, allocations16→16. Every read/iterator guard passes. Earlier failed15/17 evidence is retained. Current-head hosted execution of that gate remains mandatory; this disposition cannot waive it.

The ordinary COW diagnostic uses the unchanged public benchmark,1024 seeded keys,512 point replacements,Inline64/Pointer4096,three alternating pairs,Go1.26.0,GOMAXPROCS2,normal GC,disabled automatic maintenance and default pointer compression. Setup/Close are excluded; each operation uses the existing StartTimer/StopTimer boundary. These are bounded process observations, not sustained workload qualification.

| Profile / layout | median ns/op before→after | derived ops/s before→after | paired time delta | allocs/op before→after | median B/op before→after | median p99 ns before→after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| durable / inline |17058→17702|58624→56491|+3.78%|70→72|10722→10810|76515→69114|
| durable / pointer |46945→56602|21302→17667|+11.23%|87→89|90597→92496|268399→306581|
| relaxed / inline |13976→14842|71551→67376|+5.61%|70→72|10722→10810|67301→65348|
| relaxed / pointer |160198→180406|6242→5543|+13.62%|160→162|78208→77172|451770→563728|
| NoWAL / inline |11035→13398|90621→74638|+6.74%|67→69|10026→10098|33681→38988|
| NoWAL / pointer |27838→36203|35922→27622|+17.32%|82→84|56500→56874|208764→229636|

Paired deltas are medians of per-pair ratios and need not equal the ratio of marginal medians. Pointer bytes include variable compression/runtime work. The relaxed-pointer p99 raw samples are before206841/451770/549662ns and after563728/1902913/462400ns; do not hide the1.903ms sample.

The one marked trace pair isolates four large inherited unix.Dup outliers at the same operation ordinals on both sides. It observes real asynchronous dictionary-store sync work, but the two p99-defining operations do not overlap that trainer, foreground GC assist, STW or synchronization waits. Their remaining traced Running time cannot distinguish application CPU from OS descheduling. Consequently neither dictionary activity nor the trace excuses or explains the original candidate p99 increase. The diagnostic is retained as inconclusive on that specific cause.

## Obligation carried into Q

The final graph must run its planned landed-source sustained mixed-write/checkpoint/rotation/pin-release qualification and report ACK versus full drain, throughput/tail distributions, memory plateau and feature-enabled costs. Include default pointer compression and account separately for trainer/dictionary I/O; do not interpret zero main-WAL/value-log sync counters as absence of all I/O. Preserve the original baseline and frozen18 observations. An unexplained final integrated regression remains a final-graph acceptance decision, not a completed obligation or a promise assigned to an unrelated future project.

No new source patch, full matrix retry, benchmark threshold change, performance-observation-only label or workflow alteration is authorized by this disposition.
