# P1 repair19 evidence and intermediate disposition

Published PR: https://github.com/snissn/gomap/pull/5146

Published head `d64fba3d88fe15bd89bf086face1981a7d3f6b97`, parent `bdd08a129ee202c582aaf4b37356b0cba9f14f5a`, exact tree `9a56421e3aca8265a3eb4cd591eb6187f536eb55`. Local frozen source `1fedf4de8f2b8945089dfc814242ca4cd28696f0` has that tree. Original project baseline remains `0149d78dba2574b09d7119d2631ec78a144e088d`. No production/harness drift after freeze. Published commit/tree/parent and PR branch read back through GitHub.

## Coherent repair

Exact-binary allocation profiles from the first hosted PRIMARY failure attributed the additional allocation to `History.Prepare`. Generic singleton keys up to32bytes now belong to the original vacant FIFO slot. Install preserves that backing; eviction retires matching derived-index aliases before reuse. No managed-key specialization, skipped logical writes, active-count shortcut, new pool, or dynamic per-point lease. Longer/bulk/range storage keeps its existing owned arena. Larger fixed slots are included in admission.

The backend compatibility transaction now drains prior acknowledged registered producers before its opening snapshot. Commit-time draining, conflict checks, and actual sync remain. Failed opening does not admit a transaction or assign an LSN. Three fixture repairs close a newly enabled reverse iterator, accept existing platform-unsupported vacuum while checking logical views, and wait for actual durability after hard queue admission before asserting the model.

## Correctness

`repair19-correctness-receipts.json` binds exact argv, source, raw output and hashes. Normal, race and safe runs pass across eleven affected packages. The additional recovery/reopen run passes. All original failure semantics and new inline-key cancellation/wrap/caller-reuse and opening-failure/capture controls are included. Four CI inventory/contract suites pass. Source discovery changed; selection did not.

## Bounded performance observations

All results remain raw observations, not sustained or final landed-source Q qualification. Original failed attempts and the earlier frozen18 archive remain part of the record.

The unchanged raw gate passes all five paths across eight balanced pairs. Backend batch has median paired +2.15% time, 104→103allocations and31504.5→31501.5B/op. Durable tiny write has paired +4.04%,16→16allocations,4471.5→4480B/op. Read/iterator gates pass. Exact measured executables and build recipes are retained for this current candidate.

The enabled capture completes three fresh processes ×30shapes ×128operations, with actual source/build bindings, counter validation and final resource-drain observations.

Ordinary COW uses the same six cells, three before/after pairs,512fixed writes per sample, GOMAXPROCS2 and normal GC. Median allocation counts now match the original baseline in all cells. Fixed publication/transaction custody metadata remains; inline byte increments are32–48B/op. Pointer byte totals include the full asynchronous fixture work and vary across samples. The table reports actual observations; it does not attribute all timing differences to the product or to host noise.

| Profile/layout | Base→head ns/op | Base→head ops/s | Paired time delta | Base→head B/op | Base→head allocs/op | Base→head p99ns |
|---|---:|---:|---:|---:|---:|---:|
| durable/inline |16152→20730|61912→48239|+33.26%|10722→10770|70→70|56755→108032|
| durable/pointer |49595→66616|20163→15011|+18.68%|90857→93921|87→87|221553→313460|
| relaxed/inline |14857→16841|67308→59379|+13.17%|10722→10770|70→70|98157→55753|
| relaxed/pointer |136641→170013|7318→5882|+9.03%|77013→77543|160→160|570516→512510|
| NoWAL/inline |13663→15303|73190→65347|+19.92%|10026→10058|67→67|101172→67841|
| NoWAL/pointer |37088→31267|26963→31983|−19.61%|56134→58714|82→82|245939→233110|

The first local PRIMARY attempt fails before samples because `ps` cannot resolve its virtual PID against the host `/proc`. An external adapter binds the original script and the exact two portability changes: observe its root PID through `/proc/self`, and use the original `/proc` parser for informational process inventory. Workload, lifecycle/activation, sample order/count,5%time, strict allocation/byte parity and2%external CPU quiet limit are unchanged. Both the namespace-adapted and outside-workspace-output attempts refuse preflight: actual rsync/inotify CPU exceeds the limit. All three attempts collect zero timed samples. No process exclusion, weaker limit or replacement passing measurement is introduced. The unchanged hosted gate is still mandatory on the published head.

## Coordinator acceptance boundary

The coordinator accepts this bounded ordinary-COW cost and unresolved timing/tail uncertainty for intermediate P1 landing, extending the earlier explicitly scoped decision. This is an acceptance of the disclosed risk, not a claim that +33.26% is harmless noise or that latency is explained. The short-key allocation source has been removed using existing fixed custody; the retained mutation history and publication bookkeeping implement the accepted transaction contract. Prior exact-binary profiles/ablation remain causal evidence for history cost, not an explanation of every observed tail.

This disposition does not waive the raw or PRIMARY gate, current-head CI, review, or any final product outcome. Q must resolve sustained throughput, p99, live/peak memory and ACK-to-drain behavior on the integrated landed engine under default pointer compression/dictionary I/O. Failure there leaves the graph open and requires implementation work; it cannot be converted into completion merely by linking follow-ups.

INFRASTRUCTURE_UNAVAILABLE: dedicated quiet qualification runner. Required hosted CI is running for the exact published head. No merge readiness or review acceptance is claimed here.

## Storage and evidence retention

Verified frozen18 archive: https://github.com/snissn/gomap/tree/b726a1c871b1e83dfbad6381df317600658c46ec/docs/evidence/native-mvcc-5140 . Earlier retired executable copies and derived trace text were released after consumers finished; raw traces/profiles, source/recipe/binary hashes and successful/failed results remain. Cleanup receipts describe the actual deletions. Historical, closed entries in this graph's isolated build cache were also reclaimed under an exact allowlist after qualification completed; recent build and module caches and all source worktrees remain.
