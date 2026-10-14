Public checkpoint handoff repair and bounded pointer diagnostic — 2026-10-14

The checkpoint repair uses the existing COW backend-maintenance handoff around public command-WAL fallback refresh. Actual public durable/relaxed × inline/pointer cases reproduce the stale successor basis and checked Close failure before the change, then pass after it. Eight focused normal and ten race controls pass, including unchanged strict stale-basis rejection. Source, tests, docs, RED/GREEN receipts and exact census are preserved under checkpoint-repair/.

Both pointer comparison variants adopt the IDENTICAL checkpoint repair. Only the six frozen captured-successor production deltas distinguish baseline and prototype. The common harness adds post-timer sum/mean/first/max/index statistics from existing arrays; timed operations, seeding and warmup behavior are unchanged. All eight authorized calls passed. No further collection was run.

Per public CommitAt20+GetAt15 unit, two balanced64x pairs per size:

| Forced pointer fixture | Before→prototype ns/op | B/op | allocs/op | reader p50 ns |
|---|---:|---:|---:|---:|
| small |114426.5→126576.5|76556→29365.5|186→174|15920→13275|
| multilevel |159746→113898.5|87062→39845|191→179|19855→14835|

Small total elapsed is worse, despite improved read latency and allocation. Every small writer maximum occurs at index42; every multilevel maximum at index19, approximately3.6–7.8ms. First writer calls are much shorter. This proves a midstream writer outlier, not its cause. Zero timed fsync, directory-sync, segment-creation or checkpoint deltas were observed. Raw total elapsed remains the acceptance measure; maximum removal, arbitrary warmup and median-only acceptance are not allowed.

Production M5 remains NO-GO. This artifact commit preserves source-bound component evidence; it is not the compiled native runtime, a production-code merge, final5111/4878 acceptance or full M0 qualification. Next: one bounded source-only writer-trigger diagnosis, then choose the smallest consequential inquiry or coherent production integration. Original graph/per-PR/final gates remain binding. Parent4871 is INCOMPLETE.

Unique worktrees, binaries and evidence remain retained; no cleanup release is granted. Two exact binaries are intentionally omitted from Git and identified in the manifest. Root owns integration and final acceptance; standing skill owner has received findings and confirmed the generalized public-entry/activation/proportional-lifecycle test requirements cover these cases.
