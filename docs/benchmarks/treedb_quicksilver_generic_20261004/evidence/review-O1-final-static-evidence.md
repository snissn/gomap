# Independent O1 final static evidence review

2026-10-04; exact inherited GPT-6.1 Sol. Bounded read-only review of `O1-long-static-guard-summary.json` and `O1-composed-fast24-guard.json`, with prior short/static runtime evidence retained. No Go/native execution, production source changes, CI/GH actions, delegation or cleanup.

## Recommendation

**Continue the remaining declared guards on the current source; these new findings do not justify a source fix.** The changed cached reader is concretely beneficial under concurrent mutation, and the new sustained-static allocation measurements do not reproduce the original allocation increase. Static throughput and checkpoint differences remain observations to disclose, rather than evidence of a proved defect in that changed reader. No generic cached-routing patch, new algorithm or weakened durability is warranted.

This is not a blanket performance acceptance or a relabeling of the original short anomaly. The original 100k-read packet remains an unresolved workload-specific tradeoff: mixed throughput ratios0.843/0.763/0.839 and B/op about+8%. If the acceptance contract requires that precise cell to pass, these longer cells cannot substitute for it. Root owns the final acceptance decision and the outstanding off/original3M Sync guards.

## Verification receipt

- All66 long-static and22 fast24 declared raw SHA256 hashes match retained artifacts, with no missing file or mismatch.
- All eight runs return zero, record `validated=true`, and report `profiled=false`. Actual argv, raw config, source/binary identity, plan/manifest hash associations, receipt bindings and native ldd/library bindings agree. Source/build heads are O3 `5100fc295f02c5dc451c67cc1ddd30b2194d06c6` and O1 `63794357c03898da03a37c5412770fdf197f2ce0`; current Git diff remains the cached Get delegation plus tests/docs. Both use the same declared dynamically resolved native library hashes and successful native validation. Remote installed files were not rehashed anew during this review.
- All six predeclared long-static plan hashes match. Actual fixed read count is6000000, keys200000, holdout173,20% working set,70% misses,4 workers,64 reads/snapshot,1s concurrent duration,1000 updates, fast profile and explicit Sync. Initial/final full-byte checkpoint/reopen verification counts are200000 keys,402000 initial misses,402250 final misses in every run.
- Both fast24 runs use fresh3M primary seed24/uniform/90% miss,6M static reads,4 workers,64 reads/snapshot,8s concurrent duration,40000 updates, fast profile with **ordinary commits and explicit initial/final Checkpoint boundaries**. Initial/final full-byte checkpoint/reopen counts are3000000 keys,6030000 initial misses,6040000 final misses. These are not explicit-Sync-load guards.
- Every reported phase throughput and B/op ratio was recomputed from raw stdout and agrees with the summary to relative tolerance1e-12. The mixed paired median is0.9606628589974402. The summaries' reported checkpoint/load/oracle values agree with raw reports.

## Sustained static and concurrent results

| Long-static pair | Mixed throughput O1/O3 | Mixed B/op O1/O3 | Concurrent throughput O1/O3 | Concurrent B/op O1/O3 |
|---|---:|---:|---:|---:|
|1|1.126116|0.998733|2.005425|0.032000|
|2|0.960663|0.989487|1.950515|0.034964|
|3|0.932784|0.982919|1.864275|0.035077|

Mixed exact allocation decreases in all three long pairs. Throughput has one+12.61% result and two decreases of3.93%/6.72%; the median is a3.93% decrease. These observations do not prove static parity, a confidence interval, or random noise. Hits/misses also change direction across pairs. The concurrent improvement is much more consistent: throughput rises86.43–100.54% and process B/op falls96.49–96.80%, supporting the intended cached-read mitigation under mutation.

The new fast24 mixed result is0.990935 throughput and1.006101 B/op; concurrent is1.426360 throughput and0.018875 B/op, with composition17.829→17.547s. Static hit B/op rises4.52% in that pair despite an unchanged backend static reader. The facts warrant reporting the broader static result distribution rather than presenting a single favorable metric as a universal gain.

The prior six-million-read **profiled** diagnostic in `analysis-O1-static-runtime-and-guards.md` confirms the dominant actual receiver: `db.(*Snapshot).Get → tree.(*Tree).Get`, reached by the public backend snapshot fast path. Both profiles show that unchanged reader and no attributed changed `caching.(*Snapshot).Get` path. Exact phase allocation there differs+0.374%, versus about+8.96% in sampled alloc_space estimates; the estimates are not exact counters. The new unprofiled runs use the same frozen binaries and static setup, but do not themselves capture receiver type for every request. Neither the profile nor source equality eliminates binary-layout, physical load grouping, cache admission/residency or GC-state interactions.

Thus the strongest supported statement is that a direct extra-materialization defect in O1's changed cached Get is **not demonstrated for static phases**. The original short slowdown is real evidence, but its upstream cause remains unresolved. Longer read history changes cache/admission/GC exposure, so a 6M result is a distinct workload horizon rather than a statistical dismissal of100k.

## Fast24 initial checkpoint: where the extra time occurs

Initial Checkpoint rises3102.315590→3628.525495ms, a526.209905ms increase or16.9618%. The harness executes load writes, disjoint deleted-fixture Set/Delete batches and pure fixture/compressibility analysis before this first Checkpoint (`637943:cmd/unified_bench/suite_quicksilver.go:775–807`; `suite_quicksilver_realistic.go:372–413`). Initial full-oracle reads and warmup occur only after checkpoint/close/reopen (`suite_quicksilver.go:818–854`). There is no harness Get/snapshot read on this pre-checkpoint path; O1 changes cached Snapshot.Get only. That excludes a direct changed-reader call explanation, while still allowing indirect compiled/scheduling or physical-state effects.

| Existing initial checkpoint counter | O3 ms | O1 ms | Difference ms |
|---|---:|---:|---:|
|Active background flush wait|1930.980061|2274.305293|+343.325232|
|Checkpoint flushAll stage|1048.858621|1219.749087|+170.890466|
|Backend boundary|116.692014|128.838795|+12.146781|
|Leaf/value-log sync attribution|335.718837|324.229099|-11.489738|

The first three timer differences sum526.362479ms, approximately the full extra wall time; remaining stage differences/measurement granularity account for the small residual. This localizes the result to longer unchanged background wait and owned drain, with a small backend-boundary increase. It does **not** establish why they were slower. The leaf/value-log sync attribution is collected from vlog flush duration and apply leaf-append-wait deltas (`TreeDB/caching/db.go:25606–25611`), and overlaps those stages; do not add it as an independent time. The flushAll timer covers bounded frontier draining (`:25547–25602`); backend boundary explicitly waits through root durability before cleanup (`:25647–25658`). All of that durability behavior remains intact.

Captured debt is identical:134340956 active-in-flight bytes,235221348 queued bytes,19738755 mutable bytes, and147000 checkpoint-owned drain operations. One cannot explain the extra time by claiming more captured queued bytes. The physical database nevertheless differs: initial files total2189028674 versus2188249581B; leaf-log bytes1244019408 versus1243693947; value-log bytes878423835 versus877970203; index files equal66584576B. Written leaf counts604950/604951 differ by one4096-byte leaf. These are actual physical/grouping differences, not controlled identical physical input or a proved explanation for the timing increase.

Earlier retained original-runtime fast24 checkpoint guards moved the opposite way (9032.573→8837.002ms initial), while durable91/off increased. The current durable91 three-pair final checkpoint observations also reversed dramatically between pairs. They use distinct source/runtime or physical states and cannot cancel this current+16.96% fact. They demonstrate why checkpoint stage/debt/source identities must be retained when assessing it.

## Action boundary

No code repair is causally justified by the new static/initial-checkpoint packet. Proceed with the already-running declared off and original3M Sync guards; preserve every result and source identity. Those remaining guards can reveal a repeatable boundary-specific cost, but their existence alone does not require a speculative source fix.

If final performance acceptance remains blocked by static or checkpoint criteria, the next useful discriminator controls or explicitly measures physical input, warm-cache history and unchanged flush/read stage work. Use existing profile/counter seams on the failing boundary; avoid broad reruns or changes to cached routing before attribution. The current reader-optimization evidence supports the concurrent mechanism, while the static median decrease, original short anomaly and fast24 checkpoint increase must remain visible in the readiness report. This review neither grants a waiver nor claims a universal regression-free result.

## Addendum: current 3M compression-off guard

Read-only review of `O1-composed-off-guard.json`, `o3-o1-composed-off_guard/1-*/` and `o1-o1-composed-off_guard/1-*/`. All22 declared raw SHA256 hashes match. Both fresh native cells have rc0, validated=true, profiled=false, exact source/build/manifest/native-library bindings, and identical actual workload configs/argv apart from the bound binary. Baseline is5100fc295f02c5dc451c67cc1ddd30b2194d06c6; candidate is63794357c03898da03a37c5412770fdf197f2ce0. These are durable-profile, ordinary-commit, generic-v1, seed24, uniform working-set,90%miss cells with compression=off,3M keys,6M static reads,4workers,40k updated logical keys and8s concurrent reader windows. They are not explicit-Sync LOAD cells. Initial and final reopen full oracles pass:3M keys/6.03M misses initially and3M keys/6.04M misses finally. Raw phase ratios agree with the summary.

| Measurement | O3 | O1 | O1/O3 |
|---|---:|---:|---:|
|Static hits Mops/s|1.461633|1.481068|1.013297|
|Static misses Mops/s|2.373906|2.222520|0.936229|
|Static mixed Mops/s|2.127855|2.255565|1.060018|
|Concurrent reader Mops/s|0.629509|0.855171|1.358473|
|Concurrent reader B/op|3632.460893|106.085660|0.029205|
|Initial Checkpoint ms|1654.005707|1668.943597|1.009031|
|Final Checkpoint ms|59.214653|136.382024|2.303180|
|Sum of all six explicit Checkpoints ms|14378.014375|14556.072393|1.012384|
|Sum of40 timed update groups ms|1502.590888|1627.432459|1.083084|
|Concurrent full composition s|15.347537|15.612586|1.017270|

The four concurrent Checkpoints total12664.794015→12750.746772ms. The explicit checkpoint sum increases178.058018ms; the final boundary contributes77.167371ms of that increase. These checkpoint/update sums are component measurements already inside the concurrent composition where applicable; do not add them again to workflow/composition wall time. The update group timer covers its four mutation-generation commits, totaling160 commit batches in the report.

Initial storage is3525827074→3544107167B (+18280093B, approximately0.52%); final storage is3656186027→3674897234B (+18711207B, approximately0.51%). Growth is130358953→130790067B (+431114B). Correct logical contents are established, but identical physical grouping/storage is not established. Static B/op remains approximately equal (hits1.000035, misses0.999814, mixed1.000639 ratios). The6.38% miss-throughput decrease is a retained observed cost of this pair, not dismissed as noise or canceled by mixed/concurrent improvement.

### Final checkpoint attribution: public tail, not cached drain

The harness captures concurrent `stats_after` after writer join and captures `final_stats` immediately after final Checkpoint, before close (`637943:cmd/unified_bench/suite_quicksilver.go:924–925`). Differencing these two actual snapshots isolates one final cached checkpoint in each cell: checkpoint runs+1, noop_skips+1, backend-boundary samples+1, cutover samples+1; flushAll/reducer/leaf-sync sample counts do not increase. Final captured mutable/queued/active/frontier debt is zero in both. Therefore the1.095/1.115s flushAll and0.971/0.984s reducer `.last_ns` values are stale from an earlier full checkpoint and cannot explain this59/136ms boundary.

The cached checkpoint total increases only1.147ms baseline and1.062ms candidate. Barrier wait is353/272ns, cutover1880/1345ns, backend boundary4684/4347ns, and timed cached command-WAL cleanup1130032/1047577ns. Public checkpoint timing is59213079/136380334ns and matches the harness final wall measurement within approximately2us. Hence virtually all of the extra77.17ms lies outside `cached.Checkpoint()`, rather than in an increased cached drain or captured write debt. The public-minus-cached residual is approximately58.07→135.32ms.

The public wrapper times lifecycle admission and then `checkpointCachedForPublicCommandWAL()` (`TreeDB/public.go:2563–2605`). After cached checkpoint completion it refreshes command-WAL fallback durability, cleans covered command-WAL segments and clears pending coverage. Fallback refresh serializes with backend raw publishers, waits through the visible root, and, when required, republishes the same root/same LSN to converge both recovery slots; it then waits through the refreshed commit and checks slot convergence (`TreeDB/command_wal_public_cached.go:436`; `TreeDB/db/command_wal_publish.go:261–343`). Covered-segment cleanup takes the maintenance lock and performs existing cleanup with proof/namespace-sync rules (`TreeDB/db/command_wal_raw.go:755–776`). These source paths are unchanged by O1.

Actual final-snapshot cleanup deltas show two scans in each cell,80 frames/10725048B scanned, one5362524B segment removed, and one namespace-sync call. Total cleanup duration grows8.115575→9.816542ms (+1.700967ms); some is nested in the cached cleanup timer. This cannot account for most of the public-tail increase. There is no isolated fallback-refresh/lifecycle wait duration in this packet, so attributing the remainder specifically to root publication, a syscall, or lock contention would overclaim. The final cached noop branch is proven by the counter delta; its conditional sparse-index vacuum/retention/arena-trim work is inside the approximately1ms cached duration, not the large residual (`TreeDB/caching/db.go:25325–25448`).

O1 changes owned cached Snapshot.Get materialization, with no direct call to its changed reader routine during final checkpoint. Indirect allocator, cache, scheduling or lock history may affect unchanged publication work; this packet does not identify which. A direct changed-read-path defect is not demonstrated, and no source repair is causally justified by this singleton. If this boundary remains an acceptance blocker, the smallest next discriminator is an existing final-checkpoint CPU/syscall capture plus the same before/after counter delta, focused on `RefreshCommandWALCheckpointFallback`, coordinator publication waits and public lifecycle/maintenance-lock admission. Preserve exact physical receipts and the77.17ms cost. Original short-static anomaly disposition and the required3M explicit-Sync guard remain root-owned outstanding gates; this addendum grants no waiver.
