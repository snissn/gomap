# Independent offline M0 observation assessment

**No causal production repair is justified by this packet. The original M0 failure remains unresolved.** This one diagnostic run reproduces the prescribed workload without reproducing its prior instability. It does not replace or clear the original acceptance gate.

Bound packet: run **38824531924**, observation harness **eae34350200668e1aae35d56f7f4c8ddfc2acb47**, unchanged product **2588cd26780fe9ffd8c33797fc6a2bfdf2c00d49**, actual **Go 1.26.9**, actual retained ELF SHA-256 **e50c4926d0d8d613b78ea509b5983115f313dff3a71c34e70c686df684ceb0c3**. No Go execution, tests, builds, profiles, benchmarks, dispatch, or product/harness/Git changes were performed in this review. Only offline parsing and these local analysis artifacts were written.

Root's separately retained `packet-verification.json` verifies 124 internal hashes (1,563,394,479 bytes), 10,246 product Git/archive files, 9,942 harness files, 15,045 official SDK files, all 37 successful command outputs and 22 actual executable launches. Its receipt and the selected input hashes are bound in the JSON companion. External ZIP digest metadata was not independently verified.

## What the raw observations establish

All ten legacy exports contain 4,096 correctly ordered alternating point/range durations (40,960 total; 2,048 of each kind per arm). Independently recomputed nearest-rank combined p99 matches every corresponding raw benchmark line. Numeric-label parsing avoids the summary's lexicographic filename sample order (`1,10,2,...`). All 20 raw arm metrics are retained in the JSON companion.

| Legacy arm | Vacuum s | Writer pause ms | Foreground p99 ms | GC cycles | Total GC pause ms | Max GC pause ms |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 16.372 | 64.511 | 43.603 | 16 | 0.874 | 0.142 |
| 2 | 16.279 | 61.768 | 42.252 | 15 | 0.748 | 0.080 |
| 3 | 16.139 | 61.656 | 40.928 | 15 | 1.015 | 0.164 |
| 4 | 16.080 | 62.962 | 42.461 | 16 | 0.778 | 0.081 |
| 5 | 16.151 | 60.647 | 40.706 | 15 | 0.776 | 0.153 |
| 6 | 16.075 | 61.031 | 43.098 | 15 | 0.648 | 0.087 |
| 7 | 16.155 | 60.359 | 43.049 | 15 | 0.716 | 0.097 |
| 8 | 16.176 | 59.885 | 41.741 | 15 | 1.847 | 0.532 |
| 9 | 16.076 | 61.574 | 41.049 | 14 | 0.629 | 0.087 |
| 10 | 16.148 | 60.723 | 43.231 | 15 | 1.076 | 0.203 |

Recomputed **sample** CVs are **0.00585409** (`ns/op`), **0.02218743** (writer pause), and **0.02498764** (foreground p99), matching the packet and passing the original 0.10 thresholds. Vacuum-total CV is 0.00585407. Legacy medians are 16.150 s vacuum, 61.302 ms writer pause, and 42.356 ms foreground p99. Every legacy arm has 345 preclone pages, 2,176 tail mutations, positive full overlap, and zero vacuum errors, exposure misses, aborts, reclones, dirty descriptors, or deferred cutovers. Public arms explicitly report availability, positive overlap and zero unexpected errors/exposure misses; their 0/1 retry counts are in the JSON.

There are 14–16 GC cycles and zero forced collections per legacy window. Recorded GC pause totals of 0.629–1.847 ms, with individual maxima 0.080–0.532 ms, cannot by themselves account for foreground p99s around 41–44 ms or individual maxima of 102–201 ms. This is evidence about **recorded GC stop-the-world pauses only**: concurrent marking, mutator assist and scheduler effects are not measured. `GCCPUFraction` is cumulative since process startup and was not subtracted to invent an interval GC CPU fraction.

The 789 host snapshots span 194.555 s. External CPU policy reports no busy processes in preflight or the whole observation window. Visible cgroups have no growth in throttle counters; CPU 0/1 show no steal-tick growth; memory PSI, swap and sampled direct-reclaim counters do not grow. Host I/O PSI does grow (61.554 s `some`, 60.277 s `full`), as does the leaf cgroup's I/O pressure. This cannot be assigned to external interference: in-window benchmark process samples themselves record approximately **6.34–6.59 GB** of writes per legacy arm. Sampled endpoint fault/CPU/I/O and leader-thread context-switch deltas are preserved separately. Their coverage omits window edges, and their scopes differ; no precise blocked-time attribution is claimed. Ancestor PSI counters overlap and were not added.

## Limits and next decision

Original exact-2588 M0 medians were 55.682 s / 102.723 ms / 153.449 ms, with CVs 0.131546 / 0.552979 / 0.230199. That run has no corresponding GC/process/pressure observations here. It ran on EPYC 9V74; this observation ran on EPYC 7763. That difference and the diagnostic instrumentation prevent a controlled causal comparison. No production repair occurred between those results.

The overlay's `ReadMemStats` calls are outside the legacy timer, but can alter surrounding GC/scheduler/cache state; the active observer and pre-setup ELF copy/hash also perturb execution. Exports have **no per-operation wall starts**. Summed durations were not used to fabricate starts or align operations to GC/PSI. Normal leader joins and executable hashes do not establish adopted-child enumeration on the normal success path; exceptional cleanup was not exercised.

Do not rerun unchanged 2588 until green or promote this diagnostic to the original M0 gate. Separately reviewed a17fe is a genuine source-packing successor; fixed 14-call maintenance harness PR5137 is reviewed but CI/landing remains pending. If its source/cost evidence warrants advancing the ordinary PR to that new product head, require fresh original M0 CI for that head under the existing rules. Root retains all decisions and runtime/dispatch authority.

Direct evidence anchors: historical CPU [raw legacy-1:4](/mnt/fast4tb/gomap-mvcc-4871-20261015/performance-185/2588c-m0-hosted/packet/raw/legacy-1.txt:4); diagnostic CPU [raw legacy-1:4](/mnt/fast4tb/gomap-mvcc-4871-20261015/m0-observation-runtime/download/m0-observation-2588c-38824531924-1/raw/legacy-1.txt:4) and [lscpu:8](/mnt/fast4tb/gomap-mvcc-4871-20261015/m0-observation-runtime/download/m0-observation-2588c-38824531924-1/raw/lscpu.txt:8); before/after inspection [helper:19](/mnt/fast4tb/gomap-mvcc-4871-20261015/m0-observation-runtime/download/m0-observation-2588c-38824531924-1/harness/mvcc_m0_observation_gc.go.txt:19) and [helper:25](/mnt/fast4tb/gomap-mvcc-4871-20261015/m0-observation-runtime/download/m0-observation-2588c-38824531924-1/harness/mvcc_m0_observation_gc.go.txt:25); actual timer/export [benchmark:178](/mnt/fast4tb/gomap-mvcc-4871-20261015/m0-observation-runtime/download/m0-observation-2588c-38824531924-1/vacuum_collection_bench_test.go:178); normal leader wait [runner:229](/mnt/fast4tb/gomap-mvcc-4871-20261015/m0-observation-runtime/download/m0-observation-2588c-38824531924-1/harness/mvcc_m0_observation.py:229), versus exceptional adopted-child wait [runner:171](/mnt/fast4tb/gomap-mvcc-4871-20261015/m0-observation-runtime/download/m0-observation-2588c-38824531924-1/harness/mvcc_m0_observation.py:171).
