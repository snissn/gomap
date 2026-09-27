# F physical-expansion first gate: prospective invocation order

Source and runtime are both `03a3b73dd2fc6888e603307df5c3a00f0a879342`, tree `b55f42a991bfeb419824205a5412292adbb71fde`. Root is `/home/mikers/gomap-4753-integrated-03a3b73dd`. All invocations below use `bash ROOT/f-….sh` (ROOT denotes that absolute directory). No wrapper launches a campaign loop. Every heavy invocation requires root's exclusive runner allocation and a fresh admission check.

The compile stage uses `f-freeze-invoke.sh` (8 GiB, zero swap, 20 min, GOMAXPROCS=2, GOFLAGS=-p=1). It builds one clean VCS-pinned runtime/comparer and one external-reader binary with the prospectively hashed overlays; it does not build assets. Its binary/environment/output manifests are required before construction.

Before construction, root publishes the final measurement-package-pins, reviews its hash together with the compile result, and authorizes these commands:

```
f-build.sh prepare one
f-build.sh prepare multi
f-build.sh run one
f-build.sh run multi
f-geometry.sh
```

Each construction is 12 GiB, zero swap, 60 min; both build commands are captured before either runs. Each builds fresh D16, disjoint, graph-assigned assets with identical seed/graph/router/dependency coordinates. The only declared command difference is target hot bytes 33,554,432 (one, expected P16) versus 7,696,384 (multi, expected P64), plus destination paths. Geometry uses the landed read-only opener, binder and logical-membership digest before timing, under 8 GiB/5 min. Entire newly built DB inventories are captured before this observation and rechecked; older inventories retain their original authority.

Prepare both arms of each block before serving. The fixed external order is block1 one/multi; block2 multi/one; block3 one/multi; block4 multi/one; block5 one/multi. For each block B:

```
f-serve.sh prepare one B
f-serve.sh prepare multi B
f-serve.sh run FIRST B
f-serve.sh run SECOND B
f-replay.sh freeze ROOT/serving/one-blockB
f-replay.sh freeze ROOT/serving/multi-blockB
f-compare.sh prepare B
f-compare.sh run B
```

Serving and comparison each use 8 GiB, zero swap, 40 min. Each report has P5/P16, c1/c32, EF96/C256/top10, warmup64, five internal repetitions (20 rows), required 95% recall and complete attempt accounting. `f-compare.sh` retains the exact comparer process exit/status/result even when its subsequent fixed performance gate rejects. A QPS/p95 failure produces no accepted-result pin. The same-source comparison invokes strict replay in-process; it does not launch a replay child.

For the first complete evidence-valid pair, run BOTH resource companions even if QPS/p95 rejects. This is prospectively required diagnosis, not outcome-dependent follow-up. For later successful pairs, retain both companions as well:

```
f-resource.sh prepare one B
f-resource.sh prepare multi B
f-resource.sh run FIRST B
f-resource.sh run SECOND B
f-resource-read.sh one B
f-resource-read.sh multi B
```

Resource companions are separate one-repetition observations (8 GiB/40 min), not resources retrospectively attributed to the timed serving process. Raw profile and CPU/TotalAlloc/Mallocs intervals remain intact. Reader execution is 8 GiB/5 min. A parent quality/completion failure may correctly refuse a companion; preserve that refusal and do not synthesize success.

For block1 only, collect each layout's separately scoped physical observation after authenticating the original parent:

```
f-physical.sh prepare one
f-physical.sh prepare multi
f-physical.sh run FIRST
f-physical.sh run SECOND
f-physical.sh read one
f-physical.sh read multi
f-preserve.sh
```

Physical collection is 8 GiB/40 min and read is 8 GiB/5 min. The existing strict reader authenticates the original raw manifest, derives the serving topology digest from the original parent group order, and verifies the emitted receipt. Both observations use this same current runtime: one-pack contiguous assets versus multi-pack root/section chunks. Mapped extents are not RSS, handles are not OS FDs, accessed chunks are opened/validated chunks rather than query touches, metadata/scratch are estimates or bounds, and release counters are not Go metadata reclamation.

Only after all required successful observations and preservation checks may `f-pair-accept.sh B` create the pin that permits preparing block B+1. The script requires every comparer gate and both resource-reader exits. Root must also inspect telemetry and all resource/quality/refusal gates. Any valid required failure stops future pairs and all downstream F axes. EVIDENCE_INVALID is separate and cannot be repaired by selecting successful windows.

This packet contains no new algorithm, extra F asset, 250K run, lifecycle run, cloud action or merge. The old raw receipts remain immutable. Compilation success alone is not construction authority.
