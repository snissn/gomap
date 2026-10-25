# Independent resource/vector audit — d0407930e

**ACCEPT completeness, provenance and numerical derivation of all24 resource and18 vector attempts. Promotion remains OPEN.** No performance tradeoff, correctness, CI or broader-consumer acceptance is supplied by this artifact. No Go/build/test/benchmark/profile execution, source edit, large-profile/binary hashing or new collection occurred. Only existing small metadata/raw/source files were read; this Markdown is the sole write.

Candidate `d0407930ef138915306e1a89c2c80522174c50ad`, recorded binary SHA256 `52bc3afbc46088218e0e8f8d0b59598b434e69d343d9c861d52cb5f1553a9455`. Baseline `eb9a4ab4a8e7c2c4ff34b53552b2cdde9a9c7144` represents `f906b8a2aa1279ed6b6627028cba71d491db04c3` product plus the landed identical harness, recorded binary SHA256 `2218517aa643004a19590bb530bb2e83e236a77ee954d4c98b18cc467784182a`. The binary digests are existing driver/precollection bindings, not executables rehashed in this audit. Go1.26.0/GOMAXPROCS4 are fixed. Prior07d all106 attempts remain retained under their original source; d040 is a new full106 campaign after the reviewed direct-rerank repair, not relabeled07d or diagnostic evidence.

## Independent checks

Constructed and checked exact resource24 and vector18 membership: three pairs, four resource cells/three vector benchmarks, both routes, odd pairs B/C and even pair C/B. Exactly one receipt, raw log and host snapshot exists per planned label, with no duplicate or extra raw attempt. All42 terminal receipts are valid/exit0/non-null; no failed/null outcome exists to exclude. Resource operator completion rows exactly match24 receipts. Original18 vector operator receipts remain `completed_unvalidated` with null metrics; independently validated receipts preserve every original process/command/environment/time/error field, adding only validation/status/metrics. All106 primary→resource→vector process intervals are sequential/nonoverlapping. Commands select exact anchored single cells/count1/frozen binaries; root vector-plan fields match vectors/plan. Top/resource manifests are byte-identical. The preceding primary/precollection reviews bind the seven identical landed harness blobs,18 pressure files, quiet grant and original source/binary identities.

Every resource raw log independently has the selected test cell, actual OpenStandaloneServer profile, correct native flag, BSON, w:1/j:unspecified, two secondary indexes,64 rows per writer and4096-byte payload. Its single PUBLIC_RESOURCE_JSON equals receipt metrics exactly: operations=attempted=completed=10000, failed=false, all four quantiles nonnull. Each raw ends PASS with no FAIL/panic. The phase order is before→zero or more held-churn→after-ACK-held→reader-released→idle-2s→closed-GC; timestamps increase and the idle interval is at least2s. All RSS samples are present. Quantile arithmetic itself cannot be recomputed from per-operation latencies, which the maintained harness does not export; the reported quantiles are authenticated to raw output, not independently resampled.

Every vector raw log independently declares actual OpenStandaloneServer/no_wal_fast, the correct native flag,512 rows,8 dimensions,M16. Calibration and final declarations agree. Exactly one final numeric row has the selected benchmark/suffix4 and10 operations for BuildSaveLoad/SaveReopenLoad or10000 for Search. Raw ns/op/B/op/allocs/op equal receipt metrics; PASS/exit0 and all18 original operator observations agree. No private vector collector or new measurement parser was executed to generate the workload.

Host snapshots exist for every attempt. Executable-basename inspection found no non-defunct Go/compiler/linker/test process in those snapshots. This supports the disclosed shared-host fallback, not dedicated-runner or continuous-idleness proof. Full private argv is not reproduced here; snapshots require sanitization before publication.

## Resource arithmetic and lifetime

For all12 resource pairs, independently reconstructed route metrics from raw JSON, including maxima over **every recorded phase**, computed C/B ratios and then median-of-three paired ratios. Every pair/median equals resource-analysis.json exactly. Ratios below are C/B; these secondary observations are not new numerical gates.

| Cell | ACK | p99 | request max | sampled HeapAlloc | sampled HeapInuse | sampled RSS | sampled logical file bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| command_wal_durable/writers=1 | 0.844906922 | 0.519993371 | 4.012328887 | 0.593024061 | 0.595829228 | 0.237811470 | 0.042266332 |
| command_wal_durable/writers=8 | 0.650053389 | 0.505074203 | 1.827265462 | 0.491368715 | 0.503209692 | 0.436877273 | 0.302730299 |
| command_wal_relaxed/writers=1 | 0.281207431 | 0.074492179 | 10.148182457 | 0.497503582 | 0.502219925 | 0.231452302 | 0.036720628 |
| command_wal_relaxed/writers=8 | 0.118185986 | 0.079906206 | 1.516043618 | 0.427398561 | 0.434682111 | 0.410811996 | 0.182883844 |

Actual candidate request maxima across12 runs span **125.136607–167.414703ms**; baseline spans12.193177–122.905687ms. Durable8 pair2 is the largest candidate maximum. Every cell has lower median p99 and higher median max; better p99/memory does not waive the maximum. The old950/07d maximum ranges are not reused for d040. Existing d040 counters show1–8 pressure checkpoint runs before ACK and checkpoint max111.970–136.944ms in those endpoint samples. That is consistent with the known synchronous maintenance mechanism, but endpoint correlation is not a per-request trace attribution or acceptance of the exact new maxima. Root's separate exact-frozen trace/causal disposition remains required; this audit neither ran nor interpreted it.

Absolute ranges of sampled maxima across the three runs (bytes):

| Cell/route | HeapAlloc | HeapInuse | RSS | logical file bytes |
|---|---:|---:|---:|---:|
| command_wal_durable/writers=1/baseline | 244,508,680–249,714,752 | 248,414,208–253,804,544 | 594,948,096–613,801,984 | 419,655,053–419,655,441 |
| command_wal_durable/writers=1/candidate | 143,410,024–145,773,648 | 146,210,816–148,742,144 | 140,460,032–145,969,152 | 17,668,641–17,861,690 |
| command_wal_durable/writers=8/baseline | 322,406,688–351,528,856 | 327,966,720–357,466,112 | 410,750,976–416,268,288 | 161,626,977–166,561,284 |
| command_wal_durable/writers=8/candidate | 154,299,664–184,465,008 | 161,185,792–187,645,952 | 174,133,248–190,226,432 | 48,929,383–48,962,159 |
| command_wal_relaxed/writers=1/baseline | 261,783,336–269,435,992 | 265,297,920–273,145,856 | 596,873,216–600,322,048 | 402,876,948–402,876,990 |
| command_wal_relaxed/writers=1/candidate | 122,576,216–134,097,440 | 125,624,320–137,248,768 | 129,880,064–147,750,912 | 14,793,896–14,793,896 |
| command_wal_relaxed/writers=8/baseline | 310,424,480–347,030,024 | 315,424,768–351,674,368 | 418,705,408–429,678,592 | 161,625,425–166,562,943 |
| command_wal_relaxed/writers=8/candidate | 118,435,688–160,869,256 | 127,803,392–164,233,216 | 166,932,480–176,517,120 | 29,558,679–30,103,966 |

These maxima include before, held/churn, ACK, reader-release, idle and closed-GC. Some RSS maxima occur at closed-GC and some heap maxima at idle/release; restricting the calculation to the timed interval would understate them. Sampling is one-second plus phase boundaries, not continuous heap/RSS peak. Fast relaxed8 has no held-churn tick. RSS is whole-process VmRSS; file bytes sum regular-file logical sizes, include WAL/storage and tolerate concurrent unlink, and are not allocated blocks or WAL-excluded storage. COW charge is separate from process heap/RSS and may include admitted unused capacity. Neither metric promises RSS equals charge or memory is returned to the OS immediately.

The existing harness holds one opening public read view through all10000 official-driver updates; after ACK it rechecks all seed IDs under n_1=0 and fetches the first seed document with n=0, then closes that view. It does not validate every old document payload. Failure in a request, sampler, held-view check or finish marks failed and prevents valid quantiles; all24 outputs pass that path. Natural idle is observed without a harness checkpoint. Finish/disconnect/server.Close and runtime.GC occur after the idle phase.

All12 native runs expose the COW/maintenance/checkpoint/CWAL counters in all live phases; baseline exposes historical backend counters with COW keys absent. Native sources are4 before→6 at ACK-held→2 immediately after held-reader release→0 at idle. Generations are8→10→6→4, active cuts1→2→1→1. Releasing the held view reduces exactly20 external leases and268,576 reserved bytes immediately in every run; subsequent worker cleanup/idle releases further debt. Native retired_bytes becomes0 at idle and deferred_bytes is0 at all four endpoint phases. These observations demonstrate bounded retained-cut custody and later retirement, not zero live-owner state.

| Candidate cell | rollover/handoff ACK→idle | checkpoint runs ACK→idle | reserved bytes ACK→idle | external leases ACK→idle | file-sync calls before→ACK→idle |
|---|---|---|---|---|---|
| command_wal_durable/writers=1 | 8→9 / 8→9 | 8→9 | 315,257,264–318,362,864→270477872 | 4,412–4,622→1381 | 2→10019→10021 |
| command_wal_durable/writers=8 | 1→2 / 1→2 | 1→3 | 398703952→270487440 | 9029→356 | 9→2511→2513 |
| command_wal_relaxed/writers=1 | 7→8 / 7→8 | 7→8 | 382855824→270477872 | 8983→1381 | 0→21→24 |
| command_wal_relaxed/writers=8 | 2→3 / 2→3 | 2→3 | 349,250,336–350,487,328→290,398,464–290,594,016 | 4,435–4,525→459–472 | 0→6→9 |

Idle reserved charge remains270,477,872B for singleton,270,487,440B durable8 and290,398,464–290,594,016B relaxed8; leases remain1381/356/459–472. The fixed maintenance capacity is268,435,456B in all12 runs and included once in parent charge. Nonzero maintenance/caches/shared dictionary owners remain; sources0 is not total charge0. Recorded cumulative engine peak_bytes ranges447,464,640–541,006,016B, distinct from the512MiB in-flight limit; the product total limit is1GiB. No cap violation or leak is inferred by equating these separate statistics.

Every closed-GC stats map is empty on both routes: closed native counters are **unavailable**, not observed zero. The retained raw logs therefore do not prove a final zero-charge assertion, though successful finish is reported and process RSS/heap are sampled after GC. No admission/lifetime counter has been fabricated for the closed server.

Raw outside-ACK disconnect/Close cost is retained per attempt (milliseconds):

| Cell | baseline three attempts | candidate three attempts |
|---|---|---|
| command_wal_durable/writers=1 | 69.374473ms, 32.670073ms, 31.746928ms | 26.352324ms, 25.568984ms, 26.596282ms |
| command_wal_durable/writers=8 | 50.769035ms, 49.620432ms, 67.092342ms | 31.832613ms, 31.976038ms, 31.403277ms |
| command_wal_relaxed/writers=1 | 82.662999ms, 83.699046ms, 80.719945ms | 26.44813ms, 25.95276ms, 25.926183ms |
| command_wal_relaxed/writers=8 | 48.679627ms, 48.736595ms, 55.214856ms | 28.999752ms, 27.894118ms, 27.500496ms |

## Vector arithmetic and boundaries

All9 paired triples and3 medians recompute exactly from raw rows:

| Benchmark | time C/B | B/op C/B | allocs/op C/B |
|---|---:|---:|---:|
| BenchmarkStandalonePublicVectorBuildSaveLoad | 0.972850275 | 0.265995862 | 0.407423469 |
| BenchmarkStandalonePublicVectorSaveReopenLoad | 0.756951408 | 0.221036136 | 0.895612186 |
| BenchmarkStandalonePublicVectorSearch | 0.327091588 | 0.470102789 | 1.266666667 |

Search is19 allocations/op and12,760B/op on every candidate run, versus15 allocations/op and approximately27,143B/op baseline. This restores the95019-object level after the reviewed avoidable07d21-object regression; **it still costs26.667% more allocations than baseline**, requiring explicit promotion disposition. No prior acceptance substitutes for this fresh result. BuildSaveLoad time ratios span0.944933–1.028404: one pair is2.84% slower. Three pairs on the shared host do not justify stronger precision/confidence claims.

The landed benchmark uses ordinary BSON collection/vector APIs through actual standalone openers (not vector commands over Mongo wire), no_wal_fast,512x8D cosine HNSW/M16,ANN fallback disabled. BuildSaveLoad times Build→SaveSnapshot→hot Load; final cleanup Close is outside that timer. Native Save's logical persistence return and baseline physical publication are not asserted to have identical individual timing. The separate SaveReopenLoad timer includes Save→server.Close→fresh Open→OpenCollection→Load→validating Search on every iteration, so physical work is counted there; final reopened-server cleanup Close is outside the timer. Steady Search builds/warms outside timing and validates five results each timed operation. No durability profile was weakened to obtain these results; no unmeasured vector API/recovery/serving capability is claimed.

## Digest bindings

SHA256 below binds the exact reviewed metadata and raw membership; binaries/large profiles were not rehashed.

| Artifact | SHA256 |
|---|---|
| manifest.json | 1e3c8aab0bc290af28828ac8f329c5aefdfdb0bd4cb0fdc9b62359767aa905dc |
| resources/plan.json | 45793d1f28ea29f04d4f875d300a9b257ee02c5219c057dd56b72bc495597c15 |
| resources/manifest.json | 1e3c8aab0bc290af28828ac8f329c5aefdfdb0bd4cb0fdc9b62359767aa905dc |
| resources/receipts.json | 903d067273562cd4aed3fa1e0a80c970d0d4e0e16dcbc0cc0fd45b21e4d8a432 |
| resources-operator.log | 60aabcccbcd211f0e197813b182da840166f089a865ebaed74b236230ed3dab6 |
| resource-analysis.json | 8fc448cda672fd85bef9825800d9704d7287aeac4965758d69f6dc1dfc57c607 |
| vector-plan.json | 6fb29f979c93b06886c3ed1eb1e2a4ffffcf70c69986eef3e853532b5df56c3b |
| vectors/plan.json | 1432aaf645a7aca625704f957f5ac5272725691c4e3cd8e51c5cd326f0372397 |
| vectors/receipts.json | 44bc0c61f3de270a68bd2adafe625c8eca0ce8066e67d55b3f12d4bd4128ec39 |
| vectors/operator-process-receipts.json | 1193d18ae2f3de263b5c63ac0f6c765ba958a0bd3782464e303423acaa1537f4 |
| vectors/summary.json | c074ad416a1d8e6e21187f9e091e86425857d445297a1a38a905604bad7aad1b |
| resources/*.log (24 files) | c4d0d5b129a9b84ec7177f4869563a4433f8d1a5121a86cc56ae799cfd749151 |
| resources/*-host.txt (24 files) | 6795980582cd1f624023adbcb18c4e0a61466d95b9876e287660e81e2daf61a2 |
| vectors/*.log (18 files) | d42992a95e9312ec96209d704381ab64a21c6dfa20b3c4a68ac8a9e788496a21 |
| vectors/*-host.txt (18 files) | 1802eddacd8dcb6f26b3ed58cd370c81b1c88b01507c761a63af62667aba52d6 |

Set digest = SHA256 of concatenated UTF-8 lines `basename + " " + SHA256(file bytes) + "\n"`, lexicographically sorted by basename. Membership equals the frozen plans with `.log` or `-host.txt` suffix. Harness identity remains the landed diagnostic blob8d7d502151138a5749997f12af30e95e6716efe3, vector blobe0930c57e233853bbeb889f5b2c67fc578d8f979 and measurement-schema blob3c8ab5bc8a316f68ad0e8647a122acbb9ce02bc0; the earlier precollection review verified all seven baseline/candidate/landed entries.

No material completeness/provenance/numerical error found. This ACCEPT is limited to the42 retained outcomes and their derivations. Maximum-latency, Search-allocation and deferred-Close tradeoffs require the coordinator's explicit source-bound causal disposition; full product/parent adoption, CI/merge and closed-owner-zero guarantees remain outside this audit.
