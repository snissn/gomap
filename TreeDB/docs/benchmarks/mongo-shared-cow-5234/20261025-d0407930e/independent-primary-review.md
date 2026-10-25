# Independent primary audit — d0407930e

**ACCEPT completeness, provenance and the four numerical primary targets for the frozen 64 attempts.** This is not the resource/vector, whole-product or merge verdict. No Go/build/test/benchmark/profile execution, source mutation, large-binary/profile hashing, compression or new collection was performed. Only existing small metadata/raw files were read; this review artifact is the sole write.

Candidate `d0407930ef138915306e1a89c2c80522174c50ad`, binary recorded SHA256 `52bc3afbc46088218e0e8f8d0b59598b434e69d343d9c861d52cb5f1553a9455`. Baseline `eb9a4ab4a8e7c2c4ff34b53552b2cdde9a9c7144` represents original `f906b8a2aa1279ed6b6627028cba71d491db04c3` product plus the landed identical harness, binary recorded SHA256 `2218517aa643004a19590bb530bb2e83e236a77ee954d4c98b18cc467784182a`. Binary digests were recorded/validated by the maintained driver and precollection review; this quiet read-only pass did not rehash executables. Candidate build-info records Go 1.26.0. No broader text/stats/service source is qualified by this campaign.

## Checks

The frozen top manifest and primary manifest are byte-identical. Final precollection state and host preflight bind the same manifest SHA below, terminal full collections PASS488.271/race17.843, worker quiet acknowledgement and root grant before the first attempt. Seven identical landed harness blobs and 18 reviewed pressure blobs were independently verified during the preceding precollection check; d040 differs from 07d only by reviewed direct rerank dispatch and CI discovery hash. Prior 07d all106 evidence remains explicitly retained under its original source identity; no old sample was relabeled or replaced.

Independently constructed the exact 64-row plan: eight pairs × durable/relaxed × one/eight writers × baseline/candidate; odd pairs B/C, even pairs C/B. Every planned row equals its receipt and exact anchored command, selects one cell in a fresh process, uses 1,000 operations, count1, benchmem, and the correct frozen binary. Exactly64 unique receipts,64 raw logs and64 host snapshots exist; no extra raw attempts or duplicate labels. All receipts are valid, returncode0, with nonnull metrics and no parse/infrastructure error. All64 operator completion rows match their receipts. Attempt start/end intervals are sequential and nonoverlapping, after the grant. The fixed batch retained every planned outcome; there are no failed/null primary outcomes to exclude.

Each raw log independently validates PASS with no failure/panic, the exact profile/route/native flag, ordinary BSON, w:1/j:unspecified, two secondary indexes, 64 rows per writer and256-byte payload. Both calibration and final fixture declarations agree. Each has one exact 1,000-operation final row, correct benchmark cell and GOMAXPROCS suffix4; calibration remains separate. Raw ns/op, ACK/s, bytes/op, allocs/op, final WAL counters, native flag and both outside-timer Close durations exactly equal receipt metrics. ACK/s is reciprocal of ns/op within independent Go output rounding.

All durable singleton samples have1 calibration sync and1,000 final file-sync/sync-count deltas on both routes. Durable eight-writer candidate has250 final syncs in every sample; baseline has250 in pairs1/4/5 and999,998,999,1000,1000 in pairs2/3/6/7/8. File-sync and logical sync-count agree throughout. Every relaxed calibration/final counter is0 on both routes. Lower durable grouping counts do not imply a weaker requested profile/ACK: actual opener/profile/ACK and original durable boundary remain fixed. The counts are retained operational observations.

The maintained landed harness starts timing after setup when waiting writers are released and stops after all ACKs; counters and disconnect/Close are outside. Each writer changes indexed n on disjoint rows through the official driver, with retries disabled. This audit does not independently replay storage state from logs.

## Independent arithmetic

All32 paired ratio triples and every summary median/boolean independently recompute exactly from authenticated raw rows. Ratios are candidate/baseline. The original median-of-eight gate is unchanged; target applies to the median, not every pair.

| Cell | ns/op C/B | B/op C/B | allocs/op C/B | Paired ns/op range | Numeric target |
|---|---:|---:|---:|---:|---|
| Durable1 | 0.8948404953743785 | 0.4498418288238351 | 0.5579763823701386 | 0.8793081426–0.9128786069 | PASS |
| Durable8 | 0.19339912889628366 | 0.21786018896155107 | 0.2356973287524486 | 0.1889150374–0.6909412256 | PASS |
| Relaxed1 | 0.2382226868584939 | 0.3789655265211554 | 0.5669064748201439 | 0.2296368651–0.2531774471 | PASS |
| Relaxed8 | 0.039493458593432326 | 0.1989415355120898 | 0.26222520719202136 | 0.0368262689–0.1067526375 | PASS |

Durable1 gives10.516% median time reduction, about0.516 percentage point beyond its10% objective. Some individual pairs give less than10%; none regress. This is a narrow margin, not a stronger precision/confidence claim. Durable8 baseline has a substantial two-regime distribution:1.8445–1.8814ms/op with250 syncs versus6.6051–6.7423ms/op with998–1,000 syncs; candidate remains1.2658–1.2900ms/op with250 syncs. Relaxed8 also has material baseline variation (0.4042–1.2413ms/op) while candidate is0.0431–0.0475ms/op. Baseline dictionary-training log entries coexist with many slower eight-writer samples; these logs establish association, not a new causal attribution. The median gain must be published with all paired ratios and this dispersion, not as a uniformly stable81%/96% improvement. Even the least favorable pair in each cell improves time, bytes and allocations and satisfies the respective cell target except the stated singleton time pairs. No unfavorable sample was discarded.

Final-invocation disconnect/Close is separately retained: baseline30.524–68.165ms, candidate61.397–70.788ms. The ACK-only primary interval does not include that deferred lifetime cost. Resource/maintenance and vector tradeoffs remain for separate audits and coordinator disposition.

Host isolation is the disclosed shared-host fallback, not a dedicated-runner claim. Each64 host snapshot exists; executable-basename inspection found no non-defunct Go/compiler/linker/test process. Snapshots and the one-second preflight are not continuous host-idleness proof. Private full argv is not reproduced here; original private snapshots require sanitization before publication.

## Digest bindings

SHA256:

| Artifact | SHA256 |
|---|---|
| manifest.json and primary/manifest.json | 1e3c8aab0bc290af28828ac8f329c5aefdfdb0bd4cb0fdc9b62359767aa905dc |
| precollection-state.json | dd210a89578cc655ab1b9f9fd5aca63c0aef61331d9d44ea7fdac25b3400e5da |
| host-preflight.json | d96935cebc3db2ec08f07fbbcfa5de7348ca31fe596cdd3767da68975bdbd730 |
| primary/plan.json | 3023400e0ec60476d1fd31e3e0edbc3d15974fa274430f4ff72605f2694a5135 |
| primary/receipts.json | fdebbebf7ff0a5ab42b297d6d52e76f9f2deb6400864ef63fb5fcd8653bdd8a7 |
| primary/summary.json | ea7ae47931936457417dcbbb32d9c0712d4862229f594d50181db75cfb36ec71 |
| primary-operator.log | c91fdc53197093d46ce526f9259f2a4c09c6f5488f76f6de61058c8d0f047805 |
| All64 primary raw logs set | 10d2e32e959b612572c9d968f1062c922cade2e0f283082cccb04b3ba2d25bb6 |
| All64 primary host snapshots set | b80a76422b7d64e71198a05376e677275721299caea058873ec2f2707b3da11b |

Set digests are SHA256 over concatenated UTF-8 lines `basename + " " + SHA256(file bytes) + "\n"`, sorted lexicographically by basename. Membership is the exact64 labels in primary/plan.json, with `.log` or `-host.txt` suffix. Only the existing primary set was hashed; resource/vector collection and all frozen inputs were left untouched.

No material completeness/provenance/numerical error found. Resource24/vector18 and explicit tail/allocation/Close tradeoff promotion, capability/recovery, CI/review and parent consumer obligations are not discharged by this numeric ACCEPT.
