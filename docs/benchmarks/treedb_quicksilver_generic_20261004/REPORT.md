# Generic Quicksilver owned-value evidence

**COMPLETE_EVIDENCE** — optimization acceptance remains coordinator-owned.

The 3M primary table pairs seeds 24/91/2027. Entries show median [min, max] over different fixture seeds; these are descriptive variation, not confidence or noise intervals. Final columns remain PENDING until all three observations exist.

| ACK / engine / phase | before Mops/s | final Mops/s | before p99 µs | final p99 µs | before Go B/op / allocs/op | final Go B/op / allocs/op |
|---|---:|---:|---:|---:|---:|---:|
| treedb durable hits | 0.2786 [0.2756, 0.2864] | 0.9852 [0.9758, 0.9866] | 123.3640 [123.1760, 128.0870] | 14.8830 [14.7790, 15.0070] | 850.3379 [842.2710, 907.4326] / 1.8280 [1.8277, 1.8342] | 667.0971 [661.8106, 678.8064] / 1.8425 [1.8407, 1.8429] |
| treedb durable misses | 1.8781 [1.7473, 1.9022] | 1.8946 [1.8662, 1.9409] | 4.9760 [4.8950, 5.1740] | 5.0120 [4.9250, 5.1490] | 9.5407 [9.5151, 9.5696] / 0.0313 [0.0313, 0.0313] | 9.5921 [9.5323, 9.6502] / 0.0313 [0.0313, 0.0313] |
| treedb durable mixed | 1.1206 [1.0856, 1.1644] | 1.7161 [1.7135, 1.8034] | 65.4010 [65.3040, 67.8070] | 5.9040 [5.6520, 6.2880] | 86.9906 [85.9255, 87.0239] / 0.2110 [0.2109, 0.2111] | 72.9010 [72.7802, 74.2108] / 0.2109 [0.2109, 0.2111] |
| treedb durable concurrent | 0.3704 [0.3691, 0.3761] | 0.6540 [0.6432, 0.6715] | 50.8900 [42.7590, 69.0580] | 12.3820 [11.9780, 12.5880] | 7632.9339 [7494.5400, 7955.5335] / 2.1342 [2.1320, 2.1379] | 134.8724 [126.7570, 137.8637] / 0.1787 [0.1781, 0.1791] |
| lmdb durable hits | 2.5467 [2.5227, 2.5676] | 2.5194 [2.5158, 2.5198] | 3.0260 [3.0160, 3.0660] | 3.1280 [3.0240, 3.2610] | 510.3936 [509.6937, 512.9570] / 1.0781 [1.0781, 1.0781] | 510.3945 [509.6937, 512.9566] / 1.0781 [1.0781, 1.0781] |
| lmdb durable misses | 3.7108 [3.6438, 3.7865] | 3.7083 [3.5545, 3.7524] | 1.8720 [1.8680, 1.9180] | 1.8820 [1.8290, 1.9370] | 37.8752 [37.8751, 37.8752] / 2.0781 [2.0781, 2.0781] | 37.8752 [37.8752, 37.8752] / 2.0781 [2.0781, 2.0781] |
| lmdb durable mixed | 3.4738 [3.4624, 3.5106] | 3.4442 [3.4323, 3.5095] | 1.9970 [1.9660, 2.0060] | 2.0000 [1.9860, 2.0010] | 85.1263 [85.0286, 85.4159] / 1.9781 [1.9780, 1.9781] | 85.1263 [85.0287, 85.4159] / 1.9781 [1.9780, 1.9781] |
| lmdb durable concurrent | 3.4769 [3.4414, 3.5102] | 3.4420 [3.4241, 3.4594] | 2.0010 [1.9860, 2.0550] | 2.0200 [2.0110, 2.0620] | 81.9192 [81.6440, 82.1824] / 1.9854 [1.9853, 1.9854] | 81.9307 [81.6331, 82.1572] / 1.9854 [1.9853, 1.9854] |
| rocksdb durable hits | 0.3783 [0.3292, 0.3801] | 0.3761 [0.3399, 0.3771] | 32.7200 [32.3980, 35.3570] | 32.7810 [32.6110, 34.1290] | 525.2700 [524.5697, 527.8332] / 3.0313 [3.0313, 3.0313] | 525.2692 [524.5689, 527.8321] / 3.0313 [3.0313, 3.0313] |
| rocksdb durable misses | 1.7766 [1.6202, 1.8784] | 1.8485 [1.5549, 1.8697] | 11.7320 [10.2340, 12.1070] | 11.4030 [10.6740, 11.9910] | 16.7501 [16.7500, 16.7501] / 2.0313 [2.0313, 2.0313] | 16.7500 [16.7500, 16.7501] / 2.0313 [2.0313, 2.0313] |
| rocksdb durable mixed | 1.2508 [1.1275, 1.2622] | 1.2247 [1.1550, 1.2428] | 18.6500 [17.8490, 19.0040] | 19.1180 [17.8860, 19.3660] | 67.6021 [67.5064, 67.8924] / 2.1313 [2.1313, 2.1313] | 67.6022 [67.5065, 67.8934] / 2.1313 [2.1313, 2.1313] |
| rocksdb durable concurrent | 0.9062 [0.8974, 0.9631] | 0.9530 [0.9429, 0.9537] | 21.1060 [20.4120, 21.8850] | 21.0660 [19.7870, 21.5360] | 64.1199 [63.5660, 64.1789] / 2.1237 [2.1237, 2.1238] | 64.1213 [63.5421, 64.3287] / 2.1238 [2.1237, 2.1240] |

**TreeDB fast below acknowledges volatile ordinary writes; it is a separate policy comparison.**

| ACK / engine / phase | before Mops/s | final Mops/s | before p99 µs | final p99 µs | before Go B/op / allocs/op | final Go B/op / allocs/op |
|---|---:|---:|---:|---:|---:|---:|
| treedb fast hits | 0.2810 [0.2753, 0.2816] | 0.9786 [0.9063, 1.0277] | 122.6740 [122.2380, 124.7330] | 14.9220 [14.7330, 15.7720] | 848.8315 [824.2293, 850.7501] / 1.8717 [1.8681, 1.8725] | 662.6778 [658.0516, 676.5033] / 1.8723 [1.8674, 1.8737] |
| treedb fast misses | 1.8019 [1.7817, 1.8106] | 1.8476 [1.8178, 1.8616] | 5.2300 [5.0340, 5.2890] | 5.0080 [4.9870, 5.2130] | 9.5161 [9.5150, 9.5382] / 0.0313 [0.0313, 0.0313] | 9.8094 [9.8048, 10.1411] / 0.0313 [0.0313, 0.0314] |
| treedb fast mixed | 1.1620 [1.1571, 1.1648] | 1.6904 [1.6240, 1.7393] | 64.2220 [62.3360, 66.5930] | 6.2340 [6.0080, 6.2970] | 84.8122 [83.9614, 85.9267] / 0.2109 [0.2109, 0.2111] | 72.8978 [72.8522, 73.2633] / 0.2109 [0.2109, 0.2111] |
| treedb fast concurrent | 0.3848 [0.3843, 0.3865] | 0.7042 [0.6642, 0.7083] | 49.8510 [41.2160, 69.4130] | 11.9790 [11.7880, 12.4150] | 7765.3315 [7718.2701, 7924.3330] / 2.2133 [2.2105, 2.2204] | 129.0789 [119.3656, 131.4067] / 0.1847 [0.1760, 0.1893] |

## Coverage and interpretation

43/43 final unprofiled cells PASS. Required plans: primary 12, holdout 4, 1% working set 4, controls 2, sync 1, 10M capacity 4, scaling 16. The four-reader scaling point reuses holdout seed173; 8/16/32/64 use that same holdout20%70miss fixture. Capacity also uses holdout173. Raw labels do not define workload identity.

Readers run about 8 seconds in concurrent phases, while writer composition may last longer. Baseline durable TreeDB reader/composition is about 8s / 24.213–28.878s; fast is about 8s / 21.311–21.698s. Go MemStats bracket reader start through reader join, before writer completion: they include overlapping writer/harness work and exclude the writer-only tail. Raw data does not expose completed mutation/checkpoint counts within the reader window; sustained overlap is not inferred.

Throughput includes PRNG/key generation, distinct bitmap work and full-value checking. Per-request latency excludes key/bitmap preparation and includes Get/snapshot maintenance. p99 uses capped deterministic prefix sampling, not an unbiased whole-duration sample. The full oracle/checkpoint/reopen warms caches before reads. No cold-cache claim is made.

Go allocations exclude native C allocations and mmap; GOMEMLIMIT2GiB is not a process RSS cap. Peak RSS includes load/verification and native work. LMDB64GiB is virtual map capacity; RocksDB64MiB is its cache control. Shared-host CPU/OS/native receipts and load observations remain in raw data. Native ACK flags differ; suite ordinary/sync dispatch does not establish cross-engine durability equivalence.

Actual initial/final storage sums, checkpoint/update-batch/reopen timings, distributions, verification/request counts, four-phase allocations and original receipts are retained in RESULTS.json. TreeDB fast column-physical durability/storage accounting is unsupported and must not be inferred as zero.

The baseline 3M explicit-sync holdout173 capture is retained as a 30-minute censored performance failure (rc1, empty JSON); it supplies no completed throughput, correctness or corruption finding. Its failed database remains retained. A completed final sync cell is mandatory. Historical random4k control is separate from the new generic baseline.

Profiles are diagnostic attribution only and excluded from unprofiled performance. Baseline outer-leaf/frame allocation Pareto targets O1/O2 are retained in JSON; O3 sync work and matched candidate attribution require final evidence. Original candidate receipts preserve their original SHAs; publication requires exact landed tree, compiled project input, unchanged harness, raw hash, build/native/loader and actual config binding.

Missing or failed cells:

Independent review, CI, performance/noise/checkpoint/storage guardrails and graph acceptance remain external gates even when evidence coverage is complete.
