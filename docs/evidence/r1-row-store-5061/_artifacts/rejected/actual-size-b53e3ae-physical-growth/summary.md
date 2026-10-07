# Standalone R1 lifecycle diagnostic

Qualification: **rehearsal**. No SQLite or cross-fixture speed comparison.
Source `b53e3ae98924085c3d3331dae0d821dd101bf07f`; runtime `e69d0944e75365c682eac2ff44daee4606d9edc620eacf2438e4845e1f378254`; harness `f963bc88ed1df79fae88f5e2a7f34aea3cd3e19f70a368891011fcb73e14065b`.
1 fresh processes; 5 final epochs/process; 4096 live rows; 1024 calls/epoch.
Every final epoch revisits the same 512 IDs. Full row/posting oracles cover the live population; timed churn covers this bounded working set. This is finite hot-set evidence, not full-population or unlimited-capacity qualification.

| Final metric | Median | Minimum | Maximum | max/min |
| --- | ---: | ---: | ---: | ---: |
| B/op | 8.35873e+08 | 8.35873e+08 | 8.35873e+08 | 1 |
| allocs/op | 6.14764e+06 | 6.14764e+06 | 6.14764e+06 | 1 |
| calls/op | 1024 | 1024 | 1024 | 1 |
| loop-B/call | 816282 | 816282 | 816282 | 1 |
| loop-allocs/call | 6004 | 6004 | 6004 | 1 |
| loop-ns/call | 6.826e+06 | 6.826e+06 | 6.826e+06 | 1 |
| mixed-calls/s | 146.5 | 146.5 | 146.5 | 1 |
| mixed-p95-ns/call | 1.39416e+07 | 1.39416e+07 | 1.39416e+07 | 1 |
| mixed-p99-ns/call | 1.96377e+07 | 1.96377e+07 | 1.96377e+07 | 1 |
| ns/op | 6.99106e+09 | 6.99106e+09 | 6.99106e+09 | 1 |
| process-retained-heap-B | 8.07056e+07 | 8.07056e+07 | 8.07056e+07 | 1 |
| sampled-heap-high-B | 1.68831e+08 | 1.68831e+08 | 1.68831e+08 | 1 |

Logical storage medians across final process results:

| Phase | index | vlog | leaf log | typed assets | redo WAL | dictionary store | template store | immutable manifest metadata | other | all bytes | regular files | growth from ingest |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ingest | 8.38861e+06 | 131289 | 1.62793e+06 | 725636 | 699917 | 0 | 0 | 554 | 470 | 1.15744e+07 | 12 | 0 |
| churn-0 | 8.80804e+07 | 193991 | 7.30737e+06 | 1.04546e+06 | 878285 | 0 | 0 | 554 | 470 | 9.75065e+07 | 12 | 8.59321e+07 |
| checkpoint-0 | 8.80804e+07 | 193991 | 7.30737e+06 | 1.04546e+06 | 878285 | 0 | 0 | 554 | 470 | 9.75065e+07 | 13 | 8.59321e+07 |
| folded-0 | 8.80804e+07 | 193991 | 7.4066e+06 | 1.73664e+06 | 878285 | 0 | 0 | 554 | 470 | 9.82969e+07 | 13 | 8.67225e+07 |
| before_vacuum-0 | 8.80804e+07 | 193991 | 7.4066e+06 | 1.73664e+06 | 878285 | 0 | 0 | 554 | 795 | 9.82972e+07 | 14 | 8.67228e+07 |
| before_exhaustive-0 | 4.1943e+06 | 193991 | 7.40868e+06 | 1.73664e+06 | 878285 | 0 | 0 | 855 | 795 | 1.44135e+07 | 15 | 2.83914e+06 |
| before_final_gc-0 | 4.1943e+06 | 193991 | 7.65888e+06 | 1.73664e+06 | 878285 | 0 | 0 | 4080 | 795 | 1.4667e+07 | 24 | 3.09257e+06 |
| maintenance-0 | 4.1943e+06 | 193991 | 7.65888e+06 | 1.73664e+06 | 878285 | 0 | 0 | 4080 | 795 | 1.4667e+07 | 24 | 3.09257e+06 |
| after_view_release | 4.1943e+06 | 193991 | 189498 | 691175 | 878285 | 0 | 0 | 1277 | 795 | 6.14932e+06 | 16 | -5.42508e+06 |
| churn-1 | 8.38861e+06 | 256821 | 5.06472e+06 | 1.01296e+06 | 1.05874e+06 | 0 | 0 | 1277 | 795 | 1.57839e+07 | 17 | 4.20952e+06 |
| checkpoint-1 | 8.38861e+06 | 256821 | 5.06472e+06 | 1.01296e+06 | 180466 | 0 | 0 | 1277 | 795 | 1.49056e+07 | 17 | 3.33125e+06 |
| folded-1 | 8.38861e+06 | 256821 | 5.17126e+06 | 1.70413e+06 | 180466 | 0 | 0 | 1277 | 795 | 1.57034e+07 | 17 | 4.12896e+06 |
| before_vacuum-1 | 8.38861e+06 | 256821 | 5.17274e+06 | 1.70413e+06 | 180466 | 0 | 0 | 1277 | 946 | 1.5705e+07 | 17 | 4.1306e+06 |
| before_exhaustive-1 | 4.1943e+06 | 256821 | 5.1748e+06 | 1.70413e+06 | 180466 | 0 | 0 | 1794 | 946 | 1.15133e+07 | 18 | -61137 |
| before_final_gc-1 | 4.1943e+06 | 256821 | 5.40655e+06 | 1.70413e+06 | 180466 | 0 | 0 | 4398 | 946 | 1.17476e+07 | 24 | 173221 |
| maintenance-1 | 4.1943e+06 | 256821 | 378462 | 691175 | 180466 | 0 | 0 | 1820 | 946 | 5.70399e+06 | 18 | -5.87041e+06 |
| churn-2 | 8.38861e+06 | 319524 | 5.28238e+06 | 1.01296e+06 | 360793 | 0 | 0 | 2836 | 946 | 1.5368e+07 | 22 | 3.79364e+06 |
| checkpoint-2 | 8.38861e+06 | 319524 | 5.28238e+06 | 1.01296e+06 | 180339 | 0 | 0 | 2836 | 946 | 1.51876e+07 | 22 | 3.61319e+06 |
| folded-2 | 8.38861e+06 | 319524 | 5.38908e+06 | 1.70413e+06 | 180339 | 0 | 0 | 2836 | 946 | 1.59855e+07 | 22 | 4.41107e+06 |
| before_vacuum-2 | 8.38861e+06 | 319524 | 5.39056e+06 | 1.70413e+06 | 180339 | 0 | 0 | 2836 | 1243 | 1.59872e+07 | 22 | 4.41284e+06 |
| before_exhaustive-2 | 4.1943e+06 | 319524 | 5.39261e+06 | 1.70413e+06 | 180339 | 0 | 0 | 3852 | 1243 | 1.1796e+07 | 23 | 221607 |
| before_final_gc-2 | 4.1943e+06 | 319524 | 5.62283e+06 | 1.70413e+06 | 180339 | 0 | 0 | 2856 | 1244 | 1.20252e+07 | 23 | 450830 |
| maintenance-2 | 4.1943e+06 | 319524 | 567317 | 691175 | 180339 | 0 | 0 | 2639 | 1244 | 5.95654e+06 | 20 | -5.61786e+06 |
| churn-3 | 8.38861e+06 | 382355 | 5.4296e+06 | 1.01296e+06 | 360794 | 0 | 0 | 3941 | 1244 | 1.55795e+07 | 24 | 4.0051e+06 |
| checkpoint-3 | 8.38861e+06 | 382355 | 5.4296e+06 | 1.01296e+06 | 180467 | 0 | 0 | 3941 | 1244 | 1.53992e+07 | 24 | 3.82477e+06 |
| folded-3 | 8.38861e+06 | 382355 | 5.53824e+06 | 1.70413e+06 | 180467 | 0 | 0 | 3941 | 1244 | 1.6199e+07 | 24 | 4.62458e+06 |
| before_vacuum-3 | 8.38861e+06 | 382355 | 5.53972e+06 | 1.70413e+06 | 180467 | 0 | 0 | 3941 | 1394 | 1.62006e+07 | 24 | 4.62621e+06 |
| before_exhaustive-3 | 4.1943e+06 | 382355 | 5.54177e+06 | 1.70413e+06 | 180467 | 0 | 0 | 5243 | 1394 | 1.20097e+07 | 25 | 435267 |
| before_final_gc-3 | 4.1943e+06 | 382355 | 5.77164e+06 | 1.70413e+06 | 180467 | 0 | 0 | 3430 | 1394 | 1.22377e+07 | 25 | 663323 |
| maintenance-3 | 4.1943e+06 | 382355 | 756144 | 691175 | 180467 | 0 | 0 | 3212 | 1394 | 6.20905e+06 | 22 | -5.36535e+06 |
| churn-4 | 8.38861e+06 | 445187 | 5.58293e+06 | 1.01296e+06 | 360923 | 0 | 0 | 4799 | 1394 | 1.57968e+07 | 26 | 4.2224e+06 |
| checkpoint-4 | 8.38861e+06 | 445187 | 5.58293e+06 | 1.01296e+06 | 180468 | 0 | 0 | 4799 | 1394 | 1.56163e+07 | 26 | 4.04194e+06 |
| folded-4 | 8.38861e+06 | 445187 | 5.68803e+06 | 1.70413e+06 | 180468 | 0 | 0 | 4799 | 1394 | 1.64126e+07 | 26 | 4.83822e+06 |
| before_vacuum-4 | 8.38861e+06 | 445187 | 5.68951e+06 | 1.70413e+06 | 180468 | 0 | 0 | 4799 | 1544 | 1.64142e+07 | 26 | 4.83985e+06 |
| before_exhaustive-4 | 4.1943e+06 | 445187 | 5.69156e+06 | 1.70413e+06 | 180468 | 0 | 0 | 6386 | 1544 | 1.22236e+07 | 27 | 649184 |
| before_final_gc-4 | 4.1943e+06 | 445187 | 5.92165e+06 | 1.70413e+06 | 180468 | 0 | 0 | 3999 | 1544 | 1.24513e+07 | 27 | 876887 |
| maintenance-4 | 4.1943e+06 | 445187 | 945191 | 691175 | 180468 | 0 | 0 | 3781 | 1544 | 6.46165e+06 | 24 | -5.11275e+06 |
| reopen | 4.1943e+06 | 445187 | 945191 | 691175 | 12 | 0 | 0 | 3781 | 1544 | 6.28119e+06 | 24 | -5.29321e+06 |

Maintenance API medians (oracles and census excluded):

| Epoch | maintenance ns | fold ns | live rows folded | mutation parts before/after | active refs before/after | rewrite decision | typed deleted bytes | vacuum ns | vacuum completed |
| --- | ---: | ---: | ---: | --- | --- | --- | ---: | ---: | --- |
| 0 | 2.6703e+08 | 1.85848e+07 | 4096 | 512/0 | 896/1 | no_debt | 0 | 4.84722e+07 | True |
| 1 | 3.04052e+08 | 1.57009e+07 | 4096 | 512/0 | 769/1 | eligible | 1.70413e+06 | 4.00275e+07 | True |
| 2 | 3.52896e+08 | 1.41527e+07 | 4096 | 512/0 | 769/1 | eligible | 1.70413e+06 | 5.28838e+07 | True |
| 3 | 3.48068e+08 | 1.39111e+07 | 4096 | 512/0 | 769/1 | eligible | 1.70413e+06 | 4.69499e+07 | True |
| 4 | 3.59131e+08 | 2.98645e+07 | 4096 | 512/0 | 769/1 | eligible | 1.70413e+06 | 3.50297e+07 | True |

Post-view-release rewrite decisions: eligible. Raw packets retain eligibility, completed remaps, deleted bytes and protected/recovery retention.
Direct-backend command_wal_durable fixture; vacuum runs on the same live backend. Cached-wrapper checkpoint/reconcile overhead is omitted. Logical fold resets lineage; it does not itself prove physical reclamation.

Go calibration results remain in raw logs and are excluded from this table.
Call timers include encoding, full-row decode/oracle and callback bookkeeping. Epoch metrics also include ID preparation and latency/count/visited-ID bookkeeping. Maintenance, coverage aggregation and phase oracles are excluded.
Heap high is sampled at epoch boundaries; retained heap after GC includes live oracle maps and latency samples. RSS and unsampled peak are unavailable.
Component census uses logical file lengths, including redo WAL separately; no-op maintenance is recorded without a reclamation claim.
Full aggregate typed reachability sources/ref/segment/mapped attribution and value-log active/pending/protected/referenced classifications remain separate in the packet. Source classes can overlap; their byte counts are not unique retained bytes. Release GC is recorded; no-op or protected work is not reclamation.
Raw logs and packet retain per-epoch checkpoint/maintenance/debt/component observations and actual host load. Spread is descriptive; this diagnostic supplies no automatic performance acceptance threshold.
