# Standalone R1 lifecycle diagnostic

Qualification: **rehearsal**. No SQLite or cross-fixture speed comparison.
Source `a54d51a2a631480f2ed7203cdd66b2128e5a60f2`; runtime `4d4950757e75266540c795439eb07dee77ce273015fe36b71202cdb0ecf8d396`; harness `23b4099572225231acdb21a3b3a7c38df777a0d19620c2ece4b5eace670fb47a`.
2 fresh processes; 3 final epochs/process; 32 live rows; 8 calls/epoch.
Every final epoch revisits the same 4 IDs. Full row/posting oracles cover the live population; timed churn covers this bounded working set. This is finite hot-set evidence, not full-population or unlimited-capacity qualification.

| Final metric | Median | Minimum | Maximum | max/min |
| --- | ---: | ---: | ---: | ---: |
| B/op | 2.18963e+06 | 2.18804e+06 | 2.19121e+06 | 1.001 |
| allocs/op | 9423 | 9422 | 9424 | 1 |
| calls/op | 8 | 8 | 8 | 1 |
| loop-B/call | 273704 | 273505 | 273902 | 1.001 |
| loop-allocs/call | 1178 | 1178 | 1178 | 1 |
| loop-ns/call | 5.98686e+06 | 5.90549e+06 | 6.06823e+06 | 1.028 |
| mixed-calls/s | 167.05 | 164.8 | 169.3 | 1.027 |
| mixed-p95-ns/call | 1.22023e+07 | 1.21993e+07 | 1.22052e+07 | 1 |
| mixed-p99-ns/call | 1.23557e+07 | 1.22689e+07 | 1.24425e+07 | 1.014 |
| ns/op | 4.79029e+07 | 4.72514e+07 | 4.85545e+07 | 1.028 |
| process-retained-heap-B | 3.02788e+07 | 3.0275e+07 | 3.02827e+07 | 1 |
| sampled-heap-high-B | 4.85173e+07 | 4.85057e+07 | 4.85288e+07 | 1 |

Logical storage medians across final process results:

| Phase | index | vlog | leaf log | typed assets | redo WAL | other | all bytes | regular files | growth from ingest |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ingest | 524288 | 1014 | 436 | 5490 | 6266 | 474 | 537968 | 9 | 0 |
| churn-0 | 1.04858e+06 | 1485 | 2616 | 7964 | 7616 | 474 | 1.06873e+06 | 9 | 530763 |
| checkpoint-0 | 1.04858e+06 | 1485 | 2616 | 7964 | 7616 | 474 | 1.06873e+06 | 10 | 530763 |
| folded-0 | 1.31072e+06 | 1485 | 2616 | 13478 | 7616 | 474 | 1.33639e+06 | 10 | 798421 |
| before_vacuum-0 | 1.31072e+06 | 1485 | 2616 | 13478 | 7616 | 794 | 1.33671e+06 | 11 | 798741 |
| maintenance-0 | 262144 | 1485 | 2616 | 13478 | 7616 | 794 | 288133 | 11 | -249835 |
| after_view_release | 524288 | 1485 | 2616 | 18992 | 7616 | 794 | 555791 | 12 | 17823 |
| churn-1 | 1.04858e+06 | 1957 | 4820 | 21484 | 8985 | 794 | 1.08662e+06 | 12 | 548648 |
| checkpoint-1 | 1.04858e+06 | 1957 | 4820 | 21484 | 1381 | 794 | 1.07901e+06 | 12 | 541044 |
| folded-1 | 1.31072e+06 | 1957 | 4820 | 26998 | 1381 | 794 | 1.34667e+06 | 12 | 808702 |
| before_vacuum-1 | 1.31072e+06 | 1957 | 4820 | 26998 | 1381 | 794 | 1.34667e+06 | 12 | 808702 |
| maintenance-1 | 262144 | 1957 | 4820 | 26998 | 1381 | 794 | 298094 | 12 | -239874 |
| churn-2 | 786432 | 2428 | 7030 | 29490 | 2749 | 794 | 828923 | 12 | 290955 |
| checkpoint-2 | 1.04858e+06 | 2428 | 7030 | 29490 | 1380 | 794 | 1.0897e+06 | 12 | 551730 |
| folded-2 | 1.04858e+06 | 2428 | 7030 | 35004 | 1380 | 794 | 1.09521e+06 | 12 | 557244 |
| before_vacuum-2 | 1.04858e+06 | 2428 | 7030 | 35004 | 1380 | 794 | 1.09521e+06 | 12 | 557244 |
| maintenance-2 | 262144 | 2428 | 7030 | 35004 | 1380 | 794 | 308780 | 12 | -229188 |
| reopen | 262144 | 2428 | 7030 | 35004 | 12 | 794 | 307412 | 12 | -230556 |

Maintenance API medians (oracles and census excluded):

| Epoch | maintenance ns | fold ns | live rows folded | mutation parts before/after | active refs before/after | rewrite decision | typed deleted bytes | vacuum ns | vacuum completed |
| --- | ---: | ---: | ---: | --- | --- | --- | ---: | ---: | --- |
| 0 | 5.47797e+07 | 4.33686e+06 | 32 | 4/0 | 7/1 | no_debt | 0 | 1.6091e+07 | True |
| 1 | 6.57245e+07 | 4.36436e+06 | 32 | 4/0 | 7/1 | eligible | 5514 | 1.55685e+07 | True |
| 2 | 6.29098e+07 | 4.36143e+06 | 32 | 4/0 | 7/1 | eligible | 5514 | 1.57838e+07 | True |

Post-view-release rewrite decisions: eligible. Raw packets retain eligibility, completed remaps, deleted bytes and protected/recovery retention.
Direct-backend command_wal_durable fixture; vacuum runs on the same live backend. Cached-wrapper checkpoint/reconcile overhead is omitted. Logical fold resets lineage; it does not itself prove physical reclamation.

Go calibration results remain in raw logs and are excluded from this table.
Call timers include encoding, full-row decode/oracle and callback bookkeeping. Epoch metrics also include ID preparation and latency/count/visited-ID bookkeeping. Maintenance, coverage aggregation and phase oracles are excluded.
Heap high is sampled at epoch boundaries; retained heap after GC includes live oracle maps and latency samples. RSS and unsampled peak are unavailable.
Component census uses logical file lengths, including redo WAL separately; no-op maintenance is recorded without a reclamation claim.
Full aggregate typed reachability sources/ref/segment/mapped attribution and value-log active/pending/protected/referenced classifications remain separate in the packet. Source classes can overlap; their byte counts are not unique retained bytes. Release GC is recorded; no-op or protected work is not reclamation.
Raw logs and packet retain per-epoch checkpoint/maintenance/debt/component observations and actual host load. Spread is descriptive; this diagnostic supplies no automatic performance acceptance threshold.

