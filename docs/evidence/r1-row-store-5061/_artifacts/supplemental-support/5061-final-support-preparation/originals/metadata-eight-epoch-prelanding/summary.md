# Standalone R1 lifecycle diagnostic

Qualification: **rehearsal**. No SQLite or cross-fixture speed comparison.
Source `f79616f2aeb99b43af81877f26d646cca51177b9`; runtime `a451e5366ade3a2ee2b6f07e8994f7a6ecba9a83900c79e1a05935b2f2c6d849`; harness `125bc5ef6f0190f1ef3f57bd079a67b48efc5ad7e4b5c91a29a8f182fb649693`.
1 fresh processes; 8 final epochs/process; 32 live rows; 8 calls/epoch.
Every final epoch revisits the same 4 IDs. Full row/posting oracles cover the live population; timed churn covers this bounded working set. This is finite hot-set evidence, not full-population or unlimited-capacity qualification.

| Final metric | Median | Minimum | Maximum | max/min |
| --- | ---: | ---: | ---: | ---: |
| B/op | 2.59385e+06 | 2.59385e+06 | 2.59385e+06 | 1 |
| allocs/op | 10456 | 10456 | 10456 | 1 |
| calls/op | 8 | 8 | 8 | 1 |
| loop-B/call | 324231 | 324231 | 324231 | 1 |
| loop-allocs/call | 1307 | 1307 | 1307 | 1 |
| loop-ns/call | 7.62756e+06 | 7.62756e+06 | 7.62756e+06 | 1 |
| mixed-calls/s | 131.1 | 131.1 | 131.1 | 1 |
| mixed-p95-ns/call | 2.72636e+07 | 2.72636e+07 | 2.72636e+07 | 1 |
| mixed-p99-ns/call | 2.76586e+07 | 2.76586e+07 | 2.76586e+07 | 1 |
| ns/op | 6.10289e+07 | 6.10289e+07 | 6.10289e+07 | 1 |
| process-retained-heap-B | 3.67092e+07 | 3.67092e+07 | 3.67092e+07 | 1 |
| sampled-heap-high-B | 7.39029e+07 | 7.39029e+07 | 7.39029e+07 | 1 |

Logical storage medians across final process results:

| Phase | index | vlog | leaf log | typed assets | redo WAL | dictionary store | template store | immutable manifest metadata | other | all bytes | regular files | growth from ingest |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ingest | 4.1943e+06 | 1014 | 4586 | 5490 | 6266 | 0 | 0 | 554 | 470 | 4.21268e+06 | 12 | 0 |
| churn-0 | 4.1943e+06 | 1485 | 22274 | 7964 | 7616 | 0 | 0 | 554 | 470 | 4.23467e+06 | 12 | 21983 |
| checkpoint-0 | 4.1943e+06 | 1485 | 22274 | 7964 | 7616 | 0 | 0 | 554 | 470 | 4.23467e+06 | 13 | 21983 |
| folded-0 | 4.1943e+06 | 1485 | 24077 | 13478 | 7616 | 0 | 0 | 554 | 470 | 4.24198e+06 | 13 | 29300 |
| before_vacuum-0 | 4.1943e+06 | 1485 | 24077 | 13478 | 7616 | 0 | 0 | 554 | 791 | 4.2423e+06 | 14 | 29621 |
| before_exhaustive-0 | 4.1943e+06 | 1485 | 26146 | 13478 | 7616 | 0 | 0 | 855 | 791 | 4.24468e+06 | 15 | 31991 |
| before_final_gc-0 | 4.1943e+06 | 1485 | 34682 | 13478 | 7616 | 0 | 0 | 4049 | 791 | 4.2564e+06 | 24 | 43721 |
| maintenance-0 | 4.1943e+06 | 1485 | 34682 | 13478 | 7616 | 0 | 0 | 4049 | 791 | 4.2564e+06 | 24 | 43721 |
| after_view_release | 4.1943e+06 | 1485 | 6503 | 5514 | 7616 | 0 | 0 | 1264 | 791 | 4.21748e+06 | 16 | 4793 |
| churn-1 | 4.1943e+06 | 1957 | 24176 | 8006 | 8985 | 0 | 0 | 1264 | 791 | 4.23948e+06 | 17 | 26799 |
| checkpoint-1 | 4.1943e+06 | 1957 | 24176 | 8006 | 1381 | 0 | 0 | 1264 | 791 | 4.23188e+06 | 17 | 19195 |
| folded-1 | 4.1943e+06 | 1957 | 25990 | 13520 | 1381 | 0 | 0 | 1264 | 791 | 4.23921e+06 | 17 | 26523 |
| before_vacuum-1 | 4.1943e+06 | 1957 | 27343 | 13520 | 1381 | 0 | 0 | 1264 | 938 | 4.24071e+06 | 17 | 28023 |
| before_exhaustive-1 | 4.1943e+06 | 1957 | 29389 | 13520 | 1381 | 0 | 0 | 1776 | 938 | 4.24326e+06 | 18 | 30581 |
| before_final_gc-1 | 4.1943e+06 | 1957 | 33053 | 13520 | 1381 | 0 | 0 | 1750 | 938 | 4.2469e+06 | 19 | 34219 |
| maintenance-1 | 4.1943e+06 | 1957 | 8239 | 5514 | 1381 | 0 | 0 | 1508 | 938 | 4.21384e+06 | 16 | 1157 |
| churn-2 | 4.1943e+06 | 2428 | 26183 | 8006 | 2749 | 0 | 0 | 2228 | 938 | 4.23684e+06 | 20 | 24152 |
| checkpoint-2 | 4.1943e+06 | 2428 | 26183 | 8006 | 1380 | 0 | 0 | 2228 | 938 | 4.23547e+06 | 20 | 22783 |
| folded-2 | 4.1943e+06 | 2428 | 27997 | 13520 | 1380 | 0 | 0 | 2228 | 938 | 4.2428e+06 | 20 | 30111 |
| before_vacuum-2 | 4.1943e+06 | 2428 | 29350 | 13520 | 1380 | 0 | 0 | 2228 | 1085 | 4.2443e+06 | 20 | 31611 |
| before_exhaustive-2 | 4.1943e+06 | 2428 | 31397 | 13520 | 1380 | 0 | 0 | 2948 | 1085 | 4.24706e+06 | 21 | 34378 |
| before_final_gc-2 | 4.1943e+06 | 2428 | 31735 | 13520 | 1380 | 0 | 0 | 1994 | 1085 | 4.24645e+06 | 19 | 33762 |
| maintenance-2 | 4.1943e+06 | 2428 | 8255 | 5514 | 1380 | 0 | 0 | 1752 | 1085 | 4.21472e+06 | 16 | 2034 |
| churn-3 | 4.1943e+06 | 2900 | 26248 | 8006 | 2749 | 0 | 0 | 2475 | 1085 | 4.23777e+06 | 20 | 25083 |
| checkpoint-3 | 4.1943e+06 | 2900 | 26248 | 8006 | 1381 | 0 | 0 | 2475 | 1085 | 4.2364e+06 | 20 | 23715 |
| folded-3 | 4.1943e+06 | 2900 | 28062 | 13520 | 1381 | 0 | 0 | 2475 | 1085 | 4.24373e+06 | 20 | 31043 |
| before_vacuum-3 | 4.1943e+06 | 2900 | 29415 | 13520 | 1381 | 0 | 0 | 2475 | 1085 | 4.24508e+06 | 20 | 32396 |
| before_exhaustive-3 | 4.1943e+06 | 2900 | 31461 | 13520 | 1381 | 0 | 0 | 3198 | 1085 | 4.24785e+06 | 21 | 35165 |
| before_final_gc-3 | 4.1943e+06 | 2900 | 31781 | 13520 | 1381 | 0 | 0 | 2002 | 1085 | 4.24697e+06 | 19 | 34289 |
| maintenance-3 | 4.1943e+06 | 2900 | 8254 | 5514 | 1381 | 0 | 0 | 1759 | 1085 | 4.2152e+06 | 16 | 2513 |
| churn-4 | 4.1943e+06 | 3373 | 26227 | 8006 | 2751 | 0 | 0 | 2484 | 1085 | 4.23823e+06 | 20 | 25546 |
| checkpoint-4 | 4.1943e+06 | 3373 | 26227 | 8006 | 1382 | 0 | 0 | 2484 | 1085 | 4.23686e+06 | 20 | 24177 |
| folded-4 | 4.1943e+06 | 3373 | 28040 | 13520 | 1382 | 0 | 0 | 2484 | 1085 | 4.24419e+06 | 20 | 31504 |
| before_vacuum-4 | 4.1943e+06 | 3373 | 29394 | 13520 | 1382 | 0 | 0 | 2484 | 1085 | 4.24554e+06 | 20 | 32858 |
| before_exhaustive-4 | 4.1943e+06 | 3373 | 31440 | 13520 | 1382 | 0 | 0 | 3209 | 1085 | 4.24831e+06 | 21 | 35629 |
| before_final_gc-4 | 4.1943e+06 | 3373 | 31771 | 13520 | 1382 | 0 | 0 | 2004 | 1085 | 4.24744e+06 | 19 | 34755 |
| maintenance-4 | 4.1943e+06 | 3373 | 8263 | 5514 | 1382 | 0 | 0 | 1761 | 1085 | 4.21568e+06 | 16 | 2998 |
| churn-5 | 4.1943e+06 | 3848 | 26228 | 8006 | 2754 | 0 | 0 | 2486 | 1085 | 4.23871e+06 | 20 | 26027 |
| checkpoint-5 | 4.1943e+06 | 3848 | 26228 | 8006 | 1384 | 0 | 0 | 2486 | 1085 | 4.23734e+06 | 20 | 24657 |
| folded-5 | 4.1943e+06 | 3848 | 28035 | 13520 | 1384 | 0 | 0 | 2486 | 1085 | 4.24466e+06 | 20 | 31978 |
| before_vacuum-5 | 4.1943e+06 | 3848 | 29384 | 13520 | 1384 | 0 | 0 | 2486 | 1085 | 4.24601e+06 | 20 | 33327 |
| before_exhaustive-5 | 4.1943e+06 | 3848 | 31431 | 13520 | 1384 | 0 | 0 | 3211 | 1085 | 4.24878e+06 | 21 | 36099 |
| before_final_gc-5 | 4.1943e+06 | 3848 | 31739 | 13520 | 1384 | 0 | 0 | 2004 | 1085 | 4.24788e+06 | 19 | 35200 |
| maintenance-5 | 4.1943e+06 | 3848 | 8249 | 5514 | 1384 | 0 | 0 | 1761 | 1085 | 4.21614e+06 | 16 | 3461 |
| churn-6 | 4.1943e+06 | 4322 | 26158 | 8006 | 2755 | 0 | 0 | 2486 | 1085 | 4.23912e+06 | 20 | 26432 |
| checkpoint-6 | 4.1943e+06 | 4322 | 26158 | 8006 | 1383 | 0 | 0 | 2486 | 1085 | 4.23774e+06 | 20 | 25060 |
| folded-6 | 4.1943e+06 | 4322 | 27970 | 13520 | 1383 | 0 | 0 | 2486 | 1085 | 4.24507e+06 | 20 | 32386 |
| before_vacuum-6 | 4.1943e+06 | 4322 | 29324 | 13520 | 1383 | 0 | 0 | 2486 | 1085 | 4.24642e+06 | 20 | 33740 |
| before_exhaustive-6 | 4.1943e+06 | 4322 | 31371 | 13520 | 1383 | 0 | 0 | 3211 | 1085 | 4.2492e+06 | 21 | 36512 |
| before_final_gc-6 | 4.1943e+06 | 4322 | 31684 | 13520 | 1383 | 0 | 0 | 2004 | 1085 | 4.2483e+06 | 19 | 35618 |
| maintenance-6 | 4.1943e+06 | 4322 | 8240 | 5514 | 1383 | 0 | 0 | 1761 | 1085 | 4.21661e+06 | 16 | 3925 |
| churn-7 | 4.1943e+06 | 4797 | 26208 | 8006 | 2755 | 0 | 0 | 2486 | 1085 | 4.23964e+06 | 20 | 26957 |
| checkpoint-7 | 4.1943e+06 | 4797 | 26208 | 8006 | 1384 | 0 | 0 | 2486 | 1085 | 4.23827e+06 | 20 | 25586 |
| folded-7 | 4.1943e+06 | 4797 | 28018 | 13520 | 1384 | 0 | 0 | 2486 | 1085 | 4.24559e+06 | 20 | 32910 |
| before_vacuum-7 | 4.1943e+06 | 4797 | 29369 | 13520 | 1384 | 0 | 0 | 2486 | 1085 | 4.24694e+06 | 20 | 34261 |
| before_exhaustive-7 | 4.1943e+06 | 4797 | 31415 | 13520 | 1384 | 0 | 0 | 3211 | 1085 | 4.24972e+06 | 21 | 37032 |
| before_final_gc-7 | 4.1943e+06 | 4797 | 31740 | 13520 | 1384 | 0 | 0 | 2004 | 1085 | 4.24883e+06 | 19 | 36150 |
| maintenance-7 | 4.1943e+06 | 4797 | 8243 | 5514 | 1384 | 0 | 0 | 1761 | 1085 | 4.21709e+06 | 16 | 4404 |
| reopen | 4.1943e+06 | 4797 | 8243 | 5514 | 12 | 0 | 0 | 1761 | 1085 | 4.21572e+06 | 16 | 3032 |

Maintenance API medians (oracles and census excluded):

| Epoch | maintenance ns | fold ns | live rows folded | mutation parts before/after | active refs before/after | rewrite decision | typed deleted bytes | vacuum ns | vacuum completed |
| --- | ---: | ---: | ---: | --- | --- | --- | ---: | ---: | --- |
| 0 | 2.07415e+08 | 4.44981e+06 | 32 | 4/0 | 7/1 | no_debt | 0 | 3.28308e+07 | True |
| 1 | 2.96041e+08 | 4.5236e+06 | 32 | 4/0 | 7/1 | eligible | 13520 | 2.94728e+07 | True |
| 2 | 3.0408e+08 | 4.81116e+06 | 32 | 4/0 | 7/1 | eligible | 13520 | 2.95065e+07 | True |
| 3 | 3.03943e+08 | 4.7885e+06 | 32 | 4/0 | 7/1 | eligible | 13520 | 2.94049e+07 | True |
| 4 | 3.22103e+08 | 4.37289e+06 | 32 | 4/0 | 7/1 | eligible | 13520 | 3.10605e+07 | True |
| 5 | 3.05265e+08 | 5.4031e+06 | 32 | 4/0 | 7/1 | eligible | 13520 | 2.9757e+07 | True |
| 6 | 3.05889e+08 | 4.56358e+06 | 32 | 4/0 | 7/1 | eligible | 13520 | 2.93769e+07 | True |
| 7 | 3.04209e+08 | 4.40404e+06 | 32 | 4/0 | 7/1 | eligible | 13520 | 2.95871e+07 | True |

Post-view-release rewrite decisions: eligible. Raw packets retain eligibility, completed remaps, deleted bytes and protected/recovery retention.
Direct-backend command_wal_durable fixture; vacuum runs on the same live backend. Cached-wrapper checkpoint/reconcile overhead is omitted. Logical fold resets lineage; it does not itself prove physical reclamation.

Go calibration results remain in raw logs and are excluded from this table.
Call timers include encoding, full-row decode/oracle and callback bookkeeping. Epoch metrics also include ID preparation and latency/count/visited-ID bookkeeping. Maintenance, coverage aggregation and phase oracles are excluded.
Heap high is sampled at epoch boundaries; retained heap after GC includes live oracle maps and latency samples. RSS and unsampled peak are unavailable.
Component census uses logical file lengths, including redo WAL separately; no-op maintenance is recorded without a reclamation claim.
Full aggregate typed reachability sources/ref/segment/mapped attribution and value-log active/pending/protected/referenced classifications remain separate in the packet. Source classes can overlap; their byte counts are not unique retained bytes. Release GC is recorded; no-op or protected work is not reclamation.
Raw logs and packet retain per-epoch checkpoint/maintenance/debt/component observations and actual host load. Spread is descriptive; this diagnostic supplies no automatic performance acceptance threshold.
