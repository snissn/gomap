# Finite D descriptive summary

**Preparation/presentation only; no qualification or acceptance verdict.**

[Original full-value projection](../../../../docs/evidence/r1-row-store-5061/_artifacts/final-M-3325dfe/results/D/descriptive-projection.json) · [Complete phase trajectories / raw links](../../../../docs/evidence/r1-row-store-5061/_artifacts/final-M-3325dfe/results/D/descriptive-report.md) · [Independent validation log](../../../../docs/evidence/r1-row-store-5061/_artifacts/final-M-3325dfe/receipts/D/independent-validation.log)

Captured source `3325dfe77940fec8587d8b61b1ac4e0b2f72caca`; runtime `eab40aaf77ed307d92a7a38adf0dd80be07a7e12bdda8a2ec3118baf9cad33ff`; harness `325d5b41579a492a430da6cbb6162363ff82b42bb6553eddf2e325584fab4c13`.
Producer label `retained` copied without endorsement; 5 fresh processes, 5 final epochs/process, 4096 live rows, 1024 mixed calls/epoch.
Each fresh process also emits a Go calibration result; its scope is preserved below and excluded from final summaries. Original reported Go ns/op, B/op and allocs/op use the epoch denominator. Per-call values use each process's actual final total calls. Mixed-call timers include encode/decode/oracle/callback work; epoch metrics additionally include preparation/bookkeeping. Observed process elapsed includes calibration, setup, maintenance, oracles, reopen and process overhead.
Direct backend, command-WAL durable, repeated finite hot set; no cached-wrapper equivalence. Go allocation differences observe process activity during the loop. Sampled heap high and post-GC retained heap are separate; retained heap includes fixture/oracle maps and latency samples. Neither is RSS or an unsampled peak.
Spread is absolute max minus min in the stated units. No comparative ratios, new thresholds, automatic acceptance, or infinite/physical-capacity bound.

| Process | PID | mixed_mean_ns_per_call | mixed_calls_per_s | Go_loop_B_per_call | Go_loop_objects_per_call | observed_process_elapsed_ns | mixed_call_ns_total | mixed_calls_total | sampled_heap_high_B | post_GC_retained_heap_B | mixed_p95_ns_per_call | mixed_p99_ns_per_call | Go_ns_per_epoch_reported | Go_B_per_epoch_reported | Go_objects_per_epoch_reported |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 3201903 | 6645970.39 | 150.467116 | 811892.817 | 5988.64082 | 83537829182 | 34027368417 | 5120 | 175874240 | 79683296 | 13570921 | 18148105 | 6.80650364e+09 | 831378244 | 6132368 |
| 2 | 3203911 | 6636828.73 | 150.674372 | 810141.717 | 5988.075 | 83439921057 | 33980563082 | 5120 | 168690776 | 80641256 | 13402570 | 19875402 | 6.7971455e+09 | 829585118 | 6131788 |
| 3 | 3205743 | 6878242.86 | 145.385969 | 810896.537 | 5988.6666 | 84551984820 | 35216603437 | 5120 | 179945128 | 80552992 | 14914685 | 21275046 | 7.04436079e+09 | 830358054 | 6132394 |
| 4 | 3207576 | 6764807.78 | 147.82386 | 809936.866 | 5988.79902 | 84434568425 | 34635815824 | 5120 | 177060000 | 79678448 | 13739153 | 17694991 | 6.92841792e+09 | 829375350 | 6132530 |
| 5 | 3209881 | 6734572.72 | 148.48752 | 811837.761 | 5988.72891 | 83908298992 | 34481012342 | 5120 | 175546608 | 79684480 | 13617892 | 17620530 | 6.89737551e+09 | 831321867 | 6132458 |


| Metric / units | Median | Min | Max | Absolute spread (max − min) |
| --- | --- | --- | --- | --- |
| mixed_mean_ns_per_call | 6734572.72 | 6636828.73 | 6878242.86 | 241414.132 |
| mixed_calls_per_s | 148.48752 | 145.385969 | 150.674372 | 5.28840337 |
| Go_loop_B_per_call | 810896.537 | 809936.866 | 811892.817 | 1955.95156 |
| Go_loop_objects_per_call | 5988.6666 | 5988.075 | 5988.79902 | 0.724023438 |
| observed_process_elapsed_ns | 83908298992 | 83439921057 | 84551984820 | 1112063763 |
| mixed_call_ns_total | 34481012342 | 33980563082 | 35216603437 | 1236040355 |
| mixed_calls_total | 5120 | 5120 | 5120 | 0 |
| sampled_heap_high_B | 175874240 | 168690776 | 179945128 | 11254352 |
| post_GC_retained_heap_B | 79684480 | 79678448 | 80641256 | 962808 |
| mixed_p95_ns_per_call | 13617892 | 13402570 | 14914685 | 1512115 |
| mixed_p99_ns_per_call | 18148105 | 17620530 | 21275046 | 3654516 |
| Go_ns_per_epoch_reported | 6.89737551e+09 | 6.7971455e+09 | 7.04436079e+09 | 247215291 |
| Go_B_per_epoch_reported | 830358054 | 829375350 | 831378244 | 2002894 |
| Go_objects_per_epoch_reported | 6132394 | 6131788 | 6132530 | 742 |

Endpoint chronology: the held view is released after the first epoch. “Last pre-held-view-release” means the census immediately before that release, not the final epoch. The final epoch endpoint is shown separately. Persistent-only totals exclude redo WAL bytes and files. Component values are logical file lengths and regular-file counts; source classes/retention counters are not added to these totals.

Endpoint bytes; cross-process medians after within-process differences:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 8388608 | 131289 | 1627931 | 725636 | 699917 | 0 | 0 | 554 | 470 | 11574405 | 10874488 |
| last_pre_held_view_release | maintenance-0 | 4194304 | 193991 | 7657303 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14665395 | 13787110 |
| after_held_view_release | after_view_release | 4194304 | 193991 | 189498 | 691175 | 878285 | 0 | 0 | 1277 | 795 | 6149325 | 5271040 |
| last_epoch_maintenance | maintenance-4 | 4194304 | 445187 | 192373 | 691175 | 180468 | 0 | 0 | 1805 | 1094 | 5706406 | 5525938 |
| reopen | reopen | 4194304 | 445187 | 192373 | 691175 | 12 | 0 | 0 | 1805 | 1094 | 5525950 | 5525938 |


Endpoint regular-file counts; cross-process medians after within-process differences:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 1 | 1 | 2 | 1 | 2 | 0 | 0 | 2 | 3 | 12 | 10 |
| last_pre_held_view_release | maintenance-0 | 1 | 1 | 6 | 1 | 3 | 0 | 0 | 8 | 4 | 24 | 21 |
| after_held_view_release | after_view_release | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |
| last_epoch_maintenance | maintenance-4 | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |
| reopen | reopen | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |


Byte differences since same-process ingest; cross-process medians after within-process differences:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| last_pre_held_view_release | maintenance-0 | -4194304 | 62702 | 6029393 | 1011001 | 178368 | 0 | 0 | 3526 | 325 | 3091011 | 2912643 |
| after_held_view_release | after_view_release | -4194304 | 62702 | -1438433 | -34461 | 178368 | 0 | 0 | 723 | 325 | -5425080 | -5603448 |
| last_epoch_maintenance | maintenance-4 | -4194304 | 313898 | -1435558 | -34461 | -519449 | 0 | 0 | 1251 | 624 | -5867999 | -5348550 |
| reopen | reopen | -4194304 | 313898 | -1435558 | -34461 | -699905 | 0 | 0 | 1251 | 624 | -6048455 | -5348550 |


File-count differences since same-process ingest; cross-process medians after within-process differences:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| last_pre_held_view_release | maintenance-0 | 0 | 0 | 4 | 0 | 1 | 0 | 0 | 6 | 1 | 12 | 11 |
| after_held_view_release | after_view_release | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |
| last_epoch_maintenance | maintenance-4 | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |
| reopen | reopen | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |

API group totals are summed across matching observations within each process before medians. Aggregate maintenance overlaps its constituent stages; exhaustive work overlaps its internal API phases. Do not add rows across these categories or divide maintenance aggregates into individual user-call attribution. Held-view release work and final fallback/typed/leaf GC retain separate groups.

| API group | Median observed API calls | Median total ns | Min total ns | Max total ns | Absolute spread ns |
| --- | --- | --- | --- | --- | --- |
| epochs/maintenance_ns | 5 | 1695967068 | 1600218102 | 1701516562 | 101298460 |
| epochs/flush_ns | 5 | 54220 | 49481 | 64810 | 15329 |
| epochs/checkpoint_ns | 5 | 51792971 | 51173004 | 52931650 | 1758646 |
| epochs/before_fold_gc_ns | 5 | 3939078 | 3764577 | 4075728 | 311151 |
| epochs/fold_ns | 5 | 74626943 | 73250158 | 81299315 | 8049157 |
| epochs/fold_checkpoint_ns | 5 | 40383571 | 34824256 | 49947992 | 15123736 |
| epochs/overlay_ns | 5 | 65171 | 58932 | 65341 | 6409 |
| epochs/overlay_checkpoint_ns | 5 | 27380 | 24510 | 29531 | 5021 |
| epochs/vlog_gc_ns | 5 | 46847273 | 44062076 | 47474369 | 3412293 |
| epochs/vacuum_ns | 5 | 205580447 | 180712047 | 269858690 | 89146643 |
| epochs/reclaim/plan_ns | 5 | 5982997 | 5756464 | 6085158 | 328694 |
| epochs/reclaim/probe_ns | 5 | 1066891 | 1047420 | 1093698 | 46278 |
| epochs/reclaim/rewrite_ns | 5 | 36960457 | 33381785 | 68773754 | 35391969 |
| epochs/reclaim/checkpoint_ns | 5 | 28090842 | 27240603 | 28335515 | 1094912 |
| epochs/reclaim/gc_ns | 5 | 18002213 | 17881853 | 18169176 | 287323 |
| epochs/exhaustive/plan_ns | 5 | 5425854 | 4915668 | 5639494 | 723826 |
| epochs/exhaustive/work_ns | 5 | 856407071 | 837806619 | 901361446 | 63554827 |
| epochs/final/refresh_ns | 5 | 59359083 | 56692036 | 114240577 | 57548541 |
| epochs/final/typed_gc_ns | 5 | 18972193 | 18222576 | 19466777 | 1244201 |
| epochs/final/leaf_gc_ns | 5 | 168636950 | 167525560 | 173374716 | 5849156 |
| after_view_release/reclaim/plan_ns | 1 | 461234 | 453454 | 504525 | 51071 |
| after_view_release/reclaim/probe_ns | 1 | 349853 | 340333 | 364514 | 24181 |
| after_view_release/reclaim/rewrite_ns | 1 | 27627278 | 25258624 | 31391094 | 6132470 |
| after_view_release/reclaim/checkpoint_ns | 1 | 6491092 | 6485423 | 6678224 | 192801 |
| after_view_release/reclaim/gc_ns | 1 | 481244 | 478195 | 593156 | 114961 |
| after_view_release/final/refresh_ns | 1 | 3274801 | 3259822 | 18583740 | 15323918 |
| after_view_release/final/typed_gc_ns | 1 | 5572454 | 4507013 | 7271190 | 2764177 |
| after_view_release/final/leaf_gc_ns | 1 | 66736185 | 65809996 | 86641838 | 20831842 |


<details><summary>Process 1: ID coverage, endpoints and whole API durations</summary>

Calibration epochs [1]; distinct IDs 512; cross-epoch revisits 2048; revisited distinct IDs 512. Original Go metrics remain unchanged in the summary JSON and full projection.

| Epoch | Distinct IDs | New IDs | Revisited IDs | Cumulative distinct IDs | IDs SHA256 |
| --- | --- | --- | --- | --- | --- |
| 0 | 512 | 512 | 0 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 1 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 2 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 3 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 4 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |


Actual endpoint bytes:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 8388608 | 131289 | 1627910 | 725636 | 699917 | 0 | 0 | 554 | 470 | 11574384 | 10874467 |
| last_pre_held_view_release | maintenance-0 | 4194304 | 193991 | 7657303 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14665395 | 13787110 |
| after_held_view_release | after_view_release | 4194304 | 193991 | 189498 | 691175 | 878285 | 0 | 0 | 1277 | 795 | 6149325 | 5271040 |
| last_epoch_maintenance | maintenance-4 | 4194304 | 445187 | 192373 | 691175 | 180468 | 0 | 0 | 1805 | 1094 | 5706406 | 5525938 |
| reopen | reopen | 4194304 | 445187 | 192373 | 691175 | 12 | 0 | 0 | 1805 | 1094 | 5525950 | 5525938 |


Actual endpoint files:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 1 | 1 | 2 | 1 | 2 | 0 | 0 | 2 | 3 | 12 | 10 |
| last_pre_held_view_release | maintenance-0 | 1 | 1 | 6 | 1 | 3 | 0 | 0 | 8 | 4 | 24 | 21 |
| after_held_view_release | after_view_release | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |
| last_epoch_maintenance | maintenance-4 | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |
| reopen | reopen | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |


Actual bytes Δ from ingest:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| last_pre_held_view_release | maintenance-0 | -4194304 | 62702 | 6029393 | 1011001 | 178368 | 0 | 0 | 3526 | 325 | 3091011 | 2912643 |
| after_held_view_release | after_view_release | -4194304 | 62702 | -1438412 | -34461 | 178368 | 0 | 0 | 723 | 325 | -5425059 | -5603427 |
| last_epoch_maintenance | maintenance-4 | -4194304 | 313898 | -1435537 | -34461 | -519449 | 0 | 0 | 1251 | 624 | -5867978 | -5348529 |
| reopen | reopen | -4194304 | 313898 | -1435537 | -34461 | -699905 | 0 | 0 | 1251 | 624 | -6048434 | -5348529 |


Actual files Δ from ingest:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| last_pre_held_view_release | maintenance-0 | 0 | 0 | 4 | 0 | 1 | 0 | 0 | 6 | 1 | 12 | 11 |
| after_held_view_release | after_view_release | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |
| last_epoch_maintenance | maintenance-4 | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |
| reopen | reopen | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |


| Stage | API group | Whole API duration ns |
| --- | --- | --- |
| epoch-0 | epochs/maintenance_ns | 252475004 |
| epoch-0 | epochs/flush_ns | 10530 |
| epoch-0 | epochs/checkpoint_ns | 7178080 |
| epoch-0 | epochs/before_fold_gc_ns | 1368713 |
| epoch-0 | epochs/fold_ns | 15260427 |
| epoch-0 | epochs/fold_checkpoint_ns | 6596834 |
| epoch-0 | epochs/overlay_ns | 14391 |
| epoch-0 | epochs/overlay_checkpoint_ns | 5910 |
| epoch-0 | epochs/vlog_gc_ns | 11017417 |
| epoch-0 | epochs/vacuum_ns | 38931857 |
| epoch-0/reclaim | epochs/reclaim/plan_ns | 1098740 |
| epoch-0/reclaim | epochs/reclaim/probe_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/rewrite_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/checkpoint_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/gc_ns | 1025550 |
| epoch-0 | epochs/exhaustive/plan_ns | 1168332 |
| epoch-0 | epochs/exhaustive/work_ns | 157705295 |
| epoch-0/final | epochs/final/refresh_ns | 9692454 |
| epoch-0/final | epochs/final/typed_gc_ns | 1094141 |
| epoch-0/final | epochs/final/leaf_gc_ns | 306333 |
| epoch-1 | epochs/maintenance_ns | 298030371 |
| epoch-1 | epochs/flush_ns | 21820 |
| epoch-1 | epochs/checkpoint_ns | 11759834 |
| epoch-1 | epochs/before_fold_gc_ns | 651446 |
| epoch-1 | epochs/fold_ns | 14981785 |
| epoch-1 | epochs/fold_checkpoint_ns | 7215399 |
| epoch-1 | epochs/overlay_ns | 12880 |
| epoch-1 | epochs/overlay_checkpoint_ns | 4520 |
| epoch-1 | epochs/vlog_gc_ns | 8295540 |
| epoch-1 | epochs/vacuum_ns | 34130980 |
| epoch-1/reclaim | epochs/reclaim/plan_ns | 1206642 |
| epoch-1/reclaim | epochs/reclaim/probe_ns | 256342 |
| epoch-1/reclaim | epochs/reclaim/rewrite_ns | 9574693 |
| epoch-1/reclaim | epochs/reclaim/checkpoint_ns | 7095969 |
| epoch-1/reclaim | epochs/reclaim/gc_ns | 4177300 |
| epoch-1 | epochs/exhaustive/plan_ns | 1023620 |
| epoch-1 | epochs/exhaustive/work_ns | 133500571 |
| epoch-1/final | epochs/final/refresh_ns | 9657743 |
| epoch-1/final | epochs/final/typed_gc_ns | 4486213 |
| epoch-1/final | epochs/final/leaf_gc_ns | 49977074 |
| epoch-2 | epochs/maintenance_ns | 383605660 |
| epoch-2 | epochs/flush_ns | 11010 |
| epoch-2 | epochs/checkpoint_ns | 10928476 |
| epoch-2 | epochs/before_fold_gc_ns | 641877 |
| epoch-2 | epochs/fold_ns | 14439419 |
| epoch-2 | epochs/fold_checkpoint_ns | 7172700 |
| epoch-2 | epochs/overlay_ns | 13120 |
| epoch-2 | epochs/overlay_checkpoint_ns | 5250 |
| epoch-2 | epochs/vlog_gc_ns | 8487562 |
| epoch-2 | epochs/vacuum_ns | 36928497 |
| epoch-2/reclaim | epochs/reclaim/plan_ns | 1225352 |
| epoch-2/reclaim | epochs/reclaim/probe_ns | 266812 |
| epoch-2/reclaim | epochs/reclaim/rewrite_ns | 8148589 |
| epoch-2/reclaim | epochs/reclaim/checkpoint_ns | 6979628 |
| epoch-2/reclaim | epochs/reclaim/gc_ns | 4235511 |
| epoch-2 | epochs/exhaustive/plan_ns | 1123781 |
| epoch-2 | epochs/exhaustive/work_ns | 227677171 |
| epoch-2/final | epochs/final/refresh_ns | 12223748 |
| epoch-2/final | epochs/final/typed_gc_ns | 4517314 |
| epoch-2/final | epochs/final/leaf_gc_ns | 38579843 |
| epoch-3 | epochs/maintenance_ns | 358070154 |
| epoch-3 | epochs/flush_ns | 10280 |
| epoch-3 | epochs/checkpoint_ns | 11097148 |
| epoch-3 | epochs/before_fold_gc_ns | 633636 |
| epoch-3 | epochs/fold_ns | 14232688 |
| epoch-3 | epochs/fold_checkpoint_ns | 15154897 |
| epoch-3 | epochs/overlay_ns | 12060 |
| epoch-3 | epochs/overlay_checkpoint_ns | 5630 |
| epoch-3 | epochs/vlog_gc_ns | 8395741 |
| epoch-3 | epochs/vacuum_ns | 35104449 |
| epoch-3/reclaim | epochs/reclaim/plan_ns | 1224692 |
| epoch-3/reclaim | epochs/reclaim/probe_ns | 263963 |
| epoch-3/reclaim | epochs/reclaim/rewrite_ns | 6663984 |
| epoch-3/reclaim | epochs/reclaim/checkpoint_ns | 7005768 |
| epoch-3/reclaim | epochs/reclaim/gc_ns | 4196621 |
| epoch-3 | epochs/exhaustive/plan_ns | 1095530 |
| epoch-3 | epochs/exhaustive/work_ns | 194121448 |
| epoch-3/final | epochs/final/refresh_ns | 15200067 |
| epoch-3/final | epochs/final/typed_gc_ns | 4469003 |
| epoch-3/final | epochs/final/leaf_gc_ns | 39182549 |
| epoch-4 | epochs/maintenance_ns | 354518814 |
| epoch-4 | epochs/flush_ns | 11170 |
| epoch-4 | epochs/checkpoint_ns | 11286549 |
| epoch-4 | epochs/before_fold_gc_ns | 643406 |
| epoch-4 | epochs/fold_ns | 22384996 |
| epoch-4 | epochs/fold_checkpoint_ns | 7317811 |
| epoch-4 | epochs/overlay_ns | 12860 |
| epoch-4 | epochs/overlay_checkpoint_ns | 5880 |
| epoch-4 | epochs/vlog_gc_ns | 8699304 |
| epoch-4 | epochs/vacuum_ns | 35616264 |
| epoch-4/reclaim | epochs/reclaim/plan_ns | 1227571 |
| epoch-4/reclaim | epochs/reclaim/probe_ns | 260303 |
| epoch-4/reclaim | epochs/reclaim/rewrite_ns | 9606923 |
| epoch-4/reclaim | epochs/reclaim/checkpoint_ns | 7254150 |
| epoch-4/reclaim | epochs/reclaim/gc_ns | 4246871 |
| epoch-4 | epochs/exhaustive/plan_ns | 1117441 |
| epoch-4 | epochs/exhaustive/work_ns | 188356961 |
| epoch-4/final | epochs/final/refresh_ns | 12585071 |
| epoch-4/final | epochs/final/typed_gc_ns | 4405522 |
| epoch-4/final | epochs/final/leaf_gc_ns | 39479761 |
| after_view_release/reclaim | after_view_release/reclaim/plan_ns | 486914 |
| after_view_release/reclaim | after_view_release/reclaim/probe_ns | 364514 |
| after_view_release/reclaim | after_view_release/reclaim/rewrite_ns | 26931890 |
| after_view_release/reclaim | after_view_release/reclaim/checkpoint_ns | 6486673 |
| after_view_release/reclaim | after_view_release/reclaim/gc_ns | 478195 |
| after_view_release/final | after_view_release/final/refresh_ns | 3259822 |
| after_view_release/final | after_view_release/final/typed_gc_ns | 5622155 |
| after_view_release/final | after_view_release/final/leaf_gc_ns | 69024687 |


</details>


<details><summary>Process 2: ID coverage, endpoints and whole API durations</summary>

Calibration epochs [1]; distinct IDs 512; cross-epoch revisits 2048; revisited distinct IDs 512. Original Go metrics remain unchanged in the summary JSON and full projection.

| Epoch | Distinct IDs | New IDs | Revisited IDs | Cumulative distinct IDs | IDs SHA256 |
| --- | --- | --- | --- | --- | --- |
| 0 | 512 | 512 | 0 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 1 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 2 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 3 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 4 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |


Actual endpoint bytes:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 8388608 | 131289 | 1627932 | 725636 | 699917 | 0 | 0 | 554 | 470 | 11574406 | 10874489 |
| last_pre_held_view_release | maintenance-0 | 4194304 | 193991 | 7655589 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14663681 | 13785396 |
| after_held_view_release | after_view_release | 4194304 | 193991 | 189498 | 691175 | 878285 | 0 | 0 | 1277 | 795 | 6149325 | 5271040 |
| last_epoch_maintenance | maintenance-4 | 4194304 | 445187 | 192373 | 691175 | 180468 | 0 | 0 | 1805 | 1094 | 5706406 | 5525938 |
| reopen | reopen | 4194304 | 445187 | 192373 | 691175 | 12 | 0 | 0 | 1805 | 1094 | 5525950 | 5525938 |


Actual endpoint files:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 1 | 1 | 2 | 1 | 2 | 0 | 0 | 2 | 3 | 12 | 10 |
| last_pre_held_view_release | maintenance-0 | 1 | 1 | 6 | 1 | 3 | 0 | 0 | 8 | 4 | 24 | 21 |
| after_held_view_release | after_view_release | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |
| last_epoch_maintenance | maintenance-4 | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |
| reopen | reopen | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |


Actual bytes Δ from ingest:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| last_pre_held_view_release | maintenance-0 | -4194304 | 62702 | 6027657 | 1011001 | 178368 | 0 | 0 | 3526 | 325 | 3089275 | 2910907 |
| after_held_view_release | after_view_release | -4194304 | 62702 | -1438434 | -34461 | 178368 | 0 | 0 | 723 | 325 | -5425081 | -5603449 |
| last_epoch_maintenance | maintenance-4 | -4194304 | 313898 | -1435559 | -34461 | -519449 | 0 | 0 | 1251 | 624 | -5868000 | -5348551 |
| reopen | reopen | -4194304 | 313898 | -1435559 | -34461 | -699905 | 0 | 0 | 1251 | 624 | -6048456 | -5348551 |


Actual files Δ from ingest:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| last_pre_held_view_release | maintenance-0 | 0 | 0 | 4 | 0 | 1 | 0 | 0 | 6 | 1 | 12 | 11 |
| after_held_view_release | after_view_release | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |
| last_epoch_maintenance | maintenance-4 | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |
| reopen | reopen | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |


| Stage | API group | Whole API duration ns |
| --- | --- | --- |
| epoch-0 | epochs/maintenance_ns | 257900892 |
| epoch-0 | epochs/flush_ns | 10590 |
| epoch-0 | epochs/checkpoint_ns | 7024298 |
| epoch-0 | epochs/before_fold_gc_ns | 1342393 |
| epoch-0 | epochs/fold_ns | 14486510 |
| epoch-0 | epochs/fold_checkpoint_ns | 6657814 |
| epoch-0 | epochs/overlay_ns | 14480 |
| epoch-0 | epochs/overlay_checkpoint_ns | 5670 |
| epoch-0 | epochs/vlog_gc_ns | 13441920 |
| epoch-0 | epochs/vacuum_ns | 57309444 |
| epoch-0/reclaim | epochs/reclaim/plan_ns | 1114371 |
| epoch-0/reclaim | epochs/reclaim/probe_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/rewrite_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/checkpoint_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/gc_ns | 991309 |
| epoch-0 | epochs/exhaustive/plan_ns | 1163421 |
| epoch-0 | epochs/exhaustive/work_ns | 143418027 |
| epoch-0/final | epochs/final/refresh_ns | 9526442 |
| epoch-0/final | epochs/final/typed_gc_ns | 1081330 |
| epoch-0/final | epochs/final/leaf_gc_ns | 312873 |
| epoch-1 | epochs/maintenance_ns | 351350627 |
| epoch-1 | epochs/flush_ns | 10740 |
| epoch-1 | epochs/checkpoint_ns | 11266479 |
| epoch-1 | epochs/before_fold_gc_ns | 636666 |
| epoch-1 | epochs/fold_ns | 14021386 |
| epoch-1 | epochs/fold_checkpoint_ns | 7191989 |
| epoch-1 | epochs/overlay_ns | 11920 |
| epoch-1 | epochs/overlay_checkpoint_ns | 5420 |
| epoch-1 | epochs/vlog_gc_ns | 8371931 |
| epoch-1 | epochs/vacuum_ns | 88795489 |
| epoch-1/reclaim | epochs/reclaim/plan_ns | 1216762 |
| epoch-1/reclaim | epochs/reclaim/probe_ns | 263113 |
| epoch-1/reclaim | epochs/reclaim/rewrite_ns | 8152688 |
| epoch-1/reclaim | epochs/reclaim/checkpoint_ns | 6970418 |
| epoch-1/reclaim | epochs/reclaim/gc_ns | 4159460 |
| epoch-1 | epochs/exhaustive/plan_ns | 1009250 |
| epoch-1 | epochs/exhaustive/work_ns | 135199217 |
| epoch-1/final | epochs/final/refresh_ns | 9584812 |
| epoch-1/final | epochs/final/typed_gc_ns | 4388452 |
| epoch-1/final | epochs/final/leaf_gc_ns | 50094435 |
| epoch-2 | epochs/maintenance_ns | 384779810 |
| epoch-2 | epochs/flush_ns | 11440 |
| epoch-2 | epochs/checkpoint_ns | 10734513 |
| epoch-2 | epochs/before_fold_gc_ns | 651266 |
| epoch-2 | epochs/fold_ns | 15245107 |
| epoch-2 | epochs/fold_checkpoint_ns | 12112988 |
| epoch-2 | epochs/overlay_ns | 13620 |
| epoch-2 | epochs/overlay_checkpoint_ns | 5600 |
| epoch-2 | epochs/vlog_gc_ns | 8594863 |
| epoch-2 | epochs/vacuum_ns | 37076429 |
| epoch-2/reclaim | epochs/reclaim/plan_ns | 1245732 |
| epoch-2/reclaim | epochs/reclaim/probe_ns | 263903 |
| epoch-2/reclaim | epochs/reclaim/rewrite_ns | 44467740 |
| epoch-2/reclaim | epochs/reclaim/checkpoint_ns | 7210550 |
| epoch-2/reclaim | epochs/reclaim/gc_ns | 4304131 |
| epoch-2 | epochs/exhaustive/plan_ns | 1081661 |
| epoch-2 | epochs/exhaustive/work_ns | 186728595 |
| epoch-2/final | epochs/final/refresh_ns | 11144497 |
| epoch-2/final | epochs/final/typed_gc_ns | 4510884 |
| epoch-2/final | epochs/final/leaf_gc_ns | 39376291 |
| epoch-3 | epochs/maintenance_ns | 351378925 |
| epoch-3 | epochs/flush_ns | 10420 |
| epoch-3 | epochs/checkpoint_ns | 10434411 |
| epoch-3 | epochs/before_fold_gc_ns | 640036 |
| epoch-3 | epochs/fold_ns | 15201487 |
| epoch-3 | epochs/fold_checkpoint_ns | 7202510 |
| epoch-3 | epochs/overlay_ns | 12481 |
| epoch-3 | epochs/overlay_checkpoint_ns | 5530 |
| epoch-3 | epochs/vlog_gc_ns | 8478702 |
| epoch-3 | epochs/vacuum_ns | 46689851 |
| epoch-3/reclaim | epochs/reclaim/plan_ns | 1219701 |
| epoch-3/reclaim | epochs/reclaim/probe_ns | 260153 |
| epoch-3/reclaim | epochs/reclaim/rewrite_ns | 7966617 |
| epoch-3/reclaim | epochs/reclaim/checkpoint_ns | 7073118 |
| epoch-3/reclaim | epochs/reclaim/gc_ns | 4319972 |
| epoch-3 | epochs/exhaustive/plan_ns | 1090170 |
| epoch-3 | epochs/exhaustive/work_ns | 179377634 |
| epoch-3/final | epochs/final/refresh_ns | 17825562 |
| epoch-3/final | epochs/final/typed_gc_ns | 4388792 |
| epoch-3/final | epochs/final/leaf_gc_ns | 39181778 |
| epoch-4 | epochs/maintenance_ns | 354135354 |
| epoch-4 | epochs/flush_ns | 11030 |
| epoch-4 | epochs/checkpoint_ns | 11713303 |
| epoch-4 | epochs/before_fold_gc_ns | 644097 |
| epoch-4 | epochs/fold_ns | 15546820 |
| epoch-4 | epochs/fold_checkpoint_ns | 7218270 |
| epoch-4 | epochs/overlay_ns | 12840 |
| epoch-4 | epochs/overlay_checkpoint_ns | 5190 |
| epoch-4 | epochs/vlog_gc_ns | 8586953 |
| epoch-4 | epochs/vacuum_ns | 39987477 |
| epoch-4/reclaim | epochs/reclaim/plan_ns | 1203862 |
| epoch-4/reclaim | epochs/reclaim/probe_ns | 276362 |
| epoch-4/reclaim | epochs/reclaim/rewrite_ns | 8186709 |
| epoch-4/reclaim | epochs/reclaim/checkpoint_ns | 7058379 |
| epoch-4/reclaim | epochs/reclaim/gc_ns | 4227341 |
| epoch-4 | epochs/exhaustive/plan_ns | 1081031 |
| epoch-4 | epochs/exhaustive/work_ns | 193083146 |
| epoch-4/final | epochs/final/refresh_ns | 11139327 |
| epoch-4/final | epochs/final/typed_gc_ns | 4481644 |
| epoch-4/final | epochs/final/leaf_gc_ns | 39671573 |
| after_view_release/reclaim | after_view_release/reclaim/plan_ns | 461234 |
| after_view_release/reclaim | after_view_release/reclaim/probe_ns | 342274 |
| after_view_release/reclaim | after_view_release/reclaim/rewrite_ns | 29613786 |
| after_view_release/reclaim | after_view_release/reclaim/checkpoint_ns | 6498843 |
| after_view_release/reclaim | after_view_release/reclaim/gc_ns | 481244 |
| after_view_release/final | after_view_release/final/refresh_ns | 5207730 |
| after_view_release/final | after_view_release/final/typed_gc_ns | 7271190 |
| after_view_release/final | after_view_release/final/leaf_gc_ns | 86641838 |


</details>


<details><summary>Process 3: ID coverage, endpoints and whole API durations</summary>

Calibration epochs [1]; distinct IDs 512; cross-epoch revisits 2048; revisited distinct IDs 512. Original Go metrics remain unchanged in the summary JSON and full projection.

| Epoch | Distinct IDs | New IDs | Revisited IDs | Cumulative distinct IDs | IDs SHA256 |
| --- | --- | --- | --- | --- | --- |
| 0 | 512 | 512 | 0 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 1 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 2 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 3 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 4 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |


Actual endpoint bytes:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 8388608 | 131289 | 1627950 | 725636 | 699917 | 0 | 0 | 554 | 470 | 11574424 | 10874507 |
| last_pre_held_view_release | maintenance-0 | 4194304 | 193991 | 7658580 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14666672 | 13788387 |
| after_held_view_release | after_view_release | 4194304 | 193991 | 189498 | 691175 | 878285 | 0 | 0 | 1277 | 795 | 6149325 | 5271040 |
| last_epoch_maintenance | maintenance-4 | 4194304 | 445187 | 192373 | 691175 | 180468 | 0 | 0 | 1805 | 1094 | 5706406 | 5525938 |
| reopen | reopen | 4194304 | 445187 | 192373 | 691175 | 12 | 0 | 0 | 1805 | 1094 | 5525950 | 5525938 |


Actual endpoint files:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 1 | 1 | 2 | 1 | 2 | 0 | 0 | 2 | 3 | 12 | 10 |
| last_pre_held_view_release | maintenance-0 | 1 | 1 | 6 | 1 | 3 | 0 | 0 | 8 | 4 | 24 | 21 |
| after_held_view_release | after_view_release | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |
| last_epoch_maintenance | maintenance-4 | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |
| reopen | reopen | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |


Actual bytes Δ from ingest:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| last_pre_held_view_release | maintenance-0 | -4194304 | 62702 | 6030630 | 1011001 | 178368 | 0 | 0 | 3526 | 325 | 3092248 | 2913880 |
| after_held_view_release | after_view_release | -4194304 | 62702 | -1438452 | -34461 | 178368 | 0 | 0 | 723 | 325 | -5425099 | -5603467 |
| last_epoch_maintenance | maintenance-4 | -4194304 | 313898 | -1435577 | -34461 | -519449 | 0 | 0 | 1251 | 624 | -5868018 | -5348569 |
| reopen | reopen | -4194304 | 313898 | -1435577 | -34461 | -699905 | 0 | 0 | 1251 | 624 | -6048474 | -5348569 |


Actual files Δ from ingest:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| last_pre_held_view_release | maintenance-0 | 0 | 0 | 4 | 0 | 1 | 0 | 0 | 6 | 1 | 12 | 11 |
| after_held_view_release | after_view_release | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |
| last_epoch_maintenance | maintenance-4 | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |
| reopen | reopen | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |


| Stage | API group | Whole API duration ns |
| --- | --- | --- |
| epoch-0 | epochs/maintenance_ns | 232724631 |
| epoch-0 | epochs/flush_ns | 11060 |
| epoch-0 | epochs/checkpoint_ns | 6914647 |
| epoch-0 | epochs/before_fold_gc_ns | 1361713 |
| epoch-0 | epochs/fold_ns | 16324568 |
| epoch-0 | epochs/fold_checkpoint_ns | 6640274 |
| epoch-0 | epochs/overlay_ns | 14551 |
| epoch-0 | epochs/overlay_checkpoint_ns | 5430 |
| epoch-0 | epochs/vlog_gc_ns | 10397171 |
| epoch-0 | epochs/vacuum_ns | 36363401 |
| epoch-0/reclaim | epochs/reclaim/plan_ns | 1097790 |
| epoch-0/reclaim | epochs/reclaim/probe_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/rewrite_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/checkpoint_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/gc_ns | 990130 |
| epoch-0 | epochs/exhaustive/plan_ns | 1160282 |
| epoch-0 | epochs/exhaustive/work_ns | 139704270 |
| epoch-0/final | epochs/final/refresh_ns | 10326070 |
| epoch-0/final | epochs/final/typed_gc_ns | 1097541 |
| epoch-0/final | epochs/final/leaf_gc_ns | 315733 |
| epoch-1 | epochs/maintenance_ns | 335056310 |
| epoch-1 | epochs/flush_ns | 10680 |
| epoch-1 | epochs/checkpoint_ns | 11443511 |
| epoch-1 | epochs/before_fold_gc_ns | 630016 |
| epoch-1 | epochs/fold_ns | 14980545 |
| epoch-1 | epochs/fold_checkpoint_ns | 7259950 |
| epoch-1 | epochs/overlay_ns | 12330 |
| epoch-1 | epochs/overlay_checkpoint_ns | 5320 |
| epoch-1 | epochs/vlog_gc_ns | 8401381 |
| epoch-1 | epochs/vacuum_ns | 64003449 |
| epoch-1/reclaim | epochs/reclaim/plan_ns | 1200052 |
| epoch-1/reclaim | epochs/reclaim/probe_ns | 269082 |
| epoch-1/reclaim | epochs/reclaim/rewrite_ns | 8155639 |
| epoch-1/reclaim | epochs/reclaim/checkpoint_ns | 7121759 |
| epoch-1/reclaim | epochs/reclaim/gc_ns | 4267042 |
| epoch-1 | epochs/exhaustive/plan_ns | 988970 |
| epoch-1 | epochs/exhaustive/work_ns | 137991724 |
| epoch-1/final | epochs/final/refresh_ns | 8767835 |
| epoch-1/final | epochs/final/typed_gc_ns | 3606995 |
| epoch-1/final | epochs/final/leaf_gc_ns | 55940030 |
| epoch-2 | epochs/maintenance_ns | 354747779 |
| epoch-2 | epochs/flush_ns | 11110 |
| epoch-2 | epochs/checkpoint_ns | 10599152 |
| epoch-2 | epochs/before_fold_gc_ns | 644626 |
| epoch-2 | epochs/fold_ns | 13972526 |
| epoch-2 | epochs/fold_checkpoint_ns | 7143009 |
| epoch-2 | epochs/overlay_ns | 12760 |
| epoch-2 | epochs/overlay_checkpoint_ns | 5490 |
| epoch-2 | epochs/vlog_gc_ns | 8383021 |
| epoch-2 | epochs/vacuum_ns | 34474824 |
| epoch-2/reclaim | epochs/reclaim/plan_ns | 1198872 |
| epoch-2/reclaim | epochs/reclaim/probe_ns | 269373 |
| epoch-2/reclaim | epochs/reclaim/rewrite_ns | 11597902 |
| epoch-2/reclaim | epochs/reclaim/checkpoint_ns | 7029067 |
| epoch-2/reclaim | epochs/reclaim/gc_ns | 4292882 |
| epoch-2 | epochs/exhaustive/plan_ns | 1095090 |
| epoch-2 | epochs/exhaustive/work_ns | 199487259 |
| epoch-2/final | epochs/final/refresh_ns | 11233928 |
| epoch-2/final | epochs/final/typed_gc_ns | 4516693 |
| epoch-2/final | epochs/final/leaf_gc_ns | 38780195 |
| epoch-3 | epochs/maintenance_ns | 400761786 |
| epoch-3 | epochs/flush_ns | 10320 |
| epoch-3 | epochs/checkpoint_ns | 11334840 |
| epoch-3 | epochs/before_fold_gc_ns | 649706 |
| epoch-3 | epochs/fold_ns | 14651352 |
| epoch-3 | epochs/fold_checkpoint_ns | 6700885 |
| epoch-3 | epochs/overlay_ns | 11840 |
| epoch-3 | epochs/overlay_checkpoint_ns | 5620 |
| epoch-3 | epochs/vlog_gc_ns | 8483552 |
| epoch-3 | epochs/vacuum_ns | 37195309 |
| epoch-3/reclaim | epochs/reclaim/plan_ns | 1225222 |
| epoch-3/reclaim | epochs/reclaim/probe_ns | 260483 |
| epoch-3/reclaim | epochs/reclaim/rewrite_ns | 8423501 |
| epoch-3/reclaim | epochs/reclaim/checkpoint_ns | 6499553 |
| epoch-3/reclaim | epochs/reclaim/gc_ns | 4261051 |
| epoch-3 | epochs/exhaustive/plan_ns | 1111781 |
| epoch-3 | epochs/exhaustive/work_ns | 210556416 |
| epoch-3/final | epochs/final/refresh_ns | 45661272 |
| epoch-3/final | epochs/final/typed_gc_ns | 4557264 |
| epoch-3/final | epochs/final/leaf_gc_ns | 39161819 |
| epoch-4 | epochs/maintenance_ns | 378226056 |
| epoch-4 | epochs/flush_ns | 10630 |
| epoch-4 | epochs/checkpoint_ns | 11122568 |
| epoch-4 | epochs/before_fold_gc_ns | 656637 |
| epoch-4 | epochs/fold_ns | 14697952 |
| epoch-4 | epochs/fold_checkpoint_ns | 22203874 |
| epoch-4 | epochs/overlay_ns | 13480 |
| epoch-4 | epochs/overlay_checkpoint_ns | 5520 |
| epoch-4 | epochs/vlog_gc_ns | 8396951 |
| epoch-4 | epochs/vacuum_ns | 33543464 |
| epoch-4/reclaim | epochs/reclaim/plan_ns | 1219611 |
| epoch-4/reclaim | epochs/reclaim/probe_ns | 267953 |
| epoch-4/reclaim | epochs/reclaim/rewrite_ns | 8783415 |
| epoch-4/reclaim | epochs/reclaim/checkpoint_ns | 6590224 |
| epoch-4/reclaim | epochs/reclaim/gc_ns | 4221171 |
| epoch-4 | epochs/exhaustive/plan_ns | 1069731 |
| epoch-4 | epochs/exhaustive/work_ns | 186235440 |
| epoch-4/final | epochs/final/refresh_ns | 35566413 |
| epoch-4/final | epochs/final/typed_gc_ns | 4444083 |
| epoch-4/final | epochs/final/leaf_gc_ns | 39176939 |
| after_view_release/reclaim | after_view_release/reclaim/plan_ns | 456215 |
| after_view_release/reclaim | after_view_release/reclaim/probe_ns | 340333 |
| after_view_release/reclaim | after_view_release/reclaim/rewrite_ns | 25258624 |
| after_view_release/reclaim | after_view_release/reclaim/checkpoint_ns | 6485423 |
| after_view_release/reclaim | after_view_release/reclaim/gc_ns | 478915 |
| after_view_release/final | after_view_release/final/refresh_ns | 3268782 |
| after_view_release/final | after_view_release/final/typed_gc_ns | 5572454 |
| after_view_release/final | after_view_release/final/leaf_gc_ns | 65809996 |


</details>


<details><summary>Process 4: ID coverage, endpoints and whole API durations</summary>

Calibration epochs [1]; distinct IDs 512; cross-epoch revisits 2048; revisited distinct IDs 512. Original Go metrics remain unchanged in the summary JSON and full projection.

| Epoch | Distinct IDs | New IDs | Revisited IDs | Cumulative distinct IDs | IDs SHA256 |
| --- | --- | --- | --- | --- | --- |
| 0 | 512 | 512 | 0 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 1 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 2 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 3 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 4 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |


Actual endpoint bytes:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 8388608 | 131289 | 1627931 | 725636 | 699917 | 0 | 0 | 554 | 470 | 11574405 | 10874488 |
| last_pre_held_view_release | maintenance-0 | 4194304 | 193991 | 7656225 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14664317 | 13786032 |
| after_held_view_release | after_view_release | 4194304 | 193991 | 189498 | 691175 | 878285 | 0 | 0 | 1277 | 795 | 6149325 | 5271040 |
| last_epoch_maintenance | maintenance-4 | 4194304 | 445187 | 192373 | 691175 | 180468 | 0 | 0 | 1805 | 1094 | 5706406 | 5525938 |
| reopen | reopen | 4194304 | 445187 | 192373 | 691175 | 12 | 0 | 0 | 1805 | 1094 | 5525950 | 5525938 |


Actual endpoint files:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 1 | 1 | 2 | 1 | 2 | 0 | 0 | 2 | 3 | 12 | 10 |
| last_pre_held_view_release | maintenance-0 | 1 | 1 | 6 | 1 | 3 | 0 | 0 | 8 | 4 | 24 | 21 |
| after_held_view_release | after_view_release | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |
| last_epoch_maintenance | maintenance-4 | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |
| reopen | reopen | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |


Actual bytes Δ from ingest:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| last_pre_held_view_release | maintenance-0 | -4194304 | 62702 | 6028294 | 1011001 | 178368 | 0 | 0 | 3526 | 325 | 3089912 | 2911544 |
| after_held_view_release | after_view_release | -4194304 | 62702 | -1438433 | -34461 | 178368 | 0 | 0 | 723 | 325 | -5425080 | -5603448 |
| last_epoch_maintenance | maintenance-4 | -4194304 | 313898 | -1435558 | -34461 | -519449 | 0 | 0 | 1251 | 624 | -5867999 | -5348550 |
| reopen | reopen | -4194304 | 313898 | -1435558 | -34461 | -699905 | 0 | 0 | 1251 | 624 | -6048455 | -5348550 |


Actual files Δ from ingest:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| last_pre_held_view_release | maintenance-0 | 0 | 0 | 4 | 0 | 1 | 0 | 0 | 6 | 1 | 12 | 11 |
| after_held_view_release | after_view_release | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |
| last_epoch_maintenance | maintenance-4 | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |
| reopen | reopen | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |


| Stage | API group | Whole API duration ns |
| --- | --- | --- |
| epoch-0 | epochs/maintenance_ns | 245955628 |
| epoch-0 | epochs/flush_ns | 11130 |
| epoch-0 | epochs/checkpoint_ns | 6274871 |
| epoch-0 | epochs/before_fold_gc_ns | 1422393 |
| epoch-0 | epochs/fold_ns | 16402339 |
| epoch-0 | epochs/fold_checkpoint_ns | 6660024 |
| epoch-0 | epochs/overlay_ns | 14300 |
| epoch-0 | epochs/overlay_checkpoint_ns | 6130 |
| epoch-0 | epochs/vlog_gc_ns | 12272879 |
| epoch-0 | epochs/vacuum_ns | 39746254 |
| epoch-0/reclaim | epochs/reclaim/plan_ns | 1153261 |
| epoch-0/reclaim | epochs/reclaim/probe_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/rewrite_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/checkpoint_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/gc_ns | 1023690 |
| epoch-0 | epochs/exhaustive/plan_ns | 1202781 |
| epoch-0 | epochs/exhaustive/work_ns | 146671449 |
| epoch-0/final | epochs/final/refresh_ns | 11420771 |
| epoch-0/final | epochs/final/typed_gc_ns | 1320542 |
| epoch-0/final | epochs/final/leaf_gc_ns | 352814 |
| epoch-1 | epochs/maintenance_ns | 346069256 |
| epoch-1 | epochs/flush_ns | 11320 |
| epoch-1 | epochs/checkpoint_ns | 11910665 |
| epoch-1 | epochs/before_fold_gc_ns | 654707 |
| epoch-1 | epochs/fold_ns | 14303248 |
| epoch-1 | epochs/fold_checkpoint_ns | 6539013 |
| epoch-1 | epochs/overlay_ns | 13010 |
| epoch-1 | epochs/overlay_checkpoint_ns | 6001 |
| epoch-1 | epochs/vlog_gc_ns | 9006277 |
| epoch-1 | epochs/vacuum_ns | 81824701 |
| epoch-1/reclaim | epochs/reclaim/plan_ns | 1250132 |
| epoch-1/reclaim | epochs/reclaim/probe_ns | 291982 |
| epoch-1/reclaim | epochs/reclaim/rewrite_ns | 7667605 |
| epoch-1/reclaim | epochs/reclaim/checkpoint_ns | 6508922 |
| epoch-1/reclaim | epochs/reclaim/gc_ns | 4351122 |
| epoch-1 | epochs/exhaustive/plan_ns | 1083920 |
| epoch-1 | epochs/exhaustive/work_ns | 135833613 |
| epoch-1/final | epochs/final/refresh_ns | 9934157 |
| epoch-1/final | epochs/final/typed_gc_ns | 4592194 |
| epoch-1/final | epochs/final/leaf_gc_ns | 50286667 |
| epoch-2 | epochs/maintenance_ns | 352628510 |
| epoch-2 | epochs/flush_ns | 11320 |
| epoch-2 | epochs/checkpoint_ns | 11023667 |
| epoch-2 | epochs/before_fold_gc_ns | 671036 |
| epoch-2 | epochs/fold_ns | 15897814 |
| epoch-2 | epochs/fold_checkpoint_ns | 7194859 |
| epoch-2 | epochs/overlay_ns | 12840 |
| epoch-2 | epochs/overlay_checkpoint_ns | 5830 |
| epoch-2 | epochs/vlog_gc_ns | 8491882 |
| epoch-2 | epochs/vacuum_ns | 37320251 |
| epoch-2/reclaim | epochs/reclaim/plan_ns | 1219892 |
| epoch-2/reclaim | epochs/reclaim/probe_ns | 267052 |
| epoch-2/reclaim | epochs/reclaim/rewrite_ns | 8073738 |
| epoch-2/reclaim | epochs/reclaim/checkpoint_ns | 7162649 |
| epoch-2/reclaim | epochs/reclaim/gc_ns | 4287742 |
| epoch-2 | epochs/exhaustive/plan_ns | 1117901 |
| epoch-2 | epochs/exhaustive/work_ns | 194448960 |
| epoch-2/final | epochs/final/refresh_ns | 11302000 |
| epoch-2/final | epochs/final/typed_gc_ns | 4529574 |
| epoch-2/final | epochs/final/leaf_gc_ns | 39589503 |
| epoch-3 | epochs/maintenance_ns | 344532060 |
| epoch-3 | epochs/flush_ns | 10420 |
| epoch-3 | epochs/checkpoint_ns | 10696963 |
| epoch-3 | epochs/before_fold_gc_ns | 660116 |
| epoch-3 | epochs/fold_ns | 13621102 |
| epoch-3 | epochs/fold_checkpoint_ns | 7242720 |
| epoch-3 | epochs/overlay_ns | 12501 |
| epoch-3 | epochs/overlay_checkpoint_ns | 6020 |
| epoch-3 | epochs/vlog_gc_ns | 8691674 |
| epoch-3 | epochs/vacuum_ns | 37268760 |
| epoch-3/reclaim | epochs/reclaim/plan_ns | 1210211 |
| epoch-3/reclaim | epochs/reclaim/probe_ns | 263652 |
| epoch-3/reclaim | epochs/reclaim/rewrite_ns | 8428902 |
| epoch-3/reclaim | epochs/reclaim/checkpoint_ns | 7250970 |
| epoch-3/reclaim | epochs/reclaim/gc_ns | 4223690 |
| epoch-3 | epochs/exhaustive/plan_ns | 1136321 |
| epoch-3 | epochs/exhaustive/work_ns | 188732445 |
| epoch-3/final | epochs/final/refresh_ns | 11157318 |
| epoch-3/final | epochs/final/typed_gc_ns | 4565504 |
| epoch-3/final | epochs/final/leaf_gc_ns | 39352771 |
| epoch-4 | epochs/maintenance_ns | 406781614 |
| epoch-4 | epochs/flush_ns | 11020 |
| epoch-4 | epochs/checkpoint_ns | 11886805 |
| epoch-4 | epochs/before_fold_gc_ns | 667476 |
| epoch-4 | epochs/fold_ns | 16558640 |
| epoch-4 | epochs/fold_checkpoint_ns | 7187640 |
| epoch-4 | epochs/overlay_ns | 12520 |
| epoch-4 | epochs/overlay_checkpoint_ns | 5550 |
| epoch-4 | epochs/vlog_gc_ns | 8384561 |
| epoch-4 | epochs/vacuum_ns | 33669716 |
| epoch-4/reclaim | epochs/reclaim/plan_ns | 1251662 |
| epoch-4/reclaim | epochs/reclaim/probe_ns | 271012 |
| epoch-4/reclaim | epochs/reclaim/rewrite_ns | 9211540 |
| epoch-4/reclaim | epochs/reclaim/checkpoint_ns | 7107288 |
| epoch-4/reclaim | epochs/reclaim/gc_ns | 4282932 |
| epoch-4 | epochs/exhaustive/plan_ns | 1098571 |
| epoch-4 | epochs/exhaustive/work_ns | 190720604 |
| epoch-4/final | epochs/final/refresh_ns | 70426331 |
| epoch-4/final | epochs/final/typed_gc_ns | 4458963 |
| epoch-4/final | epochs/final/leaf_gc_ns | 39568783 |
| after_view_release/reclaim | after_view_release/reclaim/plan_ns | 504525 |
| after_view_release/reclaim | after_view_release/reclaim/probe_ns | 351703 |
| after_view_release/reclaim | after_view_release/reclaim/rewrite_ns | 31391094 |
| after_view_release/reclaim | after_view_release/reclaim/checkpoint_ns | 6678224 |
| after_view_release/reclaim | after_view_release/reclaim/gc_ns | 583435 |
| after_view_release/final | after_view_release/final/refresh_ns | 3274801 |
| after_view_release/final | after_view_release/final/typed_gc_ns | 4943448 |
| after_view_release/final | after_view_release/final/leaf_gc_ns | 66736185 |


</details>


<details><summary>Process 5: ID coverage, endpoints and whole API durations</summary>

Calibration epochs [1]; distinct IDs 512; cross-epoch revisits 2048; revisited distinct IDs 512. Original Go metrics remain unchanged in the summary JSON and full projection.

| Epoch | Distinct IDs | New IDs | Revisited IDs | Cumulative distinct IDs | IDs SHA256 |
| --- | --- | --- | --- | --- | --- |
| 0 | 512 | 512 | 0 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 1 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 2 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 3 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |
| 4 | 512 | 0 | 512 | 512 | 6510d82549b4aacbad0b118fc41d1204a6a254476dbc42eb5514df2763d7d2ef |


Actual endpoint bytes:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 8388608 | 131289 | 1627902 | 725636 | 699917 | 0 | 0 | 554 | 470 | 11574376 | 10874459 |
| last_pre_held_view_release | maintenance-0 | 4194304 | 193991 | 7658346 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14666438 | 13788153 |
| after_held_view_release | after_view_release | 4194304 | 193991 | 189498 | 691175 | 878285 | 0 | 0 | 1277 | 795 | 6149325 | 5271040 |
| last_epoch_maintenance | maintenance-4 | 4194304 | 445187 | 192373 | 691175 | 180468 | 0 | 0 | 1805 | 1094 | 5706406 | 5525938 |
| reopen | reopen | 4194304 | 445187 | 192373 | 691175 | 12 | 0 | 0 | 1805 | 1094 | 5525950 | 5525938 |


Actual endpoint files:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 1 | 1 | 2 | 1 | 2 | 0 | 0 | 2 | 3 | 12 | 10 |
| last_pre_held_view_release | maintenance-0 | 1 | 1 | 6 | 1 | 3 | 0 | 0 | 8 | 4 | 24 | 21 |
| after_held_view_release | after_view_release | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |
| last_epoch_maintenance | maintenance-4 | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |
| reopen | reopen | 1 | 1 | 4 | 1 | 3 | 0 | 0 | 2 | 4 | 16 | 13 |


Actual bytes Δ from ingest:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| last_pre_held_view_release | maintenance-0 | -4194304 | 62702 | 6030444 | 1011001 | 178368 | 0 | 0 | 3526 | 325 | 3092062 | 2913694 |
| after_held_view_release | after_view_release | -4194304 | 62702 | -1438404 | -34461 | 178368 | 0 | 0 | 723 | 325 | -5425051 | -5603419 |
| last_epoch_maintenance | maintenance-4 | -4194304 | 313898 | -1435529 | -34461 | -519449 | 0 | 0 | 1251 | 624 | -5867970 | -5348521 |
| reopen | reopen | -4194304 | 313898 | -1435529 | -34461 | -699905 | 0 | 0 | 1251 | 624 | -6048426 | -5348521 |


Actual files Δ from ingest:

| Endpoint | Actual phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | all | persistent_WAL_excluded |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | ingest | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| last_pre_held_view_release | maintenance-0 | 0 | 0 | 4 | 0 | 1 | 0 | 0 | 6 | 1 | 12 | 11 |
| after_held_view_release | after_view_release | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |
| last_epoch_maintenance | maintenance-4 | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |
| reopen | reopen | 0 | 0 | 2 | 0 | 1 | 0 | 0 | 0 | 1 | 4 | 3 |


| Stage | API group | Whole API duration ns |
| --- | --- | --- |
| epoch-0 | epochs/maintenance_ns | 245568375 |
| epoch-0 | epochs/flush_ns | 10630 |
| epoch-0 | epochs/checkpoint_ns | 6785845 |
| epoch-0 | epochs/before_fold_gc_ns | 1337063 |
| epoch-0 | epochs/fold_ns | 16040175 |
| epoch-0 | epochs/fold_checkpoint_ns | 6611324 |
| epoch-0 | epochs/overlay_ns | 14160 |
| epoch-0 | epochs/overlay_checkpoint_ns | 5280 |
| epoch-0 | epochs/vlog_gc_ns | 13946475 |
| epoch-0 | epochs/vacuum_ns | 38744885 |
| epoch-0/reclaim | epochs/reclaim/plan_ns | 1102450 |
| epoch-0/reclaim | epochs/reclaim/probe_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/rewrite_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/checkpoint_ns | 0 |
| epoch-0/reclaim | epochs/reclaim/gc_ns | 978280 |
| epoch-0 | epochs/exhaustive/plan_ns | 1112191 |
| epoch-0 | epochs/exhaustive/work_ns | 145057983 |
| epoch-0/final | epochs/final/refresh_ns | 12339969 |
| epoch-0/final | epochs/final/typed_gc_ns | 1116271 |
| epoch-0/final | epochs/final/leaf_gc_ns | 365394 |
| epoch-1 | epochs/maintenance_ns | 300928798 |
| epoch-1 | epochs/flush_ns | 10500 |
| epoch-1 | epochs/checkpoint_ns | 11778554 |
| epoch-1 | epochs/before_fold_gc_ns | 610506 |
| epoch-1 | epochs/fold_ns | 13854904 |
| epoch-1 | epochs/fold_checkpoint_ns | 7173160 |
| epoch-1 | epochs/overlay_ns | 12090 |
| epoch-1 | epochs/overlay_checkpoint_ns | 4670 |
| epoch-1 | epochs/vlog_gc_ns | 8292250 |
| epoch-1 | epochs/vacuum_ns | 36768735 |
| epoch-1/reclaim | epochs/reclaim/plan_ns | 1183331 |
| epoch-1/reclaim | epochs/reclaim/probe_ns | 258653 |
| epoch-1/reclaim | epochs/reclaim/rewrite_ns | 8586892 |
| epoch-1/reclaim | epochs/reclaim/checkpoint_ns | 7081479 |
| epoch-1/reclaim | epochs/reclaim/gc_ns | 4177760 |
| epoch-1 | epochs/exhaustive/plan_ns | 875459 |
| epoch-1 | epochs/exhaustive/work_ns | 133668031 |
| epoch-1/final | epochs/final/refresh_ns | 12042906 |
| epoch-1/final | epochs/final/typed_gc_ns | 4470444 |
| epoch-1/final | epochs/final/leaf_gc_ns | 50078474 |
| epoch-2 | epochs/maintenance_ns | 362459175 |
| epoch-2 | epochs/flush_ns | 8640 |
| epoch-2 | epochs/checkpoint_ns | 11871085 |
| epoch-2 | epochs/before_fold_gc_ns | 616866 |
| epoch-2 | epochs/fold_ns | 14458880 |
| epoch-2 | epochs/fold_checkpoint_ns | 7207440 |
| epoch-2 | epochs/overlay_ns | 10321 |
| epoch-2 | epochs/overlay_checkpoint_ns | 4830 |
| epoch-2 | epochs/vlog_gc_ns | 8293890 |
| epoch-2 | epochs/vacuum_ns | 38322471 |
| epoch-2/reclaim | epochs/reclaim/plan_ns | 1139621 |
| epoch-2/reclaim | epochs/reclaim/probe_ns | 263082 |
| epoch-2/reclaim | epochs/reclaim/rewrite_ns | 8063428 |
| epoch-2/reclaim | epochs/reclaim/checkpoint_ns | 7038418 |
| epoch-2/reclaim | epochs/reclaim/gc_ns | 4267221 |
| epoch-2 | epochs/exhaustive/plan_ns | 958289 |
| epoch-2 | epochs/exhaustive/work_ns | 203767910 |
| epoch-2/final | epochs/final/refresh_ns | 12827503 |
| epoch-2/final | epochs/final/typed_gc_ns | 4472534 |
| epoch-2/final | epochs/final/leaf_gc_ns | 38866746 |
| epoch-3 | epochs/maintenance_ns | 343459031 |
| epoch-3 | epochs/flush_ns | 9150 |
| epoch-3 | epochs/checkpoint_ns | 10737003 |
| epoch-3 | epochs/before_fold_gc_ns | 590996 |
| epoch-3 | epochs/fold_ns | 13844943 |
| epoch-3 | epochs/fold_checkpoint_ns | 7100059 |
| epoch-3 | epochs/overlay_ns | 9991 |
| epoch-3 | epochs/overlay_checkpoint_ns | 4220 |
| epoch-3 | epochs/vlog_gc_ns | 8296030 |
| epoch-3 | epochs/vacuum_ns | 36397022 |
| epoch-3/reclaim | epochs/reclaim/plan_ns | 1149301 |
| epoch-3/reclaim | epochs/reclaim/probe_ns | 283133 |
| epoch-3/reclaim | epochs/reclaim/rewrite_ns | 8962406 |
| epoch-3/reclaim | epochs/reclaim/checkpoint_ns | 7001278 |
| epoch-3/reclaim | epochs/reclaim/gc_ns | 4273641 |
| epoch-3 | epochs/exhaustive/plan_ns | 990839 |
| epoch-3 | epochs/exhaustive/work_ns | 190132240 |
| epoch-3/final | epochs/final/refresh_ns | 9649213 |
| epoch-3/final | epochs/final/typed_gc_ns | 4755026 |
| epoch-3/final | epochs/final/leaf_gc_ns | 39272540 |
| epoch-4 | epochs/maintenance_ns | 347802723 |
| epoch-4 | epochs/flush_ns | 10561 |
| epoch-4 | epochs/checkpoint_ns | 11759163 |
| epoch-4 | epochs/before_fold_gc_ns | 609146 |
| epoch-4 | epochs/fold_ns | 15051256 |
| epoch-4 | epochs/fold_checkpoint_ns | 7125598 |
| epoch-4 | epochs/overlay_ns | 12370 |
| epoch-4 | epochs/overlay_checkpoint_ns | 5510 |
| epoch-4 | epochs/vlog_gc_ns | 8406912 |
| epoch-4 | epochs/vacuum_ns | 47619760 |
| epoch-4/reclaim | epochs/reclaim/plan_ns | 1181761 |
| epoch-4/reclaim | epochs/reclaim/probe_ns | 269103 |
| epoch-4/reclaim | epochs/reclaim/rewrite_ns | 12021306 |
| epoch-4/reclaim | epochs/reclaim/checkpoint_ns | 6969667 |
| epoch-4/reclaim | epochs/reclaim/gc_ns | 4225971 |
| epoch-4 | epochs/exhaustive/plan_ns | 978890 |
| epoch-4 | epochs/exhaustive/work_ns | 178158302 |
| epoch-4/final | epochs/final/refresh_ns | 9832445 |
| epoch-4/final | epochs/final/typed_gc_ns | 4496434 |
| epoch-4/final | epochs/final/leaf_gc_ns | 39068568 |
| after_view_release/reclaim | after_view_release/reclaim/plan_ns | 453454 |
| after_view_release/reclaim | after_view_release/reclaim/probe_ns | 349853 |
| after_view_release/reclaim | after_view_release/reclaim/rewrite_ns | 27627278 |
| after_view_release/reclaim | after_view_release/reclaim/checkpoint_ns | 6491092 |
| after_view_release/reclaim | after_view_release/reclaim/gc_ns | 593156 |
| after_view_release/final | after_view_release/final/refresh_ns | 18583740 |
| after_view_release/final | after_view_release/final/typed_gc_ns | 4507013 |
| after_view_release/final | after_view_release/final/leaf_gc_ns | 66672104 |


</details>

Full original calibration/final values, complete epoch/phase component and file census, source/host observations, protection/eligibility/lifetime statistics, exhaustive audit/debt and both fallback slots/root IDs/LSNs remain in the linked projection and trajectory report. This adapter makes arithmetic/presentation checks only; independent frozen-validator replay and the coordinator own qualification.

