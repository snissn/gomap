# Finite direct-backend lifecycle observations

**Descriptive formatting only. This command does not qualify or accept evidence.**

[Original packet](../../../../docs/evidence/r1-row-store-5061/_artifacts/lifecycle/final-actual-M/3325dfe/packet.json) · [Independent validation log](../../../../docs/evidence/r1-row-store-5061/_artifacts/final-M-3325dfe/receipts/D/independent-validation.log) · [Full raw-value projection](../../../../docs/evidence/r1-row-store-5061/_artifacts/final-M-3325dfe/results/D/descriptive-projection.json)

Packet SHA256 `b8705425f0d3b0c8344ff9cab4d33e86fb964ace3da8fab7afb8d63b796c26dc`. Producer label `retained` is copied, not endorsed.
Captured source `3325dfe77940fec8587d8b61b1ac4e0b2f72caca`; runtime `eab40aaf77ed307d92a7a38adf0dd80be07a7e12bdda8a2ec3118baf9cad33ff`; harness `325d5b41579a492a430da6cbb6162363ff82b42bb6553eddf2e325584fab4c13`.
5 fresh processes; 5 final epochs/process; 4096 live rows; 1024 calls/epoch.
Working set repeats 512 IDs/epoch. Recorded working-set policy: `repeat-stride-37-each-epoch`.

The final benchmark results below exclude the Go calibration result. All emitted calibration and final JSON values, full file census, phase statistics, eligibility, debt, owner/options, and process observations are preserved in the linked projection and original logs. The projection keeps nested counters separate and changes no original packet fields.

Direct `OptionsFor(ProfileCommandWALDurable)+OpenBackend`; command-WAL durable; background prune disabled. This finite hot-set fixture excludes the A public cached-wrapper overhead. Sampled heap is not RSS or an unsampled peak. Retained heap after GC includes live fixture/oracle maps and latency samples.

Whole-call timers include encoding, full-row decode/oracle and callback bookkeeping. Epoch metrics additionally include preparation and latency/count/visited-ID tracking. Maintenance and phase oracles are excluded. Go allocation differences observe the process during the loop, including concurrent background activity; they are not exclusively attributed to user operations. Operation counts do not allocate mixed-call time to individual operations. No speed comparison, growth ratio, unlimited bound, or acceptance threshold is supplied.

| Process | PID | Raw log | Total mixed calls | Mixed call ns | Go bytes (loop) | Go objects (loop) | Mixed p95 ns/call | Mixed p99 ns/call | Sampled heap high B | Post-GC retained heap B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 3201903 | [log](../../../../docs/evidence/r1-row-store-5061/_artifacts/lifecycle/final-actual-M/3325dfe/run-001.log) | 5120 | 34027368417 | 4156891224 | 30661841 | 13570921 | 18148105 | 175874240 | 79683296 |
| 2 | 3203911 | [log](../../../../docs/evidence/r1-row-store-5061/_artifacts/lifecycle/final-actual-M/3325dfe/run-002.log) | 5120 | 33980563082 | 4147925592 | 30658944 | 13402570 | 19875402 | 168690776 | 80641256 |
| 3 | 3205743 | [log](../../../../docs/evidence/r1-row-store-5061/_artifacts/lifecycle/final-actual-M/3325dfe/run-003.log) | 5120 | 35216603437 | 4151790272 | 30661973 | 14914685 | 21275046 | 179945128 | 80552992 |
| 4 | 3207576 | [log](../../../../docs/evidence/r1-row-store-5061/_artifacts/lifecycle/final-actual-M/3325dfe/run-004.log) | 5120 | 34635815824 | 4146876752 | 30662651 | 13739153 | 17694991 | 177060000 | 79678448 |
| 5 | 3209881 | [log](../../../../docs/evidence/r1-row-store-5061/_artifacts/lifecycle/final-actual-M/3325dfe/run-005.log) | 5120 | 34481012342 | 4156609336 | 30662292 | 13617892 | 17620530 | 175546608 | 79684480 |

Cross-process phase medians follow. Component sizes are logical file lengths; redo WAL remains separate. All differences are computed within each process before taking medians. An absent predecessor is shown as —.

Logical component bytes:

| Phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | 8388608 | 131289 | 1627931 | 725636 | 699917 | 0 | 0 | 554 | 470 |
| churn-0 | 88080384 | 193991 | 7307444 | 1045462 | 878285 | 0 | 0 | 554 | 470 |
| checkpoint-0 | 88080384 | 193991 | 7307444 | 1045462 | 878285 | 0 | 0 | 554 | 470 |
| folded-0 | 88080384 | 193991 | 7405022 | 1736637 | 878285 | 0 | 0 | 554 | 470 |
| before_vacuum-0 | 88080384 | 193991 | 7405022 | 1736637 | 878285 | 0 | 0 | 554 | 795 |
| before_exhaustive-0 | 4194304 | 193991 | 7407103 | 1736637 | 878285 | 0 | 0 | 855 | 795 |
| before_final_gc-0 | 4194304 | 193991 | 7657303 | 1736637 | 878285 | 0 | 0 | 4080 | 795 |
| maintenance-0 | 4194304 | 193991 | 7657303 | 1736637 | 878285 | 0 | 0 | 4080 | 795 |
| after_view_release | 4194304 | 193991 | 189498 | 691175 | 878285 | 0 | 0 | 1277 | 795 |
| churn-1 | 8388608 | 256821 | 5064391 | 1012959 | 1058739 | 0 | 0 | 1277 | 795 |
| checkpoint-1 | 8388608 | 256821 | 5064391 | 1012959 | 180466 | 0 | 0 | 1277 | 795 |
| folded-1 | 8388608 | 256821 | 5172888 | 1704134 | 180466 | 0 | 0 | 1277 | 795 |
| before_vacuum-1 | 8388608 | 256821 | 5174367 | 1704134 | 180466 | 0 | 0 | 1277 | 946 |
| before_exhaustive-1 | 4194304 | 256821 | 5176422 | 1704134 | 180466 | 0 | 0 | 1794 | 946 |
| before_final_gc-1 | 4194304 | 256821 | 5408176 | 1704134 | 180466 | 0 | 0 | 4398 | 946 |
| maintenance-1 | 4194304 | 256821 | 190671 | 691175 | 180466 | 0 | 0 | 1540 | 946 |
| churn-2 | 8388608 | 319524 | 5094244 | 1012959 | 360793 | 0 | 0 | 2276 | 946 |
| checkpoint-2 | 8388608 | 319524 | 5094244 | 1012959 | 180339 | 0 | 0 | 2276 | 946 |
| folded-2 | 8388608 | 319524 | 5201306 | 1704134 | 180339 | 0 | 0 | 2276 | 946 |
| before_vacuum-2 | 8388608 | 319524 | 5202783 | 1704134 | 180339 | 0 | 0 | 2276 | 1092 |
| before_exhaustive-2 | 4194304 | 319524 | 5204838 | 1704134 | 180339 | 0 | 0 | 3012 | 1092 |
| before_final_gc-2 | 4194304 | 319524 | 5435056 | 1704134 | 180339 | 0 | 0 | 2296 | 1094 |
| maintenance-2 | 4194304 | 319524 | 192178 | 691175 | 180339 | 0 | 0 | 1796 | 1094 |
| churn-3 | 8388608 | 382355 | 5053348 | 1012959 | 360794 | 0 | 0 | 2535 | 1094 |
| checkpoint-3 | 8388608 | 382355 | 5053348 | 1012959 | 180467 | 0 | 0 | 2535 | 1094 |
| folded-3 | 8388608 | 382355 | 5159735 | 1704134 | 180467 | 0 | 0 | 2535 | 1094 |
| before_vacuum-3 | 8388608 | 382355 | 5161214 | 1704134 | 180467 | 0 | 0 | 2535 | 1092 |
| before_exhaustive-3 | 4194304 | 382355 | 5163269 | 1704134 | 180467 | 0 | 0 | 3274 | 1092 |
| before_final_gc-3 | 4194304 | 382355 | 5393138 | 1704134 | 180467 | 0 | 0 | 2304 | 1094 |
| maintenance-3 | 4194304 | 382355 | 192148 | 691175 | 180467 | 0 | 0 | 1803 | 1094 |
| churn-4 | 8388608 | 445187 | 5018985 | 1012959 | 360923 | 0 | 0 | 2544 | 1094 |
| checkpoint-4 | 8388608 | 445187 | 5018985 | 1012959 | 180468 | 0 | 0 | 2544 | 1094 |
| folded-4 | 8388608 | 445187 | 5126866 | 1704134 | 180468 | 0 | 0 | 2544 | 1094 |
| before_vacuum-4 | 8388608 | 445187 | 5128344 | 1704134 | 180468 | 0 | 0 | 2544 | 1094 |
| before_exhaustive-4 | 4194304 | 445187 | 5130398 | 1704134 | 180468 | 0 | 0 | 3285 | 1094 |
| before_final_gc-4 | 4194304 | 445187 | 5360488 | 1704134 | 180468 | 0 | 0 | 2307 | 1094 |
| maintenance-4 | 4194304 | 445187 | 192373 | 691175 | 180468 | 0 | 0 | 1805 | 1094 |
| reopen | 4194304 | 445187 | 192373 | 691175 | 12 | 0 | 0 | 1805 | 1094 |


Adjacent-phase component byte differences:

| Phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | — | — | — | — | — | — | — | — | — |
| churn-0 | 79691776 | 62702 | 5679494 | 319826 | 178368 | 0 | 0 | 0 | 0 |
| checkpoint-0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| folded-0 | 0 | 0 | 97652 | 691175 | 0 | 0 | 0 | 0 | 0 |
| before_vacuum-0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 325 |
| before_exhaustive-0 | -83886080 | 0 | 2081 | 0 | 0 | 0 | 0 | 301 | 0 |
| before_final_gc-0 | 0 | 0 | 250200 | 0 | 0 | 0 | 0 | 3225 | 0 |
| maintenance-0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| after_view_release | 0 | 0 | -7467805 | -1045462 | 0 | 0 | 0 | -2803 | 0 |
| churn-1 | 4194304 | 62830 | 4874893 | 321784 | 180454 | 0 | 0 | 0 | 0 |
| checkpoint-1 | 0 | 0 | 0 | 0 | -878273 | 0 | 0 | 0 | 0 |
| folded-1 | 0 | 0 | 108590 | 691175 | 0 | 0 | 0 | 0 | 0 |
| before_vacuum-1 | 0 | 0 | 1479 | 0 | 0 | 0 | 0 | 0 | 151 |
| before_exhaustive-1 | -4194304 | 0 | 2055 | 0 | 0 | 0 | 0 | 517 | 0 |
| before_final_gc-1 | 0 | 0 | 231754 | 0 | 0 | 0 | 0 | 2604 | 0 |
| maintenance-1 | 0 | 0 | -5217505 | -1012959 | 0 | 0 | 0 | -2858 | 0 |
| churn-2 | 4194304 | 62703 | 4903573 | 321784 | 180327 | 0 | 0 | 736 | 0 |
| checkpoint-2 | 0 | 0 | 0 | 0 | -180454 | 0 | 0 | 0 | 0 |
| folded-2 | 0 | 0 | 108354 | 691175 | 0 | 0 | 0 | 0 | 0 |
| before_vacuum-2 | 0 | 0 | 1477 | 0 | 0 | 0 | 0 | 0 | 146 |
| before_exhaustive-2 | -4194304 | 0 | 2055 | 0 | 0 | 0 | 0 | 736 | 0 |
| before_final_gc-2 | 0 | 0 | 230218 | 0 | 0 | 0 | 0 | -716 | 2 |
| maintenance-2 | 0 | 0 | -5242878 | -1012959 | 0 | 0 | 0 | -500 | 0 |
| churn-3 | 4194304 | 62831 | 4861170 | 321784 | 180455 | 0 | 0 | 739 | 0 |
| checkpoint-3 | 0 | 0 | 0 | 0 | -180327 | 0 | 0 | 0 | 0 |
| folded-3 | 0 | 0 | 107472 | 691175 | 0 | 0 | 0 | 0 | 0 |
| before_vacuum-3 | 0 | 0 | 1479 | 0 | 0 | 0 | 0 | 0 | -2 |
| before_exhaustive-3 | -4194304 | 0 | 2055 | 0 | 0 | 0 | 0 | 739 | 0 |
| before_final_gc-3 | 0 | 0 | 229869 | 0 | 0 | 0 | 0 | -970 | 2 |
| maintenance-3 | 0 | 0 | -5200990 | -1012959 | 0 | 0 | 0 | -501 | 0 |
| churn-4 | 4194304 | 62832 | 4826837 | 321784 | 180456 | 0 | 0 | 741 | 0 |
| checkpoint-4 | 0 | 0 | 0 | 0 | -180455 | 0 | 0 | 0 | 0 |
| folded-4 | 0 | 0 | 107872 | 691175 | 0 | 0 | 0 | 0 | 0 |
| before_vacuum-4 | 0 | 0 | 1478 | 0 | 0 | 0 | 0 | 0 | 0 |
| before_exhaustive-4 | -4194304 | 0 | 2054 | 0 | 0 | 0 | 0 | 741 | 0 |
| before_final_gc-4 | 0 | 0 | 230090 | 0 | 0 | 0 | 0 | -978 | 0 |
| maintenance-4 | 0 | 0 | -5168115 | -1012959 | 0 | 0 | 0 | -502 | 0 |
| reopen | 0 | 0 | 0 | 0 | -180456 | 0 | 0 | 0 | 0 |


Component byte growth since that process's ingest:

| Phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| churn-0 | 79691776 | 62702 | 5679494 | 319826 | 178368 | 0 | 0 | 0 | 0 |
| checkpoint-0 | 79691776 | 62702 | 5679494 | 319826 | 178368 | 0 | 0 | 0 | 0 |
| folded-0 | 79691776 | 62702 | 5777112 | 1011001 | 178368 | 0 | 0 | 0 | 0 |
| before_vacuum-0 | 79691776 | 62702 | 5777112 | 1011001 | 178368 | 0 | 0 | 0 | 325 |
| before_exhaustive-0 | -4194304 | 62702 | 5779193 | 1011001 | 178368 | 0 | 0 | 301 | 325 |
| before_final_gc-0 | -4194304 | 62702 | 6029393 | 1011001 | 178368 | 0 | 0 | 3526 | 325 |
| maintenance-0 | -4194304 | 62702 | 6029393 | 1011001 | 178368 | 0 | 0 | 3526 | 325 |
| after_view_release | -4194304 | 62702 | -1438433 | -34461 | 178368 | 0 | 0 | 723 | 325 |
| churn-1 | 0 | 125532 | 3436441 | 287323 | 358822 | 0 | 0 | 723 | 325 |
| checkpoint-1 | 0 | 125532 | 3436441 | 287323 | -519451 | 0 | 0 | 723 | 325 |
| folded-1 | 0 | 125532 | 3544957 | 978498 | -519451 | 0 | 0 | 723 | 325 |
| before_vacuum-1 | 0 | 125532 | 3546436 | 978498 | -519451 | 0 | 0 | 723 | 476 |
| before_exhaustive-1 | -4194304 | 125532 | 3548491 | 978498 | -519451 | 0 | 0 | 1240 | 476 |
| before_final_gc-1 | -4194304 | 125532 | 3780245 | 978498 | -519451 | 0 | 0 | 3844 | 476 |
| maintenance-1 | -4194304 | 125532 | -1437260 | -34461 | -519451 | 0 | 0 | 986 | 476 |
| churn-2 | 0 | 188235 | 3466294 | 287323 | -339124 | 0 | 0 | 1722 | 476 |
| checkpoint-2 | 0 | 188235 | 3466294 | 287323 | -519578 | 0 | 0 | 1722 | 476 |
| folded-2 | 0 | 188235 | 3573375 | 978498 | -519578 | 0 | 0 | 1722 | 476 |
| before_vacuum-2 | 0 | 188235 | 3574852 | 978498 | -519578 | 0 | 0 | 1722 | 622 |
| before_exhaustive-2 | -4194304 | 188235 | 3576907 | 978498 | -519578 | 0 | 0 | 2458 | 622 |
| before_final_gc-2 | -4194304 | 188235 | 3807125 | 978498 | -519578 | 0 | 0 | 1742 | 624 |
| maintenance-2 | -4194304 | 188235 | -1435753 | -34461 | -519578 | 0 | 0 | 1242 | 624 |
| churn-3 | 0 | 251066 | 3425417 | 287323 | -339123 | 0 | 0 | 1981 | 624 |
| checkpoint-3 | 0 | 251066 | 3425417 | 287323 | -519450 | 0 | 0 | 1981 | 624 |
| folded-3 | 0 | 251066 | 3531785 | 978498 | -519450 | 0 | 0 | 1981 | 624 |
| before_vacuum-3 | 0 | 251066 | 3533264 | 978498 | -519450 | 0 | 0 | 1981 | 622 |
| before_exhaustive-3 | -4194304 | 251066 | 3535319 | 978498 | -519450 | 0 | 0 | 2720 | 622 |
| before_final_gc-3 | -4194304 | 251066 | 3765188 | 978498 | -519450 | 0 | 0 | 1750 | 624 |
| maintenance-3 | -4194304 | 251066 | -1435783 | -34461 | -519450 | 0 | 0 | 1249 | 624 |
| churn-4 | 0 | 313898 | 3391035 | 287323 | -338994 | 0 | 0 | 1990 | 624 |
| checkpoint-4 | 0 | 313898 | 3391035 | 287323 | -519449 | 0 | 0 | 1990 | 624 |
| folded-4 | 0 | 313898 | 3498956 | 978498 | -519449 | 0 | 0 | 1990 | 624 |
| before_vacuum-4 | 0 | 313898 | 3500434 | 978498 | -519449 | 0 | 0 | 1990 | 624 |
| before_exhaustive-4 | -4194304 | 313898 | 3502488 | 978498 | -519449 | 0 | 0 | 2731 | 624 |
| before_final_gc-4 | -4194304 | 313898 | 3732578 | 978498 | -519449 | 0 | 0 | 1753 | 624 |
| maintenance-4 | -4194304 | 313898 | -1435558 | -34461 | -519449 | 0 | 0 | 1251 | 624 |
| reopen | -4194304 | 313898 | -1435558 | -34461 | -699905 | 0 | 0 | 1251 | 624 |


| Phase | Median total bytes | Median regular files | Median adjacent total byte Δ | Median total byte growth since ingest |
| --- | --- | --- | --- | --- |
| ingest | 11574405 | 12 | — | 0 |
| churn-0 | 97506590 | 12 | 85932166 | 85932166 |
| checkpoint-0 | 97506590 | 13 | 0 | 85932166 |
| folded-0 | 98295343 | 13 | 788827 | 86720959 |
| before_vacuum-0 | 98295668 | 14 | 325 | 86721284 |
| before_exhaustive-0 | 14411970 | 15 | -83883698 | 2837586 |
| before_final_gc-0 | 14665395 | 24 | 253425 | 3091011 |
| maintenance-0 | 14665395 | 24 | 0 | 3091011 |
| after_view_release | 6149325 | 16 | -8516070 | -5425080 |
| churn-1 | 15783590 | 17 | 9634265 | 4209166 |
| checkpoint-1 | 14905317 | 17 | -878273 | 3330893 |
| folded-1 | 15704989 | 17 | 799765 | 4130584 |
| before_vacuum-1 | 15706619 | 17 | 1630 | 4132214 |
| before_exhaustive-1 | 11514887 | 18 | -4191732 | -59518 |
| before_final_gc-1 | 11749245 | 24 | 234358 | 174840 |
| maintenance-1 | 5515923 | 16 | -6233322 | -6058482 |
| churn-2 | 15179350 | 20 | 9663427 | 3604926 |
| checkpoint-2 | 14998896 | 20 | -180454 | 3424472 |
| folded-2 | 15797133 | 20 | 799529 | 4222728 |
| before_vacuum-2 | 15798756 | 20 | 1623 | 4224351 |
| before_exhaustive-2 | 11607243 | 21 | -4191513 | 32838 |
| before_final_gc-2 | 11836747 | 21 | 229504 | 262342 |
| maintenance-2 | 5580410 | 16 | -6256337 | -5993995 |
| churn-3 | 15201693 | 20 | 9621283 | 3627288 |
| checkpoint-3 | 15021366 | 20 | -180327 | 3446961 |
| folded-3 | 15818928 | 20 | 798647 | 4244504 |
| before_vacuum-3 | 15820405 | 20 | 1477 | 4245981 |
| before_exhaustive-3 | 11628895 | 21 | -4191510 | 54471 |
| before_final_gc-3 | 11857796 | 21 | 228901 | 283372 |
| maintenance-3 | 5643346 | 16 | -6214450 | -5931059 |
| churn-4 | 15230300 | 20 | 9586954 | 3655876 |
| checkpoint-4 | 15049845 | 20 | -180455 | 3475421 |
| folded-4 | 15848901 | 20 | 799047 | 4274517 |
| before_vacuum-4 | 15850378 | 20 | 1478 | 4275994 |
| before_exhaustive-4 | 11658869 | 21 | -4191509 | 84485 |
| before_final_gc-4 | 11887982 | 21 | 229112 | 313598 |
| maintenance-4 | 5706406 | 16 | -6181576 | -5867999 |
| reopen | 5525950 | 16 | -180456 | -6048455 |


<details><summary>Process 1: complete phase trajectory and maintenance</summary>

All tables in this block are this process's actual cells. Heap cuts are census-boundary samples.

| Phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | Total B | Files | Adjacent total ΔB | Since ingest ΔB | HeapAlloc B | HeapInuse B | HeapObjects | NumGC |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | 8388608 | 131289 | 1627910 | 725636 | 699917 | 0 | 0 | 554 | 470 | 11574384 | 12 | — | 0 | 110563784 | 116629504 | 1011626 | 38 |
| churn-0 | 88080384 | 193991 | 7307370 | 1045462 | 878285 | 0 | 0 | 554 | 470 | 97506516 | 12 | 85932132 | 85932132 | 142366400 | 150659072 | 1162615 | 53 |
| checkpoint-0 | 88080384 | 193991 | 7307370 | 1045462 | 878285 | 0 | 0 | 554 | 470 | 97506516 | 13 | 0 | 85932132 | 126453464 | 135503872 | 865013 | 54 |
| folded-0 | 88080384 | 193991 | 7405022 | 1736637 | 878285 | 0 | 0 | 554 | 470 | 98295343 | 13 | 788827 | 86720959 | 123380176 | 131162112 | 782036 | 55 |
| before_vacuum-0 | 88080384 | 193991 | 7405022 | 1736637 | 878285 | 0 | 0 | 554 | 795 | 98295668 | 14 | 325 | 86721284 | 99178112 | 109527040 | 421460 | 56 |
| before_exhaustive-0 | 4194304 | 193991 | 7407103 | 1736637 | 878285 | 0 | 0 | 855 | 795 | 14411970 | 15 | -83883698 | 2837586 | 163229552 | 169828352 | 1501406 | 56 |
| before_final_gc-0 | 4194304 | 193991 | 7657303 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14665395 | 24 | 253425 | 3091011 | 153744520 | 159621120 | 1185042 | 57 |
| maintenance-0 | 4194304 | 193991 | 7657303 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14665395 | 24 | 0 | 3091011 | 136080328 | 143114240 | 1037214 | 58 |
| after_view_release | 4194304 | 193991 | 189498 | 691175 | 878285 | 0 | 0 | 1277 | 795 | 6149325 | 16 | -8516070 | -5425059 | 114742952 | 122109952 | 638537 | 59 |
| churn-1 | 8388608 | 256821 | 5064348 | 1012959 | 1058739 | 0 | 0 | 1277 | 795 | 15783547 | 17 | 9634222 | 4209163 | 164389480 | 168771584 | 928699 | 69 |
| checkpoint-1 | 8388608 | 256821 | 5064348 | 1012959 | 180466 | 0 | 0 | 1277 | 795 | 14905274 | 17 | -878273 | 3330890 | 113609896 | 125992960 | 517295 | 70 |
| folded-1 | 8388608 | 256821 | 5172420 | 1704134 | 180466 | 0 | 0 | 1277 | 795 | 15704521 | 17 | 799247 | 4130137 | 161686496 | 168861696 | 1084292 | 70 |
| before_vacuum-1 | 8388608 | 256821 | 5173899 | 1704134 | 180466 | 0 | 0 | 1277 | 946 | 15706151 | 17 | 1630 | 4131767 | 104859536 | 115982336 | 361390 | 71 |
| before_exhaustive-1 | 4194304 | 256821 | 5175954 | 1704134 | 180466 | 0 | 0 | 1794 | 946 | 11514419 | 18 | -4191732 | -59965 | 136227488 | 144138240 | 897694 | 71 |
| before_final_gc-1 | 4194304 | 256821 | 5407708 | 1704134 | 180466 | 0 | 0 | 4398 | 946 | 11748777 | 24 | 234358 | 174393 | 180364856 | 186523648 | 1451810 | 71 |
| maintenance-1 | 4194304 | 256821 | 190671 | 691175 | 180466 | 0 | 0 | 1540 | 946 | 5515923 | 16 | -6232854 | -6058461 | 109612408 | 120012800 | 625761 | 72 |
| churn-2 | 8388608 | 319524 | 5094536 | 1012959 | 360793 | 0 | 0 | 2276 | 946 | 15179642 | 20 | 9663719 | 3605258 | 129256456 | 139649024 | 687436 | 82 |
| checkpoint-2 | 8388608 | 319524 | 5094536 | 1012959 | 180339 | 0 | 0 | 2276 | 946 | 14999188 | 20 | -180454 | 3424804 | 160572680 | 168566784 | 1226481 | 82 |
| folded-2 | 8388608 | 319524 | 5202890 | 1704134 | 180339 | 0 | 0 | 2276 | 946 | 15798717 | 20 | 799529 | 4224333 | 121467448 | 132988928 | 644652 | 83 |
| before_vacuum-2 | 8388608 | 319524 | 5204367 | 1704134 | 180339 | 0 | 0 | 2276 | 1092 | 15800340 | 20 | 1623 | 4225956 | 153965840 | 162045952 | 1185783 | 83 |
| before_exhaustive-2 | 4194304 | 319524 | 5206422 | 1704134 | 180339 | 0 | 0 | 3012 | 1092 | 11608827 | 21 | -4191513 | 34443 | 97687616 | 111763456 | 190342 | 84 |
| before_final_gc-2 | 4194304 | 319524 | 5436640 | 1704134 | 180339 | 0 | 0 | 2296 | 1094 | 11838331 | 21 | 229504 | 263947 | 141479704 | 147988480 | 747716 | 84 |
| maintenance-2 | 4194304 | 319524 | 192178 | 691175 | 180339 | 0 | 0 | 1796 | 1094 | 5580410 | 16 | -6257921 | -5993974 | 172939328 | 178397184 | 1290382 | 84 |
| churn-3 | 8388608 | 382355 | 5050908 | 1012959 | 360794 | 0 | 0 | 2535 | 1094 | 15199253 | 20 | 9618843 | 3624869 | 167833008 | 172613632 | 940860 | 94 |
| checkpoint-3 | 8388608 | 382355 | 5050908 | 1012959 | 180467 | 0 | 0 | 2535 | 1094 | 15018926 | 20 | -180327 | 3444542 | 115243856 | 129589248 | 512750 | 95 |
| folded-3 | 8388608 | 382355 | 5159588 | 1704134 | 180467 | 0 | 0 | 2535 | 1094 | 15818781 | 20 | 799855 | 4244397 | 163330632 | 172023808 | 1083348 | 95 |
| before_vacuum-3 | 8388608 | 382355 | 5161067 | 1704134 | 180467 | 0 | 0 | 2535 | 1092 | 15820258 | 20 | 1477 | 4245874 | 104382936 | 117137408 | 310641 | 96 |
| before_exhaustive-3 | 4194304 | 382355 | 5163122 | 1704134 | 180467 | 0 | 0 | 3274 | 1092 | 11628748 | 21 | -4191510 | 54364 | 135866368 | 145063936 | 850554 | 96 |
| before_final_gc-3 | 4194304 | 382355 | 5392991 | 1704134 | 180467 | 0 | 0 | 2304 | 1094 | 11857649 | 21 | 228901 | 283265 | 180611368 | 186810368 | 1409786 | 96 |
| maintenance-3 | 4194304 | 382355 | 192148 | 691175 | 180467 | 0 | 0 | 1803 | 1094 | 5643346 | 16 | -6214303 | -5931038 | 110307760 | 121978880 | 604999 | 97 |
| churn-4 | 8388608 | 445187 | 5019047 | 1012959 | 360923 | 0 | 0 | 2544 | 1094 | 15230362 | 20 | 9587016 | 3655978 | 123359984 | 136708096 | 623402 | 107 |
| checkpoint-4 | 8388608 | 445187 | 5019047 | 1012959 | 180468 | 0 | 0 | 2544 | 1094 | 15049907 | 20 | -180455 | 3475523 | 154797320 | 164880384 | 1166030 | 107 |
| folded-4 | 8388608 | 445187 | 5126866 | 1704134 | 180468 | 0 | 0 | 2544 | 1094 | 15848901 | 20 | 798994 | 4274517 | 109416800 | 123944960 | 426554 | 108 |
| before_vacuum-4 | 8388608 | 445187 | 5128344 | 1704134 | 180468 | 0 | 0 | 2544 | 1093 | 15850378 | 20 | 1477 | 4275994 | 143580616 | 153583616 | 971286 | 108 |
| before_exhaustive-4 | 4194304 | 445187 | 5130398 | 1704134 | 180468 | 0 | 0 | 3285 | 1093 | 11658869 | 21 | -4191509 | 84485 | 174146424 | 181936128 | 1512923 | 108 |
| before_final_gc-4 | 4194304 | 445187 | 5360488 | 1704134 | 180468 | 0 | 0 | 2307 | 1094 | 11887982 | 21 | 229113 | 313598 | 119981896 | 131620864 | 648715 | 109 |
| maintenance-4 | 4194304 | 445187 | 192373 | 691175 | 180468 | 0 | 0 | 1805 | 1094 | 5706406 | 16 | -6181576 | -5867978 | 151459768 | 159776768 | 1194926 | 109 |
| reopen | 4194304 | 445187 | 192373 | 691175 | 12 | 0 | 0 | 1805 | 1094 | 5525950 | 16 | -180456 | -6048434 | 107647416 | 118939648 | 599684 | 110 |


| Operation | Calls |
| --- | --- |
| delete | 640 |
| indexed_update | 640 |
| ordinary_get | 640 |
| post_upsert_get | 640 |
| prepared_get | 640 |
| typed_insert | 640 |
| typed_replace | 640 |
| typed_upsert | 640 |

API durations (ns); aggregate maintenance includes its constituent stages. These overlapping timer columns must not be added again. All other raw API timers and attribution remain in the projection.

| Epoch | Maintenance | Flush | Checkpoint | Fold | Fold checkpoint | Overlay | Overlay checkpoint | Vlog GC | Vacuum | Exhaustive plan | Exhaustive work | Final refresh | Final typed GC | Final leaf GC |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 252475004 | 10530 | 7178080 | 15260427 | 6596834 | 14391 | 5910 | 11017417 | 38931857 | 1168332 | 157705295 | 9692454 | 1094141 | 306333 |
| 1 | 298030371 | 21820 | 11759834 | 14981785 | 7215399 | 12880 | 4520 | 8295540 | 34130980 | 1023620 | 133500571 | 9657743 | 4486213 | 49977074 |
| 2 | 383605660 | 11010 | 10928476 | 14439419 | 7172700 | 13120 | 5250 | 8487562 | 36928497 | 1123781 | 227677171 | 12223748 | 4517314 | 38579843 |
| 3 | 358070154 | 10280 | 11097148 | 14232688 | 15154897 | 12060 | 5630 | 8395741 | 35104449 | 1095530 | 194121448 | 15200067 | 4469003 | 39182549 |
| 4 | 354518814 | 11170 | 11286549 | 22384996 | 7317811 | 12860 | 5880 | 8699304 | 35616264 | 1117441 | 188356961 | 12585071 | 4405522 | 39479761 |

Typed reachability and unlink attribution are separate actual API cells. Sources/ref/segment classes may overlap; they are not unique retained bytes. No-op work is not reclamation.

| Stage | Eligible segments | Deleted segments | Retained segments | Eligible B | Deleted B | Retained B | Protected refs | Protected ref B | Mapped handles | Pinned refs | Pinned B | Rewrite debt B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0/before-fold | 0 | 0 | 1 | 0 | 0 | 1045462 | 896 | 1045462 | 128 | 128 | 725636 | 0 |
| epoch-0/reclaim-plan | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-0/reclaim-GC | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-1/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-1/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-1/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-2/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-2/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-2/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-3/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-3/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-3/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-4/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-4/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-4/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| after-view-release/reclaim-plan | 0 | 0 | 1 | 0 | 0 | 1736637 | 1 | 691175 | 0 | 0 | 0 | 1045462 |
| after-view-release/reclaim-GC | 1 | 0 | 2 | 1736637 | 0 | 2427812 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-0/final-GC | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-1/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-2/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-3/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-4/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| after-view-release/final-GC | 1 | 1 | 1 | 1736637 | 1736637 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |


| Stage | Decision | Plan ns | Probe ns | Rewrite ns | Checkpoint ns | GC ns | Candidate refs |
| --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | no_debt | 1098740 | 0 | 0 | 0 | 1025550 | 896 |
| epoch-1 | eligible | 1206642 | 256342 | 9574693 | 7095969 | 4177300 | 769 |
| epoch-2 | eligible | 1225352 | 266812 | 8148589 | 6979628 | 4235511 | 769 |
| epoch-3 | eligible | 1224692 | 263963 | 6663984 | 7005768 | 4196621 | 769 |
| epoch-4 | eligible | 1227571 | 260303 | 9606923 | 7254150 | 4246871 | 769 |
| after-view-release | eligible | 486914 | 364514 | 26931890 | 6486673 | 478195 | 897 |


| Epoch | Vlog segments | Active | Referenced | Protected | Pending | Eligible | Deleted | Eligible B | Deleted B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-1 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-3 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-4 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |


| Final stage | Leaf GC ns | Eligible generations | Deleted generations | Files deleted | Leaf bytes deleted | Revision GC unsupported | Revisions total | Protected revisions | Eligible revisions | Deleted revisions | Revision bytes deleted |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | 306333 | 0 | 0 | 0 | 0 | False | 8 | 8 | 0 | 0 | 0 |
| epoch-1 | 49977074 | 2 | 2 | 2 | 5174901 | False | 7 | 2 | 5 | 5 | 4464 |
| epoch-2 | 38579843 | 2 | 2 | 2 | 5202026 | False | 4 | 2 | 2 | 2 | 2112 |
| epoch-3 | 39182549 | 2 | 2 | 2 | 5158686 | False | 4 | 2 | 2 | 2 | 2120 |
| epoch-4 | 39479761 | 2 | 2 | 2 | 5125949 | False | 4 | 2 | 2 | 2 | 2122 |
| after-view-release | 69024687 | 2 | 2 | 2 | 7409880 | False | 11 | 2 | 9 | 9 | 5329 |

Refresh snapshots expose both durable slots, root IDs and command coverage. Duration repeats across its before/after pair; count it once. Full root records and all slot metadata remain in the projection.

| Stage | Boundary | Refresh ns | CommitSeq | User root | System root | AppliedLSN | NextLSN | Selected slot | Slot0 CommitSeq | Slot1 CommitSeq |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | before | 9692454 | 775 | 16 | 15 | 769 | 770 | 1 | 772 | 775 |
| epoch-0 | after | 9692454 | 776 | 16 | 15 | 769 | 770 | 0 | 776 | 775 |
| epoch-1 | before | 9657743 | 1423 | 16 | 15 | 1409 | 1410 | 1 | 1421 | 1423 |
| epoch-1 | after | 9657743 | 1424 | 16 | 15 | 1409 | 1410 | 0 | 1424 | 1423 |
| epoch-2 | before | 12223748 | 2069 | 16 | 15 | 2049 | 2050 | 1 | 2067 | 2069 |
| epoch-2 | after | 12223748 | 2070 | 16 | 15 | 2049 | 2050 | 0 | 2070 | 2069 |
| epoch-3 | before | 15200067 | 2715 | 16 | 15 | 2689 | 2690 | 1 | 2713 | 2715 |
| epoch-3 | after | 15200067 | 2716 | 16 | 15 | 2689 | 2690 | 0 | 2716 | 2715 |
| epoch-4 | before | 12585071 | 3361 | 16 | 15 | 3329 | 3330 | 1 | 3359 | 3361 |
| epoch-4 | after | 12585071 | 3362 | 16 | 15 | 3329 | 3330 | 0 | 3362 | 3361 |
| after-view-release | before | 3259822 | 777 | 16 | 77 | 769 | 770 | 1 | 776 | 777 |
| after-view-release | after | 3259822 | 778 | 16 | 77 | 769 | 770 | 0 | 778 | 777 |

Recorded exhaustive maintenance owner, work limits and remaining debt follow. Work limits are not storage-capacity bounds. An internal replay-inline owner classification does not change the public direct-backend opener. All audit/rewrite/leaf/index phases and counters remain in the projection. Existing raw ratio fields are retained there rather than interpreted in these tables.

| Epoch | Owner | Options / work limits | Remaining debt | Fully compacted | Policy fully compacted | Byte minimized |
| --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"leaf_generation","index_vacuum_required":true,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":0,"leaf_gc_generations":0,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-1 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5174901,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-2 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5202026,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-3 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5158686,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-4 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5125949,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |


</details>


<details><summary>Process 2: complete phase trajectory and maintenance</summary>

All tables in this block are this process's actual cells. Heap cuts are census-boundary samples.

| Phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | Total B | Files | Adjacent total ΔB | Since ingest ΔB | HeapAlloc B | HeapInuse B | HeapObjects | NumGC |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | 8388608 | 131289 | 1627932 | 725636 | 699917 | 0 | 0 | 554 | 470 | 11574406 | 12 | — | 0 | 108845376 | 114343936 | 982737 | 38 |
| churn-0 | 88080384 | 193991 | 7307584 | 1045462 | 878285 | 0 | 0 | 554 | 470 | 97506730 | 12 | 85932324 | 85932324 | 139585544 | 147611648 | 1097581 | 53 |
| checkpoint-0 | 88080384 | 193991 | 7307584 | 1045462 | 878285 | 0 | 0 | 554 | 470 | 97506730 | 13 | 0 | 85932324 | 114177208 | 124076032 | 713581 | 54 |
| folded-0 | 88080384 | 193991 | 7403308 | 1736637 | 878285 | 0 | 0 | 554 | 470 | 98293629 | 13 | 786899 | 86719223 | 122109200 | 129966080 | 769275 | 55 |
| before_vacuum-0 | 88080384 | 193991 | 7403308 | 1736637 | 878285 | 0 | 0 | 554 | 795 | 98293954 | 14 | 325 | 86719548 | 99774816 | 110166016 | 412963 | 56 |
| before_exhaustive-0 | 4194304 | 193991 | 7405389 | 1736637 | 878285 | 0 | 0 | 855 | 795 | 14410256 | 15 | -83883698 | 2835850 | 164663328 | 171008000 | 1492930 | 56 |
| before_final_gc-0 | 4194304 | 193991 | 7655589 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14663681 | 24 | 253425 | 3089275 | 154559896 | 160423936 | 1185058 | 57 |
| maintenance-0 | 4194304 | 193991 | 7655589 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14663681 | 24 | 0 | 3089275 | 136061784 | 143155200 | 1035949 | 58 |
| after_view_release | 4194304 | 193991 | 189498 | 691175 | 878285 | 0 | 0 | 1277 | 795 | 6149325 | 16 | -8514356 | -5425081 | 114369928 | 122372096 | 636671 | 59 |
| churn-1 | 8388608 | 256821 | 5064686 | 1012959 | 1058739 | 0 | 0 | 1277 | 795 | 15783885 | 17 | 9634560 | 4209479 | 159447152 | 164093952 | 892906 | 69 |
| checkpoint-1 | 8388608 | 256821 | 5064686 | 1012959 | 180466 | 0 | 0 | 1277 | 795 | 14905612 | 17 | -878273 | 3331206 | 108052440 | 121339904 | 417641 | 70 |
| folded-1 | 8388608 | 256821 | 5175119 | 1704134 | 180466 | 0 | 0 | 1277 | 795 | 15707220 | 17 | 801608 | 4132814 | 156150632 | 163708928 | 984657 | 70 |
| before_vacuum-1 | 8388608 | 256821 | 5176598 | 1704134 | 180466 | 0 | 0 | 1277 | 946 | 15708850 | 17 | 1630 | 4134444 | 98795808 | 111132672 | 220543 | 71 |
| before_exhaustive-1 | 4194304 | 256821 | 5178653 | 1704134 | 180466 | 0 | 0 | 1794 | 946 | 11517118 | 18 | -4191732 | -57288 | 130156464 | 137871360 | 756843 | 71 |
| before_final_gc-1 | 4194304 | 256821 | 5410407 | 1704134 | 180466 | 0 | 0 | 4398 | 946 | 11751476 | 24 | 234358 | 177070 | 174434064 | 180068352 | 1311011 | 71 |
| maintenance-1 | 4194304 | 256821 | 190671 | 691175 | 180466 | 0 | 0 | 1540 | 946 | 5515923 | 16 | -6235553 | -6058483 | 106598592 | 117301248 | 555504 | 72 |
| churn-2 | 8388608 | 319524 | 5094215 | 1012959 | 360793 | 0 | 0 | 2276 | 946 | 15179321 | 20 | 9663398 | 3604915 | 118446368 | 131588096 | 541153 | 82 |
| checkpoint-2 | 8388608 | 319524 | 5094215 | 1012959 | 180339 | 0 | 0 | 2276 | 946 | 14998867 | 20 | -180454 | 3424461 | 149756728 | 159539200 | 1080187 | 82 |
| folded-2 | 8388608 | 319524 | 5199765 | 1704134 | 180339 | 0 | 0 | 2276 | 946 | 15795592 | 20 | 796725 | 4221186 | 104604936 | 118956032 | 322325 | 83 |
| before_vacuum-2 | 8388608 | 319524 | 5201242 | 1704134 | 180339 | 0 | 0 | 2276 | 1092 | 15797215 | 20 | 1623 | 4222809 | 138680704 | 148348928 | 863507 | 83 |
| before_exhaustive-2 | 4194304 | 319524 | 5203297 | 1704134 | 180339 | 0 | 0 | 3012 | 1092 | 11605702 | 21 | -4191513 | 31296 | 169160648 | 176349184 | 1401571 | 83 |
| before_final_gc-2 | 4194304 | 319524 | 5433515 | 1704134 | 180339 | 0 | 0 | 2296 | 1094 | 11835206 | 21 | 229504 | 260800 | 120010440 | 131457024 | 637640 | 84 |
| maintenance-2 | 4194304 | 319524 | 192178 | 691175 | 180339 | 0 | 0 | 1796 | 1094 | 5580410 | 16 | -6254796 | -5993996 | 152321896 | 160497664 | 1180318 | 84 |
| churn-3 | 8388608 | 382355 | 5051780 | 1012959 | 360794 | 0 | 0 | 2535 | 1094 | 15200125 | 20 | 9619715 | 3625719 | 144721088 | 151920640 | 789920 | 94 |
| checkpoint-3 | 8388608 | 382355 | 5051780 | 1012959 | 180467 | 0 | 0 | 2535 | 1094 | 15019798 | 20 | -180327 | 3445392 | 175219576 | 180609024 | 1330720 | 94 |
| folded-3 | 8388608 | 382355 | 5156179 | 1704134 | 180467 | 0 | 0 | 2535 | 1094 | 15815372 | 20 | 795574 | 4240966 | 137058088 | 147275776 | 680871 | 95 |
| before_vacuum-3 | 8388608 | 382355 | 5157658 | 1704134 | 180467 | 0 | 0 | 2535 | 1093 | 15816850 | 20 | 1478 | 4242444 | 170253512 | 176717824 | 1223761 | 95 |
| before_exhaustive-3 | 4194304 | 382355 | 5159713 | 1704134 | 180467 | 0 | 0 | 3274 | 1093 | 11625340 | 21 | -4191510 | 50934 | 113093256 | 125689856 | 470761 | 96 |
| before_final_gc-3 | 4194304 | 382355 | 5389582 | 1704134 | 180467 | 0 | 0 | 2304 | 1094 | 11854240 | 21 | 228900 | 279834 | 157778928 | 164528128 | 1029958 | 96 |
| maintenance-3 | 4194304 | 382355 | 192148 | 691175 | 180467 | 0 | 0 | 1803 | 1094 | 5643346 | 16 | -6210894 | -5931060 | 83472128 | 100089856 | 125652 | 97 |
| churn-4 | 8388608 | 445187 | 5018912 | 1012959 | 360923 | 0 | 0 | 2544 | 1094 | 15230227 | 20 | 9586881 | 3655821 | 179112584 | 183345152 | 1036569 | 106 |
| checkpoint-4 | 8388608 | 445187 | 5018912 | 1012959 | 180468 | 0 | 0 | 2544 | 1094 | 15049772 | 20 | -180455 | 3475366 | 122574280 | 134832128 | 654037 | 107 |
| folded-4 | 8388608 | 445187 | 5127539 | 1704134 | 180468 | 0 | 0 | 2544 | 1094 | 15849574 | 20 | 799802 | 4275168 | 170772088 | 177930240 | 1226423 | 107 |
| before_vacuum-4 | 8388608 | 445187 | 5129017 | 1704134 | 180468 | 0 | 0 | 2544 | 1094 | 15851052 | 20 | 1478 | 4276646 | 119424744 | 130965504 | 590826 | 108 |
| before_exhaustive-4 | 4194304 | 445187 | 5131071 | 1704134 | 180468 | 0 | 0 | 3285 | 1094 | 11659543 | 21 | -4191509 | 85137 | 150087208 | 158474240 | 1132502 | 108 |
| before_final_gc-4 | 4194304 | 445187 | 5361161 | 1704134 | 180468 | 0 | 0 | 2307 | 1094 | 11888655 | 21 | 229112 | 314249 | 103417304 | 117260288 | 306802 | 109 |
| maintenance-4 | 4194304 | 445187 | 192373 | 691175 | 180468 | 0 | 0 | 1805 | 1094 | 5706406 | 16 | -6182249 | -5868000 | 134904440 | 144900096 | 853020 | 109 |
| reopen | 4194304 | 445187 | 192373 | 691175 | 12 | 0 | 0 | 1805 | 1094 | 5525950 | 16 | -180456 | -6048456 | 186067232 | 193150976 | 1395666 | 109 |


| Operation | Calls |
| --- | --- |
| delete | 640 |
| indexed_update | 640 |
| ordinary_get | 640 |
| post_upsert_get | 640 |
| prepared_get | 640 |
| typed_insert | 640 |
| typed_replace | 640 |
| typed_upsert | 640 |

API durations (ns); aggregate maintenance includes its constituent stages. These overlapping timer columns must not be added again. All other raw API timers and attribution remain in the projection.

| Epoch | Maintenance | Flush | Checkpoint | Fold | Fold checkpoint | Overlay | Overlay checkpoint | Vlog GC | Vacuum | Exhaustive plan | Exhaustive work | Final refresh | Final typed GC | Final leaf GC |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 257900892 | 10590 | 7024298 | 14486510 | 6657814 | 14480 | 5670 | 13441920 | 57309444 | 1163421 | 143418027 | 9526442 | 1081330 | 312873 |
| 1 | 351350627 | 10740 | 11266479 | 14021386 | 7191989 | 11920 | 5420 | 8371931 | 88795489 | 1009250 | 135199217 | 9584812 | 4388452 | 50094435 |
| 2 | 384779810 | 11440 | 10734513 | 15245107 | 12112988 | 13620 | 5600 | 8594863 | 37076429 | 1081661 | 186728595 | 11144497 | 4510884 | 39376291 |
| 3 | 351378925 | 10420 | 10434411 | 15201487 | 7202510 | 12481 | 5530 | 8478702 | 46689851 | 1090170 | 179377634 | 17825562 | 4388792 | 39181778 |
| 4 | 354135354 | 11030 | 11713303 | 15546820 | 7218270 | 12840 | 5190 | 8586953 | 39987477 | 1081031 | 193083146 | 11139327 | 4481644 | 39671573 |

Typed reachability and unlink attribution are separate actual API cells. Sources/ref/segment classes may overlap; they are not unique retained bytes. No-op work is not reclamation.

| Stage | Eligible segments | Deleted segments | Retained segments | Eligible B | Deleted B | Retained B | Protected refs | Protected ref B | Mapped handles | Pinned refs | Pinned B | Rewrite debt B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0/before-fold | 0 | 0 | 1 | 0 | 0 | 1045462 | 896 | 1045462 | 128 | 128 | 725636 | 0 |
| epoch-0/reclaim-plan | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-0/reclaim-GC | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-1/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-1/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-1/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-2/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-2/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-2/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-3/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-3/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-3/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-4/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-4/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-4/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| after-view-release/reclaim-plan | 0 | 0 | 1 | 0 | 0 | 1736637 | 1 | 691175 | 0 | 0 | 0 | 1045462 |
| after-view-release/reclaim-GC | 1 | 0 | 2 | 1736637 | 0 | 2427812 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-0/final-GC | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-1/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-2/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-3/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-4/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| after-view-release/final-GC | 1 | 1 | 1 | 1736637 | 1736637 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |


| Stage | Decision | Plan ns | Probe ns | Rewrite ns | Checkpoint ns | GC ns | Candidate refs |
| --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | no_debt | 1114371 | 0 | 0 | 0 | 991309 | 896 |
| epoch-1 | eligible | 1216762 | 263113 | 8152688 | 6970418 | 4159460 | 769 |
| epoch-2 | eligible | 1245732 | 263903 | 44467740 | 7210550 | 4304131 | 769 |
| epoch-3 | eligible | 1219701 | 260153 | 7966617 | 7073118 | 4319972 | 769 |
| epoch-4 | eligible | 1203862 | 276362 | 8186709 | 7058379 | 4227341 | 769 |
| after-view-release | eligible | 461234 | 342274 | 29613786 | 6498843 | 481244 | 897 |


| Epoch | Vlog segments | Active | Referenced | Protected | Pending | Eligible | Deleted | Eligible B | Deleted B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-1 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-3 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-4 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |


| Final stage | Leaf GC ns | Eligible generations | Deleted generations | Files deleted | Leaf bytes deleted | Revision GC unsupported | Revisions total | Protected revisions | Eligible revisions | Deleted revisions | Revision bytes deleted |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | 312873 | 0 | 0 | 0 | 0 | False | 8 | 8 | 0 | 0 | 0 |
| epoch-1 | 50094435 | 2 | 2 | 2 | 5177600 | False | 7 | 2 | 5 | 5 | 4464 |
| epoch-2 | 39376291 | 2 | 2 | 2 | 5198901 | False | 4 | 2 | 2 | 2 | 2112 |
| epoch-3 | 39181778 | 2 | 2 | 2 | 5155277 | False | 4 | 2 | 2 | 2 | 2120 |
| epoch-4 | 39671573 | 2 | 2 | 2 | 5126622 | False | 4 | 2 | 2 | 2 | 2122 |
| after-view-release | 86641838 | 2 | 2 | 2 | 7408166 | False | 11 | 2 | 9 | 9 | 5329 |

Refresh snapshots expose both durable slots, root IDs and command coverage. Duration repeats across its before/after pair; count it once. Full root records and all slot metadata remain in the projection.

| Stage | Boundary | Refresh ns | CommitSeq | User root | System root | AppliedLSN | NextLSN | Selected slot | Slot0 CommitSeq | Slot1 CommitSeq |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | before | 9526442 | 775 | 16 | 15 | 769 | 770 | 1 | 772 | 775 |
| epoch-0 | after | 9526442 | 776 | 16 | 15 | 769 | 770 | 0 | 776 | 775 |
| epoch-1 | before | 9584812 | 1423 | 16 | 15 | 1409 | 1410 | 1 | 1421 | 1423 |
| epoch-1 | after | 9584812 | 1424 | 16 | 15 | 1409 | 1410 | 0 | 1424 | 1423 |
| epoch-2 | before | 11144497 | 2069 | 16 | 15 | 2049 | 2050 | 1 | 2067 | 2069 |
| epoch-2 | after | 11144497 | 2070 | 16 | 15 | 2049 | 2050 | 0 | 2070 | 2069 |
| epoch-3 | before | 17825562 | 2715 | 16 | 15 | 2689 | 2690 | 1 | 2713 | 2715 |
| epoch-3 | after | 17825562 | 2716 | 16 | 15 | 2689 | 2690 | 0 | 2716 | 2715 |
| epoch-4 | before | 11139327 | 3361 | 16 | 15 | 3329 | 3330 | 1 | 3359 | 3361 |
| epoch-4 | after | 11139327 | 3362 | 16 | 15 | 3329 | 3330 | 0 | 3362 | 3361 |
| after-view-release | before | 5207730 | 777 | 16 | 77 | 769 | 770 | 1 | 776 | 777 |
| after-view-release | after | 5207730 | 778 | 16 | 77 | 769 | 770 | 0 | 778 | 777 |

Recorded exhaustive maintenance owner, work limits and remaining debt follow. Work limits are not storage-capacity bounds. An internal replay-inline owner classification does not change the public direct-backend opener. All audit/rewrite/leaf/index phases and counters remain in the projection. Existing raw ratio fields are retained there rather than interpreted in these tables.

| Epoch | Owner | Options / work limits | Remaining debt | Fully compacted | Policy fully compacted | Byte minimized |
| --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"leaf_generation","index_vacuum_required":true,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":0,"leaf_gc_generations":0,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-1 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5177600,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-2 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5198901,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-3 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5155277,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-4 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5126622,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |


</details>


<details><summary>Process 3: complete phase trajectory and maintenance</summary>

All tables in this block are this process's actual cells. Heap cuts are census-boundary samples.

| Phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | Total B | Files | Adjacent total ΔB | Since ingest ΔB | HeapAlloc B | HeapInuse B | HeapObjects | NumGC |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | 8388608 | 131289 | 1627950 | 725636 | 699917 | 0 | 0 | 554 | 470 | 11574424 | 12 | — | 0 | 108775912 | 114696192 | 979094 | 38 |
| churn-0 | 88080384 | 193991 | 7307444 | 1045462 | 878285 | 0 | 0 | 554 | 470 | 97506590 | 12 | 85932166 | 85932166 | 140027536 | 148463616 | 1128678 | 53 |
| checkpoint-0 | 88080384 | 193991 | 7307444 | 1045462 | 878285 | 0 | 0 | 554 | 470 | 97506590 | 13 | 0 | 85932166 | 121112688 | 130777088 | 761797 | 54 |
| folded-0 | 88080384 | 193991 | 7406299 | 1736637 | 878285 | 0 | 0 | 554 | 470 | 98296620 | 13 | 790030 | 86722196 | 115078376 | 123478016 | 703080 | 55 |
| before_vacuum-0 | 88080384 | 193991 | 7406299 | 1736637 | 878285 | 0 | 0 | 554 | 795 | 98296945 | 14 | 325 | 86722521 | 102127448 | 112353280 | 448085 | 56 |
| before_exhaustive-0 | 4194304 | 193991 | 7408380 | 1736637 | 878285 | 0 | 0 | 855 | 795 | 14413247 | 15 | -83883698 | 2838823 | 166149144 | 172867584 | 1528016 | 56 |
| before_final_gc-0 | 4194304 | 193991 | 7658580 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14666672 | 24 | 253425 | 3092248 | 154586616 | 160579584 | 1185053 | 57 |
| maintenance-0 | 4194304 | 193991 | 7658580 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14666672 | 24 | 0 | 3092248 | 136149896 | 143335424 | 1039511 | 58 |
| after_view_release | 4194304 | 193991 | 189498 | 691175 | 878285 | 0 | 0 | 1277 | 795 | 6149325 | 16 | -8517347 | -5425099 | 114721248 | 122609664 | 638545 | 59 |
| churn-1 | 8388608 | 256821 | 5064391 | 1012959 | 1058739 | 0 | 0 | 1277 | 795 | 15783590 | 17 | 9634265 | 4209166 | 164179432 | 168509440 | 931139 | 69 |
| checkpoint-1 | 8388608 | 256821 | 5064391 | 1012959 | 180466 | 0 | 0 | 1277 | 795 | 14905317 | 17 | -878273 | 3330893 | 114253424 | 126296064 | 519152 | 70 |
| folded-1 | 8388608 | 256821 | 5171883 | 1704134 | 180466 | 0 | 0 | 1277 | 795 | 15703984 | 17 | 798667 | 4129560 | 162390096 | 169369600 | 1086192 | 70 |
| before_vacuum-1 | 8388608 | 256821 | 5173362 | 1704134 | 180466 | 0 | 0 | 1277 | 946 | 15705614 | 17 | 1630 | 4131190 | 102742848 | 113614848 | 337833 | 71 |
| before_exhaustive-1 | 4194304 | 256821 | 5175417 | 1704134 | 180466 | 0 | 0 | 1794 | 946 | 11513882 | 18 | -4191732 | -60542 | 134082368 | 141418496 | 874118 | 71 |
| before_final_gc-1 | 4194304 | 256821 | 5407171 | 1704134 | 180466 | 0 | 0 | 4398 | 946 | 11748240 | 24 | 234358 | 173816 | 178256240 | 183803904 | 1428241 | 71 |
| maintenance-1 | 4194304 | 256821 | 190671 | 691175 | 180466 | 0 | 0 | 1540 | 946 | 5515923 | 16 | -6232317 | -6058501 | 109963440 | 119422976 | 637388 | 72 |
| churn-2 | 8388608 | 319524 | 5094244 | 1012959 | 360793 | 0 | 0 | 2276 | 946 | 15179350 | 20 | 9663427 | 3604926 | 133460176 | 142802944 | 717105 | 82 |
| checkpoint-2 | 8388608 | 319524 | 5094244 | 1012959 | 180339 | 0 | 0 | 2276 | 946 | 14998896 | 20 | -180454 | 3424472 | 163905280 | 171286528 | 1256113 | 82 |
| folded-2 | 8388608 | 319524 | 5204587 | 1704134 | 180339 | 0 | 0 | 2276 | 946 | 15800414 | 20 | 801518 | 4225990 | 120688152 | 132022272 | 644622 | 83 |
| before_vacuum-2 | 8388608 | 319524 | 5206064 | 1704134 | 180339 | 0 | 0 | 2276 | 1092 | 15802037 | 20 | 1623 | 4227613 | 153888104 | 161972224 | 1185752 | 83 |
| before_exhaustive-2 | 4194304 | 319524 | 5208119 | 1704134 | 180339 | 0 | 0 | 3012 | 1092 | 11610524 | 21 | -4191513 | 36100 | 104634232 | 116727808 | 352514 | 84 |
| before_final_gc-2 | 4194304 | 319524 | 5438337 | 1704134 | 180339 | 0 | 0 | 2296 | 1094 | 11840028 | 21 | 229504 | 265604 | 148484432 | 155017216 | 909911 | 84 |
| maintenance-2 | 4194304 | 319524 | 192178 | 691175 | 180339 | 0 | 0 | 1796 | 1094 | 5580410 | 16 | -6259618 | -5994014 | 179875336 | 185352192 | 1452532 | 84 |
| churn-3 | 8388608 | 382355 | 5053482 | 1012959 | 360794 | 0 | 0 | 2535 | 1094 | 15201827 | 20 | 9621417 | 3627403 | 171502920 | 176087040 | 990194 | 94 |
| checkpoint-3 | 8388608 | 382355 | 5053482 | 1012959 | 180467 | 0 | 0 | 2535 | 1094 | 15021500 | 20 | -180327 | 3447076 | 117999256 | 131342336 | 576274 | 95 |
| folded-3 | 8388608 | 382355 | 5159735 | 1704134 | 180467 | 0 | 0 | 2535 | 1094 | 15818928 | 20 | 797428 | 4244504 | 165394536 | 173350912 | 1146875 | 95 |
| before_vacuum-3 | 8388608 | 382355 | 5161214 | 1704134 | 180467 | 0 | 0 | 2535 | 1092 | 15820405 | 20 | 1477 | 4245981 | 107133048 | 119734272 | 397571 | 96 |
| before_exhaustive-3 | 4194304 | 382355 | 5163269 | 1704134 | 180467 | 0 | 0 | 3274 | 1092 | 11628895 | 21 | -4191510 | 54471 | 138584944 | 147390464 | 937473 | 96 |
| before_final_gc-3 | 4194304 | 382355 | 5393138 | 1704134 | 180467 | 0 | 0 | 2304 | 1094 | 11857796 | 21 | 228901 | 283372 | 183258040 | 189169664 | 1496662 | 96 |
| maintenance-3 | 4194304 | 382355 | 192148 | 691175 | 180467 | 0 | 0 | 1803 | 1094 | 5643346 | 16 | -6214450 | -5931078 | 111959712 | 123076608 | 644330 | 97 |
| churn-4 | 8388608 | 445187 | 5018985 | 1012959 | 360923 | 0 | 0 | 2544 | 1094 | 15230300 | 20 | 9586954 | 3655876 | 124049272 | 136708096 | 655326 | 107 |
| checkpoint-4 | 8388608 | 445187 | 5018985 | 1012959 | 180468 | 0 | 0 | 2544 | 1094 | 15049845 | 20 | -180455 | 3475421 | 155461152 | 164945920 | 1197941 | 107 |
| folded-4 | 8388608 | 445187 | 5126857 | 1704134 | 180468 | 0 | 0 | 2544 | 1094 | 15848892 | 20 | 799047 | 4274468 | 117899144 | 130859008 | 557085 | 108 |
| before_vacuum-4 | 8388608 | 445187 | 5128335 | 1704134 | 180468 | 0 | 0 | 2544 | 1094 | 15850370 | 20 | 1478 | 4275946 | 152093624 | 160890880 | 1101811 | 108 |
| before_exhaustive-4 | 4194304 | 445187 | 5130389 | 1704134 | 180468 | 0 | 0 | 3285 | 1094 | 11658861 | 21 | -4191509 | 84437 | 183579664 | 191455232 | 1643494 | 108 |
| before_final_gc-4 | 4194304 | 445187 | 5360479 | 1704134 | 180468 | 0 | 0 | 2307 | 1094 | 11887973 | 21 | 229112 | 313549 | 133729976 | 141344768 | 665836 | 109 |
| maintenance-4 | 4194304 | 445187 | 192373 | 691175 | 180468 | 0 | 0 | 1805 | 1094 | 5706406 | 16 | -6181567 | -5868018 | 165223776 | 170450944 | 1212058 | 109 |
| reopen | 4194304 | 445187 | 192373 | 691175 | 12 | 0 | 0 | 1805 | 1094 | 5525950 | 16 | -180456 | -6048474 | 110759376 | 121528320 | 647365 | 110 |


| Operation | Calls |
| --- | --- |
| delete | 640 |
| indexed_update | 640 |
| ordinary_get | 640 |
| post_upsert_get | 640 |
| prepared_get | 640 |
| typed_insert | 640 |
| typed_replace | 640 |
| typed_upsert | 640 |

API durations (ns); aggregate maintenance includes its constituent stages. These overlapping timer columns must not be added again. All other raw API timers and attribution remain in the projection.

| Epoch | Maintenance | Flush | Checkpoint | Fold | Fold checkpoint | Overlay | Overlay checkpoint | Vlog GC | Vacuum | Exhaustive plan | Exhaustive work | Final refresh | Final typed GC | Final leaf GC |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 232724631 | 11060 | 6914647 | 16324568 | 6640274 | 14551 | 5430 | 10397171 | 36363401 | 1160282 | 139704270 | 10326070 | 1097541 | 315733 |
| 1 | 335056310 | 10680 | 11443511 | 14980545 | 7259950 | 12330 | 5320 | 8401381 | 64003449 | 988970 | 137991724 | 8767835 | 3606995 | 55940030 |
| 2 | 354747779 | 11110 | 10599152 | 13972526 | 7143009 | 12760 | 5490 | 8383021 | 34474824 | 1095090 | 199487259 | 11233928 | 4516693 | 38780195 |
| 3 | 400761786 | 10320 | 11334840 | 14651352 | 6700885 | 11840 | 5620 | 8483552 | 37195309 | 1111781 | 210556416 | 45661272 | 4557264 | 39161819 |
| 4 | 378226056 | 10630 | 11122568 | 14697952 | 22203874 | 13480 | 5520 | 8396951 | 33543464 | 1069731 | 186235440 | 35566413 | 4444083 | 39176939 |

Typed reachability and unlink attribution are separate actual API cells. Sources/ref/segment classes may overlap; they are not unique retained bytes. No-op work is not reclamation.

| Stage | Eligible segments | Deleted segments | Retained segments | Eligible B | Deleted B | Retained B | Protected refs | Protected ref B | Mapped handles | Pinned refs | Pinned B | Rewrite debt B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0/before-fold | 0 | 0 | 1 | 0 | 0 | 1045462 | 896 | 1045462 | 128 | 128 | 725636 | 0 |
| epoch-0/reclaim-plan | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-0/reclaim-GC | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-1/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-1/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-1/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-2/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-2/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-2/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-3/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-3/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-3/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-4/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-4/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-4/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| after-view-release/reclaim-plan | 0 | 0 | 1 | 0 | 0 | 1736637 | 1 | 691175 | 0 | 0 | 0 | 1045462 |
| after-view-release/reclaim-GC | 1 | 0 | 2 | 1736637 | 0 | 2427812 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-0/final-GC | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-1/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-2/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-3/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-4/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| after-view-release/final-GC | 1 | 1 | 1 | 1736637 | 1736637 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |


| Stage | Decision | Plan ns | Probe ns | Rewrite ns | Checkpoint ns | GC ns | Candidate refs |
| --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | no_debt | 1097790 | 0 | 0 | 0 | 990130 | 896 |
| epoch-1 | eligible | 1200052 | 269082 | 8155639 | 7121759 | 4267042 | 769 |
| epoch-2 | eligible | 1198872 | 269373 | 11597902 | 7029067 | 4292882 | 769 |
| epoch-3 | eligible | 1225222 | 260483 | 8423501 | 6499553 | 4261051 | 769 |
| epoch-4 | eligible | 1219611 | 267953 | 8783415 | 6590224 | 4221171 | 769 |
| after-view-release | eligible | 456215 | 340333 | 25258624 | 6485423 | 478915 | 897 |


| Epoch | Vlog segments | Active | Referenced | Protected | Pending | Eligible | Deleted | Eligible B | Deleted B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-1 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-3 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-4 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |


| Final stage | Leaf GC ns | Eligible generations | Deleted generations | Files deleted | Leaf bytes deleted | Revision GC unsupported | Revisions total | Protected revisions | Eligible revisions | Deleted revisions | Revision bytes deleted |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | 315733 | 0 | 0 | 0 | 0 | False | 8 | 8 | 0 | 0 | 0 |
| epoch-1 | 55940030 | 2 | 2 | 2 | 5174364 | False | 7 | 2 | 5 | 5 | 4464 |
| epoch-2 | 38780195 | 2 | 2 | 2 | 5203723 | False | 4 | 2 | 2 | 2 | 2112 |
| epoch-3 | 39161819 | 2 | 2 | 2 | 5158833 | False | 4 | 2 | 2 | 2 | 2120 |
| epoch-4 | 39176939 | 2 | 2 | 2 | 5125940 | False | 4 | 2 | 2 | 2 | 2122 |
| after-view-release | 65809996 | 2 | 2 | 2 | 7411157 | False | 11 | 2 | 9 | 9 | 5329 |

Refresh snapshots expose both durable slots, root IDs and command coverage. Duration repeats across its before/after pair; count it once. Full root records and all slot metadata remain in the projection.

| Stage | Boundary | Refresh ns | CommitSeq | User root | System root | AppliedLSN | NextLSN | Selected slot | Slot0 CommitSeq | Slot1 CommitSeq |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | before | 10326070 | 775 | 16 | 15 | 769 | 770 | 1 | 772 | 775 |
| epoch-0 | after | 10326070 | 776 | 16 | 15 | 769 | 770 | 0 | 776 | 775 |
| epoch-1 | before | 8767835 | 1423 | 16 | 15 | 1409 | 1410 | 1 | 1421 | 1423 |
| epoch-1 | after | 8767835 | 1424 | 16 | 15 | 1409 | 1410 | 0 | 1424 | 1423 |
| epoch-2 | before | 11233928 | 2069 | 16 | 15 | 2049 | 2050 | 1 | 2067 | 2069 |
| epoch-2 | after | 11233928 | 2070 | 16 | 15 | 2049 | 2050 | 0 | 2070 | 2069 |
| epoch-3 | before | 45661272 | 2715 | 16 | 15 | 2689 | 2690 | 1 | 2713 | 2715 |
| epoch-3 | after | 45661272 | 2716 | 16 | 15 | 2689 | 2690 | 0 | 2716 | 2715 |
| epoch-4 | before | 35566413 | 3361 | 16 | 15 | 3329 | 3330 | 1 | 3359 | 3361 |
| epoch-4 | after | 35566413 | 3362 | 16 | 15 | 3329 | 3330 | 0 | 3362 | 3361 |
| after-view-release | before | 3268782 | 777 | 16 | 77 | 769 | 770 | 1 | 776 | 777 |
| after-view-release | after | 3268782 | 778 | 16 | 77 | 769 | 770 | 0 | 778 | 777 |

Recorded exhaustive maintenance owner, work limits and remaining debt follow. Work limits are not storage-capacity bounds. An internal replay-inline owner classification does not change the public direct-backend opener. All audit/rewrite/leaf/index phases and counters remain in the projection. Existing raw ratio fields are retained there rather than interpreted in these tables.

| Epoch | Owner | Options / work limits | Remaining debt | Fully compacted | Policy fully compacted | Byte minimized |
| --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"leaf_generation","index_vacuum_required":true,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":0,"leaf_gc_generations":0,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-1 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5174364,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-2 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5203723,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-3 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5158833,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-4 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5125940,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |


</details>


<details><summary>Process 4: complete phase trajectory and maintenance</summary>

All tables in this block are this process's actual cells. Heap cuts are census-boundary samples.

| Phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | Total B | Files | Adjacent total ΔB | Since ingest ΔB | HeapAlloc B | HeapInuse B | HeapObjects | NumGC |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | 8388608 | 131289 | 1627931 | 725636 | 699917 | 0 | 0 | 554 | 470 | 11574405 | 12 | — | 0 | 110050584 | 115884032 | 1005057 | 38 |
| churn-0 | 88080384 | 193991 | 7307594 | 1045462 | 878285 | 0 | 0 | 554 | 470 | 97506740 | 12 | 85932335 | 85932335 | 141699048 | 149946368 | 1141524 | 53 |
| checkpoint-0 | 88080384 | 193991 | 7307594 | 1045462 | 878285 | 0 | 0 | 554 | 470 | 97506740 | 13 | 0 | 85932335 | 124773472 | 133939200 | 821383 | 54 |
| folded-0 | 88080384 | 193991 | 7403944 | 1736637 | 878285 | 0 | 0 | 554 | 470 | 98294265 | 13 | 787525 | 86719860 | 114636608 | 122937344 | 679680 | 55 |
| before_vacuum-0 | 88080384 | 193991 | 7403944 | 1736637 | 878285 | 0 | 0 | 554 | 795 | 98294590 | 14 | 325 | 86720185 | 100354912 | 110419968 | 444618 | 56 |
| before_exhaustive-0 | 4194304 | 193991 | 7406025 | 1736637 | 878285 | 0 | 0 | 855 | 795 | 14410892 | 15 | -83883698 | 2836487 | 165237088 | 171745280 | 1524580 | 56 |
| before_final_gc-0 | 4194304 | 193991 | 7656225 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14664317 | 24 | 253425 | 3089912 | 154786528 | 160669696 | 1185078 | 57 |
| maintenance-0 | 4194304 | 193991 | 7656225 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14664317 | 24 | 0 | 3089912 | 136686704 | 143728640 | 1047213 | 58 |
| after_view_release | 4194304 | 193991 | 189498 | 691175 | 878285 | 0 | 0 | 1277 | 795 | 6149325 | 16 | -8514992 | -5425080 | 114970080 | 122413056 | 638602 | 59 |
| churn-1 | 8388608 | 256821 | 5064298 | 1012959 | 1058739 | 0 | 0 | 1277 | 795 | 15783497 | 17 | 9634172 | 4209092 | 158295384 | 162693120 | 903419 | 69 |
| checkpoint-1 | 8388608 | 256821 | 5064298 | 1012959 | 180466 | 0 | 0 | 1277 | 795 | 14905224 | 17 | -878273 | 3330819 | 111737000 | 125009920 | 455359 | 70 |
| folded-1 | 8388608 | 256821 | 5172888 | 1704134 | 180466 | 0 | 0 | 1277 | 795 | 15704989 | 17 | 799765 | 4130584 | 159115152 | 166903808 | 1022363 | 70 |
| before_vacuum-1 | 8388608 | 256821 | 5174367 | 1704134 | 180466 | 0 | 0 | 1277 | 946 | 15706619 | 17 | 1630 | 4132214 | 89890288 | 103301120 | 135234 | 71 |
| before_exhaustive-1 | 4194304 | 256821 | 5176422 | 1704134 | 180466 | 0 | 0 | 1794 | 946 | 11514887 | 18 | -4191732 | -59518 | 121250736 | 129449984 | 671529 | 71 |
| before_final_gc-1 | 4194304 | 256821 | 5408176 | 1704134 | 180466 | 0 | 0 | 4398 | 946 | 11749245 | 24 | 234358 | 174840 | 165444224 | 170868736 | 1225666 | 71 |
| maintenance-1 | 4194304 | 256821 | 190671 | 691175 | 180466 | 0 | 0 | 1540 | 946 | 5515923 | 16 | -6233322 | -6058482 | 110132128 | 120332288 | 617167 | 72 |
| churn-2 | 8388608 | 319524 | 5091725 | 1012959 | 360793 | 0 | 0 | 2276 | 946 | 15176831 | 20 | 9660908 | 3602426 | 123727168 | 135471104 | 649358 | 82 |
| checkpoint-2 | 8388608 | 319524 | 5091725 | 1012959 | 180339 | 0 | 0 | 2276 | 946 | 14996377 | 20 | -180454 | 3421972 | 154172528 | 163225600 | 1188366 | 82 |
| folded-2 | 8388608 | 319524 | 5201306 | 1704134 | 180339 | 0 | 0 | 2276 | 946 | 15797133 | 20 | 800756 | 4222728 | 119661480 | 131563520 | 628484 | 83 |
| before_vacuum-2 | 8388608 | 319524 | 5202783 | 1704134 | 180339 | 0 | 0 | 2276 | 1092 | 15798756 | 20 | 1623 | 4224351 | 153700552 | 161890304 | 1169654 | 83 |
| before_exhaustive-2 | 4194304 | 319524 | 5204838 | 1704134 | 180339 | 0 | 0 | 3012 | 1092 | 11607243 | 21 | -4191513 | 32838 | 98209296 | 112558080 | 191887 | 84 |
| before_final_gc-2 | 4194304 | 319524 | 5435056 | 1704134 | 180339 | 0 | 0 | 2296 | 1094 | 11836747 | 21 | 229504 | 262342 | 142053648 | 148611072 | 749270 | 84 |
| maintenance-2 | 4194304 | 319524 | 192178 | 691175 | 180339 | 0 | 0 | 1796 | 1094 | 5580410 | 16 | -6256337 | -5993995 | 173440648 | 178683904 | 1291890 | 84 |
| churn-3 | 8388608 | 382355 | 5053348 | 1012959 | 360794 | 0 | 0 | 2535 | 1094 | 15201693 | 20 | 9621283 | 3627288 | 166159080 | 171065344 | 925509 | 94 |
| checkpoint-3 | 8388608 | 382355 | 5053348 | 1012959 | 180467 | 0 | 0 | 2535 | 1094 | 15021366 | 20 | -180327 | 3446961 | 115554272 | 130097152 | 485296 | 95 |
| folded-3 | 8388608 | 382355 | 5164726 | 1704134 | 180467 | 0 | 0 | 2535 | 1094 | 15823919 | 20 | 802553 | 4249514 | 162148744 | 170950656 | 1055898 | 95 |
| before_vacuum-3 | 8388608 | 382355 | 5166205 | 1704134 | 180467 | 0 | 0 | 2535 | 1092 | 15825396 | 20 | 1477 | 4250991 | 91380856 | 107061248 | 124887 | 96 |
| before_exhaustive-3 | 4194304 | 382355 | 5168260 | 1704134 | 180467 | 0 | 0 | 3274 | 1092 | 11633886 | 21 | -4191510 | 59481 | 121981864 | 131858432 | 664778 | 96 |
| before_final_gc-3 | 4194304 | 382355 | 5398129 | 1704134 | 180467 | 0 | 0 | 2304 | 1094 | 11862787 | 21 | 228901 | 288382 | 166722752 | 172982272 | 1224007 | 96 |
| maintenance-3 | 4194304 | 382355 | 192148 | 691175 | 180467 | 0 | 0 | 1803 | 1094 | 5643346 | 16 | -6219441 | -5931059 | 108762752 | 120823808 | 569899 | 97 |
| churn-4 | 8388608 | 445187 | 5018044 | 1012959 | 360923 | 0 | 0 | 2544 | 1094 | 15229359 | 20 | 9586013 | 3654954 | 117975160 | 132186112 | 536646 | 107 |
| checkpoint-4 | 8388608 | 445187 | 5018044 | 1012959 | 180468 | 0 | 0 | 2544 | 1094 | 15048904 | 20 | -180455 | 3474499 | 149391728 | 159924224 | 1079260 | 107 |
| folded-4 | 8388608 | 445187 | 5127082 | 1704134 | 180468 | 0 | 0 | 2544 | 1094 | 15849117 | 20 | 800213 | 4274712 | 103565328 | 119414784 | 288961 | 108 |
| before_vacuum-4 | 8388608 | 445187 | 5128560 | 1704134 | 180468 | 0 | 0 | 2544 | 1094 | 15850595 | 20 | 1478 | 4276190 | 137747368 | 148357120 | 833697 | 108 |
| before_exhaustive-4 | 4194304 | 445187 | 5130614 | 1704134 | 180468 | 0 | 0 | 3285 | 1094 | 11659086 | 21 | -4191509 | 84681 | 168315304 | 175742976 | 1375337 | 108 |
| before_final_gc-4 | 4194304 | 445187 | 5360704 | 1704134 | 180468 | 0 | 0 | 2307 | 1094 | 11888198 | 21 | 229112 | 313793 | 119685488 | 132431872 | 622349 | 109 |
| maintenance-4 | 4194304 | 445187 | 192373 | 691175 | 180468 | 0 | 0 | 1805 | 1094 | 5706406 | 16 | -6181792 | -5867999 | 152071440 | 161161216 | 1168609 | 109 |
| reopen | 4194304 | 445187 | 192373 | 691175 | 12 | 0 | 0 | 1805 | 1094 | 5525950 | 16 | -180456 | -6048455 | 101415256 | 114057216 | 458820 | 110 |


| Operation | Calls |
| --- | --- |
| delete | 640 |
| indexed_update | 640 |
| ordinary_get | 640 |
| post_upsert_get | 640 |
| prepared_get | 640 |
| typed_insert | 640 |
| typed_replace | 640 |
| typed_upsert | 640 |

API durations (ns); aggregate maintenance includes its constituent stages. These overlapping timer columns must not be added again. All other raw API timers and attribution remain in the projection.

| Epoch | Maintenance | Flush | Checkpoint | Fold | Fold checkpoint | Overlay | Overlay checkpoint | Vlog GC | Vacuum | Exhaustive plan | Exhaustive work | Final refresh | Final typed GC | Final leaf GC |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 245955628 | 11130 | 6274871 | 16402339 | 6660024 | 14300 | 6130 | 12272879 | 39746254 | 1202781 | 146671449 | 11420771 | 1320542 | 352814 |
| 1 | 346069256 | 11320 | 11910665 | 14303248 | 6539013 | 13010 | 6001 | 9006277 | 81824701 | 1083920 | 135833613 | 9934157 | 4592194 | 50286667 |
| 2 | 352628510 | 11320 | 11023667 | 15897814 | 7194859 | 12840 | 5830 | 8491882 | 37320251 | 1117901 | 194448960 | 11302000 | 4529574 | 39589503 |
| 3 | 344532060 | 10420 | 10696963 | 13621102 | 7242720 | 12501 | 6020 | 8691674 | 37268760 | 1136321 | 188732445 | 11157318 | 4565504 | 39352771 |
| 4 | 406781614 | 11020 | 11886805 | 16558640 | 7187640 | 12520 | 5550 | 8384561 | 33669716 | 1098571 | 190720604 | 70426331 | 4458963 | 39568783 |

Typed reachability and unlink attribution are separate actual API cells. Sources/ref/segment classes may overlap; they are not unique retained bytes. No-op work is not reclamation.

| Stage | Eligible segments | Deleted segments | Retained segments | Eligible B | Deleted B | Retained B | Protected refs | Protected ref B | Mapped handles | Pinned refs | Pinned B | Rewrite debt B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0/before-fold | 0 | 0 | 1 | 0 | 0 | 1045462 | 896 | 1045462 | 128 | 128 | 725636 | 0 |
| epoch-0/reclaim-plan | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-0/reclaim-GC | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-1/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-1/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-1/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-2/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-2/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-2/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-3/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-3/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-3/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-4/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-4/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-4/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| after-view-release/reclaim-plan | 0 | 0 | 1 | 0 | 0 | 1736637 | 1 | 691175 | 0 | 0 | 0 | 1045462 |
| after-view-release/reclaim-GC | 1 | 0 | 2 | 1736637 | 0 | 2427812 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-0/final-GC | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-1/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-2/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-3/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-4/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| after-view-release/final-GC | 1 | 1 | 1 | 1736637 | 1736637 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |


| Stage | Decision | Plan ns | Probe ns | Rewrite ns | Checkpoint ns | GC ns | Candidate refs |
| --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | no_debt | 1153261 | 0 | 0 | 0 | 1023690 | 896 |
| epoch-1 | eligible | 1250132 | 291982 | 7667605 | 6508922 | 4351122 | 769 |
| epoch-2 | eligible | 1219892 | 267052 | 8073738 | 7162649 | 4287742 | 769 |
| epoch-3 | eligible | 1210211 | 263652 | 8428902 | 7250970 | 4223690 | 769 |
| epoch-4 | eligible | 1251662 | 271012 | 9211540 | 7107288 | 4282932 | 769 |
| after-view-release | eligible | 504525 | 351703 | 31391094 | 6678224 | 583435 | 897 |


| Epoch | Vlog segments | Active | Referenced | Protected | Pending | Eligible | Deleted | Eligible B | Deleted B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-1 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-3 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-4 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |


| Final stage | Leaf GC ns | Eligible generations | Deleted generations | Files deleted | Leaf bytes deleted | Revision GC unsupported | Revisions total | Protected revisions | Eligible revisions | Deleted revisions | Revision bytes deleted |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | 352814 | 0 | 0 | 0 | 0 | False | 8 | 8 | 0 | 0 | 0 |
| epoch-1 | 50286667 | 2 | 2 | 2 | 5175369 | False | 7 | 2 | 5 | 5 | 4464 |
| epoch-2 | 39589503 | 2 | 2 | 2 | 5200442 | False | 4 | 2 | 2 | 2 | 2112 |
| epoch-3 | 39352771 | 2 | 2 | 2 | 5163824 | False | 4 | 2 | 2 | 2 | 2120 |
| epoch-4 | 39568783 | 2 | 2 | 2 | 5126165 | False | 4 | 2 | 2 | 2 | 2122 |
| after-view-release | 66736185 | 2 | 2 | 2 | 7408802 | False | 11 | 2 | 9 | 9 | 5329 |

Refresh snapshots expose both durable slots, root IDs and command coverage. Duration repeats across its before/after pair; count it once. Full root records and all slot metadata remain in the projection.

| Stage | Boundary | Refresh ns | CommitSeq | User root | System root | AppliedLSN | NextLSN | Selected slot | Slot0 CommitSeq | Slot1 CommitSeq |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | before | 11420771 | 775 | 16 | 15 | 769 | 770 | 1 | 772 | 775 |
| epoch-0 | after | 11420771 | 776 | 16 | 15 | 769 | 770 | 0 | 776 | 775 |
| epoch-1 | before | 9934157 | 1423 | 16 | 15 | 1409 | 1410 | 1 | 1421 | 1423 |
| epoch-1 | after | 9934157 | 1424 | 16 | 15 | 1409 | 1410 | 0 | 1424 | 1423 |
| epoch-2 | before | 11302000 | 2069 | 16 | 15 | 2049 | 2050 | 1 | 2067 | 2069 |
| epoch-2 | after | 11302000 | 2070 | 16 | 15 | 2049 | 2050 | 0 | 2070 | 2069 |
| epoch-3 | before | 11157318 | 2715 | 16 | 15 | 2689 | 2690 | 1 | 2713 | 2715 |
| epoch-3 | after | 11157318 | 2716 | 16 | 15 | 2689 | 2690 | 0 | 2716 | 2715 |
| epoch-4 | before | 70426331 | 3361 | 16 | 15 | 3329 | 3330 | 1 | 3359 | 3361 |
| epoch-4 | after | 70426331 | 3362 | 16 | 15 | 3329 | 3330 | 0 | 3362 | 3361 |
| after-view-release | before | 3274801 | 777 | 16 | 77 | 769 | 770 | 1 | 776 | 777 |
| after-view-release | after | 3274801 | 778 | 16 | 77 | 769 | 770 | 0 | 778 | 777 |

Recorded exhaustive maintenance owner, work limits and remaining debt follow. Work limits are not storage-capacity bounds. An internal replay-inline owner classification does not change the public direct-backend opener. All audit/rewrite/leaf/index phases and counters remain in the projection. Existing raw ratio fields are retained there rather than interpreted in these tables.

| Epoch | Owner | Options / work limits | Remaining debt | Fully compacted | Policy fully compacted | Byte minimized |
| --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"leaf_generation","index_vacuum_required":true,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":0,"leaf_gc_generations":0,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-1 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5175369,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-2 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5200442,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-3 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5163824,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-4 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5126165,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |


</details>


<details><summary>Process 5: complete phase trajectory and maintenance</summary>

All tables in this block are this process's actual cells. Heap cuts are census-boundary samples.

| Phase | index | persistent_vlog | persistent_leaf_log | typed_assets | redo_wal | dictionary_store | template_store | immutable_manifest_metadata | other | Total B | Files | Adjacent total ΔB | Since ingest ΔB | HeapAlloc B | HeapInuse B | HeapObjects | NumGC |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ingest | 8388608 | 131289 | 1627902 | 725636 | 699917 | 0 | 0 | 554 | 470 | 11574376 | 12 | — | 0 | 108418168 | 114262016 | 965548 | 38 |
| churn-0 | 88080384 | 193991 | 7307390 | 1045462 | 878285 | 0 | 0 | 554 | 470 | 97506536 | 12 | 85932160 | 85932160 | 139960504 | 148332544 | 1107557 | 53 |
| checkpoint-0 | 88080384 | 193991 | 7307390 | 1045462 | 878285 | 0 | 0 | 554 | 470 | 97506536 | 13 | 0 | 85932160 | 119944608 | 129646592 | 735886 | 54 |
| folded-0 | 88080384 | 193991 | 7406065 | 1736637 | 878285 | 0 | 0 | 554 | 470 | 98296386 | 13 | 789850 | 86722010 | 113510120 | 122208256 | 652776 | 55 |
| before_vacuum-0 | 88080384 | 193991 | 7406065 | 1736637 | 878285 | 0 | 0 | 554 | 795 | 98296711 | 14 | 325 | 86722335 | 102927896 | 113311744 | 448149 | 56 |
| before_exhaustive-0 | 4194304 | 193991 | 7408146 | 1736637 | 878285 | 0 | 0 | 855 | 795 | 14413013 | 15 | -83883698 | 2838637 | 167815952 | 174497792 | 1528116 | 56 |
| before_final_gc-0 | 4194304 | 193991 | 7658346 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14666438 | 24 | 253425 | 3092062 | 154452960 | 160448512 | 1185058 | 57 |
| maintenance-0 | 4194304 | 193991 | 7658346 | 1736637 | 878285 | 0 | 0 | 4080 | 795 | 14666438 | 24 | 0 | 3092062 | 138010976 | 145301504 | 1064980 | 58 |
| after_view_release | 4194304 | 193991 | 189498 | 691175 | 878285 | 0 | 0 | 1277 | 795 | 6149325 | 16 | -8517113 | -5425051 | 115549568 | 122896384 | 641771 | 59 |
| churn-1 | 8388608 | 256821 | 5064747 | 1012959 | 1058739 | 0 | 0 | 1277 | 795 | 15783946 | 17 | 9634621 | 4209570 | 171128344 | 175710208 | 980978 | 69 |
| checkpoint-1 | 8388608 | 256821 | 5064747 | 1012959 | 180466 | 0 | 0 | 1277 | 795 | 14905673 | 17 | -878273 | 3331297 | 117463672 | 128966656 | 604343 | 70 |
| folded-1 | 8388608 | 256821 | 5174350 | 1704134 | 180466 | 0 | 0 | 1277 | 795 | 15706451 | 17 | 800778 | 4132075 | 165573184 | 172523520 | 1171380 | 70 |
| before_vacuum-1 | 8388608 | 256821 | 5175829 | 1704134 | 180466 | 0 | 0 | 1277 | 946 | 15708081 | 17 | 1630 | 4133705 | 113459096 | 124035072 | 519880 | 71 |
| before_exhaustive-1 | 4194304 | 256821 | 5177884 | 1704134 | 180466 | 0 | 0 | 1794 | 946 | 11516349 | 18 | -4191732 | -58027 | 143914816 | 151158784 | 1056129 | 71 |
| before_final_gc-1 | 4194304 | 256821 | 5409638 | 1704134 | 180466 | 0 | 0 | 4398 | 946 | 11750707 | 24 | 234358 | 176331 | 97099232 | 110739456 | 203396 | 72 |
| maintenance-1 | 4194304 | 256821 | 190671 | 691175 | 180466 | 0 | 0 | 1540 | 946 | 5515923 | 16 | -6234784 | -6058453 | 129416904 | 137568256 | 744491 | 72 |
| churn-2 | 8388608 | 319524 | 5094590 | 1012959 | 360793 | 0 | 0 | 2276 | 946 | 15179696 | 20 | 9663773 | 3605320 | 128942728 | 138870784 | 691901 | 82 |
| checkpoint-2 | 8388608 | 319524 | 5094590 | 1012959 | 180339 | 0 | 0 | 2276 | 946 | 14999242 | 20 | -180454 | 3424866 | 159387832 | 167034880 | 1230909 | 82 |
| folded-2 | 8388608 | 319524 | 5201248 | 1704134 | 180339 | 0 | 0 | 2276 | 946 | 15797075 | 20 | 797833 | 4222699 | 120537824 | 132038656 | 644662 | 83 |
| before_vacuum-2 | 8388608 | 319524 | 5202725 | 1704134 | 180339 | 0 | 0 | 2276 | 1092 | 15798698 | 20 | 1623 | 4224322 | 153753888 | 161751040 | 1185814 | 83 |
| before_exhaustive-2 | 4194304 | 319524 | 5204780 | 1704134 | 180339 | 0 | 0 | 3012 | 1092 | 11607185 | 21 | -4191513 | 32809 | 99451384 | 111935488 | 253079 | 84 |
| before_final_gc-2 | 4194304 | 319524 | 5434998 | 1704134 | 180339 | 0 | 0 | 2296 | 1094 | 11836689 | 21 | 229504 | 262313 | 144089832 | 150405120 | 810472 | 84 |
| maintenance-2 | 4194304 | 319524 | 192178 | 691175 | 180339 | 0 | 0 | 1796 | 1094 | 5580410 | 16 | -6256279 | -5993966 | 175476816 | 180486144 | 1353095 | 84 |
| churn-3 | 8388608 | 382355 | 5054410 | 1012959 | 360794 | 0 | 0 | 2535 | 1094 | 15202755 | 20 | 9622345 | 3628379 | 172681344 | 177004544 | 978532 | 94 |
| checkpoint-3 | 8388608 | 382355 | 5054410 | 1012959 | 180467 | 0 | 0 | 2535 | 1094 | 15022428 | 20 | -180327 | 3448052 | 119581576 | 132571136 | 598953 | 95 |
| folded-3 | 8388608 | 382355 | 5161882 | 1704134 | 180467 | 0 | 0 | 2535 | 1094 | 15821075 | 20 | 798647 | 4246699 | 167729624 | 175382528 | 1169564 | 95 |
| before_vacuum-3 | 8388608 | 382355 | 5163361 | 1704134 | 180467 | 0 | 0 | 2535 | 1092 | 15822552 | 20 | 1477 | 4248176 | 109079648 | 121536512 | 445070 | 96 |
| before_exhaustive-3 | 4194304 | 382355 | 5165416 | 1704134 | 180467 | 0 | 0 | 3274 | 1092 | 11631042 | 21 | -4191510 | 56666 | 140531608 | 149331968 | 984972 | 96 |
| before_final_gc-3 | 4194304 | 382355 | 5395285 | 1704134 | 180467 | 0 | 0 | 2304 | 1094 | 11859943 | 21 | 228901 | 285567 | 185202432 | 191250432 | 1544162 | 96 |
| maintenance-3 | 4194304 | 382355 | 192148 | 691175 | 180467 | 0 | 0 | 1803 | 1094 | 5643346 | 16 | -6216597 | -5931030 | 122033568 | 132997120 | 649569 | 97 |
| churn-4 | 8388608 | 445187 | 5019263 | 1012959 | 360923 | 0 | 0 | 2544 | 1094 | 15230578 | 20 | 9587232 | 3656202 | 118395968 | 133365760 | 517900 | 107 |
| checkpoint-4 | 8388608 | 445187 | 5019263 | 1012959 | 180468 | 0 | 0 | 2544 | 1094 | 15050123 | 20 | -180455 | 3475747 | 148952136 | 160129024 | 1060495 | 107 |
| folded-4 | 8388608 | 445187 | 5126768 | 1704134 | 180468 | 0 | 0 | 2544 | 1094 | 15848803 | 20 | 798680 | 4274427 | 94137976 | 112730112 | 185731 | 108 |
| before_vacuum-4 | 8388608 | 445187 | 5128246 | 1704134 | 180468 | 0 | 0 | 2544 | 1094 | 15850281 | 20 | 1478 | 4275905 | 128322200 | 139902976 | 730478 | 108 |
| before_exhaustive-4 | 4194304 | 445187 | 5130300 | 1704134 | 180468 | 0 | 0 | 3285 | 1094 | 11658772 | 21 | -4191509 | 84396 | 159787944 | 167673856 | 1272152 | 108 |
| before_final_gc-4 | 4194304 | 445187 | 5360390 | 1704134 | 180468 | 0 | 0 | 2307 | 1094 | 11887884 | 21 | 229112 | 313508 | 121665616 | 133971968 | 648854 | 109 |
| maintenance-4 | 4194304 | 445187 | 192373 | 691175 | 180468 | 0 | 0 | 1805 | 1094 | 5706406 | 16 | -6181478 | -5867970 | 153183248 | 162144256 | 1195076 | 109 |
| reopen | 4194304 | 445187 | 192373 | 691175 | 12 | 0 | 0 | 1805 | 1094 | 5525950 | 16 | -180456 | -6048426 | 105087640 | 116940800 | 541834 | 110 |


| Operation | Calls |
| --- | --- |
| delete | 640 |
| indexed_update | 640 |
| ordinary_get | 640 |
| post_upsert_get | 640 |
| prepared_get | 640 |
| typed_insert | 640 |
| typed_replace | 640 |
| typed_upsert | 640 |

API durations (ns); aggregate maintenance includes its constituent stages. These overlapping timer columns must not be added again. All other raw API timers and attribution remain in the projection.

| Epoch | Maintenance | Flush | Checkpoint | Fold | Fold checkpoint | Overlay | Overlay checkpoint | Vlog GC | Vacuum | Exhaustive plan | Exhaustive work | Final refresh | Final typed GC | Final leaf GC |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 245568375 | 10630 | 6785845 | 16040175 | 6611324 | 14160 | 5280 | 13946475 | 38744885 | 1112191 | 145057983 | 12339969 | 1116271 | 365394 |
| 1 | 300928798 | 10500 | 11778554 | 13854904 | 7173160 | 12090 | 4670 | 8292250 | 36768735 | 875459 | 133668031 | 12042906 | 4470444 | 50078474 |
| 2 | 362459175 | 8640 | 11871085 | 14458880 | 7207440 | 10321 | 4830 | 8293890 | 38322471 | 958289 | 203767910 | 12827503 | 4472534 | 38866746 |
| 3 | 343459031 | 9150 | 10737003 | 13844943 | 7100059 | 9991 | 4220 | 8296030 | 36397022 | 990839 | 190132240 | 9649213 | 4755026 | 39272540 |
| 4 | 347802723 | 10561 | 11759163 | 15051256 | 7125598 | 12370 | 5510 | 8406912 | 47619760 | 978890 | 178158302 | 9832445 | 4496434 | 39068568 |

Typed reachability and unlink attribution are separate actual API cells. Sources/ref/segment classes may overlap; they are not unique retained bytes. No-op work is not reclamation.

| Stage | Eligible segments | Deleted segments | Retained segments | Eligible B | Deleted B | Retained B | Protected refs | Protected ref B | Mapped handles | Pinned refs | Pinned B | Rewrite debt B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0/before-fold | 0 | 0 | 1 | 0 | 0 | 1045462 | 896 | 1045462 | 128 | 128 | 725636 | 0 |
| epoch-0/reclaim-plan | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-0/reclaim-GC | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-1/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-1/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-1/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-2/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-2/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-2/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-3/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-3/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-3/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-4/before-fold | 0 | 0 | 2 | 0 | 0 | 1012959 | 769 | 1012959 | 0 | 0 | 0 | 0 |
| epoch-4/reclaim-plan | 1 | 0 | 2 | 691175 | 0 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 321784 |
| epoch-4/reclaim-GC | 2 | 1 | 2 | 1704134 | 691175 | 1704134 | 1 | 691175 | 0 | 0 | 0 | 0 |
| after-view-release/reclaim-plan | 0 | 0 | 1 | 0 | 0 | 1736637 | 1 | 691175 | 0 | 0 | 0 | 1045462 |
| after-view-release/reclaim-GC | 1 | 0 | 2 | 1736637 | 0 | 2427812 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-0/final-GC | 0 | 0 | 1 | 0 | 0 | 1736637 | 897 | 1736637 | 128 | 128 | 725636 | 0 |
| epoch-1/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-2/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-3/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| epoch-4/final-GC | 1 | 1 | 1 | 1012959 | 1012959 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |
| after-view-release/final-GC | 1 | 1 | 1 | 1736637 | 1736637 | 691175 | 1 | 691175 | 0 | 0 | 0 | 0 |


| Stage | Decision | Plan ns | Probe ns | Rewrite ns | Checkpoint ns | GC ns | Candidate refs |
| --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | no_debt | 1102450 | 0 | 0 | 0 | 978280 | 896 |
| epoch-1 | eligible | 1183331 | 258653 | 8586892 | 7081479 | 4177760 | 769 |
| epoch-2 | eligible | 1139621 | 263082 | 8063428 | 7038418 | 4267221 | 769 |
| epoch-3 | eligible | 1149301 | 283133 | 8962406 | 7001278 | 4273641 | 769 |
| epoch-4 | eligible | 1181761 | 269103 | 12021306 | 6969667 | 4225971 | 769 |
| after-view-release | eligible | 453454 | 349853 | 27627278 | 6491092 | 593156 | 897 |


| Epoch | Vlog segments | Active | Referenced | Protected | Pending | Eligible | Deleted | Eligible B | Deleted B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-1 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-3 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| epoch-4 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |


| Final stage | Leaf GC ns | Eligible generations | Deleted generations | Files deleted | Leaf bytes deleted | Revision GC unsupported | Revisions total | Protected revisions | Eligible revisions | Deleted revisions | Revision bytes deleted |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | 365394 | 0 | 0 | 0 | 0 | False | 8 | 8 | 0 | 0 | 0 |
| epoch-1 | 50078474 | 2 | 2 | 2 | 5176831 | False | 7 | 2 | 5 | 5 | 4464 |
| epoch-2 | 38866746 | 2 | 2 | 2 | 5200384 | False | 4 | 2 | 2 | 2 | 2112 |
| epoch-3 | 39272540 | 2 | 2 | 2 | 5160980 | False | 4 | 2 | 2 | 2 | 2120 |
| epoch-4 | 39068568 | 2 | 2 | 2 | 5125851 | False | 4 | 2 | 2 | 2 | 2122 |
| after-view-release | 66672104 | 2 | 2 | 2 | 7410923 | False | 11 | 2 | 9 | 9 | 5329 |

Refresh snapshots expose both durable slots, root IDs and command coverage. Duration repeats across its before/after pair; count it once. Full root records and all slot metadata remain in the projection.

| Stage | Boundary | Refresh ns | CommitSeq | User root | System root | AppliedLSN | NextLSN | Selected slot | Slot0 CommitSeq | Slot1 CommitSeq |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | before | 12339969 | 775 | 16 | 15 | 769 | 770 | 1 | 772 | 775 |
| epoch-0 | after | 12339969 | 776 | 16 | 15 | 769 | 770 | 0 | 776 | 775 |
| epoch-1 | before | 12042906 | 1423 | 16 | 15 | 1409 | 1410 | 1 | 1421 | 1423 |
| epoch-1 | after | 12042906 | 1424 | 16 | 15 | 1409 | 1410 | 0 | 1424 | 1423 |
| epoch-2 | before | 12827503 | 2069 | 16 | 15 | 2049 | 2050 | 1 | 2067 | 2069 |
| epoch-2 | after | 12827503 | 2070 | 16 | 15 | 2049 | 2050 | 0 | 2070 | 2069 |
| epoch-3 | before | 9649213 | 2715 | 16 | 15 | 2689 | 2690 | 1 | 2713 | 2715 |
| epoch-3 | after | 9649213 | 2716 | 16 | 15 | 2689 | 2690 | 0 | 2716 | 2715 |
| epoch-4 | before | 9832445 | 3361 | 16 | 15 | 3329 | 3330 | 1 | 3359 | 3361 |
| epoch-4 | after | 9832445 | 3362 | 16 | 15 | 3329 | 3330 | 0 | 3362 | 3361 |
| after-view-release | before | 18583740 | 777 | 16 | 77 | 769 | 770 | 1 | 776 | 777 |
| after-view-release | after | 18583740 | 778 | 16 | 77 | 769 | 770 | 0 | 778 | 777 |

Recorded exhaustive maintenance owner, work limits and remaining debt follow. Work limits are not storage-capacity bounds. An internal replay-inline owner classification does not change the public direct-backend opener. All audit/rewrite/leaf/index phases and counters remain in the projection. Existing raw ratio fields are retained there rather than interpreted in these tables.

| Epoch | Owner | Options / work limits | Remaining debt | Fully compacted | Policy fully compacted | Byte minimized |
| --- | --- | --- | --- | --- | --- | --- |
| epoch-0 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"leaf_generation","index_vacuum_required":true,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":0,"leaf_gc_generations":0,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-1 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5176831,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-2 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5200384,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-3 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5160980,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |
| epoch-4 | {"Detail":"internally owned replay-inline wrapper exposes a compact handoff restore capability","Lifecycle":"quiesced maintenance","OwnerClass":"internal owner hidden by wrapper","Replaceable":true,"RequiresQuiescence":false,"Status":"supported-target"} | {"leaf_pack_max_bytes_to_copy_per_pass":1048576,"leaf_pack_max_passes":4,"mode":"exhaustive","sync_each_phase":true,"unsafe_value_log_reclaim_fenced_unreferenced":false,"value_log_rewrite_batch_size":32} | {"index_vacuum_collection_root_pages":11,"index_vacuum_collection_root_span":11,"index_vacuum_freelist_reclaimable_pages":0,"index_vacuum_reason":"none","index_vacuum_required":false,"index_vacuum_total_pages":38,"index_vacuum_user_pages":1,"index_vacuum_user_span":1,"leaf_gc_bytes":5125851,"leaf_gc_generations":2,"leaf_pack_bytes":0,"leaf_pack_generations":0,"value_log_gc_bytes":0,"value_log_gc_segments":0,"value_log_rewrite_bytes":0,"value_log_rewrite_segments":0,"zero_byte_value_log_files":0} | False | False | False |


</details>

Held-view assertions, release/lifetime checks, actual deleted-asset checks and final reopen row/posting parity are performed by the benchmark source. This formatter checks the emitted raw schema and log hashes, not those assertions independently. Zero active handles immediately after closing the held view is a source assertion, not a new emitted sample.
The independently pinned frozen validator and coordinator own qualification. The linked validation log is supplied by the caller; this formatter does not infer its verdict. Sampled trajectories establish finite observations only; lawful current/old-view/recovery retention and maintenance debt remain explicit, with no infinite-growth or total physical-bound claim.

