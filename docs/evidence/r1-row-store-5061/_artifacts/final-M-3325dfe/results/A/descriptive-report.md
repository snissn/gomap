# A packet descriptive report

Original packet SHA256: `e5b83232a75ecb0a30b2859dd64e0ee2e1cba2a31cb922c05d1b9384e048a17b`; source: `3325dfe77940fec8587d8b61b1ac4e0b2f72caca`.

Declared configuration: `{"batch_size": 32, "documents": 4096, "durability": "durable", "engines": ["json", "template-v1", "bson", "typed-row", "sqlite-json", "sqlite-row"], "operations": 1000, "qualification": "retained", "read_state": "flushed", "repetitions": 5}`.

Descriptive output only; no acceptance, compatibility or speedup claim.

- Go bytes/call and objects/call exclude SQLite C allocations; they are not total memory.
- Latency columns are medians of per-repetition p50/p95/p99, not pooled operation percentiles.
- Each metric median is computed independently; median calls/s need not be the reciprocal of median ns/call.
- Rows/s is derived within each repetition from actual rows/calls and calls/s; zero-row phases have no rows/s.
- Groups with different denominators, skip state/reason or storage boundary remain separate.
- Storage stats are raw declared strings, not additive components; no undeclared component is invented.
- Heap-after is a runtime snapshot including harness state, not retained engine-only memory.
- Above-15% throughput spread is descriptive timing inconclusive, never acceptance.
- This helper does not establish source trust, oracle acceptance, compatibility or speedup.

## Cells and capabilities

| Engine | Observed/declared repetitions | Unsupported/rejected | Capabilities by repetition |
| --- | ---: | --- | --- |
| json | 5/5 | [] | [{"capabilities": {"ordinary_point": "owned_complete_GetInto", "ordinary_range": "complete_retained_document", "range_decomposition": "quiescent_ids_then_prepared_full_fetch"}, "repetitions": [0, 1, 2, 3, 4]}] |
| template-v1 | 5/5 | [] | [{"capabilities": {"ordinary_point": "owned_complete_GetInto", "ordinary_range": "complete_retained_document", "range_decomposition": "quiescent_ids_then_prepared_full_fetch"}, "repetitions": [0, 1, 2, 3, 4]}] |
| bson | 5/5 | [] | [{"capabilities": {"ordinary_point": "owned_complete_GetInto", "ordinary_range": "complete_retained_document", "range_decomposition": "quiescent_ids_then_prepared_full_fetch"}, "repetitions": [0, 1, 2, 3, 4]}] |
| typed-row | 5/5 | [] | [{"capabilities": {"ordinary_point": "owned_complete_GetInto", "ordinary_range": "complete_typed_row", "range_decomposition": "quiescent_ids_then_prepared_full_fetch"}, "repetitions": [0, 1, 2, 3, 4]}] |
| sqlite-json | 5/5 | [] | [{"capabilities": {"ordinary_point": "owned_complete_SQL_select", "ordinary_range": "complete_SQL_row", "range_decomposition": "quiescent_ids_then_prepared_full_fetch"}, "repetitions": [0, 1, 2, 3, 4]}] |
| sqlite-row | 5/5 | [] | [{"capabilities": {"ordinary_point": "owned_complete_SQL_select", "ordinary_range": "complete_SQL_row", "range_decomposition": "quiescent_ids_then_prepared_full_fetch"}, "repetitions": [0, 1, 2, 3, 4]}] |

## Read-state transition

| Engine | Phase | Reps | Calls / rows per repetition | ns/call median [min–max] | Calls/s | Rows/s | Go B/call | Go objects/call | p50 / p95 / p99 ns medians | Calls/s spread | State / skip reason |
| --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | --- |
| json | read_state_transition | 5 | 1 / 0 | 6,715,715 [6,319,941–6,765,935] | 148.904 | — | 8,943,352 | 3,577 | 6,715,715 / 6,715,715 / 6,715,715 | 7.00% | within_15pct |
| template-v1 | read_state_transition | 5 | 1 / 0 | 6,660 [5,070–7,130] | 150,150.15 | — | 3,544 | 4 | 6,660 / 6,660 / 6,660 | 37.95% | inconclusive_above_15pct |
| bson | read_state_transition | 5 | 1 / 0 | 5,756,125 [5,510,302–6,553,733] | 173.728 | — | 6,973,096 | 3,067 | 5,756,125 / 5,756,125 / 5,756,125 | 16.63% | inconclusive_above_15pct |
| typed-row | read_state_transition | 5 | 1 / 0 | 5,130 [5,050–7,170] | 194,931.774 | — | 3,544 | 4 | 5,130 / 5,130 / 5,130 | 30.04% | inconclusive_above_15pct |
| sqlite-json | read_state_transition | 5 | 1 / 0 | 380 [350–420] | 2,631,578.947 | — | 0 | 0 | 380 / 380 / 380 | 18.10% | inconclusive_above_15pct |
| sqlite-row | read_state_transition | 5 | 1 / 0 | 390 [380–580] | 2,564,102.564 | — | 0 | 0 | 390 / 390 / 390 | 35.39% | inconclusive_above_15pct |

## View setup

| Engine | Phase | Reps | Calls / rows per repetition | ns/call median [min–max] | Calls/s | Rows/s | Go B/call | Go objects/call | p50 / p95 / p99 ns medians | Calls/s spread | State / skip reason |
| --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | --- |
| json | read_view_open_flush_setup | 5 | 1 / 0 | 14,390 [11,300–20,840] | 69,492.703 | — | 3,528 | 12 | 14,390 / 14,390 / 14,390 | 58.30% | inconclusive_above_15pct |
| template-v1 | read_view_open_flush_setup | 5 | 1 / 0 | 12,990 [10,270–15,400] | 76,982.294 | — | 2,096 | 22 | 12,990 / 12,990 / 12,990 | 42.13% | inconclusive_above_15pct |
| bson | read_view_open_flush_setup | 5 | 1 / 0 | 12,590 [10,691–14,260] | 79,428.118 | — | 3,512 | 11 | 12,590 / 12,590 / 12,590 | 29.47% | inconclusive_above_15pct |
| typed-row | read_view_open_flush_setup | 5 | 1 / 0 | 7,770 [7,060–9,010] | 128,700.129 | — | 1,000 | 9 | 7,770 / 7,770 / 7,770 | 23.82% | inconclusive_above_15pct |
| sqlite-json | read_view_open_flush_setup | 5 | 1 / 0 | 3,420 [3,140–4,540] | 292,397.661 | — | 856 | 12 | 3,420 / 3,420 / 3,420 | 33.59% | inconclusive_above_15pct |
| sqlite-row | read_view_open_flush_setup | 5 | 1 / 0 | 3,990 [3,760–4,160] | 250,626.566 | — | 872 | 12 | 3,990 / 3,990 / 3,990 | 10.20% | within_15pct |

## First fetch

| Engine | Phase | Reps | Calls / rows per repetition | ns/call median [min–max] | Calls/s | Rows/s | Go B/call | Go objects/call | p50 / p95 / p99 ns medians | Calls/s spread | State / skip reason |
| --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | --- |
| json | first_complete_batch | 5 | 1 / 32 | 27,220 [23,000–35,160] | 36,737.693 | 1,175,606.172 | 43,528 | 56 | 27,220 / 27,220 / 27,220 | 40.93% | inconclusive_above_15pct |
| template-v1 | first_complete_batch | 5 | 1 / 32 | 89,251 [79,961–130,132] | 11,204.356 | 358,539.4 | 100,264 | 1,623 | 89,251 / 89,251 / 89,251 | 43.03% | inconclusive_above_15pct |
| bson | first_complete_batch | 5 | 1 / 32 | 130,432 [128,541–139,121] | 7,666.83 | 245,338.567 | 129,992 | 2,502 | 130,432 / 130,432 / 130,432 | 7.72% | within_15pct |
| typed-row | first_complete_batch | 5 | 1 / 32 | 226,592 [215,372–257,092] | 4,413.218 | 141,222.991 | 225,904 | 2,194 | 226,592 / 226,592 / 226,592 | 17.07% | inconclusive_above_15pct |
| sqlite-json | first_complete_batch | 5 | 1 / 32 | 82,521 [60,961–105,161] | 12,118.128 | 387,780.08 | 46,472 | 485 | 82,521 / 82,521 / 82,521 | 56.90% | inconclusive_above_15pct |
| sqlite-row | first_complete_batch | 5 | 1 / 32 | 261,722 [256,702–298,043] | 3,820.848 | 122,267.138 | 209,960 | 3,553 | 261,722 / 261,722 / 261,722 | 14.14% | within_15pct |

## Load

| Engine | Phase | Reps | Calls / rows per repetition | ns/call median [min–max] | Calls/s | Rows/s | Go B/call | Go objects/call | p50 / p95 / p99 ns medians | Calls/s spread | State / skip reason |
| --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | --- |
| json | load | 5 | 128 / 4096 | 6,876,098.656 [3,966,066.641–7,036,744.477] | 145.431 | 4,653.802 | 97,235.438 | 205.969 | 6,771,236 / 7,049,608 / 8,207,750 | 75.66% | inconclusive_above_15pct |
| template-v1 | load | 5 | 128 / 4096 | 13,228,526.344 [7,010,687.789–16,936,764.445] | 75.594 | 2,419.015 | 747,556.688 | 3,665.281 | 7,869,716 / 39,686,363 / 65,554,271 | 110.59% | inconclusive_above_15pct |
| bson | load | 5 | 128 / 4096 | 7,156,693.125 [3,888,085.664–7,484,177.484] | 139.729 | 4,471.339 | 92,298.062 | 202.125 | 6,895,207 / 7,337,581 / 9,115,678 | 88.44% | inconclusive_above_15pct |
| typed-row | load | 5 | 128 / 4096 | 24,594,120.836 [13,225,063.008–31,788,660.711] | 40.66 | 1,301.124 | 1,190,029.438 | 5,815.516 | 25,760,919 / 48,418,537 / 85,591,116 | 108.60% | inconclusive_above_15pct |
| sqlite-json | load | 5 | 128 / 4096 | 7,086,776.945 [5,732,536.25–7,798,533.992] | 141.108 | 4,515.452 | 29,476.5 | 340.133 | 6,992,838 / 7,354,761 / 8,526,422 | 32.75% | inconclusive_above_15pct |
| sqlite-row | load | 5 | 128 / 4096 | 7,465,582.25 [6,250,576.922–7,931,894.195] | 133.948 | 4,286.337 | 107,425.688 | 2,323.672 | 7,172,839 / 8,677,994 / 9,009,307 | 25.32% | inconclusive_above_15pct |

## Read

| Engine | Phase | Reps | Calls / rows per repetition | ns/call median [min–max] | Calls/s | Rows/s | Go B/call | Go objects/call | p50 / p95 / p99 ns medians | Calls/s spread | State / skip reason |
| --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | --- |
| json | point_get_into_complete | 5 | 1000 / 1000 | 1,130.003 [1,088.352–1,349.062] | 884,953.403 | 884,953.403 | 1,160 | 6 | 1,020 / 1,540 / 2,410 | 20.06% | inconclusive_above_15pct |
| json | point_complete | 5 | 1000 / 1000 | 944.337 [921.008–1,065.352] | 1,058,944 | 1,058,944 | 832 | 10 | 871 / 1,320 / 1,600 | 13.89% | within_15pct |
| json | batch_complete | 5 | 1000 / 32000 | 15,718.329 [13,433.479–18,105.204] | 63,619.994 | 2,035,839.815 | 43,328 | 57 | 13,900 / 20,050 / 24,991 | 30.19% | inconclusive_above_15pct |
| json | range_complete | 5 | 1000 / 10000 | 7,221.393 [6,602.397–8,233.521] | 138,477.438 | 1,384,774.378 | 13,103.864 | 51.008 | 6,770 / 9,530 / 14,430 | 21.67% | inconclusive_above_15pct |
| json | range_public_complete | 5 | 1000 / 10000 | 7,135.096 [6,955.26–7,883.037] | 140,152.284 | 1,401,522.839 | 5,920 | 45 | 7,040 / 7,770 / 10,180 | 12.07% | within_15pct |
| template-v1 | point_get_into_complete | 5 | 1000 / 1000 | 4,725.424 [4,673.91–4,922.486] | 211,621.222 | 211,621.222 | 5,231.256 | 84.491 | 4,460 / 5,670 / 10,640 | 5.11% | within_15pct |
| template-v1 | point_complete | 5 | 1000 / 1000 | 2,771.748 [2,749.73–2,831.046] | 360,783.159 | 360,783.159 | 2,719.792 | 57.987 | 2,710 / 3,270 / 4,090 | 2.90% | within_15pct |
| template-v1 | batch_complete | 5 | 1000 / 32000 | 68,102.335 [66,951.225–68,610.082] | 14,683.784 | 469,881.099 | 98,447.84 | 1,591.641 | 65,940 / 73,641 / 81,750 | 2.46% | within_15pct |
| template-v1 | range_complete | 5 | 1000 / 10000 | 24,892.432 [24,357.925–27,867.896] | 40,172.853 | 401,728.525 | 31,981.72 | 531.033 | 24,311 / 28,640 / 31,090 | 12.87% | within_15pct |
| template-v1 | range_public_complete | 5 | 1000 / 10000 | 26,315.585 [26,041.62–27,745.558] | 38,000.295 | 380,002.953 | 27,778.016 | 555.5 | 26,070 / 29,310 / 31,960 | 6.21% | within_15pct |
| bson | point_get_into_complete | 5 | 1000 / 1000 | 4,715.524 [4,637.563–5,671.646] | 212,065.51 | 212,065.51 | 3,902.072 | 82.511 | 4,520 / 5,390 / 8,640 | 18.54% | inconclusive_above_15pct |
| bson | point_complete | 5 | 1000 / 1000 | 4,398.912 [4,373.208–4,496.135] | 227,328.94 | 227,328.94 | 3,568 | 86.498 | 4,300 / 5,050 / 6,060 | 2.75% | within_15pct |
| bson | batch_complete | 5 | 1000 / 32000 | 118,882.55 [118,595.134–120,493.255] | 8,411.663 | 269,173.23 | 129,944.464 | 2,504.215 | 116,421 / 124,831 / 145,821 | 1.58% | within_15pct |
| bson | range_complete | 5 | 1000 / 10000 | 39,870.865 [39,529.782–40,703.652] | 25,080.971 | 250,809.708 | 39,926.192 | 815.384 | 39,421 / 44,720 / 47,591 | 2.91% | within_15pct |
| bson | range_public_complete | 5 | 1000 / 10000 | 39,604.013 [39,551.362–40,841.199] | 25,249.966 | 252,499.665 | 33,298.728 | 809.889 | 39,570 / 43,670 / 47,021 | 3.16% | within_15pct |
| typed-row | point_get_into_complete | 5 | 1000 / 1000 | 59,896.839 [59,415.262–60,307.212] | 16,695.372 | 16,695.372 | 91,750.352 | 150.723 | 56,940 / 66,421 / 111,611 | 1.49% | within_15pct |
| typed-row | point_complete | 5 | 1000 / 1000 | 7,151.067 [7,085.903–8,554.964] | 139,839.272 | 139,839.272 | 8,158.32 | 92.987 | 7,030 / 8,230 / 13,170 | 17.33% | inconclusive_above_15pct |
| typed-row | batch_complete | 5 | 1000 / 32000 | 138,815.522 [137,532.1–142,682.359] | 7,203.805 | 230,521.771 | 128,677.848 | 2,005.847 | 136,081 / 143,522 / 177,891 | 3.64% | within_15pct |
| typed-row | range_complete | 5 | 1000 / 10000 | 48,619.047 [47,532.228–52,603.697] | 20,568.071 | 205,680.708 | 43,493.848 | 677.645 | 47,931 / 53,360 / 57,841 | 9.86% | within_15pct |
| typed-row | range_public_complete | 5 | 1000 / 10000 | 118,762.47 [117,522.406–120,394.598] | 8,420.168 | 84,201.684 | 150,486.968 | 891.178 | 116,001 / 125,831 / 145,022 | 2.41% | within_15pct |
| sqlite-json | point_get_into_complete | 5 | 1000 / 1000 | 4,856.217 [4,723.963–5,073.678] | 205,921.605 | 205,921.605 | 2,600.192 | 46.002 | 4,480 / 7,760 / 9,890 | 7.09% | within_15pct |
| sqlite-json | point_complete | 5 | 1000 / 1000 | 2,450.242 [2,392.765–2,510.795] | 408,122.953 | 408,122.953 | 1,728 | 33 | 2,370 / 2,960 / 3,580 | 4.81% | within_15pct |
| sqlite-json | batch_complete | 5 | 1000 / 32000 | 24,308.352 [24,095.623–24,739.016] | 41,138.124 | 1,316,419.97 | 45,752 | 477 | 23,700 / 29,810 / 31,580 | 2.62% | within_15pct |
| sqlite-json | range_complete | 5 | 1000 / 10000 | 18,288.827 [18,084.604–18,555.136] | 54,678.192 | 546,781.923 | 15,296.672 | 215.01 | 17,791 / 19,891 / 24,510 | 2.56% | within_15pct |
| sqlite-json | range_public_complete | 5 | 1000 / 10000 | 10,047.839 [9,849.416–10,436.308] | 99,523.888 | 995,238.877 | 9,288 | 105 | 9,760 / 11,200 / 15,850 | 5.74% | within_15pct |
| sqlite-row | point_get_into_complete | 5 | 1000 / 1000 | 12,139.013 [11,834.103–13,017.236] | 82,379.02 | 82,379.02 | 7,990.56 | 153.504 | 11,551 / 13,750 / 23,140 | 9.32% | within_15pct |
| sqlite-row | point_complete | 5 | 1000 / 1000 | 8,872.485 [8,788.096–9,014.295] | 112,707.996 | 112,707.996 | 7,100 | 140.5 | 8,780 / 9,830 / 14,210 | 2.53% | within_15pct |
| sqlite-row | batch_complete | 5 | 1000 / 32000 | 219,618.316 [218,635.11–221,568.935] | 4,553.354 | 145,707.337 | 209,242.112 | 3,545.045 | 216,282 / 226,193 / 246,473 | 1.33% | within_15pct |
| sqlite-row | range_complete | 5 | 1000 / 10000 | 77,778.805 [77,239.304–79,577.466] | 12,856.973 | 128,569.731 | 66,581.488 | 1,182.046 | 76,750 / 82,400 / 90,911 | 2.96% | within_15pct |
| sqlite-row | range_public_complete | 5 | 1000 / 10000 | 70,879.34 [70,700.405–71,738.133] | 14,108.484 | 141,084.835 | 60,583.624 | 1,072.012 | 70,171 / 75,371 / 80,401 | 1.45% | within_15pct |

## Mutation

| Engine | Phase | Reps | Calls / rows per repetition | ns/call median [min–max] | Calls/s | Rows/s | Go B/call | Go objects/call | p50 / p95 / p99 ns medians | Calls/s spread | State / skip reason |
| --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | --- |
| json | update_nonindexed | 5 | 1000 / 1000 | 10,400,237.144 [6,807,040.246–11,042,349.506] | 96.152 | 96.152 | 251,156.44 | 629.523 | 9,327,240 / 16,290,048 / 20,945,312 | 58.60% | inconclusive_above_15pct |
| json | update_indexed | 5 | 1000 / 1000 | 11,837,375.994 [6,894,561.302–12,531,060.861] | 84.478 | 84.478 | 590,774.128 | 1,441.649 | 9,667,434 / 20,526,207 / 28,722,637 | 77.23% | inconclusive_above_15pct |
| json | replace | 5 | 1000 / 1000 | 11,746,634.436 [6,858,147.196–12,507,261.4] | 85.131 | 85.131 | 557,418.472 | 1,499.859 | 9,845,115 / 21,296,015 / 39,242,708 | 77.36% | inconclusive_above_15pct |
| json | delete | 5 | 1000 / 1000 | 11,765,774.495 [6,717,232.651–11,924,247.165] | 84.992 | 84.992 | 662,043.224 | 1,733.719 | 9,581,163 / 25,603,607 / 38,947,995 | 76.49% | inconclusive_above_15pct |
| json | mixed_churn | 5 | 1000 / 1250 | 14,652,294.86 [8,474,604.132–15,351,941.754] | 68.249 | 85.311 | 785,852.736 | 2,105.658 | 12,632,162 / 27,887,059 / 41,014,115 | 77.45% | inconclusive_above_15pct |
| json | upsert | 5 | 0 / 0 | — | — | — | — | — | — | — | skipped: no equivalent public atomic retained-document upsert |
| template-v1 | update_nonindexed | 5 | 1000 / 1000 | 9,672,163.234 [5,762,464.98–11,714,483.782] | 103.389 | 103.389 | 317,873.656 | 1,083.203 | 8,613,742 / 15,952,024 / 21,442,067 | 85.28% | inconclusive_above_15pct |
| template-v1 | update_indexed | 5 | 1000 / 1000 | 11,765,566.745 [6,581,565.846–12,353,408.564] | 84.994 | 84.994 | 595,954.952 | 1,714.291 | 9,420,011 / 23,004,182 / 38,910,195 | 83.52% | inconclusive_above_15pct |
| template-v1 | replace | 5 | 1000 / 1000 | 10,396,824.067 [6,257,729.798–11,271,313.422] | 96.183 | 96.183 | 572,482.288 | 1,709.566 | 9,102,168 / 17,582,100 / 25,822,559 | 73.90% | inconclusive_above_15pct |
| template-v1 | delete | 5 | 1000 / 1000 | 10,454,802.625 [6,262,772.572–11,380,487.066] | 95.65 | 95.65 | 656,059.896 | 1,815.084 | 8,982,786 / 19,593,009 / 29,672,566 | 75.07% | inconclusive_above_15pct |
| template-v1 | mixed_churn | 5 | 1000 / 1250 | 13,131,561.954 [7,563,631.321–14,570,256.333] | 76.152 | 95.191 | 725,615.288 | 2,119.216 | 9,208,649 / 35,186,689 / 40,336,539 | 83.49% | inconclusive_above_15pct |
| template-v1 | upsert | 5 | 0 / 0 | — | — | — | — | — | — | — | skipped: no equivalent public atomic retained-document upsert |
| bson | update_nonindexed | 5 | 1000 / 1000 | 10,146,006.421 [6,135,237.793–10,794,245.491] | 98.561 | 98.561 | 250,268.872 | 615.392 | 8,987,017 / 16,414,219 / 21,811,490 | 71.38% | inconclusive_above_15pct |
| bson | update_indexed | 5 | 1000 / 1000 | 12,089,809.596 [6,623,192.674–13,630,350.688] | 82.714 | 82.714 | 573,323.664 | 1,388.413 | 9,439,871 / 21,714,469 / 36,227,239 | 93.84% | inconclusive_above_15pct |
| bson | replace | 5 | 1000 / 1000 | 10,826,704.563 [6,332,112.791–12,262,254.297] | 92.364 | 92.364 | 559,518.832 | 1,487.334 | 9,287,470 / 19,623,979 / 25,884,870 | 82.69% | inconclusive_above_15pct |
| bson | delete | 5 | 1000 / 1000 | 11,647,306.839 [6,295,083.014–12,661,530.684] | 85.857 | 85.857 | 633,926.288 | 1,655.118 | 9,074,748 / 26,524,816 / 47,742,931 | 93.03% | inconclusive_above_15pct |
| bson | mixed_churn | 5 | 1000 / 1250 | 13,453,303.187 [8,174,144.12–14,572,650.872] | 74.331 | 92.914 | 752,512.264 | 2,013.211 | 11,691,603 / 26,261,783 / 32,637,005 | 72.26% | inconclusive_above_15pct |
| bson | upsert | 5 | 0 / 0 | — | — | — | — | — | — | — | skipped: no equivalent public atomic retained-document upsert |
| typed-row | update_nonindexed | 5 | 1000 / 1000 | 19,226,916.274 [11,011,633.338–20,430,340.85] | 52.01 | 52.01 | 1,799,947.824 | 13,640.451 | 16,889,473 / 28,665,676 / 34,549,763 | 80.50% | inconclusive_above_15pct |
| typed-row | update_indexed | 5 | 1000 / 1000 | 21,295,736.32 [13,522,908.765–24,492,386.054] | 46.958 | 46.958 | 4,089,491.616 | 32,164.798 | 19,247,875 / 30,204,371 / 38,579,723 | 70.53% | inconclusive_above_15pct |
| typed-row | replace | 5 | 1000 / 1000 | 21,462,111.435 [12,665,258.996–22,500,224.091] | 46.594 | 46.594 | 4,532,610.04 | 49,690.266 | 18,411,287 / 30,217,542 / 42,988,035 | 74.07% | inconclusive_above_15pct |
| typed-row | delete | 5 | 1000 / 1000 | 23,192,009.601 [13,779,993.116–24,775,811.069] | 43.118 | 43.118 | 6,376,451.552 | 67,920.551 | 19,701,450 / 38,870,405 / 47,749,161 | 74.69% | inconclusive_above_15pct |
| typed-row | mixed_churn | 5 | 1000 / 1250 | 31,438,811.068 [19,410,501.263–33,940,189.344] | 31.808 | 39.76 | 10,281,153.92 | 110,537.711 | 23,509,177 / 59,011,279 / 72,388,079 | 69.34% | inconclusive_above_15pct |
| typed-row | upsert | 5 | 1000 / 2000 | 36,852,311.707 [23,207,517.326–41,864,697.661] | 27.135 | 54.271 | 10,599,332.04 | 118,621.972 | 31,768,016 / 56,267,633 / 67,089,837 | 70.77% | inconclusive_above_15pct |
| sqlite-json | update_nonindexed | 5 | 1000 / 1000 | 6,973,335.143 [5,178,203.726–7,503,997.92] | 143.403 | 143.403 | 2,128.224 | 34.003 | 6,697,165 / 7,498,082 / 13,404,989 | 41.74% | inconclusive_above_15pct |
| sqlite-json | update_indexed | 5 | 1000 / 1000 | 6,760,428.804 [5,327,944.996–7,501,442.92] | 147.92 | 147.92 | 2,200.688 | 37.008 | 6,608,744 / 6,951,657 / 12,923,804 | 36.76% | inconclusive_above_15pct |
| sqlite-json | replace | 5 | 1000 / 1000 | 6,780,147.259 [5,195,297.05–7,511,988.427] | 147.489 | 147.489 | 2,226.512 | 35.02 | 6,600,344 / 6,920,187 / 13,109,336 | 40.25% | inconclusive_above_15pct |
| sqlite-json | delete | 5 | 1000 / 1000 | 6,857,411.186 [5,182,943.555–7,414,888.284] | 145.828 | 145.828 | 752.016 | 25 | 6,601,443 / 7,078,839 / 12,917,175 | 39.83% | inconclusive_above_15pct |
| sqlite-json | mixed_churn | 5 | 1000 / 1250 | 8,587,339.099 [6,583,907.782–9,359,075.069] | 116.451 | 145.563 | 2,245.072 | 40.277 | 6,676,074 / 13,425,419 / 16,899,293 | 38.68% | inconclusive_above_15pct |
| sqlite-json | upsert | 5 | 1000 / 2000 | 7,044,086.337 [5,998,188.018–7,660,197.408] | 141.963 | 283.926 | 3,546.704 | 44.753 | 6,707,944 / 7,427,752 / 13,602,311 | 25.48% | inconclusive_above_15pct |
| sqlite-row | update_nonindexed | 5 | 1000 / 1000 | 7,051,688.243 [5,839,154.316–7,262,660.22] | 141.81 | 141.81 | 4,761.696 | 104.03 | 6,701,754 / 7,891,586 / 13,599,241 | 23.67% | inconclusive_above_15pct |
| sqlite-row | update_indexed | 5 | 1000 / 1000 | 7,000,340.711 [6,218,039.904–7,193,611.52] | 142.85 | 142.85 | 4,847.52 | 107.016 | 6,797,516 / 7,485,203 / 13,414,479 | 15.27% | inconclusive_above_15pct |
| sqlite-row | replace | 5 | 1000 / 1000 | 6,871,456.93 [6,322,927.573–7,832,712.97] | 145.53 | 145.53 | 4,873.2 | 105.013 | 6,791,776 / 7,206,459 / 13,094,876 | 20.95% | inconclusive_above_15pct |
| sqlite-row | delete | 5 | 1000 / 1000 | 6,815,292.377 [6,389,680.509–7,166,228.9] | 146.729 | 146.729 | 753.36 | 25.012 | 6,707,265 / 7,150,859 / 12,793,233 | 11.56% | within_15pct |
| sqlite-row | mixed_churn | 5 | 1000 / 1250 | 8,563,604.797 [8,385,518.819–9,022,237.951] | 116.773 | 145.967 | 4,828.576 | 108.511 | 6,805,146 / 13,699,083 / 15,409,749 | 7.21% | within_15pct |
| sqlite-row | upsert | 5 | 1000 / 2000 | 6,903,611.992 [6,762,011.893–7,464,666.793] | 144.852 | 289.703 | 8,541.408 | 172.619 | 6,787,955 / 7,299,790 / 12,795,343 | 9.61% | within_15pct |

## Checkpoint

| Engine | Phase | Reps | Calls / rows per repetition | ns/call median [min–max] | Calls/s | Rows/s | Go B/call | Go objects/call | p50 / p95 / p99 ns medians | Calls/s spread | State / skip reason |
| --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | --- |
| json | checkpoint | 5 | 1 / 0 | 56,905,858 [12,014,035–119,734,145] | 17.573 | — | 325,792 | 5,957 | 56,905,858 / 56,905,858 / 56,905,858 | 426.13% | inconclusive_above_15pct |
| template-v1 | checkpoint | 5 | 1 / 0 | 40,735,013 [7,920,676–66,112,338] | 24.549 | — | 226,264 | 5,704 | 40,735,013 / 40,735,013 / 40,735,013 | 452.67% | inconclusive_above_15pct |
| bson | checkpoint | 5 | 1 / 0 | 45,688,861 [10,912,135–50,135,013] | 21.887 | — | 314,256 | 5,968 | 45,688,861 / 45,688,861 / 45,688,861 | 327.57% | inconclusive_above_15pct |
| typed-row | checkpoint | 5 | 1 / 0 | 65,689,513 [38,730,063–89,106,439] | 15.223 | — | 3,829,760 | 6,534 | 65,689,513 / 65,689,513 / 65,689,513 | 95.89% | inconclusive_above_15pct |
| sqlite-json | checkpoint | 5 | 1 / 0 | 22,240,425 [20,241,946–23,094,943] | 44.963 | — | 624 | 17 | 22,240,425 / 22,240,425 / 22,240,425 | 13.57% | within_15pct |
| sqlite-row | checkpoint | 5 | 1 / 0 | 21,347,086 [20,718,289–22,323,056] | 46.845 | — | 624 | 17 | 21,347,086 / 21,347,086 / 21,347,086 | 7.41% | within_15pct |

## Engine storage

Bytes at the original declared boundary; each entry is median [minimum–maximum].

| Engine | Boundary | Reps | Persistent bytes | WAL bytes | Transient bytes |
| --- | --- | ---: | --- | --- | --- |
| json | common_checkpoint_before_upsert | 5 | 36,154,797 [36,123,135–36,155,642] | 2,872,756 [316,645–2,872,756] | 0 [0–0] |
| template-v1 | common_checkpoint_before_upsert | 5 | 33,181,615 [33,146,471–37,372,705] | 3,281,636 [674,247–3,281,636] | 0 [0–0] |
| bson | common_checkpoint_before_upsert | 5 | 33,983,394 [33,953,716–33,993,147] | 2,968,411 [2,968,411–2,968,411] | 0 [0–0] |
| typed-row | common_checkpoint_before_upsert | 5 | 64,478,509 [60,284,436–72,856,043] | 19,496 [9,006–2,865,463] | 0 [0–0] |
| sqlite-json | common_checkpoint_before_upsert | 5 | 1,990,656 [1,990,656–1,990,656] | 0 [0–0] | 196,608 [196,608–196,608] |
| sqlite-row | common_checkpoint_before_upsert | 5 | 1,658,880 [1,658,880–1,658,880] | 0 [0–0] | 196,608 [196,608–196,608] |

Raw declared storage/stat fields are retained per repetition in JSON (`engine_storage.raw_declared_components`), without summing or interpreting them.
