Generic UpdateBatch width and request-size sweep — supplied summary.

Formatting only: this report does not validate original packets, verify landing/receipts, or grant acceptance. Producer label: `retained`. Population: 4,096; serial requests per cell/repetition: 100; fresh-DB repetitions per cell: 5.

Source commit: `3325dfe77940fec8587d8b61b1ac4e0b2f72caca`. Runtime SHA256: `eab40aaf77ed307d92a7a38adf0dd80be07a7e12bdda8a2ec3118baf9cad33ff`. Harness SHA256: `706b71f11508bf9c0075b0dd9c5463cd741a49cba83996feaf753330ace77822`.

[Supplied raw summary, including every original repetition](<../../../../docs/evidence/r1-row-store-5061/_artifacts/final-M-3325dfe/results/C/summary.json>); [Original raw packet](<../../../../docs/evidence/r1-row-store-5061/_artifacts/mutation-sweep/final-actual-M/3325dfe/packet.json>). Supplied summary byte SHA256: `17f2ae7f7fbda5c12a6e249090ca3f1a5a083b75598fd6f5d41995184247dbe0`. Original packet byte SHA256 as recorded by supplied summary: `5be838ef1e24f8fec1793471531b1b6da086cdf522aa35a3cabf1f76bf531e07`.

Each value below is supplied by the reviewed summarizer. ACK ns/request is the median of per-repetition mean ACK costs. Requests/s is the median of per-repetition request throughput, not the reciprocal of median ACK cost. Rows/s uses actual request rows. p50/p95/p99 are medians of per-repetition percentiles, not pooled percentiles. Go bytes and objects are per acknowledged request and include process background/observer work; they are not owned memory. ACK spread is relative request-throughput spread; Flush spread is relative explicit-Flush-duration spread. The supplied >15% inconclusive labels remain visible; within_15pct is not an acceptance decision.

| Bio bytes | Request rows | Indexed | Changed fields | Median ACK ns/request | Requests/s | Rows/s | Go B/request | Objects/request | Median rep p50 ns | Median rep p95 ns | Median rep p99 ns | ACK spread/status | Median explicit Flush ns | Flush spread/status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 96 | 1 | no | bio | 10,141,260.65 | 98.607 | 98.607 | 835,386 | 5,116.92 | 9,198,559 | 15,084,915 | 16,085,285 | 2.958% (within_15pct) | 3,680 | 7.609% (within_15pct) |
| 96 | 1 | no | email_city | 10,202,785.5 | 98.012 | 98.012 | 845,000.64 | 5,120.83 | 9,294,980 | 15,193,396 | 15,679,521 | 4.309% (within_15pct) | 3,720 | 22.581% (inconclusive) |
| 96 | 1 | yes | bio | 10,376,470.46 | 96.372 | 96.372 | 895,423.92 | 5,463.65 | 9,351,400 | 15,109,186 | 15,856,333 | 4.435% (within_15pct) | 3,770 | 49.072% (inconclusive) |
| 96 | 1 | yes | email_city | 10,483,537.61 | 95.388 | 95.388 | 983,002.24 | 5,667.48 | 9,527,692 | 15,204,786 | 17,106,905 | 3.961% (within_15pct) | 3,840 | 52.891% (inconclusive) |
| 96 | 32 | no | bio | 25,281,799.99 | 39.554 | 1,265.733 | 4,773,989.2 | 11,696.8 | 25,113,942 | 31,421,142 | 33,677,065 | 5.102% (within_15pct) | 5,050 | 46.931% (inconclusive) |
| 96 | 32 | no | email_city | 25,646,985.92 | 38.991 | 1,247.71 | 4,839,369.76 | 11,714.15 | 25,509,486 | 32,069,319 | 34,239,320 | 4.003% (within_15pct) | 4,580 | 109.607% (inconclusive) |
| 96 | 32 | yes | bio | 25,678,731.62 | 38.943 | 1,246.167 | 5,105,572.56 | 12,504.24 | 25,508,956 | 31,887,038 | 35,527,452 | 7.071% (within_15pct) | 3,940 | 85.508% (inconclusive) |
| 96 | 32 | yes | email_city | 26,878,709.02 | 37.204 | 1,190.533 | 5,381,344.32 | 13,209.27 | 26,643,796 | 33,377,452 | 34,034,478 | 4.68% (within_15pct) | 3,690 | 33.062% (inconclusive) |
| 4096 | 1 | no | bio | 10,966,771.73 | 91.185 | 91.185 | 889,238.72 | 5,133.4 | 10,074,487 | 15,423,879 | 16,830,182 | 9.396% (within_15pct) | 4,040 | 48.267% (inconclusive) |
| 4096 | 1 | no | email_city | 11,043,074.86 | 90.554 | 90.554 | 885,456 | 5,122.45 | 10,045,257 | 15,221,987 | 16,122,105 | 7.404% (within_15pct) | 4,160 | 46.154% (inconclusive) |
| 4096 | 1 | yes | bio | 11,324,430.51 | 88.305 | 88.305 | 947,808.96 | 5,473.77 | 10,197,928 | 15,597,510 | 18,450,698 | 7.031% (within_15pct) | 3,820 | 61.518% (inconclusive) |
| 4096 | 1 | yes | email_city | 11,779,010.17 | 84.897 | 84.897 | 1,030,563.36 | 5,671.13 | 10,340,119 | 15,695,721 | 19,636,459 | 8.453% (within_15pct) | 4,210 | 42.993% (inconclusive) |
| 4096 | 32 | no | bio | 54,852,316.55 | 18.231 | 583.385 | 6,279,206.72 | 11,755.27 | 53,597,556 | 72,167,395 | 73,636,300 | 16.474% (inconclusive) | 5,920 | 125.676% (inconclusive) |
| 4096 | 32 | no | email_city | 54,562,754.95 | 18.328 | 586.481 | 7,344,309.36 | 11,783.1 | 55,457,013 | 71,219,476 | 73,120,554 | 16.558% (inconclusive) | 5,250 | 30.667% (inconclusive) |
| 4096 | 32 | yes | bio | 55,065,341.46 | 18.16 | 581.128 | 6,612,430 | 12,580.05 | 54,211,222 | 70,091,555 | 75,206,215 | 16.137% (inconclusive) | 5,490 | 15.319% (inconclusive) |
| 4096 | 32 | yes | email_city | 56,183,889.39 | 17.799 | 569.558 | 7,685,435.2 | 13,268.98 | 56,154,741 | 72,104,684 | 76,511,157 | 16.84% (inconclusive) | 5,130 | 63.158% (inconclusive) |

Counter tables retain unnormalized medians of aggregate per-repetition deltas. ACK covers the entire serial request loop; Flush covers the separate explicit Flush boundary after that loop. Calls, rows, bytes and ns are aggregate totals per repetition, not per-request or per-row estimates. Nested timers overlap and must not be added into a total. Async/default-maintenance work may contribute across boundaries. Zero is a supplied observed delta; missing counters are refused, never filled with zero.

ACK WAL/sync: medians of aggregate per-repetition deltas.

| Bio bytes | Request rows | Indexed | Changed fields | Append calls | Logical sync calls | Physical sync calls | WAL bytes | Physical sync ns | Logical sync ns |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 96 | 1 | no | bio | 100 | 100 | 100 | 38,030 | 425,778,019 | 426,171,532 |
| 96 | 1 | no | email_city | 100 | 100 | 100 | 38,920 | 427,781,096 | 428,174,729 |
| 96 | 1 | yes | bio | 100 | 100 | 100 | 38,030 | 428,036,746 | 428,403,808 |
| 96 | 1 | yes | email_city | 100 | 100 | 100 | 38,920 | 430,261,513 | 430,654,827 |
| 96 | 32 | no | bio | 100 | 100 | 100 | 934,560 | 430,742,540 | 432,316,608 |
| 96 | 32 | no | email_city | 100 | 100 | 100 | 963,040 | 436,524,697 | 438,310,146 |
| 96 | 32 | yes | bio | 100 | 100 | 100 | 934,560 | 433,715,347 | 435,462,442 |
| 96 | 32 | yes | email_city | 100 | 100 | 100 | 963,040 | 464,516,965 | 466,358,710 |
| 4096 | 1 | no | bio | 100 | 100 | 100 | 438,030 | 423,913,262 | 424,624,952 |
| 4096 | 1 | no | email_city | 100 | 100 | 100 | 438,920 | 418,930,563 | 419,786,053 |
| 4096 | 1 | yes | bio | 100 | 100 | 100 | 438,030 | 426,629,699 | 427,513,325 |
| 4096 | 1 | yes | email_city | 100 | 100 | 100 | 438,920 | 427,094,392 | 427,777,218 |
| 4096 | 32 | no | bio | 100 | 100 | 100 | 13,734,560 | 559,949,884 | 560,039,285 |
| 4096 | 32 | no | email_city | 100 | 100 | 100 | 13,763,040 | 567,034,632 | 567,148,142 |
| 4096 | 32 | yes | bio | 100 | 100 | 100 | 13,734,560 | 564,376,555 | 564,496,116 |
| 4096 | 32 | yes | email_city | 100 | 100 | 100 | 13,763,040 | 552,827,898 | 552,953,609 |

ACK publication: medians of aggregate per-repetition deltas.

| Bio bytes | Request rows | Indexed | Changed fields | Update calls | Update rows | Current read ns | Prepare ns | Publish ns | Indexed Flush calls | Indexed Flush publish ns |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 96 | 1 | no | bio | 100 | 100 | 876,499 | 12,190 | 953,088,289 | 0 | 0 |
| 96 | 1 | no | email_city | 100 | 100 | 957,250 | 12,580 | 964,419,264 | 0 | 0 |
| 96 | 1 | yes | bio | 100 | 100 | 919,597 | 12,820 | 981,785,417 | 0 | 0 |
| 96 | 1 | yes | email_city | 100 | 100 | 926,137 | 12,610 | 986,890,345 | 0 | 0 |
| 96 | 32 | no | bio | 100 | 3,200 | 7,815,378 | 16,750 | 1,013,117,510 | 0 | 0 |
| 96 | 32 | no | email_city | 100 | 3,200 | 7,961,685 | 21,960 | 1,029,182,175 | 0 | 0 |
| 96 | 32 | yes | bio | 100 | 3,200 | 7,271,205 | 17,540 | 1,034,038,598 | 0 | 0 |
| 96 | 32 | yes | email_city | 100 | 3,200 | 7,472,177 | 21,610 | 1,129,365,141 | 0 | 0 |
| 4096 | 1 | no | bio | 100 | 100 | 1,141,006 | 15,090 | 969,020,833 | 0 | 0 |
| 4096 | 1 | no | email_city | 100 | 100 | 1,053,053 | 18,231 | 978,762,675 | 0 | 0 |
| 4096 | 1 | yes | bio | 100 | 100 | 1,054,048 | 14,690 | 1,001,476,441 | 0 | 0 |
| 4096 | 1 | yes | email_city | 100 | 100 | 995,672 | 16,280 | 1,037,490,520 | 0 | 0 |
| 4096 | 32 | no | bio | 100 | 3,200 | 15,507,385 | 37,270 | 1,197,244,901 | 0 | 0 |
| 4096 | 32 | no | email_city | 100 | 3,200 | 16,639,802 | 37,481 | 1,199,282,715 | 0 | 0 |
| 4096 | 32 | yes | bio | 100 | 3,200 | 15,594,390 | 33,660 | 1,193,373,828 | 0 | 0 |
| 4096 | 32 | yes | email_city | 100 | 3,200 | 14,847,143 | 32,990 | 1,273,989,532 | 0 | 0 |

Flush WAL/sync: medians of aggregate per-repetition deltas.

| Bio bytes | Request rows | Indexed | Changed fields | Append calls | Logical sync calls | Physical sync calls | WAL bytes | Physical sync ns | Logical sync ns |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 96 | 1 | no | bio | 0 | 0 | 0 | 0 | 0 | 0 |
| 96 | 1 | no | email_city | 0 | 0 | 0 | 0 | 0 | 0 |
| 96 | 1 | yes | bio | 0 | 0 | 0 | 0 | 0 | 0 |
| 96 | 1 | yes | email_city | 0 | 0 | 0 | 0 | 0 | 0 |
| 96 | 32 | no | bio | 0 | 0 | 0 | 0 | 0 | 0 |
| 96 | 32 | no | email_city | 0 | 0 | 0 | 0 | 0 | 0 |
| 96 | 32 | yes | bio | 0 | 0 | 0 | 0 | 0 | 0 |
| 96 | 32 | yes | email_city | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 1 | no | bio | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 1 | no | email_city | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 1 | yes | bio | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 1 | yes | email_city | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 32 | no | bio | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 32 | no | email_city | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 32 | yes | bio | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 32 | yes | email_city | 0 | 0 | 0 | 0 | 0 | 0 |

Flush publication: medians of aggregate per-repetition deltas.

| Bio bytes | Request rows | Indexed | Changed fields | Update calls | Update rows | Current read ns | Prepare ns | Publish ns | Indexed Flush calls | Indexed Flush publish ns |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 96 | 1 | no | bio | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 96 | 1 | no | email_city | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 96 | 1 | yes | bio | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 96 | 1 | yes | email_city | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 96 | 32 | no | bio | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 96 | 32 | no | email_city | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 96 | 32 | yes | bio | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 96 | 32 | yes | email_city | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 1 | no | bio | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 1 | no | email_city | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 1 | yes | bio | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 1 | yes | email_city | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 32 | no | bio | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 32 | no | email_city | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 32 | yes | bio | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4096 | 32 | yes | email_city | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

Scope is generic UpdateBatch costs for this supplied finite serial fixture. No native partial-setter, metadata-reference-only mutation, larger-population, concurrency, before/after optimization ratio, or storage-capacity qualification follows. Rehearsal values remain historical diagnostics. Retained acceptance requires the existing frozen validator and separate coordinator-observed receipts.
