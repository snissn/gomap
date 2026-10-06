# Independent current974d cost evidence review

Runtime candidate `974d1ce9bd5cd91d17f5851d53663bf71643b288`, tree `b06dc9f41b8593f1c9f87d5b6d0e1a23a853b822`; source repairs compared with `8a159dc1a2aeea02611421ecc3216b8ee84a1516`. **Current actual evidence ACCEPT; residual cost disposition and whole C2 HELD.** No Go or remote jobs, source changes, GitHub writes or publication occurred in this review.

All 26 sealed collection bindings and both transfer sets (70 raw, 50 MVCC files) matched. On review-owned copies both analyzers exited zero and reproduced all emitted derivatives byte for byte. Independent direct parsing reconstructed all 540 raw rows/180 cases and 108 public MVCC rows/36 cases, every repeat/summary/paired ratio and raw N scaling. All 90 stdout/stderr hashes and 45 zero-exit nonoverlapping receipts matched. Full histories and all 360 retained-authority raw Close receipts passed: zero live resources, positive peak; COW rotations were zero. The actual source audit independently matched all 7,330 regular Git paths and modes (123 executable), without extras or symlinks.

| no_wal_fast, N2048 COW | Median ns/op | B/op | allocs/op | All 3 paired ns ratios over btree |
|---|---:|---:|---:|---:|
| Inline point capture/read/release | 1,274 | 440 | 3 | 0.546–0.921 |
| Inline forward16 | 8,440 | 1,640 | 20 | 1.433–2.222 |
| Inline ACK write | 8,086 | 10,998 | 67 | 2.288–4.775 |
| Pointer point capture/read/release | 20,007 | 43,776 | 16 | 3.010–4.688 |
| Pointer forward16 | 249,699 | 40,944 | 33 | 0.980–1.029 |
| Pointer ACK write | 116,587 | 108,729 | 85 | 18.235–33.174 |

Pointer point B ratios are 4.358–4.372x btree and pointer ACK write B ratios 2.390–2.685x. Inline ACK writes add 67 allocations over the zero-allocation comparator; a ratio is undefined. Its time spread is 2.153x. WAL-relaxed pointer ACK write time remains 8.246–9.869x btree; relaxed inline ACK 3.905–5.259x, with 44.295x bytes. Durable inline ACK time is 0.988–1.117x, while bytes are 28.813–28.932x. Explicit-sync inline bytes are 44.485–44.496x in both WAL profiles. Preserve absolute costs with those ratios because the restored default comparator can allocate very little.

Public MVCC no_wal_fast N256 point reads measure 8,772 ns/11,069 B/75 allocations, 3.004–4.069x btree time. Full histories measure 21,052 ns/14,410 B/124 allocations with output and visits of 16.5/version-accumulating operation, 0.336–0.350x btree time. Prior profiles are mechanistic evidence with full setup/bookkeeping included, not replacement current timings or proof of irreducible costs.

NoWAL pointer N2048/N1024 time ratios are 0.580–0.743 for reads, 0.966–1.015 for ACK writes and 1.108–1.223 for explicit sync. These sparse sequential fixed-count results do not establish complexity or significance. Go1.26.3 linux/amd64, GOMAXPROCS12; measured one-minute load spans 2.877–6.382 raw and 2.833–3.504 MVCC. There is no foreign-quiet claim. All repeat arrays and zero-baseline absolute deltas are retained in the accompanying direct reconstruction and full own-copy derivatives.

The frozen runtime/harness/modules still matched at the review snapshot; tracked changes were only four owning docs and CI impact metadata. All 630 draft publication provenance entries matched both source and copied hashes, with private process/dependency/binary contents excluded by the inspected explicit publication list. Final report, quantified root disposition and final descendant publication proof remain pending. This verdict does not approve future draft bytes.

Local binary receipts and private hash indexes mutually bind the recorded binaries; their actual remote bytes were not available for independent local hashing. Generic content drift checks alone do not establish file types; the separate actual Git-mode audit establishes the collected archive types. Current scope is 648 rows/360 Close receipts, distinct from historical4add 756/432; current raw has no DirtyCheckpoint row. The point fixture measures hits only. MVCC history proof is not relabeled as raw Close proof.

No established safe local dead-owner deletion remains identified within the verified canonical planner/finalizer/native read authority seams. That is a bounded source judgment, not global optimality: nonempty retained backend successor uses the full merge fallback and later API/arena redesigns could change representation. The material residuals require explicit experimental opt-in acceptance. Strict hosted default-performance, actual Windows runtime, CI and current review remain separate merge gates; inherited diagnostic Close limitations remain explicitly outside the ordinary read repair.
