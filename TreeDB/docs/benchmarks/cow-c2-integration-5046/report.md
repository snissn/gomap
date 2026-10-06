# C2 immutable cached-cut integration diagnostic (#5046)

These are bounded integration measurements with three retained repeats. They establish neither statistical significance nor sustained C4 qualification. The C3 MVCC Store fences remain in place. This report must be read with the coordinator cost disposition and final source applicability evidence.

Runtime/harness candidate: `4add47f71f165e2c7ee5e01f3cb7d6e6eefa6cad`. Complete source-manifest identity: `ae44d55e023db64fcaced92456f0e671a88886c81ee1b3996d9dba61a3869581`. Both packets use the identical frozen source. Linux 185, Intel Core i5-11400F, six cores/twelve threads, Linux 6.8, Go 1.26.3, GOMAXPROCS 12. GOGC/GOMEMLIMIT overrides were cleared and recorded. Owned validation/collection was serialized; process/load receipts are retained privately with the full runner packets.

The published packets retain stdout and stderr separately, receipts, source/binary/environment bindings, matched summaries and all samples. Analyzers require all 45 successful runs, 756 rows, exact cases/iterations, raw-stream hashes, no source drift, complete public-history outputs and zero COW snapshot rotations. The rawKV packet additionally verifies the actual retained cache authority after final Close: every live charge/count is zero and the historical peak is positive. Failed attempts are recorded separately; they are excluded from acceptance and never replaced with success.

## Raw KV dirty-cache costs

Values below are the median across three fixed-count runs, in **ns/op / ops/s / B/op / allocs/op**. Every timed latency includes the same clock overhead and correctness checks. Fresh capture and actual dirty checkpoint use 16 operations; owned point hits, forward16 and incremental writes use 128 operations; explicit-sync writes use 16 operations. Point misses are not measured.

All modes use identical profiles, two shards, N1024/N2048, inline 64 bytes or forced-pointer 4096 bytes, disabled automatic maintenance and a 1 GiB flush threshold. Pointer data is deliberately compressible and uses default compression. Fresh capture/checkpoint reseed dirty entries outside the timer. Other reads warm one cut outside the timer. Incremental writes replace existing keys without a warm capture; the first explicit sync includes seeded dirty data. Final Close/setup/layout validation are outside timing.

| Profile / layout / N / operation | append_only | btree | cow_btree |
| --- | ---: | ---: | ---: |
| command_wal_durable / Inline64 / 1024 / FreshCapture | 314,128 / 3,183 / 133,398 / 20 | 23,337 / 42,850 / 2,820 / 23 | 4,294 / 232,883 / 376 / 2 |
| command_wal_durable / Inline64 / 2048 / CaptureReadRelease | 2,597 / 385,060 / 1,038 / 3 | 2,055 / 486,618 / 1,038 / 3 | 1,143 / 874,891 / 440 / 3 |
| command_wal_durable / Inline64 / 2048 / DirtyCheckpoint | 69,524,079 / 14 / 7,991,166 / 1326 | 73,107,792 / 14 / 1,435,629 / 1332 | 64,903,354 / 15 / 1,806,346 / 1627 |
| command_wal_durable / Inline64 / 2048 / Forward16 | 9,075 / 110,193 / 2,558 / 18 | 6,943 / 144,030 / 2,822 / 22 | 9,570 / 104,493 / 1,640 / 20 |
| command_wal_durable / Inline64 / 2048 / FreshCapture | 528,385 / 1,893 / 133,386 / 20 | 22,495 / 44,454 / 2,820 / 23 | 4,042 / 247,402 / 376 / 2 |
| command_wal_durable / Inline64 / 2048 / IncrementalWrite | 6,888,492 / 145 / 955 / 6 | 9,019,485 / 111 / 551 / 6 | 6,831,673 / 146 / 11,843 / 71 |
| command_wal_durable / Inline64 / 2048 / IncrementalWriteSync | 8,477,695 / 118 / 4,489 / 6 | 8,692,321 / 115 / 264 / 6 | 6,662,867 / 150 / 11,744 / 71 |
| command_wal_durable / Pointer4096 / 1024 / FreshCapture | 515,933 / 1,938 / 133,620 / 20 | 21,547 / 46,410 / 3,060 / 24 | 4,166 / 240,038 / 376 / 2 |
| command_wal_durable / Pointer4096 / 2048 / CaptureReadRelease | 8,684 / 115,154 / 10,045 / 3 | 6,764 / 147,842 / 10,046 / 3 | 24,083 / 41,523 / 43,776 / 16 |
| command_wal_durable / Pointer4096 / 2048 / DirtyCheckpoint | 68,239,892 / 15 / 7,728,648 / 1404 | 81,679,010 / 12 / 1,241,769 / 1411 | 66,594,301 / 15 / 1,512,783 / 1766 |
| command_wal_durable / Pointer4096 / 2048 / Forward16 | 256,093 / 3,905 / 97,182 / 68 | 255,289 / 3,917 / 97,460 / 72 | 256,472 / 3,899 / 40,944 / 33 |
| command_wal_durable / Pointer4096 / 2048 / FreshCapture | 385,310 / 2,595 / 133,620 / 20 | 19,616 / 50,979 / 3,060 / 24 | 3,898 / 256,542 / 376 / 2 |
| command_wal_durable / Pointer4096 / 2048 / IncrementalWrite | 9,776,982 / 102 / 185,537 / 46 | 10,503,589 / 95 / 213,145 / 46 | 7,305,986 / 137 / 205,528 / 119 |
| command_wal_durable / Pointer4096 / 2048 / IncrementalWriteSync | 6,265,026 / 160 / 5,372 / 6 | 8,907,172 / 112 / 136,441 / 6 | 7,063,898 / 142 / 28,084 / 87 |
| command_wal_relaxed / Inline64 / 1024 / FreshCapture | 429,604 / 2,328 / 133,380 / 19 | 26,391 / 37,892 / 2,820 / 23 | 3,766 / 265,534 / 376 / 2 |
| command_wal_relaxed / Inline64 / 2048 / CaptureReadRelease | 2,088 / 478,927 / 1,038 / 3 | 1,546 / 646,831 / 1,038 / 3 | 1,233 / 811,030 / 440 / 3 |
| command_wal_relaxed / Inline64 / 2048 / DirtyCheckpoint | 68,837,985 / 15 / 8,113,454 / 1362 | 79,029,192 / 13 / 1,654,999 / 1375 | 68,662,615 / 15 / 1,954,616 / 1662 |
| command_wal_relaxed / Inline64 / 2048 / Forward16 | 6,639 / 150,625 / 2,558 / 18 | 6,528 / 153,186 / 2,782 / 22 | 8,781 / 113,882 / 1,640 / 20 |
| command_wal_relaxed / Inline64 / 2048 / FreshCapture | 433,997 / 2,304 / 133,380 / 19 | 17,565 / 56,931 / 2,820 / 23 | 3,766 / 265,534 / 376 / 2 |
| command_wal_relaxed / Inline64 / 2048 / IncrementalWrite | 4,804 / 208,160 / 808 / 6 | 5,305 / 188,501 / 264 / 6 | 14,641 / 68,301 / 11,694 / 70 |
| command_wal_relaxed / Inline64 / 2048 / IncrementalWriteSync | 6,705,987 / 149 / 4,489 / 6 | 8,918,986 / 112 / 264 / 6 | 8,657,467 / 116 / 11,744 / 71 |
| command_wal_relaxed / Pointer4096 / 1024 / FreshCapture | 384,877 / 2,598 / 133,380 / 19 | 19,681 / 50,810 / 4,308 / 41 | 3,888 / 257,202 / 376 / 2 |
| command_wal_relaxed / Pointer4096 / 2048 / CaptureReadRelease | 8,907 / 112,271 / 10,045 / 3 | 7,319 / 136,631 / 10,045 / 3 | 27,800 / 35,971 / 43,776 / 16 |
| command_wal_relaxed / Pointer4096 / 2048 / DirtyCheckpoint | 73,255,625 / 14 / 7,922,314 / 1466 | 89,853,893 / 11 / 1,352,662 / 1472 | 79,041,810 / 13 / 1,726,462 / 1878 |
| command_wal_relaxed / Pointer4096 / 2048 / Forward16 | 261,796 / 3,820 / 97,236 / 68 | 249,510 / 4,008 / 97,460 / 72 | 263,108 / 3,801 / 40,944 / 33 |
| command_wal_relaxed / Pointer4096 / 2048 / FreshCapture | 398,298 / 2,511 / 133,383 / 20 | 20,775 / 48,135 / 2,823 / 24 | 4,026 / 248,385 / 376 / 2 |
| command_wal_relaxed / Pointer4096 / 2048 / IncrementalWrite | 13,131 / 76,156 / 84,594 / 15 | 14,130 / 70,771 / 104,773 / 16 | 167,942 / 5,954 / 171,127 / 177 |
| command_wal_relaxed / Pointer4096 / 2048 / IncrementalWriteSync | 6,623,124 / 151 / 6,499 / 7 | 7,379,151 / 136 / 137,568 / 7 | 13,855,930 / 72 / 27,076 / 177 |
| no_wal_fast / Inline64 / 1024 / FreshCapture | 428,643 / 2,333 / 133,383 / 20 | 19,892 / 50,271 / 2,823 / 24 | 5,420 / 184,502 / 376 / 2 |
| no_wal_fast / Inline64 / 2048 / CaptureReadRelease | 1,650 / 606,061 / 1,039 / 3 | 1,571 / 636,537 / 1,006 / 3 | 1,116 / 896,057 / 440 / 3 |
| no_wal_fast / Inline64 / 2048 / DirtyCheckpoint | 43,527,532 / 23 / 7,505,879 / 1178 | 43,570,354 / 23 / 940,963 / 1180 | 63,351,605 / 16 / 1,367,384 / 1505 |
| no_wal_fast / Inline64 / 2048 / Forward16 | 11,541 / 86,648 / 2,559 / 18 | 6,311 / 158,453 / 2,768 / 22 | 8,588 / 116,442 / 1,640 / 20 |
| no_wal_fast / Inline64 / 2048 / FreshCapture | 463,235 / 2,159 / 133,380 / 19 | 19,311 / 51,784 / 2,820 / 23 | 3,986 / 250,878 / 376 / 2 |
| no_wal_fast / Inline64 / 2048 / IncrementalWrite | 705 / 1,418,641 / 543 / 0 | 2,351 / 425,351 / 16,384 / 0 | 7,103 / 140,786 / 10,998 / 67 |
| no_wal_fast / Inline64 / 2048 / IncrementalWriteSync | 20,431,082 / 49 / 7,787,583 / 774 | 14,088,534 / 71 / 1,534,368 / 772 | 26,380,509 / 38 / 892,884 / 1117 |
| no_wal_fast / Pointer4096 / 1024 / FreshCapture | 423,471 / 2,361 / 155,655 / 226 | 20,186 / 49,539 / 4,241 / 40 | 4,097 / 244,081 / 376 / 2 |
| no_wal_fast / Pointer4096 / 2048 / CaptureReadRelease | 8,800 / 113,636 / 10,045 / 3 | 7,511 / 133,138 / 10,045 / 3 | 21,032 / 47,547 / 43,776 / 16 |
| no_wal_fast / Pointer4096 / 2048 / DirtyCheckpoint | 58,913,040 / 17 / 7,498,045 / 1227 | 60,176,007 / 17 / 983,742 / 1230 | 58,890,564 / 17 / 1,255,136 / 1649 |
| no_wal_fast / Pointer4096 / 2048 / Forward16 | 259,560 / 3,853 / 97,236 / 68 | 252,027 / 3,968 / 97,446 / 72 | 255,305 / 3,917 / 40,944 / 33 |
| no_wal_fast / Pointer4096 / 2048 / FreshCapture | 410,462 / 2,436 / 133,380 / 19 | 19,220 / 52,029 / 2,820 / 23 | 4,345 / 230,150 / 376 / 2 |
| no_wal_fast / Pointer4096 / 2048 / IncrementalWrite | 3,904 / 256,148 / 16,417 / 1 | 4,768 / 209,732 / 39,316 / 4 | 154,321 / 6,480 / 108,406 / 85 |
| no_wal_fast / Pointer4096 / 2048 / IncrementalWriteSync | 19,150,379 / 52 / 8,911,355 / 1023 | 19,470,449 / 51 / 2,668,433 / 1023 | 28,844,861 / 35 / 2,020,987 / 1451 |

All N1024/N2048 cases and repeats are in `rawkv/matched-summary.json`; paired COW/comparator and N2048/N1024 ratios are retained without sample filtering. Ratio spread and the small 16/128 sample percentile sets are descriptive, not confidence intervals or production tails.

## Actual public MVCC diagnostic

This uses public Open→mvcc.New→CommitAt(CommitRelaxed) followed by GetAt or complete exact-key version iteration. Eight logical keys, 128 byte values, eight shards, 16 MiB flush threshold, side stores/background checkpoint off. The write acknowledgement follows the chosen profile; CommitRelaxed is not an explicit-sync promise. The timed boundary includes commit, read/scan, validation and clocks. N128/N256 are growing histories, not steady-state workloads.

For a point read output/op is 1. Full history requires every version in timestamp order with visited=retained=output, skipped 0 and no error; average output/op is 8.5 at 128 operations and 16.5 at 256 operations. This prevents an incomplete scan from appearing faster.

| Profile / read / operations | append_only | btree | cow_btree |
| --- | ---: | ---: | ---: |
| command_wal_durable / all_versions / 128 | 7,838,677 / 128 / 245,713 / 295 | 7,407,992 / 135 / 1,260,150 / 325 | 6,961,649 / 144 / 14,343 / 125 |
| command_wal_durable / all_versions / 256 | 7,327,522 / 136 / 191,332 / 311 | 7,491,958 / 133 / 1,225,433 / 339 | 6,654,435 / 150 / 15,300 / 127 |
| command_wal_durable / point / 128 | 6,249,220 / 160 / 2,744 / 14 | 6,242,723 / 160 / 50,352 / 15 | 6,336,363 / 158 / 11,108 / 78 |
| command_wal_durable / point / 256 | 6,386,610 / 157 / 1,868 / 14 | 6,343,277 / 158 / 25,772 / 15 | 6,395,338 / 156 / 11,959 / 78 |
| command_wal_relaxed / all_versions / 128 | 119,892 / 8,341 / 135,241 / 109 | 107,498 / 9,302 / 1,078,960 / 145 | 21,826 / 45,817 / 14,195 / 125 |
| command_wal_relaxed / all_versions / 256 | 123,874 / 8,073 / 148,514 / 150 | 125,166 / 7,989 / 1,097,120 / 199 | 33,303 / 30,027 / 15,108 / 127 |
| command_wal_relaxed / point / 128 | 7,173 / 139,412 / 2,529 / 14 | 4,752 / 210,438 / 50,205 / 15 | 13,999 / 71,434 / 10,910 / 78 |
| command_wal_relaxed / point / 256 | 3,856 / 259,336 / 1,676 / 14 | 4,989 / 200,441 / 25,581 / 15 | 13,705 / 72,966 / 11,767 / 78 |
| no_wal_fast / all_versions / 128 | 86,464 / 11,566 / 130,600 / 103 | 106,116 / 9,424 / 1,078,646 / 138 | 16,683 / 59,941 / 13,496 / 122 |
| no_wal_fast / all_versions / 256 | 66,791 / 14,972 / 145,592 / 144 | 79,923 / 12,512 / 1,096,206 / 193 | 22,462 / 44,520 / 14,410 / 124 |
| no_wal_fast / point / 128 | 1,814 / 551,268 / 2,201 / 8 | 2,394 / 417,711 / 49,878 / 9 | 8,364 / 119,560 / 10,211 / 75 |
| no_wal_fast / point / 256 | 2,327 / 429,738 / 1,351 / 8 | 2,314 / 432,152 / 25,255 / 9 | 9,164 / 109,123 / 11,069 / 75 |

## Memory and cost interpretation

B/op and allocs/op describe Go allocations within each benchmark timing boundary. COW engine charges/publication counters are whole-fixture snapshots outside timing, including seeding/reseeds and the final observed cut. They are not per-operation heap use. Peak/retired/pinned charges are bounded engine accounting; process maximum RSS includes fixture setup/teardown and all cases in the child process. Public-MVCC process_alloc includes diagnostic work outside its timer. No charge/RSS equivalence is claimed.

The COW mode is opt-in. Capture gains do not erase incremental-write, iterator or pointer-decode costs. Material regressions require source/profile investigation, minimization and explicit coordinator disposition. The final completion packet records that decision; this table alone cannot authorize merge or default promotion.

## Retained failures and repaired collection

The earlier af59 packet stopped on a reproduced NoWAL final Close EOF during multi-chunk leaf publication. Zero final charges did not prove final seed persistence. A source-bound overwrite→Close→reopen regression and barrier-lifetime repair address that failure. The old collector also overlapped btree/cow_btree filters and merged logger stderr into benchmark stdout; its incomplete/corrupt packet and review erratum remain retained. This packet uses component-anchored filters, separate streams and per-run exact-case validation.

## Reproduction and scope

Both standalone fixtures are Go package benchmarks, not unified-bench/benchprof profile-dir inputs. See cmd/unified_bench/README.md and cmd/benchprof/README.md for their names and fixed-count commands. The retained collector/analyzer scripts reproduce the full matrix with a new immutable archive/manifest/output directory and exclusive runner handoff. Frozen runtime/harness identity and final artifact-only descendant applicability are part of the packet.

Future C4 work must qualify matched sustained retention/read economics, negative lookups, long-lived readers, reclamation, checkpoint/storage growth and relevant tails on the landed harness. These tiny diagnostics do not establish those outcomes.

## Coordinator decision and remaining costs

The coordinator accepts these residual regressions for the explicit pre-alpha C2 immutable-cut infrastructure, after measured minimization and ownership review. See [the full quantified disposition](coordinator-cost-disposition.json). Independent whole-candidate review and current-head PR gates remain required. This decision supplies a usable experimental substrate; it does not recommend this mode for point/write workloads or complete C3, C4 or the parent outcome.

At NoWAL N2048, COW fresh capture is **3.99us inline / 4.35us pointer,376B,2allocations**. Current btree is19.31/19.22us,2820B,23allocations and append_only463.24/410.46us,133380B,19allocations. Capture allocation stays376B/2 at both tested sizes, with zero read-triggered rotations; the fixedS/Q source algorithm, rather than timing alone, proves that capture does not enumerate history.

The remaining costs are material. NoWAL N2048 COW inline writes are7.10us/10998B/67allocations, versus btree2.35us/16384B/0 and append_only0.70us/543B/0. Pointer writes are154.32us/108406B/85, versus btree4.77us/39316B/4 and append_only3.90us/16417B/1. The paired pointer-write time ratios span22.85–42.43x btree and30.12–47.63x append_only. Pointer capture-plus-hit is21.03us/43776B/16 versus btree7.51us/10045B/3; paired time is2.60–3.45x btree and allocation bytes4.36x. The new mode has a substantial acknowledged-write/private-reader cost; these ratios are retained rather than characterized as harmless overhead.

NoWAL public MVCC N256 commit-plus-point is9.16us/11069B/75allocations, versus btree2.31us/25255B/9 and append_only2.33us/1351B/8. Paired time spans3.74–4.57x btree and3.50–6.48x append_only. The earlier first minimization had139allocations/15405B and the original complete candidate147/68825; these are sequential descriptive comparisons with the same fixture, not interleaved significance evidence. Complete-history COW scans are22.46us/14410B/124allocations versus btree79.92us/1096206B/193 and append_only66.79us/145592B/144, with all16.5average returned versions validated.

Three minimizations removed empty backend projections, eager/private workspace setup, unnecessary ZSTD construction for other headers, NoWAL journal custody, separate preparation vectors, exact empty cache cursors, a one-entry dedup map and the full merged successor iterator for a proven-empty retained backend. Tombstones and nonempty bases still use the same captured snapshot fallback. Empty-key/value staging retains the public compatibility contract. All applicable normal/race/safe/vet gates pass at the frozen source.

## Profile evidence and ownership explanation

Eight final diagnostic profiles use the actual frozen cost binaries. [Pointer read allocations](profiles/profiles-4add47-material-costs/raw-cow_btree-Pointer4096-CaptureReadRelease.alloc-top.txt) show private codec306.32MB and admitted scratch growth33.13MB within482.13MB whole-process allocation. [Pointer write allocations](profiles/profiles-4add47-material-costs/raw-cow_btree-Pointer4096-IncrementalWrite.alloc-top.txt) include staged caller bytes51MB, immutable C1 node copies34.07MB and writer append buffers40.01MB within274.46MB. [Its CPU profile](profiles/profiles-4add47-material-costs/raw-cow_btree-Pointer4096-IncrementalWrite.cpu-cum.txt) also contains benchmark ReadMemStats and background training. [Public point allocations](profiles/profiles-4add47-material-costs/mvcc-cow_btree--point.alloc-cum.txt) are357.99MB whole process, with Open/Stats dominating; the empty-backend point path no longer constructs the general iterator. Setup, seeding, calibration and teardown are included, so these percentages are not causal per-operation fractions.

Both default and COW forced-pointer writes encode before acknowledgement. COW additionally establishes producer visibility, validates canonical actual RID/revision and installs independent file/dictionary deletion pins before swapping the cut. Ordinary legacy NoWAL Set may acknowledge buffered frames and flush through a later read barrier. Visibility flush is not fsync, and identical physical compression frames are unproved. The reported ratios measure these end-to-end paths; they do not isolate C1 copy overhead or the unique cost of flush.

Private staged bytes, admitted immutable node copies, ready-before-acceptance cut backing and independent read pins preserve the new mode's lifetime and atomicity guarantees. Canonical planner/finalizer and native reader authority are required by the accepted design. Further consolidation of staged owners, direct-output decoding, codec reuse or typed canonical planning may reduce costs, but requires a new verified admission/cancellation/lifetime contract. The67/85/75allocation counts are not claimed irreducible. No established safe redundant deletion remains at the currently verified seams. General nonempty-basis successor merging still allocates cache cursors; a lower-bound plus one-disk-cursor redesign remains possible and is unqualified here.

The [original default-read guard](default-read-guard/root-limited-disposition.json) is limited to three unchanged-path cases, including roughly7ns cached-inline lifecycle overhead. Source applicability to the final runtime supplies no new timing, persistence or drain evidence. Production sustained retention, checkpoint/storage growth, negative lookups and relevant tails remain C4 acceptance work; Store fences remain until the qualified C3 dependency is available.
