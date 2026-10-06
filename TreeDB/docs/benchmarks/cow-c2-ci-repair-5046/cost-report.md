# C2 repaired cached-cut cost diagnostic (#5046)

These are bounded integration measurements with three retained repeats. They establish neither statistical significance nor sustained C4 qualification. The C3 MVCC Store fences remain in place. This report must be read with the coordinator cost disposition and final source applicability evidence.

Runtime/harness candidate: `974d1ce9bd5cd91d17f5851d53663bf71643b288`. Complete source-manifest identity: `1b2e288f2a9a5707649be0a7b596aac6240eb295ed3d119328177ad595659077`. Both packets use the identical frozen source. Linux 185, Intel Core i5-11400F, six cores/twelve threads, Linux 6.8, Go 1.26.3, GOMAXPROCS 12. GOGC/GOMEMLIMIT overrides were cleared and recorded. Owned validation/collection was serialized; process/load receipts are retained privately with the full runner packets.

The published packets retain stdout and stderr separately, receipts, source/binary/environment bindings, matched summaries and all samples. Analyzers require all 45 successful runs, 648 rows, exact cases/iterations, raw-stream hashes, no source drift, complete public-history outputs and zero COW snapshot rotations. The rawKV packet additionally verifies the actual retained cache authority after final Close: every live charge/count is zero and the historical peak is positive. Failed attempts are recorded separately; they are excluded from acceptance and never replaced with success.

## Raw KV dirty-cache costs

Values below are the median across three fixed-count runs, in **ns/op / ops/s / B/op / allocs/op**. Every timed latency includes the same clock overhead and correctness checks. Fresh capture and explicit-sync writes use 16 operations; owned point hits, forward16 and incremental writes use 128 operations. DirtyCheckpoint is not collected in this current repair packet; earlier checkpoint measurements remain historical. Point misses are not measured.

All modes use identical profiles, two shards, N1024/N2048, inline 64 bytes or forced-pointer 4096 bytes, disabled automatic maintenance and a 1 GiB flush threshold. Pointer data is deliberately compressible and uses default compression. Fresh capture reseeds dirty entries outside the timer. Other reads warm one cut outside the timer. Incremental writes replace existing keys without a warm capture; the first explicit sync includes seeded dirty data. Final Close/setup/layout validation are outside timing.

| Profile / layout / N / operation | append_only | btree | cow_btree |
| --- | ---: | ---: | ---: |
| command_wal_durable / Inline64 / 1024 / FreshCapture | 239,114 / 4,182 / 133,395 / 20 | 15,235 / 65,638 / 2,820 / 23 | 3,020 / 331,126 / 376 / 2 |
| command_wal_durable / Inline64 / 2048 / CaptureReadRelease | 1,977 / 505,817 / 1,038 / 3 | 1,906 / 524,659 / 1,038 / 3 | 1,230 / 813,008 / 440 / 3 |
| command_wal_durable / Inline64 / 2048 / Forward16 | 6,913 / 144,655 / 2,558 / 18 | 6,419 / 155,788 / 2,822 / 22 | 8,965 / 111,545 / 1,640 / 20 |
| command_wal_durable / Inline64 / 2048 / FreshCapture | 412,324 / 2,425 / 133,386 / 20 | 15,599 / 64,107 / 2,820 / 23 | 3,158 / 316,656 / 376 / 2 |
| command_wal_durable / Inline64 / 2048 / IncrementalWrite | 6,137,291 / 163 / 955 / 6 | 6,188,887 / 162 / 411 / 6 | 6,205,483 / 161 / 11,842 / 71 |
| command_wal_durable / Inline64 / 2048 / IncrementalWriteSync | 6,213,460 / 161 / 4,481 / 6 | 6,190,600 / 162 / 264 / 6 | 6,179,565 / 162 / 11,744 / 71 |
| command_wal_durable / Pointer4096 / 1024 / FreshCapture | 290,425 / 3,443 / 133,620 / 20 | 17,056 / 58,630 / 3,063 / 24 | 3,681 / 271,665 / 376 / 2 |
| command_wal_durable / Pointer4096 / 2048 / CaptureReadRelease | 8,344 / 119,847 / 10,045 / 3 | 7,033 / 142,187 / 10,046 / 3 | 23,156 / 43,185 / 43,776 / 16 |
| command_wal_durable / Pointer4096 / 2048 / Forward16 | 257,262 / 3,887 / 97,182 / 68 | 259,240 / 3,857 / 97,406 / 72 | 255,541 / 3,913 / 40,944 / 33 |
| command_wal_durable / Pointer4096 / 2048 / FreshCapture | 353,135 / 2,832 / 133,620 / 20 | 17,697 / 56,507 / 3,060 / 24 | 3,393 / 294,724 / 376 / 2 |
| command_wal_durable / Pointer4096 / 2048 / IncrementalWrite | 6,470,815 / 155 / 184,995 / 46 | 6,525,758 / 153 / 206,203 / 45 | 6,988,586 / 143 / 200,022 / 117 |
| command_wal_durable / Pointer4096 / 2048 / IncrementalWriteSync | 6,312,613 / 158 / 5,372 / 6 | 6,166,473 / 162 / 136,441 / 6 | 7,049,572 / 142 / 28,198 / 87 |
| command_wal_relaxed / Inline64 / 1024 / FreshCapture | 378,616 / 2,641 / 133,380 / 19 | 13,355 / 74,878 / 2,820 / 23 | 3,394 / 294,638 / 376 / 2 |
| command_wal_relaxed / Inline64 / 2048 / CaptureReadRelease | 1,745 / 573,066 / 1,038 / 3 | 1,629 / 613,874 / 1,038 / 3 | 1,697 / 589,275 / 440 / 3 |
| command_wal_relaxed / Inline64 / 2048 / Forward16 | 5,813 / 172,028 / 2,612 / 18 | 6,009 / 166,417 / 2,836 / 22 | 8,618 / 116,036 / 1,640 / 20 |
| command_wal_relaxed / Inline64 / 2048 / FreshCapture | 382,689 / 2,613 / 133,380 / 19 | 15,327 / 65,244 / 2,820 / 23 | 3,191 / 313,381 / 376 / 2 |
| command_wal_relaxed / Inline64 / 2048 / IncrementalWrite | 3,661 / 273,149 / 808 / 6 | 3,864 / 258,799 / 264 / 6 | 15,924 / 62,798 / 11,694 / 70 |
| command_wal_relaxed / Inline64 / 2048 / IncrementalWriteSync | 6,269,084 / 160 / 4,489 / 6 | 6,174,176 / 162 / 264 / 6 | 6,120,010 / 163 / 11,744 / 71 |
| command_wal_relaxed / Pointer4096 / 1024 / FreshCapture | 356,692 / 2,804 / 133,383 / 20 | 16,500 / 60,606 / 2,820 / 23 | 3,293 / 303,674 / 376 / 2 |
| command_wal_relaxed / Pointer4096 / 2048 / CaptureReadRelease | 8,085 / 123,686 / 10,045 / 3 | 8,478 / 117,952 / 10,046 / 3 | 24,674 / 40,528 / 43,776 / 16 |
| command_wal_relaxed / Pointer4096 / 2048 / Forward16 | 258,157 / 3,874 / 97,236 / 68 | 253,984 / 3,937 / 97,460 / 72 | 259,034 / 3,860 / 40,944 / 33 |
| command_wal_relaxed / Pointer4096 / 2048 / FreshCapture | 346,800 / 2,884 / 133,380 / 19 | 17,650 / 56,657 / 2,820 / 23 | 3,411 / 293,169 / 376 / 2 |
| command_wal_relaxed / Pointer4096 / 2048 / IncrementalWrite | 12,394 / 80,684 / 84,594 / 15 | 17,072 / 58,575 / 103,186 / 16 | 152,429 / 6,560 / 168,964 / 177 |
| command_wal_relaxed / Pointer4096 / 2048 / IncrementalWriteSync | 6,595,225 / 152 / 6,499 / 7 | 6,574,432 / 152 / 137,568 / 7 | 13,630,302 / 73 / 27,062 / 177 |
| no_wal_fast / Inline64 / 1024 / FreshCapture | 382,132 / 2,617 / 133,380 / 19 | 16,228 / 61,622 / 2,820 / 23 | 3,101 / 322,477 / 376 / 2 |
| no_wal_fast / Inline64 / 2048 / CaptureReadRelease | 1,929 / 518,403 / 1,039 / 3 | 1,735 / 576,369 / 1,038 / 3 | 1,274 / 784,929 / 440 / 3 |
| no_wal_fast / Inline64 / 2048 / Forward16 | 6,598 / 151,561 / 2,613 / 18 | 5,768 / 173,370 / 2,768 / 22 | 8,440 / 118,483 / 1,640 / 20 |
| no_wal_fast / Inline64 / 2048 / FreshCapture | 385,013 / 2,597 / 133,380 / 19 | 13,559 / 73,752 / 2,820 / 23 | 3,227 / 309,885 / 376 / 2 |
| no_wal_fast / Inline64 / 2048 / IncrementalWrite | 591 / 1,691,475 / 543 / 0 | 3,523 / 283,849 / 16,384 / 0 | 8,086 / 123,671 / 10,998 / 67 |
| no_wal_fast / Inline64 / 2048 / IncrementalWriteSync | 14,688,937 / 68 / 7,809,247 / 778 | 13,697,447 / 73 / 1,545,116 / 773 | 21,882,866 / 46 / 879,824 / 1115 |
| no_wal_fast / Pointer4096 / 1024 / FreshCapture | 342,873 / 2,917 / 152,665 / 237 | 16,965 / 58,945 / 2,820 / 23 | 3,071 / 325,627 / 376 / 2 |
| no_wal_fast / Pointer4096 / 2048 / CaptureReadRelease | 8,303 / 120,438 / 10,045 / 3 | 6,192 / 161,499 / 10,045 / 3 | 20,007 / 49,983 / 43,776 / 16 |
| no_wal_fast / Pointer4096 / 2048 / Forward16 | 258,866 / 3,863 / 97,236 / 68 | 250,249 / 3,996 / 97,393 / 72 | 249,699 / 4,005 / 40,944 / 33 |
| no_wal_fast / Pointer4096 / 2048 / FreshCapture | 360,972 / 2,770 / 133,380 / 19 | 18,294 / 54,663 / 2,820 / 23 | 3,272 / 305,623 / 376 / 2 |
| no_wal_fast / Pointer4096 / 2048 / IncrementalWrite | 3,060 / 326,797 / 15,618 / 0 | 5,306 / 188,466 / 42,255 / 5 | 116,587 / 8,577 / 108,729 / 85 |
| no_wal_fast / Pointer4096 / 2048 / IncrementalWriteSync | 18,251,125 / 55 / 8,931,831 / 1028 | 17,675,945 / 57 / 2,665,601 / 1027 | 29,435,137 / 34 / 2,300,367 / 1456 |

All N1024/N2048 cases and repeats are in `rawkv/matched-summary.json`; paired COW/comparator and N2048/N1024 ratios are retained without sample filtering. Ratio spread and the small 16/128 sample percentile sets are descriptive, not confidence intervals or production tails.

## Actual public MVCC diagnostic

This uses public Open→mvcc.New→CommitAt(CommitRelaxed) followed by GetAt or complete exact-key version iteration. Eight logical keys, 128 byte values, eight shards, 16 MiB flush threshold, side stores/background checkpoint off. The write acknowledgement follows the chosen profile; CommitRelaxed is not an explicit-sync promise. The timed boundary includes commit, read/scan, validation and clocks. N128/N256 are growing histories, not steady-state workloads.

For a point read output/op is 1. Full history requires every version in timestamp order with visited=retained=output, skipped 0 and no error; average output/op is 8.5 at 128 operations and 16.5 at 256 operations. This prevents an incomplete scan from appearing faster.

| Profile / read / operations | append_only | btree | cow_btree |
| --- | ---: | ---: | ---: |
| command_wal_durable / all_versions / 128 | 7,378,507 / 136 / 246,301 / 297 | 7,606,047 / 131 / 1,266,947 / 329 | 6,262,426 / 160 / 14,343 / 125 |
| command_wal_durable / all_versions / 256 | 7,194,499 / 139 / 194,394 / 312 | 7,284,880 / 137 / 1,226,112 / 339 | 6,511,353 / 154 / 15,300 / 127 |
| command_wal_durable / point / 128 | 6,468,723 / 155 / 2,677 / 14 | 6,202,927 / 161 / 50,353 / 15 | 6,277,214 / 159 / 11,058 / 78 |
| command_wal_durable / point / 256 | 6,210,441 / 161 / 1,869 / 14 | 6,532,766 / 153 / 25,799 / 15 | 6,222,470 / 161 / 11,959 / 78 |
| command_wal_relaxed / all_versions / 128 | 93,199 / 10,730 / 135,126 / 109 | 91,926 / 10,878 / 1,078,862 / 144 | 21,445 / 46,631 / 14,195 / 125 |
| command_wal_relaxed / all_versions / 256 | 107,228 / 9,326 / 148,450 / 150 | 104,463 / 9,573 / 1,097,047 / 199 | 25,893 / 38,620 / 15,108 / 127 |
| command_wal_relaxed / point / 128 | 4,194 / 238,436 / 2,528 / 14 | 5,744 / 174,095 / 50,259 / 15 | 13,457 / 74,311 / 10,911 / 78 |
| command_wal_relaxed / point / 256 | 3,693 / 270,783 / 1,676 / 14 | 4,376 / 228,519 / 25,608 / 15 | 13,210 / 75,700 / 11,767 / 78 |
| no_wal_fast / all_versions / 128 | 80,939 / 12,355 / 129,486 / 103 | 84,878 / 11,782 / 1,077,450 / 137 | 16,439 / 60,831 / 13,496 / 122 |
| no_wal_fast / all_versions / 256 | 67,224 / 14,876 / 145,569 / 143 | 61,510 / 16,258 / 1,096,237 / 192 | 21,052 / 47,501 / 14,410 / 124 |
| no_wal_fast / point / 128 | 1,803 / 554,631 / 2,202 / 8 | 4,970 / 201,207 / 49,932 / 9 | 8,098 / 123,487 / 10,211 / 75 |
| no_wal_fast / point / 256 | 1,421 / 703,730 / 1,378 / 8 | 2,780 / 359,712 / 25,255 / 9 | 8,772 / 113,999 / 11,069 / 75 |

## Memory and cost interpretation

B/op and allocs/op describe Go allocations within each benchmark timing boundary. COW engine charges/publication counters are whole-fixture snapshots outside timing, including seeding/reseeds and the final observed cut. They are not per-operation heap use. Peak/retired/pinned charges are bounded engine accounting; process maximum RSS includes fixture setup/teardown and all cases in the child process. Public-MVCC process_alloc includes diagnostic work outside its timer. No charge/RSS equivalence is claimed.

The COW mode is opt-in. Capture gains do not erase incremental-write, iterator or pointer-decode costs. Material regressions require source/profile investigation, minimization and explicit coordinator disposition. The final completion packet records that decision; this table alone cannot authorize merge or default promotion.

## Retained failures and repaired collection

The earlier af59 packet stopped on a reproduced NoWAL final Close EOF during multi-chunk leaf publication. Zero final charges did not prove final seed persistence. A source-bound overwrite→Close→reopen regression and barrier-lifetime repair address that failure. The old collector also overlapped btree/cow_btree filters and merged logger stderr into benchmark stdout; its incomplete/corrupt packet and review erratum remain retained. This packet uses component-anchored filters, separate streams and per-run exact-case validation.

## Reproduction and scope

Both standalone fixtures are Go package benchmarks, not unified-bench/benchprof profile-dir inputs. See cmd/unified_bench/README.md and cmd/benchprof/README.md for their names and fixed-count commands. The retained collector/analyzer scripts reproduce the full matrix with a new immutable archive/manifest/output directory and exclusive runner handoff. Frozen runtime/harness identity and final artifact-only descendant applicability are part of the packet.

Future C4 work must qualify matched sustained retention/read economics, negative lookups, long-lived readers, reclamation, checkpoint/storage growth and relevant tails on the landed harness. These tiny diagnostics do not establish those outcomes.
