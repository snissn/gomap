# Allocator repair and matched public write comparison

Active graph #4871, allocation tracker #5130, implementation #5131 and #5132,
PR #5135. Exact repaired candidate7702c2a28f9c45266977ec708688717f4e19fd6a.
Baseline restores17 allocator Go inputs to merged zipper main9029eb4e06fb99913c08dd2b3d7424f310445469;
both builds retain the SAME new immediate-parent root lifetime fix.

| Median, three alternating fixed1000x public Batch.Write pairs | Baseline | Candidate |
| --- | ---: | ---: |
| B/op | 141463 | 31944 |
| allocations/op | 269 | 127 |
| ns/op | 45212 | 21356 |

77.42% fewer bytes,52.79% fewer objects,52.76% lower median time (directional).
The unchanged public benchmark uses10000seed keys, CPU0/GOMAXPROCS1 and Go1.26.3;
Checkpoint every8writes is outside timed sections. Full Go/module input census
matches before/after. This is a short viability check, not full sprint, M0,
durability/Dgraph, native ACK or Q32 qualification.

Public SetSync regression was RED on8ed with candidate-page reuse before seal.
Repair retains root records through the ACTUAL surviving successor CommitSeq;
manifest retention stays at its own commit. Required physical format-feature
checks stay fail-closed. Fresh IgnoreFormatConfig, replacement-config callers,
and the index-only snapshot fixture are repaired without weakening assertions.
SaveFormatConfig census covers187calls/91Go files. All four focused normal
commands and the focused repaired-seam race pass. Independent GPT6.1 Sol review
ACCEPT binds all16 repaired file SHA256/Git blobs;15 original allocator paths
are unchanged and3 original paths received renewed review.

This packet preserves all selected hashes, raw paired output, public runner,
overlay map, focused commands/logs, RED/crash receipts, complete failed-test
inventory, review and policy evidence. Reproducible ELF binaries, overlay input
copies and the full crash DB remain on the runner and are not published or
released by this packet. Their hashes/input recipes are included.

Current-head required PR CI and hosted review remain separate live gates.
The zipper cleanup receipts prove only that released clean worktree was deleted
without force and unregistered, with protected identities unchanged. The original
dirty zipper lane and all active source/evidence remain protected.
