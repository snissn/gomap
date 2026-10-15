# Combined allocator qualification after zipper merge

Issues #5130, #5131 and #5132. Candidate 8edabf94adc7b711786451b8c4c958a746254554; baseline main 9029eb4e06fb99913c08dd2b3d7424f310445469.
Zipper PR #5134 is merged and issue #5133 is closed. Both timed binaries retain
that exact zipper implementation. Only the allocator-owned Go inputs are restored
in the baseline overlay. The baseline binary hash exactly equals the previously
qualified zipper candidate, connecting these packets without adding medians.

| Median | Landed zipper main | Allocator plus zipper |
| --- | ---: | ---: |
| B/op | 141,606 | 31,944 |
| allocations/op | 269 | 127 |
| ns/op | 46,386 | 21,164 |

Three alternating fixed-1000x fresh-process pairs all improve: 77.44% fewer bytes,
52.79% fewer objects and 54.37% lower median time. Timing remains directional.
The unchanged public BenchmarkConditionalTxnBaselineBatchWrite uses a 10,000-key
seed, stopped-timer Checkpoint every eight writes, CPU0/GOMAXPROCS1, Go1.26.3.
Full freelist and focused DB format/fresh/readonly/reopen/fallback checks passed.
The runner completed in88.6s; source and module input census before/after match.

The18 allocator blobs are unchanged from the accepted main projection preserved
in5936ecf934248b7956a3030196be0208051b68a9. The pre-alpha allocator requires
freelist_patricia_v2. This evidence packet preserves raw commands/logs, baseline
overlay/inputs, exact binary hashes and build recipes, review binding, base
integration and zipper merged-file/current-head gate readback. Two reproducible
ELFs are omitted; no path is released for deletion by this publication.

Separate construction profiles and private-only comparisons remain separately
bound; they are not relabeled as this candidate's profile. Current-head allocator
PR gates/merge and sprint pre-sprint/M0/Dgraph/native ACK/Q32 qualification remain
open. The included prior cleanup receipt records only three separately released
old binaries; protected source/evidence and Git metadata stayed unchanged.
