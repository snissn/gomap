# Iterator allocation repair validation handoff

Root-owned source: e88d0c8b908bf3928fdc42e82419627a743b416d plus exactly three frozen files: TreeDB/tree/{iterator.go,primary_iterator.go,owned_iterator.go}. No worker source edits, Git content mutations, GitHub actions, benchmark bodies, profiles or child agents. Root retains source ownership; validation runtime lease is RELEASED and no validation process remains active at handoff.

The patch defers ordinary PRIMARY wrapper allocation until the SAME initial root has selected PRIMARY and passed original decode/base validation. Ordinary forward/reverse supply nil; owned projection supplies the SAME inline primary in its original ownedProjectionStorage and returns that pointer. All three callers preserve error surface. Root read count, seek semantics, fixed containing size, callback/scratch lifetime, pooled-base cleanup and final Close scrubbing remain unchanged by source audit. See caller-audit.md for finite callers and ownership guards. Existing tests cover actual selected PRIMARY, corruption fallback, forward/reverse/domain semantics, owned bounded reader refusal, live snapshot/final-reader retention and alias detachment. No mirrored allocation tests were added.

| Check | Exit | Top-level pass | Subtest pass | Skip | Wall seconds (includes build) |
| --- | --- | --- | --- | --- | --- |
| tree-normal | 0 | 57 | 62 | 0 | 5.07 |
| db-normal | 0 | 25 | 34 | 0 | 48.24 |
| rootpublication-normal | 0 | 4 | 0 | 0 | 7.00 |
| tree-race | 0 | 57 | 62 | 0 | 8.01 |
| db-race | 0 | 25 | 34 | 0 | 68.09 |
| rootpublication-race | 0 | 4 | 0 | 0 | 10.57 |
| public-build | 0 | 0 | 0 | 0 | 33.36 |
| public-buildinfo | 0 | 0 | 0 | 0 | 0.84 |
| compiler-version | 0 | 0 | 0 | 0 | 0.85 |
| caching-normal | 0 | 5 | 4 | 0 | 17.73 |
| caching-race | 0 | 5 | 4 | 0 | 32.37 |

All checks used pinned Go1.26.3, GOMAXPROCS2/-p1, task fastdisk tmp and existing task caches; each test has120s inner/150s outer process-group terminate/kill/join bounds. Full TreeDB/tree normal+race; focused DB primary directory/iteration/corruption/retention/native capture and snapshot iterator lifetime; rootpublication owned borrowed directory; caching SAME basis iterator/admission/old pointer cut/alias scrub normal+race. Zero race reports and zero skips. Exact inventory/argv/env/raw logs and per-command all-tracked-byte before/after manifests are in validation.json and logs/. All tracked source bytes are unchanged through every call and equal final freeze.json. No timeout occurred. These are focused local correctness controls, not full hosted CI qualification.

The public benchmark ELF was compiled only, without running test/benchmark bodies: candidate-public.test. Pinned compiler identity and go version -m output are retained in validation.json/logs. Build inputs bind HEAD plus source.patch SHA256 and every tracked file. The prior whole-process allocation profile's 1179648 bytes/144 discarded wrappers is motivation, not a measurement of this successor. prepared-profile.json specifies ONE original fixed16x checkpoint allocation-profile invocation using this ELF; CPU0,1/runtime env/selector/rate1/120s inner/150s outer match profiles-01 apart from task paths and successor binary. It is PREPARED, NOT EXECUTED. Root must grant execution. Whole-process profile includes off-timer setup, reset and all checkpoints; no timing acceptance or new threshold is implied.

Root next actions: verify freeze, independently review exact three-file patch/caller closure, then authorize the separate single profile and any bounded uninstrumented comparison. Root owns commits/publication. Original allocation/timing evidence remains preserved unchanged.
