This is a bounded diagnostic, not final performance acceptance. Twelve matched invocations (two ABC/CBA repeats per placement), fixed 64x, Go1.26.3, CPU0-3/GOMAXPROCS4, completed in 4.295 seconds. One separate 128x allocation profile was attribution only. Original uses adaptive; recent and candidate use cow_btree.

Public Checkpoint ran before the counter baseline. Both versions of each logical key (10 and 20, 256 bytes) were present in the captured backend; mutable_bytes and queue_len were zero. Each timed unit overwrites version20 then GetAt(15) returns version10, forcing backend successor participation. Four physical records and correct final inline/pointer placement are asserted. Actual root types are recorded in summary.json. All timed public-checkpoint/WAL-sync/vlog-sync deltas were zero; canonical WAL append was one per unit; all ACK/close checks passed. This does not claim no hidden I/O.

Medians (us/op; B/op; allocs/op):

| Placement | Original adaptive | Recent COW | Candidate COW |
| --- | --- | --- | --- |
| Inline | 6.1845; 3035.5; 22 | 17.5055; 54112.5; 100 | 23.710; 67328.5; 92 |
| Forced pointer | 8.9545; 2927.5; 25 | 249.5615; 134465; 200 | 117.1535; 76717; 186 |

Candidate inline is 35.44% slower than recent; reader p50 is 15.345 vs 9.020 us. Candidate pointer time improves 53.06% versus recent but repeats range 93-141 us; reader p50 remains 21.3555 vs 14.5855 us. Pointer writer p50 is similar (17.450 vs17.650 us). The populated read regression is independently visible despite lane-affinity writer improvements.

Candidate alloc_space attributes 7,626,752 bytes to 133 OwnedPointerProjectionIterator objects: exactly57,344 bytes/object, including setup/calibration. This proves the large projection allocation remains for populated PRIMARY; whole-profile setup/Stats allocations are outside the timed row and should not be mistaken for per-unit costs.

Current captured-root path is cowSeekGE -> cowSeekGEMergeAdmitted -> buildCOWIteratorLocked -> captured Snapshot.OwnedPointerProjectionIterator -> Tree.OwnedPointerProjectionIterator. GetEntryExactWithFixedScratch is exact-only and cannot replace GE. Candidate root_type0xb is a PRIMARY directory; original/recent root types are recorded, so this is a PRIMARY integration diagnostic, not proof that the old backend successor contract can be reactivated. A structural repair would require graph reassessment and a bounded captured-root successor seam that reuses existing seek/validated PRIMARY merge while retaining overlay/tombstone ordering, error propagation, stable value ownership, and the same retained basis. No implementation or M5 activation is performed here.

All original and projection production/existing-test census members, common harness hashes, and binary hashes were verified unchanged after collection. Raw files, process snapshots, build manifests, profile, exact argv/env and receipts are retained. Initial harness compilation and driver-preflight failures occurred before timing and are retained; there was no fixture failure or candidate patch.
