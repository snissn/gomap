# Optional managed version history

`mvcc.Store` stores caller timestamps independently of arrival order. It is an
optional logical history layer over the ordinary engine; COW transactions and
complete read cuts do not require managed history.

A discard floor rejects new reads and writes at or below that timestamp. Reads
already admitted on a retained complete cut remain readable after the floor
advances and physical versions are pruned. Admission reads the floor and captures
the engine cut under the same Store lock. Pruning releases that lock after the
cut is retained, so foreground work above the floor can proceed while the scan
and ordinary delete batches run.

Pruning retains all versions above the floor and the newest value at or below
it as the anchor. A tombstone anchor disappears only after all older versions
have been deleted. Reverse physical iteration and oldest-first deletion ensure
that a crash between batches cannot resurrect an older value. Durable prune
persists the floor before its first deletion. Interrupted pruning can resume
through the ordinary API after reopening.

The reverse scan inspects each at/below-floor record and validates the key,
value read and envelope of the first above-floor record. It then uses the same
captured iterator's ordered reverse `Seek` to the encoded lower bound for that
exact logical key, bypassing the remaining above-floor tail. Every bypassed
version remains retained. Empty keys, binary bytes and logical prefix neighbors
use the existing escaped-key encoding; the seek captures no replacement cut.

`PruneStats` counts inspected records and committed deletes. `Retained` and
`RetainedBytes` cover the value anchor and first above-floor record that were
inspected and kept; they do not measure all surviving history. `Skipped` is the
above-floor subset of inspected `Retained`. On success,
`Visited = Retained + Pruned`. Errors encountered in eligible records, the first
above-floor record, seek traversal or cleanup still fail the pass. A bypassed
tail's keys and payloads are not audited for corruption by pruning; their normal
read validation remains in effect.

This changes observational behavior from the earlier complete reverse scan,
which counted every surviving record and could detect corruption throughout
above-floor tails. It does not change the survivor set, floor admission,
oldest-first tombstone deletion, durability or held-cut ownership. The public
conformance trace keeps its schema, read/version results and pruned count; its
inspected `visited/skipped/retained` expectations change from `7/3/6` to `6/2/5`.
Consumers of earlier whole-snapshot counters must use the source revision that
produced them rather than interpret them as the new inspected subset.

Captured cuts own original memtable, backend and persistent value-log resources.
Their ordinary Close path releases those original resources and preserves the
first physical cleanup failure and its original custody. Closing the DB does
not wait synchronously for user-held cuts. Shutdown refuses new storage reads
through those cuts with `ErrClosed`; their cleanup custody remains until Close.
Physical deletion still requires unreachability; neither the logical discard
floor nor segment age authorizes removing live persistent value-log data.

Shared namespace, pager and retained-metadata cleanup uses the ordinary original
owner paths. These paths close physical handles and detach borrowed aliases
before refunding their original charges; a failed physical close keeps its
original custody and admission. Unused separate work-priced close/enrollment
frontends have been removed. PRIMARY construction and promotion remain separate
format-owner operations until their representation is retired.

`TestManagedHistoryFloorAdvancePreservesOpenCut` covers all production durability
profiles with inline and forced-pointer values. The tombstone crash-order test
uses fresh child processes at the successful floor and delete sync boundaries.
The captured-prune admission test verifies foreground ACK while the original
retained cut is paused. These controls establish their named contracts; the
complete release depends on the shared engine publication protocol as well.

The opt-in `TestManagedHistoryMeasurement` records one fresh-process measurement
cell. Its environment selects an output JSON file, fresh DB directory, profile
and forced-pointer setting with the `TREEDB_HISTORY_MEASURE_` prefix. It reports
foreground ACK separately from Checkpoint and Close drains, total allocation
bytes/objects per phase, observed heap at phase boundaries, and post-GC retained
heap/directory bytes with an old cut pinned, released and fully drained. Boundary
heap observations are not a continuously sampled peak. Run three fresh process
repetitions per profile/layout with an exclusive measurement slot and preserve
all raw samples. An unsupported baseline prune is reported as unsupported and
cannot support a speedup claim.

`TREEDB_HISTORY_MEASURE_DEPTH` optionally selects 16, 64 or 256 versions per
key; omitted means the original depth-16 cell and its original timestamp arrival
order. Each cell keeps 32 keys, 256-byte values and floor 12. Successful pruning
must inspect 416 records, delete 352 in 11 batches, and count 64 inspected retained
records, including 32 above-floor records, at every selectable depth. The physical
survivor set still contains `32*(depth-11)` versions. The old read at timestamp 8 returns
256 versions. The packet records these workload dimensions, the resolved
profile and activation of the public COW native backend. This extends the same
fresh-process Store measurement; it does not use the backend-only legacy
benchmark or introduce another capture driver.

Holding the deleted count fixed while increasing retained history tests the
cost of ordered tail seeking against the earlier complete scan. Historical
depth16/64 packets keep their original full-scan counters: respectively 512/2048
visited, 160/1696 retained and 128/1664 above-floor records, with 352 pruned in
each. Preserve those packets and their exact source bindings. Compare matching
depths, profiles and pointer layouts with equivalent measurement source,
ACK/drain boundaries and raw samples; the candidate's changed counter oracle
is not itself a performance result. Boundary retained
heap and directory bytes remain observations, not a sustained retention or
continuously sampled peak qualification. Ordered seeking retains existing
admission and fixed-scratch source iterators; it adds no cursor, cache or count
authority. Prune byte counters describe inspected physical records, while heap
and directory observations continue to cover the actual retained engine state.

`TestManagedHistoryPruneSkipsRetainedTail` fixes 32 keys and floor 12 at depths
16 and 64, using the public native COW owner with a published pointer basis and
an active COW overlay. It checks the inspected bound plus binary/empty/prefix
keys, out-of-order arrival, every reopened value anchor and retained version,
an above-floor tombstone, all old-cut values across checkpoint, shutdown custody
and final original-budget/identity release. `TestPruneRetainedTailInspectionBoundary`
checks eligible and first-boundary malformed records, errors at the exact seek
lower bound, inspected byte counts, and the explicit uninspected-tail behavior.

`TestManagedHistoryNativeDeferredCloseCustody` exercises the public Store and
native two-collection/raw composite on all three production profiles. It proves
actual persisted pointer versions, reads the held history after floor/prune and
checkpoint while the DB is open, then keeps that cut across DB shutdown. Late
iterator operations refuse `ErrClosed`. Releasing the original iterator queues
cleanup on the existing worker. The test waits up to five seconds for the
original producer and maintenance budgets (except historical peak counters),
plus original identity/namespace pins, to clear before reopen verifies the
persisted floor, value anchors, removed tombstone anchor, above-floor tombstone,
native rows and raw value. The diagnostic capture is closed before the workload;
only its existing allocator capabilities are retained for observations. The test
makes no full-budget-zero claim while the DB is open and no performance claim.

## Q32 supersession and source provenance

P4 retires the five tagged `native_prune*_test.go` fixtures and their three
capture drivers: `scripts/mvcc_native_prune.py`,
`scripts/native_prune_foreground.py` and `scripts/native_prune_memory.py`.
Their last source before retirement is
[`d594d6fb08917881e0651c2181f68385482587ab`](https://github.com/snissn/gomap/tree/d594d6fb08917881e0651c2181f68385482587ab).
The last callers were those drivers and the active command sections in the
unified-bench and benchprof READMEs; production Store uses ordinary admitted
bounded delete batches. The removed fixtures require retired `PruneCursor`,
work records/bytes, private preparation/cancel state or native observer APIs.
They cannot measure the maintained ordinary algorithm by changing an API call.

Historical raw artifacts, NO-GO evidence and exact-source lineage are preserved.
Use the validator from each packet's original Git revision with its recorded
source/runtime/toolchain bindings. The final tooling source above is a recovery
pointer, not a claim that its retired runtime dependencies compile. Q32 metrics,
private-output/cancellation assertions and prior qualification refusals keep
their historical meanings. The managed-history correctness controls, benchmarks
and `TestManagedHistoryMeasurement` use the ordinary maintained route; the later
ordered-tail change above updates their inspected accounting. New retained
qualification still needs its own reviewed exact-source protocol and evidence.

## Uncapped exact history prefix

On an admitted COW read cut, forward `ExactKey` histories with timestamp zero or
`math.MaxUint64` use the existing owned physical successor for their first row
and the following exhaustion check. Empty and one-version histories need no raw
cursor. Finding a second row, calling `Seek`, or calling `NextKey` switches once
to the ordinary iterator on the same cut and original bounds. Finite timestamp
ceilings retain key screening before value reads. Returned versions, physical
masks, discard-floor admission and borrowed-view lifetimes retain their existing
contracts; the probes do not count a replayed row twice.

Storage and integrity errors from either successor are terminal and never cause
a retry through another representation. Initial successor failures close the cut
and return an open error; failures on subsequent movement appear through
`Error`. Raw cursor admission may occur on that later movement. If a physical
key and its value are both corrupt, owned successor materialization may report
the value's storage/integrity error before the key can be decoded; raw iteration
can instead report the malformed key before reading that value. Both fail closed.
Deep histories pay the two lookups plus one cursor construction, so allocation
savings on shallow histories alone do not establish a workload speedup.
