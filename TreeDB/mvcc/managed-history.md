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

`TestManagedHistoryNativeDeferredCloseCustody` exercises the public Store and
native two-collection/raw composite on all three production profiles. It proves
actual persisted pointer versions, reads the held history after floor/prune and
checkpoint while the DB is open, then keeps that cut across DB shutdown. Late
iterator operations refuse `ErrClosed`. Releasing the original iterator must
clear the original producer and maintenance budgets (except historical peak
counters), plus original identity/namespace pins, before reopen verifies the
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
their historical meanings. The existing managed-history correctness controls,
benchmarks and `TestManagedHistoryMeasurement` are unchanged; new retained
qualification still needs its own reviewed exact-source protocol and evidence.
