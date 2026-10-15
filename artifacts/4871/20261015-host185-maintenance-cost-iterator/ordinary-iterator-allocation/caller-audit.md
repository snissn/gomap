# Three-file iterator initialization audit

Inspected exact uncommitted patch atop e88d0c8b908bf3928fdc42e82419627a743b416d. Root owns the source write lease; this validation worker changed no source. Full tracked-byte identities and original patch are frozen with validation artifacts.

| Initializer caller | Storage | Ordinary root | PRIMARY root | Error propagation |
| --- | --- | --- | --- | --- |
| tree.IteratorWithOptions | nil passed to initializer; existing pooled base acquired first | initializer returns nil,nil; same base.Seek(start), prefetch setup | wrapper allocated only after original root selection, directory decode and validatePrimaryBase; same merged iterator returned | root/decode/base validation returns nil,error; caller sets base.err and returns base |
| tree.ReverseIteratorWithOptions | nil; same pooled base | same reverse Seek/domain/prefetch | same selected wrapper, reverse flag and seek logic | same base.err surface |
| tree.OwnedPointerProjectionIterator | &storage.primary in SAME original ownedProjectionStorage allocation | returns same storage.iterator after Seek | initializer uses supplied nonnil address, overwrites same inline primary, returns that pointer | same storage.iterator.err surface; no fallback to an unbounded reader |

All three callers branch on p!=nil OR err!=nil, so changing selected-error return from true,error to nil,error does not drop corruption/refusal. The initializer performs the same one root load and same decode/base validation before initialization; no preliminary duplicate root read was added. p.Seek(nil) and its resulting iterator error remain unchanged. Empty-domain forward/reverse early return also remains unchanged.

The owned containing struct retains iterator, primary, deltaLeaf, traversal stack and fixed key/leaf scratch in the same allocation. OwnedPointerProjectionAllocationSize still measures that exact struct; no new allocation, pooled replacement, callback, child reader, admission or root ownership is introduced by this patch. The only new conditional allocation is when supplied p is nil AND the validated root is PRIMARY. Returning p from the owned constructor is the same &storage.primary previously returned explicitly.

Primary Close still clears baseTree, directory, delta, callbacks, scratch and child alias before base.Close. Owned base.Close still zeroes owned storage without entering iteratorPool. Selected ordinary handles still have an independently allocated PRIMARY wrapper and pooled base; errors before wrapper creation still close/return that same base. Caller code retains the same ownership and close-result behavior.

## Production callers of owned projection (finite closure)

- db.Snapshot.OwnedPointerProjectionIterator: begins/ends its existing operation guard, calls same captured snapshot tree, checks iterator.Error and closes on failure. Caller must retain Snapshot/tree owner until iterator.Close; unchanged by patch. Existing DB primary closed-iterator test covers ordinary and owned handles plus alias scrubbing and final Snapshot drainage.
- caching.Snapshot.cowIteratorSources: uses the SAME cowCut.basis.snapshot and owner.readLeaf; on error closes prior sources. Result remains retained in cowMergeIterator under existing workspace/lease and basis lifetime. No capture of a newer head or unrelated pager introduced.
- db.rebindSnapshotDependencyTreeWithMetadata: fixed scratch plus OwnedPointerProjectionAllocationSize are admitted on its existing metadata owner, iterator closes before deferred scratch refund. The private staged dependency tree must remain an ordinary dependency tree; existing type check/error surface unchanged.
- rootpublication.DependencyDirectoryV2.walkBorrowedRecords (main and exact lookup iterators): pre-admits fixed scratch plus TWO owned iterator allocations, retains the same directory, closes iterators before releasing directory/refunding charge. Exact record/order/count/refusal checks remain unchanged.

No additional initPrimaryIterator caller exists. No production source beyond the three root-owned files changed in this validation task. This is source/correctness evidence, not a new allocation or timing qualification. Original whole-process profile observations belong to the earlier source. The prepared successor profile is not executed here.
