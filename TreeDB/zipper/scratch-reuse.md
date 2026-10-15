# Apply scratch ownership

An allocator or pager clone shares the original zipper family scratch cache.
Public `db.Batch.Write` creates an allocator clone per optimistic attempt, so
keeping the cache on each individual clone would allocate scratch on every write.
Unrelated `New` zippers have independent caches. The shared owner contains only
scratch; it does not retain an allocator, pager, batch, or zipper configuration.

Acquire removes one scratch from the cache under its mutex. That Apply owns the
scratch exclusively until release, including all parallel worker usage governed
by the existing scratch locks. Concurrent Applies borrow distinct scratches.
When the cache is empty, a borrower creates fresh scratch. Release resets the
scratch and retains at most the existing `applyScratchKeep + 1` instances across
the entire family. Excess returned scratches become unreachable from the cache.
The owner lives while a family member remains reachable.

Reset preserves existing capacity limits for arenas and reusable buffers. It
also drops oversized free/active leaf-ref pointer arrays after large maintenance
operations, using the existing `mergeLeafRefCachePageKeep` limit. Active work can
exceed retention limits temporarily. These limits are per field and per scratch,
not a small total memory budget. Existing leaf-ref pages use interior pointers
into 64-page chunks, so logical page counts do not describe exact retained heap.

The in-flight persisted-leaf cache stays on each zipper clone. Its page bytes
remain owned and stable until `endLeafRefCache`; scratch/builder bytes can mutate
within the same Apply. Sharing scratch does not remove that ownership boundary.
Retired page lists, published roots, and safe Get results outlive scratch reuse.

`clone_scratch_test.go` checks the actual allocator-clone selection seam, bounded
retention, simultaneous borrowers, oversized metadata trimming, split/delete/
overwrite, error/retry, and retained roots/retired pages. The external public
caller test also checks `Batch.Write` with a retained snapshot and owned Get
result. Allocation equivalence is verified separately with the public
`BenchmarkConditionalTxnBaselineBatchWrite` and an exact `db.(*Batch).Write`
profile filter; behavior alone cannot distinguish allocation from reuse.
