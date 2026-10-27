# Captured-cut successor batches

`Snapshot.SeekGEVersionRanges` runs at most 32 probes on its original retained
MVCC cut. The opportunistic basis workspace and its three slots remain bounded,
nonblocking owners. Shared-slot contention must not disable leaf batching on an
independently admitted cut-local point reader. An already-owned local reader
also keeps its existing limits and priority.

The local lane holds `cowReadMu` and enables the existing
`OwnedSuccessorScratch.BeginLeafBatch` scope. It uses the scalar successor's
original leaf callback, immutable-source winner selection, value materialization
and owned copy-out. The tree reuses a validated ordinary page only when the
probe lies within that same physical page's complete first/last key bounds.
PRIMARY and out-of-page probes retain the normal traversal. There is no new
snapshot, cache, pool, shared slot or decoder owner.

Local preparation releases the unused staging lease first. Preparation performs
admission only, so a failed attempt refunds a newly partial workspace before
using the maintained scalar fallback. Existing local allocations stay owned by
the cut. Actual probe errors remain per-input errors and are never retried as
eligibility failures. A failed probe invalidates the batch page before the next
input. A physical-tombstone merge ends page reuse before releasing `cowReadMu`
and entering the maintained merge, then starts a fresh reuse scope afterward.
Every result owns its key/value before another probe can overwrite scratch.
`EndLeafBatch` removes page aliases before the final unlock; cut Close retains
its original admission and refund responsibility.

`TestCOWReadBatchLocalPublicLeafReuse` checks real public-cut callbacks with all
four shared owners occupied and with an existing local reader after those
owners become available. Cache-disabled cases check values because that cache's
hit/miss counters are disabled too. Local corruption, partial admission,
overlay/tombstone merge, owned results and concurrent Close have separate
controls. `BenchmarkCOWReadBatchLocal` compares fresh-cut scalar singleton,
shared 32-key and contended 32-key routes, including actual callback counts.
Callback reduction demonstrates activation; public recovery/latency outcomes
require the separate matched Dgraph workload.
