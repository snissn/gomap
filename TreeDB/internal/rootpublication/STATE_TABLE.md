# Dormant root-publication coordinator state table

The coordinator mutex owns pending candidates/debt, visible/durable/recoverable
frontiers, waiters, timer state, admission counts, failure state, and statistics.
Candidate builders own one reference-counted admission lease; nested builders
share that lease. `Publisher.Publish` executes with neither ownership held.

| From | Event | To | Ownership and observable result |
| --- | --- | --- | --- |
| idle | first enqueue | pending-window | Candidate/debt retained; visible advances; timer anchors now. |
| pending-window | later enqueue/supersede | pending-window | Latest data frontier wins; earlier debt, opaque dependency-obligation union, and waiters remain; timer is not reset. |
| pending-window | timer, waiter, soft bytes, hard admission, or drain | publishing | Snapshot latest candidate plus deterministic accumulated debt; wait for active builders to reach zero; call publisher without the coordinator lock. |
| publishing | success | idle or pending-window | Remove only captured debt; durable/recoverable advance; satisfy every waiter at or below durable; update EWMA; remaining debt starts a fresh window. |
| publishing | pre-meta/retryable failure | stalled-pending | Retain candidate/debt; fail captured waiters and hard-admission acknowledgements with the callback error; an explicit later waiter/drain retries. |
| publishing | ambiguous/post-meta result | poisoned | Retain debt until stop/reopen ownership cleanup; fail all waiters; every future enqueue, admission, wait, and drain returns `ErrRecoveryRequired`. |
| any live state | stop | stopped | Cancel callback context, fail waiters, join the sole scheduler goroutine, and report unpublished debt instead of silently discarding it. |

Hard admission is crossed only when pending debt is **greater than** 256 MiB or
65,536 commits. The crossing enqueue is owned before acknowledgement and wakes
publication; its acknowledgement waits for durable progress or returns the
publisher error. Ordinary work at or below both limits never waits for stable
I/O. Soft publication wakes at 64 MiB.

The adaptive delay is `clamp(20 * EWMA(service duration), 10ms, 100ms)`. EWMA
uses weight 1/8 for the latest successful service duration. Retryable failures
do not update it.
