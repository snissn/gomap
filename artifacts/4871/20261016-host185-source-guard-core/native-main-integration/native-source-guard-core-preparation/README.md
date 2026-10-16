# Source-guard core release

The 17 paths in `after.json` are final and released for root's combined freeze and compile. This is the connected Owner/ReaderRegistry core checkpoint, not the later producer-role integration. No compiler, test, benchmark or product runtime was executed by this worker. Source formatting used the pinned Go 1.26.3 gofmt only.

Parent: registry/API source04, manifest `b0568bd82a38112d26edf8015408e034479c58053146f54c6f1ce7da1f3e2628`, archive `6fe19cb928916c791730a56f177e7cb21677d40fed5afed4a4374e828fecdc56`. The plain before/after maps cover exactly 17 selected paths: ten modifications and seven additions, with no deletion or existing mode change. The preimage archive contains the ten original paths; postimage archive contains all seventeen. Root owns the whole-tree freeze/index/Git and every runtime command.

## Core contract

Original admitted parents embed `SourceGuardRef`, `SourceGuardConstruction`, and `SourceGuardWithdrawal`; admission must cover their actual containing sizes. The SAME ReaderRegistry header owns its single borrowed structural slot. No new keeper, Owner, registry or heap continuation exists. `SourceGuardIdentity{Generation, Source}` validates exact physical DATA generation and immutable source, but scalar identity is not authority: only the original self-bound counted cell authorizes retain/transfer/withdrawal. Ordinary reader APIs reject guard leaves.

`ConstructSourceGuard`, `RetainSourceGuard`, `TransferSourceGuard`, `WithdrawSourceGuard`, and `CancelSourceGuardConstruction` accept the original work vector. Their `Original` counterparts bind ordinary origin before effects. Native-origin unsupported withdrawal cannot be relabeled ordinary. Constructor cancellation includes reserved-cell-before-admission, helper-committed growth and failed structural search. Successful terminal calls still require ACK; keep the original containing parent admitted until ACK clears all aliases. Reusable parents use `ResetAcknowledged(w)`, which preserves embedded RemovalWork mutexes.

Owner exposes `StepPendingMutation`, `StepPendingRemoval`, `CancelPendingMutation`, `PendingMutationCommitted`, and `AcknowledgePendingMutation`. Original inline state freezes one finite binding list plus the active pending enrollment. Capability discovery, forward resize, reverse rollback, participant splice and original shrink preserve saved frontiers. `BoundedMutationLease` is an explicit concrete resize/close price capability; the maintained COWExternalLease supplies it. WorkLease alone is insufficient. Ordinary Lease-only roles remain supported under their original ordinary policy.

A last-drop participant runs under SAME Owner.mu then registry.mu. It preflights governing leases before irreversible detach, freezes last-ref authority, derives actual debit from the protected current splice, and clears node cursors. Registry paths release registry.mu before entering Owner/refund; no public RemovalWork, budget, cursor or containing-parent lock is taken while registry.mu is held. Read-only Bytes/minimum queries never help.

Every slow ordinary topology mutator helps the saved registry operation before its own change. Every original Owner binding/accounting mutator helps its pending operation before its own change. Helpers use explicit fresh work vectors through the SAME engine; safe completed rollback retains its first error only for the originating operation, while unresolved rollback/cleanup debt blocks interference. Native/helper and combined traffic overflow is a sticky error before effects.

## Pending producer integration

No production source role has yet been enrolled. The next coherent integration must cover current/snapshotView/PRIMARY root; both selected slots and both one-hop proofs; coordinator visible/durable/pending/ambiguous candidates; RecoverableRootSet; DATA directory and writer/ordered/rewrite/build sources; native partial birth/cancel; startup/offline/rebuild/vacuum and Close. Each uses exact physical generation/floor and ancestor overlap. Withdraw at actual source retirement BEFORE generation-empty teardown, otherwise the header cleanup gate correctly self-pins. No acquisition scan was removed.

## Controls and limits

`proposed-commands.json` is immutable and unexecuted: three normal package commands, then their three race counterparts; 64 named tops and 28 named children per mode (retainedalloc31/10, lifecycle31/18, memtable2/0). Exact test hashes and child labels are bound there. Root must supply the actual runtime grant/receipts. Root's separate compile-only command remains root-owned; no worker compile claim exists.

New controls include SAME-state native-only 80-governor completion with multiple Q32 turns, independent ordinary-helper completion, active-enrollment/pair mutations, reverse failure/counter overflow, original last-drop capability refusal, actual topology-derived refund, depth63 repair, right-subtree first hole5, copied/stale cell rejection, original ordinary Lease-only behavior, origin refusal, cancellation/ACK/reset reuse, and real ordinary churn after each native yield.

Full 2^63 handle occupancy is not physically exercised: that refusal is source-traced, while its shared speculative-node scrub/refund path is exercised with real admitted nodes. The unsupported-debt negative deliberately retains the SAME original guard/header obligation; it does not claim cleanup completion. Source controls are unexecuted and require root review/runtime. This release does not establish continuing-writer reclamation, complete producer closure, Q32 publication, performance or a changed public allowance. Original Q32/1MiB, acquisition scans, protected workloads and Finish/old-reader/slot/failure oracles remain unchanged.
