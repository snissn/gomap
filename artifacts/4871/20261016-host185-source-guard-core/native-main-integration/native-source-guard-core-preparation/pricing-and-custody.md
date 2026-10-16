# Actual price, retention and mutation ledger

All expressions use actual `unsafe.Sizeof` in the frozen source. No architecture-specific numeric sizeof or runtime evidence is claimed. Let O=Owner, S=PendingMutationWork, B=binding, E=EnrollmentWork, R=ReaderRegistry, G=SourceGuardRef, C=SourceGuardConstruction, D=SourceGuardWithdrawal, L=readerLeaf, H=readerBranch, M=guardMutation, A=SourceGuardAccounting. Work pairs are (records,bytes); containing storage uses the maintained Q charge.

| Actual local operation | Work row |
| --- | --- |
| Owner resumed entry | (1,2O+2S) on every invocation, including terminal calls |
| Separate binding / active enrollment touch | (1,2B) / (1,2E), in addition to actual Owner/state and concrete lease rows |
| Owner terminal observe | (1,2S+O) |
| Owner ACK | (1,2S+2O+sizeof(PendingMutationTraffic)) |
| Retain / transfer original guard cell | (1,2G+2L+2R) / (1,4G+L+2R) |
| Construct / withdraw / cancel entry | (1,2C+2G+2R) / (1,2D+2G+2R+2L) / (1,2C+2R+2G) |
| Real speculative leaf+branch allocation | (2,L+H), after Q(L)+Q(H) Owner admission |
| Saved structural step | (1,2M+2R+4H+2L) |
| Actual splice | (2,2L+4H), additional to saved step |
| Participant entry | (1,2D+2R+2G+2L) under registry.mu, in actual Owner child vector |
| Construction / withdrawal ACK | (1,2C+2R+A) / (1,2D+2R+A) plus real child ACK/reset rows |
| Field-wise reset | (1,2C) / (1,2D), after ACK only |

The COW lease declares resize and close each (1,2*sizeof(COWExternalLease)+2*sizeof(COWBudget)). The original shrink engine retains its actual per-binding/resize/dispose/close/reset rows. Cooperative capability preflight additionally checks the full repeated entry and shrink prefix against the original total record/byte limit; remaining allowance is a separate admission question. The positive controls retain RecordLimit=32 and preconsume only the intended prefix. No allowance is derived by substituting a smaller temporary limit.

`CooperativeMutationPricesForTestV6` and `SourceGuardPriceForTestV6` forward the actual typed rows. They are local rows, not whole-call envelopes: caller/entry prefixes and actual concrete children must be joined. `SourceGuardAccounting.OwnerRemoval` INCLUDES its participant/registry traffic; `IncludedRegistry` is only a breakdown and must not be added again. Cancellation reports its own child removal and included-registry breakdown. Owner addition, constructor structural work and unused-storage refund are separate operations. Public source entry/ACK/reset traffic is already in the actual caller work vector, outside internal child counters.

Retention: registry header remains on its existing metadata Owner. New floors admit separate leaf+branch peak. Constructor owns speculative/unused nodes until actual scrub and refund; last withdrawal owns actual detached debit until original removal ACK. Inline state is charged through its original containing parent and cannot outlive it. No node/parent alias survives refund. Unsupported or failed cleanup preserves SAME original debt/edge/header; no GC refund is asserted. Reset preserves RemovalWork mutexes rather than copying them.

## Owner mutation gate audit

| Existing family | Gate |
| --- | --- |
| Add/AddPending and ordinary resize | resizeLocked helps before own change |
| Enroll and pending enrollment begin/step/unwind/relock | help before binding-list/active-enrollment changes |
| ActivateProducer / TransferPending | help before accounting; paired Owner order preserved |
| atomic AddWithWork / RemoveWithTeardownWork / GrowInstallShrinkWithWork | help before actual admission/install/removal |
| original close, removeUnserialized, enrollment close | help before disposal or shrink |
| Retire | help; unresolved cleanup retained as failure before terminal retirement |
| geometry | reports pending mutation as active enrollment; read-only observation does not help |

The old private `releaseClosedBindingsLocked` has no caller. Registry slow insertion/removal, original positive unregister and native positive detach help before topology changes; fast cohorts preserve their original atomic protocol. Original header cleanup gate includes structural slot, live guards and unacknowledged continuations. Intermediate minimum is conservative; completed repaired root minimum is exact. This is not a global continuing-writer proof.
