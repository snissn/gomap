# Current COW diagnostic closeout

V1 and V3 expose harness integration mistakes; V2 exposed a public Get missing-key oracle mistake. V3 serial semantics passed, but its first continuing-write baseline deadlocked and no history cell completed. Original Q32 remains RED; native ACK is zero.

The existing contract already separates visibility/fence authority from physical Finish. The next design must reuse the original conditional mutation/visible install, commitPublishedSeal, transaction Consume and ReleaseNativePublicationTurnV6. SAME Request/Cut must protect exact current selection until the accepted visibility boundary; original physical receipt/custody survives through Finish/ACK. No second publisher or new authorization flag is needed.

Exact source references, proposed caller changes and three focused controls are in design-contract-note.json. Original failures, raw goroutine stacks, source freezes and cleanup identities are preserved in the v1-v3 packets. No history-scaling or end-to-end bound is claimed.
