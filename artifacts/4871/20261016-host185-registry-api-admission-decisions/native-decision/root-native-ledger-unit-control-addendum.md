# Canonical ledger unit-control adaptation

This adds an explicit test disposition to source selection63736977b62d008237db816afd58f5d7b98a3a95dc8067c0eb3fc4de37e49201. The sole source writer, original scope and no-runtime policy are unchanged. The complete frozen minimum01 source preserves the original tests and caps as historical evidence.

The protected PUBLIC SAME-Request Q32/1MiB/4096-call fixture and all of its oracles remain byte-for-byte unchanged. This selection confers no replacement product quantum, Q32 acceptance, native-publication permission or timing grant.

The following legacy allocator UNIT controls use literal32 as an implementation-size assumption for positive canonical construction/adoption, rather than as the protected public contract. Explicitly adapt their positive construction/admission helper to the exact checked source-derived bound of the new operation, including caller validation, full fixed-key discovery/copies, actual owner traffic and activation. Keep a finite supplied work budget; do not use nil/unlimited work to manufacture positive native coverage. Assert that actual counters stay inside the selected bound. Preserve every original slot/canonical equality, real foreground stale/conflict, cancellation, ownership and no-partial-effects assertion:

- TestPrivateCOWPreparationUsesOriginalSlotOnlyAtAdoptionV6.
- TestPrivateCOWPreparationCannotAdoptOverRealForegroundWriterV6.
- prepareAtomicPrivateCandidateV6 and its callers TestPrivateCOWAtomicAdoptionActivationRefusesBeforeSlotOrLedgerEffectsV6, TestPrivateCOWAtomicAdoptionCannotReplaceForegroundTxnV6, TestPrivateCOWMetadataClaimLeavesSharedDATAWithOriginalTxnV6, TestPrivateCOWLateDATAClaimConflictHasNoPartialEffectsV6.
- privatePreparedBoundTestV6 for BoundCaptureCanonicalAndStale and PublishedCleanupPreservesCanonical, including the latter's positive AdoptAndActivate construction allowance.

Retain the one-record refusal assertions and add discriminating below-admission/exact-admission controls using the SAME actual operation, not a copied predicate. Preserve a named Q32 refusal probe for the newly non-fitting atomic canonical operation with no ledger/slot/owner effect; do not convert repeated refusal to progress or claim it is the original public test. Document precisely which old literal assumptions changed and why in the packet.

PartialClone/AbandonedVector capture-only controls retain their existing32/1MiB budgets; reconcile their expected ORIGINAL owner baseline to the newly admitted canonical storage, not by dropping retained objects from accounting. If a different unit test requires adaptation, identify it before broadening this disposition. All new representation, fullretention, source-derived pricing and original last-alias oracles remain required.
