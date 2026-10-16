# Fixture successor verification — PASS

Exact frozen manifest `83b71641be6460d2ece624fec51b8a84ad3ecea158844eb1c61b0828a72f0fcd`, 10,342 files. Only native_pager_operations_construction_v6_test.go differs from prior rejected fixture. All production bytes unchanged. Full source archive verified; all three before/after manifests match the frozen source.

Compiler: go1.26.3; go binary `d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65`; compile binary `3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1`. Original proposal argv/env/regex/bounds unchanged.

## Results

- compile-only: PASS, 40.872s, PID/PGID 797800/797800; leader joined, verified subreaper, no remaining group/adopted children. 0 RUN entries, 0 top-level passes, 0 failures, 0 skips.
- focused-normal: PASS, 3.035s, PID/PGID 798555/798555; leader joined, verified subreaper, no remaining group/adopted children. 41 RUN entries, 20 top-level passes, 0 failures, 0 skips.
- focused-race: PASS, 98.871s, PID/PGID 798625/798625; leader joined, verified subreaper, no remaining group/adopted children. 41 RUN entries, 20 top-level passes, 0 failures, 0 skips.

Normal and race each executed the same 20 top-level tests / 41 total RUN entries. Exact passed top-level names:

- TestNativeAtomicOwnerGrowthAdmitsBeforeEffects
- TestNativeAtomicOwnerGrowthPricesOriginalRollback
- TestNativeOwnerGrowthAdapterPreservesYield
- TestNativeOwnerGrowthAdapterPreservesUnsupported
- TestOriginalCleanupCustodyKeepsSameRegistryUntilActualCompletion
- TestOriginalCleanupCustodyOverflowRefusesBeforeEffects
- TestOriginalCleanupCustodyTypedGrowthAndRefundV6
- TestOriginalCleanupCustodyTypedUnsupportedAndOrdinaryV6
- TestOriginalCleanupCustodyTypedClosingAndOverflowV6
- TestNativePublishedOutputAdmissionBeforeAdoptionV6
- TestNativeSnapshotPublicationUsesSameLeafOperandV6
- TestNativeIncompleteClosePreservesOriginalRuntimeV6
- TestNativeCompletedCloseRetainsOrdinaryTeardownV6
- TestNativeBindingCrossesOriginalTeardownGateV6
- TestNativeNoDebtClosePreservesOrdinaryHookErrorV6
- TestNativeFiniteOriginalClosureSizesV6
- TestNativePagerOperationsPartialConstructionAndCancellationV6
- TestNativePublicationRemainderAndFloorV6
- TestNativeStablePagerPartialTokenCancellationV6
- TestNativeFiniteCursorAdmissionSizesV6

Exact subtests are retained in focused-normal/result.json and focused-race/result.json. New N3 all seven partial/resumption/error cases pass, including original typed refunds and full metadata/refcount/counter/wrapper restoration. Existing six partial-token cancellation cases pass without cap/vector changes. N1/N2 and custody/growth/pure-pricing controls pass.

Prior first failure is preserved in atomic-completion-repair-source/granted-focused-verification; no relabeling or deletion. No actual33 route, native enqueue/activation/fence/Finish, benchmark, observer runtime or numeric allowance was executed/selected. Q32 remains RED/ACK0. Source froze through final verification; this exact archive remains immutable while the separately authorized source-only observer successor may develop.
