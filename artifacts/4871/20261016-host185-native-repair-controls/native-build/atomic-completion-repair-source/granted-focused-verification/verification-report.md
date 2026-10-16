# Authorized focused verification — stopped on first failure

Compile-only passed. Focused normal failed in test fixture setup before the N3 staged constructor. Focused race was not executed. No source repair, retry, actual33 route, native enqueue/activation/fence/Finish or benchmark ran.

Frozen source: 10,342 files; manifest `dd03a710cb0a0c277465d6fd5f913c7455b4f3cad4515503516e39210a39e4db`. Both commands have full before/after manifests equal to the freeze. Full archived source was hash-verified.

Compiler: pinned go1.26.3; go binary SHA256 `d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65`; compile binary SHA256 `3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1`. See identity.json for paths/archive/grant.

## Commands and process closure

- compile-only: exit 0, 65.682s, PID 793554, PGID 793554; leader joined; subreaper verified; group and direct/adopted children empty; source unchanged.
- focused-normal: exit 1, 2.503s, PID 794805, PGID 794805; leader joined; subreaper verified; group and direct/adopted children empty; source unchanged.

## Exact normal results

24 RUN entries: 18 top-level tests and 6 subtests. 17 top-level PASS, 1 top-level FAIL, 0 top-level SKIP. Exact RUN/pass/fail names are also retained in focused-normal/result.json.

Passed top-level tests:

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
- TestNativeFiniteCursorAdmissionSizesV6

Failed:

- TestNativePagerOperationsPartialConstructionAndCancellationV6 / before-output: nil pointer panic at native_pager_operations_construction_v6_test.go:32 while calling idx.primary.Pager(). The ordinary empty DB fixture has nil idx.primary. This precedes Snapshot setup and actual staged callback construction; no conclusion about N3 cleanup is drawn from this failed control.

DB process panic prevented later DB test execution. The same already-authorized multi-package go test command continued with mvcc cursor pricing and then exited nonzero. No later command was started.

The actual-size/pricing lines in focused-normal/run.log are exact-source diagnostic outputs, not an evaluated total, runtime allowance, Q32 progress or publication qualification. Q32 remains RED/ACK0. Source/runtime are idle.
