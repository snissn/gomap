# Existing controls for ordinary/native source composition

Disposition: **bounded proposal for root selection; no runtime authorized or executed**. Independent read-only inventory by `/root/ordinary_review`, explicitly routed `gpt-6.1-sol/high`; no construction role or children. I did not inspect `native-integrated` or change source, Git, GitHub, limits or test bodies.

This is a Linux control proposal for the future frozen integrated source. It checks the composed ordinary behavior and isolated native previsibility/storage primitives. It is neither source acceptance of that future candidate nor native publication/qualification evidence.

## Immutable inputs and binding

- Ordinary `6d3fc3a6e3582810d7c80fc46b784a9b30fada80`, tree `dd50b7ea3ca04193f4fa00decf15cbc98edb68d1`, identical landed ordinary merge `0149d78dba2574b09d7119d2631ec78a144e088d`. All **10,258** current ordinary files matched `ordinary-main-integration/source-final.json`, SHA256 `8f32bf3ea51c2b082f5c9cfce2b8066e6202ae89ea6e696ef44899ffb5d09476`.
- Native was read **only from the immutable archive**. All **10,349** entries matched `native-build/source-witness-repair-source/source-frozen.json`, SHA256 `4b230aa27b6a51d2394d2cd237403f956ac4897afb4b76e9e2e47bfb39947fd2`; archive SHA256 `44682995595a636fe99ab3febcebca1802f8992574eb4fab8dbef49f355ea03c`.
- Root selection SHA256 `75a6aeb46b19af4d14f89bffa6954672d6e25b15d2e3f410567284903ea8f253`; final integration plan SHA256 `cfb39f69c38fbc2c0369dfab9d789a92e952d9fd93e48589ff1c297eadd9d98d`. Exact selected file hashes, line anchors, route classifications and argv are in the companion JSON.

## Proposed scope

**13 sequential calls: 7 normal / 6 race, each `-count=1`; 44 normal / 41 race top-level tests.** This selects a small slice of six packages, not their full suites. One M4 cell runs normal-only; the two allocation assertions run normal-only. Race uses the remaining same selectors, including the genuine ordinary publication and recovery tests. No benchmarks run (`-bench` is absent).

Root should bind and review the actual integrated freeze first, then select this scope or a subset explicitly. Use the already pinned Go1.26.3 compiler, offline dependencies, `GOMAXPROCS=2`, `GOMEMLIMIT=8GiB`, fastdisk TMP/cache, `-p=1 -parallel=1`, and no competing native runtime. Compilation time is additional; these are safety deadlines, not claimed runtimes. The proposal retains original fixture deadlines, including 5s admission waits and the M4 2m operation context.

| Package / group | Normal | Race | What is actually exercised |
|---|---:|---:|---|
| `TreeDB/db` | 28 | 27 | Real ordinary Set/topology/cache/one-shot/owner/vacuum/slot recovery, plus explicitly isolated native capture/construction/cancellation wrappers |
| `TreeDB/internal/rootpublication` | 8 | 7 | Immutable diagnostic strings/counters, concurrent physical release, selected raw-Union refusal, empty clone custody, exact namespace identity |
| `TreeDB/pager` | 2 | 2 | **Actual standalone native write and retained-FD fdatasync**, refusal, adjacent cache bits, dirty membership, EBADF and retired inode |
| `TreeDB/internal/valuelog` | 2 | 2 | Actual original mapping/file physical close, fresh impossible quantum versus residual yield |
| `TreeDB/caching` | 2 | 2 | Ordinary post-refresh fence/old cut and admitted pointer-read/Close join |
| `TreeDB` default outer-leaf | 1 | 1 | Real wrapper publication, persistent vlog+leaf parents, joint vacuum, physical cut after Close and reopen |
| `TreeDB` one M4 cell | 1 | 0 | Pointer-values/backend-vacuum-online only: real DATA debt, original >=40% shrink, persistent bytes and reopened digest |

The DB owner physical receipt has a fixed 80-enrollment fixture and a maximum 4096 cleanup turns. The file-close fixture writes about 2.7MB and has a maximum 10000 cleanup turns. Stable pager construction uses six cases; owned operations seven. Those are finite test fixtures, not full native graph or cost bounds. The M4 cell seeds 4x1536 pointer records, performs three actual DATA CompactIndex rebuilds and two horizon writes; this is the largest proposed setup. It does not measure M0/CV, latency, cohort scaling, 8x output or default-path costs.

## Existing controls and route audit

### TreeDB/db

- `TestCurrentStateValueLogConsumersRetainExactSet` (ordinary source `TreeDB/db/state_value_log_lifetime_test.go:22`; normal + race): ordinary real DB publication/Refresh and root/Set lifetime. 10 consumer routes; 3 topology routes in each topology test; competing Refresh is genuine; capture callback remains nonrecursive.
- `TestTopologyPublicationPreservesPrimaryRootLifetime` (ordinary source `TreeDB/db/state_value_log_lifetime_test.go:147`; normal + race): ordinary real DB publication/Refresh and root/Set lifetime. 10 consumer routes; 3 topology routes in each topology test; competing Refresh is genuine; capture callback remains nonrecursive.
- `TestTopologyPublishersWaitForSnapshotSetCapture` (ordinary source `TreeDB/db/state_value_log_lifetime_test.go:270`; normal + race): ordinary real DB publication/Refresh and root/Set lifetime. 10 consumer routes; 3 topology routes in each topology test; competing Refresh is genuine; capture callback remains nonrecursive.
- `TestOneShotReadCustodyMatchesCapturedResources` (ordinary source `TreeDB/db/one_shot_custody_test.go:14`; normal + race): ordinary public/private one-shot capture, persistent pointer read and Close. Private empty reader avoids physical Set/Manager aliases; exported Snapshot keeps original custody. Concurrent Close joins real goroutines.
- `TestOneShotLiveValueLogReadSurvivesConcurrentDBClose` (ordinary source `TreeDB/db/one_shot_custody_test.go:71`; normal + race): ordinary public/private one-shot capture, persistent pointer read and Close. Private empty reader avoids physical Set/Manager aliases; exported Snapshot keeps original custody. Concurrent Close joins real goroutines.
- `TestOneShotEmptyReadRetainsOnlyLegacyStateWrapper` (ordinary source `TreeDB/db/one_shot_custody_test.go:150`; normal + race): ordinary public/private one-shot capture, persistent pointer read and Close. Private empty reader avoids physical Set/Manager aliases; exported Snapshot keeps original custody. Concurrent Close joins real goroutines.
- `TestOneShotReadCapturePublicationAndRelease` (ordinary source `TreeDB/db/one_shot_read_test.go:110`; normal + race): synthetic ordinary snapshot view capture and rejection. Uses valueLogPublicationMu/rootReuse locks and reader registry; no native producer.
- `TestPrimaryDataCertificateV6RefusalKeepsAcceptedCache` (ordinary source `TreeDB/db/primary_review_repairs_test.go:35`; normal + race): ordinary actual SetSync/activation refusal/Close and last iterator alias. Cache refusal has 4 seeded/phase cases; iterator has owned/unowned cases. Does not exercise native factored Activate callback.
- `TestPrimaryDataCertificateV6TerminalDetachesBeforeWithdrawal` (ordinary source `TreeDB/db/primary_review_repairs_test.go:126`; normal + race): ordinary actual SetSync/activation refusal/Close and last iterator alias. Cache refusal has 4 seeded/phase cases; iterator has owned/unowned cases. Does not exercise native factored Activate callback.
- `TestPrimaryClosedIteratorDetachesOriginalRootV6` (ordinary source `TreeDB/db/primary_review_repairs_test.go:218`; normal + race): ordinary actual SetSync/activation refusal/Close and last iterator alias. Cache refusal has 4 seeded/phase cases; iterator has owned/unowned cases. Does not exercise native factored Activate callback.
- `TestPrimarySharedTransactionV6ReusesExactDataCertificate` (ordinary source `TreeDB/db/primary_shared_transaction_v6_test.go:11`; normal + race): ordinary PRIMARY-only commits followed by actual DATA materialization. 3 PRIMARY-only writes preserve SAME DATA certificate; 3500-byte key forces real DATA replacement; old Snapshot remains readable.
- `TestPrimaryOwnerWorkActualArenaNamespaceAndExternalContainingReceipt` (ordinary source `TreeDB/db/primary_arena_release_work_test.go:19`; normal + race): original ordinary owner plus isolated native physical release receipt. Actual arena/namespace and 80 fixed enrollments, external containing lease; bounded work cleanup loop at most4096. Sticky sibling test intentionally retains failed physical debt. No publication receipt/driver.
- `TestPrimaryOwnerOrdinaryNonlastPreservesStickySiblingError` (ordinary source `TreeDB/db/primary_arena_release_work_test.go:158`; normal + race): original ordinary owner plus isolated native physical release receipt. Actual arena/namespace and 80 fixed enrollments, external containing lease; bounded work cleanup loop at most4096. Sticky sibling test intentionally retains failed physical debt. No publication receipt/driver.
- `TestPrimaryOwnerOrdinaryNonlastHasNoUnadmittedReceiptAllocation` (ordinary source `TreeDB/db/primary_arena_release_work_test.go:131`; normal only): ordinary owner nonlast no-allocation check. AllocsPerRun20; normal only, no timing or allocation-profile qualification.
- `TestNativeIncompleteClosePreservesOriginalRuntimeV6` (frozen native archive `TreeDB/db/native_close_barrier_v6_test.go:45`; normal + race): native pre-enqueue synthetic receipt binding/join/cancellation with ordinary DB Close. Calls BindNativePublicationV6/BindNativeMVCCPublicationV6, JoinNativePublicationsV6, CompleteNativePublicationV6(receipt,nil,false), Stop; Publisher is a must-not-call assertion. Incomplete control clears only its injected failure for fixture cleanup. No native Enqueue/adoption/publication.
- `TestNativeCompletedCloseRetainsOrdinaryTeardownV6` (frozen native archive `TreeDB/db/native_close_barrier_v6_test.go:88`; normal + race): native pre-enqueue synthetic receipt binding/join/cancellation with ordinary DB Close. Calls BindNativePublicationV6/BindNativeMVCCPublicationV6, JoinNativePublicationsV6, CompleteNativePublicationV6(receipt,nil,false), Stop; Publisher is a must-not-call assertion. Incomplete control clears only its injected failure for fixture cleanup. No native Enqueue/adoption/publication.
- `TestNativeBindingCrossesOriginalTeardownGateV6` (frozen native archive `TreeDB/db/native_close_barrier_v6_test.go:111`; normal + race): native pre-enqueue synthetic receipt binding/join/cancellation with ordinary DB Close. Calls BindNativePublicationV6/BindNativeMVCCPublicationV6, JoinNativePublicationsV6, CompleteNativePublicationV6(receipt,nil,false), Stop; Publisher is a must-not-call assertion. Incomplete control clears only its injected failure for fixture cleanup. No native Enqueue/adoption/publication.
- `TestNativeNoDebtClosePreservesOrdinaryHookErrorV6` (frozen native archive `TreeDB/db/native_close_barrier_v6_test.go:145`; normal + race): native pre-enqueue synthetic receipt binding/join/cancellation with ordinary DB Close. Calls BindNativePublicationV6/BindNativeMVCCPublicationV6, JoinNativePublicationsV6, CompleteNativePublicationV6(receipt,nil,false), Stop; Publisher is a must-not-call assertion. Incomplete control clears only its injected failure for fixture cleanup. No native Enqueue/adoption/publication.
- `TestNativeMVCCSnapshotActualCaptureAndCancellation` (frozen native archive `TreeDB/db/native_mvcc_snapshot_capture_test.go:9`; normal + race): native previsibility actual Snapshot capture and cancellation. BeginNativeMVCCSnapshotV6 and CloseStep only; zero-work refusal and finite capture cleanup. No delete producer or enqueue.
- `TestNativeStablePagerPartialTokenCancellationV6` (frozen native archive `TreeDB/db/native_stable_pager_capture_v6_test.go:10`; normal + race): native previsibility warm capture/token construction and cancellation. 6 cases, actual lazy PRIMARY initialization with original warm capture and close; boundaries4/12/13/14/15 plus after-retain. No finaldriver.
- `TestNativePagerOperationsPartialConstructionAndCancellationV6` (frozen native archive `TreeDB/db/native_pager_operations_construction_v6_test.go:13`; normal + race): native previsibility stable pager operation construction and cancellation. 7 states, warm capture/close precedes baseline; original Owner/borrowed file; same object resume; zero-work refusal; no activation/Enqueue.
- `TestNativeRootPreparationSourceWitnessV6` (frozen native archive `TreeDB/db/native_mvcc_root_preparation_v6_test.go:135`; normal + race): tiny original source-root witness/queue/refund primitive. 3 accepted controls (7 total RUN nodes including4 terminal children). Drain calls published-close wrapper only to reject an unfinished receipt, then explicitly synthetic phase9. Successful ContinueFinish->dispose remains SOURCE only; not actual successful publication Finish.
- `TestNativeRootPreparationSourceTerminalV6` (frozen native archive `TreeDB/db/native_mvcc_root_preparation_v6_test.go:194`; normal + race): tiny original source-root witness/queue/refund primitive. 3 accepted controls (7 total RUN nodes including4 terminal children). Drain calls published-close wrapper only to reject an unfinished receipt, then explicitly synthetic phase9. Successful ContinueFinish->dispose remains SOURCE only; not actual successful publication Finish.
- `TestNativePublishedRootPreparationDrainV6` (frozen native archive `TreeDB/db/native_mvcc_root_preparation_v6_test.go:289`; normal + race): tiny original source-root witness/queue/refund primitive. 3 accepted controls (7 total RUN nodes including4 terminal children). Drain calls published-close wrapper only to reject an unfinished receipt, then explicitly synthetic phase9. Successful ContinueFinish->dispose remains SOURCE only; not actual successful publication Finish.
- `TestNativePublishedOutputAdmissionBeforeAdoptionV6` (frozen native archive `TreeDB/db/native_activation_output_admission_v6_test.go:12`; normal + race): synthetic native activation wrapper PRE-adoption refusals and snapshot publication scope. First calls activateWithWorkV6 but all3 vectors refuse before supplied allocator callback (calls=0). Second only beginSnapshotPublicationV6/finish(false); state/snapshotView remain nil. No adopted activation.
- `TestNativeSnapshotPublicationUsesSameLeafOperandV6` (frozen native archive `TreeDB/db/native_activation_output_admission_v6_test.go:38`; normal + race): synthetic native activation wrapper PRE-adoption refusals and snapshot publication scope. First calls activateWithWorkV6 but all3 vectors refuse before supplied allocator callback (calls=0). Second only beginSnapshotPublicationV6/finish(false); state/snapshotView remain nil. No adopted activation.
- `TestPrimaryJointVacuumV6SuccessfulReplacement` (ordinary source `TreeDB/db/primary_joint_vacuum_v6_test.go:20`; normal + race): ordinary actual joint vacuum, held readers, physical-cut refusal, capsule slot recovery. Pointer control2 manifest/directory cases; corrupt each actual PRIMARY slot after Close and reopen independently; restore exact image. No crash subprocess or native producer.
- `TestPrimaryJointVacuumV6PointersAndIndependentFallback` (ordinary source `TreeDB/db/primary_joint_vacuum_v6_test.go:53`; normal + race): ordinary actual joint vacuum, held readers, physical-cut refusal, capsule slot recovery. Pointer control2 manifest/directory cases; corrupt each actual PRIMARY slot after Close and reopen independently; restore exact image. No crash subprocess or native producer.

### TreeDB/internal/rootpublication

- `TestGenericResourceDiagnosticsOwnAllNamespaceText` (ordinary source `TreeDB/internal/rootpublication/resource_generic_diagnostics_test.go:14`; normal + race): generic/selected resource primitives; concurrent final physical release. 7 string ownership cases; 8 readers x100 iterations; mixed namespace comparison under real ns.mu; released diagnostics/registry/shared counter identity; raw selected Union/flat representative/pin/scrubbed-empty refusal. No DB/coordinator publication.
- `TestGenericResourceDiagnosticsSurviveConcurrentPhysicalRelease` (ordinary source `TreeDB/internal/rootpublication/resource_generic_diagnostics_test.go:65`; normal + race): generic/selected resource primitives; concurrent final physical release. 7 string ownership cases; 8 readers x100 iterations; mixed namespace comparison under real ns.mu; released diagnostics/registry/shared counter identity; raw selected Union/flat representative/pin/scrubbed-empty refusal. No DB/coordinator publication.
- `TestRawLogicalUnionRefusesSelectedAliasesBeforeEscape` (ordinary source `TreeDB/internal/rootpublication/resource_generic_diagnostics_test.go:216`; normal + race): generic/selected resource primitives; concurrent final physical release. 7 string ownership cases; 8 readers x100 iterations; mixed namespace comparison under real ns.mu; released diagnostics/registry/shared counter identity; raw selected Union/flat representative/pin/scrubbed-empty refusal. No DB/coordinator publication.
- `TestGenericAbsentDiagnosticBackingDoesNotAllocate` (ordinary source `TreeDB/internal/rootpublication/resource_generic_diagnostics_test.go:206`; normal only): generic absent-backing allocation assertion. AllocsPerRun100 normal only; no public cost or timing inference.
- `TestGenericEmptyCloneUsesAbsentClosure` (ordinary source `TreeDB/internal/rootpublication/resource_absent_closure_test.go:8`; normal + race): generic empty Set and independent dependency-directory custody. Exported Freeze remains owned; clone may be absent only with zero independent edges.
- `TestEmptyDirectoryCloneRetainsIndependentCustody` (ordinary source `TreeDB/internal/rootpublication/resource_absent_closure_test.go:30`; normal + race): generic empty Set and independent dependency-directory custody. Exported Freeze remains owned; clone may be absent only with zero independent edges.
- `TestStableNamespaceParentGenerationTracksExactParentIdentity` (ordinary source `TreeDB/internal/rootpublication/resource_identity_unix_test.go:14`; normal + race): Unix real retained namespace parent/child identity primitives. Real child rename/replacement must refuse exact original proof Bind; no FIFO-blocking test or DB publication.
- `TestStableNamespaceCreationProofRejectsReplacementBeforeBind` (ordinary source `TreeDB/internal/rootpublication/resource_identity_unix_test.go:330`; normal + race): Unix real retained namespace parent/child identity primitives. Real child rename/replacement must refuse exact original proof Bind; no FIFO-blocking test or DB publication.

### TreeDB/pager

- `TestNativeWordInvalidationKeepsOrdinaryMaskedPolicyV6` (frozen native archive `TreeDB/pager/native_file_fence_linux_v6_test.go:18`; normal + race): standalone Linux pager native write/fdatasync physical primitives. ACTUAL native mmap write and native retained-FD index-data fence, including EBADF, retired inode and dirty-geometry preservation. These physical effects must be named in root grant; not DB/capsule/native finaldriver publication. Concurrent immutable adjacent verification100iterations.
- `TestNativeRetainedFileFencePreservesDirtyAndRetiredIdentityV6` (frozen native archive `TreeDB/pager/native_file_fence_linux_v6_test.go:75`; normal + race): standalone Linux pager native write/fdatasync physical primitives. ACTUAL native mmap write and native retained-FD index-data fence, including EBADF, retired inode and dirty-geometry preservation. These physical effects must be named in root grant; not DB/capsule/native finaldriver publication. Concurrent immutable adjacent verification100iterations.

### TreeDB/internal/valuelog

- `TestNativeMVCCOriginalFilePhysicalClose` (ordinary source `TreeDB/internal/valuelog/native_mvcc_file_close_test.go:16`; normal + race): standalone native original file/mapping physical CloseStep primitives. ~2.7MB real vlog, up to10000 bounded cleanup turns; fresh too-small vs residual yield then fitting quantum. Use landed portable File.Close ErrClosed terminal oracle, not native archive older Stat oracle. No native publication.
- `TestNativeOriginalMappingFreshQuantumTooSmall` (ordinary source `TreeDB/internal/valuelog/native_mvcc_file_close_test.go:72`; normal + race): standalone native original file/mapping physical CloseStep primitives. ~2.7MB real vlog, up to10000 bounded cleanup turns; fresh too-small vs residual yield then fitting quantum. Use landed portable File.Close ErrClosed terminal oracle, not native archive older Stat oracle. No native publication.

### TreeDB/caching

- `TestCOWMaintenancePostRefreshSettlementKeepsFenceAndOldCut` (ordinary source `TreeDB/caching/cow_maintenance_lifetime_test.go:406`; normal + race): ordinary COW maintenance refresh and held read-cut lifetime. 2 success/settlement-error cases; actual pointer checkpoint, same writer fence; settlement itself must not advance backend StateToken/checkpointRuns.
- `TestCOWDBCloseWaitsForAdmittedPointerReadAndDelayedOwnersClose` (ordinary source `TreeDB/caching/cow_pointer_read_test.go:307`; normal + race): ordinary COW admitted pointer read, DB Close, delayed Snapshot/iterator cleanup. Actual COW cache/backend; original5s admission deadline and20ms noncompletion check; original cowWait bound; no native delete/publication driver.

### TreeDB

- `TestPrimaryJointVacuumV6DefaultOuterLeafMaterializationReaderAndCut` (ordinary source `TreeDB/primary_joint_vacuum_v6_test.go:16`; normal + race): ordinary public wrapper default outer-leaf/vlog DATA materialization and joint vacuum. 200 forced-pointer values, actual DATA materialization via large key, two real parent directories, held cut refusal, actual joint vacuum, snapshot/cut after DB Close, reopen. No native driver.
- `TestIndexVacuumM4CertificationMatrix` (ordinary source `TreeDB/index_vacuum_m4_matrix_test.go:199`; normal only): ONE selected ordinary pointer-values/backend-vacuum-online matrix cell. Exact hierarchical selector below only.4x1536 seeded pointer writes plus3 REAL CompactIndex DATA rebuilds and2 horizon writes; original debt,>=40% shrink, persistent-log and reopened digest oracles. No performance/CV/8x-output claim. Other36cells excluded.

## Exact anchored selectors and deadlines

Every selector is anchored at both ends. These are **proposed**, not executable permission. Companion JSON retains each complete argv. For each normal command use `go test -p=1 -parallel=1 -run SELECTOR -count=1 -v -timeout=TIMEOUT PACKAGE`; race adds `-race`.

**normal-TreeDB-db** — `./TreeDB/db`, Go timeout `3m`, outer process deadline `300s`.

```text
^(TestCurrentStateValueLogConsumersRetainExactSet|TestTopologyPublicationPreservesPrimaryRootLifetime|TestTopologyPublishersWaitForSnapshotSetCapture|TestOneShotReadCustodyMatchesCapturedResources|TestOneShotLiveValueLogReadSurvivesConcurrentDBClose|TestOneShotEmptyReadRetainsOnlyLegacyStateWrapper|TestOneShotReadCapturePublicationAndRelease|TestPrimaryDataCertificateV6RefusalKeepsAcceptedCache|TestPrimaryDataCertificateV6TerminalDetachesBeforeWithdrawal|TestPrimaryClosedIteratorDetachesOriginalRootV6|TestPrimarySharedTransactionV6ReusesExactDataCertificate|TestPrimaryOwnerWorkActualArenaNamespaceAndExternalContainingReceipt|TestPrimaryOwnerOrdinaryNonlastPreservesStickySiblingError|TestPrimaryOwnerOrdinaryNonlastHasNoUnadmittedReceiptAllocation|TestNativeIncompleteClosePreservesOriginalRuntimeV6|TestNativeCompletedCloseRetainsOrdinaryTeardownV6|TestNativeBindingCrossesOriginalTeardownGateV6|TestNativeNoDebtClosePreservesOrdinaryHookErrorV6|TestNativeMVCCSnapshotActualCaptureAndCancellation|TestNativeStablePagerPartialTokenCancellationV6|TestNativePagerOperationsPartialConstructionAndCancellationV6|TestNativeRootPreparationSourceWitnessV6|TestNativeRootPreparationSourceTerminalV6|TestNativePublishedRootPreparationDrainV6|TestNativePublishedOutputAdmissionBeforeAdoptionV6|TestNativeSnapshotPublicationUsesSameLeafOperandV6|TestPrimaryJointVacuumV6SuccessfulReplacement|TestPrimaryJointVacuumV6PointersAndIndependentFallback)$
```

**normal-TreeDB-internal-rootpublication** — `./TreeDB/internal/rootpublication`, Go timeout `2m`, outer process deadline `240s`.

```text
^(TestGenericResourceDiagnosticsOwnAllNamespaceText|TestGenericResourceDiagnosticsSurviveConcurrentPhysicalRelease|TestRawLogicalUnionRefusesSelectedAliasesBeforeEscape|TestGenericAbsentDiagnosticBackingDoesNotAllocate|TestGenericEmptyCloneUsesAbsentClosure|TestEmptyDirectoryCloneRetainsIndependentCustody|TestStableNamespaceParentGenerationTracksExactParentIdentity|TestStableNamespaceCreationProofRejectsReplacementBeforeBind)$
```

**normal-TreeDB-pager** — `./TreeDB/pager`, Go timeout `2m`, outer process deadline `240s`.

```text
^(TestNativeWordInvalidationKeepsOrdinaryMaskedPolicyV6|TestNativeRetainedFileFencePreservesDirtyAndRetiredIdentityV6)$
```

**normal-TreeDB-internal-valuelog** — `./TreeDB/internal/valuelog`, Go timeout `2m`, outer process deadline `240s`.

```text
^(TestNativeMVCCOriginalFilePhysicalClose|TestNativeOriginalMappingFreshQuantumTooSmall)$
```

**normal-TreeDB-caching** — `./TreeDB/caching`, Go timeout `2m`, outer process deadline `240s`.

```text
^(TestCOWMaintenancePostRefreshSettlementKeepsFenceAndOldCut|TestCOWDBCloseWaitsForAdmittedPointerReadAndDelayedOwnersClose)$
```

**normal-TreeDB** — `./TreeDB`, Go timeout `3m`, outer process deadline `300s`.

```text
^(TestPrimaryJointVacuumV6DefaultOuterLeafMaterializationReaderAndCut)$
```

**normal-one-M4-pointer-cell** — `./TreeDB`, Go timeout `3m`, outer process deadline `300s`.

```text
^TestIndexVacuumM4CertificationMatrix$/^pointer-values$/^backend-vacuum-online$
```

**race-TreeDB-db** — `./TreeDB/db`, Go timeout `5m`, outer process deadline `420s`.

```text
^(TestCurrentStateValueLogConsumersRetainExactSet|TestTopologyPublicationPreservesPrimaryRootLifetime|TestTopologyPublishersWaitForSnapshotSetCapture|TestOneShotReadCustodyMatchesCapturedResources|TestOneShotLiveValueLogReadSurvivesConcurrentDBClose|TestOneShotEmptyReadRetainsOnlyLegacyStateWrapper|TestOneShotReadCapturePublicationAndRelease|TestPrimaryDataCertificateV6RefusalKeepsAcceptedCache|TestPrimaryDataCertificateV6TerminalDetachesBeforeWithdrawal|TestPrimaryClosedIteratorDetachesOriginalRootV6|TestPrimarySharedTransactionV6ReusesExactDataCertificate|TestPrimaryOwnerWorkActualArenaNamespaceAndExternalContainingReceipt|TestPrimaryOwnerOrdinaryNonlastPreservesStickySiblingError|TestNativeIncompleteClosePreservesOriginalRuntimeV6|TestNativeCompletedCloseRetainsOrdinaryTeardownV6|TestNativeBindingCrossesOriginalTeardownGateV6|TestNativeNoDebtClosePreservesOrdinaryHookErrorV6|TestNativeMVCCSnapshotActualCaptureAndCancellation|TestNativeStablePagerPartialTokenCancellationV6|TestNativePagerOperationsPartialConstructionAndCancellationV6|TestNativeRootPreparationSourceWitnessV6|TestNativeRootPreparationSourceTerminalV6|TestNativePublishedRootPreparationDrainV6|TestNativePublishedOutputAdmissionBeforeAdoptionV6|TestNativeSnapshotPublicationUsesSameLeafOperandV6|TestPrimaryJointVacuumV6SuccessfulReplacement|TestPrimaryJointVacuumV6PointersAndIndependentFallback)$
```

**race-TreeDB-internal-rootpublication** — `./TreeDB/internal/rootpublication`, Go timeout `3m`, outer process deadline `300s`.

```text
^(TestGenericResourceDiagnosticsOwnAllNamespaceText|TestGenericResourceDiagnosticsSurviveConcurrentPhysicalRelease|TestRawLogicalUnionRefusesSelectedAliasesBeforeEscape|TestGenericEmptyCloneUsesAbsentClosure|TestEmptyDirectoryCloneRetainsIndependentCustody|TestStableNamespaceParentGenerationTracksExactParentIdentity|TestStableNamespaceCreationProofRejectsReplacementBeforeBind)$
```

**race-TreeDB-pager** — `./TreeDB/pager`, Go timeout `3m`, outer process deadline `300s`.

```text
^(TestNativeWordInvalidationKeepsOrdinaryMaskedPolicyV6|TestNativeRetainedFileFencePreservesDirtyAndRetiredIdentityV6)$
```

**race-TreeDB-internal-valuelog** — `./TreeDB/internal/valuelog`, Go timeout `3m`, outer process deadline `300s`.

```text
^(TestNativeMVCCOriginalFilePhysicalClose|TestNativeOriginalMappingFreshQuantumTooSmall)$
```

**race-TreeDB-caching** — `./TreeDB/caching`, Go timeout `3m`, outer process deadline `300s`.

```text
^(TestCOWMaintenancePostRefreshSettlementKeepsFenceAndOldCut|TestCOWDBCloseWaitsForAdmittedPointerReadAndDelayedOwnersClose)$
```

**race-TreeDB** — `./TreeDB`, Go timeout `5m`, outer process deadline `420s`.

```text
^(TestPrimaryJointVacuumV6DefaultOuterLeafMaterializationReaderAndCut)$
```

## Runtime boundary and missing distinguishing control

Ordinary SetSync/Batch.WriteSync, Refresh, CompactIndex, actual ordinary Activate, ordinary joint vacuum and ordinary recovery are intentionally present. They exercise the original production publisher; they must not be misdescribed as a no-publication suite. Their fixed ordinary work does not authorize the native current-delete final driver.

The native primitive exceptions must be stated in any runtime grant:

- Native Close controls bind/join/cancel a synthetic **pre-enqueue** receipt, call `CompleteNativePublicationV6(receipt,nil,false)` and `Stop`, and assert the Publisher is never called. They do not enqueue, adopt or publish.
- Header admission invokes `activateWithWorkV6` only on three **pre-adoption refusals**: the supplied allocator callback must remain at zero calls and published state unchanged. Snapshot publication scope uses `finish(false)` with no header/root output.
- Standalone pager tests really call `WriteNativeWithWorkV6` and `SyncNativeIndexDataWithStableFileWithWorkV6`; this performs native physical write/fdatasync on a tiny private file. It is not the selected DB capsule/authority fence, but it is still an I/O effect, so a blanket “no native fences” description would be wrong.
- Owner/file primitives really execute finite physical cleanup. They do not complete a native publication receipt.
- The three accepted witness controls are preserved at their original boundary. `TestNativePublishedRootPreparationDrainV6` calls the published-close wrapper solely to obtain the unfinished-receipt refusal, then sets an explicitly synthetic phase9 cursor. A successful real `ContinueFinish`/publication-to-source-disposal transition is not constructed or tested.

**Known coverage gap:** the accepted ordinary cache-refusal/terminal tests reach actual ordinary SetSync/Close, but cannot distinguish the native-factored Activate callback. The implementation worker owns the narrowly factored new regression control. I have not invented its selector or reviewed mutable implementation; add it only after its exact source and pre-adoption/runtime boundary are independently inspected. The same requirement applies to any changed existing test, pricing assertion or helper in the future composition.

Excluded from this proposal: actual33/promoted observation, native current-delete producer/iterator/prune/public adapters, native final driver or enqueue/adoption/successful activation, PRIMARY seal/capsule fence, Consume, real publication Finish/last-root completion, full M4/fault/crash matrix, all benchmarks/profiles/performance collection, full CI, public C1/C3, Q32/1MiB or native qualification. Old-base witness F291/R145 text is historical, not composed pricing acceptance.

## Failure, platform and evidence handling

- Revalidate all input/selected test bytes against the actual integrated freeze; retained prior PASS does not become integrated-head PASS. Source bodies/helper chains and the package goleak TestMain wrappers were inspected. No wrapper executes an unselected native driver; existing DB test-producer registration and background-quiet init hooks remain test infrastructure, not publication permission.
- Require each named top-level RUN/PASS and intended children. In particular require all six stable-token cases, all seven owned-operation states, all three witness top-level tests (seven RUN nodes total), and **exactly one** M4 leaf `TestIndexVacuumM4CertificationMatrix/pointer-values/backend-vacuum-online`. A top-level PASS with zero matching children is insufficient.
- On Linux the selected physical/namespace paths must actually run; record unexpected capability skips as unavailable coverage. Unix namespace tests are build constrained; Linux pager tests do not exist on Windows. This proposal gives no Windows runtime coverage. Preserve supported-fault versus typed-unsupported no-effect oracles.
- Keep the landed portable file-close `File.Close`/`os.ErrClosed` terminal assertion. The original native archive has the older `Stat` oracle; source integration must retain the ordinary test superset, as the accepted plan requires.
- Capture actual argv/compiler/source hashes, raw normal/race logs, exact test inventory, exit status and PID/PGID lifecycle. On timeout preserve stacks/logs, kill and actually join the process group using root’s existing driver. Do not rerun-to-pass, relax assertions or raise fixture deadlines. Goleak and race failures are failures.
- Normal allocation assertions are implementation checks only; there is no public performance, M0, total retained allocation, complete Finish, native completion or merge-readiness claim.

No Go command, test body, benchmark, profile, network request or Git operation was executed for this inventory. Only immutable source/packet reads, manifest/hash verification and these two advice files were produced. The test proposal and reviewer lease are complete; root owns selection and any later runtime grant.
