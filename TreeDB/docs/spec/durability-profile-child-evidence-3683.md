# Durability Profile Child Evidence (#3683 -> #3684)

This manifest records the reusable correctness evidence that successor issue
[#3684](https://github.com/snissn/gomap/issues/3684) may consume. It does not
replace latest-head CI or the successor's own end-to-end review.

## Frozen profile contract

| Contract | Executable evidence |
| --- | --- |
| exact canonical parser/default/alias mapping and immutable open resolution | `TreeDB/profiles_test.go` |
| all exported public entry points route through the resolved profile | `TestDurabilityProfilePublicEntrypointInventory` |
| ordinary ACK, empty explicit sync, point sync, checkpoint, clean close, and reopen frontiers | `TestProductionProfileLifecycleFrontiersMatchFrozenContract` |
| forced value-log pointers, delete/reuse, command-WAL rotation, checkpoint, and reopen in every production profile | `TestProductionProfilesForcedPointersDeleteReuseRotationReopen` |
| process-crash reopen for relaxed WAL, durable WAL, and production no-WAL sync boundaries | `TestCrashRecovery_DurabilityTiers` |
| benchmark-only selection cannot enter the production parser/constructor path | `TestParsePublicProfile_RejectsDeprecatedAndUnknownNames`, `TestOpen_RejectsBenchUnsafeWithoutExplicitBoundary` |
| profile-less reopen and maintenance adopt the persisted production contract, while explicit mismatches still fail and persisted `bench_unsafe` never becomes a production default | `TestOpen_PersistedProductionProfileImplicitlyReopensSameContract`, `TestOpen_PersistedProfileCannotChangeContracts`, `TestOpen_BenchUnsafeManifestCannotBecomeProductionDefault`, `TestFormatConfig_PersistsAndGatesCanonicalDurabilityProfile`, `TestLoadPersistedDurabilityProfile_ReadsOnlyContractGate` |
| deprecated Go aliases map exactly and expose a diagnostic | `TestApplyProfile_DeprecatedAliasesResolveToFrozenMappings` |

## Owned MVCC submission surface

The exhaustive DB inventory also includes the owned submission and physical-work
interfaces below. Their routing guard is
`TestDurabilityProfileOrderedSubmissionEntrypointInventory`; it retains exact
method inventories for the new builder/completion/estimator types and the shared
producer/ACK path. These methods introduce no separate durability profile.

| Entry point | Profile classification and boundary |
| --- | --- |
| `DB.SupportsLogicalMVCCStaging`, `Store.SupportsCommitGroupStaging` | Read-only capability check under lifecycle protection; cached logical command-WAL only, with phase instrumentation excluded. Unsupported routes keep the synchronous writer. |
| `DB.BeginLogicalMVCC`, `Store.BeginCommitGroup` | Acquire existing lifecycle/floor custody and an unassigned native batch; no command, root preparation or ACK. |
| `LogicalMVCCBuilder.Append`, `CommitBuilder.AppendGroup` | Validate and copy native operands before seal; no accepted prefix or durability boundary. |
| `LogicalMVCCBuilder.CollectionDeadline`, `CommitBuilder.CollectionDeadline` | Monotonic scheduling metadata established at Begin from the existing 200us collection budget; inactive builders return zero. No command or persistence authority. Sealed Submit ends collection without a force ticket or second engine wait. |
| `DB.SubmitLogicalMVCC`, `Store.SubmitCommitGroupAt`, builder `Submit` | Use the original preparation, append/registration and complete-cut installation. Return transfers custody to the caller-owned completion; it is not a durable ACK. Unsupported direct-submission routes retain synchronous commit. |
| `LogicalMVCCCompletion.Complete`, `CommitCompletion.Complete` | Drive the existing captured-prefix and ordered ACK path, then release original batch/lifecycle/floor custody exactly once. |
| builder `Abort` | Release only unassigned staging; no rollback or retry of an assigned command. |
| `DB.NewMVCCWriteWorkEstimator`, `DB.EstimateMVCCWriteWork`, `DB.MVCCWriteWorkLimit`, estimator `Estimate` | Non-mutating shaping metadata. DB access retains lifecycle protection; the captured estimator contains immutable scalars and remains usable after Close. Zero limit is unsupported; supported without headroom returns1. No reservation, publication authority or sync boundary. |

Runtime controls include `TestOrderedCollectionSealedHandoffSkipsSecondWindow`,
`TestPublicCommandWALCollectionBudgetAgesFromRegistration`,
`TestOrderedCollectionDeadlineIsOneMonotonicBoundary`,
`TestOrderedBuilderIncrementalNativeWorkBeforeSeal`,
`TestOrderedBuilderTransfersQueuedFloorWriter`,
`TestOrderedBuilderTransfersQueuedClose`,
`TestOrderedBuilderExactRouteEligibility`,
`TestOrderedSubmissionSuccessorWhileCapturedSyncHeld`,
`TestOrderedSubmissionSyncFailureReleasesCustody`,
`TestOrderedPhysicalWorkBoundsAndOversizedAtomicRefusal`,
`TestOrderedWorkEstimateConcurrentClose` and
`TestOrderedImmutableWorkEstimatorOutlivesClose`. The test-only
`TreeDB/testutil/durabilitycut` bridge aliases the original serialized observer;
its staging event witnesses an owned copy, not physical persistence.

## Publication and asset closure inherited from merged children

The profile layer does not duplicate each physical producer. It routes every
supported public command/root through the stable-resource authority delivered by
the prerequisite publication and cleanup work.

| Asset or invariant | Executable evidence |
| --- | --- |
| index, value-log pointers, raw/packed outer leaves, leaf generations | `TestProductionAuthorityRealProducerCaptureMatrix`, `TestProductionAuthorityFullClosureAndAllButOneRetryMatrix` |
| dictionary/template nested closures | `TestProductionAuthorityExecutableCompositeOmissionMatrix` |
| column manifests, typed multipart/value/code assets, HNSW/search packs | `TestProductionAuthorityExecutableCompositeOmissionMatrix` |
| vector graph transitive closure | `TestProductionAuthorityExecutableCompositeOmissionMatrix` |
| active/rotated command-WAL segments and external RID fences | `TestProductionAuthorityRealProducerCaptureMatrix`, `TestPublicCommandWALRelaxedRotationsStabilizeExactPrefixBeforeBarrierAppend` |
| exact resource identity across lifecycle/rebind conflicts | `TestProductionAuthorityLifecycleIdentityConflictMatrix`, `TestProductionAuthorityNamespaceAsymmetryVariants` |
| collection/catalog command-intent profile propagation | `TestCommandWALIntentResolvedProfileControlsOrdinaryStagedAppendSync` |
| durable-prefix dependency debt and retry | `TestPublicCommandWALRelaxedPointerDebtStatsCloseOnSetSync`, `TestPublicCommandWALRelaxedRotationSyncCutsRetainDebtForBarrierRetry` |

## Admission, debt, and observability

| Gate | Executable evidence |
| --- | --- |
| safe profile-aware hard admission | `TestNormalizeFlushAdmission_AutoAdmitsResolvedNoWALFastProfile`, `TestFlushAdmissionStatsExposePolicyReasonHardwareAndCandidate` |
| overload surfaces bounded foreground assistance instead of deferred close-only failure | `TestFlushCoordinatorHardOverloadFallsBackToForegroundAssist`, `TestFlushCoordinatorStopBackpressureHardOverloadSkipsProgressWait` |
| coalescing preserves lane/range/frontier ownership | `TestFlushBacklogCoalescingPreservesLaneAndRangeBarriers`, `TestFlushBacklogCoalescingCheckpointUsesFrontierOwnedAdmission` |
| profile, ACK class, frontiers, debt, syncs, admission, and fallback diagnostics | `TreeDB/expvar_public_test.go`, `TreeDB/db/command_wal_stats_test.go`, `TreeDB/db/flush_admission_policy_test.go` |

## Focused reproduction commands

```sh
go test ./TreeDB -run 'Test(DurabilityProfilePublicEntrypointInventory|ProductionProfileLifecycleFrontiersMatchFrozenContract|ProductionProfilesForcedPointersDeleteReuseRotationReopen|CrashRecovery_DurabilityTiers)$' -count=1

go test ./TreeDB -run '^TestProductionAuthority(RealProducerCaptureMatrix|FullClosureAndAllButOneRetryMatrix|ExecutableCompositeOmissionMatrix|LifecycleIdentityConflictMatrix|NamespaceAsymmetryVariants)$' -count=1

go test ./TreeDB/db -run 'Test(CommandWALIntentResolvedProfileControlsOrdinaryStagedAppendSync|CommandWALStatsExposeDurablePrefixAndPendingDebt|NormalizeFlushAdmission_AutoAdmitsResolvedNoWALFastProfile|FlushAdmissionStatsExposePolicyReasonHardwareAndCandidate)$' -count=1

go test ./TreeDB/caching -run 'Test(FlushCoordinatorHardOverloadFallsBackToForegroundAssist|FlushCoordinatorStopBackpressureHardOverloadSkipsProgressWait|FlushBacklogCoalescingPreservesLaneAndRangeBarriers|FlushBacklogCoalescingCheckpointUsesFrontierOwnedAdmission)$' -count=1
```

## Performance evidence boundary

Matched-contract performance is owned by
`benchmarks/dgraph_durability`. Archive the exact commit, dirty state, host,
filesystem, commands, raw repeats, stable-I/O contract, acknowledgement
percentiles, publications/group size, dependency debt bytes/age, caller waits,
WAL/root/file/directory sync counts, and allocation metrics. Unsafe
`bench_unsafe` results are ceilings only and are not regression denominators.
