## Go Test Timing Summary

| elapsed_s | result | package | test |
| ---: | --- | --- | --- |
| 51.94 | pass | `github.com/snissn/gomap/TreeDB` | `TestReopenVerify_WALOn_Checkpoint_CompressionModes` |
| 48.80 | pass | `github.com/snissn/gomap/TreeDB/internal/raftplacement` | `TestCatalogMetaBackupRestoresFreshThreeAuthorityClusterAndSurvivesReopenFailoverRejoin` |
| 33.69 | pass | `github.com/snissn/gomap/TreeDB` | `TestCOWPublicEncodedMVCCPointsGroupsPhysicalDeleteReplay` |
| 31.50 | pass | `github.com/snissn/gomap/TreeDB` | `TestCrashRecovery_DurabilityTiers` |
| 23.22 | pass | `github.com/snissn/gomap/TreeDB/internal/raftplacement` | `TestCatalogMetaLifecycleImmutableIdentitySurvivesReopenV1` |
| 20.21 | pass | `github.com/snissn/gomap/TreeDB` | `TestReopenVerify_InternalBaseDelta_WALOn_Checkpoint` |
| 19.20 | pass | `github.com/snissn/gomap/TreeDB` | `TestWriteReadVisibilityLargeBatchPaths` |
| 18.32 | pass | `github.com/snissn/gomap/TreeDB` | `TestPublicCommandWALWriteThenDirtyWriteSyncDurabilityLedger` |
| 17.59 | fail | `github.com/snissn/gomap/TreeDB` | `TestCOWPublicEmptyCheckpointLeafRegistrationProgress` |
| 16.09 | pass | `github.com/snissn/gomap/TreeDB` | `TestCOWPublicEncodedMVCCPointsGroupsPhysicalDeleteReplay/command_wal_relaxed` |
| 16.02 | pass | `github.com/snissn/gomap/TreeDB` | `TestCOWPublicContractCheckpointCloseReopen` |
| 15.59 | pass | `github.com/snissn/gomap/TreeDB` | `TestCOWCloseErrorNotificationCanReadClosedDatabase` |
| 15.55 | pass | `github.com/snissn/gomap/TreeDB` | `TestCOWPublicEncodedMVCCPointsGroupsPhysicalDeleteReplay/command_wal_durable` |
| 15.51 | pass | `github.com/snissn/gomap/TreeDB` | `TestCrashRecovery_DurabilityTiers/wal_on_strict_sync_large_value` |
| 15.49 | pass | `github.com/snissn/gomap/TreeDB` | `TestCrashRecovery_DurabilityTiers/wal_on_relaxed_sync_large_value` |
| 15.48 | pass | `github.com/snissn/gomap/TreeDB` | `TestOpen_PersistedProductionProfileImplicitlyReopensSameContract` |
| 15.35 | pass | `github.com/snissn/gomap/TreeDB` | `TestDeferredVectorBuildMaintenanceCloseAndReopen` |
| 15.33 | pass | `github.com/snissn/gomap/TreeDB` | `TestOpen_PersistedProductionProfileImplicitlyReopensSameContract/command_wal_relaxed` |
| 11.03 | pass | `github.com/snissn/gomap/TreeDB` | `TestCOWPublicEmptyCheckpointLeafRegistrationProgress/command_wal_durable` |
| 11.02 | pass | `github.com/snissn/gomap/TreeDB` | `TestVacuumIndexOffline_LeafPagesInValueLog_ReopenParity` |
| 10.95 | pass | `github.com/snissn/gomap/TreeDB/internal/raftplacement` | `TestCatalogMetaLifecycleHarnessActivatesAndConvergesV1` |
| 10.55 | pass | `github.com/snissn/gomap/TreeDB` | `TestPublicCommandWALWriteThenDirtyWriteSyncDurabilityLedger/forced_pointer` |
| 9.70 | pass | `github.com/snissn/gomap/TreeDB/internal/rootpublication` | `TestExactHardCommitBoundaryAcknowledgesThenNextCommitWaits` |
| 9.61 | pass | `github.com/snissn/gomap/TreeDB` | `TestReopenVerify_DurableWALForcedValueLogPointers` |
| 9.53 | pass | `github.com/snissn/gomap/TreeDB` | `TestPublicCommandWALEmptyCheckpointReclaimsCoveredBenchmarkEpochs` |

## Package Results

| elapsed_s | result | package |
| ---: | --- | --- |
| 381.30 | fail | `github.com/snissn/gomap/TreeDB` |
| 88.74 | pass | `github.com/snissn/gomap/TreeDB/internal/raftplacement` |
| 15.60 | pass | `github.com/snissn/gomap/TreeDB/internal/rootpublication` |
| 1.23 | pass | `github.com/snissn/gomap/TreeDB/internal/commitlog` |
| 1.06 | pass | `github.com/snissn/gomap/TreeDB/internal/valuelog` |
| 0.68 | pass | `github.com/snissn/gomap/TreeDB/internal/powerlossoracle` |
| 0.19 | pass | `github.com/snissn/gomap/TreeDB/tree` |
| 0.08 | pass | `github.com/snissn/gomap/TreeDB/internal/authorityinventory` |
| 0.04 | pass | `github.com/snissn/gomap/TreeDB/mongo_gateway/compatdiff` |
| 0.03 | pass | `github.com/snissn/gomap/TreeDB/internal/sourcepartition` |
| 0.00 | skip | `github.com/snissn/gomap/TreeDB/cmd/db_histogram` |
| 0.00 | skip | `github.com/snissn/gomap/TreeDB/cmd/wal_classify` |
| 0.00 | skip | `github.com/snissn/gomap/TreeDB/internal/limits` |

## Tests Still Running At Log End

- none
