## Go Test Timing Summary

| elapsed_s | result | package | test |
| ---: | --- | --- | --- |
| 40.83 | pass | `github.com/snissn/gomap/TreeDB` | `TestProductionAuthorityExecutableCompositeOmissionMatrix` |
| 36.94 | pass | `github.com/snissn/gomap/TreeDB` | `TestProductionAuthorityExecutableCompositeOmissionMatrix/typed-column-physical-assets` |
| 25.36 | pass | `github.com/snissn/gomap/TreeDB/internal/raftcluster` | `TestHashicorpRaftProviderLeaderCrashRestartCatchUpSurvivesReplicationBackoff` |
| 19.93 | pass | `github.com/snissn/gomap/TreeDB/cmd/treedb_rag_benchmark` | `TestServiceOverheadParityRows` |
| 16.48 | pass | `github.com/snissn/gomap/TreeDB` | `TestCompactStorageExhaustiveCommandWALRandom4KOffline` |
| 13.95 | pass | `github.com/snissn/gomap/TreeDB/collections` | `TestCollectionCommandWALCreateCollectionPinsIndexNamespaceThroughPublish` |
| 13.58 | pass | `github.com/snissn/gomap/TreeDB` | `TestCompactStorageExhaustiveCommandWALRandom4KOffline/keys100000` |
| 10.48 | pass | `github.com/snissn/gomap/TreeDB/internal/raftcluster` | `TestHashicorpRaftProviderLeaderCrashAfterCommitRestartCatchUpConverges` |
| 8.25 | pass | `github.com/snissn/gomap/TreeDB/collections` | `TestTypedStorageLegacyNameAllowlistIsComplete` |
| 7.11 | pass | `github.com/snissn/gomap/TreeDB/internal/raftcluster` | `TestCatalogMetaRaftProviderFixedPeersFailoverSnapshotReopenAndRejoin` |
| 7.02 | pass | `github.com/snissn/gomap/TreeDB/internal/raftcluster` | `TestHashicorpRaftProviderReadIndexRejectsWithoutBarrierWhenAppliedProgressUnavailable` |
| 6.05 | pass | `github.com/snissn/gomap/TreeDB/internal/raftcluster` | `TestHashicorpRaftProviderLeaderCrashBeforeCommitDoesNotReportSuccessOrMutate` |
| 6.01 | pass | `github.com/snissn/gomap/TreeDB/internal/raftcluster` | `TestHashicorpRaftProviderReadIndexRejectsWithoutBarrierWhenAppliedProgressUnavailable/reader-missing` |
| 4.75 | pass | `github.com/snissn/gomap/TreeDB/internal/rootpublication` | `TestExactHardCommitBoundaryAcknowledgesThenNextCommitWaits` |
| 4.46 | pass | `github.com/snissn/gomap/TreeDB/collections` | `TestVectorPartitionSourceImportDependencyEncodingContinuousV2` |
| 4.36 | pass | `github.com/snissn/gomap/TreeDB/collections` | `TestVectorPartitionSourceImportDependencyEncodingCoalescedV2` |
| 4.26 | pass | `github.com/snissn/gomap/TreeDB` | `TestCOWPublicRetentionRefusalDrainAndResume` |
| 4.17 | pass | `github.com/snissn/gomap/TreeDB/internal/raftcluster` | `TestThreeNodeHarnessPreferredLeadersRotateAcrossGroups` |
| 4.08 | pass | `github.com/snissn/gomap/TreeDB/collections` | `TestVectorPartitionSourceImportDependencyEncodingReopenV2` |
| 3.88 | pass | `github.com/snissn/gomap/TreeDB` | `TestProfileFast_SetSyncRemainsVisibleAndPersistsAfterCheckpoint` |
| 3.83 | pass | `github.com/snissn/gomap/TreeDB/collections` | `TestVectorPartitionOwnerSearchOpenPlanAllocationGrowthV2` |
| 3.55 | pass | `github.com/snissn/gomap/TreeDB/internal/raftcluster` | `TestCatalogMetaRaftProviderDistinguishesPreEnqueueCancellationFromAmbiguousApply` |
| 3.31 | pass | `github.com/snissn/gomap/TreeDB` | `TestCOWPublicModeledInterruptedDependencyAndRootPublicationCuts` |
| 3.10 | pass | `github.com/snissn/gomap/TreeDB/internal/raftcluster` | `TestThreeNodeHarnessReadCoordinatorRestoresPreferredLeaderV1` |
| 3.10 | pass | `github.com/snissn/gomap/TreeDB/collections` | `TestTypedGraphPreparedFilterDispersedQuality` |

## Package Results

| elapsed_s | result | package |
| ---: | --- | --- |
| 140.29 | pass | `github.com/snissn/gomap/TreeDB` |
| 119.19 | pass | `github.com/snissn/gomap/TreeDB/collections` |
| 86.59 | pass | `github.com/snissn/gomap/TreeDB/internal/raftcluster` |
| 46.10 | pass | `github.com/snissn/gomap/TreeDB/cmd/treedb_rag_benchmark` |
| 6.84 | pass | `github.com/snissn/gomap/TreeDB/internal/rootpublication` |
| 5.20 | pass | `github.com/snissn/gomap/TreeDB/internal/raftfsm` |
| 1.49 | pass | `github.com/snissn/gomap/TreeDB/docs` |
| 1.03 | pass | `github.com/snissn/gomap/TreeDB/cmd/treedb_vector_partition_m5_bench` |
| 0.44 | pass | `github.com/snissn/gomap/TreeDB/internal/commandwalapply` |
| 0.28 | pass | `github.com/snissn/gomap/TreeDB/zipper` |
| 0.12 | pass | `github.com/snissn/gomap/TreeDB/integration/kvstoreadapter` |
| 0.07 | pass | `github.com/snissn/gomap/TreeDB/internal/vectorops` |
| 0.06 | pass | `github.com/snissn/gomap/TreeDB/internal/powerlossoracle` |
| 0.05 | pass | `github.com/snissn/gomap/TreeDB/internal/compression` |
| 0.04 | pass | `github.com/snissn/gomap/TreeDB/node` |
| 0.02 | pass | `github.com/snissn/gomap/TreeDB/internal/authorityinventory` |
| 0.02 | pass | `github.com/snissn/gomap/TreeDB/mongo_gateway/wire` |
| 0.02 | pass | `github.com/snissn/gomap/TreeDB/internal/mappedresource` |
| 0.01 | pass | `github.com/snissn/gomap/TreeDB/internal/typeddecode` |
| 0.01 | pass | `github.com/snissn/gomap/TreeDB/template` |
| 0.00 | pass | `github.com/snissn/gomap/TreeDB/internal/leafrefscan` |
| 0.00 | pass | `github.com/snissn/gomap/TreeDB/internal/mvcckey` |
| 0.00 | pass | `github.com/snissn/gomap/TreeDB/internal/rabitq` |
| 0.00 | pass | `github.com/snissn/gomap/TreeDB/mongo_gateway/benchsupport` |
| 0.00 | pass | `github.com/snissn/gomap/TreeDB/cmd/template_lab` |
| 0.00 | pass | `github.com/snissn/gomap/TreeDB/internal/collectionwal` |
| 0.00 | pass | `github.com/snissn/gomap/TreeDB/internal/sourcepartition` |
| 0.00 | pass | `github.com/snissn/gomap/TreeDB/internal/workstats` |
| 0.00 | pass | `github.com/snissn/gomap/TreeDB/internal/strictjson` |
| 0.00 | skip | `github.com/snissn/gomap/TreeDB/cmd/authority_inventory` |
| 0.00 | skip | `github.com/snissn/gomap/TreeDB/cmd/power_loss_certify` |
| 0.00 | skip | `github.com/snissn/gomap/TreeDB/cmd/verify` |
| 0.00 | skip | `github.com/snissn/gomap/TreeDB/cmd/wal_classify` |
| 0.00 | skip | `github.com/snissn/gomap/TreeDB/internal/durabilitycut` |

## Tests Still Running At Log End

- none
