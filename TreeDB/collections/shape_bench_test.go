package collections_test

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	defaultCollectionMixedSeedDocs       = 4096
	defaultCollectionMixedWriteBatchSize = 128
)

type collectionMixedScaleCase struct {
	readers int
	writers int
}

func addCollectionInsertStats(dst *collections.CollectionInsertStats, src collections.CollectionInsertStats) {
	dst.Documents += src.Documents
	dst.Indexes += src.Indexes
	dst.Runs += src.Runs
	dst.BufferedIndexedBatches += src.BufferedIndexedBatches
	dst.BufferedIndexedBypassBatches += src.BufferedIndexedBypassBatches
	dst.ValidationPreflightReused += src.ValidationPreflightReused
	dst.ValidationPreflightRechecked += src.ValidationPreflightRechecked
	dst.PrepareDocuments += src.PrepareDocuments
	dst.IndexStateExtraction += src.IndexStateExtraction
	dst.DuplicateDocumentPreflight += src.DuplicateDocumentPreflight
	dst.RetainedPayloadPrepare += src.RetainedPayloadPrepare
	dst.RetainedPayloadRows += src.RetainedPayloadRows
	dst.RetainedPayloadDeclaredRows += src.RetainedPayloadDeclaredRows
	dst.RetainedPayloadSemanticStreamBlocks += src.RetainedPayloadSemanticStreamBlocks
	if src.RetainedPayloadSemanticStreamWorkerCount > dst.RetainedPayloadSemanticStreamWorkerCount {
		dst.RetainedPayloadSemanticStreamWorkerCount = src.RetainedPayloadSemanticStreamWorkerCount
	}
	dst.RetainedPayloadSemanticStreamDeclaredRowPrepare += src.RetainedPayloadSemanticStreamDeclaredRowPrepare
	dst.RetainedPayloadSemanticStreamBlockPrepareWall += src.RetainedPayloadSemanticStreamBlockPrepareWall
	dst.RetainedPayloadSemanticStreamBlockCollect += src.RetainedPayloadSemanticStreamBlockCollect
	dst.RetainedPayloadSemanticStreamBlockEncoderSetup += src.RetainedPayloadSemanticStreamBlockEncoderSetup
	dst.RetainedPayloadSemanticStreamBlockRawEncode += src.RetainedPayloadSemanticStreamBlockRawEncode
	dst.RetainedPayloadSemanticStreamBlockStoredEncode += src.RetainedPayloadSemanticStreamBlockStoredEncode
	dst.RetainedPayloadSemanticStreamBlockFinalize += src.RetainedPayloadSemanticStreamBlockFinalize
	dst.RetainedPayloadSemanticStreamTableBuild += src.RetainedPayloadSemanticStreamTableBuild
	dst.RetainedPayloadValueLogPointerize += src.RetainedPayloadValueLogPointerize
	dst.RetainedPayloadValueLogValues += src.RetainedPayloadValueLogValues
	dst.RetainedPayloadValueLogBytes += src.RetainedPayloadValueLogBytes
	dst.RetainedStreamValueLogPointerize += src.RetainedStreamValueLogPointerize
	dst.RetainedStreamValueLogValues += src.RetainedStreamValueLogValues
	dst.RetainedStreamValueLogBytes += src.RetainedStreamValueLogBytes
	dst.ColumnPublishBuildColumnDelta += src.ColumnPublishBuildColumnDelta
	dst.ColumnPublishBuildSystemDelta += src.ColumnPublishBuildSystemDelta
	dst.ColumnPublishCommit += src.ColumnPublishCommit
	dst.ColumnPublishWriteLockWait += src.ColumnPublishWriteLockWait
	dst.ColumnPublishPreflight += src.ColumnPublishPreflight
	dst.ColumnPublishCommandWALAppend += src.ColumnPublishCommandWALAppend
	dst.ColumnPublishOrderedRootApply += src.ColumnPublishOrderedRootApply
	dst.ColumnPublishSystemRootApply += src.ColumnPublishSystemRootApply
	dst.ColumnPublishFinalize += src.ColumnPublishFinalize
	dst.ColumnPublishFinalizePrepareDurability += src.ColumnPublishFinalizePrepareDurability
	dst.ColumnPublishFinalizeCandidateBuild += src.ColumnPublishFinalizeCandidateBuild
	dst.ColumnPublishFinalizeCandidateVisibleBaseClone += src.ColumnPublishFinalizeCandidateVisibleBaseClone
	dst.ColumnPublishFinalizeCandidateInheritedFilter += src.ColumnPublishFinalizeCandidateInheritedFilter
	dst.ColumnPublishFinalizeCandidateFreshCapture += src.ColumnPublishFinalizeCandidateFreshCapture
	dst.ColumnPublishFinalizeCandidateClosureAssemble += src.ColumnPublishFinalizeCandidateClosureAssemble
	dst.ColumnPublishFinalizeCandidateVisibleClone += src.ColumnPublishFinalizeCandidateVisibleClone
	dst.ColumnPublishFinalizeCandidateCOWPrepare += src.ColumnPublishFinalizeCandidateCOWPrepare
	dst.ColumnPublishFinalizeCandidateOther += src.ColumnPublishFinalizeCandidateOther
	dst.ColumnPublishFinalizeCandidateResourceWork.Add(src.ColumnPublishFinalizeCandidateResourceWork)
	dst.ColumnPublishFinalizeEnqueueActivation += src.ColumnPublishFinalizeEnqueueActivation
	dst.ColumnPublishFinalizeAdmissionWait += src.ColumnPublishFinalizeAdmissionWait
	dst.ColumnPublishFinalizeDurabilityWait += src.ColumnPublishFinalizeDurabilityWait
	dst.ColumnPublishPostFinalize += src.ColumnPublishPostFinalize
	dst.ColumnPublishDocumentExtraction += src.ColumnPublishDocumentExtraction
	dst.ColumnPublishDeclaredColumnEncoding += src.ColumnPublishDeclaredColumnEncoding
	dst.ColumnPublishAssetPreparation += src.ColumnPublishAssetPreparation
	dst.ColumnPublishRowAssetPreparation += src.ColumnPublishRowAssetPreparation
	dst.ColumnPublishTypedColumnPreparation += src.ColumnPublishTypedColumnPreparation
	dst.ColumnPublishTypedColumnDictionaryBuild += src.ColumnPublishTypedColumnDictionaryBuild
	dst.ColumnPublishTypedColumnRowMaterialization += src.ColumnPublishTypedColumnRowMaterialization
	dst.ColumnPublishTypedColumnPartBuild += src.ColumnPublishTypedColumnPartBuild
	dst.ColumnPublishTypedColumnImageBuild += src.ColumnPublishTypedColumnImageBuild
	dst.ColumnPublishDictionaryPreparation += src.ColumnPublishDictionaryPreparation
	dst.ColumnPublishInt64Preparation += src.ColumnPublishInt64Preparation
	dst.ColumnPublishAggregateMetadataPrepare += src.ColumnPublishAggregateMetadataPrepare
	dst.ColumnPublishRowSidecarSharedBuild += src.ColumnPublishRowSidecarSharedBuild
	dst.ColumnPublishAssetAppend += src.ColumnPublishAssetAppend
	dst.ColumnPublishAssetAppendOpen += src.ColumnPublishAssetAppendOpen
	dst.ColumnPublishAssetAppendWrite += src.ColumnPublishAssetAppendWrite
	dst.ColumnPublishAssetAppendClose += src.ColumnPublishAssetAppendClose
	dst.ColumnPublishAssetAppendFileSync += src.ColumnPublishAssetAppendFileSync
	dst.ColumnPublishAssetAppendFileClose += src.ColumnPublishAssetAppendFileClose
	dst.ColumnPublishAssetAppendDirSync += src.ColumnPublishAssetAppendDirSync
	dst.ColumnPublishAssetAppendCleanup += src.ColumnPublishAssetAppendCleanup
	dst.ColumnPublishAssetAppenderCloseCount += src.ColumnPublishAssetAppenderCloseCount
	dst.ColumnPublishAssetAppendFileSyncCount += src.ColumnPublishAssetAppendFileSyncCount
	dst.ColumnPublishAssetSyncEpochCount += src.ColumnPublishAssetSyncEpochCount
	dst.ColumnPublishSharedSegmentAppenderCloseCount += src.ColumnPublishSharedSegmentAppenderCloseCount
	dst.ColumnPublishSharedSegmentAppendFileSyncCount += src.ColumnPublishSharedSegmentAppendFileSyncCount
	dst.ColumnPublishSharedSegmentAppendSyncEpochCount += src.ColumnPublishSharedSegmentAppendSyncEpochCount
	dst.ColumnPublishDirectViewSegmentAppenderCloseCount += src.ColumnPublishDirectViewSegmentAppenderCloseCount
	dst.ColumnPublishDirectViewSegmentAppendFileSyncCount += src.ColumnPublishDirectViewSegmentAppendFileSyncCount
	dst.ColumnPublishDirectViewSegmentAppendSyncEpochCount += src.ColumnPublishDirectViewSegmentAppendSyncEpochCount
	dst.ColumnPublishManifestEncode += src.ColumnPublishManifestEncode
	dst.ColumnPublishAssetClosureValidation += src.ColumnPublishAssetClosureValidation
	dst.ColumnPublishRootDeltaConstruction += src.ColumnPublishRootDeltaConstruction
	dst.ColumnPublishSystemDeltaConstruction += src.ColumnPublishSystemDeltaConstruction
	dst.ColumnPublishRootDeltaMaterialization += src.ColumnPublishRootDeltaMaterialization
	dst.ColumnPublishRows += src.ColumnPublishRows
	dst.ColumnPublishPreparedAssets += src.ColumnPublishPreparedAssets
	dst.ColumnPublishRowAssetBytes += src.ColumnPublishRowAssetBytes
	dst.ColumnPublishRowAssetCount += src.ColumnPublishRowAssetCount
	dst.ColumnPublishTypedColumnBytes += src.ColumnPublishTypedColumnBytes
	dst.ColumnPublishTypedColumnCount += src.ColumnPublishTypedColumnCount
	dst.ColumnPublishDictionaryBytes += src.ColumnPublishDictionaryBytes
	dst.ColumnPublishDictionaryCount += src.ColumnPublishDictionaryCount
	dst.ColumnPublishInt64Bytes += src.ColumnPublishInt64Bytes
	dst.ColumnPublishInt64Count += src.ColumnPublishInt64Count
	dst.ColumnPublishAggregateMetadataBytes += src.ColumnPublishAggregateMetadataBytes
	dst.ColumnPublishAggregateMetadataCount += src.ColumnPublishAggregateMetadataCount
	dst.ColumnPublishSharedAppendBytes += src.ColumnPublishSharedAppendBytes
	dst.ColumnPublishSharedAppendCount += src.ColumnPublishSharedAppendCount
	dst.ColumnPublishSharedSegmentAppendBytes += src.ColumnPublishSharedSegmentAppendBytes
	dst.ColumnPublishSharedSegmentAppendCount += src.ColumnPublishSharedSegmentAppendCount
	dst.ColumnPublishDirectViewSegmentAppendBytes += src.ColumnPublishDirectViewSegmentAppendBytes
	dst.ColumnPublishDirectViewSegmentAppendCount += src.ColumnPublishDirectViewSegmentAppendCount
	dst.ColumnPublishRequiredAssetBytes += src.ColumnPublishRequiredAssetBytes
	dst.ColumnPublishManifestBytes += src.ColumnPublishManifestBytes
	dst.ColumnPublishManifestMutationRecords += src.ColumnPublishManifestMutationRecords
	dst.ColumnPublishManifestMutationBytes += src.ColumnPublishManifestMutationBytes
	dst.UniqueIndexPreflight += src.UniqueIndexPreflight
	dst.TemplateRunBuild += src.TemplateRunBuild
	dst.PrimaryRunBuild += src.PrimaryRunBuild
	dst.IndexStateRunBuild += src.IndexStateRunBuild
	dst.SecondaryRunBuild += src.SecondaryRunBuild
	dst.Publish += src.Publish
	dst.SecondaryEntries += src.SecondaryEntries
	dst.SecondaryKeyBytes += src.SecondaryKeyBytes
	dst.SecondarySortedRuns += src.SecondarySortedRuns
	dst.SecondaryUnsortedRuns += src.SecondaryUnsortedRuns
}

func benchmarkReportCollectionInsertStats(b *testing.B, docs, batches int, stats collections.CollectionInsertStats) {
	b.Helper()
	if docs <= 0 {
		return
	}
	reportDuration := func(name string, d time.Duration) {
		if d > 0 {
			b.ReportMetric(float64(d.Nanoseconds())/float64(docs), name)
		}
	}
	reportDuration("prepare_ns/doc", stats.PrepareDocuments)
	reportDuration("index_state_extract_ns/doc", stats.IndexStateExtraction)
	reportDuration("duplicate_preflight_ns/doc", stats.DuplicateDocumentPreflight)
	reportDuration("retained_payload_prepare_ns/doc", stats.RetainedPayloadPrepare)
	reportDuration("retained_payload_semantic_stream_declared_row_prepare_ns/doc", stats.RetainedPayloadSemanticStreamDeclaredRowPrepare)
	reportDuration("retained_payload_semantic_stream_block_prepare_wall_ns/doc", stats.RetainedPayloadSemanticStreamBlockPrepareWall)
	reportDuration("retained_payload_semantic_stream_block_collect_ns/doc", stats.RetainedPayloadSemanticStreamBlockCollect)
	reportDuration("retained_payload_semantic_stream_block_encoder_setup_ns/doc", stats.RetainedPayloadSemanticStreamBlockEncoderSetup)
	reportDuration("retained_payload_semantic_stream_block_raw_encode_ns/doc", stats.RetainedPayloadSemanticStreamBlockRawEncode)
	reportDuration("retained_payload_semantic_stream_block_stored_encode_ns/doc", stats.RetainedPayloadSemanticStreamBlockStoredEncode)
	reportDuration("retained_payload_semantic_stream_block_finalize_ns/doc", stats.RetainedPayloadSemanticStreamBlockFinalize)
	reportDuration("retained_payload_semantic_stream_table_build_ns/doc", stats.RetainedPayloadSemanticStreamTableBuild)
	reportDuration("retained_payload_vlog_pointerize_ns/doc", stats.RetainedPayloadValueLogPointerize)
	reportDuration("retained_stream_vlog_pointerize_ns/doc", stats.RetainedStreamValueLogPointerize)
	reportDuration("column_publish_build_column_delta_ns/doc", stats.ColumnPublishBuildColumnDelta)
	reportDuration("column_publish_build_system_delta_ns/doc", stats.ColumnPublishBuildSystemDelta)
	reportDuration("column_publish_commit_ns/doc", stats.ColumnPublishCommit)
	reportDuration("column_publish_write_lock_wait_ns/doc", stats.ColumnPublishWriteLockWait)
	reportDuration("column_publish_preflight_ns/doc", stats.ColumnPublishPreflight)
	reportDuration("column_publish_command_wal_append_ns/doc", stats.ColumnPublishCommandWALAppend)
	reportDuration("column_publish_ordered_root_apply_ns/doc", stats.ColumnPublishOrderedRootApply)
	reportDuration("column_publish_system_root_apply_ns/doc", stats.ColumnPublishSystemRootApply)
	reportDuration("column_publish_finalize_ns/doc", stats.ColumnPublishFinalize)
	reportDuration("column_publish_finalize_prepare_durability_ns/doc", stats.ColumnPublishFinalizePrepareDurability)
	reportDuration("column_publish_finalize_candidate_build_ns/doc", stats.ColumnPublishFinalizeCandidateBuild)
	reportDuration("column_publish_candidate_visible_base_clone_ns/doc", stats.ColumnPublishFinalizeCandidateVisibleBaseClone)
	reportDuration("column_publish_candidate_inherited_filter_ns/doc", stats.ColumnPublishFinalizeCandidateInheritedFilter)
	reportDuration("column_publish_candidate_fresh_capture_ns/doc", stats.ColumnPublishFinalizeCandidateFreshCapture)
	reportDuration("column_publish_candidate_closure_assemble_ns/doc", stats.ColumnPublishFinalizeCandidateClosureAssemble)
	reportDuration("column_publish_candidate_visible_clone_ns/doc", stats.ColumnPublishFinalizeCandidateVisibleClone)
	reportDuration("column_publish_candidate_cow_prepare_ns/doc", stats.ColumnPublishFinalizeCandidateCOWPrepare)
	reportDuration("column_publish_candidate_other_ns/doc", stats.ColumnPublishFinalizeCandidateOther)
	reportDuration("column_publish_finalize_enqueue_activation_ns/doc", stats.ColumnPublishFinalizeEnqueueActivation)
	reportDuration("column_publish_finalize_admission_wait_ns/doc", stats.ColumnPublishFinalizeAdmissionWait)
	reportDuration("column_publish_finalize_durability_wait_ns/doc", stats.ColumnPublishFinalizeDurabilityWait)
	reportDuration("column_publish_post_finalize_ns/doc", stats.ColumnPublishPostFinalize)
	reportDuration("column_publish_document_extraction_ns/doc", stats.ColumnPublishDocumentExtraction)
	reportDuration("column_publish_declared_column_encoding_ns/doc", stats.ColumnPublishDeclaredColumnEncoding)
	reportDuration("column_publish_asset_prepare_ns/doc", stats.ColumnPublishAssetPreparation)
	reportDuration("column_publish_row_asset_prepare_ns/doc", stats.ColumnPublishRowAssetPreparation)
	reportDuration("column_publish_typed_column_prepare_ns/doc", stats.ColumnPublishTypedColumnPreparation)
	reportDuration("column_publish_typed_column_dictionary_build_ns/doc", stats.ColumnPublishTypedColumnDictionaryBuild)
	reportDuration("column_publish_typed_column_row_materialization_ns/doc", stats.ColumnPublishTypedColumnRowMaterialization)
	reportDuration("column_publish_typed_column_part_build_ns/doc", stats.ColumnPublishTypedColumnPartBuild)
	reportDuration("column_publish_typed_column_image_build_ns/doc", stats.ColumnPublishTypedColumnImageBuild)
	reportDuration("column_publish_dictionary_prepare_ns/doc", stats.ColumnPublishDictionaryPreparation)
	reportDuration("column_publish_int64_prepare_ns/doc", stats.ColumnPublishInt64Preparation)
	reportDuration("column_publish_aggregate_metadata_prepare_ns/doc", stats.ColumnPublishAggregateMetadataPrepare)
	reportDuration("column_publish_row_sidecar_shared_build_ns/doc", stats.ColumnPublishRowSidecarSharedBuild)
	reportDuration("column_publish_asset_append_ns/doc", stats.ColumnPublishAssetAppend)
	reportDuration("column_publish_asset_append_open_ns/doc", stats.ColumnPublishAssetAppendOpen)
	reportDuration("column_publish_asset_append_write_ns/doc", stats.ColumnPublishAssetAppendWrite)
	reportDuration("column_publish_asset_append_close_ns/doc", stats.ColumnPublishAssetAppendClose)
	reportDuration("column_publish_asset_append_file_sync_ns/doc", stats.ColumnPublishAssetAppendFileSync)
	reportDuration("column_publish_asset_append_file_close_ns/doc", stats.ColumnPublishAssetAppendFileClose)
	reportDuration("column_publish_asset_append_dir_sync_ns/doc", stats.ColumnPublishAssetAppendDirSync)
	reportDuration("column_publish_asset_append_cleanup_ns/doc", stats.ColumnPublishAssetAppendCleanup)
	reportDuration("column_publish_manifest_encode_ns/doc", stats.ColumnPublishManifestEncode)
	reportDuration("column_publish_asset_closure_ns/doc", stats.ColumnPublishAssetClosureValidation)
	reportDuration("column_publish_root_delta_ns/doc", stats.ColumnPublishRootDeltaConstruction)
	reportDuration("column_publish_system_delta_ns/doc", stats.ColumnPublishSystemDeltaConstruction)
	reportDuration("column_publish_root_delta_materialize_ns/doc", stats.ColumnPublishRootDeltaMaterialization)
	if stats.ColumnPublishRows > 0 {
		b.ReportMetric(float64(stats.ColumnPublishRows)/float64(docs), "column_publish_rows/doc")
	}
	if stats.ColumnPublishPreparedAssets > 0 {
		b.ReportMetric(float64(stats.ColumnPublishPreparedAssets)/float64(docs), "column_publish_prepared_assets/doc")
	}
	if stats.ColumnPublishRequiredAssetBytes > 0 {
		b.ReportMetric(float64(stats.ColumnPublishRequiredAssetBytes)/float64(docs), "column_publish_required_asset_bytes/doc")
	}
	if stats.ColumnPublishManifestBytes > 0 {
		b.ReportMetric(float64(stats.ColumnPublishManifestBytes)/float64(docs), "column_publish_manifest_bytes/doc")
	}
	if stats.ColumnPublishManifestMutationRecords > 0 {
		b.ReportMetric(float64(stats.ColumnPublishManifestMutationRecords)/float64(batches), "column_publish_manifest_mutation_records/batch")
	}
	if stats.ColumnPublishManifestMutationBytes > 0 {
		b.ReportMetric(float64(stats.ColumnPublishManifestMutationBytes)/float64(batches), "column_publish_manifest_mutation_bytes/batch")
	}
	if stats.ColumnPublishAssetAppenderCloseCount > 0 {
		b.ReportMetric(float64(stats.ColumnPublishAssetAppenderCloseCount)/float64(batches), "column_publish_asset_appender_closes/batch")
	}
	if stats.ColumnPublishAssetAppendFileSyncCount > 0 {
		b.ReportMetric(float64(stats.ColumnPublishAssetAppendFileSyncCount)/float64(batches), "column_publish_asset_file_syncs/batch")
	}
	if stats.ColumnPublishAssetSyncEpochCount > 0 {
		b.ReportMetric(float64(stats.ColumnPublishAssetSyncEpochCount)/float64(batches), "column_publish_asset_sync_epochs/batch")
	}
	if stats.ColumnPublishSharedSegmentAppendCount > 0 {
		b.ReportMetric(float64(stats.ColumnPublishSharedSegmentAppendCount)/float64(batches), "column_publish_shared_segment_asset_appends/batch")
	}
	if stats.ColumnPublishDirectViewSegmentAppendCount > 0 {
		b.ReportMetric(float64(stats.ColumnPublishDirectViewSegmentAppendCount)/float64(batches), "column_publish_direct_view_segment_asset_appends/batch")
	}
	if stats.ColumnPublishSharedSegmentAppenderCloseCount > 0 {
		b.ReportMetric(float64(stats.ColumnPublishSharedSegmentAppenderCloseCount)/float64(batches), "column_publish_shared_segment_appender_closes/batch")
	}
	if stats.ColumnPublishDirectViewSegmentAppenderCloseCount > 0 {
		b.ReportMetric(float64(stats.ColumnPublishDirectViewSegmentAppenderCloseCount)/float64(batches), "column_publish_direct_view_segment_appender_closes/batch")
	}
	if stats.ColumnPublishSharedSegmentAppendFileSyncCount > 0 {
		b.ReportMetric(float64(stats.ColumnPublishSharedSegmentAppendFileSyncCount)/float64(batches), "column_publish_shared_segment_file_syncs/batch")
	}
	if stats.ColumnPublishDirectViewSegmentAppendFileSyncCount > 0 {
		b.ReportMetric(float64(stats.ColumnPublishDirectViewSegmentAppendFileSyncCount)/float64(batches), "column_publish_direct_view_segment_file_syncs/batch")
	}
	if stats.ColumnPublishSharedSegmentAppendSyncEpochCount > 0 {
		b.ReportMetric(float64(stats.ColumnPublishSharedSegmentAppendSyncEpochCount)/float64(batches), "column_publish_shared_segment_sync_epochs/batch")
	}
	if stats.ColumnPublishDirectViewSegmentAppendSyncEpochCount > 0 {
		b.ReportMetric(float64(stats.ColumnPublishDirectViewSegmentAppendSyncEpochCount)/float64(batches), "column_publish_direct_view_segment_sync_epochs/batch")
	}
	if stats.ColumnPublishSharedSegmentAppendBytes > 0 {
		b.ReportMetric(float64(stats.ColumnPublishSharedSegmentAppendBytes)/float64(docs), "column_publish_shared_segment_append_bytes/doc")
	}
	if stats.ColumnPublishDirectViewSegmentAppendBytes > 0 {
		b.ReportMetric(float64(stats.ColumnPublishDirectViewSegmentAppendBytes)/float64(docs), "column_publish_direct_view_segment_append_bytes/doc")
	}
	if stats.RetainedPayloadValueLogValues > 0 {
		b.ReportMetric(float64(stats.RetainedPayloadValueLogValues)/float64(docs), "retained_payload_vlog_values/doc")
	}
	if stats.RetainedPayloadValueLogBytes > 0 {
		b.ReportMetric(float64(stats.RetainedPayloadValueLogBytes)/float64(docs), "retained_payload_vlog_bytes/doc")
	}
	if stats.RetainedStreamValueLogValues > 0 {
		b.ReportMetric(float64(stats.RetainedStreamValueLogValues)/float64(docs), "retained_stream_vlog_values/doc")
	}
	if stats.RetainedStreamValueLogBytes > 0 {
		b.ReportMetric(float64(stats.RetainedStreamValueLogBytes)/float64(docs), "retained_stream_vlog_bytes/doc")
	}
	reportDuration("unique_preflight_ns/doc", stats.UniqueIndexPreflight)
	reportDuration("template_run_ns/doc", stats.TemplateRunBuild)
	reportDuration("primary_run_ns/doc", stats.PrimaryRunBuild)
	reportDuration("index_state_run_ns/doc", stats.IndexStateRunBuild)
	reportDuration("secondary_runs_ns/doc", stats.SecondaryRunBuild)
	reportDuration("publish_ns/doc", stats.Publish)
	if stats.SecondaryEntries > 0 {
		b.ReportMetric(float64(stats.SecondaryEntries)/float64(docs), "secondary_entries/doc")
	}
	if stats.SecondaryKeyBytes > 0 {
		b.ReportMetric(float64(stats.SecondaryKeyBytes)/float64(docs), "secondary_key_bytes/doc")
	}
	if batches > 0 {
		b.ReportMetric(float64(stats.Runs)/float64(batches), "roots/batch")
		b.ReportMetric(float64(stats.SecondarySortedRuns)/float64(batches), "secondary_sorted_runs/batch")
		b.ReportMetric(float64(stats.SecondaryUnsortedRuns)/float64(batches), "secondary_unsorted_runs/batch")
		if stats.BufferedIndexedBatches > 0 {
			b.ReportMetric(float64(stats.BufferedIndexedBatches), "buffered_indexed_batches")
		}
		if stats.BufferedIndexedBypassBatches > 0 {
			b.ReportMetric(float64(stats.BufferedIndexedBypassBatches), "buffered_indexed_bypass_batches")
		}
		if stats.ValidationPreflightReused > 0 {
			b.ReportMetric(float64(stats.ValidationPreflightReused)/float64(batches), "validation_preflight_reused/batch")
		}
		if stats.ValidationPreflightRechecked > 0 {
			b.ReportMetric(float64(stats.ValidationPreflightRechecked)/float64(batches), "validation_preflight_rechecked/batch")
		}
		if stats.RetainedPayloadRows > 0 {
			b.ReportMetric(float64(stats.RetainedPayloadRows)/float64(batches), "retained_payload_rows/batch")
		}
		if stats.RetainedPayloadDeclaredRows > 0 {
			b.ReportMetric(float64(stats.RetainedPayloadDeclaredRows)/float64(batches), "retained_payload_declared_rows/batch")
		}
		if stats.RetainedPayloadSemanticStreamBlocks > 0 {
			b.ReportMetric(float64(stats.RetainedPayloadSemanticStreamBlocks)/float64(batches), "retained_payload_semantic_stream_blocks/batch")
		}
		if stats.RetainedPayloadSemanticStreamWorkerCount > 0 {
			b.ReportMetric(float64(stats.RetainedPayloadSemanticStreamWorkerCount), "retained_payload_semantic_stream_workers")
		}
	}
}

func TestBenchmarkReportCollectionInsertStatsIncludesColumnPublishExtractionM10B(t *testing.T) {
	result := testing.Benchmark(func(b *testing.B) {
		benchmarkReportCollectionInsertStats(b, 10, 1, collections.CollectionInsertStats{
			ColumnPublishDocumentExtraction:                    20 * time.Microsecond,
			ColumnPublishDeclaredColumnEncoding:                30 * time.Microsecond,
			ColumnPublishRows:                                  10,
			ColumnPublishPreparedAssets:                        2,
			ColumnPublishRequiredAssetBytes:                    2048,
			ColumnPublishManifestBytes:                         512,
			ColumnPublishManifestMutationRecords:               7,
			ColumnPublishManifestMutationBytes:                 256,
			ColumnPublishSharedSegmentAppendBytes:              1000,
			ColumnPublishSharedSegmentAppendCount:              3,
			ColumnPublishSharedSegmentAppenderCloseCount:       1,
			ColumnPublishSharedSegmentAppendFileSyncCount:      1,
			ColumnPublishSharedSegmentAppendSyncEpochCount:     1,
			ColumnPublishDirectViewSegmentAppendBytes:          2000,
			ColumnPublishDirectViewSegmentAppendCount:          4,
			ColumnPublishDirectViewSegmentAppenderCloseCount:   1,
			ColumnPublishDirectViewSegmentAppendFileSyncCount:  1,
			ColumnPublishDirectViewSegmentAppendSyncEpochCount: 1,
		})
	})
	if got := result.Extra["column_publish_document_extraction_ns/doc"]; got <= 0 {
		t.Fatalf("document extraction metric=%v want positive", got)
	}
	if got := result.Extra["column_publish_declared_column_encoding_ns/doc"]; got <= 0 {
		t.Fatalf("declared column encoding metric=%v want positive", got)
	}
	if got := result.Extra["column_publish_rows/doc"]; got <= 0 {
		t.Fatalf("column publish rows metric=%v want positive", got)
	}
	if got := result.Extra["column_publish_prepared_assets/doc"]; got <= 0 {
		t.Fatalf("column publish prepared assets metric=%v want positive", got)
	}
	if got := result.Extra["column_publish_required_asset_bytes/doc"]; got <= 0 {
		t.Fatalf("column publish required asset bytes metric=%v want positive", got)
	}
	if got := result.Extra["column_publish_manifest_bytes/doc"]; got <= 0 {
		t.Fatalf("column publish manifest bytes metric=%v want positive", got)
	}
	if got := result.Extra["column_publish_manifest_mutation_records/batch"]; got <= 0 {
		t.Fatalf("column publish manifest mutation records metric=%v want positive", got)
	}
	if got := result.Extra["column_publish_manifest_mutation_bytes/batch"]; got <= 0 {
		t.Fatalf("column publish manifest mutation bytes metric=%v want positive", got)
	}
	for _, name := range []string{
		"column_publish_shared_segment_asset_appends/batch",
		"column_publish_direct_view_segment_asset_appends/batch",
		"column_publish_shared_segment_appender_closes/batch",
		"column_publish_direct_view_segment_appender_closes/batch",
		"column_publish_shared_segment_file_syncs/batch",
		"column_publish_direct_view_segment_file_syncs/batch",
		"column_publish_shared_segment_sync_epochs/batch",
		"column_publish_direct_view_segment_sync_epochs/batch",
		"column_publish_shared_segment_append_bytes/doc",
		"column_publish_direct_view_segment_append_bytes/doc",
	} {
		if got := result.Extra[name]; got <= 0 {
			t.Fatalf("%s metric=%v want positive", name, got)
		}
	}
}

func TestCollectionShapeInsertStatsAddsSegmentAppendMetrics(t *testing.T) {
	var stats collections.CollectionInsertStats
	addCollectionInsertStats(&stats, collections.CollectionInsertStats{
		ColumnPublishSharedSegmentAppendBytes:              100,
		ColumnPublishSharedSegmentAppendCount:              1,
		ColumnPublishSharedSegmentAppenderCloseCount:       2,
		ColumnPublishSharedSegmentAppendFileSyncCount:      3,
		ColumnPublishSharedSegmentAppendSyncEpochCount:     4,
		ColumnPublishDirectViewSegmentAppendBytes:          200,
		ColumnPublishDirectViewSegmentAppendCount:          5,
		ColumnPublishDirectViewSegmentAppenderCloseCount:   6,
		ColumnPublishDirectViewSegmentAppendFileSyncCount:  7,
		ColumnPublishDirectViewSegmentAppendSyncEpochCount: 8,
	})
	addCollectionInsertStats(&stats, collections.CollectionInsertStats{
		ColumnPublishSharedSegmentAppendBytes:              10,
		ColumnPublishSharedSegmentAppendCount:              2,
		ColumnPublishSharedSegmentAppenderCloseCount:       3,
		ColumnPublishSharedSegmentAppendFileSyncCount:      4,
		ColumnPublishSharedSegmentAppendSyncEpochCount:     5,
		ColumnPublishDirectViewSegmentAppendBytes:          20,
		ColumnPublishDirectViewSegmentAppendCount:          6,
		ColumnPublishDirectViewSegmentAppenderCloseCount:   7,
		ColumnPublishDirectViewSegmentAppendFileSyncCount:  8,
		ColumnPublishDirectViewSegmentAppendSyncEpochCount: 9,
	})
	if stats.ColumnPublishSharedSegmentAppendBytes != 110 || stats.ColumnPublishSharedSegmentAppendCount != 3 ||
		stats.ColumnPublishSharedSegmentAppenderCloseCount != 5 || stats.ColumnPublishSharedSegmentAppendFileSyncCount != 7 ||
		stats.ColumnPublishSharedSegmentAppendSyncEpochCount != 9 {
		t.Fatalf("shared segment stats=%+v want accumulated bytes/count/close/file-sync/sync-epoch", stats)
	}
	if stats.ColumnPublishDirectViewSegmentAppendBytes != 220 || stats.ColumnPublishDirectViewSegmentAppendCount != 11 ||
		stats.ColumnPublishDirectViewSegmentAppenderCloseCount != 13 || stats.ColumnPublishDirectViewSegmentAppendFileSyncCount != 15 ||
		stats.ColumnPublishDirectViewSegmentAppendSyncEpochCount != 17 {
		t.Fatalf("direct-view segment stats=%+v want accumulated bytes/count/close/file-sync/sync-epoch", stats)
	}
}

func TestCollectionShapeInsertStatsIncludesRetainedValueLogPointerization(t *testing.T) {
	var stats collections.CollectionInsertStats
	addCollectionInsertStats(&stats, collections.CollectionInsertStats{
		RetainedPayloadSemanticStreamWorkerCount:        2,
		RetainedPayloadSemanticStreamDeclaredRowPrepare: 11 * time.Microsecond,
		RetainedPayloadSemanticStreamBlockPrepareWall:   12 * time.Microsecond,
		RetainedPayloadSemanticStreamBlockCollect:       13 * time.Microsecond,
		RetainedPayloadSemanticStreamBlockEncoderSetup:  14 * time.Microsecond,
		RetainedPayloadSemanticStreamBlockRawEncode:     15 * time.Microsecond,
		RetainedPayloadSemanticStreamBlockStoredEncode:  16 * time.Microsecond,
		RetainedPayloadSemanticStreamBlockFinalize:      17 * time.Microsecond,
		RetainedPayloadSemanticStreamTableBuild:         18 * time.Microsecond,
		RetainedPayloadValueLogPointerize:               10 * time.Microsecond,
		RetainedPayloadValueLogValues:                   3,
		RetainedPayloadValueLogBytes:                    300,
		RetainedStreamValueLogPointerize:                20 * time.Microsecond,
		RetainedStreamValueLogValues:                    4,
		RetainedStreamValueLogBytes:                     400,
	})
	addCollectionInsertStats(&stats, collections.CollectionInsertStats{
		RetainedPayloadSemanticStreamWorkerCount:        4,
		RetainedPayloadSemanticStreamDeclaredRowPrepare: 1 * time.Microsecond,
		RetainedPayloadSemanticStreamBlockPrepareWall:   2 * time.Microsecond,
		RetainedPayloadSemanticStreamBlockCollect:       3 * time.Microsecond,
		RetainedPayloadSemanticStreamBlockEncoderSetup:  4 * time.Microsecond,
		RetainedPayloadSemanticStreamBlockRawEncode:     5 * time.Microsecond,
		RetainedPayloadSemanticStreamBlockStoredEncode:  6 * time.Microsecond,
		RetainedPayloadSemanticStreamBlockFinalize:      7 * time.Microsecond,
		RetainedPayloadSemanticStreamTableBuild:         8 * time.Microsecond,
		RetainedPayloadValueLogPointerize:               5 * time.Microsecond,
		RetainedPayloadValueLogValues:                   2,
		RetainedPayloadValueLogBytes:                    200,
		RetainedStreamValueLogPointerize:                7 * time.Microsecond,
		RetainedStreamValueLogValues:                    1,
		RetainedStreamValueLogBytes:                     100,
	})
	if stats.RetainedPayloadSemanticStreamWorkerCount != 4 {
		t.Fatalf("semantic-stream workers=%d want max 4", stats.RetainedPayloadSemanticStreamWorkerCount)
	}
	if stats.RetainedPayloadSemanticStreamDeclaredRowPrepare != 12*time.Microsecond ||
		stats.RetainedPayloadSemanticStreamBlockPrepareWall != 14*time.Microsecond ||
		stats.RetainedPayloadSemanticStreamBlockCollect != 16*time.Microsecond ||
		stats.RetainedPayloadSemanticStreamBlockEncoderSetup != 18*time.Microsecond ||
		stats.RetainedPayloadSemanticStreamBlockRawEncode != 20*time.Microsecond ||
		stats.RetainedPayloadSemanticStreamBlockStoredEncode != 22*time.Microsecond ||
		stats.RetainedPayloadSemanticStreamBlockFinalize != 24*time.Microsecond ||
		stats.RetainedPayloadSemanticStreamTableBuild != 26*time.Microsecond {
		t.Fatalf("semantic-stream timing stats not accumulated: %+v", stats)
	}
	if stats.RetainedPayloadValueLogPointerize != 15*time.Microsecond {
		t.Fatalf("payload pointerize=%s want 15us", stats.RetainedPayloadValueLogPointerize)
	}
	if stats.RetainedPayloadValueLogValues != 5 || stats.RetainedPayloadValueLogBytes != 500 {
		t.Fatalf("payload value-log values=%d bytes=%d want 5/500", stats.RetainedPayloadValueLogValues, stats.RetainedPayloadValueLogBytes)
	}
	if stats.RetainedStreamValueLogPointerize != 27*time.Microsecond {
		t.Fatalf("stream pointerize=%s want 27us", stats.RetainedStreamValueLogPointerize)
	}
	if stats.RetainedStreamValueLogValues != 5 || stats.RetainedStreamValueLogBytes != 500 {
		t.Fatalf("stream value-log values=%d bytes=%d want 5/500", stats.RetainedStreamValueLogValues, stats.RetainedStreamValueLogBytes)
	}

	result := testing.Benchmark(func(b *testing.B) {
		benchmarkReportCollectionInsertStats(b, 10, 1, stats)
	})
	for _, name := range []string{
		"retained_payload_vlog_pointerize_ns/doc",
		"retained_payload_vlog_values/doc",
		"retained_payload_vlog_bytes/doc",
		"retained_stream_vlog_pointerize_ns/doc",
		"retained_stream_vlog_values/doc",
		"retained_stream_vlog_bytes/doc",
	} {
		if got := result.Extra[name]; got <= 0 {
			t.Fatalf("%s metric=%v want positive", name, got)
		}
	}
}

func collectionShapeIndexes(indexCount int) []collections.IndexDefinition {
	switch indexCount {
	case 0:
		return nil
	case 1:
		return []collections.IndexDefinition{{Name: "email_idx", Field: "email", ValueType: collections.IndexValueString, Unique: true}}
	case 2:
		return []collections.IndexDefinition{
			{Name: "email_idx", Field: "email", ValueType: collections.IndexValueString, Unique: true},
			{Name: "city_idx", Field: "city", ValueType: collections.IndexValueString},
		}
	case 3:
		return []collections.IndexDefinition{
			{Name: "email_idx", Field: "email", ValueType: collections.IndexValueString, Unique: true},
			{Name: "city_idx", Field: "city", ValueType: collections.IndexValueString},
			{Name: "name_idx", Field: "name", ValueType: collections.IndexValueString},
		}
	default:
		panic(fmt.Sprintf("unsupported collection benchmark index count %d", indexCount))
	}
}

func collectionSingleStringIndexes(indexCount int) []collections.IndexDefinition {
	switch indexCount {
	case 0:
		return nil
	case 1:
		return []collections.IndexDefinition{{Name: "value_idx", Field: "value", ValueType: collections.IndexValueString, Unique: true}}
	default:
		panic(fmt.Sprintf("unsupported single-string benchmark index count %d", indexCount))
	}
}

func benchmarkSingleStringDocument(n int) []byte {
	out := make([]byte, 0, len(`{"value":"value-000000000"}`))
	out = append(out, `{"value":"value-`...)
	out = appendZeroPaddedInt(out, n, 9)
	out = append(out, `"}`...)
	return out
}

func benchmarkSingleStringDocumentBatch(tb testing.TB, start, count int) ([][]byte, [][]byte) {
	tb.Helper()
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		docNum := start + i
		ids[i] = benchmarkDocumentID(docNum)
		docs[i] = benchmarkSingleStringDocument(docNum)
	}
	return ids, docs
}

func benchmarkCollectionShapeInsertBatch(b *testing.B, indexCount int, checkpoint bool) {
	backend, collection := openBenchmarkCollection(b, fmt.Sprintf("bench_shape_insert_%d", indexCount), collectionShapeIndexes(indexCount)...)
	targetBatchSize := benchmarkBatchSize(b)
	startKeyFallback, startPrefixFallback := benchmarkNativeProbeFallbackCounters(b, backend)
	startTreeDBStats := backend.Stats()
	var insertElapsed time.Duration
	var bufferedFlushElapsed time.Duration
	var syncElapsed time.Duration
	var insertStats collections.CollectionInsertStats
	batches := 0
	bufferedIndexedWrites := collection.Meta().Options.BufferedIndexedWrites

	metricName := "target_docs/batch"
	if checkpoint {
		metricName = "target_docs/checkpoint"
	}
	b.ReportAllocs()
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkDocumentBatch(b, inserted, batchSize, true)
		b.StartTimer()

		insertStart := time.Now()
		if _, err := collection.InsertBatch(ids, docs); err != nil {
			b.Fatalf("shape insert batch indexes=%d: %v", indexCount, err)
		}
		insertElapsed += time.Since(insertStart)
		b.StopTimer()
		addCollectionInsertStats(&insertStats, collection.LastInsertStats())
		batches++
		if checkpoint {
			b.StartTimer()
			if bufferedIndexedWrites {
				flushStart := time.Now()
				if err := collection.Flush(); err != nil {
					b.Fatalf("flush buffered indexed writes: %v", err)
				}
				bufferedFlushElapsed += time.Since(flushStart)
			}
			syncStart := time.Now()
			benchmarkSyncBoundary(b, backend)
			syncElapsed += time.Since(syncStart)
			b.StopTimer()
		}
		inserted += batchSize
	}
	if bufferedIndexedWrites && !checkpoint {
		b.StartTimer()
		flushStart := time.Now()
		if err := collection.Flush(); err != nil {
			b.Fatalf("flush buffered indexed writes: %v", err)
		}
		bufferedFlushElapsed += time.Since(flushStart)
		b.StopTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(targetBatchSize), metricName)
	b.ReportMetric(float64(indexCount), "indexes/doc")
	if bufferedIndexedWrites {
		meta := collection.Meta()
		b.ReportMetric(1, "buffered_indexed_writes")
		if meta.Options.BufferedIndexedWriteMaxDocuments > 0 {
			b.ReportMetric(float64(meta.Options.BufferedIndexedWriteMaxDocuments), "buffered_max_docs")
		}
		if meta.Options.BufferedIndexedWriteMaxBytes > 0 {
			b.ReportMetric(float64(meta.Options.BufferedIndexedWriteMaxBytes), "buffered_max_bytes")
		}
		if meta.Options.BufferedIndexedWriteMaxRootRuns > 0 {
			b.ReportMetric(float64(meta.Options.BufferedIndexedWriteMaxRootRuns), "buffered_max_root_runs")
		}
		if meta.Options.BufferedIndexedAsyncFlush {
			b.ReportMetric(1, "buffered_async_flush")
			if meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits > 0 {
				b.ReportMetric(float64(meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits), "buffered_async_max_units")
			}
		}
		if insertStats.BufferedIndexedBatches > 0 && insertStats.BufferedIndexedBypassBatches == 0 && insertElapsed > 0 {
			b.ReportMetric(float64(insertElapsed.Nanoseconds())/float64(b.N), "buffered_insert_ns/doc")
		}
		if insertStats.BufferedIndexedBatches > 0 && insertStats.BufferedIndexedBypassBatches == 0 && bufferedFlushElapsed > 0 {
			b.ReportMetric(float64(bufferedFlushElapsed.Nanoseconds())/float64(b.N), "buffered_flush_ns/doc")
		}
	}
	if checkpoint {
		benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
	}
	benchmarkReportCollectionInsertStats(b, b.N, batches, insertStats)
	benchmarkReportTreeDBSpanNativeStatDeltas(b, backend, startTreeDBStats)
	benchmarkReportNativeProbeFallbackDeltas(b, backend, startKeyFallback, startPrefixFallback)
	benchmarkReportTreeDBDiskUsage(b, backend, b.N)
}

func BenchmarkCollectionShapeInsertBatch(b *testing.B) {
	for _, indexCount := range []int{0, 1, 2, 3} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeInsertBatch(b, indexCount, false)
		})
	}
}

func BenchmarkCollectionShapeInsertBatchCheckpoint(b *testing.B) {
	for _, indexCount := range []int{0, 1, 2, 3} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeInsertBatch(b, indexCount, true)
		})
	}
}

func benchmarkCollectionShapeSingleStringInsertBatch(b *testing.B, indexCount int, checkpoint bool) {
	if benchmarkCollectionDocumentFormat(b) != collections.DocumentFormatJSON {
		b.Skip("single-string shape benchmark uses JSON documents")
	}
	backend, collection := openBenchmarkCollection(b, fmt.Sprintf("bench_shape_single_string_%d", indexCount), collectionSingleStringIndexes(indexCount)...)
	targetBatchSize := benchmarkBatchSize(b)
	startKeyFallback, startPrefixFallback := benchmarkNativeProbeFallbackCounters(b, backend)
	startTreeDBStats := backend.Stats()
	var insertElapsed time.Duration
	var syncElapsed time.Duration
	var insertStats collections.CollectionInsertStats
	batches := 0

	metricName := "target_docs/batch"
	if checkpoint {
		metricName = "target_docs/checkpoint"
	}
	b.ReportAllocs()
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkSingleStringDocumentBatch(b, inserted, batchSize)
		b.StartTimer()

		insertStart := time.Now()
		if _, err := collection.InsertBatch(ids, docs); err != nil {
			b.Fatalf("single-string insert batch indexes=%d: %v", indexCount, err)
		}
		insertElapsed += time.Since(insertStart)
		b.StopTimer()
		addCollectionInsertStats(&insertStats, collection.LastInsertStats())
		batches++
		if checkpoint {
			b.StartTimer()
			syncStart := time.Now()
			benchmarkSyncBoundary(b, backend)
			syncElapsed += time.Since(syncStart)
			b.StopTimer()
		}
		inserted += batchSize
	}
	b.StopTimer()
	b.ReportMetric(float64(targetBatchSize), metricName)
	b.ReportMetric(float64(indexCount), "indexes/doc")
	if checkpoint {
		benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
	}
	benchmarkReportCollectionInsertStats(b, b.N, batches, insertStats)
	benchmarkReportTreeDBSpanNativeStatDeltas(b, backend, startTreeDBStats)
	benchmarkReportNativeProbeFallbackDeltas(b, backend, startKeyFallback, startPrefixFallback)
	benchmarkReportTreeDBDiskUsage(b, backend, b.N)
}

func BenchmarkCollectionShapeInsertBatchSingleStringJSON(b *testing.B) {
	for _, indexCount := range []int{0, 1} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeSingleStringInsertBatch(b, indexCount, false)
		})
	}
}

func BenchmarkCollectionShapeInsertBatchCheckpointSingleStringJSON(b *testing.B) {
	for _, indexCount := range []int{0, 1} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeSingleStringInsertBatch(b, indexCount, true)
		})
	}
}

func benchmarkCollectionShapeReadPrimary(b *testing.B, indexCount int, parallel bool) {
	backend, collection := openBenchmarkCollection(b, fmt.Sprintf("bench_shape_read_%d", indexCount), collectionShapeIndexes(indexCount)...)
	ids := seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, true)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ReportMetric(float64(indexCount), "indexes/doc")
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if _, err := collection.Get(ids[i%len(ids)]); err != nil {
					b.Errorf("shape parallel primary read indexes=%d: %v", indexCount, err)
				}
				i++
			}
		})
		return
	}
	for i := 0; i < b.N; i++ {
		if _, err := collection.Get(ids[i%len(ids)]); err != nil {
			b.Fatalf("shape primary read indexes=%d: %v", indexCount, err)
		}
	}
}

func BenchmarkCollectionShapeReadPrimary(b *testing.B) {
	for _, indexCount := range []int{0, 2} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeReadPrimary(b, indexCount, false)
		})
	}
}

func BenchmarkCollectionShapeReadPrimaryParallel(b *testing.B) {
	for _, indexCount := range []int{0, 2} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeReadPrimary(b, indexCount, true)
		})
	}
}

func benchmarkCollectionShapeReadPrimaryInto(b *testing.B, indexCount int, parallel bool) {
	backend, collection := openBenchmarkCollection(b, fmt.Sprintf("bench_shape_read_into_%d", indexCount), collectionShapeIndexes(indexCount)...)
	ids := seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, true)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ReportMetric(float64(indexCount), "indexes/doc")
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			buf := make([]byte, 0, 8<<10)
			for pb.Next() {
				got, found, err := collection.GetInto(ids[i%len(ids)], buf)
				if err != nil {
					b.Errorf("shape parallel primary read into indexes=%d: %v", indexCount, err)
				}
				if !found {
					b.Errorf("shape parallel primary read into indexes=%d: document not found", indexCount)
				}
				buf = got
				i++
			}
		})
		return
	}
	buf := make([]byte, 0, 8<<10)
	for i := 0; i < b.N; i++ {
		got, found, err := collection.GetInto(ids[i%len(ids)], buf)
		if err != nil {
			b.Fatalf("shape primary read into indexes=%d: %v", indexCount, err)
		}
		if !found {
			b.Fatalf("shape primary read into indexes=%d: document not found", indexCount)
		}
		buf = got
	}
}

func BenchmarkCollectionShapeReadPrimaryInto(b *testing.B) {
	for _, indexCount := range []int{0, 2} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeReadPrimaryInto(b, indexCount, false)
		})
	}
}

func BenchmarkCollectionShapeReadPrimaryIntoParallel(b *testing.B) {
	for _, indexCount := range []int{0, 2} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeReadPrimaryInto(b, indexCount, true)
		})
	}
}

func benchmarkCollectionMixedReadWrite(b *testing.B, secondaryRead bool) {
	indexes := collectionShapeIndexes(2)
	collectionName := "bench_shape_mixed_read_write"
	backend, seedCollection := openBenchmarkCollection(b, collectionName, indexes...)
	seedDocs := benchmarkIntEnv(b, "TREEDB_COLLECTION_MIXED_SEED_DOCS", defaultCollectionMixedSeedDocs)
	if seedDocs <= 0 {
		seedDocs = defaultCollectionMixedSeedDocs
	}
	ids := seedBenchmarkCollection(b, seedCollection, 0, seedDocs, true)
	if err := seedCollection.Flush(); err != nil {
		b.Fatalf("flush mixed seed collection: %v", err)
	}
	benchmarkSyncBoundary(b, backend)

	manager := collections.NewCollectionManager(backend)
	readerCollection, err := manager.OpenCollection(collectionName)
	if err != nil {
		b.Fatalf("open mixed reader collection: %v", err)
	}
	writerCollection, err := manager.OpenCollection(collectionName)
	if err != nil {
		b.Fatalf("open mixed writer collection: %v", err)
	}

	writeBatchSize := benchmarkIntEnv(b, "TREEDB_COLLECTION_MIXED_WRITE_BATCH_SIZE", defaultCollectionMixedWriteBatchSize)
	if writeBatchSize <= 0 {
		writeBatchSize = defaultCollectionMixedWriteBatchSize
	}
	if maxBatch := benchmarkBatchSize(b); writeBatchSize > maxBatch {
		writeBatchSize = maxBatch
	}

	var stop atomic.Bool
	var writerDocs atomic.Uint64
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	documentFormat := benchmarkCollectionDocumentFormat(b)

	b.ReportAllocs()
	b.ReportMetric(float64(seedDocs), "seed_docs")
	b.ReportMetric(float64(writeBatchSize), "writer_docs/batch")
	b.ResetTimer()
	start := time.Now()

	go func() {
		defer wg.Done()
		var templateEncoder collections.TemplateV1Encoder
		for next := 1_000_000; !stop.Load(); next += writeBatchSize {
			ids := make([][]byte, writeBatchSize)
			docs := make([][]byte, writeBatchSize)
			for i := 0; i < writeBatchSize; i++ {
				docNum := next + i
				ids[i] = benchmarkDocumentID(docNum)
				if documentFormat == collections.DocumentFormatTemplateV1 {
					doc, err := templateEncoder.EncodeDocument(
						[]string{"name", "email", "city", "pad"},
						[]any{
							fmt.Sprintf("user-%09d", docNum),
							fmt.Sprintf("user-%09d@example.com", docNum),
							fmt.Sprintf("city-%02d", docNum%collectionBenchCities),
							collectionBenchIndexedPad,
						},
					)
					if err != nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}
					docs[i] = doc
				} else if documentFormat == collections.DocumentFormatBSON {
					docs[i] = benchmarkBSONDocument(b, docNum, true)
				} else {
					docs[i] = benchmarkIndexedDocument(docNum)
				}
			}
			if _, err := writerCollection.InsertBatch(ids, docs); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			writerDocs.Add(uint64(writeBatchSize))
		}
	}()

	readerStride := runtime.GOMAXPROCS(0)
	if readerStride <= 0 {
		readerStride = 1
	}
	var readerOffsets atomic.Uint64
	nextReaderStart := func(limit int) int {
		if limit <= 0 {
			return 0
		}
		spacing := limit / readerStride
		if spacing <= 0 {
			spacing = 1
		}
		readerID := int(readerOffsets.Add(1) - 1)
		return (readerID * spacing) % limit
	}

	if secondaryRead {
		b.RunParallel(func(pb *testing.PB) {
			i := nextReaderStart(seedDocs)
			for pb.Next() {
				email := fmt.Sprintf("user-%09d@example.com", i%seedDocs)
				if _, err := readerCollection.FindByIndex("email_idx", email); err != nil {
					b.Errorf("mixed secondary read: %v", err)
				}
				i += readerStride
			}
		})
	} else {
		b.RunParallel(func(pb *testing.PB) {
			i := nextReaderStart(len(ids))
			for pb.Next() {
				if _, err := readerCollection.Get(ids[i%len(ids)]); err != nil {
					b.Errorf("mixed primary read: %v", err)
				}
				i += readerStride
			}
		})
	}

	b.StopTimer()
	stop.Store(true)
	wg.Wait()
	end := time.Now()
	elapsed := end.Sub(start)
	select {
	case err := <-errCh:
		b.Fatalf("mixed writer: %v", err)
	default:
	}
	if elapsed > 0 {
		b.ReportMetric(float64(writerDocs.Load())/elapsed.Seconds(), "writer_docs/sec")
	}
}

func BenchmarkCollectionMixedReadWritePrimary(b *testing.B) {
	benchmarkCollectionMixedReadWrite(b, false)
}

func BenchmarkCollectionMixedReadWriteSecondaryUnique(b *testing.B) {
	benchmarkCollectionMixedReadWrite(b, true)
}

func benchmarkCollectionMixedReadWriteScaling(b *testing.B, readers, writers int, secondaryRead bool) {
	if readers <= 0 {
		readers = 1
	}
	if writers <= 0 {
		writers = 1
	}
	indexes := collectionShapeIndexes(2)
	collectionName := fmt.Sprintf("bench_shape_mixed_scaling_r%d_w%d", readers, writers)
	backend, manager, seedCollection := openBenchmarkCollectionWithManager(b, collectionName, indexes...)
	seedDocs := benchmarkIntEnv(b, "TREEDB_COLLECTION_MIXED_SEED_DOCS", defaultCollectionMixedSeedDocs)
	if seedDocs <= 0 {
		seedDocs = defaultCollectionMixedSeedDocs
	}
	ids := seedBenchmarkCollection(b, seedCollection, 0, seedDocs, true)
	if err := seedCollection.Flush(); err != nil {
		b.Fatalf("flush mixed scaling seed collection: %v", err)
	}
	benchmarkSyncBoundary(b, backend)

	readerCollections := make([]*collections.Collection, readers)
	for i := range readerCollections {
		var err error
		readerCollections[i], err = manager.OpenCollection(collectionName)
		if err != nil {
			b.Fatalf("open mixed scaling reader collection: %v", err)
		}
	}
	writerCollections := make([]*collections.Collection, writers)
	for i := range writerCollections {
		var err error
		writerCollections[i], err = manager.OpenCollection(collectionName)
		if err != nil {
			b.Fatalf("open mixed scaling writer collection: %v", err)
		}
	}

	writeBatchSize := benchmarkIntEnv(b, "TREEDB_COLLECTION_MIXED_WRITE_BATCH_SIZE", defaultCollectionMixedWriteBatchSize)
	if writeBatchSize <= 0 {
		writeBatchSize = defaultCollectionMixedWriteBatchSize
	}
	if maxBatch := benchmarkBatchSize(b); writeBatchSize > maxBatch {
		writeBatchSize = maxBatch
	}

	var stop atomic.Bool
	var writerDocs atomic.Uint64
	errCh := make(chan error, readers+writers)
	startCh := make(chan struct{})
	var readerWG sync.WaitGroup
	var writerWG sync.WaitGroup
	documentFormat := benchmarkCollectionDocumentFormat(b)

	writeDocumentBatch := func(start, count int, encoder *collections.TemplateV1Encoder) ([][]byte, [][]byte, error) {
		ids := make([][]byte, count)
		docs := make([][]byte, count)
		for i := 0; i < count; i++ {
			docNum := start + i
			ids[i] = benchmarkDocumentID(docNum)
			switch documentFormat {
			case collections.DocumentFormatTemplateV1:
				doc, err := encoder.EncodeDocument(
					[]string{"name", "email", "city", "pad"},
					[]any{
						fmt.Sprintf("user-%09d", docNum),
						fmt.Sprintf("user-%09d@example.com", docNum),
						fmt.Sprintf("city-%02d", docNum%collectionBenchCities),
						collectionBenchIndexedPad,
					},
				)
				if err != nil {
					return nil, nil, err
				}
				docs[i] = doc
			case collections.DocumentFormatBSON:
				docs[i] = benchmarkBSONDocument(b, docNum, true)
			default:
				docs[i] = benchmarkIndexedDocument(docNum)
			}
		}
		return ids, docs, nil
	}

	for writerID := 0; writerID < writers; writerID++ {
		writerID := writerID
		writerCollection := writerCollections[writerID]
		writerWG.Add(1)
		go func() {
			defer writerWG.Done()
			<-startCh
			var templateEncoder collections.TemplateV1Encoder
			for next := 1_000_000 + writerID*100_000_000; !stop.Load(); next += writeBatchSize {
				ids, docs, err := writeDocumentBatch(next, writeBatchSize, &templateEncoder)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				if _, err := writerCollection.InsertBatch(ids, docs); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				writerDocs.Add(uint64(writeBatchSize))
			}
		}()
	}

	readBase := b.N / readers
	readRemainder := b.N % readers
	readerStride := max(1, runtime.GOMAXPROCS(0))
	for readerID := 0; readerID < readers; readerID++ {
		readerID := readerID
		readOps := readBase
		if readerID < readRemainder {
			readOps++
		}
		readerCollection := readerCollections[readerID]
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			<-startCh
			if secondaryRead {
				i := (readerID * max(1, seedDocs/readers)) % seedDocs
				for op := 0; op < readOps; op++ {
					email := fmt.Sprintf("user-%09d@example.com", i%seedDocs)
					if _, err := readerCollection.FindByIndex("email_idx", email); err != nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}
					i += readerStride
				}
				return
			}
			i := (readerID * max(1, len(ids)/readers)) % len(ids)
			for op := 0; op < readOps; op++ {
				if _, err := readerCollection.Get(ids[i%len(ids)]); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				i += readerStride
			}
		}()
	}

	b.ReportAllocs()
	b.ReportMetric(float64(readers), "readers")
	b.ReportMetric(float64(writers), "writers")
	b.ReportMetric(float64(seedDocs), "seed_docs")
	b.ReportMetric(float64(writeBatchSize), "writer_docs/batch")
	b.ResetTimer()
	start := time.Now()
	close(startCh)

	readerWG.Wait()
	readerElapsed := time.Since(start)
	b.StopTimer()
	stop.Store(true)
	writerWG.Wait()
	writerElapsed := time.Since(start)
	flushStart := time.Now()
	if err := manager.FlushAll(); err != nil {
		b.Fatalf("flush mixed scaling manager: %v", err)
	}
	flushElapsed := time.Since(flushStart)
	select {
	case err := <-errCh:
		b.Fatalf("mixed scaling benchmark: %v", err)
	default:
	}
	if readerElapsed > 0 {
		b.ReportMetric(float64(b.N)/readerElapsed.Seconds(), "reader_ops/sec")
	}
	if writerElapsed > 0 {
		b.ReportMetric(float64(writerDocs.Load())/writerElapsed.Seconds(), "writer_docs/sec")
	}
	if docs := writerDocs.Load(); docs > 0 && flushElapsed > 0 {
		b.ReportMetric(float64(flushElapsed.Nanoseconds())/float64(docs), "writer_flush_ns/doc")
	}
}

func BenchmarkCollectionMixedReadWriteScalingPrimary(b *testing.B) {
	for _, tc := range []collectionMixedScaleCase{
		{readers: 1, writers: 1},
		{readers: 4, writers: 1},
		{readers: 8, writers: 2},
	} {
		b.Run(fmt.Sprintf("readers_%d/writers_%d", tc.readers, tc.writers), func(b *testing.B) {
			benchmarkCollectionMixedReadWriteScaling(b, tc.readers, tc.writers, false)
		})
	}
}

func BenchmarkCollectionMixedReadWriteScalingSecondaryUnique(b *testing.B) {
	for _, tc := range []collectionMixedScaleCase{
		{readers: 1, writers: 1},
		{readers: 4, writers: 1},
		{readers: 8, writers: 2},
	} {
		b.Run(fmt.Sprintf("readers_%d/writers_%d", tc.readers, tc.writers), func(b *testing.B) {
			benchmarkCollectionMixedReadWriteScaling(b, tc.readers, tc.writers, true)
		})
	}
}
