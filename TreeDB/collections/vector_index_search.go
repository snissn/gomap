package collections

import (
	"errors"
	"fmt"
	"math"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

// ErrVectorIndexSearchUnavailable reports that the requested vector index is
// not currently searchable through the selected product path.
var ErrVectorIndexSearchUnavailable = errors.New("collections: vector index search unavailable")

// VectorIndexSearchPath identifies the physical implementation used for a
// public vector-index search.
type VectorIndexSearchPath string

const (
	// VectorIndexSearchPathColumnGraphNativeReader searches persisted column_graph
	// state through the native reader. Current healthy indexes use TVIS/base
	// typed-column sources; legacy physical graph rows are compatibility fallback
	// only. It does not build or query a decoded in-memory ColumnVectorGraph.
	VectorIndexSearchPathColumnGraphNativeReader VectorIndexSearchPath = "column_graph_native_reader"
)

// VectorIndexSearchStatsMode selects how much vector graph-search telemetry is
// collected. The zero value preserves full diagnostics for compatibility.
// Production/minimal mode is the steady-state low-overhead mode: it keeps
// source-health, admission, result, and fallback counters while avoiding
// per-edge/per-candidate diagnostic counters on the healthy prepared path.
type VectorIndexSearchStatsMode string

const (
	VectorIndexSearchStatsModeDefault         VectorIndexSearchStatsMode = ""
	VectorIndexSearchStatsModeMinimal         VectorIndexSearchStatsMode = "minimal"
	VectorIndexSearchStatsModeProduction      VectorIndexSearchStatsMode = "production"
	VectorIndexSearchStatsModeFullDiagnostics VectorIndexSearchStatsMode = "full_diagnostics"
	VectorIndexSearchStatsModeBenchmarkDebug  VectorIndexSearchStatsMode = "benchmark_debug"
)

func columnVectorGraphNativeSearchStatsModeFromPublic(mode VectorIndexSearchStatsMode) (columnVectorGraphNativeSearchStatsMode, error) {
	switch mode {
	case VectorIndexSearchStatsModeDefault, VectorIndexSearchStatsModeFullDiagnostics:
		return columnVectorGraphNativeSearchStatsModeFullDiagnostics, nil
	case VectorIndexSearchStatsModeMinimal, VectorIndexSearchStatsModeProduction:
		return columnVectorGraphNativeSearchStatsModeMinimal, nil
	case VectorIndexSearchStatsModeBenchmarkDebug:
		return columnVectorGraphNativeSearchStatsModeBenchmarkDebug, nil
	default:
		return columnVectorGraphNativeSearchStatsModeDefault, fmt.Errorf("collections: vector index search stats mode %q is unsupported", mode)
	}
}

// VectorIndexSearcherOptions configures a reusable snapshot-bound vector-index
// searcher. Reuse this path for steady-state queries when setup/open cost should
// not be paid on every search.
type VectorIndexSearcherOptions struct {
	// IndexName is the declared collection vector-index name.
	IndexName string
	// MaxDecodedBlocks bounds the generic physical column reader cache used by
	// the column_graph path. Zero uses the reader default.
	MaxDecodedBlocks int
}

// VectorIndexSearcherSearchOptions configures one Search call on an opened
// VectorIndexSearcher.
type VectorIndexSearcherSearchOptions struct {
	// Query is the query vector. V4 column_graph search supports cosine indexes.
	Query []float32
	// TopK is the maximum number of nearest results to return.
	TopK int
	// EfSearch bounds graph exploration. Zero uses the persisted index default.
	EfSearch int
	// IncludeDocuments materializes documents after top-k selection.
	IncludeDocuments bool
	// DocumentFetchOptions controls optional projected final-fetch materialization.
	// It is used only when IncludeDocuments is true; the zero value returns full documents.
	DocumentFetchOptions DocumentFetchOptions
	// StatsMode selects graph-search telemetry detail. The zero value preserves
	// full diagnostics; production/minimal mode avoids per-candidate/per-edge
	// diagnostic accounting on the healthy combined prepared path while still
	// reporting source-health, fallback, admission, and result counters.
	StatsMode VectorIndexSearchStatsMode
	// scoreBatchMode is an internal test/benchmark hook for exact-order indexed
	// HNSW scoring. The public zero value follows the runtime default scoring
	// gate: eligible prepared views may use indexed/gathered scoring; legacy,
	// non-prepared, and fallback routes remain scalar.
	scoreBatchMode columnVectorGraphScoreBatchMode
}

// VectorIndexSearchResult is one public vector-index search hit.
type VectorIndexSearchResult struct {
	// ID is the collection document ID. Search and SearchVectorIndex return
	// response-owned bytes. SearchWithBuffer returns bytes owned by the caller's
	// VectorIndexSearchBuffer and valid only until that buffer is reused or reset.
	ID []byte `json:"id"`
	// Ordinal is the vector row ordinal in the persisted column_graph index.
	Ordinal int `json:"ordinal"`
	// Score is the cosine similarity score for the result.
	Score float64 `json:"score"`
	// Document is populated only when IncludeDocuments is true.
	Document []byte `json:"document,omitempty"`
}

// VectorIndexSearchStats reports search telemetry. Graph/search and reader
// counters are per-search deltas unless the field starts with Open; Open*
// counters describe the bound reader setup performed before Search.
type VectorIndexSearchStats struct {
	// GraphRows is the number of legacy physical graph rows resident in the bound reader. Healthy current typed-column search reports zero.
	GraphRows uint64 `json:"graph_rows,omitempty"`
	// CandidateRows is the candidate row domain after any internal row-selection/visibility composition.
	CandidateRows uint64 `json:"candidate_rows,omitempty"`
	// Candidates is the number of candidate nodes scored by graph search.
	Candidates uint64 `json:"candidates,omitempty"`
	// Edges is the number of graph edges considered by graph search.
	Edges uint64 `json:"edges,omitempty"`
	// VisitedNodes is the operation-specific graph node score/evaluation visit counter. It includes upper-layer probes and may exceed Candidates.
	VisitedNodes uint64 `json:"visited_nodes,omitempty"`
	// VisitedEdges is the operation-specific graph edge visit counter. It aliases Edges for current column_graph search.
	VisitedEdges uint64 `json:"visited_edges,omitempty"`
	// VectorBytesRead is the logical vector payload bytes read while scoring candidates.
	VectorBytesRead uint64 `json:"vector_bytes_read,omitempty"`
	// NormBytesRead is the logical inverse-norm payload bytes read while scoring candidates.
	NormBytesRead uint64 `json:"norm_bytes_read,omitempty"`
	// AdjacencyBytesRead is the logical adjacency payload bytes read while expanding graph nodes.
	AdjacencyBytesRead uint64 `json:"adjacency_bytes_read,omitempty"`
	// CandidateFetches is the per-search count of vector row fetches for scored candidates.
	CandidateFetches uint64 `json:"candidate_fetches,omitempty"`
	// ScoreBatchCalls counts logical score calls: singleton scalar calls and indexed tile calls.
	ScoreBatchCalls uint64 `json:"score_batch_calls,omitempty"`
	// ScoreBatchCandidates counts candidate scores produced by score calls.
	ScoreBatchCandidates uint64 `json:"score_batch_candidates,omitempty"`
	// ScoreBatchMaxTileSize reports the largest score tile size observed by this search.
	ScoreBatchMaxTileSize uint64 `json:"score_batch_max_tile_size,omitempty"`
	// ScoreBatchOptimizedCalls counts score calls reported as optimized by the vectorops backend.
	ScoreBatchOptimizedCalls uint64 `json:"score_batch_optimized,omitempty"`
	// ScoreBatchScalarFallbackCalls counts score calls completed through scalar/fallback execution.
	ScoreBatchScalarFallbackCalls uint64 `json:"score_batch_fallback,omitempty"`
	// PreparedScoreCalls counts candidate scores produced by the prepared vector/norm scoring view.
	PreparedScoreCalls uint64 `json:"prepared_score_calls,omitempty"`
	// ScoreFloat64Fallbacks counts rare dot-product retries using float64 after a non-finite float32 dot.
	ScoreFloat64Fallbacks uint64 `json:"score_float64_fallbacks,omitempty"`
	// ExpansionFetches is the per-search count of adjacency row fetches for expanded nodes.
	ExpansionFetches uint64 `json:"expansion_fetches,omitempty"`
	// ResultFetches is the per-search count of vector row fetches for final results.
	ResultFetches uint64 `json:"result_fetches,omitempty"`
	// DocumentsFetched is the post-top-k document materialization count.
	DocumentsFetched uint64 `json:"documents_fetched,omitempty"`
	// DocumentsMissing is the post-top-k materialization miss count.
	DocumentsMissing uint64 `json:"documents_missing,omitempty"`
	// DocumentBytes is the materialized response document byte count.
	DocumentBytes uint64 `json:"document_bytes,omitempty"`
	// DocumentOutputBytes is the materialized response document byte count attributed to projection/materialization output.
	DocumentOutputBytes uint64 `json:"document_output_bytes,omitempty"`
	// DocumentFieldsReconstructed counts top-level fields emitted into response documents.
	DocumentFieldsReconstructed uint64 `json:"document_fields_reconstructed,omitempty"`
	// DocumentFieldsSkipped counts declared or retained top-level fields skipped by projection.
	DocumentFieldsSkipped uint64 `json:"document_fields_skipped,omitempty"`
	// DocumentFetchNanos attributes end-to-end post-top-k document fetch/materialization time.
	DocumentFetchNanos uint64 `json:"document_fetch_nanos,omitempty"`
	// DocumentRetainedFetches counts primary retained-payload fetches for document materialization.
	DocumentRetainedFetches uint64 `json:"document_retained_fetches,omitempty"`
	// DocumentRetainedBytes counts retained-payload bytes read for document materialization.
	DocumentRetainedBytes uint64 `json:"document_retained_bytes,omitempty"`
	// DocumentVisibilityScans counts batched typed-row visibility scans used by materialization.
	DocumentVisibilityScans uint64 `json:"document_visibility_scans,omitempty"`
	// DocumentVisibilityRowsScanned counts typed-row asset rows scanned for materialization.
	DocumentVisibilityRowsScanned uint64 `json:"document_visibility_rows_scanned,omitempty"`
	// DocumentVisibilityRows counts visible typed rows found by materialization scans.
	DocumentVisibilityRows uint64 `json:"document_visibility_rows,omitempty"`
	// DocumentVisibilityPhysicalBytes counts typed-row physical bytes scanned for materialization.
	DocumentVisibilityPhysicalBytes uint64 `json:"document_visibility_physical_bytes,omitempty"`
	// DocumentVisibilityNanos attributes typed-row visibility scan time.
	DocumentVisibilityNanos uint64 `json:"document_visibility_nanos,omitempty"`
	// DocumentTypedColumnRows counts materialized rows that consulted typed_column_part storage.
	DocumentTypedColumnRows uint64 `json:"document_typed_column_rows,omitempty"`
	// DocumentTypedColumnCacheHits counts typed-column reconstruction cache hits.
	DocumentTypedColumnCacheHits uint64 `json:"document_typed_column_cache_hits,omitempty"`
	// DocumentTypedColumnCacheMisses counts typed-column reconstruction cache misses.
	DocumentTypedColumnCacheMisses uint64 `json:"document_typed_column_cache_misses,omitempty"`
	// DocumentTypedColumnPartLoads counts typed_column_part asset loads.
	DocumentTypedColumnPartLoads uint64 `json:"document_typed_column_part_loads,omitempty"`
	// DocumentTypedColumnPartDecodes counts typed_column_part decodes.
	DocumentTypedColumnPartDecodes uint64 `json:"document_typed_column_part_decodes,omitempty"`
	// DocumentTypedColumnNanos attributes typed-column value fetch/decode time.
	DocumentTypedColumnNanos uint64 `json:"document_typed_column_nanos,omitempty"`
	// DocumentJSONReconstructionRows counts rows serialized through JSON reconstruction.
	DocumentJSONReconstructionRows uint64 `json:"document_json_reconstruction_rows,omitempty"`
	// DocumentJSONReconstructionNanos attributes JSON merge/serialization time.
	DocumentJSONReconstructionNanos uint64 `json:"document_json_reconstruction_nanos,omitempty"`
	// DocumentRowLocatorBuilds counts snapshot-derived row-locator map builds.
	DocumentRowLocatorBuilds uint64 `json:"document_row_locator_builds,omitempty"`
	// DocumentRowLocatorLookups counts document-id to row-ref locator lookups.
	DocumentRowLocatorLookups uint64 `json:"document_row_locator_lookups,omitempty"`
	// DocumentRowLocatorMisses counts locator lookups with no latest-visible row.
	DocumentRowLocatorMisses uint64 `json:"document_row_locator_misses,omitempty"`
	// DocumentRowLocatorRowsScanned counts physical rows scanned to build locator maps.
	DocumentRowLocatorRowsScanned uint64 `json:"document_row_locator_rows_scanned,omitempty"`
	// DocumentRowLocatorPhysicalBytes counts physical bytes scanned to build locator maps.
	DocumentRowLocatorPhysicalBytes uint64 `json:"document_row_locator_physical_bytes,omitempty"`
	// DocumentRowLocatorNanos attributes row-locator map build time.
	DocumentRowLocatorNanos uint64 `json:"document_row_locator_nanos,omitempty"`
	// DocumentPointRowFetches counts direct row-ref point fetch attempts.
	DocumentPointRowFetches uint64 `json:"document_point_row_fetches,omitempty"`
	// DocumentPointRowDecodes counts physical rows decoded by direct row-ref point fetch.
	DocumentPointRowDecodes uint64 `json:"document_point_row_decodes,omitempty"`
	// DocumentRowRefFallbackScans counts row-ref requests served by an explicit scan fallback.
	DocumentRowRefFallbackScans uint64 `json:"document_row_ref_fallback_scans,omitempty"`
	// DocumentRowRefUnsupported counts unsupported row-ref materialization states.
	DocumentRowRefUnsupported uint64 `json:"document_row_ref_unsupported,omitempty"`
	// DocumentRowRefValidationFailures counts row-ref fail-closed validation failures.
	DocumentRowRefValidationFailures uint64 `json:"document_row_ref_validation_failures,omitempty"`
	// DocumentAssetMmapHits counts document materializer typed-row/typed-column asset reads served from mmap-backed views.
	DocumentAssetMmapHits uint64 `json:"document_asset_mmap_hits,omitempty"`
	// DocumentAssetReadAtFallbacks counts document materializer asset reads that fell back to heap/read-at.
	DocumentAssetReadAtFallbacks uint64 `json:"document_asset_readat_fallbacks,omitempty"`
	// DocumentAssetFileOpens counts materializer asset segment file opens.
	DocumentAssetFileOpens uint64 `json:"document_asset_file_opens,omitempty"`
	// DocumentAssetFileCloses counts materializer asset segment file closes.
	DocumentAssetFileCloses uint64 `json:"document_asset_file_closes,omitempty"`
	// DocumentAssetActiveHandles is the current active mappedresource handle count held by the materializer read view.
	DocumentAssetActiveHandles int64 `json:"document_asset_active_handles,omitempty"`

	// RowFetches is the per-search count of physical row-reader fetch calls.
	RowFetches uint64 `json:"row_fetches,omitempty"`
	// BatchFetches is the per-search count of physical row-reader batch fetch calls.
	BatchFetches uint64 `json:"batch_fetches,omitempty"`
	// RowsFetched is the per-search number of rows returned by physical reader fetches.
	RowsFetched uint64 `json:"rows_fetched,omitempty"`
	// CacheHits is the per-search count of decoded-block cache hits.
	CacheHits uint64 `json:"cache_hits,omitempty"`
	// CacheMisses is the per-search count of decoded-block cache misses.
	CacheMisses uint64 `json:"cache_misses,omitempty"`
	// DecodedBlocks is the per-search count of column blocks decoded after a miss.
	DecodedBlocks uint64 `json:"decoded_blocks,omitempty"`
	// GranulesTouched is the per-search count of physical granules touched.
	GranulesTouched uint64 `json:"granules_touched,omitempty"`
	// PhysicalBytesRead is the per-search physical byte read delta.
	PhysicalBytesRead int64 `json:"physical_bytes_read,omitempty"`
	// MaxResidentBytes is the reader's absolute high-water resident decoded bytes.
	MaxResidentBytes int64 `json:"max_resident_bytes,omitempty"`

	// OpenGranulesRead is the absolute granule count read while opening the bound reader.
	OpenGranulesRead uint64 `json:"open_granules_read,omitempty"`
	// OpenPhysicalBytesRead is the absolute physical byte count read while opening the bound reader.
	OpenPhysicalBytesRead int64 `json:"open_physical_bytes_read,omitempty"`

	// VectorDirectViews is a legacy alias for candidate vectors served from certified mmap direct views.
	VectorDirectViews uint64 `json:"vector_direct_views,omitempty"`
	// VectorMmapDirectViews is the per-search count of candidate vectors served from certified zero-copy mmap direct views.
	VectorMmapDirectViews uint64 `json:"vector_mmap_direct,omitempty"`
	// VectorHeapCopyTypedViews is the per-search count of candidate vectors served from typed views over heap-copy fallback buffers.
	VectorHeapCopyTypedViews uint64 `json:"vector_heap_copy_typed_view,omitempty"`
	// VectorScratchDecodes is the per-search count of candidate vectors served from scratch/fallback decoded vectors.
	VectorScratchDecodes uint64 `json:"vector_scratch_decodes,omitempty"`
	// VectorPreparedDirectViews counts candidate vectors served through the #2040 prepared direct scoring view.
	VectorPreparedDirectViews uint64 `json:"vector_prepared_direct,omitempty"`
	// VectorPreparedIdentityMappings counts prepared vector reads where graph ordinal equals base vector row index.
	VectorPreparedIdentityMappings uint64 `json:"vector_prepared_identity_mapping,omitempty"`
	// VectorPreparedRowRefMappings counts prepared vector reads using an ordinal-to-base-row map.
	VectorPreparedRowRefMappings uint64 `json:"vector_prepared_row_ref_mapping,omitempty"`
	// VectorCertificationFailures counts reason-specific vector source failures that are treated as certification/fail-closed fallbacks.
	VectorCertificationFailures uint64 `json:"vector_certification_failures,omitempty"`
	// VectorAbsoluteOffsetUnaligned counts typed-column vector fallback observations caused by absolute storage offset misalignment.
	VectorAbsoluteOffsetUnaligned uint64 `json:"vector_absolute_offset_unaligned,omitempty"`
	// VectorActualPointerUnaligned counts typed-column vector fallback observations caused by actual Go pointer misalignment.
	VectorActualPointerUnaligned uint64 `json:"vector_actual_pointer_unaligned,omitempty"`
	// VectorStaleHandles counts typed-column vector fallback observations caused by released/stale handles.
	VectorStaleHandles uint64 `json:"vector_stale_handles,omitempty"`
	// AdjacencyDirectViews is a legacy alias for adjacency payloads served from certified mmap direct views.
	AdjacencyDirectViews uint64 `json:"adjacency_direct_views,omitempty"`
	// AdjacencyMmapDirectViews is the per-search count of adjacency payloads served from certified zero-copy mmap direct views.
	AdjacencyMmapDirectViews uint64 `json:"adjacency_mmap_direct,omitempty"`
	// AdjacencyHeapCopyTypedViews is the per-search count of adjacency payloads served from typed heap-copy fallback views.
	AdjacencyHeapCopyTypedViews uint64 `json:"adjacency_heap_copy_typed_view,omitempty"`
	// AdjacencyPreparedCSRDirectViews is the per-search count of HNSW adjacency neighbor slices served from prepared CSR direct views.
	AdjacencyPreparedCSRDirectViews uint64 `json:"adjacency_prepared_csr_direct_views,omitempty"`
	// AdjacencyPreparedCSRMmapDirectViews is the per-search count of HNSW adjacency neighbor slices served from prepared CSR mmap direct views.
	AdjacencyPreparedCSRMmapDirectViews uint64 `json:"adjacency_prepared_csr_mmap_direct,omitempty"`
	// AdjacencyTypedListDirectViews is the per-search count of vector-index state uint32_list adjacency payloads served from generic direct typed-list views.
	AdjacencyTypedListDirectViews uint64 `json:"adjacency_typed_list_direct_views,omitempty"`
	// AdjacencyTypedListMmapDirectViews is the per-search count of vector-index state uint32_list adjacency payloads served from mmap direct views.
	AdjacencyTypedListMmapDirectViews uint64 `json:"adjacency_typed_list_mmap_direct,omitempty"`
	// AdjacencyTypedListHeapCopyTypedViews is the per-search count of vector-index state uint32_list adjacency payloads served from heap-copy typed views.
	AdjacencyTypedListHeapCopyTypedViews uint64 `json:"adjacency_typed_list_heap_copy_typed_view,omitempty"`
	// AdjacencyTypedListScratchDecodes is the per-search count of vector-index state uint32_list adjacency payloads served from decoded fallback views.
	AdjacencyTypedListScratchDecodes uint64 `json:"adjacency_typed_list_scratch_decodes,omitempty"`
	// AdjacencyLegacyFallbacks counts row-image graph adjacency fallback/quarantine reads.
	AdjacencyLegacyFallbacks uint64 `json:"adjacency_legacy_fallbacks,omitempty"`
	// AdjacencySourceUnavailable reports that this searcher had no usable certified adjacency source.
	AdjacencySourceUnavailable uint64 `json:"adjacency_source_unavailable,omitempty"`
	// AdjacencySourceFallbacks reports searches or observations that fell back from certified adjacency sources to legacy rows or fail-closed fallback handling.
	AdjacencySourceFallbacks uint64 `json:"adjacency_source_fallbacks,omitempty"`
	// AdjacencyCertificationFailures counts adjacency source certification, shape, or validation failures.
	AdjacencyCertificationFailures uint64 `json:"adjacency_certification_failures,omitempty"`
	// AdjacencyValidationFailures counts typed-list adjacency validation failures.
	AdjacencyValidationFailures uint64 `json:"adjacency_validation_failures,omitempty"`
	// AdjacencyAbsoluteOffsetUnaligned counts adjacency source fallbacks caused by absolute storage offset misalignment.
	AdjacencyAbsoluteOffsetUnaligned uint64 `json:"adjacency_absolute_offset_unaligned,omitempty"`
	// AdjacencyActualPointerUnaligned counts adjacency source fallbacks caused by actual mapped pointer misalignment.
	AdjacencyActualPointerUnaligned uint64 `json:"adjacency_actual_pointer_unaligned,omitempty"`
	// AdjacencyStaleHandles counts adjacency source fallbacks caused by released/stale mappedresource handles.
	AdjacencyStaleHandles uint64 `json:"adjacency_stale_handles,omitempty"`
	// AdjacencyScratchDecodes is the per-search count of adjacency payloads served from scratch/fallback decodes.
	AdjacencyScratchDecodes uint64 `json:"adjacency_scratch_decodes,omitempty"`
	// NormDirectViews is a legacy alias for inverse norms served from certified mmap direct views.
	NormDirectViews uint64 `json:"norm_direct_views,omitempty"`
	// NormMmapDirectViews is the per-search count of inverse norms served from certified zero-copy mmap direct views.
	NormMmapDirectViews uint64 `json:"norm_mmap_direct,omitempty"`
	// NormHeapCopyTypedViews is the per-search count of inverse norms served from typed heap-copy fallback views.
	NormHeapCopyTypedViews uint64 `json:"norm_heap_copy_typed_view,omitempty"`
	// NormScratchDecodes is the per-search count of inverse norms served from scratch/fallback decoded state.
	NormScratchDecodes uint64 `json:"norm_scratch_decodes,omitempty"`
	// NormPreparedDirectViews counts inverse norms served through the #2040 prepared direct scoring view.
	NormPreparedDirectViews uint64 `json:"norm_prepared_direct,omitempty"`
	// NormSourceUnavailable reports that this searcher had no usable inverse-norm state source and used graph-row fallback.
	NormSourceUnavailable uint64 `json:"norm_source_unavailable,omitempty"`
	// NormSourceFallbacks reports searches or observations that fell back from inverse-norm state to legacy rows or fail-closed fallback handling.
	NormSourceFallbacks uint64 `json:"norm_source_fallbacks,omitempty"`
	// NormValidationFailures counts inverse-norm state source certification, shape, or validation failures.
	NormValidationFailures uint64 `json:"norm_validation_failures,omitempty"`
	// NormAbsoluteOffsetUnaligned counts inverse-norm state fallbacks caused by absolute storage offset misalignment.
	NormAbsoluteOffsetUnaligned uint64 `json:"norm_absolute_offset_unaligned,omitempty"`
	// NormActualPointerUnaligned counts inverse-norm state fallbacks caused by actual mapped pointer misalignment.
	NormActualPointerUnaligned uint64 `json:"norm_actual_pointer_unaligned,omitempty"`
	// NormStaleHandles counts inverse-norm state fallbacks caused by released/stale mappedresource handles.
	NormStaleHandles uint64 `json:"norm_stale_handles,omitempty"`
	// NormMappedBytes is the mapped-resource mapped byte total backing the bound inverse-norm state source.
	NormMappedBytes uint64 `json:"norm_mapped_bytes,omitempty"`
	// NormHeapCopyBytes is the mapped-resource heap-copy byte total backing the bound inverse-norm state source.
	NormHeapCopyBytes uint64 `json:"norm_heap_copy_bytes,omitempty"`
	// NormDecodedBytes is decoded fallback inverse-norm state bytes held by the bound source.
	NormDecodedBytes uint64 `json:"norm_decoded_bytes,omitempty"`
	// NormActiveHandles is the current active mappedresource handle count for inverse-norm state views.
	NormActiveHandles int64 `json:"norm_active_handles,omitempty"`
	// NormDeniedResources is the total denied mappedresource acquisition count for the inverse-norm state source.
	NormDeniedResources uint64 `json:"norm_denied_resources,omitempty"`
	// TypedColumnMappedBytes is the typed-column mapped-resource mapped byte total backing the bound vector source.
	TypedColumnMappedBytes uint64 `json:"typed_column_mapped_bytes,omitempty"`
	// TypedColumnHeapCopyBytes is the typed-column mapped-resource heap-copy byte total backing the bound vector source.
	TypedColumnHeapCopyBytes uint64 `json:"typed_column_heap_copy_bytes,omitempty"`
	// TypedColumnDecodedBytes is decoded/derived typed-column metadata or fallback vector bytes for the bound vector source.
	TypedColumnDecodedBytes uint64 `json:"typed_column_decoded_bytes,omitempty"`
	// TypedColumnActiveHandles is the current active mappedresource handle count for direct typed-column vector views.
	TypedColumnActiveHandles int64 `json:"typed_column_active_handles,omitempty"`
	// TypedColumnDeniedResources is the total denied mappedresource acquisition count for the typed-column vector source.
	TypedColumnDeniedResources uint64 `json:"typed_column_denied_resources,omitempty"`
	// TypedColumnFallbacks reports that typed-column vector ownership was selected but the reader fell back to graph row vectors.
	TypedColumnFallbacks uint64 `json:"typed_column_fallbacks,omitempty"`
	// RowRefVectorSourceState reports searches whose typed-column vector locator map came from vector-index row-ref state.
	RowRefVectorSourceState uint64 `json:"row_ref_vector_source_state,omitempty"`
	// RowRefVectorSourceLegacyGraphIDs reports searches whose typed-column vector locator map used legacy graph row ID scans.
	RowRefVectorSourceLegacyGraphIDs uint64 `json:"row_ref_vector_source_legacy_graph_ids,omitempty"`
	// RowRefStatePreparedViews reports searches with an open-time certified prepared row-ref view bound to the searcher.
	RowRefStatePreparedViews uint64 `json:"row_ref_state_prepared_views,omitempty"`
	// RowRefStateMmapDirectFields counts row-ref coordinate fields admitted as mmap-direct prepared int64 views.
	RowRefStateMmapDirectFields uint64 `json:"row_ref_state_mmap_direct_fields,omitempty"`
	// RowRefStateResultRefs counts top-k result row refs served from vector-index row-ref state.
	RowRefStateResultRefs uint64 `json:"row_ref_state_result_refs,omitempty"`
	// RowRefStateSourceUnavailable reports that row-ref state was absent and compatibility source selection was used.
	RowRefStateSourceUnavailable uint64 `json:"row_ref_state_source_unavailable,omitempty"`
	// RowRefStateSourceFallbacks reports row-ref source fallback/quarantine observations.
	RowRefStateSourceFallbacks uint64 `json:"row_ref_state_source_fallbacks,omitempty"`
	// ResultIDPreparedBytesViews reports searches with an open-time certified prepared document-ID bytes view bound to the searcher.
	ResultIDPreparedBytesViews uint64 `json:"result_id_prepared_bytes_views,omitempty"`
	// ResultIDTypedBytesState counts returned document IDs copied from vector-index typed-column bytes state.
	ResultIDTypedBytesState uint64 `json:"result_id_typed_bytes_state,omitempty"`
	// ResultIDGraphFallbacks counts returned document IDs copied from legacy graph row ID bytes.
	ResultIDGraphFallbacks uint64 `json:"result_id_graph_fallbacks,omitempty"`
	// ResultIDStateValidationFailures counts searches that fell back because present document-ID state failed validation.
	ResultIDStateValidationFailures uint64 `json:"result_id_state_validation_failures,omitempty"`
	// PreparedGraphSearchViews reports searches routed through the combined open-time prepared typed-column graph-search view.
	PreparedGraphSearchViews uint64 `json:"prepared_graph_search_views,omitempty"`
	// GraphRowFallbacks aggregates compatibility graph-row reads/fallbacks observed during vector/norm/adjacency/result materialization.
	GraphRowFallbacks uint64 `json:"graph_row_fallbacks,omitempty"`

	// BenchmarkDebugSearches reports searches collected with benchmark_debug graph-control-flow instrumentation.
	BenchmarkDebugSearches uint64 `json:"benchmark_debug_searches,omitempty"`
	// NeighborTiles counts expanded adjacency neighbor tiles.
	NeighborTiles uint64 `json:"neighbor_tiles,omitempty"`
	// NeighborTileNeighbors sums neighbor counts across expanded adjacency tiles.
	NeighborTileNeighbors uint64 `json:"neighbor_tile_neighbors,omitempty"`
	// NeighborTileMaxSize is the largest expanded adjacency tile.
	NeighborTileMaxSize    uint64 `json:"neighbor_tile_max_size,omitempty"`
	NeighborTileSize0      uint64 `json:"neighbor_tile_size_0,omitempty"`
	NeighborTileSize1      uint64 `json:"neighbor_tile_size_1,omitempty"`
	NeighborTileSize2To4   uint64 `json:"neighbor_tile_size_2_4,omitempty"`
	NeighborTileSize5To8   uint64 `json:"neighbor_tile_size_5_8,omitempty"`
	NeighborTileSize9To16  uint64 `json:"neighbor_tile_size_9_16,omitempty"`
	NeighborTileSize17Plus uint64 `json:"neighbor_tile_size_17_plus,omitempty"`
	// ScoreBatchSingletons counts singleton scoring batches in benchmark_debug mode.
	ScoreBatchSingletons uint64 `json:"score_batch_singletons,omitempty"`
	ScoreBatchSize2To4   uint64 `json:"score_batch_size_2_4,omitempty"`
	ScoreBatchSize5To8   uint64 `json:"score_batch_size_5_8,omitempty"`
	ScoreBatchSize9To16  uint64 `json:"score_batch_size_9_16,omitempty"`
	ScoreBatchSize17Plus uint64 `json:"score_batch_size_17_plus,omitempty"`
	// ScoredNeighbors counts graph neighbors that reached scoring; skipped neighbors partition filter and already-visited skips.
	ScoredNeighbors                       uint64 `json:"scored_neighbors,omitempty"`
	SkippedNeighbors                      uint64 `json:"skipped_neighbors,omitempty"`
	AlreadyVisitedSkips                   uint64 `json:"already_visited_skips,omitempty"`
	FilterSkips                           uint64 `json:"filter_skips,omitempty"`
	UpperLayerScores                      uint64 `json:"upper_layer_scores,omitempty"`
	UpperLayerEntryScores                 uint64 `json:"upper_layer_entry_scores,omitempty"`
	UpperLayerNeighborScores              uint64 `json:"upper_layer_neighbor_scores,omitempty"`
	UpperLayerEdgeVisits                  uint64 `json:"upper_layer_edge_visits,omitempty"`
	UpperLayerScoredNeighbors             uint64 `json:"upper_layer_scored_neighbors,omitempty"`
	UpperLayerFilterSkips                 uint64 `json:"upper_layer_filter_skips,omitempty"`
	Layer0Scores                          uint64 `json:"layer0_scores,omitempty"`
	Layer0SeedScores                      uint64 `json:"layer0_seed_scores,omitempty"`
	Layer0NeighborScores                  uint64 `json:"layer0_neighbor_scores,omitempty"`
	Layer0EdgeVisits                      uint64 `json:"layer0_edge_visits,omitempty"`
	Layer0ScoredNeighbors                 uint64 `json:"layer0_scored_neighbors,omitempty"`
	Layer0AlreadyVisitedSkips             uint64 `json:"layer0_already_visited_skips,omitempty"`
	Layer0FilterSkips                     uint64 `json:"layer0_filter_skips,omitempty"`
	FrontierPushes                        uint64 `json:"frontier_pushes,omitempty"`
	FrontierPops                          uint64 `json:"frontier_pops,omitempty"`
	FrontierPopMisses                     uint64 `json:"frontier_pop_misses,omitempty"`
	FrontierSiftUpSteps                   uint64 `json:"frontier_sift_up_steps,omitempty"`
	FrontierSiftDownSteps                 uint64 `json:"frontier_sift_down_steps,omitempty"`
	TopKInsertAttempts                    uint64 `json:"top_k_insert_attempts,omitempty"`
	TopKInsertSuccesses                   uint64 `json:"top_k_insert_successes,omitempty"`
	TopKInsertRejections                  uint64 `json:"top_k_insert_rejections,omitempty"`
	TopKShiftSteps                        uint64 `json:"top_k_shift_steps,omitempty"`
	VisitedMarkChecks                     uint64 `json:"visited_mark_checks,omitempty"`
	VisitedMarkHits                       uint64 `json:"visited_mark_hits,omitempty"`
	VisitedMarkMisses                     uint64 `json:"visited_mark_misses,omitempty"`
	ExactModeSearches                     uint64 `json:"exact_mode_searches,omitempty"`
	ExactCandidateOrderObservations       uint64 `json:"exact_candidate_order_observations,omitempty"`
	ExactCandidateOrderTransitions        uint64 `json:"exact_candidate_order_transitions,omitempty"`
	ExactCandidateOrderAdjacentForward    uint64 `json:"exact_candidate_order_adjacent_forward,omitempty"`
	ExactCandidateOrderNonAdjacentForward uint64 `json:"exact_candidate_order_non_adjacent_forward,omitempty"`
	ExactCandidateOrderBackwardJumps      uint64 `json:"exact_candidate_order_backward_jumps,omitempty"`
	ExactCandidateOrderMaxForwardRun      uint64 `json:"exact_candidate_order_max_forward_run,omitempty"`
	// DocumentRowRefStateFetches counts post-top-k document fetches served with vector-index row-ref state.
	DocumentRowRefStateFetches uint64 `json:"document_row_ref_state_fetches,omitempty"`
	// DocumentRowRefLookupFallbacks counts post-top-k document fetches that fell back to ID-to-row-ref lookup.
	DocumentRowRefLookupFallbacks uint64 `json:"document_row_ref_lookup_fallbacks,omitempty"`
}

// VectorIndexSearchResponse is returned by public vector-index search APIs.
type VectorIndexSearchResponse struct {
	// IndexName is the searched collection vector-index name.
	IndexName string `json:"index_name"`
	// Strategy is the declared vector-index strategy.
	Strategy VectorIndexStrategy `json:"strategy"`
	// Path is the implementation path actually used for the search, when one ran.
	Path VectorIndexSearchPath `json:"path,omitempty"`
	// Status describes whether the index was loaded, unavailable, or stale.
	Status VectorIndexStatus `json:"status"`
	// Stats contains search and reader telemetry.
	Stats VectorIndexSearchStats `json:"stats,omitempty"`
	// Results contains top-k hits in descending score order. Search and
	// SearchVectorIndex return response-owned slices; SearchWithBuffer returns a
	// slice owned by the caller's VectorIndexSearchBuffer.
	Results []VectorIndexSearchResult `json:"results,omitempty"`
}

// VectorIndexSearchBuffer is caller-owned reusable response storage for
// VectorIndexSearcher.SearchWithBuffer. It is intended for steady-state
// no-document searches that need to avoid per-call response allocation.
//
// A VectorIndexSearchBuffer is not safe for concurrent use. Do not reuse or
// reset the same buffer while any caller still needs a response previously
// returned from it. The response Results slice and each result ID returned by
// SearchWithBuffer alias this buffer and remain valid only until the same
// buffer is reused or Reset is called. Parallel callers should use independent
// searcher/buffer pairs per worker.
type VectorIndexSearchBuffer struct {
	results []VectorIndexSearchResult
	idBytes []byte
}

// Reset clears the buffer's current response view while retaining reusable
// capacity. Any response previously returned by SearchWithBuffer with this
// buffer must be considered invalid after Reset returns.
func (b *VectorIndexSearchBuffer) Reset() {
	if b == nil {
		return
	}
	b.results = b.results[:0]
	b.idBytes = b.idBytes[:0]
}

// VectorIndexSearcher is a reusable, snapshot-bound vector index search handle.
// It is not concurrency-safe; parallel query workers should open independent
// searchers. Close and reopen the searcher after writes/rebuilds when callers
// need the newest column_graph generation.
type VectorIndexSearcher struct {
	collection   *Collection
	indexName    string
	strategy     VectorIndexStrategy
	path         VectorIndexSearchPath
	status       VectorIndexStatus
	snapshot     *backenddb.Snapshot
	catalog      *collectionCatalog
	reader       *columnVectorGraphPhysicalRowReader
	documentView *CollectionReadView
	scratch      columnVectorGraphNativeSearchScratch
	readerLast   columnPhysicalRowReaderStats
	closed       bool
}

// SearchVectorIndex searches a collection vector index through the public
// collection lifecycle. V4 wires only explicit column_graph indexes to the
// native physical column reader; native_runtime remains reported as native
// rather than silently falling back or pretending to use column storage. When
// availability or staleness checks fail, the returned response may still carry
// the index status so callers can distinguish rebuild-needed/unavailable cases.
func (c *Collection) SearchVectorIndex(opts VectorIndexSearchOptions) (VectorIndexSearchResponse, error) {
	if err := validateVectorIndexSearchRequest(opts.TopK, opts.EfSearch); err != nil {
		return VectorIndexSearchResponse{}, err
	}
	searcher, response, err := c.openVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        opts.IndexName,
		MaxDecodedBlocks: opts.MaxDecodedBlocks,
	})
	if err != nil {
		return response, err
	}
	response, err = searcher.Search(VectorIndexSearcherSearchOptions{
		Query:                opts.Query,
		TopK:                 opts.TopK,
		EfSearch:             opts.EfSearch,
		IncludeDocuments:     opts.IncludeDocuments,
		DocumentFetchOptions: opts.DocumentFetchOptions,
		StatsMode:            opts.StatsMode,
		scoreBatchMode:       opts.scoreBatchMode,
	})
	documentView := searcher.documentView
	if closeErr := searcher.Close(); err == nil && closeErr != nil {
		return response, closeErr
	}
	if documentView != nil {
		counters := documentView.assetCounters()
		response.Stats.DocumentAssetFileCloses = counters.fileCloses
		response.Stats.DocumentAssetActiveHandles = counters.activeHandles
	}
	return response, err
}

// OpenVectorIndexSearcher opens a reusable search handle for steady-state
// vector queries. Setup/open/decode cost is paid at open; Search then measures
// graph traversal, vector scoring, top-k production, and optional post-top-k
// document fetch.
func (c *Collection) OpenVectorIndexSearcher(opts VectorIndexSearcherOptions) (*VectorIndexSearcher, error) {
	searcher, _, err := c.openVectorIndexSearcher(opts)
	return searcher, err
}

func (c *Collection) openVectorIndexSearcher(opts VectorIndexSearcherOptions) (*VectorIndexSearcher, VectorIndexSearchResponse, error) {
	var response VectorIndexSearchResponse
	if err := ValidateIndexName(opts.IndexName); err != nil {
		return nil, response, err
	}
	if opts.MaxDecodedBlocks < 0 {
		return nil, response, errors.New("collections: vector index search max_decoded_blocks cannot be negative")
	}
	if c == nil {
		return nil, response, errCollectionNil
	}
	if c.db == nil {
		return nil, response, errCollectionDBNil
	}
	if err := c.flushBufferedWrites(); err != nil {
		return nil, response, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, response, backenddb.ErrClosed
	}
	closeOnErr := true
	defer func() {
		if closeOnErr {
			_ = snap.Close()
		}
	}()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return nil, response, err
	}
	if catalog == nil {
		return nil, response, errCollectionNotFound
	}
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, opts.IndexName)
	if !ok {
		return nil, response, ErrIndexNotFound
	}
	response.IndexName = def.Name
	response.Strategy = def.Strategy
	switch def.Strategy {
	case VectorIndexStrategyNativeRuntime:
		response.Status = VectorIndexStatus{
			Name:     def.Name,
			Strategy: def.Strategy,
			State:    VectorIndexStateNativeRuntime,
			Reason:   VectorIndexReasonNativeRuntime,
		}
		return nil, response, fmt.Errorf("%w: vector index %q uses native_runtime; public native-reader search currently requires an explicit column_graph index", ErrVectorIndexSearchUnavailable, def.Name)
	case VectorIndexStrategyColumnGraph:
	default:
		response.Status = VectorIndexStatus{
			Name:     def.Name,
			Strategy: def.Strategy,
			State:    VectorIndexStateColumnGraphUnavailable,
			Reason:   VectorIndexReasonUnsupportedStrategy,
		}
		return nil, response, fmt.Errorf("%w: vector index %q uses unsupported strategy %q", ErrVectorIndexSearchUnavailable, def.Name, def.Strategy)
	}
	if def.Metric != VectorMetricCosine {
		response.Status = VectorIndexStatus{
			Name:     def.Name,
			Strategy: def.Strategy,
			State:    VectorIndexStateColumnGraphUnavailable,
			Reason:   VectorIndexReasonColumnGraphUnsupportedMetric,
		}
		return nil, response, fmt.Errorf("%w: column_graph vector index %q uses metric %q; native reader currently supports only %q", ErrVectorIndexSearchUnavailable, def.Name, def.Metric, VectorMetricCosine)
	}
	reader, err := c.openColumnVectorGraphPhysicalRowReaderAtSnapshot(def.Name, snap, columnVectorGraphPhysicalRowReaderOptions{
		MaxDecodedBlocks: opts.MaxDecodedBlocks,
	})
	if err != nil {
		status, statusErr := c.columnGraphVectorIndexStatusAtSnapshot(def.Name, snap)
		if statusErr != nil {
			return nil, response, statusErr
		}
		status = failClosedColumnGraphReaderOpenStatus(def, status)
		response.Status = status
		return nil, response, fmt.Errorf("%w: column_graph %q is not loaded: state=%s reason=%s: %w", ErrVectorIndexSearchUnavailable, def.Name, status.State, status.Reason, err)
	}
	readerCatalog := reader.catalog
	if readerCatalog == nil {
		_ = reader.Close()
		return nil, response, errors.New("collections: column_graph physical row reader missing snapshot catalog")
	}

	response.Status = VectorIndexStatus{
		Name:     reader.def.Name,
		Strategy: reader.def.Strategy,
		State:    VectorIndexStateColumnGraphLoaded,
		Loaded:   true,
	}
	response.Path = VectorIndexSearchPathColumnGraphNativeReader
	searcher := &VectorIndexSearcher{
		collection: c,
		indexName:  response.IndexName,
		strategy:   response.Strategy,
		path:       response.Path,
		status:     response.Status,
		snapshot:   snap,
		catalog:    readerCatalog,
		reader:     reader,
		readerLast: reader.Stats(),
	}
	closeOnErr = false
	return searcher, response, nil
}

func failClosedColumnGraphReaderOpenStatus(def VectorIndexDefinition, status VectorIndexStatus) VectorIndexStatus {
	if status.Name == "" {
		status.Name = def.Name
	}
	if status.Strategy == "" {
		status.Strategy = def.Strategy
	}
	if status.State == VectorIndexStateColumnGraphLoaded || status.Loaded {
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphAssetMismatch
		status.Loaded = false
		status.RebuildNeeded = true
	}
	return status
}

// Search runs one vector-index query against the searcher's bound snapshot.
// Returned result IDs and documents are copied into response-owned buffers.
func (s *VectorIndexSearcher) Search(opts VectorIndexSearcherSearchOptions) (VectorIndexSearchResponse, error) {
	var response VectorIndexSearchResponse
	if s == nil || s.reader == nil || s.collection == nil {
		return response, errors.New("collections: nil vector index searcher")
	}
	response.IndexName = s.indexName
	response.Strategy = s.strategy
	response.Path = s.path
	response.Status = s.status
	if s.closed {
		return response, errors.New("collections: vector index searcher is closed")
	}
	if err := validateVectorIndexSearchRequest(opts.TopK, opts.EfSearch); err != nil {
		return response, err
	}
	if !opts.IncludeDocuments && documentFetchOptionsHasProjection(opts.DocumentFetchOptions) {
		return response, errors.New("collections: vector index document projection requires IncludeDocuments")
	}
	statsMode, err := columnVectorGraphNativeSearchStatsModeFromPublic(opts.StatsMode)
	if err != nil {
		return response, err
	}
	readerStatsBefore := s.readerLast
	results, searchStats, err := s.reader.SearchCosine(opts.Query, columnVectorGraphNativeSearchOptions{
		TopK:           opts.TopK,
		EfSearch:       opts.EfSearch,
		ScoreBatchMode: opts.scoreBatchMode,
		StatsMode:      statsMode,
	}, &s.scratch)
	readerStatsAfter := s.reader.Stats()
	s.readerLast = readerStatsAfter
	readerStats := columnPhysicalRowReaderStatsDelta(readerStatsBefore, readerStatsAfter)
	response.Stats = vectorIndexSearchStatsFromInternal(searchStats, readerStats)
	if err != nil {
		return response, err
	}
	if len(results) == 0 {
		return response, nil
	}
	response.Results = make([]VectorIndexSearchResult, len(results))
	idByteCount, err := vectorIndexSearchResultIDBytes(results)
	if err != nil {
		return response, err
	}
	idBytes := make([]byte, idByteCount)
	idOffset := 0
	for i, result := range results {
		if len(result.ID) > len(idBytes)-idOffset {
			return response, errors.New("collections: vector index search result id byte accounting mismatch")
		}
		nextIDOffset := idOffset + len(result.ID)
		id := idBytes[idOffset:nextIDOffset:nextIDOffset]
		idOffset = nextIDOffset
		copy(id, result.ID)
		response.Results[i] = VectorIndexSearchResult{
			ID:      id,
			Ordinal: result.Ordinal,
			Score:   result.Score,
		}
	}
	if opts.IncludeDocuments {
		if s.documentView == nil {
			s.documentView = newCollectionReadViewAtSnapshot(s.collection, s.snapshot, s.catalog, false, mappedresource.ScopePreparedSearch)
		}
		ids := make([][]byte, len(results))
		for i := range results {
			ids[i] = results[i].ID
		}
		documentFetchOptions := opts.DocumentFetchOptions
		var documents DocumentFetchResponse
		var err error
		if columnStoreCanReconstructDocument(s.catalog.meta) {
			refs := make([]DocumentRowRef, len(results))
			useResultRowRefs := true
			for i := range results {
				if !results[i].HasRowRef {
					useResultRowRefs = false
					break
				}
				ref := results[i].RowRef
				ref.DocumentID = results[i].ID
				refs[i] = ref
			}
			if useResultRowRefs {
				response.Stats.DocumentRowRefStateFetches += uint64(len(refs))
			} else {
				response.Stats.DocumentRowRefLookupFallbacks++
				rowRefs, lookupErr := s.documentView.LookupDocumentRowRefsByID(ids, DocumentFetchOptions{ColumnAssetReadIntegrity: documentFetchOptions.ColumnAssetReadIntegrity})
				if lookupErr != nil {
					return response, lookupErr
				}
				if err := addDocumentMaterializationStatsToVectorStats(&response.Stats, rowRefs.Stats); err != nil {
					return response, err
				}
				if len(rowRefs.Results) != len(response.Results) {
					return response, errors.New("collections: vector index document row-ref result count mismatch")
				}
				for i := range rowRefs.Results {
					if !rowRefs.Results[i].Found {
						return response, fmt.Errorf("collections: vector index %q result document %q not found", s.indexName, results[i].ID)
					}
					refs[i] = rowRefs.Results[i].RowRef
				}
			}
			documents, err = s.documentView.FetchDocumentsByRowRef(refs, documentFetchOptions)
			if err != nil {
				return response, err
			}
		} else {
			response.Stats.DocumentRowRefUnsupported++
			documents, err = s.documentView.FetchDocumentsByID(ids, documentFetchOptions)
			if err != nil {
				return response, err
			}
		}
		if len(documents.Results) != len(response.Results) {
			return response, errors.New("collections: vector index document materializer result count mismatch")
		}
		for i := range documents.Results {
			if !documents.Results[i].Found {
				return response, fmt.Errorf("collections: vector index %q result document %q not found", s.indexName, results[i].ID)
			}
			response.Results[i].Document = documents.Results[i].Document
		}
		if err := addDocumentMaterializationStatsToVectorStats(&response.Stats, documents.Stats); err != nil {
			return response, err
		}
	}
	return response, nil
}

// SearchWithBuffer runs one no-document vector-index query against the
// searcher's bound snapshot using caller-owned reusable response storage.
// Returned result slices and result IDs alias buffer and are valid only until
// buffer is reused or Reset is called. The same buffer must not be reused
// concurrently; parallel callers should use independent searcher/buffer pairs
// per worker. IncludeDocuments is not supported by this reusable no-document
// path.
//
// On any error after a non-nil buffer is supplied, the buffer's reusable
// result/id views are reset to length zero and the returned response has no
// Results. Search metadata and any telemetry collected before the error may
// still be present in the returned response.
func (s *VectorIndexSearcher) SearchWithBuffer(opts VectorIndexSearcherSearchOptions, buffer *VectorIndexSearchBuffer) (VectorIndexSearchResponse, error) {
	if buffer == nil {
		return VectorIndexSearchResponse{}, errors.New("collections: nil vector index search buffer")
	}
	buffer.Reset()
	var response VectorIndexSearchResponse
	if s == nil || s.reader == nil || s.collection == nil {
		return response, errors.New("collections: nil vector index searcher")
	}
	response.IndexName = s.indexName
	response.Strategy = s.strategy
	response.Path = s.path
	response.Status = s.status
	if s.closed {
		return response, errors.New("collections: vector index searcher is closed")
	}
	if err := validateVectorIndexSearchRequest(opts.TopK, opts.EfSearch); err != nil {
		return response, err
	}
	if opts.IncludeDocuments {
		return response, errors.New("collections: vector index SearchWithBuffer does not support IncludeDocuments")
	}
	if documentFetchOptionsHasProjection(opts.DocumentFetchOptions) {
		return response, errors.New("collections: vector index document projection requires IncludeDocuments")
	}
	statsMode, err := columnVectorGraphNativeSearchStatsModeFromPublic(opts.StatsMode)
	if err != nil {
		return response, err
	}
	readerStatsBefore := s.readerLast
	results, searchStats, err := s.reader.SearchCosine(opts.Query, columnVectorGraphNativeSearchOptions{
		TopK:           opts.TopK,
		EfSearch:       opts.EfSearch,
		ScoreBatchMode: opts.scoreBatchMode,
		StatsMode:      statsMode,
	}, &s.scratch)
	readerStatsAfter := s.reader.Stats()
	s.readerLast = readerStatsAfter
	readerStats := columnPhysicalRowReaderStatsDelta(readerStatsBefore, readerStatsAfter)
	response.Stats = vectorIndexSearchStatsFromInternal(searchStats, readerStats)
	if err != nil {
		return response, err
	}
	if len(results) == 0 {
		return response, nil
	}
	idByteCount, err := vectorIndexSearchResultIDBytes(results)
	if err != nil {
		return response, err
	}
	buffer.results = resizeVectorIndexSearchResultBuffer(buffer.results, len(results))
	buffer.idBytes = resizeVectorIndexSearchByteBuffer(buffer.idBytes, idByteCount)
	idOffset := 0
	for i, result := range results {
		if len(result.ID) > len(buffer.idBytes)-idOffset {
			buffer.Reset()
			return response, errors.New("collections: vector index search result id byte accounting mismatch")
		}
		nextIDOffset := idOffset + len(result.ID)
		id := buffer.idBytes[idOffset:nextIDOffset:nextIDOffset]
		idOffset = nextIDOffset
		copy(id, result.ID)
		buffer.results[i] = VectorIndexSearchResult{
			ID:      id,
			Ordinal: result.Ordinal,
			Score:   result.Score,
		}
	}
	response.Results = buffer.results
	return response, nil
}

func resizeVectorIndexSearchResultBuffer(dst []VectorIndexSearchResult, target int) []VectorIndexSearchResult {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]VectorIndexSearchResult, target)
	}
	return dst[:target]
}

func resizeVectorIndexSearchByteBuffer(dst []byte, target int) []byte {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]byte, target)
	}
	return dst[:target]
}

func validateVectorIndexSearchRequest(topK, efSearch int) error {
	if topK < 0 {
		return errors.New("collections: vector index search top_k cannot be negative")
	}
	if efSearch < 0 {
		return errors.New("collections: vector index search ef_search cannot be negative")
	}
	return nil
}

func vectorIndexSearchResultIDBytes(results []columnVectorGraphNativeSearchResult) (int, error) {
	return vectorIndexSearchResultIDBytesLimit(results, math.MaxInt)
}

func vectorIndexSearchResultIDBytesLimit(results []columnVectorGraphNativeSearchResult, limit int) (int, error) {
	var total int
	for _, result := range results {
		next, err := addVectorIndexSearchByteTotal(total, len(result.ID), limit, "result id")
		if err != nil {
			return 0, err
		}
		total = next
	}
	return total, nil
}

func addVectorIndexSearchByteTotal(total, n, limit int, label string) (int, error) {
	if n < 0 || total < 0 || limit < 0 || n > limit-total {
		return 0, fmt.Errorf("collections: vector index search %s bytes overflow", label)
	}
	return total + n, nil
}

func multiplyVectorIndexSearchByteTotal(n, count, limit int, label string) (int, error) {
	if n < 0 || count < 0 || limit < 0 {
		return 0, fmt.Errorf("collections: vector index search %s bytes overflow", label)
	}
	if count != 0 && n > limit/count {
		return 0, fmt.Errorf("collections: vector index search %s bytes overflow", label)
	}
	return n * count, nil
}

// Close releases the searcher's bound physical reader and snapshot.
func (s *VectorIndexSearcher) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	var closeErr error
	if s.documentView != nil {
		if err := s.documentView.Close(); err != nil {
			closeErr = err
		}
		s.documentView = nil
	}
	if s.reader == nil {
		if s.snapshot != nil {
			if err := s.snapshot.Close(); closeErr == nil && err != nil {
				closeErr = err
			}
			s.snapshot = nil
		}
		return closeErr
	}
	if err := s.reader.Close(); closeErr == nil && err != nil {
		closeErr = err
	}
	if s.snapshot != nil {
		if err := s.snapshot.Close(); closeErr == nil && err != nil {
			closeErr = err
		}
		s.snapshot = nil
	}
	return closeErr
}

// columnPhysicalRowReaderStatsDelta reports per-search deltas for mutable
// fetch/read counters while carrying open-time and resident-byte telemetry as
// absolute values from the bound reader.
func columnPhysicalRowReaderStatsDelta(before, after columnPhysicalRowReaderStats) columnPhysicalRowReaderStats {
	return columnPhysicalRowReaderStats{
		Rows:                  after.Rows,
		Granules:              after.Granules,
		OpenGranulesRead:      after.OpenGranulesRead,
		OpenPhysicalBytesRead: after.OpenPhysicalBytesRead,
		RowFetches:            deltaUint64(before.RowFetches, after.RowFetches),
		BatchFetches:          deltaUint64(before.BatchFetches, after.BatchFetches),
		RowsFetched:           deltaUint64(before.RowsFetched, after.RowsFetched),
		CacheHits:             deltaUint64(before.CacheHits, after.CacheHits),
		CacheMisses:           deltaUint64(before.CacheMisses, after.CacheMisses),
		DecodedBlocks:         deltaUint64(before.DecodedBlocks, after.DecodedBlocks),
		GranulesTouched:       deltaUint64(before.GranulesTouched, after.GranulesTouched),
		PhysicalBytesRead:     deltaInt64(before.PhysicalBytesRead, after.PhysicalBytesRead),
		MaxResidentBytes:      after.MaxResidentBytes,
	}
}

func deltaUint64(before, after uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func deltaInt64(before, after int64) int64 {
	if after < before {
		return 0
	}
	return after - before
}

func vectorIndexSearchStatsFromInternal(searchStats columnVectorGraphNativeSearchStats, readerStats columnPhysicalRowReaderStats) VectorIndexSearchStats {
	return VectorIndexSearchStats{
		GraphRows:                             uint64(readerStats.Rows),
		CandidateRows:                         searchStats.CandidateRows,
		Candidates:                            searchStats.Candidates,
		Edges:                                 searchStats.Edges,
		VisitedNodes:                          searchStats.VisitedNodes,
		VisitedEdges:                          searchStats.VisitedEdges,
		VectorBytesRead:                       searchStats.VectorBytesRead,
		NormBytesRead:                         searchStats.NormBytesRead,
		AdjacencyBytesRead:                    searchStats.AdjacencyBytesRead,
		CandidateFetches:                      searchStats.CandidateFetches,
		ScoreBatchCalls:                       searchStats.ScoreBatchCalls,
		ScoreBatchCandidates:                  searchStats.ScoreBatchCandidates,
		ScoreBatchMaxTileSize:                 searchStats.ScoreBatchMaxTileSize,
		ScoreBatchOptimizedCalls:              searchStats.ScoreBatchOptimizedCalls,
		ScoreBatchScalarFallbackCalls:         searchStats.ScoreBatchScalarFallbackCalls,
		PreparedScoreCalls:                    searchStats.PreparedScoreCalls,
		ScoreFloat64Fallbacks:                 searchStats.ScoreFloat64Fallbacks,
		ExpansionFetches:                      searchStats.ExpansionFetches,
		ResultFetches:                         searchStats.ResultFetches,
		RowFetches:                            readerStats.RowFetches,
		BatchFetches:                          readerStats.BatchFetches,
		RowsFetched:                           readerStats.RowsFetched,
		CacheHits:                             readerStats.CacheHits,
		CacheMisses:                           readerStats.CacheMisses,
		DecodedBlocks:                         readerStats.DecodedBlocks,
		GranulesTouched:                       readerStats.GranulesTouched,
		PhysicalBytesRead:                     readerStats.PhysicalBytesRead,
		MaxResidentBytes:                      readerStats.MaxResidentBytes,
		OpenGranulesRead:                      uint64(readerStats.OpenGranulesRead),
		OpenPhysicalBytesRead:                 readerStats.OpenPhysicalBytesRead,
		VectorDirectViews:                     searchStats.VectorDirectViews,
		VectorMmapDirectViews:                 searchStats.VectorMmapDirectViews,
		VectorHeapCopyTypedViews:              searchStats.VectorHeapCopyTypedViews,
		VectorScratchDecodes:                  searchStats.VectorScratchDecodes,
		VectorPreparedDirectViews:             searchStats.VectorPreparedDirectViews,
		VectorPreparedIdentityMappings:        searchStats.VectorPreparedIdentityMappings,
		VectorPreparedRowRefMappings:          searchStats.VectorPreparedRowRefMappings,
		VectorCertificationFailures:           searchStats.VectorCertificationFailures,
		VectorAbsoluteOffsetUnaligned:         searchStats.VectorAbsoluteOffsetUnaligned,
		VectorActualPointerUnaligned:          searchStats.VectorActualPointerUnaligned,
		VectorStaleHandles:                    searchStats.VectorStaleHandles,
		AdjacencyDirectViews:                  searchStats.AdjacencyDirectViews,
		AdjacencyMmapDirectViews:              searchStats.AdjacencyMmapDirectViews,
		AdjacencyHeapCopyTypedViews:           searchStats.AdjacencyHeapCopyTypedViews,
		AdjacencyPreparedCSRDirectViews:       searchStats.AdjacencyPreparedCSRDirectViews,
		AdjacencyPreparedCSRMmapDirectViews:   searchStats.AdjacencyPreparedCSRMmapDirectViews,
		AdjacencyTypedListDirectViews:         searchStats.AdjacencyTypedListDirectViews,
		AdjacencyTypedListMmapDirectViews:     searchStats.AdjacencyTypedListMmapDirectViews,
		AdjacencyTypedListHeapCopyTypedViews:  searchStats.AdjacencyTypedListHeapCopyTypedViews,
		AdjacencyTypedListScratchDecodes:      searchStats.AdjacencyTypedListScratchDecodes,
		AdjacencyLegacyFallbacks:              searchStats.AdjacencyLegacyFallbacks,
		AdjacencySourceUnavailable:            searchStats.AdjacencySourceUnavailable,
		AdjacencySourceFallbacks:              searchStats.AdjacencySourceFallbacks,
		AdjacencyCertificationFailures:        searchStats.AdjacencyCertificationFailures,
		AdjacencyValidationFailures:           searchStats.AdjacencyValidationFailures,
		AdjacencyAbsoluteOffsetUnaligned:      searchStats.AdjacencyAbsoluteOffsetUnaligned,
		AdjacencyActualPointerUnaligned:       searchStats.AdjacencyActualPointerUnaligned,
		AdjacencyStaleHandles:                 searchStats.AdjacencyStaleHandles,
		AdjacencyScratchDecodes:               searchStats.AdjacencyScratchDecodes,
		NormDirectViews:                       searchStats.NormDirectViews,
		NormMmapDirectViews:                   searchStats.NormMmapDirectViews,
		NormHeapCopyTypedViews:                searchStats.NormHeapCopyTypedViews,
		NormScratchDecodes:                    searchStats.NormScratchDecodes,
		NormPreparedDirectViews:               searchStats.NormPreparedDirectViews,
		NormSourceUnavailable:                 searchStats.NormSourceUnavailable,
		NormSourceFallbacks:                   searchStats.NormSourceFallbacks,
		NormValidationFailures:                searchStats.NormValidationFailures,
		NormAbsoluteOffsetUnaligned:           searchStats.NormAbsoluteOffsetUnaligned,
		NormActualPointerUnaligned:            searchStats.NormActualPointerUnaligned,
		NormStaleHandles:                      searchStats.NormStaleHandles,
		NormMappedBytes:                       searchStats.NormMappedBytes,
		NormHeapCopyBytes:                     searchStats.NormHeapCopyBytes,
		NormDecodedBytes:                      searchStats.NormDecodedBytes,
		NormActiveHandles:                     searchStats.NormActiveHandles,
		NormDeniedResources:                   searchStats.NormDeniedResources,
		TypedColumnMappedBytes:                searchStats.TypedColumnMappedBytes,
		TypedColumnHeapCopyBytes:              searchStats.TypedColumnHeapCopyBytes,
		TypedColumnDecodedBytes:               searchStats.TypedColumnDecodedBytes,
		TypedColumnActiveHandles:              searchStats.TypedColumnActiveHandles,
		TypedColumnDeniedResources:            searchStats.TypedColumnDeniedResources,
		TypedColumnFallbacks:                  searchStats.TypedColumnFallbacks,
		RowRefVectorSourceState:               searchStats.RowRefVectorSourceState,
		RowRefVectorSourceLegacyGraphIDs:      searchStats.RowRefVectorSourceLegacyGraphIDs,
		RowRefStatePreparedViews:              searchStats.RowRefStatePreparedViews,
		RowRefStateMmapDirectFields:           searchStats.RowRefStateMmapDirectFields,
		RowRefStateResultRefs:                 searchStats.RowRefStateResultRefs,
		RowRefStateSourceUnavailable:          searchStats.RowRefStateSourceUnavailable,
		RowRefStateSourceFallbacks:            searchStats.RowRefStateSourceFallbacks,
		ResultIDPreparedBytesViews:            searchStats.ResultIDPreparedBytesViews,
		ResultIDTypedBytesState:               searchStats.ResultIDTypedBytesState,
		ResultIDGraphFallbacks:                searchStats.ResultIDGraphFallbacks,
		ResultIDStateValidationFailures:       searchStats.ResultIDStateValidationFailures,
		PreparedGraphSearchViews:              searchStats.PreparedGraphSearchViews,
		GraphRowFallbacks:                     searchStats.GraphRowFallbacks,
		BenchmarkDebugSearches:                searchStats.BenchmarkDebugSearches,
		NeighborTiles:                         searchStats.NeighborTiles,
		NeighborTileNeighbors:                 searchStats.NeighborTileNeighbors,
		NeighborTileMaxSize:                   searchStats.NeighborTileMaxSize,
		NeighborTileSize0:                     searchStats.NeighborTileSize0,
		NeighborTileSize1:                     searchStats.NeighborTileSize1,
		NeighborTileSize2To4:                  searchStats.NeighborTileSize2To4,
		NeighborTileSize5To8:                  searchStats.NeighborTileSize5To8,
		NeighborTileSize9To16:                 searchStats.NeighborTileSize9To16,
		NeighborTileSize17Plus:                searchStats.NeighborTileSize17Plus,
		ScoreBatchSingletons:                  searchStats.ScoreBatchSingletons,
		ScoreBatchSize2To4:                    searchStats.ScoreBatchSize2To4,
		ScoreBatchSize5To8:                    searchStats.ScoreBatchSize5To8,
		ScoreBatchSize9To16:                   searchStats.ScoreBatchSize9To16,
		ScoreBatchSize17Plus:                  searchStats.ScoreBatchSize17Plus,
		ScoredNeighbors:                       searchStats.ScoredNeighbors,
		SkippedNeighbors:                      searchStats.SkippedNeighbors,
		AlreadyVisitedSkips:                   searchStats.AlreadyVisitedSkips,
		FilterSkips:                           searchStats.FilterSkips,
		UpperLayerScores:                      searchStats.UpperLayerScores,
		UpperLayerEntryScores:                 searchStats.UpperLayerEntryScores,
		UpperLayerNeighborScores:              searchStats.UpperLayerNeighborScores,
		UpperLayerEdgeVisits:                  searchStats.UpperLayerEdgeVisits,
		UpperLayerScoredNeighbors:             searchStats.UpperLayerScoredNeighbors,
		UpperLayerFilterSkips:                 searchStats.UpperLayerFilterSkips,
		Layer0Scores:                          searchStats.Layer0Scores,
		Layer0SeedScores:                      searchStats.Layer0SeedScores,
		Layer0NeighborScores:                  searchStats.Layer0NeighborScores,
		Layer0EdgeVisits:                      searchStats.Layer0EdgeVisits,
		Layer0ScoredNeighbors:                 searchStats.Layer0ScoredNeighbors,
		Layer0AlreadyVisitedSkips:             searchStats.Layer0AlreadyVisitedSkips,
		Layer0FilterSkips:                     searchStats.Layer0FilterSkips,
		FrontierPushes:                        searchStats.FrontierPushes,
		FrontierPops:                          searchStats.FrontierPops,
		FrontierPopMisses:                     searchStats.FrontierPopMisses,
		FrontierSiftUpSteps:                   searchStats.FrontierSiftUpSteps,
		FrontierSiftDownSteps:                 searchStats.FrontierSiftDownSteps,
		TopKInsertAttempts:                    searchStats.TopKInsertAttempts,
		TopKInsertSuccesses:                   searchStats.TopKInsertSuccesses,
		TopKInsertRejections:                  searchStats.TopKInsertRejections,
		TopKShiftSteps:                        searchStats.TopKShiftSteps,
		VisitedMarkChecks:                     searchStats.VisitedMarkChecks,
		VisitedMarkHits:                       searchStats.VisitedMarkHits,
		VisitedMarkMisses:                     searchStats.VisitedMarkMisses,
		ExactModeSearches:                     searchStats.ExactModeSearches,
		ExactCandidateOrderObservations:       searchStats.ExactCandidateOrderObservations,
		ExactCandidateOrderTransitions:        searchStats.ExactCandidateOrderTransitions,
		ExactCandidateOrderAdjacentForward:    searchStats.ExactCandidateOrderAdjacentForward,
		ExactCandidateOrderNonAdjacentForward: searchStats.ExactCandidateOrderNonAdjacentForward,
		ExactCandidateOrderBackwardJumps:      searchStats.ExactCandidateOrderBackwardJumps,
		ExactCandidateOrderMaxForwardRun:      searchStats.ExactCandidateOrderMaxForwardRun,
	}
}
