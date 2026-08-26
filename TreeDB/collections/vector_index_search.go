package collections

import (
	"errors"
	"fmt"
	"math"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

// ErrVectorIndexSearchUnavailable reports that the requested vector index is
// not currently searchable through the selected product path.
var ErrVectorIndexSearchUnavailable = errors.New("collections: vector index search unavailable")

var collectionSearchVectorIndexResponseBufferPool sync.Pool

// VectorIndexSearchPath identifies the physical implementation used for a
// public vector-index search.
type VectorIndexSearchPath string

const (
	// VectorIndexSearchPathNativeRuntime searches the collection's persistent
	// mutable native HNSW graph without fetching source documents.
	VectorIndexSearchPathNativeRuntime VectorIndexSearchPath = "native_runtime"
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
// Work-accounting mode is an explicit diagnostic mode for explaining per-query
// search cost; do not mix it into low-overhead production throughput evidence.
type VectorIndexSearchStatsMode string

const (
	VectorIndexSearchStatsModeDefault         VectorIndexSearchStatsMode = ""
	VectorIndexSearchStatsModeMinimal         VectorIndexSearchStatsMode = "minimal"
	VectorIndexSearchStatsModeProduction      VectorIndexSearchStatsMode = "production"
	VectorIndexSearchStatsModeFullDiagnostics VectorIndexSearchStatsMode = "full_diagnostics"
	VectorIndexSearchStatsModeWorkAccounting  VectorIndexSearchStatsMode = "work_accounting"
	VectorIndexSearchStatsModeBenchmarkDebug  VectorIndexSearchStatsMode = "benchmark_debug"
)

func columnVectorGraphNativeSearchStatsModeFromPublic(mode VectorIndexSearchStatsMode) (columnVectorGraphNativeSearchStatsMode, error) {
	switch mode {
	case VectorIndexSearchStatsModeDefault, VectorIndexSearchStatsModeFullDiagnostics:
		return columnVectorGraphNativeSearchStatsModeFullDiagnostics, nil
	case VectorIndexSearchStatsModeMinimal, VectorIndexSearchStatsModeProduction:
		return columnVectorGraphNativeSearchStatsModeMinimal, nil
	case VectorIndexSearchStatsModeWorkAccounting:
		return columnVectorGraphNativeSearchStatsModeWorkAccounting, nil
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
	// QueryMode selects exact, quantized-only, or quantized-rerank search. The
	// zero value is exact; quantized modes require QuantizedIndexName and fail
	// closed until matching assets/scorers are available.
	QueryMode VectorIndexQueryMode
	// QuantizedIndexName selects the named derived score plane for quantized modes.
	QuantizedIndexName string
	// QuantizedRerankCandidates bounds the quantized candidate set reranked by
	// exact float32 vectors in quantized_rerank mode. Zero uses the normalized
	// ef_search candidate set.
	QuantizedRerankCandidates int
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
	// Score is the result score for the selected query mode: exact/default and
	// quantized_rerank return authoritative float32 cosine scores, while
	// quantized_only returns the selected codec's estimated cosine score.
	Score float64 `json:"score"`
	// Document is populated only when IncludeDocuments is true.
	Document []byte `json:"document,omitempty"`
}

// VectorIndexSearchStats reports search telemetry. Graph/search and reader
// counters are per-search deltas unless the field starts with Open; Open*
// counters describe bound reader setup performed before Search or collection-level
// one-shot open/setup performed inside SearchVectorIndex.
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
	// CandidateFetches is the per-search count of score-plane row fetches for scored candidates.
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
	// FP32ScoreCalls aliases exact float32 candidate score calls for work-accounting consumers.
	FP32ScoreCalls uint64 `json:"fp32_score_calls,omitempty"`
	// QuantizedScoreCalls counts candidate scores produced by a quantized score-plane scorer.
	QuantizedScoreCalls uint64 `json:"quantized_score_calls,omitempty"`
	// QuantizedCodeBytesRead is the logical quantized code-row byte count read while scoring candidates.
	QuantizedCodeBytesRead uint64 `json:"quantized_code_bytes_read,omitempty"`
	// QuantizedRerankCandidates counts quantized shortlist candidates submitted to exact rerank.
	QuantizedRerankCandidates uint64 `json:"quantized_rerank_candidates,omitempty"`
	// QuantizedRerankExactScoreCalls counts exact FP32 score calls used by quantized_rerank.
	QuantizedRerankExactScoreCalls uint64 `json:"quantized_rerank_exact_score_calls,omitempty"`
	// ExactRerankScoreCalls aliases exact rerank score calls for work-accounting consumers.
	ExactRerankScoreCalls uint64 `json:"exact_rerank_score_calls,omitempty"`
	// QuantizedScorerActive reports that a validated quantized scorer served this search.
	QuantizedScorerActive uint64 `json:"quantized_scorer_active,omitempty"`
	// QuantizedAssetMissing reports that the selected quantized score-plane asset was absent.
	QuantizedAssetMissing uint64 `json:"quantized_asset_missing,omitempty"`
	// QuantizedAssetInvalid reports that the selected quantized score-plane asset failed validation or decode.
	QuantizedAssetInvalid uint64 `json:"quantized_asset_invalid,omitempty"`
	// QuantizedAssetStale reports that the selected quantized score-plane asset did not match the current graph/index identity.
	QuantizedAssetStale uint64 `json:"quantized_asset_stale,omitempty"`
	// QuantizedAssetClosed reports that the selected quantized score-plane asset was closed before use.
	QuantizedAssetClosed uint64 `json:"quantized_asset_closed,omitempty"`
	// QuantizedAssetUnavailable aggregates missing, invalid, stale, or closed quantized score-plane asset failures.
	QuantizedAssetUnavailable uint64 `json:"quantized_asset_unavailable,omitempty"`
	// QuantizedAssetMmapDirect reports searches with a validated direct mmap quantized score-plane asset bound to the searcher.
	QuantizedAssetMmapDirect uint64 `json:"quantized_asset_mmap_direct,omitempty"`
	// QuantizedAssetHeapCopy reports searches with a validated heap-copy quantized score-plane asset bound to the searcher.
	QuantizedAssetHeapCopy uint64 `json:"quantized_asset_heap_copy,omitempty"`
	// QuantizedAssetOpenNanos reports open/prepare time for the selected quantized score-plane asset.
	QuantizedAssetOpenNanos uint64 `json:"quantized_asset_open_nanos,omitempty"`
	// QuantizedAssetMappedBytes is the mapped byte total backing the selected quantized score-plane asset.
	QuantizedAssetMappedBytes uint64 `json:"quantized_asset_mapped_bytes,omitempty"`
	// QuantizedAssetHeapCopyBytes is the heap-copy byte total backing the selected quantized score-plane asset.
	QuantizedAssetHeapCopyBytes uint64 `json:"quantized_asset_heap_copy_bytes,omitempty"`
	// QuantizedAssetActiveHandles is the current active mappedresource handle count for the selected quantized score-plane asset.
	QuantizedAssetActiveHandles int64 `json:"quantized_asset_active_handles,omitempty"`
	// QuantizedScoreCodecScalarU8Alpha reports searches served by scalar_u8 per-granule alpha scoring.
	QuantizedScoreCodecScalarU8Alpha uint64 `json:"quantized_score_codec_scalar_u8_alpha,omitempty"`
	// QuantizedScoreCodecBRQ1Bit reports searches served by the brq_1bit quantized scorer.
	QuantizedScoreCodecBRQ1Bit uint64 `json:"quantized_score_codec_brq_1bit,omitempty"`
	// BRQ1BitQueryWeightBits reports the runtime query-weight bit width for brq_1bit searches.
	BRQ1BitQueryWeightBits uint64 `json:"brq_1bit_query_weight_bits,omitempty"`
	// BRQ1BitBitProductPasses counts logical brq_1bit bit-product passes used by scorer calls.
	BRQ1BitBitProductPasses uint64 `json:"brq_1bit_bitproduct_passes,omitempty"`
	// BRQ1BitQueryWeightScale reports the query-local uint4 weight scale for brq_1bit searches.
	BRQ1BitQueryWeightScale float64 `json:"brq_1bit_query_weight_scale,omitempty"`
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

	// SearchRouteColumnGraphPrepared reports that this search used the current prepared column_graph route.
	SearchRouteColumnGraphPrepared uint64 `json:"search_route_column_graph_prepared,omitempty"`
	// SearchRouteNativeRuntime reports that this search used the persistent mutable native HNSW graph.
	SearchRouteNativeRuntime uint64 `json:"search_route_native_runtime,omitempty"`
	// NativeRuntimeFullRebuilds reports automatic document-scan rebuilds retained by the selected live graph.
	NativeRuntimeFullRebuilds uint64 `json:"native_runtime_full_rebuilds,omitempty"`
	// SearchRouteColumnGraphFallback reports that this search used the column_graph compatibility/fallback route instead of the prepared route.
	SearchRouteColumnGraphFallback uint64 `json:"search_route_column_graph_fallback,omitempty"`
	// SearchRouteHNSWSearchPack reports that this search used the exact FP32 hnsw_search_pack_v1 route.
	SearchRouteHNSWSearchPack uint64 `json:"search_route_hnsw_search_pack,omitempty"`
	// SearchRouteQuantizedOnly reports that this search used the codec-generic quantized-only route.
	SearchRouteQuantizedOnly uint64 `json:"search_route_quantized_only,omitempty"`
	// SearchRouteQuantizedRerank reports that this search used the codec-generic quantized-rerank route.
	SearchRouteQuantizedRerank uint64 `json:"search_route_quantized_rerank,omitempty"`
	// HNSWSearchPackActive reports that a validated hnsw_search_pack_v1 served this search.
	HNSWSearchPackActive uint64 `json:"hnsw_search_pack_active,omitempty"`
	// HNSWSearchPackMissing reports that no hnsw_search_pack_v1 was available for this searcher.
	HNSWSearchPackMissing uint64 `json:"hnsw_search_pack_missing,omitempty"`
	// HNSWSearchPackInvalid reports that a hnsw_search_pack_v1 candidate was present but failed validation.
	HNSWSearchPackInvalid uint64 `json:"hnsw_search_pack_invalid,omitempty"`
	// HNSWSearchPackStale reports that a previously opened hnsw_search_pack_v1 handle is no longer live.
	HNSWSearchPackStale uint64 `json:"hnsw_search_pack_stale,omitempty"`
	// HNSWSearchPackClosed reports that the bound hnsw_search_pack_v1 view is closed.
	HNSWSearchPackClosed uint64 `json:"hnsw_search_pack_closed,omitempty"`
	// HNSWSearchPackFallbacks reports searches that fell back from hnsw_search_pack_v1 to an existing route.
	HNSWSearchPackFallbacks uint64 `json:"hnsw_search_pack_fallbacks,omitempty"`
	// HNSWSearchPackMmapDirect reports searches with a validated direct mmap hnsw_search_pack_v1 view bound to the searcher.
	HNSWSearchPackMmapDirect uint64 `json:"hnsw_search_pack_mmap_direct,omitempty"`
	// HNSWSearchPackHeapCopy reports searches with a validated heap-copy hnsw_search_pack_v1 view bound to the searcher.
	HNSWSearchPackHeapCopy uint64 `json:"hnsw_search_pack_heap_copy,omitempty"`
	// HNSWSearchPackOpenNanos reports the open/prepared-view time for the bound hnsw_search_pack_v1 view.
	HNSWSearchPackOpenNanos uint64 `json:"hnsw_search_pack_open_nanos,omitempty"`
	// HNSWSearchPackMappedBytes is the active mapped byte total backing the bound hnsw_search_pack_v1 view.
	HNSWSearchPackMappedBytes uint64 `json:"hnsw_search_pack_mapped_bytes,omitempty"`
	// HNSWSearchPackHeapCopyBytes is the active heap-copy byte total backing the bound hnsw_search_pack_v1 view.
	HNSWSearchPackHeapCopyBytes uint64 `json:"hnsw_search_pack_heap_copy_bytes,omitempty"`
	// HNSWSearchPackActiveHandles is the current active mappedresource handle count for the hnsw_search_pack_v1 view.
	HNSWSearchPackActiveHandles int64 `json:"hnsw_search_pack_active_handles,omitempty"`
	// HNSWSearchPackCacheHits reports collection-level prepared hnsw_search_pack_v1 cache hits for this public search call.
	HNSWSearchPackCacheHits uint64 `json:"hnsw_search_pack_cache_hits,omitempty"`
	// HNSWSearchPackCacheMisses reports collection-level prepared hnsw_search_pack_v1 cache misses/build admissions for this public search call.
	HNSWSearchPackCacheMisses uint64 `json:"hnsw_search_pack_cache_misses,omitempty"`
	// HNSWSearchPackCacheWaits reports waits behind an in-flight collection-level hnsw_search_pack_v1 cache build for this public search call.
	HNSWSearchPackCacheWaits uint64 `json:"hnsw_search_pack_cache_waits,omitempty"`
	// HNSWSearchPackCacheBuilds reports collection-level prepared hnsw_search_pack_v1 cache builds started by this public search call.
	HNSWSearchPackCacheBuilds uint64 `json:"hnsw_search_pack_cache_builds,omitempty"`
	// OpenSearcherCalls reports collection-level SearchVectorIndex calls that entered the one-shot VectorIndexSearcher open/setup boundary.
	OpenSearcherCalls uint64 `json:"open_searcher_calls,omitempty"`
	// OpenSetupInTimedLoop reports collection-level SearchVectorIndex calls whose one-shot open/setup work is part of the call boundary.
	OpenSetupInTimedLoop uint64 `json:"open_setup_in_timed_loop,omitempty"`
	// ResponseOwnedResultAllocs reports response-owned result storage creation by Search or SearchVectorIndex. Buffered APIs leave this zero.
	ResponseOwnedResultAllocs uint64 `json:"response_owned_result_allocs,omitempty"`

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
	UpperLayerScoreTiles                  uint64 `json:"upper_layer_score_tiles,omitempty"`
	UpperLayerScoreTileCandidates         uint64 `json:"upper_layer_score_tile_candidates,omitempty"`
	UpperLayerScoreTileMaxSize            uint64 `json:"upper_layer_score_tile_max_size,omitempty"`
	UpperLayerAdjacencyLoads              uint64 `json:"upper_layer_adjacency_loads,omitempty"`
	UpperLayerAdjacencyNeighbors          uint64 `json:"upper_layer_adjacency_neighbors,omitempty"`
	UpperLayerEdgeVisits                  uint64 `json:"upper_layer_edge_visits,omitempty"`
	UpperLayerScoredNeighbors             uint64 `json:"upper_layer_scored_neighbors,omitempty"`
	UpperLayerFilterSkips                 uint64 `json:"upper_layer_filter_skips,omitempty"`
	Layer0Scores                          uint64 `json:"layer0_scores,omitempty"`
	Layer0SeedScores                      uint64 `json:"layer0_seed_scores,omitempty"`
	Layer0NeighborScores                  uint64 `json:"layer0_neighbor_scores,omitempty"`
	Layer0ScoreTiles                      uint64 `json:"layer0_score_tiles,omitempty"`
	Layer0ScoreTileCandidates             uint64 `json:"layer0_score_tile_candidates,omitempty"`
	Layer0ScoreTileMaxSize                uint64 `json:"layer0_score_tile_max_size,omitempty"`
	Layer0AdjacencyLoads                  uint64 `json:"layer0_adjacency_loads,omitempty"`
	Layer0AdjacencyNeighbors              uint64 `json:"layer0_adjacency_neighbors,omitempty"`
	Layer0EdgeVisits                      uint64 `json:"layer0_edge_visits,omitempty"`
	Layer0ScoredNeighbors                 uint64 `json:"layer0_scored_neighbors,omitempty"`
	Layer0AlreadyVisitedSkips             uint64 `json:"layer0_already_visited_skips,omitempty"`
	Layer0FilterSkips                     uint64 `json:"layer0_filter_skips,omitempty"`
	Layer0StopChecks                      uint64 `json:"layer0_stop_checks,omitempty"`
	Layer0StopTrue                        uint64 `json:"layer0_stop_true,omitempty"`
	Layer0StopFalse                       uint64 `json:"layer0_stop_false,omitempty"`
	CandidateComparisons                  uint64 `json:"candidate_comparisons,omitempty"`
	FrontierComparisons                   uint64 `json:"frontier_comparisons,omitempty"`
	TopKComparisons                       uint64 `json:"top_k_comparisons,omitempty"`
	FrontierPushes                        uint64 `json:"frontier_pushes,omitempty"`
	FrontierPops                          uint64 `json:"frontier_pops,omitempty"`
	HeapPushes                            uint64 `json:"heap_pushes,omitempty"`
	HeapPops                              uint64 `json:"heap_pops,omitempty"`
	FrontierPopMisses                     uint64 `json:"frontier_pop_misses,omitempty"`
	FrontierSiftUpCalls                   uint64 `json:"frontier_sift_up_calls,omitempty"`
	FrontierSiftDownCalls                 uint64 `json:"frontier_sift_down_calls,omitempty"`
	FrontierSiftUpSteps                   uint64 `json:"frontier_sift_up_steps,omitempty"`
	FrontierSiftDownSteps                 uint64 `json:"frontier_sift_down_steps,omitempty"`
	TopKInsertAttempts                    uint64 `json:"top_k_insert_attempts,omitempty"`
	TopKInsertSuccesses                   uint64 `json:"top_k_insert_successes,omitempty"`
	TopKInsertRejections                  uint64 `json:"top_k_insert_rejections,omitempty"`
	TopKHeapSiftSteps                     uint64 `json:"top_k_heap_sift_steps,omitempty"`
	VisitedMarkChecks                     uint64 `json:"visited_mark_checks,omitempty"`
	VisitedMarkHits                       uint64 `json:"visited_mark_hits,omitempty"`
	VisitedMarkMisses                     uint64 `json:"visited_mark_misses,omitempty"`
	VisitedMarkInserts                    uint64 `json:"visited_mark_inserts,omitempty"`
	VisitedResetEpochAdvances             uint64 `json:"visited_reset_epoch_advances,omitempty"`
	VisitedResetClearedRows               uint64 `json:"visited_reset_cleared_rows,omitempty"`
	ExactModeSearches                     uint64 `json:"exact_mode_searches,omitempty"`
	ExactCandidateOrderObservations       uint64 `json:"exact_candidate_order_observations,omitempty"`
	ExactCandidateOrderTransitions        uint64 `json:"exact_candidate_order_transitions,omitempty"`
	ExactCandidateOrderAdjacentForward    uint64 `json:"exact_candidate_order_adjacent_forward,omitempty"`
	ExactCandidateOrderNonAdjacentForward uint64 `json:"exact_candidate_order_non_adjacent_forward,omitempty"`
	ExactCandidateOrderBackwardJumps      uint64 `json:"exact_candidate_order_backward_jumps,omitempty"`
	ExactCandidateOrderMaxForwardRun      uint64 `json:"exact_candidate_order_max_forward_run,omitempty"`
	// WorkAccountingSearches reports that this search collected explicit work-accounting counters/timers.
	WorkAccountingSearches uint64 `json:"work_accounting_searches,omitempty"`
	// DistanceKernelNanos attributes query-local candidate scoring time to exact/quantized distance kernels.
	DistanceKernelNanos uint64 `json:"distance_kernel_nanos,omitempty"`
	// GraphTraversalNanos attributes HNSW traversal wall time excluding recorded distance-kernel time.
	GraphTraversalNanos uint64 `json:"graph_traversal_nanos,omitempty"`
	// ServiceResponseNanos attributes document-service response construction after collection search.
	ServiceResponseNanos uint64 `json:"service_response_nanos,omitempty"`
	// DocumentRowRefStateFetches counts post-top-k document fetches served with vector-index row-ref state.
	DocumentRowRefStateFetches uint64 `json:"document_row_ref_state_fetches,omitempty"`
	// DocumentRowRefLookupFallbacks counts post-top-k document fetches that fell back to ID-to-row-ref lookup.
	DocumentRowRefLookupFallbacks uint64 `json:"document_row_ref_lookup_fallbacks,omitempty"`
}

type vectorIndexSearchVisibility struct {
	runtime                  *VectorIndex
	collectionName           string
	indexName                string
	strategy                 VectorIndexStrategy
	schemaGeneration         uint64
	mutationSeq              uint64
	sourceDocumentGeneration uint64
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
	// SearchVectorIndex return response-owned slices; SearchWithBuffer and
	// SearchVectorIndexWithBuffer return slices owned by the caller's
	// VectorIndexSearchBuffer.
	Results    []VectorIndexSearchResult `json:"results,omitempty"`
	visibility vectorIndexSearchVisibility
}

// VectorIndexSearchRouteKind is a compact route summary derived from public
// search stats. The exact hnsw_search_pack_v1 route is intentionally distinct
// from codec-generic quantized route kinds; a quantized search may still report
// SearchRouteHNSWSearchPack when a codec-specific score plane uses pack
// traversal, but RouteKind remains quantized.
type VectorIndexSearchRouteKind string

const (
	// VectorIndexSearchRouteUnknown reports that stats did not identify a search route.
	VectorIndexSearchRouteUnknown VectorIndexSearchRouteKind = "unknown"
	// VectorIndexSearchRouteNativeRuntime reports the persistent mutable native HNSW route.
	VectorIndexSearchRouteNativeRuntime VectorIndexSearchRouteKind = "native_runtime"
	// VectorIndexSearchRouteExactHNSWSearchPackV1 reports the exact FP32 hnsw_search_pack_v1 route.
	VectorIndexSearchRouteExactHNSWSearchPackV1 VectorIndexSearchRouteKind = "exact_hnsw_search_pack_v1"
	// VectorIndexSearchRouteQuantizedOnly reports the codec-generic quantized-only route.
	VectorIndexSearchRouteQuantizedOnly VectorIndexSearchRouteKind = "quantized_only"
	// VectorIndexSearchRouteQuantizedRerank reports the codec-generic quantized-rerank route.
	VectorIndexSearchRouteQuantizedRerank VectorIndexSearchRouteKind = "quantized_rerank"
	// VectorIndexSearchRouteColumnGraphPrepared reports the prepared column_graph route.
	VectorIndexSearchRouteColumnGraphPrepared VectorIndexSearchRouteKind = "column_graph_prepared"
	// VectorIndexSearchRouteColumnGraphFallback reports the column_graph compatibility/fallback route.
	VectorIndexSearchRouteColumnGraphFallback VectorIndexSearchRouteKind = "column_graph_fallback"
)

// VectorIndexSearchHNSWSearchPackStatus summarizes hnsw_search_pack_v1 health
// from public search stats. It describes the pack traversal asset itself; codec
// route kind and quantized score-plane health remain separate counters.
type VectorIndexSearchHNSWSearchPackStatus string

const (
	// VectorIndexSearchHNSWSearchPackStatusNone reports no hnsw_search_pack_v1 observation.
	VectorIndexSearchHNSWSearchPackStatusNone VectorIndexSearchHNSWSearchPackStatus = "none"
	// VectorIndexSearchHNSWSearchPackStatusActive reports a validated active hnsw_search_pack_v1 view.
	VectorIndexSearchHNSWSearchPackStatusActive VectorIndexSearchHNSWSearchPackStatus = "active"
	// VectorIndexSearchHNSWSearchPackStatusMissing reports no available hnsw_search_pack_v1 state.
	VectorIndexSearchHNSWSearchPackStatusMissing VectorIndexSearchHNSWSearchPackStatus = "missing"
	// VectorIndexSearchHNSWSearchPackStatusInvalid reports invalid hnsw_search_pack_v1 state.
	VectorIndexSearchHNSWSearchPackStatusInvalid VectorIndexSearchHNSWSearchPackStatus = "invalid"
	// VectorIndexSearchHNSWSearchPackStatusStale reports stale hnsw_search_pack_v1 state.
	VectorIndexSearchHNSWSearchPackStatusStale VectorIndexSearchHNSWSearchPackStatus = "stale"
	// VectorIndexSearchHNSWSearchPackStatusClosed reports a closed hnsw_search_pack_v1 view.
	VectorIndexSearchHNSWSearchPackStatusClosed VectorIndexSearchHNSWSearchPackStatus = "closed"
)

// VectorIndexSearchFallbackReason summarizes why a search did not remain on the
// preferred exact no-document route. Values are low-cardinality and derived from
// counters; no logging or map allocation is required.
type VectorIndexSearchFallbackReason string

const (
	// VectorIndexSearchFallbackReasonNone reports no observed fallback reason.
	VectorIndexSearchFallbackReasonNone VectorIndexSearchFallbackReason = "none"
	// VectorIndexSearchFallbackReasonHNSWSearchPackMissing reports fallback because exact pack state was missing.
	VectorIndexSearchFallbackReasonHNSWSearchPackMissing VectorIndexSearchFallbackReason = "hnsw_search_pack_missing"
	// VectorIndexSearchFallbackReasonHNSWSearchPackInvalid reports fallback because exact pack state was invalid.
	VectorIndexSearchFallbackReasonHNSWSearchPackInvalid VectorIndexSearchFallbackReason = "hnsw_search_pack_invalid"
	// VectorIndexSearchFallbackReasonHNSWSearchPackStale reports fallback because exact pack state was stale.
	VectorIndexSearchFallbackReasonHNSWSearchPackStale VectorIndexSearchFallbackReason = "hnsw_search_pack_stale"
	// VectorIndexSearchFallbackReasonHNSWSearchPackClosed reports fallback because exact pack state was closed.
	VectorIndexSearchFallbackReasonHNSWSearchPackClosed VectorIndexSearchFallbackReason = "hnsw_search_pack_closed"
	// VectorIndexSearchFallbackReasonHNSWSearchPackUnavailable reports a generic exact pack fallback without a more specific status.
	VectorIndexSearchFallbackReasonHNSWSearchPackUnavailable VectorIndexSearchFallbackReason = "hnsw_search_pack_unavailable"
	// VectorIndexSearchFallbackReasonColumnGraphFallback reports the column_graph compatibility/fallback route.
	VectorIndexSearchFallbackReasonColumnGraphFallback VectorIndexSearchFallbackReason = "column_graph_fallback"
	// VectorIndexSearchFallbackReasonGraphRowFallback reports legacy graph-row fallback activity.
	VectorIndexSearchFallbackReasonGraphRowFallback VectorIndexSearchFallbackReason = "graph_row_fallback"
	// VectorIndexSearchFallbackReasonTypedColumnVectorFallback reports typed-column vector fallback activity.
	VectorIndexSearchFallbackReasonTypedColumnVectorFallback VectorIndexSearchFallbackReason = "typed_column_vector_fallback"
	// VectorIndexSearchFallbackReasonVectorScratchDecode reports vector scratch/fallback decode activity.
	VectorIndexSearchFallbackReasonVectorScratchDecode VectorIndexSearchFallbackReason = "vector_scratch_decode"
)

// VectorIndexSearchDiagnostics is a compact allocation-free value summary for
// route/status and no-document guardrail checks. It is derived from
// VectorIndexSearchStats; callers that need full counters should inspect Stats.
type VectorIndexSearchDiagnostics struct {
	Route                         VectorIndexSearchRouteKind            `json:"route"`
	HNSWSearchPackStatus          VectorIndexSearchHNSWSearchPackStatus `json:"hnsw_search_pack_status"`
	FallbackReason                VectorIndexSearchFallbackReason       `json:"fallback_reason"`
	NoDocumentGuardrailsOK        bool                                  `json:"no_document_guardrails_ok"`
	ExactHNSWSearchPackNoDocRoute bool                                  `json:"exact_hnsw_search_pack_no_doc_route"`
	DocumentsFetched              uint64                                `json:"docs_fetched,omitempty"`
	GraphRowFallbacks             uint64                                `json:"graph_row_fallbacks,omitempty"`
	TypedColumnVectorFallbacks    uint64                                `json:"typed_column_vector_fallbacks,omitempty"`
	VectorScratchDecodes          uint64                                `json:"vector_scratch_decodes,omitempty"`
	OpenSearcherCalls             uint64                                `json:"open_searcher_calls,omitempty"`
	OpenSetupInTimedLoop          uint64                                `json:"open_setup_in_timed_loop,omitempty"`
	ResponseOwnedResultAllocs     uint64                                `json:"response_owned_result_allocs,omitempty"`
	HNSWSearchPackCacheHits       uint64                                `json:"hnsw_search_pack_cache_hits,omitempty"`
	HNSWSearchPackCacheMisses     uint64                                `json:"hnsw_search_pack_cache_misses,omitempty"`
	HNSWSearchPackCacheWaits      uint64                                `json:"hnsw_search_pack_cache_waits,omitempty"`
	HNSWSearchPackCacheBuilds     uint64                                `json:"hnsw_search_pack_cache_builds,omitempty"`
	LiveANN                       VectorIndexSearchLiveANNDiagnostics   `json:"live_ann"`
}

// VectorIndexSearchLiveANNDiagnostics proves that the selected query stayed on
// the mutable ANN route rather than rebuilding or scanning exact documents.
type VectorIndexSearchLiveANNDiagnostics struct {
	Enabled        bool   `json:"enabled"`
	ExactFallbacks uint64 `json:"exact_fallbacks"`
	FullRebuilds   uint64 `json:"full_rebuilds"`
}

// Diagnostics returns a compact route/status summary for the response.
func (r VectorIndexSearchResponse) Diagnostics() VectorIndexSearchDiagnostics {
	return r.Stats.Diagnostics()
}

// Diagnostics returns a compact route/status summary derived from the stats.
func (s VectorIndexSearchStats) Diagnostics() VectorIndexSearchDiagnostics {
	return VectorIndexSearchDiagnostics{
		Route:                         s.RouteKind(),
		HNSWSearchPackStatus:          s.HNSWSearchPackStatus(),
		FallbackReason:                s.FallbackReason(),
		NoDocumentGuardrailsOK:        s.NoDocumentGuardrailsOK(),
		ExactHNSWSearchPackNoDocRoute: s.ExactHNSWSearchPackNoDocumentRoute(),
		DocumentsFetched:              s.DocumentsFetched,
		GraphRowFallbacks:             s.GraphRowFallbacks,
		TypedColumnVectorFallbacks:    s.TypedColumnFallbacks,
		VectorScratchDecodes:          s.VectorScratchDecodes,
		OpenSearcherCalls:             s.OpenSearcherCalls,
		OpenSetupInTimedLoop:          s.OpenSetupInTimedLoop,
		ResponseOwnedResultAllocs:     s.ResponseOwnedResultAllocs,
		HNSWSearchPackCacheHits:       s.HNSWSearchPackCacheHits,
		HNSWSearchPackCacheMisses:     s.HNSWSearchPackCacheMisses,
		HNSWSearchPackCacheWaits:      s.HNSWSearchPackCacheWaits,
		HNSWSearchPackCacheBuilds:     s.HNSWSearchPackCacheBuilds,
		LiveANN: VectorIndexSearchLiveANNDiagnostics{
			Enabled:      s.SearchRouteNativeRuntime > 0,
			FullRebuilds: s.NativeRuntimeFullRebuilds,
		},
	}
}

// RouteKind reports the low-cardinality route selected for this search.
func (s VectorIndexSearchStats) RouteKind() VectorIndexSearchRouteKind {
	switch {
	case s.SearchRouteNativeRuntime > 0:
		return VectorIndexSearchRouteNativeRuntime
	case s.SearchRouteQuantizedOnly > 0:
		return VectorIndexSearchRouteQuantizedOnly
	case s.SearchRouteQuantizedRerank > 0:
		return VectorIndexSearchRouteQuantizedRerank
	case s.SearchRouteHNSWSearchPack > 0:
		return VectorIndexSearchRouteExactHNSWSearchPackV1
	case s.SearchRouteColumnGraphPrepared > 0:
		return VectorIndexSearchRouteColumnGraphPrepared
	case s.SearchRouteColumnGraphFallback > 0:
		return VectorIndexSearchRouteColumnGraphFallback
	default:
		return VectorIndexSearchRouteUnknown
	}
}

// HNSWSearchPackStatus reports exact hnsw_search_pack_v1 health for this search.
func (s VectorIndexSearchStats) HNSWSearchPackStatus() VectorIndexSearchHNSWSearchPackStatus {
	switch {
	case s.HNSWSearchPackActive > 0:
		return VectorIndexSearchHNSWSearchPackStatusActive
	case s.HNSWSearchPackClosed > 0:
		return VectorIndexSearchHNSWSearchPackStatusClosed
	case s.HNSWSearchPackStale > 0:
		return VectorIndexSearchHNSWSearchPackStatusStale
	case s.HNSWSearchPackInvalid > 0:
		return VectorIndexSearchHNSWSearchPackStatusInvalid
	case s.HNSWSearchPackMissing > 0:
		return VectorIndexSearchHNSWSearchPackStatusMissing
	default:
		return VectorIndexSearchHNSWSearchPackStatusNone
	}
}

// FallbackReason reports the first low-cardinality fallback reason visible in stats.
func (s VectorIndexSearchStats) FallbackReason() VectorIndexSearchFallbackReason {
	switch {
	case s.HNSWSearchPackClosed > 0:
		return VectorIndexSearchFallbackReasonHNSWSearchPackClosed
	case s.HNSWSearchPackStale > 0:
		return VectorIndexSearchFallbackReasonHNSWSearchPackStale
	case s.HNSWSearchPackInvalid > 0:
		return VectorIndexSearchFallbackReasonHNSWSearchPackInvalid
	case s.HNSWSearchPackMissing > 0:
		return VectorIndexSearchFallbackReasonHNSWSearchPackMissing
	case s.HNSWSearchPackFallbacks > 0:
		return VectorIndexSearchFallbackReasonHNSWSearchPackUnavailable
	case s.GraphRowFallbacks > 0:
		return VectorIndexSearchFallbackReasonGraphRowFallback
	case s.TypedColumnFallbacks > 0:
		return VectorIndexSearchFallbackReasonTypedColumnVectorFallback
	case s.VectorScratchDecodes > 0:
		return VectorIndexSearchFallbackReasonVectorScratchDecode
	case s.SearchRouteColumnGraphFallback > 0:
		return VectorIndexSearchFallbackReasonColumnGraphFallback
	default:
		return VectorIndexSearchFallbackReasonNone
	}
}

// NoDocumentGuardrailsOK reports whether shared no-document guardrail counters are clear.
func (s VectorIndexSearchStats) NoDocumentGuardrailsOK() bool {
	return s.DocumentsFetched == 0 &&
		s.GraphRowFallbacks == 0 &&
		s.TypedColumnFallbacks == 0 &&
		s.VectorScratchDecodes == 0
}

// ExactHNSWSearchPackNoDocumentRoute reports the exact FP32 no-document pack route guardrail.
func (s VectorIndexSearchStats) ExactHNSWSearchPackNoDocumentRoute() bool {
	return vectorIndexSearchStatsAreBufferedNoDocumentPackRoute(s)
}

// VectorIndexSearchBuffer is caller-owned reusable response storage for
// VectorIndexSearcher.SearchWithBuffer and Collection.SearchVectorIndexWithBuffer.
// It is intended for steady-state no-document searches that need to avoid
// per-call response allocation. Reuse a warmed buffer to keep result-ID searches
// allocation-free once open/prepared state is outside the measured boundary.
//
// A VectorIndexSearchBuffer is not safe for concurrent use. Do not reuse or
// reset the same buffer while any caller still needs a response previously
// returned from it. The response Results slice and each result ID returned by
// buffered search APIs alias this buffer and remain valid only until the same
// buffer is reused or Reset is called. Parallel callers should use independent
// searcher/buffer pairs per worker.
type VectorIndexSearchBuffer struct {
	results                 []VectorIndexSearchResult
	idBytes                 []byte
	baseResults             []VectorIndexSearchResult
	baseIDBytes             []byte
	deltaResults            []VectorIndexSearchResult
	deltaIDBytes            []byte
	searchScratch           columnVectorGraphNativeSearchScratch
	nativeSearchScratch     vectorIndexSearchScratch
	nativeSearchWorkEnabled bool
	nativeSearchWork        vectorIndexNativeSearchWork
}

// Reset clears the buffer's current response view while retaining reusable
// capacity. Any response previously returned by a buffered search API with this
// buffer must be considered invalid after Reset returns.
func (b *VectorIndexSearchBuffer) Reset() {
	if b == nil {
		return
	}
	clear(b.results)
	b.nativeSearchWork = vectorIndexNativeSearchWork{}
	b.resetView()
}

func (b *VectorIndexSearchBuffer) resetView() {
	b.results = b.results[:0]
	b.idBytes = b.idBytes[:0]
	b.baseResults = b.baseResults[:0]
	b.baseIDBytes = b.baseIDBytes[:0]
	b.deltaResults = b.deltaResults[:0]
	b.deltaIDBytes = b.deltaIDBytes[:0]
}

// VectorIndexSearcher is a reusable, snapshot-bound vector index search handle.
// It is not concurrency-safe; parallel query workers should open independent
// searchers/buffers. Current-format column_graph searchers opened over the same
// immutable generation share prepared mmap/resource views internally, so worker
// isolation applies to mutable scratch/result buffers rather than immutable
// typed-column assets. Close and reopen the searcher after writes/rebuilds when
// callers need the newest column_graph generation.
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
	routeStats   vectorIndexSearchRouteStats
	closed       bool
}

type vectorIndexSearchRouteStats struct {
	SearchRouteColumnGraphPrepared uint64
	SearchRouteColumnGraphFallback uint64
	SearchRouteHNSWSearchPack      uint64
	HNSWSearchPackActive           uint64
	HNSWSearchPackMissing          uint64
	HNSWSearchPackInvalid          uint64
	HNSWSearchPackStale            uint64
	HNSWSearchPackClosed           uint64
	HNSWSearchPackFallbacks        uint64
	HNSWSearchPackMmapDirect       uint64
	HNSWSearchPackHeapCopy         uint64
	HNSWSearchPackOpenNanos        uint64
	HNSWSearchPackMappedBytes      uint64
	HNSWSearchPackHeapCopyBytes    uint64
	HNSWSearchPackActiveHandles    int64
}

func vectorIndexSearchRouteStatsForColumnGraphReader(reader *columnVectorGraphPhysicalRowReader) vectorIndexSearchRouteStats {
	stats := vectorIndexSearchRouteStats{}
	if reader != nil {
		stats.add(reader.hnswSearchPack.routeStats(reader.hnswSearchPackStatus, reader.hnswSearchPackOpenNanos))
	} else {
		stats.applyHNSWSearchPackStatus(columnHNSWSearchPackPreparedStatusMissing)
	}
	if reader != nil && reader.preparedSearch != nil && reader.preparedSearch.ready() {
		stats.SearchRouteColumnGraphPrepared = 1
		return stats
	}
	stats.SearchRouteColumnGraphFallback = 1
	return stats
}

func vectorIndexSearchRouteStatsForColumnGraphSearchWithBufferFallback(cached vectorIndexSearchRouteStats) vectorIndexSearchRouteStats {
	stats := cached
	stats.SearchRouteHNSWSearchPack = 0
	stats.HNSWSearchPackFallbacks = 1
	return stats
}

func vectorIndexSearchRouteStatsForHNSWSearchPackRoute(cached vectorIndexSearchRouteStats) vectorIndexSearchRouteStats {
	stats := cached
	stats.SearchRouteColumnGraphPrepared = 0
	stats.SearchRouteColumnGraphFallback = 0
	stats.SearchRouteHNSWSearchPack = 1
	return stats
}

func vectorIndexSearchRouteStatsForHNSWSearchPackFastStatus(cached vectorIndexSearchRouteStats, status columnHNSWSearchPackPreparedStatus) vectorIndexSearchRouteStats {
	stats := cached
	switch status {
	case columnHNSWSearchPackPreparedStatusDirect, columnHNSWSearchPackPreparedStatusHeap:
		return stats
	}
	stats.clearHNSWSearchPackAvailability()
	stats.applyHNSWSearchPackStatus(status)
	if status == columnHNSWSearchPackPreparedStatusMissing || status == "" {
		stats.HNSWSearchPackFallbacks = 1
	}
	return stats
}

func vectorIndexSearchRouteStatsForColumnGraphQuantized(cached vectorIndexSearchRouteStats) vectorIndexSearchRouteStats {
	stats := cached
	stats.SearchRouteHNSWSearchPack = 0
	stats.clearHNSWSearchPackCounters()
	if stats.SearchRouteColumnGraphPrepared == 0 && stats.SearchRouteColumnGraphFallback == 0 {
		stats.SearchRouteColumnGraphFallback = 1
	}
	return stats
}

func vectorIndexSearchRouteStatsForQueryMode(cached vectorIndexSearchRouteStats, queryMode columnVectorGraphNativeSearchQueryMode) vectorIndexSearchRouteStats {
	if queryMode.quantized() {
		return vectorIndexSearchRouteStatsForColumnGraphQuantized(cached)
	}
	return cached
}

func (r *vectorIndexSearchRouteStats) clearHNSWSearchPackAvailability() {
	if r == nil {
		return
	}
	openNanos := r.HNSWSearchPackOpenNanos
	r.clearHNSWSearchPackCounters()
	r.HNSWSearchPackOpenNanos = openNanos
}

func (r *vectorIndexSearchRouteStats) clearHNSWSearchPackCounters() {
	if r == nil {
		return
	}
	r.HNSWSearchPackActive = 0
	r.HNSWSearchPackMissing = 0
	r.HNSWSearchPackInvalid = 0
	r.HNSWSearchPackStale = 0
	r.HNSWSearchPackClosed = 0
	r.HNSWSearchPackFallbacks = 0
	r.HNSWSearchPackMmapDirect = 0
	r.HNSWSearchPackHeapCopy = 0
	r.HNSWSearchPackOpenNanos = 0
	r.HNSWSearchPackMappedBytes = 0
	r.HNSWSearchPackHeapCopyBytes = 0
	r.HNSWSearchPackActiveHandles = 0
}

func (s *VectorIndexSearcher) hnswSearchPackSearchWithBufferRoute(queryMode columnVectorGraphNativeSearchQueryMode, statsMode columnVectorGraphNativeSearchStatsMode) (*columnHNSWSearchPackPreparedView, vectorIndexSearchRouteStats, bool) {
	cached := s.routeStats
	if queryMode.quantized() {
		return nil, vectorIndexSearchRouteStatsForColumnGraphQuantized(cached), false
	}
	reader := s.reader
	pack := (*columnHNSWSearchPackPreparedView)(nil)
	status := columnHNSWSearchPackPreparedStatusMissing
	if reader != nil {
		pack = reader.hnswSearchPack
		status = pack.fastStatus(reader.hnswSearchPackStatus)
	}
	if status != columnHNSWSearchPackPreparedStatusDirect && status != columnHNSWSearchPackPreparedStatusHeap {
		return nil, vectorIndexSearchRouteStatsForHNSWSearchPackFastStatus(cached, status), false
	}
	if queryMode != columnVectorGraphNativeSearchQueryModeExact || !columnHNSWSearchPackStatsModeSupportedForSearch(statsMode) || pack == nil {
		return nil, vectorIndexSearchRouteStatsForColumnGraphSearchWithBufferFallback(cached), false
	}
	return pack, vectorIndexSearchRouteStatsForHNSWSearchPackRoute(cached), true
}

func (s *VectorIndexSearcher) scalarU8PreparedTraversalSearchWithBufferRoute(queryMode columnVectorGraphNativeSearchQueryMode, statsMode columnVectorGraphNativeSearchStatsMode, quantizedIndexName string) (*columnHNSWSearchPackPreparedView, bool) {
	if s == nil {
		return nil, false
	}
	return collectionScalarU8PreparedTraversalPackForReader(s.reader, queryMode, statsMode, quantizedIndexName)
}

func (r vectorIndexSearchRouteStats) apply(stats *VectorIndexSearchStats) {
	if stats == nil {
		return
	}
	stats.SearchRouteColumnGraphPrepared = r.SearchRouteColumnGraphPrepared
	stats.SearchRouteColumnGraphFallback = r.SearchRouteColumnGraphFallback
	stats.SearchRouteHNSWSearchPack = r.SearchRouteHNSWSearchPack
	stats.HNSWSearchPackActive = r.HNSWSearchPackActive
	stats.HNSWSearchPackMissing = r.HNSWSearchPackMissing
	stats.HNSWSearchPackInvalid = r.HNSWSearchPackInvalid
	stats.HNSWSearchPackStale = r.HNSWSearchPackStale
	stats.HNSWSearchPackClosed = r.HNSWSearchPackClosed
	stats.HNSWSearchPackFallbacks = r.HNSWSearchPackFallbacks
	stats.HNSWSearchPackMmapDirect = r.HNSWSearchPackMmapDirect
	stats.HNSWSearchPackHeapCopy = r.HNSWSearchPackHeapCopy
	stats.HNSWSearchPackOpenNanos = r.HNSWSearchPackOpenNanos
	stats.HNSWSearchPackMappedBytes = r.HNSWSearchPackMappedBytes
	stats.HNSWSearchPackHeapCopyBytes = r.HNSWSearchPackHeapCopyBytes
	stats.HNSWSearchPackActiveHandles = r.HNSWSearchPackActiveHandles
}

// SearchVectorIndex searches a collection vector index through the public
// collection lifecycle. V4 wires only explicit column_graph indexes to the
// native physical column reader; native_runtime remains reported as native
// rather than silently falling back or pretending to use column storage. When
// availability or staleness checks fail, the returned response may still carry
// the index status so callers can distinguish rebuild-needed/unavailable cases.
//
// With IncludeDocuments=false, SearchVectorIndex returns response-owned result
// IDs and scores only and must not materialize documents. With
// IncludeDocuments=true, document fetch happens after top-k selection and is
// reported through document counters. Exact no-document calls use the
// collection-owned prepared hnsw_search_pack_v1 cache when healthy; unsupported
// shapes and unavailable packs fall back to the one-shot searcher path. Use
// SearchVectorIndexWithBuffer for the zero-allocation caller-owned result-buffer
// seam, and OpenVectorIndexSearcher plus SearchWithBuffer when callers can keep
// open/prepared state outside the timed query boundary. Callers that want a
// split search/fetch shape can run a no-document search first, then use
// CollectionReadView.FetchDocumentsForVectorIndexSearchResults as a separate
// materialization phase with separate counters.
func (c *Collection) SearchVectorIndex(opts VectorIndexSearchOptions) (VectorIndexSearchResponse, error) {
	if err := validateVectorIndexSearchRequest(opts.TopK, opts.EfSearch); err != nil {
		return VectorIndexSearchResponse{}, err
	}
	if collectionSearchVectorIndexCanUseBufferedNoDocumentRoute(opts) {
		response, err := c.searchVectorIndexPreparedNoDocumentOwned(opts)
		if err == nil {
			return response, nil
		}
		if !errors.Is(err, ErrVectorIndexSearchUnavailable) && !errors.Is(err, ErrIndexNotFound) {
			return response, err
		}
	}
	return c.searchVectorIndexOneShot(opts)
}

func acquireCollectionSearchVectorIndexResponseBuffer() *VectorIndexSearchBuffer {
	if buffer, ok := collectionSearchVectorIndexResponseBufferPool.Get().(*VectorIndexSearchBuffer); ok && buffer != nil {
		return buffer
	}
	return &VectorIndexSearchBuffer{}
}

func releaseCollectionSearchVectorIndexResponseBuffer(buffer *VectorIndexSearchBuffer) {
	if buffer == nil {
		return
	}
	buffer.Reset()
	collectionSearchVectorIndexResponseBufferPool.Put(buffer)
}

func (c *Collection) searchVectorIndexPreparedNoDocumentOwned(opts VectorIndexSearchOptions) (VectorIndexSearchResponse, error) {
	var response VectorIndexSearchResponse
	if c == nil {
		return response, errCollectionNil
	}
	if c.db == nil {
		return response, errCollectionDBNil
	}
	if err := c.flushBufferedWrites(); err != nil {
		return response, err
	}
	statsMode, err := columnVectorGraphNativeSearchStatsModeFromPublic(opts.StatsMode)
	if err != nil {
		return response, err
	}
	queryMode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	if err != nil {
		return response, err
	}
	if queryMode != columnVectorGraphNativeSearchQueryModeExact || !columnHNSWSearchPackStatsModeSupportedForSearch(statsMode) {
		return response, fmt.Errorf("%w: vector index %q SearchVectorIndex requires exact no-document hnsw_search_pack_v1 route", ErrVectorIndexSearchUnavailable, opts.IndexName)
	}
	slot := collectionVectorIndexPreparedSearchCacheSlotForOptions(opts, queryMode)
	buffer := acquireCollectionSearchVectorIndexResponseBuffer()
	var lastResponse VectorIndexSearchResponse
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		prepared, response, acquireStats, err := c.acquireCollectionVectorIndexPreparedSearch(opts)
		if err != nil {
			releaseCollectionSearchVectorIndexResponseBuffer(buffer)
			acquireStats.apply(&response.Stats)
			return response, err
		}
		response, err = prepared.SearchOwnedNoDocuments(opts, statsMode, &buffer.searchScratch)
		acquireStats.apply(&response.Stats)
		healthyPackRoute := vectorIndexSearchStatsAreBufferedNoDocumentPackRoute(response.Stats)
		if err == nil && healthyPackRoute {
			releaseCollectionSearchVectorIndexResponseBuffer(buffer)
			return response, nil
		}
		if err != nil && healthyPackRoute {
			releaseCollectionSearchVectorIndexResponseBuffer(buffer)
			return response, err
		}
		lastResponse, lastErr = response, err
		lastResponse.Results = nil
		c.invalidateCollectionVectorIndexPreparedSearch(slot, prepared)
	}
	releaseCollectionSearchVectorIndexResponseBuffer(buffer)
	if lastErr != nil {
		return lastResponse, lastErr
	}
	return lastResponse, fmt.Errorf("%w: vector index %q SearchVectorIndex requires exact no-document hnsw_search_pack_v1 route", ErrVectorIndexSearchUnavailable, opts.IndexName)
}

func markVectorIndexSearchResponseOwnedResultAllocs(response *VectorIndexSearchResponse) {
	if response != nil && len(response.Results) > 0 {
		response.Stats.ResponseOwnedResultAllocs = 1
	}
}

func markCollectionVectorIndexOneShotOpenSetup(stats *VectorIndexSearchStats) {
	if stats == nil {
		return
	}
	stats.OpenSearcherCalls = 1
	stats.OpenSetupInTimedLoop = 1
}

func collectionSearchVectorIndexCanUseBufferedNoDocumentRoute(opts VectorIndexSearchOptions) bool {
	if opts.IncludeDocuments || vectorIndexDocumentFetchOptionsNonZero(opts.DocumentFetchOptions) {
		return false
	}
	if opts.Filter != nil || opts.IndexRangeFilter != nil {
		return false
	}
	if opts.FetchMultiplier != 0 || opts.ExactFilterMaxDocs != 0 || opts.DisableExactFallback {
		return false
	}
	if opts.QueryMode != "" && opts.QueryMode != VectorIndexQueryModeExact {
		return false
	}
	if opts.QuantizedIndexName != "" || opts.QuantizedRerankCandidates != 0 {
		return false
	}
	statsMode, err := columnVectorGraphNativeSearchStatsModeFromPublic(opts.StatsMode)
	if err != nil || !columnHNSWSearchPackStatsModeSupportedForSearch(statsMode) {
		return false
	}
	queryMode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	return err == nil && queryMode == columnVectorGraphNativeSearchQueryModeExact
}

func (c *Collection) searchVectorIndexOneShot(opts VectorIndexSearchOptions) (VectorIndexSearchResponse, error) {
	searcher, response, err := c.openVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        opts.IndexName,
		MaxDecodedBlocks: opts.MaxDecodedBlocks,
	})
	if err != nil {
		markCollectionVectorIndexOneShotOpenSetup(&response.Stats)
		return response, err
	}
	response, err = searcher.Search(VectorIndexSearcherSearchOptions{
		Query:                     opts.Query,
		QueryMode:                 opts.QueryMode,
		QuantizedIndexName:        opts.QuantizedIndexName,
		QuantizedRerankCandidates: opts.QuantizedRerankCandidates,
		TopK:                      opts.TopK,
		EfSearch:                  opts.EfSearch,
		IncludeDocuments:          opts.IncludeDocuments,
		DocumentFetchOptions:      opts.DocumentFetchOptions,
		StatsMode:                 opts.StatsMode,
		scoreBatchMode:            opts.scoreBatchMode,
	})
	markCollectionVectorIndexOneShotOpenSetup(&response.Stats)
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

// SearchVectorIndexWithBuffer searches a collection vector index through a
// no-document high-QPS seam using caller-owned result storage. Query is read
// synchronously and is neither mutated nor retained after this method returns.
// Returned Results and result IDs alias buffer and remain valid only until buffer is reused or
// Reset is called. The same buffer must not be reused concurrently; parallel
// callers should use independent buffers.
// Native-runtime responses also carry an opaque process-local publication
// identity consumed by OpenCollectionReadViewForVectorIndexSearch; it is not
// serialized or exposed as a public token.
//
// This method supports exact/zero QueryMode through the exact
// hnsw_search_pack_v1 route, and explicit quantized_only / quantized_rerank
// modes through a collection-owned prepared quantized route selected by
// QuantizedIndexName; rabitq_1bit uses the prepared hnsw_search_pack_v1
// score-plane traversal when eligible. It intentionally fails closed for
// document materialization, projections, filters, benchmark-debug stats mode,
// missing or invalid prepared assets, stale route identities, and
// legacy/fallback routes. Healthy prepared state is opened once into the
// collection cache keyed by the current collection/vector-index/score-plane
// manifest identity; steady-state searches reuse that prepared state and
// caller-owned result buffer instead of opening a VectorIndexSearcher per call.
func (c *Collection) SearchVectorIndexWithBuffer(opts VectorIndexSearchOptions, buffer *VectorIndexSearchBuffer) (VectorIndexSearchResponse, error) {
	if err := validateCollectionVectorIndexSearchWithBufferOptions(opts, buffer); err != nil {
		return VectorIndexSearchResponse{}, err
	}
	if c == nil {
		buffer.Reset()
		return VectorIndexSearchResponse{}, errCollectionNil
	}
	if c.db == nil {
		buffer.Reset()
		return VectorIndexSearchResponse{}, errCollectionDBNil
	}
	def, found, catalogCurrent := c.cachedVectorIndexDefinitionForCurrentState(opts.IndexName)
	if found && catalogCurrent && vectorIndexDefinitionUsesNativeRuntime(def) {
		return c.searchNativeRuntimeVectorIndexWithBuffer(def, opts, buffer)
	}
	if err := c.flushBufferedWrites(); err != nil {
		buffer.Reset()
		return VectorIndexSearchResponse{}, err
	}
	def, found, catalogCurrent = c.cachedVectorIndexDefinitionForCurrentState(opts.IndexName)
	if !catalogCurrent {
		snap := c.db.AcquireSnapshot()
		if snap == nil {
			buffer.Reset()
			return VectorIndexSearchResponse{}, backenddb.ErrClosed
		}
		catalog, err := c.catalogForSnapshot(snap)
		_ = snap.Close()
		if err != nil {
			buffer.Reset()
			return VectorIndexSearchResponse{}, err
		}
		if catalog == nil {
			buffer.Reset()
			return VectorIndexSearchResponse{}, errCollectionNotFound
		}
		def, found = findVectorIndex(catalog.meta.VectorIndexes, opts.IndexName)
	}
	if found && vectorIndexDefinitionUsesNativeRuntime(def) {
		return c.searchNativeRuntimeVectorIndexWithBuffer(def, opts, buffer)
	}
	statsMode, err := columnVectorGraphNativeSearchStatsModeFromPublic(opts.StatsMode)
	if err != nil {
		buffer.Reset()
		return VectorIndexSearchResponse{}, err
	}
	queryMode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	if err != nil {
		buffer.Reset()
		return VectorIndexSearchResponse{}, err
	}
	if queryMode == columnVectorGraphNativeSearchQueryModeExact && !columnHNSWSearchPackStatsModeSupportedForSearch(statsMode) {
		buffer.Reset()
		return VectorIndexSearchResponse{}, fmt.Errorf("%w: vector index %q SearchVectorIndexWithBuffer requires the exact no-document hnsw_search_pack_v1 route for the selected stats mode; unsupported stats options must use SearchVectorIndex or OpenVectorIndexSearcher", ErrVectorIndexSearchUnavailable, opts.IndexName)
	}
	slot := collectionVectorIndexPreparedSearchCacheSlotForOptions(opts, queryMode)
	if queryMode.quantized() {
		prepared, response, acquireStats, err := c.acquireCollectionVectorIndexPreparedSearch(opts)
		if err != nil {
			buffer.Reset()
			acquireStats.apply(&response.Stats)
			return response, err
		}
		response, err = prepared.SearchQuantizedWithBuffer(opts, buffer)
		acquireStats.apply(&response.Stats)
		var healthyQuantizedRoute bool
		// Keep quantized_only on its narrow hot-path guardrail; quantized_rerank
		// validates pack-native exact counters separately below.
		if queryMode == columnVectorGraphNativeSearchQueryModeQuantizedOnly {
			healthyQuantizedRoute = vectorIndexSearchStatsAreBufferedNoDocumentQuantizedOnlyRoute(response.Stats, opts)
		} else {
			healthyQuantizedRoute = vectorIndexSearchStatsAreBufferedNoDocumentQuantizedRoute(response.Stats, queryMode, opts, prepared.dimensions)
		}
		if err == nil && healthyQuantizedRoute {
			return response, nil
		}
		if err != nil {
			if errors.Is(err, ErrVectorIndexSearchUnavailable) && !healthyQuantizedRoute {
				c.invalidateCollectionVectorIndexPreparedSearch(slot, prepared)
			}
			return response, err
		}
		resetBufferedVectorIndexSearchResponse(&response, buffer)
		c.invalidateCollectionVectorIndexPreparedSearch(slot, prepared)
		return response, fmt.Errorf("%w: vector index %q SearchVectorIndexWithBuffer quantized route did not satisfy no-document quantized guardrails; rebuild the vector index", ErrVectorIndexSearchUnavailable, opts.IndexName)
	}
	var lastResponse VectorIndexSearchResponse
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		prepared, response, acquireStats, err := c.acquireCollectionVectorIndexPreparedSearch(opts)
		if err != nil {
			buffer.Reset()
			acquireStats.apply(&response.Stats)
			return response, err
		}
		response, err = prepared.SearchWithBuffer(opts, statsMode, buffer)
		acquireStats.apply(&response.Stats)
		healthyPackRoute := vectorIndexSearchStatsAreBufferedNoDocumentPackRoute(response.Stats)
		if err == nil && healthyPackRoute {
			return response, nil
		}
		if err != nil && healthyPackRoute {
			return response, err
		}
		lastResponse, lastErr = response, err
		lastResponse.Results = nil
		resetBufferedVectorIndexSearchResponse(&response, buffer)
		c.invalidateCollectionVectorIndexPreparedSearch(slot, prepared)
	}
	if lastErr != nil {
		return lastResponse, lastErr
	}
	return lastResponse, fmt.Errorf("%w: vector index %q SearchVectorIndexWithBuffer requires a healthy exact no-document hnsw_search_pack_v1 route; rebuild the vector index or use SearchVectorIndex for the response-owned convenience path", ErrVectorIndexSearchUnavailable, opts.IndexName)
}

func (c *Collection) searchNativeRuntimeVectorIndexWithBuffer(def VectorIndexDefinition, opts VectorIndexSearchOptions, buffer *VectorIndexSearchBuffer) (VectorIndexSearchResponse, error) {
	response := VectorIndexSearchResponse{
		IndexName: def.Name,
		Strategy:  def.Strategy,
		Path:      VectorIndexSearchPathNativeRuntime,
		Status: VectorIndexStatus{
			Definition: def,
			Name:       def.Name,
			Strategy:   def.Strategy,
			State:      VectorIndexStateNativeRuntime,
			Reason:     VectorIndexReasonNativeRuntime,
		},
	}
	if def.Metric != VectorMetricCosine || def.Encoding != VectorIndexEncodingFloat32 {
		buffer.Reset()
		return response, fmt.Errorf("%w: native_runtime vector index %q buffered search supports only cosine float32", ErrVectorIndexSearchUnavailable, def.Name)
	}
	if opts.StatsMode == VectorIndexSearchStatsModeDefault || opts.StatsMode == VectorIndexSearchStatsModeFullDiagnostics {
		buffer.Reset()
		mode := string(opts.StatsMode)
		if mode == "" {
			mode = "default"
		}
		return response, collectionVectorIndexWithBufferUnsupportedOptionError("StatsMode="+mode, "native_runtime full-diagnostics counters are not implemented; use production/minimal mode with CPU profiles")
	}
	if opts.StatsMode == VectorIndexSearchStatsModeWorkAccounting {
		buffer.Reset()
		return response, collectionVectorIndexWithBufferUnsupportedOptionError("StatsMode=work_accounting", "native_runtime work-accounting counters are not implemented; use production/minimal mode with CPU profiles")
	}
	queryMode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	if err != nil {
		buffer.Reset()
		return response, err
	}
	if queryMode != columnVectorGraphNativeSearchQueryModeExact {
		buffer.Reset()
		return response, fmt.Errorf("%w: native_runtime vector index %q does not support quantized query modes", ErrVectorIndexSearchUnavailable, def.Name)
	}
	for attempt := 0; attempt < 2; attempt++ {
		index, load, err := c.loadNativeRuntimeVectorIndexForSearch(def)
		if err != nil {
			buffer.Reset()
			response.Status.ExactFallbackReason = load.ExactFallbackReason
			return response, err
		}
		if index == nil || !load.Loaded {
			buffer.Reset()
			response.Status.ExactFallbackReason = load.ExactFallbackReason
			return response, fmt.Errorf("%w: native_runtime vector index %q is not loaded: %s", ErrVectorIndexSearchUnavailable, def.Name, load.ExactFallbackReason)
		}
		publicationMu := index.nativePublicationLock()
		publicationMu.RLock()
		if c.registeredVectorIndex(def.Name) != index {
			publicationMu.RUnlock()
			continue
		}
		var searchState vectorIndexNativeSearchState
		response.Results, searchState, err = index.searchGraphOnlyWithBuffer(opts.Query, opts.TopK, opts.EfSearch, buffer)
		if err == nil {
			response.Status.Loaded = true
			response.Status.Registered = true
			response.Status.RootName = load.RootName
			response.Status.RootID = load.RootID
			response.Status.NativeRootLoaded = load.RootID != 0
			response.Status.NativeRootBytes = load.BytesDisk
			response.Status.RebuildNeeded = searchState.rebuildNeeded
			response.Stats.SearchRouteNativeRuntime = 1
			response.Stats.NativeRuntimeFullRebuilds = searchState.fullRebuilds
			response.Stats.CandidateRows = uint64(searchState.liveDocs)
			response.visibility = vectorIndexSearchVisibility{
				runtime:                  index,
				collectionName:           c.collectionName(),
				indexName:                def.Name,
				strategy:                 def.Strategy,
				schemaGeneration:         def.SchemaGeneration,
				mutationSeq:              searchState.mutationSeq,
				sourceDocumentGeneration: searchState.sourceDocumentGeneration,
			}
		} else if !index.hasValidSourceDocumentRoots() {
			response.Status.ExactFallbackReason = vectorIndexFallbackStaleDocumentRoot
		}
		publicationMu.RUnlock()
		return response, err
	}
	buffer.Reset()
	response.Status.ExactFallbackReason = vectorIndexFallbackStaleRuntimeIndex
	return response, fmt.Errorf("%w: native_runtime vector index %q changed during search validation", ErrVectorIndexSearchUnavailable, def.Name)
}

func (c *Collection) loadNativeRuntimeVectorIndexForSearch(def VectorIndexDefinition) (*VectorIndex, VectorIndexLoadStatus, error) {
	if index := c.registeredVectorIndex(def.Name); index != nil {
		if status, ok := c.publishedNativeSearchLoadStatusDuringMutation(def, index); ok {
			return index, status, nil
		}
		validated, status, err := c.validateRegisteredNativeRuntimeVectorIndexForSearch(def, index)
		if err != nil {
			if status.ExactFallbackReason == vectorIndexFallbackStaleDocumentRoot {
				if status, ok := c.publishedNativeSearchLoadStatusDuringMutation(def, index); ok {
					return index, status, nil
				}
				unlockCoverage := c.lockVectorIndexCoveragePersistence()
				current := c.registeredVectorIndex(def.Name)
				if current != nil {
					validated, status, err = c.validateRegisteredNativeRuntimeVectorIndexForSearch(def, current)
				}
				unlockCoverage()
				if current != nil {
					return validated, status, err
				}
			}
			if current := c.registeredVectorIndex(def.Name); current != nil && current != index {
				return c.validateRegisteredNativeRuntimeVectorIndexForSearch(def, current)
			}
		}
		return validated, status, err
	}
	unlockLoad := c.lockNativeVectorIndexLoad()
	defer unlockLoad()
	if index := c.registeredVectorIndex(def.Name); index != nil {
		return c.validateRegisteredNativeRuntimeVectorIndexForSearch(def, index)
	}
	index, status, err := c.LoadNativeVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		return nil, status, err
	}
	return index, status, nil
}

func (c *Collection) publishedNativeSearchLoadStatusDuringMutation(def VectorIndexDefinition, index *VectorIndex) (VectorIndexLoadStatus, bool) {
	if c == nil {
		return VectorIndexLoadStatus{}, false
	}
	if c.nativeVectorIndexMutationActive() {
		return c.publishedNativeSearchLoadStatus(def, index)
	}
	if c.writeDomain != nil {
		if coord := c.writeDomain.schemaCoordinator; coord != nil {
			if baseline := coord.nativeVectorBaseline.Load(); baseline != nil {
				if !index.publishedSearchViewCoversSourceDocumentGeneration(*baseline) {
					return VectorIndexLoadStatus{}, false
				}
				return c.publishedNativeSearchLoadStatus(def, index)
			}
		}
	}
	return VectorIndexLoadStatus{}, false
}

func (c *Collection) publishedNativeSearchLoadStatus(def VectorIndexDefinition, index *VectorIndex) (VectorIndexLoadStatus, bool) {
	status, ok := index.publishedNativeSearchLoadStatus(def)
	if !ok || c == nil || c.db == nil || status.RootName == "" {
		return VectorIndexLoadStatus{}, false
	}
	state, ok := c.db.StateToken()
	if !ok {
		return VectorIndexLoadStatus{}, false
	}
	currentDef, rootID, found, current := c.cachedVectorIndexForState(def.Name, status.RootName, state)
	if !current || !found || !vectorIndexDefinitionValuesEqual(def, currentDef) || rootID != status.RootID {
		return VectorIndexLoadStatus{}, false
	}
	return status, true
}

func (c *Collection) cachedVectorIndexDefinitionForCurrentState(name string) (VectorIndexDefinition, bool, bool) {
	if c == nil || c.db == nil {
		return VectorIndexDefinition{}, false, false
	}
	state, ok := c.db.StateToken()
	if !ok {
		return VectorIndexDefinition{}, false, false
	}
	def, _, found, current := c.cachedVectorIndexForState(name, "", state)
	return def, found, current
}

func (c *Collection) cachedVectorIndexForState(name, rootName string, state backenddb.StateToken) (VectorIndexDefinition, uint64, bool, bool) {
	c.catalogMu.RLock()
	catalog := c.catalog
	current := catalog != nil &&
		state.SystemRootPageID != 0 &&
		c.catalogSystemRoot == state.SystemRootPageID &&
		(c.catalogCommitSeq == state.CommitSeq || c.canReuseCachedCatalogAcrossDataOnlyCommits(catalog))
	c.catalogMu.RUnlock()
	if !current && catalog != nil && c.nativeVectorIndexMutationActive() && c.writeDomain != nil {
		if c.writeDomain.mu.TryRLock() {
			if currentCatalog := cachedWriteDomainCatalogForStateLocked(c.writeDomain, state.SystemRootPageID, state.CommitSeq); currentCatalog != nil {
				catalog, current = currentCatalog, true
			}
			c.writeDomain.mu.RUnlock()
		} else if def, ok := findVectorIndex(catalog.meta.VectorIndexes, name); ok && vectorIndexDefinitionUsesNativeRuntime(def) {
			return def, catalog.rootID(rootName), true, true
		}
	}
	if !current {
		catalog = cachedWriteDomainCatalogForState(c.writeDomain, state.SystemRootPageID, state.CommitSeq)
		if catalog == nil {
			return VectorIndexDefinition{}, 0, false, false
		}
		c.catalogMu.Lock()
		if c.catalogCommitSeq <= state.CommitSeq {
			c.catalog = catalog
			c.catalogSystemRoot = state.SystemRootPageID
			c.catalogCommitSeq = state.CommitSeq
		}
		c.catalogMu.Unlock()
	}
	if catalog == nil {
		return VectorIndexDefinition{}, 0, false, false
	}
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, name)
	if !ok {
		return VectorIndexDefinition{}, 0, false, true
	}
	return def, catalog.rootID(rootName), true, true
}

func (c *Collection) validateRegisteredNativeRuntimeVectorIndexForSearch(def VectorIndexDefinition, index *VectorIndex) (*VectorIndex, VectorIndexLoadStatus, error) {
	if index == nil {
		return nil, VectorIndexLoadStatus{ExactFallbackReason: "nil_index"}, fmt.Errorf("%w: native_runtime vector index %q definition mismatch: nil_index", ErrVectorIndexSearchUnavailable, def.Name)
	}
	publicationMu := index.nativePublicationLock()
	publicationMu.RLock()
	defer publicationMu.RUnlock()
	rootName := index.nativeRootName
	if state, ok := c.db.StateToken(); ok && rootName != "" && index.coversSourceDocumentState(state) {
		currentDef, rootID, found, current := c.cachedVectorIndexForState(def.Name, rootName, state)
		if current && found && index.validateNativeSnapshotDefinition(currentDef) == "" && rootID == index.nativeSnapshotBaseEpochForFullSave() {
			epoch, bytesDisk, _, _, _ := index.nativeSearchState()
			return index, VectorIndexLoadStatus{Loaded: true, RootName: rootName, RootID: rootID, Epoch: epoch, BytesDisk: bytesDisk}, nil
		}
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, VectorIndexLoadStatus{}, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	state, ok := snap.StateToken()
	if !ok {
		return nil, VectorIndexLoadStatus{}, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return nil, VectorIndexLoadStatus{}, err
	}
	if catalog == nil {
		return nil, VectorIndexLoadStatus{}, errCollectionNotFound
	}
	documentGeneration, err := vectorIndexDocumentGeneration(snap, catalog)
	if err != nil {
		return nil, VectorIndexLoadStatus{}, err
	}
	if !index.coversSourceDocumentGeneration(documentGeneration) {
		return nil, VectorIndexLoadStatus{ExactFallbackReason: vectorIndexFallbackStaleDocumentRoot}, fmt.Errorf("%w: native_runtime vector index %q does not cover current documents", ErrVectorIndexSearchUnavailable, def.Name)
	}
	currentDef, ok := findVectorIndex(catalog.meta.VectorIndexes, def.Name)
	if !ok {
		return nil, VectorIndexLoadStatus{ExactFallbackReason: vectorIndexFallbackMissingVectorIndexMetadata}, fmt.Errorf("%w: native_runtime vector index %q is not declared", ErrVectorIndexSearchUnavailable, def.Name)
	}
	if reason := index.validateNativeSnapshotDefinition(currentDef); reason != "" {
		return nil, VectorIndexLoadStatus{ExactFallbackReason: reason}, fmt.Errorf("%w: native_runtime vector index %q definition mismatch: %s", ErrVectorIndexSearchUnavailable, def.Name, reason)
	}
	rootName = collectionVectorIndexRootName(catalog.meta.Name, def.Name)
	rootID := catalog.rootID(rootName)
	if rootID != index.nativeSnapshotBaseEpochForFullSave() {
		return nil, VectorIndexLoadStatus{ExactFallbackReason: vectorIndexFallbackStaleRuntimeIndex}, fmt.Errorf("%w: native_runtime vector index %q is stale", ErrVectorIndexSearchUnavailable, def.Name)
	}
	index.recordSourceDocumentState(documentGeneration, state)
	epoch, bytesDisk, _, _, _ := index.nativeSearchState()
	return index, VectorIndexLoadStatus{Loaded: true, RootName: rootName, RootID: rootID, Epoch: epoch, BytesDisk: bytesDisk}, nil
}

func validateCollectionVectorIndexSearchWithBufferOptions(opts VectorIndexSearchOptions, buffer *VectorIndexSearchBuffer) error {
	if buffer == nil {
		return errors.New("collections: nil vector index search buffer")
	}
	if err := validateVectorIndexSearchRequest(opts.TopK, opts.EfSearch); err != nil {
		buffer.Reset()
		return err
	}
	if opts.MaxDecodedBlocks < 0 {
		buffer.Reset()
		return errors.New("collections: vector index search max_decoded_blocks cannot be negative")
	}
	if opts.IncludeDocuments {
		buffer.Reset()
		return collectionVectorIndexWithBufferUnsupportedOptionError("IncludeDocuments=true", "this API returns no-document result IDs and scores only; use SearchVectorIndex with IncludeDocuments=true for materialization or run a separate document fetch after buffered search")
	}
	if err := collectionVectorIndexWithBufferDocumentFetchOptionsError(opts.DocumentFetchOptions); err != nil {
		buffer.Reset()
		return err
	}
	if opts.Filter != nil {
		buffer.Reset()
		return collectionVectorIndexWithBufferUnsupportedOptionError("Filter", "filters are outside the exact no-document hnsw_search_pack_v1 route; use SearchVectorIndex for response-owned search or build a separate measured filtered path")
	}
	if opts.IndexRangeFilter != nil {
		buffer.Reset()
		return collectionVectorIndexWithBufferUnsupportedOptionError("IndexRangeFilter", "filters are outside the exact no-document hnsw_search_pack_v1 route; use SearchVectorIndex for response-owned search or build a separate measured filtered path")
	}
	if opts.FetchMultiplier != 0 {
		buffer.Reset()
		return collectionVectorIndexWithBufferUnsupportedOptionError("FetchMultiplier", "the collection buffered route is pack-only and does not run legacy fallback or rerank controls")
	}
	if opts.ExactFilterMaxDocs != 0 {
		buffer.Reset()
		return collectionVectorIndexWithBufferUnsupportedOptionError("ExactFilterMaxDocs", "the collection buffered route is pack-only and does not run legacy fallback or filter controls")
	}
	if opts.DisableExactFallback {
		buffer.Reset()
		return collectionVectorIndexWithBufferUnsupportedOptionError("DisableExactFallback", "the collection buffered route already fails closed instead of falling back")
	}
	if opts.QueryMode != "" && opts.QueryMode != VectorIndexQueryModeExact && opts.QueryMode != VectorIndexQueryModeQuantizedOnly && opts.QueryMode != VectorIndexQueryModeQuantizedRerank {
		buffer.Reset()
		return collectionVectorIndexWithBufferUnsupportedOptionError(fmt.Sprintf("QueryMode=%q", opts.QueryMode), "this API supports exact/zero, quantized_only, or quantized_rerank QueryMode on no-document buffered routes")
	}
	if (opts.QueryMode == "" || opts.QueryMode == VectorIndexQueryModeExact) && opts.QuantizedIndexName != "" {
		buffer.Reset()
		return collectionVectorIndexWithBufferUnsupportedOptionError("QuantizedIndexName", "exact collection buffered search cannot select a quantized index; set QueryMode=quantized_only or QueryMode=quantized_rerank for the quantized no-document route")
	}
	if (opts.QueryMode == "" || opts.QueryMode == VectorIndexQueryModeExact) && opts.QuantizedRerankCandidates != 0 {
		buffer.Reset()
		return collectionVectorIndexWithBufferUnsupportedOptionError("QuantizedRerankCandidates", "exact collection buffered search cannot set quantized rerank candidates; set QueryMode=quantized_rerank with QuantizedIndexName for the quantized no-document route")
	}
	if opts.StatsMode == VectorIndexSearchStatsModeBenchmarkDebug {
		buffer.Reset()
		return collectionVectorIndexWithBufferUnsupportedOptionError("StatsMode=benchmark_debug", "debug-only per-candidate/per-edge counters are outside the high-QPS buffered route; use SearchVectorIndex or OpenVectorIndexSearcher for diagnostic searches")
	}
	if _, err := columnVectorGraphNativeSearchStatsModeFromPublic(opts.StatsMode); err != nil {
		buffer.Reset()
		return collectionVectorIndexWithBufferUnsupportedOptionError(fmt.Sprintf("StatsMode=%q", opts.StatsMode), err.Error())
	}
	if _, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK); err != nil {
		buffer.Reset()
		return collectionVectorIndexWithBufferUnsupportedOptionError("QueryMode/QuantizedIndexName", err.Error())
	}
	return nil
}

func collectionVectorIndexWithBufferUnsupportedOptionError(option, guidance string) error {
	return fmt.Errorf("%w: vector index SearchVectorIndexWithBuffer unsupported option %s: %s", ErrVectorIndexSearchUnavailable, option, guidance)
}

func collectionVectorIndexWithBufferDocumentFetchOptionsError(opts DocumentFetchOptions) error {
	if len(opts.IncludePaths) > 0 {
		return collectionVectorIndexWithBufferUnsupportedOptionError("DocumentFetchOptions.IncludePaths", "document projection requires materialization; use SearchVectorIndex with IncludeDocuments=true or fetch documents in a separate phase")
	}
	if len(opts.ExcludePaths) > 0 {
		return collectionVectorIndexWithBufferUnsupportedOptionError("DocumentFetchOptions.ExcludePaths", "document projection requires materialization; use SearchVectorIndex with IncludeDocuments=true or fetch documents in a separate phase")
	}
	if opts.Format != "" {
		return collectionVectorIndexWithBufferUnsupportedOptionError("DocumentFetchOptions.Format", "document format selection applies only to materialization; use SearchVectorIndex with IncludeDocuments=true or fetch documents in a separate phase")
	}
	if opts.ColumnAssetReadIntegrity != "" {
		return collectionVectorIndexWithBufferUnsupportedOptionError("DocumentFetchOptions.ColumnAssetReadIntegrity", "document read-integrity controls apply only to materialization; use SearchVectorIndex with IncludeDocuments=true or fetch documents in a separate phase")
	}
	return nil
}

func resetBufferedVectorIndexSearchResponse(response *VectorIndexSearchResponse, buffer *VectorIndexSearchBuffer) {
	if buffer != nil {
		buffer.Reset()
	}
	if response != nil {
		response.Results = nil
	}
}

func vectorIndexDocumentFetchOptionsNonZero(opts DocumentFetchOptions) bool {
	return documentFetchOptionsHasProjection(opts) || opts.Format != "" || opts.ColumnAssetReadIntegrity != ""
}

func vectorIndexSearchStatsAreBufferedNoDocumentPackRoute(stats VectorIndexSearchStats) bool {
	return stats.SearchRouteQuantizedOnly == 0 &&
		stats.SearchRouteQuantizedRerank == 0 &&
		stats.QuantizedScorerActive == 0 &&
		stats.SearchRouteHNSWSearchPack == 1 &&
		stats.HNSWSearchPackActive == 1 &&
		stats.HNSWSearchPackMissing == 0 &&
		stats.HNSWSearchPackInvalid == 0 &&
		stats.HNSWSearchPackStale == 0 &&
		stats.HNSWSearchPackClosed == 0 &&
		stats.HNSWSearchPackFallbacks == 0 &&
		stats.SearchRouteColumnGraphPrepared == 0 &&
		stats.SearchRouteColumnGraphFallback == 0 &&
		stats.DocumentsFetched == 0 &&
		stats.GraphRowFallbacks == 0 &&
		stats.TypedColumnFallbacks == 0 &&
		stats.VectorScratchDecodes == 0
}

func vectorIndexSearchStatsAreBufferedNoDocumentQuantizedOnlyRoute(stats VectorIndexSearchStats, opts VectorIndexSearchOptions) bool {
	columnGraphRoute := stats.SearchRouteHNSWSearchPack == 0 &&
		stats.HNSWSearchPackActive == 0 &&
		stats.HNSWSearchPackMissing == 0 &&
		stats.HNSWSearchPackInvalid == 0 &&
		stats.HNSWSearchPackStale == 0 &&
		stats.HNSWSearchPackClosed == 0 &&
		stats.HNSWSearchPackFallbacks == 0 &&
		stats.SearchRouteColumnGraphPrepared+stats.SearchRouteColumnGraphFallback == 1
	packRoute := stats.SearchRouteHNSWSearchPack == 1 &&
		stats.HNSWSearchPackActive == 1 &&
		stats.HNSWSearchPackMissing == 0 &&
		stats.HNSWSearchPackInvalid == 0 &&
		stats.HNSWSearchPackStale == 0 &&
		stats.HNSWSearchPackClosed == 0 &&
		stats.HNSWSearchPackFallbacks == 0 &&
		stats.SearchRouteColumnGraphPrepared == 0 &&
		stats.SearchRouteColumnGraphFallback == 0
	if !columnGraphRoute && !packRoute {
		return false
	}
	emptySearch := opts.TopK == 0 || stats.CandidateRows == 0
	if !emptySearch && stats.QuantizedScorerActive != 1 {
		return false
	}
	if stats.QuantizedAssetUnavailable != 0 || stats.QuantizedAssetMissing != 0 || stats.QuantizedAssetInvalid != 0 || stats.QuantizedAssetStale != 0 || stats.QuantizedAssetClosed != 0 {
		return false
	}
	if stats.QuantizedAssetHeapCopy+stats.QuantizedAssetMmapDirect != 1 || stats.QuantizedAssetHeapCopyBytes+stats.QuantizedAssetMappedBytes == 0 {
		return false
	}
	if stats.DocumentsFetched != 0 || stats.DocumentBytes != 0 || stats.DocumentOutputBytes != 0 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 {
		return false
	}
	if !emptySearch && (stats.QuantizedScoreCalls == 0 || stats.QuantizedCodeBytesRead == 0) {
		return false
	}
	return stats.SearchRouteQuantizedOnly == 1 &&
		stats.SearchRouteQuantizedRerank == 0 &&
		stats.PreparedScoreCalls == 0 &&
		stats.QuantizedRerankCandidates == 0 &&
		stats.QuantizedRerankExactScoreCalls == 0 &&
		stats.VectorBytesRead == 0 &&
		stats.NormBytesRead == 0
}

func vectorIndexSearchStatsAreBufferedNoDocumentQuantizedRoute(stats VectorIndexSearchStats, queryMode columnVectorGraphNativeSearchQueryMode, opts VectorIndexSearchOptions, dimensions int) bool {
	if queryMode == columnVectorGraphNativeSearchQueryModeQuantizedOnly {
		return vectorIndexSearchStatsAreBufferedNoDocumentQuantizedOnlyRoute(stats, opts)
	}
	if queryMode != columnVectorGraphNativeSearchQueryModeQuantizedRerank {
		return false
	}
	columnGraphRoute := stats.SearchRouteHNSWSearchPack == 0 &&
		stats.HNSWSearchPackActive == 0 &&
		stats.HNSWSearchPackMissing == 0 &&
		stats.HNSWSearchPackInvalid == 0 &&
		stats.HNSWSearchPackStale == 0 &&
		stats.HNSWSearchPackClosed == 0 &&
		stats.HNSWSearchPackFallbacks == 0 &&
		stats.SearchRouteColumnGraphPrepared+stats.SearchRouteColumnGraphFallback == 1
	packRoute := stats.SearchRouteHNSWSearchPack == 1 &&
		stats.HNSWSearchPackActive == 1 &&
		stats.HNSWSearchPackMissing == 0 &&
		stats.HNSWSearchPackInvalid == 0 &&
		stats.HNSWSearchPackStale == 0 &&
		stats.HNSWSearchPackClosed == 0 &&
		stats.HNSWSearchPackFallbacks == 0 &&
		stats.SearchRouteColumnGraphPrepared == 0 &&
		stats.SearchRouteColumnGraphFallback == 0
	if !columnGraphRoute && !packRoute {
		return false
	}
	emptySearch := opts.TopK == 0 || stats.CandidateRows == 0
	if !emptySearch && stats.QuantizedScorerActive != 1 {
		return false
	}
	if stats.QuantizedAssetUnavailable != 0 || stats.QuantizedAssetMissing != 0 || stats.QuantizedAssetInvalid != 0 || stats.QuantizedAssetStale != 0 || stats.QuantizedAssetClosed != 0 {
		return false
	}
	if stats.QuantizedAssetHeapCopy+stats.QuantizedAssetMmapDirect != 1 || stats.QuantizedAssetHeapCopyBytes+stats.QuantizedAssetMappedBytes == 0 {
		return false
	}
	if stats.DocumentsFetched != 0 || stats.DocumentBytes != 0 || stats.DocumentOutputBytes != 0 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 {
		return false
	}
	if !emptySearch && (stats.QuantizedScoreCalls == 0 || stats.QuantizedCodeBytesRead == 0) {
		return false
	}
	if stats.SearchRouteQuantizedOnly != 0 || stats.SearchRouteQuantizedRerank != 1 {
		return false
	}
	if emptySearch {
		return stats.PreparedScoreCalls == 0 && stats.QuantizedRerankCandidates == 0 && stats.QuantizedRerankExactScoreCalls == 0 && stats.VectorBytesRead == 0 && stats.NormBytesRead == 0
	}
	if stats.QuantizedRerankCandidates == 0 || stats.QuantizedRerankExactScoreCalls != stats.QuantizedRerankCandidates {
		return false
	}
	if opts.QuantizedRerankCandidates > 0 && stats.QuantizedRerankCandidates > uint64(opts.QuantizedRerankCandidates) {
		return false
	}
	if stats.VectorBytesRead == 0 {
		return false
	}
	packNativeExactRerank := stats.PreparedScoreCalls == stats.QuantizedRerankExactScoreCalls &&
		stats.VectorPreparedDirectViews == stats.QuantizedRerankExactScoreCalls &&
		stats.NormPreparedDirectViews == 0
	if stats.NormBytesRead == 0 && stats.SearchRouteHNSWSearchPack == 0 && !packNativeExactRerank {
		return false
	}
	if dimensions > 0 {
		maxVectorBytes := stats.QuantizedRerankCandidates * uint64(dimensions) * 4
		maxNormBytes := stats.QuantizedRerankCandidates * 4
		if stats.VectorBytesRead > maxVectorBytes || stats.NormBytesRead > maxNormBytes {
			return false
		}
	}
	return true
}

// OpenVectorIndexSearcher opens a reusable search handle for steady-state
// vector queries. Setup/open/decode cost is paid at open; Search then measures
// graph traversal, vector scoring, top-k production, and optional post-top-k
// document fetch. For the current exact no-document high-QPS contract, pair a
// reusable searcher with SearchWithBuffer and a warmed caller-owned buffer.
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
		routeStats: vectorIndexSearchRouteStatsForColumnGraphReader(reader),
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
		response.Stats = VectorIndexSearchStats{HNSWSearchPackClosed: 1}
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
	queryMode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	if err != nil {
		return response, err
	}
	readerStatsBefore := s.readerLast
	results, searchStats, err := s.reader.SearchCosine(opts.Query, columnVectorGraphNativeSearchOptions{
		TopK:                      opts.TopK,
		EfSearch:                  opts.EfSearch,
		ScoreBatchMode:            opts.scoreBatchMode,
		StatsMode:                 statsMode,
		QueryMode:                 queryMode,
		QuantizedIndexName:        opts.QuantizedIndexName,
		QuantizedRerankCandidates: opts.QuantizedRerankCandidates,
	}, &s.scratch)
	readerStatsAfter := s.reader.Stats()
	s.readerLast = readerStatsAfter
	readerStats := columnPhysicalRowReaderStatsDelta(readerStatsBefore, readerStatsAfter)
	response.Stats = vectorIndexSearchStatsFromInternal(searchStats, readerStats)
	vectorIndexSearchRouteStatsForQueryMode(s.routeStats, queryMode).apply(&response.Stats)
	if err != nil {
		return response, err
	}
	if len(results) == 0 {
		return response, nil
	}
	response.Results, err = copyVectorIndexSearchResultsToOwned(results)
	if err != nil {
		return response, err
	}
	markVectorIndexSearchResponseOwnedResultAllocs(&response)
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
// The high-QPS exact public contract for this path is exact/zero QueryMode with
// no document projection and no document materialization. Healthy exact
// current-format evidence should show hnsw_search_pack_v1 active and selected,
// zero document fetches, zero graph-row fallback, zero typed-column vector
// fallback, and zero vector scratch decodes. Explicit quantized modes use the
// column_graph quantized scorer route; rabitq_1bit uses the prepared
// hnsw_search_pack_v1 score-plane traversal when eligible. Quantized modes must
// fail closed with quantized_* asset counters when the selected score plane is
// unavailable.
//
// On any error after a non-nil buffer is supplied, the buffer's reusable
// result/id views are reset to length zero and the returned response has no
// Results. Search metadata and any telemetry collected before the error may
// still be present in the returned response.
func (s *VectorIndexSearcher) SearchWithBuffer(opts VectorIndexSearcherSearchOptions, buffer *VectorIndexSearchBuffer) (VectorIndexSearchResponse, error) {
	if buffer == nil {
		return VectorIndexSearchResponse{}, errors.New("collections: nil vector index search buffer")
	}
	previousResults := buffer.results
	buffer.resetView()
	var response VectorIndexSearchResponse
	if s == nil || s.reader == nil || s.collection == nil {
		clear(previousResults)
		return response, errors.New("collections: nil vector index searcher")
	}
	response.IndexName = s.indexName
	response.Strategy = s.strategy
	response.Path = s.path
	response.Status = s.status
	if s.closed {
		response.Stats = VectorIndexSearchStats{HNSWSearchPackClosed: 1}
		clear(previousResults)
		return response, errors.New("collections: vector index searcher is closed")
	}
	if err := validateVectorIndexSearchRequest(opts.TopK, opts.EfSearch); err != nil {
		clear(previousResults)
		return response, err
	}
	if opts.IncludeDocuments {
		clear(previousResults)
		return response, errors.New("collections: vector index SearchWithBuffer does not support IncludeDocuments")
	}
	if documentFetchOptionsHasProjection(opts.DocumentFetchOptions) {
		clear(previousResults)
		return response, errors.New("collections: vector index document projection requires IncludeDocuments")
	}
	statsMode, err := columnVectorGraphNativeSearchStatsModeFromPublic(opts.StatsMode)
	if err != nil {
		clear(previousResults)
		return response, err
	}
	queryMode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	if err != nil {
		clear(previousResults)
		return response, err
	}
	pack, routeStats, usePackRoute := s.hnswSearchPackSearchWithBufferRoute(queryMode, statsMode)
	if usePackRoute {
		results, searchStats, err := pack.searchCosine(opts.Query, columnVectorGraphNativeSearchOptions{
			TopK:                      opts.TopK,
			EfSearch:                  opts.EfSearch,
			ScoreBatchMode:            opts.scoreBatchMode,
			StatsMode:                 statsMode,
			QueryMode:                 queryMode,
			QuantizedIndexName:        opts.QuantizedIndexName,
			QuantizedRerankCandidates: opts.QuantizedRerankCandidates,
		}, &s.scratch)
		response.Stats = vectorIndexSearchStatsFromInternal(searchStats, columnPhysicalRowReaderStats{})
		routeStats.apply(&response.Stats)
		if err != nil {
			clear(previousResults)
			return response, err
		}
		response.Results, err = copyVectorIndexSearchResultsToBuffer(results, buffer, previousResults)
		if err != nil {
			return response, err
		}
		return response, nil
	}

	if s.reader.rabitqHNSWSearchPackPreparedRouteEligible(queryMode, opts.QuantizedIndexName, statsMode) {
		packRouteStats := vectorIndexSearchRouteStatsForHNSWSearchPackRoute(s.routeStats)
		results, searchStats, err := s.reader.searchRabitQCosinePreparedHNSWPack(opts.Query, columnVectorGraphNativeSearchOptions{
			TopK:                      opts.TopK,
			EfSearch:                  opts.EfSearch,
			ScoreBatchMode:            opts.scoreBatchMode,
			StatsMode:                 statsMode,
			QueryMode:                 queryMode,
			QuantizedIndexName:        opts.QuantizedIndexName,
			QuantizedRerankCandidates: opts.QuantizedRerankCandidates,
		}, &s.scratch)
		response.Stats = vectorIndexSearchStatsFromInternal(searchStats, columnPhysicalRowReaderStats{})
		if err != nil && searchStats.QuantizedScorerActive == 0 && searchStats.QuantizedScoreCalls == 0 {
			vectorIndexSearchRouteStatsForColumnGraphQuantized(s.routeStats).apply(&response.Stats)
		} else {
			packRouteStats.apply(&response.Stats)
		}
		if err != nil {
			clear(previousResults)
			return response, err
		}
		response.Results, err = copyVectorIndexSearchResultsToBuffer(results, buffer, previousResults)
		if err != nil {
			return response, err
		}
		return response, nil
	}

	if pack, ok := s.scalarU8PreparedTraversalSearchWithBufferRoute(queryMode, statsMode, opts.QuantizedIndexName); ok {
		results, searchStats, err := s.reader.SearchCosineScalarU8PreparedTraversal(pack, opts.Query, columnVectorGraphNativeSearchOptions{
			TopK:                      opts.TopK,
			EfSearch:                  opts.EfSearch,
			ScoreBatchMode:            opts.scoreBatchMode,
			StatsMode:                 statsMode,
			QueryMode:                 queryMode,
			QuantizedIndexName:        opts.QuantizedIndexName,
			QuantizedRerankCandidates: opts.QuantizedRerankCandidates,
		}, &s.scratch)
		response.Stats = vectorIndexSearchStatsFromInternal(searchStats, columnPhysicalRowReaderStats{})
		routeStats.apply(&response.Stats)
		if err != nil {
			clear(previousResults)
			return response, err
		}
		response.Results, err = copyVectorIndexSearchResultsToBuffer(results, buffer, previousResults)
		if err != nil {
			return response, err
		}
		return response, nil
	}

	fallbackRouteStats := routeStats
	readerStatsBefore := s.readerLast
	results, searchStats, err := s.reader.SearchCosine(opts.Query, columnVectorGraphNativeSearchOptions{
		TopK:                      opts.TopK,
		EfSearch:                  opts.EfSearch,
		ScoreBatchMode:            opts.scoreBatchMode,
		StatsMode:                 statsMode,
		QueryMode:                 queryMode,
		QuantizedIndexName:        opts.QuantizedIndexName,
		QuantizedRerankCandidates: opts.QuantizedRerankCandidates,
	}, &s.scratch)
	readerStatsAfter := s.reader.Stats()
	s.readerLast = readerStatsAfter
	readerStats := columnPhysicalRowReaderStatsDelta(readerStatsBefore, readerStatsAfter)
	response.Stats = vectorIndexSearchStatsFromInternal(searchStats, readerStats)
	fallbackRouteStats.apply(&response.Stats)
	if err != nil {
		clear(previousResults)
		return response, err
	}
	response.Results, err = copyVectorIndexSearchResultsToBuffer(results, buffer, previousResults)
	if err != nil {
		return response, err
	}
	return response, nil
}

func copyVectorIndexSearchResultsToOwned(results []columnVectorGraphNativeSearchResult) ([]VectorIndexSearchResult, error) {
	if len(results) == 0 {
		return nil, nil
	}
	idByteCount, err := vectorIndexSearchResultIDBytes(results)
	if err != nil {
		return nil, err
	}
	out := make([]VectorIndexSearchResult, len(results))
	idBytes := make([]byte, idByteCount)
	idOffset := 0
	for i, result := range results {
		if len(result.ID) > len(idBytes)-idOffset {
			return nil, errors.New("collections: vector index search result id byte accounting mismatch")
		}
		nextIDOffset := idOffset + len(result.ID)
		id := idBytes[idOffset:nextIDOffset:nextIDOffset]
		idOffset = nextIDOffset
		copy(id, result.ID)
		out[i] = VectorIndexSearchResult{
			ID:      id,
			Ordinal: result.Ordinal,
			Score:   result.Score,
		}
	}
	return out, nil
}

func copyVectorIndexSearchResultsToBuffer(results []columnVectorGraphNativeSearchResult, buffer *VectorIndexSearchBuffer, previousResults []VectorIndexSearchResult) ([]VectorIndexSearchResult, error) {
	if len(results) == 0 {
		clear(previousResults)
		return nil, nil
	}
	idByteCount, err := vectorIndexSearchResultIDBytes(results)
	if err != nil {
		clear(previousResults)
		return nil, err
	}
	buffer.results = resizeVectorIndexSearchResultBuffer(buffer.results, len(results))
	buffer.idBytes = resizeVectorIndexSearchByteBuffer(buffer.idBytes, idByteCount)
	if len(previousResults) > len(results) {
		clear(previousResults[len(results):])
	}
	idOffset := 0
	for i, result := range results {
		if len(result.ID) > len(buffer.idBytes)-idOffset {
			clear(previousResults)
			buffer.Reset()
			return nil, errors.New("collections: vector index search result id byte accounting mismatch")
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
	return buffer.results, nil
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

func normalizeVectorIndexSearchQueryMode(mode VectorIndexQueryMode, quantizedIndexName string, quantizedRerankCandidates, topK int) (columnVectorGraphNativeSearchQueryMode, error) {
	switch mode {
	case "", VectorIndexQueryModeExact:
		if quantizedIndexName != "" {
			return columnVectorGraphNativeSearchQueryModeExact, errors.New("collections: exact vector index search cannot select a quantized index")
		}
		if quantizedRerankCandidates != 0 {
			return columnVectorGraphNativeSearchQueryModeExact, errors.New("collections: exact vector index search cannot set quantized rerank candidates")
		}
		return columnVectorGraphNativeSearchQueryModeExact, nil
	case VectorIndexQueryModeQuantizedOnly:
		if err := validateSelectedQuantizedVectorIndexName(quantizedIndexName); err != nil {
			return columnVectorGraphNativeSearchQueryModeExact, err
		}
		if quantizedRerankCandidates != 0 {
			return columnVectorGraphNativeSearchQueryModeExact, errors.New("collections: quantized_only vector index search cannot set quantized rerank candidates")
		}
		return columnVectorGraphNativeSearchQueryModeQuantizedOnly, nil
	case VectorIndexQueryModeQuantizedRerank:
		if err := validateSelectedQuantizedVectorIndexName(quantizedIndexName); err != nil {
			return columnVectorGraphNativeSearchQueryModeExact, err
		}
		if quantizedRerankCandidates < 0 {
			return columnVectorGraphNativeSearchQueryModeExact, errors.New("collections: quantized vector index rerank candidates cannot be negative")
		}
		if quantizedRerankCandidates != 0 && topK > 0 && quantizedRerankCandidates < topK {
			return columnVectorGraphNativeSearchQueryModeExact, errors.New("collections: quantized vector index rerank candidates cannot be less than top_k")
		}
		return columnVectorGraphNativeSearchQueryModeQuantizedRerank, nil
	default:
		return columnVectorGraphNativeSearchQueryModeExact, fmt.Errorf("collections: vector index query mode %q is unsupported", mode)
	}
}

func validateSelectedQuantizedVectorIndexName(name string) error {
	if name == "" {
		return errors.New("collections: quantized vector index name is required for quantized query mode")
	}
	if err := ValidateIndexName(name); err != nil {
		return fmt.Errorf("collections: quantized vector index name: %w", err)
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
	stats := VectorIndexSearchStats{
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
		QuantizedScoreCalls:                   searchStats.QuantizedScoreCalls,
		QuantizedCodeBytesRead:                searchStats.QuantizedCodeBytesRead,
		QuantizedRerankCandidates:             searchStats.QuantizedRerankCandidates,
		QuantizedRerankExactScoreCalls:        searchStats.QuantizedRerankExactScoreCalls,
		QuantizedScorerActive:                 searchStats.QuantizedScorerActive,
		QuantizedAssetMissing:                 searchStats.QuantizedAssetMissing,
		QuantizedAssetInvalid:                 searchStats.QuantizedAssetInvalid,
		QuantizedAssetStale:                   searchStats.QuantizedAssetStale,
		QuantizedAssetClosed:                  searchStats.QuantizedAssetClosed,
		QuantizedAssetUnavailable:             searchStats.QuantizedAssetUnavailable,
		QuantizedAssetMmapDirect:              searchStats.QuantizedAssetMmapDirect,
		QuantizedAssetHeapCopy:                searchStats.QuantizedAssetHeapCopy,
		QuantizedAssetOpenNanos:               searchStats.QuantizedAssetOpenNanos,
		QuantizedAssetMappedBytes:             searchStats.QuantizedAssetMappedBytes,
		QuantizedAssetHeapCopyBytes:           searchStats.QuantizedAssetHeapCopyBytes,
		QuantizedAssetActiveHandles:           searchStats.QuantizedAssetActiveHandles,
		QuantizedScoreCodecScalarU8Alpha:      searchStats.QuantizedScoreCodecScalarU8Alpha,
		QuantizedScoreCodecBRQ1Bit:            searchStats.QuantizedScoreCodecBRQ1Bit,
		BRQ1BitQueryWeightBits:                searchStats.BRQ1BitQueryWeightBits,
		BRQ1BitBitProductPasses:               searchStats.BRQ1BitBitProductPasses,
		BRQ1BitQueryWeightScale:               searchStats.BRQ1BitQueryWeightScale,
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
		SearchRouteQuantizedOnly:              searchStats.SearchRouteQuantizedOnly,
		SearchRouteQuantizedRerank:            searchStats.SearchRouteQuantizedRerank,
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
		UpperLayerScoreTiles:                  searchStats.UpperLayerScoreTiles,
		UpperLayerScoreTileCandidates:         searchStats.UpperLayerScoreTileCandidates,
		UpperLayerScoreTileMaxSize:            searchStats.UpperLayerScoreTileMaxSize,
		UpperLayerAdjacencyLoads:              searchStats.UpperLayerAdjacencyLoads,
		UpperLayerAdjacencyNeighbors:          searchStats.UpperLayerAdjacencyNeighbors,
		UpperLayerEdgeVisits:                  searchStats.UpperLayerEdgeVisits,
		UpperLayerScoredNeighbors:             searchStats.UpperLayerScoredNeighbors,
		UpperLayerFilterSkips:                 searchStats.UpperLayerFilterSkips,
		Layer0Scores:                          searchStats.Layer0Scores,
		Layer0SeedScores:                      searchStats.Layer0SeedScores,
		Layer0NeighborScores:                  searchStats.Layer0NeighborScores,
		Layer0ScoreTiles:                      searchStats.Layer0ScoreTiles,
		Layer0ScoreTileCandidates:             searchStats.Layer0ScoreTileCandidates,
		Layer0ScoreTileMaxSize:                searchStats.Layer0ScoreTileMaxSize,
		Layer0AdjacencyLoads:                  searchStats.Layer0AdjacencyLoads,
		Layer0AdjacencyNeighbors:              searchStats.Layer0AdjacencyNeighbors,
		Layer0EdgeVisits:                      searchStats.Layer0EdgeVisits,
		Layer0ScoredNeighbors:                 searchStats.Layer0ScoredNeighbors,
		Layer0AlreadyVisitedSkips:             searchStats.Layer0AlreadyVisitedSkips,
		Layer0FilterSkips:                     searchStats.Layer0FilterSkips,
		Layer0StopChecks:                      searchStats.Layer0StopChecks,
		Layer0StopTrue:                        searchStats.Layer0StopTrue,
		Layer0StopFalse:                       searchStats.Layer0StopFalse,
		CandidateComparisons:                  searchStats.CandidateComparisons,
		FrontierComparisons:                   searchStats.FrontierComparisons,
		TopKComparisons:                       searchStats.TopKComparisons,
		FrontierPushes:                        searchStats.FrontierPushes,
		FrontierPops:                          searchStats.FrontierPops,
		FrontierPopMisses:                     searchStats.FrontierPopMisses,
		FrontierSiftUpCalls:                   searchStats.FrontierSiftUpCalls,
		FrontierSiftDownCalls:                 searchStats.FrontierSiftDownCalls,
		FrontierSiftUpSteps:                   searchStats.FrontierSiftUpSteps,
		FrontierSiftDownSteps:                 searchStats.FrontierSiftDownSteps,
		TopKInsertAttempts:                    searchStats.TopKInsertAttempts,
		TopKInsertSuccesses:                   searchStats.TopKInsertSuccesses,
		TopKInsertRejections:                  searchStats.TopKInsertRejections,
		TopKHeapSiftSteps:                     searchStats.TopKHeapSiftSteps,
		VisitedMarkChecks:                     searchStats.VisitedMarkChecks,
		VisitedMarkHits:                       searchStats.VisitedMarkHits,
		VisitedMarkMisses:                     searchStats.VisitedMarkMisses,
		VisitedMarkInserts:                    searchStats.VisitedMarkInserts,
		VisitedResetEpochAdvances:             searchStats.VisitedResetEpochAdvances,
		VisitedResetClearedRows:               searchStats.VisitedResetClearedRows,
		ExactModeSearches:                     searchStats.ExactModeSearches,
		ExactCandidateOrderObservations:       searchStats.ExactCandidateOrderObservations,
		ExactCandidateOrderTransitions:        searchStats.ExactCandidateOrderTransitions,
		ExactCandidateOrderAdjacentForward:    searchStats.ExactCandidateOrderAdjacentForward,
		ExactCandidateOrderNonAdjacentForward: searchStats.ExactCandidateOrderNonAdjacentForward,
		ExactCandidateOrderBackwardJumps:      searchStats.ExactCandidateOrderBackwardJumps,
		ExactCandidateOrderMaxForwardRun:      searchStats.ExactCandidateOrderMaxForwardRun,
		WorkAccountingSearches:                searchStats.WorkAccountingSearches,
		DistanceKernelNanos:                   searchStats.DistanceKernelNanos,
		GraphTraversalNanos:                   searchStats.GraphTraversalNanos,
	}
	if searchStats.WorkAccountingSearches != 0 {
		stats.FP32ScoreCalls = searchStats.FP32ScoreCalls
		stats.ExactRerankScoreCalls = searchStats.QuantizedRerankExactScoreCalls
		stats.HeapPushes = searchStats.FrontierPushes
		stats.HeapPops = searchStats.FrontierPops
	}
	return stats
}
