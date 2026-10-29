package collections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// VectorIndexEncoding identifies the primary vector encoding. ColumnGraph
// ANN admission requires float32; exact collection scans retain their metric math.
type VectorIndexEncoding uint8

const (
	VectorIndexEncodingFloat32 VectorIndexEncoding = iota
	VectorIndexEncodingInt8
)

type VectorIndexStrategy string

const (
	// VectorIndexStrategyColumnGraph selects the physical typed-column graph path.
	// Until graph assets are built and published, status must report unavailable
	// or rebuild-needed rather than falling back to a decoded in-memory graph.
	VectorIndexStrategyColumnGraph VectorIndexStrategy = "column_graph"
)

// VectorIndexRepresentation selects an opt-in persisted serving-value
// contract. The empty value preserves the legacy representation and score
// semantics.
type VectorIndexRepresentation string

const (
	VectorIndexRepresentationCosineNormalizedF32V1 VectorIndexRepresentation = "cosine_normalized_f32_v1"
)

func normalizeVectorIndexRepresentation(representation VectorIndexRepresentation) (VectorIndexRepresentation, error) {
	switch representation {
	case "", VectorIndexRepresentationCosineNormalizedF32V1:
		return representation, nil
	default:
		return "", fmt.Errorf("collections: unsupported vector index representation %q", representation)
	}
}

func vectorIndexUsesCosineNormalizedF32V1(def VectorIndexDefinition) bool {
	return def.Representation == VectorIndexRepresentationCosineNormalizedF32V1
}

type VectorIndexState string

const (
	VectorIndexStateColumnGraphLoaded        VectorIndexState = "column_graph_loaded"
	VectorIndexStateColumnGraphUnavailable   VectorIndexState = "column_graph_unavailable"
	VectorIndexStateColumnGraphRebuildNeeded VectorIndexState = "column_graph_rebuild_needed"
)

type VectorIndexReason string

const (
	VectorIndexReasonColumnGraphRebuildNeeded          VectorIndexReason = "column_graph_rebuild_needed"
	VectorIndexReasonPhysicalColumnAssetSupportMissing VectorIndexReason = "physical_column_asset_support_missing"
	VectorIndexReasonColumnGraphAssetMismatch          VectorIndexReason = "column_graph_asset_mismatch"
	VectorIndexReasonColumnGraphCorrupt                VectorIndexReason = "column_graph_corrupt"
	VectorIndexReasonColumnGraphUnsupportedVisibility  VectorIndexReason = "column_graph_unsupported_visibility"
	VectorIndexReasonColumnGraphUnsupportedMetric      VectorIndexReason = "column_graph_unsupported_metric"
	VectorIndexReasonUnsupportedStrategy               VectorIndexReason = "unsupported_vector_index_strategy"
)

func (e VectorIndexEncoding) String() string {
	switch e {
	case VectorIndexEncodingFloat32:
		return "float32"
	case VectorIndexEncodingInt8:
		return "int8"
	default:
		return fmt.Sprintf("unknown(%d)", e)
	}
}

func (e VectorIndexEncoding) MarshalJSON() ([]byte, error) {
	encoding, err := normalizeVectorIndexEncoding(e)
	if err != nil {
		return nil, err
	}
	return json.Marshal(encoding.String())
}

func (e *VectorIndexEncoding) UnmarshalJSON(raw []byte) error {
	if e == nil {
		return errors.New("collections: nil vector index encoding")
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		encoding, err := parseVectorIndexEncoding(s)
		if err != nil {
			return err
		}
		*e = encoding
		return nil
	}
	var n uint8
	if err := json.Unmarshal(raw, &n); err != nil {
		return err
	}
	encoding, err := normalizeVectorIndexEncoding(VectorIndexEncoding(n))
	if err != nil {
		return err
	}
	*e = encoding
	return nil
}

// VectorIndexQueryMode selects the score plane used by column_graph search.
// The zero value is exact to preserve existing search behavior. Quantized modes
// must be selected explicitly with a named quantized index and fail closed until
// matching assets and scorers are available.
type VectorIndexQueryMode string

const (
	VectorIndexQueryModeExact           VectorIndexQueryMode = "exact"
	VectorIndexQueryModeQuantizedOnly   VectorIndexQueryMode = "quantized_only"
	VectorIndexQueryModeQuantizedRerank VectorIndexQueryMode = "quantized_rerank"
)

// VectorIndexSearchOptions configures one vector index search.
//
// Collection-level high-QPS no-document searches are intentionally narrow: use
// an explicit column_graph index, exact/zero QueryMode, IncludeDocuments=false,
// no document projection, and no legacy filter fields. The current
// Collection.SearchVectorIndex method is a response-owned convenience boundary
// that uses the cached hnsw_search_pack_v1 route for healthy exact no-document
// calls and falls back to a one-shot searcher for unsupported shapes.
// Collection.SearchVectorIndexWithBuffer exposes caller-owned result storage for
// the exact no-document hnsw_search_pack_v1 seam and reuses collection-owned
// prepared pack state on warmed healthy current
// state; callers that need explicit snapshot lifetime control can still use a
// reusable VectorIndexSearcher with VectorIndexSearcher.SearchWithBuffer. To
// materialize documents after a no-document search, open a CollectionReadView
// and call FetchDocumentsForVectorIndexSearchResults as an explicit, separately
// measured fetch phase.
type VectorIndexSearchOptions struct {
	// Context optionally interrupts graph traversal.
	Context context.Context
	// IndexName is used by collection-level physical column_graph search.
	IndexName string
	// Query is used by collection-level physical column_graph search.
	Query []float32
	// QueryMode selects exact, quantized-only, or quantized-rerank search for
	// collection column_graph APIs. The zero value is exact. Only exact/zero mode
	// is in the collection-level no-document high-QPS contract; quantized modes are
	// explicit score-plane paths with their own fail-closed semantics.
	QueryMode VectorIndexQueryMode
	// QuantizedIndexName selects the named derived score plane for quantized modes.
	QuantizedIndexName string
	// QuantizedRerankCandidates bounds the quantized candidate set reranked by
	// exact float32 vectors in quantized_rerank mode. Zero uses the normalized
	// ef_search candidate set.
	QuantizedRerankCandidates int
	TopK                      int
	EfSearch                  int
	FetchMultiplier           int
	Filter                    func(DocumentRecord) (bool, error)
	IndexRangeFilter          *VectorIndexRangeFilter
	// DeclaredScalarFilter is the bounded equality/range AND grammar backed by
	// declared scalar indexes. It is supported by explicitly admitted typed
	// column_graph serving. It is intentionally
	// distinct from legacy callback filters.
	DeclaredScalarFilter *HybridScalarFilter
	ExactFilterMaxDocs   int
	DisableExactFallback bool
	// IncludeDocuments materializes documents after column_graph top-k selection.
	IncludeDocuments bool
	// DocumentFetchOptions controls optional projected final-fetch materialization.
	// It is used only when IncludeDocuments is true; the zero value returns full documents.
	DocumentFetchOptions DocumentFetchOptions
	// MaxDecodedBlocks bounds the physical column row reader cache for column_graph search.
	MaxDecodedBlocks int
	// StatsMode selects column_graph search telemetry detail. The zero value
	// preserves full diagnostics; production/minimal mode keeps source-health,
	// fallback, admission, and result counters with lower hot-loop overhead on
	// the healthy combined prepared path.
	StatsMode VectorIndexSearchStatsMode
	// scoreBatchMode is an internal exact-order indexed-scoring test/benchmark hook.
	scoreBatchMode columnVectorGraphScoreBatchMode
}

// VectorIndexStats reports modern ColumnGraph status and storage accounting.
type VectorIndexStats struct {
	Name           string
	Field          string
	Metric         VectorMetric
	Encoding       VectorIndexEncoding
	Dimensions     int
	M              int
	EfConstruction int
	EfSearch       int
	Nodes          int
	LiveDocs       int
	BytesMemory    int64
	BytesDisk      int64
	Epoch          uint64
	SnapshotDirty  bool
	RebuildNeeded  bool
}

func normalizeVectorIndexEncoding(encoding VectorIndexEncoding) (VectorIndexEncoding, error) {
	switch encoding {
	case VectorIndexEncodingFloat32, VectorIndexEncodingInt8:
		return encoding, nil
	default:
		return VectorIndexEncodingFloat32, fmt.Errorf("collections: unsupported vector index encoding %d", encoding)
	}
}

func parseVectorIndexEncoding(value string) (VectorIndexEncoding, error) {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "float32":
		return VectorIndexEncodingFloat32, nil
	case "int8":
		return VectorIndexEncodingInt8, nil
	default:
		return VectorIndexEncodingFloat32, fmt.Errorf("collections: unsupported vector index encoding %q", value)
	}
}
