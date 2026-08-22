package documentservice

import (
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	// ContractVersion is returned by health and index metadata responses so
	// clients can pin the pre-alpha schema they implement.
	ContractVersion = "treedb-document-service/v1alpha1"

	defaultEmbeddingField    = "embedding"
	defaultVectorIndexName   = "embedding"
	defaultTextField         = "content"
	defaultTextIndexName     = "content"
	defaultCollectionDocType = "treedb_document_service_v1"
	searchMetaKey            = "_treedb_search"
)

// Metric selects the dense-vector score function. Scores returned by the
// service are always higher-is-better.
type Metric string

const (
	MetricCosine       Metric = "cosine"
	MetricL2           Metric = "l2"
	MetricInnerProduct Metric = "inner_product"
)

// Document is the service's Haystack-compatible document shape.
type Document struct {
	ID        string    `json:"id"`
	Content   string    `json:"content,omitempty"`
	Embedding []float32 `json:"embedding,omitempty"`
	// EmbeddingF32LEBase64 is accepted only as a compact upsert transport.
	EmbeddingF32LEBase64 string         `json:"embedding_f32_le_b64,omitempty"`
	Score                *float64       `json:"score,omitempty"`
	Meta                 map[string]any `json:"meta,omitempty"`
}

// IndexCapabilities describes the supported operations for one service index.
type IndexCapabilities struct {
	DenseVectorSearch       bool `json:"dense_vector_search"`
	ExactDenseScoring       bool `json:"exact_dense_scoring"`
	MetadataFilters         bool `json:"metadata_filters"`
	KeywordSearch           bool `json:"keyword_search"`
	HybridSearch            bool `json:"hybrid_search"`
	KeywordMetadataFilters  bool `json:"keyword_metadata_filters"`
	HybridMetadataFilters   bool `json:"hybrid_metadata_filters"`
	BenchmarkLifecycle      bool `json:"benchmark_lifecycle"`
	VectorIndexMaintenance  bool `json:"vector_index_maintenance"`
	NoDocumentVectorSearch  bool `json:"no_document_vector_search"`
	ColumnGraphVectorSearch bool `json:"column_graph_vector_search"`
	ExactColumnGraphSearch  bool `json:"exact_column_graph_search"`
	QuantizedVectorSearch   bool `json:"quantized_vector_search"`
	QuantizedRerank         bool `json:"quantized_rerank"`
	ScalarU8QuantizedRerank bool `json:"scalar_u8_quantized_rerank"`
	RabitQ1BitExperimental  bool `json:"rabitq_1bit_experimental"`
}

// QuantizedIndexInfo describes a declared quantized score plane attached to the
// service vector index.
type QuantizedIndexInfo struct {
	Name                string                                 `json:"name"`
	Codec               string                                 `json:"codec"`
	Version             uint32                                 `json:"version"`
	ScalarU8Calibration *collections.ScalarU8CalibrationConfig `json:"scalar_u8_calibration,omitempty"`
}

// IndexInfo is returned by create/open and echoed by operation responses.
type IndexInfo struct {
	Name                 string                          `json:"name"`
	Dimension            int                             `json:"dimension"`
	Metric               Metric                          `json:"metric"`
	Generation           uint64                          `json:"generation"`
	ContractVersion      string                          `json:"contract_version"`
	EmbeddingField       string                          `json:"embedding_field"`
	VectorIndexName      string                          `json:"vector_index_name"`
	VectorStrategy       collections.VectorIndexStrategy `json:"vector_strategy"`
	VectorM              int                             `json:"vector_m,omitempty"`
	VectorEfConstruction int                             `json:"vector_ef_construction,omitempty"`
	VectorEfSearch       int                             `json:"vector_ef_search,omitempty"`
	QuantizedIndexes     []QuantizedIndexInfo            `json:"quantized_indexes,omitempty"`
	TextField            string                          `json:"text_field"`
	TextIndexName        string                          `json:"text_index_name"`
	DocumentType         string                          `json:"document_type"`
	Capabilities         IndexCapabilities               `json:"capabilities"`
}

// BenchmarkVectorIndexOptions configures the service-owned collection vector
// index for benchmark lifecycle setups. Omitted fields preserve the legacy
// create-index defaults used by treedb-client and treedb-haystack.
type BenchmarkVectorIndexOptions struct {
	Strategy         collections.VectorIndexStrategy `json:"strategy,omitempty"`
	M                int                             `json:"m,omitempty"`
	EfConstruction   int                             `json:"ef_construction,omitempty"`
	EfSearch         int                             `json:"ef_search,omitempty"`
	QuantizedIndexes []QuantizedIndexInfo            `json:"quantized_indexes,omitempty"`
}

// CreateIndexRequest creates or opens a service index. Existing compatible
// indexes are returned idempotently; incompatible existing collections fail.
type CreateIndexRequest struct {
	Name               string                       `json:"name"`
	Dimension          int                          `json:"dimension"`
	Metric             Metric                       `json:"metric,omitempty"`
	VectorIndexOptions *BenchmarkVectorIndexOptions `json:"vector_index_options,omitempty"`
}

// UpsertDocumentsRequest writes or replaces service documents.
type UpsertDocumentsRequest struct {
	ExpectedGeneration uint64     `json:"expected_generation,omitempty"`
	Documents          []Document `json:"documents"`
	// DeferVectorIndexRebuild lets benchmark loaders bulk-insert documents and
	// rebuild service vector assets once via OptimizeIndex after the load phase.
	DeferVectorIndexRebuild bool `json:"defer_vector_index_rebuild,omitempty"`
}

type UpsertDocumentsResponse struct {
	Index             IndexInfo `json:"index"`
	Upserted          int       `json:"upserted"`
	Inserted          int       `json:"inserted"`
	Updated           int       `json:"updated"`
	IDs               []string  `json:"ids"`
	CompactEmbeddings int       `json:"compact_embeddings,omitempty"`
}

// DeleteDocumentsRequest deletes either explicit IDs or documents matching a
// metadata filter. Supplying both IDs and Filter is rejected as ambiguous.
type DeleteDocumentsRequest struct {
	ExpectedGeneration uint64   `json:"expected_generation,omitempty"`
	IDs                []string `json:"ids,omitempty"`
	Filter             *Filter  `json:"filter,omitempty"`
}

type DeleteDocumentsResponse struct {
	Index   IndexInfo `json:"index"`
	Deleted int       `json:"deleted"`
	IDs     []string  `json:"ids"`
}

// CountDocumentsRequest counts documents matching Filter. A nil filter counts
// every service document in the index.
type CountDocumentsRequest struct {
	ExpectedGeneration uint64  `json:"expected_generation,omitempty"`
	Filter             *Filter `json:"filter,omitempty"`
}

type CountDocumentsResponse struct {
	Index IndexInfo `json:"index"`
	Count int       `json:"count"`
}

// FilterDocumentsRequest lists documents matching Filter in stable document-ID
// order. Limit=0 returns all matches; Offset must be non-negative.
type FilterDocumentsRequest struct {
	ExpectedGeneration uint64  `json:"expected_generation,omitempty"`
	Filter             *Filter `json:"filter,omitempty"`
	Limit              int     `json:"limit,omitempty"`
	Offset             int     `json:"offset,omitempty"`
	ReturnEmbedding    bool    `json:"return_embedding,omitempty"`
}

type FilterDocumentsResponse struct {
	Index        IndexInfo  `json:"index"`
	Documents    []Document `json:"documents"`
	MatchedCount int        `json:"matched_count"`
	Truncated    bool       `json:"truncated,omitempty"`
}

// DenseVectorSearchRequest runs exact dense scoring over documents that match
// Filter. QueryEmbedding must match the index dimension.
type DenseVectorSearchRequest struct {
	ExpectedGeneration uint64    `json:"expected_generation,omitempty"`
	QueryEmbedding     []float32 `json:"query_embedding"`
	TopK               int       `json:"top_k"`
	Filter             *Filter   `json:"filter,omitempty"`
	ReturnEmbedding    bool      `json:"return_embedding,omitempty"`
}

type DenseVectorSearchResponse struct {
	Index      IndexInfo  `json:"index"`
	Documents  []Document `json:"documents"`
	Metric     Metric     `json:"metric"`
	Exact      bool       `json:"exact"`
	Candidates int        `json:"candidates"`
}

// ResetIndexRequest creates a missing benchmark index or clears an existing
// compatible non-column_graph index when DropOld is true. Column_graph benchmark
// reset fails closed for existing indexes; use a fresh data directory or unique
// index name to preserve the insert-only load boundary required by graph assets.
type ResetIndexRequest struct {
	Dimension          int                          `json:"dimension"`
	Metric             Metric                       `json:"metric,omitempty"`
	DropOld            bool                         `json:"drop_old,omitempty"`
	VectorIndexOptions *BenchmarkVectorIndexOptions `json:"vector_index_options,omitempty"`
}

type ResetIndexResponse struct {
	Index            IndexInfo `json:"index"`
	Created          bool      `json:"created"`
	Reset            bool      `json:"reset"`
	DropOld          bool      `json:"drop_old"`
	DroppedDocuments int       `json:"dropped_documents"`
}

// OptimizeIndexRequest rebuilds service vector assets after benchmark load.
type OptimizeIndexRequest struct {
	ExpectedGeneration uint64 `json:"expected_generation,omitempty"`
	VectorIndexName    string `json:"vector_index_name,omitempty"`
}

type VectorIndexMaintenanceStatus struct {
	Name             string                          `json:"name"`
	Strategy         collections.VectorIndexStrategy `json:"strategy"`
	State            collections.VectorIndexState    `json:"state"`
	Reason           collections.VectorIndexReason   `json:"reason"`
	Loaded           bool                            `json:"loaded"`
	RebuildNeeded    bool                            `json:"rebuild_needed"`
	RootID           uint64                          `json:"root_id,omitempty"`
	NativeRootLoaded bool                            `json:"native_root_loaded,omitempty"`
	NativeRootBytes  int64                           `json:"native_root_bytes,omitempty"`
	DurationNanos    int64                           `json:"duration_nanos,omitempty"`
}

type OptimizeIndexResponse struct {
	Index           IndexInfo                    `json:"index"`
	VectorIndexName string                       `json:"vector_index_name"`
	Status          VectorIndexMaintenanceStatus `json:"status"`
}

// BenchmarkVectorQueryMode selects the no-document vector-index benchmark score
// plane. The legacy /search/vector route does not accept these modes and remains
// exact dense document scoring.
type BenchmarkVectorQueryMode string

const (
	BenchmarkVectorQueryModeExact           BenchmarkVectorQueryMode = "exact"
	BenchmarkVectorQueryModeQuantizedOnly   BenchmarkVectorQueryMode = "quantized_only"
	BenchmarkVectorQueryModeQuantizedRerank BenchmarkVectorQueryMode = "quantized_rerank"
)

// BenchmarkVectorResponseFormat selects the benchmark search HTTP response
// shape. The default full response is retained for diagnostics and clients.
type BenchmarkVectorResponseFormat string

const (
	BenchmarkVectorResponseFormatFull BenchmarkVectorResponseFormat = ""
	BenchmarkVectorResponseFormatIDs  BenchmarkVectorResponseFormat = "ids"
)

// BenchmarkVectorSearchRequest runs fail-closed no-document vector-index search
// through Collection.SearchVectorIndexWithBuffer. Quantized modes require an
// explicit quantized index name; quantized_rerank may bound exact rerank with
// QuantizedRerankCandidates (rerank32 is the benchmark baseline when set to 32).
type BenchmarkVectorSearchRequest struct {
	ExpectedGeneration        uint64                                 `json:"expected_generation,omitempty"`
	VectorIndexName           string                                 `json:"vector_index_name,omitempty"`
	QueryEmbedding            []float32                              `json:"query_embedding,omitempty"`
	QueryEmbeddingF32LEBase64 string                                 `json:"query_embedding_f32_le_b64,omitempty"`
	TopK                      int                                    `json:"top_k"`
	EfSearch                  int                                    `json:"ef_search,omitempty"`
	QueryMode                 BenchmarkVectorQueryMode               `json:"query_mode,omitempty"`
	QuantizedIndexName        string                                 `json:"quantized_index_name,omitempty"`
	QuantizedRerankCandidates int                                    `json:"quantized_rerank_candidates,omitempty"`
	StatsMode                 collections.VectorIndexSearchStatsMode `json:"stats_mode,omitempty"`
	ResponseFormat            BenchmarkVectorResponseFormat          `json:"response_format,omitempty"`
}

type BenchmarkVectorSearchResult struct {
	ID      string  `json:"id"`
	Ordinal int     `json:"ordinal"`
	Score   float64 `json:"score"`
}

type BenchmarkVectorSearchResponse struct {
	Index                     IndexInfo                                `json:"index"`
	Results                   []BenchmarkVectorSearchResult            `json:"results"`
	Metric                    Metric                                   `json:"metric"`
	VectorIndexName           string                                   `json:"vector_index_name"`
	QueryMode                 BenchmarkVectorQueryMode                 `json:"query_mode"`
	QuantizedIndexName        string                                   `json:"quantized_index_name,omitempty"`
	QuantizedRerankCandidates int                                      `json:"quantized_rerank_candidates,omitempty"`
	NoDocuments               bool                                     `json:"no_documents"`
	Stats                     collections.VectorIndexSearchStats       `json:"stats"`
	Diagnostics               collections.VectorIndexSearchDiagnostics `json:"diagnostics"`
	compactIDs                []string
}

// BenchmarkVectorSearchIDsResponse is the timed benchmark response. It is
// intentionally limited to the ordered IDs after a full response preflight.
type BenchmarkVectorSearchIDsResponse struct {
	ResponseFormat BenchmarkVectorResponseFormat `json:"response_format"`
	IDs            []string                      `json:"ids"`
}

// KeywordSearchRequest runs ranked lexical search over the service content text
// index. Metadata filters intentionally fail closed for keyword search in this
// pre-alpha contract; the service never scans documents as a fallback.
type KeywordSearchRequest struct {
	ExpectedGeneration uint64                         `json:"expected_generation,omitempty"`
	Query              string                         `json:"query"`
	TopK               int                            `json:"top_k"`
	Operator           collections.TextSearchOperator `json:"operator,omitempty"`
	CandidateLimit     int                            `json:"candidate_limit,omitempty"`
	MaxPostingsScanned int                            `json:"max_postings_scanned,omitempty"`
	Filter             *Filter                        `json:"filter,omitempty"`
	ReturnEmbedding    bool                           `json:"return_embedding,omitempty"`
}

type KeywordSearchResponse struct {
	Index     IndexInfo          `json:"index"`
	Documents []Document         `json:"documents"`
	TextIndex string             `json:"text_index"`
	Stats     KeywordSearchStats `json:"stats"`
}

type KeywordSearchStats struct {
	QueryTerms                int    `json:"query_terms,omitempty"`
	CandidatesRequested       uint64 `json:"candidates_requested,omitempty"`
	CandidatesReturned        uint64 `json:"candidates_returned,omitempty"`
	PostingsScanned           uint64 `json:"postings_scanned,omitempty"`
	CandidatesScored          uint64 `json:"candidates_scored,omitempty"`
	DocumentsFetched          uint64 `json:"documents_fetched,omitempty"`
	DocumentsMissing          uint64 `json:"documents_missing,omitempty"`
	FullDocumentScanFallbacks uint64 `json:"full_document_scan_fallbacks,omitempty"`
	PostingsScanNanos         uint64 `json:"postings_scan_nanos,omitempty"`
	CandidateScoreNanos       uint64 `json:"candidate_score_nanos,omitempty"`
	DocumentFetchNanos        uint64 `json:"document_fetch_nanos,omitempty"`
	Truncated                 bool   `json:"truncated,omitempty"`
	FailClosed                uint64 `json:"fail_closed,omitempty"`
	FailClosedReason          string `json:"fail_closed_reason,omitempty"`
	Unavailable               bool   `json:"unavailable,omitempty"`
	UnavailableReason         string `json:"unavailable_reason,omitempty"`
}

// HybridSearchRequest runs collection-native text/vector hybrid retrieval. At
// least one of Query or QueryEmbedding must be supplied. Metadata filters fail
// closed for now unless the service grows a bounded scalar-index mapping.
type HybridSearchRequest struct {
	ExpectedGeneration   uint64                          `json:"expected_generation,omitempty"`
	Query                string                          `json:"query,omitempty"`
	QueryEmbedding       []float32                       `json:"query_embedding,omitempty"`
	TopK                 int                             `json:"top_k"`
	TextCandidateLimit   int                             `json:"text_candidate_limit,omitempty"`
	VectorCandidateLimit int                             `json:"vector_candidate_limit,omitempty"`
	CandidateLimit       int                             `json:"candidate_limit,omitempty"`
	EfSearch             int                             `json:"ef_search,omitempty"`
	Fusion               collections.HybridFusionOptions `json:"fusion,omitempty"`
	Filter               *Filter                         `json:"filter,omitempty"`
	ReturnEmbedding      bool                            `json:"return_embedding,omitempty"`
}

type HybridSearchResponse struct {
	Index       IndexInfo                        `json:"index"`
	Documents   []Document                       `json:"documents"`
	TextIndex   string                           `json:"text_index,omitempty"`
	VectorIndex string                           `json:"vector_index,omitempty"`
	Plan        collections.HybridSearchPlan     `json:"plan,omitempty"`
	Snapshot    collections.HybridSearchSnapshot `json:"snapshot,omitempty"`
	Stats       collections.HybridSearchStats    `json:"stats,omitempty"`
}

func normalizeMetric(metric Metric) (Metric, error) {
	switch strings.TrimSpace(strings.ToLower(string(metric))) {
	case "", string(MetricCosine):
		return MetricCosine, nil
	case string(MetricL2):
		return MetricL2, nil
	case "inner_product", "inner-product", "innerproduct":
		return MetricInnerProduct, nil
	default:
		return "", serviceErrorf(CodeInvalidRequest, "unsupported metric %q", metric)
	}
}

func metricToCollection(metric Metric) (collections.VectorMetric, error) {
	normalized, err := normalizeMetric(metric)
	if err != nil {
		return 0, err
	}
	switch normalized {
	case MetricCosine:
		return collections.VectorMetricCosine, nil
	case MetricL2:
		return collections.VectorMetricL2, nil
	case MetricInnerProduct:
		return collections.VectorMetricInnerProduct, nil
	default:
		return 0, serviceErrorf(CodeInvalidRequest, "unsupported metric %q", metric)
	}
}

func metricFromCollection(metric collections.VectorMetric) (Metric, error) {
	switch metric {
	case collections.VectorMetricCosine:
		return MetricCosine, nil
	case collections.VectorMetricL2:
		return MetricL2, nil
	case collections.VectorMetricInnerProduct:
		return MetricInnerProduct, nil
	default:
		return "", serviceErrorf(CodeIndexUnavailable, "unsupported stored vector metric %d", metric)
	}
}

func indexCapabilities(vectorDef collections.VectorIndexDefinition, hybridSearch bool) IndexCapabilities {
	columnGraph := vectorDef.Strategy == collections.VectorIndexStrategyColumnGraph && vectorDef.Metric == collections.VectorMetricCosine && vectorDef.Encoding == collections.VectorIndexEncodingFloat32
	quantized := columnGraph && len(vectorDef.QuantizedIndexes) > 0
	quantizedRerank := columnGraph && quantizedIndexRerankCapabilityDeclared(vectorDef)
	return IndexCapabilities{
		DenseVectorSearch:       true,
		ExactDenseScoring:       true,
		MetadataFilters:         true,
		KeywordSearch:           true,
		HybridSearch:            hybridSearch,
		KeywordMetadataFilters:  false,
		HybridMetadataFilters:   false,
		BenchmarkLifecycle:      true,
		VectorIndexMaintenance:  true,
		NoDocumentVectorSearch:  columnGraph,
		ColumnGraphVectorSearch: columnGraph,
		ExactColumnGraphSearch:  columnGraph,
		QuantizedVectorSearch:   quantized,
		QuantizedRerank:         quantizedRerank,
		ScalarU8QuantizedRerank: columnGraph && scalarU8QuantizedRerankCapabilityDeclared(vectorDef),
		RabitQ1BitExperimental:  columnGraph && quantizedIndexCodecDeclared(vectorDef, "rabitq_1bit"),
	}
}

func quantizedIndexInfos(def collections.VectorIndexDefinition) []QuantizedIndexInfo {
	if len(def.QuantizedIndexes) == 0 {
		return nil
	}
	out := make([]QuantizedIndexInfo, len(def.QuantizedIndexes))
	for i, q := range def.QuantizedIndexes {
		out[i] = QuantizedIndexInfo{Name: q.Name, Codec: q.Codec, Version: q.Version, ScalarU8Calibration: q.ScalarU8Calibration}
	}
	return out
}

func quantizedIndexCodecDeclared(def collections.VectorIndexDefinition, codec string) bool {
	for _, q := range def.QuantizedIndexes {
		if q.Codec == codec {
			return true
		}
	}
	return false
}

func quantizedIndexRerankCapabilityDeclared(def collections.VectorIndexDefinition) bool {
	for _, q := range def.QuantizedIndexes {
		codec := q.Codec
		if codec == "" {
			codec = collections.QuantizedVectorCodecScalarU8
		}
		return true
	}
	return false
}

func scalarU8QuantizedRerankCapabilityDeclared(def collections.VectorIndexDefinition) bool {
	for _, q := range def.QuantizedIndexes {
		codec := q.Codec
		if codec == "" {
			codec = collections.QuantizedVectorCodecScalarU8
		}
		if codec == collections.QuantizedVectorCodecScalarU8 {
			return true
		}
	}
	return false
}

func scalarU8CalibrationDefinitionIsLegacy(q collections.QuantizedVectorIndexDefinition) bool {
	if q.ScalarU8Calibration == nil {
		return true
	}
	return q.ScalarU8Calibration.Mode == "" || q.ScalarU8Calibration.Mode == collections.ScalarU8CalibrationModeLegacy
}

func scorePtr(score float64) *float64 {
	out := score
	return &out
}
