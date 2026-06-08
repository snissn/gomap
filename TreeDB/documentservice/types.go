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
	ID        string         `json:"id"`
	Content   string         `json:"content,omitempty"`
	Embedding []float32      `json:"embedding,omitempty"`
	Score     *float64       `json:"score,omitempty"`
	Meta      map[string]any `json:"meta,omitempty"`
}

// IndexCapabilities describes the supported operations for one service index.
type IndexCapabilities struct {
	DenseVectorSearch      bool `json:"dense_vector_search"`
	ExactDenseScoring      bool `json:"exact_dense_scoring"`
	MetadataFilters        bool `json:"metadata_filters"`
	KeywordSearch          bool `json:"keyword_search"`
	HybridSearch           bool `json:"hybrid_search"`
	KeywordMetadataFilters bool `json:"keyword_metadata_filters"`
	HybridMetadataFilters  bool `json:"hybrid_metadata_filters"`
}

// IndexInfo is returned by create/open and echoed by operation responses.
type IndexInfo struct {
	Name            string            `json:"name"`
	Dimension       int               `json:"dimension"`
	Metric          Metric            `json:"metric"`
	Generation      uint64            `json:"generation"`
	ContractVersion string            `json:"contract_version"`
	EmbeddingField  string            `json:"embedding_field"`
	VectorIndexName string            `json:"vector_index_name"`
	TextField       string            `json:"text_field"`
	TextIndexName   string            `json:"text_index_name"`
	DocumentType    string            `json:"document_type"`
	Capabilities    IndexCapabilities `json:"capabilities"`
}

// CreateIndexRequest creates or opens a service index. Existing compatible
// indexes are returned idempotently; incompatible existing collections fail.
type CreateIndexRequest struct {
	Name      string `json:"name"`
	Dimension int    `json:"dimension"`
	Metric    Metric `json:"metric,omitempty"`
}

// UpsertDocumentsRequest writes or replaces service documents.
type UpsertDocumentsRequest struct {
	ExpectedGeneration uint64     `json:"expected_generation,omitempty"`
	Documents          []Document `json:"documents"`
}

type UpsertDocumentsResponse struct {
	Index    IndexInfo `json:"index"`
	Upserted int       `json:"upserted"`
	Inserted int       `json:"inserted"`
	Updated  int       `json:"updated"`
	IDs      []string  `json:"ids"`
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

func indexCapabilities(hybridSearch bool) IndexCapabilities {
	return IndexCapabilities{
		DenseVectorSearch:      true,
		ExactDenseScoring:      true,
		MetadataFilters:        true,
		KeywordSearch:          true,
		HybridSearch:           hybridSearch,
		KeywordMetadataFilters: false,
		HybridMetadataFilters:  false,
	}
}

func scorePtr(score float64) *float64 {
	out := score
	return &out
}
