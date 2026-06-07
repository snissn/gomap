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
	defaultCollectionDocType = "treedb_document_service_v1"
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
	DenseVectorSearch bool `json:"dense_vector_search"`
	ExactDenseScoring bool `json:"exact_dense_scoring"`
	MetadataFilters   bool `json:"metadata_filters"`
	KeywordSearch     bool `json:"keyword_search"`
	HybridSearch      bool `json:"hybrid_search"`
}

// IndexInfo is returned by create/open and echoed by operation responses.
type IndexInfo struct {
	Name            string            `json:"name"`
	Dimension       int               `json:"dimension"`
	Metric          Metric            `json:"metric"`
	Generation      uint64            `json:"generation"`
	ContractVersion string            `json:"contract_version"`
	EmbeddingField  string            `json:"embedding_field"`
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

func indexCapabilities() IndexCapabilities {
	return IndexCapabilities{
		DenseVectorSearch: true,
		ExactDenseScoring: true,
		MetadataFilters:   true,
		KeywordSearch:     false,
		HybridSearch:      false,
	}
}

func scorePtr(score float64) *float64 {
	out := score
	return &out
}
