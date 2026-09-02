package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"github.com/snissn/gomap/TreeDB/collections/chunking"
)

const (
	applicationComparisonManifestSchema = "treedb-rag-application-comparison/v1"
	applicationComparisonProductBase    = "fcc1d79696a9b7c6655b793f6224f6937e698ce5"
)

type applicationComparisonConfig struct {
	TopK                int      `json:"top_k"`
	CandidateLimit      int      `json:"candidate_limit"`
	DenseVectorName     string   `json:"dense_vector_name"`
	SparseVectorName    string   `json:"sparse_vector_name"`
	DenseMetric         string   `json:"dense_metric"`
	LexicalFields       []string `json:"lexical_fields"`
	LexicalAnalyzer     string   `json:"lexical_analyzer"`
	SparseBM25K1        float64  `json:"sparse_bm25_k1"`
	SparseBM25B         float64  `json:"sparse_bm25_b"`
	Fusion              string   `json:"fusion"`
	ParentCollapse      string   `json:"parent_collapse"`
	WarmupsPerCell      int      `json:"warmups_per_cell"`
	SamplesPerCell      int      `json:"samples_per_cell"`
	Repetitions         int      `json:"repetitions"`
	PhaseTimeoutSeconds int      `json:"phase_timeout_seconds"`
}

type applicationComparisonFilter struct {
	ID             string `json:"id"`
	Tenant         string `json:"tenant,omitempty"`
	Workspace      string `json:"workspace,omitempty"`
	UpdatedYearGTE int    `json:"updated_year_gte,omitempty"`
}

type applicationComparisonChunk struct {
	ID          string    `json:"id"`
	ParentID    string    `json:"parent_id"`
	Ordinal     int       `json:"ordinal"`
	Content     string    `json:"content"`
	Tenant      string    `json:"tenant"`
	Workspace   string    `json:"workspace"`
	UpdatedYear int       `json:"updated_year"`
	DenseVector []float32 `json:"dense_vector"`
}

type applicationComparisonSource struct {
	ID          string   `json:"id"`
	Tenant      string   `json:"tenant"`
	Workspace   string   `json:"workspace"`
	SourceURI   string   `json:"source_uri"`
	SourceType  string   `json:"source_type"`
	ACL         []string `json:"acl"`
	UpdatedYear int      `json:"updated_year"`
	FinalBody   string   `json:"final_body"`
}

type applicationComparisonCase struct {
	Filter          string   `json:"filter"`
	RelevantChunks  []string `json:"relevant_chunks"`
	RelevantParents []string `json:"relevant_parents"`
}

type applicationComparisonQuery struct {
	ID          string                      `json:"id"`
	Text        string                      `json:"text"`
	DenseVector []float32                   `json:"dense_vector"`
	Cases       []applicationComparisonCase `json:"cases"`
}

type applicationComparisonManifest struct {
	Schema               string                        `json:"schema"`
	ProductBaseSHA       string                        `json:"product_base_sha"`
	FixtureSHA256        string                        `json:"fixture_sha256"`
	SemanticVectorSHA256 string                        `json:"semantic_vector_sha256"`
	ConfigSHA256         string                        `json:"config_sha256"`
	Config               applicationComparisonConfig   `json:"config"`
	Filters              []applicationComparisonFilter `json:"filters"`
	Sources              []applicationComparisonSource `json:"sources"`
	Chunks               []applicationComparisonChunk  `json:"chunks"`
	Queries              []applicationComparisonQuery  `json:"queries"`
}

func defaultApplicationComparisonConfig() applicationComparisonConfig {
	return applicationComparisonConfig{
		TopK: 10, CandidateLimit: 32,
		DenseVectorName: "dense_minilm", SparseVectorName: "sparse_bm25",
		DenseMetric: "cosine", LexicalFields: []string{"content"},
		LexicalAnalyzer: "lowercase ASCII alphanumeric tokens; no stopword removal",
		SparseBM25K1:    1.2, SparseBM25B: 0.75, Fusion: "qdrant_query_api_rrf",
		ParentCollapse: "disabled; raw chunks returned; parent recall derived from parent_id",
		WarmupsPerCell: 20, SamplesPerCell: 100, Repetitions: 3, PhaseTimeoutSeconds: 90,
	}
}

func applicationComparisonConfigDigest(config applicationComparisonConfig) string {
	raw, _ := json.Marshal(config)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func buildApplicationComparisonManifest() (applicationComparisonManifest, error) {
	fixture := buildApplicationFixture()
	if err := validateApplicationFixture(&fixture); err != nil {
		return applicationComparisonManifest{}, err
	}
	if got := applicationFixtureDigest(&fixture); got != applicationFixtureExpectedDigest {
		return applicationComparisonManifest{}, fmt.Errorf("comparison manifest: fixture SHA-256=%s want %s", got, applicationFixtureExpectedDigest)
	}
	vectors, err := loadSemanticVectors()
	if err != nil {
		return applicationComparisonManifest{}, err
	}
	if err := validateSemanticVectors(&fixture, vectors); err != nil {
		return applicationComparisonManifest{}, err
	}
	if got := vectors.Digest(); got != semanticVectorsExpectedDigest {
		return applicationComparisonManifest{}, fmt.Errorf("comparison manifest: vector SHA-256=%s want %s", got, semanticVectorsExpectedDigest)
	}
	config := defaultApplicationComparisonConfig()
	manifest := applicationComparisonManifest{
		Schema: applicationComparisonManifestSchema, ProductBaseSHA: applicationComparisonProductBase,
		FixtureSHA256: applicationFixtureExpectedDigest, SemanticVectorSHA256: semanticVectorsExpectedDigest,
		Config: config, ConfigSHA256: applicationComparisonConfigDigest(config),
		Filters: []applicationComparisonFilter{
			{ID: filterUnfiltered},
			{ID: filterTenantAlpha, Tenant: "alpha"},
			{ID: filterTenantAlphaWorkspaceRed, Tenant: "alpha", Workspace: "red"},
			{ID: filterModerateRange, Tenant: "alpha", Workspace: "red", UpdatedYearGTE: 2024},
		},
	}
	for _, source := range fixture.Sources {
		if source.Deleted {
			continue
		}
		manifest.Sources = append(manifest.Sources, applicationComparisonSource{
			ID: source.ID, Tenant: source.Tenant, Workspace: source.Workspace, SourceURI: source.SourceURI,
			SourceType: source.SourceType, ACL: append([]string(nil), source.ACL...), UpdatedYear: source.UpdatedYear,
			FinalBody: source.FinalBody,
		})
		for ordinal := range applicationChunksPerSource {
			content := source.FinalBody[ordinal*applicationChunkSize : (ordinal+1)*applicationChunkSize]
			manifest.Chunks = append(manifest.Chunks, applicationComparisonChunk{
				ID: chunking.ChildDocumentID(source.ID, ordinal), ParentID: source.ID, Ordinal: ordinal,
				Content: content, Tenant: source.Tenant, Workspace: source.Workspace, UpdatedYear: source.UpdatedYear,
				DenseVector: append([]float32(nil), vectors.Vectors[content]...),
			})
		}
	}
	judgments := applicationJudgmentMap(&fixture)
	for _, query := range fixture.Queries {
		row := applicationComparisonQuery{ID: query.ID, Text: query.Text, DenseVector: append([]float32(nil), vectors.Queries[query.ID]...)}
		for _, filter := range applicationFilterOrder {
			judgment := judgments[query.ID+"\x00"+filter]
			row.Cases = append(row.Cases, applicationComparisonCase{
				Filter: filter, RelevantChunks: append([]string(nil), judgment.RelevantChunks...),
				RelevantParents: append([]string(nil), judgment.RelevantParents...),
			})
		}
		manifest.Queries = append(manifest.Queries, row)
	}
	if len(manifest.Sources) != 18 || len(manifest.Chunks) != 54 || len(manifest.Queries) != 3 {
		return applicationComparisonManifest{}, fmt.Errorf("comparison manifest: cardinality sources/chunks/queries=%d/%d/%d want 18/54/3", len(manifest.Sources), len(manifest.Chunks), len(manifest.Queries))
	}
	return manifest, nil
}

func applicationComparisonManifestBytes() ([]byte, error) {
	manifest, err := buildApplicationComparisonManifest()
	if err != nil {
		return nil, err
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func writeApplicationComparisonManifest(path string) error {
	raw, err := applicationComparisonManifestBytes()
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}
