package collections

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestHybridSearchContractVocabulary2502(t *testing.T) {
	checks := map[string]string{
		"text source":            string(HybridCandidateSourceText),
		"vector source":          string(HybridCandidateSourceVector),
		"bm25 score":             string(HybridScoreKindBM25),
		"bm25f score":            string(HybridScoreKindBM25F),
		"vector score":           string(HybridScoreKindVectorSimilarity),
		"rrf method":             string(HybridFusionMethodRRF),
		"weighted rrf method":    string(HybridFusionMethodWeightedRRF),
		"normalized method":      string(HybridFusionMethodNormalizedScore),
		"tie policy":             string(HybridFusionTiePolicyScoreBestRankSourceID),
		"compact result mode":    string(HybridResultModeCompact),
		"score-only result mode": string(HybridResultModeScoreOnly),
		"full result mode":       string(HybridResultModeFull),
		"prefilter strategy":     string(HybridScalarFilterStrategyPrefilter),
		"postfilter strategy":    string(HybridScalarFilterStrategyPostfilter),
		"text-first strategy":    string(HybridScalarFilterStrategyTextFirst),
		"vector-first strategy":  string(HybridScalarFilterStrategyVectorFirst),
		"union-fusion strategy":  string(HybridScalarFilterStrategyUnionFusion),
		"current snapshot mode":  string(HybridConsistencyCurrentSnapshot),
		"bound snapshot mode":    string(HybridConsistencyBoundSnapshot),
		"unsupported reason":     string(HybridFailClosedReasonUnsupported),
		"scan-forbidden reason":  string(HybridFailClosedReasonFullDocumentScanForbidden),
	}
	wants := map[string]string{
		"text source":            "text",
		"vector source":          "vector",
		"bm25 score":             "bm25",
		"bm25f score":            "bm25f",
		"vector score":           "vector_similarity",
		"rrf method":             "rrf",
		"weighted rrf method":    "weighted_rrf",
		"normalized method":      "normalized_score",
		"tie policy":             "fused_score_best_rank_source_order_id",
		"compact result mode":    "compact",
		"score-only result mode": "score_only",
		"full result mode":       "full",
		"prefilter strategy":     "prefilter",
		"postfilter strategy":    "postfilter",
		"text-first strategy":    "text_first",
		"vector-first strategy":  "vector_first",
		"union-fusion strategy":  "union_fusion",
		"current snapshot mode":  "current_snapshot",
		"bound snapshot mode":    "bound_snapshot",
		"unsupported reason":     "unsupported",
		"scan-forbidden reason":  "full_document_scan_forbidden",
	}
	for name, got := range checks {
		if want := wants[name]; got != want {
			t.Fatalf("%s=%q want %q", name, got, want)
		}
	}
}

func TestHybridSearchContractJSONNames2502(t *testing.T) {
	opts := HybridSearchOptions{
		TopK: 5,
		Text: &HybridTextQuery{IndexName: "body_text", Query: "tree db", CandidateLimit: 32},
		Vector: &HybridVectorQuery{
			IndexName:                 "embedding_graph",
			Query:                     []float32{1, 0},
			CandidateLimit:            64,
			EfSearch:                  128,
			QueryMode:                 VectorIndexQueryModeQuantizedRerank,
			QuantizedIndexName:        "embedding.rabitq_1bit.fast",
			QuantizedRerankCandidates: 24,
		},
		ScalarFilter:         &HybridScalarFilter{IndexName: "tenant", Value: "acme"},
		ScalarFilterStrategy: HybridScalarFilterStrategyUnionFusion,
		Fusion: HybridFusionOptions{
			Method:       HybridFusionMethodRRF,
			RRFK:         60,
			TiePolicy:    HybridFusionTiePolicyScoreBestRankSourceID,
			SourceOrder:  []HybridCandidateSource{HybridCandidateSourceText, HybridCandidateSourceVector},
			TextWeight:   1.2,
			VectorWeight: 0.8,
		},
		MaxChunksPerParent: 2,
		ResultMode:         HybridResultModeFull,
		IncludeDocuments:   true,
		DocumentFetchOptions: DocumentFetchOptions{
			ExcludePaths: []string{"embedding"},
		},
		Consistency: HybridConsistencyOptions{Mode: HybridConsistencyCurrentSnapshot},
		Debug:       HybridSearchDebugOptions{IncludeCandidates: true},
	}
	raw, err := json.Marshal(opts)
	if err != nil {
		t.Fatalf("Marshal HybridSearchOptions: %v", err)
	}
	jsonText := string(raw)
	for _, want := range []string{
		`"top_k":5`,
		`"candidate_limit":32`,
		`"candidate_limit":64`,
		`"ef_search":128`,
		`"query_mode":"quantized_rerank"`,
		`"quantized_index_name":"embedding.rabitq_1bit.fast"`,
		`"quantized_rerank_candidates":24`,
		`"scalar_filter_strategy":"union_fusion"`,
		`"rrf_k":60`,
		`"tie_policy":"fused_score_best_rank_source_order_id"`,
		`"source_order":["text","vector"]`,
		`"text_weight":1.2`,
		`"vector_weight":0.8`,
		`"max_chunks_per_parent":2`,
		`"result_mode":"full"`,
		`"include_documents":true`,
		`"ExcludePaths":["embedding"]`,
		`"mode":"current_snapshot"`,
		`"include_candidates":true`,
	} {
		if !strings.Contains(jsonText, want) {
			t.Fatalf("HybridSearchOptions JSON %s missing %s", jsonText, want)
		}
	}
}

func TestHybridSearchCandidateAndResultShape2502(t *testing.T) {
	candidate := HybridSearchCandidate{
		ID:         []byte("doc-1"),
		Source:     HybridCandidateSourceText,
		IndexName:  "body_text",
		SourceRank: 1,
		Score:      12.5,
		ScoreKind:  HybridScoreKindBM25F,
		TextMatches: []HybridTextMatch{{
			Field: "body",
			Terms: []string{"tree", "db"},
		}},
	}
	result := HybridSearchResult{
		ID:         candidate.ID,
		Rank:       1,
		FusedScore: 1.0 / 61.0,
		Sources: []HybridSourceContribution{{
			Source:      candidate.Source,
			IndexName:   candidate.IndexName,
			SourceRank:  candidate.SourceRank,
			Score:       candidate.Score,
			ScoreKind:   candidate.ScoreKind,
			FusionScore: 1.0 / 61.0,
			TextMatches: candidate.TextMatches,
		}},
	}
	if string(result.ID) != "doc-1" || result.Rank != 1 || result.FusedScore <= 0 {
		t.Fatalf("result=%+v want stable id, one-based rank, positive fused score", result)
	}
	if len(result.Sources) != 1 || result.Sources[0].SourceRank != 1 || result.Sources[0].ScoreKind != HybridScoreKindBM25F {
		t.Fatalf("sources=%+v want text contribution with source rank and score kind", result.Sources)
	}
	if len(result.Sources[0].TextMatches) != 1 || result.Sources[0].TextMatches[0].Field != "body" {
		t.Fatalf("text matches=%+v want matched field attribution", result.Sources[0].TextMatches)
	}
}

func TestSearchHybridValidationFailsClosed2502(t *testing.T) {
	response, err := (&Collection{db: &backenddb.DB{}}).SearchHybrid(HybridSearchOptions{TopK: 10})
	if !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("SearchHybrid err=%v want ErrHybridSearchUnsupported", err)
	}
	if len(response.Results) != 0 || response.Stats.FailClosed != 1 || response.Stats.FailClosedReason != HybridFailClosedReasonUnsupported || response.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("SearchHybrid response=%+v want fail-closed unsupported shape with no scan fallback", response)
	}

	nilResponse, err := (*Collection)(nil).SearchHybrid(HybridSearchOptions{})
	if !errors.Is(err, errCollectionNil) {
		t.Fatalf("nil SearchHybrid err=%v want errCollectionNil", err)
	}
	if nilResponse.Stats.FailClosed != 0 || nilResponse.Stats.FailClosedReason != "" {
		t.Fatalf("nil SearchHybrid response=%+v want zero stats for receiver validation failure", nilResponse)
	}

	nilDBResponse, err := (&Collection{}).SearchHybrid(HybridSearchOptions{})
	if !errors.Is(err, errCollectionDBNil) {
		t.Fatalf("nil db SearchHybrid err=%v want errCollectionDBNil", err)
	}
	if nilDBResponse.Stats.FailClosed != 0 || nilDBResponse.Stats.FailClosedReason != "" {
		t.Fatalf("nil db SearchHybrid response=%+v want zero stats for db validation failure", nilDBResponse)
	}
}
