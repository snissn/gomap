package collections

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	hybridCloseoutTextIndexName2506   = "lexical"
	hybridCloseoutTenantIndexName2506 = "tenant"
)

type hybridCloseoutFixtureRow2506 struct {
	id     string
	title  string
	body   string
	tenant string
	region string
	vector []float32
}

type hybridCloseoutFixture2506 struct {
	db   *backenddb.DB
	col  *Collection
	def  VectorIndexDefinition
	rows []hybridCloseoutFixtureRow2506
}

func TestHybridCloseoutFixtureCandidateGenerationFetchesNoDocuments2506(t *testing.T) {
	fixture := openHybridCloseoutFixture2506(t, 64, 8, 4)
	defer func() { _ = fixture.db.Close() }()

	textCandidates, err := fixture.col.SearchHybridTextCandidates(HybridTextQuery{
		IndexName:      hybridCloseoutTextIndexName2506,
		Query:          "refund policy",
		CandidateLimit: 16,
	})
	if err != nil {
		t.Fatalf("SearchHybridTextCandidates: %v", err)
	}
	if textCandidates.Stats.DocumentsFetched != 0 || textCandidates.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("text candidate stats=%+v want zero document fetches/fallbacks", textCandidates.Stats)
	}

	vectorCandidates, err := fixture.col.SearchHybridVectorCandidates(HybridVectorQuery{
		IndexName:      fixture.def.Name,
		Query:          hybridCloseoutQueryVector2506(8),
		CandidateLimit: 16,
		EfSearch:       32,
		QueryMode:      VectorIndexQueryModeExact,
	})
	if err != nil {
		t.Fatalf("SearchHybridVectorCandidates: %v", err)
	}
	if vectorCandidates.Stats.DocumentsFetched != 0 || vectorCandidates.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("vector candidate stats=%+v want zero document fetches/fallbacks", vectorCandidates.Stats)
	}

	hybrid, err := fixture.col.SearchHybrid(HybridSearchOptions{
		TopK: 5,
		Text: &HybridTextQuery{
			IndexName:      hybridCloseoutTextIndexName2506,
			Query:          "refund policy",
			CandidateLimit: 16,
		},
		Vector: &HybridVectorQuery{
			IndexName:      fixture.def.Name,
			Query:          hybridCloseoutQueryVector2506(8),
			CandidateLimit: 16,
			EfSearch:       32,
			QueryMode:      VectorIndexQueryModeExact,
		},
		ScalarFilter:     &HybridScalarFilter{IndexName: hybridCloseoutTenantIndexName2506, Value: "tenant-rare-06pct"},
		IncludeDocuments: true,
		DocumentFetchOptions: DocumentFetchOptions{
			ExcludePaths: []string{"embedding"},
		},
	})
	if err != nil {
		t.Fatalf("SearchHybrid: %v", err)
	}
	if len(hybrid.Results) == 0 || hybrid.Stats.DocumentsFetched == 0 || hybrid.Stats.DocumentsFetched > 5 || hybrid.Stats.FullDocumentScanFallbacks != 0 || hybrid.Stats.FailClosed != 0 {
		t.Fatalf("hybrid stats=%+v results=%d want bounded final fetch only", hybrid.Stats, len(hybrid.Results))
	}
	for _, result := range hybrid.Results {
		if !result.DocumentFound || len(result.Document) == 0 {
			t.Fatalf("hybrid result=%+v missing final document", result)
		}
		if containsJSONField2506(result.Document, "embedding") {
			t.Fatalf("hybrid result document still includes embedding: %s", result.Document)
		}
	}
}

var hybridCloseoutBenchmarkSink2506 HybridSearchResponse

func BenchmarkSearchHybridCloseout2506(b *testing.B) {
	docs := hybridCloseoutEnvInt2506("TREEDB_HYBRID_BENCH_DOCS", 256)
	dims := hybridCloseoutEnvInt2506("TREEDB_HYBRID_BENCH_DIMS", 16)
	m := hybridCloseoutEnvInt2506("TREEDB_HYBRID_BENCH_M", 8)
	if docs < 64 {
		docs = 64
	}
	if dims < 3 {
		dims = 3
	}
	if m < 1 {
		m = 1
	}
	fixture := openHybridCloseoutFixture2506(b, docs, dims, m)
	defer func() { _ = fixture.db.Close() }()

	type filterCase struct {
		name        string
		filter      *HybridScalarFilter
		selectivity float64
	}
	allFilterCases := []filterCase{{name: "none_100pct", selectivity: 100}}
	rareFilterCases := []filterCase{{name: "rare_06pct", filter: &HybridScalarFilter{IndexName: hybridCloseoutTenantIndexName2506, Value: "tenant-rare-06pct"}, selectivity: hybridCloseoutTenantSelectivity2506(fixture.rows, "tenant-rare-06pct")}}
	selectivityCases := []filterCase{
		{name: "rare_06pct", filter: &HybridScalarFilter{IndexName: hybridCloseoutTenantIndexName2506, Value: "tenant-rare-06pct"}, selectivity: hybridCloseoutTenantSelectivity2506(fixture.rows, "tenant-rare-06pct")},
		{name: "narrow_25pct", filter: &HybridScalarFilter{IndexName: hybridCloseoutTenantIndexName2506, Value: "tenant-narrow-25pct"}, selectivity: hybridCloseoutTenantSelectivity2506(fixture.rows, "tenant-narrow-25pct")},
	}

	budgetCases := []struct {
		topK           int
		candidateLimit int
		filters        []filterCase
	}{
		{topK: 5, candidateLimit: 16, filters: append(allFilterCases, rareFilterCases...)},
		{topK: 10, candidateLimit: 64, filters: append(allFilterCases, selectivityCases...)},
	}

	modes := []struct {
		name             string
		withText         bool
		withVector       bool
		includeDocuments bool
	}{
		{name: "text_only_no_docs", withText: true},
		{name: "vector_only_no_docs", withVector: true},
		{name: "hybrid_no_docs", withText: true, withVector: true},
		{name: "hybrid_fetch_topk", withText: true, withVector: true, includeDocuments: true},
	}

	queryVector := hybridCloseoutQueryVector2506(dims)
	var sink HybridSearchResponse
	for _, mode := range modes {
		mode := mode
		b.Run("mode_"+mode.name, func(b *testing.B) {
			for _, budget := range budgetCases {
				budget := budget
				b.Run(fmt.Sprintf("topK_%d/candidates_%d", budget.topK, budget.candidateLimit), func(b *testing.B) {
					for _, fc := range budget.filters {
						fc := fc
						b.Run("filter_"+fc.name, func(b *testing.B) {
							opts := HybridSearchOptions{
								TopK:             budget.topK,
								ScalarFilter:     fc.filter,
								IncludeDocuments: mode.includeDocuments,
							}
							if mode.includeDocuments {
								opts.DocumentFetchOptions = DocumentFetchOptions{ExcludePaths: []string{"embedding"}}
							}
							if mode.withText {
								opts.Text = &HybridTextQuery{IndexName: hybridCloseoutTextIndexName2506, Query: "refund policy", CandidateLimit: budget.candidateLimit}
							}
							if mode.withVector {
								opts.Vector = &HybridVectorQuery{IndexName: fixture.def.Name, Query: queryVector, CandidateLimit: budget.candidateLimit, EfSearch: 128, QueryMode: VectorIndexQueryModeExact}
							}

							warm, err := fixture.col.SearchHybrid(opts)
							if err != nil {
								b.Fatalf("warm SearchHybrid: %v", err)
							}
							if warm.Stats.FailClosed != 0 || warm.Stats.FullDocumentScanFallbacks != 0 {
								b.Fatalf("warm stats=%+v want successful fail-closed-free query", warm.Stats)
							}
							if warm.Stats.DocumentsFetched > uint64(budget.topK) {
								b.Fatalf("warm stats=%+v fetched more than topK=%d", warm.Stats, budget.topK)
							}
							if !mode.includeDocuments && warm.Stats.DocumentsFetched != 0 {
								b.Fatalf("warm stats=%+v fetched documents in no-doc mode", warm.Stats)
							}

							b.ReportAllocs()
							b.ReportMetric(float64(docs), "docs_fixture")
							b.ReportMetric(float64(dims), "vector_dims")
							b.ReportMetric(float64(budget.topK), "topk/search")
							b.ReportMetric(float64(budget.candidateLimit), "candidate_budget/source")
							b.ReportMetric(fc.selectivity, "filter_selectivity_pct")
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								got, err := fixture.col.SearchHybrid(opts)
								if err != nil {
									b.Fatalf("SearchHybrid: %v", err)
								}
								if got.Stats.FailClosed != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
									b.Fatalf("stats=%+v want successful fail-closed-free query", got.Stats)
								}
								if got.Stats.DocumentsFetched > uint64(budget.topK) {
									b.Fatalf("stats=%+v fetched more than topK=%d", got.Stats, budget.topK)
								}
								if !mode.includeDocuments && got.Stats.DocumentsFetched != 0 {
									b.Fatalf("stats=%+v fetched documents in no-doc mode", got.Stats)
								}
								sink = got
							}
							b.StopTimer()
							hybridCloseoutReportStats2506(b, sink)
						})
					}
				})
			}
		})
	}
	hybridCloseoutBenchmarkSink2506 = sink
}

func openHybridCloseoutFixture2506(tb testing.TB, docs, dims, m int) hybridCloseoutFixture2506 {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	db := openCollectionCommandWALDB(tb, dir)
	def := columnGraphRebuildVectorIndexDefinitionV2A(dims, m)
	cfg := columnGraphRebuildColumnStoreConfigV2A(dims)
	cfg.RetainedPayload = ColumnRetainedPayloadFull
	cfg.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingJSON
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    cfg,
		},
		Indexes: []IndexDefinition{
			{Name: hybridCloseoutTenantIndexName2506, Field: "tenant", ValueType: IndexValueString},
			{Name: "region", Field: "region", ValueType: IndexValueString},
		},
		VectorIndexes: []VectorIndexDefinition{def},
		TextIndexes: []TextIndexDefinition{{
			Name: hybridCloseoutTextIndexName2506,
			Fields: []TextIndexField{
				{Field: "title", Weight: 3},
				{Field: "body"},
			},
			StorePositions: true,
		}},
	}
	if _, err := NewCollectionManager(db).CreateCollection(&meta); err != nil {
		_ = db.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(db).OpenCollection("docs")
	if err != nil {
		_ = db.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	rows := makeHybridCloseoutRows2506(docs, dims)
	ids := make([][]byte, len(rows))
	rawDocs := make([][]byte, len(rows))
	for i, row := range rows {
		ids[i] = []byte(row.id)
		rawDocs[i] = mustHybridCloseoutDocument2506(tb, row, i+1)
	}
	if _, err := col.InsertBatch(ids, rawDocs); err != nil {
		_ = db.Close()
		tb.Fatalf("InsertBatch: %v", err)
	}
	if err := col.Flush(); err != nil {
		_ = db.Close()
		tb.Fatalf("Flush: %v", err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = db.Close()
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	return hybridCloseoutFixture2506{db: db, col: col, def: def, rows: rows}
}

func makeHybridCloseoutRows2506(docs, dims int) []hybridCloseoutFixtureRow2506 {
	rows := make([]hybridCloseoutFixtureRow2506, docs)
	for i := range rows {
		topic := "shipping"
		title := "shipping status"
		body := fmt.Sprintf("shipping status update parcel route shard %d", i%17)
		if i%2 == 0 {
			topic = "refund"
			title = "refund policy"
			body = fmt.Sprintf("refund policy customer credit shard %d incident %d", i%17, i%31)
		}
		tenant := "tenant-common"
		if i%16 == 0 {
			tenant = "tenant-rare-06pct"
		} else if i%4 == 2 {
			tenant = "tenant-narrow-25pct"
		}
		rows[i] = hybridCloseoutFixtureRow2506{
			id:     fmt.Sprintf("doc-%04d", i),
			title:  title,
			body:   body,
			tenant: tenant,
			region: fmt.Sprintf("region-%d", i%5),
			vector: hybridCloseoutVector2506(dims, i, topic),
		}
	}
	return rows
}

func hybridCloseoutVector2506(dims, i int, topic string) []float32 {
	v := make([]float32, dims)
	if topic == "refund" {
		v[0] = 1
		v[1] = 0.10 + float32(i%7)*0.005
		v[2] = 0.05 + float32(i%11)*0.003
	} else {
		v[0] = 0.04 + float32(i%5)*0.004
		v[1] = 1
		v[2] = 0.15 + float32(i%13)*0.002
	}
	for j := 3; j < dims; j++ {
		v[j] = float32(((i+1)*(j+3))%23) * 0.002
	}
	return v
}

func hybridCloseoutQueryVector2506(dims int) []float32 {
	query := make([]float32, dims)
	query[0] = 1
	query[1] = 0.12
	query[2] = 0.06
	for i := 3; i < dims; i++ {
		query[i] = float32((i+5)%11) * 0.001
	}
	return query
}

func mustHybridCloseoutDocument2506(tb testing.TB, row hybridCloseoutFixtureRow2506, timeUS int) []byte {
	tb.Helper()
	raw, err := json.Marshal(map[string]any{
		"time_us":   int64(timeUS),
		"kind":      "hybrid-closeout",
		"did":       row.id,
		"tenant":    row.tenant,
		"region":    row.region,
		"title":     row.title,
		"body":      row.body,
		"embedding": row.vector,
	})
	if err != nil {
		tb.Fatalf("json.Marshal row %q: %v", row.id, err)
	}
	return raw
}

func hybridCloseoutTenantSelectivity2506(rows []hybridCloseoutFixtureRow2506, tenant string) float64 {
	if len(rows) == 0 {
		return 0
	}
	var matched int
	for _, row := range rows {
		if row.tenant == tenant {
			matched++
		}
	}
	return float64(matched) * 100 / float64(len(rows))
}

func hybridCloseoutReportStats2506(b *testing.B, response HybridSearchResponse) {
	b.Helper()
	b.ReportMetric(float64(len(response.Results)), "results/search")
	b.ReportMetric(float64(response.Stats.TextCandidatesRequested), "text_requested/search")
	b.ReportMetric(float64(response.Stats.TextCandidateBudgetEffective), "text_effective_budget/search")
	b.ReportMetric(float64(response.Stats.TextCandidatesReturned), "text_candidates/search")
	b.ReportMetric(float64(response.Stats.TextPostingsScanned), "text_postings/search")
	b.ReportMetric(float64(response.Stats.TextPostingBlocksVisited), "posting_blocks_visited/search")
	b.ReportMetric(float64(response.Stats.TextPostingBlocksSkipped), "posting_blocks_skipped/search")
	b.ReportMetric(float64(response.Stats.TextWANDPivots), "wand_pivots/search")
	b.ReportMetric(float64(response.Stats.TextScalarPrefilterIDs), "text_scalar_prefilter_ids/search")
	b.ReportMetric(float64(response.Stats.TextScalarPostingBlocksSkipped), "text_scalar_posting_blocks_skipped/search")
	b.ReportMetric(float64(response.Stats.TextScalarPostingsRejected), "text_scalar_postings_rejected/search")
	b.ReportMetric(float64(response.Stats.TextCandidatesScored), "text_scored/search")
	b.ReportMetric(float64(response.Stats.TextStateLookups), "text_state_lookups/search")
	b.ReportMetric(float64(response.Stats.TextNormLookups), "text_norm_lookups/search")
	b.ReportMetric(float64(response.Stats.TextMatchDetailsBuilt), "text_match_details/search")
	b.ReportMetric(float64(response.Stats.VectorCandidatesRequested), "vector_requested/search")
	b.ReportMetric(float64(response.Stats.VectorCandidateBudgetEffective), "vector_effective_budget/search")
	b.ReportMetric(float64(response.Stats.VectorCandidatesReturned), "vector_candidates/search")
	b.ReportMetric(float64(response.Stats.VectorCandidatesExamined), "vector_examined/search")
	b.ReportMetric(float64(response.Stats.VectorEdgesVisited), "vector_edges/search")
	b.ReportMetric(float64(response.Stats.ScalarPrefilterIDs), "scalar_prefilter_ids/search")
	b.ReportMetric(float64(response.Stats.ScalarPostfilterChecks), "scalar_postfilter_checks/search")
	b.ReportMetric(float64(response.Stats.ScalarFilterMatched), "scalar_matched/search")
	b.ReportMetric(float64(response.Stats.ScalarFilterRejected), "scalar_rejected/search")
	b.ReportMetric(float64(response.Stats.ScalarFilterSelectivityPPM), "scalar_selectivity_ppm/search")
	b.ReportMetric(float64(response.Stats.CandidatesFused), "candidates_fused/search")
	b.ReportMetric(float64(response.Stats.CandidatesAfterFusion), "candidates_after_fusion/search")
	b.ReportMetric(float64(response.Stats.CandidatesAfterFilter), "candidates_after_filter/search")
	b.ReportMetric(float64(response.Stats.FusionTextOnly), "fusion_text_only/search")
	b.ReportMetric(float64(response.Stats.FusionVectorOnly), "fusion_vector_only/search")
	b.ReportMetric(float64(response.Stats.FusionBoth), "fusion_both/search")
	b.ReportMetric(float64(response.Stats.FusionDuplicateCandidates), "fusion_duplicates/search")
	b.ReportMetric(float64(response.Stats.DocumentsFetched), "docs_fetched/search")
	b.ReportMetric(float64(response.Stats.DocumentsMissing), "docs_missing/search")
	b.ReportMetric(float64(response.Stats.FullDocumentScanFallbacks), "full_doc_fallbacks/search")
	b.ReportMetric(float64(response.Stats.Truncated), "truncated/search")
	if hybridCandidateBudgetPolicyIsAdaptive(response.Stats.CandidateBudgetPolicy) {
		b.ReportMetric(1, "adaptive_budget/search")
	} else {
		b.ReportMetric(0, "adaptive_budget/search")
	}
	b.ReportMetric(float64(response.Stats.CandidateBudgetIterations), "budget_iterations/search")
	b.ReportMetric(float64(response.Stats.CandidateBudgetFallbacks), "budget_fallbacks/search")
	b.ReportMetric(float64(response.Stats.FailClosed), "fail_closed/search")
}

func hybridCloseoutEnvInt2506(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func containsJSONField2506(raw []byte, field string) bool {
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return false
	}
	_, ok := decoded[field]
	return ok
}
