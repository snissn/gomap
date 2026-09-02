package collections

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type indexInsertSearchFixture2564 struct {
	db      *backenddb.DB
	col     *Collection
	def     VectorIndexDefinition
	rows    []hybridCloseoutFixtureRow2506
	ids     [][]byte
	rawDocs [][]byte
}

func TestIndexInsertSearchFixtureGuardrails2564(t *testing.T) {
	fixture := openIndexInsertSearchInsertedFixture2564(t, 64, 8, 4)
	defer func() { _ = fixture.db.Close() }()

	text, err := fixture.col.SearchHybridTextCandidates(HybridTextQuery{
		IndexName:      hybridCloseoutTextIndexName2506,
		Query:          "refund policy",
		CandidateLimit: 16,
	})
	if err != nil {
		t.Fatalf("SearchHybridTextCandidates: %v", err)
	}
	if text.Stats.DocumentsFetched != 0 || text.Stats.FullDocumentScanFallbacks != 0 || text.Stats.FailClosed != 0 {
		t.Fatalf("text candidate stats=%+v want no document fetch/fallback/fail-closed", text.Stats)
	}

	vector, err := fixture.col.SearchHybridVectorCandidates(HybridVectorQuery{
		IndexName:      fixture.def.Name,
		Query:          hybridCloseoutQueryVector2506(8),
		CandidateLimit: 16,
		EfSearch:       32,
		QueryMode:      VectorIndexQueryModeExact,
	})
	if err != nil {
		t.Fatalf("SearchHybridVectorCandidates: %v", err)
	}
	if vector.Stats.DocumentsFetched != 0 || vector.Stats.FullDocumentScanFallbacks != 0 || vector.Stats.FailClosed != 0 {
		t.Fatalf("vector candidate stats=%+v want no document fetch/fallback/fail-closed", vector.Stats)
	}

	hybrid, err := fixture.col.SearchHybrid(indexInsertSearchHybridOptions2564(fixture.def.Name, 8, true))
	if err != nil {
		t.Fatalf("SearchHybrid: %v", err)
	}
	if len(hybrid.Results) == 0 {
		t.Fatalf("SearchHybrid returned no results")
	}
	if hybrid.Stats.DocumentsFetched == 0 || hybrid.Stats.DocumentsFetched > 10 || hybrid.Stats.FullDocumentScanFallbacks != 0 || hybrid.Stats.FailClosed != 0 {
		t.Fatalf("hybrid stats=%+v want bounded final fetch only", hybrid.Stats)
	}
}

var (
	indexInsertSearchHybridSink2564    HybridSearchResponse
	indexInsertSearchCandidateSink2564 HybridCandidateResponse
)

func BenchmarkIndexInsertSearch2564(b *testing.B) {
	docs := indexInsertSearchEnvInt2564("TREEDB_INDEX_BENCH_DOCS", 256)
	dims := indexInsertSearchEnvInt2564("TREEDB_INDEX_BENCH_DIMS", 16)
	m := indexInsertSearchEnvInt2564("TREEDB_INDEX_BENCH_M", 8)
	if docs < 64 {
		docs = 64
	}
	if dims < 3 {
		dims = 3
	}
	if m < 1 {
		m = 1
	}

	b.Run("indexed_insert_batch_flush_vector_rebuild", func(b *testing.B) {
		benchmarkIndexInsertBatch2564(b, docs, dims, m)
	})

	searchFixture := openIndexInsertSearchInsertedFixture2564(b, docs, dims, m)
	defer func() { _ = searchFixture.db.Close() }()
	queryVector := hybridCloseoutQueryVector2506(dims)

	b.Run("search_text_candidates_no_docs", func(b *testing.B) {
		opts := HybridTextQuery{IndexName: hybridCloseoutTextIndexName2506, Query: "refund policy", CandidateLimit: 64}
		warm, err := searchFixture.col.SearchHybridTextCandidates(opts)
		if err != nil {
			b.Fatalf("warm SearchHybridTextCandidates: %v", err)
		}
		if warm.Stats.DocumentsFetched != 0 || warm.Stats.FullDocumentScanFallbacks != 0 || warm.Stats.FailClosed != 0 {
			b.Fatalf("warm text stats=%+v want no document fetch/fallback/fail-closed", warm.Stats)
		}
		b.ReportAllocs()
		b.ResetTimer()
		var sink HybridCandidateResponse
		for i := 0; i < b.N; i++ {
			got, err := searchFixture.col.SearchHybridTextCandidates(opts)
			if err != nil {
				b.Fatalf("SearchHybridTextCandidates: %v", err)
			}
			if got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 {
				b.Fatalf("text stats=%+v want no document fetch/fallback/fail-closed", got.Stats)
			}
			sink = got
		}
		b.StopTimer()
		indexInsertSearchReportFixtureMetrics2564(b, docs, dims, 64, 10)
		indexInsertSearchReportCandidateStats2564(b, sink)
		indexInsertSearchCandidateSink2564 = sink
	})

	b.Run("search_text_v2_candidates_no_docs", func(b *testing.B) {
		v2Fixture := openIndexInsertSearchInsertedTextV2Fixture2564(b, docs, dims, m)
		defer func() { _ = v2Fixture.db.Close() }()
		opts := HybridTextQuery{IndexName: hybridCloseoutTextIndexName2506, Query: "refund policy", CandidateLimit: 64}
		warm, err := v2Fixture.col.SearchHybridTextCandidates(opts)
		if err != nil {
			b.Fatalf("warm v2 SearchHybridTextCandidates: %v", err)
		}
		if warm.Stats.DocumentsFetched != 0 || warm.Stats.FullDocumentScanFallbacks != 0 || warm.Stats.FailClosed != 0 || warm.Stats.TextStateLookups != 0 || warm.Stats.TextMatchDetailsBuilt != 0 {
			b.Fatalf("warm v2 text stats=%+v want score-only no document/state/match-detail work", warm.Stats)
		}
		b.ReportAllocs()
		b.ResetTimer()
		var sink HybridCandidateResponse
		for i := 0; i < b.N; i++ {
			got, err := v2Fixture.col.SearchHybridTextCandidates(opts)
			if err != nil {
				b.Fatalf("v2 SearchHybridTextCandidates: %v", err)
			}
			if got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 || got.Stats.TextStateLookups != 0 || got.Stats.TextMatchDetailsBuilt != 0 {
				b.Fatalf("v2 text stats=%+v want score-only no document/state/match-detail work", got.Stats)
			}
			sink = got
		}
		b.StopTimer()
		indexInsertSearchReportFixtureMetrics2564(b, docs, dims, 64, 10)
		indexInsertSearchReportCandidateStats2564(b, sink)
		indexInsertSearchCandidateSink2564 = sink
	})

	b.Run("search_hybrid_v2_no_docs_scalar_filter", func(b *testing.B) {
		v2Fixture := openIndexInsertSearchInsertedTextV2Fixture2564(b, docs, dims, m)
		defer func() { _ = v2Fixture.db.Close() }()
		opts := HybridSearchOptions{
			TopK:         10,
			Text:         &HybridTextQuery{IndexName: hybridCloseoutTextIndexName2506, Query: "refund policy", CandidateLimit: 64},
			ScalarFilter: &HybridScalarFilter{IndexName: hybridCloseoutTenantIndexName2506, Value: "tenant-rare-06pct"},
		}
		warm, err := v2Fixture.col.SearchHybrid(opts)
		if err != nil {
			b.Fatalf("warm v2 SearchHybrid: %v", err)
		}
		if warm.Stats.FailClosed != 0 || warm.Stats.FullDocumentScanFallbacks != 0 || warm.Stats.DocumentsFetched != 0 || warm.Stats.TextStateLookups != 0 || warm.Stats.TextMatchDetailsBuilt != 0 {
			b.Fatalf("warm v2 hybrid stats=%+v want no-doc score-only text path", warm.Stats)
		}
		b.ReportAllocs()
		b.ResetTimer()
		var sink HybridSearchResponse
		for i := 0; i < b.N; i++ {
			got, err := v2Fixture.col.SearchHybrid(opts)
			if err != nil {
				b.Fatalf("v2 SearchHybrid: %v", err)
			}
			if got.Stats.FailClosed != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.DocumentsFetched != 0 || got.Stats.TextStateLookups != 0 || got.Stats.TextMatchDetailsBuilt != 0 {
				b.Fatalf("v2 hybrid stats=%+v want no-doc score-only text path", got.Stats)
			}
			sink = got
		}
		b.StopTimer()
		indexInsertSearchReportFixtureMetrics2564(b, docs, dims, 64, opts.TopK)
		b.ReportMetric(hybridCloseoutTenantSelectivity2506(v2Fixture.rows, "tenant-rare-06pct"), "filter_selectivity_pct")
		hybridCloseoutReportStats2506(b, sink)
		indexInsertSearchHybridSink2564 = sink
	})

	b.Run("search_vector_candidates_no_docs", func(b *testing.B) {
		opts := HybridVectorQuery{IndexName: searchFixture.def.Name, Query: queryVector, CandidateLimit: 64, EfSearch: 128, QueryMode: VectorIndexQueryModeExact}
		warm, err := searchFixture.col.SearchHybridVectorCandidates(opts)
		if err != nil {
			b.Fatalf("warm SearchHybridVectorCandidates: %v", err)
		}
		if warm.Stats.DocumentsFetched != 0 || warm.Stats.FullDocumentScanFallbacks != 0 || warm.Stats.FailClosed != 0 {
			b.Fatalf("warm vector stats=%+v want no document fetch/fallback/fail-closed", warm.Stats)
		}
		b.ReportAllocs()
		b.ResetTimer()
		var sink HybridCandidateResponse
		for i := 0; i < b.N; i++ {
			got, err := searchFixture.col.SearchHybridVectorCandidates(opts)
			if err != nil {
				b.Fatalf("SearchHybridVectorCandidates: %v", err)
			}
			if got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 {
				b.Fatalf("vector stats=%+v want no document fetch/fallback/fail-closed", got.Stats)
			}
			sink = got
		}
		b.StopTimer()
		indexInsertSearchReportFixtureMetrics2564(b, docs, dims, 64, 10)
		indexInsertSearchReportCandidateStats2564(b, sink)
		indexInsertSearchCandidateSink2564 = sink
	})

	for _, includeDocs := range []bool{false, true} {
		includeDocs := includeDocs
		name := "search_hybrid_no_docs_scalar_filter"
		if includeDocs {
			name = "search_hybrid_fetch_topk_scalar_filter"
		}
		b.Run(name, func(b *testing.B) {
			opts := indexInsertSearchHybridOptions2564(searchFixture.def.Name, dims, includeDocs)
			warm, err := searchFixture.col.SearchHybrid(opts)
			if err != nil {
				b.Fatalf("warm SearchHybrid: %v", err)
			}
			if warm.Stats.FailClosed != 0 || warm.Stats.FullDocumentScanFallbacks != 0 {
				b.Fatalf("warm hybrid stats=%+v want fail-closed-free query", warm.Stats)
			}
			if warm.Stats.DocumentsFetched > uint64(opts.TopK) {
				b.Fatalf("warm hybrid stats=%+v fetched more than topK=%d", warm.Stats, opts.TopK)
			}
			if !includeDocs && warm.Stats.DocumentsFetched != 0 {
				b.Fatalf("warm hybrid stats=%+v fetched documents in no-doc mode", warm.Stats)
			}
			b.ReportAllocs()
			b.ResetTimer()
			var sink HybridSearchResponse
			for i := 0; i < b.N; i++ {
				got, err := searchFixture.col.SearchHybrid(opts)
				if err != nil {
					b.Fatalf("SearchHybrid: %v", err)
				}
				if got.Stats.FailClosed != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
					b.Fatalf("hybrid stats=%+v want fail-closed-free query", got.Stats)
				}
				if got.Stats.DocumentsFetched > uint64(opts.TopK) {
					b.Fatalf("hybrid stats=%+v fetched more than topK=%d", got.Stats, opts.TopK)
				}
				if !includeDocs && got.Stats.DocumentsFetched != 0 {
					b.Fatalf("hybrid stats=%+v fetched documents in no-doc mode", got.Stats)
				}
				sink = got
			}
			b.StopTimer()
			indexInsertSearchReportFixtureMetrics2564(b, docs, dims, 64, opts.TopK)
			b.ReportMetric(hybridCloseoutTenantSelectivity2506(searchFixture.rows, "tenant-rare-06pct"), "filter_selectivity_pct")
			hybridCloseoutReportStats2506(b, sink)
			indexInsertSearchHybridSink2564 = sink
		})
	}
}

func benchmarkIndexInsertBatch2564(b *testing.B, docs, dims, m int) {
	rows := makeHybridCloseoutRows2506(docs, dims)
	ids, rawDocs := indexInsertSearchRawDocs2564(b, rows)
	baseDir := b.TempDir()
	var insertElapsed, flushElapsed, rebuildElapsed time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		fixture := openIndexInsertSearchEmptyFixture2564(b, filepath.Join(baseDir, fmt.Sprintf("iter-%06d", i)), docs, dims, m)
		b.StartTimer()

		insertStart := time.Now()
		if _, err := fixture.col.InsertBatch(ids, rawDocs); err != nil {
			b.Fatalf("InsertBatch: %v", err)
		}
		insertElapsed += time.Since(insertStart)

		flushStart := time.Now()
		if err := fixture.col.Flush(); err != nil {
			b.Fatalf("Flush: %v", err)
		}
		flushElapsed += time.Since(flushStart)

		rebuildStart := time.Now()
		if _, err := fixture.col.RebuildVectorIndex(fixture.def.Name); err != nil {
			b.Fatalf("RebuildVectorIndex: %v", err)
		}
		rebuildElapsed += time.Since(rebuildStart)

		b.StopTimer()
		_ = fixture.db.Close()
	}
	b.StopTimer()
	indexInsertSearchReportFixtureMetrics2564(b, docs, dims, 64, 10)
	b.ReportMetric(2, "scalar_indexes/doc")
	b.ReportMetric(1, "text_indexes/doc")
	b.ReportMetric(1, "vector_indexes/doc")
	b.ReportMetric(float64(docs), "docs/op")
	if b.N > 0 && docs > 0 {
		denom := float64(b.N * docs)
		b.ReportMetric(float64(insertElapsed.Nanoseconds())/denom, "insert_batch_ns/doc")
		b.ReportMetric(float64(flushElapsed.Nanoseconds())/denom, "flush_ns/doc")
		b.ReportMetric(float64(rebuildElapsed.Nanoseconds())/denom, "vector_rebuild_ns/doc")
	}
}

func openIndexInsertSearchInsertedFixture2564(tb testing.TB, docs, dims, m int) indexInsertSearchFixture2564 {
	tb.Helper()
	fixture := openIndexInsertSearchEmptyFixture2564(tb, tb.TempDir(), docs, dims, m)
	if _, err := fixture.col.InsertBatch(fixture.ids, fixture.rawDocs); err != nil {
		_ = fixture.db.Close()
		tb.Fatalf("InsertBatch: %v", err)
	}
	if err := fixture.col.Flush(); err != nil {
		_ = fixture.db.Close()
		tb.Fatalf("Flush: %v", err)
	}
	if _, err := fixture.col.RebuildVectorIndex(fixture.def.Name); err != nil {
		_ = fixture.db.Close()
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	return fixture
}

func openIndexInsertSearchEmptyFixture2564(tb testing.TB, dir string, docs, dims, m int) indexInsertSearchFixture2564 {
	tb.Helper()
	textDef := TextIndexDefinition{
		Name:    hybridCloseoutTextIndexName2506,
		Version: TextIndexVersionV1,
		Fields: []TextIndexField{
			{Field: "title", Weight: 3},
			{Field: "body"},
		},
		StorePositions: true,
	}
	return openIndexInsertSearchEmptyFixtureWithTextDefinition2564(tb, dir, docs, dims, m, &textDef, true)
}

func openIndexInsertSearchEmptyFixtureWithTextDefinition2564(tb testing.TB, dir string, docs, dims, m int, textDef *TextIndexDefinition, commandWAL bool) indexInsertSearchFixture2564 {
	tb.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		tb.Fatalf("MkdirAll: %v", err)
	}
	var db *backenddb.DB
	if commandWAL {
		if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
			tb.Fatalf("SaveFormatConfig: %v", err)
		}
		db = openCollectionCommandWALDB(tb, dir)
	} else {
		var err error
		db, err = backenddb.Open(backenddb.Options{Dir: dir})
		if err != nil {
			tb.Fatalf("open db: %v", err)
		}
	}
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
	}
	if textDef != nil {
		meta.TextIndexes = []TextIndexDefinition{*textDef}
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
	ids, rawDocs := indexInsertSearchRawDocs2564(tb, rows)
	return indexInsertSearchFixture2564{db: db, col: col, def: def, rows: rows, ids: ids, rawDocs: rawDocs}
}

func openIndexInsertSearchInsertedTextV2Fixture2564(tb testing.TB, docs, dims, m int) indexInsertSearchFixture2564 {
	tb.Helper()
	dir := tb.TempDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		tb.Fatalf("MkdirAll: %v", err)
	}
	db, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		Indexes: []IndexDefinition{
			{Name: hybridCloseoutTenantIndexName2506, Field: "tenant", ValueType: IndexValueString},
			{Name: "region", Field: "region", ValueType: IndexValueString},
		},
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
	ids, rawDocs := indexInsertSearchRawDocs2564(tb, rows)
	fixture := indexInsertSearchFixture2564{db: db, col: col, rows: rows, ids: ids, rawDocs: rawDocs}
	if _, err := fixture.col.InsertBatch(fixture.ids, fixture.rawDocs); err != nil {
		_ = fixture.db.Close()
		tb.Fatalf("InsertBatch: %v", err)
	}
	textDef := TextIndexDefinition{
		Name:    hybridCloseoutTextIndexName2506,
		Version: TextIndexVersionV2,
		Fields: []TextIndexField{
			{Field: "title", Weight: 3},
			{Field: "body"},
		},
	}
	if _, _, err := fixture.col.CreateTextIndex(textDef); err != nil {
		_ = fixture.db.Close()
		tb.Fatalf("CreateTextIndex v2: %v", err)
	}
	if err := fixture.col.Flush(); err != nil {
		_ = fixture.db.Close()
		tb.Fatalf("Flush: %v", err)
	}
	return fixture
}

func indexInsertSearchRawDocs2564(tb testing.TB, rows []hybridCloseoutFixtureRow2506) ([][]byte, [][]byte) {
	tb.Helper()
	ids := make([][]byte, len(rows))
	rawDocs := make([][]byte, len(rows))
	for i, row := range rows {
		ids[i] = []byte(row.id)
		rawDocs[i] = mustHybridCloseoutDocument2506(tb, row, i+1)
	}
	return ids, rawDocs
}

func indexInsertSearchHybridOptions2564(vectorIndexName string, dims int, includeDocs bool) HybridSearchOptions {
	opts := HybridSearchOptions{
		TopK:         10,
		Text:         &HybridTextQuery{IndexName: hybridCloseoutTextIndexName2506, Query: "refund policy", CandidateLimit: 64},
		Vector:       &HybridVectorQuery{IndexName: vectorIndexName, Query: hybridCloseoutQueryVector2506(dims), CandidateLimit: 64, EfSearch: 128, QueryMode: VectorIndexQueryModeExact},
		ScalarFilter: &HybridScalarFilter{IndexName: hybridCloseoutTenantIndexName2506, Value: "tenant-rare-06pct"},
	}
	if includeDocs {
		opts.IncludeDocuments = true
		opts.DocumentFetchOptions = DocumentFetchOptions{ExcludePaths: []string{"embedding"}}
	}
	return opts
}

func indexInsertSearchReportFixtureMetrics2564(b *testing.B, docs, dims, candidateLimit, topK int) {
	b.Helper()
	b.ReportMetric(float64(docs), "docs_fixture")
	b.ReportMetric(float64(dims), "vector_dims")
	b.ReportMetric(float64(candidateLimit), "candidate_budget/source")
	b.ReportMetric(float64(topK), "topk/search")
}

func indexInsertSearchReportCandidateStats2564(b *testing.B, response HybridCandidateResponse) {
	b.Helper()
	b.ReportMetric(float64(len(response.Candidates)), "candidates/search")
	b.ReportMetric(float64(response.Stats.TextCandidatesRequested), "text_requested/search")
	b.ReportMetric(float64(response.Stats.TextCandidatesReturned), "text_candidates/search")
	b.ReportMetric(float64(response.Stats.TextPostingsScanned), "text_postings/search")
	b.ReportMetric(float64(response.Stats.TextPostingBlocksVisited), "posting_blocks_visited/search")
	b.ReportMetric(float64(response.Stats.TextPostingBlocksSkipped), "posting_blocks_skipped/search")
	b.ReportMetric(float64(response.Stats.TextBlockMaxFallbacks), "blockmax_fallbacks/search")
	b.ReportMetric(float64(response.Stats.TextBlockMaxThresholds), "threshold_updates/search")
	b.ReportMetric(float64(response.Stats.TextWANDPivots), "wand_pivots/search")
	b.ReportMetric(float64(response.Stats.TextScalarPrefilterIDs), "text_scalar_prefilter_ids/search")
	b.ReportMetric(float64(response.Stats.TextScalarPostingBlocksSkipped), "text_scalar_posting_blocks_skipped/search")
	b.ReportMetric(float64(response.Stats.TextScalarPostingsRejected), "text_scalar_postings_rejected/search")
	b.ReportMetric(float64(response.Stats.TextCandidatesScored), "text_scored/search")
	b.ReportMetric(float64(response.Stats.TextStateLookups), "text_state_lookups/search")
	b.ReportMetric(float64(response.Stats.TextNormLookups), "text_norm_lookups/search")
	b.ReportMetric(float64(response.Stats.TextMatchDetailsBuilt), "text_match_details/search")
	b.ReportMetric(float64(response.Stats.VectorCandidatesRequested), "vector_requested/search")
	b.ReportMetric(float64(response.Stats.VectorCandidatesReturned), "vector_candidates/search")
	b.ReportMetric(float64(response.Stats.VectorCandidatesExamined), "vector_examined/search")
	b.ReportMetric(float64(response.Stats.VectorEdgesVisited), "vector_edges/search")
	b.ReportMetric(float64(response.Stats.DocumentsFetched), "docs_fetched/search")
	b.ReportMetric(float64(response.Stats.FullDocumentScanFallbacks), "full_doc_fallbacks/search")
	b.ReportMetric(float64(response.Stats.Truncated), "truncated/search")
	b.ReportMetric(float64(response.Stats.FailClosed), "fail_closed/search")
}

func indexInsertSearchEnvInt2564(name string, fallback int) int {
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
