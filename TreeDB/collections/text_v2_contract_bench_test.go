package collections

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	textV2ContractIndexName2623 = "lexical"
	textV2ContractRareTerm2623  = "raretoken2623"
)

type textV2ContractSearchCase2623 struct {
	name           string
	query          string
	operator       TextSearchOperator
	resultMode     textSearchResultMode
	candidateLimit int
	maxPostings    int
}

func TestTextV2ContractBenchmarkMatrix2623(t *testing.T) {
	sizes := textV2ContractDefaultCorpusSizes2623()
	for _, want := range []int{256, 10_000, 100_000} {
		if !textV2ContractContainsInt2623(sizes, want) {
			t.Fatalf("default corpus sizes=%v missing %d", sizes, want)
		}
	}
	cases := textV2ContractSearchCases2623(256)
	for _, want := range []string{"score_only_common_no_docs", "detailed_common_no_docs", "rare_no_docs", "multi_term_and_no_docs"} {
		if !textV2ContractContainsSearchCase2623(cases, want) {
			t.Fatalf("search cases=%v missing %q", textV2ContractSearchCaseNames2623(cases), want)
		}
	}
	counters := TextIndexV2RequiredCounterNames()
	for _, want := range []string{"posting_blocks_visited", "posting_blocks_skipped", "state_lookups", "norm_lookups", "docs_fetched", "match_details_built", "write_amplification", "index_bytes_per_doc"} {
		if !textV2ContractContainsString2623(counters, want) {
			t.Fatalf("required counters=%v missing %q", counters, want)
		}
	}
}

func TestTextV2ContractCounterGuardrails2623(t *testing.T) {
	d, col := openTextV2ContractSearchFixture2623(t, 128)
	defer func() { _ = d.Close() }()

	scoreOnly, err := col.searchText(TextSearchOptions{IndexName: textV2ContractIndexName2623, Query: "refund policy", TopK: 8, CandidateLimit: 128, MaxPostingsScanned: 256}, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("score-only search: %v", err)
	}
	detailed, err := col.searchText(TextSearchOptions{IndexName: textV2ContractIndexName2623, Query: "refund policy", TopK: 8, CandidateLimit: 128, MaxPostingsScanned: 256}, textSearchResultFull)
	if err != nil {
		t.Fatalf("detailed search: %v", err)
	}
	if scoreOnly.Stats.DocumentsFetched != 0 || detailed.Stats.DocumentsFetched != 0 || scoreOnly.Stats.FullDocumentScanFallbacks != 0 || detailed.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("score stats=%+v detailed stats=%+v want zero-doc no-fallback searches", scoreOnly.Stats, detailed.Stats)
	}
	if scoreOnly.Stats.TextMatchDetailsBuilt != 0 || detailed.Stats.TextMatchDetailsBuilt == 0 {
		t.Fatalf("score stats=%+v detailed stats=%+v want match-detail counter separation", scoreOnly.Stats, detailed.Stats)
	}
	if len(scoreOnly.Results) != len(detailed.Results) {
		t.Fatalf("score results=%d detailed results=%d", len(scoreOnly.Results), len(detailed.Results))
	}
	for i := range scoreOnly.Results {
		if !bytes.Equal(scoreOnly.Results[i].DocumentID, detailed.Results[i].DocumentID) || scoreOnly.Results[i].Score != detailed.Results[i].Score {
			t.Fatalf("result %d score-only=%+v detailed=%+v", i, scoreOnly.Results[i], detailed.Results[i])
		}
	}

	candidates, err := col.SearchHybridTextCandidates(HybridTextQuery{IndexName: textV2ContractIndexName2623, Query: "refund policy", CandidateLimit: 16})
	if err != nil {
		t.Fatalf("SearchHybridTextCandidates: %v", err)
	}
	if candidates.Stats.DocumentsFetched != 0 || candidates.Stats.FullDocumentScanFallbacks != 0 || candidates.Stats.TextStateLookups == 0 || candidates.Stats.TextNormLookups == 0 {
		t.Fatalf("candidate stats=%+v want zero-doc candidate counters", candidates.Stats)
	}

	hybrid, err := col.SearchHybrid(HybridSearchOptions{
		TopK:             5,
		Text:             &HybridTextQuery{IndexName: textV2ContractIndexName2623, Query: "refund policy", CandidateLimit: 64},
		ScalarFilter:     &HybridScalarFilter{IndexName: "tenant", Value: "tenant-rare-06pct"},
		IncludeDocuments: true,
		DocumentFetchOptions: DocumentFetchOptions{
			ExcludePaths: []string{"body"},
		},
	})
	if err != nil {
		t.Fatalf("SearchHybrid text-only final fetch: %v", err)
	}
	if hybrid.Stats.DocumentsFetched == 0 || hybrid.Stats.DocumentsFetched > 5 || hybrid.Stats.FullDocumentScanFallbacks != 0 || hybrid.Stats.FailClosed != 0 || hybrid.Stats.ScalarFilterSelectivityPPM == 0 {
		t.Fatalf("hybrid stats=%+v want bounded final fetch and scalar selectivity", hybrid.Stats)
	}
}

func BenchmarkTextV2ContractWritePaths2623(b *testing.B) {
	docsPerBatch := textV2ContractEnvInt2623("TREEDB_TEXT_V2_WRITE_DOCS", 256)
	if docsPerBatch < 1 {
		docsPerBatch = 1
	}
	ids, docs := textV2ContractDocuments2623(docsPerBatch, "insert")

	b.Run("insert_batch_no_text", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		b.ResetTimer()
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			d := openTextV2ContractDB2623(b)
			col := createTextV2ContractCollection2623(b, d, false)
			batchIDs := textV2ContractCloneIDsWithIteration2623(ids, i)
			b.StartTimer()
			_, err := col.InsertBatch(batchIDs, docs)
			b.StopTimer()
			if err != nil {
				b.Fatalf("InsertBatch no-text: %v", err)
			}
			_ = d.Close()
		}
		b.ReportMetric(float64(docsPerBatch), "docs/op")
	})

	b.Run("insert_batch_text_indexed", func(b *testing.B) {
		var lastStats TextIndexStorageStats
		b.ReportAllocs()
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		b.ResetTimer()
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			d := openTextV2ContractDB2623(b)
			col := createTextV2ContractCollection2623(b, d, true)
			batchIDs := textV2ContractCloneIDsWithIteration2623(ids, i)
			b.StartTimer()
			_, err := col.InsertBatch(batchIDs, docs)
			b.StopTimer()
			if err != nil {
				b.Fatalf("InsertBatch text-indexed: %v", err)
			}
			lastStats = textV2ContractStorageStats2623(b, col)
			_ = d.Close()
		}
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		textV2ContractReportWriteStats2623(b, docsPerBatch, lastStats, textV2ContractStorageEntryCount2623(lastStats))
	})

	b.Run("create_text_index_backfill", func(b *testing.B) {
		var lastStats TextIndexBackfillStats
		b.ReportAllocs()
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		b.ResetTimer()
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			d := openTextV2ContractDB2623(b)
			col := createTextV2ContractCollection2623(b, d, false)
			batchIDs := textV2ContractCloneIDsWithIteration2623(ids, i)
			if _, err := col.InsertBatch(batchIDs, docs); err != nil {
				b.Fatalf("InsertBatch setup: %v", err)
			}
			b.StartTimer()
			_, stats, err := col.CreateTextIndex(textV2ContractIndexDefinition2623())
			b.StopTimer()
			if err != nil {
				b.Fatalf("CreateTextIndex backfill: %v", err)
			}
			lastStats = stats
			_ = d.Close()
		}
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		textV2ContractReportBackfillStats2623(b, docsPerBatch, lastStats)
	})

	b.Run("update_batch_text_indexed", func(b *testing.B) {
		_, updatedDocs := textV2ContractDocuments2623(docsPerBatch, "update")
		var lastStats TextIndexStorageStats
		var writeAmpEntries uint64
		b.ReportAllocs()
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		b.ResetTimer()
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			d := openTextV2ContractDB2623(b)
			col := createTextV2ContractCollection2623(b, d, true)
			batchIDs := textV2ContractCloneIDsWithIteration2623(ids, i)
			if _, err := col.InsertBatch(batchIDs, docs); err != nil {
				b.Fatalf("InsertBatch setup: %v", err)
			}
			updates := make([]UpdateBatchItem, docsPerBatch)
			for j := 0; j < docsPerBatch; j++ {
				replacement := bytes.Clone(updatedDocs[j])
				updates[j] = UpdateBatchItem{DocumentID: batchIDs[j], Update: func(current []byte) ([]byte, bool, error) {
					return replacement, true, nil
				}}
			}
			b.StartTimer()
			results, err := col.UpdateBatch(updates)
			b.StopTimer()
			if err != nil {
				b.Fatalf("UpdateBatch text-indexed: %v", err)
			}
			if len(results) != docsPerBatch {
				b.Fatalf("UpdateBatch results=%d want %d", len(results), docsPerBatch)
			}
			lastStats = textV2ContractStorageStats2623(b, col)
			writeAmpEntries = 2*(lastStats.PostingEntries+lastStats.StateEntries) + lastStats.StatsEntries
			_ = d.Close()
		}
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		textV2ContractReportWriteStats2623(b, docsPerBatch, lastStats, writeAmpEntries)
	})

	b.Run("delete_batch_text_indexed", func(b *testing.B) {
		var lastStats TextIndexStorageStats
		var writeAmpEntries uint64
		b.ReportAllocs()
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		b.ResetTimer()
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			d := openTextV2ContractDB2623(b)
			col := createTextV2ContractCollection2623(b, d, true)
			batchIDs := textV2ContractCloneIDsWithIteration2623(ids, i)
			if _, err := col.InsertBatch(batchIDs, docs); err != nil {
				b.Fatalf("InsertBatch setup: %v", err)
			}
			beforeStats := textV2ContractStorageStats2623(b, col)
			b.StartTimer()
			deleted, err := col.DeleteBatch(batchIDs)
			b.StopTimer()
			if err != nil {
				b.Fatalf("DeleteBatch text-indexed: %v", err)
			}
			if deleted != docsPerBatch {
				b.Fatalf("DeleteBatch deleted=%d want %d", deleted, docsPerBatch)
			}
			lastStats = textV2ContractStorageStats2623(b, col)
			writeAmpEntries = textV2ContractStorageEntryCount2623(beforeStats)
			_ = d.Close()
		}
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		textV2ContractReportWriteStats2623(b, docsPerBatch, lastStats, writeAmpEntries)
	})
}

func BenchmarkTextV2ContractSearchScale2623(b *testing.B) {
	for _, docs := range textV2ContractSearchCorpusSizes2623() {
		docs := docs
		b.Run(fmt.Sprintf("docs_%d", docs), func(b *testing.B) {
			if docs >= 100_000 && !textV2ContractLargeCorpusEnabled2623() {
				b.Skip("set TREEDB_TEXT_V2_RUN_100K=1 or TREEDB_TEXT_V2_SEARCH_DOCS to run the >=100k local artifact row")
			}
			d, col := openTextV2ContractSearchFixture2623(b, docs)
			defer func() { _ = d.Close() }()
			for _, tc := range textV2ContractSearchCases2623(docs) {
				tc := tc
				b.Run(tc.name, func(b *testing.B) {
					warm, err := textV2ContractRunSearch2623(col, tc)
					if err != nil {
						b.Fatalf("warm search: %v", err)
					}
					if warm.Stats.DocumentsFetched != 0 || warm.Stats.FullDocumentScanFallbacks != 0 || warm.Stats.FailClosed != 0 {
						b.Fatalf("warm stats=%+v want zero-doc no-fallback search", warm.Stats)
					}
					b.ReportAllocs()
					b.ResetTimer()
					var sink TextSearchResponse
					for i := 0; i < b.N; i++ {
						got, err := textV2ContractRunSearch2623(col, tc)
						if err != nil {
							b.Fatalf("SearchText: %v", err)
						}
						if got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 {
							b.Fatalf("stats=%+v want zero-doc no-fallback search", got.Stats)
						}
						sink = got
					}
					b.StopTimer()
					b.ReportMetric(float64(docs), "docs_fixture")
					b.ReportMetric(float64(tc.candidateLimit), "candidate_budget/search")
					b.ReportMetric(float64(tc.maxPostings), "max_postings/search")
					textV2ContractReportTextStats2623(b, sink)
				})
			}
		})
	}
}

func BenchmarkTextV2ContractConcurrentServing2623(b *testing.B) {
	docs := textV2ContractEnvInt2623("TREEDB_TEXT_V2_CONCURRENT_DOCS", 256)
	readers := textV2ContractEnvInt2623("TREEDB_TEXT_V2_CONCURRENT_READERS", 4)
	if docs < 64 {
		docs = 64
	}
	if readers < 1 {
		readers = 1
	}
	d, col := openTextV2ContractSearchFixture2623(b, docs)
	defer func() { _ = d.Close() }()
	query := HybridTextQuery{IndexName: textV2ContractIndexName2623, Query: textV2ContractRareTerm2623, CandidateLimit: minTextV2ContractInt2623(docs, 64)}
	warm, err := col.SearchHybridTextCandidates(query)
	if err != nil {
		b.Fatalf("warm SearchHybridTextCandidates: %v", err)
	}
	if warm.Stats.DocumentsFetched != 0 || warm.Stats.FullDocumentScanFallbacks != 0 || warm.Stats.FailClosed != 0 {
		b.Fatalf("warm stats=%+v want zero-doc candidate serving", warm.Stats)
	}

	durations := make([]int64, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	next := 0
	var nextMu sync.Mutex
	for worker := 0; worker < readers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				nextMu.Lock()
				idx := next
				next++
				nextMu.Unlock()
				if idx >= b.N {
					return
				}
				start := time.Now()
				got, err := col.SearchHybridTextCandidates(query)
				durations[idx] = time.Since(start).Nanoseconds()
				if err != nil || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 {
					errMu.Lock()
					if firstErr == nil {
						if err != nil {
							firstErr = err
						} else {
							firstErr = fmt.Errorf("unexpected stats: %+v", got.Stats)
						}
					}
					errMu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(docs), "docs_fixture")
	b.ReportMetric(float64(readers), "readers")
	b.ReportMetric(1, "cache_warm")
	b.ReportMetric(0, "mixed_write_snapshot_churn")
	if firstErr != nil {
		b.Fatalf("concurrent search: %v", firstErr)
	}
	if len(durations) > 0 {
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		b.ReportMetric(float64(textV2ContractPercentile2623(durations, 50)), "p50_ns/search")
		b.ReportMetric(float64(textV2ContractPercentile2623(durations, 95)), "p95_ns/search")
		b.ReportMetric(float64(textV2ContractPercentile2623(durations, 99)), "p99_ns/search")
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	b.ReportMetric(float64(ms.HeapAlloc), "steady_heap_bytes")
}

func textV2ContractRunSearch2623(col *Collection, tc textV2ContractSearchCase2623) (TextSearchResponse, error) {
	return col.searchText(TextSearchOptions{
		IndexName:          textV2ContractIndexName2623,
		Query:              tc.query,
		Operator:           tc.operator,
		TopK:               10,
		CandidateLimit:     tc.candidateLimit,
		MaxPostingsScanned: tc.maxPostings,
	}, tc.resultMode)
}

func textV2ContractSearchCases2623(docs int) []textV2ContractSearchCase2623 {
	candidateLimit := docs
	maxPostings := docs * 4
	if candidateLimit < 1 {
		candidateLimit = 1
	}
	if maxPostings < 1 {
		maxPostings = 1
	}
	return []textV2ContractSearchCase2623{
		{name: "score_only_common_no_docs", query: "refund", resultMode: textSearchResultScoreOnly, candidateLimit: candidateLimit, maxPostings: maxPostings},
		{name: "detailed_common_no_docs", query: "refund", resultMode: textSearchResultFull, candidateLimit: candidateLimit, maxPostings: maxPostings},
		{name: "rare_no_docs", query: textV2ContractRareTerm2623, resultMode: textSearchResultScoreOnly, candidateLimit: candidateLimit, maxPostings: maxPostings},
		{name: "multi_term_and_no_docs", query: "refund AND policy", operator: TextSearchOperatorAND, resultMode: textSearchResultScoreOnly, candidateLimit: candidateLimit, maxPostings: maxPostings},
	}
}

func openTextV2ContractSearchFixture2623(tb testing.TB, docs int) (*backenddb.DB, *Collection) {
	tb.Helper()
	d := openTextV2ContractDB2623(tb)
	col := createTextV2ContractCollection2623(tb, d, true)
	ids, rawDocs := textV2ContractDocuments2623(docs, "search")
	if _, err := col.InsertBatch(ids, rawDocs); err != nil {
		_ = d.Close()
		tb.Fatalf("InsertBatch fixture: %v", err)
	}
	return d, col
}

func createTextV2ContractCollection2623(tb testing.TB, d *backenddb.DB, withText bool) *Collection {
	tb.Helper()
	meta := CollectionMeta{
		Name: "docs",
		Indexes: []IndexDefinition{
			{Name: "tenant", Field: "tenant", ValueType: IndexValueString},
		},
	}
	if withText {
		meta.TextIndexes = []TextIndexDefinition{textV2ContractIndexDefinition2623()}
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&meta); err != nil {
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		tb.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func textV2ContractIndexDefinition2623() TextIndexDefinition {
	return TextIndexDefinition{
		Name: textV2ContractIndexName2623,
		Fields: []TextIndexField{
			{Field: "title", Weight: 3},
			{Field: "body"},
		},
		StorePositions: true,
	}
}

func textV2ContractDocuments2623(count int, label string) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%s-%06d", label, i))
		tenant := "tenant-broad"
		if i%16 == 0 {
			tenant = "tenant-rare-06pct"
		}
		rare := ""
		if i%97 == 0 {
			rare = " " + textV2ContractRareTerm2623
		}
		docs[i] = []byte(fmt.Sprintf(`{"title":"Ticket %s %d refund policy%s","body":"refund policy support common term shard_%02d customer_%03d%s","tenant":"%s","region":"r%02d"}`,
			label, i, rare, i%32, i%257, rare, tenant, i%8))
	}
	return ids, docs
}

func textV2ContractStorageStats2623(tb testing.TB, col *Collection) TextIndexStorageStats {
	tb.Helper()
	stats, err := col.TextIndexStorageStats(textV2ContractIndexName2623)
	if err != nil {
		tb.Fatalf("TextIndexStorageStats: %v", err)
	}
	return stats
}

func textV2ContractReportBackfillStats2623(b *testing.B, docs int, stats TextIndexBackfillStats) {
	b.Helper()
	entries := uint64(stats.PostingEntries + stats.StateEntries + stats.StatsEntries)
	b.ReportMetric(float64(stats.DocumentsScanned), "docs_scanned/op")
	b.ReportMetric(float64(stats.PostingEntries), "posting_entries/op")
	b.ReportMetric(float64(stats.StateEntries), "state_entries/op")
	b.ReportMetric(float64(stats.StatsEntries), "stats_entries/op")
	b.ReportMetric(float64(entries)/float64(maxTextV2ContractInt2623(docs, 1)), "index_entries/doc")
	b.ReportMetric(float64(entries)/float64(maxTextV2ContractInt2623(docs, 1)), "write_amp_entries/doc")
	b.ReportMetric(float64(stats.EncodedBytes)/float64(maxTextV2ContractInt2623(docs, 1)), "index_bytes/doc")
}

func textV2ContractReportWriteStats2623(b *testing.B, docs int, stats TextIndexStorageStats, writeAmpEntries uint64) {
	b.Helper()
	entries := textV2ContractStorageEntryCount2623(stats)
	b.ReportMetric(float64(stats.PostingEntries), "posting_entries/op")
	b.ReportMetric(float64(stats.StateEntries), "state_entries/op")
	b.ReportMetric(float64(stats.StatsEntries), "stats_entries/op")
	b.ReportMetric(float64(entries)/float64(maxTextV2ContractInt2623(docs, 1)), "index_entries/doc")
	b.ReportMetric(float64(writeAmpEntries)/float64(maxTextV2ContractInt2623(docs, 1)), "write_amp_entries/doc")
	b.ReportMetric(float64(stats.EncodedBytes)/float64(maxTextV2ContractInt2623(docs, 1)), "index_bytes/doc")
}

func textV2ContractStorageEntryCount2623(stats TextIndexStorageStats) uint64 {
	return stats.PostingEntries + stats.StateEntries + stats.StatsEntries
}

func textV2ContractReportTextStats2623(b *testing.B, response TextSearchResponse) {
	b.Helper()
	stats := response.Stats
	b.ReportMetric(float64(len(response.Results)), "results/search")
	b.ReportMetric(float64(stats.QueryTerms), "query_terms/search")
	b.ReportMetric(float64(stats.TextCandidatesRequested), "text_requested/search")
	b.ReportMetric(float64(stats.TextCandidatesReturned), "text_candidates/search")
	b.ReportMetric(float64(stats.TextPostingsScanned), "postings_scanned/search")
	b.ReportMetric(float64(stats.TextPostingBlocksVisited), "posting_blocks_visited/search")
	b.ReportMetric(float64(stats.TextPostingBlocksSkipped), "posting_blocks_skipped/search")
	b.ReportMetric(float64(stats.TextCandidatesScored), "candidates_scored/search")
	b.ReportMetric(float64(stats.TextStateLookups), "state_lookups/search")
	b.ReportMetric(float64(stats.TextNormLookups), "norm_lookups/search")
	b.ReportMetric(float64(stats.TextMatchDetailsBuilt), "match_details/search")
	b.ReportMetric(float64(stats.DocumentsFetched), "docs_fetched/search")
	b.ReportMetric(float64(stats.FullDocumentScanFallbacks), "full_doc_fallbacks/search")
	if stats.Truncated {
		b.ReportMetric(1, "truncated/search")
	} else {
		b.ReportMetric(0, "truncated/search")
	}
	b.ReportMetric(float64(stats.FailClosed), "fail_closed/search")
}

func textV2ContractCloneIDsWithIteration2623(ids [][]byte, iteration int) [][]byte {
	out := make([][]byte, len(ids))
	for i := range ids {
		out[i] = []byte(fmt.Sprintf("%s-%06d", ids[i], iteration))
	}
	return out
}

func openTextV2ContractDB2623(tb testing.TB) *backenddb.DB {
	tb.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	return d
}

func textV2ContractDefaultCorpusSizes2623() []int {
	return []int{256, 10_000, 100_000}
}

func textV2ContractSearchCorpusSizes2623() []int {
	if raw := os.Getenv("TREEDB_TEXT_V2_SEARCH_DOCS"); raw != "" {
		var sizes []int
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			value, err := strconv.Atoi(part)
			if err == nil && value > 0 {
				sizes = append(sizes, value)
			}
		}
		if len(sizes) > 0 {
			return sizes
		}
	}
	return textV2ContractDefaultCorpusSizes2623()
}

func textV2ContractLargeCorpusEnabled2623() bool {
	if os.Getenv("TREEDB_TEXT_V2_SEARCH_DOCS") != "" {
		return true
	}
	raw := strings.TrimSpace(strings.ToLower(os.Getenv("TREEDB_TEXT_V2_RUN_100K")))
	return raw == "1" || raw == "true" || raw == "yes"
}

func textV2ContractEnvInt2623(name string, fallback int) int {
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

func textV2ContractPercentile2623(sorted []int64, pct int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if pct <= 0 {
		return sorted[0]
	}
	if pct >= 100 {
		return sorted[len(sorted)-1]
	}
	idx := (len(sorted)*pct + 99) / 100
	if idx < 1 {
		idx = 1
	}
	if idx > len(sorted) {
		idx = len(sorted)
	}
	return sorted[idx-1]
}

func textV2ContractContainsInt2623(values []int, want int) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func textV2ContractContainsString2623(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func textV2ContractContainsSearchCase2623(values []textV2ContractSearchCase2623, want string) bool {
	for _, value := range values {
		if value.name == want {
			return true
		}
	}
	return false
}

func textV2ContractSearchCaseNames2623(values []textV2ContractSearchCase2623) []string {
	out := make([]string, len(values))
	for i := range values {
		out[i] = values[i].name
	}
	return out
}

func minTextV2ContractInt2623(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxTextV2ContractInt2623(a, b int) int {
	if a > b {
		return a
	}
	return b
}
