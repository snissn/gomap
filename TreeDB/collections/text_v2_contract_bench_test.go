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
	for _, want := range []string{"score_only_common_no_docs", "detailed_common_no_docs", "rare_no_docs", "multi_term_or_no_docs", "multi_term_and_no_docs", "common_rare_or_no_docs", "high_frequency_disjunction_no_docs"} {
		if !textV2ContractContainsSearchCase2623(cases, want) {
			t.Fatalf("search cases=%v missing %q", textV2ContractSearchCaseNames2623(cases), want)
		}
	}
	gotCounters := append([]string(nil), TextIndexV2RequiredCounterNames()...)
	wantCounters := []string{
		"postings_scanned",
		"posting_blocks_visited",
		"posting_blocks_skipped",
		"blockmax_fallbacks",
		"threshold_updates",
		"wand_pivots",
		"scalar_prefilter_ids",
		"scalar_posting_blocks_skipped",
		"scalar_postings_rejected",
		"candidates_scored",
		"state_lookups",
		"norm_lookups",
		"docs_fetched",
		"match_details_built",
		"position_lookups",
		"phrase_candidates_checked",
		"phrase_candidates_matched",
		"scalar_filter_selectivity",
		"fail_closed",
		"write_amplification",
		"index_bytes_per_doc",
		"rewrite_merge_state",
	}
	sort.Strings(gotCounters)
	sort.Strings(wantCounters)
	if len(gotCounters) != len(wantCounters) {
		t.Fatalf("required counters=%v want %v", gotCounters, wantCounters)
	}
	for i := range wantCounters {
		if gotCounters[i] != wantCounters[i] {
			t.Fatalf("required counters=%v want %v", gotCounters, wantCounters)
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
	insertTermEntries := textV2ContractUniqueTermStatsEntries2626(b, docs)

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

	b.Run("insert_batch_text_v2_indexed", func(b *testing.B) {
		var lastStats TextIndexStorageStats
		var writeAmpEntries uint64
		var highDFBlocks uint64
		b.ReportAllocs()
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		b.ResetTimer()
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			d := openTextV2ContractDB2623(b)
			col := createTextV2ContractCollection2623(b, d, false)
			if _, _, err := col.CreateTextIndex(textV2ContractV2IndexDefinition2626()); err != nil {
				b.Fatalf("CreateTextIndex v2 setup: %v", err)
			}
			beforeStats := textV2ContractStorageStats2623(b, col)
			batchIDs := textV2ContractCloneIDsWithIteration2623(ids, i)
			b.StartTimer()
			_, err := col.InsertBatch(batchIDs, docs)
			b.StopTimer()
			if err != nil {
				b.Fatalf("InsertBatch text-v2-indexed: %v", err)
			}
			lastStats = textV2ContractStorageStats2623(b, col)
			highDFBlocks = textV2ContractTermPostingBlockCount2626(b, col, "refund")
			writeAmpEntries = textV2ContractV2MutationRootDeltaEntries2626(docsPerBatch, beforeStats, lastStats, insertTermEntries)
			_ = d.Close()
		}
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		textV2ContractReportWriteStats2623(b, docsPerBatch, lastStats, writeAmpEntries)
		textV2ContractReportHighDFBlocks2626(b, docsPerBatch, highDFBlocks)
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

	b.Run("create_text_v2_index_backfill", func(b *testing.B) {
		var lastStats TextIndexBackfillStats
		var highDFBlocks uint64
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
			_, stats, err := col.CreateTextIndex(textV2ContractV2IndexDefinition2626())
			b.StopTimer()
			if err != nil {
				b.Fatalf("CreateTextIndex v2 backfill: %v", err)
			}
			lastStats = stats
			highDFBlocks = textV2ContractTermPostingBlockCount2626(b, col, "refund")
			_ = d.Close()
		}
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		textV2ContractReportBackfillStats2623(b, docsPerBatch, lastStats)
		textV2ContractReportHighDFBlocks2626(b, docsPerBatch, highDFBlocks)
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

	b.Run("update_batch_text_v2_indexed", func(b *testing.B) {
		_, updatedDocs := textV2ContractDocuments2623(docsPerBatch, "update")
		updateTermEntries := textV2ContractUniqueTermStatsEntries2626(b, docs, updatedDocs)
		var lastStats TextIndexStorageStats
		var writeAmpEntries uint64
		var highDFBlocks uint64
		b.ReportAllocs()
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		b.ResetTimer()
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			d := openTextV2ContractDB2623(b)
			col := createTextV2ContractCollection2623(b, d, false)
			if _, _, err := col.CreateTextIndex(textV2ContractV2IndexDefinition2626()); err != nil {
				b.Fatalf("CreateTextIndex v2 setup: %v", err)
			}
			batchIDs := textV2ContractCloneIDsWithIteration2623(ids, i)
			if _, err := col.InsertBatch(batchIDs, docs); err != nil {
				b.Fatalf("InsertBatch setup: %v", err)
			}
			beforeStats := textV2ContractStorageStats2623(b, col)
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
				b.Fatalf("UpdateBatch text-v2-indexed: %v", err)
			}
			if len(results) != docsPerBatch {
				b.Fatalf("UpdateBatch results=%d want %d", len(results), docsPerBatch)
			}
			lastStats = textV2ContractStorageStats2623(b, col)
			highDFBlocks = textV2ContractTermPostingBlockCount2626(b, col, "refund")
			writeAmpEntries = textV2ContractV2MutationRootDeltaEntries2626(docsPerBatch, beforeStats, lastStats, updateTermEntries)
			_ = d.Close()
		}
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		textV2ContractReportWriteStats2623(b, docsPerBatch, lastStats, writeAmpEntries)
		textV2ContractReportHighDFBlocks2626(b, docsPerBatch, highDFBlocks)
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

	b.Run("delete_batch_text_v2_indexed", func(b *testing.B) {
		var lastStats TextIndexStorageStats
		var writeAmpEntries uint64
		var highDFBlocks uint64
		b.ReportAllocs()
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		b.ResetTimer()
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			d := openTextV2ContractDB2623(b)
			col := createTextV2ContractCollection2623(b, d, false)
			if _, _, err := col.CreateTextIndex(textV2ContractV2IndexDefinition2626()); err != nil {
				b.Fatalf("CreateTextIndex v2 setup: %v", err)
			}
			batchIDs := textV2ContractCloneIDsWithIteration2623(ids, i)
			if _, err := col.InsertBatch(batchIDs, docs); err != nil {
				b.Fatalf("InsertBatch setup: %v", err)
			}
			beforeStats := textV2ContractStorageStats2623(b, col)
			b.StartTimer()
			deleted, err := col.DeleteBatch(batchIDs)
			b.StopTimer()
			if err != nil {
				b.Fatalf("DeleteBatch text-v2-indexed: %v", err)
			}
			if deleted != docsPerBatch {
				b.Fatalf("DeleteBatch deleted=%d want %d", deleted, docsPerBatch)
			}
			lastStats = textV2ContractStorageStats2623(b, col)
			highDFBlocks = textV2ContractTermPostingBlockCount2626(b, col, "refund")
			writeAmpEntries = textV2ContractV2MutationRootDeltaEntries2626(docsPerBatch, beforeStats, lastStats, insertTermEntries)
			_ = d.Close()
		}
		b.ReportMetric(float64(docsPerBatch), "docs/op")
		textV2ContractReportWriteStats2623(b, docsPerBatch, lastStats, writeAmpEntries)
		textV2ContractReportHighDFBlocks2626(b, docsPerBatch, highDFBlocks)
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

func BenchmarkTextV2ScoreSearchScale2627(b *testing.B) {
	for _, docs := range textV2ContractSearchCorpusSizes2623() {
		docs := docs
		b.Run(fmt.Sprintf("docs_%d", docs), func(b *testing.B) {
			if docs >= 100_000 && !textV2ContractLargeCorpusEnabled2623() {
				b.Skip("set TREEDB_TEXT_V2_RUN_100K=1 or TREEDB_TEXT_V2_SEARCH_DOCS to run the >=100k local artifact row")
			}
			d, col := openTextV2ContractV2SearchFixture2627(b, docs)
			defer func() { _ = d.Close() }()
			for _, tc := range textV2ContractSearchCases2623(docs) {
				tc := tc
				b.Run(tc.name, func(b *testing.B) {
					warm, err := textV2ContractRunSearch2623(col, tc)
					if err != nil {
						b.Fatalf("warm v2 search: %v", err)
					}
					textV2ContractAssertV2SearchModeCounters2629(b, "warm", tc, warm)
					textV2ContractAssertSearchHit2627(b, "warm", warm)
					b.ReportAllocs()
					b.ResetTimer()
					var sink TextSearchResponse
					for i := 0; i < b.N; i++ {
						got, err := textV2ContractRunSearch2623(col, tc)
						if err != nil {
							b.Fatalf("v2 SearchText: %v", err)
						}
						textV2ContractAssertV2SearchModeCounters2629(b, "timed", tc, got)
						textV2ContractAssertSearchHit2627(b, "timed", got)
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

func textV2ContractAssertV2SearchModeCounters2629(tb testing.TB, label string, tc textV2ContractSearchCase2623, got TextSearchResponse) {
	tb.Helper()
	if got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 || got.Stats.TextStateLookups != 0 {
		tb.Fatalf("%s v2 stats=%+v want zero-doc/no-state/no-fail search", label, got.Stats)
	}
	if tc.resultMode == textSearchResultScoreOnly {
		if got.Stats.TextMatchDetailsBuilt != 0 {
			tb.Fatalf("%s v2 stats=%+v want score-only no-match search", label, got.Stats)
		}
		return
	}
	if got.Stats.TextMatchDetailsBuilt != uint64(len(got.Results)) {
		tb.Fatalf("%s v2 stats=%+v results=%d want lazy match details bounded to returned results", label, got.Stats, len(got.Results))
	}
}

func BenchmarkTextV2BlockMaxCommonTerm2628(b *testing.B) {
	docs := textV2ContractEnvInt2623("TREEDB_TEXT_V2_BLOCKMAX_DOCS", 10_000)
	if docs < int(textV2PostingBlockTargetPostings)*3 {
		docs = int(textV2PostingBlockTargetPostings) * 3
	}
	d, col := openTextV2BlockMaxBenchFixture2628(b, docs)
	defer func() { _ = d.Close() }()
	opts := TextSearchOptions{IndexName: textV2ContractIndexName2623, Query: "refund", TopK: 10, CandidateLimit: docs, MaxPostingsScanned: docs * 2}
	modes := []struct {
		name       string
		disableBMW bool
	}{
		{name: "blockmax_common_topk"},
		{name: "exhaustive_common_topk", disableBMW: true},
	}
	for _, mode := range modes {
		mode := mode
		b.Run(mode.name, func(b *testing.B) {
			runOpts := opts
			runOpts.textV2DisableBlockMax = mode.disableBMW
			warm, err := col.searchText(runOpts, textSearchResultScoreOnly)
			if err != nil {
				b.Fatalf("warm search: %v", err)
			}
			if len(warm.Results) == 0 || warm.Stats.DocumentsFetched != 0 || warm.Stats.FullDocumentScanFallbacks != 0 || warm.Stats.FailClosed != 0 || warm.Stats.TextStateLookups != 0 || warm.Stats.TextMatchDetailsBuilt != 0 {
				b.Fatalf("warm stats=%+v results=%d want score-only zero-doc blockmax search", warm.Stats, len(warm.Results))
			}
			b.ReportAllocs()
			b.ResetTimer()
			var sink TextSearchResponse
			for i := 0; i < b.N; i++ {
				got, err := col.searchText(runOpts, textSearchResultScoreOnly)
				if err != nil {
					b.Fatalf("SearchText: %v", err)
				}
				if len(got.Results) == 0 || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 || got.Stats.TextStateLookups != 0 || got.Stats.TextMatchDetailsBuilt != 0 {
					b.Fatalf("stats=%+v results=%d want score-only zero-doc blockmax search", got.Stats, len(got.Results))
				}
				sink = got
			}
			b.StopTimer()
			b.ReportMetric(float64(docs), "docs_fixture")
			b.ReportMetric(float64(opts.TopK), "topk/search")
			textV2ContractReportTextStats2623(b, sink)
		})
	}
}

func BenchmarkTextV2BlockMaxMultiTerm2730(b *testing.B) {
	docs := textV2ContractEnvInt2623("TREEDB_TEXT_V2_OR_WAND_DOCS", 10_000)
	if docs < int(textV2PostingBlockTargetPostings)*3 {
		docs = int(textV2PostingBlockTargetPostings) * 3
	}
	if docs >= 100_000 && !textV2ContractLargeCorpusEnabled2623() && os.Getenv("TREEDB_TEXT_V2_OR_WAND_DOCS") == "" {
		b.Skip("set TREEDB_TEXT_V2_RUN_100K=1 or TREEDB_TEXT_V2_OR_WAND_DOCS to run the >=100k local artifact row")
	}
	d, col := openTextV2ORWANDBenchFixture2730(b, docs)
	defer func() { _ = d.Close() }()
	maxPostings := docs * 8
	cases := []struct {
		name       string
		query      string
		operator   TextSearchOperator
		disableBMW bool
	}{
		{name: "or_common_blockmax", query: "refund OR policy", operator: TextSearchOperatorOR},
		{name: "or_common_exhaustive", query: "refund OR policy", operator: TextSearchOperatorOR, disableBMW: true},
		{name: "and_common_blockmax", query: "refund AND policy", operator: TextSearchOperatorAND},
		{name: "and_common_exhaustive", query: "refund AND policy", operator: TextSearchOperatorAND, disableBMW: true},
		{name: "or_common_rare_blockmax", query: "refund OR " + textV2ContractRareTerm2623, operator: TextSearchOperatorOR},
		{name: "or_common_rare_exhaustive", query: "refund OR " + textV2ContractRareTerm2623, operator: TextSearchOperatorOR, disableBMW: true},
		{name: "or_high_frequency_blockmax", query: "refund OR policy OR support OR common", operator: TextSearchOperatorOR},
		{name: "or_high_frequency_exhaustive", query: "refund OR policy OR support OR common", operator: TextSearchOperatorOR, disableBMW: true},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			opts := TextSearchOptions{IndexName: textV2ContractIndexName2623, Query: tc.query, Operator: tc.operator, TopK: 10, CandidateLimit: docs, MaxPostingsScanned: maxPostings, textV2DisableBlockMax: tc.disableBMW}
			warm, err := col.searchText(opts, textSearchResultScoreOnly)
			if err != nil {
				b.Fatalf("warm search: %v", err)
			}
			if len(warm.Results) == 0 || warm.Stats.DocumentsFetched != 0 || warm.Stats.FullDocumentScanFallbacks != 0 || warm.Stats.FailClosed != 0 || warm.Stats.TextStateLookups != 0 || warm.Stats.TextMatchDetailsBuilt != 0 {
				b.Fatalf("warm response=%+v want score-only zero-doc multi-term search", warm)
			}
			b.ReportAllocs()
			b.ResetTimer()
			var sink TextSearchResponse
			for i := 0; i < b.N; i++ {
				got, err := col.searchText(opts, textSearchResultScoreOnly)
				if err != nil {
					b.Fatalf("SearchText: %v", err)
				}
				if len(got.Results) == 0 || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 || got.Stats.TextStateLookups != 0 || got.Stats.TextMatchDetailsBuilt != 0 {
					b.Fatalf("response=%+v want score-only zero-doc multi-term search", got)
				}
				sink = got
			}
			b.StopTimer()
			b.ReportMetric(float64(docs), "docs_fixture")
			b.ReportMetric(float64(opts.TopK), "topk/search")
			b.ReportMetric(float64(maxPostings), "max_postings/search")
			textV2ContractReportTextStats2623(b, sink)
		})
	}
}

func BenchmarkTextV2PhraseProximity2733(b *testing.B) {
	docs := textV2ContractEnvInt2623("TREEDB_TEXT_V2_PHRASE_DOCS", 10_000)
	if docs < int(textV2PostingBlockTargetPostings)*3 {
		docs = int(textV2PostingBlockTargetPostings) * 3
	}
	if docs >= 100_000 && !textV2ContractLargeCorpusEnabled2623() && os.Getenv("TREEDB_TEXT_V2_PHRASE_DOCS") == "" {
		b.Skip("set TREEDB_TEXT_V2_RUN_100K=1 or TREEDB_TEXT_V2_PHRASE_DOCS to run the >=100k phrase/proximity artifact row")
	}
	d, col, stats := openTextV2PhraseBenchFixture2733(b, docs, nil)
	defer func() { _ = d.Close() }()
	maxPostings := docs * 8
	cases := []struct {
		name   string
		phrase TextSearchPhraseQuery
	}{
		{name: "exact_refund_policy", phrase: TextSearchPhraseQuery{Query: "refund policy"}},
		{name: "proximity_refund_support_slop1", phrase: TextSearchPhraseQuery{Query: "refund support", Slop: 1}},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			opts := TextSearchOptions{IndexName: textV2ContractIndexName2623, Phrase: &tc.phrase, TopK: 10, CandidateLimit: docs, MaxPostingsScanned: maxPostings, ResultMode: TextSearchResultModeScoreOnly}
			warm, err := col.SearchText(opts)
			if err != nil {
				b.Fatalf("warm phrase search: %v", err)
			}
			if len(warm.Results) == 0 || warm.Stats.DocumentsFetched != 0 || warm.Stats.FullDocumentScanFallbacks != 0 || warm.Stats.FailClosed != 0 || warm.Stats.TextStateLookups != 0 || warm.Stats.TextMatchDetailsBuilt != 0 || warm.Stats.TextPositionLookups == 0 {
				b.Fatalf("warm response=%+v want score-only zero-doc phrase search", warm)
			}
			b.ReportAllocs()
			b.ResetTimer()
			var sink TextSearchResponse
			for i := 0; i < b.N; i++ {
				got, err := col.SearchText(opts)
				if err != nil {
					b.Fatalf("phrase search: %v", err)
				}
				if len(got.Results) == 0 || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 || got.Stats.TextStateLookups != 0 || got.Stats.TextMatchDetailsBuilt != 0 || got.Stats.TextPositionLookups == 0 {
					b.Fatalf("response=%+v want score-only zero-doc phrase search", got)
				}
				sink = got
			}
			b.StopTimer()
			b.ReportMetric(float64(docs), "docs_fixture")
			b.ReportMetric(float64(opts.TopK), "topk/search")
			b.ReportMetric(float64(maxPostings), "max_postings/search")
			b.ReportMetric(float64(stats.V2PositionEntries), "v2_position_entries")
			b.ReportMetric(float64(stats.EncodedBytes)/float64(maxTextV2ContractInt2623(docs, 1)), "index_bytes/doc")
			textV2ContractReportBackfillLaneBytes2623(b, docs, stats)
			textV2ContractReportTextStats2623(b, sink)
		})
	}
}

func BenchmarkTextV2AnalyzerStopwordsWrite2733(b *testing.B) {
	docsPerBatch := textV2ContractEnvInt2623("TREEDB_TEXT_V2_ANALYZER_DOCS", 256)
	if docsPerBatch < 1 {
		docsPerBatch = 1
	}
	ids, docs := textV2ContractDocuments2623(docsPerBatch, "analyzer")
	cases := []struct {
		name    string
		options *TextAnalyzerOptions
	}{
		{name: "simple_no_stopwords"},
		{name: "simple_stopwords", options: &TextAnalyzerOptions{StopWords: []string{"the", "a", "term"}}},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			var lastStats TextIndexStorageStats
			b.ReportAllocs()
			b.ReportMetric(float64(docsPerBatch), "docs/op")
			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				d := openTextV2ContractDB2623(b)
				col := createTextV2ContractCollection2623(b, d, false)
				def := textV2ContractV2IndexDefinition2626()
				def.AnalyzerOptions = tc.options
				if _, _, err := col.CreateTextIndex(def); err != nil {
					b.Fatalf("CreateTextIndex v2 setup: %v", err)
				}
				batchIDs := textV2ContractCloneIDsWithIteration2623(ids, i)
				b.StartTimer()
				_, err := col.InsertBatch(batchIDs, docs)
				b.StopTimer()
				if err != nil {
					b.Fatalf("InsertBatch analyzer: %v", err)
				}
				lastStats = textV2ContractStorageStats2623(b, col)
				_ = d.Close()
			}
			b.ReportMetric(float64(docsPerBatch), "docs/op")
			textV2ContractReportWriteStats2623(b, docsPerBatch, lastStats, textV2ContractStorageEntryCount2623(lastStats))
		})
	}
}

func BenchmarkTextV2LazyDetails2629(b *testing.B) {
	docs := textV2ContractEnvInt2623("TREEDB_TEXT_V2_LAZY_DETAILS_DOCS", 10_000)
	if docs < int(textV2PostingBlockTargetPostings)*3 {
		docs = int(textV2PostingBlockTargetPostings) * 3
	}
	d, col := openTextV2BlockMaxBenchFixture2628(b, docs)
	defer func() { _ = d.Close() }()
	opts := TextSearchOptions{IndexName: textV2ContractIndexName2623, Query: "refund", TopK: 10, CandidateLimit: docs, MaxPostingsScanned: docs * 2}
	cases := []struct {
		name string
		mode textSearchResultMode
	}{
		{name: "score_only", mode: textSearchResultScoreOnly},
		{name: "detailed_topk", mode: textSearchResultFull},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			warm, err := col.searchText(opts, tc.mode)
			if err != nil {
				b.Fatalf("warm search: %v", err)
			}
			if len(warm.Results) == 0 || warm.Stats.DocumentsFetched != 0 || warm.Stats.FullDocumentScanFallbacks != 0 || warm.Stats.FailClosed != 0 || warm.Stats.TextStateLookups != 0 {
				b.Fatalf("warm response=%+v want no-doc v2 search", warm)
			}
			if tc.mode == textSearchResultScoreOnly && warm.Stats.TextMatchDetailsBuilt != 0 {
				b.Fatalf("warm stats=%+v want score-only details=0", warm.Stats)
			}
			if tc.mode != textSearchResultScoreOnly && warm.Stats.TextMatchDetailsBuilt != uint64(len(warm.Results)) {
				b.Fatalf("warm stats=%+v results=%d want details bounded to returned topK", warm.Stats, len(warm.Results))
			}
			b.ReportAllocs()
			b.ResetTimer()
			var sink TextSearchResponse
			for i := 0; i < b.N; i++ {
				got, err := col.searchText(opts, tc.mode)
				if err != nil {
					b.Fatalf("search: %v", err)
				}
				if tc.mode == textSearchResultScoreOnly && got.Stats.TextMatchDetailsBuilt != 0 {
					b.Fatalf("stats=%+v want score-only details=0", got.Stats)
				}
				if tc.mode != textSearchResultScoreOnly && got.Stats.TextMatchDetailsBuilt != uint64(len(got.Results)) {
					b.Fatalf("stats=%+v results=%d want topK-bounded details", got.Stats, len(got.Results))
				}
				sink = got
			}
			b.StopTimer()
			b.ReportMetric(float64(docs), "docs_fixture")
			b.ReportMetric(float64(opts.TopK), "topk/search")
			textV2ContractReportTextStats2623(b, sink)
		})
	}
}

func BenchmarkTextV2BlockMaxConcurrentServing2628(b *testing.B) {
	docs := textV2ContractEnvInt2623("TREEDB_TEXT_V2_BLOCKMAX_CONCURRENT_DOCS", 10_000)
	readers := textV2ContractEnvInt2623("TREEDB_TEXT_V2_BLOCKMAX_CONCURRENT_READERS", 4)
	if docs < int(textV2PostingBlockTargetPostings)*3 {
		docs = int(textV2PostingBlockTargetPostings) * 3
	}
	if readers < 1 {
		readers = 1
	}
	d, col := openTextV2BlockMaxBenchFixture2628(b, docs)
	defer func() { _ = d.Close() }()
	opts := TextSearchOptions{IndexName: textV2ContractIndexName2623, Query: "refund", TopK: 10, CandidateLimit: docs, MaxPostingsScanned: docs * 2}
	warm, err := col.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		b.Fatalf("warm blockmax search: %v", err)
	}
	if warm.Stats.TextPostingBlocksSkipped == 0 || warm.Stats.DocumentsFetched != 0 || warm.Stats.FailClosed != 0 {
		b.Fatalf("warm stats=%+v want skipped score-only search", warm.Stats)
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
				got, err := col.searchText(opts, textSearchResultScoreOnly)
				durations[idx] = time.Since(start).Nanoseconds()
				if err != nil || got.Stats.TextPostingBlocksSkipped == 0 || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 {
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
	if firstErr != nil {
		b.Fatalf("concurrent blockmax search: %v", firstErr)
	}
	b.ReportMetric(float64(docs), "docs_fixture")
	b.ReportMetric(float64(readers), "readers")
	b.ReportMetric(1, "cache_warm")
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

func openTextV2PhraseBenchFixture2733(tb testing.TB, docs int, options *TextAnalyzerOptions) (*backenddb.DB, *Collection, TextIndexBackfillStats) {
	tb.Helper()
	d := openTextV2ContractDB2623(tb)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	ids, rawDocs := textV2ORWANDBenchDocuments2730(docs, minTextV2ContractInt2623(16, docs))
	if _, err := col.InsertBatch(ids, rawDocs); err != nil {
		_ = d.Close()
		tb.Fatalf("InsertBatch fixture: %v", err)
	}
	def := textV2ContractV2IndexDefinition2626()
	def.StorePositions = true
	def.AnalyzerOptions = options
	if len(def.Fields) > 0 {
		def.Fields[0].Weight = 5
	}
	_, stats, err := col.CreateTextIndex(def)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("CreateTextIndex v2 fixture: %v", err)
	}
	return d, col, stats
}

func openTextV2ORWANDBenchFixture2730(tb testing.TB, docs int) (*backenddb.DB, *Collection) {
	tb.Helper()
	d := openTextV2ContractDB2623(tb)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	ids, rawDocs := textV2ORWANDBenchDocuments2730(docs, minTextV2ContractInt2623(16, docs))
	if _, err := col.InsertBatch(ids, rawDocs); err != nil {
		_ = d.Close()
		tb.Fatalf("InsertBatch fixture: %v", err)
	}
	def := textV2ContractV2IndexDefinition2626()
	if len(def.Fields) > 0 {
		def.Fields[0].Weight = 5
	}
	if _, _, err := col.CreateTextIndex(def); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateTextIndex v2 fixture: %v", err)
	}
	return d, col
}

func textV2ORWANDBenchDocuments2730(count, highDocs int) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-or-wand-%06d", i))
		title := "refund policy"
		body := "refund policy support common"
		if i < highDocs {
			title = strings.TrimSpace(strings.Repeat("refund policy ", 64))
			body = strings.TrimSpace(strings.Repeat("refund policy ", 128))
		}
		rare := ""
		if i%97 == 0 {
			rare = " " + textV2ContractRareTerm2623
		}
		docs[i] = []byte(fmt.Sprintf(`{"title":%q,"body":%q,"tenant":"tenant-broad"}`, title+rare, body+rare))
	}
	return ids, docs
}

func openTextV2BlockMaxBenchFixture2628(tb testing.TB, docs int) (*backenddb.DB, *Collection) {
	tb.Helper()
	d := openTextV2ContractDB2623(tb)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	ids, rawDocs := textV2BlockMaxFixtureDocs2628(docs, maxTextV2ContractInt2623(16, docs/8))
	if _, err := col.InsertBatch(ids, rawDocs); err != nil {
		_ = d.Close()
		tb.Fatalf("InsertBatch fixture: %v", err)
	}
	if _, _, err := col.CreateTextIndex(textV2ContractV2IndexDefinition2626()); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateTextIndex v2 fixture: %v", err)
	}
	return d, col
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

func textV2ContractAssertSearchHit2627(b *testing.B, phase string, response TextSearchResponse) {
	b.Helper()
	if len(response.Results) == 0 && response.Stats.TextCandidatesScored == 0 {
		b.Fatalf("%s v2 search returned no hits: %+v", phase, response.Stats)
	}
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
		{name: "multi_term_or_no_docs", query: "refund OR policy", operator: TextSearchOperatorOR, resultMode: textSearchResultScoreOnly, candidateLimit: candidateLimit, maxPostings: maxPostings},
		{name: "multi_term_and_no_docs", query: "refund AND policy", operator: TextSearchOperatorAND, resultMode: textSearchResultScoreOnly, candidateLimit: candidateLimit, maxPostings: maxPostings},
		{name: "common_rare_or_no_docs", query: "refund OR " + textV2ContractRareTerm2623, operator: TextSearchOperatorOR, resultMode: textSearchResultScoreOnly, candidateLimit: candidateLimit, maxPostings: maxPostings},
		{name: "high_frequency_disjunction_no_docs", query: "refund OR policy OR support OR common", operator: TextSearchOperatorOR, resultMode: textSearchResultScoreOnly, candidateLimit: candidateLimit, maxPostings: maxPostings},
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

func openTextV2ContractV2SearchFixture2627(tb testing.TB, docs int) (*backenddb.DB, *Collection) {
	tb.Helper()
	d := openTextV2ContractDB2623(tb)
	col := createTextV2ContractCollection2623(tb, d, false)
	ids, rawDocs := textV2ContractDocuments2623(docs, "search")
	if _, err := col.InsertBatch(ids, rawDocs); err != nil {
		_ = d.Close()
		tb.Fatalf("InsertBatch v2 fixture: %v", err)
	}
	if _, _, err := col.CreateTextIndex(textV2ContractV2IndexDefinition2626()); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateTextIndex v2 fixture: %v", err)
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
		Name:    textV2ContractIndexName2623,
		Version: TextIndexVersionV1,
		Fields: []TextIndexField{
			{Field: "title", Weight: 3},
			{Field: "body"},
		},
		StorePositions: true,
	}
}

func textV2ContractV2IndexDefinition2626() TextIndexDefinition {
	def := textV2ContractIndexDefinition2623()
	def.Version = TextIndexVersionV2
	// The canonical v2 hot-path benchmark excludes optional position payload work;
	// #2629 covers lazy positions with focused detail/positions tests and rows.
	def.StorePositions = false
	def.StoreOffsets = false
	return def
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
	v2Entries := uint64(stats.V2DocIDEntries + stats.V2DocMapBlocks + stats.V2PostingBlocks + stats.V2NormBlocks + stats.V2PositionEntries + stats.V2TermStats + stats.V2FormatRecords + stats.V2StatusRecords)
	if v2Entries > 0 {
		entries = v2Entries
	}
	docsDivisor := float64(maxTextV2ContractInt2623(docs, 1))
	b.ReportMetric(float64(stats.DocumentsScanned), "docs_scanned/op")
	b.ReportMetric(float64(stats.PostingEntries), "posting_entries/op")
	b.ReportMetric(float64(stats.StateEntries), "state_entries/op")
	b.ReportMetric(float64(stats.StatsEntries), "stats_entries/op")
	b.ReportMetric(float64(stats.V2DocIDEntries), "v2_docid_entries/op")
	b.ReportMetric(float64(stats.V2DocMapBlocks), "v2_docmap_blocks/op")
	b.ReportMetric(float64(stats.V2PostingBlocks), "v2_posting_blocks/op")
	b.ReportMetric(float64(stats.V2NormBlocks), "v2_norm_blocks/op")
	b.ReportMetric(float64(stats.V2PositionEntries), "v2_position_entries/op")
	b.ReportMetric(float64(stats.V2TermStats), "v2_term_stats/op")
	b.ReportMetric(float64(stats.V2PostingBlocks)/docsDivisor, "posting_blocks/doc")
	b.ReportMetric(0, "rewritten_blocks/doc")
	b.ReportMetric(float64(entries)/docsDivisor, "index_entries/doc")
	b.ReportMetric(float64(entries)/docsDivisor, "write_amp_entries/doc")
	b.ReportMetric(float64(stats.EncodedBytes)/docsDivisor, "index_bytes/doc")
	textV2ContractReportBackfillLaneBytes2623(b, docs, stats)
}

func textV2ContractReportWriteStats2623(b *testing.B, docs int, stats TextIndexStorageStats, writeAmpEntries uint64) {
	b.Helper()
	entries := textV2ContractStorageEntryCount2623(stats)
	docsDivisor := float64(maxTextV2ContractInt2623(docs, 1))
	b.ReportMetric(float64(stats.PostingEntries), "posting_entries/op")
	b.ReportMetric(float64(stats.StateEntries), "state_entries/op")
	b.ReportMetric(float64(stats.StatsEntries), "stats_entries/op")
	b.ReportMetric(float64(stats.V2DocIDEntries), "v2_docid_entries/op")
	b.ReportMetric(float64(stats.V2DocMapBlocks), "v2_docmap_blocks/op")
	b.ReportMetric(float64(stats.V2PostingBlocks), "v2_posting_blocks/op")
	b.ReportMetric(float64(stats.V2NormBlocks), "v2_norm_blocks/op")
	b.ReportMetric(float64(stats.V2PositionEntries), "v2_position_entries/op")
	b.ReportMetric(float64(stats.V2TermStats), "v2_term_stats/op")
	b.ReportMetric(float64(stats.V2PostingBlocks)/docsDivisor, "posting_blocks/doc")
	b.ReportMetric(0, "rewritten_blocks/doc")
	b.ReportMetric(float64(entries)/docsDivisor, "index_entries/doc")
	b.ReportMetric(float64(writeAmpEntries)/docsDivisor, "write_amp_entries/doc")
	b.ReportMetric(float64(stats.EncodedBytes)/docsDivisor, "index_bytes/doc")
	textV2ContractReportStorageLaneBytes2623(b, docs, stats)
}

func textV2ContractReportBackfillLaneBytes2623(b *testing.B, docs int, stats TextIndexBackfillStats) {
	b.Helper()
	docsDivisor := float64(maxTextV2ContractInt2623(docs, 1))
	b.ReportMetric(float64(stats.V2DocIDBytes)/docsDivisor, "v2_docid_bytes/doc")
	b.ReportMetric(float64(stats.V2DocMapBytes)/docsDivisor, "v2_docmap_bytes/doc")
	b.ReportMetric(float64(stats.V2PostingBlockBytes)/docsDivisor, "v2_posting_bytes/doc")
	b.ReportMetric(float64(stats.V2NormBlockBytes)/docsDivisor, "v2_norm_bytes/doc")
	b.ReportMetric(float64(stats.V2PositionBytes)/docsDivisor, "v2_position_bytes/doc")
	b.ReportMetric(float64(stats.V2TermStatsBytes)/docsDivisor, "v2_term_bytes/doc")
	b.ReportMetric(float64(stats.V2StatusFormatBytes)/docsDivisor, "v2_status_format_bytes/doc")
}

func textV2ContractReportStorageLaneBytes2623(b *testing.B, docs int, stats TextIndexStorageStats) {
	b.Helper()
	docsDivisor := float64(maxTextV2ContractInt2623(docs, 1))
	b.ReportMetric(float64(stats.V2DocIDBytes)/docsDivisor, "v2_docid_bytes/doc")
	b.ReportMetric(float64(stats.V2DocMapBytes)/docsDivisor, "v2_docmap_bytes/doc")
	b.ReportMetric(float64(stats.V2PostingBlockBytes)/docsDivisor, "v2_posting_bytes/doc")
	b.ReportMetric(float64(stats.V2NormBlockBytes)/docsDivisor, "v2_norm_bytes/doc")
	b.ReportMetric(float64(stats.V2PositionBytes)/docsDivisor, "v2_position_bytes/doc")
	b.ReportMetric(float64(stats.V2TermStatsBytes)/docsDivisor, "v2_term_bytes/doc")
	b.ReportMetric(float64(stats.V2StatusFormatBytes)/docsDivisor, "v2_status_format_bytes/doc")
}

func textV2ContractReportHighDFBlocks2626(b *testing.B, docs int, blocks uint64) {
	b.Helper()
	b.ReportMetric(float64(blocks), "high_df_posting_blocks/op")
	b.ReportMetric(float64(blocks)/float64(maxTextV2ContractInt2623(docs, 1)), "high_df_posting_blocks/doc")
}

func textV2ContractTermPostingBlockCount2626(tb testing.TB, col *Collection, term string) uint64 {
	tb.Helper()
	if col == nil || col.db == nil {
		return 0
	}
	snap := col.db.AcquireSnapshot()
	if snap == nil {
		tb.Fatalf("snapshot nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		tb.Fatalf("catalog snapshot: %v", err)
	}
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionTextV2TermsRootName(catalog.meta.Name, textV2ContractIndexName2623), encodeTextV2TermStatsKey(term), nil)
	if err != nil {
		tb.Fatalf("term stats %q: %v", term, err)
	}
	if !ok {
		return 0
	}
	stats, err := decodeTextV2TermStatsValue(raw)
	if err != nil {
		tb.Fatalf("decode term stats %q: %v", term, err)
	}
	return stats.PostingBlockCount
}

func textV2ContractUniqueTermStatsEntries2626(tb testing.TB, batches ...[][]byte) uint64 {
	tb.Helper()
	seen := make(map[string]struct{})
	def := textV2ContractV2IndexDefinition2626()
	for _, batch := range batches {
		for _, doc := range batch {
			analysis, err := analyzeTextIndexDocument(def, doc)
			if err != nil {
				tb.Fatalf("analyze benchmark document: %v", err)
			}
			for _, field := range analysis.Fields {
				for term := range field.Terms {
					seen[term] = struct{}{}
				}
			}
		}
	}
	return uint64(len(seen))
}

func textV2ContractV2MutationRootDeltaEntries2626(docs int, before, after TextIndexStorageStats, termStatEntries uint64) uint64 {
	docEntries := uint64(maxTextV2ContractInt2623(docs, 0))
	docMapBlocks := after.V2DocMapBlocks
	if docMapBlocks == 0 {
		docMapBlocks = before.V2DocMapBlocks
	}
	normBlocks := after.V2NormBlocks
	if normBlocks == 0 {
		normBlocks = before.V2NormBlocks
	}
	var postingBlocks uint64
	if after.V2PostingBlocks > before.V2PostingBlocks {
		postingBlocks = after.V2PostingBlocks - before.V2PostingBlocks
	}
	// V2 mutation writes include docID/docmap/norm roots, term-stat rows touched
	// by the mutated documents, appended posting blocks, and the status generation.
	return docEntries + docMapBlocks + normBlocks + postingBlocks + termStatEntries + 1
}

func textV2ContractStorageEntryCount2623(stats TextIndexStorageStats) uint64 {
	if stats.Version == TextIndexVersionV2 || stats.V2FormatRecords > 0 {
		return stats.V2DocIDEntries + stats.V2DocMapBlocks + stats.V2PostingBlocks + stats.V2NormBlocks + stats.V2PositionEntries + stats.V2TermStats + stats.V2FormatRecords + stats.V2StatusRecords
	}
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
	b.ReportMetric(float64(stats.TextBlockMaxFallbacks), "blockmax_fallbacks/search")
	b.ReportMetric(float64(stats.TextBlockMaxThresholds), "threshold_updates/search")
	b.ReportMetric(float64(stats.TextWANDPivots), "wand_pivots/search")
	b.ReportMetric(float64(stats.TextScalarPrefilterIDs), "scalar_prefilter_ids/search")
	b.ReportMetric(float64(stats.TextScalarPostingBlocksSkipped), "scalar_posting_blocks_skipped/search")
	b.ReportMetric(float64(stats.TextScalarPostingsRejected), "scalar_postings_rejected/search")
	b.ReportMetric(float64(stats.TextCandidatesScored), "candidates_scored/search")
	b.ReportMetric(float64(stats.TextStateLookups), "state_lookups/search")
	b.ReportMetric(float64(stats.TextNormLookups), "norm_lookups/search")
	b.ReportMetric(float64(stats.TextMatchDetailsBuilt), "match_details/search")
	b.ReportMetric(float64(stats.TextPositionLookups), "position_lookups/search")
	b.ReportMetric(float64(stats.TextPhraseCandidatesChecked), "phrase_candidates_checked/search")
	b.ReportMetric(float64(stats.TextPhraseCandidatesMatched), "phrase_candidates_matched/search")
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
