package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"testing"
)

func TestTextV2ScalarPruningPostFilterParity2874(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()

	const docsN = 512
	ids, docs := textV2ScalarPruningDocs2874(docsN)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}},
	}, ids, docs)

	cases := []struct {
		name         string
		query        string
		operator     TextSearchOperator
		topK         int
		allow        hybridScalarAllowSet
		wantBlocks   bool
		wantPostings bool
	}{
		{name: "selective_block_skip_single", query: "refund", topK: 5, allow: textV2ScalarAllowFirst2874(8), wantBlocks: true},
		{name: "interleaved_posting_reject_or", query: "refund OR policy", operator: TextSearchOperatorOR, topK: 8, allow: textV2ScalarAllowEvery2874(docsN, 17), wantPostings: true},
		{name: "interleaved_posting_reject_and", query: "refund AND policy", operator: TextSearchOperatorAND, topK: 8, allow: textV2ScalarAllowEvery2874(docsN, 19), wantPostings: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := TextSearchOptions{IndexName: "lexical", Query: tc.query, Operator: tc.operator, TopK: tc.topK, CandidateLimit: docsN, MaxPostingsScanned: docsN * 8, textV2AllowedDocumentIDs: tc.allow}
			got, err := col.searchText(opts, textSearchResultScoreOnly)
			if err != nil {
				t.Fatalf("scalar-pruned search: %v", err)
			}
			refOpts := opts
			refOpts.TopK = docsN
			refOpts.CandidateLimit = docsN
			refOpts.textV2AllowedDocumentIDs = nil
			refOpts.textV2DisableBlockMax = true
			all, err := col.searchText(refOpts, textSearchResultScoreOnly)
			if err != nil {
				t.Fatalf("exhaustive unfiltered reference: %v", err)
			}
			want := textV2ScalarPostFilterReference2874(all, tc.allow, tc.topK)
			assertTextV2ScalarResults2874(t, got.Results, want)
			if got.Stats.FailClosed != 0 || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.TextStateLookups != 0 {
				t.Fatalf("stats=%+v want exact no-doc scalar pruning", got.Stats)
			}
			if got.Stats.TextScalarPrefilterIDs != uint64(len(tc.allow)) {
				t.Fatalf("stats=%+v want scalar prefilter ids=%d", got.Stats, len(tc.allow))
			}
			if tc.wantBlocks && got.Stats.TextScalarPostingBlocksSkipped == 0 {
				t.Fatalf("stats=%+v want scalar posting-block skips", got.Stats)
			}
			if tc.wantPostings && got.Stats.TextScalarPostingsRejected == 0 {
				t.Fatalf("stats=%+v want scalar posting rejections", got.Stats)
			}
			if got.Stats.TextCandidatesScored > uint64(len(tc.allow)) {
				t.Fatalf("stats=%+v want scored candidates bounded by allow-set size %d", got.Stats, len(tc.allow))
			}
		})
	}
}

func TestTextV2ScalarPruningMissingAllowedDocIDFailsClosed2874(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()

	ids, docs := textV2ScalarPruningDocs2874(32)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title"}, {Field: "body"}}}, ids, docs)
	got, err := col.searchText(TextSearchOptions{
		IndexName:                "lexical",
		Query:                    "refund",
		TopK:                     5,
		CandidateLimit:           32,
		MaxPostingsScanned:       128,
		textV2AllowedDocumentIDs: hybridScalarAllowSet{"doc-000000": {}, "doc-missing": {}},
	}, textSearchResultScoreOnly)
	if !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("err=%v want text index unavailable/storage corrupt", err)
	}
	if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != textSearchFailClosedStorageCorrupt || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.TextPostingsScanned != 0 {
		t.Fatalf("response=%+v want fail-closed before posting traversal and no docs", got)
	}
}

func TestTextV2ScalarPruningHybridReopenNoDocGuardrail2874(t *testing.T) {
	dir := t.TempDir()
	d := openTextV2TestDB(t, dir, false)
	closed := false
	defer func() {
		if !closed {
			_ = d.Close()
		}
	}()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Indexes: []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	ids, docs := textV2ScalarPruningTenantDocs2874(160, 6)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 2}, {Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}

	before := textV2SearchHybridRareTenant2874(t, col, 4)
	if before.Stats.DocumentsFetched != 0 || before.Stats.FullDocumentScanFallbacks != 0 || before.Stats.FailClosed != 0 || before.Stats.TextScalarPrefilterIDs != 6 {
		t.Fatalf("before=%+v want no-doc scalar-pruned hybrid search", before)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true

	reopened := openTextV2TestDB(t, dir, false)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	after := textV2SearchHybridRareTenant2874(t, reopenedCol, 4)
	assertHybridSearchResultIDs2874(t, after.Results, hybridSearchResultIDs2874(before.Results))
	if after.Stats.DocumentsFetched != 0 || after.Stats.FullDocumentScanFallbacks != 0 || after.Stats.FailClosed != 0 || after.Stats.TextScalarPrefilterIDs != before.Stats.TextScalarPrefilterIDs {
		t.Fatalf("after=%+v before=%+v want snapshot-stable no-doc scalar pruning after reopen", after, before)
	}
}

func BenchmarkTextV2ScalarPruningSelectivity2874(b *testing.B) {
	d := openTextV2TestDB(b, b.TempDir(), false)
	defer func() { _ = d.Close() }()

	const docsN = 4096
	ids, docs := textV2ScalarPruningDocs2874(docsN)
	col := createTextSearchCollection2627(b, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}}}, ids, docs)
	cases := []struct {
		name  string
		allow hybridScalarAllowSet
	}{
		{name: "no_filter", allow: nil},
		{name: "selectivity_0_1pct", allow: textV2ScalarAllowCount2874(docsN, maxTextV2ScalarInt2874(1, docsN/1000))},
		{name: "selectivity_1pct", allow: textV2ScalarAllowCount2874(docsN, docsN/100)},
		{name: "selectivity_5pct", allow: textV2ScalarAllowCount2874(docsN, docsN*5/100)},
		{name: "selectivity_10pct", allow: textV2ScalarAllowCount2874(docsN, docsN/10)},
		{name: "selectivity_25pct", allow: textV2ScalarAllowCount2874(docsN, docsN/4)},
		{name: "selectivity_50pct", allow: textV2ScalarAllowCount2874(docsN, docsN/2)},
		{name: "selectivity_100pct", allow: textV2ScalarAllowCount2874(docsN, docsN)},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			opts := TextSearchOptions{IndexName: "lexical", Query: "refund OR policy", Operator: TextSearchOperatorOR, TopK: 10, CandidateLimit: docsN, MaxPostingsScanned: docsN * 8, textV2AllowedDocumentIDs: tc.allow}
			var last TextSearchResponse
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := col.searchText(opts, textSearchResultScoreOnly)
				if err != nil {
					b.Fatalf("searchText: %v", err)
				}
				last = got
			}
			b.StopTimer()
			b.ReportMetric(float64(last.Stats.TextScalarPrefilterIDs), "scalar_prefilter_ids/search")
			b.ReportMetric(float64(last.Stats.TextScalarPostingBlocksSkipped), "scalar_posting_blocks_skipped/search")
			b.ReportMetric(float64(last.Stats.TextScalarPostingsRejected), "scalar_postings_rejected/search")
			b.ReportMetric(float64(last.Stats.TextCandidatesScored), "candidates_scored/search")
			b.ReportMetric(float64(last.Stats.TextCandidatesReturned), "text_candidates/search")
			b.ReportMetric(float64(last.Stats.TextPostingBlocksVisited), "blocks_visited/search")
			b.ReportMetric(float64(last.Stats.TextPostingBlocksSkipped), "blocks_skipped/search")
			b.ReportMetric(float64(last.Stats.DocumentsFetched), "docs_fetched/search")
			b.ReportMetric(float64(last.Stats.TextPostingsScanned), "postings_scanned/search")
		})
	}
}

func textV2ScalarPruningDocs2874(count int) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		title := "refund policy"
		if i%7 == 0 {
			title = "refund refund policy"
		}
		body := fmt.Sprintf("refund policy support common shard-%02d bucket-%02d", i%13, i%29)
		docs[i] = []byte(fmt.Sprintf(`{"title":%q,"body":%q}`, title, body))
	}
	return ids, docs
}

func textV2ScalarPruningTenantDocs2874(count, rare int) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		tenant := "tenant-broad"
		if i < rare {
			tenant = "tenant-rare"
		}
		docs[i] = []byte(fmt.Sprintf(`{"title":"refund policy","body":"refund policy support %03d","tenant":%q}`, i, tenant))
	}
	return ids, docs
}

func textV2ScalarAllowFirst2874(count int) hybridScalarAllowSet {
	return textV2ScalarAllowCount2874(count, count)
}

func textV2ScalarAllowEvery2874(total, step int) hybridScalarAllowSet {
	allow := make(hybridScalarAllowSet)
	if step <= 0 {
		step = 1
	}
	for i := 0; i < total; i += step {
		allow[fmt.Sprintf("doc-%06d", i)] = struct{}{}
	}
	return allow
}

func textV2ScalarAllowCount2874(total, count int) hybridScalarAllowSet {
	allow := make(hybridScalarAllowSet, count)
	if count > total {
		count = total
	}
	for i := 0; i < count; i++ {
		allow[fmt.Sprintf("doc-%06d", i)] = struct{}{}
	}
	return allow
}

func textV2ScalarPostFilterReference2874(all TextSearchResponse, allow hybridScalarAllowSet, topK int) []TextSearchResult {
	filtered := make([]TextSearchResult, 0, len(all.Results))
	for _, result := range all.Results {
		if _, ok := allow[string(result.DocumentID)]; !ok {
			continue
		}
		result.Rank = len(filtered) + 1
		filtered = append(filtered, result)
		if len(filtered) == topK {
			break
		}
	}
	return filtered
}

func assertTextV2ScalarResults2874(t *testing.T, got, want []TextSearchResult) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("result length got=%d want=%d got=%+v want=%+v", len(got), len(want), got, want)
	}
	for i := range want {
		if !bytes.Equal(got[i].DocumentID, want[i].DocumentID) || got[i].Rank != want[i].Rank || math.Abs(got[i].Score-want[i].Score) > 1e-12 {
			t.Fatalf("result[%d] got=%+v want=%+v", i, got[i], want[i])
		}
	}
}

func textV2SearchHybridRareTenant2874(t testing.TB, col *Collection, topK int) HybridSearchResponse {
	t.Helper()
	got, err := col.SearchHybrid(HybridSearchOptions{TopK: topK, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund OR policy", CandidateLimit: 64}, ScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "tenant-rare"}, ResultMode: HybridResultModeScoreOnly})
	if err != nil {
		t.Fatalf("SearchHybrid rare tenant: %v", err)
	}
	return got
}

func hybridSearchResultIDs2874(results []HybridSearchResult) []string {
	ids := make([]string, len(results))
	for i := range results {
		ids[i] = string(results[i].ID)
	}
	return ids
}

func assertHybridSearchResultIDs2874(t testing.TB, results []HybridSearchResult, want []string) {
	t.Helper()
	got := hybridSearchResultIDs2874(results)
	if !slicesEqualStrings(got, want) {
		t.Fatalf("ids=%v want %v results=%+v", got, want, results)
	}
}

func maxTextV2ScalarInt2874(a, b int) int {
	if a > b {
		return a
	}
	return b
}
