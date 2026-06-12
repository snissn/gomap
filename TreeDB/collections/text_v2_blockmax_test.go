package collections

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func TestTextV2BlockMaxSingleTermExactParityAndSkips2628(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxFixtureDocs2628(384, 16)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}},
	}, ids, docs)

	opts := TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 5, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 2}
	exhaustiveOpts := opts
	exhaustiveOpts.textV2DisableBlockMax = true
	exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("exhaustive v2 search: %v", err)
	}
	got, err := col.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("block-max v2 search: %v", err)
	}
	assertTextSearchParity2627(t, got, exhaustive)
	if got.Stats.TextPostingBlocksSkipped == 0 || got.Stats.TextBlockMaxThresholds == 0 {
		t.Fatalf("stats=%+v want block skips and threshold updates", got.Stats)
	}
	if got.Stats.TextPostingsScanned >= exhaustive.Stats.TextPostingsScanned || got.Stats.TextCandidatesScored >= exhaustive.Stats.TextCandidatesScored {
		t.Fatalf("block-max stats=%+v exhaustive=%+v want fewer decoded/scored postings", got.Stats, exhaustive.Stats)
	}
	if got.Stats.TextStateLookups != 0 || got.Stats.TextMatchDetailsBuilt != 0 || got.Stats.DocumentsFetched != 0 {
		t.Fatalf("stats=%+v want score-only zero-doc v2 path", got.Stats)
	}
}

func TestTextV2BlockMaxRandomizedExactnessAndFallbacks2628(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			d := openTextV2TestDB(t, t.TempDir(), false)
			defer func() { _ = d.Close() }()
			fields := []TextIndexField{{Field: "title", Weight: 1 + float64(seed%4)}, {Field: "body", Weight: 0.5 + float64(seed%3)}}
			ids := make([][]byte, 192)
			docs := make([][]byte, 192)
			rareIDs := make(hybridScalarAllowSet)
			for i := range ids {
				ids[i] = []byte(fmt.Sprintf("doc-%03d", i))
				tenant := "tenant-broad"
				if i%37 == 0 {
					tenant = "tenant-rare"
					rareIDs[string(ids[i])] = struct{}{}
				}
				titleTerms := textV2RandomTerms2628(rng, []string{"refund", "policy", "shipping", "chargeback"}, 1+rng.Intn(5))
				bodyTerms := textV2RandomTerms2628(rng, []string{"refund", "policy", "support", "invoice", "common"}, 2+rng.Intn(9))
				docs[i] = []byte(fmt.Sprintf(`{"title":"%s","body":"%s","tenant":"%s"}`, titleTerms, bodyTerms, tenant))
			}
			col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: fields}, ids, docs)
			cases := []struct {
				name     string
				opts     TextSearchOptions
				wantFall bool
			}{
				{name: "single_top1", opts: TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 1, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4}},
				{name: "single_top7_rare_filter", opts: TextSearchOptions{IndexName: "lexical", Query: "policy", TopK: 7, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4, textV2AllowedDocumentIDs: rareIDs}},
				{name: "and_fallback", opts: TextSearchOptions{IndexName: "lexical", Query: "refund AND policy", Operator: TextSearchOperatorAND, TopK: 10, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 8}, wantFall: true},
				{name: "or_fallback_filtered", opts: TextSearchOptions{IndexName: "lexical", Query: "refund OR shipping", TopK: 10, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 8, textV2AllowedDocumentIDs: rareIDs}, wantFall: true},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					exhaustiveOpts := tc.opts
					exhaustiveOpts.textV2DisableBlockMax = true
					exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
					if err != nil {
						t.Fatalf("exhaustive: %v", err)
					}
					got, err := col.searchText(tc.opts, textSearchResultScoreOnly)
					if err != nil {
						t.Fatalf("block-max/fallback: %v", err)
					}
					assertTextSearchParity2627(t, got, exhaustive)
					if tc.wantFall && got.Stats.TextBlockMaxFallbacks == 0 {
						t.Fatalf("stats=%+v want exact exhaustive fallback counter", got.Stats)
					}
				})
			}
		})
	}
}

func TestTextV2ScalarFilterPruningEmptyRareBroad2628(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "docs",
		Indexes: []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	ids, docs := textV2BlockMaxScalarDocs2628(384)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}

	empty, err := col.SearchHybrid(HybridSearchOptions{TopK: 5, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: len(ids)}, ScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "tenant-missing"}})
	if err != nil {
		t.Fatalf("empty scalar SearchHybrid: %v", err)
	}
	if len(empty.Results) != 0 || empty.Stats.TextPostingsScanned != 0 || empty.Stats.FailClosed != 0 {
		t.Fatalf("empty response=%+v want no text traversal", empty)
	}

	rare, err := col.SearchHybrid(HybridSearchOptions{TopK: 5, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: len(ids)}, ScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "tenant-rare"}})
	if err != nil {
		t.Fatalf("rare scalar SearchHybrid: %v", err)
	}
	if len(rare.Results) == 0 || rare.Stats.TextPostingBlocksSkipped == 0 || rare.Stats.TextPostingsScanned >= uint64(len(ids)) || rare.Stats.FailClosed != 0 {
		t.Fatalf("rare response=%+v want scalar block pruning", rare)
	}
	rareIDs := map[string]struct{}{"doc-000000": {}, "doc-000001": {}, "doc-000002": {}, "doc-000003": {}}
	for _, result := range rare.Results {
		if _, ok := rareIDs[string(result.ID)]; !ok {
			t.Fatalf("rare result id=%q outside rare tenant set", result.ID)
		}
	}

	broad, err := col.SearchHybrid(HybridSearchOptions{TopK: 5, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: len(ids)}, ScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "tenant-broad"}})
	if err != nil {
		t.Fatalf("broad scalar SearchHybrid: %v", err)
	}
	if len(broad.Results) == 0 || broad.Stats.FailClosed != 0 || broad.Stats.ScalarFilterSelectivityPPM == 0 {
		t.Fatalf("broad response=%+v want successful broad indexed filter", broad)
	}
	if rare.Stats.TextPostingsScanned >= broad.Stats.TextPostingsScanned {
		t.Fatalf("rare stats=%+v broad stats=%+v want rare filter to decode fewer postings", rare.Stats, broad.Stats)
	}
}

func TestTextV2BlockMaxCorruptMetadataFailsClosed2628(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxFixtureDocs2628(160, 8)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}}}, ids, docs)
	rootName := collectionTextV2PostingBlocksRootName("docs", "lexical")
	blockKey := firstTextV2PostingBlockKeyForTerm2626(t, d, "docs", rootName, "refund")
	raw := textV2ReadRootBytes2624(t, d, "docs", rootName, blockKey)
	if len(raw) < textV2PostingBlockChecksumBytes+1 {
		t.Fatalf("posting block raw too short: %d", len(raw))
	}
	corrupt := append([]byte(nil), raw...)
	corrupt[len(corrupt)-1] ^= 0x7f
	corruptTextRootValue(t, d, "docs", rootName, blockKey, corrupt)

	got, err := col.searchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 5, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 2}, textSearchResultScoreOnly)
	if !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("SearchText err=%v want unavailable/storage corrupt", err)
	}
	if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != textSearchFailClosedStorageCorrupt || got.Stats.DocumentsFetched != 0 {
		t.Fatalf("stats=%+v want metadata fail-closed without docs", got.Stats)
	}
}

func textV2BlockMaxFixtureDocs2628(count, highDocs int) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		title := "refund"
		body := "refund"
		if i < highDocs {
			title = strings.Repeat("refund ", 80)
			body = strings.Repeat("refund ", 160)
		}
		docs[i] = []byte(fmt.Sprintf(`{"title":"%s","body":"%s","tenant":"tenant-broad"}`, strings.TrimSpace(title), strings.TrimSpace(body)))
	}
	return ids, docs
}

func textV2BlockMaxScalarDocs2628(count int) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		tenant := "tenant-broad"
		if i < 4 {
			tenant = "tenant-rare"
		}
		title := "refund"
		body := "refund policy support"
		if i < 8 {
			title = strings.Repeat("refund ", 80)
			body = strings.Repeat("refund ", 160)
		}
		docs[i] = []byte(fmt.Sprintf(`{"title":"%s","body":"%s","tenant":"%s"}`, strings.TrimSpace(title), strings.TrimSpace(body), tenant))
	}
	return ids, docs
}

func textV2RandomTerms2628(rng *rand.Rand, terms []string, count int) string {
	out := make([]string, count)
	for i := range out {
		out[i] = terms[rng.Intn(len(terms))]
		if rng.Intn(11) == 0 {
			out[i] += " " + terms[rng.Intn(len(terms))]
		}
	}
	return strings.Join(out, " ")
}
