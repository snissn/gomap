package collections

import (
	"errors"
	"fmt"
	"math"
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

func TestTextV2BlockMaxMultiTermANDExactParitySkipsAndLazyDetails2688(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxMultiTermDocs2688(512, 24)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}},
	}, ids, docs)

	opts := TextSearchOptions{IndexName: "lexical", Query: "refund AND policy", Operator: TextSearchOperatorAND, TopK: 8, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4}
	exhaustiveOpts := opts
	exhaustiveOpts.textV2DisableBlockMax = true
	exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("exhaustive v2 AND search: %v", err)
	}
	got, err := col.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("block-max v2 AND search: %v", err)
	}
	assertTextSearchParity2627(t, got, exhaustive)
	if got.Stats.TextBlockMaxFallbacks != 0 || got.Stats.TextPostingBlocksSkipped == 0 || got.Stats.TextBlockMaxThresholds == 0 {
		t.Fatalf("stats=%+v want native multi-term block-max skips without fallback", got.Stats)
	}
	if got.Stats.TextPostingsScanned >= exhaustive.Stats.TextPostingsScanned || got.Stats.TextCandidatesScored >= exhaustive.Stats.TextCandidatesScored {
		t.Fatalf("block-max stats=%+v exhaustive=%+v want less multi-term decode/scoring work", got.Stats, exhaustive.Stats)
	}
	if got.Stats.TextStateLookups != 0 || got.Stats.TextMatchDetailsBuilt != 0 || got.Stats.DocumentsFetched != 0 {
		t.Fatalf("stats=%+v want score-only zero-doc/no-state path", got.Stats)
	}

	detailed, err := col.searchText(opts, textSearchResultFull)
	if err != nil {
		t.Fatalf("detailed block-max v2 AND search: %v", err)
	}
	assertTextSearchParity2627(t, detailed, exhaustive)
	if detailed.Stats.TextMatchDetailsBuilt != uint64(len(detailed.Results)) || detailed.Stats.TextStateLookups != 0 || detailed.Stats.DocumentsFetched != 0 {
		t.Fatalf("detailed stats=%+v results=%d want lazy topK-bounded details", detailed.Stats, len(detailed.Results))
	}
}

func TestTextV2BlockMaxMultiTermORExactParitySkipsAndLazyDetails2730(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxMultiTermDocs2688(512, 24)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}},
	}, ids, docs)

	opts := TextSearchOptions{IndexName: "lexical", Query: "refund OR policy", Operator: TextSearchOperatorOR, TopK: 8, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4}
	exhaustiveOpts := opts
	exhaustiveOpts.textV2DisableBlockMax = true
	exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("exhaustive v2 OR search: %v", err)
	}
	got, err := col.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("block-max v2 OR search: %v", err)
	}
	assertTextSearchParity2627(t, got, exhaustive)
	if got.Stats.TextBlockMaxFallbacks != 0 || got.Stats.TextPostingBlocksSkipped == 0 || got.Stats.TextBlockMaxThresholds == 0 {
		t.Fatalf("stats=%+v want native multi-term OR block-max skips without fallback", got.Stats)
	}
	if got.Stats.TextPostingsScanned >= exhaustive.Stats.TextPostingsScanned || got.Stats.TextCandidatesScored >= exhaustive.Stats.TextCandidatesScored {
		t.Fatalf("block-max stats=%+v exhaustive=%+v want less OR decode/scoring work", got.Stats, exhaustive.Stats)
	}
	if got.Stats.TextStateLookups != 0 || got.Stats.TextMatchDetailsBuilt != 0 || got.Stats.DocumentsFetched != 0 {
		t.Fatalf("stats=%+v want score-only zero-doc/no-state OR path", got.Stats)
	}

	detailed, err := col.searchText(opts, textSearchResultFull)
	if err != nil {
		t.Fatalf("detailed block-max v2 OR search: %v", err)
	}
	assertTextSearchParity2627(t, detailed, exhaustive)
	if detailed.Stats.TextMatchDetailsBuilt != uint64(len(detailed.Results)) || detailed.Stats.TextStateLookups != 0 || detailed.Stats.DocumentsFetched != 0 {
		t.Fatalf("detailed stats=%+v results=%d want lazy topK-bounded OR details", detailed.Stats, len(detailed.Results))
	}
	for _, result := range detailed.Results {
		if len(result.TextMatches) == 0 || len(result.MatchedTerms) == 0 {
			t.Fatalf("detailed result=%+v want OR match attribution", result)
		}
	}
}

func TestTextV2BlockMaxMultiTermORMixedDisjunctionExact2730(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxMixedDisjunctionDocs2730(768, 48, 0)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}},
	}, ids, docs)

	opts := TextSearchOptions{IndexName: "lexical", Query: "refund OR policy", Operator: TextSearchOperatorOR, TopK: 24, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4}
	exhaustiveOpts := opts
	exhaustiveOpts.textV2DisableBlockMax = true
	exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("exhaustive mixed OR search: %v", err)
	}
	got, err := col.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("block-max mixed OR search: %v", err)
	}
	assertTextSearchParity2627(t, got, exhaustive)
	if got.Stats.TextBlockMaxFallbacks != 0 || got.Stats.TextBlockMaxThresholds == 0 || got.Stats.TextCandidatesScored >= exhaustive.Stats.TextCandidatesScored {
		t.Fatalf("stats=%+v exhaustive=%+v want native mixed OR candidate pruning", got.Stats, exhaustive.Stats)
	}
	if got.Stats.DocumentsFetched != 0 || got.Stats.TextStateLookups != 0 || got.Stats.TextMatchDetailsBuilt != 0 {
		t.Fatalf("stats=%+v want score-only zero-doc/no-state mixed OR path", got.Stats)
	}

	detailed, err := col.searchText(opts, textSearchResultFull)
	if err != nil {
		t.Fatalf("detailed mixed OR search: %v", err)
	}
	assertTextSearchParity2627(t, detailed, exhaustive)
	refundOnly, policyOnly := false, false
	for _, result := range detailed.Results {
		terms := make(map[string]struct{}, len(result.MatchedTerms))
		for _, term := range result.MatchedTerms {
			terms[term] = struct{}{}
		}
		_, hasRefund := terms["refund"]
		_, hasPolicy := terms["policy"]
		refundOnly = refundOnly || (hasRefund && !hasPolicy)
		policyOnly = policyOnly || (hasPolicy && !hasRefund)
	}
	if !refundOnly || !policyOnly {
		t.Fatalf("detailed results=%+v want both refund-only and policy-only OR hits", detailed.Results)
	}
}

func TestTextV2BlockMaxMultiTermORTieBreak2730(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("doc-c"), []byte("doc-a"), []byte("doc-b"), []byte("doc-d")}
	docs := [][]byte{
		[]byte(`{"title":"refund policy","body":"refund policy"}`),
		[]byte(`{"title":"refund policy","body":"refund policy"}`),
		[]byte(`{"title":"refund policy","body":"refund policy"}`),
		[]byte(`{"title":"refund policy","body":"refund policy"}`),
	}
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title"}, {Field: "body"}}}, ids, docs)
	opts := TextSearchOptions{IndexName: "lexical", Query: "refund OR policy", Operator: TextSearchOperatorOR, TopK: 3, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4}
	exhaustiveOpts := opts
	exhaustiveOpts.textV2DisableBlockMax = true
	exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("exhaustive OR tie search: %v", err)
	}
	got, err := col.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("block-max OR tie search: %v", err)
	}
	assertTextSearchParity2627(t, got, exhaustive)
	want := []string{"doc-a", "doc-b", "doc-c"}
	for i, id := range want {
		if string(got.Results[i].DocumentID) != id {
			t.Fatalf("results=%+v want lexical tie order %v", got.Results, want)
		}
	}
}

func TestTextV2BlockMaxHighDFTieExactPruning2873(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxUniformHighDFDocs2873(1024, false)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}},
	}, ids, docs)

	cases := []struct {
		name     string
		query    string
		operator TextSearchOperator
	}{
		{name: "common", query: "refund"},
		{name: "and", query: "refund AND policy", operator: TextSearchOperatorAND},
		{name: "or", query: "refund OR policy", operator: TextSearchOperatorOR},
		{name: "or_high_frequency", query: "refund OR policy OR support OR common", operator: TextSearchOperatorOR},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := TextSearchOptions{IndexName: "lexical", Query: tc.query, Operator: tc.operator, TopK: 10, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 8}
			exhaustiveOpts := opts
			exhaustiveOpts.textV2DisableBlockMax = true
			exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
			if err != nil {
				t.Fatalf("exhaustive %s: %v", tc.name, err)
			}
			got, err := col.searchText(opts, textSearchResultScoreOnly)
			if err != nil {
				t.Fatalf("block-max %s: %v", tc.name, err)
			}
			assertTextSearchParity2627(t, got, exhaustive)
			if got.Stats.TextBlockMaxFallbacks != 0 || got.Stats.FailClosed != 0 || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
				t.Fatalf("stats=%+v want native exact no-doc high-DF path", got.Stats)
			}
			if got.Stats.TextStateLookups != 0 || got.Stats.TextMatchDetailsBuilt != 0 {
				t.Fatalf("stats=%+v want score-only no-state/no-match-detail high-DF path", got.Stats)
			}
			if got.Stats.TextPostingBlocksSkipped == 0 || got.Stats.TextBlockMaxThresholds == 0 {
				t.Fatalf("stats=%+v want high-DF block skips and threshold updates", got.Stats)
			}
			if got.Stats.TextPostingsScanned >= exhaustive.Stats.TextPostingsScanned || got.Stats.TextCandidatesScored >= exhaustive.Stats.TextCandidatesScored {
				t.Fatalf("block-max stats=%+v exhaustive=%+v want fewer high-DF decoded/scored postings", got.Stats, exhaustive.Stats)
			}
		})
	}
}

func TestTextV2BlockMaxMissingNormBlockFailsClosed2873(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxUniformHighDFDocs2873(1024, false)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}},
	}, ids, docs)

	missingBlockStart := uint64(textV2DefaultNormBlockSize) + 1
	normRoot := collectionTextV2NormBlocksRootName("docs", "lexical")
	if raw := textV2ReadRootBytes2624(t, d, "docs", normRoot, encodeTextV2BlockKey(missingBlockStart)); len(raw) == 0 {
		t.Fatalf("missing fixture norm block %d before corruption", missingBlockStart)
	}
	deleteTextRootValue2624(t, d, "docs", normRoot, encodeTextV2BlockKey(missingBlockStart))

	opts := TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 2}
	exhaustiveOpts := opts
	exhaustiveOpts.textV2DisableBlockMax = true
	exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
	if !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("exhaustive err=%v response=%+v want unavailable/storage corrupt", err, exhaustive)
	}
	if exhaustive.Stats.FailClosed != 1 || exhaustive.Stats.FailClosedReason != textSearchFailClosedStorageCorrupt || exhaustive.Stats.DocumentsFetched != 0 {
		t.Fatalf("exhaustive stats=%+v want storage-corrupt fail-closed without docs", exhaustive.Stats)
	}

	got, err := col.searchText(opts, textSearchResultScoreOnly)
	if !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("block-max err=%v response=%+v want unavailable/storage corrupt", err, got)
	}
	if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != textSearchFailClosedStorageCorrupt || got.Stats.DocumentsFetched != 0 {
		t.Fatalf("block-max stats=%+v want storage-corrupt fail-closed without docs", got.Stats)
	}
}

func TestTextV2BlockMaxMissingDocMapBlockFailsClosed2873(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxUniformHighDFDocs2873(1024, false)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}},
	}, ids, docs)

	missingBlockStart := uint64(textV2DefaultDocMapBlockSize) + 1
	docMapRoot := collectionTextV2DocMapRootName("docs", "lexical")
	if raw := textV2ReadRootBytes2624(t, d, "docs", docMapRoot, encodeTextV2BlockKey(missingBlockStart)); len(raw) == 0 {
		t.Fatalf("missing fixture docmap block %d before corruption", missingBlockStart)
	}
	deleteTextRootValue2624(t, d, "docs", docMapRoot, encodeTextV2BlockKey(missingBlockStart))

	opts := TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 2}
	exhaustiveOpts := opts
	exhaustiveOpts.textV2DisableBlockMax = true
	exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
	if !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("exhaustive err=%v response=%+v want unavailable/storage corrupt", err, exhaustive)
	}
	if exhaustive.Stats.FailClosed != 1 || exhaustive.Stats.FailClosedReason != textSearchFailClosedStorageCorrupt || exhaustive.Stats.DocumentsFetched != 0 {
		t.Fatalf("exhaustive stats=%+v want storage-corrupt fail-closed without docs", exhaustive.Stats)
	}

	got, err := col.searchText(opts, textSearchResultScoreOnly)
	if !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("block-max err=%v response=%+v want unavailable/storage corrupt", err, got)
	}
	if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != textSearchFailClosedStorageCorrupt || got.Stats.DocumentsFetched != 0 {
		t.Fatalf("block-max stats=%+v want storage-corrupt fail-closed without docs", got.Stats)
	}
}

func TestTextV2BlockMaxEmptyNormBlockRangeFailsClosed2873(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxUniformHighDFDocs2873(1024, false)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}},
	}, ids, docs)

	blockStart := uint64(textV2DefaultNormBlockSize) + 1
	normRoot := collectionTextV2NormBlocksRootName("docs", "lexical")
	key := encodeTextV2BlockKey(blockStart)
	raw := textV2ReadRootBytes2624(t, d, "docs", normRoot, key)
	block, err := decodeTextV2NormBlockValue(raw)
	if err != nil {
		t.Fatalf("decode norm block: %v", err)
	}
	if len(block.Entries) == 0 {
		t.Fatalf("fixture norm block %d has no entries before corruption", blockStart)
	}
	block.Entries = nil
	corruptTextRootValue(t, d, "docs", normRoot, key, encodeTextV2NormBlockValue(block))

	opts := TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 2}
	got, err := col.searchText(opts, textSearchResultScoreOnly)
	if !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("block-max err=%v response=%+v want unavailable/storage corrupt", err, got)
	}
	if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != textSearchFailClosedStorageCorrupt || got.Stats.DocumentsFetched != 0 {
		t.Fatalf("block-max stats=%+v want storage-corrupt fail-closed without docs", got.Stats)
	}
}

func TestTextV2BlockMaxEmptyDocMapBlockRangeFailsClosed2873(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxUniformHighDFDocs2873(1024, false)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}},
	}, ids, docs)

	blockStart := uint64(textV2DefaultDocMapBlockSize) + 1
	docMapRoot := collectionTextV2DocMapRootName("docs", "lexical")
	key := encodeTextV2BlockKey(blockStart)
	raw := textV2ReadRootBytes2624(t, d, "docs", docMapRoot, key)
	block, err := decodeTextV2DocMapBlockValue(raw)
	if err != nil {
		t.Fatalf("decode docmap block: %v", err)
	}
	if len(block.Entries) == 0 {
		t.Fatalf("fixture docmap block %d has no entries before corruption", blockStart)
	}
	block.Entries = nil
	corruptTextRootValue(t, d, "docs", docMapRoot, key, encodeTextV2DocMapBlockValue(block))

	opts := TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 2}
	got, err := col.searchText(opts, textSearchResultScoreOnly)
	if !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("block-max err=%v response=%+v want unavailable/storage corrupt", err, got)
	}
	if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != textSearchFailClosedStorageCorrupt || got.Stats.DocumentsFetched != 0 {
		t.Fatalf("block-max stats=%+v want storage-corrupt fail-closed without docs", got.Stats)
	}
}

func TestTextV2BlockMaxHighDFMicroBlocksExactPruning2873(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxUniformHighDFDocs2873(1024, false)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:    "lexical",
			Version: TextIndexVersionV2,
			Fields:  []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}},
		}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	cases := []struct {
		name     string
		query    string
		operator TextSearchOperator
	}{
		{name: "common", query: "refund"},
		{name: "and", query: "refund AND policy", operator: TextSearchOperatorAND},
		{name: "or", query: "refund OR policy", operator: TextSearchOperatorOR},
		{name: "or_high_frequency", query: "refund OR policy OR support OR common", operator: TextSearchOperatorOR},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := TextSearchOptions{IndexName: "lexical", Query: tc.query, Operator: tc.operator, TopK: 10, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 8}
			exhaustiveOpts := opts
			exhaustiveOpts.textV2DisableBlockMax = true
			exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
			if err != nil {
				t.Fatalf("exhaustive micro %s: %v", tc.name, err)
			}
			got, err := col.searchText(opts, textSearchResultScoreOnly)
			if err != nil {
				t.Fatalf("block-max micro %s: %v", tc.name, err)
			}
			assertTextSearchParity2627(t, got, exhaustive)
			if got.Stats.TextBlockMaxFallbacks != 0 || got.Stats.FailClosed != 0 || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
				t.Fatalf("stats=%+v want native exact no-doc micro-block path", got.Stats)
			}
			if got.Stats.TextPostingBlocksSkipped == 0 || got.Stats.TextBlockMaxThresholds == 0 {
				t.Fatalf("stats=%+v want micro-block high-DF skips and threshold updates", got.Stats)
			}
			if got.Stats.TextPostingsScanned >= exhaustive.Stats.TextPostingsScanned || got.Stats.TextCandidatesScored >= exhaustive.Stats.TextCandidatesScored {
				t.Fatalf("micro block-max stats=%+v exhaustive=%+v want fewer decoded/scored postings", got.Stats, exhaustive.Stats)
			}
		})
	}
}

func TestTextV2BlockMaxHighDFTiePruningRespectsDocIDOrder2873(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxUniformHighDFDocs2873(384, true)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}}}, ids, docs)

	opts := TextSearchOptions{IndexName: "lexical", Query: "refund OR policy", Operator: TextSearchOperatorOR, TopK: 10, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4}
	exhaustiveOpts := opts
	exhaustiveOpts.textV2DisableBlockMax = true
	exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("exhaustive reversed-id OR: %v", err)
	}
	got, err := col.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("block-max reversed-id OR: %v", err)
	}
	assertTextSearchParity2627(t, got, exhaustive)
	for i := 0; i < opts.TopK; i++ {
		wantID := fmt.Sprintf("doc-%06d", i)
		if string(got.Results[i].DocumentID) != wantID {
			t.Fatalf("results=%+v want lexical tie winner %q at rank %d", got.Results, wantID, i+1)
		}
	}
	if got.Stats.TextBlockMaxFallbacks != 0 || got.Stats.FailClosed != 0 || got.Stats.DocumentsFetched != 0 {
		t.Fatalf("stats=%+v want exact native reversed-id tie path", got.Stats)
	}
}

func TestTextV2BlockMaxMultiTermORReopen2730(t *testing.T) {
	dir := t.TempDir()
	d := openTextV2TestDB(t, dir, false)
	ids, docs := textV2BlockMaxMultiTermDocs2688(384, 16)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}}}, ids, docs)
	opts := TextSearchOptions{IndexName: "lexical", Query: "refund OR policy", Operator: TextSearchOperatorOR, TopK: 8, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4}
	before, err := col.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("before reopen OR search: %v", err)
	}
	if len(before.Results) != opts.TopK || before.Stats.FailClosed != 0 || before.Stats.TextBlockMaxFallbacks != 0 {
		t.Fatalf("before response=%+v want native OR topK", before)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}
	reopened := openTextV2TestDB(t, dir, false)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	after, err := reopenedCol.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("after reopen OR search: %v", err)
	}
	assertTextSearchParity2627(t, after, before)
	if after.Stats.TextBlockMaxFallbacks != 0 || after.Stats.DocumentsFetched != 0 || after.Stats.TextStateLookups != 0 {
		t.Fatalf("after stats=%+v want native zero-doc OR after reopen", after.Stats)
	}
}

func TestTextV2BlockMaxSingleTermOverlappingMutationFallsBackExact2728(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxFixtureDocs2628(256, 8)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}}}, ids, docs)
	if _, _, err := col.Update([]byte("doc-000042"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"title":"refund refund refund","body":"refund refund"}`), true, nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	opts := TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 5, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4}
	exhaustiveOpts := opts
	exhaustiveOpts.textV2DisableBlockMax = true
	exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("exhaustive update search: %v", err)
	}
	got, err := col.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("block-max update search: %v", err)
	}
	assertTextSearchParity2627(t, got, exhaustive)
	if got.Stats.TextBlockMaxFallbacks == 0 || got.Stats.DocumentsFetched != 0 || got.Stats.TextStateLookups != 0 || got.Stats.FailClosed != 0 {
		t.Fatalf("stats=%+v want exact fallback for overlapping single-term mutation blocks without docs/state/fail", got.Stats)
	}
}

func TestTextV2BlockMaxMultiTermANDOverlappingMutationFallsBackExact2688(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxFixtureDocs2628(256, 8)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}}}, ids, docs)
	if _, _, err := col.Update([]byte("doc-000042"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"title":"refund policy","body":"refund policy"}`), true, nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	opts := TextSearchOptions{IndexName: "lexical", Query: "refund AND policy", Operator: TextSearchOperatorAND, TopK: 5, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4}
	exhaustiveOpts := opts
	exhaustiveOpts.textV2DisableBlockMax = true
	exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("exhaustive update search: %v", err)
	}
	got, err := col.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("block-max update search: %v", err)
	}
	assertTextSearchParity2627(t, got, exhaustive)
	if len(got.Results) == 0 || string(got.Results[0].DocumentID) != "doc-000042" {
		t.Fatalf("results=%+v want updated AND match", got.Results)
	}
	if got.Stats.TextBlockMaxFallbacks == 0 || got.Stats.DocumentsFetched != 0 || got.Stats.TextStateLookups != 0 {
		t.Fatalf("stats=%+v want exact fallback for overlapping mutation blocks without docs/state", got.Stats)
	}
}

func TestTextV2BlockMaxMultiTermOROverlappingMutationFallsBackExact2730(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxFixtureDocs2628(256, 8)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}}}, ids, docs)
	if _, _, err := col.Update([]byte("doc-000042"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"title":"refund policy","body":"policy policy"}`), true, nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	opts := TextSearchOptions{IndexName: "lexical", Query: "refund OR policy", Operator: TextSearchOperatorOR, TopK: 5, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4}
	exhaustiveOpts := opts
	exhaustiveOpts.textV2DisableBlockMax = true
	exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("exhaustive OR update search: %v", err)
	}
	got, err := col.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("block-max OR update search: %v", err)
	}
	assertTextSearchParity2627(t, got, exhaustive)
	if len(got.Results) == 0 || string(got.Results[0].DocumentID) != "doc-000042" {
		t.Fatalf("results=%+v want updated OR match", got.Results)
	}
	if got.Stats.TextBlockMaxFallbacks == 0 || got.Stats.DocumentsFetched != 0 || got.Stats.TextStateLookups != 0 {
		t.Fatalf("stats=%+v want exact fallback for overlapping OR mutation blocks without docs/state", got.Stats)
	}
}

func TestTextV2BlockMaxMultiTermORCandidateLimitIgnoresStaleGeneration2730(t *testing.T) {
	target := uint64(2)
	blockStart := textV2OrdinalBlockStart(target, textV2DefaultNormBlockSize)
	ctx := &textV2SearchContext{
		status:       textV2IndexStatusValue{RootGeneration: 2, NormGeneration: 2, DocMapGeneration: 2, NextOrdinal: 3},
		corpus:       textV2CorpusStatsValue{DocumentCount: 2},
		termStats:    map[string]textV2TermStatsValue{"refund": {DocumentFrequency: 1}, "policy": {DocumentFrequency: 1}},
		fieldStats:   []textV2FieldStatsValue{{DocumentCount: 2, TotalTokenCount: 4}},
		fieldNames:   []string{"body"},
		fieldWeights: []float64{1},
	}
	stalePosting := textV2SearchPostingValue{generation: 1, termFrequency: 1, fieldCount: 1, inlineFields: [4]uint32{1}}
	block := textV2PostingBlockValue{Summary: textV2PostingBlockSummary{FirstOrdinal: target, LastOrdinal: target}}
	states := []*textV2ANDBlockMaxTermState{
		{term: "refund", decoded: true, scanner: &textV2PostingBlockEntryScanner{}, block: block, entries: []textV2ANDBlockMaxPostingEntry{{ordinal: target, value: stalePosting}}},
		{term: "policy", decoded: true, scanner: &textV2PostingBlockEntryScanner{}, block: block, entries: []textV2ANDBlockMaxPostingEntry{{ordinal: target, value: stalePosting}}},
	}
	cache := &textV2SearchBlockCache{
		normBlocks: map[uint64]textV2SearchNormBlock{
			blockStart: {BlockStart: blockStart, BlockSize: textV2DefaultNormBlockSize, FieldCount: 1, Entries: []textV2SearchNormEntry{{Ordinal: target, Generation: 2, FieldLengths: []uint32{2}}}},
		},
		docMapBlocks: map[uint64]textV2SearchDocMapBlock{
			blockStart: {BlockStart: blockStart, BlockSize: textV2DefaultDocMapBlockSize, Entries: []textV2SearchDocMapEntry{{Ordinal: target, Generation: 2, DocumentID: []byte("d2")}}},
		},
	}
	stats := &TextSearchStats{TextCandidatesScored: 1}
	top := &textV2SearchTopK{limit: 1, candidates: []textV2SearchTopCandidate{{ordinal: 1, generation: 1, documentID: []byte("d1"), score: 1}}}

	// Advancement is covered by end-to-end block-max tests; this fixture isolates scoring-limit ordering.
	truncated, err := visitTextV2ORBlockMaxCandidate(nil, nil, ctx, states, nil, cache, nil, 1, 64, false, target, top, stats)
	if err != nil {
		t.Fatalf("visit OR candidate with stale generation: %v", err)
	}
	if truncated || stats.Truncated || stats.FailClosedReason != "" {
		t.Fatalf("truncated=%v stats=%+v want stale generation skipped without candidate-limit fail-closed", truncated, stats)
	}
	if stats.TextCandidatesScored != 1 || len(top.candidates) != 1 || string(top.candidates[0].documentID) != "d1" {
		t.Fatalf("top=%+v stats=%+v want existing candidate retained without scoring stale target", top.candidates, stats)
	}
}

func TestTextV2BlockMaxMultiTermANDTieBreak2688(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("doc-c"), []byte("doc-a"), []byte("doc-b"), []byte("doc-d")}
	docs := [][]byte{
		[]byte(`{"title":"refund policy","body":"refund policy"}`),
		[]byte(`{"title":"refund policy","body":"refund policy"}`),
		[]byte(`{"title":"refund policy","body":"refund policy"}`),
		[]byte(`{"title":"refund policy","body":"refund policy"}`),
	}
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title"}, {Field: "body"}}}, ids, docs)
	opts := TextSearchOptions{IndexName: "lexical", Query: "refund AND policy", Operator: TextSearchOperatorAND, TopK: 3, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4}
	exhaustiveOpts := opts
	exhaustiveOpts.textV2DisableBlockMax = true
	exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("exhaustive tie search: %v", err)
	}
	got, err := col.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("block-max tie search: %v", err)
	}
	assertTextSearchParity2627(t, got, exhaustive)
	want := []string{"doc-a", "doc-b", "doc-c"}
	for i, id := range want {
		if string(got.Results[i].DocumentID) != id {
			t.Fatalf("results=%+v want lexical tie order %v", got.Results, want)
		}
	}
}

func TestTextV2BlockMaxMultiTermANDScalarPrefilter2688(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Indexes: []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	ids, docs := textV2BlockMaxMultiTermScalarDocs2688(384)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}

	got, err := col.SearchHybrid(HybridSearchOptions{TopK: 5, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund AND policy", CandidateLimit: len(ids)}, ScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "tenant-rare"}})
	if err != nil {
		t.Fatalf("SearchHybrid scalar AND: %v", err)
	}
	if len(got.Results) != 5 || got.Stats.FailClosed != 0 || got.Stats.TextBlockMaxFallbacks != 0 || got.Stats.DocumentsFetched != 0 || got.Stats.TextStateLookups != 0 {
		t.Fatalf("response=%+v want scalar-prefiltered native multi-term results", got)
	}
	if got.Stats.ScalarPrefilterIDs != 8 || got.Stats.TextCandidatesScored == 0 || got.Stats.TextCandidatesScored > got.Stats.ScalarPrefilterIDs {
		t.Fatalf("stats=%+v want scoring bounded by scalar allow-set", got.Stats)
	}
	for _, result := range got.Results {
		if string(result.ID) < "doc-000000" || string(result.ID) > "doc-000007" {
			t.Fatalf("result id=%q outside rare tenant allow-set", result.ID)
		}
	}
}

func TestTextV2BlockMaxMultiTermORScalarPrefilter2730(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Indexes: []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	ids, docs := textV2BlockMaxMixedDisjunctionDocs2730(512, 32, 16)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}

	got, err := col.SearchHybrid(HybridSearchOptions{TopK: 6, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund OR policy", CandidateLimit: len(ids)}, ScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "tenant-rare"}})
	if err != nil {
		t.Fatalf("SearchHybrid scalar OR: %v", err)
	}
	if len(got.Results) != 6 || got.Stats.FailClosed != 0 || got.Stats.TextBlockMaxFallbacks != 0 || got.Stats.DocumentsFetched != 0 || got.Stats.TextStateLookups != 0 {
		t.Fatalf("response=%+v want scalar-prefiltered native OR results", got)
	}
	if got.Stats.ScalarPrefilterIDs != 16 || got.Stats.TextCandidatesScored == 0 || got.Stats.TextCandidatesScored > got.Stats.ScalarPrefilterIDs {
		t.Fatalf("stats=%+v want OR scoring bounded by scalar allow-set", got.Stats)
	}
	for _, result := range got.Results {
		if string(result.ID) < "doc-000000" || string(result.ID) > "doc-000015" {
			t.Fatalf("result id=%q outside rare tenant allow-set", result.ID)
		}
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
				{name: "and_blockmax", opts: TextSearchOptions{IndexName: "lexical", Query: "refund AND policy", Operator: TextSearchOperatorAND, TopK: 10, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 8}},
				{name: "or_blockmax_filtered", opts: TextSearchOptions{IndexName: "lexical", Query: "refund OR shipping", TopK: 10, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 8, textV2AllowedDocumentIDs: rareIDs}},
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
					if tc.wantFall {
						if got.Stats.TextBlockMaxFallbacks == 0 {
							t.Fatalf("stats=%+v want exact exhaustive fallback counter", got.Stats)
						}
					} else if got.Stats.TextBlockMaxFallbacks != 0 {
						t.Fatalf("stats=%+v want no exact exhaustive fallback counter", got.Stats)
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
	if rare.Stats.TextCandidatesScored >= broad.Stats.TextCandidatesScored || rare.Stats.TextScalarPostingBlocksSkipped == 0 {
		t.Fatalf("rare stats=%+v broad stats=%+v want rare filter to score fewer candidates and skip scalar-disjoint blocks", rare.Stats, broad.Stats)
	}
}

func TestTextV2HybridUnionFusionPreservesUnfilteredTextRanks2628(t *testing.T) {
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
	ids, docs := textV2HybridUnionFusionDocs2628(384)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	textQuery := HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4}
	unfilteredText, err := col.SearchHybridTextCandidates(textQuery)
	if err != nil {
		t.Fatalf("SearchHybridTextCandidates unfiltered: %v", err)
	}
	unfilteredRank := hybridCandidateSourceRank2628(t, unfilteredText.Candidates, "doc-000002", HybridCandidateSourceText)
	if unfilteredRank != 3 {
		t.Fatalf("unfiltered text candidates=%+v want doc-000002 at source rank 3", unfilteredText.Candidates)
	}

	baseOpts := HybridSearchOptions{
		TopK: 1,
		Text: &textQuery,
		ScalarFilter: &HybridScalarFilter{
			IndexName: "tenant",
			Value:     "tenant-rare",
		},
	}
	unionOpts := baseOpts
	unionOpts.ScalarFilterStrategy = HybridScalarFilterStrategyUnionFusion
	union, err := col.SearchHybrid(unionOpts)
	if err != nil {
		t.Fatalf("SearchHybrid union_fusion scalar: %v", err)
	}
	if union.Plan.ScalarFilterStrategy != HybridScalarFilterStrategyUnionFusion || len(union.Results) != 1 || string(union.Results[0].ID) != "doc-000002" {
		t.Fatalf("union_fusion response=%+v want rare doc with explicit union_fusion", union)
	}
	unionText := hybridResultSourceContribution2628(t, union.Results[0], HybridCandidateSourceText)
	if unionText.SourceRank != unfilteredRank {
		t.Fatalf("union_fusion text source rank=%d want unfiltered rank %d; result=%+v", unionText.SourceRank, unfilteredRank, union.Results[0])
	}
	wantTextFusion := 1 / float64(HybridFusionDefaultRRFK+unfilteredRank)
	if math.Abs(unionText.FusionScore-wantTextFusion) > 1e-12 {
		t.Fatalf("union_fusion text contribution=%+v want fusion score %.12f", unionText, wantTextFusion)
	}
	if math.Abs(union.Results[0].FusedScore-wantTextFusion) > 1e-12 {
		t.Fatalf("union_fusion fused score=%.12f want %.12f from unfiltered text rank result=%+v", union.Results[0].FusedScore, wantTextFusion, union.Results[0])
	}

	prefilter, err := col.SearchHybrid(baseOpts)
	if err != nil {
		t.Fatalf("SearchHybrid default prefilter scalar: %v", err)
	}
	if prefilter.Plan.ScalarFilterStrategy != HybridScalarFilterStrategyPrefilter || len(prefilter.Results) != 1 || string(prefilter.Results[0].ID) != "doc-000002" {
		t.Fatalf("prefilter response=%+v want rare doc with default prefilter", prefilter)
	}
	prefilterText := hybridResultSourceContribution2628(t, prefilter.Results[0], HybridCandidateSourceText)
	if prefilterText.SourceRank != 1 {
		t.Fatalf("prefilter text source rank=%d want rank within scalar-pruned text source; result=%+v", prefilterText.SourceRank, prefilter.Results[0])
	}
	if prefilter.Stats.ScalarPrefilterIDs != 1 || prefilter.Stats.TextPostingBlocksSkipped == 0 || prefilter.Stats.TextCandidatesScored != 1 || prefilter.Stats.TextCandidatesScored >= union.Stats.TextCandidatesScored {
		t.Fatalf("prefilter stats=%+v union stats=%+v want scalar text pruning counters only for prefilter", prefilter.Stats, union.Stats)
	}
}

func TestTextV2BlockMaxCorruptMetadataFailsClosed2628(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxMultiTermDocs2688(160, 8)
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

	for _, query := range []string{"refund", "refund AND policy", "refund OR policy"} {
		got, err := col.searchText(TextSearchOptions{IndexName: "lexical", Query: query, TopK: 5, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 2}, textSearchResultScoreOnly)
		if !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
			t.Fatalf("SearchText %q err=%v want unavailable/storage corrupt", query, err)
		}
		if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != textSearchFailClosedStorageCorrupt || got.Stats.DocumentsFetched != 0 {
			t.Fatalf("query %q stats=%+v want metadata fail-closed without docs", query, got.Stats)
		}
	}
}

func textV2BlockMaxUniformHighDFDocs2873(count int, reverseIDs bool) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		idOrdinal := i
		if reverseIDs {
			idOrdinal = count - 1 - i
		}
		ids[i] = []byte(fmt.Sprintf("doc-%06d", idOrdinal))
		docs[i] = []byte(`{"title":"refund policy","body":"refund policy support common","tenant":"tenant-broad"}`)
	}
	return ids, docs
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

func textV2BlockMaxMultiTermDocs2688(count, highDocs int) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		title := "refund policy"
		body := "refund policy"
		if i < highDocs {
			title = strings.TrimSpace(strings.Repeat("refund policy ", 80))
			body = strings.TrimSpace(strings.Repeat("refund policy ", 160))
		}
		docs[i] = []byte(fmt.Sprintf(`{"title":%q,"body":%q,"tenant":"tenant-broad"}`, title, body))
	}
	return ids, docs
}

func textV2BlockMaxMixedDisjunctionDocs2730(count, highDocs, rareDocs int) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		tenant := "tenant-broad"
		if i < rareDocs {
			tenant = "tenant-rare"
		}
		title := "shipping"
		body := "support common"
		switch i % 4 {
		case 0:
			title = "refund"
			body = "refund support"
		case 1:
			title = "policy"
			body = "policy support"
		case 2:
			title = "support"
			body = "support common"
		}
		if i < highDocs {
			switch i % 4 {
			case 0:
				title = strings.TrimSpace(strings.Repeat("refund ", 64))
				body = strings.TrimSpace(strings.Repeat("refund ", 128))
			case 1:
				title = strings.TrimSpace(strings.Repeat("policy ", 64))
				body = strings.TrimSpace(strings.Repeat("policy ", 128))
			case 2:
				title = "support"
				body = "support common"
			}
		}
		docs[i] = []byte(fmt.Sprintf(`{"title":%q,"body":%q,"tenant":%q}`, title, body, tenant))
	}
	return ids, docs
}

func textV2BlockMaxMultiTermScalarDocs2688(count int) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		tenant := "tenant-broad"
		if i < 8 {
			tenant = "tenant-rare"
		}
		title := "refund policy"
		body := "refund policy support"
		if i < 8 {
			title = strings.TrimSpace(strings.Repeat("refund policy ", 40))
			body = strings.TrimSpace(strings.Repeat("refund policy ", 80))
		}
		docs[i] = []byte(fmt.Sprintf(`{"title":%q,"body":%q,"tenant":%q}`, title, body, tenant))
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

func textV2HybridUnionFusionDocs2628(count int) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		tenant := "tenant-broad"
		if i == 2 {
			tenant = "tenant-rare"
		}
		titleRepeats := 1
		switch i {
		case 0:
			titleRepeats = 48
		case 1:
			titleRepeats = 24
		case 2:
			titleRepeats = 6
		}
		title := strings.TrimSpace(strings.Repeat("refund ", titleRepeats))
		docs[i] = []byte(fmt.Sprintf(`{"title":%q,"tenant":%q}`, title, tenant))
	}
	return ids, docs
}

func hybridCandidateSourceRank2628(tb testing.TB, candidates []HybridSearchCandidate, id string, source HybridCandidateSource) int {
	tb.Helper()
	for _, candidate := range candidates {
		if string(candidate.ID) == id && candidate.Source == source {
			return candidate.SourceRank
		}
	}
	tb.Fatalf("missing %s candidate %q in %+v", source, id, candidates)
	return 0
}

func hybridResultSourceContribution2628(tb testing.TB, result HybridSearchResult, source HybridCandidateSource) HybridSourceContribution {
	tb.Helper()
	for _, contribution := range result.Sources {
		if contribution.Source == source {
			return contribution
		}
	}
	tb.Fatalf("missing %s contribution in result %+v", source, result)
	return HybridSourceContribution{}
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
