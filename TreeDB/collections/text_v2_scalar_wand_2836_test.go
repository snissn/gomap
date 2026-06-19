package collections

import (
	"fmt"
	"testing"
)

func TestTextV2ScalarAwareWANDPruningContract2836(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()

	const docsN = 512
	ids := make([][]byte, docsN)
	docs := make([][]byte, docsN)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		// Keep both terms in every document so scalar pruning, not term rarity, is
		// responsible for reducing candidate work.
		docs[i] = []byte(fmt.Sprintf(`{"title":"refund policy","body":"refund policy support %03d"}`, i))
	}
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}},
	}, ids, docs)

	run := func(t *testing.T, opts TextSearchOptions) (TextSearchResponse, TextSearchResponse) {
		t.Helper()
		exhaustiveOpts := opts
		exhaustiveOpts.textV2DisableBlockMax = true
		exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
		if err != nil {
			t.Fatalf("exhaustive filtered scorer: %v", err)
		}
		got, err := col.searchText(opts, textSearchResultScoreOnly)
		if err != nil {
			t.Fatalf("block-max filtered scorer: %v", err)
		}
		assertTextSearchParity2627(t, got, exhaustive)
		if got.Stats.FailClosed != 0 || got.Stats.DocumentsFetched != 0 || got.Stats.TextStateLookups != 0 || got.Stats.TextBlockMaxFallbacks != 0 {
			t.Fatalf("stats=%+v want native exact scalar-aware block-max without docs/fallback/state lookups", got.Stats)
		}
		return got, exhaustive
	}

	t.Run("high_selectivity_skips_disjoint_blocks", func(t *testing.T) {
		allow := hybridScalarAllowSet{"doc-000000": {}, "doc-000001": {}, "doc-000002": {}, "doc-000003": {}}
		got, _ := run(t, TextSearchOptions{IndexName: "lexical", Query: "refund OR policy", Operator: TextSearchOperatorOR, TopK: 4, CandidateLimit: docsN, MaxPostingsScanned: docsN * 8, textV2AllowedDocumentIDs: allow})
		if got.Stats.TextScalarPrefilterIDs != uint64(len(allow)) || got.Stats.TextScalarPostingBlocksSkipped == 0 {
			t.Fatalf("stats=%+v want high-selectivity allow-set to skip posting blocks", got.Stats)
		}
		if got.Stats.TextCandidatesScored == 0 || got.Stats.TextCandidatesScored > uint64(len(allow)) {
			t.Fatalf("stats=%+v want scored candidates bounded by allow-set size %d", got.Stats, len(allow))
		}
	})

	t.Run("moderate_selectivity_rejects_disallowed_postings", func(t *testing.T) {
		allow := make(hybridScalarAllowSet)
		for i := 0; i < docsN; i += 17 {
			allow[fmt.Sprintf("doc-%06d", i)] = struct{}{}
		}
		got, _ := run(t, TextSearchOptions{IndexName: "lexical", Query: "refund AND policy", Operator: TextSearchOperatorAND, TopK: 8, CandidateLimit: docsN, MaxPostingsScanned: docsN * 8, textV2AllowedDocumentIDs: allow})
		if got.Stats.TextScalarPrefilterIDs != uint64(len(allow)) || got.Stats.TextScalarPostingBlocksSkipped != 0 || got.Stats.TextScalarPostingsRejected == 0 {
			t.Fatalf("stats=%+v want moderate-selectivity allow-set to reject postings without skipping intersecting blocks", got.Stats)
		}
		if got.Stats.TextCandidatesScored == 0 || got.Stats.TextCandidatesScored > uint64(len(allow)) {
			t.Fatalf("stats=%+v want scored candidates bounded by allow-set size %d", got.Stats, len(allow))
		}
	})
}

func BenchmarkTextV2ScalarAwareWANDPruning2836(b *testing.B) {
	d := openTextV2TestDB(b, b.TempDir(), false)
	defer func() { _ = d.Close() }()

	const docsN = 4096
	ids := make([][]byte, docsN)
	docs := make([][]byte, docsN)
	highSelectivity := make(hybridScalarAllowSet)
	moderateSelectivity := make(hybridScalarAllowSet)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		docs[i] = []byte(fmt.Sprintf(`{"title":"refund policy","body":"refund policy support common %04d"}`, i))
		if i < 16 {
			highSelectivity[string(ids[i])] = struct{}{}
		}
		if i%32 == 0 {
			moderateSelectivity[string(ids[i])] = struct{}{}
		}
	}
	col := createTextSearchCollection2627(b, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}}}, ids, docs)

	cases := []struct {
		name string
		opts TextSearchOptions
	}{
		{name: "high_selectivity_or", opts: TextSearchOptions{IndexName: "lexical", Query: "refund OR policy", Operator: TextSearchOperatorOR, TopK: 10, CandidateLimit: docsN, MaxPostingsScanned: docsN * 8, textV2AllowedDocumentIDs: highSelectivity}},
		{name: "moderate_selectivity_and", opts: TextSearchOptions{IndexName: "lexical", Query: "refund AND policy", Operator: TextSearchOperatorAND, TopK: 10, CandidateLimit: docsN, MaxPostingsScanned: docsN * 8, textV2AllowedDocumentIDs: moderateSelectivity}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			var sink TextSearchResponse
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := col.searchText(tc.opts, textSearchResultScoreOnly)
				if err != nil {
					b.Fatalf("searchText: %v", err)
				}
				sink = got
			}
			b.StopTimer()
			b.ReportMetric(float64(sink.Stats.TextScalarPrefilterIDs), "scalar_prefilter_ids/search")
			b.ReportMetric(float64(sink.Stats.TextScalarPostingBlocksSkipped), "scalar_posting_blocks_skipped/search")
			b.ReportMetric(float64(sink.Stats.TextScalarPostingsRejected), "scalar_postings_rejected/search")
			b.ReportMetric(float64(sink.Stats.TextCandidatesScored), "text_candidates_scored/search")
			b.ReportMetric(float64(sink.Stats.TextPostingsScanned), "text_postings_scanned/search")
		})
	}
}
