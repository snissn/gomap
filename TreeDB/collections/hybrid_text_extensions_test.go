package collections

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"

	"github.com/snissn/gomap/TreeDB/internal/memtable"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestSearchHybridIndexStatePostfilterBroadOver4096AndReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	mgr := NewCollectionManager(d)
	meta := &CollectionMeta{
		Name: "comments",
		Options: CollectionOptions{
			DocumentFormat:          DocumentFormatJSON,
			AllowArrayValuesInIndex: true,
		},
		Indexes: []IndexDefinition{
			{Name: "is_bot", Field: "meta.is_bot", ValueType: IndexValueBool},
			{Name: "ext", Field: "meta.ext", ValueType: IndexValueString},
			{Name: "score", Field: "meta.score", ValueType: IndexValueInt64},
			{Name: "labels", Field: "meta.labels", ValueType: IndexValueString},
		},
		TextIndexes: []TextIndexDefinition{{
			Name:           "lexical",
			Fields:         []TextIndexField{{Field: "body"}},
			Analyzer:       TextAnalyzerSimple,
			StorePositions: true,
		}},
	}
	if _, err := mgr.CreateCollection(meta); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("comments")
	if err != nil {
		t.Fatal(err)
	}

	// Insert 4500 non-bot docs (> hybridScalarDefaultLookupLimit = 4096) so Prefilter fails closed,
	// while IndexStatePostfilter serves exact filtered hits with zero primary document scans.
	const total = 4500
	ids := make([][]byte, 0, 500)
	docs := make([][]byte, 0, 500)
	flush := func() {
		if len(ids) == 0 {
			return
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			t.Fatal(err)
		}
		ids = ids[:0]
		docs = docs[:0]
	}
	for i := 0; i < total; i++ {
		body := "ordinary discussion comment"
		isBot := false
		ext := "md"
		labels := `["general"]`
		if i == 100 {
			body = "bohmian elastic electron carrier phase"
			isBot = true // should be excluded by is_bot == false
			labels = `["physics","bot"]`
		} else if i == 200 {
			body = "bohmian elastic electron carrier phase"
			isBot = false
			labels = `["physics","bell"]`
		} else if i == 300 {
			body = "bohmian elastic electron carrier phase"
			isBot = false
			ext = "py"
			labels = `["physics","code"]`
		}
		ids = append(ids, []byte(fmt.Sprintf("doc-%05d#0", i)))
		docs = append(docs, []byte(fmt.Sprintf(
			`{"body":%q,"meta":{"is_bot":%t,"ext":%q,"score":%d,"labels":%s}}`,
			body, isBot, ext, i, labels,
		)))
		if len(ids) == 500 {
			flush()
		}
	}
	flush()

	// Confirm Prefilter fails closed on broad is_bot == false (>4096 matches).
	_, preErr := col.SearchHybrid(HybridSearchOptions{
		TopK: 5,
		Text: &HybridTextQuery{IndexName: "lexical", Query: "bohmian elastic electron", CandidateLimit: 32},
		ScalarFilter: &HybridScalarFilter{
			IndexName: "is_bot",
			Value:     false,
		},
		ScalarFilterStrategy: HybridScalarFilterStrategyPrefilter,
	})
	if preErr == nil {
		t.Fatalf("expected Prefilter to fail closed on 4499 matches > 4096 limit")
	}

	checkQueries := func(c *Collection) {
		// 1. Broad single equality (is_bot == false, ~4499 matches)
		resp, err := c.SearchHybrid(HybridSearchOptions{
			TopK: 5,
			Text: &HybridTextQuery{IndexName: "lexical", Query: "bohmian elastic electron", CandidateLimit: 32},
			ScalarFilter: &HybridScalarFilter{
				IndexName: "is_bot",
				Value:     false,
			},
			ScalarFilterStrategy: HybridScalarFilterStrategyIndexStatePostfilter,
			ResultMode:           HybridResultModeCompact,
		})
		if err != nil {
			t.Fatalf("IndexStatePostfilter single leaf: %v", err)
		}
		if len(resp.Results) != 2 {
			t.Fatalf("got %d results want 2: %+v", len(resp.Results), resp.Results)
		}
		if resp.Stats.DocumentsFetched != 0 || resp.Stats.FullDocumentScanFallbacks != 0 {
			t.Fatalf("must not fetch primary docs in compact mode: %+v", resp.Stats)
		}
		if resp.Stats.ScalarPostfilterChecks != 3 || resp.Stats.ScalarFilterMatched != 2 || resp.Stats.ScalarFilterRejected != 1 {
			t.Fatalf("unexpected postfilter counters: %+v", resp.Stats)
		}

		// 2. Multi-field AND with equality + range + multikey array
		resp2, err := c.SearchHybrid(HybridSearchOptions{
			TopK: 5,
			Text: &HybridTextQuery{IndexName: "lexical", Query: "bohmian elastic electron", CandidateLimit: 32},
			ScalarFilter: &HybridScalarFilter{
				And: []HybridScalarFilter{
					{IndexName: "is_bot", Value: false},
					{IndexName: "ext", Value: "md"},
					{IndexName: "labels", Value: "bell"},
					{IndexName: "score", Range: &IndexRangeOptions{
						Lower: IndexRangeBound{Value: int64(150), Inclusive: true},
						Upper: IndexRangeBound{Value: int64(250), Inclusive: true},
					}},
				},
			},
			ScalarFilterStrategy: HybridScalarFilterStrategyIndexStatePostfilter,
			IncludeDocuments:     true,
		})
		if err != nil {
			t.Fatalf("IndexStatePostfilter multi-field AND: %v", err)
		}
		if len(resp2.Results) != 1 || string(resp2.Results[0].ID) != "doc-00200#0" || !resp2.Results[0].DocumentFound {
			t.Fatalf("multi-field AND got %+v want doc-00200#0", resp2.Results)
		}

		// 3. Phrase query inside HybridTextQuery combined with IndexStatePostfilter
		phraseResp, err := c.SearchHybrid(HybridSearchOptions{
			TopK: 5,
			Text: &HybridTextQuery{
				IndexName:      "lexical",
				Phrase:         &TextSearchPhraseQuery{Query: "elastic electron carrier", Slop: 0},
				CandidateLimit: 32,
			},
			ScalarFilter: &HybridScalarFilter{
				IndexName: "is_bot",
				Value:     false,
			},
			ScalarFilterStrategy: HybridScalarFilterStrategyIndexStatePostfilter,
			MaxChunksPerParent:   1,
		})
		if err != nil {
			t.Fatalf("Hybrid phrase + IndexStatePostfilter: %v", err)
		}
		if len(phraseResp.Results) != 2 {
			t.Fatalf("phrase got %d results want 2: %+v", len(phraseResp.Results), phraseResp.Results)
		}

		// 4. Undeclared scalar index fails closed even before text scan
		if _, err := c.SearchHybrid(HybridSearchOptions{
			TopK:                 5,
			Text:                 &HybridTextQuery{IndexName: "lexical", Query: "bohmian", CandidateLimit: 8},
			ScalarFilter:         &HybridScalarFilter{IndexName: "undeclared_idx", Value: "x"},
			ScalarFilterStrategy: HybridScalarFilterStrategyIndexStatePostfilter,
		}); err == nil {
			t.Fatal("expected undeclared index to fail closed")
		}
	}

	checkQueries(col)

	// Reopen and verify persistence parity.
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d2, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()
	col2, err := NewCollectionManager(d2).OpenCollection("comments")
	if err != nil {
		t.Fatal(err)
	}
	checkQueries(col2)
}

func BenchmarkSearchHybridIndexStatePostfilter(b *testing.B) {
	dir := b.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "bench_comments",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		Indexes: []IndexDefinition{
			{Name: "is_bot", Field: "meta.is_bot", ValueType: IndexValueBool},
			{Name: "ext", Field: "meta.ext", ValueType: IndexValueString},
		},
		TextIndexes: []TextIndexDefinition{{
			Name:           "lexical",
			Fields:         []TextIndexField{{Field: "body"}},
			Analyzer:       TextAnalyzerSimple,
			StorePositions: true,
		}},
	}); err != nil {
		b.Fatal(err)
	}
	col, err := mgr.OpenCollection("bench_comments")
	if err != nil {
		b.Fatal(err)
	}
	ids := make([][]byte, 512)
	docs := make([][]byte, 512)
	for i := range ids {
		body := "ordinary comment text"
		if i%16 == 0 {
			body = "bohmian elastic electron carrier phase"
		}
		ids[i] = []byte(fmt.Sprintf("doc-%04d#0", i))
		docs[i] = []byte(fmt.Sprintf(`{"body":%q,"meta":{"is_bot":%t,"ext":"md"}}`, body, i%32 == 0))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		b.Fatal(err)
	}
	opts := HybridSearchOptions{
		TopK: 10,
		Text: &HybridTextQuery{IndexName: "lexical", Query: "bohmian elastic electron", CandidateLimit: 32},
		ScalarFilter: &HybridScalarFilter{
			And: []HybridScalarFilter{
				{IndexName: "is_bot", Value: false},
				{IndexName: "ext", Value: "md"},
			},
		},
		ScalarFilterStrategy: HybridScalarFilterStrategyIndexStatePostfilter,
		ResultMode:           HybridResultModeCompact,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		resp, err := col.SearchHybrid(opts)
		if err != nil || len(resp.Results) != 10 || resp.Stats.DocumentsFetched != 0 || resp.Stats.FullDocumentScanFallbacks != 0 {
			b.Fatalf("resp=%+v err=%v", resp, err)
		}
	}
}

func TestNativeHybridIndexStatePostfilterAdmitsCompiledBounds(t *testing.T) {
	dir := t.TempDir()
	owner, manager, c := openNativeTextPublic(t, dir, treedb.ProfileCommandWALRelaxed, true, TextIndexVersionV2)
	defer func() {
		_ = manager.Close()
		_ = owner.Close()
	}()
	if _, err := c.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"tenant":"a","content":"alpha beta","embedding":[1,0]}`),
			[]byte(`{"tenant":"b","content":"alpha beta","embedding":[0,1]}`),
			[]byte(`{"tenant":"c","content":"gamma","embedding":[1,0]}`),
		},
	); err != nil {
		t.Fatal(err)
	}

	// Verify normal index_state_postfilter works on native COW collections for
	// both equality and range filters without primary document scans.
	eqOpts := HybridSearchOptions{
		TopK: 4,
		Text: &HybridTextQuery{
			IndexName:      "content",
			Query:          "alpha",
			CandidateLimit: 8,
		},
		ScalarFilter:         &HybridScalarFilter{IndexName: "tenant", Value: "a"},
		ScalarFilterStrategy: HybridScalarFilterStrategyIndexStatePostfilter,
	}
	res, err := c.SearchHybrid(eqOpts)
	if err != nil {
		t.Fatalf("native index_state_postfilter equality failed: %v", err)
	}
	if len(res.Results) != 1 || string(res.Results[0].ID) != "a" {
		t.Fatalf("unexpected equality results: %+v", res.Results)
	}
	if res.Stats.FullDocumentScanFallbacks != 0 || res.Stats.DocumentsFetched != 0 {
		t.Fatalf("expected zero doc scans/fetches: %+v", res.Stats)
	}

	rangeOpts := eqOpts
	rangeOpts.ScalarFilter = &HybridScalarFilter{
		IndexName: "tenant",
		Range: &IndexRangeOptions{
			Lower: IndexRangeBound{Value: "a", Inclusive: true},
			Upper: IndexRangeBound{Value: "b", Inclusive: true},
		},
	}
	resRange, err := c.SearchHybrid(rangeOpts)
	if err != nil {
		t.Fatalf("native index_state_postfilter range failed: %v", err)
	}
	if len(resRange.Results) != 2 {
		t.Fatalf("expected 2 range results, got %d", len(resRange.Results))
	}

	// Verify oversized caller-supplied string bounds exceeding MaxInFlightBytes
	// are rejected before allocating encoded buffers and leak zero budget debt.
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	budget := read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	maxBytes := budget.Limits().MaxInFlightBytes
	read.Close()

	oversized := strings.Repeat("x", int(maxBytes)+1024)
	for name, filter := range map[string]*HybridScalarFilter{
		"equality": {IndexName: "tenant", Value: oversized},
		"range_lower": {
			IndexName: "tenant",
			Range: &IndexRangeOptions{
				Lower: IndexRangeBound{Value: oversized, Inclusive: true},
				Upper: IndexRangeBound{Unbounded: true},
			},
		},
		"range_upper": {
			IndexName: "tenant",
			Range: &IndexRangeOptions{
				Lower: IndexRangeBound{Unbounded: true},
				Upper: IndexRangeBound{Value: oversized, Inclusive: true},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			before := budget.Stats()
			badOpts := eqOpts
			badOpts.ScalarFilter = filter
			failed, err := c.SearchHybrid(badOpts)
			after := budget.Stats()
			if !errors.Is(err, memtable.ErrCOWCapacity) {
				t.Fatalf("expected ErrCOWCapacity for oversized %s bound, got err=%v", name, err)
			}
			if len(failed.Results) != 0 || after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases {
				t.Fatalf("budget leaked on %s refusal: before=%+v after=%+v", name, before, after)
			}
		})
	}
}
