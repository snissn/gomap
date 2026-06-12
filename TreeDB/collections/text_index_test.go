package collections

import (
	"bytes"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTextAnalyzerSimpleFixtures(t *testing.T) {
	tests := []struct {
		name  string
		input string
		terms []string
	}{
		{name: "punctuation lowercase", input: "Hello, WORLD!", terms: []string{"hello", "world"}},
		{name: "unicode and numbers", input: "Café42 東京 123", terms: []string{"café42", "東京", "123"}},
		{name: "code-ish underscore", input: "HTTP_500 retry-count model.v1", terms: []string{"http_500", "retry", "count", "model", "v1"}},
		{name: "repeated terms kept", input: "error.error ERROR", terms: []string{"error", "error", "error"}},
		{name: "empty", input: " -- \t\n ", terms: []string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tokens, err := AnalyzeText(TextAnalyzerSimple, tc.input)
			if err != nil {
				t.Fatalf("AnalyzeText: %v", err)
			}
			terms := make([]string, len(tokens))
			for i, token := range tokens {
				terms[i] = token.Term
				if token.Position != i {
					t.Fatalf("token %d position=%d want %d", i, token.Position, i)
				}
				if token.StartOffset < 0 || token.EndOffset < token.StartOffset || token.EndOffset > len(tc.input) {
					t.Fatalf("token %d invalid offsets %+v for input len %d", i, token, len(tc.input))
				}
			}
			if !slices.Equal(terms, tc.terms) {
				t.Fatalf("terms=%q want %q", terms, tc.terms)
			}
		})
	}
}

func TestCollectionTextIndexMetadataValidateAndReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name: "lexical",
			Fields: []TextIndexField{
				{Field: "body"},
				{Field: "title", Weight: 2.5},
			},
			StoragePolicy: RootStorageCompressed,
		}},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	assertTextIndexMetadataNormalized(t, *meta)
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	assertTextIndexMetadataNormalized(t, col.Meta())
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	assertTextIndexMetadataNormalized(t, reopenedCol.Meta())
}

func assertTextIndexMetadataNormalized(t *testing.T, meta CollectionMeta) {
	t.Helper()
	if len(meta.TextIndexes) != 1 {
		t.Fatalf("text indexes=%d want 1: %+v", len(meta.TextIndexes), meta.TextIndexes)
	}
	idx := meta.TextIndexes[0]
	if idx.Name != "lexical" || idx.Analyzer != TextAnalyzerSimple || idx.Version != TextIndexVersionV1 || idx.Rollout != TextIndexRolloutPrimary || idx.StoragePolicy != RootStorageCompressed {
		t.Fatalf("text index metadata=%+v", idx)
	}
	if len(idx.Fields) != 2 {
		t.Fatalf("fields=%d want 2", len(idx.Fields))
	}
	if idx.Fields[0] != (TextIndexField{Field: "body", Weight: 1}) {
		t.Fatalf("field[0]=%+v want body weight 1", idx.Fields[0])
	}
	if idx.Fields[1] != (TextIndexField{Field: "title", Weight: 2.5}) {
		t.Fatalf("field[1]=%+v want title weight 2.5", idx.Fields[1])
	}
	if !meta.Options.BufferedIndexedWrites {
		t.Fatal("text-indexed metadata should normalize as an indexed schema")
	}
}

func TestCollectionTextIndexMetadataRejectsInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name string
		meta CollectionMeta
		want string
	}{
		{
			name: "missing name",
			meta: CollectionMeta{Name: "docs", TextIndexes: []TextIndexDefinition{{Fields: []TextIndexField{{Field: "body"}}}}},
			want: "name is required",
		},
		{
			name: "missing fields",
			meta: CollectionMeta{Name: "docs", TextIndexes: []TextIndexDefinition{{Name: "text"}}},
			want: "fields are required",
		},
		{
			name: "bad analyzer",
			meta: CollectionMeta{Name: "docs", TextIndexes: []TextIndexDefinition{{Name: "text", Analyzer: TextAnalyzer("english"), Fields: []TextIndexField{{Field: "body"}}}}},
			want: "unsupported analyzer",
		},
		{
			name: "duplicate field",
			meta: CollectionMeta{Name: "docs", TextIndexes: []TextIndexDefinition{{Name: "text", Fields: []TextIndexField{{Field: "body"}, {Field: "body"}}}}},
			want: "duplicate field",
		},
		{
			name: "offsets require positions",
			meta: CollectionMeta{Name: "docs", TextIndexes: []TextIndexDefinition{{Name: "text", StoreOffsets: true, Fields: []TextIndexField{{Field: "body"}}}}},
			want: "store_offsets requires store_positions",
		},
		{
			name: "v2 version reserved fail closed",
			meta: CollectionMeta{Name: "docs", TextIndexes: []TextIndexDefinition{{Name: "text", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}}},
			want: "text index version \"v2\" is reserved",
		},
		{
			name: "shadow rollout reserved fail closed",
			meta: CollectionMeta{Name: "docs", TextIndexes: []TextIndexDefinition{{Name: "text", Rollout: TextIndexRolloutShadow, Fields: []TextIndexField{{Field: "body"}}}}},
			want: "text index rollout mode \"shadow\" is reserved",
		},
		{
			name: "negative weight",
			meta: CollectionMeta{Name: "docs", TextIndexes: []TextIndexDefinition{{Name: "text", Fields: []TextIndexField{{Field: "body", Weight: -1}}}}},
			want: "weight must be finite",
		},
		{
			name: "nan weight",
			meta: CollectionMeta{Name: "docs", TextIndexes: []TextIndexDefinition{{Name: "text", Fields: []TextIndexField{{Field: "body", Weight: math.NaN()}}}}},
			want: "weight must be finite",
		},
		{
			name: "duplicate scalar name",
			meta: CollectionMeta{
				Name:    "docs",
				Indexes: []IndexDefinition{{Name: "text", Field: "kind", ValueType: IndexValueString}},
				TextIndexes: []TextIndexDefinition{{
					Name:   "text",
					Fields: []TextIndexField{{Field: "body"}},
				}},
			},
			want: "duplicate index",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := normalizeCollectionMeta(tc.meta)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("normalizeCollectionMeta err=%v want containing %q", err, tc.want)
			}
		})
	}
}

func TestTextSearchQueryParserRejectsUnsupportedShapes(t *testing.T) {
	for _, query := range []string{"refund AND policy OR error", "refund AND", "refund AND --", "\"refund policy\""} {
		if _, _, err := parseTextSearchQuery(TextAnalyzerSimple, query, TextSearchOperatorOR); err == nil {
			t.Fatalf("parseTextSearchQuery(%q) err=nil want unsupported/malformed", query)
		}
	}
}

func TestCollectionTextRootNames(t *testing.T) {
	meta, err := normalizeCollectionMeta(CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:   "lexical",
			Fields: []TextIndexField{{Field: "body"}},
		}},
	})
	if err != nil {
		t.Fatalf("normalize metadata: %v", err)
	}
	roots := collectionRootNames(meta)
	for _, want := range []string{
		collectionPrimaryRootName("docs"),
		collectionTextIndexRootName("docs", "lexical"),
		collectionTextStateRootName("docs", "lexical"),
		collectionTextStatsRootName("docs", "lexical"),
	} {
		if !slices.Contains(roots, want) {
			t.Fatalf("roots=%q missing %q", roots, want)
		}
	}
	if slices.Contains(roots, collectionSecondaryRootName("docs", "lexical")) || slices.Contains(roots, collectionVectorIndexRootName("docs", "lexical")) {
		t.Fatalf("text root set included scalar/vector root: %q", roots)
	}
}

func TestCollectionTextV2RootNamesAndStatusContract2623(t *testing.T) {
	meta, err := normalizeCollectionMeta(CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:   "lexical",
			Fields: []TextIndexField{{Field: "body"}},
		}},
	})
	if err != nil {
		t.Fatalf("normalize metadata: %v", err)
	}
	status := textIndexStatusForDefinition(meta.Name, meta.TextIndexes[0])
	if status.Version != TextIndexVersionV1 || status.Rollout != TextIndexRolloutPrimary || !status.Ready || !status.Readable || !status.Writable || status.FailClosed {
		t.Fatalf("status=%+v want active v1 primary", status)
	}
	wantActive := collectionTextRootNames("docs", "lexical")
	if !slices.Equal(status.ActiveRootNames, wantActive) {
		t.Fatalf("active roots=%q want %q", status.ActiveRootNames, wantActive)
	}
	wantV2 := []string{
		"docs/text-v2-docid/lexical",
		"docs/text-v2-docmap/lexical",
		"docs/text-v2-terms/lexical",
		"docs/text-v2-posting-blocks/lexical",
		"docs/text-v2-norm-blocks/lexical",
		"docs/text-v2-positions/lexical",
		"docs/text-v2-generations/lexical",
	}
	if !slices.Equal(status.ReservedV2RootNames, wantV2) {
		t.Fatalf("v2 roots=%q want %q", status.ReservedV2RootNames, wantV2)
	}
	for _, want := range []string{"postings_scanned", "posting_blocks_visited", "state_lookups", "norm_lookups", "docs_fetched", "match_details_built", "scalar_filter_selectivity", "fail_closed", "write_amplification", "index_bytes_per_doc", "rewrite_merge_state"} {
		if !slices.Contains(status.RequiredCounterNames, want) {
			t.Fatalf("required counters=%q missing %q", status.RequiredCounterNames, want)
		}
	}
	unsupported := textIndexStatusForDefinition("docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Rollout: TextIndexRolloutPrimary})
	if !unsupported.FailClosed || unsupported.Ready || unsupported.Readable || unsupported.Writable || unsupported.FailClosedReason != "text_index_version_unavailable" {
		t.Fatalf("unsupported status=%+v want fail-closed v2", unsupported)
	}
}

func TestCollectionTextIndexStatusAPI2623(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:        "docs",
		TextIndexes: []TextIndexDefinition{{Name: "lexical", Fields: []TextIndexField{{Field: "body"}}}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	status, err := col.TextIndexStatus("lexical")
	if err != nil {
		t.Fatalf("TextIndexStatus: %v", err)
	}
	if !status.Ready || status.Version != TextIndexVersionV1 || status.PhysicalReclamationPath != TextIndexPhysicalReclamationTreeDB {
		t.Fatalf("status=%+v want active v1 TreeDB reclamation", status)
	}
	if _, err := col.TextIndexStatus("missing"); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("missing TextIndexStatus err=%v want ErrIndexNotFound", err)
	}
}

func TestCollectionTextRootStoragePolicies(t *testing.T) {
	meta, err := normalizeCollectionMeta(CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			IndexStateStoragePolicy: RootStorageFast,
		},
		TextIndexes: []TextIndexDefinition{{
			Name:          "lexical",
			Fields:        []TextIndexField{{Field: "body"}},
			StoragePolicy: RootStorageCompressed,
		}},
	})
	if err != nil {
		t.Fatalf("normalize metadata: %v", err)
	}
	cases := []struct {
		name string
		root string
		want backenddb.OrderedRootStoragePolicy
	}{
		{
			name: "postings honor text index storage policy",
			root: collectionTextIndexRootName("docs", "lexical"),
			want: backenddb.OrderedRootStorageValueLogLeaves,
		},
		{
			name: "state uses index state storage policy",
			root: collectionTextStateRootName("docs", "lexical"),
			want: backenddb.OrderedRootStoragePagerLeaves,
		},
		{
			name: "stats uses index state storage policy",
			root: collectionTextStatsRootName("docs", "lexical"),
			want: backenddb.OrderedRootStoragePagerLeaves,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := collectionRootStoragePolicyForDB(nil, meta, tc.root)
			if err != nil {
				t.Fatalf("collectionRootStoragePolicyForDB(%q): %v", tc.root, err)
			}
			if got != tc.want {
				t.Fatalf("policy=%v want %v", got, tc.want)
			}
		})
	}
	if _, err := collectionRootStoragePolicyForDB(nil, meta, collectionTextIndexRootName("docs", "missing")); err == nil || !strings.Contains(err.Error(), "unknown collection root") {
		t.Fatalf("missing text root err=%v want unknown collection root", err)
	}
}

func TestCollectionTextIndexedWritesMaintainStorageAndSearchRanks(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:   "lexical",
			Fields: []TextIndexField{{Field: "body"}},
		}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("d1")}, [][]byte{[]byte(`{"body":"refund policy"}`)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	storage, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
	if storage.Documents != 1 || storage.StateEntries != 1 || storage.PostingEntries != 2 {
		t.Fatalf("storage stats after maintained insert=%+v want docs=1 state=1 postings=2", storage)
	}

	response, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund AND policy", TopK: 10})
	if err != nil {
		t.Fatalf("SearchText: %v", err)
	}
	if len(response.Results) != 1 || string(response.Results[0].DocumentID) != "d1" || response.Results[0].Rank != 1 || response.Results[0].Score <= 0 {
		t.Fatalf("SearchText results=%+v want ranked d1", response.Results)
	}
	if response.Stats.QueryTerms != 2 || response.Stats.TextPostingsScanned != 2 || response.Stats.TextCandidatesScored != 1 || response.Stats.TextStateLookups != 1 || response.Stats.TextNormLookups != 1 || response.Stats.TextMatchDetailsBuilt != 1 || response.Stats.DocumentsFetched != 0 || response.Stats.FailClosed != 0 {
		t.Fatalf("SearchText stats=%+v want indexed search counters", response.Stats)
	}
	if _, err := col.SearchText(TextSearchOptions{IndexName: "missing", Query: "refund", TopK: 10}); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("SearchText missing err=%v want ErrIndexNotFound", err)
	}
	response, err = col.SearchText(TextSearchOptions{IndexName: "lexical", Query: " -- ", TopK: 10})
	if err != nil {
		t.Fatalf("SearchText empty analyzed query: %v", err)
	}
	if len(response.Results) != 0 || response.Stats.QueryTerms != 0 || response.Stats.DocumentsFetched != 0 {
		t.Fatalf("empty query response=%+v", response)
	}
}

func BenchmarkSimpleTextAnalyzer(b *testing.B) {
	cases := []struct {
		name string
		text []byte
	}{
		{name: "short", text: []byte("HTTP_500 refund policy model.v1 retry-count")},
		{name: "long", text: bytes.Repeat([]byte("User prompt failed refund policy HTTP_500 tool_call model.v1 東京 Café42. "), 64)},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			text := string(tc.text)
			b.ReportAllocs()
			b.SetBytes(int64(len(text)))
			for i := 0; i < b.N; i++ {
				tokens, err := AnalyzeText(TextAnalyzerSimple, text)
				if err != nil {
					b.Fatalf("AnalyzeText: %v", err)
				}
				if len(text) > 0 && len(tokens) == 0 {
					b.Fatal("expected tokens")
				}
			}
		})
	}
}
