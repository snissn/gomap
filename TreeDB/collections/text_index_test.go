package collections

import (
	"bytes"
	"errors"
	"fmt"
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

func TestRetriableTextIndexMutationErrorClassifiesOnlyExpectedConflicts(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "general concurrent mutation", err: ErrConcurrentMutation, want: true},
		{name: "ordered root conflict", err: errors.New("concurrent modification detected during ordered root group publish"), want: true},
		{name: "schema conflict", err: fmt.Errorf(`%w detected for "volatile"`, errConcurrentSchemaModification), want: true},
		{name: "wrapped schema conflict", err: fmt.Errorf("outer: %w", fmt.Errorf(`%w detected for "volatile"`, errConcurrentSchemaModification)), want: true},
		{name: "accepted root conflict", err: acceptedTextIndexCommitErrorForTest{error: errors.New("durable-root candidate base changed: injected")}, want: false},
		{name: "plain schema text", err: errors.New(`collections: concurrent schema modification detected for "volatile"`), want: false},
		{name: "root conflict", err: errors.New(`collections: concurrent root modification detected for "volatile/primary"`), want: false},
		{name: "unrelated", err: errors.New("boom"), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetriableTextIndexMutationError(tc.err); got != tc.want {
				t.Fatalf("isRetriableTextIndexMutationError(%v)=%t want %t", tc.err, got, tc.want)
			}
		})
	}
}

type acceptedTextIndexCommitErrorForTest struct {
	error
}

func (acceptedTextIndexCommitErrorForTest) CommitPublicationAccepted() bool {
	return true
}

func (e acceptedTextIndexCommitErrorForTest) Unwrap() error {
	return e.error
}

func TestTextIndexAcceptedPublicationReconcilesWithoutLogicalReplay(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	def := TextIndexDefinition{
		Name:    "volatile",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "body"}},
	}
	acceptedCause := errors.New("accepted root conflict")
	acceptedErr := acceptedTextIndexCommitErrorForTest{error: fmt.Errorf("durable-root candidate base changed: %w", acceptedCause)}

	createCalls := 0
	created, _, err := col.createTextIndexWithRetry(def, func(got TextIndexDefinition) (*CollectionMeta, TextIndexBackfillStats, error) {
		createCalls++
		meta, stats, createErr := col.createTextIndexOnce(got)
		if createErr != nil {
			return meta, stats, createErr
		}
		return meta, stats, acceptedErr
	})
	if err != nil {
		t.Fatalf("reconcile accepted create: %v", err)
	}
	if createCalls != 1 {
		t.Fatalf("create attempts=%d want 1 logical publication", createCalls)
	}
	gotDef, ok := findTextIndex(created.TextIndexes, def.Name)
	if !ok || gotDef.Version != TextIndexVersionV2 {
		t.Fatalf("created text index=%+v found=%t", gotDef, ok)
	}
	if _, _, err := col.CreateTextIndex(def); err == nil || !strings.Contains(err.Error(), "duplicate index") {
		t.Fatalf("ordinary duplicate create err=%v want duplicate index", err)
	}
	mismatched := created.copy()
	for i := range mismatched.TextIndexes {
		if mismatched.TextIndexes[i].Name == def.Name {
			mismatched.TextIndexes[i].StorePositions = true
		}
	}
	if _, _, err := col.reconcileAcceptedTextIndexCreate(def.Name, mismatched, TextIndexBackfillStats{}, acceptedErr); !errors.Is(err, acceptedCause) || !strings.Contains(err.Error(), "did not reconcile exact index") {
		t.Fatalf("mismatched accepted create err=%v want original conflict plus exact-state diagnostic", err)
	}
	if _, err := col.reconcileAcceptedTextIndexDrop(def.Name, acceptedErr); !errors.Is(err, acceptedCause) || !strings.Contains(err.Error(), "left index") {
		t.Fatalf("mismatched accepted drop err=%v want original conflict plus presence diagnostic", err)
	}

	dropCalls := 0
	dropped, err := col.dropTextIndexWithRetry(def.Name, func(name string) (*CollectionMeta, error) {
		dropCalls++
		meta, dropErr := col.dropTextIndexOnce(name)
		if dropErr != nil {
			return meta, dropErr
		}
		return meta, acceptedErr
	})
	if err != nil {
		t.Fatalf("reconcile accepted drop: %v", err)
	}
	if dropCalls != 1 {
		t.Fatalf("drop attempts=%d want 1 logical publication", dropCalls)
	}
	if _, ok := findTextIndex(dropped.TextIndexes, def.Name); ok {
		t.Fatalf("dropped metadata still contains %q", def.Name)
	}
	if _, err := col.DropTextIndex(def.Name); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("ordinary missing drop err=%v want ErrIndexNotFound", err)
	}

	terminalCause := errors.New("injected storage failure")
	terminalErr := acceptedTextIndexCommitErrorForTest{error: terminalCause}
	terminalCalls := 0
	if _, _, err := col.createTextIndexWithRetry(def, func(TextIndexDefinition) (*CollectionMeta, TextIndexBackfillStats, error) {
		terminalCalls++
		return nil, TextIndexBackfillStats{}, terminalErr
	}); !errors.Is(err, terminalCause) {
		t.Fatalf("non-conflict accepted create err=%v want original storage failure", err)
	}
	if terminalCalls != 1 {
		t.Fatalf("non-conflict accepted create attempts=%d want no logical replay", terminalCalls)
	}
	if _, ok := findTextIndex(col.meta.TextIndexes, def.Name); ok {
		t.Fatalf("non-conflict accepted error unexpectedly recreated %q", def.Name)
	}
}

func TestCollectionMutationRetryDoesNotReplayAcceptedPublication(t *testing.T) {
	cause := errors.New("accepted root conflict")
	wantErr := acceptedTextIndexCommitErrorForTest{error: fmt.Errorf("durable-root candidate base changed: %w", cause)}
	attempts := 0
	_, err := retryInsertBatchMutation(func() ([][]byte, error) {
		attempts++
		return nil, wantErr
	})
	if !errors.Is(err, cause) {
		t.Fatalf("retryInsertBatchMutation err=%v want accepted conflict", err)
	}
	if attempts != 1 {
		t.Fatalf("retryInsertBatchMutation attempts=%d want no logical replay", attempts)
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
			Name:    "lexical",
			Version: TextIndexVersionV1,
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
			name: "bad version",
			meta: CollectionMeta{Name: "docs", TextIndexes: []TextIndexDefinition{{Name: "text", Version: TextIndexVersion("v3"), Fields: []TextIndexField{{Field: "body"}}}}},
			want: "unsupported text index version",
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
			Name:    "lexical",
			Version: TextIndexVersionV1,
			Fields:  []TextIndexField{{Field: "body"}},
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
			Name:    "lexical",
			Version: TextIndexVersionV1,
			Fields:  []TextIndexField{{Field: "body"}},
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
	v2Status := textIndexStatusForDefinition("docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Rollout: TextIndexRolloutPrimary})
	if v2Status.FailClosed || !v2Status.Ready || !v2Status.Readable || !v2Status.Writable || v2Status.FailClosedReason != "" {
		t.Fatalf("v2 status=%+v want root-ready/readable/writable v2", v2Status)
	}
	if !slices.Equal(v2Status.ActiveRootNames, wantV2) {
		t.Fatalf("v2 active roots=%q want %q", v2Status.ActiveRootNames, wantV2)
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
		TextIndexes: []TextIndexDefinition{{Name: "lexical", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}}},
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

func TestCollectionTextIndexStatusUsesCurrentCatalog2623(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	staleCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection stale: %v", err)
	}
	freshCol, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection fresh: %v", err)
	}
	if _, err := staleCol.TextIndexStatus("lexical"); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("pre-create TextIndexStatus err=%v want ErrIndexNotFound", err)
	}
	if _, _, err := freshCol.CreateTextIndex(TextIndexDefinition{Name: "lexical", Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	status, err := staleCol.TextIndexStatus("lexical")
	if err != nil {
		t.Fatalf("stale handle TextIndexStatus after create: %v", err)
	}
	if !status.Ready || status.Name != "lexical" || status.Version != TextIndexVersionV2 || len(status.ActiveRootNames) != len(collectionTextV2RootNames("docs", "lexical")) {
		t.Fatalf("status after create=%+v want current ready default-v2 index", status)
	}
	if _, err := freshCol.DropTextIndex("lexical"); err != nil {
		t.Fatalf("DropTextIndex: %v", err)
	}
	if _, err := staleCol.TextIndexStatus("lexical"); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("stale handle TextIndexStatus after drop err=%v want ErrIndexNotFound", err)
	}
}

func TestCollectionTextRootStoragePolicies(t *testing.T) {
	metaV1, err := normalizeCollectionMeta(CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			IndexStateStoragePolicy: RootStorageFast,
		},
		TextIndexes: []TextIndexDefinition{{
			Name:          "lexical",
			Version:       TextIndexVersionV1,
			Fields:        []TextIndexField{{Field: "body"}},
			StoragePolicy: RootStorageCompressed,
		}},
	})
	if err != nil {
		t.Fatalf("normalize v1 metadata: %v", err)
	}
	metaV2, err := normalizeCollectionMeta(CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			IndexStateStoragePolicy: RootStorageFast,
		},
		TextIndexes: []TextIndexDefinition{{
			Name:          "lexical",
			Version:       TextIndexVersionV2,
			Fields:        []TextIndexField{{Field: "body"}},
			StoragePolicy: RootStorageCompressed,
		}},
	})
	if err != nil {
		t.Fatalf("normalize v2 metadata: %v", err)
	}
	cases := []struct {
		name string
		meta CollectionMeta
		root string
		want backenddb.OrderedRootStoragePolicy
	}{
		{
			name: "postings honor text index storage policy",
			meta: metaV1,
			root: collectionTextIndexRootName("docs", "lexical"),
			want: backenddb.OrderedRootStorageValueLogLeaves,
		},
		{
			name: "state uses index state storage policy",
			meta: metaV1,
			root: collectionTextStateRootName("docs", "lexical"),
			want: backenddb.OrderedRootStoragePagerLeaves,
		},
		{
			name: "stats uses index state storage policy",
			meta: metaV1,
			root: collectionTextStatsRootName("docs", "lexical"),
			want: backenddb.OrderedRootStoragePagerLeaves,
		},
		{
			name: "v2 docmap honors text index storage policy",
			meta: metaV2,
			root: collectionTextV2DocMapRootName("docs", "lexical"),
			want: backenddb.OrderedRootStorageValueLogLeaves,
		},
		{
			name: "v2 norm blocks honor text index storage policy",
			meta: metaV2,
			root: collectionTextV2NormBlocksRootName("docs", "lexical"),
			want: backenddb.OrderedRootStorageValueLogLeaves,
		},
		{
			name: "v2 generations use index state storage policy",
			meta: metaV2,
			root: collectionTextV2GenerationsRootName("docs", "lexical"),
			want: backenddb.OrderedRootStoragePagerLeaves,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := collectionRootStoragePolicyForDB(nil, tc.meta, tc.root)
			if err != nil {
				t.Fatalf("collectionRootStoragePolicyForDB(%q): %v", tc.root, err)
			}
			if got != tc.want {
				t.Fatalf("policy=%v want %v", got, tc.want)
			}
		})
	}
	if _, err := collectionRootStoragePolicyForDB(nil, metaV1, collectionTextIndexRootName("docs", "missing")); err == nil || !strings.Contains(err.Error(), "unknown collection root") {
		t.Fatalf("missing text root err=%v want unknown collection root", err)
	}
	if _, err := collectionRootStoragePolicyForDB(nil, metaV1, collectionTextV2DocMapRootName("docs", "lexical")); err == nil || !strings.Contains(err.Error(), "unknown collection root") {
		t.Fatalf("v1 v2-root policy err=%v want unknown collection root", err)
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
			Name:    "lexical",
			Version: TextIndexVersionV1,
			Fields:  []TextIndexField{{Field: "body"}},
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
