package collections

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

type textV2HardeningModelDoc2734 struct {
	body   string
	tenant string
	live   bool
}

func TestTextV2HardeningRandomizedModel2734(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col := createTextV2HardeningCollection2734(t, d, "docs")

	model := make(map[string]textV2HardeningModelDoc2734)
	var ids [][]byte
	var docs [][]byte
	for i := 0; i < 16; i++ {
		id := fmt.Sprintf("doc-%03d", i)
		doc := textV2HardeningModelDoc2734{
			body:   textV2HardeningBody2734(i),
			tenant: textV2HardeningTenant2734(i),
			live:   true,
		}
		model[id] = doc
		ids = append(ids, []byte(id))
		docs = append(docs, textV2HardeningJSON2734(doc))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch seed: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{
		Name:           "lexical",
		Version:        TextIndexVersionV2,
		StorePositions: true,
		Fields:         []TextIndexField{{Field: "body"}},
	}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	assertTextV2HardeningModelQueries2734(t, col, model)

	rng := rand.New(rand.NewSource(2734))
	nextID := 16
	for step := 0; step < 54; step++ {
		switch rng.Intn(3) {
		case 0:
			id := fmt.Sprintf("doc-%03d", nextID)
			nextID++
			doc := textV2HardeningModelDoc2734{
				body:   textV2HardeningBody2734(step + nextID),
				tenant: textV2HardeningTenant2734(step),
				live:   true,
			}
			if _, err := col.Insert([]byte(id), textV2HardeningJSON2734(doc)); err != nil {
				t.Fatalf("step %d Insert %s: %v", step, id, err)
			}
			model[id] = doc
		case 1:
			id := textV2HardeningRandomLiveID2734(t, rng, model)
			doc := model[id]
			doc.body = textV2HardeningBody2734(step + 100)
			doc.tenant = textV2HardeningTenant2734(step + 7)
			matched, modified, err := col.Update([]byte(id), func([]byte) ([]byte, bool, error) {
				return textV2HardeningJSON2734(doc), true, nil
			})
			if err != nil || !matched || !modified {
				t.Fatalf("step %d Update %s matched=%t modified=%t err=%v", step, id, matched, modified, err)
			}
			model[id] = doc
		default:
			id := textV2HardeningRandomLiveID2734(t, rng, model)
			deleted, err := col.DeleteBatch([][]byte{[]byte(id)})
			if err != nil || deleted != 1 {
				t.Fatalf("step %d DeleteBatch %s deleted=%d err=%v", step, id, deleted, err)
			}
			doc := model[id]
			doc.live = false
			model[id] = doc
		}
		if step%3 == 0 {
			assertTextV2HardeningModelQueries2734(t, col, model)
		}
	}
	assertTextV2HardeningModelQueries2734(t, col, model)
}

func TestTextV2HardeningCommandWALDefaultCreateReplay2734(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	meta := CollectionMeta{
		Name:    "docs",
		Indexes: []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}},
		TextIndexes: []TextIndexDefinition{{
			Name:           "lexical",
			StorePositions: true,
			Fields:         []TextIndexField{{Field: "body"}},
		}},
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCatalogCreateCollection, commitlog.PayloadFormatCatalogCreateCollectionV1, catalogCreateCollectionPayload(t, meta))
	insertPayload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("docs", []commitlog.CollectionDocument{
		{ID: []byte("d1"), Document: []byte(`{"body":"refund policy support","tenant":"tenant-a"}`)},
		{ID: []byte("d2"), Document: []byte(`{"body":"shipping support","tenant":"tenant-b"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 2, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, insertPayload)

	d := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection replayed: %v", err)
	}
	status, err := col.TextIndexStatus("lexical")
	if err != nil {
		_ = d.Close()
		t.Fatalf("TextIndexStatus replayed: %v", err)
	}
	if status.Version != TextIndexVersionV2 || !status.Ready || !status.Readable || !status.Writable || status.FailClosed {
		_ = d.Close()
		t.Fatalf("status=%+v want replayed default-v2 ready index", status)
	}
	storage, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		_ = d.Close()
		t.Fatalf("TextIndexStorageStats replayed: %v", err)
	}
	if storage.Version != TextIndexVersionV2 || storage.Documents != 2 || storage.V2PositionEntries == 0 {
		_ = d.Close()
		t.Fatalf("storage=%+v want replayed v2 roots with positions", storage)
	}
	got, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		_ = d.Close()
		t.Fatalf("SearchText replayed: %v", err)
	}
	assertTextV2HardeningSearchIDs2734(t, got, []string{"d1"})
	assertTextV2HardeningNoDocStats2734(t, got.Stats)
	if _, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{Policy: TextIndexMaintenancePolicy{MinDeletedDocuments: 1}}); !errors.Is(err, backenddb.ErrCommandWALRejected) {
		_ = d.Close()
		t.Fatalf("MaintainTextIndex under command WAL err=%v want ErrCommandWALRejected", err)
	}

	if _, err := col.Insert([]byte("d3"), []byte(`{"body":"refund policy","tenant":"tenant-a"}`)); err != nil {
		_ = d.Close()
		t.Fatalf("Insert live command WAL: %v", err)
	}
	if _, _, err := col.Update([]byte("d2"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"body":"refund shipping","tenant":"tenant-b"}`), true, nil
	}); err != nil {
		_ = d.Close()
		t.Fatalf("Update live command WAL: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close command WAL DB: %v", err)
	}

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	after, err := reopenedCol.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("SearchText reopened: %v", err)
	}
	assertTextV2HardeningSearchIDs2734(t, after, []string{"d1", "d2", "d3"})
	assertTextV2HardeningNoDocStats2734(t, after.Stats)
}

func TestTextV2HardeningCorruptionFailsClosedAcrossShapes2734(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col := createTextV2HardeningCollection2734(t, d, "docs")
	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1"), []byte("d2"), []byte("d3")},
		[][]byte{
			[]byte(`{"body":"refund policy support","tenant":"tenant-a"}`),
			[]byte(`{"body":"refund shipping","tenant":"tenant-a"}`),
			[]byte(`{"body":"policy billing","tenant":"tenant-b"}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, StorePositions: true, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	rootName := collectionTextV2PostingBlocksRootName("docs", "lexical")
	blockKey := firstTextV2PostingBlockKeyForTerm2626(t, d, "docs", rootName, "refund")
	corruptTextRootValue(t, d, "docs", rootName, blockKey, []byte{99})

	for _, opts := range []TextSearchOptions{
		{IndexName: "lexical", Query: "refund", TopK: 4, CandidateLimit: 16, MaxPostingsScanned: 64, ResultMode: TextSearchResultModeScoreOnly},
		{IndexName: "lexical", Query: "refund OR policy", Operator: TextSearchOperatorOR, TopK: 4, CandidateLimit: 16, MaxPostingsScanned: 64, ResultMode: TextSearchResultModeScoreOnly},
		{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund policy"}, TopK: 4, CandidateLimit: 16, MaxPostingsScanned: 64, ResultMode: TextSearchResultModeScoreOnly},
	} {
		got, err := col.SearchText(opts)
		if !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
			t.Fatalf("SearchText opts=%+v err=%v want unavailable/storage corrupt", opts, err)
		}
		if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != textSearchFailClosedStorageCorrupt || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
			t.Fatalf("SearchText opts=%+v stats=%+v want storage fail-closed without document work", opts, got.Stats)
		}
	}

	candidates, err := col.SearchHybridTextCandidates(HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4})
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) || !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("SearchHybridTextCandidates err=%v want hybrid/text/storage unavailable", err)
	}
	if len(candidates.Candidates) != 0 || candidates.Stats.FailClosed != 1 || candidates.Stats.FailClosedReason != HybridFailClosedReasonTextIndexUnavailable || candidates.Stats.DocumentsFetched != 0 || candidates.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("candidates=%+v want fail-closed text candidates without document work", candidates)
	}

	hybrid, err := col.SearchHybrid(HybridSearchOptions{
		TopK:         4,
		Text:         &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		ScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "tenant-a"},
		ResultMode:   HybridResultModeScoreOnly,
	})
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) || !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("SearchHybrid err=%v want hybrid/text/storage unavailable", err)
	}
	if len(hybrid.Results) != 0 || hybrid.Stats.FailClosed != 1 || hybrid.Stats.FailClosedReason != HybridFailClosedReasonTextIndexUnavailable || hybrid.Stats.DocumentsFetched != 0 || hybrid.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("hybrid=%+v want fail-closed hybrid without document work", hybrid)
	}
}

func TestTextV2HardeningConcurrentRewriteCreateDropSearchWrite2734(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	stable := createTextV2HardeningCollection2734(t, d, "docs")
	var ids [][]byte
	var docs [][]byte
	for i := 0; i < 64; i++ {
		doc := textV2HardeningModelDoc2734{
			body:   textV2HardeningBody2734(i),
			tenant: textV2HardeningTenant2734(i),
			live:   true,
		}
		ids = append(ids, []byte(fmt.Sprintf("doc-%03d", i)))
		docs = append(docs, textV2HardeningJSON2734(doc))
	}
	if _, err := stable.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch stable: %v", err)
	}
	if _, _, err := stable.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, StorePositions: true, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex stable: %v", err)
	}
	volatile := createTextV2HardeningCollection2734(t, d, "volatile")

	errCh := make(chan error, 16)
	var wg sync.WaitGroup
	run := func(fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil {
				errCh <- err
			}
		}()
	}
	run(func() error {
		for i := 0; i < 40; i++ {
			got, err := stable.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund OR policy", Operator: TextSearchOperatorOR, TopK: 8, CandidateLimit: 128, MaxPostingsScanned: 512, ResultMode: TextSearchResultModeScoreOnly})
			if err != nil {
				return fmt.Errorf("SearchText iteration %d: %w", i, err)
			}
			if got.Stats.FailClosed != 0 || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
				return fmt.Errorf("SearchText iteration %d stats=%+v want no fail/doc fallback", i, got.Stats)
			}
		}
		return nil
	})
	run(func() error {
		for i := 0; i < 40; i++ {
			got, err := stable.SearchHybrid(HybridSearchOptions{
				TopK:         6,
				Text:         &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 32},
				ScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: textV2HardeningTenant2734(i)},
				ResultMode:   HybridResultModeScoreOnly,
			})
			if err != nil {
				if errors.Is(err, ErrHybridSearchStaleIndex) {
					if got.Stats.FailClosed == 0 || got.Stats.FailClosedReason != HybridFailClosedReasonSnapshotMismatch || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
						return fmt.Errorf("SearchHybrid stale iteration %d stats=%+v want snapshot-mismatch fail-closed without document fallback", i, got.Stats)
					}
					continue
				}
				return fmt.Errorf("SearchHybrid iteration %d: %w", i, err)
			}
			if got.Stats.FailClosed != 0 || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
				return fmt.Errorf("SearchHybrid iteration %d stats=%+v want no fail/doc fallback", i, got.Stats)
			}
		}
		return nil
	})
	run(func() error {
		for i := 0; i < 32; i++ {
			id := []byte(fmt.Sprintf("doc-%03d", i%64))
			doc := textV2HardeningModelDoc2734{
				body:   textV2HardeningBody2734(i + 200),
				tenant: textV2HardeningTenant2734(i),
				live:   true,
			}
			matched, _, err := stable.Update(id, func([]byte) ([]byte, bool, error) {
				return textV2HardeningJSON2734(doc), true, nil
			})
			if err != nil || !matched {
				return fmt.Errorf("Update iteration %d matched=%t err=%v", i, matched, err)
			}
		}
		return nil
	})
	run(func() error {
		for i := 0; i < 10; i++ {
			stats, err := stable.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
				Policy: TextIndexMaintenancePolicy{MinStalePostings: 1, MinDeletedDocuments: 1, MinMicroPostingBlocks: 1},
				Force:  i%3 == 0,
			})
			if err != nil {
				return fmt.Errorf("MaintainTextIndex iteration %d: %w", i, err)
			}
			if stats.IndexesScanned != 1 {
				return fmt.Errorf("MaintainTextIndex iteration %d stats=%+v want one scanned index", i, stats)
			}
		}
		return nil
	})
	run(func() error {
		for i := 0; i < 16; i++ {
			if _, _, err := volatile.CreateTextIndex(TextIndexDefinition{Name: "volatile", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
				return fmt.Errorf("CreateTextIndex volatile iteration %d: %w", i, err)
			}
			if _, err := volatile.DropTextIndex("volatile"); err != nil {
				return fmt.Errorf("DropTextIndex volatile iteration %d: %w", i, err)
			}
		}
		return nil
	})
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func createTextV2HardeningCollection2734(t *testing.T, d *backenddb.DB, name string) *Collection {
	t.Helper()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    name,
		Indexes: []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("CreateCollection %s: %v", name, err)
	}
	col, err := mgr.OpenCollection(name)
	if err != nil {
		t.Fatalf("OpenCollection %s: %v", name, err)
	}
	return col
}

func assertTextV2HardeningModelQueries2734(t *testing.T, col *Collection, model map[string]textV2HardeningModelDoc2734) {
	t.Helper()
	cases := []struct {
		name string
		opts TextSearchOptions
		want []string
	}{
		{
			name: "single_refund",
			opts: TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 256, CandidateLimit: 512, MaxPostingsScanned: 2048, ResultMode: TextSearchResultModeScoreOnly},
			want: textV2HardeningModelIDs2734(model, func(doc textV2HardeningModelDoc2734) bool {
				return textV2HardeningHasTerm2734(doc.body, "refund")
			}),
		},
		{
			name: "and_refund_policy",
			opts: TextSearchOptions{IndexName: "lexical", Query: "refund AND policy", Operator: TextSearchOperatorAND, TopK: 256, CandidateLimit: 512, MaxPostingsScanned: 2048, ResultMode: TextSearchResultModeScoreOnly},
			want: textV2HardeningModelIDs2734(model, func(doc textV2HardeningModelDoc2734) bool {
				return textV2HardeningHasTerm2734(doc.body, "refund") && textV2HardeningHasTerm2734(doc.body, "policy")
			}),
		},
		{
			name: "or_refund_shipping",
			opts: TextSearchOptions{IndexName: "lexical", Query: "refund OR shipping", Operator: TextSearchOperatorOR, TopK: 256, CandidateLimit: 512, MaxPostingsScanned: 2048, ResultMode: TextSearchResultModeScoreOnly},
			want: textV2HardeningModelIDs2734(model, func(doc textV2HardeningModelDoc2734) bool {
				return textV2HardeningHasTerm2734(doc.body, "refund") || textV2HardeningHasTerm2734(doc.body, "shipping")
			}),
		},
		{
			name: "phrase_refund_policy",
			opts: TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund policy"}, TopK: 256, CandidateLimit: 512, MaxPostingsScanned: 2048, ResultMode: TextSearchResultModeScoreOnly},
			want: textV2HardeningModelIDs2734(model, func(doc textV2HardeningModelDoc2734) bool {
				return strings.Contains(" "+doc.body+" ", " refund policy ")
			}),
		},
	}
	for _, tc := range cases {
		got, err := col.SearchText(tc.opts)
		if err != nil {
			t.Fatalf("%s SearchText: %v", tc.name, err)
		}
		assertTextV2HardeningSearchIDs2734(t, got, tc.want)
		assertTextV2HardeningNoDocStats2734(t, got.Stats)
	}

	wantHybrid := textV2HardeningModelIDs2734(model, func(doc textV2HardeningModelDoc2734) bool {
		return doc.tenant == "tenant-a" && textV2HardeningHasTerm2734(doc.body, "refund")
	})
	hybrid, err := col.SearchHybrid(HybridSearchOptions{
		TopK:         256,
		Text:         &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 256},
		ScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "tenant-a"},
		ResultMode:   HybridResultModeScoreOnly,
	})
	if err != nil {
		t.Fatalf("SearchHybrid refund tenant-a: %v", err)
	}
	assertTextV2HardeningHybridIDs2734(t, hybrid, wantHybrid)
	if hybrid.Stats.FailClosed != 0 || hybrid.Stats.DocumentsFetched != 0 || hybrid.Stats.FullDocumentScanFallbacks != 0 || hybrid.Stats.TextStateLookups != 0 || hybrid.Stats.TextMatchDetailsBuilt != 0 {
		t.Fatalf("hybrid stats=%+v want zero-doc/fail-closed path", hybrid.Stats)
	}
}

func assertTextV2HardeningSearchIDs2734(t *testing.T, got TextSearchResponse, want []string) {
	t.Helper()
	ids := make([]string, 0, len(got.Results))
	for _, result := range got.Results {
		ids = append(ids, string(result.DocumentID))
	}
	sort.Strings(ids)
	sort.Strings(want)
	if !slicesEqualStrings(ids, want) {
		t.Fatalf("search ids=%v want %v response=%+v", ids, want, got)
	}
}

func assertTextV2HardeningHybridIDs2734(t *testing.T, got HybridSearchResponse, want []string) {
	t.Helper()
	ids := make([]string, 0, len(got.Results))
	for _, result := range got.Results {
		ids = append(ids, string(result.ID))
	}
	sort.Strings(ids)
	sort.Strings(want)
	if !slicesEqualStrings(ids, want) {
		t.Fatalf("hybrid ids=%v want %v response=%+v", ids, want, got)
	}
}

func assertTextV2HardeningNoDocStats2734(t *testing.T, stats TextSearchStats) {
	t.Helper()
	if stats.FailClosed != 0 || stats.DocumentsFetched != 0 || stats.FullDocumentScanFallbacks != 0 || stats.TextStateLookups != 0 || stats.TextMatchDetailsBuilt != 0 {
		t.Fatalf("text stats=%+v want zero-doc/fail-closed path", stats)
	}
}

func textV2HardeningModelIDs2734(model map[string]textV2HardeningModelDoc2734, match func(textV2HardeningModelDoc2734) bool) []string {
	ids := make([]string, 0)
	for id, doc := range model {
		if doc.live && match(doc) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func textV2HardeningRandomLiveID2734(t *testing.T, rng *rand.Rand, model map[string]textV2HardeningModelDoc2734) string {
	t.Helper()
	live := textV2HardeningModelIDs2734(model, func(textV2HardeningModelDoc2734) bool { return true })
	if len(live) == 0 {
		t.Fatal("model has no live documents")
	}
	return live[rng.Intn(len(live))]
}

func textV2HardeningHasTerm2734(body, term string) bool {
	for _, field := range strings.Fields(body) {
		if field == term {
			return true
		}
	}
	return false
}

func textV2HardeningBody2734(i int) string {
	switch i % 8 {
	case 0:
		return "refund policy support"
	case 1:
		return "refund shipping delay"
	case 2:
		return "policy billing invoice"
	case 3:
		return "shipping tracking support"
	case 4:
		return "refund policy"
	case 5:
		return "support refund policy escalation"
	case 6:
		return "warranty exchange"
	default:
		return "refund support shipping"
	}
}

func textV2HardeningTenant2734(i int) string {
	if i%2 == 0 {
		return "tenant-a"
	}
	return "tenant-b"
}

func textV2HardeningJSON2734(doc textV2HardeningModelDoc2734) []byte {
	return []byte(fmt.Sprintf(`{"body":%q,"tenant":%q}`, doc.body, doc.tenant))
}
