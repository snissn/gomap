package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestTextIndexDefaultCreateCollectionPublishesV2RootsStatus2690(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:   "lexical",
			Fields: []TextIndexField{{Field: "body"}, {Field: "title", Weight: 2}},
		}},
	})
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if len(meta.TextIndexes) != 1 || meta.TextIndexes[0].Version != TextIndexVersionV2 {
		t.Fatalf("metadata text indexes=%+v want default v2", meta.TextIndexes)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	assertDefaultV2StatusAndEmptyRoots2690(t, d, col, "lexical")
}

func TestTextIndexDefaultCreateCollectionNoopDoesNotReplaceRacedV2Roots2690(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	meta := &CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:   "lexical",
			Fields: []TextIndexField{{Field: "body"}},
		}},
	}

	blocked := make(chan struct{})
	release := make(chan struct{})
	var blockedOnce atomic.Bool
	testBeforeCreateCollectionPublishHook.installMu.Lock()
	testBeforeCreateCollectionPublishHook.ptr.Store(&testCreateCollectionPublishHook{fn: func(got CollectionMeta) {
		if got.Name != "docs" || !blockedOnce.CompareAndSwap(false, true) {
			return
		}
		close(blocked)
		<-release
	}})
	defer func() {
		testBeforeCreateCollectionPublishHook.ptr.Store(nil)
		testBeforeCreateCollectionPublishHook.installMu.Unlock()
	}()

	firstErr := make(chan error, 1)
	go func() {
		_, err := NewCollectionManager(d).CreateCollection(meta)
		firstErr <- err
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first CreateCollection to block before publish")
	}

	secondErr := make(chan error, 1)
	go func() {
		_, err := NewCollectionManager(d).CreateCollection(meta)
		secondErr <- err
	}()
	select {
	case err := <-secondErr:
		t.Fatalf("second CreateCollection completed before first publish err=%v; want schema serialization", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-firstErr:
		if err != nil {
			t.Fatalf("first CreateCollection: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first CreateCollection to finish")
	}
	select {
	case err := <-secondErr:
		if err != nil {
			t.Fatalf("second CreateCollection: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for second CreateCollection to finish")
	}

	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection after raced creates: %v", err)
	}
	stateBeforeIdempotent := d.State()
	if stateBeforeIdempotent == nil {
		t.Fatal("db state before idempotent create is nil")
	}
	if _, err := NewCollectionManager(d).CreateCollection(meta); err != nil {
		t.Fatalf("idempotent CreateCollection after raced creates: %v", err)
	}
	stateAfterIdempotent := d.State()
	if stateAfterIdempotent == nil {
		t.Fatal("db state after idempotent create is nil")
	}
	if stateAfterIdempotent.CommitSeq != stateBeforeIdempotent.CommitSeq {
		t.Fatalf("idempotent CreateCollection advanced commit seq from %d to %d; want no orphan root publish", stateBeforeIdempotent.CommitSeq, stateAfterIdempotent.CommitSeq)
	}
	if _, err := col.Insert([]byte("d1"), []byte(`{"body":"refund policy"}`)); err != nil {
		t.Fatalf("Insert after raced creates: %v", err)
	}
	after, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("SearchText after raced no-op create: %v", err)
	}
	assertSearchIDs2690(t, after, []string{"d1"})
	assertZeroDocV2SearchStats2690(t, after.Stats)
	status, err := col.TextIndexStatus("lexical")
	if err != nil {
		t.Fatalf("TextIndexStatus after raced no-op create: %v", err)
	}
	if status.Version != TextIndexVersionV2 || !slicesEqualStrings(status.ActiveRootNames, collectionTextV2RootNames("docs", "lexical")) {
		t.Fatalf("status after raced no-op create=%+v want default-v2 roots preserved", status)
	}
}

func TestTextIndexDefaultCreateCollectionPreflightSkipsStaleV2Plan2690(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	meta := CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:   "lexical",
			Fields: []TextIndexField{{Field: "body"}},
		}},
	}
	normalized, err := mgr.CreateCollection(&meta)
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.Insert([]byte("d1"), []byte(`{"body":"refund policy"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	stateBefore := d.State()
	if stateBefore == nil {
		t.Fatal("db state before stale preflight publish is nil")
	}

	plan, err := mgr.buildCreateCollectionInitialTextV2Plan(*normalized)
	if err != nil {
		t.Fatalf("buildCreateCollectionInitialTextV2Plan: %v", err)
	}
	defer resetCollectionTables(plan.tables)
	if len(plan.rootNames) == 0 {
		t.Fatal("stale default-v2 create plan has no roots")
	}
	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(plan.rootNames))
	for i, rootName := range plan.rootNames {
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      plan.baseRootIDs[rootName],
			Iter:          plan.tables[i].NewIterator(nil, nil),
			StoragePolicy: plan.policies[i],
		})
	}
	_, _, err = d.PublishOrderedRootDeltaGroupWithPreflightAndSystemDeltaBuilder(ordered, mgr.createCollectionExistingSchemaPreflight(*normalized, nil), func([]uint64) (iterator.UnsafeIterator, error) {
		t.Fatal("system builder called after no-op create preflight")
		return nil, nil
	})
	if !errors.Is(err, errCreateCollectionNoopExistingSchema) {
		t.Fatalf("stale preflight publish err=%v want errCreateCollectionNoopExistingSchema", err)
	}
	stateAfter := d.State()
	if stateAfter == nil {
		t.Fatal("db state after stale preflight publish is nil")
	}
	if stateAfter.CommitSeq != stateBefore.CommitSeq {
		t.Fatalf("stale preflight publish advanced commit seq from %d to %d; want no root apply/finalize", stateBefore.CommitSeq, stateAfter.CommitSeq)
	}
	got, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("SearchText after stale preflight publish: %v", err)
	}
	assertSearchIDs2690(t, got, []string{"d1"})
}

func TestTextIndexDefaultCreateTextIndexBackfillUpdateDeleteReopenSearch2690(t *testing.T) {
	dir := t.TempDir()
	d := openTextV2TestDB(t, dir, false)
	firstClosed := false
	defer func() {
		if !firstClosed {
			_ = d.Close()
		}
	}()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("d1"), []byte("d2"), []byte("d3")}, [][]byte{
		[]byte(`{"title":"Refund","body":"refund policy"}`),
		[]byte(`{"title":"Refund","body":"refund delayed"}`),
		[]byte(`{"title":"Policy","body":"refund policy"}`),
	}); err != nil {
		t.Fatalf("InsertBatch setup: %v", err)
	}
	meta, backfill, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Fields: []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}}, StorePositions: true})
	if err != nil {
		t.Fatalf("CreateTextIndex default v2: %v", err)
	}
	if len(meta.TextIndexes) != 1 || meta.TextIndexes[0].Version != TextIndexVersionV2 {
		t.Fatalf("metadata text indexes=%+v want default v2", meta.TextIndexes)
	}
	if backfill.DocumentsScanned != 3 || backfill.V2DocIDEntries != 3 || backfill.V2StatusRecords != 1 || backfill.V2FormatRecords != len(collectionTextV2RootNames("docs", "lexical")) || backfill.StateEntries != 0 || backfill.PostingEntries != 0 {
		t.Fatalf("backfill stats=%+v want v2 backfill roots/status and no v1 entries", backfill)
	}

	before, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("SearchText before mutations: %v", err)
	}
	assertSearchIDs2690(t, before, []string{"d1", "d2", "d3"})
	assertZeroDocV2SearchStats2690(t, before.Stats)

	matched, modified, err := col.Update([]byte("d1"), func(current []byte) ([]byte, bool, error) {
		return []byte(`{"title":"Shipping","body":"shipping only"}`), true, nil
	})
	if err != nil || !matched || !modified {
		t.Fatalf("Update d1 matched=%v modified=%v err=%v", matched, modified, err)
	}
	deleted, err := col.DeleteDocument([]byte("d2"))
	if err != nil || !deleted {
		t.Fatalf("DeleteDocument d2 deleted=%v err=%v", deleted, err)
	}
	after, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("SearchText after mutations: %v", err)
	}
	assertSearchIDs2690(t, after, []string{"d3"})
	assertZeroDocV2SearchStats2690(t, after.Stats)

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	firstClosed = true
	reopened := openTextV2TestDB(t, dir, false)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	reopenedStatus, err := reopenedCol.TextIndexStatus("lexical")
	if err != nil {
		t.Fatalf("TextIndexStatus reopened: %v", err)
	}
	if reopenedStatus.Version != TextIndexVersionV2 || !reopenedStatus.Ready || !reopenedStatus.Readable || !reopenedStatus.Writable {
		t.Fatalf("reopened status=%+v want ready default v2", reopenedStatus)
	}
	reopenedSearch, err := reopenedCol.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("SearchText reopened: %v", err)
	}
	assertSearchIDs2690(t, reopenedSearch, []string{"d3"})
	assertZeroDocV2SearchStats2690(t, reopenedSearch.Stats)
}

func TestTextIndexExplicitV1AndDefaultV2Coexist2690(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("d1"), []byte("d2")}, [][]byte{[]byte(`{"body":"refund policy"}`), []byte(`{"body":"shipping policy"}`)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "legacy", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}, StorePositions: true}); err != nil {
		t.Fatalf("CreateTextIndex explicit v1: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Fields: []TextIndexField{{Field: "body"}}, StorePositions: true}); err != nil {
		t.Fatalf("CreateTextIndex default v2: %v", err)
	}
	legacyStatus, err := col.TextIndexStatus("legacy")
	if err != nil {
		t.Fatalf("TextIndexStatus legacy: %v", err)
	}
	defaultStatus, err := col.TextIndexStatus("lexical")
	if err != nil {
		t.Fatalf("TextIndexStatus lexical: %v", err)
	}
	if legacyStatus.Version != TextIndexVersionV1 || !slicesEqualStrings(legacyStatus.ActiveRootNames, collectionTextRootNames("docs", "legacy")) {
		t.Fatalf("legacy status=%+v want explicit v1 roots", legacyStatus)
	}
	if defaultStatus.Version != TextIndexVersionV2 || !slicesEqualStrings(defaultStatus.ActiveRootNames, collectionTextV2RootNames("docs", "lexical")) {
		t.Fatalf("default status=%+v want default v2 roots", defaultStatus)
	}
	legacySearch, err := col.SearchText(TextSearchOptions{IndexName: "legacy", Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("SearchText legacy: %v", err)
	}
	defaultSearch, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("SearchText default v2: %v", err)
	}
	assertSearchIDs2690(t, legacySearch, []string{"d1"})
	assertSearchIDs2690(t, defaultSearch, []string{"d1"})
	if defaultSearch.Stats.TextStateLookups != 0 || defaultSearch.Stats.DocumentsFetched != 0 || defaultSearch.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("default v2 stats=%+v want no v1 state/doc fallback", defaultSearch.Stats)
	}
}

func TestHybridTextCandidatesDefaultV2ZeroDocumentWork2690(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Fields: []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}}, StorePositions: true}); err != nil {
		t.Fatalf("CreateTextIndex default v2: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("d1"), []byte("d2"), []byte("d3")}, [][]byte{
		[]byte(`{"title":"refund","body":"refund policy"}`),
		[]byte(`{"title":"plain","body":"refund policy"}`),
		[]byte(`{"title":"shipping","body":"policy"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	status, err := col.TextIndexStatus("lexical")
	if err != nil {
		t.Fatalf("TextIndexStatus: %v", err)
	}
	if status.Version != TextIndexVersionV2 {
		t.Fatalf("status=%+v want default v2", status)
	}
	got, err := col.SearchHybridTextCandidates(HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 2})
	if err != nil {
		t.Fatalf("SearchHybridTextCandidates: %v", err)
	}
	if len(got.Candidates) != 2 || string(got.Candidates[0].ID) != "d1" {
		t.Fatalf("candidates=%+v want two ranked v2 text candidates headed by d1", got.Candidates)
	}
	if got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.TextStateLookups != 0 || got.Stats.TextMatchDetailsBuilt != 0 || got.Stats.FailClosed != 0 {
		t.Fatalf("hybrid stats=%+v want default v2 score-only zero-doc candidate generation", got.Stats)
	}
}

func TestLegacyDiskTextIndexMissingVersionDecodesAsV12690(t *testing.T) {
	dir := t.TempDir()
	d := openTextV2TestDB(t, dir, false)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.Insert([]byte("d1"), []byte(`{"body":"refund policy"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "legacy", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}, StorePositions: true}); err != nil {
		t.Fatalf("CreateTextIndex explicit v1: %v", err)
	}
	legacyMeta := col.Meta()
	legacyMeta.TextIndexes[0].Version = TextIndexVersionDefault
	legacyMeta.TextIndexes[0].Rollout = TextIndexRolloutDefault
	raw, err := json.Marshal(collectionMetaDisk{
		Version:       collectionMetaVersion,
		Name:          legacyMeta.Name,
		Options:       legacyMeta.Options,
		Indexes:       legacyMeta.Indexes,
		VectorIndexes: legacyMeta.VectorIndexes,
		TextIndexes:   legacyMeta.TextIndexes,
	})
	if err != nil {
		t.Fatalf("marshal legacy metadata: %v", err)
	}
	if _, _, err := d.PublishOrderedRootGroupWithSystemBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		snap := d.AcquireSnapshot()
		if snap == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = snap.Close() }()
		return buildSystemTargetIterator(snap, map[string][]byte{systemCollectionMetaKey("docs"): raw})
	}); err != nil {
		t.Fatalf("publish legacy metadata: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openTextV2TestDB(t, dir, false)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	reopenedMeta := reopenedCol.Meta()
	if len(reopenedMeta.TextIndexes) != 1 || reopenedMeta.TextIndexes[0].Version != TextIndexVersionV1 {
		t.Fatalf("reopened metadata=%+v want missing disk version preserved as v1", reopenedMeta.TextIndexes)
	}
	got, err := reopenedCol.SearchText(TextSearchOptions{IndexName: "legacy", Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("SearchText reopened legacy v1: %v", err)
	}
	assertSearchIDs2690(t, got, []string{"d1"})
}

func assertDefaultV2StatusAndEmptyRoots2690(t *testing.T, d *backenddb.DB, col *Collection, indexName string) {
	t.Helper()
	status, err := col.TextIndexStatus(indexName)
	if err != nil {
		t.Fatalf("TextIndexStatus: %v", err)
	}
	if status.Version != TextIndexVersionV2 || !status.Ready || !status.Readable || !status.Writable || status.FailClosed || !slicesEqualStrings(status.ActiveRootNames, collectionTextV2RootNames("docs", indexName)) {
		t.Fatalf("status=%+v want ready default v2", status)
	}
	stats, err := col.TextIndexStorageStats(indexName)
	if err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
	if stats.Version != TextIndexVersionV2 || stats.V2FormatRecords != uint64(len(collectionTextV2RootNames("docs", indexName))) || stats.V2StatusRecords != 1 || stats.V2NextOrdinal != 1 || stats.V2LiveDocuments != 0 || stats.V2DocIDEntries != 0 || stats.V2PostingBlocks != 0 {
		t.Fatalf("storage stats=%+v want empty v2 roots/status", stats)
	}
	for _, rootName := range collectionTextV2RootNames("docs", indexName) {
		if rootID := requireCollectionRootDescriptor2624(t, d, rootName); rootID == 0 {
			t.Fatalf("root %q descriptor is zero", rootName)
		}
	}
}

func assertSearchIDs2690(t *testing.T, got TextSearchResponse, want []string) {
	t.Helper()
	if len(got.Results) != len(want) {
		t.Fatalf("results=%+v want ids %q", got.Results, want)
	}
	for i := range want {
		if !bytes.Equal(got.Results[i].DocumentID, []byte(want[i])) {
			t.Fatalf("result[%d]=%+v want id %q; all results=%+v", i, got.Results[i], want[i], got.Results)
		}
	}
}

func assertZeroDocV2SearchStats2690(t *testing.T, stats TextSearchStats) {
	t.Helper()
	if stats.DocumentsFetched != 0 || stats.FullDocumentScanFallbacks != 0 || stats.TextStateLookups != 0 || stats.TextMatchDetailsBuilt != 0 || stats.FailClosed != 0 {
		t.Fatalf("stats=%+v want v2 score-only search with zero docs/state/details/fail-closed", stats)
	}
}
