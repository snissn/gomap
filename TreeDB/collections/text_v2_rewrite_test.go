package collections

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTextV2RewriteMergePurgesStaleTombstonesPositionsAndReopens2630(t *testing.T) {
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
	def := TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, StorePositions: true, Fields: []TextIndexField{{Field: "body"}}}
	if _, err := col.InsertBatch([][]byte{[]byte("d1"), []byte("d2"), []byte("d3")}, [][]byte{
		[]byte(`{"body":"refund old keep"}`),
		[]byte(`{"body":"refund delete keep"}`),
		[]byte(`{"body":"shipping keep"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(def); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if _, _, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"body":"updated keep"}`), true, nil
	}); err != nil {
		t.Fatalf("Update d1: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("d2")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch d2 deleted=%d err=%v", deleted, err)
	}
	if _, err := col.Insert([]byte("d4"), []byte(`{"body":"refund refund live"}`)); err != nil {
		t.Fatalf("Insert d4: %v", err)
	}

	beforeStats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats before: %v", err)
	}
	if beforeStats.V2DeletedDocs == 0 || beforeStats.V2MicroPostingBlocks == 0 || beforeStats.V2RewriteMergeState != TextIndexRewriteMergeStatePending {
		t.Fatalf("before stats=%+v want pending tombstones/micro blocks", beforeStats)
	}
	beforeRefund, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("refund before rewrite: %v", err)
	}
	if gotIDs := textSearchResultIDs2630(beforeRefund); !slicesEqualStrings(gotIDs, []string{"d4"}) {
		t.Fatalf("refund before ids=%v response=%+v want only d4", gotIDs, beforeRefund)
	}

	stats, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{TargetPostingsPerBlock: 8})
	if err != nil {
		t.Fatalf("RewriteTextIndex: %v", err)
	}
	if stats.Noop || stats.TermsRewritten == 0 || stats.StalePostingsPurged == 0 || stats.TombstoneDocIDEntriesPurged != 1 || stats.PostingBlocksWritten == 0 {
		t.Fatalf("rewrite stats=%+v want rewritten stale/tombstone purge", stats)
	}
	afterStats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats after: %v", err)
	}
	if afterStats.V2DeletedDocs != 0 || afterStats.V2MicroPostingBlocks != 0 || afterStats.V2RewriteMergeState != TextIndexRewriteMergeStateCompacted {
		t.Fatalf("after stats=%+v want compacted no tombstones/micro blocks", afterStats)
	}
	if afterStats.V2PostingBlocks >= beforeStats.V2PostingBlocks {
		t.Fatalf("posting blocks before=%d after=%d want coalesced fewer blocks", beforeStats.V2PostingBlocks, afterStats.V2PostingBlocks)
	}
	if status, err := col.TextIndexStatus("lexical"); err != nil || status.RewriteMergeState != TextIndexRewriteMergeStateReady || status.FailClosed {
		t.Fatalf("TextIndexStatus after=%+v err=%v want lightweight ready", status, err)
	}

	refund, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10})
	if err != nil {
		t.Fatalf("refund after rewrite detailed: %v", err)
	}
	if gotIDs := textSearchResultIDs2630(refund); !slicesEqualStrings(gotIDs, []string{"d4"}) || refund.Stats.TextMatchDetailsBuilt != 1 || len(refund.Results[0].TextMatches) == 0 {
		t.Fatalf("refund after response=%+v ids=%v want detailed d4 with lazy details", refund, gotIDs)
	}
	old, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "old OR delete", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("old/delete after rewrite: %v", err)
	}
	if len(old.Results) != 0 {
		t.Fatalf("old/delete results=%+v want stale terms purged", old.Results)
	}
	updated, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "updated", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil || len(updated.Results) != 1 || string(updated.Results[0].DocumentID) != "d1" {
		t.Fatalf("updated after rewrite response=%+v err=%v want d1", updated, err)
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened := openTextV2TestDB(t, dir, false)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	reopenedRefund, err := reopenedCol.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10})
	if err != nil {
		t.Fatalf("reopened refund: %v", err)
	}
	if gotIDs := textSearchResultIDs2630(reopenedRefund); !slicesEqualStrings(gotIDs, []string{"d4"}) || reopenedRefund.Stats.TextMatchDetailsBuilt != 1 {
		t.Fatalf("reopened refund=%+v ids=%v want d4 detailed", reopenedRefund, gotIDs)
	}
}

func TestTextV2RewritePurgesSameIDReinsertTombstones2630(t *testing.T) {
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
	if _, err := col.Insert([]byte("same"), []byte(`{"body":"old tombstone token"}`)); err != nil {
		t.Fatalf("Insert same old: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("same")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch same deleted=%d err=%v", deleted, err)
	}
	if _, err := col.Insert([]byte("same"), []byte(`{"body":"live replacement token"}`)); err != nil {
		t.Fatalf("Insert same replacement: %v", err)
	}

	beforeStats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats before: %v", err)
	}
	if beforeStats.V2DeletedDocs != 1 || beforeStats.V2RewriteMergeState != TextIndexRewriteMergeStatePending {
		t.Fatalf("before stats=%+v want one overwritten tombstone pending", beforeStats)
	}
	old, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "old", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil || len(old.Results) != 0 {
		t.Fatalf("old before rewrite response=%+v err=%v want no deleted result", old, err)
	}
	live, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "live", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil || len(live.Results) != 1 || string(live.Results[0].DocumentID) != "same" {
		t.Fatalf("live before rewrite response=%+v err=%v want replacement", live, err)
	}

	stats, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{TargetPostingsPerBlock: 8})
	if err != nil {
		t.Fatalf("RewriteTextIndex: %v", err)
	}
	if stats.TombstoneDocIDEntriesPurged != 0 || stats.TombstoneDocMapEntriesPurged != 1 || stats.TombstoneNormEntriesPurged != 1 {
		t.Fatalf("rewrite stats=%+v want same-ID tombstone purged from docmap/norm without docID tombstone", stats)
	}
	afterStats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats after: %v", err)
	}
	if afterStats.V2DeletedDocs != 0 || afterStats.V2RewriteMergeState != TextIndexRewriteMergeStateCompacted {
		t.Fatalf("after stats=%+v want compacted tombstone-free status", afterStats)
	}
	oldAfter, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "old", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil || len(oldAfter.Results) != 0 {
		t.Fatalf("old after rewrite response=%+v err=%v want no deleted result", oldAfter, err)
	}
	liveAfter, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "live", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil || len(liveAfter.Results) != 1 || string(liveAfter.Results[0].DocumentID) != "same" {
		t.Fatalf("live after rewrite response=%+v err=%v want replacement", liveAfter, err)
	}
}

func TestTextV2RewriteSnapshotBoundAndConcurrentServing2630(t *testing.T) {
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
	ids := make([][]byte, 64)
	docs := make([][]byte, 64)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%03d", i))
		docs[i] = []byte(fmt.Sprintf(`{"body":"refund common token%d"}`, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	def := TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}
	if _, _, err := col.CreateTextIndex(def); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	for i := 0; i < 16; i++ {
		id := []byte(fmt.Sprintf("doc-%03d", i))
		if _, _, err := col.Update(id, func([]byte) ([]byte, bool, error) {
			return []byte(`{"body":"updated common"}`), true, nil
		}); err != nil {
			t.Fatalf("Update %s: %v", id, err)
		}
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot nil")
	}
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("catalog: %v", err)
	}
	idx, ok := findTextIndex(catalog.meta.TextIndexes, "lexical")
	if !ok {
		_ = snap.Close()
		t.Fatal("missing lexical index")
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 4)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				got, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 5, ResultMode: TextSearchResultModeScoreOnly})
				if err != nil {
					errCh <- err
					return
				}
				if len(got.Results) == 0 || got.Stats.DocumentsFetched != 0 || got.Stats.TextMatchDetailsBuilt != 0 {
					errCh <- fmt.Errorf("unexpected concurrent search response %+v", got)
					return
				}
			}
		}()
	}
	if _, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{TargetPostingsPerBlock: 16}); err != nil {
		_ = snap.Close()
		t.Fatalf("RewriteTextIndex: %v", err)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent search: %v", err)
		}
	}

	oldResponse := TextSearchResponse{IndexName: "lexical"}
	oldResponse.Stats.QueryTerms = 1
	oldResponse.Stats.TextCandidatesRequested = 64
	old, err := executeTextV2SearchAtSnapshot(col, snap, catalog, idx, TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 64, ResultMode: TextSearchResultModeScoreOnly}, []string{"refund"}, TextSearchOperatorOR, 64, 256, textSearchResultScoreOnly, oldResponse)
	if closeErr := snap.Close(); closeErr != nil {
		t.Fatalf("snapshot close: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("old snapshot search: %v", err)
	}
	if len(old.Results) != 48 {
		t.Fatalf("old snapshot refund results=%d want 48", len(old.Results))
	}
	current, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 64, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("current refund search: %v", err)
	}
	if len(current.Results) != 48 || current.Stats.TextMatchDetailsBuilt != 0 || current.Stats.DocumentsFetched != 0 {
		t.Fatalf("current response=%+v want 48 score-only zero-doc", current)
	}
}

func TestTextV2RewriteStorageMaintenanceAndValueLogGC2630(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	prevGOMAXPROCS := runtime.GOMAXPROCS(2)
	t.Cleanup(func() { runtime.GOMAXPROCS(prevGOMAXPROCS) })

	dir := t.TempDir()
	d, closeDB := openTextV2PostingBlockCompressedDB2625(t, dir)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	ids := make([][]byte, 96)
	docs := make([][]byte, 96)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%03d", i))
		docs[i] = []byte(fmt.Sprintf(`{"body":"refund common policy %03d"}`, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}, StoragePolicy: RootStorageCompressed}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	for i := 0; i < 8; i++ {
		id := []byte(fmt.Sprintf("doc-%03d", i))
		if _, _, err := col.Update(id, func([]byte) ([]byte, bool, error) {
			return []byte(`{"body":"updated common policy"}`), true, nil
		}); err != nil {
			t.Fatalf("Update %s: %v", id, err)
		}
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("doc-008"), []byte("doc-009")}); err != nil || deleted != 2 {
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}
	rootName := collectionTextV2PostingBlocksRootName("docs", "lexical")
	oldMicroKey := firstTextV2PostingBlockKeyForKind2630(t, d, "docs", rootName, "updated", textV2PostingBlockKindMicro)
	if oldMicroKey == nil {
		t.Fatalf("missing updated micro block before rewrite")
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot nil")
	}

	stats, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{TargetPostingsPerBlock: 32})
	if err != nil {
		_ = snap.Close()
		t.Fatalf("RewriteTextIndex: %v", err)
	}
	if stats.PostingBlocksDeleted == 0 || stats.PostingBlocksWritten == 0 || stats.StalePostingsPurged == 0 {
		_ = snap.Close()
		t.Fatalf("rewrite stats=%+v want deleted/written/stale", stats)
	}
	withTextCatalog(t, d, "docs", func(current *backenddb.Snapshot, catalog *collectionCatalog) {
		if _, ok, err := collectionGetAppendAtCatalogRoot(current, catalog, rootName, oldMicroKey, nil); err != nil || ok {
			t.Fatalf("old micro key current ok=%v err=%v want unreachable", ok, err)
		}
	})
	oldCatalog, err := loadCollectionCatalog(snap, "docs")
	if err != nil {
		_ = snap.Close()
		t.Fatalf("old catalog: %v", err)
	}
	if raw, ok, err := collectionGetAppendAtCatalogRoot(snap, oldCatalog, rootName, oldMicroKey, nil); err != nil || !ok || len(raw) == 0 {
		_ = snap.Close()
		t.Fatalf("old snapshot micro ok=%v len=%d err=%v want pinned old payload", ok, len(raw), err)
	}
	// Settle the activated rewrite publication before destructive maintenance
	// captures its recoverable-root capability. The open snapshot still pins the
	// old text-v2 root and is the retention boundary this test exercises.
	if err := d.Checkpoint(); err != nil {
		_ = snap.Close()
		t.Fatalf("Checkpoint rewritten text-v2 index with snapshot pinned: %v", err)
	}
	pinnedLeafGC, err := d.LeafGenerationGC(context.Background(), backenddb.LeafGenerationGCOptions{})
	if err != nil {
		_ = snap.Close()
		t.Fatalf("LeafGenerationGC while text-v2 snapshot pinned: %v", err)
	}
	if pinnedLeafGC.GenerationsDeleted != 0 || pinnedLeafGC.BytesDeleted != 0 {
		_ = snap.Close()
		t.Fatalf("LeafGenerationGC deleted pinned text-v2 roots: %+v", pinnedLeafGC)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot close: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if _, err := d.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	compactStats, err := d.CompactStorage(context.Background(), backenddb.CompactStorageOptions{
		LeafPackMinExpectedReclaimBytes: 1,
		LeafPackMinReclaimPerCopyPPM:    1,
	})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if !compactStats.FullyCompacted {
		t.Fatalf("CompactStorage left text-v2 maintenance debt after snapshot release: leaf_gc=%+v remaining=%+v", compactStats.LeafGenerationGC, compactStats.RemainingDebt)
	}
	if got, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "updated", TopK: 10, ResultMode: TextSearchResultModeScoreOnly}); err != nil || len(got.Results) != 8 {
		t.Fatalf("post-maintenance updated response=%+v err=%v want 8", got, err)
	}
	if stats, err := col.TextIndexStorageStats("lexical"); err != nil || stats.V2RewriteMergeState != TextIndexRewriteMergeStateCompacted || stats.V2PostingBlocks == 0 || stats.V2NormBlocks == 0 || stats.V2DocMapBlocks == 0 {
		t.Fatalf("post-maintenance storage stats=%+v err=%v want compacted live roots", stats, err)
	}
	if err := closeDB(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, closeReopened := openTextV2PostingBlockCompressedDB2625(t, dir)
	defer func() { _ = closeReopened() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	if got, err := reopenedCol.SearchText(TextSearchOptions{IndexName: "lexical", Query: "updated", TopK: 10, ResultMode: TextSearchResultModeScoreOnly}); err != nil || len(got.Results) != 8 {
		t.Fatalf("reopened updated response=%+v err=%v want 8", got, err)
	}
}

func TestTextV2CoexistenceAndDefaultSelection2630(t *testing.T) {
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
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical_v1", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex explicit v1: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical_v2", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex explicit v2: %v", err)
	}
	v1Status, err := col.TextIndexStatus("lexical_v1")
	if err != nil {
		t.Fatalf("TextIndexStatus v1: %v", err)
	}
	v2Status, err := col.TextIndexStatus("lexical_v2")
	if err != nil {
		t.Fatalf("TextIndexStatus v2: %v", err)
	}
	if v1Status.Version != TextIndexVersionV1 || !v1Status.Ready || !slicesEqualStrings(v1Status.ActiveRootNames, collectionTextRootNames("docs", "lexical_v1")) {
		t.Fatalf("v1 status=%+v want explicit v1 roots", v1Status)
	}
	if v2Status.Version != TextIndexVersionV2 || !v2Status.Ready || v2Status.RewriteMergeState != TextIndexRewriteMergeStateReady || !slicesEqualStrings(v2Status.ActiveRootNames, collectionTextV2RootNames("docs", "lexical_v2")) {
		t.Fatalf("v2 status=%+v want explicit v2 roots", v2Status)
	}
	v1, err := col.SearchText(TextSearchOptions{IndexName: "lexical_v1", Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("SearchText v1: %v", err)
	}
	v2, err := col.SearchText(TextSearchOptions{IndexName: "lexical_v2", Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("SearchText v2: %v", err)
	}
	if len(v1.Results) != 1 || len(v2.Results) != 1 || !bytes.Equal(v1.Results[0].DocumentID, v2.Results[0].DocumentID) {
		t.Fatalf("coexistence v1=%+v v2=%+v want same refund doc", v1.Results, v2.Results)
	}
	if v2.Stats.TextStateLookups != 0 || v2.Stats.DocumentsFetched != 0 || v2.Stats.TextMatchDetailsBuilt != 0 {
		t.Fatalf("v2 stats=%+v want score-only zero-doc/no-state", v2.Stats)
	}
}

func BenchmarkTextV2RewriteMerge2630(b *testing.B) {
	docsN := textV2ContractEnvInt2623("TREEDB_TEXT_V2_REWRITE_DOCS", 512)
	if docsN < 32 {
		docsN = 32
	}
	updates := docsN / 8
	deletes := docsN / 16
	b.ReportAllocs()
	b.ReportMetric(float64(docsN), "docs/op")
	var last TextIndexRewriteStats
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		d, err := backenddb.Open(backenddb.Options{Dir: b.TempDir(), DisableBackgroundPrune: true})
		if err != nil {
			b.Fatalf("open db: %v", err)
		}
		mgr := NewCollectionManager(d)
		if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
			b.Fatalf("CreateCollection: %v", err)
		}
		col, err := mgr.OpenCollection("docs")
		if err != nil {
			b.Fatalf("OpenCollection: %v", err)
		}
		ids := make([][]byte, docsN)
		docs := make([][]byte, docsN)
		for j := range ids {
			ids[j] = []byte(fmt.Sprintf("doc-%06d-%d", j, i))
			docs[j] = []byte(fmt.Sprintf(`{"body":"refund common policy token%d"}`, j%31))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			b.Fatalf("InsertBatch: %v", err)
		}
		if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
			b.Fatalf("CreateTextIndex: %v", err)
		}
		for j := 0; j < updates; j++ {
			id := append([]byte(nil), ids[j]...)
			if _, _, err := col.Update(id, func([]byte) ([]byte, bool, error) {
				return []byte(`{"body":"updated common policy"}`), true, nil
			}); err != nil {
				b.Fatalf("Update: %v", err)
			}
		}
		deleteIDs := make([][]byte, deletes)
		for j := 0; j < deletes; j++ {
			deleteIDs[j] = ids[updates+j]
		}
		if deleted, err := col.DeleteBatch(deleteIDs); err != nil || deleted != deletes {
			b.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
		}
		b.StartTimer()
		stats, err := col.RewriteTextIndex("lexical", TextIndexRewriteOptions{})
		b.StopTimer()
		if err != nil {
			b.Fatalf("RewriteTextIndex: %v", err)
		}
		last = stats
		_ = d.Close()
	}
	b.ReportMetric(float64(last.PostingBlocksRead), "posting_blocks_read/op")
	b.ReportMetric(float64(last.PostingBlocksWritten), "posting_blocks_written/op")
	b.ReportMetric(float64(last.PostingBlocksDeleted), "posting_blocks_deleted/op")
	b.ReportMetric(float64(last.StalePostingsPurged), "stale_postings_purged/op")
	b.ReportMetric(float64(last.TombstoneDocIDEntriesPurged), "tombstones_purged/op")
}

func firstTextV2PostingBlockKeyForKind2630(t *testing.T, d *backenddb.DB, collection, rootName, term string, kind textV2PostingBlockKind) []byte {
	t.Helper()
	var out []byte
	withTextCatalog(t, d, collection, func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		if err := scanTextV2PostingBlocksForTerm(snap, catalog, rootName, term, func(key textV2PostingBlockKey, _ textV2PostingBlockSummary, scanner *textV2PostingBlockEntryScanner) error {
			if scanner.block.Kind == kind && out == nil {
				out = encodeTextV2PostingBlockKey(key.Term, key.BlockStart, key.BlockID)
			}
			return nil
		}); err != nil {
			t.Fatalf("scan posting blocks for %q kind %s: %v", term, kind, err)
		}
	})
	return out
}

func textSearchResultIDs2630(response TextSearchResponse) []string {
	ids := make([]string, 0, len(response.Results))
	for _, result := range response.Results {
		ids = append(ids, string(result.DocumentID))
	}
	return ids
}
