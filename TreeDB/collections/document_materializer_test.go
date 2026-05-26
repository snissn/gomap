package collections

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionReadViewFetchDocumentsByIDColumnReconstructionParity(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("e1"), []byte("e2")}
	docs := [][]byte{
		[]byte(`{"row_id":1,"kind":"alpha","score":1.5,"payload":"retained-a"}`),
		[]byte(`{"row_id":2,"kind":"beta","score":2.5,"payload":"retained-b"}`),
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	wantE1, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get e1: %v", err)
	}
	wantE2, err := col.Get([]byte("e2"))
	if err != nil {
		t.Fatalf("Get e2: %v", err)
	}

	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	got, err := view.FetchDocumentsByID([][]byte{[]byte("e2"), []byte("missing"), []byte("e1"), []byte("e1")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	if len(got.Results) != 4 {
		t.Fatalf("results=%d want 4", len(got.Results))
	}
	if !got.Results[0].Found || !bytes.Equal(got.Results[0].Document, wantE2) {
		t.Fatalf("e2 found=%t doc=%s want %s", got.Results[0].Found, got.Results[0].Document, wantE2)
	}
	if got.Results[1].Found || got.Results[1].Document != nil {
		t.Fatalf("missing result=%+v want not found", got.Results[1])
	}
	if !got.Results[2].Found || !bytes.Equal(got.Results[2].Document, wantE1) || !got.Results[3].Found || !bytes.Equal(got.Results[3].Document, wantE1) {
		t.Fatalf("e1 duplicate docs=%s/%s want %s", got.Results[2].Document, got.Results[3].Document, wantE1)
	}
	if got.Stats.DocumentsRequested != 4 || got.Stats.DocumentsFetched != 3 || got.Stats.DocumentsMissing != 1 {
		t.Fatalf("stats=%+v want requested=4 fetched=3 missing=1", got.Stats)
	}
	if got.Stats.RetainedPayloadFetches != 3 || got.Stats.VisibilityScans != 1 || got.Stats.VisibilityRowsScanned != 2 || got.Stats.VisibilityRows != 2 || got.Stats.JSONReconstructionRows != 3 {
		t.Fatalf("stats=%+v want retained fetches, one visibility scan over two row-asset rows, three reconstructions", got.Stats)
	}
	if got.Stats.TypedColumnRows != 3 || got.Stats.TypedColumnPartLoads == 0 || got.Stats.TypedColumnPartDecodes == 0 {
		t.Fatalf("stats=%+v want typed_column_part reconstruction counters", got.Stats)
	}

	rowRefFetch, err := view.FetchDocumentsByRowRef([]DocumentRowRef{got.Results[0].RowRef}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByRowRef: %v", err)
	}
	if len(rowRefFetch.Results) != 1 || !rowRefFetch.Results[0].Found || !bytes.Equal(rowRefFetch.Results[0].Document, wantE2) {
		t.Fatalf("row ref fetch=%+v want e2", rowRefFetch.Results)
	}
	badRef := got.Results[0].RowRef
	badRef.RowIndex++
	if _, err := view.FetchDocumentsByRowRef([]DocumentRowRef{badRef}, DocumentFetchOptions{}); err == nil || !strings.Contains(err.Error(), "row_index") {
		t.Fatalf("bad row ref err=%v want row_index mismatch", err)
	}
}

func TestCollectionReadViewReusesMappedAssetViewsAcrossFetches(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("e1"), []byte("e2")}
	if _, err := col.InsertBatch(ids, [][]byte{
		[]byte(`{"row_id":1,"kind":"alpha","score":1.5,"payload":"retained-a"}`),
		[]byte(`{"row_id":2,"kind":"beta","score":2.5,"payload":"retained-b"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	first, err := view.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		_ = view.Close()
		t.Fatalf("first FetchDocumentsByID: %v", err)
	}
	second, err := view.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		_ = view.Close()
		t.Fatalf("second FetchDocumentsByID: %v", err)
	}
	if first.Stats.AssetMmapHits == 0 {
		_ = view.Close()
		t.Skipf("mmap-backed column asset views unavailable; stats=%+v", first.Stats)
	}
	if first.Stats.AssetReadAtFallbacks != 0 || second.Stats.AssetReadAtFallbacks != 0 {
		_ = view.Close()
		t.Fatalf("unexpected read-at fallback first=%+v second=%+v", first.Stats, second.Stats)
	}
	if first.Stats.AssetFileOpens == 0 {
		_ = view.Close()
		t.Fatalf("first stats=%+v want asset file opens", first.Stats)
	}
	if second.Stats.AssetFileOpens != 0 {
		_ = view.Close()
		t.Fatalf("second stats=%+v want reusable read caches without file opens", second.Stats)
	}
	if second.Stats.AssetMmapHits == 0 || second.Stats.AssetActiveHandles == 0 {
		_ = view.Close()
		t.Fatalf("second stats=%+v want mmap hits and active handles", second.Stats)
	}
	for i := range first.Results {
		if !first.Results[i].Found || !second.Results[i].Found || !bytes.Equal(first.Results[i].Document, second.Results[i].Document) {
			_ = view.Close()
			t.Fatalf("result[%d] first=%+v second=%+v want identical documents", i, first.Results[i], second.Results[i])
		}
	}
	if err := view.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed := view.assetCounters()
	if closed.activeHandles != 0 {
		t.Fatalf("closed asset counters=%+v want no active handles", closed)
	}
	if closed.fileCloses < closed.fileOpens {
		t.Fatalf("closed asset counters=%+v want closes for opened files", closed)
	}
	if stats := view.assetManager.Stats(); stats.ActiveHandles != 0 || stats.ActiveMappedBytes != 0 || stats.ActiveHeapCopyBytes != 0 {
		t.Fatalf("mappedresource stats after Close=%+v want released handles", stats)
	}
}

func TestCollectionReadViewForcedReadAtFallbackReturnsIdenticalDocuments(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("e1"), []byte("e2")}
	if _, err := col.InsertBatch(ids, [][]byte{
		[]byte(`{"row_id":1,"kind":"alpha","score":1.5,"payload":"retained-a"}`),
		[]byte(`{"row_id":2,"kind":"beta","score":2.5,"payload":"retained-b"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	view.forceAssetReadAtFallbackForTest = true
	defer func() { _ = view.Close() }()
	first, err := view.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("first FetchDocumentsByID: %v", err)
	}
	second, err := view.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("second FetchDocumentsByID: %v", err)
	}
	if first.Stats.AssetMmapHits != 0 || second.Stats.AssetMmapHits != 0 || first.Stats.AssetReadAtFallbacks == 0 || second.Stats.AssetReadAtFallbacks == 0 {
		t.Fatalf("fallback stats first=%+v second=%+v want forced read-at fallback only", first.Stats, second.Stats)
	}
	if second.Stats.AssetFileOpens != 0 {
		t.Fatalf("second stats=%+v want fallback read caches reused", second.Stats)
	}
	for i := range first.Results {
		if !first.Results[i].Found || !second.Results[i].Found || !bytes.Equal(first.Results[i].Document, second.Results[i].Document) {
			t.Fatalf("result[%d] first=%+v second=%+v want identical fallback documents", i, first.Results[i], second.Results[i])
		}
	}
}

func TestCollectionReadViewMappedPinsProtectRewriteCandidates(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("e1"), []byte("e2")},
		[][]byte{
			[]byte(`{"row_id":1,"kind":"alpha","score":1.5,"payload":"retained-a"}`),
			[]byte(`{"row_id":2,"kind":"beta","score":2.5,"payload":"retained-b"}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(beforeRefs) == 0 {
		t.Fatal("manifest refs empty, test requires live physical assets")
	}
	candidate := writeDocumentMaterializerCandidateAsset(t, d, col, 3, 99)
	if candidate.FileID != beforeRefs[0].FileID {
		t.Fatalf("candidate file_id=%d live file_id=%d, test requires mixed segment", candidate.FileID, beforeRefs[0].FileID)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	if got, err := view.FetchDocumentsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{}); err != nil || got.Stats.AssetActiveHandles == 0 {
		_ = view.Close()
		t.Fatalf("FetchDocumentsByID stats=%+v err=%v want active materializer handles", got.Stats, err)
	}
	pinned, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		_ = view.Close()
		t.Fatalf("ColumnAssetRewrite while materializer pin active: %v", err)
	}
	if pinned.SegmentsEligible != 0 || pinned.SegmentsRewritten != 0 || pinned.RefsEligible != 0 || pinned.RefsRemapped != 0 {
		_ = view.Close()
		t.Fatalf("pinned rewrite stats=%+v want materializer pin to skip segment", pinned)
	}
	if pinned.Plan.MappedResources.ActiveHandles == 0 || pinned.Plan.MappedResources.PinnedRefs == 0 || pinned.Plan.Sources.MappedResourcePins == 0 {
		_ = view.Close()
		t.Fatalf("mappedresource stats=%+v sources=%+v want active materializer pin", pinned.Plan.MappedResources, pinned.Plan.Sources)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if stats := view.assetManager.Stats(); stats.ActiveHandles != 0 {
		t.Fatalf("mappedresource stats after close=%+v want released pins", stats)
	}
}

func TestColumnPhysicalScanReadCacheIntegrityMismatchFails(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	if _, err := col.Insert([]byte("e1"), []byte(`{"row_id":1,"kind":"alpha","score":1.5,"payload":"retained-a"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	view, closeView, err := col.prepareColumnPhysicalScanSnapshotView()
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	defer closeView()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, ColumnAssetReadIntegritySkipChecksums)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetReadCacheWithIntegrity: %v", err)
	}
	defer func() { _ = readCache.close() }()
	_, err = col.scanColumnPhysicalRowsInSnapshotView(view, columnPhysicalScanRequest{
		ReadIntegrity: ColumnAssetReadIntegrityVerify,
		ReadCache:     &readCache,
		Visitor:       func(columnPhysicalScanRowView) error { return nil },
	})
	if err == nil || !strings.Contains(err.Error(), "does not match request integrity") {
		t.Fatalf("scan err=%v want integrity mismatch", err)
	}
}

func TestCollectionReadViewFetchDocumentsByRowRefRequiresTypedStorage(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "plain", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("plain")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.Insert([]byte("e1"), []byte(`{"name":"plain"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	_, err = view.FetchDocumentsByRowRef([]DocumentRowRef{{DocumentID: []byte("e1"), RowIndex: 0}}, DocumentFetchOptions{})
	if err == nil || !strings.Contains(err.Error(), "typed-storage reconstruction") {
		t.Fatalf("FetchDocumentsByRowRef err=%v want typed-storage reconstruction error", err)
	}
}

func TestCollectionReadViewClosedAndNilFailClosed(t *testing.T) {
	var nilView *CollectionReadView
	if _, err := nilView.FetchDocumentsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{}); err == nil || !strings.Contains(err.Error(), "nil collection read view") {
		t.Fatalf("nil FetchDocumentsByID err=%v want fail closed", err)
	}
	if _, err := nilView.FetchDocumentsByRowRef([]DocumentRowRef{{DocumentID: []byte("e1"), RowIndex: 0}}, DocumentFetchOptions{}); err == nil || !strings.Contains(err.Error(), "nil collection read view") {
		t.Fatalf("nil FetchDocumentsByRowRef err=%v want fail closed", err)
	}

	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := view.FetchDocumentsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{}); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("closed FetchDocumentsByID err=%v want fail closed", err)
	}
}

func TestCollectionReadViewMissingAndDeletedMatchCollectionGet(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("e1"), []byte("e2")},
		[][]byte{
			[]byte(`{"row_id":1,"kind":"alpha","score":1,"payload":"live"}`),
			[]byte(`{"row_id":2,"kind":"beta","score":2,"payload":"deleted"}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := col.Delete([]byte("e2")); err != nil {
		t.Fatalf("Delete e2: %v", err)
	}
	if got, err := col.Get([]byte("e2")); err != nil || got != nil {
		t.Fatalf("Get deleted=%s err=%v want missing", got, err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	got, err := view.FetchDocumentsByID([][]byte{[]byte("e1"), []byte("e2"), []byte("missing")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	if !got.Results[0].Found || got.Results[1].Found || got.Results[2].Found {
		t.Fatalf("results=%+v want only e1 found", got.Results)
	}
	if got.Stats.DocumentsFetched != 1 || got.Stats.DocumentsMissing != 2 {
		t.Fatalf("stats=%+v want one fetched and two missing", got.Stats)
	}
}

func TestCollectionReadViewBoundSnapshotIgnoresLaterWrites(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	oldDoc := []byte(`{"row_id":1,"kind":"old","score":1,"payload":"before"}`)
	if _, err := col.Insert([]byte("e1"), oldDoc); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	if err := col.Delete([]byte("e1")); err != nil {
		t.Fatalf("Delete after read-view open: %v", err)
	}
	if live, err := col.Get([]byte("e1")); err != nil || live != nil {
		t.Fatalf("live Get after delete=%s err=%v want missing", live, err)
	}
	got, err := view.FetchDocumentsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	if len(got.Results) != 1 || !got.Results[0].Found {
		t.Fatalf("results=%+v want old snapshot document", got.Results)
	}
	want, err := reconstructDocumentMaterializerFixtureDoc(oldDoc)
	if err != nil {
		t.Fatalf("reconstruct old fixture: %v", err)
	}
	if !bytes.Equal(got.Results[0].Document, want) {
		t.Fatalf("snapshot document=%s want %s", got.Results[0].Document, want)
	}
}

func TestCollectionReadViewResponseDocumentsAreOwned(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("e1"), []byte("e2")},
		[][]byte{
			[]byte(`{"row_id":1,"kind":"alpha","score":1,"payload":"first"}`),
			[]byte(`{"row_id":2,"kind":"beta","score":2,"payload":"second"}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	got, err := view.FetchDocumentsByID([][]byte{[]byte("e1"), []byte("e2")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	secondBefore := append([]byte(nil), got.Results[1].Document...)
	_ = append(got.Results[0].Document, '!')
	if !bytes.Equal(got.Results[1].Document, secondBefore) {
		t.Fatalf("second document changed after appending to first: got %s want %s", got.Results[1].Document, secondBefore)
	}
	got.Results[0].Document[0] = 'X'
	fresh, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get e1 after mutating response: %v", err)
	}
	if len(fresh) == 0 || fresh[0] == 'X' {
		t.Fatalf("mutating response document affected stored document: %s", fresh)
	}
}

func TestCollectionReadViewFetchDocumentsByRowRefPointFetchInsertOnly1874(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("e1"), []byte("e2"), []byte("e3")}
	docs := [][]byte{
		[]byte(`{"row_id":1,"kind":"alpha","score":1,"payload":"first"}`),
		[]byte(`{"row_id":2,"kind":"beta","score":2,"payload":"second"}`),
		[]byte(`{"row_id":3,"kind":"gamma","score":3,"payload":"third"}`),
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	want := make(map[string][]byte, len(ids))
	for _, id := range ids {
		doc, err := col.Get(id)
		if err != nil {
			t.Fatalf("Get %q: %v", id, err)
		}
		want[string(id)] = doc
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	resolved, err := view.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	refsByID := make(map[string]DocumentRowRef, len(resolved.Results))
	for i := range resolved.Results {
		if !resolved.Results[i].Found {
			t.Fatalf("resolved[%d] missing", i)
		}
		refsByID[string(resolved.Results[i].ID)] = resolved.Results[i].RowRef
	}
	orderedRefs := []DocumentRowRef{refsByID["e3"], refsByID["e1"], refsByID["e3"], refsByID["e2"]}
	got, err := view.FetchDocumentsByRowRef(orderedRefs, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByRowRef: %v", err)
	}
	wantIDs := []string{"e3", "e1", "e3", "e2"}
	if len(got.Results) != len(wantIDs) {
		t.Fatalf("results=%d want %d", len(got.Results), len(wantIDs))
	}
	for i, wantID := range wantIDs {
		if !got.Results[i].Found || !bytes.Equal(got.Results[i].ID, []byte(wantID)) || !bytes.Equal(got.Results[i].Document, want[wantID]) {
			t.Fatalf("result[%d]=%+v doc=%s want id=%s doc=%s", i, got.Results[i], got.Results[i].Document, wantID, want[wantID])
		}
	}
	if got.Stats.PointRowFetches != uint64(len(orderedRefs)) || got.Stats.PointRowDecodes != uint64(len(orderedRefs)) {
		t.Fatalf("stats=%+v want one point fetch/decode per ref", got.Stats)
	}
	if got.Stats.RowRefFallbackScans != 0 || got.Stats.VisibilityScans != 0 || got.Stats.VisibilityRowsScanned != 0 {
		t.Fatalf("stats=%+v want direct row-ref point fetch without visibility scan fallback", got.Stats)
	}
}

func TestCollectionReadViewFetchDocumentsByRowRefMutationLatestVisible1874(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	oldE1Doc := []byte(`{"row_id":1,"kind":"old","score":1,"payload":"before"}`)
	oldE2Doc := []byte(`{"row_id":2,"kind":"keep","score":2,"payload":"delete-me"}`)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{oldE1Doc, oldE2Doc}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	oldE1, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get old e1: %v", err)
	}
	oldE2, err := col.Get([]byte("e2"))
	if err != nil {
		t.Fatalf("Get old e2: %v", err)
	}
	oldView, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("Open old read view: %v", err)
	}
	defer func() { _ = oldView.Close() }()
	oldResolved, err := oldView.FetchDocumentsByID([][]byte{[]byte("e1"), []byte("e2")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("old FetchDocumentsByID: %v", err)
	}
	oldE1Ref := oldResolved.Results[0].RowRef
	oldE2Ref := oldResolved.Results[1].RowRef

	newE1Doc := []byte(`{"row_id":1,"kind":"new","score":11,"payload":"after"}`)
	matched, modified, err := col.Update([]byte("e1"), func([]byte) ([]byte, bool, error) { return newE1Doc, true, nil })
	if err != nil || !matched || !modified {
		t.Fatalf("Update e1 matched=%t modified=%t err=%v", matched, modified, err)
	}
	if err := col.Delete([]byte("e2")); err != nil {
		t.Fatalf("Delete e2: %v", err)
	}
	newE1, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get new e1: %v", err)
	}
	if liveE2, err := col.Get([]byte("e2")); err != nil || liveE2 != nil {
		t.Fatalf("Get deleted e2=%s err=%v want missing", liveE2, err)
	}

	currentView, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("Open current read view: %v", err)
	}
	defer func() { _ = currentView.Close() }()
	lookup, err := currentView.LookupDocumentRowRefsByID([][]byte{[]byte("e1"), []byte("e2")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("LookupDocumentRowRefsByID: %v", err)
	}
	if !lookup.Results[0].Found || lookup.Results[1].Found || lookup.Stats.RowLocatorLookups != 2 || lookup.Stats.RowLocatorMisses != 1 || lookup.Stats.RowLocatorBuilds != 1 {
		t.Fatalf("lookup=%+v stats=%+v want updated e1 ref and deleted e2 miss", lookup.Results, lookup.Stats)
	}
	current, err := currentView.FetchDocumentsByRowRef([]DocumentRowRef{lookup.Results[0].RowRef}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("current FetchDocumentsByRowRef: %v", err)
	}
	if len(current.Results) != 1 || !current.Results[0].Found || !bytes.Equal(current.Results[0].Document, newE1) {
		t.Fatalf("current row-ref doc=%s want %s", current.Results[0].Document, newE1)
	}
	if current.Stats.RowRefFallbackScans != 0 || current.Stats.PointRowFetches != 1 || current.Stats.PointRowDecodes != 1 {
		t.Fatalf("current stats=%+v want direct point fetch", current.Stats)
	}
	if _, err := currentView.FetchDocumentsByRowRef([]DocumentRowRef{oldE1Ref}, DocumentFetchOptions{}); err == nil || !strings.Contains(err.Error(), "latest-visible") {
		t.Fatalf("old e1 ref on current view err=%v want latest-visible fail closed", err)
	}
	if _, err := currentView.FetchDocumentsByRowRef([]DocumentRowRef{oldE2Ref}, DocumentFetchOptions{}); err == nil || !strings.Contains(err.Error(), "not visible") {
		t.Fatalf("old e2 ref on current view err=%v want deleted/missing fail closed", err)
	}

	stale, err := oldView.FetchDocumentsByRowRef([]DocumentRowRef{oldE1Ref, oldE2Ref}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("old snapshot FetchDocumentsByRowRef after later writes: %v", err)
	}
	if len(stale.Results) != 2 || !bytes.Equal(stale.Results[0].Document, oldE1) || !bytes.Equal(stale.Results[1].Document, oldE2) {
		t.Fatalf("stale docs=%s/%s want old %s/%s", stale.Results[0].Document, stale.Results[1].Document, oldE1, oldE2)
	}
}

func TestCollectionReadViewFetchDocumentsByRowRefMismatchFailsClosed1874(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("e1"), []byte("e2")},
		[][]byte{
			[]byte(`{"row_id":1,"kind":"alpha","score":1,"payload":"first"}`),
			[]byte(`{"row_id":2,"kind":"beta","score":2,"payload":"second"}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	resolved, err := view.FetchDocumentsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	ref := resolved.Results[0].RowRef
	tests := []struct {
		name string
		mut  func(DocumentRowRef) DocumentRowRef
		want string
	}{
		{name: "row_index", mut: func(r DocumentRowRef) DocumentRowRef { r.RowIndex = 99; return r }, want: "row_index"},
		{name: "generation", mut: func(r DocumentRowRef) DocumentRowRef { r.Generation += 100; return r }, want: "generation"},
		{name: "part", mut: func(r DocumentRowRef) DocumentRowRef { r.PartID += 100; return r }, want: "part_id"},
		{name: "lsn", mut: func(r DocumentRowRef) DocumentRowRef { r.AppliedCommandLSN++; return r }, want: "applied_command_lsn"},
		{name: "document_id", mut: func(r DocumentRowRef) DocumentRowRef { r.DocumentID = []byte("e2"); return r }, want: "id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := view.FetchDocumentsByRowRef([]DocumentRowRef{tt.mut(ref)}, DocumentFetchOptions{})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("FetchDocumentsByRowRef err=%v want %q", err, tt.want)
			}
		})
	}
}

func TestCollectionReadViewFetchDocumentsByRowRefResponseDocumentsAreOwned1874(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("e1"), []byte("e2")},
		[][]byte{
			[]byte(`{"row_id":1,"kind":"alpha","score":1,"payload":"first"}`),
			[]byte(`{"row_id":2,"kind":"beta","score":2,"payload":"second"}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	resolved, err := view.FetchDocumentsByID([][]byte{[]byte("e1"), []byte("e2")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	got, err := view.FetchDocumentsByRowRef([]DocumentRowRef{resolved.Results[0].RowRef, resolved.Results[1].RowRef}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByRowRef: %v", err)
	}
	secondBefore := append([]byte(nil), got.Results[1].Document...)
	_ = append(got.Results[0].Document, '!')
	if !bytes.Equal(got.Results[1].Document, secondBefore) {
		t.Fatalf("second document changed after appending to first: got %s want %s", got.Results[1].Document, secondBefore)
	}
	got.Results[0].Document[0] = 'X'
	fresh, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get e1 after mutating response: %v", err)
	}
	if len(fresh) == 0 || fresh[0] == 'X' {
		t.Fatalf("mutating row-ref response document affected stored document: %s", fresh)
	}
}

func TestCollectionReadViewEnsureAssetReadCachesInvalidatesDerivedRowCaches1874(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("e1"), []byte("e2")},
		[][]byte{
			[]byte(`{"row_id":1,"kind":"alpha","score":1,"payload":"first"}`),
			[]byte(`{"row_id":2,"kind":"beta","score":2,"payload":"second"}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	lookup, err := view.LookupDocumentRowRefsByID([][]byte{[]byte("e1"), []byte("e2")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("LookupDocumentRowRefsByID: %v", err)
	}
	if _, err := view.FetchDocumentsByRowRef([]DocumentRowRef{lookup.Results[0].RowRef}, DocumentFetchOptions{}); err != nil {
		t.Fatalf("FetchDocumentsByRowRef: %v", err)
	}
	if view.rowLocator == nil || view.columnSnapshotView == nil || len(view.pointRowRefs) == 0 || len(view.pointRowBlocks) == 0 || view.pointRowProjection == nil {
		t.Fatalf("expected derived caches to be populated before integrity change")
	}
	cfg := view.columnSnapshotView.Config
	if err := view.ensureAssetReadCaches(cfg, ColumnAssetReadIntegritySkipChecksums); err != nil {
		t.Fatalf("ensureAssetReadCaches: %v", err)
	}
	if view.rowLocator != nil {
		t.Fatalf("rowLocator=%v want nil after row asset cache rebuild", view.rowLocator)
	}
	if view.columnSnapshotView != nil {
		t.Fatalf("columnSnapshotView=%v want nil after row asset cache rebuild", view.columnSnapshotView)
	}
	if view.pointRowRefs != nil {
		t.Fatalf("pointRowRefs=%v want nil after row asset cache rebuild", view.pointRowRefs)
	}
	if view.pointRowProjection != nil {
		t.Fatalf("pointRowProjection=%v want nil after row asset cache rebuild", view.pointRowProjection)
	}
	if len(view.pointRowBlocks) != 0 {
		t.Fatalf("pointRowBlocks=%d want empty after row asset cache rebuild", len(view.pointRowBlocks))
	}
	lookup, err = view.LookupDocumentRowRefsByID([][]byte{[]byte("e1"), []byte("e2")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("LookupDocumentRowRefsByID after rebuild: %v", err)
	}
	if _, err := view.FetchDocumentsByRowRef([]DocumentRowRef{lookup.Results[0].RowRef}, DocumentFetchOptions{}); err != nil {
		t.Fatalf("FetchDocumentsByRowRef after rebuild: %v", err)
	}
	if view.rowLocator == nil || view.columnSnapshotView == nil || len(view.pointRowRefs) == 0 || len(view.pointRowBlocks) == 0 || view.pointRowProjection == nil {
		t.Fatalf("expected derived caches to be repopulated before close")
	}
	if err := view.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if view.rowLocator != nil || view.columnSnapshotView != nil || view.pointRowRefs != nil || view.pointRowProjection != nil || view.pointRowBlocks != nil {
		t.Fatalf("derived caches retained after Close: rowLocator=%v columnSnapshotView=%v pointRowRefs=%v pointRowProjection=%v pointRowBlocks=%v", view.rowLocator, view.columnSnapshotView, view.pointRowRefs, view.pointRowProjection, view.pointRowBlocks)
	}
}

func TestDocumentRowLocatorCandidateNewerUsesOrdinalTieBreaker1874(t *testing.T) {
	base := DocumentRowRef{Generation: 2, PartID: 7, RowIndex: 11, AppliedCommandLSN: 42}
	older := documentRowLocatorCandidate{ref: base, ordinal: 3}
	newer := documentRowLocatorCandidate{ref: base, ordinal: 4}
	if !documentRowLocatorCandidateNewer(newer, older) {
		t.Fatalf("higher ordinal should win exact row-ref ties")
	}
	if documentRowLocatorCandidateNewer(older, newer) {
		t.Fatalf("lower ordinal should not win exact row-ref ties")
	}
}

func BenchmarkCollectionReadViewFetchDocumentsByIDMaterializer(b *testing.B) {
	benchmarkCollectionReadViewFetchDocumentsByIDMaterializer(b, false)
}

func BenchmarkCollectionReadViewFetchDocumentsByIDMaterializerReadAtFallback(b *testing.B) {
	benchmarkCollectionReadViewFetchDocumentsByIDMaterializer(b, true)
}

func benchmarkCollectionReadViewFetchDocumentsByIDMaterializer(b *testing.B, forceReadAtFallback bool) {
	b.Helper()
	const rows = 1024
	d, col := newDocumentMaterializerTestCollection(b)
	defer func() { _ = d.Close() }()
	ids := make([][]byte, rows)
	docs := make([][]byte, rows)
	for i := 0; i < rows; i++ {
		ids[i] = []byte(fmt.Sprintf("e%04d", i))
		docs[i] = []byte(fmt.Sprintf(`{"row_id":%d,"kind":"kind-%d","score":%0.1f,"payload":"retained-%d"}`, i, i%8, float64(i)+0.5, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		b.Fatalf("InsertBatch: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		b.Fatalf("OpenCollectionReadView: %v", err)
	}
	view.forceAssetReadAtFallbackForTest = forceReadAtFallback
	defer func() { _ = view.Close() }()
	fetchIDs := [][]byte{ids[37], ids[128], ids[255], ids[512], ids[700], ids[900], ids[1000], ids[3], ids[44], ids[88]}
	if _, err := view.FetchDocumentsByID(fetchIDs, DocumentFetchOptions{}); err != nil {
		b.Fatalf("warm FetchDocumentsByID: %v", err)
	}
	measured, err := view.FetchDocumentsByID(fetchIDs, DocumentFetchOptions{})
	if err != nil {
		b.Fatalf("measure FetchDocumentsByID: %v", err)
	}
	stats := measured.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := view.FetchDocumentsByID(fetchIDs, DocumentFetchOptions{})
		if err != nil {
			b.Fatalf("FetchDocumentsByID: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
	}
	b.StopTimer()
	reportDocumentMaterializerBenchMetrics(b, stats)
}

func reportDocumentMaterializerBenchMetrics(b *testing.B, stats DocumentMaterializationStats) {
	b.Helper()
	b.ReportMetric(float64(stats.DocumentsFetched), "docs_fetched/fetch")
	b.ReportMetric(float64(stats.DocumentBytes), "doc_B/fetch")
	b.ReportMetric(float64(stats.FetchNanos), "fetch_ns/fetch")
	b.ReportMetric(float64(stats.RetainedPayloadFetches), "retained_fetches/fetch")
	b.ReportMetric(float64(stats.VisibilityRowsScanned), "visibility_rows_scanned/fetch")
	b.ReportMetric(float64(stats.VisibilityPhysicalBytes), "visibility_physical_B/fetch")
	b.ReportMetric(float64(stats.VisibilityNanos), "visibility_ns/fetch")
	b.ReportMetric(float64(stats.TypedColumnRows), "typed_column_rows/fetch")
	b.ReportMetric(float64(stats.TypedColumnCacheHits), "typed_column_cache_hits/fetch")
	b.ReportMetric(float64(stats.TypedColumnCacheMisses), "typed_column_cache_misses/fetch")
	b.ReportMetric(float64(stats.TypedColumnPartLoads), "typed_column_part_loads/fetch")
	b.ReportMetric(float64(stats.TypedColumnPartDecodes), "typed_column_part_decodes/fetch")
	b.ReportMetric(float64(stats.TypedColumnNanos), "typed_column_ns/fetch")
	b.ReportMetric(float64(stats.JSONReconstructionRows), "json_reconstruction_rows/fetch")
	b.ReportMetric(float64(stats.JSONReconstructionNanos), "json_reconstruction_ns/fetch")
	b.ReportMetric(float64(stats.RowLocatorBuilds), "row_locator_builds/fetch")
	b.ReportMetric(float64(stats.RowLocatorLookups), "row_locator_lookups/fetch")
	b.ReportMetric(float64(stats.RowLocatorMisses), "row_locator_misses/fetch")
	b.ReportMetric(float64(stats.RowLocatorRowsScanned), "row_locator_rows_scanned/fetch")
	b.ReportMetric(float64(stats.RowLocatorPhysicalBytes), "row_locator_physical_B/fetch")
	b.ReportMetric(float64(stats.RowLocatorNanos), "row_locator_ns/fetch")
	b.ReportMetric(float64(stats.PointRowFetches), "point_row_fetches/fetch")
	b.ReportMetric(float64(stats.PointRowDecodes), "point_row_decodes/fetch")
	b.ReportMetric(float64(stats.RowRefFallbackScans), "row_ref_fallback_scans/fetch")
	b.ReportMetric(float64(stats.RowRefUnsupported), "row_ref_unsupported/fetch")
	b.ReportMetric(float64(stats.RowRefValidationFailures), "row_ref_validation_failures/fetch")
	b.ReportMetric(float64(stats.AssetMmapHits), "asset_mmap_hits/fetch")
	b.ReportMetric(float64(stats.AssetReadAtFallbacks), "asset_readat_fallbacks/fetch")
	b.ReportMetric(float64(stats.AssetFileOpens), "asset_file_opens/fetch")
	b.ReportMetric(float64(stats.AssetFileCloses), "asset_file_closes/fetch")
	b.ReportMetric(float64(stats.AssetActiveHandles), "asset_active_handles")
}

func newDocumentMaterializerTestCollection(t testing.TB) (*backenddb.DB, *Collection) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "score", Path: "score", ValueType: ColumnStoreValueDouble, Owner: TypedStorageOwnerColumnPart},
	}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: cfg}}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	return d, col
}

func writeDocumentMaterializerCandidateAsset(t testing.TB, d *backenddb.DB, col *Collection, generation, partID uint64) ColumnAssetRef {
	t.Helper()
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil {
		t.Fatalf("missing column store config: %+v", cfg)
	}
	encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        col.Meta().Name,
		Namespace:         cfg.AssetManager.Namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: d.State().AppliedCommandLSN + 1,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        cfg.SchemaHash,
		Columns:           columnStoreRowAssetConfig(*cfg).Columns,
		Rows: []columnDeclaredRow{{
			ID: []byte("candidate"),
			Values: []columnDeclaredValue{
				{Type: ColumnStoreValueInt64, Present: true, Int64: int64(generation)},
			},
		}},
	})
	if err != nil {
		t.Fatalf("encode candidate asset: %v", err)
	}
	ref, err := writeColumnPhysicalAssetToManager(d.ColumnAssetRootDir(), *cfg, encoded, generation, partID)
	if err != nil {
		t.Fatalf("write candidate asset: %v", err)
	}
	return ref
}

func reconstructDocumentMaterializerFixtureDoc(doc []byte) ([]byte, error) {
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "score", Path: "score", ValueType: ColumnStoreValueDouble, Owner: TypedStorageOwnerColumnPart},
	}
	cfg.RetainedPayload = ColumnRetainedPayloadNonColumn
	retained, err := columnRetainedPayloadFromJSONDocument(*cfg, doc)
	if err != nil {
		return nil, err
	}
	return reconstructColumnJSONDocument(*cfg, retained, []columnDeclaredValue{
		{Type: ColumnStoreValueInt64, Int64: 1, Present: true},
		{Type: ColumnStoreValueString, String: "old", Present: true},
		{Type: ColumnStoreValueDouble, Double: 1, Present: true},
	})
}
