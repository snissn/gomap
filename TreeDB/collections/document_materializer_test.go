package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionReadViewFetchDocumentsByIDUsesBatchViewForOwnedRetainedPayloads2242(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir: t.TempDir(),
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	docA := []byte(`{"kind":"alpha","payload":"` + strings.Repeat("a", 128) + `"}`)
	docB := []byte(`{}`)
	if _, err := col.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{docA, docB}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	got, err := view.FetchDocumentsByID([][]byte{[]byte("a"), []byte("missing"), []byte("b"), []byte("a")}, DocumentFetchOptions{})
	if err != nil {
		_ = view.Close()
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: 8}
	if _, err := view.FetchDocumentsByID([][]byte{[]byte("a"), []byte("b"), []byte("a"), []byte("b")}, DocumentFetchOptions{Context: ctx}); !errors.Is(err, context.Canceled) || ctx.calls != ctx.cancelAfter {
		_ = view.Close()
		t.Fatalf("FetchDocumentsByID cancellation err=%v calls=%d", err, ctx.calls)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("Close view: %v", err)
	}
	if len(got.Results) != 4 {
		t.Fatalf("results=%d want 4", len(got.Results))
	}
	if !got.Results[0].Found || !bytes.Equal(got.Results[0].Document, docA) || got.Results[1].Found || got.Results[1].Document != nil || !got.Results[2].Found || !bytes.Equal(got.Results[2].Document, docB) || !got.Results[3].Found || !bytes.Equal(got.Results[3].Document, docA) {
		t.Fatalf("unexpected results: %+v", got.Results)
	}
	if got.Stats.DocumentsRequested != 4 || got.Stats.DocumentsFetched != 3 || got.Stats.DocumentsMissing != 1 || got.Stats.RetainedPayloadFetches != 3 {
		t.Fatalf("stats=%+v want requested=4 fetched=3 missing=1 retained=3", got.Stats)
	}
	if len(got.Results[0].Document) > 0 {
		got.Results[0].Document[0] = '['
	}
	fresh, err := col.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get after response mutation: %v", err)
	}
	if !bytes.Equal(fresh, docA) {
		t.Fatalf("mutating response affected stored document: got=%s want=%s", fresh, docA)
	}
}

func TestCollectionReadViewLookupDocumentRowRefsByIDEmptyCollectionReturnsMissingP3890(t *testing.T) {
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 0, nil)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex empty collection: %v", err)
	}

	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()

	ids := [][]byte{[]byte("missing-a"), []byte("missing-b")}
	got, err := view.LookupDocumentRowRefsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("LookupDocumentRowRefsByID: %v", err)
	}
	if len(got.Results) != len(ids) {
		t.Fatalf("results=%d want %d", len(got.Results), len(ids))
	}
	for i := range ids {
		if got.Results[i].Found || !bytes.Equal(got.Results[i].ID, ids[i]) {
			t.Fatalf("result[%d]=%+v want owned missing id %q", i, got.Results[i], ids[i])
		}
	}
	if got.Stats.DocumentsRequested != uint64(len(ids)) ||
		got.Stats.RowLocatorLookups != uint64(len(ids)) ||
		got.Stats.RowLocatorMisses != uint64(len(ids)) {
		t.Fatalf("stats=%+v want requested/lookups/misses=%d", got.Stats, len(ids))
	}
}

func TestCollectionReadViewLookupDocumentRowRefsByIDMissingLocatorWithPrimaryFailsClosedP3890(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("e1")},
		[][]byte{[]byte(`{"row_id":1,"kind":"alpha","score":1.5}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	catalogWithoutLocator := *view.catalog
	catalogWithoutLocator.roots = make(map[string]uint64, len(view.catalog.roots))
	for name, rootID := range view.catalog.roots {
		catalogWithoutLocator.roots[name] = rootID
	}
	delete(catalogWithoutLocator.roots, collectionColumnRowLocatorRootName(view.catalog.meta.Name))
	view.catalog = &catalogWithoutLocator

	if _, err := view.LookupDocumentRowRefsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{}); err == nil ||
		!strings.Contains(err.Error(), "primary row locator root is absent") {
		t.Fatalf("LookupDocumentRowRefsByID err=%v want absent-locator fail-closed error", err)
	}
}

func TestCollectionReadViewLookupDocumentRowRefsByIDMissingLocatorWithPrimaryOverlayFailsClosedP3890(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("e1")},
		[][]byte{[]byte(`{"row_id":1,"kind":"alpha","score":1.5}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	primaryRootName := collectionPrimaryRootName(view.catalog.meta.Name)
	primaryRootID := view.catalog.rootID(primaryRootName)
	if primaryRootID == 0 {
		t.Fatal("test requires a populated primary root")
	}
	catalogWithPrimaryOverlay := *view.catalog
	catalogWithPrimaryOverlay.roots = make(map[string]uint64, len(view.catalog.roots))
	for name, rootID := range view.catalog.roots {
		catalogWithPrimaryOverlay.roots[name] = rootID
	}
	delete(catalogWithPrimaryOverlay.roots, primaryRootName)
	delete(catalogWithPrimaryOverlay.roots, collectionColumnRowLocatorRootName(view.catalog.meta.Name))
	catalogWithPrimaryOverlay.rootOverlays = make(map[string][]uint64, len(view.catalog.rootOverlays)+1)
	for name, rootIDs := range view.catalog.rootOverlays {
		catalogWithPrimaryOverlay.rootOverlays[name] = append([]uint64(nil), rootIDs...)
	}
	catalogWithPrimaryOverlay.rootOverlays[primaryRootName] = []uint64{primaryRootID}
	view.catalog = &catalogWithPrimaryOverlay

	if _, err := view.LookupDocumentRowRefsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{}); err == nil ||
		!strings.Contains(err.Error(), "primary row locator root is absent") {
		t.Fatalf("LookupDocumentRowRefsByID err=%v want overlay-primary absent-locator fail-closed error", err)
	}
}

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

func TestCollectionReadViewFetchDocumentsByIDCancelsDuringColumnVisibilityScan(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	const rows = 64
	ids := make([][]byte, rows)
	docs := make([][]byte, rows)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%03d", i))
		docs[i] = []byte(fmt.Sprintf(`{"row_id":%d,"kind":"kind","score":1}`, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()

	// The first six checks occur before the physical-row visitor. The seventh
	// is its 64-row poll; the final check maps the scan abort back to ctx.Err.
	ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: 7}
	if _, err := view.FetchDocumentsByID([][]byte{ids[0]}, DocumentFetchOptions{Context: ctx}); !errors.Is(err, context.Canceled) || ctx.calls != 8 {
		t.Fatalf("FetchDocumentsByID cancellation err=%v calls=%d want context canceled after row scan", err, ctx.calls)
	}
}

func TestCollectionReadViewForegroundLifetimeIdleAndOperations(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("e1")},
		[][]byte{[]byte(`{"row_id":1,"kind":"alpha","score":1.5,"payload":"retained-a"}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	begins, ends, active := 0, 0, 0
	unregister := d.RegisterForegroundReadObserver(func() {}, func() func() {
		begins++
		active++
		return func() {
			ends++
			active--
		}
	})
	defer unregister()

	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	assertOperation := func(name string, previousBegins int) {
		t.Helper()
		if active != 0 || begins != ends || begins != previousBegins+1 {
			t.Fatalf("%s foreground begin/end/active=%d/%d/%d want one additional balanced operation", name, begins, ends, active)
		}
	}
	if active != 0 || begins != ends {
		t.Fatalf("foreground begin/end/active after open=%d/%d/%d want balanced idle", begins, ends, active)
	}

	before := begins
	fetched, err := view.FetchDocumentsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	assertOperation("FetchDocumentsByID", before)

	before = begins
	lookup, err := view.LookupDocumentRowRefsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("LookupDocumentRowRefsByID: %v", err)
	}
	assertOperation("LookupDocumentRowRefsByID", before)
	if len(lookup.Results) != 1 || !lookup.Results[0].Found || len(fetched.Results) != 1 || !fetched.Results[0].Found {
		t.Fatalf("fetch=%+v lookup=%+v want found e1", fetched.Results, lookup.Results)
	}

	before = begins
	if _, err := view.FetchDocumentsByRowRef([]DocumentRowRef{lookup.Results[0].RowRef}, DocumentFetchOptions{}); err != nil {
		t.Fatalf("FetchDocumentsByRowRef: %v", err)
	}
	assertOperation("FetchDocumentsByRowRef", before)
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
	requireStandaloneColumnProductionAuthorityTest(t)
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

func TestCollectionReadViewProjectionValidationMatrix1875(t *testing.T) {
	d, col := newDocumentProjectionTestCollection1875(t)
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("e1"), []byte("e2")},
		[][]byte{
			[]byte(`{"row_id":1,"row_maybe":null,"kind":"alpha","score":1.5,"typed_maybe":null,"payload":"retained-a","note":null,"extra":{"nested":true}}`),
			[]byte(`{"row_id":2,"row_maybe":7,"kind":"beta","score":2.5,"typed_maybe":"present","payload":"retained-b","note":"kept","extra":{"nested":false}}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()

	got, err := view.FetchDocumentsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{IncludePaths: []string{"payload", "row_id", "kind", "typed_maybe", "row_maybe", "note"}})
	if err != nil {
		t.Fatalf("FetchDocumentsByID include projection: %v", err)
	}
	assertJSONMapEqual1875(t, got.Results[0].Document, map[string]any{
		"row_id":      float64(1),
		"row_maybe":   nil,
		"kind":        "alpha",
		"typed_maybe": nil,
		"payload":     "retained-a",
		"note":        nil,
	})
	if got.Stats.OutputBytes != got.Stats.DocumentBytes || got.Stats.FieldsReconstructed != 6 || got.Stats.FieldsSkipped == 0 {
		t.Fatalf("stats=%+v want output bytes, six reconstructed fields, and skipped fields", got.Stats)
	}

	precedence, err := view.FetchDocumentsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{IncludePaths: []string{"kind", "payload"}, ExcludePaths: []string{"kind"}})
	if err != nil {
		t.Fatalf("FetchDocumentsByID precedence projection: %v", err)
	}
	assertJSONMapEqual1875(t, precedence.Results[0].Document, map[string]any{"payload": "retained-a"})

	missing, err := view.FetchDocumentsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{IncludePaths: []string{"missing", "payload"}})
	if err != nil {
		t.Fatalf("FetchDocumentsByID missing projection: %v", err)
	}
	assertJSONMapEqual1875(t, missing.Results[0].Document, map[string]any{"payload": "retained-a"})

	retainedNested, err := view.FetchDocumentsByID([][]byte{[]byte("e2")}, DocumentFetchOptions{IncludePaths: []string{"extra"}})
	if err != nil {
		t.Fatalf("FetchDocumentsByID retained nested top-level projection: %v", err)
	}
	assertJSONMapEqual1875(t, retainedNested.Results[0].Document, map[string]any{"extra": map[string]any{"nested": false}})

	if _, err := view.FetchDocumentsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{IncludePaths: []string{"extra.nested"}}); err == nil || !strings.Contains(err.Error(), "nested paths") {
		t.Fatalf("nested projection err=%v want fail-closed nested path rejection", err)
	}

	refs, err := view.LookupDocumentRowRefsByID([][]byte{[]byte("e2")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("LookupDocumentRowRefsByID: %v", err)
	}
	rowRefProjected, err := view.FetchDocumentsByRowRef([]DocumentRowRef{refs.Results[0].RowRef}, DocumentFetchOptions{ExcludePaths: []string{"score", "extra"}})
	if err != nil {
		t.Fatalf("FetchDocumentsByRowRef projected: %v", err)
	}
	rowRefDoc := decodeJSONDocumentMap1875(t, rowRefProjected.Results[0].Document)
	if _, ok := rowRefDoc["score"]; ok {
		t.Fatalf("row-ref projection retained excluded score: %s", rowRefProjected.Results[0].Document)
	}
	if _, ok := rowRefDoc["extra"]; ok {
		t.Fatalf("row-ref projection retained excluded extra: %s", rowRefProjected.Results[0].Document)
	}
	if rowRefProjected.Stats.PointRowFetches != 1 || rowRefProjected.Stats.FieldsSkipped == 0 {
		t.Fatalf("row-ref projection stats=%+v want point fetch and skipped fields", rowRefProjected.Stats)
	}
}

func TestCollectionReadViewProjectionPlainJSONAndFormatValidation1875(t *testing.T) {
	d, col := newDocumentProjectionPlainJSONCollection1875(t)
	defer func() { _ = d.Close() }()
	if _, err := col.Insert([]byte("doc-a"), []byte(`{"kind":"plain","embedding":[1,2,3],"payload":"retained","note":null}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	projected, err := view.FetchDocumentsByID([][]byte{[]byte("doc-a")}, DocumentFetchOptions{IncludePaths: []string{"kind", "note"}})
	if err != nil {
		t.Fatalf("FetchDocumentsByID plain JSON projection: %v", err)
	}
	assertJSONMapEqual1875(t, projected.Results[0].Document, map[string]any{"kind": "plain", "note": nil})
	if projected.Stats.FieldsReconstructed != 2 || projected.Stats.FieldsSkipped != 2 || projected.Stats.OutputBytes != projected.Stats.DocumentBytes {
		t.Fatalf("plain JSON projection stats=%+v want reconstructed/skipped/output counters", projected.Stats)
	}
	fullJSON, err := view.FetchDocumentsByID([][]byte{[]byte("doc-a")}, DocumentFetchOptions{Format: DocumentFormatJSON})
	if err != nil {
		t.Fatalf("FetchDocumentsByID JSON format: %v", err)
	}
	assertJSONMapEqual1875(t, fullJSON.Results[0].Document, map[string]any{"kind": "plain", "embedding": []any{float64(1), float64(2), float64(3)}, "payload": "retained", "note": nil})
	if _, err := view.FetchDocumentsByID([][]byte{[]byte("doc-a")}, DocumentFetchOptions{Format: DocumentFormatBSON}); err == nil || !strings.Contains(err.Error(), "requires stored document format") {
		t.Fatalf("FetchDocumentsByID mismatched format err=%v want fail closed", err)
	}
}

func TestCollectionReadViewFetchFormatBSONNoProjection1875(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "bson_docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("bson_docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	doc := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "doc-b"}, {Key: "kind", Value: "bson"}, {Key: "score", Value: int32(7)}})
	if _, err := col.Insert([]byte("doc-b"), doc); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	got, err := view.FetchDocumentsByID([][]byte{[]byte("doc-b")}, DocumentFetchOptions{Format: DocumentFormatBSON})
	if err != nil {
		t.Fatalf("FetchDocumentsByID BSON format: %v", err)
	}
	if len(got.Results) != 1 || !got.Results[0].Found {
		t.Fatalf("FetchDocumentsByID BSON result=%+v want found result", got.Results)
	}
	if !bytes.Equal(got.Results[0].Document, doc) {
		t.Fatalf("FetchDocumentsByID BSON doc=%x want raw BSON %x", got.Results[0].Document, doc)
	}
	if _, err := view.FetchDocumentsByID([][]byte{[]byte("doc-b")}, DocumentFetchOptions{Format: DocumentFormatBSON, ExcludePaths: []string{"score"}}); err == nil || !strings.Contains(err.Error(), "document projection requires JSON") {
		t.Fatalf("FetchDocumentsByID BSON projection err=%v want fail closed", err)
	}
}

func TestCollectionReadViewProjectionExcludeEmbeddingAndIncludeMetadata1875(t *testing.T) {
	d, col := newDocumentProjectionVectorTestCollection1875(t, TypedStorageOwnerRowAsset)
	defer func() { _ = d.Close() }()
	if _, err := col.Insert([]byte("doc-a"), []byte(`{"row_id":1,"kind":"vector","did":"doc-a","embedding":[1,0,0],"payload":"retained"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	got, err := view.FetchDocumentsByID([][]byte{[]byte("doc-a")}, DocumentFetchOptions{ExcludePaths: []string{"embedding"}})
	if err != nil {
		t.Fatalf("FetchDocumentsByID exclude embedding: %v", err)
	}
	doc := decodeJSONDocumentMap1875(t, got.Results[0].Document)
	if _, ok := doc["embedding"]; ok {
		t.Fatalf("exclude embedding document=%s", got.Results[0].Document)
	}
	if doc["did"] != "doc-a" || doc["kind"] != "vector" || doc["payload"] != "retained" {
		t.Fatalf("exclude embedding document=%v want metadata and retained payload", doc)
	}
	selected, err := view.FetchDocumentsByID([][]byte{[]byte("doc-a")}, DocumentFetchOptions{IncludePaths: []string{"did", "kind"}})
	if err != nil {
		t.Fatalf("FetchDocumentsByID include metadata: %v", err)
	}
	assertJSONMapEqual1875(t, selected.Results[0].Document, map[string]any{"kind": "vector", "did": "doc-a"})
	if selected.Stats.FieldsSkipped == 0 || selected.Stats.FieldsReconstructed != 2 {
		t.Fatalf("include metadata stats=%+v want skipped fields and two reconstructed fields", selected.Stats)
	}
}

func TestCollectionReadViewProjectionTypedColumnPartEmbeddingSkip1875(t *testing.T) {
	d, col := newDocumentProjectionVectorTestCollection1875(t, TypedStorageOwnerColumnPart)
	defer func() { _ = d.Close() }()
	if _, err := col.Insert([]byte("doc-a"), []byte(`{"row_id":1,"kind":"vector","did":"doc-a","embedding":[1,0,0],"payload":"retained"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	got, err := view.FetchDocumentsByID([][]byte{[]byte("doc-a")}, DocumentFetchOptions{ExcludePaths: []string{"embedding"}})
	if err != nil {
		t.Fatalf("FetchDocumentsByID typed-column exclude embedding: %v", err)
	}
	doc := decodeJSONDocumentMap1875(t, got.Results[0].Document)
	if _, ok := doc["embedding"]; ok {
		t.Fatalf("typed-column exclude embedding document=%s", got.Results[0].Document)
	}
	if got.Stats.TypedColumnRows != 1 || got.Stats.FieldsSkipped == 0 {
		t.Fatalf("typed-column projection stats=%+v want typed-column row and skipped embedding", got.Stats)
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

func TestCollectionReadViewResponseArenaFetchDocumentsByIDAndRowRef1888(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("e1"), []byte("e2")}
	if _, err := col.InsertBatch(
		ids,
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

	fullByID, err := view.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByID full: %v", err)
	}
	if len(fullByID.Results) != len(ids) || !fullByID.Results[0].Found || !fullByID.Results[1].Found {
		t.Fatalf("full by id results=%+v want two found documents", fullByID.Results)
	}
	assertJSONMapEqual1875(t, fullByID.Results[0].Document, map[string]any{"row_id": float64(1), "kind": "alpha", "score": float64(1), "payload": "first"})
	assertJSONMapEqual1875(t, fullByID.Results[1].Document, map[string]any{"row_id": float64(2), "kind": "beta", "score": float64(2), "payload": "second"})
	if cap(fullByID.Results[0].Document) != len(fullByID.Results[0].Document) || cap(fullByID.Results[1].Document) != len(fullByID.Results[1].Document) {
		t.Fatalf("full by id documents are not cap-limited: caps=%d/%d lens=%d/%d", cap(fullByID.Results[0].Document), cap(fullByID.Results[1].Document), len(fullByID.Results[0].Document), len(fullByID.Results[1].Document))
	}
	secondBefore := append([]byte(nil), fullByID.Results[1].Document...)
	_ = append(fullByID.Results[0].Document, '!')
	if !bytes.Equal(fullByID.Results[1].Document, secondBefore) {
		t.Fatalf("full by id response documents share capacity: second=%s want %s", fullByID.Results[1].Document, secondBefore)
	}

	projectedByID, err := view.FetchDocumentsByID(ids, DocumentFetchOptions{IncludePaths: []string{"kind", "payload"}})
	if err != nil {
		t.Fatalf("FetchDocumentsByID projected: %v", err)
	}
	assertJSONMapEqual1875(t, projectedByID.Results[0].Document, map[string]any{"kind": "alpha", "payload": "first"})
	assertJSONMapEqual1875(t, projectedByID.Results[1].Document, map[string]any{"kind": "beta", "payload": "second"})
	if projectedByID.Stats.FieldsSkipped == 0 || projectedByID.Stats.FieldsReconstructed != 4 || projectedByID.Stats.DocumentBytes != projectedByID.Stats.OutputBytes {
		t.Fatalf("projected by id stats=%+v want skipped/reconstructed/output counters", projectedByID.Stats)
	}

	refs := []DocumentRowRef{fullByID.Results[0].RowRef, fullByID.Results[1].RowRef}
	fullByRowRef, err := view.FetchDocumentsByRowRef(refs, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByRowRef full: %v", err)
	}
	assertJSONMapEqual1875(t, fullByRowRef.Results[0].Document, map[string]any{"row_id": float64(1), "kind": "alpha", "score": float64(1), "payload": "first"})
	assertJSONMapEqual1875(t, fullByRowRef.Results[1].Document, map[string]any{"row_id": float64(2), "kind": "beta", "score": float64(2), "payload": "second"})
	if cap(fullByRowRef.Results[0].Document) != len(fullByRowRef.Results[0].Document) || cap(fullByRowRef.Results[1].Document) != len(fullByRowRef.Results[1].Document) {
		t.Fatalf("full row-ref documents are not cap-limited: caps=%d/%d lens=%d/%d", cap(fullByRowRef.Results[0].Document), cap(fullByRowRef.Results[1].Document), len(fullByRowRef.Results[0].Document), len(fullByRowRef.Results[1].Document))
	}

	projectedByRowRef, err := view.FetchDocumentsByRowRef(refs, DocumentFetchOptions{ExcludePaths: []string{"score"}})
	if err != nil {
		t.Fatalf("FetchDocumentsByRowRef projected: %v", err)
	}
	assertJSONMapEqual1875(t, projectedByRowRef.Results[0].Document, map[string]any{"row_id": float64(1), "kind": "alpha", "payload": "first"})
	assertJSONMapEqual1875(t, projectedByRowRef.Results[1].Document, map[string]any{"row_id": float64(2), "kind": "beta", "payload": "second"})
	if projectedByRowRef.Stats.PointRowFetches != uint64(len(refs)) || projectedByRowRef.Stats.FieldsSkipped == 0 || projectedByRowRef.Stats.DocumentBytes != projectedByRowRef.Stats.OutputBytes {
		t.Fatalf("projected row-ref stats=%+v want point fetches and projection counters", projectedByRowRef.Stats)
	}

	projectedByID.Results[0].Document[0] = 'X'
	fresh, err := view.FetchDocumentsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{IncludePaths: []string{"kind", "payload"}})
	if err != nil {
		t.Fatalf("FetchDocumentsByID after mutating response: %v", err)
	}
	assertJSONMapEqual1875(t, fresh.Results[0].Document, map[string]any{"kind": "alpha", "payload": "first"})
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
	if !lookup.Results[0].Found || lookup.Results[1].Found || lookup.Stats.RowLocatorLookups != 2 || lookup.Stats.RowLocatorMisses != 1 || lookup.Stats.RowLocatorBuilds != 0 {
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
	if view.columnSnapshotView == nil || len(view.pointRowRefs) == 0 || len(view.pointRowBlocks) == 0 || view.pointRowProjection == nil {
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
	if view.columnSnapshotView == nil || len(view.pointRowRefs) == 0 || len(view.pointRowBlocks) == 0 || view.pointRowProjection == nil {
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

func BenchmarkCollectionReadViewFetchDocumentsByIDMaterializerProjection1875(b *testing.B) {
	for _, tc := range []struct {
		name string
		opts DocumentFetchOptions
	}{
		{name: "full", opts: DocumentFetchOptions{}},
		{name: "exclude_embedding", opts: DocumentFetchOptions{ExcludePaths: []string{"embedding"}}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkCollectionReadViewFetchDocumentsByIDMaterializerProjection1875(b, tc.opts)
		})
	}
}

func benchmarkCollectionReadViewFetchDocumentsByIDMaterializerProjection1875(b *testing.B, opts DocumentFetchOptions) {
	b.Helper()
	const (
		rows = 1024
		dims = 128
	)
	d, col := newDocumentProjectionVectorTestCollectionWithDims1875(b, TypedStorageOwnerRowAsset, dims)
	defer func() { _ = d.Close() }()
	ids := make([][]byte, rows)
	docs := make([][]byte, rows)
	for i := 0; i < rows; i++ {
		vector := make([]float32, dims)
		for j := range vector {
			vector[j] = float32((i+j)%17) / 17
		}
		raw, err := json.Marshal(map[string]any{
			"row_id":    int64(i),
			"kind":      "vector",
			"did":       fmt.Sprintf("doc-%04d", i),
			"embedding": vector,
			"payload":   fmt.Sprintf("retained-%d", i),
		})
		if err != nil {
			b.Fatalf("json.Marshal row %d: %v", i, err)
		}
		ids[i] = []byte(fmt.Sprintf("doc-%04d", i))
		docs[i] = raw
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		b.Fatalf("InsertBatch: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		b.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	fetchIDs := [][]byte{ids[37], ids[128], ids[255], ids[512], ids[700], ids[900], ids[1000], ids[3], ids[44], ids[88]}
	if _, err := view.FetchDocumentsByID(fetchIDs, opts); err != nil {
		b.Fatalf("warm FetchDocumentsByID: %v", err)
	}
	measured, err := view.FetchDocumentsByID(fetchIDs, opts)
	if err != nil {
		b.Fatalf("measure FetchDocumentsByID: %v", err)
	}
	stats := measured.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := view.FetchDocumentsByID(fetchIDs, opts)
		if err != nil {
			b.Fatalf("FetchDocumentsByID: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
	}
	b.StopTimer()
	reportDocumentMaterializerBenchMetrics(b, stats)
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
	b.ReportMetric(float64(stats.OutputBytes), "output_B/fetch")
	b.ReportMetric(float64(stats.FieldsReconstructed), "fields_reconstructed/fetch")
	b.ReportMetric(float64(stats.FieldsSkipped), "fields_skipped/fetch")
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

func newDocumentProjectionTestCollection1875(t testing.TB) (*backenddb.DB, *Collection) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "row_maybe", Path: "row_maybe", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset, Nullable: true},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "score", Path: "score", ValueType: ColumnStoreValueDouble, Owner: TypedStorageOwnerColumnPart},
		{Name: "typed_maybe", Path: "typed_maybe", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Nullable: true, Dictionary: true},
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

func newDocumentProjectionPlainJSONCollection1875(t testing.TB) (*backenddb.DB, *Collection) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}); err != nil {
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

func newDocumentProjectionVectorTestCollection1875(t testing.TB, embeddingOwner TypedStorageFieldOwner) (*backenddb.DB, *Collection) {
	t.Helper()
	return newDocumentProjectionVectorTestCollectionWithDims1875(t, embeddingOwner, 3)
}

func newDocumentProjectionVectorTestCollectionWithDims1875(t testing.TB, embeddingOwner TypedStorageFieldOwner, dims int) (*backenddb.DB, *Collection) {
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
		{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, Owner: embeddingOwner, VectorDims: dims},
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

func decodeJSONDocumentMap1875(t testing.TB, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("document=%q is not valid JSON: %v", raw, err)
	}
	return out
}

func assertJSONMapEqual1875(t testing.TB, raw []byte, want map[string]any) {
	t.Helper()
	got := decodeJSONDocumentMap1875(t, raw)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("document=%s decoded=%v want %v", raw, got, want)
	}
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
