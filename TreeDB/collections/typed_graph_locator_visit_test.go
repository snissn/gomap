package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestTypedGraphLocatorVisitorOwnership(t *testing.T) {
	db, col := newDocumentMaterializerTestCollection(t)
	defer db.Close()
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{[]byte(`{"row_id":1,"kind":"a","score":1.5}`), []byte(`{"row_id":2,"kind":"b","score":2.5}`)}); err != nil {
		t.Fatal(err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	ids := [][]byte{[]byte("e1"), []byte("missing"), []byte("e2")}
	owned, err := view.LookupDocumentRowRefsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	visited := 0
	stats, err := view.visitDocumentRowRefsByID(ids, func(id []byte, ref DocumentRowRef, found bool) error {
		want := owned.Results[visited]
		if !bytes.Equal(id, want.ID) || found != want.Found {
			t.Fatalf("visitor row%d id=%s found=%t", visited, id, found)
		}
		if found && (ref.Generation != want.RowRef.Generation || ref.PartID != want.RowRef.PartID || ref.RowIndex != want.RowRef.RowIndex || ref.AppliedCommandLSN != want.RowRef.AppliedCommandLSN || &ref.DocumentID[0] != &id[0]) {
			t.Fatalf("visitor row%d must borrow ID and preserve coordinates: %+v", visited, ref)
		}
		visited++
		return nil
	})
	if err != nil || visited != 3 || stats.RowLocatorLookups != 3 || stats.RowLocatorMisses != 1 {
		t.Fatalf("visit count=%d stats=%+v err=%v", visited, stats, err)
	}
	ids[0][0] = 'x'
	if string(owned.Results[0].ID) != "e1" || string(owned.Results[0].RowRef.DocumentID) != "e1" {
		t.Fatal("public response borrowed caller ID")
	}
	owned.Results[0].ID[0] = 'z'
	if string(owned.Results[0].RowRef.DocumentID) != "e1" {
		t.Fatal("public result ID mutation changed independently owned row-ref ID")
	}
	injected := errors.New("stop visitor")
	visited = 0
	_, err = view.visitDocumentRowRefsByID(ids, func([]byte, DocumentRowRef, bool) error { visited++; return injected })
	if !errors.Is(err, injected) || visited != 1 {
		t.Fatalf("callback error not preserved: visited=%d err=%v", visited, err)
	}
	if _, err := view.visitDocumentRowRefsByID(nil, nil); err != nil {
		t.Fatalf("empty visitor: %v", err)
	}
	if _, err := view.visitDocumentRowRefsByID([][]byte{nil}, func([]byte, DocumentRowRef, bool) error { return nil }); err == nil {
		t.Fatal("empty ID accepted")
	}
	ref := DocumentRowRef{DocumentID: []byte("owned"), Generation: 3, PartID: 2, RowIndex: 4, AppliedCommandLSN: 9}
	encoded := encodeColumnPrimaryRowLocator(ref)
	borrowed, err := decodeColumnPrimaryRowLocatorBorrowedID(ref.DocumentID, encoded)
	if err != nil {
		t.Fatal(err)
	}
	clear(encoded)
	if borrowed.Generation != 3 || borrowed.PartID != 2 || borrowed.RowIndex != 4 || borrowed.AppliedCommandLSN != 9 {
		t.Fatal("borrowed decoder retained encoded locator scratch")
	}
	for _, invalid := range [][]byte{nil, encoded, []byte("CRL1")} {
		if _, err := decodeColumnPrimaryRowLocatorBorrowedID(ref.DocumentID, invalid); err == nil {
			t.Fatal("invalid locator accepted")
		}
	}
}

func openTypedLocatorGroupedFixture(t testing.TB, policy RootStoragePolicy) (*Collection, *CollectionReadView, [][]byte) {
	t.Helper()
	db, cleanup, _, _, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cleanup(); err != nil {
			t.Error(err)
		}
	})
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset}}
	cfg.SortKey, cfg.AggregateMetadata = nil, nil
	cfg.ControlRootStoragePolicy = policy
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "locator", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: cfg}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("locator")
	if err != nil {
		t.Fatal(err)
	}
	const rows = 2048
	ids, docs := make([][]byte, rows), make([][]byte, rows)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("row-%05d", i))
		docs[i] = []byte(fmt.Sprintf(`{"row_id":%d}`, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := view.Close(); err != nil {
			t.Error(err)
		}
	})
	return col, view, ids
}

func TestTypedGraphLocatorVisitorGroupsCapturedRoot(t *testing.T) {
	_, view, allIDs := openTypedLocatorGroupedFixture(t, RootStorageDefault)
	ids := make([][]byte, 512)
	for i := range ids {
		ids[i] = allIDs[(i*13)%len(allIDs)]
	}
	root := collectionColumnRowLocatorRootName(view.catalog.meta.Name)
	if view.catalog.rootID(root) == 0 || len(view.catalog.overlayRootIDs(root)) != 0 {
		t.Fatal("fixture has no unoverlaid locator root")
	}
	before := tree.GetManyReadStatsSnapshot()
	if err := collectionGetManyViewAtCatalogRoot(view.snapshot, view.catalog, root, ids, func(i int, id, value []byte, found bool) error {
		if !found || !bytes.Equal(id, ids[i]) {
			t.Fatal("direct grouped locator missing")
		}
		_, err := decodeColumnPrimaryRowLocatorBorrowedID(id, value)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	after := tree.GetManyReadStatsSnapshot()
	if after.GroupedCallsTotal-before.GroupedCallsTotal != 1 || after.LeafGroupsTotal-before.LeafGroupsTotal < 2 || after.LeafLoadsSavedTotal == before.LeafLoadsSavedTotal {
		t.Fatalf("fixture not groupable: before=%+v after=%+v", before, after)
	}
	t.Logf("direct groups=%d saved=%d", after.LeafGroupsTotal-before.LeafGroupsTotal, after.LeafLoadsSavedTotal-before.LeafLoadsSavedTotal)
	var first, last runtime.MemStats
	runtime.ReadMemStats(&first)
	before = tree.GetManyReadStatsSnapshot()
	visited := 0
	stats, err := view.visitDocumentRowRefsByID(ids, func(id []byte, ref DocumentRowRef, found bool) error {
		if !found || !bytes.Equal(id, ids[visited]) || ref.RowIndex != (visited*13)%len(allIDs) {
			t.Fatalf("ordered row %d: %+v found=%t", visited, ref, found)
		}
		visited++
		return nil
	})
	after = tree.GetManyReadStatsSnapshot()
	runtime.ReadMemStats(&last)
	t.Logf("visitor bytes=%d mallocs=%d groups=%d saved=%d", last.TotalAlloc-first.TotalAlloc, last.Mallocs-first.Mallocs, after.LeafGroupsTotal-before.LeafGroupsTotal, after.LeafLoadsSavedTotal-before.LeafLoadsSavedTotal)
	if err != nil || visited != len(ids) || stats.RowLocatorLookups != uint64(len(ids)) {
		t.Fatalf("visited=%d stats=%+v err=%v", visited, stats, err)
	}
	if after.GroupedCallsTotal-before.GroupedCallsTotal != 1 || after.LeafLoadsSavedTotal == before.LeafLoadsSavedTotal {
		t.Fatal("locator visitor did not use eligible grouped reads")
	}
}

func TestTypedGraphLocatorVisitorGroupedBoundaries(t *testing.T) {
	col, view, allIDs := openTypedLocatorGroupedFixture(t, RootStorageDefault)
	ids := make([][]byte, 600)
	for i := range ids {
		ids[i] = allIDs[(i*13)%len(allIDs)]
	}
	ids[0], ids[511], ids[63], ids[512] = []byte("missing-first"), []byte("missing-last"), ids[20], ids[20]
	owned, err := view.LookupDocumentRowRefsByID(ids, DocumentFetchOptions{})
	if err != nil || owned.Stats.RowLocatorLookups != 600 || owned.Stats.RowLocatorMisses != 2 {
		t.Fatalf("lookup stats=%+v err=%v", owned.Stats, err)
	}
	root := collectionColumnRowLocatorRootName(view.catalog.meta.Name)
	for i, result := range owned.Results {
		value, found, err := collectionGetAppendAtCatalogRoot(view.snapshot, view.catalog, root, ids[i], nil)
		if err != nil || found != result.Found || !bytes.Equal(ids[i], result.ID) {
			t.Fatalf("point oracle row %d found=%t err=%v", i, found, err)
		}
		if found {
			ref, err := decodeColumnPrimaryRowLocatorBorrowedID(ids[i], value)
			if err != nil || ref.Generation != result.RowRef.Generation || ref.PartID != result.RowRef.PartID || ref.RowIndex != result.RowRef.RowIndex || ref.AppliedCommandLSN != result.RowRef.AppliedCommandLSN {
				t.Fatalf("row %d coordinates differ", i)
			}
		}
	}
	// Public IDs own separate buffers even across duplicate requests and chunks.
	owned.Results[20].ID[0] = 'x'
	owned.Results[63].RowRef.DocumentID[0] = 'y'
	if owned.Results[20].RowRef.DocumentID[0] != 'r' || owned.Results[512].RowRef.DocumentID[0] != 'r' || ids[20][0] != 'r' {
		t.Fatal("public locator IDs alias")
	}
	before := tree.GetManyReadStatsSnapshot()
	if _, err := view.LookupDocumentRowRefsByID(allIDs[:5], DocumentFetchOptions{}); err != nil {
		t.Fatal(err)
	}
	if tree.GetManyReadStatsSnapshot().CallsTotal != before.CallsTotal {
		t.Fatal("small request left point path")
	}

	stop := fmt.Errorf("visitor: %w", tree.ErrKeyNotFound)
	visited := 0
	stats, err := view.visitDocumentRowRefsByID(ids, func([]byte, DocumentRowRef, bool) error {
		visited++
		if visited == 3 {
			return stop
		}
		return nil
	})
	if err != stop || visited != 3 || stats.RowLocatorLookups != 512 || stats.RowLocatorMisses != 2 {
		t.Fatalf("stop visited=%d stats=%+v err=%v", visited, stats, err)
	}
	ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: 513}
	before = tree.GetManyReadStatsSnapshot()
	stats, err = view.visitDocumentRowRefsByID(allIDs[:1025], func([]byte, DocumentRowRef, bool) error { return ctx.Err() })
	if !errors.Is(err, context.Canceled) || ctx.calls != 513 || stats.RowLocatorLookups != 1024 || tree.GetManyReadStatsSnapshot().CallsTotal-before.CallsTotal != 2 {
		t.Fatalf("cancel calls=%d stats=%+v err=%v", ctx.calls, stats, err)
	}

	original := view.catalog
	// As in the existing malformed-locator control, redirect only this held
	// view to real primary values. Two bad rows arrive in storage group order;
	// the earlier input error must follow exactly its preceding missing rows.
	bad := *original
	bad.roots = make(map[string]uint64, len(original.roots))
	for key, value := range original.roots {
		bad.roots[key] = value
	}
	bad.roots[root] = original.rootID(collectionPrimaryRootName(original.meta.Name))
	view.catalog = &bad
	badIDs := make([][]byte, 512)
	for i := range badIDs {
		badIDs[i] = []byte(fmt.Sprintf("missing-%04d", i))
	}
	badIDs[7], badIDs[450] = allIDs[0], allIDs[1]
	visited = 0
	stats, err = view.visitDocumentRowRefsByID(badIDs, func([]byte, DocumentRowRef, bool) error { visited++; return nil })
	if err == nil || !strings.Contains(err.Error(), "invalid primary row locator") || !strings.Contains(err.Error(), string(allIDs[0])) || visited != 7 || stats.RowLocatorLookups != 512 || stats.RowLocatorMisses != 510 {
		t.Fatalf("bad locator visited=%d stats=%+v err=%v", visited, stats, err)
	}
	visited = 0
	_, err = view.visitDocumentRowRefsByID(badIDs, func([]byte, DocumentRowRef, bool) error { visited++; return stop })
	if err != stop || visited != 1 {
		t.Fatalf("earlier callback lost: visited=%d err=%v", visited, err)
	}
	view.catalog = original

	// A real later write cannot change coordinates or payload behind this pin.
	if _, err := col.Replace(allIDs[0], []byte(`{"row_id":9999}`)); err != nil {
		t.Fatal(err)
	}
	got, err := view.FetchDocumentsByID(allIDs[:512], DocumentFetchOptions{})
	if err != nil || len(got.Results) != 512 || !bytes.Contains(got.Results[0].Document, []byte(`"row_id":0`)) {
		t.Fatalf("held full output rows=%d err=%v", len(got.Results), err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	visited = 0
	if _, err := view.visitDocumentRowRefsByID(ids, func([]byte, DocumentRowRef, bool) error { visited++; return nil }); err == nil || visited != 0 {
		t.Fatal("closed view accepted")
	}
	fresh, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if got, err := fresh.FetchDocumentsByID(allIDs[:512], DocumentFetchOptions{}); err != nil || !bytes.Contains(got.Results[0].Document, []byte(`"row_id":9999`)) {
		t.Fatalf("fresh retry err=%v", err)
	}
	// A storage-level rejection before any result must not invent lookups or
	// deliver zero-valued scratch as misses. Keep the view open for validation.
	if err := fresh.snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	visited = 0
	stats, err = fresh.visitDocumentRowRefsByID(allIDs[:512], func([]byte, DocumentRowRef, bool) error { visited++; return nil })
	if err == nil || !strings.Contains(err.Error(), "primary row locator batch lookup") || visited != 0 || stats.RowLocatorLookups != 0 || stats.RowLocatorMisses != 0 {
		t.Fatalf("storage rejection visited=%d stats=%+v err=%v", visited, stats, err)
	}
}

func TestTypedGraphLocatorVisitorFallbacks(t *testing.T) {
	for _, policy := range []RootStoragePolicy{RootStorageDefault, RootStorageFast} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			_, view, ids := openTypedLocatorGroupedFixture(t, policy)
			if policy == RootStorageDefault {
				// Exercise an immutable catalog overlay stack referencing the same
				// real locator root; it must keep direct overlay-aware point reads.
				catalog := *view.catalog
				root := collectionColumnRowLocatorRootName(catalog.meta.Name)
				catalog.rootOverlays = map[string][]uint64{root: {catalog.rootID(root)}}
				view.catalog = &catalog
			}
			before := tree.GetManyReadStatsSnapshot()
			got, err := view.LookupDocumentRowRefsByID(ids[:512], DocumentFetchOptions{})
			after := tree.GetManyReadStatsSnapshot()
			if err != nil || got.Stats.RowLocatorLookups != 512 || got.Stats.RowLocatorMisses != 0 || after.GroupedCallsTotal != before.GroupedCallsTotal {
				t.Fatalf("fallback stats=%+v err=%v tree=%+v", got.Stats, err, after)
			}
			for i, result := range got.Results {
				if !result.Found || result.RowRef.RowIndex != i {
					t.Fatalf("fallback row %d: %+v", i, result)
				}
			}
			if policy == RootStorageFast && after.FallbackCallsTotal-before.FallbackCallsTotal != 1 {
				t.Fatal("pager leaf fallback not reached")
			}
			if policy == RootStorageDefault && after.CallsTotal != before.CallsTotal {
				t.Fatal("overlay left point path")
			}
		})
	}
}
