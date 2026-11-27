package collections

import (
	"errors"
	"strconv"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestInspectNativeMeta_NativeSettledUnsettledDDLAndMissing(t *testing.T) {
	owner, mgr, col, _ := nativeCatalogFixture(t, t.TempDir(), true)

	// Nil receiver / nil callback error handling.
	var nilCol *Collection
	if handled, err := nilCol.InspectNativeMeta(func(*CollectionMeta) error { return nil }); handled || !errors.Is(err, errCollectionNil) {
		t.Fatalf("nil receiver: handled=%v err=%v want false, %v", handled, err, errCollectionNil)
	}
	if handled, err := col.InspectNativeMeta(nil); handled || err == nil {
		t.Fatalf("nil callback: handled=%v err=%v want false, non-nil", handled, err)
	}

	// Add an index and insert a document so both unsettled and settled states have live data.
	if _, err := col.CreateIndex(IndexDefinition{Name: "age_1", Field: "age", ValueType: IndexValueInt64}); err != nil {
		t.Fatalf("CreateIndex(age_1): %v", err)
	}
	rawDoc, err := bson.Marshal(bson.D{{Key: "_id", Value: "doc-1"}, {Key: "age", Value: int64(30)}})
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	if _, err := col.Insert([]byte("doc-1"), rawDoc); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	statUint64 := func(key string) uint64 {
		t.Helper()
		raw, ok := owner.Stats()[key]
		if !ok {
			t.Fatalf("missing stat %q", key)
		}
		v, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatalf("parse stat %q=%q: %v", key, raw, err)
		}
		return v
	}

	checkInspect := func(label string, wantIndexes int, wantHasAge bool) {
		t.Helper()
		baseLeases := statUint64("treedb.cache.cow.external_leases")
		baseCuts := statUint64("treedb.cache.cow.active_cuts")
		baseViews := statUint64("treedb.cache.cow.views")
		beforeCaptures := statUint64("treedb.cache.cow.capture_calls_total")

		var gotFormat DocumentFormat
		var gotIndexCount int
		var gotHasAge bool
		handled, err := col.InspectNativeMeta(func(meta *CollectionMeta) error {
			gotFormat = meta.Options.DocumentFormat
			gotIndexCount = len(meta.Indexes)
			for _, idx := range meta.Indexes {
				if idx.Name == "age_1" {
					gotHasAge = true
				}
			}
			return nil
		})
		if err != nil || !handled {
			t.Fatalf("%s: InspectNativeMeta handled=%v err=%v want true, nil", label, handled, err)
		}
		if gotFormat != DocumentFormatBSON || gotIndexCount != wantIndexes || gotHasAge != wantHasAge {
			t.Fatalf("%s: got format=%q indexes=%d hasAge=%v want %q %d %v",
				label, gotFormat, gotIndexCount, gotHasAge, DocumentFormatBSON, wantIndexes, wantHasAge)
		}
		if afterCaptures := statUint64("treedb.cache.cow.capture_calls_total"); afterCaptures != beforeCaptures {
			t.Fatalf("%s: InspectNativeMeta triggered %d COW captures, want 0", label, afterCaptures-beforeCaptures)
		}
		if leases, cuts, views := statUint64("treedb.cache.cow.external_leases"), statUint64("treedb.cache.cow.active_cuts"), statUint64("treedb.cache.cow.views"); leases > baseLeases || cuts > baseCuts || views > baseViews {
			t.Fatalf("%s: COW leak leases=%d(base %d) cuts=%d(base %d) views=%d(base %d)",
				label, leases, baseLeases, cuts, baseCuts, views, baseViews)
		}
	}

	checkInspect("unsettled", 1, true)

	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	checkInspect("settled", 1, true)

	// Index DDL visibility.
	if _, err := col.DropIndex("age_1"); err != nil {
		t.Fatalf("DropIndex(age_1): %v", err)
	}
	checkInspect("after-drop-index", 0, false)

	// Missing collection in the pinned cut returns ErrCollectionNotFound without leaking cuts.
	absentCol := &Collection{manager: mgr, nativeJSON: mgr.nativeJSON, name: "absent"}
	baseCuts := statUint64("treedb.cache.cow.active_cuts")
	handled, err := absentCol.InspectNativeMeta(func(*CollectionMeta) error {
		t.Fatal("callback must not run for absent collection")
		return nil
	})
	if !handled || !errors.Is(err, ErrCollectionNotFound) {
		t.Fatalf("absent collection: handled=%v err=%v want true, ErrCollectionNotFound", handled, err)
	}
	if cuts := statUint64("treedb.cache.cow.active_cuts"); cuts > baseCuts {
		t.Fatalf("absent collection leaked active cuts: %d > %d", cuts, baseCuts)
	}
}

func TestInspectNativeMeta_NonNativeFallsBack(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("backenddb.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("db.Close: %v", err)
		}
	})
	mgr := NewCollectionManager(db)
	t.Cleanup(func() {
		if err := mgr.Close(); err != nil {
			t.Errorf("mgr.Close: %v", err)
		}
	})
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "legacy_docs",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("legacy_docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	called := false
	handled, err := col.InspectNativeMeta(func(*CollectionMeta) error {
		called = true
		return nil
	})
	if handled || err != nil || called {
		t.Fatalf("non-native InspectNativeMeta handled=%v err=%v called=%v want false, nil, false", handled, err, called)
	}
}
