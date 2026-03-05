package collections

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCollectionPrimaryRoot_UsesDescriptorOuterLeafValueLogWhenDBGlobalModeDisabled(t *testing.T) {
	d, err := db.Open(db.Options{
		Dir:                        t.TempDir(),
		Durability:                 db.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: false,
		LeafPrefixCompression:      true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	docID := []byte("u1")
	doc := []byte(`{"email":"ada@example.com"}`)
	if _, err := col.Insert(docID, doc); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rootDesc := mustLoadPrimaryRootDescriptor(t, d, meta.PrimaryRoot)
	if rootDesc.RootPageID == 0 {
		t.Fatalf("expected non-zero primary root page id")
	}
	if _, ok := page.DecodeLeafRef(rootDesc.RootPageID); !ok {
		t.Fatalf("expected primary root %d to use descriptor outer-leaf value-log mode", rootDesc.RootPageID)
	}
}

func TestCollectionSecondaryRoot_StaysPagerBackedWhenDBGlobalModeEnabled(t *testing.T) {
	d, err := db.Open(db.Options{
		Dir:                        t.TempDir(),
		Durability:                 db.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "city_idx", Field: "city"}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	indexDef, ok := col.indexByName("city_idx")
	if !ok {
		t.Fatalf("expected city_idx metadata")
	}
	rootDesc := mustLoadSecondaryRootDescriptor(t, d, indexDef.RootName)
	if rootDesc.RootPageID == 0 {
		t.Fatalf("expected non-zero secondary root page id")
	}
	if _, ok := page.DecodeLeafRef(rootDesc.RootPageID); ok {
		t.Fatalf("expected secondary root %d to stay pager-backed", rootDesc.RootPageID)
	}
}

func TestCollectionSecondaryIndexBackfill_StaysPagerBackedWhenDBGlobalModeEnabled(t *testing.T) {
	d, err := db.Open(db.Options{
		Dir:                        t.TempDir(),
		Durability:                 db.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert before backfill: %v", err)
	}

	indexDef, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "city_idx", Field: "city"})
	if err != nil {
		t.Fatalf("create index with backfill: %v", err)
	}

	rootDesc := mustLoadSecondaryRootDescriptor(t, d, indexDef.RootName)
	if rootDesc.RootPageID == 0 {
		t.Fatalf("expected backfilled secondary root page id")
	}
	if _, ok := page.DecodeLeafRef(rootDesc.RootPageID); ok {
		t.Fatalf("expected backfilled secondary root %d to stay pager-backed", rootDesc.RootPageID)
	}
}
