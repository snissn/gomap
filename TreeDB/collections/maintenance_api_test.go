package collections

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/db"
)

func TestListIndexesAndStats(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create email index: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "city_idx", Field: "city"}); err != nil {
		t.Fatalf("create city index: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"a@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"b@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("insert u2: %v", err)
	}

	indexes, err := col.ListIndexes()
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	if len(indexes) != 2 {
		t.Fatalf("expected 2 indexes, got %d", len(indexes))
	}

	stats, err := col.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.MetadataVersion != collectionMetaVersion {
		t.Fatalf("metadata version = %d, want %d", stats.MetadataVersion, collectionMetaVersion)
	}
	if stats.DocumentCount != 2 {
		t.Fatalf("document count = %d, want 2", stats.DocumentCount)
	}
	if stats.IndexCount != 2 {
		t.Fatalf("index count = %d, want 2", stats.IndexCount)
	}
	if stats.IndexEntryCounts["email_idx"] != 2 {
		t.Fatalf("email_idx count = %d, want 2", stats.IndexEntryCounts["email_idx"])
	}
	if stats.IndexEntryCounts["city_idx"] != 2 {
		t.Fatalf("city_idx count = %d, want 2", stats.IndexEntryCounts["city_idx"])
	}
	if stats.UserRootPageID == 0 || stats.SystemRootPageID == 0 {
		t.Fatalf("expected non-zero root ids, got user=%d system=%d", stats.UserRootPageID, stats.SystemRootPageID)
	}
}

func TestConsistencyDiagnosticCommand(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	id := []byte("u1")
	if _, err := col.Insert(id, []byte(`{"email":"a@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	report, err := col.CheckConsistency()
	if err != nil {
		t.Fatalf("check consistency healthy: %v", err)
	}
	if report.MissingIndexEntries != 0 || report.OrphanIndexEntries != 0 {
		t.Fatalf("expected healthy report, got %+v", report)
	}

	docKey, err := col.documentKey(id)
	if err != nil {
		t.Fatalf("document key: %v", err)
	}
	rootDesc := mustLoadPrimaryRootDescriptor(t, d, meta.PrimaryRoot)
	rootKey, err := SystemCollectionRootKey(meta.PrimaryRoot)
	if err != nil {
		t.Fatalf("root key: %v", err)
	}
	if _, err := d.MutateRoot(rootDesc.RootPageID, false, func(root batch.Interface) error {
		return root.Delete(docKey)
	}, func(sys batch.Interface, newRootID uint64) error {
		return writeRootDescriptorUpdate(sys, rootKey, &rootDesc, newRootID)
	}); err != nil {
		t.Fatalf("corrupt primary delete: %v", err)
	}

	report, err = col.CheckConsistency()
	if err != nil {
		t.Fatalf("check consistency orphan: %v", err)
	}
	if report.OrphanIndexEntries == 0 {
		t.Fatalf("expected orphan index entries after corrupt primary delete, got %+v", report)
	}

	if _, err := col.Insert(id, []byte(`{"email":"a@example.com"}`)); err != nil {
		t.Fatalf("restore doc: %v", err)
	}
	indexDef, ok := col.indexByName("email_idx")
	if !ok {
		t.Fatalf("expected email_idx metadata")
	}
	indexRootDesc := mustLoadSecondaryRootDescriptor(t, d, indexDef.RootName)
	indexRootKey, err := SystemCollectionRootKey(indexDef.RootName)
	if err != nil {
		t.Fatalf("index root key: %v", err)
	}
	idxPrefix, err := CollectionIndexPrefix(meta.Name, "email_idx")
	if err != nil {
		t.Fatalf("index prefix: %v", err)
	}
	it, err := d.IteratorAtRoot(indexRootDesc.RootPageID, idxPrefix, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	var staleKey []byte
	for ; it.Valid(); it.Next() {
		if bytes.HasPrefix(it.UnsafeKey(), idxPrefix) && !it.IsDeleted() {
			staleKey = append([]byte{}, it.UnsafeKey()...)
			break
		}
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	_ = it.Close()
	if len(staleKey) == 0 {
		t.Fatalf("expected index key for corruption setup")
	}
	if _, err := d.MutateRoot(indexRootDesc.RootPageID, false, func(root batch.Interface) error {
		return root.Delete(staleKey)
	}, func(sys batch.Interface, newRootID uint64) error {
		return writeRootDescriptorUpdate(sys, indexRootKey, &indexRootDesc, newRootID)
	}); err != nil {
		t.Fatalf("corrupt index delete: %v", err)
	}

	report, err = col.CheckConsistency()
	if err != nil {
		t.Fatalf("check consistency missing index: %v", err)
	}
	if report.MissingIndexEntries == 0 {
		t.Fatalf("expected missing index entry after corrupt delete, got %+v", report)
	}
}
