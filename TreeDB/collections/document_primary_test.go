package collections

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestInsertGetDelete_DocumentsRoundTrip(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	collection, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode:                  idModeCallerProvided,
			StorageMode:             CollectionStorageModeOuterLeafInValueLog,
			RejectMissingFields:     true,
			AllowArrayValuesInIndex: false,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(collection.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	smallID := []byte("small-doc")
	if _, err := col.Insert(smallID, []byte("small")); err != nil {
		t.Fatalf("insert small doc: %v", err)
	}
	smallDoc, err := col.Get(smallID)
	if err != nil {
		t.Fatalf("get small doc: %v", err)
	}
	if !bytes.Equal(smallDoc, []byte("small")) {
		t.Fatalf("small doc mismatch: got=%q want=%q", smallDoc, []byte("small"))
	}

	largeID := []byte("large-doc")
	large := bytes.Repeat([]byte("x"), 1024)
	if _, err := col.Insert(largeID, large); err != nil {
		t.Fatalf("insert large doc: %v", err)
	}
	roundTrip, err := col.Get(largeID)
	if err != nil {
		t.Fatalf("get large doc: %v", err)
	}
	if !bytes.Equal(roundTrip, large) {
		t.Fatalf("large doc mismatch: len got=%d want=%d", len(roundTrip), len(large))
	}

	if err := col.Delete(smallID); err != nil {
		t.Fatalf("delete small doc: %v", err)
	}
	afterDelete, err := col.Get(smallID)
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if afterDelete != nil {
		t.Fatalf("expected deleted doc missing, got %q", afterDelete)
	}
}

func TestInsertStoresDocumentsInDedicatedPrimaryRoot(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
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
	doc := []byte(`{"email":"ada@example.com","city":"hnl"}`)
	if _, err := col.Insert(docID, doc); err != nil {
		t.Fatalf("insert: %v", err)
	}

	desc := mustLoadPrimaryRootDescriptor(t, d, meta.PrimaryRoot)
	if desc.RootPageID == 0 {
		t.Fatalf("expected dedicated primary root page id after insert")
	}

	rootValue, err := d.GetAtRoot(desc.RootPageID, docID)
	if err != nil {
		t.Fatalf("GetAtRoot: %v", err)
	}
	if !bytes.Equal(rootValue, doc) {
		t.Fatalf("dedicated root value mismatch: got=%q want=%q", rootValue, doc)
	}

	sharedPrefix, err := CollectionDataPrefix(meta.Name)
	if err != nil {
		t.Fatalf("collection data prefix: %v", err)
	}
	sharedKey := append(append([]byte{}, sharedPrefix...), docID...)
	sharedValue, err := d.Get(sharedKey)
	if err != nil {
		t.Fatalf("Get shared key: %v", err)
	}
	if sharedValue != nil {
		t.Fatalf("expected shared keyspace to remain empty, got %q", sharedValue)
	}
}

func TestInsertUpsertOverwrite_ReplacesDocument(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	collection, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(collection.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	id := []byte("doc-1")
	if _, err := col.Insert(id, []byte("v1")); err != nil {
		t.Fatalf("insert v1: %v", err)
	}
	if _, err := col.Insert(id, []byte("v2")); err != nil {
		t.Fatalf("insert v2: %v", err)
	}

	got, err := col.Get(id)
	if err != nil {
		t.Fatalf("get doc: %v", err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("expected upsert replacement, got=%q", got)
	}
}

func TestPrimaryReadAfterCheckpoint_Reopen(t *testing.T) {
	dir := t.TempDir()
	collectionName := "events"
	dataPrefix, err := CollectionDataPrefix(collectionName)
	if err != nil {
		t.Fatalf("collection data prefix: %v", err)
	}
	d, err := db.Open(db.Options{
		Dir: dir,
		ValueLog: db.ValueLogOptions{
			DomainInlineThresholds: []db.ValueLogDomainThreshold{
				{
					Prefix:          append(append([]byte{}, dataPrefix...), 0xff),
					InlineThreshold: 8,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	collection, err := mgr.CreateCollection(&CollectionMeta{Name: collectionName})
	if err != nil {
		d.Close()
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(collection.Name)
	if err != nil {
		d.Close()
		t.Fatalf("open collection: %v", err)
	}

	id := []byte("ev-1")
	doc := bytes.Repeat([]byte("z"), 256)
	if _, err := col.Insert(id, doc); err != nil {
		d.Close()
		t.Fatalf("insert doc: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopen, err := db.Open(db.Options{
		Dir: dir,
		ValueLog: db.ValueLogOptions{
			DomainInlineThresholds: []db.ValueLogDomainThreshold{
				{
					Prefix:          append(append([]byte{}, dataPrefix...), 0xff),
					InlineThreshold: 8,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopen.Close()

	reopenMgr := NewCollectionManager(reopen)
	reopenCol, err := reopenMgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	got, err := reopenCol.Get(id)
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("value mismatch after reopen: got=%d want=%d", len(got), len(doc))
	}
}

func TestPrimaryStoreUsesValueLogMode(t *testing.T) {
	d, err := db.Open(db.Options{
		Dir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	collection, err := mgr.CreateCollection(&CollectionMeta{
		Name: "vlogged",
		Options: CollectionOptions{
			IDMode:                  idModeCallerProvided,
			StorageMode:             CollectionStorageModeOuterLeafInValueLog,
			RejectMissingFields:     true,
			AllowArrayValuesInIndex: false,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(collection.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	id := []byte("payload")
	large := bytes.Repeat([]byte("v"), 1024)
	if _, err := col.Insert(id, large); err != nil {
		t.Fatalf("insert large: %v", err)
	}

	desc := mustLoadPrimaryRootDescriptor(t, d, collection.PrimaryRoot)
	start := []byte(id)
	end := append(append([]byte{}, id...), 0xff)
	it, err := d.IteratorAtRootWithOptions(desc.RootPageID, start, end, db.IteratorOptions{Mode: db.IteratorModePointerProjection})
	if err != nil {
		t.Fatalf("projection iterator: %v", err)
	}
	defer it.Close()

	seen := false
	for ; it.Valid(); it.Next() {
		key := it.UnsafeKey()
		if !bytes.Equal(key, id) {
			continue
		}
		val := it.UnsafeValue()
		if len(val) != 0 {
			t.Fatalf("expected projection value omitted for pointer-backed collection value")
		}
		seen = true
		break
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if !seen {
		t.Fatalf("did not observe collection key in dedicated root pointer projection scan")
	}

	legacyStart, err := CollectionDataPrefix(collection.Name)
	if err != nil {
		t.Fatalf("data prefix: %v", err)
	}
	legacyEnd := append(append([]byte{}, legacyStart...), 0xff)
	legacyIt, err := d.IteratorWithOptions(legacyStart, legacyEnd, db.IteratorOptions{Mode: db.IteratorModePointerProjection})
	if err != nil {
		t.Fatalf("legacy projection iterator: %v", err)
	}
	defer legacyIt.Close()
	for ; legacyIt.Valid(); legacyIt.Next() {
		if !legacyIt.IsDeleted() && bytes.HasPrefix(legacyIt.UnsafeKey(), legacyStart) {
			t.Fatalf("expected no shared-keyspace primary documents, found %q", legacyIt.UnsafeKey())
		}
	}
	if err := legacyIt.Error(); err != nil {
		t.Fatalf("legacy iterator error: %v", err)
	}
}

func mustLoadPrimaryRootDescriptor(t *testing.T, d *db.DB, rootName string) CollectionRootDescriptor {
	t.Helper()
	rootKey, err := SystemCollectionRootKey(rootName)
	if err != nil {
		t.Fatalf("root key: %v", err)
	}
	raw, err := d.GetSystem(rootKey)
	if err != nil {
		t.Fatalf("get root descriptor: %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("expected root descriptor for %q", rootName)
	}
	var desc CollectionRootDescriptor
	if err := desc.Decode(raw); err != nil {
		t.Fatalf("decode root descriptor: %v", err)
	}
	return desc
}
