package collections

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestInsertBatch_RoundTripWithSecondaryIndexes(t *testing.T) {
	dbx, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer dbx.Close()

	mgr := NewCollectionManager(dbx)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode: IDModeCallerProvided,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create unique index: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "city_idx", Field: "city"}); err != nil {
		t.Fatalf("create non-unique index: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	ids, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
			[]byte(`{"email":"grace@example.com","city":"hnl"}`),
		},
	)
	if err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("resolved ids len=%d want 2", len(ids))
	}
	if !bytes.Equal(ids[0], []byte("u1")) || !bytes.Equal(ids[1], []byte("u2")) {
		t.Fatalf("resolved ids=%q", ids)
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Equal(got, []byte(`{"email":"ada@example.com","city":"hnl"}`)) {
		t.Fatalf("u1 doc=%q", got)
	}

	uniqueIDs, err := col.FindByIndex("email_idx", "grace@example.com")
	if err != nil {
		t.Fatalf("find unique: %v", err)
	}
	if len(uniqueIDs) != 1 || !bytes.Equal(uniqueIDs[0], []byte("u2")) {
		t.Fatalf("unique ids=%q", uniqueIDs)
	}

	cityIDs, err := col.FindByIndex("city_idx", "hnl")
	if err != nil {
		t.Fatalf("find non-unique: %v", err)
	}
	if len(cityIDs) != 2 {
		t.Fatalf("city ids len=%d want 2", len(cityIDs))
	}
	if !bytes.Equal(cityIDs[0], []byte("u1")) || !bytes.Equal(cityIDs[1], []byte("u2")) {
		t.Fatalf("city ids=%q", cityIDs)
	}
}

func TestInsertBatch_AutoIDAllocatesOnceAndIndexesDocuments(t *testing.T) {
	dbx, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer dbx.Close()

	mgr := NewCollectionManager(dbx)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode: IDModeAuto,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create unique index: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	ids, err := col.InsertBatch(nil, [][]byte{
		[]byte(`{"email":"ada@example.com"}`),
		[]byte(`{"email":"grace@example.com"}`),
	})
	if err != nil {
		t.Fatalf("insert batch auto id: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("resolved ids len=%d want 2", len(ids))
	}
	if len(ids[0]) == 0 || len(ids[1]) == 0 || bytes.Equal(ids[0], ids[1]) {
		t.Fatalf("invalid auto ids=%x %x", ids[0], ids[1])
	}

	found, err := col.FindByIndex("email_idx", "ada@example.com")
	if err != nil {
		t.Fatalf("find by index: %v", err)
	}
	if len(found) != 1 || !bytes.Equal(found[0], ids[0]) {
		t.Fatalf("found ids=%q want %q", found, ids[0])
	}
}

func TestInsertBatch_RejectsExistingDocumentID_Atomically(t *testing.T) {
	dbx, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer dbx.Close()

	mgr := NewCollectionManager(dbx)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode: IDModeCallerProvided,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"seed"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"name":"dup"}`), []byte(`{"name":"new"}`)},
	); err == nil {
		t.Fatalf("expected existing id conflict")
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get seed: %v", err)
	}
	if !bytes.Equal(got, []byte(`{"name":"seed"}`)) {
		t.Fatalf("seed doc=%q", got)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get new doc: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("unexpected u2 doc=%q", got)
	}
}

func TestInsertBatch_RejectsDuplicateUniqueValue_Atomically(t *testing.T) {
	dbx, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer dbx.Close()

	mgr := NewCollectionManager(dbx)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode: IDModeCallerProvided,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create unique index: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"ada@example.com"}`),
		},
	); err == nil {
		t.Fatalf("expected duplicate unique value conflict")
	}

	for _, id := range [][]byte{[]byte("u1"), []byte("u2")} {
		got, err := col.Get(id)
		if err != nil {
			t.Fatalf("get %q: %v", id, err)
		}
		if len(got) != 0 {
			t.Fatalf("unexpected doc for %q=%q", id, got)
		}
	}
}

func TestInsertBatch_RejectsExistingUniqueValue_Atomically(t *testing.T) {
	dbx, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer dbx.Close()

	mgr := NewCollectionManager(dbx)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode: IDModeCallerProvided,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create unique index: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("seed"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
		},
	); err == nil {
		t.Fatalf("expected existing unique value conflict")
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("unexpected u1 doc=%q", got)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("unexpected u2 doc=%q", got)
	}
}
