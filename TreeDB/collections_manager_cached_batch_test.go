package treedb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestCachedCollectionsInsertBatch_BufferedBeforeCheckpoint(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			IDMode: collections.IDModeCallerProvided,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	ids, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
		},
	)
	if err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("resolved ids len=%d want 2", len(ids))
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get before checkpoint: %v", err)
	}
	if !bytes.Equal(got, []byte(`{"email":"ada@example.com"}`)) {
		t.Fatalf("u1 doc=%q", got)
	}

	found, err := col.FindByIndex("email_idx", "grace@example.com")
	if err != nil {
		t.Fatalf("find by index before checkpoint: %v", err)
	}
	if len(found) != 1 || !bytes.Equal(found[0], []byte("u2")) {
		t.Fatalf("found ids=%q", found)
	}
}

func TestCachedCollectionsInsertBatch_UsesSingleRootBulkMutationCall(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			IDMode: collections.IDModeCallerProvided,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "city_idx", Field: "city"}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema create: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	bulkCalls := 0
	restore := setRootBulkMutationOpsTestHook(func(rootCount int) {
		bulkCalls++
		if rootCount != 4 {
			t.Fatalf("root count=%d want 4", rootCount)
		}
	})
	defer restore()

	ids := make([][]byte, 8)
	docs := make([][]byte, 8)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("u-%d", i))
		docs[i] = []byte(fmt.Sprintf(`{"email":"user-%d@example.com","city":"city-%d"}`, i, i%2))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if bulkCalls != 1 {
		t.Fatalf("bulk calls=%d want 1", bulkCalls)
	}
}
