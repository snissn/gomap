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

func TestCachedCollectionsInsertBatch_UsesSingleRootIteratorMutationCall(t *testing.T) {
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

	iteratorCalls := 0
	restoreIter := setRootIteratorMutationTestHook(func(rootCount int) {
		iteratorCalls++
		if rootCount != 4 {
			t.Fatalf("root count=%d want 4", rootCount)
		}
	})
	defer restoreIter()

	bulkCalls := 0
	restoreBulk := setRootBulkMutationOpsTestHook(func(rootCount int) {
		bulkCalls++
	})
	defer restoreBulk()

	ids := make([][]byte, 8)
	docs := make([][]byte, 8)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("u-%d", i))
		docs[i] = []byte(fmt.Sprintf(`{"email":"user-%d@example.com","city":"city-%d"}`, i, i%2))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if iteratorCalls != 1 {
		t.Fatalf("iterator calls=%d want 1", iteratorCalls)
	}
	if bulkCalls != 0 {
		t.Fatalf("bulk calls=%d want 0", bulkCalls)
	}
}

func TestCachedCollectionsInsertBatch_AutoFlushRefreshesValueLogState(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 128})
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

	makeBatch := func(offset int) ([][]byte, [][]byte) {
		const batchSize = 256
		ids := make([][]byte, batchSize)
		docs := make([][]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			n := offset + i
			ids[i] = []byte(fmt.Sprintf("u-%d", n))
			docs[i] = []byte(fmt.Sprintf(`{"email":"user-%d@example.com","city":"city-%d"}`, n, n%32))
		}
		return ids, docs
	}

	ids, docs := makeBatch(0)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("insert batch 1: %v", err)
	}
	waitForCachedFlushCondition(t, "named roots auto flush after batch 1", func() bool {
		return !d.cached.PendingNamedRoots()
	})

	ids, docs = makeBatch(256)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("insert batch 2 after auto flush: %v", err)
	}
}
