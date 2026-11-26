package collections

import (
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeS16_3OpenCacheInvalidationUniqueConflictsAndBatchPrefetch(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsForBenchmark(treedb.ProfileCommandWALRelaxed, dir)
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = owner.Close()
		}
	}()
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)

	const name = "s16_3_items"
	_, err = manager.CreateCollection(&CollectionMeta{
		Name:    name,
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
		Indexes: []IndexDefinition{
			{Name: "email_1", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city_1", Field: "city", ValueType: IndexValueString, Unique: false},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	col1, err := manager.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	col2, err := manager.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	if col1.nativeSchemaCoordinator != col2.nativeSchemaCoordinator {
		t.Fatalf("expected openCache to reuse schema coordinator")
	}

	// 1. InsertBatchValidatedBSONNoResultIDs happy path + intra-batch unique conflict rejection.
	const n = 200
	ids := make([][]byte, n)
	docs := make([][]byte, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("u-%06d", i)
		raw, mErr := bson.Marshal(bson.D{
			{Key: "_id", Value: id},
			{Key: "email", Value: fmt.Sprintf("user-%06d@example.com", i)},
			{Key: "city", Value: fmt.Sprintf("city-%d", i%5)},
			{Key: "score", Value: int32(i)},
		})
		if mErr != nil {
			t.Fatal(mErr)
		}
		ids[i] = []byte(id)
		docs[i] = raw
	}
	if err := col1.InsertBatchValidatedBSONNoResultIDs(ids, docs); err != nil {
		t.Fatalf("InsertBatchValidatedBSONNoResultIDs: %v", err)
	}

	// Intra-batch duplicate unique email must be rejected without modifying collection.
	dupDocA, _ := bson.Marshal(bson.D{{Key: "_id", Value: "dup-a"}, {Key: "email", Value: "same@example.com"}, {Key: "city", Value: "nyc"}})
	dupDocB, _ := bson.Marshal(bson.D{{Key: "_id", Value: "dup-b"}, {Key: "email", Value: "same@example.com"}, {Key: "city", Value: "la"}})
	if err := col1.InsertBatchValidatedBSONNoResultIDs([][]byte{[]byte("dup-a"), []byte("dup-b")}, [][]byte{dupDocA, dupDocB}); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("expected ErrUniqueIndexConflict for intra-batch duplicate unique key, got %v", err)
	}

	// Existing-row duplicate unique email must also be rejected.
	existingDup, _ := bson.Marshal(bson.D{{Key: "_id", Value: "dup-existing"}, {Key: "email", Value: "user-000010@example.com"}, {Key: "city", Value: "nyc"}})
	if err := col1.InsertBatchValidatedBSONNoResultIDs([][]byte{[]byte("dup-existing")}, [][]byte{existingDup}); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("expected ErrUniqueIndexConflict for existing unique key, got %v", err)
	}

	// 2. Checkpoint so docs live on disk (exercises NativeEntriesAtRootAdmitted during batch prefetch).
	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// 3. Batch UpdateBSONSet including existing + missing IDs in one batch.
	val999 := bson.RawValue{Type: bson.TypeInt32, Value: []byte{0xe7, 0x03, 0x00, 0x00}}
	val1000 := bson.RawValue{Type: bson.TypeInt32, Value: []byte{0xe8, 0x03, 0x00, 0x00}}
	items := []BSONSetUpdateBatchItem{
		{DocumentID: []byte("u-000005"), Fields: []BSONSetField{{Key: "score", Value: val1000}}},
		{DocumentID: []byte("missing-id"), Fields: []BSONSetField{{Key: "score", Value: val999}}},
		{DocumentID: []byte("u-000042"), Fields: []BSONSetField{{Key: "score", Value: val999}}},
		{DocumentID: []byte("u-000099"), Fields: []BSONSetField{{Key: "score", Value: val999}}},
	}
	results, batched, err := col2.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges(items)
	if err != nil {
		t.Fatalf("UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched || len(results) != 4 || !results[0].Matched || results[1].Matched || !results[2].Matched || !results[3].Matched {
		t.Fatalf("unexpected batch update results (batched=%v): %+v", batched, results)
	}

	// 4. Schema change must invalidate openCache cleanly.
	if _, err := col1.DropIndex("city_1"); err != nil {
		t.Fatalf("DropIndex: %v", err)
	}
	colAfterDropIdx, err := manager.OpenCollection(name)
	if err != nil {
		t.Fatalf("OpenCollection after DropIndex: %v", err)
	}
	if len(colAfterDropIdx.Meta().Indexes) != 1 {
		t.Fatalf("expected 1 index after DropIndex, got %d", len(colAfterDropIdx.Meta().Indexes))
	}

	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint after updates: %v", err)
	}

	// Verify reopen durability of updated rows and schema.
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true

	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reBackend, err := reopened.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	reMgr := NewCollectionManager(reBackend)
	defer func() { _ = reMgr.Close() }()
	reCol, err := reMgr.OpenCollection(name)
	if err != nil {
		t.Fatalf("reopen OpenCollection: %v", err)
	}
	raw, err := reCol.Get([]byte("u-000005"))
	if err != nil {
		t.Fatalf("reopen Get u-000005: %v", err)
	}
	if got := bson.Raw(raw).Lookup("score").Int32(); got != 1000 {
		t.Fatalf("expected score=1000 after reopen, got %d", got)
	}
	raw99, err := reCol.Get([]byte("u-000099"))
	if err != nil {
		t.Fatalf("reopen Get u-000099: %v", err)
	}
	if got := bson.Raw(raw99).Lookup("score").Int32(); got != 999 {
		t.Fatalf("expected score=999 after reopen, got %d", got)
	}

	// Confirm missing collection returns ErrCollectionNotFound cleanly.
	if _, err := reMgr.OpenCollection("missing_s16_3_col"); !errors.Is(err, ErrCollectionNotFound) {
		t.Fatalf("expected ErrCollectionNotFound, got %v", err)
	}
}

func TestNativeEngineBuildSharedBatchTightBudgetBackoff(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsForBenchmark(treedb.ProfileCommandWALRelaxed, dir)
	opts.MemtableMode = "cow_btree"
	limits := treedb.DefaultCOWMemtableLimits()
	limits.MaxInFlightBytes = 8 << 20
	limits.MaxGenerationBytes = 16 << 20
	limits.MaxTotalBytes = 64 << 20
	limits.MaxRetiredBytes = 64 << 20
	opts.COWMemtableLimits = limits

	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		_ = owner.Close()
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	_, err = manager.CreateCollection(&CollectionMeta{
		Name:    "tight_build",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
		Indexes: []IndexDefinition{{Name: "k_1", Field: "k", ValueType: IndexValueString, Unique: false}},
	})
	if err != nil {
		_ = manager.Close()
		_ = owner.Close()
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("tight_build")
	if err != nil {
		_ = manager.Close()
		_ = owner.Close()
		t.Fatal(err)
	}
	const count = 4000
	const chunk = 1000
	for base := 0; base < count; base += chunk {
		ids := make([][]byte, chunk)
		docs := make([][]byte, chunk)
		for i := 0; i < chunk; i++ {
			id := fmt.Sprintf("k-%06d", base+i)
			raw, _ := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "k", Value: fmt.Sprintf("v-%06d", base+i)}})
			ids[i] = []byte(id)
			docs[i] = raw
		}
		if err := col.InsertBatchValidatedBSONNoResultIDs(ids, docs); err != nil {
			_ = manager.Close()
			_ = owner.Close()
			t.Fatalf("insert batch %d: %v", base, err)
		}
	}
	if err := owner.Checkpoint(); err != nil {
		_ = manager.Close()
		_ = owner.Close()
		t.Fatalf("Checkpoint under tight budget: %v", err)
	}
	if err := manager.Close(); err != nil {
		_ = owner.Close()
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reBackend, err := reopened.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	reMgr := NewCollectionManager(reBackend)
	defer func() { _ = reMgr.Close() }()
	reCol, err := reMgr.OpenCollection("tight_build")
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 1234, 3999} {
		raw, err := reCol.Get([]byte(fmt.Sprintf("k-%06d", i)))
		if err != nil {
			t.Fatalf("reopen Get k-%06d: %v", i, err)
		}
		if got := bson.Raw(raw).Lookup("k").StringValue(); got != fmt.Sprintf("v-%06d", i) {
			t.Fatalf("reopen k-%06d got %q", i, got)
		}
	}
}
