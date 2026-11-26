package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestS16_3Wave3WithMetaViewFreshnessAndSameCachedCatalog(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsForBenchmark(treedb.ProfileCommandWALRelaxed, dir)
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Close() }()

	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	mgr := NewCollectionManager(backend)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "wave3_meta",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
	}); err != nil {
		t.Fatal(err)
	}
	colA, err := mgr.OpenCollection("wave3_meta")
	if err != nil {
		t.Fatal(err)
	}
	colB, err := mgr.OpenCollection("wave3_meta")
	if err != nil {
		t.Fatal(err)
	}
	if !colA.SameCachedCatalog(colA) {
		t.Fatal("expected SameCachedCatalog(self) == true")
	}
	if !colA.SameCachedCatalog(colB) {
		t.Fatal("expected SameCachedCatalog(same manager handle) == true")
	}

	// Mutate schema via colB and verify colA.WithMetaView sees the fresh index immediately.
	if _, err := colB.CreateIndex(IndexDefinition{Name: "email_1", Field: "email", ValueType: IndexValueString, Unique: true}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	var seenIdx int
	var seenFmt DocumentFormat
	colA.WithMetaView(func(meta *CollectionMeta) {
		seenIdx = len(meta.Indexes)
		seenFmt = meta.Options.DocumentFormat
	})
	if seenIdx != 1 || seenFmt != DocumentFormatBSON {
		t.Fatalf("WithMetaView after CreateIndex got indexes=%d fmt=%q want 1, bson", seenIdx, seenFmt)
	}

	if _, err := colB.DropIndex("email_1"); err != nil {
		t.Fatalf("DropIndex: %v", err)
	}
	colA.WithMetaView(func(meta *CollectionMeta) {
		seenIdx = len(meta.Indexes)
	})
	if seenIdx != 0 {
		t.Fatalf("WithMetaView after DropIndex got indexes=%d want 0", seenIdx)
	}
}

func TestS16_3Wave3PrimaryOnlyBSONSetColdScanLargeValuesAndReopen(t *testing.T) {
	dir := t.TempDir()
	openDB := func() (*treedb.DB, *CollectionManager, *Collection) {
		opts := treedb.OptionsForBenchmark(treedb.ProfileCommandWALRelaxed, dir)
		opts.MemtableMode = "cow_btree"
		opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
		owner, err := treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err := owner.NativeSemanticBackend()
		if err != nil {
			_ = owner.Close()
			t.Fatal(err)
		}
		mgr := NewCollectionManager(backend)
		col, err := mgr.OpenCollection("wave3_docs")
		if err != nil {
			if _, err = mgr.CreateCollection(&CollectionMeta{
				Name:    "wave3_docs",
				Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
				Indexes: []IndexDefinition{
					{Name: "email_1", Field: "email", ValueType: IndexValueString, Unique: true},
					{Name: "city_1", Field: "city", ValueType: IndexValueString},
				},
			}); err != nil {
				_ = owner.Close()
				t.Fatal(err)
			}
			col, err = mgr.OpenCollection("wave3_docs")
			if err != nil {
				_ = owner.Close()
				t.Fatal(err)
			}
		}
		return owner, mgr, col
	}

	mustRawInt32 := func(v int32) bson.RawValue {
		t.Helper()
		tp, val, err := bson.MarshalValue(v)
		if err != nil {
			t.Fatal(err)
		}
		return bson.RawValue{Type: tp, Value: val}
	}

	owner, _, col := openDB()
	const numDocs = 45
	ids := make([][]byte, numDocs)
	docs := make([][]byte, numDocs)
	largeBlob := string(bytes.Repeat([]byte("L"), 5000))
	for i := 0; i < numDocs; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%04d", i))
		payload := "small"
		if i%15 == 0 {
			// Exercise large value offload inside cold-scan Checkpoint build.
			payload = largeBlob
		}
		raw, err := bson.Marshal(bson.D{
			{Key: "_id", Value: fmt.Sprintf("doc-%04d", i)},
			{Key: "email", Value: fmt.Sprintf("user-%04d@example.com", i)},
			{Key: "city", Value: fmt.Sprintf("city-%d", i%3)},
			{Key: "score", Value: int32(i)},
			{Key: "blob", Value: payload},
		})
		if err != nil {
			t.Fatal(err)
		}
		docs[i] = raw
	}
	if err := col.InsertBatchValidatedBSONNoResultIDs(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	// Checkpoint exercises BuildNativeOrderedRootColdScan on primary, email_1, city_1
	// including large value-log offload.
	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint after insert: %v", err)
	}

	// Singleton primaryOnlyBSONSet update + no-op + missing + _id rejection.
	matched, modified, err := col.UpdateBSONSet(ids[0], []BSONSetField{{Key: "score", Value: mustRawInt32(900)}})
	if err != nil || !matched || !modified {
		t.Fatalf("singleton UpdateBSONSet matched=%t modified=%t err=%v", matched, modified, err)
	}
	matched, modified, err = col.UpdateBSONSet(ids[0], []BSONSetField{{Key: "score", Value: mustRawInt32(900)}})
	if err != nil || !matched || modified {
		t.Fatalf("singleton unchanged UpdateBSONSet matched=%t modified=%t err=%v", matched, modified, err)
	}
	matched, modified, err = col.UpdateBSONSet([]byte("missing-id"), []BSONSetField{{Key: "score", Value: mustRawInt32(1)}})
	if err != nil || matched || modified {
		t.Fatalf("singleton missing UpdateBSONSet matched=%t modified=%t err=%v", matched, modified, err)
	}
	if _, _, err := col.UpdateBSONSet(ids[0], []BSONSetField{{Key: "_id", Value: mustRawInt32(1)}}); err == nil {
		t.Fatal("expected error modifying _id via UpdateBSONSet")
	}

	// Batched primaryOnlyBSONSet (> 32 items to exercise both <=32 and >32 spec paths).
	batchItems := make([]BSONSetUpdateBatchItem, 0, numDocs-1)
	for i := 1; i < numDocs; i++ {
		batchItems = append(batchItems, BSONSetUpdateBatchItem{
			DocumentID: ids[i],
			Fields:     []BSONSetField{{Key: "score", Value: mustRawInt32(int32(1000 + i))}},
		})
	}
	results, batched, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges(batchItems)
	if err != nil || !batched || len(results) != len(batchItems) {
		t.Fatalf("batch UpdateBSONSet batched=%t len=%d err=%v", batched, len(results), err)
	}
	for i, r := range results {
		if !r.Matched || !r.Modified {
			t.Fatalf("batch item %d matched=%t modified=%t want true/true", i, r.Matched, r.Modified)
		}
	}

	// Duplicate DocumentID rejection in batch (both <=32 and >32).
	_, _, dupErr := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]BSONSetUpdateBatchItem{
		{DocumentID: ids[1], Fields: []BSONSetField{{Key: "score", Value: mustRawInt32(1)}}},
		{DocumentID: ids[1], Fields: []BSONSetField{{Key: "score", Value: mustRawInt32(2)}}},
	})
	if !errors.Is(dupErr, ErrDuplicateDocumentID) {
		t.Fatalf("duplicate ID err=%v want ErrDuplicateDocumentID", dupErr)
	}

	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint after updates: %v", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify every document + large blob + secondary index survived.
	owner2, _, col2 := openDB()
	defer func() { _ = owner2.Close() }()
	for i := 0; i < numDocs; i++ {
		raw, err := col2.Get(ids[i])
		if err != nil {
			t.Fatalf("reopen Get(%s): %v", ids[i], err)
		}
		wantScore := int32(1000 + i)
		if i == 0 {
			wantScore = 900
		}
		if got := bson.Raw(raw).Lookup("score").Int32(); got != wantScore {
			t.Fatalf("reopen doc %d score=%d want %d", i, got, wantScore)
		}
		if i%15 == 0 {
			if got := bson.Raw(raw).Lookup("blob").StringValue(); got != largeBlob {
				t.Fatalf("reopen doc %d large blob len=%d want %d", i, len(got), len(largeBlob))
			}
		}
	}
}
