package collections

import (
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCommandWALNoIndexLargeBatchDirectPublishAndMixedFlush(t *testing.T) {
	for _, tc := range []struct {
		name   string
		format DocumentFormat
	}{
		{name: "json", format: DocumentFormatJSON},
		{name: "bson", format: DocumentFormatBSON},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
			db, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			closeDB := collectionMaintenanceCloseOnce(cleanup)
			defer func() { _ = closeDB() }()

			mgr := NewCollectionManager(db)
			if _, err := mgr.CreateCollection(&CollectionMeta{
				Name: "docs",
				Options: CollectionOptions{
					DocumentFormat:        tc.format,
					DataRootStoragePolicy: RootStorageCompressed,
				},
			}); err != nil {
				t.Fatalf("create collection: %v", err)
			}
			col, err := mgr.OpenCollection("docs")
			if err != nil {
				t.Fatalf("open collection: %v", err)
			}

			makeDoc := func(i int) ([]byte, []byte) {
				id := []byte(fmt.Sprintf("doc-%06d", i))
				if tc.format == DocumentFormatBSON {
					raw, err := bson.Marshal(bson.D{
						{Key: "_id", Value: string(id)},
						{Key: "seq", Value: int32(i)},
					})
					if err != nil {
						t.Fatalf("marshal bson: %v", err)
					}
					return id, raw
				}
				return id, []byte(fmt.Sprintf(`{"_id":"%s","seq":%d}`, id, i))
			}

			// 1. Small batch (< DefaultIndexedWriteMemtableDirectBatchDocuments) buffers.
			smallIDs := make([][]byte, 8)
			smallDocs := make([][]byte, 8)
			for i := range smallIDs {
				smallIDs[i], smallDocs[i] = makeDoc(i)
			}
			if _, err := col.InsertBatch(smallIDs, smallDocs); err != nil {
				t.Fatalf("small insert batch: %v", err)
			}
			if got := col.LastInsertStats().BufferedIndexedBatches; got != 1 {
				t.Fatalf("small batch BufferedIndexedBatches=%d, want 1", got)
			}

			// 2. Large batch (>= DefaultNoIndexCommandWALDirectBatchDocuments) with a duplicate
			// from the earlier small buffered batch must flush the buffer first and reject the duplicate.
			largeN := DefaultNoIndexCommandWALDirectBatchDocuments
			dupIDs := make([][]byte, largeN)
			dupDocs := make([][]byte, largeN)
			dupIDs[0], dupDocs[0] = makeDoc(3)
			for i := 1; i < largeN; i++ {
				dupIDs[i], dupDocs[i] = makeDoc(100 + i)
			}
			if _, err := col.InsertBatch(dupIDs, dupDocs); !errors.Is(err, ErrDocumentExists) {
				t.Fatalf("large batch duplicate err=%v, want ErrDocumentExists", err)
			}

			// 3. Valid large batch bypasses write-domain buffering and publishes directly.
			largeIDs := make([][]byte, largeN)
			largeDocs := make([][]byte, largeN)
			for i := 0; i < largeN; i++ {
				largeIDs[i], largeDocs[i] = makeDoc(100 + i)
			}
			if _, err := col.InsertBatch(largeIDs, largeDocs); err != nil {
				t.Fatalf("large direct insert batch: %v", err)
			}
			stats := col.LastInsertStats()
			if stats.BufferedIndexedBatches != 0 {
				t.Fatalf("large batch BufferedIndexedBatches=%d, want 0 (direct publish)", stats.BufferedIndexedBatches)
			}
			if stats.Documents != largeN {
				t.Fatalf("large batch Documents=%d, want %d", stats.Documents, largeN)
			}

			// Verify both previously-buffered small docs and direct large docs survive Checkpoint + reopen.
			if err := mgr.FlushAll(); err != nil {
				t.Fatalf("flush all: %v", err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("checkpoint: %v", err)
			}
			if err := closeDB(); err != nil {
				t.Fatalf("close db: %v", err)
			}

			db2, cleanup2, err := treedb.OpenBackendWithCachedLeafLog(opts)
			if err != nil {
				t.Fatalf("reopen db: %v", err)
			}
			defer func() { _ = cleanup2() }()
			col2, err := NewCollectionManager(db2).OpenCollection("docs")
			if err != nil {
				t.Fatalf("reopen collection: %v", err)
			}
			for _, checkID := range [][]byte{[]byte("doc-000000"), []byte("doc-000007"), []byte("doc-000100"), []byte(fmt.Sprintf("doc-%06d", 100+largeN-1))} {
				got, err := col2.Get(checkID)
				if err != nil || len(got) == 0 {
					t.Fatalf("Get(%q) after reopen: len=%d err=%v", checkID, len(got), err)
				}
			}
		})
	}
}

func TestCanBufferCommandWALNoIndexInsertBatchThresholdAndCustomMaxDocs(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	db, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = cleanup() }()

	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	defaultMeta := CollectionMeta{Name: "docs"}
	for _, format := range []DocumentFormat{DocumentFormatJSON, DocumentFormatBSON} {
		if !col.canBufferCommandWALNoIndexInsertBatch(defaultMeta, format, nil, DefaultNoIndexCommandWALDirectBatchDocuments-1) {
			t.Fatalf("format=%s below threshold should buffer", format)
		}
		if col.canBufferCommandWALNoIndexInsertBatch(defaultMeta, format, nil, DefaultNoIndexCommandWALDirectBatchDocuments) {
			t.Fatalf("format=%s at threshold should bypass buffer for direct publish", format)
		}
		customMeta := CollectionMeta{
			Name: "docs",
			Options: CollectionOptions{
				BufferedIndexedWriteMaxDocuments: DefaultIndexedWriteMemtableDirectBatchDocuments * 2,
			},
		}
		if !col.canBufferCommandWALNoIndexInsertBatch(customMeta, format, nil, DefaultNoIndexCommandWALDirectBatchDocuments) {
			t.Fatalf("format=%s with custom non-default max docs should keep buffering", format)
		}
	}
	if col.canBufferCommandWALNoIndexInsertBatch(defaultMeta, DocumentFormatTemplateV1, nil, 1) {
		t.Fatal("template-v1 should not use command-WAL no-index buffer")
	}
}
