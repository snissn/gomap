package collections

import (
	"bytes"
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestColumnStoreWritesRequireCommandWALM10B(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	_, err = col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","did":"d1"}`)})
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "InsertBatch")
	assertColumnStoreDocumentMissingM10B(t, col, "e1")
	assertColumnStoreWriteDomainEmptyM10B(t, col)
}

func TestRecordColumnPublishTimingMirrorsPhysicalEntryLookupWork(t *testing.T) {
	stats := &CollectionInsertStats{}
	recordColumnPublishTiming(stats, backenddb.CommandWALPublishTiming{
		FinalizeCandidateResourceWork: rootpublication.StableResourceClosureWork{
			PhysicalEntryLookupProbes:               11,
			PhysicalEntryLookupComparisons:          7,
			PhysicalEntryLookupAdmissions:           5,
			AggregateMembershipProbes:               19,
			AggregateMembershipNodeVisits:           23,
			AggregateMembershipNodeCopies:           29,
			AggregateMembershipAdmissions:           31,
			FinalRequirementProofFastPath:           3,
			FinalRequirementProofFallbacks:          2,
			FinalRequirementRecordsDecoded:          13,
			FinalRequirementObligationsMaterialized: 17,
		},
	})
	got := stats.ColumnPublishFinalizeCandidateResourceWork
	if got.PhysicalEntryLookupProbes != 11 || got.PhysicalEntryLookupComparisons != 7 || got.PhysicalEntryLookupAdmissions != 5 {
		t.Fatalf("physical lookup work=%+v, want probes=11 comparisons=7 admissions=5", got)
	}
	if got.AggregateMembershipProbes != 19 || got.AggregateMembershipNodeVisits != 23 || got.AggregateMembershipNodeCopies != 29 || got.AggregateMembershipAdmissions != 31 {
		t.Fatalf("aggregate membership work=%+v", got)
	}
	if got.FinalRequirementProofFastPath != 3 || got.FinalRequirementProofFallbacks != 2 || got.FinalRequirementRecordsDecoded != 13 || got.FinalRequirementObligationsMaterialized != 17 {
		t.Fatalf("final requirement work=%+v", got)
	}
}

func TestColumnStoreBenchmarkRelaxedRejectsBufferedWritesM10B(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    t.TempDir(),
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	cfg := testColumnStoreConfig(nil)
	cfg.ProfileSupport = ColumnStoreProfileBenchmarkRelaxed
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "events",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
			ColumnStore:    cfg,
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	doc, err := bson.Marshal(bson.D{
		{Key: "time_us", Value: int64(1)},
		{Key: "kind", Value: "like"},
		{Key: "did", Value: "d1"},
	})
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	_, err = col.Insert([]byte("e1"), doc)
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "Insert")
	assertColumnStoreDocumentMissingM10B(t, col, "e1")
	_, err = col.InsertBatchValidatedBSON([][]byte{[]byte("e2")}, [][]byte{doc})
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "InsertBatchValidatedBSON")
	assertColumnStoreDocumentMissingM10B(t, col, "e2")
	assertColumnStoreWriteDomainEmptyM10B(t, col)
}

func TestColumnStoreStaleNoIndexBufferedInsertRechecksCommandWALAfterCatalogRefreshM10B(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    t.TempDir(),
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "events",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	stale, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection stale: %v", err)
	}
	enableColumnStoreForExistingCollectionM10B(t, d, "events", ColumnStoreProfileBenchmarkRelaxed, mgr)

	_, err = stale.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`))
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "stale Insert")
	assertColumnStoreDocumentMissingM10B(t, stale, "e1")
	assertColumnStoreWriteDomainEmptyM10B(t, stale)
}

func TestColumnStoreStaleBufferedNoIndexFlushRechecksCommandWALM10B(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    t.TempDir(),
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "events",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	stale, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection stale: %v", err)
	}
	doc, err := bson.Marshal(bson.D{
		{Key: "time_us", Value: int64(1)},
		{Key: "kind", Value: "like"},
		{Key: "did", Value: "d1"},
	})
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	insertedIDs, err := stale.InsertBatchValidatedBSON([][]byte{[]byte("e1")}, [][]byte{doc})
	if err != nil {
		t.Fatalf("InsertBatchValidatedBSON buffered: %v", err)
	}
	if len(insertedIDs) != 1 {
		t.Fatalf("InsertBatchValidatedBSON buffered inserted=%d, want 1", len(insertedIDs))
	}
	if stale.writeDomain == nil || stale.writeDomain.count == 0 {
		t.Fatalf("stale write domain was not buffered before column-store enablement")
	}

	enableColumnStoreForExistingCollectionM10B(t, d, "events", ColumnStoreProfileBenchmarkRelaxed, mgr)

	err = stale.Flush()
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "stale Flush")
	assertColumnStorePersistedDocumentMissingM10B(t, d, "events", "e1")
}

func TestColumnStoreStaleUpdateCallbacksRecheckCommandWALAfterCatalogRefreshM10B(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    t.TempDir(),
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "events",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	stale, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection stale: %v", err)
	}
	doc, err := bson.Marshal(bson.D{
		{Key: "time_us", Value: int64(1)},
		{Key: "kind", Value: "like"},
		{Key: "did", Value: "d1"},
	})
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	if _, err := stale.InsertBatchValidatedBSON([][]byte{[]byte("e1")}, [][]byte{doc}); err != nil {
		t.Fatalf("InsertBatchValidatedBSON seed: %v", err)
	}
	if err := stale.Flush(); err != nil {
		t.Fatalf("Flush seed: %v", err)
	}
	enableColumnStoreForExistingCollectionM10B(t, d, "events", ColumnStoreProfileBenchmarkRelaxed, mgr)
	before, err := stale.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get before stale update: %v", err)
	}

	updateCalled := false
	matched, modified, err := stale.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		updateCalled = true
		return current, true, nil
	})
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "stale Update")
	if matched || modified {
		t.Fatalf("stale Update matched=%v modified=%v, want false/false on rejected write", matched, modified)
	}
	if updateCalled {
		t.Fatal("stale Update callback ran before refreshed column-store command WAL validation")
	}

	batchCalled := false
	results, err := stale.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("e1"),
		Update: func(current []byte) ([]byte, bool, error) {
			batchCalled = true
			return current, true, nil
		},
	}})
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "stale UpdateBatch")
	if len(results) != 0 {
		t.Fatalf("stale UpdateBatch results len=%d, want 0 on rejected write", len(results))
	}
	if batchCalled {
		t.Fatal("stale UpdateBatch callback ran before refreshed column-store command WAL validation")
	}
	after, err := stale.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get after stale updates: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("document changed after stale rejected updates: before=%x after=%x", before, after)
	}
	assertColumnStoreWriteDomainEmptyM10B(t, stale)
}

func TestColumnStoreBufferedDeletePathsRequireCommandWALM10B(t *testing.T) {
	for _, tc := range []struct {
		name string
		del  func(*Collection) error
	}{
		{
			name: "DeleteDocument",
			del: func(col *Collection) error {
				deleted, err := col.DeleteDocument([]byte("e1"))
				if deleted {
					return errors.New("DeleteDocument deleted=true, want rejected before delete")
				}
				return err
			},
		},
		{
			name: "DeleteBatch",
			del: func(col *Collection) error {
				deleted, err := col.DeleteBatch([][]byte{[]byte("e1")})
				if deleted != 0 {
					return errors.New("DeleteBatch deleted rows before rejection")
				}
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, col := openBufferedBSONColumnStoreSeedM10B(t, true, ColumnStoreProfileBenchmarkRelaxed)
			defer func() { _ = d.Close() }()

			err := tc.del(col)
			assertColumnStoreCommandWALWriteRejectedM10B(t, err, tc.name)
			got, err := col.Get([]byte("e1"))
			if err != nil {
				t.Fatalf("Get after rejected delete: %v", err)
			}
			if got == nil {
				t.Fatalf("document was deleted after rejected %s", tc.name)
			}
			assertColumnStoreWriteDomainEmptyM10B(t, col)
		})
	}
}

func TestColumnStoreBufferedUpdatePathRequiresCommandWALM10B(t *testing.T) {
	d, col := openBufferedBSONColumnStoreSeedM10B(t, false, ColumnStoreProfileBenchmarkRelaxed)
	defer func() { _ = d.Close() }()

	before, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get before update: %v", err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("e1"), []BSONSetField{{
		Key:   "kind",
		Value: mustBSONRawValue(t, "post"),
	}})
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "UpdateBSONSet")
	if matched || modified {
		t.Fatalf("UpdateBSONSet matched=%v modified=%v, want false/false on rejected write", matched, modified)
	}
	after, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get after rejected update: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("document changed after rejected update: before=%x after=%x", before, after)
	}
	assertColumnStoreWriteDomainEmptyM10B(t, col)
}

func TestColumnStoreUpdateCallbacksRequireCommandWALBeforeInvocationM10B(t *testing.T) {
	d, col := openBufferedBSONColumnStoreSeedM10B(t, false, ColumnStoreProfileBenchmarkRelaxed)
	defer func() { _ = d.Close() }()

	before, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get before update: %v", err)
	}
	callbackCalled := false
	matched, modified, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		callbackCalled = true
		return current, true, nil
	})
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "Update")
	if matched || modified {
		t.Fatalf("Update matched=%v modified=%v, want false/false on rejected write", matched, modified)
	}
	if callbackCalled {
		t.Fatal("Update callback ran before column-store command WAL validation")
	}

	batchCallbackCalled := false
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("e1"),
		Update: func(current []byte) ([]byte, bool, error) {
			batchCallbackCalled = true
			return current, true, nil
		},
	}})
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "UpdateBatch")
	if len(results) != 0 {
		t.Fatalf("UpdateBatch results len=%d, want 0 on rejected write", len(results))
	}
	if batchCallbackCalled {
		t.Fatal("UpdateBatch callback ran before column-store command WAL validation")
	}

	after, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get after rejected updates: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("document changed after rejected updates: before=%x after=%x", before, after)
	}
	assertColumnStoreWriteDomainEmptyM10B(t, col)
}

func TestColumnStoreCommandWALInsertPublishesManifestM10B(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()

	mgr := NewCollectionManager(d)
	col := openColumnStoreCollectionM10B(t, d, mgr)
	baseLSN := d.State().AppliedCommandLSN
	if baseLSN == 0 {
		t.Fatal("create AppliedCommandLSN=0, want command WAL create LSN")
	}

	insertedIDs, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	})
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if len(insertedIDs) != 2 {
		t.Fatalf("InsertBatch inserted=%d, want 2", len(insertedIDs))
	}
	insertLSN := d.State().AppliedCommandLSN
	if insertLSN == 0 || insertLSN <= baseLSN {
		t.Fatalf("insert AppliedCommandLSN=%d, want non-zero LSN greater than create LSN %d", insertLSN, baseLSN)
	}
	assertColumnManifestStateM10B(t, col, 1, insertLSN, mgr)
	assertDBAppliedCommandLSNM10B(t, d, insertLSN)
	assertCollectionDocument(t, col, "e1", `{"time_us":1,"kind":"like","did":"d1"}`)
	assertCollectionDocument(t, col, "e2", `{"time_us":2,"kind":"post","did":"d2"}`)

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d = nil
	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopenMgr := NewCollectionManager(reopen)
	reopened := openColumnStoreCollectionM10B(t, reopen, reopenMgr)
	assertColumnManifestStateM10B(t, reopened, 1, insertLSN, reopenMgr)
	assertDBAppliedCommandLSNM10B(t, reopen, insertLSN)
	assertCollectionDocument(t, reopened, "e1", `{"time_us":1,"kind":"like","did":"d1"}`)
	assertCollectionDocument(t, reopened, "e2", `{"time_us":2,"kind":"post","did":"d2"}`)
}

func TestColumnStoreSupportMatrixRejectsNonJSONMutationsBeforeExecutionM12C(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "events",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	doc, err := bson.Marshal(bson.D{
		{Key: "time_us", Value: int64(1)},
		{Key: "kind", Value: "like"},
		{Key: "did", Value: "d1"},
	})
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{doc, doc}); err != nil {
		t.Fatalf("InsertBatchValidatedBSON seed: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("Flush seed: %v", err)
	}
	installSyntheticNonJSONColumnStoreCatalogM12C(t, d, col)
	framesBefore := countCollectionCommandWALFrames(t, dir)
	appliedBefore := d.State().AppliedCommandLSN

	callbackCalled := false
	matched, modified, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		callbackCalled = true
		return current, true, nil
	})
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "Update")
	if matched || modified {
		t.Fatalf("Update matched=%v modified=%v, want false/false on rejected write", matched, modified)
	}
	if callbackCalled {
		t.Fatal("Update callback ran before M12C non-JSON support-matrix rejection")
	}

	batchCallbackCalled := false
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("e1"),
		Update: func(current []byte) ([]byte, bool, error) {
			batchCallbackCalled = true
			return current, true, nil
		},
	}})
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "UpdateBatch")
	if len(results) != 0 {
		t.Fatalf("UpdateBatch results len=%d, want 0 on rejected write", len(results))
	}
	if batchCallbackCalled {
		t.Fatal("UpdateBatch callback ran before M12C non-JSON support-matrix rejection")
	}

	deleted, err := col.DeleteDocument([]byte("e2"))
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "DeleteDocument")
	if deleted {
		t.Fatal("DeleteDocument deleted=true, want rejected before delete")
	}
	deletedRows, err := col.DeleteBatch([][]byte{[]byte("e2"), []byte("missing")})
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "DeleteBatch")
	if deletedRows != 0 {
		t.Fatalf("DeleteBatch deleted=%d, want rejected before delete", deletedRows)
	}

	if got := d.State().AppliedCommandLSN; got != appliedBefore {
		t.Fatalf("AppliedCommandLSN after rejected mutations=%d, want %d", got, appliedBefore)
	}
	if got := countCollectionCommandWALFrames(t, dir); got != framesBefore {
		t.Fatalf("command WAL frames after rejected mutations=%d, want %d", got, framesBefore)
	}
}

func TestColumnStoreMutationAssetsPublishAndReopenM12C(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()

	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		t.Fatalf("InsertBatch seed: %v", err)
	}
	insertLSN := d.State().AppliedCommandLSN
	assertColumnManifestStateM10B(t, col, 1, insertLSN)

	matched, modified, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		return []byte(`{"time_us":3,"kind":"like","did":"d1"}`), true, nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("Update matched=%v modified=%v, want true/true", matched, modified)
	}
	updateLSN := d.State().AppliedCommandLSN
	if updateLSN <= insertLSN {
		t.Fatalf("update AppliedCommandLSN=%d, want greater than insert LSN %d", updateLSN, insertLSN)
	}
	assertColumnManifestStateM10B(t, col, 2, updateLSN)

	deletedRows, err := col.DeleteBatch([][]byte{[]byte("e2"), []byte("missing")})
	if err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	}
	if deletedRows != 1 {
		t.Fatalf("DeleteBatch deleted=%d, want 1", deletedRows)
	}
	deleteWork := col.LastInsertStats().ColumnPublishFinalizeCandidateResourceWork
	if deleteWork.AppendOnlyFastPath == 0 || deleteWork.RemovedObligations != 0 || deleteWork.FullClosureValidations != 0 {
		t.Fatalf("delete tombstone publication work=%+v want append-only tombstone admission", deleteWork)
	}
	deleteLSN := d.State().AppliedCommandLSN
	if deleteLSN <= updateLSN {
		t.Fatalf("delete AppliedCommandLSN=%d, want greater than update LSN %d", deleteLSN, updateLSN)
	}
	assertColumnManifestStateM10B(t, col, 3, deleteLSN)
	assertCollectionDocument(t, col, "e1", `{"time_us":3,"kind":"like","did":"d1"}`)
	gotDeleted, err := col.Get([]byte("e2"))
	if err != nil {
		t.Fatalf("Get deleted e2: %v", err)
	}
	if gotDeleted != nil {
		t.Fatalf("Get deleted e2=%q, want nil", gotDeleted)
	}

	parts := columnManifestAllPartsForCollectionM12C(t, d, col)
	if len(parts) != 3 {
		t.Fatalf("manifest parts=%+v, want insert/update/delete parts", parts)
	}
	assertColumnPhysicalAssetRowsM12C(t, d, col, columnManifestPartByReasonM12C(t, parts, ColumnPublishOperationInsert), ColumnPublishOperationInsert, []string{"e1", "e2"}, []bool{false, false})
	assertColumnPhysicalAssetRowsM12C(t, d, col, columnManifestPartByReasonM12C(t, parts, ColumnPublishOperationUpdate), ColumnPublishOperationUpdate, []string{"e1"}, []bool{false})
	assertColumnPhysicalAssetRowsM12C(t, d, col, columnManifestPartByReasonM12C(t, parts, ColumnPublishOperationDelete), ColumnPublishOperationDelete, []string{"e2"}, []bool{true})

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d = nil
	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	assertColumnManifestStateM10B(t, reopened, 3, deleteLSN)
	assertDBAppliedCommandLSNM10B(t, reopen, deleteLSN)
	assertCollectionDocument(t, reopened, "e1", `{"time_us":3,"kind":"like","did":"d1"}`)
	reopenDeleted, err := reopened.Get([]byte("e2"))
	if err != nil {
		t.Fatalf("reopen Get deleted e2: %v", err)
	}
	if reopenDeleted != nil {
		t.Fatalf("reopen Get deleted e2=%q, want nil", reopenDeleted)
	}
	reopenParts := columnManifestAllPartsForCollectionM12C(t, reopen, reopened)
	if len(reopenParts) != 3 {
		t.Fatalf("reopen manifest parts=%+v, want 3", reopenParts)
	}
	assertColumnPhysicalAssetRowsM12C(t, reopen, reopened, columnManifestPartByReasonM12C(t, reopenParts, ColumnPublishOperationInsert), ColumnPublishOperationInsert, []string{"e1", "e2"}, []bool{false, false})
	assertColumnPhysicalAssetRowsM12C(t, reopen, reopened, columnManifestPartByReasonM12C(t, reopenParts, ColumnPublishOperationUpdate), ColumnPublishOperationUpdate, []string{"e1"}, []bool{false})
	assertColumnPhysicalAssetRowsM12C(t, reopen, reopened, columnManifestPartByReasonM12C(t, reopenParts, ColumnPublishOperationDelete), ColumnPublishOperationDelete, []string{"e2"}, []bool{true})
}

func TestColumnStoreUpdateBatchMutationAssetsPublishM12C(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		t.Fatalf("InsertBatch seed: %v", err)
	}
	insertLSN := d.State().AppliedCommandLSN
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("e1"),
		Update: func(current []byte) ([]byte, bool, error) {
			return []byte(`{"time_us":4,"kind":"share","did":"d1"}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("UpdateBatch results=%+v, want one matched modified", results)
	}
	updateLSN := d.State().AppliedCommandLSN
	if updateLSN <= insertLSN {
		t.Fatalf("update AppliedCommandLSN=%d, want greater than insert LSN %d", updateLSN, insertLSN)
	}
	assertColumnManifestStateM10B(t, col, 2, updateLSN)
	parts := columnManifestAllPartsForCollectionM12C(t, d, col)
	if len(parts) != 2 {
		t.Fatalf("manifest parts=%+v, want insert and update parts", parts)
	}
	assertColumnPhysicalAssetRowsM12C(t, d, col, columnManifestPartByReasonM12C(t, parts, ColumnPublishOperationUpdate), ColumnPublishOperationUpdate, []string{"e1"}, []bool{false})
}

func TestColumnStoreSupportMatrixRejectsNonJSONInsertBeforeCommandAppendM12B(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "events",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
			ColumnStore:    testColumnStoreConfig(nil),
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	framesBefore := countCollectionCommandWALFrames(t, dir)
	appliedBefore := d.State().AppliedCommandLSN
	doc, err := bson.Marshal(bson.D{
		{Key: "time_us", Value: int64(1)},
		{Key: "kind", Value: "like"},
		{Key: "did", Value: "d1"},
	})
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}

	_, err = col.InsertBatchValidatedBSON([][]byte{[]byte("e1")}, [][]byte{doc})
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "InsertBatchValidatedBSON")
	assertColumnStoreDocumentMissingM10B(t, col, "e1")
	if got := countCollectionCommandWALFrames(t, dir); got != framesBefore {
		t.Fatalf("command WAL frames after rejected BSON insert=%d, want %d", got, framesBefore)
	}
	if got := d.State().AppliedCommandLSN; got != appliedBefore {
		t.Fatalf("AppliedCommandLSN after rejected BSON insert=%d, want %d", got, appliedBefore)
	}
}

func TestColumnStoreCommandWALWritesPhysicalColumnAssetsM12A(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()

	mgr := NewCollectionManager(d)
	col := openColumnStoreCollectionM10B(t, d, mgr)
	insertedIDs, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1","commit":{"repo_id":10}}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2","commit":{"repo_id":11}}`),
	})
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if len(insertedIDs) != 2 {
		t.Fatalf("InsertBatch inserted=%d, want 2", len(insertedIDs))
	}
	insertLSN := d.State().AppliedCommandLSN
	assertColumnManifestStateM10B(t, col, 1, insertLSN, mgr)
	refs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	physicalRefs := columnManifestPhysicalAssetRefsForTestM1634(refs)
	if len(physicalRefs) != 1 {
		t.Fatalf("manifest refs=%+v, want one physical asset ref", refs)
	}
	ref := physicalRefs[0]
	if ref.Namespace != col.Meta().Options.ColumnStore.AssetManager.Namespace || ref.Length <= 0 {
		t.Fatalf("invalid physical asset ref: %+v", ref)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref)
	if err != nil {
		t.Fatalf("readColumnPhysicalAssetFromManager: %v", err)
	}
	if checksum := page.Checksum(raw); checksum != ref.Checksum {
		t.Fatalf("physical asset checksum=%d want ref checksum=%d", checksum, ref.Checksum)
	}
	if err := validateColumnPhysicalAssetForManifest(raw, ref, *col.Meta().Options.ColumnStore); err != nil {
		t.Fatalf("validateColumnPhysicalAssetForManifest: %v", err)
	}
	assetPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	if strings.Contains(assetPath, "value_vlog") || strings.Contains(assetPath, "leaf_vlog") {
		t.Fatalf("column asset path must be isolated from row value/leaf logs: %q", assetPath)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d = nil
	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	assertColumnManifestStateM10B(t, reopened, 1, insertLSN)
	reopenRefs := columnManifestAssetRefsForCollectionM12A(t, reopen, reopened)
	reopenPhysicalRefs := columnManifestPhysicalAssetRefsForTestM1634(reopenRefs)
	if len(reopenPhysicalRefs) != 1 || reopenPhysicalRefs[0] != ref {
		t.Fatalf("reopen refs=%+v want %+v", reopenRefs, []ColumnAssetRef{ref})
	}
	reopenSnapshot := columnManifestSnapshotForCollectionM12A(t, reopen, reopened)
	if reopenSnapshot.Generation != 1 || len(reopenSnapshot.Parts) != 1 || reopenSnapshot.Parts[0].AssetRef != reopenPhysicalRefs[0] {
		t.Fatalf("reopen snapshot generation=%d parts=%+v, want only active generation ref %+v", reopenSnapshot.Generation, reopenSnapshot.Parts, reopenPhysicalRefs[0])
	}
	reopenRaw, err := readColumnPhysicalAssetFromManager(reopen.ColumnAssetRootDir(), reopenRefs[0])
	if err != nil {
		t.Fatalf("reopen readColumnPhysicalAssetFromManager: %v", err)
	}
	if err := validateColumnPhysicalAssetForManifest(reopenRaw, reopenRefs[0], *reopened.Meta().Options.ColumnStore); err != nil {
		t.Fatalf("reopen validateColumnPhysicalAssetForManifest: %v", err)
	}
}

func TestColumnStoreInvalidDeclaredColumnRejectedBeforeCommandWALM12A(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	col := openColumnStoreCollectionM10B(t, d, mgr)
	framesBefore := countCollectionCommandWALFrames(t, dir)
	appliedBefore := d.State().AppliedCommandLSN
	_, err := col.InsertBatch([][]byte{[]byte("bad")}, [][]byte{
		[]byte(`{"time_us":"not-an-int","kind":"like","did":"d1"}`),
	})
	if !errors.Is(err, ErrColumnDeclaredValueUnsupported) {
		t.Fatalf("InsertBatch error=%v, want ErrColumnDeclaredValueUnsupported", err)
	}
	if got := countCollectionCommandWALFrames(t, dir); got != framesBefore {
		t.Fatalf("command WAL frames after invalid declared column=%d, want %d", got, framesBefore)
	}
	if got := d.State().AppliedCommandLSN; got != appliedBefore {
		t.Fatalf("AppliedCommandLSN after invalid declared column=%d, want %d", got, appliedBefore)
	}
	assertColumnStoreDocumentMissingM10B(t, col, "bad")
	assertColumnStorePersistedDocumentMissingM10B(t, d, "events", "bad")
	if err := d.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("CheckCommandWALPublishReady after rejected declared column: %v", err)
	}
}

func TestColumnStoreCommandWALReplayPublishesManifestM10B(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)

	mgr := NewCollectionManager(d)
	col := openColumnStoreCollectionM10B(t, d, mgr)
	baseLSN := d.State().AppliedCommandLSN
	if baseLSN == 0 {
		_ = d.Close()
		t.Fatal("create AppliedCommandLSN=0, want command WAL create LSN")
	}
	docs := []commitlog.CollectionDocument{{
		ID:       []byte("e1"),
		Document: []byte(`{"time_us":1,"kind":"like","did":"d1"}`),
	}}
	intent, err := col.newCollectionInsertCommandWALIntent(docs, nil, nil)
	if err != nil {
		_ = d.Close()
		t.Fatalf("newCollectionInsertCommandWALIntent: %v", err)
	}
	lsn, err := d.AppendCommandWALIntent(intent, true)
	if err != nil {
		_ = d.Close()
		t.Fatalf("AppendCommandWALIntent: %v", err)
	}
	if lsn == 0 || lsn <= baseLSN {
		_ = d.Close()
		t.Fatalf("appended LSN=%d, want non-zero LSN greater than base LSN %d", lsn, baseLSN)
	}
	if got := d.State().AppliedCommandLSN; got != baseLSN {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN before replay=%d, want %d", got, baseLSN)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close before replay: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopenMgr := NewCollectionManager(reopen)
	reopened := openColumnStoreCollectionM10B(t, reopen, reopenMgr)
	assertCollectionDocument(t, reopened, "e1", `{"time_us":1,"kind":"like","did":"d1"}`)
	assertColumnManifestStateM10B(t, reopened, 1, lsn, reopenMgr)
	assertDBAppliedCommandLSNM10B(t, reopen, lsn)
}

func TestColumnStoreInsertWithCommandWALIntentPreservesCallerPublishTimingM10B(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	col := openColumnStoreCollectionM10B(t, d, mgr)
	ids := [][]byte{[]byte("timed-e1")}
	docs := [][]byte{[]byte(`{"time_us":1,"kind":"like","did":"d1"}`)}
	intent, err := col.newCollectionInsertCommandWALIntent([]commitlog.CollectionDocument{{ID: ids[0], Document: docs[0]}}, nil, nil)
	if err != nil {
		t.Fatalf("newCollectionInsertCommandWALIntent: %v", err)
	}
	if _, err := d.AppendCommandWALIntent(intent, true); err != nil {
		t.Fatalf("AppendCommandWALIntent: %v", err)
	}
	var callerTiming backenddb.CommandWALPublishTiming
	intent.SetPublishTiming(&callerTiming)
	if _, err := col.InsertBatchWithCommandWALIntent(ids, docs, false, intent); err != nil {
		t.Fatalf("InsertBatchWithCommandWALIntent: %v", err)
	}
	if restored := intent.SetPublishTiming(nil); restored != &callerTiming {
		t.Fatalf("caller publish timing target was not restored: got %p, want %p", restored, &callerTiming)
	}
	// Individual sub-millisecond phases can measure as zero on Windows'
	// coarser monotonic clock, including both apply phases. The target identity
	// assertion above is the portable preservation check; require measured work
	// as an additional signal where the clock is sufficiently fine-grained.
	if runtime.GOOS != "windows" && callerTiming.RootApply+callerTiming.SystemApply <= 0 {
		t.Fatalf("caller publish apply timing root=%s system=%s, want combined > 0", callerTiming.RootApply, callerTiming.SystemApply)
	}
}

func TestColumnStoreStaleColumnRootPreflightDoesNotAppendCommandWALM10B(t *testing.T) {
	for _, tc := range []struct {
		name    string
		publish func(*Collection, columnWritePublishInput) error
	}{
		{
			name: "iterator",
			publish: func(col *Collection, input columnWritePublishInput) error {
				_, _, _, _, err := col.publishRootDeltaGroupMaybeColumn(nil, input)
				return err
			},
		},
		{
			name: "batch",
			publish: func(col *Collection, input columnWritePublishInput) error {
				_, _, _, _, err := col.publishRootDeltaBatchGroupMaybeColumn(nil, nil, input)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, _ := prepareColumnStoreCommandWALDirM10B(t)
			d := openCollectionCommandWALDB(t, dir)
			defer func() { _ = d.Close() }()

			mgr := NewCollectionManager(d)
			stale := openColumnStoreCollectionM10B(t, d, mgr)
			fresh := openColumnStoreCollectionM10B(t, d, mgr)
			staleCommitSeq, staleSystemRoot := dbCommitSeqAndSystemRoot(d)
			stale.catalogMu.RLock()
			staleCatalog := stale.catalog
			stale.catalogMu.RUnlock()
			if got := staleCatalog.rootID(collectionColumnManifestRootName("events")); got != 0 {
				t.Fatalf("stale column root=%d, want initial zero root", got)
			}

			if _, err := fresh.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","did":"d1"}`)}); err != nil {
				t.Fatalf("InsertBatch fresh: %v", err)
			}
			current := openColumnStoreCollectionM10B(t, d, mgr)
			currentMeta := current.Meta()
			if currentMeta.Options.ColumnStore == nil || currentMeta.Options.ColumnStore.ActiveManifest == nil {
				t.Fatalf("fresh insert did not publish active manifest: %+v", currentMeta.Options.ColumnStore)
			}
			staleMeta := stale.Meta()
			if staleMeta.Options.ColumnStore == nil || staleMeta.Options.ColumnStore.ActiveManifest != nil {
				t.Fatalf("stale handle has unexpected column manifest metadata: %+v", staleMeta.Options.ColumnStore)
			}
			framesBefore := countCollectionCommandWALFrames(t, dir)
			appliedBefore := d.State().AppliedCommandLSN
			registry := d.StableResourceIdentityPinRegistry()
			baselinePins := registry.ActivePins()
			baselineIdentities := registry.ActiveIdentities()

			intent, err := stale.newCollectionInsertCommandWALIntent([]commitlog.CollectionDocument{{
				ID:       []byte("stale"),
				Document: []byte(`{"time_us":2,"kind":"post","did":"d2"}`),
			}}, nil, nil)
			if err != nil {
				t.Fatalf("newCollectionInsertCommandWALIntent: %v", err)
			}
			err = tc.publish(stale, columnWritePublishInput{
				meta:             staleMeta,
				catalog:          staleCatalog,
				baseCommitSeq:    staleCommitSeq,
				baseSystemRoot:   staleSystemRoot,
				commandWALIntent: intent,
				operation:        ColumnPublishOperationInsert,
				documents: []columnWriteDocument{{
					ID:       []byte("stale"),
					Document: []byte(`{"time_us":2,"kind":"post","did":"d2"}`),
				}},
				rows: 1,
			})
			if !errors.Is(err, ErrConcurrentMutation) {
				t.Fatalf("stale column-root publish error=%v, want ErrConcurrentMutation", err)
			}
			if !strings.Contains(err.Error(), collectionColumnManifestRootName("events")) {
				t.Fatalf("stale column-root publish error=%v, want column manifest root in error", err)
			}
			if strings.Contains(err.Error(), "concurrent schema modification") {
				t.Fatalf("stale column-root publish error=%v, want retryable root modification not schema drift", err)
			}
			if got := countCollectionCommandWALFrames(t, dir); got != framesBefore {
				t.Fatalf("command WAL frames after stale preflight=%d, want %d", got, framesBefore)
			}
			if got := d.State().AppliedCommandLSN; got != appliedBefore {
				t.Fatalf("AppliedCommandLSN after stale preflight=%d, want %d", got, appliedBefore)
			}
			if got := registry.ActivePins(); got != baselinePins {
				t.Fatalf("stable asset pins after failed publish=%d want baseline %d", got, baselinePins)
			}
			if got := registry.ActiveIdentities(); got != baselineIdentities {
				t.Fatalf("stable asset identities after failed publish=%d want baseline %d", got, baselineIdentities)
			}
		})
	}
}

func TestColumnStoreCommandWALReplayInsertPublishesEquivalentManifestM10C(t *testing.T) {
	dir, baseLSN := prepareColumnStoreCommandWALDirM10B(t)

	insertPayload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("events", []commitlog.CollectionDocument{
		{ID: []byte("e1"), Document: []byte(`{"time_us":1,"kind":"like","did":"d1"}`)},
		{ID: []byte("e2"), Document: []byte(`{"time_us":2,"kind":"post","did":"d2"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, baseLSN+1, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, insertPayload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	assertCollectionDocument(t, reopened, "e1", `{"time_us":1,"kind":"like","did":"d1"}`)
	assertCollectionDocument(t, reopened, "e2", `{"time_us":2,"kind":"post","did":"d2"}`)
	assertColumnManifestStateM10B(t, reopened, 1, baseLSN+1)
	if got := reopen.State().AppliedCommandLSN; got != baseLSN+1 {
		t.Fatalf("AppliedCommandLSN after replay=%d, want %d", got, baseLSN+1)
	}
}

func TestColumnStoreCommandWALReplayPublishesMutationAssetsM12C(t *testing.T) {
	for _, tc := range []struct {
		name          string
		kind          commitlog.CommandKind
		format        commitlog.PayloadFormat
		payload       func(*testing.T) []byte
		wantOperation ColumnPublishOperation
		wantIDs       []string
		wantDeleted   []bool
		verifyRows    func(*testing.T, *Collection)
	}{
		{
			name:          "update",
			kind:          commitlog.CommandKindCollectionUpdateBatchByID,
			format:        commitlog.PayloadFormatCollectionUpdateBatchByIDV1,
			wantOperation: ColumnPublishOperationUpdate,
			wantIDs:       []string{"e1"},
			wantDeleted:   []bool{false},
			payload: func(t *testing.T) []byte {
				t.Helper()
				payload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload("events", []commitlog.CollectionDocument{{
					ID:       []byte("e1"),
					Document: []byte(`{"time_us":2,"kind":"like","did":"d1"}`),
				}})
				if err != nil {
					t.Fatalf("EncodeCollectionUpdateBatchByIDPayload: %v", err)
				}
				return payload
			},
			verifyRows: func(t *testing.T, col *Collection) {
				t.Helper()
				assertCollectionDocument(t, col, "e1", `{"time_us":2,"kind":"like","did":"d1"}`)
			},
		},
		{
			name:          "delete",
			kind:          commitlog.CommandKindCollectionDeleteBatchByID,
			format:        commitlog.PayloadFormatCollectionDeleteBatchByIDV1,
			wantOperation: ColumnPublishOperationDelete,
			wantIDs:       []string{"e1"},
			wantDeleted:   []bool{true},
			payload: func(t *testing.T) []byte {
				t.Helper()
				payload, err := commitlog.EncodeCollectionDeleteBatchByIDPayload("events", [][]byte{[]byte("e1")})
				if err != nil {
					t.Fatalf("EncodeCollectionDeleteBatchByIDPayload: %v", err)
				}
				return payload
			},
			verifyRows: func(t *testing.T, col *Collection) {
				t.Helper()
				got, err := col.Get([]byte("e1"))
				if err != nil {
					t.Fatalf("Get deleted e1: %v", err)
				}
				if got != nil {
					t.Fatalf("Get deleted e1=%q, want nil", got)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, baseLSN := prepareColumnStoreCommandWALDirM10B(t)
			insertPayload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("events", []commitlog.CollectionDocument{{
				ID:       []byte("e1"),
				Document: []byte(`{"time_us":1,"kind":"like","did":"d1"}`),
			}})
			if err != nil {
				t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
			}
			writeCollectionCommandWALFrame(t, dir, baseLSN+1, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, insertPayload)
			writeCollectionCommandWALFrame(t, dir, baseLSN+2, tc.kind, tc.format, tc.payload(t))

			reopen := openCollectionCommandWALDB(t, dir)
			defer func() { _ = reopen.Close() }()
			reopened := openColumnStoreCollectionM10B(t, reopen)
			tc.verifyRows(t, reopened)
			assertColumnManifestStateM10B(t, reopened, 2, baseLSN+2)
			parts := columnManifestAllPartsForCollectionM12C(t, reopen, reopened)
			if len(parts) != 2 {
				t.Fatalf("replay manifest parts=%+v, want insert and %s parts", parts, tc.name)
			}
			assertColumnPhysicalAssetRowsM12C(t, reopen, reopened, columnManifestPartByReasonM12C(t, parts, ColumnPublishOperationInsert), ColumnPublishOperationInsert, []string{"e1"}, []bool{false})
			assertColumnPhysicalAssetRowsM12C(t, reopen, reopened, columnManifestPartByReasonM12C(t, parts, tc.wantOperation), tc.wantOperation, tc.wantIDs, tc.wantDeleted)
		})
	}
}

func TestColumnStoreReplayIntentBypassesRelaxedDurabilityGateM10C(t *testing.T) {
	dir, baseLSN := prepareColumnStoreCommandWALDirWithProfileM10C(t, ColumnStoreProfileBenchmarkRelaxed)
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("events", []commitlog.CollectionDocument{
		{ID: []byte("e1"), Document: []byte(`{"time_us":1,"kind":"like","did":"d1"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, baseLSN+1, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)

	reopen, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		CommandWAL:             true,
		Durability:             backenddb.DurabilityWALOnRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open relaxed command WAL DB: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	assertCollectionDocument(t, reopened, "e1", `{"time_us":1,"kind":"like","did":"d1"}`)
	assertColumnManifestStateM10B(t, reopened, 1, baseLSN+1)
	if got := reopen.State().AppliedCommandLSN; got != baseLSN+1 {
		t.Fatalf("AppliedCommandLSN after relaxed replay=%d, want %d", got, baseLSN+1)
	}

	framesBefore := countCollectionCommandWALFrames(t, dir)
	_, err = reopened.InsertBatch([][]byte{[]byte("e2")}, [][]byte{[]byte(`{"time_us":2,"kind":"post","did":"d2"}`)})
	if !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("foreground relaxed InsertBatch error=%v, want ErrCommandWALRejected", err)
	}
	if framesAfter := countCollectionCommandWALFrames(t, dir); framesAfter != framesBefore {
		t.Fatalf("command WAL frames after rejected foreground write=%d, want %d", framesAfter, framesBefore)
	}
	if err := reopen.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("CheckCommandWALPublishReady after rejected foreground write: %v", err)
	}
}

func TestColumnStoreAssignedForegroundIntentDoesNotBypassRelaxedDurabilityGateM10C(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirWithProfileM10C(t, ColumnStoreProfileBenchmarkRelaxed)
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		CommandWAL:             true,
		Durability:             backenddb.DurabilityWALOnRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open relaxed command WAL DB: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	docs := []commitlog.CollectionDocument{{
		ID:       []byte("e1"),
		Document: []byte(`{"time_us":1,"kind":"like","did":"d1"}`),
	}}
	intent, err := col.newCollectionInsertCommandWALIntent(docs, nil, nil)
	if err != nil {
		t.Fatalf("newCollectionInsertCommandWALIntent: %v", err)
	}
	lsn, err := d.AppendCommandWALIntent(intent, false)
	if err != nil {
		t.Fatalf("AppendCommandWALIntent: %v", err)
	}
	replayLSN, replay := intent.ReplayAssignedLSN()
	if lsn == 0 || intent.AssignedLSN() != lsn || replay || replayLSN != 0 {
		t.Fatalf("assigned foreground intent lsn=%d assigned=%d replay=(%d,%t)", lsn, intent.AssignedLSN(), replayLSN, replay)
	}
	if err := col.requireColumnStoreCommandWAL(col.meta, intent); !errors.Is(err, backenddb.ErrCommandWALRejected) ||
		!strings.Contains(err.Error(), "relaxed durability modes are unsupported") ||
		!strings.Contains(err.Error(), "command_wal=true") {
		t.Fatalf("assigned foreground relaxed intent error=%v, want ErrCommandWALRejected with relaxed durability diagnostics", err)
	}
	if err := col.requireColumnStoreCommandWAL(col.meta, nil); !errors.Is(err, backenddb.ErrCommandWALRejected) ||
		!strings.Contains(err.Error(), "relaxed durability modes are unsupported") ||
		!strings.Contains(err.Error(), "command_wal=true") {
		t.Fatalf("nil relaxed intent error=%v, want ErrCommandWALRejected with relaxed durability diagnostics", err)
	}
}

func TestColumnStoreReadOnlyOpenWithUnappliedCollectionFrameFailsM10C(t *testing.T) {
	dir, baseLSN := prepareColumnStoreCommandWALDirM10B(t)
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("events", []commitlog.CollectionDocument{
		{ID: []byte("e1"), Document: []byte(`{"time_us":1,"kind":"like","did":"d1"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, baseLSN+1, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)

	ro, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true, DisableBackgroundPrune: true})
	if err == nil {
		_ = ro.Close()
		t.Fatalf("Open read-only with unapplied column collection frame succeeded, want ErrRecoveryRequired")
	}
	if !errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("Open read-only error=%v, want ErrRecoveryRequired", err)
	}
}

func TestColumnStoreRelaxedProfileWritesRejectedBeforeCommandAppendM10C(t *testing.T) {
	tests := []struct {
		name       string
		opts       backenddb.Options
		commandWAL bool
	}{
		{
			name: "wal_on_fast_command_wal",
			opts: backenddb.Options{
				CommandWAL:             true,
				Durability:             backenddb.DurabilityWALOnRelaxed,
				DisableBackgroundPrune: true,
			},
			commandWAL: true,
		},
		{
			name: "fast_no_command_wal",
			opts: backenddb.Options{
				Durability:             backenddb.DurabilityWALOffRelaxed,
				DisableBackgroundPrune: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.opts.Dir = t.TempDir()
			d, err := backenddb.Open(tt.opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = d.Close() }()

			cfg := testColumnStoreConfig(nil)
			cfg.ProfileSupport = ColumnStoreProfileBenchmarkRelaxed
			mgr := NewCollectionManager(d)
			if _, err := mgr.CreateCollection(&CollectionMeta{
				Name:    "events",
				Options: CollectionOptions{ColumnStore: cfg},
			}); err != nil {
				t.Fatalf("CreateCollection: %v", err)
			}
			col, err := mgr.OpenCollection("events")
			if err != nil {
				t.Fatalf("OpenCollection: %v", err)
			}
			framesBefore := countCollectionCommandWALFrames(t, tt.opts.Dir)

			_, err = col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","did":"d1"}`)})
			if !errors.Is(err, backenddb.ErrCommandWALRejected) {
				t.Fatalf("InsertBatch error=%v, want ErrCommandWALRejected", err)
			}
			if framesAfter := countCollectionCommandWALFrames(t, tt.opts.Dir); framesAfter != framesBefore {
				t.Fatalf("command WAL frames after rejected write=%d, want %d", framesAfter, framesBefore)
			}
			deleted, err := col.DeleteDocument([]byte("missing"))
			if !errors.Is(err, backenddb.ErrCommandWALRejected) {
				t.Fatalf("DeleteDocument missing error=%v, want ErrCommandWALRejected", err)
			}
			if deleted {
				t.Fatalf("DeleteDocument missing deleted=true, want rejected before delete")
			}
			framesAfterDelete := countCollectionCommandWALFrames(t, tt.opts.Dir)
			if framesAfterDelete != framesBefore {
				t.Fatalf("command WAL frames after rejected no-op delete=%d, want %d", framesAfterDelete, framesBefore)
			}
			matched, modified, err := col.Update([]byte("missing"), func([]byte) ([]byte, bool, error) {
				return nil, false, nil
			})
			if !errors.Is(err, backenddb.ErrCommandWALRejected) {
				t.Fatalf("Update missing error=%v, want ErrCommandWALRejected", err)
			}
			if matched || modified {
				t.Fatalf("Update missing matched=%v modified=%v, want false/false", matched, modified)
			}
			if framesAfter := countCollectionCommandWALFrames(t, tt.opts.Dir); framesAfter != framesAfterDelete {
				t.Fatalf("command WAL frames after rejected no-op update=%d, want %d", framesAfter, framesAfterDelete)
			}
			if tt.commandWAL {
				if err := d.CheckCommandWALPublishReady(); err != nil {
					t.Fatalf("CheckCommandWALPublishReady after rejected write: %v", err)
				}
			}
		})
	}
}

func TestColumnStoreDeleteMissesRejectedBeforeCommandWALM12B(t *testing.T) {
	d, col := openBufferedBSONColumnStoreSeedM10B(t, false, ColumnStoreProfileBenchmarkRelaxed)
	defer func() { _ = d.Close() }()

	deleted, err := col.DeleteDocument([]byte("missing"))
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "DeleteDocument missing")
	if deleted {
		t.Fatal("DeleteDocument missing deleted=true, want rejected before delete")
	}
	deletedBatch, err := col.DeleteBatch([][]byte{[]byte("missing")})
	assertColumnStoreCommandWALWriteRejectedM10B(t, err, "DeleteBatch missing")
	if deletedBatch != 0 {
		t.Fatalf("DeleteBatch missing deleted=%d, want rejected before delete", deletedBatch)
	}
	if got, err := col.Get([]byte("e1")); err != nil {
		t.Fatalf("Get existing after missing deletes: %v", err)
	} else if len(got) == 0 {
		t.Fatal("Get existing after missing deletes returned empty document")
	}
}

func TestColumnStoreBenchmarkRelaxedAllowsDurableCommandWALWritesM10C(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	cfg := testColumnStoreConfig(nil)
	cfg.ProfileSupport = ColumnStoreProfileBenchmarkRelaxed
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: cfg},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	baseLSN := d.State().AppliedCommandLSN
	if baseLSN == 0 {
		t.Fatal("create AppliedCommandLSN=0, want command WAL create LSN")
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","did":"d1"}`)}); err != nil {
		t.Fatalf("InsertBatch durable benchmark-relaxed: %v", err)
	}
	assertCollectionDocument(t, col, "e1", `{"time_us":1,"kind":"like","did":"d1"}`)
	assertColumnManifestStateM10B(t, col, 1, baseLSN+1)
}

func TestColumnStorePublishRejectsMissingCommandWALIntentM10B(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	col := openColumnStoreCollectionM10B(t, d, mgr)
	input := columnWritePublishInput{
		meta:      col.meta,
		operation: ColumnPublishOperationInsert,
	}
	if _, _, _, _, err := col.publishRootDeltaGroupMaybeColumn(nil, input); !errors.Is(err, backenddb.ErrCommandWALContextMissingFrame) {
		t.Fatalf("publishRootDeltaGroupMaybeColumn error=%v, want ErrCommandWALContextMissingFrame", err)
	} else if errors.Is(err, backenddb.ErrCommandWALUnsupported) {
		t.Fatalf("publishRootDeltaGroupMaybeColumn error=%v must not look like ErrCommandWALUnsupported", err)
	}
	if _, _, _, _, err := col.publishRootDeltaBatchGroupMaybeColumn(nil, nil, input); !errors.Is(err, backenddb.ErrCommandWALContextMissingFrame) {
		t.Fatalf("publishRootDeltaBatchGroupMaybeColumn error=%v, want ErrCommandWALContextMissingFrame", err)
	} else if errors.Is(err, backenddb.ErrCommandWALUnsupported) {
		t.Fatalf("publishRootDeltaBatchGroupMaybeColumn error=%v must not look like ErrCommandWALUnsupported", err)
	}
}

func TestAppendColumnManifestRootPublishBaseAppendsColumnRootM10B(t *testing.T) {
	columnRootName := collectionColumnManifestRootName("events")
	primaryRootName := collectionPrimaryRootName("events")
	rootNames := []string{primaryRootName}
	baseRootIDs := map[string]uint64{
		primaryRootName: 11,
	}

	gotNames, gotBases, err := appendColumnManifestRootPublishBase(rootNames, baseRootIDs, columnRootName, 33)
	if err != nil {
		t.Fatalf("appendColumnManifestRootPublishBase: %v", err)
	}
	wantNames := []string{primaryRootName, columnRootName}
	if len(gotNames) != len(wantNames) {
		t.Fatalf("root names len=%d want %d names=%v", len(gotNames), len(wantNames), gotNames)
	}
	for i := range wantNames {
		if gotNames[i] != wantNames[i] {
			t.Fatalf("rootNames[%d]=%q want %q", i, gotNames[i], wantNames[i])
		}
	}
	if gotBases[columnRootName] != 33 {
		t.Fatalf("column base root=%d want updated base 33", gotBases[columnRootName])
	}
	if gotBases[primaryRootName] != 11 {
		t.Fatalf("primary base root changed: %v", gotBases)
	}
}

func TestAppendColumnManifestRootPublishBaseConsumesOwnedInputsM10B(t *testing.T) {
	columnRootName := collectionColumnManifestRootName("events")
	primaryRootName := collectionPrimaryRootName("events")
	rootNames := make([]string, 0, 2)
	rootNames = append(rootNames, primaryRootName)
	baseRootIDs := map[string]uint64{
		primaryRootName: 11,
	}

	gotNames, gotBases, err := appendColumnManifestRootPublishBase(rootNames, baseRootIDs, columnRootName, 33)
	if err != nil {
		t.Fatalf("appendColumnManifestRootPublishBase: %v", err)
	}
	if len(gotNames) == 0 || &gotNames[0] != &rootNames[0] {
		t.Fatalf("root names were not appended into the owned backing array: got=%v input=%v", gotNames, rootNames)
	}
	if gotBases[columnRootName] != 33 || baseRootIDs[columnRootName] != 33 {
		t.Fatalf("column base root not recorded through owned map: got=%v input=%v", gotBases, baseRootIDs)
	}
}

func TestColumnPublishRootInputClonesProtectCallersM10B(t *testing.T) {
	columnRootName := collectionColumnManifestRootName("events")
	primaryRootName := collectionPrimaryRootName("events")
	rootNames := []string{primaryRootName}
	baseRootIDs := map[string]uint64{
		primaryRootName: 11,
	}

	clonedNames := cloneColumnPublishRootNames(rootNames)
	clonedBases := cloneColumnPublishBaseRootIDs(baseRootIDs)
	gotNames, gotBases, err := appendColumnManifestRootPublishBase(clonedNames, clonedBases, columnRootName, 33)
	if err != nil {
		t.Fatalf("appendColumnManifestRootPublishBase: %v", err)
	}
	if len(gotNames) != 2 || gotNames[0] != primaryRootName || gotNames[1] != columnRootName {
		t.Fatalf("root names=%v, want primary then column", gotNames)
	}
	if gotBases[columnRootName] != 33 {
		t.Fatalf("column base root=%d want 33", gotBases[columnRootName])
	}
	if len(rootNames) != 1 || rootNames[0] != primaryRootName {
		t.Fatalf("caller rootNames mutated: %v", rootNames)
	}
	if _, ok := baseRootIDs[columnRootName]; ok {
		t.Fatalf("caller baseRootIDs mutated with column root: %v", baseRootIDs)
	}
}

func TestAppendColumnManifestRootPublishBaseRejectsDuplicateColumnRootM10B(t *testing.T) {
	columnRootName := collectionColumnManifestRootName("events")
	rootNames := []string{collectionPrimaryRootName("events"), columnRootName}
	baseRootIDs := map[string]uint64{
		collectionPrimaryRootName("events"): 11,
		columnRootName:                      22,
	}

	gotNames, gotBases, err := appendColumnManifestRootPublishBase(rootNames, baseRootIDs, columnRootName, 33)
	if err == nil || !strings.Contains(err.Error(), "must be published by the column context delta") {
		t.Fatalf("appendColumnManifestRootPublishBase error=%v want duplicate column root rejection", err)
	}
	if gotNames != nil || gotBases != nil {
		t.Fatalf("appendColumnManifestRootPublishBase returned names=%v bases=%v on rejection", gotNames, gotBases)
	}
}

func TestEncodeColumnManifestIdentityForWriteRejectsNegativeBytesM10B(t *testing.T) {
	_, err := encodeColumnManifestIdentityForWrite(ColumnPublishManifestEncodeInput{
		Collection:        "events",
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 1,
		Prepared: ColumnPublishPreparedAssets{
			RowCount:           1,
			CommandBytes:       12,
			RowRemainderBytes:  -1,
			ColumnPayloadBytes: 34,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "byte counts cannot be negative") {
		t.Fatalf("encodeColumnManifestIdentityForWrite error=%v want negative byte count rejection", err)
	}
}

func TestPrepareColumnPhysicalAssetRowsKeepsPendingAssetOrder3236(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Dictionary: true},
			{Name: "score", Path: "score", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
			{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		},
		SortKey: []ColumnSortKey{{Column: "score"}},
		AggregateMetadata: []ColumnAggregateMetadata{
			{Name: "typed_score_min_by_did", Column: "score", GroupColumn: "did", Kind: ColumnAggregateMin},
			{Name: "row_time_min_by_kind", Column: "time_us", GroupColumn: "kind", Kind: ColumnAggregateMin},
		},
	})
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := &Collection{db: d}
	rows := []columnDeclaredRow{
		{ID: []byte("e3"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 30},
			{Type: ColumnStoreValueString, Present: true, String: "post"},
			{Type: ColumnStoreValueInt64, Present: true, Int64: 3},
			{Type: ColumnStoreValueString, Present: true, String: "did:b"},
		}},
		{ID: []byte("e1"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 10},
			{Type: ColumnStoreValueString, Present: true, String: "like"},
			{Type: ColumnStoreValueInt64, Present: true, Int64: 1},
			{Type: ColumnStoreValueString, Present: true, String: "did:a"},
		}},
		{ID: []byte("e2"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 20},
			{Type: ColumnStoreValueString, Present: true, String: "like"},
			{Type: ColumnStoreValueInt64, Present: true, Int64: 2},
			{Type: ColumnStoreValueString, Present: true, String: "did:a"},
		}},
	}
	prepared, err := col.prepareColumnPhysicalAssetRowsForCommand(ColumnPublishPreparedAssets{}, columnWritePublishInput{
		meta:      CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}},
		operation: ColumnPublishOperationInsert,
		rows:      len(rows),
	}, ColumnPublishAssetPrepareInput{
		Collection:        "events",
		ColumnStore:       *cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 77,
	}, rows)
	if err != nil {
		t.Fatalf("prepareColumnPhysicalAssetRowsForCommand: %v", err)
	}
	if ordinaryColumnStableAuthorityEnabled() {
		if prepared.stableResources == nil || !prepared.stableResourcesRequired {
			t.Fatalf("production preparation stable resources=%v required=%t want active stable authority", prepared.stableResources, prepared.stableResourcesRequired)
		}
		defer prepared.stableResources.Release()
	} else if prepared.stableResources != nil || prepared.stableResourcesRequired {
		t.Fatalf("production preparation stable resources=%v required=%t want legacy pre-cutover path", prepared.stableResources, prepared.stableResourcesRequired)
	}
	want := []struct {
		kind   ColumnAssetKind
		partID uint64
		reason string
		role   ColumnManifestPartRole
	}{
		{kind: ColumnAssetKindTCS1PartImage, partID: columnPhysicalRowAssetPartID, reason: string(ColumnPublishOperationInsert), role: ColumnManifestPartRoleBase},
		{kind: ColumnAssetKindTCS1TypedColumnPart, partID: typedColumnPartAssetPartID, reason: string(ColumnPublishOperationInsert), role: ColumnManifestPartRoleBase},
		{kind: ColumnAssetKindTCS1AggregateMetadata, partID: typedColumnPartAssetPartID, reason: "typed_score_min_by_did"},
		{kind: ColumnAssetKindTCS1DictionaryCodes, partID: columnPhysicalRowAssetPartID, reason: "kind"},
		{kind: ColumnAssetKindTCS1Int64Values, partID: columnPhysicalRowAssetPartID, reason: "time_us"},
		{kind: ColumnAssetKindTCS1AggregateMetadata, partID: columnPhysicalRowAssetPartID, reason: "row_time_min_by_kind"},
	}
	if len(prepared.Assets) != len(want) {
		t.Fatalf("prepared assets=%d want %d: %+v", len(prepared.Assets), len(want), prepared.Assets)
	}
	for i, expected := range want {
		asset := prepared.Assets[i]
		if asset.Ref.Kind != expected.kind || asset.Ref.PartID != expected.partID || asset.Reason != expected.reason || asset.PartRole != expected.role {
			t.Fatalf("asset[%d]=%+v want kind=%s part_id=%d reason=%q role=%q", i, asset, expected.kind, expected.partID, expected.reason, expected.role)
		}
		if asset.Ref.FileID != columnAssetM12ASegmentFileID {
			t.Fatalf("asset[%d] ref=%+v want shared regular segment file_id=%d", i, asset.Ref, columnAssetM12ASegmentFileID)
		}
		if i > 0 && asset.Ref.Offset <= prepared.Assets[i-1].Ref.Offset {
			t.Fatalf("asset offsets not in pending order at %d: prev=%+v current=%+v", i, prepared.Assets[i-1].Ref, asset.Ref)
		}
	}
	if prepared.AssetMetrics.SharedAppendCount != len(want) {
		t.Fatalf("shared append count=%d want %d", prepared.AssetMetrics.SharedAppendCount, len(want))
	}
	if prepared.AssetMetrics.SharedAppendCloseCount != 1 {
		t.Fatalf("shared append close count=%d want 1", prepared.AssetMetrics.SharedAppendCloseCount)
	}
	if prepared.AssetMetrics.SharedAppendFileSyncCount != 1 {
		t.Fatalf("shared append file sync count=%d want 1", prepared.AssetMetrics.SharedAppendFileSyncCount)
	}
	if prepared.AssetMetrics.SharedAppendSyncEpochCount != 1 {
		t.Fatalf("shared append sync epoch count=%d want 1", prepared.AssetMetrics.SharedAppendSyncEpochCount)
	}
	if prepared.AssetMetrics.RowAssetCount != 1 || prepared.AssetMetrics.TypedColumnPartCount != 1 ||
		prepared.AssetMetrics.DictionarySidecarCount != 1 || prepared.AssetMetrics.Int64SidecarCount != 1 ||
		prepared.AssetMetrics.AggregateMetadataCount != 2 {
		t.Fatalf("asset metrics=%+v want row=1 typed=1 dict=1 int64=1 aggregate=2", prepared.AssetMetrics)
	}
}

func TestPrepareColumnPhysicalAssetRowsCountsDirectViewTypedColumnSync3151(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
			{Name: "score", Path: "score", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
		},
		SortKey: []ColumnSortKey{{Column: "score"}},
	})
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	if !columnStoreConfigNeedsDirectViewTypedColumnAlignment(*cfg) {
		t.Fatal("test config does not use direct-view typed-column alignment")
	}
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := &Collection{db: d}
	rows := []columnDeclaredRow{
		{ID: []byte("e1"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 10},
			{Type: ColumnStoreValueInt64, Present: true, Int64: 1},
		}},
		{ID: []byte("e2"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 20},
			{Type: ColumnStoreValueInt64, Present: true, Int64: 2},
		}},
	}
	prepared, err := col.prepareColumnPhysicalAssetRowsForCommand(ColumnPublishPreparedAssets{}, columnWritePublishInput{
		meta:      CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}},
		operation: ColumnPublishOperationInsert,
		rows:      len(rows),
	}, ColumnPublishAssetPrepareInput{
		Collection:        "events",
		ColumnStore:       *cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 77,
	}, rows)
	if err != nil {
		t.Fatalf("prepareColumnPhysicalAssetRowsForCommand: %v", err)
	}
	if ordinaryColumnStableAuthorityEnabled() {
		if prepared.stableResources == nil || !prepared.stableResourcesRequired {
			t.Fatalf("production preparation stable resources=%v required=%t want active stable authority", prepared.stableResources, prepared.stableResourcesRequired)
		}
		defer prepared.stableResources.Release()
	} else if prepared.stableResources != nil || prepared.stableResourcesRequired {
		t.Fatalf("production preparation stable resources=%v required=%t want legacy pre-cutover path", prepared.stableResources, prepared.stableResourcesRequired)
	}
	directFileID, err := directViewTypedColumnSegmentFileID(1)
	if err != nil {
		t.Fatalf("directViewTypedColumnSegmentFileID: %v", err)
	}
	var sawRowAsset bool
	var sawDirectTypedColumn bool
	var sharedSegmentBytes int64
	var sharedSegmentCount int
	var directViewSegmentBytes int64
	var directViewSegmentCount int
	for _, asset := range prepared.Assets {
		if columnAssetSegmentFileIDIsDirectView(asset.Ref.FileID) {
			directViewSegmentBytes = saturatingAddNonNegativeInt64(directViewSegmentBytes, asset.Ref.Length)
			directViewSegmentCount++
		} else {
			sharedSegmentBytes = saturatingAddNonNegativeInt64(sharedSegmentBytes, asset.Ref.Length)
			sharedSegmentCount++
		}
		switch {
		case asset.Ref.Kind == ColumnAssetKindTCS1PartImage && asset.Ref.PartID == columnPhysicalRowAssetPartID:
			sawRowAsset = true
			if asset.Ref.FileID != columnAssetM12ASegmentFileID {
				t.Fatalf("row asset ref=%+v want shared segment file_id=%d", asset.Ref, columnAssetM12ASegmentFileID)
			}
		case asset.Ref.Kind == ColumnAssetKindTCS1TypedColumnPart && asset.Ref.PartID == typedColumnPartAssetPartID:
			sawDirectTypedColumn = true
			if asset.Ref.FileID != directFileID {
				t.Fatalf("typed-column ref=%+v want direct-view file_id=%d", asset.Ref, directFileID)
			}
			if asset.Ref.Offset%typedColumnPartDirectViewAssetAlignment != 0 {
				t.Fatalf("typed-column offset=%d want %d-byte alignment", asset.Ref.Offset, typedColumnPartDirectViewAssetAlignment)
			}
		}
	}
	if !sawRowAsset || !sawDirectTypedColumn {
		t.Fatalf("prepared assets missing row=%t direct typed=%t: %+v", sawRowAsset, sawDirectTypedColumn, prepared.Assets)
	}
	if prepared.AssetMetrics.SharedAppendCount != 3 {
		t.Fatalf("shared append count=%d want 3", prepared.AssetMetrics.SharedAppendCount)
	}
	if prepared.AssetMetrics.SharedAppendCloseCount != 2 {
		t.Fatalf("shared append close count=%d want 2", prepared.AssetMetrics.SharedAppendCloseCount)
	}
	if prepared.AssetMetrics.SharedAppendFileSyncCount != 2 {
		t.Fatalf("shared append file sync count=%d want 2", prepared.AssetMetrics.SharedAppendFileSyncCount)
	}
	if prepared.AssetMetrics.SharedAppendSyncEpochCount != 2 {
		t.Fatalf("shared append sync epoch count=%d want 2", prepared.AssetMetrics.SharedAppendSyncEpochCount)
	}
	if sharedSegmentCount != 2 || directViewSegmentCount != 1 {
		t.Fatalf("prepared segment class counts shared/direct=%d/%d want 2/1", sharedSegmentCount, directViewSegmentCount)
	}
	if prepared.AssetMetrics.SharedSegmentAppendBytes != sharedSegmentBytes || prepared.AssetMetrics.SharedSegmentAppendCount != sharedSegmentCount {
		t.Fatalf("shared segment append bytes/count=%d/%d want %d/%d", prepared.AssetMetrics.SharedSegmentAppendBytes, prepared.AssetMetrics.SharedSegmentAppendCount, sharedSegmentBytes, sharedSegmentCount)
	}
	if prepared.AssetMetrics.DirectViewSegmentAppendBytes != directViewSegmentBytes || prepared.AssetMetrics.DirectViewSegmentAppendCount != directViewSegmentCount {
		t.Fatalf("direct-view segment append bytes/count=%d/%d want %d/%d", prepared.AssetMetrics.DirectViewSegmentAppendBytes, prepared.AssetMetrics.DirectViewSegmentAppendCount, directViewSegmentBytes, directViewSegmentCount)
	}
	if prepared.AssetMetrics.SharedSegmentAppendCloseCount != 1 || prepared.AssetMetrics.SharedSegmentAppendFileSyncCount != 1 || prepared.AssetMetrics.SharedSegmentAppendSyncEpochCount != 1 {
		t.Fatalf("shared segment append close/file-sync/sync-epoch=%d/%d/%d want 1/1/1",
			prepared.AssetMetrics.SharedSegmentAppendCloseCount,
			prepared.AssetMetrics.SharedSegmentAppendFileSyncCount,
			prepared.AssetMetrics.SharedSegmentAppendSyncEpochCount)
	}
	if prepared.AssetMetrics.DirectViewSegmentAppendCloseCount != 1 || prepared.AssetMetrics.DirectViewSegmentAppendFileSyncCount != 1 || prepared.AssetMetrics.DirectViewSegmentAppendSyncEpochCount != 1 {
		t.Fatalf("direct-view segment append close/file-sync/sync-epoch=%d/%d/%d want 1/1/1",
			prepared.AssetMetrics.DirectViewSegmentAppendCloseCount,
			prepared.AssetMetrics.DirectViewSegmentAppendFileSyncCount,
			prepared.AssetMetrics.DirectViewSegmentAppendSyncEpochCount)
	}
	if prepared.AssetMetrics.RowAssetCount != 1 || prepared.AssetMetrics.TypedColumnPartCount != 1 {
		t.Fatalf("asset metrics=%+v want row=1 typed=1", prepared.AssetMetrics)
	}
}

func TestPrepareColumnPhysicalAssetRowsDoesNotAllocateDirectViewAfterSharedCloseFailure3151(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
			{Name: "score", Path: "score", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
		},
		SortKey: []ColumnSortKey{{Column: "score"}},
	})
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := &Collection{db: d}
	registry := d.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()
	sharedSeedRef, err := writeColumnAssetToManagerSegment(d.ColumnAssetRootDir(), *cfg, []byte("existing-shared"), ColumnAssetKindTCS1PartImage, 99, columnPhysicalRowAssetPartID, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("writeColumnAssetToManagerSegment: %v", err)
	}
	sharedPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), sharedSeedRef)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath shared: %v", err)
	}
	directFileID, err := directViewTypedColumnSegmentFileID(1)
	if err != nil {
		t.Fatalf("directViewTypedColumnSegmentFileID: %v", err)
	}
	directPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), ColumnAssetRef{
		Namespace: cfg.AssetManager.Namespace,
		FileID:    directFileID,
	})
	if err != nil {
		t.Fatalf("columnAssetSegmentPath direct: %v", err)
	}
	if _, err := os.Stat(directPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("direct path before prepare stat=%v want not exist", err)
	}
	syncErr := errors.New("forced shared segment close sync failure")
	withColumnAssetSegmentFileSyncForTest(t, func(file *os.File) error {
		if file == nil {
			t.Fatalf("syncColumnAssetSegmentFileForPublish called with nil file")
		}
		if file.Name() == sharedPath {
			return syncErr
		}
		return nil
	})
	rows := []columnDeclaredRow{
		{ID: []byte("e1"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 10},
			{Type: ColumnStoreValueInt64, Present: true, Int64: 1},
		}},
		{ID: []byte("e2"), Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: 20},
			{Type: ColumnStoreValueInt64, Present: true, Int64: 2},
		}},
	}
	_, err = col.prepareColumnPhysicalAssetRowsForCommand(ColumnPublishPreparedAssets{}, columnWritePublishInput{
		meta:      CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}},
		operation: ColumnPublishOperationInsert,
		rows:      len(rows),
	}, ColumnPublishAssetPrepareInput{
		Collection:        "events",
		ColumnStore:       *cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 77,
	}, rows)
	if !errors.Is(err, syncErr) {
		t.Fatalf("prepareColumnPhysicalAssetRowsForCommand err=%v want %v", err, syncErr)
	}
	if _, err := os.Stat(sharedPath); err != nil {
		t.Fatalf("shared path after failed prepare stat=%v want preserved existing segment", err)
	}
	if _, err := os.Stat(directPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("direct path after shared close failure stat=%v want never allocated", err)
	}
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("stable pins after prepare failure=%d want baseline %d", got, baselinePins)
	}
	if got := registry.ActiveIdentities(); got != baselineIdentities {
		t.Fatalf("stable identities after prepare failure=%d want baseline %d", got, baselineIdentities)
	}
}

func TestColumnManifestRootDescriptorSystemDeltaRejectsPlanRootMismatchM10B(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col := openColumnStoreCollectionM10B(t, d, mgr)

	planInput := testColumnPublishPlanInputM10A(
		ColumnManifestIdentity{Generation: 1, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234},
		testColumnPublishPreparedAssetM10A(),
	)
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	plan.ManifestRootName = collectionColumnManifestRootName("other")
	plan.RootDelta.RootName = plan.ManifestRootName

	rootName := collectionColumnManifestRootName("events")
	iter, err := col.buildRootDescriptorAndColumnManifestSystemDeltaIteratorForMeta(
		col.Meta(),
		0,
		0,
		[]string{rootName},
		map[string]uint64{rootName: 0},
		[]uint64{1},
		plan,
	)
	if iter != nil {
		_ = iter.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "does not match collection root") {
		t.Fatalf("buildRootDescriptorAndColumnManifestSystemDeltaIteratorForMeta err=%v want root mismatch", err)
	}
}

func TestColumnManifestRootDescriptorSystemDeltaDoesNotReadPublishedRootBeforeCommitM10B(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col := openColumnStoreCollectionM10B(t, d, mgr)

	planInput := testColumnPublishPlanInputM10A(
		ColumnManifestIdentity{Generation: 1, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234},
		testColumnPublishPreparedAssetM10A(),
	)
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}

	rootName := collectionColumnManifestRootName("events")
	iter, err := col.buildRootDescriptorAndColumnManifestSystemDeltaIteratorForMeta(
		col.Meta(),
		0,
		0,
		[]string{rootName},
		map[string]uint64{rootName: 0},
		[]uint64{123456789},
		plan,
	)
	if err != nil {
		t.Fatalf("buildRootDescriptorAndColumnManifestSystemDeltaIteratorForMeta: %v", err)
	}
	if iter == nil {
		t.Fatal("buildRootDescriptorAndColumnManifestSystemDeltaIteratorForMeta returned nil iterator")
	}
	_ = iter.Close()
}

func TestColumnManifestRootDescriptorSystemDeltaReturnsPreparedUpdatedMetaM10B(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col := openColumnStoreCollectionM10B(t, d, mgr)

	identity := ColumnManifestIdentity{Generation: 1, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	planInput := testColumnPublishPlanInputM10A(identity, testColumnPublishPreparedAssetM10A())
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	identity = plan.UpdatedActiveManifest

	rootName := collectionColumnManifestRootName("events")
	iter, updatedMeta, err := col.buildRootDescriptorAndColumnManifestSystemDeltaIteratorAndMetaForMeta(
		col.Meta(),
		0,
		0,
		[]string{rootName},
		map[string]uint64{rootName: 0},
		[]uint64{99},
		plan,
	)
	if err != nil {
		t.Fatalf("buildRootDescriptorAndColumnManifestSystemDeltaIteratorAndMetaForMeta: %v", err)
	}
	defer func() { _ = iter.Close() }()
	if cfg := updatedMeta.Options.ColumnStore; cfg == nil ||
		cfg.ActiveManifest == nil || *cfg.ActiveManifest != identity ||
		cfg.RecoveryAuthoritativeManifest == nil || *cfg.RecoveryAuthoritativeManifest != identity ||
		cfg.RecoveryAuthoritativeAppliedCommandLSN != plan.AppliedCommandLSN {
		t.Fatalf("updated meta column store=%+v want active/recovery identity %+v and recovery LSN %d", cfg, identity, plan.AppliedCommandLSN)
	}

	var encodedMeta []byte
	for ; iter.Valid(); iter.Next() {
		if bytes.Equal(iter.Key(), []byte(systemCollectionMetaKey("events"))) {
			encodedMeta = iter.ValueCopy(nil)
			break
		}
	}
	if len(encodedMeta) == 0 {
		t.Fatal("system delta did not include updated collection metadata")
	}
	decodedMeta, err := decodeCollectionMeta(encodedMeta)
	if err != nil {
		t.Fatalf("decodeCollectionMeta: %v", err)
	}
	if !sameCollectionMeta(decodedMeta, updatedMeta) {
		t.Fatalf("encoded metadata did not match prepared updated metadata: decoded=%+v updated=%+v", decodedMeta, updatedMeta)
	}
}

func TestPrepareColumnWritePublishInputRecordsDocumentExtractionM10B(t *testing.T) {
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString},
		},
	}
	input, err := prepareColumnWritePublishInputBeforeCommandWAL(columnWritePublishInput{
		meta: CollectionMeta{Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    cfg,
		}},
		operation: ColumnPublishOperationInsert,
		documents: []columnWriteDocument{{
			ID:       []byte("doc-1"),
			Document: []byte(`{"row_id":42,"kind":"post"}`),
		}},
		rows: 1,
	})
	if err != nil {
		t.Fatalf("prepare column write input: %v", err)
	}
	if !input.declaredRowsReady || len(input.declaredRows) != 1 {
		t.Fatalf("declared rows not prepared: %+v", input)
	}
	if input.documentExtraction < 0 {
		t.Fatalf("document extraction duration=%s want non-negative", input.documentExtraction)
	}
	if got := input.declaredRows[0].Values[0].Int64; got != 42 {
		t.Fatalf("row_id=%d want 42", got)
	}
	if got := input.declaredRows[0].Values[1].String; got != "post" {
		t.Fatalf("kind=%q want post", got)
	}
}

func TestRecordColumnPublishPlanStatsIncludesDocumentExtractionM10B(t *testing.T) {
	stats := &CollectionInsertStats{}
	documentExtraction := 123 * time.Microsecond
	recordColumnPublishPlanStats(stats, ColumnPublishPlan{
		Enabled: true,
		Rows:    2,
		StageMetrics: ColumnPublishStageMetrics{
			DocumentExtraction: documentExtraction,
			AssetMetrics: ColumnPublishAssetPreparationMetrics{
				SharedAppendCloseCount:                3,
				SharedAppendFileSyncCount:             2,
				SharedAppendSyncEpochCount:            1,
				SharedSegmentAppendBytes:              100,
				SharedSegmentAppendCount:              4,
				SharedSegmentAppendCloseCount:         2,
				SharedSegmentAppendFileSyncCount:      1,
				SharedSegmentAppendSyncEpochCount:     1,
				DirectViewSegmentAppendBytes:          200,
				DirectViewSegmentAppendCount:          5,
				DirectViewSegmentAppendCloseCount:     1,
				DirectViewSegmentAppendFileSyncCount:  1,
				DirectViewSegmentAppendSyncEpochCount: 0,
			},
		},
	})
	if stats.ColumnPublishDocumentExtraction != documentExtraction {
		t.Fatalf("document extraction=%s want %s", stats.ColumnPublishDocumentExtraction, documentExtraction)
	}
	if stats.ColumnPublishDeclaredColumnEncoding != 0 {
		t.Fatalf("declared column encoding=%s want 0", stats.ColumnPublishDeclaredColumnEncoding)
	}
	if stats.ColumnPublishAssetAppenderCloseCount != 3 {
		t.Fatalf("asset appender close count=%d want 3", stats.ColumnPublishAssetAppenderCloseCount)
	}
	if stats.ColumnPublishAssetAppendFileSyncCount != 2 {
		t.Fatalf("asset append file sync count=%d want 2", stats.ColumnPublishAssetAppendFileSyncCount)
	}
	if stats.ColumnPublishAssetSyncEpochCount != 1 {
		t.Fatalf("asset sync epoch count=%d want 1", stats.ColumnPublishAssetSyncEpochCount)
	}
	if stats.ColumnPublishSharedSegmentAppendBytes != 100 || stats.ColumnPublishSharedSegmentAppendCount != 4 {
		t.Fatalf("shared segment append bytes/count=%d/%d want 100/4", stats.ColumnPublishSharedSegmentAppendBytes, stats.ColumnPublishSharedSegmentAppendCount)
	}
	if stats.ColumnPublishSharedSegmentAppenderCloseCount != 2 || stats.ColumnPublishSharedSegmentAppendFileSyncCount != 1 || stats.ColumnPublishSharedSegmentAppendSyncEpochCount != 1 {
		t.Fatalf("shared segment close/file-sync/sync-epoch=%d/%d/%d want 2/1/1",
			stats.ColumnPublishSharedSegmentAppenderCloseCount,
			stats.ColumnPublishSharedSegmentAppendFileSyncCount,
			stats.ColumnPublishSharedSegmentAppendSyncEpochCount)
	}
	if stats.ColumnPublishDirectViewSegmentAppendBytes != 200 || stats.ColumnPublishDirectViewSegmentAppendCount != 5 {
		t.Fatalf("direct-view segment append bytes/count=%d/%d want 200/5", stats.ColumnPublishDirectViewSegmentAppendBytes, stats.ColumnPublishDirectViewSegmentAppendCount)
	}
	if stats.ColumnPublishDirectViewSegmentAppenderCloseCount != 1 || stats.ColumnPublishDirectViewSegmentAppendFileSyncCount != 1 || stats.ColumnPublishDirectViewSegmentAppendSyncEpochCount != 0 {
		t.Fatalf("direct-view segment close/file-sync/sync-epoch=%d/%d/%d want 1/1/0",
			stats.ColumnPublishDirectViewSegmentAppenderCloseCount,
			stats.ColumnPublishDirectViewSegmentAppendFileSyncCount,
			stats.ColumnPublishDirectViewSegmentAppendSyncEpochCount)
	}
}

func prepareColumnStoreCommandWALDirM10B(t testing.TB) (string, uint64) {
	t.Helper()
	return prepareColumnStoreCommandWALDirWithProfileM10C(t, "")
}

func prepareColumnStoreCommandWALDirWithProfileM10C(t testing.TB, profileSupport ColumnStoreProfileSupport) (string, uint64) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	cfg := testColumnStoreConfig(nil)
	if profileSupport != "" {
		cfg.ProfileSupport = profileSupport
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: cfg},
	}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	baseLSN := d.State().AppliedCommandLSN
	if baseLSN == 0 {
		_ = d.Close()
		t.Fatal("setup AppliedCommandLSN=0, want command WAL create LSN")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close setup DB: %v", err)
	}
	return dir, baseLSN
}

func openBufferedBSONColumnStoreSeedM10B(t *testing.T, indexed bool, profile ColumnStoreProfileSupport) (*backenddb.DB, *Collection) {
	t.Helper()
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    t.TempDir(),
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "events",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatBSON,
			BufferedIndexedWrites: indexed,
		},
	}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	doc, err := bson.Marshal(bson.D{
		{Key: "time_us", Value: int64(1)},
		{Key: "kind", Value: "like"},
		{Key: "did", Value: "d1"},
	})
	if err != nil {
		_ = d.Close()
		t.Fatalf("bson.Marshal: %v", err)
	}
	insertedIDs, err := col.InsertBatchValidatedBSON([][]byte{[]byte("e1")}, [][]byte{doc})
	if err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatchValidatedBSON seed: %v", err)
	}
	if len(insertedIDs) != 1 {
		_ = d.Close()
		t.Fatalf("InsertBatchValidatedBSON seed inserted=%d, want 1", len(insertedIDs))
	}
	if err := col.Flush(); err != nil {
		_ = d.Close()
		t.Fatalf("Flush seed: %v", err)
	}
	if indexed {
		if _, err := col.CreateIndex(IndexDefinition{Name: "kind", Field: "kind", ValueType: IndexValueString}); err != nil {
			_ = d.Close()
			t.Fatalf("CreateIndex: %v", err)
		}
	}
	col = enableColumnStoreForExistingCollectionM10B(t, d, "events", profile, mgr)
	return d, col
}

func enableColumnStoreForExistingCollectionM10B(t *testing.T, d *backenddb.DB, collectionName string, profile ColumnStoreProfileSupport, managers ...*CollectionManager) *Collection {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot: nil")
	}
	catalog, err := loadCollectionCatalog(snap, collectionName)
	_ = snap.Close()
	if err != nil {
		t.Fatalf("loadCollectionCatalog: %v", err)
	}
	if catalog == nil {
		t.Fatalf("missing collection catalog for %q", collectionName)
	}
	meta := catalog.meta
	cfg := testColumnStoreConfig(nil)
	if profile != "" {
		cfg.ProfileSupport = profile
	}
	meta.Options.ColumnStore = cfg
	normalized, err := normalizeCollectionMeta(meta)
	if err != nil {
		t.Fatalf("normalizeCollectionMeta: %v", err)
	}
	encoded, err := encodeCollectionMeta(normalized)
	if err != nil {
		t.Fatalf("encodeCollectionMeta: %v", err)
	}
	_, _, err = d.PublishOrderedRootGroupWithSystemBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		current := d.AcquireSnapshot()
		if current == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = current.Close() }()
		return buildSystemTargetIterator(current, map[string][]byte{
			systemCollectionMetaKey(normalized.Name): encoded,
		})
	})
	if err != nil {
		t.Fatalf("publish column-store metadata: %v", err)
	}
	var mgr *CollectionManager
	if len(managers) != 0 && managers[0] != nil {
		mgr = managers[0]
	} else {
		mgr = NewCollectionManager(d)
	}
	col, err := mgr.OpenCollection(collectionName)
	if err != nil {
		t.Fatalf("OpenCollection after column-store enable: %v", err)
	}
	return col
}

func openColumnStoreCollectionM10B(t testing.TB, d *backenddb.DB, managers ...*CollectionManager) *Collection {
	t.Helper()
	var mgr *CollectionManager
	if len(managers) != 0 && managers[0] != nil {
		mgr = managers[0]
	} else {
		mgr = NewCollectionManager(d)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func assertColumnStoreWriteDomainEmptyM10B(t testing.TB, col *Collection) {
	t.Helper()
	if col.writeDomain == nil {
		return
	}
	col.writeDomain.mu.RLock()
	defer col.writeDomain.mu.RUnlock()
	if col.writeDomain.count != 0 || col.writeDomain.rootRunCount != 0 {
		t.Fatalf("write domain staged count=%d rootRunCount=%d, want empty", col.writeDomain.count, col.writeDomain.rootRunCount)
	}
}

func assertColumnStoreCommandWALWriteRejectedM10B(t testing.TB, err error, operation string) {
	t.Helper()
	if !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("%s error=%v, want ErrCommandWALRejected", operation, err)
	}
	if errors.Is(err, backenddb.ErrCommandWALUnsupported) {
		t.Fatalf("%s error=%v must not look like ErrCommandWALUnsupported", operation, err)
	}
	if !strings.Contains(err.Error(), `collection="events"`) {
		t.Fatalf("%s error=%v missing collection context", operation, err)
	}
}

func assertColumnStoreDocumentMissingM10B(t testing.TB, col *Collection, id string) {
	t.Helper()
	got, err := col.Get([]byte(id))
	if err != nil {
		t.Fatalf("Get(%q): %v", id, err)
	}
	if got != nil {
		t.Fatalf("Get(%q)=%q, want missing after rejected column-store write", id, string(got))
	}
}

func assertColumnStorePersistedDocumentMissingM10B(t testing.TB, d *backenddb.DB, collectionName, id string) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot: nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collectionName)
	if err != nil {
		t.Fatalf("loadCollectionCatalog(%q): %v", collectionName, err)
	}
	if catalog == nil {
		t.Fatalf("missing collection catalog for %q", collectionName)
	}
	got, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionPrimaryRootName(collectionName), []byte(id), nil)
	if err != nil {
		t.Fatalf("collectionGetAppendAtCatalogRoot(%q): %v", id, err)
	}
	if found {
		t.Fatalf("persisted Get(%q)=%q, want missing after rejected column-store write", id, string(got))
	}
}

func assertDBAppliedCommandLSNM10B(t testing.TB, d *backenddb.DB, want uint64) {
	t.Helper()
	if got := d.State().AppliedCommandLSN; got != want {
		t.Fatalf("AppliedCommandLSN=%d, want %d", got, want)
	}
}

func assertColumnManifestStateM10B(t testing.TB, col *Collection, generation, appliedLSN uint64, managers ...*CollectionManager) {
	t.Helper()
	var mgr *CollectionManager
	if len(managers) != 0 && managers[0] != nil {
		mgr = managers[0]
	} else {
		mgr = NewCollectionManager(col.db)
	}
	reopened, err := mgr.OpenCollection(col.meta.Name)
	if err != nil {
		t.Fatalf("OpenCollection fresh: %v", err)
	}
	meta := reopened.Meta()
	cfg := meta.Options.ColumnStore
	if cfg == nil || cfg.ActiveManifest == nil {
		t.Fatalf("missing active column manifest metadata: %+v", cfg)
	}
	if cfg.ActiveManifest.Generation != generation {
		t.Fatalf("active generation=%d, want %d", cfg.ActiveManifest.Generation, generation)
	}
	if cfg.ActiveManifest.Format != columnManifestFormatTCS1 || cfg.ActiveManifest.Version != columnManifestIdentityVersion || cfg.ActiveManifest.Checksum == 0 {
		t.Fatalf("invalid active manifest identity: %+v", cfg.ActiveManifest)
	}
	if cfg.RecoveryAuthoritativeManifest == nil || !columnManifestIdentityValueEqual(*cfg.RecoveryAuthoritativeManifest, *cfg.ActiveManifest) {
		t.Fatalf("recovery-authoritative manifest mismatch: %+v active=%+v", cfg.RecoveryAuthoritativeManifest, cfg.ActiveManifest)
	}
	if cfg.RecoveryAuthoritativeAppliedCommandLSN != appliedLSN {
		t.Fatalf("recovery AppliedCommandLSN=%d, want %d", cfg.RecoveryAuthoritativeAppliedCommandLSN, appliedLSN)
	}
	id, ok := reopened.ColumnStoreCacheIdentity()
	if !ok {
		t.Fatalf("ColumnStoreCacheIdentity ok=false")
	}
	if id.ManifestRoot == 0 {
		t.Fatalf("ManifestRoot=0, want non-zero")
	}
	if id.ManifestGeneration != generation || id.RecoveryAuthoritativeGeneration != generation || id.RecoveryAuthoritativeAppliedCommandLSN != appliedLSN {
		t.Fatalf("unexpected cache identity: %+v want generation=%d appliedLSN=%d", id, generation, appliedLSN)
	}
	snap := col.db.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	entry, err := snap.GetEntryAtRoot(id.ManifestRoot, []byte(columnManifestIdentityRecordKey))
	if err != nil {
		t.Fatalf("GetEntryAtRoot manifest identity: %v", err)
	}
	record, err := decodeColumnManifestIdentityRecord(entry.Value)
	if err != nil {
		t.Fatalf("decodeColumnManifestIdentityRecord: %v", err)
	}
	if record.Generation != generation || record.Version != columnManifestIdentityVersion || record.Checksum != cfg.ActiveManifest.Checksum {
		t.Fatalf("manifest root record=%+v active=%+v", record, cfg.ActiveManifest)
	}
}

func columnManifestAssetRefsForCollectionM12A(t testing.TB, d *backenddb.DB, col *Collection) []ColumnAssetRef {
	t.Helper()
	id, ok := col.ColumnStoreCacheIdentity()
	if !ok || id.ManifestRoot == 0 {
		t.Fatalf("ColumnStoreCacheIdentity=%+v ok=%v, want manifest root", id, ok)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	iter, err := snap.IteratorAtRoot(id.ManifestRoot, []byte(columnManifestPartRecordPrefix), nil)
	if err != nil {
		t.Fatalf("IteratorAtRoot manifest root: %v", err)
	}
	defer func() { _ = iter.Close() }()
	refs, err := enumerateColumnManifestAssetRefs(iter)
	if err != nil {
		t.Fatalf("enumerateColumnManifestAssetRefs: %v", err)
	}
	return refs
}

func columnManifestPhysicalAssetRefsForTestM1634(refs []ColumnAssetRef) []ColumnAssetRef {
	out := make([]ColumnAssetRef, 0, len(refs))
	for _, ref := range refs {
		if ref.Kind == ColumnAssetKindTCS1PartImage {
			out = append(out, ref)
		}
	}
	return out
}

func columnManifestAllPartsForCollectionM12C(t testing.TB, d *backenddb.DB, col *Collection) []columnManifestPartSnapshot {
	t.Helper()
	id, ok := col.ColumnStoreCacheIdentity()
	if !ok || id.ManifestRoot == 0 {
		t.Fatalf("ColumnStoreCacheIdentity=%+v ok=%v, want manifest root", id, ok)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	iter, err := snap.IteratorAtRoot(id.ManifestRoot, []byte(columnManifestPartRecordPrefix), nil)
	if err != nil {
		t.Fatalf("IteratorAtRoot manifest root: %v", err)
	}
	defer func() { _ = iter.Close() }()
	parts := make([]columnManifestPartSnapshot, 0, 4)
	iter.Seek([]byte(columnManifestPartRecordPrefix))
	for iter.Valid() {
		key := iter.UnsafeKey()
		if !bytes.HasPrefix(key, []byte(columnManifestPartRecordPrefix)) {
			break
		}
		if iter.IsDeleted() {
			iter.Next()
			continue
		}
		value, _, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			t.Fatalf("manifest part record %q must be inline", key)
		}
		part, err := decodeColumnManifestPartRecord(value)
		if err != nil {
			t.Fatalf("decodeColumnManifestPartRecord(%q): %v", key, err)
		}
		parts = append(parts, part)
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("manifest part iterator: %v", err)
	}
	return parts
}

func columnManifestPartByReasonM12C(t testing.TB, parts []columnManifestPartSnapshot, operation ColumnPublishOperation) columnManifestPartSnapshot {
	t.Helper()
	var matches []columnManifestPartSnapshot
	for _, part := range parts {
		if part.Reason == string(operation) {
			matches = append(matches, part)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("manifest parts with reason %q=%d want 1; parts=%+v", operation, len(matches), parts)
	}
	return matches[0]
}

func installSyntheticNonJSONColumnStoreCatalogM12C(t testing.TB, d *backenddb.DB, col *Collection) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot: nil")
	}
	catalog, err := col.catalogForSnapshot(snap)
	systemRoot := snapshotSystemRoot(snap)
	_ = snap.Close()
	if err != nil {
		t.Fatalf("catalogForSnapshot: %v", err)
	}
	if catalog == nil {
		t.Fatal("catalogForSnapshot: nil catalog")
	}
	meta := catalog.meta
	meta.Options.ColumnStore = testColumnStoreConfig(nil)
	normalized, err := normalizeCollectionMeta(meta)
	if err != nil {
		t.Fatalf("normalizeCollectionMeta: %v", err)
	}
	// This is intentionally a handle-local catalog install: the test exercises
	// the public write preflight for a BSON column-enabled collection with real
	// existing rows, not catalog metadata publication.
	col.meta = normalized
	col.rememberCatalogAtSystemRoot(systemRoot, cloneCatalogWithRootUpdates(catalog, normalized, nil, nil))
}

func assertColumnPhysicalAssetRowsM12C(t testing.TB, d *backenddb.DB, col *Collection, part columnManifestPartSnapshot, operation ColumnPublishOperation, wantIDs []string, wantDeleted []bool) {
	t.Helper()
	if len(wantIDs) != len(wantDeleted) {
		t.Fatalf("bad test expectation ids=%d deleted=%d", len(wantIDs), len(wantDeleted))
	}
	if part.Reason != string(operation) {
		t.Fatalf("part reason=%q want %q", part.Reason, operation)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), part.AssetRef)
	if err != nil {
		t.Fatalf("readColumnPhysicalAssetFromManager: %v", err)
	}
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil {
		t.Fatalf("missing column store config")
	}
	if err := validateColumnPhysicalAssetForManifest(raw, part.AssetRef, *cfg); err != nil {
		t.Fatalf("validateColumnPhysicalAssetForManifest: %v", err)
	}
	asset, err := decodeColumnPhysicalAsset(raw)
	if err != nil {
		t.Fatalf("decodeColumnPhysicalAsset: %v", err)
	}
	if asset.Header.Operation != operation {
		t.Fatalf("asset operation=%q want %q", asset.Header.Operation, operation)
	}
	if len(asset.Rows) != len(wantIDs) {
		t.Fatalf("asset rows=%d want %d", len(asset.Rows), len(wantIDs))
	}
	for i, row := range asset.Rows {
		if gotID := string(row.ID); gotID != wantIDs[i] {
			t.Fatalf("row[%d] id=%q want %q", i, gotID, wantIDs[i])
		}
		if row.Deleted != wantDeleted[i] {
			t.Fatalf("row[%d] deleted=%v want %v", i, row.Deleted, wantDeleted[i])
		}
		if row.Deleted && len(row.Values) != 0 {
			t.Fatalf("row[%d] deleted values=%d want 0", i, len(row.Values))
		}
		if !row.Deleted && len(row.Values) != len(cfg.Columns) {
			t.Fatalf("row[%d] values=%d want %d", i, len(row.Values), len(cfg.Columns))
		}
	}
}

func columnAssetRefsEqualM12B(a, b []ColumnAssetRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func columnManifestSnapshotForCollectionM12A(t testing.TB, d *backenddb.DB, col *Collection) columnManifestSnapshot {
	t.Helper()
	id, ok := col.ColumnStoreCacheIdentity()
	if !ok || id.ManifestRoot == 0 {
		t.Fatalf("ColumnStoreCacheIdentity=%+v ok=%v, want manifest root", id, ok)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	iter, err := snap.IteratorAtRoot(id.ManifestRoot, []byte(columnManifestHeaderRecordKey), nil)
	if err != nil {
		t.Fatalf("IteratorAtRoot manifest root: %v", err)
	}
	defer func() { _ = iter.Close() }()
	records := make([]columnManifestRecord, 0, 4)
	for iter.Valid() {
		key := iter.UnsafeKey()
		if !bytes.Equal(key, []byte(columnManifestHeaderRecordKey)) && !bytes.HasPrefix(key, []byte(columnManifestPartRecordPrefix)) {
			break
		}
		if iter.IsDeleted() {
			iter.Next()
			continue
		}
		value, _, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			t.Fatalf("manifest record %q must be inline", key)
		}
		records = append(records, columnManifestRecord{key: bytes.Clone(key), value: bytes.Clone(value)})
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("manifest iterator: %v", err)
	}
	snapshot, err := decodeColumnManifestRecords(records)
	if err != nil {
		t.Fatalf("decodeColumnManifestRecords: %v", err)
	}
	return snapshot
}
