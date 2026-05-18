package collections

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
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
	if !errors.Is(err, backenddb.ErrCommandWALUnsupported) {
		t.Fatalf("InsertBatch error=%v, want ErrCommandWALUnsupported", err)
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
	if _, err := col.Insert([]byte("e1"), doc); !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("Insert error=%v, want ErrCommandWALRejected", err)
	}
	if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("e2")}, [][]byte{doc}); !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("InsertBatchValidatedBSON error=%v, want ErrCommandWALRejected", err)
	}
	if col.writeDomain != nil {
		col.writeDomain.mu.RLock()
		count := col.writeDomain.count
		col.writeDomain.mu.RUnlock()
		if count != 0 {
			t.Fatalf("write domain count=%d, want 0 after rejected column-store writes", count)
		}
	}
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
			d, col := openBufferedBSONColumnStoreSeedM10B(t, true)
			defer func() { _ = d.Close() }()

			err := tc.del(col)
			if !errors.Is(err, backenddb.ErrCommandWALRejected) {
				t.Fatalf("%s error=%v, want ErrCommandWALRejected", tc.name, err)
			}
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
	d, col := openBufferedBSONColumnStoreSeedM10B(t, false)
	defer func() { _ = d.Close() }()

	before, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get before update: %v", err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("e1"), []BSONSetField{{
		Key:   "kind",
		Value: mustBSONRawValue(t, "post"),
	}})
	if !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("UpdateBSONSet error=%v, want ErrCommandWALRejected", err)
	}
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
	d, col := openBufferedBSONColumnStoreSeedM10B(t, false)
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
	if !errors.Is(err, backenddb.ErrCommandWALUnsupported) {
		t.Fatalf("Update error=%v, want ErrCommandWALUnsupported", err)
	}
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
	if !errors.Is(err, backenddb.ErrCommandWALUnsupported) {
		t.Fatalf("UpdateBatch error=%v, want ErrCommandWALUnsupported", err)
	}
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

func TestColumnStoreCommandWALMutationsPublishManifestM10B(t *testing.T) {
	dir := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	col := openColumnStoreCollectionM10B(t, d)
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("create AppliedCommandLSN=%d, want 1", got)
	}

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	assertColumnManifestStateM10B(t, col, 1, 2)
	assertCollectionDocument(t, col, "e1", `{"time_us":1,"kind":"like","did":"d1"}`)

	matched, modified, err := col.Update([]byte("e1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"time_us":3,"kind":"like","did":"d1"}`), true, nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("Update matched=%v modified=%v, want true true", matched, modified)
	}
	assertColumnManifestStateM10B(t, col, 2, 3)
	assertCollectionDocument(t, col, "e1", `{"time_us":3,"kind":"like","did":"d1"}`)

	deleted, err := col.DeleteDocument([]byte("e2"))
	if err != nil {
		t.Fatalf("DeleteDocument: %v", err)
	}
	if !deleted {
		t.Fatalf("DeleteDocument deleted=false, want true")
	}
	assertColumnManifestStateM10B(t, col, 3, 4)

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	assertColumnManifestStateM10B(t, reopened, 3, 4)
	assertCollectionDocument(t, reopened, "e1", `{"time_us":3,"kind":"like","did":"d1"}`)
	if got, err := reopened.Get([]byte("e2")); err != nil || got != nil {
		t.Fatalf("Get deleted document=(%q, %v), want nil, nil", got, err)
	}
}

func TestColumnStoreCommandWALReplayPublishesManifestM10B(t *testing.T) {
	dir := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)

	col := openColumnStoreCollectionM10B(t, d)
	docs := []commitlog.CollectionDocument{{
		ID:       []byte("e1"),
		Document: []byte(`{"time_us":1,"kind":"like","did":"d1"}`),
	}}
	intent, err := col.newCollectionInsertCommandWALIntent(docs, nil)
	if err != nil {
		_ = d.Close()
		t.Fatalf("newCollectionInsertCommandWALIntent: %v", err)
	}
	lsn, err := d.AppendCommandWALIntent(intent, false)
	if err != nil {
		_ = d.Close()
		t.Fatalf("AppendCommandWALIntent: %v", err)
	}
	if lsn != 2 {
		_ = d.Close()
		t.Fatalf("appended LSN=%d, want 2", lsn)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		_ = d.Close()
		t.Fatalf("AppliedCommandLSN before replay=%d, want 1", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close before replay: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	assertCollectionDocument(t, reopened, "e1", `{"time_us":1,"kind":"like","did":"d1"}`)
	assertColumnManifestStateM10B(t, reopened, 1, 2)
	if got := reopen.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN after replay=%d, want 2", got)
	}
}

func TestColumnStoreCommandWALReplayInsertUpdateDeletePublishesEquivalentManifestM10C(t *testing.T) {
	dir := prepareColumnStoreCommandWALDirM10B(t)

	insertPayload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("events", []commitlog.CollectionDocument{
		{ID: []byte("e1"), Document: []byte(`{"time_us":1,"kind":"like","did":"d1"}`)},
		{ID: []byte("e2"), Document: []byte(`{"time_us":2,"kind":"post","did":"d2"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 2, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, insertPayload)

	updatePayload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload("events", []commitlog.CollectionDocument{
		{ID: []byte("e1"), Document: []byte(`{"time_us":3,"kind":"like","did":"d1"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 3, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionUpdateBatchByIDV1, updatePayload)

	deletePayload, err := commitlog.EncodeCollectionDeleteBatchByIDPayload("events", [][]byte{[]byte("e2")})
	if err != nil {
		t.Fatalf("EncodeCollectionDeleteBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 4, commitlog.CommandKindCollectionDeleteBatchByID, commitlog.PayloadFormatCollectionDeleteBatchByIDV1, deletePayload)

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	assertCollectionDocument(t, reopened, "e1", `{"time_us":3,"kind":"like","did":"d1"}`)
	if got, err := reopened.Get([]byte("e2")); err != nil || got != nil {
		t.Fatalf("Get deleted document=(%q, %v), want nil, nil", got, err)
	}
	assertColumnManifestStateM10B(t, reopened, 3, 4)
	if got := reopen.State().AppliedCommandLSN; got != 4 {
		t.Fatalf("AppliedCommandLSN after replay=%d, want 4", got)
	}
}

func TestColumnStoreReadOnlyOpenWithUnappliedCollectionFrameFailsM10C(t *testing.T) {
	dir := prepareColumnStoreCommandWALDirM10B(t)
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("events", []commitlog.CollectionDocument{
		{ID: []byte("e1"), Document: []byte(`{"time_us":1,"kind":"like","did":"d1"}`)},
	})
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 2, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)

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
				t.Fatalf("DeleteDocument missing deleted=true, want false")
			}
			if framesAfter := countCollectionCommandWALFrames(t, tt.opts.Dir); framesAfter != framesBefore {
				t.Fatalf("command WAL frames after rejected no-op delete=%d, want %d", framesAfter, framesBefore)
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
			if framesAfter := countCollectionCommandWALFrames(t, tt.opts.Dir); framesAfter != framesBefore {
				t.Fatalf("command WAL frames after rejected no-op update=%d, want %d", framesAfter, framesBefore)
			}
			if tt.commandWAL {
				if err := d.CheckCommandWALPublishReady(); err != nil {
					t.Fatalf("CheckCommandWALPublishReady after rejected write: %v", err)
				}
			}
		})
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
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","did":"d1"}`)}); err != nil {
		t.Fatalf("InsertBatch durable benchmark-relaxed: %v", err)
	}
	assertCollectionDocument(t, col, "e1", `{"time_us":1,"kind":"like","did":"d1"}`)
	assertColumnManifestStateM10B(t, col, 1, 2)
}

func TestColumnStorePublishRejectsMissingCommandWALIntentM10B(t *testing.T) {
	dir := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	col := openColumnStoreCollectionM10B(t, d)
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
	col := openColumnStoreCollectionM10B(t, d)

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
		nil,
	)
	if iter != nil {
		_ = iter.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "does not match collection root") {
		t.Fatalf("buildRootDescriptorAndColumnManifestSystemDeltaIteratorForMeta err=%v want root mismatch", err)
	}
}

func prepareColumnStoreCommandWALDirM10B(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close setup DB: %v", err)
	}
	return dir
}

func openBufferedBSONColumnStoreSeedM10B(t *testing.T, indexed bool) (*backenddb.DB, *Collection) {
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
	if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("e1")}, [][]byte{doc}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatchValidatedBSON seed: %v", err)
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
	col = enableColumnStoreForExistingCollectionM10B(t, d, "events")
	return d, col
}

func enableColumnStoreForExistingCollectionM10B(t *testing.T, d *backenddb.DB, collectionName string) *Collection {
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
	cfg.ProfileSupport = ColumnStoreProfileBenchmarkRelaxed
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
	col, err := NewCollectionManager(d).OpenCollection(collectionName)
	if err != nil {
		t.Fatalf("OpenCollection after column-store enable: %v", err)
	}
	return col
}

func openColumnStoreCollectionM10B(t *testing.T, d *backenddb.DB) *Collection {
	t.Helper()
	col, err := NewCollectionManager(d).OpenCollection("events")
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

func assertColumnManifestStateM10B(t testing.TB, col *Collection, generation, appliedLSN uint64) {
	t.Helper()
	reopened, err := NewCollectionManager(col.db).OpenCollection(col.meta.Name)
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
