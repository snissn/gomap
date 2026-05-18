package collections

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
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
			if tt.commandWAL {
				if err := d.CheckCommandWALPublishReady(); err != nil {
					t.Fatalf("CheckCommandWALPublishReady after rejected write: %v", err)
				}
			}
		})
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

func openColumnStoreCollectionM10B(t *testing.T, d *backenddb.DB) *Collection {
	t.Helper()
	col, err := NewCollectionManager(d).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col
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
