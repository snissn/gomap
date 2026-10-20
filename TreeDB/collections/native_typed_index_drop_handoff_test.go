package collections

import (
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

// Pause the actual off-P typed asset preparation, remove an index from the
// newer logical cut, and let the original captured build hand off. Physical
// metadata must advance without restoring the captured index declarations.
func TestNativeTypedIndexDropDuringCapturedBuild(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(handle)
	defer func() {
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
		Enabled: true, Columns: []ColumnStoreColumn{{Name: "label", Path: "label", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerRowAsset}},
		RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
	}}, Indexes: []IndexDefinition{{Name: "discard", Field: "label", ValueType: IndexValueString}, {Name: "keep", Field: "label", ValueType: IndexValueString, Unique: true}}}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	insert := func(id, label string) {
		t.Helper()
		if ids, _, err := collection.InsertTypedBatchWithStats([][]byte{[]byte(id)}, [][]byte{[]byte(`{"id":"` + id + `"}`)}, []TypedColumnBatch{{Name: "label", Strings: []string{label}}}); err != nil || len(ids) != 1 {
			t.Fatalf("insert %s: ids=%q err=%v", id, ids, err)
		}
	}
	insert("seed", "alpha")
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if token, ok := handle.StateToken(); !ok || token.AppliedCommandLSN != 2 || handle.CommandWALNextLSN() != 3 {
		t.Fatalf("seed physical basis=%+v ok=%v next=%d", token, ok, handle.CommandWALNextLSN())
	}
	held, err := collection.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	insert("pending", "beta")
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(ColumnPublishPreparedAssets) error {
		once.Do(func() { close(entered); <-release })
		return nil
	})
	defer restore()
	checkpointDone := make(chan error, 1)
	checkpointFinished := false
	go func() { checkpointDone <- database.Checkpoint() }()
	defer func() {
		unblock()
		if !checkpointFinished {
			select {
			case err := <-checkpointDone:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(15 * time.Second):
				t.Error("captured checkpoint did not finish after release")
			}
		}
	}()
	select {
	case <-entered:
	case err := <-checkpointDone:
		checkpointFinished = true
		t.Fatalf("checkpoint did not reach actual typed asset preparation: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("checkpoint did not reach actual typed asset preparation")
	}
	dropDone := make(chan error, 1)
	dropFinished := false
	defer func() {
		unblock()
		if !dropFinished {
			select {
			case err := <-dropDone:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(15 * time.Second):
				t.Error("logical DROP did not finish after release")
			}
		}
	}()
	go func() { _, err := collection.DropIndex("discard"); dropDone <- err }()
	select {
	case err := <-dropDone:
		dropFinished = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("logical DROP blocked on off-P asset preparation")
	}
	if handle.CommandWALNextLSN() != 5 {
		t.Fatalf("typed row and concurrent DROP did not append exactly two frames: next=%d", handle.CommandWALNextLSN())
	}
	checkCurrent := func() {
		t.Helper()
		meta := collection.Meta()
		if len(meta.Indexes) != 1 || meta.Indexes[0].Name != "keep" {
			t.Fatalf("captured build restored removed declarations: %+v", meta.Indexes)
		}
		if ids, err := collection.FindByIndex("discard", "alpha"); err != nil || len(ids) != 0 {
			t.Fatalf("removed index=%q err=%v", ids, err)
		}
		for _, row := range []struct{ id, label string }{{"seed", "alpha"}, {"pending", "beta"}} {
			got, err := collection.Get([]byte(row.id))
			var doc struct {
				ID    string `json:"id"`
				Label string `json:"label"`
			}
			if err != nil || json.Unmarshal(got, &doc) != nil || doc.ID != row.id || doc.Label != row.label {
				t.Fatalf("current typed document %s=%q err=%v", row.id, got, err)
			}
			if ids, err := collection.FindByIndex("keep", row.label); err != nil || len(ids) != 1 || string(ids[0]) != row.id {
				t.Fatalf("surviving index %s=%q err=%v", row.id, ids, err)
			}
		}
	}
	checkCurrent()
	unblock()
	select {
	case err := <-checkpointDone:
		checkpointFinished = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("captured checkpoint did not finish")
	}
	restore()
	checkCurrent()
	if cfg := collection.Meta().Options.ColumnStore; cfg == nil || cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeAppliedCommandLSN < 3 {
		t.Fatal("typed physical manifest did not advance through captured pending row")
	}
	var oldIDs []string
	if err := held.VisitIndexValueIDs("discard", "alpha", func(id []byte) error { oldIDs = append(oldIDs, string(id)); return nil }); err != nil || !reflect.DeepEqual(oldIDs, []string{"seed"}) {
		t.Fatalf("held original index=%q err=%v", oldIDs, err)
	}
	oldRows, err := held.FetchDocumentsByID([][]byte{[]byte("seed"), []byte("pending")}, DocumentFetchOptions{})
	if err != nil || len(oldRows.Results) != 2 || !oldRows.Results[0].Found || oldRows.Results[1].Found {
		t.Fatalf("held original documents=%+v err=%v", oldRows.Results, err)
	}
	var oldDocument struct {
		ID    string `json:"id"`
		Label string `json:"label"`
	}
	if json.Unmarshal(oldRows.Results[0].Document, &oldDocument) != nil || oldDocument.ID != "seed" || oldDocument.Label != "alpha" {
		t.Fatalf("held original document changed: %q", oldRows.Results[0].Document)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkCurrent()
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	if stats := handle.ValueLogIdentityPinRegistry().Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("final original resources=%+v", stats)
	}
	database, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	handle, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = NewCollectionManager(handle)
	collection, err = manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	checkCurrent()
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	if stats := handle.ValueLogIdentityPinRegistry().Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("reopened final resources=%+v", stats)
	}
}
