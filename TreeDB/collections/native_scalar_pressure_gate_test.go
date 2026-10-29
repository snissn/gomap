package collections

import (
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Pause real typed-asset preparation inside a producer-pressure checkpoint.
// Scalar DDL must reach that maintenance before taking its schema mutation gate.
func TestNativeScalarDDLPressurePrecedesMutationGate(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	owner, err := treedb.Open(nativeTypedGroupOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}()
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	defer func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "rows"}); err != nil {
		t.Fatal(err)
	}
	rows, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rows.Insert([]byte("first"), []byte(`{"marker":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "build_pause", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
		Enabled: true, Columns: []ColumnStoreColumn{{Name: "label", Path: "label", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerRowAsset}},
		RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
	}}}); err != nil {
		t.Fatal(err)
	}
	pause, err := manager.OpenCollection("build_pause")
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := pause.InsertTypedBatchWithStats([][]byte{[]byte("pending")}, [][]byte{[]byte(`{"id":"pending"}`)}, []TypedColumnBatch{{Name: "label", Strings: []string{"pending"}}}); err != nil {
		t.Fatal(err)
	}
	read, err := rows.nativeJSON.Capture("rows")
	if err != nil {
		t.Fatal(err)
	}
	budget := read.capture.NativeAllocator().(*memtable.COWBudget)
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	stats, limits := budget.Stats(), budget.Limits()
	if limits != memtable.DefaultCOWLimits() {
		t.Fatalf("fixture changed finite limits: %+v", limits)
	}
	target := limits.MaxInFlightBytes - limits.MaxInFlightBytes/8
	if stats.ReservedBytes >= target {
		t.Fatal("fixture began above pressure watermark")
	}
	filler, err := budget.AcquireExternal(target - stats.ReservedBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer filler.Close()
	observation, err := backend.ObserveNativeProducer()
	if err != nil || !observation.UnderPressure {
		t.Fatalf("producer pressure: %+v %v", observation, err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var pauseOnce, releaseOnce sync.Once
	restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(ColumnPublishPreparedAssets) error {
		pauseOnce.Do(func() { close(entered); <-release })
		return nil
	})
	defer restore()
	go func() {
		_, err := rows.CreateIndex(IndexDefinition{Name: "marker", Field: "marker", ValueType: IndexValueInt64})
		done <- err
	}()
	joined := false
	defer func() {
		filler.Close()
		releaseOnce.Do(func() { close(release) })
		if !joined {
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	}()
	select {
	case <-entered:
	case err := <-done:
		joined = true
		t.Fatalf("DDL missed pressure preparation: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("DDL did not reach pressure preparation")
	}
	available := make(chan struct{})
	go func() {
		unlock := rows.lockVectorIndexMutation()
		unlock()
		close(available)
	}()
	select {
	case <-available:
	case <-time.After(5 * time.Second):
		t.Fatal("scalar DDL held mutation gate during pressure maintenance")
	}
	filler.Close()
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("scalar DDL did not finish after pressure maintenance")
	}
	ids, err := rows.FindByIndexValue("marker", int64(1))
	if err != nil || len(ids) != 1 || string(ids[0]) != "first" {
		t.Fatalf("scalar DDL lost committed row: %q %v", ids, err)
	}
}
