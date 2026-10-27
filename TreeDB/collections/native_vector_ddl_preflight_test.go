package collections

import (
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Keep real pending typed construction above its original pressure watermark.
// Rejected vector DDL must not service that pressure, even when a valid first
// declaration precedes an unsupported declaration in the same owner request.
func TestNativeVectorDDLPreflightRejectsBeforePressureMaintenance(t *testing.T) {
	if !ordinaryColumnStableAuthorityEnabled() {
		t.Skip("stable relative namespace unavailable")
	}
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	f := openNativePressureDrainFixture(t, t.TempDir(), true, true)
	for _, meta := range []CollectionMeta{
		{Name: "bson_docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}},
		{Name: "rows", Indexes: []IndexDefinition{{Name: "scalar", Field: "marker", ValueType: IndexValueInt64}}},
	} {
		if _, err := f.manager.CreateCollection(&meta); err != nil {
			t.Fatal(err)
		}
	}
	bson, err := f.manager.OpenCollection("bson_docs")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := f.manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	before, _ := f.pressurePrefix(t)
	beforeState, ok := f.backend.StateToken()
	if !ok {
		t.Fatal("missing physical state token")
	}
	beforeNext := f.backend.CommandWALNextLSN()
	beforeStats := f.owner.Stats()
	walPath := beforeStats["treedb.command_wal.writer.path"]
	beforeWAL, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	catalog := []struct {
		col  *Collection
		meta CollectionMeta
	}{{f.col, f.col.Meta()}, {bson, bson.Meta()}, {rows, rows.Meta()}}
	var preparations atomic.Int64
	restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(ColumnPublishPreparedAssets) error {
		preparations.Add(1)
		return nil
	})
	defer restore()
	valid := VectorIndexDefinition{Name: "newvec", Field: "embedding", Dimensions: 2, Strategy: VectorIndexStrategyColumnGraph}
	retired := valid
	retired.Strategy = VectorIndexStrategy("native_runtime")
	invalid := valid
	invalid.Dimensions = 0
	conflict := valid
	conflict.Name = "scalar"
	for _, tc := range []struct {
		name string
		col  *Collection
		def  VectorIndexDefinition
		want error
		text string
	}{
		{"bson", bson, valid, backenddb.ErrNativeSemanticUnsupported, "BSON"},
		{"untyped_json", rows, valid, backenddb.ErrNativeSemanticUnsupported, ""},
		{"retired", rows, retired, backenddb.ErrNativeSemanticUnsupported, "retired"},
		{"column_store_add", f.col, valid, backenddb.ErrNativeSemanticUnsupported, ""},
		{"invalid_dimensions", rows, invalid, nil, "dimensions must be positive"},
		{"scalar_name_conflict", rows, conflict, nil, "index"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			ownerErr := tc.col.WithVectorIndexDDL([]VectorIndexDefinition{tc.def}, func(*CollectionIndexDDL) error {
				called = true
				return nil
			})
			if called {
				t.Fatal("rejected declaration reached publication callback")
			}
			_, createErr := tc.col.CreateVectorIndex(tc.def)
			for _, err := range []error{ownerErr, createErr} {
				if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) || !strings.Contains(err.Error(), tc.text) {
					t.Fatalf("preflight error=%v want=%v containing=%q", err, tc.want, tc.text)
				}
			}
		})
	}
	called := false
	err = rows.WithVectorIndexDDL([]VectorIndexDefinition{valid, retired}, func(*CollectionIndexDDL) error {
		called = true
		return nil
	})
	if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || called {
		t.Fatalf("full candidate preflight: called=%t err=%v", called, err)
	}
	// Public creation keeps its original duplicate rejection; owner admission
	// remains idempotent (the successful pressure control below uses it).
	if _, err := f.col.CreateVectorIndex(f.col.Meta().VectorIndexes[0]); err == nil || !strings.Contains(err.Error(), "duplicate index") {
		t.Fatalf("public duplicate creation err=%v", err)
	}
	if preparations.Load() != 0 {
		t.Fatalf("rejected DDL prepared %d physical assets", preparations.Load())
	}
	after, err := f.backend.ObserveNativeProducer()
	// ObserveNativeProducer does not join the original semantic cleanup worker
	// (cow_native_txn.go). Already queued last-cut cleanup can refund external
	// leases independently of DDL (cow_semantic_cleanup.go), without changing
	// pending construction. Require identical construction and all other
	// observation fields, and permit only reservation refunds.
	pendingBefore, pendingAfter := before, after
	pendingBefore.ReservedBytes, pendingAfter.ReservedBytes = 0, 0
	if err != nil || pendingAfter != pendingBefore || after.ReservedBytes > before.ReservedBytes {
		t.Fatalf("rejected DDL changed pending construction or grew reservations: before=%+v after=%+v err=%v", before, after, err)
	}
	if state, ok := f.backend.StateToken(); !ok || state != beforeState {
		t.Fatalf("rejected DDL published physical state: before=%+v after=%+v", beforeState, state)
	}
	if next := f.backend.CommandWALNextLSN(); next != beforeNext {
		t.Fatalf("rejected DDL appended command: before=%d after=%d", beforeNext, next)
	}
	afterStats := f.owner.Stats()
	for _, key := range []string{
		"treedb.cache.checkpoint.runs",
		"treedb.command_wal.writer.path",
		"treedb.command_wal.durable_wal_lsn",
		"treedb.command_wal.flush.count_total",
		"treedb.command_wal.sync.count_total",
	} {
		if beforeStats[key] == "" || afterStats[key] != beforeStats[key] {
			t.Fatalf("rejected DDL changed %s: %q -> %q", key, beforeStats[key], afterStats[key])
		}
	}
	if info, err := os.Stat(walPath); err != nil || info.Size() != beforeWAL.Size() {
		t.Fatalf("rejected DDL changed active WAL size: before=%d after=%v err=%v", beforeWAL.Size(), info, err)
	}
	for _, entry := range catalog {
		if !collectionMetaValuesEqual(entry.meta, entry.col.Meta()) {
			t.Fatalf("rejected DDL changed catalog %q", entry.meta.Name)
		}
	}
	f.assertRows(t)
	f.assertHeld(t)

	// Prove the same pending construction reaches the real preparation hook
	// for an accepted idempotent owner declaration, with both locks available.
	restore()
	assertNativeVectorDDLPressureLocksAvailable(t, f)
}

func assertNativeVectorDDLPressureLocksAvailable(t *testing.T, f *nativePressureDrainFixture) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	var pauseOnce, releaseOnce sync.Once
	restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(ColumnPublishPreparedAssets) error {
		pauseOnce.Do(func() { close(entered); <-release })
		return nil
	})
	defer restore()
	done := make(chan error, 1)
	definition := f.col.Meta().VectorIndexes[0]
	go func() {
		done <- f.col.WithVectorIndexDDL([]VectorIndexDefinition{definition}, func(owner *CollectionIndexDDL) error {
			_, err := owner.CreateVectorIndex(definition)
			return err
		})
	}()
	joined := false
	defer func() {
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
		t.Fatalf("accepted DDL missed pressure preparation: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("accepted DDL did not reach pressure preparation")
	}
	available := make(chan struct{})
	go func() {
		unlock := f.col.lockVectorIndexMutation()
		f.col.nativeJSON.mu.Lock()
		f.col.nativeJSON.mu.Unlock()
		unlock()
		close(available)
	}()
	select {
	case <-available:
	case <-time.After(5 * time.Second):
		t.Fatal("vector DDL held authority during pressure maintenance")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("accepted DDL did not finish after pressure maintenance")
	}
}
