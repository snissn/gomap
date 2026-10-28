package collections

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// A real public scalar DROP changes the schema after vector DDL's successful
// preflight and pressure drain. The later locked capture must preserve that
// change in both the callback's metadata and its idempotent vector publication.
func TestNativeVectorDDLOwnerRecapturesSchemaAfterPressure(t *testing.T) {
	if !ordinaryColumnStableAuthorityEnabled() {
		t.Skip("stable relative namespace unavailable")
	}
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	f := openNativeVectorDDLRevalidationFixture(t)
	before, last := f.pressurePrefix(t)
	definition := f.col.Meta().VectorIndexes[0]
	var preparations atomic.Int64
	restorePreparation := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(ColumnPublishPreparedAssets) error {
		preparations.Add(1)
		return nil
	})
	defer restorePreparation()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	restore := setNativeVectorIndexDDLAfterPrepareHookForTest(func(col *Collection) {
		if col == f.col {
			close(entered)
			<-release
		}
	})
	defer restore()
	ownerDone := make(chan error, 1)
	var callbackCalled atomic.Bool
	ownerJoined, dropJoined := false, false
	var dropDone chan error
	defer func() {
		releaseOnce.Do(func() { close(release) })
		for _, pending := range []struct {
			name   string
			done   <-chan error
			joined bool
		}{{"vector owner", ownerDone, ownerJoined}, {"scalar DROP", dropDone, dropJoined}} {
			if pending.done == nil || pending.joined {
				continue
			}
			select {
			case err := <-pending.done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(15 * time.Second):
				stacks := make([]byte, 64<<10)
				t.Errorf("%s did not finish after release\n%s", pending.name, stacks[:runtime.Stack(stacks, true)])
			}
		}
	}()
	currentSchema := func(meta CollectionMeta) error {
		if len(meta.Indexes) != 1 || meta.Indexes[0].Name != "keep" || len(meta.VectorIndexes) != 1 || !VectorIndexDefinitionsLogicallyEqual(meta.VectorIndexes[0], definition) {
			return fmt.Errorf("DDL owner retained stale preflight schema: indexes=%+v vectors=%+v", meta.Indexes, meta.VectorIndexes)
		}
		return nil
	}
	go func() {
		ownerDone <- f.col.WithVectorIndexDDL([]VectorIndexDefinition{definition}, func(owner *CollectionIndexDDL) error {
			callbackCalled.Store(true)
			if err := currentSchema(owner.Meta()); err != nil {
				return err
			}
			meta, err := owner.CreateVectorIndex(definition)
			if err != nil {
				return err
			}
			return currentSchema(*meta)
		})
	}()
	select {
	case <-entered:
	case err := <-ownerDone:
		ownerJoined = true
		t.Fatalf("owner missed post-pressure boundary: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("owner did not finish pressure preparation")
	}
	after, err := f.backend.ObserveNativeProducer()
	state, valid := f.backend.StateToken()
	if err != nil || preparations.Load() == 0 || after.Semantic.ConstructionBytes != 0 || after.Semantic.Sources != 0 || after.LogicalSequence != before.LogicalSequence || !valid || state.AppliedCommandLSN != last {
		t.Fatalf("missing real unchanged-command pressure drain: preparations=%d before=%+v after=%+v state=%+v valid=%t err=%v", preparations.Load(), before, after, state, valid, err)
	}
	// Pressure preparation has returned. A second public DDL may prepare and
	// acquire both authorities without joining a worker paused by this test.
	beforeDropLSN := f.backend.CommandWALNextLSN()
	dropDone = make(chan error, 1)
	go func() { _, err := f.col.DropIndex("discard"); dropDone <- err }()
	select {
	case err := <-dropDone:
		dropJoined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("public DROP could not finish while vector DDL awaited revalidation")
	}
	if err := currentSchema(f.col.Meta()); err != nil {
		t.Fatal(err)
	}
	afterDropLSN := f.backend.CommandWALNextLSN()
	if afterDropLSN != beforeDropLSN+1 {
		t.Fatalf("one public DROP command: before=%d after=%d", beforeDropLSN, afterDropLSN)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-ownerDone:
		ownerJoined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("vector DDL did not finish after schema change")
	}
	if !callbackCalled.Load() {
		t.Fatal("validated DDL owner did not reach its callback")
	}
	if next := f.backend.CommandWALNextLSN(); next != afterDropLSN {
		t.Fatalf("idempotent owner appended command: before=%d after=%d", afterDropLSN, next)
	}
	if err := currentSchema(f.col.Meta()); err != nil {
		t.Fatal(err)
	}
	heldMeta, err := f.held.Meta()
	if err != nil || len(heldMeta.Indexes) != 2 {
		t.Fatalf("held opening lost original schema: meta=%+v err=%v", heldMeta, err)
	}
	f.assertRows(t)
	f.assertHeld(t)
}

func openNativeVectorDDLRevalidationFixture(t *testing.T) *nativePressureDrainFixture {
	t.Helper()
	dir := t.TempDir()
	f := new(nativePressureDrainFixture)
	t.Cleanup(func() { f.close(t) })
	opts := nativeTypedGroupOptions(dir)
	opts.MaxWALBytes = 2 << 30 // original size-maintenance capability, no timer race.
	var err error
	f.owner, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	f.backend, err = f.owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	f.manager = NewCollectionManager(f.backend)
	_, err = f.manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{
		DocumentFormat: DocumentFormatJSON,
		ColumnStore: &ColumnStoreConfig{
			Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
			Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: TypedStorageOwnerColumnPart}},
		},
	}, Indexes: []IndexDefinition{
		{Name: "discard", Field: "content", ValueType: IndexValueString},
		{Name: "keep", Field: "content", ValueType: IndexValueString},
	}, VectorIndexes: []VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}}})
	if err != nil {
		t.Fatal(err)
	}
	f.col, err = f.manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	read, err := f.col.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	f.budget = read.capture.NativeAllocator().(interface {
		Stats() memtable.COWStats
		Limits() memtable.COWLimits
	})
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	if limits := f.budget.Limits(); limits != memtable.DefaultCOWLimits() {
		t.Fatalf("fixture changed original finite limits: %+v", limits)
	}
	if err := f.owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	return f
}
