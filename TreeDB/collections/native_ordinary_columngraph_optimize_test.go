package collections

import (
	"bytes"
	"errors"
	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
	"testing"
)

// No checkpoint is inserted between accepted pending rows and Optimize. The
// unchanged conservative constructor promise may refuse, but eligibility may
// not masquerade as absent native semantic preparation. Refusal has no effect.
func TestNativeOrdinaryColumnGraphOptimizePendingAdmission(t *testing.T) {
	owner, err := treedb.Open(nativeTypedGroupOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	defer manager.Close()
	_, err = manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON, Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: TypedStorageOwnerColumnPart}}}}, VectorIndexes: []VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}}})
	if err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	ids := [][]byte{[]byte("a"), []byte("b")}
	docs := [][]byte{[]byte(`{"embedding":[1,0]}`), []byte(`{"embedding":[0,1]}`)}
	if _, _, err = col.InsertBatchWithStatsValidatedFloat32Projection(ids, docs, "embedding", VectorMetricCosine, [][]float32{{1, 0}, {0, 1}}); err != nil {
		t.Fatal(err)
	}
	held, err := col.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	before := backend.CommandWALNextLSN()
	_, err = col.RebuildVectorIndex("vec")
	if err != nil && (!errors.Is(err, memtable.ErrCOWCapacity) || backenddb.CommitPublicationAccepted(err)) {
		t.Fatalf("Optimize pending rows: %v", err)
	}
	if err != nil && backend.CommandWALNextLSN() != before {
		t.Fatal("declined graph changed WAL")
	}
	for i, id := range ids {
		got, e := held.Get(id)
		if e != nil || !bytes.Equal(got, docs[i]) {
			t.Fatalf("held row %q: %s %v", id, got, e)
		}
		got, e = col.nativeJSON.Get("docs", id)
		if e != nil || !bytes.Equal(got, docs[i]) {
			t.Fatalf("current row %q: %s %v", id, got, e)
		}
	}
	if e := held.Close(); e != nil {
		t.Fatal(e)
	}
	if e := manager.Close(); e != nil {
		t.Fatal(e)
	}
	if e := owner.Close(); e != nil {
		t.Fatal(e)
	}
}

// This finite test budget removes the old coarse graph promise as the cause
// of refusal. It does not change production limits or the public4096 fixture.
func openNativeOrdinaryGraphExecutionFixture(t *testing.T) (*treedb.DB, *backenddb.DB, *CollectionManager, *Collection) {
	t.Helper()
	opts := nativeTypedGroupOptions(t.TempDir())
	opts.COWMemtableLimits = treedb.DefaultCOWMemtableLimits()
	opts.COWMemtableLimits.MaxInFlightBytes = 1 << 30
	opts.COWMemtableLimits.MaxTotalBytes = 2 << 30
	opts.COWMemtableLimits.MaxRetiredBytes = 1 << 30
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if e := owner.Close(); e != nil {
			t.Error(e)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	t.Cleanup(func() {
		if e := manager.Close(); e != nil {
			t.Error(e)
		}
	})
	_, err = manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON, Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: TypedStorageOwnerColumnPart}}}}, VectorIndexes: []VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}}})
	if err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	return owner, backend, manager, col
}

// Before this guard a sufficiently funded empty Optimize could assign its
// recipe and only then discover the absent source manifest in the finalizer.
func TestNativeOrdinaryColumnGraphEmptyOptimizePreaccept(t *testing.T) {
	owner, backend, manager, col := openNativeOrdinaryGraphExecutionFixture(t)
	held, err := col.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { held.Close() })
	budget := held.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	before := budget.Stats()
	lsn := backend.CommandWALNextLSN()
	if _, err = col.RebuildVectorIndex("vec"); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || backenddb.CommitPublicationAccepted(err) {
		t.Fatalf("empty Optimize must refuse before assignment: %v", err)
	}
	after := budget.Stats()
	if after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases || backend.CommandWALNextLSN() != lsn {
		t.Fatalf("empty Optimize added custody or changed WAL: before=%+v after=%+v", before, after)
	}
	current, err := col.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	if current.capture.LogicalSequence() != held.capture.LogicalSequence() || current.collection.incarnation != held.collection.incarnation || current.collection.documentGeneration != held.collection.documentGeneration || !collectionMetaValuesEqual(current.meta, held.meta) {
		t.Fatal("empty refusal changed logical source/schema")
	}
	if current.meta.Options.ColumnStore.ActiveManifest != nil || current.meta.Options.ColumnStore.RecoveryAuthoritativeManifest != nil {
		t.Fatal("empty refusal installed a manifest")
	}
	for _, read := range []*NativeJSONRead{held, current} {
		if _, e := read.Get([]byte("a")); !errors.Is(e, tree.ErrKeyNotFound) {
			t.Fatalf("empty refusal installed row: %v", e)
		}
	}
	if err = current.Close(); err != nil {
		t.Fatal(err)
	}
	// Refusal must leave the same owner usable; no close/checkpoint rescue.
	if _, _, err = col.InsertBatchWithStatsValidatedFloat32Projection([][]byte{[]byte("a")}, [][]byte{[]byte(`{"embedding":[1,0]}`)}, "embedding", VectorMetricCosine, [][]float32{{1, 0}}); err != nil {
		t.Fatalf("insert after refusal: %v", err)
	}
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	if got := budget.Stats(); got.TotalBytes != 0 || got.ExternalLeases != 0 {
		t.Fatalf("full close retained custody: %+v", got)
	}
}

// Real graph construction, not the default-budget refusal branch: pending
// rows precede a successful graph marker, a later row invalidates that graph,
// and a second marker includes the later row. This public sequence does not
// claim that all three publications are one materialization Work.
func TestNativeOrdinaryColumnGraphOptimizeExecutesPendingAndLaterRows(t *testing.T) {
	owner, _, manager, col := openNativeOrdinaryGraphExecutionFixture(t)
	ids := [][]byte{[]byte("a"), []byte("b")}
	docs := [][]byte{[]byte(`{"embedding":[1,0]}`), []byte(`{"embedding":[0,1]}`)}
	if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids, docs, "embedding", VectorMetricCosine, [][]float32{{1, 0}, {0, 1}}); err != nil {
		t.Fatal(err)
	}
	held, err := col.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { held.Close() })
	budget := held.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	requireGraph := func(want int) {
		t.Helper()
		status, e := col.RebuildVectorIndex("vec")
		if e != nil {
			t.Fatalf("funded Optimize: %v", e)
		}
		if !status.Loaded || status.State != VectorIndexStateColumnGraphLoaded || status.Stats.LiveDocs != want || status.RebuildNeeded || status.Stats.RebuildNeeded || status.Stats.BytesDisk <= 0 {
			t.Fatalf("graph did not execute for %d rows: %+v", want, status)
		}
	}
	requireGraph(2)
	if _, _, err = col.InsertBatchWithStatsValidatedFloat32Projection([][]byte{[]byte("c")}, [][]byte{[]byte(`{"embedding":[1,1]}`)}, "embedding", VectorMetricCosine, [][]float32{{1, 1}}); err != nil {
		t.Fatal(err)
	}
	if status, e := col.VectorIndexStatus("vec"); e == nil && !status.RebuildNeeded && !status.Stats.RebuildNeeded && status.Stats.LiveDocs == 2 {
		t.Fatalf("later row left old graph falsely current: %+v", status)
	}
	requireGraph(3)
	for i, id := range ids {
		got, e := held.Get(id)
		if e != nil || !bytes.Equal(got, docs[i]) {
			t.Fatalf("old cut row %q: %s %v", id, got, e)
		}
	}
	if _, e := held.Get([]byte("c")); !errors.Is(e, tree.ErrKeyNotFound) {
		t.Fatalf("old cut saw later row: %v", e)
	}
	if got, e := col.nativeJSON.Get("docs", []byte("c")); e != nil || !bytes.Equal(got, []byte(`{"embedding":[1,1]}`)) {
		t.Fatalf("current later row: %s %v", got, e)
	}
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	if got := budget.Stats(); got.TotalBytes != 0 || got.ExternalLeases != 0 {
		t.Fatalf("full close retained custody: %+v", got)
	}
}
