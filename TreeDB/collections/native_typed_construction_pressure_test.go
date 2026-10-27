package collections

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

var _ backenddb.NativeSemanticProducerPressureState = (*nativeEngineVersion)(nil)

func TestNativeTypedConstructionPressureMatchesBuildReservation(t *testing.T) {
	database, handle, manager, col := openNativeTypedGroup(t, t.TempDir(), true)
	defer func() { _ = manager.Close(); _ = database.Close() }()
	// After the physical checkpoint there is no pending typed constructor;
	// even a nil basis must take the ordinary zero-inventory fast path.
	capture, err := handle.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	e := capture.State().(*nativeEngineVersion)
	pressure, err := e.NativeProducerPressure(nil)
	if err != nil || pressure != (backenddb.NativeSemanticProducerPressure{}) {
		t.Fatalf("no pending typed construction=%+v: %v", pressure, err)
	}
	if err = capture.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = col.InsertTypedBatchWithStats(nativeTypedGroupBatch("pending-pressure")); err != nil {
		t.Fatal(err)
	}
	// Two public collections share one sequential operation workspace, while
	// retaining both original member/output cohorts in the complete inventory.
	otherMeta := CollectionMeta{Name: "second-pressure", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
		Enabled: true, Columns: col.Meta().Options.ColumnStore.Columns,
		RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
	}}}
	if _, err = manager.CreateCollection(&otherMeta); err != nil {
		t.Fatal(err)
	}
	other, err := manager.OpenCollection(otherMeta.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = other.InsertTypedBatchWithStats(nativeTypedGroupBatch("second-pending")); err != nil {
		t.Fatal(err)
	}
	// Keep an actual pending typed catalog image in the same current state, so
	// a missing projection charge cannot pass merely because the term is zero.
	if _, err = manager.CreateCollection(&CollectionMeta{Name: "catalog-pressure", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
		Enabled: true, Columns: []ColumnStoreColumn{{Name: "label", Path: "label", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerRowAsset}},
		RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
	}}}); err != nil {
		t.Fatal(err)
	}
	capture, err = handle.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Close()
	e = capture.State().(*nativeEngineVersion)
	basis := capture.Snapshot()
	// Independently assemble the actual ordinary PrepareBuild reservation
	// formula, including its mandatory catalog projection term. This guards
	// against an advisory/Ensure refactor silently dropping a builder owner.
	promise, _, domains, err := nativeHandoffCapacity(e)
	if err != nil {
		t.Fatal(err)
	}
	build, _, err := nativeEngineBuildCapacity(domains, e.allocator.Limits().MaxSources, promise)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := nativeEngineRootDescriptorKeyCharge(e)
	if err != nil {
		t.Fatal(err)
	}
	typed, _, _, sources, err := nativeTypedBuildCapacity(e, basis)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := nativeCatalogProjectionCapacity(e)
	if err != nil {
		t.Fatal(err)
	}
	expected := build
	for _, n := range []uint64{keys, typed, catalog} {
		if err = nativeTypedCheckedAdd(&expected, n); err != nil {
			t.Fatal(err)
		}
	}
	inventory, err := nativeTypedConstructionCapacity(e, basis)
	if err != nil {
		t.Fatal(err)
	}
	if typed == 0 || sources == 0 || catalog == 0 || inventory.total != expected || inventory.catalog != catalog {
		t.Fatalf("incomplete construction inventory=%+v expected=%d catalog=%d", inventory, expected, catalog)
	}
	pressure, err = e.NativeProducerPressure(basis)
	if err != nil {
		t.Fatal(err)
	}
	var workspace, sumWorkspace uint64
	participating := 0
	scanNativeBuildCollections(e, 0, func(_ uint64, c *nativeCollectionState) bool {
		primary, ok := c.domains.Get("primary")
		if !ok || primary.typed == nil {
			return true
		}
		local, capacityErr := nativeTypedOperationWorkspaceCapacity(*c.schema.meta, false, nativeTypedGraphOnly(primary.typed))
		if capacityErr != nil {
			t.Fatal(capacityErr)
		}
		workspace = max(workspace, local)
		sumWorkspace += local
		participating++
		return true
	})
	if participating != 2 || workspace == 0 || sumWorkspace <= workspace || inventory.workspace != workspace {
		t.Fatalf("workspace must be a maximum, not sum: participants=%d max=%d sum=%d inventory=%+v", participating, workspace, sumWorkspace, inventory)
	}
	limits := e.maintenance.Limits()
	want := backenddb.NativeSemanticProducerPressure{ConstructionBytes: expected, FixedConstructionBytes: workspace, MaxInFlightBytes: limits.MaxInFlightBytes, MaxTotalBytes: limits.MaxTotalBytes, Sources: sources, MaxSources: e.allocator.Limits().MaxSources}
	if pressure != want {
		t.Fatalf("pressure=%+v want=%+v", pressure, want)
	}
	err = nativeTypedEnsureConstructible(e, basis)
	if expected > limits.MaxInFlightBytes || expected > limits.MaxTotalBytes {
		if !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("lost exact capacity refusal: %v", err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	if _, err = e.NativeProducerPressure(nil); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("pending typed state accepted an unrelated/missing basis: %v", err)
	}
}

// A graph-only typed member never opens the row/TCIM codec. It must keep the
// original zero workspace, so advisory scheduling retains its original policy.
func TestNativeTypedGraphOnlyConstructionWorkspaceIsZero(t *testing.T) {
	workspace, err := nativeTypedOperationWorkspaceCapacity(CollectionMeta{}, false, true)
	if err != nil || workspace != 0 {
		t.Fatalf("graph-only workspace=%d: %v", workspace, err)
	}
	if _, err := nativeTypedOperationWorkspaceCapacity(CollectionMeta{}, true, true); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("unsupported graph-only compaction accepted: %v", err)
	}
}
