package collections

import (
	"errors"
	"strconv"
	"testing"
)

func TestTypedGraphRetiredOwnerCharge(t *testing.T) {
	for _, rows := range []int{128, 1024} {
		t.Run(strconv.Itoa(rows), func(t *testing.T) { testTypedGraphRetiredOwnerCharge(t, rows) })
	}
}

func testTypedGraphRetiredOwnerCharge(t *testing.T, rows int) {
	col, fixture, _, _, _, _ := openTypedGraphQualityFixture(t, rows)
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphOverlapLimits()
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 512, Tombstones: 512, ValueSlots: 2048, OwnedBytes: 8 << 20}, limits.Cold); err != nil {
		t.Fatal(err)
	}
	keeper, err := col.acquireTypedGraphCapturedBaseCache("embedding_graph", limits)
	if err != nil {
		t.Fatal(err)
	}
	holder := keeper.capturedBase.ref.holder
	owner, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if owner.overlay.base.reader.sharedPreparedSearch.holder != holder {
		t.Fatal("holder not shared")
	}
	if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
		t.Fatal(err)
	}
	a := &col.collectionSchemaCoordinator().typedGraphOwners
	a.Lock()
	retained := a.stateBytes
	a.Unlock()
	if retained <= owner.stateBytes {
		t.Fatalf("keeper closed with live query: retained=%d suffix=%d; holder uncharged", retained, owner.stateBytes)
	}
	t.Logf("retained=%d suffix=%d owner_descriptors=%d conservative_holder_backing=%d", retained, owner.stateBytes, owner.descriptorBytes, owner.backingBytes)
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	limits.StateBytes = retained
	first, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	before := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	other, err := NewCollectionManager(col.db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []*Collection{col, other} {
		second, err := c.openTypedGraphReadOwner(limits)
		if second != nil {
			second.Close()
		}
		if !errors.Is(err, errTypedGraphOwnerBudget) {
			t.Fatalf("second same-state owner: %v", err)
		}
		if _, err := c.acquireTypedGraphCapturedBaseCache("embedding_graph", limits); !errors.Is(err, errTypedGraphOwnerBudget) {
			t.Fatalf("keeper ignored retired owner: %v", err)
		}
	}
	after := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	if before.CacheBuilds != after.CacheBuilds || before.Refs != after.Refs {
		t.Fatalf("failed admission changed mappings before=%+v after=%+v", before, after)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	a.Lock()
	defer a.Unlock()
	if a.stateBytes != 0 || a.assetBytes != 0 || a.owners != 0 || a.baseOwners != 0 {
		t.Fatalf("leaked accounting %+v", a)
	}
}
