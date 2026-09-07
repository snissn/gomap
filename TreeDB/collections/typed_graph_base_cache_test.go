package collections

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestTypedGraphCapturedBaseCache(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	col, fixture, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 1024)
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
	if keeper.searcher != nil || keeper.capturedBase == nil {
		t.Fatal("keeper must contain only base resources")
	}
	initial := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	holder := keeper.capturedBase.ref.holder
	checkTypedGraphCapturedBacking(t, keeper.capturedBase)
	tighter := limits
	tighter.AssetBytes--
	if _, err := col.acquireTypedGraphCapturedBaseCache("embedding_graph", tighter); !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("hit ignored limits: %v", err)
	}
	for i := 0; i < 2; i++ {
		owner, err := col.openTypedGraphReadOwner(limits)
		if err != nil {
			t.Fatal(err)
		}
		if owner.overlay.base.reader.sharedPreparedSearch.holder != keeper.capturedBase.ref.holder {
			t.Fatal("different holder")
		}
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got := col.columnVectorGraphSharedPreparedSearchCacheSnapshot(); got.CacheBuilds != initial.CacheBuilds || got.Refs != 1 {
		t.Fatalf("cache=%+v initial=%+v", got, initial)
	}
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"changed"}}, {Name: "user", Strings: columns[2].Strings[:1]}, {Name: "path", Strings: []string{"changed"}}}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	keeper, err = col.acquireTypedGraphCapturedBaseCache("embedding_graph", limits)
	if err != nil {
		t.Fatal(err)
	}
	if keeper.capturedBase.ref.holder != holder {
		t.Fatal("suffix refresh rebuilt base")
	}
	old, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if len(old.overlay.rows) != 1 {
		t.Fatal("lost suffix")
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	keeper, err = col.acquireTypedGraphCapturedBaseCache("embedding_graph", limits)
	if err != nil {
		t.Fatal(err)
	}
	if keeper.capturedBase.ref.holder == holder {
		t.Fatal("cutover reused stale base")
	}
	var buffer VectorIndexSearchBuffer
	if _, _, err := old.overlay.search(columns[0].Float32Vectors[0], 10, 128, 4096, &buffer); err != nil {
		t.Fatalf("old reader after cutover: %v", err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
		t.Fatal(err)
	}
	if got := col.columnVectorGraphSharedPreparedSearchCacheSnapshot(); got.Refs != 0 {
		t.Fatalf("leaked cache: %+v", got)
	}
	a := &col.collectionSchemaCoordinator().typedGraphOwners
	a.Lock()
	defer a.Unlock()
	if a.baseOwners != 0 || a.baseAssetBytes != 0 || a.baseDescriptorBytes != 0 {
		t.Fatalf("leaked accounting: owners=%d assets=%d descriptors=%d", a.baseOwners, a.baseAssetBytes, a.baseDescriptorBytes)
	}
}

func checkTypedGraphCapturedBacking(t testing.TB, r *typedGraphCapturedBaseResources) {
	t.Helper()
	h := r.ref.holder
	v := h.typedVectorSource
	if cap(v.parts) != len(v.parts) {
		t.Fatalf("part capacity=%d length=%d", cap(v.parts), len(v.parts))
	}
	fixed, err := typedGraphCapturedBaseBackingBound(0, 0, 0, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	actual := fixed - 10*int64(reflect.TypeFor[columnHNSWSearchPackSection]().Size())
	actual += int64(cap(v.locations)) * int64(reflect.TypeFor[columnVectorGraphTypedColumnVectorLocation]().Size())
	actual += int64(len(v.parts)) * int64(reflect.TypeFor[columnVectorGraphTypedColumnVectorPart]().Size())
	actual += int64(cap(v.parts)+cap(v.prepared.parts)) * int64(reflect.TypeFor[*columnVectorGraphTypedColumnVectorPart]().Size())
	actual += int64(cap(v.prepared.partIndexByOrdinal)+cap(v.prepared.rowIndexByOrdinal)) * 4
	a := h.adjacencyLayerSources
	actual += int64(cap(a.sources)) * int64(reflect.TypeFor[*columnVectorGraphLayer0AdjacencyDirectSource]().Size())
	actual += int64(len(a.sources)) * int64(reflect.TypeFor[columnVectorGraphLayer0AdjacencyDirectSource]().Size())
	p := h.hnswSearchPack
	actual += int64(cap(p.Sections)) * int64(reflect.TypeFor[columnHNSWSearchPackSection]().Size())
	actual += int64(cap(p.AdjacencyLayers)) * int64(reflect.TypeFor[columnHNSWSearchPackPreparedLayer]().Size())
	if actual > r.backingBytes {
		t.Fatalf("known backing=%d reservation=%d", actual, r.backingBytes)
	}
	t.Logf("keeper assets=%d lease/key descriptors=%d known backing actual=%d reserved=%d mappedresource=%+v", r.assetBytes, r.descriptorBytes, actual, r.backingBytes, h.stats())
}

func TestTypedGraphCapturedBaseCacheCrossManagerAndFailure(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	col, fixture, _, _, _, _ := openTypedGraphQualityFixture(t, 128)
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphOverlapLimits()
	limits.Owners = 2
	first, err := col.acquireTypedGraphCapturedBaseCache("embedding_graph", limits)
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewCollectionManager(col.db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	second, err := other.acquireTypedGraphCapturedBaseCache("embedding_graph", limits)
	if err != nil {
		t.Fatal(err)
	}
	if first.capturedBase.ref.holder == second.capturedBase.ref.holder {
		t.Fatal("unexpected cross-manager holder sharing")
	}
	third, err := NewCollectionManager(col.db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := third.acquireTypedGraphCapturedBaseCache("embedding_graph", limits); !errors.Is(err, errTypedGraphOwnerBudget) {
		t.Fatalf("cross-manager cap: %v", err)
	}
	if third.columnVectorGraphSharedPreparedSearchCacheSnapshot().CacheBuilds != 0 {
		t.Fatal("built before cross-manager reservation")
	}
	if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
		t.Fatal(err)
	}
	if err := other.CloseVectorIndexPreparedSearchCache(); err != nil {
		t.Fatal(err)
	}
	// An empty base is supported by the ordinary coherent owner, but has no
	// shared holder to keep. The internal warm seam fails without retaining a
	// lease/reservation; it must not fabricate a pack or alter the public route.
	empty, emptyFixture, _, _, _, _ := openTypedGraphQualityFixture(t, 0)
	if err := emptyFixture.Close(); err != nil {
		t.Fatal(err)
	}
	if p, err := empty.acquireTypedGraphCapturedBaseCache("embedding_graph", limits); p != nil || !errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
		t.Fatalf("empty p=%v err=%v", p, err)
	}
	a := &empty.collectionSchemaCoordinator().typedGraphOwners
	a.Lock()
	defer a.Unlock()
	if a.baseOwners != 0 || a.baseAssetBytes != 0 || a.baseDescriptorBytes != 0 || a.baseBackingBytes != 0 {
		t.Fatal("failed build leaked accounting")
	}
}

func TestTypedGraphCapturedBaseCacheConcurrentAndBudget(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	col, fixture, _, _, _, _ := openTypedGraphQualityFixture(t, 1024)
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphOverlapLimits()
	tiny := limits
	tiny.AssetBytes = 1
	before := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	if _, err := col.acquireTypedGraphCapturedBaseCache("embedding_graph", tiny); !errors.Is(err, errTypedGraphOwnerBudget) {
		t.Fatalf("tiny: %v", err)
	}
	if after := col.columnVectorGraphSharedPreparedSearchCacheSnapshot(); after.CacheBuilds != before.CacheBuilds {
		t.Fatal("mapped before budget rejection")
	}
	var wg sync.WaitGroup
	const workers = 8
	results := make(chan *collectionVectorIndexPreparedSearch, workers)
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := col.acquireTypedGraphCapturedBaseCache("embedding_graph", limits)
			results <- p
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first *collectionVectorIndexPreparedSearch
	for p := range results {
		if first == nil {
			first = p
		}
		if p != first {
			t.Fatal("singleflight returned different keepers")
		}
	}
	if got := col.columnVectorGraphSharedPreparedSearchCacheSnapshot(); got.CacheBuilds != before.CacheBuilds+1 || got.Refs != 1 {
		t.Fatalf("singleflight: %+v", got)
	}
	if err := col.db.Close(); err != nil {
		t.Fatal(err)
	}
	if first.readyForCurrentSearch() {
		t.Fatal("DB close left keeper ready")
	}
	if _, err := col.acquireTypedGraphCapturedBaseCache("embedding_graph", limits); err == nil {
		t.Fatal("warm after close succeeded")
	}
}
