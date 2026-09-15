package collections

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func acquireTypedGraphServingCallerPinForTest(t testing.TB, col *Collection, state *typedGraphPublicationState, owner string) (columnVectorGraphSharedPreparedSearchKey, *ColumnAssetLifecyclePinSet) {
	t.Helper()
	if state == nil || state.servingBase == nil {
		t.Fatal("missing serving base")
	}
	key, err := state.servingBase.servingPreparedSearchKey(col)
	if err != nil {
		t.Fatal(err)
	}
	snap, current, pin, err := col.acquireColumnVectorGraphServingPinnedSnapshot(context.Background(), key, state.servingRefs, owner, false)
	if err != nil {
		t.Fatal(err)
	}
	if current != state.servingBase {
		_ = snap.Close()
		_ = pin.Close()
		t.Fatal("caller pin did not bind the installed base")
	}
	if err := snap.Close(); err != nil {
		_ = pin.Close()
		t.Fatal(err)
	}
	return key, pin
}

func requireTypedGraphServingFoundationReleased(t testing.TB, col *Collection) {
	t.Helper()
	cache := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	if cache.Entries != 0 || cache.Refs != 0 || cache.BuildingEntries != 0 {
		t.Fatalf("serving holder cache retained state: %+v", cache)
	}
	if pins := col.columnAssetLifecyclePinSetSnapshot(); len(pins) != 0 {
		t.Fatalf("serving holder retained %d lifecycle pins: %+v", len(pins), pins)
	}
	requireColumnServingLedgerEmpty(t, &col.collectionSchemaCoordinator().typedGraphPhysical)
}

func TestTypedGraphServingHolderCapabilityPairsCallerPinsThroughCancellation(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	col, fixture, _, _, _, _ := openTypedGraphQualityFixture(t, 128)
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphOverlapLimits()
	installTypedGraphServingStateForInternalTest(t, col, "embedding_graph", limits)
	state := col.collectionSchemaCoordinator().typedPublication.Load()
	key, firstPin := acquireTypedGraphServingCallerPinForTest(t, col, state, "serving-holder-cancel-first")
	_, secondPin := acquireTypedGraphServingCallerPinForTest(t, col, state, "serving-holder-cancel-waiter")

	entered, release := make(chan struct{}), make(chan struct{})
	var builds atomic.Int64
	build := func(snap *backenddb.Snapshot, current *typedGraphServingBaseMetadata, pool *columnServingSegmentLeaseSet) (*columnVectorGraphSharedPreparedSearch, error) {
		builds.Add(1)
		close(entered)
		<-release
		return col.buildColumnVectorGraphServingPreparedSearchFromBase(snap, current, pool)
	}
	type result struct {
		capability *typedGraphServingHolderCapability
		err        error
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstDone := make(chan result, 1)
	go func() {
		capability, err := state.servingBase.acquireServingHolderWithOwnedPinAndBuild(firstCtx, col, firstPin, limits.Physical, build)
		firstDone <- result{capability: capability, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first holder builder did not start")
	}

	secondDone := make(chan result, 1)
	go func() {
		capability, err := state.servingBase.acquireServingHolderWithOwnedPinAndBuild(context.Background(), col, secondPin, limits.Physical, build)
		secondDone <- result{capability: capability, err: err}
	}()
	waitForColumnServingCondition(t, func() bool {
		col.vectorPreparedSearchMu.Lock()
		defer col.vectorPreparedSearchMu.Unlock()
		entry := col.vectorPreparedSearch[key]
		return entry != nil && entry.building && entry.waiters == 2
	})
	if pins := col.columnAssetLifecyclePinSetSnapshot(); len(pins) != 3 {
		t.Fatalf("builder guardian plus two callers pins=%d want=3", len(pins))
	}
	cancelFirst()
	select {
	case got := <-firstDone:
		if got.capability != nil {
			_ = got.capability.Close()
		}
		t.Fatalf("synchronous first builder returned before construction completed: %v", got.err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)

	first := <-firstDone
	if first.capability != nil || !errors.Is(first.err, context.Canceled) {
		if first.capability != nil {
			_ = first.capability.Close()
		}
		t.Fatalf("canceled first acquisition capability=%v err=%v", first.capability, first.err)
	}
	second := <-secondDone
	if second.err != nil || second.capability == nil {
		t.Fatalf("independent waiter capability=%v err=%v", second.capability, second.err)
	}
	if builds.Load() != 1 {
		t.Fatalf("serving holder builds=%d want=1", builds.Load())
	}
	if pins := col.columnAssetLifecyclePinSetSnapshot(); len(pins) != 2 {
		t.Fatalf("completed holder guardian plus successful caller pins=%d want=2", len(pins))
	}
	if err := second.capability.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.capability.Close(); err != nil {
		t.Fatalf("idempotent capability close: %v", err)
	}
	requireTypedGraphServingFoundationReleased(t, col)
}

func TestTypedGraphServingHolderRebindsEquivalentCurrentMetadata(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	col, fixture, _, _, _, _ := openTypedGraphQualityFixture(t, 128)
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphOverlapLimits()
	installTypedGraphServingStateForInternalTest(t, col, "embedding_graph", limits)
	coord := col.collectionSchemaCoordinator()
	oldState := coord.typedPublication.Load()
	_, pin := acquireTypedGraphServingCallerPinForTest(t, col, oldState, "serving-holder-rebind-caller")
	oldBase := oldState.servingBase
	reboundBase := *oldBase
	reboundState := *oldState
	reboundState.servingBase = &reboundBase
	coord.typedPublication.Store(&reboundState)

	var observed atomic.Bool
	capability, err := oldBase.acquireServingHolderWithOwnedPinAndBuild(context.Background(), col, pin, limits.Physical, func(snap *backenddb.Snapshot, current *typedGraphServingBaseMetadata, pool *columnServingSegmentLeaseSet) (*columnVectorGraphSharedPreparedSearch, error) {
		if current != &reboundBase || current == oldBase {
			return nil, errors.New("builder did not rebind equivalent current metadata")
		}
		observed.Store(true)
		return col.buildColumnVectorGraphServingPreparedSearchFromBase(snap, current, pool)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !observed.Load() {
		t.Fatal("holder build did not observe current metadata")
	}
	if err := capability.Close(); err != nil {
		t.Fatal(err)
	}
	requireTypedGraphServingFoundationReleased(t, col)
}

func TestTypedGraphServingHolderSiblingCollectionsSharePhysicalLedger(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	col, fixture, _, _, _, _ := openTypedGraphQualityFixture(t, 128)
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphOverlapLimits()
	installTypedGraphServingStateForInternalTest(t, col, "embedding_graph", limits)
	manager := NewCollectionManager(col.db)
	sibling, err := manager.OpenCollection(col.Name())
	if err != nil {
		t.Fatal(err)
	}
	coord := col.collectionSchemaCoordinator()
	if sibling.collectionSchemaCoordinator() != coord {
		t.Fatal("sibling Collection did not resolve the shared DB/collection coordinator")
	}
	state := coord.typedPublication.Load()
	_, firstPin := acquireTypedGraphServingCallerPinForTest(t, col, state, "serving-holder-primary-handle")
	first, err := state.servingBase.acquireServingHolderWithOwnedPin(context.Background(), col, firstPin, limits.Physical)
	if err != nil {
		t.Fatal(err)
	}
	one := coord.typedGraphPhysical.snapshot()
	if one.inventoryHolders != 1 || one.potentialSegments <= 0 || one.potentialDescriptors <= 0 || one.potentialMappedBytes <= 0 || one.potentialFallbackBytes <= 0 || one.inventoryBytes <= 0 {
		_ = first.Close()
		t.Fatalf("first handle physical reservation=%+v", one)
	}

	_, secondPin := acquireTypedGraphServingCallerPinForTest(t, sibling, state, "serving-holder-sibling-handle")
	second, err := state.servingBase.acquireServingHolderWithOwnedPin(context.Background(), sibling, secondPin, limits.Physical)
	if err != nil {
		_ = first.Close()
		t.Fatal(err)
	}
	two := coord.typedGraphPhysical.snapshot()
	if two.inventoryHolders != 2 || two.inventoryRefs != 2*one.inventoryRefs || two.inventorySegments != 2*one.inventorySegments || two.inventoryBytes != 2*one.inventoryBytes || two.potentialSegments != 2*one.potentialSegments || two.potentialDescriptors != 2*one.potentialDescriptors || two.potentialMappedBytes != 2*one.potentialMappedBytes || two.potentialFallbackBytes != 2*one.potentialFallbackBytes {
		_ = first.Close()
		_ = second.Close()
		t.Fatalf("sibling reservations are not additive: first=%+v both=%+v", one, two)
	}
	if err := first.Close(); err != nil {
		_ = second.Close()
		t.Fatal(err)
	}
	afterFirst := coord.typedGraphPhysical.snapshot()
	if afterFirst.inventoryHolders != one.inventoryHolders || afterFirst.inventoryRefs != one.inventoryRefs || afterFirst.inventorySegments != one.inventorySegments || afterFirst.inventoryBytes != one.inventoryBytes || afterFirst.potentialSegments != one.potentialSegments || afterFirst.potentialDescriptors != one.potentialDescriptors || afterFirst.potentialMappedBytes != one.potentialMappedBytes || afterFirst.potentialFallbackBytes != one.potentialFallbackBytes {
		_ = second.Close()
		t.Fatalf("first handle close did not leave exactly the sibling reservation: first=%+v remaining=%+v", one, afterFirst)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	requireTypedGraphServingFoundationReleased(t, col)
	if cache := sibling.columnVectorGraphSharedPreparedSearchCacheSnapshot(); cache.Entries != 0 || cache.Refs != 0 || cache.BuildingEntries != 0 {
		t.Fatalf("sibling holder cache retained state: %+v", cache)
	}
}

func TestTypedGraphReadOwnerRetriesServingHolderMissAcrossFold(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	col, fixture, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphOverlapLimits()
	installTypedGraphServingStateForInternalTest(t, col, "embedding_graph", limits)
	changed := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]},
		{Name: "content", Strings: []string{"fold-miss-retry"}},
		{Name: "user", Strings: []string{"fold-miss-retry"}},
		{Name: "path", Strings: []string{"fold-miss-retry"}},
	}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	oldState := col.collectionSchemaCoordinator().typedPublication.Load()
	oldKey, err := oldState.servingBase.servingPreparedSearchKey(col)
	if err != nil {
		t.Fatal(err)
	}
	baselineCache := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	if baselineCache.Entries != 0 {
		t.Fatalf("fixture unexpectedly warmed old holder: %+v", baselineCache)
	}

	var once sync.Once
	var foldErr error
	typedGraphOwnerAfterSnapshotHook.Lock()
	typedGraphOwnerAfterSnapshotHook.beforeServingOpen = func(c *Collection) {
		if c == col {
			once.Do(func() { foldErr = c.FoldColumnGraphServing(context.Background(), "embedding_graph") })
		}
	}
	typedGraphOwnerAfterSnapshotHook.Unlock()
	defer func() {
		typedGraphOwnerAfterSnapshotHook.Lock()
		typedGraphOwnerAfterSnapshotHook.beforeServingOpen = nil
		typedGraphOwnerAfterSnapshotHook.Unlock()
	}()
	owner, err := col.openTypedGraphReadOwnerWithContext(context.Background(), limits)
	if foldErr != nil {
		if owner != nil {
			_ = owner.Close()
		}
		t.Fatalf("fold at holder-miss boundary: %v", foldErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	current := col.collectionSchemaCoordinator().typedPublication.Load()
	newKey, keyErr := current.servingBase.servingPreparedSearchKey(col)
	if keyErr != nil {
		_ = owner.Close()
		t.Fatal(keyErr)
	}
	if oldKey == newKey || owner.state != current {
		_ = owner.Close()
		t.Fatalf("holder-miss retry old_key_equal=%v owner_current=%v", oldKey == newKey, owner.state == current)
	}
	cache := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	if cache.CacheBuilds != baselineCache.CacheBuilds+2 || cache.CacheMisses != baselineCache.CacheMisses+2 || cache.CacheHits <= baselineCache.CacheHits {
		_ = owner.Close()
		t.Fatalf("fold miss did not fail old build then reuse current holder: %+v", cache)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
		t.Fatal(err)
	}
	requireTypedGraphServingFoundationReleased(t, col)
}

func TestTypedGraphStaleBaseKeeperUsesCompleteNonGraphClosureKey(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	col, fixture, _, _, _, _ := openTypedGraphQualityFixture(t, 128)
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphOverlapLimits()
	installTypedGraphServingStateForInternalTest(t, col, "embedding_graph", limits)
	keeper, err := col.acquireTypedGraphCapturedBaseCache("embedding_graph", limits)
	if err != nil {
		t.Fatal(err)
	}
	coord := col.collectionSchemaCoordinator()
	oldState := coord.typedPublication.Load()
	oldBase := oldState.servingBase
	changedBase := *oldBase
	changedBase.refs = append([]ColumnAssetRef(nil), oldBase.refs...)
	graphRefs := make(map[ColumnAssetRef]struct{}, len(oldBase.view.GraphAssetRefs))
	for _, ref := range oldBase.view.GraphAssetRefs {
		graphRefs[ref] = struct{}{}
	}
	changed := false
	for i, ref := range changedBase.refs {
		if _, isGraph := graphRefs[ref]; isGraph {
			continue
		}
		changedBase.refs[i].PartID += 1 << 32
		changed = true
		break
	}
	if !changed {
		t.Fatal("fixture has no non-graph base ref")
	}
	slices.SortFunc(changedBase.refs, compareColumnAssetRefs)
	changedBase.refsDigest, err = digestTypedGraphServingBaseRefs(changedBase.refs)
	if err != nil {
		t.Fatal(err)
	}
	if changedBase.preparedKey != oldBase.preparedKey {
		t.Fatal("test changed logical graph key")
	}
	changedState := *oldState
	changedState.servingBase = &changedBase
	coord.typedPublication.Store(&changedState)
	col.invalidateTypedGraphStaleBaseKeeper("embedding_graph")

	slot := collectionVectorIndexPreparedSearchCacheSlot{family: collectionVectorIndexPreparedSearchFamilyCapturedBase, indexName: "embedding_graph"}
	col.vectorBufferedSearchMu.Lock()
	entry := col.vectorBufferedSearch[slot]
	col.vectorBufferedSearchMu.Unlock()
	if entry != nil || !keeper.closed {
		t.Fatalf("keeper survived non-graph closure change entry=%v closed=%v", entry, keeper.closed)
	}
	requireTypedGraphServingFoundationReleased(t, col)
}
