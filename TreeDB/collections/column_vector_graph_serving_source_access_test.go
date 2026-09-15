package collections

import (
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

type columnVectorGraphSourceSemantics struct {
	vectors   [][][]float32
	invNorms  []float32
	rowRefs   []DocumentRowRef
	docIDs    [][]byte
	adjacency [][][]uint32
	pack      [4]int
}

func snapshotColumnVectorGraphSourceSemantics(t testing.TB, holder *columnVectorGraphSharedPreparedSearch, rows int) columnVectorGraphSourceSemantics {
	t.Helper()
	if holder == nil || holder.typedVectorSource == nil || holder.invNormSource == nil || holder.rowRefSource == nil || holder.documentIDSource == nil || holder.adjacencyLayerSources == nil {
		t.Fatal("incomplete prepared source holder")
	}
	snapshot := columnVectorGraphSourceSemantics{
		vectors:  make([][][]float32, 1),
		invNorms: make([]float32, rows),
		rowRefs:  make([]DocumentRowRef, rows),
		docIDs:   make([][]byte, rows),
	}
	snapshot.vectors[0] = make([][]float32, rows)
	for ordinal := range rows {
		vector, _, reason, ok := holder.typedVectorSource.vectorForOrdinal(ordinal)
		if !ok {
			t.Fatalf("typed vector ordinal=%d unavailable reason=%s", ordinal, reason)
		}
		snapshot.vectors[0][ordinal] = slices.Clone(vector)
		invNorm, _, reason, ok := holder.invNormSource.invNormForOrdinal(ordinal)
		if !ok {
			t.Fatalf("inverse norm ordinal=%d unavailable reason=%s", ordinal, reason)
		}
		snapshot.invNorms[ordinal] = invNorm
		rowRef, ok := holder.rowRefSource.rowRefForOrdinal(ordinal)
		if !ok {
			t.Fatalf("row ref ordinal=%d unavailable", ordinal)
		}
		rowRef.DocumentID = slices.Clone(rowRef.DocumentID)
		snapshot.rowRefs[ordinal] = rowRef
		documentID, ok := holder.documentIDSource.documentIDForOrdinal(ordinal)
		if !ok {
			t.Fatalf("document ID ordinal=%d unavailable", ordinal)
		}
		snapshot.docIDs[ordinal] = slices.Clone(documentID)
	}
	layers := 0
	if holder.hnswSearchPack != nil {
		layers = holder.hnswSearchPack.Header.AdjacencyLayerCount
		snapshot.pack = [4]int{holder.hnswSearchPack.Header.Rows, holder.hnswSearchPack.Header.Dimensions, len(holder.hnswSearchPack.Sections), layers}
	} else {
		layers = len(holder.adjacencyLayerSources.sources)
	}
	snapshot.adjacency = make([][][]uint32, layers)
	for layer := range layers {
		snapshot.adjacency[layer] = make([][]uint32, rows)
		for ordinal := range rows {
			neighbors, _, reason, ok := holder.adjacencyLayerSources.Neighbors(layer, ordinal)
			if !ok {
				t.Fatalf("adjacency layer=%d ordinal=%d unavailable reason=%s", layer, ordinal, reason)
			}
			snapshot.adjacency[layer][ordinal] = slices.Clone(neighbors)
		}
	}
	return snapshot
}

func columnVectorGraphHolderSourceManagers(holder *columnVectorGraphSharedPreparedSearch) map[string]*mappedresource.Manager {
	managers := make(map[string]*mappedresource.Manager)
	if holder == nil {
		return managers
	}
	if holder.typedVectorSource != nil && holder.typedVectorSource.manager != nil {
		managers["typed_vectors"] = holder.typedVectorSource.manager
	}
	if holder.invNormSource != nil && holder.invNormSource.manager != nil {
		managers["inverse_norm"] = holder.invNormSource.manager
	}
	if holder.rowRefSource != nil && holder.rowRefSource.manager != nil {
		managers["row_refs"] = holder.rowRefSource.manager
	}
	if holder.documentIDSource != nil && holder.documentIDSource.manager != nil {
		managers["document_ids"] = holder.documentIDSource.manager
	}
	if holder.hnswSearchPack != nil && holder.hnswSearchPack.manager != nil {
		managers["hnsw_pack"] = holder.hnswSearchPack.manager
	}
	if holder.adjacencyLayerSources != nil {
		for layer, source := range holder.adjacencyLayerSources.sources {
			if source != nil && source.manager != nil {
				managers["adjacency_"+strconv.Itoa(layer)] = source.manager
			}
		}
	}
	return managers
}

func requireColumnVectorGraphServingManagerSources(t testing.TB, holder *columnVectorGraphSharedPreparedSearch, want mappedresource.Source) int64 {
	t.Helper()
	managers := columnVectorGraphHolderSourceManagers(holder)
	if len(managers) == 0 {
		t.Fatal("serving holder has no logical source managers")
	}
	var handles int64
	for name, manager := range managers {
		stats := manager.Stats()
		if stats.Opens != 0 || stats.Closes != 0 || stats.ActiveHandles == 0 || stats.ValidationModeReads[mappedresource.ValidationVerify] == 0 {
			t.Fatalf("%s logical manager stats=%+v", name, stats)
		}
		handles += stats.ActiveHandles
		for _, pin := range manager.PinSummary() {
			if pin.Source != want || pin.Root == "" || pin.Path == "" {
				t.Fatalf("%s logical pin=%+v want source=%q and storage identity", name, pin, want)
			}
		}
	}
	return handles
}

func TestTypedGraphServingPackedSourcesUseOneSegmentPool(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	modes := []struct {
		name     string
		mapErr   error
		want     mappedresource.Source
		isMapped bool
	}{
		{name: "mapped", want: mappedresource.SourceMapped, isMapped: true},
		{name: "ordinary_mmap_failure", mapErr: errColumnServingLeaseInjected, want: mappedresource.SourceHeapCopy},
		{name: "mmap_unsupported", mapErr: mappedresource.ErrMmapUnsupported, want: mappedresource.SourceHeapCopy},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			if mode.isMapped && !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
				t.Skip("prefix mmap is unsupported")
			}
			const rows = 32
			col, base, _, _, columns, _ := openTypedGraphQualityFixture(t, rows)
			genericHolder := base.reader.sharedPreparedSearch.holder
			if genericHolder == nil || genericHolder.servingSegments != nil || genericHolder.servingSourceAccess != nil || genericHolder.stats().Opens == 0 {
				t.Fatalf("generic prepared route did not remain private: holder=%p stats=%+v", genericHolder, genericHolder.stats())
			}
			wantSemantics := snapshotColumnVectorGraphSourceSemantics(t, genericHolder, rows)
			query := columns[0].Float32Vectors[0]
			wantSearch, err := base.Search(VectorIndexSearcherSearchOptions{Query: query, TopK: 4, EfSearch: rows, StatsMode: VectorIndexSearchStatsModeMinimal})
			if err != nil {
				t.Fatal(err)
			}
			if err := base.Close(); err != nil {
				t.Fatal(err)
			}
			limits := typedGraphOverlapLimits()
			installTypedGraphServingStateForInternalTest(t, col, "embedding_graph", limits)
			var readAtCalls atomic.Int64
			if mode.mapErr != nil {
				installColumnServingLeaseHooks(t, func() {
					columnServingSegmentLeaseHooks.Lock()
					columnServingSegmentLeaseHooks.mmap = func(*os.File, int64) ([]byte, error) { return nil, mode.mapErr }
					columnServingSegmentLeaseHooks.readAt = func(file *os.File, dst []byte, offset int64) (int, error) {
						readAtCalls.Add(1)
						return file.ReadAt(dst, offset)
					}
					columnServingSegmentLeaseHooks.Unlock()
				})
			}
			owner, err := col.openTypedGraphReadOwner(limits)
			if err != nil {
				t.Fatal(err)
			}
			holder := owner.overlay.base.reader.sharedPreparedSearch.holder
			if holder == nil || holder.key.family != columnVectorGraphSharedPreparedSearchKeyServing || holder.servingSourceAccess == nil || holder.servingSourceAccess.pool != holder.servingSegments {
				_ = owner.Close()
				t.Fatal("serving holder did not retain its single source-access capability")
			}
			queryInvNorm, err := columnVectorGraphInvNorm(query)
			if err != nil {
				_ = owner.Close()
				t.Fatal(err)
			}
			source := columnVectorGraphSearchSource{
				reader:                       owner.overlay.base.reader,
				dims:                         holder.preparedSearch.dims,
				typedVectorSource:            holder.typedVectorSource,
				preparedVector:               holder.preparedSearch.vector,
				preparedScoreReady:           true,
				preparedScoreIdentityMapping: holder.preparedSearch.vectorIdentityMapping,
				preparedNorm:                 holder.preparedSearch.norm,
			}
			var sourceStats columnVectorGraphNativeSearchStats
			if _, handled, err := source.scorePreparedOrdinal(nil, query, queryInvNorm, 0, &sourceStats); err != nil || !handled {
				_ = owner.Close()
				t.Fatalf("prepared source score handled=%v err=%v", handled, err)
			}
			requireColumnVectorGraphPreparedHolderSourceStats(t, sourceStats, mode.isMapped)
			var combinedStats columnVectorGraphNativeSearchStats
			if _, err := holder.preparedSearch.scoreOrdinal(nil, query, queryInvNorm, 0, &combinedStats); err != nil {
				_ = owner.Close()
				t.Fatal(err)
			}
			requireColumnVectorGraphPreparedHolderSourceStats(t, combinedStats, mode.isMapped)
			minimalCounters := newColumnVectorGraphPreparedMinimalSearchCounters(holder.preparedSearch)
			minimalCounters.recordPreparedScore(0, false, true)
			var minimalStats columnVectorGraphNativeSearchStats
			minimalCounters.publish(&minimalStats)
			requireColumnVectorGraphPreparedHolderSourceStats(t, minimalStats, mode.isMapped)
			gotSemantics := snapshotColumnVectorGraphSourceSemantics(t, holder, rows)
			if !reflect.DeepEqual(gotSemantics, wantSemantics) {
				_ = owner.Close()
				t.Fatal("pool-backed decoded/prepared sources differ from the generic route")
			}
			var searchBuffer VectorIndexSearchBuffer
			gotSearch, readView, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: "embedding_graph", Query: query, TopK: 4, EfSearch: rows, StatsMode: VectorIndexSearchStatsModeMinimal}, &searchBuffer)
			if err != nil {
				_ = owner.Close()
				t.Fatal(err)
			}
			resultsEqual := len(gotSearch.Results) == len(wantSearch.Results)
			for i := 0; resultsEqual && i < len(gotSearch.Results); i++ {
				resultsEqual = bytes.Equal(gotSearch.Results[i].ID, wantSearch.Results[i].ID) && gotSearch.Results[i].Score == wantSearch.Results[i].Score
			}
			if !resultsEqual {
				_ = readView.Close()
				_ = owner.Close()
				t.Fatalf("public serving result semantics changed: got=%+v want=%+v", gotSearch.Results, wantSearch.Results)
			}
			stats := gotSearch.Stats
			if stats.SearchRouteColumnGraphPrepared != 1 || stats.SearchRouteColumnGraphFallback != 0 || stats.SearchRouteHNSWSearchPack != 1 || stats.HNSWSearchPackActive != 1 || stats.HNSWSearchPackFallbacks != 0 || stats.ColumnGraphWork.Route != "typed_hnsw" || !stats.ColumnGraphWork.Completed || stats.GraphRowFallbacks != 0 || stats.VectorScratchDecodes != 0 || stats.AdjacencyTypedListScratchDecodes != 0 {
				_ = readView.Close()
				_ = owner.Close()
				t.Fatalf("public serving route/diagnostics changed under %s: %+v work=%+v", mode.name, stats, stats.ColumnGraphWork)
			}
			if mode.isMapped {
				if stats.HNSWSearchPackMmapDirect != 1 || stats.HNSWSearchPackHeapCopy != 0 || stats.HNSWSearchPackMappedBytes == 0 || stats.HNSWSearchPackHeapCopyBytes != 0 {
					_ = readView.Close()
					_ = owner.Close()
					t.Fatalf("mapped public source diagnostics=%+v", stats)
				}
			} else if stats.HNSWSearchPackHeapCopy != 1 || stats.HNSWSearchPackMmapDirect != 0 || stats.HNSWSearchPackHeapCopyBytes == 0 || stats.HNSWSearchPackMappedBytes != 0 {
				_ = readView.Close()
				_ = owner.Close()
				t.Fatalf("fallback public source diagnostics=%+v", stats)
			}
			if err := readView.Close(); err != nil {
				_ = owner.Close()
				t.Fatal(err)
			}
			logicalHandles := requireColumnVectorGraphServingManagerSources(t, holder, mode.want)
			physical := holder.servingSegments.ledger.snapshot()
			if mode.isMapped {
				if physical.mappedBackings == 0 || physical.descriptorsLive != 0 || physical.fallbackSegments != 0 || physical.fallbackBackings != 0 || physical.totalOpens != uint64(physical.mappedBackings) || physical.totalCloses != physical.totalOpens {
					_ = owner.Close()
					t.Fatalf("mapped serving physical stats=%+v", physical)
				}
				_ = logicalHandles
			} else {
				if physical.mappedBackings != 0 || physical.descriptorsLive == 0 || physical.descriptorsLive != physical.fallbackSegments || physical.totalOpens != uint64(physical.fallbackSegments) || physical.fallbackBackings == 0 || physical.totalCloses != 0 {
					_ = owner.Close()
					t.Fatalf("fallback serving physical stats=%+v", physical)
				}
				if got := readAtCalls.Load(); got <= int64(physical.fallbackBackings) {
					_ = owner.Close()
					t.Fatalf("setup reads=%d persistent parent backings=%d; setup did not remain transient", got, physical.fallbackBackings)
				}
				for i := range holder.servingSegments.segments {
					if !errors.Is(holder.servingSegments.segments[i].mapErr, mode.mapErr) {
						_ = owner.Close()
						t.Fatalf("segment %d mmap classification=%v want %v", i, holder.servingSegments.segments[i].mapErr, mode.mapErr)
					}
				}
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			after := col.collectionSchemaCoordinator().typedGraphPhysical.snapshot()
			if !after.empty() || after.totalCloses != after.totalOpens {
				t.Fatalf("serving source close did not reconcile pool: %+v", after)
			}
			requireTypedGraphServingFoundationReleased(t, col)
		})
	}
}

func requireColumnVectorGraphPreparedHolderSourceStats(t testing.TB, stats columnVectorGraphNativeSearchStats, mapped bool) {
	t.Helper()
	if stats.PreparedScoreCalls != 1 || stats.CandidateFetches != 1 || stats.VectorPreparedDirectViews != 1 || stats.NormPreparedDirectViews != 1 {
		t.Fatalf("prepared source stats=%+v", stats)
	}
	if mapped {
		if stats.VectorDirectViews != 1 || stats.VectorMmapDirectViews != 1 || stats.VectorHeapCopyTypedViews != 0 || stats.NormDirectViews != 1 || stats.NormMmapDirectViews != 1 || stats.NormHeapCopyTypedViews != 0 {
			t.Fatalf("mapped prepared source stats=%+v", stats)
		}
		return
	}
	if stats.VectorDirectViews != 0 || stats.VectorMmapDirectViews != 0 || stats.VectorHeapCopyTypedViews != 1 || stats.NormDirectViews != 0 || stats.NormMmapDirectViews != 0 || stats.NormHeapCopyTypedViews != 1 {
		t.Fatalf("fallback prepared source stats=%+v", stats)
	}
}

func columnVectorGraphParentRefsForManagers(t testing.TB, pool *columnServingSegmentLeaseSet, managers map[string]*mappedresource.Manager) map[ColumnAssetRef]struct{} {
	t.Helper()
	parents := make(map[ColumnAssetRef]struct{})
	for name, manager := range managers {
		for _, pin := range manager.PinSummary() {
			found := false
			for _, ref := range pool.refs {
				end := pin.Key.Offset + pin.Key.Length
				if pin.Key.Namespace == ref.Namespace && pin.Key.Kind == string(ref.Kind) && pin.Key.Generation == ref.Generation && pin.Key.PartID == ref.PartID && pin.Key.FileID == ref.FileID && pin.Key.Offset >= ref.Offset && end >= pin.Key.Offset && end <= ref.Offset+ref.Length {
					parents[ref] = struct{}{}
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("%s pin has no exact authorized parent: %+v", name, pin)
			}
		}
	}
	return parents
}

func TestTypedGraphServingStandaloneStateSourcesUseCommonAccess(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	for _, forceFallback := range []bool{false, true} {
		name := "mapped"
		if forceFallback {
			name = "fallback"
		}
		t.Run(name, func(t *testing.T) {
			if !forceFallback && !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
				t.Skip("prefix mmap is unsupported")
			}
			const rows = 24
			_, db, col, def := openColumnGraphRebuildTestCollectionV2A(t, 8, 16, columnGraphRebuildSyntheticRowsV2A(rows, 8))
			defer db.Close()
			if _, err := col.RebuildVectorIndex(def.Name); err != nil {
				t.Fatal(err)
			}
			snap, err := col.acquireColumnVectorGraphPhysicalRowReaderSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snap.Close()
			_, graph, view, err := col.columnVectorGraphPhysicalRowReaderSnapshotViewAtSnapshot(def.Name, snap)
			if err != nil {
				t.Fatal(err)
			}
			view.VectorIndexState.Assets = slices.DeleteFunc(slices.Clone(view.VectorIndexState.Assets), func(asset columnVectorIndexStateAssetSnapshot) bool {
				return asset.Role == columnVectorIndexStateAssetRoleHNSWSearchPack
			})
			candidate, err := prepareTypedGraphServingBaseMetadata(graph, view, typedGraphOverlapLimits().Cold)
			if err != nil {
				t.Fatal(err)
			}
			genericView := candidate.view
			genericView.snapshot = snap
			generic, err := col.buildColumnVectorGraphSharedPreparedSearchFromView(snap, def, candidate.graph, genericView)
			if err != nil {
				t.Fatal(err)
			}
			wantSemantics := snapshotColumnVectorGraphSourceSemantics(t, generic, rows)
			if generic.hnswSearchPack != nil || generic.documentIDSource.manager == nil || generic.rowRefSource.manager == nil || len(generic.adjacencyLayerSources.sources) == 0 {
				_ = generic.close()
				t.Fatal("candidate did not exercise standalone state sources")
			}
			if err := generic.close(); err != nil {
				t.Fatal(err)
			}

			key, err := candidate.servingPreparedSearchKey(col)
			if err != nil {
				t.Fatal(err)
			}
			guardian := &ColumnAssetLifecyclePinSet{source: ColumnAssetLifecyclePinSourcePreparedQuery, owner: "standalone-serving-source-test", refs: slices.Clone(candidate.refs)}
			ledger := new(typedGraphPhysicalResourceLedger)
			pool, err := newColumnServingSegmentLeaseSet(key, candidate.refs, guardian, ledger, typedGraphOverlapLimits().Physical)
			if err != nil {
				t.Fatal(err)
			}
			if forceFallback {
				installColumnServingLeaseHooks(t, func() {
					columnServingSegmentLeaseHooks.Lock()
					columnServingSegmentLeaseHooks.mmap = func(*os.File, int64) ([]byte, error) { return nil, errColumnServingLeaseInjected }
					columnServingSegmentLeaseHooks.Unlock()
				})
			}
			serving, err := col.buildColumnVectorGraphServingPreparedSearchFromBase(snap, candidate, pool)
			if err != nil {
				_ = pool.Close()
				t.Fatal(err)
			}
			serving.servingSegments = pool
			serving.key = key
			if !serving.ready() {
				_ = serving.close()
				t.Fatal("standalone serving source holder is not ready")
			}
			gotSemantics := snapshotColumnVectorGraphSourceSemantics(t, serving, rows)
			if !reflect.DeepEqual(gotSemantics, wantSemantics) {
				_ = serving.close()
				t.Fatal("standalone pool-backed sources differ from generic decoded data")
			}
			managers := columnVectorGraphHolderSourceManagers(serving)
			logicalHandles := requireColumnVectorGraphServingManagerSources(t, serving, map[bool]mappedresource.Source{false: mappedresource.SourceMapped, true: mappedresource.SourceHeapCopy}[forceFallback])
			parents := columnVectorGraphParentRefsForManagers(t, pool, managers)
			physical := ledger.snapshot()
			if serving.hnswSearchPack != nil || serving.documentIDSource.manager.Stats().ActiveHandles != 2 || serving.rowRefSource.manager.Stats().ActiveHandles != int64(len(columnVectorGraphRowRefStateFields)) || len(serving.adjacencyLayerSources.sources) == 0 {
				_ = serving.close()
				t.Fatalf("standalone source shape row_handles=%d doc_handles=%d adjacency=%d", serving.rowRefSource.manager.Stats().ActiveHandles, serving.documentIDSource.manager.Stats().ActiveHandles, len(serving.adjacencyLayerSources.sources))
			}
			if forceFallback {
				if physical.fallbackBackings != len(parents) || logicalHandles <= int64(physical.fallbackBackings) || physical.descriptorsLive != physical.fallbackSegments {
					_ = serving.close()
					t.Fatalf("standalone parent fallback sharing stats=%+v parents=%d logical_handles=%d", physical, len(parents), logicalHandles)
				}
			} else if physical.mappedBackings == 0 || physical.descriptorsLive != 0 || physical.fallbackBackings != 0 {
				_ = serving.close()
				t.Fatalf("standalone mapped stats=%+v", physical)
			}
			if err := serving.close(); err != nil {
				t.Fatal(err)
			}
			if after := ledger.snapshot(); !after.empty() || after.totalCloses != after.totalOpens {
				t.Fatalf("standalone serving source close did not reconcile: %+v", after)
			}
		})
	}
}

func TestColumnVectorGraphSourceAccessSetupBorrowIsTransientAndRetryable(t *testing.T) {
	fixture := newColumnServingLeaseTestFixture(t, 1)
	ref := fixture.refs[0]
	var reads atomic.Int64
	installColumnServingLeaseHooks(t, func() {
		columnServingSegmentLeaseHooks.Lock()
		columnServingSegmentLeaseHooks.mmap = func(*os.File, int64) ([]byte, error) { return nil, errColumnServingLeaseInjected }
		columnServingSegmentLeaseHooks.readAt = func(file *os.File, dst []byte, offset int64) (int, error) {
			reads.Add(1)
			return file.ReadAt(dst, offset)
		}
		columnServingSegmentLeaseHooks.Unlock()
	})
	access, err := newColumnVectorGraphServingSourceAccess(context.Background(), fixture.set)
	if err != nil {
		t.Fatal(err)
	}
	// The capability must reject a same-FileID ref that is not the exact
	// authorized parent before it can touch the already-inventoried segment.
	suffix := ref
	suffix.Generation++
	suffix.PartID++
	suffix.Offset++
	suffix.Length--
	if err := access.withAsset(nil, fixture.root, suffix, "unauthorized suffix", func([]byte) error { return nil }); !errors.Is(err, errColumnServingSegmentUnauthorized) {
		t.Fatalf("same-file unauthorized setup ref err=%v", err)
	}
	key, scope := fixture.rangeIdentity(ref, ref.Length-1, 2)
	if _, err := access.acquireRange(nil, fixture.root, ref, mappedresource.NewManager(), key, scope, mappedresource.AcquireOptions{PreferMapped: true, AllowHeapCopy: true}); !errors.Is(err, errColumnServingSegmentUnauthorized) {
		t.Fatalf("authorized parent with overflowing logical range err=%v", err)
	}
	unauthorized := fixture.ledger.snapshot()
	if unauthorized.descriptorsLive != 0 || unauthorized.mappedBackings != 0 || unauthorized.fallbackSegments != 0 || reads.Load() != 0 {
		t.Fatalf("unauthorized adapter requests reached physical acquisition: stats=%+v reads=%d", unauthorized, reads.Load())
	}
	injected := errors.New("injected setup parse failure")
	if err := access.withAsset(nil, fixture.root, ref, "setup failure", func([]byte) error { return injected }); !errors.Is(err, injected) {
		t.Fatalf("setup failure=%v", err)
	}
	failed := fixture.ledger.snapshot()
	if failed.fallbackBackings != 0 || failed.fallbackBytes != 0 || failed.descriptorsLive != 1 || reads.Load() != 1 {
		t.Fatalf("failed setup retained a logical fallback: stats=%+v reads=%d", failed, reads.Load())
	}
	if err := access.withAsset(nil, fixture.root, ref, "setup retry", func(raw []byte) error {
		if int64(len(raw)) != ref.Length {
			t.Fatalf("retry setup bytes=%d want=%d", len(raw), ref.Length)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	afterRetry := fixture.ledger.snapshot()
	if afterRetry.fallbackBackings != 0 || afterRetry.fallbackBytes != 0 || reads.Load() != 2 {
		t.Fatalf("successful setup retry retained a logical fallback: stats=%+v reads=%d", afterRetry, reads.Load())
	}
	if err := fixture.set.Close(); err != nil {
		t.Fatal(err)
	}
	requireColumnServingLedgerEmpty(t, fixture.ledger)
}

type typedGraphServingScalarU8SourceFixture struct {
	db       interface{ Close() error }
	col      *Collection
	limits   typedGraphReadOwnerLimits
	query    []float32
	selected VectorIndexSearchOptions
	codes    ColumnAssetRef
}

func openTypedGraphServingScalarU8SourceFixture(t testing.TB) typedGraphServingScalarU8SourceFixture {
	t.Helper()
	meta := typedMinimaCollectionMeta()
	const quantizedName = "embedding.scalar_u8.legacy"
	meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: quantizedName}}
	_, db, col := openTypedMinimaCollectionMeta(t, meta)
	ids := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	retained := [][]byte{[]byte(`{"id":"a"}`), []byte(`{"id":"b"}`), []byte(`{"id":"c"}`), []byte(`{"id":"d"}`)}
	vectors := [][]float32{
		{1, 0, 0, 0, 0, 0, 0, 0},
		{0.9, 0.1, 0, 0, 0, 0, 0, 0},
		{0, 1, 0, 0, 0, 0, 0, 0},
		{-1, 0, 0, 0, 0, 0, 0, 0},
	}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: vectors},
		{Name: "content", Strings: []string{"a", "b", "c", "d"}},
		{Name: "user", Strings: []string{"u", "u", "u", "u"}},
		{Name: "path", Strings: []string{"p", "p", "p", "p"}},
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	limits := typedGraphOverlapLimits()
	installTypedGraphServingStateForInternalTest(t, col, "embedding_graph", limits)
	state := col.collectionSchemaCoordinator().typedPublication.Load()
	if state == nil || state.servingBase == nil {
		_ = db.Close()
		t.Fatal("serving scalar-u8 fixture has no installed base metadata")
	}
	def := state.servingBase.view.Catalog.meta.VectorIndexes[0]
	assets, ok := columnVectorGraphQuantizedAssetSetsByName(state.servingBase.view.VectorIndexState, def)[quantizedName]
	if !ok || !assets.HasCodes || assets.Codes.Ref.Length <= 0 {
		_ = db.Close()
		t.Fatalf("serving scalar-u8 fixture code plane=%+v", assets)
	}
	return typedGraphServingScalarU8SourceFixture{
		db:     db,
		col:    col,
		limits: limits,
		query:  vectors[0],
		selected: VectorIndexSearchOptions{
			IndexName:                 "embedding_graph",
			Query:                     vectors[0],
			QueryMode:                 VectorIndexQueryModeQuantizedRerank,
			QuantizedIndexName:        quantizedName,
			QuantizedRerankCandidates: len(vectors),
			TopK:                      2,
			EfSearch:                  len(vectors),
			StatsMode:                 VectorIndexSearchStatsModeMinimal,
		},
		codes: assets.Codes.Ref,
	}
}

func typedGraphServingScalarU8Holder(t testing.TB, owner *typedGraphReadOwner) *columnVectorGraphSharedPreparedSearch {
	t.Helper()
	if owner == nil || owner.overlay == nil || owner.overlay.base == nil || owner.overlay.base.reader == nil || owner.overlay.base.reader.sharedPreparedSearch == nil {
		t.Fatal("serving scalar-u8 owner has no prepared holder")
	}
	holder := owner.overlay.base.reader.sharedPreparedSearch.holder
	if holder == nil || holder.servingSegments == nil || holder.servingSourceAccess == nil {
		t.Fatal("serving scalar-u8 owner has no source capability")
	}
	return holder
}

func typedGraphServingScalarU8Status(t testing.TB, holder *columnVectorGraphSharedPreparedSearch) columnVectorGraphQuantizedAssetLoadStatus {
	t.Helper()
	holder.legacyScalarU8Mu.Lock()
	defer holder.legacyScalarU8Mu.Unlock()
	if len(holder.legacyScalarU8Entries) != 1 || holder.legacyScalarU8Entries[0] == nil || holder.legacyScalarU8Entries[0].building {
		t.Fatalf("serving scalar-u8 entries=%+v", holder.legacyScalarU8Entries)
	}
	return holder.legacyScalarU8Entries[0].status
}

func TestTypedGraphServingLazyScalarU8UsesSharedSegmentPool(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	modes := []struct {
		name   string
		mapErr error
		want   mappedresource.Source
	}{
		{name: "mapped", want: mappedresource.SourceMapped},
		{name: "ordinary_mmap_failure", mapErr: errColumnServingLeaseInjected, want: mappedresource.SourceHeapCopy},
		{name: "mmap_unsupported", mapErr: mappedresource.ErrMmapUnsupported, want: mappedresource.SourceHeapCopy},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			fixture := openTypedGraphServingScalarU8SourceFixture(t)
			defer func() {
				if err := fixture.db.Close(); err != nil {
					t.Errorf("db close: %v", err)
				}
			}()
			if mode.mapErr != nil {
				installColumnServingLeaseHooks(t, func() {
					columnServingSegmentLeaseHooks.Lock()
					columnServingSegmentLeaseHooks.mmap = func(*os.File, int64) ([]byte, error) { return nil, mode.mapErr }
					columnServingSegmentLeaseHooks.Unlock()
				})
			}
			anchor, err := fixture.col.openTypedGraphReadOwner(fixture.limits)
			if err != nil {
				t.Fatal(err)
			}
			holder := typedGraphServingScalarU8Holder(t, anchor)
			before := holder.servingSegments.ledger.snapshot()
			if len(holder.legacyScalarU8Entries) != 0 {
				_ = anchor.Close()
				t.Fatal("serving holder loaded the lazy scalar-u8 plane during construction")
			}

			const callers = 8
			type result struct {
				response VectorIndexSearchResponse
				err      error
			}
			started := make(chan struct{})
			results := make(chan result, callers)
			var ready sync.WaitGroup
			ready.Add(callers)
			for range callers {
				go func() {
					ready.Done()
					<-started
					var buffer VectorIndexSearchBuffer
					response, view, searchErr := fixture.col.SearchVectorIndexWithBufferReadView(fixture.selected, &buffer)
					if view != nil {
						searchErr = errors.Join(searchErr, view.Close())
					}
					results <- result{response: response, err: searchErr}
				}()
			}
			ready.Wait()
			close(started)
			for range callers {
				got := <-results
				if got.err != nil {
					_ = anchor.Close()
					t.Fatal(got.err)
				}
				if len(got.response.Results) != 2 || !bytes.Equal(got.response.Results[0].ID, []byte("a")) || !bytes.Equal(got.response.Results[1].ID, []byte("b")) {
					_ = anchor.Close()
					t.Fatalf("selected scalar-u8 results=%+v", got.response.Results)
				}
				stats := got.response.Stats
				if stats.SearchRouteQuantizedRerank != 1 || stats.SearchRouteColumnGraphPrepared != 1 || stats.SearchRouteColumnGraphFallback != 0 || stats.ColumnGraphWork.Route != "typed_hnsw" || !stats.ColumnGraphWork.Completed || stats.QuantizedScorerActive != 1 || stats.QuantizedAssetActiveHandles != 1 {
					_ = anchor.Close()
					t.Fatalf("selected scalar-u8 route stats=%+v work=%+v", stats, stats.ColumnGraphWork)
				}
				if mode.want == mappedresource.SourceMapped {
					if stats.QuantizedAssetMmapDirect != 1 || stats.QuantizedAssetHeapCopy != 0 || stats.QuantizedAssetMappedBytes == 0 || stats.QuantizedAssetHeapCopyBytes != 0 {
						_ = anchor.Close()
						t.Fatalf("mapped scalar-u8 diagnostics=%+v", stats)
					}
				} else if stats.QuantizedAssetHeapCopy != 1 || stats.QuantizedAssetMmapDirect != 0 || stats.QuantizedAssetHeapCopyBytes == 0 || stats.QuantizedAssetMappedBytes != 0 {
					_ = anchor.Close()
					t.Fatalf("fallback scalar-u8 diagnostics=%+v", stats)
				}
			}

			status := typedGraphServingScalarU8Status(t, holder)
			if status.resource == nil || !status.ownsResource || status.Prepared == nil || status.resource.handle == nil || status.resource.handle.Source() != mode.want {
				_ = anchor.Close()
				t.Fatalf("serving scalar-u8 retained status=%+v want source=%q", status, mode.want)
			}
			managerStats := status.resource.manager.Stats()
			if managerStats.Opens != 0 || managerStats.Closes != 0 || managerStats.ActiveHandles != 1 || managerStats.ValidationModeReads[mappedresource.ValidationVerify] != 1 {
				_ = anchor.Close()
				t.Fatalf("serving scalar-u8 logical manager stats=%+v", managerStats)
			}
			after := holder.servingSegments.ledger.snapshot()
			if mode.want == mappedresource.SourceHeapCopy {
				if after.fallbackBackings != before.fallbackBackings+1 || after.fallbackBytes != before.fallbackBytes+fixture.codes.Length {
					_ = anchor.Close()
					t.Fatalf("lazy scalar-u8 did not add exactly one parent fallback: before=%+v after=%+v ref=%+v", before, after, fixture.codes)
				}
			} else if after.fallbackBackings != before.fallbackBackings || after.fallbackBytes != before.fallbackBytes {
				_ = anchor.Close()
				t.Fatalf("mapped lazy scalar-u8 changed fallback accounting: before=%+v after=%+v", before, after)
			}
			if err := anchor.Close(); err != nil {
				t.Fatal(err)
			}
			if final := fixture.col.collectionSchemaCoordinator().typedGraphPhysical.snapshot(); !final.empty() || final.totalCloses != final.totalOpens {
				t.Fatalf("lazy scalar-u8 final close did not reconcile: %+v", final)
			}
		})
	}
}

func TestTypedGraphServingLazyScalarU8SurvivesOriginalPathRetirement(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	fixture := openTypedGraphServingScalarU8SourceFixture(t)
	defer func() {
		if err := fixture.db.Close(); err != nil {
			t.Errorf("db close: %v", err)
		}
	}()
	installColumnServingLeaseHooks(t, func() {
		columnServingSegmentLeaseHooks.Lock()
		columnServingSegmentLeaseHooks.mmap = func(*os.File, int64) ([]byte, error) { return nil, errColumnServingLeaseInjected }
		columnServingSegmentLeaseHooks.Unlock()
	})
	anchor, err := fixture.col.openTypedGraphReadOwner(fixture.limits)
	if err != nil {
		t.Fatal(err)
	}
	holder := typedGraphServingScalarU8Holder(t, anchor)
	if len(holder.legacyScalarU8Entries) != 0 {
		_ = anchor.Close()
		t.Fatal("serving holder loaded the lazy scalar-u8 plane during construction")
	}
	rootDir := fixture.col.db.ColumnAssetRootDir()
	_, retained, source, err := readColumnVectorGraphQuantizedAssetResourceBytesWithSourceAccess(context.Background(), rootDir, fixture.codes, holder.servingSourceAccess)
	if err != nil {
		_ = anchor.Close()
		t.Fatal(err)
	}
	if source != mappedresource.SourceHeapCopy {
		_ = retained.close()
		_ = anchor.Close()
		t.Fatalf("retained scalar-u8 source=%q want heap-copy", source)
	}
	codesPath, err := columnAssetSegmentPath(fixture.col.db.ColumnAssetRootDir(), fixture.codes)
	if err != nil {
		_ = retained.close()
		_ = anchor.Close()
		t.Fatal(err)
	}
	retiredPath := codesPath + ".retired"
	if err := os.Rename(codesPath, retiredPath); err != nil {
		_ = retained.close()
		_ = anchor.Close()
		t.Fatal(err)
	}
	restored := false
	defer func() {
		if !restored {
			if err := os.Rename(retiredPath, codesPath); err != nil {
				t.Errorf("restore retired segment: %v", err)
			}
		}
	}()

	var buffer VectorIndexSearchBuffer
	response, view, err := fixture.col.SearchVectorIndexWithBufferReadView(fixture.selected, &buffer)
	if view != nil {
		err = errors.Join(err, view.Close())
	}
	if err != nil || len(response.Results) != 2 || response.Stats.QuantizedScorerActive != 1 || response.Stats.QuantizedAssetHeapCopy != 1 {
		_ = retained.close()
		_ = anchor.Close()
		t.Fatalf("retired-path scalar-u8 response=%+v err=%v", response, err)
	}
	if err := os.Rename(retiredPath, codesPath); err != nil {
		_ = retained.close()
		_ = anchor.Close()
		t.Fatal(err)
	}
	restored = true
	if err := retained.close(); err != nil {
		_ = anchor.Close()
		t.Fatal(err)
	}
	if err := anchor.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTypedGraphServingLazyScalarU8ReadFailureRetries(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	fixture := openTypedGraphServingScalarU8SourceFixture(t)
	defer func() {
		if err := fixture.db.Close(); err != nil {
			t.Errorf("db close: %v", err)
		}
	}()
	codesPath, err := columnAssetSegmentPath(fixture.col.db.ColumnAssetRootDir(), fixture.codes)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected lazy scalar-u8 parent read failure")
	var fail atomic.Bool
	fail.Store(true)
	var codeReads atomic.Int64
	installColumnServingLeaseHooks(t, func() {
		columnServingSegmentLeaseHooks.Lock()
		columnServingSegmentLeaseHooks.mmap = func(*os.File, int64) ([]byte, error) { return nil, errColumnServingLeaseInjected }
		columnServingSegmentLeaseHooks.readAt = func(file *os.File, dst []byte, offset int64) (int, error) {
			if file.Name() == codesPath && offset == fixture.codes.Offset && int64(len(dst)) == fixture.codes.Length {
				codeReads.Add(1)
				if fail.CompareAndSwap(true, false) {
					return 0, injected
				}
			}
			return file.ReadAt(dst, offset)
		}
		columnServingSegmentLeaseHooks.Unlock()
	})
	anchor, err := fixture.col.openTypedGraphReadOwner(fixture.limits)
	if err != nil {
		t.Fatal(err)
	}
	holder := typedGraphServingScalarU8Holder(t, anchor)
	before := holder.servingSegments.ledger.snapshot()
	var buffer VectorIndexSearchBuffer
	failed, failedView, err := fixture.col.SearchVectorIndexWithBufferReadView(fixture.selected, &buffer)
	if failedView != nil {
		_ = failedView.Close()
	}
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) || !bytes.Contains([]byte(err.Error()), []byte(injected.Error())) || len(failed.Results) != 0 {
		_ = anchor.Close()
		t.Fatalf("lazy scalar-u8 injected failure response=%+v view=%v err=%v", failed, failedView, err)
	}
	holder.legacyScalarU8Mu.Lock()
	entries := len(holder.legacyScalarU8Entries)
	var retained bool
	for _, entry := range holder.legacyScalarU8Entries[:cap(holder.legacyScalarU8Entries)] {
		retained = retained || entry != nil
	}
	holder.legacyScalarU8Mu.Unlock()
	if entries != 0 || retained {
		_ = anchor.Close()
		t.Fatalf("failed lazy scalar-u8 generation retained entry len=%d backing=%v", entries, retained)
	}
	afterFailure := holder.servingSegments.ledger.snapshot()
	if afterFailure.fallbackBackings != before.fallbackBackings || afterFailure.fallbackBytes != before.fallbackBytes || afterFailure.fallbackBytesInFlight != 0 {
		_ = anchor.Close()
		t.Fatalf("failed lazy scalar-u8 parent allocation did not roll back: before=%+v after=%+v", before, afterFailure)
	}

	buffer.Reset()
	retried, retriedView, err := fixture.col.SearchVectorIndexWithBufferReadView(fixture.selected, &buffer)
	if err != nil {
		_ = anchor.Close()
		t.Fatalf("lazy scalar-u8 retry: %v", err)
	}
	if retriedView == nil || len(retried.Results) != 2 || retried.Stats.QuantizedAssetHeapCopy != 1 || retried.Stats.QuantizedAssetMmapDirect != 0 {
		if retriedView != nil {
			_ = retriedView.Close()
		}
		_ = anchor.Close()
		t.Fatalf("lazy scalar-u8 retry response=%+v view=%v", retried, retriedView)
	}
	if err := retriedView.Close(); err != nil {
		_ = anchor.Close()
		t.Fatal(err)
	}
	if got := codeReads.Load(); got != 2 {
		_ = anchor.Close()
		t.Fatalf("lazy scalar-u8 code parent reads=%d want failed attempt plus retry", got)
	}
	afterRetry := holder.servingSegments.ledger.snapshot()
	if afterRetry.fallbackBackings != before.fallbackBackings+1 || afterRetry.fallbackBytes != before.fallbackBytes+fixture.codes.Length || afterRetry.totalBuildFailures != before.totalBuildFailures+1 {
		_ = anchor.Close()
		t.Fatalf("lazy scalar-u8 retry accounting before=%+v after=%+v", before, afterRetry)
	}
	if err := anchor.Close(); err != nil {
		t.Fatal(err)
	}
	if final := fixture.col.collectionSchemaCoordinator().typedGraphPhysical.snapshot(); !final.empty() || final.totalCloses != final.totalOpens {
		t.Fatalf("lazy scalar-u8 retry final close did not reconcile: %+v", final)
	}
}
