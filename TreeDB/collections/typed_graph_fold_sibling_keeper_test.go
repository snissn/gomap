package collections

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
)

type typedGraphServingProcessResources struct {
	segmentFDs, segmentVMAs int
	mappedBytes             int64
}

func typedGraphServingViewAccessForTest(t testing.TB, view *CollectionReadView) (*columnVectorGraphSourceAccess, columnVectorGraphSharedPreparedSearchKey) {
	t.Helper()
	access, err := view.materializerServingSourceAccess()
	if err != nil || access == nil || view.typedGraphOwner == nil || view.typedGraphOwner.overlay == nil || view.typedGraphOwner.overlay.base == nil || view.typedGraphOwner.overlay.base.reader == nil {
		t.Fatalf("serving view access=%p err=%v", access, err)
	}
	capability := view.typedGraphOwner.overlay.base.reader.sharedServingHolder
	if capability == nil || capability.ref == nil || capability.ref.holder == nil || capability.ref.holder.servingSegments != access.pool {
		t.Fatal("serving view capability does not own its source pool")
	}
	return access, capability.ref.key
}

func requireTypedGraphServingPoolReservationsForTest(t testing.TB, ledger *typedGraphPhysicalResourceLedger, pools ...*columnServingSegmentLeaseSet) typedGraphPhysicalResourceAccounting {
	t.Helper()
	var refs, segments, descriptors int
	var inventoryBytes, mappedBytes, fallbackBytes int64
	for _, pool := range pools {
		if pool == nil || pool.ledger != ledger {
			t.Fatal("serving pool does not belong to the shared coordinator ledger")
		}
		refs += pool.reservation.refs
		segments += pool.reservation.segments
		descriptors += pool.reservation.descriptors
		inventoryBytes += pool.reservation.inventoryBytes
		mappedBytes += pool.reservation.mappedBytes
		fallbackBytes += pool.reservation.fallbackBytes
	}
	stats := ledger.snapshot()
	if stats.inventoryHolders != len(pools) || stats.inventoryRefs != refs || stats.inventorySegments != segments || stats.inventoryBytes != inventoryBytes || stats.potentialSegments != segments || stats.potentialDescriptors != descriptors || stats.potentialMappedBytes != mappedBytes || stats.potentialFallbackBytes != fallbackBytes {
		t.Fatalf("coordinator reservation mismatch pools=%d stats=%+v", len(pools), stats)
	}
	if stats.descriptorsInFlight != 0 || stats.descriptorsLive != 0 || stats.unconfirmedDescriptors != 0 || stats.mappedBytesInFlight != 0 || stats.fallbackSegments != 0 || stats.fallbackBackings != 0 || stats.fallbackBytesInFlight != 0 || stats.fallbackBytes != 0 || stats.cleanupBackings != 0 || stats.cleanupQuarantines != 0 {
		t.Fatalf("mapped serving lifecycle has unstable or fallback resources: %+v", stats)
	}
	if stats.mappedBackings <= 0 || stats.mappedBytes <= 0 || uint64(stats.mappedBackings) != stats.totalMaps-stats.totalUnmaps || stats.totalOpens != stats.totalCloses {
		t.Fatalf("mapped serving lifecycle counters do not reconcile: %+v", stats)
	}
	return stats
}

func observeTypedGraphServingProcessResourcesForTest(t testing.TB, pools ...*columnServingSegmentLeaseSet) (typedGraphServingProcessResources, bool) {
	t.Helper()
	if runtime.GOOS != "linux" {
		return typedGraphServingProcessResources{}, false
	}
	wantedDirs := make(map[string]struct{})
	for _, pool := range pools {
		for _, ref := range pool.refs {
			path, err := columnAssetSegmentPath(pool.rootDir, ref)
			if err != nil {
				t.Fatal(err)
			}
			wantedDirs[filepath.Dir(filepath.Clean(path))] = struct{}{}
		}
	}
	fdEntries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("read process descriptors: %v", err)
	}
	var out typedGraphServingProcessResources
	for _, entry := range fdEntries {
		link, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err != nil {
			continue
		}
		link = strings.TrimSuffix(link, " (deleted)")
		if _, ok := wantedDirs[filepath.Dir(filepath.Clean(link))]; ok {
			out.segmentFDs++
		}
	}
	maps, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		t.Fatalf("read process maps: %v", err)
	}
	for _, line := range bytes.Split(maps, []byte{'\n'}) {
		fields := strings.Fields(string(line))
		if len(fields) < 6 {
			continue
		}
		path := strings.TrimSuffix(strings.Join(fields[5:], " "), " (deleted)")
		if _, ok := wantedDirs[filepath.Dir(filepath.Clean(path))]; !ok {
			continue
		}
		bounds := strings.SplitN(fields[0], "-", 2)
		if len(bounds) != 2 {
			t.Fatalf("invalid process map bounds %q", fields[0])
		}
		start, startErr := strconv.ParseUint(bounds[0], 16, 64)
		end, endErr := strconv.ParseUint(bounds[1], 16, 64)
		if startErr != nil || endErr != nil || end < start {
			t.Fatalf("invalid process map bounds %q: %v %v", fields[0], startErr, endErr)
		}
		out.segmentVMAs++
		out.mappedBytes += int64(end - start)
	}
	return out, true
}

func typedGraphServingProcessBaselineForTest(t testing.TB, stats typedGraphPhysicalResourceAccounting, pools ...*columnServingSegmentLeaseSet) (typedGraphServingProcessResources, bool) {
	t.Helper()
	observed, ok := observeTypedGraphServingProcessResourcesForTest(t, pools...)
	if !ok {
		t.Log("process segment FD/map observation is unavailable off Linux")
		return typedGraphServingProcessResources{}, false
	}
	if observed.segmentFDs < stats.descriptorsLive || observed.mappedBytes < stats.mappedBytes || (stats.mappedBackings > 0 && observed.segmentVMAs == 0) {
		t.Fatalf("process resources cannot contain ledger resources: observed=%+v ledger=%+v", observed, stats)
	}
	baseline := observed
	baseline.segmentFDs -= stats.descriptorsLive
	baseline.mappedBytes -= stats.mappedBytes
	// Kernels may split or merge VMAs, so only the total mapped extent is an
	// accounting equality; the observed VMA count remains diagnostic evidence.
	t.Logf("serving process baseline: segment_fds=%d segment_vmas=%d mapped_bytes=%d; holder_fds=%d holder_mapped_bytes=%d backings=%d", baseline.segmentFDs, observed.segmentVMAs, baseline.mappedBytes, stats.descriptorsLive, stats.mappedBytes, stats.mappedBackings)
	return baseline, true
}

func requireTypedGraphServingProcessResourcesForTest(t testing.TB, baseline typedGraphServingProcessResources, available bool, stats typedGraphPhysicalResourceAccounting, pools ...*columnServingSegmentLeaseSet) {
	t.Helper()
	if !available {
		return
	}
	observed, ok := observeTypedGraphServingProcessResourcesForTest(t, pools...)
	if !ok {
		t.Fatal("process segment FD/map observation disappeared")
	}
	wantFDs := baseline.segmentFDs + stats.descriptorsLive
	wantMappedBytes := baseline.mappedBytes + stats.mappedBytes
	if observed.segmentFDs != wantFDs || observed.mappedBytes != wantMappedBytes || (stats.mappedBackings > 0 && observed.segmentVMAs == 0) {
		t.Fatalf("process resource deltas do not reconcile with ledger: observed=%+v baseline=%+v ledger=%+v", observed, baseline, stats)
	}
	t.Logf("serving process resources: segment_fds=%d segment_vmas=%d mapped_bytes=%d holder_backings=%d", observed.segmentFDs, observed.segmentVMAs, observed.mappedBytes, stats.mappedBackings)
}

func TestTypedGraphFoldRetiresSiblingKeepers(t *testing.T) {
	for _, separateManager := range []bool{false, true} {
		name := "same_manager"
		if separateManager {
			name = "separate_manager"
		}
		t.Run(name, func(t *testing.T) {
			requireTypedGraphPublicServingTest(t)
			col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
			index := base.indexName
			if err := base.Close(); err != nil {
				t.Fatal(err)
			}
			opts := typedGraphPublicTestOptions()
			if err := col.EnsureColumnGraphServing(context.Background(), index, opts); err != nil {
				t.Fatal(err)
			}
			manager := col.manager
			if separateManager {
				manager = NewCollectionManager(col.db)
			}
			sibling, err := manager.OpenCollection(col.Name())
			if err != nil {
				t.Fatal(err)
			}
			if err := sibling.EnsureColumnGraphServing(context.Background(), index, opts); err != nil {
				t.Fatal(err)
			}
			defer col.CloseVectorIndexPreparedSearchCache()
			defer sibling.CloseVectorIndexPreparedSearchCache()
			slot := collectionVectorIndexPreparedSearchCacheSlot{family: collectionVectorIndexPreparedSearchFamilyCapturedBase, indexName: index}
			sibling.vectorBufferedSearchMu.Lock()
			keeper := sibling.vectorBufferedSearch[slot].prepared
			sibling.vectorBufferedSearchMu.Unlock()
			query := VectorIndexSearchOptions{IndexName: index, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal}
			var buffer VectorIndexSearchBuffer
			response, held, err := sibling.SearchVectorIndexWithBufferReadView(query, &buffer)
			if err != nil {
				t.Fatal(err)
			}
			heldOpen := true
			defer func() {
				if heldOpen {
					_ = held.Close()
				}
			}()
			before, err := held.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
			if err != nil || len(before.Results) != 1 || held.assetCounters().servingBorrows == 0 {
				t.Fatalf("old fetch=%+v stats=%+v error=%v", before.Results, before.Stats, err)
			}
			oldDocument := bytes.Clone(before.Results[0].Document)
			allBefore, err := held.FetchDocumentsByID(ids, DocumentFetchOptions{})
			if err != nil || len(allBefore.Results) != len(ids) {
				t.Fatalf("old full materializer results=%d stats=%+v err=%v", len(allBefore.Results), allBefore.Stats, err)
			}
			oldAccess, oldKey := typedGraphServingViewAccessForTest(t, held)
			oldPool := oldAccess.pool
			oldRefs := slices.Clone(oldPool.refs)
			ledger := &col.collectionSchemaCoordinator().typedGraphPhysical
			if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
				t.Fatal(err)
			}
			oldPhysical := requireTypedGraphServingPoolReservationsForTest(t, ledger, oldPool)
			processBaseline, processObservation := typedGraphServingProcessBaselineForTest(t, oldPhysical, oldPool)

			changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"sibling-fold-update"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
			if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
				t.Fatal(err)
			}
			state := col.collectionSchemaCoordinator().typedPublication.Load()
			if state == nil || state.servingBase == nil || state.servingBase.refsDigest != oldPool.refsDigest || !slices.Equal(oldRefs, oldPool.refs) {
				t.Fatal("suffix-only publication changed the exact base pool authority")
			}
			var suffixBuffer VectorIndexSearchBuffer
			suffixQuery := query
			suffixQuery.DeclaredScalarFilter = &HybridScalarFilter{IndexName: "path", Value: "new"}
			_, suffixView, err := sibling.SearchVectorIndexWithBufferReadView(suffixQuery, &suffixBuffer)
			if err != nil {
				t.Fatal(err)
			}
			suffixDocs, fetchErr := suffixView.FetchDocumentsByID(ids, DocumentFetchOptions{})
			suffixAccess, suffixKey := typedGraphServingViewAccessForTest(t, suffixView)
			suffixAssets := suffixView.assetCounters()
			closeErr := suffixView.Close()
			if fetchErr != nil || closeErr != nil || suffixAccess != oldAccess || suffixKey != oldKey || suffixAssets.servingBorrows == 0 || suffixDocs.Stats.AssetFileOpens == 0 {
				t.Fatalf("suffix-only holder reuse stats=%+v fetch=%v close=%v same_access=%t same_key=%t", suffixDocs.Stats, fetchErr, closeErr, suffixAccess == oldAccess, suffixKey == oldKey)
			}
			var unlisted int
			for _, ref := range state.servingRefs {
				authorized, err := oldAccess.authorizesMaterializerAsset(oldPool.rootDir, ref)
				if err != nil {
					t.Fatal(err)
				}
				if !authorized {
					unlisted++
				}
			}
			if unlisted == 0 || !slices.Equal(oldRefs, oldPool.refs) {
				t.Fatal("suffix refs widened the serving pool authority")
			}
			oldPhysical = requireTypedGraphServingPoolReservationsForTest(t, ledger, oldPool)
			// Replace may leave unrelated append descriptors open. Re-establish the
			// namespace baseline only at this stable publication boundary.
			processBaseline, processObservation = typedGraphServingProcessBaselineForTest(t, oldPhysical, oldPool)

			if err := col.FoldColumnGraphServing(context.Background(), index); err != nil {
				t.Fatal(err)
			}
			sibling.vectorBufferedSearchMu.Lock()
			entry := sibling.vectorBufferedSearch[slot]
			sibling.vectorBufferedSearchMu.Unlock()
			if entry != nil {
				t.Fatal("idle sibling retained its captured-base keeper after fold")
			}
			keeper.mu.RLock()
			closed := keeper.closed && keeper.capturedBase == nil
			keeper.mu.RUnlock()
			if !closed {
				t.Fatal("detached sibling keeper still owns captured resources")
			}
			col.vectorBufferedSearchMu.Lock()
			foldedEntry := col.vectorBufferedSearch[slot]
			col.vectorBufferedSearchMu.Unlock()
			if foldedEntry == nil || foldedEntry.prepared == nil || foldedEntry.prepared.capturedBase == nil {
				t.Fatal("fold did not install the current captured-base keeper")
			}
			newRef := foldedEntry.prepared.capturedBase.holderRef()
			if newRef == nil || newRef.holder == nil || newRef.holder.servingSegments == nil {
				t.Fatal("folded keeper does not own a serving pool")
			}
			newPool, foldedKey := newRef.holder.servingSegments, newRef.key
			overlap := requireTypedGraphServingPoolReservationsForTest(t, ledger, oldPool, newPool)
			if overlap.mappedBytes <= oldPhysical.mappedBytes {
				t.Fatalf("folded holder did not add live mappings: old=%+v overlap=%+v", oldPhysical, overlap)
			}
			// Fold writes and builds the next holder as one operation. Calibrate its
			// unrelated descriptors only after both generations reach this stable
			// point; subsequent holder releases must match exact ledger deltas.
			processBaseline, processObservation = typedGraphServingProcessBaselineForTest(t, overlap, oldPool, newPool)
			var foldedBuffer VectorIndexSearchBuffer
			folded, foldedView, err := col.SearchVectorIndexWithBufferReadView(suffixQuery, &foldedBuffer)
			if err != nil {
				t.Fatal(err)
			}
			foldedDocs, fetchErr := foldedView.FetchDocumentsByID(ids, DocumentFetchOptions{})
			newAccess, newKey := typedGraphServingViewAccessForTest(t, foldedView)
			foldedAssets := foldedView.assetCounters()
			closeErr = foldedView.Close()
			if fetchErr != nil || closeErr != nil || len(folded.Results) != 1 || len(foldedDocs.Results) != len(ids) || newAccess == oldAccess || newAccess.pool != newPool || newKey != foldedKey || newKey == oldKey || foldedAssets.servingBorrows == 0 {
				t.Fatalf("folded holder identity/results=%d/%d stats=%+v fetch=%v close=%v", len(folded.Results), len(foldedDocs.Results), foldedDocs.Stats, fetchErr, closeErr)
			}
			var oldOnly, newOnly bool
			for _, ref := range oldPool.refs {
				authorized, err := newAccess.authorizesMaterializerAsset(newPool.rootDir, ref)
				if err != nil {
					t.Fatal(err)
				}
				oldOnly = oldOnly || !authorized
			}
			for _, ref := range newPool.refs {
				authorized, err := oldAccess.authorizesMaterializerAsset(oldPool.rootDir, ref)
				if err != nil {
					t.Fatal(err)
				}
				newOnly = newOnly || !authorized
			}
			if !oldOnly || !newOnly {
				t.Fatal("old and folded holders did not retain disjoint exact-ref authority")
			}
			requireTypedGraphServingProcessResourcesForTest(t, processBaseline, processObservation, overlap, oldPool, newPool)
			after, err := held.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
			if err != nil || len(after.Results) != 1 || !bytes.Equal(after.Results[0].Document, oldDocument) {
				t.Fatalf("independent old reader changed: %+v error=%v", after.Results, err)
			}
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			heldOpen = false
			oldPool.mu.Lock()
			oldClosed := oldPool.closed && oldPool.reservationReleased
			oldPool.mu.Unlock()
			if !oldClosed {
				t.Fatal("old pool survived its final held-view release")
			}
			newPhysical := requireTypedGraphServingPoolReservationsForTest(t, ledger, newPool)
			requireTypedGraphServingProcessResourcesForTest(t, processBaseline, processObservation, newPhysical, oldPool, newPool)
			if err := sibling.EnsureColumnGraphServing(context.Background(), index, opts); err != nil {
				t.Fatalf("rewarm sibling: %v", err)
			}
			sibling.vectorBufferedSearchMu.Lock()
			currentEntry := sibling.vectorBufferedSearch[slot]
			sibling.vectorBufferedSearchMu.Unlock()
			col.retireTypedGraphCapturedBaseKeepers(index)
			sibling.vectorBufferedSearchMu.Lock()
			preserved := sibling.vectorBufferedSearch[slot]
			sibling.vectorBufferedSearchMu.Unlock()
			currentClosed := true
			if currentEntry != nil && currentEntry.prepared != nil {
				currentEntry.prepared.mu.RLock()
				currentClosed = currentEntry.prepared.closed
				currentEntry.prepared.mu.RUnlock()
			}
			if currentEntry == nil || preserved != currentEntry || currentClosed {
				t.Fatal("sibling keeper for current folded base was retired")
			}
			query.DeclaredScalarFilter = &HybridScalarFilter{IndexName: "path", Value: "new"}
			var currentBuffer VectorIndexSearchBuffer
			current, view, err := sibling.SearchVectorIndexWithBufferReadView(query, &currentBuffer)
			if err != nil {
				t.Fatal(err)
			}
			documents, err := view.FetchDocumentsForVectorIndexSearchResults(current.Results, DocumentFetchOptions{})
			if err != nil || len(documents.Results) != 1 || !bytes.Contains(documents.Results[0].Document, []byte("sibling-fold-update")) {
				t.Fatalf("current sibling result=%+v error=%v", documents.Results, err)
			}
			siblingAccess, siblingKey := typedGraphServingViewAccessForTest(t, view)
			if siblingKey != newKey || siblingAccess.pool == newPool {
				t.Fatal("current sibling did not own a distinct holder for the folded exact key")
			}
			if err := view.Close(); err != nil {
				t.Fatal(err)
			}
			currentPhysical := requireTypedGraphServingPoolReservationsForTest(t, ledger, newPool, siblingAccess.pool)
			requireTypedGraphServingProcessResourcesForTest(t, processBaseline, processObservation, currentPhysical, oldPool, newPool, siblingAccess.pool)
			if err := sibling.CloseVectorIndexPreparedSearchCache(); err != nil {
				t.Fatal(err)
			}
			if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
				t.Fatal(err)
			}
			requireColumnServingLedgerEmpty(t, ledger)
			requireTypedGraphServingProcessResourcesForTest(t, processBaseline, processObservation, ledger.snapshot(), oldPool, newPool, siblingAccess.pool)
		})
	}
}
