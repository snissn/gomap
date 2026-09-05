package collections

import (
	"errors"
	"reflect"
)

// No snapshot, catalog or suffix is retained here. Ref owns immutable prepared
// mappings; pin protects the complete captured typed closure. Both are released
// by the existing collection cache lifecycle, outside its mutex.
type typedGraphCapturedBaseResources struct {
	ref             *columnVectorGraphSharedPreparedSearchRef
	pin             *ColumnAssetLifecyclePinSet
	accounting      *typedGraphReadOwnerAccounting
	assetBytes      int64
	descriptorBytes int64
	backingBytes    int64
}

func (r *typedGraphCapturedBaseResources) Close() error {
	if r == nil {
		return nil
	}
	err := r.ref.release()
	err = errors.Join(err, r.pin.Close())
	r.ref, r.pin = nil, nil
	if a := r.accounting; a != nil {
		a.Lock()
		a.baseOwners--
		a.baseAssetBytes -= r.assetBytes
		a.baseDescriptorBytes -= r.descriptorBytes
		a.baseBackingBytes -= r.backingBytes
		if a.owners == 0 && a.baseOwners == 0 {
			a.states = nil
			a.limits = typedGraphReadOwnerLimits{}
		}
		a.Unlock()
		r.accounting = nil
	}
	return err
}

func (r *typedGraphCapturedBaseResources) reserve(a *typedGraphReadOwnerAccounting, limits typedGraphReadOwnerLimits) error {
	a.Lock()
	defer a.Unlock()
	if (a.owners != 0 || a.baseOwners != 0) && a.limits != limits {
		return ErrVectorIndexSnapshotMismatch
	}
	remaining := limits.StateBytes - a.stateBytes - a.baseDescriptorBytes - a.baseBackingBytes
	if a.owners+a.baseOwners >= limits.Owners || r.assetBytes > limits.AssetBytes-a.assetBytes-a.baseAssetBytes || r.descriptorBytes > remaining || r.backingBytes > remaining-r.descriptorBytes {
		return errTypedGraphOwnerBudget
	}
	a.limits = limits
	a.baseOwners++
	a.baseAssetBytes += r.assetBytes
	a.baseDescriptorBytes += r.descriptorBytes
	a.baseBackingBytes += r.backingBytes
	r.accounting = a
	return nil
}

// Internal warm-before-owner seam, never called under the storage barrier.
// It is not a pinned-identity handshake: a concurrent base cutover can make the
// next current owner build a different holder. Current authority stays checked
// by openTypedGraphReadOwner; warming cannot authorize stale serving.
func (c *Collection) acquireTypedGraphCapturedBaseCache(index string, limits typedGraphReadOwnerLimits) (*collectionVectorIndexPreparedSearch, error) {
	if err := ValidateIndexName(index); err != nil {
		return nil, err
	}
	if limits.Owners <= 0 || limits.States <= 0 || limits.StateBytes <= 0 || limits.AssetBytes <= 0 || limits.Cold.ManifestRecords <= 0 || limits.Cold.ManifestBytes <= 0 || limits.Cold.AssetBytes <= 0 || limits.Cold.DecodedTermBytes <= 0 {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	prepared, _, _, err := c.acquireCollectionVectorIndexPreparedSearchSlot(VectorIndexSearchOptions{IndexName: index}, collectionVectorIndexPreparedSearchCacheSlot{family: collectionVectorIndexPreparedSearchFamilyCapturedBase, indexName: index}, func() (*collectionVectorIndexPreparedSearch, VectorIndexSearchResponse, error) {
		p, err := c.openTypedGraphCapturedBaseCache(index, limits)
		return p, VectorIndexSearchResponse{}, err
	})
	if err != nil {
		return nil, err
	}
	prepared.mu.RLock()
	defer prepared.mu.RUnlock()
	if prepared.closed || prepared.capturedBase == nil {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	a := prepared.capturedBase.accounting
	a.Lock()
	match := a.limits == limits
	a.Unlock()
	if !match {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	return prepared, nil
}

func (c *Collection) openTypedGraphCapturedBaseCache(index string, limits typedGraphReadOwnerLimits) (prepared *collectionVectorIndexPreparedSearch, err error) {
	err = WithVectorPartitionStorageBarrierV1(c.db.Dir(), func() (err error) {
		snap := c.db.AcquireSnapshot()
		if snap == nil {
			return ErrVectorIndexSnapshotMismatch
		}
		defer func() { err = errors.Join(err, snap.Close()) }()
		catalog, err := loadCollectionCatalog(snap, c.collectionName())
		if err != nil {
			return err
		}
		if catalog == nil || catalog.typedGraphBase == nil || len(catalog.meta.VectorIndexes) != 1 || catalog.meta.VectorIndexes[0].Name != index || !typedGraphBaseSchemaMatches(catalog.typedGraphBase.meta, catalog.meta) {
			return ErrVectorIndexSnapshotMismatch
		}
		base := catalog.typedGraphBase
		if err := validateTypedGraphColdManifestBudget(snap, base.roots[collectionColumnManifestRootName(catalog.meta.Name)], limits.Cold); err != nil {
			return err
		}
		graph, view, err := base.readerView(c, snap)
		if err != nil {
			return err
		}
		// Per decoded working term, not a claimed total Go heap bound. Metadata
		// pre-scan and physical extents have separate caps before mapping.
		for _, term := range [][2]int64{{int64(graph.Dimensions), 4}, {1, int64(reflect.TypeFor[DocumentRowRef]().Size())}, {int64(graph.M), 8}} {
			if term[0] <= 0 || term[0] > limits.Cold.DecodedTermBytes/term[1] || graph.RowCount < 0 || int64(graph.RowCount) > limits.Cold.DecodedTermBytes/(term[0]*term[1]) {
				return errTypedGraphOverlayFoldNeeded
			}
		}
		refs, err := typedGraphOwnerRefs(view.graphOwnerRecords, view.Config.ActiveManifest.Generation, view.AssetNamespace, graph, view.VectorIndexState)
		if err != nil {
			return err
		}
		r := &typedGraphCapturedBaseResources{}
		defer func() {
			if err != nil {
				err = errors.Join(err, r.Close())
			}
		}()
		for _, ref := range refs {
			if ref.Length <= 0 || ref.Length > limits.Cold.AssetBytes-r.assetBytes {
				return errTypedGraphOverlayFoldNeeded
			}
			r.assetBytes += ref.Length
			if int64(len(ref.Namespace)) > limits.StateBytes-r.descriptorBytes {
				return errTypedGraphOwnerBudget
			}
			r.descriptorBytes += int64(len(ref.Namespace))
		}
		// Exact retained slice capacity plus conservatively repeated namespace
		// bytes. This is lease metadata, NOT total holder/Go allocator residency.
		fixed := int64(reflect.TypeFor[typedGraphCapturedBaseResources]().Size() + reflect.TypeFor[ColumnAssetLifecyclePinSet]().Size() + reflect.TypeFor[collectionVectorIndexPreparedSearch]().Size())
		refSize := int64(reflect.TypeFor[ColumnAssetRef]().Size())
		if fixed > limits.StateBytes-r.descriptorBytes || int64(cap(refs)) > (limits.StateBytes-r.descriptorBytes-fixed)/refSize {
			return errTypedGraphOwnerBudget
		}
		r.descriptorBytes += fixed + int64(cap(refs))*refSize
		identity, err := columnVectorGraphSharedPreparedSearchCacheKey(catalog.meta.Name, view.AssetNamespace, catalog.meta.VectorIndexes[0], graph, view.VectorIndexState)
		if err != nil {
			return err
		}
		// A cache hit may retain both the original holder key and this ref key.
		if int64(len(identity)) > (limits.StateBytes-r.descriptorBytes)/2 {
			return errTypedGraphOwnerBudget
		}
		r.descriptorBytes += 2 * int64(len(identity))
		r.backingBytes, err = typedGraphCapturedBaseBackingBound(graph.RowCount, len(view.graphOwnerRecords), graph.AdjacencyLayerCount, limits.StateBytes-r.descriptorBytes)
		if err != nil {
			return err
		}
		coord := c.collectionSchemaCoordinator()
		if coord == nil {
			return ErrVectorIndexSnapshotMismatch
		}
		if err := r.reserve(&coord.typedGraphOwners, limits); err != nil {
			return err
		}
		r.pin, err = c.acquireColumnAssetLifecyclePinSetOwned(ColumnAssetLifecyclePinSetOptions{Source: ColumnAssetLifecyclePinSourcePreparedQuery, Owner: "typed_graph_captured_base_cache", Refs: refs})
		if err != nil {
			return err
		}
		view.graphOwnerRecords = nil
		reader, err := c.openColumnVectorGraphPhysicalRowReaderFromView(snap, catalog.meta.VectorIndexes[0], graph, view, columnVectorGraphPhysicalRowReaderOptions{SkipQuantizedAssets: true})
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, reader.Close()) }()
		if reader.sharedPreparedSearch == nil {
			return errColumnVectorGraphSharedPreparedSearchNotEligible
		}
		r.ref = reader.detachSharedPreparedSearch()
		prepared = &collectionVectorIndexPreparedSearch{family: collectionVectorIndexPreparedSearchFamilyCapturedBase, collection: c, indexName: index, commitSeq: snapshotCommitSeq(snap), systemRoot: snapshotSystemRoot(snap), capturedBase: r}
		return nil
	})
	if err != nil && prepared != nil {
		err = errors.Join(err, prepared.Close())
		prepared = nil
	}
	return prepared, err
}

// Bounds retained source structs and explicit Go slice backing before mapping.
// Each used vector generation has a manifest record; source and prepared part
// pointer slices have exact capacities. Pack codec validates 8+2*layers sections
// plus two optional navigation sections. This deliberately excludes manager
// bookkeeping/allocator overhead and temporary decoder/validation maps; it is
// not a total process heap ceiling. Shared holders are charged per keeper.
func typedGraphCapturedBaseBackingBound(rows, records, layers int, limit int64) (int64, error) {
	var total int64
	add := func(count int, size uintptr) bool {
		if count < 0 || size == 0 || int64(count) > (limit-total)/int64(size) {
			return false
		}
		total += int64(count) * int64(size)
		return true
	}
	if limit < 0 || rows < 0 || records < 0 || layers < 0 {
		return 0, errTypedGraphOwnerBudget
	}
	for _, size := range []uintptr{
		reflect.TypeFor[columnVectorGraphSharedPreparedSearch]().Size(),
		reflect.TypeFor[columnVectorGraphTypedColumnVectorSource]().Size(),
		reflect.TypeFor[columnVectorGraphInvNormStateSource]().Size(),
		reflect.TypeFor[columnVectorGraphRowRefStateSource]().Size(),
		reflect.TypeFor[columnVectorGraphDocumentIDStateSource]().Size(),
		reflect.TypeFor[columnVectorGraphAdjacencyDirectSources]().Size(),
		reflect.TypeFor[columnVectorGraphPreparedSearchView]().Size(),
		reflect.TypeFor[columnHNSWSearchPackPreparedView]().Size(),
	} {
		if !add(1, size) {
			return 0, errTypedGraphOwnerBudget
		}
	}
	parts := min(rows, records)
	for _, term := range []struct {
		n    int
		size uintptr
	}{
		{rows, reflect.TypeFor[columnVectorGraphTypedColumnVectorLocation]().Size()},
		{rows, 2 * reflect.TypeFor[uint32]().Size()},
		{parts, reflect.TypeFor[columnVectorGraphTypedColumnVectorPart]().Size() + 2*reflect.TypeFor[*columnVectorGraphTypedColumnVectorPart]().Size()},
		{layers, reflect.TypeFor[columnVectorGraphLayer0AdjacencyDirectSource]().Size() + reflect.TypeFor[*columnVectorGraphLayer0AdjacencyDirectSource]().Size() + reflect.TypeFor[columnHNSWSearchPackPreparedLayer]().Size()},
		{layers, 2 * reflect.TypeFor[columnHNSWSearchPackSection]().Size()},
		{10, reflect.TypeFor[columnHNSWSearchPackSection]().Size()},
	} {
		if !add(term.n, term.size) {
			return 0, errTypedGraphOwnerBudget
		}
	}
	return total, nil
}
