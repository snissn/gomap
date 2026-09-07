package collections

import (
	"context"
	"reflect"
	"slices"
)

// Snapshot-free metadata belongs to installed publication state, not a query
// cache. Base metadata is immutable and shared across same-base suffix states.
type typedGraphServingBaseMetadata struct {
	graph       columnVectorGraphManifestSnapshot
	view        columnPhysicalScanSnapshotView
	refs        []ColumnAssetRef
	recordCount int
	bytes       int64
}

func (c *Collection) prepareTypedGraphServingMetadata(ctx context.Context, cold typedGraphColdLimits) error {
	unlock := c.lockCollectionSchemaWrite()
	defer unlock()
	if err := c.flushCollectionWriteDomainsForSchemaMutation(); err != nil {
		return err
	}
	coord := c.collectionSchemaCoordinator()
	return WithVectorPartitionStorageBarrierWithContextV1(ctx, c.db.Dir(), func() error {
		snap := c.db.AcquireSnapshot()
		if snap == nil {
			return ErrVectorIndexSnapshotMismatch
		}
		defer snap.Close()
		catalog, err := loadCollectionCatalog(snap, c.collectionName())
		if err != nil {
			return err
		}
		state := coord.typedPublication.Load()
		if !state.matches(catalog) {
			return ErrVectorIndexSnapshotMismatch
		}
		if state.servingBase != nil {
			return nil
		}
		base := catalog.typedGraphBase
		if base == nil {
			return ErrVectorIndexSnapshotMismatch
		}
		if len(catalog.meta.VectorIndexes) != 1 || catalog.meta.Options.ColumnStore == nil {
			return ErrHybridSearchUnsupported
		}
		for _, column := range catalog.meta.Options.ColumnStore.Columns {
			if column.ValueType != ColumnStoreValueString && column.ValueType != ColumnStoreValueFloat32Vector {
				return ErrHybridSearchUnsupported
			}
		}
		if err := validateTypedGraphOverlayVectorOwners(*catalog.meta.Options.ColumnStore); err != nil {
			return err
		}
		for _, root := range []uint64{catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name)), base.roots[collectionColumnManifestRootName(catalog.meta.Name)]} {
			if err := validateTypedGraphColdManifestBudget(snap, root, cold); err != nil {
				return err
			}
		}
		graph, view, err := base.readerView(c, snap)
		if err != nil {
			return err
		}
		metadata, err := prepareTypedGraphServingBaseMetadata(graph, view, cold)
		if err != nil {
			return err
		}
		records, err := loadColumnManifestRecordsFromRoot(snap, catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name)))
		if err != nil {
			return err
		}
		next := *state
		next.servingBase = metadata
		if err := next.prepareServingRefs(records, catalog.meta.Options.ColumnStore.ActiveManifest.Generation, cold); err != nil {
			return err
		}
		if !coord.typedPublication.CompareAndSwap(state, &next) {
			return ErrConcurrentMutation
		}
		return nil
	})
}

func prepareTypedGraphServingBaseMetadata(graph columnVectorGraphManifestSnapshot, view columnPhysicalScanSnapshotView, cold typedGraphColdLimits) (*typedGraphServingBaseMetadata, error) {
	if len(view.graphOwnerRecords) > cold.ManifestRecords || columnManifestRecordsBytes(view.graphOwnerRecords) > cold.ManifestBytes {
		return nil, errTypedGraphOverlayFoldNeeded
	}
	refs, err := typedGraphOwnerRefs(view.graphOwnerRecords, view.Config.ActiveManifest.Generation, view.AssetNamespace, graph, view.VectorIndexState)
	if err != nil {
		return nil, err
	}
	metadata := &typedGraphServingBaseMetadata{graph: graph, view: view, refs: refs, recordCount: len(view.graphOwnerRecords)}
	metadata.bytes, err = typedGraphServingMetadataBytes(metadata, cold.DecodedTermBytes)
	if err != nil {
		return nil, err
	}
	metadata.view.snapshot = nil
	metadata.view.graphOwnerRecords = nil
	metadata.view.CommitSeq, metadata.view.SystemRoot = 0, 0
	return metadata, nil
}

func typedGraphServingMetadataBytes(b *typedGraphServingBaseMetadata, limit int64) (int64, error) {
	n := int64(reflect.TypeFor[typedGraphServingBaseMetadata]().Size())
	add := func(count int, size uintptr) bool {
		if count < 0 || size == 0 || n > limit || int64(count) > (limit-n)/int64(size) {
			return false
		}
		n += int64(count) * int64(size)
		return true
	}
	v := b.view
	for _, term := range []struct {
		count int
		size  uintptr
	}{
		{cap(b.refs), reflect.TypeFor[ColumnAssetRef]().Size()},
		{cap(v.AssetRefs), reflect.TypeFor[columnManifestAssetRefForScan]().Size()},
		{cap(v.TypedColumnPartRefs), reflect.TypeFor[columnManifestAssetRefForScan]().Size()},
		{cap(v.AggregateMetadata), reflect.TypeFor[columnManifestAggregateMetadataSnapshot]().Size()},
		{cap(v.DictionaryCodes), reflect.TypeFor[columnManifestDictionaryCodesSnapshot]().Size()},
		{cap(v.Int64Values), reflect.TypeFor[columnManifestInt64ValuesSnapshot]().Size()},
		{cap(v.GraphAssetRefs), reflect.TypeFor[ColumnAssetRef]().Size()},
		{cap(v.VectorIndexState.Assets), reflect.TypeFor[columnVectorIndexStateAssetSnapshot]().Size()},
		{cap(b.graph.AdjacencyLayerSources), reflect.TypeFor[columnVectorGraphAdjacencySourceSnapshot]().Size()},
	} {
		if !add(term.count, term.size) {
			return 0, errTypedGraphOwnerBudget
		}
	}
	// Both decoded views originate in the same bounded manifest. Account their
	// decoded string payload separately from slice backing; namespace strings
	// borrow catalog storage and are conservatively repeated per ref here.
	for _, ref := range b.refs {
		if !add(len(ref.Namespace), 1) {
			return 0, errTypedGraphOwnerBudget
		}
	}
	for _, record := range v.graphOwnerRecords {
		if !add(len(record.key), 2) || !add(len(record.value), 2) {
			return 0, errTypedGraphOwnerBudget
		}
	}
	return n, nil
}

func (s *typedGraphPublicationState) prepareServingRefs(records []columnManifestRecord, generation uint64, cold typedGraphColdLimits) error {
	if s.servingBase == nil {
		return nil
	}
	if len(records) > cold.ManifestRecords || columnManifestRecordsBytes(records) > cold.ManifestBytes {
		return errTypedGraphOverlayFoldNeeded
	}
	b := s.servingBase
	refs, err := typedGraphOwnerRefs(records, generation, b.view.AssetNamespace, b.graph, b.view.VectorIndexState)
	if err != nil {
		return err
	}
	refs = append(refs, b.refs...)
	slices.SortFunc(refs, compareColumnAssetRefs)
	refs = slices.Compact(refs)
	n := int64(cap(refs)) * int64(reflect.TypeFor[ColumnAssetRef]().Size())
	if n < 0 || n > cold.DecodedTermBytes-b.bytes {
		return errTypedGraphOwnerBudget
	}
	s.servingRefs, s.servingMetadataBytes = refs, n+b.bytes
	return nil
}

func (p *typedGraphPublicationCandidate) prepareServingPlan(plan ColumnPublishPlan) error {
	if p == nil || p.next == nil || p.next.servingBase == nil {
		return nil
	}
	policy := p.coord.typedGraphServing.Load()
	if policy == nil {
		return ErrVectorIndexSnapshotMismatch
	}
	return p.next.prepareServingRefs(plan.RootDelta.Records, plan.UpdatedActiveManifest.Generation, policy.options.Owners.Cold)
}
