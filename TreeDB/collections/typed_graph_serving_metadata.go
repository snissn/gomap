package collections

import (
	"context"
	"reflect"
	"slices"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Snapshot-free metadata belongs to installed publication state, not a query
// cache. Base metadata is immutable and shared across same-base suffix states.
type typedGraphServingBaseMetadata struct {
	graph            columnVectorGraphManifestSnapshot
	view             columnPhysicalScanSnapshotView
	materializerView columnPhysicalScanSnapshotView
	refs             []ColumnAssetRef
	recordCount      int
	bytes            int64
	preparedKey      string
}

func (b *typedGraphServingBaseMetadata) openPhysicalReader(c *Collection, snap *backenddb.Snapshot, opts columnVectorGraphPhysicalRowReaderOptions) (*columnVectorGraphPhysicalRowReader, error) {
	if b == nil || b.view.Catalog == nil || len(b.view.Catalog.meta.VectorIndexes) != 1 || columnVectorGraphSharedPreparedEligible(b.graph, b.view) != (b.preparedKey != "") {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	view := b.view
	view.snapshot = snap
	return c.openColumnVectorGraphPhysicalRowReaderWithBoundKey(snap, view.Catalog.meta.VectorIndexes[0], b.graph, view, opts, b.preparedKey)
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
			if err := validateTypedGraphColdManifestBudget(ctx, snap, root, cold); err != nil {
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
	materializer, err := prepareTypedGraphMaterializerMetadata(view, cold)
	if err != nil {
		return nil, err
	}
	refs, err := typedGraphOwnerRefs(view.graphOwnerRecords, view.Config.ActiveManifest.Generation, view.AssetNamespace, graph, view.VectorIndexState)
	if err != nil {
		return nil, err
	}
	metadata := &typedGraphServingBaseMetadata{graph: graph, view: view, materializerView: materializer, refs: refs, recordCount: len(view.graphOwnerRecords)}
	if columnVectorGraphSharedPreparedEligible(graph, view) {
		if len(view.Catalog.meta.VectorIndexes) != 1 {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		metadata.preparedKey, err = columnVectorGraphSharedPreparedSearchCacheKey(view.Catalog.meta.Name, view.AssetNamespace, view.Catalog.meta.VectorIndexes[0], graph, view.VectorIndexState)
		if err != nil {
			return nil, err
		}
	}
	metadata.bytes, err = typedGraphServingMetadataBytes(metadata, cold.DecodedTermBytes)
	if err != nil {
		return nil, err
	}
	metadata.view.snapshot = nil
	metadata.view.graphOwnerRecords = nil
	metadata.view.CommitSeq, metadata.view.SystemRoot = 0, 0
	return metadata, nil
}

// Full materializer metadata is decoded while the real captured records are
// retained. Fold prepares this before publishing roots, so no root is required.
func prepareTypedGraphMaterializerMetadata(view columnPhysicalScanSnapshotView, cold typedGraphColdLimits) (columnPhysicalScanSnapshotView, error) {
	if view.Catalog == nil || view.Catalog.meta.Options.ColumnStore == nil {
		return columnPhysicalScanSnapshotView{}, ErrVectorIndexSnapshotMismatch
	}
	cfg := *view.Catalog.meta.Options.ColumnStore
	if !cfg.Enabled || cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil || cfg.AssetManager == nil ||
		!columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		return columnPhysicalScanSnapshotView{}, ErrVectorIndexSnapshotMismatch
	}
	records := view.graphOwnerRecords
	// Include the synthetic identity entry and its owned bytes before allocating
	// or sorting the adapter. Payload records remain borrowed only during decode.
	if cold.ManifestRecords <= 0 || len(records) >= cold.ManifestRecords || columnManifestRecordsBytes(records) > cold.ManifestBytes {
		return columnPhysicalScanSnapshotView{}, errTypedGraphOverlayFoldNeeded
	}
	identityBytes := int64(len(columnManifestIdentityRecordKey) + columnManifestIdentityRecordSize)
	if cold.DecodedTermBytes < identityBytes || int64(len(records)+1) > (cold.DecodedTermBytes-identityBytes)/int64(reflect.TypeFor[systemTargetEntry]().Size()) {
		return columnPhysicalScanSnapshotView{}, errTypedGraphOwnerBudget
	}
	iter := columnManifestRootRecordIteratorOwned(encodeColumnManifestIdentityRecordArray(*cfg.ActiveManifest), records)
	defer func() { _ = iter.Close() }()
	iter.Seek(columnManifestHeaderRecordKeyBytes)
	rowConfig := columnStoreRowAssetConfig(cfg)
	manifest, refs, typedRefs, graphRefs, mutations, count, err := decodeColumnManifestSnapshotViewForScanFromIterator(iter, rowConfig, *cfg.ActiveManifest, view.Catalog.meta.Name, columnManifestScanNoSidecars(), true, view.Catalog.meta.VectorIndexes)
	if err != nil {
		return columnPhysicalScanSnapshotView{}, err
	}
	return columnPhysicalScanSnapshotView{
		CollectionName: view.Catalog.meta.Name, Catalog: view.Catalog,
		Config: rowConfig, FullConfig: cfg, ColumnStoreEnabled: true,
		ColumnAssetRootDir: view.ColumnAssetRootDir, AssetNamespace: cfg.AssetManager.Namespace,
		AssetRefs: refs, TypedColumnPartRefs: typedRefs, GraphAssetRefs: graphRefs,
		SegmentOwnership: manifest.SegmentOwnership, AggregateMetadata: manifest.AggregateMetadata, DictionaryCodes: manifest.DictionaryCodes, Int64Values: manifest.Int64Values,
		MutationParts: mutations, ManifestCatalogBytes: manifest.ManifestBytes,
		Diagnostics: columnPhysicalScanDiagnostics{ManifestGeneration: cfg.ActiveManifest.Generation, ActiveManifestChecksum: cfg.ActiveManifest.Checksum, RecoveryManifestGeneration: cfg.RecoveryAuthoritativeManifest.Generation, RecoveryManifestChecksum: cfg.RecoveryAuthoritativeManifest.Checksum, AppliedCommandLSN: cfg.RecoveryAuthoritativeAppliedCommandLSN, ManifestRecords: count, AssetRefs: len(refs), MutationParts: mutations},
	}, nil
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
	if !add(len(b.preparedKey), 1) {
		return 0, errTypedGraphOwnerBudget
	}
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
	// The full view adds owned ref/SortKey slices and a row-asset config copy.
	// FullConfig borrows the already-retained catalog. Conservatively charge row
	// config strings and pointer fields even when they also borrow catalog data.
	m := b.materializerView
	for _, term := range []struct {
		count int
		size  uintptr
	}{
		{cap(m.AssetRefs), reflect.TypeFor[columnManifestAssetRefForScan]().Size()},
		{cap(m.TypedColumnPartRefs), reflect.TypeFor[columnManifestAssetRefForScan]().Size()},
		{cap(m.GraphAssetRefs), reflect.TypeFor[ColumnAssetRef]().Size()},
		{cap(m.Config.Columns), reflect.TypeFor[ColumnStoreColumn]().Size()},
		{cap(m.Config.SortKey), reflect.TypeFor[ColumnSortKey]().Size()},
		{cap(m.Config.AggregateMetadata), reflect.TypeFor[ColumnAggregateMetadata]().Size()},
	} {
		if !add(term.count, term.size) {
			return 0, errTypedGraphOwnerBudget
		}
	}
	addStrings := func(values ...string) bool {
		for _, value := range values {
			if !add(len(value), 1) {
				return false
			}
		}
		return true
	}
	addSortKey := func(keys []ColumnSortKey) bool {
		for _, key := range keys {
			if !addStrings(key.Column, string(key.Direction)) {
				return false
			}
		}
		return true
	}
	for _, refs := range [][]columnManifestAssetRefForScan{m.AssetRefs, m.TypedColumnPartRefs} {
		for _, ref := range refs {
			if !add(cap(ref.SortKey), reflect.TypeFor[ColumnSortKey]().Size()) || !addSortKey(ref.SortKey) || !addStrings(string(ref.Ref.Kind), ref.Ref.Namespace, string(ref.Reason), string(ref.Role)) {
				return 0, errTypedGraphOwnerBudget
			}
		}
	}
	for _, ref := range m.GraphAssetRefs {
		if !addStrings(string(ref.Kind), ref.Namespace) {
			return 0, errTypedGraphOwnerBudget
		}
	}
	cfg := m.Config
	if !addSortKey(cfg.SortKey) || !addStrings(m.CollectionName, m.ColumnAssetRootDir, m.AssetNamespace, string(cfg.RetainedPayload), string(cfg.RetainedPayloadEncoding), string(cfg.Reconstruction), string(cfg.ProfileSupport), string(cfg.TypedColumnCompression), string(cfg.TypedColumnSectionCompression), string(cfg.ControlRootStoragePolicy)) {
		return 0, errTypedGraphOwnerBudget
	}
	for _, column := range cfg.Columns {
		if !addStrings(column.Name, column.Path, string(column.ValueType), string(column.Owner), string(column.AdjacencyLayout), string(column.FixedWidthEncoding)) {
			return 0, errTypedGraphOwnerBudget
		}
	}
	for _, aggregate := range cfg.AggregateMetadata {
		if !add(cap(aggregate.Predicates), reflect.TypeFor[ColumnPhysicalQueryPredicate]().Size()) || !addStrings(aggregate.Name, aggregate.Column, aggregate.GroupColumn, string(aggregate.Kind)) {
			return 0, errTypedGraphOwnerBudget
		}
		for _, predicate := range aggregate.Predicates {
			if !add(cap(predicate.Values), reflect.TypeFor[string]().Size()) || !addStrings(predicate.Column, string(predicate.Kind), predicate.Value) || !addStrings(predicate.Values...) {
				return 0, errTypedGraphOwnerBudget
			}
		}
	}
	if cfg.AssetManager != nil && (!add(1, reflect.TypeFor[ColumnAssetManagerConfig]().Size()) || !addStrings(string(cfg.AssetManager.Kind), cfg.AssetManager.Namespace)) {
		return 0, errTypedGraphOwnerBudget
	}
	if cfg.ManifestRoot != nil && (!add(1, reflect.TypeFor[ColumnManifestRootDescriptor]().Size()) || !addStrings(cfg.ManifestRoot.Name, string(cfg.ManifestRoot.StoragePolicy))) {
		return 0, errTypedGraphOwnerBudget
	}
	for _, identity := range []*ColumnManifestIdentity{cfg.ActiveManifest, cfg.RecoveryAuthoritativeManifest} {
		if identity != nil && (!add(1, reflect.TypeFor[ColumnManifestIdentity]().Size()) || !addStrings(identity.Format)) {
			return 0, errTypedGraphOwnerBudget
		}
	}
	if cfg.Locator != nil && (!add(1, reflect.TypeFor[ColumnLocatorConfig]().Size()) || !addStrings(string(cfg.Locator.Strategy))) {
		return 0, errTypedGraphOwnerBudget
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
