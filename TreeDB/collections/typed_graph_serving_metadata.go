package collections

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"

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
	refsDigest       [32]byte
	// Empty graphs do not have an exact shared holder. Keep their selected
	// scalar-u8 payload validation with the immutable base metadata instead of
	// pretending a zero-row code plane is already ready from its declaration.
	zeroRowLegacyScalarU8Validation *columnVectorGraphLegacyScalarU8ZeroRowValidationCache
}

func (b *typedGraphServingBaseMetadata) openPhysicalReader(c *Collection, snap *backenddb.Snapshot, opts columnVectorGraphPhysicalRowReaderOptions) (*columnVectorGraphPhysicalRowReader, error) {
	if b == nil || b.view.Catalog == nil || len(b.view.Catalog.meta.VectorIndexes) != 1 || columnVectorGraphSharedPreparedEligible(b.graph, b.view) != (b.preparedKey != "") {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	view := b.view
	view.snapshot = snap
	reader, err := c.openColumnVectorGraphPhysicalRowReaderWithBoundKey(snap, view.Catalog.meta.VectorIndexes[0], b.graph, view, opts, b.preparedKey)
	if err != nil {
		return nil, err
	}
	if reader.RowCount() == 0 {
		reader.zeroRowLegacyScalarU8Validation = b.zeroRowLegacyScalarU8Validation
	}
	return reader, nil
}

// openServingPhysicalReaderWithOwnedPin is the serving-only reader boundary.
// It starts from installed immutable metadata, consumes a caller pin acquired
// with the caller snapshot, and attaches only the typed serving capability.
// The holder cache-miss builder independently rebinds current metadata before
// constructing sources; this request snapshot is never captured by the build.
func (b *typedGraphServingBaseMetadata) openServingPhysicalReaderWithOwnedPin(ctx context.Context, c *Collection, snap *backenddb.Snapshot, pin *ColumnAssetLifecyclePinSet, limits typedGraphPhysicalResourceLimits, opts columnVectorGraphPhysicalRowReaderOptions) (*columnVectorGraphPhysicalRowReader, error) {
	if b == nil || c == nil || snap == nil || pin == nil || b.view.Catalog == nil || len(b.view.Catalog.meta.VectorIndexes) != 1 || b.preparedKey == "" || !columnVectorGraphSharedPreparedEligible(b.graph, b.view) {
		return nil, errors.Join(ErrVectorIndexSnapshotMismatch, pin.Close())
	}
	def := b.view.Catalog.meta.VectorIndexes[0]
	reader := &columnVectorGraphPhysicalRowReader{
		def:                        def,
		graph:                      b.graph,
		catalog:                    b.view.Catalog,
		quantizedAssetStatus:       make(map[string]columnVectorGraphQuantizedAssetLoadStatus),
		useResourceQuantizedAssets: opts.UseResourceQuantizedAssets,
		skipQuantizedAssets:        opts.SkipQuantizedAssets,
	}
	capability, err := b.acquireServingHolderWithOwnedPin(ctx, c, pin, limits)
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	if err := reader.attachServingPreparedSearch(capability); err != nil {
		return nil, errors.Join(err, capability.Close(), reader.Close())
	}
	if !reader.skipQuantizedAssets {
		c.prepareColumnVectorGraphQuantizedAssetsForReader(reader, b.view)
	}
	return reader, nil
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
		if err := next.prepareServingRefs(records, catalog.meta, cold); err != nil {
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
	refsDigest, err := digestTypedGraphServingBaseRefs(refs)
	if err != nil {
		return nil, err
	}
	metadata := &typedGraphServingBaseMetadata{graph: graph, view: view, materializerView: materializer, refs: refs, refsDigest: refsDigest, recordCount: len(view.graphOwnerRecords)}
	if graph.RowCount == 0 && len(view.Catalog.meta.VectorIndexes) == 1 {
		metadata.zeroRowLegacyScalarU8Validation = newColumnVectorGraphLegacyScalarU8ZeroRowValidationCache(view.Catalog.meta.VectorIndexes[0], view.VectorIndexState)
	}
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

func digestTypedGraphServingBaseRefs(refs []ColumnAssetRef) ([32]byte, error) {
	if len(refs) == 0 {
		return [32]byte{}, nil
	}
	h := sha256.New()
	var encoded [8]byte
	writeUint64 := func(value uint64) {
		binary.BigEndian.PutUint64(encoded[:], value)
		_, _ = h.Write(encoded[:])
	}
	writeString := func(value string) {
		writeUint64(uint64(len(value)))
		_, _ = h.Write([]byte(value))
	}
	writeUint64(uint64(len(refs)))
	for i, ref := range refs {
		if err := validateColumnAssetRefForPlan(ref); err != nil {
			return [32]byte{}, fmt.Errorf("collections: serving base digest ref[%d]: %w", i, err)
		}
		if i > 0 && compareColumnAssetRefs(refs[i-1], ref) >= 0 {
			return [32]byte{}, errors.New("collections: serving base digest requires strictly sorted unique refs")
		}
		writeString(string(ref.Kind))
		writeString(ref.Namespace)
		writeUint64(ref.Generation)
		writeUint64(ref.PartID)
		writeUint64(uint64(ref.FileID))
		writeUint64(uint64(ref.Offset))
		writeUint64(uint64(ref.Length))
		writeUint64(uint64(ref.Checksum))
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func (b *typedGraphServingBaseMetadata) servingPreparedSearchKey(c *Collection) (columnVectorGraphSharedPreparedSearchKey, error) {
	if b == nil || c == nil || c.db == nil || b.preparedKey == "" || len(b.refs) == 0 || b.view.Catalog == nil || b.view.Catalog.meta.Name != c.collectionName() || b.view.AssetNamespace == "" {
		return columnVectorGraphSharedPreparedSearchKey{}, ErrVectorIndexSnapshotMismatch
	}
	root, err := normalizeColumnServingRoot(c.db.ColumnAssetRootDir())
	if err != nil {
		return columnVectorGraphSharedPreparedSearchKey{}, err
	}
	key := columnVectorGraphSharedPreparedSearchKey{
		family:     columnVectorGraphSharedPreparedSearchKeyServing,
		db:         c.db,
		assetRoot:  root,
		collection: b.view.Catalog.meta.Name,
		namespace:  b.view.AssetNamespace,
		logical:    b.preparedKey,
		refsDigest: b.refsDigest,
		refsCount:  len(b.refs),
	}
	if !key.valid() {
		return columnVectorGraphSharedPreparedSearchKey{}, ErrVectorIndexSnapshotMismatch
	}
	return key, nil
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
	// Preserve the full reconstruction loader's generation-wide validation once,
	// before any reader may use the immutable sorted refs without request maps.
	if _, err := typedColumnPartRefsByGenerationFromManifestRecords(records, cfg.AssetManager.Namespace); err != nil {
		return columnPhysicalScanSnapshotView{}, err
	}
	for _, parts := range [][]columnManifestAssetRefForScan{refs, typedRefs} {
		for i := 1; i < len(parts); i++ {
			if compareMaterializerPartRefs(parts[i-1], parts[i]) >= 0 {
				return columnPhysicalScanSnapshotView{}, ErrVectorIndexSnapshotMismatch
			}
		}
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
		{cap(v.SegmentOwnership), reflect.TypeFor[columnManifestSegmentOwnership]().Size()},
		{cap(v.VectorIndexState.Assets), reflect.TypeFor[columnVectorIndexStateAssetSnapshot]().Size()},
		{cap(b.graph.AdjacencyLayerSources), reflect.TypeFor[columnVectorGraphAdjacencySourceSnapshot]().Size()},
	} {
		if !add(term.count, term.size) {
			return 0, errTypedGraphOwnerBudget
		}
	}
	if cache := b.zeroRowLegacyScalarU8Validation; cache != nil {
		descriptors, entries := cache.retainedCapacities()
		for _, term := range []struct {
			count int
			size  uintptr
		}{
			{1, reflect.TypeFor[columnVectorGraphLegacyScalarU8ZeroRowValidationCache]().Size()},
			{1, reflect.TypeFor[sync.Cond]().Size()},
			{descriptors, reflect.TypeFor[columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor]().Size()},
			{entries, reflect.TypeFor[columnVectorGraphLegacyScalarU8ZeroRowValidationEntry]().Size()},
			{descriptors, reflect.TypeFor[ScalarU8CalibrationConfig]().Size()},
		} {
			if !add(term.count, term.size) {
				return 0, errTypedGraphOwnerBudget
			}
		}
		descriptorPayloadBytes, err := typedGraphLegacyScalarU8DescriptorPayloadBytes(cache.descriptors, limit-n)
		if err != nil {
			return 0, err
		}
		n += descriptorPayloadBytes
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
	for _, record := range v.SegmentOwnership {
		if !add(len(record.Ref.Kind), 1) || !add(len(record.Ref.Namespace), 1) {
			return 0, errTypedGraphOwnerBudget
		}
	}
	materializerBytes, err := typedGraphMaterializerMetadataBytes(b.materializerView, limit-n)
	if err != nil {
		return 0, err
	}
	return n + materializerBytes, nil
}

// The view header is already charged as part of its owning base or publication
// state. This accounts only the backing owned by a full materializer view.
func typedGraphMaterializerMetadataBytes(m columnPhysicalScanSnapshotView, limit int64) (int64, error) {
	var n int64
	add := func(count int, size uintptr) bool {
		if count < 0 || size == 0 || n > limit || int64(count) > (limit-n)/int64(size) {
			return false
		}
		n += int64(count) * int64(size)
		return true
	}
	// The full view adds owned ref/SortKey slices and a row-asset config copy.
	// FullConfig borrows the already-retained catalog. Conservatively charge row
	// config strings and pointer fields even when they also borrow catalog data.
	for _, term := range []struct {
		count int
		size  uintptr
	}{
		{cap(m.AssetRefs), reflect.TypeFor[columnManifestAssetRefForScan]().Size()},
		{cap(m.TypedColumnPartRefs), reflect.TypeFor[columnManifestAssetRefForScan]().Size()},
		{cap(m.GraphAssetRefs), reflect.TypeFor[ColumnAssetRef]().Size()},
		{cap(m.SegmentOwnership), reflect.TypeFor[columnManifestSegmentOwnership]().Size()},
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
	for _, record := range m.SegmentOwnership {
		if !addStrings(string(record.Ref.Kind), record.Ref.Namespace) {
			return 0, errTypedGraphOwnerBudget
		}
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

func (s *typedGraphPublicationState) prepareServingRefs(records []columnManifestRecord, meta CollectionMeta, cold typedGraphColdLimits) error {
	if s.servingBase == nil {
		s.servingRefs = nil
		s.servingBaseRefsDigest = [32]byte{}
		s.servingBaseRefsCount = 0
		s.servingPinBytes = 0
		s.servingOwnerRefBytes = 0
		return nil
	}
	if len(records) > cold.ManifestRecords || columnManifestRecordsBytes(records) > cold.ManifestBytes {
		return errTypedGraphOverlayFoldNeeded
	}
	b := s.servingBase
	if meta.Options.ColumnStore == nil || meta.Options.ColumnStore.ActiveManifest == nil {
		return ErrVectorIndexSnapshotMismatch
	}
	refs, err := typedGraphOwnerRefs(records, meta.Options.ColumnStore.ActiveManifest.Generation, b.view.AssetNamespace, b.graph, b.view.VectorIndexState)
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
	pinBytes, ownerRefBytes := int64(0), n
	for _, ref := range refs {
		pinBytes = addColumnAssetReachabilityBytes(pinBytes, positiveColumnAssetReachabilityLength(ref.Length))
		ownerRefBytes = addColumnAssetReachabilityBytes(ownerRefBytes, int64(len(ref.Namespace)))
	}
	materializer := b.materializerView
	if !collectionMetaValuesEqual(b.view.Catalog.meta, meta) {
		materializer, err = prepareTypedGraphMaterializerMetadata(columnPhysicalScanSnapshotView{
			Catalog: &collectionCatalog{meta: meta}, ColumnAssetRootDir: b.view.ColumnAssetRootDir, graphOwnerRecords: records,
		}, cold)
		if err != nil {
			return err
		}
		owned, err := typedGraphMaterializerMetadataBytes(materializer, cold.DecodedTermBytes-b.bytes-n)
		if err != nil {
			return err
		}
		n += owned
	}
	// Installed state.catalog is the current authority. Bind the actual catalog,
	// full config and snapshot at read admission; retain no temporary catalog or
	// pager roots that would need a separate relocation protocol.
	materializer.Catalog, materializer.snapshot = nil, nil
	materializer.FullConfig = ColumnStoreConfig{}
	materializer.CommitSeq, materializer.SystemRoot = 0, 0
	materializer.Diagnostics.ManifestRoot = 0
	s.servingMaterializer = materializer
	s.servingRefs = refs
	s.servingBaseRefsDigest = b.refsDigest
	s.servingBaseRefsCount = len(b.refs)
	s.servingPinBytes = pinBytes
	s.servingOwnerRefBytes = ownerRefBytes
	s.servingMetadataBytes = n + b.bytes
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
	meta, err := columnPublishUpdatedMeta(p.before.catalog.meta, plan)
	if err != nil {
		return err
	}
	return p.next.prepareServingRefs(plan.RootDelta.Records, meta, policy.options.Owners.Cold)
}
