package collections

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Folding cannot release caller-held owners. Keep this distinct from suffix
// debt, so future lifecycle admission cannot enter a futile fold/retry loop.
var errTypedGraphOwnerBudget = errors.New("collections: typed graph owner retention budget exhausted")

// Explicit internal setup limits, not public production defaults. StateBytes
// charges the state object and typed payload/header retention once per state
// plus known holder backing and owner/lease descriptors conservatively per owner;
// AssetBytes conservatively charges each owner's complete union, even when
// mapped handles are shared. Cold bounds metadata/decoded terms separately.
type typedGraphReadOwnerLimits struct {
	Owners, States         int
	StateBytes, AssetBytes int64
	Cold                   typedGraphColdLimits
}

type typedGraphReadOwnerAccounting struct {
	sync.Mutex
	// stateBytes includes deduplicated suffix state and per-owner known backing.
	limits                 typedGraphReadOwnerLimits
	owners                 int
	stateBytes, assetBytes int64
	states                 map[*typedGraphPublicationState]int
	baseOwners             int
	baseAssetBytes         int64
	baseDescriptorBytes    int64
	baseBackingBytes       int64
}

type typedGraphReadOwner struct {
	overlay                       *typedGraphOverlaySearch
	accounting                    *typedGraphReadOwnerAccounting
	state                         *typedGraphPublicationState
	stateBytes, assetBytes        int64
	descriptorBytes, backingBytes int64
	closed                        bool
}

// Like VectorIndexSearcher.Close, Close is idempotent, not concurrently callable
// with searches. Resource release never runs under the accounting mutex.
func (o *typedGraphReadOwner) Close() error {
	if o == nil || o.closed {
		return nil
	}
	o.closed = true
	var err error
	if o.overlay != nil {
		err = errors.Join(o.overlay.current.Close(), o.overlay.base.Close())
		o.overlay.rows, o.overlay.invNorms = nil, nil
		o.overlay = nil
	}
	if a := o.accounting; a != nil {
		a.Lock()
		a.owners--
		a.assetBytes -= o.assetBytes
		a.stateBytes -= o.descriptorBytes + o.backingBytes
		if a.states[o.state]--; a.states[o.state] == 0 {
			delete(a.states, o.state)
			a.stateBytes -= o.stateBytes
		}
		if a.owners == 0 && a.baseOwners == 0 {
			a.states = nil
			a.limits = typedGraphReadOwnerLimits{}
		}
		a.Unlock()
		o.accounting, o.state = nil, nil
	}
	return err
}

func (o *typedGraphReadOwner) reserve(a *typedGraphReadOwnerAccounting, limits typedGraphReadOwnerLimits) error {
	a.Lock()
	defer a.Unlock()
	if (a.owners != 0 || a.baseOwners != 0) && a.limits != limits {
		return ErrVectorIndexSnapshotMismatch
	}
	newState := a.states[o.state] == 0
	remaining := limits.StateBytes - a.stateBytes - a.baseDescriptorBytes - a.baseBackingBytes
	if o.descriptorBytes < 0 || o.backingBytes < 0 || o.descriptorBytes > remaining || o.backingBytes > remaining-o.descriptorBytes {
		return errTypedGraphOwnerBudget
	}
	remaining -= o.descriptorBytes + o.backingBytes
	if a.owners+a.baseOwners >= limits.Owners || o.assetBytes > limits.AssetBytes-a.assetBytes-a.baseAssetBytes || (newState && (len(a.states) >= limits.States || o.stateBytes > remaining)) {
		return errTypedGraphOwnerBudget
	}
	if a.states == nil {
		a.states = make(map[*typedGraphPublicationState]int)
	}
	a.limits = limits
	a.owners++
	a.assetBytes += o.assetBytes
	a.stateBytes += o.descriptorBytes + o.backingBytes
	if newState {
		a.stateBytes += o.stateBytes
	}
	a.states[o.state]++
	o.accounting = a
	return nil
}

// Open consumes already installed derived state. It never bootstraps, decodes a
// suffix or repairs an invalid state. Public admission additionally requires
// ready metadata on this exact installed state, never the cold fallback below.
// The existing schema-exclusive cross-domain drain is outside the non-reentrant
// storage barrier, and released before mapping. It includes pre-open accepted
// buffered work from every manager. Later writes may linearize after this read.
func (c *Collection) openTypedGraphReadOwner(limits typedGraphReadOwnerLimits) (owner *typedGraphReadOwner, err error) {
	return c.openTypedGraphReadOwnerWithContext(context.Background(), limits)
}

func (c *Collection) openTypedGraphReadOwnerWithContext(ctx context.Context, limits typedGraphReadOwnerLimits) (owner *typedGraphReadOwner, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c == nil || c.db == nil || c.db.IsClosing() || limits.Owners <= 0 || limits.States <= 0 || limits.StateBytes <= 0 || limits.AssetBytes <= 0 || limits.Cold.ManifestRecords <= 0 || limits.Cold.ManifestBytes <= 0 || limits.Cold.AssetBytes <= 0 || limits.Cold.DecodedTermBytes <= 0 {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	unlock := c.lockCollectionSchemaWrite()
	drainErr := c.flushCollectionWriteDomainsForSchemaMutation()
	unlock()
	if drainErr != nil {
		return nil, drainErr
	}
	typedGraphOwnerAfterSnapshotHook.RLock()
	afterDrain := typedGraphOwnerAfterSnapshotHook.afterDrain
	typedGraphOwnerAfterSnapshotHook.RUnlock()
	if afterDrain != nil {
		afterDrain(c)
	}
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	err = WithVectorPartitionStorageBarrierWithContextV1(ctx, c.db.Dir(), func() (err error) {
		snap := c.db.AcquireSnapshot()
		if snap == nil {
			return ErrVectorIndexSnapshotMismatch
		}
		base := &VectorIndexSearcher{collection: c, snapshot: snap}
		candidate := &typedGraphReadOwner{overlay: &typedGraphOverlaySearch{base: base, vectorColumn: -1}}
		defer func() {
			if err != nil {
				_ = candidate.Close()
			}
		}()
		catalog, err := loadCollectionCatalog(snap, c.collectionName())
		if err != nil {
			return err
		}
		state := coord.typedPublication.Load()
		if coord.typedGraphServing.Load() != nil && (state == nil || state.servingBase == nil || !state.servingAdmitted) {
			return ErrVectorIndexSnapshotMismatch
		}
		if !state.matches(catalog) || catalog.typedGraphBase == nil || len(catalog.meta.VectorIndexes) != 1 || !typedGraphBaseSchemaMatches(catalog.typedGraphBase.meta, catalog.meta) {
			return ErrVectorIndexSnapshotMismatch
		}
		candidate.state = state
		// Cumulative admitted bytes/slots upper-bound retained payloads; unchanged
		// headers/norms are shared, not cloned or recomputed here.
		candidate.stateBytes = state.admittedPayloadBytes + state.servingMetadataBytes
		charge := func(n, size int64) bool {
			if n < 0 || size <= 0 || n > (limits.StateBytes-candidate.stateBytes)/size {
				return false
			}
			candidate.stateBytes += n * size
			return true
		}
		if candidate.stateBytes < 0 || candidate.stateBytes > limits.StateBytes || !charge(1, int64(reflect.TypeFor[typedGraphPublicationState]().Size())) || !charge(int64(cap(state.rows)), int64(reflect.TypeFor[columnPhysicalVisibleRow]().Size())) || !charge(int64(state.valueSlots), int64(reflect.TypeFor[columnDeclaredValue]().Size())) || !charge(int64(cap(state.invNorms)), 4) {
			return errTypedGraphOwnerBudget
		}
		root := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
		baseRoot := catalog.typedGraphBase.roots[collectionColumnManifestRootName(catalog.meta.Name)]
		if state.servingBase == nil {
			if err := validateTypedGraphColdManifestBudget(ctx, snap, root, limits.Cold); err != nil {
				return err
			}
			if root != baseRoot {
				if err := validateTypedGraphColdManifestBudget(ctx, snap, baseRoot, limits.Cold); err != nil {
					return err
				}
			}
		}
		var graph columnVectorGraphManifestSnapshot
		var baseView columnPhysicalScanSnapshotView
		if state.servingBase != nil {
			graph, baseView = state.servingBase.graph, state.servingBase.view
			baseView.snapshot = snap
		} else {
			graph, baseView, err = catalog.typedGraphBase.readerView(c, snap)
		}
		if err != nil {
			return err
		}
		// Conservative decoded working terms, distinct from physical mmap and
		// retained state charges. Check products before source assembly. Metadata
		// itself is already bounded by the pre-scan above.
		for _, term := range [][2]int64{{int64(graph.Dimensions), 4}, {1, int64(reflect.TypeFor[DocumentRowRef]().Size())}, {int64(graph.M), 8}} {
			if term[0] <= 0 || term[0] > limits.Cold.DecodedTermBytes/term[1] || graph.RowCount < 0 || int64(graph.RowCount) > limits.Cold.DecodedTermBytes/(term[0]*term[1]) {
				return errTypedGraphOverlayFoldNeeded
			}
		}
		cfg := catalog.meta.Options.ColumnStore
		refs := state.servingRefs
		if state.servingBase == nil {
			refs, err = typedGraphOwnerRefs(baseView.graphOwnerRecords, baseView.Config.ActiveManifest.Generation, baseView.AssetNamespace, graph, baseView.VectorIndexState)
			if err != nil {
				return err
			}
			if root != baseRoot {
				records, err := loadColumnManifestRecordsFromRoot(snap, root)
				if err != nil {
					return err
				}
				currentRefs, err := typedGraphOwnerRefs(records, cfg.ActiveManifest.Generation, cfg.AssetManager.Namespace, graph, baseView.VectorIndexState)
				if err != nil {
					return err
				}
				refs = append(refs, currentRefs...)
				slices.SortFunc(refs, compareColumnAssetRefs)
				refs = slices.Compact(refs)
			}
		}
		for _, ref := range refs {
			if ref.Length <= 0 || ref.Length > limits.Cold.AssetBytes-candidate.assetBytes {
				return errTypedGraphOverlayFoldNeeded
			}
			candidate.assetBytes += int64(ref.Length)
		}
		// Per-owner duplicate charging keeps a shared holder covered after the
		// snapshot-free keeper closes. No holder identity registry is required.
		// Charge known backing/descriptors before mapping; temporary validation
		// maps and allocator/manager bookkeeping are deliberately not claimed here.
		addDescriptor := func(n int64, size uintptr) bool {
			if n < 0 || size == 0 || n > (limits.StateBytes-candidate.descriptorBytes)/int64(size) {
				return false
			}
			candidate.descriptorBytes += n * int64(size)
			return true
		}
		for _, ref := range refs {
			if !addDescriptor(int64(len(ref.Namespace)), 1) {
				return errTypedGraphOwnerBudget
			}
		}
		for _, size := range []uintptr{
			reflect.TypeFor[typedGraphReadOwner]().Size(), reflect.TypeFor[typedGraphOverlaySearch]().Size(),
			reflect.TypeFor[VectorIndexSearcher]().Size(), reflect.TypeFor[CollectionReadView]().Size(),
			reflect.TypeFor[columnVectorGraphPhysicalRowReader]().Size(), reflect.TypeFor[columnVectorGraphSharedPreparedSearchRef]().Size(),
			reflect.TypeFor[ColumnAssetLifecyclePinSet]().Size(),
		} {
			if !addDescriptor(1, size) {
				return errTypedGraphOwnerBudget
			}
		}
		if !addDescriptor(int64(cap(refs)), reflect.TypeFor[ColumnAssetRef]().Size()) {
			return errTypedGraphOwnerBudget
		}
		recordCount := len(baseView.graphOwnerRecords)
		if state.servingBase != nil {
			recordCount = state.servingBase.recordCount
		}
		candidate.backingBytes, err = typedGraphCapturedBaseBackingBound(graph.RowCount, recordCount, graph.AdjacencyLayerCount, limits.StateBytes-candidate.descriptorBytes)
		if err != nil {
			return err
		}
		def := catalog.meta.VectorIndexes[0]
		baseView.graphOwnerRecords = nil
		readerOptions := columnVectorGraphPhysicalRowReaderOptions{admitSources: func(keyBytes int) error {
			if !addDescriptor(int64(keyBytes), 2) {
				return errTypedGraphOwnerBudget
			}
			if err := candidate.reserve(&coord.typedGraphOwners, limits); err != nil {
				return err
			}
			var pinErr error
			base.lifecyclePin, pinErr = c.acquireColumnAssetLifecyclePinSetOwned(ColumnAssetLifecyclePinSetOptions{Source: ColumnAssetLifecyclePinSourcePreparedQuery, Owner: "typed_graph_read_owner", Refs: refs})
			return pinErr
		}}
		if state.servingBase != nil {
			base.reader, err = state.servingBase.openPhysicalReader(c, snap, readerOptions)
		} else {
			base.reader, err = c.openColumnVectorGraphPhysicalRowReaderFromView(snap, def, graph, baseView, readerOptions)
		}
		if err != nil {
			return err
		}
		base.catalog, base.indexName, base.strategy = baseView.Catalog, def.Name, def.Strategy
		base.readerLast = base.reader.Stats()
		base.routeStats = vectorIndexSearchRouteStatsForColumnGraphReader(base.reader)
		view := candidate.overlay
		view.current = newCollectionReadViewAtSnapshot(c, snap, catalog, false, "")
		var eligible bool
		view.pack, _, eligible = base.hnswSearchPackSearchWithBufferRoute(columnVectorGraphNativeSearchQueryModeExact, columnVectorGraphNativeSearchStatsModeMinimal)
		if !eligible {
			return errColumnHNSWSearchPackSearchUnavailable
		}
		for i, column := range cfg.Columns {
			if column.Path == def.Field {
				view.vectorColumn = i
				break
			}
		}
		if view.vectorColumn < 0 || len(state.rows) != len(state.invNorms) {
			return ErrVectorIndexSnapshotMismatch
		}
		view.rows, view.invNorms = state.rows, state.invNorms
		view.sourceRows, view.sourceTombstones, view.sourceBytes = state.physicalRows, state.tombstones, state.installedAssetBytes
		// Captured roots are independently published copies. Full metadata equality,
		// under the already-admitted joint owner, proves current materializer coverage.
		if metadata := state.servingBase; metadata != nil && collectionMetaValuesEqual(metadata.view.Catalog.meta, catalog.meta) {
			prepared := metadata.materializerView
			token, ok := snap.StateToken()
			if !ok {
				return backenddb.ErrClosed
			}
			prepared.Catalog, prepared.snapshot = catalog, snap
			prepared.CommitSeq, prepared.SystemRoot = token.CommitSeq, token.SystemRootPageID
			prepared.Diagnostics.ManifestRootName = catalog.columnManifestRootName
			if prepared.Diagnostics.ManifestRootName == "" && cfg.ManifestRoot != nil {
				prepared.Diagnostics.ManifestRootName = cfg.ManifestRoot.Name
			}
			if prepared.Diagnostics.ManifestRootName == "" {
				prepared.Diagnostics.ManifestRootName = collectionColumnManifestRootName(catalog.meta.Name)
			}
			prepared.Diagnostics.ManifestRoot = catalog.rootID(prepared.Diagnostics.ManifestRootName)
			view.current.columnSnapshotView = &prepared
		}
		snap.DetachForegroundRead()
		owner = candidate
		return nil
	})
	return owner, err
}
