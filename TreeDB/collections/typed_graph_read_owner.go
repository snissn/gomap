package collections

import (
	"context"
	"errors"
	"reflect"
	"runtime"
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
	Physical               typedGraphPhysicalResourceLimits
}

// Physical limits are distinct from logical AssetBytes/StateBytes. Segments,
// Descriptors, MappedBytes, and FallbackBytes admit one holder's complete
// authorized potential and all overlapping holders collection-wide;
// InventoryBytes limits a deterministic modeled charge for retained exact-ref,
// segment, guardian, and holder-cache metadata. It is intentionally independent
// of StateBytes and is not a measurement or hard bound of Go heap/RSS because
// allocator-class and shared map-bucket slack are outside the model.
type typedGraphPhysicalResourceLimits struct {
	Segments       int   `json:"segments"`
	Descriptors    int   `json:"descriptors"`
	MappedBytes    int64 `json:"mapped_bytes"`
	FallbackBytes  int64 `json:"fallback_bytes"`
	InventoryBytes int64 `json:"inventory_bytes"`
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

// attachTypedGraphLegacyScalarU8QuantizedAsset is the Q2-only acquisition seam
// for the private filtered scalar-u8 collector. Owner assembly keeps generic
// quantized assets skipped; this attaches exactly one holder-owned legacy v1
// code plane to the base reader without transferring resource ownership.
func (o *typedGraphReadOwner) attachTypedGraphLegacyScalarU8QuantizedAsset(name string) error {
	return o.attachTypedGraphLegacyScalarU8QuantizedAssetWithContext(context.Background(), name)
}

func (o *typedGraphReadOwner) attachTypedGraphLegacyScalarU8QuantizedAssetWithContext(ctx context.Context, name string) error {
	if o == nil || o.closed || o.overlay == nil || o.overlay.base == nil || o.overlay.base.reader == nil || o.overlay.base.collection == nil {
		return ErrVectorIndexSnapshotMismatch
	}
	return o.overlay.base.collection.requestAndAttachColumnVectorGraphSharedPreparedLegacyScalarU8AssetWithContext(ctx, o.overlay.base.reader, name)
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

type typedGraphReadOwnerServingOpen struct {
	candidate *typedGraphReadOwner
	base      *VectorIndexSearcher
	snapshot  *backenddb.Snapshot
	catalog   *collectionCatalog
	state     *typedGraphPublicationState
	metadata  *typedGraphServingBaseMetadata
	baseView  columnPhysicalScanSnapshotView
	def       VectorIndexDefinition
}

// finishTypedGraphReadOwnerOpen binds request-local state after the immutable
// base reader exists. For serving owners this runs after the storage barrier is
// released: the snapshot fixes catalog/current-row visibility and the already
// registered complete lifecycle pin protects every referenced asset.
func (c *Collection) finishTypedGraphReadOwnerOpen(open *typedGraphReadOwnerServingOpen) error {
	if c == nil || open == nil || open.candidate == nil || open.base == nil || open.snapshot == nil || open.catalog == nil || open.state == nil || open.base.reader == nil {
		return ErrVectorIndexSnapshotMismatch
	}
	base, snap, state := open.base, open.snapshot, open.state
	config := &open.baseView.FullConfig
	base.catalog, base.indexName, base.strategy = open.baseView.Catalog, open.def.Name, open.def.Strategy
	base.readerLast = base.reader.Stats()
	base.routeStats = vectorIndexSearchRouteStatsForColumnGraphReader(base.reader)
	view := open.candidate.overlay
	view.current = newCollectionReadViewAtSnapshot(c, snap, open.catalog, false, "")
	var eligible bool
	view.pack, _, eligible = base.hnswSearchPackSearchWithBufferRoute(columnVectorGraphNativeSearchQueryModeExact, columnVectorGraphNativeSearchStatsModeMinimal)
	if !eligible {
		return errColumnHNSWSearchPackSearchUnavailable
	}
	for i, column := range config.Columns {
		if column.Path == open.def.Field {
			view.vectorColumn = i
			break
		}
	}
	if view.vectorColumn < 0 || len(state.rows) != len(state.invNorms) {
		return ErrVectorIndexSnapshotMismatch
	}
	view.rows, view.invNorms = state.rows, state.invNorms
	view.sourceRows, view.sourceTombstones, view.sourceBytes = state.physicalRows, state.tombstones, state.installedAssetBytes
	if state.servingBase != nil {
		prepared := state.servingMaterializer
		token, ok := snap.StateToken()
		if !ok {
			return backenddb.ErrClosed
		}
		prepared.Catalog, prepared.snapshot = open.catalog, snap
		prepared.FullConfig = *config
		prepared.CommitSeq, prepared.SystemRoot = token.CommitSeq, token.SystemRootPageID
		prepared.Diagnostics.ManifestRootName = open.catalog.columnManifestRootName
		if prepared.Diagnostics.ManifestRootName == "" && config.ManifestRoot != nil {
			prepared.Diagnostics.ManifestRootName = config.ManifestRoot.Name
		}
		if prepared.Diagnostics.ManifestRootName == "" {
			prepared.Diagnostics.ManifestRootName = collectionColumnManifestRootName(open.catalog.meta.Name)
		}
		prepared.Diagnostics.ManifestRoot = open.catalog.rootID(prepared.Diagnostics.ManifestRootName)
		view.current.columnSnapshotView = &prepared
		view.current.preparedMaterializer = &state.servingMaterializer
	}
	snap.DetachForegroundRead()
	return nil
}

// Open consumes already installed derived state. It never bootstraps, decodes a
// suffix or repairs an invalid state. Public admission additionally requires
// ready metadata on this exact installed state, never the cold fallback below.
// Only acknowledged buffered work needs a cross-domain drain. Shared schema
// admission keeps maintenance exclusive while allowing a fully published read
// to capture the previous coherent generation during an immediate write. A
// post-snapshot buffered check upgrades only that capture to an exclusive drain
// and retry. Later writes may linearize after this read.
func (c *Collection) openTypedGraphReadOwner(limits typedGraphReadOwnerLimits) (owner *typedGraphReadOwner, err error) {
	return c.openTypedGraphReadOwnerWithContext(context.Background(), limits)
}

func (c *Collection) openTypedGraphReadOwnerWithContext(ctx context.Context, limits typedGraphReadOwnerLimits) (owner *typedGraphReadOwner, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c == nil || c.db == nil || c.db.IsClosing() || !typedGraphReadOwnerLimitsValid(limits) {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	typedGraphOwnerAfterSnapshotHook.RLock()
	afterDrain := typedGraphOwnerAfterSnapshotHook.afterDrain
	afterCapture := typedGraphOwnerAfterSnapshotHook.afterCapture
	beforeServingOpen := typedGraphOwnerAfterSnapshotHook.beforeServingOpen
	typedGraphOwnerAfterSnapshotHook.RUnlock()
	if afterDrain != nil {
		afterDrain(c)
	}
	var drain, retry bool
	var changed <-chan struct{}
	var pendingServing *typedGraphReadOwnerServingOpen
	capture := func() (err error) {
		before := coord.typedPublication.Load()
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
		if afterCapture != nil {
			afterCapture(c)
		}
		coord.typedPublicationDebtMu.Lock()
		if c.db.IsClosing() || coord.typedPublicationClosed {
			coord.typedPublicationDebtMu.Unlock()
			return backenddb.ErrClosed
		}
		if state := coord.typedPublication.Load(); state == nil || state.invalid {
			coord.typedPublicationDebtMu.Unlock()
			return ErrVectorIndexSnapshotMismatch
		}
		if coord.typedPublicationBuffered != 0 {
			drain = true
			coord.typedPublicationDebtMu.Unlock()
			return ErrVectorIndexSnapshotMismatch
		}
		coord.typedPublicationDebtMu.Unlock()
		catalog, err := loadCollectionCatalog(snap, c.collectionName())
		if err != nil {
			return err
		}
		state := coord.typedPublication.Load()
		if coord.typedGraphServing.Load() != nil && (state == nil || state.servingBase == nil || !state.servingAdmitted) {
			return ErrVectorIndexSnapshotMismatch
		}
		if state == nil || state.invalid || catalog == nil || catalog.typedGraphBase == nil || len(catalog.meta.VectorIndexes) != 1 || !typedGraphBaseSchemaMatches(catalog.typedGraphBase.meta, catalog.meta) {
			return ErrVectorIndexSnapshotMismatch
		}
		if !state.matches(catalog) {
			coord.typedPublicationDebtMu.Lock()
			defer coord.typedPublicationDebtMu.Unlock()
			if c.db.IsClosing() || coord.typedPublicationClosed {
				return backenddb.ErrClosed
			}
			// A changed pointer can put an older snapshot beside a newer state.
			// Otherwise only an unfinished publication can repair this mismatch.
			retry = before != state || coord.typedPublication.Load() != state
			if !retry && coord.typedPublicationPending.rows != 0 {
				retry = true
				if coord.typedPublicationChanged == nil {
					coord.typedPublicationChanged = make(chan struct{})
				}
				changed = coord.typedPublicationChanged
			}
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
		// The compact installed serving metadata does not retain the complete
		// catalog config. Carry the current validated value in this request-local
		// view across the storage barrier for materializer attachment.
		baseView.FullConfig = *cfg
		refs := state.servingRefs
		var servingKey columnVectorGraphSharedPreparedSearchKey
		if state.servingBase != nil && state.servingBase.preparedKey != "" {
			servingKey, err = state.servingBase.servingPreparedSearchKey(c)
			if err != nil || state.servingBaseRefsDigest != servingKey.refsDigest || state.servingBaseRefsCount != servingKey.refsCount || len(refs) < servingKey.refsCount {
				return ErrVectorIndexSnapshotMismatch
			}
		}
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
		if state.servingBase != nil {
			if state.servingPinBytes <= 0 || state.servingPinBytes > limits.Cold.AssetBytes {
				return errTypedGraphOverlayFoldNeeded
			}
			candidate.assetBytes = state.servingPinBytes
		} else {
			for _, ref := range refs {
				if ref.Length <= 0 || ref.Length > limits.Cold.AssetBytes-candidate.assetBytes {
					return errTypedGraphOverlayFoldNeeded
				}
				candidate.assetBytes += int64(ref.Length)
			}
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
		if state.servingBase != nil {
			if !addDescriptor(state.servingOwnerRefBytes, 1) {
				return errTypedGraphOwnerBudget
			}
		} else {
			for _, ref := range refs {
				if !addDescriptor(int64(len(ref.Namespace)), 1) {
					return errTypedGraphOwnerBudget
				}
			}
		}
		for _, size := range []uintptr{
			reflect.TypeFor[typedGraphReadOwner]().Size(), reflect.TypeFor[typedGraphOverlaySearch]().Size(),
			reflect.TypeFor[VectorIndexSearcher]().Size(), reflect.TypeFor[CollectionReadView]().Size(),
			reflect.TypeFor[columnVectorGraphPhysicalRowReader]().Size(), reflect.TypeFor[columnVectorGraphSharedPreparedSearchRef]().Size(),
			reflect.TypeFor[ColumnAssetLifecyclePinSet]().Size(), reflect.TypeFor[typedGraphServingHolderCapability]().Size(),
		} {
			if !addDescriptor(1, size) {
				return errTypedGraphOwnerBudget
			}
		}
		if state.servingBase == nil && !addDescriptor(int64(cap(refs)), reflect.TypeFor[ColumnAssetRef]().Size()) {
			return errTypedGraphOwnerBudget
		}
		recordCount := len(baseView.graphOwnerRecords)
		if state.servingBase != nil {
			recordCount = state.servingBase.recordCount
		}
		def := catalog.meta.VectorIndexes[0]
		var legacyScalarU8Descriptors []columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptor
		if graph.RowCount > 0 {
			// Per-owner admission covers a holder built independently of any
			// keeper, so construct the exact descriptor shape that can outlive
			// this decoded base view before opening sources.
			legacyScalarU8Descriptors = columnVectorGraphSharedPreparedLegacyScalarU8AssetDescriptors(def, baseView.VectorIndexState)
		}
		candidate.backingBytes, err = typedGraphCapturedBaseBackingBoundWithLegacyScalarU8Assets(graph.RowCount, recordCount, graph.AdjacencyLayerCount, legacyScalarU8Descriptors, limits.StateBytes-candidate.descriptorBytes)
		if err != nil {
			return err
		}
		readerAttachmentBytes, err := typedGraphLegacyScalarU8ReaderAttachmentBackingBound(len(legacyScalarU8Descriptors), limits.StateBytes-candidate.descriptorBytes-candidate.backingBytes)
		if err != nil {
			return err
		}
		candidate.backingBytes += readerAttachmentBytes
		baseView.graphOwnerRecords = nil
		admit := func(keyBytes int) error {
			if !addDescriptor(int64(keyBytes), 2) {
				return errTypedGraphOwnerBudget
			}
			if err := candidate.reserve(&coord.typedGraphOwners, limits); err != nil {
				return err
			}
			var pinErr error
			if servingKey.valid() {
				base.lifecyclePin, pinErr = c.acquireTypedGraphServingLifecyclePin(state, servingKey, "typed_graph_read_owner")
			} else {
				ownedRefs := append([]ColumnAssetRef(nil), refs...)
				base.lifecyclePin, pinErr = c.acquireColumnAssetLifecyclePinSetOwned(ColumnAssetLifecyclePinSetOptions{Source: ColumnAssetLifecyclePinSourcePreparedQuery, Owner: "typed_graph_read_owner", Refs: ownedRefs})
			}
			return pinErr
		}
		readerOptions := columnVectorGraphPhysicalRowReaderOptions{SkipQuantizedAssets: true, admitSources: admit}
		if state.servingBase != nil {
			if state.servingBase.preparedKey != "" {
				if err := admit(len(state.servingBase.preparedKey)); err != nil {
					return err
				}
				// Source construction cannot nest the non-reentrant storage barrier.
				// Carry only the pinned snapshot/state plan outside this capture; the
				// cache-miss builder will acquire and validate its own snapshot.
				pendingServing = &typedGraphReadOwnerServingOpen{
					candidate: candidate, base: base, snapshot: snap, catalog: catalog,
					state: state, metadata: state.servingBase, baseView: baseView,
					def: def,
				}
				return nil
			}
			// A zero-row base has no eligible shared prepared holder. Preserve
			// its established empty reader and immutable zero-row quantized
			// validation cache; suffix rows are served by the overlay above it.
			base.reader, err = state.servingBase.openPhysicalReader(c, snap, readerOptions)
		} else {
			base.reader, err = c.openColumnVectorGraphPhysicalRowReaderFromView(snap, def, graph, baseView, readerOptions)
		}
		if err != nil {
			return err
		}
		if err := c.finishTypedGraphReadOwnerOpen(&typedGraphReadOwnerServingOpen{
			candidate: candidate, base: base, snapshot: snap, catalog: catalog,
			state: state, baseView: baseView, def: def,
		}); err != nil {
			return err
		}
		owner = candidate
		return nil
	}
	for {
		if c.db.IsClosing() {
			return nil, backenddb.ErrClosed
		}
		exclusive := drain
		drain = false
		var unlockSchema func()
		if exclusive {
			unlockSchema = c.lockCollectionSchemaWrite()
			if err := c.flushCollectionWriteDomainsForSchemaMutation(); err != nil {
				unlockSchema()
				return nil, err
			}
		} else {
			unlockSchema = c.lockCollectionSchemaRead()
		}
		retry, changed, pendingServing = false, nil, nil
		err = WithVectorPartitionStorageBarrierWithContextV1(ctx, c.db.Dir(), capture)
		unlockSchema()
		if err != nil && pendingServing != nil {
			err = errors.Join(err, pendingServing.candidate.Close())
			pendingServing = nil
		}
		if err == nil && pendingServing != nil {
			open := pendingServing
			// Test-only observation at the exact admitted-request/independent-
			// builder boundary. No schema or storage lock is held here, so a fold
			// may replace the captured base and exercise the transparent retry.
			if beforeServingOpen != nil {
				beforeServingOpen(c)
			}
			// Transfer the complete caller pin to the capability before opening.
			pin := open.base.lifecyclePin
			open.base.lifecyclePin = nil
			open.base.reader, err = open.metadata.openServingPhysicalReaderWithOwnedPin(ctx, c, open.snapshot, pin, limits.Physical, columnVectorGraphPhysicalRowReaderOptions{SkipQuantizedAssets: true})
			if err == nil {
				err = c.finishTypedGraphReadOwnerOpen(open)
			}
			if err != nil {
				openErr := err
				closeErr := open.candidate.Close()
				if errors.Is(openErr, errTypedGraphServingCurrentKeyChanged) && closeErr == nil {
					// A first holder miss deliberately rebuilds from independently
					// captured current metadata. If a fold replaced the captured
					// request key after admission, discard that fully pinned request
					// and transparently recapture. An old-key holder hit never enters
					// the builder handshake and remains valid for its pinned owner.
					retry = true
					err = ErrVectorIndexSnapshotMismatch
				} else {
					err = errors.Join(openErr, closeErr)
				}
			} else {
				owner = open.candidate
			}
			pendingServing = nil
		}
		if drain {
			continue
		}
		if !retry {
			return owner, err
		}
		// Never retain a snapshot or the storage barrier while waiting for the
		// durable-root -> immutable-state installation gap to close.
		if changed != nil {
			select {
			case <-changed:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		} else {
			runtime.Gosched()
		}
	}
}
