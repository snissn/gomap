package collections

import (
	"errors"
	"fmt"
	"sync"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// ErrColumnVectorGraphStablePreparePendingWrites reports that a prepared
// vector closure cannot be cut from the current durable snapshot without
// first publishing collection-local writes. Stable preparation is deliberately
// side-effect free, so callers must establish an explicit Flush boundary.
var ErrColumnVectorGraphStablePreparePendingWrites = errors.New("collections: stable vector prepare requires an explicitly flushed collection snapshot")

// ErrColumnVectorGraphStablePreparedClosureConsumed reports a second ownership
// transfer from an already-consumed or abandoned prepared closure.
var ErrColumnVectorGraphStablePreparedClosureConsumed = errors.New("collections: stable vector prepared closure consumed")

var (
	columnVectorGraphStableBeforeCaptureAdmissionTestHookMu sync.RWMutex
	columnVectorGraphStableBeforeCaptureAdmissionTestHook   func()
)

func setColumnVectorGraphStableBeforeCaptureAdmissionTestHook(hook func()) func() {
	columnVectorGraphStableBeforeCaptureAdmissionTestHookMu.Lock()
	previous := columnVectorGraphStableBeforeCaptureAdmissionTestHook
	columnVectorGraphStableBeforeCaptureAdmissionTestHook = hook
	columnVectorGraphStableBeforeCaptureAdmissionTestHookMu.Unlock()
	return func() {
		columnVectorGraphStableBeforeCaptureAdmissionTestHookMu.Lock()
		columnVectorGraphStableBeforeCaptureAdmissionTestHook = previous
		columnVectorGraphStableBeforeCaptureAdmissionTestHookMu.Unlock()
	}
}

func runColumnVectorGraphStableBeforeCaptureAdmissionTestHook() {
	columnVectorGraphStableBeforeCaptureAdmissionTestHookMu.RLock()
	hook := columnVectorGraphStableBeforeCaptureAdmissionTestHook
	columnVectorGraphStableBeforeCaptureAdmissionTestHookMu.RUnlock()
	if hook != nil {
		hook()
	}
}

// ColumnVectorGraphStableObservations records the production work that cut a
// vector graph closure. Counts come from the same physical appenders used by
// publication; this seam does not reconstruct them from paths.
type ColumnVectorGraphStableObservations struct {
	Segments           uint64
	Descriptors        uint64
	LogicalObligations uint64
	ContentSyncs       uint64
	NamespaceSyncs     uint64
}

// ColumnVectorGraphStablePreparedClosure owns the exact production authority
// returned by the physical vector graph preparer before any collection root is
// published. Ownership can be transferred or abandoned exactly once.
type ColumnVectorGraphStablePreparedClosure struct {
	mu           sync.Mutex
	resources    *rootpublication.StableResourceSet
	observations ColumnVectorGraphStableObservations
	consumed     bool
}

// Observations returns the production durability counters for this closure.
func (closure *ColumnVectorGraphStablePreparedClosure) Observations() ColumnVectorGraphStableObservations {
	if closure == nil {
		return ColumnVectorGraphStableObservations{}
	}
	closure.mu.Lock()
	defer closure.mu.Unlock()
	return closure.observations
}

// TakeStableResources transfers exact-handle authority exactly once.
func (closure *ColumnVectorGraphStablePreparedClosure) TakeStableResources() (*rootpublication.StableResourceSet, error) {
	if closure == nil {
		return nil, ErrColumnVectorGraphStablePreparedClosureConsumed
	}
	closure.mu.Lock()
	defer closure.mu.Unlock()
	if closure.consumed || closure.resources == nil {
		return nil, ErrColumnVectorGraphStablePreparedClosureConsumed
	}
	resources := closure.resources
	closure.resources = nil
	closure.consumed = true
	return resources, nil
}

// Release abandons untransferred authority and is idempotent.
func (closure *ColumnVectorGraphStablePreparedClosure) Release() {
	if closure == nil {
		return
	}
	closure.mu.Lock()
	resources := closure.resources
	closure.resources = nil
	closure.consumed = true
	closure.mu.Unlock()
	if resources != nil {
		resources.Release()
	}
}

// Abandon is an explicit alias for Release on pre-visibility failure.
func (closure *ColumnVectorGraphStablePreparedClosure) Abandon() { closure.Release() }

func (prepared *columnVectorGraphPreparedPhysicalAsset) takeStablePreparedClosure() (*ColumnVectorGraphStablePreparedClosure, error) {
	if prepared == nil || prepared.stableResources == nil {
		return nil, fmt.Errorf("%w: vector graph preparation returned no stable authority", rootpublication.ErrUnresolvedResource)
	}
	descriptors := prepared.stableResources.Descriptors()
	var obligations uint64
	for _, descriptor := range descriptors {
		fields := descriptor.ReachabilityFields()
		if len(fields) != 1 || fields[0] != rootpublication.ReachabilityVectorGraphPack {
			return nil, fmt.Errorf("%w: vector graph descriptor reachability=%q", rootpublication.ErrResourceConflict, fields)
		}
		obligations += uint64(len(descriptor.LogicalObligations()))
	}
	if len(descriptors) == 0 || obligations == 0 || prepared.stableSegments == 0 ||
		prepared.stableContentSyncs != prepared.stableSegments || prepared.stableNamespaceSyncs != prepared.stableSegments {
		return nil, fmt.Errorf("%w: vector graph closure descriptors=%d obligations=%d segments=%d content_syncs=%d namespace_syncs=%d",
			rootpublication.ErrUnresolvedResource, len(descriptors), obligations, prepared.stableSegments,
			prepared.stableContentSyncs, prepared.stableNamespaceSyncs)
	}
	// Stats also verifies that the resource set can still enumerate its exact
	// namespace evidence before ownership is detached from the prepared value.
	var namespaceSyncs uint64
	for _, stats := range prepared.stableResources.Stats(time.Now()) {
		namespaceSyncs += stats.NamespaceSyncs
	}
	if namespaceSyncs != prepared.stableNamespaceSyncs {
		return nil, fmt.Errorf("%w: vector graph namespace observations=%d producer=%d", rootpublication.ErrResourceConflict, namespaceSyncs, prepared.stableNamespaceSyncs)
	}
	closure := &ColumnVectorGraphStablePreparedClosure{
		resources: prepared.stableResources,
		observations: ColumnVectorGraphStableObservations{
			Segments: prepared.stableSegments, Descriptors: uint64(len(descriptors)), LogicalObligations: obligations,
			ContentSyncs: prepared.stableContentSyncs, NamespaceSyncs: prepared.stableNamespaceSyncs,
		},
	}
	prepared.stableResources = nil
	return closure, nil
}

// PrepareVectorIndexStableClosure scans the current collection snapshot and
// runs the production column_graph physical preparer without publishing any
// collection root or metadata change. The returned closure owns the exact
// state.Assets authority that a later root publication would consume.
func (c *Collection) PrepareVectorIndexStableClosure(name string) (*ColumnVectorGraphStablePreparedClosure, error) {
	if err := ValidateIndexName(name); err != nil {
		return nil, err
	}
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if domain := c.writeDomain; domain != nil {
		if domain.indexedAsyncFlushRunning() {
			return nil, ErrColumnVectorGraphStablePreparePendingWrites
		}
		domain.mu.RLock()
		pending := domain.count != 0 || hasBufferedIndexedPendingWrites(domain) || hasBufferedNoIndexTableWritesLocked(domain)
		domain.mu.RUnlock()
		if pending || domain.indexedAsyncFlushRunning() {
			return nil, ErrColumnVectorGraphStablePreparePendingWrites
		}
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() {
		if snap != nil {
			_ = snap.Close()
		}
	}()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		return nil, err
	}
	baseMeta := catalog.meta
	def, ok := findVectorIndex(baseMeta.VectorIndexes, name)
	if !ok {
		return nil, ErrIndexNotFound
	}
	if def.Strategy != VectorIndexStrategyColumnGraph {
		return nil, errors.New("collections: stable vector prepared closure requires column_graph strategy")
	}
	cfg := baseMeta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.AssetManager == nil {
		return nil, errors.New("collections: stable vector prepared closure requires enabled physical column storage")
	}
	if normalizedDocumentFormat(baseMeta.Options.DocumentFormat) != DocumentFormatJSON {
		return nil, fmt.Errorf("collections: stable vector prepared closure for %q requires JSON documents", name)
	}
	rootID := catalog.rootID(collectionColumnManifestRootName(baseMeta.Name))
	if rootID == 0 {
		return nil, errors.New("collections: stable vector prepared closure requires an existing physical column manifest root")
	}
	if cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil {
		return nil, errors.New("collections: stable vector prepared closure requires active and recovery-authoritative manifest identities")
	}
	if err := validateColumnManifestIdentityAtRoot(snap, rootID, *cfg.ActiveManifest); err != nil {
		return nil, err
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, rootID)
	if err != nil {
		return nil, err
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		return nil, err
	}
	if err := validateColumnManifestSnapshot(manifest, records, *cfg, *cfg.ActiveManifest, baseMeta.Name, "column vector graph stable prepare"); err != nil {
		return nil, err
	}
	if manifest.AppliedCommandLSN == 0 {
		return nil, errors.New("collections: stable vector prepared closure requires non-zero manifest AppliedCommandLSN")
	}
	rows, typedSource, usedTypedColumns, err := c.columnVectorGraphRowsFromTypedColumnCatalogSnapshot(snap, catalog, *cfg, records, manifest, def)
	if err == nil && !usedTypedColumns {
		rows, err = c.columnVectorGraphRowsFromCatalogSnapshot(snap, catalog, def)
	}
	if err != nil {
		return nil, err
	}
	if typedSource != nil {
		defer func() { _ = typedSource.Close() }()
	}
	if !usedTypedColumns {
		if err := c.assignColumnVectorGraphRowRefsFromBaseManifest(baseMeta.Name, *cfg, records, manifest.Generation, rows); err != nil {
			return nil, err
		}
	}
	closeErr := snap.Close()
	// Snapshot pointers are single-use after Close returns, including when it
	// reports an error. Clear deferred-cleanup ownership before checking it.
	snap = nil
	if closeErr != nil {
		return nil, closeErr
	}
	runColumnVectorGraphStableBeforeCaptureAdmissionTestHook()
	captureLease, err := c.db.AcquireStableResourceCaptureLease()
	if err != nil {
		return nil, err
	}
	defer captureLease.Release()
	if err := buildColumnVectorGraphAdjacency(rows, def); err != nil {
		return nil, err
	}
	prepared, _, _, err := prepareColumnVectorGraphRebuildManifestForPublicationTimedWithTypedSource(
		baseMeta.Name, *cfg, baseMeta.VectorIndexes, def, manifest, records,
		manifest.AppliedCommandLSN, rows, c.db.ColumnAssetRootDir(), c.db.StableResourceIdentityPinRegistry(), typedSource, nil,
	)
	if err != nil {
		return nil, err
	}
	defer prepared.releaseStableResources()
	return prepared.takeStablePreparedClosure()
}
