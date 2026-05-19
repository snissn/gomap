package collections

import (
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// VectorIndexStatus reports the operational state of one declared collection
// vector index. Status checks may inspect the persisted graph root and are
// intended for operational paths, not search hot paths.
type VectorIndexStatus struct {
	Definition                    VectorIndexDefinition
	Name                          string
	RootName                      string
	RootID                        uint64
	Strategy                      VectorIndexStrategy
	NativeRuntimeUsed             bool
	NativeRootLoaded              bool
	NativeRootBytes               int64
	ExactFallbackReason           string
	ColumnGraphLoaded             bool
	ColumnGraphUnavailableReason  string
	PhysicalColumnAssetsSupported bool
	Registered                    bool
	Stats                         VectorIndexStats
	RebuildNeeded                 bool
	Duration                      time.Duration
}

// VectorIndexStatus returns persisted-root and runtime status for a declared
// vector index.
func (c *Collection) VectorIndexStatus(name string) (VectorIndexStatus, error) {
	status, err := c.vectorIndexStatus(name, true)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	return status, nil
}

// RebuildVectorIndex scans canonical collection documents, rebuilds the
// declared HNSW graph, and publishes a full native vector-index root. It is an
// operational maintenance call: collection writes wait while the rebuild scans
// and publishes so the replacement graph cannot miss committed mutations. For
// explicit column_graph indexes, this is currently a status probe that returns
// rebuild-needed until the physical column asset writer/publisher lands.
func (c *Collection) RebuildVectorIndex(name string) (VectorIndexStatus, error) {
	start := time.Now()
	if c == nil {
		return VectorIndexStatus{}, errCollectionNil
	}
	if c.db == nil {
		return VectorIndexStatus{}, errCollectionDBNil
	}
	if err := ValidateIndexName(name); err != nil {
		return VectorIndexStatus{}, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return VectorIndexStatus{}, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalogWithoutColumnRootValidation(snap, c.meta.Name)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if catalog == nil {
		return VectorIndexStatus{}, errCollectionNotFound
	}
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, name)
	if !ok {
		return VectorIndexStatus{}, ErrIndexNotFound
	}
	if vectorIndexDefinitionStrategy(def) == VectorIndexStrategyColumnGraph {
		graph, loadStatus, err := c.loadColumnGraphVectorIndexSnapshotFromCatalog(snap, catalog, def)
		if err != nil {
			return VectorIndexStatus{}, err
		}
		if graph != nil {
			status := vectorIndexStatusFromColumnGraphLoad(def, loadStatus)
			status.Duration = collectionObservedElapsedSince(start)
			return status, nil
		}
		status := columnGraphRebuildUnsupportedStatus(def, loadStatus)
		status.Duration = collectionObservedElapsedSince(start)
		return status, nil
	}
	// Native rebuild publishes a full replacement graph root. Hold the same
	// mutation barrier used by writes across the primary scan and root publish
	// so no committed write can be skipped by the clean replacement snapshot.
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWrites(); err != nil {
		return VectorIndexStatus{}, err
	}
	def, err = c.declaredVectorIndexDefinitionPrepared(name)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if vectorIndexDefinitionStrategy(def) == VectorIndexStrategyColumnGraph {
		graph, loadStatus, err := c.LoadColumnGraphVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
		if err != nil {
			return VectorIndexStatus{}, err
		}
		if graph != nil {
			status := vectorIndexStatusFromColumnGraphLoad(def, loadStatus)
			status.Duration = collectionObservedElapsedSince(start)
			return status, nil
		}
		status := columnGraphRebuildUnsupportedStatus(def, loadStatus)
		status.Duration = collectionObservedElapsedSince(start)
		return status, nil
	}
	index, err := c.buildVectorIndexPrepared(vectorIndexOptionsFromDefinition(def), false, false)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	index.setNativePersistent(true)
	baseEpoch, err := c.currentNativeVectorIndexRootID(def.Name)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	index.recordFullSnapshotBaseEpoch(baseEpoch)
	native, err := index.saveNativeSnapshotPrepared()
	if err != nil {
		return VectorIndexStatus{}, err
	}
	c.RegisterVectorIndex(index)
	if c.manager != nil && index.isNativePersistent() {
		c.manager.registerCollectionHandle(c)
	}
	duration := collectionObservedElapsedSince(start)
	index.mu.Lock()
	index.lastRebuildDuration = duration
	index.mu.Unlock()

	stats := index.Stats()
	stats.BytesDisk = native.BytesDisk
	status := VectorIndexStatus{
		Definition:          def,
		Name:                def.Name,
		RootName:            collectionVectorIndexRootName(c.meta.Name, def.Name),
		RootID:              native.RootID,
		Strategy:            VectorIndexStrategyNativeRuntime,
		NativeRuntimeUsed:   true,
		NativeRootLoaded:    native.Loaded,
		NativeRootBytes:     native.BytesDisk,
		ExactFallbackReason: native.ExactFallbackReason,
		Registered:          true,
		Stats:               stats,
		RebuildNeeded:       native.ExactFallbackReason != "" || stats.RebuildNeeded || stats.SnapshotDirty,
		Duration:            duration,
	}
	return status, nil
}

func (c *Collection) declaredVectorIndexDefinition(name string) (VectorIndexDefinition, error) {
	if c == nil {
		return VectorIndexDefinition{}, errCollectionNil
	}
	if c.db == nil {
		return VectorIndexDefinition{}, errCollectionDBNil
	}
	if err := c.flushBufferedWrites(); err != nil {
		return VectorIndexDefinition{}, err
	}
	return c.declaredVectorIndexDefinitionPrepared(name)
}

func (c *Collection) declaredVectorIndexDefinitionPrepared(name string) (VectorIndexDefinition, error) {
	if c == nil {
		return VectorIndexDefinition{}, errCollectionNil
	}
	if c.db == nil {
		return VectorIndexDefinition{}, errCollectionDBNil
	}
	if err := ValidateIndexName(name); err != nil {
		return VectorIndexDefinition{}, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return VectorIndexDefinition{}, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		return VectorIndexDefinition{}, err
	}
	if catalog == nil {
		return VectorIndexDefinition{}, errCollectionNotFound
	}
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, name)
	if !ok {
		return VectorIndexDefinition{}, ErrIndexNotFound
	}
	return def, nil
}

func (c *Collection) vectorIndexStatus(name string, inspectNativeRoot bool) (VectorIndexStatus, error) {
	if c == nil {
		return VectorIndexStatus{}, errCollectionNil
	}
	if c.db == nil {
		return VectorIndexStatus{}, errCollectionDBNil
	}
	if err := ValidateIndexName(name); err != nil {
		return VectorIndexStatus{}, err
	}
	if err := c.flushBufferedWrites(); err != nil {
		return VectorIndexStatus{}, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return VectorIndexStatus{}, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalogWithoutColumnRootValidation(snap, c.meta.Name)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if catalog == nil {
		return VectorIndexStatus{}, errCollectionNotFound
	}
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, name)
	if !ok {
		return VectorIndexStatus{}, ErrIndexNotFound
	}
	if vectorIndexDefinitionStrategy(def) == VectorIndexStrategyColumnGraph {
		return c.columnGraphVectorIndexStatusFromCatalog(snap, catalog, def)
	}
	if err := validateColumnStoreCatalogRoot(snap, catalog); err != nil {
		return VectorIndexStatus{}, err
	}

	rootName := collectionVectorIndexRootName(catalog.meta.Name, def.Name)
	rootID := catalog.rootID(rootName)
	overlayRootIDs := catalog.overlayRootIDs(rootName)
	if rootID == 0 && len(overlayRootIDs) != 0 {
		rootID = overlayRootIDs[len(overlayRootIDs)-1]
	}
	status := VectorIndexStatus{
		Definition: def,
		Name:       def.Name,
		RootName:   rootName,
		RootID:     rootID,
		Strategy:   VectorIndexStrategyNativeRuntime,
	}
	registeredRuntimeStale := false
	if runtimeIdx := c.registeredVectorIndex(def.Name); runtimeIdx != nil {
		status.Registered = true
		registeredRuntimeStale = runtimeIdx.validateNativeSnapshotDefinition(def) != "" || rootID != runtimeIdx.nativeSnapshotBaseEpochForFullSave()
		if !registeredRuntimeStale {
			status.NativeRuntimeUsed = true
			status.Stats = runtimeIdx.Stats()
		}
	}
	if status.RootID == 0 && len(overlayRootIDs) == 0 {
		status.ExactFallbackReason = vectorIndexFallbackMissingGraphRoot
		status.RebuildNeeded = true
		return status, nil
	}
	if !inspectNativeRoot {
		status.NativeRootLoaded = status.RootID != 0 || len(overlayRootIDs) != 0
		status.RebuildNeeded = status.Stats.RebuildNeeded || status.Stats.SnapshotDirty
		return status, nil
	}

	snapshot, bytesDisk, reason, err := readVectorIndexNativeSnapshot(snap, catalog, rootName)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if reason != "" {
		status.ExactFallbackReason = reason
		status.RebuildNeeded = true
		return status, nil
	}
	probe, err := newVectorIndex(c, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if reason := probe.loadPersistSnapshot(snapshot); reason != "" {
		status.ExactFallbackReason = reason
		status.RebuildNeeded = true
		return status, nil
	}
	probe.recordLoadedSnapshot(status.RootID, bytesDisk)
	status.NativeRuntimeUsed = true
	status.NativeRootLoaded = true
	status.NativeRootBytes = bytesDisk
	if !status.Registered || registeredRuntimeStale {
		status.Stats = probe.Stats()
	} else if status.Stats.BytesDisk == 0 {
		status.Stats.BytesDisk = bytesDisk
	}
	status.RebuildNeeded = status.Stats.RebuildNeeded || status.Stats.SnapshotDirty
	return status, nil
}

func (c *Collection) registeredVectorIndex(name string) *VectorIndex {
	if c == nil {
		return nil
	}
	c.vectorIndexesMu.RLock()
	defer c.vectorIndexesMu.RUnlock()
	return c.vectorIndexes[name]
}
