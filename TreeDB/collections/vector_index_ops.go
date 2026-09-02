package collections

import (
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// VectorIndexStatus reports the operational state of one declared collection
// vector index. Status checks may inspect the persisted graph root and are
// intended for operational paths, not search hot paths.
type VectorIndexStatus struct {
	Definition          VectorIndexDefinition
	Name                string
	Strategy            VectorIndexStrategy
	State               VectorIndexState
	Reason              VectorIndexReason
	Loaded              bool
	RootName            string
	RootID              uint64
	NativeRootLoaded    bool
	NativeRootBytes     int64
	ExactFallbackReason string
	Registered          bool
	Stats               VectorIndexStats
	RebuildNeeded       bool
	Duration            time.Duration
	ColumnGraphBuild    ColumnGraphBuildTiming
}

// ColumnGraphBuildTiming records completed named rebuild stages.
// Asset preparation and publication overlap because the command-WAL publisher
// invokes the preparation callback; sync durations are nested producer work.
// It is populated only for a successful column_graph rebuild.
type ColumnGraphBuildTiming struct {
	Total                     time.Duration
	Snapshot                  time.Duration
	RowExtraction             time.Duration
	AdjacencyBuild            time.Duration
	LocalityRemap             time.Duration
	AssetPreparation          time.Duration
	InvNormPreparation        time.Duration
	AdjacencyStatePreparation time.Duration
	RowRefPreparation         time.Duration
	DocumentIDPreparation     time.Duration
	QuantizedPreparation      time.Duration
	SearchPackPreparation     time.Duration
	ManifestFinalization      time.Duration
	FileSync                  time.Duration
	FileSyncCount             uint64
	NamespaceSync             time.Duration
	NamespaceSyncCount        uint64
	Publication               time.Duration
	// ConstructionDecisions is present only when the owning CollectionManager
	// enabled bounded construction diagnostics for this rebuild.
	ConstructionDecisions *VectorIndexConstructionDecisionSnapshot
}

// VectorIndexStatus returns persisted-root and runtime status for a declared
// vector index.
func (c *Collection) VectorIndexStatus(name string) (VectorIndexStatus, error) {
	status, err := c.vectorIndexStatus(name, true)
	if err != nil {
		return status, err
	}
	return status, nil
}

// RebuildVectorIndex scans canonical collection documents, rebuilds the
// declared HNSW graph, and publishes a full native vector-index root. It is an
// operational maintenance call: collection writes wait while the rebuild scans
// and publishes so the replacement graph cannot miss committed mutations.
func (c *Collection) RebuildVectorIndex(name string) (VectorIndexStatus, error) {
	return c.rebuildVectorIndexWithCommandWALIntent(name, nil)
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
	snap.MarkForegroundRead()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
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
	if def.Strategy == VectorIndexStrategyColumnGraph {
		return c.columnGraphVectorIndexStatusAtSnapshot(def.Name, snap)
	}

	rootName := collectionVectorIndexRootName(catalog.meta.Name, def.Name)
	rootID, overlayRootIDs := vectorIndexStatusRootID(catalog, rootName)
	status := VectorIndexStatus{
		Definition: def,
		Name:       def.Name,
		Strategy:   def.Strategy,
		RootName:   rootName,
		RootID:     rootID,
	}
	if def.Strategy == VectorIndexStrategyNativeRuntime {
		status.State = VectorIndexStateNativeRuntime
		status.Reason = VectorIndexReasonNativeRuntime
	}
	registeredRuntimeStale := false
	if runtimeIdx := c.registeredVectorIndex(def.Name); runtimeIdx != nil {
		status.Registered = true
		registeredRuntimeStale = runtimeIdx.validateNativeSnapshotDefinition(def) != "" || rootID != runtimeIdx.nativeSnapshotBaseEpochForFullSave()
		if !registeredRuntimeStale {
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
	matchesDocumentRoots, err := vectorIndexSnapshotMatchesDocumentRoots(snapshot.Meta, catalog, snap)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if !matchesDocumentRoots {
		status.ExactFallbackReason = vectorIndexFallbackStaleDocumentRoot
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

func vectorIndexStatusRootID(catalog *collectionCatalog, rootName string) (uint64, []uint64) {
	rootID := catalog.rootID(rootName)
	overlayRootIDs := catalog.overlayRootIDs(rootName)
	if rootID == 0 && len(overlayRootIDs) != 0 {
		rootID = overlayRootIDs[0]
	}
	return rootID, overlayRootIDs
}

func (c *Collection) registeredVectorIndex(name string) *VectorIndex {
	if c == nil {
		return nil
	}
	if c.writeDomain != nil {
		c.writeDomain.nativeVectorIndexesMu.RLock()
		index := c.writeDomain.nativeVectorIndexes[name]
		c.writeDomain.nativeVectorIndexesMu.RUnlock()
		if index != nil {
			return index
		}
	}
	c.vectorIndexesMu.RLock()
	defer c.vectorIndexesMu.RUnlock()
	return c.vectorIndexes[name]
}
