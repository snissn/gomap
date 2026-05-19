package collections

import backenddb "github.com/snissn/gomap/TreeDB/db"

// ColumnVectorGraphIndexLoader is the product-contract boundary for the future
// physical column asset implementation. The loader must read vector, invNorm,
// and adjacency-list column assets referenced by the collection column manifest
// and return an immutable ColumnVectorGraph. It must not invent a vector-only
// sidecar format.
type ColumnVectorGraphIndexLoader interface {
	LoadColumnVectorGraphIndex(ColumnVectorGraphIndexLoadInput) (ColumnVectorGraphIndexLoadResult, error)
}

type ColumnVectorGraphIndexLoadInput struct {
	Collection             *Collection
	Snapshot               *backenddb.Snapshot
	Definition             VectorIndexDefinition
	ColumnStore            ColumnStoreConfig
	ColumnManifestRootName string
	ColumnManifestRootID   uint64
	ActiveManifest         *ColumnManifestIdentity
}

type ColumnVectorGraphIndexLoadResult struct {
	Graph  *ColumnVectorGraph
	Status VectorIndexLoadStatus
}

type unsupportedColumnVectorGraphIndexLoader struct{}

func (unsupportedColumnVectorGraphIndexLoader) LoadColumnVectorGraphIndex(ColumnVectorGraphIndexLoadInput) (ColumnVectorGraphIndexLoadResult, error) {
	return ColumnVectorGraphIndexLoadResult{
		Status: columnGraphUnavailableLoadStatus(vectorIndexFallbackColumnGraphPhysicalMissing),
	}, nil
}

var defaultColumnVectorGraphIndexLoader ColumnVectorGraphIndexLoader = unsupportedColumnVectorGraphIndexLoader{}

// LoadColumnGraphVectorIndexSnapshot loads an explicit column_graph vector
// index through the column-backed graph seam. Until physical column assets can
// be scanned into ColumnVectorGraphColumns, this reports a precise unavailable
// status instead of falling back to the native runtime graph.
func (c *Collection) LoadColumnGraphVectorIndexSnapshot(opts VectorIndexOptions) (*ColumnVectorGraph, VectorIndexLoadStatus, error) {
	status := VectorIndexLoadStatus{Strategy: VectorIndexStrategyColumnGraph, RebuildNeeded: true}
	if c == nil {
		return nil, status, errCollectionNil
	}
	if c.db == nil {
		return nil, status, errCollectionDBNil
	}
	name := vectorIndexOptionName(opts)
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, status, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalogWithoutColumnRootValidation(snap, c.meta.Name)
	if err != nil {
		return nil, status, err
	}
	if catalog == nil {
		return nil, status, errCollectionNotFound
	}
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, name)
	if !ok {
		status = columnGraphUnavailableLoadStatus(vectorIndexFallbackMissingVectorIndexMetadata)
		return nil, status, nil
	}
	return c.loadColumnGraphVectorIndexSnapshotFromCatalog(snap, catalog, def)
}

func (c *Collection) loadColumnGraphVectorIndexSnapshotFromCatalog(snap *backenddb.Snapshot, catalog *collectionCatalog, def VectorIndexDefinition) (*ColumnVectorGraph, VectorIndexLoadStatus, error) {
	status := baseColumnGraphLoadStatus(catalog)
	if vectorIndexDefinitionStrategy(def) != VectorIndexStrategyColumnGraph {
		status = columnGraphUnavailableLoadStatus(vectorIndexFallbackColumnGraphStrategyMissing)
		status.RootName = collectionColumnManifestRootName(catalog.meta.Name)
		status.RootID = catalog.rootID(status.RootName)
		status.Epoch = status.RootID
		return nil, status, nil
	}
	if def.Metric != VectorMetricCosine {
		setColumnGraphUnavailable(&status, vectorIndexFallbackColumnGraphMetric)
		return nil, status, nil
	}
	if def.Encoding != VectorIndexEncodingFloat32 {
		setColumnGraphUnavailable(&status, vectorIndexFallbackColumnGraphEncoding)
		return nil, status, nil
	}
	cfg := catalog.meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.ActiveManifest == nil {
		setColumnGraphUnavailable(&status, vectorIndexFallbackColumnGraphPhysicalMissing)
		return nil, status, nil
	}
	if status.RootID == 0 {
		setColumnGraphUnavailable(&status, vectorIndexFallbackColumnGraphManifestMissing)
		return nil, status, nil
	}
	if validateColumnStoreCatalogRoot(snap, catalog) != nil {
		// Report manifest/root mismatches as explicit status so operational
		// callers can show rebuild-needed state instead of failing discovery.
		setColumnGraphUnavailable(&status, vectorIndexFallbackColumnGraphManifestInvalid)
		return nil, status, nil
	}

	result, err := defaultColumnVectorGraphIndexLoader.LoadColumnVectorGraphIndex(ColumnVectorGraphIndexLoadInput{
		Collection:             c,
		Snapshot:               snap,
		Definition:             def,
		ColumnStore:            *cfg,
		ColumnManifestRootName: status.RootName,
		ColumnManifestRootID:   status.RootID,
		ActiveManifest:         cloneColumnManifestIdentityPtr(cfg.ActiveManifest),
	})
	if err != nil {
		return nil, status, err
	}
	mergeColumnGraphLoadStatus(&status, result.Status)
	if result.Graph == nil {
		if status.ExactFallbackReason == "" {
			setColumnGraphUnavailable(&status, vectorIndexFallbackColumnGraphPhysicalMissing)
		}
		return nil, status, nil
	}
	status.Loaded = true
	status.ColumnGraphLoaded = true
	status.ColumnGraphUnavailableReason = ""
	status.ExactFallbackReason = ""
	status.PhysicalColumnAssetsSupported = true
	status.RebuildNeeded = false
	return result.Graph, status, nil
}

func baseColumnGraphLoadStatus(catalog *collectionCatalog) VectorIndexLoadStatus {
	rootName := collectionColumnManifestRootName(catalog.meta.Name)
	rootID := catalog.rootID(rootName)
	return VectorIndexLoadStatus{
		Strategy:      VectorIndexStrategyColumnGraph,
		RootName:      rootName,
		RootID:        rootID,
		Epoch:         rootID,
		RebuildNeeded: true,
	}
}

func columnGraphUnavailableLoadStatus(reason string) VectorIndexLoadStatus {
	status := VectorIndexLoadStatus{
		Strategy:      VectorIndexStrategyColumnGraph,
		RebuildNeeded: true,
	}
	setColumnGraphUnavailable(&status, reason)
	return status
}

func setColumnGraphUnavailable(status *VectorIndexLoadStatus, reason string) {
	if status == nil {
		return
	}
	status.Loaded = false
	status.ColumnGraphLoaded = false
	status.PhysicalColumnAssetsSupported = false
	status.ExactFallbackReason = reason
	status.ColumnGraphUnavailableReason = reason
	status.RebuildNeeded = true
}

func mergeColumnGraphLoadStatus(status *VectorIndexLoadStatus, update VectorIndexLoadStatus) {
	if status == nil {
		return
	}
	reason := update.ColumnGraphUnavailableReason
	if reason == "" {
		reason = update.ExactFallbackReason
	}
	if reason != "" {
		setColumnGraphUnavailable(status, reason)
		if update.BytesDisk != 0 {
			status.BytesDisk = update.BytesDisk
		}
		return
	}
	if update.PhysicalColumnAssetsSupported {
		status.PhysicalColumnAssetsSupported = true
	}
	if update.BytesDisk != 0 {
		status.BytesDisk = update.BytesDisk
	}
}

func (c *Collection) columnGraphVectorIndexStatusFromCatalog(snap *backenddb.Snapshot, catalog *collectionCatalog, def VectorIndexDefinition) (VectorIndexStatus, error) {
	_, loadStatus, err := c.loadColumnGraphVectorIndexSnapshotFromCatalog(snap, catalog, def)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	return vectorIndexStatusFromColumnGraphLoad(def, loadStatus), nil
}

func vectorIndexStatusFromColumnGraphLoad(def VectorIndexDefinition, loadStatus VectorIndexLoadStatus) VectorIndexStatus {
	return VectorIndexStatus{
		Definition:                    def,
		Name:                          def.Name,
		RootName:                      loadStatus.RootName,
		RootID:                        loadStatus.RootID,
		Strategy:                      VectorIndexStrategyColumnGraph,
		ExactFallbackReason:           loadStatus.ExactFallbackReason,
		ColumnGraphLoaded:             loadStatus.ColumnGraphLoaded,
		ColumnGraphUnavailableReason:  loadStatus.ColumnGraphUnavailableReason,
		PhysicalColumnAssetsSupported: loadStatus.PhysicalColumnAssetsSupported,
		RebuildNeeded:                 loadStatus.RebuildNeeded || !loadStatus.ColumnGraphLoaded || loadStatus.ExactFallbackReason != "",
	}
}

func columnGraphRebuildUnsupportedStatus(def VectorIndexDefinition, loadStatus VectorIndexLoadStatus) VectorIndexStatus {
	status := vectorIndexStatusFromColumnGraphLoad(def, loadStatus)
	if status.ExactFallbackReason == "" {
		status.ExactFallbackReason = vectorIndexFallbackColumnGraphPhysicalMissing
		status.ColumnGraphUnavailableReason = vectorIndexFallbackColumnGraphPhysicalMissing
	}
	status.RebuildNeeded = true
	return status
}
