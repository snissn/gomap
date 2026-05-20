package collections

import (
	"errors"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

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

type physicalColumnVectorGraphIndexLoader struct{}

var errColumnVectorGraphPhysicalLoadUnavailable = errors.New("collections: column graph physical load unavailable")

var defaultColumnVectorGraphIndexLoader ColumnVectorGraphIndexLoader = physicalColumnVectorGraphIndexLoader{}

func (physicalColumnVectorGraphIndexLoader) LoadColumnVectorGraphIndex(input ColumnVectorGraphIndexLoadInput) (ColumnVectorGraphIndexLoadResult, error) {
	status := VectorIndexLoadStatus{
		Strategy:                      VectorIndexStrategyColumnGraph,
		PhysicalColumnAssetsSupported: true,
		RebuildNeeded:                 true,
	}
	if input.Collection == nil {
		return ColumnVectorGraphIndexLoadResult{Status: status}, errCollectionNil
	}
	if input.Snapshot == nil || input.Collection.db == nil {
		return ColumnVectorGraphIndexLoadResult{Status: status}, errCollectionDBNil
	}
	if !input.ColumnStore.Enabled || input.ColumnStore.ActiveManifest == nil {
		return ColumnVectorGraphIndexLoadResult{Status: columnGraphUnavailableLoadStatus(vectorIndexFallbackColumnGraphPhysicalMissing)}, nil
	}
	if input.ColumnStore.PhysicalMutationParts > 0 {
		status = columnGraphPhysicalUnavailableLoadStatus(vectorIndexFallbackColumnGraphVisibility)
		return ColumnVectorGraphIndexLoadResult{Status: status}, nil
	}
	columns, reason := resolveColumnVectorGraphPhysicalColumns(input.Definition, input.ColumnStore)
	if reason != "" {
		status = columnGraphPhysicalUnavailableLoadStatus(reason)
		return ColumnVectorGraphIndexLoadResult{Status: status}, nil
	}
	catalog, err := loadCollectionCatalogWithoutColumnRootValidation(input.Snapshot, input.Collection.meta.Name)
	if err != nil {
		return ColumnVectorGraphIndexLoadResult{Status: status}, err
	}
	if catalog == nil {
		return ColumnVectorGraphIndexLoadResult{Status: status}, errCollectionNotFound
	}
	state := columnVectorGraphPhysicalLoadState{
		dimensions: input.Definition.Dimensions,
		offsets:    []uint32{0},
	}
	diag, err := input.Collection.scanColumnPhysicalRowsAtSnapshot(
		input.Snapshot,
		catalog,
		catalog.meta.Name,
		input.ColumnManifestRootID,
		input.ColumnStore,
		true,
		columnPhysicalScanRequest{
			ProjectedColumns:  []string{columns.vector, columns.invNorm, columns.neighbors},
			Visitor:           state.visit,
			RequireInsertOnly: true,
		},
	)
	status.BytesDisk = diag.PhysicalBytesScanned
	if err != nil {
		if errors.Is(err, errColumnPhysicalQueryNeedsVisibility) {
			status = columnGraphPhysicalUnavailableLoadStatus(vectorIndexFallbackColumnGraphVisibility)
			status.BytesDisk = diag.PhysicalBytesScanned
			return ColumnVectorGraphIndexLoadResult{Status: status}, nil
		}
		if errors.Is(err, errColumnVectorGraphPhysicalLoadUnavailable) {
			reason := state.unavailableReason
			if reason == "" {
				reason = vectorIndexFallbackColumnGraphInvalid
			}
			status = columnGraphPhysicalUnavailableLoadStatus(reason)
			status.BytesDisk = diag.PhysicalBytesScanned
			return ColumnVectorGraphIndexLoadResult{Status: status}, nil
		}
		return ColumnVectorGraphIndexLoadResult{Status: status}, err
	}
	if len(state.documentIDs) == 0 {
		status = columnGraphPhysicalUnavailableLoadStatus(vectorIndexFallbackColumnGraphEmpty)
		status.BytesDisk = diag.PhysicalBytesScanned
		return ColumnVectorGraphIndexLoadResult{Status: status}, nil
	}
	graph, err := NewColumnVectorGraphFromColumns(ColumnVectorGraphColumns{
		DocumentIDs:     state.documentIDs,
		Vectors:         state.vectors,
		InvNorms:        state.invNorms,
		NeighborOffsets: state.offsets,
		Neighbors:       state.neighbors,
		Dimensions:      input.Definition.Dimensions,
		EntryPoint:      0,
		EfSearch:        input.Definition.EfSearch,
	})
	if err != nil {
		status = columnGraphPhysicalUnavailableLoadStatus(vectorIndexFallbackColumnGraphInvalid)
		status.BytesDisk = diag.PhysicalBytesScanned
		return ColumnVectorGraphIndexLoadResult{Status: status}, nil
	}
	status.Loaded = true
	status.ColumnGraphLoaded = true
	status.RebuildNeeded = false
	return ColumnVectorGraphIndexLoadResult{Graph: graph, Status: status}, nil
}

type columnVectorGraphPhysicalColumns struct {
	vector    string
	invNorm   string
	neighbors string
}

func resolveColumnVectorGraphPhysicalColumns(def VectorIndexDefinition, cfg ColumnStoreConfig) (columnVectorGraphPhysicalColumns, string) {
	if def.Dimensions <= 0 {
		return columnVectorGraphPhysicalColumns{}, vectorIndexFallbackColumnGraphSchema
	}
	vectorName, ok := findColumnVectorGraphPhysicalColumn(cfg, ColumnStoreValueFloat32Vector, def.Dimensions, columnGraphVectorColumnCandidates(def)...)
	if !ok {
		return columnVectorGraphPhysicalColumns{}, vectorIndexFallbackColumnGraphSchema
	}
	invNormName, ok := findColumnVectorGraphPhysicalColumn(cfg, ColumnStoreValueFloat32, 0, columnGraphSideColumnCandidates(def, vectorName, "_inv_norm")...)
	if !ok {
		return columnVectorGraphPhysicalColumns{}, vectorIndexFallbackColumnGraphSchema
	}
	neighborsName, ok := findColumnVectorGraphPhysicalColumn(cfg, ColumnStoreValueAdjacencyList, 0, columnGraphSideColumnCandidates(def, vectorName, "_neighbors")...)
	if !ok {
		return columnVectorGraphPhysicalColumns{}, vectorIndexFallbackColumnGraphSchema
	}
	return columnVectorGraphPhysicalColumns{
		vector:    vectorName,
		invNorm:   invNormName,
		neighbors: neighborsName,
	}, ""
}

func findColumnVectorGraphPhysicalColumn(cfg ColumnStoreConfig, valueType ColumnStoreValueType, vectorDims int, candidates ...string) (string, bool) {
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		for _, col := range cfg.Columns {
			if col.Name != candidate && col.Path != candidate {
				continue
			}
			if col.ValueType != valueType {
				continue
			}
			if valueType == ColumnStoreValueFloat32Vector && col.VectorDims != vectorDims {
				continue
			}
			return col.Name, true
		}
	}
	return "", false
}

func columnGraphVectorColumnCandidates(def VectorIndexDefinition) []string {
	var candidates []string
	candidates = appendUniqueColumnGraphCandidate(candidates, def.Field)
	candidates = appendUniqueColumnGraphCandidate(candidates, def.Name)
	return candidates
}

func columnGraphSideColumnCandidates(def VectorIndexDefinition, vectorName string, suffix string) []string {
	var candidates []string
	candidates = appendUniqueColumnGraphCandidate(candidates, def.Name+suffix)
	candidates = appendUniqueColumnGraphCandidate(candidates, def.Field+suffix)
	candidates = appendUniqueColumnGraphCandidate(candidates, vectorName+suffix)
	return candidates
}

func appendUniqueColumnGraphCandidate(candidates []string, candidate string) []string {
	if candidate == "" {
		return candidates
	}
	for _, existing := range candidates {
		if existing == candidate {
			return candidates
		}
	}
	return append(candidates, candidate)
}

type columnVectorGraphPhysicalLoadState struct {
	dimensions        int
	documentIDs       [][]byte
	vectors           []float32
	invNorms          []float32
	offsets           []uint32
	neighbors         []uint32
	unavailableReason string
}

func (s *columnVectorGraphPhysicalLoadState) visit(row columnPhysicalScanRowView) error {
	if row.Deleted || row.Operation != ColumnPublishOperationInsert {
		return s.fail(vectorIndexFallbackColumnGraphVisibility, "row requires mutation visibility")
	}
	if len(row.Values) != 3 {
		return s.fail(vectorIndexFallbackColumnGraphInvalid, "projected value count mismatch")
	}
	vector := row.Values[0]
	if vector.Type != ColumnStoreValueFloat32Vector || !vector.Present || vector.Null || len(vector.Float32Vector) != s.dimensions {
		return s.fail(vectorIndexFallbackColumnGraphInvalid, "invalid vector column value")
	}
	invNorm := row.Values[1]
	if invNorm.Type != ColumnStoreValueFloat32 || !invNorm.Present || invNorm.Null {
		return s.fail(vectorIndexFallbackColumnGraphInvalid, "invalid inverse norm column value")
	}
	adjacency := row.Values[2]
	if adjacency.Type != ColumnStoreValueAdjacencyList || !adjacency.Present || adjacency.Null {
		return s.fail(vectorIndexFallbackColumnGraphInvalid, "invalid adjacency column value")
	}
	if uint64(len(s.documentIDs)) >= columnVectorGraphMaxUint32 {
		return s.fail(vectorIndexFallbackColumnGraphInvalid, "row ordinal space exhausted")
	}
	if uint64(len(s.neighbors))+uint64(len(adjacency.AdjacencyList)) > columnVectorGraphMaxUint32 {
		return s.fail(vectorIndexFallbackColumnGraphInvalid, "neighbor ordinal space exhausted")
	}
	s.documentIDs = append(s.documentIDs, append([]byte(nil), row.ID...))
	s.vectors = append(s.vectors, vector.Float32Vector...)
	s.invNorms = append(s.invNorms, invNorm.Float32)
	s.neighbors = append(s.neighbors, adjacency.AdjacencyList...)
	s.offsets = append(s.offsets, uint32(len(s.neighbors)))
	return nil
}

func (s *columnVectorGraphPhysicalLoadState) fail(reason string, detail string) error {
	if s.unavailableReason == "" {
		s.unavailableReason = reason
	}
	if detail == "" {
		return errColumnVectorGraphPhysicalLoadUnavailable
	}
	return fmt.Errorf("%w: %s", errColumnVectorGraphPhysicalLoadUnavailable, detail)
}

// LoadColumnGraphVectorIndexSnapshot loads an explicit column_graph vector
// index through the column-backed graph seam. The default loader scans
// projected vector, inverse-norm, and adjacency physical columns into an
// immutable ColumnVectorGraph without fetching full documents. Mutation-bearing
// manifests remain rebuild-needed until dynamic overlay maintenance lands.
func (c *Collection) LoadColumnGraphVectorIndexSnapshot(opts VectorIndexOptions) (*ColumnVectorGraph, VectorIndexLoadStatus, error) {
	return c.loadColumnGraphVectorIndexSnapshot(opts, defaultColumnVectorGraphIndexLoader)
}

func (c *Collection) loadColumnGraphVectorIndexSnapshot(opts VectorIndexOptions, loader ColumnVectorGraphIndexLoader) (*ColumnVectorGraph, VectorIndexLoadStatus, error) {
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
	return c.loadColumnGraphVectorIndexSnapshotFromCatalogWithLoader(snap, catalog, def, loader)
}

func (c *Collection) loadColumnGraphVectorIndexSnapshotFromCatalog(snap *backenddb.Snapshot, catalog *collectionCatalog, def VectorIndexDefinition) (*ColumnVectorGraph, VectorIndexLoadStatus, error) {
	return c.loadColumnGraphVectorIndexSnapshotFromCatalogWithLoader(snap, catalog, def, defaultColumnVectorGraphIndexLoader)
}

func (c *Collection) loadColumnGraphVectorIndexSnapshotFromCatalogWithLoader(snap *backenddb.Snapshot, catalog *collectionCatalog, def VectorIndexDefinition, loader ColumnVectorGraphIndexLoader) (*ColumnVectorGraph, VectorIndexLoadStatus, error) {
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
	if err := validateColumnStoreCatalogRoot(snap, catalog); err != nil {
		reason := columnGraphCatalogRootUnavailableReason(err)
		if reason == "" {
			return nil, status, err
		}
		// Report manifest/root mismatches as explicit status so operational
		// callers can show rebuild-needed state instead of failing discovery.
		setColumnGraphUnavailable(&status, reason)
		return nil, status, nil
	}
	if loader == nil {
		loader = unsupportedColumnVectorGraphIndexLoader{}
	}

	result, err := loader.LoadColumnVectorGraphIndex(ColumnVectorGraphIndexLoadInput{
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

func columnGraphPhysicalUnavailableLoadStatus(reason string) VectorIndexLoadStatus {
	status := VectorIndexLoadStatus{
		Strategy:                      VectorIndexStrategyColumnGraph,
		PhysicalColumnAssetsSupported: true,
		RebuildNeeded:                 true,
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
	if reason == vectorIndexFallbackColumnGraphPhysicalMissing {
		status.PhysicalColumnAssetsSupported = false
	}
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
		supported := status.PhysicalColumnAssetsSupported || update.PhysicalColumnAssetsSupported
		setColumnGraphUnavailable(status, reason)
		if reason != vectorIndexFallbackColumnGraphPhysicalMissing {
			status.PhysicalColumnAssetsSupported = supported
		}
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

func columnGraphCatalogRootUnavailableReason(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, errColumnManifestIdentityMissing) ||
		errors.Is(err, errColumnManifestIdentityMalformed) ||
		errors.Is(err, errColumnManifestIdentityBadMagic) ||
		errors.Is(err, errColumnManifestIdentityUnsupportedVersion) ||
		errors.Is(err, errColumnManifestIdentityNonZeroReserved) ||
		errors.Is(err, errColumnManifestRootDescriptorMissing) ||
		errors.Is(err, errColumnManifestIdentityDeleted) ||
		errors.Is(err, errColumnManifestIdentityInvalid) ||
		errors.Is(err, errColumnManifestIdentityMismatch) {
		return vectorIndexFallbackColumnGraphManifestRootMismatch
	}
	return ""
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
