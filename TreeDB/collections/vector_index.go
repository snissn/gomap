package collections

import (
	"fmt"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

const (
	defaultVectorIndexM              = 16
	defaultVectorIndexEfConstruction = 128
	defaultVectorIndexEfSearch       = 128
)

type VectorMetric string

const (
	VectorMetricCosine VectorMetric = "cosine"
)

type VectorIndexEncoding string

const (
	VectorIndexEncodingFloat32 VectorIndexEncoding = "float32"
)

type VectorIndexStrategy string

const (
	VectorIndexStrategyNativeRuntime VectorIndexStrategy = "native_runtime"
	// VectorIndexStrategyColumnGraph selects the physical column-store graph path.
	// Until graph assets are built and published, status must report unavailable
	// or rebuild-needed rather than falling back to a decoded in-memory graph.
	VectorIndexStrategyColumnGraph VectorIndexStrategy = "column_graph"
)

type VectorIndexState string

const (
	VectorIndexStateNativeRuntime            VectorIndexState = "native_runtime"
	VectorIndexStateColumnGraphLoaded        VectorIndexState = "column_graph_loaded"
	VectorIndexStateColumnGraphUnavailable   VectorIndexState = "column_graph_unavailable"
	VectorIndexStateColumnGraphRebuildNeeded VectorIndexState = "column_graph_rebuild_needed"
)

type VectorIndexReason string

const (
	VectorIndexReasonNativeRuntime                     VectorIndexReason = "native_runtime_index"
	VectorIndexReasonColumnGraphRebuildNeeded          VectorIndexReason = "column_graph_rebuild_needed"
	VectorIndexReasonPhysicalColumnAssetSupportMissing VectorIndexReason = "physical_column_asset_support_missing"
	VectorIndexReasonColumnGraphAssetMismatch          VectorIndexReason = "column_graph_asset_mismatch"
	VectorIndexReasonColumnGraphCorrupt                VectorIndexReason = "column_graph_corrupt"
)

type VectorIndexDefinition struct {
	Name           string              `json:"name"`
	Field          string              `json:"field"`
	Metric         VectorMetric        `json:"metric,omitempty"`
	Dimensions     int                 `json:"dimensions"`
	M              int                 `json:"m,omitempty"`
	EfConstruction int                 `json:"ef_construction,omitempty"`
	EfSearch       int                 `json:"ef_search,omitempty"`
	Encoding       VectorIndexEncoding `json:"encoding,omitempty"`
	Strategy       VectorIndexStrategy `json:"strategy,omitempty"`
	StoragePolicy  RootStoragePolicy   `json:"storage_policy,omitempty"`
}

type VectorIndexStatus struct {
	Name          string              `json:"name"`
	Strategy      VectorIndexStrategy `json:"strategy"`
	State         VectorIndexState    `json:"state"`
	Reason        VectorIndexReason   `json:"reason,omitempty"`
	Loaded        bool                `json:"loaded,omitempty"`
	RebuildNeeded bool                `json:"rebuild_needed,omitempty"`
}

func (c *Collection) CreateVectorIndex(def VectorIndexDefinition) (*CollectionMeta, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if c.db.CommandWALEnabled() {
		return nil, fmt.Errorf("%w: collection catalog vector index mutation is rejected under command_wal_v1 until catalog vector index commands are supported", backenddb.ErrCommandWALRejected)
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWrites(); err != nil {
		return nil, err
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	_ = snap.Close()
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
	c.meta = baseMeta
	newMeta, _, err := addVectorIndexToCollectionMeta(baseMeta, def)
	if err != nil {
		return nil, err
	}
	encodedMeta, err := encodeCollectionMeta(newMeta)
	if err != nil {
		return nil, err
	}
	newSystemRoot, _, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return c.buildSchemaOnlySystemDeltaIterator(baseMeta, encodedMeta, nil)
	})
	if err != nil {
		return nil, err
	}
	c.meta = newMeta
	nextCatalog := cloneCatalogWithRootUpdates(catalog, newMeta, nil, nil)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return newMeta.copy(), nil
}

func (c *Collection) DropVectorIndex(name string) (*CollectionMeta, error) {
	if err := ValidateIndexName(name); err != nil {
		return nil, err
	}
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if c.db.CommandWALEnabled() {
		return nil, fmt.Errorf("%w: collection catalog vector index mutation is rejected under command_wal_v1 until catalog vector index commands are supported", backenddb.ErrCommandWALRejected)
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWrites(); err != nil {
		return nil, err
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	_ = snap.Close()
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
	c.meta = baseMeta

	nextVectorIndexes := make([]VectorIndexDefinition, 0, len(baseMeta.VectorIndexes))
	dropped := false
	for _, idx := range baseMeta.VectorIndexes {
		if idx.Name == name {
			dropped = true
			continue
		}
		nextVectorIndexes = append(nextVectorIndexes, idx)
	}
	if !dropped {
		return nil, ErrIndexNotFound
	}
	newMeta, err := normalizeCollectionMeta(CollectionMeta{
		Name:          baseMeta.Name,
		Options:       baseMeta.Options,
		Indexes:       baseMeta.Indexes,
		VectorIndexes: nextVectorIndexes,
	})
	if err != nil {
		return nil, err
	}
	encodedMeta, err := encodeCollectionMeta(newMeta)
	if err != nil {
		return nil, err
	}
	newSystemRoot, _, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return c.buildSchemaOnlySystemDeltaIterator(baseMeta, encodedMeta, nil)
	})
	if err != nil {
		return nil, err
	}
	c.meta = newMeta
	nextCatalog := cloneCatalogWithRootUpdates(catalog, newMeta, nil, nil)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return newMeta.copy(), nil
}

func (c *Collection) VectorIndexStatus(name string) (VectorIndexStatus, error) {
	if err := ValidateIndexName(name); err != nil {
		return VectorIndexStatus{}, err
	}
	if c == nil {
		return VectorIndexStatus{}, errCollectionNil
	}
	if c.db == nil {
		return VectorIndexStatus{}, errCollectionDBNil
	}
	def, ok := findVectorIndex(c.meta.VectorIndexes, name)
	if !ok {
		return VectorIndexStatus{}, ErrIndexNotFound
	}
	status := VectorIndexStatus{
		Name:     def.Name,
		Strategy: def.Strategy,
	}
	switch def.Strategy {
	case VectorIndexStrategyNativeRuntime:
		status.State = VectorIndexStateNativeRuntime
		status.Reason = VectorIndexReasonNativeRuntime
		return status, nil
	case VectorIndexStrategyColumnGraph:
		cfg := c.meta.Options.ColumnStore
		if cfg == nil || !cfg.Enabled {
			status.State = VectorIndexStateColumnGraphUnavailable
			status.Reason = VectorIndexReasonPhysicalColumnAssetSupportMissing
			status.RebuildNeeded = true
			return status, nil
		}
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphRebuildNeeded
		status.RebuildNeeded = true
		return status, nil
	default:
		return VectorIndexStatus{}, fmt.Errorf("collections: unsupported vector index strategy %q", def.Strategy)
	}
}

func addVectorIndexToCollectionMeta(meta CollectionMeta, def VectorIndexDefinition) (CollectionMeta, VectorIndexDefinition, error) {
	normalizedDef, err := normalizeVectorIndexDefinition(def)
	if err != nil {
		return CollectionMeta{}, VectorIndexDefinition{}, err
	}
	if _, ok := findVectorIndex(meta.VectorIndexes, normalizedDef.Name); ok {
		return CollectionMeta{}, VectorIndexDefinition{}, fmt.Errorf("collections: duplicate vector index %q", normalizedDef.Name)
	}
	if _, ok := findIndex(meta.Indexes, normalizedDef.Name); ok {
		return CollectionMeta{}, VectorIndexDefinition{}, fmt.Errorf("collections: duplicate index %q", normalizedDef.Name)
	}
	candidate := CollectionMeta{
		Name:          meta.Name,
		Options:       meta.Options,
		Indexes:       append([]IndexDefinition(nil), meta.Indexes...),
		VectorIndexes: append(append([]VectorIndexDefinition(nil), meta.VectorIndexes...), normalizedDef),
	}
	normalized, err := normalizeCollectionMeta(candidate)
	if err != nil {
		return CollectionMeta{}, VectorIndexDefinition{}, err
	}
	out, ok := findVectorIndex(normalized.VectorIndexes, normalizedDef.Name)
	if !ok {
		return CollectionMeta{}, VectorIndexDefinition{}, fmt.Errorf("collections: normalized vector index %q not found", normalizedDef.Name)
	}
	return normalized, out, nil
}

func normalizeVectorIndexDefinitions(defs []VectorIndexDefinition, seen map[string]struct{}) ([]VectorIndexDefinition, error) {
	if len(defs) == 0 {
		return nil, nil
	}
	out := append([]VectorIndexDefinition(nil), defs...)
	for i := range out {
		def, err := normalizeVectorIndexDefinition(out[i])
		if err != nil {
			return nil, err
		}
		if _, ok := seen[def.Name]; ok {
			return nil, fmt.Errorf("collections: duplicate index %q", def.Name)
		}
		seen[def.Name] = struct{}{}
		out[i] = def
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func normalizeVectorIndexDefinition(def VectorIndexDefinition) (VectorIndexDefinition, error) {
	if def.Name == "" {
		def.Name = def.Field
	}
	if err := ValidateIndexName(def.Name); err != nil {
		return VectorIndexDefinition{}, fmt.Errorf("collections: invalid vector index name %q: %w", def.Name, err)
	}
	if err := ValidateIndexPath(def.Field); err != nil {
		return VectorIndexDefinition{}, fmt.Errorf("collections: invalid vector index %q field: %w", def.Name, err)
	}
	if def.Dimensions <= 0 {
		return VectorIndexDefinition{}, fmt.Errorf("collections: invalid vector index %q dimensions: must be positive", def.Name)
	}
	metric, err := normalizeVectorMetric(def.Metric)
	if err != nil {
		return VectorIndexDefinition{}, fmt.Errorf("collections: invalid vector index %q metric: %w", def.Name, err)
	}
	def.Metric = metric
	encoding, err := normalizeVectorIndexEncoding(def.Encoding)
	if err != nil {
		return VectorIndexDefinition{}, fmt.Errorf("collections: invalid vector index %q encoding: %w", def.Name, err)
	}
	def.Encoding = encoding
	strategy, err := normalizeVectorIndexStrategy(def.Strategy)
	if err != nil {
		return VectorIndexDefinition{}, fmt.Errorf("collections: invalid vector index %q strategy: %w", def.Name, err)
	}
	def.Strategy = strategy
	if def.M < 0 || def.EfConstruction < 0 || def.EfSearch < 0 {
		return VectorIndexDefinition{}, fmt.Errorf("collections: invalid vector index %q build/search parameters: values cannot be negative", def.Name)
	}
	if def.M == 0 {
		def.M = defaultVectorIndexM
	}
	if def.EfConstruction == 0 {
		def.EfConstruction = defaultVectorIndexEfConstruction
	}
	if def.EfSearch == 0 {
		def.EfSearch = defaultVectorIndexEfSearch
	}
	if _, err := backendRootStoragePolicy(def.StoragePolicy); err != nil {
		return VectorIndexDefinition{}, err
	}
	return def, nil
}

func normalizeVectorMetric(metric VectorMetric) (VectorMetric, error) {
	switch metric {
	case "", VectorMetricCosine:
		return VectorMetricCosine, nil
	default:
		return "", fmt.Errorf("unsupported metric %q", metric)
	}
}

func normalizeVectorIndexEncoding(encoding VectorIndexEncoding) (VectorIndexEncoding, error) {
	switch encoding {
	case "", VectorIndexEncodingFloat32:
		return VectorIndexEncodingFloat32, nil
	default:
		return "", fmt.Errorf("unsupported encoding %q", encoding)
	}
}

func normalizeVectorIndexStrategy(strategy VectorIndexStrategy) (VectorIndexStrategy, error) {
	switch strategy {
	case "":
		return VectorIndexStrategyNativeRuntime, nil
	case VectorIndexStrategyNativeRuntime, VectorIndexStrategyColumnGraph:
		return strategy, nil
	default:
		return "", fmt.Errorf("unsupported strategy %q", strategy)
	}
}

func findVectorIndex(indexes []VectorIndexDefinition, name string) (VectorIndexDefinition, bool) {
	for _, idx := range indexes {
		if idx.Name == name {
			return idx, true
		}
	}
	return VectorIndexDefinition{}, false
}
