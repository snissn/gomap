package collections

import (
	"errors"
	"fmt"
	"math"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type columnVectorGraphPhysicalRowReaderOptions struct {
	// MaxDecodedBlocks is the maximum number of decoded column blocks retained
	// in the reader cache. Zero uses the underlying row reader default.
	MaxDecodedBlocks int
	// detachCatalog copies immutable catalog metadata for readers returned after
	// their assembly snapshot closes.
	detachCatalog bool
}

const (
	// These positions must match the ProjectedColumns order in
	// openColumnVectorGraphPhysicalRowReader.
	columnVectorGraphPhysicalRowValueVector = iota
	columnVectorGraphPhysicalRowValueInvNorm
	columnVectorGraphPhysicalRowValueAdjacency
	columnVectorGraphPhysicalRowValueCount
)

var (
	errColumnVectorGraphAdjacencyOrdinalOutOfBounds = errors.New("column_graph adjacency ordinal outside row_count")
	errColumnVectorGraphManifestMismatch            = errors.New("column_graph manifest mismatch")
	errNilColumnVectorGraphPhysicalRowReader        = errors.New("collections: nil column vector graph physical row reader")
)

// columnVectorGraphPhysicalRowReader fetches graph rows from the persisted
// column graph asset by ordinal. It is a graph-specific wrapper around the
// generic physical row reader, not a decoded ColumnVectorGraph.
//
// The reader is not concurrency-safe. Parallel native search uses one reader
// and one scratch per worker over immutable physical graph assets.
type columnVectorGraphPhysicalRowReader struct {
	def     VectorIndexDefinition
	graph   columnVectorGraphManifestSnapshot
	catalog *collectionCatalog
	reader  *columnPhysicalRowReader
}

// columnVectorGraphPhysicalRow aliases caller-owned scratch and cached asset
// bytes. Copy ID, Vector, or Adjacency before the next fetch if retention is
// required.
type columnVectorGraphPhysicalRow struct {
	Ordinal   int
	RowIndex  int
	ID        []byte
	Vector    []float32
	InvNorm   float32
	Adjacency []uint32
}

func (c *Collection) openColumnVectorGraphPhysicalRowReader(name string, opts columnVectorGraphPhysicalRowReaderOptions) (*columnVectorGraphPhysicalRowReader, error) {
	snap, err := c.acquireColumnVectorGraphPhysicalRowReaderSnapshot()
	if err != nil {
		return nil, err
	}
	defer func() { _ = snap.Close() }()
	opts.detachCatalog = true
	return c.openColumnVectorGraphPhysicalRowReaderAtSnapshot(name, snap, opts)
}

// openColumnVectorGraphPhysicalRowReaderAtSnapshot binds the returned reader to
// the caller-owned snapshot unless opts.detachCatalog requests an owned catalog
// copy for readers returned after snapshot close.
func (c *Collection) openColumnVectorGraphPhysicalRowReaderAtSnapshot(name string, snap *backenddb.Snapshot, opts columnVectorGraphPhysicalRowReaderOptions) (*columnVectorGraphPhysicalRowReader, error) {
	def, graph, view, err := c.columnVectorGraphPhysicalRowReaderSnapshotViewAtSnapshot(name, snap)
	if err != nil {
		return nil, err
	}
	catalog := view.Catalog
	if opts.detachCatalog && catalog != nil {
		catalog = catalog.copy()
		view.Catalog = catalog
	}
	reader, err := newColumnPhysicalRowReaderFromSnapshotView(view, columnPhysicalRowReaderOptions{
		ProjectedColumns: []string{
			columnVectorGraphVectorColumnName,
			columnVectorGraphInvNormColumnName,
			columnVectorGraphAdjacencyColumnName,
		},
		MaxDecodedBlocks:  opts.MaxDecodedBlocks,
		RequireInsertOnly: true,
	})
	if err != nil {
		return nil, err
	}
	return &columnVectorGraphPhysicalRowReader{
		def:     def,
		graph:   graph,
		catalog: catalog,
		reader:  reader,
	}, nil
}

func (c *Collection) columnVectorGraphPhysicalRowReaderSnapshotView(name string) (VectorIndexDefinition, columnVectorGraphManifestSnapshot, columnPhysicalScanSnapshotView, error) {
	snap, err := c.acquireColumnVectorGraphPhysicalRowReaderSnapshot()
	if err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	defer func() { _ = snap.Close() }()
	return c.columnVectorGraphPhysicalRowReaderSnapshotViewAtSnapshot(name, snap)
}

func (c *Collection) acquireColumnVectorGraphPhysicalRowReaderSnapshot() (*backenddb.Snapshot, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	return snap, nil
}

func (c *Collection) columnVectorGraphPhysicalRowReaderSnapshotViewAtSnapshot(name string, snap *backenddb.Snapshot) (VectorIndexDefinition, columnVectorGraphManifestSnapshot, columnPhysicalScanSnapshotView, error) {
	if c == nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, errCollectionNil
	}
	if c.db == nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, errCollectionDBNil
	}
	if snap == nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	if catalog == nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, errCollectionNotFound
	}
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, name)
	if !ok {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, ErrIndexNotFound
	}
	if def.Strategy != VectorIndexStrategyColumnGraph {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, fmt.Errorf("collections: vector index %q strategy=%q is not column_graph", def.Name, def.Strategy)
	}
	cfg := catalog.meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.AssetManager == nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, errors.New("collections: column_graph physical row reader requires physical column asset support")
	}
	if cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, errors.New("collections: column_graph physical row reader requires active and recovery-authoritative column manifests")
	}
	if !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, errors.New("collections: column_graph physical row reader requires active recovery-authoritative column manifest")
	}
	rootID := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
	if rootID == 0 {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, fmt.Errorf("collections: column_graph physical row reader missing manifest root %q", collectionColumnManifestRootName(catalog.meta.Name))
	}
	if err := validateColumnManifestIdentityAtRoot(snap, rootID, *cfg.ActiveManifest); err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, rootID)
	if err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	if err := validateColumnManifestSnapshot(manifest, records, *cfg, *cfg.ActiveManifest, catalog.meta.Name, "column vector graph row reader"); err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	graphRecord, ok := findColumnVectorGraphManifestRecord(records, def.Name)
	if !ok {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, fmt.Errorf("collections: column_graph %q has no published graph manifest", def.Name)
	}
	graph, err := decodeColumnVectorGraphManifestRecord(graphRecord.value)
	if err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	if !columnVectorGraphManifestMatchesDefinition(catalog.meta.Name, graph, def, *cfg, manifest, records) {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, fmt.Errorf("collections: column_graph %q graph manifest does not match vector index definition: %w", def.Name, errColumnVectorGraphManifestMismatch)
	}
	if err := validateColumnVectorGraphAssetRefAvailable(c.db.ColumnAssetRootDir(), graph.AssetRef); err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig(catalog.meta.Name, *cfg, def)
	if err != nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, err
	}
	graphCfg.ActiveManifest = cfg.ActiveManifest
	graphCfg.RecoveryAuthoritativeManifest = cfg.RecoveryAuthoritativeManifest
	graphCfg.RecoveryAuthoritativeAppliedCommandLSN = cfg.RecoveryAuthoritativeAppliedCommandLSN
	state := snap.State()
	if state == nil {
		return VectorIndexDefinition{}, columnVectorGraphManifestSnapshot{}, columnPhysicalScanSnapshotView{}, backenddb.ErrClosed
	}
	view := columnPhysicalScanSnapshotView{
		CollectionName:     catalog.meta.Name,
		Catalog:            catalog,
		Config:             graphCfg,
		ColumnStoreEnabled: true,
		CommitSeq:          state.CommitSeq,
		AssetRefs: []columnManifestAssetRefForScan{{
			Ref:    graph.AssetRef,
			Reason: ColumnPublishOperationInsert,
		}},
		Diagnostics: columnPhysicalScanDiagnostics{
			ManifestRoot:               rootID,
			ManifestGeneration:         cfg.ActiveManifest.Generation,
			RecoveryManifestGeneration: cfg.RecoveryAuthoritativeManifest.Generation,
			AppliedCommandLSN:          cfg.RecoveryAuthoritativeAppliedCommandLSN,
			ManifestRecords:            len(records),
			AssetRefs:                  1,
		},
		ColumnAssetRootDir: c.db.ColumnAssetRootDir(),
		AssetNamespace:     graphCfg.AssetManager.Namespace,
	}
	return def, graph, view, nil
}

func (r *columnVectorGraphPhysicalRowReader) Close() error {
	if r == nil || r.reader == nil {
		return nil
	}
	return r.reader.Close()
}

func (r *columnVectorGraphPhysicalRowReader) RowCount() int {
	if r == nil || r.reader == nil {
		return 0
	}
	return r.reader.RowCount()
}

func (r *columnVectorGraphPhysicalRowReader) Stats() columnPhysicalRowReaderStats {
	if r == nil || r.reader == nil {
		return columnPhysicalRowReaderStats{}
	}
	return r.reader.Stats()
}

func (r *columnVectorGraphPhysicalRowReader) rowReader() (*columnPhysicalRowReader, error) {
	if r == nil || r.reader == nil {
		return nil, errNilColumnVectorGraphPhysicalRowReader
	}
	return r.reader, nil
}

// FetchRow returns one graph row and performs fail-closed adjacency ordinal
// validation. Native search uses fetchRowUnchecked and validates each edge once
// when expanded to avoid duplicate adjacency scans on the hot path.
func (r *columnVectorGraphPhysicalRowReader) FetchRow(ordinal int, scratch *columnPhysicalRowReaderScratch) (columnVectorGraphPhysicalRow, error) {
	reader, err := r.rowReader()
	if err != nil {
		return columnVectorGraphPhysicalRow{}, err
	}
	row, err := reader.FetchRow(ordinal, scratch)
	if err != nil {
		return columnVectorGraphPhysicalRow{}, err
	}
	return r.graphRowFromPhysicalRow(row)
}

func (r *columnVectorGraphPhysicalRowReader) fetchRowUnchecked(ordinal int, scratch *columnPhysicalRowReaderScratch) (columnVectorGraphPhysicalRow, error) {
	reader, err := r.rowReader()
	if err != nil {
		return columnVectorGraphPhysicalRow{}, err
	}
	row, err := reader.FetchRow(ordinal, scratch)
	if err != nil {
		return columnVectorGraphPhysicalRow{}, err
	}
	return r.graphRowFromPhysicalRowUnchecked(row)
}

// FetchBatch returns graph rows in the underlying reader's batch order and
// performs fail-closed adjacency ordinal validation for each row. Native search
// uses fetchBatchUnchecked for final result fetches after validating expanded
// edges during traversal.
func (r *columnVectorGraphPhysicalRowReader) FetchBatch(ordinals []int, scratch *columnPhysicalRowReaderScratch, visitor func(columnVectorGraphPhysicalRow) error) error {
	reader, err := r.rowReader()
	if err != nil {
		return err
	}
	if visitor == nil {
		return errors.New("collections: column vector graph physical row reader batch visitor is nil")
	}
	return reader.FetchBatch(ordinals, scratch, func(row columnPhysicalRowReaderRow) error {
		graphRow, err := r.graphRowFromPhysicalRow(row)
		if err != nil {
			return err
		}
		return visitor(graphRow)
	})
}

func (r *columnVectorGraphPhysicalRowReader) fetchBatchUnchecked(ordinals []int, scratch *columnPhysicalRowReaderScratch, visitor func(columnVectorGraphPhysicalRow) error) error {
	reader, err := r.rowReader()
	if err != nil {
		return err
	}
	if visitor == nil {
		return errors.New("collections: column vector graph physical row reader batch visitor is nil")
	}
	return reader.FetchBatch(ordinals, scratch, func(row columnPhysicalRowReaderRow) error {
		graphRow, err := r.graphRowFromPhysicalRowUnchecked(row)
		if err != nil {
			return err
		}
		return visitor(graphRow)
	})
}

func (r *columnVectorGraphPhysicalRowReader) graphRowFromPhysicalRow(row columnPhysicalRowReaderRow) (columnVectorGraphPhysicalRow, error) {
	reader, err := r.rowReader()
	if err != nil {
		return columnVectorGraphPhysicalRow{}, err
	}
	graphRow, err := r.graphRowFromPhysicalRowUnchecked(row)
	if err != nil {
		return columnVectorGraphPhysicalRow{}, err
	}
	rowCount := reader.totalRows
	for i, neighbor := range graphRow.Adjacency {
		if err := validateColumnVectorGraphAdjacencyOrdinal(r.def.Name, row.Ordinal, i, neighbor, rowCount); err != nil {
			return columnVectorGraphPhysicalRow{}, err
		}
	}
	return graphRow, nil
}

// graphRowFromPhysicalRowUnchecked validates row shape and vector scoring
// inputs, but intentionally does not scan adjacency bounds. Native search uses
// this path and validates each edge exactly when it is expanded.
func (r *columnVectorGraphPhysicalRowReader) graphRowFromPhysicalRowUnchecked(row columnPhysicalRowReaderRow) (columnVectorGraphPhysicalRow, error) {
	if r == nil {
		return columnVectorGraphPhysicalRow{}, errNilColumnVectorGraphPhysicalRowReader
	}
	if len(row.ID) == 0 {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d missing document id", r.def.Name, row.Ordinal)
	}
	if len(row.Values) != columnVectorGraphPhysicalRowValueCount {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d values=%d want %d", r.def.Name, row.Ordinal, len(row.Values), columnVectorGraphPhysicalRowValueCount)
	}
	vector := row.Values[columnVectorGraphPhysicalRowValueVector]
	invNorm := row.Values[columnVectorGraphPhysicalRowValueInvNorm]
	adjacency := row.Values[columnVectorGraphPhysicalRowValueAdjacency]
	if vector.Type != ColumnStoreValueFloat32Vector || invNorm.Type != ColumnStoreValueFloat32 || adjacency.Type != ColumnStoreValueAdjacencyList {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d unexpected graph value types: vector=%q inv_norm=%q adjacency=%q", r.def.Name, row.Ordinal, vector.Type, invNorm.Type, adjacency.Type)
	}
	if !vector.Present || !invNorm.Present || !adjacency.Present {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d missing graph value", r.def.Name, row.Ordinal)
	}
	if vector.Null || invNorm.Null || adjacency.Null {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d contains null graph value", r.def.Name, row.Ordinal)
	}
	if len(vector.Float32Vector) != r.def.Dimensions {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d vector dims=%d want %d", r.def.Name, row.Ordinal, len(vector.Float32Vector), r.def.Dimensions)
	}
	if invNorm.Float32 <= 0 || math.IsNaN(float64(invNorm.Float32)) || math.IsInf(float64(invNorm.Float32), 0) {
		return columnVectorGraphPhysicalRow{}, fmt.Errorf("collections: column_graph %q ordinal=%d invalid inv_norm=%v", r.def.Name, row.Ordinal, invNorm.Float32)
	}
	return columnVectorGraphPhysicalRow{
		Ordinal:   row.Ordinal,
		RowIndex:  row.RowIndex,
		ID:        row.ID,
		Vector:    vector.Float32Vector,
		InvNorm:   invNorm.Float32,
		Adjacency: adjacency.AdjacencyList,
	}, nil
}

func validateColumnVectorGraphAdjacencyOrdinal(graphName string, ordinal int, adjacencyIndex int, neighbor uint32, rowCount int) error {
	if uint64(neighbor) >= uint64(rowCount) {
		return fmt.Errorf("collections: column_graph %q ordinal=%d adjacency[%d]=%d outside row_count=%d: %w", graphName, ordinal, adjacencyIndex, neighbor, rowCount, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
	}
	return nil
}
