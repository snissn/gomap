package collections

import (
	"bytes"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type columnPhysicalVisibleRow struct {
	Generation        uint64
	PartID            uint64
	AppliedCommandLSN uint64
	Operation         ColumnPublishOperation
	RowIndex          int
	ID                []byte
	Values            []columnDeclaredValue
	Deleted           bool
}

type columnPhysicalVisibilityResult struct {
	Rows        []columnPhysicalVisibleRow
	Diagnostics columnPhysicalScanDiagnostics
}

func (c *Collection) scanColumnPhysicalVisibleRows(projected []string) (columnPhysicalVisibilityResult, error) {
	return c.scanColumnPhysicalVisibleRowsWithReadIntegrity(projected, ColumnAssetReadIntegrityVerify)
}

func (c *Collection) scanColumnPhysicalVisibleRowsWithReadIntegrity(projected []string, readIntegrity ColumnAssetReadIntegrity) (columnPhysicalVisibilityResult, error) {
	if c == nil {
		return columnPhysicalVisibilityResult{}, errCollectionNil
	}
	if c.db == nil {
		return columnPhysicalVisibilityResult{}, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return columnPhysicalVisibilityResult{}, errCollectionDBNil
	}
	defer func() { _ = snap.Close() }()
	snap.MarkForegroundRead()
	c.catalogMu.RLock()
	collectionName := c.meta.Name
	c.catalogMu.RUnlock()
	catalog, err := loadCollectionCatalog(snap, collectionName)
	if err != nil {
		return columnPhysicalVisibilityResult{}, err
	}
	if catalog == nil {
		return columnPhysicalVisibilityResult{}, errCollectionNotFound
	}
	rootName := collectionColumnManifestRootName(collectionName)
	rootID := catalog.rootID(rootName)
	cfgPtr := catalog.meta.Options.ColumnStore
	columnStoreEnabled := cfgPtr != nil
	var cfg ColumnStoreConfig
	if cfgPtr != nil {
		cfg = cfgPtr.copy()
	}
	return c.scanColumnPhysicalVisibleRowsAtSnapshotWithReadIntegrity(snap, catalog, collectionName, rootID, cfg, columnStoreEnabled, projected, readIntegrity)
}

func (c *Collection) scanColumnPhysicalVisibleRowsAtSnapshot(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	collectionName string,
	rootID uint64,
	cfg ColumnStoreConfig,
	columnStoreEnabled bool,
	projected []string,
) (columnPhysicalVisibilityResult, error) {
	return c.scanColumnPhysicalVisibleRowsAtSnapshotWithReadIntegrity(snap, catalog, collectionName, rootID, cfg, columnStoreEnabled, projected, ColumnAssetReadIntegrityVerify)
}

func (c *Collection) scanColumnPhysicalVisibleRowsAtSnapshotWithReadIntegrity(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	collectionName string,
	rootID uint64,
	cfg ColumnStoreConfig,
	columnStoreEnabled bool,
	projected []string,
	readIntegrity ColumnAssetReadIntegrity,
) (columnPhysicalVisibilityResult, error) {
	return c.scanColumnPhysicalVisibleRowsAtSnapshotForTargetsWithReadIntegrity(snap, catalog, collectionName, rootID, cfg, columnStoreEnabled, nil, projected, readIntegrity)
}

func (c *Collection) scanColumnPhysicalVisibleRowsAtSnapshotForTargets(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	collectionName string,
	rootID uint64,
	cfg ColumnStoreConfig,
	columnStoreEnabled bool,
	targets *columnPhysicalVisibilityTargetIDs,
	projected []string,
) (columnPhysicalVisibilityResult, error) {
	return c.scanColumnPhysicalVisibleRowsAtSnapshotForTargetsWithReadIntegrity(snap, catalog, collectionName, rootID, cfg, columnStoreEnabled, targets, projected, ColumnAssetReadIntegrityVerify)
}

func (c *Collection) scanColumnPhysicalVisibleRowsAtSnapshotForTargetsWithReadIntegrity(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	collectionName string,
	rootID uint64,
	cfg ColumnStoreConfig,
	columnStoreEnabled bool,
	targets *columnPhysicalVisibilityTargetIDs,
	projected []string,
	readIntegrity ColumnAssetReadIntegrity,
) (columnPhysicalVisibilityResult, error) {
	return c.scanColumnPhysicalVisibleRowsAtSnapshotForTargetsWithReadCache(snap, catalog, collectionName, rootID, cfg, columnStoreEnabled, targets, projected, readIntegrity, nil)
}

func (c *Collection) scanColumnPhysicalVisibleRowsAtSnapshotForTargetsWithReadCache(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	collectionName string,
	rootID uint64,
	cfg ColumnStoreConfig,
	columnStoreEnabled bool,
	targets *columnPhysicalVisibilityTargetIDs,
	projected []string,
	readIntegrity ColumnAssetReadIntegrity,
	readCache *columnPhysicalAssetReadCache,
) (columnPhysicalVisibilityResult, error) {
	var latest columnPhysicalVisibilityIndex
	diag, err := c.scanColumnPhysicalRowsAtSnapshot(snap, catalog, collectionName, rootID, cfg, columnStoreEnabled, columnPhysicalScanRequest{
		ProjectedColumns: projected,
		ReadIntegrity:    readIntegrity,
		ReadCache:        readCache,
		Visitor: func(row columnPhysicalScanRowView) error {
			if targets != nil && !targets.contains(row.ID) {
				return nil
			}
			latest.upsert(row)
			return nil
		},
	})
	if err != nil {
		return columnPhysicalVisibilityResult{Diagnostics: diag}, err
	}
	rows := latest.rows
	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(rows[i].ID, rows[j].ID) < 0
	})
	return columnPhysicalVisibilityResult{
		Rows:        rows,
		Diagnostics: diag,
	}, nil
}

type columnPhysicalVisibilityTargetIDs struct {
	byHash map[uint64][][]byte
}

func newColumnPhysicalVisibilityTargetIDs(ids [][]byte) *columnPhysicalVisibilityTargetIDs {
	if len(ids) == 0 {
		return nil
	}
	targets := &columnPhysicalVisibilityTargetIDs{
		byHash: make(map[uint64][][]byte, len(ids)),
	}
	for _, id := range ids {
		hash := columnPhysicalQueryHashBytes(id)
		targets.byHash[hash] = append(targets.byHash[hash], id)
	}
	return targets
}

func (targets *columnPhysicalVisibilityTargetIDs) contains(id []byte) bool {
	if targets == nil {
		return true
	}
	candidates := targets.byHash[columnPhysicalQueryHashBytes(id)]
	for _, candidate := range candidates {
		if bytes.Equal(candidate, id) {
			return true
		}
	}
	return false
}

func (c *Collection) latestColumnPhysicalVisibleRowAtSnapshot(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	id []byte,
	projected []string,
) (columnPhysicalVisibleRow, columnPhysicalScanDiagnostics, bool, error) {
	if catalog == nil {
		return columnPhysicalVisibleRow{}, columnPhysicalScanDiagnostics{}, false, errCollectionNotFound
	}
	cfgPtr := catalog.meta.Options.ColumnStore
	columnStoreEnabled := cfgPtr != nil
	var cfg ColumnStoreConfig
	if cfgPtr != nil {
		cfg = cfgPtr.copy()
	}
	collectionName := catalog.meta.Name
	rootID := catalog.rootID(collectionColumnManifestRootName(collectionName))
	var latest columnPhysicalVisibleRow
	found := false
	diag, err := c.scanColumnPhysicalRowsAtSnapshot(snap, catalog, collectionName, rootID, cfg, columnStoreEnabled, columnPhysicalScanRequest{
		ProjectedColumns: projected,
		Visitor: func(row columnPhysicalScanRowView) error {
			if !bytes.Equal(row.ID, id) {
				return nil
			}
			if !found {
				latest.ID = bytes.Clone(row.ID)
			}
			if !found || columnPhysicalScanRowViewNewer(row, latest) {
				assignColumnPhysicalVisibleRow(&latest, row)
				found = true
			}
			return nil
		},
	})
	return latest, diag, found, err
}

type columnPhysicalVisibilityIndex struct {
	rows        []columnPhysicalVisibleRow
	byHash      map[uint64][]int
	bytesArena  []byte
	valuesArena []columnDeclaredValue
}

const (
	columnPhysicalVisibilityBytesArenaChunk  = 64 << 10
	columnPhysicalVisibilityValuesArenaChunk = 4096
)

func (idx *columnPhysicalVisibilityIndex) upsert(row columnPhysicalScanRowView) {
	hash := columnPhysicalQueryHashBytes(row.ID)
	if idx.byHash != nil {
		for _, pos := range idx.byHash[hash] {
			if bytes.Equal(idx.rows[pos].ID, row.ID) {
				if columnPhysicalScanRowViewNewer(row, idx.rows[pos]) {
					idx.assignColumnPhysicalVisibleRow(&idx.rows[pos], row)
				}
				return
			}
		}
	}
	if idx.byHash == nil {
		idx.byHash = make(map[uint64][]int, 1024)
	}
	idx.rows = append(idx.rows, columnPhysicalVisibleRow{ID: idx.cloneBytes(row.ID)})
	pos := len(idx.rows) - 1
	idx.assignColumnPhysicalVisibleRow(&idx.rows[pos], row)
	idx.byHash[hash] = append(idx.byHash[hash], pos)
}

func (idx *columnPhysicalVisibilityIndex) assignColumnPhysicalVisibleRow(dst *columnPhysicalVisibleRow, row columnPhysicalScanRowView) {
	dst.Generation = row.Generation
	dst.PartID = row.PartID
	dst.AppliedCommandLSN = row.AppliedCommandLSN
	dst.Operation = row.Operation
	dst.RowIndex = row.RowIndex
	dst.Deleted = row.Deleted
	if row.Deleted {
		dst.Values = nil
		return
	}
	dst.Values = idx.cloneColumnDeclaredValues(row.Values)
}

func (idx *columnPhysicalVisibilityIndex) cloneColumnDeclaredValues(values []columnDeclaredValue) []columnDeclaredValue {
	if len(values) == 0 {
		return nil
	}
	if cap(idx.valuesArena)-len(idx.valuesArena) < len(values) {
		chunk := columnPhysicalVisibilityValuesArenaChunk
		if len(values) > chunk {
			chunk = len(values)
		}
		idx.valuesArena = make([]columnDeclaredValue, 0, chunk)
	}
	start := len(idx.valuesArena)
	idx.valuesArena = append(idx.valuesArena, values...)
	out := idx.valuesArena[start:len(idx.valuesArena)]
	for i := range out {
		if out[i].Float32Vector != nil {
			out[i].Float32Vector = append([]float32(nil), out[i].Float32Vector...)
		}
		if out[i].DenseNumericVector != nil {
			out[i].DenseNumericVector = idx.cloneBytes(out[i].DenseNumericVector)
		}
		if out[i].Uint32List != nil {
			out[i].Uint32List = append([]uint32(nil), out[i].Uint32List...)
		}
		if out[i].AdjacencyList != nil {
			out[i].AdjacencyList = append([]uint32(nil), out[i].AdjacencyList...)
		}
		if out[i].Bytes != nil {
			out[i].Bytes = idx.cloneBytes(out[i].Bytes)
		}
		if out[i].StringBytes != nil {
			out[i].StringBytes = idx.cloneBytes(out[i].StringBytes)
			out[i].String = ""
		}
	}
	return out
}

func (idx *columnPhysicalVisibilityIndex) cloneBytes(raw []byte) []byte {
	if len(raw) == 0 {
		return nil
	}
	if cap(idx.bytesArena)-len(idx.bytesArena) < len(raw) {
		chunk := columnPhysicalVisibilityBytesArenaChunk
		if len(raw) > chunk {
			chunk = len(raw)
		}
		idx.bytesArena = make([]byte, 0, chunk)
	}
	start := len(idx.bytesArena)
	idx.bytesArena = append(idx.bytesArena, raw...)
	return idx.bytesArena[start:len(idx.bytesArena)]
}

func assignColumnPhysicalVisibleRow(dst *columnPhysicalVisibleRow, row columnPhysicalScanRowView) {
	dst.Generation = row.Generation
	dst.PartID = row.PartID
	dst.AppliedCommandLSN = row.AppliedCommandLSN
	dst.Operation = row.Operation
	dst.RowIndex = row.RowIndex
	dst.Deleted = row.Deleted
	if row.Deleted {
		dst.Values = dst.Values[:0]
		return
	}
	dst.Values = cloneColumnDeclaredValuesInto(dst.Values, row.Values)
}

func columnPhysicalScanRowViewNewer(a columnPhysicalScanRowView, b columnPhysicalVisibleRow) bool {
	if a.AppliedCommandLSN != b.AppliedCommandLSN {
		return a.AppliedCommandLSN > b.AppliedCommandLSN
	}
	if a.Generation != b.Generation {
		return a.Generation > b.Generation
	}
	if a.PartID != b.PartID {
		return a.PartID > b.PartID
	}
	return a.RowIndex > b.RowIndex
}

func columnPhysicalVisibleRowNewer(a, b columnPhysicalVisibleRow) bool {
	if a.AppliedCommandLSN != b.AppliedCommandLSN {
		return a.AppliedCommandLSN > b.AppliedCommandLSN
	}
	if a.Generation != b.Generation {
		return a.Generation > b.Generation
	}
	if a.PartID != b.PartID {
		return a.PartID > b.PartID
	}
	return a.RowIndex > b.RowIndex
}

func cloneColumnDeclaredValues(values []columnDeclaredValue) []columnDeclaredValue {
	if len(values) == 0 {
		return nil
	}
	return cloneColumnDeclaredValuesInto(make([]columnDeclaredValue, len(values)), values)
}

func cloneColumnDeclaredValuesInto(out []columnDeclaredValue, values []columnDeclaredValue) []columnDeclaredValue {
	if len(values) == 0 {
		return out[:0]
	}
	if cap(out) < len(values) {
		out = make([]columnDeclaredValue, len(values))
	} else {
		out = out[:len(values)]
	}
	for i := range values {
		out[i] = values[i]
		if values[i].Float32Vector != nil {
			out[i].Float32Vector = append([]float32(nil), values[i].Float32Vector...)
		}
		if values[i].DenseNumericVector != nil {
			out[i].DenseNumericVector = append([]byte(nil), values[i].DenseNumericVector...)
		}
		if values[i].Uint32List != nil {
			out[i].Uint32List = append([]uint32(nil), values[i].Uint32List...)
		}
		if values[i].AdjacencyList != nil {
			out[i].AdjacencyList = append([]uint32(nil), values[i].AdjacencyList...)
		}
		if values[i].Bytes != nil {
			out[i].Bytes = append([]byte(nil), values[i].Bytes...)
		}
		if values[i].StringBytes != nil {
			out[i].String = string(values[i].StringBytes)
			out[i].StringBytes = nil
		}
	}
	return out
}
