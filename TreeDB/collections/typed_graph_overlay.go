package collections

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
)

// prepareRows reads only the checked suffix, using the existing typed-column
// reconstruction codec directly, never retained JSON. The returned newest-ID
// rows are owned; tombstones remain present to shadow immutable base entries.
func (suffix typedGraphOverlaySuffix) prepareRows(current *CollectionReadView, maxOwnedBytes int64) ([]columnPhysicalVisibleRow, error) {
	if current == nil || current.snapshot == nil || current.catalog != suffix.view.Catalog || maxOwnedBytes <= 0 {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	reader, err := newColumnPhysicalRowReaderFromSnapshotView(suffix.view, columnPhysicalRowReaderOptions{})
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	if reader.RowCount() != suffix.rows {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	cfg := suffix.view.FullConfig
	// Validate the worst-case FP32 plane from schema/count metadata before the
	// existing decoder allocates its matrix. Source bytes, decoded value/header
	// working space, and retained payload are distinct accounting terms.
	var vectorBytes int64
	for _, column := range cfg.Columns {
		switch column.ValueType {
		case ColumnStoreValueFloat32Vector:
			if column.VectorDims <= 0 || (suffix.rows > 0 && int64(column.VectorDims) > (maxOwnedBytes-vectorBytes)/4/int64(suffix.rows)) {
				return nil, errTypedGraphOverlayFoldNeeded
			}
			vectorBytes += int64(column.VectorDims) * 4 * int64(suffix.rows)
		case ColumnStoreValueString:
		default:
			return nil, errors.New("collections: typed graph overlay supports declared strings and FP32 vectors only")
		}
	}
	if err := current.ensureAssetReadCaches(cfg, ColumnAssetReadIntegrityVerify); err != nil {
		return nil, err
	}
	// Reuse the existing decoder with only the checked suffix refs. Its default
	// lazy ref loader builds a map for the whole manifest, which is unnecessary
	// here. Decoded FP32 arrays own their storage and can transfer to the output.
	cache := &typedColumnPartReconstructionCache{RefsLoaded: true, Refs: make(map[uint64]columnManifestAssetRefForScan, len(suffix.view.TypedColumnPartRefs)), ReadCache: current.typedColumnAssetReadCache}
	for _, ref := range suffix.view.TypedColumnPartRefs {
		if _, exists := cache.Refs[ref.Ref.Generation]; exists {
			return nil, errors.New("collections: typed graph overlay requires one typed vector part per generation")
		}
		cache.Refs[ref.Ref.Generation] = ref
	}
	rows := make([]columnPhysicalVisibleRow, 0, suffix.rows)
	byID := make(map[string]int, suffix.rows)
	var scratch columnPhysicalRowReaderScratch
	var typedScratch, mergedScratch []columnDeclaredValue
	var ownedBytes int64
	for ordinal := 0; ordinal < reader.RowCount(); ordinal++ {
		physical, err := reader.FetchRow(ordinal, &scratch)
		if err != nil {
			return nil, err
		}
		row := columnPhysicalVisibleRowFromReaderRow(physical)
		index, exists := byID[string(row.ID)]
		if exists && !columnPhysicalVisibleRowNewer(row, rows[index]) {
			continue
		}
		if !row.Deleted {
			typed, err := current.collection.typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCache(current.snapshot, current.catalog.rootID(collectionColumnManifestRootName(current.catalog.meta.Name)), cfg, row, cache, typedScratch)
			if err != nil {
				return nil, err
			}
			typedScratch = typed.Values
			row.Values, err = mergeColumnReconstructionValuesInto(cfg, row.Values, typed.Values, mergedScratch)
			if err != nil {
				return nil, err
			}
			mergedScratch = row.Values
		}
		// Charge every retained version before cloning, including overwritten
		// versions. Row/header/map counts are separately bounded by suffix.rows.
		charge := func(n int64) bool {
			if n > maxOwnedBytes-ownedBytes {
				return false
			}
			ownedBytes += n
			return true
		}
		if !charge(int64(len(row.ID))) {
			return nil, errTypedGraphOverlayFoldNeeded
		}
		for _, value := range row.Values {
			if value.Type != ColumnStoreValueString && value.Type != ColumnStoreValueFloat32Vector {
				return nil, errors.New("collections: typed graph overlay supports declared strings and FP32 vectors only")
			}
			if !charge(int64(len(value.String)) + int64(len(value.StringBytes)) + int64(len(value.Float32Vector))*4) {
				return nil, errTypedGraphOverlayFoldNeeded
			}
		}
		row.ID = bytes.Clone(row.ID)
		values := make([]columnDeclaredValue, len(row.Values))
		copy(values, row.Values)
		for i := range values {
			if values[i].StringBytes != nil {
				values[i].String = string(values[i].StringBytes)
				values[i].StringBytes = nil
			}
		}
		row.Values = values
		if exists {
			rows[index] = row
		} else {
			byID[string(row.ID)] = len(rows)
			rows = append(rows, row)
		}
	}
	return rows, nil
}

var errTypedGraphOverlayFoldNeeded = errors.New("collections: typed graph overlay requires fold")

// Limits apply to cumulative physical work, including overwritten rows and
// tombstones, not just the surviving delta. There are deliberately no defaults
// or public admission route: production lifecycle installation belongs to M3.
type typedGraphOverlayLimits struct {
	Rows, Tombstones int
	Bytes            int64
}

type typedGraphOverlaySuffix struct {
	view       columnPhysicalScanSnapshotView
	baseParts  []columnManifestAssetRefForScan
	rows       int
	tombstones int
	bytes      int64
}

// prepareTypedGraphOverlaySuffix borrows both pins. Current is the sole logical
// query snapshot; base is only an immutable accelerator whose complete original
// asset lineage must remain reachable. Callers keep both pins open throughout
// use. This does not install an overlay or change ordinary search admission.
func prepareTypedGraphOverlaySuffix(base *VectorIndexSearcher, current *CollectionReadView, limits typedGraphOverlayLimits) (typedGraphOverlaySuffix, error) {
	if base == nil || base.reader == nil || base.snapshot == nil || base.catalog == nil || current == nil || current.snapshot == nil || current.catalog == nil || base.collection == nil || base.collection != current.collection {
		return typedGraphOverlaySuffix{}, ErrVectorIndexSnapshotMismatch
	}
	baseCfg := base.catalog.meta.Options.ColumnStore
	currentCfg := current.catalog.meta.Options.ColumnStore
	def, found := findVectorIndex(current.catalog.meta.VectorIndexes, base.reader.def.Name)
	if !found || !vectorIndexDefinitionValuesEqual(base.reader.def, def) || baseCfg == nil || currentCfg == nil || baseCfg.SchemaHash != currentCfg.SchemaHash || base.catalog.meta.Name != current.catalog.meta.Name || !reflect.DeepEqual(baseCfg.Columns, currentCfg.Columns) {
		return typedGraphOverlaySuffix{}, ErrVectorIndexSnapshotMismatch
	}
	collection := base.collection
	baseView, err := collection.prepareColumnPhysicalScanSnapshotViewAtSnapshotWithSidecars(base.snapshot, base.catalog, base.catalog.meta.Name, base.catalog.rootID(collectionColumnManifestRootName(base.catalog.meta.Name)), *baseCfg, true, columnManifestScanNoSidecars())
	if err != nil {
		return typedGraphOverlaySuffix{}, err
	}
	currentRoot := current.catalog.rootID(collectionColumnManifestRootName(current.catalog.meta.Name))
	currentView, err := collection.prepareColumnPhysicalScanSnapshotViewAtSnapshotWithSidecars(current.snapshot, current.catalog, current.catalog.meta.Name, currentRoot, *currentCfg, true, columnManifestScanNoSidecars())
	if err != nil {
		return typedGraphOverlaySuffix{}, err
	}
	// A rebuild, drop/recreate, or altered graph asset identity is not a delta
	// over this pinned graph, even if its field and dimensions happen to match.
	raw, err := current.snapshot.GetAtRoot(currentRoot, columnVectorGraphManifestRecordKey(def.Name))
	if err != nil {
		return typedGraphOverlaySuffix{}, err
	}
	if len(raw) == 0 {
		return typedGraphOverlaySuffix{}, ErrVectorIndexSnapshotMismatch
	}
	graph, err := decodeColumnVectorGraphManifestRecord(raw)
	if err != nil {
		return typedGraphOverlaySuffix{}, err
	}
	if !reflect.DeepEqual(graph, base.reader.graph) {
		return typedGraphOverlaySuffix{}, ErrVectorIndexSnapshotMismatch
	}
	return checkedTypedGraphOverlaySuffix(baseView, currentView, limits)
}

func checkedTypedGraphOverlaySuffix(base, current columnPhysicalScanSnapshotView, limits typedGraphOverlayLimits) (typedGraphOverlaySuffix, error) {
	if limits.Rows <= 0 || limits.Tombstones < 0 || limits.Bytes <= 0 {
		return typedGraphOverlaySuffix{}, fmt.Errorf("%w: invalid limits", errTypedGraphOverlayFoldNeeded)
	}
	if base.FullConfig.ActiveManifest == nil || current.FullConfig.ActiveManifest == nil || base.AssetNamespace != current.AssetNamespace || base.CollectionName != current.CollectionName || base.FullConfig.SchemaHash != current.FullConfig.SchemaHash || current.CommitSeq < base.CommitSeq {
		return typedGraphOverlaySuffix{}, ErrVectorIndexSnapshotMismatch
	}
	generation := base.FullConfig.ActiveManifest.Generation
	if current.FullConfig.ActiveManifest.Generation < generation {
		return typedGraphOverlaySuffix{}, ErrVectorIndexSnapshotMismatch
	}
	result := typedGraphOverlaySuffix{view: current, baseParts: base.AssetRefs}
	// Records are already in canonical manifest key order. Check identity and
	// cumulative bounds before allocating suffix slices or touching row data.
	check := func(old, now []columnManifestAssetRefForScan, rowAssets bool) error {
		next := 0
		for _, part := range now {
			if part.Ref.Generation > current.FullConfig.ActiveManifest.Generation || part.Ref.Namespace != current.AssetNamespace {
				return ErrVectorIndexSnapshotMismatch
			}
			if part.Ref.Generation <= generation {
				if next == len(old) || part.Ref != old[next].Ref || part.Rows != old[next].Rows || part.Role != old[next].Role || part.Reason != old[next].Reason || !reflect.DeepEqual(part.SortKey, old[next].SortKey) {
					return ErrVectorIndexSnapshotMismatch
				}
				next++
				continue
			}
			if part.Ref.Length <= 0 || part.Ref.Length > limits.Bytes-result.bytes || part.Rows <= 0 || part.Rows > limits.Rows {
				return errTypedGraphOverlayFoldNeeded
			}
			result.bytes += part.Ref.Length
			if rowAssets {
				if part.Rows > limits.Rows-result.rows {
					return errTypedGraphOverlayFoldNeeded
				}
				result.rows += part.Rows
				if part.Reason == ColumnPublishOperationDelete {
					if part.Rows > limits.Tombstones-result.tombstones {
						return errTypedGraphOverlayFoldNeeded
					}
					result.tombstones += part.Rows
				}
			}
		}
		if next != len(old) {
			return ErrVectorIndexSnapshotMismatch
		}
		return nil
	}
	if err := check(base.AssetRefs, current.AssetRefs, true); err != nil {
		return typedGraphOverlaySuffix{}, err
	}
	if err := check(base.TypedColumnPartRefs, current.TypedColumnPartRefs, false); err != nil {
		return typedGraphOverlaySuffix{}, err
	}
	result.view.AssetRefs = nil
	result.view.TypedColumnPartRefs = nil
	for _, part := range current.AssetRefs {
		if part.Ref.Generation > generation {
			result.view.AssetRefs = append(result.view.AssetRefs, part)
		}
	}
	for _, part := range current.TypedColumnPartRefs {
		if part.Ref.Generation > generation {
			result.view.TypedColumnPartRefs = append(result.view.TypedColumnPartRefs, part)
		}
	}
	return result, nil
}
