package collections

import (
	"fmt"
	"time"
)

type columnInt64ValueHourCountRunner struct {
	column       string
	assets       []columnInt64ValueHourCountAsset
	counts       [24]int
	resultGroups []ColumnPhysicalQueryGroup
	assetBytes   int64
}

type columnInt64ValueHourCountAsset struct {
	values []int64
}

func prepareColumnInt64ValueHourCountRunner(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache) (*columnInt64ValueHourCountRunner, error) {
	if req.Kind != ColumnPhysicalQueryHourCount || req.ValueColumn == "" || view.MutationParts != 0 {
		return nil, nil
	}
	if readCache == nil {
		return nil, fmt.Errorf("collections: int64 values query missing read cache")
	}
	col, _, ok := columnPhysicalQueryDeclaredColumn(view.Config, req.ValueColumn)
	if !ok || col.ValueType != ColumnStoreValueInt64 || col.Nullable {
		return nil, nil
	}
	byPart := columnInt64ValueSnapshotsByPart(view, req.ValueColumn)
	if len(byPart) == 0 {
		return nil, nil
	}
	runner := &columnInt64ValueHourCountRunner{
		column: req.ValueColumn,
		assets: make([]columnInt64ValueHourCountAsset, 0, len(view.AssetRefs)),
	}
	var scratch []byte
	for _, part := range view.AssetRefs {
		if part.Reason != ColumnPublishOperationInsert {
			return nil, nil
		}
		snapshot, ok := byPart[[2]uint64{part.Ref.Generation, part.Ref.PartID}]
		if !ok {
			return nil, nil
		}
		raw, err := readCache.read(snapshot.AssetRef, scratch)
		if err != nil {
			return nil, fmt.Errorf("collections: int64 values read generation=%d part_id=%d column=%q: %w", snapshot.AssetRef.Generation, snapshot.AssetRef.PartID, req.ValueColumn, err)
		}
		scratch = raw
		decoded, err := decodeColumnInt64ValuesAsset(raw, snapshot.AssetRef, view.Config, view.CollectionName, req.ValueColumn, readCache.verifyChecksum)
		if err != nil {
			return nil, err
		}
		if len(decoded.Values) != snapshot.Rows {
			return nil, fmt.Errorf("collections: int64 values asset generation=%d part_id=%d column=%q rows=%d want manifest rows=%d", snapshot.AssetRef.Generation, snapshot.AssetRef.PartID, req.ValueColumn, len(decoded.Values), snapshot.Rows)
		}
		runner.assets = append(runner.assets, columnInt64ValueHourCountAsset{
			values: decoded.Values,
		})
		runner.assetBytes += snapshot.AssetRef.Length
	}
	if len(runner.assets) == 0 {
		return nil, nil
	}
	runner.resultGroups = make([]ColumnPhysicalQueryGroup, 0, 24)
	return runner, nil
}

func columnInt64ValueSnapshotsByPart(view columnPhysicalScanSnapshotView, column string) map[[2]uint64]columnManifestInt64ValuesSnapshot {
	byPart := make(map[[2]uint64]columnManifestInt64ValuesSnapshot, len(view.Int64Values))
	for _, snapshot := range view.Int64Values {
		if snapshot.ColumnName != column {
			continue
		}
		byPart[[2]uint64{snapshot.AssetRef.Generation, snapshot.AssetRef.PartID}] = snapshot
	}
	return byPart
}

func runColumnInt64ValueHourCountOneShot(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache) (ColumnPhysicalQueryResult, bool, error) {
	if req.Kind != ColumnPhysicalQueryHourCount || req.ValueColumn == "" || view.MutationParts != 0 {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	if readCache == nil {
		return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: int64 values query missing read cache")
	}
	col, _, ok := columnPhysicalQueryDeclaredColumn(view.Config, req.ValueColumn)
	if !ok || col.ValueType != ColumnStoreValueInt64 || col.Nullable {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	byPart := columnInt64ValueSnapshotsByPart(view, req.ValueColumn)
	if len(byPart) == 0 {
		return ColumnPhysicalQueryResult{}, false, nil
	}

	start := time.Now()
	var counts [24]int
	rows := 0
	assetBytes := int64(0)
	blocks := 0
	var scratch []byte
	for _, part := range view.AssetRefs {
		if part.Reason != ColumnPublishOperationInsert {
			return ColumnPhysicalQueryResult{}, false, nil
		}
		snapshot, ok := byPart[[2]uint64{part.Ref.Generation, part.Ref.PartID}]
		if !ok {
			return ColumnPhysicalQueryResult{}, false, nil
		}
		raw, err := readCache.read(snapshot.AssetRef, scratch)
		if err != nil {
			return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: int64 values read generation=%d part_id=%d column=%q: %w", snapshot.AssetRef.Generation, snapshot.AssetRef.PartID, req.ValueColumn, err)
		}
		scratch = raw
		decoded, err := decodeColumnInt64ValuesAsset(raw, snapshot.AssetRef, view.Config, view.CollectionName, req.ValueColumn, readCache.verifyChecksum)
		if err != nil {
			return ColumnPhysicalQueryResult{}, true, err
		}
		if len(decoded.Values) != snapshot.Rows {
			return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: int64 values asset generation=%d part_id=%d column=%q rows=%d want manifest rows=%d", snapshot.AssetRef.Generation, snapshot.AssetRef.PartID, req.ValueColumn, len(decoded.Values), snapshot.Rows)
		}
		for _, value := range decoded.Values {
			counts[columnPhysicalQueryUTCHour(value)]++
			rows++
		}
		assetBytes += snapshot.AssetRef.Length
		blocks++
	}
	if blocks == 0 {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	groups := make([]ColumnPhysicalQueryGroup, 0, 24)
	for hour, count := range counts {
		if count == 0 {
			continue
		}
		groups = append(groups, ColumnPhysicalQueryGroup{
			Key:   columnPhysicalQueryHourKey(hour),
			Count: count,
		})
	}
	diag := columnPhysicalQueryDiagnosticsFromScan(view.Diagnostics)
	diag.WorkerCount = 1
	diag.ProjectedColumns = 1
	diag.ScheduledGranules = blocks
	diag.DecodedBlocks = blocks
	diag.DirectReduceBlocks = blocks
	diag.Int64ValueHits = blocks
	diag.RowsScanned = rows
	diag.PhysicalBytesScanned = assetBytes
	diag.ReduceRows = rows
	diag.ResultGroups = len(groups)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	diag.ScanNanos = time.Since(start).Nanoseconds()
	return ColumnPhysicalQueryResult{Groups: groups, Diagnostics: diag}, true, nil
}

func (r *columnInt64ValueHourCountRunner) run(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) ColumnPhysicalQueryResult {
	start := time.Now()
	for idx := range r.counts {
		r.counts[idx] = 0
	}
	rows := 0
	for _, asset := range r.assets {
		for _, value := range asset.values {
			r.counts[columnPhysicalQueryUTCHour(value)]++
			rows++
		}
	}
	r.resultGroups = r.resultGroups[:0]
	for hour, count := range r.counts {
		if count == 0 {
			continue
		}
		r.resultGroups = append(r.resultGroups, ColumnPhysicalQueryGroup{
			Key:   columnPhysicalQueryHourKey(hour),
			Count: count,
		})
	}
	diag := columnPhysicalQueryDiagnosticsFromScan(view.Diagnostics)
	diag.WorkerCount = 1
	diag.ProjectedColumns = 1
	diag.ScheduledGranules = len(r.assets)
	diag.DecodedBlocks = len(r.assets)
	diag.DirectReduceBlocks = len(r.assets)
	diag.Int64ValueHits = len(r.assets)
	diag.RowsScanned = rows
	diag.PhysicalBytesScanned = r.assetBytes
	diag.ReduceRows = rows
	diag.ResultGroups = len(r.resultGroups)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	diag.ScanNanos = time.Since(start).Nanoseconds()
	return ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}
}
