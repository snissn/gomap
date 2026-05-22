package collections

import (
	"fmt"
	"time"
)

type columnAggregateMetadataRunnerEntry struct {
	groupCode int
	min       int64
	max       int64
}

type columnAggregateMetadataRunner struct {
	kind                     ColumnPhysicalQueryKind
	assetBytes               int64
	metadataHits             int
	rows                     int
	scheduledGranules        int
	groupKeys                []string
	entries                  []columnAggregateMetadataRunnerEntry
	present                  []bool
	mins                     []int64
	maxs                     []int64
	resultGroups             []ColumnPhysicalQueryGroup
	columnAssetReadIntegrity string
}

func prepareColumnAggregateMetadataRunner(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache) (*columnAggregateMetadataRunner, error) {
	if view.MutationParts != 0 {
		return nil, fmt.Errorf("%w: prepared aggregate metadata query requires insert-only manifest", ErrColumnQueryPlanUnsupported)
	}
	if readCache == nil {
		return nil, fmt.Errorf("collections: prepared aggregate metadata query requires read cache")
	}
	aggregate, ok := columnPhysicalQueryAggregateMetadataConfig(view.Config, req)
	if !ok {
		return nil, fmt.Errorf("%w: aggregate metadata %q does not match physical query shape", ErrColumnQueryPlanUnsupported, req.AggregateMetadataName)
	}
	refs := columnPhysicalQueryAggregateMetadataRefs(view.AggregateMetadata, aggregate.Name)
	if len(refs) == 0 {
		return nil, nil
	}
	runner := &columnAggregateMetadataRunner{
		kind:                     req.Kind,
		scheduledGranules:        len(refs),
		columnAssetReadIntegrity: columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity),
	}
	groupCodes := make(map[string]int)
	var rawScratch []byte
	for _, metadataRef := range refs {
		raw, err := readCache.read(metadataRef.AssetRef, rawScratch)
		if err != nil {
			return nil, fmt.Errorf("collections: aggregate metadata read %q generation=%d part_id=%d: %w", metadataRef.Name, metadataRef.AssetRef.Generation, metadataRef.AssetRef.PartID, err)
		}
		rawScratch = raw
		runner.assetBytes += int64(len(raw))
		asset, err := decodeColumnAggregateMetadataAsset(raw, metadataRef.AssetRef, view.Config, view.CollectionName, aggregate.Name)
		if err != nil {
			return nil, err
		}
		if asset.GroupColumn != req.GroupColumn || asset.ValueColumn != req.ValueColumn {
			return nil, fmt.Errorf("%w: aggregate metadata %q columns %s/%s do not match query %s/%s", ErrColumnQueryPlanUnsupported, aggregate.Name, asset.GroupColumn, asset.ValueColumn, req.GroupColumn, req.ValueColumn)
		}
		runner.metadataHits++
		for _, entry := range asset.Entries {
			code, ok := groupCodes[entry.Group]
			if !ok {
				code = len(runner.groupKeys)
				groupCodes[entry.Group] = code
				runner.groupKeys = append(runner.groupKeys, entry.Group)
			}
			runner.entries = append(runner.entries, columnAggregateMetadataRunnerEntry{
				groupCode: code,
				min:       entry.Min,
				max:       entry.Max,
			})
			runner.rows += entry.Count
		}
	}
	groupCount := len(runner.groupKeys)
	runner.present = make([]bool, groupCount)
	runner.mins = make([]int64, groupCount)
	runner.maxs = make([]int64, groupCount)
	runner.resultGroups = make([]ColumnPhysicalQueryGroup, 0, groupCount)
	return runner, nil
}

func (r *columnAggregateMetadataRunner) run(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) ColumnPhysicalQueryResult {
	start := time.Now()
	for i := range r.present {
		r.present[i] = false
	}
	for _, entry := range r.entries {
		code := entry.groupCode
		if !r.present[code] {
			r.present[code] = true
			r.mins[code] = entry.min
			r.maxs[code] = entry.max
			continue
		}
		if entry.min < r.mins[code] {
			r.mins[code] = entry.min
		}
		if entry.max > r.maxs[code] {
			r.maxs[code] = entry.max
		}
	}
	r.resultGroups = r.resultGroups[:0]
	for code, key := range r.groupKeys {
		if !r.present[code] {
			continue
		}
		group := ColumnPhysicalQueryGroup{Key: key}
		switch r.kind {
		case ColumnPhysicalQueryGroupMinInt64:
			group.Int64 = r.mins[code]
		case ColumnPhysicalQueryGroupMaxInt64:
			group.Int64 = r.maxs[code]
		case ColumnPhysicalQueryGroupInt64Span:
			group.Int64 = r.maxs[code] - r.mins[code]
		}
		r.resultGroups = append(r.resultGroups, group)
	}
	sortColumnPhysicalQueryGroupsByKey(r.resultGroups)
	diag := columnPhysicalQueryDiagnosticsFromScan(view.Diagnostics)
	diag.WorkerCount = 1
	diag.ProjectedColumns = 2
	diag.MetadataHits = r.metadataHits
	diag.ScheduledGranules = r.scheduledGranules
	diag.PhysicalBytesScanned = r.assetBytes
	diag.ReduceRows = r.rows
	diag.ResultGroups = len(r.resultGroups)
	diag.ColumnAssetReadIntegrity = r.columnAssetReadIntegrity
	diag.ScanNanos = time.Since(start).Nanoseconds()
	return ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}
}
