package collections

import (
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

type columnAggregateMetadataRunnerEntry struct {
	groupCode int
	count     int
	min       int64
	max       int64
}

type columnAggregateMetadataRunner struct {
	kind                     ColumnPhysicalQueryKind
	assetBytes               int64
	decodedMetadataBytes     uint64
	mappedBytes              uint64
	heapCopyBytes            uint64
	metadataHits             int
	metadataEntries          int
	rows                     int
	scheduledGranules        int
	groupKeys                []string
	groupHours               []int
	entries                  []columnAggregateMetadataRunnerEntry
	seenGeneration           []uint32
	runGeneration            uint32
	touchedCodes             []int
	counts                   []int
	mins                     []int64
	maxs                     []int64
	resultGroups             []ColumnPhysicalQueryGroup
	columnAssetReadIntegrity string
	predicateDiagnostics     columnPhysicalQueryPredicateDiagnosticPlan
}

type columnAggregateMetadataRunnerGroupKey struct {
	group string
	hour  int
}

func prepareColumnAggregateMetadataRunner(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache) (*columnAggregateMetadataRunner, error) {
	if view.MutationParts != 0 {
		return nil, fmt.Errorf("%w: prepared aggregate metadata query requires insert-only manifest", ErrColumnQueryPlanUnsupported)
	}
	if readCache == nil {
		return nil, fmt.Errorf("collections: prepared aggregate metadata query requires read cache")
	}
	aggregate, ok := columnPhysicalQueryAggregateMetadataConfig(view.FullConfig, req)
	if !ok {
		return nil, fmt.Errorf("%w: aggregate metadata %q does not match physical query shape", ErrColumnQueryPlanUnsupported, req.AggregateMetadataName)
	}
	refs := columnPhysicalQueryAggregateMetadataRefs(view.AggregateMetadata, aggregate.Name)
	if len(refs) == 0 {
		return nil, nil
	}
	mgr := mappedresource.NewManager()
	if err := readCache.useMappedResourceManager(mgr, mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "column-aggregate-metadata-runner-prepare", Collection: view.CollectionName, Namespace: view.AssetNamespace, Generation: view.Diagnostics.ManifestGeneration, Reason: "prepared column aggregate metadata query"}, "prepared column aggregate metadata query"); err != nil {
		return nil, err
	}
	runner := &columnAggregateMetadataRunner{
		kind:                     req.Kind,
		scheduledGranules:        len(refs),
		columnAssetReadIntegrity: columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity),
		predicateDiagnostics:     newColumnPhysicalQueryPredicateDiagnosticPlan(req),
	}
	groupCodes := make(map[columnAggregateMetadataRunnerGroupKey]int)
	var rawScratch []byte
	for _, metadataRef := range refs {
		raw, err := readCache.read(metadataRef.AssetRef, rawScratch)
		if err != nil {
			return nil, fmt.Errorf("collections: aggregate metadata read %q generation=%d part_id=%d: %w", metadataRef.Name, metadataRef.AssetRef.Generation, metadataRef.AssetRef.PartID, err)
		}
		rawScratch = raw
		runner.assetBytes += int64(len(raw))
		runner.decodedMetadataBytes += uint64(len(raw))
		asset, err := decodeColumnAggregateMetadataAsset(raw, metadataRef.AssetRef, view.FullConfig, view.CollectionName, aggregate.Name)
		if err != nil {
			return nil, err
		}
		if asset.GroupColumn != req.GroupColumn || asset.ValueColumn != req.ValueColumn {
			return nil, fmt.Errorf("%w: aggregate metadata %q columns %s/%s do not match query %s/%s", ErrColumnQueryPlanUnsupported, aggregate.Name, asset.GroupColumn, asset.ValueColumn, req.GroupColumn, req.ValueColumn)
		}
		if !columnPhysicalQueryPredicatesExactEqual(asset.Predicates, aggregate.Predicates) {
			return nil, fmt.Errorf("%w: aggregate metadata %q predicate coverage does not match declared metadata", ErrColumnQueryPlanUnsupported, aggregate.Name)
		}
		runner.metadataHits++
		for _, entry := range asset.Entries {
			groupKey := columnAggregateMetadataRunnerKey(req, entry)
			code, ok := groupCodes[groupKey]
			if !ok {
				code = len(runner.groupKeys)
				groupCodes[groupKey] = code
				runner.groupKeys = append(runner.groupKeys, entry.Group)
				runner.groupHours = append(runner.groupHours, groupKey.hour)
			}
			runner.entries = append(runner.entries, columnAggregateMetadataRunnerEntry{
				groupCode: code,
				count:     entry.Count,
				min:       entry.Min,
				max:       entry.Max,
			})
			runner.metadataEntries++
			runner.rows += entry.Count
		}
	}
	groupCount := len(runner.groupKeys)
	runner.seenGeneration = make([]uint32, groupCount)
	runner.touchedCodes = make([]int, 0, groupCount)
	if req.Kind == ColumnPhysicalQueryGroupCount || req.Kind == ColumnPhysicalQueryGroupHourCount {
		runner.counts = make([]int, groupCount)
	}
	runner.mins = make([]int64, groupCount)
	runner.maxs = make([]int64, groupCount)
	resultCap := groupCount
	if req.TopK > 0 && req.TopK < resultCap {
		resultCap = req.TopK
	}
	runner.resultGroups = make([]ColumnPhysicalQueryGroup, 0, resultCap)
	stats := readCache.mappedResourceStats()
	runner.mappedBytes = stats.TotalMappedBytes
	runner.heapCopyBytes = stats.TotalHeapCopyBytes
	return runner, nil
}

func columnAggregateMetadataRunnerKey(req ColumnPhysicalQueryRequest, entry columnAggregateMetadataEntry) columnAggregateMetadataRunnerGroupKey {
	key := columnAggregateMetadataRunnerGroupKey{group: entry.Group}
	if req.Kind == ColumnPhysicalQueryGroupHourCount {
		key.hour = entry.Hour
	}
	return key
}

func (r *columnAggregateMetadataRunner) run(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) ColumnPhysicalQueryResult {
	start := time.Now()
	reduceStart := start
	r.reduceEntries()
	reduceNanos := time.Since(reduceStart).Nanoseconds()

	shapeStart := time.Now()
	r.resultGroups = r.resultGroups[:0]
	if req.TopK > 0 {
		r.appendTopKGroups(req)
	} else {
		r.appendAllGroups()
	}
	shapeNanos := time.Since(shapeStart).Nanoseconds()

	diag := columnPhysicalQueryDiagnosticsFromScan(view.Diagnostics)
	diag.WorkerCount = 1
	projectedColumns := 2
	if req.Kind == ColumnPhysicalQueryGroupCount {
		projectedColumns = 1
	}
	diag.ProjectedColumns = columnPhysicalQueryDiagnosticProjectedColumns(r.predicateDiagnostics, projectedColumns)
	diag.MetadataHits = r.metadataHits
	diag.MetadataEntries = r.metadataEntries
	diag.ScheduledGranules = r.scheduledGranules
	diag.PhysicalBytesScanned = r.assetBytes
	diag.DecodedMetadataBytes = r.decodedMetadataBytes
	diag.MappedBytes = r.mappedBytes
	diag.HeapCopyBytes = r.heapCopyBytes
	diag.ReduceRows = r.rows
	applyColumnPhysicalQueryPredicateDiagnostics(&diag, r.predicateDiagnostics, r.rows, 0)
	diag.ResultGroups = len(r.resultGroups)
	diag.TopKLimit = req.TopK
	diag.TopKCandidates = columnPhysicalTopKCandidates(req, r.touchedCodes, r.groupKeys)
	diag.TopKOrder = string(req.TopKOrder)
	diag.ColumnAssetReadIntegrity = r.columnAssetReadIntegrity
	diag.ScanNanos = time.Since(start).Nanoseconds()
	diag.ReduceNanos = reduceNanos
	diag.ResultShapeNanos = shapeNanos
	return ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}
}

func (r *columnAggregateMetadataRunner) reduceEntries() {
	generation := r.nextGeneration()
	r.touchedCodes = r.touchedCodes[:0]
	for _, entry := range r.entries {
		code := entry.groupCode
		if r.seenGeneration[code] != generation {
			r.seenGeneration[code] = generation
			r.touchedCodes = append(r.touchedCodes, code)
			if r.kind == ColumnPhysicalQueryGroupCount || r.kind == ColumnPhysicalQueryGroupHourCount {
				r.counts[code] = entry.count
				continue
			}
			r.mins[code] = entry.min
			r.maxs[code] = entry.max
			continue
		}
		if r.kind == ColumnPhysicalQueryGroupCount || r.kind == ColumnPhysicalQueryGroupHourCount {
			r.counts[code] += entry.count
			continue
		}
		if entry.min < r.mins[code] {
			r.mins[code] = entry.min
		}
		if entry.max > r.maxs[code] {
			r.maxs[code] = entry.max
		}
	}
}

func (r *columnAggregateMetadataRunner) nextGeneration() uint32 {
	r.runGeneration++
	if r.runGeneration == 0 {
		clear(r.seenGeneration)
		r.runGeneration = 1
	}
	return r.runGeneration
}

func (r *columnAggregateMetadataRunner) appendAllGroups() {
	for _, code := range r.touchedCodes {
		r.resultGroups = append(r.resultGroups, r.groupForCode(code))
	}
	if r.kind == ColumnPhysicalQueryGroupHourCount {
		sortColumnPhysicalQueryGroupsByKeyHour(r.resultGroups)
	} else {
		sortColumnPhysicalQueryGroupsByKey(r.resultGroups)
	}
}

func (r *columnAggregateMetadataRunner) appendTopKGroups(req ColumnPhysicalQueryRequest) {
	for _, code := range r.touchedCodes {
		key := r.groupKeys[code]
		if req.SkipEmptyGroupKey && key == "" {
			continue
		}
		insertColumnPhysicalTopKGroup(&r.resultGroups, r.groupForCode(code), req.TopK, req.TopKOrder)
	}
}

func (r *columnAggregateMetadataRunner) groupForCode(code int) ColumnPhysicalQueryGroup {
	group := ColumnPhysicalQueryGroup{Key: r.groupKeys[code]}
	switch r.kind {
	case ColumnPhysicalQueryGroupCount:
		group.Count = r.counts[code]
	case ColumnPhysicalQueryGroupHourCount:
		group.Hour = r.groupHours[code]
		group.Count = r.counts[code]
	case ColumnPhysicalQueryGroupMinInt64:
		group.Int64 = r.mins[code]
	case ColumnPhysicalQueryGroupMaxInt64:
		group.Int64 = r.maxs[code]
	case ColumnPhysicalQueryGroupInt64Span:
		group.Int64 = r.maxs[code] - r.mins[code]
	}
	return group
}

func columnPhysicalTopKCandidates(req ColumnPhysicalQueryRequest, touched []int, groupKeys []string) int {
	if req.TopK <= 0 {
		return 0
	}
	if !req.SkipEmptyGroupKey {
		return len(touched)
	}
	candidates := 0
	for _, code := range touched {
		if groupKeys[code] != "" {
			candidates++
		}
	}
	return candidates
}

func insertColumnPhysicalTopKGroup(groups *[]ColumnPhysicalQueryGroup, group ColumnPhysicalQueryGroup, limit int, order ColumnPhysicalQueryTopKOrder) {
	if limit <= 0 {
		return
	}
	out := *groups
	if len(out) < limit {
		out = append(out, group)
		for i := len(out) - 1; i > 0 && columnPhysicalTopKLess(order, out[i], out[i-1]); i-- {
			out[i], out[i-1] = out[i-1], out[i]
		}
		*groups = out
		return
	}
	if !columnPhysicalTopKLess(order, group, out[len(out)-1]) {
		return
	}
	out[len(out)-1] = group
	for i := len(out) - 1; i > 0 && columnPhysicalTopKLess(order, out[i], out[i-1]); i-- {
		out[i], out[i-1] = out[i-1], out[i]
	}
	*groups = out
}

func columnPhysicalTopKLess(order ColumnPhysicalQueryTopKOrder, a, b ColumnPhysicalQueryGroup) bool {
	if a.Int64 != b.Int64 {
		if order == ColumnPhysicalQueryTopKInt64Desc {
			return a.Int64 > b.Int64
		}
		return a.Int64 < b.Int64
	}
	return a.Key < b.Key
}
