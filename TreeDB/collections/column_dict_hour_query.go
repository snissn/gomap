package collections

import (
	"cmp"
	"fmt"
	"slices"
	"time"
)

const columnDictionaryHourCountMaxGroups = 1 << 16

type columnDictionaryHourCountRunner struct {
	groupColumn          string
	valueColumn          string
	groupDict            []string
	assets               []columnDictionaryHourCountAsset
	predicateAssets      []columnDictionaryPredicateAsset
	counts               []int
	result               []ColumnPhysicalQueryGroup
	assetBytes           int64
	predicateBytes       int64
	predicateCodeHit     int
	predicateDiagnostics columnPhysicalQueryPredicateDiagnosticPlan
}

type columnDictionaryHourCountAsset struct {
	groupCodes []uint32
	values     []int64
}

func prepareColumnDictionaryHourCountRunner(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache) (*columnDictionaryHourCountRunner, error) {
	if req.AggregateMetadataName != "" || req.Kind != ColumnPhysicalQueryGroupHourCount || req.GroupColumn == "" || req.ValueColumn == "" || view.MutationParts != 0 {
		return nil, nil
	}
	if readCache == nil {
		return nil, fmt.Errorf("collections: dictionary/hour query missing read cache")
	}
	if !columnDictionaryInt64GroupColumnsEligible(view, req) {
		return nil, nil
	}
	runner := &columnDictionaryHourCountRunner{
		groupColumn: req.GroupColumn,
		valueColumn: req.ValueColumn,
		assets:      make([]columnDictionaryHourCountAsset, 0, len(view.AssetRefs)),
	}
	if len(view.AssetRefs) == 0 {
		if _, err := columnPhysicalQueryPredicateSpecs(view.Config, req); err != nil {
			return nil, err
		}
		runner.predicateDiagnostics = newColumnPhysicalQueryPredicateDiagnosticPlan(req)
		runner.initScratch()
		return runner, nil
	}
	groupByPart := columnDictionaryCodeSnapshotsByPart(view, req.GroupColumn)
	valueByPart := columnInt64ValueSnapshotsByPart(view, req.ValueColumn)
	if len(groupByPart) == 0 || len(valueByPart) == 0 || !columnDictionaryInt64GroupSnapshotsCoverParts(view, groupByPart, valueByPart) {
		return nil, nil
	}
	runner = &columnDictionaryHourCountRunner{
		groupColumn: req.GroupColumn,
		valueColumn: req.ValueColumn,
		assets:      make([]columnDictionaryHourCountAsset, 0, len(view.AssetRefs)),
	}
	predicateAssets, predicateBytes, err := prepareColumnDictionaryPredicateAssets(view, req, readCache)
	if err != nil {
		return nil, err
	}
	runner.predicateAssets = predicateAssets
	runner.predicateBytes = predicateBytes
	runner.predicateCodeHit = columnDictionaryPredicateAssetHits(predicateAssets)
	runner.predicateDiagnostics = newColumnPhysicalQueryPredicateDiagnosticPlan(req)
	groupByValue := make(map[string]uint32)
	var scratch []byte
	for _, part := range view.AssetRefs {
		partKey := [2]uint64{part.Ref.Generation, part.Ref.PartID}
		groupSnapshot := groupByPart[partKey]
		valueSnapshot := valueByPart[partKey]
		groupRaw, err := readCache.read(groupSnapshot.AssetRef, scratch)
		if err != nil {
			return nil, fmt.Errorf("collections: dictionary/hour group codes read generation=%d part_id=%d column=%q: %w", groupSnapshot.AssetRef.Generation, groupSnapshot.AssetRef.PartID, req.GroupColumn, err)
		}
		scratch = groupRaw
		groupAsset, err := decodeColumnDictionaryCodesAsset(groupRaw, groupSnapshot.AssetRef, view.Config, view.CollectionName, req.GroupColumn, false)
		if err != nil {
			return nil, err
		}
		valueRaw, err := readCache.read(valueSnapshot.AssetRef, scratch)
		if err != nil {
			return nil, fmt.Errorf("collections: dictionary/hour values read generation=%d part_id=%d column=%q: %w", valueSnapshot.AssetRef.Generation, valueSnapshot.AssetRef.PartID, req.ValueColumn, err)
		}
		valueRawIsView := readCache.lastView
		scratch = valueRaw
		_, valuePayload, err := decodeColumnInt64ValuesAssetPayload(valueRaw, valueSnapshot.AssetRef, view.Config, view.CollectionName, req.ValueColumn, false)
		if err != nil {
			return nil, err
		}
		if len(groupAsset.Codes) != valuePayload.rowCount || valuePayload.rowCount != valueSnapshot.Rows {
			return nil, fmt.Errorf("collections: dictionary/hour row count mismatch group=%d values=%d manifest=%d", len(groupAsset.Codes), valuePayload.rowCount, valueSnapshot.Rows)
		}
		var values []int64
		if valueRawIsView {
			values, _, err = viewColumnInt64ValuesPayload(valueRaw, valuePayload)
		} else {
			values, err = copyColumnInt64ValuesPayload(valueRaw, valuePayload)
		}
		if err != nil {
			return nil, err
		}
		groupCodes := make([]uint32, len(groupAsset.Codes))
		for codeIdx, localCode := range groupAsset.Codes {
			value := groupAsset.Dictionary[localCode]
			globalCode, ok := groupByValue[value]
			if !ok {
				if len(runner.groupDict) >= columnDictionaryHourCountMaxGroups || uint64(len(runner.groupDict)) == uint64(^uint32(0)) {
					return nil, nil
				}
				globalCode = uint32(len(runner.groupDict))
				groupByValue[value] = globalCode
				runner.groupDict = append(runner.groupDict, value)
			}
			groupCodes[codeIdx] = globalCode
		}
		runner.assets = append(runner.assets, columnDictionaryHourCountAsset{
			groupCodes: groupCodes,
			values:     values,
		})
		runner.assetBytes += groupSnapshot.AssetRef.Length + valueSnapshot.AssetRef.Length
	}
	runner.initScratch()
	return runner, nil
}

func (r *columnDictionaryHourCountRunner) initScratch() {
	groups := len(r.groupDict)
	r.counts = make([]int, groups*24)
	resultCap := groups * 24
	if resultCap > 1024 {
		resultCap = 1024
	}
	r.result = make([]ColumnPhysicalQueryGroup, 0, resultCap)
}

func (r *columnDictionaryHourCountRunner) run(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) ColumnPhysicalQueryResult {
	start := time.Now()
	clear(r.counts)
	rows := 0
	matched := 0
	if len(r.predicateAssets) == 0 {
		for _, asset := range r.assets {
			rows += len(asset.groupCodes)
			for rowIdx, groupCode := range asset.groupCodes {
				hour := columnPhysicalQueryUTCHour(asset.values[rowIdx])
				r.counts[int(groupCode)*24+hour]++
			}
		}
		matched = rows
	} else {
		for assetIdx, asset := range r.assets {
			predicates := &r.predicateAssets[assetIdx]
			rows += len(asset.groupCodes)
			if predicates.rejectsAll {
				continue
			}
			fast, ok := predicates.fastPath(len(asset.groupCodes))
			if !ok {
				for rowIdx, groupCode := range asset.groupCodes {
					if !predicates.matches(rowIdx) {
						continue
					}
					hour := columnPhysicalQueryUTCHour(asset.values[rowIdx])
					r.counts[int(groupCode)*24+hour]++
					matched++
				}
				continue
			}
			switch fast.predicateCount() {
			case 1:
				for rowIdx, groupCode := range asset.groupCodes {
					if !fast.matches1(rowIdx) {
						continue
					}
					hour := columnPhysicalQueryUTCHour(asset.values[rowIdx])
					r.counts[int(groupCode)*24+hour]++
					matched++
				}
			case 2:
				for rowIdx, groupCode := range asset.groupCodes {
					if !fast.matches2(rowIdx) {
						continue
					}
					hour := columnPhysicalQueryUTCHour(asset.values[rowIdx])
					r.counts[int(groupCode)*24+hour]++
					matched++
				}
			default:
				for rowIdx, groupCode := range asset.groupCodes {
					if !fast.matches(rowIdx) {
						continue
					}
					hour := columnPhysicalQueryUTCHour(asset.values[rowIdx])
					r.counts[int(groupCode)*24+hour]++
					matched++
				}
			}
		}
	}
	r.result = r.result[:0]
	for code, key := range r.groupDict {
		base := code * 24
		for hour := 0; hour < 24; hour++ {
			count := r.counts[base+hour]
			if count == 0 {
				continue
			}
			r.result = append(r.result, ColumnPhysicalQueryGroup{Key: key, Hour: hour, Count: count})
		}
	}
	sortColumnPhysicalQueryGroupsByKeyHour(r.result)
	diag := columnPhysicalQueryDiagnosticsFromScan(view.Diagnostics)
	diag.WorkerCount = 1
	diag.ProjectedColumns = columnPhysicalQueryDiagnosticProjectedColumns(r.predicateDiagnostics, 2)
	diag.ScheduledGranules = len(r.assets)
	diag.DecodedBlocks = len(r.assets)
	diag.DirectReduceBlocks = len(r.assets)
	diag.DictionaryCodeHits = len(r.assets)
	diag.PredicateDictionaryCodeHits = r.predicateCodeHit
	diag.Int64ValueHits = len(r.assets)
	diag.RowsScanned = rows
	diag.PhysicalBytesScanned = r.assetBytes + r.predicateBytes
	diag.ReduceRows = matched
	diag.ResultGroups = len(r.result)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	applyColumnPhysicalQueryPredicateDiagnostics(&diag, r.predicateDiagnostics, matched, r.predicateCodeHit)
	diag.ScanNanos = time.Since(start).Nanoseconds()
	return ColumnPhysicalQueryResult{Groups: r.result, Diagnostics: diag}
}

func sortColumnPhysicalQueryGroupsByKeyHour(groups []ColumnPhysicalQueryGroup) {
	const insertionSortLimit = 64
	less := func(a, b ColumnPhysicalQueryGroup) int {
		if c := cmp.Compare(a.Key, b.Key); c != 0 {
			return c
		}
		return cmp.Compare(a.Hour, b.Hour)
	}
	if len(groups) > insertionSortLimit {
		slices.SortFunc(groups, less)
		return
	}
	for i := 1; i < len(groups); i++ {
		group := groups[i]
		j := i - 1
		for ; j >= 0 && less(group, groups[j]) < 0; j-- {
			groups[j+1] = groups[j]
		}
		groups[j+1] = group
	}
}
