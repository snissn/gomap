package collections

import (
	"errors"
	"fmt"
	"time"
)

type columnDictionaryCodeGroupCountRunner struct {
	column       string
	dictionary   []string
	counts       []int
	assets       []columnDictionaryCodeGroupCountAsset
	resultGroups []ColumnPhysicalQueryGroup
	assetBytes   int64
}

type columnDictionaryCodeGroupCountDistinctRunner struct {
	groupColumn    string
	distinctColumn string
	groupDict      []string
	groupCounts    []int
	seen           []uint64
	wordsPerGroup  int
	assets         []columnDictionaryCodeGroupCountDistinctAsset
	resultGroups   []ColumnPhysicalQueryGroup
	assetBytes     int64
}

type columnDictionaryCodeGroupCountAsset struct {
	codes []uint32
}

type columnDictionaryCodeGroupCountDistinctAsset struct {
	groupCodes    []uint32
	distinctCodes []uint32
}

func prepareColumnDictionaryCodeGroupCountRunner(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache) (*columnDictionaryCodeGroupCountRunner, error) {
	if req.Kind != ColumnPhysicalQueryGroupCount || req.GroupColumn == "" || view.MutationParts != 0 {
		return nil, nil
	}
	if readCache == nil {
		return nil, fmt.Errorf("collections: dictionary code query missing read cache")
	}
	col, _, ok := columnPhysicalQueryDeclaredColumn(view.Config, req.GroupColumn)
	if !ok || col.ValueType != ColumnStoreValueString || !col.Dictionary {
		return nil, nil
	}
	byPart := make(map[[2]uint64]columnManifestDictionaryCodesSnapshot, len(view.DictionaryCodes))
	for _, snapshot := range view.DictionaryCodes {
		if snapshot.ColumnName != req.GroupColumn {
			continue
		}
		byPart[[2]uint64{snapshot.AssetRef.Generation, snapshot.AssetRef.PartID}] = snapshot
	}
	if len(byPart) == 0 {
		return nil, nil
	}
	globalByValue := make(map[string]uint32)
	runner := &columnDictionaryCodeGroupCountRunner{
		column: req.GroupColumn,
		assets: make([]columnDictionaryCodeGroupCountAsset, 0, len(view.AssetRefs)),
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
			return nil, fmt.Errorf("collections: dictionary codes read generation=%d part_id=%d column=%q: %w", snapshot.AssetRef.Generation, snapshot.AssetRef.PartID, req.GroupColumn, err)
		}
		scratch = raw
		decoded, err := decodeColumnDictionaryCodesAsset(raw, snapshot.AssetRef, view.Config, view.CollectionName, req.GroupColumn, readCache.verifyChecksum)
		if err != nil {
			return nil, err
		}
		translated := make([]uint32, len(decoded.Codes))
		for codeIdx, localCode := range decoded.Codes {
			value := decoded.Dictionary[localCode]
			globalCode, ok := globalByValue[value]
			if !ok {
				if uint64(len(runner.dictionary)) == uint64(^uint32(0)) {
					return nil, fmt.Errorf("collections: dictionary code query cardinality exceeds uint32")
				}
				globalCode = uint32(len(runner.dictionary))
				globalByValue[value] = globalCode
				runner.dictionary = append(runner.dictionary, value)
			}
			translated[codeIdx] = globalCode
		}
		runner.assets = append(runner.assets, columnDictionaryCodeGroupCountAsset{
			codes: translated,
		})
		runner.assetBytes += snapshot.AssetRef.Length
	}
	if len(runner.assets) == 0 || len(runner.dictionary) == 0 {
		return nil, nil
	}
	runner.counts = make([]int, len(runner.dictionary))
	runner.resultGroups = make([]ColumnPhysicalQueryGroup, 0, len(runner.dictionary))
	return runner, nil
}

func (r *columnDictionaryCodeGroupCountRunner) run(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) ColumnPhysicalQueryResult {
	start := time.Now()
	clear(r.counts)
	rows := 0
	for _, asset := range r.assets {
		for _, code := range asset.codes {
			r.counts[code]++
			rows++
		}
	}
	r.resultGroups = r.resultGroups[:0]
	for code, count := range r.counts {
		if count == 0 {
			continue
		}
		r.resultGroups = append(r.resultGroups, ColumnPhysicalQueryGroup{
			Key:   r.dictionary[code],
			Count: count,
		})
	}
	sortColumnPhysicalQueryGroupsByKey(r.resultGroups)
	diag := columnPhysicalQueryDiagnosticsFromScan(view.Diagnostics)
	diag.WorkerCount = 1
	diag.ProjectedColumns = 1
	diag.ScheduledGranules = len(r.assets)
	diag.DecodedBlocks = len(r.assets)
	diag.DirectReduceBlocks = len(r.assets)
	diag.RowsScanned = rows
	diag.PhysicalBytesScanned = r.assetBytes
	diag.ReduceRows = rows
	diag.ResultGroups = len(r.resultGroups)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	diag.ScanNanos = time.Since(start).Nanoseconds()
	return ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}
}

func prepareColumnDictionaryCodeGroupCountDistinctRunner(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache) (*columnDictionaryCodeGroupCountDistinctRunner, error) {
	if req.Kind != ColumnPhysicalQueryGroupCountDistinct || req.GroupColumn == "" || req.DistinctColumn == "" || view.MutationParts != 0 {
		return nil, nil
	}
	if readCache == nil {
		return nil, fmt.Errorf("collections: dictionary code distinct query missing read cache")
	}
	groupCol, _, ok := columnPhysicalQueryDeclaredColumn(view.Config, req.GroupColumn)
	if !ok || groupCol.ValueType != ColumnStoreValueString || !groupCol.Dictionary {
		return nil, nil
	}
	distinctCol, _, ok := columnPhysicalQueryDeclaredColumn(view.Config, req.DistinctColumn)
	if !ok || distinctCol.ValueType != ColumnStoreValueString || !distinctCol.Dictionary {
		return nil, nil
	}
	groupByPart := columnDictionaryCodeSnapshotsByPart(view, req.GroupColumn)
	distinctByPart := columnDictionaryCodeSnapshotsByPart(view, req.DistinctColumn)
	if len(groupByPart) == 0 || len(distinctByPart) == 0 {
		return nil, nil
	}
	runner := &columnDictionaryCodeGroupCountDistinctRunner{
		groupColumn:    req.GroupColumn,
		distinctColumn: req.DistinctColumn,
		assets:         make([]columnDictionaryCodeGroupCountDistinctAsset, 0, len(view.AssetRefs)),
	}
	groupByValue := make(map[string]uint32)
	distinctByValue := make(map[string]uint32)
	var scratch []byte
	for _, part := range view.AssetRefs {
		if part.Reason != ColumnPublishOperationInsert {
			return nil, nil
		}
		partKey := [2]uint64{part.Ref.Generation, part.Ref.PartID}
		groupSnapshot, ok := groupByPart[partKey]
		if !ok {
			return nil, nil
		}
		distinctSnapshot, ok := distinctByPart[partKey]
		if !ok {
			return nil, nil
		}
		groupRaw, err := readCache.read(groupSnapshot.AssetRef, scratch)
		if err != nil {
			return nil, fmt.Errorf("collections: dictionary codes read generation=%d part_id=%d column=%q: %w", groupSnapshot.AssetRef.Generation, groupSnapshot.AssetRef.PartID, req.GroupColumn, err)
		}
		scratch = groupRaw
		groupAsset, err := decodeColumnDictionaryCodesAsset(groupRaw, groupSnapshot.AssetRef, view.Config, view.CollectionName, req.GroupColumn, readCache.verifyChecksum)
		if err != nil {
			return nil, err
		}
		distinctRaw, err := readCache.read(distinctSnapshot.AssetRef, scratch)
		if err != nil {
			return nil, fmt.Errorf("collections: dictionary codes read generation=%d part_id=%d column=%q: %w", distinctSnapshot.AssetRef.Generation, distinctSnapshot.AssetRef.PartID, req.DistinctColumn, err)
		}
		scratch = distinctRaw
		distinctAsset, err := decodeColumnDictionaryCodesAsset(distinctRaw, distinctSnapshot.AssetRef, view.Config, view.CollectionName, req.DistinctColumn, readCache.verifyChecksum)
		if err != nil {
			return nil, err
		}
		if len(groupAsset.Codes) != len(distinctAsset.Codes) {
			return nil, fmt.Errorf("collections: dictionary code distinct row count mismatch group=%d distinct=%d", len(groupAsset.Codes), len(distinctAsset.Codes))
		}
		groupCodes := make([]uint32, len(groupAsset.Codes))
		for codeIdx, localCode := range groupAsset.Codes {
			value := groupAsset.Dictionary[localCode]
			globalCode, ok := groupByValue[value]
			if !ok {
				if uint64(len(runner.groupDict)) == uint64(^uint32(0)) {
					return nil, fmt.Errorf("collections: dictionary code distinct group cardinality exceeds uint32")
				}
				globalCode = uint32(len(runner.groupDict))
				groupByValue[value] = globalCode
				runner.groupDict = append(runner.groupDict, value)
			}
			groupCodes[codeIdx] = globalCode
		}
		distinctCodes := make([]uint32, len(distinctAsset.Codes))
		for codeIdx, localCode := range distinctAsset.Codes {
			value := distinctAsset.Dictionary[localCode]
			globalCode, ok := distinctByValue[value]
			if !ok {
				if uint64(len(distinctByValue)) == uint64(^uint32(0)) {
					return nil, fmt.Errorf("collections: dictionary code distinct cardinality exceeds uint32")
				}
				globalCode = uint32(len(distinctByValue))
				distinctByValue[value] = globalCode
			}
			distinctCodes[codeIdx] = globalCode
		}
		runner.assets = append(runner.assets, columnDictionaryCodeGroupCountDistinctAsset{
			groupCodes:    groupCodes,
			distinctCodes: distinctCodes,
		})
		runner.assetBytes += groupSnapshot.AssetRef.Length + distinctSnapshot.AssetRef.Length
	}
	if len(runner.assets) == 0 || len(runner.groupDict) == 0 || len(distinctByValue) == 0 {
		return nil, nil
	}
	wordsPerGroup, totalWords, err := columnDictionaryCodeDistinctSeenWords(len(runner.groupDict), len(distinctByValue))
	if err != nil {
		return nil, err
	}
	runner.wordsPerGroup = wordsPerGroup
	runner.groupCounts = make([]int, len(runner.groupDict))
	runner.seen = make([]uint64, totalWords)
	runner.resultGroups = make([]ColumnPhysicalQueryGroup, 0, len(runner.groupDict))
	return runner, nil
}

func columnDictionaryCodeDistinctSeenWords(groupCount, distinctCount int) (int, int, error) {
	if groupCount <= 0 || distinctCount <= 0 {
		return 0, 0, errors.New("collections: dictionary code distinct query requires non-empty group and distinct dictionaries")
	}
	wordsPerGroup := (distinctCount + 63) / 64
	if wordsPerGroup != 0 && groupCount > maxCollectionInt/wordsPerGroup {
		return 0, 0, errors.New("collections: dictionary code distinct bitset dimensions overflow int")
	}
	return wordsPerGroup, groupCount * wordsPerGroup, nil
}

func columnDictionaryCodeSnapshotsByPart(view columnPhysicalScanSnapshotView, column string) map[[2]uint64]columnManifestDictionaryCodesSnapshot {
	byPart := make(map[[2]uint64]columnManifestDictionaryCodesSnapshot, len(view.DictionaryCodes))
	for _, snapshot := range view.DictionaryCodes {
		if snapshot.ColumnName != column {
			continue
		}
		byPart[[2]uint64{snapshot.AssetRef.Generation, snapshot.AssetRef.PartID}] = snapshot
	}
	return byPart
}

func (r *columnDictionaryCodeGroupCountDistinctRunner) run(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) ColumnPhysicalQueryResult {
	start := time.Now()
	clear(r.groupCounts)
	clear(r.seen)
	rows := 0
	for _, asset := range r.assets {
		for rowIdx, groupCode := range asset.groupCodes {
			distinctCode := asset.distinctCodes[rowIdx]
			seenIdx := int(groupCode)*r.wordsPerGroup + int(distinctCode/64)
			mask := uint64(1) << (distinctCode & 63)
			if r.seen[seenIdx]&mask == 0 {
				r.seen[seenIdx] |= mask
				r.groupCounts[groupCode]++
			}
			rows++
		}
	}
	r.resultGroups = r.resultGroups[:0]
	for code, count := range r.groupCounts {
		if count == 0 {
			continue
		}
		r.resultGroups = append(r.resultGroups, ColumnPhysicalQueryGroup{
			Key:   r.groupDict[code],
			Count: count,
		})
	}
	sortColumnPhysicalQueryGroupsByKey(r.resultGroups)
	diag := columnPhysicalQueryDiagnosticsFromScan(view.Diagnostics)
	diag.WorkerCount = 1
	diag.ProjectedColumns = 2
	diag.ScheduledGranules = len(r.assets)
	diag.DecodedBlocks = len(r.assets)
	diag.DirectReduceBlocks = len(r.assets)
	diag.RowsScanned = rows
	diag.PhysicalBytesScanned = r.assetBytes
	diag.ReduceRows = rows
	diag.ResultGroups = len(r.resultGroups)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	diag.ScanNanos = time.Since(start).Nanoseconds()
	return ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}
}
