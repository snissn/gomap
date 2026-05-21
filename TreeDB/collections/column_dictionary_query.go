package collections

import (
	"errors"
	"fmt"
	"time"
)

const (
	columnDictionaryCodeDistinctMaxSeenWords = 1 << 20
	// Keep routed one-shot dictionary scans to small/medium payloads for now.
	// Larger snapshots use the prepared runner path until routed sessions can
	// reuse decoded dictionaries without moving setup into every query.
	columnDictionaryCodeOneShotMaxBytes          = 3 << 20
	columnDictionaryCodeDistinctOneShotMaxValues = 1 << 16
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

func columnDictionaryCodeIndex(code uint32, cardinality int) (int, bool) {
	if uint64(code) >= uint64(cardinality) {
		return 0, false
	}
	return int(code), true
}

type columnDictionaryCodeGroupCountOneShotReducer struct {
	dictionary    []string
	byValue       map[string]uint32
	localToGlobal []uint32
	counts        []int
	result        []ColumnPhysicalQueryGroup
}

type columnDictionaryCodeGroupCountDistinctOneShotAsset struct {
	groupRaw        []byte
	groupRef        ColumnAssetRef
	groupPayload    columnDictionaryCodesAssetPayload
	distinctRaw     []byte
	distinctRef     ColumnAssetRef
	distinctPayload columnDictionaryCodesAssetPayload
	bytes           int64
}

type columnDictionaryCodeGroupCountDistinctOneShotReducer struct {
	groupDict       []string
	groupByValue    map[string]uint32
	distinctByValue map[string]uint32
	groupLocal      []uint32
	distinctLocal   []uint32
	groupCounts     []int
	seen            []uint64
	wordsPerGroup   int
	assets          []columnDictionaryCodeGroupCountDistinctOneShotAsset
	result          []ColumnPhysicalQueryGroup
	assetBytes      int64
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
		dictCur, cardinality, rowCount, err := decodeColumnDictionaryCodesAssetHeader(raw, snapshot.AssetRef, view.Config, view.CollectionName, req.GroupColumn, false)
		if err != nil {
			return nil, err
		}
		localToGlobal := make([]uint32, cardinality)
		for localCode := 0; localCode < cardinality; localCode++ {
			value := dictCur.stringBytes()
			globalCode, ok := globalByValue[unsafeStringFromBytes(value)]
			if !ok {
				if uint64(len(runner.dictionary)) == uint64(^uint32(0)) {
					return nil, fmt.Errorf("collections: dictionary code query cardinality exceeds uint32")
				}
				globalCode = uint32(len(runner.dictionary))
				key := columnDictionaryCodeOwnedString(value)
				globalByValue[key] = globalCode
				runner.dictionary = append(runner.dictionary, key)
			}
			localToGlobal[localCode] = globalCode
			if dictCur.err != nil {
				break
			}
		}
		if dictCur.err != nil {
			return nil, dictCur.err
		}
		cur := manifestCursor{raw: raw, pos: dictCur.pos}
		translated := make([]uint32, rowCount)
		for codeIdx := range translated {
			localCode := cur.u32()
			localIdx, ok := columnDictionaryCodeIndex(localCode, len(localToGlobal))
			if !ok {
				return nil, fmt.Errorf("collections: dictionary codes asset code[%d]=%d outside cardinality=%d", codeIdx, localCode, len(localToGlobal))
			}
			translated[codeIdx] = localToGlobal[localIdx]
		}
		if cur.err != nil {
			return nil, cur.err
		}
		if cur.pos != len(raw) {
			return nil, errors.New("collections: trailing bytes in dictionary codes asset")
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

func runColumnDictionaryCodeGroupCountOneShot(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache) (ColumnPhysicalQueryResult, bool, error) {
	if req.Kind != ColumnPhysicalQueryGroupCount || req.GroupColumn == "" || view.MutationParts != 0 {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	if readCache == nil {
		return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: dictionary code query missing read cache")
	}
	col, _, ok := columnPhysicalQueryDeclaredColumn(view.Config, req.GroupColumn)
	if !ok || col.ValueType != ColumnStoreValueString || !col.Dictionary {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	byPart := columnDictionaryCodeSnapshotsByPart(view, req.GroupColumn)
	if len(byPart) == 0 || !columnDictionaryCodeSnapshotsCoverParts(view, byPart) {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	if bytes := columnDictionaryCodeSnapshotBytes(view, byPart); bytes > columnDictionaryCodeOneShotMaxBytes {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	start := time.Now()
	reducer := columnDictionaryCodeGroupCountOneShotReducer{
		dictionary: make([]string, 0, 16),
		byValue:    make(map[string]uint32, 16),
		counts:     make([]int, 0, 16),
		result:     make([]ColumnPhysicalQueryGroup, 0, 16),
	}
	var scratch []byte
	assetBytes := int64(0)
	blocks := 0
	rows := 0
	for _, part := range view.AssetRefs {
		snapshot := byPart[[2]uint64{part.Ref.Generation, part.Ref.PartID}]
		raw, err := readCache.read(snapshot.AssetRef, scratch)
		if err != nil {
			return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: dictionary codes read generation=%d part_id=%d column=%q: %w", snapshot.AssetRef.Generation, snapshot.AssetRef.PartID, req.GroupColumn, err)
		}
		scratch = raw
		if !readCache.lastView {
			return ColumnPhysicalQueryResult{}, false, nil
		}
		dictCur, cardinality, rowCount, err := decodeColumnDictionaryCodesAssetHeader(raw, snapshot.AssetRef, view.Config, view.CollectionName, req.GroupColumn, false)
		if err != nil {
			return ColumnPhysicalQueryResult{}, true, err
		}
		localToGlobal := reducer.prepareDictionary(cardinality)
		ok := true
		for localCode := 0; localCode < cardinality; localCode++ {
			if !reducer.translateDictionaryValue(localToGlobal, localCode, dictCur.stringBytes()) {
				ok = false
				break
			}
			if dictCur.err != nil {
				break
			}
		}
		if dictCur.err != nil {
			return ColumnPhysicalQueryResult{}, true, dictCur.err
		}
		if !ok {
			return ColumnPhysicalQueryResult{}, false, nil
		}
		cur := manifestCursor{raw: raw, pos: dictCur.pos}
		for i := 0; i < rowCount; i++ {
			localCode := cur.u32()
			if int(localCode) >= len(localToGlobal) {
				return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: dictionary codes asset code[%d]=%d outside cardinality=%d", i, localCode, len(localToGlobal))
			}
			reducer.counts[localToGlobal[localCode]]++
			rows++
		}
		if cur.err != nil {
			return ColumnPhysicalQueryResult{}, true, cur.err
		}
		if cur.pos != len(raw) {
			return ColumnPhysicalQueryResult{}, true, errors.New("collections: trailing bytes in dictionary codes asset")
		}
		assetBytes += snapshot.AssetRef.Length
		blocks++
	}
	if blocks == 0 {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	groups := reducer.groups()
	diag := columnPhysicalQueryDiagnosticsFromScan(view.Diagnostics)
	diag.WorkerCount = 1
	diag.ProjectedColumns = 1
	diag.ScheduledGranules = blocks
	diag.DecodedBlocks = blocks
	diag.DirectReduceBlocks = blocks
	diag.DictionaryCodeHits = blocks
	diag.RowsScanned = rows
	diag.PhysicalBytesScanned = assetBytes
	diag.ReduceRows = rows
	diag.ResultGroups = len(groups)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	diag.ScanNanos = time.Since(start).Nanoseconds()
	return ColumnPhysicalQueryResult{Groups: groups, Diagnostics: diag}, true, nil
}

func prepareColumnDictionaryCodeGroupCountDistinctRunner(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache) (*columnDictionaryCodeGroupCountDistinctRunner, error) {
	if req.Kind != ColumnPhysicalQueryGroupCountDistinct || req.GroupColumn == "" || req.DistinctColumn == "" || view.MutationParts != 0 {
		return nil, nil
	}
	if req.GroupColumn == req.DistinctColumn {
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
		groupCur, groupCardinality, groupRows, err := decodeColumnDictionaryCodesAssetHeader(groupRaw, groupSnapshot.AssetRef, view.Config, view.CollectionName, req.GroupColumn, false)
		if err != nil {
			return nil, err
		}
		groupLocal := make([]uint32, groupCardinality)
		for localCode := 0; localCode < groupCardinality; localCode++ {
			value := groupCur.stringBytes()
			valueKey := unsafeStringFromBytes(value)
			globalCode, ok := groupByValue[valueKey]
			if !ok {
				if uint64(len(runner.groupDict)) == uint64(^uint32(0)) {
					return nil, fmt.Errorf("collections: dictionary code distinct group cardinality exceeds uint32")
				}
				globalCode = uint32(len(runner.groupDict))
				key := columnDictionaryCodeOwnedString(value)
				groupByValue[key] = globalCode
				runner.groupDict = append(runner.groupDict, key)
			}
			groupLocal[localCode] = globalCode
			if groupCur.err != nil {
				break
			}
		}
		if groupCur.err != nil {
			return nil, groupCur.err
		}
		groupCodeCur := manifestCursor{raw: groupRaw, pos: groupCur.pos}
		groupCodes := make([]uint32, groupRows)
		for codeIdx := range groupCodes {
			groupCode := groupCodeCur.u32()
			groupIdx, ok := columnDictionaryCodeIndex(groupCode, len(groupLocal))
			if !ok {
				return nil, fmt.Errorf("collections: dictionary codes asset code[%d]=%d outside cardinality=%d", codeIdx, groupCode, len(groupLocal))
			}
			groupCodes[codeIdx] = groupLocal[groupIdx]
		}
		if groupCodeCur.err != nil {
			return nil, groupCodeCur.err
		}
		if groupCodeCur.pos != len(groupRaw) {
			return nil, errors.New("collections: trailing bytes in dictionary codes asset")
		}

		distinctRaw, err := readCache.read(distinctSnapshot.AssetRef, scratch)
		if err != nil {
			return nil, fmt.Errorf("collections: dictionary codes read generation=%d part_id=%d column=%q: %w", distinctSnapshot.AssetRef.Generation, distinctSnapshot.AssetRef.PartID, req.DistinctColumn, err)
		}
		scratch = distinctRaw
		distinctCur, distinctCardinality, distinctRows, err := decodeColumnDictionaryCodesAssetHeader(distinctRaw, distinctSnapshot.AssetRef, view.Config, view.CollectionName, req.DistinctColumn, false)
		if err != nil {
			return nil, err
		}
		if groupRows != distinctRows {
			return nil, fmt.Errorf("collections: dictionary code distinct row count mismatch group=%d distinct=%d", groupRows, distinctRows)
		}
		distinctLocal := make([]uint32, distinctCardinality)
		for localCode := 0; localCode < distinctCardinality; localCode++ {
			value := distinctCur.stringBytes()
			valueKey := unsafeStringFromBytes(value)
			globalCode, ok := distinctByValue[valueKey]
			if !ok {
				if uint64(len(distinctByValue)) == uint64(^uint32(0)) {
					return nil, fmt.Errorf("collections: dictionary code distinct cardinality exceeds uint32")
				}
				globalCode = uint32(len(distinctByValue))
				distinctByValue[columnDictionaryCodeOwnedString(value)] = globalCode
			}
			distinctLocal[localCode] = globalCode
			if distinctCur.err != nil {
				break
			}
		}
		if distinctCur.err != nil {
			return nil, distinctCur.err
		}
		_, _, ok, err = columnDictionaryCodeDistinctSeenWords(len(runner.groupDict), len(distinctByValue))
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		distinctCodeCur := manifestCursor{raw: distinctRaw, pos: distinctCur.pos}
		distinctCodes := make([]uint32, distinctRows)
		for codeIdx := range distinctCodes {
			distinctCode := distinctCodeCur.u32()
			distinctIdx, ok := columnDictionaryCodeIndex(distinctCode, len(distinctLocal))
			if !ok {
				return nil, fmt.Errorf("collections: dictionary codes asset code[%d]=%d outside cardinality=%d", codeIdx, distinctCode, len(distinctLocal))
			}
			distinctCodes[codeIdx] = distinctLocal[distinctIdx]
		}
		if distinctCodeCur.err != nil {
			return nil, distinctCodeCur.err
		}
		if distinctCodeCur.pos != len(distinctRaw) {
			return nil, errors.New("collections: trailing bytes in dictionary codes asset")
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
	wordsPerGroup, totalWords, ok, err := columnDictionaryCodeDistinctSeenWords(len(runner.groupDict), len(distinctByValue))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	runner.wordsPerGroup = wordsPerGroup
	runner.groupCounts = make([]int, len(runner.groupDict))
	runner.seen = make([]uint64, totalWords)
	runner.resultGroups = make([]ColumnPhysicalQueryGroup, 0, len(runner.groupDict))
	return runner, nil
}

func columnDictionaryCodeDistinctSeenWords(groupCount, distinctCount int) (int, int, bool, error) {
	if groupCount <= 0 || distinctCount <= 0 {
		return 0, 0, false, nil
	}
	wordsPerGroup := (distinctCount + 63) / 64
	if wordsPerGroup != 0 && groupCount > maxCollectionInt/wordsPerGroup {
		return 0, 0, false, nil
	}
	totalWords := groupCount * wordsPerGroup
	if totalWords > columnDictionaryCodeDistinctMaxSeenWords {
		return wordsPerGroup, totalWords, false, nil
	}
	return wordsPerGroup, totalWords, true, nil
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

func columnDictionaryCodeSnapshotsCoverParts(view columnPhysicalScanSnapshotView, byPart map[[2]uint64]columnManifestDictionaryCodesSnapshot) bool {
	for _, part := range view.AssetRefs {
		if part.Reason != ColumnPublishOperationInsert {
			return false
		}
		if _, ok := byPart[[2]uint64{part.Ref.Generation, part.Ref.PartID}]; !ok {
			return false
		}
	}
	return len(view.AssetRefs) > 0
}

func columnDictionaryCodeSnapshotBytes(view columnPhysicalScanSnapshotView, byPart map[[2]uint64]columnManifestDictionaryCodesSnapshot) int64 {
	var bytes int64
	for _, part := range view.AssetRefs {
		snapshot, ok := byPart[[2]uint64{part.Ref.Generation, part.Ref.PartID}]
		if !ok {
			return 0
		}
		if snapshot.AssetRef.Length > int64(maxCollectionInt)-bytes {
			return int64(maxCollectionInt)
		}
		bytes += snapshot.AssetRef.Length
	}
	return bytes
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

func runColumnDictionaryCodeGroupCountDistinctOneShot(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache) (ColumnPhysicalQueryResult, bool, error) {
	if req.Kind != ColumnPhysicalQueryGroupCountDistinct || req.GroupColumn == "" || req.DistinctColumn == "" || view.MutationParts != 0 {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	if req.GroupColumn == req.DistinctColumn {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	if readCache == nil {
		return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: dictionary code distinct query missing read cache")
	}
	groupCol, _, ok := columnPhysicalQueryDeclaredColumn(view.Config, req.GroupColumn)
	if !ok || groupCol.ValueType != ColumnStoreValueString || !groupCol.Dictionary {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	distinctCol, _, ok := columnPhysicalQueryDeclaredColumn(view.Config, req.DistinctColumn)
	if !ok || distinctCol.ValueType != ColumnStoreValueString || !distinctCol.Dictionary {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	groupByPart := columnDictionaryCodeSnapshotsByPart(view, req.GroupColumn)
	distinctByPart := columnDictionaryCodeSnapshotsByPart(view, req.DistinctColumn)
	if len(groupByPart) == 0 || len(distinctByPart) == 0 || !columnDictionaryCodeSnapshotsCoverParts(view, groupByPart) || !columnDictionaryCodeSnapshotsCoverParts(view, distinctByPart) {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	if bytes := columnDictionaryCodeSnapshotBytes(view, groupByPart) + columnDictionaryCodeSnapshotBytes(view, distinctByPart); bytes > columnDictionaryCodeOneShotMaxBytes {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	start := time.Now()
	reducer := columnDictionaryCodeGroupCountDistinctOneShotReducer{
		groupDict:       make([]string, 0, 16),
		groupByValue:    make(map[string]uint32, 16),
		distinctByValue: make(map[string]uint32, 16),
		assets:          make([]columnDictionaryCodeGroupCountDistinctOneShotAsset, 0, len(view.AssetRefs)),
		result:          make([]ColumnPhysicalQueryGroup, 0, 16),
	}
	var scratch []byte
	for _, part := range view.AssetRefs {
		partKey := [2]uint64{part.Ref.Generation, part.Ref.PartID}
		groupSnapshot := groupByPart[partKey]
		distinctSnapshot := distinctByPart[partKey]
		groupRaw, err := readCache.read(groupSnapshot.AssetRef, scratch)
		if err != nil {
			return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: dictionary codes read generation=%d part_id=%d column=%q: %w", groupSnapshot.AssetRef.Generation, groupSnapshot.AssetRef.PartID, req.GroupColumn, err)
		}
		scratch = groupRaw
		if !readCache.lastView {
			return ColumnPhysicalQueryResult{}, false, nil
		}
		ok := true
		groupCur, groupCardinality, groupRows, err := decodeColumnDictionaryCodesAssetHeader(groupRaw, groupSnapshot.AssetRef, view.Config, view.CollectionName, req.GroupColumn, false)
		if err != nil {
			return ColumnPhysicalQueryResult{}, true, err
		}
		reducer.prepareGroupDictionary(groupCardinality)
		for localCode := 0; localCode < groupCardinality; localCode++ {
			if !reducer.translateGroupDictionaryValue(localCode, groupCur.stringBytes()) {
				ok = false
				break
			}
			if groupCur.err != nil {
				break
			}
		}
		if groupCur.err != nil {
			return ColumnPhysicalQueryResult{}, true, groupCur.err
		}
		if !ok {
			return ColumnPhysicalQueryResult{}, false, nil
		}
		groupPayload := columnDictionaryCodesAssetPayload{rowCount: groupRows, offset: groupCur.pos}
		distinctRaw, err := readCache.read(distinctSnapshot.AssetRef, scratch)
		if err != nil {
			return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: dictionary codes read generation=%d part_id=%d column=%q: %w", distinctSnapshot.AssetRef.Generation, distinctSnapshot.AssetRef.PartID, req.DistinctColumn, err)
		}
		scratch = distinctRaw
		if !readCache.lastView {
			return ColumnPhysicalQueryResult{}, false, nil
		}
		distinctCur, distinctCardinality, distinctRows, err := decodeColumnDictionaryCodesAssetHeader(distinctRaw, distinctSnapshot.AssetRef, view.Config, view.CollectionName, req.DistinctColumn, false)
		if err != nil {
			return ColumnPhysicalQueryResult{}, true, err
		}
		reducer.prepareDistinctDictionary(distinctCardinality)
		for localCode := 0; localCode < distinctCardinality; localCode++ {
			if !reducer.translateDistinctDictionaryValue(localCode, distinctCur.stringBytes()) {
				ok = false
				break
			}
			if distinctCur.err != nil {
				break
			}
		}
		if distinctCur.err != nil {
			return ColumnPhysicalQueryResult{}, true, distinctCur.err
		}
		distinctPayload := columnDictionaryCodesAssetPayload{rowCount: distinctRows, offset: distinctCur.pos}
		if groupPayload.rowCount != distinctPayload.rowCount {
			return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: dictionary code distinct row count mismatch group=%d distinct=%d", groupPayload.rowCount, distinctPayload.rowCount)
		}
		if !ok {
			return ColumnPhysicalQueryResult{}, false, nil
		}
		if len(reducer.distinctByValue) > columnDictionaryCodeDistinctOneShotMaxValues {
			return ColumnPhysicalQueryResult{}, false, nil
		}
		_, _, ok, err = columnDictionaryCodeDistinctSeenWords(len(reducer.groupDict), len(reducer.distinctByValue))
		if err != nil {
			return ColumnPhysicalQueryResult{}, true, err
		}
		if !ok {
			return ColumnPhysicalQueryResult{}, false, nil
		}
		// groupRaw and distinctRaw are retained for the deferred second pass, so
		// the path above requires mmap/view-backed reads and falls back before
		// appending when reads are scratch-backed.
		reducer.assets = append(reducer.assets, columnDictionaryCodeGroupCountDistinctOneShotAsset{
			groupRaw:        groupRaw,
			groupRef:        groupSnapshot.AssetRef,
			groupPayload:    groupPayload,
			distinctRaw:     distinctRaw,
			distinctRef:     distinctSnapshot.AssetRef,
			distinctPayload: distinctPayload,
			bytes:           groupSnapshot.AssetRef.Length + distinctSnapshot.AssetRef.Length,
		})
		reducer.assetBytes += groupSnapshot.AssetRef.Length + distinctSnapshot.AssetRef.Length
	}
	if len(reducer.assets) == 0 || len(reducer.groupDict) == 0 || len(reducer.distinctByValue) == 0 {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	wordsPerGroup, totalWords, ok, err := columnDictionaryCodeDistinctSeenWords(len(reducer.groupDict), len(reducer.distinctByValue))
	if err != nil {
		return ColumnPhysicalQueryResult{}, true, err
	}
	if !ok {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	reducer.wordsPerGroup = wordsPerGroup
	reducer.groupCounts = make([]int, len(reducer.groupDict))
	reducer.seen = make([]uint64, totalWords)
	rows, err := reducer.reduceAssets(view, req)
	if err != nil {
		return ColumnPhysicalQueryResult{}, true, err
	}
	groups := reducer.groups()
	diag := columnPhysicalQueryDiagnosticsFromScan(view.Diagnostics)
	diag.WorkerCount = 1
	diag.ProjectedColumns = 2
	diag.ScheduledGranules = len(reducer.assets)
	diag.DecodedBlocks = len(reducer.assets)
	diag.DirectReduceBlocks = len(reducer.assets)
	diag.DictionaryCodeHits = len(reducer.assets)
	diag.RowsScanned = rows
	diag.PhysicalBytesScanned = reducer.assetBytes
	diag.ReduceRows = rows
	diag.ResultGroups = len(groups)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	diag.ScanNanos = time.Since(start).Nanoseconds()
	return ColumnPhysicalQueryResult{Groups: groups, Diagnostics: diag}, true, nil
}

func (r *columnDictionaryCodeGroupCountOneShotReducer) translateDictionary(dictionary []string) ([]uint32, bool) {
	if cap(r.localToGlobal) < len(dictionary) {
		r.localToGlobal = make([]uint32, len(dictionary))
	}
	localToGlobal := r.localToGlobal[:len(dictionary)]
	for localCode, value := range dictionary {
		globalCode, ok := r.byValue[value]
		if !ok {
			if uint64(len(r.dictionary)) == uint64(^uint32(0)) {
				return nil, false
			}
			globalCode = uint32(len(r.dictionary))
			r.byValue[value] = globalCode
			r.dictionary = append(r.dictionary, value)
			r.counts = append(r.counts, 0)
		}
		localToGlobal[localCode] = globalCode
	}
	return localToGlobal, true
}

func (r *columnDictionaryCodeGroupCountOneShotReducer) prepareDictionary(cardinality int) []uint32 {
	if cap(r.localToGlobal) < cardinality {
		r.localToGlobal = make([]uint32, cardinality)
	}
	return r.localToGlobal[:cardinality]
}

func (r *columnDictionaryCodeGroupCountOneShotReducer) translateDictionaryValue(localToGlobal []uint32, localCode int, value []byte) bool {
	globalCode, ok := r.byValue[unsafeStringFromBytes(value)]
	if !ok {
		if uint64(len(r.dictionary)) == uint64(^uint32(0)) {
			return false
		}
		globalCode = uint32(len(r.dictionary))
		key := columnDictionaryCodeOwnedString(value)
		r.byValue[key] = globalCode
		r.dictionary = append(r.dictionary, key)
		r.counts = append(r.counts, 0)
	}
	localToGlobal[localCode] = globalCode
	return true
}

func (r *columnDictionaryCodeGroupCountOneShotReducer) groups() []ColumnPhysicalQueryGroup {
	r.result = r.result[:0]
	for code, count := range r.counts {
		if count == 0 {
			continue
		}
		r.result = append(r.result, ColumnPhysicalQueryGroup{
			Key:   r.dictionary[code],
			Count: count,
		})
	}
	sortColumnPhysicalQueryGroupsByKey(r.result)
	return r.result
}

func (r *columnDictionaryCodeGroupCountDistinctOneShotReducer) prepareGroupDictionary(cardinality int) []uint32 {
	if cap(r.groupLocal) < cardinality {
		r.groupLocal = make([]uint32, cardinality)
	}
	return r.groupLocal[:cardinality]
}

func (r *columnDictionaryCodeGroupCountDistinctOneShotReducer) translateGroupDictionaryValue(localCode int, value []byte) bool {
	globalCode, ok := r.groupByValue[unsafeStringFromBytes(value)]
	if !ok {
		if uint64(len(r.groupDict)) == uint64(^uint32(0)) {
			return false
		}
		globalCode = uint32(len(r.groupDict))
		key := columnDictionaryCodeOwnedString(value)
		r.groupByValue[key] = globalCode
		r.groupDict = append(r.groupDict, key)
	}
	r.groupLocal[localCode] = globalCode
	return true
}

func (r *columnDictionaryCodeGroupCountDistinctOneShotReducer) prepareDistinctDictionary(cardinality int) []uint32 {
	if cap(r.distinctLocal) < cardinality {
		r.distinctLocal = make([]uint32, cardinality)
	}
	return r.distinctLocal[:cardinality]
}

func (r *columnDictionaryCodeGroupCountDistinctOneShotReducer) translateDistinctDictionaryValue(localCode int, value []byte) bool {
	globalCode, ok := r.distinctByValue[unsafeStringFromBytes(value)]
	if !ok {
		if uint64(len(r.distinctByValue)) == uint64(^uint32(0)) {
			return false
		}
		globalCode = uint32(len(r.distinctByValue))
		r.distinctByValue[columnDictionaryCodeOwnedString(value)] = globalCode
	}
	r.distinctLocal[localCode] = globalCode
	return true
}

func (r *columnDictionaryCodeGroupCountDistinctOneShotReducer) reduceAssets(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (int, error) {
	rows := 0
	for _, asset := range r.assets {
		groupDictCur, groupCardinality, _, err := decodeColumnDictionaryCodesAssetHeader(asset.groupRaw, asset.groupRef, view.Config, view.CollectionName, req.GroupColumn, false)
		if err != nil {
			return 0, err
		}
		groupMap := r.prepareGroupDictionary(groupCardinality)
		for localCode := 0; localCode < groupCardinality; localCode++ {
			groupMap[localCode] = r.groupByValue[unsafeStringFromBytes(groupDictCur.stringBytes())]
			if groupDictCur.err != nil {
				break
			}
		}
		if groupDictCur.err != nil {
			return 0, groupDictCur.err
		}
		distinctDictCur, distinctCardinality, _, err := decodeColumnDictionaryCodesAssetHeader(asset.distinctRaw, asset.distinctRef, view.Config, view.CollectionName, req.DistinctColumn, false)
		if err != nil {
			return 0, err
		}
		distinctMap := r.prepareDistinctDictionary(distinctCardinality)
		for localCode := 0; localCode < distinctCardinality; localCode++ {
			distinctMap[localCode] = r.distinctByValue[unsafeStringFromBytes(distinctDictCur.stringBytes())]
			if distinctDictCur.err != nil {
				break
			}
		}
		if distinctDictCur.err != nil {
			return 0, distinctDictCur.err
		}
		groupCur := manifestCursor{raw: asset.groupRaw, pos: asset.groupPayload.offset}
		distinctCur := manifestCursor{raw: asset.distinctRaw, pos: asset.distinctPayload.offset}
		for i := 0; i < asset.groupPayload.rowCount; i++ {
			groupLocal := groupCur.u32()
			if int(groupLocal) >= len(groupMap) {
				return 0, fmt.Errorf("collections: dictionary codes asset code[%d]=%d outside cardinality=%d", i, groupLocal, len(groupMap))
			}
			distinctLocal := distinctCur.u32()
			if int(distinctLocal) >= len(distinctMap) {
				return 0, fmt.Errorf("collections: dictionary codes asset code[%d]=%d outside cardinality=%d", i, distinctLocal, len(distinctMap))
			}
			groupCode := groupMap[groupLocal]
			distinctCode := distinctMap[distinctLocal]
			seenIdx := int(groupCode)*r.wordsPerGroup + int(distinctCode/64)
			mask := uint64(1) << (distinctCode & 63)
			if r.seen[seenIdx]&mask == 0 {
				r.seen[seenIdx] |= mask
				r.groupCounts[groupCode]++
			}
			rows++
		}
		if groupCur.err != nil {
			return 0, groupCur.err
		}
		if distinctCur.err != nil {
			return 0, distinctCur.err
		}
		if groupCur.pos != len(asset.groupRaw) {
			return 0, errors.New("collections: trailing bytes in dictionary codes asset")
		}
		if distinctCur.pos != len(asset.distinctRaw) {
			return 0, errors.New("collections: trailing bytes in dictionary codes asset")
		}
	}
	return rows, nil
}

func (r *columnDictionaryCodeGroupCountDistinctOneShotReducer) groups() []ColumnPhysicalQueryGroup {
	r.result = r.result[:0]
	for code, count := range r.groupCounts {
		if count == 0 {
			continue
		}
		r.result = append(r.result, ColumnPhysicalQueryGroup{
			Key:   r.groupDict[code],
			Count: count,
		})
	}
	sortColumnPhysicalQueryGroupsByKey(r.result)
	return r.result
}
