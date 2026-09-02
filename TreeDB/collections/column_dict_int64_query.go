package collections

import (
	"fmt"
	"time"
)

const columnDictionaryInt64GroupMaxGroups = 1 << 20

type columnDictionaryInt64GroupRunner struct {
	groupColumn          string
	valueColumn          string
	kind                 ColumnPhysicalQueryKind
	groupDict            []string
	assets               []columnDictionaryInt64GroupAsset
	predicateAssets      []columnDictionaryPredicateAsset
	seen                 []uint64
	minValues            []int64
	maxValues            []int64
	result               []ColumnPhysicalQueryGroup
	assetBytes           int64
	predicateBytes       int64
	predicateCodeHit     int
	predicateDiagnostics columnPhysicalQueryPredicateDiagnosticPlan
}

type columnDictionaryInt64GroupAsset struct {
	groupCodes []uint32
	values     []int64
}

func prepareColumnDictionaryInt64GroupRunner(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache) (*columnDictionaryInt64GroupRunner, error) {
	if req.AggregateMetadataName != "" || !columnDictionaryInt64GroupQueryKind(req.Kind) || req.GroupColumn == "" || req.ValueColumn == "" || view.MutationParts != 0 {
		return nil, nil
	}
	if readCache == nil {
		return nil, fmt.Errorf("collections: dictionary/int64 query missing read cache")
	}
	if !columnDictionaryInt64GroupColumnsEligible(view, req) {
		return nil, nil
	}
	groupByPart := columnDictionaryCodeSnapshotsByPart(view, req.GroupColumn)
	valueByPart := columnInt64ValueSnapshotsByPart(view, req.ValueColumn)
	if len(groupByPart) == 0 || len(valueByPart) == 0 || !columnDictionaryInt64GroupSnapshotsCoverParts(view, groupByPart, valueByPart) {
		return nil, nil
	}
	runner := &columnDictionaryInt64GroupRunner{
		groupColumn: req.GroupColumn,
		valueColumn: req.ValueColumn,
		kind:        req.Kind,
		assets:      make([]columnDictionaryInt64GroupAsset, 0, len(view.AssetRefs)),
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
			return nil, fmt.Errorf("collections: dictionary/int64 group codes read generation=%d part_id=%d column=%q: %w", groupSnapshot.AssetRef.Generation, groupSnapshot.AssetRef.PartID, req.GroupColumn, err)
		}
		scratch = groupRaw
		groupAsset, err := decodeColumnDictionaryCodesAsset(groupRaw, groupSnapshot.AssetRef, view.Config, view.CollectionName, req.GroupColumn, false)
		if err != nil {
			return nil, err
		}
		valueRaw, err := readCache.read(valueSnapshot.AssetRef, scratch)
		if err != nil {
			return nil, fmt.Errorf("collections: dictionary/int64 values read generation=%d part_id=%d column=%q: %w", valueSnapshot.AssetRef.Generation, valueSnapshot.AssetRef.PartID, req.ValueColumn, err)
		}
		valueRawIsView := readCache.lastView
		scratch = valueRaw
		_, valuePayload, err := decodeColumnInt64ValuesAssetPayload(valueRaw, valueSnapshot.AssetRef, view.Config, view.CollectionName, req.ValueColumn, false)
		if err != nil {
			return nil, err
		}
		if len(groupAsset.Codes) != valuePayload.rowCount || valuePayload.rowCount != valueSnapshot.Rows {
			return nil, fmt.Errorf("collections: dictionary/int64 row count mismatch group=%d values=%d manifest=%d", len(groupAsset.Codes), valuePayload.rowCount, valueSnapshot.Rows)
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
				if len(runner.groupDict) >= columnDictionaryInt64GroupMaxGroups || uint64(len(runner.groupDict)) == uint64(^uint32(0)) {
					return nil, nil
				}
				globalCode = uint32(len(runner.groupDict))
				groupByValue[value] = globalCode
				runner.groupDict = append(runner.groupDict, value)
			}
			groupCodes[codeIdx] = globalCode
		}
		runner.assets = append(runner.assets, columnDictionaryInt64GroupAsset{
			groupCodes: groupCodes,
			values:     values,
		})
		runner.assetBytes += groupSnapshot.AssetRef.Length + valueSnapshot.AssetRef.Length
	}
	if len(runner.assets) == 0 || len(runner.groupDict) == 0 {
		return nil, nil
	}
	runner.initScratch()
	return runner, nil
}

func runColumnDictionaryInt64GroupOneShot(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache) (ColumnPhysicalQueryResult, bool, error) {
	if !columnDictionaryInt64GroupQueryKind(req.Kind) || req.GroupColumn == "" || req.ValueColumn == "" || view.MutationParts != 0 {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	if readCache == nil {
		return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: dictionary/int64 query missing read cache")
	}
	if !columnDictionaryInt64GroupColumnsEligible(view, req) {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	groupByPart := columnDictionaryCodeSnapshotsByPart(view, req.GroupColumn)
	valueByPart := columnInt64ValueSnapshotsByPart(view, req.ValueColumn)
	if len(groupByPart) == 0 || len(valueByPart) == 0 || !columnDictionaryInt64GroupSnapshotsCoverParts(view, groupByPart, valueByPart) {
		return ColumnPhysicalQueryResult{}, false, nil
	}

	start := time.Now()
	reducer := columnDictionaryInt64GroupOneShotReducer{
		kind:         req.Kind,
		groupByValue: make(map[string]uint32),
	}
	var scratch []byte
	assetBytes := int64(0)
	blocks := 0
	rows := 0
	for _, part := range view.AssetRefs {
		partKey := [2]uint64{part.Ref.Generation, part.Ref.PartID}
		groupSnapshot := groupByPart[partKey]
		valueSnapshot := valueByPart[partKey]
		groupRaw, err := readCache.read(groupSnapshot.AssetRef, scratch)
		if err != nil {
			return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: dictionary/int64 group codes read generation=%d part_id=%d column=%q: %w", groupSnapshot.AssetRef.Generation, groupSnapshot.AssetRef.PartID, req.GroupColumn, err)
		}
		scratch = groupRaw
		groupIsView := readCache.lastView
		var groupPayload columnDictionaryCodesAssetPayload
		var groupCodes []uint32
		var localToGlobal []uint32
		var ok bool
		if !groupIsView {
			decoded, err := decodeColumnDictionaryCodesAsset(groupRaw, groupSnapshot.AssetRef, view.Config, view.CollectionName, req.GroupColumn, false)
			if err != nil {
				return ColumnPhysicalQueryResult{}, true, err
			}
			groupPayload = columnDictionaryCodesAssetPayload{rowCount: len(decoded.Codes)}
			groupCodes = decoded.Codes
			localToGlobal, ok = reducer.translateDictionary(decoded.Dictionary)
		} else {
			ok = true
			dictCur, cardinality, rowCount, err := decodeColumnDictionaryCodesAssetHeader(groupRaw, groupSnapshot.AssetRef, view.Config, view.CollectionName, req.GroupColumn, false)
			if err != nil {
				return ColumnPhysicalQueryResult{}, true, err
			}
			localToGlobal = reducer.prepareDictionary(cardinality)
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
			groupPayload, err = columnDictionaryCodesPayloadAfterDictionary(groupRaw, groupSnapshot.AssetRef, &dictCur, rowCount)
			if err != nil {
				return ColumnPhysicalQueryResult{}, true, err
			}
		}
		if !ok {
			return ColumnPhysicalQueryResult{}, false, nil
		}
		valueRaw, err := readCache.read(valueSnapshot.AssetRef, scratch)
		if err != nil {
			return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: dictionary/int64 values read generation=%d part_id=%d column=%q: %w", valueSnapshot.AssetRef.Generation, valueSnapshot.AssetRef.PartID, req.ValueColumn, err)
		}
		scratch = valueRaw
		_, valuePayload, err := decodeColumnInt64ValuesAssetPayload(valueRaw, valueSnapshot.AssetRef, view.Config, view.CollectionName, req.ValueColumn, false)
		if err != nil {
			return ColumnPhysicalQueryResult{}, true, err
		}
		if groupPayload.rowCount != valuePayload.rowCount || valuePayload.rowCount != valueSnapshot.Rows {
			return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: dictionary/int64 row count mismatch group=%d values=%d manifest=%d", groupPayload.rowCount, valuePayload.rowCount, valueSnapshot.Rows)
		}
		values, _, err := viewColumnInt64ValuesPayload(valueRaw, valuePayload)
		if err != nil {
			return ColumnPhysicalQueryResult{}, true, err
		}
		if groupCodes != nil {
			for i, localCode := range groupCodes {
				reducer.reduceValue(localToGlobal[localCode], values[i])
				rows++
			}
		} else {
			localCodes, _, err := viewColumnDictionaryCodesPayload(groupRaw, groupPayload)
			if err != nil {
				return ColumnPhysicalQueryResult{}, true, err
			}
			for i, localCode := range localCodes {
				localIdx, ok := columnDictionaryCodeIndex(localCode, len(localToGlobal))
				if !ok {
					return ColumnPhysicalQueryResult{}, true, fmt.Errorf("collections: dictionary codes asset code[%d]=%d outside cardinality=%d", i, localCode, len(localToGlobal))
				}
				reducer.reduceValue(localToGlobal[localIdx], values[i])
				rows++
			}
		}
		assetBytes += groupSnapshot.AssetRef.Length + valueSnapshot.AssetRef.Length
		blocks++
	}
	if blocks == 0 {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	groups := reducer.groups(req)
	diag := columnPhysicalQueryDiagnosticsFromScan(view.Diagnostics)
	diag.WorkerCount = 1
	diag.ProjectedColumns = 2
	diag.ScheduledGranules = blocks
	diag.DecodedBlocks = blocks
	diag.DirectReduceBlocks = blocks
	diag.DictionaryCodeHits = blocks
	diag.Int64ValueHits = blocks
	diag.RowsScanned = rows
	diag.PhysicalBytesScanned = assetBytes
	diag.ReduceRows = rows
	diag.ResultGroups = len(groups)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	diag.ScanNanos = time.Since(start).Nanoseconds()
	return ColumnPhysicalQueryResult{Groups: groups, Diagnostics: diag}, true, nil
}

func columnDictionaryInt64GroupQueryKind(kind ColumnPhysicalQueryKind) bool {
	return kind == ColumnPhysicalQueryGroupMinInt64 ||
		kind == ColumnPhysicalQueryGroupMaxInt64 ||
		kind == ColumnPhysicalQueryGroupInt64Span
}

func columnDictionaryInt64GroupColumnsEligible(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) bool {
	groupCol, _, ok := columnPhysicalQueryDeclaredColumn(view.Config, req.GroupColumn)
	if !ok || groupCol.ValueType != ColumnStoreValueString || !groupCol.Dictionary {
		return false
	}
	valueCol, _, ok := columnPhysicalQueryDeclaredColumn(view.Config, req.ValueColumn)
	if !ok || valueCol.ValueType != ColumnStoreValueInt64 || valueCol.Nullable {
		return false
	}
	return true
}

func columnDictionaryInt64GroupSnapshotsCoverParts(view columnPhysicalScanSnapshotView, groupByPart map[[2]uint64]columnManifestDictionaryCodesSnapshot, valueByPart map[[2]uint64]columnManifestInt64ValuesSnapshot) bool {
	for _, part := range view.AssetRefs {
		if part.Reason != ColumnPublishOperationInsert {
			return false
		}
		partKey := [2]uint64{part.Ref.Generation, part.Ref.PartID}
		if _, ok := groupByPart[partKey]; !ok {
			return false
		}
		if _, ok := valueByPart[partKey]; !ok {
			return false
		}
	}
	return len(view.AssetRefs) > 0
}

func (r *columnDictionaryInt64GroupRunner) initScratch() {
	groups := len(r.groupDict)
	words := (groups + 63) / 64
	r.seen = make([]uint64, words)
	r.minValues = make([]int64, groups)
	if r.kind == ColumnPhysicalQueryGroupInt64Span {
		r.maxValues = make([]int64, groups)
	}
	r.result = make([]ColumnPhysicalQueryGroup, 0, groups)
}

func (r *columnDictionaryInt64GroupRunner) run(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) ColumnPhysicalQueryResult {
	start := time.Now()
	clear(r.seen)
	rows := 0
	matched := 0
	if len(r.predicateAssets) == 0 {
		for _, asset := range r.assets {
			rows += len(asset.groupCodes)
			for rowIdx, groupCode := range asset.groupCodes {
				value := asset.values[rowIdx]
				r.reduceValue(groupCode, value)
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
					value := asset.values[rowIdx]
					r.reduceValue(groupCode, value)
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
					value := asset.values[rowIdx]
					r.reduceValue(groupCode, value)
					matched++
				}
			case 2:
				for rowIdx, groupCode := range asset.groupCodes {
					if !fast.matches2(rowIdx) {
						continue
					}
					value := asset.values[rowIdx]
					r.reduceValue(groupCode, value)
					matched++
				}
			default:
				for rowIdx, groupCode := range asset.groupCodes {
					if !fast.matches(rowIdx) {
						continue
					}
					value := asset.values[rowIdx]
					r.reduceValue(groupCode, value)
					matched++
				}
			}
		}
	}
	r.result = r.result[:0]
	for code, key := range r.groupDict {
		if !r.seenCode(uint32(code)) {
			continue
		}
		value := r.minValues[code]
		if r.kind == ColumnPhysicalQueryGroupInt64Span {
			value = r.maxValues[code] - r.minValues[code]
		}
		r.result = append(r.result, ColumnPhysicalQueryGroup{Key: key, Int64: value})
	}
	if req.TopK == 0 {
		sortColumnPhysicalQueryGroupsByKey(r.result)
	}
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

func (r *columnDictionaryInt64GroupRunner) reduceValue(code uint32, value int64) {
	idx := int(code)
	word := idx / 64
	mask := uint64(1) << uint(idx&63)
	if r.seen[word]&mask == 0 {
		r.seen[word] |= mask
		r.minValues[idx] = value
		if r.kind == ColumnPhysicalQueryGroupInt64Span {
			r.maxValues[idx] = value
		}
		return
	}
	switch r.kind {
	case ColumnPhysicalQueryGroupMinInt64:
		if value < r.minValues[idx] {
			r.minValues[idx] = value
		}
	case ColumnPhysicalQueryGroupMaxInt64:
		if value > r.minValues[idx] {
			r.minValues[idx] = value
		}
	case ColumnPhysicalQueryGroupInt64Span:
		if value < r.minValues[idx] {
			r.minValues[idx] = value
		}
		if value > r.maxValues[idx] {
			r.maxValues[idx] = value
		}
	}
}

func (r *columnDictionaryInt64GroupRunner) seenCode(code uint32) bool {
	idx := int(code)
	return r.seen[idx/64]&(uint64(1)<<uint(idx&63)) != 0
}

type columnDictionaryInt64GroupOneShotReducer struct {
	kind          ColumnPhysicalQueryKind
	groupByValue  map[string]uint32
	groupDict     []string
	localToGlobal []uint32
	seen          []uint64
	minValues     []int64
	maxValues     []int64
	result        []ColumnPhysicalQueryGroup
}

func (r *columnDictionaryInt64GroupOneShotReducer) translateDictionary(dictionary []string) ([]uint32, bool) {
	if cap(r.localToGlobal) < len(dictionary) {
		r.localToGlobal = make([]uint32, len(dictionary))
	}
	localToGlobal := r.localToGlobal[:len(dictionary)]
	if len(r.groupDict) == 0 {
		r.growScratch(len(dictionary))
	}
	for localCode, value := range dictionary {
		globalCode, ok := r.groupByValue[value]
		if !ok {
			if len(r.groupDict) >= columnDictionaryInt64GroupMaxGroups || uint64(len(r.groupDict)) == uint64(^uint32(0)) {
				return nil, false
			}
			globalCode = uint32(len(r.groupDict))
			r.groupByValue[value] = globalCode
			r.groupDict = append(r.groupDict, value)
			r.growScratch(len(r.groupDict))
		}
		localToGlobal[localCode] = globalCode
	}
	return localToGlobal, true
}

func (r *columnDictionaryInt64GroupOneShotReducer) prepareDictionary(cardinality int) []uint32 {
	if cap(r.localToGlobal) < cardinality {
		r.localToGlobal = make([]uint32, cardinality)
	}
	localToGlobal := r.localToGlobal[:cardinality]
	if len(r.groupDict) == 0 {
		r.growScratch(cardinality)
	}
	return localToGlobal
}

func (r *columnDictionaryInt64GroupOneShotReducer) translateDictionaryValue(localToGlobal []uint32, localCode int, value []byte) bool {
	globalCode, ok := r.groupByValue[unsafeStringFromBytes(value)]
	if !ok {
		if len(r.groupDict) >= columnDictionaryInt64GroupMaxGroups || uint64(len(r.groupDict)) == uint64(^uint32(0)) {
			return false
		}
		globalCode = uint32(len(r.groupDict))
		key := columnDictionaryCodeOwnedString(value)
		r.groupByValue[key] = globalCode
		r.groupDict = append(r.groupDict, key)
		r.growScratch(len(r.groupDict))
	}
	localToGlobal[localCode] = globalCode
	return true
}

func (r *columnDictionaryInt64GroupOneShotReducer) growScratch(groups int) {
	if groups > columnDictionaryInt64GroupMaxGroups {
		groups = columnDictionaryInt64GroupMaxGroups
	}
	words := (groups + 63) / 64
	if len(r.seen) < words {
		if cap(r.seen) < words {
			next := make([]uint64, words, columnDictionaryInt64GroupNextScratchCap(cap(r.seen), words))
			copy(next, r.seen)
			r.seen = next
		} else {
			r.seen = r.seen[:words]
		}
	}
	if len(r.minValues) < groups {
		if cap(r.minValues) < groups {
			next := make([]int64, groups, columnDictionaryInt64GroupNextScratchCap(cap(r.minValues), groups))
			copy(next, r.minValues)
			r.minValues = next
		} else {
			r.minValues = r.minValues[:groups]
		}
	}
	if r.kind == ColumnPhysicalQueryGroupInt64Span && len(r.maxValues) < groups {
		if cap(r.maxValues) < groups {
			next := make([]int64, groups, columnDictionaryInt64GroupNextScratchCap(cap(r.maxValues), groups))
			copy(next, r.maxValues)
			r.maxValues = next
		} else {
			r.maxValues = r.maxValues[:groups]
		}
	}
	if cap(r.result) < groups {
		r.result = make([]ColumnPhysicalQueryGroup, 0, columnDictionaryInt64GroupNextScratchCap(cap(r.result), groups))
	}
}

func columnDictionaryInt64GroupNextScratchCap(current, required int) int {
	next := current * 2
	if next < 4 {
		next = 4
	}
	if next < required {
		next = required
	}
	if next > columnDictionaryInt64GroupMaxGroups {
		next = columnDictionaryInt64GroupMaxGroups
	}
	return next
}

func (r *columnDictionaryInt64GroupOneShotReducer) reduceValue(code uint32, value int64) {
	idx := int(code)
	word := idx / 64
	mask := uint64(1) << uint(idx&63)
	if r.seen[word]&mask == 0 {
		r.seen[word] |= mask
		r.minValues[idx] = value
		if r.kind == ColumnPhysicalQueryGroupInt64Span {
			r.maxValues[idx] = value
		}
		return
	}
	switch r.kind {
	case ColumnPhysicalQueryGroupMinInt64:
		if value < r.minValues[idx] {
			r.minValues[idx] = value
		}
	case ColumnPhysicalQueryGroupMaxInt64:
		if value > r.minValues[idx] {
			r.minValues[idx] = value
		}
	case ColumnPhysicalQueryGroupInt64Span:
		if value < r.minValues[idx] {
			r.minValues[idx] = value
		}
		if value > r.maxValues[idx] {
			r.maxValues[idx] = value
		}
	}
}

func (r *columnDictionaryInt64GroupOneShotReducer) groups(req ColumnPhysicalQueryRequest) []ColumnPhysicalQueryGroup {
	r.result = r.result[:0]
	for code, key := range r.groupDict {
		word := code / 64
		mask := uint64(1) << uint(code&63)
		if r.seen[word]&mask == 0 {
			continue
		}
		value := r.minValues[code]
		if r.kind == ColumnPhysicalQueryGroupInt64Span {
			value = r.maxValues[code] - r.minValues[code]
		}
		r.result = append(r.result, ColumnPhysicalQueryGroup{Key: key, Int64: value})
	}
	if req.TopK == 0 {
		sortColumnPhysicalQueryGroupsByKey(r.result)
	}
	return r.result
}
