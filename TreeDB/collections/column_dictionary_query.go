package collections

import (
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

type columnDictionaryCodeGroupCountAsset struct {
	codes []uint32
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
