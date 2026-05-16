package colgranule

import (
	"fmt"
	"sort"
	"time"
)

type ColumnPartSetReader struct {
	manifest       ColumnCollectionManifest
	parts          []columnPartSetLoadedPart
	latest         map[int64]columnPartSetRowRef
	visibleRows    map[int]map[int]struct{}
	visibleRowList []columnPartSetVisibleRows
	tombstoneByID  map[int64]uint64
	cacheStats     ColumnWorkspaceCacheStats
	visibilityStat ColumnPartSetVisibilityStats
}

type columnPartSetLoadedPart struct {
	Ref     ColumnManifestPartRef
	Part    *ColumnPart
	Load    ColumnWorkspaceLoadResult
	Ordinal int
}

type columnPartSetRowRef struct {
	PrimaryID    int64
	PartIndex    int
	PartRow      int
	GenerationID uint64
	Ordinal      int
	Locator      RowLocator
}

type columnPartSetVisibleRows struct {
	Rows []int
	All  bool
}

type ColumnPartSetVisibilityStats struct {
	Parts          int `json:"parts"`
	BaseParts      int `json:"base_parts"`
	DeltaParts     int `json:"delta_parts"`
	InputRows      int `json:"input_rows"`
	VisibleRows    int `json:"visible_rows"`
	SupersededRows int `json:"superseded_rows"`
	DeletedRows    int `json:"deleted_rows"`
	Tombstones     int `json:"tombstones"`
}

type ColumnPartSetScanResult struct {
	Rows        int
	Columns     map[string][]int64
	Diagnostics ColumnPartSetScanDiagnostics
}

type ColumnPartSetScanDiagnostics struct {
	RowsReturned       int                       `json:"rows_returned"`
	RowsScanned        int                       `json:"rows_scanned"`
	RowsSuperseded     int                       `json:"rows_superseded"`
	RowsDeleted        int                       `json:"rows_deleted"`
	PartsConsidered    int                       `json:"parts_considered"`
	BaseParts          int                       `json:"base_parts"`
	DeltaParts         int                       `json:"delta_parts"`
	Tombstones         int                       `json:"tombstones"`
	ColumnsProjected   []string                  `json:"columns_projected"`
	GranulesConsidered int                       `json:"granules_considered"`
	BlocksDecoded      int                       `json:"blocks_decoded"`
	BytesDecoded       int                       `json:"bytes_decoded"`
	CacheStats         ColumnWorkspaceCacheStats `json:"cache_stats"`
}

type ColumnPartSetCompactionResult struct {
	Manifest         ColumnCollectionManifest    `json:"manifest"`
	Part             ColumnWorkspacePartManifest `json:"part"`
	InputRows        int                         `json:"input_rows"`
	VisibleRows      int                         `json:"visible_rows"`
	DroppedRows      int                         `json:"dropped_rows"`
	SupersededRows   int                         `json:"superseded_rows"`
	DeletedRows      int                         `json:"deleted_rows"`
	OldAssetBytes    int                         `json:"old_asset_bytes"`
	NewAssetBytes    int                         `json:"new_asset_bytes"`
	ReclaimableBytes int                         `json:"reclaimable_bytes"`
	NetBytesReduced  int                         `json:"net_bytes_reduced"`
	CompactionUnix   int64                       `json:"compaction_unix_nano"`
}

func OpenColumnPartSetReader(workspace *ColumnWorkspace, manifest ColumnCollectionManifest, opts ColumnPartImageReadOptions) (*ColumnPartSetReader, error) {
	if workspace == nil {
		return nil, fmt.Errorf("colgranule: nil column workspace")
	}
	if err := validateColumnCollectionManifest(manifest); err != nil {
		return nil, err
	}
	opts.IncludeRowLocators = true
	opts.ValidateRowLocators = true
	refs := make([]ColumnManifestPartRef, 0, len(manifest.PartSet.BaseParts)+len(manifest.PartSet.DeltaParts))
	refs = append(refs, manifest.PartSet.BaseParts...)
	refs = append(refs, manifest.PartSet.DeltaParts...)
	reader := &ColumnPartSetReader{
		manifest:      manifest,
		latest:        make(map[int64]columnPartSetRowRef),
		visibleRows:   make(map[int]map[int]struct{}),
		tombstoneByID: make(map[int64]uint64, len(manifest.PartSet.Tombstones)),
	}
	for _, ref := range refs {
		load, err := workspace.LoadPartWithOptions(ref.Part.PartID, opts)
		if err != nil {
			return nil, err
		}
		if load.Manifest.AssetRef != ref.Part.AssetRef || load.Manifest.TCS1 != ref.Part.TCS1 {
			return nil, fmt.Errorf("colgranule: loaded part %d does not match collection manifest ref", ref.Part.PartID)
		}
		reader.parts = append(reader.parts, columnPartSetLoadedPart{
			Ref:     ref,
			Part:    load.Part,
			Load:    load,
			Ordinal: len(reader.parts),
		})
		reader.cacheStats = load.CacheStats
	}
	if err := reader.buildVisibility(); err != nil {
		return nil, err
	}
	return reader, nil
}

func (r *ColumnPartSetReader) Manifest() ColumnCollectionManifest {
	if r == nil {
		return ColumnCollectionManifest{}
	}
	return r.manifest
}

func (r *ColumnPartSetReader) VisibilityStats() ColumnPartSetVisibilityStats {
	if r == nil {
		return ColumnPartSetVisibilityStats{}
	}
	return r.visibilityStat
}

func (r *ColumnPartSetReader) CacheStats() ColumnWorkspaceCacheStats {
	if r == nil {
		return ColumnWorkspaceCacheStats{}
	}
	return r.cacheStats
}

func (r *ColumnPartSetReader) LatestLocator(primaryID int64) (RowLocator, bool) {
	if r == nil {
		return RowLocator{}, false
	}
	ref, ok := r.latest[primaryID]
	if !ok {
		return RowLocator{}, false
	}
	return ref.Locator, true
}

func (r *ColumnPartSetReader) ScanProjected(columns []string) (ColumnPartSetScanResult, error) {
	return r.ScanProjectedInto(nil, columns)
}

func (r *ColumnPartSetReader) ScanProjectedInto(dst map[string][]int64, columns []string) (ColumnPartSetScanResult, error) {
	if r == nil {
		return ColumnPartSetScanResult{}, fmt.Errorf("colgranule: nil part set reader")
	}
	if len(columns) == 0 {
		return ColumnPartSetScanResult{}, fmt.Errorf("colgranule: empty part set projection")
	}
	if dst == nil {
		dst = make(map[string][]int64, len(columns))
	}
	for existing := range dst {
		if !containsString(columns, existing) {
			delete(dst, existing)
		}
	}
	for _, name := range columns {
		if cap(dst[name]) < r.visibilityStat.VisibleRows {
			dst[name] = make([]int64, 0, r.visibilityStat.VisibleRows)
		} else {
			dst[name] = dst[name][:0]
		}
	}
	diagnostics := ColumnPartSetScanDiagnostics{
		RowsSuperseded:   r.visibilityStat.SupersededRows,
		RowsDeleted:      r.visibilityStat.DeletedRows,
		PartsConsidered:  len(r.parts),
		BaseParts:        r.visibilityStat.BaseParts,
		DeltaParts:       r.visibilityStat.DeltaParts,
		Tombstones:       r.visibilityStat.Tombstones,
		ColumnsProjected: append([]string(nil), columns...),
		CacheStats:       r.cacheStats,
	}
	for partIndex, loaded := range r.parts {
		visible := r.visibleRowsForPart(partIndex)
		if len(visible.Rows) == 0 {
			continue
		}
		diagnostics.RowsScanned += loaded.Part.Descriptor.RowCount
		diagnostics.GranulesConsidered += len(loaded.Part.Descriptor.Granules)
		scanner := loaded.Part.NewScanner()
		for _, name := range columns {
			var columnDiagnostics PartScanDiagnostics
			var err error
			dst[name], columnDiagnostics, err = scanner.scanColumnRowsInto(name, dst[name], visible.Rows)
			if err != nil {
				return ColumnPartSetScanResult{}, err
			}
			diagnostics.BlocksDecoded += columnDiagnostics.BlocksDecoded
			diagnostics.BytesDecoded += columnDiagnostics.BytesDecoded
		}
		diagnostics.RowsReturned += len(visible.Rows)
	}
	return ColumnPartSetScanResult{
		Rows:        diagnostics.RowsReturned,
		Columns:     dst,
		Diagnostics: diagnostics,
	}, nil
}

func (r *ColumnPartSetReader) buildVisibility() error {
	for _, tombstone := range r.manifest.PartSet.Tombstones {
		if prev, ok := r.tombstoneByID[tombstone.PrimaryID]; !ok || tombstone.GenerationID > prev {
			r.tombstoneByID[tombstone.PrimaryID] = tombstone.GenerationID
		}
	}
	stats := ColumnPartSetVisibilityStats{
		Parts:      len(r.parts),
		BaseParts:  len(r.manifest.PartSet.BaseParts),
		DeltaParts: len(r.manifest.PartSet.DeltaParts),
		Tombstones: len(r.manifest.PartSet.Tombstones),
	}
	for partIndex, loaded := range r.parts {
		stats.InputRows += loaded.Part.Descriptor.RowCount
		for primaryID, locator := range loaded.Part.Locators {
			if locator.PartID != loaded.Part.Descriptor.PartID {
				return fmt.Errorf("colgranule: part set locator part=%d want %d", locator.PartID, loaded.Part.Descriptor.PartID)
			}
			if locator.PartRow < 0 || locator.PartRow >= loaded.Part.Descriptor.RowCount {
				return fmt.Errorf("colgranule: part set locator row=%d outside part %d rows=%d", locator.PartRow, loaded.Part.Descriptor.PartID, loaded.Part.Descriptor.RowCount)
			}
			row := columnPartSetRowRef{
				PrimaryID:    primaryID,
				PartIndex:    partIndex,
				PartRow:      locator.PartRow,
				GenerationID: loaded.Ref.GenerationID,
				Ordinal:      loaded.Ordinal,
				Locator:      locator,
			}
			if tombstoneGeneration, ok := r.tombstoneByID[primaryID]; ok && tombstoneGeneration >= row.GenerationID {
				stats.DeletedRows++
				continue
			}
			prev, ok := r.latest[primaryID]
			if !ok || row.newerThan(prev) {
				if ok {
					stats.SupersededRows++
				}
				r.latest[primaryID] = row
				continue
			}
			stats.SupersededRows++
		}
	}
	for _, row := range r.latest {
		if r.visibleRows[row.PartIndex] == nil {
			r.visibleRows[row.PartIndex] = make(map[int]struct{})
		}
		r.visibleRows[row.PartIndex][row.PartRow] = struct{}{}
	}
	r.visibleRowList = make([]columnPartSetVisibleRows, len(r.parts))
	for partIndex, rows := range r.visibleRows {
		list := make([]int, 0, len(rows))
		for row := range rows {
			list = append(list, row)
		}
		sort.Ints(list)
		r.visibleRowList[partIndex] = columnPartSetVisibleRows{
			Rows: list,
			All:  len(list) == r.parts[partIndex].Part.Descriptor.RowCount,
		}
	}
	stats.VisibleRows = len(r.latest)
	r.visibilityStat = stats
	return nil
}

func (r *ColumnPartSetReader) visibleRowsForPart(partIndex int) columnPartSetVisibleRows {
	if r == nil || partIndex < 0 || partIndex >= len(r.visibleRowList) {
		return columnPartSetVisibleRows{}
	}
	return r.visibleRowList[partIndex]
}

func (r columnPartSetRowRef) newerThan(other columnPartSetRowRef) bool {
	if r.GenerationID != other.GenerationID {
		return r.GenerationID > other.GenerationID
	}
	return r.Ordinal > other.Ordinal
}

func BuildColumnDeltaPart(partID uint64, opts ColumnStoreOptions, replacements ColumnBatch) (*ColumnPart, error) {
	return BuildColumnPart(partID, opts, replacements)
}

func CompactColumnPartSet(workspace *ColumnWorkspace, reader *ColumnPartSetReader, opts ColumnStoreOptions, dictionaries map[string]map[string]int64, newPartID uint64) (ColumnPartSetCompactionResult, error) {
	if workspace == nil {
		return ColumnPartSetCompactionResult{}, fmt.Errorf("colgranule: nil column workspace")
	}
	if reader == nil {
		return ColumnPartSetCompactionResult{}, fmt.Errorf("colgranule: nil part set reader")
	}
	normalized, err := normalizeColumnStoreOptions(opts)
	if err != nil {
		return ColumnPartSetCompactionResult{}, err
	}
	columnNames := make([]string, 0, len(normalized.Columns))
	for _, def := range normalized.Columns {
		columnNames = append(columnNames, def.Name)
	}
	scan, err := reader.ScanProjected(columnNames)
	if err != nil {
		return ColumnPartSetCompactionResult{}, err
	}
	if scan.Rows == 0 {
		return ColumnPartSetCompactionResult{}, fmt.Errorf("colgranule: cannot compact empty visible part set")
	}
	part, err := BuildColumnPart(newPartID, normalized, ColumnBatch{Rows: scan.Rows, Columns: scan.Columns})
	if err != nil {
		return ColumnPartSetCompactionResult{}, err
	}
	entry, err := workspace.PublishPart(part, dictionaries)
	if err != nil {
		return ColumnPartSetCompactionResult{}, err
	}
	oldAssetBytes := reader.manifest.ByteAccounting.TotalAssetBytes
	newGeneration := reader.manifest.ActiveGeneration + 1
	manifest, err := NewColumnCollectionManifest(
		reader.manifest.Collection,
		normalized,
		[]ColumnManifestPartRef{NewColumnManifestPartRef(ColumnPartRoleBase, newGeneration, entry)},
		nil,
		nil,
	)
	if err != nil {
		return ColumnPartSetCompactionResult{}, err
	}
	manifest.CreatedUnix = reader.manifest.CreatedUnix
	manifest.UpdatedUnix = time.Now().UnixNano()
	manifest.ActiveGeneration = columnManifestActiveGeneration(manifest.PartSet)
	manifest.ByteAccounting = columnManifestByteAccounting(manifest)
	if err := validateColumnCollectionManifest(manifest); err != nil {
		return ColumnPartSetCompactionResult{}, err
	}
	netReduced := oldAssetBytes - entry.AssetBytes
	if netReduced < 0 {
		netReduced = 0
	}
	return ColumnPartSetCompactionResult{
		Manifest:         manifest,
		Part:             entry,
		InputRows:        reader.visibilityStat.InputRows,
		VisibleRows:      scan.Rows,
		DroppedRows:      reader.visibilityStat.InputRows - scan.Rows,
		SupersededRows:   reader.visibilityStat.SupersededRows,
		DeletedRows:      reader.visibilityStat.DeletedRows,
		OldAssetBytes:    oldAssetBytes,
		NewAssetBytes:    entry.AssetBytes,
		ReclaimableBytes: oldAssetBytes,
		NetBytesReduced:  netReduced,
		CompactionUnix:   time.Now().UnixNano(),
	}, nil
}

func RunJSONBenchColumnPartSetQueries(reader *ColumnPartSetReader, ds JSONBenchDataset, attempts int) ([]JSONBenchPartQueryTiming, error) {
	if attempts <= 0 {
		attempts = 3
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		return nil, err
	}
	queries := []struct {
		name        string
		description string
		run         func(*ColumnPartSetReader, queryCodeSet, *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error)
	}{
		{"Q1", "Top event types", runJSONBenchPartSetQ1},
		{"Q2", "Top event types with unique users", runJSONBenchPartSetQ2},
		{"Q3", "Event counts by hour", runJSONBenchPartSetQ3},
		{"Q4", "Top 3 post veterans", runJSONBenchPartSetQ4},
		{"Q5", "Top 3 users with longest activity", runJSONBenchPartSetQ5},
	}
	out := make([]JSONBenchPartQueryTiming, 0, len(queries))
	for _, q := range queries {
		timing := JSONBenchPartQueryTiming{
			Query:       q.name,
			Description: q.description,
			Engine:      "encoded_column_part_set",
			Attempts:    make([]JSONBenchPartQueryAttempt, 0, attempts),
		}
		scratch := &jsonBenchPartQueryScratch{}
		for i := 0; i < attempts; i++ {
			cache := "cold"
			if i > 0 {
				cache = "warm"
			}
			start := time.Now()
			rows, digest, diagnostics, err := q.run(reader, codes, scratch)
			if err != nil {
				return nil, fmt.Errorf("%s encoded part set query: %w", q.name, err)
			}
			elapsed := time.Since(start)
			diagnostics.CacheState = cache
			attempt := JSONBenchPartQueryAttempt{
				Cache:        cache,
				Duration:     elapsed,
				ResultRows:   rows,
				ResultDigest: digest,
				Diagnostics:  diagnostics,
			}
			timing.Attempts = append(timing.Attempts, attempt)
			timing.ResultRows = rows
			timing.ResultDigest = digest
			if timing.Best == 0 || elapsed < timing.Best {
				timing.Best = elapsed
				timing.BestCache = cache
				timing.Diagnostics = diagnostics
			}
		}
		out = append(out, timing)
	}
	return out, nil
}

func runJSONBenchPartSetQ1(reader *ColumnPartSetReader, _ queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	cardinality, err := reader.partSetCodeCardinality("commit_collection_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	counts, err := scratch.resetQ1Dense(cardinality)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	var decoded columnPartSetDecodedStats
	for _, loaded := range reader.parts {
		visible := reader.visibleRowsForPart(loaded.Ordinal)
		if len(visible.Rows) == 0 {
			continue
		}
		collectionBlocks, err := lowCardinalityBlocks(loaded.Part, "commit_collection_code")
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		cursor := 0
		for _, block := range collectionBlocks {
			var rows []int
			if !visible.All {
				rows = visibleRowsInBlock(visible.Rows, &cursor, block.Descriptor.FirstRow, block.Descriptor.RowCount)
				if len(rows) == 0 {
					continue
				}
			}
			header, err := scratch.tinyCodeHeader(0, block)
			if err != nil {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, err
			}
			decoded.addBlock(block)
			if visible.All {
				for row := 0; row < block.Descriptor.RowCount; row++ {
					code := readTinyCode(header, row)
					if code >= header.cardinality || code >= cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: collection code %d outside cardinality %d", code, cardinality)
					}
					counts[code]++
				}
			} else {
				for _, partRow := range rows {
					code := readTinyCode(header, partRow-block.Descriptor.FirstRow)
					if code >= header.cardinality || code >= cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: collection code %d outside cardinality %d", code, cardinality)
					}
					counts[code]++
				}
			}
		}
	}
	rows, digest := digestUint64Counts(counts)
	return rows, digest, reader.partSetDiagnostics([]string{"commit_collection_code"}, "multipart_grouped_count_codes", decoded), nil
}

func runJSONBenchPartSetQ2(reader *ColumnPartSetReader, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	collectionCardinality, err := reader.partSetCodeCardinality("commit_collection_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	didCardinality, err := reader.partSetCodeCardinality("did_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	counts, seen, didWords, err := scratch.resetQ2Dense(collectionCardinality, didCardinality)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	var decoded columnPartSetDecodedStats
	for _, loaded := range reader.parts {
		visible := reader.visibleRowsForPart(loaded.Ordinal)
		if len(visible.Rows) == 0 {
			continue
		}
		kindBlocks, operationBlocks, collectionBlocks, didBlocks, err := jsonBenchPartSetCodeBlocks(loaded.Part, jsonBenchPartQ2Columns)
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		cursor := 0
		for blockIndex := range kindBlocks {
			first := kindBlocks[blockIndex].Descriptor.FirstRow
			rowCount := kindBlocks[blockIndex].Descriptor.RowCount
			var rows []int
			if !visible.All {
				rows = visibleRowsInBlock(visible.Rows, &cursor, first, rowCount)
				if len(rows) == 0 {
					continue
				}
			}
			kindHeader, operationHeader, collectionHeader, didHeader, err := scratch.partSetTinyCodeHeaders(kindBlocks[blockIndex], operationBlocks[blockIndex], collectionBlocks[blockIndex], didBlocks[blockIndex])
			if err != nil {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, err
			}
			decoded.addBlocks(kindBlocks[blockIndex], operationBlocks[blockIndex], collectionBlocks[blockIndex], didBlocks[blockIndex])
			if visible.All {
				for row := 0; row < rowCount; row++ {
					kind := readTinyCode(kindHeader, row)
					if kind >= kindHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: kind code %d outside cardinality %d", kind, kindHeader.cardinality)
					}
					if int64(kind) != codes.kindCommit {
						continue
					}
					operation := readTinyCode(operationHeader, row)
					if operation >= operationHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: operation code %d outside cardinality %d", operation, operationHeader.cardinality)
					}
					if int64(operation) != codes.operationCreate {
						continue
					}
					event := readTinyCode(collectionHeader, row)
					if event >= collectionHeader.cardinality || event >= collectionCardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: collection code %d outside cardinality %d", event, collectionCardinality)
					}
					did := readUint32Code(didHeader.data, didHeader.width, row)
					if did >= didHeader.cardinality || did >= didCardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: did code %d outside cardinality %d", did, didCardinality)
					}
					counts[event]++
					seen[int(event)*didWords+int(did/64)] |= uint64(1) << uint(did%64)
				}
			} else {
				for _, partRow := range rows {
					row := partRow - first
					kind := readTinyCode(kindHeader, row)
					if kind >= kindHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: kind code %d outside cardinality %d", kind, kindHeader.cardinality)
					}
					if int64(kind) != codes.kindCommit {
						continue
					}
					operation := readTinyCode(operationHeader, row)
					if operation >= operationHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: operation code %d outside cardinality %d", operation, operationHeader.cardinality)
					}
					if int64(operation) != codes.operationCreate {
						continue
					}
					event := readTinyCode(collectionHeader, row)
					if event >= collectionHeader.cardinality || event >= collectionCardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: collection code %d outside cardinality %d", event, collectionCardinality)
					}
					did := readUint32Code(didHeader.data, didHeader.width, row)
					if did >= didHeader.cardinality || did >= didCardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: did code %d outside cardinality %d", did, didCardinality)
					}
					counts[event]++
					seen[int(event)*didWords+int(did/64)] |= uint64(1) << uint(did%64)
				}
			}
		}
	}
	rows, digest := digestDenseQ2(counts, seen, didWords)
	return rows, digest, reader.partSetDiagnostics(jsonBenchPartQ2Columns, "multipart_fused_dense_group_count_distinct_codes", decoded), nil
}

func runJSONBenchPartSetQ3(reader *ColumnPartSetReader, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	collectionCardinality, err := reader.partSetCodeCardinality("commit_collection_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	counts, err := scratch.resetQ3Dense(collectionCardinality)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	var decoded columnPartSetDecodedStats
	for _, loaded := range reader.parts {
		visible := reader.visibleRowsForPart(loaded.Ordinal)
		if len(visible.Rows) == 0 {
			continue
		}
		kindBlocks, operationBlocks, collectionBlocks, hourBlocks, err := jsonBenchPartSetCodeBlocks(loaded.Part, jsonBenchPartQ3Columns)
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		cursor := 0
		for blockIndex := range kindBlocks {
			first := kindBlocks[blockIndex].Descriptor.FirstRow
			rowCount := kindBlocks[blockIndex].Descriptor.RowCount
			var rows []int
			if !visible.All {
				rows = visibleRowsInBlock(visible.Rows, &cursor, first, rowCount)
				if len(rows) == 0 {
					continue
				}
			}
			kindHeader, operationHeader, collectionHeader, hourHeader, err := scratch.partSetTinyCodeHeaders(kindBlocks[blockIndex], operationBlocks[blockIndex], collectionBlocks[blockIndex], hourBlocks[blockIndex])
			if err != nil {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, err
			}
			decoded.addBlocks(kindBlocks[blockIndex], operationBlocks[blockIndex], collectionBlocks[blockIndex], hourBlocks[blockIndex])
			if visible.All {
				for row := 0; row < rowCount; row++ {
					kind := readTinyCode(kindHeader, row)
					if kind >= kindHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: kind code %d outside cardinality %d", kind, kindHeader.cardinality)
					}
					if int64(kind) != codes.kindCommit {
						continue
					}
					operation := readTinyCode(operationHeader, row)
					if operation >= operationHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: operation code %d outside cardinality %d", operation, operationHeader.cardinality)
					}
					if int64(operation) != codes.operationCreate {
						continue
					}
					event := readTinyCode(collectionHeader, row)
					if event >= collectionHeader.cardinality || event >= collectionCardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: collection code %d outside cardinality %d", event, collectionCardinality)
					}
					if int64(event) != codes.collectionPost && int64(event) != codes.collectionRepost && int64(event) != codes.collectionLike {
						continue
					}
					hour := readUint32Code(hourHeader.data, hourHeader.width, row)
					if hour >= hourHeader.cardinality || hour >= jsonBenchHoursPerDay {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: hour_of_day code %d outside cardinality %d", hour, hourHeader.cardinality)
					}
					counts[int(event)*jsonBenchHoursPerDay+int(hour)]++
				}
			} else {
				for _, partRow := range rows {
					row := partRow - first
					kind := readTinyCode(kindHeader, row)
					if kind >= kindHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: kind code %d outside cardinality %d", kind, kindHeader.cardinality)
					}
					if int64(kind) != codes.kindCommit {
						continue
					}
					operation := readTinyCode(operationHeader, row)
					if operation >= operationHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: operation code %d outside cardinality %d", operation, operationHeader.cardinality)
					}
					if int64(operation) != codes.operationCreate {
						continue
					}
					event := readTinyCode(collectionHeader, row)
					if event >= collectionHeader.cardinality || event >= collectionCardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: collection code %d outside cardinality %d", event, collectionCardinality)
					}
					if int64(event) != codes.collectionPost && int64(event) != codes.collectionRepost && int64(event) != codes.collectionLike {
						continue
					}
					hour := readUint32Code(hourHeader.data, hourHeader.width, row)
					if hour >= hourHeader.cardinality || hour >= jsonBenchHoursPerDay {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: hour_of_day code %d outside cardinality %d", hour, hourHeader.cardinality)
					}
					counts[int(event)*jsonBenchHoursPerDay+int(hour)]++
				}
			}
		}
	}
	rows, digest := digestDenseQ3(counts)
	return rows, digest, reader.partSetDiagnostics(jsonBenchPartQ3Columns, "multipart_fused_dense_group_count_hour_codes", decoded), nil
}

func runJSONBenchPartSetQ4(reader *ColumnPartSetReader, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	didCardinality, err := reader.partSetCodeCardinality("did_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	seen, minTime, users, err := scratch.resetQ4FullDense(didCardinality)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	var decoded columnPartSetDecodedStats
	for _, loaded := range reader.parts {
		visible := reader.visibleRowsForPart(loaded.Ordinal)
		if len(visible.Rows) == 0 {
			continue
		}
		kindBlocks, operationBlocks, collectionBlocks, didBlocks, timeBlocks, err := jsonBenchPartSetQ45Blocks(loaded.Part)
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		cursor := 0
		for blockIndex := range kindBlocks {
			first := kindBlocks[blockIndex].Descriptor.FirstRow
			rowCount := kindBlocks[blockIndex].Descriptor.RowCount
			var rows []int
			if !visible.All {
				rows = visibleRowsInBlock(visible.Rows, &cursor, first, rowCount)
				if len(rows) == 0 {
					continue
				}
			}
			kindHeader, operationHeader, collectionHeader, didHeader, err := scratch.partSetTinyCodeHeaders(kindBlocks[blockIndex], operationBlocks[blockIndex], collectionBlocks[blockIndex], didBlocks[blockIndex])
			if err != nil {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, err
			}
			timeValues, err := scratch.timeReader.DecodeInt64(timeBlocks[blockIndex].Granule)
			if err != nil {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, err
			}
			decoded.addBlocks(kindBlocks[blockIndex], operationBlocks[blockIndex], collectionBlocks[blockIndex], didBlocks[blockIndex], timeBlocks[blockIndex])
			if visible.All {
				for row := 0; row < rowCount; row++ {
					kind := readTinyCode(kindHeader, row)
					if kind >= kindHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: kind code %d outside cardinality %d", kind, kindHeader.cardinality)
					}
					if int64(kind) != codes.kindCommit {
						continue
					}
					operation := readTinyCode(operationHeader, row)
					if operation >= operationHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: operation code %d outside cardinality %d", operation, operationHeader.cardinality)
					}
					if int64(operation) != codes.operationCreate {
						continue
					}
					event := readTinyCode(collectionHeader, row)
					if event >= collectionHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: collection code %d outside cardinality %d", event, collectionHeader.cardinality)
					}
					if int64(event) != codes.collectionPost {
						continue
					}
					user := readUint32Code(didHeader.data, didHeader.width, row)
					if user >= didHeader.cardinality || user >= didCardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: did code %d outside cardinality %d", user, didCardinality)
					}
					timestamp := timeValues[row]
					if bitsetTestAndSet(seen, user) {
						if timestamp < minTime[user] {
							minTime[user] = timestamp
						}
						continue
					}
					minTime[user] = timestamp
					users = append(users, user)
				}
			} else {
				for _, partRow := range rows {
					row := partRow - first
					kind := readTinyCode(kindHeader, row)
					if kind >= kindHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: kind code %d outside cardinality %d", kind, kindHeader.cardinality)
					}
					if int64(kind) != codes.kindCommit {
						continue
					}
					operation := readTinyCode(operationHeader, row)
					if operation >= operationHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: operation code %d outside cardinality %d", operation, operationHeader.cardinality)
					}
					if int64(operation) != codes.operationCreate {
						continue
					}
					event := readTinyCode(collectionHeader, row)
					if event >= collectionHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: collection code %d outside cardinality %d", event, collectionHeader.cardinality)
					}
					if int64(event) != codes.collectionPost {
						continue
					}
					user := readUint32Code(didHeader.data, didHeader.width, row)
					if user >= didHeader.cardinality || user >= didCardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: did code %d outside cardinality %d", user, didCardinality)
					}
					timestamp := timeValues[row]
					if bitsetTestAndSet(seen, user) {
						if timestamp < minTime[user] {
							minTime[user] = timestamp
						}
						continue
					}
					minTime[user] = timestamp
					users = append(users, user)
				}
			}
		}
	}
	scratch.q4Users = users
	top := scratch.q4Pairs[:0]
	for _, user := range users {
		top = insertQ4Top(top, jsonBenchPartTimePair{user: int64(user), t: minTime[user]})
	}
	scratch.q4Pairs = top
	return len(top), digestQ4Top(top), reader.partSetDiagnostics(jsonBenchPartQ4Columns, "multipart_fused_dense_min_by_user", decoded), nil
}

func runJSONBenchPartSetQ5(reader *ColumnPartSetReader, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	didCardinality, err := reader.partSetCodeCardinality("did_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	seen, minTime, maxTime, users, err := scratch.resetQ5Dense(didCardinality)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	var decoded columnPartSetDecodedStats
	for _, loaded := range reader.parts {
		visible := reader.visibleRowsForPart(loaded.Ordinal)
		if len(visible.Rows) == 0 {
			continue
		}
		kindBlocks, operationBlocks, collectionBlocks, didBlocks, timeBlocks, err := jsonBenchPartSetQ45Blocks(loaded.Part)
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		cursor := 0
		for blockIndex := range kindBlocks {
			first := kindBlocks[blockIndex].Descriptor.FirstRow
			rowCount := kindBlocks[blockIndex].Descriptor.RowCount
			var rows []int
			if !visible.All {
				rows = visibleRowsInBlock(visible.Rows, &cursor, first, rowCount)
				if len(rows) == 0 {
					continue
				}
			}
			kindHeader, operationHeader, collectionHeader, didHeader, err := scratch.partSetTinyCodeHeaders(kindBlocks[blockIndex], operationBlocks[blockIndex], collectionBlocks[blockIndex], didBlocks[blockIndex])
			if err != nil {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, err
			}
			timeValues, err := scratch.timeReader.DecodeInt64(timeBlocks[blockIndex].Granule)
			if err != nil {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, err
			}
			decoded.addBlocks(kindBlocks[blockIndex], operationBlocks[blockIndex], collectionBlocks[blockIndex], didBlocks[blockIndex], timeBlocks[blockIndex])
			if visible.All {
				for row := 0; row < rowCount; row++ {
					kind := readTinyCode(kindHeader, row)
					if kind >= kindHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: kind code %d outside cardinality %d", kind, kindHeader.cardinality)
					}
					if int64(kind) != codes.kindCommit {
						continue
					}
					operation := readTinyCode(operationHeader, row)
					if operation >= operationHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: operation code %d outside cardinality %d", operation, operationHeader.cardinality)
					}
					if int64(operation) != codes.operationCreate {
						continue
					}
					event := readTinyCode(collectionHeader, row)
					if event >= collectionHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: collection code %d outside cardinality %d", event, collectionHeader.cardinality)
					}
					if int64(event) != codes.collectionPost {
						continue
					}
					user := readUint32Code(didHeader.data, didHeader.width, row)
					if user >= didHeader.cardinality || user >= didCardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: did code %d outside cardinality %d", user, didCardinality)
					}
					timestamp := timeValues[row]
					if bitsetTestAndSet(seen, user) {
						if timestamp < minTime[user] {
							minTime[user] = timestamp
						}
						if timestamp > maxTime[user] {
							maxTime[user] = timestamp
						}
						continue
					}
					minTime[user] = timestamp
					maxTime[user] = timestamp
					users = append(users, user)
				}
			} else {
				for _, partRow := range rows {
					row := partRow - first
					kind := readTinyCode(kindHeader, row)
					if kind >= kindHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: kind code %d outside cardinality %d", kind, kindHeader.cardinality)
					}
					if int64(kind) != codes.kindCommit {
						continue
					}
					operation := readTinyCode(operationHeader, row)
					if operation >= operationHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: operation code %d outside cardinality %d", operation, operationHeader.cardinality)
					}
					if int64(operation) != codes.operationCreate {
						continue
					}
					event := readTinyCode(collectionHeader, row)
					if event >= collectionHeader.cardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: collection code %d outside cardinality %d", event, collectionHeader.cardinality)
					}
					if int64(event) != codes.collectionPost {
						continue
					}
					user := readUint32Code(didHeader.data, didHeader.width, row)
					if user >= didHeader.cardinality || user >= didCardinality {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: did code %d outside cardinality %d", user, didCardinality)
					}
					timestamp := timeValues[row]
					if bitsetTestAndSet(seen, user) {
						if timestamp < minTime[user] {
							minTime[user] = timestamp
						}
						if timestamp > maxTime[user] {
							maxTime[user] = timestamp
						}
						continue
					}
					minTime[user] = timestamp
					maxTime[user] = timestamp
					users = append(users, user)
				}
			}
		}
	}
	scratch.q5Users = users
	top := scratch.q5Pairs[:0]
	for _, user := range users {
		top = insertQ5Top(top, jsonBenchPartSpanPair{user: int64(user), span: maxTime[user] - minTime[user]})
	}
	scratch.q5Pairs = top
	return len(top), digestQ5Top(top), reader.partSetDiagnostics(jsonBenchPartQ5Columns, "multipart_fused_dense_span_by_user", decoded), nil
}

type columnPartSetDecodedStats struct {
	GranulesDecoded int
	BlocksDecoded   int
	BytesDecoded    int
}

func (s *columnPartSetDecodedStats) addBlock(block ColumnBlock) {
	s.GranulesDecoded += blockGranuleSpan(block)
	s.BlocksDecoded++
	s.BytesDecoded += block.Granule.RawBytes
}

func (s *columnPartSetDecodedStats) addBlocks(blocks ...ColumnBlock) {
	for _, block := range blocks {
		s.addBlock(block)
	}
}

func (r *ColumnPartSetReader) partSetCodeCardinality(column string) (uint32, error) {
	if r == nil {
		return 0, fmt.Errorf("colgranule: nil part set reader")
	}
	for _, loaded := range r.parts {
		if len(r.visibleRowsForPart(loaded.Ordinal).Rows) == 0 {
			continue
		}
		return partCodeCardinality(loaded.Part, column)
	}
	return 0, fmt.Errorf("colgranule: no visible parts for code cardinality column %s", column)
}

func (r *ColumnPartSetReader) partSetDiagnostics(columns []string, kernel string, decoded columnPartSetDecodedStats) JSONBenchPartQueryDiagnostics {
	return JSONBenchPartQueryDiagnostics{
		RowsScanned:        r.visibilityStat.InputRows,
		RowsReturned:       r.visibilityStat.VisibleRows,
		RowsSuperseded:     r.visibilityStat.SupersededRows,
		RowsDeleted:        r.visibilityStat.DeletedRows,
		PartsConsidered:    len(r.parts),
		BaseParts:          r.visibilityStat.BaseParts,
		DeltaParts:         r.visibilityStat.DeltaParts,
		Tombstones:         r.visibilityStat.Tombstones,
		GranulesConsidered: r.totalGranules(),
		GranulesDecoded:    decoded.GranulesDecoded,
		BlocksDecoded:      decoded.BlocksDecoded,
		BytesDecoded:       decoded.BytesDecoded,
		ColumnsProjected:   append([]string(nil), columns...),
		AggregateKernel:    kernel,
		PartSetCacheStats:  r.cacheStats,
	}
}

func (r *ColumnPartSetReader) totalGranules() int {
	if r == nil {
		return 0
	}
	total := 0
	for _, loaded := range r.parts {
		total += len(loaded.Part.Descriptor.Granules)
	}
	return total
}

func visibleRowsInBlock(visible []int, cursor *int, first int, rowCount int) []int {
	limit := first + rowCount
	for *cursor < len(visible) && visible[*cursor] < first {
		*cursor = *cursor + 1
	}
	start := *cursor
	for *cursor < len(visible) && visible[*cursor] < limit {
		*cursor = *cursor + 1
	}
	return visible[start:*cursor]
}

func jsonBenchPartSetCodeBlocks(part *ColumnPart, columns []string) ([]ColumnBlock, []ColumnBlock, []ColumnBlock, []ColumnBlock, error) {
	if len(columns) != 4 {
		return nil, nil, nil, nil, fmt.Errorf("colgranule: part-set code block helper got %d columns", len(columns))
	}
	first, err := lowCardinalityBlocks(part, columns[0])
	if err != nil {
		return nil, nil, nil, nil, err
	}
	second, err := lowCardinalityBlocks(part, columns[1])
	if err != nil {
		return nil, nil, nil, nil, err
	}
	third, err := lowCardinalityBlocks(part, columns[2])
	if err != nil {
		return nil, nil, nil, nil, err
	}
	fourth, err := lowCardinalityBlocks(part, columns[3])
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if err := validateAlignedBlocks(first, second, third, fourth); err != nil {
		return nil, nil, nil, nil, err
	}
	return first, second, third, fourth, nil
}

func jsonBenchPartSetQ45Blocks(part *ColumnPart) ([]ColumnBlock, []ColumnBlock, []ColumnBlock, []ColumnBlock, []ColumnBlock, error) {
	kindBlocks, err := lowCardinalityBlocks(part, "kind_code")
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	operationBlocks, err := lowCardinalityBlocks(part, "commit_operation_code")
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	collectionBlocks, err := lowCardinalityBlocks(part, "commit_collection_code")
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	didBlocks, err := lowCardinalityBlocks(part, "did_code")
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	timeBlocks, err := int64Blocks(part, "time_us")
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	if err := validateAlignedBlocks(kindBlocks, operationBlocks, collectionBlocks, didBlocks, timeBlocks); err != nil {
		return nil, nil, nil, nil, nil, err
	}
	return kindBlocks, operationBlocks, collectionBlocks, didBlocks, timeBlocks, nil
}

func (s *jsonBenchPartQueryScratch) resetQ1Dense(cardinality uint32) ([]uint64, error) {
	if cardinality == 0 {
		return nil, fmt.Errorf("colgranule: invalid q1 cardinality 0")
	}
	if cardinality > maxAggregateCells {
		return nil, fmt.Errorf("colgranule: q1 cardinality=%d exceeds cap %d", cardinality, maxAggregateCells)
	}
	if cap(s.q1Counts) < int(cardinality) {
		s.q1Counts = make([]uint64, cardinality)
	} else {
		s.q1Counts = s.q1Counts[:cardinality]
	}
	clear(s.q1Counts)
	return s.q1Counts, nil
}

func (s *jsonBenchPartQueryScratch) partSetTinyCodeHeaders(first ColumnBlock, second ColumnBlock, third ColumnBlock, fourth ColumnBlock) (tinyCodeHeader, tinyCodeHeader, tinyCodeHeader, uint32CodesHeader, error) {
	firstHeader, err := s.tinyCodeHeader(0, first)
	if err != nil {
		return tinyCodeHeader{}, tinyCodeHeader{}, tinyCodeHeader{}, uint32CodesHeader{}, err
	}
	secondHeader, err := s.tinyCodeHeader(1, second)
	if err != nil {
		return tinyCodeHeader{}, tinyCodeHeader{}, tinyCodeHeader{}, uint32CodesHeader{}, err
	}
	thirdHeader, err := s.tinyCodeHeader(2, third)
	if err != nil {
		return tinyCodeHeader{}, tinyCodeHeader{}, tinyCodeHeader{}, uint32CodesHeader{}, err
	}
	fourthHeader, err := s.codeHeader(3, fourth)
	if err != nil {
		return tinyCodeHeader{}, tinyCodeHeader{}, tinyCodeHeader{}, uint32CodesHeader{}, err
	}
	return firstHeader, secondHeader, thirdHeader, fourthHeader, nil
}

func jsonBenchPartSetDiagnostics(scan ColumnPartSetScanResult, kernel string) JSONBenchPartQueryDiagnostics {
	return JSONBenchPartQueryDiagnostics{
		RowsScanned:        scan.Diagnostics.RowsScanned,
		RowsReturned:       scan.Diagnostics.RowsReturned,
		RowsSuperseded:     scan.Diagnostics.RowsSuperseded,
		RowsDeleted:        scan.Diagnostics.RowsDeleted,
		GranulesConsidered: scan.Diagnostics.GranulesConsidered,
		BlocksDecoded:      scan.Diagnostics.BlocksDecoded,
		BytesDecoded:       scan.Diagnostics.BytesDecoded,
		ColumnsProjected:   append([]string(nil), scan.Diagnostics.ColumnsProjected...),
		AggregateKernel:    kernel,
		PartsConsidered:    scan.Diagnostics.PartsConsidered,
		BaseParts:          scan.Diagnostics.BaseParts,
		DeltaParts:         scan.Diagnostics.DeltaParts,
		Tombstones:         scan.Diagnostics.Tombstones,
		PartSetCacheStats:  scan.Diagnostics.CacheStats,
	}
}
