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
		dst[name] = dst[name][:0]
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
		visibleRows := r.visibleRows[partIndex]
		if len(visibleRows) == 0 {
			continue
		}
		scan, err := loaded.Part.NewScanner().ScanProjected(columns)
		if err != nil {
			return ColumnPartSetScanResult{}, err
		}
		diagnostics.RowsScanned += loaded.Part.Descriptor.RowCount
		diagnostics.GranulesConsidered += scan.Diagnostics.GranulesConsidered
		diagnostics.BlocksDecoded += scan.Diagnostics.BlocksDecoded
		diagnostics.BytesDecoded += scan.Diagnostics.BytesDecoded
		for row := 0; row < loaded.Part.Descriptor.RowCount; row++ {
			if _, ok := visibleRows[row]; !ok {
				continue
			}
			for _, name := range columns {
				dst[name] = append(dst[name], scan.Columns[name][row])
			}
			diagnostics.RowsReturned++
		}
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
	stats.VisibleRows = len(r.latest)
	r.visibilityStat = stats
	return nil
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
		run         func(*ColumnPartSetReader, queryCodeSet) (int, uint64, JSONBenchPartQueryDiagnostics, error)
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
		for i := 0; i < attempts; i++ {
			cache := "cold"
			if i > 0 {
				cache = "warm"
			}
			start := time.Now()
			rows, digest, diagnostics, err := q.run(reader, codes)
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

func runJSONBenchPartSetQ1(reader *ColumnPartSetReader, _ queryCodeSet) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	scan, err := reader.ScanProjected([]string{"commit_collection_code"})
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	counts := make(map[int64]int64, 16)
	for _, code := range scan.Columns["commit_collection_code"] {
		counts[code]++
	}
	return len(counts), digestCounts(counts), jsonBenchPartSetDiagnostics(scan, "multipart_grouped_count_codes"), nil
}

func runJSONBenchPartSetQ2(reader *ColumnPartSetReader, codes queryCodeSet) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	scan, err := reader.ScanProjected(jsonBenchPartQ2Columns)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	kind := scan.Columns["kind_code"]
	operation := scan.Columns["commit_operation_code"]
	collection := scan.Columns["commit_collection_code"]
	did := scan.Columns["did_code"]
	counts := make(map[int64]int64, 16)
	unique := make(map[int64]map[int64]struct{}, 16)
	for i := range collection {
		if kind[i] != codes.kindCommit || operation[i] != codes.operationCreate {
			continue
		}
		event := collection[i]
		counts[event]++
		if unique[event] == nil {
			unique[event] = make(map[int64]struct{})
		}
		unique[event][did[i]] = struct{}{}
	}
	digest := digestCounts(counts)
	events := make([]int64, 0, len(unique))
	for event := range unique {
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool { return events[i] < events[j] })
	for _, event := range events {
		digest = digestMix(digest, uint64(event), uint64(len(unique[event])))
	}
	return len(counts), digest, jsonBenchPartSetDiagnostics(scan, "multipart_group_count_distinct_codes"), nil
}

func runJSONBenchPartSetQ3(reader *ColumnPartSetReader, codes queryCodeSet) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	scan, err := reader.ScanProjected(jsonBenchPartQ3Columns)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	kind := scan.Columns["kind_code"]
	operation := scan.Columns["commit_operation_code"]
	collection := scan.Columns["commit_collection_code"]
	hour := scan.Columns["hour_of_day"]
	counts := make(map[int64]int64, 128)
	for i := range collection {
		event := collection[i]
		if kind[i] != codes.kindCommit || operation[i] != codes.operationCreate {
			continue
		}
		if event != codes.collectionPost && event != codes.collectionRepost && event != codes.collectionLike {
			continue
		}
		counts[event*100+hour[i]]++
	}
	return len(counts), digestCounts(counts), jsonBenchPartSetDiagnostics(scan, "multipart_group_count_hour_codes"), nil
}

func runJSONBenchPartSetQ4(reader *ColumnPartSetReader, codes queryCodeSet) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	scan, err := reader.ScanProjected(jsonBenchPartQ4Columns)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	kind := scan.Columns["kind_code"]
	operation := scan.Columns["commit_operation_code"]
	collection := scan.Columns["commit_collection_code"]
	did := scan.Columns["did_code"]
	timeUS := scan.Columns["time_us"]
	first := make(map[int64]int64, 128)
	for i := range collection {
		if kind[i] != codes.kindCommit || operation[i] != codes.operationCreate || collection[i] != codes.collectionPost {
			continue
		}
		user := did[i]
		if prev, ok := first[user]; !ok || timeUS[i] < prev {
			first[user] = timeUS[i]
		}
	}
	top := make([]jsonBenchPartTimePair, 0, len(first))
	for user, t := range first {
		top = insertQ4Top(top, jsonBenchPartTimePair{user: user, t: t})
	}
	return len(top), digestQ4Top(top), jsonBenchPartSetDiagnostics(scan, "multipart_min_by_user"), nil
}

func runJSONBenchPartSetQ5(reader *ColumnPartSetReader, codes queryCodeSet) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	scan, err := reader.ScanProjected(jsonBenchPartQ5Columns)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	kind := scan.Columns["kind_code"]
	operation := scan.Columns["commit_operation_code"]
	collection := scan.Columns["commit_collection_code"]
	did := scan.Columns["did_code"]
	timeUS := scan.Columns["time_us"]
	spans := make(map[int64]jsonBenchPartSpan, 128)
	for i := range collection {
		if kind[i] != codes.kindCommit || operation[i] != codes.operationCreate || collection[i] != codes.collectionPost {
			continue
		}
		user := did[i]
		span, ok := spans[user]
		if !ok {
			spans[user] = jsonBenchPartSpan{min: timeUS[i], max: timeUS[i]}
			continue
		}
		if timeUS[i] < span.min {
			span.min = timeUS[i]
		}
		if timeUS[i] > span.max {
			span.max = timeUS[i]
		}
		spans[user] = span
	}
	top := make([]jsonBenchPartSpanPair, 0, len(spans))
	for user, span := range spans {
		top = insertQ5Top(top, jsonBenchPartSpanPair{user: user, span: span.max - span.min})
	}
	return len(top), digestQ5Top(top), jsonBenchPartSetDiagnostics(scan, "multipart_span_by_user"), nil
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
