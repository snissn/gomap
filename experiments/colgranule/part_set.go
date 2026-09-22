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

type ColumnPartSetCompactionPolicy struct {
	MaxDeltaParts             int `json:"max_delta_parts,omitempty"`
	MaxDeltaBytes             int `json:"max_delta_bytes,omitempty"`
	MaxTombstones             int `json:"max_tombstones,omitempty"`
	MaxReadAmplificationParts int `json:"max_read_amplification_parts,omitempty"`
	MaxStaleBytes             int `json:"max_stale_bytes,omitempty"`
	MinExpectedReclaimPPM     int `json:"min_expected_reclaim_ppm,omitempty"`
	MinVisibleRowsPPM         int `json:"min_visible_rows_ppm,omitempty"`
}

type ColumnPartSetCompactionPlan struct {
	ShouldCompact                bool     `json:"should_compact"`
	Reasons                      []string `json:"reasons,omitempty"`
	SelectedParts                int      `json:"selected_parts"`
	SkippedParts                 int      `json:"skipped_parts"`
	BaseParts                    int      `json:"base_parts"`
	DeltaParts                   int      `json:"delta_parts"`
	Tombstones                   int      `json:"tombstones"`
	ReadAmplificationParts       int      `json:"read_amplification_parts"`
	LiveBytes                    int      `json:"live_bytes"`
	StaleBytes                   int      `json:"stale_bytes"`
	TombstoneDebt                int      `json:"tombstone_debt"`
	ExpectedReclaimPPM           int      `json:"expected_reclaim_ppm"`
	VisibleRowsPPM               int      `json:"visible_rows_ppm"`
	AggregateMetadataInvalid     bool     `json:"aggregate_metadata_invalid"`
	AggregateMetadataRebuilds    bool     `json:"aggregate_metadata_rebuilds"`
	ColumnAssetStaleBytePressure bool     `json:"column_asset_stale_byte_pressure"`
}

type ColumnPartSetScanResult struct {
	Rows        int
	Columns     map[string][]int64
	Diagnostics ColumnPartSetScanDiagnostics
}

type ColumnPartSetPointLookupScratch struct {
	scanner ColumnPartScanner
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
	Manifest                ColumnCollectionManifest    `json:"manifest"`
	Part                    ColumnWorkspacePartManifest `json:"part"`
	InputRows               int                         `json:"input_rows"`
	VisibleRows             int                         `json:"visible_rows"`
	DroppedRows             int                         `json:"dropped_rows"`
	SupersededRows          int                         `json:"superseded_rows"`
	DeletedRows             int                         `json:"deleted_rows"`
	OldAssetBytes           int                         `json:"old_asset_bytes"`
	NewAssetBytes           int                         `json:"new_asset_bytes"`
	ReclaimableBytes        int                         `json:"reclaimable_bytes"`
	RewriteDebtBytes        int                         `json:"rewrite_debt_bytes"`
	NetBytesReduced         int                         `json:"net_bytes_reduced"`
	SelectionPlan           ColumnPartSetCompactionPlan `json:"selection_plan"`
	PrePublishReachability  ColumnAssetReachabilityPlan `json:"pre_publish_reachability"`
	PostPublishReachability ColumnAssetReachabilityPlan `json:"post_publish_reachability"`
	CompactionUnix          int64                       `json:"compaction_unix_nano"`
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

func (r *ColumnPartSetReader) ScanLatestLocator(primaryID int64) (RowLocator, bool) {
	ref, ok := r.scanLatestRowRef(primaryID)
	if !ok {
		return RowLocator{}, false
	}
	return ref.Locator, true
}

func (r *ColumnPartSetReader) ValueAtLatest(primaryID int64, columnName string) (int64, bool, error) {
	return r.ValueAtLatestWithScratch(primaryID, columnName, nil)
}

func (r *ColumnPartSetReader) ValueAtLatestWithScratch(primaryID int64, columnName string, scratch *ColumnPartSetPointLookupScratch) (int64, bool, error) {
	if r == nil {
		return 0, false, nil
	}
	ref, ok := r.latest[primaryID]
	if !ok {
		return 0, false, nil
	}
	value, err := r.valueAtRowRef(ref, columnName, scratch)
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
}

func (r *ColumnPartSetReader) ScanValueAtLatest(primaryID int64, columnName string) (int64, bool, error) {
	return r.ScanValueAtLatestWithScratch(primaryID, columnName, nil)
}

func (r *ColumnPartSetReader) ScanValueAtLatestWithScratch(primaryID int64, columnName string, scratch *ColumnPartSetPointLookupScratch) (int64, bool, error) {
	ref, ok := r.scanLatestRowRef(primaryID)
	if !ok {
		return 0, false, nil
	}
	value, err := r.valueAtRowRef(ref, columnName, scratch)
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
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

func (r *ColumnPartSetReader) scanLatestRowRef(primaryID int64) (columnPartSetRowRef, bool) {
	if r == nil {
		return columnPartSetRowRef{}, false
	}
	var best columnPartSetRowRef
	var found bool
	tombstoneGeneration, tombstoned := r.tombstoneByID[primaryID]
	for partIndex, loaded := range r.parts {
		locator, ok := loaded.Part.LocatePrimaryID(primaryID)
		if !ok {
			continue
		}
		row := columnPartSetRowRef{
			PrimaryID:    primaryID,
			PartIndex:    partIndex,
			PartRow:      locator.PartRow,
			GenerationID: loaded.Ref.GenerationID,
			Ordinal:      loaded.Ordinal,
			Locator:      locator,
		}
		if tombstoned && tombstoneGeneration >= row.GenerationID {
			continue
		}
		if !found || row.newerThan(best) {
			best = row
			found = true
		}
	}
	return best, found
}

func (r *ColumnPartSetReader) valueAtRowRef(ref columnPartSetRowRef, columnName string, scratch *ColumnPartSetPointLookupScratch) (int64, error) {
	if r == nil {
		return 0, fmt.Errorf("colgranule: nil part set reader")
	}
	if ref.PartIndex < 0 || ref.PartIndex >= len(r.parts) {
		return 0, fmt.Errorf("colgranule: row ref part index %d outside %d parts", ref.PartIndex, len(r.parts))
	}
	if scratch == nil {
		scanner := ColumnPartScanner{part: r.parts[ref.PartIndex].Part}
		return scanner.ValueAt(ref.Locator, columnName)
	}
	scratch.scanner.part = r.parts[ref.PartIndex].Part
	return scratch.scanner.ValueAt(ref.Locator, columnName)
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

func PlanColumnPartSetCompaction(manifest ColumnCollectionManifest, stats ColumnPartSetVisibilityStats, policy ColumnPartSetCompactionPolicy) (ColumnPartSetCompactionPlan, error) {
	if err := validateColumnCollectionManifest(manifest); err != nil {
		return ColumnPartSetCompactionPlan{}, err
	}
	totalParts := len(manifest.PartSet.BaseParts) + len(manifest.PartSet.DeltaParts)
	if stats.Parts == 0 {
		stats.Parts = totalParts
		stats.BaseParts = len(manifest.PartSet.BaseParts)
		stats.DeltaParts = len(manifest.PartSet.DeltaParts)
		stats.InputRows = manifest.ByteAccounting.Rows
		stats.VisibleRows = manifest.ByteAccounting.VisibleRows
		stats.Tombstones = len(manifest.PartSet.Tombstones)
	}
	totalBytes := manifest.ByteAccounting.TotalAssetBytes
	staleRows := stats.SupersededRows + stats.DeletedRows
	staleBytes := proportionalBytes(totalBytes, staleRows, stats.InputRows)
	if staleBytes > totalBytes {
		staleBytes = totalBytes
	}
	deltaBytes := manifest.ByteAccounting.DeltaAssetBytes
	plan := ColumnPartSetCompactionPlan{
		BaseParts:                    len(manifest.PartSet.BaseParts),
		DeltaParts:                   len(manifest.PartSet.DeltaParts),
		Tombstones:                   len(manifest.PartSet.Tombstones),
		ReadAmplificationParts:       totalParts,
		LiveBytes:                    totalBytes - staleBytes,
		StaleBytes:                   staleBytes,
		TombstoneDebt:                len(manifest.PartSet.Tombstones),
		ExpectedReclaimPPM:           ppm(staleBytes, totalBytes),
		VisibleRowsPPM:               ppm(stats.VisibleRows, stats.InputRows),
		AggregateMetadataInvalid:     stats.SupersededRows != 0 || stats.DeletedRows != 0 || len(manifest.PartSet.Tombstones) != 0,
		AggregateMetadataRebuilds:    totalParts != 0,
		ColumnAssetStaleBytePressure: policy.MaxStaleBytes > 0 && staleBytes >= policy.MaxStaleBytes,
	}
	if policy.MaxDeltaParts > 0 && len(manifest.PartSet.DeltaParts) >= policy.MaxDeltaParts {
		plan.Reasons = append(plan.Reasons, "delta_part_count")
	}
	if policy.MaxDeltaBytes > 0 && deltaBytes >= policy.MaxDeltaBytes {
		plan.Reasons = append(plan.Reasons, "delta_bytes")
	}
	if policy.MaxTombstones > 0 && len(manifest.PartSet.Tombstones) >= policy.MaxTombstones {
		plan.Reasons = append(plan.Reasons, "tombstone_count")
	}
	if policy.MaxReadAmplificationParts > 0 && totalParts >= policy.MaxReadAmplificationParts {
		plan.Reasons = append(plan.Reasons, "read_amplification")
	}
	if plan.ColumnAssetStaleBytePressure {
		plan.Reasons = append(plan.Reasons, "column_asset_stale_bytes")
	}
	if policy.MinExpectedReclaimPPM > 0 && plan.ExpectedReclaimPPM >= policy.MinExpectedReclaimPPM {
		plan.Reasons = append(plan.Reasons, "expected_reclaim_ratio")
	}
	if policy.MinVisibleRowsPPM > 0 && plan.VisibleRowsPPM <= policy.MinVisibleRowsPPM {
		plan.Reasons = append(plan.Reasons, "sparse_visible_rows")
	}
	if plan.AggregateMetadataInvalid {
		plan.Reasons = append(plan.Reasons, "aggregate_metadata_invalidation")
	}
	plan.ShouldCompact = len(plan.Reasons) != 0
	if plan.ShouldCompact {
		plan.SelectedParts = totalParts
	} else {
		plan.SkippedParts = totalParts
	}
	return plan, nil
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
	selectionPlan, err := PlanColumnPartSetCompaction(reader.manifest, reader.visibilityStat, ColumnPartSetCompactionPolicy{
		MaxDeltaParts:             1,
		MaxTombstones:             1,
		MaxReadAmplificationParts: 2,
		MinExpectedReclaimPPM:     1,
	})
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
		[]ColumnManifestPartRef{NewColumnManifestPartRefWithCoverage(ColumnPartRoleBase, newGeneration, entry, columnPartSetCompactionSourceParts(reader.manifest.PartSet), columnPartSetNextCompactionLevel(reader.manifest.PartSet))},
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
	oldManifest := reader.manifest
	prePublishPlan, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		ActiveManifest:   &oldManifest,
		PendingManifests: []ColumnCollectionManifest{manifest},
	})
	if err != nil {
		return ColumnPartSetCompactionResult{}, err
	}
	postPublishPlan, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		RecoveryAuthoritativeManifests: []ColumnCollectionManifest{manifest},
		CleanupSafeManifests:           []ColumnCollectionManifest{oldManifest},
	})
	if err != nil {
		return ColumnPartSetCompactionResult{}, err
	}
	netReduced := oldAssetBytes - entry.AssetBytes
	if netReduced < 0 {
		netReduced = 0
	}
	return ColumnPartSetCompactionResult{
		Manifest:                manifest,
		Part:                    entry,
		InputRows:               reader.visibilityStat.InputRows,
		VisibleRows:             scan.Rows,
		DroppedRows:             reader.visibilityStat.InputRows - scan.Rows,
		SupersededRows:          reader.visibilityStat.SupersededRows,
		DeletedRows:             reader.visibilityStat.DeletedRows,
		OldAssetBytes:           oldAssetBytes,
		NewAssetBytes:           entry.AssetBytes,
		ReclaimableBytes:        postPublishPlan.ReclaimableBytes,
		RewriteDebtBytes:        postPublishPlan.RewriteDebtBytes,
		NetBytesReduced:         netReduced,
		SelectionPlan:           selectionPlan,
		PrePublishReachability:  prePublishPlan,
		PostPublishReachability: postPublishPlan,
		CompactionUnix:          time.Now().UnixNano(),
	}, nil
}

func proportionalBytes(totalBytes int, rows int, totalRows int) int {
	if totalBytes <= 0 || rows <= 0 || totalRows <= 0 {
		return 0
	}
	if rows >= totalRows {
		return totalBytes
	}
	return int((int64(totalBytes) * int64(rows)) / int64(totalRows))
}

func ppm(numerator int, denominator int) int {
	if numerator <= 0 || denominator <= 0 {
		return 0
	}
	if numerator >= denominator {
		return 1_000_000
	}
	return int((int64(numerator) * 1_000_000) / int64(denominator))
}

func columnPartSetCompactionSourceParts(partSet ColumnPartSetManifest) []ColumnSourcePartGeneration {
	refs := append([]ColumnManifestPartRef(nil), partSet.BaseParts...)
	refs = append(refs, partSet.DeltaParts...)
	out := make([]ColumnSourcePartGeneration, 0, len(refs))
	for _, ref := range refs {
		out = append(out, ColumnSourcePartGeneration{PartID: ref.Part.PartID, GenerationID: ref.GenerationID})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].GenerationID != out[j].GenerationID {
			return out[i].GenerationID < out[j].GenerationID
		}
		return out[i].PartID < out[j].PartID
	})
	return out
}

func columnPartSetNextCompactionLevel(partSet ColumnPartSetManifest) uint8 {
	var maxLevel uint8
	for _, ref := range partSet.BaseParts {
		if ref.Coverage.CompactionLevel > maxLevel {
			maxLevel = ref.Coverage.CompactionLevel
		}
	}
	for _, ref := range partSet.DeltaParts {
		if ref.Coverage.CompactionLevel > maxLevel {
			maxLevel = ref.Coverage.CompactionLevel
		}
	}
	if maxLevel == ^uint8(0) {
		return maxLevel
	}
	return maxLevel + 1
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
			kindHeader, err := scratch.tinyCodeHeader(0, kindBlocks[blockIndex])
			if err != nil {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, err
			}
			operationHeader, err := scratch.tinyCodeHeader(1, operationBlocks[blockIndex])
			if err != nil {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, err
			}
			collectionHeader, err := scratch.tinyCodeHeader(2, collectionBlocks[blockIndex])
			if err != nil {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, err
			}
			hourHeader, err := scratch.tinyCodeHeader(3, hourBlocks[blockIndex])
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
					hour := readTinyCode(hourHeader, row)
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
					hour := readTinyCode(hourHeader, row)
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
	if reader.canUseTimeOrderedPartSetQ4() {
		return runJSONBenchPartSetQ4TimeOrdered(reader, codes, scratch)
	}
	return runJSONBenchPartSetQ4FullScan(reader, codes, scratch)
}

func runJSONBenchPartSetQ4TimeOrdered(reader *ColumnPartSetReader, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	var global [3]jsonBenchPartTimePair
	top := global[:0]
	rowsScanned := 0
	granulesConsidered := 0
	granulesDecoded := 0
	blocksDecoded := 0
	bytesDecoded := 0
	for _, loaded := range reader.parts {
		visible := reader.visibleRowsForPart(loaded.Ordinal)
		if len(visible.Rows) == 0 {
			continue
		}
		if !visible.All {
			return runJSONBenchPartSetQ4FullScan(reader, codes, scratch)
		}
		if err := requireTimeUSAscendingSortKey(loaded.Part); err != nil {
			return runJSONBenchPartSetQ4FullScan(reader, codes, scratch)
		}
		// Any user outside a part-local top 3 has three distinct earlier users in that part,
		// so it cannot enter the global top 3 after merging parts.
		_, _, diagnostics, err := runJSONBenchPartQ4(loaded.Part, codes, scratch)
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		for _, candidate := range scratch.q4Pairs {
			top = insertQ4TopUnique(top, candidate)
		}
		rowsScanned += diagnostics.RowsScanned
		granulesConsidered += diagnostics.GranulesConsidered
		granulesDecoded += diagnostics.GranulesDecoded
		blocksDecoded += diagnostics.BlocksDecoded
		bytesDecoded += diagnostics.BytesDecoded
	}
	scratch.q4Pairs = append(scratch.q4Pairs[:0], top...)
	diagnostics := reader.partSetKernelDiagnostics(jsonBenchPartQ4Columns, "multipart_sort_key_early_stop_min_by_user", rowsScanned, granulesConsidered, 0, columnPartSetDecodedStats{
		GranulesDecoded: granulesDecoded,
		BlocksDecoded:   blocksDecoded,
		BytesDecoded:    bytesDecoded,
	})
	diagnostics.EarlyStopAvailable = true
	diagnostics.SortKey = reader.partSetSortKeyColumns()
	return len(top), digestQ4Top(top), diagnostics, nil
}

func runJSONBenchPartSetQ4FullScan(reader *ColumnPartSetReader, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
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

func runJSONBenchPartSetQ4ClickHouseOrder(reader *ColumnPartSetReader, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	didCardinality, err := reader.partSetCodeCardinality("did_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	seen, minTime, users, err := scratch.resetQ4FullDense(didCardinality)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	sortKeyRanges := []Int64RangePredicate{
		{Column: "kind_code", Low: codes.kindCommit, High: codes.kindCommit},
		{Column: "commit_operation_code", Low: codes.operationCreate, High: codes.operationCreate},
		{Column: "commit_collection_code", Low: codes.collectionPost, High: codes.collectionPost},
	}
	rowsScanned := 0
	granulesSkipped := 0
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
			mayContain, skipped, err := blockMayContainSortKeyRanges(loaded.Part, kindBlocks[blockIndex], sortKeyRanges)
			if err != nil {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, err
			}
			if !mayContain {
				granulesSkipped += skipped
				continue
			}
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
				timeCursor, err := scratch.timeReader.int64Cursor(timeBlocks[blockIndex].Granule)
				if err != nil {
					return 0, 0, JSONBenchPartQueryDiagnostics{}, err
				}
				rowsScanned += rowCount
				for row := 0; row < rowCount; row++ {
					timestamp, err := timeCursor.Next()
					if err != nil {
						return 0, 0, JSONBenchPartQueryDiagnostics{}, err
					}
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
					if bitsetTestAndSet(seen, user) {
						if timestamp < minTime[user] {
							minTime[user] = timestamp
						}
						continue
					}
					minTime[user] = timestamp
					users = append(users, user)
				}
				if err := timeCursor.Finish(); err != nil {
					return 0, 0, JSONBenchPartQueryDiagnostics{}, err
				}
				decoded.GranulesDecoded += blockGranuleSpan(timeBlocks[blockIndex])
				decoded.BlocksDecoded++
				decoded.BytesDecoded += timeCursor.RawBytesRead()
			} else {
				timeValues, err := scratch.timeReader.DecodeInt64(timeBlocks[blockIndex].Granule)
				if err != nil {
					return 0, 0, JSONBenchPartQueryDiagnostics{}, err
				}
				if len(timeValues) != rowCount {
					return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: q4b time/code row mismatch time=%d codes=%d", len(timeValues), rowCount)
				}
				decoded.addBlock(timeBlocks[blockIndex])
				rowsScanned += len(rows)
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
	diagnostics := reader.partSetKernelDiagnostics(jsonBenchPartQ4Columns, "multipart_clickhouse_order_prefix_scan_min_by_user", rowsScanned, reader.totalGranules(), granulesSkipped, decoded)
	diagnostics.SortKey = reader.partSetSortKeyColumns()
	return len(top), digestQ4Top(top), diagnostics, nil
}

func runJSONBenchPartSetQ4AggregateMetadata(reader *ColumnPartSetReader, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	didCardinality, err := reader.partSetCodeCardinality("did_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	seen, minTime, users, err := scratch.resetQ4FullDense(didCardinality)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	var aggregate aggregateMetadataPartSetStats
	for _, loaded := range reader.parts {
		visible := reader.visibleRowsForPart(loaded.Ordinal)
		if len(visible.Rows) == 0 {
			continue
		}
		if !visible.All {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: aggregate metadata q4 requires all-visible part %d", loaded.Part.Descriptor.PartID)
		}
		metadata, err := requireJSONBenchPostCreateDidTimeMetadata(loaded.Part, codes)
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		aggregate.add(metadata)
		for _, granule := range metadata.Granules {
			for _, entry := range granule.Entries {
				if entry.Group >= didCardinality {
					return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: aggregate metadata did code %d outside cardinality %d", entry.Group, didCardinality)
				}
				if bitsetTestAndSet(seen, entry.Group) {
					if entry.Min < minTime[entry.Group] {
						minTime[entry.Group] = entry.Min
					}
					continue
				}
				minTime[entry.Group] = entry.Min
				users = append(users, entry.Group)
			}
		}
	}
	scratch.q4Users = users
	top := scratch.q4Pairs[:0]
	for _, user := range users {
		top = insertQ4Top(top, jsonBenchPartTimePair{user: int64(user), t: minTime[user]})
	}
	scratch.q4Pairs = top
	diagnostics := reader.partSetAggregateMetadataDiagnostics(jsonBenchPartQ4Columns, "multipart_aggregate_metadata_min_by_user", aggregate)
	return len(top), digestQ4Top(top), diagnostics, nil
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

func runJSONBenchPartSetQ5AggregateMetadata(reader *ColumnPartSetReader, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	didCardinality, err := reader.partSetCodeCardinality("did_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	seen, minTime, maxTime, users, err := scratch.resetQ5Dense(didCardinality)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	var aggregate aggregateMetadataPartSetStats
	for _, loaded := range reader.parts {
		visible := reader.visibleRowsForPart(loaded.Ordinal)
		if len(visible.Rows) == 0 {
			continue
		}
		if !visible.All {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: aggregate metadata q5 requires all-visible part %d", loaded.Part.Descriptor.PartID)
		}
		metadata, err := requireJSONBenchPostCreateDidTimeMetadata(loaded.Part, codes)
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		aggregate.add(metadata)
		for _, granule := range metadata.Granules {
			for _, entry := range granule.Entries {
				if entry.Group >= didCardinality {
					return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: aggregate metadata did code %d outside cardinality %d", entry.Group, didCardinality)
				}
				if bitsetTestAndSet(seen, entry.Group) {
					if entry.Min < minTime[entry.Group] {
						minTime[entry.Group] = entry.Min
					}
					if entry.Max > maxTime[entry.Group] {
						maxTime[entry.Group] = entry.Max
					}
					continue
				}
				minTime[entry.Group] = entry.Min
				maxTime[entry.Group] = entry.Max
				users = append(users, entry.Group)
			}
		}
	}
	scratch.q5Users = users
	top := scratch.q5Pairs[:0]
	for _, user := range users {
		top = insertQ5Top(top, jsonBenchPartSpanPair{user: int64(user), span: maxTime[user] - minTime[user]})
	}
	scratch.q5Pairs = top
	diagnostics := reader.partSetAggregateMetadataDiagnostics(jsonBenchPartQ5Columns, "multipart_aggregate_metadata_span_by_user", aggregate)
	return len(top), digestQ5Top(top), diagnostics, nil
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

func insertQ4TopUnique(top []jsonBenchPartTimePair, candidate jsonBenchPartTimePair) []jsonBenchPartTimePair {
	for i := range top {
		if top[i].user != candidate.user {
			continue
		}
		if !q4PairLess(candidate, top[i]) {
			return top
		}
		top[i] = candidate
		for i > 0 && q4PairLess(top[i], top[i-1]) {
			top[i], top[i-1] = top[i-1], top[i]
			i--
		}
		return top
	}
	return insertQ4Top(top, candidate)
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

func (r *ColumnPartSetReader) partSetKernelDiagnostics(columns []string, kernel string, rowsScanned int, granulesConsidered int, granulesSkipped int, decoded columnPartSetDecodedStats) JSONBenchPartQueryDiagnostics {
	return JSONBenchPartQueryDiagnostics{
		RowsScanned:        rowsScanned,
		RowsReturned:       r.visibilityStat.VisibleRows,
		RowsSuperseded:     r.visibilityStat.SupersededRows,
		RowsDeleted:        r.visibilityStat.DeletedRows,
		PartsConsidered:    len(r.parts),
		BaseParts:          r.visibilityStat.BaseParts,
		DeltaParts:         r.visibilityStat.DeltaParts,
		Tombstones:         r.visibilityStat.Tombstones,
		GranulesConsidered: granulesConsidered,
		GranulesSkipped:    granulesSkipped,
		GranulesDecoded:    decoded.GranulesDecoded,
		BlocksDecoded:      decoded.BlocksDecoded,
		BytesDecoded:       decoded.BytesDecoded,
		ColumnsProjected:   append([]string(nil), columns...),
		AggregateKernel:    kernel,
		PartSetCacheStats:  r.cacheStats,
	}
}

type aggregateMetadataPartSetStats struct {
	Granules      int
	RowsMatched   int
	Entries       int
	Bytes         int
	BuildDuration time.Duration
	Compression   string
}

func (s *aggregateMetadataPartSetStats) add(metadata AggregateMetadata) {
	s.Granules += metadata.Stats.Granules
	s.RowsMatched += metadata.Stats.RowsMatched
	s.Entries += metadata.Stats.Entries
	s.Bytes += metadata.Stats.TotalBytes
	s.BuildDuration += metadata.Stats.BuildDuration
	if s.Compression == "" {
		s.Compression = metadata.Stats.Compression
	} else if s.Compression != metadata.Stats.Compression {
		s.Compression = "mixed"
	}
}

func (r *ColumnPartSetReader) partSetAggregateMetadataDiagnostics(columns []string, kernel string, aggregate aggregateMetadataPartSetStats) JSONBenchPartQueryDiagnostics {
	bytesPerRow := 0.0
	if r.visibilityStat.InputRows > 0 {
		bytesPerRow = float64(aggregate.Bytes) / float64(r.visibilityStat.InputRows)
	}
	return JSONBenchPartQueryDiagnostics{
		RowsScanned:                    0,
		RowsReturned:                   r.visibilityStat.VisibleRows,
		RowsSuperseded:                 r.visibilityStat.SupersededRows,
		RowsDeleted:                    r.visibilityStat.DeletedRows,
		PartsConsidered:                len(r.parts),
		BaseParts:                      r.visibilityStat.BaseParts,
		DeltaParts:                     r.visibilityStat.DeltaParts,
		Tombstones:                     r.visibilityStat.Tombstones,
		GranulesConsidered:             aggregate.Granules,
		ColumnsProjected:               append([]string(nil), columns...),
		AggregateKernel:                kernel,
		SortKey:                        r.partSetSortKeyColumns(),
		AggregateMetadataUsed:          true,
		AggregateMetadataName:          jsonBenchPostCreateDidTimeMetadata,
		AggregateMetadataRows:          aggregate.RowsMatched,
		AggregateMetadataEntries:       aggregate.Entries,
		AggregateMetadataBytes:         aggregate.Bytes,
		AggregateMetadataBuildDuration: aggregate.BuildDuration,
		AggregateMetadataBytesPerRow:   bytesPerRow,
		AggregateMetadataCompression:   aggregate.Compression,
		PartSetCacheStats:              r.cacheStats,
	}
}

func (r *ColumnPartSetReader) partSetSortKeyColumns() []string {
	if r == nil {
		return nil
	}
	if len(r.manifest.SortKey) != 0 {
		out := make([]string, 0, len(r.manifest.SortKey))
		for _, column := range r.manifest.SortKey {
			out = append(out, column.Column)
		}
		return out
	}
	for _, loaded := range r.parts {
		if len(r.visibleRowsForPart(loaded.Ordinal).Rows) == 0 {
			continue
		}
		return sortKeyColumns(loaded.Part)
	}
	return nil
}

func (r *ColumnPartSetReader) canUseTimeOrderedPartSetQ4() bool {
	if r == nil {
		return false
	}
	for _, loaded := range r.parts {
		visible := r.visibleRowsForPart(loaded.Ordinal)
		if len(visible.Rows) == 0 {
			continue
		}
		if !visible.All || requireTimeUSAscendingSortKey(loaded.Part) != nil {
			return false
		}
	}
	return true
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
