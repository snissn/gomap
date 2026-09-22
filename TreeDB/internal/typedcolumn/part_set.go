package typedcolumn

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

// PartRole records whether a typed-column part participates as a base or delta
// in a logical part set. Publication/control-plane ownership is intentionally
// outside this package; callers provide already-open non-authoritative parts.
type PartRole string

const (
	PartRoleBase  PartRole = "base"
	PartRoleDelta PartRole = "delta"
)

// PartRef is the data-plane identity needed to evaluate latest-visible rows
// across base and delta parts. It deliberately excludes collection manifest,
// WAL, recovery, and asset publication references.
type PartRef struct {
	Role          PartRole
	GenerationID  uint64
	Part          *ColumnPart
	PrimaryIDMode QueryReadyPrimaryIDMode
	PrimaryIDBase int64
}

// Tombstone removes a primary ID at or below the tombstone generation.
type Tombstone struct {
	PrimaryID    int64
	GenerationID uint64
}

type PartSetReader struct {
	parts          []partSetLoadedPart
	latest         map[int64]partSetRowRef
	visibleRows    map[int]map[int]struct{}
	visibleRowList []partSetVisibleRows
	tombstoneByID  map[int64]uint64
	visibilityStat PartSetVisibilityStats
}

type partSetLoadedPart struct {
	Ref     PartRef
	Part    *ColumnPart
	Ordinal int
}

type partSetRowRef struct {
	PrimaryID    int64
	PartIndex    int
	PartRow      int
	GenerationID uint64
	Ordinal      int
	Locator      RowLocator
}

type partSetVisibleRows struct {
	Rows []int
	All  bool
}

type PartSetVisibilityStats struct {
	Parts          int `json:"parts"`
	BaseParts      int `json:"base_parts"`
	DeltaParts     int `json:"delta_parts"`
	InputRows      int `json:"input_rows"`
	VisibleRows    int `json:"visible_rows"`
	SupersededRows int `json:"superseded_rows"`
	DeletedRows    int `json:"deleted_rows"`
	Tombstones     int `json:"tombstones"`
}

// NewPartSetReader builds a non-authoritative latest-visible row index over
// caller-owned parts. The algorithm is copied/adapted from experiments/colgranule
// ColumnPartSetReader.buildVisibility without workspace or collection-manifest
// publication plumbing.
func NewPartSetReader(refs []PartRef, tombstones []Tombstone) (*PartSetReader, error) {
	reader := &PartSetReader{
		latest:        make(map[int64]partSetRowRef),
		visibleRows:   make(map[int]map[int]struct{}),
		tombstoneByID: make(map[int64]uint64, len(tombstones)),
	}
	for i, ref := range refs {
		if ref.Part == nil {
			return nil, fmt.Errorf("typedcolumn: nil part set part at index %d", i)
		}
		switch ref.Role {
		case PartRoleBase, PartRoleDelta:
		case "":
			return nil, fmt.Errorf("typedcolumn: empty part set role at index %d", i)
		default:
			return nil, fmt.Errorf("typedcolumn: unsupported part set role %q", ref.Role)
		}
		if err := validateQueryReadyPrimaryIDMetadata(ref.PrimaryIDMode, ref.PrimaryIDBase, int64(ref.Part.Descriptor.RowCount)); err != nil {
			return nil, fmt.Errorf("typedcolumn: part set part[%d] primary IDs: %w", i, err)
		}
		reader.parts = append(reader.parts, partSetLoadedPart{
			Ref:     ref,
			Part:    ref.Part,
			Ordinal: len(reader.parts),
		})
	}
	if err := reader.buildVisibility(tombstones); err != nil {
		return nil, err
	}
	return reader, nil
}

// newDenseDisjointScanPartSetReader prepares the production insert-only scan
// shape without decoding row locators or constructing an O(rows) latest-row
// map. Every part must declare a non-overlapping dense part-local logical-ID
// range, be a base part, and have no tombstones. The returned reader is for
// whole-part encoded scans only; point lookup continues to use NewPartSetReader.
func newDenseDisjointScanPartSetReader(refs []PartRef) (*PartSetReader, error) {
	reader := &PartSetReader{
		latest:         make(map[int64]partSetRowRef),
		visibleRows:    make(map[int]map[int]struct{}),
		tombstoneByID:  make(map[int64]uint64),
		visibleRowList: make([]partSetVisibleRows, len(refs)),
	}
	type logicalRange struct {
		base  int64
		limit int64
		part  int
	}
	ranges := make([]logicalRange, 0, len(refs))
	stats := PartSetVisibilityStats{Parts: len(refs), BaseParts: len(refs)}
	for i, ref := range refs {
		if ref.Part == nil {
			return nil, fmt.Errorf("typedcolumn: nil dense scan part at index %d", i)
		}
		if ref.Role != PartRoleBase || ref.PrimaryIDMode != QueryReadyPrimaryIDDensePartLocal {
			return nil, fmt.Errorf("typedcolumn: dense scan part[%d] role/mode=%q/%d want base/dense-part-local", i, ref.Role, ref.PrimaryIDMode)
		}
		rows := ref.Part.Descriptor.RowCount
		if err := validateQueryReadyPrimaryIDMetadata(ref.PrimaryIDMode, ref.PrimaryIDBase, int64(rows)); err != nil {
			return nil, fmt.Errorf("typedcolumn: dense scan part[%d] primary IDs: %w", i, err)
		}
		reader.parts = append(reader.parts, partSetLoadedPart{Ref: ref, Part: ref.Part, Ordinal: i})
		reader.visibleRowList[i] = partSetVisibleRows{All: true}
		stats.InputRows += rows
		stats.VisibleRows += rows
		ranges = append(ranges, logicalRange{base: ref.PrimaryIDBase, limit: ref.PrimaryIDBase + int64(rows), part: i})
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].base != ranges[j].base {
			return ranges[i].base < ranges[j].base
		}
		return ranges[i].part < ranges[j].part
	})
	for i := 1; i < len(ranges); i++ {
		if ranges[i].base < ranges[i-1].limit {
			return nil, fmt.Errorf("typedcolumn: dense scan logical ranges overlap for parts %d and %d", ranges[i-1].part, ranges[i].part)
		}
	}
	reader.visibilityStat = stats
	return reader, nil
}

func (r *PartSetReader) VisibilityStats() PartSetVisibilityStats {
	if r == nil {
		return PartSetVisibilityStats{}
	}
	return r.visibilityStat
}

func (r *PartSetReader) LatestLocator(primaryID int64) (RowLocator, bool) {
	if r == nil {
		return RowLocator{}, false
	}
	ref, ok := r.latest[primaryID]
	if !ok {
		return RowLocator{}, false
	}
	return ref.Locator, true
}

// ScanLatestLocator recomputes latest-visible selection from the caller-owned
// parts instead of reading the cached index. It is retained as a data-plane
// cross-check and adapter seam for future lazy/mappedresource-backed readers;
// immutable in-memory parts should match LatestLocator.
func (r *PartSetReader) ScanLatestLocator(primaryID int64) (RowLocator, bool) {
	ref, ok := r.scanLatestRowRef(primaryID)
	if !ok {
		return RowLocator{}, false
	}
	return ref.Locator, true
}

func (r *PartSetReader) ValueAtLatest(primaryID int64, columnName string) (int64, bool, error) {
	if r == nil {
		return 0, false, nil
	}
	ref, ok := r.latest[primaryID]
	if !ok {
		return 0, false, nil
	}
	value, err := r.valueAtRowRef(ref, columnName)
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
}

// NullableInt64AtLatest returns the physical nullable/default state from the
// latest-visible row without constructing a cross-part value domain.
func (r *PartSetReader) NullableInt64AtLatest(primaryID int64, columnName string) (value int64, null, defaulted, ok bool, err error) {
	if r == nil {
		return 0, false, false, false, nil
	}
	ref, ok := r.latest[primaryID]
	if !ok {
		return 0, false, false, false, nil
	}
	if ref.PartIndex < 0 || ref.PartIndex >= len(r.parts) {
		return 0, false, false, false, fmt.Errorf("typedcolumn: row ref part index %d outside %d parts", ref.PartIndex, len(r.parts))
	}
	part := r.parts[ref.PartIndex].Part
	column, exists := part.Columns[columnName]
	if !exists {
		return 0, false, false, false, fmt.Errorf("typedcolumn: missing column %s", columnName)
	}
	if column.Definition.Encoding != EncodingNullableInt64 {
		return 0, false, false, false, fmt.Errorf("typedcolumn: column %s encoding=%s want %s", columnName, column.Definition.Encoding, EncodingNullableInt64)
	}
	var reader GranuleReader
	for _, block := range column.Blocks {
		if ref.PartRow < block.Descriptor.FirstRow || ref.PartRow >= block.Descriptor.FirstRow+block.Descriptor.RowCount {
			continue
		}
		values, nulls, defaults, decodeErr := reader.DecodeNullableInt64(block.Granule)
		if decodeErr != nil {
			return 0, false, false, false, decodeErr
		}
		row := ref.PartRow - block.Descriptor.FirstRow
		return values[row], nulls[row], defaults[row], true, nil
	}
	return 0, false, false, false, fmt.Errorf("typedcolumn: locator row %d outside column %s", ref.PartRow, columnName)
}

func (r *PartSetReader) VisibleRowsForPart(partIndex int) ([]int, bool) {
	visible := r.visibleRowsForPart(partIndex)
	return append([]int(nil), visible.Rows...), visible.All
}

func (r *PartSetReader) buildVisibility(tombstones []Tombstone) error {
	for _, tombstone := range tombstones {
		if prev, ok := r.tombstoneByID[tombstone.PrimaryID]; !ok || tombstone.GenerationID > prev {
			r.tombstoneByID[tombstone.PrimaryID] = tombstone.GenerationID
		}
	}
	stats := PartSetVisibilityStats{
		Parts:      len(r.parts),
		Tombstones: len(tombstones),
	}
	for partIndex, loaded := range r.parts {
		switch loaded.Ref.Role {
		case PartRoleBase:
			stats.BaseParts++
		case PartRoleDelta:
			stats.DeltaParts++
		}
		stats.InputRows += loaded.Part.Descriptor.RowCount
		if err := validateDecodedRowLocators(loaded.Part.Descriptor, loaded.Part.Descriptor.PartID, loaded.Part.Locators); err != nil {
			return fmt.Errorf("typedcolumn: part set part %d locators: %w", loaded.Part.Descriptor.PartID, err)
		}
		for encodedPrimaryID, locator := range loaded.Part.Locators {
			if locator.PartID != loaded.Part.Descriptor.PartID {
				return fmt.Errorf("typedcolumn: part set locator part=%d want %d", locator.PartID, loaded.Part.Descriptor.PartID)
			}
			if locator.PartRow < 0 || locator.PartRow >= loaded.Part.Descriptor.RowCount {
				return fmt.Errorf("typedcolumn: part set locator row=%d outside part %d rows=%d", locator.PartRow, loaded.Part.Descriptor.PartID, loaded.Part.Descriptor.RowCount)
			}
			primaryID, err := loaded.logicalPrimaryID(encodedPrimaryID)
			if err != nil {
				return fmt.Errorf("typedcolumn: part set part %d primary ID %d: %w", loaded.Part.Descriptor.PartID, encodedPrimaryID, err)
			}
			row := partSetRowRef{
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
	r.visibleRowList = make([]partSetVisibleRows, len(r.parts))
	for partIndex, rows := range r.visibleRows {
		list := make([]int, 0, len(rows))
		for row := range rows {
			list = append(list, row)
		}
		sort.Ints(list)
		r.visibleRowList[partIndex] = partSetVisibleRows{
			Rows: list,
			All:  len(list) == r.parts[partIndex].Part.Descriptor.RowCount,
		}
	}
	stats.VisibleRows = len(r.latest)
	r.visibilityStat = stats
	return nil
}

func (r *PartSetReader) visibleRowsForPart(partIndex int) partSetVisibleRows {
	if r == nil || partIndex < 0 || partIndex >= len(r.visibleRowList) {
		return partSetVisibleRows{}
	}
	return r.visibleRowList[partIndex]
}

func (r *PartSetReader) scanLatestRowRef(primaryID int64) (partSetRowRef, bool) {
	if r == nil {
		return partSetRowRef{}, false
	}
	var best partSetRowRef
	var found bool
	tombstoneGeneration, tombstoned := r.tombstoneByID[primaryID]
	for partIndex, loaded := range r.parts {
		encodedPrimaryID, ok := loaded.encodedPrimaryID(primaryID)
		if !ok {
			continue
		}
		locator, ok := loaded.Part.LocatePrimaryID(encodedPrimaryID)
		if !ok {
			continue
		}
		row := partSetRowRef{
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

func (p partSetLoadedPart) logicalPrimaryID(encoded int64) (int64, error) {
	switch p.Ref.PrimaryIDMode {
	case QueryReadyPrimaryIDPreserve:
		return encoded, nil
	case QueryReadyPrimaryIDDensePartLocal:
		if encoded < 0 || encoded >= int64(p.Part.Descriptor.RowCount) {
			return 0, fmt.Errorf("dense part-local ID outside [0,%d)", p.Part.Descriptor.RowCount)
		}
		if p.Ref.PrimaryIDBase > math.MaxInt64-encoded {
			return 0, errors.New("dense part-local ID translation overflows int64")
		}
		return p.Ref.PrimaryIDBase + encoded, nil
	default:
		return 0, fmt.Errorf("unsupported primary ID mode %d", p.Ref.PrimaryIDMode)
	}
}

func (p partSetLoadedPart) encodedPrimaryID(logical int64) (int64, bool) {
	switch p.Ref.PrimaryIDMode {
	case QueryReadyPrimaryIDPreserve:
		return logical, true
	case QueryReadyPrimaryIDDensePartLocal:
		if logical < p.Ref.PrimaryIDBase {
			return 0, false
		}
		encoded := logical - p.Ref.PrimaryIDBase
		return encoded, encoded >= 0 && encoded < int64(p.Part.Descriptor.RowCount)
	default:
		return 0, false
	}
}

func (r *PartSetReader) valueAtRowRef(ref partSetRowRef, columnName string) (int64, error) {
	if r == nil {
		return 0, fmt.Errorf("typedcolumn: nil part set reader")
	}
	if ref.PartIndex < 0 || ref.PartIndex >= len(r.parts) {
		return 0, fmt.Errorf("typedcolumn: row ref part index %d outside %d parts", ref.PartIndex, len(r.parts))
	}
	scanner := ColumnPartScanner{part: r.parts[ref.PartIndex].Part}
	return scanner.ValueAt(ref.Locator, columnName)
}

func (r partSetRowRef) newerThan(other partSetRowRef) bool {
	if r.GenerationID != other.GenerationID {
		return r.GenerationID > other.GenerationID
	}
	return r.Ordinal > other.Ordinal
}
