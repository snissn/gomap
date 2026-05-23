package typedcolumn

import (
	"fmt"
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
	Role         PartRole
	GenerationID uint64
	Part         *ColumnPart
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
		for primaryID, locator := range loaded.Part.Locators {
			if locator.PartID != loaded.Part.Descriptor.PartID {
				return fmt.Errorf("typedcolumn: part set locator part=%d want %d", locator.PartID, loaded.Part.Descriptor.PartID)
			}
			if locator.PartRow < 0 || locator.PartRow >= loaded.Part.Descriptor.RowCount {
				return fmt.Errorf("typedcolumn: part set locator row=%d outside part %d rows=%d", locator.PartRow, loaded.Part.Descriptor.PartID, loaded.Part.Descriptor.RowCount)
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
		locator, ok := loaded.Part.LocatePrimaryID(primaryID)
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
