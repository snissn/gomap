package colgranule

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

const aggregateMetadataGroupMinMaxEntryBytes = 24

type AggregateMetadataKind string

const (
	AggregateMetadataGroupMinMax AggregateMetadataKind = "group_min_max"
)

type AggregateMetadataFilter struct {
	Column string `json:"column"`
	Equals int64  `json:"equals"`
}

type AggregateMetadataDefinition struct {
	Name           string                    `json:"name"`
	Kind           AggregateMetadataKind     `json:"kind"`
	GroupColumn    string                    `json:"group_column"`
	ValueColumn    string                    `json:"value_column"`
	Filters        []AggregateMetadataFilter `json:"filters"`
	MaxBytesPerRow float64                   `json:"max_bytes_per_row"`
}

type AggregateMetadata struct {
	Definition AggregateMetadataDefinition `json:"definition"`
	Granules   []AggregateMetadataGranule  `json:"granules,omitempty"`
	Stats      AggregateMetadataStats      `json:"stats"`
}

type AggregateMetadataStats struct {
	Admitted            bool          `json:"admitted"`
	RejectedReason      string        `json:"rejected_reason,omitempty"`
	BuildDuration       time.Duration `json:"build_duration"`
	Granules            int           `json:"granules"`
	GranulesWithRows    int           `json:"granules_with_rows"`
	RowsMatched         int           `json:"rows_matched"`
	Entries             int           `json:"entries"`
	ValueBytes          int           `json:"value_bytes"`
	DescriptorBytes     int           `json:"descriptor_bytes"`
	TotalBytes          int           `json:"total_bytes"`
	BytesPerPartRow     float64       `json:"bytes_per_part_row"`
	BytesPerMatchedRow  float64       `json:"bytes_per_matched_row"`
	Compression         string        `json:"compression"`
	AdmissionMaxBytes   float64       `json:"admission_max_bytes_per_row"`
	AdmissionMeasuredBy string        `json:"admission_measured_by"`
}

type AggregateMetadataGranule struct {
	GranuleOrdinal int                      `json:"granule_ordinal"`
	FirstRow       int                      `json:"first_row"`
	RowCount       int                      `json:"row_count"`
	MatchedRows    int                      `json:"matched_rows"`
	Entries        []AggregateMetadataEntry `json:"entries,omitempty"`
}

type AggregateMetadataEntry struct {
	Group uint32 `json:"group"`
	Count uint32 `json:"count"`
	Min   int64  `json:"min"`
	Max   int64  `json:"max"`
}

func (p *ColumnPart) AggregateMetadataByName(name string) (AggregateMetadata, bool) {
	if p == nil || p.AggregateMetadata == nil {
		return AggregateMetadata{}, false
	}
	metadata, ok := p.AggregateMetadata[name]
	return metadata, ok
}

func normalizeAggregateMetadataDefinition(def AggregateMetadataDefinition, columns map[string]ColumnDefinition) (AggregateMetadataDefinition, error) {
	if def.Name == "" {
		return AggregateMetadataDefinition{}, errors.New("colgranule: aggregate metadata name is empty")
	}
	if def.Kind == "" {
		def.Kind = AggregateMetadataGroupMinMax
	}
	if def.Kind != AggregateMetadataGroupMinMax {
		return AggregateMetadataDefinition{}, fmt.Errorf("colgranule: unsupported aggregate metadata kind %s", def.Kind)
	}
	groupDef, ok := columns[def.GroupColumn]
	if !ok {
		return AggregateMetadataDefinition{}, fmt.Errorf("colgranule: aggregate metadata %s group column %s is not declared", def.Name, def.GroupColumn)
	}
	if groupDef.Type != ColumnTypeLowCardinalityCode {
		return AggregateMetadataDefinition{}, fmt.Errorf("colgranule: aggregate metadata %s group column %s type=%s want %s", def.Name, def.GroupColumn, groupDef.Type, ColumnTypeLowCardinalityCode)
	}
	valueDef, ok := columns[def.ValueColumn]
	if !ok {
		return AggregateMetadataDefinition{}, fmt.Errorf("colgranule: aggregate metadata %s value column %s is not declared", def.Name, def.ValueColumn)
	}
	if valueDef.Type != ColumnTypeInt64 {
		return AggregateMetadataDefinition{}, fmt.Errorf("colgranule: aggregate metadata %s value column %s type=%s want %s", def.Name, def.ValueColumn, valueDef.Type, ColumnTypeInt64)
	}
	seenFilters := make(map[string]struct{}, len(def.Filters))
	for i, filter := range def.Filters {
		if filter.Column == "" {
			return AggregateMetadataDefinition{}, fmt.Errorf("colgranule: aggregate metadata %s filter %d has empty column", def.Name, i)
		}
		if _, ok := columns[filter.Column]; !ok {
			return AggregateMetadataDefinition{}, fmt.Errorf("colgranule: aggregate metadata %s filter column %s is not declared", def.Name, filter.Column)
		}
		if _, ok := seenFilters[filter.Column]; ok {
			return AggregateMetadataDefinition{}, fmt.Errorf("colgranule: aggregate metadata %s duplicate filter column %s", def.Name, filter.Column)
		}
		seenFilters[filter.Column] = struct{}{}
	}
	if def.MaxBytesPerRow < 0 {
		return AggregateMetadataDefinition{}, fmt.Errorf("colgranule: aggregate metadata %s max bytes per row %.3f is negative", def.Name, def.MaxBytesPerRow)
	}
	def.Filters = append([]AggregateMetadataFilter(nil), def.Filters...)
	return def, nil
}

func (b *ColumnPartBuilder) buildAggregateMetadata(part *ColumnPart, batch ColumnBatch) error {
	if len(b.opts.AggregateMetadata) == 0 {
		return nil
	}
	part.AggregateMetadata = make(map[string]AggregateMetadata, len(b.opts.AggregateMetadata))
	for _, def := range b.opts.AggregateMetadata {
		metadata, err := b.buildGroupMinMaxMetadata(part, batch, def)
		if err != nil {
			return err
		}
		part.AggregateMetadata[def.Name] = metadata
	}
	return nil
}

func (b *ColumnPartBuilder) buildGroupMinMaxMetadata(part *ColumnPart, batch ColumnBatch, def AggregateMetadataDefinition) (AggregateMetadata, error) {
	start := time.Now()
	groupValues := batch.Columns[def.GroupColumn]
	valueValues := batch.Columns[def.ValueColumn]
	filterValues := make([][]int64, len(def.Filters))
	for i, filter := range def.Filters {
		filterValues[i] = batch.Columns[filter.Column]
	}

	metadata := AggregateMetadata{
		Definition: AggregateMetadataDefinition{
			Name:           def.Name,
			Kind:           def.Kind,
			GroupColumn:    def.GroupColumn,
			ValueColumn:    def.ValueColumn,
			Filters:        append([]AggregateMetadataFilter(nil), def.Filters...),
			MaxBytesPerRow: def.MaxBytesPerRow,
		},
		Granules: make([]AggregateMetadataGranule, 0, len(part.Descriptor.Granules)),
	}
	groupIndex := make(map[uint32]int)
	for _, granule := range part.Descriptor.Granules {
		clear(groupIndex)
		outGranule := AggregateMetadataGranule{
			GranuleOrdinal: granule.Ordinal,
			FirstRow:       granule.FirstRow,
			RowCount:       granule.RowCount,
		}
		for partRow := granule.FirstRow; partRow < granule.FirstRow+granule.RowCount; partRow++ {
			sourceRow := b.order[partRow]
			if !aggregateMetadataFiltersMatch(filterValues, def.Filters, sourceRow) {
				continue
			}
			group64 := groupValues[sourceRow]
			if group64 < 0 || group64 > math.MaxUint32 {
				return AggregateMetadata{}, fmt.Errorf("colgranule: aggregate metadata %s group value %d outside uint32", def.Name, group64)
			}
			group := uint32(group64)
			value := valueValues[sourceRow]
			outGranule.MatchedRows++
			if index, ok := groupIndex[group]; ok {
				entry := &outGranule.Entries[index]
				entry.Count++
				if value < entry.Min {
					entry.Min = value
				}
				if value > entry.Max {
					entry.Max = value
				}
				continue
			}
			if outGranule.MatchedRows > math.MaxUint32 {
				return AggregateMetadata{}, fmt.Errorf("colgranule: aggregate metadata %s granule %d matched rows exceed uint32", def.Name, granule.Ordinal)
			}
			groupIndex[group] = len(outGranule.Entries)
			outGranule.Entries = append(outGranule.Entries, AggregateMetadataEntry{
				Group: group,
				Count: 1,
				Min:   value,
				Max:   value,
			})
		}
		sort.Slice(outGranule.Entries, func(i, j int) bool {
			return outGranule.Entries[i].Group < outGranule.Entries[j].Group
		})
		if outGranule.MatchedRows > 0 {
			metadata.Stats.GranulesWithRows++
		}
		metadata.Stats.RowsMatched += outGranule.MatchedRows
		metadata.Stats.Entries += len(outGranule.Entries)
		metadata.Granules = append(metadata.Granules, outGranule)
	}
	metadata.Stats.BuildDuration = time.Since(start)
	metadata.Stats.Granules = len(part.Descriptor.Granules)
	metadata.Stats.ValueBytes = metadata.Stats.Entries * aggregateMetadataGroupMinMaxEntryBytes
	metadata.Stats.DescriptorBytes = len(part.Descriptor.Granules)*32 + len(def.Filters)*16 + 64
	metadata.Stats.TotalBytes = metadata.Stats.ValueBytes + metadata.Stats.DescriptorBytes
	metadata.Stats.Compression = "none_prototype"
	metadata.Stats.AdmissionMaxBytes = def.MaxBytesPerRow
	metadata.Stats.AdmissionMeasuredBy = "total_metadata_bytes / part_rows"
	if part.Descriptor.RowCount > 0 {
		metadata.Stats.BytesPerPartRow = float64(metadata.Stats.TotalBytes) / float64(part.Descriptor.RowCount)
	}
	if metadata.Stats.RowsMatched > 0 {
		metadata.Stats.BytesPerMatchedRow = float64(metadata.Stats.TotalBytes) / float64(metadata.Stats.RowsMatched)
	}
	metadata.Stats.Admitted = true
	if def.MaxBytesPerRow > 0 && metadata.Stats.BytesPerPartRow > def.MaxBytesPerRow {
		metadata.Stats.Admitted = false
		metadata.Stats.RejectedReason = fmt.Sprintf("bytes_per_part_row %.6f exceeds max %.6f", metadata.Stats.BytesPerPartRow, def.MaxBytesPerRow)
		metadata.Granules = nil
	}
	return metadata, nil
}

func aggregateMetadataFiltersMatch(filterValues [][]int64, filters []AggregateMetadataFilter, row int) bool {
	for i, filter := range filters {
		if filterValues[i][row] != filter.Equals {
			return false
		}
	}
	return true
}
