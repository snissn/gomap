package typedcolumn

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

const AggregateMetadataDefinitionVersion uint16 = 1

type AggregateMetadataScope string

const (
	AggregateMetadataScopeGranule AggregateMetadataScope = "granule"
)

type AggregateMetadataMeasureOp string

const (
	AggregateMetadataMeasureCount AggregateMetadataMeasureOp = "count"
	AggregateMetadataMeasureMin   AggregateMetadataMeasureOp = "min"
	AggregateMetadataMeasureMax   AggregateMetadataMeasureOp = "max"
)

type AggregateMetadataPredicateOp string

const (
	AggregateMetadataPredicateEq AggregateMetadataPredicateOp = "eq"
)

type AggregateMetadataMeasure struct {
	Op     AggregateMetadataMeasureOp `json:"op"`
	Column string                     `json:"column,omitempty"`
}

type AggregateMetadataPredicate struct {
	Column string                       `json:"column"`
	Op     AggregateMetadataPredicateOp `json:"op"`
	Value  int64                        `json:"value"`
}

type AggregateMetadataDefinition struct {
	Name           string                       `json:"name"`
	Version        uint16                       `json:"version"`
	Kind           AggregateMetadataKind        `json:"kind"`
	Scope          AggregateMetadataScope       `json:"scope"`
	GroupKeys      []string                     `json:"group_keys"`
	Measures       []AggregateMetadataMeasure   `json:"measures"`
	Predicates     []AggregateMetadataPredicate `json:"predicates,omitempty"`
	MaxBytesPerRow float64                      `json:"max_bytes_per_row"`
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
	DescriptorBytes     int           `json:"estimated_descriptor_bytes"`
	TotalBytes          int           `json:"estimated_total_bytes"`
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
	if !ok {
		return AggregateMetadata{}, false
	}
	return cloneAggregateMetadata(metadata), true
}

func cloneAggregateMetadata(metadata AggregateMetadata) AggregateMetadata {
	metadata.Definition = cloneAggregateMetadataDefinition(metadata.Definition)
	if len(metadata.Granules) != 0 {
		granules := make([]AggregateMetadataGranule, len(metadata.Granules))
		for i, granule := range metadata.Granules {
			granules[i] = granule
			granules[i].Entries = append([]AggregateMetadataEntry(nil), granule.Entries...)
		}
		metadata.Granules = granules
	}
	return metadata
}

func normalizeAggregateMetadataDefinition(def AggregateMetadataDefinition, columns map[string]ColumnDefinition) (AggregateMetadataDefinition, error) {
	if def.Name == "" {
		return AggregateMetadataDefinition{}, errors.New("typedcolumn: aggregate metadata name is empty")
	}
	if def.Version == 0 {
		def.Version = AggregateMetadataDefinitionVersion
	}
	if def.Version != AggregateMetadataDefinitionVersion {
		return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: unsupported aggregate metadata %s version %d", def.Name, def.Version)
	}
	if def.Kind == "" {
		def.Kind = AggregateMetadataGroupMinMax
	}
	if def.Kind != AggregateMetadataGroupMinMax {
		return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: unsupported aggregate metadata kind %s", def.Kind)
	}
	if def.Scope == "" {
		def.Scope = AggregateMetadataScopeGranule
	}
	if def.Scope != AggregateMetadataScopeGranule {
		return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: unsupported aggregate metadata %s scope %s", def.Name, def.Scope)
	}
	if len(def.GroupKeys) != 1 {
		return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: aggregate metadata %s supports exactly one group key, got %d", def.Name, len(def.GroupKeys))
	}
	groupColumn := def.GroupKeys[0]
	if groupColumn == "" {
		return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: aggregate metadata %s has empty group key", def.Name)
	}
	groupDef, ok := columns[groupColumn]
	if !ok {
		return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: aggregate metadata %s group column %s is not declared", def.Name, groupColumn)
	}
	if groupDef.Type != ColumnTypeLowCardinalityCode {
		return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: aggregate metadata %s group column %s type=%s want %s", def.Name, groupColumn, groupDef.Type, ColumnTypeLowCardinalityCode)
	}

	valueColumn, err := aggregateMetadataGroupMinMaxValueColumn(def)
	if err != nil {
		return AggregateMetadataDefinition{}, err
	}
	valueDef, ok := columns[valueColumn]
	if !ok {
		return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: aggregate metadata %s value column %s is not declared", def.Name, valueColumn)
	}
	if valueDef.Type != ColumnTypeInt64 {
		return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: aggregate metadata %s value column %s type=%s want %s", def.Name, valueColumn, valueDef.Type, ColumnTypeInt64)
	}

	seenPredicates := make(map[string]struct{}, len(def.Predicates))
	for i, predicate := range def.Predicates {
		if predicate.Column == "" {
			return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: aggregate metadata %s predicate %d has empty column", def.Name, i)
		}
		if predicate.Op != AggregateMetadataPredicateEq {
			return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: aggregate metadata %s predicate %d op %s is unsupported", def.Name, i, predicate.Op)
		}
		predicateDef, ok := columns[predicate.Column]
		if !ok {
			return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: aggregate metadata %s predicate column %s is not declared", def.Name, predicate.Column)
		}
		if !isInt64SortCarrier(predicateDef.Type) {
			return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: aggregate metadata %s predicate column %s type=%s is not scalar/int64 predicate carrier", def.Name, predicate.Column, predicateDef.Type)
		}
		if _, ok := seenPredicates[predicate.Column]; ok {
			return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: aggregate metadata %s duplicate predicate column %s", def.Name, predicate.Column)
		}
		seenPredicates[predicate.Column] = struct{}{}
	}
	if math.IsNaN(def.MaxBytesPerRow) || math.IsInf(def.MaxBytesPerRow, 0) {
		return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: aggregate metadata %s max bytes per row %.3f is not finite", def.Name, def.MaxBytesPerRow)
	}
	if def.MaxBytesPerRow < 0 {
		return AggregateMetadataDefinition{}, fmt.Errorf("typedcolumn: aggregate metadata %s max bytes per row %.3f is negative", def.Name, def.MaxBytesPerRow)
	}
	def.GroupKeys = append([]string(nil), def.GroupKeys...)
	def.Measures = append([]AggregateMetadataMeasure(nil), def.Measures...)
	def.Predicates = append([]AggregateMetadataPredicate(nil), def.Predicates...)
	return def, nil
}

func aggregateMetadataGroupMinMaxValueColumn(def AggregateMetadataDefinition) (string, error) {
	if len(def.Measures) != 3 {
		return "", fmt.Errorf("typedcolumn: aggregate metadata %s supports exactly count/min/max measures, got %d", def.Name, len(def.Measures))
	}
	var hasCount bool
	var minColumn string
	var maxColumn string
	for i, measure := range def.Measures {
		switch measure.Op {
		case AggregateMetadataMeasureCount:
			if hasCount {
				return "", fmt.Errorf("typedcolumn: aggregate metadata %s has duplicate count measure", def.Name)
			}
			if measure.Column != "" {
				return "", fmt.Errorf("typedcolumn: aggregate metadata %s count measure must not bind column %s", def.Name, measure.Column)
			}
			hasCount = true
		case AggregateMetadataMeasureMin:
			if minColumn != "" {
				return "", fmt.Errorf("typedcolumn: aggregate metadata %s has duplicate min measure", def.Name)
			}
			minColumn = measure.Column
		case AggregateMetadataMeasureMax:
			if maxColumn != "" {
				return "", fmt.Errorf("typedcolumn: aggregate metadata %s has duplicate max measure", def.Name)
			}
			maxColumn = measure.Column
		default:
			return "", fmt.Errorf("typedcolumn: aggregate metadata %s measure %d op %s is unsupported", def.Name, i, measure.Op)
		}
	}
	if !hasCount || minColumn == "" || maxColumn == "" {
		return "", fmt.Errorf("typedcolumn: aggregate metadata %s requires count, min, and max measures", def.Name)
	}
	if minColumn != maxColumn {
		return "", fmt.Errorf("typedcolumn: aggregate metadata %s min column %s differs from max column %s", def.Name, minColumn, maxColumn)
	}
	return minColumn, nil
}

func (b *ColumnPartBuilder) buildAggregateMetadata(part *ColumnPart, batch Batch) error {
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

func (b *ColumnPartBuilder) buildGroupMinMaxMetadata(part *ColumnPart, batch Batch, def AggregateMetadataDefinition) (AggregateMetadata, error) {
	start := time.Now()
	groupColumn := def.GroupKeys[0]
	valueColumn, err := aggregateMetadataGroupMinMaxValueColumn(def)
	if err != nil {
		return AggregateMetadata{}, err
	}
	groupValues := batch.Columns[groupColumn]
	valueValues := batch.Columns[valueColumn]
	predicateValues := make([][]int64, len(def.Predicates))
	for i, predicate := range def.Predicates {
		predicateValues[i] = batch.Columns[predicate.Column]
	}

	metadata := AggregateMetadata{
		Definition: cloneAggregateMetadataDefinition(def),
		Granules:   make([]AggregateMetadataGranule, 0, len(part.Descriptor.Granules)),
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
			if !aggregateMetadataPredicatesMatch(predicateValues, def.Predicates, sourceRow) {
				continue
			}
			group64 := groupValues[sourceRow]
			if group64 < 0 || group64 > math.MaxUint32 {
				return AggregateMetadata{}, fmt.Errorf("typedcolumn: aggregate metadata %s group value %d outside uint32", def.Name, group64)
			}
			group := uint32(group64)
			value := valueValues[sourceRow]
			outGranule.MatchedRows++
			if index, ok := groupIndex[group]; ok {
				entry := &outGranule.Entries[index]
				if entry.Count == math.MaxUint32 {
					return AggregateMetadata{}, fmt.Errorf("typedcolumn: aggregate metadata %s granule %d group %d count exceeds uint32", def.Name, granule.Ordinal, group)
				}
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
				return AggregateMetadata{}, fmt.Errorf("typedcolumn: aggregate metadata %s granule %d matched rows exceed uint32", def.Name, granule.Ordinal)
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
	metadata.Stats.DescriptorBytes = len(part.Descriptor.Granules)*32 + len(def.Predicates)*24 + len(def.Measures)*16 + len(def.GroupKeys)*16 + 72
	metadata.Stats.TotalBytes = metadata.Stats.ValueBytes + metadata.Stats.DescriptorBytes
	metadata.Stats.Compression = "none_prototype"
	metadata.Stats.AdmissionMaxBytes = def.MaxBytesPerRow
	metadata.Stats.AdmissionMeasuredBy = "estimated_total_metadata_bytes / part_rows"
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

func cloneAggregateMetadataDefinition(def AggregateMetadataDefinition) AggregateMetadataDefinition {
	def.GroupKeys = append([]string(nil), def.GroupKeys...)
	def.Measures = append([]AggregateMetadataMeasure(nil), def.Measures...)
	def.Predicates = append([]AggregateMetadataPredicate(nil), def.Predicates...)
	return def
}

func aggregateMetadataPredicatesMatch(predicateValues [][]int64, predicates []AggregateMetadataPredicate, row int) bool {
	for i, predicate := range predicates {
		if predicateValues[i][row] != predicate.Value {
			return false
		}
	}
	return true
}
