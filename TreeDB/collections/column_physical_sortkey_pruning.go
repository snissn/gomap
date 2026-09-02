package collections

import (
	"fmt"
	"strconv"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

const (
	columnSortKeyMarkFallbackNone                          = "none"
	columnSortKeyMarkFallbackMissingSortKey                = "missing_sort_key"
	columnSortKeyMarkFallbackNoPrefixPredicate             = "no_sorted_prefix_predicate"
	columnSortKeyMarkFallbackUnsupportedPredicate          = "unsupported_prefix_predicate"
	columnSortKeyMarkFallbackUnsupportedDescending         = "unsupported_descending_sort_key"
	columnSortKeyMarkFallbackNullableDefaulted             = "nullable_or_defaulted_sort_key"
	columnSortKeyMarkFallbackUnsupportedColumnType         = "unsupported_sort_key_column_type"
	columnSortKeyMarkFallbackUnsupportedSortKeyWidth       = "unsupported_sort_key_width"
	columnSortKeyMarkFallbackMissingMarks                  = "missing_sort_key_marks"
	columnSortKeyMarkFallbackStaleMarks                    = "stale_sort_key_marks"
	columnSortKeyMarkFallbackUncertifiedDictionaryOrdering = "uncertified_dictionary_order"
	columnSortKeyMarkFallbackLiteralAbsent                 = "prefix_literal_absent_from_part_dictionary"
	columnSortedGroupedDistinctFallbackNone                = "none"
	columnSortedGroupedDistinctFallbackNotQ2               = "not_group_count_and_distinct"
	columnSortedGroupedDistinctFallbackMissingPrefix       = "missing_sorted_prefix_plan"
	columnSortedGroupedDistinctFallbackSortKeyLayout       = "sort_key_layout_not_group_distinct"
)

type columnTypedColumnSortKeyPrefixPlan struct {
	Planned                             bool
	PrefixLen                           int
	Columns                             [typedColumnPartSortKeyMaxColumns]string
	Values                              [typedColumnPartSortKeyMaxColumns]string
	FallbackReason                      string
	SortedGroupedDistinctReady          bool
	SortedGroupedDistinctFallbackReason string
}

func (plan *columnTypedColumnSortKeyPrefixPlan) prefixColumns() []string {
	if plan == nil || plan.PrefixLen == 0 {
		return nil
	}
	return plan.Columns[:plan.PrefixLen]
}

type columnTypedColumnSortKeyPartPruneResult struct {
	Rows            []int
	AllRows         bool
	Considered      int
	DecodedGranules int
	Checks          int
	Matches         int
	Skips           int
	FallbackReason  string
}

func planColumnTypedColumnSortKeyPrefix(cfg ColumnStoreConfig, sortKey []ColumnSortKey, req ColumnPhysicalQueryRequest) columnTypedColumnSortKeyPrefixPlan {
	plan := columnTypedColumnSortKeyPrefixPlan{FallbackReason: columnSortKeyMarkFallbackNone}
	if len(sortKey) == 0 {
		plan.FallbackReason = columnSortKeyMarkFallbackMissingSortKey
		plan.SortedGroupedDistinctFallbackReason = columnSortedGroupedDistinctFallbackMissingPrefix
		return plan
	}
	for _, sortColumn := range sortKey {
		if sortColumn.Direction != "" && sortColumn.Direction != ColumnSortAscending {
			plan.FallbackReason = columnSortKeyMarkFallbackUnsupportedDescending
			plan.SortedGroupedDistinctFallbackReason = columnSortedGroupedDistinctFallbackMissingPrefix
			return plan
		}
		declared, ok := columnStoreColumnByNameForSortKeyPrefix(cfg, sortColumn.Column)
		if !ok {
			plan.FallbackReason = columnSortKeyMarkFallbackMissingSortKey
			plan.SortedGroupedDistinctFallbackReason = columnSortedGroupedDistinctFallbackMissingPrefix
			return plan
		}
		if declared.Nullable {
			plan.FallbackReason = columnSortKeyMarkFallbackNullableDefaulted
			plan.SortedGroupedDistinctFallbackReason = columnSortedGroupedDistinctFallbackMissingPrefix
			return plan
		}
		if !columnStoreValueTypeSupportsTypedColumnPartSort(declared.ValueType) {
			plan.FallbackReason = columnSortKeyMarkFallbackUnsupportedColumnType
			plan.SortedGroupedDistinctFallbackReason = columnSortedGroupedDistinctFallbackMissingPrefix
			return plan
		}
		predicate, ok := columnPhysicalQueryPredicateForSortKeyPrefix(req, sortColumn.Column)
		if !ok {
			break
		}
		kind := columnPhysicalQueryPredicateKindOrDefault(predicate.Kind)
		if kind != ColumnPhysicalQueryPredicateEqual || len(predicate.Values) != 0 {
			if plan.PrefixLen == 0 {
				plan.FallbackReason = columnSortKeyMarkFallbackUnsupportedPredicate
			}
			break
		}
		if plan.PrefixLen >= typedColumnPartSortKeyMaxColumns {
			plan.FallbackReason = columnSortKeyMarkFallbackUnsupportedSortKeyWidth
			plan.SortedGroupedDistinctFallbackReason = columnSortedGroupedDistinctFallbackMissingPrefix
			return plan
		}
		plan.Columns[plan.PrefixLen] = sortColumn.Column
		plan.Values[plan.PrefixLen] = predicate.Value
		plan.PrefixLen++
	}
	if plan.PrefixLen == 0 {
		if plan.FallbackReason == columnSortKeyMarkFallbackNone {
			plan.FallbackReason = columnSortKeyMarkFallbackNoPrefixPredicate
		}
		plan.SortedGroupedDistinctFallbackReason = columnSortedGroupedDistinctFallbackMissingPrefix
		return plan
	}
	if plan.FallbackReason != columnSortKeyMarkFallbackNone {
		plan.SortedGroupedDistinctFallbackReason = columnSortedGroupedDistinctFallbackMissingPrefix
		return plan
	}
	plan.Planned = true
	plan.SortedGroupedDistinctReady, plan.SortedGroupedDistinctFallbackReason = sortedGroupedDistinctReadiness(plan, sortKey, req)
	return plan
}

func columnStoreColumnByNameForSortKeyPrefix(cfg ColumnStoreConfig, name string) (ColumnStoreColumn, bool) {
	for _, column := range cfg.Columns {
		if column.Name == name {
			return column, true
		}
	}
	return ColumnStoreColumn{}, false
}

func columnPhysicalQueryPredicateForSortKeyPrefix(req ColumnPhysicalQueryRequest, column string) (ColumnPhysicalQueryPredicate, bool) {
	for _, predicate := range req.Predicates {
		if predicate.Column == column {
			return predicate, true
		}
	}
	return ColumnPhysicalQueryPredicate{}, false
}

func sortedGroupedDistinctReadiness(plan columnTypedColumnSortKeyPrefixPlan, sortKey []ColumnSortKey, req ColumnPhysicalQueryRequest) (bool, string) {
	if req.Kind != ColumnPhysicalQueryGroupCountAndDistinct {
		return false, columnSortedGroupedDistinctFallbackNotQ2
	}
	if !plan.Planned {
		return false, columnSortedGroupedDistinctFallbackMissingPrefix
	}
	prefixLen := plan.PrefixLen
	if req.GroupColumn == "" || req.DistinctColumn == "" || len(sortKey) < prefixLen+2 {
		return false, columnSortedGroupedDistinctFallbackSortKeyLayout
	}
	if sortKey[prefixLen].Column != req.GroupColumn || sortKey[prefixLen+1].Column != req.DistinctColumn {
		return false, columnSortedGroupedDistinctFallbackSortKeyLayout
	}
	return true, columnSortedGroupedDistinctFallbackNone
}

func (plan columnTypedColumnSortKeyPrefixPlan) prunePartRows(part *typedColumnAdapterPart) (columnTypedColumnSortKeyPartPruneResult, error) {
	if part == nil || part.Part == nil {
		return columnTypedColumnSortKeyPartPruneResult{}, fmt.Errorf("collections: nil typed-column part for sort-key pruning")
	}
	result := columnTypedColumnSortKeyPartPruneResult{Considered: len(part.Part.Descriptor.Granules)}
	if !plan.Planned {
		result.FallbackReason = plan.FallbackReason
		result.AllRows = true
		result.DecodedGranules = len(part.Part.Descriptor.Granules)
		return result, nil
	}
	if len(part.Part.Marks) == 0 {
		result.FallbackReason = columnSortKeyMarkFallbackMissingMarks
		result.AllRows = true
		result.DecodedGranules = len(part.Part.Descriptor.Granules)
		return result, nil
	}
	if len(part.Part.Marks) != len(part.Part.Descriptor.Granules) {
		result.FallbackReason = columnSortKeyMarkFallbackStaleMarks
		result.AllRows = true
		result.DecodedGranules = len(part.Part.Descriptor.Granules)
		return result, nil
	}
	ranges, rangeCount, literalAbsent, fallback, err := plan.compilePartRanges(part)
	if err != nil {
		return result, err
	}
	if fallback != columnSortKeyMarkFallbackNone {
		result.FallbackReason = fallback
		result.AllRows = true
		result.DecodedGranules = len(part.Part.Descriptor.Granules)
		return result, nil
	}
	result.FallbackReason = columnSortKeyMarkFallbackNone
	if literalAbsent {
		result.FallbackReason = columnSortKeyMarkFallbackLiteralAbsent
		result.Checks = len(part.Part.Marks)
		result.Skips = len(part.Part.Marks)
		return result, nil
	}
	matchedGranules := make([]typedcolumn.GranuleDescriptor, 0, len(part.Part.Descriptor.Granules))
	selectedRows := 0
	for i, granule := range part.Part.Descriptor.Granules {
		mark := part.Part.Marks[i]
		if mark.Rows != granule.RowCount || granule.MarkOrdinal != i {
			result.FallbackReason = columnSortKeyMarkFallbackStaleMarks
			result.AllRows = true
			result.DecodedGranules = len(part.Part.Descriptor.Granules)
			return result, nil
		}
		mayContain, constrained, err := mark.MayContainRanges(ranges[:rangeCount])
		if err != nil {
			return result, fmt.Errorf("collections: sort-key mark prune granule %d: %w", i, err)
		}
		if constrained {
			result.Checks++
		}
		if constrained && !mayContain {
			result.Skips++
			continue
		}
		if constrained {
			result.Matches++
		}
		matchedGranules = append(matchedGranules, granule)
		selectedRows += granule.RowCount
		result.DecodedGranules++
	}
	if result.Skips == 0 && result.DecodedGranules == len(part.Part.Descriptor.Granules) {
		result.AllRows = true
		return result, nil
	}
	if selectedRows == 0 {
		result.Rows = []int{}
		return result, nil
	}
	result.Rows = make([]int, 0, selectedRows)
	for _, granule := range matchedGranules {
		result.Rows = appendRowsForGranule(result.Rows, granule)
	}
	return result, nil
}

func (plan columnTypedColumnSortKeyPrefixPlan) compilePartRanges(part *typedColumnAdapterPart) ([typedColumnPartSortKeyMaxColumns]typedcolumn.Int64RangePredicate, int, bool, string, error) {
	var ranges [typedColumnPartSortKeyMaxColumns]typedcolumn.Int64RangePredicate
	for i := 0; i < plan.PrefixLen; i++ {
		columnName := plan.Columns[i]
		column, ok := part.columnByName(columnName)
		if !ok {
			return ranges, 0, false, columnSortKeyMarkFallbackStaleMarks, nil
		}
		if column.Field.Nullable {
			return ranges, 0, false, columnSortKeyMarkFallbackNullableDefaulted, nil
		}
		encoded, present, fallback, err := encodeSortKeyPrefixPredicateValue(column, plan.Values[i])
		if err != nil || fallback != columnSortKeyMarkFallbackNone {
			return ranges, 0, false, fallback, err
		}
		if !present {
			return ranges, 0, true, columnSortKeyMarkFallbackNone, nil
		}
		ranges[i] = typedcolumn.Int64RangePredicate{Column: column.Definition.Name, Low: encoded, High: encoded}
	}
	return ranges, plan.PrefixLen, false, columnSortKeyMarkFallbackNone, nil
}

func encodeSortKeyPrefixPredicateValue(column typedColumnAdapterColumn, value string) (int64, bool, string, error) {
	switch column.Field.ValueType {
	case ColumnStoreValueString:
		if validateTypedColumnAdapterStringDictionary(column, column.Definition.Cardinality, column.Dictionary) != nil {
			return 0, false, columnSortKeyMarkFallbackUncertifiedDictionaryOrdering, nil
		}
		code, ok := column.Dictionary[value]
		return code, ok, columnSortKeyMarkFallbackNone, nil
	case ColumnStoreValueBool:
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return 0, false, columnSortKeyMarkFallbackUnsupportedPredicate, nil
		}
		if parsed {
			return 1, true, columnSortKeyMarkFallbackNone, nil
		}
		return 0, true, columnSortKeyMarkFallbackNone, nil
	case ColumnStoreValueInt64:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, false, columnSortKeyMarkFallbackUnsupportedPredicate, nil
		}
		return parsed, true, columnSortKeyMarkFallbackNone, nil
	default:
		return 0, false, columnSortKeyMarkFallbackUnsupportedColumnType, nil
	}
}

func appendRowsForGranule(dst []int, granule typedcolumn.GranuleDescriptor) []int {
	oldLen := len(dst)
	newLen := oldLen + granule.RowCount
	if cap(dst) < newLen {
		next := make([]int, newLen)
		copy(next, dst)
		dst = next
	} else {
		dst = dst[:newLen]
	}
	for i := 0; i < granule.RowCount; i++ {
		dst[oldLen+i] = granule.FirstRow + i
	}
	return dst
}

func mergeColumnPhysicalSortKeyFallbackReason(left, right string) string {
	if left == "" || left == columnSortKeyMarkFallbackNone {
		if right == "" {
			return columnSortKeyMarkFallbackNone
		}
		return right
	}
	if right == "" || right == columnSortKeyMarkFallbackNone || right == left {
		return left
	}
	return "mixed"
}
