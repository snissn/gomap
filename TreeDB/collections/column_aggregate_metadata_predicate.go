package collections

import (
	"fmt"
	"sort"
)

type columnAggregateMetadataPredicateSpec struct {
	column              string
	kind                ColumnPhysicalQueryPredicateKind
	values              []string
	columnIdx           int
	missingMatchesEmpty bool
}

func columnAggregateMetadataPredicateSpecs(cfg ColumnStoreConfig, predicates []ColumnPhysicalQueryPredicate) ([]columnAggregateMetadataPredicateSpec, error) {
	if len(predicates) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(predicates))
	specs := make([]columnAggregateMetadataPredicateSpec, 0, len(predicates))
	for idx, predicate := range predicates {
		if predicate.Column == "" {
			return nil, fmt.Errorf("%w: aggregate metadata predicate[%d] column is required", ErrColumnQueryPlanUnsupported, idx)
		}
		if _, ok := seen[predicate.Column]; ok {
			return nil, fmt.Errorf("%w: multiple aggregate metadata predicates on column %q are not supported", ErrColumnQueryPlanUnsupported, predicate.Column)
		}
		seen[predicate.Column] = struct{}{}
		col, columnIdx, ok := columnPhysicalQueryDeclaredColumn(cfg, predicate.Column)
		if !ok {
			return nil, fmt.Errorf("%w: aggregate metadata predicate requested undeclared column %q", ErrColumnQueryPlanUnsupported, predicate.Column)
		}
		if col.ValueType != ColumnStoreValueString {
			return nil, fmt.Errorf("%w: aggregate metadata predicate column %q has type %q, want %q", ErrColumnQueryPlanUnsupported, predicate.Column, col.ValueType, ColumnStoreValueString)
		}
		if !col.Dictionary {
			return nil, fmt.Errorf("%w: aggregate metadata predicate column %q requires dictionary string storage", ErrColumnQueryPlanUnsupported, predicate.Column)
		}
		kind := columnPhysicalQueryPredicateKindOrDefault(predicate.Kind)
		var values []string
		switch kind {
		case ColumnPhysicalQueryPredicateEqual:
			if len(predicate.Values) != 0 {
				return nil, fmt.Errorf("%w: aggregate metadata predicate column %q equal uses Value, not Values", ErrColumnQueryPlanUnsupported, predicate.Column)
			}
			values = []string{predicate.Value}
		case ColumnPhysicalQueryPredicateInList:
			if predicate.Value != "" {
				return nil, fmt.Errorf("%w: aggregate metadata predicate column %q in-list uses Values, not Value", ErrColumnQueryPlanUnsupported, predicate.Column)
			}
			if len(predicate.Values) == 0 {
				return nil, fmt.Errorf("%w: aggregate metadata predicate column %q in-list requires at least one value", ErrColumnQueryPlanUnsupported, predicate.Column)
			}
			if len(predicate.Values) > columnPhysicalQueryMaxPredicateValues {
				return nil, fmt.Errorf("%w: aggregate metadata predicate column %q in-list values=%d exceeds limit=%d", ErrColumnQueryPlanUnsupported, predicate.Column, len(predicate.Values), columnPhysicalQueryMaxPredicateValues)
			}
			values = append([]string(nil), predicate.Values...)
		default:
			return nil, fmt.Errorf("%w: unsupported aggregate metadata predicate kind %q for column %q", ErrColumnQueryPlanUnsupported, predicate.Kind, predicate.Column)
		}
		if kind == ColumnPhysicalQueryPredicateInList {
			sort.Strings(values)
			for valueIdx := 1; valueIdx < len(values); valueIdx++ {
				if values[valueIdx] == values[valueIdx-1] {
					return nil, fmt.Errorf("%w: aggregate metadata predicate column %q has duplicate in-list value %q", ErrColumnQueryPlanUnsupported, predicate.Column, values[valueIdx])
				}
			}
		}
		specs = append(specs, columnAggregateMetadataPredicateSpec{
			column:              predicate.Column,
			kind:                kind,
			values:              values,
			columnIdx:           columnIdx,
			missingMatchesEmpty: col.Nullable && columnAggregateMetadataPredicateValuesContainEmpty(values),
		})
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].column < specs[j].column })
	return specs, nil
}

func columnAggregateMetadataPredicateValuesContainEmpty(values []string) bool {
	for _, value := range values {
		if value == "" {
			return true
		}
	}
	return false
}

func columnAggregateMetadataCanonicalPredicates(cfg ColumnStoreConfig, predicates []ColumnPhysicalQueryPredicate) ([]ColumnPhysicalQueryPredicate, error) {
	specs, err := columnAggregateMetadataPredicateSpecs(cfg, predicates)
	if err != nil || len(specs) == 0 {
		return nil, err
	}
	out := make([]ColumnPhysicalQueryPredicate, len(specs))
	for idx, spec := range specs {
		out[idx] = ColumnPhysicalQueryPredicate{Column: spec.column, Kind: spec.kind}
		if spec.kind == ColumnPhysicalQueryPredicateInList {
			out[idx].Values = append([]string(nil), spec.values...)
		} else {
			out[idx].Value = spec.values[0]
		}
	}
	return out, nil
}

func columnAggregateMetadataPredicatesEqual(cfg ColumnStoreConfig, left, right []ColumnPhysicalQueryPredicate) (bool, error) {
	leftCanon, err := columnAggregateMetadataCanonicalPredicates(cfg, left)
	if err != nil {
		return false, err
	}
	rightCanon, err := columnAggregateMetadataCanonicalPredicates(cfg, right)
	if err != nil {
		return false, err
	}
	return columnPhysicalQueryPredicatesExactEqual(leftCanon, rightCanon), nil
}

func columnPhysicalQueryPredicatesExactEqual(left, right []ColumnPhysicalQueryPredicate) bool {
	if len(left) != len(right) {
		return false
	}
	for idx := range left {
		if left[idx].Column != right[idx].Column || columnPhysicalQueryPredicateKindOrDefault(left[idx].Kind) != columnPhysicalQueryPredicateKindOrDefault(right[idx].Kind) || left[idx].Value != right[idx].Value || len(left[idx].Values) != len(right[idx].Values) {
			return false
		}
		for valueIdx := range left[idx].Values {
			if left[idx].Values[valueIdx] != right[idx].Values[valueIdx] {
				return false
			}
		}
	}
	return true
}

func cloneColumnAggregateMetadata(aggregates []ColumnAggregateMetadata) []ColumnAggregateMetadata {
	if len(aggregates) == 0 {
		return nil
	}
	out := make([]ColumnAggregateMetadata, len(aggregates))
	for idx := range aggregates {
		out[idx] = aggregates[idx]
		out[idx].Predicates = cloneColumnPhysicalQueryPredicates(aggregates[idx].Predicates)
	}
	return out
}

func cloneColumnPhysicalQueryPredicates(predicates []ColumnPhysicalQueryPredicate) []ColumnPhysicalQueryPredicate {
	if len(predicates) == 0 {
		return nil
	}
	out := make([]ColumnPhysicalQueryPredicate, len(predicates))
	for idx := range predicates {
		out[idx] = predicates[idx]
		if len(predicates[idx].Values) != 0 {
			out[idx].Values = append([]string(nil), predicates[idx].Values...)
		}
	}
	return out
}

func columnAggregateMetadataEqual(left, right ColumnAggregateMetadata) bool {
	return left.Name == right.Name &&
		left.Column == right.Column &&
		left.GroupColumn == right.GroupColumn &&
		left.Kind == right.Kind &&
		columnPhysicalQueryPredicatesExactEqual(left.Predicates, right.Predicates)
}

func columnAggregateMetadataPredicatesMatchRow(specs []columnAggregateMetadataPredicateSpec, values []columnDeclaredValue) (bool, error) {
	for _, spec := range specs {
		if spec.columnIdx < 0 || spec.columnIdx >= len(values) {
			return false, fmt.Errorf("collections: aggregate metadata predicate column %q is missing declared row value", spec.column)
		}
		value := values[spec.columnIdx]
		if value.Null || !value.Present {
			if spec.missingMatchesEmpty {
				continue
			}
			return false, nil
		}
		if value.Type != ColumnStoreValueString {
			return false, fmt.Errorf("%w: aggregate metadata predicate column %q encountered incompatible row value type %q", ErrColumnQueryPlanUnsupported, spec.column, value.Type)
		}
		rowValue := value.String
		if rowValue == "" && value.StringBytes != nil {
			rowValue = string(value.StringBytes)
		}
		matched := false
		for _, want := range spec.values {
			if rowValue == want {
				matched = true
				break
			}
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}
