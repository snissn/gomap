package documentservice

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
)

// scalarFieldPrefix is the stored-document meta namespace backing every
// declared service scalar field.
const scalarFieldPrefix = "meta."

// normalizedScalarField is a validated create-index scalar declaration.
type normalizedScalarField struct {
	field        string
	indexName    string
	collectionTy collections.IndexValueType
	serviceType  ScalarFieldType
}

// normalizeScalarFieldDeclarations validates create-index scalar field
// declarations and derives the stable collection secondary index names that
// back them. Declaration-time only: indexes are created with the collection,
// never retrofitted onto existing documents.
func normalizeScalarFieldDeclarations(declarations []ScalarFieldDeclaration) ([]normalizedScalarField, error) {
	if len(declarations) == 0 {
		return nil, nil
	}
	out := make([]normalizedScalarField, 0, len(declarations))
	byField := make(map[string]struct{}, len(declarations))
	byIndexName := make(map[string]struct{}, len(declarations))
	for i, declaration := range declarations {
		path := strings.TrimSpace(declaration.Field)
		if path == "" {
			return nil, serviceErrorf(CodeInvalidRequest, "scalar_fields[%d].field must not be empty", i)
		}
		if path == "id" || path == "content" || path == "embedding" {
			return nil, serviceErrorf(CodeInvalidRequest, "scalar_fields[%d].field %q is a reserved document field; declare meta fields only", i, declaration.Field)
		}
		path = strings.TrimPrefix(path, scalarFieldPrefix)
		if path == "id" || path == "content" || path == "embedding" {
			return nil, serviceErrorf(CodeInvalidRequest, "scalar_fields[%d].field %q is a reserved document field; declare meta fields only", i, declaration.Field)
		}
		if strings.TrimSpace(path) == "" || strings.HasSuffix(path, ".") || strings.Contains(path, "..") {
			return nil, serviceErrorf(CodeInvalidRequest, "scalar_fields[%d].field %q is not a valid meta path", i, declaration.Field)
		}
		for _, segment := range strings.Split(path, ".") {
			if strings.TrimSpace(segment) == "" {
				return nil, serviceErrorf(CodeInvalidRequest, "scalar_fields[%d].field %q is not a valid meta path", i, declaration.Field)
			}
		}
		valueType, err := normalizeScalarFieldType(i, declaration.ValueType)
		if err != nil {
			return nil, err
		}
		field := scalarFieldPrefix + path
		indexName := "meta_" + strings.ReplaceAll(path, ".", "_")
		if err := collections.ValidateIndexName(indexName); err != nil {
			return nil, serviceErrorf(CodeInvalidRequest, "scalar_fields[%d].field %q derives invalid index name %q: %v", i, declaration.Field, indexName, err)
		}
		if _, ok := byField[field]; ok {
			return nil, serviceErrorf(CodeInvalidRequest, "scalar_fields[%d] declares meta field %q more than once", i, field)
		}
		if _, ok := byIndexName[indexName]; ok {
			return nil, serviceErrorf(CodeInvalidRequest, "scalar_fields[%d] maps meta field %q to already-used scalar index name %q", i, field, indexName)
		}
		byField[field] = struct{}{}
		byIndexName[indexName] = struct{}{}
		out = append(out, normalizedScalarField{
			field:        field,
			indexName:    indexName,
			collectionTy: scalarFieldTypeToCollection(valueType),
			serviceType:  valueType,
		})
	}
	return out, nil
}

func normalizeScalarFieldType(position int, raw ScalarFieldType) (ScalarFieldType, error) {
	switch ScalarFieldType(strings.TrimSpace(strings.ToLower(string(raw)))) {
	case "", ScalarFieldString:
		return ScalarFieldString, nil
	case ScalarFieldBool:
		return ScalarFieldBool, nil
	case ScalarFieldInt64:
		return ScalarFieldInt64, nil
	case ScalarFieldDouble:
		return ScalarFieldDouble, nil
	default:
		return "", serviceErrorf(CodeInvalidRequest, "scalar_fields[%d].value_type %q is unsupported; use string, bool, int64, or double", position, raw)
	}
}

func scalarFieldTypeToCollection(valueType ScalarFieldType) collections.IndexValueType {
	switch valueType {
	case ScalarFieldBool:
		return collections.IndexValueBool
	case ScalarFieldInt64:
		return collections.IndexValueInt64
	case ScalarFieldDouble:
		return collections.IndexValueDouble
	default:
		return collections.IndexValueString
	}
}

func scalarFieldTypeFromCollection(valueType collections.IndexValueType) ScalarFieldType {
	switch valueType {
	case collections.IndexValueBool:
		return ScalarFieldBool
	case collections.IndexValueInt64:
		return ScalarFieldInt64
	case collections.IndexValueDouble:
		return ScalarFieldDouble
	default:
		return ScalarFieldString
	}
}

// scalarFieldsFromCollectionIndexes recovers the declared scalar schema from
// the collection meta on open. Only service-owned names (meta_ prefix) are
// treated as scalar schema; anything else belongs to the embedding/text lanes.
func scalarFieldsFromCollectionIndexes(indexes []collections.IndexDefinition) []ScalarFieldInfo {
	var out []ScalarFieldInfo
	for _, index := range indexes {
		if !strings.HasPrefix(index.Name, "meta_") || !strings.HasPrefix(index.Field, scalarFieldPrefix) {
			continue
		}
		out = append(out, ScalarFieldInfo{
			Field:     index.Field,
			IndexName: index.Name,
			ValueType: scalarFieldTypeFromCollection(index.ValueType),
		})
	}
	return out
}

type scalarSchema struct {
	fields    map[string]normalizedScalarField
	declared  bool
	hasFields bool
}

func newScalarSchema(fields []ScalarFieldInfo) scalarSchema {
	schema := scalarSchema{fields: make(map[string]normalizedScalarField, len(fields)), declared: true, hasFields: len(fields) > 0}
	for _, info := range fields {
		schema.fields[info.Field] = normalizedScalarField{
			field:        info.Field,
			indexName:    info.IndexName,
			collectionTy: scalarFieldTypeToCollection(info.ValueType),
			serviceType:  info.ValueType,
		}
	}
	return schema
}

func (s scalarSchema) lookup(field string) (normalizedScalarField, bool) {
	if s.fields == nil {
		return normalizedScalarField{}, false
	}
	resolved, ok := s.fields[field]
	if ok {
		return resolved, true
	}
	if !strings.HasPrefix(field, scalarFieldPrefix) {
		resolved, ok = s.fields[scalarFieldPrefix+field]
	}
	return resolved, ok
}

// translateScalarFilter compiles the service filter AST into the single bounded
// collection scalar predicate the hybrid executor can serve from a secondary
// index. Anything outside that vocabulary fails closed with typed errors
// instead of degrading into scans or partial results.
//
// Supported shapes:
//   - leaf equality/range on one declared scalar field;
//   - AND whose conditions all resolve to that same single field (merged into
//     one equality or range predicate).
//
// OR, NOT, !=, in/not in, multi-field AND, and undeclared fields return typed
// errors and never fall back to document scans.
func translateScalarFilter(filter *Filter, schema scalarSchema) (*collections.HybridScalarFilter, error) {
	if filter == nil {
		return nil, nil
	}
	op, err := normalizeFilterOperator(filter.Operator)
	if err != nil {
		return nil, err
	}
	switch op {
	case filterOpAND:
		var merged *collections.HybridScalarFilter
		var mergedField string
		for i := range filter.Conditions {
			child, childField, err := translateScalarLeafOrConjunction(&filter.Conditions[i], schema)
			if err != nil {
				return nil, err
			}
			if merged == nil {
				merged, mergedField = child, childField
				continue
			}
			combined, err := mergeScalarPredicates(merged, mergedField, child, childField)
			if err != nil {
				return nil, err
			}
			merged = combined
		}
		return merged, nil
	default:
		predicate, _, err := translateScalarLeafOrConjunction(filter, schema)
		return predicate, err
	}
}

// translateScalarLeafOrConjunction returns the compiled predicate plus the
// canonical meta field it targets so AND nodes can verify single-field scope.
func translateScalarLeafOrConjunction(filter *Filter, schema scalarSchema) (*collections.HybridScalarFilter, string, error) {
	op, err := normalizeFilterOperator(filter.Operator)
	if err != nil {
		return nil, "", err
	}
	switch op {
	case filterOpAND:
		var merged *collections.HybridScalarFilter
		var mergedField string
		for i := range filter.Conditions {
			child, childField, err := translateScalarLeafOrConjunction(&filter.Conditions[i], schema)
			if err != nil {
				return nil, "", err
			}
			if merged == nil {
				merged, mergedField = child, childField
				continue
			}
			combined, err := mergeScalarPredicates(merged, mergedField, child, childField)
			if err != nil {
				return nil, "", err
			}
			merged = combined
		}
		return merged, mergedField, nil
	case filterOpEQ, filterOpGT, filterOpGTE, filterOpLT, filterOpLTE:
		resolved, ok := schema.lookup(strings.TrimSpace(filter.Field))
		if !ok {
			return nil, "", serviceErrorf(CodeInvalidRequest, "filter field %q is not declared in the index scalar schema; filtered keyword/hybrid search requires create-index scalar_fields", filter.Field)
		}
		if op == filterOpEQ {
			value, err := convertScalarFilterValue(resolved, filter.Value)
			if err != nil {
				return nil, "", err
			}
			return &collections.HybridScalarFilter{IndexName: resolved.indexName, Value: value}, resolved.field, nil
		}
		bound, err := convertScalarRangeBound(resolved, op, filter.Value)
		if err != nil {
			return nil, "", err
		}
		rangeOpts := &collections.IndexRangeOptions{
			Lower: collections.IndexRangeBound{Unbounded: true},
			Upper: collections.IndexRangeBound{Unbounded: true},
		}
		switch op {
		case filterOpGT, filterOpGTE:
			rangeOpts.Lower = bound
		case filterOpLT, filterOpLTE:
			rangeOpts.Upper = bound
		}
		return &collections.HybridScalarFilter{IndexName: resolved.indexName, Range: rangeOpts}, resolved.field, nil
	default:
		return nil, "", serviceError(CodeUnsupported, fmt.Sprintf("filter operator %q cannot be served as one bounded scalar allow-set; the service will not scan documents as a fallback", filter.Operator))
	}
}

func mergeScalarPredicates(left *collections.HybridScalarFilter, leftField string, right *collections.HybridScalarFilter, rightField string) (*collections.HybridScalarFilter, error) {
	if leftField != rightField || left.IndexName != right.IndexName {
		return nil, serviceError(CodeUnsupported, "AND filters spanning multiple meta fields cannot be served as one bounded scalar allow-set; the service will not scan documents as a fallback")
	}
	switch {
	case left.Value != nil && right.Value != nil:
		return nil, serviceError(CodeUnsupported, "conflicting equality conditions cannot be served as one bounded scalar allow-set")
	case left.Value != nil:
		if right.Range != nil {
			contains, known := scalarRangeContainsValue(right.Range, left.Value)
			if !known {
				return nil, serviceError(CodeUnsupported, "equality and range conditions cannot be safely merged for this scalar type")
			}
			if !contains {
				return nil, serviceError(CodeUnsupported, "equality condition contradicts range condition")
			}
		}
		return &collections.HybridScalarFilter{IndexName: left.IndexName, Value: left.Value}, nil
	case right.Value != nil:
		if left.Range != nil {
			contains, known := scalarRangeContainsValue(left.Range, right.Value)
			if !known {
				return nil, serviceError(CodeUnsupported, "equality and range conditions cannot be safely merged for this scalar type")
			}
			if !contains {
				return nil, serviceError(CodeUnsupported, "equality condition contradicts range condition")
			}
		}
		return &collections.HybridScalarFilter{IndexName: left.IndexName, Value: right.Value}, nil
	}
	merged := &collections.HybridScalarFilter{IndexName: left.IndexName, Range: &collections.IndexRangeOptions{
		Lower: collections.IndexRangeBound{Unbounded: true},
		Upper: collections.IndexRangeBound{Unbounded: true},
	}}
	lowerSet, upperSet := false, false
	for _, source := range []*collections.HybridScalarFilter{left, right} {
		if source.Range == nil {
			continue
		}
		if !source.Range.Lower.Unbounded {
			if lowerSet {
				return nil, serviceError(CodeUnsupported, "multiple lower bounds cannot be served as one bounded scalar allow-set")
			}
			merged.Range.Lower = source.Range.Lower
			lowerSet = true
		}
		if !source.Range.Upper.Unbounded {
			if upperSet {
				return nil, serviceError(CodeUnsupported, "multiple upper bounds cannot be served as one bounded scalar allow-set")
			}
			merged.Range.Upper = source.Range.Upper
			upperSet = true
		}
	}
	return merged, nil
}

func scalarRangeContainsValue(rangeOpts *collections.IndexRangeOptions, value any) (bool, bool) {
	if rangeOpts == nil {
		return true, true
	}
	if !rangeOpts.Lower.Unbounded {
		cmp, ok := compareScalarValues(value, rangeOpts.Lower.Value)
		if !ok {
			return false, false
		}
		if cmp < 0 || (cmp == 0 && !rangeOpts.Lower.Inclusive) {
			return false, true
		}
	}
	if !rangeOpts.Upper.Unbounded {
		cmp, ok := compareScalarValues(value, rangeOpts.Upper.Value)
		if !ok {
			return false, false
		}
		if cmp > 0 || (cmp == 0 && !rangeOpts.Upper.Inclusive) {
			return false, true
		}
	}
	return true, true
}

func compareScalarValues(left, right any) (int, bool) {
	if leftNumber, ok := numberAsFloat64(left); ok {
		rightNumber, rightOK := numberAsFloat64(right)
		if !rightOK {
			return 0, false
		}
		switch {
		case leftNumber < rightNumber:
			return -1, true
		case leftNumber > rightNumber:
			return 1, true
		default:
			return 0, true
		}
	}
	leftString, leftOK := left.(string)
	rightString, rightOK := right.(string)
	if !leftOK || !rightOK {
		return 0, false
	}
	switch {
	case leftString < rightString:
		return -1, true
	case leftString > rightString:
		return 1, true
	default:
		return 0, true
	}
}

func convertScalarFilterValue(field normalizedScalarField, value any) (any, error) {
	label := "filter value for field " + field.field
	switch field.serviceType {
	case ScalarFieldString:
		text, ok := scalarValueAsString(value)
		if !ok {
			return nil, serviceErrorf(CodeInvalidRequest, "%s must be a string", label)
		}
		return text, nil
	case ScalarFieldBool:
		if flag, ok := value.(bool); ok {
			return flag, nil
		}
		return nil, serviceErrorf(CodeInvalidRequest, "%s must be a boolean", label)
	case ScalarFieldInt64:
		number, ok := scalarValueAsNumber(value)
		if !ok {
			return nil, serviceErrorf(CodeInvalidRequest, "%s must be numeric", label)
		}
		parsed, err := number.Int64()
		if err != nil {
			return nil, serviceErrorf(CodeInvalidRequest, "%s must be an int64", label)
		}
		return parsed, nil
	case ScalarFieldDouble:
		number, ok := scalarValueAsNumber(value)
		if !ok {
			return nil, serviceErrorf(CodeInvalidRequest, "%s must be numeric", label)
		}
		float, err := number.Float64()
		if err != nil {
			return nil, serviceErrorf(CodeInvalidRequest, "%s must be a double", label)
		}
		return float, nil
	}
	return nil, serviceErrorf(CodeInvalidRequest, "%s has unsupported declared type %q", label, field.serviceType)
}

func convertScalarRangeBound(field normalizedScalarField, operator string, value any) (collections.IndexRangeBound, error) {
	converted, err := convertScalarFilterValue(field, value)
	if err != nil {
		return collections.IndexRangeBound{}, err
	}
	inclusive := operator == filterOpGTE || operator == filterOpLTE
	return collections.IndexRangeBound{Value: converted, Inclusive: inclusive}, nil
}

// scalarValueAsString accepts only JSON strings for string-declared fields.
func scalarValueAsString(value any) (string, bool) {
	text, ok := value.(string)
	return text, ok
}

func scalarValueAsNumber(value any) (json.Number, bool) {
	switch typed := value.(type) {
	case json.Number:
		return typed, true
	case int64:
		return json.Number(fmt.Sprintf("%d", typed)), true
	case int:
		return json.Number(fmt.Sprintf("%d", typed)), true
	case float64:
		return json.Number(strconv.FormatFloat(typed, 'f', -1, 64)), true
	default:
		return "", false
	}
}
