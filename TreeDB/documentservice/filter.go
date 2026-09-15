package documentservice

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
)

const (
	filterOpAND   = "and"
	filterOpOR    = "or"
	filterOpNOT   = "not"
	filterOpEQ    = "=="
	filterOpNE    = "!="
	filterOpGT    = ">"
	filterOpGTE   = ">="
	filterOpLT    = "<"
	filterOpLTE   = "<="
	filterOpIn    = "in"
	filterOpNotIn = "not in"
)

// Filter is the supported Haystack-style metadata filter AST.
//
// Boolean nodes use operator AND/OR/NOT and conditions. Leaf nodes use field,
// operator, and value. Field names may be id, content, meta.<path>, or a
// metadata path without the meta. prefix.
type Filter struct {
	Operator   string   `json:"operator"`
	Field      string   `json:"field,omitempty"`
	Value      any      `json:"value,omitempty"`
	Conditions []Filter `json:"conditions,omitempty"`
}

// Validate rejects unsupported operators and malformed filter shapes before any
// document scan runs.
func (f *Filter) Validate() error {
	if f == nil {
		return nil
	}
	return f.validateAt("filter")
}

func (f *Filter) validateAt(path string) error {
	op, err := normalizeFilterOperator(f.Operator)
	if err != nil {
		return err
	}
	switch op {
	case filterOpAND, filterOpOR:
		if f.Field != "" {
			return serviceErrorf(CodeInvalidRequest, "%s: boolean operator %q cannot set field", path, f.Operator)
		}
		if len(f.Conditions) == 0 {
			return serviceErrorf(CodeInvalidRequest, "%s: operator %q requires at least one condition", path, f.Operator)
		}
		for i := range f.Conditions {
			if err := f.Conditions[i].validateAt(fmt.Sprintf("%s.conditions[%d]", path, i)); err != nil {
				return err
			}
		}
		return nil
	case filterOpNOT:
		if f.Field != "" {
			return serviceErrorf(CodeInvalidRequest, "%s: NOT cannot set field", path)
		}
		if len(f.Conditions) != 1 {
			return serviceErrorf(CodeInvalidRequest, "%s: NOT requires exactly one condition", path)
		}
		return f.Conditions[0].validateAt(path + ".conditions[0]")
	case filterOpEQ, filterOpNE, filterOpGT, filterOpGTE, filterOpLT, filterOpLTE, filterOpIn, filterOpNotIn:
		field := strings.TrimSpace(f.Field)
		if field == "" {
			return serviceErrorf(CodeInvalidRequest, "%s: field is required for operator %q", path, f.Operator)
		}
		if field == "embedding" {
			return serviceErrorf(CodeInvalidRequest, "%s: embedding filters are unsupported; filter metadata fields instead", path)
		}
		if len(f.Conditions) != 0 {
			return serviceErrorf(CodeInvalidRequest, "%s: leaf operator %q cannot set conditions", path, f.Operator)
		}
		switch op {
		case filterOpGT, filterOpGTE, filterOpLT, filterOpLTE:
			if !isComparableFilterValue(f.Value) {
				return serviceErrorf(CodeInvalidRequest, "%s: operator %q requires a numeric or string value", path, f.Operator)
			}
		case filterOpIn, filterOpNotIn:
			if _, ok := filterListValues(f.Value); !ok {
				return serviceErrorf(CodeInvalidRequest, "%s: operator %q requires an array value", path, f.Operator)
			}
		}
		return nil
	default:
		return serviceErrorf(CodeInvalidRequest, "%s: unsupported filter operator %q", path, f.Operator)
	}
}

func normalizeFilterOperator(raw string) (string, error) {
	op := strings.TrimSpace(strings.ToLower(raw))
	switch op {
	case filterOpAND:
		return filterOpAND, nil
	case filterOpOR:
		return filterOpOR, nil
	case filterOpNOT:
		return filterOpNOT, nil
	case filterOpEQ:
		return filterOpEQ, nil
	case filterOpNE:
		return filterOpNE, nil
	case filterOpGT:
		return filterOpGT, nil
	case filterOpGTE:
		return filterOpGTE, nil
	case filterOpLT:
		return filterOpLT, nil
	case filterOpLTE:
		return filterOpLTE, nil
	case filterOpIn:
		return filterOpIn, nil
	case filterOpNotIn:
		return filterOpNotIn, nil
	default:
		return "", serviceErrorf(CodeInvalidRequest, "unsupported filter operator %q", raw)
	}
}

func matchFilter(filter *Filter, doc Document) (bool, error) {
	if filter == nil {
		return true, nil
	}
	return filter.match(doc)
}

// MatchesDocument reports whether doc satisfies f. The caller must first call
// Validate once before matching a result set.
func (f *Filter) MatchesDocument(doc Document) (bool, error) {
	if f == nil {
		return true, nil
	}
	return f.match(doc)
}

func (f *Filter) match(doc Document) (bool, error) {
	op, err := normalizeFilterOperator(f.Operator)
	if err != nil {
		return false, err
	}
	switch op {
	case filterOpAND:
		for i := range f.Conditions {
			ok, err := f.Conditions[i].match(doc)
			if err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	case filterOpOR:
		for i := range f.Conditions {
			ok, err := f.Conditions[i].match(doc)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	case filterOpNOT:
		ok, err := f.Conditions[0].match(doc)
		return !ok, err
	}
	left, found := lookupFilterField(doc, f.Field)
	if !found {
		return false, nil
	}
	switch op {
	case filterOpEQ:
		return valuesEqual(left, f.Value), nil
	case filterOpNE:
		return !valuesEqual(left, f.Value), nil
	case filterOpGT, filterOpGTE, filterOpLT, filterOpLTE:
		cmp, err := compareFilterValues(left, f.Value)
		if err != nil {
			return false, err
		}
		switch op {
		case filterOpGT:
			return cmp > 0, nil
		case filterOpGTE:
			return cmp >= 0, nil
		case filterOpLT:
			return cmp < 0, nil
		case filterOpLTE:
			return cmp <= 0, nil
		}
	case filterOpIn, filterOpNotIn:
		values, ok := filterListValues(f.Value)
		if !ok {
			return false, serviceErrorf(CodeInvalidRequest, "operator %q requires an array value", f.Operator)
		}
		contained := valueInList(left, values)
		if op == filterOpNotIn {
			return !contained, nil
		}
		return contained, nil
	}
	return false, serviceErrorf(CodeInvalidRequest, "unsupported filter operator %q", f.Operator)
}

func lookupFilterField(doc Document, field string) (any, bool) {
	field = strings.TrimSpace(field)
	switch field {
	case "id":
		return doc.ID, true
	case "content":
		return doc.Content, true
	}
	if strings.HasPrefix(field, "meta.") {
		return lookupPath(doc.Meta, strings.TrimPrefix(field, "meta."))
	}
	return lookupPath(doc.Meta, field)
}

func lookupPath(root map[string]any, path string) (any, bool) {
	if root == nil || path == "" {
		return nil, false
	}
	parts := strings.Split(path, ".")
	var current any = root
	for _, part := range parts {
		if part == "" {
			return nil, false
		}
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func isComparableFilterValue(value any) bool {
	if _, ok := numberAsFloat64(value); ok {
		return true
	}
	_, ok := value.(string)
	return ok
}

func compareFilterValues(left, right any) (int, error) {
	if comparison, ok := compareFilterNumbers(left, right); ok {
		return comparison, nil
	}
	if _, ok := numberAsFloat64(left); ok {
		return 0, serviceErrorf(CodeInvalidRequest, "cannot compare numeric field to non-numeric filter value")
	}
	ls, lok := left.(string)
	rs, rok := right.(string)
	if lok && rok {
		return strings.Compare(ls, rs), nil
	}
	return 0, serviceErrorf(CodeInvalidRequest, "filter comparisons require numeric or string operands")
}

func valuesEqual(left, right any) bool {
	if comparison, ok := compareFilterNumbers(left, right); ok {
		return comparison == 0
	}
	return reflect.DeepEqual(left, right)
}

func compareFilterNumbers(left, right any) (int, bool) {
	leftInteger, leftIsInteger := filterInt64Value(left)
	rightInteger, rightIsInteger := filterInt64Value(right)
	if leftIsInteger && rightIsInteger {
		return compareInt64(leftInteger, rightInteger), true
	}
	leftFloat, leftIsNumber := numberAsFloat64(left)
	rightFloat, rightIsNumber := numberAsFloat64(right)
	if !leftIsNumber || !rightIsNumber {
		return 0, false
	}
	if leftIsInteger {
		return compareInt64ToFloat64(leftInteger, rightFloat), true
	}
	if rightIsInteger {
		return -compareInt64ToFloat64(rightInteger, leftFloat), true
	}
	switch {
	case leftFloat < rightFloat:
		return -1, true
	case leftFloat > rightFloat:
		return 1, true
	default:
		return 0, true
	}
}

func filterInt64Value(value any) (int64, bool) {
	switch v := value.(type) {
	case json.Number:
		if integer, err := v.Int64(); err == nil {
			return integer, true
		}
		floating, err := v.Float64()
		if err != nil {
			return 0, false
		}
		return filterInt64Value(floating)
	case float64:
		if !finiteFloat(v) || math.Trunc(v) != v || v < -0x1p63 || v >= 0x1p63 {
			return 0, false
		}
		return int64(v), true
	case float32:
		return filterInt64Value(float64(v))
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		if uint64(v) <= uint64(^uint64(0)>>1) {
			return int64(v), true
		}
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v <= uint64(^uint64(0)>>1) {
			return int64(v), true
		}
	}
	return 0, false
}

func compareInt64(left, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareInt64ToFloat64(integer int64, floating float64) int {
	if floating < -0x1p63 {
		return 1
	}
	if floating >= 0x1p63 {
		return -1
	}
	truncated := int64(floating)
	if comparison := compareInt64(integer, truncated); comparison != 0 {
		return comparison
	}
	switch {
	case float64(truncated) < floating:
		return -1
	case float64(truncated) > floating:
		return 1
	default:
		return 0
	}
}

func valueInList(value any, list []any) bool {
	if values, ok := filterListValues(value); ok && !isBytes(value) {
		for _, item := range values {
			if valueInList(item, list) {
				return true
			}
		}
		return false
	}
	for _, item := range list {
		if valuesEqual(value, item) {
			return true
		}
	}
	return false
}

func filterListValues(value any) ([]any, bool) {
	if value == nil {
		return nil, false
	}
	if list, ok := value.([]any); ok {
		return list, true
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out, true
}

func isBytes(value any) bool {
	_, ok := value.([]byte)
	return ok
}

func finiteFloat(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func numberAsFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case json.Number:
		f, err := v.Float64()
		return f, err == nil && finiteFloat(f)
	case float64:
		return v, finiteFloat(v)
	case float32:
		f := float64(v)
		return f, finiteFloat(f)
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}
