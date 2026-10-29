package collections

import (
	"bytes"
	"fmt"
)

func supportedNativeScalarType(valueType IndexValueType) bool {
	switch valueType {
	case IndexValueString, IndexValueBool, IndexValueInt64, IndexValueDouble:
		return true
	default:
		return false
	}
}

type nativeScalarClause struct {
	indexName      string
	valueType      IndexValueType
	lower          []byte
	upper          []byte
	lowerInclusive bool
	upperInclusive bool
}

func (c nativeScalarClause) matches(value []byte, present bool) bool {
	if !present {
		return false
	}
	if c.lower != nil {
		cmp := bytes.Compare(value, c.lower)
		if cmp < 0 || cmp == 0 && !c.lowerInclusive {
			return false
		}
	}
	if c.upper != nil {
		cmp := bytes.Compare(value, c.upper)
		if cmp > 0 || cmp == 0 && !c.upperInclusive {
			return false
		}
	}
	return true
}

func nativeScalarClausesEqual(a, b []nativeScalarClause) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := &a[i], &b[i]
		if x.indexName != y.indexName || x.valueType != y.valueType ||
			x.lowerInclusive != y.lowerInclusive || x.upperInclusive != y.upperInclusive ||
			(x.lower == nil) != (y.lower == nil) || (x.upper == nil) != (y.upper == nil) ||
			!bytes.Equal(x.lower, y.lower) || !bytes.Equal(x.upper, y.upper) {
			return false
		}
	}
	return true
}

func compileNativeScalarClause(def IndexDefinition, filter HybridScalarFilter) (nativeScalarClause, error) {
	if !supportedNativeScalarType(def.ValueType) || def.MultiKey || len(def.Components) > 1 {
		return nativeScalarClause{}, fmt.Errorf("%w: native scalar index %q has unsupported type or multikey/compound shape", ErrHybridSearchUnsupported, def.Name)
	}
	clause := nativeScalarClause{indexName: def.Name, valueType: def.ValueType}
	if filter.Range == nil {
		encoded, err := encodeIndexScalar(def.ValueType, filter.Value)
		if err != nil {
			return nativeScalarClause{}, fmt.Errorf("%w: native scalar index %q value: %v", ErrHybridSearchUnsupported, def.Name, err)
		}
		clause.lower, clause.upper = encoded, bytes.Clone(encoded)
		clause.lowerInclusive, clause.upperInclusive = true, true
		return clause, nil
	}
	if !filter.Range.Lower.Unbounded {
		encoded, err := encodeIndexScalar(def.ValueType, filter.Range.Lower.Value)
		if err != nil {
			return nativeScalarClause{}, fmt.Errorf("%w: native scalar index %q lower bound: %v", ErrHybridSearchUnsupported, def.Name, err)
		}
		clause.lower, clause.lowerInclusive = encoded, filter.Range.Lower.Inclusive
	}
	if !filter.Range.Upper.Unbounded {
		encoded, err := encodeIndexScalar(def.ValueType, filter.Range.Upper.Value)
		if err != nil {
			return nativeScalarClause{}, fmt.Errorf("%w: native scalar index %q upper bound: %v", ErrHybridSearchUnsupported, def.Name, err)
		}
		clause.upper, clause.upperInclusive = encoded, filter.Range.Upper.Inclusive
	}
	return clause, nil
}

const (
	nativeScalarPlanCacheMaxEntries = 64
	nativeScalarPlanCacheMaxBytes   = 8 << 20
)

// NativeScalarFilterPlan reports the bounded typed/scalar-forward execution plan.
type NativeScalarFilterPlan string

const (
	NativeScalarFilterPlanNone           NativeScalarFilterPlan = "none"
	NativeScalarFilterPlanCompleteExact  NativeScalarFilterPlan = "complete_exact"
	NativeScalarFilterPlanCompleteFinite NativeScalarFilterPlan = "complete_finite_ann"
	NativeScalarFilterPlanMixed          NativeScalarFilterPlan = "mixed_refined"
	NativeScalarFilterPlanVectorAligned  NativeScalarFilterPlan = "vector_aligned_ann"
)
