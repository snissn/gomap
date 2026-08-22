package documentservice

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func testScalarFilterSchema() scalarSchema {
	return newScalarSchema([]ScalarFieldInfo{{
		Field:     "meta.priority",
		IndexName: "meta_priority",
		ValueType: ScalarFieldInt64,
	}})
}

func TestTranslateScalarFilterMergesTwoSidedRange(t *testing.T) {
	filter := &Filter{Operator: "AND", Conditions: []Filter{
		{Field: "meta.priority", Operator: ">=", Value: int64(2)},
		{Field: "meta.priority", Operator: "<=", Value: int64(3)},
	}}
	got, err := translateScalarFilter(filter, testScalarFilterSchema())
	if err != nil {
		t.Fatalf("translateScalarFilter: %v", err)
	}
	if got == nil || got.Range == nil {
		t.Fatalf("got=%+v, want a range", got)
	}
	if got.Range.Lower.Unbounded || got.Range.Upper.Unbounded {
		t.Fatalf("range=%+v, want both bounds", got.Range)
	}
	if got.Range.Lower.Value != int64(2) || !got.Range.Lower.Inclusive {
		t.Fatalf("lower=%+v, want inclusive 2", got.Range.Lower)
	}
	if got.Range.Upper.Value != int64(3) || !got.Range.Upper.Inclusive {
		t.Fatalf("upper=%+v, want inclusive 3", got.Range.Upper)
	}
}

func TestTranslateScalarFilterEqualityRangeOrderAndContradiction(t *testing.T) {
	cases := []struct {
		name       string
		conditions []Filter
		wantValue  int64
		wantError  bool
	}{
		{
			name: "equality then range",
			conditions: []Filter{
				{Field: "meta.priority", Operator: "==", Value: int64(2)},
				{Field: "meta.priority", Operator: ">=", Value: int64(1)},
			},
			wantValue: 2,
		},
		{
			name: "range then equality",
			conditions: []Filter{
				{Field: "meta.priority", Operator: "<=", Value: int64(3)},
				{Field: "meta.priority", Operator: "==", Value: int64(2)},
			},
			wantValue: 2,
		},
		{
			name: "equality outside range",
			conditions: []Filter{
				{Field: "meta.priority", Operator: "==", Value: int64(4)},
				{Field: "meta.priority", Operator: "<=", Value: int64(3)},
			},
			wantError: true,
		},
		{
			name: "range then equality outside",
			conditions: []Filter{
				{Field: "meta.priority", Operator: ">=", Value: int64(2)},
				{Field: "meta.priority", Operator: "==", Value: int64(1)},
			},
			wantError: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := translateScalarFilter(&Filter{Operator: "AND", Conditions: tc.conditions}, testScalarFilterSchema())
			if tc.wantError {
				if ErrorCodeOf(err) != CodeUnsupported {
					t.Fatalf("err=%v code=%s, want unsupported", err, ErrorCodeOf(err))
				}
				return
			}
			if err != nil || got == nil || got.Value != tc.wantValue {
				t.Fatalf("got=%+v err=%v, want equality %d", got, err, tc.wantValue)
			}
		})
	}
}

func TestNormalizeScalarFieldDeclarationValidatesDerivedIndexName(t *testing.T) {
	_, err := normalizeScalarFieldDeclarations([]ScalarFieldDeclaration{{Field: "meta.bad/name"}})
	if ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("err=%v code=%s, want invalid_request", err, ErrorCodeOf(err))
	}
	if err := collections.ValidateIndexName("meta_bad/name"); err == nil {
		t.Fatal("test fixture name unexpectedly accepted")
	}
}
