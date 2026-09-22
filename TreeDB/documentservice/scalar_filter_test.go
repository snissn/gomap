package documentservice

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func testScalarFilterSchema() scalarSchema {
	return newScalarSchema([]ScalarFieldInfo{
		{Field: "meta.priority", IndexName: "meta_priority", ValueType: ScalarFieldInt64},
		{Field: "meta.tenant", IndexName: "meta_tenant", ValueType: ScalarFieldString},
		{Field: "meta.workspace", IndexName: "meta_workspace", ValueType: ScalarFieldString},
	})
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

func TestTranslateScalarFilterRejectsContradictoryRangesAndKeepsInt64Exact(t *testing.T) {
	cases := []struct {
		name       string
		conditions []Filter
		wantError  bool
	}{
		{
			name: "identical equality predicates",
			conditions: []Filter{
				{Field: "meta.priority", Operator: "==", Value: int64(2)},
				{Field: "meta.priority", Operator: "==", Value: int64(2)},
			},
		},
		{
			name: "lower above upper",
			conditions: []Filter{
				{Field: "meta.priority", Operator: ">=", Value: int64(3)},
				{Field: "meta.priority", Operator: "<=", Value: int64(2)},
			},
			wantError: true,
		},
		{
			name: "equal exclusive bounds",
			conditions: []Filter{
				{Field: "meta.priority", Operator: ">", Value: int64(2)},
				{Field: "meta.priority", Operator: "<=", Value: int64(2)},
			},
			wantError: true,
		},
		{
			name: "int64 values beyond float precision",
			conditions: []Filter{
				{Field: "meta.priority", Operator: "==", Value: int64(9007199254740993)},
				{Field: "meta.priority", Operator: "<=", Value: int64(9007199254740992)},
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
			if err != nil || got == nil || got.Value != int64(2) {
				t.Fatalf("got=%+v err=%v, want equality 2", got, err)
			}
		})
	}
}

func TestTranslateScalarFilterMultiFieldANDGroupsInFirstAppearanceOrder4292(t *testing.T) {
	filter := &Filter{Operator: "AND", Conditions: []Filter{
		{Field: "meta.workspace", Operator: "==", Value: "red"},
		{Operator: "AND", Conditions: []Filter{
			{Field: "meta.priority", Operator: ">=", Value: int64(2)},
			{Field: "meta.tenant", Operator: "==", Value: "alpha"},
		}},
		{Field: "meta.priority", Operator: "<=", Value: int64(4)},
	}}
	got, err := translateScalarFilter(filter, testScalarFilterSchema())
	if err != nil {
		t.Fatalf("translateScalarFilter: %v", err)
	}
	if got == nil || len(got.And) != 3 {
		t.Fatalf("got=%+v want three field groups", got)
	}
	if got.And[0].IndexName != "meta_workspace" || got.And[0].Value != "red" {
		t.Fatalf("first predicate=%+v want workspace equality", got.And[0])
	}
	if got.And[1].IndexName != "meta_priority" || got.And[1].Range == nil || got.And[1].Range.Lower.Value != int64(2) || got.And[1].Range.Upper.Value != int64(4) {
		t.Fatalf("second predicate=%+v want merged priority range", got.And[1])
	}
	if got.And[2].IndexName != "meta_tenant" || got.And[2].Value != "alpha" {
		t.Fatalf("third predicate=%+v want tenant equality", got.And[2])
	}

	single, err := translateScalarFilter(&Filter{Field: "meta.tenant", Operator: "==", Value: "alpha"}, testScalarFilterSchema())
	if err != nil || single == nil || len(single.And) != 0 || single.IndexName != "meta_tenant" {
		t.Fatalf("single-field translation=%+v err=%v", single, err)
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
