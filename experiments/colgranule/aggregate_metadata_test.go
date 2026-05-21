package colgranule

import (
	"math"
	"strings"
	"testing"
)

func TestAggregateMetadataDefinitionExplicitShape(t *testing.T) {
	opts := partTestOptions([]SortKeyColumn{{Column: "id"}})
	opts.AggregateMetadata = []AggregateMetadataDefinition{aggregateMetadataTestDefinition()}
	part, err := BuildColumnPart(17, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {1, 2, 3, 4, 5},
		"time_us":   {10, 20, 30, 40, 50},
		"value":     {100, 200, 300, 400, 500},
		"kind_code": {0, 1, 1, 2, 0},
		"has_reply": {1, 1, 0, 1, 0},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	metadata, ok := part.AggregateMetadataByName("test_kind_time")
	if !ok {
		t.Fatal("missing aggregate metadata")
	}
	if !metadata.Stats.Admitted {
		t.Fatalf("metadata rejected: %+v", metadata.Stats)
	}
	if metadata.Definition.Version != AggregateMetadataDefinitionVersion {
		t.Fatalf("version=%d want %d", metadata.Definition.Version, AggregateMetadataDefinitionVersion)
	}
	if metadata.Definition.Scope != AggregateMetadataScopeGranule {
		t.Fatalf("scope=%q want %q", metadata.Definition.Scope, AggregateMetadataScopeGranule)
	}
	if !aggregateMetadataSingleGroupKey(metadata.Definition, "kind_code") || !aggregateMetadataHasCountMinMax(metadata.Definition, "time_us") {
		t.Fatalf("definition not normalized to explicit count/min/max shape: %+v", metadata.Definition)
	}
	if len(metadata.Definition.Predicates) != 1 || !aggregateMetadataPredicateEquals(metadata.Definition.Predicates, "has_reply", 1) {
		t.Fatalf("predicates=%+v want has_reply eq 1", metadata.Definition.Predicates)
	}
	if metadata.Stats.RowsMatched != 3 || metadata.Stats.Entries == 0 || len(metadata.Granules) != 3 {
		t.Fatalf("stats=%+v granules=%d want matched=3 entries>0 granules=3", metadata.Stats, len(metadata.Granules))
	}
}

func TestAggregateMetadataDefinitionRejectsUnsupportedShapes(t *testing.T) {
	cases := []struct {
		name string
		edit func(*AggregateMetadataDefinition)
		want string
	}{
		{
			name: "version",
			edit: func(def *AggregateMetadataDefinition) {
				def.Version = AggregateMetadataDefinitionVersion + 1
			},
			want: "version",
		},
		{
			name: "scope",
			edit: func(def *AggregateMetadataDefinition) {
				def.Scope = AggregateMetadataScope("part")
			},
			want: "scope part",
		},
		{
			name: "multi_group",
			edit: func(def *AggregateMetadataDefinition) {
				def.GroupKeys = []string{"kind_code", "has_reply"}
			},
			want: "exactly one group key",
		},
		{
			name: "group_type",
			edit: func(def *AggregateMetadataDefinition) {
				def.GroupKeys = []string{"time_us"}
			},
			want: "want low_cardinality_code",
		},
		{
			name: "predicate_op",
			edit: func(def *AggregateMetadataDefinition) {
				def.Predicates = []AggregateMetadataPredicate{{Column: "has_reply", Op: AggregateMetadataPredicateOp("range"), Value: 1}}
			},
			want: "op range is unsupported",
		},
		{
			name: "duplicate_predicate",
			edit: func(def *AggregateMetadataDefinition) {
				def.Predicates = append(def.Predicates, def.Predicates[0])
			},
			want: "duplicate predicate column",
		},
		{
			name: "missing_measure",
			edit: func(def *AggregateMetadataDefinition) {
				def.Measures = def.Measures[:2]
			},
			want: "count/min/max",
		},
		{
			name: "unsupported_measure",
			edit: func(def *AggregateMetadataDefinition) {
				def.Measures[0] = AggregateMetadataMeasure{Op: AggregateMetadataMeasureOp("sum"), Column: "time_us"}
			},
			want: "op sum is unsupported",
		},
		{
			name: "count_column",
			edit: func(def *AggregateMetadataDefinition) {
				def.Measures[0].Column = "time_us"
			},
			want: "count measure must not bind column",
		},
		{
			name: "mismatched_value_column",
			edit: func(def *AggregateMetadataDefinition) {
				def.Measures[2].Column = "value"
			},
			want: "differs from max column",
		},
		{
			name: "value_type",
			edit: func(def *AggregateMetadataDefinition) {
				def.Measures[1].Column = "kind_code"
				def.Measures[2].Column = "kind_code"
			},
			want: "want int64",
		},
		{
			name: "nan_max_bytes",
			edit: func(def *AggregateMetadataDefinition) {
				def.MaxBytesPerRow = math.NaN()
			},
			want: "not finite",
		},
		{
			name: "inf_max_bytes",
			edit: func(def *AggregateMetadataDefinition) {
				def.MaxBytesPerRow = math.Inf(1)
			},
			want: "not finite",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := partTestOptions([]SortKeyColumn{{Column: "id"}})
			def := aggregateMetadataTestDefinition()
			tc.edit(&def)
			opts.AggregateMetadata = []AggregateMetadataDefinition{def}
			_, err := NewColumnPartBuilder(opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NewColumnPartBuilder err=%v want substring %q", err, tc.want)
			}
		})
	}
}

func aggregateMetadataTestDefinition() AggregateMetadataDefinition {
	return AggregateMetadataDefinition{
		Name:      "test_kind_time",
		Version:   AggregateMetadataDefinitionVersion,
		Kind:      AggregateMetadataGroupMinMax,
		Scope:     AggregateMetadataScopeGranule,
		GroupKeys: []string{"kind_code"},
		Measures: []AggregateMetadataMeasure{
			{Op: AggregateMetadataMeasureCount},
			{Op: AggregateMetadataMeasureMin, Column: "time_us"},
			{Op: AggregateMetadataMeasureMax, Column: "time_us"},
		},
		Predicates: []AggregateMetadataPredicate{
			{Column: "has_reply", Op: AggregateMetadataPredicateEq, Value: 1},
		},
		MaxBytesPerRow: 256,
	}
}
