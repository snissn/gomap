package collections

import (
	"slices"
	"strings"
	"testing"
)

func TestColumnQueryPlannerM11BChoosesExpectedKindsForOneFixture(t *testing.T) {
	catalog := &collectionCatalog{
		meta: CollectionMeta{
			Name: "events",
			Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{
					{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
					{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString},
					{Name: "did", Path: "did", ValueType: ColumnStoreValueString},
				},
				SortKey: []ColumnSortKey{{Column: "time_us"}},
				AggregateMetadata: []ColumnAggregateMetadata{
					{Name: "q5_did_time_span", Column: "time_us", Kind: ColumnAggregateMin},
				},
			}},
			Indexes: []IndexDefinition{
				{Name: "kind_idx", Field: "kind", ValueType: IndexValueString},
				{Name: "time_us_idx", Field: "time_us", ValueType: IndexValueInt64},
				{Name: "did_idx", Field: "did", ValueType: IndexValueString},
			},
		},
	}
	identity := ColumnStoreCacheIdentity{
		Collection:                      "events",
		ManifestRoot:                    99,
		ManifestGeneration:              7,
		RecoveryAuthoritativeGeneration: 7,
	}
	base := ColumnQueryPlanRequest{
		Name:                  "q5",
		ProjectedColumns:      []string{"time_us", "kind", "did"},
		CandidateIndexColumns: []string{"kind"},
		EstimatedRows:         100_000,
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:       true,
			AggregateMetadata:      true,
			ParallelColumnScan:     true,
			PhysicalAssetCount:     8,
			PartCount:              4,
			GranuleCount:           128,
			MaxParallelWorkers:     4,
			PlannerCandidateBudget: 5,
		},
	}

	tests := []struct {
		name  string
		req   ColumnQueryPlanRequest
		want  ColumnQueryPlanKind
		index string
	}{
		{
			name: "row fallback",
			req: ColumnQueryPlanRequest{
				Name:             "q1",
				ProjectedColumns: []string{"time_us", "kind", "did"},
			},
			want: ColumnQueryPlanRowStoreBaseline,
		},
		{
			name: "b tree index",
			req: func() ColumnQueryPlanRequest {
				req := base
				req.ForceKind = ColumnQueryPlanBTreeIndexBaseline
				return req
			}(),
			want:  ColumnQueryPlanBTreeIndexBaseline,
			index: "kind_idx",
		},
		{
			name: "serial column scan",
			req: func() ColumnQueryPlanRequest {
				req := base
				req.ForceKind = ColumnQueryPlanSerialColumnScan
				return req
			}(),
			want: ColumnQueryPlanSerialColumnScan,
		},
		{
			name: "aggregate metadata",
			req: func() ColumnQueryPlanRequest {
				req := base
				req.ForceKind = ColumnQueryPlanAggregateMetadata
				req.AggregateMetadataName = "q5_did_time_span"
				return req
			}(),
			want: ColumnQueryPlanAggregateMetadata,
		},
		{
			name: "parallel column scan",
			req: func() ColumnQueryPlanRequest {
				req := base
				req.ForceKind = ColumnQueryPlanParallelColumnScan
				return req
			}(),
			want: ColumnQueryPlanParallelColumnScan,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan := planColumnQueryForCatalog(catalog, identity, true, tc.req)
			if !plan.Supported {
				t.Fatalf("plan unsupported: %+v", plan.Diagnostics)
			}
			if plan.Kind != tc.want {
				t.Fatalf("plan kind=%q want %q diagnostics=%+v", plan.Kind, tc.want, plan.Diagnostics)
			}
			if tc.index != "" && plan.IndexName != tc.index {
				t.Fatalf("index=%q want %q", plan.IndexName, tc.index)
			}
			if !plan.Diagnostics.RecoveryAuthoritative && plan.Kind != ColumnQueryPlanRowStoreBaseline && plan.Kind != ColumnQueryPlanBTreeIndexBaseline {
				t.Fatalf("physical plan did not record recovery-authoritative manifest: %+v", plan.Diagnostics)
			}
		})
	}
}

func TestColumnQueryPlannerM11BMatchesBTreeIndexesCaseSensitively(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name: "events",
		Indexes: []IndexDefinition{
			{Name: "kind_idx", Field: "kind", ValueType: IndexValueString},
		},
	}}
	identity := ColumnStoreCacheIdentity{
		Collection:                      "events",
		ManifestRoot:                    99,
		ManifestGeneration:              7,
		RecoveryAuthoritativeGeneration: 7,
	}
	req := ColumnQueryPlanRequest{
		Name:                  "q1",
		CandidateIndexColumns: []string{"KIND"},
		ForceKind:             ColumnQueryPlanBTreeIndexBaseline,
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if plan.Supported {
		t.Fatalf("case-mismatched candidate selected index: %+v", plan)
	}
	if !strings.Contains(plan.Diagnostics.UnsupportedPlanReason, "no matching collection secondary index") {
		t.Fatalf("unsupported reason=%q", plan.Diagnostics.UnsupportedPlanReason)
	}

	req.CandidateIndexColumns = []string{"kind"}
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.IndexName != "kind_idx" {
		t.Fatalf("exact-case candidate did not select index: %+v", plan)
	}
}

func TestColumnQueryPlannerM11BDoesNotMatchPredicateColumnsAgainstIndexNames(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name: "events",
		Indexes: []IndexDefinition{
			{Name: "kind", Field: "category", ValueType: IndexValueString},
		},
	}}
	identity := ColumnStoreCacheIdentity{
		Collection:                      "events",
		ManifestRoot:                    99,
		ManifestGeneration:              7,
		RecoveryAuthoritativeGeneration: 7,
	}
	req := ColumnQueryPlanRequest{
		Name:       "q1",
		Predicates: []ColumnQueryPredicate{{Column: "kind", Operator: ColumnQueryPredicateEqual}},
		ForceKind:  ColumnQueryPlanBTreeIndexBaseline,
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if plan.Supported {
		t.Fatalf("predicate column matched unrelated index name: %+v", plan)
	}

	req.CandidateIndexColumns = []string{"kind"}
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.IndexName != "kind" {
		t.Fatalf("explicit candidate did not match index name: %+v", plan)
	}

	req.CandidateIndexColumns = nil
	catalog.meta.Indexes = append(catalog.meta.Indexes, IndexDefinition{Name: "kind_idx", Field: "kind", ValueType: IndexValueString})
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.IndexName != "kind_idx" {
		t.Fatalf("predicate column did not match index field: %+v", plan)
	}
}

func TestColumnQueryPlannerM11BRejectsNonRecoveryAuthoritativeManifest(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{Enabled: true}},
	}}
	identity := ColumnStoreCacheIdentity{
		Collection:                      "events",
		ManifestRoot:                    99,
		ManifestGeneration:              8,
		RecoveryAuthoritativeGeneration: 7,
	}
	req := ColumnQueryPlanRequest{
		Name:      "q1",
		ForceKind: ColumnQueryPlanSerialColumnScan,
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			PhysicalAssetCount: 1,
			GranuleCount:       1,
		},
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if plan.Supported {
		t.Fatalf("expected non-recovery-authoritative manifest to fail closed: %+v", plan)
	}
	if plan.Kind != ColumnQueryPlanSerialColumnScan {
		t.Fatalf("kind=%q want serial", plan.Kind)
	}
	if !strings.Contains(plan.Diagnostics.UnsupportedPlanReason, "recovery-authoritative") {
		t.Fatalf("unsupported reason=%q", plan.Diagnostics.UnsupportedPlanReason)
	}
}

func TestColumnQueryPlannerM11BDoesNotReportPhysicalGranulesForRowOrIndexPlans(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name: "events",
		Indexes: []IndexDefinition{
			{Name: "kind_idx", Field: "kind", ValueType: IndexValueString},
		},
	}}
	identity := ColumnStoreCacheIdentity{
		Collection:                      "events",
		ManifestRoot:                    99,
		ManifestGeneration:              7,
		RecoveryAuthoritativeGeneration: 7,
	}
	base := ColumnQueryPlanRequest{
		Name:                  "q1",
		CandidateIndexColumns: []string{"kind"},
		Capabilities: ColumnQueryPlannerCapabilities{
			GranuleCount:       128,
			MaxParallelWorkers: 4,
		},
	}

	rowReq := base
	rowReq.ForceKind = ColumnQueryPlanRowStoreBaseline
	rowPlan := planColumnQueryForCatalog(catalog, identity, true, rowReq)
	if rowPlan.Diagnostics.ScheduledGranules != 0 || rowPlan.Diagnostics.WorkerCount != 0 {
		t.Fatalf("row plan reported physical execution counters: %+v", rowPlan.Diagnostics)
	}

	indexReq := base
	indexReq.ForceKind = ColumnQueryPlanBTreeIndexBaseline
	indexPlan := planColumnQueryForCatalog(catalog, identity, true, indexReq)
	if indexPlan.Diagnostics.ScheduledGranules != 0 || indexPlan.Diagnostics.WorkerCount != 0 {
		t.Fatalf("B-tree plan reported physical execution counters: %+v", indexPlan.Diagnostics)
	}
}

func TestColumnQueryPlannerM11BRejectsUnknownAggregateMetadata(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name: "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true,
			AggregateMetadata: []ColumnAggregateMetadata{
				{Name: "known_span", Column: "time_us", Kind: ColumnAggregateMin},
			},
		}},
	}}
	identity := ColumnStoreCacheIdentity{
		Collection:                      "events",
		ManifestRoot:                    99,
		ManifestGeneration:              7,
		RecoveryAuthoritativeGeneration: 7,
	}
	req := ColumnQueryPlanRequest{
		Name:                  "q5_metadata",
		AggregateMetadataName: "missing_span",
		ForceKind:             ColumnQueryPlanAggregateMetadata,
		Capabilities: ColumnQueryPlannerCapabilities{
			AggregateMetadata:  true,
			PhysicalAssetCount: 1,
			GranuleCount:       8,
		},
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if plan.Supported {
		t.Fatalf("expected unknown aggregate metadata to fail closed: %+v", plan)
	}
	if !strings.Contains(plan.Diagnostics.UnsupportedPlanReason, "unknown aggregate metadata") {
		t.Fatalf("unsupported reason=%q", plan.Diagnostics.UnsupportedPlanReason)
	}
}

func TestColumnQueryPlannerM11BMatchesAggregateMetadataNamesExactly(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name: "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true,
			AggregateMetadata: []ColumnAggregateMetadata{
				{Name: "q5_did_time_span", Column: "time_us", Kind: ColumnAggregateMin},
			},
		}},
	}}
	identity := ColumnStoreCacheIdentity{
		Collection:                      "events",
		ManifestRoot:                    99,
		ManifestGeneration:              7,
		RecoveryAuthoritativeGeneration: 7,
	}
	req := ColumnQueryPlanRequest{
		Name:                  "q5_metadata",
		AggregateMetadataName: "Q5_DID_TIME_SPAN",
		ForceKind:             ColumnQueryPlanAggregateMetadata,
		Capabilities: ColumnQueryPlannerCapabilities{
			AggregateMetadata:  true,
			PhysicalAssetCount: 1,
			GranuleCount:       8,
		},
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if plan.Supported {
		t.Fatalf("case-mismatched aggregate metadata name matched catalog: %+v", plan)
	}
	if !strings.Contains(plan.Diagnostics.UnsupportedPlanReason, "unknown aggregate metadata") {
		t.Fatalf("unsupported reason=%q", plan.Diagnostics.UnsupportedPlanReason)
	}

	req.AggregateMetadataName = " q5_did_time_span "
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.Kind != ColumnQueryPlanAggregateMetadata {
		t.Fatalf("exact trimmed aggregate metadata name did not match catalog: %+v", plan)
	}
}

func TestColumnSkipScanM11BPrunesOnlyLeftPrefixMarks(t *testing.T) {
	marks := []ColumnSkipScanMark{
		{Name: "before", Rows: 10, MinKeys: [][]byte{{0x01}}, MaxKeys: [][]byte{{0x09}}},
		{Name: "overlap", Rows: 10, MinKeys: [][]byte{{0x10}}, MaxKeys: [][]byte{{0x20}}},
		{Name: "after", Rows: 10, MinKeys: [][]byte{{0x30}}, MaxKeys: [][]byte{{0x40}}},
	}
	result := PlanColumnSkipScan([]ColumnSkipScanPredicate{{
		Position: 0,
		Lower:    ColumnSkipScanBound{Key: []byte{0x10}, Inclusive: true},
		Upper:    ColumnSkipScanBound{Key: []byte{0x35}, Inclusive: true},
	}}, marks)
	if got, want := result.LeftPrefixColumns, 1; got != want {
		t.Fatalf("left prefix=%d want %d", got, want)
	}
	if got, want := result.ScheduledMarks, []int{1, 2}; !equalInts(got, want) {
		t.Fatalf("scheduled=%v want %v", got, want)
	}
	if got, want := result.SkippedMarks, []int{0}; !equalInts(got, want) {
		t.Fatalf("skipped=%v want %v", got, want)
	}

	nonLeftPrefix := PlanColumnSkipScan([]ColumnSkipScanPredicate{{
		Position: 1,
		Lower:    ColumnSkipScanBound{Key: []byte{0x99}, Inclusive: true},
		Upper:    ColumnSkipScanBound{Key: []byte{0xaa}, Inclusive: true},
	}}, marks)
	if got, want := nonLeftPrefix.LeftPrefixColumns, 0; got != want {
		t.Fatalf("non-left-prefix columns=%d want %d", got, want)
	}
	if got, want := nonLeftPrefix.ScheduledMarks, []int{0, 1, 2}; !equalInts(got, want) {
		t.Fatalf("non-left-prefix scheduled=%v want %v", got, want)
	}
	if len(nonLeftPrefix.SkippedMarks) != 0 {
		t.Fatalf("non-left-prefix skipped=%v want none", nonLeftPrefix.SkippedMarks)
	}

	unboundedFirstColumn := PlanColumnSkipScan([]ColumnSkipScanPredicate{
		{
			Position: 0,
			Lower:    ColumnSkipScanBound{Unbounded: true},
			Upper:    ColumnSkipScanBound{Unbounded: true},
		},
		{
			Position: 1,
			Lower:    ColumnSkipScanBound{Key: []byte{0x99}, Inclusive: true},
			Upper:    ColumnSkipScanBound{Key: []byte{0xaa}, Inclusive: true},
		},
	}, marks)
	if got, want := unboundedFirstColumn.LeftPrefixColumns, 0; got != want {
		t.Fatalf("unbounded-first left prefix=%d want %d", got, want)
	}
	if got, want := unboundedFirstColumn.ScheduledMarks, []int{0, 1, 2}; !equalInts(got, want) {
		t.Fatalf("unbounded-first scheduled=%v want %v", got, want)
	}
	if len(unboundedFirstColumn.SkippedMarks) != 0 {
		t.Fatalf("unbounded-first skipped=%v want none", unboundedFirstColumn.SkippedMarks)
	}
}

func TestColumnSkipScanIntoM11BReusesScratchWithoutAllocating(t *testing.T) {
	marks := []ColumnSkipScanMark{
		{Name: "before", Rows: 10, MinKeys: [][]byte{{0x01}, {0x01}}, MaxKeys: [][]byte{{0x09}, {0x09}}},
		{Name: "inside", Rows: 10, MinKeys: [][]byte{{0x10}, {0x10}}, MaxKeys: [][]byte{{0x20}, {0x20}}},
		{Name: "after", Rows: 10, MinKeys: [][]byte{{0x30}, {0x30}}, MaxKeys: [][]byte{{0x40}, {0x40}}},
	}
	predicates := []ColumnSkipScanPredicate{
		{Position: 1, Lower: ColumnSkipScanBound{Key: []byte{0x10}, Inclusive: true}},
		{Position: 0, Lower: ColumnSkipScanBound{Key: []byte{0x10}, Inclusive: true}, Upper: ColumnSkipScanBound{Key: []byte{0x35}, Inclusive: true}},
	}
	result := ColumnSkipScanResult{
		ScheduledMarks: make([]int, 0, len(marks)),
		SkippedMarks:   make([]int, 0, len(marks)),
	}
	PlanColumnSkipScanInto(&result, predicates, marks)
	if got, want := result.LeftPrefixColumns, 2; got != want {
		t.Fatalf("left prefix=%d want %d", got, want)
	}
	if got, want := result.ScheduledMarks, []int{1, 2}; !equalInts(got, want) {
		t.Fatalf("scheduled=%v want %v", got, want)
	}
	if got, want := result.SkippedMarks, []int{0}; !equalInts(got, want) {
		t.Fatalf("skipped=%v want %v", got, want)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		PlanColumnSkipScanInto(&result, predicates, marks)
	})
	if allocs != 0 {
		t.Fatalf("PlanColumnSkipScanInto allocations=%f want 0", allocs)
	}
}

func equalInts(left, right []int) bool {
	return slices.Equal(left, right)
}
