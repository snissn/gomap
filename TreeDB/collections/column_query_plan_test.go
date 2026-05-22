package collections

import (
	"fmt"
	"runtime"
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
					{Name: "q5_did_time_span", Column: "time_us", GroupColumn: "did", Kind: ColumnAggregateMin},
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
			if tc.want == ColumnQueryPlanBTreeIndexBaseline && !strings.Contains(plan.Diagnostics.Reason, "full-scan B-tree baseline") {
				t.Fatalf("B-tree reason=%q want full-scan baseline disclosure", plan.Diagnostics.Reason)
			}
			if !plan.Diagnostics.RecoveryAuthoritative && plan.Kind != ColumnQueryPlanRowStoreBaseline && plan.Kind != ColumnQueryPlanBTreeIndexBaseline {
				t.Fatalf("physical plan did not record recovery-authoritative manifest: %+v", plan.Diagnostics)
			}
			if tc.want == ColumnQueryPlanAggregateMetadata {
				if plan.Diagnostics.ScheduledGranules != base.Capabilities.GranuleCount || plan.Diagnostics.WorkerCount != 1 {
					t.Fatalf("aggregate metadata diagnostics=%+v want granules=%d worker=1", plan.Diagnostics, base.Capabilities.GranuleCount)
				}
				if !strings.Contains(plan.Diagnostics.Reason, "metadata asset") {
					t.Fatalf("aggregate metadata reason=%q want metadata asset disclosure", plan.Diagnostics.Reason)
				}
			}
		})
	}
}

func TestColumnQueryPlannerM11BForcedPlanCountsSingleCandidate(t *testing.T) {
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
		CandidateIndexColumns: []string{"kind"},
		ForceKind:             ColumnQueryPlanBTreeIndexBaseline,
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			AggregateMetadata:  true,
			ParallelColumnScan: true,
		},
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported {
		t.Fatalf("plan unsupported: %+v", plan.Diagnostics)
	}
	if got, want := plan.Diagnostics.CandidatePlans, 1; got != want {
		t.Fatalf("candidate plans=%d want %d for forced plan diagnostics", got, want)
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

	catalog.meta.Indexes[0].Field = " kind "
	req.CandidateIndexColumns = []string{" kind "}
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.IndexName != "kind_idx" {
		t.Fatalf("trimmed candidate/catalog field did not select index: %+v", plan)
	}
}

func TestColumnQueryPlannerM11BMatchesBTreeIndexFieldsOnly(t *testing.T) {
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
	if plan.Supported {
		t.Fatalf("candidate column matched unrelated index name: %+v", plan)
	}

	req.CandidateIndexColumns = []string{"kind"}
	catalog.meta.Indexes = append(catalog.meta.Indexes, IndexDefinition{Name: "kind_idx", Field: "kind", ValueType: IndexValueString})
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.IndexName != "kind_idx" {
		t.Fatalf("candidate column did not match index field: %+v", plan)
	}
}

func TestColumnQueryPlannerM11BPredicateOperatorOrdersIndexCandidates(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name: "events",
		Indexes: []IndexDefinition{
			{Name: "time_us_idx", Field: "time_us", ValueType: IndexValueInt64},
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
		Name: "q1",
		Predicates: []ColumnQueryPredicate{
			{Column: "time_us", Operator: ColumnQueryPredicateGreaterOrEqual},
			{Column: "kind", Operator: ColumnQueryPredicateEqual},
		},
		ForceKind: ColumnQueryPlanBTreeIndexBaseline,
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.IndexName != "kind_idx" {
		t.Fatalf("equality predicate should win before range predicate: %+v", plan)
	}

	req.Predicates = []ColumnQueryPredicate{{Column: "time_us", Operator: ColumnQueryPredicateGreaterThan}}
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.IndexName != "time_us_idx" {
		t.Fatalf("range predicate should select matching index: %+v", plan)
	}

	req.Predicates = []ColumnQueryPredicate{{Column: "kind", Operator: ColumnQueryPredicateOperator("contains")}}
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if plan.Supported {
		t.Fatalf("unknown predicate operator should not silently select index: %+v", plan)
	}
	if !strings.Contains(plan.Diagnostics.UnsupportedPlanReason, "no matching collection secondary index") {
		t.Fatalf("unsupported reason=%q", plan.Diagnostics.UnsupportedPlanReason)
	}

	req.Predicates = []ColumnQueryPredicate{{Column: "kind", Operator: ColumnQueryPredicateEqual}}
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.IndexName != "kind_idx" {
		t.Fatalf("known predicate operator did not select B-tree baseline: %+v", plan)
	}
}

func TestColumnQueryPlannerM11BCountsPredicateIndexCandidates(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name: "events",
		Indexes: []IndexDefinition{
			{Name: "time_us_idx", Field: "time_us", ValueType: IndexValueInt64},
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
		CandidateIndexColumns: []string{"kind", "missing", " kind "},
		Predicates: []ColumnQueryPredicate{
			{Column: "kind", Operator: ColumnQueryPredicateEqual},
			{Column: "time_us", Operator: ColumnQueryPredicateGreaterOrEqual},
			{Column: "missing_predicate", Operator: ColumnQueryPredicateEqual},
			{Column: "time_us", Operator: ColumnQueryPredicateOperator("contains")},
		},
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.Kind != ColumnQueryPlanBTreeIndexBaseline {
		t.Fatalf("predicate-driven B-tree plan not selected: %+v", plan)
	}
	if got, want := plan.Diagnostics.CandidatePlans, 3; got != want {
		t.Fatalf("candidate plans=%d want %d (row fallback + distinct kind/time_us B-tree candidates)", got, want)
	}

	req.CandidateIndexColumns = nil
	req.Predicates = []ColumnQueryPredicate{{Column: "time_us", Operator: ColumnQueryPredicateGreaterOrEqual}}
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.Kind != ColumnQueryPlanBTreeIndexBaseline || plan.IndexField != "time_us" {
		t.Fatalf("predicate-only index plan not selected: %+v", plan)
	}
	if got, want := plan.Diagnostics.CandidatePlans, 2; got != want {
		t.Fatalf("predicate-only candidate plans=%d want %d (row fallback + time_us B-tree candidate)", got, want)
	}
}

func TestColumnQueryPlannerM11BCountsOnlyRequestFeasibleCandidates(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name: "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true,
			Columns: []ColumnStoreColumn{
				{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString},
				{Name: "did", Path: "did", ValueType: ColumnStoreValueString},
				{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
			},
			AggregateMetadata: []ColumnAggregateMetadata{
				{Name: "q5_did_time_span", Column: "time_us", GroupColumn: "did", Kind: ColumnAggregateMin},
			},
		}},
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
		CandidateIndexColumns: []string{"kind"},
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			AggregateMetadata:  true,
			ParallelColumnScan: true,
			PartCount:          2,
			GranuleCount:       2,
			MaxParallelWorkers: 2,
		},
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if got, want := plan.Diagnostics.CandidatePlans, 2; got != want {
		t.Fatalf("no-assets candidate plans=%d want %d (row fallback + B-tree)", got, want)
	}

	req.Capabilities.PhysicalAssetCount = 1
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if got, want := plan.Diagnostics.CandidatePlans, 4; got != want {
		t.Fatalf("no-aggregate-request candidate plans=%d want %d (row + B-tree + serial + parallel)", got, want)
	}

	req.AggregateMetadataName = "q5_did_time_span"
	req.Capabilities.PlannerCandidateBudget = 3
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if got, want := plan.Diagnostics.CandidatePlans, 3; got != want {
		t.Fatalf("budgeted candidate plans=%d want %d", got, want)
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

func TestColumnQueryPlannerM11BRejectsPhysicalPlansWithoutColumnStoreConfig(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{Name: "events"}}
	identity := ColumnStoreCacheIdentity{
		Collection:                      "events",
		ManifestRoot:                    99,
		ManifestGeneration:              7,
		RecoveryAuthoritativeGeneration: 7,
	}
	req := ColumnQueryPlanRequest{
		Name:             "q1",
		ProjectedColumns: []string{"time_us"},
		Predicates:       []ColumnQueryPredicate{{Column: "kind"}},
		ForceKind:        ColumnQueryPlanSerialColumnScan,
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			PhysicalAssetCount: 1,
			GranuleCount:       8,
		},
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if plan.Supported {
		t.Fatalf("expected non-column collection to fail physical plan: %+v", plan)
	}
	if got, want := plan.Diagnostics.UnsupportedPlanReason, "collection has no enabled column store"; got != want {
		t.Fatalf("unsupported reason=%q want %q", got, want)
	}

	req.ForceKind = ""
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.Kind != ColumnQueryPlanRowStoreBaseline {
		t.Fatalf("non-column collection should fall back to row baseline: %+v", plan)
	}
	if got := plan.Diagnostics.UnsupportedPlanReason; got != "" {
		t.Fatalf("fallback should not report requested columns as undeclared without column-store config, got %q", got)
	}
}

func TestColumnQueryPlannerM11BReportsParallelShapeReason(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{Enabled: true}},
	}}
	identity := ColumnStoreCacheIdentity{
		Collection:                      "events",
		ManifestRoot:                    99,
		ManifestGeneration:              7,
		RecoveryAuthoritativeGeneration: 7,
	}
	req := ColumnQueryPlanRequest{
		Name:      "q1",
		ForceKind: ColumnQueryPlanParallelColumnScan,
		Capabilities: ColumnQueryPlannerCapabilities{
			ParallelColumnScan: true,
			PhysicalAssetCount: 1,
			PartCount:          1,
			GranuleCount:       1,
			MaxParallelWorkers: 4,
		},
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if plan.Supported {
		t.Fatalf("expected undersized parallel shape to fail closed: %+v", plan)
	}
	if !strings.Contains(plan.Diagnostics.UnsupportedPlanReason, "more than one available granule or part") {
		t.Fatalf("unsupported reason=%q", plan.Diagnostics.UnsupportedPlanReason)
	}

	req.Capabilities.GranuleCount = 2
	req.Capabilities.MaxParallelWorkers = 1
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if plan.Supported {
		t.Fatalf("expected single-worker parallel shape to fail closed: %+v", plan)
	}
	if !strings.Contains(plan.Diagnostics.UnsupportedPlanReason, "at least two workers") {
		t.Fatalf("single-worker unsupported reason=%q", plan.Diagnostics.UnsupportedPlanReason)
	}
}

func TestColumnQueryPlannerM11BClampsParallelWorkersToAvailableWork(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{Enabled: true}},
	}}
	identity := ColumnStoreCacheIdentity{
		Collection:                      "events",
		ManifestRoot:                    99,
		ManifestGeneration:              7,
		RecoveryAuthoritativeGeneration: 7,
	}
	req := ColumnQueryPlanRequest{
		Name:      "q1",
		ForceKind: ColumnQueryPlanParallelColumnScan,
		Capabilities: ColumnQueryPlannerCapabilities{
			ParallelColumnScan: true,
			PhysicalAssetCount: 1,
			PartCount:          8,
			GranuleCount:       2,
			MaxParallelWorkers: 16,
		},
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported {
		t.Fatalf("parallel plan unsupported: %+v", plan.Diagnostics)
	}
	if got, want := plan.Diagnostics.WorkerCount, 2; got != want {
		t.Fatalf("parallel worker count=%d want %d", got, want)
	}
	if got, want := plan.Diagnostics.ScheduledGranules, 2; got != want {
		t.Fatalf("parallel scheduled granules=%d want %d", got, want)
	}
}

func TestColumnQueryPlannerM11BRecoveryAuthoritativeDoesNotRequireManifestRoot(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{Enabled: true}},
	}}
	identity := ColumnStoreCacheIdentity{
		Collection:                      "events",
		ManifestGeneration:              7,
		RecoveryAuthoritativeGeneration: 7,
	}
	req := ColumnQueryPlanRequest{
		Name:      "q1",
		ForceKind: ColumnQueryPlanSerialColumnScan,
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			PhysicalAssetCount: 0,
			GranuleCount:       1,
		},
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if plan.Supported {
		t.Fatalf("expected no-assets manifest to fail closed: %+v", plan)
	}
	if !plan.Diagnostics.RecoveryAuthoritative {
		t.Fatalf("zero manifest root should not hide recovery-authoritative generation match: %+v", plan.Diagnostics)
	}
	if got, want := plan.Diagnostics.UnsupportedPlanReason, columnQueryUnsupportedNoPhysicalAssetsReason; got != want {
		t.Fatalf("unsupported reason=%q want %q", got, want)
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

func TestColumnQueryPlannerM11BForcedSourceBaselinesClearPhysicalDiagnostics(t *testing.T) {
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
		ProjectedColumns:      []string{"missing_column"},
		Predicates:            []ColumnQueryPredicate{{Column: "missing_predicate"}},
		CandidateIndexColumns: []string{"kind"},
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			PhysicalAssetCount: 1,
			GranuleCount:       128,
		},
	}

	rowReq := base
	rowReq.ForceKind = ColumnQueryPlanRowStoreBaseline
	rowPlan := planColumnQueryForCatalog(catalog, identity, true, rowReq)
	if !rowPlan.Supported || rowPlan.Kind != ColumnQueryPlanRowStoreBaseline {
		t.Fatalf("forced row baseline should remain supported without physical columns: %+v", rowPlan)
	}
	if rowPlan.Diagnostics.UnsupportedPlanKind != "" || rowPlan.Diagnostics.UnsupportedPlanReason != "" {
		t.Fatalf("forced row baseline carried physical unsupported diagnostics: %+v", rowPlan.Diagnostics)
	}

	indexReq := base
	indexReq.ForceKind = ColumnQueryPlanBTreeIndexBaseline
	indexPlan := planColumnQueryForCatalog(catalog, identity, true, indexReq)
	if !indexPlan.Supported || indexPlan.Kind != ColumnQueryPlanBTreeIndexBaseline || indexPlan.IndexName != "kind_idx" {
		t.Fatalf("forced B-tree baseline should remain supported without physical columns: %+v", indexPlan)
	}
	if indexPlan.Diagnostics.UnsupportedPlanKind != "" || indexPlan.Diagnostics.UnsupportedPlanReason != "" {
		t.Fatalf("forced B-tree baseline carried physical unsupported diagnostics: %+v", indexPlan.Diagnostics)
	}
}

func TestColumnQueryPlannerM11BSourceFallbacksClearPhysicalDiagnostics(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name: "events",
		Indexes: []IndexDefinition{
			{Name: "kind_idx", Field: "kind", ValueType: IndexValueString},
		},
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true,
			Columns: []ColumnStoreColumn{
				{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString},
			},
		}},
	}}
	identity := ColumnStoreCacheIdentity{
		Collection:                      "events",
		ManifestRoot:                    99,
		ManifestGeneration:              7,
		RecoveryAuthoritativeGeneration: 7,
	}
	base := ColumnQueryPlanRequest{
		Name:             "q1",
		ProjectedColumns: []string{"missing_column"},
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			PhysicalAssetCount: 1,
			GranuleCount:       128,
		},
	}

	btreeReq := base
	btreeReq.CandidateIndexColumns = []string{"kind"}
	btreePlan := planColumnQueryForCatalog(catalog, identity, true, btreeReq)
	if !btreePlan.Supported || btreePlan.Kind != ColumnQueryPlanBTreeIndexBaseline || btreePlan.IndexName != "kind_idx" {
		t.Fatalf("B-tree source fallback should remain supported without requested physical columns: %+v", btreePlan)
	}
	if btreePlan.Diagnostics.UnsupportedPlanKind != "" || btreePlan.Diagnostics.UnsupportedPlanReason != "" {
		t.Fatalf("B-tree source fallback carried physical unsupported diagnostics: %+v", btreePlan.Diagnostics)
	}

	rowPlan := planColumnQueryForCatalog(catalog, identity, true, base)
	if !rowPlan.Supported || rowPlan.Kind != ColumnQueryPlanRowStoreBaseline {
		t.Fatalf("row source fallback should remain supported without requested physical columns: %+v", rowPlan)
	}
	if rowPlan.Diagnostics.UnsupportedPlanKind != "" || rowPlan.Diagnostics.UnsupportedPlanReason != "" {
		t.Fatalf("row source fallback carried physical unsupported diagnostics: %+v", rowPlan.Diagnostics)
	}
}

func TestColumnQueryPlannerM11BReportsSerialWorkerCount(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{Enabled: true}},
	}}
	identity := ColumnStoreCacheIdentity{
		Collection:                      "events",
		ManifestRoot:                    99,
		ManifestGeneration:              7,
		RecoveryAuthoritativeGeneration: 7,
	}
	req := ColumnQueryPlanRequest{
		Name:      "q1",
		ForceKind: ColumnQueryPlanSerialColumnScan,
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			PhysicalAssetCount: 1,
			GranuleCount:       8,
			MaxParallelWorkers: 0,
		},
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported {
		t.Fatalf("serial plan unsupported: %+v", plan.Diagnostics)
	}
	if got, want := plan.Diagnostics.WorkerCount, 1; got != want {
		t.Fatalf("serial worker count=%d want %d", got, want)
	}
	if got, want := plan.Diagnostics.ScheduledGranules, 8; got != want {
		t.Fatalf("serial scheduled granules=%d want %d", got, want)
	}
}

func TestColumnQueryPlannerM11BRejectsMissingProjectedColumnForPhysicalPlans(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name: "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true,
			Columns: []ColumnStoreColumn{
				{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
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
		Name:             "q1",
		ProjectedColumns: []string{"time_us", "missing"},
		ForceKind:        ColumnQueryPlanSerialColumnScan,
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			PhysicalAssetCount: 1,
			GranuleCount:       8,
		},
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if plan.Supported {
		t.Fatalf("expected missing projected column to fail physical plan: %+v", plan)
	}
	if !strings.Contains(plan.Diagnostics.UnsupportedPlanReason, `requested column "missing"`) {
		t.Fatalf("unsupported reason=%q", plan.Diagnostics.UnsupportedPlanReason)
	}

	req.ProjectedColumns = []string{" time_us "}
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.Kind != ColumnQueryPlanSerialColumnScan {
		t.Fatalf("trimmed projected column should match declared physical column: %+v", plan)
	}

	req.ProjectedColumns = []string{"time_us"}
	req.Predicates = []ColumnQueryPredicate{{Column: "missing_predicate"}}
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if plan.Supported {
		t.Fatalf("expected missing predicate column to fail physical plan: %+v", plan)
	}
	if !strings.Contains(plan.Diagnostics.UnsupportedPlanReason, `requested column "missing_predicate"`) {
		t.Fatalf("unsupported reason=%q", plan.Diagnostics.UnsupportedPlanReason)
	}

	req.ForceKind = ""
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.Kind != ColumnQueryPlanRowStoreBaseline {
		t.Fatalf("non-forced missing predicate should fall back to row store: %+v", plan)
	}
	if plan.Diagnostics.UnsupportedPlanKind != "" || plan.Diagnostics.UnsupportedPlanReason != "" {
		t.Fatalf("row fallback carried physical unsupported diagnostics: %+v", plan.Diagnostics)
	}

	req.Predicates = nil
	req.ForceKind = ColumnQueryPlanRowStoreBaseline
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.Kind != ColumnQueryPlanRowStoreBaseline {
		t.Fatalf("row baseline should not enforce physical projection gate: %+v", plan)
	}

	req.ForceKind = ""
	req.ProjectedColumns = []string{"time_us", "missing"}
	req.Capabilities.SerialColumnScan = true
	req.Capabilities.ParallelColumnScan = true
	req.Capabilities.PartCount = 4
	req.Capabilities.GranuleCount = 8
	req.Capabilities.MaxParallelWorkers = 4
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !plan.Supported || plan.Kind != ColumnQueryPlanRowStoreBaseline {
		t.Fatalf("missing physical column should fall back to row baseline when not forced: %+v", plan)
	}
	if plan.Diagnostics.UnsupportedPlanKind != "" || plan.Diagnostics.UnsupportedPlanReason != "" {
		t.Fatalf("row fallback carried physical unsupported diagnostics: %+v", plan.Diagnostics)
	}
}

func TestColumnQueryPlannerM11BRejectsUnknownAggregateMetadata(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name: "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true,
			Columns: []ColumnStoreColumn{
				{Name: "did", Path: "did", ValueType: ColumnStoreValueString},
				{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
			},
			AggregateMetadata: []ColumnAggregateMetadata{
				{Name: "known_span", Column: "time_us", GroupColumn: "did", Kind: ColumnAggregateMin},
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

	req.Capabilities.PhysicalAssetCount = 0
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if !strings.Contains(plan.Diagnostics.UnsupportedPlanReason, "unknown aggregate metadata") {
		t.Fatalf("unknown aggregate should be reported before physical-asset gate, got %q", plan.Diagnostics.UnsupportedPlanReason)
	}
}

func TestColumnQueryPlannerM11BReportsPhysicalGateBeforeMissingAggregateName(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{Enabled: true}},
	}}
	identity := ColumnStoreCacheIdentity{
		Collection:                      "events",
		ManifestRoot:                    99,
		ManifestGeneration:              7,
		RecoveryAuthoritativeGeneration: 7,
	}
	req := ColumnQueryPlanRequest{
		Name:      "q5_metadata",
		ForceKind: ColumnQueryPlanAggregateMetadata,
		Capabilities: ColumnQueryPlannerCapabilities{
			AggregateMetadata: true,
		},
	}

	plan := planColumnQueryForCatalog(catalog, identity, true, req)
	if plan.Supported {
		t.Fatalf("expected aggregate metadata without physical assets to fail closed: %+v", plan)
	}
	if got, want := plan.Diagnostics.UnsupportedPlanReason, columnQueryUnsupportedNoPhysicalAssetsReason; got != want {
		t.Fatalf("unsupported reason=%q want %q", got, want)
	}

	identity.ManifestRoot = 0
	plan = planColumnQueryForCatalog(catalog, identity, true, req)
	if got, want := plan.Diagnostics.UnsupportedPlanReason, columnQueryUnsupportedNoPhysicalAssetsReason; got != want {
		t.Fatalf("zero-root/no-assets unsupported reason=%q want %q", got, want)
	}
}

func TestColumnQueryPlannerM11BMatchesAggregateMetadataNamesCaseSensitivelyAfterTrim(t *testing.T) {
	catalog := &collectionCatalog{meta: CollectionMeta{
		Name: "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true,
			Columns: []ColumnStoreColumn{
				{Name: "did", Path: "did", ValueType: ColumnStoreValueString},
				{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
			},
			AggregateMetadata: []ColumnAggregateMetadata{
				{Name: "q5_did_time_span", Column: "time_us", GroupColumn: "did", Kind: ColumnAggregateMin},
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

	rangeThenLaterColumn := PlanColumnSkipScan([]ColumnSkipScanPredicate{
		{
			Position: 0,
			Lower:    ColumnSkipScanBound{Key: []byte{0x10}, Inclusive: true},
			Upper:    ColumnSkipScanBound{Key: []byte{0x30}, Inclusive: true},
		},
		{
			Position: 1,
			Lower:    ColumnSkipScanBound{Key: []byte{0x99}, Inclusive: true},
			Upper:    ColumnSkipScanBound{Key: []byte{0xaa}, Inclusive: true},
		},
	}, []ColumnSkipScanMark{
		{Name: "below-first-column-range", Rows: 10, MinKeys: [][]byte{{0x01}, {0xff}}, MaxKeys: [][]byte{{0x09}, {0xff}}},
		{Name: "overlaps-first-column", Rows: 10, MinKeys: [][]byte{{0x12}, {0x01}}, MaxKeys: [][]byte{{0x20}, {0x09}}},
		{Name: "above-first-column-range", Rows: 10, MinKeys: [][]byte{{0x31}, {0x99}}, MaxKeys: [][]byte{{0x40}, {0xaa}}},
	})
	if got, want := rangeThenLaterColumn.LeftPrefixColumns, 1; got != want {
		t.Fatalf("range-then-later left prefix=%d want %d", got, want)
	}
	if got, want := rangeThenLaterColumn.ScheduledMarks, []int{1}; !equalInts(got, want) {
		t.Fatalf("range-then-later scheduled=%v want %v", got, want)
	}
	if got, want := rangeThenLaterColumn.SkippedMarks, []int{0, 2}; !equalInts(got, want) {
		t.Fatalf("range-then-later skipped=%v want %v", got, want)
	}

	spanningComposite := PlanColumnSkipScan([]ColumnSkipScanPredicate{
		{
			Position: 0,
			Lower:    ColumnSkipScanBound{Key: []byte{0x10}, Inclusive: true},
			Upper:    ColumnSkipScanBound{Key: []byte{0x10}, Inclusive: true},
		},
		{
			Position: 1,
			Lower:    ColumnSkipScanBound{Key: []byte{0x99}, Inclusive: true},
			Upper:    ColumnSkipScanBound{Key: []byte{0x99}, Inclusive: true},
		},
	}, []ColumnSkipScanMark{
		{Name: "spans-first-column", Rows: 10, MinKeys: [][]byte{{0x09}, {0x01}}, MaxKeys: [][]byte{{0x20}, {0x09}}},
		{Name: "point-first-column-suffix-outside", Rows: 10, MinKeys: [][]byte{{0x10}, {0x01}}, MaxKeys: [][]byte{{0x10}, {0x09}}},
		{Name: "point-first-column-suffix-overlap", Rows: 10, MinKeys: [][]byte{{0x10}, {0x90}}, MaxKeys: [][]byte{{0x10}, {0xa0}}},
	})
	if got, want := spanningComposite.LeftPrefixColumns, 2; got != want {
		t.Fatalf("spanning-composite left prefix=%d want %d", got, want)
	}
	if got, want := spanningComposite.ScheduledMarks, []int{0, 2}; !equalInts(got, want) {
		t.Fatalf("spanning-composite scheduled=%v want %v", got, want)
	}
	if got, want := spanningComposite.SkippedMarks, []int{1}; !equalInts(got, want) {
		t.Fatalf("spanning-composite skipped=%v want %v", got, want)
	}

	highPositionOnly := PlanColumnSkipScan([]ColumnSkipScanPredicate{{
		Position: 1_000_000,
		Lower:    ColumnSkipScanBound{Key: []byte{0x01}, Inclusive: true},
	}}, marks)
	if got, want := highPositionOnly.LeftPrefixColumns, 0; got != want {
		t.Fatalf("high-position-only left prefix=%d want %d", got, want)
	}

	highPositionAfterPrefix := PlanColumnSkipScan([]ColumnSkipScanPredicate{
		{
			Position: 0,
			Lower:    ColumnSkipScanBound{Key: []byte{0x10}, Inclusive: true},
			Upper:    ColumnSkipScanBound{Key: []byte{0x35}, Inclusive: true},
		},
		{
			Position: 1_000_000,
			Lower:    ColumnSkipScanBound{Key: []byte{0xff}, Inclusive: true},
		},
	}, marks)
	if got, want := highPositionAfterPrefix.LeftPrefixColumns, 1; got != want {
		t.Fatalf("high-position-after-prefix left prefix=%d want %d", got, want)
	}
	if got, want := highPositionAfterPrefix.ScheduledMarks, []int{1, 2}; !equalInts(got, want) {
		t.Fatalf("high-position-after-prefix scheduled=%v want %v", got, want)
	}
	if got, want := highPositionAfterPrefix.SkippedMarks, []int{0}; !equalInts(got, want) {
		t.Fatalf("high-position-after-prefix skipped=%v want %v", got, want)
	}

	duplicatePositionLastWins := PlanColumnSkipScan([]ColumnSkipScanPredicate{
		{
			Position: 0,
			Lower:    ColumnSkipScanBound{Key: []byte{0x10}, Inclusive: true},
			Upper:    ColumnSkipScanBound{Key: []byte{0x35}, Inclusive: true},
		},
		{
			Position: 0,
			Lower:    ColumnSkipScanBound{Key: []byte{0x30}, Inclusive: true},
			Upper:    ColumnSkipScanBound{Key: []byte{0x40}, Inclusive: true},
		},
	}, marks)
	if got, want := duplicatePositionLastWins.LeftPrefixColumns, 1; got != want {
		t.Fatalf("duplicate-position left prefix=%d want %d", got, want)
	}
	if got, want := duplicatePositionLastWins.ScheduledMarks, []int{2}; !equalInts(got, want) {
		t.Fatalf("duplicate-position scheduled=%v want %v", got, want)
	}
	if got, want := duplicatePositionLastWins.SkippedMarks, []int{0, 1}; !equalInts(got, want) {
		t.Fatalf("duplicate-position skipped=%v want %v", got, want)
	}
}

func TestColumnSkipScanM11BPrunesRangeAtDeeperLeftPrefix(t *testing.T) {
	result := PlanColumnSkipScan([]ColumnSkipScanPredicate{
		{
			Position: 0,
			Lower:    ColumnSkipScanBound{Key: []byte{0x10}, Inclusive: true},
			Upper:    ColumnSkipScanBound{Key: []byte{0x10}, Inclusive: true},
		},
		{
			Position: 1,
			Lower:    ColumnSkipScanBound{Key: []byte{0x20}, Inclusive: true},
			Upper:    ColumnSkipScanBound{Key: []byte{0x30}, Inclusive: true},
		},
	}, []ColumnSkipScanMark{
		{Name: "suffix-before", Rows: 10, MinKeys: [][]byte{{0x10}, {0x01}}, MaxKeys: [][]byte{{0x10}, {0x1f}}},
		{Name: "suffix-overlap", Rows: 10, MinKeys: [][]byte{{0x10}, {0x25}}, MaxKeys: [][]byte{{0x10}, {0x26}}},
		{Name: "suffix-after", Rows: 10, MinKeys: [][]byte{{0x10}, {0x31}}, MaxKeys: [][]byte{{0x10}, {0x40}}},
		{Name: "first-column-spans", Rows: 10, MinKeys: [][]byte{{0x09}, {0x01}}, MaxKeys: [][]byte{{0x20}, {0x1f}}},
	})
	if got, want := result.LeftPrefixColumns, 2; got != want {
		t.Fatalf("deeper-range left prefix=%d want %d", got, want)
	}
	if got, want := result.ScheduledMarks, []int{1, 3}; !equalInts(got, want) {
		t.Fatalf("deeper-range scheduled=%v want %v", got, want)
	}
	if got, want := result.SkippedMarks, []int{0, 2}; !equalInts(got, want) {
		t.Fatalf("deeper-range skipped=%v want %v", got, want)
	}
}

func TestColumnSkipScanM11BDocumentsSparseAndDuplicatePositions(t *testing.T) {
	marks := []ColumnSkipScanMark{
		{Name: "low", Rows: 10, MinKeys: [][]byte{{0x01}}, MaxKeys: [][]byte{{0x09}}},
		{Name: "high", Rows: 10, MinKeys: [][]byte{{0x30}}, MaxKeys: [][]byte{{0x40}}},
	}

	sparse := PlanColumnSkipScan([]ColumnSkipScanPredicate{{
		Position: 2,
		Lower:    ColumnSkipScanBound{Key: []byte{0x30}, Inclusive: true},
		Upper:    ColumnSkipScanBound{Key: []byte{0x40}, Inclusive: true},
	}}, marks)
	if got, want := sparse.LeftPrefixColumns, 0; got != want {
		t.Fatalf("sparse left prefix=%d want %d", got, want)
	}
	if got, want := sparse.ScheduledMarks, []int{0, 1}; !equalInts(got, want) {
		t.Fatalf("sparse scheduled=%v want %v", got, want)
	}
	if len(sparse.SkippedMarks) != 0 {
		t.Fatalf("sparse skipped=%v want none", sparse.SkippedMarks)
	}

	duplicateLastWins := PlanColumnSkipScan([]ColumnSkipScanPredicate{
		{
			Position: 0,
			Lower:    ColumnSkipScanBound{Key: []byte{0x01}, Inclusive: true},
			Upper:    ColumnSkipScanBound{Key: []byte{0x09}, Inclusive: true},
		},
		{
			Position: 0,
			Lower:    ColumnSkipScanBound{Key: []byte{0x30}, Inclusive: true},
			Upper:    ColumnSkipScanBound{Key: []byte{0x40}, Inclusive: true},
		},
	}, marks)
	if got, want := duplicateLastWins.LeftPrefixColumns, 1; got != want {
		t.Fatalf("duplicate left prefix=%d want %d", got, want)
	}
	if got, want := duplicateLastWins.ScheduledMarks, []int{1}; !equalInts(got, want) {
		t.Fatalf("duplicate scheduled=%v want %v", got, want)
	}
	if got, want := duplicateLastWins.SkippedMarks, []int{0}; !equalInts(got, want) {
		t.Fatalf("duplicate skipped=%v want %v", got, want)
	}
}

func TestColumnSkipScanIntoM11BReusesScratchWithoutAllocating(t *testing.T) {
	marks := []ColumnSkipScanMark{
		{Name: "before", Rows: 10, MinKeys: [][]byte{{0x10}, {0x01}}, MaxKeys: [][]byte{{0x10}, {0x09}}},
		{Name: "inside", Rows: 10, MinKeys: [][]byte{{0x10}, {0x10}}, MaxKeys: [][]byte{{0x20}, {0x20}}},
		{Name: "after", Rows: 10, MinKeys: [][]byte{{0x10}, {0x30}}, MaxKeys: [][]byte{{0x10}, {0x40}}},
	}
	predicates := []ColumnSkipScanPredicate{
		{Position: 1, Lower: ColumnSkipScanBound{Key: []byte{0x10}, Inclusive: true}},
		{Position: 0, Lower: ColumnSkipScanBound{Key: []byte{0x10}, Inclusive: true}, Upper: ColumnSkipScanBound{Key: []byte{0x10}, Inclusive: true}},
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

	PlanColumnSkipScanInto(&result, []ColumnSkipScanPredicate{{
		Position: 1,
		Lower:    ColumnSkipScanBound{Key: []byte{0x10}, Inclusive: true},
		Upper:    ColumnSkipScanBound{Key: []byte{0x20}, Inclusive: true},
	}}, marks)
	if result.LeftPrefixColumns != 0 {
		t.Fatalf("stale scratch extended left prefix: %+v", result)
	}
	if got, want := result.ScheduledMarks, []int{0, 1, 2}; !equalInts(got, want) {
		t.Fatalf("scheduled after sparse reuse=%v want %v", got, want)
	}
	if len(result.SkippedMarks) != 0 {
		t.Fatalf("sparse reuse skipped marks with stale predicate scratch: %+v", result.SkippedMarks)
	}
}

func TestColumnQueryPlannerM14BRoutesForcedPhysicalPlansFromManifestCapabilities(t *testing.T) {
	events := columnPhysicalQueryFixtureEventsM13B(32)
	reopened, closeFn := openColumnPhysicalQueryFixtureM13B(t, events)
	defer closeFn()

	tests := []struct {
		name     string
		req      ColumnQueryPlanRequest
		wantKind ColumnQueryPlanKind
		workers  int
	}{
		{
			name: "serial",
			req: ColumnQueryPlanRequest{
				Name:             "m14b_serial",
				ProjectedColumns: []string{"time_us", "kind"},
				ForceKind:        ColumnQueryPlanSerialColumnScan,
			},
			wantKind: ColumnQueryPlanSerialColumnScan,
			workers:  1,
		},
		{
			name: "aggregate metadata",
			req: ColumnQueryPlanRequest{
				Name:                  "m14b_aggregate",
				ProjectedColumns:      []string{"time_us", "did"},
				AggregateMetadataName: "min_time_us",
				ForceKind:             ColumnQueryPlanAggregateMetadata,
			},
			wantKind: ColumnQueryPlanAggregateMetadata,
			workers:  1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := tc.req
			req.Capabilities = ColumnQueryPlannerCapabilities{
				SerialColumnScan:       true,
				AggregateMetadata:      true,
				ParallelColumnScan:     true,
				PhysicalAssetCount:     999,
				PartCount:              999,
				GranuleCount:           999,
				MaxParallelWorkers:     4,
				PlannerCandidateBudget: 5,
			}
			plan, err := reopened.PlanColumnQuery(req)
			if err != nil {
				t.Fatalf("PlanColumnQuery: %v", err)
			}
			if !plan.Supported {
				t.Fatalf("forced physical plan unsupported: %+v", plan)
			}
			if plan.Kind != tc.wantKind {
				t.Fatalf("plan kind=%q want %q diagnostics=%+v", plan.Kind, tc.wantKind, plan.Diagnostics)
			}
			if tc.workers > 0 && plan.Diagnostics.WorkerCount != tc.workers {
				t.Fatalf("worker count=%d want %d diagnostics=%+v", plan.Diagnostics.WorkerCount, tc.workers, plan.Diagnostics)
			}
			if got := plan.Diagnostics.PhysicalAssetCount; got <= 0 || got == 999 {
				t.Fatalf("physical asset count=%d want real manifest-derived count", got)
			}
			if got, want := plan.Diagnostics.PartCount, plan.Diagnostics.PhysicalAssetCount; got != want {
				t.Fatalf("part count=%d want physical asset count %d", got, want)
			}
			if got, want := plan.Diagnostics.GranuleCount, plan.Diagnostics.PartCount; got != want {
				t.Fatalf("granule count=%d want part-count fallback %d", got, want)
			}
			if got, want := plan.Diagnostics.ParallelWorkUnits, plan.Diagnostics.GranuleCount; got != want {
				t.Fatalf("parallel work units=%d want granule count %d", got, want)
			}
			if got, want := plan.Diagnostics.DeclaredColumnCount, 3; got != want {
				t.Fatalf("declared columns=%d want %d", got, want)
			}
			if got, want := plan.Diagnostics.AggregateMetadataCount, 2; got != want {
				t.Fatalf("aggregate metadata count=%d want %d", got, want)
			}
			if plan.Diagnostics.MutationParts != 0 {
				t.Fatalf("mutation parts=%d want zero for insert-only fixture", plan.Diagnostics.MutationParts)
			}
			if plan.Diagnostics.VisibilityMetadata {
				t.Fatalf("visibility metadata unexpectedly set for insert-only fixture: %+v", plan.Diagnostics)
			}
			if !plan.Diagnostics.RecoveryAuthoritative || plan.Diagnostics.SelectedManifestRoot == 0 || plan.Diagnostics.SelectedManifestGen == 0 || plan.Diagnostics.AppliedCommandLSN == 0 {
				t.Fatalf("manifest diagnostics were not populated from recovery-authoritative state: %+v", plan.Diagnostics)
			}
		})
	}

	parallelCollection, parallelClose := openColumnPhysicalInsertMultiGenerationFixtureM14B(t, 4)
	defer parallelClose()
	parallel, err := parallelCollection.PlanColumnQuery(ColumnQueryPlanRequest{
		Name:             "m14b_parallel",
		ProjectedColumns: []string{"time_us", "kind"},
		ForceKind:        ColumnQueryPlanParallelColumnScan,
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:       true,
			AggregateMetadata:      true,
			ParallelColumnScan:     true,
			PhysicalAssetCount:     999,
			PartCount:              999,
			GranuleCount:           999,
			MaxParallelWorkers:     4,
			PlannerCandidateBudget: 5,
		},
	})
	if err != nil {
		t.Fatalf("PlanColumnQuery parallel: %v", err)
	}
	if !parallel.Supported || parallel.Kind != ColumnQueryPlanParallelColumnScan {
		t.Fatalf("forced parallel plan unsupported: %+v", parallel)
	}
	if got, want := parallel.Diagnostics.WorkerCount, 4; got != want {
		t.Fatalf("worker count=%d want %d diagnostics=%+v", got, want, parallel.Diagnostics)
	}
	if got := parallel.Diagnostics.PhysicalAssetCount; got < 4 || got == 999 {
		t.Fatalf("physical asset count=%d want real multi-generation assets", got)
	}
}

func TestColumnQueryPlannerM14BRoutesSerialMutationVisibilityButNotParallel(t *testing.T) {
	reopened, closeFn, _ := openColumnPhysicalMutationFixtureM13C(t, 64)
	defer closeFn()

	plan, err := reopened.PlanColumnQuery(ColumnQueryPlanRequest{
		Name:             "m14a_mutation",
		ProjectedColumns: []string{"time_us", "kind"},
		ForceKind:        ColumnQueryPlanSerialColumnScan,
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			AggregateMetadata:  true,
			ParallelColumnScan: true,
			PhysicalAssetCount: 999,
			PartCount:          999,
			GranuleCount:       999,
			MaxParallelWorkers: 4,
		},
	})
	if err != nil {
		t.Fatalf("PlanColumnQuery: %v", err)
	}
	if !plan.Supported || plan.Kind != ColumnQueryPlanSerialColumnScan {
		t.Fatalf("forced serial mutation plan not routed through physical visibility path: %+v", plan)
	}
	if got := plan.Diagnostics.PhysicalAssetCount; got <= 1 || got == 999 {
		t.Fatalf("physical asset count=%d want real multi-part mutation manifest count", got)
	}
	if got := plan.Diagnostics.MutationParts; got <= 0 {
		t.Fatalf("mutation parts=%d want mutation visibility metadata", got)
	}
	if !plan.Diagnostics.VisibilityMetadata {
		t.Fatalf("visibility metadata not advertised for mutation-bearing manifest: %+v", plan.Diagnostics)
	}
	if got, want := plan.Diagnostics.PartCount, plan.Diagnostics.PhysicalAssetCount; got != want {
		t.Fatalf("part count=%d want physical asset count %d", got, want)
	}
	if got, want := plan.Diagnostics.GranuleCount, plan.Diagnostics.PartCount; got != want {
		t.Fatalf("granule count=%d want part-count fallback %d", got, want)
	}

	metadata, err := reopened.PlanColumnQuery(ColumnQueryPlanRequest{
		Name:                  "m14b_mutation_metadata",
		ProjectedColumns:      []string{"time_us", "did"},
		AggregateMetadataName: "min_time_us",
		ForceKind:             ColumnQueryPlanAggregateMetadata,
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			AggregateMetadata:  true,
			ParallelColumnScan: true,
			MaxParallelWorkers: 4,
		},
	})
	if err != nil {
		t.Fatalf("PlanColumnQuery aggregate metadata mutation: %v", err)
	}
	if metadata.Supported {
		t.Fatalf("aggregate metadata mutation plan should fail closed until mutation-aware metadata execution lands: %+v", metadata)
	}
	if got, want := metadata.Diagnostics.UnsupportedPlanReason, columnQueryUnsupportedAggregateMetadataDisabledReason; got != want {
		t.Fatalf("aggregate metadata unsupported reason=%q want %q diagnostics=%+v", got, want, metadata.Diagnostics)
	}
	if metadata.Diagnostics.MutationParts <= 0 || !metadata.Diagnostics.VisibilityMetadata {
		t.Fatalf("aggregate metadata mutation diagnostics did not expose mutation visibility state: %+v", metadata.Diagnostics)
	}

	parallel, err := reopened.PlanColumnQuery(ColumnQueryPlanRequest{
		Name:             "m14b_parallel_mutation",
		ProjectedColumns: []string{"time_us", "kind"},
		ForceKind:        ColumnQueryPlanParallelColumnScan,
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			AggregateMetadata:  true,
			ParallelColumnScan: true,
			MaxParallelWorkers: 4,
		},
	})
	if err != nil {
		t.Fatalf("PlanColumnQuery parallel: %v", err)
	}
	if parallel.Supported {
		t.Fatalf("parallel mutation plan should fail closed until partitioned visibility execution lands: %+v", parallel)
	}
	if got, want := parallel.Diagnostics.UnsupportedPlanReason, columnQueryUnsupportedParallelMutationPartsReason; got != want {
		t.Fatalf("parallel unsupported reason=%q want %q diagnostics=%+v", got, want, parallel.Diagnostics)
	}
}

func openColumnPhysicalInsertMultiGenerationFixtureM14B(t *testing.T, batches int) (*Collection, func()) {
	t.Helper()
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	for i := 0; i < batches; i++ {
		id := []byte(fmt.Sprintf("e%d", i))
		doc := []byte(fmt.Sprintf(`{"time_us":%d,"kind":"kind_%02d","did":"did_%02d","payload":"p%d"}`, i+1, i%4, i%8, i))
		if _, err := col.InsertBatch([][]byte{id}, [][]byte{doc}); err != nil {
			_ = d.Close()
			t.Fatalf("InsertBatch %d: %v", i, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint before reopen: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close before reopen: %v", err)
	}
	reopen := openCollectionCommandWALDB(t, dir)
	return openColumnStoreCollectionM10B(t, reopen), func() { _ = reopen.Close() }
}

func TestColumnQueryPlannerM14AFailsClosedWhenDBUnavailable(t *testing.T) {
	reopened, closeFn := openColumnPhysicalQueryFixtureM13B(t, columnPhysicalQueryFixtureEventsM13B(16))
	defer closeFn()

	reopened.catalogMu.RLock()
	catalog := reopened.catalog
	systemRoot := reopened.catalogSystemRoot
	commitSeq := reopened.catalogCommitSeq
	reopened.catalogMu.RUnlock()

	unavailable := &Collection{
		catalog:           catalog,
		catalogSystemRoot: systemRoot,
		catalogCommitSeq:  commitSeq,
	}
	plan, err := unavailable.PlanColumnQuery(ColumnQueryPlanRequest{
		Name:             "m14a_unavailable_db",
		ProjectedColumns: []string{"time_us", "kind"},
		ForceKind:        ColumnQueryPlanSerialColumnScan,
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			AggregateMetadata:  true,
			ParallelColumnScan: true,
			PhysicalAssetCount: 999,
			PartCount:          999,
			GranuleCount:       999,
			MaxParallelWorkers: 4,
		},
	})
	if err != nil {
		t.Fatalf("PlanColumnQuery: %v", err)
	}
	if plan.Supported {
		t.Fatalf("forced physical plan unexpectedly used caller capabilities with unavailable DB: %+v", plan)
	}
	if got, want := plan.Diagnostics.UnsupportedPlanReason, errCollectionDBNil.Error(); got != want {
		t.Fatalf("unsupported reason=%q want %q diagnostics=%+v", got, want, plan.Diagnostics)
	}
	if got, want := plan.Diagnostics.CapabilityError, errCollectionDBNil.Error(); got != want {
		t.Fatalf("capability error=%q want %q diagnostics=%+v", got, want, plan.Diagnostics)
	}
	if got := plan.Diagnostics.PhysicalAssetCount; got != 0 {
		t.Fatalf("physical asset count=%d want caller-supplied count cleared", got)
	}
	if got, want := plan.Diagnostics.DeclaredColumnCount, 3; got != want {
		t.Fatalf("declared column count=%d want %d", got, want)
	}
}

func TestColumnQueryPlannerM14AIgnoresCallerSuppliedCapabilityError(t *testing.T) {
	reopened, closeFn := openColumnPhysicalQueryFixtureM13B(t, columnPhysicalQueryFixtureEventsM13B(16))
	defer closeFn()

	plan, err := reopened.PlanColumnQuery(ColumnQueryPlanRequest{
		Name:             "m14a_ignore_forged_capability_error",
		ProjectedColumns: []string{"time_us", "kind"},
		ForceKind:        ColumnQueryPlanSerialColumnScan,
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			AggregateMetadata:  true,
			ParallelColumnScan: true,
			CapabilityError:    "caller-forged failure",
			PhysicalAssetCount: 999,
			PartCount:          999,
			GranuleCount:       999,
			MaxParallelWorkers: 4,
		},
	})
	if err != nil {
		t.Fatalf("PlanColumnQuery: %v", err)
	}
	if got := plan.Diagnostics.CapabilityError; got != "" {
		t.Fatalf("capability error=%q want caller-supplied value ignored", got)
	}
	if got := plan.Diagnostics.UnsupportedPlanReason; got == "caller-forged failure" || strings.Contains(got, "caller-forged") {
		t.Fatalf("unsupported reason=%q used caller-supplied capability error", got)
	}
	if got := plan.Diagnostics.PhysicalAssetCount; got <= 0 || got == 999 {
		t.Fatalf("physical asset count=%d want manifest-derived count", got)
	}
}

func TestColumnQueryPlannerM14AFailsClosedUnknownForceKindClearsCallerCapabilities(t *testing.T) {
	reopened, closeFn := openColumnPhysicalQueryFixtureM13B(t, columnPhysicalQueryFixtureEventsM13B(16))
	defer closeFn()

	plan, err := reopened.PlanColumnQuery(ColumnQueryPlanRequest{
		Name:             "m14a_unknown_force",
		ProjectedColumns: []string{"time_us", "kind"},
		ForceKind:        ColumnQueryPlanKind("unknown_physical_force"),
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			AggregateMetadata:  true,
			ParallelColumnScan: true,
			PhysicalAssetCount: 999,
			PartCount:          999,
			GranuleCount:       999,
			MaxParallelWorkers: 4,
		},
	})
	if err != nil {
		t.Fatalf("PlanColumnQuery: %v", err)
	}
	if plan.Supported {
		t.Fatalf("unknown forced physical plan unexpectedly supported: %+v", plan)
	}
	if got := plan.Diagnostics.UnsupportedPlanReason; !strings.Contains(got, "unknown column query plan kind") {
		t.Fatalf("unsupported reason=%q want unknown force-kind rejection diagnostics=%+v", got, plan.Diagnostics)
	}
	if got := plan.Diagnostics.PhysicalAssetCount; got <= 0 || got == 999 {
		t.Fatalf("physical asset count=%d want manifest-derived count, not caller-supplied capabilities", got)
	}
	if got := plan.Diagnostics.UnsupportedPlanKind; got != ColumnQueryPlanKind("unknown_physical_force") {
		t.Fatalf("unsupported plan kind=%q want unknown forced kind diagnostics=%+v", got, plan.Diagnostics)
	}
	if plan.Diagnostics.ScheduledGranules != 0 || plan.Diagnostics.WorkerCount != 0 {
		t.Fatalf("unknown forced kind should not schedule physical work: %+v", plan.Diagnostics)
	}
}

func BenchmarkColumnQueryPlannerCapabilitiesM14A(b *testing.B) {
	b.Run("insert_manifest_rows_1024", func(b *testing.B) {
		reopened, closeFn := openColumnPhysicalQueryFixtureM13B(b, columnPhysicalQueryFixtureEventsM13B(1024))
		defer closeFn()
		req := ColumnQueryPlanRequest{
			Name:             "m14a_insert",
			ProjectedColumns: []string{"time_us", "kind", "did"},
			ForceKind:        ColumnQueryPlanSerialColumnScan,
			Capabilities: ColumnQueryPlannerCapabilities{
				SerialColumnScan:   true,
				AggregateMetadata:  true,
				ParallelColumnScan: true,
				MaxParallelWorkers: 4,
			},
		}
		b.ReportAllocs()
		b.ResetTimer()
		var assets int
		for i := 0; i < b.N; i++ {
			plan, err := reopened.PlanColumnQuery(req)
			if err != nil {
				b.Fatal(err)
			}
			if !plan.Supported || plan.Kind != ColumnQueryPlanSerialColumnScan {
				b.Fatalf("unexpected plan: %+v", plan)
			}
			assets += plan.Diagnostics.PhysicalAssetCount
		}
		columnQueryPlannerBenchSinkM14A = assets
	})
	b.Run("mutation_manifest_rows_1024", func(b *testing.B) {
		reopened, closeFn, _ := openColumnPhysicalMutationFixtureM13C(b, 1024)
		defer closeFn()
		req := ColumnQueryPlanRequest{
			Name:             "m14a_mutation",
			ProjectedColumns: []string{"time_us", "kind", "did"},
			ForceKind:        ColumnQueryPlanSerialColumnScan,
			Capabilities: ColumnQueryPlannerCapabilities{
				SerialColumnScan:   true,
				AggregateMetadata:  true,
				ParallelColumnScan: true,
				MaxParallelWorkers: 4,
			},
		}
		b.ReportAllocs()
		b.ResetTimer()
		var mutationParts int
		for i := 0; i < b.N; i++ {
			plan, err := reopened.PlanColumnQuery(req)
			if err != nil {
				b.Fatal(err)
			}
			if !plan.Supported || !plan.Diagnostics.VisibilityMetadata {
				b.Fatalf("unexpected mutation plan: %+v", plan)
			}
			mutationParts += plan.Diagnostics.MutationParts
		}
		columnQueryPlannerBenchSinkM14A = mutationParts
	})
}

var columnQueryPlannerBenchSinkM14A int

func TestColumnQueryPlannerCapabilitiesAllocationBudgetM1634(t *testing.T) {
	reopened, closeFn := openColumnPhysicalQueryFixtureM13B(t, columnPhysicalQueryFixtureEventsM13B(1024))
	defer closeFn()
	req := ColumnQueryPlanRequest{
		Name:             "m1634_insert",
		ProjectedColumns: []string{"time_us", "kind", "did"},
		ForceKind:        ColumnQueryPlanSerialColumnScan,
		Capabilities: ColumnQueryPlannerCapabilities{
			SerialColumnScan:   true,
			AggregateMetadata:  true,
			ParallelColumnScan: true,
			MaxParallelWorkers: 4,
		},
	}
	plan, err := reopened.PlanColumnQuery(req)
	if err != nil {
		t.Fatalf("PlanColumnQuery preview: %v", err)
	}
	if !plan.Supported || plan.Kind != ColumnQueryPlanSerialColumnScan || plan.Diagnostics.PhysicalAssetCount == 0 {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	allocs := testing.AllocsPerRun(50, func() {
		plan, err := reopened.PlanColumnQuery(req)
		if err != nil {
			panic(fmt.Sprintf("PlanColumnQuery: %v", err))
		}
		if !plan.Supported || plan.Kind != ColumnQueryPlanSerialColumnScan || plan.Diagnostics.PhysicalAssetCount == 0 {
			panic(fmt.Sprintf("unexpected plan: %+v", plan))
		}
	})
	maxAllocs := 12.0
	if runtime.GOOS == "windows" || collectionsRaceEnabled {
		maxAllocs += 16
	}
	if allocs > maxAllocs {
		t.Fatalf("planner capability allocs/run=%.2f want <= %.2f", allocs, maxAllocs)
	}
}

func equalInts(left, right []int) bool {
	return slices.Equal(left, right)
}
