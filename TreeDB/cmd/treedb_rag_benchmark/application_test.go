package main

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/collections"
	"path/filepath"
	"strings"
	"testing"
)

func TestApplicationMatrixRetainsUnsupportedRows(t *testing.T) {
	rows := applicationCellMatrix("hashing_regression")
	want := len(applicationRoutes) * len(applicationProjections) * len(applicationFilterOrder) * 2 * len(applicationSurfaces) * len(applicationClients)
	if len(rows) != want {
		t.Fatalf("matrix rows=%d want %d", len(rows), want)
	}
	seenSupported, seenTenantFilter, seenMultiFilter, seenCollapseSupported, seenHTTPVectorGap, seenHTTPScore := false, false, false, false, false, false
	for _, row := range rows {
		capability := unsupportedCapability(row)
		filteredVector := row.Filter != filterUnfiltered && (row.Route == "hybrid" || row.Route == "vector_only" && row.Surface == "direct_collection")
		if filteredVector && (capability == nil || !strings.Contains(capability.Code, "ordinary_filtered_vector_unavailable")) {
			t.Fatalf("filtered ordinary vector cell lacks explicit refusal: %+v", row)
		}
		if capability == nil {
			seenSupported = true
			seenTenantFilter = seenTenantFilter || row.Filter == filterTenantAlpha
			seenMultiFilter = seenMultiFilter || row.Filter == filterModerateRange
			seenCollapseSupported = seenCollapseSupported || row.Collapse == "enabled_cap_2"
			continue
		}
		if strings.Contains(capability.Code, "source_metadata_not_propagated") {
			t.Fatalf("metadata capability remains unsupported after propagation: %+v", row)
		}
		if strings.Contains(capability.Code, "multi_field_filter_unavailable") {
			t.Fatalf("multi-field filter remains unsupported: %+v", row)
		}
		if strings.Contains(capability.Code, "parent_collapse_unavailable") && !strings.Contains(capability.Code, "http_vector_parent_collapse_unavailable") {
			t.Fatalf("general parent collapse remains unsupported: %+v", row)
		}
		seenHTTPVectorGap = seenHTTPVectorGap || strings.Contains(capability.Code, "http_vector_parent_collapse_unavailable")
		seenHTTPScore = seenHTTPScore || strings.Contains(capability.Code, "http_score_only_route_unavailable")
	}
	if !seenSupported || !seenTenantFilter || !seenMultiFilter || !seenCollapseSupported || !seenHTTPVectorGap || !seenHTTPScore {
		t.Fatalf("matrix capability coverage supported=%t tenant_filter=%t multi_filter=%t collapse_supported=%t http_vector_gap=%t http_score=%t", seenSupported, seenTenantFilter, seenMultiFilter, seenCollapseSupported, seenHTTPVectorGap, seenHTTPScore)
	}
}

func TestApplicationDiagnosticSmokeLifecycleServiceAndArtifacts(t *testing.T) {
	for _, repetitions := range []int{1, 2} {
		t.Run(fmt.Sprintf("ingestion_reps_%d", repetitions), func(t *testing.T) {
			cfg := defaultApplicationConfig()
			cfg.FinalEvidence = false
			cfg.Repetitions = 1
			cfg.SamplesPerRep = 9
			cfg.IngestionReps = repetitions
			cfg.WarmupQueries = 1
			cfg.Dir = filepath.Join(t.TempDir(), "db")
			cfg.ProductBaseSHA = "99929cdeb2ae2ec1e411236c853eb36942075d72"
			report, err := runApplicationBaseline(cfg)
			var ingestErr *collections.IngestError
			if report != nil || !errors.As(err, &ingestErr) || ingestErr.Stage != collections.IngestStageStorage ||
				!errors.Is(err, collections.ErrColumnDeclaredValueUnsupported) || !strings.Contains(err.Error(), "incompatible source parent shape") || !strings.Contains(err.Error(), `"embedding"`) {
				t.Fatalf("expected pre-publication parent shape refusal without lifecycle/report evidence: report=%+v err=%v", report, err)
			}
			var aggregate interface{ Unwrap() []error }
			if !errors.As(err, &aggregate) || len(aggregate.Unwrap()) != repetitions {
				t.Fatalf("expected every ingestion failure in typed chain: reps=%d err=%v", repetitions, err)
			}
			for rep, failure := range aggregate.Unwrap() {
				var typed *collections.IngestError
				if !errors.As(failure, &typed) || typed.Stage != collections.IngestStageStorage || !errors.Is(failure, collections.ErrColumnDeclaredValueUnsupported) || !strings.Contains(failure.Error(), fmt.Sprintf("ingestion rep %d:", rep)) {
					t.Fatalf("ingestion rep %d lost its context or typed cause: %v", rep, failure)
				}
			}
			// #5289 owns restoration of lifecycle, cold-reopen and artifact evidence.
			// A fail-closed diagnostic cannot claim query samples or a passing lifecycle.
		})
	}
}

func TestApplicationTenantFilterRequestsAndLeakageAccounting(t *testing.T) {
	cell := applicationCellIdentity{Filter: filterTenantAlpha}
	direct := applicationDirectScalarFilter(cell)
	if direct == nil || direct.IndexName != "meta_tenant_id" || direct.Value != "alpha" {
		t.Fatalf("direct tenant filter=%+v", direct)
	}
	service := applicationServiceFilter(cell)
	if service == nil || service.Field != "meta.tenant_id" || service.Operator != "==" || service.Value != "alpha" {
		t.Fatalf("service tenant filter=%+v", service)
	}
	fixture := &applicationFixture{Sources: []applicationSource{
		{ID: "alpha", Tenant: "alpha", Workspace: "red"},
		{ID: "beta", Tenant: "beta", Workspace: "blue"},
	}}
	tenant, workspace, year := applicationScopeViolations(fixture, filterTenantAlpha, []string{"alpha#0", "beta#0", "unknown#0"})
	if tenant != 2 || workspace != 1 || year != 1 {
		t.Fatalf("tenant/workspace/range violations=%d/%d/%d want 2/1/1", tenant, workspace, year)
	}
	multi := applicationDirectScalarFilter(applicationCellIdentity{Filter: filterModerateRange})
	if multi == nil || len(multi.And) != 3 || multi.And[2].Range == nil {
		t.Fatalf("direct multi-field filter=%+v", multi)
	}
	serviceMulti := applicationServiceFilter(applicationCellIdentity{Filter: filterModerateRange})
	if serviceMulti == nil || serviceMulti.Operator != "AND" || len(serviceMulti.Conditions) != 3 {
		t.Fatalf("service multi-field filter=%+v", serviceMulti)
	}
	if got := applicationVectorRoute(applicationCellIdentity{Route: "vector_only", Surface: "http_service", Filter: filterUnfiltered}); got != "declared_column_graph_ann" {
		t.Fatalf("unfiltered HTTP vector route=%q", got)
	}
	if got := applicationVectorRoute(applicationCellIdentity{Route: "vector_only", Surface: "http_service", Filter: filterTenantAlpha}); got != "declared_column_graph_exact_filtered" {
		t.Fatalf("filtered HTTP vector route=%q", got)
	}
	if got := applicationVectorRoute(applicationCellIdentity{Route: "vector_only", Surface: "direct_collection", Filter: filterUnfiltered}); got != "declared_column_graph_ann_fp32" {
		t.Fatalf("direct vector route=%q", got)
	}
}

func TestApplicationGateRetainsPreCandidateThresholds(t *testing.T) {
	for _, summary := range []ingestionSummary{
		{MedianDocsPerSec: 10, MedianBytesPerOp: 1, HistoricalReproduced: true},
		{MedianDocsPerSec: 1000, MedianBytesPerOp: 1 << 40},
	} {
		gate := freezeApplicationGate(summary)
		if gate.CandidateMinDocsPerSec != 325.45 || gate.CandidateMaxBytesPerOp != 1947235 {
			t.Fatalf("gate changed with measured candidate summary: %+v", gate)
		}
	}
}

func TestFinalApplicationPolicyRejectsDiagnosticCounts(t *testing.T) {
	cfg := defaultApplicationConfig()
	cfg.FinalEvidence = true
	cfg.Repetitions = 2
	cfg.SamplesPerRep = 100
	cfg.IngestionReps = 4
	if err := validateApplicationConfig(cfg); err == nil {
		t.Fatal("final evidence accepted insufficient query/ingestion repetitions")
	}
}
