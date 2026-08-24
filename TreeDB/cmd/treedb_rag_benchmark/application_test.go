package main

import (
	"os"
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
	seenSupported, seenMetadata, seenFilter, seenCollapseSupported, seenHTTPScore := false, false, false, false, false
	for _, row := range rows {
		capability := unsupportedCapability(row)
		if capability == nil {
			seenSupported = true
			seenCollapseSupported = seenCollapseSupported || row.Collapse == "enabled_cap_2"
			continue
		}
		if strings.Contains(capability.Code, "source_metadata_not_propagated") {
			seenMetadata = true
		}
		if row.Filter == filterTenantAlphaWorkspaceRed || row.Filter == filterModerateRange {
			seenFilter = seenFilter || strings.Contains(capability.Code, "multi_field_filter_unavailable")
		}
		if strings.Contains(capability.Code, "parent_collapse_unavailable") {
			t.Fatalf("parent collapse remains unsupported: %+v", row)
		}
		seenHTTPScore = seenHTTPScore || strings.Contains(capability.Code, "http_score_only_route_unavailable")
	}
	if !seenSupported || !seenMetadata || !seenFilter || !seenCollapseSupported || !seenHTTPScore {
		t.Fatalf("matrix capability coverage supported=%t metadata=%t filter=%t collapse_supported=%t http_score=%t", seenSupported, seenMetadata, seenFilter, seenCollapseSupported, seenHTTPScore)
	}
}

func TestApplicationDiagnosticSmokeLifecycleServiceAndArtifacts(t *testing.T) {
	cfg := defaultApplicationConfig()
	cfg.FinalEvidence = false
	cfg.Repetitions = 1
	cfg.SamplesPerRep = 9
	cfg.IngestionReps = 1
	cfg.WarmupQueries = 1
	cfg.Dir = filepath.Join(t.TempDir(), "db")
	cfg.ProductBaseSHA = "99929cdeb2ae2ec1e411236c853eb36942075d72"
	report, err := runApplicationBaseline(cfg)
	if err != nil {
		t.Fatalf("run diagnostic: %v", err)
	}
	if report.Authority != "DIAGNOSTIC_NOT_FINAL_EVIDENCE" {
		t.Fatalf("authority=%q", report.Authority)
	}
	for name, lifecycle := range report.Lifecycle {
		if !lifecycle.UnchangedReingest || !lifecycle.ColdReopenParity || !lifecycle.TextIndexParity || !lifecycle.VectorIndexParity || !lifecycle.ScalarIndexParity {
			t.Fatalf("%s lifecycle=%+v", name, lifecycle)
		}
		if lifecycle.InitialSources != 19 || lifecycle.FinalSources != 18 || lifecycle.InitialChunks != 57 || lifecycle.FinalChunks != 54 {
			t.Fatalf("%s lifecycle counts=%+v", name, lifecycle)
		}
	}
	var direct, service, unsupported bool
	for _, row := range report.Rows {
		if row.Status == "unsupported" {
			unsupported = unsupported || row.Capability != nil && row.Capability.FailClosed && row.Capability.ResultsReturned == 0
			continue
		}
		if row.Errors != 0 || row.Quality.ChunkRecallAt10 <= 0 || len(row.Samples) != cfg.SamplesPerRep {
			t.Fatalf("supported row invalid: %+v", row)
		}
		if row.Cell.Surface == "direct_collection" && row.Cell.Projection == "score_only" {
			if row.Counters["documents_fetched"] != 0 || row.Quality.AttributionMode != "untimed_compact_same_work_route_filter" {
				t.Fatalf("direct score-only projection/attribution invalid: %+v", row)
			}
			if row.Cell.Route != "vector_only" && row.Quality.TextAttributedResults == 0 {
				t.Fatalf("direct score-only text attribution missing: %+v", row.Cell)
			}
			if row.Cell.Route != "text_only" && row.Quality.VectorAttributedResults == 0 {
				t.Fatalf("direct score-only vector attribution missing: %+v", row.Cell)
			}
		}
		direct = direct || row.Cell.Surface == "direct_collection"
		service = service || row.Cell.Surface == "http_service"
	}
	if !direct || !service || !unsupported {
		t.Fatalf("surface/capability evidence direct=%t service=%t unsupported=%t", direct, service, unsupported)
	}
	outDir := filepath.Join(t.TempDir(), "artifacts")
	jsonPath, markdownPath, manifestPath, err := writeApplicationArtifacts(report, outDir)
	if err != nil {
		t.Fatalf("write artifacts: %v", err)
	}
	for _, path := range []string{jsonPath, markdownPath, manifestPath} {
		if info, err := os.Stat(path); err != nil || info.Size() == 0 {
			t.Fatalf("artifact %s info=%v err=%v", path, info, err)
		}
	}
	bad := *report
	bad.Lifecycle = make(map[string]lifecycleEvidence, len(report.Lifecycle))
	for name, lifecycle := range report.Lifecycle {
		bad.Lifecycle[name] = lifecycle
	}
	for name, lifecycle := range bad.Lifecycle {
		lifecycle.ColdReopenParity = false
		bad.Lifecycle[name] = lifecycle
		break
	}
	if _, _, _, err := writeApplicationArtifacts(&bad, filepath.Join(t.TempDir(), "bad-artifacts")); err == nil {
		t.Fatal("artifact writer accepted cold_reopen_parity=false")
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
