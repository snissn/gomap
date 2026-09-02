package main

import (
	"os"
	"strconv"
	"testing"
)

// TestServiceOverheadParityRows runs the C6 (#4273) service-level parity rows
// on a small slice of the committed fixture and prints the observational
// overhead table (service HTTP vs direct collection API, p50/p99 per query).
// RAG_SVC_OVERHEAD_DOCS / RAG_SVC_OVERHEAD_REPS let evidence runs scale the
// fixture up without changing the deterministic default.
func TestServiceOverheadParityRows(t *testing.T) {
	docs := 128
	if raw := os.Getenv("RAG_SVC_OVERHEAD_DOCS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			t.Fatalf("RAG_SVC_OVERHEAD_DOCS=%q must be a positive integer", raw)
		}
		docs = parsed
	}
	reps := 3
	if raw := os.Getenv("RAG_SVC_OVERHEAD_REPS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			t.Fatalf("RAG_SVC_OVERHEAD_REPS=%q must be a positive integer", raw)
		}
		reps = parsed
	}

	report, err := runServiceOverheadBenchmark(serviceOverheadConfig{
		Docs:           docs,
		Dims:           ragFixtureDims,
		M:              8,
		TopK:           10,
		CandidateLimit: 64,
		Reps:           reps,
		Warmup:         2,
		Dir:            t.TempDir(),
	})
	if err != nil {
		t.Fatalf("runServiceOverheadBenchmark: %v", err)
	}
	if report.Schema != svcOverheadSchema || len(report.Rows) != 4 {
		t.Fatalf("report schema=%s rows=%d want %s/4", report.Schema, len(report.Rows), svcOverheadSchema)
	}
	byKey := map[string]serviceOverheadRow{}
	for _, row := range report.Rows {
		byKey[row.Lane+"/"+row.Path] = row
		if row.Samples != row.Reps*row.Queries {
			t.Fatalf("%s/%s samples=%d want %d", row.Lane, row.Path, row.Samples, row.Reps*row.Queries)
		}
		if row.P50Millis <= 0 || row.P99Millis < row.P50Millis {
			t.Fatalf("%s/%s p50=%f p99=%f invalid percentiles", row.Lane, row.Path, row.P50Millis, row.P99Millis)
		}
	}
	for _, lane := range []string{"filtered_hybrid", "ann_dense"} {
		direct, ok := byKey[lane+"/direct_collection_api"]
		if !ok {
			t.Fatalf("missing direct row for %s", lane)
		}
		service, ok := byKey[lane+"/http_service"]
		if !ok {
			t.Fatalf("missing service row for %s", lane)
		}
		t.Logf("%s: direct p50=%.3fms p99=%.3fms | service p50=%.3fms p99=%.3fms | overhead p50=%+.3fms p99=%+.3fms",
			lane, direct.P50Millis, direct.P99Millis, service.P50Millis, service.P99Millis,
			service.P50Millis-direct.P50Millis, service.P99Millis-direct.P99Millis)
	}
}
