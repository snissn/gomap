package main

import (
	"fmt"
	"testing"
)

// tinyBenchConfig sizes the in-process end-to-end runs used by the contract
// tests; small enough to keep focused runs fast.
func tinyBenchConfig(dir string) benchConfig {
	return benchConfig{
		Docs:           128,
		Dims:           ragFixtureDims,
		M:              4,
		EfSearch:       64,
		TopK:           10,
		CandidateLimit: 64,
		Reps:           1,
		Warmup:         2,
		BatchSize:      ragDefaultBatchSize,
		Dir:            dir,
	}
}

// TestCounterContractEndToEnd runs the full harness on a tiny fixture and
// enforces the fail-closed counter contract on the measured rows:
// score-only rows fetch zero documents, fetch_topk rows stay bounded by TopK.
func TestCounterContractEndToEnd(t *testing.T) {
	out, err := runBenchmark(tinyBenchConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if len(out.Rows) == 0 {
		t.Fatal("no rows measured")
	}
	for _, row := range out.Rows {
		fetches := row.Counters["documents_fetched"]
		switch row.ResultMode {
		case "score_only":
			if fetches != 0 {
				t.Fatalf("row %s/%s/%s documents_fetched=%v want 0", row.Route, row.ResultMode, row.Filter, fetches)
			}
		case "fetch_topk":
			if fetches > float64(row.TopK) {
				t.Fatalf("row %s/%s/%s documents_fetched=%v exceeds topK=%d", row.Route, row.ResultMode, row.Filter, fetches, row.TopK)
			}
			if fetches == 0 {
				t.Fatalf("row %s/%s/%s fetched nothing in fetch mode", row.Route, row.ResultMode, row.Filter)
			}
		}
		if fb := row.Counters["full_document_scan_fallbacks"]; fb != 0 {
			t.Fatalf("row %s/%s/%s full_document_scan_fallbacks=%v want 0", row.Route, row.ResultMode, row.Filter, fb)
		}
	}
	vs := validateCounterContract(out.Rows, 10)
	if !validationsAllOK(vs) {
		t.Fatalf("counter validations failed: %v", validationFailures(vs))
	}
	// Quality sanity: metrics live in [0,1] and hybrid recall@10 is not
	// degenerate on the committed tiny fixture.
	for _, row := range out.Rows {
		for name, v := range map[string]float64{
			"recall@5":  row.RecallAt5,
			"recall@10": row.RecallAt10,
			"mrr@10":    row.MRRAt10,
		} {
			if v < 0 || v > 1 {
				t.Fatalf("row %s/%s/%s %s=%f outside [0,1]", row.Route, row.ResultMode, row.Filter, name, v)
			}
		}
	}
	hybridRow := out.Rows[2*len(ragResultModes)*len(ragFilterCases)] // hybrid/score_only/none_100pct
	if hybridRow.Route != "hybrid" || hybridRow.ResultMode != "score_only" {
		t.Fatalf("unexpected row ordering at hybrid slot: %+v", hybridRow.rowKey)
	}
	if hybridRow.RecallAt10 <= 0 {
		t.Fatalf("hybrid score_only recall@10=%f want > 0", hybridRow.RecallAt10)
	}
}

// TestScoreOnlyDocFetchFailsReport proves report generation fails closed when
// a score-only row shows document fetches.
func TestScoreOnlyDocFetchFailsReport(t *testing.T) {
	out, err := runBenchmark(tinyBenchConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	doctored := append([]rowResult(nil), out.Rows...)
	for i := range doctored {
		if doctored[i].ResultMode == "score_only" {
			doctored[i].Counters = map[string]float64{}
			for k, v := range out.Rows[i].Counters {
				doctored[i].Counters[k] = v
			}
			doctored[i].Counters["documents_fetched"] = 3
			break
		}
	}
	validations := validateCounterContract(doctored, 10)
	if validationsAllOK(validations) {
		t.Fatal("doctored score-only row with docs_fetched=3 must fail the counter contract")
	}
	cfg := tinyBenchConfig("")
	out.Rows = doctored
	if _, err := buildReport(out, cfg, "4267", ""); err == nil {
		t.Fatal("buildReport must fail closed on score-only doc fetch violation")
	} else {
		t.Logf("expected failure: %v", err)
	}
}

// TestFetchTopkBoundedByTopK proves a fetch_topk row exceeding TopK per query
// fails the counter contract and blocks report generation.
func TestFetchTopkBoundedByTopK(t *testing.T) {
	out, err := runBenchmark(tinyBenchConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	doctored := append([]rowResult(nil), out.Rows...)
	found := false
	for i := range doctored {
		if doctored[i].ResultMode == "fetch_topk" {
			doctored[i].Counters = map[string]float64{}
			for k, v := range out.Rows[i].Counters {
				doctored[i].Counters[k] = v
			}
			doctored[i].Counters["documents_fetched"] = float64(doctored[i].TopK) + 1
			found = true
			break
		}
	}
	if !found {
		t.Fatal("no fetch_topk row found")
	}
	validations := validateCounterContract(doctored, 10)
	if validationsAllOK(validations) {
		t.Fatal("fetch_topk row exceeding topK must fail the counter contract")
	}
	cfg := tinyBenchConfig("")
	out.Rows = doctored
	if _, err := buildReport(out, cfg, "4267", ""); err == nil {
		t.Fatal("buildReport must fail closed on unbounded doc fetch")
	} else {
		t.Logf("expected failure: %v", fmt.Sprint(err))
	}
}

// TestZeroDocCorpusFailsClosed proves a zero-document corpus refuses to run.
func TestZeroDocCorpusFailsClosed(t *testing.T) {
	if _, _, err := buildRagCorpus(0, ragFixtureDims); err == nil {
		t.Fatal("zero-doc corpus builder must fail closed")
	}
	cfg := tinyBenchConfig(t.TempDir())
	cfg.Docs = 0
	if _, err := runBenchmark(cfg); err == nil {
		t.Fatal("runBenchmark with zero docs must fail closed")
	}
}
