package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestCanonicalNativeCOWConfig(t *testing.T) {
	defaults, err := parseConfig(nil)
	if err != nil || defaults.NativeCOWCompanion {
		t.Fatalf("canonical default enables native COW: config=%+v err=%v", defaults, err)
	}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"zero indexes both formats", []string{"-indexes", "0", "-formats", "bson,json"}, ""},
		{"indexed BSON", []string{"-indexes", "2", "-formats", "bson"}, ""},
		{"indexed JSON", []string{"-indexes", "2", "-formats", "json"}, "supports BSON/JSON"},
		{"wrong profile", []string{"-formats", "bson", "-tree-engine", "command_wal_durable"}, "requires command_wal_relaxed"},
		{"wrong iteration count", []string{"-formats", "bson", "-benchtime", "900x"}, "requires a fixed"},
		{"skipped timed", []string{"-formats", "bson", "-skip-timed-matrix"}, "requires the timed matrix"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfig(append([]string{"-native-cow-companion"}, tc.args...))
			if tc.want == "" {
				if err != nil || !cfg.NativeCOWCompanion {
					t.Fatalf("config=%+v err=%v", cfg, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestCanonicalNativeCOWRequiresProofInEverySample(t *testing.T) {
	for _, tc := range []struct {
		name, missing string
		owner         int
		changed       *float64
	}{
		{name: "missing publication delta", missing: "cow_insert_publication_delta", owner: 1},
		{name: "no publication despite positive aggregate", missing: "cow_publications_after_insert", owner: 1, changed: floatPtr(1)},
		{name: "missing checkpoint", missing: "native_checkpoint_calls", owner: 1},
		{name: "checkpoint failure", missing: "native_checkpoint_errors", owner: 1, changed: floatPtr(1)},
		{name: "checkpoint before timer stopped", missing: "native_checkpoint_calls_during_insert", owner: 1, changed: floatPtr(1)},
		{name: "unfinished checkpoint", missing: "native_checkpoint_applied_lsn", owner: 1, changed: floatPtr(2)},
		{name: "unverified count", missing: "verified_shape_docs", owner: 1, changed: floatPtr(31)},
		{name: "missing legacy route", missing: "ordered_root_route_command_wal_publish_used_ops", owner: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, canon, report := testNativeCOWReport(t)
			bench := &report.Sections[0].Benchmarks[tc.owner]
			if tc.changed == nil {
				delete(bench.Runs[1].Metrics, tc.missing)
			} else {
				bench.Runs[1].Metrics[tc.missing] = *tc.changed
			}
			dir := t.TempDir()
			if err := writeJSON(filepath.Join(dir, "collections_report.json"), report); err != nil {
				t.Fatal(err)
			}
			if err := parseTimedMatrixReports(canon, dir, cfg); err == nil || !strings.Contains(err.Error(), "sample 2") {
				t.Fatalf("expected bad second sample rejection, got %v", err)
			}
		})
	}
}

func TestCanonicalNativeCOWTimedOnlyPair(t *testing.T) {
	cfg, canon, report := testNativeCOWReport(t)
	dir := t.TempDir()
	if err := writeJSON(filepath.Join(dir, "collections_report.json"), report); err != nil {
		t.Fatal(err)
	}
	if err := parseTimedMatrixReports(canon, dir, cfg); err != nil {
		t.Fatal(err)
	}
	finalizeRunMetadata(canon)
	canon.Checks = validateCanonicalRun(canon)
	if len(canon.Results) != 2 || hasErrorCheck(canon.Checks) {
		t.Fatalf("valid owner pair rejected: %d rows, checks=%+v", len(canon.Results), canon.Checks)
	}
	if got := renderMarkdownReport(canon); !strings.Contains(got, nativeCOWOwner) ||
		!strings.Contains(got, "compaction.not_requested") || strings.Contains(got, "## Fair Compacted-State Comparison") {
		t.Fatalf("unexpected companion report:\n%s", got)
	}
	canon.Results = canon.Results[:1]
	if checks := validateCanonicalRun(canon); !hasErrorCheck(checks) {
		t.Fatalf("missing native COW peer accepted: %+v", checks)
	}
}

func testNativeCOWReport(t *testing.T) (config, *canonicalRun, collectionReport) {
	t.Helper()
	cfg, err := parseConfig([]string{"-native-cow-companion", "-indexes", "0", "-formats", "bson", "-docs", "32", "-batch-size", "16", "-count", "2"})
	if err != nil {
		t.Fatal(err)
	}
	canon := &canonicalRun{
		Commit: "abcdef", Worktree: t.TempDir(), RunDir: t.TempDir(), GeneratedAt: "fixture", CommandLine: []string{"test"},
		Config: canonicalConfig{NativeCOWCompanion: true, TimedOnly: true, SkipSQLite: true,
			Docs: cfg.Docs, Indexes: cfg.Indexes, Count: cfg.Count, Formats: []string{"bson"}},
	}
	report := collectionReport{
		Status: "ok", BenchmarkEngine: companionProfile, DocumentFormat: "bson", StoragePolicy: companionStorage,
		CollectionBatchSize: cfg.BatchSize, Count: cfg.Count, Commit: canon.Commit, Worktree: canon.Worktree,
		Sections: []reportSection{{Title: "Batch ingest"}},
	}
	for _, name := range []string{legacyShapeBenchmark + "/indexes_0", nativeShapeBenchmark + "/indexes_0"} {
		bench := reportBenchmark{Name: name, MeanNSPerOp: 2000, OpsPerSec: 500000, MeanBytesPerOp: floatPtr(128), MeanAllocsPerOp: floatPtr(4)}
		for range cfg.Count {
			metrics := map[string]float64{
				"stored_docs": 32, "verified_shape_docs": 32, "target_docs/batch": 16, "indexes/doc": 0,
				"checkpoint_ns/op": 1000, "disk_total_bytes": 10000,
			}
			if strings.HasPrefix(name, nativeShapeBenchmark) {
				metrics["native_cow_owner"] = 1
				metrics["native_acknowledged_docs"] = 32
				metrics["native_insert_batches"] = 2
				metrics["cow_publications_before_insert"] = 1
				metrics["cow_publications_after_insert"] = 3
				metrics["cow_insert_publication_delta"] = 2
				metrics["native_accepted_lsn_before_insert"] = 1
				metrics["native_accepted_lsn_after_insert"] = 3
				metrics["native_checkpoint_applied_lsn"] = 3
				metrics["native_checkpoint_calls_during_insert"] = 0
				metrics["native_checkpoint_calls"] = 1
				metrics["native_checkpoint_errors"] = 0
			} else {
				metrics["legacy_backend_owner"] = 1
				metrics["ordered_root_route_command_wal_publish_candidate_ops"] = 2
				metrics["ordered_root_route_command_wal_publish_used_ops"] = 2
				metrics["gomaxprocs"] = 4
				metrics["flush_admission_effective_concurrency"] = 2
				metrics["flush_admission_admitted"] = 1
				metrics["flush_admission_span_native"] = 1
				metrics["flush_admission_backlog_coalescing"] = 1
				metrics["flush_span_fallbacks"] = 0
				metrics["ordered_root_span_fallbacks"] = 0
			}
			bench.Runs = append(bench.Runs, benchmarkSample{
				Name: name, Iterations: 32, NsPerOp: 2000,
				BytesPerOp: floatPtr(128), AllocsPerOp: floatPtr(4), Metrics: metrics,
			})
		}
		bench.MeanMetrics = bench.Runs[0].Metrics
		report.Sections[0].Benchmarks = append(report.Sections[0].Benchmarks, bench)
	}
	return cfg, canon, report
}
