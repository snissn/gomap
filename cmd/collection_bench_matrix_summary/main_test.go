package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMatrixSummaryRendersBenchmarkMetrics(t *testing.T) {
	dir := t.TempDir()
	cellDir := filepath.Join(dir, "production_fast_data_vlog_index_leaf")
	if err := os.MkdirAll(cellDir, 0o755); err != nil {
		t.Fatalf("mkdir cell: %v", err)
	}
	reportJSON := filepath.Join(cellDir, "collections_report.json")
	reportMarkdown := filepath.Join(cellDir, "collections_report.md")
	if err := os.WriteFile(reportMarkdown, []byte("# report\n"), 0o644); err != nil {
		t.Fatalf("write report md: %v", err)
	}
	if err := os.WriteFile(reportJSON, []byte(`{
  "status": "ok",
  "sections": [
    {
      "benchmarks": [
        {
          "name": "BenchmarkCollectionInsertBatchWithSecondaryIndexes",
          "mean_ns_per_op": 2812.5,
          "mean_bytes_per_op": 1980,
          "mean_allocs_per_op": 30,
          "mean_metrics": {
            "per_item_key_probe_fallback_count": 0,
            "per_item_prefix_probe_fallback_count": 0
          }
        },
        {
          "name": "BenchmarkCollectionOverheadPlanIndexedPrecomputedState",
          "mean_ns_per_op": 596.5,
          "mean_bytes_per_op": 398,
          "mean_allocs_per_op": 5
        },
        {
          "name": "BenchmarkSQLiteInsertBatchWithSecondaryIndexes",
          "mean_ns_per_op": 4100,
          "mean_bytes_per_op": 2048,
          "mean_allocs_per_op": 32,
          "mean_metrics": {
            "target_docs/batch": 8000
          }
        }
      ]
    }
  ]
}`), 0o644); err != nil {
		t.Fatalf("write report json: %v", err)
	}
	indexPath := filepath.Join(dir, "matrix_index.tsv")
	index := strings.Join([]string{
		"cell\tengine\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\treport_md\treport_json\tcpu_profile\tmem_profile",
		"production_fast_data_vlog_index_leaf\tproduction_fast\ttrue\tfalse\t" + reportMarkdown + "\t" + reportJSON + "\t/cpu.pprof\t/mem.pprof",
		"",
	}, "\n")
	if err := os.WriteFile(indexPath, []byte(index), 0o644); err != nil {
		t.Fatalf("write matrix index: %v", err)
	}

	if err := run(config{matrixIndexPath: indexPath, outDir: dir}); err != nil {
		t.Fatalf("run: %v", err)
	}
	md, err := os.ReadFile(filepath.Join(dir, "collections_matrix_summary.md"))
	if err != nil {
		t.Fatalf("read md: %v", err)
	}
	got := string(md)
	for _, want := range []string{
		"`production_fast_data_vlog_index_leaf`",
		"`BenchmarkCollectionInsertBatchWithSecondaryIndexes`",
		"`BenchmarkSQLiteInsertBatchWithSecondaryIndexes`",
		"2,812.5",
		"30",
		"0",
		"[report](production_fast_data_vlog_index_leaf/collections_report.md)",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("markdown missing %q:\n%s", want, got)
		}
	}
	tsv, err := os.ReadFile(filepath.Join(dir, "collections_matrix_summary.tsv"))
	if err != nil {
		t.Fatalf("read tsv: %v", err)
	}
	gotTSV := string(tsv)
	if !strings.Contains(gotTSV, "BenchmarkCollectionInsertBatchWithSecondaryIndexes\t2812.5\t1980\t30\t0\t0\t") {
		t.Fatalf("tsv missing raw numeric row:\n%s", gotTSV)
	}
	if strings.Contains(gotTSV, "2,812") {
		t.Fatalf("tsv should not contain comma-formatted numbers:\n%s", gotTSV)
	}
}

func TestMatrixSummaryFailsOnUnavailableReport(t *testing.T) {
	dir := t.TempDir()
	cellDir := filepath.Join(dir, "production_fast_data_vlog_index_leaf")
	if err := os.MkdirAll(cellDir, 0o755); err != nil {
		t.Fatalf("mkdir cell: %v", err)
	}
	reportJSON := filepath.Join(cellDir, "collections_report.json")
	reportMarkdown := filepath.Join(cellDir, "collections_report.md")
	if err := os.WriteFile(reportMarkdown, []byte("# report\n"), 0o644); err != nil {
		t.Fatalf("write report md: %v", err)
	}
	if err := os.WriteFile(reportJSON, []byte(`{
  "status": "unavailable",
  "unavailable_reason": "N/A before harness bring-up",
  "sections": []
}`), 0o644); err != nil {
		t.Fatalf("write report json: %v", err)
	}
	indexPath := filepath.Join(dir, "matrix_index.tsv")
	index := strings.Join([]string{
		"cell\tengine\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\treport_md\treport_json\tcpu_profile\tmem_profile",
		"production_fast_data_vlog_index_leaf\tproduction_fast\ttrue\tfalse\t" + reportMarkdown + "\t" + reportJSON + "\t/cpu.pprof\t/mem.pprof",
		"",
	}, "\n")
	if err := os.WriteFile(indexPath, []byte(index), 0o644); err != nil {
		t.Fatalf("write matrix index: %v", err)
	}

	err := run(config{matrixIndexPath: indexPath, outDir: dir})
	if err == nil {
		t.Fatal("run got nil error for unavailable report")
	}
	if !strings.Contains(err.Error(), `status "unavailable"`) {
		t.Fatalf("error=%q want unavailable status", err)
	}
}
