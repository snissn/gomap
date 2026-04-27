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
  "collection_batch_size": 8000,
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
          "name": "BenchmarkCollectionOverheadIndexStateJSONExtraction",
          "mean_ns_per_op": 1412.5,
          "mean_bytes_per_op": 872,
          "mean_allocs_per_op": 24
        },
        {
          "name": "BenchmarkCollectionOverheadPlanIndexedPrecomputedState",
          "mean_ns_per_op": 596.5,
          "mean_bytes_per_op": 398,
          "mean_allocs_per_op": 5
        },
        {
          "name": "BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes",
          "mean_ns_per_op": 8000,
          "mean_bytes_per_op": 2000,
          "mean_allocs_per_op": 30,
          "mean_metrics": {
            "target_docs/checkpoint": 8000,
            "insert_ns/doc": 2500,
            "sync_ns/doc": 5500,
            "per_item_key_probe_fallback_count": 0,
            "per_item_prefix_probe_fallback_count": 0
          }
        }
      ]
    }
  ]
}`), 0o644); err != nil {
		t.Fatalf("write report json: %v", err)
	}
	sqliteCellDir := filepath.Join(dir, "sqlite_wal_normal")
	if err := os.MkdirAll(sqliteCellDir, 0o755); err != nil {
		t.Fatalf("mkdir sqlite cell: %v", err)
	}
	sqliteReportJSON := filepath.Join(sqliteCellDir, "collections_report.json")
	sqliteReportMarkdown := filepath.Join(sqliteCellDir, "collections_report.md")
	if err := os.WriteFile(sqliteReportMarkdown, []byte("# sqlite report\n"), 0o644); err != nil {
		t.Fatalf("write sqlite report md: %v", err)
	}
	if err := os.WriteFile(sqliteReportJSON, []byte(`{
  "status": "ok",
  "collection_batch_size": 8000,
  "sections": [
    {
      "benchmarks": [
        {
          "name": "BenchmarkSQLiteInsertBatchWithSecondaryIndexes",
          "mean_ns_per_op": 4100,
          "mean_bytes_per_op": 2048,
          "mean_allocs_per_op": 32,
          "mean_metrics": {
            "target_docs/batch": 8000
          }
        },
        {
          "name": "BenchmarkSQLiteInsertBatchCheckpointWithSecondaryIndexes",
          "mean_ns_per_op": 8200,
          "mean_bytes_per_op": 4096,
          "mean_allocs_per_op": 40,
          "mean_metrics": {
            "target_docs/batch": 8000
          }
        }
      ]
    }
  ]
}`), 0o644); err != nil {
		t.Fatalf("write sqlite report json: %v", err)
	}
	indexPath := filepath.Join(dir, "matrix_index.tsv")
	index := strings.Join([]string{
		"cell\tengine\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\treport_md\treport_json\tcpu_profile\tmem_profile",
		"production_fast_data_vlog_index_leaf\tproduction_fast\ttrue\tfalse\t" + reportMarkdown + "\t" + reportJSON + "\t/cpu.pprof\t/mem.pprof",
		"sqlite_wal_normal\tsqlite_wal_normal\t-\t-\t" + sqliteReportMarkdown + "\t" + sqliteReportJSON + "\t/sqlite-cpu.pprof\t/sqlite-mem.pprof",
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
		"## User-Facing Throughput",
		"bulk indexed insert",
		"checkpointed indexed insert",
		"355,556",
		"22.5",
		"insert ms/batch",
		"sync ms/batch",
		"64",
		"44",
		"44.44",
		"## Diagnostic Rows",
		"These rows are not user stories.",
		"`BenchmarkCollectionOverheadIndexStateJSONExtraction`",
		"## Raw Matrix",
		"`production_fast_data_vlog_index_leaf`",
		"`BenchmarkCollectionInsertBatchWithSecondaryIndexes`",
		"`BenchmarkSQLiteInsertBatchWithSecondaryIndexes`",
		"2,812.5",
		"30",
		"0",
		"[report](production_fast_data_vlog_index_leaf/collections_report.md)",
		"[report](sqlite_wal_normal/collections_report.md)",
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
	if !strings.Contains(gotTSV, "BenchmarkCollectionInsertBatchWithSecondaryIndexes\t2812.5\t1980\t30\t-\t-\t0\t0\t") {
		t.Fatalf("tsv missing raw numeric row:\n%s", gotTSV)
	}
	if !strings.Contains(gotTSV, "BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes\t8000\t2000\t30\t2500\t5500\t0\t0\t") {
		t.Fatalf("tsv missing checkpoint split row:\n%s", gotTSV)
	}
	if !strings.Contains(gotTSV, "BenchmarkSQLiteInsertBatchWithSecondaryIndexes\t4100\t2048\t32\t-\t-\t") {
		t.Fatalf("tsv missing sqlite numeric row:\n%s", gotTSV)
	}
	if strings.Contains(gotTSV, "2,812") {
		t.Fatalf("tsv should not contain comma-formatted numbers:\n%s", gotTSV)
	}
	userStoryTSV, err := os.ReadFile(filepath.Join(dir, "collections_user_story_summary.tsv"))
	if err != nil {
		t.Fatalf("read user story tsv: %v", err)
	}
	gotUserStoryTSV := string(userStoryTSV)
	if !strings.Contains(gotUserStoryTSV, "bulk indexed insert\tBenchmarkCollectionInsertBatchWithSecondaryIndexes\t8000\t355555.55555555556\t22.5\t-\t-\t44.44444444444444\t2812.5\t") {
		t.Fatalf("user story tsv missing collection throughput row:\n%s", gotUserStoryTSV)
	}
	if !strings.Contains(gotUserStoryTSV, "checkpointed indexed insert\tBenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes\t8000\t125000\t64\t20\t44\t15.625\t8000\t") {
		t.Fatalf("user story tsv missing checkpoint split row:\n%s", gotUserStoryTSV)
	}
	if !strings.Contains(gotUserStoryTSV, "bulk indexed insert\tBenchmarkSQLiteInsertBatchWithSecondaryIndexes\t8000\t243902.43902439025\t32.8\t-\t-\t30.48780487804878\t4100\t") {
		t.Fatalf("user story tsv missing sqlite throughput row:\n%s", gotUserStoryTSV)
	}
}

func TestBuildUserStoryRowsSkipsMissingBatchSize(t *testing.T) {
	rows := []summaryRow{
		{
			Benchmark: "BenchmarkCollectionInsertBatchWithSecondaryIndexes",
			NsPerOp:   2500,
		},
		{
			Benchmark:           "BenchmarkCollectionInsertBatchWithSecondaryIndexes",
			NsPerOp:             2500,
			CollectionBatchSize: 8000,
		},
	}

	got := buildUserStoryRows(rows)
	if len(got) != 1 {
		t.Fatalf("user story rows=%d want 1", len(got))
	}
	if got[0].CollectionBatchSize != 8000 {
		t.Fatalf("batch size=%d want 8000", got[0].CollectionBatchSize)
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

func TestMatrixSummaryFailsOnMissingExpectedBenchmark(t *testing.T) {
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
          "mean_allocs_per_op": 30
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

	err := run(config{matrixIndexPath: indexPath, outDir: dir})
	if err == nil {
		t.Fatal("run got nil error for missing expected benchmark")
	}
	if !strings.Contains(err.Error(), `missing benchmark "BenchmarkCollectionOverheadIndexStateJSONExtraction"`) {
		t.Fatalf("error=%q want missing benchmark", err)
	}
	if !strings.Contains(err.Error(), reportJSON) {
		t.Fatalf("error=%q want report path", err)
	}
}
