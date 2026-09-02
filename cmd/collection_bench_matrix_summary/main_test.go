package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadMatrixIndexResolvesRelativeReportPaths(t *testing.T) {
	dir := t.TempDir()
	cellDir := filepath.Join(dir, "cell_a")
	if err := os.MkdirAll(cellDir, 0o755); err != nil {
		t.Fatalf("mkdir cell: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cellDir, "collections_report.md"), []byte("# report\n"), 0o644); err != nil {
		t.Fatalf("write report markdown: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cellDir, "collections_report.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write report json: %v", err)
	}
	indexPath := filepath.Join(dir, "matrix_index.tsv")
	index := strings.Join([]string{
		"cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json",
		"cell_a\tproduction_fast\tjson\ttrue\ttrue\tprofile/default\tprofile/default\tcell_a/collections_report.md\tcell_a/collections_report.json",
	}, "\n")
	if err := os.WriteFile(indexPath, []byte(index), 0o644); err != nil {
		t.Fatalf("write matrix index: %v", err)
	}

	rows, err := readMatrixIndex(indexPath)
	if err != nil {
		t.Fatalf("readMatrixIndex: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d want 1", len(rows))
	}
	if got, want := rows[0].ReportMarkdownPath, filepath.Join(dir, "cell_a", "collections_report.md"); got != want {
		t.Fatalf("ReportMarkdownPath=%q want %q", got, want)
	}
	if got, want := rows[0].ReportJSONPath, filepath.Join(dir, "cell_a", "collections_report.json"); got != want {
		t.Fatalf("ReportJSONPath=%q want %q", got, want)
	}
}

func TestReadMatrixIndexRejectsEscapingRelativeReportPath(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "matrix_index.tsv")
	index := strings.Join([]string{
		"cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json",
		"cell_a\tproduction_fast\tjson\ttrue\ttrue\tprofile/default\tprofile/default\t../outside.md\tcell_a/collections_report.json",
	}, "\n")
	if err := os.WriteFile(indexPath, []byte(index), 0o644); err != nil {
		t.Fatalf("write matrix index: %v", err)
	}

	_, err := readMatrixIndex(indexPath)
	if err == nil || !strings.Contains(err.Error(), "escapes matrix directory") {
		t.Fatalf("readMatrixIndex err=%v want escape rejection", err)
	}
}

func TestReadMatrixIndexRejectsUnverifiableReportPath(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "matrix_index.tsv")
	index := strings.Join([]string{
		"cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json",
		"cell_a\tproduction_fast\tjson\ttrue\ttrue\tprofile/default\tprofile/default\tcell_a/missing_report.md\tcell_a/collections_report.json",
	}, "\n")
	if err := os.WriteFile(indexPath, []byte(index), 0o644); err != nil {
		t.Fatalf("write matrix index: %v", err)
	}

	_, err := readMatrixIndex(indexPath)
	if err == nil || !strings.Contains(err.Error(), "resolve artifact path") {
		t.Fatalf("readMatrixIndex err=%v want unverifiable path rejection", err)
	}
}

func TestReadMatrixIndexAllowsAbsoluteReportPathInsideMatrixDir(t *testing.T) {
	dir := t.TempDir()
	cellDir := filepath.Join(dir, "cell_a")
	if err := os.MkdirAll(cellDir, 0o755); err != nil {
		t.Fatalf("mkdir cell: %v", err)
	}
	indexPath := filepath.Join(dir, "matrix_index.tsv")
	reportMarkdown := filepath.Join(dir, "collections_report.md")
	if err := os.WriteFile(reportMarkdown, []byte("# report\n"), 0o644); err != nil {
		t.Fatalf("write report markdown: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cellDir, "collections_report.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write report json: %v", err)
	}
	index := strings.Join([]string{
		"cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json",
		"cell_a\tproduction_fast\tjson\ttrue\ttrue\tprofile/default\tprofile/default\t" + reportMarkdown + "\tcell_a/collections_report.json",
	}, "\n")
	if err := os.WriteFile(indexPath, []byte(index), 0o644); err != nil {
		t.Fatalf("write matrix index: %v", err)
	}

	rows, err := readMatrixIndex(indexPath)
	if err != nil {
		t.Fatalf("readMatrixIndex: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d want 1", len(rows))
	}
	if got := rows[0].ReportMarkdownPath; got != reportMarkdown {
		t.Fatalf("ReportMarkdownPath=%q want %q", got, reportMarkdown)
	}
}

func TestReadMatrixIndexAllowsAbsoluteReportPathWithRelativeMatrixIndex(t *testing.T) {
	dir := t.TempDir()
	cellDir := filepath.Join(dir, "cell_a")
	if err := os.MkdirAll(cellDir, 0o755); err != nil {
		t.Fatalf("mkdir cell: %v", err)
	}
	indexPath := filepath.Join(dir, "matrix_index.tsv")
	reportMarkdown := filepath.Join(dir, "collections_report.md")
	index := strings.Join([]string{
		"cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json",
		"cell_a\tproduction_fast\tjson\ttrue\ttrue\tprofile/default\tprofile/default\t" + reportMarkdown + "\tcell_a/collections_report.json",
	}, "\n")
	if err := os.WriteFile(indexPath, []byte(index), 0o644); err != nil {
		t.Fatalf("write matrix index: %v", err)
	}
	if err := os.WriteFile(reportMarkdown, []byte("# report\n"), 0o644); err != nil {
		t.Fatalf("write report markdown: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cellDir, "collections_report.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write report json: %v", err)
	}

	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(filepath.Dir(dir)); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer func() {
		if err := os.Chdir(oldwd); err != nil {
			t.Fatalf("restore wd: %v", err)
		}
	}()

	rows, err := readMatrixIndex(filepath.Join(filepath.Base(dir), "matrix_index.tsv"))
	if err != nil {
		t.Fatalf("readMatrixIndex: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d want 1", len(rows))
	}
	if got := rows[0].ReportMarkdownPath; got != reportMarkdown {
		t.Fatalf("ReportMarkdownPath=%q want %q", got, reportMarkdown)
	}
}

func TestReadMatrixIndexRejectsAbsoluteReportPathOutsideMatrixDir(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "matrix_index.tsv")
	outside := filepath.Join(filepath.Dir(dir), "collections_report.md")
	index := strings.Join([]string{
		"cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json",
		"cell_a\tproduction_fast\tjson\ttrue\ttrue\tprofile/default\tprofile/default\t" + outside + "\tcell_a/collections_report.json",
	}, "\n")
	if err := os.WriteFile(indexPath, []byte(index), 0o644); err != nil {
		t.Fatalf("write matrix index: %v", err)
	}

	_, err := readMatrixIndex(indexPath)
	if err == nil || !strings.Contains(err.Error(), "escapes matrix directory") {
		t.Fatalf("readMatrixIndex err=%v want escape rejection", err)
	}
}

func TestReadMatrixIndexRejectsSymlinkEscapingReportPath(t *testing.T) {
	dir := t.TempDir()
	outsideDir := t.TempDir()
	outside := filepath.Join(outsideDir, "collections_report.md")
	if err := os.WriteFile(outside, []byte("# outside\n"), 0o644); err != nil {
		t.Fatalf("write outside report: %v", err)
	}
	cellDir := filepath.Join(dir, "cell_a")
	if err := os.MkdirAll(cellDir, 0o755); err != nil {
		t.Fatalf("mkdir cell: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(cellDir, "collections_report.md")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	indexPath := filepath.Join(dir, "matrix_index.tsv")
	index := strings.Join([]string{
		"cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json",
		"cell_a\tproduction_fast\tjson\ttrue\ttrue\tprofile/default\tprofile/default\tcell_a/collections_report.md\tcell_a/collections_report.json",
	}, "\n")
	if err := os.WriteFile(indexPath, []byte(index), 0o644); err != nil {
		t.Fatalf("write matrix index: %v", err)
	}

	_, err := readMatrixIndex(indexPath)
	if err == nil || !strings.Contains(err.Error(), "resolved artifact path") {
		t.Fatalf("readMatrixIndex err=%v want symlink escape rejection", err)
	}
}

func TestReadMatrixIndexRejectsVolumeQualifiedReportPath(t *testing.T) {
	volumePath := filepath.Join("C:cell_a", "collections_report.md")
	if filepath.VolumeName(volumePath) == "" {
		t.Skip("host filepath implementation does not expose volume-qualified paths")
	}
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "matrix_index.tsv")
	index := strings.Join([]string{
		"cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json",
		"cell_a\tproduction_fast\tjson\ttrue\ttrue\tprofile/default\tprofile/default\t" + volumePath + "\tcell_a/collections_report.json",
	}, "\n")
	if err := os.WriteFile(indexPath, []byte(index), 0o644); err != nil {
		t.Fatalf("write matrix index: %v", err)
	}

	_, err := readMatrixIndex(indexPath)
	if err == nil || !strings.Contains(err.Error(), "volume-qualified artifact path") {
		t.Fatalf("readMatrixIndex err=%v want volume-qualified path rejection", err)
	}
}

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
  "document_format": "template-v1",
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
            "disk_bytes/doc": 100,
            "disk_total_bytes": 1600000,
            "per_item_key_probe_fallback_count": 0,
            "per_item_prefix_probe_fallback_count": 0,
            "stored_docs": 16000
          }
        },
        {
          "name": "BenchmarkCollectionShapeInsertBatch/indexes_0",
          "mean_ns_per_op": 1000,
          "mean_bytes_per_op": 512,
          "mean_allocs_per_op": 5,
          "mean_metrics": {
            "target_docs/batch": 8000,
            "indexes/doc": 0,
            "stored_docs": 8000,
            "disk_total_bytes": 800000,
            "disk_bytes/doc": 100
          }
        },
        {
          "name": "BenchmarkCollectionShapeInsertBatchSingleStringJSON/indexes_0",
          "mean_ns_per_op": 900,
          "mean_bytes_per_op": 480,
          "mean_allocs_per_op": 4,
          "mean_metrics": {
            "target_docs/batch": 8000,
            "indexes/doc": 0,
            "stored_docs": 8000,
            "disk_total_bytes": 160000,
            "disk_bytes/doc": 20
          }
        },
        {
          "name": "BenchmarkCollectionShapeInsertBatch/indexes_2",
          "mean_ns_per_op": 1500,
          "mean_bytes_per_op": 768,
          "mean_allocs_per_op": 6,
          "mean_metrics": {
            "target_docs/batch": 8000,
            "indexes/doc": 2,
            "stored_docs": 16000,
            "disk_total_bytes": 2400000,
            "disk_bytes/doc": 150,
            "vlog_rewrite_ns/op": 50000000,
            "vlog_gc_ns/op": 10000000,
            "vlog_rewrite_disk_total_bytes_before": 2400000,
            "vlog_rewrite_disk_total_bytes_after": 2600000,
            "vlog_rewrite_disk_total_bytes_delta": 200000,
            "vlog_rewrite_gc_disk_total_bytes_after": 1800000,
            "vlog_rewrite_gc_disk_total_bytes_delta": -600000,
            "vlog_rewrite_gc_vacuum_ns/op": 5000000,
            "vlog_rewrite_gc_vacuum_disk_total_bytes_after": 1200000,
            "vlog_rewrite_gc_vacuum_disk_total_bytes_delta": -1200000
          }
        },
        {
          "name": "BenchmarkCollectionOverheadIndexStateJSONExtraction",
          "mean_ns_per_op": 1412.5,
          "mean_bytes_per_op": 872,
          "mean_allocs_per_op": 24
        },
        {
          "name": "BenchmarkCollectionOverheadIndexStateTemplateV1Extraction",
          "mean_ns_per_op": 250,
          "mean_bytes_per_op": 144,
          "mean_allocs_per_op": 3
        },
        {
          "name": "BenchmarkCollectionOverheadPlanIndexedTemplateV1",
          "mean_ns_per_op": 638,
          "mean_bytes_per_op": 652,
          "mean_allocs_per_op": 1
        },
        {
          "name": "BenchmarkCollectionOverheadPlanIndexedPrecomputedState",
          "mean_ns_per_op": 596.5,
          "mean_bytes_per_op": 398,
          "mean_allocs_per_op": 5
        },
        {
          "name": "BenchmarkCollectionMixedReadWritePrimary",
          "mean_ns_per_op": 1000,
          "mean_bytes_per_op": 512,
          "mean_allocs_per_op": 5,
          "mean_metrics": {
            "writer_docs/sec": 12345
          }
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
	sqliteCell := "sqlite_wal_custom"
	sqliteCellDir := filepath.Join(dir, sqliteCell)
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
          "name": "BenchmarkSQLiteShapeInsertBatchJSON/indexes_2",
          "mean_ns_per_op": 4400,
          "mean_bytes_per_op": 2048,
          "mean_allocs_per_op": 32,
          "mean_metrics": {
            "target_docs/batch": 8000,
            "indexes/doc": 2,
            "stored_docs": 16000,
            "disk_total_bytes": 3200000,
            "disk_bytes/doc": 200,
            "collection_disk_bytes": 1800000,
            "collection_disk_bytes/doc": 112.5,
            "index_disk_bytes": 1400000,
            "index_disk_bytes/doc": 87.5,
            "sqlite_vacuum_ns/op": 25000000,
            "sqlite_vacuum_disk_total_bytes_before": 3200000,
            "sqlite_vacuum_disk_total_bytes_after": 2800000,
            "sqlite_vacuum_disk_total_bytes_delta": -400000
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
        },
        {
          "name": "BenchmarkSQLiteNativeColumnsInsertBatchWithSecondaryIndexes",
          "mean_ns_per_op": 3000,
          "mean_bytes_per_op": 1500,
          "mean_allocs_per_op": 25,
          "mean_metrics": {
            "target_docs/batch": 8000
          }
        },
        {
          "name": "BenchmarkSQLiteNativeColumnsInsertBatchCheckpointWithSecondaryIndexes",
          "mean_ns_per_op": 7000,
          "mean_bytes_per_op": 3000,
          "mean_allocs_per_op": 35,
          "mean_metrics": {
            "target_docs/checkpoint": 8000,
            "insert_ns/doc": 2000,
            "sync_ns/doc": 5000
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
		"cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json\tcpu_profile\tmem_profile",
		"production_fast_data_vlog_index_leaf\tproduction_fast\ttemplate-v1\ttrue\tfalse\tprofile/default\tprofile/default\t" + filepath.Join("production_fast_data_vlog_index_leaf", "collections_report.md") + "\t" + filepath.Join("production_fast_data_vlog_index_leaf", "collections_report.json") + "\t/cpu.pprof\t/mem.pprof",
		sqliteCell + "\twal_custom\tjson\t-\t-\t-\t-\t" + filepath.Join(sqliteCell, "collections_report.md") + "\t" + filepath.Join(sqliteCell, "collections_report.json") + "\t/sqlite-cpu.pprof\t/sqlite-mem.pprof",
		"",
	}, "\n")
	if err := os.WriteFile(indexPath, []byte(index), 0o644); err != nil {
		t.Fatalf("write matrix index: %v", err)
	}

	if err := run(config{matrixIndexPath: indexPath, outDir: dir, availableBenchmarks: true}); err != nil {
		t.Fatalf("run: %v", err)
	}
	md, err := os.ReadFile(filepath.Join(dir, "collections_matrix_summary.md"))
	if err != nil {
		t.Fatalf("read md: %v", err)
	}
	got := string(md)
	for _, want := range []string{
		"## Executive Summary",
		"Fastest bulk indexed insert",
		"Smallest two-index bulk-insert footprint",
		"Largest sqlite_vacuum disk reduction",
		"Largest treedb_vlog_rewrite disk reduction",
		"Fastest diagnostic row",
		"## User-Facing Throughput",
		"bulk indexed insert",
		"checkpointed indexed insert",
		"Format",
		"`template-v1`",
		"355,556",
		"22.5",
		"insert ms/batch",
		"sync ms/batch",
		"Pager chunk",
		"Pager sync",
		"`profile/default`",
		"64",
		"44",
		"44.44",
		"## Disk Usage",
		"| Cell | Engine | Format | Data vlog | Index vlog | Pager chunk | Pager sync | Story | Benchmark | Indexes/doc | Stored docs | Total disk | Total B/doc | Collection disk | Collection B/doc | Index disk | Index B/doc | Split | Report |",
		"`zero_index_delta`",
		"`reported`",
		"2,400,000",
		"800,000",
		"1,400,000",
		"## Maintenance Compaction",
		"| Cell | Engine | Format | Data vlog | Index vlog | Pager chunk | Pager sync | Maintenance | Benchmark | ns/op | ops/sec | GC ns/op | GC ops/sec | Vacuum ns/op | Vacuum ops/sec | Before | After | Delta | After GC | Delta after GC | After GC+vacuum | Delta after GC+vacuum | Frames | Max K | Report |",
		"`treedb_vlog_rewrite`",
		"`sqlite_vacuum`",
		"-600,000",
		"-1,200,000",
		"## Diagnostic Rows",
		"| Cell | Engine | Format | Pager chunk | Pager sync | Diagnostic | ns/doc | ops/sec | B/doc | allocs/doc | Report |",
		"These rows are not user stories.",
		"`BenchmarkCollectionOverheadIndexStateJSONExtraction`",
		"`BenchmarkCollectionOverheadIndexStateTemplateV1Extraction`",
		"`BenchmarkCollectionOverheadPlanIndexedTemplateV1`",
		"## Raw Matrix",
		"| Cell | Engine | Format | Data vlog | Index vlog | Pager chunk | Pager sync | Benchmark | ns/op | ops/sec | B/op | allocs/op | insert ns/doc | insert docs/sec | sync ns/doc | sync docs/sec | writer docs/sec | Key fallbacks | Prefix fallbacks | indexes/doc | stored docs | disk total | disk B/doc | collection disk | collection B/doc | index disk | index B/doc | Report |",
		"`production_fast_data_vlog_index_leaf`",
		"`BenchmarkCollectionInsertBatchWithSecondaryIndexes`",
		"`BenchmarkCollectionShapeInsertBatch/indexes_2`",
		"`BenchmarkCollectionMixedReadWritePrimary`",
		"`BenchmarkSQLiteInsertBatchWithSecondaryIndexes`",
		"`BenchmarkSQLiteShapeInsertBatchJSON/indexes_2`",
		"`BenchmarkSQLiteNativeColumnsInsertBatchWithSecondaryIndexes`",
		"`native-columns`",
		"12,345",
		"2,812.5",
		"30",
		"0",
		"[report](production_fast_data_vlog_index_leaf/collections_report.md)",
		"[report](sqlite_wal_custom/collections_report.md)",
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
	if !strings.Contains(gotTSV, "writer_docs_per_sec") || strings.Contains(strings.SplitN(gotTSV, "\n", 2)[0], "writer_docs/sec") {
		t.Fatalf("tsv should use normalized writer throughput column:\n%s", gotTSV)
	}
	if !strings.Contains(gotTSV, "template-v1\ttrue\tfalse\tprofile/default\tprofile/default\tBenchmarkCollectionInsertBatchWithSecondaryIndexes\t2812.5\t355555.55555555556\t1980\t30\t-\t-\t-\t-\t-\t0\t0\t") {
		t.Fatalf("tsv missing raw numeric row:\n%s", gotTSV)
	}
	if !strings.Contains(gotTSV, "template-v1\ttrue\tfalse\tprofile/default\tprofile/default\tBenchmarkCollectionMixedReadWritePrimary\t1000\t1000000\t512\t5\t-\t-\t-\t-\t12345\t-\t-\t") {
		t.Fatalf("tsv missing mixed writer throughput row:\n%s", gotTSV)
	}
	if !strings.Contains(gotTSV, "json\t-\t-\t-\t-\tBenchmarkSQLiteInsertBatchWithSecondaryIndexes\t4100\t243902.43902439025\t2048\t32\t-\t-\t-\t-\t-\t-\t-\t") {
		t.Fatalf("tsv missing sqlite numeric row:\n%s", gotTSV)
	}
	if !strings.Contains(gotTSV, "native-columns\t-\t-\t-\t-\tBenchmarkSQLiteNativeColumnsInsertBatchWithSecondaryIndexes\t3000\t333333.3333333333\t1500\t25\t-\t-\t-\t-\t-\t-\t-\t") {
		t.Fatalf("tsv missing sqlite native-columns numeric row:\n%s", gotTSV)
	}
	if !strings.Contains(gotTSV, "template-v1\ttrue\tfalse\tprofile/default\tprofile/default\tBenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes\t8000\t125000\t2000\t30\t2500\t400000\t5500\t181818.18181818182\t-\t0\t0\t") {
		t.Fatalf("tsv missing checkpoint split row:\n%s", gotTSV)
	}
	if strings.Contains(gotTSV, "2,812") {
		t.Fatalf("tsv should not contain comma-formatted numbers:\n%s", gotTSV)
	}
	userStoryTSV, err := os.ReadFile(filepath.Join(dir, "collections_user_story_summary.tsv"))
	if err != nil {
		t.Fatalf("read user story tsv: %v", err)
	}
	gotUserStoryTSV := string(userStoryTSV)
	if !strings.Contains(gotUserStoryTSV, "template-v1\ttrue\tfalse\tprofile/default\tprofile/default\tbulk indexed insert\tBenchmarkCollectionInsertBatchWithSecondaryIndexes\t8000\t2812.5\t355555.55555555556\t22.5\t-\t-\t44.44444444444444\t") {
		t.Fatalf("user story tsv missing collection throughput row:\n%s", gotUserStoryTSV)
	}
	if !strings.Contains(gotUserStoryTSV, "template-v1\ttrue\tfalse\tprofile/default\tprofile/default\tcheckpointed indexed insert\tBenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes\t8000\t8000\t125000\t64\t20\t44\t15.625\t") {
		t.Fatalf("user story tsv missing checkpoint split row:\n%s", gotUserStoryTSV)
	}
	if !strings.Contains(gotUserStoryTSV, "json\t-\t-\t-\t-\tbulk indexed insert\tBenchmarkSQLiteInsertBatchWithSecondaryIndexes\t8000\t4100\t243902.43902439025\t32.8\t-\t-\t30.48780487804878\t") {
		t.Fatalf("user story tsv missing sqlite throughput row:\n%s", gotUserStoryTSV)
	}
	if !strings.Contains(gotUserStoryTSV, "native-columns\t-\t-\t-\t-\tbulk indexed insert\tBenchmarkSQLiteNativeColumnsInsertBatchWithSecondaryIndexes\t8000\t3000\t333333.3333333333\t24\t-\t-\t41.666666666666664\t") {
		t.Fatalf("user story tsv missing sqlite native-columns throughput row:\n%s", gotUserStoryTSV)
	}
	if !strings.Contains(gotUserStoryTSV, "native-columns\t-\t-\t-\t-\tcheckpointed indexed insert\tBenchmarkSQLiteNativeColumnsInsertBatchCheckpointWithSecondaryIndexes\t8000\t7000\t142857.14285714287\t56\t16\t40\t17.857142857142858\t") {
		t.Fatalf("user story tsv missing sqlite native-columns checkpoint row:\n%s", gotUserStoryTSV)
	}
	diskUsageTSV, err := os.ReadFile(filepath.Join(dir, "collections_disk_usage_summary.tsv"))
	if err != nil {
		t.Fatalf("read disk usage tsv: %v", err)
	}
	gotDiskUsageTSV := string(diskUsageTSV)
	if !strings.Contains(gotDiskUsageTSV, "template-v1\ttrue\tfalse\tprofile/default\tprofile/default\tbulk indexed insert\tBenchmarkCollectionShapeInsertBatch/indexes_2\t2\t16000\t2400000\t150\t1600000\t100\t800000\t50\tzero_index_delta\t") {
		t.Fatalf("disk usage tsv missing TreeDB zero-index delta row:\n%s", gotDiskUsageTSV)
	}
	if !strings.Contains(gotDiskUsageTSV, "json\t-\t-\t-\t-\tbulk indexed insert\tBenchmarkSQLiteShapeInsertBatchJSON/indexes_2\t2\t16000\t3200000\t200\t1800000\t112.5\t1400000\t87.5\treported\t") {
		t.Fatalf("disk usage tsv missing SQLite reported split row:\n%s", gotDiskUsageTSV)
	}
	maintenanceTSV, err := os.ReadFile(filepath.Join(dir, "collections_maintenance_summary.tsv"))
	if err != nil {
		t.Fatalf("read maintenance tsv: %v", err)
	}
	gotMaintenanceTSV := string(maintenanceTSV)
	if !strings.Contains(gotMaintenanceTSV, "template-v1\ttrue\tfalse\tprofile/default\tprofile/default\ttreedb_vlog_rewrite\tBenchmarkCollectionShapeInsertBatch/indexes_2\t50000000\t20\t10000000\t100\t5000000\t200\t2400000\t2600000\t200000\t1800000\t-600000\t1200000\t-1200000\t") {
		t.Fatalf("maintenance tsv missing TreeDB rewrite row:\n%s", gotMaintenanceTSV)
	}
	if !strings.Contains(gotMaintenanceTSV, "json\t-\t-\t-\t-\tsqlite_vacuum\tBenchmarkSQLiteShapeInsertBatchJSON/indexes_2\t25000000\t40\t-\t-\t-\t-\t3200000\t2800000\t-400000\t-\t-\t-\t-\t") {
		t.Fatalf("maintenance tsv missing SQLite vacuum row:\n%s", gotMaintenanceTSV)
	}
	for name, content := range map[string]string{
		"raw":         gotTSV,
		"user story":  gotUserStoryTSV,
		"disk usage":  gotDiskUsageTSV,
		"maintenance": gotMaintenanceTSV,
	} {
		if strings.Contains(content, dir) {
			t.Fatalf("%s tsv should use portable report paths, got absolute temp dir:\n%s", name, content)
		}
		if !strings.Contains(content, "production_fast_data_vlog_index_leaf/collections_report.md") {
			t.Fatalf("%s tsv missing relative TreeDB report path:\n%s", name, content)
		}
	}
}

func TestMatrixSummaryAllowsLegacySQLiteReportsWithoutNativeColumns(t *testing.T) {
	dir := t.TempDir()
	sqliteCell := "sqlite_wal_normal"
	sqliteCellDir := filepath.Join(dir, sqliteCell)
	if err := os.MkdirAll(sqliteCellDir, 0o755); err != nil {
		t.Fatalf("mkdir sqlite cell: %v", err)
	}
	sqliteReportMarkdown := filepath.Join(sqliteCellDir, "collections_report.md")
	if err := os.WriteFile(sqliteReportMarkdown, []byte("# sqlite report\n"), 0o644); err != nil {
		t.Fatalf("write sqlite report md: %v", err)
	}
	sqliteReportJSON := filepath.Join(sqliteCellDir, "collections_report.json")
	if err := os.WriteFile(sqliteReportJSON, []byte(`{
  "status": "ok",
  "document_format": "json",
  "collection_batch_size": 8000,
  "sections": [
    {
      "benchmarks": [
        {
          "name": "BenchmarkSQLiteInsertBatchWithSecondaryIndexes",
          "mean_ns_per_op": 4100,
          "mean_bytes_per_op": 2048,
          "mean_allocs_per_op": 32
        },
        {
          "name": "BenchmarkSQLiteInsertBatchCheckpointWithSecondaryIndexes",
          "mean_ns_per_op": 8200,
          "mean_bytes_per_op": 4096,
          "mean_allocs_per_op": 40
        }
      ]
    }
  ]
}`), 0o644); err != nil {
		t.Fatalf("write sqlite report json: %v", err)
	}
	indexPath := filepath.Join(dir, "matrix_index.tsv")
	index := strings.Join([]string{
		"cell\tengine\tdocument_format\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json\tcpu_profile\tmem_profile",
		sqliteCell + "\tsqlite_wal_normal\tjson\t-\t-\t-\t-\t" + filepath.Join(sqliteCell, "collections_report.md") + "\t" + filepath.Join(sqliteCell, "collections_report.json") + "\t/sqlite-cpu.pprof\t/sqlite-mem.pprof",
		"",
	}, "\n")
	if err := os.WriteFile(indexPath, []byte(index), 0o644); err != nil {
		t.Fatalf("write matrix index: %v", err)
	}

	if err := run(config{matrixIndexPath: indexPath, outDir: dir}); err != nil {
		t.Fatalf("run legacy sqlite report: %v", err)
	}
	tsv, err := os.ReadFile(filepath.Join(dir, "collections_matrix_summary.tsv"))
	if err != nil {
		t.Fatalf("read tsv: %v", err)
	}
	got := string(tsv)
	if !strings.Contains(got, "BenchmarkSQLiteInsertBatchWithSecondaryIndexes") {
		t.Fatalf("legacy sqlite summary missing json row:\n%s", got)
	}
	if strings.Contains(got, "BenchmarkSQLiteNativeColumns") {
		t.Fatalf("legacy sqlite summary should not synthesize native-columns rows:\n%s", got)
	}
}

func TestDocumentFormatForSQLiteShapeNativeColumns(t *testing.T) {
	for _, benchmark := range []string{
		"BenchmarkSQLiteShapeInsertBatchNativeColumns/indexes_2",
		"BenchmarkSQLiteShapeInsertBatchCheckpointNativeColumns/indexes_2",
		"BenchmarkSQLiteShapeReadPrimaryNativeColumns/indexes_2",
		"BenchmarkSQLiteShapeSecondaryLookupNativeColumns/unique",
	} {
		defaultFormat := "json"
		if got := documentFormatForBenchmark(defaultFormat, benchmark); got != "native-columns" {
			t.Fatalf("documentFormatForBenchmark(%q, %q)=%q want native-columns", defaultFormat, benchmark, got)
		}
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
		"cell\tengine\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json\tcpu_profile\tmem_profile",
		"production_fast_data_vlog_index_leaf\tproduction_fast\ttrue\tfalse\tprofile/default\tprofile/default\t" + filepath.Join("production_fast_data_vlog_index_leaf", "collections_report.md") + "\t" + filepath.Join("production_fast_data_vlog_index_leaf", "collections_report.json") + "\t/cpu.pprof\t/mem.pprof",
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
		"cell\tengine\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tpager_chunk_size\tpager_sync_concurrency\treport_md\treport_json\tcpu_profile\tmem_profile",
		"production_fast_data_vlog_index_leaf\tproduction_fast\ttrue\tfalse\tprofile/default\tprofile/default\t" + filepath.Join("production_fast_data_vlog_index_leaf", "collections_report.md") + "\t" + filepath.Join("production_fast_data_vlog_index_leaf", "collections_report.json") + "\t/cpu.pprof\t/mem.pprof",
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

func TestExecutiveSummaryDoesNotLabelPositiveMaintenanceDeltaAsReduction(t *testing.T) {
	delta := 170.0
	after := 6764795.0
	lines := buildExecutiveSummary(nil, nil, []maintenanceRow{
		{
			summaryRow: summaryRow{
				matrixRow: matrixRow{
					Cell:           "treedb_json",
					DocumentFormat: "json",
				},
			},
			Kind:    "treedb_vlog_rewrite",
			DeltaGC: &delta,
			AfterGC: &after,
		},
	}, nil)

	got := strings.Join(lines, "\n")
	if strings.Contains(got, "Largest treedb_vlog_rewrite disk reduction") {
		t.Fatalf("positive delta was labeled as a reduction:\n%s", got)
	}
	if !strings.Contains(got, "No treedb_vlog_rewrite disk reduction was observed") {
		t.Fatalf("missing no-reduction summary:\n%s", got)
	}
}

func TestExecutiveSummaryUsesGCRateForGCDelta(t *testing.T) {
	delta := 120.0
	after := 1120.0
	deltaGC := -100.0
	afterGC := 900.0
	maintNs := 1_000_000_000.0
	gcNs := 250_000_000.0
	lines := buildExecutiveSummary(nil, nil, []maintenanceRow{
		{
			summaryRow: summaryRow{
				matrixRow: matrixRow{
					Cell:           "treedb_json",
					DocumentFormat: "json",
				},
			},
			Kind:       "treedb_leafgen_pack_gc",
			NsPerMaint: &maintNs,
			GCNs:       &gcNs,
			Delta:      &delta,
			After:      &after,
			DeltaGC:    &deltaGC,
			AfterGC:    &afterGC,
		},
	}, nil)

	got := strings.Join(lines, "\n")
	if !strings.Contains(got, "changed total disk by -100 to 900") {
		t.Fatalf("summary did not use GC disk delta:\n%s", got)
	}
	if !strings.Contains(got, "(4 ops/sec maintenance rate)") {
		t.Fatalf("summary did not use GC rate:\n%s", got)
	}
}
