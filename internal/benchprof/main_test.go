package benchprof

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseScanOpsMarkdown(t *testing.T) {
	input := `
Full Scan / TreeDB (vlog=off) = 2,432,548
Prefix Scan / TreeDB (vlog=off) = 5,457,443
`
	rows := parseScanOpsMarkdown(input)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	row := rows[0]
	if row.Label != "TreeDB (vlog=off)" {
		t.Fatalf("unexpected label: %q", row.Label)
	}
	if row.FullScan != 2432548 {
		t.Fatalf("unexpected full_scan: %.0f", row.FullScan)
	}
	if row.Prefix != 5457443 {
		t.Fatalf("unexpected prefix_scan: %.0f", row.Prefix)
	}
	wantRatio := 5457443.0 / 2432548.0
	if math.Abs(row.PrefixDiv-wantRatio) > 0.000001 {
		t.Fatalf("unexpected ratio: got %.8f want %.8f", row.PrefixDiv, wantRatio)
	}
}

func TestParsePprofTopOutput(t *testing.T) {
	input := `
Showing nodes accounting for 14.35s, 92.82% of 15.46s total
Dropped 233 nodes (cum <= 0.08s)
      flat  flat%   sum%        cum   cum%
     8.15s 52.72% 52.72%      8.15s 52.72%  runtime.memmove
     1.11s  7.18% 59.90%      1.11s  7.18%  bytes.(*Buffer).Write
`
	got := parsePprofTopOutput(input)
	if got.total != "15.46s" {
		t.Fatalf("unexpected total: %q", got.total)
	}
	if len(got.entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got.entries))
	}
	if got.entries[0].Function != "runtime.memmove" {
		t.Fatalf("unexpected first fn: %q", got.entries[0].Function)
	}
	if got.entries[0].FlatPct != 52.72 {
		t.Fatalf("unexpected first flat pct: %.2f", got.entries[0].FlatPct)
	}
}

func TestDiscoverProfileFiles(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile := func(rel string) {
		path := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	mustWriteFile("cpu_full_scan_treedb_vlog_off.pprof")
	mustWriteFile("nested/cpu_random_delete_treedb_vlog_off.pprof")
	mustWriteFile("checkpoint_cpu_checkpoint_full_scan_treedb_vlog_off.pprof")
	mustWriteFile("allocs_random_read_treedb_vlog_off.pprof")
	mustWriteFile("block_random_delete_treedb_vlog_off.pprof")
	mustWriteFile("mutex_random_delete_treedb_vlog_off.pprof")
	mustWriteFile("block.pprof")
	mustWriteFile("mutex.pprof")
	mustWriteFile("trace.out")

	files, err := discoverProfileFiles(dir, map[string]struct{}{
		"full_scan":     {},
		"random_delete": {},
	})
	if err != nil {
		t.Fatalf("discoverProfileFiles: %v", err)
	}

	if len(files.cpuProfiles) != 3 {
		t.Fatalf("expected 3 cpu profiles, got %d", len(files.cpuProfiles))
	}
	if len(files.allocs) != 1 {
		t.Fatalf("expected 1 alloc profile, got %d", len(files.allocs))
	}
	if len(files.blockProfiles) != 1 {
		t.Fatalf("expected 1 per-test block profile, got %d", len(files.blockProfiles))
	}
	if len(files.mutexProfiles) != 1 {
		t.Fatalf("expected 1 per-test mutex profile, got %d", len(files.mutexProfiles))
	}
	if got := files.blockProfiles[0].Test + "/" + files.blockProfiles[0].DBTag; got != "random_delete/treedb_vlog_off" {
		t.Fatalf("unexpected per-test block profile key: %q", got)
	}
	if got := files.mutexProfiles[0].Test + "/" + files.mutexProfiles[0].DBTag; got != "random_delete/treedb_vlog_off" {
		t.Fatalf("unexpected per-test mutex profile key: %q", got)
	}
	found := map[string]bool{
		"full_scan/treedb_vlog_off":            false,
		"random_delete/treedb_vlog_off":        false,
		"checkpoint/full_scan/treedb_vlog_off": false,
	}
	for _, p := range files.cpuProfiles {
		key := p.Test + "/" + p.DBTag
		if _, ok := found[key]; ok {
			found[key] = true
		}
	}
	for key, ok := range found {
		if !ok {
			t.Fatalf("missing cpu profile %s", key)
		}
	}
	if files.blockPath == "" {
		t.Fatalf("missing block profile")
	}
	if files.mutexPath == "" {
		t.Fatalf("missing mutex profile")
	}
	if files.tracePath == "" {
		t.Fatalf("missing trace profile")
	}
}

func TestSplitProfileTail_UsesKnownTests(t *testing.T) {
	testName, dbTag := splitProfileTail("prefix_scan_treedb_vlog_off", map[string]struct{}{
		"prefix_scan": {},
		"prefix":      {},
	})
	if testName != "prefix_scan" {
		t.Fatalf("unexpected test split: %q", testName)
	}
	if dbTag != "treedb_vlog_off" {
		t.Fatalf("unexpected db split: %q", dbTag)
	}

	testName, dbTag = splitProfileTail("batch_delete_range_treedb", map[string]struct{}{
		"batch_delete_range": {},
		"batch_delete":       {},
	})
	if testName != "batch_delete_range" {
		t.Fatalf("unexpected batch_delete_range test split: %q", testName)
	}
	if dbTag != "treedb" {
		t.Fatalf("unexpected batch_delete_range db split: %q", dbTag)
	}
}

func TestParseAllocsProfileFilename(t *testing.T) {
	got, ok := parseAllocsProfileFilename("allocs_random_write_treedb_vlog_off.pprof", map[string]struct{}{
		"random_write": {},
	})
	if !ok {
		t.Fatalf("expected allocs filename to parse")
	}
	if got.Test != "random_write" {
		t.Fatalf("unexpected test: %q", got.Test)
	}
	if got.DBTag != "treedb_vlog_off" {
		t.Fatalf("unexpected db tag: %q", got.DBTag)
	}

	got, ok = parseAllocsProfileFilename("allocs_batch_delete_range_treedb.pprof", map[string]struct{}{
		"batch_delete_range": {},
	})
	if !ok {
		t.Fatalf("expected batch_delete_range allocs filename to parse")
	}
	if got.Test != "batch_delete_range" || got.DBTag != "treedb" {
		t.Fatalf("unexpected batch_delete_range allocs parse: %+v", got)
	}
}

func TestParseContentionProfileFilename(t *testing.T) {
	tests := map[string]struct{}{
		"random_read_batch": {},
	}
	block, ok := parseContentionProfileFilename("block_random_read_batch_treedb.pprof", "block", tests)
	if !ok {
		t.Fatalf("expected block contention filename to parse")
	}
	if block.Test != "random_read_batch" || block.DBTag != "treedb" || block.Kind != "block" {
		t.Fatalf("unexpected block parse: %+v", block)
	}

	mutex, ok := parseContentionProfileFilename("mutex_random_read_batch_treedb_vlog_off.pprof", "mutex", tests)
	if !ok {
		t.Fatalf("expected mutex contention filename to parse")
	}
	if mutex.Test != "random_read_batch" || mutex.DBTag != "treedb_vlog_off" || mutex.Kind != "mutex" {
		t.Fatalf("unexpected mutex parse: %+v", mutex)
	}
}

func TestBuildInsights_PerTestContentionProfiles(t *testing.T) {
	rep := report{
		BlockProfiles: []pprofSummary{
			{
				Kind:  "block",
				Test:  "random_read_batch",
				DBTag: "treedb",
				TopEntries: []pprofEntry{
					{Function: "runtime.pthread_cond_wait", FlatPct: 52.7},
				},
			},
		},
		MutexProfiles: []pprofSummary{
			{
				Kind:  "mutex",
				Test:  "random_read_batch",
				DBTag: "treedb",
				TopEntries: []pprofEntry{
					{Function: "sync.(*RWMutex).RLock", FlatPct: 34.2},
				},
			},
		},
	}
	insights := buildInsights(rep, nil)
	text := strings.Join(insights, "\n")
	if !strings.Contains(text, "block/random_read_batch/treedb: contention hotspot") {
		t.Fatalf("missing block contention insight: %v", insights)
	}
	if !strings.Contains(text, "mutex/random_read_batch/treedb: contention hotspot") {
		t.Fatalf("missing mutex contention insight: %v", insights)
	}
}

func TestBuildInvestigations_AllocProfiles(t *testing.T) {
	rep := report{
		AllocObjects: []pprofSummary{
			{
				Kind:  "alloc_objects",
				Test:  "random_write",
				DBTag: "treedb_vlog_off",
				TopEntries: []pprofEntry{
					{Flat: "120000", FlatPct: 44.0, Function: "github.com/snissn/gomap/TreeDB/caching.(*DB).noteWriteKey"},
				},
			},
		},
		AllocSpace: []pprofSummary{
			{
				Kind:  "alloc_space",
				Test:  "random_write",
				DBTag: "treedb_vlog_off",
				TopEntries: []pprofEntry{
					{Flat: "512MB", FlatPct: 41.0, Function: "runtime.makeslice"},
				},
			},
		},
	}

	targets, inferred := buildInvestigations(rep)
	if len(targets) == 0 {
		t.Fatalf("expected investigation targets for alloc profiles")
	}
	if len(inferred) == 0 {
		t.Fatalf("expected inferred insights for alloc profiles")
	}
	foundObjects := false
	foundSpace := false
	for _, line := range inferred {
		if strings.Contains(line, "top allocator by count") {
			foundObjects = true
		}
		if strings.Contains(line, "top allocator by bytes") {
			foundSpace = true
		}
	}
	if !foundObjects || !foundSpace {
		t.Fatalf("expected alloc object/space insights, got: %+v", inferred)
	}
}

func TestParseScanOpsResultsJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "benchprof_results.json")
	payload := benchprofResultsFile{
		Runs: []benchprofResultsRun{
			{
				Keys: 800000,
				Results: map[string]map[string]float64{
					"full_scan": {
						"TreeDB (vlog=off)":  1000,
						"LevelDB (block=on)": 500,
					},
					"prefix_scan": {
						"TreeDB (vlog=off)":  1200,
						"LevelDB (block=on)": 550,
					},
				},
			},
		},
	}
	js, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, js, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	rows, err := parseScanOpsResultsJSON(path)
	if err != nil {
		t.Fatalf("parseScanOpsResultsJSON: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestLoadTreeDBStatsMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "benchprof_results.json")
	payload := benchprofResultsFile{
		Runs: []benchprofResultsRun{
			{
				Keys: 800000,
				TreeDBStats: map[string]map[string]string{
					"TreeDB": {
						"treedb.publish.ordered_root_delta_group.root_apply_calls_total":                       "4",
						"treedb.publish.ordered_root_delta_group.root_apply_ns_total":                          "1200",
						"treedb.vlog.mmap_max_mapped_leaf_sealed_segments":                                     "512",
						"treedb.vlog.mmap_max_mapped_leaf_sealed_bytes":                                        "8589934592",
						"treedb.cache.vlog_auto.frames.block_lz4":                                              "9",
						"treedb.cache.vlog_block.k.bucket.lz4.le_1":                                            "9",
						"treedb.cache.flush_span_run.source_point_ops_total":                                   "11",
						"treedb.cache.flush_span_run.planned_point_ops_total":                                  "10",
						"treedb.cache.flush_span_run.target_leaves_split_across_chunks_total":                  "1",
						"treedb.flush_apply.leaf_log_output.append_wait_ns_total":                              "1234",
						"treedb.flush_apply.span_native.scheduler.worker_busy_ns_total":                        "7000",
						"treedb.flush_apply.span_native.scheduler.worker_idle_ns_total":                        "3000",
						"treedb.flush_apply.span_native.scheduler.ready_tasks_total":                           "8",
						"treedb.flush_apply.span_native.scheduler.task_spans_per_task":                         "6.000000",
						"treedb.flush_apply.publish_prepare.ns_total":                                          "44",
						"treedb.flush_apply.publish_final_install.ns_total":                                    "55",
						"treedb.flush_apply.publish_total.ns_total":                                            "99",
						"treedb.flush_apply.reducer_publish.ns_total":                                          "99",
						"treedb.publish.ordered_root_delta_group.publish_prepare_ns_total":                     "66",
						"treedb.flush_apply.span_native.fallback.reason.span_native_not_implemented.ops_total": "10",
					},
				},
			},
		},
	}
	js, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, js, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	stats, err := loadTreeDBStatsMetadata(dir)
	if err != nil {
		t.Fatalf("loadTreeDBStatsMetadata: %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("stats runs=%d want 1: %+v", len(stats), stats)
	}
	if got, want := stats[0].Keys, 800000; got != want {
		t.Fatalf("keys=%d want %d", got, want)
	}
	if got, want := stats[0].DBName, "TreeDB"; got != want {
		t.Fatalf("db name=%q want %q", got, want)
	}
	if got, want := stats[0].Stats["treedb.publish.ordered_root_delta_group.root_apply_calls_total"], "4"; got != want {
		t.Fatalf("root_apply_calls_total=%q want %q", got, want)
	}

	md := renderMarkdown(report{
		GeneratedAt: "now",
		ProfilesDir: dir,
		TreeDBStats: stats,
	})
	if !strings.Contains(md, "## TreeDB Stats Metadata") ||
		!strings.Contains(md, "treedb.publish.ordered_root_delta_group.root_apply_calls_total") ||
		!strings.Contains(md, "treedb.vlog.mmap_max_mapped_leaf_sealed_bytes") ||
		!strings.Contains(md, "treedb.cache.vlog_auto.frames.block_lz4") ||
		!strings.Contains(md, "treedb.cache.vlog_block.k.bucket.lz4.le_1") ||
		!strings.Contains(md, "treedb.cache.flush_span_run.source_point_ops_total") ||
		!strings.Contains(md, "treedb.cache.flush_span_run.planned_point_ops_total") ||
		!strings.Contains(md, "treedb.cache.flush_span_run.target_leaves_split_across_chunks_total") ||
		!strings.Contains(md, "treedb.flush_apply.leaf_log_output.append_wait_ns_total") ||
		!strings.Contains(md, "treedb.flush_apply.span_native.scheduler.worker_busy_ns_total") ||
		!strings.Contains(md, "treedb.flush_apply.span_native.scheduler.worker_idle_ns_total") ||
		!strings.Contains(md, "treedb.flush_apply.span_native.scheduler.ready_tasks_total") ||
		!strings.Contains(md, "treedb.flush_apply.span_native.scheduler.task_spans_per_task") ||
		!strings.Contains(md, "treedb.flush_apply.publish_prepare.ns_total") ||
		!strings.Contains(md, "treedb.flush_apply.publish_final_install.ns_total") ||
		!strings.Contains(md, "treedb.flush_apply.publish_total.ns_total") ||
		!strings.Contains(md, "treedb.flush_apply.reducer_publish.ns_total") ||
		!strings.Contains(md, "treedb.publish.ordered_root_delta_group.publish_prepare_ns_total") ||
		!strings.Contains(md, "treedb.flush_apply.span_native.fallback.reason.span_native_not_implemented.ops_total") {
		t.Fatalf("markdown missing TreeDB stats metadata:\n%s", md)
	}
}

func TestLoadCollectionWorkloadMetadata(t *testing.T) {
	dir := t.TempDir()
	js := []byte(`{"runs":[{"keys":8,"collection_workloads":[` +
		`{"suite":"collection_storage","mode":"typed_column_part","workload":"aggregate","rows":8,"semantic_equivalent":true,"correctness_validated":true,"rows_per_second":123,"queries_per_second":4,"ns_per_op":250,"typed_column_asset_bytes":99,"counters":{"mapped_bytes":9007199254740993,"typed_column_parts_decoded":7,"vector_edges":11,"vector_documents_fetched":3}},` +
		`{"suite":"collection_storage","mode":"document_only","workload":"aggregate","rows":8,"semantic_equivalent":true,"correctness_validated":true,"rows_per_second":23,"queries_per_second":2,"ns_per_op":500}` +
		`] }]}`)
	if err := os.WriteFile(filepath.Join(dir, "benchprof_results.json"), js, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	workloads, err := loadCollectionWorkloadMetadata(dir)
	if err != nil {
		t.Fatalf("loadCollectionWorkloadMetadata: %v", err)
	}
	if len(workloads) != 2 {
		t.Fatalf("workloads=%d want 2: %+v", len(workloads), workloads)
	}
	if workloads[0].Mode != "document_only" || workloads[1].Mode != "typed_column_part" {
		t.Fatalf("workloads not sorted by mode: %+v", workloads)
	}
	if got, want := workloads[1].Counters.MappedBytes, uint64(9007199254740993); got != want {
		t.Fatalf("mapped_bytes=%d want %d", got, want)
	}
	if got, want := workloads[1].Counters.TypedColumnPartsDecoded, int64(7); got != want {
		t.Fatalf("typed_column_parts_decoded=%d want %d", got, want)
	}
	if got, want := workloads[1].Counters.VectorEdges, uint64(11); got != want {
		t.Fatalf("vector_edges=%d want %d", got, want)
	}
	if got, want := workloads[1].Counters.VectorDocumentsFetched, uint64(3); got != want {
		t.Fatalf("vector_documents_fetched=%d want %d", got, want)
	}
	md := renderMarkdown(report{GeneratedAt: "now", ProfilesDir: dir, CollectionWorkloads: workloads})
	if !strings.Contains(md, "## Collection Workload Metadata") || !strings.Contains(md, "typed_column_part") || !strings.Contains(md, "row asset bytes") {
		t.Fatalf("markdown missing collection workload metadata:\n%s", md)
	}
}

func TestLoadCollectionWorkloadMetadataMissingAndDirectory(t *testing.T) {
	missing, err := loadCollectionWorkloadMetadata(t.TempDir())
	if err != nil {
		t.Fatalf("missing metadata err=%v", err)
	}
	if missing != nil {
		t.Fatalf("missing metadata workloads=%v want nil", missing)
	}

	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "benchprof_results.json"), 0o755); err != nil {
		t.Fatalf("mkdir metadata path: %v", err)
	}
	if _, err := loadCollectionWorkloadMetadata(dir); err == nil {
		t.Fatal("directory metadata path returned nil error")
	}
}

func TestLoadTreeDBStatsMetadataMissingAndDirectory(t *testing.T) {
	missingStats, err := loadTreeDBStatsMetadata(t.TempDir())
	if err != nil {
		t.Fatalf("missing metadata err=%v", err)
	}
	if missingStats != nil {
		t.Fatalf("missing metadata stats=%v want nil", missingStats)
	}

	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "benchprof_results.json"), 0o755); err != nil {
		t.Fatalf("mkdir metadata path: %v", err)
	}
	if _, err := loadTreeDBStatsMetadata(dir); err == nil {
		t.Fatal("directory metadata path returned nil error")
	}
}

func TestBuildInvestigations_IteratorOverheadInference(t *testing.T) {
	rep := report{
		OpsRows: []opsRow{
			{
				Label:     "TreeDB (vlog=off)",
				FullScan:  1377396,
				Prefix:    1132616,
				PrefixDiv: 1132616.0 / 1377396.0,
			},
		},
		CPUProfiles: []pprofSummary{
			{
				Kind:  "cpu",
				Test:  "prefix_scan",
				DBTag: "treedb_vlog_off",
				TopEntries: []pprofEntry{
					{Function: "github.com/snissn/gomap/TreeDB/caching.(*DB).Iterator", FlatPct: 5.58},
					{Function: "github.com/snissn/gomap/TreeDB/tree.(*Tree).Iterator", FlatPct: 3.10},
					{Function: "github.com/snissn/gomap/TreeDB/tree.(*Iterator).seek", FlatPct: 2.90},
					{Function: "github.com/snissn/gomap/TreeDB/internal/memtable.(*hashIterator).Seek", FlatPct: 2.40},
					{Function: "runtime.madvise", FlatPct: 76.36},
				},
			},
			{
				Kind:  "cpu",
				Test:  "full_scan",
				DBTag: "treedb_vlog_off",
				TopEntries: []pprofEntry{
					{Function: "github.com/snissn/gomap/TreeDB/caching.(*valueLogIterator).loadValue", FlatPct: 1.20},
				},
			},
		},
	}

	targets, inferred := buildInvestigations(rep)
	if len(inferred) == 0 {
		t.Fatalf("expected at least one inferred insight")
	}
	foundPhrase := false
	for _, in := range inferred {
		if strings.Contains(in, "That points to iterator setup/seek overhead, not value decoding.") {
			foundPhrase = true
			break
		}
	}
	if !foundPhrase {
		t.Fatalf("expected iterator-overhead phrase in inferred insights: %+v", inferred)
	}

	if len(targets) == 0 {
		t.Fatalf("expected investigation targets")
	}

	var foundDBIterator bool
	for _, target := range targets {
		if strings.Contains(target.Function, "(*DB).Iterator") {
			foundDBIterator = true
			if target.File == "" {
				t.Fatalf("expected source file for DB iterator target")
			}
			if target.Line <= 0 {
				t.Fatalf("expected positive source line for DB iterator target, got %d", target.Line)
			}
		}
	}
	if !foundDBIterator {
		t.Fatalf("expected DB iterator target in %+v", targets)
	}
}
