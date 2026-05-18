package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/trace"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestRunColumnStoreSuiteWritesArtifactsAndMetricsM11A(t *testing.T) {
	dir := t.TempDir()
	cfg := BenchConfig{
		Keys:                 64,
		BatchSize:            16,
		DBsArg:               "treedb",
		Profile:              "durable",
		Progress:             false,
		SeedUsed:             1,
		CPUProfile:           filepath.Join(dir, "cpu"),
		AllocsProfile:        filepath.Join(dir, "allocs"),
		AllocsProfileRate:    1,
		CheckpointCPUProfile: filepath.Join(dir, "checkpoint_cpu"),
		BlockProfile:         filepath.Join(dir, "block.pprof"),
		BlockProfileRate:     1,
		MutexProfile:         filepath.Join(dir, "mutex.pprof"),
		MutexProfileFraction: 1,
		TraceProfile:         filepath.Join(dir, "trace.out"),
	}

	out, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ProfileDir:    dir,
		ExecutionPath: "native-fastpath",
		ForcedPath:    columnStorePathRowStoreBaseline,
		RunBenchprof:  true,
	})
	if err != nil {
		t.Fatalf("runColumnStoreSuite: %v", err)
	}

	for _, want := range []string{
		"# unified_bench suite: column_store",
		"q1",
		"q4a",
		"q4b",
		"q5_metadata",
		"row_store_baseline",
		"Parity",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("suite output missing %q:\n%s", want, out)
		}
	}

	for _, name := range []string{
		"benchprof_results.json",
		"benchprof_results.md",
		"insights.md",
		"insights.json",
		"insights.html",
		"column_store_results.json",
		"column_store_results.md",
		"column_store_results.html",
		"cpu_column_store_treedb_column_store.pprof",
		"allocs_column_store_treedb_column_store.pprof",
		"checkpoint_cpu_checkpoint_column_store_treedb_column_store.pprof",
		"block.pprof",
		"block_column_store_treedb_column_store.pprof",
		"mutex.pprof",
		"mutex_column_store_treedb_column_store.pprof",
		"trace.out",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected %s: %v", name, err)
		}
	}

	var benchprof benchprofExport
	benchprofData, err := os.ReadFile(filepath.Join(dir, "benchprof_results.json"))
	if err != nil {
		t.Fatalf("read benchprof_results.json: %v", err)
	}
	if err := json.Unmarshal(benchprofData, &benchprof); err != nil {
		t.Fatalf("unmarshal benchprof_results.json: %v", err)
	}
	if len(benchprof.Runs) != 1 {
		t.Fatalf("benchprof runs=%d want 1", len(benchprof.Runs))
	}
	if got := benchprof.Runs[0].Results["column_store"]["TreeDB Column Store"]; got <= 0 {
		t.Fatalf("benchprof column_store aggregate rows/sec=%f want positive", got)
	}
	benchprofMarkdown, err := os.ReadFile(filepath.Join(dir, "benchprof_results.md"))
	if err != nil {
		t.Fatalf("read benchprof_results.md: %v", err)
	}
	if !strings.Contains(string(benchprofMarkdown), "Column store query phase") {
		t.Fatalf("benchprof markdown missing aggregate column_store display name:\n%s", benchprofMarkdown)
	}

	var report columnStoreSuiteReport
	data, err := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
	if err != nil {
		t.Fatalf("read column_store_results.json: %v", err)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal column_store_results.json: %v", err)
	}
	if got, want := report.Suite, "column_store"; got != want {
		t.Fatalf("suite=%q want %q", got, want)
	}
	if got, want := report.Profile, "durable"; got != want {
		t.Fatalf("profile=%q want %q", got, want)
	}
	if got, want := report.PathLabel, "native-fastpath"; got != want {
		t.Fatalf("path_label=%q want %q", got, want)
	}
	if got, want := report.ForcedPath, columnStorePathRowStoreBaseline; got != want {
		t.Fatalf("forced_path=%q want %q", got, want)
	}
	queryMetrics := assertColumnStoreQueryMetricCoverageM11A(t, report.Queries)
	for _, q := range report.Queries {
		if q.PlanLabel != columnStorePathRowStoreBaseline {
			t.Fatalf("query %s plan_label=%q want %q", q.Name, q.PlanLabel, columnStorePathRowStoreBaseline)
		}
		if q.DurationMS < 0 || q.RowsPerSecond < 0 {
			t.Fatalf("query %s has invalid timing metrics: %+v", q.Name, q)
		}
		if q.BytesRead <= 0 {
			t.Fatalf("query %s bytes_read=%d", q.Name, q.BytesRead)
		}
		if q.ScanDurationMS < 0 {
			t.Fatalf("query %s scan_duration_ms=%v", q.Name, q.ScanDurationMS)
		}
		if q.PlannerCandidates == 0 || q.PlannerReason == "" {
			t.Fatalf("query %s missing planner diagnostics: %+v", q.Name, q)
		}
		if q.RowMaterializations != report.Rows {
			t.Fatalf("query %s row_materializations=%d want %d", q.Name, q.RowMaterializations, report.Rows)
		}
	}
	if q := queryMetrics["q5_metadata"]; q.AliasOf != "q5" || q.ImplementationNote == "" {
		t.Fatalf("q5_metadata should be explicitly reported as a q5 alias placeholder: %+v", q)
	}
	if got, want := queryMetrics["q5_metadata"].ProductionHash, queryMetrics["q5"].ProductionHash; got != want {
		t.Fatalf("q5_metadata production hash=%016x want q5 hash=%016x", got, want)
	}
	if got, want := queryMetrics["q5_metadata"].RawHash, queryMetrics["q5"].RawHash; got != want {
		t.Fatalf("q5_metadata raw hash=%016x want q5 hash=%016x", got, want)
	}
	assertColumnStoreParityCoverageM11A(t, report.Parity)
	for name, parity := range report.Parity {
		if !parity.Pass {
			t.Fatalf("parity %s failed: %+v", name, parity)
		}
		if parity.RawHash == 0 || parity.ProductionHash == 0 {
			t.Fatalf("parity %s has zero hash: %+v", name, parity)
		}
	}
	if report.ByteAccounting.CommandWALBytesBeforeCheckpoint == 0 {
		t.Fatalf("expected command WAL bytes in byte accounting: %+v", report.ByteAccounting)
	}
	if report.ByteAccounting.RetainedPayloadBytesNote == "" || report.ByteAccounting.ColumnAssetBytesNote == "" {
		t.Fatalf("expected M11A byte-accounting placeholder notes: %+v", report.ByteAccounting)
	}
	if got, want := report.ByteAccounting.TotalReconstructableBytes, report.ByteAccounting.RetainedPayloadBytes+report.ByteAccounting.ColumnAssetBytes+report.ByteAccounting.ManifestControlBytes; got != want {
		t.Fatalf("total_reconstructable_bytes=%d want retained+column+manifest=%d", got, want)
	}
	var sawReopenRecovery bool
	for _, stage := range report.Stages {
		if stage.Name == "reopen_recovery" {
			sawReopenRecovery = true
		}
		if stage.Name == "reopen" {
			t.Fatalf("stage name should use reopen_recovery, got legacy stage: %+v", stage)
		}
	}
	if !sawReopenRecovery {
		t.Fatalf("missing reopen_recovery stage: %+v", report.Stages)
	}
	if !strings.Contains(string(data), `"command_wal_bytes_before_checkpoint"`) {
		t.Fatalf("column store JSON missing before-checkpoint command WAL label:\n%s", data)
	}
	if !strings.Contains(string(data), `"column_asset_bytes_note"`) || !strings.Contains(string(data), `"retained_payload_bytes_note"`) {
		t.Fatalf("column store JSON missing byte-accounting notes:\n%s", data)
	}
	if strings.Contains(string(data), `"command_wal_bytes":`) {
		t.Fatalf("column store JSON contains ambiguous command WAL label:\n%s", data)
	}
	columnMarkdown, err := os.ReadFile(filepath.Join(dir, "column_store_results.md"))
	if err != nil {
		t.Fatalf("read column_store_results.md: %v", err)
	}
	if !strings.Contains(string(columnMarkdown), "command_wal_bytes_before_checkpoint") {
		t.Fatalf("column store markdown missing before-checkpoint command WAL label:\n%s", columnMarkdown)
	}
	if !strings.Contains(string(columnMarkdown), "column_asset_bytes_note") || !strings.Contains(string(columnMarkdown), "retained_payload_bytes_note") {
		t.Fatalf("column store markdown missing byte-accounting notes:\n%s", columnMarkdown)
	}
	if strings.Contains(string(columnMarkdown), "| `` |") {
		t.Fatalf("column store markdown should render empty notes as placeholder:\n%s", columnMarkdown)
	}
	if report.ByteAccounting.ManifestControlBytes == 0 || report.ByteAccounting.DBTotalBytes == 0 || report.ByteAccounting.DBTotalFiles == 0 {
		t.Fatalf("expected measured manifest/control and DB byte accounting: %+v", report.ByteAccounting)
	}
	if report.Manifest.ActiveGeneration == 0 || report.Manifest.AppliedCommandLSN == 0 {
		t.Fatalf("expected active/recovery-authoritative manifest identity: %+v", report.Manifest)
	}
	if len(report.UnsupportedForcedPaths) == 0 {
		t.Fatalf("expected unsupported forced path labels to be recorded")
	}
	if report.Artifacts.CPUProfile == "" ||
		report.Artifacts.AllocsProfile == "" ||
		report.Artifacts.CheckpointCPUProfile == "" ||
		report.Artifacts.BlockProfile == "" ||
		report.Artifacts.BlockDeltaProfile == "" ||
		report.Artifacts.MutexProfile == "" ||
		report.Artifacts.MutexDeltaProfile == "" ||
		report.Artifacts.TraceProfile == "" {
		t.Fatalf("expected all configured profile artifacts to be recorded: %+v", report.Artifacts)
	}
}

func TestWriteColumnStoreSuiteArtifactsUsesRecordedColumnPathsM11A(t *testing.T) {
	dir := t.TempDir()
	defaultDir := filepath.Join(dir, "default")
	recordedDir := filepath.Join(dir, "recorded")
	report := columnStoreSuiteReport{
		Suite: "column_store",
		Artifacts: columnStoreArtifactPaths{
			ColumnJSON:     filepath.Join(recordedDir, "custom_column.json"),
			ColumnMarkdown: filepath.Join(recordedDir, "custom_column.md"),
			ColumnHTML:     filepath.Join(recordedDir, "custom_column.html"),
		},
	}
	run := BenchRun{
		Config: BenchConfig{Keys: 1, Profile: "durable"},
		Results: map[string]map[string]float64{
			"column_store": {columnStoreSuiteBenchDisplayName: 1},
		},
	}

	if err := writeColumnStoreSuiteArtifacts(defaultDir, "native-fastpath", report, "# column store", run); err != nil {
		t.Fatalf("writeColumnStoreSuiteArtifacts: %v", err)
	}
	for _, path := range []string{
		report.Artifacts.ColumnJSON,
		report.Artifacts.ColumnMarkdown,
		report.Artifacts.ColumnHTML,
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected recorded artifact path %s: %v", path, err)
		}
	}
	for _, name := range []string{
		"column_store_results.json",
		"column_store_results.md",
		"column_store_results.html",
	} {
		if _, err := os.Stat(filepath.Join(defaultDir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected no fallback column artifact %s, stat err=%v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(defaultDir, "benchprof_results.json")); err != nil {
		t.Fatalf("expected benchprof artifact in profile dir: %v", err)
	}
}

func TestColumnStoreBenchRunUsesDurationForAggregateM11A(t *testing.T) {
	run := columnStoreBenchRun(BenchConfig{}, "durable", t.TempDir(), columnStoreSuiteReport{
		Rows:      30,
		BatchSize: 10,
		Queries: []columnStoreQueryMetric{
			{Name: "q1", RowMaterializations: 10, RowsPerSecond: 1, duration: 10 * time.Millisecond},
			{Name: "q2", RowMaterializations: 20, RowsPerSecond: 1, duration: 20 * time.Millisecond},
		},
	}, nil, 0)

	got := run.Results[columnStoreSuiteBenchTestName][columnStoreSuiteBenchDisplayName]
	if got != 1000 {
		t.Fatalf("aggregate rows/sec=%f want 1000 from exact durations", got)
	}
}

func TestRenderColumnStoreSuiteMarkdownCodeListsM11A(t *testing.T) {
	md := renderColumnStoreSuiteMarkdown(columnStoreSuiteReport{
		Profile:                "durable",
		Fixture:                "fixture",
		ForcedPath:             columnStorePathRowStoreBaseline,
		StageSeparatedBoundary: "boundary",
		ByteAccounting: columnStoreByteAccounting{
			ManifestControlMissing: []string{"manifest", "dictionary"},
		},
		SupportedForcedPaths:   []string{"row_store_baseline", "physical_column"},
		UnsupportedForcedPaths: []string{"planner_skipscan", "aggregate_metadata"},
	})

	for _, want := range []string{
		"- manifest_control_missing: `manifest`, `dictionary`",
		"- supported: `row_store_baseline`, `physical_column`",
		"- fail-closed until physical planner paths exist: `planner_skipscan`, `aggregate_metadata`",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q:\n%s", want, md)
		}
	}
}

func TestColumnStoreSuiteRuntimeProfilesDoNotEnableOnCreateFailureM11A(t *testing.T) {
	prevMutexFraction := runtime.SetMutexProfileFraction(0)
	t.Cleanup(func() {
		runtime.SetMutexProfileFraction(prevMutexFraction)
	})

	_, err := startColumnStoreSuiteRuntimeProfiles(BenchConfig{
		MutexProfile:         filepath.Join(t.TempDir(), "missing", "mutex.pprof"),
		MutexProfileFraction: 1,
	})
	if err == nil {
		t.Fatal("expected mutex profile create failure")
	}
	if !strings.Contains(err.Error(), "mutexprofile") {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotPrev := runtime.SetMutexProfileFraction(0); gotPrev != 0 {
		t.Fatalf("mutex profiler was left enabled after create failure: previous fraction=%d", gotPrev)
	}
}

func TestColumnStoreSuiteRuntimeProfilesDoNotChangeMemRateWhenFilteredM11A(t *testing.T) {
	prevRate := runtime.MemProfileRate
	t.Cleanup(func() {
		runtime.MemProfileRate = prevRate
	})
	runtime.MemProfileRate = 4096

	finish, err := startColumnStoreSuiteRuntimeProfiles(BenchConfig{
		AllocsProfile:      filepath.Join(t.TempDir(), "allocs"),
		AllocsProfileRate:  1,
		AllocsProfileTests: map[string]struct{}{"not_column_store": {}},
	})
	if err != nil {
		t.Fatalf("start runtime profiles: %v", err)
	}
	if err := finish(); err != nil {
		t.Fatalf("finish runtime profiles: %v", err)
	}
	if got := runtime.MemProfileRate; got != 4096 {
		t.Fatalf("MemProfileRate changed despite allocs test filter: got %d", got)
	}
}

func TestColumnStoreSuiteCPUProfileStartsAfterProfileBaselinesM11A(t *testing.T) {
	var events []string
	profileTmpDir := t.TempDir()
	newProfilePath := func(prefix string) (string, error) {
		f, err := os.CreateTemp(profileTmpDir, prefix+"_*.pprof")
		if err != nil {
			return "", err
		}
		path := f.Name()
		if err := f.Close(); err != nil {
			_ = os.Remove(path)
			return "", err
		}
		return path, nil
	}
	profileHooks := &benchmarkProfileHooks{
		startCPUProfile: func(_ io.Writer) error {
			events = append(events, "cpu_start")
			return nil
		},
		stopCPUProfile: func() {
			events = append(events, "cpu_stop")
		},
		writeAllocsSnapshotTemp: func(prefix string) (string, error) {
			events = append(events, prefix)
			return newProfilePath(prefix)
		},
		writeRuntimeProfileSnapshotTemp: func(prefix, profileName string) (string, error) {
			events = append(events, prefix)
			return newProfilePath(prefix)
		},
		writeAllocsDeltaProfile: func(basePath, afterPath, outPath string) error {
			events = append(events, "alloc_delta")
			return nil
		},
		writeRuntimeProfileDeltaProfile: func(basePath, afterPath, outPath string) (bool, error) {
			events = append(events, filepath.Base(outPath))
			return true, nil
		},
	}

	fixture, _ := buildColumnStoreSyntheticFixture(8, 1)
	dir := t.TempDir()
	db, err := openColumnStoreSuiteDB(dir)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "events",
		Options: collections.CollectionOptions{
			DocumentFormat:               collections.DocumentFormatJSON,
			DisableIndexedWriteMemtables: true,
			ColumnStore:                  columnStoreSuiteConfig(),
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, fixture, 4); err != nil {
		t.Fatalf("insert fixture: %v", err)
	}
	rawHashes, err := columnStoreReferenceHashes(fixture)
	if err != nil {
		t.Fatalf("reference hashes: %v", err)
	}

	_, parity, parityErr, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{
		CPUProfile:           filepath.Join(profileTmpDir, "cpu"),
		AllocsProfile:        filepath.Join(profileTmpDir, "allocs"),
		AllocsProfileRate:    1,
		BlockProfile:         filepath.Join(profileTmpDir, "block.pprof"),
		MutexProfile:         filepath.Join(profileTmpDir, "mutex.pprof"),
		MutexProfileFraction: 1,
		profileHooks:         profileHooks,
	}, collection, len(fixture), rawHashes, columnStorePathRowStoreBaseline)
	if err != nil {
		t.Fatalf("profiled queries: %v", err)
	}
	if parityErr != nil {
		t.Fatalf("parity: %v", parityErr)
	}
	assertColumnStoreParityCoverageM11A(t, parity)

	indexOf := func(target string) int {
		for i, event := range events {
			if event == target {
				return i
			}
		}
		return -1
	}
	cpuStartIdx := indexOf("cpu_start")
	cpuStopIdx := indexOf("cpu_stop")
	if cpuStartIdx < 0 || cpuStopIdx < 0 {
		t.Fatalf("missing CPU profile events: %v", events)
	}
	for _, baseline := range []string{
		"unified_bench_column_store_allocs_base",
		"unified_bench_column_store_block_base",
		"unified_bench_column_store_mutex_base",
	} {
		idx := indexOf(baseline)
		if idx < 0 {
			t.Fatalf("missing baseline event %s: %v", baseline, events)
		}
		if idx > cpuStartIdx {
			t.Fatalf("expected %s before cpu_start: %v", baseline, events)
		}
	}
	for _, after := range []string{
		"unified_bench_column_store_allocs_after",
		"unified_bench_column_store_block_after",
		"unified_bench_column_store_mutex_after",
	} {
		idx := indexOf(after)
		if idx < 0 {
			t.Fatalf("missing after event %s: %v", after, events)
		}
		if cpuStopIdx > idx {
			t.Fatalf("expected cpu_stop before %s: %v", after, events)
		}
	}
}

func TestColumnStoreSuiteCheckpointCPUProfileUsesResolvedHooksM11A(t *testing.T) {
	var events []string
	profileHooks := benchmarkProfileHooks{
		startCPUProfile: func(_ io.Writer) error {
			events = append(events, "checkpoint_cpu_start")
			return nil
		},
		stopCPUProfile: func() {
			events = append(events, "checkpoint_cpu_stop")
		},
	}
	cfg := BenchConfig{
		CheckpointCPUProfile: filepath.Join(t.TempDir(), "checkpoint_cpu"),
	}

	f, err := startCheckpointCPUProfile(cfg, profileHooks, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	if err != nil {
		t.Fatalf("start checkpoint CPU profile: %v", err)
	}
	profileHooks.stopCPUProfile()
	if err := f.Close(); err != nil {
		t.Fatalf("close checkpoint CPU profile: %v", err)
	}

	if got, want := strings.Join(events, ","), "checkpoint_cpu_start,checkpoint_cpu_stop"; got != want {
		t.Fatalf("checkpoint CPU profile hooks = %s, want %s", got, want)
	}
}

func TestColumnStoreSuiteCheckpointCPUProfileStartFailureRemovesArtifactM11A(t *testing.T) {
	profileHooks := benchmarkProfileHooks{
		startCPUProfile: func(_ io.Writer) error {
			return errors.New("checkpoint start failed")
		},
	}
	cfg := BenchConfig{
		CheckpointCPUProfile: filepath.Join(t.TempDir(), "checkpoint_cpu"),
	}

	f, err := startCheckpointCPUProfile(cfg, profileHooks, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	if err == nil {
		if f != nil {
			_ = f.Close()
		}
		t.Fatal("expected checkpoint CPU profile start failure")
	}
	profilePath := fmt.Sprintf("%s_checkpoint_%s_%s.pprof", cfg.CheckpointCPUProfile, sanitizeProfileSegment(columnStoreSuiteBenchTestName), sanitizeProfileSegment(columnStoreSuiteBenchDBName))
	if _, statErr := os.Stat(profilePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected failed checkpoint CPU profile artifact to be removed, stat err=%v", statErr)
	}
}

func TestColumnStoreSuiteCheckpointCPUProfileNilStartHookReturnsErrorM11A(t *testing.T) {
	cfg := BenchConfig{
		CheckpointCPUProfile: filepath.Join(t.TempDir(), "checkpoint_cpu"),
	}

	f, err := startCheckpointCPUProfile(cfg, benchmarkProfileHooks{}, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	if err == nil {
		if f != nil {
			_ = f.Close()
		}
		t.Fatal("expected checkpoint CPU profile nil hook error")
	}
	if !strings.Contains(err.Error(), "start hook is nil") {
		t.Fatalf("error=%v, want nil hook context", err)
	}
	profilePath := fmt.Sprintf("%s_checkpoint_%s_%s.pprof", cfg.CheckpointCPUProfile, sanitizeProfileSegment(columnStoreSuiteBenchTestName), sanitizeProfileSegment(columnStoreSuiteBenchDBName))
	if _, statErr := os.Stat(profilePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected failed checkpoint CPU profile artifact to be removed, stat err=%v", statErr)
	}
}

func TestColumnStoreSuiteCPUProfileStartFailureRemovesArtifactM11A(t *testing.T) {
	profileHooks := &benchmarkProfileHooks{
		startCPUProfile: func(_ io.Writer) error {
			return errors.New("start failed")
		},
	}

	collection, events, rawHashes := newColumnStoreSuiteTestCollectionM11A(t, 8, 4)
	profilePrefix := filepath.Join(t.TempDir(), "cpu")
	_, _, parityErr, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{
		CPUProfile:   profilePrefix,
		profileHooks: profileHooks,
	}, collection, len(events), rawHashes, columnStorePathRowStoreBaseline)
	if err == nil {
		t.Fatal("expected CPU profile start failure")
	}
	if parityErr != nil {
		t.Fatalf("hard CPU profile error returned as parity error: %v", parityErr)
	}
	profilePath := fmt.Sprintf("%s_%s_%s.pprof", profilePrefix, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	if _, statErr := os.Stat(profilePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected failed CPU profile artifact to be removed, stat err=%v", statErr)
	}
}

func TestColumnStoreSuiteRuntimeTraceStartFailureRemovesArtifactM11A(t *testing.T) {
	activeTracePath := filepath.Join(t.TempDir(), "active_trace.out")
	activeTrace, err := os.Create(activeTracePath)
	if err != nil {
		t.Fatalf("create active trace: %v", err)
	}
	if err := trace.Start(activeTrace); err != nil {
		_ = activeTrace.Close()
		t.Skipf("runtime trace unavailable: %v", err)
	}
	defer func() {
		trace.Stop()
		_ = activeTrace.Close()
	}()

	tracePath := filepath.Join(t.TempDir(), "trace.out")
	finish, err := startColumnStoreSuiteRuntimeProfiles(BenchConfig{TraceProfile: tracePath})
	if err == nil {
		if finish != nil {
			_ = finish()
		}
		t.Fatal("expected trace start failure while trace is already active")
	}
	if _, statErr := os.Stat(tracePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected failed trace artifact to be removed, stat err=%v", statErr)
	}
}

func TestColumnStoreSuiteHardQueryFailureRemovesCPUArtifactM11A(t *testing.T) {
	collection, events, rawHashes := newColumnStoreSuiteTestCollectionM11A(t, 4, 2)
	profileDir := t.TempDir()
	profilePrefix := filepath.Join(profileDir, "cpu")
	profileHooks := &benchmarkProfileHooks{
		startCPUProfile: func(_ io.Writer) error { return nil },
		stopCPUProfile:  func() {},
	}
	queries, parity, parityErr, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{
		CPUProfile:   profilePrefix,
		profileHooks: profileHooks,
	}, collection, len(events)+1, rawHashes, columnStorePathRowStoreBaseline)
	if err == nil {
		t.Fatal("expected hard query error")
	}
	if parityErr != nil {
		t.Fatalf("hard query error returned as parity error: %v", parityErr)
	}
	if queries != nil || parity != nil {
		t.Fatalf("expected no query/parity output on hard failure, queries=%v parity=%v", queries, parity)
	}
	profilePath := fmt.Sprintf("%s_%s_%s.pprof", profilePrefix, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	if _, statErr := os.Stat(profilePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected hard-failure CPU profile artifact to be removed, stat err=%v", statErr)
	}
}

func TestColumnStoreSuiteRuntimeDeltaSkipsEmptyOutputM11A(t *testing.T) {
	collection, events, rawHashes := newColumnStoreSuiteTestCollectionM11A(t, 8, 4)
	profileDir := t.TempDir()
	blockPath := filepath.Join(profileDir, "block.pprof")
	profileHooks := &benchmarkProfileHooks{
		writeRuntimeProfileSnapshotTemp: func(prefix, profileName string) (string, error) {
			path := filepath.Join(profileDir, prefix+".pprof")
			return path, os.WriteFile(path, []byte("snapshot"), 0o644)
		},
		writeRuntimeProfileDeltaProfile: func(basePath, afterPath, outPath string) (bool, error) {
			return false, nil
		},
	}
	_, _, _, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{
		BlockProfile: blockPath,
		profileHooks: profileHooks,
	}, collection, len(events), rawHashes, columnStorePathRowStoreBaseline)
	if err != nil {
		t.Fatalf("empty block delta output should be skipped: %v", err)
	}
	outPath := contentionProfilePath(blockPath, "block", columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	if _, statErr := os.Stat(outPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected empty block delta artifact to be omitted, stat err=%v", statErr)
	}
}

func TestColumnStoreSuiteRuntimeProfilesTrimPaddedPathsM11A(t *testing.T) {
	dir := t.TempDir()
	blockPath := filepath.Join(dir, "block.pprof")
	mutexPath := filepath.Join(dir, "mutex.pprof")
	tracePath := filepath.Join(dir, "trace.out")

	finish, err := startColumnStoreSuiteRuntimeProfiles(BenchConfig{
		BlockProfile:         " \t" + blockPath + "\t ",
		BlockProfileRate:     1,
		MutexProfile:         " \t" + mutexPath + "\t ",
		MutexProfileFraction: 1,
		TraceProfile:         " \t" + tracePath + "\t ",
	})
	if err != nil {
		t.Fatalf("startColumnStoreSuiteRuntimeProfiles: %v", err)
	}
	if err := finish(); err != nil {
		t.Fatalf("finish runtime profiles: %v", err)
	}
	for _, path := range []string{blockPath, mutexPath, tracePath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected trimmed profile path %q to exist: %v", path, err)
		}
	}
}

func TestColumnStoreSuiteProfiledQueriesSkipWhitespaceOnlyRuntimeProfilePathsM11A(t *testing.T) {
	collection, events, rawHashes := newColumnStoreSuiteTestCollectionM11A(t, 8, 4)
	profileHooks := &benchmarkProfileHooks{
		writeRuntimeProfileSnapshotTemp: func(prefix, profileName string) (string, error) {
			t.Fatalf("runtime profile snapshot should not run for whitespace-only %s path", profileName)
			return "", nil
		},
		writeRuntimeProfileDeltaProfile: func(basePath, afterPath, outPath string) (bool, error) {
			t.Fatalf("runtime profile delta should not run for whitespace-only path %q", outPath)
			return false, nil
		},
	}
	queries, parity, parityErr, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{
		BlockProfile: " \t ",
		MutexProfile: " \n ",
		profileHooks: profileHooks,
	}, collection, len(events), rawHashes, columnStorePathRowStoreBaseline)
	if err != nil || parityErr != nil {
		t.Fatalf("whitespace-only runtime profiles should be ignored: err=%v parityErr=%v", err, parityErr)
	}
	assertColumnStoreQueryMetricCoverageM11A(t, queries)
	assertColumnStoreParityCoverageM11A(t, parity)
}

func TestColumnStoreSuiteProfiledQueriesTrimAllocsDeltaPathM11A(t *testing.T) {
	collection, events, rawHashes := newColumnStoreSuiteTestCollectionM11A(t, 8, 4)
	dir := t.TempDir()
	allocsPrefix := filepath.Join(dir, "allocs")
	var gotOutPath string
	profileHooks := &benchmarkProfileHooks{
		writeAllocsSnapshotTemp: func(prefix string) (string, error) {
			path := filepath.Join(dir, prefix+".pprof")
			return path, os.WriteFile(path, []byte("snapshot"), 0o644)
		},
		writeAllocsDeltaProfile: func(basePath, afterPath, outPath string) error {
			gotOutPath = outPath
			return os.WriteFile(outPath, []byte("delta"), 0o644)
		},
	}
	queries, parity, parityErr, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{
		AllocsProfile:     " \t" + allocsPrefix + "\t ",
		AllocsProfileRate: 1,
		profileHooks:      profileHooks,
	}, collection, len(events), rawHashes, columnStorePathRowStoreBaseline)
	if err != nil || parityErr != nil {
		t.Fatalf("profiled queries failed: err=%v parityErr=%v", err, parityErr)
	}
	assertColumnStoreQueryMetricCoverageM11A(t, queries)
	assertColumnStoreParityCoverageM11A(t, parity)
	wantOutPath := fmt.Sprintf("%s_%s_%s.pprof", allocsPrefix, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	if gotOutPath != wantOutPath {
		t.Fatalf("allocs delta out path=%q want %q", gotOutPath, wantOutPath)
	}
	if _, err := os.Stat(wantOutPath); err != nil {
		t.Fatalf("expected trimmed allocs delta artifact: %v", err)
	}
}

func TestColumnStoreSuiteArtifactsTrimRuntimeProfilePathsM11A(t *testing.T) {
	dir := t.TempDir()
	cpuPath := filepath.Join(dir, "cpu")
	allocsPath := filepath.Join(dir, "allocs")
	checkpointPath := filepath.Join(dir, "checkpoint_cpu")
	blockPath := filepath.Join(dir, "block.pprof")
	mutexPath := filepath.Join(dir, "mutex.pprof")
	tracePath := filepath.Join(dir, "trace.out")

	paths := columnStoreArtifactPathsForProfileDir(dir, BenchConfig{
		CPUProfile:           " \t" + cpuPath + "\t ",
		AllocsProfile:        " \t" + allocsPath + "\t ",
		CheckpointCPUProfile: " \t" + checkpointPath + "\t ",
		BlockProfile:         " \t" + blockPath + "\t ",
		MutexProfile:         " \t" + mutexPath + "\t ",
		TraceProfile:         " \t" + tracePath + "\t ",
	})
	if paths.CPUProfile != fmt.Sprintf("%s_%s_%s.pprof", cpuPath, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName) {
		t.Fatalf("cpu artifact path was not trimmed: %+v", paths)
	}
	if paths.AllocsProfile != fmt.Sprintf("%s_%s_%s.pprof", allocsPath, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName) {
		t.Fatalf("allocs artifact path was not trimmed: %+v", paths)
	}
	if paths.CheckpointCPUProfile != fmt.Sprintf("%s_checkpoint_%s_%s.pprof", checkpointPath, sanitizeProfileSegment(columnStoreSuiteBenchTestName), sanitizeProfileSegment(columnStoreSuiteBenchDBName)) {
		t.Fatalf("checkpoint artifact path was not trimmed: %+v", paths)
	}
	if paths.BlockProfile != blockPath || paths.MutexProfile != mutexPath || paths.TraceProfile != tracePath {
		t.Fatalf("runtime artifact paths were not trimmed: %+v", paths)
	}
	if paths.BlockDeltaProfile != contentionProfilePath(blockPath, "block", columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName) {
		t.Fatalf("block delta path=%q", paths.BlockDeltaProfile)
	}
	if paths.MutexDeltaProfile != contentionProfilePath(mutexPath, "mutex", columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName) {
		t.Fatalf("mutex delta path=%q", paths.MutexDeltaProfile)
	}
}

func TestColumnStoreSuiteArtifactsOmitMissingRuntimeDeltaPathsM11A(t *testing.T) {
	dir := t.TempDir()
	blockDeltaPath := filepath.Join(dir, "block_delta.pprof")
	mutexDeltaPath := filepath.Join(dir, "mutex_delta.pprof")
	blockPath := filepath.Join(dir, "block.pprof")
	mutexPath := filepath.Join(dir, "mutex.pprof")
	if err := os.WriteFile(blockDeltaPath, []byte("delta"), 0o644); err != nil {
		t.Fatalf("write block delta: %v", err)
	}

	paths := columnStoreSuitePruneMissingRuntimeDeltaArtifacts(columnStoreArtifactPaths{
		BlockDeltaProfile: blockDeltaPath,
		MutexDeltaProfile: mutexDeltaPath,
		BlockProfile:      blockPath,
		MutexProfile:      mutexPath,
	})
	if paths.BlockDeltaProfile != blockDeltaPath {
		t.Fatalf("block delta path=%q want %q", paths.BlockDeltaProfile, blockDeltaPath)
	}
	if paths.MutexDeltaProfile != "" {
		t.Fatalf("missing mutex delta path should be omitted, got %q", paths.MutexDeltaProfile)
	}
	if paths.BlockProfile != blockPath || paths.MutexProfile != mutexPath {
		t.Fatalf("base runtime profile artifacts should remain advertised: %+v", paths)
	}
}

func TestColumnStoreSuiteArtifactsOmitRuntimeDeltaStatErrorsM11A(t *testing.T) {
	dir := t.TempDir()
	notDir := filepath.Join(dir, "not-dir")
	if err := os.WriteFile(notDir, []byte("file"), 0o644); err != nil {
		t.Fatalf("write not-dir marker: %v", err)
	}
	paths := columnStoreSuitePruneMissingRuntimeDeltaArtifacts(columnStoreArtifactPaths{
		BlockDeltaProfile: filepath.Join(notDir, "block_delta.pprof"),
		MutexDeltaProfile: " \t ",
	})
	if paths.BlockDeltaProfile != "" || paths.MutexDeltaProfile != "" {
		t.Fatalf("stat-error/blank optional delta paths should be omitted: %+v", paths)
	}
}

func TestColumnStoreSuiteProfiledQueriesReturnHardErrorsSeparatelyM11A(t *testing.T) {
	collection, events, rawHashes := newColumnStoreSuiteTestCollectionM11A(t, 4, 2)
	queries, parity, parityErr, err := runColumnStoreSuiteQueriesProfiled(BenchConfig{}, collection, len(events)+1, rawHashes, columnStorePathRowStoreBaseline)
	if err == nil {
		t.Fatal("expected hard query error")
	}
	if parityErr != nil {
		t.Fatalf("hard query error returned as parity error: %v", parityErr)
	}
	if queries != nil || parity != nil {
		t.Fatalf("expected no query/parity output on hard failure, queries=%v parity=%v", queries, parity)
	}
}

func TestColumnStoreQueryHashRejectsUnknownQueryM11A(t *testing.T) {
	_, _, err := columnStoreQueryHash("missing_query", nil)
	if err == nil {
		t.Fatal("expected unknown query to fail")
	}
	if !strings.Contains(err.Error(), "unknown column_store query") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreQueryHashCanonicalizesQ5MetadataAliasM11A(t *testing.T) {
	events := []columnStoreDecodedEvent{
		{TimeUS: 10, Kind: "create", Did: "did:example:a"},
		{TimeUS: 40, Kind: "update", Did: "did:example:a"},
		{TimeUS: 25, Kind: "create", Did: "did:example:b"},
		{TimeUS: 55, Kind: "delete", Did: "did:example:b"},
	}
	q5Hash, q5Count, err := columnStoreQueryHash("q5", events)
	if err != nil {
		t.Fatalf("q5 hash: %v", err)
	}
	aliasHash, aliasCount, err := columnStoreQueryHash("q5_metadata", events)
	if err != nil {
		t.Fatalf("q5_metadata hash: %v", err)
	}
	if q5Hash != aliasHash || q5Count != aliasCount {
		t.Fatalf("q5_metadata hash/count=(%016x,%d) want q5=(%016x,%d)", aliasHash, aliasCount, q5Hash, q5Count)
	}
}

func TestColumnStoreSuiteRejectsForcedColumnPathM11B(t *testing.T) {
	cfg := BenchConfig{Keys: 8, BatchSize: 4, DBsArg: "treedb", Profile: "durable", SeedUsed: 1}
	_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ForcedPath: "serial_column_scan",
	})
	if err == nil {
		t.Fatal("expected forced serial column scan to fail closed")
	}
	if !errors.Is(err, collections.ErrColumnQueryPlanUnsupported) {
		t.Fatalf("expected ErrColumnQueryPlanUnsupported, got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "serial_column_scan") ||
		!strings.Contains(msg, "unsupported") ||
		!strings.Contains(msg, "reason=no durable physical column assets are available") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreSuitePlanKindMapsKnownPathsM11B(t *testing.T) {
	cases := []struct {
		path string
		want collections.ColumnQueryPlanKind
	}{
		{columnStorePathRowStoreBaseline, collections.ColumnQueryPlanRowStoreBaseline},
		{columnStorePathBTreeIndexBaseline, collections.ColumnQueryPlanBTreeIndexBaseline},
		{columnStorePathSerialColumnScan, collections.ColumnQueryPlanSerialColumnScan},
		{columnStorePathAggregateMetadata, collections.ColumnQueryPlanAggregateMetadata},
		{columnStorePathParallelColumnScan, collections.ColumnQueryPlanParallelColumnScan},
	}
	for _, tc := range cases {
		if tc.path != string(tc.want) {
			t.Fatalf("path constant %q diverged from planner kind %q", tc.path, tc.want)
		}
		got, err := columnStoreSuitePlanKind(tc.path)
		if err != nil {
			t.Fatalf("columnStoreSuitePlanKind(%q): %v", tc.path, err)
		}
		if got != tc.want {
			t.Fatalf("columnStoreSuitePlanKind(%q)=%q want %q", tc.path, got, tc.want)
		}
	}
	if _, err := columnStoreSuitePlanKind("future_alias"); err == nil {
		t.Fatal("expected unknown path to fail")
	} else {
		msg := err.Error()
		for _, want := range []string{
			"future_alias",
			"supported=",
			"aliases=",
			"serial-column-scan",
			"aggregate-metadata",
			"fail_closed=",
			"-column-store-path",
		} {
			if !strings.Contains(msg, want) {
				t.Fatalf("unknown path error %q missing %q", msg, want)
			}
		}
	}
}

func TestColumnStoreSuitePathFlagDocumentsAliasesM11B(t *testing.T) {
	f := flag.Lookup("column-store-path")
	if f == nil {
		t.Fatal("missing column-store-path flag")
	}
	for _, want := range []string{"aliases:", "executable:", "fail-closed", "row-store-baseline", "b-tree-index-baseline", "serial-column-scan", "aggregate-metadata", "parallel-column-scan"} {
		if !strings.Contains(f.Usage, want) {
			t.Fatalf("column-store-path help missing %q:\n%s", want, f.Usage)
		}
	}
}

func TestColumnStoreSuiteRunsBTreeIndexBaselineM11B(t *testing.T) {
	dir := t.TempDir()
	cfg := BenchConfig{
		Keys:      64,
		BatchSize: 16,
		DBsArg:    "treedb",
		Profile:   "durable",
		Progress:  false,
		SeedUsed:  1,
	}

	_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ProfileDir:    dir,
		ExecutionPath: "native-fastpath",
		ForcedPath:    columnStorePathBTreeIndexBaseline,
		RunBenchprof:  false,
	})
	if err != nil {
		t.Fatalf("runColumnStoreSuite btree baseline: %v", err)
	}

	var report columnStoreSuiteReport
	data, err := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
	if err != nil {
		t.Fatalf("read column_store_results.json: %v", err)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal column_store_results.json: %v", err)
	}
	queryMetrics := assertColumnStoreQueryMetricCoverageM11A(t, report.Queries)
	for _, q := range report.Queries {
		if q.PlanLabel != columnStorePathBTreeIndexBaseline {
			t.Fatalf("query %s plan_label=%q want %q", q.Name, q.PlanLabel, columnStorePathBTreeIndexBaseline)
		}
		if q.PlannerReason == "" || q.PlannerCandidates == 0 {
			t.Fatalf("query %s missing planner diagnostics: %+v", q.Name, q)
		}
		if q.WorkerCount != 1 {
			t.Fatalf("query %s worker_count=%d want 1 for caller-thread B-tree baseline", q.Name, q.WorkerCount)
		}
		if !strings.Contains(q.PlannerReason, "full-scan B-tree baseline") {
			t.Fatalf("query %s planner_reason=%q want full-scan baseline disclosure", q.Name, q.PlannerReason)
		}
		if q.RowMaterializations != report.Rows {
			t.Fatalf("query %s row_materializations=%d want %d", q.Name, q.RowMaterializations, report.Rows)
		}
		if q.Name != "q5_metadata" && !strings.Contains(q.ImplementationNote, "no_predicate_pushdown") {
			t.Fatalf("query %s missing B-tree baseline implementation note: %+v", q.Name, q)
		}
	}
	if q := queryMetrics["q5_metadata"]; q.AliasOf != "q5" || !strings.Contains(q.ImplementationNote, "no_predicate_pushdown") || !strings.Contains(q.ImplementationNote, "physical_aggregate_metadata_path") {
		t.Fatalf("q5_metadata should report both q5 aliasing and B-tree baseline scan semantics: %+v", q)
	}
	if got, want := queryMetrics["q5_metadata"].ProductionHash, queryMetrics["q5"].ProductionHash; got != want {
		t.Fatalf("q5_metadata production hash=%016x want q5 hash=%016x", got, want)
	}
	for name, parity := range report.Parity {
		if !parity.Pass {
			t.Fatalf("parity %s failed: %+v", name, parity)
		}
	}
}

func TestColumnStoreSuiteQueriesNormalizeForcedPathAliasesM11B(t *testing.T) {
	const rows = 16
	events, _ := buildColumnStoreSyntheticFixture(rows, 1)
	db, err := openColumnStoreSuiteDB(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(columnStoreSuiteCollectionMeta(columnStorePathRowStoreBaseline)); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, events, 8); err != nil {
		t.Fatalf("insert fixture: %v", err)
	}

	rawHashes, err := columnStoreReferenceHashes(events)
	if err != nil {
		t.Fatalf("reference hashes: %v", err)
	}
	queries, parity, err := runColumnStoreSuiteQueries(collection, rows, rawHashes, "row")
	if err != nil {
		t.Fatalf("runColumnStoreSuiteQueries row alias: %v", err)
	}
	if len(queries) == 0 || len(parity) == 0 {
		t.Fatalf("queries=%d parity=%d want non-empty", len(queries), len(parity))
	}
	for _, q := range queries {
		if q.PlanLabel != columnStorePathRowStoreBaseline {
			t.Fatalf("query %s plan_label=%q want %q", q.Name, q.PlanLabel, columnStorePathRowStoreBaseline)
		}
		if q.WorkerCount != 1 {
			t.Fatalf("query %s worker_count=%d want 1 for caller-thread row baseline", q.Name, q.WorkerCount)
		}
	}

	physicalAliases := map[string]collections.ColumnQueryPlanKind{
		"serial":               collections.ColumnQueryPlanSerialColumnScan,
		"serial-column-scan":   collections.ColumnQueryPlanSerialColumnScan,
		"metadata":             collections.ColumnQueryPlanAggregateMetadata,
		"aggregate-metadata":   collections.ColumnQueryPlanAggregateMetadata,
		"parallel":             collections.ColumnQueryPlanParallelColumnScan,
		"parallel-column-scan": collections.ColumnQueryPlanParallelColumnScan,
	}
	for alias, want := range physicalAliases {
		normalized := normalizeColumnStoreSuitePath(alias)
		got, err := columnStoreSuitePlanKind(normalized)
		if err != nil {
			t.Fatalf("columnStoreSuitePlanKind(%q): %v", alias, err)
		}
		if got != want {
			t.Fatalf("columnStoreSuitePlanKind(%q)=%q want %q", alias, got, want)
		}
		if _, _, err := runColumnStoreSuiteQueries(collection, rows, rawHashes, alias); !errors.Is(err, collections.ErrColumnQueryPlanUnsupported) {
			t.Fatalf("physical alias %q err=%v want ErrColumnQueryPlanUnsupported", alias, err)
		}
	}
}

func TestColumnStoreSuiteIndexCandidateCoverageM11B(t *testing.T) {
	for _, name := range columnStoreQueryNames() {
		if got := columnStoreSuiteQueryIndexCandidates(name); len(got) == 0 {
			t.Fatalf("query %s has no B-tree baseline candidate columns", name)
		}
	}
}

func TestColumnStoreSuiteBTreeIndexScanMaterializesOneEntryPerDocumentM11B(t *testing.T) {
	const rows = 2048
	events, _ := buildColumnStoreSyntheticFixture(rows, 1)
	db, err := openColumnStoreSuiteDB(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(columnStoreSuiteCollectionMeta(columnStorePathBTreeIndexBaseline)); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, events, 256); err != nil {
		t.Fatalf("insert fixture: %v", err)
	}

	decoded, materialized, _, err := scanColumnStoreSuiteEventsByIndex(collection, rows, "q1", "kind_idx")
	if err != nil {
		t.Fatalf("scanColumnStoreSuiteEventsByIndex: %v", err)
	}
	if materialized != rows || len(decoded) != rows {
		t.Fatalf("index scan materialized=%d decoded=%d want %d", materialized, len(decoded), rows)
	}
}

func TestColumnStoreSuiteReportsParityMismatchM11A(t *testing.T) {
	dir := t.TempDir()
	cfg := BenchConfig{Keys: 16, BatchSize: 8, DBsArg: "treedb", Profile: "durable", SeedUsed: 1}
	_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ProfileDir:              dir,
		ExecutionPath:           "native-fastpath",
		ForcedPath:              columnStorePathRowStoreBaseline,
		CorruptReferenceForTest: "q1",
	})
	if err == nil {
		t.Fatal("expected parity mismatch")
	}
	if !strings.Contains(err.Error(), "parity mismatch") || !strings.Contains(err.Error(), "q1") {
		t.Fatalf("unexpected parity error: %v", err)
	}

	var report columnStoreSuiteReport
	data, readErr := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
	if readErr != nil {
		t.Fatalf("expected column_store_results.json after mismatch: %v", readErr)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	assertColumnStoreParityCoverageM11A(t, report.Parity)
	q1, ok := report.Parity["q1"]
	if !ok {
		t.Fatal("missing q1 parity entry")
	}
	if q1.Pass {
		t.Fatalf("expected q1 parity to be recorded as failed: %+v", q1)
	}
}

func TestColumnStoreSuiteRejectsUnknownCorruptReferenceM11A(t *testing.T) {
	cfg := BenchConfig{Keys: 8, BatchSize: 4, DBsArg: "treedb", Profile: "durable", SeedUsed: 1}
	_, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ForcedPath:              columnStorePathRowStoreBaseline,
		CorruptReferenceForTest: "missing_query",
	})
	if err == nil {
		t.Fatal("expected unknown corrupt reference query to fail")
	}
	if !strings.Contains(err.Error(), "unknown corrupt reference query") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreSuiteRejectsMixedDBSelectionM11A(t *testing.T) {
	err := validateColumnStoreSuiteDBSelection("treedb,leveldb", "")
	if err == nil {
		t.Fatal("expected mixed DB selection to fail")
	}
	if !strings.Contains(err.Error(), "only supports TreeDB") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreSuiteAppliesDBExclusionsBeforeMixedCheckM11A(t *testing.T) {
	if err := validateColumnStoreSuiteDBSelection("treedb,leveldb", "leveldb"); err != nil {
		t.Fatalf("expected excluded non-TreeDB selection to pass: %v", err)
	}
	if err := validateColumnStoreSuiteDBSelection("treedbcached", ""); err != nil {
		t.Fatalf("expected TreeDB alias selection to pass: %v", err)
	}
	if err := validateColumnStoreSuiteDBSelection("treedb,leveldb", "leveldb,treedbcached"); err == nil || !strings.Contains(err.Error(), "excludes it") {
		t.Fatalf("expected TreeDB alias exclusion to reject suite, got %v", err)
	}
	err := validateColumnStoreSuiteDBSelection("leveldb", "leveldb")
	if err == nil {
		t.Fatal("expected all selected DBs excluded to fail")
	}
	if !strings.Contains(err.Error(), "requires TreeDB") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreSuiteRejectsExcludedTreeDBM11A(t *testing.T) {
	err := validateColumnStoreSuiteDBSelection("all", "treedb")
	if err == nil {
		t.Fatal("expected TreeDB exclusion to fail")
	}
	if !strings.Contains(err.Error(), "excludes it") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnStoreSuiteReportsKeptDataDirM11A(t *testing.T) {
	dir := t.TempDir()
	cfg := BenchConfig{
		Keys:      8,
		BatchSize: 4,
		DBsArg:    "treedb",
		Profile:   "durable",
		Progress:  false,
		SeedUsed:  1,
		KeepDir:   true,
	}
	out, err := runColumnStoreSuite(cfg, columnStoreSuiteOptions{
		ProfileDir:    dir,
		ExecutionPath: "native-fastpath",
		ForcedPath:    columnStorePathRowStoreBaseline,
	})
	if err != nil {
		t.Fatalf("runColumnStoreSuite: %v", err)
	}

	var report columnStoreSuiteReport
	data, err := os.ReadFile(filepath.Join(dir, "column_store_results.json"))
	if err != nil {
		t.Fatalf("read column_store_results.json: %v", err)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	if report.DataDir == "" {
		t.Fatalf("expected kept data dir in report: %+v", report)
	}
	if _, err := os.Stat(report.DataDir); err != nil {
		t.Fatalf("expected kept data dir to exist: %v", err)
	}
	if !strings.Contains(out, "data-dir") {
		t.Fatalf("markdown output missing kept data-dir:\n%s", out)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(report.DataDir)
	})
}

func TestColumnStoreSuiteConfigUsesExplicitAggregateMetadataNamesM11A(t *testing.T) {
	cfg := columnStoreSuiteConfig()
	var names []string
	for _, agg := range cfg.AggregateMetadata {
		names = append(names, agg.Name)
	}
	got := strings.Join(names, ",")
	if !strings.Contains(got, columnStoreSuiteQ5AggregateMin) || !strings.Contains(got, columnStoreSuiteQ5AggregateMax) {
		t.Fatalf("aggregate metadata names=%q", got)
	}
	if strings.Contains(got, "q5_did_time_span,") || strings.HasSuffix(got, "q5_did_time_span") {
		t.Fatalf("aggregate metadata min name is ambiguous: %q", got)
	}
}

func TestColumnStoreSuiteAggregateMetadataRequestUsesRegisteredNameM11B(t *testing.T) {
	cfg := columnStoreSuiteConfig()
	registered := make(map[string]bool, len(cfg.AggregateMetadata))
	for _, agg := range cfg.AggregateMetadata {
		registered[agg.Name] = true
	}
	name := columnStoreSuiteAggregateMetadataName("q5_metadata")
	if name == "" {
		t.Fatal("q5_metadata did not request aggregate metadata")
	}
	if !registered[name] {
		t.Fatalf("q5_metadata requested aggregate metadata %q outside registered names", name)
	}
}

func TestColumnStoreSuiteDirUsageFailsOnMissingPathM11A(t *testing.T) {
	_, _, err := columnStoreSuiteDirUsage(filepath.Join(t.TempDir(), "missing"))
	if err == nil {
		t.Fatal("expected missing path to fail")
	}
}

func TestColumnStoreSuiteManifestControlUsageTreatsMissingFilesAsZeroM11A(t *testing.T) {
	bytes, missing, err := columnStoreSuiteManifestControlUsage(t.TempDir())
	if err != nil {
		t.Fatalf("manifest/control usage: %v", err)
	}
	if bytes != 0 {
		t.Fatalf("manifest/control bytes=%d want 0", bytes)
	}
	if len(missing) != len(columnStoreSuiteManifestControlFiles) {
		t.Fatalf("missing manifest/control files=%v want %v", missing, columnStoreSuiteManifestControlFiles)
	}
}

func TestColumnStoreSuiteREADMEDocumentsCommandM11A(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("README.md"))
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	text := string(data)
	for _, want := range []string{
		"-suite column_store",
		"-column-store-path row_store_baseline",
		"column_store_results.html",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("README missing %q", want)
		}
	}
}

func BenchmarkColumnStoreSuiteRowBaselineQueriesM11B(b *testing.B) {
	benchmarkColumnStoreSuiteQueriesM11B(b, columnStorePathRowStoreBaseline)
}

func BenchmarkColumnStoreSuiteBTreeIndexBaselineQueriesM11B(b *testing.B) {
	benchmarkColumnStoreSuiteQueriesM11B(b, columnStorePathBTreeIndexBaseline)
}

func benchmarkColumnStoreSuiteQueriesM11B(b *testing.B, path string) {
	const rows = 10_000
	const batchSize = 1_000

	events, sourceBytes := buildColumnStoreSyntheticFixture(rows, 1)
	dir := b.TempDir()
	db, err := openColumnStoreSuiteDB(dir)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(columnStoreSuiteCollectionMeta(path)); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, events, batchSize); err != nil {
		b.Fatalf("insert fixture: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		b.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}

	db, err = openColumnStoreSuiteDB(dir)
	if err != nil {
		b.Fatalf("reopen db: %v", err)
	}
	defer db.Close()
	collection, err = collections.NewCollectionManager(db).OpenCollection("events")
	if err != nil {
		b.Fatalf("reopen collection: %v", err)
	}

	rawHashes, err := columnStoreReferenceHashes(events)
	if err != nil {
		b.Fatalf("reference hashes: %v", err)
	}
	queryCount := len(columnStoreQueryNames())
	b.ReportAllocs()
	b.SetBytes(sourceBytes * int64(queryCount))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		queries, parity, err := runColumnStoreSuiteQueries(collection, rows, rawHashes, path)
		if err != nil {
			b.Fatalf("queries: %v", err)
		}
		assertColumnStoreQueryMetricCoverageM11A(b, queries)
		assertColumnStoreParityCoverageM11A(b, parity)
		for name, p := range parity {
			if !p.Pass {
				b.Fatalf("parity failed for %s: %+v", name, p)
			}
		}
	}
	b.ReportMetric(float64(rows*queryCount), "rows/op")
}

func assertColumnStoreQueryMetricCoverageM11A(t testing.TB, queries []columnStoreQueryMetric) map[string]columnStoreQueryMetric {
	t.Helper()
	names := columnStoreQueryNames()
	if len(queries) != len(names) {
		t.Fatalf("query metrics=%d want %d", len(queries), len(names))
	}
	byName := make(map[string]columnStoreQueryMetric, len(queries))
	for _, q := range queries {
		if _, exists := byName[q.Name]; exists {
			t.Fatalf("duplicate query metric for %s", q.Name)
		}
		byName[q.Name] = q
	}
	for _, name := range names {
		if _, ok := byName[name]; !ok {
			t.Fatalf("missing query metric for %s", name)
		}
	}
	if len(byName) != len(names) {
		t.Fatalf("query metrics include unexpected names: %+v", byName)
	}
	return byName
}

func TestColumnStoreQueryNamesReturnsDefensiveCopyM11A(t *testing.T) {
	names := columnStoreQueryNames()
	if len(names) == 0 {
		t.Fatal("columnStoreQueryNames returned no names")
	}
	names[0] = "mutated"
	names = append(names, "extra")

	fresh := columnStoreQueryNames()
	if got, want := fresh[0], "q1"; got != want {
		t.Fatalf("fresh query names first entry = %q, want %q", got, want)
	}
	if len(fresh) != len(columnStoreQueryNameList) {
		t.Fatalf("fresh query names len = %d, want %d", len(fresh), len(columnStoreQueryNameList))
	}
}

func assertColumnStoreParityCoverageM11A(t testing.TB, parity map[string]columnStoreParity) {
	t.Helper()
	names := columnStoreQueryNames()
	if len(parity) != len(names) {
		t.Fatalf("parity entries=%d want %d", len(parity), len(names))
	}
	for _, name := range names {
		if _, ok := parity[name]; !ok {
			t.Fatalf("missing parity entry for %s", name)
		}
	}
}

func newColumnStoreSuiteTestCollectionM11A(t testing.TB, rows, batchSize int) (*collections.Collection, []columnStoreFixtureEvent, map[string]uint64) {
	t.Helper()
	events, _ := buildColumnStoreSyntheticFixture(rows, 1)
	dir := t.TempDir()
	db, err := openColumnStoreSuiteDB(dir)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "events",
		Options: collections.CollectionOptions{
			DocumentFormat:               collections.DocumentFormatJSON,
			DisableIndexedWriteMemtables: true,
			ColumnStore:                  columnStoreSuiteConfig(),
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := insertColumnStoreFixture(collection, events, batchSize); err != nil {
		t.Fatalf("insert fixture: %v", err)
	}
	rawHashes, err := columnStoreReferenceHashes(events)
	if err != nil {
		t.Fatalf("reference hashes: %v", err)
	}
	return collection, events, rawHashes
}
