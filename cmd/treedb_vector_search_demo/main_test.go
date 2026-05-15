package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecuteSmokeCompactsReopensValidatesAndBenchmarks(t *testing.T) {
	res, err := execute(context.Background(), config{
		dir:                   t.TempDir(),
		keepDir:               true,
		docs:                  128,
		dimensions:            16,
		queries:               8,
		readOps:               16,
		readConcurrency:       []int{2},
		validateQueries:       4,
		validateDocs:          4,
		topK:                  5,
		batchSize:             32,
		m:                     8,
		efConstruction:        64,
		efSearch:              64,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.5,
		compact:               true,
		disableExactFallback:  true,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Validation.DocumentsChecked != 4 || res.Validation.QueriesChecked != 4 {
		t.Fatalf("validation counts=%+v, want 4 docs and 4 queries", res.Validation)
	}
	if res.Validation.Recall < res.Validation.MinRecall {
		t.Fatalf("recall=%f below min=%f", res.Validation.Recall, res.Validation.MinRecall)
	}
	if res.Search.Queries != 8 || res.Search.AvgNanos <= 0 || res.Search.ExactFallbacks != 0 {
		t.Fatalf("unexpected search benchmark result: %+v", res.Search)
	}
	if len(res.ReadBenchmarks) != 2 || res.ReadBenchmarks[0].Concurrency != 1 || res.ReadBenchmarks[1].Concurrency != 2 {
		t.Fatalf("unexpected read benchmarks: %+v", res.ReadBenchmarks)
	}
	if res.Profile != "bench" {
		t.Fatalf("profile=%q want bench", res.Profile)
	}
	if res.ValuePointerThreshold != defaultValuePointerThreshold {
		t.Fatalf("value pointer threshold=%d want %d", res.ValuePointerThreshold, defaultValuePointerThreshold)
	}
	if res.LeafGenerationTarget != defaultLeafGenerationTarget {
		t.Fatalf("leaf generation target=%d want %d", res.LeafGenerationTarget, defaultLeafGenerationTarget)
	}
	if res.StorageAfterCompact.TotalBytes <= 0 || res.StorageAfterCompact.BytesPerDoc <= 0 {
		t.Fatalf("missing compacted storage report: %+v", res.StorageAfterCompact)
	}
	if res.FormatConfig == nil {
		t.Fatal("missing format config report")
	}
	if !res.FormatConfig.IndexOuterLeavesInValueLog {
		t.Fatalf("index_outer_leaves_in_vlog=false, want true: %+v", res.FormatConfig)
	}
	if !res.FormatConfig.LeafPrefixCompression {
		t.Fatalf("leaf_prefix_compression=false, want true: %+v", res.FormatConfig)
	}
	if res.StorageExpectation.IndexBytes <= 0 {
		t.Fatalf("missing storage expectation index bytes: %+v", res.StorageExpectation)
	}
	if res.StorageExpectation.LeafVLogBytes <= 0 {
		t.Fatalf("missing leaf value-log bytes: %+v", res.StorageExpectation)
	}
	if res.Memory.IndexBytesMemory <= 0 {
		t.Fatalf("missing index memory report: %+v", res.Memory)
	}
	if res.CompactStorage == nil || !res.CompactStorage.FullyCompacted {
		t.Fatalf("compact storage stats=%+v, want fully compacted", res.CompactStorage)
	}
	if res.IndexStatsLoaded.LiveDocs != 128 {
		t.Fatalf("loaded live docs=%d want 128", res.IndexStatsLoaded.LiveDocs)
	}
}

func TestRunJSONOutput(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run([]string{
		"-matrix=false",
		"-dir", t.TempDir(),
		"-keep-dir",
		"-docs", "64",
		"-dims", "8",
		"-queries", "4",
		"-validate-queries", "2",
		"-validate-docs", "2",
		"-top-k", "3",
		"-m", "4",
		"-ef-construction", "32",
		"-ef-search", "32",
		"-min-recall", "0.5",
		"-json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v stderr=%s", err, stderr.String())
	}
	var res result
	if err := json.Unmarshal(stdout.Bytes(), &res); err != nil {
		t.Fatalf("decode JSON output: %v\n%s", err, stdout.String())
	}
	if res.Profile != "bench" || res.Docs != 64 || res.Search.Queries != 4 || res.StorageAfterCompact.TotalBytes <= 0 {
		t.Fatalf("unexpected JSON result: %+v", res)
	}
	if res.ValuePointerThreshold != defaultValuePointerThreshold {
		t.Fatalf("JSON value pointer threshold=%d want %d", res.ValuePointerThreshold, defaultValuePointerThreshold)
	}
	if res.LeafGenerationTarget != defaultLeafGenerationTarget {
		t.Fatalf("JSON leaf generation target=%d want %d", res.LeafGenerationTarget, defaultLeafGenerationTarget)
	}
	if res.FormatConfig == nil || !res.FormatConfig.IndexOuterLeavesInValueLog || !res.FormatConfig.LeafPrefixCompression {
		t.Fatalf("unexpected JSON format config: %+v", res.FormatConfig)
	}
}

func TestExecuteRequireLeafVLogBytesPassesWithDefaultBenchProfile(t *testing.T) {
	res, err := execute(context.Background(), config{
		dir:                   t.TempDir(),
		keepDir:               true,
		docs:                  64,
		dimensions:            8,
		queries:               2,
		readOps:               8,
		readConcurrency:       []int{2},
		validateQueries:       1,
		validateDocs:          1,
		topK:                  3,
		batchSize:             32,
		m:                     4,
		efConstruction:        32,
		efSearch:              32,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.5,
		compact:               true,
		disableExactFallback:  true,
		requireLeafVLogBytes:  true,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Profile != "bench" {
		t.Fatalf("profile=%q want bench", res.Profile)
	}
	if res.StorageExpectation.LeafVLogBytes <= 0 {
		t.Fatalf("missing leaf value-log bytes: %+v", res.StorageExpectation)
	}
}

func TestExecuteRejectsMainDBDir(t *testing.T) {
	root := filepath.Join(t.TempDir(), "demo-root")
	maindb := filepath.Join(root, "maindb")
	if err := os.MkdirAll(filepath.Join(root, "dictdb"), 0o755); err != nil {
		t.Fatalf("mkdir stale dictdb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "dictdb", "stale"), []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale side-store file: %v", err)
	}

	_, err := execute(context.Background(), config{
		dir:                   maindb,
		keepDir:               true,
		docs:                  64,
		dimensions:            8,
		queries:               2,
		readOps:               8,
		readConcurrency:       []int{2},
		validateQueries:       1,
		validateDocs:          1,
		topK:                  3,
		batchSize:             32,
		m:                     4,
		efConstruction:        32,
		efSearch:              32,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.5,
		compact:               true,
		disableExactFallback:  true,
	})
	if err == nil {
		t.Fatal("execute accepted maindb path, want error")
	}
	if !strings.Contains(err.Error(), "TreeDB root directory") {
		t.Fatalf("error=%v, want TreeDB root directory guidance", err)
	}
	if _, err := os.Stat(filepath.Join(root, "dictdb", "stale")); err != nil {
		t.Fatalf("stale side-store file err=%v, want untouched after rejected maindb path", err)
	}
}

func TestParseProfileRejectsUnsupportedProfile(t *testing.T) {
	_, err := parseProfile("raw")
	if err == nil {
		t.Fatal("parseProfile succeeded, want error")
	}
	if !strings.Contains(err.Error(), "unsupported -profile") {
		t.Fatalf("error=%v, want unsupported -profile", err)
	}
}

func TestParseConfigValuePointerThreshold(t *testing.T) {
	cfg, err := parseConfig([]string{"-value-pointer-threshold", "2048"})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.valuePointerThreshold != 2048 {
		t.Fatalf("value pointer threshold=%d want 2048", cfg.valuePointerThreshold)
	}
	if _, err := parseConfig([]string{"-value-pointer-threshold", "-1"}); err == nil {
		t.Fatal("parseConfig accepted negative value pointer threshold")
	}
}

func TestParseConfigLeafGenerationSegmentTarget(t *testing.T) {
	cfg, err := parseConfig([]string{"-leaf-generation-segment-target", "8388608"})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.leafGenerationTarget != 8388608 {
		t.Fatalf("leaf generation target=%d want 8388608", cfg.leafGenerationTarget)
	}
	if _, err := parseConfig([]string{"-leaf-generation-segment-target", "-1"}); err == nil {
		t.Fatal("parseConfig accepted negative leaf generation target")
	}
}

func TestRunMatrixJSONOutput(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run([]string{
		"-dir", t.TempDir(),
		"-keep-dir",
		"-docs", "48",
		"-dims", "8",
		"-queries", "2",
		"-read-ops", "12",
		"-read-concurrency", "2,4",
		"-validate-queries", "1",
		"-validate-docs", "1",
		"-top-k", "3",
		"-m", "4",
		"-ef-construction", "32",
		"-ef-search", "32",
		"-min-recall", "0.5",
		"-json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run matrix: %v stderr=%s", err, stderr.String())
	}
	var res matrixResult
	if err := json.Unmarshal(stdout.Bytes(), &res); err != nil {
		t.Fatalf("decode matrix JSON output: %v\n%s", err, stdout.String())
	}
	if len(res.Cases) != 3 {
		t.Fatalf("matrix cases=%d want 3", len(res.Cases))
	}
	wantNames := []string{"index_db_outer_leaves", "leaf_vlog_before_compact", "leaf_vlog_after_compact"}
	for i, want := range wantNames {
		if res.Cases[i].Name != want {
			t.Fatalf("case %d name=%q want %q", i, res.Cases[i].Name, want)
		}
		reads := res.Cases[i].Result.ReadBenchmarks
		if len(reads) != 3 || reads[0].Concurrency != 1 || reads[1].Concurrency != 2 || reads[2].Concurrency != 4 {
			t.Fatalf("case %s read benchmarks=%+v", want, reads)
		}
	}
	if res.Cases[0].Result.FormatConfig == nil || res.Cases[0].Result.FormatConfig.IndexOuterLeavesInValueLog {
		t.Fatalf("index_db_outer_leaves format=%+v, want outer leaves in index.db", res.Cases[0].Result.FormatConfig)
	}
	if res.Cases[1].Result.FormatConfig == nil || !res.Cases[1].Result.FormatConfig.IndexOuterLeavesInValueLog {
		t.Fatalf("leaf_vlog_before_compact format=%+v, want outer leaves in leaf_vlog", res.Cases[1].Result.FormatConfig)
	}
	if res.Cases[2].Result.CompactStorage == nil || !res.Cases[2].Result.CompactStorage.FullyCompacted {
		t.Fatalf("leaf_vlog_after_compact compact stats=%+v", res.Cases[2].Result.CompactStorage)
	}
}

func TestRunTextOutput(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run([]string{
		"-matrix=false",
		"-dir", t.TempDir(),
		"-keep-dir",
		"-docs", "64",
		"-dims", "8",
		"-queries", "4",
		"-validate-queries", "2",
		"-validate-docs", "2",
		"-top-k", "3",
		"-m", "4",
		"-ef-construction", "32",
		"-ef-search", "32",
		"-min-recall", "0.5",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v stderr=%s", err, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{
		"TreeDB vector search demo",
		"profile=bench",
		"value_pointer_threshold=1024",
		"leaf_generation_segment_target=4194304",
		"compact_storage_full:",
		"Storage",
		"format index_outer_leaves_in_vlog=",
		"storage_domains index_db=",
		"Memory",
		"avg=",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("text output missing %q:\n%s", want, out)
		}
	}
}
