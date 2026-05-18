package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
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
		searchConcurrency:     []int{2},
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
	if len(res.SearchBenchmarks) != 2 || res.SearchBenchmarks[0].Concurrency != 1 || res.SearchBenchmarks[1].Concurrency != 2 {
		t.Fatalf("unexpected search benchmarks: %+v", res.SearchBenchmarks)
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

func TestExecuteConsumesDatasetDir(t *testing.T) {
	datasetDir := writeDemoDataset(t, 64, 8, 4, 3)
	res, err := execute(context.Background(), config{
		dir:                   t.TempDir(),
		datasetDir:            datasetDir,
		keepDir:               true,
		docs:                  64,
		dimensions:            8,
		queries:               4,
		searchConcurrency:     []int{2},
		validateQueries:       2,
		validateDocs:          2,
		topK:                  3,
		batchSize:             16,
		m:                     4,
		efConstruction:        32,
		efSearch:              32,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.5,
		disableExactFallback:  true,
	})
	if err != nil {
		t.Fatalf("execute with dataset: %v", err)
	}
	if res.DatasetDir != datasetDir {
		t.Fatalf("dataset_dir=%q want %q", res.DatasetDir, datasetDir)
	}
	if res.Validation.DocumentsChecked != 2 || res.Validation.QueriesChecked != 2 {
		t.Fatalf("validation counts=%+v, want 2 docs and 2 queries", res.Validation)
	}
	if res.Search.Queries != 4 {
		t.Fatalf("search queries=%d want 4", res.Search.Queries)
	}
}

func TestExecuteDatasetDirClampsValidateQueries(t *testing.T) {
	datasetDir := writeDemoDataset(t, 64, 8, 2, 3)
	res, err := execute(context.Background(), config{
		dir:                   t.TempDir(),
		datasetDir:            datasetDir,
		keepDir:               true,
		docs:                  64,
		dimensions:            8,
		queries:               2,
		searchConcurrency:     []int{2},
		validateQueries:       64,
		validateDocs:          1,
		topK:                  3,
		batchSize:             16,
		m:                     4,
		efConstruction:        32,
		efSearch:              32,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.5,
		disableExactFallback:  true,
	})
	if err != nil {
		t.Fatalf("execute with dataset: %v", err)
	}
	if res.ValidateQueries != 2 || res.Validation.QueriesChecked != 2 {
		t.Fatalf("validate queries result=%d validation=%+v, want clamped to 2", res.ValidateQueries, res.Validation)
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
	if res.MinRecall != 0.5 || res.Compact || !res.DisableExactFallback {
		t.Fatalf("missing reproducibility config in JSON result: %+v", res)
	}
}

func TestExecuteRequireLeafVLogBytesPassesWithDefaultBenchProfile(t *testing.T) {
	res, err := execute(context.Background(), config{
		dir:                   t.TempDir(),
		keepDir:               true,
		docs:                  64,
		dimensions:            8,
		queries:               2,
		searchConcurrency:     []int{2},
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
		searchConcurrency:     []int{2},
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
		"-search-concurrency", "2,4",
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
		searches := res.Cases[i].Result.SearchBenchmarks
		if len(searches) != 3 || searches[0].Concurrency != 1 || searches[1].Concurrency != 2 || searches[2].Concurrency != 4 {
			t.Fatalf("case %s search benchmarks=%+v", want, searches)
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

func TestExecuteMatrixReportsNestedKeptDirFromRoot(t *testing.T) {
	res, err := executeMatrix(context.Background(), config{
		docs:                  24,
		dimensions:            8,
		queries:               1,
		searchConcurrency:     []int{2},
		validateQueries:       1,
		validateDocs:          1,
		topK:                  3,
		batchSize:             12,
		m:                     4,
		efConstruction:        32,
		efSearch:              32,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.5,
		compact:               true,
		disableExactFallback:  true,
	})
	if err != nil {
		t.Fatalf("executeMatrix: %v", err)
	}
	if res.KeptDir {
		t.Fatalf("matrix kept_dir=true, want false")
	}
	for _, testCase := range res.Cases {
		if testCase.Result.KeptDir {
			t.Fatalf("case %s kept_dir=true, want false inherited from matrix root", testCase.Name)
		}
	}
	if _, err := os.Stat(res.Dir); !os.IsNotExist(err) {
		t.Fatalf("matrix dir stat err=%v, want removed temp dir", err)
	}
}

func TestExecuteRequireValueLogBytesFailsOnPagerBackedDefault(t *testing.T) {
	_, err := execute(context.Background(), config{
		dir:                  t.TempDir(),
		keepDir:              true,
		docs:                 64,
		dimensions:           8,
		queries:              2,
		validateQueries:      1,
		validateDocs:         1,
		topK:                 3,
		batchSize:            32,
		m:                    4,
		efConstruction:       32,
		efSearch:             32,
		minRecall:            0.5,
		compact:              true,
		disableExactFallback: true,
		requireValueLogBytes: true,
	})
	if err == nil {
		t.Fatal("execute succeeded, want value-log requirement failure")
	}
	// These assertions intentionally describe the current default backend
	// profile, where this demo does not force value-log storage.
	if !strings.Contains(err.Error(), "zero value_vlog bytes") {
		t.Fatalf("error=%v, want zero value_vlog bytes", err)
	}
}

func TestExecuteRejectsNonEmptyDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "unrelated"), []byte("keep"), 0o644); err != nil {
		t.Fatalf("write unrelated file: %v", err)
	}
	_, err := execute(context.Background(), config{
		dir:                  dir,
		keepDir:              true,
		docs:                 64,
		dimensions:           8,
		queries:              2,
		validateQueries:      1,
		validateDocs:         1,
		topK:                 3,
		batchSize:            32,
		m:                    4,
		efConstruction:       32,
		efSearch:             32,
		minRecall:            0.5,
		compact:              true,
		disableExactFallback: true,
	})
	if err == nil {
		t.Fatal("execute accepted non-empty -dir, want error")
	}
	if !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("error=%v, want not-empty directory error", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "unrelated")); err != nil {
		t.Fatalf("unrelated file err=%v, want untouched", err)
	}
}

func TestExecuteMatrixRejectsNonEmptyDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "unrelated"), []byte("keep"), 0o644); err != nil {
		t.Fatalf("write unrelated file: %v", err)
	}
	_, err := executeMatrix(context.Background(), config{
		dir:                  dir,
		docs:                 24,
		dimensions:           8,
		queries:              1,
		searchConcurrency:    []int{2},
		validateQueries:      1,
		validateDocs:         1,
		topK:                 3,
		batchSize:            12,
		m:                    4,
		efConstruction:       32,
		efSearch:             32,
		minRecall:            0.5,
		compact:              true,
		disableExactFallback: true,
	})
	if err == nil {
		t.Fatal("executeMatrix accepted non-empty -dir, want error")
	}
	if !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("error=%v, want not-empty directory error", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "unrelated")); err != nil {
		t.Fatalf("unrelated file err=%v, want untouched", err)
	}
}

func TestExecuteRemovesTemporaryDirWhenNotKept(t *testing.T) {
	res, err := execute(context.Background(), config{
		docs:                 64,
		dimensions:           8,
		queries:              4,
		validateQueries:      2,
		validateDocs:         2,
		topK:                 3,
		batchSize:            32,
		m:                    4,
		efConstruction:       32,
		efSearch:             32,
		minRecall:            0.5,
		compact:              false,
		disableExactFallback: true,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.KeptDir {
		t.Fatalf("KeptDir=%t want false", res.KeptDir)
	}
	if _, err := os.Stat(res.Dir); !os.IsNotExist(err) {
		t.Fatalf("temporary dir stat err=%v, want not exist", err)
	}
}

func TestParseConfigRejectsInvalidValidationCombinations(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "recall gate without validation queries",
			args: []string{"-validate-queries", "0"},
			want: "-min-recall must be 0",
		},
		{
			name: "topk exceeds docs",
			args: []string{"-docs", "2", "-top-k", "3"},
			want: "-top-k cannot exceed -docs",
		},
		{
			name: "synthetic validate queries exceeds docs",
			args: []string{"-docs", "2", "-top-k", "2", "-validate-queries", "3"},
			want: "-validate-queries cannot exceed -docs",
		},
		{
			name: "negative validate docs",
			args: []string{"-validate-docs", "-1"},
			want: "-validate-docs cannot be negative",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseConfig(tc.args)
			if err == nil {
				t.Fatal("parseConfig succeeded, want error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestParseConfigDatasetDirAllowsValidateQueriesAboveDocs(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-dataset-dir", filepath.Join("tmp", "dataset"),
		"-docs", "2",
		"-top-k", "2",
		"-validate-queries", "7",
		"-validate-docs", "2",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.validateQueries != 7 {
		t.Fatalf("validateQueries=%d want 7", cfg.validateQueries)
	}
	if cfg.datasetDir != filepath.Join("tmp", "dataset") {
		t.Fatalf("datasetDir=%q", cfg.datasetDir)
	}
}

func TestParseConfigDoesNotWriteFlagErrorsToProcessStderr(t *testing.T) {
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	originalStderr := os.Stderr
	os.Stderr = writePipe
	t.Cleanup(func() {
		os.Stderr = originalStderr
		_ = readPipe.Close()
	})
	_, err = parseConfig([]string{"-not-a-real-flag"})
	_ = writePipe.Close()
	if err == nil {
		t.Fatal("parseConfig accepted unknown flag")
	}
	var stderr bytes.Buffer
	if _, err := stderr.ReadFrom(readPipe); err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("parseConfig wrote to stderr: %q", stderr.String())
	}
}

func TestSyntheticQueriesDoNotOverlapValidationQueries(t *testing.T) {
	docs := 97
	validateCount := 17
	benchmarkCount := 53
	stride := queryDocStride(docs)
	seen := make(map[int]struct{}, validateCount)
	for i := 0; i < validateCount; i++ {
		seen[syntheticQueryID(i, docs, 0, docs, stride)] = struct{}{}
	}
	benchmarkStart := validateCount
	benchmarkSpan := docs - benchmarkStart
	for i := 0; i < benchmarkCount; i++ {
		queryID := syntheticQueryID(i, docs, benchmarkStart, benchmarkSpan, stride)
		if _, ok := seen[queryID]; ok {
			t.Fatalf("benchmark query id %d overlapped validation set", queryID)
		}
	}
}

func TestSyntheticQueryIDSpillsPastCorpusAfterFullValidation(t *testing.T) {
	docs := 17
	stride := queryDocStride(docs)
	for i := 0; i < 5; i++ {
		if got := syntheticQueryID(i, docs, docs, 0, stride); got != docs+i {
			t.Fatalf("syntheticQueryID(%d)=%d want %d", i, got, docs+i)
		}
	}
}

func TestValidationDocIndexSamplesDistinctDocsWhenStrideWouldCollapse(t *testing.T) {
	docs := 1543
	seen := make(map[int]struct{}, 16)
	for i := 0; i < 16; i++ {
		docIndex := validationDocIndex(i, docs)
		if _, ok := seen[docIndex]; ok {
			t.Fatalf("validation doc index repeated %d for docs=%d", docIndex, docs)
		}
		seen[docIndex] = struct{}{}
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
		"Storage",
		"format index_outer_leaves_in_vlog=",
		"storage_domains index_db=",
		"Memory",
		"avg=",
		"Parallel Search Benchmark",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("text output missing %q:\n%s", want, out)
		}
	}
}

func writeDemoDataset(t *testing.T, docs, dims, queries, topK int) string {
	t.Helper()
	dir := t.TempDir()
	m := map[string]any{
		"version":               1,
		"docs":                  docs,
		"dimensions":            dims,
		"queries":               queries,
		"top_k":                 topK,
		"metric":                "cosine",
		"document_vectors_file": "documents.f32",
		"query_vectors_file":    "queries.f32",
		"documents_jsonl_file":  "documents.jsonl",
		"float_format":          "float32_le_row_major",
	}
	manifest, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), append(manifest, '\n'), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	writeTestVectorFile(t, filepath.Join(dir, "documents.f32"), docs, dims, func(i int) []float32 {
		return embedding(i, dims)
	})
	stride := queryDocStride(docs)
	writeTestVectorFile(t, filepath.Join(dir, "queries.f32"), queries, dims, func(i int) []float32 {
		return embedding(queryDocIndex(i, docs, stride), dims)
	})
	f, err := os.Create(filepath.Join(dir, "documents.jsonl"))
	if err != nil {
		t.Fatalf("create documents jsonl: %v", err)
	}
	enc := json.NewEncoder(f)
	for i := 0; i < docs; i++ {
		if err := enc.Encode(struct {
			Index     int       `json:"index"`
			ID        string    `json:"id"`
			Group     int       `json:"group"`
			Source    string    `json:"source"`
			Embedding []float32 `json:"embedding"`
		}{
			Index:     i,
			ID:        fmt.Sprintf("dataset-doc-%06d", i),
			Group:     i % 16,
			Source:    "exported-test-dataset",
			Embedding: embedding(i, dims),
		}); err != nil {
			_ = f.Close()
			t.Fatalf("write document %d: %v", i, err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close documents jsonl: %v", err)
	}
	return dir
}

func writeTestVectorFile(t *testing.T, path string, count, dims int, vector func(int) []float32) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create vector file: %v", err)
	}
	for i := 0; i < count; i++ {
		v := vector(i)
		if len(v) != dims {
			_ = f.Close()
			t.Fatalf("vector %d dims=%d want %d", i, len(v), dims)
		}
		for _, value := range v {
			if err := binary.Write(f, binary.LittleEndian, value); err != nil {
				_ = f.Close()
				t.Fatalf("write vector %d: %v", i, err)
			}
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close vector file: %v", err)
	}
}
