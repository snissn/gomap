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
		dir:                  t.TempDir(),
		keepDir:              true,
		docs:                 128,
		dimensions:           16,
		queries:              8,
		validateQueries:      4,
		validateDocs:         4,
		topK:                 5,
		batchSize:            32,
		m:                    8,
		efConstruction:       64,
		efSearch:             64,
		minRecall:            0.5,
		compact:              true,
		disableExactFallback: true,
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
	if res.StorageAfterCompact.TotalBytes <= 0 || res.StorageAfterCompact.BytesPerDoc <= 0 {
		t.Fatalf("missing compacted storage report: %+v", res.StorageAfterCompact)
	}
	if res.FormatConfig == nil {
		t.Fatal("missing format config report")
	}
	if res.StorageExpectation.IndexBytes <= 0 {
		t.Fatalf("missing storage expectation index bytes: %+v", res.StorageExpectation)
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
	if res.Docs != 64 || res.Search.Queries != 4 || res.StorageAfterCompact.TotalBytes <= 0 {
		t.Fatalf("unexpected JSON result: %+v", res)
	}
	if res.MinRecall != 0.5 || !res.Compact || !res.DisableExactFallback {
		t.Fatalf("missing reproducibility config in JSON result: %+v", res)
	}
}

func TestExecuteRequireLeafVLogBytesFailsOnPagerBackedDefault(t *testing.T) {
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
		requireLeafVLogBytes: true,
	})
	if err == nil {
		t.Fatal("execute succeeded, want leaf-vlog requirement failure")
	}
	// These assertions intentionally describe the current default backend
	// profile, where this demo does not force leaf value-log storage.
	if !strings.Contains(err.Error(), "zero leaf_vlog bytes") {
		t.Fatalf("error=%v, want zero leaf_vlog bytes", err)
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
			name: "overlapping validation and benchmark queries",
			args: []string{"-docs", "10", "-queries", "8", "-validate-queries", "3", "-validate-docs", "0"},
			want: "-validate-queries plus -queries cannot exceed -docs",
		},
		{
			name: "topk exceeds docs",
			args: []string{"-docs", "2", "-top-k", "3"},
			want: "-top-k cannot exceed -docs",
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
		seen[syntheticQueryID(i, docs, 0, stride)] = struct{}{}
	}
	for i := 0; i < benchmarkCount; i++ {
		docIndex := syntheticQueryID(i, docs, validateCount, stride)
		if _, ok := seen[docIndex]; ok {
			t.Fatalf("benchmark query doc %d overlapped validation set", docIndex)
		}
	}
}

func TestSyntheticQueryIDSpillsPastCorpusAfterFullValidation(t *testing.T) {
	docs := 17
	stride := queryDocStride(docs)
	for i := 0; i < 5; i++ {
		if got := syntheticQueryID(i, docs, docs, stride); got != docs+i {
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
