package colgranule

import (
	"os"
	"testing"
)

func TestLoadJSONBenchColumnsSample(t *testing.T) {
	ds, err := LoadJSONBenchColumns("testdata/jsonbench_sample.jsonl", 0)
	if err != nil {
		t.Fatalf("LoadJSONBenchColumns(sample): %v", err)
	}
	if ds.Rows != 5 {
		t.Fatalf("rows=%d want 5", ds.Rows)
	}
	for _, name := range []string{"time_us", "line_bytes", "did_code", "commit_collection_code", "record_created_at_unix_ms", "record_text_bytes"} {
		if got := len(ds.Columns[name]); got != ds.Rows {
			t.Fatalf("column %s len=%d want %d", name, got, ds.Rows)
		}
	}
	if got := ds.Columns["record_has_reply"][0]; got != 1 {
		t.Fatalf("record_has_reply[0]=%d want 1", got)
	}
	if got := ds.Columns["record_has_subject"][1]; got != 1 {
		t.Fatalf("record_has_subject[1]=%d want 1", got)
	}
	if got := ds.Columns["record_subject_string_bytes"][2]; got == 0 {
		t.Fatalf("record_subject_string_bytes[2]=0 want nonzero")
	}
	if got := ds.Dictionaries["kind_code"]["commit"]; got == 0 {
		t.Fatalf("missing kind_code dictionary entry for commit")
	}
}

func TestSummarizeJSONBenchDatasetSample(t *testing.T) {
	ds, err := LoadJSONBenchColumns("testdata/jsonbench_sample.jsonl", 0)
	if err != nil {
		t.Fatalf("LoadJSONBenchColumns(sample): %v", err)
	}
	summaries, err := SummarizeJSONBenchDataset(ds, 2, DefaultJSONBenchConfigs())
	if err != nil {
		t.Fatalf("SummarizeJSONBenchDataset: %v", err)
	}
	want := len(ds.Columns) * len(DefaultJSONBenchConfigs())
	if len(summaries) != want {
		t.Fatalf("summaries=%d want %d", len(summaries), want)
	}
	if summaries[0].Rows != ds.Rows {
		t.Fatalf("summary rows=%d want %d", summaries[0].Rows, ds.Rows)
	}
}

func TestRunJSONBenchQueriesSample(t *testing.T) {
	ds, err := LoadJSONBenchColumns("testdata/jsonbench_sample.jsonl", 0)
	if err != nil {
		t.Fatalf("LoadJSONBenchColumns(sample): %v", err)
	}
	timings, err := RunJSONBenchQueries(ds, 1)
	if err != nil {
		t.Fatalf("RunJSONBenchQueries: %v", err)
	}
	if len(timings) != 5 {
		t.Fatalf("timings=%d want 5", len(timings))
	}
	for _, timing := range timings {
		if timing.Best <= 0 {
			t.Fatalf("%s best=%s want positive", timing.Query, timing.Best)
		}
	}
}

func TestLoadJSONBenchColumnsLocal1MIfPresent(t *testing.T) {
	path := os.Getenv("JSONBENCH_DATA")
	if path == "" {
		t.Skip("JSONBENCH_DATA not set")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Skipf("local JSONBench fixture not present at %s", path)
	}
	if info.IsDir() {
		t.Skipf("JSONBENCH_DATA points to directory %s; file-only local fixture test skipped", path)
	}
	ds, err := LoadJSONBenchColumns(path, 1000)
	if err != nil {
		t.Fatalf("LoadJSONBenchColumns(local): %v", err)
	}
	if ds.Rows != 1000 {
		t.Fatalf("rows=%d want 1000", ds.Rows)
	}
	if got := len(ds.Columns["time_us"]); got != 1000 {
		t.Fatalf("time_us len=%d want 1000", got)
	}
}

func TestLoadJSONBenchColumnsLocalDirIfPresent(t *testing.T) {
	path := os.Getenv("JSONBENCH_DATA")
	if path == "" {
		t.Skip("JSONBENCH_DATA not set")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Skipf("local JSONBench fixture directory not present at %s", path)
	}
	if !info.IsDir() {
		t.Skipf("JSONBENCH_DATA points to file %s; directory-only local fixture test skipped", path)
	}
	ds, err := LoadJSONBenchColumns(path, 1000)
	if err != nil {
		t.Fatalf("LoadJSONBenchColumns(local dir): %v", err)
	}
	if ds.Rows != 1000 {
		t.Fatalf("rows=%d want 1000", ds.Rows)
	}
	if len(ds.Files) == 0 {
		t.Fatalf("files=0 want nonzero")
	}
}
