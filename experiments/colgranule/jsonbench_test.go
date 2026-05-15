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

func TestRunJSONBenchPartQueriesSampleMatchesRawReference(t *testing.T) {
	ds, err := LoadJSONBenchColumns("testdata/jsonbench_sample.jsonl", 0)
	if err != nil {
		t.Fatalf("LoadJSONBenchColumns(sample): %v", err)
	}
	rawTimings, err := RunJSONBenchQueries(ds, 1)
	if err != nil {
		t.Fatalf("RunJSONBenchQueries: %v", err)
	}
	partTimings, err := RunJSONBenchPartQueries(ds, 2, 2)
	if err != nil {
		t.Fatalf("RunJSONBenchPartQueries: %v", err)
	}
	if len(partTimings) != len(rawTimings) {
		t.Fatalf("part timings=%d raw timings=%d", len(partTimings), len(rawTimings))
	}
	rawByQuery := make(map[string]JSONBenchQueryTiming, len(rawTimings))
	for _, timing := range rawTimings {
		rawByQuery[timing.Query] = timing
	}
	for _, timing := range partTimings {
		raw, ok := rawByQuery[timing.Query]
		if !ok {
			t.Fatalf("unexpected part query %s", timing.Query)
		}
		if timing.ResultRows != raw.ResultRows || timing.ResultDigest != raw.ResultDigest {
			t.Fatalf("%s part rows/digest=(%d,%d) raw=(%d,%d)", timing.Query, timing.ResultRows, timing.ResultDigest, raw.ResultRows, raw.ResultDigest)
		}
		if timing.Engine != "encoded_column_part" {
			t.Fatalf("%s engine=%q want encoded_column_part", timing.Query, timing.Engine)
		}
		if len(timing.Attempts) != 2 || timing.Attempts[0].Cache != "cold" || timing.Attempts[1].Cache != "warm" {
			t.Fatalf("%s attempts=%+v want cold,warm labels", timing.Query, timing.Attempts)
		}
		for _, attempt := range timing.Attempts {
			if attempt.ResultRows != raw.ResultRows || attempt.ResultDigest != raw.ResultDigest {
				t.Fatalf("%s/%s rows/digest=(%d,%d) raw=(%d,%d)", timing.Query, attempt.Cache, attempt.ResultRows, attempt.ResultDigest, raw.ResultRows, raw.ResultDigest)
			}
			assertJSONBenchPartDiagnostics(t, timing.Query, attempt.Diagnostics, ds.Rows, 3)
		}
		assertJSONBenchPartDiagnostics(t, timing.Query, timing.Diagnostics, ds.Rows, 3)
		switch timing.Query {
		case "Q2":
			if timing.Diagnostics.AggregateKernel != "fused_dense_group_count_distinct_codes" {
				t.Fatalf("Q2 kernel=%q want fused dense distinct", timing.Diagnostics.AggregateKernel)
			}
		case "Q3":
			if timing.Diagnostics.AggregateKernel != "fused_dense_group_count_hour_codes" {
				t.Fatalf("Q3 kernel=%q want fused dense hour count", timing.Diagnostics.AggregateKernel)
			}
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

func TestRunJSONBenchPartQueriesLocalIfPresent(t *testing.T) {
	path := os.Getenv("JSONBENCH_DATA")
	if path == "" {
		t.Skip("JSONBENCH_DATA not set")
	}
	if _, err := os.Stat(path); err != nil {
		t.Skipf("local JSONBench fixture not present at %s", path)
	}
	ds, err := LoadJSONBenchColumns(path, 1000)
	if err != nil {
		t.Fatalf("LoadJSONBenchColumns(local): %v", err)
	}
	rawTimings, err := RunJSONBenchQueries(ds, 1)
	if err != nil {
		t.Fatalf("RunJSONBenchQueries(local): %v", err)
	}
	partTimings, err := RunJSONBenchPartQueries(ds, DefaultRowsPerGranule, 2)
	if err != nil {
		t.Fatalf("RunJSONBenchPartQueries(local): %v", err)
	}
	rawByQuery := make(map[string]JSONBenchQueryTiming, len(rawTimings))
	for _, timing := range rawTimings {
		rawByQuery[timing.Query] = timing
	}
	for _, timing := range partTimings {
		raw := rawByQuery[timing.Query]
		if timing.ResultRows != raw.ResultRows || timing.ResultDigest != raw.ResultDigest {
			t.Fatalf("%s local part rows/digest=(%d,%d) raw=(%d,%d)", timing.Query, timing.ResultRows, timing.ResultDigest, raw.ResultRows, raw.ResultDigest)
		}
	}
}

func assertJSONBenchPartDiagnostics(t *testing.T, query string, diagnostics JSONBenchPartQueryDiagnostics, rows int, granules int) {
	t.Helper()
	if diagnostics.RowsScanned != rows {
		t.Fatalf("%s diagnostics rows=%d want %d: %+v", query, diagnostics.RowsScanned, rows, diagnostics)
	}
	if diagnostics.GranulesConsidered != granules {
		t.Fatalf("%s diagnostics granules=%d want %d: %+v", query, diagnostics.GranulesConsidered, granules, diagnostics)
	}
	if diagnostics.GranulesSkipped != 0 {
		t.Fatalf("%s diagnostics skipped=%d want 0: %+v", query, diagnostics.GranulesSkipped, diagnostics)
	}
	if diagnostics.GranulesDecoded <= 0 || diagnostics.GranulesDecoded > granules {
		t.Fatalf("%s diagnostics decoded granules=%d want 1..%d: %+v", query, diagnostics.GranulesDecoded, granules, diagnostics)
	}
	if diagnostics.BlocksDecoded <= 0 || diagnostics.BytesDecoded <= 0 {
		t.Fatalf("%s diagnostics missing decoded blocks/bytes: %+v", query, diagnostics)
	}
	if diagnostics.AggregateKernel == "" {
		t.Fatalf("%s diagnostics missing aggregate kernel: %+v", query, diagnostics)
	}
	if diagnostics.CacheState != "cold" && diagnostics.CacheState != "warm" {
		t.Fatalf("%s diagnostics cache=%q want cold or warm", query, diagnostics.CacheState)
	}
	if len(diagnostics.ColumnsProjected) == 0 {
		t.Fatalf("%s diagnostics missing columns projected: %+v", query, diagnostics)
	}
}
