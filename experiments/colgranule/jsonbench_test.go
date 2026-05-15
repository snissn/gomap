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
	for _, name := range []string{"time_us", "hour_of_day", "line_bytes", "did_code", "commit_collection_code", "record_created_at_unix_ms", "record_text_bytes"} {
		if got := len(ds.Columns[name]); got != ds.Rows {
			t.Fatalf("column %s len=%d want %d", name, got, ds.Rows)
		}
	}
	if got, want := ds.Columns["hour_of_day"][0], unixMicroHour(ds.Columns["time_us"][0]); got != want {
		t.Fatalf("hour_of_day[0]=%d want %d", got, want)
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
	part, err := BuildJSONBenchColumnPart(ds, 2)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPart: %v", err)
	}
	hourColumn := part.Columns["hour_of_day"]
	if hourColumn.Definition.Type != ColumnTypeLowCardinalityCode || hourColumn.Definition.Cardinality != jsonBenchHoursPerDay {
		t.Fatalf("hour_of_day definition=%+v want low-cardinality/%d", hourColumn.Definition, jsonBenchHoursPerDay)
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
			assertContainsString(t, timing.Diagnostics.ColumnsProjected, "hour_of_day")
			assertNotContainsString(t, timing.Diagnostics.ColumnsProjected, "time_us")
		case "Q4":
			if timing.Diagnostics.AggregateKernel != "sort_key_early_stop_min_by_user" {
				t.Fatalf("Q4 kernel=%q want sort-key early stop", timing.Diagnostics.AggregateKernel)
			}
		case "Q5":
			if timing.Diagnostics.AggregateKernel != "fused_dense_span_by_user" {
				t.Fatalf("Q5 kernel=%q want fused dense span", timing.Diagnostics.AggregateKernel)
			}
		}
	}
}

func TestRunJSONBenchPartQ4EarlyStopUsesTimePrefix(t *testing.T) {
	ds := syntheticJSONBenchDataset(128)
	part, err := BuildJSONBenchColumnPart(ds, 128)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPart: %v", err)
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	rows, digest, diagnostics, err := runJSONBenchPartQ4(part, codes, &jsonBenchPartQueryScratch{})
	if err != nil {
		t.Fatalf("runJSONBenchPartQ4: %v", err)
	}
	if rows != 3 || digest == 0 {
		t.Fatalf("q4 rows/digest=(%d,%d), want non-empty top 3", rows, digest)
	}
	if diagnostics.RowsScanned >= ds.Rows {
		t.Fatalf("q4 rows scanned=%d want early stop before %d", diagnostics.RowsScanned, ds.Rows)
	}
	fullDiagnostics := partColumnDiagnostics(part, jsonBenchPartQ4Columns, "full_decode_reference")
	if diagnostics.BytesDecoded >= fullDiagnostics.BytesDecoded {
		t.Fatalf("q4 decoded bytes=%d want less than full=%d", diagnostics.BytesDecoded, fullDiagnostics.BytesDecoded)
	}
}

func TestTouchedBitsetResetClearsOnlyTouchedWords(t *testing.T) {
	var bitset touchedBitset
	words := bitset.reset(4)
	if bitsetTestAndSetTouched(words, &bitset.touched, 3) {
		t.Fatal("first code was already set")
	}
	if bitsetTestAndSetTouched(words, &bitset.touched, 3) == false {
		t.Fatal("second code was not reported as already set")
	}
	bitsetTestAndSetTouched(words, &bitset.touched, 135)
	if len(bitset.touched) != 2 {
		t.Fatalf("touched words=%v want 2 unique words", bitset.touched)
	}
	words = bitset.reset(4)
	for i, word := range words {
		if word != 0 {
			t.Fatalf("word %d=%d after reset, want 0", i, word)
		}
	}
	if len(bitset.touched) != 0 {
		t.Fatalf("touched after reset=%v want empty", bitset.touched)
	}
	if bitsetTestAndSetTouched(words, &bitset.touched, 3) {
		t.Fatal("code remained set after reset")
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
	if got := len(ds.Columns["hour_of_day"]); got != 1000 {
		t.Fatalf("hour_of_day len=%d want 1000", got)
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

func assertContainsString(t *testing.T, values []string, want string) {
	t.Helper()
	for _, value := range values {
		if value == want {
			return
		}
	}
	t.Fatalf("%q not found in %v", want, values)
}

func assertNotContainsString(t *testing.T, values []string, unwanted string) {
	t.Helper()
	for _, value := range values {
		if value == unwanted {
			t.Fatalf("%q unexpectedly found in %v", unwanted, values)
		}
	}
}
