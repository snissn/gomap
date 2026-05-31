package colgranule

import (
	"os"
	"strings"
	"testing"
	"time"
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
	row := summaries[0].CompressionRow
	if row.CodecLayoutLabel == "" || row.CompressionPolicyLabel == "" || row.RequestedCompression == "" || row.ActualCompression == "" {
		t.Fatalf("summary compression row missing labels: %+v", row)
	}
	if row.CompressedBytes == 0 || row.RawBytes == 0 || row.DecompressedBytes == 0 || row.CompressionRatio <= 0 {
		t.Fatalf("summary compression row missing byte attribution: %+v", row)
	}
	if row.CompressionDurationSource == "" || row.DecompressionDurationSource == "" {
		t.Fatalf("summary compression row missing duration sources: %+v", row)
	}
	if row.BenchmarkAllocationSource == "" {
		t.Fatalf("summary compression row missing B/op allocs source: %+v", row)
	}
	formatted := FormatColumnCodecSummary(summaries[0])
	for _, want := range []string{"codec_layout=", "compression_policy=", "compressed_bytes=", "decompressed_bytes=", "raw_bytes=", "ratio=", "compression_duration_source=", "decompression_duration_source=", "B/op=", "allocs/op="} {
		if !strings.Contains(formatted, want) {
			t.Fatalf("formatted summary missing %q: %s", want, formatted)
		}
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
	const rowsPerGranule = 2
	partTimings, err := RunJSONBenchPartQueries(ds, rowsPerGranule, 2)
	if err != nil {
		t.Fatalf("RunJSONBenchPartQueries: %v", err)
	}
	part, err := BuildJSONBenchColumnPart(ds, rowsPerGranule)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPart: %v", err)
	}
	hourColumn := part.Columns["hour_of_day"]
	if hourColumn.Definition.Type != ColumnTypeLowCardinalityCode || hourColumn.Definition.Cardinality != jsonBenchHoursPerDay {
		t.Fatalf("hour_of_day definition=%+v want low-cardinality/%d", hourColumn.Definition, jsonBenchHoursPerDay)
	}
	granules := (ds.Rows + rowsPerGranule - 1) / rowsPerGranule
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
			assertJSONBenchPartDiagnostics(t, timing.Query, attempt.Diagnostics, ds.Rows, granules)
		}
		assertJSONBenchPartDiagnostics(t, timing.Query, timing.Diagnostics, ds.Rows, granules)
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
			if !timing.Diagnostics.EarlyStopAvailable {
				t.Fatalf("Q4 diagnostics missing early-stop label: %+v", timing.Diagnostics)
			}
		case "Q5":
			if timing.Diagnostics.AggregateKernel != "fused_dense_span_by_user" {
				t.Fatalf("Q5 kernel=%q want fused dense span", timing.Diagnostics.AggregateKernel)
			}
		}
	}
}

func TestRunJSONBenchPartQ4FairnessQueries(t *testing.T) {
	ds := syntheticJSONBenchDataset(256)
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	rawRows, rawDigest := runJSONBenchQ4(ds, codes)
	timings, err := RunJSONBenchPartQ4FairnessQueries(ds, 32, 2)
	if err != nil {
		t.Fatalf("RunJSONBenchPartQ4FairnessQueries: %v", err)
	}
	if len(timings) != 2 {
		t.Fatalf("fairness timings=%d want 2", len(timings))
	}
	seen := map[string]bool{}
	for _, timing := range timings {
		seen[timing.Query] = true
		if timing.ResultRows != rawRows || timing.ResultDigest != rawDigest {
			t.Fatalf("%s rows/digest=(%d,%d) raw=(%d,%d)", timing.Query, timing.ResultRows, timing.ResultDigest, rawRows, rawDigest)
		}
		if len(timing.Attempts) != 2 || timing.Attempts[0].Cache != "cold" || timing.Attempts[1].Cache != "warm" {
			t.Fatalf("%s attempts=%+v want cold,warm labels", timing.Query, timing.Attempts)
		}
		switch timing.Query {
		case "Q4a":
			if timing.Diagnostics.AggregateKernel != "sort_key_early_stop_min_by_user" {
				t.Fatalf("Q4a kernel=%q want early-stop kernel", timing.Diagnostics.AggregateKernel)
			}
			if !timing.Diagnostics.EarlyStopAvailable {
				t.Fatalf("Q4a early_stop_available=false: %+v", timing.Diagnostics)
			}
			assertStringSliceEqual(t, timing.Diagnostics.SortKey, []string{"time_us"})
		case "Q4b":
			if timing.Diagnostics.AggregateKernel != "clickhouse_order_prefix_scan_min_by_user" {
				t.Fatalf("Q4b kernel=%q want ClickHouse-order prefix scan", timing.Diagnostics.AggregateKernel)
			}
			if timing.Diagnostics.EarlyStopAvailable {
				t.Fatalf("Q4b unexpectedly reported early-stop available: %+v", timing.Diagnostics)
			}
			if timing.Diagnostics.RowsScanned <= 3 {
				t.Fatalf("Q4b rows scanned=%d want no top-3 global-time short-circuit", timing.Diagnostics.RowsScanned)
			}
			if timing.Diagnostics.GranulesSkipped == 0 {
				t.Fatalf("Q4b skipped no granules; want ClickHouse-order prefix pruning diagnostics: %+v", timing.Diagnostics)
			}
			assertStringSliceEqual(t, timing.Diagnostics.SortKey, []string{"kind_code", "commit_operation_code", "commit_collection_code", "did_code", "time_us"})
		default:
			t.Fatalf("unexpected fairness query %s", timing.Query)
		}
	}
	if !seen["Q4a"] || !seen["Q4b"] {
		t.Fatalf("fairness queries seen=%v want Q4a and Q4b", seen)
	}
}

func TestRunJSONBenchPartAggregateMetadataQueries(t *testing.T) {
	ds := syntheticJSONBenchDataset(256)
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	rawQ4Rows, rawQ4Digest := runJSONBenchQ4(ds, codes)
	rawQ5Rows, rawQ5Digest := runJSONBenchQ5(ds, codes)
	timings, err := RunJSONBenchPartAggregateMetadataQueries(ds, 32, 2)
	if err != nil {
		t.Fatalf("RunJSONBenchPartAggregateMetadataQueries: %v", err)
	}
	if len(timings) != 2 {
		t.Fatalf("aggregate metadata timings=%d want 2", len(timings))
	}
	seen := map[string]bool{}
	for _, timing := range timings {
		seen[timing.Query] = true
		if len(timing.Attempts) != 2 || timing.Attempts[0].Cache != "cold" || timing.Attempts[1].Cache != "warm" {
			t.Fatalf("%s attempts=%+v want cold,warm labels", timing.Query, timing.Attempts)
		}
		d := timing.Diagnostics
		if !d.AggregateMetadataUsed {
			t.Fatalf("%s did not report aggregate metadata: %+v", timing.Query, d)
		}
		if d.AggregateMetadataName != jsonBenchPostCreateDidTimeMetadata {
			t.Fatalf("%s metadata=%q want %q", timing.Query, d.AggregateMetadataName, jsonBenchPostCreateDidTimeMetadata)
		}
		if d.RowsScanned != 0 || d.BlocksDecoded != 0 || d.BytesDecoded != 0 {
			t.Fatalf("%s diagnostics scanned/decode rows=%d blocks=%d bytes=%d want metadata-only", timing.Query, d.RowsScanned, d.BlocksDecoded, d.BytesDecoded)
		}
		if d.AggregateMetadataRows == 0 || d.AggregateMetadataEntries == 0 || d.AggregateMetadataBytes == 0 || d.AggregateMetadataCompression == "" {
			t.Fatalf("%s missing metadata accounting: %+v", timing.Query, d)
		}
		switch timing.Query {
		case "Q4b-meta":
			if timing.ResultRows != rawQ4Rows || timing.ResultDigest != rawQ4Digest {
				t.Fatalf("Q4b-meta rows/digest=(%d,%d) raw=(%d,%d)", timing.ResultRows, timing.ResultDigest, rawQ4Rows, rawQ4Digest)
			}
			if d.AggregateKernel != "aggregate_metadata_min_by_user" {
				t.Fatalf("Q4b-meta kernel=%q want metadata min", d.AggregateKernel)
			}
			assertStringSliceEqual(t, d.SortKey, []string{"kind_code", "commit_operation_code", "commit_collection_code", "did_code", "time_us"})
		case "Q5-meta":
			if timing.ResultRows != rawQ5Rows || timing.ResultDigest != rawQ5Digest {
				t.Fatalf("Q5-meta rows/digest=(%d,%d) raw=(%d,%d)", timing.ResultRows, timing.ResultDigest, rawQ5Rows, rawQ5Digest)
			}
			if d.AggregateKernel != "aggregate_metadata_span_by_user" {
				t.Fatalf("Q5-meta kernel=%q want metadata span", d.AggregateKernel)
			}
			assertStringSliceEqual(t, d.SortKey, []string{"time_us"})
		default:
			t.Fatalf("unexpected aggregate metadata query %s", timing.Query)
		}
	}
	if !seen["Q4b-meta"] || !seen["Q5-meta"] {
		t.Fatalf("aggregate metadata queries seen=%v want Q4b-meta and Q5-meta", seen)
	}
}

func TestRunJSONBenchPartBuildReports(t *testing.T) {
	ds, err := LoadJSONBenchColumns("testdata/jsonbench_sample.jsonl", 0)
	if err != nil {
		t.Fatalf("LoadJSONBenchColumns(sample): %v", err)
	}
	reports, err := RunJSONBenchPartBuildReports(ds, 32, 2)
	if err != nil {
		t.Fatalf("RunJSONBenchPartBuildReports: %v", err)
	}
	if len(reports) != 2 {
		t.Fatalf("reports=%d want 2", len(reports))
	}
	for _, report := range reports {
		if report.Rows != ds.Rows || report.RawJSONBytes == 0 || report.DictionaryBytes == 0 {
			t.Fatalf("bad report input accounting: %+v", report)
		}
		if report.Best.Duration <= 0 || report.RowsPerSecond <= 0 || report.NanosPerRow <= 0 {
			t.Fatalf("bad build timing: %+v", report)
		}
		if report.Accounting.TotalStoredBytes == 0 || report.Accounting.TotalStoredBytes != report.Accounting.CategoryBytes() {
			t.Fatalf("bad byte accounting: %+v", report.Accounting)
		}
		if report.Accounting.DictionaryBytes != report.DictionaryBytes {
			t.Fatalf("dictionary bytes accounting=%d report=%d", report.Accounting.DictionaryBytes, report.DictionaryBytes)
		}
		if report.Best.DeclaredColumnStoredBytes != report.Accounting.DeclaredColumnStoredBytes {
			t.Fatalf("declared column bytes attempt=%d accounting=%d", report.Best.DeclaredColumnStoredBytes, report.Accounting.DeclaredColumnStoredBytes)
		}
		if report.Accounting.RetainedJSONPayload != "absent_declared_columns_only" {
			t.Fatalf("retained JSON label=%q", report.Accounting.RetainedJSONPayload)
		}
		if len(report.Accounting.CompressionDetail) == 0 {
			t.Fatalf("missing compression detail: %+v", report.Accounting)
		}
		if len(report.CompressionRows) == 0 {
			t.Fatalf("missing compression report rows: %+v", report)
		}
		for _, row := range report.CompressionRows {
			if row.CodecLayoutLabel == "" || row.CompressionPolicyLabel == "" || row.RequestedCompression == "" || row.ActualCompression == "" || row.SupportState == "" {
				t.Fatalf("build compression row missing labels: %+v", row)
			}
			if row.CompressedBytes == 0 || row.RawBytes == 0 || row.DecompressedBytes == 0 || row.CompressionRatio <= 0 {
				t.Fatalf("build compression row missing byte attribution: %+v", row)
			}
			if row.CompressionDurationSource == "" || row.DecompressionDurationSource == "" {
				t.Fatalf("build compression row missing duration sources: %+v", row)
			}
			if row.BenchmarkAllocationSource == "" {
				t.Fatalf("build compression row missing B/op/allocs source: %+v", row)
			}
		}
	}
}

func TestJSONBenchPartBuildReportZeroRowsDoesNotSetNanosPerRow(t *testing.T) {
	report := JSONBenchPartBuildReport{
		Rows: 0,
		Best: JSONBenchPartBuildAttempt{
			Duration: time.Nanosecond,
		},
	}
	report.fillDerivedMetrics()
	if report.NanosPerRow != 0 {
		t.Fatalf("nanos per row=%f want 0", report.NanosPerRow)
	}
}

func TestJSONBenchAggregateMetadataAdmissionRejectsOversizedMetadata(t *testing.T) {
	ds := syntheticJSONBenchDataset(256)
	opts, err := JSONBenchColumnPartOptionsWithAggregateMetadataForLayout(ds, 32, JSONBenchColumnPartLayoutTimeUS)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptionsWithAggregateMetadataForLayout: %v", err)
	}
	if len(opts.AggregateMetadata) != 1 {
		t.Fatalf("aggregate metadata defs=%d want 1", len(opts.AggregateMetadata))
	}
	opts.AggregateMetadata[0].MaxBytesPerRow = 0.001
	part, err := BuildColumnPart(1, opts, ColumnBatch{Rows: ds.Rows, Columns: ds.Columns})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	metadata, ok := part.AggregateMetadataByName(jsonBenchPostCreateDidTimeMetadata)
	if !ok {
		t.Fatalf("missing aggregate metadata %s", jsonBenchPostCreateDidTimeMetadata)
	}
	if metadata.Stats.Admitted {
		t.Fatalf("metadata admitted with tiny budget: %+v", metadata.Stats)
	}
	if metadata.Stats.RejectedReason == "" || metadata.Stats.TotalBytes == 0 || metadata.Stats.Entries == 0 {
		t.Fatalf("metadata rejection missing accounting: %+v", metadata.Stats)
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	if _, _, _, err := runJSONBenchPartQ5AggregateMetadata(part, codes, &jsonBenchPartQueryScratch{}); err == nil {
		t.Fatal("runJSONBenchPartQ5AggregateMetadata accepted metadata rejected by admission")
	}
}

func TestRunJSONBenchPartQ4EarlyStopRejectsClickHouseOrder(t *testing.T) {
	ds := syntheticJSONBenchDataset(128)
	part, err := BuildJSONBenchColumnPartForLayout(ds, 32, JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPartForLayout: %v", err)
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	if _, _, _, err := runJSONBenchPartQ4(part, codes, &jsonBenchPartQueryScratch{}); err == nil {
		t.Fatal("runJSONBenchPartQ4 accepted ClickHouse-order part, want fail-closed early-stop precondition")
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

func TestTinyCodeHeaderRejectsWideCodes(t *testing.T) {
	g, err := NewGranuleBuilder(Config{Compression: CompressionNone}).BuildUint32Codes([]uint32{1, 255, 256}, 300)
	if err != nil {
		t.Fatalf("BuildUint32Codes: %v", err)
	}
	block := ColumnBlock{
		Descriptor: ColumnBlockDescriptor{RowCount: 3},
		Granule:    g,
	}
	var scratch jsonBenchPartQueryScratch
	if _, err := scratch.tinyCodeHeader(0, block); err == nil {
		t.Fatal("tinyCodeHeader accepted width > 1, want error")
	}
}

func TestRunJSONBenchPartQ2FallsBackForHighCardinalityDID(t *testing.T) {
	ds := jsonBenchPartEdgeDataset()
	ds.Dictionaries["did_code"] = map[string]int64{"outside-low-cardinality-cap": maxCodeCardinality + 1}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	rawRows, rawDigest := runJSONBenchQ2(ds, codes)
	part, err := BuildJSONBenchColumnPart(ds, 2)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPart: %v", err)
	}
	rows, digest, diagnostics, err := runJSONBenchPartQ2(part, codes, &jsonBenchPartQueryScratch{})
	if err != nil {
		t.Fatalf("runJSONBenchPartQ2: %v", err)
	}
	if rows != rawRows || digest != rawDigest {
		t.Fatalf("Q2 fallback rows/digest=(%d,%d) raw=(%d,%d)", rows, digest, rawRows, rawDigest)
	}
	if diagnostics.AggregateKernel != "projected_scan_group_count_distinct" {
		t.Fatalf("Q2 fallback kernel=%q want projected scan", diagnostics.AggregateKernel)
	}
}

func TestRunJSONBenchPartQ3RejectsInvalidHourCode(t *testing.T) {
	ds := jsonBenchPartEdgeDataset()
	part, err := BuildJSONBenchColumnPart(ds, 2)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPart: %v", err)
	}
	hourColumn := part.Columns["hour_of_day"]
	hourCodes := []uint32{jsonBenchHoursPerDay, 0}
	invalidHourGranule, err := NewGranuleBuilder(Config{}).BuildUint32Codes(hourCodes, jsonBenchHoursPerDay+1)
	if err != nil {
		t.Fatalf("BuildUint32Codes(invalid hour): %v", err)
	}
	hourColumn.Blocks[0].Granule = invalidHourGranule
	hourColumn.Definition.Cardinality = jsonBenchHoursPerDay + 1
	part.Columns["hour_of_day"] = hourColumn
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	_, _, _, err = runJSONBenchPartQ3(part, codes, &jsonBenchPartQueryScratch{})
	if err == nil || !strings.Contains(err.Error(), "hour_of_day code") {
		t.Fatalf("runJSONBenchPartQ3 err=%v want hour_of_day validation", err)
	}
}

func TestRunJSONBenchPartQ4RejectsIncompatibleSortKey(t *testing.T) {
	ds := jsonBenchPartEdgeDataset()
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	cases := []struct {
		name    string
		sortKey []SortKeyColumn
	}{
		{name: "missing"},
		{name: "compound", sortKey: []SortKeyColumn{{Column: "time_us"}, {Column: "row_index"}}},
		{name: "descending", sortKey: []SortKeyColumn{{Column: "time_us", Direction: SortKeyDesc}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			part, err := BuildJSONBenchColumnPart(ds, 2)
			if err != nil {
				t.Fatalf("BuildJSONBenchColumnPart: %v", err)
			}
			part.Descriptor.SortKey = append([]SortKeyColumn(nil), tc.sortKey...)
			_, _, _, err = runJSONBenchPartQ4(part, codes, &jsonBenchPartQueryScratch{})
			if err == nil || !strings.Contains(err.Error(), "q4 early-stop requires") {
				t.Fatalf("runJSONBenchPartQ4 err=%v want sort-key validation", err)
			}
		})
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

func jsonBenchPartEdgeDataset() JSONBenchDataset {
	return JSONBenchDataset{
		Rows: 4,
		Columns: map[string][]int64{
			"row_index":              {0, 1, 2, 3},
			"time_us":                {0, 3_600_000_000, 7_200_000_000, 10_800_000_000},
			"hour_of_day":            {0, 1, 2, 3},
			"did_code":               {maxCodeCardinality + 1, 42, maxCodeCardinality + 1, 99},
			"kind_code":              {1, 1, 1, 1},
			"commit_operation_code":  {1, 1, 1, 1},
			"commit_collection_code": {1, 1, 2, 3},
		},
		Dictionaries: map[string]map[string]int64{
			"did_code": {
				"a": maxCodeCardinality + 1,
				"b": 42,
				"c": 99,
			},
			"kind_code": {
				"commit": 1,
			},
			"commit_operation_code": {
				"create": 1,
			},
			"commit_collection_code": {
				"app.bsky.feed.post":   1,
				"app.bsky.feed.repost": 2,
				"app.bsky.feed.like":   3,
			},
		},
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
		raw, ok := rawByQuery[timing.Query]
		if !ok {
			t.Fatalf("unexpected local part query %s", timing.Query)
		}
		if timing.ResultRows != raw.ResultRows || timing.ResultDigest != raw.ResultDigest {
			t.Fatalf("%s local part rows/digest=(%d,%d) raw=(%d,%d)", timing.Query, timing.ResultRows, timing.ResultDigest, raw.ResultRows, raw.ResultDigest)
		}
	}
}

func TestRunJSONBenchPartAggregateMetadataQueriesLocalIfPresent(t *testing.T) {
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
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	rawQ4Rows, rawQ4Digest := runJSONBenchQ4(ds, codes)
	rawQ5Rows, rawQ5Digest := runJSONBenchQ5(ds, codes)
	timings, err := RunJSONBenchPartAggregateMetadataQueries(ds, DefaultRowsPerGranule, 2)
	if err != nil {
		t.Fatalf("RunJSONBenchPartAggregateMetadataQueries(local): %v", err)
	}
	for _, timing := range timings {
		if !timing.Diagnostics.AggregateMetadataUsed {
			t.Fatalf("%s local diagnostics did not use aggregate metadata: %+v", timing.Query, timing.Diagnostics)
		}
		switch timing.Query {
		case "Q4b-meta":
			if timing.ResultRows != rawQ4Rows || timing.ResultDigest != rawQ4Digest {
				t.Fatalf("Q4b-meta local rows/digest=(%d,%d) raw=(%d,%d)", timing.ResultRows, timing.ResultDigest, rawQ4Rows, rawQ4Digest)
			}
		case "Q5-meta":
			if timing.ResultRows != rawQ5Rows || timing.ResultDigest != rawQ5Digest {
				t.Fatalf("Q5-meta local rows/digest=(%d,%d) raw=(%d,%d)", timing.ResultRows, timing.ResultDigest, rawQ5Rows, rawQ5Digest)
			}
		}
	}
}

func assertJSONBenchPartDiagnostics(t *testing.T, query string, diagnostics JSONBenchPartQueryDiagnostics, rows int, granules int) {
	t.Helper()
	if diagnostics.RowsScanned != rows {
		t.Fatalf("%s diagnostics rows=%d want %d: %+v", query, diagnostics.RowsScanned, rows, diagnostics)
	}
	if diagnostics.GranulesConsidered <= 0 || diagnostics.GranulesConsidered > granules {
		t.Fatalf("%s diagnostics granules=%d want 1..%d: %+v", query, diagnostics.GranulesConsidered, granules, diagnostics)
	}
	if diagnostics.GranulesSkipped < 0 || diagnostics.GranulesSkipped > diagnostics.GranulesConsidered {
		t.Fatalf("%s diagnostics skipped=%d want 0..%d: %+v", query, diagnostics.GranulesSkipped, diagnostics.GranulesConsidered, diagnostics)
	}
	if diagnostics.GranulesDecoded <= 0 || diagnostics.GranulesDecoded > diagnostics.GranulesConsidered {
		t.Fatalf("%s diagnostics decoded granules=%d want 1..%d: %+v", query, diagnostics.GranulesDecoded, diagnostics.GranulesConsidered, diagnostics)
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

func assertStringSliceEqual(t *testing.T, got []string, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("slice=%v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("slice=%v want %v", got, want)
		}
	}
}
