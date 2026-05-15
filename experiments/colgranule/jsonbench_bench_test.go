package colgranule

import (
	"os"
	"strconv"
	"testing"
)

func BenchmarkJSONBenchLocalColumns(b *testing.B) {
	path := os.Getenv("JSONBENCH_DATA")
	if path == "" {
		path = DefaultJSONBenchDir
	}
	if _, err := os.Stat(path); err != nil {
		b.Skipf("JSONBench data not present; set JSONBENCH_DATA or install %s", DefaultJSONBenchDir)
	}
	limit := 1000000
	ds, err := LoadJSONBenchColumns(path, limit)
	if err != nil {
		b.Fatalf("LoadJSONBenchColumns: %v", err)
	}
	b.Logf("loaded rows=%d columns=%d path=%s", ds.Rows, len(ds.Columns), path)
	for _, name := range ds.ColumnNames() {
		values := ds.Columns[name]
		for _, cfg := range DefaultJSONBenchConfigs() {
			b.Run(name+"/"+cfg.Encoding.String()+"/"+cfg.Compression.String(), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(values) * 8))
				for i := 0; i < b.N; i++ {
					summaries, err := SummarizeJSONBenchDataset(JSONBenchDataset{
						Rows:    ds.Rows,
						Columns: map[string][]int64{name: values},
					}, DefaultRowsPerGranule, []Config{cfg})
					if err != nil {
						b.Fatal(err)
					}
					benchSink += int64(summaries[0].StoredBytes)
				}
			})
		}
	}
}

func BenchmarkJSONBenchEncodedPartQueries(b *testing.B) {
	ds := syntheticJSONBenchDataset(DefaultRowsPerGranule * 100)
	part, err := BuildJSONBenchColumnPart(ds, DefaultRowsPerGranule)
	if err != nil {
		b.Fatalf("BuildJSONBenchColumnPart: %v", err)
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		b.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	queries := []struct {
		name string
		run  jsonBenchPartQueryRunner
	}{
		{"Q1_grouped_count", runJSONBenchPartQ1},
		{"Q2_group_count_distinct", runJSONBenchPartQ2},
		{"Q3_hourly_grouped_count", runJSONBenchPartQ3},
		{"Q4_min_by_user", runJSONBenchPartQ4},
		{"Q5_span_by_user", runJSONBenchPartQ5},
	}
	for _, q := range queries {
		b.Run(q.name, func(b *testing.B) {
			b.ReportAllocs()
			scratch := &jsonBenchPartQueryScratch{
				scanner:   part.NewScanner(),
				projected: make(map[string][]int64, 6),
			}
			rows, digest, diagnostics, err := q.run(part, codes, scratch)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(rows) + int64(digest)
			rowsScanned := diagnostics.RowsScanned
			if rowsScanned == 0 {
				rowsScanned = ds.Rows
			}
			valueBytes := rowsScanned * len(diagnostics.ColumnsProjected) * 8
			if valueBytes == 0 {
				valueBytes = rowsScanned * 8
			}
			b.SetBytes(int64(valueBytes))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, digest, diagnostics, err = q.run(part, codes, scratch)
				if err != nil {
					b.Fatal(err)
				}
				benchSink += int64(rows) + int64(digest)
			}
			rowsScanned = diagnostics.RowsScanned
			if rowsScanned == 0 {
				rowsScanned = ds.Rows
			}
			reportGranuleBenchMetrics(b, rowsScanned, valueBytes, diagnostics.BytesDecoded)
		})
	}
}

func BenchmarkJSONBenchQ4SortOrderFairness(b *testing.B) {
	ds := syntheticJSONBenchDataset(DefaultRowsPerGranule * 100)
	timePart, err := BuildJSONBenchColumnPartForLayout(ds, DefaultRowsPerGranule, JSONBenchColumnPartLayoutTimeUS)
	if err != nil {
		b.Fatalf("BuildJSONBenchColumnPartForLayout(time): %v", err)
	}
	clickHouseOrderPart, err := BuildJSONBenchColumnPartForLayout(ds, DefaultRowsPerGranule, JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	if err != nil {
		b.Fatalf("BuildJSONBenchColumnPartForLayout(clickhouse): %v", err)
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		b.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	queries := []struct {
		name string
		part *ColumnPart
		run  jsonBenchPartQueryRunner
	}{
		{"Q4a_time_ordered", timePart, runJSONBenchPartQ4},
		{"Q4b_clickhouse_order", clickHouseOrderPart, runJSONBenchPartQ4ClickHouseOrder},
	}
	for _, q := range queries {
		b.Run(q.name, func(b *testing.B) {
			b.ReportAllocs()
			scratch := &jsonBenchPartQueryScratch{
				scanner:   q.part.NewScanner(),
				projected: make(map[string][]int64, 6),
			}
			rows, digest, diagnostics, err := q.run(q.part, codes, scratch)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(rows) + int64(digest)
			rowsScanned := diagnostics.RowsScanned
			valueBytes := rowsScanned * len(diagnostics.ColumnsProjected) * 8
			if valueBytes == 0 {
				valueBytes = rowsScanned * 8
			}
			b.SetBytes(int64(valueBytes))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, digest, diagnostics, err = q.run(q.part, codes, scratch)
				if err != nil {
					b.Fatal(err)
				}
				benchSink += int64(rows) + int64(digest)
			}
			reportGranuleBenchMetrics(b, diagnostics.RowsScanned, valueBytes, diagnostics.BytesDecoded)
		})
	}
}

func BenchmarkJSONBenchAggregateMetadataQueries(b *testing.B) {
	ds := syntheticJSONBenchDataset(DefaultRowsPerGranule * 100)
	timePart, err := BuildJSONBenchColumnPartWithAggregateMetadataForLayout(ds, DefaultRowsPerGranule, JSONBenchColumnPartLayoutTimeUS)
	if err != nil {
		b.Fatalf("BuildJSONBenchColumnPartWithAggregateMetadataForLayout(time): %v", err)
	}
	clickHouseOrderPart, err := BuildJSONBenchColumnPartWithAggregateMetadataForLayout(ds, DefaultRowsPerGranule, JSONBenchColumnPartLayoutClickHouseFilterUserTime)
	if err != nil {
		b.Fatalf("BuildJSONBenchColumnPartWithAggregateMetadataForLayout(clickhouse): %v", err)
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		b.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	queries := []struct {
		name string
		part *ColumnPart
		run  jsonBenchPartQueryRunner
	}{
		{"Q4b_metadata_min_by_user", clickHouseOrderPart, runJSONBenchPartQ4ClickHouseOrderAggregateMetadata},
		{"Q5_metadata_span_by_user", timePart, runJSONBenchPartQ5AggregateMetadata},
		{"Q4b_row_scan_min_by_user", clickHouseOrderPart, runJSONBenchPartQ4ClickHouseOrder},
		{"Q5_row_scan_span_by_user", timePart, runJSONBenchPartQ5},
	}
	for _, q := range queries {
		b.Run(q.name, func(b *testing.B) {
			b.ReportAllocs()
			scratch := &jsonBenchPartQueryScratch{
				scanner:   q.part.NewScanner(),
				projected: make(map[string][]int64, 6),
			}
			rows, digest, diagnostics, err := q.run(q.part, codes, scratch)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(rows) + int64(digest)
			rowsMeasured := diagnostics.RowsScanned
			if rowsMeasured == 0 {
				rowsMeasured = diagnostics.AggregateMetadataEntries
			}
			if rowsMeasured == 0 {
				rowsMeasured = ds.Rows
			}
			valueBytes := rowsMeasured * len(diagnostics.ColumnsProjected) * 8
			if valueBytes == 0 {
				valueBytes = rowsMeasured * 8
			}
			b.SetBytes(int64(valueBytes))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, digest, diagnostics, err = q.run(q.part, codes, scratch)
				if err != nil {
					b.Fatal(err)
				}
				benchSink += int64(rows) + int64(digest)
			}
			rowsMeasured = diagnostics.RowsScanned
			storedBytes := diagnostics.BytesDecoded
			if rowsMeasured == 0 {
				rowsMeasured = diagnostics.AggregateMetadataEntries
				storedBytes = diagnostics.AggregateMetadataBytes
			}
			reportGranuleBenchMetrics(b, rowsMeasured, valueBytes, storedBytes)
		})
	}
}

func BenchmarkJSONBenchAggregateMetadataBuild(b *testing.B) {
	ds := syntheticJSONBenchDataset(DefaultRowsPerGranule * 100)
	for _, layout := range []JSONBenchColumnPartLayout{
		JSONBenchColumnPartLayoutTimeUS,
		JSONBenchColumnPartLayoutClickHouseFilterUserTime,
	} {
		b.Run(string(layout), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(ds.Rows * len(ds.Columns) * 8))
			for i := 0; i < b.N; i++ {
				part, err := BuildJSONBenchColumnPartWithAggregateMetadataForLayout(ds, DefaultRowsPerGranule, layout)
				if err != nil {
					b.Fatal(err)
				}
				metadata, ok := part.AggregateMetadataByName(jsonBenchPostCreateDidTimeMetadata)
				if !ok || !metadata.Stats.Admitted {
					b.Fatalf("metadata missing/rejected: %+v", metadata.Stats)
				}
				benchSink += int64(metadata.Stats.TotalBytes)
			}
		})
	}
}

func BenchmarkJSONBenchPartBuildM1C(b *testing.B) {
	ds := syntheticJSONBenchDataset(DefaultRowsPerGranule * 100)
	benchmarkJSONBenchPartBuildM1C(b, ds)
}

func BenchmarkJSONBenchLocalPartBuildM1C(b *testing.B) {
	path := os.Getenv("JSONBENCH_DATA")
	if path == "" {
		path = DefaultJSONBenchDir
	}
	if _, err := os.Stat(path); err != nil {
		b.Skipf("JSONBench data not present; set JSONBENCH_DATA or install %s", DefaultJSONBenchDir)
	}
	ds, err := LoadJSONBenchColumns(path, 1_000_000)
	if err != nil {
		b.Fatalf("LoadJSONBenchColumns: %v", err)
	}
	benchmarkJSONBenchPartBuildM1C(b, ds)
}

func benchmarkJSONBenchPartBuildM1C(b *testing.B, ds JSONBenchDataset) {
	for _, layout := range []JSONBenchColumnPartLayout{
		JSONBenchColumnPartLayoutTimeUS,
		JSONBenchColumnPartLayoutClickHouseFilterUserTime,
	} {
		b.Run(string(layout), func(b *testing.B) {
			part, err := BuildJSONBenchColumnPartWithAggregateMetadataForLayout(ds, DefaultRowsPerGranule, layout)
			if err != nil {
				b.Fatalf("BuildJSONBenchColumnPartWithAggregateMetadataForLayout: %v", err)
			}
			accounting := part.ByteAccounting()
			accounting.DictionaryBytes = EstimateJSONBenchDictionaryBytes(ds)
			accounting.RecomputeTotals()
			b.ReportAllocs()
			b.SetBytes(int64(accounting.EncodedRawBytes))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				part, err = BuildJSONBenchColumnPartWithAggregateMetadataForLayout(ds, DefaultRowsPerGranule, layout)
				if err != nil {
					b.Fatal(err)
				}
				benchSink += int64(part.ByteAccounting().TotalStoredBytes)
			}
			seconds := b.Elapsed().Seconds()
			if seconds > 0 {
				b.ReportMetric(float64(ds.Rows*b.N)/seconds, "rows/s")
				b.ReportMetric(float64(accounting.EncodedRawBytes*b.N)/seconds/(1024*1024), "encoded_MiB/s")
				b.ReportMetric(float64(accounting.TotalStoredBytes*b.N)/seconds/(1024*1024), "stored_MiB/s")
			}
			if ds.Rows > 0 {
				b.ReportMetric(float64(accounting.TotalStoredBytes)/float64(ds.Rows), "output_B/row")
				b.ReportMetric(float64(accounting.EncodedRawBytes)/float64(ds.Rows), "encoded_B/row")
			}
		})
	}
}

func syntheticJSONBenchDataset(rows int) JSONBenchDataset {
	columns := map[string][]int64{
		"row_index":              make([]int64, rows),
		"time_us":                make([]int64, rows),
		"hour_of_day":            make([]int64, rows),
		"did_code":               make([]int64, rows),
		"kind_code":              make([]int64, rows),
		"commit_operation_code":  make([]int64, rows),
		"commit_collection_code": make([]int64, rows),
	}
	const didCardinality = 65536
	for i := 0; i < rows; i++ {
		columns["row_index"][i] = int64(i)
		columns["time_us"][i] = 1_700_000_000_000_000 + int64(i)*1_000_000
		columns["hour_of_day"][i] = unixMicroHour(columns["time_us"][i])
		columns["did_code"][i] = int64((i % didCardinality) + 1)
		columns["kind_code"][i] = 1
		columns["commit_operation_code"][i] = 1
		columns["commit_collection_code"][i] = int64((i % 3) + 1)
	}
	didDict := make(map[string]int64, didCardinality)
	for i := 1; i <= didCardinality; i++ {
		didDict[strconv.Itoa(i)] = int64(i)
	}
	return JSONBenchDataset{
		Rows:    rows,
		Columns: columns,
		Dictionaries: map[string]map[string]int64{
			"did_code": didDict,
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
